package looptest

import (
	"fmt"
	"os"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/loop"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// timedDisk is Disk on real time: a clock goroutine signals its eventfd once the earliest
// completion, due, is.
type timedDisk struct {
	*Disk
	efd    int
	due    atomic.Int64
	armed  core.Tick
	writes int
}

func (d *timedDisk) Fd() int { return d.efd }

func (d *timedDisk) Execute(a *core.Action) {
	if a.Kind == core.ActDiskWrite {
		d.writes++
	}
	d.Disk.Execute(a)
	d.arm()
}

func (d *timedDisk) Reap(evs []core.Event) []core.Event {
	evs = d.Disk.Reap(evs)
	d.arm()
	return evs
}

func (d *timedDisk) arm() {
	if at, ok := d.Next(); ok && at != d.armed {
		d.armed = at
		d.due.Store(int64(at))
	}
}

type timedDriver struct {
	diskDriver
	d *timedDisk
}

func (t *timedDriver) Execute(a *core.Action) {
	if t.d != nil {
		t.d.Execute(a)
	}
}

// fed proposes to a random shard of its loop as many times as each timer event the clock
// goroutine posts says.
type fed struct {
	shards []*stub
	rand   *core.Rand
}

func (f *fed) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	if ev.Kind != core.EvTimer || len(f.shards) == 0 {
		return out
	}
	for range ev.Timer {
		f.shards[f.rand.Below(uint64(len(f.shards)))].propose(ctx.Now)
	}
	return out
}

func bootUs() int64 {
	var ts unix.Timespec
	_ = unix.ClockGettime(unix.CLOCK_BOOTTIME, &ts)
	return ts.Nano() / 1000
}

// TestRoundLatencyOnRealLoops measures commit latency on real loop goroutines over epoll, for as
// long as RS_ROUND_LATENCY says, against the raft rounds spike's 3 ms p50 and 5 ms p99. Like the
// stepped rig, each producer loop has 16 shards and gets a proposal every 20 µs, and the owner's
// disk completes a write after 20 µs and its flush after 0.95 ms. A clock goroutine spinning on
// its own thread posts the proposals, one or eight to an inbox event, and signals the
// completions, since a kernel without CONFIG_HIGH_RES_TIMERS, such as Docker Desktop's, fires a
// timerfd on the next 1 ms tick. It asserts no timing.
func TestRoundLatencyOnRealLoops(t *testing.T) {
	run, err := time.ParseDuration(os.Getenv("RS_ROUND_LATENCY"))
	if err != nil {
		t.Skip("RS_ROUND_LATENCY is not a duration; run in docker capped at 4 CPUs")
	}
	for _, n := range []int{1, 8} {
		for _, batch := range []int{1, 8} {
			t.Run(fmt.Sprintf("%d loops, %d a post", n, batch), func(t *testing.T) { realRounds(t, n, 16, 20, batch, run) })
		}
	}
}

func realRounds(t *testing.T, n, perLoop int, gap int64, batch int, run time.Duration) {
	// A loop blocked in epoll_wait keeps its P until sysmon retakes it, so as a node runs
	// GOMAXPROCS - 1 loops, the run gives its loops and the clock goroutine a P each and one spare.
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(n + 3))
	lat := sim.DefaultDiskLatency
	lat.Flush = flush
	d := sim.NewDiskWith(1<<30/core.BlockSize, sim.DiskSettings{Latency: lat})
	var (
		loops []*loop.Loop
		pools []*loop.Pool
		ms    []*fed
		disk  *timedDisk
		log   *disklog.Log
	)
	for i := range n + 1 {
		k, err := loop.NewEpoll(loop.VMClockPath)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(k.Close)
		size := 4 << 20
		if i == n {
			size = 64 << 20
		}
		pool, err := loop.NewPool(size, 0)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = pool.Close() })
		m := &fed{rand: core.NewRand(uint64(i) + 1)}
		drv := &timedDriver{}
		l := loop.New(loop.Config{ID: uint16(i), Pool: pool}, k, m, drv)
		if i == n {
			log = formatLog(t, d, pool, roundLogConfig(0), 1)
			dr, err := NewDisk(k, d, pool)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(dr.Close)
			efd, err := unix.Eventfd(0, unix.EFD_NONBLOCK|unix.EFD_CLOEXEC)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = unix.Close(efd) })
			disk = &timedDisk{Disk: dr, efd: efd}
			if err := l.AttachDisk(disk); err != nil {
				t.Fatal(err)
			}
			drv.d = disk
		}
		loops, pools, ms = append(loops, l), append(pools, pool), append(ms, m)
	}
	lg := loops[n].AttachLog(log, 1, logIOs, n+1)
	lats := make([][]core.Tick, n)
	shards := make([][]*stub, n)
	for i := range n {
		lats[i] = make([]core.Tick, 0, 1<<20)
		for range perLoop {
			st := &stub{l: loops[i], pool: pools[i], lat: &lats[i], early: new(error)}
			st.id = loops[i].AddShard(st, lg)
			shards[i] = append(shards[i], st)
		}
		ms[i].shards = shards[i]
	}
	var before, after unix.Rusage
	_ = unix.Getrusage(unix.RUSAGE_SELF, &before)
	var wg sync.WaitGroup
	for _, l := range loops {
		wg.Go(l.Run)
	}
	var stop atomic.Bool
	var clockCPU time.Duration
	refused := 0
	wg.Go(func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		var start unix.Rusage
		_ = unix.Getrusage(unix.RUSAGE_THREAD, &start)
		one := [8]byte{1}
		ev := core.Event{Kind: core.EvTimer, Timer: core.TimerID(batch)}
		next := bootUs()
		for !stop.Load() {
			now := bootUs()
			if due := disk.due.Load(); due != 0 && now >= due && disk.due.CompareAndSwap(due, 0) {
				_, _ = unix.Write(disk.efd, one[:])
			}
			if now >= next {
				next += gap * int64(batch)
				for _, l := range loops[:n] {
					if !l.Inbox().Post(&ev) {
						refused++
					}
				}
			}
		}
		var end unix.Rusage
		_ = unix.Getrusage(unix.RUSAGE_THREAD, &end)
		clockCPU = time.Duration(end.Utime.Nano() + end.Stime.Nano() - start.Utime.Nano() - start.Stime.Nano())
	})
	time.Sleep(run)
	stop.Store(true)
	for _, l := range loops {
		l.Stop()
	}
	wg.Wait()
	_ = unix.Getrusage(unix.RUSAGE_SELF, &after)

	var all []core.Tick
	proposals := 0
	for i := range n {
		for _, st := range shards[i] {
			if st.err != nil || *st.early != nil {
				t.Fatalf("loop %d shard %d: %v, %v", i, st.id, st.err, *st.early)
			}
		}
		if len(lats[i]) == 0 {
			t.Fatalf("loop %d: no proposal durable", i)
		}
		proposals += len(lats[i])
		all = append(all, lats[i][len(lats[i])/10:]...)
	}
	if disk.writes == 0 {
		t.Fatal("the owner wrote no round")
	}
	slices.Sort(all)
	cpu := time.Duration(after.Utime.Nano()+after.Stime.Nano()-before.Utime.Nano()-before.Stime.Nano()) - clockCPU
	t.Logf("%d producer loops, GOMAXPROCS %d: p50 %d µs, p90 %d µs, p99 %d µs, max %d µs; %d rounds, %.0f proposals/s durable, %d refused, %.1f a round; loops' CPU %.2f cores, %v a round",
		n, runtime.GOMAXPROCS(0), all[len(all)/2], all[len(all)*90/100], all[len(all)*99/100], all[len(all)-1], disk.writes,
		float64(proposals)/run.Seconds(), refused, float64(proposals)/float64(disk.writes),
		cpu.Seconds()/run.Seconds(), cpu/time.Duration(disk.writes))
}
