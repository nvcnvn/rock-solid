package uring

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/loop"
	"golang.org/x/sys/unix"
)

const enterGetEvents = 1

// durableGaps are the idle gaps between one write being durable and the next being proposed, and
// how many writes each runs.
var durableGaps = []struct {
	gap time.Duration
	n   int
}{{0, 1000}, {10 * time.Millisecond, 200}, {100 * time.Millisecond, 50}, {time.Second, 15}}

// TestDurableWriteBenchmark runs durable writes at queue depth 1 through an event loop, a disk log
// and io_uring, each a 128-byte proposal whose round's write and linked flush must complete before
// the next is proposed, after each of durableGaps' idle gaps. It runs on RS_RAW_DEVICE and on a
// disk file written in full under RS_URING_DIR, each with the loop waiting in epoll_wait for the
// ring's eventfd, as it does, and waiting in io_uring_enter whenever the ring has I/O in flight.
// It logs latency, and the CPU time and cycles of the loop's thread per write; it asserts no
// timing.
func TestDurableWriteBenchmark(t *testing.T) {
	logCPUPolicy(t)
	t.Run("device", func(t *testing.T) {
		dev := rawDevice(t)
		for _, wait := range []bool{false, true} {
			t.Run(waitName(wait), func(t *testing.T) { durableRuns(t, dev, wait) })
		}
	})
	t.Run("file", func(t *testing.T) {
		if os.Getenv("RS_URING_DIR") == "" {
			t.Skip("RS_URING_DIR names no directory on the drive to measure")
		}
		path := diskFile(t, 4<<30/bs)
		for _, wait := range []bool{false, true} {
			t.Run(waitName(wait), func(t *testing.T) { durableRuns(t, path, wait) })
		}
	})
}

func waitName(wait bool) string {
	if wait {
		return "wait"
	}
	return "event"
}

// logCPUPolicy logs the frequency driver, governor and idle states of CPU 0.
func logCPUPolicy(t *testing.T) {
	read := func(p string) string {
		b, err := os.ReadFile(p)
		if err != nil {
			return "?"
		}
		return strings.TrimSpace(string(b))
	}
	const cpu = "/sys/devices/system/cpu/cpu0/"
	states, _ := filepath.Glob(cpu + "cpuidle/state*")
	var idle []string
	for _, s := range states {
		name := read(s + "/name")
		if read(s+"/disable") == "1" {
			name += " (disabled)"
		}
		idle = append(idle, name)
	}
	t.Logf("cpufreq %s, governor %s, EPP %s, %s to %s kHz; idle states %s", read(cpu+"cpufreq/scaling_driver"),
		read(cpu+"cpufreq/scaling_governor"), read(cpu+"cpufreq/energy_performance_preference"),
		read(cpu+"cpufreq/scaling_min_freq"), read(cpu+"cpufreq/scaling_max_freq"), strings.Join(idle, " "))
}

func durableRuns(t *testing.T, path string, wait bool) {
	r, pool := newRing(t, 64<<20, 64)
	d, err := r.Open(path, false)
	if err != nil {
		t.Fatal(err)
	}
	geo, err := disklog.GeometryFor(d.Blocks() * bs)
	if err != nil {
		t.Fatal(err)
	}
	cfg := disklog.DefaultConfig()
	cfg.MaxReplayBytes, cfg.NoReserve, cfg.NoTrim = 1<<60, true, true
	log := formatOn(t, r, d, pool, cfg, disklog.Superblock{LogID: 1, Geometry: geo})
	k, err := loop.NewEpoll("/nonexistent/vmclock0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(k.Close)
	var kern loop.Kernel = k
	if wait {
		kern = ringWait{k, r}
	}
	w := &writer{pool: pool}
	drv := &countingDriver{d: d}
	w.l = loop.New(loop.Config{Pool: pool}, kern, w, drv)
	if err := w.l.AttachDisk(r); err != nil {
		t.Fatal(err)
	}
	w.id = w.l.AddShard(w, w.l.AttachLog(log, 1, 1<<32, 1))

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	cycles := cycleCounter(t)
	for _, g := range durableGaps {
		w.gap, w.left, w.lat = core.TickFromDuration(g.gap), g.n, w.lat[:0]
		drv.writes, drv.bytes = 0, 0
		var self0, self1, th0, th1 unix.Rusage
		_ = unix.Getrusage(unix.RUSAGE_SELF, &self0)
		_ = unix.Getrusage(unix.RUSAGE_THREAD, &th0)
		c0 := cycles()
		w.propose()
		for w.left > 0 {
			w.l.Pass()
		}
		c1 := cycles()
		_ = unix.Getrusage(unix.RUSAGE_THREAD, &th1)
		_ = unix.Getrusage(unix.RUSAGE_SELF, &self1)
		if w.err != nil {
			t.Fatal(w.err)
		}
		n := len(w.lat)
		thread, self := cpuOf(&th0, &th1)/time.Duration(n), cpuOf(&self0, &self1)/time.Duration(n)
		cyc := (c1 - c0) / uint64(n)
		slices.Sort(w.lat)
		t.Logf("gap %v: %d writes of %d B: p50 %v, p90 %v, p99 %v, max %v; loop thread %v CPU and %d cycles a write, process %v CPU",
			g.gap, n, drv.bytes/max(drv.writes, 1), w.lat[n/2], w.lat[n*9/10], w.lat[n*99/100], w.lat[n-1], thread, cyc, self)
	}
}

func cpuOf(a, b *unix.Rusage) time.Duration {
	return time.Duration(b.Utime.Nano() + b.Stime.Nano() - a.Utime.Nano() - a.Stime.Nano())
}

// cycleCounter counts the CPU cycles of the calling thread, user and kernel, which must stay locked
// to it; it reads 0 where the kernel offers no counter, and fails the test if the kernel multiplexed
// the counter, which would undercount.
func cycleCounter(t *testing.T) func() uint64 {
	attr := unix.PerfEventAttr{
		Type:        unix.PERF_TYPE_HARDWARE,
		Config:      unix.PERF_COUNT_HW_CPU_CYCLES,
		Size:        uint32(unsafe.Sizeof(unix.PerfEventAttr{})),
		Read_format: unix.PERF_FORMAT_TOTAL_TIME_ENABLED | unix.PERF_FORMAT_TOTAL_TIME_RUNNING,
	}
	fd, err := unix.PerfEventOpen(&attr, 0, -1, -1, unix.PERF_FLAG_FD_CLOEXEC)
	if err != nil {
		t.Logf("no cycle counter: %v", err)
		return func() uint64 { return 0 }
	}
	t.Cleanup(func() { _ = unix.Close(fd) })
	var b [24]byte
	return func() uint64 {
		if _, err := unix.Read(fd, b[:]); err != nil {
			t.Fatal(err)
		}
		value, enabled, running := binary.NativeEndian.Uint64(b[0:]), binary.NativeEndian.Uint64(b[8:]), binary.NativeEndian.Uint64(b[16:])
		if running < enabled {
			t.Fatalf("cycle counter multiplexed: running %d ns of %d enabled", running, enabled)
		}
		return value
	}
}

// ringWait waits in io_uring_enter for a completion, instead of in epoll_wait, whenever the loop
// would block and its ring has I/O in flight; nothing else wakes such a wait, so it stands for a
// loop whose ring is all it waits on. The completion signals the eventfd too, which epoll then
// reports.
type ringWait struct {
	loop.Kernel
	r *Ring
}

func (k ringWait) Wait(ready []loop.Ready, block bool) []loop.Ready {
	if block && k.r.InFlight() > 0 {
		for {
			_, _, e := unix.Syscall6(unix.SYS_IO_URING_ENTER, uintptr(k.r.fd), 0, 1, enterGetEvents, 0, 0)
			if e != unix.EINTR {
				break
			}
		}
	}
	return k.Kernel.Wait(ready, block)
}

// writer is the loop's machine and its one shard: a proposal of 128 bytes, then, once it is
// durable, the next after gap, until left are durable.
type writer struct {
	l    *loop.Loop
	id   loop.ShardID
	pool core.BufPool
	gap  core.Tick
	left int
	at   time.Time
	lat  []time.Duration
	err  error
	ask  bool
}

func (w *writer) OnEvent(_ *core.Context, ev *core.Event, out []core.Action) []core.Action {
	if ev.Kind == core.EvTimer {
		w.propose()
	}
	return out
}

func (w *writer) propose() {
	w.ask, w.at = true, time.Now()
	w.l.Mark(w.id)
}

func (w *writer) HasReady() bool { return w.ask }

func (w *writer) Ready(items []disklog.Item) []disklog.Item {
	buf, err := w.pool.Alloc(128)
	if err != nil {
		panic(err)
	}
	w.ask = false
	return append(items, disklog.Item{Entry: disklog.BlobEntry(disklog.BlobRaftEntries, uint32(w.id), 128), Buf: buf})
}

func (w *writer) Durable(ctx *core.Context, err error, _ []disklog.BlobRef, out []core.Action) []core.Action {
	w.lat = append(w.lat, time.Since(w.at))
	if err != nil && w.err == nil {
		w.err = err
	}
	if w.left--; w.left == 0 {
		return out
	}
	if w.gap == 0 {
		w.propose()
		return out
	}
	return append(out, core.Action{Kind: core.ActSetTimer, Timer: 1, At: ctx.Now + w.gap})
}

func (w *writer) Mail(_ *core.Context, _ *loop.Mail, out []core.Action) []core.Action { return out }

type countingDriver struct {
	d             *Disk
	writes, bytes int
}

func (*countingDriver) Read(loop.ConnID, []byte) {}
func (*countingDriver) Closed(loop.ConnID)       {}
func (*countingDriver) Accept(loop.ConnID)       {}
func (*countingDriver) Flush() bool              { return false }
func (c *countingDriver) Execute(a *core.Action) {
	if a.Kind == core.ActDiskWrite {
		c.writes++
		c.bytes += int(a.Len)
	}
	c.d.Execute(a)
}

// formatOn formats a log on d, running its actions on r until it asks for no more.
func formatOn(t *testing.T, r *Ring, d *Disk, pool core.BufPool, cfg disklog.Config, sb disklog.Superblock) *disklog.Log {
	t.Helper()
	ctx := core.Context{Now: 1, Raw: 1, Pool: pool}
	var out disklog.Output
	log, err := disklog.Format(&ctx, cfg, sb, 1, &out)
	if err != nil {
		t.Fatal(err)
	}
	for len(out.Actions) > 0 {
		n := len(out.Actions)
		for i := range out.Actions {
			d.Execute(&out.Actions[i])
		}
		out.Reset()
		r.Submit()
		for _, ev := range reap(t, r, n) {
			log.OnEvent(&ctx, &ev, &out)
		}
	}
	return log
}

// TestRecoveryReadsAFullDisksSegmentHeaders writes a segment header into every segment of
// RS_RAW_DEVICE, as a disk the log has filled holds, and times recovery's reads of them at several
// read depths, up to the last header read.
func TestRecoveryReadsAFullDisksSegmentHeaders(t *testing.T) {
	dev := rawDevice(t)
	r, pool := newRing(t, 64<<20, 256)
	d, err := r.Open(dev, false)
	if err != nil {
		t.Fatal(err)
	}
	geo, err := disklog.GeometryFor(d.Blocks() * bs)
	if err != nil {
		t.Fatal(err)
	}
	cfg := disklog.DefaultConfig()
	sb := disklog.Superblock{LogID: 7, Geometry: geo}
	formatOn(t, r, d, pool, cfg, sb)
	writeSegmentHeaders(t, dev, sb)
	for _, depth := range []int{1, 8, 64, 256} {
		cfg.ReadDepth = depth
		ctx := core.Context{Now: 1, Raw: 1, Pool: pool}
		var out disklog.Output
		headers := map[core.IOID]bool{}
		var evs []core.Event
		var drain [8]byte
		done, most := 0, 0
		start := time.Now()
		rec, err := disklog.Recover(&ctx, cfg, d.Blocks(), disklog.ModeOpen, 1<<40, &out)
		if err != nil {
			t.Fatal(err)
		}
		for done < int(geo.SegmentCount)-1 {
			for i := range out.Actions {
				if a := &out.Actions[i]; a.Kind == core.ActDiskRead && a.Len == 2*bs {
					headers[a.IO] = true
				}
				d.Execute(&out.Actions[i])
			}
			out.Reset()
			r.Submit()
			most = max(most, r.InFlight())
			if evs = r.Reap(evs[:0]); len(evs) == 0 {
				_, _ = unix.Poll([]unix.PollFd{{Fd: int32(r.Fd()), Events: unix.POLLIN}}, -1)
				_, _ = unix.Read(r.Fd(), drain[:])
				continue
			}
			for _, ev := range evs {
				if ev.Err != core.DiskOK {
					t.Fatalf("completion %+v", ev)
				}
				if headers[ev.IO] {
					done++
				}
				rec.OnEvent(&ctx, &ev, &out)
			}
			if rec.Err() != nil {
				t.Fatal(rec.Err())
			}
		}
		took := time.Since(start)
		t.Logf("read depth %d: %d segment headers of %d MiB segments in %v, %v each, at most %d reads in flight",
			depth, done, geo.SegmentBytes()>>20, took, took/time.Duration(done), most)
	}
}

// writeSegmentHeaders writes a valid header, and a zero block after it, at the start of every
// segment but segment 0.
func writeSegmentHeaders(t *testing.T, dev string, sb disklog.Superblock) {
	fd, err := unix.Open(dev, unix.O_RDWR|unix.O_DIRECT|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unix.Close(fd) }()
	buf, err := unix.Mmap(-1, 0, 2*bs, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANON)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unix.Munmap(buf) }()
	for s := uint32(1); s < sb.SegmentCount; s++ {
		h := disklog.SegmentHeader{LogID: sb.LogID, Seq: uint64(s), Index: s, Stream: disklog.Cold, Lane: uint8(s % 2)}
		if _, err := disklog.AppendSegmentHeader(buf[:0], h); err != nil {
			t.Fatal(err)
		}
		if _, err := unix.Pwrite(fd, buf, int64(sb.LBA(s, 0))*bs); err != nil {
			t.Fatal(fmt.Errorf("segment %d: %w", s, err))
		}
	}
	if err := unix.Fdatasync(fd); err != nil {
		t.Fatal(err)
	}
}
