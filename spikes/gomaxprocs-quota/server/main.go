// Command server runs N event loops shaped like the daemon's: each loop blocks
// in epoll_wait on one socket, reads with a raw syscall, and spends a fixed
// amount of CPU per 16-byte message. Background goroutines stand in for work
// off the data path: a pointerful live heap with steady garbage (so the
// collector runs) and a worker that computes in bursts (the cleaner).
//
// It reads its sockets from fds 3..3+loops-1 (passed by gen), signals readiness
// on fd 3+loops, and prints one RESULT line when every loop got its stop
// message.
package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/metrics"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

//go:linkname nanotime runtime.nanotime
func nanotime() int64

const histMaxUS = 1 << 20

var sink uint64

var garbageBytes, workerSpins atomic.Int64

func spin(iters int) uint64 {
	x := uint64(88172645463325252)
	for i := 0; i < iters; i++ {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
	}
	return x
}

type loopStats struct {
	hist [histMaxUS + 1]uint32
	ops  uint64
	_    [64]byte
}

func runLoop(fd int, iters int, lock bool, st *loopStats, done chan<- struct{}) {
	if lock {
		runtime.LockOSThread()
	}
	epfd, err := syscall.EpollCreate1(0)
	if err != nil {
		panic(err)
	}
	if err := syscall.SetNonblock(fd, true); err != nil {
		panic(err)
	}
	ev := syscall.EpollEvent{Events: syscall.EPOLLIN, Fd: int32(fd)}
	if err := syscall.EpollCtl(epfd, syscall.EPOLL_CTL_ADD, fd, &ev); err != nil {
		panic(err)
	}
	events := make([]syscall.EpollEvent, 4)
	buf := make([]byte, 64<<10)
	have := 0
	var acc uint64
	for {
		n, err := syscall.EpollWait(epfd, events, 1000)
		if err == syscall.EINTR || n == 0 {
			continue
		}
		if err != nil {
			panic(err)
		}
		for {
			r, _, e := syscall.RawSyscall(syscall.SYS_READ, uintptr(fd), uintptr(unsafe.Pointer(&buf[have])), uintptr(len(buf)-have))
			if e == syscall.EAGAIN {
				break
			}
			if e != 0 {
				panic(e)
			}
			if r == 0 {
				done <- struct{}{}
				return
			}
			have += int(r)
			off := 0
			for ; have-off >= 16; off += 16 {
				ts := le64(buf[off:])
				if ts == 0 {
					sink += acc
					done <- struct{}{}
					return
				}
				acc += spin(iters)
				us := (nanotime() - int64(ts)) / 1000
				if us > histMaxUS {
					us = histMaxUS
				}
				if us < 0 {
					us = 0
				}
				st.hist[us]++
				st.ops++
			}
			copy(buf, buf[off:have])
			have -= off
			if have+16 > len(buf) {
				panic("buffer")
			}
		}
	}
}

func le64(b []byte) uint64 {
	_ = b[7]
	return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
		uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48 | uint64(b[7])<<56
}

type node struct {
	next *node
	pad  [6]uint64
}

func buildLive(liveMB int) []*node {
	live := make([]*node, liveMB<<20/64)
	for i := range live {
		live[i] = &node{}
		if i > 0 {
			live[i].next = live[i-1]
		}
	}
	return live
}

func garbage(live []*node, rateMBs int, stop <-chan struct{}) {
	per := 64
	perTick := rateMBs << 20 / per / 1000
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	i := 0
	for {
		select {
		case <-stop:
			runtime.KeepAlive(live)
			return
		case <-tick.C:
		}
		for j := 0; j < perTick; j++ {
			live[i] = &node{next: live[(i+1)%len(live)]}
			i = (i + 7919) % len(live)
		}
		garbageBytes.Add(int64(perTick * per))
	}
}

func worker(burst, every time.Duration, iters int, stop <-chan struct{}) {
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		select {
		case <-stop:
			return
		case <-tick.C:
		}
		end := nanotime() + int64(burst)
		n := int64(0)
		for nanotime() < end {
			sink += spin(iters)
			n++
		}
		workerSpins.Add(n * int64(iters))
	}
}

func histPct(h *metrics.Float64Histogram, q float64) float64 {
	var total uint64
	for _, c := range h.Counts {
		total += c
	}
	if total == 0 {
		return 0
	}
	want := uint64(q * float64(total))
	var cum uint64
	for i, c := range h.Counts {
		cum += c
		if cum > want || (q == 1 && cum == total) {
			return h.Buckets[i+1]
		}
	}
	return h.Buckets[len(h.Buckets)-1]
}

func histMax(h *metrics.Float64Histogram) float64 {
	for i := len(h.Counts) - 1; i >= 0; i-- {
		if h.Counts[i] > 0 {
			return h.Buckets[i+1]
		}
	}
	return 0
}

func main() {
	loops := flag.Int("loops", 2, "event loops")
	iters := flag.Int("iters", 0, "spin iterations per message")
	lock := flag.Bool("lock", false, "LockOSThread in each loop")
	liveMB := flag.Int("live", 32, "pointerful live heap MB")
	rate := flag.Int("garbage", 64, "garbage MB/s")
	burst := flag.Duration("burst", 10*time.Millisecond, "worker burst")
	every := flag.Duration("every", 100*time.Millisecond, "worker period")
	calibrate := flag.Bool("calibrate", false, "print spin iterations per microsecond")
	watch := flag.Bool("watch", false, "print GOMAXPROCS every 250ms until stdin closes")
	flag.Parse()

	if *calibrate {
		best := 0.0
		for r := 0; r < 20; r++ {
			t0 := nanotime()
			sink += spin(1_000_000)
			d := float64(nanotime()-t0) / 1000
			if v := 1_000_000 / d; v > best {
				best = v
			}
		}
		fmt.Printf("%.1f\n", best)
		return
	}
	if *watch {
		go func() {
			b := make([]byte, 1)
			os.Stdin.Read(b)
			os.Exit(0)
		}()
		t0 := time.Now()
		for {
			fmt.Printf("WATCH t=%.2f gomaxprocs=%d\n", time.Since(t0).Seconds(), runtime.GOMAXPROCS(0))
			time.Sleep(250 * time.Millisecond)
		}
	}

	stop := make(chan struct{})
	bgDone := make(chan struct{}, 2)
	if *liveMB > 0 {
		live := buildLive(*liveMB)
		go func() { garbage(live, *rate, stop); bgDone <- struct{}{} }()
	}
	if *burst > 0 {
		go func() { worker(*burst, *every, 1000, stop); bgDone <- struct{}{} }()
	}
	runtime.GC()

	names := []string{
		"/gc/cycles/total:gc-cycles",
		"/cpu/classes/gc/total:cpu-seconds",
		"/cpu/classes/total:cpu-seconds",
		"/sched/pauses/total/gc:seconds",
		"/sched/latencies:seconds",
	}
	before := make([]metrics.Sample, len(names))
	after := make([]metrics.Sample, len(names))
	for i, n := range names {
		before[i].Name = n
		after[i].Name = n
	}
	metrics.Read(before)
	g0, w0 := garbageBytes.Load(), workerSpins.Load()
	var ru0, ru1 syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &ru0)

	stats := make([]*loopStats, *loops)
	done := make(chan struct{}, *loops)
	for i := 0; i < *loops; i++ {
		stats[i] = &loopStats{}
		go runLoop(3+i, *iters, *lock, stats[i], done)
	}
	ready := os.NewFile(uintptr(3+*loops), "ready")
	ready.Write([]byte{1})
	ready.Close()
	t0 := nanotime()
	for i := 0; i < *loops; i++ {
		<-done
	}
	wall := float64(nanotime()-t0) / 1e9
	syscall.Getrusage(syscall.RUSAGE_SELF, &ru1)
	metrics.Read(after)
	g1, w1 := garbageBytes.Load(), workerSpins.Load()
	close(stop)

	var hist [histMaxUS + 1]uint64
	var ops uint64
	for _, s := range stats {
		for i, c := range s.hist {
			hist[i] += uint64(c)
		}
		ops += s.ops
	}
	pct := func(q float64) int {
		want := uint64(q * float64(ops))
		var cum uint64
		for i, c := range hist {
			cum += c
			if cum > want {
				return i
			}
		}
		return histMaxUS
	}
	maxUS := 0
	for i, c := range hist {
		if c > 0 {
			maxUS = i
		}
	}
	tv := func(t syscall.Timeval) float64 { return float64(t.Sec) + float64(t.Usec)/1e6 }
	cpu := tv(ru1.Utime) - tv(ru0.Utime) + tv(ru1.Stime) - tv(ru0.Stime)
	gcCycles := after[0].Value.Uint64() - before[0].Value.Uint64()
	gcCPU := after[1].Value.Float64() - before[1].Value.Float64()
	pauses := after[3].Value.Float64Histogram()
	pb := before[3].Value.Float64Histogram()
	ph := &metrics.Float64Histogram{Buckets: pauses.Buckets, Counts: make([]uint64, len(pauses.Counts))}
	for i := range pauses.Counts {
		ph.Counts[i] = pauses.Counts[i] - pb.Counts[i]
	}
	sl := after[4].Value.Float64Histogram()
	sb := before[4].Value.Float64Histogram()
	sh := &metrics.Float64Histogram{Buckets: sl.Buckets, Counts: make([]uint64, len(sl.Counts))}
	for i := range sl.Counts {
		sh.Counts[i] = sl.Counts[i] - sb.Counts[i]
	}
	fmt.Printf("RESULT loops=%d gomaxprocs=%d lock=%v ops=%d wall_s=%.2f ops_per_s=%.0f cpu_s=%.3f cores=%.2f cpu_us_per_op=%.2f "+
		"lat_p50_us=%d lat_p99_us=%d lat_p999_us=%d lat_max_us=%d gc_cycles=%d gc_cpu_s=%.3f gc_pause_p99_us=%.0f gc_pause_max_us=%.0f sched_lat_p99_us=%.0f garbage_mb_per_s=%.1f worker_miters_per_s=%.1f\n",
		*loops, runtime.GOMAXPROCS(0), *lock, ops, wall, float64(ops)/wall, cpu, cpu/wall, cpu*1e6/float64(ops),
		pct(0.5), pct(0.99), pct(0.999), maxUS,
		gcCycles, gcCPU, histPct(ph, 0.99)*1e6, histMax(ph)*1e6, histPct(sh, 0.99)*1e6, float64(g1-g0)/(1<<20)/wall, float64(w1-w0)/1e6/wall)
}
