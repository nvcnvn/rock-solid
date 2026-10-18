// Command logpercore measures what owning a disk's log from one event loop costs against one log per loop:
// device flushes, items per second, latency and CPU per item, and the cost of an inbox hop between loops.
package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

const (
	block     = 4096
	ringSize  = 4096
	efdNonblk = 0x800
)

// ring is a single-producer single-consumer queue of uint64; the producer writes the consumer's eventfd only when the ring was empty.
type ring struct {
	head atomic.Uint64
	_    [56]byte
	tail atomic.Uint64
	_    [56]byte
	buf  [ringSize]uint64
}

func (r *ring) push(v uint64) (wasEmpty bool) {
	t := r.tail.Load()
	for t-r.head.Load() >= ringSize {
		runtime.Gosched()
	}
	r.buf[t%ringSize] = v
	r.tail.Store(t + 1)
	return t == r.head.Load()
}

func (r *ring) drain(f func(uint64)) int {
	h, t := r.head.Load(), r.tail.Load()
	for i := h; i < t; i++ {
		f(r.buf[i%ringSize])
	}
	r.head.Store(t)
	return int(t - h)
}

// waker is a loop's epoll set holding one eventfd.
type waker struct{ ep, efd int }

func newWaker() *waker {
	efd, _, e := syscall.RawSyscall(syscall.SYS_EVENTFD2, 0, efdNonblk|syscall.O_CLOEXEC, 0)
	if e != 0 {
		panic(e)
	}
	ep, err := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	if err != nil {
		panic(err)
	}
	ev := syscall.EpollEvent{Events: syscall.EPOLLIN, Fd: int32(efd)}
	if err := syscall.EpollCtl(ep, syscall.EPOLL_CTL_ADD, int(efd), &ev); err != nil {
		panic(err)
	}
	return &waker{ep: ep, efd: int(efd)}
}

var one = [8]byte{1}

func (w *waker) wake() {
	syscall.RawSyscall(syscall.SYS_WRITE, uintptr(w.efd), uintptr(unsafe.Pointer(&one[0])), 8)
	wakes.Add(1)
}

func (w *waker) wait(stop *atomic.Bool) {
	var evs [1]syscall.EpollEvent
	var b [8]byte
	for !stop.Load() {
		n, err := syscall.EpollWait(w.ep, evs[:], 50)
		if err == syscall.EINTR {
			continue
		}
		if n > 0 {
			syscall.RawSyscall(syscall.SYS_READ, uintptr(w.efd), uintptr(unsafe.Pointer(&b[0])), 8)
			return
		}
	}
}

var wakes atomic.Uint64

type stats struct {
	items   uint64
	lat     []int64
	records uint64
}

func cpuNow() (time.Duration, int64) {
	var ru syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano()), ru.Nvcsw + ru.Nivcsw
}

func devFlushes(dev string) uint64 {
	b, err := os.ReadFile("/sys/block/" + dev + "/stat")
	if err != nil {
		return 0
	}
	f := strings.Fields(string(b))
	if len(f) < 16 {
		return 0
	}
	v, _ := strconv.ParseUint(f[15], 10, 64)
	return v
}

func alignedBuf(n int) []byte {
	b, err := syscall.Mmap(-1, 0, n, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
	if err != nil {
		panic(err)
	}
	for i := range b {
		b[i] = byte(i * 7)
	}
	return b
}

// prepare creates the file and writes it in full once, so fdatasync never commits the filesystem journal.
func prepare(path string, size int64) {
	if st, err := os.Stat(path); err == nil && st.Size() == size {
		return
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		panic(err)
	}
	buf := alignedBuf(1 << 20)
	for off := int64(0); off < size; off += int64(len(buf)) {
		if _, err := f.WriteAt(buf, off); err != nil {
			panic(err)
		}
	}
	f.Sync()
	f.Close()
}

type region struct {
	fd       int
	base     int64
	size     int64
	off      int64
	buf      []byte
	maxItems int
}

func (r *region) commit(n int) {
	length := int64(n * block)
	if r.off+length > r.size {
		r.off = 0
	}
	if _, err := syscall.Pwrite(r.fd, r.buf[:length], r.base+r.off); err != nil {
		panic(err)
	}
	r.off += length
	if err := syscall.Fdatasync(r.fd); err != nil {
		panic(err)
	}
}

// runPerLoop: each of k loops group-commits its own depth items into its own region with its own flush.
func runPerLoop(fd int, fileSize int64, k, depth int, dur time.Duration) []stats {
	var stop atomic.Bool
	var wg sync.WaitGroup
	out := make([]stats, k)
	for i := range k {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			reg := &region{fd: fd, base: int64(i) * (fileSize / int64(k)), size: fileSize / int64(k), buf: alignedBuf(depth * block)}
			st := &out[i]
			for !stop.Load() {
				t0 := time.Now()
				reg.commit(depth)
				l := time.Since(t0).Nanoseconds()
				for range depth {
					st.lat = append(st.lat, l)
				}
				st.items += uint64(depth)
				st.records++
			}
		}(i)
	}
	time.Sleep(dur)
	stop.Store(true)
	wg.Wait()
	return out
}

// runOwner: k producer loops hand items through their inbox ring to one owner loop, which commits
// everything queued in one record and one flush per round and hands completions back.
func runOwner(fd int, fileSize int64, k, depth int, dur time.Duration, maxItems int) []stats {
	var stop atomic.Bool
	var wg sync.WaitGroup
	owner := newWaker()
	toOwner := make([]*ring, k)
	toProd := make([]*ring, k)
	prodW := make([]*waker, k)
	for i := range k {
		toOwner[i], toProd[i], prodW[i] = new(ring), new(ring), newWaker()
	}
	out := make([]stats, k+1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		reg := &region{fd: fd, size: fileSize, buf: alignedBuf(maxItems * block)}
		type pend struct {
			p  int
			ts uint64
		}
		batch := make([]pend, 0, k*depth)
		st := &out[k]
		for !stop.Load() {
			for p := range k {
				toOwner[p].drain(func(v uint64) { batch = append(batch, pend{p, v}) })
			}
			if len(batch) == 0 {
				owner.wait(&stop)
				continue
			}
			for len(batch) > 0 {
				n := min(len(batch), maxItems)
				reg.commit(n)
				st.records++
				var wakeP [64]bool
				for _, it := range batch[:n] {
					if toProd[it.p].push(it.ts) {
						wakeP[it.p] = true
					}
				}
				for p := range k {
					if wakeP[p] {
						prodW[p].wake()
					}
				}
				batch = batch[n:]
			}
			batch = batch[:0]
		}
	}()
	base := time.Now()
	for i := range k {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			st := &out[i]
			outstanding := 0
			for !stop.Load() {
				wake := false
				for ; outstanding < depth; outstanding++ {
					if toOwner[i].push(uint64(time.Since(base).Nanoseconds())) {
						wake = true
					}
				}
				if wake {
					owner.wake()
				}
				done := toProd[i].drain(func(v uint64) {
					st.lat = append(st.lat, time.Since(base).Nanoseconds()-int64(v))
				})
				if done == 0 {
					prodW[i].wait(&stop)
					continue
				}
				st.items += uint64(done)
				outstanding -= done
			}
		}(i)
	}
	time.Sleep(dur)
	stop.Store(true)
	wg.Wait()
	return out
}

// runHop: loop A sends batch items to loop B's inbox and waits for B to hand them back; each round trip is two hops.
func runHop(batch int, dur time.Duration) (rt []int64, trips uint64) {
	var stop atomic.Bool
	var wg sync.WaitGroup
	wa, wb := newWaker(), newWaker()
	ab, ba := new(ring), new(ring)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			n := 0
			wake := false
			n = ab.drain(func(v uint64) {
				if ba.push(v) {
					wake = true
				}
			})
			if wake {
				wa.wake()
			}
			if n == 0 {
				wb.wait(&stop)
			}
		}
	}()
	base := time.Now()
	for !stop.Load() {
		t0 := time.Since(base).Nanoseconds()
		wake := false
		for range batch {
			if ab.push(uint64(t0)) {
				wake = true
			}
		}
		if wake {
			wb.wake()
		}
		got := 0
		for got < batch {
			n := ba.drain(func(uint64) {})
			if n == 0 {
				wa.wait(&stop)
				if stop.Load() {
					break
				}
			}
			got += n
		}
		rt = append(rt, time.Since(base).Nanoseconds()-t0)
		trips++
		if trips%1024 == 0 && time.Since(base) > dur {
			stop.Store(true)
			wb.wake()
		}
	}
	wg.Wait()
	return rt, trips
}

func pct(l []int64, p float64) float64 {
	if len(l) == 0 {
		return 0
	}
	slices.Sort(l)
	return float64(l[int(float64(len(l)-1)*p)]) / 1e3
}

func main() {
	mode := flag.String("mode", "perloop", "perloop | owner | hop")
	k := flag.Int("k", 1, "loops (perloop) or producer loops (owner)")
	depth := flag.Int("depth", 8, "items outstanding per loop")
	batch := flag.Int("batch", 1, "items per round trip (hop)")
	dur := flag.Duration("dur", 3*time.Second, "duration")
	path := flag.String("file", "work/log.dat", "file")
	dev := flag.String("dev", "vda", "block device for flush counts")
	maxItems := flag.Int("max", 256, "items per record (owner), 256 = 1 MiB")
	flag.Parse()

	switch *mode {
	case "hop":
		c0, cs0 := cpuNow()
		w0 := wakes.Load()
		rt, trips := runHop(*batch, *dur)
		c1, cs1 := cpuNow()
		fmt.Printf("hop batch=%d trips=%d rt_p50=%.1fus rt_p99=%.1fus cpu/trip=%.2fus cpu/item/hop=%.3fus ctxsw/trip=%.2f eventfd_writes/trip=%.2f\n",
			*batch, trips, pct(rt, .5), pct(rt, .99), float64((c1-c0).Microseconds())/float64(trips),
			float64((c1-c0).Microseconds())/float64(trips)/float64(2**batch), float64(cs1-cs0)/float64(trips), float64(wakes.Load()-w0)/float64(trips))
		return
	}
	const fileSize = 1 << 30
	prepare(*path, fileSize)
	fd, err := syscall.Open(*path, syscall.O_RDWR|syscall.O_DIRECT, 0)
	if err != nil {
		panic(err)
	}
	defer syscall.Close(fd)
	c0, cs0 := cpuNow()
	w0 := wakes.Load()
	f0 := devFlushes(*dev)
	t0 := time.Now()
	var st []stats
	if *mode == "perloop" {
		st = runPerLoop(fd, fileSize, *k, *depth, *dur)
	} else {
		st = runOwner(fd, fileSize, *k, *depth, *dur, *maxItems)
	}
	el := time.Since(t0).Seconds()
	c1, cs1 := cpuNow()
	f1 := devFlushes(*dev)
	var items, records uint64
	var lat []int64
	for _, s := range st {
		items += s.items
		records += s.records
		lat = append(lat, s.lat...)
	}
	fmt.Printf("%s k=%d depth=%d items/s=%.0f records/s=%.0f items/record=%.1f p50=%.0fus p99=%.0fus cpu/item=%.2fus ctxsw/item=%.3f dev_flushes/record=%.2f eventfd_writes/record=%.2f\n",
		*mode, *k, *depth, float64(items)/el, float64(records)/el, float64(items)/float64(max(records, 1)),
		pct(lat, .5), pct(lat, .99), float64((c1-c0).Microseconds())/float64(items), float64(cs1-cs0)/float64(items),
		float64(f1-f0)/float64(max(records, 1)), float64(wakes.Load()-w0)/float64(max(records, 1)))
}
