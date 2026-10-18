// Go port of spikes/parallel-cleaner: the same in-memory disk log, user writes and greedy cleaner,
// with the cleaner inline on the owner, on worker goroutines that send their moves to the owner, or
// on workers that update a shared atomic index. It prints the same RUN line as the Rust harness.
//
// usage: cleaner-go key=value ...   (mode, workers, u, segs, seg_blocks, free_low, reserve, warm,
// measure, rate, seed, tag)
package main

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math/bits"
	"os"
	"runtime"
	"runtime/pprof"
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
	block          = 4096
	invalid        = ^uint32(0)
	victimsPerPass = 4
)

const (
	stateFree uint8 = iota
	stateOpen
	stateSealed
	stateCleaning
)

type mode int

const (
	modeInline mode = iota
	modeOwner
	modeShared
)

var modeNames = map[string]mode{"inline": modeInline, "owner": modeOwner, "shared": modeShared}

type cfg struct {
	mode                      mode
	modeName                  string
	workers, freeLow, reserve int
	u, warm, measure, rate    float64
	crc                       bool
	segs, segBlocks           uint32
	seed                      uint64
	tag                       string
}

func parse() cfg {
	kv := map[string]string{}
	for _, a := range os.Args[1:] {
		k, v, ok := strings.Cut(a, "=")
		if !ok {
			panic("bad arg " + a)
		}
		kv[k] = v
	}
	get := func(k, d string) string {
		if v, ok := kv[k]; ok {
			return v
		}
		return d
	}
	f := func(k, d string) float64 { x, err := strconv.ParseFloat(get(k, d), 64); must(err); return x }
	i := func(k, d string) int { x, err := strconv.Atoi(get(k, d)); must(err); return x }
	c := cfg{modeName: get("mode", "inline"), u: f("u", "0.9"), warm: f("warm", "4"), measure: f("measure", "2"),
		rate: f("rate", "0"), segs: uint32(i("segs", "4096")), segBlocks: uint32(i("seg_blocks", "64")),
		freeLow: i("free_low", "8"), crc: get("crc", "1") == "1", seed: uint64(i("seed", "1")), tag: get("tag", "-")}
	c.mode = modeNames[c.modeName]
	if c.mode != modeInline {
		c.workers = i("workers", "1")
	}
	c.reserve = i("reserve", strconv.Itoa(2+c.workers))
	return c
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func crc32c(b []byte) uint32 { return crc32.Checksum(b, castagnoli) }

func threadCPU() time.Duration {
	var ts syscall.Timespec
	const clockThreadCPUTimeID = 3
	syscall.Syscall(syscall.SYS_CLOCK_GETTIME, clockThreadCPUTimeID, uintptr(unsafe.Pointer(&ts)), 0)
	return time.Duration(ts.Nano())
}

func processCPU() time.Duration {
	var ru syscall.Rusage
	must(syscall.Getrusage(syscall.RUSAGE_SELF, &ru))
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

type move struct{ l, p, q uint32 }

type batch struct {
	victim uint32
	moves  []move
}

type log struct {
	c               cfg
	disk            []byte
	crc, rev, index []atomic.Uint32
	live, pending   []atomic.Uint32
	state           []atomic.Uint32
	freeMu          sync.Mutex
	free            []uint32
	freeN           atomic.Int64
	picker          sync.Mutex
	stop            atomic.Bool
	copies, wasted  atomic.Int64
	victims         atomic.Int64
}

func newLog(c cfg, lbas int) *log {
	blocks := int(c.segs) * int(c.segBlocks)
	l := &log{c: c, disk: make([]byte, blocks*block), crc: make([]atomic.Uint32, blocks),
		rev: make([]atomic.Uint32, blocks), index: make([]atomic.Uint32, lbas), live: make([]atomic.Uint32, c.segs),
		pending: make([]atomic.Uint32, c.segs), state: make([]atomic.Uint32, c.segs)}
	for i := range l.rev {
		l.rev[i].Store(invalid)
	}
	for i := range l.index {
		l.index[i].Store(invalid)
	}
	for s := int(c.segs) - 1; s >= 0; s-- {
		l.free = append(l.free, uint32(s))
	}
	l.freeN.Store(int64(c.segs))
	return l
}

func (l *log) blk(p uint32) []byte { return l.disk[int(p)*block : int(p+1)*block] }
func (l *log) seg(p uint32) uint32 { return p / l.c.segBlocks }

func (l *log) takeFree(keep int) (uint32, bool) {
	l.freeMu.Lock()
	defer l.freeMu.Unlock()
	if len(l.free) <= keep {
		return 0, false
	}
	s := l.free[len(l.free)-1]
	l.free = l.free[:len(l.free)-1]
	l.freeN.Store(int64(len(l.free)))
	l.state[s].Store(uint32(stateOpen))
	return s, true
}

func (l *log) release(v uint32) {
	l.live[v].Store(0)
	l.state[v].Store(uint32(stateFree))
	l.freeMu.Lock()
	l.free = append(l.free, v)
	l.freeN.Store(int64(len(l.free)))
	l.freeMu.Unlock()
}

func (l *log) pick() (uint32, bool) {
	l.picker.Lock()
	defer l.picker.Unlock()
	best, bestLive := invalid, ^uint32(0)
	for s := range l.c.segs {
		if l.state[s].Load() != uint32(stateSealed) || l.pending[s].Load() != 0 {
			continue
		}
		if lv := l.live[s].Load(); lv < bestLive {
			best, bestLive = s, lv
		}
	}
	if best == invalid || bestLive >= l.c.segBlocks {
		return 0, false
	}
	l.state[best].Store(uint32(stateCleaning))
	return best, true
}

type cursor struct{ seg, next uint32 }

func (l *log) cleanVictim(v uint32, cold *cursor, m mode) *batch {
	sb := l.c.segBlocks
	var moves []move
	for i := range sb {
		p := v*sb + i
		lb := l.rev[p].Load()
		if lb == invalid || l.index[lb].Load() != p {
			continue
		}
		src := l.blk(p)
		c := l.crc[p].Load()
		if l.c.crc && crc32c(src) != c {
			panic(fmt.Sprintf("CRC mismatch at block %d", p))
		}
		if cold.seg == invalid || cold.next == sb {
			if cold.seg != invalid {
				l.state[cold.seg].Store(uint32(stateSealed))
			}
			for {
				if s, ok := l.takeFree(0); ok {
					cold.seg = s
					break
				}
				runtime.Gosched()
			}
			cold.next = 0
		}
		q := cold.seg*sb + cold.next
		cold.next++
		copy(l.blk(q), src)
		l.crc[q].Store(c)
		l.rev[q].Store(lb)
		qs := l.seg(q)
		switch m {
		case modeInline:
			l.index[lb].Store(q)
			l.live[qs].Add(1)
			l.live[v].Add(^uint32(0))
			l.copies.Add(1)
		case modeShared:
			l.live[qs].Add(1)
			if l.index[lb].CompareAndSwap(p, q) {
				l.live[v].Add(^uint32(0))
				l.copies.Add(1)
			} else {
				l.live[qs].Add(^uint32(0))
				l.wasted.Add(1)
			}
		case modeOwner:
			l.pending[qs].Add(1)
			moves = append(moves, move{lb, p, q})
		}
	}
	l.victims.Add(1)
	if m == modeOwner {
		return &batch{victim: v, moves: moves}
	}
	l.release(v)
	return nil
}

func (l *log) apply(b *batch) {
	for _, mv := range b.moves {
		if l.index[mv.l].Load() == mv.p {
			l.index[mv.l].Store(mv.q)
			l.live[l.seg(mv.q)].Add(1)
			l.live[b.victim].Add(^uint32(0))
			l.copies.Add(1)
		} else {
			l.wasted.Add(1)
		}
		l.pending[l.seg(mv.q)].Add(^uint32(0))
	}
	l.release(b.victim)
}

func (l *log) worker(out chan<- *batch, wake <-chan struct{}, done *sync.WaitGroup) {
	defer done.Done()
	cold := cursor{seg: invalid}
	t := time.NewTimer(time.Hour)
	for !l.stop.Load() {
		if l.freeN.Load() < int64(l.c.freeLow) {
			if v, ok := l.pick(); ok {
				if b := l.cleanVictim(v, &cold, l.c.mode); b != nil {
					out <- b
				}
				continue
			}
		}
		t.Reset(200 * time.Microsecond)
		select {
		case <-wake:
		case <-t.C:
		}
	}
}

type owner struct {
	l         *log
	hot, cold cursor
	in        chan *batch
	wake      []chan struct{}
	src       []byte
	stall     time.Duration
}

func (o *owner) drain() {
	for {
		select {
		case b := <-o.in:
			o.l.apply(b)
		default:
			return
		}
	}
}

func (o *owner) wakeAll() {
	for _, w := range o.wake {
		select {
		case w <- struct{}{}:
		default:
		}
	}
}

func (o *owner) cleanInline() {
	for o.l.freeN.Load() < int64(o.l.c.freeLow) {
		any := false
		for range victimsPerPass {
			if v, ok := o.l.pick(); ok {
				o.l.cleanVictim(v, &o.cold, modeInline)
				any = true
			}
		}
		if !any {
			return
		}
	}
}

func (o *owner) nextHot() {
	l := o.l
	if o.hot.seg != invalid {
		l.state[o.hot.seg].Store(uint32(stateSealed))
	}
	if l.c.mode == modeInline {
		o.cleanInline()
	} else if l.freeN.Load() < int64(l.c.freeLow) {
		o.wakeAll()
	}
	t0 := time.Now()
	stalled := false
	for {
		if s, ok := l.takeFree(l.c.reserve); ok {
			o.hot.seg = s
			break
		}
		stalled = true
		o.drain()
		o.wakeAll()
		runtime.Gosched()
	}
	if stalled {
		o.stall += time.Since(t0)
	}
	o.hot.next = 0
}

func (o *owner) write(lb uint32, seq uint64) {
	l := o.l
	o.drain()
	if o.hot.seg == invalid || o.hot.next == l.c.segBlocks {
		o.nextHot()
	}
	p := o.hot.seg*l.c.segBlocks + o.hot.next
	o.hot.next++
	binary.LittleEndian.PutUint64(o.src, uint64(lb))
	binary.LittleEndian.PutUint64(o.src[8:], seq)
	dst := l.blk(p)
	copy(dst, o.src)
	if l.c.crc {
		l.crc[p].Store(crc32c(dst))
	}
	l.rev[p].Store(lb)
	l.live[l.seg(p)].Add(1)
	if old := l.index[lb].Swap(p); old != invalid {
		l.live[l.seg(old)].Add(^uint32(0))
	}
}

type rng uint64

func (r *rng) below(n uint64) uint64 {
	*r += 0x9E3779B97F4A7C15
	z := uint64(*r)
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	z ^= z >> 31
	hi, _ := bits.Mul64(z, n)
	return hi
}

func main() {
	c := parse()
	runtime.LockOSThread()
	blocks := int(c.segs) * int(c.segBlocks)
	lbas := int(c.u * float64(blocks))
	warm := int(c.warm * float64(blocks))
	n := int(c.measure * float64(blocks))
	l := newLog(c, lbas)
	in := make(chan *batch, 64)
	o := &owner{l: l, hot: cursor{seg: invalid}, cold: cursor{seg: invalid}, in: in, src: make([]byte, block)}
	for i := range o.src {
		o.src[i] = 0x5a
	}
	var wg sync.WaitGroup
	for range c.workers {
		w := make(chan struct{}, 1)
		o.wake = append(o.wake, w)
		wg.Add(1)
		go l.worker(in, w, &wg)
	}
	r := rng(c.seed)
	var seq uint64
	for lb := range uint32(lbas) {
		o.write(lb, seq)
		seq++
	}
	for range warm {
		o.write(uint32(r.below(uint64(lbas))), seq)
		seq++
	}
	l.copies.Store(0)
	l.wasted.Store(0)
	l.victims.Store(0)
	o.stall = 0
	o.wakeAll()
	lat := make([]time.Duration, n)
	var period time.Duration
	if c.rate > 0 {
		period = time.Duration(1e9 / c.rate)
	}
	if path := os.Getenv("CPUPROFILE"); path != "" {
		f, err := os.Create(path)
		must(err)
		must(pprof.StartCPUProfile(f))
		defer pprof.StopCPUProfile()
	}
	cpu0, pcpu0 := threadCPU(), processCPU()
	t0 := time.Now()
	var busy time.Duration
	for i := range n {
		if period > 0 {
			for time.Since(t0) < time.Duration(i)*period {
			}
		}
		s := time.Now()
		o.write(uint32(r.below(uint64(lbas))), seq)
		d := time.Since(s)
		busy += d
		lat[i] = d
		seq++
	}
	secs := time.Since(t0).Seconds()
	ownerCPU := threadCPU() - cpu0
	otherCPU := processCPU() - pcpu0 - ownerCPU
	l.stop.Store(true)
	o.wakeAll()
	go func() {
		for range in {
		}
	}()
	wg.Wait()
	report(c, lat, secs, ownerCPU, otherCPU, busy, o.stall, n, l)
}

func report(c cfg, lat []time.Duration, secs float64, ownerCPU, otherCPU, busy, stall time.Duration, n int, l *log) {
	nf := float64(n)
	slices.Sort(lat)
	p := func(q float64) float64 {
		return float64(lat[min(len(lat)-1, int(float64(len(lat))*q))].Nanoseconds()) / 1000
	}
	copies, wasted := float64(l.copies.Load())/nf, float64(l.wasted.Load())/nf
	wcpu := float64(otherCPU.Nanoseconds()) / nf
	ocpu := float64(ownerCPU.Nanoseconds()) / nf
	fmt.Printf("RUN tag=%s lang=go mode=%s workers=%d u=%.2f rate=%g segs=%d seg_blocks=%d | writes_per_s=%.0f "+
		"owner_busy_ns=%.0f owner_work_ns=%.0f owner_cpu_ns=%.0f worker_cpu_ns=%.0f total_cpu_ns=%.0f stall_ns=%.0f "+
		"copies_per_user=%.3f wasted_per_user=%.4f wa=%.3f lat_p50_us=%.2f lat_p99_us=%.2f lat_p999_us=%.2f "+
		"lat_max_us=%.1f secs=%.2f\n", c.tag, c.modeName, c.workers, c.u, c.rate, c.segs, c.segBlocks, nf/secs,
		float64(busy.Nanoseconds())/nf, float64((busy-stall).Nanoseconds())/nf, ocpu, wcpu, ocpu+wcpu,
		float64(stall.Nanoseconds())/nf, copies, wasted, 1+copies+wasted, p(0.5), p(0.99), p(0.999),
		float64(lat[len(lat)-1].Nanoseconds())/1000, secs)
}
