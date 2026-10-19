// Command manifestsim measures how many segment usage rows change between
// checkpoints, to compare a full usage table in every manifest with
// incremental edits plus periodic snapshots (Pebble's RotationHelper rule and
// RocksDB's max_manifest_space_amp_pct rule).
//
// The disk is modelled at segment granularity: each segment holds live block
// counts per temperature class. A user overwrite kills one live block chosen
// uniformly among all live blocks of its class (a Fenwick tree samples a
// segment in proportion to its live count) and appends the new block to the
// hot open segment. A greedy cleaner keeps a reserve of free segments by moving
// the live blocks of the least-live used segment to the cold open segment.
// Every MaxReplay bytes of log writes (user and cleaner) is a checkpoint.
package main

import (
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"sort"
)

const (
	blockSize = 4096
	rowBytes  = 17
	reserve   = 8
)

type fenwick []int64

func (f fenwick) add(i int, d int64) {
	for i++; i < len(f); i += i & -i {
		f[i] += d
	}
}

func (f fenwick) total() int64 {
	var s int64
	for i := len(f) - 1; i > 0; i -= i & -i {
		s += f[i]
	}
	return s
}

// find returns the index whose prefix range covers target (0 <= target < total).
func (f fenwick) find(target int64) int {
	pos := 0
	step := 1
	for step*2 < len(f) {
		step *= 2
	}
	for ; step > 0; step /= 2 {
		if pos+step < len(f) && f[pos+step] <= target {
			pos += step
			target -= f[pos]
		}
	}
	return pos
}

const (
	stFree = iota
	stOpen
	stUsed
	stVictim
)

type sim struct {
	S, B    int
	classes int
	live    [2][]int32
	fw      [2]fenwick
	state   []uint8
	written []int32
	free    []int
	hot     int
	cold    int
	victim  int
	quota   float64
	dirty   []bool
	nDirty  int
	used    int // segments not free

	// greedy buckets by total live, used segments only
	head       []int
	prev, next []int
	inBucket   []bool
	lo         int

	logBlocks  int64
	userBlocks int64
	cleanCopy  int64
	sinceCP    int64
	cpEvery    int64
	onCP       func(rows, delta int)
	rng        *rand.Rand
}

func newSim(S, B, classes int, seed uint64, cpEvery int64) *sim {
	s := &sim{S: S, B: B, classes: classes, cpEvery: cpEvery}
	for c := range classes {
		s.live[c] = make([]int32, S)
		s.fw[c] = make(fenwick, S+1)
	}
	s.state = make([]uint8, S)
	s.written = make([]int32, S)
	s.dirty = make([]bool, S)
	s.head = make([]int, B+1)
	for i := range s.head {
		s.head[i] = -1
	}
	s.prev = make([]int, S)
	s.next = make([]int, S)
	s.inBucket = make([]bool, S)
	for i := S - 1; i >= 0; i-- {
		s.free = append(s.free, i)
	}
	s.rng = rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	s.hot = s.open()
	s.cold = s.open()
	s.victim = -1
	return s
}

func (s *sim) mark(seg int) {
	if !s.dirty[seg] {
		s.dirty[seg] = true
		s.nDirty++
	}
}

func (s *sim) totalLive(seg int) int {
	n := 0
	for c := range s.classes {
		n += int(s.live[c][seg])
	}
	return n
}

func (s *sim) bucketRemove(seg int) {
	if !s.inBucket[seg] {
		return
	}
	l := s.totalLive(seg)
	if s.prev[seg] >= 0 {
		s.next[s.prev[seg]] = s.next[seg]
	} else {
		s.head[l] = s.next[seg]
	}
	if s.next[seg] >= 0 {
		s.prev[s.next[seg]] = s.prev[seg]
	}
	s.inBucket[seg] = false
}

func (s *sim) bucketInsert(seg int) {
	l := s.totalLive(seg)
	s.prev[seg] = -1
	s.next[seg] = s.head[l]
	if s.head[l] >= 0 {
		s.prev[s.head[l]] = seg
	}
	s.head[l] = seg
	s.inBucket[seg] = true
	if l < s.lo {
		s.lo = l
	}
}

func (s *sim) open() int {
	if len(s.free) == 0 {
		panic("out of segments")
	}
	seg := s.free[len(s.free)-1]
	s.free = s.free[:len(s.free)-1]
	s.state[seg] = stOpen
	s.written[seg] = 0
	s.used++
	s.mark(seg)
	return seg
}

func (s *sim) seal(seg int) {
	s.state[seg] = stUsed
	s.bucketInsert(seg)
}

func (s *sim) logWrite(n int64) {
	s.logBlocks += n
	s.sinceCP += n
	for s.sinceCP >= s.cpEvery {
		s.sinceCP -= s.cpEvery
		s.checkpoint()
	}
}

func (s *sim) checkpoint() {
	if s.onCP != nil {
		s.onCP(s.used, s.nDirty)
	}
	for i := range s.dirty {
		s.dirty[i] = false
	}
	s.nDirty = 0
}

// appendTo adds one block of class c to the open segment *seg, rolling it.
func (s *sim) appendTo(seg *int, c int) {
	g := *seg
	s.live[c][g]++
	s.fw[c].add(g, 1)
	s.written[g]++
	s.mark(g)
	if int(s.written[g]) == s.B {
		s.seal(g)
		*seg = s.open()
	}
}

func (s *sim) kill(c int) int {
	t := s.fw[c].total()
	seg := s.fw[c].find(s.rng.Int64N(t))
	s.killAt(seg, c, 1)
	return seg
}

func (s *sim) killAt(seg, c, n int) {
	if s.state[seg] == stUsed {
		s.bucketRemove(seg)
	}
	s.live[c][seg] -= int32(n)
	s.fw[c].add(seg, -int64(n))
	if s.state[seg] == stUsed {
		s.bucketInsert(seg)
	}
	s.mark(seg)
}

func (s *sim) pickVictim() int {
	for s.lo <= s.B && s.head[s.lo] < 0 {
		s.lo++
	}
	if s.lo >= s.B {
		panic("cleaner cannot free a segment")
	}
	v := s.head[s.lo]
	s.bucketRemove(v)
	s.state[v] = stVictim
	return v
}

// copyOne moves one live block of the current victim to the cold stream and
// frees the victim once it is empty.
func (s *sim) copyOne() {
	v := s.victim
	if s.totalLive(v) > 0 {
		c := 0
		if s.classes == 2 && (s.live[0][v] == 0 || (s.live[1][v] > 0 && s.rng.Int32N(s.live[0][v]+s.live[1][v]) >= s.live[0][v])) {
			c = 1
		}
		s.live[c][v]--
		s.fw[c].add(v, -1)
		s.appendTo(&s.cold, c)
		s.cleanCopy++
		s.logWrite(1)
	}
	if s.totalLive(v) == 0 {
		s.state[v] = stFree
		s.used--
		s.mark(v)
		s.free = append(s.free, v)
		s.victim = -1
	}
}

// clean interleaves copying with user writes: each user block pays for est
// copied blocks, the copy rate seen so far, so checkpoints see the mix of user
// writes and cleaner copies they would in steady state. Below the reserve it
// copies without waiting.
func (s *sim) clean() {
	est := 4.0
	if s.userBlocks > 0 && s.cleanCopy > 0 {
		est = float64(s.cleanCopy) / float64(s.userBlocks)
	}
	s.quota += est
	for s.quota >= 1 {
		if s.victim < 0 {
			if len(s.free) >= reserve+2 {
				s.quota = 0
				break
			}
			s.victim = s.pickVictim()
		}
		s.copyOne()
		s.quota--
	}
	for len(s.free) < reserve {
		if s.victim < 0 {
			s.victim = s.pickVictim()
		}
		s.copyOne()
	}
}

// userWrite overwrites ext blocks of class c: ext blocks die where they lived
// (one extent: all in one segment when it holds that many) and ext new blocks append.
func (s *sim) userWrite(c, ext int) {
	left := ext
	for left > 0 {
		t := s.fw[c].total()
		seg := s.fw[c].find(s.rng.Int64N(t))
		n := min(left, int(s.live[c][seg]))
		s.killAt(seg, c, n)
		left -= n
	}
	for range ext {
		s.appendTo(&s.hot, c)
	}
	s.userBlocks += int64(ext)
	s.logWrite(int64(ext))
	s.clean()
}

// fill writes whole segments sequentially until frac of the disk is live,
// the first hotShare of them holding the hot class.
func (s *sim) fill(frac float64, hotShare float64) {
	segs := int(float64(s.S) * frac)
	for i := range segs {
		c := 0
		if s.classes == 2 && float64(i) < float64(segs)*hotShare {
			c = 1
		}
		g := s.hot
		s.live[c][g] = int32(s.B)
		s.fw[c].add(g, int64(s.B))
		s.written[g] = int32(s.B)
		s.seal(g)
		s.hot = s.open()
	}
	for i := range s.dirty {
		s.dirty[i] = false
	}
	s.nDirty = 0
}

type rotator struct {
	name      string
	ratio     float64 // rotate when edits since snapshot > ratio * min(last, next) (Pebble: 1)
	spaceAmp  float64 // or: rotate when chain > (1+spaceAmp) * next (RocksDB pct/100); 0 = unused
	lastSnap  int
	since     int
	started   bool
	bytes     int64
	maxChain  int
	snapshots int
}

func (r *rotator) cp(rows, delta int) {
	rotate := !r.started
	if r.started {
		if r.spaceAmp > 0 {
			rotate = float64(r.lastSnap+r.since+delta) > (1+r.spaceAmp)*float64(rows)
		} else {
			rotate = float64(r.since+delta) > r.ratio*float64(min(r.lastSnap, rows))
		}
	}
	if rotate {
		r.started = true
		r.lastSnap, r.since = rows, 0
		r.bytes += int64(rows) * rowBytes
		r.snapshots++
	} else {
		r.since += delta
		r.bytes += int64(delta) * rowBytes
	}
	if c := r.lastSnap + r.since; c > r.maxChain {
		r.maxChain = c
	}
}

func main() {
	diskGiB := flag.Int("disk", 1024, "disk size in GiB")
	segMiB := flag.Int("seg", 64, "segment size in MiB")
	fillF := flag.Float64("fill", 0.9, "fraction of the disk live")
	ext := flag.Int("ext", 1, "blocks per user write (one extent)")
	hc := flag.Bool("hotcold", false, "80% of writes to 20% of the data")
	warm := flag.Float64("warm", 1.0, "user writes before measuring, in disk capacities")
	meas := flag.Float64("meas", 0.25, "user writes measured, in disk capacities")
	replayMiB := flag.Int("replay", 64, "max_replay_bytes in MiB")
	seed := flag.Uint64("seed", 1, "seed")
	flag.Parse()

	B := *segMiB << 20 / blockSize
	S := int((int64(*diskGiB) << 30) / (int64(*segMiB) << 20))
	classes := 1
	if *hc {
		classes = 2
	}
	cpEvery := int64(*replayMiB) << 20 / blockSize
	s := newSim(S, B, classes, *seed, cpEvery)
	s.fill(*fillF, 0.2)

	capacity := int64(S) * int64(B)
	warmN := int64(*warm * float64(capacity) / float64(*ext))
	measN := int64(*meas * float64(capacity) / float64(*ext))

	pick := func() int {
		if classes == 2 && s.rng.Float64() < 0.8 {
			return 1
		}
		return 0
	}
	for range warmN {
		s.userWrite(pick(), *ext)
	}
	// measurement window
	var rowsSum, deltaSum int64
	var deltas []int
	cps := 0
	rots := []*rotator{
		{name: "pebble-ratio1", ratio: 1},
		{name: "rocksdb-amp500", spaceAmp: 5},
	}
	s.onCP = func(rows, delta int) {
		cps++
		rowsSum += int64(rows)
		deltaSum += int64(delta)
		deltas = append(deltas, delta)
		for _, r := range rots {
			r.cp(rows, delta)
		}
	}
	u0, c0 := s.userBlocks, s.cleanCopy
	for range measN {
		s.userWrite(pick(), *ext)
	}
	du, dc := s.userBlocks-u0, s.cleanCopy-c0
	sort.Ints(deltas)
	pct := func(p float64) int {
		if len(deltas) == 0 {
			return 0
		}
		return deltas[int(p*float64(len(deltas)-1))]
	}
	cpBytes := float64(cpEvery * blockSize)
	avgRows := float64(rowsSum) / float64(cps)
	avgDelta := float64(deltaSum) / float64(cps)
	wl := "uniform"
	if *hc {
		wl = "hotcold80/20"
	}
	fmt.Printf("disk=%dGiB seg=%dMiB segments=%d fill=%.2f write=%dKiB %s checkpoints=%d WA=%.2f\n",
		*diskGiB, *segMiB, S, *fillF, *ext*4, wl, cps, float64(du+dc)/float64(du))
	fmt.Printf("  full table   : %8.0f rows %9.1f KiB/checkpoint %6.3f%% of log\n",
		avgRows, avgRows*rowBytes/1024, 100*avgRows*rowBytes/cpBytes)
	fmt.Printf("  delta only   : %8.0f rows %9.1f KiB/checkpoint %6.3f%% of log  (p50 %d p99 %d max %d rows; bound %d)\n",
		avgDelta, avgDelta*rowBytes/1024, 100*avgDelta*rowBytes/cpBytes, pct(0.5), pct(0.99), deltas[len(deltas)-1], cpEvery)
	for _, r := range rots {
		avg := float64(r.bytes) / float64(cps)
		fmt.Printf("  %-13s: %9.1f KiB/checkpoint %6.3f%% of log, snapshot every %.1f checkpoints, recovery reads at most %.1f KiB (%.2fx full)\n",
			r.name, avg/1024, 100*avg/cpBytes, float64(cps)/float64(r.snapshots), float64(r.maxChain*rowBytes)/1024, float64(r.maxChain)/avgRows)
	}
	_ = os.Stdout.Sync()
}
