// Command sim counts the chunk-index writes a log cleaner causes under three ways of finding
// relocated blocks: updating the index per move (the design, D), the same with adjacent extents
// coalesced after a move (DC), and a Pebble-style forwarding table keyed by the block's original
// address that the cleaner updates instead of the index (F).
package main

import (
	"flag"
	"fmt"
	"math/rand/v2"
	"slices"
	"time"
)

const chunkBlocks = 1024
const recBlocks = 256

type extent struct {
	first, count uint16
	addr         int64
	rec          int32
}

type model struct {
	name      string
	coalesce  bool
	fwd       bool
	ext       []extent
	free      []int32
	b2e       []int32
	userPuts  uint64
	movePuts  uint64
	countPuts uint64
	fwdPuts   uint64
}

func newModel(name string, L int, coalesce, fwd bool) *model {
	m := &model{name: name, coalesce: coalesce, fwd: fwd, b2e: make([]int32, L)}
	for i := range m.b2e {
		m.b2e[i] = -1
	}
	return m
}

func (m *model) alloc(e extent) int32 {
	if n := len(m.free); n > 0 {
		id := m.free[n-1]
		m.free = m.free[:n-1]
		m.ext[id] = e
		return id
	}
	m.ext = append(m.ext, e)
	return int32(len(m.ext) - 1)
}

func (m *model) release(id int32) { m.free = append(m.free, id) }

func (m *model) live() int { return len(m.ext) - len(m.free) }

func (m *model) setRange(chunkBase int, first, count uint16, id int32) {
	for b := int(first); b < int(first)+int(count); b++ {
		m.b2e[chunkBase+b] = id
	}
}

// carve removes blocks [b, b+n) from the extent holding them and returns the puts it cost: the
// left remainder keeps the key and changes its value, the right remainder gets a new key, and a
// fully covered extent's key is overwritten by the caller's new extent.
func (m *model) carve(l, n int) (puts uint64, keyAtB bool) {
	id := m.b2e[l]
	if id < 0 {
		return 0, false
	}
	e := m.ext[id]
	base := l - l%chunkBlocks
	b := uint16(l % chunkBlocks)
	end := int(e.first) + int(e.count)
	if e.first < b {
		left := e
		left.count = b - e.first
		m.ext[id] = left
		puts++
	} else {
		keyAtB = true
		m.release(id)
	}
	if end > int(b)+n {
		right := e
		shift := uint16(int(b) + n - int(e.first))
		right.first = e.first + shift
		right.count = e.count - shift
		right.addr = e.addr + int64(shift)
		rid := m.alloc(right)
		m.setRange(base, right.first, right.count, rid)
		puts++
	}
	return puts, keyAtB
}

func (m *model) userWrite(l int, addr int64, rec int32) {
	p, _ := m.carve(l, 1)
	id := m.alloc(extent{first: uint16(l % chunkBlocks), count: 1, addr: addr, rec: rec})
	m.b2e[l] = id
	m.userPuts += p + 1
	m.countPuts += 2
}

// moveRun relocates n consecutive blocks of one chunk, all in one extent of this model, to addr
// in record rec.
func (m *model) moveRun(l, n int, addr int64, rec int32) {
	p, keyAtB := m.carve(l, n)
	base := l - l%chunkBlocks
	b := uint16(l % chunkBlocks)
	ne := extent{first: b, count: uint16(n), addr: addr, rec: rec}
	merged := false
	if m.coalesce && b > 0 {
		if lid := m.b2e[l-1]; lid >= 0 {
			le := m.ext[lid]
			if le.rec == rec && le.addr+int64(le.count) == addr {
				le.count += uint16(n)
				m.ext[lid] = le
				m.setRange(base, b, uint16(n), lid)
				merged = true
				p++
				if keyAtB {
					p++
				}
			}
		}
	}
	if !merged {
		id := m.alloc(ne)
		m.setRange(base, b, uint16(n), id)
		p++
	}
	m.movePuts += p
	m.countPuts += 2
}

type move struct {
	l      int
	src    int32
	dst    int32
	origin int64
}

type sim struct {
	S, B, L    int
	rng        *rand.Rand
	cur        []int32
	phys       []int32
	physOrigin []int64
	physRec    []int32
	origin     []int64
	moved      []bool
	originRec  []int32
	recPuts    uint64
	countKeys  uint64
	fwdRecSum  float64
	segLive    []int32
	segTime    []int64
	segState   []uint8
	segLogical []int64
	freeSegs   []int32
	nextLog    int64
	now        int64
	userSeg    int32
	userPos    int
	userRec    int32
	userRecN   int
	cleanSeg   int32
	cleanPos   int
	cleanRec   int32
	cleanRecN  int
	nextRec    int32
	order      string
	models     []*model
	userW      uint64
	copied     uint64
	snapshots  int
	liveSum    []float64
	fwdLiveSum float64
	hopSum     float64
	logSegSum  float64
	moves      []move
	wl         string
	hotChunks  int
	hotW       float64
}

const (
	segFree = iota
	segOpen
	segUsed
)

func newSim(S, B int, u float64, wl string, order string, seed uint64) *sim {
	L := int(u*float64(S*B)) / chunkBlocks * chunkBlocks
	s := &sim{S: S, B: B, L: L, rng: rand.New(rand.NewPCG(seed, 1)), order: order, wl: wl}
	s.cur = make([]int32, L)
	s.origin = make([]int64, L)
	s.moved = make([]bool, L)
	s.originRec = make([]int32, L)
	s.phys = make([]int32, S*B)
	s.physOrigin = make([]int64, S*B)
	s.physRec = make([]int32, S*B)
	for i := range s.phys {
		s.phys[i] = -1
	}
	s.segLive = make([]int32, S)
	s.segTime = make([]int64, S)
	s.segState = make([]uint8, S)
	s.segLogical = make([]int64, S)
	for i := S - 1; i >= 0; i-- {
		s.freeSegs = append(s.freeSegs, int32(i))
	}
	s.models = []*model{newModel("D", L, false, false), newModel("DC", L, true, false), newModel("F", L, false, true)}
	s.liveSum = make([]float64, len(s.models))
	switch wl {
	case "hc90":
		s.hotChunks, s.hotW = L/chunkBlocks/10, 0.9
	case "hc80":
		s.hotChunks, s.hotW = L/chunkBlocks/5, 0.8
	}
	s.userSeg, s.cleanSeg = -1, -1
	return s
}

func (s *sim) openSeg() int32 {
	n := len(s.freeSegs)
	seg := s.freeSegs[n-1]
	s.freeSegs = s.freeSegs[:n-1]
	s.segState[seg] = segOpen
	s.segLogical[seg] = s.nextLog
	s.nextLog++
	return seg
}

func (s *sim) closeSeg(seg int32) {
	s.segState[seg] = segUsed
	s.segTime[seg] = s.now
}

func (s *sim) pickBlock() int {
	if s.hotChunks == 0 {
		return s.rng.IntN(s.L)
	}
	nch := s.L / chunkBlocks
	if s.rng.Float64() < s.hotW {
		return s.rng.IntN(s.hotChunks * chunkBlocks)
	}
	return s.hotChunks*chunkBlocks + s.rng.IntN((nch-s.hotChunks)*chunkBlocks)
}

func (s *sim) kill(lba int32) {
	if lba < 0 {
		return
	}
	s.phys[lba] = -1
	s.segLive[int(lba)/s.B]--
}

func (s *sim) userWrite(l int, count bool) {
	if s.userSeg < 0 || s.userPos == s.B {
		if s.userSeg >= 0 {
			s.closeSeg(s.userSeg)
		}
		for len(s.freeSegs) <= 8 {
			s.clean()
		}
		s.userSeg, s.userPos = s.openSeg(), 0
		s.userRecN = recBlocks
	}
	if s.userRecN == recBlocks {
		s.userRec, s.userRecN = s.nextRec, 0
		s.nextRec++
	}
	lba := int32(int(s.userSeg)*s.B + s.userPos)
	s.userPos++
	s.userRecN++
	s.now++
	old := s.cur[l]
	s.kill(old)
	s.cur[l] = lba
	s.phys[lba] = int32(l)
	org := s.segLogical[s.userSeg]*int64(s.B) + int64(lba%int32(s.B))
	s.physOrigin[lba] = org
	s.physRec[lba] = s.userRec
	s.segLive[s.userSeg]++
	wasMoved := s.moved[l]
	s.origin[l] = org
	s.moved[l] = false
	s.originRec[l] = s.userRec
	if !count {
		return
	}
	s.userW++
	for _, m := range s.models {
		if m.fwd {
			m.userWrite(l, org, s.userRec)
			if wasMoved {
				m.fwdPuts++
			}
		} else {
			m.userWrite(l, int64(lba), s.userRec)
		}
	}
}

func (s *sim) clean() {
	type cand struct {
		seg   int32
		score float64
	}
	var cs []cand
	capAge := int64(s.S * s.B)
	for seg := 0; seg < s.S; seg++ {
		if s.segState[seg] != segUsed || int(s.segLive[seg]) >= s.B {
			continue
		}
		u := float64(s.segLive[seg]) / float64(s.B)
		age := min(s.now-s.segTime[seg], capAge)
		cs = append(cs, cand{int32(seg), (1 - u) * float64(age+1) / (1 + u)})
	}
	slices.SortFunc(cs, func(a, b cand) int {
		if a.score > b.score {
			return -1
		}
		if a.score < b.score {
			return 1
		}
		return int(a.seg - b.seg)
	})
	if len(cs) > 4 {
		cs = cs[:4]
	}
	s.moves = s.moves[:0]
	for _, c := range cs {
		for off := 0; off < s.B; off++ {
			lba := int32(int(c.seg)*s.B + off)
			if l := s.phys[lba]; l >= 0 {
				s.moves = append(s.moves, move{l: int(l), src: lba, origin: s.physOrigin[lba]})
			}
		}
	}
	if s.order == "origin" {
		slices.SortFunc(s.moves, func(a, b move) int { return cmp64(a.origin, b.origin) })
	} else {
		slices.SortFunc(s.moves, func(a, b move) int { return a.l - b.l })
	}
	recs := make([]int32, len(s.moves))
	for i := range s.moves {
		if s.cleanSeg < 0 || s.cleanPos == s.B {
			if s.cleanSeg >= 0 {
				s.closeSeg(s.cleanSeg)
			}
			s.cleanSeg, s.cleanPos = s.openSeg(), 0
			s.cleanRecN = recBlocks
		}
		if s.cleanRecN == recBlocks {
			s.cleanRec, s.cleanRecN = s.nextRec, 0
			s.nextRec++
		}
		mv := &s.moves[i]
		mv.dst = int32(int(s.cleanSeg)*s.B + s.cleanPos)
		s.cleanPos++
		s.cleanRecN++
		recs[i] = s.cleanRec
		s.phys[mv.dst] = int32(mv.l)
		s.physOrigin[mv.dst] = mv.origin
		s.physRec[mv.dst] = s.cleanRec
		s.segLive[s.cleanSeg]++
		s.cur[mv.l] = mv.dst
		s.moved[mv.l] = true
	}
	s.copied += uint64(len(s.moves))
	s.passKeys(recs)
	for _, m := range s.models {
		if m.fwd {
			s.applyForward(m, recs)
		} else {
			s.applyMoves(m, recs)
		}
	}
	for _, c := range cs {
		for off := 0; off < s.B; off++ {
			s.phys[int(c.seg)*s.B+off] = -1
		}
		s.segLive[c.seg] = 0
		s.segState[c.seg] = segFree
		s.freeSegs = append(s.freeSegs, c.seg)
	}
}

// passKeys counts what one pass writes when the memtable absorbs repeated puts: the distinct
// record-count keys it touches (source and destination records), and the forwarding entries a
// per-record remap needs, one per (original record, destination record) pair.
func (s *sim) passKeys(recs []int32) {
	keys := map[int32]struct{}{}
	type pr struct{ o, d int32 }
	pairs := map[pr]struct{}{}
	for i, mv := range s.moves {
		keys[s.physRec[mv.src]] = struct{}{}
		keys[recs[i]] = struct{}{}
		pairs[pr{s.originRec[mv.l], recs[i]}] = struct{}{}
	}
	s.countKeys += uint64(len(keys))
	s.recPuts += uint64(len(pairs))
}

func cmp64(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// applyMoves groups the pass's moves, in destination order, into runs that one Move call
// applies: consecutive blocks of one chunk in one extent of the model, consecutive at both ends,
// in one destination record.
func (s *sim) applyMoves(m *model, recs []int32) {
	i := 0
	for i < len(s.moves) {
		a := s.moves[i]
		id := m.b2e[a.l]
		j := i + 1
		for j < len(s.moves) {
			b := s.moves[j]
			if b.l != a.l+(j-i) || b.l/chunkBlocks != a.l/chunkBlocks || m.b2e[b.l] != id ||
				b.src != a.src+int32(j-i) || b.dst != a.dst+int32(j-i) || recs[j] != recs[i] {
				break
			}
			j++
		}
		m.moveRun(a.l, j-i, int64(a.dst), recs[i])
		i = j
	}
}

// applyForward counts the forwarding entries the pass writes: one per run of moves consecutive
// in original address and in destination. The index is not touched; the per-record live counts
// still move to the copy record.
func (s *sim) applyForward(m *model, recs []int32) {
	i := 0
	for i < len(s.moves) {
		a := s.moves[i]
		j := i + 1
		for j < len(s.moves) {
			b := s.moves[j]
			if b.origin != a.origin+int64(j-i) || b.dst != a.dst+int32(j-i) || recs[j] != recs[i] {
				break
			}
			j++
		}
		m.fwdPuts++
		m.countPuts += 2
		i = j
	}
}

func (s *sim) snapshot() {
	s.snapshots++
	for i, m := range s.models {
		s.liveSum[i] += float64(m.live())
	}
	type pair struct{ o, d int64 }
	var ps []pair
	logical := map[int64]struct{}{}
	for l := 0; l < s.L; l++ {
		logical[s.origin[l]/int64(s.B)] = struct{}{}
		if s.moved[l] {
			ps = append(ps, pair{s.origin[l], int64(s.cur[l])})
		}
	}
	slices.SortFunc(ps, func(a, b pair) int { return cmp64(a.o, b.o) })
	runs := 0
	for i := range ps {
		if i == 0 || ps[i].o != ps[i-1].o+1 || ps[i].d != ps[i-1].d+1 {
			runs++
		}
	}
	s.fwdLiveSum += float64(runs)
	type pr struct{ o, d int32 }
	pairs := map[pr]struct{}{}
	for l := 0; l < s.L; l++ {
		if s.moved[l] {
			pairs[pr{s.originRec[l], s.physRec[s.cur[l]]}] = struct{}{}
		}
	}
	s.fwdRecSum += float64(len(pairs))
	s.hopSum += float64(len(ps)) / float64(s.L)
	s.logSegSum += float64(len(logical))
}

func (s *sim) reset() {
	s.userW, s.copied = 0, 0
	for _, m := range s.models {
		m.userPuts, m.movePuts, m.countPuts, m.fwdPuts = 0, 0, 0, 0
	}
	s.snapshots, s.fwdLiveSum, s.hopSum, s.logSegSum, s.fwdRecSum = 0, 0, 0, 0, 0
	s.recPuts, s.countKeys = 0, 0
	for i := range s.liveSum {
		s.liveSum[i] = 0
	}
}

func main() {
	S := flag.Int("segs", 2048, "segments")
	B := flag.Int("blocks", 1024, "blocks per segment")
	u := flag.Float64("u", 0.9, "fill")
	wl := flag.String("wl", "uniform", "uniform | hc90 | hc80")
	order := flag.String("order", "chunk", "cleaner copy order: chunk | origin")
	warm := flag.Float64("warm", 4, "warm-up, in logical capacities of writes")
	meas := flag.Float64("meas", 2, "measurement, in logical capacities of writes")
	seed := flag.Uint64("seed", 1, "seed")
	flag.Parse()
	t0 := time.Now()
	s := newSim(*S, *B, *u, *wl, *order, *seed)
	for l := 0; l < s.L; l++ {
		s.userWrite(l, false)
	}
	for _, m := range s.models {
		for c := 0; c < s.L/chunkBlocks; c++ {
			for f := 0; f < chunkBlocks; f += recBlocks {
				l := c*chunkBlocks + f
				addr := int64(s.cur[l])
				if m.fwd {
					addr = s.origin[l]
				}
				id := m.alloc(extent{first: uint16(f), count: recBlocks, addr: addr, rec: s.physRec[s.cur[l]]})
				m.setRange(c*chunkBlocks, uint16(f), recBlocks, id)
			}
		}
	}
	nw := int(*warm * float64(s.L))
	for i := 0; i < nw; i++ {
		s.userWrite(s.pickBlock(), true)
	}
	s.reset()
	nm := int(*meas * float64(s.L))
	snapEvery := nm / 8
	for i := 0; i < nm; i++ {
		s.userWrite(s.pickBlock(), true)
		if (i+1)%snapEvery == 0 {
			s.snapshot()
		}
	}
	uw := float64(s.userW)
	mv := float64(s.copied)
	fmt.Printf("RUN wl=%s u=%.2f order=%s segs=%d blocks=%d L=%d WA=%.3f moved_per_user=%.3f secs=%.1f\n",
		*wl, *u, *order, *S, *B, s.L, (uw+mv)/uw, mv/uw, time.Since(t0).Seconds())
	fmt.Printf("  distinct_count_keys/moved=%.4f fwd_rec_puts/moved=%.4f live_fwd_rec_entries/live_block=%.4f\n",
		float64(s.countKeys)/mv, float64(s.recPuts)/mv, s.fwdRecSum/float64(s.snapshots)/float64(s.L))
	for i, m := range s.models {
		live := s.liveSum[i] / float64(s.snapshots)
		idx := float64(m.userPuts + m.movePuts)
		share := float64(m.movePuts) / idx
		fmt.Printf("  model=%-2s user_puts/uw=%.3f move_puts/moved=%.3f move_puts/uw=%.3f cleaner_share_extent=%.3f count_puts/uw=%.3f fwd_puts/moved=%.3f fwd_puts/uw=%.3f live_extents=%.0f extents/live_block=%.4f",
			m.name, float64(m.userPuts)/uw, float64(m.movePuts)/mv, float64(m.movePuts)/uw, share,
			float64(m.countPuts)/uw, float64(m.fwdPuts)/mv, float64(m.fwdPuts)/uw, live, live/float64(s.L))
		if m.fwd {
			fl := s.fwdLiveSum / float64(s.snapshots)
			fmt.Printf(" live_fwd_runs=%.0f fwd_runs/live_block=%.4f entries_total/live_block=%.4f reads_via_fwd=%.3f logical_segs=%.0f",
				fl, fl/float64(s.L), (live+fl)/float64(s.L), s.hopSum/float64(s.snapshots), s.logSegSum/float64(s.snapshots))
		}
		fmt.Println()
	}
}
