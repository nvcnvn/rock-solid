package subblock

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
)

// The append model: one disk log of S segments of B blocks, a hot stream for user records and a
// cold stream for the cleaner's copies, a greedy cleaner keeping `reserve` segments free, and
// liveness counted in bytes. Background data is a block volume at fill u, overwritten at random
// 4 KiB (or never, in the "alone" scenario). Append files grow by `a` bytes per fsync, each fsync
// one flush; a file is deleted at fileMax and starts again. Variants:
//
//	rmw       the flush rewrites each touched block whole (a 33-byte write entry, a 4 KiB block)
//	patch     a 35-byte patch entry and the bytes, packed after the record's blocks; the flush that
//	          completes a block rewrites it whole and frees its patches
//	patch8    as patch, also folding once a block holds 8 patches
//	patchhdr  as patch, the bytes kept in the header blocks, so a record of patches alone is one block
//	short     the flush rewrites the tail block as a short block: only its bytes up to the end of
//	          file, packed like a patch, with a 35-byte write entry carrying the stored length

type variant int

const (
	vRMW variant = iota
	vPatch
	vPatch8
	vPatchHdr
	vShort
)

func (v variant) String() string {
	return [...]string{"rmw", "patch", "patch8", "patchhdr", "short"}[v]
}

type acfg struct {
	v         variant
	u         float64
	appends   int // append flushes per round, each from its own file
	bg        int // background 4 KiB overwrites per round
	a         int // bytes per append
	alone     bool
	segs, blk int
	reserve   int
	fileMax   int
}

type item struct {
	seg   int32
	bytes int32
	live  bool
}

type piece struct {
	id      int32
	bytes   int
	entry   int
	kind    int // 0 whole block, 1 packed in the data area, 2 in the header
	cleaner bool
}

type file struct {
	size    int
	block   map[int]int32   // whole-block item per block
	patches map[int][]int32 // patch items per block
}

type asim struct {
	c        acfg
	rng      *rand.Rand
	items    []item
	segLive  []int64
	segItems [][]int32
	segUsed  []int
	segOpen  []bool
	free     []int32
	open     [2]int32 // hot, cold
	bgItem   []int32
	files    []file
	nextFile int

	dev, devCleaner      int64
	userApp, userBg, nap int64
}

func newAsim(c acfg, seed uint64) *asim {
	s := &asim{c: c, rng: rand.New(rand.NewPCG(seed, 7))}
	s.segLive = make([]int64, c.segs)
	s.segItems = make([][]int32, c.segs)
	s.segUsed = make([]int, c.segs)
	s.segOpen = make([]bool, c.segs)
	for i := c.segs - 1; i >= 0; i-- {
		s.free = append(s.free, int32(i))
	}
	s.open = [2]int32{-1, -1}
	nf := max(c.appends, 1)
	s.files = make([]file, nf)
	for i := range s.files {
		s.files[i] = file{block: map[int]int32{}, patches: map[int][]int32{}}
	}
	return s
}

func (s *asim) newItem(bytes int) int32 {
	s.items = append(s.items, item{seg: -1, bytes: int32(bytes), live: true})
	return int32(len(s.items) - 1)
}

func (s *asim) kill(id int32) {
	it := &s.items[id]
	if it.live {
		it.live = false
		if it.seg >= 0 {
			s.segLive[it.seg] -= int64(it.bytes)
		}
	}
}

func (s *asim) takeFree(stream int) {
	if s.open[stream] >= 0 {
		s.segOpen[s.open[stream]] = false
	}
	if len(s.free) == 0 {
		panic("log full")
	}
	n := s.free[len(s.free)-1]
	s.free = s.free[:len(s.free)-1]
	s.segOpen[n] = true
	s.segUsed[n] = 0
	s.open[stream] = n
}

// write appends the pieces as one record, or, where the open segment cannot hold it, as a
// record that fills the segment and the rest in the next, each part with its own header, as
// the cleaner-wa spike splits records.
func (s *asim) write(stream int, ps []piece) {
	for len(ps) > 0 {
		if s.open[stream] < 0 || s.segUsed[s.open[stream]]+2 > s.c.blk {
			s.takeFree(stream)
		}
		room := s.c.blk - s.segUsed[s.open[stream]]
		var r recLayout
		n := 0
		for n < len(ps) {
			next := r
			next.headerBytes += ps[n].entry
			if ps[n].kind == 2 {
				next.headerBytes += ps[n].bytes
			} else {
				next.dataBytes += ps[n].bytes
			}
			next.finish()
			if next.blocks() > room {
				break
			}
			r = next
			n++
		}
		if n == 0 {
			s.takeFree(stream)
			continue
		}
		s.place(stream, ps[:n], r)
		ps = ps[n:]
	}
}

func (s *asim) place(stream int, ps []piece, r recLayout) {
	seg := s.open[stream]
	s.segUsed[seg] += r.blocks()
	for _, p := range ps {
		it := &s.items[p.id]
		it.seg = seg
		if !it.live {
			continue
		}
		s.segLive[seg] += int64(p.bytes)
		s.segItems[seg] = append(s.segItems[seg], p.id)
	}
	b := int64(r.blocks()) * blockSize
	s.dev += b
	if stream == 1 {
		s.devCleaner += b
	}
}

func (s *asim) clean() {
	for passes := 0; len(s.free) < s.c.reserve; passes++ {
		if passes > 1000 {
			var lv []int64
			for i := range s.c.segs {
				if !s.segOpen[i] && s.segItems[i] != nil {
					lv = append(lv, s.segLive[i]/blockSize)
				}
			}
			slices.Sort(lv)
			panic(fmt.Sprintf("cleaner livelock: free %d, emptiest sealed segments hold %v blocks", len(s.free), lv[:12]))
		}
		var victims []int32
		for range 4 {
			best, bestLive := int32(-1), int64(-1)
			for i := range s.c.segs {
				if s.segOpen[i] || s.segItems[i] == nil || contains(victims, int32(i)) {
					continue
				}
				if best < 0 || s.segLive[i] < bestLive {
					best, bestLive = int32(i), s.segLive[i]
				}
			}
			if best < 0 {
				break
			}
			victims = append(victims, best)
		}
		var ps []piece
		size := 0
		for _, v := range victims {
			for _, id := range s.segItems[v] {
				it := &s.items[id]
				if !it.live || it.seg != v {
					continue
				}
				p := piece{id: id, bytes: int(it.bytes), entry: copyEntry, cleaner: true}
				if it.bytes != blockSize {
					p.kind, p.entry = 1, patchCopy
				}
				s.segLive[v] -= int64(it.bytes)
				ps = append(ps, p)
				size += p.bytes + p.entry + crcSize
				if size >= 64*blockSize {
					s.write(1, ps)
					ps, size = ps[:0], 0
				}
			}
		}
		if len(ps) > 0 {
			s.write(1, ps)
		}
		for _, v := range victims {
			if s.segLive[v] != 0 {
				panic(fmt.Sprintf("victim %d still holds %d bytes", v, s.segLive[v]))
			}
			s.segItems[v] = nil
			s.free = append(s.free, v)
		}
	}
}

func contains(xs []int32, x int32) bool {
	for _, y := range xs {
		if y == x {
			return true
		}
	}
	return false
}

func (s *asim) appendTo(f *file, ps []piece) []piece {
	a := s.c.a
	start, end := f.size, f.size+a
	for b := start / blockSize; b <= (end-1)/blockSize; b++ {
		lo, hi := max(start, b*blockSize), min(end, (b+1)*blockSize)
		if s.c.v == vShort {
			if old, ok := f.block[b]; ok {
				s.kill(old)
			}
			n := hi - b*blockSize
			id := s.newItem(n)
			f.block[b] = id
			kind := 1
			if n == blockSize {
				kind = 0
			}
			ps = append(ps, piece{id: id, bytes: n, entry: writeEntry + 2, kind: kind})
			continue
		}
		fold := s.c.v == vRMW || hi == (b+1)*blockSize || (s.c.v == vPatch8 && len(f.patches[b]) >= 8)
		if fold {
			if old, ok := f.block[b]; ok {
				s.kill(old)
			}
			for _, p := range f.patches[b] {
				s.kill(p)
			}
			delete(f.patches, b)
			id := s.newItem(blockSize)
			f.block[b] = id
			ps = append(ps, piece{id: id, bytes: blockSize, entry: writeEntry})
			continue
		}
		id := s.newItem(hi - lo)
		f.patches[b] = append(f.patches[b], id)
		kind := 1
		if s.c.v == vPatchHdr {
			kind = 2
		}
		ps = append(ps, piece{id: id, bytes: hi - lo, entry: patchEntry, kind: kind})
	}
	f.size = end
	s.userApp += int64(a)
	s.nap++
	if f.size >= s.c.fileMax {
		for _, id := range f.block {
			s.kill(id)
		}
		for _, l := range f.patches {
			for _, id := range l {
				s.kill(id)
			}
		}
		*f = file{block: map[int]int32{}, patches: map[int][]int32{}}
	}
	return ps
}

func (s *asim) fill() {
	n := int(s.c.u * float64(s.c.segs*s.c.blk))
	s.bgItem = make([]int32, n)
	var ps []piece
	for i := range n {
		id := s.newItem(blockSize)
		s.bgItem[i] = id
		ps = append(ps, piece{id: id, bytes: blockSize, entry: writeEntry})
		if len(ps) == 64 {
			s.write(0, ps)
			ps = ps[:0]
			s.clean()
		}
	}
	if len(ps) > 0 {
		s.write(0, ps)
	}
}

func (s *asim) round() {
	var ps []piece
	for range s.c.appends {
		f := &s.files[s.nextFile]
		s.nextFile = (s.nextFile + 1) % len(s.files)
		ps = s.appendTo(f, ps)
	}
	for range s.c.bg {
		i := s.rng.IntN(len(s.bgItem))
		s.kill(s.bgItem[i])
		id := s.newItem(blockSize)
		s.bgItem[i] = id
		ps = append(ps, piece{id: id, bytes: blockSize, entry: writeEntry})
		s.userBg += blockSize
	}
	if len(ps) > 0 {
		s.write(0, ps)
	}
	s.clean()
	if len(s.items) > 1<<22 {
		s.compactItems()
	}
}

// compactItems drops dead items so the item table stays small; ids are remapped everywhere.
func (s *asim) compactItems() {
	remap := make([]int32, len(s.items))
	var kept []item
	for i, it := range s.items {
		if it.live {
			remap[i] = int32(len(kept))
			kept = append(kept, it)
		} else {
			remap[i] = -1
		}
	}
	s.items = kept
	for seg := range s.segItems {
		var l []int32
		for _, id := range s.segItems[seg] {
			if remap[id] >= 0 {
				l = append(l, remap[id])
			}
		}
		if s.segItems[seg] != nil && l == nil {
			l = []int32{}
		}
		s.segItems[seg] = l
	}
	for i := range s.bgItem {
		s.bgItem[i] = remap[s.bgItem[i]]
	}
	for fi := range s.files {
		f := &s.files[fi]
		for b, id := range f.block {
			f.block[b] = remap[id]
		}
		for _, l := range f.patches {
			for j := range l {
				l[j] = remap[l[j]]
			}
		}
	}
}

type aresult struct {
	wa, devPerRound, cleanerShare, devPerAppend float64
	rounds                                      int64
}

func runAppend(c acfg, seed uint64) aresult {
	s := newAsim(c, seed)
	s.fill()
	capBytes := int64(c.segs*c.blk) * blockSize
	for s.dev < 6*capBytes {
		s.round()
	}
	d0, dc0, ua0, ub0, n0 := s.dev, s.devCleaner, s.userApp, s.userBg, s.nap
	var rounds int64
	for s.dev-d0 < 6*capBytes {
		s.round()
		rounds++
	}
	dev, cl := float64(s.dev-d0), float64(s.devCleaner-dc0)
	user := float64(s.userApp - ua0 + s.userBg - ub0)
	r := aresult{wa: dev / user, devPerRound: dev / float64(rounds), cleanerShare: cl / dev, rounds: rounds}
	if n := s.nap - n0; n > 0 {
		r.devPerAppend = dev / float64(n)
	}
	return r
}

func baseAcfg() acfg {
	return acfg{segs: 1024, blk: 256, reserve: 8, fileMax: 1 << 20, a: 100}
}

func TestAppendsAlone(t *testing.T) {
	for _, u := range []float64{0.8, 0.9} {
		for _, a := range []int{100, 1000} {
			if a == 1000 && u == 0.9 {
				continue
			}
			for _, v := range []variant{vRMW, vPatch, vPatch8, vPatchHdr, vShort} {
				c := baseAcfg()
				c.v, c.u, c.appends, c.alone, c.a = v, u, 1, true, a
				r := runAppend(c, 1)
				t.Logf("A1 alone u=%.2f a=%d %-8s: %6.0f device bytes per append, %5.1fx the appended bytes, cleaner %4.1f%% of device writes",
					u, a, v, r.devPerAppend, r.devPerAppend/float64(a), 100*r.cleanerShare)
			}
		}
	}
}

func avgRuns(c acfg, seeds int) aresult {
	var sum aresult
	for seed := range seeds {
		r := runAppend(c, uint64(seed+1))
		sum.wa += r.wa
		sum.devPerRound += r.devPerRound
		sum.cleanerShare += r.cleanerShare
	}
	n := float64(seeds)
	return aresult{wa: sum.wa / n, devPerRound: sum.devPerRound / n, cleanerShare: sum.cleanerShare / n}
}

func TestAppendsShared(t *testing.T) {
	const seeds = 3
	for _, u := range []float64{0.8, 0.9} {
		for _, k := range []int{1, 8} {
			b := baseAcfg()
			b.u, b.bg = u, 16-k
			base := avgRuns(b, seeds)
			t.Logf("A2 shared u=%.2f baseline %2d overwrites/round: WA %.2f, cleaner %4.1f%%, %6.0f device bytes per round", u, 16-k, base.wa, 100*base.cleanerShare, base.devPerRound)
			for _, v := range []variant{vRMW, vPatch, vPatch8, vPatchHdr, vShort} {
				c := b
				c.v, c.appends = v, k
				r := avgRuns(c, seeds)
				marg := (r.devPerRound - base.devPerRound) / float64(k)
				t.Logf("A2 shared u=%.2f %d appends + %2d overwrites/round %-8s: WA %.2f, cleaner %4.1f%%, marginal %6.0f device bytes per append (%5.1fx)",
					u, k, 16-k, v, r.wa, 100*r.cleanerShare, marg, marg/float64(c.a))
			}
		}
	}
}
