package chunkstore

import (
	"cmp"
	"math"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// Victim scores, (1 − u) × age ÷ (1 + u) with age capped at one disk capacity (262,144 blocks on
// the 1 GiB disk): of A and B, B; of C, nearly full snapshot data 10,000 capacities old, and D,
// D; uncapped, C would score 6.7 × 10⁷ and always win.
func TestCostBenefitScores(t *testing.T) {
	const capacity = 262144
	capped := func(age uint64) uint64 { return min(age, capacity) }
	a, b := costBenefit(0.5, 10), costBenefit(0.9, 100)
	c, d := costBenefit(0.95, capped(10000*capacity)), costBenefit(0.5, capped(30000))
	if math.Abs(a-3.33) > 0.01 || math.Abs(b-5.26) > 0.01 || b <= a {
		t.Fatalf("A scores %.2f and B %.2f, want 3.33 and 5.26", a, b)
	}
	if math.Abs(c-6722) > 1 || math.Abs(d-10000) > 0.01 || d <= c {
		t.Fatalf("C scores %.0f and D %.0f, want 6,722 and 10,000", c, d)
	}
	if u := costBenefit(0.95, 10000*capacity); u < 6.7e7 || u <= d {
		t.Fatalf("uncapped C scores %.3g", u)
	}
}

// cleanModel is the cleaner's victim choice over a disk of segments of uniform 4K overwrites, as
// the cleaner spike modelled it: blocks go to a hot stream in records of 16 under a header block,
// passes of 4 victims copy their live blocks to a cold stream whenever fewer than 8 segments are
// free, a moved block keeps its write time, and a frozen share of the data, which a snapshot
// pins, is never overwritten.
type cleanModel struct {
	r        *core.Rand
	segs, b  int
	live     []int
	mtime    []uint64
	free     []int
	sealed   []bool
	rev      []int32
	loc      []int32
	bm       []uint64
	open     [2]int
	fill     [2]int
	inRec    [2]int
	now      uint64
	user     int
	capAge   bool
	hot      bool
	ageOrder bool
	passes   int
	stalled  int
	maxStall int
}

func newCleanModel(seed uint64, segs, b int, full, frozen float64, capAge bool) *cleanModel {
	m := &cleanModel{r: core.NewRand(seed), segs: segs, b: b, live: make([]int, segs), mtime: make([]uint64, segs),
		sealed: make([]bool, segs), rev: make([]int32, segs*b), capAge: capAge}
	for i := range m.rev {
		m.rev[i] = -1
	}
	for s := segs - 1; s >= 0; s-- {
		m.free = append(m.free, s)
	}
	m.open = [2]int{m.take(), m.take()}
	blocks := int(full * float64(segs*b) * 15 / 16)
	m.user = int(float64(blocks) * (1 - frozen))
	m.loc, m.bm = make([]int32, blocks), make([]uint64, blocks)
	for id := range blocks {
		m.loc[id] = -1
		m.append(0, id)
	}
	return m
}

func (m *cleanModel) take() int {
	s := m.free[len(m.free)-1]
	m.free = m.free[:len(m.free)-1]
	return s
}

// append writes block id to stream st, a header block first at each record's start.
func (m *cleanModel) append(st, id int) {
	if m.inRec[st] == 0 {
		m.slot(st, -1)
	}
	m.inRec[st] = (m.inRec[st] + 1) % 16
	if old := m.loc[id]; old >= 0 {
		m.live[int(old)/m.b]--
		m.rev[old] = -1
	}
	if st == 0 {
		m.bm[id] = m.now
	}
	m.loc[id] = int32(m.slot(st, id))
	m.live[int(m.loc[id])/m.b]++
	m.mtime[int(m.loc[id])/m.b] = max(m.mtime[int(m.loc[id])/m.b], m.bm[id])
}

func (m *cleanModel) slot(st, id int) int {
	if m.fill[st] == m.b {
		m.sealed[m.open[st]] = true
		for len(m.free) < 2 {
			m.pass()
		}
		m.open[st], m.fill[st], m.inRec[st] = m.take(), 0, 0
		m.mtime[m.open[st]] = 0
		if id >= 0 {
			m.inRec[st] = 1
			m.slot(st, -1)
		}
	}
	at := m.open[st]*m.b + m.fill[st]
	m.fill[st]++
	m.rev[at] = int32(id)
	return at
}

func (m *cleanModel) write() {
	for len(m.free) < 8 {
		before := len(m.free)
		m.pass()
		if len(m.free) > before {
			m.stalled = 0
		} else if m.stalled++; m.stalled > m.maxStall {
			m.maxStall = m.stalled
		}
		if m.stalled >= 1000 {
			return
		}
	}
	m.now++
	if m.hot && m.r.Chance(9, 10) {
		m.append(0, int(m.r.Below(uint64(m.user/10))))
		return
	}
	m.append(0, int(m.r.Below(uint64(m.user))))
}

// pass copies the live blocks of the 4 best victims, by the cleaner's score, that would free more
// than their copy costs, header blocks included.
func (m *cleanModel) pass() {
	m.passes++
	var best [4]int
	var score [4]float64
	n := 0
	for s := range m.segs {
		if !m.sealed[s] {
			continue
		}
		lv := m.live[s]
		if m.b-lv <= (lv+15)/16+1 {
			continue
		}
		age := m.now - min(m.mtime[s], m.now) + 1
		if m.capAge {
			age = min(age, uint64(m.segs*m.b))
		}
		sc := costBenefit(float64(lv)/float64(m.b), age)
		k := n
		if n < 4 {
			n++
		} else if sc <= score[3] {
			continue
		} else {
			k = 3
		}
		for ; k > 0 && score[k-1] < sc; k-- {
			best[k], score[k] = best[k-1], score[k-1]
		}
		best[k], score[k] = s, sc
	}
	var ids []int
	for _, v := range best[:n] {
		m.sealed[v] = false
		for off := range m.b {
			if id := m.rev[v*m.b+off]; id >= 0 {
				ids = append(ids, int(id))
			}
		}
	}
	if m.ageOrder {
		slices.SortStableFunc(ids, func(a, b int) int { return cmp.Compare(m.bm[a], m.bm[b]) })
	}
	for _, id := range ids {
		m.append(1, id)
	}
	m.free = append(m.free, best[:n]...)
}

// The age cap regression (cleaner spike, C2): a 1 GiB disk of 4,096 segments of 64 blocks at 90%
// full, 20% of its data frozen by a snapshot, under 90/10 hot/cold overwrites, its copies in age
// order. With age capped at one disk capacity of writes, at most a few passes in a row gain no
// segment over 2 million writes; uncapped, old, nearly full segments win more often and the same
// writes take more passes. This model's net-gain guard keeps the uncapped cleaner out of the
// spike's livelock, which needed its own record and header layout.
func TestAgeCapKeepsTheCleanerGainingSegments(t *testing.T) {
	var passes [2]int
	for i, capAge := range []bool{true, false} {
		m := newCleanModel(3, 4096, 64, 0.9, 0.2, capAge)
		m.hot, m.ageOrder = true, true
		for range 2000000 {
			m.write()
		}
		if m.maxStall > 8 || m.passes < 1000 {
			t.Fatalf("cap %v: %d passes, %d in a row without gaining a segment", capAge, m.passes, m.maxStall)
		}
		passes[i] = m.passes
	}
	if passes[1] <= passes[0] {
		t.Fatalf("uncapped age took %d passes, capped %d", passes[1], passes[0])
	}
}
