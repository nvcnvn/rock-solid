package chunkstore

import (
	"cmp"
	"fmt"
	"math"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

const allMoves = 1 << 30

// layout is a pool on disks, placed by Next from an empty table.
type layout struct {
	pool  Pool
	disks []Disk
	now   core.Tick
	held  uint64
	ids   uint32
	table Table
}

// grow adds a disk of size TB on node, with other TB of another pool's data on it.
func (l *layout) grow(node uint32, size, other float64) uint32 {
	id := l.ids
	l.ids++
	l.disks = append(l.disks, Disk{ID: id, Node: node, Blocks: tb(size), Live: tb(other)})
	return id
}

func newLayout(p Pool, sizes ...float64) *layout {
	l := &layout{pool: p}
	for i, s := range sizes {
		l.grow(uint32(i), s, 0)
	}
	return l
}

func (l *layout) inputs(moves int) *Inputs {
	return &Inputs{Now: l.now, Pools: []PoolInputs{{ID: 1, Pool: l.pool, Disks: l.disks}}, Moves: moves}
}

// step computes and keeps the next table, checking its delta, and reports its moves.
func (l *layout) step(t *testing.T, in *Inputs) int {
	t.Helper()
	next, moves := Next(&l.table, in)
	if err := checkDelta(&l.table, &next); err != nil {
		t.Fatal(err)
	}
	l.table = next
	return moves
}

// settle runs computations, each once the last one's moves have copied and activated, until one proposes no move.
func (l *layout) settle(t *testing.T) (moves, computations int) {
	t.Helper()
	for computations = 1; ; computations++ {
		l.hold()
		n := l.step(t, l.activateAll(allMoves))
		moves += n
		if n == 0 || computations == 20 {
			return moves, computations
		}
	}
}

func (l *layout) activateAll(moves int) *Inputs {
	in := l.inputs(moves)
	for i, e := range l.pools().PGs {
		in.Activations = append(in.Activations, PGInterval{core.PGID{Pool: 1, Index: uint32(i)}, e.Interval})
	}
	return in
}

func (l *layout) pools() *PoolTable { return &l.table.Pools[0] }

// loads counts each disk's members in the table.
func (l *layout) loads() []uint32 {
	n := make([]uint32, len(l.disks))
	for _, e := range l.pools().PGs {
		for _, m := range e.Members {
			if k := slices.IndexFunc(l.disks, func(d Disk) bool { return d.ID == m.Disk }); k >= 0 {
				n[k]++
			}
		}
	}
	return n
}

// hold lays out the pool's own data as the table sends it, held blocks over the pool, and sets each disk's slots.
func (l *layout) hold() {
	for i, n := range l.loads() {
		d := &l.disks[i]
		other := d.Live - d.Own
		d.Slots = n
		d.Own = l.held * uint64(n) / uint64(l.pool.PGCount)
		d.Live = other + d.Own
	}
}

func (l *layout) figures() Figures {
	l.hold()
	return l.pool.Capacity(l.disks, 0)
}

// placer is the computation's view of the layout's table: its disks by ID with their targets and loads.
func (l *layout) placer() *placer {
	pl := &placer{pool: l.pool, now: l.now, disks: slices.Clone(l.disks)}
	slices.SortFunc(pl.disks, func(a, b Disk) int { return cmp.Compare(a.ID, b.ID) })
	pl.target = l.pool.targets(pl.disks, pl.sheds)
	pl.load = make([]int, len(pl.disks))
	for _, e := range l.pools().PGs {
		for _, m := range e.Members {
			if k := pl.find(m.Disk); k >= 0 {
				pl.load[k]++
			}
		}
	}
	return pl
}

// converged reports whether every disk is within its margin, listing each disk's load and target.
func (l *layout) converged() (bool, string) {
	pl := l.placer()
	out := ""
	for k, d := range pl.disks {
		out += fmt.Sprintf(" %d:%d/%.1f", d.ID, pl.load[k], pl.target[k])
	}
	return pl.converged(), out
}

// checkDelta checks that next's delta from prev reproduces next and its CRC through its encoding.
func checkDelta(prev, next *Table) error {
	d := MakeDelta(prev, next)
	got, err := DecodeDelta(AppendDelta(nil, &d))
	if err != nil {
		return fmt.Errorf("delta of table %d does not decode: %v", next.Epoch, err)
	}
	applied, err := prev.Apply(&got)
	if err != nil {
		return fmt.Errorf("delta of table %d does not apply: %v", next.Epoch, err)
	}
	if a, b := AppendTable(nil, &applied), AppendTable(nil, next); string(a) != string(b) {
		return fmt.Errorf("delta of table %d gives another table", next.Epoch)
	}
	return nil
}

func capacityLayouts() map[string]*layout {
	ec := rep(3, 128)
	ec.Data, ec.Parity, ec.MaxPerDomain = 4, 2, 2
	c3 := &layout{pool: ec}
	for _, s := range []float64{2, 2, 2, 2, 0.5, 0.5} {
		c3.grow(uint32(len(c3.disks)/2), s, 0)
	}
	c4 := newLayout(rep(1, 256), 2, 2, 2)
	c4.disks[0].Live = tb(1.2)
	c1 := newLayout(rep(2, 128))
	c1.grow(0, 0.0007, 0)
	c1.grow(1, 0.002, 0)
	return map[string]*layout{
		"C1": c1,
		"C2": newLayout(rep(3, 128), 4, 4, 2, 1),
		"C3": c3,
		"C4": c4,
		"C5": newLayout(rep(3, 512), 4, 4, 4, 2, 2, 2, 1, 1, 1, 0.5),
		"C6": newLayout(rep(2, 128), 4, 1, 1),
	}
}

// lowerBound is the fewest moves that bring every disk within its fill margin, since each move takes one member off one disk and puts it on another, and the sum of the disks' margins, the most that moving from disks inside their margins adds.
func (l *layout) lowerBound() (int, int) {
	pl := l.placer()
	var over, under int
	var margins float64
	for k, t := range pl.target {
		hi, lo := 0, 0
		if t > 0 {
			margins += margin(t)
			hi, lo = int(math.Floor(t+margin(t)+1e-9)), int(math.Ceil(t-margin(t)-1e-9))
		}
		over += max(0, pl.load[k]-hi)
		under += max(0, lo-pl.load[k])
	}
	return max(over, under), int(margins)
}

// placed is the layout's table from scratch, settled and with every interval activated.
func placed(t *testing.T, l *layout) *layout {
	t.Helper()
	l.step(t, l.inputs(allMoves))
	l.settle(t)
	return l
}

// nodesOf is a layout of nodes, each of disks disks of size TB.
func nodesOf(p Pool, nodes, disks int, size float64) *layout {
	l := &layout{pool: p}
	for n := range nodes {
		for range disks {
			l.grow(uint32(n), size, 0)
		}
	}
	return l
}

// raised steps to the next table and returns the PGs whose interval it raised, failing on any other change of interval.
func (l *layout) raised(t *testing.T, in *Inputs) []int {
	t.Helper()
	prev := l.table
	l.step(t, in)
	if l.table.Epoch != prev.Epoch+1 {
		t.Fatalf("table epoch %d after %d", l.table.Epoch, prev.Epoch)
	}
	var pgs []int
	old := prev.Pools[0].PGs
	for i, e := range l.pools().PGs {
		switch e.Interval {
		case old[i].Interval:
		case old[i].Interval + 1:
			pgs = append(pgs, i)
		default:
			t.Fatalf("PG %d went from interval %d to %d", i, old[i].Interval, e.Interval)
		}
	}
	return pgs
}

// holding lists the PGs with a member on any of disks.
func (l *layout) holding(disks ...uint32) []int {
	var pgs []int
	for i, e := range l.pools().PGs {
		if slices.ContainsFunc(e.Members, func(m Member) bool { return slices.Contains(disks, m.Disk) }) {
			pgs = append(pgs, i)
		}
	}
	return pgs
}

func (l *layout) disk(id uint32) *Disk {
	return &l.disks[slices.IndexFunc(l.disks, func(d Disk) bool { return d.ID == id })]
}

func (l *layout) onNode(node uint32) []uint32 {
	var ids []uint32
	for _, d := range l.disks {
		if d.Node == node {
			ids = append(ids, d.ID)
		}
	}
	return ids
}

// freeNode is the lowest node holding no member of e.
func (l *layout) freeNode(e *PGEntry) uint32 {
	for node := uint32(0); ; node++ {
		if !slices.ContainsFunc(e.Members, func(m Member) bool { return l.disk(m.Disk).Node == node }) {
			return node
		}
	}
}

func (l *layout) mark(m Mark, ids ...uint32) {
	for _, id := range ids {
		d := l.disk(id)
		if m == MarkDown && d.Mark != MarkDown {
			d.DownSince = l.now
		}
		d.Mark = m
	}
}

func (l *layout) remove(id uint32) {
	l.disks = slices.DeleteFunc(l.disks, func(d Disk) bool { return d.ID == id })
}

func pg(i int) core.PGID { return core.PGID{Pool: 1, Index: uint32(i)} }

func disksOf(e *PGEntry) []uint32 {
	ids := make([]uint32, len(e.Members))
	for i, m := range e.Members {
		ids[i] = m.Disk
	}
	return ids
}

func all(n int) []int {
	pgs := make([]int, n)
	for i := range pgs {
		pgs[i] = i
	}
	return pgs
}

func samePGs(t *testing.T, row string, got, want []int) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("%s: raised %d PGs %v, want %d %v", row, len(got), got[:min(len(got), 8)], len(want), want[:min(len(want), 8)])
	}
}

// Each row of "Which PGs get a new interval" is one table: its epoch goes up by one, and exactly the PGs the row names go up by one interval.
func TestIntervalsRaisedByTableChanges(t *testing.T) {
	fresh := func() *layout { return placed(t, nodesOf(rep(3, 2048), 4, 3, 2)) }

	t.Run("pool created", func(t *testing.T) {
		l := fresh()
		b := rep(2, 64)
		in := l.inputs(0)
		in.Pools = append(in.Pools, PoolInputs{ID: 2, Pool: b, Disks: l.disks})
		samePGs(t, "pool B created", l.raised(t, in), nil)
		for i, e := range l.table.Pools[1].PGs {
			if e.Interval != 1 || len(e.Members) != 2 {
				t.Fatalf("pool B's PG %d at interval %d with %d members", i, e.Interval, len(e.Members))
			}
		}
	})
	t.Run("disk added, then balance moves one member", func(t *testing.T) {
		l := fresh()
		l.grow(0, 2, 0)
		samePGs(t, "disk added", l.raised(t, l.inputs(0)), nil)
		prev := l.table
		got := l.raised(t, l.inputs(1))
		if len(got) != 1 {
			t.Fatalf("one balance move raised %v", got)
		}
		was, is := prev.Pools[0].PGs[got[0]].Members, l.pools().PGs[got[0]].Members
		diff := 0
		for j := range was {
			if was[j] != is[j] {
				diff++
			}
		}
		if diff != 1 {
			t.Fatalf("PG %d went from %v to %v", got[0], was, is)
		}
		if d := MakeDelta(&prev, &l.table); len(d.Entries) != 1 {
			t.Fatalf("the delta holds %d entries", len(d.Entries))
		}
	})
	t.Run("disk and node marks", func(t *testing.T) {
		l := fresh()
		d := l.onNode(1)[0]
		l.mark(MarkDown, d)
		samePGs(t, "disk down", l.raised(t, l.inputs(0)), l.holding(d))
		l.mark(MarkUp, d)
		samePGs(t, "disk up", l.raised(t, l.inputs(0)), l.holding(d))
		node := l.onNode(2)
		for _, m := range []Mark{MarkDown, MarkUp, MarkMaint, MarkUp} {
			l.mark(m, node...)
			samePGs(t, fmt.Sprintf("node marked %d", m), l.raised(t, l.inputs(0)), l.holding(node...))
		}
	})
	t.Run("node down on 3 nodes and on 10", func(t *testing.T) {
		l := placed(t, nodesOf(rep(3, 2048), 3, 1, 2))
		l.mark(MarkDown, l.onNode(0)...)
		samePGs(t, "node down on 3 nodes", l.raised(t, l.inputs(0)), all(2048))
		l = placed(t, nodesOf(rep(3, 2048), 10, 3, 2))
		l.mark(MarkDown, l.onNode(4)...)
		got := l.raised(t, l.inputs(0))
		samePGs(t, "node down on 10 nodes", got, l.holding(l.onNode(4)...))
		if len(got) != 615 {
			t.Fatalf("a node down on 10 nodes raised %d of 2,048 PGs", len(got))
		}
	})
	t.Run("members reordered", func(t *testing.T) {
		l := fresh()
		disks := disksOf(&l.pools().PGs[7])
		slices.Reverse(disks)
		in := l.inputs(0)
		in.Edits = []Edit{{PG: pg(7), Disks: disks}}
		samePGs(t, "PG 7 reordered", l.raised(t, in), []int{7})
	})
	t.Run("pool settings", func(t *testing.T) {
		l := placed(t, nodesOf(rep(2, 256), 4, 1, 2))
		l.pool.Replicas = 3
		samePGs(t, "replicas 2 to 3", l.raised(t, l.inputs(0)), all(256))
		l.pool.MinLive = 2
		samePGs(t, "min_live changed", l.raised(t, l.inputs(0)), all(256))
		l.pool.WriteLimit = 80
		samePGs(t, "write_limit changed", l.raised(t, l.inputs(0)), nil)
	})
	t.Run("activation folded", func(t *testing.T) {
		l := fresh()
		d := l.onNode(1)[0]
		l.mark(MarkDown, d)
		l.raised(t, l.inputs(0))
		e := l.pools().PGs[l.holding(d)[0]]
		if len(e.Past) != 1 || e.Past[0].Interval != e.Interval-1 || e.Activated != e.Interval-1 {
			t.Fatalf("after a down mark, PG entry %+v", e)
		}
		prev := l.table
		samePGs(t, "activation folded", l.raised(t, l.activateAll(0)), nil)
		changed := MakeDelta(&prev, &l.table).Entries
		if len(changed) != len(l.holding(d)) {
			t.Fatalf("the activation delta holds %d entries, not %d", len(changed), len(l.holding(d)))
		}
		for _, c := range changed {
			if c.Entry.Activated != c.Entry.Interval || len(c.Entry.Past) != 0 {
				t.Fatalf("PG %d activated %d at interval %d with past %v", c.PG.Index, c.Entry.Activated, c.Entry.Interval, c.Entry.Past)
			}
		}
	})
	t.Run("pg_count doubled", func(t *testing.T) {
		l := placed(t, nodesOf(rep(3, 64), 4, 1, 2))
		for range 8 {
			in := l.inputs(0)
			in.Asks = []PGInterval{{pg(5), l.pools().PGs[5].Interval}}
			l.raised(t, in)
		}
		if got := l.pools().PGs[5].Interval; got != 9 {
			t.Fatalf("PG 5 at interval %d", got)
		}
		prev := l.table
		l.pool.PGCount = 128
		l.step(t, l.inputs(0))
		for i, e := range l.pools().PGs {
			parent := prev.Pools[0].PGs[i/2]
			if e.Interval != parent.Interval+1 || !slices.Equal(e.Members, parent.Members) {
				t.Fatalf("PG %d at %d with %v, from PG %d at %d with %v", i, e.Interval, e.Members, i/2, parent.Interval, parent.Members)
			}
		}
		if l.pools().PGs[10].Interval != 10 || l.pools().PGs[11].Interval != 10 {
			t.Fatal("PG 5 at 9 did not give PGs 10 and 11 at 10")
		}
	})
	t.Run("primary asks", func(t *testing.T) {
		l := fresh()
		in := l.inputs(0)
		in.Asks = []PGInterval{{pg(7), l.pools().PGs[7].Interval}, {pg(8), l.pools().PGs[8].Interval - 1}}
		samePGs(t, "PG 7 asks, PG 8 from an older interval", l.raised(t, in), []int{7})
		node := l.onNode(3)
		in = l.inputs(0)
		var want []int
		for i, e := range l.pools().PGs {
			if slices.Contains(node, e.Members[0].Disk) {
				in.Asks = append(in.Asks, PGInterval{pg(i), e.Interval})
				want = append(want, i)
			}
		}
		samePGs(t, "node 3 restarts and asks", l.raised(t, in), want)
	})
	t.Run("member removed and added back", func(t *testing.T) {
		l := fresh()
		e := l.pools().PGs[7]
		start := e.Interval
		disks := disksOf(&e)
		free := l.freeNode(&e)
		in := l.inputs(0)
		in.Edits = []Edit{{PG: pg(7), Disks: []uint32{disks[0], disks[1], l.onNode(free)[0]}}}
		samePGs(t, "member C removed", l.raised(t, in), []int{7})
		in = l.inputs(0)
		in.Edits = []Edit{{PG: pg(7), Disks: disks}}
		samePGs(t, "member C added back", l.raised(t, in), []int{7})
		if got := l.pools().PGs[7]; got.Interval != start+2 || !slices.Equal(got.Members, e.Members) {
			t.Fatalf("PG 7 back to %v at interval %d, from %v at %d", got.Members, got.Interval, e.Members, start)
		}
	})
	t.Run("down mark and refill", func(t *testing.T) {
		l := fresh()
		d := l.onNode(1)[0]
		pgs := l.holding(d)
		l.remove(d)
		samePGs(t, "a lost log: down mark and refill in one table", l.raised(t, l.inputs(0)), pgs)
		if len(l.holding(d)) != 0 {
			t.Fatal("a lost disk's members were not refilled at once")
		}
		l = fresh()
		l.mark(MarkDown, d)
		samePGs(t, "down mark", l.raised(t, l.inputs(0)), pgs)
		l.remove(d)
		samePGs(t, "refill in the next table", l.raised(t, l.inputs(0)), pgs)
		l = fresh()
		l.mark(MarkDown, d)
		samePGs(t, "down mark", l.raised(t, l.inputs(0)), pgs)
		l.now += RefillAfter
		samePGs(t, "refill 10 minutes later", l.raised(t, l.inputs(0)), pgs)
		if len(l.holding(d)) != 0 {
			t.Fatal("a member down for refill_after was not refilled")
		}
	})
}
