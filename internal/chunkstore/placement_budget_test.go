package chunkstore

import (
	"bytes"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// stepBudget is max_ctl_txn_bytes, and stepCost an entry's bytes in a ctl/ Txn near enough: its
// encoding and 16 bytes of key, kind and lengths.
const stepBudget = 896 << 10

func stepCost(_ core.PGID, _, e *PGEntry) int { return len(AppendEntry(nil, e)) + 16 }

func deltaCost(prev, next *Table) int {
	n := 0
	for _, de := range MakeDelta(prev, next).Entries {
		n += stepCost(de.PG, nil, &de.Entry)
	}
	return n
}

// spread steps t under stepBudget with in until a table leaves nothing, failing on one over the
// budget, and returns how many tables it took.
func spread(tb *testing.T, t *Table, in Inputs) int {
	tb.Helper()
	in.Budget, in.Cost = stepBudget, stepCost
	for n := 1; ; n++ {
		next, _, left := Next(t, &in)
		if c := deltaCost(t, &next); c > stepBudget {
			tb.Fatalf("table %d holds %d bytes of changes", n, c)
		}
		*t = next
		if left == nil {
			return n
		}
		in.From = *left
	}
}

func sameTable(a, b *Table) bool {
	x, y := *a, *b
	x.Epoch, y.Epoch = 0, 0
	return bytes.Equal(AppendTable(nil, &x), AppendTable(nil, &y))
}

// The measured steps past max_ctl_txn_bytes, the refill after losing every third node of a rep3
// pool at its default pg_count on 246 disks, its members down for refill_after, and of a 4,096-PG
// ec16-2 pool, whose PGs are wider than a member merely down is refilled beside, its disks gone,
// and the down marks before them, each commit over several tables within the budget and end at the
// table one unbounded Next gives.
func TestAStepOverItsBudgetSpreadsOverTables(t *testing.T) {
	ec := Pool{Data: 16, Parity: 2, MinLive: 17, FailureDomain: DomainNode, MaxPerDomain: 1, Locality: LocalitySpread, Overcommit: true, WriteLimit: 90, PGCount: 4096}
	for _, c := range []struct {
		name  string
		pool  Pool
		nodes int
		gone  bool
	}{{"rep3", rep(3, PGCountFor(246, 3)), 246, false}, {"ec16-2", ec, 30, true}} {
		t.Run(c.name, func(t *testing.T) {
			l := nodesOf(c.pool, c.nodes, 1, 8)
			var whole, cut Table
			step := func(in *Inputs) (bool, int) {
				next, _, _ := Next(&whole, in)
				over := deltaCost(&whole, &next) > stepBudget
				whole = next
				return over, spread(t, &cut, *in)
			}
			step(l.inputs(0))
			in := l.inputs(0)
			for i, e := range whole.Pools[0].PGs {
				in.Activations = append(in.Activations, PGInterval{pg(i), e.Interval})
			}
			step(in)
			for n := range uint32(c.nodes) {
				if n%3 == 0 {
					l.mark(MarkDown, l.onNode(n)...)
				}
			}
			step(l.inputs(0))
			l.now += RefillAfter
			for n := range uint32(c.nodes) {
				for _, id := range l.onNode(n) {
					if c.gone && n%3 == 0 {
						l.remove(id)
					}
				}
			}
			if over, tables := step(l.inputs(0)); !over || tables < 2 {
				t.Fatalf("the refill was over the budget %v, took %d tables", over, tables)
			}
			if !sameTable(&whole, &cut) {
				t.Fatal("the tables under the budget end elsewhere than the unbounded one")
			}
		})
	}
}

// A pool whose head changes in a step over the budget, by a split or a min_live change, changes
// whole in its first table, beside what fits of another pool's down marks, though the order starts
// at the other pool; the tables after it hold the rest of those and nothing of the changed pool.
func TestAPoolWhoseHeadChangesStaysInOneTable(t *testing.T) {
	for _, c := range []struct {
		name   string
		change func(*Pool)
		budget int
	}{{"split", func(p *Pool) { p.PGCount = 8192 }, stepBudget}, {"min_live", func(p *Pool) { p.MinLive = 2 }, stepBudget / 2}} {
		name := c.name
		l := nodesOf(rep(3, 4096), 30, 1, 8)
		in := func() *Inputs {
			return &Inputs{Now: l.now, Pools: []PoolInputs{{ID: 1, Pool: l.pool, Disks: l.disks}, {ID: 2, Pool: rep(3, 8192), Disks: l.disks}}}
		}
		var tbl Table
		if spread(t, &tbl, *in()) != 1 {
			t.Fatal("the pools' creation took more than one table")
		}
		c.change(&l.pool)
		for n := range uint32(30) {
			if n%3 == 0 {
				l.mark(MarkDown, l.onNode(n)...)
			}
		}
		whole, _, _ := Next(&tbl, in())
		bounded := *in()
		bounded.Budget, bounded.Cost, bounded.From = c.budget, stepCost, core.PGID{Pool: 2}
		first, _, left := Next(&tbl, &bounded)
		if left == nil || len(first.Pools[0].PGs) != int(l.pool.PGCount) || slices.ContainsFunc(first.Pools[0].PGs, func(e PGEntry) bool { return e.Interval != 2 }) {
			t.Fatalf("the %s's table left %v, holds pool 1 as %d PGs", name, left, len(first.Pools[0].PGs))
		}
		for prev := first; ; prev = tbl {
			bounded.From = *left
			tbl, _, left = Next(&prev, &bounded)
			if slices.ContainsFunc(MakeDelta(&prev, &tbl).Entries, func(de DeltaEntry) bool { return de.PG.Pool == 1 }) {
				t.Fatalf("a table after the %s changes pool 1", name)
			}
			if left == nil {
				break
			}
		}
		if !sameTable(&tbl, &whole) {
			t.Fatalf("the tables under the budget after the %s end elsewhere than the unbounded one", name)
		}
	}
}

// A table over its budget is followed by one that takes the changes from the PG it stopped at, so
// the activations of the PGs it changed, reported in between, wait behind the down marks it left.
func TestTheNextTableResumesWhereTheLastStopped(t *testing.T) {
	l := nodesOf(rep(3, 256), 6, 1, 8)
	l.step(t, l.inputs(0))
	l.mark(MarkDown, l.onNode(0)...)
	budget := 100 * stepCost(core.PGID{}, nil, &PGEntry{Members: make([]Member, 3), Past: []PastInterval{{Members: make([]Member, 3)}}})
	in := l.inputs(0)
	in.Budget, in.Cost = budget, stepCost
	next, _, left := Next(&l.table, in)
	if left == nil {
		t.Fatal("the down marks fit one table")
	}
	l.table = next
	in = l.reportAll(0)
	in.Budget, in.Cost, in.From = budget, stepCost, *left
	l.step(t, in)
	for i, e := range l.pools().PGs {
		if slices.ContainsFunc(e.Members, func(m Member) bool { return m.Disk == l.onNode(0)[0] && m.Mark != MarkDown }) {
			t.Fatalf("PG %d still has node 0's disk up", i)
		}
	}
}
