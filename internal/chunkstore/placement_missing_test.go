package chunkstore

import (
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

const minute = core.Tick(60_000_000)

// Each row of "Which members are missing": a rep3 pool on 4 nodes unless the row says otherwise, member M on disk m.
func TestMissingMembers(t *testing.T) {
	down := func(t *testing.T, l *layout, m uint32, d core.Tick) {
		l.mark(MarkDown, m)
		l.step(t, l.inputs(0))
		l.now += d
	}
	rep2 := rep(2, 128)
	rep2.MinLive = 2
	rows := []struct {
		name     string
		pool     Pool
		nodes    int
		refilled bool
		run      func(t *testing.T, l *layout, m uint32)
	}{
		{"down 9 min 59 s", rep(3, 128), 4, false, func(t *testing.T, l *layout, m uint32) { down(t, l, m, RefillAfter-1_000_000) }},
		{"down 10 min", rep(3, 128), 4, true, func(t *testing.T, l *layout, m uint32) { down(t, l, m, RefillAfter) }},
		{"down 5 min, up, down 5 min", rep(3, 128), 4, false, func(t *testing.T, l *layout, m uint32) {
			down(t, l, m, 5*minute)
			l.mark(MarkUp, m)
			l.step(t, l.inputs(0))
			down(t, l, m, 5*minute)
		}},
		{"down, then a new log ID", rep(3, 128), 4, true, func(t *testing.T, l *layout, m uint32) {
			down(t, l, m, minute)
			node := l.disk(m).Node
			l.remove(m)
			l.grow(node, 2, 0)
		}},
		{"down, then declared lost", rep(3, 128), 4, true, func(t *testing.T, l *layout, m uint32) {
			down(t, l, m, minute)
			l.remove(m)
		}},
		{"down 59 s in rep2 with min_live 2", rep2, 4, false, func(t *testing.T, l *layout, m uint32) {
			down(t, l, m, DegradedRefillAfter-1_000_000)
		}},
		{"down 1 min in rep2 with min_live 2", rep2, 4, true, func(t *testing.T, l *layout, m uint32) { down(t, l, m, DegradedRefillAfter) }},
		{"in maintenance for an hour", rep2, 4, false, func(t *testing.T, l *layout, m uint32) {
			l.mark(MarkMaint, m)
			l.step(t, l.inputs(0))
			l.now += 60 * minute
		}},
		{"down for an hour on 3 nodes", rep(3, 128), 3, false, func(t *testing.T, l *layout, m uint32) { down(t, l, m, 60*minute) }},
	}
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			l := placed(t, nodesOf(r.pool, r.nodes, 1, 2))
			m := l.onNode(1)[0]
			pgs := l.holding(m)
			r.run(t, l, m)
			l.step(t, l.inputs(0))
			kept := l.holding(m)
			if r.refilled && len(kept) != 0 || !r.refilled && len(kept) != len(pgs) {
				t.Fatalf("M kept %d of its %d PGs", len(kept), len(pgs))
			}
			for _, i := range pgs {
				e := &l.pools().PGs[i]
				if len(e.Members) != r.pool.Replicas || live(e.Members) < r.pool.MinLive && r.name != "down 59 s in rep2 with min_live 2" {
					t.Fatalf("PG %d: %v", i, e.Members)
				}
			}
		})
	}
}

// A 5 s hiccup in a rep2 PG with min_live 2 moves no member: one interval at the down mark, one when it comes back.
func TestHiccupMovesNoMember(t *testing.T) {
	p := rep(2, 128)
	p.MinLive = 2
	l := placed(t, nodesOf(p, 4, 1, 2))
	m := l.onNode(1)[0]
	before := l.table
	l.mark(MarkDown, m)
	samePGs(t, "down", l.raised(t, l.inputs(allMoves)), l.holding(m))
	l.now += 5_000_000
	l.mark(MarkUp, m)
	samePGs(t, "up", l.raised(t, l.inputs(allMoves)), l.holding(m))
	for i, e := range l.pools().PGs {
		if !slices.Equal(e.Members, before.Pools[0].PGs[i].Members) {
			t.Fatalf("PG %d moved from %v to %v", i, before.Pools[0].PGs[i].Members, e.Members)
		}
	}
}

// A refill member counts toward min_live from the table that adds it: a rep2 PG with min_live 2 whose other member is live has two live members at once.
func TestRefillMemberCountsAtOnce(t *testing.T) {
	p := rep(2, 128)
	p.MinLive = 2
	l := placed(t, nodesOf(p, 4, 1, 2))
	m := l.onNode(1)[0]
	pgs := l.holding(m)
	l.mark(MarkDown, m)
	l.step(t, l.inputs(0))
	l.now += DegradedRefillAfter
	l.step(t, l.inputs(0))
	for _, i := range pgs {
		if e := l.pools().PGs[i]; slices.ContainsFunc(e.Members, func(x Member) bool { return x.Mark != MarkUp }) {
			t.Fatalf("PG %d after its refill: %v", i, e.Members)
		}
	}
}

// Peering waits for the only member live in an interval since the last activated one, so no refill rule replaces it, while a member it does not wait for in the same PG is refilled. The interval counts by the min_live it ran under: raising min_live to 2 after it keeps the member awaited.
func TestAwaitedMemberStays(t *testing.T) {
	for _, raise := range []bool{false, true} {
		l := placed(t, nodesOf(rep(2, 128), 4, 1, 2))
		e := l.pools().PGs[0]
		a, m := e.Members[0].Disk, e.Members[1].Disk
		l.mark(MarkDown, a)
		l.step(t, l.inputs(0))
		l.now += DegradedRefillAfter / 2
		l.mark(MarkDown, m)
		if raise {
			l.pool.MinLive = 2
		}
		l.step(t, l.inputs(0))
		l.now += DegradedRefillAfter
		l.step(t, l.inputs(0))
		got := disksOf(&l.pools().PGs[0])
		if slices.Contains(got, a) || !slices.Contains(got, m) {
			t.Fatalf("min_live raised %v: PG 0 went from %v to %v: A refilled and M kept expected", raise, e.Members, got)
		}
	}
}

// Refill that finds no eligible disk keeps the failure-domain rule and leaves the member marked down; its PG raises Stuck after 60 s of copying nothing, never CannotReprotect. A disk failure on a node with a second disk refills there after 10 minutes.
func TestRefillWithNoEligibleDisk(t *testing.T) {
	l := placed(t, nodesOf(rep(3, 128), 3, 1, 2))
	node := l.onNode(0)
	l.mark(MarkDown, node...)
	l.step(t, l.inputs(0))
	start := l.now
	l.now += 60 * minute
	l.step(t, l.inputs(allMoves))
	var copying []Copying
	for i, e := range l.pools().PGs {
		if !slices.Contains(e.Members, Member{Disk: node[0], Mark: MarkDown}) {
			t.Fatalf("PG %d: %v", i, e.Members)
		}
		copying = append(copying, Copying{PG: uint32(i), Since: start})
	}
	var c Conditions
	c.Watch(start+StuckAfter-1, copying)
	if len(c.Stuck) != 0 {
		t.Fatal("Stuck before 60 s")
	}
	c.Watch(start+StuckAfter, copying)
	f := l.figures()
	c.Update(&f, false)
	if len(c.Stuck) != 128 || c.CannotReprotect {
		t.Fatalf("Stuck on %d PGs, CannotReprotect %v", len(c.Stuck), c.CannotReprotect)
	}

	l = placed(t, nodesOf(rep(3, 128), 3, 2, 2))
	m, other := l.onNode(0)[0], l.onNode(0)[1]
	l.mark(MarkDown, m)
	l.step(t, l.inputs(0))
	l.now += RefillAfter
	l.step(t, l.inputs(0))
	if len(l.holding(m)) != 0 || !slices.Equal(l.holding(other), all(128)) {
		t.Fatal("a failed disk's PGs were not refilled onto its node's other disk")
	}
}
