package chunkstore

import (
	"math"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// Refill puts each missing member on the eligible disk furthest below its target.
func TestRefillFillsTheDiskFurthestBelow(t *testing.T) {
	l := placed(t, newLayout(rep(1, 128), 2, 2, 2))
	l.grow(3, 2, 0)
	l.step(t, l.inputs(0))
	l.remove(0)
	l.step(t, l.inputs(0))
	loads := l.loads()
	if slices.Max(loads)-slices.Min(loads) > 1 {
		t.Fatalf("refilled to loads %v", loads)
	}
}

// Balance moves from the disk furthest above its target even inside its own margin, the lowest ID of two tied, and stops once every disk is within its margin.
func TestBalanceTakesFromTheFurthestAbove(t *testing.T) {
	l := newLayout(rep(1, 128), 2, 2, 2)
	pt := PoolTable{ID: 1, Replicas: 1, MinLive: 1, PGs: make([]PGEntry, 128)}
	for i := range pt.PGs {
		disk := uint32(2)
		if i < 88 {
			disk = uint32(i % 2)
		}
		pt.PGs[i] = PGEntry{Members: []Member{{Disk: disk}}, Interval: 1, Activated: 1}
	}
	l.table = Table{Epoch: 1, Pools: []PoolTable{pt}}
	if moves := l.step(t, l.inputs(allMoves)); moves != 1 || !slices.Equal(l.loads(), []uint32{43, 44, 41}) {
		t.Fatalf("%d moves to loads %v", moves, l.loads())
	}
}

// An operator's edit that names more disks than the width, a disk twice or two disks in one domain is ignored, pin and all.
func TestEditsKeepTheRules(t *testing.T) {
	l := placed(t, nodesOf(rep(3, 16), 4, 2, 2))
	e := l.pools().PGs[0]
	a, b, c := e.Members[0].Disk, e.Members[1].Disk, e.Members[2].Disk
	sibling := l.onNode(l.disk(a).Node)[0]
	if sibling == a {
		sibling = l.onNode(l.disk(a).Node)[1]
	}
	other := l.onNode(l.freeNode(&e))[0]
	for _, row := range []struct {
		name  string
		disks []uint32
		want  []uint32
	}{
		{"a disk twice", []uint32{a, a, b}, []uint32{a, b, c}},
		{"two disks on one node", []uint32{a, sibling, b}, []uint32{a, b, c}},
		{"four disks for rep3", []uint32{a, b, c, other}, []uint32{a, b, c}},
		{"another node's disk in place of C", []uint32{a, b, other}, []uint32{a, b, other}},
	} {
		in := l.inputs(0)
		in.Edits = []Edit{{PG: pg(0), Disks: row.disks, Pinned: true}}
		l.step(t, in)
		got := l.pools().PGs[0]
		if accepted := slices.Equal(row.want, row.disks); !slices.Equal(disksOf(&got), row.want) || got.Pinned != accepted {
			t.Errorf("%s: members %v, pinned %v", row.name, disksOf(&got), got.Pinned)
		}
	}
}

// Lowering replicas marks the member it would drop, the last down one or else the last, leaving, so that the move's removal takes it once the others have recovered; it deletes at once only a member refill would replace anyway, and a PG keeps any further surplus until that removal.
func TestLoweringReplicasKeepsLiveMembers(t *testing.T) {
	const a, b, c = 0, 1, 2
	start := func() *layout {
		l := newLayout(rep(3, 1), 2, 2, 2)
		l.table = Table{Epoch: 1, Pools: []PoolTable{{ID: 1, Replicas: 3, MinLive: 1, PGs: []PGEntry{{Members: []Member{{Disk: a}, {Disk: b}, {Disk: c}}, Interval: 1, Activated: 1}}}}}
		l.pool.MinLive = 1
		return l
	}
	for _, row := range []struct {
		name     string
		down     []uint32
		lost     []uint32
		replicas int
		want     []Member
	}{
		{"all up, rep3 to rep2", nil, nil, 2, []Member{{Disk: a}, {Disk: b}, {Disk: c, Leaving: true}}},
		{"B down, rep3 to rep2", []uint32{b}, nil, 2, []Member{{Disk: a}, {Disk: b, Mark: MarkDown, Leaving: true}, {Disk: c}}},
		{"A and C down, rep3 to rep1", []uint32{a, c}, nil, 1, []Member{{Disk: a, Mark: MarkDown}, {Disk: b}, {Disk: c, Mark: MarkDown, Leaving: true}}},
		{"C's disk gone, rep3 to rep2", nil, []uint32{c}, 2, []Member{{Disk: a}, {Disk: b}}},
		{"C's disk gone, rep3 to rep1", nil, []uint32{c}, 1, []Member{{Disk: a}, {Disk: b, Leaving: true}}},
	} {
		l := start()
		l.mark(MarkDown, row.down...)
		for _, d := range row.lost {
			l.remove(d)
		}
		l.pool.Replicas = row.replicas
		l.step(t, l.inputs(0))
		if got := l.pools().PGs[0].Members; !slices.Equal(got, row.want) {
			t.Errorf("%s: members %v, want %v", row.name, got, row.want)
		}
	}
	// Only B holds every chunk, A and C being refill members still recovering. A primary reports
	// recovered only once every member but a leaving one is up and has copied from an up member
	// that holds the chunks; the down members come back up after three tables.
	for _, row := range []struct {
		name string
		down []uint32
	}{{"all up", nil}, {"A down", []uint32{a}}, {"B down", []uint32{b}}, {"C down", []uint32{c}}} {
		l := start()
		complete := []uint32{b}
		l.mark(MarkDown, row.down...)
		l.pool.Replicas = 1
		for round := range 6 {
			if round == 3 {
				l.mark(MarkUp, row.down...)
			}
			e := l.pools().PGs[0]
			in := l.inputs(0)
			source := slices.ContainsFunc(e.Members, func(m Member) bool { return m.Mark == MarkUp && slices.Contains(complete, m.Disk) })
			if round > 0 && source && recovers(e.Members) {
				in.Recovered = []PGInterval{{pg(0), e.Interval}}
				for _, m := range e.Members {
					if !m.Leaving && !slices.Contains(complete, m.Disk) {
						complete = append(complete, m.Disk)
					}
				}
			}
			l.step(t, in)
			if got := l.pools().PGs[0].Members; !slices.ContainsFunc(got, func(m Member) bool { return slices.Contains(complete, m.Disk) }) {
				t.Fatalf("%s, table %d: members %v, none of them holding every chunk of %v", row.name, round, got, complete)
			}
		}
		if got := l.pools().PGs[0].Members; len(got) != 1 || got[0].Leaving || !slices.Contains(complete, got[0].Disk) {
			t.Errorf("%s: ended with members %v, complete %v", row.name, got, complete)
		}
	}
}

// A disk's fill margin is max(5% of its target, 1 member): a 0.5 TB disk beside 8 TB disks has a target of 10 to 20 members and a margin of 1.
func TestFillMargin(t *testing.T) {
	if margin(100) != 5 || margin(15) != 1 {
		t.Fatalf("margins %v and %v", margin(100), margin(15))
	}
	l := placed(t, newLayout(rep(3, PGCountFor(4, 3)), 8, 8, 8, 0.5))
	target := l.pool.targets(l.disks, 0)
	if target[3] < 10 || target[3] > 20 || margin(target[3]) != 1 {
		t.Fatalf("the 0.5 TB disk's target %.1f", target[3])
	}
	if ok, s := l.converged(); !ok {
		t.Fatalf("not within margins:%s", s)
	}
}

// A disk's target is its capacity for the pool: write_limit less other pools' data, so the pool's own data does not move it.
func TestTargetsFollowCapacity(t *testing.T) {
	l := newLayout(rep(3, 256), 2, 2, 2, 2)
	l.disks[0].Live = tb(1)
	before := l.pool.targets(l.disks, 0)
	if before[0] >= before[1] {
		t.Fatalf("another pool's data leaves targets %v", before)
	}
	for i := range l.disks {
		l.disks[i].Own = tb(0.3)
		l.disks[i].Live += tb(0.3)
	}
	if after := l.pool.targets(l.disks, 0); !slices.Equal(before, after) {
		t.Fatalf("the pool's own data moves targets from %v to %v", before, after)
	}
}

// The capacity figures list placement's targets, counting a disk added with no member yet and none for a draining disk.
func TestFiguresListPlacementTargets(t *testing.T) {
	l := placed(t, nodesOf(rep(3, 256), 4, 2, 2))
	l.disk(l.onNode(1)[0]).Drain = true
	l.hold()
	l.grow(2, 4, 0)
	f := l.pool.Capacity(l.disks, 0, l.now)
	pl := l.placer()
	for _, d := range f.Disks {
		if want := pl.target[pl.find(d.ID)]; d.Target != want {
			t.Errorf("disk %d: figures' target %.2f, placement's %.2f", d.ID, d.Target, want)
		}
	}
	if len(f.Disks) != len(l.disks)-1 {
		t.Fatalf("figures list %d disks", len(f.Disks))
	}
}

// After a disk is added to a pool holding data, the second computation, once the first one's moves have copied, proposes no move.
func TestMovesAfterADiskAdded(t *testing.T) {
	c6 := capacityLayouts()["C6"]
	c2 := capacityLayouts()["C2"]
	one := newLayout(rep(1, 128), 2)
	for _, c := range []struct {
		name  string
		l     *layout
		held  float64
		node  uint32
		size  float64
		moves int
	}{
		{"none, one 2 TB node holding 1 TB, a 2 TB node added", one, 1, 1, 2, 61},
		{"C6 holding 0.8 TB, a 2 TB disk added to node B", c6, 0.8, 1, 2, 62},
		{"C2 holding 1.5 TB, a 4 TB disk added to node D", c2, 1.5, 3, 4, 98},
	} {
		l := placed(t, c.l)
		l.held = tb(c.held)
		l.grow(c.node, c.size, 0)
		moves, computations := l.settle(t)
		if moves != c.moves || computations != 2 {
			t.Errorf("%s: %d moves over %d computations", c.name, moves, computations)
		}
	}
}

// With capped targets the loop converges on C1 to C6, reaching within 1% of balanced; with targets by size alone it never reaches its margins on C1, C2, C3 and C6, and still stops.
func TestConvergenceOnCapacityLayouts(t *testing.T) {
	for _, name := range []string{"C1", "C2", "C3", "C4", "C5", "C6"} {
		l := placed(t, capacityLayouts()[name])
		if ok, s := l.converged(); !ok {
			t.Errorf("%s stopped outside its margins:%s", name, s)
		}
		f := l.figures()
		if 100*(f.Available+f.Held) < 99*f.Balanced {
			t.Errorf("%s: available + held %d against balanced %d", name, f.Available+f.Held, f.Balanced)
		}
		if name == "C4" && f.Balanced != tb(4.2) {
			t.Errorf("C4 balanced %d, not 4.2 TB", f.Balanced)
		}
	}
	for _, c := range []struct {
		name  string
		worst float64
	}{{"C1", 0.93}, {"C2", 0.23}, {"C3", 2}, {"C6", 0.5}} {
		l := capacityLayouts()[c.name]
		p := l.pool
		l.table = Table{Pools: []PoolTable{{}}}
		pl := l.placer()
		pl.entries, pl.target = make([]PGEntry, p.PGCount), nil
		var size float64
		for _, d := range pl.disks {
			size += float64(d.Blocks)
		}
		for _, d := range pl.disks {
			pl.target = append(pl.target, float64(d.Blocks)/size*float64(p.PGCount)*float64(p.width()))
		}
		pl.load = make([]int, len(pl.disks))
		for i := range pl.entries {
			pl.refill(&pl.entries[i], &PGEntry{})
		}
		pl.balance(allMoves)
		worst := 0.0
		for k, tg := range pl.target {
			worst = max(worst, math.Abs(float64(pl.load[k])-tg)/tg)
		}
		if pl.converged() || math.Abs(worst-c.worst) > 0.01 {
			t.Errorf("%s by size: worst disk %.0f%% off its target", c.name, 100*worst)
		}
	}
}

// Balance never moves a member of a pinned PG, though refill replaces a missing one; a drained disk's members move off while they stay up, and it holds none, a leaving one included, once the moves have recovered.
func TestPinAndDrain(t *testing.T) {
	l := placed(t, nodesOf(rep(3, 256), 4, 1, 2))
	in := l.inputs(0)
	var pinned []int
	for i := range l.pools().PGs {
		if i%2 == 0 {
			in.Edits = append(in.Edits, Edit{PG: pg(i), Disks: disksOf(&l.pools().PGs[i]), Pinned: true})
			pinned = append(pinned, i)
		}
	}
	samePGs(t, "pinning without a change of members", l.raised(t, in), nil)
	before := l.table
	l.grow(0, 2, 0)
	l.settle(t)
	for _, i := range pinned {
		if !slices.Equal(l.pools().PGs[i].Members, before.Pools[0].PGs[i].Members) || !l.pools().PGs[i].Pinned {
			t.Fatalf("balance moved pinned PG %d", i)
		}
	}
	d := l.onNode(1)[0]
	l.remove(d)
	l.step(t, l.inputs(0))
	if len(l.holding(d)) != 0 {
		t.Fatal("refill left members of pinned PGs on a lost disk")
	}

	l = placed(t, nodesOf(rep(3, 256), 4, 2, 2))
	drained := l.onNode(2)[1]
	l.disk(drained).Drain = true
	l.step(t, l.inputs(10))
	for _, i := range l.holding(drained) {
		if slices.Contains(l.pools().PGs[i].Members, Member{Disk: drained, Mark: MarkDown}) {
			t.Fatalf("a draining disk's member in PG %d was marked down", i)
		}
	}
	l.settle(t)
	if n := len(l.holding(drained)); n != 0 {
		t.Fatalf("a drained disk keeps %d members", n)
	}
}

// The study's test U: 200 random layouts each for rep2 and rep3, 3 to 6 nodes of 1 or 2 disks of 0.5 to 8 TB, some partly used by other pools, and one disk added to a random node of a converged table. No addition raises Unbalanced at any fill, and Unbalanced keeps its value while the computation proposes moves.
func TestAddedDiskNeverUnbalances(t *testing.T) {
	r := core.NewRand(42)
	worst := 0.0
	for _, replicas := range []int{2, 3} {
		for range 200 {
			l := &layout{}
			nodes := int(r.Range(3, 6))
			for node := range nodes {
				for range r.Range(1, 2) {
					size := []float64{0.5, 1, 2, 4, 8}[r.Below(5)]
					l.grow(uint32(node), size, size*[]float64{0, 0, 0, 0, 0.2, 0.5}[r.Below(6)])
				}
			}
			l.pool = rep(replicas, PGCountFor(len(l.disks), replicas))
			placed(t, l)
			l.grow(uint32(r.Below(uint64(nodes))), []float64{1, 2, 4, 8}[r.Below(4)], 0)
			var c Conditions
			c.Unbalanced = true
			f := l.figures()
			if moves := l.step(t, l.inputs(allMoves)); moves > 0 {
				c.Update(&f, false)
				if !c.Unbalanced {
					t.Fatal("Unbalanced changed while placement proposed moves")
				}
			}
			c.Unbalanced = false
			for _, fill := range []float64{0, 0.5, 0.7, 0.9} {
				l.held = uint64(fill * float64(f.Balanced))
				l.settle(t)
				if len(l.moving()) != 0 {
					t.Fatalf("PGs %v still move once placement proposes none", l.moving())
				}
				f = l.figures()
				c.Update(&f, true)
				if c.Unbalanced {
					t.Fatalf("rep%d with a disk added, held %.0f%%: Unbalanced", replicas, 100*fill)
				}
				worst = max(worst, 1-float64(f.Available+f.Held)/float64(f.Balanced))
			}
		}
	}
	t.Logf("at worst available + held %.1f%% under balanced", 100*worst)
}

// moving lists the PGs with a leaving member.
func (l *layout) moving() []int {
	var pgs []int
	for i, e := range l.pools().PGs {
		if e.leaving() >= 0 {
			pgs = append(pgs, i)
		}
	}
	return pgs
}

// A move adds the new member and marks the old one leaving; a later table removes every leaving member a recovered report covers, starting an interval and counting no move. A leaving member missing for refill_after is dropped, one a missing member's refill can keep stays in its place, and on 3 nodes a move within a node keeps the failure-domain rule but for the leaving member. A report for an interval before the PG's current one removes nothing, nor does one in a table that marks another member down or in maintenance.
func TestMovesAddThenRemove(t *testing.T) {
	t.Run("removals share one table", func(t *testing.T) {
		l := placed(t, nodesOf(rep(3, 256), 4, 2, 2))
		l.grow(1, 2, 0)
		moves := l.step(t, l.inputs(allMoves))
		pgs := l.moving()
		if moves == 0 || len(pgs) != moves {
			t.Fatalf("%d moves left %d PGs moving", moves, len(pgs))
		}
		in := l.inputs(0)
		for _, i := range pgs {
			in.Recovered = append(in.Recovered, PGInterval{pg(i), l.pools().PGs[i].Interval})
		}
		prev := l.table
		samePGs(t, "every recovered move", l.raised(t, in), pgs)
		if len(l.moving()) != 0 {
			t.Fatalf("PGs %v still move", l.moving())
		}
		if _, n := Next(&prev, in); n != 0 {
			t.Fatalf("the removals counted %d moves", n)
		}
	})
	t.Run("leaving member down", func(t *testing.T) {
		l := placed(t, nodesOf(rep(3, 256), 4, 2, 2))
		l.grow(1, 2, 0)
		l.step(t, l.inputs(1))
		i := l.moving()[0]
		e := l.pools().PGs[i]
		gone := e.Members[e.leaving()].Disk
		l.mark(MarkDown, gone)
		l.step(t, l.inputs(0))
		if got := l.pools().PGs[i]; got.leaving() < 0 || len(got.Members) != 4 {
			t.Fatalf("a leaving member just down: %v", got.Members)
		}
		l.now += RefillAfter
		l.step(t, l.inputs(0))
		want := slices.DeleteFunc(slices.Clone(e.Members), func(m Member) bool { return m.Leaving })
		if got := l.pools().PGs[i]; !slices.Equal(disksOf(&got), disksOf(&PGEntry{Members: want})) {
			t.Fatalf("a leaving member down for refill_after: %v, from %v", got.Members, e.Members)
		}
	})
	t.Run("refill keeps the leaving member", func(t *testing.T) {
		l := placed(t, nodesOf(rep(3, 256), 4, 2, 2))
		l.grow(1, 2, 0)
		l.step(t, l.inputs(allMoves))
		i := slices.IndexFunc(l.moving(), func(i int) bool {
			e := l.pools().PGs[i]
			return e.leaving() != 0 && l.disk(e.Members[e.leaving()].Disk).Node != l.disk(e.Members[3].Disk).Node
		})
		if i < 0 {
			t.Fatal("no move between nodes")
		}
		i = l.moving()[i]
		e := l.pools().PGs[i]
		l.remove(e.Members[0].Disk)
		l.step(t, l.inputs(0))
		got := l.pools().PGs[i]
		want := slices.Clone(e.Members[1:])
		want[e.leaving()-1].Leaving = false
		if !slices.Equal(got.Members, want) {
			t.Fatalf("a missing member of %v left %v", e.Members, got.Members)
		}
	})
	t.Run("within a node on 3 nodes", func(t *testing.T) {
		l := placed(t, nodesOf(rep(3, 128), 3, 1, 2))
		added := l.grow(0, 2, 0)
		l.step(t, l.inputs(allMoves))
		pgs := l.moving()
		if len(pgs) == 0 {
			t.Fatal("no move onto the added disk")
		}
		for _, i := range pgs {
			e := l.pools().PGs[i]
			if out := e.Members[e.leaving()].Disk; l.disk(out).Node != 0 || e.Members[3].Disk != added {
				t.Fatalf("PG %d moved within no node: %v", i, e.Members)
			}
		}
		l.settle(t)
		if ok, s := l.converged(); !ok || len(l.moving()) != 0 {
			t.Fatalf("PGs %v moving, converged %v:%s", l.moving(), ok, s)
		}
	})
	t.Run("reports across a split", func(t *testing.T) {
		l := placed(t, nodesOf(rep(3, 64), 4, 2, 2))
		l.grow(1, 2, 0)
		l.step(t, l.inputs(allMoves))
		if len(l.moving()) == 0 {
			t.Fatal("no move")
		}
		prev := l.table
		in := l.inputs(0)
		for i, e := range l.pools().PGs {
			in.Activations = append(in.Activations, PGInterval{pg(i), e.Interval})
			in.Recovered = append(in.Recovered, PGInterval{pg(i), e.Interval})
		}
		l.pool.PGCount = 128
		in.Pools = l.inputs(0).Pools
		l.step(t, in)
		for i, e := range l.pools().PGs {
			parent := prev.Pools[0].PGs[i/2]
			if !slices.Equal(e.Members, parent.Members) || e.Activated != parent.Activated {
				t.Fatalf("a report for PG %d before the split changed PG %d: %+v from %+v", i/2, i, e, parent)
			}
		}
	})
	for _, c := range []struct {
		name  string
		event func(t *testing.T, l *layout, disk uint32)
	}{
		{"a refill joins", func(t *testing.T, l *layout, disk uint32) { l.remove(disk) }},
		{"a member goes down and back up", func(t *testing.T, l *layout, disk uint32) {
			l.mark(MarkDown, disk)
			l.step(t, l.inputs(0))
			l.mark(MarkUp, disk)
		}},
		{"a member goes to maintenance and back up", func(t *testing.T, l *layout, disk uint32) {
			l.mark(MarkMaint, disk)
			l.step(t, l.inputs(0))
			l.mark(MarkUp, disk)
		}},
	} {
		t.Run(c.name+" after a recovered interval", func(t *testing.T) {
			l := placed(t, nodesOf(rep(3, 128), 3, 2, 2))
			l.grow(0, 2, 0)
			l.step(t, l.inputs(1))
			i := l.moving()[0]
			e := l.pools().PGs[i]
			j := slices.IndexFunc(e.Members, func(m Member) bool { return !m.Leaving && l.disk(m.Disk).Node != 0 })
			c.event(t, l, e.Members[j].Disk)
			l.step(t, l.inputs(0))
			in := l.inputs(0)
			in.Recovered = []PGInterval{{pg(i), e.Interval}}
			l.step(t, in)
			got := l.pools().PGs[i]
			if got.leaving() < 0 {
				t.Fatalf("a report for the add's interval %d, from before the member's copies, removed the leaving member of %v: %v", e.Interval, e.Members, got.Members)
			}
			in.Recovered = []PGInterval{{pg(i), got.Interval}}
			l.step(t, in)
			if got := l.pools().PGs[i]; got.leaving() >= 0 {
				t.Fatalf("a report for the current interval kept the leaving member: %v", got.Members)
			}
		})
	}
	for name, mark := range map[string]Mark{"down": MarkDown, "in maintenance": MarkMaint} {
		t.Run("a report in the table that marks the new member "+name+" on rep1", func(t *testing.T) {
			l := placed(t, nodesOf(rep(1, 64), 3, 1, 2))
			l.grow(0, 2, 0)
			l.step(t, l.inputs(1))
			i := l.moving()[0]
			e := l.pools().PGs[i]
			l.mark(mark, e.Members[1-e.leaving()].Disk)
			in := l.inputs(0)
			in.Activations = []PGInterval{{pg(i), e.Interval}}
			in.Recovered = []PGInterval{{pg(i), e.Interval}}
			l.step(t, in)
			if got := l.pools().PGs[i]; got.leaving() < 0 {
				t.Fatalf("a report for interval %d removed the leaving member of %v in the table that marks the other one: %v", e.Interval, e.Members, got.Members)
			}
		})
	}
}
