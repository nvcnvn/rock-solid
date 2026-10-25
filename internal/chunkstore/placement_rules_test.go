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

// Lowering replicas drops the last down member first, then the last member, so the live copies stay.
func TestLoweringReplicasKeepsLiveMembers(t *testing.T) {
	const a, b, c = 0, 1, 2
	for _, row := range []struct {
		name     string
		down     []uint32
		replicas int
		want     []uint32
	}{
		{"all up, rep3 to rep2", nil, 2, []uint32{a, b}},
		{"A and B down, rep3 to rep2", []uint32{a, b}, 2, []uint32{a, c}},
		{"B down, rep3 to rep2", []uint32{b}, 2, []uint32{a, c}},
		{"A and C down, rep3 to rep1", []uint32{a, c}, 1, []uint32{b}},
		{"all down, rep3 to rep1", []uint32{a, b, c}, 1, []uint32{a}},
	} {
		l := newLayout(rep(3, 1), 2, 2, 2)
		l.table = Table{Epoch: 1, Pools: []PoolTable{{ID: 1, Replicas: 3, MinLive: 1, PGs: []PGEntry{{Members: []Member{{Disk: a}, {Disk: b}, {Disk: c}}, Interval: 1, Activated: 1}}}}}
		l.mark(MarkDown, row.down...)
		l.pool.Replicas = row.replicas
		l.step(t, l.inputs(0))
		if got := disksOf(&l.pools().PGs[0]); !slices.Equal(got, row.want) {
			t.Errorf("%s: members %v, want %v", row.name, got, row.want)
		}
	}
}

// A disk's fill margin is max(5% of its target, 1 member): a 0.5 TB disk beside 8 TB disks has a target of 10 to 20 members and a margin of 1.
func TestFillMargin(t *testing.T) {
	if margin(100) != 5 || margin(15) != 1 {
		t.Fatalf("margins %v and %v", margin(100), margin(15))
	}
	l := placed(t, newLayout(rep(3, PGCountFor(4, 3)), 8, 8, 8, 0.5))
	target := l.pool.targets(l.disks, func(*Disk) bool { return false })
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
	none := func(*Disk) bool { return false }
	before := l.pool.targets(l.disks, none)
	if before[0] >= before[1] {
		t.Fatalf("another pool's data leaves targets %v", before)
	}
	for i := range l.disks {
		l.disks[i].Own = tb(0.3)
		l.disks[i].Live += tb(0.3)
	}
	if after := l.pool.targets(l.disks, none); !slices.Equal(before, after) {
		t.Fatalf("the pool's own data moves targets from %v to %v", before, after)
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

// Balance never moves a member of a pinned PG, though refill replaces a missing one; a drained disk's members move off while they stay up.
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

// The study's test U: 200 random layouts each for rep2 and rep3, 3 to 6 nodes of 1 or 2 disks of 0.5 to 8 TB, some partly used by other pools, and one disk added to a random node of a converged table. No addition raises Unbalanced at any fill, and Unbalanced keeps its value while the computation proposes moves or a PG lists past members.
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
