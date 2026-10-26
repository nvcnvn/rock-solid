package chunkstore

import (
	"fmt"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

// randomLayout is the Testing strategy matrix: 3 or 5 nodes of 1 or 4 disks of mixed sizes, some holding another pool's data, under none, rep2 or rep3.
func randomLayout(r *core.Rand) *layout {
	replicas := int(r.Range(1, 3))
	nodes, perNode := 3+2*int(r.Below(2)), 1+3*int(r.Below(2))
	l := &layout{}
	for node := range nodes {
		for range perNode {
			l.grow(uint32(node), randomSize(r), 0)
		}
	}
	for i := range l.disks {
		if r.Chance(1, 4) {
			l.disks[i].Live = r.Below(l.disks[i].Blocks / 2)
		}
	}
	l.pool = rep(replicas, PGCountFor(len(l.disks), replicas))
	l.pool.MinLive = int(r.Range(1, uint64(replicas)))
	return l
}

func randomSize(r *core.Rand) float64 { return []float64{0.5, 1, 2, 4, 8}[r.Below(5)] }

// history is what the property test remembers of a run: every node a disk had; for each PG and disk the interval of the last table the disk left the PG in, and the interval since which it has been a live member; and for each PG the members that hold all its data, those it started with and those a recovered report covered that stayed live since, and the intervals its primary could report recovered, with their members.
type history struct {
	nodeOf      []uint32
	left        [][]diskAt
	liveSince   [][]diskAt
	complete    [][]uint32
	recoverable [][]PastInterval
	stats       *placementStats
}

type diskAt struct {
	disk     uint32
	interval uint32
}

type placementStats struct {
	runs, moves, bound, computations int
	worstReach                       float64
}

func TestPlacementKeepsItsRules(t *testing.T) {
	n := 40
	if testing.Short() {
		n = 8
	}
	stats := &placementStats{}
	seeds.Run(t, 0, n, func(seed uint64) { placementRun(t, seed, stats) })
	t.Logf("%d seeds: a disk added moved %d members against a lower bound of %d; available + held at worst %.2f%% under balanced; at most %d computations to settle",
		stats.runs, stats.moves, stats.bound, 100*stats.worstReach, stats.computations)
	// A single seed, or a few, can sit well above their bound; the 5% is a mean over at least 200 runs.
	if stats.runs >= 200 && 100*stats.moves > 105*stats.bound {
		t.Fatalf("disk additions over %d seeds moved %d members, over 5%% above the lower bound %d", stats.runs, stats.moves, stats.bound)
	}
}

// Seed 207 stalled outside the margins before balance had its second kind of move: every PG on the disks above their targets had a member on the node of the disk below its margin.
func TestPlacementSeed207(t *testing.T) { placementRun(t, 207, &placementStats{}) }

func placementRun(t *testing.T, seed uint64, stats *placementStats) {
	r := core.NewRand(seed)
	l := randomLayout(r)
	h := &history{stats: stats}
	h.next(t, l, l.inputs(allMoves))
	for range 30 {
		h.next(t, l, h.event(r, l))
	}
	h.heal(t, l)
	h.addDisk(t, r, l)
}

// event changes the layout at random and returns the next computation's inputs.
func (h *history) event(r *core.Rand, l *layout) *Inputs {
	moves := []int{0, 1, 5, allMoves}[r.Below(4)]
	in := l.inputs(moves)
	pt := l.pools()
	pick := func() *Disk { return &l.disks[r.Below(uint64(len(l.disks)))] }
	nodeDisks := func(node uint32) []int {
		var ks []int
		for k, d := range l.disks {
			if d.Node == node {
				ks = append(ks, k)
			}
		}
		return ks
	}
	mark := func(node uint32, m Mark) {
		for _, k := range nodeDisks(node) {
			if l.disks[k].Mark != m && m == MarkDown {
				l.disks[k].DownSince = l.now
			}
			l.disks[k].Mark = m
		}
	}
	switch r.Below(14) {
	case 0:
		l.grow(pick().Node, randomSize(r), 0)
	case 1:
		if d := pick(); len(nodeDisks(d.Node)) > 1 {
			l.disks = slices.DeleteFunc(l.disks, func(x Disk) bool { return x.ID == d.ID })
		}
	case 2:
		if d := pick(); d.Mark != MarkDown {
			d.Mark, d.DownSince = MarkDown, l.now
		}
	case 3:
		pick().Mark = MarkUp
	case 4:
		mark(pick().Node, MarkDown)
	case 5:
		mark(pick().Node, MarkUp)
	case 6:
		mark(pick().Node, MarkMaint)
	case 7:
		if d := pick(); len(nodeDisks(d.Node)) > 1 || d.Drain {
			d.Drain = !d.Drain
		}
	case 8:
		l.now += core.Tick(r.Below(uint64(15 * 60_000_000)))
	case 9:
		for range r.Range(1, 20) {
			i := r.Below(uint64(len(pt.PGs)))
			in.Asks = append(in.Asks, PGInterval{core.PGID{Pool: 1, Index: uint32(i)}, pt.PGs[i].Interval})
		}
	case 10:
		for i, e := range pt.PGs {
			if r.Chance(1, 2) {
				in.Activations = append(in.Activations, PGInterval{pg(i), e.Interval})
				if recovers(e.Members) {
					in.Recovered = append(in.Recovered, PGInterval{pg(i), e.Interval})
				}
			}
			if past := h.recoverable[i]; len(past) > 0 && r.Chance(1, 4) {
				in.Recovered = append(in.Recovered, PGInterval{pg(i), past[r.Below(uint64(len(past)))].Interval})
			}
		}
	case 11:
		most := l.pool.Replicas
		if !slices.ContainsFunc(l.disks, func(d Disk) bool { return d.Mark != MarkUp || d.Drain }) {
			nodes := 0
			for _, d := range l.disks {
				nodes = max(nodes, int(d.Node)+1)
			}
			most = min(3, nodes)
		}
		l.pool.Replicas = int(r.Range(1, uint64(most)))
		l.pool.MinLive = min(l.pool.MinLive, l.pool.Replicas)
	case 12:
		l.pool.MinLive = int(r.Range(1, uint64(l.pool.Replicas)))
	case 13:
		if l.pool.PGCount < 2048 && r.Chance(1, 3) {
			l.pool.PGCount *= 2
		} else {
			i := r.Below(uint64(len(pt.PGs)))
			var disks []uint32
			for _, m := range pt.PGs[i].Members {
				disks = append(disks, m.Disk)
			}
			slices.Reverse(disks)
			in.Edits = append(in.Edits, Edit{PG: core.PGID{Pool: 1, Index: uint32(i)}, Disks: disks, Pinned: r.Chance(1, 2)})
		}
	}
	in.Now, in.Pools = l.now, l.inputs(moves).Pools
	return in
}

// next computes the next table from in and checks it against the previous one.
func (h *history) next(t *testing.T, l *layout, in *Inputs) {
	t.Helper()
	for _, d := range l.disks {
		for uint32(len(h.nodeOf)) <= d.ID {
			h.nodeOf = append(h.nodeOf, 0)
		}
		h.nodeOf[d.ID] = d.Node
	}
	prev := l.table
	l.step(t, in)
	if err := h.check(&prev, l, in); err != nil {
		t.Fatalf("table %d: %v", l.table.Epoch, err)
	}
}

// check holds every table to the rules: the pool's width and at most one leaving member beside it, more members only beside a leaving one after replicas went down and never more than the table before, at most perDomain members per domain but a leaving one, live members kept when replicas go down, an interval raised by one exactly when the rules say, a member removed and added back in a new interval, a live leaving member removed exactly in the first table after a recovered report for its current interval that marks no other member down or in maintenance, an entry recovered exactly while its interval has had such a report, a move only from a recovered interval, and no live member that holds all the PG's data removed unless the width still does.
func (h *history) check(prev *Table, l *layout, in *Inputs) error {
	p, pt := l.pool, l.pools()
	var old *PoolTable
	if len(prev.Pools) > 0 {
		old = &prev.Pools[0]
	}
	split := old != nil && len(old.PGs) != len(pt.PGs)
	if split {
		h.left, h.recoverable = nil, nil
		complete, liveSince := make([][]uint32, len(pt.PGs)), make([][]diskAt, len(pt.PGs))
		for i := range complete {
			complete[i], liveSince[i] = h.complete[i/2], h.liveSince[i/2]
		}
		h.complete, h.liveSince = complete, liveSince
	}
	for uint32(len(h.left)) < p.PGCount {
		h.left = append(h.left, nil)
		h.recoverable = append(h.recoverable, nil)
	}
	for len(h.complete) < len(pt.PGs) {
		h.complete = append(h.complete, nil)
		h.liveSince = append(h.liveSince, nil)
	}
	isLive := func(disk uint32) bool {
		return slices.ContainsFunc(l.disks, func(d Disk) bool { return d.ID == disk && d.Mark != MarkDown })
	}
	for i := range pt.PGs {
		e := &pt.PGs[i]
		leaving := leavingIn(e.Members)
		var b PGEntry
		if old != nil {
			b = old.PGs[i*len(old.PGs)/len(pt.PGs)]
		}
		n, surplus := len(e.Members)-leaving, len(b.Members)-leavingIn(b.Members)
		if leaving > 1 || n < p.width() || n > p.width() && (leaving == 0 || n > surplus) {
			return fmt.Errorf("PG %d at interval %d has members %v for a width of %d", i, e.Interval, e.Members, p.width())
		}
		perDomain := map[uint32]int{}
		for j, m := range e.Members {
			if slices.ContainsFunc(e.Members[:j], func(o Member) bool { return o.Disk == m.Disk }) {
				return fmt.Errorf("PG %d has disk %d twice", i, m.Disk)
			}
			if !m.Leaving && slices.ContainsFunc(l.disks, func(d Disk) bool { return d.ID == m.Disk }) {
				if perDomain[h.nodeOf[m.Disk]]++; perDomain[h.nodeOf[m.Disk]] > p.perDomain() {
					return fmt.Errorf("PG %d has %d members on node %d", i, perDomain[h.nodeOf[m.Disk]], h.nodeOf[m.Disk])
				}
			}
		}
		raise := old == nil || split || int(old.Replicas) != p.Replicas || int(old.MinLive) != p.MinLive ||
			!slices.Equal(b.Members, e.Members) ||
			slices.Contains(in.Asks, PGInterval{core.PGID{Pool: 1, Index: uint32(i)}, b.Interval})
		want := b.Interval
		if raise {
			want++
		}
		if e.Interval != want {
			return fmt.Errorf("PG %d at interval %d after %d, want %d (members %v after %v)", i, e.Interval, b.Interval, want, e.Members, b.Members)
		}
		if old != nil && !split && p.width() < int(old.Replicas) && live(e.Members) < min(live(b.Members), p.width()) {
			return fmt.Errorf("PG %d narrowed from %v to %v, dropping a live member", i, b.Members, e.Members)
		}
		if e.Activated < b.Activated || slices.ContainsFunc(e.Past, func(pi PastInterval) bool { return pi.Interval < e.Activated }) {
			return fmt.Errorf("PG %d activated %d after %d, past %v", i, e.Activated, b.Activated, e.Past)
		}
		if err := h.checkCopies(i, old, &b, e, l, in, isLive); err != nil {
			return err
		}
		var liveSince []diskAt
		for _, m := range e.Members {
			if m.Mark == MarkDown {
				continue
			}
			since := e.Interval
			wasLive := slices.ContainsFunc(b.Members, func(o Member) bool { return o.Disk == m.Disk && o.Mark != MarkDown })
			if j := slices.IndexFunc(h.liveSince[i], func(s diskAt) bool { return s.disk == m.Disk }); j >= 0 && wasLive {
				since = h.liveSince[i][j].interval
			}
			liveSince = append(liveSince, diskAt{m.Disk, since})
		}
		h.liveSince[i] = liveSince
		if recovers(e.Members) && !slices.ContainsFunc(h.recoverable[i], func(pi PastInterval) bool { return pi.Interval == e.Interval }) {
			h.recoverable[i] = append(h.recoverable[i], PastInterval{Interval: e.Interval, Members: e.Members})
		}
		for _, m := range b.Members {
			if !slices.ContainsFunc(e.Members, func(o Member) bool { return o.Disk == m.Disk }) {
				h.left[i] = append(h.left[i], diskAt{m.Disk, e.Interval})
			}
		}
		for _, m := range e.Members {
			for _, la := range h.left[i] {
				if la.disk == m.Disk && e.Interval <= la.interval {
					return fmt.Errorf("PG %d took disk %d back at interval %d, the interval it left in", i, m.Disk, e.Interval)
				}
			}
		}
	}
	return nil
}

// checkCopies holds PG i's step from b to e to the move rules, and keeps the members that hold all of its data. An edit may remove any member.
func (h *history) checkCopies(i int, old *PoolTable, b, e *PGEntry, l *layout, in *Inputs, isLive func(uint32) bool) error {
	disks := disksOf(e)
	if old == nil {
		h.complete[i] = disks
		return nil
	}
	edited := slices.ContainsFunc(in.Edits, func(ed Edit) bool { return ed.PG == pg(i) })
	due := false
	for _, r := range in.Recovered {
		if r.PG != pg(i) {
			continue
		}
		due = due || b.leaving() >= 0 && r.Interval == b.Interval && !slices.ContainsFunc(b.Members, func(m Member) bool {
			return !m.Leaving && !slices.ContainsFunc(l.disks, func(d Disk) bool { return d.ID == m.Disk && d.Mark == MarkUp })
		})
		k := slices.IndexFunc(h.recoverable[i], func(pi PastInterval) bool { return pi.Interval == r.Interval })
		if k < 0 {
			return fmt.Errorf("PG %d reported recovered at interval %d, which never was", i, r.Interval)
		}
		for _, m := range h.recoverable[i][k].Members {
			covered := slices.ContainsFunc(h.liveSince[i], func(s diskAt) bool { return s.disk == m.Disk && s.interval <= r.Interval })
			if !m.Leaving && covered && !slices.Contains(h.complete[i], m.Disk) {
				h.complete[i] = append(h.complete[i], m.Disk)
			}
		}
	}
	held := 0
	for _, d := range disks {
		if slices.Contains(h.complete[i], d) {
			held++
		}
	}
	for _, m := range b.Members {
		if slices.Contains(disks, m.Disk) || edited || !isLive(m.Disk) {
			continue
		}
		if m.Leaving && !due {
			return fmt.Errorf("PG %d removed leaving member %d with no recovered report for interval %d", i, m.Disk, b.Interval)
		}
		if slices.Contains(h.complete[i], m.Disk) && held < l.pool.width() {
			return fmt.Errorf("PG %d removed live member %d, which holds all its data, leaving %d such of %v", i, m.Disk, held, e.Members)
		}
	}
	reported := slices.Contains(in.Recovered, PGInterval{pg(i), b.Interval}) && !slices.ContainsFunc(b.Members, func(m Member) bool {
		return !slices.ContainsFunc(l.disks, func(d Disk) bool { return d.ID == m.Disk && d.Mark == MarkUp })
	})
	if e.Recovered != (e.Interval == b.Interval && (b.Recovered || reported)) {
		return fmt.Errorf("PG %d at interval %d after %d, recovered %v after %v, reported %v", i, e.Interval, b.Interval, e.Recovered, b.Recovered, reported)
	}
	if j := e.leaving(); j >= 0 && slices.ContainsFunc(e.Members, func(m Member) bool {
		return !slices.ContainsFunc(b.Members, func(o Member) bool { return o.Disk == m.Disk })
	}) &&
		b.leaving() < 0 && !b.Recovered && !reported {
		return fmt.Errorf("PG %d moved %v to %v from an interval with no recovered report", i, b.Members, e.Members)
	}
	if j := b.leaving(); due && j >= 0 && slices.Contains(e.Members, b.Members[j]) {
		return fmt.Errorf("PG %d kept leaving member %d after a recovered report", i, b.Members[j].Disk)
	}
	h.complete[i] = slices.DeleteFunc(h.complete[i], func(d uint32) bool { return !slices.Contains(disks, d) || !isLive(d) })
	return nil
}

// heal brings every disk up, undrained and unpinned, and computes tables, each once the last one's moves have copied, until one proposes no move and leaves no PG moving: every disk is then within its margin and available + held within the largest margin of balanced.
func (h *history) heal(t *testing.T, l *layout) {
	t.Helper()
	for i := range l.disks {
		l.disks[i].Mark, l.disks[i].Drain = MarkUp, false
	}
	in := l.inputs(0)
	for i, e := range l.pools().PGs {
		if e.Pinned {
			disks := make([]uint32, len(e.Members))
			for j, m := range e.Members {
				disks[j] = m.Disk
			}
			in.Edits = append(in.Edits, Edit{PG: core.PGID{Pool: 1, Index: uint32(i)}, Disks: disks})
		}
	}
	h.next(t, l, in)
	h.settle(t, l)
}

func (h *history) settle(t *testing.T, l *layout) int {
	t.Helper()
	moves := 0
	for c := 0; ; c++ {
		l.hold()
		prev, in := l.table, l.reportAll(allMoves)
		n := l.step(t, in)
		if err := h.check(&prev, l, in); err != nil {
			t.Fatalf("table %d: %v", l.table.Epoch, err)
		}
		moves += n
		if l.settled(n) {
			h.stats.computations = max(h.stats.computations, c+1)
			break
		}
		if c == 20 {
			t.Fatalf("a computation once the moves copied proposed %d more", n)
		}
	}
	if ok, s := l.converged(); !ok {
		t.Fatalf("stopped outside the margins:%s", s)
	}
	f := l.figures()
	largest := 0.0
	for _, d := range f.Disks {
		if d.Target > 0 {
			largest = max(largest, margin(d.Target)/d.Target)
		}
	}
	reach := float64(f.Available + f.Held)
	if reach > float64(f.Balanced) || reach < float64(f.Balanced)/(1+largest) {
		t.Fatalf("available + held %v against balanced %d, largest margin %.3f", reach, f.Balanced, largest)
	}
	h.stats.worstReach = max(h.stats.worstReach, 1-reach/float64(f.Balanced))
	return moves
}

// addDisk adds a disk to a converged pool holding data: the moves stay near the minimum, and the computation once they have copied proposes none.
func (h *history) addDisk(t *testing.T, r *core.Rand, l *layout) {
	t.Helper()
	f := l.figures()
	l.held = r.Below(f.Available + 1)
	l.grow(l.disks[r.Below(uint64(len(l.disks)))].Node, randomSize(r), 0)
	h.next(t, l, l.inputs(0))
	bound, margins := l.lowerBound()
	moves := h.settle(t, l)
	if 4*(moves-bound) > bound+4*margins {
		t.Fatalf("a disk added moved %d members, over a quarter above the lower bound %d plus the margins %d", moves, bound, margins)
	}
	h.stats.runs++
	h.stats.moves += moves
	h.stats.bound += bound
}
