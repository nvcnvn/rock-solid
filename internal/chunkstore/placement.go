package chunkstore

import (
	"cmp"
	"fmt"
	"math"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// RefillAfter is refill_after: how long a member stays down before it is refilled onto another disk.
const RefillAfter = core.Tick(600_000_000)

// DegradedRefillAfter is degraded_refill_after: refill_after for a member of a PG below min_live.
const DegradedRefillAfter = core.Tick(60_000_000)

// Mark is a member's live mark; up and maintenance count as live.
type Mark uint8

// The live marks.
const (
	MarkUp Mark = iota
	MarkDown
	MarkMaint
)

// Member is one member of a PG: its disk, its live mark, and whether a move is taking it out once its replacement has recovered.
type Member struct {
	Disk    uint32
	Mark    Mark
	Leaving bool
}

// PastInterval is an interval of a PG since its last activated one, with that interval's members.
type PastInterval struct {
	Interval uint32
	Members  []Member
}

// PGEntry is a PG's entry in the placement table: its members with the primary first, its interval, its last activated interval, the intervals since, whether an operator pinned it, and whether its primary reported the interval recovered.
type PGEntry struct {
	Members   []Member
	Interval  uint32
	Activated uint32
	Past      []PastInterval
	Pinned    bool
	Recovered bool
}

// PoolTable is a pool's part of the placement table: the settings its intervals depend on, and its PGs.
type PoolTable struct {
	ID       core.PoolID
	Replicas uint8
	MinLive  uint8
	PGs      []PGEntry
}

// Table is the placement table: its epoch and its pools in ascending ID order.
type Table struct {
	Epoch uint64
	Pools []PoolTable
}

// PoolInputs is a pool as placement reads it: its settings and its disks, a member on any other disk having left the pool.
type PoolInputs struct {
	ID    core.PoolID
	Pool  Pool
	Disks []Disk
}

// PGInterval names one interval of a PG.
type PGInterval struct {
	PG       core.PGID
	Interval uint32
}

// Edit is an operator's edit of a PG: its members' disks in order, primary first, and whether balance leaves it alone.
type Edit struct {
	PG     core.PGID
	Disks  []uint32
	Pinned bool
}

// Inputs are what the next table is computed from beside the previous one: the pools, the PGs whose primary asks for a new interval, the activations and recoveries reported, operators' edits, and how many balance moves the table may make.
type Inputs struct {
	Now         core.Tick
	Pools       []PoolInputs
	Asks        []PGInterval
	Activations []PGInterval
	Recovered   []PGInterval
	Edits       []Edit
	Moves       int
}

// Next computes the table after prev: live marks, settings, splits, asks and activations, leaving members removed on a recovered report for their PG's current interval while its other members stay up, reports ignored in a pool it splits since they name the parent's PG numbers, every missing member refilled, then up to in.Moves balance moves. It reports the moves it made.
func Next(prev *Table, in *Inputs) (Table, int) {
	pools := slices.Clone(in.Pools)
	slices.SortFunc(pools, func(a, b PoolInputs) int { return cmp.Compare(a.ID, b.ID) })
	next := Table{Epoch: prev.Epoch + 1, Pools: make([]PoolTable, len(pools))}
	moves := 0
	for i := range pools {
		var old *PoolTable
		if j, ok := slices.BinarySearchFunc(prev.Pools, pools[i].ID, func(t PoolTable, id core.PoolID) int { return cmp.Compare(t.ID, id) }); ok {
			old = &prev.Pools[j]
		}
		var n int
		next.Pools[i], n = nextPool(old, &pools[i], in, moves)
		moves += n
	}
	return next, moves
}

// placer is one pool's computation: its disks by ascending ID with their targets and member counts.
type placer struct {
	pool    Pool
	now     core.Tick
	disks   []Disk
	target  []float64
	load    []int
	base    []PGEntry
	entries []PGEntry
}

func nextPool(old *PoolTable, in *PoolInputs, all *Inputs, moved int) (PoolTable, int) {
	p := in.Pool
	n := int(p.PGCount)
	base := make([]PGEntry, n)
	split := false
	switch {
	case old == nil:
	case len(old.PGs) == n:
		copy(base, old.PGs)
	case 2*len(old.PGs) == n:
		split = true
		for i := range base {
			base[i] = old.PGs[i/2]
		}
	default:
		panic(fmt.Sprintf("chunkstore: pool %d's pg_count went from %d to %d", in.ID, len(old.PGs), n))
	}
	settings := old != nil && (int(old.Replicas) != p.Replicas || int(old.MinLive) != p.MinLive)

	pl := &placer{pool: p, now: all.Now, disks: slices.Clone(in.Disks), base: base, entries: make([]PGEntry, n)}
	slices.SortFunc(pl.disks, func(a, b Disk) int { return cmp.Compare(a.ID, b.ID) })
	pl.target = p.targets(pl.disks, all.Now)
	pl.load = make([]int, len(pl.disks))
	asked := make([]bool, n)
	for _, a := range all.Asks {
		if a.PG.Pool == in.ID && int(a.PG.Index) < n && a.Interval == base[a.PG.Index].Interval {
			asked[a.PG.Index] = true
		}
	}
	edits := make([]*Edit, n)
	for i, ed := range all.Edits {
		if ed.PG.Pool == in.ID && int(ed.PG.Index) < n && pl.accepts(&ed) {
			edits[ed.PG.Index] = &all.Edits[i]
		}
	}
	for i := range base {
		e := &pl.entries[i]
		*e = base[i]
		e.Members = slices.Clone(e.Members)
		e.Past = slices.Clone(e.Past)
		if ed := edits[i]; ed != nil {
			e.Members, e.Pinned = e.Members[:0], ed.Pinned
			for _, d := range ed.Disks {
				e.Members = append(e.Members, Member{Disk: d})
			}
		}
		for j := range e.Members {
			e.Members[j].Mark = MarkDown
			if k := pl.find(e.Members[j].Disk); k >= 0 {
				e.Members[j].Mark = pl.disks[k].Mark
			}
		}
		for _, m := range e.Members {
			if k := pl.find(m.Disk); k >= 0 && !m.Leaving {
				pl.load[k]++
			}
		}
	}
	for _, a := range all.Activations {
		if !split && a.PG.Pool == in.ID && int(a.PG.Index) < n {
			e := &pl.entries[a.PG.Index]
			if a.Interval > e.Activated && a.Interval <= e.Interval {
				e.Activated = a.Interval
				e.Past = slices.DeleteFunc(e.Past, func(pi PastInterval) bool { return pi.Interval < a.Interval })
			}
		}
	}
	for _, r := range all.Recovered {
		if !split && r.PG.Pool == in.ID && int(r.PG.Index) < n {
			if e := &pl.entries[r.PG.Index]; r.Interval == e.Interval && recovers(e.Members) {
				e.dropLeaving()
				e.Recovered = true
			}
		}
	}
	for i := range pl.entries {
		pl.refill(&pl.entries[i], &base[i])
	}
	moves := pl.balance(max(0, all.Moves-moved))
	for i := range pl.entries {
		e, b := &pl.entries[i], &base[i]
		if b.Interval > 0 && slices.Equal(e.Members, b.Members) && !settings && !split && !asked[i] {
			continue
		}
		if b.Interval > 0 {
			e.Past = append(e.Past, PastInterval{Interval: b.Interval, Members: b.Members})
		}
		e.Interval, e.Recovered = b.Interval+1, false
	}
	return PoolTable{ID: in.ID, Replicas: uint8(p.Replicas), MinLive: uint8(p.MinLive), PGs: pl.entries}, moves
}

// accepts reports whether an operator's edit keeps the pool's rules: no more disks than its width, no disk twice and no domain over perDomain; nextPool ignores any other.
func (pl *placer) accepts(ed *Edit) bool {
	if len(ed.Disks) > pl.pool.width() {
		return false
	}
	for j, d := range ed.Disks {
		if slices.Contains(ed.Disks[:j], d) {
			return false
		}
		k := pl.find(d)
		if k < 0 {
			continue
		}
		same := 0
		for _, o := range ed.Disks[:j] {
			if ko := pl.find(o); ko >= 0 && pl.pool.domainOf(&pl.disks[ko]) == pl.pool.domainOf(&pl.disks[k]) {
				same++
			}
		}
		if same >= pl.pool.perDomain() {
			return false
		}
	}
	return true
}

// narrow brings e's members but a leaving one down to the width after replicas went down: it deletes a member refill would replace anyway, and marks the one it would otherwise drop, its last down member or else its last, leaving, so that it goes once the others have recovered; a PG with a leaving member keeps the rest until its removal.
func (pl *placer) narrow(e, base *PGEntry) {
	for len(e.Members)-leavingIn(e.Members) > pl.pool.width() {
		j := len(e.Members) - 1
		for j >= 0 && (e.Members[j].Leaving || !pl.missing(e, base, j)) {
			j--
		}
		if j >= 0 {
			if k := pl.find(e.Members[j].Disk); k >= 0 {
				pl.load[k]--
			}
			e.Members = slices.Delete(e.Members, j, j+1)
			continue
		}
		if e.leaving() >= 0 {
			return
		}
		j = len(e.Members) - 1
		if k := lastDown(e.Members); k >= 0 {
			j = k
		}
		e.Members[j].Leaving = true
		if k := pl.find(e.Members[j].Disk); k >= 0 {
			pl.load[k]--
		}
	}
}

// recovers reports whether every member but a leaving one is up, as a recovered report needs: a table that marks one otherwise ignores the report.
func recovers(ms []Member) bool {
	return !slices.ContainsFunc(ms, func(m Member) bool { return !m.Leaving && m.Mark != MarkUp })
}

func leavingIn(ms []Member) int {
	n := 0
	for _, m := range ms {
		if m.Leaving {
			n++
		}
	}
	return n
}

func (e *PGEntry) leaving() int {
	return slices.IndexFunc(e.Members, func(m Member) bool { return m.Leaving })
}

func (e *PGEntry) dropLeaving() {
	e.Members = slices.DeleteFunc(e.Members, func(m Member) bool { return m.Leaving })
}

func lastDown(ms []Member) int {
	for j := len(ms) - 1; j >= 0; j-- {
		if ms[j].Mark == MarkDown {
			return j
		}
	}
	return -1
}

// targets gives each disk its target in members, zero for a disk that sheds its members at now: its domain's water-filled share of the others, split within the domain by capacity.
func (p Pool) targets(disks []Disk, now core.Tick) []float64 {
	caps := make([]uint64, len(disks))
	var domains []uint32
	var domainCaps []uint64
	domainOf := make([]int, len(disks))
	for i := range disks {
		d := &disks[i]
		if !d.sheds(now) {
			_, caps[i] = p.diskCapacity(d)
		}
		j := slices.Index(domains, p.domainOf(d))
		if j < 0 {
			j = len(domains)
			domains, domainCaps = append(domains, p.domainOf(d)), append(domainCaps, 0)
		}
		domainOf[i] = j
		domainCaps[j] += caps[i]
	}
	share := waterFill(domainCaps, float64(p.perDomain())/float64(p.width()))
	target := make([]float64, len(disks))
	for i, j := range domainOf {
		if caps[i] > 0 {
			target[i] = share[j] * float64(caps[i]) / float64(domainCaps[j]) * float64(p.PGCount) * float64(p.width())
		}
	}
	return target
}

// sheds reports whether d gives up its members at now: it drains, or its members are refilled for having been down for refill_after.
func (d *Disk) sheds(now core.Tick) bool {
	return d.Drain || d.Mark == MarkDown && d.downFor(now) >= RefillAfter
}

func (d *Disk) downFor(now core.Tick) core.Tick { return now - min(d.DownSince, now) }

func (pl *placer) find(id uint32) int {
	k, ok := slices.BinarySearchFunc(pl.disks, id, func(d Disk, id uint32) int { return cmp.Compare(d.ID, id) })
	if !ok {
		return -1
	}
	return k
}

// refill drops a leaving member whose disk left the pool, narrows e, keeps the other leaving member in place of a missing member or where the width needs it, replaces every other missing member of e, and adds members until all but a leaving one fill the width, each on the eligible disk furthest below its target; a member with no eligible disk stays.
func (pl *placer) refill(e *PGEntry, base *PGEntry) {
	// A leaving member merely down stays however long: the member that replaces it is no copy
	// until the recovered report, and may never become one if the leaving member holds the only
	// copy, while activations with the new member live drop the past intervals that would have
	// made peering wait for it.
	if j := e.leaving(); j >= 0 && pl.find(e.Members[j].Disk) < 0 {
		e.dropLeaving()
	}
	pl.narrow(e, base)
	for j := 0; j < len(e.Members); j++ {
		if e.Members[j].Leaving || !pl.missing(e, base, j) {
			continue
		}
		if e.leaving() >= 0 && pl.stay(e, j) {
			if o := pl.find(e.Members[j].Disk); o >= 0 {
				pl.load[o]--
			}
			e.Members = slices.Delete(e.Members, j, j+1)
			j--
		} else if k := pl.pick(e, j); k >= 0 {
			pl.swap(e, j, k)
		}
	}
	if e.leaving() >= 0 && len(e.Members) <= pl.pool.width() {
		pl.stay(e, -1)
	}
	for len(e.Members)-leavingIn(e.Members) < pl.pool.width() {
		k := pl.pick(e, -1)
		if k < 0 {
			return
		}
		e.Members = append(e.Members, Member{Disk: pl.disks[k].ID})
		pl.load[k]++
	}
}

// stay makes e's leaving member an ordinary member in place of member out (-1 for none) if its disk is up and not draining and its domain has room, and reports whether it did.
func (pl *placer) stay(e *PGEntry, out int) bool {
	j := e.leaving()
	k := pl.find(e.Members[j].Disk)
	if k < 0 || pl.disks[k].Mark != MarkUp || pl.disks[k].Drain || !pl.room(e, k, out) {
		return false
	}
	e.Members[j].Leaving = false
	pl.load[k]++
	return true
}

// missing reports whether e's member j is refilled: its disk left the pool, or it has been down for refill_after, or for degraded_refill_after with e below min_live, and peering does not wait for it anyway.
func (pl *placer) missing(e, base *PGEntry, j int) bool {
	m := e.Members[j]
	k := pl.find(m.Disk)
	if k < 0 {
		return true
	}
	if m.Mark != MarkDown {
		return false
	}
	down := pl.disks[k].downFor(pl.now)
	if down < RefillAfter && (down < DegradedRefillAfter || live(e.Members) >= pl.pool.MinLive) {
		return false
	}
	return !pl.awaited(m.Disk, e.Past) && !pl.awaited(m.Disk, []PastInterval{{base.Interval, base.Members}})
}

// awaited reports whether disk was the only live member of an interval that accepted writes, among intervals, so peering waits for it.
func (pl *placer) awaited(disk uint32, intervals []PastInterval) bool {
	for _, pi := range intervals {
		if pl.pool.MinLive <= 1 && live(pi.Members) == 1 && slices.ContainsFunc(pi.Members, func(m Member) bool { return m.Disk == disk && m.Mark == MarkUp }) {
			return true
		}
	}
	return false
}

func live(ms []Member) int {
	n := 0
	for _, m := range ms {
		if m.Mark != MarkDown {
			n++
		}
	}
	return n
}

// pick is the eligible disk for e whose fill after one more member is furthest below its target, the lowest ID on a tie, replacing member out (-1 for none); -1 if none is eligible.
func (pl *placer) pick(e *PGEntry, out int) int {
	best := -1
	for k := range pl.disks {
		if pl.eligible(e, k, out) && (best < 0 || pl.after(k) < pl.after(best)) {
			best = k
		}
	}
	return best
}

func (pl *placer) after(k int) float64 { return float64(pl.load[k]+1) / pl.target[k] }

// eligible reports whether disk k may take a member of e in place of member out: it is up with a target, not in e, and its domain holds fewer than perDomain of e's other members but a leaving one.
func (pl *placer) eligible(e *PGEntry, k, out int) bool {
	d := &pl.disks[k]
	if d.Mark != MarkUp || pl.target[k] == 0 || slices.ContainsFunc(e.Members, func(m Member) bool { return m.Disk == d.ID }) {
		return false
	}
	return pl.room(e, k, out)
}

// room reports whether disk k's domain holds fewer than perDomain of e's members but member out and a leaving one.
func (pl *placer) room(e *PGEntry, k, out int) bool {
	domain, same := pl.pool.domainOf(&pl.disks[k]), 0
	for j, m := range e.Members {
		if o := pl.find(m.Disk); j != out && !m.Leaving && o >= 0 && pl.pool.domainOf(&pl.disks[o]) == domain {
			same++
		}
	}
	return same < pl.pool.perDomain()
}

func (pl *placer) swap(e *PGEntry, j, k int) {
	if o := pl.find(e.Members[j].Disk); o >= 0 {
		pl.load[o]--
	}
	e.Members[j] = Member{Disk: pl.disks[k].ID}
	pl.load[k]++
}

// margin is a disk's fill margin in members: max(5% of its target, 1 member).
func margin(target float64) float64 { return max(0.05*target, 1) }

func (pl *placer) ratio(k int) float64 {
	switch {
	case pl.target[k] > 0:
		return float64(pl.load[k]) / pl.target[k]
	case pl.load[k] > 0:
		return math.Inf(1)
	}
	return 0
}

// converged reports whether every disk is within its fill margin, a disk with no target holding no member.
func (pl *placer) converged() bool {
	for k, t := range pl.target {
		if t == 0 && pl.load[k] > 0 || t > 0 && math.Abs(float64(pl.load[k])-t) > margin(t) {
			return false
		}
	}
	return true
}

// balance makes up to limit moves while some disk is outside its fill margin: from the disk furthest above its target that has an up member of an unpinned PG to the eligible disk furthest below, if that leaves it no higher than the source; else one that brings a disk outside its margin nearer it and takes none outside its own.
func (pl *placer) balance(limit int) int {
	order := make([]int, len(pl.disks))
	dests := make([]int, len(pl.disks))
	moves := 0
	// Each move lowers, in order of precedence, the members on disks without a target, the members
	// outside margins, or Σ load²/target, so the loop ends.
	for moves < limit && !pl.converged() {
		for k := range order {
			order[k], dests[k] = k, k
		}
		slices.SortStableFunc(order, func(a, b int) int { return cmp.Compare(pl.ratio(b), pl.ratio(a)) })
		slices.SortStableFunc(dests, func(a, b int) int { return cmp.Compare(pl.after(a), pl.after(b)) })
		if !pl.moveOne(order, dests) && !pl.moveNearer(order, dests) {
			break
		}
		moves++
	}
	return moves
}

func (pl *placer) moveOne(order, dests []int) bool {
	for _, src := range order {
		if pl.ratio(src) <= 1 {
			return false
		}
		srcAfter := math.Inf(1)
		if pl.target[src] > 0 {
			srcAfter = float64(pl.load[src]-1) / pl.target[src]
		}
		for _, k := range dests {
			if pl.target[k] == 0 || pl.after(k) >= srcAfter+1e-9 {
				break
			}
			if pl.move(src, k) {
				return true
			}
		}
	}
	return false
}

// moveNearer moves a member from a disk above its margin, or to one below it, keeping both within their margins on the other side.
func (pl *placer) moveNearer(order, dests []int) bool {
	for _, src := range order {
		t, n := pl.target[src], float64(pl.load[src])
		if n == 0 || n-1 < t-margin(t) {
			continue
		}
		for _, k := range dests {
			tk, nk := pl.target[k], float64(pl.load[k])
			if tk > 0 && nk+1 <= tk+margin(tk) && (n > t+margin(t) || t == 0 || nk < tk-margin(tk)) && pl.move(src, k) {
				return true
			}
		}
	}
	return false
}

// move adds disk k to the lowest unpinned PG with no leaving member whose up member on disk src it may replace, marking that member leaving, if there is one; the PG's interval must be recovered and its members unchanged since, so each is complete when the move starts.
func (pl *placer) move(src, k int) bool {
	id := pl.disks[src].ID
	for i := range pl.entries {
		e := &pl.entries[i]
		j := slices.Index(e.Members, Member{Disk: id, Mark: MarkUp})
		if j >= 0 && !e.Pinned && e.Recovered && e.leaving() < 0 && slices.Equal(e.Members, pl.base[i].Members) && pl.eligible(e, k, j) {
			e.Members[j].Leaving = true
			e.Members = append(e.Members, Member{Disk: pl.disks[k].ID})
			pl.load[src]--
			pl.load[k]++
			return true
		}
	}
	return false
}

// Recovered reports whether every PG of p records a recovered report for its current interval, so none has a leaving member and balance may move any unpinned one.
func (p *PoolTable) Recovered() bool {
	return !slices.ContainsFunc(p.PGs, func(e PGEntry) bool { return !e.Recovered })
}
