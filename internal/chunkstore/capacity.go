package chunkstore

import (
	"cmp"
	"math/bits"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// StuckAfter is stuck_after: how long a PG with past or missing members may copy nothing before its pool is Stuck.
const StuckAfter = core.Tick(60_000_000)

// Disk is one disk of a pool, at its last reported fill, with its live mark and whether it drains; sizes and fills are in blocks.
type Disk struct {
	ID        uint32
	Node      uint32
	Class     string
	Blocks    uint64
	Live      uint64
	Own       uint64
	Slots     uint32
	Mark      Mark
	DownSince core.Tick
	Drain     bool
}

// DiskFigures is what the CLI lists for one disk; Used counts the pool's grants at the disk's share, and Target is placement's.
type DiskFigures struct {
	ID        uint32
	Percent   uint8
	Blocks    uint64
	Limit     uint64
	Live      uint64
	Used      uint64
	Capacity  uint64
	Room      uint64
	Slots     uint32
	Available uint64
	Target    float64
}

// Full reports the primary's admission predicate: the disk refuses writes that add data.
func (d *DiskFigures) Full() bool { return d.Used >= d.Limit }

// level is the disk's limit lowered by points of its size, in whole blocks.
func (d *DiskFigures) level(points uint8) uint64 {
	return d.Blocks * uint64(d.Percent-min(points, d.Percent)) / 100
}

// DomainFigures is a failure domain's capacity, its load at balance and its stranded room.
type DomainFigures struct {
	ID       uint32
	Capacity uint64
	Load     uint64
	Stranded uint64
}

// LossKind is what a pool's reprotectable figure loses: nothing it can count, a domain or a disk.
type LossKind uint8

// The losses reprotectable considers.
const (
	LossNone LossKind = iota
	LossDomain
	LossDisk
)

// Loss names the loss that sets a pool's reprotectable figure.
type Loss struct {
	Kind LossKind
	ID   uint32
}

// Figures are a pool's capacity figures in blocks, over the disks that hold members of it.
type Figures struct {
	Disks         []DiskFigures
	Domains       []DomainFigures
	Copies        int
	Capacity      uint64
	Held          uint64
	Available     uint64
	Ungranted     uint64
	SetsAvailable int
	Balanced      uint64
	Stranded      float64
	SpareDomain   bool
	Reprotectable uint64
	Loss          Loss
}

// Asks reports whether creating the layout asks for confirmation: some domain strands more than half its capacity.
func (f *Figures) Asks() bool {
	for _, d := range f.Domains {
		if 2*d.Stranded > d.Capacity {
			return true
		}
	}
	return false
}

// Capacity computes p's figures at now from its disks' fills, its own data and member slots on each, and the grants outstanding on it.
func (p Pool) Capacity(disks []Disk, grants uint64, now core.Tick) Figures {
	w, q, k := uint64(p.width()), uint64(p.perDomain()), uint64(p.dataShards())
	den := uint64(p.PGCount) * k
	f := Figures{Copies: p.Replicas, SetsAvailable: -1}
	var own uint64
	var domainOf []uint32
	target := p.targets(disks, now)
	for i := range disks {
		d := &disks[i]
		if d.Slots == 0 {
			continue
		}
		df := DiskFigures{ID: d.ID, Percent: p.limitPercent(d.Blocks), Blocks: d.Blocks, Live: d.Live, Slots: d.Slots, Target: target[i]}
		df.Limit, df.Capacity = p.diskCapacity(d)
		df.Room = df.Limit - min(df.Limit, d.Live)
		granted := mulDivUp(grants, uint64(d.Slots), den)
		df.Used = d.Live + granted
		df.Available = mulDiv(df.Room-min(df.Room, granted), den, uint64(d.Slots))
		ungranted := mulDiv(df.Room, den, uint64(d.Slots))
		if f.SetsAvailable < 0 || df.Available < f.Available {
			f.Available, f.SetsAvailable = df.Available, len(f.Disks)
		}
		if len(f.Disks) == 0 || ungranted < f.Ungranted {
			f.Ungranted = ungranted
		}
		own += d.Own
		f.Disks = append(f.Disks, df)
		domainOf = append(domainOf, p.domainOf(d))
		f.Domains = addTo(f.Domains, p.domainOf(d), df.Capacity)
	}
	f.Held = mulDiv(own, k, w)
	caps := make([]uint64, len(f.Domains))
	for i, d := range f.Domains {
		caps[i] = d.Capacity
		f.Capacity += d.Capacity
	}
	f.Balanced = balanced(caps, w, q, k)
	var stranded uint64
	for i := range f.Domains {
		d := &f.Domains[i]
		atBalance := mulDiv(f.Balanced, q, k)
		d.Load = min(d.Capacity, atBalance)
		d.Stranded = d.Capacity - d.Load
		stranded += d.Stranded
	}
	if f.Capacity > 0 {
		f.Stranded = float64(stranded) / float64(f.Capacity)
	}
	f.SpareDomain = len(f.Domains) > 0 && uint64(len(f.Domains)-1)*q >= w
	f.reprotect(caps, domainOf, w, q, k)
	return f
}

// diskCapacity is d's limit under write_limit and its capacity for p: the limit less other pools' data.
func (p Pool) diskCapacity(d *Disk) (limit, capacity uint64) {
	limit = d.Blocks * uint64(p.limitPercent(d.Blocks)) / 100
	return limit, limit - min(limit, d.Live-min(d.Own, d.Live))
}

func addTo(domains []DomainFigures, id uint32, capacity uint64) []DomainFigures {
	for i := range domains {
		if domains[i].ID == id {
			domains[i].Capacity += capacity
			return domains
		}
	}
	return append(domains, DomainFigures{ID: id, Capacity: capacity})
}

func (f *Figures) domainIndex(id uint32) int {
	return slices.IndexFunc(f.Domains, func(d DomainFigures) bool { return d.ID == id })
}

// reprotect sets the least balanced over the survivors of each loss the design lists whose survivors
// can hold a PG's width, a domain holding at most one member per disk: with a spare domain the domain
// of most capacity, which is the worst single loss for copies; without one, each disk that shares its
// domain with another.
func (f *Figures) reprotect(caps []uint64, domainOf []uint32, w, q, k uint64) {
	if f.SpareDomain {
		largest := 0
		for i, c := range caps {
			if c > caps[largest] {
				largest = i
			}
		}
		rest := slices.Delete(slices.Clone(caps), largest, largest+1)
		f.Reprotectable, f.Loss = balanced(rest, w, q, k), Loss{LossDomain, f.Domains[largest].ID}
		return
	}
	shared := make([]int, len(caps))
	for _, id := range domainOf {
		shared[f.domainIndex(id)]++
	}
	var fit uint64
	for _, n := range shared {
		fit += min(q, uint64(n))
	}
	left := slices.Clone(caps)
	for i, id := range domainOf {
		j := f.domainIndex(id)
		if shared[j] < 2 || uint64(shared[j]) <= q && fit-1 < w {
			continue
		}
		left[j] -= f.Disks[i].Capacity
		b := balanced(left, w, q, k)
		left[j] = caps[j]
		if f.Loss.Kind == LossNone || b < f.Reprotectable {
			f.Reprotectable, f.Loss = b, Loss{LossDisk, f.Disks[i].ID}
		}
	}
}

// waterFill gives each domain its share of a PG's members, in proportion to its capacity with each capped at limit.
func waterFill(caps []uint64, limit float64) []float64 {
	share := make([]float64, len(caps))
	capped := make([]bool, len(caps))
	for {
		var rest float64
		var held int
		for i, c := range caps {
			if capped[i] {
				held++
			} else {
				rest += float64(c)
			}
		}
		if rest == 0 {
			return share
		}
		lambda := (1 - limit*float64(held)) / rest
		over := false
		for i, c := range caps {
			if !capped[i] {
				share[i] = lambda * float64(c)
				if share[i] > limit+1e-12 {
					capped[i], share[i], over = true, limit, true
				}
			}
		}
		if !over {
			return share
		}
	}
}

// balanced is the most a pool of width w, at most q members of a PG per domain and k data shards, holds over
// the domains' capacities: min over j of S₍>ⱼ₎ ÷ (b − j·a), with b = w ÷ k and a = q ÷ k, rounded down.
// It is exact for copies and q = 1; above that it ignores that a domain holds one member per disk.
//
// ponytail: closed form only, so q > 1 overstates (ec4-2 at 2 per node on 2×2, 2×2, 2×2 and 1×4 TB gives
// 9.60 TB, not 8.64), reprotect's largest-domain loss and CheckLayout's domain count share the gap; phase 10
// adds the bisection with min(cₖ, D ÷ data) per disk, each domain in turn for reprotect, and min(q, disks) per domain.
func balanced(caps []uint64, w, q, k uint64) uint64 {
	sorted := slices.Clone(caps)
	slices.SortFunc(sorted, func(a, b uint64) int { return cmp.Compare(b, a) })
	var rest uint64
	for _, c := range sorted {
		rest += c
	}
	best := ^uint64(0)
	for j := uint64(0); j*q < w; j++ {
		best = min(best, mulDiv(rest, k, w-j*q))
		if j == uint64(len(sorted)) {
			break
		}
		rest -= sorted[j]
	}
	return best
}

func mulDiv(a, b, c uint64) uint64 {
	hi, lo := bits.Mul64(a, b)
	q, _ := bits.Div64(hi, lo, c)
	return q
}

func mulDivUp(a, b, c uint64) uint64 {
	hi, lo := bits.Mul64(a, b)
	q, r := bits.Div64(hi, lo, c)
	if r > 0 {
		q++
	}
	return q
}

// Reserves reports whether p admits reserving add more bytes beside reserved bytes of volumes and snapshots bytes held by snapshots.
func (p Pool) Reserves(f *Figures, reserved, snapshots, add uint64) bool {
	return p.Overcommit || reserved+snapshots+add <= f.Balanced*core.BlockSize
}

// Copying is a PG of the pool with a leaving or missing member or a chunk on a member's missing list, when it last copied or began to need copying, and the chunks its primary's last peering left unfound, or Stopped with no active primary to count them (Replica.Unfound).
type Copying struct {
	PG      uint32
	Since   core.Tick
	Unfound uint32
	Stopped bool
}

// BlockedPG is a PG that is stuck or has unfound chunks: how many are unfound, unknown if Stopped, and its Candidates, the members down or in maintenance, each of which may hold what it lacks and none known to. No candidate means none is left to return.
type BlockedPG struct {
	PG         uint32
	Unfound    uint32
	Stopped    bool
	Candidates []uint32
}

// Conditions are a pool's conditions, raised and cleared by Update, Watch and Confirm.
type Conditions struct {
	Full            bool
	NearFull        bool
	Stranded        bool
	Unbalanced      bool
	CannotReprotect bool
	Stuck           []uint32
	Blocked         []BlockedPG
	Confirmed       float64
}

// Update raises and clears the conditions on f; Unbalanced keeps its value unless settled: placement proposes no move for the pool and its table records every PG recovered (PoolTable.Recovered).
func (c *Conditions) Update(f *Figures, settled bool) {
	if c.Full {
		c.Full = f.anyAbove(1)
	} else {
		c.Full = f.anyReaches(0)
	}
	switch {
	case c.Full:
		c.NearFull = false
	case c.NearFull:
		c.NearFull = f.anyAbove(11)
	default:
		c.NearFull = f.anyReaches(10)
	}
	threshold := max(0.10, c.Confirmed+0.05)
	if c.Stranded {
		c.Stranded = f.Stranded > threshold-0.01
	} else {
		c.Stranded = f.Stranded > threshold
	}
	if settled {
		// Before grants, which may take half of available and say nothing of the table.
		reach := f.Ungranted + f.Held
		if c.Unbalanced {
			c.Unbalanced = 100*reach < 91*f.Balanced
		} else {
			c.Unbalanced = 10*reach < 9*f.Balanced
		}
	}
	c.CannotReprotect = f.Copies >= 2 && f.Loss.Kind != LossNone && f.Held > f.Reprotectable
}

// anyReaches reports whether some disk's use, grants counted, reaches its limit less points.
func (f *Figures) anyReaches(points uint8) bool {
	return slices.ContainsFunc(f.Disks, func(d DiskFigures) bool { return d.Used >= d.level(points) })
}

// anyAbove reports whether some disk's use, grants counted, is above its limit less points.
func (f *Figures) anyAbove(points uint8) bool {
	return slices.ContainsFunc(f.Disks, func(d DiskFigures) bool { return d.Used > d.level(points) })
}

// Watch sets Stuck to the PGs that have copied nothing for StuckAfter by now, and Blocked to those and the PGs with unfound chunks, each naming its candidates in t, the pool's table. A PG t does not have, as a report from before a split or from a stale table may name, is dropped.
func (c *Conditions) Watch(now core.Tick, pgs []Copying, t *PoolTable) {
	c.Stuck, c.Blocked = c.Stuck[:0], c.Blocked[:0]
	for _, pg := range pgs {
		if int(pg.PG) >= len(t.PGs) {
			continue
		}
		stuck := now >= pg.Since && now-pg.Since >= StuckAfter
		if stuck {
			c.Stuck = append(c.Stuck, pg.PG)
		}
		if stuck || pg.Unfound > 0 {
			b := BlockedPG{PG: pg.PG, Unfound: pg.Unfound, Stopped: pg.Stopped}
			for _, m := range t.PGs[pg.PG].Members {
				if m.Mark != MarkUp {
					b.Candidates = append(b.Candidates, m.Disk)
				}
			}
			c.Blocked = append(c.Blocked, b)
		}
	}
}

// Watcher is what an authority holds of each PG for its pool's Stuck and Blocked: its primary's latest report, whether it needs copying and since when it last copied or began to, and the interval and copies lacking it last measured progress against.
type Watcher struct {
	pgs     []pgWatch
	copying []Copying
}

type pgWatch struct {
	pg       core.PGID
	lead     LeadState
	heard    bool
	needs    bool
	since    core.Tick
	interval uint32
	lacking  uint32
}

func (w *Watcher) find(pg core.PGID) (int, bool) {
	return slices.BinarySearchFunc(w.pgs, pg, func(x pgWatch, pg core.PGID) int {
		return cmp.Or(cmp.Compare(x.pg.Pool, pg.Pool), cmp.Compare(x.pg.Index, pg.Index))
	})
}

// Hear keeps a copy of each PG's latest report from the primary of its newest interval, of the PGs Watch has
// seen; a report of any other is dropped, and the next heartbeat repeats it.
func (w *Watcher) Hear(ls []LeadState) {
	for _, l := range ls {
		if i, ok := w.find(l.PG); ok {
			if x := &w.pgs[i]; !x.heard || l.Interval >= x.lead.Interval {
				cs := append(x.lead.Chunks[:0], l.Chunks...)
				x.lead, x.heard = l, true
				x.lead.Chunks = cs
			}
		}
	}
}

// Watch feeds cond's Watch at now from pt, the pool's committed table, and the reports heard. A PG needs copying while it has a leaving or down member or, with no member in maintenance, its table records no recovered report for its current interval or its primary reports copies lacking; it copied when the copies lacking fall within one interval. A PG whose current interval no active primary reports is stopped, its count unknown.
func (w *Watcher) Watch(now core.Tick, pt *PoolTable, cond *Conditions) {
	w.copying = w.copying[:0]
	for j := range pt.PGs {
		e := &pt.PGs[j]
		id := core.PGID{Pool: pt.ID, Index: uint32(j)}
		i, ok := w.find(id)
		if !ok {
			w.pgs = slices.Insert(w.pgs, i, pgWatch{pg: id})
		}
		x := &w.pgs[i]
		active := x.heard && x.lead.Active && x.lead.Interval == e.Interval
		var lacking, unfound uint32
		if active {
			lacking, unfound = x.lead.Lacking, x.lead.Unfound
		}
		maint := slices.ContainsFunc(e.Members, func(m Member) bool { return m.Mark == MarkMaint })
		needs := slices.ContainsFunc(e.Members, func(m Member) bool { return m.Leaving || m.Mark == MarkDown }) || !maint && (!e.Recovered || lacking > 0)
		if needs && !x.needs {
			x.since = now
		}
		if active {
			if x.needs && x.interval == e.Interval && lacking < x.lacking {
				x.since = now
			}
			x.interval, x.lacking = e.Interval, lacking
		}
		x.needs = needs
		since := now
		if needs {
			since = x.since
		}
		if needs || unfound > 0 {
			w.copying = append(w.copying, Copying{PG: uint32(j), Since: since, Unfound: unfound, Stopped: !active})
		}
	}
	cond.Watch(now, w.copying, pt)
}

// Lead is the latest report Hear kept of pg's primary, nil for none.
func (w *Watcher) Lead(pg core.PGID) *LeadState {
	if i, ok := w.find(pg); ok && w.pgs[i].heard {
		return &w.pgs[i].lead
	}
	return nil
}

// Forget drops the PGs t does not have.
func (w *Watcher) Forget(t *Table) {
	w.pgs = slices.DeleteFunc(w.pgs, func(x pgWatch) bool {
		p := t.pool(x.pg.Pool)
		return p == nil || int(x.pg.Index) >= len(p.PGs)
	})
}

// Confirm records f's stranded fraction as the confirmed fraction, as rock-solid pool create, disk changes and pool confirm do.
func (c *Conditions) Confirm(f *Figures) {
	c.Confirmed, c.Stranded = f.Stranded, false
}
