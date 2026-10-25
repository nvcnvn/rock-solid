package chunkstore

import (
	"cmp"
	"math/bits"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// StuckAfter is stuck_after: how long a PG with past or missing members may copy nothing before its pool is Stuck.
const StuckAfter = core.Tick(60_000_000)

// Disk is one disk of a pool's figures, at its last reported fill; sizes and fills are in blocks.
type Disk struct {
	ID     uint32
	Node   uint32
	Class  string
	Blocks uint64
	Live   uint64
	Own    uint64
	Slots  uint32
}

// DiskFigures is what the CLI lists for one disk; Used counts the pool's grants at the disk's share.
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

// Capacity computes p's figures from its disks' fills, its own data and member slots on each, and the grants outstanding on it.
func (p Pool) Capacity(disks []Disk, grants uint64) Figures {
	w, q, k := uint64(p.width()), uint64(p.perDomain()), uint64(p.dataShards())
	den := uint64(p.PGCount) * k
	f := Figures{Copies: p.Replicas, SetsAvailable: -1}
	var own uint64
	var domainOf []uint32
	for i := range disks {
		d := &disks[i]
		if d.Slots == 0 {
			continue
		}
		df := DiskFigures{ID: d.ID, Percent: p.limitPercent(d.Blocks), Blocks: d.Blocks, Live: d.Live, Slots: d.Slots}
		df.Limit = d.Blocks * uint64(df.Percent) / 100
		df.Capacity = df.Limit - min(df.Limit, d.Live-min(d.Own, d.Live))
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
	f.targets(caps, domainOf, p.PGCount, w, q)
	return f
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

// targets sets each disk's target in member slots: its domain's capacity water-filled with each domain
// capped at perDomain of a PG's width, split over the domain's disks by capacity.
func (f *Figures) targets(caps []uint64, domainOf []uint32, pgs uint32, w, q uint64) {
	share := make([]float64, len(caps))
	capped := make([]bool, len(caps))
	limit := float64(q) / float64(w)
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
			break
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
			break
		}
	}
	for i, id := range domainOf {
		j := f.domainIndex(id)
		if caps[j] > 0 {
			f.Disks[i].Target = share[j] * float64(f.Disks[i].Capacity) / float64(caps[j]) * float64(pgs) * float64(w)
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

// Copying is a PG of the pool with past or missing members, and when it last copied or began to need copying.
type Copying struct {
	PG    uint32
	Since core.Tick
}

// Conditions are a pool's conditions, raised and cleared by Update, Watch and Confirm.
type Conditions struct {
	Full            bool
	NearFull        bool
	Stranded        bool
	Unbalanced      bool
	CannotReprotect bool
	Stuck           []uint32
	Confirmed       float64
}

// Update raises and clears the conditions on f; Unbalanced keeps its value unless settled: no move proposed, no past members listed.
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

// Watch sets Stuck to the PGs that have copied nothing for StuckAfter by now.
func (c *Conditions) Watch(now core.Tick, pgs []Copying) {
	c.Stuck = c.Stuck[:0]
	for _, pg := range pgs {
		if now >= pg.Since && now-pg.Since >= StuckAfter {
			c.Stuck = append(c.Stuck, pg.PG)
		}
	}
}

// Confirm records f's stranded fraction as the confirmed fraction, as rock-solid pool create, disk changes and pool confirm do.
func (c *Conditions) Confirm(f *Figures) {
	c.Confirmed, c.Stranded = f.Stranded, false
}
