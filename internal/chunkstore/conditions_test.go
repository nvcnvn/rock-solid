package chunkstore

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

func tb(x float64) uint64 { return uint64(x*1e12) / core.BlockSize }

func rep(replicas int, pgs uint32) Pool {
	return Pool{Replicas: replicas, MinLive: 1, FailureDomain: DomainNode, MaxPerDomain: 1, Locality: LocalitySpread, Overcommit: true, WriteLimit: 90, PGCount: pgs}
}

// c2 is the capacity model spike's C2 with its table at seed 1.
func c2() []Disk {
	return []Disk{
		{ID: 0, Node: 0, Blocks: tb(4), Slots: 128},
		{ID: 1, Node: 1, Blocks: tb(4), Slots: 128},
		{ID: 2, Node: 2, Blocks: tb(2), Slots: 86},
		{ID: 3, Node: 3, Blocks: tb(1), Slots: 42},
	}
}

func raised(c *Conditions) string {
	return fmt.Sprintf("Full %v NearFull %v", c.Full, c.NearFull)
}

// The fullest member disk's fill, with write_limit 90% and no grants, raises and clears Full and NearFull in whole blocks.
func TestFillConditions(t *testing.T) {
	p := rep(3, 128)
	disks := nodes(1, 1, 1)
	for i := range disks {
		disks[i].Blocks, disks[i].Slots = 1<<28, 128
	}
	blocks := uint64(1 << 28)
	limit := blocks * 90 / 100
	var c Conditions
	for _, s := range []struct {
		name      string
		live      uint64
		full      bool
		nearFull  bool
		available uint64
	}{
		{"79%", blocks * 79 / 100, false, false, limit - blocks*79/100},
		{"exactly 80%", blocks * 80 / 100, false, true, limit - blocks*80/100},
		{"one block under the limit", limit - 1, false, true, 1},
		{"exactly the limit", limit, true, false, 0},
		{"89.5% after a discard", blocks * 895 / 1000, true, false, limit - blocks*895/1000},
		{"89%", blocks * 89 / 100, false, true, limit - blocks*89/100},
		{"79.5%", blocks * 795 / 1000, false, true, limit - blocks*795/1000},
		{"79% again", blocks * 79 / 100, false, false, limit - blocks*79/100},
	} {
		disks[0].Live = s.live
		f := p.Capacity(disks, 0)
		c.Update(&f, true)
		if c.Full != s.full || c.NearFull != s.nearFull || f.Available != s.available || f.Disks[0].Full() != (s.live >= limit) {
			t.Fatalf("%s: %s, available %d, want Full %v NearFull %v, available %d", s.name, raised(&c), f.Available, s.full, s.nearFull, s.available)
		}
	}
}

// C2 with four 256 MiB grants and disk D 1 GiB × its share under its limit is Full, and admission refuses there.
func TestGrantsMakeAPoolFull(t *testing.T) {
	p, disks := rep(3, 128), c2()
	d := &disks[3]
	d.Live = d.Blocks*90/100 - (1<<30/core.BlockSize)*42/128
	f := p.Capacity(disks, 4*(256<<20)/core.BlockSize)
	var c Conditions
	c.Update(&f, true)
	if !c.Full || f.Available != 0 || !f.Disks[3].Full() {
		t.Fatalf("%s, available %d, D admits: %v", raised(&c), f.Available, !f.Disks[3].Full())
	}
	f = p.Capacity(disks, 0)
	var fresh Conditions
	fresh.Update(&f, true)
	if fresh.Full || f.Disks[3].Full() {
		t.Fatalf("without the grants: %s", raised(&fresh))
	}
}

// Pools that share a disk each check their own limit against the disk's total fill.
func TestTwoPoolsOnOneDisk(t *testing.T) {
	disks := nodes(1, 1, 1)
	disks[0].Live = disks[0].Blocks * 92 / 100
	for _, s := range []struct {
		limit          uint8
		full, nearFull bool
	}{{90, true, false}, {95, false, true}} {
		p := rep(3, 1)
		p.WriteLimit = s.limit
		f := p.Capacity(disks, 0)
		var c Conditions
		if c.Update(&f, true); c.Full != s.full || c.NearFull != s.nearFull {
			t.Fatalf("limit %d%%: %s", s.limit, raised(&c))
		}
	}
}

func TestAvailable(t *testing.T) {
	p := rep(3, 128)
	empty := p.Capacity(c2(), 0)
	grants := uint64(4 * (256 << 20) / core.BlockSize)
	granted := p.Capacity(c2(), grants)
	if gap := int64(empty.Available) - int64(granted.Available) - int64(grants); gap < 0 || gap > 4 {
		t.Fatalf("four 256 MiB grants take %d blocks off available, want 1 GiB to within rounding", empty.Available-granted.Available)
	}
	outside := append(c2(), Disk{ID: 4, Node: 4, Blocks: 1, Class: "hdd"})
	if f := p.Capacity(outside, 0); f.Available != empty.Available || len(f.Disks) != 4 || f.Capacity != empty.Capacity {
		t.Fatalf("a disk without members of the pool changes its figures: %+v", f)
	}
	full := c2()
	full[3].Live = tb(0.95)
	f := p.Capacity(full, 0)
	if f.Disks[3].Capacity != 0 || f.Disks[3].Room != 0 || f.Available != 0 {
		t.Fatalf("a disk past write_limit because of another pool: capacity %d, room %d, available %d", f.Disks[3].Capacity, f.Disks[3].Room, f.Available)
	}
}

// A none pool with locality node has per-node figures, each over the node's own PGs.
func TestPerNodeAvailable(t *testing.T) {
	p := rep(1, 256)
	p.Locality = LocalityNode
	disks := nodes(2, 2, 2)
	disks[0].Live = tb(1.2)
	for i, want := range []string{"0.600 TB", "1.800 TB", "1.800 TB"} {
		d := disks[i]
		d.Slots = 256
		if f := p.Capacity([]Disk{d}, 0); size(f.Available) != want {
			t.Fatalf("node %d: available %s, want %s", i, size(f.Available), want)
		}
	}
}

// Pools that share disks draw on the same capacity, so their figures overlap.
func TestPoolsShareCapacity(t *testing.T) {
	disks := c2()
	if f := rep(3, 128).Capacity(disks, 0); size(f.Balanced) != "2.700 TB" || size(f.Capacity) != "9.900 TB" {
		t.Fatalf("rep3: balanced %s of %s", size(f.Balanced), size(f.Capacity))
	}
	if f := rep(1, 128).Capacity(disks, 0); size(f.Balanced) != "9.900 TB" {
		t.Fatalf("none: balanced %s", size(f.Balanced))
	}
}

// With overcommit off, reservations plus what snapshots hold stay within balanced over capacity.
func TestReservations(t *testing.T) {
	p := rep(3, 128)
	p.Overcommit = false
	c := &capacityCase{pool: p, disks: c2()}
	f := p.Capacity(c.disks, 0)
	const T = 1e12
	for _, s := range []struct {
		name                     string
		reserved, snapshots, add float64
		ok                       bool
	}{
		{"first 1 TB", 0, 0, 1, true},
		{"second 1 TB", 1, 0, 1, true},
		{"third 1 TB", 2, 0, 1, false},
		{"an expand to 1.5 TB", 2, 0, 0.5, true},
		{"0.19 TB, above available", 2.5, 0, 0.19, true},
		{"an expand by 0.02 TB", 2.69, 0, 0.02, false},
		{"snapshots of 0.3 TB and an expand by 0.1 TB", 2.5, 0.3, 0.1, false},
	} {
		if got := p.Reserves(&f, uint64(s.reserved*T), uint64(s.snapshots*T), uint64(s.add*T)); got != s.ok {
			t.Fatalf("%s: admitted %v", s.name, got)
		}
		if on := (Pool{Overcommit: true}); !on.Reserves(&f, uint64(s.reserved*T), uint64(s.snapshots*T), uint64(s.add*T)) {
			t.Fatalf("%s: refused with overcommit on", s.name)
		}
	}
	c.hold(tb(2))
	f = p.Capacity(c.disks, 0)
	if !p.Reserves(&f, 2.5*T, 0, 0.1*T) || p.Reserves(&f, 2.5*T, 0, 0.3*T) {
		t.Fatalf("with 2 TB written, balanced %s", size(f.Balanced))
	}
}

func TestStuck(t *testing.T) {
	var c Conditions
	pgs := []Copying{{PG: 7, Since: 1_000}}
	if c.Watch(1_000+StuckAfter-1, pgs); len(c.Stuck) != 0 {
		t.Fatalf("Stuck %v before stuck_after", c.Stuck)
	}
	if c.Watch(1_000+StuckAfter, pgs); !slices.Equal(c.Stuck, []uint32{7}) {
		t.Fatalf("Stuck %v after stuck_after", c.Stuck)
	}
	pgs[0].Since = 1_000 + StuckAfter
	if c.Watch(1_000+StuckAfter+1, pgs); len(c.Stuck) != 0 {
		t.Fatalf("Stuck %v once the PG copies again", c.Stuck)
	}
	if c.Watch(10*StuckAfter, nil); len(c.Stuck) != 0 {
		t.Fatalf("Stuck %v once the past members are trimmed", c.Stuck)
	}
}

// A rebooting node's disks keep their shares at their last reported fills, so no figure or condition changes.
func TestRebootChangesNoFigure(t *testing.T) {
	p := rep(3, 128)
	disks := nodes(2, 2, 2)
	for i := range disks {
		disks[i].Slots, disks[i].Live, disks[i].Own = 128, tb(1), tb(1)
	}
	before := p.Capacity(disks, 0)
	var c Conditions
	c.Update(&before, true)
	was := c
	lastReported := slices.Clone(disks)
	during := p.Capacity(lastReported, 0)
	if c.Update(&during, true); !reflect.DeepEqual(before, during) || !reflect.DeepEqual(c, was) {
		t.Fatal("a reboot changed the figures")
	}
}

// Any layout that strands space is accepted, and creating it asks for confirmation.
func TestStrandingLayoutsAreAccepted(t *testing.T) {
	disks := nodes(4, 1, 1)
	disks[0].Slots, disks[1].Slots, disks[2].Slots = 128, 64, 64
	p := rep(2, 128)
	if _, err := p.CheckLayout(disks); err != nil {
		t.Fatal(err)
	}
	if f := p.Capacity(disks, 0); !f.Asks() {
		t.Fatal("C6 does not ask for confirmation")
	}
}

// For random layouts, valid tables and amounts held, the figures keep the bounds the design states.
func TestPoolFiguresHoldTheirBounds(t *testing.T) {
	n := 2000
	if testing.Short() {
		n = 200
	}
	var cases [4]int
	seeds.Run(t, 0, n, func(seed uint64) {
		r := core.NewRand(seed)
		p := rep(int(r.Range(1, 3)), 1<<r.Range(4, 9))
		var disks []Disk
		nodeCount := int(r.Range(3, 10))
		for node := range nodeCount {
			for range r.Range(1, 4) {
				d := Disk{ID: uint32(len(disks)), Node: uint32(node), Blocks: tb(float64(r.Range(1, 8)) / 2)}
				if r.Chance(1, 4) {
					d.Live = r.Below(d.Blocks)
				}
				disks = append(disks, d)
			}
		}
		perNode := make([][]int, nodeCount)
		for i, d := range disks {
			perNode[d.Node] = append(perNode[d.Node], i)
		}
		order := make([]int, nodeCount)
		for range p.PGCount {
			for i := range order {
				order[i] = i
			}
			r.Shuffle(nodeCount, func(i, j int) { order[i], order[j] = order[j], order[i] })
			for _, node := range order[:p.Replicas] {
				disks[perNode[node][r.Below(uint64(len(perNode[node])))]].Slots++
			}
		}
		empty := p.Capacity(disks, 0)
		held := r.Below(empty.Available/uint64(p.PGCount)+1) * uint64(p.PGCount)
		for i := range disks {
			own := held * uint64(disks[i].Slots) / uint64(p.PGCount)
			disks[i].Own, disks[i].Live = own, disks[i].Live+own
		}
		f := p.Capacity(disks, 0)

		var inv sim.Invariants
		inv.Register("pool figures within balanced", func() error { return figureBounds(p, &f) })
		inv.Register("pool stranded fraction does not move as the pool fills", func() error {
			if f.Stranded != empty.Stranded || f.Balanced != empty.Balanced || f.Reprotectable != empty.Reprotectable {
				return fmt.Errorf("holding %d moves f from %v to %v, balanced from %d to %d", held, empty.Stranded, f.Stranded, empty.Balanced, f.Balanced)
			}
			return nil
		})
		inv.Register("pool reprotectable is the worst single loss", func() error { return worstLoss(p, disks, &f) })
		if err := inv.Check(); err != nil {
			t.Fatalf("pool %+v held %d: %v", p, held, err)
		}
		var c Conditions
		c.Update(&f, true)
		cases[0] += b2i(held > 0)
		cases[1] += b2i(f.Stranded > 0)
		cases[2] += b2i(c.CannotReprotect)
		cases[3] += b2i(f.Loss.Kind == LossDisk)
	})
	if !seeds.Default() {
		return
	}
	t.Logf("%d seeds: %d hold data, %d strand, %d CannotReprotect, %d reprotect over a disk loss", n, cases[0], cases[1], cases[2], cases[3])
	if slices.Contains(cases[:], 0) {
		t.Fatal("the layouts missed a case")
	}
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

func figureBounds(p Pool, f *Figures) error {
	w := uint64(p.Replicas)
	if f.Available+f.Held > f.Balanced || f.Balanced > f.Capacity/w {
		return fmt.Errorf("available %d + held %d, balanced %d, capacity ÷ b %d", f.Available, f.Held, f.Balanced, f.Capacity/w)
	}
	g := func(d uint64) bool {
		var sum uint64
		for _, dom := range f.Domains {
			sum += min(dom.Capacity, d)
		}
		return sum >= w*d
	}
	if !g(f.Balanced) || g(f.Balanced+1) {
		return fmt.Errorf("balanced %d is not the largest D with Σ min(cᵢ, D) ≥ b·D", f.Balanced)
	}
	var stranded uint64
	for _, d := range f.Domains {
		stranded += d.Stranded
	}
	if want := f.Capacity - w*f.Balanced; stranded > want || want-stranded > uint64(len(f.Domains))*w {
		return fmt.Errorf("stranded rooms sum to %d, capacity − b·balanced is %d", stranded, want)
	}
	return nil
}

func worstLoss(p Pool, disks []Disk, f *Figures) error {
	w := uint64(p.Replicas)
	caps := make([]uint64, len(f.Domains))
	for i, d := range f.Domains {
		caps[i] = d.Capacity
	}
	var c Conditions
	c.Update(f, true)
	switch {
	case p.Replicas == 1 && c.CannotReprotect:
		return errors.New("CannotReprotect on a one-copy pool")
	case f.Loss.Kind == LossDomain && uint64(len(caps)-1) < w:
		return errors.New("a domain loss whose survivors cannot hold the width")
	case f.Loss.Kind == LossNone && c.CannotReprotect:
		return errors.New("CannotReprotect with no loss counted")
	}
	if !f.SpareDomain {
		return nil
	}
	least := ^uint64(0)
	for i := range caps {
		least = min(least, balanced(slices.Delete(slices.Clone(caps), i, i+1), w, 1, 1))
	}
	if f.Reprotectable != least {
		return fmt.Errorf("reprotectable %d, the least over single domains %d", f.Reprotectable, least)
	}
	for i := range f.Disks {
		left := slices.Clone(caps)
		left[f.domainIndex(disks[f.Disks[i].ID].Node)] -= f.Disks[i].Capacity
		if b := balanced(left, w, 1, 1); f.Reprotectable > b {
			return fmt.Errorf("reprotectable %d above balanced %d after losing disk %d", f.Reprotectable, b, f.Disks[i].ID)
		}
	}
	return nil
}
