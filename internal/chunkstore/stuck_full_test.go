package chunkstore

import (
	"bytes"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// P23 on the real control plane: a rep3 PG on three of four disks loses a member, declared lost, and
// its refill copies onto the fourth until the copies would pass its log reserve, which its node
// reports and the primary's recovery gate keeps, before the copy ends. The leader raises Stuck on
// the PG from its own record of when the PG last copied. A fifth disk joining takes the full
// member's place with no edit: placement parks the full member beside it before any recovered
// report, and Stuck clears once the new disk copies.
func TestARefillStoppedAtALogReserveGoesStuck(t *testing.T) {
	a := &authority{pool: Pool{Replicas: 3, MinLive: 2, FailureDomain: DomainNode, MaxPerDomain: 1, Locality: LocalitySpread, Overcommit: true, WriteLimit: 90, PGCount: 1},
		down: map[uint32]core.Tick{}, drain: map[uint32]bool{}, opened: map[uint32][]OpenedLog{}}
	tbl := realControl(t, a, 4)
	ms := tbl.Entry(pg0).Members
	var full uint32
	for d := uint32(1); d <= 4; d++ {
		if !slices.ContainsFunc(ms, func(m Member) bool { return m.Disk == d }) {
			full = d
		}
	}
	geo, err := disklog.GeometryFor(diskBytes)
	if err != nil {
		t.Fatal(err)
	}
	started := 0
	// A sixth node, which never joins, makes the first table's PG count a power of two.
	c := newSoleCluster(t, tbl, 6, func(cfg *Config) {
		if started == int(full)-1 {
			cfg.Log.NoReserve, cfg.Log.Reserve = false, geo.SegmentCount-24
		}
		started++
	})
	added := 4
	a.late = map[int]bool{added: true, added + 1: true}
	size := OpenedLog{Blocks: uint64(geo.SegmentCount) * uint64(geo.SegmentBlocks), Reserve: uint64(geo.SegmentCount-24) * uint64(geo.SegmentBlocks)}
	a.sizes = map[uint32]OpenedLog{full - 1: size}
	for k := range c.reps {
		c.call(k, func(ctx *core.Context, r *Replica, out *ReplicaOutput) { r.SetDisk(full, size.Blocks, size.Reserve) })
	}
	a.attach(c)
	r := &soleRun{c: c, a: a, isolated: map[uint32]bool{}, acked: map[core.ChunkID]uint64{}}
	stuck := func() bool {
		cond, ok := a.cp.Conditions(testPool)
		return ok && slices.Contains(cond.Stuck, 0)
	}

	var chunks []core.ChunkID
	for i := range uint32(40) {
		ch := chunkIn(pg0, 1, i)
		chunks = append(chunks, ch)
		for b := range uint32(4) {
			c.until(c.write(pg0, ch, b*16*core.BlockSize, bytes.Repeat([]byte{byte(i)}, 16*core.BlockSize)))
		}
	}
	c.settle()

	lost := ms[len(ms)-1].Disk
	r.lose(lost)
	for i := 0; r.lost(lost) != nil; i++ {
		if i == 30 {
			t.Fatalf("disk %d never declared lost", lost)
		}
		r.tick(1_000_000)
	}
	if !slices.ContainsFunc(r.members(), func(m Member) bool { return m.Disk == full }) {
		t.Fatalf("PG 0 refilled to %v, not onto disk %d", r.members(), full)
	}
	n := c.node(full)
	lacking := func(n int) int {
		ix := c.reps[n].Store().Index()
		k := 0
		for _, ch := range chunks {
			if x, _ := ix.Chunk(testPool, ch); x.ListedIn != 0 || x.Whole == 0 {
				k++
			}
		}
		return k
	}
	var at core.Tick
	for live := uint64(0); ; {
		r.tick(1_000_000)
		if l := c.reps[n].Store().Log().LiveBlocks(); l != live {
			live, at = l, c.s.Time()
		} else if c.s.Time()-at > 5_000_000 {
			break
		}
	}
	if c.reps[n].Store().Log().FreeOutsideReserve() == 0 || lacking(n) == 0 {
		t.Fatalf("disk %d stopped copying with %d segments free outside its reserve and %d chunks lacking", full, c.reps[n].Store().Log().FreeOutsideReserve(), lacking(n))
	}
	for !stuck() {
		if c.s.Time() > at+StuckAfter+5_000_000 {
			t.Fatalf("PG 0 not Stuck %d s after disk %d stopped copying", (c.s.Time()-at)/1_000_000, full)
		}
		r.tick(1_000_000)
	}

	a.join(added)
	joined := c.s.Time()
	for stuck() {
		if c.s.Time() > joined+StuckAfter {
			t.Fatalf("PG 0 still Stuck %d s after disk 5 joined: %v", (c.s.Time()-joined)/1_000_000, r.members())
		}
		r.tick(1_000_000)
	}
	if ms := r.members(); !slices.ContainsFunc(ms, func(m Member) bool { return m.Disk == 5 && !m.Leaving }) || slices.ContainsFunc(ms, func(m Member) bool { return m.Disk == full }) || lacking(added) != 0 {
		t.Fatalf("PG 0 cleared Stuck as %v with disk 5 lacking %d chunks", ms, lacking(added))
	}
}
