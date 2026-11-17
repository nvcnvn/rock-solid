package chunkstore

import (
	"bytes"
	"slices"
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

const strandSeeds = 20

// strandedChunk is a chunk whose acknowledged write X sits under Y, a write never acknowledged, on the primary and the new member, with a primary that restarted or moved since Y.
type strandedChunk struct {
	*soleRun
	ch                      core.ChunkID
	x                       uint64
	primary, leaving, added uint32
}

// plantAll sets a plant on every replica and on those a restart builds.
func plantAll(c *rcluster, set func(*replicaPlants)) {
	set(&c.replicaPlants)
	for n := range c.reps {
		set(&c.reps[n].plant)
	}
}

// seededSoleRun is newSoleRun over the scheduler of seed.
func seededSoleRun(t *testing.T, seed uint64, replicas, disks int) *soleRun {
	geo, err := disklog.GeometryFor(diskBytes)
	if err != nil {
		t.Fatal(err)
	}
	return seededSoleRunOn(t, seed, replicas, disks, sim.DefaultSettings(), geo, nil)
}

// seededSoleRunOn is seededSoleRun under set, on disks cut as geo, each store's configuration changed by edit if set.
func seededSoleRunOn(t *testing.T, seed uint64, replicas, disks int, set sim.Settings, geo disklog.Geometry, edit func(*Config)) *soleRun {
	a := &authority{pool: Pool{Replicas: replicas, MinLive: 2, FailureDomain: DomainNode, MaxPerDomain: 1, Locality: LocalitySpread, Overcommit: true, WriteLimit: 90, PGCount: 1},
		down: map[uint32]core.Tick{}, drain: map[uint32]bool{}}
	for n := range disks {
		if _, err := a.rec.Report(uint32(n), 0, 0x5eed+uint64(n), ""); err != nil {
			t.Fatal(err)
		}
	}
	var empty Table
	tbl, _ := Next(&empty, &Inputs{Pools: a.pools()})
	sized := cloneTable(tbl)
	_, e := tbl.entry(pg0)
	for d := uint32(1); d <= uint32(disks); d++ {
		if !slices.ContainsFunc(e.Members, func(m Member) bool { return m.Disk == d }) {
			sized.Pools[0].PGs = append(sized.Pools[0].PGs, PGEntry{Members: []Member{{Disk: d}}, Interval: 1})
		}
	}
	c := newRClusterOnDisks(t, sized, sim.New(seed, set), edit, geo)
	if len(sized.Pools[0].PGs) > 1 {
		next := cloneTable(tbl)
		next.Epoch = sized.Epoch + 1
		c.publish(next, true)
	}
	return &soleRun{c: c, a: a, isolated: map[uint32]bool{}}
}

// strandedMove is a rep2 PG draining a member to a third disk, no disk lost, after yPastTheLeaving: Y's send to the leaving member dies with the primary, as the table removing that member commits.
func strandedMove(t *testing.T, seed uint64, yOff uint32, plant func(*replicaPlants)) *strandedChunk {
	r := seededSoleRun(t, seed, 2, 3)
	s := yPastTheLeaving(t, r, yOff, plant)
	restartPastTheMove(t, r)
	return s
}

// restartPastTheMove crashes the primary, publishes the table that removes the leaving member, and restarts the primary, whose replay gates Y until its next decision.
func restartPastTheMove(t *testing.T, r *soleRun) {
	c := r.c
	ms := r.members()
	primary, leaving := ms[0].Disk, ms[1].Disk
	p, l := uint32(c.node(primary)), uint32(c.node(leaving))
	c.s.CrashRandom(p)
	r.isolated[primary] = true
	r.publish()
	if ms := r.members(); slices.ContainsFunc(ms, func(m Member) bool { return m.Disk == leaving }) {
		t.Fatalf("no removal: members %v", ms)
	}
	c.s.Heal(p, l)
	c.s.Restart(p)
	delete(r.isolated, primary)
	r.tick(10_000_000)
}

// movedPrimary is the model's NoStranded counterexample on real placement, as neg-keep-past-member has it: after yPastTheLeaving one table removes the leaving member and puts the new one, which holds Y logged, first. Its peering reaches the leaving member through a past interval. That member lacks Y, which every up member holds, so peering keeps Y; under plant it vetoes Y instead, and each member lists the chunk.
func movedPrimary(t *testing.T, r *soleRun, yOff uint32, plant func(*replicaPlants)) *strandedChunk {
	s := yPastTheLeaving(t, r, yOff, plant)
	c := s.c
	s.a.edits = append(s.a.edits, Edit{PG: pg0, Disks: []uint32{s.added, s.primary}})
	s.publish()
	if ms := s.members(); len(ms) != 2 || ms[0].Disk != s.added {
		t.Fatalf("no move of the primary: members %v", ms)
	}
	c.s.Heal(uint32(c.node(s.primary)), uint32(c.node(s.leaving)))
	s.tick(10_000_000)
	return s
}

// yPastTheLeaving writes X, drains a member of a rep2 PG to a third disk until the interval is recovered, and writes Y with the primary cut off from the leaving member.
func yPastTheLeaving(t *testing.T, r *soleRun, yOff uint32, plant func(*replicaPlants)) *strandedChunk {
	c := r.c
	plantAll(c, plant)
	s := &strandedChunk{soleRun: r, ch: chunkIn(pg0, 1, 0)}
	id := c.write(pg0, s.ch, 0, bytes.Repeat([]byte{0x11}, core.BlockSize))
	c.until(id)
	s.x = c.reply(id).Version
	c.settle()
	ms := r.members()
	s.primary, s.leaving = ms[0].Disk, ms[1].Disk
	r.a.moves = true
	r.a.drain[s.leaving] = true
	r.publish()
	if ms = r.members(); len(ms) != 3 {
		t.Fatalf("no move: members %v", ms)
	}
	s.added = ms[2].Disk
	c.untilActive()
	c.settle()
	_, e := c.table.entry(pg0)
	if !slices.ContainsFunc(c.recovered, func(f PGFields) bool { return f.Interval == e.Interval }) {
		t.Fatalf("interval %d not reported recovered: %+v", e.Interval, c.recovered)
	}
	c.s.Partition(uint32(c.node(s.primary)), uint32(c.node(s.leaving)), true)
	c.write(pg0, s.ch, yOff, bytes.Repeat([]byte{0x22}, core.BlockSize))
	c.runFor(5_000)
	return s
}

// disk is what the store of disk holds of the chunk, by its index.
func (s *strandedChunk) disk(d uint32) Chunk { return s.c.listed(s.c.node(d), s.ch) }

// block is the first byte of block b of the chunk as disk maps it.
func (s *strandedChunk) block(d uint32, b uint16) byte { return s.c.diskBlock(s.c.node(d), s.ch, b)[0] }

func noPlant(*replicaPlants) {}

func keepPastMember(p *replicaPlants) { p.keepPastMember = true }

// X was acknowledged, no disk was lost: after the primary restarts or moves, on every seed the members hold the chunk clean, and X reads. The removed disk's copy is not needed.
func TestAnAcknowledgedWriteReadsAfterAMove(t *testing.T) {
	shapes := []func(seed uint64, yOff uint32) *strandedChunk{
		func(seed uint64, yOff uint32) *strandedChunk { return strandedMove(t, seed, yOff, noPlant) },
		func(seed uint64, yOff uint32) *strandedChunk {
			return movedPrimary(t, seededSoleRun(t, seed, 2, 3), yOff, noPlant)
		},
	}
	for seed := uint64(1); seed <= strandSeeds; seed++ {
		for _, yOff := range []uint32{core.BlockSize, 0} {
			for i, shape := range shapes {
				s := shape(seed, yOff)
				rp, got := s.read(s.ch)
				if rp.Status != StatusOK {
					t.Fatalf("shape %d, seed %d: acknowledged X read %+v, members %v", i, seed, rp, s.members())
				}
				want := byte(0x11)
				if yOff == 0 {
					want = 0x22
				}
				if got[0] != want {
					t.Fatalf("shape %d, seed %d: read %#x", i, seed, got[0])
				}
				for _, d := range []uint32{s.primary, s.added} {
					if x := s.disk(d); x.ListedIn != 0 {
						t.Fatalf("shape %d, seed %d: disk %d holds %+v", i, seed, d, x)
					}
				}
			}
		}
	}
}

// With keepPastMember the removed disk's veto leaves both members listing the chunk: the decision calls it unfound, as the disk cannot be a recovery source, and reads answer that for as long as the run lasts.
func TestKeepPastMemberLeavesAMoveUnfound(t *testing.T) {
	for seed := uint64(1); seed <= strandSeeds; seed++ {
		for _, yOff := range []uint32{core.BlockSize, 0} {
			s := movedPrimary(t, seededSoleRun(t, seed, 2, 3), yOff, keepPastMember)
			c := s.c
			check := func() {
				t.Helper()
				if a, b := s.disk(s.primary), s.disk(s.added); a.ListedIn == 0 || b.ListedIn == 0 || a.Listed != s.x || b.Listed != s.x {
					t.Fatalf("seed %d: members hold %+v and %+v", seed, a, b)
				}
				if x := s.disk(s.leaving); x.Whole != s.x || x.Version != s.x || x.ListedIn != 0 || s.block(s.leaving, 0) != 0x11 {
					t.Fatalf("seed %d: removed disk holds %+v", seed, x)
				}
				if rp, _ := s.read(s.ch); rp.Status != StatusUnfound {
					t.Fatalf("seed %d: read %+v", seed, rp)
				}
			}
			check()
			if n, stopped := c.reps[c.primary(pg0)].Unfound(pg0); n != 1 || stopped {
				t.Fatalf("seed %d: unfound %d, stopped %v", seed, n, stopped)
			}
			s.tick(120_000_000)
			check()
		}
	}
}

// A chunk with no source that nothing calls unfound breaks the source invariant.
func TestNeverUnfoundBreaksTheSourceInvariant(t *testing.T) {
	r := seededSoleRun(t, 1, 2, 3)
	r.c.planted = true
	watchEpochs(r.c, nil)
	movedPrimary(t, r, core.BlockSize, func(p *replicaPlants) { p.keepPastMember, p.neverUnfound = true, true })
	if r.c.found == nil || !strings.Contains(r.c.found.Error(), invHasSource) {
		t.Fatalf("found %v", r.c.found)
	}
}

// Without the restart too: the primary still holds Y gated, peering hears the removed disk, and every up member holding Y keeps it. The new member's replies are held back so that the removed disk's reply comes first, as a restart's wait lets it.
func TestAPrimaryThatDidNotRestartRecoversTheMove(t *testing.T) {
	r := seededSoleRun(t, 1, 2, 3)
	c := r.c
	ch := chunkIn(pg0, 1, 0)
	id := c.write(pg0, ch, 0, bytes.Repeat([]byte{0x11}, core.BlockSize))
	c.until(id)
	x := c.reply(id).Version
	c.settle()
	ms := r.members()
	primary, leaving := ms[0].Disk, ms[1].Disk
	r.a.moves = true
	r.a.drain[leaving] = true
	r.publish()
	added := r.members()[2].Disk
	c.untilActive()
	c.settle()

	p, l, n := uint32(c.node(primary)), uint32(c.node(leaving)), uint32(c.node(added))
	c.s.Partition(p, l, true)
	c.write(pg0, ch, core.BlockSize, bytes.Repeat([]byte{0x22}, core.BlockSize))
	c.runFor(5_000)
	c.s.Heal(p, l)
	c.s.Queue(n, p, 50_000)
	r.publish()
	r.tick(10_000_000)
	if rp, got := r.read(ch); rp.Status != StatusOK || rp.Version <= x || got[0] != 0x11 {
		t.Fatalf("X read %+v, %#x, members %v", rp, got, r.members())
	}
	for _, d := range []uint32{primary, added} {
		if got := c.listed(c.node(d), ch); got.ListedIn != 0 {
			t.Fatalf("disk %d still lists the chunk: %+v", d, got)
		}
	}
}

// An operator's edit naming the removed disk heals an unfound chunk: the disk is a clean source of X, and Y is gone.
func TestAnEditNamingTheRemovedDiskHealsAnUnfoundChunk(t *testing.T) {
	for _, yOff := range []uint32{core.BlockSize, 0} {
		s := movedPrimary(t, seededSoleRun(t, 1, 2, 3), yOff, keepPastMember)
		s.a.edits = append(s.a.edits, Edit{PG: pg0, Disks: []uint32{s.primary, s.leaving}})
		s.tick(10_000_000)
		if rp, got := s.read(s.ch); rp.Status != StatusOK || rp.Version != s.x || got[0] != 0x11 {
			t.Fatalf("X read %+v, %#x, members %v", rp, got, s.members())
		}
	}
}

// No write lands on an unfound chunk, whole-block or partial, so the removed disk still heals it.
func TestAWriteOnAnUnfoundChunkKeepsTheWayBack(t *testing.T) {
	s := movedPrimary(t, seededSoleRun(t, 1, 2, 3), core.BlockSize, keepPastMember)
	c := s.c
	for _, w := range []core.RequestID{
		c.write(pg0, s.ch, 10, []byte{0x33}),
		c.write(pg0, s.ch, 2*core.BlockSize, bytes.Repeat([]byte{0x44}, core.BlockSize)),
	} {
		if rp, ok := s.answered(w, 5_000_000); !ok || rp.Status != StatusUnfound {
			t.Fatalf("write answered %+v, %v", rp, ok)
		}
	}
	s.a.edits = append(s.a.edits, Edit{PG: pg0, Disks: []uint32{s.primary, s.leaving}})
	s.tick(10_000_000)
	if rp, got := s.read(s.ch); rp.Status != StatusOK || rp.Version != s.x || got[0] != 0x11 {
		t.Fatalf("X read %+v, %#x, members %v", rp, got, s.members())
	}
}

// With writeOnUnfound a whole-block write to the unfound chunk is not answered unfound: the primary lists the chunk, so the write waits for recovery, which has no source, and its client never hears.
func TestWriteOnUnfoundHoldsAWriteForEver(t *testing.T) {
	s := movedPrimary(t, seededSoleRun(t, 1, 2, 3), core.BlockSize, func(p *replicaPlants) { p.keepPastMember, p.writeOnUnfound = true, true })
	c := s.c
	z := c.write(pg0, s.ch, 2*core.BlockSize, bytes.Repeat([]byte{0x44}, core.BlockSize))
	if rp, ok := s.answered(z, 60_000_000); ok {
		t.Fatalf("a whole-block write answered %+v", rp)
	}
	if l := &c.reps[c.node(s.added)].leads[0]; !l.isUnfound(s.ch) || len(l.held) != 1 {
		t.Fatalf("lead %+v", l)
	}
}

// recoveringLister has a disk come back and list a chunk on a rep3 PG, and before recovery reaches it Y lands on the other two, not on it, and the primary restarts. noRecovery stands in for recovery that has not reached the chunk.
func recoveringLister(t *testing.T, r *soleRun, plant func(*replicaPlants)) (ch core.ChunkID, x uint64, a, b, d uint32) {
	ch, x, a, b, d = yPastTheLister(t, r, plant)
	c := r.c
	an, dn := uint32(c.node(a)), uint32(c.node(d))
	c.s.CrashRandom(an)
	c.s.Heal(an, dn)
	c.s.Restart(an)
	r.tick(20_000_000)
	return ch, x, a, b, d
}

// rotatedLister is recoveringLister with the primary handing its place to the other holder of Y in place of a restart, as neg-lister-vetoes has it.
func rotatedLister(t *testing.T, r *soleRun, plant func(*replicaPlants)) (ch core.ChunkID, x uint64, a, b, d uint32) {
	ch, x, a, b, d = yPastTheLister(t, r, plant)
	c := r.c
	r.a.edits = append(r.a.edits, Edit{PG: pg0, Disks: []uint32{b, a, d}})
	r.publish()
	if ms := r.members(); ms[0].Disk != b {
		t.Fatalf("no rotation: members %v", ms)
	}
	c.s.Heal(uint32(c.node(a)), uint32(c.node(d)))
	r.tick(20_000_000)
	return ch, x, a, b, d
}

// yPastTheLister has a disk come back and list a chunk on a rep3 PG, and before recovery reaches it Y lands on the other two, not on it.
func yPastTheLister(t *testing.T, r *soleRun, plant func(*replicaPlants)) (ch core.ChunkID, x uint64, a, b, d uint32) {
	c := r.c
	plantAll(c, plant)
	ch = chunkIn(pg0, 1, 0)
	c.until(c.write(pg0, ch, 0, bytes.Repeat([]byte{0x11}, core.BlockSize)))
	c.settle()
	ms := r.members()
	a, b, d = ms[0].Disk, ms[1].Disk, ms[2].Disk
	r.lose(d)
	r.publish()
	c.untilActive()
	c.settle()
	id := c.write(pg0, ch, 0, bytes.Repeat([]byte{0x33}, core.BlockSize))
	c.until(id)
	x = c.reply(id).Version
	c.settle()
	plantAll(c, func(p *replicaPlants) { p.noRecovery = true })
	r.back(d)
	r.publish()
	c.untilActive()
	c.settle()
	if got := c.listed(c.node(d), ch); got.Listed != x || got.ListedIn == 0 {
		t.Fatalf("returned disk holds %+v", got)
	}

	c.s.Partition(uint32(c.node(a)), uint32(c.node(d)), true)
	c.write(pg0, ch, core.BlockSize, bytes.Repeat([]byte{0x22}, core.BlockSize))
	c.runFor(5_000)
	return ch, x, a, b, d
}

// The disk that lacks Y lists the chunk, so it is no source and has no veto: after a restart or a rotation the other two hold Y clean, X reads, and recovery brings the third once it runs.
func TestALeadChangeDuringRecoveryKeepsASource(t *testing.T) {
	for i, shape := range []func(*testing.T, *soleRun, func(*replicaPlants)) (core.ChunkID, uint64, uint32, uint32, uint32){recoveringLister, rotatedLister} {
		r := seededSoleRun(t, 1, 3, 3)
		c := r.c
		ch, x, a, b, d := shape(t, r, noPlant)
		for _, m := range []uint32{a, b} {
			if got := c.listed(c.node(m), ch); got.ListedIn != 0 || got.Whole <= x {
				t.Fatalf("shape %d: disk %d holds %+v", i, m, got)
			}
		}
		if rp, got := r.read(ch); rp.Status != StatusOK || got[0] != 0x33 {
			t.Fatalf("shape %d: read %+v, %#x", i, rp, got[0])
		}
		plantAll(c, func(p *replicaPlants) { p.noRecovery = false })
		r.tick(20_000_000)
		if got := c.listed(c.node(d), ch); got.ListedIn != 0 {
			t.Fatalf("shape %d: disk %d holds %+v", i, d, got)
		}
	}
}

// With listerVetoes the disk that lacks Y drops it, which leaves every member listing: unfound with every disk up, and X held under Y by two of them.
func TestListerVetoesLeavesNoMemberASource(t *testing.T) {
	r := seededSoleRun(t, 1, 3, 3)
	c := r.c
	ch, x, a, b, d := rotatedLister(t, r, func(p *replicaPlants) { p.listerVetoes = true })
	for _, m := range []uint32{a, b, d} {
		if got := c.listed(c.node(m), ch); got.Listed != x || got.ListedIn == 0 {
			t.Fatalf("disk %d holds %+v", m, got)
		}
	}
	r.tick(120_000_000)
	if rp, _ := r.read(ch); rp.Status != StatusUnfound {
		t.Fatalf("read %+v", rp)
	}
}

// tornMember has a member of a rep3 PG crash with part of Y, a whole-chunk write the third member never gets, durable and its last record lost. A peering without the member drops Y, and the member, restarted while cut off, comes back reporting X whole and Y logged. It returns the chunk, X's version and the member's node.
func tornMember(t *testing.T, seed uint64, plant func(*replicaPlants)) (*soleRun, core.ChunkID, uint64, int) {
	r := seededSoleRun(t, seed, 3, 3)
	c := r.c
	plantAll(c, plant)
	ch := chunkIn(pg0, 1, 0)
	id := c.write(pg0, ch, 0, bytes.Repeat([]byte{0x11}, 128*core.BlockSize))
	c.until(id)
	x := c.reply(id).Version
	c.settle()
	ms := r.members()
	p, m, o := c.node(ms[0].Disk), c.node(ms[1].Disk), c.node(ms[2].Disk)
	c.s.Partition(uint32(p), uint32(o), true)
	c.write(pg0, ch, 0, bytes.Repeat([]byte{0x22}, 128*core.BlockSize))
	for !holdsInPart(c.reps[m].Store(), ch, x) {
		if !c.step() {
			t.Fatal("the member never held Y in part")
		}
	}
	c.s.Crash(uint32(m), func(int, int, core.LBA) bool { return false })
	r.lose(ms[1].Disk)
	r.publish()
	c.s.Heal(uint32(p), uint32(o))
	c.untilActive()
	c.settle()
	r.back(ms[1].Disk)
	r.tick(20_000_000)
	return r, ch, x, m
}

// holdsInPart reports whether s has a version of ch above v with some of its records durable and not its last.
func holdsInPart(s *Store, ch core.ChunkID, v uint64) bool {
	ci, ok := s.chunkOf[ch]
	if !ok {
		return false
	}
	for _, oi := range s.chunks[ci].pending {
		if o := &s.ops[oi]; o.version > v {
			durable, last := 0, false
			for _, it := range o.items {
				if it.state == itemDurable {
					durable++
					last = last || it.entry.Last
				}
			}
			return durable > 0 && !last
		}
	}
	return false
}

// A member that holds the kept version whole and a later one in part is listed, and recovery rewrites it: on every seed it maps X.
func TestAMemberHoldingAVersionInPartIsListed(t *testing.T) {
	for seed := uint64(1); seed <= strandSeeds; seed++ {
		r, ch, _, m := tornMember(t, seed, noPlant)
		for b := range uint16(128) {
			if got := r.c.diskBlock(m, ch, b)[0]; got != 0x11 {
				t.Fatalf("seed %d: block %d reads %#x, members %v", seed, b, got, r.members())
			}
		}
	}
}

// With unlistTorn the member is left off the list, and on every seed it maps the part of Y it holds, which peering dropped.
func TestUnlistTornMapsADroppedVersion(t *testing.T) {
	for seed := uint64(1); seed <= strandSeeds; seed++ {
		r, ch, _, m := tornMember(t, seed, func(p *replicaPlants) { p.unlistTorn = true })
		if x := r.c.listed(m, ch); x.ListedIn != 0 {
			t.Fatalf("seed %d: the member lists %+v", seed, x)
		}
		mapsY := false
		for b := range uint16(128) {
			mapsY = mapsY || r.c.diskBlock(m, ch, b)[0] == 0x22
		}
		if !mapsY {
			t.Fatalf("seed %d: the member maps none of Y", seed)
		}
	}
}
