package chunkstore

import (
	"bytes"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// rollbackRun is rep2 with X acknowledged on both members and Y, never acknowledged, on the primary alone, the two cut
// apart while Y was ordered.
type rollbackRun struct {
	*soleRun
	ch   core.ChunkID
	p, m uint32
}

func newRollbackRun(t *testing.T, seed uint64) *rollbackRun {
	r := seededSoleRun(t, seed, 2, 3)
	c := r.c
	ch := chunkIn(pg0, 1, 0)
	id := c.write(pg0, ch, 0, bytes.Repeat([]byte{0x11}, core.BlockSize))
	c.until(id)
	c.settle()
	ms := r.members()
	rr := &rollbackRun{soleRun: r, ch: ch, p: ms[0].Disk, m: ms[1].Disk}
	plantAll(c, func(p *replicaPlants) { p.noRecovery = true })
	c.s.Partition(rr.node(rr.p), rr.node(rr.m), true)
	c.write(pg0, ch, core.BlockSize, bytes.Repeat([]byte{0x22}, core.BlockSize))
	c.runFor(5_000)
	return rr
}

func (r *rollbackRun) node(d uint32) uint32 { return uint32(r.c.node(d)) }

func (r *rollbackRun) heal() { r.c.s.Heal(r.node(r.p), r.node(r.m)) }

// restart crashes the primary's node and starts it again.
func (r *rollbackRun) restart() {
	r.c.s.CrashRandom(r.node(r.p))
	r.c.s.Restart(r.node(r.p))
}

// dropY has a peering of the next interval drop Y, the primary listing the chunk for it while its image is X.
func (r *rollbackRun) dropY(t *testing.T) {
	t.Helper()
	_, e := r.c.table.entry(pg0)
	r.a.asks = []PGInterval{{pg0, e.Interval}}
	r.publish()
	r.heal()
	r.tick(5_000_000)
	if l := r.c.listed(r.c.node(r.p), r.ch); l.Listed == 0 {
		t.Fatalf("primary %+v: the shape needs it listing X for its dropped Y", l)
	}
}

// loseMemberAndRead has the other member's disk die and be declared lost, lets recovery run and reads X back, and
// the dropped Y's block as zero.
func (r *rollbackRun) loseMemberAndRead(t *testing.T) {
	t.Helper()
	rp, data := r.xAfterLoss(t)
	if !readsX(rp, data) {
		t.Fatalf("X read %+v %x; primary %+v; members %v", rp, data[:min(1, len(data))], r.c.listed(r.c.node(r.p), r.ch), r.members())
	}
	if len(data) != 2*core.BlockSize || !bytes.Equal(data[core.BlockSize:], make([]byte, core.BlockSize)) {
		t.Fatalf("the dropped Y reads back: %d bytes, block 1 starts %x", len(data), data[min(core.BlockSize, len(data)-1):][:1])
	}
}

func (r *rollbackRun) xAfterLoss(t *testing.T) (Response, []byte) {
	t.Helper()
	r.lose(r.m)
	r.tick(DegradedRefillAfter + 2_000_000)
	r.declare(t, r.m)
	r.tick(5_000_000)
	plantAll(r.c, func(p *replicaPlants) { p.noRecovery = false })
	r.tick(60_000_000)
	return r.readLen(r.ch, 2*core.BlockSize)
}

func readsX(rp Response, data []byte) bool {
	return rp.Status == StatusOK && len(data) > 0 && data[0] == 0x11
}

// The rollback shapes, each up to the other member's loss.
func restartPending(_ *testing.T, r *rollbackRun) {
	r.restart()
	r.heal()
	r.tick(10_000_000)
}

func restartAfterDrop(t *testing.T, r *rollbackRun) {
	r.dropY(t)
	r.restart()
	r.tick(10_000_000)
}

func decideAfterRestart(t *testing.T, r *rollbackRun) {
	restartAfterDrop(t, r)
	_, e := r.c.table.entry(pg0)
	r.a.asks = []PGInterval{{pg0, e.Interval}}
	r.publish()
	r.tick(5_000_000)
}

func bouncePending(_ *testing.T, r *rollbackRun) {
	r.lose(r.p)
	r.tick(3_000_000)
	r.back(r.p)
	r.heal()
	r.tick(5_000_000)
}

func bounceAfterDrop(t *testing.T, r *rollbackRun) {
	r.dropY(t)
	r.lose(r.p)
	r.tick(3_000_000)
	r.back(r.p)
	r.tick(5_000_000)
}

func TestARestartedPrimaryRollsBackItsPendingWrite(t *testing.T) {
	for seed := uint64(1); seed <= 3; seed++ {
		r := newRollbackRun(t, seed)
		restartPending(t, r)
		r.loseMemberAndRead(t)
	}
}

func TestAPrimaryRestartedAfterItsDropServesFromItsFlag(t *testing.T) {
	for seed := uint64(1); seed <= 3; seed++ {
		r := newRollbackRun(t, seed)
		restartAfterDrop(t, r)
		r.loseMemberAndRead(t)
	}
}

// A restarted primary decides again with the other member up, then that member's disk goes before recovery.
func TestARestartedPrimaryKeepsItsDropsAcrossItsNextDecision(t *testing.T) {
	r := newRollbackRun(t, 1)
	decideAfterRestart(t, r)
	r.loseMemberAndRead(t)
}

func TestABouncedPrimaryWithItsWritePendingRollsItBack(t *testing.T) {
	r := newRollbackRun(t, 1)
	bouncePending(t, r)
	r.loseMemberAndRead(t)
}

func TestABouncedPrimaryKeepsTheImageOfItsDroppedWrite(t *testing.T) {
	r := newRollbackRun(t, 1)
	bounceAfterDrop(t, r)
	r.loseMemberAndRead(t)
}

// Each replay rollback plant loses X on the shape that needs its rule.
func TestReplayRollbackPlantsLoseX(t *testing.T) {
	for _, tc := range []struct {
		name  string
		set   func(*replicaPlants, *plants)
		shape func(*testing.T, *rollbackRun)
	}{
		{"mapOnReplay", func(_ *replicaPlants, s *plants) { s.mapOnReplay = true }, restartPending},
		{"replayForgetsDrops", func(_ *replicaPlants, s *plants) { s.replayForgetsDrops = true }, restartAfterDrop},
		{"ignoreImageFlag", func(p *replicaPlants, _ *plants) { p.ignoreImageFlag = true }, restartAfterDrop},
		{"mapOnDemote", func(p *replicaPlants, _ *plants) { p.mapOnDemote = true }, bouncePending},
		{"forgetImageDown", func(p *replicaPlants, _ *plants) { p.forgetImageDown = true }, bounceAfterDrop},
	} {
		r := newRollbackRun(t, 1)
		c := r.c
		tc.set(&c.replicaPlants, &c.storePlants)
		for n := range c.reps {
			tc.set(&c.reps[n].plant, &c.reps[n].Store().plant)
		}
		tc.shape(t, r)
		if rp, data := r.xAfterLoss(t); readsX(rp, data) {
			t.Errorf("%s: X still reads", tc.name)
		}
	}
}

// The primary persists the decision that drops Y, which drops it there and then, and the other member's acknowledgement
// of its own list never comes, so the lead does not activate before the next peering.
func TestADecisionPersistedAndNotActivatedKeepsItsDrops(t *testing.T) {
	r := newRollbackRun(t, 1)
	_, e := r.c.table.entry(pg0)
	r.a.asks = []PGInterval{{pg0, e.Interval}}
	r.publish()
	r.heal()
	for i := 0; r.c.listed(r.c.node(r.p), r.ch).ListedIn <= e.Interval; i++ {
		if i == 100_000 {
			t.Fatal("the primary never persisted the dropping decision")
		}
		r.c.runFor(10)
	}
	r.c.s.Partition(r.node(r.p), r.node(r.m), true)
	if i := r.c.reps[r.c.node(r.p)].lead(pg0); i < 0 || r.c.reps[r.c.node(r.p)].leads[i].active {
		t.Fatal("the shape needs the lead not yet active")
	}
	if slices.Contains(r.c.reps[r.c.node(r.p)].Store().Unsettled(nil), r.ch) {
		t.Fatal("the persisted decision left Y gated")
	}
	r.loseMemberAndRead(t)
}

// The primary's own entry for the chunk it lists only for its dropped Y carries the image flag, and a restart replays it.
func TestThePrimarysEntryForItsDroppedWriteCarriesTheImageFlag(t *testing.T) {
	r := newRollbackRun(t, 1)
	r.dropY(t)
	if l := r.c.listed(r.c.node(r.p), r.ch); !l.Image {
		t.Fatalf("primary %+v: no image flag", l)
	}
	r.restart()
	r.c.runFor(1_000_000)
	if l := r.c.listed(r.c.node(r.p), r.ch); !l.Image {
		t.Fatalf("primary after a restart %+v: no image flag", l)
	}
}

// After a restart, the versions replay holds gated keep the replay point before their records and their segments from the
// cleaner, until a decision settles them, a whole one and one a crash tore alike; mapped, they count live where they lie.
func TestReplayGatedVersionsKeepTheirSegmentsAndTheReplayPoint(t *testing.T) {
	h := newHarness(t, cleanConfig())
	h.oracle = sim.NewOracle[writtenRecord](0, newStoreModel())
	whole, torn := chunk(2), chunk(3)
	h.persist(0, 1, 2, nil)
	h.gated(whole, 0, fill(4*core.BlockSize, 0x7e), 2)
	h.settle()
	rec := len(h.records)
	h.gated(torn, 0, pattern(9, core.ChunkSize), 2)
	for first := uint64(0); ; h.step() {
		if first == 0 && len(h.records) > rec {
			first = h.records[rec].lsn
		}
		if first != 0 && h.s.Log().DurableLSN() >= first+2 {
			break
		}
	}
	restart(h)
	if got := h.s.Unsettled(nil); !slices.Contains(got, whole) || !slices.Contains(got, torn) {
		t.Fatalf("replay left %v gated", got)
	}
	var segs []uint32
	oldest := uint64(0)
	for _, r := range h.records {
		if r.lost || !slices.ContainsFunc(r.entries, func(e disklog.Entry) bool { return e.Chunk == whole || e.Chunk == torn }) {
			continue
		}
		segs = append(segs, r.segment)
		if oldest == 0 {
			oldest = r.lsn
		}
	}
	for i := range 300 {
		id := h.write(chunk(1), 0, fill(64*core.BlockSize, byte(i)), 0)
		h.settle()
		h.ok(id)
	}
	h.clean(1000)
	if got := h.s.replayPoint(); got >= oldest {
		t.Fatalf("replay point %d past the gated record at LSN %d", got, oldest)
	}
	for _, s := range segs {
		if u, _ := h.s.Log().Usage(s); u.State != disklog.SegmentUsed || h.s.Log().Cleanable(s) {
			t.Fatalf("segment %d holding a gated record is %+v, cleanable %v", s, u, h.s.Log().Cleanable(s))
		}
	}
	h.persist(0, 1, 3, nil)
	h.settleChunk(whole, h.s.Newest(whole))
	h.settleChunk(torn, h.s.Newest(torn))
	if got := h.s.Unsettled(nil); len(got) != 0 {
		t.Fatalf("the decision left %v gated", got)
	}
	if got := h.readNow(whole, 0, 4*core.BlockSize); !bytes.Equal(got, fill(4*core.BlockSize, 0x7e)) {
		t.Fatalf("the whole version reads %x", got[:8])
	}
	h.holds()
}

// restartMember writes X to rep2's PG, restarts its member's node and lets 10 s pass with the PG in its interval, its
// store's index then mapping X, or not, and the chunks it keeps gated.
func restartMember(t *testing.T, seed uint64, plant func(*replicaPlants)) (mapped bool, gated []core.ChunkID) {
	r := seededSoleRun(t, seed, 2, 3)
	c := r.c
	plantAll(c, plant)
	ch := chunkIn(pg0, 1, 0)
	id := c.write(pg0, ch, 0, bytes.Repeat([]byte{0x11}, core.BlockSize))
	c.until(id)
	c.settle()
	m := c.node(r.members()[1].Disk)
	c.s.CrashRandom(uint32(m))
	c.s.Restart(uint32(m))
	r.tick(10_000_000)
	x, _ := c.reps[m].Store().Index().Chunk(testPool, ch)
	return x.Whole != 0, c.reps[m].Store().Unsettled(nil)
}

// A member restarts while its PG stays in its interval, so no decision of it comes on its own: the versions replay
// gated there settle once the member asks for a new interval (seed 410 of TestReplicatedWritesSurviveFaults).
func TestARestartedMemberSettlesItsReplayedWrites(t *testing.T) {
	for seed := uint64(1); seed <= 3; seed++ {
		if mapped, gated := restartMember(t, seed, func(*replicaPlants) {}); !mapped || len(gated) != 0 {
			t.Fatalf("seed %d: the restarted member maps X %v, keeps %v gated", seed, mapped, gated)
		}
	}
}

func TestAMemberThatNeverAsksKeepsItsReplayedWritesGated(t *testing.T) {
	if mapped, gated := restartMember(t, 1, func(p *replicaPlants) { p.memberNoAsk = true }); mapped || len(gated) == 0 {
		t.Fatalf("memberNoAsk: the restarted member maps X %v, keeps %v gated", mapped, gated)
	}
}

// A whole chunk a member wrote to a slot, which replay then holds, counts live in its record's segment once a decision
// maps it, so a later overwrite takes its blocks back out.
func TestAReplayHeldSlotWriteCountsOnceMapped(t *testing.T) {
	h := newHarnessOn(t, testConfig(), slotGeometry)
	c := chunk(2)
	h.sync(h.persist(0, 1, 2, nil))
	v := MakeVersion(2, 1)
	h.sync(h.memberWrite(c, 0, core.BlocksPerChunk, v, 0xaa))
	if got := h.s.Log().SlotsInUse(nil); len(got) != 1 {
		t.Fatalf("the whole chunk took slots %v", got)
	}
	h.inv.Off(invReplay)
	restart(h)
	if got := h.s.Unsettled(nil); !slices.Contains(got, c) {
		t.Fatalf("replay left %v gated", got)
	}
	h.sync(h.persist(0, 1, 3, nil))
	h.settleChunk(c, v)
	h.settle()
	h.holds()
	h.sync(h.memberWrite(c, 0, 4, MakeVersion(3, 1), 0xbb))
	h.holds()
}

// A whole chunk written to a slot flags its version's last entry: the chunk is whole at that version once it maps, and a
// member whose replay holds it and a write after it reports the later one whole to peering, which keeps both. Without
// the mark, peering drops the versions a member acknowledged after a slot write, and slot seeds 130, 354, 402, 418 and
// 930 of TestReplicatedWritesSurviveFaults find a primary missing writes it answered OK.
func TestASlotWriteIsWhole(t *testing.T) {
	h := newHarnessOn(t, testConfig(), slotGeometry)
	c := chunk(2)
	h.sync(h.persist(0, 1, 2, nil))
	v, w := MakeVersion(2, 1), MakeVersion(2, 2)
	h.sync(h.memberWrite(c, 0, core.BlocksPerChunk, v, 0xaa))
	if ch, _ := h.s.Index().Chunk(testPool, c); ch.Whole != v {
		t.Fatalf("the slot write left the chunk %+v, not whole at %#x", ch, v)
	}
	h.sync(h.memberWrite(c, 0, 1, w, 0xbb))
	h.inv.Off(invReplay)
	restart(h)
	got, _ := h.peerState(0, 1)
	if i := slices.IndexFunc(got, func(p ChunkPeer) bool { return p.Chunk == c }); i < 0 || got[i].Whole != w {
		t.Fatalf("the restarted member reports %+v, not %v whole at %#x", got, c, w)
	}
}

// A slot that a write replay holds overwrote, freed and taken again by another chunk's write replay also holds, stays in
// use until the second chunk lets it go and is then freed once. Freed while the second is held, the log can give the
// slot to a third write and free it twice: slot seed 322 of TestReplicatedWritesSurviveFaults then maps a slot the log
// holds free, and 594, 626, 898 and 914 panic on the second free.
func TestAReusedSlotOfTwoHeldWritesIsFreedOnce(t *testing.T) {
	h := newHarnessOn(t, testConfig(), slotGeometry)
	a, b := chunk(2), chunk(3)
	h.sync(h.persist(0, 1, 1, nil))
	h.sync(h.memberWrite(a, 0, core.BlocksPerChunk, MakeVersion(1, 1), 0xaa))
	first := h.slotsInUse()
	h.sync(h.persist(0, 1, 2, nil))
	h.sync(h.memberWrite(a, 0, core.BlocksPerChunk, MakeVersion(2, 1), 0xbb))
	h.sync(h.memberWrite(a, 0, 1, MakeVersion(2, 2), 0xbc))
	h.sync(h.memberWrite(b, 0, core.BlocksPerChunk, MakeVersion(2, 1), 0xcc))
	if e, _ := h.s.Index().Lookup(testPool, b, 0); e.LBA != first[0] {
		t.Fatalf("b took the slot at %d, not a's first at %v", e.LBA, first)
	}
	h.sync(h.memberWrite(b, 0, core.BlocksPerChunk, MakeVersion(2, 2), 0xdd))
	h.inv.Off(invReplay)
	restart(h)
	h.sync(h.persist(0, 1, 3, nil))
	h.settleChunk(a, MakeVersion(2, 2))
	c := chunk(4)
	h.sync(h.memberWrite(c, 0, core.BlocksPerChunk, MakeVersion(3, 1), 0xee))
	if e, _ := h.s.Index().Lookup(testPool, c, 0); e.LBA == first[0] {
		t.Fatalf("c took the slot at %d that b's held write names", e.LBA)
	}
	h.settleChunk(b, MakeVersion(2, 2))
	h.settle()
	h.holds()
	h.checkSlots()
}

// A slot write replay holds that the decision then drops gives its slot back to the log, whether the store drops it
// once replay ended or replay drops it at the decision's marker: no map ever refers to the slot, so the index never
// frees it. A chunk that held an older slot keeps that one.
func TestADroppedHeldSlotWriteFreesItsSlot(t *testing.T) {
	for _, older := range []bool{false, true} {
		h := newHarnessOn(t, testConfig(), slotGeometry)
		a := chunk(2)
		h.sync(h.persist(0, 1, 1, nil))
		keep := uint64(0)
		if older {
			keep = MakeVersion(1, 1)
			h.sync(h.memberWrite(a, 0, core.BlocksPerChunk, keep, 0xaa))
		}
		h.sync(h.persist(0, 1, 2, nil))
		h.sync(h.memberWrite(a, 0, core.BlocksPerChunk, MakeVersion(2, 1), 0xbb))
		h.inv.Off(invReplay)
		restart(h)
		h.sync(h.persist(0, 1, 3, []ChunkVersion{{Chunk: a, Version: keep}}))
		h.settleChunk(a, keep)
		h.settle()
		h.checkSlots()
		restart(h)
		h.checkSlots()
		if got := h.slotsInUse(); older != (len(got) == 1) {
			t.Fatalf("older %v: slots in use %v", older, got)
		}
	}
}

// A slot waiting to go back on a held write that a decision then drops is queued once and freed once: TakeFreed keeps
// out of its list only the slots it adds itself, so dropSlot must not queue a slot already waiting.
func TestASlotWaitingOnADroppedHeldWriteIsFreedOnce(t *testing.T) {
	h := newHarnessOn(t, testConfig(), slotGeometry)
	a, b := chunk(2), chunk(3)
	h.sync(h.persist(0, 1, 1, nil))
	h.sync(h.memberWrite(a, 0, core.BlocksPerChunk, MakeVersion(1, 1), 0xaa))
	first := h.slotsInUse()
	h.sync(h.persist(0, 1, 2, nil))
	h.sync(h.memberWrite(a, 0, core.BlocksPerChunk, MakeVersion(2, 1), 0xbb))
	h.sync(h.memberWrite(a, 0, 1, MakeVersion(2, 2), 0xbc))
	h.sync(h.memberWrite(b, 0, core.BlocksPerChunk, MakeVersion(2, 1), 0xcc))
	if e, _ := h.s.Index().Lookup(testPool, b, 0); e.LBA != first[0] {
		t.Fatalf("b took the slot at %d, not a's first at %v", e.LBA, first)
	}
	h.inv.Off(invReplay)
	restart(h)
	h.sync(h.persist(0, 1, 3, nil))
	h.settleChunk(a, MakeVersion(2, 2))
	if !slices.Contains(h.s.freed, first[0]) {
		t.Fatalf("slot %d is not waiting on b's held write: waiting %v", first[0], h.s.freed)
	}
	h.sync(h.persist(0, 1, 4, []ChunkVersion{{Chunk: b, Version: 0}}))
	h.settle()
	h.holds()
	h.checkSlots()
	restart(h)
	h.checkSlots()
}
