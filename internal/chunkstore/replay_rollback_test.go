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

// loseMemberAndRead has the other member's disk die and be declared lost, lets recovery run and reads X back.
func (r *rollbackRun) loseMemberAndRead(t *testing.T) {
	t.Helper()
	if rp, data := r.xAfterLoss(t); !readsX(rp, data) {
		t.Fatalf("X read %+v %x; primary %+v; members %v", rp, data[:min(1, len(data))], r.c.listed(r.c.node(r.p), r.ch), r.members())
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
	return r.read(r.ch)
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
