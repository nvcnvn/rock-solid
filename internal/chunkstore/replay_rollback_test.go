package chunkstore

import (
	"bytes"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
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
	r.lose(r.m)
	r.tick(DegradedRefillAfter + 2_000_000)
	r.declare(t, r.m)
	r.tick(5_000_000)
	plantAll(r.c, func(p *replicaPlants) { p.noRecovery = false })
	r.tick(60_000_000)
	rp, data := r.read(r.ch)
	if rp.Status != StatusOK || len(data) == 0 || data[0] != 0x11 {
		t.Fatalf("X read %+v %x; primary %+v; members %v", rp, data[:min(1, len(data))], r.c.listed(r.c.node(r.p), r.ch), r.members())
	}
}

func TestARestartedPrimaryRollsBackItsPendingWrite(t *testing.T) {
	t.Skip("fails until Replay rollback has replay gating")
	for seed := uint64(1); seed <= 3; seed++ {
		r := newRollbackRun(t, seed)
		r.restart()
		r.heal()
		r.tick(10_000_000)
		r.loseMemberAndRead(t)
	}
}

func TestAPrimaryRestartedAfterItsDropServesFromItsFlag(t *testing.T) {
	t.Skip("fails until Replay rollback has the image flag after a restart")
	for seed := uint64(1); seed <= 3; seed++ {
		r := newRollbackRun(t, seed)
		r.dropY(t)
		r.restart()
		r.tick(10_000_000)
		r.loseMemberAndRead(t)
	}
}

// A restarted primary decides again with the other member up, then that member's disk goes before recovery.
func TestARestartedPrimaryKeepsItsDropsAcrossItsNextDecision(t *testing.T) {
	t.Skip("fails until Replay rollback has replay settling at each decision marker")
	r := newRollbackRun(t, 1)
	r.dropY(t)
	r.restart()
	r.tick(10_000_000)
	_, e := r.c.table.entry(pg0)
	r.a.asks = []PGInterval{{pg0, e.Interval}}
	r.publish()
	r.tick(5_000_000)
	r.loseMemberAndRead(t)
}

func TestABouncedPrimaryWithItsWritePendingRollsItBack(t *testing.T) {
	t.Skip("fails until Replay rollback has demotion without Ungate")
	r := newRollbackRun(t, 1)
	r.lose(r.p)
	r.tick(3_000_000)
	r.back(r.p)
	r.heal()
	r.tick(5_000_000)
	r.loseMemberAndRead(t)
}

func TestABouncedPrimaryKeepsTheImageOfItsDroppedWrite(t *testing.T) {
	t.Skip("fails until Replay rollback has the image outside the lead")
	r := newRollbackRun(t, 1)
	r.dropY(t)
	r.lose(r.p)
	r.tick(3_000_000)
	r.back(r.p)
	r.tick(5_000_000)
	r.loseMemberAndRead(t)
}

// The primary persists the decision that drops Y, and the other member's acknowledgement of its own list never comes, so
// the lead does not activate before the next peering.
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
	r.loseMemberAndRead(t)
}
