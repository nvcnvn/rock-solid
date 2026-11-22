package chunkstore

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
	"github.com/nvcnvn/rock-solid/internal/trace"
)

// conformBegin starts a conformance trace: the table now is the spec's Init, so interval 1's bootstrap peering before this record is no part of it.
func (c *rcluster) conformBegin() {
	c.conforming = true
	c.noteTable(&c.table)
}

func (c *rcluster) noteTable(next *Table) {
	if !c.conforming {
		return
	}
	var prev Table
	if c.noted {
		prev = c.table
	}
	c.noted = true
	for _, r := range TableRecords(nil, &prev, next) {
		c.s.Node(c.client.Node).Output(c.client, r)
	}
}

// noteSends records the client writes in acts.
func (c *rcluster) noteSends(acts []core.Action) {
	if !c.conforming {
		return
	}
	for i := range acts {
		a := &acts[i]
		if a.Kind != core.ActNetSend || a.Msg.Len < headSize || Kind(a.Msg.Body[1]) != KindWrite {
			continue
		}
		data := 0
		if a.Msg.Buf != core.NoBuf {
			data = len(c.s.Node(c.client.Node).Pool().Bytes(a.Msg.Buf))
		}
		var u Update
		if DecodeUpdate(a.Msg.Inline(), data, &u) != nil {
			continue
		}
		c.s.Node(c.client.Node).Output(c.client, trace.Record{Name: trace.OutSend, Fields: [trace.MaxFields]uint64{
			uint64(u.ID), u.Chunk.Hash(), pgField(u.PG.PG), uint64(u.Off)<<32 | uint64(u.Len), uint64(u.Incarnation)}})
	}
}

// conformActor records what a client actor sends.
type conformActor struct {
	sim.Actor
	c *rcluster
}

func (a conformActor) Next(ctx *core.Context, acts []core.Action) []core.Action {
	n := len(acts)
	acts = a.Actor.Next(ctx, acts)
	a.c.noteSends(acts[n:])
	return acts
}

// conformWrite keeps a run's trace for scripts/conform.sh.
func conformWrite(t *testing.T, name string, c *rcluster) {
	t.Helper()
	dir := os.Getenv("CONFORM_DIR")
	if dir == "" {
		return
	}
	if err := os.WriteFile(filepath.Join(dir, name+".trace"), c.s.Trace(), 0o644); err != nil {
		t.Fatal(err)
	}
}

// A clean run, one PG on three disks: writes, then a primary change, then writes under the new primary.
func TestConformScriptedPrimaryChange(t *testing.T) {
	c := newRCluster(t, rtable(1, 2))
	c.conformBegin()
	ch := chunkIn(pg0, 1, 0)
	block := make([]byte, core.BlockSize)
	for i := range 2 {
		block[0] = byte(i + 1)
		c.until(c.write(pg0, ch, 0, block))
	}
	conformWrite(t, "scripted-clean", c)
	next := c.table
	next.Epoch++
	next.Pools = []PoolTable{next.Pools[0]}
	next.Pools[0].PGs = []PGEntry{next.Pools[0].PGs[0]}
	e := &next.Pools[0].PGs[0]
	e.Past = append(e.Past, PastInterval{Interval: e.Interval, MinLive: 2, Members: e.Members})
	e.Interval++
	e.Members = []Member{e.Members[1], e.Members[0], e.Members[2]}
	c.publish(next, true)
	for i := range 2 {
		block[0] = byte(i + 3)
		c.until(c.write(pg0, ch, 0, block))
	}
	c.settle()
	conformWrite(t, "scripted-primary-change", c)
}

// loneCopyTrace is the trace of a primary that keeps leading with W on its log alone: disk 2 down, W on its way to disk 3 when disk 3 goes too and disk 2 comes back. It reports W's answer.
func loneCopyTrace(t *testing.T, plant bool) (*rcluster, Response) {
	c := newRCluster(t, rtable(1, 2))
	c.replicaPlants.ackOnLoneCopy = plant
	for _, r := range c.reps {
		r.plant = c.replicaPlants
	}
	c.conformBegin()
	ch := chunkIn(pg0, 1, 0)
	block := make([]byte, core.BlockSize)
	c.until(c.write(pg0, ch, 0, block))
	c.settle()
	c.publish(c.next(members(Member{Disk: 1}, Member{Disk: 2, Mark: MarkDown}, Member{Disk: 3})), true)
	c.settle()
	c.s.Partition(0, 2, false)
	block[0] = 1
	w := c.write(pg0, ch, 0, block)
	c.runFor(5_000)
	c.publish(c.next(members(Member{Disk: 1}, Member{Disk: 2, Mark: MarkDown}, Member{Disk: 3, Mark: MarkDown})), false)
	c.publish(c.next(members(Member{Disk: 1}, Member{Disk: 2}, Member{Disk: 3, Mark: MarkDown})), false)
	c.until(w)
	rp := c.reply(w)
	c.s.Heal(0, 2)
	c.settle()
	return c, rp
}

// A kept version with too few copies is refused at activation, its client resends, and the spec takes the whole run.
func TestConformScriptedLoneCopy(t *testing.T) {
	c, rp := loneCopyTrace(t, false)
	if rp.Status != StatusRefused {
		t.Fatalf("W answered %+v", rp)
	}
	block := make([]byte, core.BlockSize)
	block[0] = 1
	c.until(c.write(pg0, chunkIn(pg0, 1, 0), 0, block))
	c.settle()
	conformWrite(t, "scripted-lone-copy", c)
}

// A member merely down is refilled and leaves, the next waits, and both return: the spec takes the whole run.
func TestConformScriptedRefill(t *testing.T) {
	if err := lastSourceRun(t, nil, true, "scripted-refill"); err != nil {
		t.Fatal(err)
	}
}

// A move and a restart: the restarted primary's replay gates Y until its next decision.
func TestConformScriptedMoveRestarted(t *testing.T) {
	r := seededSoleRun(t, 1, 2, 3)
	r.c.conformBegin()
	yPastTheLeaving(t, r, core.BlockSize, noPlant)
	restartPastTheMove(t, r)
	conformWrite(t, "scripted-move-restarted", r.c)
}

// A move and a new primary that holds Y: peering keeps Y, which every up member holds, over the removed disk that lacks it.
func TestConformScriptedMovePrimary(t *testing.T) {
	r := seededSoleRun(t, 1, 2, 3)
	r.c.conformBegin()
	movedPrimary(t, r, core.BlockSize, noPlant)
	conformWrite(t, "scripted-move-primary", r.c)
}

// A restart during recovery: the restarted primary's replay gates Y until its next decision.
func TestConformScriptedListerRestarted(t *testing.T) {
	r := seededSoleRun(t, 1, 3, 3)
	r.c.conformBegin()
	recoveringLister(t, r, noPlant)
	conformWrite(t, "scripted-lister-restarted", r.c)
}

// A rotation during recovery: the new primary holds Y logged, and peering keeps it over the member that lists the chunk.
func TestConformScriptedListerRotated(t *testing.T) {
	r := seededSoleRun(t, 1, 3, 3)
	r.c.conformBegin()
	rotatedLister(t, r, noPlant)
	conformWrite(t, "scripted-lister-rotated", r.c)
}

// A leaving member up takes the place of a member merely down, which leaves in its turn: the spec takes the whole run.
func TestConformScriptedStay(t *testing.T) {
	if err := staySourceRun(t, nil, "scripted-stay"); err != nil {
		t.Fatal(err)
	}
}

// The leaving disk returns first, on three disks so that no refill took the other member's place: one table marks it up and has it take that place, which is one interval where the spec's mark and Stay make two.
func TestConformScriptedLeavingFirst(t *testing.T) {
	r, ch, v := refillRun(t, 2, 2, 3, nil, "scripted-leaving-first")
	ms := r.members()
	d1, d2 := ms[0].Disk, ms[1].Disk
	r.recovery(false)
	r.lose(d2)
	r.tick(DegradedRefillAfter + 2_000_000)
	r.lose(d1)
	r.tick(RefillAfter + 2_000_000)
	r.back(d2)
	r.publish()
	if ms = r.members(); ms[len(ms)-1].Disk != d1 || !ms[len(ms)-1].Leaving || slices.ContainsFunc(ms, func(m Member) bool { return m.Disk == d2 && m.Leaving }) {
		t.Fatalf("after disk %d returned: members %v", d2, ms)
	}
	r.recovery(true)
	r.tick(10_000_000)
	r.repaired(t, 2, ch, v)
	conformWrite(t, "scripted-leaving-first", r.c)
}

// A leaving disk declared lost leaves the pool in the table that refills the member gone down after it: the leaving member goes on no report, the other takes the leaving mark and a new disk joins, in one interval.
func TestConformScriptedDrop(t *testing.T) {
	r, ch, v := refillRun(t, 2, 2, 5, nil, "scripted-drop")
	ms := r.members()
	d1, d2 := ms[0].Disk, ms[1].Disk
	r.recovery(false)
	r.lose(d2)
	r.tick(DegradedRefillAfter + 2_000_000)
	r.lose(d1)
	r.c.runFor(DegradedRefillAfter + 2_000_000)
	r.declare(t, d2)
	r.publish()
	if ms = r.members(); slices.ContainsFunc(ms, func(m Member) bool { return m.Disk == d2 }) || !slices.ContainsFunc(ms, func(m Member) bool { return m.Disk == d1 && m.Leaving }) {
		t.Fatalf("after disk %d was declared lost: members %v", d2, ms)
	}
	r.back(d1)
	r.recovery(true)
	r.tick(10_000_000)
	r.repaired(t, 2, ch, v)
	conformWrite(t, "scripted-drop", r.c)
}

// Two members refilled and leaving, one declared lost: Drop takes it alone, and the other heals the PG on its return.
func TestConformScriptedLost(t *testing.T) {
	if err := lostRun(t, nil, "scripted-lost"); err != nil {
		t.Fatal(err)
	}
}

// Several leaving members on a PG of three copies: repair ends on the recovered report with neither dead disk back.
func TestConformScriptedSecondFailure(t *testing.T) {
	for how, name := range map[string]string{"during the copy": "scripted-second-failure", "together": "scripted-together"} {
		r, ch, v := refillRun(t, 3, 2, 6, nil, name)
		secondFailures[how](r)
		r.tick(RefillAfter + 30_000_000)
		r.repaired(t, 3, ch, v)
		conformWrite(t, name, r.c)
	}
}

// Sweep seeds the check once stopped at: 387423's primary records its own copy of a recovery write after it decided the
// next interval, whose decision lists the chunk again (discrepancy 36).
func TestConformSweepSeeds(t *testing.T) {
	for _, seed := range []uint64{387423} {
		if err := runPrimaries(t, seed, nil); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
	}
}

// conformPlants are the plants of the rule map and of the oracle test that scripts/conform.sh runs in the primary-change run.
var conformPlants = []struct {
	name string
	set  func(r *primaryRun)
}{
	{"ackOnLoneCopy", func(r *primaryRun) { r.c.replicaPlants.ackOnLoneCopy = true }},
	{"noDegradedHold", func(r *primaryRun) { r.c.replicaPlants.noDegradedHold = true }},
	{"listerOrders", func(r *primaryRun) { r.c.replicaPlants.listerOrders = true }},
	{"oldReport", func(r *primaryRun) { r.a.plant.oldReport = true }},
	{"activateEarly", func(r *primaryRun) { r.c.replicaPlants.activateEarly = true }},
	{"sameMembers", func(r *primaryRun) { r.c.replicaPlants.sameMembers = true }},
	{"sendEpoch", func(r *primaryRun) { r.c.replicaPlants.sendEpoch = true }},
	{"ownInterval", func(r *primaryRun) { r.c.replicaPlants.ownInterval = true }},
	{"fenceOnArrival", func(r *primaryRun) { r.c.replicaPlants.fenceOnArrival = true }},
	{"freshGrantNoCheck", func(r *primaryRun) { r.c.replicaPlants.freshGrantNoCheck = true }},
	{"keepID", func(r *primaryRun) { r.a.keepID = true }},
	{"resendNewestFirst", func(r *primaryRun) { r.c.replicaPlants.resendNewestFirst = true }},
	{"mergeWhileLacking", func(r *primaryRun) { r.c.replicaPlants.mergeWhileLacking, r.c.replicaPlants.listerOrders = true, true }},
	{"mergeOnDropped", func(r *primaryRun) { r.c.replicaPlants.mergeOnDropped = true }},
	{"editKeepsInterval", func(r *primaryRun) { r.a.plant.editKeepsInterval = true }},
	{"relistNever", func(r *primaryRun) { r.c.replicaPlants.relistNever, r.c.storePlants.clearListed = true, true }},
	{"unlistTorn", func(r *primaryRun) { r.c.replicaPlants.unlistTorn = true }},
	{"lastActivatedOnly", func(r *primaryRun) { r.c.replicaPlants.lastActivatedOnly = true }},
	{"readBelowMinLive", func(r *primaryRun) { r.c.replicaPlants.readBelowMinLive = true }},
	{"coverAtRead", func(r *primaryRun) { r.c.replicaPlants.coverAtRead = true }},
	{"removeOnActivation", func(r *primaryRun) { r.a.plant.removeOnActivation = true }},
	{"reorderAfterOther", func(r *primaryRun) { r.c.replicaPlants.reorderAfterOther = true }},
	{"reportDecided", func(r *primaryRun) { r.c.replicaPlants.reportDecided = true }},
	{"neverAsk", func(r *primaryRun) { r.c.replicaPlants.neverAsk = true }},
	{"memberNoAsk", func(r *primaryRun) { r.c.replicaPlants.memberNoAsk = true }},
	{"peerInOldInterval", func(r *primaryRun) { r.c.replicaPlants.peerInOldInterval = true }},
	{"stopPreviousOnly", func(r *primaryRun) { r.c.replicaPlants.stopPreviousOnly = true }},
	{"settleByNumber", func(r *primaryRun) { r.c.storePlants.settleByNumber = true }},
	{"settleSubset", func(r *primaryRun) { r.c.storePlants.settleSubset = true }},
	{"recoverSelfOnly", func(r *primaryRun) { r.c.replicaPlants.recoverSelfOnly = true }},
	{"recoverFromListed", func(r *primaryRun) { r.c.replicaPlants.recoverFromListed = true }},
	{"clearNoSource", func(r *primaryRun) { r.c.replicaPlants.clearNoSource = true }},
	{"goneWhileUp", func(r *primaryRun) { r.c.replicaPlants.goneWhileUp = true }},
	{"readOldDisk", func(r *primaryRun) { r.c.replicaPlants.readOldDisk = true }},
	{"listerVetoes", func(r *primaryRun) { r.c.replicaPlants.listerVetoes = true }},
	{"keepPastMember", func(r *primaryRun) { r.c.replicaPlants.keepPastMember = true }},
	{"writeOnUnfound", func(r *primaryRun) { r.c.replicaPlants.writeOnUnfound = true }},
	{"forgetImage", func(r *primaryRun) { r.c.replicaPlants.forgetImage = true }},
	{"mapOnReplay", func(r *primaryRun) { r.c.storePlants.mapOnReplay = true }},
	{"mapOnDemote", func(r *primaryRun) { r.c.replicaPlants.mapOnDemote = true }},
	{"forgetImageDown", func(r *primaryRun) { r.c.replicaPlants.forgetImageDown = true }},
	{"ignoreImageFlag", func(r *primaryRun) { r.c.replicaPlants.ignoreImageFlag = true }},
	{"replayForgetsDrops", func(r *primaryRun) { r.c.storePlants.replayForgetsDrops = true }},
}

// conformPlant keeps a planted run's trace, however the run stops, and beside it a .stop file saying how; with CONFORM_RUNON set the run goes on past its first broken invariant, for a plant whose break comes before the record of what broke it.
func conformPlant(t *testing.T, name string, set func(r *primaryRun), seed uint64) {
	var run *primaryRun
	stop := "end"
	defer func() {
		if e := recover(); e != nil {
			stop = fmt.Sprint("panic: ", e)
		}
		conformWrite(t, fmt.Sprintf("%s-%d", name, seed), run.c)
		if err := os.WriteFile(filepath.Join(os.Getenv("CONFORM_DIR"), fmt.Sprintf("%s-%d.stop", name, seed)), []byte(stop), 0o644); err != nil {
			t.Fatal(err)
		}
	}()
	if err := runPrimaries(t, seed, &peerPlant{set: func(r *primaryRun) {
		run, r.runOn = r, os.Getenv("CONFORM_RUNON") != ""
		set(r)
	}}); err != nil {
		stop = err.Error()
	}
}

// Keeps, for scripts/conform.sh plants, each seed's unplanted trace and its trace under each plant CONFORM_PLANTS names, or every plant.
func TestPlantsKeepTraces(t *testing.T) {
	if os.Getenv("CONFORM_DIR") == "" {
		t.Skip("CONFORM_DIR not set")
	}
	want := strings.Fields(os.Getenv("CONFORM_PLANTS"))
	seeds.Run(t, 0, 10, func(seed uint64) {
		if err := runPrimaries(t, seed, nil); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		for _, p := range conformPlants {
			if len(want) == 0 || slices.Contains(want, p.name) {
				conformPlant(t, p.name, p.set, seed)
			}
		}
	})
}
