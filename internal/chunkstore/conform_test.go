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

// conformPlants are the plants of the rule map and of the oracle test that scripts/conform.sh runs in the primary-change run.
var conformPlants = []struct {
	name string
	set  func(r *primaryRun)
}{
	{"oldReport", func(r *primaryRun) { r.a.plant.oldReport = true }},
	{"activateEarly", func(r *primaryRun) { r.c.replicaPlants.activateEarly = true }},
	{"sameMembers", func(r *primaryRun) { r.c.replicaPlants.sameMembers = true }},
	{"sendEpoch", func(r *primaryRun) { r.c.replicaPlants.sendEpoch = true }},
	{"ownInterval", func(r *primaryRun) { r.c.replicaPlants.ownInterval = true }},
	{"fenceOnArrival", func(r *primaryRun) { r.c.replicaPlants.fenceOnArrival = true }},
	{"freshGrantNoCheck", func(r *primaryRun) { r.c.replicaPlants.freshGrantNoCheck = true }},
	{"keepID", func(r *primaryRun) { r.a.keepID = true }},
	{"resendNewestFirst", func(r *primaryRun) { r.c.replicaPlants.resendNewestFirst = true }},
	{"mergeWhileLacking", func(r *primaryRun) { r.c.replicaPlants.mergeWhileLacking = true }},
	{"relistNever", func(r *primaryRun) { r.c.replicaPlants.relistNever, r.c.storePlants.clearListed = true, true }},
	{"lastActivatedOnly", func(r *primaryRun) { r.c.replicaPlants.lastActivatedOnly = true }},
	{"readBelowMinLive", func(r *primaryRun) { r.c.replicaPlants.readBelowMinLive = true }},
	{"coverAtRead", func(r *primaryRun) { r.c.replicaPlants.coverAtRead = true }},
	{"removeOnActivation", func(r *primaryRun) { r.a.plant.removeOnActivation = true }},
	{"reorderAfterOther", func(r *primaryRun) { r.c.replicaPlants.reorderAfterOther = true }},
	{"reportDecided", func(r *primaryRun) { r.c.replicaPlants.reportDecided = true }},
	{"neverAsk", func(r *primaryRun) { r.c.replicaPlants.neverAsk = true }},
	{"peerInOldInterval", func(r *primaryRun) { r.c.replicaPlants.peerInOldInterval = true }},
	{"stopPreviousOnly", func(r *primaryRun) { r.c.replicaPlants.stopPreviousOnly = true }},
	{"settleByNumber", func(r *primaryRun) { r.c.storePlants.settleByNumber = true }},
	{"settleSubset", func(r *primaryRun) { r.c.storePlants.settleSubset = true }},
	{"recoverSelfOnly", func(r *primaryRun) { r.c.replicaPlants.recoverSelfOnly = true }},
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
