package raftshard

import (
	"fmt"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

const (
	invNeverLost = "a committed entry is never lost"
	// diskCrashes is how many crashes a disk run makes, simOwner the owner its shard's blobs carry.
	diskCrashes = 3
	simOwner    = 1
)

// simGeometry is a disk of 128 segments of 1 MiB.
var simGeometry = disklog.Geometry{SegmentBlocks: 256, SegmentCount: 128}

// diskRun runs each replica's rounds through a host on its node's disk log. Each crash comes while
// the node's log has a round in flight and lands the disk's unflushed writes at random, so a round
// can be durable with its shard never told, and the restarted replica rebuilds its storage by
// replaying the log from the base it booted with, as a manifest would give it. The run crashes a
// node diskCrashes times, one at a time, each while every replica runs, and is done once a value
// proposed after the last crash is applied.
type diskRun struct {
	formatted [simNodes]bool
	crashes   int
	// unreported counts the restarts whose replay found a round its shard was never told was durable.
	unreported int
	// skipReplay plants a restart that replays nothing.
	skipReplay bool
	lost       error
}

// bootDisk formats the replica's disk on its first boot and recovers it on a restart, reporting
// whether the shard waits for the recovery.
func (r *simReplica) bootDisk(ctx *core.Context) bool {
	c := r.c
	st := bootStorage(simNodes)
	r.cfg.Storage = st
	if !c.disk.formatted[r.id] {
		c.disk.formatted[r.id] = true
		log, err := disklog.Format(ctx, rigConfig(), disklog.Superblock{LogID: uint64(r.id) + 1, Geometry: simGeometry}, 1, &r.lout)
		if err != nil {
			panic(err)
		}
		r.host = NewHost(log)
		r.host.Add(simOwner, st)
		c.storage[r.id] = st
		return false
	}
	r.host = NewHost(nil)
	r.host.Add(simOwner, st)
	rec, err := disklog.Recover(ctx, rigConfig(), simGeometry.DiskBlocks(), disklog.ModeOpen, 1, &r.lout)
	if err != nil {
		panic(err)
	}
	r.rec = rec
	return true
}

// diskEvent hands a disk completion to the log or its recovery, and drops any other event while
// the replica recovers, reporting whether it took the event.
func (r *simReplica) diskEvent(ctx *core.Context, ev *core.Event) bool {
	switch {
	case ev.Kind == core.EvDiskDone:
		if ev.Err != core.DiskOK {
			panic(ev.Err)
		}
		if r.rec != nil {
			r.rec.OnEvent(ctx, ev, &r.lout)
		} else {
			r.host.Log.OnEvent(ctx, ev, &r.lout)
		}
		return true
	case r.s == nil:
		if ev.Kind == core.EvNetRecv && ev.Msg.Buf != core.NoBuf {
			ctx.Pool.Free(ev.Msg.Buf)
		}
		return true
	}
	return false
}

// flushDisk takes the log's events, reporting done rounds to the shard, writes the shard's new
// round, and appends the log's actions.
func (r *simReplica) flushDisk(ctx *core.Context, acts []core.Action) []core.Action {
	for i := 0; i < len(r.lout.Events); i++ {
		ev := r.lout.Events[i]
		var err error
		switch ev.Kind {
		case disklog.Durable:
			r.dones, err = r.host.Durable(ctx, &ev, &r.lout, r.dones[:0])
			for _, d := range r.dones {
				r.s.Durable(ctx, d.Round, &r.out)
			}
		case disklog.ManifestLoaded:
			err = r.rec.Replay(ctx, &r.lout)
		case disklog.Replayed:
			if !r.c.disk.skipReplay {
				err = r.host.Replayed(&ev)
			}
		case disklog.Recovered:
			var log *disklog.Log
			if log, err = r.rec.Log(ctx); err == nil {
				r.host.Recovered(log)
				r.rec = nil
				r.c.recovered(r)
				r.start(ctx)
			}
		case disklog.Failed, disklog.InDoubt, disklog.RecoveryFailed:
			err = ev.Err
		}
		if err != nil {
			panic(fmt.Sprintf("replica %d: %v", r.id, err))
		}
	}
	if r.out.Flush != 0 {
		n := len(r.lout.Events)
		if err := r.host.Write(ctx, simOwner, r.out.Flush, &r.out.Round, &r.lout); err != nil {
			panic(err)
		}
		if len(r.lout.Events) != n {
			panic("raftshard: the log reported events while it took a round")
		}
	}
	acts = append(acts, r.lout.Actions...)
	r.lout.Reset()
	return acts
}

// recovered swaps in the storage a restart's replay rebuilt, which must hold every committed entry
// the storage before the crash held.
func (c *simCluster) recovered(r *simReplica) {
	was, now := c.storage[r.id], r.cfg.Storage
	if m := min(was.last(), uint64(len(c.log))-1); len(c.log) > 0 && m > was.base.Index && c.disk.lost == nil {
		want, _ := was.Term(m)
		if got, err := now.Term(m); want == c.log[m].Term && (err != nil || got != want) {
			c.disk.lost = fmt.Errorf("replica %d held committed entry %d of term %d before its crash and replayed term %d (%v)", r.id, m, want, got, err)
		}
	}
	if now.last() > was.last() || now.hs.Term != was.hs.Term || now.hs.Vote != was.hs.Vote {
		c.disk.unreported++
	}
	c.storage[r.id] = now
}

// committedHeld checks that a majority of the replicas' storages hold the newest entry any replica
// applied, a crashed replica's as it was until its replay ends.
func (c *simCluster) committedHeld() error {
	if c.disk.lost != nil || len(c.log) == 0 {
		return c.disk.lost
	}
	e := c.log[len(c.log)-1]
	held := 0
	for _, st := range c.storage {
		if t, err := st.Term(e.Index); err == nil && t == e.Term {
			held++
		}
	}
	if held <= len(c.storage)/2 {
		return fmt.Errorf("entry %d of term %d was applied, and %d of %d replicas hold it", e.Index, e.Term, held, len(c.storage))
	}
	return nil
}

// crashDisk crashes a random node with a round in flight on its log, one step in 300 while every
// replica runs, diskCrashes times.
func (c *simCluster) crashDisk() {
	if c.disk.crashes == diskCrashes {
		return
	}
	for id, r := range c.reps {
		if !c.s.Node(uint32(id)).Up() || r.s == nil {
			return
		}
	}
	if !c.rng.Chance(1, 300) {
		return
	}
	id := uint32(c.rng.Below(uint64(len(c.reps))))
	if c.reps[id].host.Log.InFlight() == 0 {
		return
	}
	c.s.CrashRandom(id)
	c.disk.crashes++
	c.crashed, c.afterCrash, c.sinceCrash = true, false, c.next
}

func newDiskCluster(seed uint64, d *diskRun) *simCluster {
	return newCluster(seed, plants{}, faultSettings(), nil, d)
}

// Each replica's rounds go to its disk log, and three crashes, each landing unflushed writes at
// random, restart a replica from what its log replays; every entry a replica applied stays held
// by a majority, and one a replica held before its crash it holds after.
func TestCommittedEntriesSurviveCrashesOnTheDiskLog(t *testing.T) {
	unreported := 0
	seeds.Run(t, 0, 20, func(seed uint64) {
		c := newDiskCluster(seed, &diskRun{}).run(t, seed)
		if !c.done {
			t.Fatalf("seed %d: %d crashes, no value proposed after the last applied", seed, c.disk.crashes)
		}
		unreported += c.disk.unreported
		if err := sim.CompareTraces(c.s.Trace(), newDiskCluster(seed, &diskRun{}).run(t, seed).s.Trace()); err != nil {
			t.Fatalf("seed %d: trace replays differently: %v", seed, err)
		}
	})
	if seeds.Default() && unreported == 0 {
		t.Error("no restart in 20 seeds replayed a round its shard was never told was durable")
	}
}

func TestARestartThatReplaysNothingLosesCommittedEntries(t *testing.T) {
	for seed := range uint64(20) {
		c := newDiskCluster(seed, &diskRun{skipReplay: true})
		c.inv.Only(invNeverLost)
		for c.s.Time() < simRunFor && c.s.Step() {
			if c.inv.Check() != nil {
				return
			}
			c.after(t, seed)
		}
	}
	t.Fatal("20 seeds restarting replicas that replay nothing broke no invariant")
}
