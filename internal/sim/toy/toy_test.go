package toy_test

import (
	"bytes"
	"errors"
	"flag"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/toy"
	"github.com/nvcnvn/rock-solid/internal/trace"
)

var seeds = flag.Int("toy.seeds", 100, "seeds the fixed protocol runs")

const (
	ops   = 40
	steps = 3000
	// commitSeeds are the seeds every commit runs, within which each planted bug must be found.
	commitSeeds = 100
)

func chaos() sim.Settings {
	set := sim.DefaultSettings()
	set.DropNum, set.DropDen = 1, 8
	set.CrashNum, set.CrashDen = 1, 300
	set.StaleNum, set.StaleDen = 1, 4
	return set
}

func run(seed uint64, set sim.Settings, bug toy.Bug) (*toy.Cluster, error) {
	c := toy.NewCluster(seed, set, ops, bug)
	return c, c.Run(steps)
}

func TestTheFixedProtocolKeepsEveryAcknowledgedWrite(t *testing.T) {
	acked, crashes := 0, uint64(0)
	for seed := range uint64(*seeds) {
		c, err := run(seed, chaos(), toy.NoBug)
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		acked += c.Acked()
		b := c.Boots()
		crashes += b[0] + b[1] + b[2] - toy.Nodes
	}
	if acked < *seeds*ops*3/4 || crashes < uint64(*seeds) {
		t.Fatalf("%d writes acknowledged and %d crashes over %d seeds", acked, crashes, *seeds)
	}
}

func TestEachPlantedBugIsFoundAndItsSeedReplays(t *testing.T) {
	for _, bug := range []toy.Bug{toy.AckBeforeReplica, toy.IgnoreEpoch} {
		seed, found := uint64(0), false
		var first error
		for ; seed < commitSeeds; seed++ {
			if _, first = run(seed, chaos(), bug); first != nil {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("bug %d: no seed below %d trips the invariant", bug, commitSeeds)
		}
		var lost *toy.LostWriteError
		if !errors.As(first, &lost) {
			t.Fatalf("bug %d seed %d: %v", bug, seed, first)
		}
		a, errA := run(seed, chaos(), bug)
		b, errB := run(seed, chaos(), bug)
		if err := sim.CompareTraces(a.Scheduler().Trace(), b.Scheduler().Trace()); err != nil {
			t.Fatalf("bug %d seed %d replays differently: %v", bug, seed, err)
		}
		if errA == nil || errB == nil || errA.Error() != first.Error() || errB.Error() != first.Error() {
			t.Fatalf("bug %d seed %d failed with %v, then %v and %v", bug, seed, first, errA, errB)
		}
		if _, err := run(seed, chaos(), toy.NoBug); err != nil {
			t.Fatalf("seed %d fails the fixed protocol too: %v", seed, err)
		}
		t.Logf("bug %d: seed %d, %v", bug, seed, first)
	}
}

func TestAStaleReplyNeedsAPrimaryCrash(t *testing.T) {
	set := chaos()
	set.CrashNum, set.CrashDen = 0, 0
	for seed := range uint64(commitSeeds) {
		if _, err := run(seed, set, toy.IgnoreEpoch); err != nil {
			t.Fatalf("seed %d with no crashes: %v", seed, err)
		}
	}
}

func TestTheHistoryHoldsEveryWriteAndOnlyAcknowledgedOnesComplete(t *testing.T) {
	incomplete := 0
	for seed := range uint64(20) {
		c, err := run(seed, chaos(), toy.NoBug)
		if err != nil {
			t.Fatal(err)
		}
		done := 0
		for i, op := range c.History().Ops() {
			if op.Input[0] != uint64(i+1) || op.Client != 0 {
				t.Fatalf("seed %d op %d: %+v", seed, i, op)
			}
			if !op.Done {
				incomplete++
				continue
			}
			done++
			if op.Complete < op.Invoke {
				t.Fatalf("seed %d op %d completes before it starts: %+v", seed, i, op)
			}
		}
		if done != c.Acked() {
			t.Fatalf("seed %d: %d complete, %d acknowledged", seed, done, c.Acked())
		}
	}
	if incomplete == 0 {
		t.Fatal("no write was left incomplete by a primary crash over 20 seeds")
	}
}

func TestTheMachineAllocatesNothingPerEvent(t *testing.T) {
	pool := sim.NewPool(64*core.BlockSize, 0)
	acks := 0
	primary := toy.New(core.NewRand(1), toy.Config{Self: core.NodeID{}, Peers: []core.NodeID{{Node: 1}, {Node: 2}}, Epoch: 1, Ack: func(uint64) { acks++ }})
	replica := toy.New(core.NewRand(2), toy.Config{Self: core.NodeID{Node: 1}})
	ctx := core.Context{Now: 1000, Pool: pool}
	out := make([]core.Action, 0, 16)
	value := uint64(0)
	cycle := func() {
		value++
		out = primary.Propose(&ctx, value, out[:0])
		write, fsync, send := out[0], out[1], out[2]
		ctx.Now += 1000
		out = primary.OnEvent(&ctx, &core.Event{Kind: core.EvTimer, Timer: toy.RetryTimer}, out[:0])
		out = primary.OnEvent(&ctx, &core.Event{Kind: core.EvDiskDone, IO: write.IO}, out[:0])
		out = primary.OnEvent(&ctx, &core.Event{Kind: core.EvDiskDone, IO: fsync.IO}, out[:0])
		out = replica.OnEvent(&ctx, &core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{}, Msg: send.Msg}, out[:0])
		rw, rf := out[0].IO, out[1].IO
		out = replica.OnEvent(&ctx, &core.Event{Kind: core.EvDiskDone, IO: rw}, out[:0])
		out = replica.OnEvent(&ctx, &core.Event{Kind: core.EvDiskDone, IO: rf}, out[:0])
		reply := out[0]
		if reply.Kind != core.ActNetSend || trace.Msg(reply.Msg.Body[0]) != trace.MsgWriteReply {
			t.Fatalf("replica emitted %+v", out)
		}
		out = primary.OnEvent(&ctx, &core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{Node: 1}, Msg: reply.Msg}, out[:0])
	}
	cycle()
	if got := testing.AllocsPerRun(100, cycle); got != 0 {
		t.Fatalf("%v allocations per write's events", got)
	}
	if acks != 102 || primary.Pending() != 0 || pool.Live() != 0 {
		t.Fatalf("%d acknowledged, %d pending, %d buffers live", acks, primary.Pending(), pool.Live())
	}
}

func TestMessagesRoundTripAndMalformedOnesAreIgnored(t *testing.T) {
	m := toy.Msg(trace.MsgWrite, 3, 5, 7)
	if m.Len != 25 || !bytes.Equal(m.Inline()[:9], []byte{byte(trace.MsgWrite), 3, 0, 0, 0, 0, 0, 0, 0}) {
		t.Fatalf("write %v", m.Inline())
	}
	if r := toy.Msg(trace.MsgWriteReply, 3, 5, 7); r.Len != 17 {
		t.Fatalf("reply of %d bytes", r.Len)
	}
	replica := toy.New(core.NewRand(1), toy.Config{Self: core.NodeID{Node: 1}})
	ctx := core.Context{Now: 1000, Pool: sim.NewPool(core.BlockSize, 0)}
	short := m
	short.Len = 24
	for _, bad := range []core.Msg{short, toy.Msg(trace.MsgWriteReply, 3, 5, 0), {}} {
		if out := replica.OnEvent(&ctx, &core.Event{Kind: core.EvNetRecv, Msg: bad}, nil); len(out) != 0 {
			t.Fatalf("%v gave %+v", bad.Inline(), out)
		}
	}
}

func FuzzMachineRecv(f *testing.F) {
	for _, m := range []core.Msg{toy.Msg(trace.MsgWrite, 1, 2, 3), toy.Msg(trace.MsgWriteReply, 1, 2, 0)} {
		f.Add(m.Inline())
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		var msg core.Msg
		if msg.SetInline(body) != nil {
			return
		}
		ctx := core.Context{Now: 1000, Pool: sim.NewPool(4*core.BlockSize, 0)}
		primary := toy.New(core.NewRand(1), toy.Config{Peers: []core.NodeID{{Node: 1}}, Epoch: 1})
		primary.Propose(&ctx, 1, nil)
		replica := toy.New(core.NewRand(2), toy.Config{Self: core.NodeID{Node: 1}})
		ev := core.Event{Kind: core.EvNetRecv, Msg: msg}
		write := len(body) == 25 && trace.Msg(body[0]) == trace.MsgWrite
		if out := replica.OnEvent(&ctx, &ev, nil); len(out) != 0 && !write {
			t.Fatalf("replica answered %v with %+v", body, out)
		}
		if out := primary.OnEvent(&ctx, &ev, nil); len(out) != 0 && !write {
			t.Fatalf("primary answered %v with %+v", body, out)
		}
	})
}
