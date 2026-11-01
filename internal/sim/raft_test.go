package sim_test

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"testing"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

const (
	raftNodes               = 3
	raftTick                = core.Tick(10_000)
	raftTimer  core.TimerID = 1
	raftOps                 = 40
	raftRunFor              = core.Tick(30_000_000)
)

var quietRaft = &raft.DefaultLogger{Logger: log.New(io.Discard, "", 0)}

// raftCluster is one Raft group of three replicas on the simulator's network, whose leaders
// propose a value per tick until a replica has applied raftOps, and whose first leader to apply half
// of them crashes.
type raftCluster struct {
	s        *sim.Scheduler
	seeded   bool
	storage  [raftNodes]*raft.MemoryStorage
	replicas [raftNodes]*raftReplica
	history  []byte
	next     uint64
	crashed  bool
	// elections counts replicas becoming leader, and atCrash the count when the leader crashed.
	elections int
	atCrash   int
}

type raftReplica struct {
	c        *raftCluster
	rn       *raft.RawNode
	id       uint32
	deadline core.Tick
	applied  int
}

func newRaftCluster(seed uint64, seeded bool) *raftCluster {
	set := sim.DefaultSettings()
	set.NetJitter = 200
	set.DropNum, set.DropDen = 1, 100
	set.DupNum, set.DupDen = 1, 50
	set.PartitionNum, set.PartitionDen = 1, 3000
	set.MsgType = raftMsgType
	c := &raftCluster{s: sim.New(seed, set), seeded: seeded, next: 1}
	for id := range uint32(raftNodes) {
		// The storage outlives crashes, standing for a disk each pass writes before its sends leave.
		c.storage[id] = raft.NewMemoryStorage()
		boot := raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 1, Term: 1, ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}}}}
		if err := c.storage[id].ApplySnapshot(boot); err != nil {
			panic(err)
		}
		c.s.AddNode(sim.DefaultClock, nil, func(n *sim.Node) { c.boot(n, id) })
	}
	return c
}

func (c *raftCluster) boot(n *sim.Node, id uint32) {
	cfg := &raft.Config{
		ID:              uint64(id) + 1,
		ElectionTick:    100,
		HeartbeatTick:   10,
		Storage:         c.storage[id],
		MaxSizePerMsg:   128,
		MaxInflightMsgs: 16,
		PreVote:         true,
		Logger:          quietRaft,
	}
	src := n.Fork()
	if c.seeded {
		cfg.Rand = rand.New(rand.NewPCG(src.Uint64(), src.Uint64()))
	}
	rn, err := raft.NewRawNode(cfg)
	if err != nil {
		panic(err)
	}
	r := &raftReplica{c: c, rn: rn, id: id}
	c.replicas[id] = r
	n.Call(n.Start(r, nil), r.arm)
}

func (c *raftCluster) run() *raftCluster {
	for c.s.Time() < raftRunFor && c.s.Step() {
		if c.crashed {
			continue
		}
		for id, r := range c.replicas {
			if c.s.Node(uint32(id)).Up() && r.rn.BasicStatus().RaftState == raft.StateLeader && r.applied >= raftOps/2 {
				c.s.CrashRandom(uint32(id))
				c.crashed, c.atCrash = true, c.elections
				break
			}
		}
	}
	return c
}

func (c *raftCluster) mostApplied() int {
	most := 0
	for _, r := range c.replicas {
		most = max(most, r.applied)
	}
	return most
}

func raftMsgType(m *core.Msg) uint64 {
	var pm raftpb.Message
	if err := pm.Unmarshal(m.Inline()); err != nil {
		panic(err)
	}
	return uint64(pm.Type)
}

func (r *raftReplica) arm(ctx *core.Context, out []core.Action) []core.Action {
	r.deadline = ctx.Now + raftTick
	return append(out, core.Action{Kind: core.ActSetTimer, Timer: raftTimer, At: r.deadline})
}

func (r *raftReplica) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	switch ev.Kind {
	case core.EvTimer:
		if ctx.Now < r.deadline {
			return out
		}
		r.rn.Tick()
		if r.rn.BasicStatus().RaftState == raft.StateLeader && r.c.mostApplied() < raftOps {
			_ = r.rn.Propose(binary.LittleEndian.AppendUint64(nil, r.c.next))
			r.c.next++
		}
		out = r.arm(ctx, out)
	case core.EvNetRecv:
		var m raftpb.Message
		if err := m.Unmarshal(ev.Msg.Inline()); err != nil {
			panic(err)
		}
		_ = r.rn.Step(m)
	case core.EvPeerLost:
		r.rn.ReportUnreachable(uint64(ev.Peer.Node) + 1)
	}
	return r.ready(ctx, out)
}

func (r *raftReplica) ready(ctx *core.Context, out []core.Action) []core.Action {
	c := r.c
	for r.rn.HasReady() {
		rd := r.rn.Ready()
		c.history = fmt.Appendf(c.history, "%d n%d %s\n", ctx.Now, r.id, raft.DescribeReady(rd, nil))
		if !raft.IsEmptyHardState(rd.HardState) {
			if err := c.storage[r.id].SetHardState(rd.HardState); err != nil {
				panic(err)
			}
		}
		if err := c.storage[r.id].Append(rd.Entries); err != nil {
			panic(err)
		}
		if rd.SoftState != nil && rd.SoftState.RaftState == raft.StateLeader {
			c.elections++
		}
		for _, m := range rd.Messages {
			b, err := m.Marshal()
			if err != nil {
				panic(err)
			}
			a := core.Action{Kind: core.ActNetSend, Peer: core.NodeID{Node: uint32(m.To - 1)}}
			if err := a.Msg.SetInline(b); err != nil {
				panic(fmt.Sprintf("%s: %v", raft.DescribeMessage(m, nil), err))
			}
			out = append(out, a)
		}
		for _, e := range rd.CommittedEntries {
			if e.Type == raftpb.EntryNormal && len(e.Data) > 0 {
				r.applied++
			}
		}
		r.rn.Advance(rd)
	}
	return out
}

func TestRaftRunsReplayFromTheirSeed(t *testing.T) {
	seeds.Run(t, 0, 20, func(seed uint64) {
		a := newRaftCluster(seed, true).run()
		if !a.crashed || a.elections <= a.atCrash || a.mostApplied() <= raftOps/2 {
			t.Fatalf("seed %d: crashed %v, %d elections (%d before the crash), %d values applied", seed, a.crashed, a.elections, a.atCrash, a.mostApplied())
		}
		b := newRaftCluster(seed, true).run()
		if err := sim.CompareTraces(a.s.Trace(), b.s.Trace()); err != nil {
			t.Fatalf("seed %d: trace replays differently: %v", seed, err)
		}
		if err := sim.CompareTraces(a.history, b.history); err != nil {
			t.Fatalf("seed %d: Ready history replays differently: %v", seed, err)
		}
	})
}

func TestUnseededElectionTimeoutsBreakReplay(t *testing.T) {
	a, b := newRaftCluster(1, false).run(), newRaftCluster(1, false).run()
	if sim.CompareTraces(a.s.Trace(), b.s.Trace()) == nil {
		t.Fatal("two runs of seed 1 with crypto/rand election timeouts gave one trace, so the replay test would miss unseeded randomness")
	}
}
