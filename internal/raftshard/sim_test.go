package raftshard

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

const (
	simNodes  = 3
	crashAt   = 20
	simRunFor = core.Tick(30_000_000)
)

// simCluster is one Raft group of three shards on the simulator, each flushing its rounds as an
// fsync on its node's disk. Leaders propose a numbered value per tick, the first leader to apply
// crashAt values crashes, and the run is done once a value proposed after the crash is applied.
type simCluster struct {
	s       *sim.Scheduler
	plant   plants
	inv     sim.Invariants
	storage [simNodes]*raft.MemoryStorage
	reps    [simNodes]*simReplica
	// log is every applied entry by index, and history every application in order.
	log        []raftpb.Entry
	history    []byte
	next       uint64
	crashed    bool
	sinceCrash uint64
	done       bool
	early      error
	disagree   error
}

type simReplica struct {
	c       *simCluster
	id      int
	s       *Shard
	out     Output
	applied int
	leads   bool
}

func newSimCluster(seed uint64, p plants) *simCluster {
	set := sim.DefaultSettings()
	set.NetJitter = 200
	set.DropNum, set.DropDen = 1, 100
	set.DupNum, set.DupDen = 1, 50
	set.PartitionNum, set.PartitionDen = 1, 3000
	set.PauseNum, set.PauseDen = 1, 20000
	set.MsgType = simMsgType
	c := &simCluster{s: sim.New(seed, set), plant: p, next: 1}
	c.inv.Register("driver contract", c.s.DriverContract)
	c.inv.Register("acknowledged only once durable", func() error { return c.early })
	c.inv.Register("applied entries agree", func() error { return c.disagree })
	disk := []sim.DiskSpec{{Blocks: 16, Settings: sim.DefaultDiskSettings()}}
	for id := range simNodes {
		// The storage outlives crashes: it is what the replica's rounds made durable.
		c.storage[id] = bootStorage(simNodes)
		c.s.AddNode(sim.DefaultClock, disk, func(n *sim.Node) { c.boot(n, id) })
	}
	return c
}

func (c *simCluster) boot(n *sim.Node, id int) {
	s, err := newShard(testConfig(simNodes, id, c.storage[id], n.Fork().Uint64()), c.plant)
	if err != nil {
		panic(err)
	}
	r := &simReplica{c: c, id: id, s: s}
	c.reps[id] = r
	loop := n.Start(sim.MachineFunc(r.onEvent), n.Disk(0))
	n.Call(loop, func(ctx *core.Context, acts []core.Action) []core.Action {
		r.out.Reset()
		s.Start(ctx, &r.out)
		return r.finish(ctx, acts)
	})
}

// run steps the group under faults for simRunFor, then, if it is not done, heals every partition
// and runs it with only drops left for simRunFor more, since a partition that never heals stops it.
func (c *simCluster) run(t *testing.T, seed uint64) *simCluster {
	for c.s.Time() < simRunFor && c.s.Step() {
		c.after(t, seed)
	}
	for a := range uint32(simNodes) {
		for b := range uint32(simNodes) {
			c.s.Heal(a, b)
		}
	}
	for end := c.s.Time() + simRunFor; c.s.Time() < end && !c.done; {
		cs := c.s.Choices()
		if len(cs) == 0 {
			break
		}
		c.s.Take(cs[0])
		c.after(t, seed)
	}
	return c
}

// after checks the invariants and crashes the first leader to apply crashAt values.
func (c *simCluster) after(t *testing.T, seed uint64) {
	if err := c.inv.Check(); err != nil {
		t.Fatalf("seed %d step %d: %v", seed, c.s.Steps(), err)
	}
	if c.crashed {
		return
	}
	for id, r := range c.reps {
		if c.s.Node(uint32(id)).Up() && r.leads && r.applied >= crashAt {
			c.s.CrashRandom(uint32(id))
			c.crashed, c.sinceCrash = true, c.next
			return
		}
	}
}

func simMsgType(m *core.Msg) uint64 {
	if m.Buf != core.NoBuf {
		return 1 << 8
	}
	var pm raftpb.Message
	if err := pm.Unmarshal(m.Inline()[groupBytes:]); err != nil {
		panic(err)
	}
	return uint64(pm.Type)
}

func (r *simReplica) onEvent(ctx *core.Context, ev *core.Event, acts []core.Action) []core.Action {
	r.out.Reset()
	if ev.Kind == core.EvDiskDone {
		if ev.Err != core.DiskOK {
			panic(ev.Err)
		}
		r.s.Durable(ctx, uint64(ev.IO), &r.out)
		return r.finish(ctx, acts)
	}
	r.s.OnEvent(ctx, ev, &r.out)
	if c := r.c; ev.Kind == core.EvTimer && r.leads && !c.done {
		v := binary.LittleEndian.AppendUint64(nil, c.next)
		if c.next%3 == 0 {
			v = append(v, make([]byte, 300)...)
		}
		_ = r.s.Propose(ctx, v, &r.out)
		c.next++
	}
	return r.finish(ctx, acts)
}

// finish checks what the pass sent against what its replica holds durable, records what it applied and hands its actions to the scheduler.
func (r *simReplica) finish(ctx *core.Context, acts []core.Action) []core.Action {
	c := r.c
	for i := range r.out.Actions {
		if a := &r.out.Actions[i]; a.Kind == core.ActNetSend && c.early == nil {
			c.early = r.durableFor(ctx, a)
		}
	}
	acts = append(acts, r.out.Actions...)
	if r.out.Flush != 0 {
		acts = append(acts, core.Action{Kind: core.ActDiskFsync, IO: core.IOID(r.out.Flush)})
	}
	for _, e := range r.out.Committed {
		c.history = fmt.Appendf(c.history, "%d n%d %d/%d %x\n", ctx.Now, r.id, e.Index, e.Term, e.Data)
		for uint64(len(c.log)) <= e.Index {
			c.log = append(c.log, raftpb.Entry{})
		}
		switch got := &c.log[e.Index]; {
		case got.Index == 0:
			*got = e
		case got.Term != e.Term || !bytes.Equal(got.Data, e.Data):
			if c.disagree == nil {
				c.disagree = fmt.Errorf("replica %d applied %d/%d %x where another applied %d/%d %x", r.id, e.Index, e.Term, e.Data, got.Index, got.Term, got.Data)
			}
		}
		if e.Type == raftpb.EntryNormal && len(e.Data) > 0 {
			r.applied++
			c.done = c.done || c.crashed && binary.LittleEndian.Uint64(e.Data) >= c.sinceCrash
		}
	}
	r.leads = r.s.rn.BasicStatus().RaftState == raft.StateLeader
	return acts
}

// durableFor reports an acknowledgment of entries, or a granted vote, that a sends before the
// replica's storage holds them. One Ready can hold a vote and a later term the replica moved to
// before the Ready was taken; once that term is durable the replica never votes in the older one
// again, so the vote may leave.
func (r *simReplica) durableFor(ctx *core.Context, a *core.Action) error {
	var m raftpb.Message
	if _, err := decode(ctx.Pool, &a.Msg, &m); err != nil {
		return err
	}
	st := r.c.storage[r.id]
	hs, _, _ := st.InitialState()
	switch {
	case m.Type == raftpb.MsgAppResp && !m.Reject:
		if last, _ := st.LastIndex(); last < m.Index {
			return fmt.Errorf("replica %d acknowledged index %d in term %d holding up to %d in term %d", r.id, m.Index, m.Term, last, hs.Term)
		}
	case m.Type == raftpb.MsgVoteResp && !m.Reject && hs.Term <= m.Term:
		if hs.Term < m.Term || hs.Vote != m.To {
			return fmt.Errorf("replica %d granted %d its vote in term %d holding %+v", r.id, m.To, m.Term, hs)
		}
	}
	return nil
}

func TestRaftRunsReplayFromTheirSeed(t *testing.T) {
	seen := map[string]int{}
	seeds.Run(t, 0, 20, func(seed uint64) {
		a := newSimCluster(seed, plants{}).run(t, seed)
		for _, name := range []string{"msg=256", " Pause ", " Restart", " PeerLost "} {
			seen[name] += bytes.Count(a.s.Trace(), []byte(name))
		}
		if !a.done {
			t.Fatalf("seed %d: crashed %v, no value proposed after the crash applied", seed, a.crashed)
		}
		b := newSimCluster(seed, plants{}).run(t, seed)
		if err := sim.CompareTraces(a.s.Trace(), b.s.Trace()); err != nil {
			t.Fatalf("seed %d: trace replays differently: %v", seed, err)
		}
		if err := sim.CompareTraces(a.history, b.history); err != nil {
			t.Fatalf("seed %d: applied history replays differently: %v", seed, err)
		}
	})
	if !seeds.Default() {
		return
	}
	for name, n := range seen {
		if n == 0 {
			t.Errorf("no %q in 20 seeds' traces", name)
		}
	}
}

func TestUnseededElectionTimeoutsBreakReplay(t *testing.T) {
	a, b := newSimCluster(1, plants{unseeded: true}).run(t, 1), newSimCluster(1, plants{unseeded: true}).run(t, 1)
	if sim.CompareTraces(a.s.Trace(), b.s.Trace()) == nil {
		t.Fatal("two runs of seed 1 with crypto/rand election timeouts gave one trace, so the replay test would miss unseeded randomness")
	}
}

func TestSendingBeforeTheFlushBreaksTheDurabilityInvariant(t *testing.T) {
	for seed := range uint64(20) {
		c := newSimCluster(seed, plants{early: true})
		c.inv.Only("acknowledged only once durable")
		for c.s.Time() < simRunFor && c.s.Step() {
			if c.inv.Check() != nil {
				return
			}
		}
	}
	t.Fatal("20 seeds sending every message before its round's flush broke no invariant")
}
