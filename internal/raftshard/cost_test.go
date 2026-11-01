package raftshard

import (
	"math/rand/v2"
	"runtime"
	"runtime/metrics"
	"testing"

	"go.etcd.io/raft/v3"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/lease"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

const (
	costGroups = 1_000
	costNodes  = 3
	// costStep is one pass of the cluster: open load at 60,000 proposals/s puts 60 in each.
	costStep          core.Tick = 1_000
	costStepProposals           = 60
	maxAllocsPerMsg             = 3
	maxIdleGroupBytes           = 4 << 10
	// costCompaction is how often storages compact to their applied index. A node's state machines
	// compact once a checkpoint has flushed their memtables, about every 7 s at this load (64 MiB
	// of max_replay_bytes over some 9 MB/s of entries per node); once a second errs toward more
	// chunks freed and allocated again.
	costCompaction core.Tick = 1_000_000
)

// costNet is costGroups groups of three replicas, replica i of each on node i, where every shard
// of a node shares the node's buffer pool, node-pair leases and output, as the shards of one loop
// do. A pass ticks every shard when a tick is due, proposes, then delivers messages and completes
// flushes until none is left: synchronous rounds with flushes and network that take no time. No
// node-pair lease is granted, so no shard fortifies.
type costNet struct {
	now      core.Tick
	nodes    [costNodes]costNode
	shards   [][costNodes]*Shard
	inbox    []envelope
	flushing []costRound
	sent     uint64
	proposed uint64
	rng      *rand.Rand
	data     []byte
	// heap is the live heap before the groups started.
	heap uint64
}

type costNode struct {
	pool *sim.Pool
	ctx  core.Context
	ev   core.Event
	out  Output
}

type costRound struct {
	group, node int
	round       uint64
}

// newCostNet starts the groups and elects their leaders, spread over the nodes.
func newCostNet(t testing.TB) *costNet {
	c := &costNet{now: 5_000_000, shards: make([][costNodes]*Shard, costGroups), rng: rand.New(rand.NewPCG(1, 2)), data: make([]byte, 128)}
	var holders [costNodes]*lease.Holder
	var grantors [costNodes]*lease.Grantor
	peers := make([]Peer, costNodes)
	for i := range c.nodes {
		c.nodes[i].pool = sim.NewPool(1<<20, 0)
		holders[i] = lease.NewHolder(lease.DefaultConfig)
		grantors[i] = lease.NewGrantor(lease.DefaultConfig, uint64(i), lease.Time{})
		peers[i] = Peer{ID: uint64(i) + 1, Node: core.NodeID{Node: uint32(i)}}
	}
	c.heap = liveHeap()
	for g := range c.shards {
		for i := range costNodes {
			s, err := New(Config{Group: uint64(g) + 1, ID: uint64(i) + 1, Peers: peers, Storage: bootStorage(costNodes),
				Seed: uint64(g*costNodes + i), Timer: core.TimerID(g), Holder: holders[i], Grantor: grantors[i]})
			if err != nil {
				t.Fatal(err)
			}
			c.shards[g][i] = s
			s.Start(c.begin(i))
			c.collect(g, i)
		}
	}
	for g := range c.shards {
		s := c.shards[g][g%costNodes]
		if err := s.rn.Campaign(); err != nil {
			t.Fatal(err)
		}
		ctx, out := c.begin(g % costNodes)
		s.ready(ctx, out)
		c.collect(g, g%costNodes)
	}
	c.settle()
	for g := range c.shards {
		if c.shards[g][g%costNodes].rn.BasicStatus().RaftState != raft.StateLeader {
			t.Fatalf("group %d elected no leader on node %d", g, g%costNodes)
		}
	}
	return c
}

// idleHeap is the heap the groups hold per replica once they idle for a second.
func (c *costNet) idleHeap() uint64 {
	for range 1_000 {
		c.pass(0)
	}
	// Bursts grew the harness's queues, which no group holds.
	c.inbox, c.flushing = nil, nil
	return (liveHeap() - c.heap) / (costGroups * costNodes)
}

func liveHeap() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

func (c *costNet) begin(i int) (*core.Context, *Output) {
	n := &c.nodes[i]
	n.ctx = core.Context{Now: c.now, Raw: c.now, Pool: n.pool}
	n.out.Reset()
	return &n.ctx, &n.out
}

// collect takes node i's output from group g's replica: the round to flush and the messages to deliver.
func (c *costNet) collect(g, i int) {
	out := &c.nodes[i].out
	if out.Flush != 0 {
		c.flushing = append(c.flushing, costRound{group: g, node: i, round: out.Flush})
	}
	for _, a := range out.Actions {
		if a.Kind != core.ActNetSend {
			continue
		}
		c.sent++
		to := int(a.Peer.Node)
		if a.Msg.Buf != core.NoBuf {
			from, dst := c.nodes[i].pool, c.nodes[to].pool
			b, err := dst.Alloc(len(from.Bytes(a.Msg.Buf)))
			if err != nil {
				panic(err)
			}
			copy(dst.Bytes(b), from.Bytes(a.Msg.Buf))
			from.Free(a.Msg.Buf)
			a.Msg.Buf = b
		}
		c.inbox = append(c.inbox, envelope{from: i, to: to, msg: a.Msg})
	}
}

func (c *costNet) event(g, i int, ev core.Event) {
	n := &c.nodes[i]
	ctx, out := c.begin(i)
	n.ev = ev
	c.shards[g][i].OnEvent(ctx, &n.ev, out)
	c.collect(g, i)
}

func (c *costNet) settle() {
	for len(c.inbox)+len(c.flushing) > 0 {
		for k := 0; k < len(c.inbox); k++ {
			e := c.inbox[k]
			g := int(le.Uint64(e.msg.Inline())) - 1
			c.event(g, e.to, core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{Node: uint32(e.from)}, Msg: e.msg})
		}
		c.inbox = c.inbox[:0]
		for k := 0; k < len(c.flushing); k++ {
			f := c.flushing[k]
			ctx, out := c.begin(f.node)
			c.shards[f.group][f.node].Durable(ctx, f.round, out)
			c.collect(f.group, f.node)
		}
		c.flushing = c.flushing[:0]
	}
}

// pass advances the clock by costStep, compacts every storage to its applied index once a
// second, ticks every shard if a tick is due, proposes n 128-byte entries to leaders of groups
// drawn at random, and settles.
func (c *costNet) pass(n int) {
	c.now += costStep
	if c.now%costCompaction == 0 {
		c.compact()
	}
	if c.now%TickInterval == 0 {
		for g := range c.shards {
			for i := range costNodes {
				c.event(g, i, core.Event{Kind: core.EvTimer, Timer: core.TimerID(g)})
			}
		}
	}
	for range n {
		g := c.rng.IntN(costGroups)
		i := g % costNodes
		ctx, out := c.begin(i)
		if err := c.shards[g][i].Propose(ctx, c.data, out); err != nil {
			panic(err)
		}
		c.collect(g, i)
		c.proposed++
	}
	c.settle()
}

func (c *costNet) compact() {
	for g := range c.shards {
		for _, s := range c.shards[g] {
			if i, cs := s.Applied(); i > s.cfg.Storage.Base().Index {
				if err := s.cfg.Storage.Compact(i, cs); err != nil {
					panic(err)
				}
			}
		}
	}
}

// cost is what a run costs: allocations, messages sent, proposals, and the collector's and the program's CPU.
type cost struct {
	allocs, msgs, proposals uint64
	gc, cpu                 float64
}

func (c *costNet) measure(run func()) cost {
	samples := []metrics.Sample{{Name: "/cpu/classes/gc/total:cpu-seconds"}, {Name: "/cpu/classes/user:cpu-seconds"}}
	var before, after runtime.MemStats
	metrics.Read(samples)
	runtime.ReadMemStats(&before)
	gc, user, sent, proposed := samples[0].Value.Float64(), samples[1].Value.Float64(), c.sent, c.proposed
	run()
	runtime.ReadMemStats(&after)
	metrics.Read(samples)
	gc, user = samples[0].Value.Float64()-gc, samples[1].Value.Float64()-user
	return cost{allocs: after.Mallocs - before.Mallocs, msgs: c.sent - sent, proposals: c.proposed - proposed, gc: gc, cpu: gc + user}
}

func (c *costNet) load(passes int) {
	for range passes {
		c.pass(costStepProposals)
	}
}

// TestCostPerGroup holds the Raft shard's cost at 1,000 groups per node to its budget: at most
// 3 allocations per message under open load at 60,000 proposals/s, and at most 4 KiB of heap per
// idle group, whether it never wrote or wrote and compacted to its applied index.
func TestCostPerGroup(t *testing.T) {
	c := newCostNet(t)
	idle := c.idleHeap()
	if idle > maxIdleGroupBytes {
		t.Errorf("an idle group holds %d bytes of heap per node, over %d", idle, maxIdleGroupBytes)
	}
	c.load(100)
	m := c.measure(func() { c.load(200) })
	if per := float64(m.allocs) / float64(m.msgs); per > maxAllocsPerMsg {
		t.Errorf("%.2f allocations per message (%d over %d messages), over %d", per, m.allocs, m.msgs, maxAllocsPerMsg)
	}
	c.compact()
	written := c.idleHeap()
	if written > maxIdleGroupBytes {
		t.Errorf("an idle group that wrote holds %d bytes of heap per node, over %d", written, maxIdleGroupBytes)
	}
	t.Logf("idle group %d B, written %d B per node; %.2f allocations per message, %.1f per proposal, %.1f messages per proposal",
		idle, written, float64(m.allocs)/float64(m.msgs), float64(m.allocs)/float64(m.proposals), float64(m.msgs)/float64(m.proposals))
}

// BenchmarkCostPerGroup is one pass of 1 ms at 60,000 proposals/s over 1,000 groups per node, and
// BenchmarkCostPerGroup/idle one 10 ms tick of every replica with no proposals; it reports the
// metrics docs/test-scenarios.md tracks under Raft cost per group.
func BenchmarkCostPerGroup(b *testing.B) {
	b.Run("load", func(b *testing.B) {
		c := newCostNet(b)
		idle := c.idleHeap()
		c.load(100)
		b.ReportAllocs()
		m := c.measure(func() {
			for b.Loop() {
				c.pass(costStepProposals)
			}
		})
		b.ReportMetric(float64(m.allocs)/float64(m.msgs), "allocs/msg")
		b.ReportMetric(float64(m.allocs)/float64(m.proposals), "allocs/proposal")
		b.ReportMetric(float64(m.msgs)/float64(m.proposals), "msgs/proposal")
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(m.proposals), "ns/proposal")
		b.ReportMetric(100*m.gc/m.cpu, "gc-cpu-%")
		b.ReportMetric(float64(idle), "B/idle-group")
	})
	b.Run("idle", func(b *testing.B) {
		c := newCostNet(b)
		b.ReportAllocs()
		for b.Loop() {
			for range TickInterval / costStep {
				c.pass(0)
			}
		}
		b.ReportMetric(100*float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(TickInterval*1000)/costNodes, "core-%/node")
	})
}
