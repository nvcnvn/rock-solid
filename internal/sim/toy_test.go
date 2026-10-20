package sim_test

import (
	"encoding/binary"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/trace"
)

const retryTimer core.TimerID = 1

const toyBlocks = 64

type toyIO struct {
	io    core.IOID
	seq   uint64
	from  core.NodeID
	buf   core.IOBuf
	fsync bool
}

type toyPending struct {
	seq      uint64
	local    bool
	replicas int
}

// toy is a primary-backup protocol: the primary writes each sequence number to its disk and
// to every replica's, and acknowledges it once it is durable locally and on one replica.
type toy struct {
	self     core.NodeID
	peers    []core.NodeID
	rand     *core.Rand
	nextIO   core.IOID
	ios      []toyIO
	seq      uint64
	pending  []toyPending
	deadline core.Tick
	armed    bool
	acked    []uint64
	unknown  []core.IOID
	stale    int
	diskErrs int
}

func newToy(r *core.Rand, self core.NodeID, peers []core.NodeID) *toy {
	return &toy{self: self, peers: peers, rand: r, nextIO: 1}
}

func toyMsg(kind trace.Msg, seq uint64) core.Msg {
	var m core.Msg
	var b [9]byte
	b[0] = byte(kind)
	binary.LittleEndian.PutUint64(b[1:], seq)
	if err := m.SetInline(b[:]); err != nil {
		panic(err)
	}
	return m
}

func (t *toy) write(ctx *core.Context, seq uint64, from core.NodeID, out []core.Action) []core.Action {
	buf, err := ctx.Pool.AllocZeroed(core.BlockSize)
	if err != nil {
		return out
	}
	binary.LittleEndian.PutUint64(ctx.Pool.Bytes(buf), seq)
	w, f := t.nextIO, t.nextIO+1
	t.nextIO += 2
	t.ios = append(t.ios, toyIO{io: w, seq: seq, from: from, buf: buf}, toyIO{io: f, seq: seq, from: from, fsync: true})
	out = append(out, core.Action{Kind: core.ActDiskWrite, IO: w, LBA: core.LBA(seq % toyBlocks), Buf: buf, Len: core.BlockSize})
	return append(out, core.Action{Kind: core.ActDiskFsync, IO: f, Linked: true})
}

func (t *toy) arm(ctx *core.Context, out []core.Action) []core.Action {
	t.deadline = ctx.Now + 300 + core.Tick(t.rand.Below(100))
	t.armed = true
	return append(out, core.Action{Kind: core.ActSetTimer, Timer: retryTimer, At: t.deadline})
}

func (t *toy) propose(ctx *core.Context, out []core.Action) []core.Action {
	t.seq++
	t.pending = append(t.pending, toyPending{seq: t.seq})
	out = t.write(ctx, t.seq, t.self, out)
	for _, p := range t.peers {
		out = append(out, core.Action{Kind: core.ActNetSend, Peer: p, Msg: toyMsg(trace.MsgWrite, t.seq)})
	}
	return t.arm(ctx, out)
}

func (t *toy) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	switch ev.Kind {
	case core.EvDiskDone:
		return t.diskDone(ctx, ev, out)
	case core.EvNetRecv:
		if ev.Msg.Len != 9 {
			return out
		}
		b := ev.Msg.Inline()
		seq := binary.LittleEndian.Uint64(b[1:])
		switch trace.Msg(b[0]) {
		case trace.MsgWrite:
			return t.write(ctx, seq, ev.Peer, out)
		case trace.MsgWriteReply:
			if i := t.find(seq); i >= 0 {
				t.pending[i].replicas++
				t.commit()
			}
		}
	case core.EvTimer:
		if !t.armed || ctx.Now < t.deadline {
			t.stale++
			return out
		}
		t.armed = false
		if len(t.pending) == 0 {
			return out
		}
		for _, p := range t.pending {
			for _, peer := range t.peers {
				out = append(out, core.Action{Kind: core.ActNetSend, Peer: peer, Msg: toyMsg(trace.MsgWrite, p.seq)})
			}
		}
		return t.arm(ctx, out)
	}
	return out
}

func (t *toy) diskDone(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	i := slices.IndexFunc(t.ios, func(x toyIO) bool { return x.io == ev.IO })
	if i < 0 {
		t.unknown = append(t.unknown, ev.IO)
		return out
	}
	x := t.ios[i]
	t.ios = slices.Delete(t.ios, i, i+1)
	if !x.fsync {
		ctx.Pool.Free(x.buf)
	}
	if ev.Err != core.DiskOK {
		t.diskErrs++
		return out
	}
	if !x.fsync {
		return out
	}
	if x.from != t.self {
		return append(out, core.Action{Kind: core.ActNetSend, Peer: x.from, Msg: toyMsg(trace.MsgWriteReply, x.seq)})
	}
	if j := t.find(x.seq); j >= 0 {
		t.pending[j].local = true
		t.commit()
	}
	return out
}

func (t *toy) find(seq uint64) int {
	return slices.IndexFunc(t.pending, func(p toyPending) bool { return p.seq == seq })
}

func (t *toy) commit() {
	t.pending = slices.DeleteFunc(t.pending, func(p toyPending) bool {
		if p.local && p.replicas > 0 {
			t.acked = append(t.acked, p.seq)
			return true
		}
		return false
	})
}

type toyClient struct {
	m    *toy
	left int
}

func (c *toyClient) Ready() bool { return c.left > 0 && len(c.m.pending) < 2 }

func (c *toyClient) Next(ctx *core.Context, out []core.Action) []core.Action {
	c.left--
	return c.m.propose(ctx, out)
}

// toyCluster is three nodes running the toy protocol, node 0 the primary with a client of ops writes.
type toyCluster struct {
	s     *sim.Scheduler
	nodes [3]*toy
	boots [3]int
}

func newToyCluster(seed uint64, set sim.Settings, ops int) *toyCluster {
	c := &toyCluster{s: sim.New(seed, set)}
	disk := []sim.DiskSpec{{Blocks: toyBlocks, Settings: sim.DefaultDiskSettings()}}
	for id := range uint32(3) {
		c.s.AddNode(sim.DefaultClock, disk, func(n *sim.Node) {
			var peers []core.NodeID
			if id == 0 {
				peers = []core.NodeID{{Node: 1}, {Node: 2}}
			}
			m := newToy(n.Fork(), core.NodeID{Node: id}, peers)
			c.nodes[id] = m
			c.boots[id]++
			loop := n.Start(m, n.Disk(0))
			if id == 0 {
				n.AddActor(loop, &toyClient{m: m, left: ops})
			}
		})
	}
	return c
}

func chaosSettings() sim.Settings {
	set := sim.DefaultSettings()
	set.DropNum, set.DropDen = 1, 8
	set.CrashNum, set.CrashDen = 1, 300
	set.StaleNum, set.StaleDen = 1, 4
	return set
}

func seedTrace(seed uint64) []byte {
	c := newToyCluster(seed, chaosSettings(), 40)
	c.s.Run(3000)
	return c.s.Trace()
}
