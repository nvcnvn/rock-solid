// Package toy is a primary-backup protocol with bugs to plant, on which the simulator shows it finds a planted bug and replays its seed.
package toy

import (
	"encoding/binary"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/trace"
)

// RetryTimer is the timer on which the primary resends the writes still waiting for a replica.
const RetryTimer core.TimerID = 1

const (
	writeLen = 25
	replyLen = 17
)

// Bug is a mistake planted in the protocol.
type Bug uint8

// The bugs a test can plant.
const (
	NoBug Bug = iota
	// AckBeforeReplica acknowledges a value once the primary's own copy is durable.
	AckBeforeReplica
	// IgnoreEpoch takes a reply to an earlier primary incarnation's write as one to its own write of the same sequence number.
	IgnoreEpoch
)

// Config is a machine's place in the cluster and the bug planted in it.
type Config struct {
	Self core.NodeID
	// Peers are the replicas the primary writes to; a replica has none.
	Peers []core.NodeID
	// Epoch is the machine's incarnation, raised at every start.
	Epoch uint64
	Bug   Bug
	// Ack is called with each value the primary acknowledges.
	Ack func(value uint64)
}

type diskIO struct {
	id    core.IOID
	epoch uint64
	seq   uint64
	value uint64
	from  core.NodeID
	buf   core.IOBuf
	fsync bool
}

type pending struct {
	seq      uint64
	value    uint64
	local    bool
	replicas int
}

// Machine is one node: a primary writes each value to its disk and its replicas', resending until one replies, and acknowledges it once durable locally and on one replica.
type Machine struct {
	cfg      Config
	rand     *core.Rand
	nextIO   core.IOID
	ios      []diskIO
	seq      uint64
	pending  []pending
	deadline core.Tick
	armed    bool
	stale    int
	unknown  int
	diskErrs int
}

// New makes a machine that draws its retry jitter from r.
func New(r *core.Rand, cfg Config) *Machine {
	return &Machine{cfg: cfg, rand: r, nextIO: 1}
}

// Pending is the number of values the primary has not acknowledged yet.
func (m *Machine) Pending() int { return len(m.pending) }

// Armed reports whether the retry timer is armed.
func (m *Machine) Armed() bool { return m.armed }

// Stale is the number of retry timer events that arrived before the timer's deadline.
func (m *Machine) Stale() int { return m.stale }

// Unknown is the number of disk completions for no action the machine emitted.
func (m *Machine) Unknown() int { return m.unknown }

// DiskErrors is the number of disk actions that failed.
func (m *Machine) DiskErrors() int { return m.diskErrs }

// Msg encodes a write of value as sequence number seq of primary incarnation epoch, or with kind trace.MsgWriteReply its reply, which carries no value.
func Msg(kind trace.Msg, epoch, seq, value uint64) core.Msg {
	var m core.Msg
	var b [writeLen]byte
	b[0] = byte(kind)
	binary.LittleEndian.PutUint64(b[1:], epoch)
	binary.LittleEndian.PutUint64(b[9:], seq)
	n := replyLen
	if kind == trace.MsgWrite {
		binary.LittleEndian.PutUint64(b[17:], value)
		n = writeLen
	}
	if err := m.SetInline(b[:n]); err != nil {
		panic(err)
	}
	return m
}

// Propose starts writing value, which names the block it goes to.
func (m *Machine) Propose(ctx *core.Context, value uint64, out []core.Action) []core.Action {
	m.seq++
	m.pending = append(m.pending, pending{seq: m.seq, value: value})
	out = m.write(ctx, m.cfg.Epoch, m.seq, value, m.cfg.Self, out)
	for _, p := range m.cfg.Peers {
		out = append(out, core.Action{Kind: core.ActNetSend, Peer: p, Msg: Msg(trace.MsgWrite, m.cfg.Epoch, m.seq, value)})
	}
	return m.arm(ctx, out)
}

func (m *Machine) write(ctx *core.Context, epoch, seq, value uint64, from core.NodeID, out []core.Action) []core.Action {
	buf, err := ctx.Pool.AllocZeroed(core.BlockSize)
	if err != nil {
		return out
	}
	binary.LittleEndian.PutUint64(ctx.Pool.Bytes(buf), value)
	w, f := m.nextIO, m.nextIO+1
	m.nextIO += 2
	m.ios = append(m.ios,
		diskIO{id: w, epoch: epoch, seq: seq, value: value, from: from, buf: buf},
		diskIO{id: f, epoch: epoch, seq: seq, value: value, from: from, fsync: true})
	out = append(out, core.Action{Kind: core.ActDiskWrite, IO: w, LBA: core.LBA(value), Buf: buf, Len: core.BlockSize})
	return append(out, core.Action{Kind: core.ActDiskFsync, IO: f, Linked: true})
}

func (m *Machine) arm(ctx *core.Context, out []core.Action) []core.Action {
	m.deadline = ctx.Now + 300 + core.Tick(m.rand.Below(100))
	m.armed = true
	return append(out, core.Action{Kind: core.ActSetTimer, Timer: RetryTimer, At: m.deadline})
}

// OnEvent handles a disk completion, a message or the retry timer.
func (m *Machine) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	switch ev.Kind {
	case core.EvDiskDone:
		return m.diskDone(ctx, ev, out)
	case core.EvNetRecv:
		return m.recv(ctx, ev, out)
	case core.EvTimer:
		return m.retry(ctx, out)
	}
	return out
}

func (m *Machine) recv(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	b := ev.Msg.Inline()
	if len(b) < replyLen {
		return out
	}
	epoch := binary.LittleEndian.Uint64(b[1:])
	seq := binary.LittleEndian.Uint64(b[9:])
	switch {
	case trace.Msg(b[0]) == trace.MsgWrite && len(b) == writeLen:
		return m.write(ctx, epoch, seq, binary.LittleEndian.Uint64(b[17:]), ev.Peer, out)
	case trace.Msg(b[0]) == trace.MsgWriteReply && len(b) == replyLen:
		if epoch != m.cfg.Epoch && m.cfg.Bug != IgnoreEpoch {
			return out
		}
		if i := m.find(seq); i >= 0 {
			m.pending[i].replicas++
			m.commit()
		}
	}
	return out
}

func (m *Machine) retry(ctx *core.Context, out []core.Action) []core.Action {
	if !m.armed || ctx.Now < m.deadline {
		m.stale++
		return out
	}
	m.armed = false
	if len(m.pending) == 0 {
		return out
	}
	for _, p := range m.pending {
		for _, peer := range m.cfg.Peers {
			out = append(out, core.Action{Kind: core.ActNetSend, Peer: peer, Msg: Msg(trace.MsgWrite, m.cfg.Epoch, p.seq, p.value)})
		}
	}
	return m.arm(ctx, out)
}

func (m *Machine) diskDone(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	i := -1
	for j := range m.ios {
		if m.ios[j].id == ev.IO {
			i = j
			break
		}
	}
	if i < 0 {
		m.unknown++
		return out
	}
	x := m.ios[i]
	m.ios = append(m.ios[:i], m.ios[i+1:]...)
	if !x.fsync {
		ctx.Pool.Free(x.buf)
	}
	if ev.Err != core.DiskOK {
		m.diskErrs++
		return out
	}
	if !x.fsync {
		return out
	}
	if x.from != m.cfg.Self {
		return append(out, core.Action{Kind: core.ActNetSend, Peer: x.from, Msg: Msg(trace.MsgWriteReply, x.epoch, x.seq, 0)})
	}
	if j := m.find(x.seq); j >= 0 {
		m.pending[j].local = true
		m.commit()
	}
	return out
}

func (m *Machine) find(seq uint64) int {
	for i := range m.pending {
		if m.pending[i].seq == seq {
			return i
		}
	}
	return -1
}

func (m *Machine) commit() {
	kept := m.pending[:0]
	for _, p := range m.pending {
		if p.local && (p.replicas > 0 || m.cfg.Bug == AckBeforeReplica) {
			if m.cfg.Ack != nil {
				m.cfg.Ack(p.value)
			}
			continue
		}
		kept = append(kept, p)
	}
	m.pending = kept
}
