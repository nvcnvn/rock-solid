// Package raftshard is one replica of a Raft group: etcd-raft's RawNode driven by events, ticked from ctx.Now, its messages framed for the network, and each Ready that must be durable a round the driver flushes before the messages that depend on it leave.
package raftshard

import (
	"errors"
	"fmt"
	"math/rand/v2"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/core"
)

const (
	// TickInterval is the election timer's tick, 10 ms.
	TickInterval core.Tick = 10_000
	// HeartbeatTick is raft_heartbeat in ticks, 100 ms.
	HeartbeatTick = 10
	// ElectionTick is the shortest raft_election_timeout in ticks; etcd-raft draws each timeout from 1 to 1.99 s.
	ElectionTick = 100
	// maxTicks is the most ticks one event credits. A longer gap means the loop stood still, by a
	// suspend or a stall, and the leader's messages may wait behind it in the inbox, so the rest of
	// the gap is dropped rather than campaigned on.
	maxTicks = HeartbeatTick
)

// Peer is one replica of the group: its Raft ID and the loop that hosts it.
type Peer struct {
	ID   uint64
	Node core.NodeID
}

// Config is one replica's settings.
type Config struct {
	Group uint64
	ID    uint64
	// Peers names every replica the group sends to.
	Peers []Peer
	// Storage holds the replica's durable log and hard state; the shard writes a round to it once the round is durable.
	Storage *raft.MemoryStorage
	// Seed seeds the replica's election timeouts.
	Seed  uint64
	Timer core.TimerID
}

// Stats counts the messages a shard dropped: undecodable, addressed to another group or replica, or unsent for want of a pool buffer.
type Stats struct {
	Undecodable uint64
	Misrouted   uint64
	Unsent      uint64
}

// Output is what one call into a shard hands its driver: actions, the round to flush, and committed entries to apply in order.
type Output struct {
	Actions []core.Action
	// Flush, when not zero, is the round the driver makes durable and then reports with Durable.
	Flush     uint64
	Committed []raftpb.Entry
}

// Reset empties the output for reuse.
func (o *Output) Reset() {
	o.Actions, o.Flush, o.Committed = o.Actions[:0], 0, o.Committed[:0]
}

// Shard is one replica of a Raft group.
type Shard struct {
	cfg   Config
	rn    *raft.RawNode
	base  core.Tick
	armed core.Tick
	// rounds counts the rounds taken, round is the one in flight or 0, and rd its Ready.
	rounds uint64
	round  uint64
	rd     raft.Ready
	stats  Stats
	plant  plants
}

// plants are bugs a test plants: election timeouts from crypto/rand, and every message of a Ready sent before its flush.
type plants struct {
	unseeded, early bool
}

// New starts a replica from its storage.
func New(cfg Config) (*Shard, error) { return newShard(cfg, plants{}) }

func newShard(cfg Config, p plants) (*Shard, error) {
	rc := raftConfig(&cfg)
	if p.unseeded {
		rc.Rand = nil
	}
	rn, err := raft.NewRawNode(rc)
	if err != nil {
		return nil, err
	}
	return &Shard{cfg: cfg, rn: rn, plant: p}, nil
}

func raftConfig(cfg *Config) *raft.Config {
	return &raft.Config{
		ID:              cfg.ID,
		ElectionTick:    ElectionTick,
		HeartbeatTick:   HeartbeatTick,
		Storage:         cfg.Storage,
		MaxSizePerMsg:   1 << 20,
		MaxInflightMsgs: 256,
		PreVote:         true,
		// A Txn goes to the leader, which answers NotLeader elsewhere; a follower never forwards it.
		DisableProposalForwarding: true,
		Logger:                    quiet{},
		Rand:                      rand.New(rand.NewPCG(cfg.Seed, 0)),
	}
}

// Start counts ticks from now and arms the tick timer.
func (s *Shard) Start(ctx *core.Context, out *Output) {
	s.base = ctx.Now
	s.arm(out)
}

// Stats returns the shard's drop counts.
func (s *Shard) Stats() Stats { return s.stats }

// OnEvent ticks the replica up to ctx.Now, then handles ev: a peer's message, or a lost connection to a peer.
func (s *Shard) OnEvent(ctx *core.Context, ev *core.Event, out *Output) {
	s.tick(ctx, out)
	switch ev.Kind {
	case core.EvNetRecv:
		s.recv(ctx, &ev.Msg)
	case core.EvPeerLost:
		for _, p := range s.cfg.Peers {
			if p.Node == ev.Peer {
				s.rn.ReportUnreachable(p.ID)
			}
		}
	}
	s.ready(ctx, out)
}

// Propose appends data to the group's log if this replica leads, and returns raft.ErrProposalDropped otherwise.
func (s *Shard) Propose(ctx *core.Context, data []byte, out *Output) error {
	err := s.rn.Propose(data)
	s.ready(ctx, out)
	return err
}

// Durable reports the round in flight durable: the shard writes it to storage, sends the messages it held, hands over its committed entries and takes the next Ready.
func (s *Shard) Durable(ctx *core.Context, round uint64, out *Output) {
	if round == 0 || round != s.round {
		panic(fmt.Sprintf("raftshard: round %d reported durable while round %d is in flight", round, s.round))
	}
	rd := s.rd
	s.round, s.rd = 0, raft.Ready{}
	s.done(ctx, &rd, held, out)
	s.ready(ctx, out)
}

func (s *Shard) tick(ctx *core.Context, out *Output) {
	n := (ctx.Now - s.base) / TickInterval
	s.base += n * TickInterval
	for range min(n, maxTicks) {
		s.rn.Tick()
	}
	if s.base+TickInterval != s.armed {
		s.arm(out)
	}
}

func (s *Shard) arm(out *Output) {
	s.armed = s.base + TickInterval
	out.Actions = append(out.Actions, core.Action{Kind: core.ActSetTimer, Timer: s.cfg.Timer, At: s.armed})
}

func (s *Shard) recv(ctx *core.Context, msg *core.Msg) {
	var m raftpb.Message
	group, err := decode(ctx.Pool, msg, &m)
	if msg.Buf != core.NoBuf {
		ctx.Pool.Free(msg.Buf)
	}
	switch {
	case err != nil:
		s.stats.Undecodable++
	case group != s.cfg.Group || m.To != s.cfg.ID:
		s.stats.Misrouted++
	default:
		if errors.Is(s.rn.Step(m), raft.ErrStepPeerNotFound) {
			s.stats.Misrouted++
		}
	}
}

// ready takes Readys until one must be durable, which becomes the round in flight; no Ready is taken while a round is.
func (s *Shard) ready(ctx *core.Context, out *Output) {
	for s.round == 0 && s.rn.HasReady() {
		rd := s.rn.Ready()
		if rd.MustSync || !raft.IsEmptySnap(rd.Snapshot) {
			s.rounds++
			s.round, s.rd, out.Flush = s.rounds, rd, s.rounds
			s.send(ctx, rd.Messages, early, out)
			return
		}
		s.done(ctx, &rd, all, out)
	}
}

func (s *Shard) done(ctx *core.Context, rd *raft.Ready, p part, out *Output) {
	st := s.cfg.Storage
	if !raft.IsEmptySnap(rd.Snapshot) {
		if err := st.ApplySnapshot(rd.Snapshot); err != nil {
			panic(err)
		}
	}
	if !raft.IsEmptyHardState(rd.HardState) {
		if err := st.SetHardState(rd.HardState); err != nil {
			panic(err)
		}
	}
	if err := st.Append(rd.Entries); err != nil {
		panic(err)
	}
	s.send(ctx, rd.Messages, p, out)
	out.Committed = append(out.Committed, rd.CommittedEntries...)
	s.rn.Advance(*rd)
}

// part is which of a Ready's messages a send covers: all of them, those that leave with the round's write, or those held for its flush.
type part uint8

const (
	all part = iota
	early
	held
)

// leavesEarly reports whether a message leaves with its round's write: a leader's append, which
// the leader need not hold durable first, and a pre-vote or its answer, which change no hard state.
func leavesEarly(t raftpb.MessageType) bool {
	return t == raftpb.MsgApp || t == raftpb.MsgPreVote || t == raftpb.MsgPreVoteResp
}

func (s *Shard) send(ctx *core.Context, msgs []raftpb.Message, p part, out *Output) {
	for i := range msgs {
		m := &msgs[i]
		e := s.plant.early || leavesEarly(m.Type)
		if p == early && !e || p == held && e {
			continue
		}
		a := core.Action{Kind: core.ActNetSend, Peer: s.node(m.To), Reply: true}
		if !encode(ctx.Pool, s.cfg.Group, m, &a.Msg) {
			s.stats.Unsent++
			continue
		}
		out.Actions = append(out.Actions, a)
	}
}

func (s *Shard) node(id uint64) core.NodeID {
	for _, p := range s.cfg.Peers {
		if p.ID == id {
			return p.Node
		}
	}
	panic(fmt.Sprintf("raftshard: group %d sends to replica %d, which no peer names", s.cfg.Group, id))
}

// quiet drops etcd-raft's logs, since a sans-IO machine writes none, and keeps its panics.
type quiet struct{}

func (quiet) Debug(...any)            {}
func (quiet) Debugf(string, ...any)   {}
func (quiet) Error(...any)            {}
func (quiet) Errorf(string, ...any)   {}
func (quiet) Info(...any)             {}
func (quiet) Infof(string, ...any)    {}
func (quiet) Warning(...any)          {}
func (quiet) Warningf(string, ...any) {}
func (quiet) Fatal(v ...any)          { panic(fmt.Sprint(v...)) }
func (quiet) Fatalf(f string, v ...any) {
	panic(fmt.Sprintf(f, v...))
}
func (quiet) Panic(v ...any) { panic(fmt.Sprint(v...)) }
func (quiet) Panicf(f string, v ...any) {
	panic(fmt.Sprintf(f, v...))
}
