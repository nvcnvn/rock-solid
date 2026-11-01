// Package raftshard is one replica of a Raft group: etcd-raft's RawNode driven by events, ticked from ctx.Now, its messages framed for the network, each Ready that must be durable a round the driver flushes before the messages that depend on it leave, and its leader's lease fortified on the node-pair leases.
package raftshard

import (
	"errors"
	"fmt"
	"math/rand/v2"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/lease"
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
	// LapseCampaignMin and LapseCampaignMax bound raft_lapse_campaign, how long after its fortification ends with its grant a follower campaigns.
	LapseCampaignMin core.Tick = 150_000
	LapseCampaignMax core.Tick = 300_000
	// transferContext is the context etcd-raft puts on a transfer target's votes.
	transferContext = "CampaignTransfer"
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
	// Holder and Grantor are the node-pair leases of the replica's loop, which the loop renews and every shard on it reads: Holder those taken from its peers' loops, Grantor those it gives them.
	Holder  *lease.Holder
	Grantor *lease.Grantor
}

// Stats counts the messages a shard dropped: undecodable, addressed to another group or replica, or unsent for want of a pool buffer.
type Stats struct {
	Undecodable uint64
	Misrouted   uint64
	Unsent      uint64
}

// Output is what one call into a shard hands its driver: actions, the round to flush, committed entries to apply in order, and confirmed ReadIndex requests.
type Output struct {
	Actions []core.Action
	// Flush, when not zero, is the round the driver makes durable and then reports with Durable.
	Flush     uint64
	Committed []raftpb.Entry
	// Reads are ReadIndex requests a quorum confirmed: each read may be served once every entry up to its index is applied.
	Reads []raft.ReadState
}

// Reset empties the output for reuse.
func (o *Output) Reset() {
	o.Actions, o.Flush, o.Committed, o.Reads = o.Actions[:0], 0, o.Committed[:0], o.Reads[:0]
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
	rng    *rand.Rand
	conf   raftpb.ConfState
	// fort is, per peer, the term, run and support epoch it fortified this replica under as leader.
	fort []fortified
	// fortifyAt is when a leader next asks its unfortified peers; transferred the last term in which MsgTimeoutNow left; committed the newest term of an entry handed over to apply.
	fortifyAt   core.Tick
	transferred uint64
	committed   uint64
	// support is the leader's term and the support epoch this follower fortified it under, lead and leadID the leader's node and replica, and lapseAt when the follower campaigns after its grant to that node lapsed.
	support fortified
	lead    core.NodeID
	leadID  uint64
	lapseAt core.Tick
}

// fortified is a fortification: its term, and the run and support epoch of the follower's node's grant to the leader's node.
type fortified struct {
	term, run, support uint64
}

// plants are bugs a test plants: election timeouts from crypto/rand, every message of a Ready sent before its flush, and followers that vote and campaign while they support a leader.
type plants struct {
	unseeded, early, disloyal bool
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
	_, conf, err := cfg.Storage.InitialState()
	if err != nil {
		return nil, err
	}
	return &Shard{cfg: cfg, rn: rn, plant: p, rng: rand.New(rand.NewPCG(cfg.Seed, 1)), conf: conf, fort: make([]fortified, len(cfg.Peers))}, nil
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
		s.recv(ctx, &ev.Msg, out)
	case core.EvPeerLost:
		for _, p := range s.cfg.Peers {
			if p.Node == ev.Peer {
				s.rn.ReportUnreachable(p.ID)
			}
		}
	}
	s.ready(ctx, out)
	s.fortify(ctx, out)
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

// ApplyConfChange applies a committed configuration change, as the driver does when it applies the change's entry.
func (s *Shard) ApplyConfChange(cc raftpb.ConfChangeI) {
	s.conf = *s.rn.ApplyConfChange(cc)
}

// ReadIndex asks the group for the commit index a read must wait for; once a quorum confirms this replica still led when it asked, an Output's Reads carry rctx and that index.
func (s *Shard) ReadIndex(ctx *core.Context, rctx []byte, out *Output) {
	s.rn.ReadIndex(rctx)
	s.ready(ctx, out)
}

// LeaseRead reports whether this replica may serve a read now from the entries it handed over to apply, with no round trip: it leads, no MsgTimeoutNow left in its term, an entry of its term has committed, and each half of its configuration has a quorum of voters, itself included, whose fortification of its term runs on the node-pair leases.
func (s *Shard) LeaseRead(ctx *core.Context) bool {
	st := s.rn.BasicStatus()
	if st.RaftState != raft.StateLeader || s.transferred == st.Term || s.committed != st.Term {
		return false
	}
	t := lease.Now(ctx)
	return s.quorum(s.conf.Voters, st.Term, t) && s.quorum(s.conf.VotersOutgoing, st.Term, t)
}

// quorum reports whether a majority of voters, or an empty half, fortified the leader's term with a lease that runs now. The leader's own entry never ends: it votes for a challenger only after stepping down to a later term, which ends its lease.
func (s *Shard) quorum(voters []uint64, term uint64, t lease.Time) bool {
	if len(voters) == 0 {
		return true
	}
	n := 0
	for _, id := range voters {
		if id == s.cfg.ID || s.fortifies(s.peer(id), term, t) {
			n++
		}
	}
	return n > len(voters)/2
}

// fortifies reports whether peer i's fortification of term runs now: the holder's lease from its node runs under the run and support epoch it fortified under.
func (s *Shard) fortifies(i int, term uint64, t lease.Time) bool {
	f := s.fort[i]
	return f.term == term && s.cfg.Holder.Supported(s.cfg.Peers[i].Node, f.run, f.support, t)
}

// fortify has a leader ask, once a heartbeat, every peer whose fortification of its term does not run now; a follower whose grant to its leader's node lapsed stops supporting it and campaigns raft_lapse_campaign later.
func (s *Shard) fortify(ctx *core.Context, out *Output) {
	st := s.rn.BasicStatus()
	t := lease.Now(ctx)
	if st.RaftState == raft.StateLeader {
		if ctx.Now < s.fortifyAt {
			return
		}
		s.fortifyAt = ctx.Now + HeartbeatTick*TickInterval
		for i, p := range s.cfg.Peers {
			if p.ID != s.cfg.ID && !s.fortifies(i, st.Term, t) {
				s.sendFortify(&fortify{kind: kindFortify, from: s.cfg.ID, to: p.ID, term: st.Term}, p.Node, out)
			}
		}
		return
	}
	if s.support.term != 0 && !s.supports(&st, t) {
		if s.support.term == st.Term {
			s.lapseAt = ctx.Now + LapseCampaignMin + core.Tick(s.rng.Uint64N(uint64(LapseCampaignMax-LapseCampaignMin)+1))
		}
		s.support = fortified{}
	}
	if s.lapseAt != 0 && ctx.Now >= s.lapseAt {
		s.lapseAt = 0
		if st.RaftState == raft.StateFollower && s.support.term == 0 {
			_ = s.rn.Campaign()
			s.ready(ctx, out)
		}
	}
}

// supports reports whether this follower supports a leader of its term: its node's grant to the leader's node runs in the support epoch it fortified the leader under.
func (s *Shard) supports(st *raft.BasicStatus, t lease.Time) bool {
	if s.support.term == 0 || s.support.term != st.Term {
		return false
	}
	sup, ok := s.cfg.Grantor.Supporting(s.lead, t)
	return ok && sup == s.support.support
}

// refuses reports whether a (pre-)vote request is dropped before RawNode: in the restart wait, or while this follower supports a leader, unless it is a transfer target's vote. A request from the supported leader itself, for a later term, shows that it no longer leads, as after its node restarted and renewed the grant, so the follower stops supporting it.
func (s *Shard) refuses(ctx *core.Context, m *raftpb.Message) bool {
	t := lease.Now(ctx)
	if s.cfg.Grantor.Waiting(t) {
		return true
	}
	if m.From == s.leadID && m.Term > s.support.term {
		s.support = fortified{}
	}
	st := s.rn.BasicStatus()
	return !s.plant.disloyal && s.supports(&st, t) && !(m.Type == raftpb.MsgVote && string(m.Context) == transferContext)
}

// stopped reports whether the election timer stands still: a follower ticks no election while it supports a leader or its node waits out its restart.
func (s *Shard) stopped(ctx *core.Context) bool {
	st := s.rn.BasicStatus()
	if st.RaftState != raft.StateFollower {
		return false
	}
	t := lease.Now(ctx)
	return s.cfg.Grantor.Waiting(t) || !s.plant.disloyal && s.supports(&st, t)
}

func (s *Shard) tick(ctx *core.Context, out *Output) {
	n := (ctx.Now - s.base) / TickInterval
	s.base += n * TickInterval
	if n > 0 && s.stopped(ctx) {
		n = 0
	}
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

func (s *Shard) recv(ctx *core.Context, msg *core.Msg, out *Output) {
	var m raftpb.Message
	var f fortify
	group, kind, err := decode(ctx.Pool, msg, &m, &f)
	if msg.Buf != core.NoBuf {
		ctx.Pool.Free(msg.Buf)
	}
	to, from := m.To, m.From
	if kind != kindRaft {
		to, from = f.to, f.from
	}
	switch {
	case err != nil:
		s.stats.Undecodable++
	case group != s.cfg.Group || to != s.cfg.ID || kind != kindRaft && s.peer(from) < 0:
		s.stats.Misrouted++
	case kind == kindFortify:
		s.fortifyAsked(ctx, &f, out)
	case kind == kindFortified:
		s.fortifyAnswered(&f)
	case (m.Type == raftpb.MsgVote || m.Type == raftpb.MsgPreVote) && s.refuses(ctx, &m):
	default:
		if errors.Is(s.rn.Step(m), raft.ErrStepPeerNotFound) {
			s.stats.Misrouted++
		}
	}
}

// fortifyAsked supports the leader of this follower's term if its node's grant to the leader's node runs, and answers with that grant's run and support epoch.
func (s *Shard) fortifyAsked(ctx *core.Context, f *fortify, out *Output) {
	st := s.rn.BasicStatus()
	if f.term != st.Term || f.from != st.Lead {
		return
	}
	node := s.cfg.Peers[s.peer(f.from)].Node
	sup, ok := s.cfg.Grantor.Supporting(node, lease.Now(ctx))
	if !ok {
		return
	}
	s.support, s.lead, s.leadID, s.lapseAt = fortified{term: f.term, support: sup}, node, f.from, 0
	s.sendFortify(&fortify{kind: kindFortified, from: s.cfg.ID, to: f.from, term: f.term, run: s.cfg.Grantor.Run(), support: sup}, node, out)
}

// fortifyAnswered records a follower's fortification of this leader's term.
func (s *Shard) fortifyAnswered(f *fortify) {
	if st := s.rn.BasicStatus(); st.RaftState == raft.StateLeader && f.term == st.Term {
		s.fort[s.peer(f.from)] = fortified{term: f.term, run: f.run, support: f.support}
	}
}

func (s *Shard) sendFortify(f *fortify, node core.NodeID, out *Output) {
	a := core.Action{Kind: core.ActNetSend, Peer: node, Reply: true}
	encodeFortify(s.cfg.Group, f, &a.Msg)
	out.Actions = append(out.Actions, a)
}

// peer is the index in Peers of replica id, or -1.
func (s *Shard) peer(id uint64) int {
	for i, p := range s.cfg.Peers {
		if p.ID == id {
			return i
		}
	}
	return -1
}

// ready takes Readys until one must be durable, which becomes the round in flight; no Ready is taken while a round is.
func (s *Shard) ready(ctx *core.Context, out *Output) {
	for s.round == 0 && s.rn.HasReady() {
		rd := s.rn.Ready()
		if rd.SoftState != nil && rd.SoftState.RaftState == raft.StateLeader {
			s.fortifyAt = 0
		}
		for i := range rd.Messages {
			if rd.Messages[i].Type == raftpb.MsgTimeoutNow {
				s.transferred = rd.Messages[i].Term
			}
		}
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
		s.conf = rd.Snapshot.Metadata.ConfState
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
	if n := len(rd.CommittedEntries); n > 0 {
		s.committed = max(s.committed, rd.CommittedEntries[n-1].Term)
	}
	out.Reads = append(out.Reads, rd.ReadStates...)
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
	if i := s.peer(id); i >= 0 {
		return s.cfg.Peers[i].Node
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
