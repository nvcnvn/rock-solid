// Package raftshard is one replica of a Raft group: etcd-raft's RawNode driven by events, ticked from ctx.Now, its messages framed for the network, each Ready that must be durable a round the driver flushes before the messages that depend on it leave, and its leader's lease fortified on the node-pair leases.
package raftshard

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"

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
	// Peers names the replicas the group sends to; a replica a configuration change adds joins them from the change's context, and one it removes leaves them.
	Peers []Peer
	// Storage holds the replica's log and hard state as its durable rounds left them; the shard writes a round to it once the round is durable.
	Storage *Storage
	// SnapshotData is the state machine's contents at the index last handed over to apply, which a snapshot built on demand carries; nil leaves snapshots without data.
	SnapshotData func(index uint64) []byte
	// Apply, when set, takes each restored snapshot, empty if none, and the committed entries as the shard hands them over, in place of Output's Snapshot and Committed, so the state machine holds them before the shard can build a snapshot later in the same call.
	Apply func(snap raftpb.Snapshot, ents []raftpb.Entry)
	// CheckSnapshot, when set, reports whether a snapshot's data restores; a snapshot message whose data does not is dropped as undecodable.
	CheckSnapshot func(data []byte) bool
	// Seed seeds the replica's election timeouts.
	Seed  uint64
	Timer core.TimerID
	// Holder and Grantor are the node-pair leases of the replica's loop, which the loop renews and every shard on it reads: Holder those taken from its peers' loops, Grantor those it gives them.
	Holder  *lease.Holder
	Grantor *lease.Grantor
}

// Stats counts the messages a shard dropped: undecodable, addressed to another group or replica, or unsent for want of a pool buffer or of the node of a replica no longer in Peers.
type Stats struct {
	Undecodable uint64
	Misrouted   uint64
	Unsent      uint64
}

// Output is what one call into a shard hands its driver: actions, the round to flush, committed entries to apply in order, and confirmed ReadIndex requests.
type Output struct {
	Actions []core.Action
	// Flush, when not zero, is the round the driver makes durable, writing Round to the disk log, and then reports with Durable.
	Flush     uint64
	Round     Round
	Committed []raftpb.Entry
	// Reads are ReadIndex requests a quorum confirmed: each read may be served once every entry up to its index is applied.
	Reads []raft.ReadState
	// Snapshot, when not empty, is a snapshot the replica restored, which its state machine restores before applying Committed.
	Snapshot raftpb.Snapshot
	// Removed reports that this replica learned the group's committed configuration no longer holds it, from its log or a peer's notice. It stays a voter of the configuration it applied, and its node deletes it only once the control plane says the group's plan is complete.
	Removed bool
}

// Reset empties the output for reuse.
func (o *Output) Reset() {
	o.Actions, o.Flush, o.Committed, o.Reads = o.Actions[:0], 0, o.Committed[:0], o.Reads[:0]
	o.Round = Round{}
	o.Snapshot, o.Removed = raftpb.Snapshot{}, false
}

// Shard is one replica of a Raft group.
type Shard struct {
	cfg   Config
	rn    *raft.RawNode
	base  core.Tick
	armed core.Tick
	// rounds counts the rounds taken, round is the one in flight or 0, and rd its Ready, with the hard state the round writes.
	rounds uint64
	round  uint64
	rd     raft.Ready
	stats  Stats
	plant  plants
	rng    *rand.Rand
	conf   raftpb.ConfState
	// hold is which of the round's messages its flush sends, hs the newest hard state etcd-raft handed out, which it hands out only once, and timeoutTerm and timeoutFrom the term and sender of a transfer's MsgTimeoutNow that came while a round was in flight, or 0.
	hold        part
	hs          raftpb.HardState
	timeoutTerm uint64
	timeoutFrom uint64
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
	// plan is the voters the control plane drives the group toward, confTerm the term of a configuration change this leader proposed and has not applied, mark the commit index at markAt less a heartbeat, applied the index of the last entry handed over to apply, confAt an index at which the applied configuration holds, heard when this replica last heard from its leader, removed whether it learned it left the group, leave the index of the committed change that would take it out of the configuration, which it holds unapplied and before which it hands nothing over, and saving whether the round in flight records the removal.
	plan     []Peer
	confTerm uint64
	mark     uint64
	markAt   core.Tick
	applied  uint64
	confAt   uint64
	heard    core.Tick
	removed  bool
	leave    uint64
	saving   bool
	// applying is set while etcd-raft applies a configuration change, snaps are the snapshots sent but not yet reported, and kept a leader the applied configuration dropped that stays in Peers while it leads.
	applying bool
	snaps    []sentSnap
	kept     uint64
	// halted is whether the state machine halted; a halted leader next hands its leadership on at handAt, to the voter after handedTo.
	halted   bool
	handAt   core.Tick
	handedTo uint64
}

// fortified is a fortification: its term, and the run and support epoch of the follower's node's grant to the leader's node.
type fortified struct {
	term, run, support uint64
}

// plants are bugs a test plants: a removed replica that stops at once, as if its node deleted it, and one that applies its removal to its Raft configuration and so campaigns no more; election timeouts from crypto/rand, every message of a Ready sent before its flush, followers that vote and campaign while they support a leader, a learner promoted before it caught up, a replacement added as a voter, a learner held to the current commit index, lease reads before an entry of the leader's term commits, snapshots kept from the last compaction instead of built at the applied index, a leader counting a fortification under whatever support epoch its lease from the peer now runs, as if grantors renewed a lapsed grant under its old epoch, a leader serving lease reads after MsgTimeoutNow left, a vote granted to every candidate, a follower serving lease reads while it supports its leader, a follower that never asks its leader's node about the leader, a leader serving lease reads after it applied its own removal, rounds that write only their own Ready's hard state and none for a Ready that commits a configuration change and moves only the commit index, and a configuration change applied, and its commit sent, before that commit index is durable.
type plants struct {
	unseeded, early, disloyal, eager, voterFirst, current, uncommitted, staleSnapshot bool
	noEpochBump, transferKeepsLease, voteEveryone, followerReads                      bool
	noSupportAsk, removedKeepsLease, oldHardState, noConfRound                        bool
	deletesAtOnce, appliesRemoval                                                     bool
}

// New starts a replica from its storage.
func New(cfg Config) (*Shard, error) { return newShard(cfg, plants{}) }

func newShard(cfg Config, p plants) (*Shard, error) {
	// The state machine restarts from the storage's base, and its configuration is the base's.
	base := cfg.Storage.Base()
	rc := raftConfig(&cfg)
	rc.Applied = base.Index
	if p.unseeded {
		rc.Rand = nil
	}
	rn, err := raft.NewRawNode(rc)
	if err != nil {
		return nil, err
	}
	cfg.Peers = slices.Clone(cfg.Peers)
	s := &Shard{cfg: cfg, rn: rn, plant: p, rng: rand.New(rand.NewPCG(cfg.Seed, 1)), conf: base.ConfState, applied: base.Index, confAt: base.Index, removed: cfg.Storage.Removed(), fort: make([]fortified, len(cfg.Peers)), hs: rn.BasicStatus().HardState}
	cfg.Storage.build = s.snapshot
	return s, nil
}

// Applied is the index of the last entry handed over to apply and the configuration at it. The log compacts to the state machine's applied index, which is this one unless the state machine halted before it.
func (s *Shard) Applied() (uint64, raftpb.ConfState) { return s.applied, s.conf }

// snapshot is a snapshot at the applied index, whose configuration holds every replica that
// joined by then: one kept from before a learner joined would not hold it, the learner would
// refuse it, and the leader would wait in StateSnapshot until the connection failed. While a
// configuration change applies, etcd-raft probes every replica and sends a snapshot to one behind
// the compaction, but the applied index and configuration do not yet hold the change, so the
// snapshot is temporarily unavailable and the leader asks again at its next append.
func (s *Shard) snapshot() (raftpb.Snapshot, error) {
	if s.plant.staleSnapshot {
		return raftpb.Snapshot{Metadata: s.cfg.Storage.Base()}, nil
	}
	if s.applying {
		return raftpb.Snapshot{}, raft.ErrSnapshotTemporarilyUnavailable
	}
	term, err := s.cfg.Storage.Term(s.applied)
	if err != nil {
		panic(fmt.Sprintf("raftshard: no term for applied index %d: %v", s.applied, err))
	}
	snap := raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: s.applied, Term: term, ConfState: s.conf}}
	if s.cfg.SnapshotData != nil {
		snap.Data = s.cfg.SnapshotData(s.applied)
	}
	return snap, nil
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
	s.base, s.heard = ctx.Now, ctx.Now
	s.arm(out)
}

// SetPlan hands the shard its group's voters in the shard map. While it leads, it adds each planned replica the configuration lacks as a learner, swaps a caught-up learner for a voter the plan dropped in one joint change, and removes a learner the plan dropped, one change at a time.
func (s *Shard) SetPlan(voters []Peer) { s.plan = append(s.plan[:0], voters...) }

// Stats returns the shard's drop counts.
func (s *Shard) Stats() Stats { return s.stats }

// OnEvent ticks the replica up to ctx.Now, then handles ev: a peer's message, or a lost connection to a peer.
func (s *Shard) OnEvent(ctx *core.Context, ev *core.Event, out *Output) {
	if s.gone() {
		if ev.Kind == core.EvNetRecv {
			s.answerGone(ev, out)
			if ev.Msg.Buf != core.NoBuf {
				ctx.Pool.Free(ev.Msg.Buf)
			}
		}
		return
	}
	s.tick(ctx, out)
	switch ev.Kind {
	case core.EvNetRecv:
		s.recv(ctx, ev, out)
	case core.EvPeerLost:
		for _, p := range s.cfg.Peers {
			if p.Node == ev.Peer {
				s.rn.ReportUnreachable(p.ID)
				s.rn.ReportSnapshot(p.ID, raft.SnapshotFailure)
			}
		}
	}
	if s.ready(ctx, out); !s.gone() {
		s.fortify(ctx, out)
	}
}

// gone reports whether the replica stopped for good on learning its removal, as only the
// deletesAtOnce plant has it: a removed replica runs on as a voter until its node deletes it.
func (s *Shard) gone() bool { return s.removed && s.plant.deletesAtOnce }

// Removed reports whether the replica learned its removal, and the index of the change that removed
// it, which it holds unapplied, or 0 for one that learned it from a peer's notice.
func (s *Shard) Removed() (bool, uint64) { return s.removed, s.leave }

// ConfIndex is an index at which the applied configuration holds: that of the last configuration
// change applied, or the log's base.
func (s *Shard) ConfIndex() uint64 { return s.confAt }

// Propose appends data to the group's log if this replica leads and has not learned its removal, and returns raft.ErrProposalDropped otherwise.
func (s *Shard) Propose(ctx *core.Context, data []byte, out *Output) error {
	if s.removed {
		return raft.ErrProposalDropped
	}
	err := s.rn.Propose(data)
	s.ready(ctx, out)
	return err
}

// Durable reports the round in flight durable: the shard writes it to storage, sends the messages it held, hands over its committed entries and takes the next Ready.
func (s *Shard) Durable(ctx *core.Context, round uint64, out *Output) {
	if s.gone() {
		return
	}
	if round == 0 || round != s.round {
		panic(fmt.Sprintf("raftshard: round %d reported durable while round %d is in flight", round, s.round))
	}
	rd := s.rd
	s.round, s.rd = 0, raft.Ready{}
	if s.saving {
		s.cfg.Storage.removed, s.saving = true, false
	}
	s.done(ctx, &rd, s.hold, out)
	s.ready(ctx, out)
	if s.round == 0 && s.timeoutTerm != 0 {
		m := raftpb.Message{Type: raftpb.MsgTimeoutNow, From: s.timeoutFrom, To: s.cfg.ID, Term: s.timeoutTerm}
		s.timeoutTerm, s.timeoutFrom = 0, 0
		_ = s.rn.Step(m)
		s.ready(ctx, out)
	}
}

// ReadIndex asks the group for the commit index a read must wait for; once a quorum confirms this replica still led when it asked, an Output's Reads carry rctx and that index.
func (s *Shard) ReadIndex(ctx *core.Context, rctx []byte, out *Output) {
	if s.removed {
		return
	}
	s.rn.ReadIndex(rctx)
	s.ready(ctx, out)
}

// Leader is the replica this one last heard lead its current term, or 0, which a NotLeader answer names.
func (s *Shard) Leader() uint64 { return s.rn.BasicStatus().Lead }

// Node is the node that hosts replica id, and false for a replica not in Peers.
func (s *Shard) Node(id uint64) (core.NodeID, bool) {
	if i := s.peer(id); i >= 0 {
		return s.cfg.Peers[i].Node, true
	}
	return core.NodeID{}, false
}

// Term is the replica's current term, which every entry it appends while it leads carries.
func (s *Shard) Term() uint64 { return s.rn.BasicStatus().Term }

// Halt reports that the state machine stopped applying, at an entry it cannot decode. The replica then acts as a down one, so its log stops growing and the control plane replaces it: it takes no Raft message but, while it leads, its followers' answers, campaigns in no election and fortifies no leader, hands its leadership on while it leads, answers an ask about it as gone once it no longer leads, since a follower still supporting it would refuse every other candidate for good, and still learns of its removal.
func (s *Shard) Halt() { s.halted, s.timeoutTerm, s.timeoutFrom = true, 0, 0 }

// Flushing reports whether a round is in flight, so the next Ready waits for its Durable.
func (s *Shard) Flushing() bool { return s.round != 0 }

// LeaseRead reports whether this replica may serve a read now from the entries it handed over to apply, with no round trip: it leads, no MsgTimeoutNow left in its term, an entry of its term has committed, and each half of its configuration has a quorum of voters, itself included, whose fortification of its term runs on the node-pair leases.
func (s *Shard) LeaseRead(ctx *core.Context) bool {
	st := s.rn.BasicStatus()
	if s.plant.followerReads && st.RaftState == raft.StateFollower && !s.removed {
		return s.supports(&st, lease.Now(ctx))
	}
	if s.removed && !s.plant.removedKeepsLease || st.RaftState != raft.StateLeader || s.transferred == st.Term || s.committed != st.Term && !s.plant.uncommitted {
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
		if i := s.peer(id); id == s.cfg.ID || i >= 0 && s.fortifies(i, term, t) {
			n++
		}
	}
	return n > len(voters)/2
}

// fortifies reports whether peer i's fortification of term runs now: the holder's lease from its node runs under the run and support epoch it fortified under.
func (s *Shard) fortifies(i int, term uint64, t lease.Time) bool {
	f := s.fort[i]
	if s.plant.noEpochBump {
		f.support, _ = s.cfg.Holder.Support(s.cfg.Peers[i].Node)
	}
	return f.term == term && s.cfg.Holder.Supported(s.cfg.Peers[i].Node, f.run, f.support, t)
}

// fortify has a leader ask, once a heartbeat, every peer it holds a node-pair lease from whose fortification of its term does not run now, so a down node is not asked; a follower whose grant to its leader's node lapsed stops supporting it and campaigns raft_lapse_campaign later, and one that heard nothing from the leader it supports for an election timeout asks the leader's node whether it still holds the leader.
func (s *Shard) fortify(ctx *core.Context, out *Output) {
	st := s.rn.BasicStatus()
	t := lease.Now(ctx)
	if st.RaftState == raft.StateLeader {
		if ctx.Now < s.fortifyAt {
			return
		}
		s.fortifyAt = ctx.Now + HeartbeatTick*TickInterval
		for i, p := range s.cfg.Peers {
			if p.ID != s.cfg.ID && s.cfg.Holder.Valid(p.Node, t) && !s.fortifies(i, st.Term, t) {
				s.sendFortify(&fortify{kind: kindFortify, from: s.cfg.ID, to: p.ID, term: st.Term}, p.Node, out)
			}
		}
		return
	}
	if ctx.Now-s.heard >= ElectionTick*TickInterval {
		s.heard = ctx.Now
		if s.supports(&st, t) && !s.plant.noSupportAsk {
			s.sendFortify(&fortify{kind: kindAsk, from: s.cfg.ID, to: s.leadID}, s.lead, out)
		}
		for _, p := range s.cfg.Peers {
			if p.ID != s.cfg.ID && !s.removed && member(&s.conf, s.cfg.ID) {
				s.sendFortify(&fortify{kind: kindMember, from: s.cfg.ID, to: p.ID}, p.Node, out)
			}
		}
	}
	if s.support.term != 0 && !s.supports(&st, t) {
		if s.support.term == st.Term {
			s.lapse(ctx)
		}
		s.support = fortified{}
	}
	if s.lapseAt != 0 && ctx.Now >= s.lapseAt {
		s.lapseAt = 0
		if st.RaftState == raft.StateFollower && s.support.term == 0 && !s.halted {
			_ = s.rn.Campaign()
			s.ready(ctx, out)
		}
	}
}

// lapse has this follower campaign raft_lapse_campaign from now, unless it supports a leader again by then.
func (s *Shard) lapse(ctx *core.Context) {
	s.lapseAt = ctx.Now + LapseCampaignMin + core.Tick(s.rng.Uint64N(uint64(LapseCampaignMax-LapseCampaignMin)+1))
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

// stopped reports whether the election timer stands still: a follower ticks no election while it supports a leader or its node waits out its restart, and a halted replica ticks only while it leads.
func (s *Shard) stopped(ctx *core.Context) bool {
	st := s.rn.BasicStatus()
	if s.halted {
		return st.RaftState != raft.StateLeader
	}
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
	s.reportSnapshots(ctx.Now)
	if s.base+TickInterval != s.armed {
		s.arm(out)
	}
}

func (s *Shard) arm(out *Output) {
	s.armed = s.base + TickInterval
	out.Actions = append(out.Actions, core.Action{Kind: core.ActSetTimer, Timer: s.cfg.Timer, At: s.armed})
}

func (s *Shard) recv(ctx *core.Context, ev *core.Event, out *Output) {
	msg := &ev.Msg
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
	case group != s.cfg.Group || to != s.cfg.ID:
		s.stats.Misrouted++
	case kind == kindMember:
		if !member(&s.conf, from) {
			s.sendFortify(&fortify{kind: kindRemoved, from: s.cfg.ID, to: from, term: s.applied}, ev.Peer, out)
		}
	case kind == kindRemoved:
		if !s.removed && f.term >= s.applied && member(&s.conf, s.cfg.ID) {
			s.removed, out.Removed = true, true
		}
	case kind == kindAsk:
		if s.removed || s.halted && s.rn.BasicStatus().RaftState != raft.StateLeader {
			s.sendFortify(&fortify{kind: kindGone, from: f.to, to: f.from}, ev.Peer, out)
		}
	case kind == kindGone:
		if s.support.term != 0 && f.from == s.leadID && ev.Peer == s.lead {
			s.support = fortified{}
			s.lapse(ctx)
		}
	case s.halted && !(kind == kindRaft && (m.Type == raftpb.MsgAppResp || m.Type == raftpb.MsgHeartbeatResp) && s.rn.BasicStatus().RaftState == raft.StateLeader):
	case kind != kindRaft && s.peer(from) < 0:
		s.stats.Misrouted++
	case kind == kindFortify:
		s.fortifyAsked(ctx, &f, out)
	case kind == kindFortified:
		s.fortifyAnswered(&f)
	case (m.Type == raftpb.MsgVote || m.Type == raftpb.MsgPreVote) && s.refuses(ctx, &m):
	// etcd-raft drops a MsgTimeoutNow while a configuration change it knows committed is unapplied,
	// as one is while the round that commits it is in flight, so the transfer would wait out its
	// election timeout with proposals dropped; it is stepped once no round is.
	case m.Type == raftpb.MsgTimeoutNow && s.round != 0:
		s.timeoutTerm, s.timeoutFrom = m.Term, m.From
	case m.Type == raftpb.MsgSnap && s.cfg.CheckSnapshot != nil && (m.Snapshot == nil || !s.cfg.CheckSnapshot(m.Snapshot.Data)):
		s.stats.Undecodable++
	default:
		if errors.Is(s.rn.Step(m), raft.ErrStepPeerNotFound) {
			s.stats.Misrouted++
		} else if m.From == s.rn.BasicStatus().Lead {
			s.heard = ctx.Now
		}
	}
	if err == nil && kind == kindRaft && group == s.cfg.Group && to == s.cfg.ID {
		s.sender(from, ev.Peer, out)
	}
}

// sender learns the node of a member of the applied configuration that sent a Raft message, and
// tells one outside it that it was removed, as it answers a membership question: a partitioned old
// leader asks none, since it still leads its old term.
func (s *Shard) sender(id uint64, node core.NodeID, out *Output) {
	switch {
	case member(&s.conf, id):
		if s.peer(id) < 0 {
			s.cfg.Peers = append(s.cfg.Peers, Peer{ID: id, Node: node})
			s.fort = append(s.fort, fortified{})
		}
	case member(&s.conf, s.cfg.ID):
		s.sendFortify(&fortify{kind: kindRemoved, from: s.cfg.ID, to: id, term: s.applied}, node, out)
	}
}

// answerGone answers, once this replica stopped on learning its removal, an ask that names it.
func (s *Shard) answerGone(ev *core.Event, out *Output) {
	b := ev.Msg.Inline()
	if len(b) != fortBytes || le.Uint64(b[1:]) != s.cfg.Group || le.Uint64(b[kindBytes+8:]) != s.cfg.ID {
		return
	}
	var a core.Action
	if Gone(&ev.Msg, ev.Peer, &a) {
		out.Actions = append(out.Actions, a)
	}
}

// Asked reports the group and replica an ask names, and false for any other message.
func Asked(msg *core.Msg) (group, replica uint64, ok bool) {
	b := msg.Inline()
	if msg.Buf != core.NoBuf || len(b) != fortBytes || b[0] != core.ProtoGroup || b[groupBytes] != kindAsk {
		return 0, 0, false
	}
	return le.Uint64(b[1:]), le.Uint64(b[kindBytes+8:]), true
}

// Gone is a node's answer to an ask about a replica it no longer holds, one that applied its removal or whose state the node deleted, sent back to node from; it reports false for any other message. A node hands Gone only an ask naming a replica it does not hold, never one for another replica of the group.
func Gone(msg *core.Msg, from core.NodeID, a *core.Action) bool {
	if b := msg.Inline(); msg.Buf != core.NoBuf || len(b) != fortBytes || b[groupBytes] != kindAsk {
		return false
	}
	var f fortify
	if group, _, err := decode(nil, msg, nil, &f); err == nil {
		*a = core.Action{Kind: core.ActNetSend, Peer: from, Reply: true}
		encodeFortify(group, &fortify{kind: kindGone, from: f.to, to: f.from}, &a.Msg)
		return true
	}
	return false
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

// ready takes Readys until one must be durable, which becomes the round in flight; no Ready is
// taken while a round is. A round writes the newest hard state when the storage lacks it: etcd-raft
// hands a hard state out once, so a commit index moved by a Ready that is not a round reaches the
// disk with the next round. A Ready that commits a configuration change, or whose messages carry
// a commit index past the entries it hands over, is a round of its own while that commit index is
// not durable, and sends nothing before its flush: a replica that applied the change, or whose
// commit of it reached a peer, never restarts in the configuration before it.
func (s *Shard) ready(ctx *core.Context, out *Output) {
	if s.round == 0 {
		s.reconcile(ctx)
		s.handOff(ctx)
	}
	if s.round == 0 && s.removed && !s.cfg.Storage.Removed() && !s.gone() {
		s.rounds++
		s.round, s.rd, out.Flush, s.hold, s.saving = s.rounds, raft.Ready{HardState: s.hs}, s.rounds, held, true
		out.Round = Round{HardState: s.hs, Removed: true}
		return
	}
	for s.round == 0 && !s.gone() && s.rn.HasReady() {
		rd := s.rn.Ready()
		if rd.SoftState != nil && rd.SoftState.RaftState == raft.StateLeader {
			s.fortifyAt = 0
		}
		for i := range rd.Messages {
			if rd.Messages[i].Type == raftpb.MsgTimeoutNow && !s.plant.transferKeepsLease {
				s.transferred = rd.Messages[i].Term
			}
		}
		if !raft.IsEmptyHardState(rd.HardState) {
			s.hs = rd.HardState
		}
		lags := s.hs != s.cfg.Storage.HardState() && !s.plant.oldHardState
		conf := lags && !s.plant.noConfRound && s.passesConf(rd.CommittedEntries)
		if rd.MustSync || !raft.IsEmptySnap(rd.Snapshot) || conf {
			if lags {
				rd.HardState = s.hs
			}
			s.rounds++
			s.round, s.rd, out.Flush, s.hold, s.saving = s.rounds, rd, s.rounds, held, s.removed
			out.Round = Round{HardState: rd.HardState, Entries: rd.Entries, Snapshot: rd.Snapshot, Removed: s.removed}
			if conf {
				s.hold = all
			} else {
				s.send(ctx, rd.Messages, early, out)
			}
			return
		}
		rd.HardState = raftpb.HardState{}
		s.done(ctx, &rd, all, out)
	}
}

// passesConf reports whether the committed entries ents hold a configuration change, or stop
// short of the newest commit index, which the Ready's messages carry past entries a later Ready
// hands over.
func (s *Shard) passesConf(ents []raftpb.Entry) bool {
	if n := len(ents); n > 0 && ents[n-1].Index < s.hs.Commit {
		return true
	}
	for i := range ents {
		if ents[i].Type != raftpb.EntryNormal {
			return true
		}
	}
	return false
}

// done writes a Ready to storage, sends its messages, applies its configuration changes and hands
// over its committed entries. A committed change that would take this replica out of the
// configuration it holds unapplied, reporting itself removed, and from that change on it hands
// nothing over: its state machine stops before it, so the log compacts no further and a snapshot it
// sends holds the configuration the replica stays a voter of.
func (s *Shard) done(ctx *core.Context, rd *raft.Ready, p part, out *Output) {
	st := s.cfg.Storage
	in := member(&s.conf, s.cfg.ID)
	if !raft.IsEmptySnap(rd.Snapshot) {
		if err := st.ApplySnapshot(rd.Snapshot); err != nil {
			panic(err)
		}
		was := s.conf
		s.conf, s.applied = rd.Snapshot.Metadata.ConfState, rd.Snapshot.Metadata.Index
		if s.cfg.Apply == nil {
			out.Snapshot = rd.Snapshot
		}
		s.prune(&was)
	}
	if !raft.IsEmptyHardState(rd.HardState) {
		st.SetHardState(rd.HardState)
	}
	st.Append(rd.Entries)
	s.send(ctx, rd.Messages, p, out)
	ents := rd.CommittedEntries
	if s.leave != 0 {
		ents = nil
	}
	for i := range ents {
		if e := &ents[i]; e.Type != raftpb.EntryNormal && s.applyConf(e, out) {
			ents = ents[:i]
			break
		}
	}
	if s.cfg.Apply == nil {
		out.Committed = append(out.Committed, ents...)
	} else if !raft.IsEmptySnap(rd.Snapshot) || len(ents) > 0 {
		s.cfg.Apply(rd.Snapshot, ents)
	}
	if n := len(rd.CommittedEntries); n > 0 {
		s.committed = max(s.committed, rd.CommittedEntries[n-1].Term)
	}
	if n := len(ents); n > 0 {
		s.applied = ents[n-1].Index
	}
	out.Reads = append(out.Reads, rd.ReadStates...)
	s.rn.Advance(*rd)
	if in && !member(&s.conf, s.cfg.ID) && !s.removed {
		s.removed, out.Removed = true, true
	}
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
		if s.plant.voteEveryone && m.Type == raftpb.MsgVoteResp {
			m.Reject = false
		}
		e := s.plant.early || leavesEarly(m.Type)
		if p == early && !e || p == held && e {
			continue
		}
		var a core.Action
		sent := false
		if i := s.peer(m.To); i >= 0 {
			a = core.Action{Kind: core.ActNetSend, Peer: s.cfg.Peers[i].Node, Reply: true}
			sent = encode(ctx.Pool, s.cfg.Group, m, &a.Msg)
		}
		if sent {
			out.Actions = append(out.Actions, a)
		} else {
			s.stats.Unsent++
		}
		if m.Type == raftpb.MsgSnap && sent {
			s.snaps = append(s.snaps, sentSnap{to: m.To, at: ctx.Now})
		} else if m.Type == raftpb.MsgSnap {
			s.rn.ReportSnapshot(m.To, raft.SnapshotFailure)
		}
	}
}

// reportSnapshots reports each snapshot sent a heartbeat ago as finished, as it went whole in one
// message, so the leader probes the follower again rather than wait in StateSnapshot for an answer
// that, once its log compacted past the snapshot, never moves it on. A follower that did not
// restore it refuses the probe and is sent another, at most one a heartbeat.
func (s *Shard) reportSnapshots(now core.Tick) {
	k := 0
	for _, sn := range s.snaps {
		if now-sn.at < HeartbeatTick*TickInterval {
			s.snaps[k] = sn
			k++
			continue
		}
		s.rn.ReportSnapshot(sn.to, raft.SnapshotFinish)
	}
	s.snaps = s.snaps[:k]
}

// sentSnap is a snapshot sent to replica to at a time.
type sentSnap struct {
	to uint64
	at core.Tick
}

// applyConf applies a committed configuration change: a replica it adds joins Peers at the node its
// context names, and one it takes out of the configuration leaves them. It reports true, applying
// nothing, for a change that would take this replica out: the replica stays a voter of the
// configuration it holds, joint for a swap's leave, since a voter that holds the leave but applied
// only the joint configuration can lead and needs the outgoing half to commit. A leave of a
// configuration that is not joint changes nothing: a replica holding its own removal, leading and
// still joint, proposes the leave again each time it applies the last one, and etcd-raft panics on
// leaving a configuration that is not joint.
func (s *Shard) applyConf(e *raftpb.Entry, out *Output) bool {
	var cc raftpb.ConfChangeV2
	if e.Type != raftpb.EntryConfChangeV2 || cc.Unmarshal(e.Data) != nil {
		panic(fmt.Sprintf("raftshard: group %d committed configuration change %d/%d, which no replica proposes", s.cfg.Group, e.Index, e.Term))
	}
	joint := len(s.conf.VotersOutgoing) != 0
	if len(cc.Changes) == 0 && !joint {
		return false
	}
	if s.removes(&cc, joint) && !s.plant.deletesAtOnce && !s.plant.appliesRemoval {
		s.leave, s.confTerm = e.Index, 0
		if !s.removed {
			s.removed, out.Removed = true, true
		}
		return true
	}
	was := s.conf
	s.applying = true
	s.conf = *s.rn.ApplyConfChange(cc)
	s.applying = false
	s.confTerm, s.confAt = 0, e.Index
	if node, ok := decodeNode(cc.Context); ok {
		for _, c := range cc.Changes {
			if c.Type != raftpb.ConfChangeRemoveNode && s.peer(c.NodeID) < 0 {
				s.cfg.Peers = append(s.cfg.Peers, Peer{ID: c.NodeID, Node: node})
				s.fort = append(s.fort, fortified{})
			}
		}
	}
	s.prune(&was)
	return false
}

// removes reports whether cc would take this replica out of the configuration: a leave of the
// joint configuration whose incoming half lacks it, or a change outside a joint one that removes it.
func (s *Shard) removes(cc *raftpb.ConfChangeV2, joint bool) bool {
	id := s.cfg.ID
	if len(cc.Changes) == 0 {
		return joint && !slices.Contains(s.conf.Voters, id) && !slices.Contains(s.conf.Learners, id) && !slices.Contains(s.conf.LearnersNext, id)
	}
	if _, enters := cc.EnterJoint(); enters {
		return false
	}
	return slices.ContainsFunc(cc.Changes, func(c raftpb.ConfChangeSingle) bool {
		return c.Type == raftpb.ConfChangeRemoveNode && c.NodeID == id
	})
}

// prune drops from Peers each replica that was holds and the applied configuration does not, but
// the leader, kept until the next change applies: a removed leader holds its leave unapplied and
// re-proposes it, so it hands its leadership on only once the target acknowledged that entry, and
// steps down only once a follower answers its heartbeat with the next leader's term, since that
// leader's configuration lacks it.
func (s *Shard) prune(was *raftpb.ConfState) {
	lead, kept := s.rn.BasicStatus().Lead, s.kept
	s.kept = 0
	for i := len(s.cfg.Peers) - 1; i >= 0; i-- {
		switch id := s.cfg.Peers[i].ID; {
		case id == s.cfg.ID || member(&s.conf, id):
		case id == lead && member(was, id):
			s.kept = id
		case id == kept || member(was, id):
			s.cfg.Peers = slices.Delete(s.cfg.Peers, i, i+1)
			s.fort = slices.Delete(s.fort, i, i+1)
		}
	}
}

// member reports whether cs holds replica id, as a voter or a learner, in either half.
func member(cs *raftpb.ConfState, id uint64) bool {
	return slices.Contains(cs.Voters, id) || slices.Contains(cs.VotersOutgoing, id) || slices.Contains(cs.Learners, id) || slices.Contains(cs.LearnersNext, id)
}

// reconcile has a leader that applied an entry of its term, with no configuration change in flight
// and none joint, propose the next change toward its plan. A leader never removes itself: one the
// plan drops hands its leadership to a planned voter once one is ready, and until a planned voter
// exists it adds and swaps in the planned replicas for the other dropped voters. A leader that
// learned its removal proposes nothing and, once an entry of its term committed, hands its
// leadership to a caught-up planned voter, joint or not: it holds the leave unapplied, so it stays
// joint. A learner counts as caught up once it
// holds every entry committed by mark, the commit index of at most a heartbeat ago: one held to
// the current commit index never catches up under steady writes over a slower link.
func (s *Shard) reconcile(ctx *core.Context) {
	now := ctx.Now
	if len(s.plan) == 0 && s.leave == 0 || s.settled() {
		return
	}
	st := s.rn.BasicStatus()
	if now >= s.markAt {
		s.mark, s.markAt = st.Commit, now+HeartbeatTick*TickInterval
	}
	if st.RaftState != raft.StateLeader || s.committed != st.Term {
		return
	}
	if s.removed {
		if id := s.target(lease.Now(ctx)); id != 0 {
			s.rn.TransferLeader(id)
		}
		return
	}
	if s.confTerm == st.Term || len(s.conf.VotersOutgoing) != 0 {
		return
	}
	if !s.planned(s.cfg.ID) && slices.ContainsFunc(s.conf.Voters, s.planned) {
		if id := s.target(lease.Now(ctx)); id != 0 {
			s.rn.TransferLeader(id)
		}
		return
	}
	caughtUp := s.mark
	if s.plant.current {
		caughtUp = st.Commit
	}
	if cc, ok := s.change(caughtUp); ok && s.rn.ProposeConfChange(cc) == nil {
		s.confTerm = st.Term
	}
}

// target is a planned voter to hand leadership to, or 0: one this leader holds a node-pair lease
// from, so its node runs, being sent appends and caught up as a learner must be. etcd-raft drops
// proposals while a transfer waits for its target to reach the leader's last index, so a down or
// lagging target would stall them; a caught-up one makes that wait about a round trip. A replica
// holding its own leave also takes a voter of the leave's configuration, the joint one's incoming
// half, as planned, since its node may hand it no plan.
func (s *Shard) target(t lease.Time) uint64 {
	var to uint64
	s.rn.WithProgress(func(id uint64, _ raft.ProgressType, pr tracker.Progress) {
		planned := s.planned(id) || s.leave != 0 && slices.Contains(s.conf.Voters, id)
		if to == 0 && planned && s.replicating(id, &pr) && s.cfg.Holder.Valid(s.cfg.Peers[s.peer(id)].Node, t) {
			to = id
		}
	})
	return to
}

// replicating reports whether peer id is a voter this leader sends appends to, holding every entry committed by mark.
func (s *Shard) replicating(id uint64, pr *tracker.Progress) bool {
	return id != s.cfg.ID && s.peer(id) >= 0 && !pr.IsLearner && pr.State == tracker.StateReplicate && pr.Match >= s.mark
}

// handOff has a halted leader with no transfer pending hand its leadership to the next replicating
// voter in ID order, at most once an election timeout: when every replica halts, each would
// otherwise hand it on as soon as it wins, a term and a durable hard state per round trip.
func (s *Shard) handOff(ctx *core.Context) {
	if !s.halted || s.gone() || ctx.Now < s.handAt {
		return
	}
	if st := s.rn.BasicStatus(); st.RaftState != raft.StateLeader || st.LeadTransferee != 0 {
		return
	}
	s.handAt = ctx.Now + ElectionTick*TickInterval
	var first, next uint64
	s.rn.WithProgress(func(id uint64, _ raft.ProgressType, pr tracker.Progress) {
		if !s.replicating(id, &pr) {
			return
		}
		if first == 0 {
			first = id
		}
		if next == 0 && id > s.handedTo {
			next = id
		}
	})
	if next == 0 {
		next = first
	}
	if next != 0 {
		s.handedTo = next
		s.rn.TransferLeader(next)
	}
}

// settled reports whether the applied configuration is the plan: its voters, no learner, not joint.
// Most groups are, so a Ready costs them a few comparisons.
func (s *Shard) settled() bool {
	if len(s.conf.Learners)+len(s.conf.VotersOutgoing) != 0 || len(s.conf.Voters) != len(s.plan) {
		return false
	}
	return !slices.ContainsFunc(s.conf.Voters, func(id uint64) bool { return !s.planned(id) })
}

func (s *Shard) planned(id uint64) bool {
	return slices.ContainsFunc(s.plan, func(p Peer) bool { return p.ID == id })
}

// change is the next configuration change toward the plan: remove a learner the plan dropped; add
// a planned replica the configuration lacks as a learner; swap a learner holding every entry up
// to commit for a voter the plan dropped, or promote it if none; remove a voter the plan
// dropped once every planned replica votes.
func (s *Shard) change(commit uint64) (raftpb.ConfChangeV2, bool) {
	planned := s.planned
	one := func(t raftpb.ConfChangeType, id uint64) raftpb.ConfChangeV2 {
		return raftpb.ConfChangeV2{Changes: []raftpb.ConfChangeSingle{{Type: t, NodeID: id}}}
	}
	for _, id := range s.conf.Learners {
		if !planned(id) {
			return one(raftpb.ConfChangeRemoveNode, id), true
		}
	}
	for _, p := range s.plan {
		if !member(&s.conf, p.ID) {
			t := raftpb.ConfChangeAddLearnerNode
			if s.plant.voterFirst {
				t = raftpb.ConfChangeAddNode
			}
			cc := one(t, p.ID)
			cc.Context = encodeNode(p.Node)
			return cc, true
		}
	}
	dropped := slices.IndexFunc(s.conf.Voters, func(id uint64) bool { return id != s.cfg.ID && !planned(id) })
	var ready uint64
	s.rn.WithProgress(func(id uint64, _ raft.ProgressType, pr tracker.Progress) {
		if ready == 0 && pr.IsLearner && (pr.Match >= commit || s.plant.eager) {
			ready = id
		}
	})
	switch {
	case ready != 0 && dropped >= 0:
		cc := one(raftpb.ConfChangeRemoveNode, s.conf.Voters[dropped])
		cc.Changes = append(cc.Changes, raftpb.ConfChangeSingle{Type: raftpb.ConfChangeAddNode, NodeID: ready})
		return cc, true
	case ready != 0:
		return one(raftpb.ConfChangeAddNode, ready), true
	case dropped >= 0 && len(s.conf.Learners) == 0:
		return one(raftpb.ConfChangeRemoveNode, s.conf.Voters[dropped]), true
	}
	return raftpb.ConfChangeV2{}, false
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
