package raftshard

import (
	"slices"
	"testing"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/lease"
)

// jointConf is the joint configuration that swaps D in for C; leaveAt is the index of the leave
// that the first leader of a group booted in it proposes, after its empty entry.
var jointConf = raftpb.ConfState{Voters: []uint64{1, 2, 4}, VotersOutgoing: []uint64{1, 2, 3}, AutoLeave: true}

const leaveAt = 3

// jointNet is four replicas in jointConf, none leading, C running with p planted.
func jointNet(t *testing.T, p plants) *testNet {
	n := newConfNet(t, 4, 1, func(int) *Storage { return confStorage(jointConf) })
	if p != (plants{}) {
		r := n.r[rC]
		cfg := testConfig(4, rC, r.st, n.seed)
		cfg.Holder, cfg.Grantor = r.holder, r.grantor
		s, err := newShard(cfg, p)
		if err != nil {
			t.Fatal(err)
		}
		r.s = s
		n.call(rC, func(ctx *core.Context, out *Output) { s.Start(ctx, out) })
	}
	return n
}

func (n *testNet) campaign(i int) {
	n.call(i, func(ctx *core.Context, out *Output) {
		if err := n.r[i].s.rn.Campaign(); err != nil {
			n.t.Fatal(err)
		}
		n.r[i].s.ready(ctx, out)
	})
}

// renewAmong has every node of live renew its lease from every other one of live.
func (n *testNet) renewAmong(live ...int) {
	for _, h := range live {
		for _, g := range live {
			if g != h {
				n.renew(h, g)
			}
		}
	}
}

// leadsPlan reports whether one of who leads, not halted, having applied the plan's configuration.
func (n *testNet) leadsPlan(who ...int) bool {
	l := n.leaderOf(who...)
	if l < 0 || n.r[l].s.halted {
		return false
	}
	c := n.conf(l)
	return len(c.VotersOutgoing) == 0 && slices.Equal(sorted(c.Voters), []uint64{1, 2, 4})
}

// commitBelow keeps a message unless it is a Raft message from replica from carrying a commit index at or past leaveAt.
func (n *testNet) commitBelow(e envelope, from int) bool {
	if e.from != from {
		return true
	}
	m, raft := n.message(e)
	return !raft || m.Commit < leaveAt
}

// toldC is the voter model's ci-halt-live wedge: A leads the joint configuration, commits the
// leave with B and tells C alone, and then halts or dies. It reports how long after that one of B
// and D led having applied the plan's configuration, or -1 if none did within bound.
func toldC(t *testing.T, p plants, die bool, bound core.Tick) (core.Tick, bool) {
	n := jointNet(t, p)
	n.renewAll()
	n.campaign(rA)
	told := func(e envelope) bool { return e.to == rC || n.commitBelow(e, rA) }
	n.settleKeeping(told)
	if c := n.conf(rA); len(c.VotersOutgoing) != 0 || !n.r[rC].removed {
		t.Fatalf("A is in %+v and C learned its removal %v, want the leave applied and learned", c, n.r[rC].removed)
	}
	for _, i := range []int{rB, rD} {
		if c := n.conf(i); len(c.VotersOutgoing) == 0 {
			t.Fatalf("replica %d left the joint configuration: %+v", i, c)
		}
	}
	live, keep, cut := []int{rA, rB, rC, rD}, told, []int(nil)
	if die {
		live, cut = []int{rB, rC, rD}, []int{rA}
		keep = func(e envelope) bool { return e.from != rA && e.to != rA }
	} else {
		n.r[rA].s.Halt()
	}
	start := n.now
	for n.now-start <= bound {
		n.renewAmong(live...)
		n.advance(HeartbeatTick*TickInterval, live...)
		n.settleKeeping(keep, cut...)
		if n.leadsPlan(rB, rD) {
			return n.now - start, true
		}
	}
	return 0, false
}

// A leader of the joint configuration commits the leave with B, tells C alone, and halts or dies:
// B or D, holding the leave with only the joint configuration applied, leads with C's vote, the
// outgoing half's last one up, and applies the leave. A halted leader hands its leadership on at
// once, so it takes an election timeout and a round trip; a dead one's followers campaign once
// their grants to its node lapse. Planted, a C that stops once it learns its removal, as if its
// node deleted it, leaves the outgoing half one voter, and no leader of the plan's
// configuration follows (the voter model's neg-removed-deletes-at-once).
func TestARemovedVoterKeepsTheOutgoingHalfsVoteForAVoterThatHoldsTheLeave(t *testing.T) {
	rt := 3 * HeartbeatTick * TickInterval
	for _, c := range []struct {
		name  string
		die   bool
		bound core.Tick
	}{
		{"halt", false, ElectionTick*TickInterval + rt},
		{"death", true, lease.DefaultConfig.Lease + lease.DefaultConfig.Heartbeat + LapseCampaignMax + ElectionTick*TickInterval + rt},
	} {
		if _, ok := toldC(t, plants{}, c.die, c.bound); !ok {
			t.Errorf("%s: no voter of the plan led within %v", c.name, c.bound.Duration())
		}
		if took, ok := toldC(t, plants{deletesAtOnce: true}, c.die, 10*c.bound); ok {
			t.Errorf("%s, C deleted at once: a voter of the plan led after %v", c.name, took.Duration())
		}
	}
}

// leaveBehind is the voter model's ci-leave-die-live wedge: C, leading from the outgoing half,
// commits the leave on B and D while A lags, and neither learns it committed; B wins the next
// term, appends its entry to C alone and dies. It reports whether one of A and D led having
// applied the plan's configuration within bound, and whether C led.
func leaveBehind(t *testing.T, p plants, bound core.Tick) (led bool, cLed bool) {
	n := jointNet(t, p)
	n.campaign(rC)
	cutA := func(e envelope) bool { return e.from != rA && e.to != rA }
	n.settleKeeping(func(e envelope) bool {
		m, raft := n.message(e)
		return cutA(e) && (e.from != rB || !raft || m.Type != raftpb.MsgAppResp || m.Index < leaveAt)
	})
	n.recv(rC, rB, raftpb.Message{Type: raftpb.MsgAppResp, From: 2, To: 3, Term: n.term(rC), Index: leaveAt})
	if n.leaderOf(rC) < 0 || n.r[rC].flush == 0 {
		t.Fatalf("C leads %v, its round in flight %d: want it holding the round that commits the leave", n.leaderOf(rC) >= 0, n.r[rC].flush)
	}
	fromB := func(e envelope) bool {
		m, raft := n.message(e)
		return e.from != rB || e.to == rC || !raft || m.Type != raftpb.MsgApp
	}
	n.campaign(rB)
	n.settleKeeping(func(e envelope) bool { return fromB(e) && n.commitBelow(e, rC) }, rC)
	n.durable(rC)
	n.settleKeeping(func(e envelope) bool { return fromB(e) && n.commitBelow(e, rC) })
	if n.leaderOf(rB) < 0 || !n.r[rC].removed {
		t.Fatalf("B leads %v and C learned its removal %v, want both", n.leaderOf(rB) >= 0, n.r[rC].removed)
	}
	if lc, _ := n.r[rC].st.LastIndex(); lc != leaveAt+1 || n.conf(rD).VotersOutgoing == nil {
		t.Fatalf("C holds up to %d, D is in %+v: want C holding B's entry and D still joint", lc, n.conf(rD))
	}
	live := []int{rA, rC, rD}
	for _, h := range live {
		n.leases(h, slices.DeleteFunc(slices.Clone(live), func(g int) bool { return g == h })...)
	}
	withoutB := func(e envelope) bool {
		if m, raft := n.message(e); e.from == rC && raft && (m.Type == raftpb.MsgApp || m.Type == raftpb.MsgHeartbeat) {
			cLed = true
		}
		return e.from != rB && e.to != rB
	}
	start := n.now
	for n.now-start <= bound {
		n.renewAmong(live...)
		n.advance(HeartbeatTick*TickInterval, live...)
		n.settleKeeping(withoutB, rB)
		n.r[rB].flush = 0
		if n.leadsPlan(rA, rD) {
			return true, cLed
		}
	}
	return false, cLed
}

// The voter model's neg-removed-applies-removal trace: C leads term 2 from the outgoing half and
// commits the leave on B and D while A lags; B wins term 3 before applying it, appends its entry
// to C alone and dies. C, holding the newest entry of the outgoing half, wins, commits the leave on
// A and D and hands its leadership to one of them. Planted, a C that applies its removal campaigns
// no more, and A and D, which applied only the joint configuration, never win its vote. A halted
// B wedges nothing: it still votes, and hands its leadership on.
func TestARemovedVoterThatHoldsTheOutgoingHalfsNewestEntryLeadsAndHandsOff(t *testing.T) {
	bound := 4 * ElectionTick * TickInterval
	if led, cLed := leaveBehind(t, plants{}, bound); !led || !cLed {
		t.Fatalf("within %v C led %v, and a voter of the plan %v", bound.Duration(), cLed, led)
	}
	if led, cLed := leaveBehind(t, plants{appliesRemoval: true}, 10*bound); led || cLed {
		t.Fatalf("C applying its removal: C led %v, a voter of the plan %v", cLed, led)
	}
}

// C leads the joint configuration and commits its removal, and no node holds a lease from another,
// so C has no planned voter to hand its leadership to. Its followers acknowledge every entry, and
// etcd-raft proposes the leave again each time C applies the last one: C takes each commit at
// most once a heartbeat, so its log grows by about an entry a heartbeat, not one a round trip.
func TestARemovedLeaderWithNoTargetProposesItsLeaveAtMostOnceAHeartbeat(t *testing.T) {
	n := jointNet(t, plants{})
	every := func(envelope) bool { return true }
	n.campaign(rC)
	n.settleKeeping(every)
	if !n.r[rC].removed || n.leaderOf(rC) < 0 {
		t.Fatalf("C learned its removal %v and leads %v, want both", n.r[rC].removed, n.leaderOf(rC) >= 0)
	}
	first, _ := n.r[rC].st.LastIndex()
	const beats = 20
	for range beats {
		n.advance(HeartbeatTick * TickInterval)
		n.settleKeeping(every)
	}
	if last, _ := n.r[rC].st.LastIndex(); n.leaderOf(rC) < 0 || last-first > beats {
		t.Fatalf("over %d heartbeats C, leading %v, wrote %d entries", beats, n.leaderOf(rC) >= 0, last-first)
	}
}

// outgoingNet is the voter model's ci-live-moved start: four replicas in jointConf, every node
// holding a lease from every other; C, in the outgoing half alone, wins an election and commits
// the leave, which it holds unapplied, so it stays joint and leads on. keep filters every message.
func outgoingNet(t *testing.T, p plants, keep func(n *testNet, e envelope) bool) *testNet {
	n := jointNet(t, p)
	n.renewAll()
	n.campaign(rC)
	n.settleKeeping(func(e envelope) bool { return keep(n, e) })
	if !n.r[rC].removed || len(n.conf(rC).VotersOutgoing) == 0 {
		t.Fatalf("C learned its removal %v and is in %+v, want it holding the leave in the joint configuration", n.r[rC].removed, n.conf(rC))
	}
	return n
}

// A leader from the outgoing half that committed its removal keeps leading the joint
// configuration and hands its leadership to a caught-up voter of the plan, which applies the leave.
func TestALeaderThatCommittedItsOwnRemovalHandsItsLeadershipOn(t *testing.T) {
	n := outgoingNet(t, plants{}, func(*testNet, envelope) bool { return true })
	if n.leaderOf(rC) >= 0 || !n.leadsPlan(rA, rB, rD) {
		t.Fatalf("C still leads %v; a voter of the plan leads %v", n.leaderOf(rC) >= 0, n.leadsPlan(rA, rB, rD))
	}
	if r, at := n.r[rC].s.Removed(); !r || at != leaveAt || n.r[rC].s.applied != leaveAt-1 {
		t.Fatalf("C removed %v at %d, applied %d: want it stopped before the leave at %d", r, at, n.r[rC].s.applied, leaveAt)
	}
}

// lostTimeoutNow keeps every message but C's MsgTimeoutNow, so C cannot hand its leadership on.
func lostTimeoutNow(n *testNet, e envelope) bool {
	m, raft := n.message(e)
	return e.from != rC || !raft || m.Type != raftpb.MsgTimeoutNow
}

// C, which committed its removal leading from the outgoing half, cannot hand its leadership on;
// every voter of the plan applied the leave, so the plan is complete and C's node deletes C while
// it leads. That node keeps renewing the followers' grants, so they would support C for good:
// within an election timeout each asks C's node about replica 3, which answers that it is gone,
// and they campaign after raft_lapse_campaign. A leader of the plan's configuration follows within
// an election timeout, a heartbeat, raft_lapse_campaign and three heartbeats, and no two replicas
// could serve a lease read at once.
func TestFollowersOfALeaderThatAppliedItsOwnRemovalCampaign(t *testing.T) {
	n := outgoingNet(t, plants{}, lostTimeoutNow)
	for _, i := range []int{rA, rB, rD} {
		if c := n.conf(i); len(c.VotersOutgoing) != 0 || n.r[i].s.support.term != n.term(rC) {
			t.Fatalf("replica %d is in %+v supporting term %d, want the plan's configuration supporting C's term %d", i, c, n.r[i].s.support.term, n.term(rC))
		}
	}
	if n.leaderOf(rC) < 0 {
		t.Fatal("C handed its leadership on with its MsgTimeoutNow lost")
	}
	deleted := func(e envelope) bool {
		if e.to != rC {
			return true
		}
		var a core.Action
		if _, _, ok := Asked(&e.msg); ok && Gone(&e.msg, core.NodeID{Node: uint32(e.from)}, &a) {
			n.inbox = append(n.inbox, envelope{from: rC, to: e.from, msg: a.Msg})
		}
		if e.msg.Buf != core.NoBuf {
			n.r[rC].pool.Free(e.msg.Buf)
		}
		return false
	}
	start := n.now
	bound := ElectionTick*TickInterval + HeartbeatTick*TickInterval + LapseCampaignMax + 3*HeartbeatTick*TickInterval
	for {
		if n.now-start > bound {
			t.Fatalf("no voter of the plan led within %v of C's deletion", bound.Duration())
		}
		n.renewAll()
		n.advance(HeartbeatTick*TickInterval, rA, rB, rD)
		n.settleKeeping(deleted, rC)
		readers := 0
		for _, i := range []int{rA, rB, rD} {
			if n.leaseRead(i) {
				readers++
			}
		}
		if readers > 1 {
			t.Fatalf("%d replicas may serve a lease read", readers)
		}
		if n.leadsPlan(rA, rB, rD) {
			return
		}
	}
}

// cutC keeps every message but C's Raft messages and those to C, as a link that carries only the
// nodes' lease and fortification frames.
func cutC(n *testNet, e envelope) bool {
	if e.from != rC && e.to != rC {
		return true
	}
	_, raft := n.message(e)
	return !raft
}

// unacked keeps every message but the followers' acknowledgements to C past the leave: C
// re-proposes the leave it holds, so its transfer target never catches up and it sends no
// MsgTimeoutNow, which would end its lease.
func unacked(n *testNet, e envelope) bool {
	m, raft := n.message(e)
	return e.to != rC || !raft || m.Type != raftpb.MsgAppResp || m.Index <= leaveAt
}

// The voter model's neg-removed-keeps-lease: C, leading from the outgoing half, holds its
// removal and is then cut off from the others' Raft messages while its node renews every grant.
// Its followers ask its node, hear that it is gone and elect a leader, which serves lease reads;
// C, which learned its removal, serves none. Planted, it keeps serving them, at once with the new
// leader.
func TestARemovedLeaderThatKeepsReadingBreaksTheLeaseInvariant(t *testing.T) {
	for _, p := range []plants{{}, {removedKeepsLease: true}} {
		n := outgoingNet(t, p, unacked)
		both := false
		for k := 0; k < 30 && !both; k++ {
			n.renewAll()
			n.advance(HeartbeatTick * TickInterval)
			n.settleKeeping(func(e envelope) bool { return cutC(n, e) })
			both = n.leaseRead(rC) && slices.ContainsFunc([]int{rA, rB, rD}, n.leaseRead)
		}
		if both != p.removedKeepsLease {
			t.Fatalf("planted %v: C and another replica could serve lease reads at once: %v", p.removedKeepsLease, both)
		}
	}
}

// A leader that applied the leave and crashes restarts with it committed, in the plan's configuration.
func TestALeaderThatCrashesAfterCommittingTheLeaveRestartsWithItCommitted(t *testing.T) {
	n := jointNet(t, plants{})
	n.campaign(rA)
	n.settle()
	if c := n.conf(rA); len(c.VotersOutgoing) != 0 {
		t.Fatalf("A is in %+v, want the leave applied", c)
	}
	n.crash(rA)
	n.call(rA, func(ctx *core.Context, out *Output) { n.r[rA].s.ready(ctx, out) })
	n.settleWithout(rB, rC, rD)
	if hs := n.r[rA].st.HardState(); hs.Commit < leaveAt {
		t.Fatalf("A restarted at commit index %d, below the leave at %d", hs.Commit, leaveAt)
	}
	if c := n.conf(rA); len(c.VotersOutgoing) != 0 || !slices.Equal(sorted(c.Voters), []uint64{1, 2, 4}) {
		t.Fatalf("A restarted in %+v", c)
	}
}

// A replica that learns its removal from a peer's notice keeps it across a restart: it writes a
// round recording it, which a restart from its disk log replays.
func TestARemovalLearnedFromANoticeSurvivesARestart(t *testing.T) {
	n := newNet(t, 3, 1)
	n.elect(0)
	ev := core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{Node: 0}}
	encodeFortify(testGroup, &fortify{kind: kindRemoved, from: 1, to: 2, term: n.r[1].s.applied}, &ev.Msg)
	n.event(1, ev)
	if !n.r[1].out.Removed || n.r[1].flush == 0 || !n.r[1].out.Round.Removed {
		t.Fatalf("the notice reported removed %v and took round %d recording it %v", n.r[1].out.Removed, n.r[1].flush, n.r[1].out.Round.Removed)
	}
	n.durable(1)
	n.crash(1)
	if removed, _ := n.r[1].s.Removed(); !removed {
		t.Fatal("B forgot its removal across a restart")
	}
	if n.leaseRead(1) || n.r[1].s.Propose(&n.r[1].ctx, []byte("x"), &n.r[1].out) != raft.ErrProposalDropped {
		t.Fatal("B, removed, serves reads or takes proposals")
	}
}
