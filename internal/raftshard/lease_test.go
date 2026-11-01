package raftshard

import (
	"testing"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/lease"
)

// leaseLeft is primary_lease less lease_drift: how long after its ask a holder's lease runs at a rate bound of 1.
var leaseLeft = lease.DefaultConfig.Lease * core.Tick(lease.PPM-lease.DefaultConfig.Drift) / lease.PPM

func (n *testNet) leaseRead(i int) bool {
	ctx := n.ctx(i)
	return n.r[i].s.LeaseRead(&ctx)
}

func (n *testNet) term(i int) uint64 { return n.r[i].s.rn.BasicStatus().Term }

// challenge is a (pre-)vote request from replica from to replica to for the term after to's, with to's log.
func (n *testNet) challenge(t raftpb.MessageType, from, to int, context string) raftpb.Message {
	last, _ := n.r[to].st.LastIndex()
	term := n.term(to)
	return raftpb.Message{Type: t, From: uint64(from) + 1, To: uint64(to) + 1, Term: term + 1, Index: last, LogTerm: term, Context: []byte(context)}
}

// typeOf is the Raft type of a queued message, or false for a fortification message.
func (n *testNet) typeOf(e envelope) (raftpb.MessageType, bool) {
	var m raftpb.Message
	kind, err := decodeKind(n.r[e.to].pool, &e.msg, &m, &fortify{})
	if err != nil {
		n.t.Fatal(err)
	}
	return m.Type, kind == kindRaft
}

// lapse steps every replica 10 ms at a time, delivering nothing, until follower f stops supporting its leader; it returns when it did.
func (n *testNet) lapse(f int, renew func()) core.Tick {
	for range 1000 {
		n.advance(TickInterval)
		n.inbox = n.inbox[:0]
		if renew != nil {
			renew()
		}
		if n.r[f].s.support.term == 0 {
			return n.now
		}
	}
	n.t.Fatalf("follower %d still supports its leader after 10 s", f)
	return 0
}

func TestASupportingFollowerDropsChallengersButPassesATransfer(t *testing.T) {
	n := newNet(t, 3, 1)
	n.leases(0, 1, 2)
	n.elect(0)
	f := n.r[1]
	if f.s.support.term != n.term(0) {
		t.Fatalf("follower supports term %d, leader is in %d", f.s.support.term, n.term(0))
	}
	term := n.term(1)
	for _, typ := range []raftpb.MessageType{raftpb.MsgPreVote, raftpb.MsgVote} {
		if sent := n.recv(1, 2, n.challenge(typ, 2, 1, "")); len(sent) != 0 || f.flush != 0 || n.term(1) != term {
			t.Fatalf("%v from a challenger: sent %v, round %d, term %d", typ, types(sent), f.flush, n.term(1))
		}
	}
	n.recv(1, 2, n.challenge(raftpb.MsgVote, 2, 1, transferContext))
	if sent := n.durable(1); count(sent, raftpb.MsgVoteResp) != 1 || sent[0].Reject {
		t.Fatalf("a transfer target's vote got %v", types(sent))
	}

	n = newNet(t, 3, 1)
	n.leases(0, 1, 2)
	n.elect(0)
	n.lapse(1, nil)
	if sent := n.recv(1, 2, n.challenge(raftpb.MsgPreVote, 2, 1, "")); count(sent, raftpb.MsgPreVoteResp) != 1 || sent[0].Reject {
		t.Fatalf("a pre-vote after the grant lapsed got %v", types(sent))
	}
	n.recv(1, 2, n.challenge(raftpb.MsgVote, 2, 1, ""))
	if sent := n.durable(1); count(sent, raftpb.MsgVoteResp) != 1 || sent[0].Reject {
		t.Fatalf("a vote after the grant lapsed got %v", types(sent))
	}
}

// A vote request from the supported leader itself, for a later term, shows that it no longer leads,
// as after it stepped down while its node's grant held, or after its node restarted and a grantor
// renewed the old grant, on a slowed clock or at a late ask of the old run: the follower stops
// supporting it and passes the request.
func TestTheSupportedLeadersOwnCampaignEndsSupport(t *testing.T) {
	n := newNet(t, 3, 1)
	n.leases(0, 1, 2)
	n.elect(0)
	f := n.r[1]
	if sent := n.recv(1, 0, n.challenge(raftpb.MsgPreVote, 0, 1, "")); count(sent, raftpb.MsgPreVoteResp) != 1 || sent[0].Reject {
		t.Fatalf("the leader's own pre-vote got %v", types(sent))
	}
	if f.s.support.term != 0 {
		t.Fatal("still supports the leader that campaigned")
	}
}

// A follower whose grant lapses campaigns raft_lapse_campaign after it sees the lapse, not at its
// election timeout, and its election timer stands still before: 4 s without the leader's
// heartbeats is past any election timeout.
func TestALapsedFollowerCampaignsWithin300ms(t *testing.T) {
	seen := map[core.Tick]bool{}
	for seed := range uint64(20) {
		n := newNet(t, 3, seed)
		n.leases(0, 1, 2)
		granted := n.now
		n.elect(0)
		lapsed := n.lapse(1, nil)
		if lapsed-granted < lease.DefaultConfig.Lease {
			t.Fatalf("seed %d: the follower stopped supporting %v after the grant", seed, (lapsed - granted).Duration())
		}
		for {
			n.now += 1_000
			sent := n.event(1, core.Event{Kind: core.EvTimer, Timer: 1})
			if count(sent, raftpb.MsgPreVote) == 0 {
				continue
			}
			d := n.now - lapsed
			if d < LapseCampaignMin || d > LapseCampaignMax+1_000 {
				t.Fatalf("seed %d: campaigned %v after the lapse", seed, d.Duration())
			}
			seen[d] = true
			break
		}
	}
	if len(seen) < 10 {
		t.Fatalf("20 seeds campaigned at only %d distinct delays", len(seen))
	}
}

// A follower whose pre-vote fails, because the other follower's grant still holds, campaigns again at its election timeout.
func TestAFailedLapseCampaignRetriesAtTheElectionTimeout(t *testing.T) {
	n := newNet(t, 3, 1)
	n.leases(0, 1, 2)
	n.elect(0)
	lapsed := n.lapse(1, func() {
		if n.now%lease.DefaultConfig.Heartbeat == 0 {
			n.renew(0, 2)
		}
	})
	var campaigns []core.Tick
	for len(campaigns) < 2 && n.now-lapsed < 3_000_000 {
		sent := n.advance(TickInterval, 1, 2)
		if count(sent[1], raftpb.MsgPreVote) > 0 {
			campaigns = append(campaigns, n.now)
		}
		n.deliver(func(e envelope) bool { return e.from == 1 && e.to == 2 })
		if len(n.r[2].forts) > 0 || len(n.r[2].out.Actions) > 1 {
			t.Fatalf("the supporting follower answered: %v", n.r[2].out.Actions)
		}
		if n.now%lease.DefaultConfig.Heartbeat == 0 {
			n.renew(0, 2)
		}
	}
	if len(campaigns) != 2 {
		t.Fatalf("campaigned at %v", campaigns)
	}
	if d := campaigns[0] - lapsed; d > LapseCampaignMax+TickInterval {
		t.Fatalf("first campaign %v after the lapse", d.Duration())
	}
	if d := campaigns[1] - lapsed; d < ElectionTick*TickInterval || d >= 2*ElectionTick*TickInterval {
		t.Fatalf("second campaign %v after the lapse, want its election timeout", d.Duration())
	}
}

func TestALeaseIsTheQuorumthExpiryOverFortifiedVoters(t *testing.T) {
	const none core.Tick = 1 << 62
	voters := func(ids ...uint64) []uint64 { return ids }
	for _, tc := range []struct {
		name string
		conf raftpb.ConfState
		n    int
		// asks is, per follower node, when the leader's node asked it for its lease, in seconds.
		asks map[int]core.Tick
		end  core.Tick
	}{
		{"3 voters, one follower fortified", raftpb.ConfState{Voters: voters(1, 2, 3)}, 3, map[int]core.Tick{1: 0}, 0},
		{"3 voters, both fortified", raftpb.ConfState{Voters: voters(1, 2, 3)}, 3, map[int]core.Tick{1: 0, 2: 2}, 2},
		{"3 voters, the leader alone", raftpb.ConfState{Voters: voters(1, 2, 3)}, 3, nil, none},
		{"5 voters, three fortified", raftpb.ConfState{Voters: voters(1, 2, 3, 4, 5)}, 5, map[int]core.Tick{1: 0, 2: 1, 3: 2}, 1},
		{"5 voters, one fortified", raftpb.ConfState{Voters: voters(1, 2, 3, 4, 5)}, 5, map[int]core.Tick{1: 0}, none},
		{"joint, one in each half", raftpb.ConfState{Voters: voters(1, 4, 5), VotersOutgoing: voters(1, 2, 3)}, 5, map[int]core.Tick{1: 0, 3: 1}, 0},
		{"joint, the old half alone", raftpb.ConfState{Voters: voters(1, 4, 5), VotersOutgoing: voters(1, 2, 3)}, 5, map[int]core.Tick{1: 0, 2: 1}, none},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := newConfNet(t, tc.n, 1, func(int) *raft.MemoryStorage { return confStorage(tc.conf) })
			base := n.now
			for f := 1; f < tc.n; f++ {
				if at, ok := tc.asks[f]; ok {
					n.now = base + at*1_000_000
					n.leases(0, f)
				}
			}
			n.elect(0)
			if tc.end == none {
				if n.leaseRead(0) {
					t.Fatal("serves lease reads")
				}
				return
			}
			end := base + tc.end*1_000_000 + leaseLeft
			if n.now = end - 1; !n.leaseRead(0) {
				t.Fatalf("no lease read %v after the first ask", (n.now - base).Duration())
			}
			if n.now = end; n.leaseRead(0) {
				t.Fatalf("serves lease reads %v after the first ask", (n.now - base).Duration())
			}
		})
	}
}

func TestARestartedNodeRefusesVotesForPrimaryLease(t *testing.T) {
	for _, tc := range []struct {
		name       string
		after, lag core.Tick
		answered   bool
	}{
		{"2 s on both clocks", 2_000_000, 0, false},
		{"4.1 s on both clocks", 4_100_000, 0, true},
		{"4.1 s on CLOCK_BOOTTIME, 3.9 s on CLOCK_MONOTONIC_RAW", 4_100_000, 200_000, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := newNet(t, 3, 1)
			n.elect(0)
			n.restart(1)
			n.now += tc.after
			n.r[1].rawLag = tc.lag
			f := n.r[1]
			sent := n.recv(1, 2, n.challenge(raftpb.MsgPreVote, 2, 1, ""))
			if got := count(sent, raftpb.MsgPreVoteResp) == 1; got != tc.answered {
				t.Fatalf("pre-vote answered %v, want %v", got, tc.answered)
			}
			n.recv(1, 2, n.challenge(raftpb.MsgVote, 2, 1, ""))
			if got := f.flush != 0; got != tc.answered {
				t.Fatalf("vote took a round %v, want %v", got, tc.answered)
			}
		})
	}
	n := newNet(t, 3, 1)
	n.restart(1)
	for range 390 {
		if sent := n.advance(TickInterval, 1)[1]; len(sent) != 0 {
			t.Fatalf("a restarted node campaigned %v into its restart wait", (n.now - 5_000_000).Duration())
		}
	}
}

func TestAHolderThatEndsItsLeasesStopsServing(t *testing.T) {
	for _, tc := range []struct {
		name string
		end  func(n *testNet)
	}{
		{"Resumed", func(n *testNet) { n.r[0].holder.Resumed() }},
		{"a grantor's heartbeat trips the step check", func(n *testNet) {
			n.r[0].holder.OnHeartbeat(core.NodeID{Node: 1}, n.r[1].grantor.Run(), n.now+lease.DefaultConfig.Lease, n.leaseTime(0))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := newNet(t, 3, 1)
			n.leases(0, 1, 2)
			n.elect(0)
			if !n.leaseRead(0) {
				t.Fatal("no lease read once fortified")
			}
			tc.end(n)
			if n.leaseRead(0) {
				t.Fatal("serves lease reads after its holder ended its leases")
			}
			n.now += lease.DefaultConfig.RateGap
			n.leases(0, 1, 2)
			if !n.leaseRead(0) {
				t.Fatal("no lease read once the holder took its leases again in the same support epochs")
			}
		})
	}
}

func TestALapsedEpochsRenewalIsRefused(t *testing.T) {
	n := newNet(t, 3, 1)
	n.leases(0, 1)
	n.elect(0)
	n0 := core.NodeID{Node: 0}
	before, _ := n.r[1].grantor.Supporting(n0, n.leaseTime(1))
	n.now += lease.DefaultConfig.Lease + lease.DefaultConfig.RateGap
	if _, ok := n.r[1].grantor.Supporting(n0, n.leaseTime(1)); ok {
		t.Fatal("the grant runs a whole lease after it was given")
	}
	n.leases(0, 1)
	if after, _ := n.r[1].grantor.Supporting(n0, n.leaseTime(1)); after != before+1 {
		t.Fatalf("renewal after the lapse granted support epoch %d, the lapsed one %d", after, before)
	}
	if n.leaseRead(0) {
		t.Fatal("the leader counts a fortification under the lapsed epoch")
	}
	n.advance(HeartbeatTick * TickInterval)
	n.settle()
	if !n.leaseRead(0) || n.r[1].s.support.support != before+1 {
		t.Fatalf("lease read %v, follower supports epoch %d, after fortifying again", n.leaseRead(0), n.r[1].s.support.support)
	}
}

func TestATransferEndsTheLeaseWhenMsgTimeoutNowLeaves(t *testing.T) {
	n := newNet(t, 3, 1)
	for i := range 3 {
		n.leases(i, (i+1)%3, (i+2)%3)
	}
	n.elect(0)
	if !n.leaseRead(0) {
		t.Fatal("no lease read once fortified")
	}
	sent := n.call(0, func(ctx *core.Context, out *Output) {
		n.r[0].s.rn.TransferLeader(3)
		n.r[0].s.ready(ctx, out)
	})
	if count(sent, raftpb.MsgTimeoutNow) != 1 {
		t.Fatalf("transfer sent %v", types(sent))
	}
	if n.leaseRead(0) {
		t.Fatal("serves lease reads after MsgTimeoutNow left")
	}
	n.settle()
	if st := n.r[2].s.rn.BasicStatus(); st.RaftState != raft.StateLeader {
		t.Fatalf("the target is %v: the supporting follower did not pass its transfer vote", st.RaftState)
	}
	if n.leaseRead(0) || !n.leaseRead(2) {
		t.Fatalf("lease reads: old leader %v, new leader %v", n.leaseRead(0), n.leaseRead(2))
	}
}

func TestANewLeaderReadsThroughReadIndexUntilAnEntryOfItsTermCommits(t *testing.T) {
	n := newNet(t, 3, 1)
	n.elect(1)
	n.leases(0, 1, 2)
	n.call(0, func(ctx *core.Context, out *Output) {
		if err := n.r[0].s.rn.Campaign(); err != nil {
			t.Fatal(err)
		}
		n.r[0].s.ready(ctx, out)
	})
	for range 20 {
		n.deliver(func(e envelope) bool {
			typ, isRaft := n.typeOf(e)
			return !isRaft || typ != raftpb.MsgAppResp
		})
		for i, r := range n.r {
			if r.flush != 0 {
				n.durable(i)
			}
		}
	}
	l := n.r[0]
	if l.s.rn.BasicStatus().RaftState != raft.StateLeader || l.s.fort[1].term != n.term(0) {
		t.Fatalf("replica 0 is %v, fortified %+v", l.s.rn.BasicStatus().RaftState, l.s.fort)
	}
	if n.leaseRead(0) {
		t.Fatal("serves lease reads before an entry of its term committed")
	}
	n.call(0, func(ctx *core.Context, out *Output) { l.s.ReadIndex(ctx, []byte("r1"), out) })
	if len(l.reads) != 0 {
		t.Fatalf("ReadIndex confirmed %v before an entry of the term committed", l.reads)
	}
	for range 5 {
		n.advance(HeartbeatTick * TickInterval)
		n.settle()
	}
	commit := l.s.rn.BasicStatus().Commit
	if len(l.reads) != 1 || string(l.reads[0].RequestCtx) != "r1" || l.reads[0].Index != commit {
		t.Fatalf("ReadIndex gave %+v, commit %d", l.reads, commit)
	}
	if !n.leaseRead(0) {
		t.Fatal("no lease read once an entry of its term committed")
	}
}

// BenchmarkLeaseRead is a fortified leader's check before a read, which allocates nothing.
func BenchmarkLeaseRead(b *testing.B) {
	n := newNet(b, 3, 1)
	n.leases(0, 1, 2)
	n.elect(0)
	ctx := n.ctx(0)
	s := n.r[0].s
	b.ReportAllocs()
	for b.Loop() {
		if !s.LeaseRead(&ctx) {
			b.Fatal("no lease read")
		}
	}
}

// A restarted follower node counts support epochs from 1 again, so its new run's grant must not
// match the fortification its earlier run gave.
func TestARestartedFollowersNewRunDoesNotCount(t *testing.T) {
	n := newNet(t, 3, 1)
	n.leases(0, 1)
	n.elect(0)
	n.restart(1)
	n.now += lease.DefaultConfig.Lease + lease.DefaultConfig.RateGap
	n.inbox = n.inbox[:0]
	n.leases(0, 1)
	if n.leaseRead(0) {
		t.Fatal("counts the restarted follower's new run")
	}
}

// A follower that voted in a later term ignores the earlier leader's fortification request.
func TestAFollowerInALaterTermIgnoresAnOldFortification(t *testing.T) {
	n := newNet(t, 3, 1)
	n.leases(0, 1, 2)
	n.elect(0)
	term := n.term(0)
	n.recv(1, 2, n.challenge(raftpb.MsgVote, 2, 1, transferContext))
	n.durable(1)
	n.inbox = n.inbox[:0]
	ev := core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{Node: 0}}
	encodeFortify(testGroup, &fortify{kind: kindFortify, from: 1, to: 2, term: term}, &ev.Msg)
	n.event(1, ev)
	if f := n.r[1]; len(f.forts) != 0 || f.s.support.term != 0 {
		t.Fatalf("answered %v, supports term %d, in term %d", f.forts, f.s.support.term, n.term(1))
	}
}
