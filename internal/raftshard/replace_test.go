package raftshard

import (
	"slices"
	"testing"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// Replicas of a replacement net: voters A, B and C, and D, a fresh replica with no state on a fourth node.
const (
	rA, rB, rC, rD = 0, 1, 2, 3
)

// newReplaceNet is a group of three voters, IDs 1 to 3, and an empty replica with ID 4, every one knowing every node; A leads.
func newReplaceNet(t *testing.T, p plants) *testNet {
	n := newConfNet(t, 4, 1, func(i int) *raft.MemoryStorage {
		if i == rD {
			return raft.NewMemoryStorage()
		}
		return bootStorage(3)
	})
	if p != (plants{}) {
		r := n.r[rA]
		cfg := testConfig(4, rA, r.st, n.seed)
		cfg.Holder, cfg.Grantor = r.holder, r.grantor
		s, err := newShard(cfg, p)
		if err != nil {
			t.Fatal(err)
		}
		r.s = s
		n.call(rA, func(ctx *core.Context, out *Output) { s.Start(ctx, out) })
	}
	n.elect(rA)
	return n
}

// replacing is the shard map's plan once D replaces C.
var replacing = []Peer{{ID: 1, Node: core.NodeID{Node: 0}}, {ID: 2, Node: core.NodeID{Node: 1}}, {ID: 4, Node: core.NodeID{Node: 3}}}

// settleWithout delivers and flushes until nothing is left to do, losing every message to or from the replicas in cut.
func (n *testNet) settleWithout(cut ...int) {
	keep := func(e envelope) bool { return !slices.Contains(cut, e.from) && !slices.Contains(cut, e.to) }
	for range 1000 {
		n.deliver(keep)
		flushed := false
		for i, r := range n.r {
			if r.flush != 0 && !slices.Contains(cut, i) {
				n.durable(i)
				flushed = true
			}
		}
		if !flushed && len(n.inbox) == 0 {
			return
		}
	}
	n.t.Fatal("the group did not settle")
}

// snapshot has replica i's storage take a snapshot at its applied index, as a point-in-time view gives one on demand.
func (n *testNet) snapshot(i int) {
	r := n.r[i]
	if snap, _ := r.st.Snapshot(); r.s.applied > snap.Metadata.Index {
		if _, err := r.st.CreateSnapshot(r.s.applied, &r.s.conf, le.AppendUint64(nil, uint64(len(r.applied)))); err != nil {
			n.t.Fatal(err)
		}
	}
}

// heartbeat moves the clock a heartbeat on every replica not in cut, then settles without them.
func (n *testNet) heartbeat(cut ...int) {
	var who []int
	for i := range n.r {
		if !slices.Contains(cut, i) {
			who = append(who, i)
		}
	}
	n.advance(HeartbeatTick*TickInterval, who...)
	n.settleWithout(cut...)
}

func (n *testNet) conf(i int) raftpb.ConfState { return n.r[i].s.conf }

// committed reports whether replica i applied an entry holding data.
func (n *testNet) committed(i int, data string) bool {
	return slices.ContainsFunc(n.r[i].applied, func(e raftpb.Entry) bool { return string(e.Data) == data })
}

func TestAPlanThatKeepsTheVotersChangesNothing(t *testing.T) {
	n := newReplaceNet(t, plants{})
	n.r[rA].s.SetPlan([]Peer{{ID: 1}, {ID: 2}, {ID: 3}})
	n.propose(rA, []byte("x"))
	n.settleWithout(rC, rD)
	if !n.committed(rB, "x") {
		t.Fatal("A and B did not commit with C down")
	}
	if c := n.conf(rA); !slices.Equal(c.Voters, []uint64{1, 2, 3}) || len(c.Learners) != 0 {
		t.Fatalf("configuration %+v under a plan of the voters it has", c)
	}
}

func TestAReplacementJoinsAsALearnerThenSwapsInOneJointChange(t *testing.T) {
	n := newReplaceNet(t, plants{})
	n.r[rA].s.SetPlan(replacing)
	n.propose(rA, []byte("a"))
	n.settleWithout(rC, rD)
	if c := n.conf(rA); !slices.Equal(c.Learners, []uint64{4}) || !slices.Equal(c.Voters, []uint64{1, 2, 3}) {
		t.Fatalf("configuration %+v once the plan names D, want D a learner", c)
	}
	for k := range 5 {
		v := string(rune('b' + k))
		n.propose(rA, []byte(v))
		n.heartbeat(rC, rD)
		if !n.committed(rA, v) || !n.committed(rB, v) {
			t.Fatalf("%q did not commit on A and B while D, a learner, hears nothing", v)
		}
	}
	if c := n.conf(rA); !slices.Equal(c.Learners, []uint64{4}) {
		t.Fatalf("D promoted before it caught up: %+v", c)
	}
	// D's first append is refused, its log empty, so A sends a snapshot taken after D joined.
	n.snapshot(rA)
	for range 5 {
		n.heartbeat(rC)
	}
	n.propose(rA, []byte("z"))
	n.settleWithout(rC)
	for _, i := range []int{rA, rB, rD} {
		if c := n.conf(i); !slices.Equal(sorted(c.Voters), []uint64{1, 2, 4}) || len(c.VotersOutgoing) != 0 || len(c.Learners) != 0 {
			t.Fatalf("replica %d ends in %+v, want voters 1, 2, 4 out of the joint configuration", i, c)
		}
	}
	if n.r[rD].snaps != 1 || !n.committed(rD, "z") {
		t.Fatalf("D restored %d snapshots and applied z: %v", n.r[rD].snaps, n.committed(rD, "z"))
	}
	joint := 0
	for _, e := range n.r[rB].applied {
		var cc raftpb.ConfChangeV2
		if e.Type == raftpb.EntryConfChangeV2 && cc.Unmarshal(e.Data) == nil && len(cc.Changes) == 2 {
			joint++
			if cc.Changes[0] != (raftpb.ConfChangeSingle{Type: raftpb.ConfChangeRemoveNode, NodeID: 3}) || cc.Changes[1] != (raftpb.ConfChangeSingle{Type: raftpb.ConfChangeAddNode, NodeID: 4}) {
				t.Fatalf("the swap is %+v", cc.Changes)
			}
		}
	}
	if joint != 1 {
		t.Fatalf("%d two-change swaps applied, want one", joint)
	}
}

func sorted(ids []uint64) []uint64 {
	ids = slices.Clone(ids)
	slices.Sort(ids)
	return ids
}

// A learner promoted at once, planted, joins the voters while it holds nothing, which the
// simulator's invariant catches; here the trace shows the swap the correct shard withholds.
func TestAnEagerLeaderPromotesALearnerThatHoldsNothing(t *testing.T) {
	n := newReplaceNet(t, plants{eager: true})
	n.r[rA].s.SetPlan(replacing)
	n.propose(rA, []byte("a"))
	n.settleWithout(rC, rD)
	n.propose(rA, []byte("b"))
	n.settleWithout(rC, rD)
	if c := n.conf(rA); !slices.Contains(c.Voters, 4) {
		t.Fatalf("the planted leader did not promote D: %+v", c)
	}
	if last, _ := n.r[rD].st.LastIndex(); last != 0 {
		t.Fatalf("D holds up to %d", last)
	}
}

// A replacement added as a voter, planted, puts D into every quorum: with C down nothing commits until D catches up.
func TestAReplacementAddedAsAVoterStallsProposals(t *testing.T) {
	n := newReplaceNet(t, plants{voterFirst: true})
	n.r[rA].s.SetPlan(replacing)
	n.propose(rA, []byte("a"))
	n.settleWithout(rC, rD)
	n.propose(rA, []byte("b"))
	n.heartbeat(rC, rD)
	if n.committed(rA, "b") {
		t.Fatal("b committed with D a voter that holds nothing")
	}
}

func TestALearnerIsNotPromotedToMakeAQuorum(t *testing.T) {
	n := newReplaceNet(t, plants{})
	n.r[rA].s.SetPlan(replacing)
	n.propose(rA, []byte("a"))
	n.settleWithout(rC, rD)
	n.snapshot(rA)
	n.propose(rA, []byte("b"))
	for range 5 {
		n.heartbeat(rB, rC)
	}
	if n.r[rD].snaps != 1 {
		t.Fatal("D did not catch up while B was down")
	}
	if n.committed(rA, "b") {
		t.Fatal("b committed on A and D, a learner, with B and C down")
	}
	if c := n.conf(rA); slices.Contains(c.Voters, 4) {
		t.Fatalf("D promoted to make a quorum: %+v", c)
	}
	for range 3 {
		n.heartbeat(rC)
	}
	if c := n.conf(rA); !n.committed(rA, "b") || !slices.Equal(sorted(c.Voters), []uint64{1, 2, 4}) || len(c.VotersOutgoing) != 0 {
		t.Fatalf("once B returned: b committed %v, configuration %+v", n.committed(rA, "b"), c)
	}
}

func TestARemovedReplicaThatReturnsDeletesItsState(t *testing.T) {
	n := newReplaceNet(t, plants{})
	n.r[rA].s.SetPlan(replacing)
	n.propose(rA, []byte("a"))
	n.settleWithout(rC, rD)
	n.snapshot(rA)
	for range 3 {
		n.heartbeat(rC)
	}
	if c := n.conf(rA); !slices.Equal(sorted(c.Voters), []uint64{1, 2, 4}) {
		t.Fatalf("the swap did not finish: %+v", c)
	}
	n.restart(rC)
	for k := 0; !n.r[rC].removed; k++ {
		if k == 30 {
			t.Fatal("C never learned it was removed")
		}
		sent := n.advance(HeartbeatTick*TickInterval, rC)[rC]
		if count(sent, raftpb.MsgPreVote)+count(sent, raftpb.MsgVote) != 0 {
			t.Fatalf("C campaigned on its return: %v", types(sent))
		}
		n.settleWithout()
	}
	for range 100 {
		if sent := n.advance(HeartbeatTick*TickInterval, rC)[rC]; len(sent) != 0 || len(n.r[rC].out.Actions) != 0 {
			t.Fatalf("C acted after its removal: %v", n.r[rC].out.Actions)
		}
	}
	for i, r := range n.r {
		if i != rC && r.s.Stats().Misrouted != 0 {
			t.Fatalf("replica %d dropped C's queries as misrouted: %+v", i, r.s.Stats())
		}
	}
}

// A peer whose applied configuration lacks this replica tells it nothing unless it applied at least as far: a lagging peer may not yet hold the change that added it.
func TestARemovalNoticeFromALaggingPeerIsIgnored(t *testing.T) {
	n := newNet(t, 3, 1)
	n.elect(0)
	n.propose(0, []byte("x"))
	n.settle()
	applied := n.r[1].s.applied
	for _, c := range []struct {
		at   uint64
		want bool
	}{{applied - 1, false}, {applied, true}} {
		ev := core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{Node: 0}}
		encodeFortify(testGroup, &fortify{kind: kindRemoved, from: 1, to: 2, term: c.at}, &ev.Msg)
		n.event(1, ev)
		if n.r[1].out.Removed != c.want {
			t.Fatalf("a notice at applied index %d of %d: removed %v", c.at, applied, n.r[1].out.Removed)
		}
	}
}

// A leader the plan drops hands its leadership to a planned voter, which then carries out the replacement.
func TestALeaderThePlanDropsTransfersLeadership(t *testing.T) {
	n := newReplaceNet(t, plants{})
	n.leases(rA, rB, rC)
	plan := []Peer{{ID: 2, Node: core.NodeID{Node: 1}}, {ID: 3, Node: core.NodeID{Node: 2}}, {ID: 4, Node: core.NodeID{Node: 3}}}
	for _, r := range n.r {
		r.s.SetPlan(plan)
	}
	n.propose(rA, []byte("a"))
	n.settleWithout(rD)
	if st := n.r[rA].s.rn.BasicStatus(); st.RaftState == raft.StateLeader {
		t.Fatal("A still leads under a plan without it")
	}
	n.snapshot(rB)
	n.snapshot(rC)
	for range 5 {
		n.heartbeat(rA)
	}
	for _, i := range []int{rB, rC, rD} {
		if c := n.conf(i); !slices.Equal(sorted(c.Voters), []uint64{2, 3, 4}) || len(c.VotersOutgoing) != 0 {
			t.Fatalf("replica %d ends in %+v", i, c)
		}
	}
}

// A leader cut off while the others elect a new leader and swap it out still leads its old term,
// so it asks no membership question; a peer answers its messages with the removal notice.
func TestAPartitionedLeaderLearnsItWasRemoved(t *testing.T) {
	n := newReplaceNet(t, plants{})
	n.propose(rA, []byte("a"))
	n.settleWithout(rD)
	n.call(rB, func(ctx *core.Context, out *Output) {
		if err := n.r[rB].s.rn.Campaign(); err != nil {
			t.Fatal(err)
		}
		n.r[rB].s.ready(ctx, out)
	})
	n.settleWithout(rA, rD)
	n.r[rB].s.SetPlan([]Peer{{ID: 2, Node: core.NodeID{Node: 1}}, {ID: 3, Node: core.NodeID{Node: 2}}, {ID: 4, Node: core.NodeID{Node: 3}}})
	n.propose(rB, []byte("b"))
	n.heartbeat(rA, rD)
	for range 10 {
		n.snapshot(rB)
		n.heartbeat(rA)
	}
	if c := n.conf(rB); !slices.Equal(sorted(c.Voters), []uint64{2, 3, 4}) {
		t.Fatalf("the swap did not finish: %+v", c)
	}
	if st := n.r[rA].s.rn.BasicStatus(); st.RaftState != raft.StateLeader {
		t.Fatalf("A is %v, want still leading its old term", st.RaftState)
	}
	for k := 0; !n.r[rA].removed; k++ {
		if k == 30 {
			t.Fatal("A never learned it was removed")
		}
		n.heartbeat()
	}
}

// A leader the plan drops transfers only to a planned voter whose node runs and that holds its
// log: etcd-raft drops proposals while a transfer waits, so a transfer to a down voter stalls them.
func TestALeaderTransfersOnlyToAPlannedVoterThatIsUp(t *testing.T) {
	n := newReplaceNet(t, plants{})
	n.leases(rA, rC)
	n.r[rA].s.SetPlan([]Peer{{ID: 2, Node: core.NodeID{Node: 1}}, {ID: 3, Node: core.NodeID{Node: 2}}, {ID: 4, Node: core.NodeID{Node: 3}}})
	n.propose(rA, []byte("a"))
	for range 3 {
		if to := n.r[rA].s.rn.Status().LeadTransferee; to == 2 {
			t.Fatal("A transfers to B, which is down")
		}
		n.heartbeat(rB, rD)
	}
	if st := n.r[rC].s.rn.BasicStatus(); st.RaftState != raft.StateLeader {
		t.Fatalf("C is %v, want leader after A's transfer", st.RaftState)
	}
}

// With no planned voter yet, a leader the plan drops swaps the planned replicas in for the other voters first.
func TestALeaderThePlanDropsWithEveryVoterSwapsTheOthersFirst(t *testing.T) {
	n := newConfNet(t, 6, 1, func(i int) *raft.MemoryStorage {
		if i >= 3 {
			return raft.NewMemoryStorage()
		}
		return bootStorage(3)
	})
	n.elect(rA)
	n.leases(rA, 3, 4, 5)
	for _, r := range n.r {
		r.s.SetPlan([]Peer{{ID: 4, Node: core.NodeID{Node: 3}}, {ID: 5, Node: core.NodeID{Node: 4}}, {ID: 6, Node: core.NodeID{Node: 5}}})
	}
	// The new replicas hear nothing until all three joined, so the first snapshot they get holds them.
	for range 5 {
		n.heartbeat(3, 4, 5)
	}
	for range 20 {
		for i := range 6 {
			if n.r[i].s.rn.BasicStatus().RaftState == raft.StateLeader {
				n.snapshot(i)
			}
		}
		n.heartbeat()
	}
	for i := 3; i < 6; i++ {
		if c := n.conf(i); !slices.Equal(sorted(c.Voters), []uint64{4, 5, 6}) || len(c.VotersOutgoing) != 0 {
			t.Fatalf("replica %d ends in %+v", i, c)
		}
	}
}

// swapUnderLoad runs the replacement with a proposal every 10 ms and D's link one step slower than
// the others, for 3 s, and reports whether D was swapped in.
func swapUnderLoad(t *testing.T, p plants) bool {
	n := newReplaceNet(t, p)
	n.r[rA].s.SetPlan(replacing)
	n.propose(rA, []byte("a"))
	n.settleWithout(rC, rD)
	n.snapshot(rA)
	var late []envelope
	for step := range 300 {
		n.now += TickInterval
		for _, i := range []int{rA, rB, rD} {
			n.event(i, core.Event{Kind: core.EvTimer, Timer: 1})
		}
		if n.r[rA].s.rn.BasicStatus().RaftState == raft.StateLeader {
			n.call(rA, func(ctx *core.Context, out *Output) { _ = n.r[rA].s.Propose(ctx, []byte{byte(step)}, out) })
		}
		now := late
		late = nil
		for _, e := range n.inbox {
			switch {
			case e.from == rC || e.to == rC:
				if e.msg.Buf != core.NoBuf {
					n.r[e.to].pool.Free(e.msg.Buf)
				}
			case e.from == rD || e.to == rD:
				late = append(late, e)
			default:
				now = append(now, e)
			}
		}
		n.inbox = n.inbox[:0]
		for _, e := range now {
			n.event(e.to, core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{Node: uint32(e.from)}, Msg: e.msg})
		}
		for i, r := range n.r {
			if r.flush != 0 && i != rC {
				n.durable(i)
			}
		}
	}
	return slices.Contains(n.conf(rA).Voters, 4)
}

// Under steady writes a learner behind a slower link trails the current commit index for good: held
// to the commit index of a heartbeat ago it is swapped in, held to the current one it never is.
func TestALearnerBehindASlowerLinkIsPromotedUnderSteadyWrites(t *testing.T) {
	if !swapUnderLoad(t, plants{}) {
		t.Fatal("D never swapped in under steady writes")
	}
	if swapUnderLoad(t, plants{current: true}) {
		t.Fatal("a learner held to the current commit index was swapped in, so this test would miss that rule")
	}
}

// A replica its own applied configuration does not hold yet, as a new one before its addition
// reaches it, ignores a removal notice at any index: a notice proves a removal only to a member.
func TestARemovalNoticeNeedsTheReplicaInItsOwnConfiguration(t *testing.T) {
	n := newReplaceNet(t, plants{})
	d := n.r[rD]
	ev := core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{Node: 0}}
	encodeFortify(testGroup, &fortify{kind: kindRemoved, from: 1, to: 4, term: 1 << 40}, &ev.Msg)
	n.event(rD, ev)
	if d.removed {
		t.Fatal("a replica outside its own applied configuration took a removal notice")
	}
}

// A leader the plan drops does not transfer to a planned voter whose node runs but that lags its log.
func TestALeaderTransfersOnlyToAPlannedVoterThatHoldsItsLog(t *testing.T) {
	n := newReplaceNet(t, plants{})
	n.leases(rA, rC)
	for k := range 5 {
		n.propose(rA, []byte{byte(k)})
		n.heartbeat(rC, rD)
	}
	n.r[rA].s.SetPlan([]Peer{{ID: 2, Node: core.NodeID{Node: 1}}, {ID: 3, Node: core.NodeID{Node: 2}}, {ID: 4, Node: core.NodeID{Node: 3}}})
	n.propose(rA, []byte("x"))
	if to := n.r[rA].s.rn.Status().LeadTransferee; to == 3 {
		t.Fatal("A transfers to C, which lags its log")
	}
}

func TestALeaderRemovesALearnerThePlanDropped(t *testing.T) {
	n := newReplaceNet(t, plants{})
	n.r[rA].s.SetPlan(replacing)
	n.propose(rA, []byte("a"))
	n.settleWithout(rC, rD)
	if c := n.conf(rA); !slices.Equal(c.Learners, []uint64{4}) {
		t.Fatalf("configuration %+v once the plan names D, want D a learner", c)
	}
	n.r[rA].s.SetPlan([]Peer{{ID: 1}, {ID: 2}, {ID: 3}})
	n.propose(rA, []byte("b"))
	n.settleWithout(rC, rD)
	if c := n.conf(rA); len(c.Learners) != 0 || !slices.Equal(c.Voters, []uint64{1, 2, 3}) {
		t.Fatalf("configuration %+v under a plan of the voters, want D removed", c)
	}
}

// A peer answers with its applied index, not its commit index: one that committed D's addition
// but has not applied it lacks D in its configuration, and its commit index may reach D's applied
// index, so answering with it would have D delete the state of a replica the group holds.
func TestARemovalNoticeCarriesTheAnswerersAppliedIndex(t *testing.T) {
	n := newReplaceNet(t, plants{})
	n.r[rA].s.SetPlan(replacing)
	n.propose(rA, []byte("a"))
	n.settleWithout(rB, rD)
	n.snapshot(rA)
	for range 5 {
		n.heartbeat(rB)
	}
	b, d := n.r[rB], n.r[rD]
	if !member(&d.s.conf, 4) {
		t.Fatalf("D did not apply its addition: %+v", d.s.conf)
	}
	for k := 0; b.flush == 0; k++ {
		if k == 30 {
			t.Fatal("B never took a round of the entries it missed")
		}
		n.advance(HeartbeatTick*TickInterval, rA, rB, rC, rD)
		n.deliver(nil)
		for i, r := range n.r {
			if r.flush != 0 && i != rB {
				n.durable(i)
			}
		}
	}
	if st := b.s.rn.BasicStatus(); st.Commit < d.s.applied || b.s.applied >= d.s.applied || member(&b.s.conf, 4) {
		t.Fatalf("B commits up to %d and applied %d, D applied %d: want B's commit index alone at D's", st.Commit, b.s.applied, d.s.applied)
	}
	ask := core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{Node: 3}}
	encodeFortify(testGroup, &fortify{kind: kindMember, from: 4, to: 2}, &ask.Msg)
	for _, c := range []struct {
		what string
		send func()
	}{
		{"membership question", func() { n.event(rB, ask) }},
		{"Raft message", func() {
			n.recv(rB, rD, raftpb.Message{Type: raftpb.MsgHeartbeatResp, From: 4, To: 2, Term: n.term(rA)})
		}},
	} {
		n.inbox = n.inbox[:0]
		c.send()
		n.deliver(func(e envelope) bool { return e.from == rB && e.to == rD })
		if d.removed {
			t.Fatalf("D took B's answer to its %s as a removal", c.what)
		}
	}
}

// A replica whose Peers lack a member of its applied configuration, as one restored from a
// snapshot that a later change was folded into, learns that member's node from its messages.
func TestAReplicaLearnsTheNodeOfAMemberThatSendsToIt(t *testing.T) {
	n := newNet(t, 3, 1)
	n.elect(0)
	r := n.r[2]
	cfg := testConfig(3, 2, r.st, n.seed)
	cfg.Peers = cfg.Peers[2:]
	cfg.Holder, cfg.Grantor = r.holder, r.grantor
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	r.s, r.flush = s, 0
	n.call(2, func(ctx *core.Context, out *Output) { s.Start(ctx, out) })
	n.advance(HeartbeatTick*TickInterval, 0)
	n.settle()
	if i := s.peer(1); i < 0 || s.cfg.Peers[i].Node != (core.NodeID{Node: 0}) || s.Stats().Unsent != 0 {
		t.Fatalf("peers %+v, stats %+v: want the leader learned at node 0 and the reply sent", s.cfg.Peers, s.Stats())
	}
}
