package meta

import (
	"encoding/binary"
	"errors"
	"slices"
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/lease"
	"github.com/nvcnvn/rock-solid/internal/raftshard"
)

// renewLeases has every host not down or stopped renew its node-pair lease from every other,
// twice rate_gap apart the first time.
func (r *hostRun) renewLeases() {
	if r.leaseID == 0 {
		r.now -= lease.DefaultConfig.RateGap
		r.renewAll()
		r.now += lease.DefaultConfig.RateGap
	}
	r.renewAll()
}

func (r *hostRun) renewAll() {
	t := lease.Time{Boot: r.now, Raw: r.now}
	for h := range r.hs {
		for g := range r.hs {
			if h == g || r.gone(h) || r.gone(g) {
				continue
			}
			r.leaseID++
			id := core.RequestID(r.leaseID)
			r.holders[h].Ask(voterNode(uint64(g)+1), id, t)
			if sup, ok := r.grantors[g].Grant(voterNode(uint64(h)+1), 1, 1, t); ok {
				r.holders[h].OnGrant(voterNode(uint64(g)+1), lease.Grant{ID: id, Run: r.grantors[g].Run(), Raw: t.Raw, Support: sup}, t)
			}
		}
	}
}

// gone reports whether host i is down or stopped, so its node renews no lease.
func (r *hostRun) gone(i int) bool { return r.down[i] || r.hs[i] == nil || r.hs[i].Err() != nil }

// raftOf decodes w's Raft message, if it carries one.
func (r *hostRun) raftOf(w *hostWork) (raftpb.Message, bool) {
	var m raftpb.Message
	b := w.a.Msg.Inline()
	if len(b) < 10 || b[0] != core.ProtoGroup || b[9] != 0 {
		return m, false
	}
	body := b[10:]
	if w.a.Msg.Buf != core.NoBuf {
		body = r.pool.Bytes(w.a.Msg.Buf)[:binary.LittleEndian.Uint32(body)]
	}
	return m, m.Unmarshal(body) == nil
}

// lowerCommit has w's Raft message carry a commit index of at most c, so its receiver takes its
// entries without learning they committed.
func (r *hostRun) lowerCommit(w *hostWork, c uint64) {
	m, ok := r.raftOf(w)
	if !ok || m.Commit <= c {
		return
	}
	m.Commit = c
	n := m.Size()
	var dst []byte
	if w.a.Msg.Buf != core.NoBuf {
		dst = r.pool.Bytes(w.a.Msg.Buf)[:n]
		binary.LittleEndian.PutUint32(w.a.Msg.Body[10:], uint32(n))
	} else {
		dst = w.a.Msg.Body[10 : 10+n]
		w.a.Msg.Len = uint16(10 + n)
	}
	if _, err := m.MarshalTo(dst); err != nil {
		r.t.Fatal(err)
	}
}

// ctlPlan is a shard map whose plan for the control-plane group is the hosts of ids, its last
// replica ID 4, beside a data group.
func ctlPlan(ids ...uint64) *ShardMap {
	g := Group{ID: hostGroup, LastID: 4}
	for _, id := range ids {
		g.Voters = append(g.Voters, Replica{ID: id, Node: voterNode(id).Node})
	}
	return &ShardMap{Groups: []Group{g, {ID: 2, Start: PrefixEnd([]byte{SpaceCtl})}}}
}

// deleted reports whether host i stopped because the group's leader deleted its replica.
func (r *hostRun) deleted(i int) bool { return errors.Is(r.hs[i].Err(), ErrDeleted) }

// applies reports whether host i applied the configuration of the voters of plan alone.
func (r *hostRun) applies(i int, plan ...uint64) bool {
	_, cs := r.hs[i].Shard().Applied()
	return len(cs.VotersOutgoing) == 0 && slices.Equal(sorted(cs.Voters), plan)
}

// leadsPlan reports whether one of the hosts in who leads, not halted, having applied plan.
func (r *hostRun) leadsPlan(plan []uint64, who ...int) bool {
	for _, i := range who {
		h := r.hs[i]
		if !r.gone(i) && h.Shard().Raft().Leader() == uint64(i)+1 && h.Shard().Halted() == 0 && r.applies(i, plan...) {
			return true
		}
	}
	return false
}

// controlled runs the group until a host has a Control, and returns it.
func (r *hostRun) controlled() int {
	r.t.Helper()
	for range 300 {
		r.run(StepInterval)
		for i, h := range r.hs {
			if !r.gone(i) && h.Control() != nil {
				return i
			}
		}
	}
	r.t.Fatal("no host led with a Control")
	return -1
}

func sorted(ids []uint64) []uint64 {
	s := slices.Clone(ids)
	slices.Sort(s)
	return s
}

// ctlSwap runs the control-plane group of A, B and C, each node holding a lease from every other,
// with D a fresh replica on a fourth, until its leader has a Control; that Control's plan swaps D
// in for the leader, when dropLeader, or else for a follower. It reports the dropped host and the
// plan once the dropped host is deleted.
func ctlSwap(t *testing.T, dropLeader bool) (*hostRun, int, []uint64) {
	r := newHostGroupWith(t, 4, ctlLogConfig(), func(r *hostRun) {
		r.boot = raftpb.ConfState{Voters: []uint64{1, 2, 3}}
		r.fresh[3], r.leasing, r.failing = true, true, true
	})
	l := r.controlled()
	dropped := (l + 1) % 3
	if dropLeader {
		dropped = l
	}
	var plan []uint64
	for i := range 3 {
		if i != dropped {
			plan = append(plan, uint64(i)+1)
		}
	}
	plan = append(plan, 4)
	if err := r.hs[l].Control().SetShardMap(ctlPlan(plan...)); err != nil {
		t.Fatal(err)
	}
	for range 600 {
		r.run(StepInterval)
		if r.hs[dropped].Err() == nil {
			continue
		}
		if !r.deleted(dropped) {
			t.Fatalf("the dropped voter stopped with %v", r.hs[dropped].Err())
		}
		for _, id := range plan {
			if !r.applies(int(id)-1, plan...) {
				t.Fatalf("the dropped voter is deleted while voter %d of the plan %v applied %+v", id, plan, r.hs[id-1].Shard().Report(nil))
			}
		}
		return r, dropped, plan
	}
	t.Fatalf("the voter the plan %v drops was never deleted", plan)
	return nil, 0, nil
}

// The control-plane group's leader drives the group to its own Control's plan: it adds D as a
// learner, swaps it in for a follower and leaves the joint configuration; the dropped voter, which
// learned its removal, is deleted only once every voter of the plan reported having applied the
// leave, and the group goes on committing.
func TestTheControlPlaneGroupSwapsInItsPlanAndDeletesTheDroppedVoter(t *testing.T) {
	r, _, plan := ctlSwap(t, false)
	for range 50 {
		r.run(StepInterval)
		for _, id := range plan {
			if r.leadsPlan(plan, int(id)-1) && r.ask(int(id)-1, 9, &Txn{Ops: []Op{Put(attr(1, 2, 1), b("v"))}}) != nil {
				return
			}
		}
	}
	t.Fatal("no voter of the plan leads and answers a Txn")
}

// A deleted voter stays deleted, as the voter model's Delete is final: a restart replays its log,
// finds the delete recorded and stops with ErrDeleted before its replica starts.
func TestADeletedControlPlaneVoterStaysDeletedAcrossARestart(t *testing.T) {
	r, dropped, _ := ctlSwap(t, false)
	r.restart(dropped)
	r.run(StepInterval)
	if !r.deleted(dropped) || r.hs[dropped].Shard() != nil {
		t.Fatalf("the restarted voter runs a replica %v and stopped with %v", r.hs[dropped].Shard() != nil, r.hs[dropped].Err())
	}
}

// A voter's report is kept apart from the nodes': the leader's Control counts it toward no node,
// so a voter, whose report carries release 0, holds back no format level, and a voter is sent no
// placement table, session table or disk membership.
func TestAVoterReportCountsTowardNoNode(t *testing.T) {
	r := newHostGroupWith(t, 3, ctlLogConfig(), func(r *hostRun) { r.leasing = true })
	var kinds []byte
	r.pass = func(w *hostWork) bool {
		if in := w.a.Msg.Inline(); len(in) > kindAt && slices.Contains([]byte{KindTable, KindDelta, KindDisks, KindSessions}, in[kindAt]) {
			kinds = append(kinds, in[kindAt])
		}
		return true
	}
	c := r.hs[r.controlled()].Control()
	r.run(3 * reportInterval)
	if len(c.voters) != 3 || len(c.nodes) != 0 || len(kinds) != 0 {
		t.Fatalf("the Control holds %d voters and %d nodes; the voters were sent kinds %x", len(c.voters), len(c.nodes), kinds)
	}
	if err := c.SetFormat(c.Format() + 1); err != nil {
		t.Fatalf("the voters hold back the format level: %v", err)
	}
}

// A delete naming a voter that has not learned its removal, such as a stale or replayed one,
// leaves its host running.
func TestADeleteToAVoterThatWasNotRemovedLeavesItRunning(t *testing.T) {
	r := newHostGroupWith(t, 3, ctlLogConfig(), func(r *hostRun) { r.leasing = true })
	r.controlled()
	a := core.Action{Kind: core.ActNetSend, Peer: voterNode(2)}
	if err := EncodeDelete(r.pool, hostGroup, []ReplicaRef{{Group: hostGroup, ID: 2}}, &a.Msg); err != nil {
		t.Fatal(err)
	}
	r.work = append(r.work, hostWork{from: 0, at: 1, a: a})
	r.drain()
	r.run(StepInterval)
	if err := r.hs[1].Err(); err != nil {
		t.Fatalf("a voter in the configuration stopped on a delete: %v", err)
	}
}

// A control-plane leader the plan drops hands its leadership to a planned voter, whose own Control
// then hands its replica the same plan and carries out the swap; the old leader is deleted once
// the plan is complete.
func TestAControlPlaneLeaderThePlanDropsHandsItsLeadershipOn(t *testing.T) {
	r, dropped, plan := ctlSwap(t, true)
	if removed, _ := r.hs[dropped].Shard().Raft().Removed(); !removed {
		t.Fatal("the old leader is deleted before it learned its removal")
	}
	for range 50 {
		r.run(StepInterval)
		if r.leadsPlan(plan, 0, 1, 2, 3) {
			return
		}
	}
	t.Fatalf("no voter of the plan %v leads", plan)
}

// ctlJoint is the joint configuration that swaps D in for C; ctlLeave is the index of the leave
// that the first leader of a group booted in it proposes, after its empty entry.
var ctlJoint = raftpb.ConfState{Voters: []uint64{1, 2, 4}, VotersOutgoing: []uint64{1, 2, 3}, AutoLeave: true}

const ctlLeave = 3

// toldCOnHosts is the voter model's ci-halt-live wedge on the control-plane group: A, B, C and D
// boot in ctlJoint, every node holding a lease from every other, and A leads, with a Control whose
// plan is the joint configuration's incoming half, counting joint reports when planted. A commits
// the leave with B and tells C alone, and the voters report for 3 s; then A halts or dies. It
// reports whether one of B and D then led having applied the plan within bound, and whether C was
// deleted within 3 bounds; C deleted before B and D applied the plan fails the test.
func toldCOnHosts(t *testing.T, die, jointCounts bool, bound core.Tick) (led, deleted bool) {
	r := newHostGroupWith(t, 4, ctlLogConfig(), func(r *hostRun) {
		r.boot, r.leasing, r.failing = ctlJoint, true, true
		r.paused[1], r.paused[2], r.paused[3] = true, true, true
	})
	r.pass = func(w *hostWork) bool {
		if w.from == 0 && (w.at == 1 || w.at == 3) {
			r.lowerCommit(w, ctlLeave-1)
		}
		return true
	}
	if l := r.controlled(); l != 0 {
		t.Fatalf("host %d leads, want A", l)
	}
	r.paused[1], r.paused[2], r.paused[3] = false, false, false
	r.hs[0].Control().jointCounts = jointCounts
	if err := r.hs[0].Control().SetShardMap(ctlPlan(1, 2, 4)); err != nil {
		t.Fatal(err)
	}
	r.run(3 * reportInterval)
	if removed, _ := r.hs[2].Shard().Raft().Removed(); !r.applies(0, 1, 2, 4) || !removed || r.applies(1, 1, 2, 4) || r.applies(3, 1, 2, 4) {
		t.Fatalf("A applied %+v, C learned its removal %v, B applied %+v, D %+v", r.hs[0].Shard().Report(nil), removed,
			r.hs[1].Shard().Report(nil), r.hs[3].Shard().Report(nil))
	}
	if die {
		r.down[0] = true
	} else {
		r.hs[0].Shard().Raft().Halt()
	}
	for end := r.now + 3*bound; r.now < end && !(led && deleted); {
		r.run(raftshard.TickInterval)
		led = led || r.now <= end-2*bound && r.leadsPlan([]uint64{1, 2, 4}, 1, 3)
		if !deleted && r.deleted(2) && !jointCounts && !(r.applies(1, 1, 2, 4) && r.applies(3, 1, 2, 4)) {
			t.Fatalf("C is deleted while B applied %+v and D %+v", r.hs[1].Shard().Report(nil), r.hs[3].Shard().Report(nil))
		}
		deleted = r.deleted(2)
	}
	return led, deleted
}

// The same wedge as TestARemovedVoterKeepsTheOutgoingHalfsVoteForAVoterThatHoldsTheLeave, on the
// control-plane group, whose own leader's Control deletes the dropped voter: A leads the joint
// configuration, commits the leave with B, tells C and then halts or dies, and B or D, holding the
// leave with only the joint configuration applied, leads with C's vote and applies the leave. C is
// deleted only once A, B and D reported having applied it, so never while A is dead. Planted,
// A's Control counts joint reports and deletes C before the halt or death, and no voter of the plan
// leads again.
func TestTheControlPlaneGroupsRemovedVoterKeepsTheOutgoingHalfsVote(t *testing.T) {
	bound := 2 * (lease.DefaultConfig.Lease + lease.DefaultConfig.Heartbeat + raftshard.LapseCampaignMax + raftshard.ElectionTick*raftshard.TickInterval)
	for _, die := range []bool{false, true} {
		led, deleted := toldCOnHosts(t, die, false, bound)
		if !led || deleted == die {
			t.Errorf("A dies %v: a voter of the plan led %v; C deleted %v", die, led, deleted)
		}
		if led, deleted := toldCOnHosts(t, die, true, bound); led || !deleted {
			t.Errorf("A dies %v, joint reports counted: a voter of the plan led %v; C deleted %v", die, led, deleted)
		}
	}
}

// leaveBehindOnHosts is the voter model's ci-leave-die-live wedge on the control-plane group: A,
// B, C and D boot in ctlJoint and A is cut off; C leads and commits the leave on B and D, which do
// not learn it committed; B wins the next term, appends its entry to C alone and dies. Then A
// returns, every node left holding a lease from every other. It reports whether one of A and D led
// having applied the plan within bound, and whether C led meanwhile.
func leaveBehindOnHosts(t *testing.T, bound core.Tick) (led, cLed bool) {
	r := newHostGroupWith(t, 4, ctlLogConfig(), func(r *hostRun) {
		r.boot, r.failing = ctlJoint, true
		r.paused[0], r.paused[1], r.paused[3] = true, true, true
	})
	cutA, bToC, mute := true, false, false
	r.pass = func(w *hostWork) bool {
		m, isRaft := r.raftOf(w)
		switch {
		case cutA && (w.from == 0 || w.at == 0):
			return false
		case mute && isRaft && w.from == 2 && w.at == 1 && (m.Type == raftpb.MsgApp || m.Type == raftpb.MsgHeartbeat):
			return false
		case cutA && isRaft && w.at == 2 && (w.from == 1 || w.from == 3) && m.Type == raftpb.MsgAppResp && m.Index > ctlLeave:
			return false
		case w.from == 2 && (w.at == 1 || w.at == 3):
			r.lowerCommit(w, ctlLeave-1)
		case w.from == 1 && isRaft && m.Type == raftpb.MsgApp:
			bToC = bToC || w.at == 2 && len(m.Entries) > 0
			return w.at == 2
		}
		return true
	}
	for range 500 {
		if removed, _ := r.hs[2].Shard().Raft().Removed(); removed {
			break
		}
		r.run(raftshard.TickInterval)
	}
	if removed, _ := r.hs[2].Shard().Raft().Removed(); !removed || r.applies(1, 1, 2, 4) || r.applies(3, 1, 2, 4) {
		t.Fatalf("C learned its removal %v; B applied %+v, D %+v", removed, r.hs[1].Shard().Report(nil), r.hs[3].Shard().Report(nil))
	}
	r.paused[1], r.paused[2], mute = false, true, true
	for range 500 {
		if bToC {
			break
		}
		r.run(raftshard.TickInterval)
	}
	if !bToC || r.hs[1].Shard().Raft().Leader() != 2 {
		t.Fatalf("B leads %v, appended to C %v; terms %d %d %d %d leaders %d %d %d %d", r.hs[1].Shard().Raft().Leader() == 2, bToC, r.hs[0].Shard().Raft().Term(), r.hs[1].Shard().Raft().Term(), r.hs[2].Shard().Raft().Term(), r.hs[3].Shard().Raft().Term(), r.hs[0].Shard().Raft().Leader(), r.hs[1].Shard().Raft().Leader(), r.hs[2].Shard().Raft().Leader(), r.hs[3].Shard().Raft().Leader())
	}
	r.down[1], cutA, r.leasing, mute = true, false, true, false
	r.paused[0], r.paused[2], r.paused[3] = false, false, false
	for end := r.now + bound; r.now < end; {
		r.run(raftshard.TickInterval)
		cLed = cLed || r.hs[2].Shard().Raft().Leader() == 3
		if r.leadsPlan([]uint64{1, 2, 4}, 0, 3) {
			return true, cLed
		}
	}
	return false, cLed
}

// The same wedge as TestARemovedVoterThatHoldsTheOutgoingHalfsNewestEntryLeadsAndHandsOff, on the
// control-plane group: C, holding the newest entry of the outgoing half, wins, commits the leave on
// A and D and hands its leadership to one of them, and is kept, since B, a voter of the plan, is
// down for good.
func TestTheControlPlaneGroupsRemovedVoterHoldingTheNewestEntryLeadsAndHandsOff(t *testing.T) {
	bound := 4 * raftshard.ElectionTick * raftshard.TickInterval
	led, cLed := leaveBehindOnHosts(t, bound)
	if !led || !cLed {
		t.Fatalf("within %v C led %v, and a voter of the plan %v", bound.Duration(), cLed, led)
	}
}
