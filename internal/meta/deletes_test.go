package meta

import (
	"slices"
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/raftshard"
)

// deleting is a Control whose shard map plans group 5 onto voters, each replica ID on the node of
// the same number, and that heard nothing yet.
func deleting(voters ...uint64) *Control {
	g := Group{ID: 5, LastID: slices.Max(voters)}
	for _, id := range voters {
		g.Voters = append(g.Voters, Replica{ID: id, Node: uint32(id), Disk: uint32(id)})
	}
	return &Control{ctlState: ctlState{smap: ShardMap{Groups: []Group{g}}}}
}

// hear has node id report its replica of group 5 at now.
func hear(t *testing.T, c *Control, now core.Tick, r ReplicaReport) {
	t.Helper()
	r.Group = 5
	if err := c.Heartbeat(now, &NodeReport{Node: uint32(r.ID), Replicas: []ReplicaReport{r}}, &ControlOutput{}); err != nil {
		t.Fatal(err)
	}
}

// deletes is what the leader tells node 3, holding replica 3, to delete.
func deletes(c *Control, leased bool) []ReplicaRef { return c.Deletes(3, leased, nil) }

// The plan swaps D, replica 4, in for C, replica 3. C, which learned its removal, is deleted only
// once A, B and D each reported a configuration of the plan's voters alone at or past the leave:
// reports of the joint configuration, whose incoming half is the plan's, do not count, since a
// voter that applied only that can lead it and need C's vote. A leader without its lease tells no
// node to delete anything, and a replica that has not learned its removal is never named.
func TestADroppedReplicaIsDeletedOnlyOnceEveryPlannedVoterAppliedTheLeave(t *testing.T) {
	c := deleting(1, 2, 4)
	plan := []uint64{1, 2, 4}
	hear(t, c, 1, ReplicaReport{ID: 3, ConfIndex: 3, Joint: true, Voters: plan})
	for _, id := range []uint64{1, 2, 4} {
		hear(t, c, 1, ReplicaReport{ID: id, ConfIndex: 3, Joint: true, Voters: plan})
	}
	if d := deletes(c, true); len(d) != 0 {
		t.Fatalf("C, which has not learned its removal, is deleted: %v", d)
	}
	hear(t, c, 2, ReplicaReport{ID: 3, ConfIndex: 3, Joint: true, Removed: true, Voters: plan})
	if d := deletes(c, true); len(d) != 0 {
		t.Fatalf("C is deleted once every planned voter applied only the joint configuration: %v", d)
	}
	hear(t, c, 3, ReplicaReport{ID: 1, ConfIndex: 5, Voters: plan})
	hear(t, c, 3, ReplicaReport{ID: 4, ConfIndex: 7, Voters: plan})
	if d := deletes(c, true); len(d) != 0 {
		t.Fatalf("C is deleted while B applied only the joint configuration: %v", d)
	}
	hear(t, c, 4, ReplicaReport{ID: 2, ConfIndex: 5, Voters: plan})
	if d := deletes(c, false); len(d) != 0 {
		t.Fatalf("a leader without its lease deletes %v", d)
	}
	if d := deletes(c, true); !slices.Equal(d, []ReplicaRef{{Group: 5, ID: 3}}) {
		t.Fatalf("once every planned voter applied the leave, the leader deletes %v, want C", d)
	}
	if d := c.Deletes(1, true, nil); len(d) != 0 {
		t.Fatalf("a planned voter is deleted: %v", d)
	}
}

// Planted, a report of the joint configuration counts: C is deleted once A, B and D applied only
// that, any of which can lead it and need C's vote, as the voter model's neg-joint-report-counts
// finds.
func TestCountingJointReportsDeletesWhileAVoterAppliedOnlyTheJointConfiguration(t *testing.T) {
	c := deleting(1, 2, 4)
	c.jointCounts = true
	plan := []uint64{1, 2, 4}
	for _, id := range []uint64{1, 2, 4} {
		hear(t, c, 1, ReplicaReport{ID: id, ConfIndex: 3, Joint: true, Voters: plan})
	}
	hear(t, c, 1, ReplicaReport{ID: 3, ConfIndex: 3, Joint: true, Removed: true, Voters: plan})
	if d := deletes(c, true); !slices.Equal(d, []ReplicaRef{{Group: 5, ID: 3}}) {
		t.Fatalf("planted, the leader deletes %v, want C", d)
	}
}

// The plan swaps D in for C while B is down for good: B never reports applying the leave, so C is
// kept, however long A and D report. The next plan, swapping E, replica 5, in for B, is what
// completes: once A, D and E report its leave applied, C is deleted, the earlier plan complete
// through the later one's leave, which removed B.
func TestADroppedReplicaIsKeptWhileAPlannedVoterIsDownForGood(t *testing.T) {
	c := deleting(1, 2, 4)
	plan := []uint64{1, 2, 4}
	now := core.Tick(1)
	hear(t, c, now, ReplicaReport{ID: 2, ConfIndex: 3, Joint: true, Voters: plan})
	for range 3 * chunkstore.NodeDownAfter / beat {
		hear(t, c, now, ReplicaReport{ID: 1, ConfIndex: 5, Voters: plan})
		hear(t, c, now, ReplicaReport{ID: 4, ConfIndex: 5, Voters: plan})
		hear(t, c, now, ReplicaReport{ID: 3, ConfIndex: 3, Joint: true, Removed: true, Voters: plan})
		if d := deletes(c, true); len(d) != 0 {
			t.Fatalf("at %d C is deleted while B is down: %v", now, d)
		}
		now += beat
	}
	next := deleting(1, 4, 5)
	next.nodes = c.nodes
	c, plan = next, []uint64{1, 4, 5}
	hear(t, c, now, ReplicaReport{ID: 5, ConfIndex: 9, Voters: plan})
	hear(t, c, now, ReplicaReport{ID: 1, ConfIndex: 8, Joint: true, Voters: plan})
	if d := deletes(c, true); len(d) != 0 {
		t.Fatalf("C is deleted while A applied only the next plan's joint configuration: %v", d)
	}
	hear(t, c, now, ReplicaReport{ID: 1, ConfIndex: 9, Voters: plan})
	hear(t, c, now, ReplicaReport{ID: 4, ConfIndex: 11, Voters: []uint64{1, 4, 5}})
	if d := deletes(c, true); !slices.Equal(d, []ReplicaRef{{Group: 5, ID: 3}}) {
		t.Fatalf("once the next plan is complete, the leader deletes %v, want C", d)
	}
}

// beat is how often each node heartbeats in these tests.
const beat = core.Tick(1_000_000)

// A, B and C run group 5 and the plan swaps D in for C, the shard map's plan the Control holds. The
// one of A and B that does not lead hears nothing for 10 heartbeats once it applied the joint
// configuration, while the others commit the leave without it. On every tick each replica's report
// goes to the Control, which names C for deletion only once A, B and D applied the leave and C
// learned its removal, and does within a few heartbeats of the lagging voter hearing again.
func TestShardReportsDeleteTheDroppedReplicaOnceThePlanIsComplete(t *testing.T) {
	c := newGroup(t, 4, false)
	for i := range 4 {
		c.n[i].st = bootStorage(t, raftpb.ConfState{Voters: []uint64{1, 2, 3}})
		if i == 3 {
			c.n[i].st = raftshard.NewStorage()
		}
		c.start(i)
	}
	c.cut[2] = true
	l := c.elect()
	c.cut[2] = false
	ctl := deleting(1, 2, 4)
	var plan []raftshard.Peer
	for _, v := range ctl.smap.Groups[0].Voters {
		plan = append(plan, raftshard.Peer{ID: v.ID, Node: core.NodeID{Node: uint32(v.ID) - 1}})
	}
	c.n[l].s.Raft().SetPlan(plan)
	lag, until := 1-l, -1
	c.drop = func(e envelope) bool { return e.to == lag && until != 0 && len(c.conf(lag).VotersOutgoing) != 0 }
	for tick := range 50 * raftshard.HeartbeatTick {
		c.tick()
		switch {
		case until < 0 && len(c.conf(lag).VotersOutgoing) != 0:
			until = tick + 10*raftshard.HeartbeatTick
		case tick == until:
			until = 0
		}
		for i, r := range c.n {
			rp := r.s.Report(nil)
			if err := ctl.Heartbeat(c.now, &NodeReport{Node: uint32(i) + 1, Replicas: []ReplicaReport{rp}}, &ControlOutput{}); err != nil {
				t.Fatal(err)
			}
		}
		if len(ctl.Deletes(3, true, nil)) == 0 {
			continue
		}
		if until < 0 {
			t.Fatalf("at tick %d C is deleted before replica %d applied the joint configuration", tick, lag+1)
		}
		for _, i := range []int{0, 1, 3} {
			if cs := c.conf(i); len(cs.VotersOutgoing) != 0 || !slices.Equal(cs.Voters, []uint64{1, 2, 4}) {
				t.Fatalf("at tick %d C is deleted while replica %d applied %+v", tick, i+1, cs)
			}
		}
		if removed, _ := c.n[2].s.Raft().Removed(); !removed {
			t.Fatalf("at tick %d C is deleted before it learned its removal", tick)
		}
		return
	}
	t.Fatal("C was never deleted")
}

func (c *group) conf(i int) raftpb.ConfState {
	_, cs := c.n[i].s.Raft().Applied()
	return cs
}
