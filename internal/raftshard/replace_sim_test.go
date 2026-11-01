package raftshard

import (
	"fmt"
	"slices"
	"testing"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/clock"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

const (
	invCaughtUp    = "no learner is promoted to voter before it has caught up with its leader's log"
	invOnlyRemoved = "only a replica the group's configuration removed deletes its state"
	// stallBound is the longest a replacement may hold up commits between two voters of the old configuration that run.
	stallBound = core.Tick(100_000)
)

// replacement is one voter replacement in a simulated run: from cutAt, once one of the first three
// nodes leads, a follower among them, or with cutLeader the leader, is cut off from every other
// node, planDelay later the shard map's plan names the fourth node's replica in its place, and the
// cut-off replica is reconnected, in half the runs up to a second after the plan, mid-swap, and
// otherwise a second after a replica applies the configuration without it; either way it must
// learn that it was removed.
type replacement struct {
	crash, pauseD, early, cutLeader bool
	cutAt, planDelay, healDelay     core.Tick
	dead                            int
	planAt                          core.Tick
	plan                            []Peer
	swappedAt                       core.Tick
	healed                          bool
	// removed is whether the cut-off replica reported itself removed, sinceSwap the first value
	// proposed after the swap, and caughtUp whether the fourth replica applied one from then on.
	removed   bool
	sinceSwap uint64
	caughtUp  bool
	// top is the highest index a running old voter applied since the plan, last when, and gap the longest wait between two such indexes before the swap.
	top       uint64
	last, gap core.Tick
	// commits is, per replica, its commit index after each pass of the last two heartbeats.
	commits [simNodes + 1][]commitAt
}

type commitAt struct {
	at     core.Tick
	commit uint64
}

// committedBy is replica id's commit index at t, or 0 before its history.
func (p *replacement) committedBy(id int, t core.Tick) uint64 {
	var c uint64
	for _, h := range p.commits[id] {
		if h.at <= t {
			c = h.commit
		}
	}
	return c
}

// faults are newSimCluster's scheduler settings.
func faults() sim.Settings {
	set := sim.DefaultSettings()
	set.NetJitter = 200
	set.DropNum, set.DropDen = 1, 100
	set.DupNum, set.DupDen = 1, 50
	set.PartitionNum, set.PartitionDen = 1, 3000
	set.ClockNum, set.ClockDen = 1, 2000
	return set
}

// newReplaceCluster runs a replacement cut 2 to 4 s in and planned up to 2 s later; with crash the first leader to apply crashAt values also crashes, and with pauseD the fourth node is suspended for 2 s once the plan names it.
func newReplaceCluster(seed uint64, p plants, set sim.Settings, crash, pauseD bool) *simCluster {
	rng := core.NewRand(seed ^ 0x7e91ace)
	return newCluster(seed, p, set, &replacement{crash: crash, pauseD: pauseD, dead: -1,
		cutAt: 2_000_000 + core.Tick(rng.Below(2_000_001)), planDelay: core.Tick(rng.Below(2_000_001)),
		early: rng.Chance(1, 2), healDelay: core.Tick(rng.Below(1_000_001)), cutLeader: crash && rng.Chance(1, 2)})
}

// replace moves the replacement on after a step: cut a follower off, hand every replica the plan, reconnect the removed one.
func (c *simCluster) replace() {
	p := c.rep
	if p == nil {
		return
	}
	now := c.s.Time()
	switch {
	case p.dead < 0 && now >= p.cutAt && slices.ContainsFunc(c.reps[:simNodes], func(r *simReplica) bool { return r != nil && r.leads }):
		p.dead = slices.IndexFunc(c.reps[:simNodes], func(r *simReplica) bool { return r.leads == p.cutLeader })
		p.planAt = now + p.planDelay
	case p.dead >= 0 && p.plan == nil && now >= p.planAt:
		for i := range simNodes {
			if i != p.dead {
				p.plan = append(p.plan, Peer{ID: uint64(i) + 1, Node: core.NodeID{Node: uint32(i)}})
			}
		}
		p.plan = append(p.plan, Peer{ID: simNodes + 1, Node: core.NodeID{Node: simNodes}})
		for _, r := range c.reps {
			if r != nil && r.s != nil {
				r.s.SetPlan(p.plan)
			}
		}
		p.last = now
		if nd := c.s.Node(simNodes); p.pauseD && nd.Up() && !nd.Paused() {
			c.s.Pause(simNodes, clock.Suspend, 2_000_000, false)
		}
	case !p.healed && (p.early && p.plan != nil && now >= p.planAt+p.healDelay || p.swappedAt != 0 && now >= p.swappedAt+1_000_000):
		p.healed = true
		for x := range uint32(len(c.reps)) {
			c.s.Heal(uint32(p.dead), x)
		}
	}
	if p.dead >= 0 && !p.healed {
		for x := range uint32(len(c.reps)) {
			if x != uint32(p.dead) {
				c.s.Heal(uint32(p.dead), x)
				c.s.Partition(uint32(p.dead), x, false)
			}
		}
	}
}

// applied records that replica r applied value v at index.
func (c *simCluster) applied(r *simReplica, index, v uint64) {
	p := c.rep
	if p == nil {
		return
	}
	if r.id == simNodes && p.sinceSwap != 0 && v >= p.sinceSwap {
		p.caughtUp = true
	}
	if now := c.s.Time(); p.plan != nil && p.swappedAt == 0 && r.id != p.dead && index > p.top {
		p.top, p.gap, p.last = index, max(p.gap, now-p.last), now
	}
}

// replaced checks a pass of r against the replacement: a learner it promotes holds, durably, every
// entry r had committed a heartbeat earlier; only the cut-off replica reports itself removed. It records when the plan's
// configuration first applies, and has r's storage take a snapshot at its applied index, as a
// point-in-time view gives one on demand, and compact all but 10 entries below it.
func (c *simCluster) replaced(r *simReplica, st *raft.BasicStatus) {
	p := c.rep
	now := c.s.Time()
	h := &p.commits[r.id]
	*h = append(*h, commitAt{at: now, commit: st.Commit})
	for len(*h) > 1 && (*h)[1].at <= now-2*HeartbeatTick*TickInterval {
		*h = (*h)[1:]
	}
	if r.out.Flush != 0 && st.RaftState == raft.StateLeader && c.promoted == nil {
		for _, e := range r.s.rd.Entries {
			var cc raftpb.ConfChangeV2
			if e.Type != raftpb.EntryConfChangeV2 || cc.Unmarshal(e.Data) != nil {
				continue
			}
			for _, ch := range cc.Changes {
				if ch.Type != raftpb.ConfChangeAddNode || !slices.Contains(r.s.conf.Learners, ch.NodeID) {
					continue
				}
				if last, _ := c.storage[ch.NodeID-1].LastIndex(); last < p.committedBy(r.id, now-HeartbeatTick*TickInterval) {
					c.promoted = fmt.Errorf("replica %d promoted learner %d at index %d holding up to %d, %d committed a heartbeat earlier", r.id, ch.NodeID, e.Index, last, p.committedBy(r.id, now-HeartbeatTick*TickInterval))
				}
			}
		}
	}
	if r.out.Removed {
		if r.id != p.dead && c.removed == nil {
			c.removed = fmt.Errorf("replica %d, never removed, reported itself removed", r.id)
		}
		p.removed = p.removed || r.id == p.dead
	}
	if p.plan != nil && p.swappedAt == 0 && len(r.s.conf.VotersOutgoing) == 0 && slices.Equal(sorted(r.s.conf.Voters), planned(p.plan)) {
		p.swappedAt, p.sinceSwap = now, c.next
		p.gap = max(p.gap, p.swappedAt-p.last)
	}
	stor := c.storage[r.id]
	if snap, _ := stor.Snapshot(); r.s.applied > snap.Metadata.Index && !r.s.removed {
		if _, err := stor.CreateSnapshot(r.s.applied, &r.s.conf, le.AppendUint64(nil, uint64(r.applied))); err != nil {
			panic(err)
		}
		if first, _ := stor.FirstIndex(); r.s.applied >= first+20 {
			if err := stor.Compact(r.s.applied - 10); err != nil {
				panic(err)
			}
		}
	}
}

func planned(plan []Peer) []uint64 {
	var ids []uint64
	for _, p := range plan {
		ids = append(ids, p.ID)
	}
	return sorted(ids)
}

// Under the Raft runs' faults, with the first leader crashed, a follower's replacement adds the
// fourth replica as a learner, catches it up from snapshots of compacted logs, swaps it in through a
// joint configuration and leaves it; the replaced replica, once reconnected, deletes its state.
// Seeds that found bugs: 6389, a lease-reading leader behind a follower that applied first, which
// only a leader's applies now count against; 8218, a leader whose configuration change of an
// earlier term was lost, and which then proposed none again; 1005, a leader the plan dropped
// that waited for a follower to reach its own durable index, which steady writes never allowed.
func TestVoterReplacementRegressionSeeds(t *testing.T) {
	for _, seed := range []uint64{6389, 8218, 1005} {
		if c := newReplaceCluster(seed, plants{}, faults(), true, false).run(t, seed); !c.done {
			t.Fatalf("seed %d: swapped at %d, removed %v, the new replica caught up %v", seed, c.rep.swappedAt, c.rep.removed, c.rep.caughtUp)
		}
	}
}

func TestVoterReplacementRunsReplayFromTheirSeed(t *testing.T) {
	seeds.Run(t, 0, 20, func(seed uint64) {
		a := newReplaceCluster(seed, plants{}, faults(), true, false).run(t, seed)
		if p := a.rep; !a.done {
			t.Fatalf("seed %d: after crash %v, cut %d at %d, planned at %d, swapped at %d, removed %v, the new replica caught up %v", seed, a.afterCrash, p.dead, p.cutAt, p.planAt, p.swappedAt, p.removed, p.caughtUp)
		}
		b := newReplaceCluster(seed, plants{}, faults(), true, false).run(t, seed)
		if err := sim.CompareTraces(a.s.Trace(), b.s.Trace()); err != nil {
			t.Fatalf("seed %d: trace replays differently: %v", seed, err)
		}
		if err := sim.CompareTraces(a.history, b.history); err != nil {
			t.Fatalf("seed %d: applied history replays differently: %v", seed, err)
		}
	})
}

// stallGap runs a replacement with no fault but the cut-off voter and the new replica's node
// suspended for 2 s as the plan names it, and returns the longest the two running old voters went
// without committing between the plan and the swap.
func stallGap(t *testing.T, seed uint64, p plants) core.Tick {
	c := newReplaceCluster(seed, p, sim.DefaultSettings(), false, true)
	for !c.done && c.s.Time() < simRunFor && c.s.Step() {
		c.after(t, seed)
	}
	if !c.done {
		t.Fatalf("seed %d: swapped at %d, removed %v, the new replica caught up %v", seed, c.rep.swappedAt, c.rep.removed, c.rep.caughtUp)
	}
	return c.rep.gap
}

func TestAReplacementNeverStallsProposals(t *testing.T) {
	seeds.Run(t, 0, 20, func(seed uint64) {
		gap := stallGap(t, seed, plants{})
		if gap > stallBound {
			t.Fatalf("seed %d: the old voters went %v without a commit during the replacement", seed, gap.Duration())
		}
	})
}

func TestAddingTheReplacementAsAVoterStallsProposals(t *testing.T) {
	for seed := range uint64(20) {
		if stallGap(t, seed, plants{voterFirst: true}) > stallBound {
			return
		}
	}
	t.Fatal("20 seeds adding the replacement as a voter never stalled proposals")
}

func TestPromotingALearnerBeforeItCaughtUpBreaksTheInvariant(t *testing.T) {
	for seed := range uint64(20) {
		c := newReplaceCluster(seed, plants{eager: true}, sim.DefaultSettings(), false, true)
		c.inv.Only(invCaughtUp)
		for c.s.Time() < simRunFor && c.s.Step() {
			c.replace()
			if c.inv.Check() != nil {
				return
			}
		}
	}
	t.Fatal("20 seeds promoting learners at once broke no invariant")
}
