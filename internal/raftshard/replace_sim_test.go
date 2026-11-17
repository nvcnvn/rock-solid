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
// learn that it was removed. With outgoing, the leader that proposes the swap, once the cut-off
// replica is reconnected, hands that replica its leadership, and every MsgTimeoutNow the cut-off
// replica sends is lost, so it cannot hand its leadership on. With wedge, a leader that is the
// first replica to apply the plan's configuration while the cut-off replica is reconnected sends
// the messages carrying that commit, and every later one, to the cut-off replica alone, until the
// faults stop; once that replica learned its removal, the leader halts, or with die, its node
// stands still for good. The run stands in for the control plane: once
// every planned voter applied the plan's configuration, the node of the cut-off replica, if that
// replica learned its removal, deletes it.
type replacement struct {
	crash, pauseD, early, cutLeader, outgoing, wedge, die bool
	cutAt, planDelay, healDelay                           core.Tick
	dead                                                  int
	planAt                                                core.Tick
	plan                                                  []Peer
	swappedAt                                             core.Tick
	healed, deleted                                       bool
	// wedged is the leader the wedge fault took, or -1, wedgeLeave the index of the leave it
	// applied, and stopped whether it halted or stood still.
	wedged     int
	wedgeLeave uint64
	stopped    bool
	// noLeader reports a run in which no replica led having applied the plan's configuration
	// within newLeaderBound of the faults stopping, and planLed whether one did.
	noLeader error
	planLed  bool
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
	// swapBy is the leader that proposed the swap, or -1, handed whether it handed its leadership
	// on, and leadRemoved whether the cut-off replica learned its removal leading.
	swapBy      int
	handed      bool
	leadRemoved bool
	// With regress, the first old voter that runs, follows and applied the joint configuration
	// while holding the leave beyond its commit index crashes, regressed, at regressAt; until
	// 10 s on it reaches only the cut-off replica, which reaches only it, and the leader hands its
	// leadership to the fourth replica once it applied the leave.
	regress   bool
	regressed int
	regressAt core.Tick
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
	return newCluster(seed, p, set, &replacement{crash: crash, pauseD: pauseD, dead: -1, swapBy: -1, regressed: -1, wedged: -1,
		cutAt: 2_000_000 + core.Tick(rng.Below(2_000_001)), planDelay: core.Tick(rng.Below(2_000_001)),
		early: rng.Chance(1, 2), healDelay: core.Tick(rng.Below(1_000_001)), cutLeader: crash && rng.Chance(1, 2), outgoing: rng.Chance(1, 2),
		wedge: rng.Chance(1, 2), die: rng.Chance(1, 2)}, nil)
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
	case !p.healed && p.regressed < 0 && (p.early && p.plan != nil && now >= p.planAt+p.healDelay || p.swappedAt != 0 && now >= p.swappedAt+1_000_000):
		p.healed = true
		for x := range uint32(len(c.reps)) {
			c.s.Heal(uint32(p.dead), x)
		}
	}
	if p.swapBy >= 0 && p.healed && !p.handed {
		p.handed = true
		if nd := c.s.Node(uint32(p.swapBy)); c.reps[p.swapBy].leads && nd.Up() && !nd.Paused() {
			c.transfer(p.swapBy, uint64(p.dead)+1)
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
	if p.regress {
		c.regress()
	}
	if p.wedged >= 0 && !p.stopped && c.reps[p.dead].s.removed {
		p.stopped = true
		if nd := c.s.Node(uint32(p.wedged)); p.die && nd.Up() && !nd.Paused() {
			c.s.Pause(uint32(p.wedged), clock.Suspend, 100_000_000, true)
		} else {
			c.reps[p.wedged].s.Halt()
		}
	}
	if p.plan != nil && !p.deleted && c.reps[p.dead].s.removed && !slices.ContainsFunc(p.plan, func(v Peer) bool { return !p.holds(c.reps[v.ID-1]) }) {
		d := c.reps[p.dead]
		p.deleted, d.deleted, d.leads = true, true, false
	}
	if c.quietAt != 0 && p.plan != nil && !p.planLed {
		if slices.ContainsFunc(c.reps, func(r *simReplica) bool {
			nd := c.s.Node(uint32(r.id))
			return r.leads && nd.Up() && !nd.Paused() && !r.s.halted && p.holds(r)
		}) {
			p.planLed = true
		} else if now > c.quietAt+newLeaderBound && p.noLeader == nil {
			p.noLeader = fmt.Errorf("no replica led having applied the plan's configuration within %v of the faults stopping", newLeaderBound.Duration())
		}
	}
}

// holds reports whether r applied the plan's configuration.
func (p *replacement) holds(r *simReplica) bool {
	return p.plan != nil && len(r.s.conf.VotersOutgoing) == 0 && slices.Equal(sorted(r.s.conf.Voters), planned(p.plan))
}

// take has the wedge fault take r if it is an old voter that leads and is the first replica to
// apply the plan's configuration while the cut-off replica is reconnected: the run ends only once
// the fourth replica applies a value.
func (p *replacement) take(r *simReplica) {
	if p.wedge && p.wedged < 0 && p.healed && p.swappedAt == 0 && r.id != p.dead && r.id != simNodes && p.holds(r) && r.s.rn.BasicStatus().RaftState == raft.StateLeader {
		p.wedged, p.wedgeLeave = r.id, r.s.confAt
	}
}

// regress moves the regress fault on.
func (c *simCluster) regress() {
	p := c.rep
	now := c.s.Time()
	switch l := c.leader(); {
	case p.regressed < 0 && p.plan != nil && l >= 0:
		for id, r := range c.reps[:simNodes] {
			if id == p.dead || r.leads || !c.s.Node(uint32(id)).Up() || len(r.s.conf.VotersOutgoing) == 0 || !holdsConf(c.storage[id], r.s.rn.BasicStatus().Commit) {
				continue
			}
			c.s.CrashRandom(uint32(id))
			p.regressed, p.regressAt = id, now
			for x := range uint32(len(c.reps)) {
				if x != uint32(p.dead) && x != uint32(id) {
					c.s.Partition(uint32(id), x, false)
				}
			}
			break
		}
	case p.regressed >= 0 && !p.handed && l >= 0 && l != simNodes && p.holds(c.reps[l]):
		p.handed = true
		c.transfer(l, simNodes+1)
	case p.regressed >= 0 && !p.healed && now >= p.regressAt+10_000_000:
		p.healed = true
		for a := range uint32(len(c.reps)) {
			for b := range uint32(len(c.reps)) {
				c.s.Heal(a, b)
			}
		}
	}
	if p.regressed >= 0 && !p.healed {
		c.s.Heal(uint32(p.dead), uint32(p.regressed))
	}
}

// holdsConf reports whether st holds a configuration change past commit.
func holdsConf(st *Storage, commit uint64) bool {
	for i := max(commit, st.base.Index) + 1; i <= st.last(); i++ {
		if st.entry(i).Type != raftpb.EntryNormal {
			return true
		}
	}
	return false
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
			if p.outgoing && p.swapBy < 0 && len(cc.Changes) == 2 {
				p.swapBy = r.id
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
		p.leadRemoved = p.leadRemoved || r.id == p.dead && st.RaftState == raft.StateLeader
	}
	if p.swappedAt == 0 && p.holds(r) {
		p.swappedAt, p.sinceSwap = now, c.next
		p.gap = max(p.gap, p.swappedAt-p.last)
	}
	stor := c.storage[r.id]
	if first, _ := stor.FirstIndex(); r.s.applied >= first+20 && !r.s.removed {
		if err := stor.Compact(r.s.Applied()); err != nil {
			panic(err)
		}
	}
}

// lost reports whether a, sent by r, is lost to a fault: under the outgoing fault, a MsgTimeoutNow
// of the cut-off replica once it learned its removal, and under the wedge fault, until the faults
// stop, a raft message from the wedged leader to any but the cut-off replica carrying the commit
// of the leave it applied.
func (c *simCluster) lost(ctx *core.Context, r *simReplica, a *core.Action) bool {
	p := c.rep
	if r.id != p.dead && (r.id != p.wedged || c.quietAt != 0) || a.Kind != core.ActNetSend || a.Msg.Buf == core.NoBuf && le.Uint64(a.Msg.Body[1:]) == leaseGroup {
		return false
	}
	var m raftpb.Message
	if _, kind, err := decode(ctx.Pool, &a.Msg, &m, &fortify{}); err != nil || kind != kindRaft {
		return false
	}
	if r.id == p.wedged {
		return a.Peer.Node != uint32(p.dead) && m.Commit >= p.wedgeLeave
	}
	return p.outgoing && r.s.removed && m.Type == raftpb.MsgTimeoutNow
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
// Seeds that found bugs: 6389, a lease-reading leader behind a follower that applied first (the run
// has since changed and no longer reaches it), and only a leader's applies count against it; 8218, a leader whose configuration change of an
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
	c.rep.outgoing, c.rep.wedge = false, false
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

// outgoingCluster is seed's replacement run with the outgoing fault.
func outgoingCluster(seed uint64, p plants) *simCluster {
	c := newReplaceCluster(seed, p, faults(), true, false)
	c.rep.outgoing = true
	return c
}

// The voter model's neg-no-support-ask: followers that never ask the node of the leader they
// support whether its replica still exists keep supporting it once it learned its removal leading,
// could not hand its leadership on, and was deleted when the plan completed, so no follower leads
// again; the same seed finishes when they ask.
func TestFollowersThatNeverAskKeepARemovedLeader(t *testing.T) {
	for seed := range uint64(20) {
		c := outgoingCluster(seed, plants{noSupportAsk: true})
		if !c.breaks(t, seed, invNewLeader) || !c.rep.leadRemoved {
			continue
		}
		if !outgoingCluster(seed, plants{}).run(t, seed).done {
			t.Fatalf("seed %d: the run does not finish when followers ask", seed)
		}
		return
	}
	t.Fatal("20 seeds whose followers never ask all elected a leader after a removed leader was deleted")
}

// wedgeCluster is seed's replacement run with the wedge fault's leader standing still: a halted
// one's heartbeats tell the others the leave committed once the faults stop, so only a dead one
// leaves the outgoing half needing the cut-off replica for good.
func wedgeCluster(seed uint64, p plants) *simCluster {
	c := newReplaceCluster(seed, p, faults(), true, false)
	c.rep.wedge, c.rep.die = true, true
	return c
}

// noLeaderWithout reports whether, on one of n seeds of wedgeCluster, planting p leaves no leader
// of the plan's configuration, failing the test unless the same seed finishes with p off.
func noLeaderWithout(t *testing.T, n uint64, p plants) bool {
	for seed := range n {
		if !wedgeCluster(seed, p).breaks(t, seed, invNewLeader) {
			continue
		}
		if !wedgeCluster(seed, plants{}).run(t, seed).done {
			t.Fatalf("seed %d: the run does not finish with the plant off", seed)
		}
		return true
	}
	return false
}

// The voter model's neg-removed-deletes-at-once: a cut-off replica that stops once it learns its
// removal, as if its node deleted it, leaves the outgoing half one voter when the wedge fault's
// leader told it alone that the leave committed, and no voter of the plan leads again.
func TestARemovedVoterDeletedAtOnceLeavesNoLeader(t *testing.T) {
	if !noLeaderWithout(t, 40, plants{deletesAtOnce: true}) {
		t.Fatal("40 seeds deleting a removed voter at once all elected a leader of the plan's configuration")
	}
}

// The voter model's neg-removed-applies-removal: a cut-off replica that applies its removal
// campaigns no more, so once the wedge fault's leader appended to it alone it holds the outgoing
// half's newest entry and refuses every voter of the plan its vote.
func TestARemovedVoterThatAppliesItsRemovalLeavesNoLeader(t *testing.T) {
	if !noLeaderWithout(t, 40, plants{appliesRemoval: true}) {
		t.Fatal("40 seeds applying a removed voter's removal all elected a leader of the plan's configuration")
	}
}

// The voter model's neg-halted-silent: a halted leader whose hand-off fails steps down on its
// target's later term and never campaigns again, and a follower that still supports it, its node
// renewing the grant, refuses every candidate unless that node answers that the replica is gone.
// Seeds 770 and 1388 found it.
func TestAHaltedLeaderThatStepsDownAndKeepsSilentLeavesNoLeader(t *testing.T) {
	for _, seed := range []uint64{770, 1388} {
		if !newReplaceCluster(seed, plants{haltedSilent: true}, faults(), true, false).breaks(t, seed, invNewLeader) {
			t.Fatalf("seed %d: a silent halted ex-leader still left a leader of the plan's configuration", seed)
		}
		if !newReplaceCluster(seed, plants{}, faults(), true, false).run(t, seed).done {
			t.Fatalf("seed %d: the run does not finish when the halted ex-leader's node answers", seed)
		}
	}
}

// regressCluster is seed's replacement run with no fault but the regress one.
func regressCluster(seed uint64, p plants) *simCluster {
	c := newReplaceCluster(seed, p, sim.DefaultSettings(), false, false)
	c.rep.outgoing, c.rep.wedge, c.rep.early, c.rep.regress = false, false, false, true
	return c
}

// Under the regress fault a voter that restarts once the joint configuration committed, holding
// the leave, restarts in that configuration and wins no term on its vote and the cut-off replica's.
func TestAVoterRestartedMidSwapKeepsTheConfigurationItKnewCommitted(t *testing.T) {
	seeds.Run(t, 0, 20, func(seed uint64) {
		if c := regressCluster(seed, plants{}).run(t, seed); !c.done || c.rep.regressed < 0 || !c.rep.handed {
			t.Fatalf("seed %d: regressed %d, handed %v, swapped at %d, removed %v, the new replica caught up %v", seed, c.rep.regressed, c.rep.handed, c.rep.swappedAt, c.rep.removed, c.rep.caughtUp)
		}
	})
}

// The voter model's neg-hard-state-old: with rounds that write only their own Ready's hard state,
// and none for a Ready that commits a configuration change, the regressed voter restarts in the
// configuration before the swap and wins the term the fourth replica won.
func TestRoundsThatLeaveTheCommitOfASwapBehindElectTwoLeaders(t *testing.T) {
	for seed := range uint64(5) {
		c := regressCluster(seed, plants{oldHardState: true})
		c.inv.Only(invOneLeader)
		for c.s.Time() < simRunFor && c.s.Step() {
			if c.inv.Check() != nil {
				return
			}
			c.after(t, seed)
		}
	}
	t.Fatal("5 seeds whose rounds leave the commit of a swap behind broke no one-leader invariant")
}

// The voter model's neg-conf-commit-not-durable: a replica that applies a configuration change
// before its commit index is durable would restart in the configuration before it.
func TestAConfigurationChangeAppliedBeforeItsCommitIsDurableIsForgotten(t *testing.T) {
	for seed := range uint64(5) {
		c := newReplaceCluster(seed, plants{noConfRound: true}, faults(), false, false)
		c.inv.Only(invDurableCommit)
		for c.s.Time() < simRunFor && c.s.Step() {
			c.pause()
			if c.inv.Check() != nil {
				return
			}
			c.after(t, seed)
		}
	}
	t.Fatal("5 seeds whose replicas apply configuration changes before their commit is durable broke no durable-commit invariant")
}
