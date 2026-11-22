package raftshard

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/lease"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
	"github.com/nvcnvn/rock-solid/internal/trace"
)

func (r *simReplica) output(name trace.Name, f [trace.MaxFields]uint64) {
	r.c.s.Node(uint32(r.id)).Output(r.loop, trace.Record{Name: name, Fields: f})
}

// bits is ids as a bit set.
func bits(ids []uint64) uint64 {
	var b uint64
	for _, id := range ids {
		b |= 1 << id
	}
	return b
}

// noteRound records the log change of a round as the replica takes it, which is when etcd-raft's
// log holds it.
func (r *simReplica) noteRound(round *Round) {
	if !r.c.conform {
		return
	}
	r.logged = true
	if s := &round.Snapshot; !raft.IsEmptySnap(*s) {
		cs := &s.Metadata.ConfState
		r.output(trace.OutRaftSnap, [trace.MaxFields]uint64{s.Metadata.Index, s.Metadata.Term, bits(cs.Voters), bits(cs.VotersOutgoing), bits(cs.Learners)})
	}
	for i := range round.Entries {
		e := &round.Entries[i]
		var kind, change uint64
		switch {
		case e.Type == raftpb.EntryConfChangeV2:
			var cc raftpb.ConfChangeV2
			if err := cc.Unmarshal(e.Data); err != nil {
				panic(err)
			}
			kind, change = 2, uint64(len(cc.Changes))
			for j, ch := range cc.Changes {
				change |= (uint64(ch.Type)<<6 | ch.NodeID) << (8 * (j + 1))
			}
		case e.Type != raftpb.EntryNormal:
			panic("conform: a configuration change that is not a ConfChangeV2")
		case len(e.Data) == 0:
			kind = 1
		}
		r.output(trace.OutRaftEntry, [trace.MaxFields]uint64{e.Index, e.Term, kind, change})
	}
}

// noteState records the replica's Raft state when the pass changed it or its log.
func (r *simReplica) noteState(st *raft.BasicStatus) {
	if !r.c.conform {
		return
	}
	var flags uint64
	for i, on := range [...]bool{st.RaftState == raft.StateLeader, r.s.removed, r.s.halted, r.deleted} {
		if on {
			flags |= 1 << i
		}
	}
	f := [trace.MaxFields]uint64{r.s.hs.Term, st.Lead, r.s.hs.Commit, r.s.applied, flags}
	if f != r.state || r.logged {
		r.state, r.logged = f, false
		r.output(trace.OutRaftState, f)
	}
}

// noteSupport records, when the pass changed it, the leader of its term the replica supports now,
// while it leads the peers whose fortification of its term runs now, and why the support it had
// ended.
func (r *simReplica) noteSupport(ctx *core.Context, st *raft.BasicStatus) {
	if !r.c.conform {
		return
	}
	t := lease.Now(ctx)
	var f [trace.MaxFields]uint64
	if st.RaftState != raft.StateLeader && r.s.supports(st, t) {
		f[0] = r.s.leadID
	}
	if st.RaftState == raft.StateLeader {
		for i, p := range r.s.cfg.Peers {
			if p.ID != r.s.cfg.ID && r.s.fortifies(i, st.Term, t) {
				f[1] |= 1 << p.ID
			}
		}
	}
	if f[0] == r.support[0] && f[1] == r.support[1] {
		return
	}
	if r.support[0] != 0 && f[0] != r.support[0] {
		f[2] = r.supportEnd(st)
	}
	if f[0] != 0 {
		r.heldTerm = r.s.support.term
	}
	r.support = f
	r.output(trace.OutRaftSupport, f)
}

// supportEnd is why the support of the leader the replica last noted ended.
func (r *simReplica) supportEnd(st *raft.BasicStatus) uint64 {
	switch vote := r.allowed || r.held && r.s.timeoutTerm == 0; {
	case st.Term == r.heldTerm:
		return trace.SupportInTerm
	case vote && st.Term == r.c.forcedTerm+1:
		return trace.SupportForced
	case vote:
		return trace.SupportVote
	}
	return trace.SupportTerm
}

// noteEvent notes, before the replica takes ev, what a support that ends in this pass may have
// ended on: allowed whether ev is a vote request the support lets through, a transfer target's or
// the supported leader's own, or a MsgTimeoutNow, and held whether a MsgTimeoutNow the shard held
// back may be stepped.
func (r *simReplica) noteEvent(ev *core.Event) {
	r.allowed, r.held = false, r.s != nil && r.s.timeoutTerm != 0
	b := ev.Msg.Inline()
	if !r.c.conform || ev.Kind != core.EvNetRecv || ev.Msg.Buf != core.NoBuf || len(b) <= kindBytes || b[groupBytes] != kindRaft {
		return
	}
	var m raftpb.Message
	if m.Unmarshal(b[kindBytes:]) == nil {
		r.allowed = m.Type == raftpb.MsgTimeoutNow || m.Type == raftpb.MsgVote && (string(m.Context) == transferContext || r.s != nil && m.From == r.s.leadID)
	}
}

// conformWrite keeps a run's trace for scripts/conform.sh.
func conformWrite(t *testing.T, name string, c *simCluster) {
	t.Helper()
	dir := os.Getenv("CONFORM_DIR")
	if dir == "" {
		return
	}
	if err := os.WriteFile(filepath.Join(dir, name+".trace"), c.s.Trace(), 0o644); err != nil {
		t.Fatal(err)
	}
}

// A replacement with no fault but the cut-off voter: the plan's learner joins, catches up from a
// snapshot and is swapped in, the leader leaves the joint configuration, and the cut-off replica,
// reconnected, learns its removal and is deleted.
func TestConformScriptedReplacement(t *testing.T) {
	c := newReplaceCluster(1, plants{}, sim.DefaultSettings(), false, false)
	c.rep.outgoing, c.rep.wedge = false, false
	c.conform = true
	end := simRunFor
	for c.s.Time() < end && c.s.Step() {
		c.after(t, 1)
		if c.done && c.rep.deleted && end == simRunFor {
			end = c.s.Time() + core.Tick(ElectionTick)*TickInterval
		}
	}
	if !c.done || !c.rep.deleted {
		t.Fatalf("swapped at %d, removed %v, deleted %v, the new replica caught up %v", c.rep.swappedAt, c.rep.removed, c.rep.deleted, c.rep.caughtUp)
	}
	conformWrite(t, "voters-scripted-replacement", c)
}

// The replacement run of TestVoterReplacementRunsReplayFromTheirSeed, under the Raft runs' faults
// with its first leader crashed, one trace per seed.
func TestConformReplacementSeeds(t *testing.T) {
	seeds.Run(t, 0, 10, func(seed uint64) {
		c := newReplaceCluster(seed, plants{}, faults(), true, false)
		c.conform = true
		if c.run(t, seed); !c.done {
			t.Fatalf("seed %d: swapped at %d, removed %v, the new replica caught up %v", seed, c.rep.swappedAt, c.rep.removed, c.rep.caughtUp)
		}
		conformWrite(t, fmt.Sprintf("voters-replace-%d", seed), c)
	})
}

// eagerCluster and stallCluster are the runs of the eager and voter-first plants' tests.
func eagerCluster(seed uint64, p plants) *simCluster {
	return newReplaceCluster(seed, p, sim.DefaultSettings(), false, true)
}

func stallCluster(seed uint64, p plants) *simCluster {
	c := eagerCluster(seed, p)
	c.rep.outgoing, c.rep.wedge = false, false
	return c
}

// conformPlants are the voter model's rules with a Go plant, and disloyal for Run's support rule,
// each on the run and seed of its test, run until the invariant named breaks, or to its end for
// one no invariant shows.
var conformPlants = []struct {
	name    string
	p       plants
	cluster func(uint64, plants) *simCluster
	seed    uint64
	inv     string
}{
	{"campaign-unapplied", plants{campaignUnapplied: true}, regressCluster, 0, invOneLeader},
	{"voter-first", plants{voterFirst: true}, stallCluster, 0, ""},
	{"eager", plants{eager: true}, eagerCluster, 0, invCaughtUp},
	{"joint-one-half", plants{jointOneHalf: true}, splitCluster, 0, invOneLeader},
	{"one-at-a-time", plants{oneAtATime: true}, splitCluster, 0, invOneLeader},
	{"delete-on-plan", plants{deletesOnPlan: true}, replaceCluster, 0, invOnlyRemoved},
	{"notice-any-index", plants{noticeAnyIndex: true}, replaceCluster, 0, invOnlyRemoved},
	{"notice-non-member", plants{noticeNonMember: true}, replaceCluster, 0, invOnlyRemoved},
	{"no-support-ask", plants{noSupportAsk: true}, outgoingCluster, 0, invNewLeader},
	{"removed-keeps-lease", plants{removedKeepsLease: true}, outgoingCluster, 0, invOneLeaseReader},
	{"restart-commit-at-base", plants{restartCommitAtBase: true}, regressCluster, 0, invOneLeader},
	{"halted-keeps-leading", plants{haltedKeepsLeading: true}, haltCluster, 1, invNewLeader},
	{"removed-deletes-at-once", plants{deletesAtOnce: true}, wedgeCluster, 0, invNewLeader},
	{"removed-applies-removal", plants{appliesRemoval: true}, wedgeCluster, 0, invNewLeader},
	{"halted-silent", plants{haltedSilent: true}, replaceCluster, 770, invNewLeader},
	{"conf-commit-not-durable", plants{noConfRound: true}, func(seed uint64, p plants) *simCluster {
		return newReplaceCluster(seed, p, faults(), false, false)
	}, 0, invDurableCommit},
	{"hard-state-old", plants{oldHardState: true}, regressCluster, 0, invOneLeader},
	{"disloyal", plants{disloyal: true}, replaceCluster, 0, invOneLeaseReader},
}

// Keeps, for scripts/conform.sh plants, each plant's run on the first seed from its own whose run
// breaks its invariant, to the step it broke, and the same seed's run with no plant, which finishes.
func TestVoterPlantsKeepTraces(t *testing.T) {
	if os.Getenv("CONFORM_DIR") == "" {
		t.Skip("CONFORM_DIR names no directory for the traces")
	}
	for _, x := range conformPlants {
		seed := x.seed
		c := x.cluster(seed, x.p)
		c.conform = true
		for x.inv != "" && !c.breaks(t, seed, x.inv) {
			if seed++; seed == x.seed+40 {
				t.Fatalf("%s: 40 seeds from %d kept %q", x.name, x.seed, x.inv)
			}
			c = x.cluster(seed, x.p)
			c.conform = true
		}
		if x.inv == "" {
			c.run(t, seed)
		}
		conformWrite(t, fmt.Sprintf("voters-plant-%s-%d", x.name, seed), c)
		base := x.cluster(seed, plants{})
		base.conform = true
		if !base.run(t, seed).done {
			t.Fatalf("%s: seed %d does not finish with the plant off", x.name, seed)
		}
		conformWrite(t, fmt.Sprintf("voters-base-%s-%d", x.name, seed), base)
	}
}
