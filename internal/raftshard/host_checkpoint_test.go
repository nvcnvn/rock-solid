package raftshard

import (
	"testing"

	"go.etcd.io/raft/v3/raftpb"
)

// commit writes owner's round and, once it is durable, takes it into st as the shard does; it
// returns the durable LSN before the round, the LSN its record follows.
func (r *rig) commit(st *Storage, owner uint32, flush uint64, rd Round) uint64 {
	r.t.Helper()
	before := r.host.Log.DurableLSN()
	r.write(owner, flush, &rd)
	r.run()
	st.Append(rd.Entries)
	if rd.HardState != (raftpb.HardState{}) {
		st.SetHardState(rd.HardState)
	}
	st.removed = st.removed || rd.Removed
	if d := r.takeDone(); len(d) != 1 || d[0].Round != flush {
		r.t.Fatalf("owner %d round %d: done %v", owner, flush, d)
	}
	return before
}

// roll writes and removes a shard of its own until the log opens a segment, whose header names
// the newest manifest, so that a restart loads it.
func (r *rig) roll() {
	r.t.Helper()
	r.host.Add(99, NewStorage())
	for i := uint64(1); i <= 40; i++ {
		r.write(99, i, &Round{Entries: entries(i, 1, 1, 8<<10)})
		r.run()
	}
	r.takeDone()
	r.host.Remove(99)
}

func (r *rig) replayLSN(owner uint32) uint64 {
	r.t.Helper()
	for _, ix := range r.host.Indexes(nil, r.host.Log.DurableLSN()) {
		if ix.Owner == owner {
			return ix.ReplayLSN
		}
	}
	r.t.Fatalf("owner %d not in the checkpoint's indexes", owner)
	return 0
}

// A shard's replay LSN is the LSN before its oldest round a restart needs, moving past a round
// once a compaction and a newer hard state make it dead, and before a blob still in flight.
func TestAShardsReplayLSNIsBeforeItsOldestLiveRound(t *testing.T) {
	r := newRig(t)
	st := NewStorage()
	r.host.Add(5, st)
	first := r.commit(st, 5, 1, Round{Entries: entries(1, 2, 1, 100), HardState: raftpb.HardState{Term: 1, Vote: 1}})
	second := r.commit(st, 5, 2, Round{Entries: entries(3, 1, 1, 100), HardState: raftpb.HardState{Term: 1, Vote: 1, Commit: 3}})
	if got := r.replayLSN(5); got != first {
		t.Fatalf("replay LSN %d, want %d before the first round", got, first)
	}
	if err := r.host.Compact(5, 2, raftpb.ConfState{Voters: []uint64{1}}); err != nil {
		t.Fatal(err)
	}
	if got := r.replayLSN(5); got != second {
		t.Fatalf("with the first round dead the replay LSN is %d, want %d before the second", got, second)
	}
	idle := NewStorage()
	r.host.Add(6, idle)
	durable := r.host.Log.DurableLSN()
	if got := r.replayLSN(6); got != durable {
		t.Fatalf("a shard with no round has replay LSN %d, want the durable %d", got, durable)
	}
	r.write(6, 1, &Round{Entries: entries(1, 1, 1, 100)})
	if got := r.host.Indexes(nil, durable+5)[1].ReplayLSN; got != durable {
		t.Fatalf("a shard with a round in flight has replay LSN %d, want %d, the durable LSN at its submit", got, durable)
	}
}

// A checkpoint names every shard, so its manifest's replay floor stays before the oldest round a
// restart needs; the restart replays each shard from its own replay LSN on the base its runs
// restore, skipping the dead rounds between the floor and it.
func TestACheckpointKeepsTheFloorAtTheOldestLiveRound(t *testing.T) {
	r := newRig(t)
	busy, idle := NewStorage(), NewStorage()
	r.host.Add(5, busy)
	r.host.Add(6, idle)
	held := r.commit(idle, 6, 1, Round{Entries: entries(1, 1, 1, 100), HardState: raftpb.HardState{Term: 1, Vote: 6, Commit: 1}})
	r.commit(busy, 5, 1, Round{Entries: entries(1, 2, 1, 100), HardState: raftpb.HardState{Term: 1, Vote: 1}})
	live := r.commit(busy, 5, 2, Round{Entries: entries(3, 2, 1, 100), HardState: raftpb.HardState{Term: 1, Vote: 1, Commit: 4}})
	if err := r.host.Compact(5, 2, raftpb.ConfState{Voters: []uint64{1}}); err != nil {
		t.Fatal(err)
	}
	if floor := r.checkpoint(); floor != held {
		t.Fatalf("the manifest's replay floor is %d, want %d before the idle shard's round", floor, held)
	}
	r.roll()
	refs := map[uint32]int{5: len(r.host.shard(5).refs), 6: len(r.host.shard(6).refs)}
	r.bases = map[uint32]raftpb.SnapshotMetadata{5: busy.Base()}
	sts := r.crash(5, 6)
	for o, want := range map[uint32]*Storage{5: busy, 6: idle} {
		got := sts[o]
		gl, _ := got.LastIndex()
		wl, _ := want.LastIndex()
		if got.HardState() != want.HardState() || gl != wl || got.Base().Index != want.Base().Index {
			t.Fatalf("owner %d recovered hard state %+v, last %d, base %d; want %+v, %d, %d", o, got.HardState(), gl, got.Base().Index, want.HardState(), wl, want.Base().Index)
		}
		if n := len(r.host.shard(o).refs); n != refs[o] {
			t.Fatalf("owner %d holds %d blobs after the restart, want %d: a dead round before its replay LSN came back", o, n, refs[o])
		}
	}
	if got := r.replayLSN(5); got != live {
		t.Fatalf("after the restart owner 5's replay LSN is %d, want %d", got, live)
	}
}

// An idle shard's newest hard state would hold the floor back for good: a checkpoint writes it
// again, with the replica's removal, and the next checkpoint's floor passes the old round, from
// which a restart recovers the same hard state and removal.
func TestACheckpointRewritesAnIdleShardsHardState(t *testing.T) {
	r := newRig(t)
	busy, idle := NewStorage(), NewStorage()
	r.host.Add(5, busy)
	r.host.Add(6, idle)
	hs := raftpb.HardState{Term: 2, Vote: 3, Commit: 2}
	held := r.commit(idle, 6, 1, Round{Entries: entries(1, 2, 2, 100), HardState: hs, Removed: true})
	if err := r.host.Compact(6, 2, raftpb.ConfState{Voters: []uint64{3}}); err != nil {
		t.Fatal(err)
	}
	r.commit(busy, 5, 1, Round{Entries: entries(1, 1, 1, 100), HardState: raftpb.HardState{Term: 1, Vote: 1, Commit: 1}})
	base := r.host.Log.DurableLSN()
	if floor := r.checkpoint(); floor != held {
		t.Fatalf("the first manifest's floor is %d, want %d before the idle shard's round", floor, held)
	}
	if err := r.host.Rewrite(&r.ctx, base, &r.out); err != nil {
		t.Fatal(err)
	}
	r.absorb()
	r.run()
	if len(r.takeDone()) != 0 {
		t.Fatal("the host's own round was reported done to the shard")
	}
	if floor := r.checkpoint(); floor <= held {
		t.Fatalf("after the rewrite the floor is %d, still at or before the old round's %d", floor, held)
	}
	r.roll()
	r.bases = map[uint32]raftpb.SnapshotMetadata{6: idle.Base()}
	sts := r.crashCleaned(5, 6)
	if got := sts[6]; got.HardState() != hs || !got.Removed() {
		t.Fatalf("the idle shard recovered hard state %+v, removed %v; want %+v, removed", got.HardState(), got.Removed(), hs)
	}
}

// A shard with a round in flight is left to that round, whose hard state is newer; one whose
// oldest live round holds entries a compaction has not reached is not rewritten either.
func TestRewriteLeavesAShardWithARoundInFlightOrLiveEntries(t *testing.T) {
	r := newRig(t)
	st, other := NewStorage(), NewStorage()
	r.host.Add(5, st)
	r.host.Add(6, other)
	r.commit(st, 5, 1, Round{Entries: entries(1, 1, 1, 100), HardState: raftpb.HardState{Term: 1, Vote: 1, Commit: 1}})
	if err := r.host.Compact(5, 1, raftpb.ConfState{Voters: []uint64{1}}); err != nil {
		t.Fatal(err)
	}
	r.commit(other, 6, 1, Round{Entries: entries(1, 1, 1, 100), HardState: raftpb.HardState{Term: 1, Vote: 1, Commit: 1}})
	r.write(5, 2, &Round{HardState: raftpb.HardState{Term: 2, Vote: 1, Commit: 1}})
	n := len(r.host.flights)
	if err := r.host.Rewrite(&r.ctx, r.host.Log.DurableLSN(), &r.out); err != nil {
		t.Fatal(err)
	}
	if len(r.host.flights) != n {
		t.Fatalf("rewrite submitted %d blobs, want none", len(r.host.flights)-n)
	}
}

// An owner whose removed shard's blobs can still replay is given to no shard until a manifest's
// floor passes them, and a restart that replays them as an owner hosted by none keeps it so.
func TestARemovedOwnerIsReusableOnceTheFloorPassesItsBlobs(t *testing.T) {
	r := newRig(t)
	st := NewStorage()
	r.host.Add(5, st)
	r.commit(st, 5, 1, Round{Entries: entries(1, 1, 1, 100), HardState: raftpb.HardState{Term: 1, Vote: 1, Commit: 1}})
	r.host.Remove(5)
	if r.host.Reusable(5) {
		t.Fatal("a removed owner is reusable while its blobs can replay")
	}
	r.crash()
	if r.host.Reusable(5) {
		t.Fatal("after a restart that replayed its blobs a removed owner is reusable")
	}
	r.checkpoint()
	if !r.host.Reusable(5) {
		t.Fatal("a removed owner whose blobs lie before the floor is not reusable")
	}
	r.host.Add(5, NewStorage())
}

// A shard hosted on a restored base takes its compaction index from it, so a replayed round the
// base covers is dead after the restart, as it was before, and one past the base stays live.
func TestARestartTakesTheCompactionIndexFromTheBase(t *testing.T) {
	r := newRig(t)
	st := NewStorage()
	r.host.Add(5, st)
	r.commit(st, 5, 1, Round{Entries: entries(1, 2, 1, 100), HardState: raftpb.HardState{Term: 1, Vote: 1}})
	r.commit(st, 5, 2, Round{Entries: entries(3, 1, 1, 100)})
	r.commit(st, 5, 3, Round{Entries: entries(4, 1, 1, 100), HardState: raftpb.HardState{Term: 1, Vote: 1, Commit: 4}})
	if err := r.host.Compact(5, 3, raftpb.ConfState{Voters: []uint64{1}}); err != nil {
		t.Fatal(err)
	}
	want := len(r.host.shard(5).refs)
	r.bases = map[uint32]raftpb.SnapshotMetadata{5: st.Base()}
	r.crash(5)
	if hd := r.host.shard(5); hd.compacted != 3 || len(hd.refs) != want {
		t.Fatalf("after the restart the host's compaction index is %d and it holds %d blobs; want 3 and %d", hd.compacted, len(hd.refs), want)
	}
}
