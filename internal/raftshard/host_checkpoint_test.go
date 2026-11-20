package raftshard

import (
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/sim"
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
	st.Took(flush)
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
// again, with the replica's removal and deletion, and the next checkpoint's floor passes the old
// round, from which a restart recovers the same hard state, removal and deletion.
func TestACheckpointRewritesAnIdleShardsHardState(t *testing.T) {
	r := newRig(t)
	busy, idle := NewStorage(), NewStorage()
	r.host.Add(5, busy)
	r.host.Add(6, idle)
	hs := raftpb.HardState{Term: 2, Vote: 3, Commit: 2}
	held := r.commit(idle, 6, 1, Round{Entries: entries(1, 2, 2, 100), HardState: hs, Removed: true, Deleted: true})
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
	if got := sts[6]; got.HardState() != hs || !got.Removed() || !got.Deleted() {
		t.Fatalf("the idle shard recovered hard state %+v, removed %v, deleted %v; want %+v, removed and deleted", got.HardState(), got.Removed(), got.Deleted(), hs)
	}
}

// A shard whose round lies past the LSN a rewrite is given is not rewritten; one with a round in
// flight is left to that round, whose hard state is newer; one whose oldest live round holds
// entries a compaction has not reached is not rewritten either.
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
	rewrite := func(lsn uint64, why string) {
		t.Helper()
		n := len(r.host.flights)
		if err := r.host.Rewrite(&r.ctx, lsn, &r.out); err != nil {
			t.Fatal(err)
		}
		if len(r.host.flights) != n {
			t.Fatalf("rewrite submitted %d blobs %s, want none", len(r.host.flights)-n, why)
		}
	}
	rewrite(r.host.shard(5).refs[0].LSN-1, "for a round past its LSN")
	r.write(5, 2, &Round{HardState: raftpb.HardState{Term: 2, Vote: 1, Commit: 1}})
	rewrite(r.host.Log.DurableLSN(), "with a round in flight")
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
	r.roll()
	if !r.host.Reusable(5) {
		t.Fatal("a removed owner whose blobs lie before the floor is not reusable")
	}
	r.host.Add(5, NewStorage())
}

// A manifest made durable is not loaded until a segment header names it, so an owner whose removed
// shard's blobs only that manifest's floor passes is not given to a new shard until then, which
// would replay them as its own.
func TestARemovedOwnerIsNotReusableBeforeASegmentHeaderNamesTheManifest(t *testing.T) {
	r := newRig(t)
	st := NewStorage()
	r.host.Add(5, st)
	r.commit(st, 5, 1, Round{Entries: entries(1, 1, 1, 100), HardState: raftpb.HardState{Term: 1, Vote: 1, Commit: 1}})
	r.host.Remove(5)
	r.checkpoint()
	if r.host.Reusable(5) {
		t.Fatal("owner 5 is reusable while recovery would still load the manifest before the one whose floor passes its blobs")
	}
	r.roll()
	fresh := NewStorage()
	r.host.Add(5, fresh)
	hs := raftpb.HardState{Term: 3, Vote: 2}
	r.commit(fresh, 5, 1, Round{HardState: hs})
	sts := r.crash(5)
	if l, _ := sts[5].LastIndex(); l != 0 || sts[5].HardState() != hs {
		t.Fatalf("the new shard of owner 5 recovered last index %d and hard state %+v, want 0 and %+v", l, sts[5].HardState(), hs)
	}
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
	r.checkpoint()
	r.roll()
	r.bases = map[uint32]raftpb.SnapshotMetadata{5: st.Base()}
	r.crash(5)
	if hd := r.host.shard(5); hd.compacted != 3 || len(hd.refs) != want {
		t.Fatalf("after the restart the host's compaction index is %d and it holds %d blobs; want 3 and %d", hd.compacted, len(hd.refs), want)
	}
}

// A rewrite repeats the newest hard state the host made durable, which the shard may not have
// taken yet: its storage's older one would replace it on a restart, a vote and term gone back.
func TestARewriteWritesTheHostsNewestHardStateBeforeTheShardTakesIt(t *testing.T) {
	r := newRig(t)
	st := NewStorage()
	r.host.Add(6, st)
	r.commit(st, 6, 1, Round{Entries: entries(1, 1, 1, 100), HardState: raftpb.HardState{Term: 1, Vote: 1, Commit: 1}})
	if err := r.host.Compact(6, 1, raftpb.ConfState{Voters: []uint64{1}}); err != nil {
		t.Fatal(err)
	}
	newer := raftpb.HardState{Term: 2, Vote: 2, Commit: 1}
	r.write(6, 2, &Round{HardState: newer, Removed: true})
	r.run()
	r.checkpoint()
	if err := r.host.Rewrite(&r.ctx, r.host.Log.DurableLSN(), &r.out); err != nil {
		t.Fatal(err)
	}
	r.absorb()
	r.run()
	st.SetHardState(newer)
	r.bases = map[uint32]raftpb.SnapshotMetadata{6: st.Base()}
	sts := r.crashCleaned(6)
	if got := sts[6]; got.HardState() != newer || !got.Removed() {
		t.Fatalf("the restart recovered hard state %+v, removed %v; want %+v, removed", got.HardState(), got.Removed(), newer)
	}
}

// A shard started on a base no run holds yet, as format's boot state or a snapshot is, keeps the
// round that wrote the base live: only a restart that restores the base from its runs compacts to it.
func TestTheRoundOfABaseNoRunHoldsStaysLive(t *testing.T) {
	r := newRig(t)
	st := NewStorage()
	boot := raftpb.SnapshotMetadata{Index: 3, Term: 1, ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}}}
	if err := st.ApplySnapshot(raftpb.Snapshot{Metadata: boot}); err != nil {
		t.Fatal(err)
	}
	r.host.Add(5, st)
	r.commit(st, 5, 1, Round{Snapshot: raftpb.Snapshot{Metadata: boot}})
	r.commit(st, 5, 2, Round{HardState: raftpb.HardState{Term: 2, Vote: 1, Commit: 3}})
	r.commit(st, 5, 3, Round{HardState: raftpb.HardState{Term: 3, Vote: 2, Commit: 3}})
	sts := r.crashCleaned(5)
	if got := sts[5].Base(); got.Index != 3 || len(got.ConfState.Voters) != 3 {
		t.Fatalf("the restart lost the boot base: %+v", got)
	}
}

// A removed shard's round that turns durable after the removal keeps its owner from a new shard
// until the loadable manifest's floor passes that round too.
func TestARemovedShardsRoundInFlightKeepsItsOwnerOnceDurable(t *testing.T) {
	r := newRig(t)
	st := NewStorage()
	r.host.Add(5, st)
	r.commit(st, 5, 1, Round{Entries: entries(1, 1, 1, 100), HardState: raftpb.HardState{Term: 1, Vote: 1, Commit: 1}})
	r.write(5, 2, &Round{Entries: entries(2, 1, 1, 100), HardState: raftpb.HardState{Term: 1, Vote: 1, Commit: 2}})
	r.host.Remove(5)
	floor := r.checkpoint()
	r.roll()
	if r.host.Reusable(5) {
		t.Fatalf("owner 5 is reusable with its removed shard's round durable after the floor %d", floor)
	}
}

// A restart on a base its runs restore past the host's last compaction reports dead the replayed
// entry rounds that base covers, even when the one round holding a hard state closes no earlier one.
func TestARestartCollectsTheRoundsItsRestoredBaseCovers(t *testing.T) {
	r := newRig(t)
	st := NewStorage()
	r.host.Add(5, st)
	r.commit(st, 5, 1, Round{Entries: entries(1, 2, 1, 100)})
	r.commit(st, 5, 2, Round{Entries: entries(3, 1, 1, 100), HardState: raftpb.HardState{Term: 1, Vote: 1, Commit: 3}})
	want := r.host.shard(5).rounds[1].blobs
	r.checkpoint()
	r.roll()
	if err := st.Compact(3, raftpb.ConfState{Voters: []uint64{1}}); err != nil {
		t.Fatal(err)
	}
	r.bases = map[uint32]raftpb.SnapshotMetadata{5: st.Base()}
	r.crash(5)
	if n := len(r.host.shard(5).refs); n != want {
		t.Fatalf("after the restart the host holds %d blobs of owner 5, want %d of the round holding its hard state", n, want)
	}
}

// A shard retains the payload its storage holds and that of its rounds written and not yet
// taken into it, durable or not, so a shard told late of its durable rounds is never under its
// bound only for the time being.
func TestRetainedCountsTheStorageAndTheRoundsNotTaken(t *testing.T) {
	r := newRig(t)
	st := NewStorage()
	r.host.Add(6, st)
	r.commit(st, 6, 1, Round{Entries: entries(1, 2, 1, 100), HardState: raftpb.HardState{Term: 1, Vote: 1}})
	if got := r.host.Retained(6); got != 200 {
		t.Fatalf("retained %d after a round of 200 bytes", got)
	}
	next := Round{Entries: entries(3, 1, 1, 300)}
	r.write(6, 2, &next)
	if got := r.host.Retained(6); got != 500 {
		t.Fatalf("retained %d with a round of 300 bytes in flight, want 500", got)
	}
	r.run()
	if got := r.host.Retained(6); got != 500 {
		t.Fatalf("retained %d with the round durable and not taken, want 500", got)
	}
	st.Append(next.Entries)
	st.Took(2)
	if got := r.host.Retained(6); got != 500 {
		t.Fatalf("retained %d once the shard took the round, want 500", got)
	}
}

// Regression: a full rewrite writes the storage's entries, so one taken after a round is durable
// and before the shard took that round into its storage, as a chunk store's due checkpoint in the
// same handler is, would make the round dead without its entries.
func TestAFullRewriteWaitsForTheShardToTakeEveryDurableRound(t *testing.T) {
	r := newRig(t)
	st := NewStorage()
	r.host.Add(6, st)
	hs := raftpb.HardState{Term: 1, Vote: 2}
	r.commit(st, 6, 1, Round{Entries: entries(1, 2, 1, 100), HardState: hs})
	second := Round{Entries: entries(3, 2, 1, 100), HardState: hs}
	r.write(6, 2, &second)
	r.run()
	r.checkpoint()
	if err := r.host.Rewrite(&r.ctx, r.host.Log.DurableLSN(), &r.out); err != nil {
		t.Fatal(err)
	}
	r.absorb()
	r.run()
	st.Append(second.Entries)
	r.takeDone()
	sts := r.crashCleaned(6)
	if last, _ := sts[6].LastIndex(); last != 4 {
		t.Fatalf("the restart recovered entries to %d, want the 4 the shard was told durable", last)
	}
}

// A full rewrite holding more than the pool has free goes out a few blobs at a time as buffers
// free, a compaction waiting until it read the entries, and a shard after it in the host still
// gets its own rewrite: both floors move on, and a restart recovers every entry and hard state.
func TestAFullRewriteLargerThanThePoolGoesOutAsBuffersFree(t *testing.T) {
	r := newRig(t)
	stuck, idle := NewStorage(), NewStorage()
	r.host.Add(6, stuck)
	r.host.Add(5, idle)
	hs := raftpb.HardState{Term: 1, Vote: 2, Commit: 2}
	for i := uint64(1); i <= 12; i++ {
		r.commit(stuck, 6, i, Round{Entries: entries(4*i-3, 4, 1, 2000), HardState: hs})
	}
	idleHS := raftpb.HardState{Term: 2, Vote: 3, Commit: 1}
	r.commit(idle, 5, 1, Round{Entries: entries(1, 1, 2, 100), HardState: idleHS})
	if err := r.host.Compact(5, 1, raftpb.ConfState{Voters: []uint64{3}}); err != nil {
		t.Fatal(err)
	}
	r.checkpoint()
	pool := r.ctx.Pool.(*sim.Pool)
	hog, err := pool.Alloc(pool.Available() - 32<<10)
	if err != nil {
		t.Fatal(err)
	}
	before := r.host.Log.DurableLSN()
	if err := r.host.Rewrite(&r.ctx, before, &r.out); err != nil {
		t.Fatal(err)
	}
	r.absorb()
	if err := r.host.Compact(6, 2, raftpb.ConfState{Voters: []uint64{2}}); err != nil {
		t.Fatal(err)
	}
	if b := stuck.Base().Index; b != 0 {
		t.Fatalf("the storage compacted to %d while its rewrite was going out", b)
	}
	r.run()
	pool.Free(hog)
	if b := stuck.Base().Index; b != 2 {
		t.Fatalf("the storage is at base %d once its rewrite went out, want 2", b)
	}
	if floor := r.checkpoint(); floor < before {
		t.Fatalf("after the rewrites the floor is %d, before the rounds durable at %d", floor, before)
	}
	r.roll()
	r.bases = map[uint32]raftpb.SnapshotMetadata{5: idle.Base(), 6: stuck.Base()}
	sts := r.crashCleaned(5, 6)
	if last, _ := sts[6].LastIndex(); last != 48 || sts[6].HardState() != hs {
		t.Fatalf("the stuck shard recovered entries to %d and hard state %+v, want 48 and %+v", last, sts[6].HardState(), hs)
	}
	if got := sts[5].HardState(); got != idleHS {
		t.Fatalf("the idle shard recovered hard state %+v, want %+v", got, idleHS)
	}
}

// A round written while a full rewrite goes out waits behind it, so a crash at any point of the two, the
// cleaner run or not, keeps every entry the shard was told durable and never a part of the rewrite.
func TestARoundHeldBehindAFullRewriteSurvivesACrashAtEveryPoint(t *testing.T) {
	for _, cleaned := range []bool{false, true} {
		for k := 0; ; k++ {
			r := newRig(t)
			stuck := NewStorage()
			r.host.Add(6, stuck)
			hs := raftpb.HardState{Term: 1, Vote: 2, Commit: 2}
			for i := uint64(1); i <= 12; i++ {
				r.commit(stuck, 6, i, Round{Entries: entries(4*i-3, 4, 1, 2000), HardState: hs})
			}
			r.checkpoint()
			pool := r.ctx.Pool.(*sim.Pool)
			if _, err := pool.Alloc(pool.Available() - 32<<10); err != nil {
				t.Fatal(err)
			}
			if err := r.host.Rewrite(&r.ctx, r.host.Log.DurableLSN(), &r.out); err != nil {
				t.Fatal(err)
			}
			r.absorb()
			if r.host.shard(6).rw == nil {
				t.Fatal("rewrite went out whole")
			}
			hs2 := raftpb.HardState{Term: 1, Vote: 2, Commit: 3}
			held := Round{Entries: entries(49, 4, 1, 2000), HardState: hs2}
			r.write(6, 13, &held)
			if r.host.shard(6).wait != 13 {
				t.Fatal("round 13 not held")
			}
			for n := 0; n < k && len(r.queue) > 0; n++ {
				r.step()
			}
			finished := len(r.queue) == 0
			want, wantHS := uint64(48), hs
			for _, d := range r.takeDone() {
				if d.Round == 13 {
					want, wantHS = 52, hs2
				}
			}
			var sts map[uint32]*Storage
			if cleaned {
				sts = r.crashCleaned(6)
			} else {
				sts = r.crash(6)
			}
			last, _ := sts[6].LastIndex()
			got := sts[6].HardState()
			if last < want || last > 52 || (want == 52 && got != wantHS) || (got != hs && got != hs2) {
				t.Fatalf("cleaned %v, crash after %d steps: last %d hs %+v, want >= %d hs %+v", cleaned, k, last, got, want, wantHS)
			}
			if finished {
				break
			}
		}
	}
}
