package raftshard

import (
	"errors"
	"maps"
	"reflect"
	"slices"
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

type landedBlob struct {
	ref  disklog.BlobRef
	kind disklog.BlobKind
	data []byte
}

// TestRoundsReplayRebuildsTheStorage writes six rounds of several blobs each through Rounds,
// compacting before the fourth: ReplayLSN reports the LSN before the oldest blob of the rounds
// left live, once. Then a seventh round's last blob never lands, and the test replays every blob
// not reported dead into a fresh storage on the base, as recovery's mail hands them over: the
// storage equals the one before, the torn round's blobs are the only ones that die, and the blobs
// left live are those the first Rounds kept.
func TestRoundsReplayRebuildsTheStorage(t *testing.T) {
	pool := sim.NewPool(1<<20, 0)
	st := NewStorage()
	rs := NewRounds(3, st, 600)
	var landed []landedBlob
	var lsn uint64
	write := func(round uint64) []landedBlob {
		last, _ := st.LastIndex()
		ents := make([]raftpb.Entry, 3)
		for i := range ents {
			ents[i] = raftpb.Entry{Index: last + 1 + uint64(i), Term: 1, Data: slices.Repeat([]byte{byte(round)}, 300)}
		}
		rd := Round{Entries: ents, HardState: raftpb.HardState{Term: 1, Vote: 1, Commit: last + 3}}
		rs.Write(round, &rd)
		items, err := rs.Ready(pool, nil)
		if err != nil || len(items) < 2 {
			t.Fatalf("round %d cut into %d blobs: %v", round, len(items), err)
		}
		var out []landedBlob
		for _, it := range items {
			lsn++
			out = append(out, landedBlob{disklog.BlobRef{Segment: 1, Block: uint32(lsn), LSN: lsn}, it.Entry.BlobKind, slices.Clone(pool.Bytes(it.Buf)[:it.Entry.Length])})
			pool.Free(it.Buf)
		}
		landed = append(landed, out...)
		if got, err := rs.Durable(nil, refsOf(out)); err != nil || got != round {
			t.Fatalf("Durable named round %d of %d: %v", got, round, err)
		}
		st.Append(ents)
		st.SetHardState(rd.HardState)
		st.Took(round)
		return out
	}
	for round := uint64(1); round <= 6; round++ {
		if round == 4 {
			last, _ := st.LastIndex()
			if err := rs.Compact(last, raftpb.ConfState{Voters: []uint64{1}}); err != nil {
				t.Fatal(err)
			}
		}
		write(round)
	}
	dead := rs.Dead(nil)
	if len(dead) == 0 {
		t.Fatal("the compaction killed no round")
	}
	live := slices.IndexFunc(landed, func(b landedBlob) bool { return !slices.Contains(dead, b.ref) })
	if lsn, ok := rs.ReplayLSN(); !ok || lsn != landed[live].ref.LSN-1 {
		t.Fatalf("ReplayLSN reported %d, %v; the oldest live blob is at %d", lsn, ok, landed[live].ref.LSN)
	}
	if lsn, ok := rs.ReplayLSN(); ok {
		t.Fatalf("ReplayLSN reported %d again with no blob dead since", lsn)
	}
	torn := write(7)
	torn = torn[:len(torn)-1]
	landed = landed[:len(landed)-1]

	again := NewStorage()
	if err := again.ApplySnapshot(raftpb.Snapshot{Metadata: st.Base()}); err != nil {
		t.Fatal(err)
	}
	rs2 := NewRounds(3, again, 600)
	for _, b := range landed {
		if slices.Contains(dead, b.ref) {
			continue
		}
		if err := rs2.Replay(b.ref, b.kind, b.data); err != nil {
			t.Fatal(err)
		}
	}
	rs2.Replayed()
	if got := rs2.Dead(nil); !reflect.DeepEqual(got, refsOf(torn)) {
		t.Fatalf("the replay reported dead %v, the torn round's blobs %v", got, refsOf(torn))
	}
	last, _ := again.LastIndex()
	want, _ := st.Entries(st.Base().Index+1, last+1, ^uint64(0))
	got, _ := again.Entries(again.Base().Index+1, last+1, ^uint64(0))
	if again.HardState().Commit != last || !reflect.DeepEqual(got, want) {
		t.Fatalf("replayed storage holds %d entries to %d, commit %d; want %d", len(got), last, again.HardState().Commit, len(want))
	}
	rs.Delete()
	rs2.Delete()
	kept := rs.Dead(nil)
	if gotLive := rs2.Dead(nil); !reflect.DeepEqual(gotLive, kept[:len(kept)-len(torn)-1]) {
		t.Fatalf("the replay keeps %v live, the writer kept %v", gotLive, kept)
	}
}

func refsOf(bs []landedBlob) []disklog.BlobRef {
	var refs []disklog.BlobRef
	for _, b := range bs {
		refs = append(refs, b.ref)
	}
	return refs
}

// TestRoundsReplayDropsATornRoundAtTheNextFirstBlob replays a round missing its last blob, then the
// first blob of the next: the torn round's blobs are dead at once, before the replay ends.
func TestRoundsReplayDropsATornRoundAtTheNextFirstBlob(t *testing.T) {
	pool := sim.NewPool(1<<20, 0)
	w := NewRounds(3, NewStorage(), 600)
	var lsn uint64
	cut := func(flush uint64) []landedBlob {
		ents := make([]raftpb.Entry, 3)
		for i := range ents {
			ents[i] = raftpb.Entry{Index: 1 + uint64(i), Term: flush, Data: make([]byte, 300)}
		}
		w.Write(flush, &Round{Entries: ents, HardState: raftpb.HardState{Term: flush, Commit: 3}})
		items, err := w.Ready(pool, nil)
		if err != nil || len(items) < 2 {
			t.Fatalf("round %d cut into %d blobs: %v", flush, len(items), err)
		}
		var out []landedBlob
		for _, it := range items {
			lsn++
			out = append(out, landedBlob{disklog.BlobRef{Segment: 1, Block: uint32(lsn), LSN: lsn}, it.Entry.BlobKind, slices.Clone(pool.Bytes(it.Buf)[:it.Entry.Length])})
			pool.Free(it.Buf)
		}
		if _, err := w.Durable(nil, refsOf(out)); err != nil {
			t.Fatal(err)
		}
		return out
	}
	torn, next := cut(1), cut(2)
	torn = torn[:len(torn)-1]
	r := NewRounds(3, NewStorage(), 600)
	for _, b := range slices.Concat(torn, next[:1]) {
		if err := r.Replay(b.ref, b.kind, b.data); err != nil {
			t.Fatal(err)
		}
	}
	if got := r.Dead(nil); !reflect.DeepEqual(got, refsOf(torn)) {
		t.Fatalf("dead %v once the next round began, want the torn round's %v", got, refsOf(torn))
	}
}

// TestRoundsReplayDropsASnapshotRoundTheRunsDoNotHold replays, on a base of 0, an entries round
// and then a round restoring a snapshot at 8 whose data is in the replica's runs: the runs the
// restart loaded are older than the snapshot, so the round is dropped whole, its hard state too,
// and its blobs are dead once the replay ends. On runs at the snapshot, the round drops every
// entry replayed before it, though they lie past the snapshot.
func TestRoundsReplayDropsASnapshotRoundTheRunsDoNotHold(t *testing.T) {
	snap := &Round{
		Snapshot:  raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 8, Term: 2, ConfState: raftpb.ConfState{Voters: []uint64{1}}}},
		HardState: raftpb.HardState{Term: 2, Commit: 8},
		Runs:      true,
	}
	var lsn uint64
	blobs := func(r *Round) []landedBlob {
		var out []landedBlob
		for _, b := range roundBlobs(t, r, 1<<20) {
			lsn++
			out = append(out, landedBlob{disklog.BlobRef{Segment: 1, Block: uint32(lsn), LSN: lsn}, b.kind, b.payload})
		}
		return out
	}
	old, restore := blobs(&Round{Entries: ents(1, 1, 10, 10), HardState: raftpb.HardState{Term: 1, Commit: 5}}), blobs(snap)
	replayAll := func(base uint64) (*Storage, []disklog.BlobRef) {
		st := NewStorage()
		if base > 0 {
			if err := st.ApplySnapshot(raftpb.Snapshot{Metadata: snap.Snapshot.Metadata}); err != nil {
				t.Fatal(err)
			}
		}
		r := NewRounds(3, st, 1<<20)
		for _, b := range slices.Concat(old, restore) {
			if err := r.Replay(b.ref, b.kind, b.data); err != nil {
				t.Fatal(err)
			}
		}
		r.Replayed()
		return st, r.Dead(nil)
	}
	st, dead := replayAll(0)
	if last, _ := st.LastIndex(); st.Base().Index != 0 || last != 10 || st.HardState().Term != 1 {
		t.Fatalf("on older runs: base %d, last %d, hard state %+v", st.Base().Index, last, st.HardState())
	}
	if !reflect.DeepEqual(dead, refsOf(restore)) {
		t.Fatalf("dead %v on older runs, want the dropped round's %v", dead, refsOf(restore))
	}
	st, _ = replayAll(8)
	if last, _ := st.LastIndex(); last != 8 || st.HardState().Term != 2 {
		t.Fatalf("on runs at the snapshot: last %d, hard state %+v", last, st.HardState())
	}
}

// TestRoundsKeepTheVoteASnapshotRoundTheRunsDoNotHoldWouldDrop writes a round holding a vote for 2
// in term 3, then a round restoring a snapshot whose data is in the replica's runs, whose hard
// state frees the first round. A restart on runs older than the snapshot, replaying after the LSN
// ReplayLSN reports, drops the snapshot's round and must still find the vote, so ReplayLSN moves
// past the first round only once a later round of the replica is durable, which the replica writes
// once a manifest naming runs that hold the snapshot is.
func TestRoundsKeepTheVoteASnapshotRoundTheRunsDoNotHoldWouldDrop(t *testing.T) {
	pool := sim.NewPool(1<<20, 0)
	st := NewStorage()
	rs := NewRounds(3, st, 1<<20)
	var lsn uint64
	var landed []landedBlob
	write := func(round uint64, rd *Round) {
		rs.Write(round, rd)
		items, err := rs.Ready(pool, nil)
		if err != nil {
			t.Fatal(err)
		}
		var out []landedBlob
		for _, it := range items {
			lsn++
			out = append(out, landedBlob{disklog.BlobRef{Segment: 1, Block: uint32(lsn), LSN: lsn}, it.Entry.BlobKind, slices.Clone(pool.Bytes(it.Buf)[:it.Entry.Length])})
			pool.Free(it.Buf)
		}
		landed = append(landed, out...)
		if got, err := rs.Durable(nil, refsOf(out)); err != nil || got != round {
			t.Fatal(got, err)
		}
	}
	vote := raftpb.HardState{Term: 3, Vote: 2}
	write(1, &Round{HardState: vote})
	st.SetHardState(vote)
	st.Took(1)
	write(2, &Round{Snapshot: raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 8, Term: 3, ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}}}},
		HardState: raftpb.HardState{Term: 3, Vote: 2, Commit: 8}, Runs: true})
	if dead := rs.Dead(nil); !reflect.DeepEqual(dead, refsOf(landed[:1])) {
		t.Fatalf("dead %v once the snapshot's round is durable, want the vote's round %v", dead, refsOf(landed[:1]))
	}
	replay, _ := rs.ReplayLSN()
	st2 := NewStorage()
	r2 := NewRounds(3, st2, 1<<20)
	for _, b := range landed {
		if b.ref.LSN <= replay {
			continue
		}
		if err := r2.Replay(b.ref, b.kind, b.data); err != nil {
			t.Fatal(err)
		}
	}
	r2.Replayed()
	if hs := st2.HardState(); hs != vote {
		t.Fatalf("a restart on older runs after replay LSN %d restored hard state %+v, want %+v", replay, hs, vote)
	}
	write(3, &Round{})
	if replay, ok := rs.ReplayLSN(); !ok || replay != landed[1].ref.LSN-1 {
		t.Fatalf("replay LSN %d %v once a round after the snapshot's is durable, want %d", replay, ok, landed[1].ref.LSN-1)
	}
}

// TestRoundsPanicOnABrokenCallOrder checks that each call out of order panics rather than lose or
// double count a round.
func TestRoundsPanicOnABrokenCallOrder(t *testing.T) {
	pool := sim.NewPool(1<<20, 0)
	rd := &Round{HardState: raftpb.HardState{Term: 1}}
	inFlight := func() *Rounds {
		r := NewRounds(3, NewStorage(), 600)
		r.Write(1, rd)
		items, err := r.Ready(pool, nil)
		if err != nil || len(items) == 0 {
			t.Fatalf("Ready gave %d blobs: %v", len(items), err)
		}
		for _, it := range items {
			pool.Free(it.Buf)
		}
		return r
	}
	cases := map[string]func(){
		"a round 0": func() { NewRounds(3, NewStorage(), 600).Write(0, rd) },
		"a write while one is queued": func() {
			r := NewRounds(3, NewStorage(), 600)
			r.Write(1, rd)
			r.Write(2, rd)
		},
		"a write while one is in flight": func() { inFlight().Write(2, rd) },
		"Durable with none in flight":    func() { _, _ = NewRounds(3, NewStorage(), 600).Durable(nil, nil) },
		"Durable after Delete": func() {
			r := inFlight()
			r.Delete()
			_, _ = r.Durable(nil, []disklog.BlobRef{{Segment: 1, LSN: 1}})
		},
	}
	for _, name := range slices.Sorted(maps.Keys(cases)) {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("no panic")
				}
			}()
			cases[name]()
		})
	}
}

// landed takes items as the log would land them, each at the next LSN, freeing their buffers.
func landed(pool core.BufPool, items []disklog.Item, lsn *uint64) []landedBlob {
	var out []landedBlob
	for _, it := range items {
		*lsn++
		out = append(out, landedBlob{disklog.BlobRef{Segment: 1, Block: uint32(*lsn), LSN: *lsn}, it.Entry.BlobKind, slices.Clone(pool.Bytes(it.Buf)[:it.Entry.Length])})
		pool.Free(it.Buf)
	}
	return out
}

// TestRoundsRewriteWhatHoldsTheFloorBack checks Rewrite on a shard of the round driver. Idle, its
// oldest round compacted and holding its newest hard state, it writes that hard state again in a
// round of its own, which the replica is not told of and which kills the old round. Holding
// entries past its commit, it writes every entry again a few blobs a Ready, the replica's next
// round and a compaction waiting behind; once the rewrite is durable every earlier blob is dead,
// and the blobs left replay into the storage it holds.
func TestRoundsRewriteWhatHoldsTheFloorBack(t *testing.T) {
	pool := sim.NewPool(1<<20, 0)
	var lsn uint64
	write := func(rs *Rounds, st *Storage, round uint64, rd Round) []landedBlob {
		t.Helper()
		rs.Write(round, &rd)
		items, err := rs.Ready(pool, nil)
		if err != nil || len(items) == 0 {
			t.Fatalf("round %d: %d blobs, %v", round, len(items), err)
		}
		out := landed(pool, items, &lsn)
		if got, err := rs.Durable(nil, refsOf(out)); err != nil || got != round {
			t.Fatalf("Durable named round %d of %d: %v", got, round, err)
		}
		st.Append(rd.Entries)
		st.SetHardState(rd.HardState)
		st.Took(round)
		return out
	}
	t.Run("idle", func(t *testing.T) {
		st := NewStorage()
		rs := NewRounds(3, st, 600)
		old := write(rs, st, 1, Round{Entries: entries(1, 2, 1, 100), HardState: raftpb.HardState{Term: 1, Vote: 1, Commit: 2}})
		if rs.Rewrite(lsn); rs.HasReady() {
			t.Fatal("a shard whose round holds uncompacted entries rewrote its hard state")
		}
		if err := rs.Compact(2, raftpb.ConfState{Voters: []uint64{1}}); err != nil {
			t.Fatal(err)
		}
		if rs.Rewrite(old[0].ref.LSN - 1); rs.HasReady() {
			t.Fatal("a shard whose oldest round follows the base rewrote its hard state")
		}
		rs.Rewrite(lsn)
		items, err := rs.Ready(pool, nil)
		if err != nil || len(items) == 0 {
			t.Fatalf("the rewrite gave %d blobs: %v", len(items), err)
		}
		rs.Write(2, &Round{HardState: raftpb.HardState{Term: 2, Vote: 1, Commit: 2}})
		if rs.HasReady() {
			t.Fatal("the replica's round was ready beside the rewrite in flight")
		}
		rw := landed(pool, items, &lsn)
		if got, err := rs.Durable(nil, refsOf(rw)); err != nil || got != 0 {
			t.Fatalf("the rewrite's Durable named round %d: %v", got, err)
		}
		if dead := rs.Dead(nil); !reflect.DeepEqual(dead, refsOf(old)) {
			t.Fatalf("the rewrite killed %v, want the old round's %v", dead, refsOf(old))
		}
		if got, ok := rs.ReplayLSN(); !ok || got != rw[0].ref.LSN-1 {
			t.Fatalf("ReplayLSN reported %d, %v; want %d", got, ok, rw[0].ref.LSN-1)
		}
		if !rs.HasReady() {
			t.Fatal("the replica's round did not follow the rewrite")
		}
	})
	t.Run("full", func(t *testing.T) {
		st := NewStorage()
		rs := NewRounds(3, st, 100)
		old := write(rs, st, 1, Round{Entries: entries(1, 3, 1, 300), HardState: raftpb.HardState{Term: 1, Vote: 1}})
		rs.Rewrite(lsn)
		var rw []landedBlob
		for n := 0; rs.HasReady(); n++ {
			items, err := rs.Ready(pool, nil)
			if err != nil || len(items) == 0 || len(items) > rewriteBlobs {
				t.Fatalf("Ready %d of the rewrite gave %d blobs: %v", n, len(items), err)
			}
			if n == 0 {
				rs.Write(2, &Round{HardState: raftpb.HardState{Term: 1, Vote: 1, Commit: 2}})
				if err := rs.Compact(2, raftpb.ConfState{Voters: []uint64{1}}); err != nil || st.Base().Index != 0 {
					t.Fatalf("a compaction during the rewrite compacted to %d: %v", st.Base().Index, err)
				}
			}
			out := landed(pool, items, &lsn)
			rw = append(rw, out...)
			if got, err := rs.Durable(nil, refsOf(out)); err != nil || got != 0 {
				t.Fatalf("the rewrite's Durable named round %d: %v", got, err)
			}
			if rs.hd.rw == nil {
				break
			}
		}
		if len(rw) <= rewriteBlobs || st.Base().Index != 2 {
			t.Fatalf("the rewrite went out in %d blobs, the compaction to %d", len(rw), st.Base().Index)
		}
		if dead := rs.Dead(nil); !reflect.DeepEqual(dead, refsOf(old)) {
			t.Fatalf("the rewrite killed %v, want the old round's %v", dead, refsOf(old))
		}
		again := NewStorage()
		r2 := NewRounds(3, again, 100)
		for _, b := range rw {
			if err := r2.Replay(b.ref, b.kind, b.data); err != nil {
				t.Fatal(err)
			}
		}
		r2.Replayed()
		want, _ := st.Entries(3, 4, ^uint64(0))
		got, _ := again.Entries(3, 4, ^uint64(0))
		if again.HardState() != st.HardState() || !reflect.DeepEqual(got, want) {
			t.Fatalf("the rewrite replays to %+v and %v, the storage holds %+v and %v", again.HardState(), got, st.HardState(), want)
		}
		items, err := rs.Ready(pool, nil)
		if err != nil || len(items) == 0 {
			t.Fatalf("the replica's round after the rewrite gave %d blobs: %v", len(items), err)
		}
		if got, err := rs.Durable(nil, refsOf(landed(pool, items, &lsn))); err != nil || got != 2 {
			t.Fatalf("Durable named round %d of 2: %v", got, err)
		}
	})
	t.Run("failed", func(t *testing.T) {
		st := NewStorage()
		rs := NewRounds(3, st, 100)
		old := write(rs, st, 1, Round{Entries: entries(1, 3, 1, 300), HardState: raftpb.HardState{Term: 1, Vote: 1}})
		rs.Rewrite(lsn)
		items, err := rs.Ready(pool, nil)
		if err != nil || len(items) == 0 {
			t.Fatalf("the rewrite gave %d blobs: %v", len(items), err)
		}
		first := landed(pool, items, &lsn)
		if _, err := rs.Durable(nil, refsOf(first)); err != nil {
			t.Fatal(err)
		}
		if err := rs.Compact(2, raftpb.ConfState{Voters: []uint64{1}}); err != nil {
			t.Fatal(err)
		}
		items, err = rs.Ready(pool, nil)
		if err != nil || len(items) == 0 {
			t.Fatalf("the rewrite's second Ready gave %d blobs: %v", len(items), err)
		}
		landed(pool, items, &lsn)
		failed := errors.New("disk failed")
		if got, err := rs.Durable(failed, nil); got != 0 || !errors.Is(err, failed) {
			t.Fatalf("the failed rewrite's Durable named round %d: %v", got, err)
		}
		if rs.HasReady() || st.Base().Index != 2 {
			t.Fatalf("after its batch failed the rewrite went on %v, the compaction it held went to %d", rs.HasReady(), st.Base().Index)
		}
		if dead := rs.Dead(nil); !reflect.DeepEqual(dead, refsOf(first)) {
			t.Fatalf("the failed rewrite killed %v, want its own %v", dead, refsOf(first))
		}
		again := NewStorage()
		r2 := NewRounds(3, again, 100)
		for _, b := range old {
			if err := r2.Replay(b.ref, b.kind, b.data); err != nil {
				t.Fatal(err)
			}
		}
		r2.Replayed()
		want, _ := st.Entries(3, 4, ^uint64(0))
		got, _ := again.Entries(3, 4, ^uint64(0))
		if again.HardState() != st.HardState() || !reflect.DeepEqual(got, want) {
			t.Fatalf("the old round replays to %+v and %v, the storage holds %+v and %v", again.HardState(), got, st.HardState(), want)
		}
		if rs.Rewrite(lsn); !rs.HasReady() {
			t.Fatal("a later Rewrite did not write the round again")
		}
	})
}

// TestRoundsAreFullAtTheShareTheOwnerMailed checks a shard of the round driver takes no new
// entries once it retains the share its disk's owner mailed, and none before one is mailed, and
// asks for a checkpoint once for each commit it applied while full.
func TestRoundsAreFullAtTheShareTheOwnerMailed(t *testing.T) {
	st := NewStorage()
	rs := NewRounds(3, st, 600)
	st.Append(entries(1, 4, 1, 100))
	st.SetHardState(raftpb.HardState{Term: 1, Vote: 1, Commit: 3})
	if rs.Full() || rs.FullDue(3) {
		t.Fatal("a shard no share was mailed to was full")
	}
	rs.SetShare(401)
	if rs.Full() || rs.FullDue(3) {
		t.Fatalf("a shard retaining %d of its share 401 was full", rs.Retained())
	}
	rs.SetShare(400)
	switch {
	case !rs.Full():
		t.Fatalf("a shard retaining %d of its share 400 was not full", rs.Retained())
	case rs.FullDue(2):
		t.Fatal("a full shard asked for a checkpoint before it applied its commit")
	case !rs.FullDue(3):
		t.Fatal("a full shard that applied its commit asked for no checkpoint")
	case rs.FullDue(3):
		t.Fatal("a full shard asked twice for a checkpoint at one commit")
	}
	st.SetHardState(raftpb.HardState{Term: 1, Vote: 1, Commit: 4})
	if !rs.FullDue(4) {
		t.Fatal("a full shard that applied a new commit asked for no checkpoint")
	}
}
