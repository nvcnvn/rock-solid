package raftshard

import (
	"bytes"
	"errors"
	"runtime"
	"testing"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

func ents(term, first, n uint64, size int) []raftpb.Entry {
	var out []raftpb.Entry
	for i := range n {
		out = append(out, raftpb.Entry{Term: term, Index: first + i, Data: bytes.Repeat([]byte{byte(first + i)}, size)})
	}
	return out
}

func storageWith(es []raftpb.Entry) *Storage {
	st := NewStorage()
	st.Append(es)
	return st
}

func TestStorageServesEntriesAfterItsBase(t *testing.T) {
	st := storageWith(ents(1, 1, 10, 100))
	if err := st.Compact(3, raftpb.ConfState{Voters: []uint64{1}}); err != nil {
		t.Fatal(err)
	}
	first, _ := st.FirstIndex()
	last, _ := st.LastIndex()
	if first != 4 || last != 10 {
		t.Fatalf("first %d last %d after compacting to 3", first, last)
	}
	for _, lo := range []uint64{1, 3} {
		if _, err := st.Entries(lo, 5, 1<<20); !errors.Is(err, raft.ErrCompacted) {
			t.Fatalf("Entries(%d, 5) = %v, want ErrCompacted", lo, err)
		}
	}
	got, err := st.Entries(4, 11, 1<<20)
	if err != nil || len(got) != 7 || got[0].Index != 4 || got[6].Index != 10 {
		t.Fatalf("Entries(4, 11) = %d entries, %v", len(got), err)
	}
	for _, e := range got {
		if !bytes.Equal(e.Data, bytes.Repeat([]byte{byte(e.Index)}, 100)) || e.Term != 1 {
			t.Fatalf("entry %d holds %d bytes of term %d", e.Index, len(e.Data), e.Term)
		}
	}
	one := got[0].Size()
	if got, _ := st.Entries(4, 11, uint64(2*one)); len(got) != 2 {
		t.Fatalf("maxSize of two entries returned %d", len(got))
	}
	if got, _ := st.Entries(4, 11, 0); len(got) != 1 {
		t.Fatalf("maxSize 0 returned %d entries, want at least one", len(got))
	}
	for i, want := range map[uint64]error{2: raft.ErrCompacted, 3: nil, 10: nil, 11: raft.ErrUnavailable} {
		if _, err := st.Term(i); !errors.Is(err, want) && err != want {
			t.Fatalf("Term(%d) = %v, want %v", i, err, want)
		}
	}
	defer func() {
		if recover() == nil {
			t.Fatal("Entries past last + 1 did not panic")
		}
	}()
	_, _ = st.Entries(4, 12, 1<<20)
}

// Entries sized to one entry allocates for a few, not for the whole range a lagging follower's hi spans.
func TestEntriesAllocatesForWhatItReturns(t *testing.T) {
	st := storageWith(ents(1, 1, 100_000, 0))
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	got, err := st.Entries(1, 100_001, 1)
	runtime.ReadMemStats(&after)
	if err != nil || len(got) != 1 {
		t.Fatalf("Entries = %d entries, %v", len(got), err)
	}
	if n := after.TotalAlloc - before.TotalAlloc; n > 64<<10 {
		t.Fatalf("Entries allocated %d bytes for one entry", n)
	}
}

// A later append replaces the entries from its first index on, as a later record does at replay.
func TestAppendReplacesTheConflictingTail(t *testing.T) {
	st := storageWith(ents(2, 1, 8, 10))
	st.Append(ents(3, 5, 1, 20))
	last, _ := st.LastIndex()
	got, _ := st.Entries(5, 6, 1<<20)
	if last != 5 || got[0].Term != 3 || len(got[0].Data) != 20 {
		t.Fatalf("last %d, entry 5 %+v", last, got[0])
	}
}

// Compaction drops the chunks only compacted entries used, and keeps those the rest still read.
func TestCompactionFreesChunks(t *testing.T) {
	st := storageWith(ents(1, 1, 64, 4<<10))
	if len(st.chunks) != 4 {
		t.Fatalf("64 entries of 4 KiB in %d chunks, want 4", len(st.chunks))
	}
	if err := st.Compact(40, raftpb.ConfState{}); err != nil {
		t.Fatal(err)
	}
	if len(st.chunks) != 2 {
		t.Fatalf("%d chunks after compacting to 40, want 2", len(st.chunks))
	}
	got, _ := st.Entries(41, 65, 1<<30)
	for _, e := range got {
		if !bytes.Equal(e.Data, bytes.Repeat([]byte{byte(e.Index)}, 4<<10)) {
			t.Fatalf("entry %d corrupt after compaction", e.Index)
		}
	}
	if err := st.Compact(64, raftpb.ConfState{}); err != nil || len(st.chunks) != 1 {
		t.Fatalf("compacting everything: %v, %d chunks", err, len(st.chunks))
	}
	if err := st.Compact(64, raftpb.ConfState{}); !errors.Is(err, raft.ErrCompacted) {
		t.Fatalf("compacting to the base again = %v", err)
	}
}

type blob struct {
	kind    disklog.BlobKind
	payload []byte
}

// roundBlobs is r's record cut into blobs of at most maxBlob bytes for owner 7.
func roundBlobs(t *testing.T, r *Round, maxBlob uint64) []blob {
	t.Helper()
	pool := sim.NewPool(1<<20, 0)
	items, err := RoundItems(pool, 7, maxBlob, AppendRound(nil, r), nil)
	if err != nil {
		t.Fatal(err)
	}
	var out []blob
	for _, it := range items {
		if it.Entry.Owner != 7 || it.Entry.Length > maxBlob {
			t.Fatalf("blob of %d bytes owned by %d", it.Entry.Length, it.Entry.Owner)
		}
		out = append(out, blob{it.Entry.BlobKind, append([]byte(nil), pool.Bytes(it.Buf)[:it.Entry.Length]...)})
		pool.Free(it.Buf)
	}
	return out
}

func replay(t *testing.T, st *Storage, r *Round, maxBlob uint64) {
	t.Helper()
	for _, b := range roundBlobs(t, r, maxBlob) {
		if err := st.Replay(b.kind, b.payload); err != nil {
			t.Fatal(err)
		}
	}
}

// Replay applies records in LSN order: a later record's entry at index 5 of term 3 replaces an
// earlier one's index 5 of term 2 and drops its 6 on, and the latest hard state wins.
func TestReplayLaterRecordsWin(t *testing.T) {
	st := NewStorage()
	replay(t, st, &Round{HardState: raftpb.HardState{Term: 2, Vote: 1, Commit: 3}, Entries: ents(2, 1, 8, 10)}, 1<<20)
	replay(t, st, &Round{HardState: raftpb.HardState{Term: 3, Vote: 2, Commit: 4}, Entries: ents(3, 5, 1, 10)}, 1<<20)
	last, _ := st.LastIndex()
	term, _ := st.Term(5)
	if last != 5 || term != 3 || st.HardState() != (raftpb.HardState{Term: 3, Vote: 2, Commit: 4}) {
		t.Fatalf("after replay: last %d, term of 5 %d, hard state %+v", last, term, st.HardState())
	}
}

// Replay on a recorded base drops a tail that a later round at or below the base replaced: the
// log was 5..12 of term 1, then 7..8 and 9..10 of term 2, compacted to 10.
func TestReplayOnABaseDropsATailReplacedBelowIt(t *testing.T) {
	st := NewStorage()
	if err := st.ApplySnapshot(raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 10, Term: 2}}); err != nil {
		t.Fatal(err)
	}
	replay(t, st, &Round{Entries: ents(1, 5, 8, 10)}, 1<<20)
	replay(t, st, &Round{Entries: ents(2, 7, 2, 10)}, 1<<20)
	replay(t, st, &Round{Entries: ents(2, 9, 2, 10)}, 1<<20)
	if last, _ := st.LastIndex(); last != 10 {
		t.Fatalf("last %d after replaying a log that ends at the base 10", last)
	}
}

// A round larger than a blob goes out as several, and only its last, of kind BlobRaftState,
// applies it: a round torn before its last blob changes nothing, nor does one missing its first.
func TestARoundSpansBlobsAndATornOneChangesNothing(t *testing.T) {
	r := &Round{
		Snapshot:  raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 9, Term: 2, ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}}}, Data: []byte("state")},
		Entries:   ents(2, 10, 6, 1000),
		HardState: raftpb.HardState{Term: 2, Vote: 1, Commit: 12},
	}
	bs := roundBlobs(t, r, 1500)
	if len(bs) < 4 || bs[len(bs)-1].kind != disklog.BlobRaftState || bs[0].kind != disklog.BlobRaftEntries {
		t.Fatalf("%d blobs, kinds first %d last %d", len(bs), bs[0].kind, bs[len(bs)-1].kind)
	}
	torn := NewStorage()
	for _, b := range bs[:len(bs)-1] {
		if err := torn.Replay(b.kind, b.payload); err != nil {
			t.Fatal(err)
		}
	}
	if last, _ := torn.LastIndex(); last != 0 || torn.HardState() != (raftpb.HardState{}) {
		t.Fatalf("a torn round left last %d, hard state %+v", last, torn.HardState())
	}
	headless := NewStorage()
	for _, b := range bs[1:] {
		if err := headless.Replay(b.kind, b.payload); err != nil {
			t.Fatal(err)
		}
	}
	if last, _ := headless.LastIndex(); last != 0 || headless.HardState() != (raftpb.HardState{}) {
		t.Fatalf("a round missing its first blob left last %d, hard state %+v", last, headless.HardState())
	}
	st := NewStorage()
	replay(t, st, &Round{Entries: ents(1, 1, 20, 10)}, 1<<20)
	replay(t, st, r, 1500)
	first, _ := st.FirstIndex()
	last, _ := st.LastIndex()
	if b := st.Base(); first != 10 || last != 15 || b.Term != 2 || len(b.ConfState.Learners) != 1 || st.HardState().Commit != 12 {
		t.Fatalf("after a snapshot round: first %d last %d base %+v hard state %+v", first, last, b, st.HardState())
	}
}

func TestRoundRecordsRoundTrip(t *testing.T) {
	r := Round{
		Snapshot:  raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 4, Term: 1, ConfState: raftpb.ConfState{Voters: []uint64{1, 4}, VotersOutgoing: []uint64{1, 2}, LearnersNext: []uint64{2}, AutoLeave: true}}, Data: []byte("x")},
		Entries:   []raftpb.Entry{{Term: 1, Index: 5, Type: raftpb.EntryConfChangeV2, Data: []byte("cc")}, {Term: 1, Index: 6}},
		HardState: raftpb.HardState{Term: 1, Vote: 4, Commit: 5},
	}
	var got Round
	if err := DecodeRound(AppendRound(nil, &r), &got); err != nil {
		t.Fatal(err)
	}
	if got.Snapshot.String() != r.Snapshot.String() || got.HardState != r.HardState || len(got.Entries) != 2 || got.Entries[0].String() != r.Entries[0].String() || got.Entries[1].String() != r.Entries[1].String() {
		t.Fatalf("decoded %+v, want %+v", got, r)
	}
	for n := range len(AppendRound(nil, &r)) {
		if err := DecodeRound(AppendRound(nil, &r)[:n], &got); !errors.Is(err, ErrBadRound) {
			t.Fatalf("record cut to %d bytes decoded: %v", n, err)
		}
	}
}

func FuzzDecodeRound(f *testing.F) {
	f.Add(AppendRound(nil, &Round{HardState: raftpb.HardState{Term: 1, Commit: 2}, Entries: ents(1, 1, 2, 3)}))
	f.Add(AppendRound(nil, &Round{Snapshot: raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 3, Term: 1, ConfState: raftpb.ConfState{Voters: []uint64{1}}}}}))
	f.Fuzz(func(t *testing.T, b []byte) {
		var r Round
		if DecodeRound(b, &r) != nil {
			return
		}
		if again := AppendRound(nil, &r); !bytes.Equal(again, b) {
			t.Fatalf("decoded record encodes as %x, was %x", again, b)
		}
		st := NewStorage()
		_ = st.Replay(disklog.BlobRaftState, append([]byte{1}, b...))
	})
}

func BenchmarkStorageAppendCompact(b *testing.B) {
	st := NewStorage()
	batch := ents(1, 1, 16, 128)
	b.ReportAllocs()
	for i := range b.N {
		for j := range batch {
			batch[j].Index = uint64(i*16 + j + 1)
		}
		st.Append(batch)
		if i%64 == 63 {
			if err := st.Compact(uint64(i*16), raftpb.ConfState{}); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkStorageEntries(b *testing.B) {
	st := storageWith(ents(1, 1, 1024, 128))
	b.ReportAllocs()
	for range b.N {
		if _, err := st.Entries(1, 65, 1<<20); err != nil {
			b.Fatal(err)
		}
	}
}
