package raftshard

import (
	"maps"
	"reflect"
	"slices"
	"testing"

	"go.etcd.io/raft/v3/raftpb"

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
