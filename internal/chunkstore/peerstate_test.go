package chunkstore

import (
	"errors"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// inPG is n chunks of PG pg of pgs PGs.
func inPG(pg, pgs uint32, n int) []core.ChunkID {
	var cs []core.ChunkID
	for i := uint32(0); len(cs) < n; i++ {
		if c := (core.ChunkID{Volume: 7, Inode: core.InodeID(1 + i)}); core.PGIndexOf(c.Hash(), pgs) == pg {
			cs = append(cs, c)
		}
	}
	return cs
}

func (h *harness) persist(pg, pgs, interval uint32, missing []ChunkVersion) uint64 {
	id := h.id()
	h.s.Persist(&h.ctx, id, pg, pgs, interval, missing, &h.out)
	h.take()
	return id
}

func (h *harness) peerState(pg, pgs uint32) ([]ChunkPeer, uint32) {
	h.t.Helper()
	got, decided, err := h.s.PeerState(pg, pgs, nil)
	if err != nil {
		h.t.Fatalf("PeerState(%d of %d): %v", pg, pgs, err)
	}
	return got, decided
}

// Every write and punch flags its version's last entry, and no other: across records, a merged
// punch's edge blocks and a freeze ahead of its blocks.
func TestTheLastEntryOfEachVersionIsFlagged(t *testing.T) {
	h := newRecoveryHarness(t)
	c, d := recoveryChunks[0], recoveryChunks[1]
	mw := int(h.s.maxWrite)
	h.sync(h.write(c, 0, fill(core.BlockSize, 1), 0))
	h.sync(h.write(c, 100, fill((2*mw+1)*core.BlockSize, 2), 1))
	h.sync(h.punch(c, 10, 3*core.BlockSize, 1))
	h.sync(h.punch(c, 2*core.BlockSize, core.BlockSize, 2))
	h.sync(h.write(d, 0, fill(core.BlockSize, 3), 2))
	h.sync(h.punch(d, 0, core.ChunkSize, 3))
	type key struct {
		c core.ChunkID
		v uint64
	}
	last, flagged := map[key][2]int{}, map[key][][2]int{}
	for i, r := range h.records {
		for j, e := range r.entries {
			if e.Kind != disklog.EntryWrite && e.Kind != disklog.EntryPunch && e.Kind != disklog.EntrySlot {
				continue
			}
			k := key{e.Chunk, e.Version}
			last[k] = [2]int{i, j}
			if e.Last {
				flagged[k] = append(flagged[k], [2]int{i, j})
			}
		}
	}
	if len(last) != 6 {
		t.Fatalf("logged %d versions, want 6", len(last))
	}
	for k, at := range last {
		if f := flagged[k]; len(f) != 1 || f[0] != at {
			t.Fatalf("chunk %v version %#x: last entry at %v, flagged %v", k.c, k.v, at, f)
		}
	}
}

// A version cut into several records that a crash leaves durable in part is logged but not held
// whole: the chunk's entry names it as the chunk's version and the version before it as the newest
// whole one. Once every record is durable both name it.
func TestAVersionHeldInPartIsNotWhole(t *testing.T) {
	c := recoveryChunks[0]
	parted, whole := false, false
	for k := range 16 {
		h := newRecoveryHarness(t)
		v1 := h.sync(h.write(c, 0, fill(core.BlockSize, 1), 0))
		base, mw := h.executed, int(h.s.maxWrite)
		h.crashIf = func(*core.Action) bool { return h.executed == base+k }
		h.write(c, 0, fill(2*mw*core.BlockSize, 2), 0)
		crashes(h.settle)
		h.crashIf = nil
		h.disk.Crash(func(int, core.LBA) bool { return false })
		h.recoverStore(disklog.ModeOpen)
		got, _ := h.s.Index().Chunk(testPool, c)
		switch {
		case got.Version == v1 && got.Whole == v1:
		case got.Version > v1 && got.Whole == v1:
			parted = true
		case got.Version > v1 && got.Whole == got.Version:
			whole = true
			if b := h.readNow(c, uint32(2*mw-1)*core.BlockSize, 1); b[0] != 2 {
				t.Fatalf("crash at action %d: whole version %#x reads %#x in its last block", k, got.Version, b[0])
			}
		default:
			t.Fatalf("crash at action %d: entry %+v after version %#x", k, got, v1)
		}
	}
	if !parted || !whole {
		t.Fatalf("crashes left the version in part %v, whole %v", parted, whole)
	}
}

// A missing list is answered once durable and taken whole: the member then reports each listed
// chunk with its kept version, held or not, and the decision's interval, refuses to read a listed
// chunk, and reports the same after a restart. Chunks of other PGs are left out.
func TestAPersistedMissingListListsItsChunks(t *testing.T) {
	h := newRecoveryHarness(t)
	ours, theirs := inPG(1, 2, 3), inPG(0, 2, 1)
	va := h.sync(h.write(ours[0], 0, fill(core.BlockSize, 1), 0))
	vb := h.sync(h.write(ours[1], 0, fill(core.BlockSize, 2), 0))
	h.sync(h.write(theirs[0], 0, fill(core.BlockSize, 3), 0))
	kept := MakeVersion(4, 2)
	id := h.persist(1, 2, 5, []ChunkVersion{{Chunk: ours[0], Version: kept}, {Chunk: ours[2]}})
	if _, _, err := h.s.PeerState(1, 2, nil); !errors.Is(err, ErrUnresolved) {
		t.Fatalf("PeerState while the list is being persisted: %v", err)
	}
	if _, ok := h.replies[id]; ok {
		t.Fatal("the missing list was answered before it was durable")
	}
	h.sync(id)
	want := []ChunkPeer{
		{Chunk: ours[0], Whole: va, Logged: va, Listed: true, Kept: kept},
		{Chunk: ours[1], Whole: vb, Logged: vb},
		{Chunk: ours[2], Listed: true},
	}
	slices.SortFunc(want, func(a, b ChunkPeer) int { return compareChunkKeys(a.Chunk, b.Chunk) })
	for round := range 2 {
		got, decided := h.peerState(1, 2)
		if !slices.Equal(got, want) || decided != 5 {
			t.Fatalf("round %d: PeerState %+v decided %d, want %+v decided 5", round, got, decided, want)
		}
		for _, c := range []core.ChunkID{ours[0], ours[2]} {
			rid := h.read(c, 0, 1)
			h.settle()
			if err := h.reply(rid).Err; !errors.Is(err, ErrMissing) {
				t.Fatalf("round %d: read of listed chunk %v: %v", round, c, err)
			}
		}
		if got := h.readNow(ours[1], 0, 1); got[0] != 2 {
			t.Fatalf("round %d: unlisted chunk reads %#x", round, got[0])
		}
		restart(h)
	}
	if got, decided := h.peerState(0, 2); len(got) != 1 || got[0].Chunk != theirs[0] || decided != 0 {
		t.Fatalf("the other PG reports %+v decided %d", got, decided)
	}
}

func compareChunkKeys(a, b core.ChunkID) int {
	return slices.Compare(appendChunkPrefix(nil, testPool, a), appendChunkPrefix(nil, testPool, b))
}

// A crash part way through persisting a list leaves either the whole list and its decision or
// neither, whichever records landed: replay takes missing-list entries only with their marker.
func TestAMissingListPersistsWholeOrNotAtAll(t *testing.T) {
	cs := inPG(0, 1, 40)
	missing := make([]ChunkVersion, len(cs))
	for i, c := range cs {
		missing[i] = ChunkVersion{Chunk: c, Version: MakeVersion(2, uint32(1+i))}
	}
	none, all := false, false
	for k := range 12 {
		h := newRecoveryHarness(t)
		h.settle()
		base := h.executed
		h.crashIf = func(*core.Action) bool { return h.executed == base+k }
		h.persist(0, 1, 3, missing)
		crashes(h.settle)
		h.crashIf = nil
		h.disk.Crash(func(int, core.LBA) bool { return true })
		h.recoverStore(disklog.ModeOpen)
		got, decided := h.peerState(0, 1)
		switch {
		case len(got) == 0 && decided == 0:
			none = true
		case len(got) == len(cs) && decided == 3:
			all = true
		default:
			t.Fatalf("crash at action %d: %d of %d chunks listed, decided %d", k, len(got), len(cs), decided)
		}
	}
	if !none || !all {
		t.Fatalf("crashes left no list %v, the whole list %v", none, all)
	}
}

// A decision marker covers the PG it was persisted for and both halves of each split since, and an
// older decision never replaces a newer one.
func TestADecisionCoversTheHalvesOfItsPG(t *testing.T) {
	h := newHarness(t, testConfig())
	h.sync(h.persist(1, 2, 3, nil))
	for _, c := range []struct{ pg, pgs, want uint32 }{{1, 2, 3}, {0, 2, 0}, {2, 4, 3}, {3, 4, 3}, {1, 4, 0}, {7, 8, 3}, {0, 1, 0}} {
		if got := h.s.Index().Decided(testPool, c.pg, c.pgs); got != c.want {
			t.Fatalf("PG %d of %d decided %d, want %d", c.pg, c.pgs, got, c.want)
		}
	}
	h.sync(h.persist(2, 4, 5, nil))
	h.sync(h.persist(1, 2, 2, nil))
	for _, c := range []struct{ pg, pgs, want uint32 }{{1, 2, 3}, {2, 4, 5}, {3, 4, 3}, {5, 8, 5}} {
		if got := h.s.Index().Decided(testPool, c.pg, c.pgs); got != c.want {
			t.Fatalf("PG %d of %d decided %d, want %d", c.pg, c.pgs, got, c.want)
		}
	}
}

// Persist refuses a PG count that is not a power of two, a PG past it, interval 0 and a chunk of
// another PG, logging nothing; PeerState refuses the same PGs, and waits while a write of the PG
// is pending.
func TestPeeringStateRefusals(t *testing.T) {
	h := newHarness(t, testConfig())
	other := inPG(0, 2, 1)
	mark := len(h.acts)
	for _, p := range []struct{ pg, pgs, interval uint32 }{{0, 3, 1}, {2, 2, 1}, {0, 0, 1}, {1, 2, 0}} {
		if err := h.reply(h.persist(p.pg, p.pgs, p.interval, nil)).Err; !errors.Is(err, ErrRange) {
			t.Fatalf("persist of PG %d of %d in interval %d: %v", p.pg, p.pgs, p.interval, err)
		}
		if _, _, err := h.s.PeerState(p.pg, p.pgs, nil); p.interval != 0 && !errors.Is(err, ErrRange) {
			t.Fatalf("PeerState of PG %d of %d: %v", p.pg, p.pgs, err)
		}
	}
	if err := h.reply(h.persist(1, 2, 1, []ChunkVersion{{Chunk: other[0]}})).Err; !errors.Is(err, ErrRange) {
		t.Fatalf("persist of a chunk of another PG: %v", err)
	}
	if n := h.actions(mark, core.ActDiskWrite); n != 0 {
		t.Fatalf("refused lists wrote %d times", n)
	}
	id := h.write(other[0], 0, fill(core.BlockSize, 1), 0)
	if _, _, err := h.s.PeerState(0, 2, nil); !errors.Is(err, ErrUnresolved) {
		t.Fatalf("PeerState with a write pending: %v", err)
	}
	if got, _ := h.peerState(1, 2); len(got) != 0 {
		t.Fatalf("PG 1 reports %+v while a write of PG 0 is pending", got)
	}
	h.sync(id)
	if got, _ := h.peerState(0, 2); len(got) != 1 {
		t.Fatalf("PG 0 reports %+v", got)
	}
}

// A primary whose disk lists a chunk answers its reads as missing, and serves the PG's other chunks.
func TestAPrimaryAnswersAListedChunkAsMissing(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	listed, other := chunkIn(pg0, 4, 0), chunkIn(pg0, 4, 1)
	c.until(c.write(pg0, listed, 0, []byte("a")))
	c.until(c.write(pg0, other, 0, []byte("b")))
	c.call(c.primary(pg0), func(ctx *core.Context, r *Replica, out *ReplicaOutput) {
		r.Store().Persist(ctx, 1<<40, pg0.Index, 4, 2, []ChunkVersion{{Chunk: listed, Version: MakeVersion(1, 1)}}, &r.sout)
		r.drain(ctx, out)
	})
	c.settle()
	id := c.read(pg0, listed, 0, 1, c.primary(pg0))
	c.until(id)
	if rp := c.reply(id); rp.Status != StatusMissing || rp.Version != 0 {
		t.Fatalf("read of the listed chunk answered %+v", rp)
	}
	if got := c.readNow(pg0, other, 0, 1); string(got) != "b" {
		t.Fatalf("read of an unlisted chunk %q", got)
	}
}

// Missing-list entries take effect only with the decision marker of their PG and interval, and the
// end of a replay drops those no marker followed.
func TestMissingEntriesWaitForTheirMarker(t *testing.T) {
	x := slotIndex(t)
	a, b := inPG(0, 2, 1)[0], inPG(1, 2, 1)[0]
	apply := func(es ...disklog.Entry) {
		t.Helper()
		if err := x.Apply(record(100, es...)); err != nil {
			t.Fatal(err)
		}
	}
	listed := func(c core.ChunkID) Chunk {
		ch, _ := x.Chunk(1, c)
		return ch
	}
	apply(disklog.MissingEntry(1, a, 5, 3), disklog.MissingEntry(1, b, 6, 3))
	apply(disklog.DecisionEntry(1, 0, 2, 4))
	if listed(a).ListedIn != 0 || listed(b).ListedIn != 0 {
		t.Fatal("a marker of another interval listed chunks")
	}
	apply(disklog.DecisionEntry(1, 0, 2, 3))
	if got := listed(a); got.Listed != 5 || got.ListedIn != 3 || listed(b).ListedIn != 0 {
		t.Fatalf("PG 0's marker listed %+v and %+v", got, listed(b))
	}
	x.EndReplay()
	apply(disklog.DecisionEntry(1, 1, 2, 3))
	if listed(b).ListedIn != 0 || x.Decided(1, 1, 2) != 3 || x.Decided(1, 0, 2) != 4 {
		t.Fatalf("after the replay ended: %+v, decided %d and %d", listed(b), x.Decided(1, 1, 2), x.Decided(1, 0, 2))
	}
	if err := x.Check(); err != nil {
		t.Fatal(err)
	}
}

// A primary's gated writes count once their records resolve: logged, and held whole up to the
// first whose records failed, here 4.2, though 4.3 after it is durable.
func TestAPrimaryReportsItsGatedWrites(t *testing.T) {
	h := newHarness(t, testConfig())
	h.sync(h.write(chunkC, 0, []byte{1}, 0))
	h.gated(chunkC, 0, []byte{2}, 4)
	if _, _, err := h.s.PeerState(0, 1, nil); !errors.Is(err, ErrUnresolved) {
		t.Fatalf("PeerState with 4.1 in flight: %v", err)
	}
	h.settle()
	if got, _ := h.peerState(0, 1); len(got) != 1 || got[0].Whole != MakeVersion(4, 1) || got[0].Logged != MakeVersion(4, 1) {
		t.Fatalf("reports %+v with 4.1 durable", got)
	}
	ci := h.s.chunkSlot(chunkC)
	oi := h.s.newOp(opWrite, Request{Chunk: chunkC, Gated: true, Interval: 4})
	o := &h.s.ops[oi]
	o.chunk, o.version, o.left = ci, MakeVersion(4, 2), 1
	o.items = append(o.items, item{entry: disklog.WriteEntry(testPool, chunkC, o.version, 0, 1)})
	h.s.chunks[ci].version = o.version
	h.s.chunks[ci].pending = append(h.s.chunks[ci].pending, oi)
	h.s.itemFailed(&h.ctx, oi, 0, disklog.ErrNoSpace, &h.out)
	h.take()
	h.gated(chunkC, 0, []byte{3}, 4)
	h.settle()
	if got, _ := h.peerState(0, 1); len(got) != 1 || got[0].Whole != MakeVersion(4, 1) || got[0].Logged != MakeVersion(4, 3) {
		t.Fatalf("reports %+v after 4.2 failed", got)
	}
}
