package chunkstore

import (
	"bytes"
	"errors"
	"math"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

func (h *harness) gated(c core.ChunkID, off uint32, data []byte, interval uint32) uint64 {
	return h.gatedAt(c, off, data, interval, 0)
}

func (h *harness) gatedAt(c core.ChunkID, off uint32, data []byte, interval uint32, epoch uint64) uint64 {
	id := h.change(c)
	h.send(id, change{c: c, off: off, n: uint32(len(data)), data: data, epoch: epoch})
	h.s.Write(&h.ctx, Request{ID: id, Chunk: c, Off: off, Len: uint32(len(data)), Buf: h.writeBuf(off, data), Interval: interval, Epoch: epoch, Gated: true}, &h.out)
	h.take()
	return id
}

func (h *harness) commit(c core.ChunkID, v uint64) {
	h.s.Commit(&h.ctx, c, v, &h.out)
	h.take()
}

func TestVersionsAreNumberedFromTheInterval(t *testing.T) {
	const top = math.MaxUint32
	for _, tc := range []struct {
		newest   uint64
		interval uint32
		want     uint64
	}{
		{0, 7, MakeVersion(7, 1)},
		{MakeVersion(7, 5), 7, MakeVersion(7, 6)},
		{MakeVersion(6, 9), 7, MakeVersion(7, 1)},
		{MakeVersion(9, 4), 10, MakeVersion(10, 1)},
		{MakeVersion(7, top-3), 7, MakeVersion(7, top-2)},
		{MakeVersion(7, top), 7, 0},
		{MakeVersion(8, 1), 7, 0},
	} {
		got, ok := intervalVersion(tc.newest, tc.interval)
		if got != tc.want || ok != (tc.want != 0) {
			t.Errorf("after %#x in interval %d: %#x %v, want %#x", tc.newest, tc.interval, got, ok, tc.want)
		}
	}
	if MakeVersion(1, top) >= MakeVersion(2, 1) {
		t.Fatal("1.(2^32-1) does not order below 2.1")
	}
}

func TestAGatedWriteIsMappedOnlyOnceCommitted(t *testing.T) {
	h := newHarness(t, testConfig())
	base := h.sync(h.write(chunkC, 0, bytes.Repeat([]byte{1}, core.BlockSize), 0))
	w := h.gated(chunkC, 100, []byte{9, 9}, 7)
	h.settle()
	if _, ok := h.replies[w]; ok {
		t.Fatal("a gated write was answered before its commit")
	}
	lg, ok := h.durableAt[w]
	if !ok || lg.Err != nil || lg.Version != MakeVersion(7, 1) {
		t.Fatalf("logged %+v %v, want 7.1", lg, ok)
	}
	merged := h.ordered[w]
	if len(merged) != core.BlockSize || merged[0] != 1 || merged[100] != 9 || merged[102] != 1 {
		t.Fatalf("ordered blocks not merged on the base: % x", merged[:104])
	}
	if got := h.readNow(chunkC, 100, 2); !bytes.Equal(got, []byte{1, 1}) {
		t.Fatalf("read before the commit returns % x, want the committed bytes", got)
	}
	if r := h.readAt(chunkC, 0, 1, lg.Version); h.settleReply(r).Err == nil {
		t.Fatal("at_ver of a pending version was served")
	}
	h.commit(chunkC, lg.Version)
	h.settle()
	if v := h.ok(w); v != lg.Version {
		t.Fatalf("answered %#x, want %#x", v, lg.Version)
	}
	if got := h.readNow(chunkC, 99, 4); !bytes.Equal(got, []byte{1, 9, 9, 1}) {
		t.Fatalf("read after the commit returns % x", got)
	}
	if base == lg.Version {
		t.Fatal("no new version")
	}
}

func (h *harness) settleReply(id uint64) Reply {
	h.t.Helper()
	h.settle()
	r := h.reply(id)
	if r.Buf != core.NoBuf {
		h.pool.Free(r.Buf)
	}
	return r
}

// Commits that arrive out of version order are still mapped in version order; the mapOnCommit
// plant maps them as they arrive and leaves the older version's bytes in the index.
func TestCommitsAreMappedInVersionOrder(t *testing.T) {
	for _, plant := range []bool{false, true} {
		h := newHarness(t, testConfig())
		h.s.plant.mapOnCommit = plant
		if plant {
			h.inv.Off(invMove)
			h.inv.Off(invReads)
		}
		w1 := h.gated(chunkC, 0, []byte{1}, 3)
		w2 := h.gated(chunkC, 0, []byte{2}, 3)
		h.settle()
		h.commit(chunkC, h.durableAt[w2].Version)
		h.commit(chunkC, h.durableAt[w1].Version)
		h.settle()
		h.ok(w1)
		h.ok(w2)
		got := h.readNow(chunkC, 0, 1)[0]
		if plant != (got != 2) {
			t.Fatalf("plant %v: reads %d after both commits", plant, got)
		}
	}
}

func (h *harness) settleChunk(c core.ChunkID, keep uint64) {
	if err := h.s.Settle(&h.ctx, c, keep, &h.out); err != nil {
		h.t.Fatal(err)
	}
	h.take()
	h.settle()
}

func TestADroppedWriteIsNeverMappedNorMergedOn(t *testing.T) {
	h := newHarness(t, testConfig())
	base := h.sync(h.write(chunkC, 0, []byte{1, 1, 1}, 0))
	w1 := h.gated(chunkC, 0, []byte{5}, 4)
	w2 := h.gated(chunkC, 1, []byte{6}, 4)
	h.settle()
	h.settleChunk(chunkC, base)
	for _, w := range []uint64{w1, w2} {
		if r := h.reply(w); !errors.Is(r.Err, ErrDropped) {
			t.Fatalf("dropped write answered %+v", r)
		}
	}
	w3 := h.gated(chunkC, 2, []byte{7}, 5)
	h.settle()
	if got := h.ordered[w3][:3]; !bytes.Equal(got, []byte{1, 1, 7}) {
		t.Fatalf("a write after the drop merged on % x, want the committed bytes", got)
	}
	if v := h.durableAt[w3].Version; v != MakeVersion(5, 1) {
		t.Fatalf("the next version is %#x, want 5.1", v)
	}
	h.commit(chunkC, h.durableAt[w3].Version)
	h.settle()
	if got := h.readNow(chunkC, 0, 3); !bytes.Equal(got, []byte{1, 1, 7}) {
		t.Fatalf("reads % x", got)
	}
}

func TestASpentCounterRefusesTheWrite(t *testing.T) {
	h := newHarness(t, testConfig())
	id := h.change(chunkC)
	h.s.Write(&h.ctx, Request{ID: id, Chunk: chunkC, Off: 0, Len: core.BlockSize, Buf: h.writeBuf(0, make([]byte, core.BlockSize)), Version: MakeVersion(7, math.MaxUint32)}, &h.out)
	h.take()
	h.sync(id)
	w := h.gated(chunkC, 0, []byte{2}, 7)
	h.settle()
	if r := h.reply(w); !errors.Is(r.Err, ErrCounter) {
		t.Fatalf("answered %+v, want ErrCounter", r)
	}
	if _, ok := h.ordered[w]; ok {
		t.Fatal("a refused write was ordered")
	}
	w = h.gated(chunkC, 0, []byte{2}, 8)
	h.settle()
	if v := h.durableAt[w].Version; v != MakeVersion(8, 1) {
		t.Fatalf("got %#x in interval 8", v)
	}
	h.commit(chunkC, MakeVersion(8, 1))
	h.settle()
}

func TestAMemberLogsThePrimarysVersionsAndEdges(t *testing.T) {
	h := newHarness(t, testConfig())
	buf := h.writeBuf(0, bytes.Repeat([]byte{4}, 2*core.BlockSize))
	id := h.change(chunkC)
	h.s.Write(&h.ctx, Request{ID: id, Chunk: chunkC, Off: 0, Len: 2 * core.BlockSize, Buf: buf, Version: MakeVersion(5, 2)}, &h.out)
	h.take()
	if v := h.sync(id); v != MakeVersion(5, 2) {
		t.Fatalf("logged %#x", v)
	}
	if h.s.Newest(chunkC) != MakeVersion(5, 2) {
		t.Fatalf("newest %#x", h.s.Newest(chunkC))
	}
	edge := h.writeBuf(0, bytes.Repeat([]byte{8}, core.BlockSize))
	id = h.change(chunkC)
	h.s.Punch(&h.ctx, Request{ID: id, Chunk: chunkC, Off: 100, Len: 2*core.BlockSize - 100, Buf: edge, Version: MakeVersion(5, 3)}, &h.out)
	h.take()
	h.sync(id)
	got := h.readNow(chunkC, 0, 2*core.BlockSize)
	if got[0] != 8 || got[core.BlockSize-1] != 8 || got[core.BlockSize] != 0 {
		t.Fatalf("punch with a sent edge reads % x ... % x", got[:2], got[core.BlockSize-1:core.BlockSize+1])
	}
	id = h.change(chunkC)
	h.s.Write(&h.ctx, Request{ID: id, Chunk: chunkC, Off: 0, Len: 1, Buf: h.writeBuf(0, []byte{1}), Version: MakeVersion(5, 3)}, &h.out)
	h.take()
	h.settle()
	if r := h.reply(id); !errors.Is(r.Err, ErrVersion) {
		t.Fatalf("a version not above the newest answered %+v", r)
	}
}

func TestAWriteOfAnOlderIncarnationIsFencedWhenOrdered(t *testing.T) {
	h := newHarness(t, testConfig())
	h.sync(h.write(chunkC, 0, []byte{1}, 0))
	h.s.SeeIncarnation(9, 100)
	ok := h.change(chunkC)
	h.s.Write(&h.ctx, Request{ID: ok, Chunk: chunkC, Off: core.BlockSize, Len: core.BlockSize, Buf: h.writeBuf(0, make([]byte, core.BlockSize)), Session: 9, Incarnation: 100}, &h.out)
	h.take()
	held := h.change(chunkC)
	h.s.Write(&h.ctx, Request{ID: held, Chunk: chunkC, Off: 6, Len: 1, Buf: h.writeBuf(6, []byte{3}), Session: 9, Incarnation: 100}, &h.out)
	h.take()
	h.s.SeeIncarnation(9, 101)
	h.settle()
	h.ok(ok)
	if r := h.reply(held); !errors.Is(r.Err, ErrFenced) {
		t.Fatalf("a write queued behind a base read when a newer incarnation came answered %+v", r)
	}
}

func TestAMemberServesOnlyFrozenVersions(t *testing.T) {
	h := newHarness(t, testConfig())
	v := h.sync(h.write(chunkC, 0, []byte{1}, 0))
	id := h.id()
	h.s.Read(&h.ctx, Request{ID: id, Chunk: chunkC, Off: 0, Len: 1, FrozenOnly: true}, &h.out)
	h.take()
	if r := h.settleReply(id); !errors.Is(r.Err, ErrNoVersion) {
		t.Fatalf("a member's live read answered %+v", r)
	}
	id = h.id()
	h.s.Read(&h.ctx, Request{ID: id, Chunk: chunkC, Off: 0, Len: 1, FrozenOnly: true, HasVer: true, Ver: v}, &h.out)
	h.take()
	if r := h.settleReply(id); !errors.Is(r.Err, ErrNoVersion) {
		t.Fatalf("a member's read at its newest version, not frozen, answered %+v", r)
	}
}

// A dropped gated write that froze epoch 1 gives the freeze back: the next write at epoch 1 freezes 1.1 again before it overwrites block 0.
func TestADroppedVersionGivesBackItsFreeze(t *testing.T) {
	h := newHarness(t, testConfig())
	base := h.sync(h.write(chunkC, 0, []byte{1}, 0))
	dropped := h.gatedAt(chunkC, 0, []byte{2}, 1, 1)
	h.settle()
	h.settleChunk(chunkC, base)
	if r := h.reply(dropped); !errors.Is(r.Err, ErrDropped) {
		t.Fatalf("the dropped write answered %+v", r)
	}
	w := h.gatedAt(chunkC, 0, []byte{3}, 2, 1)
	h.settle()
	h.commit(chunkC, h.durableAt[w].Version)
	h.settle()
	h.ok(w)
	if v, ok := h.s.Index().FrozenAt(testPool, chunkC, 1); !ok || v != base {
		t.Fatalf("epoch 1 froze %#x %v, want %#x", v, ok, base)
	}
	if got := h.block(chunkC, 0, 1); got[0] != 1 {
		t.Fatalf("epoch 1 reads %d", got[0])
	}
}

// Peering's decision on a chunk with 4.1 to 4.3 pending keeps 4.2: 4.1 and 4.2 are mapped, 4.3 is dropped, and the next interval's first write merges on 4.2.
func TestSettleMapsTheKeptVersionsAndDropsTheRest(t *testing.T) {
	h := newHarness(t, testConfig())
	w1 := h.gated(chunkC, 0, []byte{1}, 4)
	w2 := h.gated(chunkC, 1, []byte{2}, 4)
	w3 := h.gated(chunkC, 2, []byte{3}, 4)
	h.settle()
	h.settleChunk(chunkC, MakeVersion(4, 2))
	if v := h.ok(w2); v != MakeVersion(4, 2) {
		t.Fatalf("kept 4.2 answered %#x", v)
	}
	h.ok(w1)
	if r := h.reply(w3); !errors.Is(r.Err, ErrDropped) {
		t.Fatalf("dropped 4.3 answered %+v", r)
	}
	if got := h.readNow(chunkC, 0, 3); !bytes.Equal(got, []byte{1, 2, 0}) {
		t.Fatalf("reads % x", got)
	}
	w := h.gated(chunkC, 3, []byte{4}, 5)
	h.settle()
	if got, v := h.ordered[w][:4], h.durableAt[w].Version; !bytes.Equal(got, []byte{1, 2, 0, 4}) || v != MakeVersion(5, 1) {
		t.Fatalf("the next write is %#x, merged on % x", v, got)
	}
}

// A member's 3.1 and 3.2 wait for write admission, and 3.1 then fails before it is logged, the pool too full to cut it into records: 3.2 is refused, never logged without 3.1.
func TestAMemberNeverLogsAVersionAfterOneThatFailed(t *testing.T) {
	h := newHarness(t, testConfig())
	other := core.ChunkID{Volume: 1, Inode: 7, Index: 4}
	h.gated(other, 0, []byte{1}, 3)
	h.settle()
	oi := h.s.chunkOf[other]
	h.s.adm.held, h.s.chunks[oi].held = []int32{oi}, true
	n := int(h.s.maxWrite) + 1
	if n >= core.BlocksPerChunk {
		t.Fatalf("a write of %d blocks is one item", n)
	}
	member := func(v uint64, blocks int) uint64 {
		id := h.id()
		h.s.Write(&h.ctx, Request{ID: id, Chunk: chunkC, Len: uint32(blocks) * core.BlockSize, Buf: h.writeBuf(0, make([]byte, blocks*core.BlockSize)), Version: v}, &h.out)
		h.take()
		return id
	}
	first, next := member(MakeVersion(3, 1), n), member(MakeVersion(3, 2), 1)
	var hog []core.IOBuf
	for {
		b, err := h.pool.Alloc(core.BlockSize)
		if err != nil {
			break
		}
		hog = append(hog, b)
	}
	h.s.adm.heldHead = 1
	h.s.OnEvent(&h.ctx, &core.Event{Kind: core.EvTimer, Timer: AdmitTimer}, &h.out)
	h.take()
	for _, b := range hog {
		h.pool.Free(b)
	}
	h.settle()
	if r := h.reply(first); r.Err == nil {
		t.Fatalf("3.1 answered %+v with the pool full", r)
	}
	if r := h.reply(next); r.Err == nil {
		t.Fatalf("3.2 answered %+v after 3.1 failed", r)
	}
}

// A gated write that fails with none of its items durable leaves the chunk at its version: its primary may have sent that version to members, so it is never handed out again with other bytes.
func TestAFailedGatedWriteKeepsItsVersion(t *testing.T) {
	h := newHarness(t, testConfig())
	ci := h.s.chunkSlot(chunkC)
	oi := h.s.newOp(opWrite, Request{Chunk: chunkC, Gated: true, Interval: 4})
	o := &h.s.ops[oi]
	v := MakeVersion(4, 1)
	o.chunk, o.version, o.left = ci, v, 1
	o.items = append(o.items, item{entry: disklog.WriteEntry(testPool, chunkC, v, 0, 1)})
	cs := &h.s.chunks[ci]
	cs.version = v
	cs.pending = append(cs.pending, oi)
	h.s.itemFailed(&h.ctx, oi, 0, disklog.ErrNoSpace, &h.out)
	h.take()
	if cs.version != v {
		t.Fatalf("chunk at %#x after 4.1 failed, want 4.1", cs.version)
	}
}

// Settle refuses a chunk whose gated write has a record neither durable nor failed, settling nothing, and settles it once the record is durable.
func TestSettleWaitsForTheChunksWritesToResolve(t *testing.T) {
	h := newHarness(t, testConfig())
	w := h.gated(chunkC, 0, []byte{1}, 4)
	if err := h.s.Settle(&h.ctx, chunkC, MakeVersion(4, 1), &h.out); !errors.Is(err, ErrUnresolved) {
		t.Fatalf("Settle with 4.1 in flight: %v", err)
	}
	h.take()
	h.settle()
	if r, ok := h.replies[w]; ok {
		t.Fatalf("4.1 answered %+v before a Settle took", r)
	}
	h.settleChunk(chunkC, MakeVersion(4, 1))
	if v := h.ok(w); v != MakeVersion(4, 1) {
		t.Fatalf("answered %#x", v)
	}
}
