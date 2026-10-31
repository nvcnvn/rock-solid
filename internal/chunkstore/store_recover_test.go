package chunkstore

import (
	"bytes"
	"errors"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// recoveryBuf builds a recovery buffer: the given blocks written, each filled with its byte, and
// the given blocks punched.
func recoveryBuf(h *harness, data map[uint16]byte, holes ...uint16) core.IOBuf {
	var m blockMap
	for b := range data {
		m.data.set(b)
	}
	for _, b := range holes {
		m.holes.set(b)
	}
	buf, err := h.pool.Alloc((1 + len(data)) * core.BlockSize)
	if err != nil {
		h.t.Fatal(err)
	}
	dst := h.pool.Bytes(buf)
	m.put(dst)
	k := 1
	m.data.runs(func(first, n uint16) {
		for b := first; b < first+n; b++ {
			copy(dst[k*core.BlockSize:][:core.BlockSize], fill(core.BlockSize, data[b]))
			k++
		}
	})
	return buf
}

// memberWrite logs whole blocks [first, first+n) of c filled with v as a member does, at version.
func (h *harness) memberWrite(c core.ChunkID, first, n uint16, version uint64, v byte) uint64 {
	buf, err := h.pool.Alloc(int(n) * core.BlockSize)
	if err != nil {
		h.t.Fatal(err)
	}
	copy(h.pool.Bytes(buf), fill(int(n)*core.BlockSize, v))
	id := h.id()
	h.changed = true
	h.s.Write(&h.ctx, Request{ID: id, Chunk: c, Off: uint32(first) * core.BlockSize, Len: uint32(n) * core.BlockSize, Buf: buf, Version: version}, &h.out)
	h.take()
	return id
}

func (h *harness) recoverWrite(r Request) uint64 {
	r.ID, r.Off, r.Len, r.Recover = h.id(), 0, core.ChunkSize, true
	h.changed = true
	h.s.Write(&h.ctx, r, &h.out)
	h.take()
	return r.ID
}

// mapOf decodes a recovery buffer's map and gives each block it carries as its first byte.
func mapOf(t *testing.T, b []byte) (blockMap, map[uint16]byte) {
	t.Helper()
	var m blockMap
	if err := decodeBlockMap(b, &m); err != nil {
		t.Fatal(err)
	}
	got := map[uint16]byte{}
	k := 1
	m.data.runs(func(first, n uint16) {
		for x := first; x < first+n; x++ {
			got[x] = b[k*core.BlockSize]
			k++
		}
	})
	return m, got
}

// A recovery read gives the chunk's live image: the written blocks' bytes packed after the map,
// and every other block, punched or never written, a hole. The store reads a chunk it lists too:
// its replica decides whether its copy can be a source.
func TestARecoveryReadIsTheChunksLiveImage(t *testing.T) {
	h := newRecoveryHarness(t)
	h.sync(h.write(chunkC, 0, fill(2*core.BlockSize, 3), 0))
	h.sync(h.write(chunkC, 5*core.BlockSize, fill(core.BlockSize, 7), 0))
	h.sync(h.punch(chunkC, core.BlockSize, core.BlockSize, 0))
	id := h.id()
	h.s.Read(&h.ctx, Request{ID: id, Chunk: chunkC, Off: 0, Len: core.ChunkSize, Recover: true}, &h.out)
	h.take()
	h.settle()
	r := h.reply(id)
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	m, got := mapOf(t, h.pool.Bytes(r.Buf))
	h.pool.Free(r.Buf)
	if len(got) != 2 || got[0] != 3 || got[5] != 7 || m.holes.count() != core.BlocksPerChunk-2 || !m.holes.has(1) {
		t.Fatalf("image %v with %d holes", got, m.holes.count())
	}
	h.sync(h.persist(0, 1, 2, []ChunkVersion{{Chunk: chunkC, Version: r.Version}}))
	id = h.id()
	h.s.Read(&h.ctx, Request{ID: id, Chunk: chunkC, Len: core.ChunkSize, Recover: true}, &h.out)
	h.take()
	h.settle()
	if r := h.reply(id); r.Err != nil {
		t.Fatalf("a recovery read of a listed chunk, as a primary whose index holds the kept version reads itself: %v", r.Err)
	} else {
		h.pool.Free(r.Buf)
	}
}

// A member's recovery write makes the chunk the image it carries, punching its holes over a
// dropped version's blocks, at the version of the chunk's write before it, which it takes no
// version above: one that only kept versions preceded in the interval brings the member back down
// to the kept version past the dropped ones. It holds that version whole, and alone removes the
// chunk's missing-list entry: a later peering's entry stays, and so does every entry across a
// restart.
func TestARecoveryWriteReplacesAMembersChunk(t *testing.T) {
	h := newRecoveryHarness(t)
	c2 := core.ChunkID{Volume: 1, Inode: 7, Index: 9}
	kept := MakeVersion(3, 1)
	for _, c := range []core.ChunkID{chunkC, c2} {
		h.sync(h.memberWrite(c, 0, 4, kept, 1))
		h.sync(h.memberWrite(c, 2, 2, MakeVersion(3, 2), 9))
	}
	h.changed = true
	h.sync(h.persist(0, 1, 4, []ChunkVersion{{Chunk: chunkC, Version: kept}, {Chunk: c2, Version: kept}}))
	h.sync(h.memberWrite(chunkC, 0, 1, MakeVersion(4, 1), 5))
	if ch, _ := h.s.Index().Chunk(testPool, chunkC); ch.ListedIn != 4 || ch.Listed != kept {
		t.Fatalf("a write of the interval cleared the entry: %+v", ch)
	}
	img := map[uint16]byte{1: 1, 2: 1}
	h.sync(h.recoverWrite(Request{Chunk: chunkC, HasVer: true, Ver: MakeVersion(4, 1), Interval: 4, Buf: recoveryBuf(h, img, 3)}))
	h.sync(h.recoverWrite(Request{Chunk: c2, HasVer: true, Ver: kept, Interval: 4, Buf: recoveryBuf(h, map[uint16]byte{0: 1, 1: 1, 2: 1, 3: 1})}))
	restart(h)
	for c, v := range map[core.ChunkID]uint64{chunkC: MakeVersion(4, 1), c2: kept} {
		if ch, _ := h.s.Index().Chunk(testPool, c); ch.ListedIn != 0 || ch.Version != v || ch.Whole != v {
			t.Fatalf("after recovery %v holds %+v", c, ch)
		}
	}
	for b, want := range []byte{5, 1, 1, 0, 0} {
		if got := h.readNow(chunkC, uint32(b)*core.BlockSize, core.BlockSize); !bytes.Equal(got, fill(core.BlockSize, want)) {
			t.Fatalf("block %d reads %#x, want %#x", b, got[0], want)
		}
	}
	for b := range uint32(4) {
		if got := h.readNow(c2, b*core.BlockSize, core.BlockSize); !bytes.Equal(got, fill(core.BlockSize, 1)) {
			t.Fatalf("block %d of the chunk brought back to the kept version reads %#x", b, got[0])
		}
	}
	h.changed = true
	h.sync(h.persist(0, 1, 6, []ChunkVersion{{Chunk: chunkC, Version: MakeVersion(4, 1)}}))
	h.sync(h.recoverWrite(Request{Chunk: chunkC, HasVer: true, Ver: MakeVersion(4, 1), Interval: 5}))
	restart(h)
	if ch, _ := h.s.Index().Chunk(testPool, chunkC); ch.ListedIn != 6 {
		t.Fatalf("recovery of interval 5 over an entry of interval 6: %+v", ch)
	}
	if got := h.block(chunkC, 1, 0); got[0] != 1 {
		t.Fatalf("a recovery write with no buffer changed block 1 to %#x", got[0])
	}
}

// A primary's recovery write leaves out every block a gated write of its interval ordered before it
// covers, writing or punching only the rest, at that write's version, and hands the members that
// remainder; a whole-chunk punch of the interval leaves it nothing to carry, and one ordered before
// any write of the interval takes the kept version. One that relays logs nothing.
func TestAPrimarysRecoveryWriteLeavesOutWhatTheIntervalCovered(t *testing.T) {
	h := newRecoveryHarness(t)
	k := uint32(7)
	h.s.Cover(chunkC, k)
	w := h.id()
	h.s.Write(&h.ctx, Request{ID: w, Chunk: chunkC, Len: core.BlockSize, Buf: h.writeBuf(0, fill(core.BlockSize, 8)), Interval: k, Gated: true}, &h.out)
	h.take()
	img := map[uint16]byte{0: 1, 1: 1, 2: 1}
	rec := h.recoverWrite(Request{Chunk: chunkC, Interval: k, Gated: true, Ver: MakeVersion(3, 1), Buf: recoveryBuf(h, img, 3, 4)})
	h.settle()
	m, got := mapOf(t, h.ordered[rec])
	if len(got) != 2 || got[1] != 1 || got[2] != 1 || m.holes.count() != 2 || m.data.has(0) {
		t.Fatalf("the members get %v with holes %d", got, m.holes.count())
	}
	v1, v2 := h.durableAt[w].Version, h.durableAt[rec].Version
	if v2 != v1 || intervalOf(v1) != k {
		t.Fatalf("recovery took %#x after %#x", v2, v1)
	}
	h.s.Commit(&h.ctx, chunkC, rec, &h.out)
	h.take()
	h.settle()
	if ch, _ := h.s.Index().Chunk(testPool, chunkC); ch.Version != 0 {
		t.Fatalf("the recovery write, committed before the write it follows, was mapped: %+v", ch)
	}
	h.s.Commit(&h.ctx, chunkC, w, &h.out)
	h.take()
	h.settle()
	for b, want := range []byte{8, 1, 1, 0} {
		if got := h.readNow(chunkC, uint32(b)*core.BlockSize, core.BlockSize); !bytes.Equal(got, fill(core.BlockSize, want)) {
			t.Fatalf("block %d reads %#x, want %#x", b, got[0], want)
		}
	}

	c2 := core.ChunkID{Volume: 1, Inode: 7, Index: 9}
	h.s.Cover(c2, k)
	p := h.id()
	h.s.Punch(&h.ctx, Request{ID: p, Chunk: c2, Off: 0, Len: core.ChunkSize, Interval: k, Gated: true, Lacking: true}, &h.out)
	h.take()
	rec = h.recoverWrite(Request{Chunk: c2, Interval: k, Gated: true, Buf: recoveryBuf(h, img)})
	h.settle()
	if b := h.ordered[rec]; len(b) != 0 {
		t.Fatalf("after a whole-chunk punch the members get %d bytes", len(b))
	}

	c3 := core.ChunkID{Volume: 1, Inode: 7, Index: 10}
	h.s.Cover(c3, k)
	rec = h.recoverWrite(Request{Chunk: c3, Interval: k, Gated: true, Relay: true, Ver: MakeVersion(3, 1), Buf: recoveryBuf(h, img)})
	h.settle()
	if _, got := mapOf(t, h.ordered[rec]); len(got) != 3 || h.durableAt[rec].Version != MakeVersion(3, 1) {
		t.Fatalf("relaying at %#x hands the members %v", h.durableAt[rec].Version, got)
	}
	for _, r := range h.records {
		for _, e := range r.entries {
			if e.Chunk == c3 {
				t.Fatalf("relaying logged %+v", e)
			}
		}
	}
}

// A recovery buffer's map round-trips, and one whose blocks are written and punched at once, whose
// first block has bytes past the map or whose length is not its written blocks is refused.
func TestBlockMapErrors(t *testing.T) {
	var m blockMap
	m.data.setRange(3, 5)
	m.holes.set(1000)
	buf := make([]byte, 4*core.BlockSize)
	m.put(buf)
	var got blockMap
	if err := decodeBlockMap(buf, &got); err != nil || got != m {
		t.Fatalf("decoded %v, %v", got, err)
	}
	for what, b := range map[string][]byte{
		"short":         buf[:3*core.BlockSize],
		"long":          append(bytes.Clone(buf), make([]byte, core.BlockSize)...),
		"part of block": buf[:core.BlockSize+1],
		"empty":         nil,
	} {
		if err := decodeBlockMap(b, &got); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: %v", what, err)
		}
	}
	both := bytes.Clone(buf)
	both[8*blockWords] = 1 << 3
	tail := bytes.Clone(buf)
	tail[300] = 1
	for what, b := range map[string][]byte{"written and punched": both, "bytes past the map": tail} {
		if err := decodeBlockMap(b, &got); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: %v", what, err)
		}
	}
}

// A primary that lacks a chunk's kept version orders a punch of it although its own store holds
// nothing of the chunk, which its members may hold, rather than answering it done: the punch would
// otherwise be acknowledged and never reach them. One that does not lack the chunk logs nothing for
// a punch of a chunk with no version, as a lone store does.
func TestAPrimaryOrdersAPunchOfAChunkItHoldsNothingOf(t *testing.T) {
	h := newRecoveryHarness(t)
	p := h.id()
	h.s.Punch(&h.ctx, Request{ID: p, Chunk: chunkC, Len: core.ChunkSize, Interval: 7, Gated: true, Lacking: true}, &h.out)
	h.take()
	h.settle()
	if _, ok := h.ordered[p]; !ok {
		t.Fatalf("the punch was not ordered: %+v", h.replies[p])
	}
	c := core.ChunkID{Volume: 1, Inode: 7, Index: 9}
	p = h.id()
	h.s.Punch(&h.ctx, Request{ID: p, Chunk: c, Off: 100, Len: core.BlockSize, Interval: 7, Gated: true, Lacking: true}, &h.out)
	h.take()
	h.settle()
	if got := h.ordered[p]; !bytes.Equal(got, make([]byte, 2*core.BlockSize)) {
		t.Fatalf("a punch in part of two blocks hands the members %d bytes of edges", len(got))
	}
	c.Index = 10
	p = h.id()
	h.s.Punch(&h.ctx, Request{ID: p, Chunk: c, Len: core.ChunkSize, Interval: 7, Gated: true}, &h.out)
	h.take()
	h.settle()
	if _, ok := h.ordered[p]; ok || h.replies[p].Err != nil {
		t.Fatalf("a punch of a chunk absent everywhere was ordered (%v) or answered %+v", ok, h.replies[p])
	}
}

// A gated write the store fails before ordering it, for want of a pool buffer for its second
// record, neither covers blocks of a recovery write nor lends it its version: the recovery write
// ordered next carries every block and takes the kept version, so the member it recovers holds the
// image the others hold.
func TestAWriteFailedBeforeItIsOrderedCoversNothing(t *testing.T) {
	h := newRecoveryHarness(t)
	k := uint32(7)
	h.s.Cover(chunkC, k)
	n := 32
	wbuf := h.writeBuf(0, fill(n*core.BlockSize, 8))
	var hoard []core.IOBuf
	for {
		b, err := h.pool.Alloc(core.BlockSize)
		if err != nil {
			break
		}
		hoard = append(hoard, b)
	}
	w := h.id()
	h.s.Write(&h.ctx, Request{ID: w, Chunk: chunkC, Len: uint32(n * core.BlockSize), Buf: wbuf, Interval: k, Gated: true}, &h.out)
	h.take()
	for _, b := range hoard {
		h.pool.Free(b)
	}
	h.settle()
	if _, ordered := h.ordered[w]; ordered || h.replies[w].Err == nil {
		t.Fatalf("the write was ordered (%v) or answered %+v with no pool buffer", ordered, h.replies[w])
	}
	img := map[uint16]byte{}
	for b := range uint16(40) {
		img[b] = 1
	}
	kept := MakeVersion(3, 1)
	rec := h.recoverWrite(Request{Chunk: chunkC, Interval: k, Gated: true, Ver: kept, Buf: recoveryBuf(h, img)})
	h.settle()
	m, _ := mapOf(t, h.ordered[rec])
	if v := h.durableAt[rec].Version; m.data.count() != 40 || v != kept {
		t.Fatalf("the recovery write carries %d of 40 blocks at %#x, want all at %#x", m.data.count(), v, kept)
	}
}

// A member refuses a recovery write logged under a version it does not stand at: one of the
// interval other than its own newest, or the kept version once it holds a write of the interval,
// since the write would then lie under a version whose bytes the other members hold differently.
func TestAMemberRefusesARecoveryWriteAtAVersionItLacks(t *testing.T) {
	h := newRecoveryHarness(t)
	kept := MakeVersion(3, 1)
	h.sync(h.memberWrite(chunkC, 0, 4, kept, 1))
	h.sync(h.memberWrite(chunkC, 0, 1, MakeVersion(4, 1), 5))
	img := map[uint16]byte{1: 1}
	for _, v := range []uint64{MakeVersion(4, 2), kept} {
		id := h.recoverWrite(Request{Chunk: chunkC, HasVer: true, Ver: v, Interval: 4, Buf: recoveryBuf(h, img)})
		h.settle()
		if !errors.Is(h.replies[id].Err, ErrVersion) {
			t.Fatalf("a recovery write at %#x over 4.1 answered %+v", v, h.replies[id])
		}
	}
}

// A recovery read the buffer pool cannot hold fails with the pool's error.
func TestARecoveryReadIntoAFullPoolFails(t *testing.T) {
	h := newRecoveryHarness(t)
	h.sync(h.write(chunkC, 0, fill(core.BlockSize, 3), 0))
	hog, err := h.pool.Alloc(h.pool.Available())
	if err != nil {
		t.Fatal(err)
	}
	id := h.id()
	h.s.Read(&h.ctx, Request{ID: id, Chunk: chunkC, Len: core.ChunkSize, Recover: true}, &h.out)
	h.take()
	var full *core.PoolExhaustedError
	if r := h.reply(id); !errors.As(r.Err, &full) || r.Buf != core.NoBuf {
		t.Fatalf("reply %+v", r)
	}
	h.pool.Free(hog)
}
