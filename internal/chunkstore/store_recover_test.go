package chunkstore

import (
	"bytes"
	"errors"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
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
// and every other block, punched or never written, a hole.
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
	if err := h.reply(id).Err; !errors.Is(err, ErrMissing) {
		t.Fatalf("a recovery read of a listed chunk: %v", err)
	}
}

// A member's recovery write makes the chunk the image it carries, punching its holes over a
// dropped version's blocks, holds its version whole, and alone removes the chunk's missing-list
// entry: a later peering's entry stays, and so does every entry across a restart.
func TestARecoveryWriteReplacesAMembersChunk(t *testing.T) {
	h := newRecoveryHarness(t)
	h.sync(h.memberWrite(chunkC, 0, 4, MakeVersion(3, 1), 1))
	h.sync(h.memberWrite(chunkC, 2, 2, MakeVersion(3, 2), 9))
	kept := MakeVersion(3, 1)
	h.sync(h.persist(0, 1, 4, []ChunkVersion{{Chunk: chunkC, Version: kept}}))
	h.sync(h.memberWrite(chunkC, 0, 1, MakeVersion(4, 1), 5))
	if ch, _ := h.s.Index().Chunk(testPool, chunkC); ch.ListedIn != 4 || ch.Listed != kept {
		t.Fatalf("a write of the interval cleared the entry: %+v", ch)
	}
	rv := MakeVersion(4, 2)
	h.sync(h.recoverWrite(Request{Chunk: chunkC, Version: rv, Buf: recoveryBuf(h, map[uint16]byte{1: 1, 2: 1}, 3)}))
	restart(h)
	ch, _ := h.s.Index().Chunk(testPool, chunkC)
	if ch.ListedIn != 0 || ch.Version != rv || ch.Whole != rv {
		t.Fatalf("after recovery %+v", ch)
	}
	for b, want := range []byte{5, 1, 1, 0, 0} {
		if got := h.readNow(chunkC, uint32(b)*core.BlockSize, core.BlockSize); !bytes.Equal(got, fill(core.BlockSize, want)) {
			t.Fatalf("block %d reads %#x, want %#x", b, got[0], want)
		}
	}
	h.changed = true
	h.sync(h.persist(0, 1, 6, []ChunkVersion{{Chunk: chunkC, Version: rv}}))
	h.sync(h.recoverWrite(Request{Chunk: chunkC, Version: MakeVersion(5, 1)}))
	restart(h)
	if ch, _ := h.s.Index().Chunk(testPool, chunkC); ch.ListedIn != 6 || ch.Version != MakeVersion(5, 1) || ch.Whole != MakeVersion(5, 1) {
		t.Fatalf("recovery of interval 5 over an entry of interval 6: %+v", ch)
	}
	if got := h.block(chunkC, 1, 0); got[0] != 1 {
		t.Fatalf("a marker changed block 1 to %#x", got[0])
	}
}

// A primary's recovery write leaves out every block a gated write of its interval ordered before it
// covers, writing or punching only the rest, and hands the members that remainder; a whole-chunk
// punch of the interval leaves it nothing to carry. With MarkOnly the primary logs only the entry
// ending the version.
func TestAPrimarysRecoveryWriteLeavesOutWhatTheIntervalCovered(t *testing.T) {
	h := newRecoveryHarness(t)
	k := uint32(7)
	h.s.Cover(chunkC, k)
	w := h.id()
	h.s.Write(&h.ctx, Request{ID: w, Chunk: chunkC, Len: core.BlockSize, Buf: h.writeBuf(0, fill(core.BlockSize, 8)), Interval: k, Gated: true}, &h.out)
	h.take()
	img := map[uint16]byte{0: 1, 1: 1, 2: 1}
	rec := h.recoverWrite(Request{Chunk: chunkC, Interval: k, Gated: true, Buf: recoveryBuf(h, img, 3, 4)})
	h.settle()
	m, got := mapOf(t, h.ordered[rec])
	if len(got) != 2 || got[1] != 1 || got[2] != 1 || m.holes.count() != 2 || m.data.has(0) {
		t.Fatalf("the members get %v with holes %d", got, m.holes.count())
	}
	v1, v2 := h.durableAt[w].Version, h.durableAt[rec].Version
	if v2 != v1+1 {
		t.Fatalf("recovery took %#x after %#x", v2, v1)
	}
	h.commit(chunkC, v1)
	h.commit(chunkC, v2)
	h.settle()
	for b, want := range []byte{8, 1, 1, 0} {
		if got := h.readNow(chunkC, uint32(b)*core.BlockSize, core.BlockSize); !bytes.Equal(got, fill(core.BlockSize, want)) {
			t.Fatalf("block %d reads %#x, want %#x", b, got[0], want)
		}
	}

	c2 := core.ChunkID{Volume: 1, Inode: 7, Index: 9}
	h.s.Cover(c2, k)
	p := h.id()
	h.s.Punch(&h.ctx, Request{ID: p, Chunk: c2, Off: 0, Len: core.ChunkSize, Interval: k, Gated: true}, &h.out)
	h.take()
	rec = h.recoverWrite(Request{Chunk: c2, Interval: k, Gated: true, Buf: recoveryBuf(h, img)})
	h.settle()
	if b := h.ordered[rec]; len(b) != 0 {
		t.Fatalf("after a whole-chunk punch the members get %d bytes", len(b))
	}

	c3 := core.ChunkID{Volume: 1, Inode: 7, Index: 10}
	h.s.Cover(c3, k)
	rec = h.recoverWrite(Request{Chunk: c3, Interval: k, Gated: true, MarkOnly: true, Buf: recoveryBuf(h, img)})
	h.settle()
	if _, got := mapOf(t, h.ordered[rec]); len(got) != 3 {
		t.Fatalf("MarkOnly hands the members %v", got)
	}
	logged := 0
	for _, r := range h.records {
		for _, e := range r.entries {
			if e.Chunk == c3 {
				logged++
				if e.Kind != disklog.EntryRecovered {
					t.Fatalf("MarkOnly logged %+v", e)
				}
			}
		}
	}
	if logged != 1 {
		t.Fatalf("MarkOnly logged %d entries", logged)
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

// A primary orders a punch of a chunk its own store holds nothing of, which its members may hold
// (a primary that lacks the chunk's kept version), rather than answering it done: the punch would
// otherwise be acknowledged and never reach them.
func TestAPrimaryOrdersAPunchOfAChunkItHoldsNothingOf(t *testing.T) {
	h := newRecoveryHarness(t)
	p := h.id()
	h.s.Punch(&h.ctx, Request{ID: p, Chunk: chunkC, Len: core.ChunkSize, Interval: 7, Gated: true}, &h.out)
	h.take()
	h.settle()
	if _, ok := h.ordered[p]; !ok {
		t.Fatalf("the punch was not ordered: %+v", h.replies[p])
	}
	c := core.ChunkID{Volume: 1, Inode: 7, Index: 9}
	p = h.id()
	h.s.Punch(&h.ctx, Request{ID: p, Chunk: c, Off: 100, Len: core.BlockSize, Interval: 7, Gated: true}, &h.out)
	h.take()
	h.settle()
	if got := h.ordered[p]; !bytes.Equal(got, make([]byte, 2*core.BlockSize)) {
		t.Fatalf("a punch in part of two blocks hands the members %d bytes of edges", len(got))
	}
}
