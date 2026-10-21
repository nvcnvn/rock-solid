package chunkstore

import (
	"errors"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// A write that merges into a block of a failed write still pending behind one in flight takes
// its base from the versions that are left, never from the failed write's freed buffers, and the
// live index stays what a rebuild from the log gives.
func TestMergeSkipsFailedPendingWrite(t *testing.T) {
	cfg := testConfig()
	cfg.Log.NoReserve, cfg.Log.Reserve = false, 1000
	h := smallDisk(t, cfg)
	for i := range 40 {
		n := core.BlockSize
		if i%4 == 2 {
			n = 300 * core.BlockSize
		}
		h.write(chunkC, uint32((i/2)%4)*core.BlockSize, fill(n, byte(i+1)), 0)
	}
	var block uint16
	found := false
	for !found && h.step() {
		ci, ok := h.s.chunkOf[chunkC]
		if !ok {
			continue
		}
		cs := &h.s.chunks[ci]
		if len(cs.pending) < 2 || h.s.ops[cs.pending[0]].left == 0 {
			continue
		}
		for _, oi := range cs.pending[1:] {
			if o := &h.s.ops[oi]; o.err != nil && len(o.items) > 0 && o.items[0].state == itemFailed {
				block, found = o.items[len(o.items)-1].entry.First, true
			}
		}
	}
	if !found {
		t.Fatal("no failed write pending behind one in flight")
	}
	c := h.write(chunkC, uint32(block)*core.BlockSize+5, []byte{0xcc}, 0)
	h.settle()
	if err := h.reply(c).Err; err != nil && !errors.Is(err, disklog.ErrNoSpace) {
		t.Fatal(err)
	}
	if err := h.s.Index().Check(); err != nil {
		t.Fatal(err)
	}
	live := dumpIndex(h.s.Index())
	h.recoverStore(disklog.ModeRebuild)
	if got := dumpIndex(h.s.Index()); !slices.Equal(got, live) {
		t.Fatal("the rebuilt index differs from the live one")
	}
}

// Under steady pipelined load the store's list of items handed to the log stays short.
func TestSentItemsStayBounded(t *testing.T) {
	h := newHarness(t, testConfig())
	most := 0
	for i := range 5000 {
		h.write(core.ChunkID{Index: uint32(i % 50)}, 0, fill(core.BlockSize, byte(i)), 0)
		for range 1 + i%2 {
			h.step()
		}
		most = max(most, len(h.s.sent))
		clear(h.replies)
	}
	if most > 200 {
		t.Fatalf("the sent list grew to %d items", most)
	}
}

// A write that fails with some of its blocks durable stays applied, as recovery would replay it,
// and its reply carries its version with the error, so the caller can compare-and-set against it.
func TestFailedWriteReportsItsVersion(t *testing.T) {
	cfg := testConfig()
	cfg.Log.NoReserve, cfg.Log.Reserve = false, 1000
	h := smallDisk(t, cfg)
	for i := 1; ; i++ {
		id := h.write(chunkC, 0, fill(300*core.BlockSize, byte(i)), 0)
		h.settle()
		r := h.reply(id)
		if r.Err == nil {
			continue
		}
		ch, _ := h.s.Index().Chunk(testPool, chunkC)
		if r.Version != ch.Version || h.readNow(chunkC, 0, 1)[0] != byte(i) {
			t.Fatalf("failed write: version %#x, the index at %#x", r.Version, ch.Version)
		}
		id = h.writeExpect(chunkC, 0, []byte{1}, r.Version)
		h.settle()
		if err := h.reply(id).Err; errors.Is(err, ErrVersion) {
			t.Fatalf("expectVer of the failed write's version: %v", err)
		}
		return
	}
}

// A failed write whose only durable item is its freeze gives the chunk its version back, since
// no write holds it.
func TestFailedWriteWithOnlyItsFreezeDurable(t *testing.T) {
	h := newHarness(t, testConfig())
	v := h.sync(h.write(chunkC, 0, []byte{1}, 0))
	ci := h.s.chunkSlot(chunkC)
	oi := h.s.newOp(opWrite, Request{Chunk: chunkC, Epoch: 1})
	o := &h.s.ops[oi]
	o.chunk, o.prevVer, o.version, o.left = ci, v, v+1, 2
	o.items = append(o.items,
		item{entry: disklog.FreezeEntry(testPool, chunkC, v, 1), state: itemDurable},
		item{entry: disklog.WriteEntry(testPool, chunkC, v+1, 0, 1)})
	cs := &h.s.chunks[ci]
	cs.version, cs.epoch = v+1, 1
	cs.pending = append(cs.pending, oi)
	h.s.itemDone(oi)
	h.s.itemFailed(&h.ctx, oi, 1, disklog.ErrNoSpace)
	if cs.version != v || cs.epoch != 1 {
		t.Fatalf("chunk at %#x epoch %d after the failure, want %#x epoch 1", cs.version, cs.epoch, v)
	}
}

// A checkpoint that finds the pool momentarily full waits for a buffer instead of stopping the store.
func TestCheckpointWaitsForAPieceBuffer(t *testing.T) {
	cfg := testConfig()
	cfg.Log.MaxReplayBytes = 32 * core.BlockSize
	h := newHarness(t, cfg)
	var held []core.IOBuf
	for {
		b, err := h.pool.Alloc(core.BlockSize)
		if err != nil {
			break
		}
		held = append(held, b)
	}
	for range 8 {
		h.pool.Free(held[len(held)-1])
		held = held[:len(held)-1]
	}
	before := h.s.Log().Manifest()
	for i := 0; !h.s.checkpointing && h.s.Serving(); i++ {
		if i > 200 || h.s.Log().Manifest() != before {
			t.Fatalf("after %d writes: manifest %+v", i, h.s.Log().Manifest())
		}
		h.write(chunkC, uint32(i%8)*core.BlockSize, fill(core.BlockSize, byte(i)), 0)
		h.settle()
	}
	if !h.s.Serving() {
		t.Fatalf("a full pool stopped the store: %v", h.s.Err())
	}
	for _, b := range held {
		h.pool.Free(b)
	}
	for i := 0; h.s.Log().Manifest() == before; i++ {
		if i > 50 {
			t.Fatal("the checkpoint never finished")
		}
		h.sync(h.write(chunkC, 0, []byte{byte(i)}, 0))
	}
}
