package chunkstore

import (
	"bytes"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// Regression (review of Chunk slots): at commit_depth 2 a whole chunk written then punched, both
// acknowledged, freed its slot once the punch was durable, before any header named the write
// durable; the slot's TRIM then made recovery take the write for a round in flight, reporting
// the log corrupt or, pipelined, dropping acknowledged records.
func TestASlotFreedAtDepth2KeepsItsWriteRecoverable(t *testing.T) {
	for _, pipelined := range []bool{false, true} {
		cfg := testConfig()
		cfg.Log.CommitDepth = 2
		h := newHarnessOn(t, cfg, slotGeometry)
		c, d := crashChunks[0], crashChunks[1]
		h.sync(h.write(d, 0, fill(core.BlockSize, 0xa0), 0))
		w := h.write(c, 0, fill(core.ChunkSize, 0xb1), 0)
		if !pipelined {
			h.sync(w)
		}
		p := h.punch(c, 0, core.ChunkSize, 0)
		h.sync(w)
		h.ok(p)
		acked := h.s.Log().DurableLSN()
		h.disk.Crash(func(int, core.LBA) bool { return true })
		h.recoverStore(disklog.ModeOpen)
		if got := h.s.Log().DurableLSN(); got != acked {
			t.Fatalf("pipelined %v: acknowledged through LSN %d, recovered to %d", pipelined, acked, got)
		}
		h.checkSlots()
	}
}

// Regression (review of Chunk slots): a read in flight from a slot does not let the slot go: a
// rewrite of its chunk made durable meanwhile, and whole chunks written after it, leave the slot
// unwritten until the read completes, which then reads the old chunk, not a checksum error.
func TestAReadInFlightKeepsItsSlot(t *testing.T) {
	h := newHarnessOn(t, testConfig(), slotGeometry)
	old := pattern(11, core.ChunkSize)
	h.sync(h.write(chunkC, 0, old, 0))
	id := h.read(chunkC, 0, 4*core.BlockSize)
	reads := slices.Clone(h.queue)
	h.queue = nil
	h.sync(h.write(chunkC, 0, pattern(12, core.ChunkSize), 0))
	for i := range uint32(6) {
		h.sync(h.write(core.ChunkID{Volume: 9, Index: i}, 0, pattern(13, core.ChunkSize), 0))
	}
	h.queue = append(h.queue, reads...)
	h.settle()
	if got := h.bytesOf(id, 0, 4*core.BlockSize); !bytes.Equal(got, old[:4*core.BlockSize]) {
		t.Fatalf("byte %d differs from the old chunk", firstDiff(got, old))
	}
	h.sync(h.write(core.ChunkID{Volume: 9, Index: 9}, 0, pattern(14, core.ChunkSize), 0))
	h.sync(h.write(core.ChunkID{Volume: 9, Index: 10}, 0, pattern(14, core.ChunkSize), 0))
	if !slices.Contains(h.slotsInUse(), slotGeometry.SlotLBA(1, 0)) {
		t.Fatalf("the slot is not reused once the read completed: %v", h.slotsInUse())
	}
	h.checkSlots()
}
