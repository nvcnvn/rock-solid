package chunkstore

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/lsm"
)

func pattern(seed uint64, n int) []byte {
	b := make([]byte, n)
	core.NewRand(seed).Fill(b)
	return b
}

func (h *harness) slotsInUse() []core.LBA { return h.s.Log().SlotsInUse(nil) }

func (h *harness) checkSlots() {
	h.t.Helper()
	if err := h.s.CheckSlots(); err != nil {
		h.t.Fatal(err)
	}
}

func (h *harness) wantSlots(want ...core.LBA) {
	h.t.Helper()
	if got := h.slotsInUse(); !slices.Equal(got, want) {
		h.t.Fatalf("slots in use %v, want %v", got, want)
	}
	h.checkSlots()
}

// A whole chunk goes to a chunk slot in one item: one record in the hot stream names the slot,
// the request's buffer is written to the slot as it is, and the index maps the chunk's 1,024
// blocks to the slot in one extent.
func TestWholeChunkWriteGoesToASlot(t *testing.T) {
	h := newHarnessOn(t, testConfig(), slotGeometry)
	data := pattern(1, core.ChunkSize)
	mark, acts := len(h.records), len(h.acts)
	v := h.sync(h.write(chunkC, 0, data, 0))
	if got := h.logged(mark, chunkC); !slices.Equal(got, []string{"slot 0 of 1 v0x100000001"}) {
		t.Fatalf("logged %q", got)
	}
	slot := slotGeometry.SlotLBA(1, 0)
	i := slices.IndexFunc(h.acts[acts:], func(a core.Action) bool { return a.Kind == core.ActDiskWrite && a.LBA == slot })
	if i < 0 || len(h.acts[acts+i].Vec) != 1 || h.acts[acts+i].Len != core.ChunkSize {
		t.Fatalf("no slot write of the request's one buffer: %+v", h.acts[acts:])
	}
	ext := h.s.Index().Extents(nil, testPool, chunkC, 0)
	if len(ext) != 1 || ext[0].First != 0 || ext[0].Count != core.BlocksPerChunk || ext[0].LBA != slot || ext[0].Version != v {
		t.Fatalf("extents %+v", ext)
	}
	if h.s.Index().Live(slot) != core.BlocksPerChunk {
		t.Fatalf("slot count %d", h.s.Index().Live(slot))
	}
	h.wantSlots(slot)
	if got := h.readNow(chunkC, 0, core.ChunkSize); !bytes.Equal(got, data) {
		t.Fatalf("byte %d differs", firstDiff(got, data))
	}
	if got := h.readNow(chunkC, 5*core.BlockSize+3, 9000); !bytes.Equal(got, data[5*core.BlockSize+3:][:9000]) {
		t.Fatal("a part of the chunk does not read back")
	}
}

// A partial write of a chunk in a slot goes to the log and trims the slot's extent; the slot
// stays in use while any block of it is live, and a punch of the whole chunk frees it, its
// segment's last, which is TRIMmed once a round begun after the free is durable.
func TestAPartialWriteTrimsTheSlotsExtent(t *testing.T) {
	h := newHarnessOn(t, testConfig(), slotGeometry)
	data := pattern(2, core.ChunkSize)
	h.sync(h.write(chunkC, 0, data, 0))
	slot := slotGeometry.SlotLBA(1, 0)
	h.sync(h.write(chunkC, 5*core.BlockSize, fill(core.BlockSize, 0x77), 0))
	copy(data[5*core.BlockSize:], fill(core.BlockSize, 0x77))
	ext := h.s.Index().Extents(nil, testPool, chunkC, 0)
	if len(ext) != 3 || ext[0].LBA != slot || ext[0].Count != 5 || ext[1].First != 5 || ext[1].LBA/4096 == 1 || ext[2].LBA != slot+6 {
		t.Fatalf("extents %+v", ext)
	}
	if h.s.Index().Live(slot) != core.BlocksPerChunk-1 {
		t.Fatalf("slot count %d", h.s.Index().Live(slot))
	}
	h.wantSlots(slot)
	if got := h.readNow(chunkC, 0, core.ChunkSize); !bytes.Equal(got, data) {
		t.Fatalf("byte %d differs", firstDiff(got, data))
	}
	h.sync(h.punch(chunkC, 0, 5*core.BlockSize, 0))
	h.wantSlots(slot)
	acts := len(h.acts)
	h.sync(h.punch(chunkC, 0, core.ChunkSize, 0))
	h.wantSlots()
	if h.actions(acts, core.ActDiskTrim) != 0 {
		t.Fatal("the slot segment is TRIMmed before a round begun after the free is durable")
	}
	h.sync(h.write(crashChunks[1], 0, fill(core.BlockSize, 1), 0))
	if h.actions(acts, core.ActDiskTrim) != 1 {
		t.Fatal("the slot segment freed with its last slot is not TRIMmed")
	}
}

// A slot is free for reuse only once the record that rewrote its chunk is durable: until then
// the chunk reads its old data from the old slot, which no write takes. The log holds it from
// reuse until a round begun after the free is durable too.
func TestASlotIsFreeOnceItsRewriteIsDurable(t *testing.T) {
	h := newHarnessOn(t, testConfig(), slotGeometry)
	old, data := pattern(3, core.ChunkSize), pattern(4, core.ChunkSize)
	h.sync(h.write(chunkC, 0, old, 0))
	first := slotGeometry.SlotLBA(1, 0)
	id := h.write(chunkC, 0, data, 0)
	for len(h.queue) > 0 && h.queue[0].Kind != core.ActDiskFsync {
		h.step()
	}
	h.wantSlots(first)
	if got := h.block(chunkC, 9, 0); !bytes.Equal(got, old[9*core.BlockSize:][:core.BlockSize]) {
		t.Fatal("the chunk does not read its old data while its rewrite is in flight")
	}
	h.sync(id)
	second := slotGeometry.SlotLBA(1, 1)
	h.wantSlots(second)
	if got := h.readNow(chunkC, 0, core.ChunkSize); !bytes.Equal(got, data) {
		t.Fatalf("byte %d differs", firstDiff(got, data))
	}
	h.sync(h.write(crashChunks[1], 0, data, 0))
	third := slotGeometry.SlotLBA(1, 2)
	h.wantSlots(second, third)
	h.sync(h.write(core.ChunkID{Volume: 9}, 0, data, 0))
	h.wantSlots(first, second, third)
}

// A crash before a rewrite's record is durable leaves the old chunk readable from its old slot,
// whatever of the rewrite's slot and record landed.
func TestACrashBeforeTheRewriteIsDurableKeepsTheOldChunk(t *testing.T) {
	for _, landing := range []string{"none", "slot", "record", "slot and part of the record"} {
		h := newHarnessOn(t, testConfig(), slotGeometry)
		old := pattern(5, core.ChunkSize)
		h.sync(h.write(chunkC, 0, old, 0))
		h.write(chunkC, 0, pattern(6, core.ChunkSize), 0)
		slot, record := h.queue[0], h.queue[1]
		h.step()
		h.step()
		h.disk.Crash(func(_ int, lba core.LBA) bool {
			inSlot := lba >= slot.LBA && lba < slot.LBA+core.BlocksPerChunk
			inRecord := lba >= record.LBA && uint64(lba-record.LBA) < record.Len/core.BlockSize
			switch landing {
			case "slot":
				return inSlot
			case "record":
				return inRecord
			case "slot and part of the record":
				return inSlot || lba == record.LBA
			}
			return false
		})
		h.recoverStore(disklog.ModeOpen)
		if got := h.readNow(chunkC, 0, core.ChunkSize); !bytes.Equal(got, old) {
			t.Fatalf("%s landed: byte %d differs from the old chunk", landing, firstDiff(got, old))
		}
		h.wantSlots(slotGeometry.SlotLBA(1, 0))
	}
}

// A snapshot keeps the slot of the version it froze: the chunk rewritten whole after a cut leaves
// its old slot in use, which reads at the snapshot's version.
func TestASnapshotKeepsItsSlot(t *testing.T) {
	h := newHarnessOn(t, testConfig(), slotGeometry)
	old, data := pattern(7, core.ChunkSize), pattern(8, core.ChunkSize)
	v1 := h.sync(h.write(chunkC, 0, old, 0))
	h.sync(h.write(chunkC, 0, data, 1))
	h.wantSlots(slotGeometry.SlotLBA(1, 0), slotGeometry.SlotLBA(1, 1))
	id := h.readAt(chunkC, 0, core.ChunkSize, v1)
	h.settle()
	if got := h.bytesOf(id, 0, core.ChunkSize); !bytes.Equal(got, old) {
		t.Fatal("the frozen version does not read its slot")
	}
}

// crashSlotWorkload writes chunks whole, rewrites, trims and punches them, across a cut and the
// checkpoints a small max_replay_bytes makes, settling only now and then so rounds pipeline, and
// ends with a whole chunk punched as it is written, the last records the crash may follow.
func crashSlotWorkload(h *harness) {
	c, d := crashChunks[0], crashChunks[1]
	h.write(c, 0, fill(core.ChunkSize, 0xb1), 0)
	h.write(d, 0, fill(core.ChunkSize, 0xb2), 0)
	h.step()
	h.write(c, 5*core.BlockSize, fill(core.BlockSize, 0xb3), 0)
	h.settle()
	h.write(c, 0, fill(core.ChunkSize, 0xb4), 0)
	h.punch(d, 0, core.ChunkSize, 0)
	h.step()
	h.step()
	h.write(d, 0, fill(core.ChunkSize, 0xb5), 1)
	h.settle()
	h.write(c, 0, fill(core.ChunkSize, 0xb6), 2)
	h.punch(d, 0, core.ChunkSize, 2)
	h.settle()
	h.write(d, 0, fill(core.ChunkSize, 0xb7), 2)
	h.punch(d, 0, core.ChunkSize, 2)
	h.settle()
}

// [property] The crash property over chunks written to slots, at commit_depth 1, 2 and 4: every
// recovery holds every acknowledged chunk, and the log's slots in use are the ones the index
// refers to.
func TestCrashAtEveryActionWithSlots(t *testing.T) {
	for _, depth := range []int{1, 2, 4} {
		t.Run(fmt.Sprint("depth ", depth), func(t *testing.T) {
			cfg := crashConfig()
			cfg.Log.MaxReplayBytes = 4 * core.BlockSize
			cfg.Log.CommitDepth = depth
			geo := slotGeometry
			crashEveryAction(t, cfg, &geo, crashSlotWorkload, true)
		})
	}
}

// Whole chunks deleted at random cost no cleaning: their slots are reused in place, so the disk
// writes little beyond the chunks, near the cleaner streams spike's 1.004.
func TestWholeChunksDeletedAtRandomCostNoCleaning(t *testing.T) {
	cfg := testConfig()
	cfg.Log.MaxReplayBytes = 1 << 20
	geo := disklog.Geometry{SegmentBlocks: 4096, SegmentCount: 24}
	h := newHarnessOn(t, cfg, geo)
	const live, rounds = 76, 200
	r := core.NewRand(9)
	data := pattern(9, core.ChunkSize)
	chunk := func(i uint32) core.ChunkID { return core.ChunkID{Volume: 2, Inode: 1, Index: i} }
	var ids []uint32
	for i := range uint32(live) {
		h.sync(h.write(chunk(i), 0, data, 0))
		ids = append(ids, i)
	}
	freeBefore := h.s.Log().FreeSegments()
	acts := len(h.acts)
	next := uint32(live)
	for range rounds {
		k := r.Below(uint64(len(ids)))
		h.sync(h.punch(chunk(ids[k]), 0, core.ChunkSize, 0))
		h.sync(h.write(chunk(next), 0, data, 0))
		ids[k] = next
		next++
	}
	var written uint64
	for _, a := range h.acts[acts:] {
		if a.Kind == core.ActDiskWrite {
			written += a.Len
		}
	}
	wa := float64(written) / float64(rounds*core.ChunkSize)
	t.Logf("%d whole chunks live in %d slots (%.0f%% of the disk), %d deleted at random and as many written: write amplification %.4f", live, (geo.SegmentCount-1)*uint32(geo.SlotsPerSegment()), 100*float64(live)/float64(geo.SegmentCount-1)/4, rounds, wa)
	if wa > 1.01 {
		t.Fatalf("write amplification %.4f", wa)
	}
	if free := h.s.Log().FreeSegments(); free+2 < freeBefore {
		t.Fatalf("free segments fell from %d to %d", freeBefore, free)
	}
	h.checkSlots()
}

const slotSegmentBlocks = 4096

// slotIndex is an index of a disk of 16 MiB segments, 4 chunk slots each.
func slotIndex(t testing.TB) *Index {
	t.Helper()
	tree, err := lsm.New(TreeConfig(lsm.Config{MaxPiece: 1 << 20, Check: true}))
	if err != nil {
		t.Fatal(err)
	}
	return NewIndex(tree, slotSegmentBlocks)
}

// The index counts the blocks its maps refer to in each chunk slot, beside its record's, and
// hands over a slot whose count falls to zero once, unless a later write refers to it again.
func TestTheIndexCountsChunkSlots(t *testing.T) {
	x := slotIndex(t)
	slot := core.LBA(2*slotSegmentBlocks + 1024)
	apply(t, x, record(100, disklog.SlotEntry(1, chunk(1), 1, 2, 1)))
	if e, ok := x.Lookup(1, chunk(1), 9); !ok || e.LBA != slot || e.BlockLBA(9) != slot+9 || e.Header != 100 {
		t.Fatalf("block 9: %+v %v", e, ok)
	}
	apply(t, x, record(200, disklog.Entry{Kind: disklog.EntryWrite, Pool: 1, Chunk: chunk(1), Version: 2, First: 9, Count: 1}))
	apply(t, x, record(300, disklog.FreezeEntry(1, chunk(1), 2, 5)))
	if x.Live(slot) != 2*(core.BlocksPerChunk-1) || x.Live(100) != 2*(core.BlocksPerChunk-1) {
		t.Fatalf("slot count %d, record count %d", x.Live(slot), x.Live(100))
	}
	apply(t, x, record(400, disklog.PunchEntry(1, chunk(1), 3, 0, core.BlocksPerChunk)))
	if got := x.TakeFreed(nil); len(got) != 0 || x.Live(slot) != core.BlocksPerChunk-1 {
		t.Fatalf("freed %v with the frozen version referring to %d blocks", got, x.Live(slot))
	}
	x.Release(1, chunk(1), 2)
	if got := x.TakeFreed(nil); !slices.Equal(got, []core.LBA{slot}) || x.Live(slot) != 0 {
		t.Fatalf("freed %v, count %d", got, x.Live(slot))
	}
	if got := x.TakeFreed(nil); len(got) != 0 {
		t.Fatalf("freed again: %v", got)
	}
	apply(t, x, record(500, disklog.SlotEntry(1, chunk(2), 1, 2, 1)))
	apply(t, x, record(600, disklog.SlotEntry(1, chunk(2), 2, 2, 1)))
	if got := x.TakeFreed(nil); len(got) != 0 || x.Live(slot) != core.BlocksPerChunk {
		t.Fatalf("a slot written again over itself: freed %v, count %d", got, x.Live(slot))
	}
	if err := x.Check(); err != nil {
		t.Fatal(err)
	}
	if err := newIndex(t).Apply(record(100, disklog.SlotEntry(1, chunk(1), 1, 2, 1))); !errors.Is(err, ErrNoSlots) {
		t.Fatalf("a slot write on a disk without slots: %v", err)
	}
}

// slotReuseWorkload frees a slot whose write no header names durable yet and writes a whole chunk
// at once, with no checkpoint, whose records would name the write durable (raised in the second
// review of Chunk slots).
func slotReuseWorkload(h *harness) {
	c, d := crashChunks[0], crashChunks[1]
	h.write(d, 0, fill(core.BlockSize, 0xa0), 0)
	h.write(c, 0, fill(core.ChunkSize, 0xb1), 0)
	h.punch(c, 0, core.ChunkSize, 0)
	h.settle()
	h.write(d, 0, fill(core.ChunkSize, 0xb2), 0)
	h.settle()
}

// [property] The crash property over a slot freed and a whole chunk written at once, with no
// checkpoint, at commit_depth 2 and 4, where the log's hold on the freed slot is what keeps the
// freed slot's write recoverable.
func TestCrashAtEveryActionReusingASlot(t *testing.T) {
	for _, depth := range []int{2, 4} {
		t.Run(fmt.Sprint("depth ", depth), func(t *testing.T) {
			cfg := testConfig()
			cfg.Log.CommitDepth = depth
			geo := slotGeometry
			crashEveryAction(t, cfg, &geo, slotReuseWorkload, false)
		})
	}
}
