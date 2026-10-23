package disklog_test

import (
	"bytes"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// Regression (review of Chunk slots): at commit_depth 4 a slot freed and written again within
// commit_depth LSNs of the record that first wrote it made recovery check that record's slot
// against the new data and report the log corrupt.
func TestASlotReusedNearTheTailKeepsItsFirstWriteRecoverable(t *testing.T) {
	cfg := testConfig(0)
	cfg.CommitDepth = 4
	h := slotHarness(t, 4096, 8, cfg)
	h.submit(disklog.Hot, h.write(9, 1, 9))
	h.submit(disklog.Hot, h.whole(1, 1, chunkData(1, 1)))
	h.run()
	h.submit(disklog.Hot, h.whole(1, 2, chunkData(1, 2)))
	h.run()
	h.wantSlots(8192, 9216)
	if err := h.log.FreeSlot(8192, &h.out); err != nil {
		t.Fatal(err)
	}
	h.absorb()
	h.submit(disklog.Hot, h.whole(3, 1, chunkData(3, 1)))
	h.run()
	h.crashLanding()
	res := h.recover(disklog.ModeOpen)
	if res.err != nil || res.durable != 4 {
		t.Fatalf("four durable rounds recovered as LSN %d, %v", res.durable, res.err)
	}
}

// A slot segment freed with its last slot and opened again for records by a round in flight, whose
// record at block 1 landed and whose segment header did not: recovery erases that record, and
// since the round could open the segment only once its slots were freed, leaves none of them in
// use, although replay finds the record that wrote its slot (raised in review of Chunk slots).
func TestARecordErasedInASlotSegmentLeavesNoSlotInUse(t *testing.T) {
	h := slotHarness(t, 4096, 8, testConfig(0))
	h.submit(disklog.Hot, h.write(1, 1, 1))
	h.submit(disklog.Hot, h.whole(2, 1, chunkData(2, 1)))
	h.run()
	h.wantSlots(h.geo.SlotLBA(2, 0))
	if err := h.log.FreeSlot(h.geo.SlotLBA(2, 0), &h.out); err != nil {
		t.Fatal(err)
	}
	h.absorb()
	h.submit(disklog.Hot, h.write(3, 1, 3))
	h.run()
	if !slices.Contains(h.trace, "trim 8192+4096 io111") {
		t.Fatalf("trace %q", h.trace)
	}
	h.submit(disklog.Cold, h.write(4, 1, 4))
	if len(h.pending) != 2 || h.pending[0].act.LBA != h.geo.LBA(2, 0) {
		t.Fatalf("pending %+v", h.pending)
	}
	h.step(0)
	h.crashLanding(h.geo.LBA(2, 1), h.geo.LBA(2, 2))
	res := h.reopen()
	if res.durable != 3 || res.erased != 1 {
		t.Fatalf("recovered to LSN %d, %d headers erased", res.durable, res.erased)
	}
	h.wantSlots()
	if slices.ContainsFunc(disklog.SegmentUsageOf(h.log), func(u disklog.SegmentUsage) bool { return u.Segment == 2 }) {
		t.Fatalf("usage %+v", disklog.SegmentUsageOf(h.log))
	}
}

// Recovery reads two blocks of a slot segment opened since the manifest, its header and block 1
// with the header reads, once a later record names the chunk durable, not the whole segment
// its chunk data at block 0 would have it read in full as damage: 64 MiB at every start for each such segment (raised in review of Chunk slots).
func TestRecoveryReadsNoSlotSegmentInFull(t *testing.T) {
	h := slotHarness(t, 16384, 5, testConfig(0))
	h.submit(disklog.Hot, h.write(1, 1, 1))
	h.submit(disklog.Hot, h.whole(2, 1, chunkData(2, 1)))
	h.run()
	h.submit(disklog.Hot, h.write(3, 1, 3))
	h.run()
	h.crashLanding()
	res := h.reopen()
	if n := h.readsIn(res.trace, 2); n != 2 {
		t.Fatalf("recovery read %d blocks of the slot segment", n)
	}
	h.wantSlots(h.geo.SlotLBA(2, 0))
}

// Regression (second review of Chunk slots): a segment used for a slot after the manifest's base,
// freed and opened again for records, whose header is then damaged, is read in full like any
// damaged segment, and its durable records past the gap are corruption, not dropped as rounds in
// flight, though replay finds the record that wrote its slot.
func TestADamagedHeaderOfASlotSegmentOpenedForRecordsIsCorruption(t *testing.T) {
	h := slotHarness(t, 4096, 8, testConfig(0))
	h.submit(disklog.Hot, h.write(1, 1, 1))
	h.submit(disklog.Hot, h.whole(2, 1, chunkData(2, 1)))
	h.run()
	if err := h.log.FreeSlot(h.geo.SlotLBA(2, 0), &h.out); err != nil {
		t.Fatal(err)
	}
	h.absorb()
	h.submit(disklog.Hot, h.write(3, 1, 3))
	h.run()
	for i := range uint32(3) {
		h.submit(disklog.Cold, h.write(4+i, 1, uint64(4+i)))
		h.run()
	}
	if got := h.segmentHeader(2); got.Stream != disklog.Cold {
		t.Fatalf("segment 2 not opened for the cold records: %+v", got)
	}
	h.disk.FlipBit(h.geo.LBA(2, 0), 20, 0x10)
	h.crashLanding()
	res := h.recover(disklog.ModeOpen)
	if res.err == nil {
		t.Fatalf("durable records 4 to 6 dropped: recovered to LSN %d", res.durable)
	}
}

// Regression (second review of Chunk slots): a slot segment freed with its last slot has its first
// block zeroed after its TRIM, which here fails and leaves the chunk data, as a disk that is a
// file or a discard that does not read back as zeros does: a segment no manifest lists is free
// to recovery when its first block reads as zeros, and read in full at every start otherwise.
func TestAFreedSlotSegmentReadsAsFreeWhateverItsTRIMLeft(t *testing.T) {
	h := slotHarness(t, 4096, 8, testConfig(0))
	h.submit(disklog.Hot, h.write(1, 1, 1))
	h.submit(disklog.Hot, h.whole(2, 1, chunkData(2, 1)))
	h.run()
	if err := h.log.FreeSlot(h.geo.SlotLBA(2, 0), &h.out); err != nil {
		t.Fatal(err)
	}
	h.absorb()
	h.failWhen = func(a core.Action) core.DiskError {
		if a.Kind == core.ActDiskTrim {
			return core.DiskIO
		}
		return core.DiskOK
	}
	h.submit(disklog.Hot, h.write(3, 1, 3))
	h.run()
	h.failWhen = nil
	if !slices.Contains(h.trace, "write 8192+1 io112") || disklog.FreeSegments(h.log) != 5 {
		t.Fatalf("no write of zeros over the freed segment's first block: %q, %d free", h.trace, disklog.FreeSegments(h.log))
	}
	h.submit(disklog.Hot, h.write(4, 1, 4))
	h.run()
	if b := h.disk.Durable(h.geo.LBA(2, 0)); b == nil || !bytes.Equal(b, make([]byte, core.BlockSize)) {
		t.Fatal("the freed slot segment's first block is not durably zero")
	}
}

// A stall on either hot lane fails both, in submit order: with the shared hot lane out of room
// and the slot lane still able to append, a freeze of chunk 7 and its whole-chunk write after it
// both fail, rather than the freeze alone, which would leave the write durable at the new epoch
// with no freeze before it.
func TestAStallFailsBothHotLanes(t *testing.T) {
	cfg := testConfig(0)
	cfg.NoReserve, cfg.Reserve = false, 4
	h := slotHarness(t, 4096, 8, cfg)
	h.submit(disklog.Hot, h.write(1, 1, 1))
	h.submit(disklog.Hot, h.whole(2, 2, chunkData(2, 2)))
	h.run()
	h.victim = true
	for n := uint32(1); !h.log.Stalled(); n++ {
		if n > 200 {
			t.Fatal("the hot lane never stalled")
		}
		h.submit(disklog.Hot, h.write(100+n, 64, uint64(10+n)))
		h.run()
	}
	fz := h.submit(disklog.Hot, disklog.Item{Entry: disklog.FreezeEntry(1, chunkOf(7), 0, 5)})
	wt := h.submit(disklog.Hot, h.whole(7, 1, chunkData(7, 1)))
	h.run()
	h.log.FailStalled(&h.ctx, &h.out)
	h.absorb()
	h.run()
	var failed []disklog.Ticket
	for _, ev := range h.eventsOf(disklog.Failed) {
		failed = append(failed, ev.Tickets...)
	}
	if !slices.Contains(failed, fz) || !slices.Contains(failed, wt) || !slices.IsSorted(failed) {
		t.Fatalf("failed %v; want the freeze %d and the slot write %d among them, in submit order", failed, fz, wt)
	}
	for _, ev := range h.eventsOf(disklog.Durable) {
		for _, e := range ev.Entries {
			if e.Chunk == chunkOf(7) {
				t.Fatalf("chunk 7's %v became durable at LSN %d after its freeze failed", e.Kind, ev.LSN)
			}
		}
	}
	h.wantSlots(h.geo.SlotLBA(2, 0))
}
