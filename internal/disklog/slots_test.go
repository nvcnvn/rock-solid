package disklog_test

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// slotHarness is formatted on a disk of segments of segBlocks blocks.
func slotHarness(t testing.TB, segBlocks, segments uint32, cfg disklog.Config) *harness {
	t.Helper()
	h := blankHarness(t, gib, 64<<20, cfg)
	h.geo = disklog.Geometry{SegmentBlocks: segBlocks, SegmentCount: segments}
	h.disk = sim.NewDisk(h.geo.DiskBlocks())
	var err error
	h.log, err = disklog.Format(&h.ctx, h.cfg, disklog.Superblock{LogID: testLogID, Geometry: h.geo}, firstIO, &h.out)
	if err != nil {
		t.Fatal(err)
	}
	h.own = h.pool.Live()
	h.absorb()
	h.run()
	h.trace = nil
	return h
}

// chunkData is chunk idx written whole at version: blockData of each block.
func chunkData(idx uint32, version uint64) []byte {
	b := make([]byte, 0, core.ChunkSize)
	for k := range uint16(core.BlocksPerChunk) {
		b = append(b, blockData(chunkOf(idx), version, k)...)
	}
	return b
}

// whole is a write of chunk idx whole at version, holding data.
func (h *harness) whole(idx uint32, version uint64, data []byte) disklog.Item {
	buf := h.alloc(core.ChunkSize)
	copy(h.pool.Bytes(buf), data)
	return disklog.Item{Entry: disklog.WriteEntry(1, chunkOf(idx), version, 0, core.BlocksPerChunk), Buf: buf}
}

func (h *harness) wantSlots(want ...core.LBA) {
	h.t.Helper()
	if got := h.log.SlotsInUse(nil); !slices.Equal(got, want) {
		h.t.Fatalf("slots in use %v, want %v", got, want)
	}
}

func TestSlotsPerSegment(t *testing.T) {
	for _, c := range []struct {
		disk  uint64
		slots uint16
	}{{256 * gib, 16}, {16 * gib, 1}, {8 * gib, 0}, {gib, 0}} {
		geo, err := disklog.GeometryFor(c.disk)
		if err != nil {
			t.Fatal(err)
		}
		if got := geo.SlotsPerSegment(); got != c.slots {
			t.Fatalf("%d GiB disk of %d-block segments: %d slots, want %d", c.disk/gib, geo.SegmentBlocks, got, c.slots)
		}
	}
	h := formatted(t, gib, testConfig(0))
	if h.log.Slots() != 0 {
		t.Fatalf("%d slots in 256 KiB segments", h.log.Slots())
	}
	it := h.whole(1, 1, chunkData(1, 1))
	if _, err := h.log.Submit(&h.ctx, disklog.Hot, it, &h.out); !errors.Is(err, disklog.ErrTooLarge) {
		t.Fatalf("a whole chunk on a disk without slots: %v", err)
	}
	h.pool.Free(it.Buf)
}

// A whole chunk goes to a slot of a slot segment, which no header precedes, in a DiskWrite of its
// own beside the round's record in the hot stream; the round's one flush follows both writes,
// unlinked, and a round takes one slot write.
func TestAWholeChunkGoesToASlot(t *testing.T) {
	h := slotHarness(t, 4096, 8, testConfig(0))
	if h.log.Slots() != 4 {
		t.Fatalf("%d slots in 16 MiB segments", h.log.Slots())
	}
	h.submit(disklog.Hot, h.write(1, 1, 1))
	first, second := chunkData(2, 2), chunkData(3, 3)
	h.submit(disklog.Hot, h.whole(2, 2, first))
	h.submit(disklog.Hot, h.write(4, 1, 4))
	h.submit(disklog.Hot, h.whole(3, 3, second))
	h.run()
	h.wantTrace(
		"write 4096+3 io104", "fsync io105",
		"write 8192+1024 io108", "write 4099+3 io106", "fsync io107",
		"write 9216+1024 io111", "write 4102+2 io109", "fsync io110")
	if h.unlinked != 2 {
		t.Fatalf("%d unlinked flushes, want the two of the slot rounds", h.unlinked)
	}
	durable := h.eventsOf(disklog.Durable)
	if len(durable) != 3 {
		t.Fatalf("%d durable rounds", len(durable))
	}
	if e := durable[1].Entries; len(e) != 2 || e[0] != disklog.SlotEntry(1, chunkOf(2), 2, 2, 0) || e[1].Kind != disklog.EntryWrite {
		t.Fatalf("LSN 2 entries %+v", e)
	}
	if e := durable[2].Entries; len(e) != 1 || e[0] != disklog.SlotEntry(1, chunkOf(3), 3, 2, 1) {
		t.Fatalf("LSN 3 entries %+v", e)
	}
	d := h.record(durable[1])
	if d.Layout.SlotBlocks != core.BlocksPerChunk || d.SlotCRC(0, 7) != core.CRC32C(first[7*core.BlockSize:8*core.BlockSize]) {
		t.Fatalf("LSN 2 layout %+v", d.Layout)
	}
	if !bytes.Equal(h.read(8192, core.BlocksPerChunk), first) || !bytes.Equal(h.read(9216, core.BlocksPerChunk), second) {
		t.Fatal("slot data does not read back")
	}
	h.wantSlots(8192, 9216)
	rows := disklog.SegmentUsageOf(h.log)
	if i := slices.IndexFunc(rows, func(u disklog.SegmentUsage) bool { return u.Segment == 2 }); i < 0 || rows[i] != (disklog.SegmentUsage{Segment: 2, State: disklog.SegmentSlot, Live: 0b11, Newest: 3}) {
		t.Fatalf("usage %+v", rows)
	}
	h.wantLive(0)
}

// A slot its owner frees is held from reuse and TRIM until a round begun after the free is
// durable; the segment's last slot then frees the segment once its TRIM completes, and the next
// slot write opens it again.
func TestFreeingTheLastSlotTrimsAndFreesTheSegment(t *testing.T) {
	h := slotHarness(t, 4096, 8, testConfig(0))
	h.submit(disklog.Hot, h.write(9, 1, 9))
	h.submit(disklog.Hot, h.whole(1, 1, chunkData(1, 1)))
	h.submit(disklog.Hot, h.whole(2, 1, chunkData(2, 1)))
	h.run()
	h.wantSlots(8192, 9216)
	free := disklog.FreeSegments(h.log)
	for _, lba := range []core.LBA{8193, 4096, 10240, 0} {
		if err := h.log.FreeSlot(lba, &h.out); !errors.Is(err, disklog.ErrMalformed) {
			t.Fatalf("free LBA %d: %v", lba, err)
		}
	}
	for _, lba := range []core.LBA{8192, 9216} {
		if err := h.log.FreeSlot(lba, &h.out); err != nil {
			t.Fatal(err)
		}
	}
	if err := h.log.FreeSlot(8192, &h.out); !errors.Is(err, disklog.ErrMalformed) {
		t.Fatalf("free a free slot: %v", err)
	}
	h.absorb()
	h.wantSlots()
	h.trace = nil
	h.run()
	if len(h.trace) != 0 {
		t.Fatalf("actions before a round: %q", h.trace)
	}
	h.trace = nil
	h.submit(disklog.Hot, h.write(4, 1, 4))
	h.run()
	if !slices.Contains(h.trace, "trim 8192+4096 io114") {
		t.Fatalf("no TRIM once a round begun after the free is durable: %q", h.trace)
	}
	if disklog.FreeSegments(h.log) != free+1 {
		t.Fatalf("after the TRIM: %d free, want %d", disklog.FreeSegments(h.log), free+1)
	}
}

// Allocation fills the fullest slot segment first, its lowest free slot: with segments of 3 and
// 9 free slots, the next chunk goes to the one with 3.
func TestSlotAllocationFillsTheFullestSegmentFirst(t *testing.T) {
	h := slotHarness(t, 16384, 5, testConfig(0))
	if h.log.Slots() != 16 {
		t.Fatalf("%d slots in 64 MiB segments", h.log.Slots())
	}
	h.submit(disklog.Hot, h.write(99, 1, 99))
	data := chunkData(1, 1)
	for i := range uint32(32) {
		h.submit(disklog.Hot, h.whole(i, 1, data))
		h.run()
	}
	slot := func(s uint32, k uint16) core.LBA { return h.geo.SlotLBA(s, k) }
	for k := range uint16(16) {
		if k < 9 {
			if err := h.log.FreeSlot(slot(3, k), &h.out); err != nil {
				t.Fatal(err)
			}
		}
		if k >= 2 && k < 5 {
			if err := h.log.FreeSlot(slot(2, k), &h.out); err != nil {
				t.Fatal(err)
			}
		}
	}
	h.absorb()
	h.submit(disklog.Hot, h.write(98, 1, 98))
	h.run()
	h.submit(disklog.Hot, h.whole(40, 1, data))
	h.run()
	ev := h.eventsOf(disklog.Durable)
	if e := ev[len(ev)-1].Entries[0]; e.Segment != 2 || e.Slot != 2 {
		t.Fatalf("the next chunk went to slot %d of segment %d, want slot 2 of segment 2", e.Slot, e.Segment)
	}
}

// An ordinary item never opens a segment of the reserve for a slot: with no slot free and only
// the reserve, a whole chunk is not available for a slot, and Submit takes it as a write, too
// large for one record, which its owner then cuts into several; the hot lane's other items go on.
func TestASlotSegmentIsNeverOpenedFromTheReserve(t *testing.T) {
	cfg := testConfig(0)
	cfg.NoReserve, cfg.Reserve = false, 5
	h := slotHarness(t, 4096, 8, cfg)
	h.submit(disklog.Hot, h.write(1, 1, 1))
	data := chunkData(1, 1)
	for i := range uint32(4) {
		h.submit(disklog.Hot, h.whole(i, 1, data))
		h.run()
	}
	if disklog.FreeSegments(h.log) != 5 || h.log.SlotAvailable() {
		t.Fatalf("%d free segments, a slot available: %v", disklog.FreeSegments(h.log), h.log.SlotAvailable())
	}
	it := h.whole(9, 1, data)
	if _, err := h.log.Submit(&h.ctx, disklog.Hot, it, &h.out); !errors.Is(err, disklog.ErrTooLarge) {
		t.Fatalf("a whole chunk with no slot: %v", err)
	}
	h.pool.Free(it.Buf)
	h.submit(disklog.Hot, h.write(2, 1, 2))
	h.run()
	if len(h.eventsOf(disklog.Failed)) != 0 || h.log.Queued() != 0 {
		t.Fatalf("failed %+v, %d queued", h.eventsOf(disklog.Failed), h.log.Queued())
	}
	h.wantLive(0)
}

// slotRound writes W1 durably, then a whole chunk 2 whose round's two writes complete but are
// not flushed, and returns the record's and the slot's write.
func slotRound(t *testing.T, h *harness) (record, slot core.Action) {
	h.submit(disklog.Hot, h.write(1, 1, 1))
	h.run()
	h.submit(disklog.Hot, h.whole(2, 2, chunkData(2, 2)))
	if len(h.pending) != 2 {
		t.Fatalf("pending %+v", h.pending)
	}
	slot, record = h.pending[0].act, h.pending[1].act
	h.step(0)
	h.step(0)
	return record, slot
}

func lbasOf(a core.Action) []core.LBA {
	var lbas []core.LBA
	for b := range a.Len / core.BlockSize {
		lbas = append(lbas, a.LBA+core.LBA(b))
	}
	return lbas
}

// Recovery checks a tail record's slot data as it checks data in the log: a round in flight
// whose slot write did not land whole is dropped and its header erased, while one whose slot and
// record landed is replayed.
func TestRecoveryChecksATailRecordsSlotData(t *testing.T) {
	for _, c := range []struct {
		name    string
		landed  func(record, slot core.Action) []core.LBA
		durable uint64
	}{
		{"record and slot", func(r, s core.Action) []core.LBA { return append(lbasOf(r), lbasOf(s)...) }, 2},
		{"record alone", func(r, _ core.Action) []core.LBA { return lbasOf(r) }, 1},
		{"record and all but the slot's last block", func(r, s core.Action) []core.LBA { return append(lbasOf(r), lbasOf(s)[:core.BlocksPerChunk-1]...) }, 1},
		{"slot alone", func(_, s core.Action) []core.LBA { return lbasOf(s) }, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := slotHarness(t, 4096, 8, testConfig(0))
			record, slot := slotRound(t, h)
			h.crashLanding(c.landed(record, slot)...)
			res := h.reopen()
			if res.durable != c.durable {
				t.Fatalf("recovered to LSN %d, want %d", res.durable, c.durable)
			}
			if c.durable == 1 {
				h.wantSlots()
				if c.name != "slot alone" && res.erased != 1 {
					t.Fatalf("%d headers erased", res.erased)
				}
				return
			}
			h.wantSlots(slot.LBA)
			if !bytes.Equal(h.read(slot.LBA, core.BlocksPerChunk), chunkData(2, 2)) {
				t.Fatal("slot data does not read back")
			}
		})
	}
}

// Recovery reads no slot under a record a later header names durable: its round's flush
// covered the slot, and a damaged block of it fails its CRC when read. At commit_depth 2, LSN 3
// was built once LSN 2, a slot write, was durable, and recovery replays both whether or not LSN
// 2's slot is damaged, without reading it.
func TestRecoveryLeavesASlotUnderARecordNamedDurableToReads(t *testing.T) {
	for _, damage := range []bool{false, true} {
		cfg := testConfig(0)
		cfg.CommitDepth = 2
		h := slotHarness(t, 4096, 8, cfg)
		h.submit(disklog.Hot, h.write(1, 1, 1))
		h.run()
		h.submit(disklog.Hot, h.whole(2, 2, chunkData(2, 2)))
		h.run()
		h.submit(disklog.Hot, h.write(3, 1, 3))
		write := h.pending[0].act
		h.step(0)
		if damage {
			h.disk.FlipBit(h.geo.SlotLBA(2, 0)+500, 9, 0x10)
		}
		h.crashLanding(lbasOf(write)...)
		res := h.recover(disklog.ModeOpen)
		if res.err != nil || res.durable != 3 {
			t.Fatalf("damaged %v: LSN %d, %v", damage, res.durable, res.err)
		}
		if n := h.readsIn(res.trace, 2); n != 2 {
			t.Fatalf("damaged %v: recovery read %d blocks of the slot segment, want its first two: %q", damage, n, res.trace)
		}
	}
}

// Replay marks a slot segment opened after the manifest from the records naming its slots, never
// from its first block: here chunk data whose first block is a segment header of this log naming
// the slot's segment, as a cold segment opened after the manifest, and whose second is a record
// header of this log. Nor does that header make a log segment of one the manifest lists as
// slot. The resumed log appends no record there, and the chunk reads back.
func TestReplayMarksASlotSegmentFromItsRecordsNotItsFirstBlock(t *testing.T) {
	for _, listed := range []bool{false, true} {
		cfg := testConfig(0)
		if listed {
			cfg.MaxReplayBytes = 4 * core.BlockSize
		}
		h := slotHarness(t, 4096, 8, cfg)
		h.submit(disklog.Hot, h.write(1, 1, 1))
		h.run()
		data := chunkData(2, 2)
		fake, err := disklog.AppendSegmentHeader(nil, disklog.SegmentHeader{LogID: testLogID, Seq: 99, Index: 2, Stream: disklog.Cold})
		if err != nil {
			t.Fatal(err)
		}
		copy(data, fake)
		copy(data[core.BlockSize:], h.read(h.geo.LBA(1, 1), 1))
		h.submit(disklog.Hot, h.whole(2, 2, data))
		h.run()
		h.wantSlots(h.geo.SlotLBA(2, 0))
		if listed {
			n := newNode(h, 0)
			for n.checkpoints == 0 && n.step() {
				h.run()
			}
			h.onEvent = nil
			h.submit(disklog.Cold, h.write(5, 1, 5))
			h.run()
		}
		h.crashLanding()
		for range 2 {
			res := h.reopen()
			if listed && !slices.ContainsFunc(res.loaded.Segments, func(u disklog.SegmentUsage) bool { return u.Segment == 2 && u.State == disklog.SegmentSlot }) {
				t.Fatalf("manifest usage %+v", res.loaded.Segments)
			}
			h.wantSlots(h.geo.SlotLBA(2, 0))
			h.submit(disklog.Cold, h.write(3, 1, 3))
			h.submit(disklog.Hot, h.write(4, 1, 4))
			h.run()
			for _, ev := range h.eventsOf(disklog.Durable) {
				if ev.Segment == 2 {
					t.Fatalf("listed %v: LSN %d appended to the slot segment", listed, ev.LSN)
				}
			}
			if !bytes.Equal(h.read(h.geo.SlotLBA(2, 0), core.BlocksPerChunk), data) {
				t.Fatalf("listed %v: the chunk does not read back", listed)
			}
			h.crashLanding()
		}
	}
}

// The manifest's usage table lists a slot segment as slot, with its slots in use, and a recovery
// from it, with no record past it naming the segment, restores them; a cold segment opened after
// the checkpoint names its manifest.
func TestTheUsageTableListsSlotSegments(t *testing.T) {
	cfg := testConfig(0)
	cfg.MaxReplayBytes = 4 * core.BlockSize
	h := slotHarness(t, 4096, 8, cfg)
	n := newNode(h, 0)
	h.submit(disklog.Hot, h.write(9, 1, 9))
	h.submit(disklog.Hot, h.whole(1, 1, chunkData(1, 1)))
	h.submit(disklog.Hot, h.whole(2, 1, chunkData(2, 1)))
	h.run()
	if err := h.log.FreeSlot(h.geo.SlotLBA(2, 0), &h.out); err != nil {
		t.Fatal(err)
	}
	h.absorb()
	for n.checkpoints == 0 && n.step() {
		h.run()
	}
	h.run()
	if n.checkpoints != 1 {
		t.Fatalf("%d checkpoints", n.checkpoints)
	}
	h.onEvent = nil
	h.submit(disklog.Cold, h.write(3, 1, 3))
	h.run()
	h.crashLanding()
	res := h.reopen()
	if i := slices.IndexFunc(res.loaded.Segments, func(u disklog.SegmentUsage) bool { return u.State == disklog.SegmentSlot }); i < 0 || res.loaded.Segments[i].Segment != 2 || res.loaded.Segments[i].Live != 0b10 {
		t.Fatalf("manifest usage %+v", res.loaded.Segments)
	}
	h.wantSlots(h.geo.SlotLBA(2, 1))
}

// readsIn counts the blocks of segment seg a trace's reads cover.
func (h *harness) readsIn(trace []string, seg uint32) uint64 {
	h.t.Helper()
	lo, hi := uint64(h.geo.LBA(seg, 0)), uint64(h.geo.LBA(seg+1, 0))
	var n uint64
	for _, a := range trace {
		var lba, blocks, io uint64
		if _, err := fmt.Sscanf(a, "read %d+%d io%d", &lba, &blocks, &io); err != nil {
			continue
		}
		if end, start := min(lba+blocks, hi), max(lba, lo); end > start {
			n += end - start
		}
	}
	return n
}
