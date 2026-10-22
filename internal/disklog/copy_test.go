package disklog_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// copyItem builds a cleaner's copy of count blocks of chunk index idx read from source.
func (h *harness) copyItem(idx uint32, count uint16, source core.LBA, seed uint64) disklog.Item {
	it := h.write(idx, count, seed)
	e := it.Entry
	it.Entry = disklog.CopyEntry(e.Pool, e.Chunk, e.Version, e.First, e.Count, source)
	return it
}

func (h *harness) submitCopy(it disklog.Item) disklog.Ticket {
	h.t.Helper()
	tks, err := h.log.Copy(&h.ctx, []disklog.Item{it}, nil, &h.out)
	if err != nil {
		h.t.Fatalf("copy %+v: %v", it.Entry, err)
	}
	tk := tks[0]
	h.tickets.track(tk, it.Entry)
	h.absorb()
	return tk
}

func TestCopiesGoToTheCleanersLane(t *testing.T) {
	h := formatted(t, gib, testConfig(0))
	tw := h.submit(disklog.Hot, h.write(0, 2, 1))
	h.run()
	w := h.durableOf(tw)
	c := h.submitCopy(h.copyItem(0, 2, w.LBA+1, 1))
	cold := h.submit(disklog.Cold, h.blob(disklog.BlobRaftEntries, 5, 10, 2))
	h.run()
	ce, oe := h.durableOf(c), h.durableOf(cold)
	if ce.Segment == oe.Segment || ce.Segment == w.Segment {
		t.Fatalf("the copy went to segment %d, the cold blob to %d, the write to %d: want a segment of its own", ce.Segment, oe.Segment, w.Segment)
	}
	h.wantLane(ce.Segment, disklog.Cold, 1)
	h.wantLane(oe.Segment, disklog.Cold, 0)
	if d := h.record(ce); d.Entries[0].Kind != disklog.EntryCopy || d.Entries[0].Source != w.LBA+1 {
		t.Fatalf("copy record holds %+v", d.Entries)
	}

	again := h.submitCopy(h.copyItem(1, 1, w.LBA+1, 3))
	h.run()
	if seg := h.durableOf(again).Segment; seg != ce.Segment {
		t.Fatalf("the next copy opened segment %d instead of appending to %d", seg, ce.Segment)
	}
	h.reopen()
	resumed := h.submitCopy(h.copyItem(2, 1, w.LBA+1, 4))
	h.run()
	if seg := h.durableOf(resumed).Segment; seg != ce.Segment {
		t.Fatalf("after a restart a copy opened segment %d instead of resuming %d", seg, ce.Segment)
	}
	h.close()
}

// Copies handed over together go out in one record, as the net-gain guard charges them, rather
// than the first alone in a round of its own.
func TestCopiesHandedOverTogetherShareARecord(t *testing.T) {
	h := formatted(t, gib, testConfig(0))
	tw := h.submit(disklog.Hot, h.write(0, 3, 1))
	h.run()
	w := h.durableOf(tw)
	items := []disklog.Item{h.copyItem(0, 1, w.LBA+1, 1), h.copyItem(1, 1, w.LBA+2, 2), h.copyItem(2, 1, w.LBA+3, 3)}
	tks, err := h.log.Copy(&h.ctx, items, nil, &h.out)
	if err != nil || len(tks) != len(items) {
		t.Fatalf("%d tickets for %d copies: %v", len(tks), len(items), err)
	}
	for i, tk := range tks {
		h.tickets.track(tk, items[i].Entry)
	}
	h.absorb()
	h.run()
	for _, tk := range tks[1:] {
		if lsn := h.durableOf(tk).LSN; lsn != h.durableOf(tks[0]).LSN {
			t.Fatalf("copies went out in records %d and %d, want one", h.durableOf(tks[0]).LSN, lsn)
		}
	}
	h.close()
}

func TestCopyRefusesOtherKindsAndSourcesOutsideASegment(t *testing.T) {
	h := formatted(t, gib, testConfig(0))
	it := h.copyItem(0, 1, 5000, 1)
	if _, err := h.log.Submit(&h.ctx, disklog.Cold, it, &h.out); !errors.Is(err, disklog.ErrReservedKind) {
		t.Fatalf("submit of a copy: %v", err)
	}
	for _, e := range []disklog.Entry{
		disklog.WriteEntry(1, testChunk, 1, 0, 1),
		disklog.CopyEntry(1, testChunk, 1, 0, 1, core.LBA(h.geo.SegmentBlocks-1)),
		disklog.CopyEntry(1, testChunk, 1, 0, 1, core.LBA(h.geo.DiskBlocks())),
		disklog.CopyEntry(1, testChunk, 1, 0, 2, core.LBA(2*h.geo.SegmentBlocks-1)),
	} {
		it.Entry = e
		if _, err := h.log.Copy(&h.ctx, []disklog.Item{it}, nil, &h.out); !errors.Is(err, disklog.ErrMalformed) {
			t.Fatalf("copy of %+v: %v", e, err)
		}
	}
	h.pool.Free(it.Buf)
	h.close()
}

// The cleaner's copies go out past the hold behind a checkpoint and into the reserve, where
// ordinary items wait, so a pass finishes on a disk filled to its reserve.
func TestCopiesPassTheHoldAndIntoTheReserve(t *testing.T) {
	cfg := testConfig(0)
	cfg.MaxReplayBytes = 4 * core.BlockSize
	h := formatted(t, gib, cfg)
	h.put(disklog.Hot, h.write(0, 3, 0))
	h.put(disklog.Hot, h.write(1, 3, 1))
	held := h.submit(disklog.Hot, h.write(2, 1, 2))
	c := h.submitCopy(h.copyItem(0, 1, 5000, 3))
	h.run()
	if h.log.Queued() != 1 || slices.ContainsFunc(h.eventsOf(disklog.Durable), func(ev disklog.Event) bool { return slices.Contains(ev.Tickets, held) }) {
		t.Fatalf("the hold let the write out or kept the copy: %+v", h.events)
	}
	h.durableOf(c)
	if err := h.log.BeginCheckpoint(nil); err != nil {
		t.Fatal(err)
	}
	if err := h.log.EndCheckpoint(&h.ctx, &h.out); err != nil {
		t.Fatal(err)
	}
	h.absorb()
	h.run()
	h.durableOf(held)
	h.close()

	cfg = testConfig(0)
	cfg.NoReserve, cfg.Reserve = false, 2
	h = formatted(t, 32*core.BlockSize, cfg)
	h.victim = true
	for i := range 5 {
		h.put(disklog.Hot, h.write(uint32(i), 2, uint64(i)))
	}
	held = h.submit(disklog.Hot, h.write(5, 1, 5))
	c = h.submitCopy(h.copyItem(0, 1, 5*4+1, 6))
	h.run()
	if ev := h.durableOf(c); ev.Segment != 6 || !h.log.Stalled() {
		t.Fatalf("copy in segment %d, write stalled %v: want the reserve's segment 6 and the write held", ev.Segment, h.log.Stalled())
	}
	h.log.FailStalled(&h.ctx, &h.out)
	h.absorb()
	if failed := h.eventsOf(disklog.Failed); len(failed) != 1 || !slices.Equal(failed[0].Tickets, []disklog.Ticket{held}) {
		t.Fatalf("no victim: %+v", h.events)
	}
	h.close()
}
