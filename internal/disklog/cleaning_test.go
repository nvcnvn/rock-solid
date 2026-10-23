package disklog_test

import (
	"errors"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// The net-gain guard rounds header blocks up per record, charges one more record's, and the tail
// the largest copy can leave: in 16,384-block (64 MiB) segments a copy record of one-block entries
// holds 253 data blocks under 3 header blocks and a copy holds up to 254 blocks under 1, so 15,936
// live blocks cost 16,382, which with the header of the segment the copies open fits one, while
// 15,937 cost 16,383.
func TestCopyCostRoundsHeadersUpPerRecord(t *testing.T) {
	h := blankHarness(t, 256<<30, 8<<20, testConfig(0))
	if h.geo.SegmentBlocks != 16384 {
		t.Fatalf("%d-block segments", h.geo.SegmentBlocks)
	}
	log, err := disklog.Format(&h.ctx, h.cfg, disklog.Superblock{LogID: testLogID, Geometry: h.geo}, firstIO, &h.out)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct{ live, cost uint64 }{{0, 0}, {1, 8}, {253, 512}, {254, 517}, {15936, 16382}, {15937, 16383}} {
		if got := log.CopyCost(c.live); got != c.cost {
			t.Fatalf("%d live blocks cost %d, want %d", c.live, got, c.cost)
		}
	}
}

// A rewrite costs the records holding its pieces as they lie, header blocks included and each
// record once, and the tail the largest leaves behind in a segment too short for it: without
// that, a run filling a segment would fill a new one but for a block no record fits in, and its
// rewrite would gain nothing.
func TestRewriteCostChargesRecordsAndTheTail(t *testing.T) {
	h := formatted(t, gib, testConfig(0))
	h.put(disklog.Hot, h.write(0, 1, 1))
	big := h.checkpointPieces(nil, 1, 3*core.BlockSize).runs
	if got := h.log.RewriteCost(big, 0, 0); got != 4+3 {
		t.Fatalf("a 3-block piece in a 4-block record costs %d, want 7", got)
	}
	shared := h.checkpointPieces(nil, 3, 100).runs[1:]
	if shared[0].LSN != shared[1].LSN {
		t.Fatalf("the two pieces went out in records %d and %d, want one", shared[0].LSN, shared[1].LSN)
	}
	if got := h.log.RewriteCost(shared, 0, 0); got != 2+1 {
		t.Fatalf("two pieces sharing a 2-block record cost %d, want 3", got)
	}
	if got := h.log.RewriteCost(append(big, shared...), 0, 0); got != 4+2+3 {
		t.Fatalf("both records cost %d, want 9", got)
	}
	h.close()
}

// Only a used segment that no lane has open and whose records all precede the replay points of
// the oldest manifest recovery may load can be cleaned; before any manifest is named, none can.
// Free takes one with no live block and no piece, and makes it reusable at once.
func TestWhatTheCleanerMayFree(t *testing.T) {
	h, _, lsns, m := usedLog(t)
	floor := m.Indexes[0].ReplayLSN
	open := map[disklog.Stream]uint32{}
	newest := map[disklog.Stream]uint64{}
	for s, l := range lsns {
		st := h.segmentHeader(s).Stream
		if top := l[len(l)-1]; top > newest[st] {
			newest[st], open[st] = top, s
		}
	}
	var past uint32
	for s, l := range lsns {
		isOpen := open[h.segmentHeader(s).Stream] == s
		want := !isOpen && l[len(l)-1] <= floor
		if got := h.log.Cleanable(s); got != want {
			t.Fatalf("segment %d (LSNs %v, open %v, replay floor %d): cleanable %v, want %v", s, l, isOpen, floor, got, want)
		}
		if !isOpen && l[len(l)-1] > floor {
			past = s
		}
	}
	if err := h.log.Free(&h.ctx, open[disklog.Hot], &h.out); !errors.Is(err, disklog.ErrNotCleanable) {
		t.Fatalf("freeing the open segment: %v", err)
	}
	if err := h.log.Free(&h.ctx, 1, &h.out); !errors.Is(err, disklog.ErrNotCleanable) {
		t.Fatalf("freeing segment 1 with live blocks: %v", err)
	}
	u, _ := h.log.Usage(1)
	if err := h.log.AddLive(1, -int(u.Live)); err != nil {
		t.Fatal(err)
	}
	free := disklog.FreeSegments(h.log)
	if err := h.log.Free(&h.ctx, 1, &h.out); err != nil || disklog.FreeSegments(h.log) != free+1 {
		t.Fatalf("freeing an empty cleanable segment: %v", err)
	}
	if err := h.log.Quarantine(1); !errors.Is(err, disklog.ErrMalformed) {
		t.Fatalf("quarantining a free segment: %v", err)
	}
	if past != 0 {
		if err := h.log.Free(&h.ctx, past, &h.out); !errors.Is(err, disklog.ErrNotCleanable) {
			t.Fatalf("freeing segment %d past the replay point: %v", past, err)
		}
	}
	q := uint32(2)
	if err := h.log.Quarantine(q); err != nil || h.log.Cleanable(q) {
		t.Fatalf("quarantined segment %d: %v, cleanable %v", q, err, h.log.Cleanable(q))
	}
	if u, _ := h.log.Usage(q); u.State != disklog.SegmentQuarantined {
		t.Fatalf("segment %d is %+v", q, u)
	}

	fresh := formatted(t, gib, testConfig(0))
	fresh.put(disklog.Hot, fresh.write(0, 1, 1))
	fresh.put(disklog.Hot, fresh.write(1, 1, 2))
	if fresh.log.Cleanable(1) {
		t.Fatal("a segment is cleanable before any manifest is named")
	}
}

// copyTrial hands the log one victim's copies of counts blocks each and returns the blocks they
// took from the cleaner's open segment at (segment, block) on: the tail they left behind there when
// they rolled, the header of the segment they opened and their records, and where the lane's open
// segment now ends. With a segment open, it fails unless BackgroundBlocks grew by as many.
func (h *harness) copyTrial(counts []uint16, source core.LBA, segment, block uint32) (taken uint64, endSeg, endBlock uint32) {
	h.t.Helper()
	before := h.log.BackgroundBlocks()
	defer func() {
		if got := h.log.BackgroundBlocks() - before; segment != 0 && got != taken {
			h.t.Fatalf("copies of %v took %d blocks and counted %d background blocks", counts, taken, got)
		}
	}()
	items := make([]disklog.Item, len(counts))
	for i, c := range counts {
		items[i] = h.copyItem(uint32(i%200), c, source, uint64(i))
	}
	tks, err := h.log.Copy(&h.ctx, items, nil, &h.out)
	if err != nil {
		h.t.Fatal(err)
	}
	for i, tk := range tks {
		h.tickets.track(tk, items[i].Entry)
	}
	h.absorb()
	h.run()
	seg := h.geo.SegmentBlocks
	endSeg, endBlock = segment, block
	rolled := false
	for _, tk := range tks {
		ev := h.durableOf(tk)
		d := h.record(ev)
		end := uint32(uint64(ev.LBA)%uint64(seg)) + d.Layout.HeaderBlocks + d.Layout.DataBlocks
		switch {
		case ev.Segment != segment && rolled && ev.Segment != endSeg:
			h.t.Fatalf("one victim's copies opened segments %d and %d", endSeg, ev.Segment)
		case ev.Segment != segment:
			if !rolled {
				endBlock = 0
			}
			rolled, endSeg, endBlock = true, ev.Segment, max(endBlock, end)
		case !rolled:
			endBlock = max(endBlock, end)
		}
	}
	if rolled {
		return uint64(seg-block) + uint64(endBlock), endSeg, endBlock
	}
	return uint64(endBlock - block), endSeg, endBlock
}

// CopyCost bounds what a victim's copies take, with the header of the segment they open, below a
// segment: a record never spans two segments, so copies rolling into a new segment leave behind a
// tail up to a block short of the largest copy and its header, and their records split in two.
// The worst case is a tail just too short for a copy of the largest size; random copies after
// random fills hold too.
func TestCopyCostBoundsWhatAVictimsCopiesTake(t *testing.T) {
	h := formatted(t, gib, testConfig(16))
	seg := uint64(h.geo.SegmentBlocks)
	maxc := h.log.MaxCopyBlocks()
	tw := h.submit(disklog.Hot, h.write(0, 1, 1))
	h.run()
	src := h.durableOf(tw).LBA + 1
	_, s, b := h.copyTrial([]uint16{1}, src, 0, 0)
	for seg-uint64(b) > uint64(maxc) {
		_, s, b = h.copyTrial([]uint16{1}, src, s, b)
	}
	admit := func(counts []uint16, n uint16) bool {
		var live uint64
		for _, c := range counts {
			live += uint64(c)
		}
		return h.log.CopyCost(live+uint64(n))+1 < seg
	}
	check := func(counts []uint16) {
		t.Helper()
		var live uint64
		for _, c := range counts {
			live += uint64(c)
		}
		var taken uint64
		taken, s, b = h.copyTrial(counts, src, s, b)
		if cost := h.log.CopyCost(live); taken > cost+1 {
			t.Fatalf("copies of %v (%d live blocks) took %d blocks, charged %d and a segment header", counts, live, taken, cost)
		}
	}
	var counts []uint16
	for admit(counts, maxc) {
		counts = append(counts, maxc)
	}
	if len(counts) == 0 {
		t.Fatalf("no copy of %d blocks fits a %d-block segment", maxc, seg)
	}
	check(counts)
	r := core.NewRand(3)
	for range 300 {
		for range r.Below(seg / 2) {
			_, s, b = h.copyTrial([]uint16{1}, src, s, b)
		}
		counts = counts[:0]
		for {
			n := uint16(1 + r.Below(uint64(maxc)))
			if !admit(counts, n) {
				break
			}
			counts = append(counts, n)
		}
		check(counts)
	}
	h.close()
}
