package disklog_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// segmentOfSeq is the segment whose header carries open sequence seq.
func (h *harness) segmentOfSeq(seq uint64) uint32 {
	h.t.Helper()
	used, _ := h.usedSegments()
	for _, s := range used {
		if h.segmentSeq(s) == seq {
			return s
		}
	}
	h.t.Fatalf("no segment opened at sequence %d", seq)
	return 0
}

func usageRow(h *harness, s uint32) (disklog.SegmentUsage, bool) {
	rows := disklog.SegmentUsageOf(h.log)
	i := slices.IndexFunc(rows, func(u disklog.SegmentUsage) bool { return u.Segment == s })
	if i < 0 {
		return disklog.SegmentUsage{}, false
	}
	return rows[i], true
}

// roundInFlightAtBegin fills the segments opened at sequences 1 to 4 with durable one-block
// writes, submits a write that opens a segment at sequence 5, begins a checkpoint while that
// round is in flight, then lets everything land and writes on until a segment header names
// the manifest. It returns the manifest's NextSeq and a recovery from the disk.
func roundInFlightAtBegin(t *testing.T, plant bool) (*harness, *node, uint64, *recovered) {
	h := formatted(t, 1<<16*core.BlockSize, testConfig(0))
	n := newNode(h, 0)
	if plant {
		disklog.TakeNextSeqAtBegin(h.log)
	}
	i := 0
	for ; i < 28; i++ {
		h.put(disklog.Hot, h.chunkWrite(uint32(i%16), 0, 1, uint64(i+1)))
	}
	h.submit(disklog.Hot, h.chunkWrite(uint32(i%16), 0, 1, uint64(i+1)))
	if h.log.InFlight() != 1 || disklog.NextSeqOf(h.log) != 5 || h.segmentSeq(4) != 4 {
		t.Fatalf("%d rounds in flight after durable rounds opened sequences 1 to %d", h.log.InFlight(), disklog.NextSeqOf(h.log)-1)
	}
	if err := h.log.BeginCheckpoint([]disklog.CheckpointIndex{{Owner: indexOwner}}); err != nil {
		t.Fatal(err)
	}
	if err := h.log.EndCheckpoint(&h.ctx, &h.out); err != nil {
		t.Fatal(err)
	}
	h.absorb()
	h.run()
	for i++; h.named() != h.log.Manifest(); i++ {
		h.put(disklog.Hot, h.chunkWrite(uint32(i%16), 0, 1, uint64(i+1)))
	}
	nextSeq := h.readManifestAt(h.log.Manifest()).NextSeq
	h.crashLanding()
	return h, n, nextSeq, h.recover(disklog.ModeOpen)
}

func TestNextSeqIsOneBeforeAnyDurableRoundOpensASegment(t *testing.T) {
	h := formatted(t, gib, testConfig(0))
	h.submit(disklog.Hot, h.write(0, 1, 1))
	if err := h.log.BeginCheckpoint(nil); err != nil {
		t.Fatal(err)
	}
	if err := h.log.EndCheckpoint(&h.ctx, &h.out); err != nil {
		t.Fatal(err)
	}
	h.absorb()
	h.run()
	if m := h.readManifestAt(h.log.Manifest()); m.NextSeq != 1 {
		t.Fatalf("the first manifest, begun while the first segment's round was in flight, records NextSeq %d, want 1", m.NextSeq)
	}
}

func TestNextSeqCountsOnlySegmentsDurableRoundsOpened(t *testing.T) {
	h, n, nextSeq, res := roundInFlightAtBegin(t, false)
	if nextSeq != 5 || res.err != nil || res.loaded.NextSeq != 5 {
		t.Fatalf("manifest records NextSeq %d, recovery loaded %d: %v; want 5", nextSeq, res.loaded.NextSeq, res.err)
	}
	if row, ok := usageRow(h, h.segmentOfSeq(5)); !ok || row.State != disklog.SegmentUsed || row.Live == 0 {
		t.Fatalf("the segment opened at sequence 5 is %+v after recovery, want used", row)
	}
	h.wantModel("open", res.index, n.live)
	h.readBack(res.index)
}

// Planted bug: NextSeq taken from the log's next sequence number when the checkpoint begins
// counts the segment the round in flight opened, so recovery frees it with its durable records.
func TestNextSeqTakenAtBeginLosesTheRoundInFlight(t *testing.T) {
	_, _, nextSeq, res := roundInFlightAtBegin(t, true)
	if nextSeq != 6 {
		t.Fatalf("planted NextSeq %d, want 6", nextSeq)
	}
	if !errors.Is(res.err, disklog.ErrCorrupt) {
		t.Fatalf("recovery after the planted bug: %v, want Corrupt", res.err)
	}
}

// drive writes one-block records through the node, checkpointing whenever one is due, until done.
func (h *harness) drive(n *node, done func() bool) {
	h.t.Helper()
	for i := 0; !done(); i++ {
		h.submit(disklog.Hot, h.chunkWrite(uint32(i%16), 0, 1, uint64(5000+i)))
		for {
			h.run()
			if !n.step() {
				break
			}
		}
	}
}

// freedSegment returns usedLog with segment 1, whose records all precede the replay point and
// which holds no manifest piece, freed as the cleaner would.
func freedSegment(t *testing.T, reuse bool) (*harness, *node) {
	h, n, lsns, m := usedLog(t)
	if slices.Max(lsns[1]) > m.Indexes[0].ReplayLSN || h.log.HoldsManifestPiece(1) {
		t.Fatalf("segment 1 holds LSNs %v past the replay point %d, or a manifest piece", lsns[1], m.Indexes[0].ReplayLSN)
	}
	disklog.FreeSegment(h.log, 1, reuse)
	return h, n
}

// freedAndNamed drives freedSegment until a segment header names a manifest that frees it.
func freedAndNamed(t *testing.T) (*harness, *node) {
	h, n := freedSegment(t, false)
	before := h.log.Manifest()
	h.drive(n, func() bool { return h.log.Manifest() != before && h.named() == h.log.Manifest() })
	if slices.ContainsFunc(h.readManifestAt(h.named()).Segments, func(u disklog.SegmentUsage) bool { return u.Segment == 1 }) {
		t.Fatal("the named manifest still lists segment 1")
	}
	return h, n
}

func TestRecoverFreedSegmentKeepingItsHeaderIsFree(t *testing.T) {
	h, n := freedAndNamed(t)
	used, _ := h.usedSegments()
	res := h.reopen()
	if row, ok := usageRow(h, 1); ok {
		t.Fatalf("segment 1 is %+v after recovery; the table freed it", row)
	}
	if got, want := disklog.FreeSegments(h.log), int(h.geo.SegmentCount)-1-(len(used)-1); got != want {
		t.Fatalf("%d segments free, want %d", got, want)
	}
	if h.readsInside(res.trace, 1) {
		t.Fatal("recovery read the freed segment in full")
	}
	h.wantModel("open", res.index, n.live)
}

func TestRecoverFreeingManifestNobodyNamesKeepsSegmentUsed(t *testing.T) {
	h, n := freedSegment(t, false)
	before := h.log.Manifest()
	h.drive(n, func() bool { return h.log.Manifest() != before })
	if h.named() == h.log.Manifest() {
		t.Fatal("a segment header already names the freeing manifest")
	}
	h.reopen()
	if row, ok := usageRow(h, 1); !ok || row.State != disklog.SegmentUsed {
		t.Fatalf("segment 1 is %+v, %v after recovery; want used", row, ok)
	}
}

// recordAt writes a one-block chunk write as a record at block 1 of segment s.
func recordAt(h *harness, s uint32, seq, lsn uint64) {
	h.t.Helper()
	e := disklog.WriteEntry(1, chunkOf(99), 1, 0, 1)
	img, err := disklog.AppendRecord(nil, disklog.Record{Place: disklog.Place{LogID: h.logID, Seq: seq, Offset: 1}, LSN: lsn},
		[]disklog.Entry{e}, blockData(chunkOf(99), 1, 0))
	if err != nil {
		h.t.Fatal(err)
	}
	h.disk.Put(h.geo.LBA(s, 1), img)
}

func maxSeq(h *harness) uint64 {
	used, _ := h.usedSegments()
	var s uint64
	for _, seg := range used {
		s = max(s, h.segmentSeq(seg))
	}
	return s
}

// A round in flight opened segment 1 again: its record at block 1 landed, its header did not.
func TestRecoverRecordUnderAStaleHeaderIsErased(t *testing.T) {
	for _, freed := range []bool{true, false} {
		var h *harness
		if freed {
			h, _ = freedAndNamed(t)
		} else {
			h, _, _, _ = usedLog(t)
		}
		stray := maxSeq(h) + 1
		durable := h.log.DurableLSN()
		recordAt(h, 1, stray, durable+1)
		res := h.reopen()
		if res.durable != durable || res.erased != 1 || !isZeroBlock(h.read(h.geo.LBA(1, 1), 1)) {
			t.Fatalf("freed %v: durable LSN %d, erased %d; want %d and the record at segment 1 block 1", freed, res.durable, res.erased, durable)
		}
		if _, listed := usageRow(h, 1); listed == freed || (freed && h.readsInside(res.trace, 1)) {
			t.Fatalf("freed %v: segment 1 used %v, read in full %v", freed, listed, h.readsInside(res.trace, 1))
		}
		before, _ := h.usedSegments()
		for i := 0; ; i++ {
			h.put(disklog.Hot, h.chunkWrite(uint32(i%16), 0, 1, uint64(3000+i)))
			after, _ := h.usedSegments()
			if i := slices.IndexFunc(after, func(s uint32) bool { return !slices.Contains(before, s) }); i >= 0 {
				if seq := h.segmentSeq(after[i]); seq <= stray {
					t.Fatalf("freed %v: segment %d opened at sequence %d, which the erased record carried", freed, after[i], seq)
				}
				break
			}
		}
	}
}

func TestRecoverReusedSegmentRowComesFromReplay(t *testing.T) {
	h, n := freedSegment(t, true)
	old := h.segmentSeq(1)
	h.drive(n, func() bool { return h.segmentSeq(1) != old })
	listed := h.readManifestAt(h.named()).Segments
	if i := slices.IndexFunc(listed, func(u disklog.SegmentUsage) bool { return u.Segment == 1 }); i < 0 || h.segmentSeq(1) < h.readManifestAt(h.named()).NextSeq {
		t.Fatalf("the named manifest's table %+v does not list segment 1, or its new header is below NextSeq", listed)
	}
	usage := disklog.SegmentUsageOf(h.log)
	res := h.reopen()
	if got := disklog.SegmentUsageOf(h.log); !slices.Equal(got, usage) {
		t.Fatalf("usage after recovery %+v, before %+v", got, usage)
	}
	if row, _ := usageRow(h, 1); row.Newest <= res.loaded.BaseLSN {
		t.Fatalf("segment 1 is %+v, its row not from the records written since it was reused", row)
	}
}

func TestRecoverIgnoresAnOldRecordAtBlock1(t *testing.T) {
	h, _ := freedAndNamed(t)
	h.disk.Put(h.geo.LBA(1, 0), make([]byte, core.BlockSize))
	res := h.reopen()
	if _, ok := usageRow(h, 1); ok || h.readsInside(res.trace, 1) || res.erased != 0 {
		t.Fatalf("segment 1 used %v, read in full %v, %d erased; want free and unread", ok, h.readsInside(res.trace, 1), res.erased)
	}

	durable := h.log.DurableLSN()
	recordAt(h, 1, maxSeq(h)+1, durable+1)
	res = h.reopen()
	if !h.readsInside(res.trace, 1) || res.erased != 1 || res.durable != durable {
		t.Fatalf("a record at NextSeq or more under a zeroed header: read in full %v, %d erased, durable %d", h.readsInside(res.trace, 1), res.erased, res.durable)
	}
}
