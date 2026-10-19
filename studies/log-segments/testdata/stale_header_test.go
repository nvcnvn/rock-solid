package disklog_test

// Crash orders around a segment the cleaner frees, staged on the disk of usedLog. No built code
// frees a segment that holds this log's header, so none of these states can arise today; each
// test writes what a freeing checkpoint and a reopened segment would leave, and asserts what
// recovery must do once the cleaner exists. run.sh runs them against a copy of internal/disklog.

import (
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// rewriteNamedManifest rewrites the newest named manifest in place, its blob's length and the
// record's checksums included.
func rewriteNamedManifest(h *harness, edit func(*disklog.Manifest)) {
	h.t.Helper()
	pos := h.named()
	ev := disklog.Event{LSN: pos.LSN, Segment: pos.Segment, Block: pos.Block, LBA: h.geo.LBA(pos.Segment, pos.Block)}
	d := h.record(ev)
	i := slices.IndexFunc(d.Entries, func(e disklog.Entry) bool { return e.BlobKind == disklog.BlobManifest })
	earlier, rest, err := disklog.DecodeManifestPiece(d.Payload(i), nil)
	if err != nil {
		h.t.Fatal(err)
	}
	m, err := disklog.DecodeManifest(rest)
	if err != nil {
		h.t.Fatal(err)
	}
	edit(&m)
	piece, err := disklog.AppendManifestPiece(nil, earlier, disklog.AppendManifest(nil, &m))
	if err != nil {
		h.t.Fatal(err)
	}
	entries := slices.Clone(d.Entries)
	payloads := make([][]byte, len(entries))
	for j := range entries {
		payloads[j] = d.Payload(j)
	}
	entries[i].Length, payloads[i] = uint64(len(piece)), piece
	lay, err := disklog.LayoutOf(entries)
	if err != nil {
		h.t.Fatal(err)
	}
	data := make([]byte, uint64(lay.DataBlocks)*core.BlockSize)
	for j := range entries {
		copy(data[disklog.PayloadOffset(entries, j):], payloads[j])
	}
	img, err := disklog.AppendRecord(nil, d.Record, entries, data)
	if err != nil {
		h.t.Fatal(err)
	}
	h.disk.Put(ev.LBA, img)
}

// freeInTable stands in for the checkpoint after a cleaner pass: the table no longer lists s,
// whose header stays on disk. An edit would carry a free row instead; edits are not built,
// and a whole table that leaves a segment out says the same.
func freeInTable(h *harness, s uint32) {
	h.t.Helper()
	rewriteNamedManifest(h, func(m *disklog.Manifest) {
		i := slices.IndexFunc(m.Segments, func(u disklog.SegmentUsage) bool { return u.Segment == s })
		if i < 0 {
			h.t.Fatalf("segment %d is not in the table", s)
		}
		m.Segments = slices.Delete(m.Segments, i, i+1)
	})
}

func maxSeq(h *harness) uint64 {
	used, _ := h.usedSegments()
	var s uint64
	for _, seg := range used {
		s = max(s, h.segmentSeq(seg))
	}
	return s
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

func usageRow(h *harness, s uint32) (disklog.SegmentUsage, bool) {
	rows := disklog.SegmentUsageOf(h.log)
	i := slices.IndexFunc(rows, func(u disklog.SegmentUsage) bool { return u.Segment == s })
	if i < 0 {
		return disklog.SegmentUsage{}, false
	}
	return rows[i], true
}

// freedSegment returns usedLog with segment 1, whose records all precede the replay point,
// listed in the newest manifest's table.
func freedSegment(t *testing.T) (*harness, uint32) {
	h, _, lsns, m := usedLog(t)
	const s = 1
	if slices.Max(lsns[s]) > m.Indexes[0].ReplayLSN {
		t.Fatalf("segment %d holds LSNs %v past the replay point %d", s, lsns[s], m.Indexes[0].ReplayLSN)
	}
	return h, s
}

// Freed, a segment header naming the freeing manifest durable, not reused, TRIM not run (or a
// disk that is a file): the header of this log names its own segment, and the table leaves it
// out.
func TestStudyFreedSegmentKeepingItsHeaderIsFree(t *testing.T) {
	h, s := freedSegment(t)
	freeInTable(h, s)
	used, _ := h.usedSegments()
	h.reopen()
	if row, ok := usageRow(h, s); ok {
		t.Errorf("segment %d is %+v after recovery; the table freed it", s, row)
	}
	if got, want := disklog.FreeSegments(h.log), int(h.geo.SegmentCount)-1-(len(used)-1); got != want {
		t.Errorf("%d segments free, want %d", got, want)
	}
}

// Freed, the freeing manifest possibly durable, but no segment header names it yet: recovery
// loads the older manifest, which lists the segment, so it stays used and the cleaner frees it
// again. On disk this is usedLog unchanged.
func TestStudyFreeingManifestNobodyNamesKeepsSegmentUsed(t *testing.T) {
	h, s := freedSegment(t)
	h.reopen()
	if row, ok := usageRow(h, s); !ok || row.State != disklog.SegmentUsed {
		t.Errorf("segment %d is %+v, %v after recovery; want used", s, row, ok)
	}
}

// Freed, then opened again by a round still in flight at the crash: its record at block 1
// landed, its segment header did not, so the stale header stays. A segment header may name the
// freeing manifest (the table leaves the segment out) or not yet (the table recovery loads still
// lists it).
func TestStudyRecordUnderAStaleHeaderIsErased(t *testing.T) {
	t.Run("freed in the table", func(t *testing.T) { strayUnderStaleHeader(t, true) })
	t.Run("still listed", func(t *testing.T) { strayUnderStaleHeader(t, false) })
}

func strayUnderStaleHeader(t *testing.T, freed bool) {
	h, s := freedSegment(t)
	if freed {
		freeInTable(h, s)
	}
	stray := maxSeq(h) + 1
	durable := h.log.DurableLSN()
	recordAt(h, s, stray, durable+1)
	res := h.reopen()
	if res.durable != durable {
		t.Fatalf("durable LSN %d, want %d", res.durable, durable)
	}
	if res.erased != 1 {
		t.Errorf("erased %d record headers, want the one at segment %d block 1", res.erased, s)
	}
	for i := 0; ; i++ {
		before, _ := h.usedSegments()
		h.put(disklog.Hot, h.chunkWrite(uint32(i%16), 0, 1, uint64(3000+i)))
		after, _ := h.usedSegments()
		if len(after) > len(before) {
			for _, seg := range after {
				if !slices.Contains(before, seg) && h.segmentSeq(seg) <= stray {
					t.Errorf("segment %d opened with seq %d, which the record left at segment %d block 1 carries", seg, h.segmentSeq(seg), s)
				}
			}
			return
		}
	}
}

// Freed, then opened again and written before any segment header names the freeing manifest:
// recovery loads a table whose row describes the segment's previous contents.
func TestStudyReusedSegmentRowComesFromReplay(t *testing.T) {
	h, s := freedSegment(t)
	old, _ := usageRow(h, s)
	seq := maxSeq(h) + 1
	durable := h.log.DurableLSN()
	h.disk.Put(h.geo.LBA(s, 0), make([]byte, h.geo.SegmentBytes()))
	hdr, err := disklog.AppendSegmentHeader(nil, disklog.SegmentHeader{LogID: h.logID, Seq: seq, Index: s, Stream: disklog.Hot, Manifest: h.named()})
	if err != nil {
		t.Fatal(err)
	}
	h.disk.Put(h.geo.LBA(s, 0), hdr)
	recordAt(h, s, seq, durable+1)
	res := h.reopen()
	if res.durable != durable+1 {
		t.Fatalf("durable LSN %d, want %d", res.durable, durable+1)
	}
	row, _ := usageRow(h, s)
	if row.Live != 1 || row.Newest != durable+1 {
		t.Errorf("segment %d is %+v (the table had %+v); want 1 live block, newest LSN %d", s, row, old, durable+1)
	}
}
