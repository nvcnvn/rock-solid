package disklog_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

const runOwner = 5

// begun is a checkpoint as the log stood when it began, and its manifest's last piece once durable.
type begun struct {
	base    uint64
	nextSeq uint64
	rows    []disklog.SegmentUsage
	runs    []disklog.BlobRef
	last    disklog.Event
}

// checkpointRuns checkpoints index runOwner, carrying runs and adding n one-byte runs, and
// runs the disk until the manifest is durable.
func (h *harness) checkpointRuns(carried []disklog.BlobRef, n int) begun {
	h.t.Helper()
	b := begun{base: h.log.DurableLSN(), nextSeq: disklog.NextSeqOf(h.log), rows: disklog.SegmentUsageOf(h.log)}
	if err := h.log.BeginCheckpoint([]disklog.CheckpointIndex{{Owner: runOwner, ReplayLSN: b.base, Runs: carried}}); err != nil {
		h.t.Fatal(err)
	}
	var tickets []disklog.Ticket
	for i := range n {
		it := h.blob(disklog.BlobRun, runOwner, 1, uint64(i))
		tk, err := h.log.CheckpointPiece(&h.ctx, runOwner, it.Buf, 1, true, &h.out)
		if err != nil {
			h.t.Fatal(err)
		}
		h.absorb()
		tickets = append(tickets, tk)
	}
	if err := h.log.EndCheckpoint(&h.ctx, &h.out); err != nil {
		h.t.Fatal(err)
	}
	h.absorb()
	h.run()
	done := h.eventsOf(disklog.CheckpointDone)
	if len(done) == 0 || done[len(done)-1].Manifest != h.log.Manifest() {
		h.t.Fatalf("checkpoint did not finish: %+v", h.eventsOf(disklog.CheckpointFailed))
	}
	b.runs, b.last = slices.Clone(carried), posEvent(h, h.log.Manifest())
	for _, ev := range h.eventsOf(disklog.Durable) {
		for i, tk := range ev.Tickets {
			if slices.Contains(tickets, tk) {
				b.runs = append(b.runs, disklog.BlobRef{Segment: ev.Segment, Block: ev.Block, LSN: ev.LSN, Entry: uint16(i)})
			}
		}
	}
	return b
}

// posEvent is the Durable event of the record holding a manifest's last piece.
func posEvent(h *harness, pos disklog.ManifestPos) disklog.Event {
	return disklog.Event{LSN: pos.LSN, Segment: pos.Segment, Block: pos.Block, LBA: h.geo.LBA(pos.Segment, pos.Block)}
}

// nameManifest writes to the hot stream until a durable segment header names the newest manifest.
func (h *harness) nameManifest() {
	h.t.Helper()
	blocks := uint16(min(40, h.cfg.MaxRecordBytes/core.BlockSize-2))
	for i := 0; h.named() != h.log.Manifest(); i++ {
		h.put(disklog.Hot, h.write(uint32(i), blocks, uint64(900+i)))
	}
}

func (h *harness) isSnapshot(b begun) bool { return len(h.readChain(b.last).bodies) == 1 }

// changedBetween is the rows of the segments whose records became durable after from and up
// to to, and of the freed ones, as rows holds them, or free.
func (h *harness) changedBetween(from, to uint64, rows []disklog.SegmentUsage, freed ...uint32) []disklog.SegmentUsage {
	segs := slices.Clone(freed)
	for _, ev := range h.eventsOf(disklog.Durable) {
		if ev.LSN > from && ev.LSN <= to {
			segs = append(segs, ev.Segment)
		}
	}
	slices.Sort(segs)
	var want []disklog.SegmentUsage
	for _, s := range slices.Compact(segs) {
		u := disklog.SegmentUsage{Segment: s}
		if i := slices.IndexFunc(rows, func(r disklog.SegmentUsage) bool { return r.Segment == s }); i >= 0 {
			u = rows[i]
		}
		want = append(want, u)
	}
	return want
}

func TestCheckpointsWriteASnapshotThenEdits(t *testing.T) {
	h := formatted(t, gib, testConfig(0))
	h.put(disklog.Hot, h.write(0, 1, 1))
	first := h.checkpointRuns(nil, 20)
	c := h.readChain(first.last)
	snap, err := disklog.DecodeManifest(c.bodies[0])
	if len(c.bodies) != 1 || err != nil {
		t.Fatalf("first checkpoint wrote %d manifests: %v", len(c.bodies), err)
	}
	want := disklog.Manifest{BaseLSN: first.base, NextSeq: 2, Indexes: []disklog.ManifestIndex{{Owner: runOwner, ReplayLSN: first.base, Runs: first.runs}}, Segments: first.rows}
	if !manifestsEqual(snap, want) {
		t.Fatalf("snapshot\n got %+v\nwant %+v", snap, want)
	}

	h.put(disklog.Hot, h.write(1, 40, 2))
	h.put(disklog.Hot, h.write(2, 40, 3))
	disklog.FreeSegment(h.log, 1, false)
	second := h.checkpointRuns(first.runs[1:], 1)
	c = h.readChain(second.last)
	if len(c.bodies) != 2 || !slices.Equal(c.bodies[0], h.readChain(first.last).bodies[0]) {
		t.Fatalf("the second manifest lists %d manifests, its first not the snapshot", len(c.bodies))
	}
	snapRef := disklog.BlobRef{Segment: first.last.Segment, Block: first.last.Block, LSN: first.last.LSN}
	if !slices.Equal(c.pieces[0], []disklog.BlobRef{snapRef}) || len(c.pieces[1]) != 0 {
		t.Fatalf("last piece lists %+v, want the snapshot's last piece %+v then nothing", c.pieces, snapRef)
	}
	edit, err := disklog.DecodeManifestEdit(c.bodies[1])
	if err != nil {
		t.Fatal(err)
	}
	wantEdit := disklog.ManifestEdit{
		BaseLSN:  second.base,
		NextSeq:  second.nextSeq,
		Indexes:  []disklog.IndexEdit{{Owner: runOwner, ReplayLSN: second.base, Added: second.runs[19:], Removed: first.runs[:1]}},
		Segments: h.changedBetween(first.base, second.base, second.rows, 1),
	}
	if !editsEqual(edit, wantEdit) || slices.IndexFunc(edit.Segments, func(u disklog.SegmentUsage) bool { return u.Segment == 1 && u.State == disklog.SegmentFree }) < 0 {
		t.Fatalf("edit\n got %+v\nwant %+v, segment 1 free", edit, wantEdit)
	}
	got := h.readManifest(second.last)
	if !manifestsEqual(got, disklog.Manifest{BaseLSN: second.base, NextSeq: wantEdit.NextSeq, Indexes: []disklog.ManifestIndex{{Owner: runOwner, ReplayLSN: second.base, Runs: second.runs}}, Segments: second.rows}) {
		t.Fatalf("snapshot plus edit\n got %+v\nwant the log's own table %+v", got, second.rows)
	}
	h.nameManifest()
	if res := h.reopen(); !manifestsEqual(res.loaded, got) {
		t.Fatalf("recovery loaded %+v, want %+v", res.loaded, got)
	}
}

// snapshotThenEdits writes a snapshot of 400 runs and two edits adding 150 each, then restarts
// if asked, and reports whether a third edit of 150 or 50 runs becomes a snapshot.
func snapshotThenEdits(t *testing.T, edits int, restart bool, third int) bool {
	h := formatted(t, gib, testConfig(0))
	b := h.checkpointRuns(nil, 400)
	h.nameManifest()
	for range edits {
		b = h.checkpointRuns(b.runs, 150)
		if h.isSnapshot(b) {
			t.Fatal("an edit of 150 runs after a snapshot of 400 became a snapshot")
		}
		h.nameManifest()
	}
	if restart {
		h.reopen()
	}
	return h.isSnapshot(h.checkpointRuns(b.runs, third))
}

func TestSnapshotRuleSurvivesARestart(t *testing.T) {
	for _, restart := range []bool{false, true} {
		if !snapshotThenEdits(t, 2, restart, 150) {
			t.Fatalf("restart %v: 3 edits of 150 runs after a snapshot of 400 did not become a snapshot", restart)
		}
		if snapshotThenEdits(t, 1, restart, 150) {
			t.Fatalf("restart %v: 2 edits of 150 runs after a snapshot of 400 became a snapshot", restart)
		}
	}
}

func TestManifestsStayWithinTwiceTheirSnapshot(t *testing.T) {
	h := formatted(t, gib, testConfig(8))
	var runs []disklog.BlobRef
	var snapshots, edits, firstSnapshot uint64
	for i := range 60 {
		h.put(disklog.Hot, h.write(uint32(i), 3, uint64(i)))
		if len(runs) > 30 {
			runs = runs[10:]
		}
		b := h.checkpointRuns(runs, 1+i%7)
		runs = b.runs
		c := h.readChain(b.last)
		var total uint64
		for _, s := range c.sizes {
			total += s
		}
		if total > 2*c.sizes[0] {
			t.Fatalf("checkpoint %d: recovery reads %d manifest bytes, more than twice its snapshot's %d", i, total, c.sizes[0])
		}
		newest := c.sizes[len(c.sizes)-1]
		switch {
		case len(c.sizes) > 1:
			edits += newest
		case firstSnapshot == 0:
			firstSnapshot = newest
		default:
			snapshots += newest
		}
	}
	if edits == 0 || snapshots == 0 {
		t.Fatalf("snapshots after the first wrote %d bytes, edits %d", snapshots, edits)
	}
}

func TestChangedSegmentsSurviveAFailedCheckpoint(t *testing.T) {
	h := formatted(t, 64*core.BlockSize, testConfig(0))
	h.put(disklog.Hot, h.write(0, 1, 1))
	first := h.checkpointRuns(nil, 40)
	for i := 0; disklog.FreeSegments(h.log) > 0; i++ {
		h.put(disklog.Hot, h.write(uint32(i+1), 1, uint64(i+2)))
	}
	if err := h.log.BeginCheckpoint([]disklog.CheckpointIndex{{Owner: runOwner, ReplayLSN: h.log.DurableLSN(), Runs: first.runs}}); err != nil {
		t.Fatal(err)
	}
	it := h.blob(disklog.BlobRun, runOwner, int(h.log.MaxBlobBytes()), 7)
	if _, err := h.log.CheckpointPiece(&h.ctx, runOwner, it.Buf, h.log.MaxBlobBytes(), true, &h.out); err != nil {
		t.Fatal(err)
	}
	h.absorb()
	if failed := h.eventsOf(disklog.CheckpointFailed); len(failed) != 1 || !errors.Is(failed[0].Err, disklog.ErrNoSpace) {
		t.Fatalf("checkpoint on a full disk: %+v", failed)
	}
	var freed []uint32
	for s := uint32(1); len(freed) < 4; s++ {
		if !h.log.HoldsManifestPiece(s) {
			disklog.FreeSegment(h.log, s, true)
			freed = append(freed, s)
		}
	}
	second := h.checkpointRuns(first.runs, 0)
	edit, err := disklog.DecodeManifestEdit(h.readChain(second.last).bodies[1])
	if err != nil {
		t.Fatal(err)
	}
	if want := h.changedBetween(first.base, second.base, second.rows, freed...); !slices.Equal(edit.Segments, want) || len(want) < 4 {
		t.Fatalf("edit after a failed one holds rows\n%+v\nwant every segment changed since the snapshot\n%+v", edit.Segments, want)
	}
	third := h.checkpointRuns(second.runs, 0)
	edit, err = disklog.DecodeManifestEdit(h.readChain(third.last).bodies[2])
	if err != nil {
		t.Fatal(err)
	}
	if want := h.changedBetween(second.base, third.base, third.rows); !slices.Equal(edit.Segments, want) {
		t.Fatalf("edit after a finished one holds rows %+v, want those changed since it %+v", edit.Segments, want)
	}
}

func TestEditSizeDoesNotGrowWithTheDisk(t *testing.T) {
	var sizes []int
	for _, disk := range []uint64{gib, 1 << 40} {
		h := formatted(t, disk, testConfig(0))
		h.put(disklog.Hot, h.write(0, 3, 1))
		first := h.checkpointRuns(nil, 20)
		for i := range 5 {
			h.put(disklog.Hot, h.write(uint32(i), 3, uint64(i)))
		}
		second := h.checkpointRuns(first.runs, 1)
		body := h.readChain(second.last).bodies[1]
		edit, err := disklog.DecodeManifestEdit(body)
		if err != nil {
			t.Fatal(err)
		}
		if want := h.changedBetween(first.base, second.base, second.rows); !slices.Equal(edit.Segments, want) {
			t.Fatalf("disk of %d segments: edit rows %+v, want %+v", h.geo.SegmentCount, edit.Segments, want)
		}
		sizes = append(sizes, len(body))
	}
	if sizes[0] != sizes[1] {
		t.Fatalf("edits of %d and %d bytes for the same writes", sizes[0], sizes[1])
	}
}

func TestManifestPiecesStayHeldUntilASnapshotIsNamed(t *testing.T) {
	h := formatted(t, gib, testConfig(0))
	h.put(disklog.Hot, h.write(0, 1, 1))
	b := h.checkpointRuns(nil, 20)
	old := b.last.Segment
	if !h.log.HoldsManifestPiece(old) || h.log.HoldsManifestPiece(1) {
		t.Fatalf("holds the snapshot's segment %d: %v; the hot segment 1: %v", old, h.log.HoldsManifestPiece(old), h.log.HoldsManifestPiece(1))
	}
	h.nameManifest()
	b = h.checkpointRuns(b.runs, 1)
	if h.isSnapshot(b) || !h.log.HoldsManifestPiece(old) {
		t.Fatal("an edit let go of its snapshot's segment")
	}
	h.nameManifest()
	h.put(disklog.Cold, h.blob(disklog.BlobRaftEntries, 9, int(h.log.MaxBlobBytes()), 1))
	h.log.RequestSnapshot()
	b = h.checkpointRuns(b.runs, 1)
	if !h.isSnapshot(b) || b.last.Segment == old {
		t.Fatalf("requested snapshot in segment %d: snapshot %v", b.last.Segment, h.isSnapshot(b))
	}
	if !h.log.HoldsManifestPiece(old) {
		t.Fatal("the old pieces were let go before a segment header named the snapshot")
	}
	h.nameManifest()
	if h.log.HoldsManifestPiece(old) || !h.log.HoldsManifestPiece(b.last.Segment) {
		t.Fatalf("once the snapshot is named: holds segment %d %v, segment %d %v", old, h.log.HoldsManifestPiece(old), b.last.Segment, h.log.HoldsManifestPiece(b.last.Segment))
	}
	if b = h.checkpointRuns(b.runs, 1); h.isSnapshot(b) {
		t.Fatal("the checkpoint after the requested snapshot wrote another")
	}
}

func TestQuarantinedRowsAreAlwaysCarried(t *testing.T) {
	h, n, _, _ := usedLog(t)
	h.disk.FlipBit(h.geo.LBA(1, 0), 30, 1)
	n.live = h.reopen().index
	quarantined := func(rows []disklog.SegmentUsage) bool {
		i := slices.IndexFunc(rows, func(u disklog.SegmentUsage) bool { return u.Segment == 1 })
		return i >= 0 && rows[i].State == disklog.SegmentQuarantined
	}
	var snapshots, edits int
	for i := 0; snapshots < 2 || edits < 2; i++ {
		before := h.log.Manifest()
		h.submit(disklog.Hot, h.chunkWrite(uint32(i%16), 0, 1, uint64(2000+i)))
		for {
			h.run()
			if !n.step() {
				break
			}
		}
		if h.log.Manifest() == before {
			continue
		}
		c := h.readChain(posEvent(h, h.log.Manifest()))
		if len(c.bodies) == 1 {
			snapshots++
			m, err := disklog.DecodeManifest(c.bodies[0])
			if err != nil || !quarantined(m.Segments) {
				t.Fatalf("snapshot %+v does not carry the quarantine: %v", m.Segments, err)
			}
		} else {
			edits++
			e, err := disklog.DecodeManifestEdit(c.bodies[len(c.bodies)-1])
			if err != nil || slices.ContainsFunc(e.Segments, func(u disklog.SegmentUsage) bool { return u.Segment == 1 }) {
				t.Fatalf("edit rows %+v touch the quarantined segment: %v", e.Segments, err)
			}
		}
		if m := h.readManifestAt(h.log.Manifest()); !quarantined(m.Segments) {
			t.Fatalf("manifest %+v drops the quarantine", m.Segments)
		}
		if edits > snapshots {
			h.log.RequestSnapshot()
		}
	}
}
