package disklog_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

func (h *harness) liveOf(s uint32) uint32 {
	row, _ := usageRow(h, s)
	return row.Live
}

// A merge's consumed inputs, a replaced run and the like stay live, and their segments uncleanable,
// until a segment header names the manifest that dropped them: a crash before that loads the
// older manifest, which still names them. Then their blocks leave the usage table.
func TestDeadPiecesWaitForTheirManifestToBeNamed(t *testing.T) {
	h := formatted(t, gib, testConfig(0))
	h.put(disklog.Hot, h.write(0, 1, 1))
	first := h.checkpointPieces(nil, 1, 3*core.BlockSize)
	h.nameManifest()
	piece := first.runs[0]
	h.checkpointRuns(nil, 0)
	before := h.liveOf(piece.Segment)
	if err := h.log.PiecesDead(first.runs); err != nil {
		t.Fatal(err)
	}
	names := func() bool {
		_, indexes := h.log.Named(nil)
		return slices.ContainsFunc(indexes, func(ix disklog.ManifestIndex) bool { return slices.Contains(ix.Runs, piece) })
	}
	if !disklog.HoldsLivePiece(h.log, piece) || h.liveOf(piece.Segment) != before || !names() {
		t.Fatal("a piece the named manifest still names left the live pieces, or Named no longer lists its run")
	}
	h.nameManifest()
	if names() {
		t.Fatal("Named lists a run the newly named manifest dropped")
	}
	if disklog.HoldsLivePiece(h.log, piece) || h.liveOf(piece.Segment) > before-3 {
		t.Fatalf("segment %d counts %d live blocks after its 3-block piece died, %d before", piece.Segment, h.liveOf(piece.Segment), before)
	}
	if err := h.log.PiecesDead(first.runs); !errors.Is(err, disklog.ErrMalformed) {
		t.Fatalf("a piece reported dead twice: %v", err)
	}
	h.close()
}

// Pieces packed into one round share their blocks: a block stays live until the last piece
// touching it is dead. The harness checks after every step that each metadata segment counts
// exactly the blocks its live pieces touch; the piece set's own test checks the sharing.
func TestPiecesSharingABlockKeepItLive(t *testing.T) {
	h := formatted(t, gib, testConfig(0))
	h.put(disklog.Hot, h.write(0, 1, 1))
	first := h.checkpointPieces(nil, 3, 100)
	h.nameManifest()
	a, b := first.runs[1], first.runs[2]
	if a.LSN != b.LSN {
		t.Fatalf("the two pieces went out in records %d and %d, want one", a.LSN, b.LSN)
	}
	h.checkpointRuns(first.runs[2:], 0)
	if err := h.log.PiecesDead(first.runs[:2]); err != nil {
		t.Fatal(err)
	}
	h.nameManifest()
	if disklog.HoldsLivePiece(h.log, a) || !disklog.HoldsLivePiece(h.log, b) || !h.log.HoldsPiece(b.Segment) {
		t.Fatal("piece a is still live, or b, which shares its block, left with it")
	}
	h.checkpointRuns(nil, 0)
	if err := h.log.PiecesDead(first.runs[2:]); err != nil {
		t.Fatal(err)
	}
	h.nameManifest()
	if disklog.HoldsLivePiece(h.log, b) {
		t.Fatal("piece b is still live")
	}
	h.close()
}

// A merge step written but never named before a crash counts nothing once the log is open
// again, so the reserve bound holds across crashes.
func TestAStepNeverNamedCountsNothingAfterACrash(t *testing.T) {
	h := formatted(t, gib, mergeConfig(1))
	h.put(disklog.Hot, h.write(0, 1, 1))
	h.checkpointRuns(nil, 1)
	step := h.putMerge(1, runOwner, 3*core.BlockSize, 2)
	if h.liveOf(step.Segment) != 3 {
		t.Fatalf("the step's segment counts %d live blocks, want 3", h.liveOf(step.Segment))
	}
	h.reopen()
	if row, ok := usageRow(h, step.Segment); !ok || row.Live != 0 || row.State != disklog.SegmentUsed {
		t.Fatalf("after the crash the unnamed step's segment is %+v, %v; want used with no live block", row, ok)
	}
	h.close()
}

// The owner sets the live blocks of every segment outside the metadata stream from its index's
// record counts at open, and reports each change after.
func TestOwnerCountsChunkSegments(t *testing.T) {
	h := formatted(t, gib, testConfig(0))
	a := h.submit(disklog.Hot, h.write(0, 4, 1))
	h.run()
	seg := h.durableOf(a).Segment
	if h.liveOf(seg) != 4 {
		t.Fatalf("segment %d counts %d live blocks after a 4-block write", seg, h.liveOf(seg))
	}
	if err := h.log.AddLive(seg, -3); err != nil || h.liveOf(seg) != 1 {
		t.Fatalf("after 3 blocks died segment %d counts %d: %v", seg, h.liveOf(seg), err)
	}
	for _, c := range []struct {
		seg   uint32
		delta int
	}{{seg, -2}, {0, 1}, {h.geo.SegmentCount, 1}, {seg + 1, 1}} {
		if err := h.log.AddLive(c.seg, c.delta); !errors.Is(err, disklog.ErrMalformed) {
			t.Fatalf("AddLive(%d, %d): %v", c.seg, c.delta, err)
		}
	}
	chunk := make([]uint32, h.geo.SegmentCount)
	chunk[seg] = 2
	h.log.Recount(chunk)
	if h.liveOf(seg) != 2 {
		t.Fatalf("recount left segment %d at %d live blocks, want 2", seg, h.liveOf(seg))
	}
	h.close()
}

// A rebuild reads no run, so it cannot tell live pieces from dead: every metadata segment is
// held until a segment header names a manifest written since, and only then counted.
func TestRebuildHoldsMetadataSegmentsUntilItsManifestIsNamed(t *testing.T) {
	h := formatted(t, gib, testConfig(0))
	h.put(disklog.Hot, h.write(0, 1, 1))
	first := h.checkpointPieces(nil, 1, 3*core.BlockSize)
	h.nameManifest()
	if res := h.recover(disklog.ModeRebuild); res.err != nil {
		t.Fatal(res.err)
	}
	seg := first.runs[0].Segment
	if disklog.HoldsLivePiece(h.log, first.runs[0]) || !h.log.HoldsPiece(seg) {
		t.Fatal("a rebuild left the run's segment unheld")
	}
	h.checkpointRuns(nil, 0)
	h.nameManifest()
	if slices.ContainsFunc(first.runs, func(r disklog.BlobRef) bool { return disklog.HoldsLivePiece(h.log, r) }) || h.liveOf(seg) > 2 {
		t.Fatalf("after the rebuild's manifest is named segment %d still counts %d live blocks", seg, h.liveOf(seg))
	}
	h.close()
}
