package disklog_test

import (
	"fmt"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// freeSegments frees the n lowest segments of usedLog whose records all precede the replay point
// and that hold no piece, as the cleaner would once their moves applied. Allocation takes the
// lowest free segment, so the first one freed is the first opened again.
func freeSegments(t *testing.T, n int, cfg func(*disklog.Config)) (*harness, *node, []uint32) {
	t.Helper()
	h, nd, _, _ := usedLog(t)
	if cfg != nil {
		cfg(&h.cfg)
		h.reopen()
		nd = newNode(h, 0)
	}
	var freed []uint32
	for s := uint32(1); s < h.geo.SegmentCount && len(freed) < n; s++ {
		if !h.log.Cleanable(s) || h.log.HoldsPiece(s) || h.log.HoldsManifestPiece(s) {
			continue
		}
		u, _ := h.log.Usage(s)
		if err := h.log.AddLive(s, -int(u.Live)); err != nil {
			t.Fatal(err)
		}
		if err := h.log.Free(&h.ctx, s, &h.out); err != nil {
			t.Fatal(err)
		}
		freed = append(freed, s)
	}
	if len(freed) < n {
		t.Fatalf("freed %v of %d", freed, n)
	}
	h.absorb()
	return h, nd, freed
}

func (h *harness) trimmedSegments() []uint32 {
	var segs []uint32
	for _, a := range h.trims {
		segs = append(segs, uint32(uint64(a.LBA)/uint64(h.geo.SegmentBlocks)))
	}
	return segs
}

// A freed segment is trimmed only once a segment header names the manifest that freed it: until
// then the manifest recovery loads still lists it, and a listed segment that reads as zeros is
// damage. The TRIM covers the whole segment.
func TestATrimWaitsForAHeaderNamingTheFreeingManifest(t *testing.T) {
	h, n, freed := freeSegments(t, 8, nil)
	seqs := map[uint32]uint64{}
	for _, s := range freed {
		seqs[s] = h.segmentSeq(s)
	}
	before := h.log.Manifest()
	h.drive(n, func() bool { return h.log.Manifest() != before })
	if len(h.trims) != 0 {
		t.Fatalf("trimmed %v before a header named the freeing manifest", h.trimmedSegments())
	}
	h.drive(n, func() bool { return len(h.trims) > 0 })
	if h.named() != h.log.Manifest() || h.trims[0].Len != uint64(h.geo.SegmentBlocks)*core.BlockSize {
		t.Fatalf("trims %+v once %+v is named", h.trims, h.named())
	}
	for _, s := range h.trimmedSegments() {
		if !slices.Contains(freed[1:], s) {
			t.Fatalf("trimmed segment %d; freed %v, the first opened again to name the manifest", s, freed)
		}
	}
}

// A freed segment allocated again before its TRIM runs drops it, whether the checkpoint that
// freed it had begun or not.
func TestASegmentReusedBeforeItsTrimDropsIt(t *testing.T) {
	for _, during := range []bool{false, true} {
		h, n, _ := freeSegments(t, 1, nil)
		if during {
			if err := h.log.BeginCheckpoint([]disklog.CheckpointIndex{{Owner: indexOwner, ReplayLSN: h.log.DurableLSN()}}); err != nil {
				t.Fatal(err)
			}
		}
		old := h.segmentSeq(1)
		for i := 0; h.segmentSeq(1) == old; i++ {
			h.put(disklog.Hot, h.chunkWrite(uint32(i%16), 0, 1, uint64(7000+i)))
		}
		if during {
			if err := h.log.EndCheckpoint(&h.ctx, &h.out); err != nil {
				t.Fatal(err)
			}
			h.absorb()
			h.run()
		}
		before := h.log.Manifest()
		h.drive(n, func() bool { return h.log.Manifest() != before && h.named() == h.log.Manifest() })
		if len(h.trims) != 0 {
			t.Fatalf("checkpoint running %v: trimmed %v after segment 1 was opened again", during, h.trimmedSegments())
		}
	}
}

// A segment whose TRIM is in flight is not opened until the TRIM completes, so a late discard
// cannot erase new records.
func TestAnIssuedTrimHoldsItsSegment(t *testing.T) {
	h, n, _ := freeSegments(t, 8, nil)
	h.holdTrims = true
	h.drive(n, func() bool { return len(h.trims) > 0 })
	held := h.trimmedSegments()
	mark, from := len(h.trace), h.log.DurableLSN()
	h.drive(n, func() bool { return h.log.DurableLSN() > from+60 })
	for _, line := range h.trace[mark:] {
		var lba, blocks, io uint64
		if _, err := fmt.Sscanf(line, "write %d+%d io%d", &lba, &blocks, &io); err == nil && slices.Contains(held, uint32(lba/uint64(h.geo.SegmentBlocks))) {
			t.Fatalf("%s: a segment written while its TRIM was in flight (%v)", line, held)
		}
	}
	h.holdTrims = false
	h.run()
}

// A disk that is a file never trims.
func TestAFileNeverTrims(t *testing.T) {
	h, n, _ := freeSegments(t, 8, func(c *disklog.Config) { c.NoTrim = true })
	before := h.log.Manifest()
	h.drive(n, func() bool { return h.log.Manifest() != before && h.named() == h.log.Manifest() })
	from := h.log.DurableLSN()
	h.drive(n, func() bool { return h.log.DurableLSN() > from+60 })
	if len(h.trims) != 0 {
		t.Fatalf("trimmed %v with NoTrim", h.trimmedSegments())
	}
}
