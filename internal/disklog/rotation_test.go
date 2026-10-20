package disklog

import (
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

func TestSnapshotRuleIsPebbles(t *testing.T) {
	const kib = 1 << 10
	for _, c := range []struct {
		snapshot, since, edit, next uint64
		snapshotDue                 bool
	}{
		{100 * kib, 40 * kib, 20 * kib, 110 * kib, false},
		{100 * kib, 90 * kib, 20 * kib, 110 * kib, true},
		{100 * kib, 40 * kib, 15 * kib, 50 * kib, true},
		{100 * kib, 0, 100 * kib, 120 * kib, false},
	} {
		if got := snapshotDue(c.snapshot, c.since, c.edit, c.next); got != c.snapshotDue {
			t.Fatalf("%+v: snapshot due %v", c, got)
		}
	}
}

func TestManifestBytesCountsEveryPiece(t *testing.T) {
	prior := [][]BlobRef{{{Segment: 1}}, {{Segment: 2}, {Segment: 3}}}
	list := pieceListSize + pieceListBytes(prior) + pieceListEntrySize
	for _, c := range []struct {
		body    int
		maxBlob uint64
		want    uint64
		ok      bool
	}{
		{100, core.BlockSize, uint64(list + 100), true},
		{core.BlockSize - list, core.BlockSize, core.BlockSize, true},
		{core.BlockSize - list + 1, core.BlockSize, uint64(core.BlockSize-list+1) + uint64(list+BlobRefSize), true},
		{10, uint64(list - 1), 0, false},
	} {
		got, ok := manifestBytes(c.body, prior, c.maxBlob)
		if got != c.want || ok != c.ok {
			t.Fatalf("%+v: %d bytes, fits %v", c, got, ok)
		}
	}
}
