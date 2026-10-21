package disklog

import (
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

func TestOrdinaryBytesCountsTouchedBlocks(t *testing.T) {
	r := core.NewRand(1)
	for i := range 10000 {
		rec := randomRecord(t, r, Record{LSN: 1}, 64)
		ref := make([]bool, rec.layout.Blocks())
		for b := range rec.layout.HeaderBlocks {
			ref[b] = true
		}
		var own, ordinary int
		for j := range rec.entries {
			e := &rec.entries[j]
			if e.Kind == EntryBlob && (e.BlobKind == BlobRun || isManifestKind(e)) {
				own++
				continue
			}
			ordinary++
			off := uint64(rec.layout.HeaderBlocks)*core.BlockSize + PayloadOffset(rec.entries, j)
			n := e.dataBytes()
			for b := off / core.BlockSize; n > 0 && b < (off+n+core.BlockSize-1)/core.BlockSize; b++ {
				ref[b] = true
			}
		}
		var want uint64
		for _, set := range ref {
			if set {
				want += core.BlockSize
			}
		}
		if ordinary == 0 {
			want = 0
		}
		got := ordinaryBytes(rec.entries, rec.layout)
		if got != want || own == 0 && got != rec.layout.Blocks()*core.BlockSize {
			t.Fatalf("record %d, %+v in %+v: %d ordinary bytes, want %d", i, rec.entries, rec.layout, got, want)
		}
	}
}
