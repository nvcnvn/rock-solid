package disklog

import (
	"bytes"
	"math"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

func FuzzDecodeSuperblock(f *testing.F) {
	golden := readGolden(f)
	f.Add(goldenImages(f)["superblock"])
	f.Add(golden["superblock"])
	f.Add(make([]byte, core.BlockSize))
	f.Fuzz(func(t *testing.T, b []byte) {
		sb, err := DecodeSuperblock(b)
		if err != nil {
			return
		}
		again, err := AppendSuperblock(nil, sb)
		if err != nil || !bytes.Equal(again, b[:core.BlockSize]) {
			t.Fatalf("re-encoding %+v differs: %v", sb, err)
		}
	})
}

func FuzzDecodeSegmentHeader(f *testing.F) {
	golden := readGolden(f)
	f.Add(goldenImages(f)["segment_header"])
	f.Add(goldenImages(f)["segment_header_merge"])
	f.Add(golden["segment_header"])
	f.Add(make([]byte, core.BlockSize))
	f.Fuzz(func(t *testing.T, b []byte) {
		h, err := DecodeSegmentHeader(b)
		if err != nil {
			return
		}
		again, err := AppendSegmentHeader(nil, h)
		if err != nil || !bytes.Equal(again, b[:core.BlockSize]) {
			t.Fatalf("re-encoding %+v differs: %v", h, err)
		}
	})
}

func FuzzDecodeRecord(f *testing.F) {
	golden := readGolden(f)
	rec := goldenRecord(f)
	f.Add(rec.image)
	f.Add(rec.image[:core.BlockSize])
	f.Add(golden["record_header"])
	f.Add(randomRecord(f, core.NewRand(6), rec.rec, 3).image)
	f.Fuzz(func(t *testing.T, img []byte) {
		d, err := DecodeRecord(img, testPlace, math.MaxUint32, nil)
		if err != nil {
			return
		}
		again, err := AppendRecord(nil, d.Record, d.Entries, d.Data)
		if err != nil || !bytes.Equal(again, img[:len(again)]) {
			t.Fatalf("re-encoding %+v differs: %v", d.Record, err)
		}
		if uint64(len(again)) != d.Layout.Blocks()*core.BlockSize {
			t.Fatalf("re-encoded %d bytes for %+v", len(again), d.Layout)
		}
	})
}
