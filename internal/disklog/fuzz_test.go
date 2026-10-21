package disklog

import (
	"bytes"
	"math"
	"slices"
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
	f.Add(goldenSlotRecord(f).image)
	f.Fuzz(func(t *testing.T, img []byte) {
		d, err := DecodeRecord(img, testPlace, math.MaxUint32, nil)
		if err != nil {
			return
		}
		again, err := AppendHeader(nil, d.Record, d.Entries, d.CRCs(nil))
		again = append(again, d.Data...)
		if err != nil || !bytes.Equal(again, img[:len(again)]) {
			t.Fatalf("re-encoding %+v differs: %v", d.Record, err)
		}
		if uint64(len(again)) != d.Layout.Blocks()*core.BlockSize {
			t.Fatalf("re-encoded %d bytes for %+v", len(again), d.Layout)
		}
	})
}

// BlockCRC and SlotBlockCRC never panic, and on every header DecodeHeader accepts they find each
// data block's and each slot write's blocks' stored checksums.
func FuzzBlockCRC(f *testing.F) {
	rec := goldenRecord(f)
	f.Add(rec.image, uint32(0), uint32(3), uint16(2), uint16(0))
	f.Add(rec.image[:core.BlockSize], uint32(1), uint32(3), uint16(2), uint16(5))
	f.Add(randomRecord(f, core.NewRand(6), rec.rec, 3).image, uint32(2), uint32(0), uint16(0), uint16(0))
	f.Add(goldenSlotRecord(f).image, uint32(0), uint32(3), uint16(2), uint16(1023))
	f.Fuzz(func(t *testing.T, img []byte, i, segment uint32, slot, b uint16) {
		crc, _, err := BlockCRC(img, testPlace.LogID, testPlace.Offset, i)
		scrc, _, serr := SlotBlockCRC(img, testPlace.LogID, testPlace.Offset, segment, slot, b)
		h, herr := DecodeHeader(img, testPlace, math.MaxUint32, nil)
		if herr != nil {
			return
		}
		switch {
		case i < h.Layout.DataBlocks && (err != nil || crc != h.DataCRC(int(i))):
			t.Fatalf("block %d: checksum %x, %v; the header holds %x", i, crc, err, h.DataCRC(int(i)))
		case i >= h.Layout.DataBlocks && err == nil:
			t.Fatalf("block %d of %d: checksum %x", i, h.Layout.DataBlocks, crc)
		}
		k := slices.IndexFunc(h.Entries, func(e Entry) bool { return e.Kind == EntrySlot && e.Segment == segment && e.Slot == slot })
		switch {
		case k >= 0 && b < core.BlocksPerChunk && (serr != nil || scrc != h.SlotCRC(k, b)):
			t.Fatalf("slot %d of segment %d block %d: checksum %x, %v; the header holds %x", slot, segment, b, scrc, serr, h.SlotCRC(k, b))
		case (k < 0 || b >= core.BlocksPerChunk) && serr == nil:
			t.Fatalf("slot %d of segment %d block %d, not written: checksum %x", slot, segment, b, scrc)
		}
	})
}
