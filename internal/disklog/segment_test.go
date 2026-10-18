package disklog

import (
	"bytes"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

func testSegmentHeader() SegmentHeader {
	return SegmentHeader{LogID: testLogID, Seq: 7, Index: 3, Stream: Cold, Manifest: ManifestPos{Segment: 2, Block: 9, LSN: 41}}
}

func TestSegmentHeaderRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		h    SegmentHeader
	}{
		{"cold with manifest", testSegmentHeader()},
		{"hot with manifest", SegmentHeader{LogID: 5, Seq: 1, Index: 1, Stream: Hot, Manifest: ManifestPos{Segment: 4000, Block: 1, LSN: 1 << 40}}},
		{"hot no manifest", SegmentHeader{LogID: testLogID, Seq: 1 << 50, Index: 1 << 20, Stream: Hot}},
		{"cold no manifest", SegmentHeader{LogID: 1, Seq: 2, Index: 4095, Stream: Cold}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b, err := AppendSegmentHeader(nil, c.h)
			if err != nil {
				t.Fatal(err)
			}
			if len(b) != core.BlockSize || !bytes.Equal(b[:8], []byte(segmentMagic)) {
				t.Fatalf("encoded %d bytes starting % x", len(b), b[:8])
			}
			got, err := DecodeSegmentHeader(b)
			if err != nil || got != c.h {
				t.Fatalf("decoded %+v, %v; want %+v", got, err, c.h)
			}
			if got.Manifest.IsNone() != (c.h.Manifest.Segment == 0) {
				t.Fatal("IsNone")
			}
		})
	}
}

func TestSegmentHeaderBitFlips(t *testing.T) {
	good, err := AppendSegmentHeader(nil, testSegmentHeader())
	if err != nil {
		t.Fatal(err)
	}
	b := bytes.Clone(good)
	for i := range b {
		for bit := range 8 {
			b[i] ^= 1 << bit
			if _, err := DecodeSegmentHeader(b); err == nil {
				t.Fatalf("flipping bit %d of byte %d still decodes", bit, i)
			}
			b[i] ^= 1 << bit
		}
	}
	if !bytes.Equal(b, good) {
		t.Fatal("header not restored")
	}
}

func TestSegmentHeaderErrors(t *testing.T) {
	good, err := AppendSegmentHeader(nil, testSegmentHeader())
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		mod  func(b []byte)
		want error
	}{
		{"zeros", func(b []byte) { clear(b) }, ErrBadMagic},
		{"superblock magic", func(b []byte) { copy(b, superblockMagic) }, ErrBadMagic},
		{"version", func(b []byte) { b[sgVersion] = 0; fixCRC(b, sgCRC) }, ErrBadVersion},
		{"checksum", func(b []byte) { b[sgSeq] ^= 0x10 }, ErrBadChecksum},
		{"stream 2", func(b []byte) { b[sgStream] = 2; fixCRC(b, sgCRC) }, ErrMalformed},
		{"pad", func(b []byte) { b[sgStream+1] = 1; fixCRC(b, sgCRC) }, ErrMalformed},
		{"tail", func(b []byte) { b[segmentHeaderSize] = 1; fixCRC(b, sgCRC) }, ErrMalformed},
		{"index 0", func(b []byte) { b[sgIndex] = 0; fixCRC(b, sgCRC) }, ErrMalformed},
		{"manifest block 0", func(b []byte) { b[sgManifestBlock] = 0; fixCRC(b, sgCRC) }, ErrMalformed},
		{"no manifest with lsn", func(b []byte) { b[sgManifestSegment] = 0; b[sgManifestBlock] = 0; fixCRC(b, sgCRC) }, ErrMalformed},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b := bytes.Clone(good)
			c.mod(b)
			_, err := DecodeSegmentHeader(b)
			wantErr(t, c.name, err, c.want)
		})
	}
	t.Run("short", func(t *testing.T) {
		_, err := DecodeSegmentHeader(good[:core.BlockSize-1])
		wantErr(t, "short", err, ErrTruncated)
	})
	encodes := []struct {
		name string
		h    SegmentHeader
	}{
		{"stream 2", SegmentHeader{LogID: 1, Seq: 1, Index: 1, Stream: 2}},
		{"index 0", SegmentHeader{LogID: 1, Seq: 1, Index: 0}},
		{"none with block", SegmentHeader{LogID: 1, Seq: 1, Index: 1, Manifest: ManifestPos{Block: 1}}},
		{"manifest block 0", SegmentHeader{LogID: 1, Seq: 1, Index: 1, Manifest: ManifestPos{Segment: 2}}},
	}
	for _, c := range encodes {
		t.Run("encode "+c.name, func(t *testing.T) {
			_, err := AppendSegmentHeader(nil, c.h)
			wantErr(t, c.name, err, ErrMalformed)
		})
	}
}
