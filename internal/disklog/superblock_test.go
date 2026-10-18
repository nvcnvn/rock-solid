package disklog

import (
	"bytes"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

func testSuperblock(t testing.TB) Superblock {
	t.Helper()
	g, err := GeometryFor(1 << 30)
	if err != nil {
		t.Fatal(err)
	}
	return Superblock{LogID: testLogID, Geometry: g}
}

func TestSuperblockRoundTrip(t *testing.T) {
	sb := testSuperblock(t)
	b, err := AppendSuperblock(nil, sb)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) != core.BlockSize || !bytes.Equal(b[:8], []byte(superblockMagic)) {
		t.Fatalf("encoded %d bytes starting % x", len(b), b[:8])
	}
	got, err := DecodeSuperblock(b)
	if err != nil || got != sb {
		t.Fatalf("decoded %+v, %v; want %+v", got, err, sb)
	}
	if _, err := DecodeSuperblock(append(b, 0xFF)); err != nil {
		t.Fatalf("decoding a block with trailing bytes: %v", err)
	}
}

func TestSuperblockErrors(t *testing.T) {
	sb := testSuperblock(t)
	good, err := AppendSuperblock(nil, sb)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		mod  func(b []byte)
		want error
	}{
		{"zeros", func(b []byte) { clear(b) }, ErrBadMagic},
		{"magic", func(b []byte) { b[0] = 'X' }, ErrBadMagic},
		{"version", func(b []byte) { b[sbVersion] = 2; fixCRC(b, sbCRC) }, ErrBadVersion},
		{"log id bit", func(b []byte) { b[sbLogID] ^= 1 }, ErrBadChecksum},
		{"crc bit", func(b []byte) { b[sbCRC] ^= 1 }, ErrBadChecksum},
		{"tail bit", func(b []byte) { b[core.BlockSize-1] ^= 1 }, ErrBadChecksum},
		{"pad", func(b []byte) { b[sbVersion+2] = 1; fixCRC(b, sbCRC) }, ErrMalformed},
		{"tail", func(b []byte) { b[superblockSize] = 1; fixCRC(b, sbCRC) }, ErrMalformed},
		{"segment blocks 3", func(b []byte) { b[sbSegmentBlocks] = 3; fixCRC(b, sbCRC) }, ErrMalformed},
		{"segment blocks 96", func(b []byte) { b[sbSegmentBlocks] = 96; fixCRC(b, sbCRC) }, ErrMalformed},
		{"segment count 3", func(b []byte) { b[sbSegmentCount], b[sbSegmentCount+1] = 3, 0; fixCRC(b, sbCRC) }, ErrMalformed},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b := bytes.Clone(good)
			c.mod(b)
			_, err := DecodeSuperblock(b)
			wantErr(t, c.name, err, c.want)
		})
	}
	t.Run("short", func(t *testing.T) {
		_, err := DecodeSuperblock(good[:superblockSize])
		wantErr(t, "short", err, ErrTruncated)
	})
	t.Run("encode bad geometry", func(t *testing.T) {
		_, err := AppendSuperblock(nil, Superblock{LogID: 1, Geometry: Geometry{SegmentBlocks: 6, SegmentCount: 4}})
		wantErr(t, "encode", err, ErrMalformed)
	})
}
