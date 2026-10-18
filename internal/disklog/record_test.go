package disklog

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

var (
	testPlace = Place{LogID: testLogID, Seq: 9, Offset: 1}
	testChunk = core.ChunkID{Volume: 2, Inode: 3, Index: 4}
)

func goldenRecord(t testing.TB) testRecord {
	t.Helper()
	entries := []Entry{
		WriteEntry(1, testChunk, 5, 6, 1),
		PunchEntry(1, testChunk, 6, 0, core.BlocksPerChunk),
		BlobEntry(BlobManifest, 7, 3),
	}
	payloads := [][]byte{bytes.Repeat([]byte{0xAB}, core.BlockSize), nil, []byte("abc")}
	return encodeRecord(t, Record{Place: testPlace, LSN: 10}, entries, payloads)
}

func oneWriteRecord(t testing.TB, r *core.Rand, blocks uint16) testRecord {
	t.Helper()
	entries := []Entry{WriteEntry(3, testChunk, 1, 0, blocks)}
	return encodeRecord(t, Record{Place: testPlace, LSN: 77}, entries, [][]byte{randomBytes(r, int(blocks)*core.BlockSize)})
}

func TestLayoutExamples(t *testing.T) {
	write := func(blocks uint16) Entry { return WriteEntry(1, testChunk, 1, 0, blocks) }
	blob := func(n uint64) Entry { return BlobEntry(BlobRun, 1, n) }
	manyWrites := make([]Entry, 200)
	for i := range manyWrites {
		manyWrites[i] = write(1)
	}
	cases := []struct {
		name    string
		entries []Entry
		want    Layout
	}{
		{"write entry", []Entry{write(1)}, Layout{48 + 33 + 4, 1, 1}},
		{"punch entry", []Entry{PunchEntry(1, testChunk, 1, 0, 1)}, Layout{48 + 33, 1, 0}},
		{"freeze entry", []Entry{FreezeEntry(1, testChunk, 1, 2)}, Layout{48 + 37, 1, 0}},
		{"empty blob", []Entry{blob(0)}, Layout{48 + 14, 1, 0}},
		{"blobs 1 and 4095", []Entry{blob(1), blob(4095)}, Layout{48 + 28 + 4, 1, 1}},
		{"blob 4097", []Entry{blob(4097)}, Layout{48 + 14 + 8, 1, 2}},
		{"write 1003", []Entry{write(1003)}, Layout{4093, 1, 1003}},
		{"write 1004", []Entry{write(1004)}, Layout{4097, 2, 1004}},
		{"200 writes", manyWrites, Layout{48 + 200*33 + 200*4, 2, 200}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := LayoutOf(c.entries)
			if err != nil || got != c.want {
				t.Fatalf("got %+v, %v; want %+v", got, err, c.want)
			}
		})
	}
	t.Run("bad extents", func(t *testing.T) {
		for _, e := range []Entry{write(0), write(1025), WriteEntry(1, testChunk, 1, 1, 1024), PunchEntry(1, testChunk, 1, 1024, 1)} {
			_, err := LayoutOf([]Entry{e})
			wantErr(t, "extent", err, ErrBadExtent)
		}
	})
	t.Run("unknown kind", func(t *testing.T) {
		_, err := LayoutOf([]Entry{{Kind: 100}})
		wantErr(t, "kind", err, ErrMalformed)
	})
	t.Run("a field of another kind", func(t *testing.T) {
		freeze := FreezeEntry(1, testChunk, 1, 2)
		freeze.Owner = 7
		punch := PunchEntry(1, testChunk, 1, 0, 1)
		punch.Epoch = 3
		write := WriteEntry(1, testChunk, 1, 0, 1)
		write.Length = 4096
		blob := BlobEntry(BlobRun, 1, 10)
		blob.Version = 5
		for _, e := range []Entry{freeze, punch, write, blob} {
			_, err := LayoutOf([]Entry{e})
			wantErr(t, "field", err, ErrMalformed)
		}
	})
	t.Run("too large", func(t *testing.T) {
		_, err := LayoutOf([]Entry{blob(1 << 60)})
		wantErr(t, "blob", err, ErrTooLarge)
		_, err = LayoutOf([]Entry{blob(maxDataBytes), blob(1)})
		wantErr(t, "sum", err, ErrTooLarge)
	})
}

func TestPayloadOffset(t *testing.T) {
	entries := []Entry{
		BlobEntry(BlobRun, 1, 5),
		WriteEntry(1, testChunk, 1, 0, 2),
		PunchEntry(1, testChunk, 1, 0, 1),
		BlobEntry(BlobRun, 1, 7),
		WriteEntry(1, testChunk, 1, 0, 1),
		FreezeEntry(1, testChunk, 1, 1),
	}
	want := []uint64{3 * core.BlockSize, 0, 3*core.BlockSize + 5, 3*core.BlockSize + 5, 2 * core.BlockSize, 3*core.BlockSize + 12}
	for i, w := range want {
		if got := PayloadOffset(entries, i); got != w {
			t.Fatalf("entry %d at %d, want %d", i, got, w)
		}
	}
}

func TestRecordRoundTrip(t *testing.T) {
	r := core.NewRand(11)
	t.Run("golden", func(t *testing.T) {
		d := decodeExact(t, goldenRecord(t))
		if d.Layout != (Layout{48 + 33 + 33 + 14 + 8, 1, 2}) {
			t.Fatalf("layout %+v", d.Layout)
		}
		if d.DataCRC(0) != core.CRC32C(d.Data[:core.BlockSize]) || d.Payload(1) != nil {
			t.Fatal("data CRC or punch payload")
		}
	})
	t.Run("1-block write is 2 blocks", func(t *testing.T) {
		tr := oneWriteRecord(t, r, 1)
		if len(tr.image) != 2*core.BlockSize {
			t.Fatalf("image is %d bytes", len(tr.image))
		}
		decodeExact(t, tr)
	})
	t.Run("200 writes", func(t *testing.T) {
		entries := make([]Entry, 200)
		payloads := make([][]byte, 200)
		for i := range entries {
			entries[i] = WriteEntry(core.PoolID(i), randomChunk(r), uint64(i), uint16(i), 1)
			payloads[i] = randomBytes(r, core.BlockSize)
		}
		tr := encodeRecord(t, Record{Place: testPlace, LSN: 200}, entries, payloads)
		d := decodeExact(t, tr)
		if d.Layout.HeaderBlocks != 2 || d.Layout.DataBlocks != 200 {
			t.Fatalf("layout %+v", d.Layout)
		}
	})
	t.Run("freeze with the rest", func(t *testing.T) {
		freeze := FreezeEntry(1, testChunk, 5, 9)
		solo := encodeRecord(t, Record{Place: testPlace, LSN: 1}, []Entry{freeze}, [][]byte{nil})
		if solo.layout.DataBlocks != 0 || len(solo.image) != core.BlockSize {
			t.Fatalf("freeze alone: %+v", solo.layout)
		}
		decodeExact(t, solo)
		entries := []Entry{WriteEntry(1, testChunk, 5, 0, 1), freeze, PunchEntry(1, testChunk, 6, 10, 20), BlobEntry(BlobRaftEntries, 3, 100)}
		payloads := [][]byte{randomBytes(r, core.BlockSize), nil, nil, randomBytes(r, 100)}
		mixed := encodeRecord(t, Record{Place: testPlace, LSN: 2}, entries, payloads)
		d := decodeExact(t, mixed)
		if d.Layout.DataBlocks != 2 || d.Entries[1] != freeze {
			t.Fatalf("mixed: %+v", d.Layout)
		}
	})
	t.Run("header from crcs", func(t *testing.T) {
		tr := goldenRecord(t)
		crcs := []uint32{core.CRC32C(tr.image[core.BlockSize : 2*core.BlockSize]), core.CRC32C(tr.image[2*core.BlockSize:])}
		h, err := AppendHeader(nil, tr.rec, tr.entries, crcs)
		if err != nil || !bytes.Equal(h, tr.image[:core.BlockSize]) {
			t.Fatalf("AppendHeader: %v", err)
		}
		_, err = AppendHeader(nil, tr.rec, tr.entries, crcs[:1])
		wantErr(t, "one crc short", err, ErrMalformed)
		_, err = AppendRecord(nil, tr.rec, tr.entries, tr.image[core.BlockSize:core.BlockSize+1])
		wantErr(t, "data too short", err, ErrMalformed)
		rh, err := DecodeHeader(h, testPlace, manyLeft, nil)
		if err != nil || rh.Record != tr.rec || rh.DataCRC(1) != crcs[1] {
			t.Fatalf("DecodeHeader: %+v, %v", rh, err)
		}
	})
	t.Run("entries append", func(t *testing.T) {
		tr := goldenRecord(t)
		entries := []Entry{FreezeEntry(9, testChunk, 9, 9)}
		d, err := DecodeRecord(tr.image, testPlace, manyLeft, entries)
		if err != nil || len(d.Entries) != 3 || d.Entries[0] != tr.entries[0] {
			t.Fatalf("appending after an entry: %v", err)
		}
	})
}

func TestRecordRoundTripSeed1(t *testing.T) {
	r := core.NewRand(1)
	for lsn := uint64(1); lsn < 500; lsn++ {
		rec := Record{Place: Place{LogID: r.Uint64(), Seq: r.Uint64(), Offset: r.Uint32()}, LSN: lsn}
		tr := randomRecord(t, r, rec, 8)
		if tr.layout.DataBlocks > 8 {
			t.Fatalf("LSN %d: %d data blocks", lsn, tr.layout.DataBlocks)
		}
		d := decodeExact(t, tr)
		for i, e := range d.Entries {
			if e.Kind == EntryBlob && uint64(len(d.Payload(i))) != e.Length {
				t.Fatalf("LSN %d: blob %d has %d bytes, declared %d", lsn, i, len(d.Payload(i)), e.Length)
			}
		}
	}
}

func TestRecordCorruptByte(t *testing.T) {
	tr := oneWriteRecord(t, core.NewRand(2), 2)
	img := bytes.Clone(tr.image)
	if len(img) != 3*core.BlockSize {
		t.Fatalf("image is %d bytes", len(img))
	}
	for i := range img {
		img[i] ^= 0x10
		if _, err := DecodeRecord(img, testPlace, manyLeft, nil); err == nil {
			t.Fatalf("corrupting byte %d still decodes", i)
		}
		img[i] ^= 0x10
	}
	decodeExact(t, tr)
}

func TestRecordErrors(t *testing.T) {
	tr := goldenRecord(t)
	hdr := func(b []byte) []byte { return b[:core.BlockSize] }
	cases := []struct {
		name string
		mod  func(b []byte)
		want error
	}{
		{"magic", func(b []byte) { b[0] = 'X' }, ErrBadMagic},
		{"zeros", func(b []byte) { clear(b) }, ErrBadMagic},
		{"version", func(b []byte) { b[recVersion] = 2; fixCRC(hdr(b), recCRC) }, ErrBadVersion},
		{"header checksum", func(b []byte) { b[recLSN] ^= 1 }, ErrBadChecksum},
		{"data checksum", func(b []byte) { b[core.BlockSize+100] ^= 1 }, ErrBadChecksum},
		{"blob pad", func(b []byte) { b[len(b)-1] = 1 }, ErrBadChecksum},
		{"unknown tag", func(b []byte) { b[preludeSize] = 100; fixCRC(hdr(b), recCRC) }, ErrMalformed},
		{"zero tag", func(b []byte) { b[preludeSize] = 0; fixCRC(hdr(b), recCRC) }, ErrMalformed},
		{"header blocks 0", func(b []byte) { b[recHeaderBlocks] = 0; fixCRC(hdr(b), recCRC) }, ErrMalformed},
		{"header blocks 2", func(b []byte) { b[recHeaderBlocks] = 2; fixCRC(b[:2*core.BlockSize], recCRC) }, ErrMalformed},
		{"data blocks 3", func(b []byte) { b[recDataBlocks] = 3; fixCRC(hdr(b), recCRC) }, ErrMalformed},
		{"entry count 4", func(b []byte) { b[recEntries] = 4; fixCRC(hdr(b), recCRC) }, ErrMalformed},
		{"entry count 2", func(b []byte) { b[recEntries] = 2; fixCRC(hdr(b), recCRC) }, ErrMalformed},
		{"entry count 0", func(b []byte) { b[recEntries] = 0; fixCRC(hdr(b), recCRC) }, ErrMalformed},
		{"header pad", func(b []byte) { b[tr.layout.HeaderBytes] = 1; fixCRC(hdr(b), recCRC) }, ErrMalformed},
		{"write count 0", func(b []byte) { b[preludeSize+31] = 0; fixCRC(hdr(b), recCRC) }, ErrBadExtent},
		{"punch past chunk", func(b []byte) { b[preludeSize+33+29] = 1; fixCRC(hdr(b), recCRC) }, ErrBadExtent},
		{"log id", func(b []byte) { b[recLogID] ^= 1; fixCRC(hdr(b), recCRC) }, ErrMismatch},
		{"segment seq", func(b []byte) { b[recSeq] = 10; fixCRC(hdr(b), recCRC) }, ErrMismatch},
		{"offset", func(b []byte) { b[recOffset] = 2; fixCRC(hdr(b), recCRC) }, ErrMismatch},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b := bytes.Clone(tr.image)
			c.mod(b)
			_, err := DecodeRecord(b, testPlace, manyLeft, nil)
			wantErr(t, c.name, err, c.want)
		})
	}
	t.Run("truncated", func(t *testing.T) {
		for _, n := range []int{0, 4, 48, core.BlockSize, len(tr.image) - 1} {
			_, err := DecodeRecord(tr.image[:n], testPlace, manyLeft, nil)
			wantErr(t, "truncated", err, ErrTruncated)
		}
		_, err := DecodeHeader(tr.image[:core.BlockSize], testPlace, manyLeft, nil)
		if err != nil {
			t.Fatalf("header alone: %v", err)
		}
	})
	t.Run("length at end", func(t *testing.T) {
		_, err := DecodeRecord(tr.image, testPlace, 2, nil)
		wantErr(t, "2 left", err, ErrBadLengthAtEnd)
		if _, err := DecodeRecord(tr.image, testPlace, 3, nil); err != nil {
			t.Fatalf("3 left: %v", err)
		}
	})
	t.Run("expected place", func(t *testing.T) {
		for _, at := range []Place{{testLogID + 1, 9, 1}, {testLogID, 8, 1}, {testLogID, 9, 0}} {
			_, err := DecodeRecord(tr.image, at, manyLeft, nil)
			wantErr(t, "place", err, ErrMismatch)
		}
	})
	t.Run("recycled segment", func(t *testing.T) {
		old := encodeRecord(t, Record{Place: Place{testLogID, 8, 1}, LSN: 3}, tr.entries, tr.payloads)
		_, err := DecodeRecord(old.image, testPlace, manyLeft, nil)
		wantErr(t, "old incarnation", err, ErrMismatch)
	})
	t.Run("entry sizes", func(t *testing.T) {
		h := tr.image
		if h[preludeSize] != byte(EntryWrite) || h[preludeSize+33] != byte(EntryPunch) || h[preludeSize+66] != byte(EntryBlob) {
			t.Fatal("entry tags not at 48, 81 and 114")
		}
		if binary.LittleEndian.Uint16(h[preludeSize+29:]) != 6 || binary.LittleEndian.Uint16(h[preludeSize+31:]) != 1 {
			t.Fatal("write extent not at the end of the entry")
		}
	})
}
