package disklog

import (
	"bytes"
	"encoding/binary"
	"errors"
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
	return encodeRecord(t, Record{Place: testPlace, LSN: 10, Durable: 9}, entries, payloads)
}

// goldenSlotRecord is a write of a whole chunk to slot 2 of segment 3, its data 0xCD, and a
// 1-block write of data 0xAB.
func goldenSlotRecord(t testing.TB) testRecord {
	t.Helper()
	entries := []Entry{SlotEntry(1, testChunk, 5, 3, 2), WriteEntry(1, testChunk, 6, 6, 1)}
	payloads := [][]byte{bytes.Repeat([]byte{0xCD}, core.ChunkSize), bytes.Repeat([]byte{0xAB}, core.BlockSize)}
	return encodeRecord(t, Record{Place: testPlace, LSN: 10, Durable: 9}, entries, payloads)
}

func oneWriteRecord(t testing.TB, r *core.Rand, blocks uint16) testRecord {
	t.Helper()
	entries := []Entry{WriteEntry(3, testChunk, 1, 0, blocks)}
	return encodeRecord(t, Record{Place: testPlace, LSN: 77, Durable: 0x4c}, entries, [][]byte{randomBytes(r, int(blocks)*core.BlockSize)})
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
		{"write entry", []Entry{write(1)}, Layout{56 + 33 + 4, 1, 1, 0}},
		{"punch entry", []Entry{PunchEntry(1, testChunk, 1, 0, 1)}, Layout{56 + 33, 1, 0, 0}},
		{"freeze entry", []Entry{FreezeEntry(1, testChunk, 1, 2)}, Layout{56 + 37, 1, 0, 0}},
		{"empty blob", []Entry{blob(0)}, Layout{56 + 14, 1, 0, 0}},
		{"blobs 1 and 4095", []Entry{blob(1), blob(4095)}, Layout{56 + 28 + 4, 1, 1, 0}},
		{"blob 4097", []Entry{blob(4097)}, Layout{56 + 14 + 8, 1, 2, 0}},
		{"write 1001", []Entry{write(1001)}, Layout{4093, 1, 1001, 0}},
		{"write 1002", []Entry{write(1002)}, Layout{4097, 2, 1002, 0}},
		{"write 1003", []Entry{write(1003)}, Layout{4101, 2, 1003, 0}},
		{"write 1004", []Entry{write(1004)}, Layout{4105, 2, 1004, 0}},
		{"200 writes", manyWrites, Layout{56 + 200*33 + 200*4, 2, 200, 0}},
		{"slot write", []Entry{SlotEntry(1, testChunk, 1, 3, 2)}, Layout{56 + 39 + 1024*4, 2, 0, 1024}},
		{"slot write and a write", []Entry{SlotEntry(1, testChunk, 1, 3, 2), write(1)}, Layout{56 + 39 + 33 + 1025*4, 2, 1, 1024}},
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
		if d.Layout != (Layout{56 + 33 + 33 + 14 + 8, 1, 2, 0}) {
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
		rec := Record{Place: Place{LogID: r.Uint64(), Seq: r.Uint64(), Offset: r.Uint32()}, LSN: lsn, Durable: r.Below(lsn)}
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

func TestRecordDurableLSN(t *testing.T) {
	entries := []Entry{PunchEntry(1, testChunk, 1, 0, 1)}
	first := encodeRecord(t, Record{Place: testPlace, LSN: 1}, entries, [][]byte{nil})
	if binary.LittleEndian.Uint64(first.image[recDurable:]) != 0 {
		t.Fatal("the first record's durable LSN is not 0")
	}
	decodeExact(t, first)
	for _, durable := range []uint64{1, 2, 1 << 63} {
		rec := Record{Place: testPlace, LSN: 1, Durable: durable}
		if img, err := AppendRecord(nil, rec, entries, nil); !errors.Is(err, ErrMalformed) || len(img) != 0 {
			t.Fatalf("AppendRecord with durable LSN %d: %d bytes, %v", durable, len(img), err)
		}
		if img, err := AppendHeader(nil, rec, entries, nil); !errors.Is(err, ErrMalformed) || len(img) != 0 {
			t.Fatalf("AppendHeader with durable LSN %d: %d bytes, %v", durable, len(img), err)
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
		{"a bit of the durable LSN", func(b []byte) { b[recDurable] ^= 1 }, ErrBadChecksum},
		{"durable LSN at its own", func(b []byte) { b[recDurable] = 10; fixCRC(hdr(b), recCRC) }, ErrMalformed},
		{"durable LSN past its own", func(b []byte) { b[recDurable+7] = 1; fixCRC(hdr(b), recCRC) }, ErrMalformed},
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
		for _, n := range []int{0, 4, 56, core.BlockSize, len(tr.image) - 1} {
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
			t.Fatal("entry tags not at 56, 89 and 122")
		}
		if binary.LittleEndian.Uint16(h[preludeSize+29:]) != 6 || binary.LittleEndian.Uint16(h[preludeSize+31:]) != 1 {
			t.Fatal("write extent not at the end of the entry")
		}
	})
}

func TestSlotRecord(t *testing.T) {
	tr := goldenSlotRecord(t)
	if tr.layout != (Layout{56 + 39 + 33 + 1025*4, 2, 1, 1024}) || len(tr.image) != 3*core.BlockSize {
		t.Fatalf("layout %+v, image of %d bytes", tr.layout, len(tr.image))
	}
	h := tr.image
	if h[preludeSize] != byte(EntrySlot) || binary.LittleEndian.Uint32(h[preludeSize+33:]) != 3 || binary.LittleEndian.Uint16(h[preludeSize+37:]) != 2 || h[preludeSize+39] != byte(EntryWrite) {
		t.Fatal("the slot's segment and index do not follow the write's fields")
	}
	d := decodeExact(t, tr)
	slotCRC := core.CRC32C(bytes.Repeat([]byte{0xCD}, core.BlockSize))
	if d.Entries[0] != SlotEntry(1, testChunk, 5, 3, 2) || d.DataCRC(0) != core.CRC32C(d.Data) || d.SlotCRC(0, 0) != slotCRC || d.SlotCRC(0, 1023) != slotCRC || d.Payload(0) != nil {
		t.Fatalf("decoded %+v", d.Entries)
	}
	if crc, _, err := BlockCRC(tr.image, testLogID, 1, 0); err != nil || crc != d.DataCRC(0) {
		t.Fatalf("data block 0: %x, %v", crc, err)
	}
	if _, _, err := BlockCRC(tr.image, testLogID, 1, 1); !errors.Is(err, ErrMalformed) {
		t.Fatalf("data block 1 of 1: %v", err)
	}
	if crc, hb, err := SlotBlockCRC(tr.image, testLogID, 1, 3, 2, 1023); err != nil || crc != slotCRC || hb != 2 {
		t.Fatalf("slot block 1023: %x, %d header blocks, %v", crc, hb, err)
	}
	for _, c := range []struct {
		segment uint32
		slot    uint16
		block   uint16
	}{{3, 1, 0}, {4, 2, 0}, {3, 2, 1024}} {
		if _, _, err := SlotBlockCRC(tr.image, testLogID, 1, c.segment, c.slot, c.block); !errors.Is(err, ErrMalformed) {
			t.Fatalf("slot %d of segment %d block %d: %v", c.slot, c.segment, c.block, err)
		}
	}
	if _, hb, err := SlotBlockCRC(tr.image[:core.BlockSize], testLogID, 1, 3, 2, 0); !errors.Is(err, ErrTruncated) || hb != 2 {
		t.Fatalf("one header block of two: %d, %v", hb, err)
	}
	if _, err := AppendRecord(nil, tr.rec, tr.entries, nil); !errors.Is(err, ErrMalformed) {
		t.Fatalf("AppendRecord of a slot write: %v", err)
	}
	fix := func(b []byte) { fixCRC(b[:2*core.BlockSize], recCRC) }
	for name, c := range map[string]struct {
		mod  func(b []byte)
		want error
	}{
		"not the whole chunk": {func(b []byte) { b[preludeSize+31] = 1; fix(b) }, ErrBadExtent},
		"from block 1":        {func(b []byte) { b[preludeSize+29] = 1; fix(b) }, ErrBadExtent},
		"segment 0":           {func(b []byte) { clear(b[preludeSize+33 : preludeSize+37]); fix(b) }, ErrMalformed},
		"a slot checksum":     {func(b []byte) { b[preludeSize+39+33+4+100] ^= 1 }, ErrBadChecksum},
	} {
		b := bytes.Clone(tr.image)
		c.mod(b)
		_, err := DecodeRecord(b, testPlace, manyLeft, nil)
		wantErr(t, name, err, c.want)
	}
	for _, e := range []Entry{{Kind: EntrySlot, Pool: 1, Chunk: testChunk, Count: 1, Segment: 3}, {Kind: EntrySlot, Pool: 1, Chunk: testChunk, Count: core.BlocksPerChunk, Segment: 3, Epoch: 1}} {
		if _, err := LayoutOf([]Entry{e}); err == nil {
			t.Fatalf("layout of %+v", e)
		}
	}
}

// For every subset of a slot record's blocks landing over zeros or random old content, decoding
// returns the whole record or rejects it.
func TestTornSlotRecord(t *testing.T) {
	tr := goldenSlotRecord(t)
	n := len(tr.image) / core.BlockSize
	r := core.NewRand(5)
	for _, old := range [][]byte{make([]byte, len(tr.image)), randomBytes(r, len(tr.image))} {
		for landed := range 1 << n {
			img := bytes.Clone(old)
			for b := range n {
				if landed&(1<<b) != 0 {
					copy(img[b*core.BlockSize:], tr.image[b*core.BlockSize:(b+1)*core.BlockSize])
				}
			}
			d, err := DecodeRecord(img, testPlace, manyLeft, nil)
			switch {
			case landed == 1<<n-1 && (err != nil || !sameRecord(d, tr)):
				t.Fatalf("whole record: %v", err)
			case landed != 1<<n-1 && err == nil:
				t.Fatalf("blocks %b landed: decoded %+v", landed, d.Record)
			}
		}
	}
}
