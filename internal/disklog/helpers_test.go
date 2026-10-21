package disklog

import (
	"bytes"
	"errors"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

const (
	testLogID = 0x0123456789abcdef
	manyLeft  = 1 << 20
)

type testRecord struct {
	rec      Record
	entries  []Entry
	payloads [][]byte
	layout   Layout
	image    []byte
}

func fixCRC(b []byte, at int) { core.PutCRC32C(b[at:], headerCRC(b, at)) }

func buildData(t testing.TB, entries []Entry, payloads [][]byte) []byte {
	t.Helper()
	lay, err := LayoutOf(entries)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, uint64(lay.DataBlocks)*core.BlockSize)
	for i := range entries {
		if payloads[i] != nil {
			copy(data[PayloadOffset(entries, i):], payloads[i])
		}
	}
	return data
}

// encodeRecord encodes a record whose payloads are its entries', a slot write's being the slot's
// data, which the record's image leaves out; payloads holds nil for it in the result.
func encodeRecord(t testing.TB, rec Record, entries []Entry, payloads [][]byte) testRecord {
	t.Helper()
	lay, err := LayoutOf(entries)
	if err != nil {
		t.Fatal(err)
	}
	payloads = slices.Clone(payloads)
	var slots [][]byte
	for i := range entries {
		if entries[i].Kind == EntrySlot {
			slots, payloads[i] = append(slots, payloads[i]), nil
		}
	}
	data := buildData(t, entries, payloads)
	var crcs []uint32
	for _, b := range append([][]byte{data}, slots...) {
		for i := 0; i < len(b); i += core.BlockSize {
			crcs = append(crcs, core.CRC32C(b[i:i+core.BlockSize]))
		}
	}
	img, err := AppendHeader(nil, rec, entries, crcs)
	if err != nil {
		t.Fatal(err)
	}
	return testRecord{rec: rec, entries: entries, payloads: payloads, layout: lay, image: append(img, data...)}
}

func randomChunk(r *core.Rand) core.ChunkID {
	return core.ChunkID{Volume: core.VolumeID(r.Uint32()), Inode: core.InodeID(r.Uint64()), Index: r.Uint32()}
}

func randomBytes(r *core.Rand, n int) []byte {
	b := make([]byte, n)
	r.Fill(b)
	return b
}

// randomRecord draws 1 to 5 entries: writes up to 3 blocks, punches up to 512, blobs under
// 8 KiB, freezes; an entry that would push the data past maxDataBlocks becomes a punch.
func randomRecord(t testing.TB, r *core.Rand, rec Record, maxDataBlocks uint32) testRecord {
	t.Helper()
	n := int(r.Range(1, 5))
	entries := make([]Entry, 0, n)
	payloads := make([][]byte, 0, n)
	for range n {
		pool := core.PoolID(r.Below(16))
		chunk := randomChunk(r)
		version := r.Uint64()
		var e Entry
		var payload []byte
		switch r.Below(4) {
		case 0:
			count := uint16(r.Range(1, 3))
			e = WriteEntry(pool, chunk, version, uint16(r.Below(uint64(core.BlocksPerChunk-count)+1)), count)
			payload = randomBytes(r, int(count)*core.BlockSize)
		case 1:
			count := uint16(r.Range(1, 512))
			e = PunchEntry(pool, chunk, version, uint16(r.Below(uint64(core.BlocksPerChunk-count)+1)), count)
		case 2:
			e = FreezeEntry(pool, chunk, version, r.Uint64())
		case 3:
			length := int(r.Below(8 << 10))
			e = BlobEntry(BlobKind(r.Below(6)), r.Uint32(), uint64(length))
			payload = randomBytes(r, length)
		}
		lay, err := LayoutOf(append(slices.Clone(entries), e))
		if err != nil {
			t.Fatal(err)
		}
		if lay.DataBlocks > maxDataBlocks {
			e = PunchEntry(pool, chunk, version, 0, 1)
			payload = nil
		}
		entries = append(entries, e)
		payloads = append(payloads, payload)
	}
	return encodeRecord(t, rec, entries, payloads)
}

func sameRecord(d DecodedRecord, want testRecord) bool {
	if d.Record != want.rec || d.Layout != want.layout || !slices.Equal(d.Entries, want.entries) {
		return false
	}
	for i := range want.entries {
		if !bytes.Equal(d.Payload(i), want.payloads[i]) {
			return false
		}
	}
	return true
}

func decodeExact(t testing.TB, want testRecord) DecodedRecord {
	t.Helper()
	d, err := DecodeRecord(want.image, want.rec.Place, manyLeft, nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !sameRecord(d, want) {
		t.Fatalf("decoded %+v, want %+v", d.RecordHeader, want.rec)
	}
	return d
}

func wantErr(t testing.TB, name string, err, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("%s: got %v, want %v", name, err, want)
	}
}
