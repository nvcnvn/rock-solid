package disklog

import (
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

func sixteenEntryRecord(b *testing.B) testRecord {
	b.Helper()
	r := core.NewRand(16)
	entries := make([]Entry, 0, 16)
	payloads := make([][]byte, 0, 16)
	for i := range 16 {
		chunk := randomChunk(r)
		switch i % 4 {
		case 0, 1:
			entries = append(entries, WriteEntry(1, chunk, uint64(i), uint16(i), 1))
			payloads = append(payloads, randomBytes(r, core.BlockSize))
		case 2:
			entries = append(entries, PunchEntry(1, chunk, uint64(i), 0, 1024))
			payloads = append(payloads, nil)
		case 3:
			entries = append(entries, BlobEntry(BlobRaftEntries, uint32(i), 300))
			payloads = append(payloads, randomBytes(r, 300))
		}
	}
	return encodeRecord(b, Record{Place: testPlace, LSN: 1}, entries, payloads)
}

func BenchmarkAppendRecord16(b *testing.B) {
	tr := sixteenEntryRecord(b)
	data := tr.image[tr.layout.HeaderBlocks*core.BlockSize:]
	dst := make([]byte, 0, len(tr.image))
	b.ReportAllocs()
	b.SetBytes(int64(len(tr.image)))
	for b.Loop() {
		var err error
		dst, err = AppendRecord(dst[:0], tr.rec, tr.entries, data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppendHeader16(b *testing.B) {
	tr := sixteenEntryRecord(b)
	crcs := make([]uint32, tr.layout.DataBlocks)
	for i := range crcs {
		crcs[i] = core.CRC32C(tr.image[(int(tr.layout.HeaderBlocks)+i)*core.BlockSize:][:core.BlockSize])
	}
	dst := make([]byte, 0, core.BlockSize)
	b.ReportAllocs()
	for b.Loop() {
		var err error
		dst, err = AppendHeader(dst[:0], tr.rec, tr.entries, crcs)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeRecord16(b *testing.B) {
	tr := sixteenEntryRecord(b)
	entries := make([]Entry, 0, 16)
	b.ReportAllocs()
	b.SetBytes(int64(len(tr.image)))
	for b.Loop() {
		d, err := DecodeRecord(tr.image, tr.rec.Place, manyLeft, entries[:0])
		if err != nil || len(d.Entries) != 16 {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeHeader16(b *testing.B) {
	tr := sixteenEntryRecord(b)
	entries := make([]Entry, 0, 16)
	b.ReportAllocs()
	for b.Loop() {
		h, err := DecodeHeader(tr.image, tr.rec.Place, manyLeft, entries[:0])
		if err != nil || len(h.Entries) != 16 {
			b.Fatal(err)
		}
	}
}
