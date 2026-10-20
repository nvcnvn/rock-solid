package core

import (
	"bytes"
	"testing"
)

// Vectors from RFC 3720, appendix B.4.
func TestCRC32CVectors(t *testing.T) {
	incrementing := make([]byte, 32)
	decrementing := make([]byte, 32)
	for i := range incrementing {
		incrementing[i] = byte(i)
		decrementing[i] = byte(31 - i)
	}
	readPDU := []byte{
		0x01, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x18,
		0x28, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}
	cases := []struct {
		name string
		data []byte
		want uint32
	}{
		{"123456789", []byte("123456789"), 0xE3069283},
		{"32 zeros", make([]byte, 32), 0x8A9136AA},
		{"32 ones", bytes.Repeat([]byte{0xFF}, 32), 0x62A8AB43},
		{"incrementing", incrementing, 0x46DD794E},
		{"decrementing", decrementing, 0x113FDB5C},
		{"iSCSI READ(10)", readPDU, 0xD9963A56},
	}
	for _, c := range cases {
		if got := CRC32C(c.data); got != c.want {
			t.Errorf("%s: %#08x, want %#08x", c.name, got, c.want)
		}
	}
	var stored [4]byte
	PutCRC32C(stored[:], 0x8A9136AA)
	if stored != [4]byte{0xAA, 0x36, 0x91, 0x8A} {
		t.Fatalf("stored % x, want little-endian", stored)
	}
	if GetCRC32C(stored[:]) != CRC32C(make([]byte, 32)) {
		t.Fatal("the stored checksum does not read back as the data's")
	}
	var wrong [4]byte
	PutCRC32C(wrong[:], 0x8A9136AA+1)
	if GetCRC32C(wrong[:]) == CRC32C(make([]byte, 32)) {
		t.Fatal("a stored value+1 reads back as the data's checksum")
	}
	if got := AppendCRC32C(nil, make([]byte, 32)); !bytes.Equal(got, stored[:]) {
		t.Fatalf("AppendCRC32C = % x", got)
	}
}

func TestCRC32CIncremental(t *testing.T) {
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i * 31)
	}
	want := CRC32C(data)
	for _, split := range []int{0, 1, 7, 16, 63, 64, 511, 2048, 4095, 4096} {
		if got := CRC32CUpdate(CRC32C(data[:split]), data[split:]); got != want {
			t.Errorf("split %d: %#08x, want %#08x", split, got, want)
		}
	}
}

func BenchmarkCRC32CBlock(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(BlockSize)
	block := make([]byte, BlockSize)
	var sink uint32
	for b.Loop() {
		sink += CRC32C(block)
	}
	_ = sink
}
