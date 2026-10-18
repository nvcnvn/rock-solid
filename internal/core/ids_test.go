package core

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
)

func TestInodeIDPacking(t *testing.T) {
	if BlockVolumeInode.Slot() != 0 || BlockVolumeInode.Seq() != 0 {
		t.Fatal("block volume inode is not slot 0, seq 0")
	}
	if RootInode.Slot() != 0 || RootInode.Seq() != 1 {
		t.Fatal("root inode is not slot 0, seq 1")
	}
	slot, err := SlotInode(0x123456)
	if err != nil || slot != 0x1234560000000000 {
		t.Fatalf("slot inode = %#x, %v", uint64(slot), err)
	}
	id, err := NewInodeID(0xABCDEF, 0x9876543210)
	if err != nil {
		t.Fatal(err)
	}
	if id.Slot() != 0xABCDEF || id.Seq() != 0x9876543210 {
		t.Fatalf("round trip gave slot %#x seq %#x", id.Slot(), id.Seq())
	}
	c := ChunkID{Inode: id}
	b := c.Bytes()
	if want := []byte{0xAB, 0xCD, 0xEF, 0x98, 0x76, 0x54, 0x32, 0x10}; string(b[4:12]) != string(want) {
		t.Fatalf("inode bytes = % x, want % x", b[4:12], want)
	}
	if _, err := NewInodeID(MaxSlot+1, 0); err == nil {
		t.Fatal("slot over 24 bits accepted")
	}
	if _, err := NewInodeID(0, MaxSeq+1); err == nil {
		t.Fatal("sequence over 40 bits accepted")
	}
}

func TestChunkIDPacking(t *testing.T) {
	c := ChunkID{Volume: 0x01020304, Inode: 0x05060708090A0B0C, Index: 0x0D0E0F10}
	b := c.Bytes()
	for i := range b {
		if b[i] != byte(i+1) {
			t.Fatalf("byte %d = %#x", i, b[i])
		}
	}
	if ChunkIDFromBytes(b) != c {
		t.Fatal("round trip through bytes differs")
	}
	if got := c.AppendTo(nil); string(got) != string(b[:]) {
		t.Fatal("AppendTo differs from Bytes")
	}
}

func TestChunkIndexOf(t *testing.T) {
	cases := []struct {
		offset uint64
		index  uint32
	}{
		{0, 0}, {ChunkSize - 1, 0}, {ChunkSize, 1}, {(1<<32)*ChunkSize - 1, 1<<32 - 1},
	}
	for _, c := range cases {
		got, err := ChunkIndexOf(c.offset)
		if err != nil || got != c.index {
			t.Fatalf("ChunkIndexOf(%d) = %d, %v; want %d", c.offset, got, err, c.index)
		}
	}
	_, err := ChunkIndexOf((1 << 32) * ChunkSize)
	if !errors.Is(err, ErrOffsetOverflow) {
		t.Fatalf("offset past 16 PiB gave %v", err)
	}
}

func TestPGCount(t *testing.T) {
	for _, bad := range []uint32{0, 3, 6, 1<<31 + 1} {
		if err := CheckPGCount(bad); err == nil {
			t.Fatalf("count %d accepted", bad)
		}
	}
	for _, h := range []uint64{0, 1, 1<<64 - 1, 0xd0a66a65c7528968} {
		if PGIndexOf(h, 1) != 0 {
			t.Fatalf("hash %#x with one PG is not PG 0", h)
		}
	}
	if PGIndexOf(1<<64-1, 64) != 63 || PGIndexOf(1<<63, 2) != 1 {
		t.Fatal("PG index is not the top bits")
	}
}

func TestPGSplitKeepsRangesContiguous(t *testing.T) {
	r := NewRand(7)
	for range 64 {
		c := ChunkID{Volume: VolumeID(r.Uint32()), Inode: InodeID(r.Uint64()), Index: r.Uint32()}
		h := c.Hash()
		for count := uint32(1); count < 1<<11; count *= 2 {
			p, q := PGIndexOf(h, count), PGIndexOf(h, count*2)
			if q != 2*p && q != 2*p+1 {
				t.Fatalf("chunk %+v: PG %d at %d groups became %d at %d", c, p, count, q, count*2)
			}
		}
	}
}

// chunk_id_golden.txt is frozen disk format; the test reads it, never the reverse.
func TestChunkIDGoldenVectors(t *testing.T) {
	f, err := os.Open("testdata/chunk_id_golden.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	rows := 0
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 8 {
			t.Fatalf("bad row: %q", line)
		}
		volume := parseHex(t, fields[0])
		slot := parseHex(t, fields[1])
		seq := parseHex(t, fields[2])
		index := parseHex(t, fields[3])
		inode, err := NewInodeID(uint32(slot), seq)
		if err != nil {
			t.Fatal(err)
		}
		c := ChunkID{Volume: VolumeID(volume), Inode: inode, Index: uint32(index)}
		if got := fmt.Sprintf("%x", c.Bytes()); got != fields[4] {
			t.Errorf("%+v packs to %s, want %s", c, got, fields[4])
		}
		h := c.Hash()
		if got := fmt.Sprintf("%#016x", h); got != fields[5] {
			t.Errorf("%+v hashes to %s, want %s", c, got, fields[5])
		}
		if got := uint64(PGIndexOf(h, 64)); got != parseDec(t, fields[6]) {
			t.Errorf("%+v in 64 PGs = %d, want %s", c, got, fields[6])
		}
		if got := uint64(PGIndexOf(h, 2048)); got != parseDec(t, fields[7]) {
			t.Errorf("%+v in 2048 PGs = %d, want %s", c, got, fields[7])
		}
		rows++
	}
	if rows != 10 {
		t.Fatalf("read %d rows, want 10", rows)
	}
}

func parseHex(t *testing.T, s string) uint64 {
	t.Helper()
	v, err := strconv.ParseUint(strings.TrimPrefix(s, "0x"), 16, 64)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func parseDec(t *testing.T, s string) uint64 {
	t.Helper()
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func BenchmarkChunkHash(b *testing.B) {
	b.ReportAllocs()
	c := ChunkID{Volume: 1, Inode: RootInode, Index: 3}
	var sink uint64
	for b.Loop() {
		sink += c.Hash()
	}
	_ = sink
}
