package disklog

import (
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

func TestGeometryFor(t *testing.T) {
	cases := []struct {
		name     string
		disk     uint64
		segBytes uint64
		count    uint32
	}{
		{"16 TiB", 16 << 40, 64 << 20, 262144},
		{"1 TiB", 1 << 40, 64 << 20, 16384},
		{"256 GiB", 256 << 30, 64 << 20, 4096},
		{"255 GiB", 255 << 30, 32 << 20, 8160},
		{"16 GiB", 16 << 30, 4 << 20, 4096},
		{"1 GiB", 1 << 30, 256 << 10, 4096},
		{"1 GiB + 3 blocks", 1<<30 + 3*core.BlockSize, 256 << 10, 4096},
		{"64 MiB", 64 << 20, 16 << 10, 4096},
		{"1 MiB", 1 << 20, 16 << 10, 64},
		{"64 KiB", 64 << 10, 16 << 10, 4},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			g, err := GeometryFor(c.disk)
			if err != nil {
				t.Fatal(err)
			}
			if g.SegmentBytes() != c.segBytes || g.SegmentCount != c.count {
				t.Fatalf("got %d-byte segments x %d, want %d x %d", g.SegmentBytes(), g.SegmentCount, c.segBytes, c.count)
			}
			if g.check() != nil {
				t.Fatal("geometry fails its own check")
			}
		})
	}
	t.Run("15 blocks", func(t *testing.T) {
		_, err := GeometryFor(15 * core.BlockSize)
		wantErr(t, "15 blocks", err, ErrDiskTooSmall)
	})
	t.Run("too large", func(t *testing.T) {
		_, err := GeometryFor(1 << 63)
		wantErr(t, "8 EiB", err, ErrDiskTooLarge)
	})
}

func TestGeometryAddressing(t *testing.T) {
	g, err := GeometryFor(1 << 30)
	if err != nil {
		t.Fatal(err)
	}
	if got := g.LBA(3, 5); got != 197 {
		t.Fatalf("segment 3 block 5 = LBA %d, want 197", got)
	}
	if g.SuperblockLBA() != 0 || g.SuperblockCopyLBA() != 63 {
		t.Fatalf("superblock at %d, copy at %d", g.SuperblockLBA(), g.SuperblockCopyLBA())
	}
	if g.DiskBlocks() != 1<<30/core.BlockSize {
		t.Fatalf("disk blocks %d", g.DiskBlocks())
	}
}
