package disklog

import (
	"math"
	"math/bits"

	"github.com/nvcnvn/rock-solid/internal/core"
)

const (
	// MaxSegmentBytes is the largest segment: disks of 256 GiB and up use it.
	MaxSegmentBytes = 64 << 20
	// MinSegmentBlocks is the smallest segment.
	MinSegmentBlocks = 4
	// MinSegmentCount is the fewest segments a disk may be cut into.
	MinSegmentCount = 4

	segmentDivisor = 4096
)

// Geometry is how a disk is cut into segments. Segment 0 holds the superblock in its
// first and last block; segments 1 and up hold records.
type Geometry struct {
	SegmentBlocks uint32
	SegmentCount  uint32
}

// GeometryFor cuts a disk of diskBytes into segments of min(MaxSegmentBytes, diskBytes/4096)
// rounded down to a power of two and no smaller than MinSegmentBlocks.
func GeometryFor(diskBytes uint64) (Geometry, error) {
	segBytes := min(uint64(MaxSegmentBytes), diskBytes/segmentDivisor)
	segBytes = max(floorPow2(segBytes), MinSegmentBlocks*core.BlockSize)
	count := diskBytes / segBytes
	if count < MinSegmentCount {
		return Geometry{}, ErrDiskTooSmall
	}
	if count > math.MaxUint32 {
		return Geometry{}, ErrDiskTooLarge
	}
	return Geometry{SegmentBlocks: uint32(segBytes / core.BlockSize), SegmentCount: uint32(count)}, nil
}

func floorPow2(x uint64) uint64 {
	if x == 0 {
		return 0
	}
	return 1 << (bits.Len64(x) - 1)
}

// LBA is the address of block b of segment s.
func (g Geometry) LBA(s, b uint32) core.LBA {
	return core.LBA(s)*core.LBA(g.SegmentBlocks) + core.LBA(b)
}

// SuperblockLBA is the address of the superblock: the first block of segment 0.
func (g Geometry) SuperblockLBA() core.LBA { return 0 }

// SuperblockCopyLBA is the address of the superblock's copy: the last block of segment 0.
func (g Geometry) SuperblockCopyLBA() core.LBA { return core.LBA(g.SegmentBlocks) - 1 }

// SegmentBytes is the size of one segment.
func (g Geometry) SegmentBytes() uint64 { return uint64(g.SegmentBlocks) * core.BlockSize }

// DiskBlocks is the number of blocks the segments cover.
func (g Geometry) DiskBlocks() uint64 { return uint64(g.SegmentBlocks) * uint64(g.SegmentCount) }

func (g Geometry) check() error {
	blocks := uint64(g.SegmentBlocks)
	if blocks < MinSegmentBlocks || blocks*core.BlockSize > MaxSegmentBytes || blocks&(blocks-1) != 0 {
		return ErrMalformed
	}
	if g.SegmentCount < MinSegmentCount {
		return ErrMalformed
	}
	return nil
}
