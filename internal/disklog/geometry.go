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

// Geometry is how a disk is cut into segments: segment 0 holds the superblock and its copy, the rest hold records.
type Geometry struct {
	SegmentBlocks uint32
	SegmentCount  uint32
}

// GeometryFor cuts a disk into segments of min(MaxSegmentBytes, diskBytes/4096) rounded down to a power of two.
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

// SlotsPerSegment is how many chunk slots a slot segment holds: as many whole chunks as fit, none in segments smaller than a chunk.
func (g Geometry) SlotsPerSegment() uint16 { return uint16(g.SegmentBlocks / core.BlocksPerChunk) }

// SlotLBA is the address of the first block of chunk slot slot of segment s; a slot segment has no header.
func (g Geometry) SlotLBA(s uint32, slot uint16) core.LBA {
	return g.LBA(s, uint32(slot)*core.BlocksPerChunk)
}

// SlotOf is the segment and slot whose first block is lba, if lba starts a slot.
func (g Geometry) SlotOf(lba core.LBA) (uint32, uint16, bool) {
	s, b := uint64(lba)/uint64(g.SegmentBlocks), uint64(lba)%uint64(g.SegmentBlocks)
	if s == 0 || s >= uint64(g.SegmentCount) || b%core.BlocksPerChunk != 0 {
		return 0, 0, false
	}
	return uint32(s), uint16(b / core.BlocksPerChunk), true
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
