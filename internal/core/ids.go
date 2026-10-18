// Package core holds the types every state machine shares: identifiers, time,
// events and actions, the buffer pool contract, checksums and seeded randomness.
package core

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"

	"github.com/zeebo/xxh3"
)

const (
	// BlockSize is the unit of every disk address, checksum and buffer.
	BlockSize = 4096
	// ChunkSize is the span of one chunk ID within a file or volume.
	ChunkSize = 4 << 20
	// BlocksPerChunk is ChunkSize in blocks.
	BlocksPerChunk = ChunkSize / BlockSize

	slotBits = 24
	seqBits  = 40
	// MaxSlot is the largest inode slot.
	MaxSlot = 1<<slotBits - 1
	// MaxSeq is the largest sequence number within a slot.
	MaxSeq = 1<<seqBits - 1
)

// VolumeID names a block volume or filesystem; it is never reused.
type VolumeID uint32

// PoolID names a pool of placement groups.
type PoolID uint32

// InodeID is a 24-bit slot in its high bits and a 40-bit sequence number in its low bits.
type InodeID uint64

const (
	// BlockVolumeInode is the inode every chunk of a block volume uses.
	BlockVolumeInode InodeID = 0
	// RootInode is the root directory of every filesystem.
	RootInode InodeID = 1
)

// NewInodeID packs a slot and a sequence number.
func NewInodeID(slot uint32, seq uint64) (InodeID, error) {
	if slot > MaxSlot {
		return 0, fmt.Errorf("inode slot %#x exceeds %d bits", slot, slotBits)
	}
	if seq > MaxSeq {
		return 0, fmt.Errorf("inode sequence %#x exceeds %d bits", seq, seqBits)
	}
	return InodeID(uint64(slot)<<seqBits | seq), nil
}

// SlotInode is sequence number 0 of a slot, which holds the slot's own keys.
func SlotInode(slot uint32) (InodeID, error) { return NewInodeID(slot, 0) }

// Slot returns the inode's slot.
func (i InodeID) Slot() uint32 { return uint32(i >> seqBits) }

// Seq returns the inode's sequence number within its slot.
func (i InodeID) Seq() uint64 { return uint64(i) & MaxSeq }

// ChunkID names one chunk; it is computed from the write's position, never looked up.
type ChunkID struct {
	Volume VolumeID
	Inode  InodeID
	Index  uint32
}

// ChunkIDSize is the length of a chunk ID on disk and on the wire.
const ChunkIDSize = 16

// ErrOffsetOverflow reports an offset past the 16 PiB a chunk index can address.
var ErrOffsetOverflow = errors.New("offset past the last chunk")

// ChunkIndexOf returns the chunk index of a byte offset within a file or volume.
func ChunkIndexOf(offset uint64) (uint32, error) {
	idx := offset / ChunkSize
	if idx > 1<<32-1 {
		return 0, fmt.Errorf("%w: %d", ErrOffsetOverflow, offset)
	}
	return uint32(idx), nil
}

// Bytes returns the big-endian packing: volume, inode, index.
func (c ChunkID) Bytes() [ChunkIDSize]byte {
	var b [ChunkIDSize]byte
	binary.BigEndian.PutUint32(b[0:], uint32(c.Volume))
	binary.BigEndian.PutUint64(b[4:], uint64(c.Inode))
	binary.BigEndian.PutUint32(b[12:], c.Index)
	return b
}

// AppendTo appends the packed chunk ID.
func (c ChunkID) AppendTo(b []byte) []byte {
	p := c.Bytes()
	return append(b, p[:]...)
}

// ChunkIDFromBytes unpacks a chunk ID.
func ChunkIDFromBytes(b [ChunkIDSize]byte) ChunkID {
	return ChunkID{
		Volume: VolumeID(binary.BigEndian.Uint32(b[0:])),
		Inode:  InodeID(binary.BigEndian.Uint64(b[4:])),
		Index:  binary.BigEndian.Uint32(b[12:]),
	}
}

// Hash is the placement hash of the chunk: XXH3-64 with seed 0 over its packed bytes.
// It is disk format and never changes.
func (c ChunkID) Hash() uint64 {
	b := c.Bytes()
	return xxh3.HashSeed(b[:], 0)
}

// PGID names a placement group within a pool.
type PGID struct {
	Pool  PoolID
	Index uint32
}

// ErrBadPGCount reports a placement group count that is not a power of two.
var ErrBadPGCount = errors.New("placement group count must be a non-zero power of two")

// CheckPGCount rejects counts that are not a non-zero power of two.
func CheckPGCount(count uint32) error {
	if count == 0 || count&(count-1) != 0 {
		return fmt.Errorf("%w: %d", ErrBadPGCount, count)
	}
	return nil
}

// PGIndexOf maps a chunk hash to one of count placement groups by its top bits, so each
// group is one contiguous hash range and doubling count splits group p into 2p and 2p+1.
// count must pass CheckPGCount.
func PGIndexOf(hash uint64, count uint32) uint32 {
	shift := 64 - bits.TrailingZeros32(count)
	if shift == 64 {
		return 0
	}
	return uint32(hash >> shift)
}
