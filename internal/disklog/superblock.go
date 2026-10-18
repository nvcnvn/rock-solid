package disklog

import (
	"encoding/binary"

	"github.com/nvcnvn/rock-solid/internal/core"
)

const (
	superblockMagic = "RSOLIDSB"
	superblockSize  = 32

	sbVersion       = 8
	sbCRC           = 12
	sbLogID         = 16
	sbSegmentBlocks = 24
	sbSegmentCount  = 28
)

// Superblock is the first and last block of segment 0: the log's identity and geometry.
type Superblock struct {
	LogID uint64
	Geometry
}

// AppendSuperblock appends the superblock encoded as one zero-padded block.
func AppendSuperblock(dst []byte, sb Superblock) ([]byte, error) {
	if err := sb.check(); err != nil {
		return dst, err
	}
	dst = appendZeros(dst, core.BlockSize)
	b := dst[len(dst)-core.BlockSize:]
	copy(b, superblockMagic)
	binary.LittleEndian.PutUint16(b[sbVersion:], FormatVersion)
	binary.LittleEndian.PutUint64(b[sbLogID:], sb.LogID)
	binary.LittleEndian.PutUint32(b[sbSegmentBlocks:], sb.SegmentBlocks)
	binary.LittleEndian.PutUint32(b[sbSegmentCount:], sb.SegmentCount)
	core.PutCRC32C(b[sbCRC:], headerCRC(b, sbCRC))
	return dst, nil
}

// DecodeSuperblock decodes the block at the start of b.
func DecodeSuperblock(b []byte) (Superblock, error) {
	if len(b) < core.BlockSize {
		return Superblock{}, ErrTruncated
	}
	b = b[:core.BlockSize]
	if string(b[:len(superblockMagic)]) != superblockMagic {
		return Superblock{}, ErrBadMagic
	}
	if binary.LittleEndian.Uint16(b[sbVersion:]) != FormatVersion {
		return Superblock{}, ErrBadVersion
	}
	if core.GetCRC32C(b[sbCRC:]) != headerCRC(b, sbCRC) {
		return Superblock{}, ErrBadChecksum
	}
	if !isZero(b[sbVersion+2:sbCRC]) || !isZero(b[superblockSize:]) {
		return Superblock{}, ErrMalformed
	}
	sb := Superblock{
		LogID: binary.LittleEndian.Uint64(b[sbLogID:]),
		Geometry: Geometry{
			SegmentBlocks: binary.LittleEndian.Uint32(b[sbSegmentBlocks:]),
			SegmentCount:  binary.LittleEndian.Uint32(b[sbSegmentCount:]),
		},
	}
	if err := sb.check(); err != nil {
		return Superblock{}, err
	}
	return sb, nil
}
