// Package disklog is the on-disk log of one disk: its geometry, superblock, segment headers,
// record and manifest codecs, and group commit.
package disklog

import (
	"errors"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// FormatVersion is the version every header of this format carries.
const FormatVersion uint16 = 1

var (
	// ErrDiskTooSmall reports a disk that cannot hold four segments.
	ErrDiskTooSmall = errors.New("disk too small for four segments")
	// ErrDiskTooLarge reports a disk with more segments than a superblock can count.
	ErrDiskTooLarge = errors.New("disk has more segments than the format can count")
	// ErrBadMagic reports a block that does not start with the header's magic.
	ErrBadMagic = errors.New("bad magic")
	// ErrBadVersion reports a header of a format version this code does not read.
	ErrBadVersion = errors.New("unsupported format version")
	// ErrBadChecksum reports a header or data block whose checksum does not match.
	ErrBadChecksum = errors.New("checksum mismatch")
	// ErrMalformed reports a header whose checksum matches but whose contents are inconsistent.
	ErrMalformed = errors.New("malformed header")
	// ErrBadExtent reports a block range that is empty or runs past the end of its chunk.
	ErrBadExtent = errors.New("block range outside the chunk")
	// ErrBadLengthAtEnd reports a record longer than the blocks left in its segment.
	ErrBadLengthAtEnd = errors.New("record longer than the blocks left in its segment")
	// ErrTruncated reports an image shorter than the header or record it holds.
	ErrTruncated = errors.New("truncated record")
	// ErrMismatch reports a record that names another log, segment incarnation or offset.
	ErrMismatch = errors.New("record names another place")
	// ErrTooLarge reports a record the header fields cannot describe.
	ErrTooLarge = errors.New("record too large to encode")
)

var zeroCRCField [core.CRC32CSize]byte

func headerCRC(b []byte, crcAt int) uint32 {
	crc := core.CRC32C(b[:crcAt])
	crc = core.CRC32CUpdate(crc, zeroCRCField[:])
	return core.CRC32CUpdate(crc, b[crcAt+core.CRC32CSize:])
}

func isZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

func appendZeros(dst []byte, n int) []byte { return append(dst, make([]byte, n)...) }
