package disklog

import (
	"encoding/binary"

	"github.com/nvcnvn/rock-solid/internal/core"
)

const (
	segmentMagic      = "RSOLIDSG"
	segmentHeaderSize = 52

	sgVersion         = 8
	sgStream          = 10
	sgCRC             = 12
	sgLogID           = 16
	sgSeq             = 24
	sgManifestLSN     = 32
	sgIndex           = 40
	sgManifestSegment = 44
	sgManifestBlock   = 48
)

// Stream tags which of the two streams a segment belongs to.
type Stream uint8

// Streams.
const (
	Hot  Stream = 0
	Cold Stream = 1
)

// ManifestPos is where the newest manifest piece sits; Segment 0 means there is none.
type ManifestPos struct {
	Segment uint32
	Block   uint32
	LSN     uint64
}

// IsNone reports whether no manifest is named.
func (p ManifestPos) IsNone() bool { return p.Segment == 0 }

// SegmentHeader is the first block of every segment but segment 0.
type SegmentHeader struct {
	LogID    uint64
	Seq      uint64
	Index    uint32
	Stream   Stream
	Manifest ManifestPos
}

func (h SegmentHeader) check() error {
	if h.Stream > Cold || h.Index == 0 {
		return ErrMalformed
	}
	if h.Manifest.IsNone() {
		if h.Manifest.Block != 0 || h.Manifest.LSN != 0 {
			return ErrMalformed
		}
	} else if h.Manifest.Block == 0 {
		return ErrMalformed
	}
	return nil
}

// AppendSegmentHeader appends the header encoded as one zero-padded block.
func AppendSegmentHeader(dst []byte, h SegmentHeader) ([]byte, error) {
	if err := h.check(); err != nil {
		return dst, err
	}
	dst = appendZeros(dst, core.BlockSize)
	b := dst[len(dst)-core.BlockSize:]
	copy(b, segmentMagic)
	binary.LittleEndian.PutUint16(b[sgVersion:], FormatVersion)
	b[sgStream] = byte(h.Stream)
	binary.LittleEndian.PutUint64(b[sgLogID:], h.LogID)
	binary.LittleEndian.PutUint64(b[sgSeq:], h.Seq)
	binary.LittleEndian.PutUint64(b[sgManifestLSN:], h.Manifest.LSN)
	binary.LittleEndian.PutUint32(b[sgIndex:], h.Index)
	binary.LittleEndian.PutUint32(b[sgManifestSegment:], h.Manifest.Segment)
	binary.LittleEndian.PutUint32(b[sgManifestBlock:], h.Manifest.Block)
	core.PutCRC32C(b[sgCRC:], headerCRC(b, sgCRC))
	return dst, nil
}

// DecodeSegmentHeader decodes the block at the start of b.
func DecodeSegmentHeader(b []byte) (SegmentHeader, error) {
	if len(b) < core.BlockSize {
		return SegmentHeader{}, ErrTruncated
	}
	b = b[:core.BlockSize]
	if string(b[:len(segmentMagic)]) != segmentMagic {
		return SegmentHeader{}, ErrBadMagic
	}
	if binary.LittleEndian.Uint16(b[sgVersion:]) != FormatVersion {
		return SegmentHeader{}, ErrBadVersion
	}
	if core.GetCRC32C(b[sgCRC:]) != headerCRC(b, sgCRC) {
		return SegmentHeader{}, ErrBadChecksum
	}
	if b[sgStream+1] != 0 || !isZero(b[segmentHeaderSize:]) {
		return SegmentHeader{}, ErrMalformed
	}
	h := SegmentHeader{
		LogID:  binary.LittleEndian.Uint64(b[sgLogID:]),
		Seq:    binary.LittleEndian.Uint64(b[sgSeq:]),
		Index:  binary.LittleEndian.Uint32(b[sgIndex:]),
		Stream: Stream(b[sgStream]),
		Manifest: ManifestPos{
			Segment: binary.LittleEndian.Uint32(b[sgManifestSegment:]),
			Block:   binary.LittleEndian.Uint32(b[sgManifestBlock:]),
			LSN:     binary.LittleEndian.Uint64(b[sgManifestLSN:]),
		},
	}
	if err := h.check(); err != nil {
		return SegmentHeader{}, err
	}
	return h, nil
}
