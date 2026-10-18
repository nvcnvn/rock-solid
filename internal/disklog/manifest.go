package disklog

import (
	"encoding/binary"
	"math"
)

const (
	// BlobRefSize is the encoded length of a BlobRef.
	BlobRefSize = 18

	manifestFixedSize = 2 + 8 + 4
	manifestIndexSize = 4 + 8 + 4
	pieceListSize     = 2 + 2
)

// BlobRef names a blob entry of a durable record by the record's segment, block offset and LSN and the entry's index.
type BlobRef struct {
	Segment uint32
	Block   uint32
	LSN     uint64
	Entry   uint16
}

// ManifestIndex is an index's runs, each named by its last piece, and the LSN after which records replay into it.
type ManifestIndex struct {
	Owner     uint32
	ReplayLSN uint64
	Runs      []BlobRef
}

// Manifest is a checkpoint: each index's runs and replay point, all at or before BaseLSN, the durable LSN when the checkpoint began.
type Manifest struct {
	BaseLSN uint64
	Indexes []ManifestIndex
}

// AppendManifest appends the encoded manifest.
func AppendManifest(dst []byte, m *Manifest) []byte {
	dst = binary.LittleEndian.AppendUint16(dst, FormatVersion)
	dst = binary.LittleEndian.AppendUint64(dst, m.BaseLSN)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(m.Indexes)))
	for i := range m.Indexes {
		ix := &m.Indexes[i]
		dst = binary.LittleEndian.AppendUint32(dst, ix.Owner)
		dst = binary.LittleEndian.AppendUint64(dst, ix.ReplayLSN)
		dst = binary.LittleEndian.AppendUint32(dst, uint32(len(ix.Runs)))
		for _, r := range ix.Runs {
			dst = appendBlobRef(dst, r)
		}
	}
	return dst
}

// ManifestSize is the encoded length of m.
func ManifestSize(m *Manifest) int {
	n := manifestFixedSize
	for i := range m.Indexes {
		n += manifestIndexSize + BlobRefSize*len(m.Indexes[i].Runs)
	}
	return n
}

// DecodeManifest decodes a manifest, whose records' checksums already vouch for its bytes, so any inconsistency is ErrMalformed.
func DecodeManifest(b []byte) (Manifest, error) {
	r := reader{b: b}
	if v := r.u16(); v != FormatVersion && !r.bad {
		return Manifest{}, ErrBadVersion
	}
	m := Manifest{BaseLSN: r.u64()}
	count := r.u32()
	if r.bad || uint64(count)*manifestIndexSize > uint64(r.left()) {
		return Manifest{}, ErrMalformed
	}
	m.Indexes = make([]ManifestIndex, count)
	owners := make(map[uint32]bool, count)
	for i := range m.Indexes {
		ix := &m.Indexes[i]
		ix.Owner = r.u32()
		ix.ReplayLSN = r.u64()
		if owners[ix.Owner] || ix.ReplayLSN > m.BaseLSN {
			return Manifest{}, ErrMalformed
		}
		owners[ix.Owner] = true
		runs := r.u32()
		if r.bad || uint64(runs)*BlobRefSize > uint64(r.left()) {
			return Manifest{}, ErrMalformed
		}
		if runs > 0 {
			ix.Runs = make([]BlobRef, runs)
		}
		for j := range ix.Runs {
			if ix.Runs[j] = r.blobRef(); ix.Runs[j].Segment == 0 {
				return Manifest{}, ErrMalformed
			}
		}
	}
	if r.bad || r.left() != 0 {
		return Manifest{}, ErrMalformed
	}
	return m, nil
}

// AppendManifestPiece appends a manifest's last piece: a version, references to the earlier pieces in order, then the manifest's remaining bytes.
func AppendManifestPiece(dst []byte, earlier []BlobRef, rest []byte) ([]byte, error) {
	if len(earlier) > math.MaxUint16 {
		return dst, ErrTooLarge
	}
	dst = binary.LittleEndian.AppendUint16(dst, FormatVersion)
	dst = binary.LittleEndian.AppendUint16(dst, uint16(len(earlier)))
	for _, r := range earlier {
		dst = appendBlobRef(dst, r)
	}
	return append(dst, rest...), nil
}

// ManifestPieceSize is the length of a last piece with earlier references and rest bytes.
func ManifestPieceSize(earlier, rest int) int { return pieceListSize + BlobRefSize*earlier + rest }

// DecodeManifestPiece splits a manifest's last piece into references to the earlier pieces, appended to earlier, and the remaining bytes.
func DecodeManifestPiece(b []byte, earlier []BlobRef) ([]BlobRef, []byte, error) {
	r := reader{b: b}
	if v := r.u16(); v != FormatVersion && !r.bad {
		return earlier, nil, ErrBadVersion
	}
	n := int(r.u16())
	if r.bad || n*BlobRefSize > r.left() {
		return earlier, nil, ErrMalformed
	}
	for range n {
		ref := r.blobRef()
		if ref.Segment == 0 {
			return earlier, nil, ErrMalformed
		}
		earlier = append(earlier, ref)
	}
	return earlier, r.b, nil
}

func appendBlobRef(dst []byte, r BlobRef) []byte {
	dst = binary.LittleEndian.AppendUint32(dst, r.Segment)
	dst = binary.LittleEndian.AppendUint32(dst, r.Block)
	dst = binary.LittleEndian.AppendUint64(dst, r.LSN)
	return binary.LittleEndian.AppendUint16(dst, r.Entry)
}

type reader struct {
	b   []byte
	bad bool
}

var zeroField [8]byte

func (r *reader) take(n int) []byte {
	if r.bad || len(r.b) < n {
		r.bad = true
		return zeroField[:n]
	}
	p := r.b[:n]
	r.b = r.b[n:]
	return p
}

func (r *reader) left() int   { return len(r.b) }
func (r *reader) u16() uint16 { return binary.LittleEndian.Uint16(r.take(2)) }
func (r *reader) u32() uint32 { return binary.LittleEndian.Uint32(r.take(4)) }
func (r *reader) u64() uint64 { return binary.LittleEndian.Uint64(r.take(8)) }
func (r *reader) blobRef() BlobRef {
	return BlobRef{Segment: r.u32(), Block: r.u32(), LSN: r.u64(), Entry: r.u16()}
}
