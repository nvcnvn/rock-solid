package lsm

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// FormatVersion is the version every run piece carries.
const FormatVersion uint16 = 1

// A piece is the format version u16, one block of a run, then its trailer: on the run's last
// piece, the root, references to the earlier pieces in order; then the run's sequence number u64
// and the reference count u16, all little-endian. Reading the trailer from the end lets a piece be
// built at its final size without knowing in advance whether it is the root.
const (
	pieceHeader   = 2
	pieceOverhead = pieceHeader + 8 + 2
)

func rootOverhead(earlier int) int { return pieceOverhead + disklog.BlobRefSize*earlier }

// splitPiece separates a piece into its block, its run's sequence number and the references it
// lists, appended to refs.
func splitPiece(b []byte, refs []disklog.BlobRef) ([]byte, uint64, []disklog.BlobRef, error) {
	if len(b) < pieceOverhead {
		return nil, 0, refs, ErrMalformed
	}
	if binary.LittleEndian.Uint16(b) != FormatVersion {
		return nil, 0, refs, ErrBadVersion
	}
	n := int(binary.LittleEndian.Uint16(b[len(b)-2:]))
	seq := binary.LittleEndian.Uint64(b[len(b)-10:])
	end := len(b) - rootOverhead(n) + pieceHeader
	if end < pieceHeader {
		return nil, 0, refs, ErrMalformed
	}
	for i := range n {
		ref := disklog.DecodeBlobRef(b[end+i*disklog.BlobRefSize:])
		if ref.Segment == 0 {
			return nil, 0, refs, ErrMalformed
		}
		refs = append(refs, ref)
	}
	return b[pieceHeader:end], seq, refs, nil
}

// RootRefs returns the references a run's root piece lists to its earlier pieces, appended to refs;
// a restore reads those pieces before it loads the run.
func RootRefs(root []byte, refs []disklog.BlobRef) ([]disklog.BlobRef, error) {
	_, _, refs, err := splitPiece(root, refs)
	return refs, err
}

// run is an immutable sorted run held in memory as the blocks of its pieces, empty blocks left
// out. Each block's first key, stored whole, is its fence pointer.
type run struct {
	seq    uint64
	blocks [][]byte
	bytes  int
	pieces []disklog.BlobRef
}

func (r *run) durable() bool         { return len(r.pieces) > 0 }
func (r *run) root() disklog.BlobRef { return r.pieces[len(r.pieces)-1] }

func (r *run) addBlock(b []byte) {
	r.blocks = append(r.blocks, b)
	r.bytes += len(b)
}

func firstKey(b []byte) []byte {
	_, n := binary.Uvarint(b)
	unshared, m := binary.Uvarint(b[n:])
	n += m
	_, m = binary.Uvarint(b[n:])
	n += m
	return b[n : n+int(unshared)]
}

// blockFor is the last block whose first key is <= k, or < k when strict, or -1.
func (r *run) blockFor(k []byte, strict bool) int {
	lo, hi := 0, len(r.blocks)
	for lo < hi {
		m := int(uint(lo+hi) >> 1)
		c := bytes.Compare(firstKey(r.blocks[m]), k)
		if c < 0 || (c == 0 && !strict) {
			lo = m + 1
		} else {
			hi = m
		}
	}
	return lo - 1
}

func (r *run) get(it *blockIter, k []byte) ([]byte, bool) {
	b := r.blockFor(k, false)
	if b < 0 {
		return nil, false
	}
	it.init(r.blocks[b])
	if !it.seekGE(k) || !bytes.Equal(it.key, k) {
		return nil, false
	}
	return it.val, true
}

type runIter struct {
	r  *run
	b  int
	it blockIter
}

func (ri *runIter) init(r *run) { ri.r, ri.b, ri.it.off, ri.it.restarts = r, -1, 0, 0 }

func (ri *runIter) valid() bool { return ri.b >= 0 && ri.b < len(ri.r.blocks) && ri.it.valid() }

func (ri *runIter) seekGE(k []byte) bool {
	ri.b = max(ri.r.blockFor(k, false), 0)
	if ri.b >= len(ri.r.blocks) {
		return false
	}
	ri.it.init(ri.r.blocks[ri.b])
	if ri.it.seekGE(k) {
		return true
	}
	return ri.nextBlock()
}

func (ri *runIter) next() bool {
	if !ri.valid() {
		return false
	}
	if ri.it.advance() {
		return true
	}
	return ri.nextBlock()
}

func (ri *runIter) nextBlock() bool {
	for ri.b++; ri.b < len(ri.r.blocks); ri.b++ {
		ri.it.init(ri.r.blocks[ri.b])
		if ri.it.first() {
			return true
		}
	}
	return false
}

func (ri *runIter) floor(k []byte, strict bool) bool {
	ri.b = ri.r.blockFor(k, strict)
	if ri.b < 0 {
		return false
	}
	ri.it.init(ri.r.blocks[ri.b])
	return ri.it.floor(k, strict)
}

// fillBlock adds entries from src to w while the finished block stays within limit bytes,
// leaving out each removal no run in older holds the key of, since it has nothing to hide. It
// reports whether src ran out.
func fillBlock(w *blockWriter, src *iterState, limit int, older []*run, it *blockIter) bool {
	for ; src.valid(); src.next() {
		v := src.value()
		if v[0] == kindDel && !holds(older, it, src.key()) {
			continue
		}
		if w.sizeWith(src.key(), len(v)) > limit {
			return false
		}
		w.add(src.key(), v)
	}
	return true
}

func holds(runs []*run, it *blockIter, k []byte) bool {
	for _, r := range runs {
		if _, ok := r.get(it, k); ok {
			return true
		}
	}
	return false
}

// writePiece writes into dst a piece of block, which already sits at dst[pieceHeader:] when
// inPlace. It is the root if it is the last block and its trailer fits within max; otherwise the
// run gets an empty root after it.
func writePiece(dst, block []byte, inPlace, last bool, seq uint64, earlier []disklog.BlobRef, max int) ([]byte, bool, error) {
	if len(earlier) >= math.MaxUint16 {
		return nil, false, ErrTooLarge
	}
	root := last && len(block)+rootOverhead(len(earlier)) <= max
	if last && !root && len(block) <= emptyBlockSize {
		return nil, false, ErrTooLarge
	}
	dst = binary.LittleEndian.AppendUint16(dst[:0], FormatVersion)
	if inPlace {
		dst = dst[:pieceHeader+len(block)]
	} else {
		dst = append(dst, block...)
	}
	if root {
		for _, r := range earlier {
			dst = disklog.AppendBlobRef(dst, r)
		}
	} else {
		earlier = nil
	}
	dst = binary.LittleEndian.AppendUint64(dst, seq)
	return binary.LittleEndian.AppendUint16(dst, uint16(len(earlier))), root, nil
}
