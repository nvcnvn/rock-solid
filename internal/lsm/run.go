package lsm

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// FormatVersion is the version every run piece carries.
const FormatVersion uint16 = 1

// A piece is the format version u16, one block of a run, then its trailer: on the run's last
// piece, the root, references to the earlier pieces in order; then the block's kind u8, the run's
// sequence number u64 and the reference count u16, all little-endian. Reading the trailer from the
// end lets a piece be built at its final size without knowing in advance whether it is the root.
// A run's entry blocks come first, then its fragment blocks: `start → end` sorted by start, never
// overlapping, then its filter cut into blocks of raw bytes.
const (
	pieceHeader   = 2
	pieceOverhead = pieceHeader + 1 + 8 + 2
)

const (
	entryBlock  byte = 0
	fragBlock   byte = 1
	filterBlock byte = 2
)

func rootOverhead(earlier int) int { return pieceOverhead + disklog.BlobRefSize*earlier }

// splitPiece separates a piece into its block, the block's kind, its run's sequence number and
// the references it lists, appended to refs.
func splitPiece(b []byte, refs []disklog.BlobRef) ([]byte, byte, uint64, []disklog.BlobRef, error) {
	if len(b) < pieceOverhead {
		return nil, 0, 0, refs, ErrMalformed
	}
	if binary.LittleEndian.Uint16(b) != FormatVersion {
		return nil, 0, 0, refs, ErrBadVersion
	}
	n := int(binary.LittleEndian.Uint16(b[len(b)-2:]))
	seq := binary.LittleEndian.Uint64(b[len(b)-10:])
	kind := b[len(b)-11]
	end := len(b) - rootOverhead(n) + pieceHeader
	if end < pieceHeader || kind > filterBlock {
		return nil, 0, 0, refs, ErrMalformed
	}
	for i := range n {
		ref := disklog.DecodeBlobRef(b[end+i*disklog.BlobRefSize:])
		if ref.Segment == 0 {
			return nil, 0, 0, refs, ErrMalformed
		}
		refs = append(refs, ref)
	}
	return b[pieceHeader:end], kind, seq, refs, nil
}

// RootRefs appends to refs the run's earlier pieces that its root piece lists.
func RootRefs(root []byte, refs []disklog.BlobRef) ([]disklog.BlobRef, error) {
	_, _, _, refs, err := splitPiece(root, refs)
	return refs, err
}

// run is an immutable sorted run held in memory as the blocks of its pieces, empty blocks left
// out: entry blocks, then fragment blocks, and the filter of what its entries' keys hold. Each
// block's first key, stored whole, is its fence pointer. Its bounds span its entries and
// fragments, so a run holding only fragments is searched. sums holds each block's CRC32C, entry
// blocks then fragment blocks, then the filter's, since runs are never read back from disk. A
// merge's input past its first step holds only its keys above lo, its merge bound: its blocks
// start at entry piece base, the first ones wholly at or below lo left out.
type run struct {
	seq     uint64
	blocks  blockList
	frags   blockList
	filter  []byte
	sums    []uint32
	entries int
	nfrags  int
	bounded bool
	first   []byte
	last    []byte
	bytes   int
	pieces  []disklog.BlobRef
	held    bool
	lo      []byte
	base    int
}

// blockList is a run's entry or fragment blocks in key order.
type blockList [][]byte

func (r *run) root() disklog.BlobRef { return r.pieces[len(r.pieces)-1] }

func (r *run) addBlock(b []byte, n int) {
	r.blocks = append(r.blocks, b)
	r.entries += n
	r.bytes += len(b)
}

func (r *run) addFrag(b []byte, n int) {
	r.frags = append(r.frags, b)
	r.nfrags += n
	r.bytes += len(b)
}

func (r *run) setFilter(f []byte) {
	r.filter = f
	r.bytes += len(f)
}

// filterPieces is how many pieces of at most limit bytes the filter takes.
func (r *run) filterPieces(limit int) int { return (len(r.filter) + limit - 1) / limit }

// filterPiece is the filter's piece i of at most limit bytes.
func (r *run) filterPiece(i, limit int) []byte {
	return r.filter[i*limit : min((i+1)*limit, len(r.filter))]
}

// checksum sets sums from the run's blocks.
func (r *run) checksum() {
	r.sums = r.sums[:0]
	for _, b := range r.blocks {
		r.sums = append(r.sums, core.CRC32C(b))
	}
	for _, b := range r.frags {
		r.sums = append(r.sums, core.CRC32C(b))
	}
	r.sums = append(r.sums, core.CRC32C(r.filter))
}

// intact reports whether every block still has the checksum it was added with.
func (r *run) intact() bool {
	for i, b := range r.blocks {
		if core.CRC32C(b) != r.sums[i] {
			return false
		}
	}
	return r.fragsIntact()
}

// fragsIntact is intact for the fragment blocks and the filter.
func (r *run) fragsIntact() bool {
	for i, b := range r.frags {
		if core.CRC32C(b) != r.sums[len(r.blocks)+i] {
			return false
		}
	}
	return core.CRC32C(r.filter) == r.sums[len(r.blocks)+len(r.frags)]
}

// pieceIntact reports whether piece k of a flush writing r from entry block fromB and fragment
// block fromF on still has its checksum: a block's, or at the filter's first piece the whole
// filter's.
func (r *run) pieceIntact(k, fromB, fromF int) bool {
	nb, nf := len(r.blocks)-fromB, len(r.frags)-fromF
	switch {
	case k < nb:
		return core.CRC32C(r.blocks[fromB+k]) == r.sums[fromB+k]
	case k < nb+nf:
		i := fromF + k - nb
		return core.CRC32C(r.frags[i]) == r.sums[len(r.blocks)+i]
	case k == nb+nf:
		return core.CRC32C(r.filter) == r.sums[len(r.blocks)+len(r.frags)]
	}
	return true
}

// above is r holding only its keys above b: the entry blocks wholly at or below b left out, the
// block b falls in cut, and the fragments clipped to start past b. It shares r's other blocks and
// its filter, and fails with ErrCheck when a block it cuts no longer has its checksum.
func (r *run) above(b []byte) (*run, error) {
	n := &run{seq: r.seq, filter: r.filter, pieces: r.pieces, held: r.held, lo: bytes.Clone(b), base: r.base, entries: r.entries, nfrags: r.nfrags}
	var w blockWriter
	var it blockIter
	from := r.blocks.blockFor(b, false)
	kept := max(from, 0)
	if from >= 0 {
		if core.CRC32C(r.blocks[from]) != r.sums[from] {
			return nil, ErrCheck
		}
		w.reset(make([]byte, 0, len(r.blocks[from])+len(b)))
		it.init(r.blocks[from])
		for ok := it.first(); ok; ok = it.advance() {
			if bytes.Compare(it.key, b) > 0 {
				w.add(it.key, it.val)
			}
		}
		if w.n > 0 {
			n.blocks = append(n.blocks, w.finish())
			n.sums = append(n.sums, core.CRC32C(n.blocks[0]))
		}
		kept = from + 1
	}
	n.base += kept - len(n.blocks)
	n.blocks = append(n.blocks, r.blocks[kept:]...)
	n.sums = append(n.sums, r.sums[kept:len(r.blocks)]...)
	succ := append(bytes.Clone(b), 0)
	f := max(r.frags.blockFor(b, false), 0)
	for i := f; i < len(r.frags); i++ {
		if i > f {
			n.frags = append(n.frags, r.frags[i:]...)
			n.sums = append(n.sums, r.sums[len(r.blocks)+i:len(r.blocks)+len(r.frags)]...)
			break
		}
		if core.CRC32C(r.frags[i]) != r.sums[len(r.blocks)+i] {
			return nil, ErrCheck
		}
		w.reset(make([]byte, 0, len(r.frags[i])+len(succ)))
		it.init(r.frags[i])
		for ok := it.first(); ok; ok = it.advance() {
			switch {
			case bytes.Compare(it.val, succ) <= 0:
			case bytes.Compare(it.key, b) <= 0:
				w.add(succ, it.val)
			default:
				w.add(it.key, it.val)
			}
		}
		if w.n > 0 {
			n.frags = append(n.frags, w.finish())
			n.sums = append(n.sums, core.CRC32C(n.frags[len(n.frags)-1]))
		}
	}
	n.sums = append(n.sums, r.sums[len(r.sums)-1])
	for _, bl := range n.blocks {
		n.bytes += len(bl)
	}
	for _, bl := range n.frags {
		n.bytes += len(bl)
	}
	n.bytes += len(n.filter)
	n.setBounds()
	return n, nil
}

// entriesAbove counts r's entries above b, stepping through those at or below it from the start.
func entriesAbove(r *run, b []byte) int {
	n := r.entries
	if b == nil {
		return n
	}
	var it blockIter
	for _, bl := range r.blocks {
		it.init(bl)
		for ok := it.first(); ok; ok = it.advance() {
			if bytes.Compare(it.key, b) > 0 {
				return n
			}
			n--
		}
	}
	return n
}

// joinRuns is prev, which may be nil, followed by step's blocks and fragments, with filter.
func joinRuns(prev, step *run, filter []byte) *run {
	if prev == nil {
		prev = &run{seq: step.seq}
	}
	n := &run{seq: prev.seq, held: true}
	for _, src := range []*run{prev, step} {
		for _, b := range src.blocks {
			n.addBlock(b, 0)
			n.sums = append(n.sums, core.CRC32C(b))
		}
		n.entries += src.entries
	}
	for _, src := range []*run{prev, step} {
		for _, b := range src.frags {
			n.addFrag(b, 0)
			n.sums = append(n.sums, core.CRC32C(b))
		}
		n.nfrags += src.nfrags
	}
	n.setFilter(filter)
	n.sums = append(n.sums, core.CRC32C(filter))
	n.setBounds()
	return n
}

// setBounds sets the run's first key, its smallest key or fragment start, and its last, its
// largest key or fragment end.
func (r *run) setBounds() {
	r.bounded, r.first, r.last = false, nil, nil
	var it blockIter
	widen := func(lo, hi []byte) {
		if !r.bounded || bytes.Compare(lo, r.first) < 0 {
			r.first = bytes.Clone(lo)
		}
		if !r.bounded || bytes.Compare(hi, r.last) > 0 {
			r.last = bytes.Clone(hi)
		}
		r.bounded = true
	}
	if n := len(r.blocks); n > 0 {
		it.init(r.blocks[n-1])
		for ok := it.first(); ok && it.next < it.restarts; ok = it.advance() {
		}
		widen(firstKey(r.blocks[0]), it.key)
	}
	if n := len(r.frags); n > 0 {
		it.init(r.frags[n-1])
		for ok := it.first(); ok && it.next < it.restarts; ok = it.advance() {
		}
		widen(firstKey(r.frags[0]), it.val)
	}
}

// outside reports whether k lies outside the run's bounds, so neither its entries nor its
// fragments hold k.
func (r *run) outside(k []byte) bool {
	return r.bounded && (bytes.Compare(k, r.first) < 0 || bytes.Compare(k, r.last) > 0)
}

// below reports whether the run holds nothing at or below b, or below b when strict.
func (r *run) below(b []byte, strict bool) bool {
	if !r.bounded {
		return false
	}
	c := bytes.Compare(b, r.first)
	return c < 0 || c == 0 && strict
}

// beyond reports whether the run holds nothing at or above k.
func (r *run) beyond(k []byte) bool { return r.bounded && bytes.Compare(k, r.last) > 0 }

func firstKey(b []byte) []byte {
	_, n := binary.Uvarint(b)
	unshared, m := binary.Uvarint(b[n:])
	n += m
	_, m = binary.Uvarint(b[n:])
	n += m
	return b[n : n+int(unshared)]
}

// blockFor is the last block whose first key is <= k, or < k when strict, or -1.
func (bl blockList) blockFor(k []byte, strict bool) int { return bl.blockFrom(0, k, strict) }

// blockFrom is blockFor among the blocks from the given one on, or from-1.
func (bl blockList) blockFrom(from int, k []byte, strict bool) int {
	lo, hi := from, len(bl)
	for lo < hi {
		m := int(uint(lo+hi) >> 1)
		c := bytes.Compare(firstKey(bl[m]), k)
		if c < 0 || (c == 0 && !strict) {
			lo = m + 1
		} else {
			hi = m
		}
	}
	return lo - 1
}

func (bl blockList) get(it *blockIter, k []byte) ([]byte, bool) {
	b := bl.blockFor(k, false)
	if b < 0 {
		return nil, false
	}
	it.init(bl[b])
	if !it.seekGE(k) || !bytes.Equal(it.key, k) {
		return nil, false
	}
	return it.val, true
}

// runIter walks a run's entry or fragment blocks; searches counts its binary searches. With sums,
// moving forward into a block whose checksum fails ends the walk and sets bad.
type runIter struct {
	bl       blockList
	b        int
	it       blockIter
	searches int
	sums     []uint32
	bad      bool
}

// forwardSteps is how many entries seekForward steps over before it searches instead.
const forwardSteps = 16

func (ri *runIter) init(bl blockList) { ri.bl, ri.b, ri.it.off, ri.it.restarts = bl, -1, 0, 0 }

func (ri *runIter) valid() bool { return ri.b >= 0 && ri.b < len(ri.bl) && ri.it.valid() }

func (ri *runIter) seekGE(k []byte) bool {
	ri.searches++
	b := max(ri.bl.blockFor(k, false), 0)
	if b >= len(ri.bl) || !ri.enter(b) {
		return false
	}
	if ri.it.seekGE(k) {
		return true
	}
	return ri.nextBlock()
}

// seekForward moves to the first entry >= k, never back, so k must not fall below a key it was
// given before. It steps forward when k is near and searches the blocks ahead otherwise.
func (ri *runIter) seekForward(k []byte) bool {
	if ri.valid() {
		for range forwardSteps {
			if bytes.Compare(ri.it.key, k) >= 0 {
				return true
			}
			if !ri.next() {
				return false
			}
		}
		if bytes.Compare(ri.it.key, k) >= 0 {
			return true
		}
	}
	from := max(ri.b, 0)
	if from >= len(ri.bl) {
		return false
	}
	ri.searches++
	if b := max(ri.bl.blockFrom(from, k, false), from); b != ri.b && !ri.enter(b) {
		return false
	}
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
	for ri.b++; ri.b < len(ri.bl); ri.b++ {
		if !ri.enter(ri.b) {
			return false
		}
		if ri.it.first() {
			return true
		}
	}
	return false
}

func (ri *runIter) enter(b int) bool {
	if ri.sums != nil && core.CRC32C(ri.bl[b]) != ri.sums[b] {
		ri.b, ri.bad = len(ri.bl), true
		return false
	}
	ri.b = b
	ri.it.init(ri.bl[b])
	return true
}

func (ri *runIter) floor(k []byte, strict bool) bool {
	ri.searches++
	ri.b = ri.bl.blockFor(k, strict)
	if ri.b < 0 {
		return false
	}
	ri.it.init(ri.bl[ri.b])
	return ri.it.floor(k, strict)
}

// fillBlock adds entries from src to w and their keys to f while the finished block stays within
// limit bytes, leaving out each entry a fragment hides and each removal that a fragment of src covers or no run
// in older holds the key of, since it has nothing to hide. It counts what it leaves out in
// src.dropped and reports whether src ran out.
func fillBlock(w *blockWriter, f *filterWriter, src *iterState, limit int, older []runIter) bool {
	for ; src.valid(); src.next() {
		k, v := src.key(), src.value()
		if src.hidden() || v[0] == kindDel && (src.covered(k) || !holds(older, k)) {
			src.dropped++
			continue
		}
		if w.sizeWith(k, len(v)) > limit {
			return false
		}
		w.add(k, v)
		f.add(k)
	}
	return true
}

// fillFragBlock adds the fragments of src's union that a run in older holds a key in, up to those
// starting at upTo unless it is nil, to w while the finished block stays within limit bytes, and
// reports whether none is left.
func fillFragBlock(w *blockWriter, src *iterState, limit int, older []runIter, upTo []byte) bool {
	for src.pendingFrag(older) {
		if upTo != nil && bytes.Compare(src.ulo, upTo) > 0 {
			return false
		}
		if w.sizeWith(src.ulo, len(src.uhi)) > limit {
			return false
		}
		w.add(src.ulo, src.uhi)
		src.uok = false
		src.fragsDropped += src.united - 1
	}
	return true
}

func holds(older []runIter, k []byte) bool {
	for i := range older {
		if older[i].seekForward(k) && bytes.Equal(older[i].it.key, k) {
			return true
		}
	}
	return false
}

// heldIn reports whether a run in older holds a key in [lo, hi); ranges come in increasing order.
func heldIn(older []runIter, lo, hi []byte) bool {
	for i := range older {
		if older[i].seekForward(lo) && bytes.Compare(older[i].it.key, hi) < 0 {
			return true
		}
	}
	return false
}

// runIters walks the entry blocks of runs, checking each block's checksum as it enters it.
func runIters(runs []*run) []runIter {
	its := make([]runIter, len(runs))
	for i, r := range runs {
		its[i].init(r.blocks)
		its[i].sums = r.sums[:len(r.blocks)]
	}
	return its
}

func anyBad(its []runIter) bool {
	for i := range its {
		if its[i].bad {
			return true
		}
	}
	return false
}

// writePiece writes into dst a piece of block, of the given kind, which already sits at
// dst[pieceHeader:] when inPlace. It is the root if it is the last block and its trailer fits
// within max; otherwise the run gets an empty root after it.
func writePiece(dst, block []byte, kind byte, inPlace, last bool, seq uint64, earlier []disklog.BlobRef, max int) ([]byte, bool, error) {
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
	dst = append(dst, kind)
	dst = binary.LittleEndian.AppendUint64(dst, seq)
	return binary.LittleEndian.AppendUint16(dst, uint16(len(earlier))), root, nil
}
