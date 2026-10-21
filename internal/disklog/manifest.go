package disklog

import (
	"encoding/binary"
	"math"
	"slices"
)

const (
	// BlobRefSize is the encoded length of a BlobRef.
	BlobRefSize = 18

	manifestFixedSize  = 2 + 1 + 8 + 8 + 4 + 4
	manifestIndexSize  = 4 + 8 + 4 + 4
	editIndexSize      = 4 + 8 + 4 + 4 + 4
	mergeBoundSize     = BlobRefSize + 4 + 2
	pieceListSize      = 2 + 2
	pieceListEntrySize = 2

	// SegmentUsageSize is the encoded length of a SegmentUsage.
	SegmentUsageSize = 4 + 1 + 4 + 8
)

const (
	kindSnapshot byte = 1
	kindEdit     byte = 2
)

// BlobRef names a blob entry of a durable record by the record's segment, block offset and LSN and the entry's index.
type BlobRef struct {
	Segment uint32
	Block   uint32
	LSN     uint64
	Entry   uint16
}

// ManifestIndex is an index's runs, each named by its last piece, the bounds its running merges set on some of them, and the LSN after which records replay into it.
type ManifestIndex struct {
	Owner     uint32
	ReplayLSN uint64
	Runs      []BlobRef
	Bounds    []MergeBound
}

// SegmentState says whether a segment may be opened; zero is free.
type SegmentState uint8

// Segment states.
const (
	SegmentFree SegmentState = iota
	SegmentUsed
	SegmentQuarantined
	SegmentSlot
)

// SegmentUsage is a segment's row in the usage table: its state, the data blocks of its records, or for a slot segment the bitmap of its slots in use, and the LSN of its newest record, or of the newest naming one of its slots.
type SegmentUsage struct {
	Segment uint32
	State   SegmentState
	Live    uint32
	Newest  uint64
}

// Manifest is a snapshot: each index's runs and replay point, all at or before BaseLSN, the durable LSN when the checkpoint began, NextSeq, the open sequence the first segment opened after BaseLSN takes, and the usage table as of BaseLSN, every segment it leaves out free.
type Manifest struct {
	BaseLSN  uint64
	NextSeq  uint64
	Indexes  []ManifestIndex
	Segments []SegmentUsage
}

// MergeBound is the key at or below which a merge has consumed a run, which holds only keys above it, and how many of the run's first pieces lie wholly at or below it, which are dead.
type MergeBound struct {
	Run  BlobRef
	Key  []byte
	Dead uint32
}

// IndexEdit is an index's replay point, the runs an edit adds to and removes from it, and every bound its merges hold after it.
type IndexEdit struct {
	Owner     uint32
	ReplayLSN uint64
	Added     []BlobRef
	Removed   []BlobRef
	Bounds    []MergeBound
}

// ManifestEdit is what changed since the manifest before it: every index it keeps, the others dropped, and the usage rows that changed, a freed segment as a row in state free.
type ManifestEdit struct {
	BaseLSN  uint64
	NextSeq  uint64
	Indexes  []IndexEdit
	Segments []SegmentUsage
}

// AppendManifest appends the encoded snapshot.
func AppendManifest(dst []byte, m *Manifest) []byte {
	dst = appendManifestHead(dst, kindSnapshot, m.BaseLSN, m.NextSeq, len(m.Indexes))
	for i := range m.Indexes {
		ix := &m.Indexes[i]
		dst = binary.LittleEndian.AppendUint32(dst, ix.Owner)
		dst = binary.LittleEndian.AppendUint64(dst, ix.ReplayLSN)
		dst = appendRefs(dst, ix.Runs)
		dst = appendBounds(dst, ix.Bounds)
	}
	return appendRows(dst, m.Segments)
}

// ManifestSize is the encoded length of m.
func ManifestSize(m *Manifest) int {
	n := manifestFixedSize
	for i := range m.Indexes {
		n += manifestIndexSize + BlobRefSize*len(m.Indexes[i].Runs) + boundsSize(m.Indexes[i].Bounds)
	}
	return n + SegmentUsageSize*len(m.Segments)
}

// AppendManifestEdit appends the encoded edit.
func AppendManifestEdit(dst []byte, e *ManifestEdit) []byte {
	dst = appendManifestHead(dst, kindEdit, e.BaseLSN, e.NextSeq, len(e.Indexes))
	for i := range e.Indexes {
		ix := &e.Indexes[i]
		dst = binary.LittleEndian.AppendUint32(dst, ix.Owner)
		dst = binary.LittleEndian.AppendUint64(dst, ix.ReplayLSN)
		dst = appendRefs(dst, ix.Added)
		dst = appendRefs(dst, ix.Removed)
		dst = appendBounds(dst, ix.Bounds)
	}
	return appendRows(dst, e.Segments)
}

// ManifestEditSize is the encoded length of e.
func ManifestEditSize(e *ManifestEdit) int {
	n := manifestFixedSize
	for i := range e.Indexes {
		ix := &e.Indexes[i]
		n += editIndexSize + BlobRefSize*(len(ix.Added)+len(ix.Removed)) + boundsSize(ix.Bounds)
	}
	return n + SegmentUsageSize*len(e.Segments)
}

func appendManifestHead(dst []byte, kind byte, base, nextSeq uint64, indexes int) []byte {
	dst = binary.LittleEndian.AppendUint16(dst, FormatVersion)
	dst = append(dst, kind)
	dst = binary.LittleEndian.AppendUint64(dst, base)
	dst = binary.LittleEndian.AppendUint64(dst, nextSeq)
	return binary.LittleEndian.AppendUint32(dst, uint32(indexes))
}

func appendRefs(dst []byte, refs []BlobRef) []byte {
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(refs)))
	for _, r := range refs {
		dst = AppendBlobRef(dst, r)
	}
	return dst
}

func appendBounds(dst []byte, bounds []MergeBound) []byte {
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(bounds)))
	for _, b := range bounds {
		dst = AppendBlobRef(dst, b.Run)
		dst = binary.LittleEndian.AppendUint32(dst, b.Dead)
		dst = binary.LittleEndian.AppendUint16(dst, uint16(len(b.Key)))
		dst = append(dst, b.Key...)
	}
	return dst
}

func boundsSize(bounds []MergeBound) int {
	n := 0
	for _, b := range bounds {
		n += mergeBoundSize + len(b.Key)
	}
	return n
}

func appendRows(dst []byte, rows []SegmentUsage) []byte {
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(rows)))
	for _, u := range rows {
		dst = binary.LittleEndian.AppendUint32(dst, u.Segment)
		dst = append(dst, byte(u.State))
		dst = binary.LittleEndian.AppendUint32(dst, u.Live)
		dst = binary.LittleEndian.AppendUint64(dst, u.Newest)
	}
	return dst
}

// DecodeManifest decodes a snapshot, whose records' checksums already vouch for its bytes, so any inconsistency, an edit included, is ErrMalformed.
func DecodeManifest(b []byte) (Manifest, error) {
	r := reader{b: b}
	base, nextSeq, count, err := r.manifestHead(kindSnapshot, manifestIndexSize)
	if err != nil {
		return Manifest{}, err
	}
	m := Manifest{BaseLSN: base, NextSeq: nextSeq, Indexes: make([]ManifestIndex, count)}
	owners := make(map[uint32]bool, count)
	for i := range m.Indexes {
		ix := &m.Indexes[i]
		ix.Owner = r.u32()
		ix.ReplayLSN = r.u64()
		if owners[ix.Owner] || ix.ReplayLSN > base {
			return Manifest{}, ErrMalformed
		}
		owners[ix.Owner] = true
		if ix.Runs, err = r.refs(); err != nil {
			return Manifest{}, err
		}
		if ix.Bounds, err = r.bounds(); err != nil {
			return Manifest{}, err
		}
		if !validBounds(ix.Runs, ix.Bounds) {
			return Manifest{}, ErrMalformed
		}
	}
	if m.Segments, err = r.rows(base, false); err != nil {
		return Manifest{}, err
	}
	return m, nil
}

// DecodeManifestEdit decodes an edit as DecodeManifest decodes a snapshot.
func DecodeManifestEdit(b []byte) (ManifestEdit, error) {
	r := reader{b: b}
	base, nextSeq, count, err := r.manifestHead(kindEdit, editIndexSize)
	if err != nil {
		return ManifestEdit{}, err
	}
	e := ManifestEdit{BaseLSN: base, NextSeq: nextSeq, Indexes: make([]IndexEdit, count)}
	owners := make(map[uint32]bool, count)
	for i := range e.Indexes {
		ix := &e.Indexes[i]
		ix.Owner = r.u32()
		ix.ReplayLSN = r.u64()
		if owners[ix.Owner] || ix.ReplayLSN > base {
			return ManifestEdit{}, ErrMalformed
		}
		owners[ix.Owner] = true
		if ix.Added, err = r.refs(); err != nil {
			return ManifestEdit{}, err
		}
		if ix.Removed, err = r.refs(); err != nil {
			return ManifestEdit{}, err
		}
		if ix.Bounds, err = r.bounds(); err != nil {
			return ManifestEdit{}, err
		}
	}
	if e.Segments, err = r.rows(base, true); err != nil {
		return ManifestEdit{}, err
	}
	return e, nil
}

func (r *reader) manifestHead(kind byte, indexSize int) (base, nextSeq uint64, count uint32, err error) {
	if v := r.u16(); v != FormatVersion && !r.bad {
		return 0, 0, 0, ErrBadVersion
	}
	k := r.take(1)[0]
	base, nextSeq, count = r.u64(), r.u64(), r.u32()
	if r.bad || k != kind || nextSeq == 0 || uint64(count)*uint64(indexSize) > uint64(r.left()) {
		return 0, 0, 0, ErrMalformed
	}
	return base, nextSeq, count, nil
}

func (r *reader) refs() ([]BlobRef, error) {
	n := r.u32()
	if r.bad || uint64(n)*BlobRefSize > uint64(r.left()) {
		return nil, ErrMalformed
	}
	if n == 0 {
		return nil, nil
	}
	refs := make([]BlobRef, n)
	for i := range refs {
		if refs[i] = r.blobRef(); refs[i].Segment == 0 {
			return nil, ErrMalformed
		}
	}
	return refs, nil
}

func (r *reader) bounds() ([]MergeBound, error) {
	n := r.u32()
	if r.bad || uint64(n)*mergeBoundSize > uint64(r.left()) {
		return nil, ErrMalformed
	}
	if n == 0 {
		return nil, nil
	}
	bounds := make([]MergeBound, n)
	for i := range bounds {
		b := &bounds[i]
		b.Run = r.blobRef()
		b.Dead = r.u32()
		b.Key = slices.Clone(r.take(int(r.u16())))
		if r.bad || b.Run.Segment == 0 {
			return nil, ErrMalformed
		}
	}
	return bounds, nil
}

// rows decodes a usage table in segment order, ending the manifest; free rows, all zero but
// the segment, belong to edits only.
func (r *reader) rows(base uint64, edit bool) ([]SegmentUsage, error) {
	n := r.u32()
	if r.bad || uint64(n)*SegmentUsageSize != uint64(r.left()) {
		return nil, ErrMalformed
	}
	if n == 0 {
		return nil, nil
	}
	rows := make([]SegmentUsage, n)
	var prev uint32
	for i := range rows {
		u := SegmentUsage{Segment: r.u32(), State: SegmentState(r.take(1)[0]), Live: r.u32(), Newest: r.u64()}
		switch {
		case u.Segment <= prev || u.Newest > base || u.State > SegmentSlot:
			return nil, ErrMalformed
		case u.State == SegmentFree && (!edit || u.Live != 0 || u.Newest != 0):
			return nil, ErrMalformed
		}
		rows[i], prev = u, u.Segment
	}
	return rows, nil
}

// ApplyManifestEdit is m with e applied: e's indexes in its order, each keeping its runs in m not removed, then the added ones, and taking e's bounds; and m's usage table with e's rows. Removing a run m does not hold, adding one it does, a bound on a run the index does not hold, changing a quarantined row or going back in base LSN or NextSeq is ErrMalformed.
func ApplyManifestEdit(m *Manifest, e *ManifestEdit) (Manifest, error) {
	if e.BaseLSN < m.BaseLSN || e.NextSeq < m.NextSeq {
		return Manifest{}, ErrMalformed
	}
	indexes, err := applyIndexes(m.Indexes, e.Indexes)
	if err != nil {
		return Manifest{}, err
	}
	rows := make([]SegmentUsage, 0, len(m.Segments)+len(e.Segments))
	old := m.Segments
	for _, u := range e.Segments {
		for len(old) > 0 && old[0].Segment < u.Segment {
			rows = append(rows, old[0])
			old = old[1:]
		}
		if len(old) > 0 && old[0].Segment == u.Segment {
			if old[0].State == SegmentQuarantined && u.State != SegmentQuarantined {
				return Manifest{}, ErrMalformed
			}
			old = old[1:]
		}
		if u.State != SegmentFree {
			rows = append(rows, u)
		}
	}
	rows = append(rows, old...)
	return Manifest{BaseLSN: e.BaseLSN, NextSeq: e.NextSeq, Indexes: indexes, Segments: rows}, nil
}

func applyIndexes(base []ManifestIndex, edits []IndexEdit) ([]ManifestIndex, error) {
	at := ownerPositions(base)
	out := make([]ManifestIndex, len(edits))
	for i := range edits {
		ed := &edits[i]
		var runs []BlobRef
		if j, ok := at[ed.Owner]; ok {
			runs = slices.Clone(base[j].Runs)
		}
		for _, r := range ed.Removed {
			k := slices.Index(runs, r)
			if k < 0 {
				return nil, ErrMalformed
			}
			runs = slices.Delete(runs, k, k+1)
		}
		for _, r := range ed.Added {
			if slices.Contains(runs, r) {
				return nil, ErrMalformed
			}
			runs = append(runs, r)
		}
		if len(runs) == 0 {
			runs = nil
		}
		if !validBounds(runs, ed.Bounds) {
			return nil, ErrMalformed
		}
		out[i] = ManifestIndex{Owner: ed.Owner, ReplayLSN: ed.ReplayLSN, Runs: runs, Bounds: ed.Bounds}
	}
	return out, nil
}

// validBounds reports whether each bound names a different run of runs.
func validBounds(runs []BlobRef, bounds []MergeBound) bool {
	for i, b := range bounds {
		if !slices.Contains(runs, b.Run) || slices.ContainsFunc(bounds[:i], func(o MergeBound) bool { return o.Run == b.Run }) {
			return false
		}
	}
	return true
}

// diffIndexes is the index edits that turn base into next.
func diffIndexes(base, next []ManifestIndex) []IndexEdit {
	at := ownerPositions(base)
	edits := make([]IndexEdit, len(next))
	for i := range next {
		ix := &next[i]
		var old []BlobRef
		if j, ok := at[ix.Owner]; ok {
			old = base[j].Runs
		}
		ed := IndexEdit{Owner: ix.Owner, ReplayLSN: ix.ReplayLSN, Bounds: ix.Bounds}
		for _, r := range old {
			if !slices.Contains(ix.Runs, r) {
				ed.Removed = append(ed.Removed, r)
			}
		}
		for _, r := range ix.Runs {
			if !slices.Contains(old, r) {
				ed.Added = append(ed.Added, r)
			}
		}
		edits[i] = ed
	}
	return edits
}

func ownerPositions(indexes []ManifestIndex) map[uint32]int {
	at := make(map[uint32]int, len(indexes))
	for i := range indexes {
		at[indexes[i].Owner] = i
	}
	return at
}

// LoadManifest decodes a snapshot and applies the edits after it in order, the bodies a last piece's list names.
func LoadManifest(bodies [][]byte) (Manifest, error) {
	if len(bodies) == 0 {
		return Manifest{}, ErrMalformed
	}
	m, err := DecodeManifest(bodies[0])
	if err != nil {
		return Manifest{}, err
	}
	for _, b := range bodies[1:] {
		e, err := DecodeManifestEdit(b)
		if err != nil {
			return Manifest{}, err
		}
		if m, err = ApplyManifestEdit(&m, &e); err != nil {
			return Manifest{}, err
		}
	}
	return m, nil
}

// AppendManifestPiece appends a manifest's last piece: a version, the references to the pieces of its snapshot and of each edit since in order, the newest's without this one, then the newest's remaining bytes.
func AppendManifestPiece(dst []byte, pieces [][]BlobRef, rest []byte) ([]byte, error) {
	if len(pieces) == 0 || len(pieces) > math.MaxUint16 || slices.ContainsFunc(pieces, func(p []BlobRef) bool { return len(p) > math.MaxUint16 }) {
		return dst, ErrTooLarge
	}
	dst = binary.LittleEndian.AppendUint16(dst, FormatVersion)
	dst = binary.LittleEndian.AppendUint16(dst, uint16(len(pieces)))
	for _, p := range pieces {
		dst = binary.LittleEndian.AppendUint16(dst, uint16(len(p)))
		for _, r := range p {
			dst = AppendBlobRef(dst, r)
		}
	}
	return append(dst, rest...), nil
}

// ManifestPieceSize is the length of a last piece listing pieces with rest bytes after them.
func ManifestPieceSize(pieces [][]BlobRef, rest int) int {
	return pieceListSize + pieceListBytes(pieces) + rest
}

func pieceListBytes(pieces [][]BlobRef) int {
	n := 0
	for _, p := range pieces {
		n += pieceListEntrySize + BlobRefSize*len(p)
	}
	return n
}

// DecodeManifestPiece splits a manifest's last piece into the references it lists, one list per manifest from the snapshot on, and the remaining bytes.
func DecodeManifestPiece(b []byte) ([][]BlobRef, []byte, error) {
	r := reader{b: b}
	if v := r.u16(); v != FormatVersion && !r.bad {
		return nil, nil, ErrBadVersion
	}
	n := int(r.u16())
	if r.bad || n == 0 || n*pieceListEntrySize > r.left() {
		return nil, nil, ErrMalformed
	}
	pieces := make([][]BlobRef, n)
	for i := range pieces {
		c := int(r.u16())
		if r.bad || c*BlobRefSize > r.left() {
			return nil, nil, ErrMalformed
		}
		pieces[i] = make([]BlobRef, c)
		for j := range pieces[i] {
			if pieces[i][j] = r.blobRef(); pieces[i][j].Segment == 0 {
				return nil, nil, ErrMalformed
			}
		}
	}
	return pieces, r.b, nil
}

// manifestBytes is what a manifest of body bytes writes in blobs of at most maxBlob, with prior
// the pieces of the manifests its last piece lists before it, and whether its last piece fits.
func manifestBytes(body int, prior [][]BlobRef, maxBlob uint64) (uint64, bool) {
	fixed := pieceListSize + pieceListBytes(prior) + pieceListEntrySize
	var total uint64
	own, written := 0, 0
	for {
		rest := body - written
		if last := fixed + BlobRefSize*own + rest; uint64(last) <= maxBlob && len(prior) < math.MaxUint16 {
			return total + uint64(last), true
		}
		if rest == 0 || own >= math.MaxUint16 {
			return 0, false
		}
		n := min(rest, int(maxBlob))
		total += uint64(n)
		written += n
		own++
	}
}

// snapshotDue is Pebble's rotation rule: a checkpoint writes a snapshot once the edits since the
// last one, this one included, would exceed the smaller of that snapshot and the next.
func snapshotDue(snapshot, editsSince, edit, next uint64) bool {
	return editsSince+edit > min(snapshot, next)
}

// AppendBlobRef appends the encoded reference.
func AppendBlobRef(dst []byte, r BlobRef) []byte {
	dst = binary.LittleEndian.AppendUint32(dst, r.Segment)
	dst = binary.LittleEndian.AppendUint32(dst, r.Block)
	dst = binary.LittleEndian.AppendUint64(dst, r.LSN)
	return binary.LittleEndian.AppendUint16(dst, r.Entry)
}

type reader struct {
	b   []byte
	bad bool
}

var zeroField [BlobRefSize]byte

func (r *reader) take(n int) []byte {
	if r.bad || len(r.b) < n {
		r.bad = true
		if n > len(zeroField) {
			return nil
		}
		return zeroField[:n]
	}
	p := r.b[:n]
	r.b = r.b[n:]
	return p
}

func (r *reader) left() int        { return len(r.b) }
func (r *reader) u16() uint16      { return binary.LittleEndian.Uint16(r.take(2)) }
func (r *reader) u32() uint32      { return binary.LittleEndian.Uint32(r.take(4)) }
func (r *reader) u64() uint64      { return binary.LittleEndian.Uint64(r.take(8)) }
func (r *reader) blobRef() BlobRef { return DecodeBlobRef(r.take(BlobRefSize)) }

// DecodeBlobRef decodes the reference in the first BlobRefSize bytes of b.
func DecodeBlobRef(b []byte) BlobRef {
	return BlobRef{
		Segment: binary.LittleEndian.Uint32(b),
		Block:   binary.LittleEndian.Uint32(b[4:]),
		LSN:     binary.LittleEndian.Uint64(b[8:]),
		Entry:   binary.LittleEndian.Uint16(b[16:]),
	}
}
