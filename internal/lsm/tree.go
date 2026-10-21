// Package lsm is the log-structured merge tree behind every index in a disk log: a copy-on-write
// page-tree memtable, immutable prefix-compressed sorted runs held in memory, universal
// compaction, point-in-time views, flushes written to the log one piece at a time and restores
// from the pieces a manifest names.
package lsm

import (
	"bytes"
	"encoding/binary"
	"errors"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/disklog"
)

const (
	// MaxKey is the longest key a tree takes.
	MaxKey = 1032
	// MaxValue is the longest value a tree takes.
	MaxValue = 1000
	// MaxBound is the longest bound of a range removal a tree takes.
	MaxBound = 1000
	// MinPiece is the smallest piece size a tree is configured with.
	MinPiece = 64
)

var (
	// ErrTooLarge reports a key or value over its limit or larger than a piece, or a run with more pieces than its root can list.
	ErrTooLarge = errors.New("lsm: too large")
	// ErrBadConfig reports settings a tree cannot run with.
	ErrBadConfig = errors.New("lsm: bad config")
	// ErrBusy reports a flush begun while another runs.
	ErrBusy = errors.New("lsm: already running")
	// ErrNoFlush reports a flush call with no flush running, or at the wrong step.
	ErrNoFlush = errors.New("lsm: no flush at that step")
	// ErrMalformed reports a run piece that does not decode.
	ErrMalformed = errors.New("lsm: malformed run piece")
	// ErrBadVersion reports a run piece of a format version this code does not read.
	ErrBadVersion = errors.New("lsm: unsupported run format version")
	// ErrStale reports a compaction whose inputs are no longer the tree's.
	ErrStale = errors.New("lsm: compaction inputs changed")
	// ErrEmptyRange reports a range removal whose start is not below its end.
	ErrEmptyRange = errors.New("lsm: empty range")
	// ErrCheck reports a flush or merge whose entry counts or the checksums of the runs it read do not add up.
	ErrCheck = errors.New("lsm: run check failed")
)

// Config is a tree's settings.
type Config struct {
	// MaxPiece is the largest run piece, the log's MaxBlobBytes; at least MinPiece.
	MaxPiece int
	// RunLimit is the most runs before the newest merge whatever their sizes; 0 tunes it from the largest run.
	RunLimit int
	// Check decodes every piece a flush builds, checks a merge's output order and the run list's, and panics on a mismatch.
	Check bool
	// Filter is what each run's filter holds; prefixes need Prefix.
	Filter FilterKeys
	// Prefix is the length of k's prefix, 0 if k has none; every key starting with a prefix must have that prefix.
	Prefix func(k []byte) int
}

// Tree is an LSM tree. Only its owner calls it; a compaction's Run alone may run elsewhere.
type Tree struct {
	cfg         Config
	pg          pages
	mem         *memtable
	frozen      *memtable
	frozenSeq   uint64
	runs        []*run
	nextSeq     uint64
	fl          *flush
	written     []*run
	merges      []*Compaction
	limitBuffer int
	maxFrag     int
	it          iterState
	linear      bool
	plant       plants
}

// plants are bugs a test turns on to show that its checks catch them.
type plants struct {
	noCut         bool
	dropFrags     bool
	keepCovered   bool
	skipEncoded   bool
	skipFiltered  bool
	repeatBlock   bool
	installOldest bool
	noSums        bool
	dropRemovals  bool
	loadUnordered bool
	skipFrozen    bool
	mergeNoOlder  bool
}

type flush struct {
	carried     []disklog.BlobRef
	named       []*run
	write       []*run
	frozen      *run
	src         iterState
	bw          blockWriter
	older       []runIter
	olderFrag   []runIter
	filter      filterWriter
	entriesDone bool
	cur         int
	next        int
	refs        []disklog.BlobRef
	inFlight    bool
	isRoot      bool
	written     bool
}

// New makes an empty tree.
func New(cfg Config) (*Tree, error) {
	if cfg.MaxPiece < MinPiece || cfg.RunLimit < 0 || cfg.Filter > KeysAndPrefixes || cfg.Filter != WholeKeys && cfg.Prefix == nil {
		return nil, ErrBadConfig
	}
	t := &Tree{cfg: cfg, nextSeq: 1, limitBuffer: autoLimitBuffer}
	for t.maxFrag = MaxBound; t.maxFrag > 0 && !t.fits(t.maxFrag, t.maxFrag); t.maxFrag-- {
	}
	t.mem = newMemtable(&t.pg)
	return t, nil
}

func (t *Tree) state() state {
	st := state{mem: t.mem, memRoot: t.mem.root, memFRoot: t.mem.froot, runs: t.runs, linear: t.linear, filter: t.cfg.Filter, prefix: t.cfg.Prefix}
	if t.frozen != nil && !t.plant.skipFrozen {
		st.frozen, st.frozenRoot, st.frozenFRoot = t.frozen, t.frozen.root, t.frozen.froot
	}
	return st
}

// Put sets k to v.
func (t *Tree) Put(k, v []byte) error {
	if !t.fits(len(k), len(v)) {
		return ErrTooLarge
	}
	if !t.plant.noCut {
		t.mem.cut(k, t.maxFrag)
	}
	t.mem.put(k, kindSet, v)
	return nil
}

// DeleteRange removes every key in [lo, hi).
func (t *Tree) DeleteRange(lo, hi []byte) error {
	if bytes.Compare(lo, hi) >= 0 {
		return ErrEmptyRange
	}
	if max(len(lo), len(hi)) > t.maxFrag {
		return ErrTooLarge
	}
	t.mem.deleteRange(lo, hi)
	return nil
}

// Delete removes k.
func (t *Tree) Delete(k []byte) error {
	if !t.fits(len(k), 0) {
		return ErrTooLarge
	}
	t.mem.put(k, kindDel, nil)
	return nil
}

// fits reports whether an entry fits a page and, alone in a block, a piece.
func (t *Tree) fits(k, v int) bool {
	entry := uvarintLen(0) + uvarintLen(k) + uvarintLen(v+1) + k + v + 1
	return k <= MaxKey && v <= MaxValue && entry+emptyBlockSize+pieceOverhead <= t.cfg.MaxPiece
}

// Get returns k's value, valid until the tree's next write.
func (t *Tree) Get(k []byte) ([]byte, bool) {
	st := t.state()
	t.it.reset(&st, false)
	return t.it.get(k)
}

// Floor returns the greatest key <= k and its value, valid until the tree's next write.
func (t *Tree) Floor(k []byte) ([]byte, []byte, bool) {
	st := t.state()
	t.it.reset(&st, false)
	return t.it.floor(k)
}

// FloorPrefix returns the greatest key <= k with k's prefix, or any when k has none, and its value, valid until the tree's next write.
func (t *Tree) FloorPrefix(k []byte) ([]byte, []byte, bool) {
	st := t.state()
	t.it.reset(&st, false)
	return t.it.floorPrefix(k)
}

// Iter opens it on the tree; it is valid until the tree's next write. Seek it before reading.
func (t *Tree) Iter(it *Iter) {
	st := t.state()
	it.s.reset(&st, false)
}

// MemtableBytes is the memory the memtables' pages take.
func (t *Tree) MemtableBytes() int { return t.pg.inUse * pageSize }

// RunBytes is the memory the runs take.
func (t *Tree) RunBytes() int {
	n := 0
	for _, r := range t.runs {
		n += r.bytes
	}
	return n
}

// Runs is the number of runs.
func (t *Tree) Runs() int { return len(t.runs) }

// Frozen reports whether a memtable is frozen for a flush that has not finished writing it.
func (t *Tree) Frozen() bool { return t.frozen != nil }

// Searches is how many layers the tree's gets and floors have searched.
func (t *Tree) Searches() int { return t.it.probes }

// View is a point-in-time view of a tree that pins what it reads until closed; only the tree's owner reads it.
type View struct {
	st        state
	memGen    uint32
	frozenGen uint32
	it        iterState
	closed    bool
}

// View opens a view of the tree as it is now.
func (t *Tree) View() *View {
	v := &View{st: t.state()}
	v.st.runs = slices.Clone(t.runs)
	v.memGen = t.mem.openView()
	if t.frozen != nil {
		v.frozenGen = t.frozen.openView()
	}
	return v
}

// Get returns k's value as of the view.
func (v *View) Get(k []byte) ([]byte, bool) {
	v.it.reset(&v.st, false)
	return v.it.get(k)
}

// Floor returns the greatest key <= k and its value as of the view.
func (v *View) Floor(k []byte) ([]byte, []byte, bool) {
	v.it.reset(&v.st, false)
	return v.it.floor(k)
}

// FloorPrefix returns the greatest key <= k with k's prefix, or any when k has none, and its value as of the view.
func (v *View) FloorPrefix(k []byte) ([]byte, []byte, bool) {
	v.it.reset(&v.st, false)
	return v.it.floorPrefix(k)
}

// Iter opens it on the view. Seek it before reading.
func (v *View) Iter(it *Iter) { it.s.reset(&v.st, false) }

// Close releases what the view pins.
func (v *View) Close() {
	if v.closed {
		return
	}
	v.closed = true
	v.st.mem.closeView(v.memGen)
	if v.st.frozen != nil {
		v.st.frozen.closeView(v.frozenGen)
	}
	v.st = state{}
}

// BeginFlush starts writing the frozen memtable and every run not yet durable, and reports whether it froze the memtable.
func (t *Tree) BeginFlush() (bool, error) {
	if t.fl != nil {
		return false, ErrBusy
	}
	froze := false
	if t.frozen == nil && (t.mem.count > 0 || t.mem.frags > 0) {
		t.frozen, t.frozenSeq = t.mem, t.nextSeq
		t.nextSeq++
		t.mem = newMemtable(&t.pg)
		froze = true
	}
	fl := &flush{named: slices.Clone(t.runs), older: runIters(t.runs), olderFrag: runIters(t.runs)}
	for _, r := range t.runs {
		if r.durable() {
			fl.carried = append(fl.carried, r.root())
		} else {
			fl.write = append(fl.write, r)
		}
	}
	if t.frozen != nil {
		fl.frozen = &run{seq: t.frozenSeq}
		fl.write = append(fl.write, fl.frozen)
		st := state{mem: t.frozen, memRoot: t.frozen.root, memFRoot: t.frozen.froot, linear: t.linear}
		fl.src.reset(&st, true)
		fl.src.seek(nil)
		keys := t.frozen.count
		if t.cfg.Filter == KeysAndPrefixes {
			keys *= 2
		}
		fl.filter.reset(t.cfg.Filter, t.cfg.Prefix, keys)
	}
	t.fl = fl
	return froze, nil
}

// CarriedRuns is the root piece of each durable run the running flush does not rewrite.
func (t *Tree) CarriedRuns() []disklog.BlobRef {
	if t.fl == nil {
		return nil
	}
	return t.fl.carried
}

// NextPiece builds the flush's next piece in buf and returns its length, 0 when done, and whether it is a run's root.
func (t *Tree) NextPiece(buf []byte) (int, bool, error) {
	fl := t.fl
	if fl == nil || fl.inFlight {
		return 0, false, ErrNoFlush
	}
	if len(buf) < t.cfg.MaxPiece {
		return 0, false, ErrBadConfig
	}
	buf = buf[:t.cfg.MaxPiece:t.cfg.MaxPiece]
	if fl.cur == len(fl.write) {
		t.finishWrite()
		return 0, false, nil
	}
	w := fl.write[fl.cur]
	limit := t.cfg.MaxPiece - pieceOverhead
	var block []byte
	var last, inPlace bool
	kind := entryBlock
	if w == fl.frozen && w.filter == nil {
		bw := &fl.bw
		bw.reset(buf[pieceHeader:])
		bw.record, bw.skipSecond = t.cfg.Check, t.plant.skipEncoded
		fl.filter.skipSecond = t.plant.skipFiltered
		if !fl.entriesDone {
			older := fl.older
			if t.plant.dropRemovals {
				older = nil
			}
			if fl.entriesDone = fillBlock(bw, &fl.filter, &fl.src, limit, older); fl.entriesDone {
				fl.src.fragsFromStart()
			}
		}
		if bw.n == 0 && fl.entriesDone {
			kind = fragBlock
			fillFragBlock(bw, &fl.src, limit, fl.olderFrag)
		}
		done := fl.entriesDone && !fl.src.pendingFrag(fl.olderFrag)
		if bw.n == 0 && !done {
			return 0, false, ErrTooLarge
		}
		if t.cfg.Check && kind == entryBlock {
			fl.filter.checkHolds(bw.taken)
		}
		if bw.n > 0 {
			block, inPlace = bw.finish(), true
			if kind == fragBlock {
				w.addFrag(bytes.Clone(block), bw.n)
			} else {
				w.addBlock(bytes.Clone(block), bw.n)
			}
		}
		if done {
			if err := t.checkFlushCounts(w); err != nil {
				return 0, false, err
			}
			if len(w.blocks)+len(w.frags) == 0 {
				fl.frozen, fl.write = nil, fl.write[:fl.cur]
				t.finishWrite()
				return 0, false, nil
			}
			w.setFilter(fl.filter.finish())
		}
	}
	if block == nil {
		nb, nf := len(w.blocks), len(w.frags)
		total := nb + nf + w.filterPieces(limit)
		switch {
		case fl.next < nb:
			block = w.blocks[fl.next]
		case fl.next < nb+nf:
			block, kind = w.frags[fl.next-nb], fragBlock
		case fl.next < total:
			block, kind = w.filterPiece(fl.next-nb-nf, limit), filterBlock
		default:
			block = emptyBlock[:]
		}
		last = fl.next >= total-1
	}
	if !inPlace && w.sums != nil && !w.pieceIntact(fl.next) {
		return 0, false, ErrCheck
	}
	piece, root, err := writePiece(buf, block, kind, inPlace, last, w.seq, fl.refs, t.cfg.MaxPiece)
	if err != nil {
		return 0, false, err
	}
	if t.cfg.Check && inPlace {
		checkPiece(piece, kind, w.seq, fl.bw.taken)
	}
	fl.next++
	fl.inFlight, fl.isRoot = true, root
	return len(piece), root, nil
}

var emptyBlock = [emptyBlockSize]byte{4: 1}

// checkFlushCounts fails the flush unless the run it wrote from the frozen memtable and what it
// dropped hold as many entries and fragments as the memtable.
func (t *Tree) checkFlushCounts(w *run) error {
	fl := t.fl
	if anyBad(fl.older) || anyBad(fl.olderFrag) ||
		w.entries+fl.src.dropped != t.frozen.count || w.nfrags+fl.src.fragsDropped != t.frozen.frags {
		return ErrCheck
	}
	return nil
}

// checkPiece panics unless piece decodes as a block of kind in run seq holding exactly the entries
// in taken, each a uvarint-prefixed key then value.
func checkPiece(piece []byte, kind byte, seq uint64, taken []byte) {
	block, k, s, _, err := splitPiece(piece, nil)
	if err != nil || k != kind || s != seq {
		panic("lsm: a flush built a piece that does not decode")
	}
	n, _, ok := walkBlock(block, nil, false, kind == fragBlock)
	if !ok {
		panic("lsm: a flush built a block that does not decode")
	}
	var it blockIter
	it.init(block)
	for ok := it.first(); ok; ok = it.advance() {
		var key, val []byte
		key, taken = cutUvarint(taken)
		val, taken = cutUvarint(taken)
		if !bytes.Equal(it.key, key) || !bytes.Equal(it.val, val) {
			panic("lsm: a flush built a block that differs from the entries it took")
		}
		n--
	}
	if n != 0 || len(taken) != 0 {
		panic("lsm: a flush built a block that differs from the entries it took")
	}
}

func cutUvarint(b []byte) ([]byte, []byte) {
	l, n := binary.Uvarint(b)
	if n <= 0 || uint64(len(b)-n) < l {
		return nil, nil
	}
	return b[n : n+int(l)], b[n+int(l):]
}

// checkRuns panics, with Check on, unless the run list is newest first with no sequence number twice.
func (t *Tree) checkRuns() {
	if !t.cfg.Check {
		return
	}
	for i := 1; i < len(t.runs); i++ {
		if t.runs[i].seq >= t.runs[i-1].seq {
			panic("lsm: run list out of sequence order")
		}
	}
}

// PieceDurable reports the piece NextPiece built last durable at ref.
func (t *Tree) PieceDurable(ref disklog.BlobRef) error {
	fl := t.fl
	if fl == nil || !fl.inFlight {
		return ErrNoFlush
	}
	fl.inFlight = false
	if !fl.isRoot {
		fl.refs = append(fl.refs, ref)
		return nil
	}
	w := fl.write[fl.cur]
	w.pieces = append(slices.Clone(fl.refs), ref)
	t.written = append(t.written, w)
	fl.refs, fl.cur, fl.next = fl.refs[:0], fl.cur+1, 0
	if fl.cur == len(fl.write) {
		t.finishWrite()
	}
	return nil
}

func (t *Tree) finishWrite() {
	fl := t.fl
	if fl.written {
		return
	}
	fl.written = true
	if fl.frozen != nil {
		fl.frozen.setBounds()
		fl.frozen.checksum()
		t.runs = append([]*run{fl.frozen}, t.runs...)
		t.checkRuns()
	}
	if t.frozen != nil {
		t.frozen.unref()
		t.frozen = nil
	}
}

// CheckpointDone ends the flush once its manifest is durable and returns the pieces of the runs it no longer names.
func (t *Tree) CheckpointDone() ([]disklog.BlobRef, error) {
	fl := t.fl
	if fl == nil || !fl.written {
		return nil, ErrNoFlush
	}
	named := fl.named
	if fl.frozen != nil {
		named = append(named, fl.frozen)
	}
	var dead []disklog.BlobRef
	keep := t.written[:0]
	for _, r := range t.written {
		if slices.Contains(named, r) {
			keep = append(keep, r)
		} else {
			dead = append(dead, r.pieces...)
		}
	}
	clear(t.written[len(keep):])
	t.written = keep
	t.fl = nil
	return dead, nil
}

// FlushFailed ends a failed flush, keeping its frozen memtable for the next, and returns the pieces nothing names.
func (t *Tree) FlushFailed() []disklog.BlobRef {
	fl := t.fl
	if fl == nil {
		return nil
	}
	t.fl = nil
	if fl.written {
		return nil
	}
	if fl.frozen != nil {
		*fl.frozen = run{seq: fl.frozen.seq}
	}
	return slices.Clone(fl.refs)
}

// LoadRun restores a run from its pieces in order, its root at ref last; runs may load in any order.
func (t *Tree) LoadRun(ref disklog.BlobRef, pieces [][]byte) error {
	if len(pieces) == 0 {
		return ErrMalformed
	}
	_, _, seq, refs, err := splitPiece(pieces[len(pieces)-1], nil)
	if err != nil {
		return err
	}
	if len(refs) != len(pieces)-1 || slices.ContainsFunc(t.runs, func(r *run) bool { return r.seq == seq }) {
		return ErrMalformed
	}
	r := &run{seq: seq, pieces: append(refs, ref)}
	var last, end, filter []byte
	sawFilter := false
	for i, p := range pieces {
		block, kind, s, rr, err := splitPiece(p, nil)
		if err != nil {
			return err
		}
		if s != seq || (i < len(pieces)-1 && len(rr) > 0) {
			return ErrMalformed
		}
		if kind == filterBlock {
			filter, sawFilter = append(filter, block...), true
			continue
		}
		if sawFilter && !(i == len(pieces)-1 && bytes.Equal(block, emptyBlock[:])) {
			return ErrMalformed
		}
		if kind == fragBlock {
			n, e, ok := validateFragBlock(block, end, len(r.frags) > 0)
			if !ok {
				return ErrMalformed
			}
			end = e
			if n > 0 {
				r.addFrag(bytes.Clone(block), n)
			}
			continue
		}
		n, l, ok := validateBlock(block, last, len(r.blocks) > 0)
		if !ok || n > 0 && len(r.frags) > 0 {
			return ErrMalformed
		}
		last = l
		if n > 0 {
			r.addBlock(bytes.Clone(block), n)
		}
	}
	if !validFilter(filter, t.cfg.Filter) {
		return ErrMalformed
	}
	r.setFilter(filter)
	r.setBounds()
	r.checksum()
	at, _ := slices.BinarySearchFunc(t.runs, seq, func(r *run, s uint64) int {
		switch {
		case r.seq > s:
			return -1
		case r.seq < s:
			return 1
		}
		return 0
	})
	if t.plant.loadUnordered {
		at = 0
	}
	t.runs = slices.Insert(t.runs, at, r)
	t.written = append(t.written, r)
	t.nextSeq = max(t.nextSeq, seq+1)
	return nil
}
