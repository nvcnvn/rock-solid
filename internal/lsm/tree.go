// Package lsm is the log-structured merge tree behind every index in a disk log: a copy-on-write
// page-tree memtable, immutable prefix-compressed sorted runs held in memory, size-tiered
// compaction, point-in-time views, flushes written to the log one piece at a time and restores
// from the pieces a manifest names.
package lsm

import (
	"bytes"
	"errors"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/disklog"
)

const (
	// MaxKey is the longest key a tree takes.
	MaxKey = 1024
	// MaxValue is the longest value a tree takes.
	MaxValue = 1000
	// MinPiece is the smallest piece size a tree is configured with.
	MinPiece = 64
)

var (
	// ErrTooLarge reports a key or value over its limit or larger than a piece, or a run with more pieces than its root can list.
	ErrTooLarge = errors.New("lsm: too large")
	// ErrBadConfig reports settings a tree cannot run with.
	ErrBadConfig = errors.New("lsm: bad config")
	// ErrBusy reports a flush begun while another runs, or a compaction picked while another runs.
	ErrBusy = errors.New("lsm: already running")
	// ErrNoFlush reports a flush call with no flush running, or at the wrong step.
	ErrNoFlush = errors.New("lsm: no flush at that step")
	// ErrMalformed reports a run piece that does not decode.
	ErrMalformed = errors.New("lsm: malformed run piece")
	// ErrBadVersion reports a run piece of a format version this code does not read.
	ErrBadVersion = errors.New("lsm: unsupported run format version")
	// ErrStale reports a compaction whose inputs are no longer the tree's.
	ErrStale = errors.New("lsm: compaction inputs changed")
)

// Config is a tree's settings.
type Config struct {
	// MaxPiece is the largest run piece, the log's MaxBlobBytes; at least MinPiece.
	MaxPiece int
}

// Tree is an LSM tree. Only its owner calls it; a compaction's Run alone may run elsewhere.
type Tree struct {
	cfg       Config
	pg        pages
	mem       *memtable
	frozen    *memtable
	frozenSeq uint64
	runs      []*run
	nextSeq   uint64
	fl        *flush
	written   []*run
	compact   *Compaction
	it        iterState
}

type flush struct {
	carried  []disklog.BlobRef
	named    []*run
	write    []*run
	frozen   *run
	src      iterState
	bw       blockWriter
	older    []*run
	probe    blockIter
	cur      int
	next     int
	refs     []disklog.BlobRef
	inFlight bool
	isRoot   bool
	written  bool
}

// New makes an empty tree.
func New(cfg Config) (*Tree, error) {
	if cfg.MaxPiece < MinPiece {
		return nil, ErrBadConfig
	}
	t := &Tree{cfg: cfg, nextSeq: 1}
	t.mem = newMemtable(&t.pg)
	return t, nil
}

func (t *Tree) state() state {
	st := state{mem: t.mem, memRoot: t.mem.root, runs: t.runs}
	if t.frozen != nil {
		st.frozen, st.frozenRoot = t.frozen, t.frozen.root
	}
	return st
}

// Put sets k to v.
func (t *Tree) Put(k, v []byte) error {
	if !t.fits(len(k), len(v)) {
		return ErrTooLarge
	}
	t.mem.put(k, kindSet, v)
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

// View is a point-in-time view of a tree. It pins the pages and runs it reads, so it is closed
// once done; only the tree's owner reads it.
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

// BeginFlush starts writing what the newest manifest will need: the memtable, frozen now unless a
// failed flush left one frozen, and every run not yet durable. It reports whether it froze the
// memtable, which fixes the index's replay point. Pieces follow, each once the one before is durable.
func (t *Tree) BeginFlush() (bool, error) {
	if t.fl != nil {
		return false, ErrBusy
	}
	froze := false
	if t.frozen == nil && t.mem.count > 0 {
		t.frozen, t.frozenSeq = t.mem, t.nextSeq
		t.nextSeq++
		t.mem = newMemtable(&t.pg)
		froze = true
	}
	fl := &flush{named: slices.Clone(t.runs)}
	fl.older = fl.named
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
		st := state{mem: t.frozen, memRoot: t.frozen.root}
		fl.src.reset(&st, true)
		fl.src.seek(nil)
	}
	t.fl = fl
	return froze, nil
}

// CarriedRuns is the root piece of each durable run the running flush does not rewrite, for the
// checkpoint to name again.
func (t *Tree) CarriedRuns() []disklog.BlobRef {
	if t.fl == nil {
		return nil
	}
	return t.fl.carried
}

// NextPiece builds the flush's next piece in buf, which holds MaxPiece bytes, and returns its length
// and whether it is a run's root; a length of 0 means every piece is written.
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
	var block []byte
	var last, inPlace bool
	switch {
	case w == fl.frozen:
		bw := &fl.bw
		bw.reset(buf[pieceHeader:])
		last = fillBlock(bw, &fl.src, t.cfg.MaxPiece-pieceOverhead, fl.older, &fl.probe)
		if bw.n == 0 && len(w.blocks) == 0 && last {
			fl.frozen, fl.write = nil, fl.write[:fl.cur]
			t.finishWrite()
			return 0, false, nil
		}
		if bw.n == 0 && !last {
			return 0, false, ErrTooLarge
		}
		block, inPlace = bw.finish(), true
		if bw.n > 0 {
			w.addBlock(bytes.Clone(block))
		}
	case fl.next < len(w.blocks):
		block, last = w.blocks[fl.next], fl.next == len(w.blocks)-1
	default:
		block, last = emptyBlock[:], true
	}
	piece, root, err := writePiece(buf, block, inPlace, last, w.seq, fl.refs, t.cfg.MaxPiece)
	if err != nil {
		return 0, false, err
	}
	fl.next++
	fl.inFlight, fl.isRoot = true, root
	return len(piece), root, nil
}

var emptyBlock = [emptyBlockSize]byte{4: 1}

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
		t.runs = append([]*run{fl.frozen}, t.runs...)
	}
	if t.frozen != nil {
		t.frozen.unref()
		t.frozen = nil
	}
}

// CheckpointDone reports the manifest naming this flush's runs durable and ends the flush. It
// returns every piece of the runs that manifest no longer names; they are dead once a segment
// header names it.
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

// FlushFailed ends a flush whose checkpoint failed. A frozen memtable not yet written stays
// frozen for the next flush, and runs already durable stay so; it returns the pieces of the run it
// was writing, which nothing names.
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
		fl.frozen.blocks = nil
	}
	return slices.Clone(fl.refs)
}

// LoadRun restores a run from its pieces in order, its root at ref last, before replay applies
// what followed the manifest. Runs may load in any order.
func (t *Tree) LoadRun(ref disklog.BlobRef, pieces [][]byte) error {
	if len(pieces) == 0 {
		return ErrMalformed
	}
	_, seq, refs, err := splitPiece(pieces[len(pieces)-1], nil)
	if err != nil {
		return err
	}
	if len(refs) != len(pieces)-1 || slices.ContainsFunc(t.runs, func(r *run) bool { return r.seq == seq }) {
		return ErrMalformed
	}
	r := &run{seq: seq, pieces: append(refs, ref)}
	var last []byte
	for i, p := range pieces {
		block, s, rr, err := splitPiece(p, nil)
		if err != nil {
			return err
		}
		if s != seq || (i < len(pieces)-1 && len(rr) > 0) {
			return ErrMalformed
		}
		n, l, ok := validateBlock(block, last, len(r.blocks) > 0)
		if !ok {
			return ErrMalformed
		}
		last = l
		if n > 0 {
			r.addBlock(bytes.Clone(block))
		}
	}
	at, _ := slices.BinarySearchFunc(t.runs, seq, func(r *run, s uint64) int {
		switch {
		case r.seq > s:
			return -1
		case r.seq < s:
			return 1
		}
		return 0
	})
	t.runs = slices.Insert(t.runs, at, r)
	t.written = append(t.written, r)
	t.nextSeq = max(t.nextSeq, seq+1)
	return nil
}
