// Package lsm is the log-structured merge tree behind every index in a disk log: a copy-on-write
// page-tree memtable, immutable prefix-compressed sorted runs held in memory, universal
// compaction, point-in-time views, flushes written to the log one piece at a time and restores
// from the pieces a manifest names.
package lsm

import (
	"bytes"
	"cmp"
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
	// StepBytes is compaction_step_bytes, the most output a merge installs at once; 0 installs a merge whole.
	StepBytes int
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
	written     []writtenRun
	resume      []*run
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
	noFencePrefix bool
}

// writtenRun is a durable run's pieces, its root last, of which the first dead are reported dead.
type writtenRun struct {
	pieces []disklog.BlobRef
	dead   int
}

func (w writtenRun) root() disklog.BlobRef { return w.pieces[len(w.pieces)-1] }

// flush is a checkpoint's writes for the tree: a job per merge step not yet durable, then the
// frozen memtable's, and what its manifest names.
type flush struct {
	carried     []disklog.BlobRef
	bounds      []disklog.MergeBound
	merges      []mergeName
	jobs        []*job
	roots       []disklog.BlobRef
	frozen      *run
	src         iterState
	bw          blockWriter
	older       []runIter
	olderFrag   []runIter
	filter      filterWriter
	entriesDone bool
	cur         int
	inFlight    bool
	isRoot      bool
	written     bool
}

// job writes a run from entry block fromB, fragment block fromF and filter partition fromP on:
// the frozen memtable's whole, or a merge's step, whose root lists the output's earlier pieces too
// and, until the last step, is an empty root of its own, so that no later step lists a root.
type job struct {
	c                   *Compaction
	r                   *run
	fromB, fromF, fromP int
	step                int
	final               bool
	next                int
	refs                []disklog.BlobRef
}

// mergeName is what a checkpoint's manifest names of a merge: its steps up to step, and its
// inputs' roots and first live pieces unless the last step is among them.
type mergeName struct {
	c      *Compaction
	step   int
	final  bool
	inputs []disklog.BlobRef
	bases  []int
}

// New makes an empty tree.
func New(cfg Config) (*Tree, error) {
	if cfg.MaxPiece < MinPiece || cfg.RunLimit < 0 || cfg.StepBytes < 0 || cfg.Filter > KeysAndPrefixes || cfg.Filter != WholeKeys && cfg.Prefix == nil {
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

// BeginFlush starts writing the frozen memtable and each merge step not yet durable, and reports whether it froze the memtable.
func (t *Tree) BeginFlush() (bool, error) {
	if t.fl != nil {
		return false, ErrBusy
	}
	t.pairMerges()
	froze := false
	if t.frozen == nil && (t.mem.count > 0 || t.mem.frags > 0) {
		t.frozen, t.frozenSeq = t.mem, t.nextSeq
		t.nextSeq++
		t.mem = newMemtable(&t.pg)
		froze = true
	}
	fl := &flush{older: runIters(t.runs), olderFrag: runIters(t.runs)}
	for _, r := range t.runs {
		if !slices.ContainsFunc(t.merges, func(c *Compaction) bool { return c.holds(r) }) {
			fl.carried = append(fl.carried, r.root())
		}
	}
	for _, c := range t.merges {
		fl.name(c)
	}
	if t.frozen != nil {
		fl.frozen = &run{seq: t.frozenSeq}
		fl.jobs = append(fl.jobs, &job{r: fl.frozen})
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

// holds reports whether r is one of the merge's inputs or its output.
func (c *Compaction) holds(r *run) bool {
	return r == c.output || !c.final && slices.Contains(c.inputs, r)
}

// name adds what the manifest names of c: the step installed and not yet durable, which the
// flush writes, or else the last durable one, and the inputs bounded by it until the last step.
func (fl *flush) name(c *Compaction) {
	mn := mergeName{c: c, step: c.written, final: c.final && c.written == c.installed}
	switch {
	case c.installed > c.written:
		mn.step, mn.final = c.installed, c.final
		fl.jobs = append(fl.jobs, &job{c: c, r: c.output, fromB: len(c.entryRefs), fromF: len(c.fragRefs), fromP: c.writtenParts, step: c.installed, final: c.final})
	case c.written > 0 && c.output != nil:
		fl.carried = append(fl.carried, c.tail[len(c.tail)-1])
	}
	if !mn.final {
		for _, in := range c.inputs {
			fl.carried = append(fl.carried, in.root())
			mn.inputs = append(mn.inputs, in.root())
			mn.bases = append(mn.bases, in.base)
			if mn.step > 0 {
				fl.bounds = append(fl.bounds, disklog.MergeBound{Run: in.root(), Key: c.bound, Dead: uint32(in.base)})
			}
		}
	}
	fl.merges = append(fl.merges, mn)
}

// CarriedRuns is the root piece of each durable run the running flush does not rewrite.
func (t *Tree) CarriedRuns() []disklog.BlobRef {
	if t.fl == nil {
		return nil
	}
	return t.fl.carried
}

// Bounds is the merge bound of each run the running flush's manifest names bounded, all among CarriedRuns.
func (t *Tree) Bounds() []disklog.MergeBound {
	if t.fl == nil {
		return nil
	}
	return t.fl.bounds
}

// PieceWorker is the merge worker whose lane takes the piece NextPiece built last, through
// disklog's MergePiece, or 0 for a piece of the frozen memtable, which CheckpointPiece takes.
func (t *Tree) PieceWorker() int {
	if fl := t.fl; fl != nil && fl.cur < len(fl.jobs) && fl.jobs[fl.cur].c != nil {
		return fl.jobs[fl.cur].c.worker
	}
	return 0
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
	if fl.cur == len(fl.jobs) {
		t.finishWrite()
		return 0, false, nil
	}
	jb := fl.jobs[fl.cur]
	w := jb.r
	limit := t.cfg.MaxPiece - pieceOverhead
	var block []byte
	var last, inPlace bool
	kind := entryBlock
	if w == fl.frozen && len(w.parts) == 0 {
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
			fillFragBlock(bw, &fl.src, limit, fl.olderFrag, nil)
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
				fl.frozen, fl.jobs = nil, fl.jobs[:fl.cur]
				t.finishWrite()
				return 0, false, nil
			}
			w.addPart(newPart(fl.filter.finish(), nil, false))
		}
	}
	nb, nf := len(w.blocks)-jb.fromB, len(w.frags)-jb.fromF
	if block == nil {
		np := w.filterPieces(jb.fromP, limit)
		total := nb + nf + np
		if jb.c != nil && !jb.final {
			total++
		}
		switch {
		case jb.next < nb:
			block = w.blocks[jb.fromB+jb.next]
		case jb.next < nb+nf:
			block, kind = w.frags[jb.fromF+jb.next-nb], fragBlock
		case jb.next < nb+nf+np:
			block, _, _ = w.filterPiece(jb.fromP, jb.next-nb-nf, limit)
			kind = filterBlock
		default:
			block = emptyBlock[:]
		}
		last = jb.next >= total-1
	}
	if !inPlace && w.sums != nil && !w.pieceIntact(jb.next, jb.fromB, jb.fromF, jb.fromP, limit) {
		return 0, false, ErrCheck
	}
	var earlier []disklog.BlobRef
	if last {
		earlier = jb.earlier(nb, nf)
	}
	piece, root, err := writePiece(buf, block, kind, inPlace, last, w.seq, earlier, t.cfg.MaxPiece)
	if err != nil {
		return 0, false, err
	}
	if t.cfg.Check && inPlace {
		checkPiece(piece, kind, w.seq, fl.bw.taken)
	}
	jb.next++
	fl.inFlight, fl.isRoot = true, root
	return len(piece), root, nil
}

// earlier is the pieces the job's root lists: the run's entry pieces, its fragment pieces and its
// partitions', those of a merge's earlier steps included, nb and nf of the entry and fragment
// pieces the job wrote.
func (jb *job) earlier(nb, nf int) []disklog.BlobRef {
	if jb.c == nil {
		return jb.refs
	}
	nb, nf = min(nb, len(jb.refs)), min(nb+nf, len(jb.refs))
	return slices.Concat(jb.c.entryRefs, jb.refs[:nb], jb.c.fragRefs, jb.refs[nb:nf], jb.c.partRefs, jb.refs[nf:])
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

// checkRuns panics, with Check on, unless the run list is newest first with no sequence number
// twice, but for a merge's oldest bounded input and the output that follows it.
func (t *Tree) checkRuns() {
	if !t.cfg.Check {
		return
	}
	for i := 1; i < len(t.runs); i++ {
		a, b := t.runs[i-1], t.runs[i]
		if b.seq > a.seq || b.seq == a.seq && (a.lo == nil || b.lo != nil) {
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
	jb := fl.jobs[fl.cur]
	if !fl.isRoot {
		jb.refs = append(jb.refs, ref)
		return nil
	}
	w := jb.r
	nb, nf := len(w.blocks)-jb.fromB, len(w.frags)-jb.fromF
	np := min(nb+nf+w.filterPieces(jb.fromP, t.cfg.MaxPiece-pieceOverhead), len(jb.refs))
	all := append(jb.earlier(nb, nf), ref)
	fl.roots = append(fl.roots, ref)
	if c := jb.c; c != nil {
		c.entryRefs = append(c.entryRefs, jb.refs[:nb]...)
		c.fragRefs = append(c.fragRefs, jb.refs[nb:nb+nf]...)
		c.partRefs = append(c.partRefs, jb.refs[nb+nf:np]...)
		c.writtenParts = len(w.parts)
		c.stale = append(c.stale, c.tail...)
		c.tail = append(slices.Clone(jb.refs[np:]), ref)
		c.written = jb.step
		if jb.final {
			w.pieces = all
		}
	} else {
		w.pieces = all
		t.written = append(t.written, writtenRun{pieces: all})
	}
	fl.cur++
	if fl.cur == len(fl.jobs) {
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

// CheckpointDone ends the flush once its manifest is durable and returns the pieces it made dead:
// those of the runs it no longer names, of each input up to its bound, and of a merge's roots a
// later step replaced.
func (t *Tree) CheckpointDone() ([]disklog.BlobRef, error) {
	fl := t.fl
	if fl == nil || !fl.written {
		return nil, ErrNoFlush
	}
	var dead []disklog.BlobRef
	for _, mn := range fl.merges {
		dead = t.nameStep(mn, dead)
	}
	named := append(slices.Clone(fl.carried), fl.roots...)
	keep := t.written[:0]
	for _, w := range t.written {
		if slices.Contains(named, w.root()) {
			keep = append(keep, w)
		} else {
			dead = append(dead, w.pieces[w.dead:]...)
		}
	}
	clear(t.written[len(keep):])
	t.written = keep
	t.fl = nil
	return dead, nil
}

// nameStep records that a durable manifest names a merge up to mn's step, appending to dead what
// that made dead, and ends the merge when the step is its last.
func (t *Tree) nameStep(mn mergeName, dead []disklog.BlobRef) []disklog.BlobRef {
	c := mn.c
	if mn.step <= c.named {
		return dead
	}
	dead = append(dead, c.stale...)
	c.stale = nil
	for k, root := range mn.inputs {
		i := slices.IndexFunc(t.written, func(w writtenRun) bool { return w.root() == root })
		if i >= 0 && t.written[i].dead < mn.bases[k] {
			dead = append(dead, t.written[i].pieces[t.written[i].dead:mn.bases[k]]...)
			t.written[i].dead = mn.bases[k]
		}
	}
	c.named = mn.step
	if mn.final {
		c.finished = true
		t.merges = slices.DeleteFunc(t.merges, func(o *Compaction) bool { return o == c })
		if c.output != nil {
			c.output.held = false
			t.written = append(t.written, writtenRun{pieces: c.output.pieces})
		}
	}
	return dead
}

// FlushFailed ends a failed flush, keeping its frozen memtable and any merge step it did not
// finish writing for the next, and returns the pieces nothing names.
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
	if fl.cur < len(fl.jobs) {
		return slices.Clone(fl.jobs[fl.cur].refs)
	}
	return nil
}

// LoadRun restores a run from its pieces in order, its root at ref last; runs may load in any order.
func (t *Tree) LoadRun(ref disklog.BlobRef, pieces [][]byte) error {
	r, err := t.parseRun(ref, pieces, 0)
	if err != nil {
		return err
	}
	return t.addLoaded(r, nil)
}

// LoadBoundedRun restores a run a merge has consumed up to b.Key from its pieces in order past
// the first b.Dead, which are dead, its root at b.Run last.
func (t *Tree) LoadBoundedRun(b disklog.MergeBound, pieces [][]byte) error {
	raw, err := t.parseRun(b.Run, pieces, int(b.Dead))
	if err != nil {
		return err
	}
	r, err := raw.above(b.Key)
	if err != nil {
		return err
	}
	return t.addLoaded(r, raw)
}

// parseRun decodes a run from its pieces past the first skip, its root at ref last.
func (t *Tree) parseRun(ref disklog.BlobRef, pieces [][]byte, skip int) (*run, error) {
	if len(pieces) == 0 {
		return nil, ErrMalformed
	}
	_, _, seq, refs, err := splitPiece(pieces[len(pieces)-1], nil)
	if err != nil {
		return nil, err
	}
	if len(refs) != len(pieces)-1+skip {
		return nil, ErrMalformed
	}
	r := &run{seq: seq, pieces: append(refs, ref), base: skip}
	var last, end, filter []byte
	sawFilter := false
	for i, p := range pieces {
		block, kind, s, rr, err := splitPiece(p, nil)
		if err != nil {
			return nil, err
		}
		if s != seq || (i < len(pieces)-1 && len(rr) > 0) {
			return nil, ErrMalformed
		}
		if kind == filterBlock {
			filter, sawFilter = append(filter, block...), true
			continue
		}
		if sawFilter && !(i == len(pieces)-1 && bytes.Equal(block, emptyBlock[:])) {
			return nil, ErrMalformed
		}
		if kind == fragBlock {
			n, e, ok := validateFragBlock(block, end, len(r.frags) > 0)
			if !ok {
				return nil, ErrMalformed
			}
			end = e
			if n > 0 {
				r.addFrag(bytes.Clone(block), n)
			}
			continue
		}
		n, l, ok := validateBlock(block, last, len(r.blocks) > 0)
		if !ok || n > 0 && len(r.frags) > 0 {
			return nil, ErrMalformed
		}
		last = l
		if n > 0 {
			r.addBlock(bytes.Clone(block), n)
		}
	}
	parts, ok := parseParts(filter, t.cfg.Filter)
	if !ok || parts[len(parts)-1].fenced && last != nil && bytes.Compare(last, parts[len(parts)-1].fence) > 0 {
		return nil, ErrMalformed
	}
	for _, p := range parts {
		r.addPart(p)
	}
	r.setBounds()
	r.checksum()
	return r, nil
}

// addLoaded puts a restored run in sequence order, a bounded input before the output of its
// merge, which shares its sequence number; raw is a bounded run's whole pieces, for its merge.
func (t *Tree) addLoaded(r, raw *run) error {
	if slices.ContainsFunc(t.runs, func(o *run) bool { return o.seq == r.seq && (o.lo == nil) == (r.lo == nil) }) {
		return ErrMalformed
	}
	at, _ := slices.BinarySearchFunc(t.runs, r, func(a, b *run) int {
		switch {
		case a.seq != b.seq:
			return cmp.Compare(b.seq, a.seq)
		case a.lo != nil && b.lo == nil:
			return -1
		case a.lo == nil && b.lo != nil:
			return 1
		}
		return 0
	})
	if t.plant.loadUnordered {
		at = 0
	}
	t.runs = slices.Insert(t.runs, at, r)
	t.written = append(t.written, writtenRun{pieces: r.pieces, dead: r.base})
	if raw != nil {
		t.resume = append(t.resume, raw)
	}
	t.nextSeq = max(t.nextSeq, r.seq+1)
	return nil
}
