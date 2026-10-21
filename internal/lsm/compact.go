package lsm

import (
	"bytes"
	"math"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// Compaction follows RocksDB's universal compaction (UniversalCompactionBuilder::PickCompaction,
// db/compaction/compaction_picker_universal.cc at v11.8.1): runs span the whole key range, newest
// first, and a merge takes runs adjacent in age, so entries need no sequence number.
const (
	compactTrigger  = 4
	sizeRatio       = 1.0
	maxSizeAmp      = 200.0
	minMergeWidth   = 2
	autoLimitBuffer = 1 << 20
)

type pickOpts struct {
	trigger  int
	ratio    float64
	sizeAmp  float64
	minWidth int
	buffer   int
	limit    int
	maxBytes int
}

type pickRun struct {
	bytes int
	held  bool
}

type rule uint8

const (
	ruleNone rule = iota
	ruleSizeAmp
	ruleSizeRatio
	ruleRunLimit
)

func (r rule) String() string {
	return [...]string{"none", "size amplification", "size ratio", "run limit"}[r]
}

// pick returns the runs [i, j), newest first, that the first rule to apply merges.
func pick(runs []pickRun, o pickOpts) (int, int, rule) {
	if len(runs) == 0 || len(runs) < o.trigger {
		return 0, 0, ruleNone
	}
	if i, j, ok := pickSizeAmp(runs, o.sizeAmp); ok && o.fits(runs[i:j]) {
		return i, j, ruleSizeAmp
	}
	if i, j, ok := pickSizeRatio(runs, o.ratio, o.minWidth, math.MaxInt); ok && o.fits(runs[i:j]) {
		return i, j, ruleSizeRatio
	}
	free, largest := 0, 0
	for _, r := range runs {
		if !r.held {
			free++
		}
		largest = max(largest, r.bytes)
	}
	limit := o.limit
	if limit <= 0 {
		limit = autoLimit(largest, o.buffer, o.ratio)
	}
	if free > limit {
		if i, j, ok := pickSizeRatio(runs, math.Inf(1), o.minWidth, free-limit+1); ok && o.fits(runs[i:j]) {
			return i, j, ruleRunLimit
		}
	}
	return 0, 0, ruleNone
}

func (o pickOpts) fits(span []pickRun) bool {
	if o.maxBytes <= 0 {
		return true
	}
	n := 0
	for _, r := range span {
		n += r.bytes
	}
	return n <= o.maxBytes
}

// pickSizeAmp takes the free runs adjacent to the oldest, and the oldest, once the newer of them
// hold sizeAmp percent of the oldest's bytes (PickCompactionToReduceSizeAmp).
func pickSizeAmp(runs []pickRun, sizeAmp float64) (int, int, bool) {
	end := len(runs) - 1
	if runs[end].held {
		return 0, 0, false
	}
	start, newer := end, 0
	for start > 0 && !runs[start-1].held {
		start--
		newer += runs[start].bytes
	}
	if start == end || float64(newer)*100 < sizeAmp*float64(runs[end].bytes) {
		return 0, 0, false
	}
	return start, end + 1, true
}

// pickSizeRatio walks from the newest free run, taking the next while it is at most ratio
// percent larger than the runs taken so far, up to maxRuns, and returns the first span of at
// least minWidth (PickCompactionToReduceSortedRuns with kCompactionStopStyleTotalSize).
func pickSizeRatio(runs []pickRun, ratio float64, minWidth, maxRuns int) (int, int, bool) {
	minWidth = max(minWidth, 2)
	for start := range runs {
		if runs[start].held {
			continue
		}
		n, taken := 1, runs[start].bytes
		for i := start + 1; n < maxRuns && i < len(runs) && !runs[i].held; i++ {
			if !math.IsInf(ratio, 1) && float64(taken)*(100+ratio)/100 < float64(runs[i].bytes) {
				break
			}
			taken += runs[i].bytes
			n++
		}
		if n >= minWidth {
			return start, start + n, true
		}
	}
	return 0, 0, false
}

// autoLimit is RocksDB's max_read_amp of 0: the runs a size-ratio tree needs for its largest run,
// the first holding buffer bytes and each next ratio percent more than all before it.
func autoLimit(largest, buffer int, ratio float64) int {
	limit := 1
	next, total := float64(buffer), 0.0
	for next < float64(largest) {
		total += next
		next = (100 + ratio) / 100 * total
		limit++
	}
	return limit
}

// Compaction merges adjacent runs into one in steps: Run, which may run on a worker while the
// owner goes on, merges up to Config.StepBytes of output, and Install installs it, bounding the
// inputs below the step's last key; a checkpoint writes each step to the merge worker's lane.
type Compaction struct {
	tree   *Tree
	rule   rule
	worker int
	inputs []*run
	output *run
	runs   []*run
	older  []*run
	plant  plants
	check  bool
	filter FilterKeys
	prefix func([]byte) int
	linear bool
	seq    uint64
	limit  int
	step   int
	bound  []byte

	started      bool
	it, fit      *iterState
	olderIt      []runIter
	olderFrag    []runIter
	f            filterWriter
	baseEntries  int
	baseFrags    int
	outEntries   int
	outFrags     int
	fragsSkipped int
	lastFrag     []byte
	out          *run
	stepBound    []byte
	done         bool
	err          error

	running   bool
	failed    bool
	finished  bool
	final     bool
	installed int
	written   int
	named     int
	freed     []int
	entryRefs []disklog.BlobRef
	fragRefs  []disklog.BlobRef
	tail      []disklog.BlobRef
	stale     []disklog.BlobRef
}

// Worker is the merge worker, from 1, whose lane of the metadata stream the merge's output fills.
func (c *Compaction) Worker() int { return c.worker }

// Finished reports whether the merge is over: its last step named by a durable manifest, or dropped.
func (c *Compaction) Finished() bool { return c.finished }

// ready reports whether the merge may run its next step: the manifest naming its last one is durable.
func (c *Compaction) ready() bool {
	return !c.running && !c.failed && !c.final && !c.finished && c.installed == c.named
}

func (t *Tree) opts() pickOpts {
	return pickOpts{
		trigger:  compactTrigger,
		ratio:    sizeRatio,
		sizeAmp:  maxSizeAmp,
		minWidth: minMergeWidth,
		buffer:   t.limitBuffer,
		limit:    t.cfg.RunLimit,
		maxBytes: t.maxRunBytes(),
	}
}

// PickCompaction returns the next merge step due over runs no other merge holds, or nil: first
// that of a merge whose last step a durable manifest names, a restored one included, then a new merge.
func (t *Tree) PickCompaction() *Compaction {
	t.pairMerges()
	for _, c := range t.merges {
		if c.ready() {
			return t.start(c)
		}
	}
	runs := make([]pickRun, len(t.runs))
	for i, r := range t.runs {
		runs[i] = pickRun{bytes: r.bytes, held: r.held}
	}
	i, j, rule := pick(runs, t.opts())
	if rule == ruleNone {
		return nil
	}
	return t.start(t.compaction(i, j, rule))
}

func (t *Tree) start(c *Compaction) *Compaction {
	c.running = true
	if c.worker == 0 {
		for c.worker = 1; slices.ContainsFunc(t.merges, func(o *Compaction) bool { return o != c && o.worker == c.worker }); c.worker++ {
		}
	}
	return c
}

// compaction holds runs [i, j) for a merge by rule, its output taking the oldest's place and sequence number.
func (t *Tree) compaction(i, j int, rule rule) *Compaction {
	c := &Compaction{
		tree:   t,
		rule:   rule,
		inputs: slices.Clone(t.runs[i:j]),
		seq:    t.runs[j-1].seq,
		limit:  t.cfg.MaxPiece - pieceOverhead,
		step:   t.cfg.StepBytes,
		plant:  t.plant,
		check:  t.cfg.Check,
		filter: t.cfg.Filter,
		prefix: t.cfg.Prefix,
		linear: t.linear,
	}
	for _, r := range c.inputs {
		r.held = true
		in := *r
		in.blocks, in.frags = slices.Clone(r.blocks), slices.Clone(r.frags)
		c.runs = append(c.runs, &in)
	}
	c.freed = make([]int, len(c.runs))
	c.older = t.olderCopies(j)
	t.merges = append(t.merges, c)
	return c
}

// olderCopies is what a merge of runs before j reads of the runs from j on.
func (t *Tree) olderCopies(j int) []*run {
	if t.plant.mergeNoOlder {
		return nil
	}
	var older []*run
	for _, r := range t.runs[j:] {
		older = append(older, &run{blocks: slices.Clone(r.blocks), sums: r.sums})
	}
	return older
}

// pairMerges rebuilds the merges a restored manifest left between steps: each run of bounded
// inputs sharing one bound, followed by the output, which has the oldest input's sequence number.
func (t *Tree) pairMerges() {
	if len(t.resume) == 0 {
		return
	}
	for i := 0; i < len(t.runs); {
		r := t.runs[i]
		if r.lo == nil || r.held {
			i++
			continue
		}
		j := i + 1
		for j < len(t.runs) && t.runs[j].lo != nil && !t.runs[j].held && bytes.Equal(t.runs[j].lo, r.lo) {
			j++
		}
		if j == len(t.runs) || t.runs[j].lo != nil || t.runs[j].seq != t.runs[j-1].seq {
			i = j
			continue
		}
		c := t.compaction(i, j, ruleNone)
		for k, in := range c.inputs {
			c.runs[k] = t.resume[slices.IndexFunc(t.resume, func(x *run) bool { return x.root() == in.root() })]
		}
		out := t.runs[j]
		out.held = true
		nb, nf := len(out.blocks), len(out.frags)
		c.output, c.bound = out, r.lo
		c.entryRefs, c.fragRefs, c.tail = out.pieces[:nb:nb], out.pieces[nb:nb+nf:nb+nf], slices.Clone(out.pieces[nb+nf:])
		c.installed, c.written, c.named = 1, 1, 1
		c.older = t.olderCopies(j + 1)
		t.written = slices.DeleteFunc(t.written, func(w writtenRun) bool { return w.root() == out.root() })
		i = j + 1
	}
	t.resume = nil
}

// runLimit is the most runs the tree keeps before merging the newest whatever their sizes.
func (t *Tree) runLimit() int {
	if t.cfg.RunLimit > 0 {
		return t.cfg.RunLimit
	}
	largest := 0
	for _, r := range t.runs {
		largest = max(largest, r.bytes)
	}
	return autoLimit(largest, t.limitBuffer, sizeRatio)
}

// score is the tree's runs no compaction holds over its run limit, RocksDB's universal score
// (VersionStorageInfo::ComputeCompactionScore), as a fraction.
func (t *Tree) score() (free, limit int) {
	for _, r := range t.runs {
		if !r.held {
			free++
		}
	}
	return free, t.runLimit()
}

// maxRunBytes is the most a run may hold for its root to list every piece. A block is closed only
// when the next entry would not fit, so it is short of full by less than that entry, and two
// adjacent blocks together hold more than a piece since the second starts with that entry; the
// filter fills its pieces. Entries, fragments and the filter may each end in a short piece, so
// two pieces are spare beyond the root, and a merge's filter may round up to one line more than
// its inputs' together.
func (t *Tree) maxRunBytes() int {
	room := t.cfg.MaxPiece - pieceOverhead
	refs := (room - emptyBlockSize) / disklog.BlobRefSize
	grow := min(room-emptyBlockSize, maxEntryBytes) + 4
	return (refs-2)*max(room/2, room-grow) - cacheLineSize
}

var maxEntryBytes = uvarintLen(0) + uvarintLen(MaxKey) + uvarintLen(MaxValue+1) + MaxKey + MaxValue + 1

// Run merges the next step from the inputs: the newest entry of each key wins unless a newer input's fragment covers it, the fragments unite, and a removal or fragment is dropped unless an older run holds a key it hides.
func (c *Compaction) Run() {
	c.out, c.done, c.err = nil, false, nil
	if !c.running {
		c.err = ErrStale
		return
	}
	if !c.started && !c.begin() {
		return
	}
	it := c.it
	out := &run{seq: c.seq}
	buf := make([]byte, 0, c.limit)
	var w blockWriter
	size := 0
	for it.valid() && (c.step == 0 || len(out.blocks) == 0 || size+c.limit <= c.step) {
		w.reset(buf)
		fillBlock(&w, &c.f, it, c.limit, c.olderIt)
		if w.n > 0 {
			b := bytes.Clone(w.finish())
			out.addBlock(b, w.n)
			size += len(b)
		}
	}
	c.done = !it.valid()
	var upTo []byte
	if !c.done {
		c.stepBound = bytes.Clone(it.prev)
		upTo = c.stepBound
	}
	if c.plant.repeatBlock && c.done && len(out.blocks) > 0 {
		out.addBlock(out.blocks[0], 0)
	}
	if !c.plant.dropFrags {
		for c.fit.pendingFrag(c.olderFrag) && (upTo == nil || bytes.Compare(c.fit.ulo, upTo) <= 0) {
			w.reset(buf)
			fillFragBlock(&w, c.fit, c.limit, c.olderFrag, upTo)
			if w.n == 0 {
				panic("lsm: a fragment larger than a piece")
			}
			out.addFrag(bytes.Clone(w.finish()), w.n)
		}
	}
	c.outEntries += out.entries
	c.outFrags += out.nfrags
	if c.done {
		out.setFilter(c.f.finish())
	}
	out.setBounds()
	if !c.plant.noSums {
		out.checksum()
	}
	if c.check {
		c.checkOrder(out)
	}
	bad := anyBad(c.olderIt) || anyBad(c.olderFrag)
	for i := range it.srcs {
		bad = bad || it.srcs[i].ri.bad
	}
	if bad || c.done && (c.outEntries+it.dropped != c.baseEntries || c.outFrags+c.fit.fragsDropped+c.fragsSkipped != c.baseFrags) {
		c.err = ErrCheck
		return
	}
	c.out = out
}

// begin sets up the merge from its bound: its inputs from the start, or, restored between steps,
// from past the bound, with the output's keys in the filter and the fragments it holds skipped.
func (c *Compaction) begin() bool {
	c.started = true
	keys := 0
	for _, r := range c.runs {
		if !c.plant.noSums && !r.fragsIntact() {
			c.err = ErrCheck
			return false
		}
		keys += filterKeyCount(r.filter)
	}
	c.it = &iterState{srcs: make([]source, len(c.runs)), cur: -1, keepDel: true, linear: c.linear}
	c.fit = &iterState{srcs: make([]source, len(c.runs)), cur: -1, linear: c.linear}
	for i, r := range c.runs {
		c.it.srcs[i].setRun(r)
		if !c.plant.noSums {
			c.it.srcs[i].ri.sums = r.sums[:len(r.blocks)]
		}
		c.it.ranges = c.it.ranges || c.it.srcs[i].frags
		c.fit.srcs[i].setRun(r)
		c.baseEntries += entriesAbove(r, c.bound)
		c.baseFrags += r.nfrags
	}
	if c.plant.keepCovered {
		c.it.ranges = false
	}
	c.it.seek(c.bound)
	if c.bound != nil && c.it.valid() && bytes.Equal(c.it.key(), c.bound) {
		c.it.skip()
		c.it.settle()
	}
	c.olderIt, c.olderFrag = runIters(c.older), runIters(c.older)
	c.fit.fragsFromStart()
	for c.bound != nil && c.fit.nextFrag() {
		if bytes.Compare(c.fit.ulo, c.bound) > 0 {
			if c.fit.uok = heldIn(c.olderFrag, c.fit.ulo, c.fit.uhi); !c.fit.uok {
				c.fit.fragsDropped += c.fit.united
			}
			break
		}
		c.fragsSkipped += c.fit.united
	}
	c.f.reset(c.filter, c.prefix, keys)
	if c.output != nil {
		var bi blockIter
		for _, b := range c.output.blocks {
			bi.init(b)
			for ok := bi.first(); ok; ok = bi.advance() {
				c.f.add(bi.key)
			}
		}
	}
	return true
}

// checkOrder panics unless a step's entries and fragments are each in strictly increasing order,
// past the step before, and within the bounds of the inputs.
func (c *Compaction) checkOrder(out *run) {
	prev := bytes.Clone(c.bound)
	for i, b := range out.blocks {
		var ok bool
		if _, prev, ok = validateBlock(b, prev, i > 0 || c.bound != nil); !ok {
			panic("lsm: a merge's output keys are out of order")
		}
	}
	for i, b := range out.frags {
		var ok bool
		if _, c.lastFrag, ok = validateFragBlock(b, c.lastFrag, i > 0 || c.lastFrag != nil); !ok {
			panic("lsm: a merge's output fragments are out of order")
		}
	}
	if !out.bounded {
		return
	}
	var first, last []byte
	for _, r := range c.runs {
		if r.bounded && (first == nil || bytes.Compare(r.first, first) < 0) {
			first = r.first
		}
		if r.bounded && (last == nil || bytes.Compare(r.last, last) > 0) {
			last = r.last
		}
	}
	if first == nil || bytes.Compare(out.first, first) < 0 || bytes.Compare(out.last, last) > 0 {
		panic("lsm: a merge's output lies outside its inputs")
	}
}

// Install installs the step Run merged: the inputs are bounded below its last key and its pieces
// follow the output's, or, at the last step, the output takes the inputs' place. Views opened
// before keep reading what they read.
func (t *Tree) Install(c *Compaction) error {
	if c == nil || !slices.Contains(t.merges, c) || !c.running {
		return ErrStale
	}
	c.running = false
	out, err := c.out, c.err
	c.out, c.err = nil, nil
	if err == nil && out == nil {
		err = ErrStale
	}
	i := slices.Index(t.runs, c.inputs[0])
	n := len(c.inputs)
	if err == nil && (i < 0 || i+n > len(t.runs) || !slices.Equal(t.runs[i:i+n], c.inputs) || c.output != nil && (i+n == len(t.runs) || t.runs[i+n] != c.output)) {
		err = ErrStale
	}
	if err != nil {
		t.failMerge(c)
		return err
	}
	if c.output != nil {
		n++
	}
	if c.done {
		var final *run
		if c.output != nil || len(out.blocks)+len(out.frags) > 0 {
			final = joinRuns(c.output, out, out.filter)
		}
		runs := slices.Delete(slices.Clone(t.runs), i, i+n)
		if final != nil && !t.plant.installOldest {
			runs = slices.Insert(runs, i, final)
		} else if final != nil {
			runs = append(runs, final)
		}
		t.runs, c.output, c.final = runs, final, true
		c.installed++
		if final == nil {
			c.written = c.installed
		}
		t.checkRuns()
		return nil
	}
	runs := slices.Clone(t.runs)
	for k, in := range c.inputs {
		trimmed, err := in.above(c.stepBound)
		if err != nil {
			t.failMerge(c)
			return err
		}
		runs[i+k], c.inputs[k] = trimmed, trimmed
	}
	output := joinRuns(c.output, out, t.maybeFilter())
	if c.output == nil {
		runs = slices.Insert(runs, i+n, output)
	} else {
		runs[i+n-1] = output
	}
	t.runs, c.output, c.bound = runs, output, c.stepBound
	c.installed++
	for k, r := range c.runs {
		from := r.blocks.blockFrom(c.freed[k], c.bound, false)
		for ; c.freed[k] < from; c.freed[k]++ {
			r.blocks[c.freed[k]] = nil
		}
	}
	t.checkRuns()
	return nil
}

// failMerge drops a merge that failed before installing a step, freeing its inputs; one that
// installed steps stays, never ready again, since its index is rebuilt.
func (t *Tree) failMerge(c *Compaction) {
	if c.installed > 0 {
		c.failed = true
		return
	}
	for _, r := range c.inputs {
		r.held = false
	}
	t.merges = slices.DeleteFunc(t.merges, func(o *Compaction) bool { return o == c })
	c.finished = true
}

// maybeFilter is a filter that may hold every key, which a merge's output carries between steps.
func (t *Tree) maybeFilter() []byte {
	var f filterWriter
	f.reset(t.cfg.Filter, t.cfg.Prefix, 0)
	for i := filterKeyHdr; i < len(f.blob)-filterTrailer; i++ {
		f.blob[i] = 0xff
	}
	return f.finish()
}

// SchedulerConfig is a disk's merge scheduler's settings.
type SchedulerConfig struct {
	// MergeWorkers is merge_workers, how many merges run at once, as disklog.MergeWorkers resolves it for the log too.
	MergeWorkers int
}

// Scheduler runs the merges of one disk's trees, the tree whose runs most exceed its run limit in
// proportion first. A merge keeps its worker, and so its lane, until it is finished.
type Scheduler struct {
	workers int
	trees   []*Tree
	running []*Compaction
}

// NewScheduler makes a scheduler with no trees.
func NewScheduler(cfg SchedulerConfig) (*Scheduler, error) {
	workers, ok := disklog.MergeWorkers(cfg.MergeWorkers)
	if !ok {
		return nil, ErrBadConfig
	}
	return &Scheduler{workers: workers}, nil
}

// Add makes the scheduler run t's merges.
func (s *Scheduler) Add(t *Tree) { s.trees = append(s.trees, t) }

// Running is the number of merges holding a worker.
func (s *Scheduler) Running() int {
	s.running = slices.DeleteFunc(s.running, (*Compaction).Finished)
	return len(s.running)
}

// Next returns the merge step to run next, or nil: a running merge's next step once the manifest
// naming its last is durable, else a new merge on the lowest free worker, if one is free.
func (s *Scheduler) Next() *Compaction {
	for _, c := range s.running {
		if c.ready() {
			return c.tree.start(c)
		}
	}
	if s.Running() >= s.workers {
		return nil
	}
	order := slices.Clone(s.trees)
	slices.SortStableFunc(order, func(a, b *Tree) int {
		af, al := a.score()
		bf, bl := b.score()
		return bf*al - af*bl
	})
	for _, t := range order {
		if c := t.PickCompaction(); c != nil {
			if !slices.Contains(s.running, c) {
				for c.worker = 1; slices.ContainsFunc(s.running, func(o *Compaction) bool { return o.worker == c.worker }); c.worker++ {
				}
				s.running = append(s.running, c)
			}
			return c
		}
	}
	return nil
}

// Install installs c's step in its tree; the merge keeps its worker until it is finished.
func (s *Scheduler) Install(c *Compaction) error {
	if c == nil {
		return ErrStale
	}
	return c.tree.Install(c)
}
