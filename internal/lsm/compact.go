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

// Compaction merges adjacent runs into one; Run may run on a worker while the owner goes on, and the owner installs it.
type Compaction struct {
	tree   *Tree
	rule   rule
	inputs []*run
	runs   []*run
	older  []*run
	plant  plants
	check  bool
	filter FilterKeys
	prefix func([]byte) int
	linear bool
	seq    uint64
	limit  int
	out    *run
	err    error
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

// PickCompaction returns the next compaction due over runs no running compaction holds, or nil.
func (t *Tree) PickCompaction() *Compaction {
	runs := make([]pickRun, len(t.runs))
	for i, r := range t.runs {
		runs[i] = pickRun{bytes: r.bytes, held: r.held}
	}
	i, j, rule := pick(runs, t.opts())
	if rule == ruleNone {
		return nil
	}
	return t.compaction(i, j, rule)
}

// compaction holds runs [i, j) for a merge by rule.
func (t *Tree) compaction(i, j int, rule rule) *Compaction {
	c := &Compaction{
		tree:   t,
		rule:   rule,
		inputs: slices.Clone(t.runs[i:j]),
		seq:    t.runs[i].seq,
		limit:  t.cfg.MaxPiece - pieceOverhead,
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
	for _, r := range t.runs[j:] {
		if !t.plant.mergeNoOlder {
			c.older = append(c.older, &run{blocks: slices.Clone(r.blocks), sums: r.sums})
		}
	}
	t.merges = append(t.merges, c)
	return c
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

// Run merges the inputs: the newest entry of each key wins unless a newer input's fragment covers it, the fragments unite, and a removal or fragment is dropped unless an older run holds a key it hides.
func (c *Compaction) Run() {
	c.out, c.err = nil, nil
	for _, r := range c.runs {
		if !c.plant.noSums && !r.intact() {
			c.err = ErrCheck
			return
		}
	}
	it := iterState{srcs: make([]source, len(c.runs)), cur: -1, keepDel: true, linear: c.linear}
	for i, r := range c.runs {
		it.srcs[i].setRun(r)
		it.ranges = it.ranges || it.srcs[i].frags
	}
	if c.plant.keepCovered {
		it.ranges = false
	}
	it.seek(nil)
	older := runIters(c.older)
	out := &run{seq: c.seq}
	buf := make([]byte, 0, c.limit)
	var w blockWriter
	var f filterWriter
	keys := 0
	for _, r := range c.runs {
		keys += filterKeyCount(r.filter)
	}
	f.reset(c.filter, c.prefix, keys)
	for it.valid() {
		w.reset(buf)
		fillBlock(&w, &f, &it, c.limit, older)
		if w.n > 0 {
			out.addBlock(bytes.Clone(w.finish()), w.n)
		}
	}
	if c.plant.repeatBlock && len(out.blocks) > 0 {
		out.addBlock(out.blocks[0], 0)
	}
	olderFrag := runIters(c.older)
	if !c.plant.dropFrags {
		it.fragsFromStart()
		for it.pendingFrag(olderFrag) {
			w.reset(buf)
			fillFragBlock(&w, &it, c.limit, olderFrag)
			if w.n == 0 {
				panic("lsm: a fragment larger than a piece")
			}
			out.addFrag(bytes.Clone(w.finish()), w.n)
		}
	}
	out.setFilter(f.finish())
	out.setBounds()
	if !c.plant.noSums {
		out.checksum()
	}
	if c.check {
		c.checkOrder(out)
	}
	entries, frags := 0, 0
	for _, r := range c.runs {
		entries += r.entries
		frags += r.nfrags
	}
	if anyBad(older) || anyBad(olderFrag) || out.entries+it.dropped != entries || out.nfrags+it.fragsDropped != frags {
		c.err = ErrCheck
		return
	}
	c.out = out
}

// checkOrder panics unless out's entries and fragments are each in strictly increasing order and
// within the bounds of the inputs.
func (c *Compaction) checkOrder(out *run) {
	var prev []byte
	for i, b := range out.blocks {
		var ok bool
		if _, prev, ok = validateBlock(b, prev, i > 0); !ok {
			panic("lsm: a merge's output keys are out of order")
		}
	}
	prev = nil
	for i, b := range out.frags {
		var ok bool
		if _, prev, ok = validateFragBlock(b, prev, i > 0); !ok {
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

// Install swaps the compaction's result in for its inputs, which views opened before keep reading.
func (t *Tree) Install(c *Compaction) error {
	k := slices.Index(t.merges, c)
	if c == nil || k < 0 {
		return ErrStale
	}
	t.merges = slices.Delete(t.merges, k, k+1)
	for _, r := range c.inputs {
		r.held = false
	}
	if c.err != nil {
		return c.err
	}
	if c.out == nil {
		return ErrStale
	}
	i := slices.Index(t.runs, c.inputs[0])
	if i < 0 || i+len(c.inputs) > len(t.runs) || !slices.Equal(t.runs[i:i+len(c.inputs)], c.inputs) {
		return ErrStale
	}
	runs := make([]*run, 0, len(t.runs)-len(c.inputs)+1)
	runs = append(runs, t.runs[:i]...)
	keep := len(c.out.blocks)+len(c.out.frags) > 0
	if keep && !t.plant.installOldest {
		runs = append(runs, c.out)
	}
	t.runs = append(runs, t.runs[i+len(c.inputs):]...)
	if keep && t.plant.installOldest {
		t.runs = append(t.runs, c.out)
	}
	t.checkRuns()
	return nil
}

// SchedulerConfig is a disk's merge scheduler's settings.
type SchedulerConfig struct {
	// MergeWorkers is merge_workers, how many merges run at once, as disklog.MergeWorkers resolves it for the log too.
	MergeWorkers int
}

// Scheduler runs the merges of one disk's trees, the tree whose runs most exceed its run limit in proportion first.
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

// Running is the number of merges started and not yet installed.
func (s *Scheduler) Running() int { return len(s.running) }

// Next picks the merge to start next, or nil when every worker is busy or no tree has one due.
func (s *Scheduler) Next() *Compaction {
	if len(s.running) >= s.workers {
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
			s.running = append(s.running, c)
			return c
		}
	}
	return nil
}

// Install installs c in its tree and frees its worker.
func (s *Scheduler) Install(c *Compaction) error {
	if c == nil {
		return ErrStale
	}
	if i := slices.Index(s.running, c); i >= 0 {
		s.running = slices.Delete(s.running, i, i+1)
	}
	return c.tree.Install(c)
}
