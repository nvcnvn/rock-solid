package lsm

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/disklog"
)

const mib = 1 << 20

func defaultOpts() pickOpts {
	return pickOpts{trigger: compactTrigger, ratio: sizeRatio, sizeAmp: maxSizeAmp, minWidth: minMergeWidth, buffer: autoLimitBuffer}
}

// The auto run limit is counted as RocksDB counts it, from the design's examples.
func TestAutoRunLimit(t *testing.T) {
	for _, c := range []struct {
		largest float64
		want    int
	}{
		{1, 1},
		{math.Pow(1.5, 9) * mib, 8},
		{44e6, 8},
		{64 * mib, 8},
		{1.6e9, 13},
		{16 << 30, 16},
	} {
		if got := autoLimit(int(c.largest), autoLimitBuffer, sizeRatio); got != c.want {
			t.Errorf("largest run %.0f B: limit %d, want %d", c.largest, got, c.want)
		}
		if bound := 2 + math.Log2(max(c.largest, mib)/mib); float64(c.want) > bound+1 {
			t.Errorf("largest run %.0f B: limit %d over 2 + log2(largest / 1 MiB) + 1", c.largest, c.want)
		}
	}
	tr, err := New(Config{MaxPiece: 512, RunLimit: 5, Check: true})
	if err != nil {
		t.Fatal(err)
	}
	if tr.runLimit() != 5 {
		t.Fatalf("fixed limit 5 gives %d", tr.runLimit())
	}
	if _, err := New(Config{MaxPiece: 512, RunLimit: -1}); !errors.Is(err, ErrBadConfig) {
		t.Fatalf("negative run limit: %v", err)
	}
}

// flushRuns flushes n runs of one key each, named from prefix, into tr.
func flushRuns(t testing.TB, f *fakeLog, tr *Tree, m model, prefix string, n int) {
	t.Helper()
	for i := range n {
		put(t, tr, m, fmt.Sprint(prefix, i), "v")
		f.checkpoint(t, tr, nil)
	}
}

func seqs(tr *Tree) []uint64 {
	var out []uint64
	for _, r := range tr.runs {
		out = append(out, r.seq)
	}
	return out
}

// A merge running over the oldest runs does not stop a merge of newer runs, the two share no
// run, and installing them in either order leaves the runs in age order.
func TestTwoMergesInstallInEitherOrder(t *testing.T) {
	for _, olderFirst := range []bool{true, false} {
		tr := newTree(t, 512)
		m := model{}
		f := newFakeLog()
		flushRuns(t, f, tr, m, "a", 4)
		older := tr.PickCompaction()
		if older == nil || older.rule != ruleSizeAmp || len(older.inputs) != 4 {
			t.Fatalf("first merge %+v", older)
		}
		if c := tr.PickCompaction(); c != nil {
			t.Fatalf("picked %v over runs the first merge holds", c.rule)
		}
		flushRuns(t, f, tr, m, "b", 4)
		newer := tr.PickCompaction()
		if newer == nil || newer.rule != ruleSizeRatio || len(newer.inputs) != 4 {
			t.Fatalf("second merge %+v", newer)
		}
		for _, r := range newer.inputs {
			if slices.Contains(older.inputs, r) {
				t.Fatalf("the merges share run %d", r.seq)
			}
		}
		older.Run()
		newer.Run()
		first, second := older, newer
		if !olderFirst {
			first, second = newer, older
		}
		for _, c := range []*Compaction{first, second} {
			if err := tr.Install(c); err != nil {
				t.Fatal(err)
			}
			if s := seqs(tr); !slices.IsSortedFunc(s, func(a, b uint64) int { return int(b) - int(a) }) {
				t.Fatalf("runs out of age order: %v", s)
			}
			checkReads(t, tr, m, []string{"a0", "b3", "c"})
		}
		if tr.Runs() != 2 || len(tr.merges) != 0 {
			t.Fatalf("%d runs, %d merges running", tr.Runs(), len(tr.merges))
		}
	}
}

// A crash while a merge runs, before any manifest names its output, restores the previous runs
// plus replay, and the restored tree picks the merge again.
func TestCrashDuringMergePicksItAgain(t *testing.T) {
	tr := newTree(t, 512)
	m := model{}
	f := newFakeLog()
	var roots []disklog.BlobRef
	for i := range 4 {
		put(t, tr, m, fmt.Sprint("k", i), "v")
		roots, _ = f.checkpoint(t, tr, nil)
	}
	c := tr.PickCompaction()
	if c == nil {
		t.Fatal("no merge picked")
	}
	put(t, tr, m, "after", "v")
	c.Run()
	restored := f.restore(t, 512, roots)
	put(t, restored, nil, "after", "v")
	checkReads(t, restored, m, []string{"k0", "after"})
	again := restored.PickCompaction()
	if again == nil || again.rule != c.rule || len(again.inputs) != len(c.inputs) || again.inputs[0].seq != c.inputs[0].seq {
		t.Fatalf("restored tree picked %+v, want %+v", again, c)
	}
}

// treeWithRuns makes a tree with a fixed run limit and n flushed runs of one key each.
func treeWithRuns(t *testing.T, limit, n int) *Tree {
	t.Helper()
	tr, err := New(Config{MaxPiece: 512, RunLimit: limit, Check: true})
	if err != nil {
		t.Fatal(err)
	}
	flushRuns(t, newFakeLog(), tr, nil, "k", n)
	return tr
}

// The scheduler starts the tree whose runs most exceed its limit in proportion first, as
// RocksDB scores universal compaction (version_set.cc:3601).
func TestSchedulerOrder(t *testing.T) {
	for _, c := range []struct {
		name          string
		aRuns, aLimit int
		bRuns, bLimit int
	}{
		{"A ahead by either measure", 10, 8, 14, 13},
		{"A ahead by ratio, B by runs over its limit", 6, 4, 16, 13},
	} {
		t.Run(c.name, func(t *testing.T) {
			a, b := treeWithRuns(t, c.aLimit, c.aRuns), treeWithRuns(t, c.bLimit, c.bRuns)
			s, err := NewScheduler(SchedulerConfig{})
			if err != nil {
				t.Fatal(err)
			}
			s.Add(b)
			s.Add(a)
			first := s.Next()
			if first == nil || first.tree != a {
				t.Fatalf("started %+v first, want index A's merge", first)
			}
			if s.Next() != nil {
				t.Fatal("a second merge started with one worker")
			}
		})
	}
}

// One merge worker by default: index B's merge waits until index A's installs; with two, both run.
func TestSchedulerWorkers(t *testing.T) {
	for _, workers := range []int{-1, disklog.MaxMergeWorkers + 1} {
		if _, err := NewScheduler(SchedulerConfig{MergeWorkers: workers}); !errors.Is(err, ErrBadConfig) {
			t.Fatalf("%d workers: %v", workers, err)
		}
	}
	for _, workers := range []int{0, 2} {
		a, b := treeWithRuns(t, 0, 6), treeWithRuns(t, 0, 4)
		s, err := NewScheduler(SchedulerConfig{MergeWorkers: workers})
		if err != nil {
			t.Fatal(err)
		}
		s.Add(a)
		s.Add(b)
		ca := s.Next()
		if ca == nil || ca.tree != a {
			t.Fatalf("workers %d: first merge %+v", workers, ca)
		}
		cb := s.Next()
		if workers == 0 {
			if cb != nil {
				t.Fatal("index B's merge started while index A's runs on the one worker")
			}
			ca.Run()
			if err := s.Install(ca); err != nil {
				t.Fatal(err)
			}
			cb = s.Next()
		}
		want := 1
		if workers == 2 {
			want = 2
		}
		if cb == nil || cb.tree != b || s.Running() != want {
			t.Fatalf("workers %d: second merge %+v, %d running", workers, cb, s.Running())
		}
	}
}

// holds with forward cursors agrees with a fresh search of every older run for each key, the
// keys given in increasing order with repeats, as a flush retries the entry that ends a block.
func TestHoldsForwardCursors(t *testing.T) {
	r := rand.New(rand.NewPCG(21, 0))
	for range 100 {
		f := newFakeLog()
		tr := newTree(t, 256+r.IntN(400))
		for range 1 + r.IntN(4) {
			for range r.IntN(60) {
				put(t, tr, nil, randKey(r, 1+r.IntN(3), 4), "v")
			}
			f.checkpoint(t, tr, nil)
		}
		older := make([]runIter, len(tr.runs))
		for i, run := range tr.runs {
			older[i].init(run.blocks)
		}
		var keys []string
		for range r.IntN(80) {
			keys = append(keys, randKey(r, 1+r.IntN(3), 4))
		}
		slices.Sort(keys)
		var probe blockIter
		for _, k := range keys {
			want := false
			for _, run := range tr.runs {
				if _, ok := run.blocks.get(&probe, []byte(k)); ok {
					want = true
				}
			}
			for range 1 + r.IntN(2) {
				if got := holds(older, []byte(k)); got != want {
					t.Fatalf("holds %q = %v, want %v", k, got, want)
				}
			}
		}
	}
}

// maxRunBytes leaves a block short of full by at most one entry: with 1 MiB pieces a run holds
// about 56.8 GiB, and only pieces too small for two of the largest entries fall back to half full.
func TestMaxRunBytes(t *testing.T) {
	big, small := newTree(t, 1<<20), newTree(t, 512)
	if got := big.maxRunBytes(); got < 56<<30 || got > 57<<30 {
		t.Fatalf("1 MiB pieces: %d bytes", got)
	}
	room := 512 - pieceOverhead
	if got, want := small.maxRunBytes(), ((room-8)/disklog.BlobRefSize-1)*(room/2); got != want {
		t.Fatalf("512-byte pieces: %d bytes, want %d", got, want)
	}
}

// workload is a sequence of checkpoint flushes over an index of keys live keys, each flush
// holding n uniform random updates of existing keys (the LSM compaction study's seq.go).
type workload struct {
	name    string
	keys    int
	entryB  float64
	flushes []int
}

func sequences(keys int, entryB float64, small, large, flushes int, seed uint64) []workload {
	rng := rand.New(rand.NewPCG(seed, 1))
	mk := func(name string, f func(i int) int) workload {
		w := workload{name: name, keys: keys, entryB: entryB}
		for i := range flushes {
			w.flushes = append(w.flushes, f(i))
		}
		return w
	}
	lo, hi := math.Log(float64(small)/10), math.Log(float64(large))
	return []workload{
		mk("steady", func(int) int { return small }),
		mk("alternating", func(i int) int {
			if i%2 == 0 {
				return small
			}
			return large
		}),
		mk("bursty", func(int) int {
			if rng.Float64() < 0.2 {
				return large
			}
			return small
		}),
		mk("loguniform", func(int) int { return int(math.Exp(lo + rng.Float64()*(hi-lo))) }),
	}
}

type seqResult struct {
	runsMean            float64
	runsMax             int
	flushedB, rewritten float64
	spaceMean, peak     float64
}

func (r seqResult) wa() float64 { return (r.flushedB + r.rewritten) / r.flushedB }

// simulate replays w through the picker in the study's size model: a run is the fraction of the
// key space it covers, a flush of n entries over K keys covering 1-exp(-n/K) and a merge the union
// of its inputs; a merge's output counts as rewritten only if it survives to the next checkpoint.
func simulate(w workload, o pickOpts) seqResult {
	type mrun struct {
		cover   float64
		pending bool
	}
	live := float64(w.keys) * w.entryB
	size := func(r mrun) float64 { return r.cover * live }
	runs := []mrun{{cover: 1}}
	var res seqResult
	half, samples := len(w.flushes)/2, 0
	for fi, n := range w.flushes {
		measure := fi >= half
		r := mrun{cover: 1 - math.Exp(-float64(n)/float64(w.keys))}
		runs = append([]mrun{r}, runs...)
		if measure {
			res.flushedB += size(r)
		}
		for {
			sizes := make([]pickRun, len(runs))
			total := 0.0
			for i, r := range runs {
				sizes[i].bytes = int(size(r))
				total += size(r)
			}
			i, j, rule := pick(sizes, o)
			if rule == ruleNone {
				break
			}
			keep := 1.0
			for _, r := range runs[i:j] {
				keep *= 1 - r.cover
			}
			out := mrun{cover: 1 - keep, pending: true}
			if measure {
				for _, r := range runs[i:j] {
					if r.pending {
						res.rewritten -= size(r)
					}
				}
				res.rewritten += size(out)
				res.peak = max(res.peak, (total+size(out))/live)
			}
			runs = append(append(append([]mrun{}, runs[:i]...), out), runs[j:]...)
		}
		for k := range runs {
			runs[k].pending = false
		}
		if measure {
			total := 0.0
			for _, r := range runs {
				total += size(r)
			}
			samples++
			res.runsMean += float64(len(runs))
			res.runsMax = max(res.runsMax, len(runs))
			res.spaceMean += total / live
		}
	}
	res.runsMean /= float64(samples)
	res.spaceMean /= float64(samples)
	return res
}

// The LSM compaction study's sequences M1 to M3 through the picker in its size model keep the
// study's bars: run count, bytes rewritten per flushed byte and the runs' bytes over the live index.
func TestFlushSequencesModel(t *testing.T) {
	type bar struct {
		runsMean float64
		runsMax  int
		wa       float64
		space    float64
	}
	scales := []struct {
		name                        string
		keys, small, large, flushes int
		limit                       int
		bars                        map[string]bar
	}{
		{"M1 44 MB", 1_100_000, 33_000, 825_000, 2000, 0, map[string]bar{
			"steady":      {4.03, 7, 3.65, 1.70},
			"alternating": {2.25, 3, 2.38, 1.29},
			"bursty":      {3.07, 7, 2.27, 1.51},
			"loguniform":  {3.59, 8, 2.37, 1.67},
		}},
		{"M2 1.6 GB", 40_000_000, 33_000, 825_000, 4000, 0, map[string]bar{
			"steady":      {6.46, 12, 6.28, 1.64},
			"alternating": {4.51, 8, 4.48, 1.55},
			"bursty":      {6.22, 13, 4.00, 1.69},
			"loguniform":  {6.51, 13, 4.29, 1.61},
		}},
		{"M2 1.6 GB fixed limit 4", 40_000_000, 33_000, 825_000, 4000, 4, map[string]bar{
			"steady": {3.96, 4, 100.05, 1.64},
		}},
		{"M3 whole-chunk 30 MB", 760_000, 40, 1000, 4000, 0, map[string]bar{
			"steady":      {6.56, 7, 15.31, 1.15},
			"alternating": {6.05, 7, 9.16, 1.55},
			"bursty":      {6.70, 7, 27.10, 1.70},
			"loguniform":  {6.74, 7, 23.88, 1.58},
		}},
	}
	for _, sc := range scales {
		for _, w := range sequences(sc.keys, 40, sc.small, sc.large, sc.flushes, 1) {
			b, ok := sc.bars[w.name]
			if !ok {
				continue
			}
			o := defaultOpts()
			o.limit = sc.limit
			res := simulate(w, o)
			t.Logf("%s %s: runs %.2f / %d, WA %.2f, space %.2f, peak %.2f", sc.name, w.name, res.runsMean, res.runsMax, res.wa(), res.spaceMean, res.peak)
			if res.runsMax > b.runsMax || !near(res.runsMean, b.runsMean, 0.02) || !near(res.wa(), b.wa, 0.02) || !near(res.spaceMean, b.space, 0.02) {
				t.Errorf("%s %s: runs %.2f / %d, WA %.2f, space %.2f; the study's %.2f / %d, %.2f, %.2f",
					sc.name, w.name, res.runsMean, res.runsMax, res.wa(), res.spaceMean, b.runsMean, b.runsMax, b.wa, b.space)
			}
		}
	}
}

func near(got, want, tolerance float64) bool { return math.Abs(got-want) <= tolerance*want }

// The alternating, bursty and log-uniform sequences through the real tree, at a 33rd of the
// study's M1 bytes in 400-byte entries with the auto limit's buffer scaled alike, and in pieces
// large enough that a root lists every merge's output: after each flush the runs never exceed the
// auto limit plus that flush, and after its merges the limit, which is within
// 2 + log2(largest run / buffer) + 1; the runs' bytes and the peak during a merge stay within the
// study's bounds, and mean runs and bytes rewritten land within 25% of the size model's.
func TestFlushSequencesRealTree(t *testing.T) {
	const (
		keys     = 3_300
		valueB   = 384
		maxPiece = 64 << 10
		flushes  = 2000
	)
	if testing.Short() {
		t.Skip("replays 6,000 checkpoints")
	}
	for _, w := range sequences(keys, 0, 99, 2475, flushes, 1)[1:] {
		tr := newTree(t, maxPiece)
		rng := rand.New(rand.NewPCG(1, 2))
		kb, vb := make([]byte, 16), make([]byte, valueB)
		key := func(id uint64) []byte {
			binary.BigEndian.PutUint64(kb[0:8], id*0x9E3779B97F4A7C15)
			binary.BigEndian.PutUint64(kb[8:16], id)
			return kb
		}
		var ver uint64
		putAt := func(id uint64) {
			ver++
			binary.LittleEndian.PutUint64(vb, ver)
			if err := tr.Put(key(id), vb); err != nil {
				t.Fatal(err)
			}
		}
		buf := make([]byte, maxPiece)
		var next uint64
		checkpoint := func() (all, mem int) {
			froze, err := tr.BeginFlush()
			if err != nil {
				t.Fatal(err)
			}
			cur, last := 0, 0
			for {
				n, root, err := tr.NextPiece(buf)
				if err != nil {
					t.Fatal(err)
				}
				if n == 0 {
					break
				}
				next++
				cur += n
				if err := tr.PieceDurable(disklog.BlobRef{Segment: 1, Block: uint32(next), LSN: next}); err != nil {
					t.Fatal(err)
				}
				if root {
					all, last, cur = all+cur, cur, 0
				}
			}
			if _, err := tr.CheckpointDone(); err != nil {
				t.Fatal(err)
			}
			if froze {
				mem = last
			}
			return all, mem
		}
		for id := range uint64(keys) {
			putAt(id)
		}
		checkpoint()
		live := float64(tr.RunBytes())
		tr.limitBuffer = int(live * mib / 44e6)
		var res seqResult
		half, samples := flushes/2, 0
		for fi, n := range w.flushes {
			measure := fi >= half
			for range n {
				putAt(rng.Uint64N(keys))
			}
			all, mem := checkpoint()
			if measure {
				res.flushedB += float64(mem)
			}
			if fi > half {
				res.rewritten += float64(all - mem)
			}
			if limit := tr.runLimit(); tr.Runs() > limit+1 {
				t.Fatalf("%s flush %d: %d runs, limit %d", w.name, fi, tr.Runs(), limit)
			}
			for {
				c := tr.PickCompaction()
				if c == nil {
					break
				}
				c.Run()
				if measure {
					res.peak = max(res.peak, float64(tr.RunBytes()+c.out.bytes)/live)
				}
				if err := tr.Install(c); err != nil {
					t.Fatal(err)
				}
			}
			largest := 0
			for _, r := range tr.runs {
				largest = max(largest, r.bytes)
			}
			bound := 2 + math.Log2(max(float64(largest), float64(tr.limitBuffer))/float64(tr.limitBuffer)) + 1
			if limit := tr.runLimit(); tr.Runs() > limit || float64(limit) > bound {
				t.Fatalf("%s flush %d: %d runs, limit %d, bound %.1f", w.name, fi, tr.Runs(), limit, bound)
			}
			if measure {
				samples++
				res.runsMean += float64(tr.Runs())
				res.runsMax = max(res.runsMax, tr.Runs())
				res.spaceMean += float64(tr.RunBytes()) / live
			}
		}
		all, mem := checkpoint()
		res.rewritten += float64(all - mem)
		res.runsMean /= float64(samples)
		res.spaceMean /= float64(samples)

		mw := w
		mw.entryB = live / keys
		o := defaultOpts()
		o.buffer = tr.limitBuffer
		model := simulate(mw, o)
		t.Logf("%s: runs %.2f / %d, WA %.2f, space %.2f, peak %.2f; model %.2f / %d, %.2f, %.2f, %.2f", w.name,
			res.runsMean, res.runsMax, res.wa(), res.spaceMean, res.peak, model.runsMean, model.runsMax, model.wa(), model.spaceMean, model.peak)
		if !near(res.runsMean, model.runsMean, 0.25) || !near(res.wa(), model.wa(), 0.25) {
			t.Errorf("%s: runs %.2f, WA %.2f; the model's %.2f, %.2f", w.name, res.runsMean, res.wa(), model.runsMean, model.wa())
		}
		if res.spaceMean < 1.29*0.9 || res.spaceMean > 1.70*1.1 || res.peak > 4.5 {
			t.Errorf("%s: runs' bytes %.2f of the live index on average, peak %.2f", w.name, res.spaceMean, res.peak)
		}
	}
}
