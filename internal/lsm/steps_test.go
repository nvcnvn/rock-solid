package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// stepConfig is a tree of piece-byte pieces whose merges install step bytes at a time.
func stepConfig(piece, step int) Config {
	return Config{MaxPiece: piece, StepBytes: step, Check: true}
}

func stepTree(t testing.TB, piece, step int) *Tree {
	t.Helper()
	tr, err := New(stepConfig(piece, step))
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

// fourRuns flushes four runs over keys k000 to k119, the odd ones in every other run, the second
// newest removing [k015, k100).
func fourRuns(t testing.TB, tr *Tree, f *fakeLog) model { return fourRunsOf(t, tr, f, 120) }

// fourRunsOf is fourRuns over keys k000 up to n.
func fourRunsOf(t testing.TB, tr *Tree, f *fakeLog, n int) model {
	t.Helper()
	m := model{}
	for run := range 4 {
		for i := 0; i < n; i += 1 + run%2 {
			put(t, tr, m, fmt.Sprintf("k%03d", i), fmt.Sprintf("r%d-%03d", run, i))
		}
		if run == 2 {
			delRange(t, tr, m, "k015", "k100")
		}
		f.checkpoint(t, tr, nil)
	}
	return m
}

var stepProbes = []string{"k000", "k014", "k015", "k016", "k040", "k059", "k063", "k099", "k100", "k101", "k119", "k2", "a"}

// lastKey is the last key of the run's entries.
func lastKey(r *run) []byte {
	var it blockIter
	it.init(r.blocks[len(r.blocks)-1])
	for ok := it.first(); ok && it.next < it.restarts; ok = it.advance() {
	}
	return bytes.Clone(it.key)
}

// blockKeys lists the keys of an entry block.
func blockKeys(b []byte) [][]byte {
	var keys [][]byte
	var it blockIter
	it.init(b)
	for ok := it.first(); ok; ok = it.advance() {
		keys = append(keys, bytes.Clone(it.key))
	}
	return keys
}

// stepWorkers is the merge workers of the pieces f took since from, those of the frozen memtable left out.
func stepWorkers(f *fakeLog, from int) []int {
	return slices.DeleteFunc(slices.Clone(f.workers[from:]), func(w int) bool { return w == 0 })
}

// Four runs merged in steps of two pieces (docs/test-scenarios.md, Merge steps): after the first
// step, which installs while a flush writes, the inputs hold only keys above the bound, a range
// removal across it clipped, the output the keys at or below it, a view opened before still
// reads the four runs, and the merge's copies of the input blocks below the bound are gone. That
// flush's manifest names the inputs whole; the next, a manifest alone, the bounded inputs and the
// output so far, and reports dead the input pieces wholly at or below the bound. A crash then
// recovers both and resumes the merge from the bound, each later checkpoint writing one step,
// until the last names the merged run alone; every key reads as before the merge throughout.
func TestMergeInSteps(t *testing.T) {
	const piece = 256
	cfg := stepConfig(piece, 2*piece)
	tr := stepTree(t, piece, 2*piece)
	f := newFakeLog()
	m := fourRunsOf(t, tr, f, 160)
	before := m.clone()
	origs := slices.Clone(tr.runs)
	view := tr.View()
	c := tr.pickSpan(0, 4, ruleNone)
	put(t, tr, m, "new", "v")
	from := len(f.workers)
	_, dead := f.checkpoint(t, tr, func() {
		if c.running {
			c.Run()
			if err := tr.Install(c); err != nil {
				t.Fatal(err)
			}
		}
	})
	if len(f.bounds) != 0 || len(dead) != 0 || len(stepWorkers(f, from)) != 0 {
		t.Fatalf("the flush the step installed during names bounds %+v, reports %d pieces dead, writes merge pieces %v", f.bounds, len(dead), stepWorkers(f, from))
	}
	bound := bytes.Clone(c.bound)
	if bytes.Compare(bound, []byte("k015")) <= 0 || bytes.Compare(bound, []byte("k099")) >= 0 {
		t.Fatalf("the first step ends at %q, not inside the range removal", bound)
	}
	if n := c.output.bytes - len(c.output.filter); n > 2*piece {
		t.Fatalf("the first step wrote %d bytes, step 512", n)
	}
	for _, r := range tr.runs[1:5] {
		if !bytes.Equal(r.lo, bound) || bytes.Compare(r.first, bound) <= 0 {
			t.Fatalf("input %d bounded at %q holds %q", r.seq, r.lo, r.first)
		}
	}
	if out := tr.runs[5]; bytes.Compare(lastKey(out), bound) > 0 || out.seq != origs[3].seq {
		t.Fatalf("output %d ends at %q past the bound", out.seq, lastKey(out))
	}
	if got, want := runFrags(tr.runs[2]), []string{string(bound) + "\x00-k100"}; !slices.Equal(got, want) {
		t.Fatalf("the input's range removal reads %q, want %q", got, want)
	}
	for k, r := range c.runs {
		for i, b := range r.blocks[:c.freed[k]] {
			if b != nil {
				t.Fatalf("the merge keeps block %d of input %d below the bound", i, k)
			}
		}
	}
	if slices.Max(c.freed) == 0 {
		t.Fatal("the first step freed no input block")
	}
	checkReads(t, tr, m, stepProbes)
	checkReads(t, view, before, stepProbes)
	if !slices.Equal(view.st.runs, origs) {
		t.Fatal("the view no longer reads the four runs")
	}
	view.Close()

	from = len(f.workers)
	roots, dead := f.checkpoint(t, tr, nil)
	if w := stepWorkers(f, from); len(w) == 0 || len(w) != len(f.workers)-from || w[0] != c.Worker() {
		t.Fatalf("the manifest alone wrote pieces of workers %v, the merge's %d", f.workers[from:], c.Worker())
	}
	var wantDead []disklog.BlobRef
	for k, in := range tr.runs[1:5] {
		b := f.bounds[k]
		if b.Run != in.root() || !bytes.Equal(b.Key, bound) || int(b.Dead) != in.base || !slices.Contains(roots, in.root()) {
			t.Fatalf("bound %+v on input %d, base %d", b, in.seq, in.base)
		}
		orig := origs[k]
		for _, bl := range orig.blocks[:in.base] {
			if keys := blockKeys(bl); bytes.Compare(keys[len(keys)-1], bound) > 0 {
				t.Fatalf("input %d's piece reported dead holds %q past the bound", in.seq, keys[len(keys)-1])
			}
		}
		if in.base < len(orig.blocks) {
			if keys := blockKeys(orig.blocks[in.base]); bytes.Compare(keys[len(keys)-1], bound) <= 0 {
				t.Fatalf("input %d's first live piece lies wholly at or below the bound", in.seq)
			}
		}
		wantDead = append(wantDead, orig.pieces[:in.base]...)
	}
	if !slices.Contains(roots, c.tail[len(c.tail)-1]) || len(roots) != 6 {
		t.Fatalf("manifest names %d runs, not the frozen run, the four inputs and the output", len(roots))
	}
	if !slices.Equal(dead, wantDead) || len(dead) == 0 {
		t.Fatalf("dead %v, want the pieces wholly at or below the bound %v", dead, wantDead)
	}
	f.drop(dead)

	tr = f.restoreManifest(t, cfg)
	checkReads(t, tr, m, stepProbes)
	c = tr.PickCompaction()
	if c == nil || !bytes.Equal(c.bound, bound) || len(c.inputs) != 4 {
		t.Fatalf("the restored tree resumes %+v, want the merge from %q", c, bound)
	}
	steps := 1
	for ; c != nil; c = tr.PickCompaction() {
		prev := bytes.Clone(c.bound)
		c.Run()
		if c.out != nil && len(c.out.blocks) > 0 && bytes.Compare(c.out.first, prev) <= 0 {
			t.Fatalf("a step starts at %q, at or below the bound %q", c.out.first, prev)
		}
		if err := tr.Install(c); err != nil {
			t.Fatal(err)
		}
		checkReads(t, tr, m, stepProbes)
		from := len(f.workers)
		roots, dead = f.checkpoint(t, tr, nil)
		if w := stepWorkers(f, from); len(w) == 0 || len(f.bounds) != 4 && !c.final {
			t.Fatalf("checkpoint after step %d wrote merge pieces %v and %d bounds", steps+1, w, len(f.bounds))
		}
		f.drop(dead)
		steps++
	}
	if steps < 3 || tr.Runs() != 2 || len(f.bounds) != 0 || len(roots) != 2 || len(tr.merges) != 0 {
		t.Fatalf("after %d steps: %d runs, %d bounds, %d roots", steps, tr.Runs(), len(f.bounds), len(roots))
	}
	live := 0
	for _, r := range tr.runs {
		live += len(r.pieces)
	}
	if len(f.blobs) != live {
		t.Fatalf("%d pieces live, the two runs hold %d", len(f.blobs), live)
	}
	checkReads(t, f.restoreManifest(t, cfg), m, stepProbes)
}

// Bounded runs: with inputs A (newer) and B over keys a to z, B lacking g, and a step ending at f,
// a get of c or f reads the output alone, of q A's value, of r A then B, a scan from a returns
// each key once, and a floor of g returns f from the output; a lookup at or below the bound never
// searches an input, even when every filter says maybe.
func TestBoundedRunsRead(t *testing.T) {
	val := strings.Repeat("x", 30)
	for piece := 220; piece < 400; piece++ {
		tr := stepTree(t, piece, piece)
		f := newFakeLog()
		m := model{}
		for c := 'a'; c <= 'z'; c++ {
			if c != 'g' {
				put(t, tr, m, string(c), "B"+val)
			}
		}
		f.checkpoint(t, tr, nil)
		for _, k := range []string{"c", "f", "q", "x"} {
			put(t, tr, m, k, "A"+val)
		}
		f.checkpoint(t, tr, nil)
		c := tr.pickSpan(0, 2, ruleNone)
		c.Run()
		if c.done || string(c.stepBound) != "f" {
			continue
		}
		if err := tr.Install(c); err != nil {
			t.Fatal(err)
		}
		for _, r := range tr.runs[:2] {
			r.filter = tr.maybeFilter()
		}
		for _, p := range []struct {
			k, v     string
			searches int
		}{{"c", "A", 1}, {"f", "A", 1}, {"a", "B", 1}, {"q", "A", 1}, {"r", "B", 2}} {
			n := tr.Searches()
			if v, ok := tr.Get([]byte(p.k)); !ok || string(v) != p.v+val || tr.Searches()-n != p.searches {
				t.Errorf("get %s = %.1s %v in %d searches, want %s in %d", p.k, v, ok, tr.Searches()-n, p.v, p.searches)
			}
		}
		if k, v, ok := tr.Floor([]byte("g")); !ok || string(k) != "f" || string(v) != "A"+val {
			t.Fatalf("floor g = %q %.1q %v", k, v, ok)
		}
		checkReads(t, tr, m, []string{"a", "c", "f", "g", "q", "r", "z", "zz"})
		return
	}
	t.Fatal("no piece size ends the first step at f")
}

// stepsOf runs a merge of the newest four runs to the end, a checkpoint after each step, and
// returns how many steps it took.
func stepsOf(t testing.TB, tr *Tree, f *fakeLog, m model) int {
	t.Helper()
	t.Helper()
	n := 0
	for c := tr.pickSpan(0, 4, ruleNone); c != nil; c = tr.PickCompaction() {
		c.Run()
		if err := tr.Install(c); err != nil {
			t.Fatal(err)
		}
		checkReads(t, tr, m, stepProbes)
		_, dead := f.checkpoint(t, tr, nil)
		f.drop(dead)
		n++
	}
	return n
}

// A restart between steps resumes the merge from the bound in the manifest: a crash after step
// 2's manifest of a 5-step merge recovers, and the next step starts past step 2's bound; every key
// reads the same before the merge, between steps, after the crash and after the merge.
func TestRestartBetweenSteps(t *testing.T) {
	const piece = 384
	for step := piece; step < 8*piece; step += 32 {
		probe := stepTree(t, piece, step)
		if stepsOf(t, probe, newFakeLog(), fourRunsOf(t, probe, newFakeLog(), 300)) != 5 {
			continue
		}
		cfg := stepConfig(piece, step)
		tr := stepTree(t, piece, step)
		f := newFakeLog()
		m := fourRunsOf(t, tr, f, 300)
		c := tr.pickSpan(0, 4, ruleNone)
		for range 2 {
			c.Run()
			if err := tr.Install(c); err != nil {
				t.Fatal(err)
			}
			checkReads(t, tr, m, stepProbes)
			_, dead := f.checkpoint(t, tr, nil)
			f.drop(dead)
			c = tr.PickCompaction()
		}
		bound := bytes.Clone(c.bound)
		tr = f.restoreManifest(t, cfg)
		checkReads(t, tr, m, stepProbes)
		c = tr.PickCompaction()
		if c == nil || !bytes.Equal(c.bound, bound) {
			t.Fatalf("resumed %+v, want the merge from step 2's bound %q", c, bound)
		}
		c.Run()
		if bytes.Compare(c.out.first, bound) <= 0 {
			t.Fatalf("step 3 starts at %q, at or below step 2's bound %q", c.out.first, bound)
		}
		if err := tr.Install(c); err != nil {
			t.Fatal(err)
		}
		checkReads(t, tr, m, stepProbes)
		_, dead := f.checkpoint(t, tr, nil)
		f.drop(dead)
		for c = tr.PickCompaction(); c != nil; c = tr.PickCompaction() {
			c.Run()
			if err := tr.Install(c); err != nil {
				t.Fatal(err)
			}
			_, dead := f.checkpoint(t, tr, nil)
			f.drop(dead)
		}
		if tr.Runs() != 1 {
			t.Fatalf("%d runs after the merge", tr.Runs())
		}
		checkReads(t, tr, m, stepProbes)
		checkReads(t, f.restoreManifest(t, cfg), m, stepProbes)
		return
	}
	t.Fatal("no step size merges the four runs in 5 steps")
}

// A crash after a step's pieces are durable but before its manifest recovers to the previous
// manifest's bound, and the step is merged again into the same blocks.
func TestCrashBeforeStepManifest(t *testing.T) {
	const piece = 256
	cfg := stepConfig(piece, 2*piece)
	tr := stepTree(t, piece, 2*piece)
	f := newFakeLog()
	m := fourRuns(t, tr, f)
	c := tr.pickSpan(0, 4, ruleNone)
	c.Run()
	if err := tr.Install(c); err != nil {
		t.Fatal(err)
	}
	_, dead := f.checkpoint(t, tr, nil)
	f.drop(dead)
	bound := bytes.Clone(c.bound)
	c = tr.PickCompaction()
	c.Run()
	lost := slices.Clone(c.out.blocks)
	if err := tr.Install(c); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.BeginFlush(); err != nil {
		t.Fatal(err)
	}
	if err := drainFlush(tr, f); err != nil {
		t.Fatal(err)
	}
	tr = f.restoreManifest(t, cfg)
	checkReads(t, tr, m, stepProbes)
	c = tr.PickCompaction()
	if c == nil || !bytes.Equal(c.bound, bound) {
		t.Fatalf("resumed %+v, want the previous manifest's bound %q", c, bound)
	}
	c.Run()
	if !slices.EqualFunc(c.out.blocks, lost, bytes.Equal) {
		t.Fatal("the step merged again differs from the one the crash lost")
	}
}

// A checkpoint that fails while writing a step reports the step's pieces it wrote dead and keeps
// the inputs' bounds, and the next writes the step again; one that fails after the step's root
// is durable reports nothing dead, and the next names the step without writing it again.
func TestFailedCheckpointDuringSteps(t *testing.T) {
	const piece = 256
	for _, rootDurable := range []bool{false, true} {
		tr := stepTree(t, piece, 2*piece)
		f := newFakeLog()
		m := fourRuns(t, tr, f)
		c := tr.pickSpan(0, 4, ruleNone)
		c.Run()
		if err := tr.Install(c); err != nil {
			t.Fatal(err)
		}
		if _, err := tr.BeginFlush(); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, piece)
		var written []disklog.BlobRef
		for {
			n, root, err := tr.NextPiece(buf)
			if err != nil || n == 0 {
				t.Fatalf("piece %d: %v", n, err)
			}
			if root && !rootDurable {
				break
			}
			ref := f.put(buf[:n])
			written = append(written, ref)
			if err := tr.PieceDurable(ref); err != nil {
				t.Fatal(err)
			}
			if root {
				break
			}
		}
		dead := tr.FlushFailed()
		if rootDurable && len(dead) != 0 || !rootDurable && !slices.Equal(dead, written) {
			t.Fatalf("root durable %v: dead %v, wrote %v", rootDurable, dead, written)
		}
		from := len(f.workers)
		f.checkpoint(t, tr, nil)
		if w := stepWorkers(f, from); rootDurable != (len(w) == 0) {
			t.Fatalf("root durable %v: the next checkpoint wrote merge pieces %v", rootDurable, w)
		}
		if len(f.bounds) != 4 || !bytes.Equal(f.bounds[0].Key, c.bound) || !slices.Contains(f.roots, c.tail[len(c.tail)-1]) {
			t.Fatalf("root durable %v: the next checkpoint names bounds %+v", rootDurable, f.bounds)
		}
		checkReads(t, f.restoreManifest(t, stepConfig(piece, 2*piece)), m, stepProbes)
	}
}

// A merge of the newest runs, picked while a stepped merge holds the oldest, runs in its own
// steps; one manifest names both merges' bounds, a restart resumes both, and every key reads the
// same throughout.
func TestTwoMergesInSteps(t *testing.T) {
	const piece = 384
	cfg := stepConfig(piece, piece)
	tr := stepTree(t, piece, piece)
	f := newFakeLog()
	m := fourRuns(t, tr, f)
	older := tr.pickSpan(0, 4, ruleNone)
	older.Run()
	if err := tr.Install(older); err != nil {
		t.Fatal(err)
	}
	for run := range 4 {
		for i := range 60 {
			put(t, tr, m, fmt.Sprintf("n%03d", i), fmt.Sprint("n", run))
		}
		f.checkpoint(t, tr, nil)
	}
	if again := tr.PickCompaction(); again != older {
		t.Fatal("the stepped merge's next step is not picked first")
	}
	newer := tr.PickCompaction()
	if newer == nil || len(newer.inputs) != 4 || newer.inputs[0] != tr.runs[0] || newer.Worker() == older.Worker() {
		t.Fatalf("the second merge %+v", newer)
	}
	for _, c := range []*Compaction{newer, older} {
		c.Run()
		if err := tr.Install(c); err != nil {
			t.Fatal(err)
		}
	}
	checkReads(t, tr, m, append(stepProbes, "n000", "n030"))
	_, dead := f.checkpoint(t, tr, nil)
	f.drop(dead)
	keys := map[string]int{}
	for _, b := range f.bounds {
		keys[string(b.Key)]++
	}
	if len(f.bounds) != 8 || len(keys) != 2 {
		t.Fatalf("one manifest names bounds %+v", f.bounds)
	}
	tr = f.restoreManifest(t, cfg)
	checkReads(t, tr, m, append(stepProbes, "n000", "n030"))
	var cs []*Compaction
	for c := tr.PickCompaction(); c != nil; c = tr.PickCompaction() {
		cs = append(cs, c)
	}
	if len(cs) != 2 || cs[0].Worker() == cs[1].Worker() {
		t.Fatalf("the restored tree resumes %d merges", len(cs))
	}
	for len(tr.merges) > 0 {
		for _, c := range cs {
			if c.running {
				c.Run()
				if err := tr.Install(c); err != nil {
					t.Fatal(err)
				}
			}
		}
		checkReads(t, tr, m, append(stepProbes, "n000", "n030"))
		_, dead := f.checkpoint(t, tr, nil)
		f.drop(dead)
		cs = cs[:0]
		for c := tr.PickCompaction(); c != nil; c = tr.PickCompaction() {
			cs = append(cs, c)
		}
	}
	if tr.Runs() != 2 {
		t.Fatalf("%d runs after both merges", tr.Runs())
	}
	checkReads(t, f.restoreManifest(t, cfg), m, append(stepProbes, "n000", "n030"))
}

// A run check that fails in a later step, a bit flipped in an input above the bound, fails that
// step's install with ErrCheck and leaves the merge never ready again, for the index is rebuilt.
func TestCheckFailsInALaterStep(t *testing.T) {
	const piece = 256
	tr := stepTree(t, piece, 2*piece)
	f := newFakeLog()
	fourRuns(t, tr, f)
	c := tr.pickSpan(0, 4, ruleNone)
	c.Run()
	if err := tr.Install(c); err != nil {
		t.Fatal(err)
	}
	f.checkpoint(t, tr, nil)
	in := tr.runs[3]
	flipLastValue(in.blocks[len(in.blocks)-1])
	if c = tr.PickCompaction(); c == nil {
		t.Fatal("no next step")
	}
	for !c.done && c.err == nil {
		c.Run()
		if c.err == nil {
			if err := tr.Install(c); err != nil {
				t.Fatal(err)
			}
			f.checkpoint(t, tr, nil)
			c = tr.PickCompaction()
		}
	}
	if err := tr.Install(c); !errors.Is(err, ErrCheck) {
		t.Fatalf("install: %v, want ErrCheck", err)
	}
	if !c.failed || tr.PickCompaction() != nil {
		t.Fatal("the failed merge is ready again")
	}
}
