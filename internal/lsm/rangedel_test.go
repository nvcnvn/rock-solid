package lsm

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/disklog"
)

func delRange(t testing.TB, tr *Tree, m model, lo, hi string) {
	t.Helper()
	if err := tr.DeleteRange([]byte(lo), []byte(hi)); err != nil {
		t.Fatal(err)
	}
	for k := range m {
		if k >= lo && k < hi {
			delete(m, k)
		}
	}
}

// memFrags lists the fragments of the tree at froot as "start-end".
func memFrags(m *memtable, froot uint32) []string {
	if !m.hasFrags(froot) {
		return nil
	}
	var c memCursor
	c.init(m, froot)
	var out []string
	for ok := c.seekGE(nil); ok; ok = c.next() {
		out = append(out, string(c.key())+"-"+string(c.value()))
	}
	return out
}

// runFrags lists a run's fragments as "start-end".
func runFrags(r *run) []string {
	var out []string
	var it blockIter
	for _, b := range r.frags {
		it.init(b)
		for ok := it.first(); ok; ok = it.advance() {
			out = append(out, string(it.key)+"-"+string(it.val))
		}
	}
	return out
}

// checkFrags checks the memtable's fragments: sorted, neither overlapping nor abutting, and, unless
// the memtable is mixed, holding only removals.
func checkFrags(t testing.TB, m *memtable) {
	t.Helper()
	if !m.hasFrags(m.froot) {
		return
	}
	checkTree(t, m, m.froot, false)
	var c, e memCursor
	c.init(m, m.froot)
	e.init(m, m.root)
	var prevEnd []byte
	n := 0
	for ok := c.seekGE(nil); ok; ok = c.next() {
		n++
		lo, hi := c.key(), c.value()
		if bytes.Compare(lo, hi) >= 0 || prevEnd != nil && bytes.Compare(lo, prevEnd) <= 0 {
			t.Fatalf("fragment %q-%q after one ending at %q", lo, hi, prevEnd)
		}
		prevEnd = append(prevEnd[:0], hi...)
		for ok := e.seekGE(lo); ok && bytes.Compare(e.key(), hi) < 0; ok = e.next() {
			if e.value()[0] != kindDel && !m.mixed {
				t.Fatalf("entry %q under fragment %q-%q is not a removal", e.key(), lo, hi)
			}
		}
	}
	if n != m.frags {
		t.Fatalf("%d fragments, counted %d", n, m.frags)
	}
}

// countPages counts the pages reachable from root.
func countPages(pg *pages, root uint32) int {
	if root == noPage {
		return 0
	}
	p := pg.get(root)
	if p.kind() == leafPage {
		return 1
	}
	n := 1 + countPages(pg, p.left())
	for i := range p.n() {
		n += countPages(pg, p.child(i))
	}
	return n
}

// The memtable fragment table of the range removals scenarios: fragments that overlap or abut
// merge, a later put cuts its key out, and a range removal turns the memtable's entries into
// removals.
func TestMemtableFragments(t *testing.T) {
	cases := []struct {
		ops    []string
		frags  []string
		absent []string
		reads  model
	}{
		{[]string{"del b e"}, []string{"b-e"}, []string{"c"}, nil},
		{[]string{"put c", "del b e"}, []string{"b-e"}, []string{"c"}, nil},
		{[]string{"del b e", "put c"}, []string{"b-c", "c\x00-e"}, []string{"d"}, model{"c": "c"}},
		{[]string{"del b d", "del c f"}, []string{"b-f"}, nil, nil},
		{[]string{"del b c", "del c d"}, []string{"b-d"}, nil, nil},
		{[]string{"del b e", "del c d"}, []string{"b-e"}, nil, nil},
		{[]string{"del c d", "del a c"}, []string{"a-d"}, nil, nil},
		{[]string{"del b c", "del e f", "del h i", "del a g"}, []string{"a-g", "h-i"}, nil, nil},
		{[]string{"del b e", "put b"}, []string{"b\x00-e"}, nil, model{"b": "b"}},
		{[]string{"del b e", "put d"}, []string{"b-d", "d\x00-e"}, nil, model{"d": "d"}},
	}
	for _, c := range cases {
		tr := newTree(t, 1<<20)
		for _, op := range c.ops {
			f := strings.Fields(op)
			if f[0] == "put" {
				put(t, tr, nil, f[1], f[1])
			} else {
				delRange(t, tr, nil, f[1], f[2])
			}
		}
		if got := memFrags(tr.mem, tr.mem.froot); !slices.Equal(got, c.frags) {
			t.Fatalf("%v: fragments %q, want %q", c.ops, got, c.frags)
		}
		checkFrags(t, tr.mem)
		for _, k := range c.absent {
			if v, ok := tr.Get([]byte(k)); ok {
				t.Fatalf("%v: get %s = %q", c.ops, k, v)
			}
		}
		for k, want := range c.reads {
			if v, ok := tr.Get([]byte(k)); !ok || string(v) != want {
				t.Fatalf("%v: get %s = %q %v", c.ops, k, v, ok)
			}
		}
	}

	tr := newTree(t, 1<<20)
	put(t, tr, nil, "c", "1")
	delRange(t, tr, nil, "b", "e")
	if v, ok := tr.mem.get(tr.mem.root, []byte("c")); !ok || v[0] != kindDel {
		t.Fatalf("c is %q %v after the range removal, want a removal", v, ok)
	}
	newFakeLog().checkpoint(t, tr, nil)
	if tr.Runs() != 0 {
		t.Fatal("a flush with nothing older kept the fragment or the removal of c")
	}

	tr = newTree(t, 1<<20)
	f := newFakeLog()
	put(t, tr, nil, "c", "0")
	f.checkpoint(t, tr, nil)
	put(t, tr, nil, "c", "1")
	delRange(t, tr, nil, "b", "e")
	f.checkpoint(t, tr, nil)
	if n, _ := runKinds(tr.runs[0]); n != 0 || !slices.Equal(runFrags(tr.runs[0]), []string{"b-e"}) {
		t.Fatalf("over an older run holding c the flush kept %d entries and fragments %q, want only b-e", n, runFrags(tr.runs[0]))
	}
	if v, ok := tr.Get([]byte("c")); ok {
		t.Fatalf("get c = %q after the flush", v)
	}
}

// A range removal whose start is not below its end is an error and stores nothing.
func TestEmptyRangeRefused(t *testing.T) {
	tr := newTree(t, 1<<20)
	put(t, tr, nil, "c", "1")
	for _, r := range [][2]string{{"c", "c"}, {"d", "c"}, {"", ""}} {
		if err := tr.DeleteRange([]byte(r[0]), []byte(r[1])); !errors.Is(err, ErrEmptyRange) {
			t.Fatalf("DeleteRange(%q, %q): %v", r[0], r[1], err)
		}
	}
	if tr.mem.frags != 0 || tr.mem.froot != noPage {
		t.Fatal("a refused range removal stored a fragment")
	}
	if v, ok := tr.Get([]byte("c")); !ok || string(v) != "1" {
		t.Fatalf("get c = %q %v", v, ok)
	}
}

var rangeConfigs = []Config{
	{MaxPiece: 4096, Check: true},
	{MaxPiece: 256, Check: true},
	{MaxPiece: 1 << 20, Check: true},
	{MaxPiece: 4096, RunLimit: 2, Check: true},
	{MaxPiece: 512, RunLimit: 1, Check: true},
	{MaxPiece: 4096, Check: true, Filter: Prefixes, Prefix: prefix3},
	{MaxPiece: 256, Check: true, Filter: KeysAndPrefixes, Prefix: prefix3},
}

// prefix3 groups the property tests' keys k000 to k299 ten to a prefix; shorter keys have none.
func prefix3(k []byte) int {
	if len(k) < 3 {
		return 0
	}
	return 3
}

// rangeModel runs the LSM runs spike's map-model test on one seed: 3,000 random puts, removals and
// range removals over 300 keys, with flushes, picked compactions and merges of random spans, and a
// view opened now and then, and returns the first read that differs from the map.
func rangeModel(t *testing.T, cfg Config, seed uint64, plant plants) string {
	t.Helper()
	tr, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	tr.plant = plant
	r := rand.New(rand.NewPCG(seed, 7))
	m := model{}
	f := newFakeLog()
	key := func(i int) string { return fmt.Sprintf("k%03d", i) }
	var probes []string
	for i := range 300 {
		probes = append(probes, key(i), key(i)+"~")
	}
	probes = append(probes, "k", "k0", "k29")
	var view *View
	var viewModel model
	merge := func(c *Compaction) error {
		c.Run()
		return tr.Install(c)
	}
	for step := range 3000 {
		var err error
		switch x := r.IntN(100); {
		case x < 55:
			put(t, tr, m, key(r.IntN(300)), fmt.Sprint("v", step))
		case x < 75:
			del(t, tr, m, key(r.IntN(300)))
		case x < 80:
			lo := r.IntN(300)
			delRange(t, tr, m, key(lo), key(lo+1+r.IntN(40)))
		case x < 84:
			f.checkpoint(t, tr, nil)
		case x < 87:
			if c := tr.PickCompaction(); c != nil {
				err = merge(c)
			}
		case x < 89:
			if n := len(tr.runs); n >= 2 {
				i := r.IntN(n - 1)
				err = merge(tr.compaction(i, i+2+r.IntN(n-i-1), ruleNone))
			}
		case x < 90:
			if view == nil {
				view, viewModel = tr.View(), m.clone()
			}
		}
		if err != nil {
			return fmt.Sprintf("step %d: merge: %v", step, err)
		}
		if step%100 == 99 {
			if !plant.noCut {
				checkFrags(t, tr.mem)
			}
			if d := readsDiffer(tr, m, probes); d != "" {
				return fmt.Sprintf("step %d: %s", step, d)
			}
			if view != nil {
				if d := readsDiffer(view, viewModel, probes); d != "" {
					return fmt.Sprintf("view at step %d: %s", step, d)
				}
				view.Close()
				view = nil
			}
		}
	}
	if view != nil {
		view.Close()
	}
	f.checkpoint(t, tr, nil)
	if d := readsDiffer(tr, m, probes); d != "" {
		return "after the last flush: " + d
	}
	if n := countPages(&tr.pg, tr.mem.root) + countPages(&tr.pg, tr.mem.froot); n != tr.pg.inUse {
		t.Fatalf("%d pages reachable, %d in use", n, tr.pg.inUse)
	}
	return ""
}

// [property] Seeds 0-59 under seven configurations, two with prefix filters: gets, floors, prefix
// floors and scans from every probe read as a map.
func TestRangeRemovalsMatchModel(t *testing.T) {
	for i, cfg := range rangeConfigs {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			for seed := range uint64(60) {
				if d := rangeModel(t, cfg, seed, plants{}); d != "" {
					t.Fatalf("%+v seed %d: %s", cfg, seed, d)
				}
			}
		})
	}
}

// The map model fails with a planted merge that drops its inputs' fragments, a merge that keeps an
// older input's entry a newer input's fragment covers, and a memtable that never cuts a put out of
// its fragment.
func TestRangeModelCatchesPlantedBugs(t *testing.T) {
	for _, p := range []struct {
		name  string
		plant plants
	}{
		{"merge drops fragments", plants{dropFrags: true}},
		{"merge keeps covered entries", plants{keepCovered: true}},
		{"no cut", plants{noCut: true}},
	} {
		failed := 0
		for seed := range uint64(60) {
			if rangeModel(t, rangeConfigs[0], seed, p.plant) != "" {
				failed++
			}
		}
		t.Logf("%s: %d of 60 seeds fail", p.name, failed)
		if failed == 0 {
			t.Errorf("%s: no seed fails", p.name)
		}
	}
}

// A view opened before a range removal still reads the keys in it, before and after a flush.
func TestViewBeforeRangeRemoval(t *testing.T) {
	tr := newTree(t, 1<<20)
	m := model{}
	f := newFakeLog()
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		put(t, tr, m, k, k)
	}
	f.checkpoint(t, tr, nil)
	put(t, tr, m, "f", "f")
	v := tr.View()
	before := m.clone()
	delRange(t, tr, m, "b", "g")
	probes := []string{"a", "b", "c", "f", "g", ""}
	checkReads(t, v, before, probes)
	checkReads(t, tr, m, probes)
	f.checkpoint(t, tr, nil)
	checkReads(t, v, before, probes)
	checkReads(t, tr, m, probes)
	v.Close()
}

// MaxKey and MaxValue fit a point entry in half a page, and MaxBound both bounds of a fragment: the
// largest entries and fragments split pages, one byte more is ErrTooLarge, and the block decoder
// follows the same limits.
func TestSizeBounds(t *testing.T) {
	if MaxKey+MaxValue > 2033 || 4+MaxKey+1+MaxValue+2 > pageCap/2 {
		t.Fatalf("an entry of a %d-byte key and a %d-byte value does not fit half a page", MaxKey, MaxValue)
	}
	if 4+2*MaxBound+2 > pageCap/2 {
		t.Fatalf("a fragment with two %d-byte bounds does not fit half a page", MaxBound)
	}
	tr := newTree(t, 1<<20)
	m := model{}
	long := func(i int, fill string, n int) string { return fmt.Sprintf("%04d", i) + strings.Repeat(fill, n-4) }
	for i := range 8 {
		put(t, tr, m, long(2*i+1, "k", MaxKey), strings.Repeat("v", MaxValue))
	}
	if tr.pg.get(tr.mem.root).kind() != innerPage {
		t.Fatal("eight of the largest entries did not split a page")
	}
	checkPages(t, tr.mem, tr.mem.root)
	if err := tr.Put(make([]byte, MaxKey+1), nil); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("a %d-byte key: %v", MaxKey+1, err)
	}
	f := newFakeLog()
	f.checkpoint(t, tr, nil)
	for i := range 8 {
		delRange(t, tr, m, long(2*i+1, "a", MaxBound), long(2*i+1, "z", MaxBound))
	}
	if tr.pg.get(tr.mem.froot).kind() != innerPage {
		t.Fatal("fragments with two of the largest bounds did not split a page")
	}
	checkFrags(t, tr.mem)
	for _, r := range [][2]string{
		{strings.Repeat("a", MaxBound+1), "b"},
		{"a", strings.Repeat("b", MaxBound+1)},
	} {
		if err := tr.DeleteRange([]byte(r[0]), []byte(r[1])); !errors.Is(err, ErrTooLarge) {
			t.Fatalf("a bound over MaxBound: %v", err)
		}
	}
	roots, _ := f.checkpoint(t, tr, nil)
	checkReads(t, tr, m, nil)
	checkReads(t, f.restore(t, 1<<20, roots), m, nil)

	entry := func(k, v int) []byte {
		var w blockWriter
		w.reset(make([]byte, 0, 4096))
		w.add(make([]byte, k), append([]byte{kindSet}, make([]byte, v)...))
		return w.finish()
	}
	if _, _, ok := validateBlock(entry(MaxKey, MaxValue), nil, false); !ok {
		t.Fatal("the decoder refused the largest entry")
	}
	if _, _, ok := validateBlock(entry(MaxKey+1, 0), nil, false); ok {
		t.Fatal("the decoder took a key over MaxKey")
	}
	if _, _, ok := validateBlock(entry(1, MaxValue+1), nil, false); ok {
		t.Fatal("the decoder took a value over MaxValue")
	}
	frag := func(lo, hi int) []byte {
		var w blockWriter
		w.reset(make([]byte, 0, 4096))
		w.add(bytes.Repeat([]byte{'a'}, lo), bytes.Repeat([]byte{'b'}, hi))
		return w.finish()
	}
	if _, _, ok := validateFragBlock(frag(MaxBound, MaxBound), nil, false); !ok {
		t.Fatal("the decoder refused the largest fragment")
	}
	if _, _, ok := validateFragBlock(frag(MaxBound+1, 1), nil, false); ok {
		t.Fatal("the decoder took a start over MaxBound")
	}
	if _, _, ok := validateFragBlock(frag(1, MaxBound+1), nil, false); ok {
		t.Fatal("the decoder took an end over MaxBound")
	}
}

// A put whose cut would leave a fragment bound over MaxBound leaves the fragment whole and the
// memtable mixed, whose own entries then win over its fragments; reads stay right.
func TestCutTooLongForAFragment(t *testing.T) {
	tr := newTree(t, 1<<20)
	m := model{}
	f := newFakeLog()
	put(t, tr, m, "b5", "old")
	f.checkpoint(t, tr, nil)
	delRange(t, tr, m, "b", "c")
	k1 := "b1" + strings.Repeat("x", MaxKey-2)
	put(t, tr, m, k1, "1")
	if !tr.mem.mixed || !slices.Equal(memFrags(tr.mem, tr.mem.froot), []string{"b-c"}) {
		t.Fatalf("mixed %v, fragments %q", tr.mem.mixed, memFrags(tr.mem, tr.mem.froot))
	}
	del(t, tr, m, k1)
	put(t, tr, m, "b7", "7")
	probes := []string{"b", "b1", k1, "b5", "b7", "b8", "c", ""}
	checkReads(t, tr, m, probes)
	f.checkpoint(t, tr, nil)
	checkReads(t, tr, m, probes)
}

// A scan that starts inside a fragment and is bounded inside it returns nothing and searches no
// layer past the bound; unbounded, it moves the older layer past the fragment in one search. The
// memtable holds only the fragment, so its entries are not searched.
func TestScanBoundedInsideFragment(t *testing.T) {
	tr := newTree(t, 1<<20)
	f := newFakeLog()
	for _, k := range []string{"b", "c", "d", "e", "x", "y", "z"} {
		put(t, tr, nil, k, k)
	}
	f.checkpoint(t, tr, nil)
	delRange(t, tr, nil, "b", "y")
	var it Iter
	tr.Iter(&it)
	it.SetUpperBound([]byte("d"))
	if it.SeekGE([]byte("c")) {
		t.Fatalf("bounded scan returned %q", it.Key())
	}
	if it.s.probes != 1 {
		t.Fatalf("bounded scan searched %d times, want the run once", it.s.probes)
	}
	tr.Iter(&it)
	it.s.probes = 0
	if !it.SeekGE([]byte("c")) || string(it.Key()) != "y" {
		t.Fatal("unbounded scan did not land on y")
	}
	if it.s.probes != 2 {
		t.Fatalf("unbounded scan searched %d times, want 2: the run, then the run past the fragment", it.s.probes)
	}
	tr.Iter(&it)
	it.SetUpperBound([]byte("z"))
	var got []string
	for ok := it.SeekGE(nil); ok; ok = it.Next() {
		got = append(got, string(it.Key()))
	}
	if !slices.Equal(got, []string{"y"}) {
		t.Fatalf("scan below z: %q", got)
	}
}

// A range removal over a run's keys, flushed: a get or a floor of a key in it searches each layer
// at most once; a write into the range reads back before and after the flush; a fragment no older
// run has a key in is dropped by the flush.
func TestRangeRemovalOverRun(t *testing.T) {
	tr := newTree(t, 1<<20)
	m := model{}
	f := newFakeLog()
	for i := 1; i <= 9; i++ {
		put(t, tr, m, fmt.Sprint("k", i), "v")
	}
	f.checkpoint(t, tr, nil)
	delRange(t, tr, m, "k3", "k7")
	searchesPerLayer := func(when string) {
		t.Helper()
		layers := 1 + len(tr.runs)
		for _, k := range []string{"k3", "k4", "k5x", "k6"} {
			before := tr.Searches()
			if _, ok := tr.Get([]byte(k)); ok {
				t.Fatalf("%s: get %s found", when, k)
			}
			if n := tr.Searches() - before; n > layers {
				t.Fatalf("%s: get %s searched %d times over %d layers", when, k, n, layers)
			}
			before = tr.Searches()
			if fk, _, ok := tr.Floor([]byte(k)); !ok || string(fk) != "k2" {
				t.Fatalf("%s: floor %s = %q %v, want k2", when, k, fk, ok)
			}
			if n := tr.Searches() - before; n > layers {
				t.Fatalf("%s: floor %s searched %d times over %d layers", when, k, n, layers)
			}
		}
	}
	searchesPerLayer("in the memtable")
	f.checkpoint(t, tr, nil)
	if !slices.Equal(runFrags(tr.runs[0]), []string{"k3-k7"}) {
		t.Fatalf("flushed fragments %q", runFrags(tr.runs[0]))
	}
	searchesPerLayer("flushed")
	put(t, tr, m, "k5", "again")
	checkReads(t, tr, m, []string{"k4", "k5", "k6", "k7"})
	f.checkpoint(t, tr, nil)
	checkReads(t, tr, m, []string{"k4", "k5", "k6", "k7"})

	runs := tr.Runs()
	delRange(t, tr, m, "m", "p")
	f.checkpoint(t, tr, nil)
	if tr.Runs() != runs {
		t.Fatal("a fragment no older run has a key in was flushed")
	}
}

// A run's bounds include its fragments', so a run holding only fragments is searched.
func TestFragmentOnlyRunBounds(t *testing.T) {
	tr := newTree(t, 1<<20)
	f := newFakeLog()
	put(t, tr, nil, "c", "1")
	f.checkpoint(t, tr, nil)
	delRange(t, tr, nil, "a", "x")
	f.checkpoint(t, tr, nil)
	r := tr.runs[0]
	if len(r.blocks) != 0 || string(r.first) != "a" || string(r.last) != "x" {
		t.Fatalf("run of %d entry blocks bounded [%q, %q], want only fragments, [a, x]", len(r.blocks), r.first, r.last)
	}
	if _, ok := tr.Get([]byte("c")); ok {
		t.Fatal("get c found it under the fragment-only run")
	}
}

// compactSpan merges runs [i, j) and installs the result.
func compactSpan(t *testing.T, tr *Tree, i, j int) {
	t.Helper()
	c := tr.compaction(i, j, ruleNone)
	c.Run()
	if err := tr.Install(c); err != nil {
		t.Fatal(err)
	}
}

// Within a run entries win over fragments, so a merge drops an older input's entry that a newer
// input's fragment covers and keeps a newer input's entry over an older input's fragment.
func TestMergeFragments(t *testing.T) {
	build := func(plant plants) *Tree {
		tr := newTree(t, 1<<20)
		tr.plant = plant
		f := newFakeLog()
		put(t, tr, nil, "c", "C")
		put(t, tr, nil, "d", "C")
		f.checkpoint(t, tr, nil)
		put(t, tr, nil, "c", "B")
		f.checkpoint(t, tr, nil)
		delRange(t, tr, nil, "b", "e")
		f.checkpoint(t, tr, nil)
		compactSpan(t, tr, 0, 2)
		return tr
	}
	tr := build(plants{})
	if n, _ := runKinds(tr.runs[0]); n != 0 || !slices.Equal(runFrags(tr.runs[0]), []string{"b-e"}) {
		t.Fatalf("merge of A [b, e) and B c: %d entries, fragments %q", n, runFrags(tr.runs[0]))
	}
	if v, ok := tr.Get([]byte("c")); ok {
		t.Fatalf("get c = %q after the merge", v)
	}
	if v, ok := build(plants{keepCovered: true}).Get([]byte("c")); !ok || string(v) != "B" {
		t.Fatalf("the planted merge read c as %q %v; the scenario no longer catches it", v, ok)
	}

	tr = newTree(t, 1<<20)
	f := newFakeLog()
	put(t, tr, nil, "c", "C")
	put(t, tr, nil, "d", "C")
	f.checkpoint(t, tr, nil)
	delRange(t, tr, nil, "b", "e")
	f.checkpoint(t, tr, nil)
	put(t, tr, nil, "c", "A")
	f.checkpoint(t, tr, nil)
	compactSpan(t, tr, 0, 2)
	if n, _ := runKinds(tr.runs[0]); n != 1 || !slices.Equal(runFrags(tr.runs[0]), []string{"b-e"}) {
		t.Fatalf("merge of A c and B [b, e): %d entries, fragments %q", n, runFrags(tr.runs[0]))
	}
	if v, ok := tr.Get([]byte("c")); !ok || string(v) != "A" {
		t.Fatalf("a run holding c and [b, e) reads c as %q %v", v, ok)
	}
	if _, ok := tr.Get([]byte("d")); ok {
		t.Fatal("get d found the oldest run's d")
	}
	if k, _, ok := tr.Floor([]byte("dd")); !ok || string(k) != "c" {
		t.Fatalf("floor dd = %q %v", k, ok)
	}
}

// Removals arrive at a flush in key order, so each older run is searched once and then stepped
// forward; the output equals what a search per removal gives.
func TestFlushRemovalsForwardCursors(t *testing.T) {
	tr := newTree(t, 1<<20)
	f := newFakeLog()
	key := func(i int) string { return fmt.Sprintf("k%05d", i) }
	for run := range 3 {
		for i := run; i < 10_000; i += 4 {
			put(t, tr, nil, key(i), "v")
		}
		f.checkpoint(t, tr, nil)
	}
	for i := range 10_000 {
		del(t, tr, nil, key(i))
	}
	if _, err := tr.BeginFlush(); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, tr.cfg.MaxPiece)
	for {
		n, _, err := tr.NextPiece(buf)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
		if err := tr.PieceDurable(f.put(buf[:n])); err != nil {
			t.Fatal(err)
		}
	}
	for i := range tr.fl.older {
		if n := tr.fl.older[i].searches; n != 1 {
			t.Fatalf("older run %d searched %d times", i, n)
		}
	}
	if _, err := tr.CheckpointDone(); err != nil {
		t.Fatal(err)
	}
	var probe blockIter
	var want []string
	for i := range 10_000 {
		for _, r := range tr.runs[1:] {
			if _, ok := r.blocks.get(&probe, []byte(key(i))); ok {
				want = append(want, key(i))
				break
			}
		}
	}
	var got []string
	for _, b := range tr.runs[0].blocks {
		probe.init(b)
		for ok := probe.first(); ok; ok = probe.advance() {
			got = append(got, string(probe.key))
		}
	}
	if len(want) != 7500 || !slices.Equal(got, want) {
		t.Fatalf("kept %d removals, a search per removal keeps %d", len(got), len(want))
	}
}

// Keys fs/6/…, fs/7/… and fs/8/… with fs/7/ removed as one range: every fs/7/ key reads absent, the
// others as before, and a later put of fs/7/1/a reads back, in the memtable and flushed.
func TestRemovePrefixAsRange(t *testing.T) {
	tr := newTree(t, 1<<20)
	m := model{}
	f := newFakeLog()
	var probes []string
	for _, v := range []string{"6", "7", "8"} {
		for i := range 50 {
			k := fmt.Sprintf("fs/%s/%d/%c", v, i%7, 'a'+i%5)
			put(t, tr, m, k, k)
			probes = append(probes, k)
		}
	}
	f.checkpoint(t, tr, nil)
	delRange(t, tr, m, "fs/7/", "fs/70")
	checkReads(t, tr, m, probes)
	put(t, tr, m, "fs/7/1/a", "again")
	checkReads(t, tr, m, probes)
	f.checkpoint(t, tr, nil)
	checkReads(t, tr, m, probes)
	compactSpan(t, tr, 0, len(tr.runs))
	checkReads(t, tr, m, probes)
}

// A run with fragments in its format: the entry piece first, then the fragment piece, then the
// filter piece as its root, each marked in its trailer. Loading rejects fragment blocks that are
// truncated, unsorted, overlapping or not marked, a run with no filter or a truncated one, and
// entries or fragments after the filter.
func TestRunFragmentFormat(t *testing.T) {
	tr := newTree(t, 128)
	f := newFakeLog()
	put(t, tr, nil, "c", "0")
	f.checkpoint(t, tr, nil)
	put(t, tr, nil, "a", "1")
	delRange(t, tr, nil, "b", "d")
	roots, _ := f.checkpoint(t, tr, nil)
	roots = roots[len(roots)-1:]
	root := f.blobs[roots[0]]
	refs, err := RootRefs(root, nil)
	if err != nil || len(refs) != 2 {
		t.Fatalf("refs %v: %v", refs, err)
	}
	entries := []byte("\x01\x00" + "\x00\x01\x02a\x011" + "\x00\x00\x00\x00\x01\x00\x00\x00" +
		"\x00" + "\x02\x00\x00\x00\x00\x00\x00\x00" + "\x00\x00")
	if got := f.blobs[refs[0]]; !bytes.Equal(got, entries) {
		t.Fatalf("entry piece\n%q\nwant\n%q", got, entries)
	}
	frags := []byte("\x01\x00" + "\x00\x01\x01bd" + "\x00\x00\x00\x00\x01\x00\x00\x00" +
		"\x01" + "\x02\x00\x00\x00\x00\x00\x00\x00" + "\x00\x00")
	if got := f.blobs[refs[1]]; !bytes.Equal(got, frags) {
		t.Fatalf("fragment piece\n%q\nwant\n%q", got, frags)
	}
	line := make([]byte, 64)
	line[3], line[10], line[17], line[24], line[54], line[60] = 0x40, 0x20, 0x10, 0x08, 0x01, 0x80
	filter := append([]byte("\x01\x00"+"\x00"+"\x01\x00\x00\x00\x00\x00\x00\x00"), line...)
	filter = append(filter, "\x06"+"\x01\x00\x00\x00"...)
	for _, r := range refs {
		filter = disklog.AppendBlobRef(filter, r)
	}
	filter = append(filter, "\x02"+"\x02\x00\x00\x00\x00\x00\x00\x00"+"\x02\x00"...)
	if !bytes.Equal(root, filter) {
		t.Fatalf("filter piece\n%q\nwant\n%q", root, filter)
	}
	piece := func(block string, kind byte, refs []disklog.BlobRef) []byte {
		p := []byte("\x01\x00" + block)
		for _, r := range refs {
			p = disklog.AppendBlobRef(p, r)
		}
		p = append(p, kind)
		p = binary.LittleEndian.AppendUint64(p, 2)
		return binary.LittleEndian.AppendUint16(p, uint16(len(refs)))
	}
	pieces := func(fragBlock string, kind byte) [][]byte {
		return [][]byte{entries, piece(fragBlock, kind, nil), root}
	}
	restarts := "\x00\x00\x00\x00\x01\x00\x00\x00"
	if err := newTree(t, 128).LoadRun(roots[0], pieces("\x00\x01\x01bd"+restarts, fragBlock)); err != nil {
		t.Fatalf("rebuilt pieces: %v", err)
	}
	for name, block := range map[string]string{
		"truncated":   "\x00\x01\x01bd" + restarts[:7],
		"unsorted":    "\x00\x01\x01fg\x00\x01\x01bd" + restarts,
		"overlapping": "\x00\x01\x01bf\x00\x01\x01dg" + restarts,
		"empty":       "\x00\x01\x01db" + restarts,
	} {
		if err := newTree(t, 128).LoadRun(roots[0], pieces(block, fragBlock)); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s fragment block: %v, want ErrMalformed", name, err)
		}
	}
	for _, kind := range []byte{entryBlock, filterBlock, 3} {
		if err := newTree(t, 128).LoadRun(roots[0], pieces("\x00\x01\x01bd"+restarts, kind)); !errors.Is(err, ErrMalformed) {
			t.Errorf("fragment block marked %d: %v, want ErrMalformed", kind, err)
		}
	}
	fragRoot := piece("\x00\x01\x01bd"+restarts, fragBlock, refs[:1])
	shortFilter := piece(string(filter[2:2+9+64+4]), filterBlock, refs)
	for name, ps := range map[string][][]byte{
		"entries after fragments": {piece("\x00\x01\x01bd"+restarts, fragBlock, nil), entries, root},
		"no filter":               {entries, fragRoot},
		"truncated filter":        {entries, frags, shortFilter},
		"fragments after filter":  {entries, piece(string(filter[2:2+9+64+5]), filterBlock, nil), piece("\x00\x01\x01bd"+restarts, fragBlock, refs)},
	} {
		if err := newTree(t, 128).LoadRun(roots[0], ps); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: %v, want ErrMalformed", name, err)
		}
	}
}

// With 262,144 fragments in a memtable, a range removal and a cut each search the memtable once
// and allocate nothing.
func TestRangeRemovalAndCutOneSearch(t *testing.T) {
	for name, op := range map[string]func(*Tree, int){
		"range removal": func(tr *Tree, i int) { fresh(t, tr, i) },
		"cut":           func(tr *Tree, i int) { cutPut(t, tr, i) },
	} {
		tr, i := fragTree(t), 0
		before := tr.pg.searches
		op(tr, i)
		i++
		if n := tr.pg.searches - before; n != 1 {
			t.Errorf("%s searched %d times", name, n)
		}
		if a := testing.AllocsPerRun(1000, func() { op(tr, i); i++ }); a > 0.01 {
			t.Errorf("%s allocates %.3f times", name, a)
		}
	}
}

const benchFrags = 262_144

func fragKey(i, off int) []byte {
	return binary.BigEndian.AppendUint64(make([]byte, 0, 8), uint64(i)<<24|uint64(off))
}

// fragTree is a tree whose memtable holds benchFrags fragments, [i<<24, i<<24 + 1<<20).
func fragTree(t testing.TB) *Tree {
	tr := newTree(t, 1<<20)
	for i := range benchFrags {
		if err := tr.DeleteRange(fragKey(i, 0), fragKey(i, 1<<20)); err != nil {
			t.Fatal(err)
		}
	}
	return tr
}

var lo, hi [8]byte

// fresh removes a new range in the gap after fragment n%benchFrags.
func fresh(t testing.TB, tr *Tree, n int) {
	i, j := n%benchFrags, n/benchFrags
	binary.BigEndian.PutUint64(lo[:], uint64(i)<<24|uint64(1<<21+2*j))
	binary.BigEndian.PutUint64(hi[:], uint64(i)<<24|uint64(1<<21+2*j+1))
	if err := tr.DeleteRange(lo[:], hi[:]); err != nil {
		t.Fatal(err)
	}
}

// cutPut puts a new key inside fragment n%benchFrags.
func cutPut(t testing.TB, tr *Tree, n int) {
	i, j := n%benchFrags, n/benchFrags
	binary.BigEndian.PutUint64(lo[:], uint64(i)<<24|uint64(1+2*j))
	if err := tr.Put(lo[:], lo[:]); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkDeleteRange(b *testing.B) {
	tr := fragTree(b)
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		fresh(b, tr, i*7919)
		i++
	}
}

func BenchmarkCut(b *testing.B) {
	tr := fragTree(b)
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		cutPut(b, tr, i*7919)
		i++
	}
}
