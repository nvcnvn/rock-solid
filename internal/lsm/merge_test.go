package lsm

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
)

// mergesDiffer describes the first step at which the heap merge and the linear merge of r differ,
// over a scan from the start and up to 8 steps from each probe, and from every other probe bounded
// at the probe four after it, or returns "".
func mergesDiffer(r reader, probes []string) string {
	var heap, lin Iter
	keyOf := func(it *Iter, ok bool) []byte {
		if !ok {
			return nil
		}
		return it.Key()
	}
	differ := func(from, upper []byte, steps int) string {
		r.Iter(&heap)
		r.Iter(&lin)
		lin.s.linear = true
		heap.SetUpperBound(upper)
		lin.SetUpperBound(upper)
		hok, lok := heap.SeekGE(from), lin.SeekGE(from)
		for i := 0; i < steps && (hok || lok); i++ {
			if hok != lok || !bytes.Equal(heap.Key(), lin.Key()) || !bytes.Equal(heap.Value(), lin.Value()) {
				return fmt.Sprintf("from %q below %q step %d: heap %q %v, linear %q %v", from, upper, i, keyOf(&heap, hok), hok, keyOf(&lin, lok), lok)
			}
			hok, lok = heap.Next(), lin.Next()
		}
		return ""
	}
	if d := differ(nil, nil, 1<<30); d != "" {
		return d
	}
	for i, p := range probes {
		if d := differ([]byte(p), nil, 8); d != "" {
			return d
		}
		if i%2 == 1 && i+4 < len(probes) {
			if d := differ([]byte(p), []byte(probes[i+4]), 8); d != "" {
				return d
			}
		}
	}
	return ""
}

func scanAll(it *Iter) []kv {
	var out []kv
	for ok := it.SeekGE(nil); ok; ok = it.Next() {
		out = append(out, kv{string(it.Key()), string(it.Value())})
	}
	return out
}

// A key in every layer is returned once with the newest layer's value; a removal in the newest
// layer hides it. A merge of the runs keeps only the newest copy.
func TestScanTies(t *testing.T) {
	tr := newTree(t, 1<<20)
	f := newFakeLog()
	for i := range 4 {
		put(t, tr, nil, "a", fmt.Sprint("a", i))
		put(t, tr, nil, fmt.Sprint("b", i), "b")
		put(t, tr, nil, "c", fmt.Sprint("c", i))
		if i < 3 {
			f.checkpoint(t, tr, nil)
		}
	}
	var it Iter
	tr.Iter(&it)
	want := []kv{{"a", "a3"}, {"b0", "b"}, {"b1", "b"}, {"b2", "b"}, {"b3", "b"}, {"c", "c3"}}
	if got := scanAll(&it); !slices.Equal(got, want) {
		t.Fatalf("scan %v, want %v", got, want)
	}
	del(t, tr, nil, "a")
	tr.Iter(&it)
	if got := scanAll(&it); !slices.Equal(got, want[1:]) {
		t.Fatalf("scan after removing a: %v, want %v", got, want[1:])
	}
	if !it.SeekGE([]byte("a")) || string(it.Key()) != "b0" {
		t.Fatal("seek to the removed key did not land on b0")
	}

	c := tr.compaction(0, 3, ruleNone)
	c.Run()
	if c.err != nil {
		t.Fatal(c.err)
	}
	if err := tr.Install(c); err != nil {
		t.Fatal(err)
	}
	tr.Iter(&it)
	if got := scanAll(&it); !slices.Equal(got, want[1:]) {
		t.Fatalf("scan after merging the runs: %v, want %v", got, want[1:])
	}
	if tr.runs[0].entries != 5 {
		t.Fatalf("merged run holds %d entries, want 5", tr.runs[0].entries)
	}
}

// A scan that crosses k fragments of one layer searches that layer's fragments at most k + 1
// times, in a run and in the memtable.
func TestScanFragmentSearches(t *testing.T) {
	const k = 10
	build := func(flushFrags bool) (*Tree, []kv) {
		tr := newTree(t, 1<<20)
		f := newFakeLog()
		m := model{}
		for i := range 100 {
			put(t, tr, m, fmt.Sprintf("k%03d", i), "v")
		}
		f.checkpoint(t, tr, nil)
		for i := range k {
			delRange(t, tr, m, fmt.Sprintf("k%03d", 10*i+2), fmt.Sprintf("k%03d", 10*i+6))
		}
		if flushFrags {
			f.checkpoint(t, tr, nil)
		}
		return tr, m.sorted()
	}

	tr, want := build(true)
	var it Iter
	tr.Iter(&it)
	if got := scanAll(&it); !slices.Equal(got, want) {
		t.Fatalf("scan %v, want %v", clip(got), clip(want))
	}
	fragRun := &it.s.srcs[1]
	if fragRun.run == nil || len(fragRun.run.frags) == 0 || fragRun.run.nfrags != k {
		t.Fatalf("layer 1 is not a run of %d fragments", k)
	}
	if n := fragRun.fr.searches; n > k+1 {
		t.Fatalf("run's fragments searched %d times over %d fragments", n, k)
	}

	tr, want = build(false)
	tr.Iter(&it)
	before := tr.pg.searches
	if got := scanAll(&it); !slices.Equal(got, want) {
		t.Fatalf("scan %v, want %v", clip(got), clip(want))
	}
	if !it.s.srcs[0].empty {
		t.Fatal("memtable holds entries")
	}
	if n := tr.pg.searches - before; n > k+1 {
		t.Fatalf("memtable's fragments searched %d times over %d fragments", n, k)
	}
}

// A seek and the steps after it allocate nothing over 24 runs with range removals.
func TestSeekAllocatesNothing(t *testing.T) {
	tr := newTree(t, 1<<20)
	f := newFakeLog()
	r := rand.New(rand.NewPCG(3, 3))
	key := func() []byte { return binary.BigEndian.AppendUint64(nil, r.Uint64()) }
	for range 24 {
		for range 1000 {
			put(t, tr, nil, string(key()), "v")
		}
		lo := r.Uint64() >> 1
		if err := tr.DeleteRange(binary.BigEndian.AppendUint64(nil, lo), binary.BigEndian.AppendUint64(nil, lo+1<<56)); err != nil {
			t.Fatal(err)
		}
		f.checkpoint(t, tr, nil)
	}
	seeks := make([][]byte, 64)
	for i := range seeks {
		seeks[i] = key()
	}
	var it Iter
	tr.Iter(&it)
	i := 0
	if a := testing.AllocsPerRun(1000, func() {
		it.SeekGE(seeks[i%len(seeks)])
		for range 20 {
			it.Next()
		}
		i++
	}); a > 0 {
		t.Fatalf("a seek and 20 steps allocate %.3f times", a)
	}
}

// A scan over the memtable alone, over a frozen memtable and no runs, and over an empty tree works,
// and one that runs out reports the end on every later step.
func TestScanFewSources(t *testing.T) {
	tr := newTree(t, 1<<20)
	var it Iter
	tr.Iter(&it)
	if it.SeekGE(nil) || it.Next() || it.Valid() {
		t.Fatal("empty tree scanned an entry")
	}
	m := model{}
	for _, k := range []string{"c", "a", "b"} {
		put(t, tr, m, k, k)
	}
	tr.Iter(&it)
	if got := scanAll(&it); !slices.Equal(got, m.sorted()) {
		t.Fatalf("memtable scan %v", got)
	}
	for range 3 {
		if it.Next() || it.Valid() {
			t.Fatal("scan past the end found an entry")
		}
	}
	newFakeLog().checkpoint(t, tr, func() {
		put(t, tr, m, "d", "d")
		var during Iter
		tr.Iter(&during)
		if got := scanAll(&during); !slices.Equal(got, m.sorted()) {
			t.Fatalf("scan during the flush %v", got)
		}
		if len(during.s.srcs) != 2 {
			t.Fatalf("%d sources during the flush, want the memtable and the frozen one", len(during.s.srcs))
		}
	})
}
