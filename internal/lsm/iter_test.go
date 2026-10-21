package lsm

import (
	"fmt"
	"slices"
	"testing"
)

func scanKeys(r reader) []string {
	var got []string
	for _, e := range dump(r) {
		got = append(got, e.k+"="+e.v)
	}
	return got
}

// When several layers hold one key, the newest layer's entry is returned once, and a removal in the
// newest layer hides the key.
func TestHeapMergeTies(t *testing.T) {
	tr := newTree(t, 1<<20)
	f := newFakeLog()
	for i := range 3 {
		put(t, tr, nil, "k", fmt.Sprint("run", i))
		put(t, tr, nil, fmt.Sprint("a", i), "x")
		put(t, tr, nil, fmt.Sprint("z", i), "x")
		f.checkpoint(t, tr, nil)
	}
	put(t, tr, nil, "k", "mem")
	if got, want := scanKeys(tr), []string{"a0=x", "a1=x", "a2=x", "k=mem", "z0=x", "z1=x", "z2=x"}; !slices.Equal(got, want) {
		t.Fatalf("scan %q, want %q", got, want)
	}
	del(t, tr, nil, "k")
	if got, want := scanKeys(tr), []string{"a0=x", "a1=x", "a2=x", "z0=x", "z1=x", "z2=x"}; !slices.Equal(got, want) {
		t.Fatalf("scan after removal %q, want %q", got, want)
	}
}

// A scan that crosses k fragments of one run searches that run's fragments at most k + 1 times.
func TestScanFragmentSearches(t *testing.T) {
	tr := newTree(t, 1<<20)
	f := newFakeLog()
	for i := range 100 {
		put(t, tr, nil, fmt.Sprintf("k%03d", i), "v")
	}
	f.checkpoint(t, tr, nil)
	const k = 10
	for i := range k {
		delRange(t, tr, nil, fmt.Sprintf("k%03d", 10*i+5), fmt.Sprintf("k%03d", 10*i+7))
	}
	f.checkpoint(t, tr, nil)
	var it Iter
	tr.Iter(&it)
	n := 0
	for ok := it.SeekGE(nil); ok; ok = it.Next() {
		n++
	}
	if n != 100-2*k {
		t.Fatalf("scan returned %d keys, want %d", n, 100-2*k)
	}
	if s := it.s.srcs[1].fr.searches; s > k+1 {
		t.Fatalf("scan searched the run's fragments %d times, want at most %d", s, k+1)
	}
}

// A scan over the memtable alone and over no runs works, and a scan that exhausts every source
// reports the end once.
func TestScanEdges(t *testing.T) {
	tr := newTree(t, 1<<20)
	var it Iter
	tr.Iter(&it)
	if it.SeekGE(nil) || it.Next() || it.Valid() {
		t.Fatal("an empty tree's scan found a key")
	}
	put(t, tr, nil, "a", "1")
	put(t, tr, nil, "b", "2")
	if got := scanKeys(tr); !slices.Equal(got, []string{"a=1", "b=2"}) {
		t.Fatalf("memtable scan %q", got)
	}
	newFakeLog().checkpoint(t, tr, nil)
	tr.Iter(&it)
	if !it.SeekGE([]byte("b")) || string(it.Key()) != "b" {
		t.Fatal("seek in a run missed b")
	}
	for range 3 {
		if it.Next() || it.Valid() {
			t.Fatal("an exhausted scan moved on")
		}
	}
}

// benchRuns is a tree of 240,000 random keys spread over n runs and the memtable.
func benchRuns(b *testing.B, n int) *Tree {
	tr := newTree(b, 1<<20)
	f := newFakeLog()
	per := 240_000 / (n + 1)
	for layer := range n + 1 {
		for i := range per {
			put(b, tr, nil, fmt.Sprintf("%08d", (i*(n+1)+layer)*7919%1_000_000_007), "value-of-some-length")
		}
		if layer < n {
			f.checkpoint(b, tr, nil)
		}
	}
	return tr
}

// A scan step over 24 runs costs less than twice the step over 6.
func BenchmarkIterNextRuns(b *testing.B) {
	for _, n := range []int{6, 24} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			tr := benchRuns(b, n)
			if tr.Runs() != n {
				b.Fatalf("%d runs, want %d", tr.Runs(), n)
			}
			var it Iter
			tr.Iter(&it)
			it.SeekGE(nil)
			b.ReportAllocs()
			for b.Loop() {
				if !it.Next() {
					it.SeekGE(nil)
				}
			}
		})
	}
}
