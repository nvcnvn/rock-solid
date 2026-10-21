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
