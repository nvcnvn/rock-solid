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

// A prefix floor of ascending keys searches each layer once, then steps; a key more than
// forwardSteps entries on searches each layer again, as does one below the last; one iterator
// probes the filters once for its prefix, and none of it allocates. A floor exactly forwardSteps
// entries past each layer's candidate steps; one more searches.
func TestFloorPrefixSteps(t *testing.T) {
	tr, err := New(Config{MaxPiece: 1 << 20, Check: true, Filter: Prefixes, Prefix: chunkPrefix})
	if err != nil {
		t.Fatal(err)
	}
	f := newFakeLog()
	key := func(c, b int) string { return fmt.Sprintf("%08d/%04d", c, b) }
	for layer := range 3 {
		for b := layer; b < 1000; b += 3 {
			put(t, tr, nil, key(1, b), fmt.Sprint(b))
		}
		f.checkpoint(t, tr, nil)
	}
	put(t, tr, nil, key(0, 0), "before")
	put(t, tr, nil, key(2, 0), "after")
	f.checkpoint(t, tr, nil)
	put(t, tr, nil, key(1, 1000), "last")
	var it Iter
	tr.Iter(&it)
	for _, c := range []struct {
		block, searches, filters int
	}{{3, 4, 4}, {4, 0, 0}, {5, 0, 0}, {700, 3, 0}, {700, 0, 0}, {2, 3, 0}, {2 + 3*forwardSteps, 0, 0}, {2 + 3*(2*forwardSteps+1), 3, 0}} {
		s, fp := it.Searches(), it.FilterProbes()
		k, v, ok := it.FloorPrefix([]byte(key(1, c.block)))
		if !ok || string(k) != key(1, c.block) || string(v) != fmt.Sprint(c.block) {
			t.Fatalf("floor of block %d = %q %q %v", c.block, k, v, ok)
		}
		if s, fp = it.Searches()-s, it.FilterProbes()-fp; s != c.searches || fp != c.filters {
			t.Fatalf("floor of block %d: %d searches, %d filter probes, want %d and %d", c.block, s, fp, c.searches, c.filters)
		}
	}
	reads := [][]byte{[]byte(key(1, 3)), []byte(key(1, 4)), []byte(key(1, 700))}
	if a := testing.AllocsPerRun(100, func() {
		tr.Iter(&it)
		for _, k := range reads {
			it.FloorPrefix(k)
		}
	}); a != 0 {
		t.Fatalf("a read of three blocks allocates %v", a)
	}
}
