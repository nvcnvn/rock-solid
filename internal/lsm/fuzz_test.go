package lsm

import (
	"bytes"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// FuzzLoadRun feeds arbitrary bytes as a one-piece run, of entries or fragments: loading never
// panics, and a run it accepts iterates in strictly increasing key order, and gets and floors of
// its keys find them.
func FuzzLoadRun(f *testing.F) {
	tr := newTree(f, 512)
	for i := range 30 {
		put(f, tr, nil, string(rune('a'+i%26))+string(rune('a'+i/26)), "v")
	}
	fl := newFakeLog()
	roots, _ := fl.checkpoint(f, tr, nil)
	for _, b := range fl.blobs {
		f.Add(b)
	}
	f.Add(fl.blobs[roots[0]][:5])
	for _, r := range [][2]string{{"aa", "ab"}, {"ac", "zz"}} {
		if err := tr.DeleteRange([]byte(r[0]), []byte(r[1])); err != nil {
			f.Fatal(err)
		}
	}
	roots, _ = fl.checkpoint(f, tr, nil)
	f.Add(fl.blobs[roots[len(roots)-1]])
	f.Fuzz(func(t *testing.T, piece []byte) {
		root := disklog.BlobRef{Segment: 1, Block: 1, LSN: 1}
		if _, err := RootRefs(piece, nil); err != nil {
			return
		}
		tr := newTree(t, 512)
		if tr.LoadRun(root, [][]byte{piece}) != nil {
			return
		}
		var fi blockIter
		for _, b := range tr.runs[0].frags {
			fi.init(b)
			for ok := fi.first(); ok; ok = fi.advance() {
				if v, ok := tr.Get(fi.key); ok {
					t.Fatalf("get %q = %q under its fragment", fi.key, v)
				}
			}
		}
		var prev []byte
		var it Iter
		tr.Iter(&it)
		for ok := it.SeekGE(nil); ok; ok = it.Next() {
			if prev != nil && bytes.Compare(it.Key(), prev) <= 0 {
				t.Fatalf("key %q after %q", it.Key(), prev)
			}
			prev = append(prev[:0], it.Key()...)
			if v, ok := tr.Get(it.Key()); !ok || !bytes.Equal(v, it.Value()) {
				t.Fatalf("get %q = %q %v, want %q", it.Key(), v, ok, it.Value())
			}
			if k, v, ok := tr.Floor(it.Key()); !ok || !bytes.Equal(k, it.Key()) || !bytes.Equal(v, it.Value()) {
				t.Fatalf("floor %q = %q %q %v, want itself and %q", it.Key(), k, v, ok, it.Value())
			}
		}
	})
}
