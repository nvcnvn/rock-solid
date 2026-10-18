package lsm

import (
	"bytes"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// FuzzLoadRun feeds arbitrary bytes as a one-piece run: loading never panics, and a run it accepts
// iterates in strictly increasing key order.
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
	f.Fuzz(func(t *testing.T, piece []byte) {
		root := disklog.BlobRef{Segment: 1, Block: 1, LSN: 1}
		if _, err := RootRefs(piece, nil); err != nil {
			return
		}
		tr := newTree(t, 512)
		if tr.LoadRun(root, [][]byte{piece}) != nil {
			return
		}
		var prev []byte
		var it Iter
		tr.Iter(&it)
		for ok := it.SeekGE(nil); ok; ok = it.Next() {
			if prev != nil && bytes.Compare(it.Key(), prev) <= 0 {
				t.Fatalf("key %q after %q", it.Key(), prev)
			}
			prev = append(prev[:0], it.Key()...)
			tr.Get(it.Key())
			tr.Floor(it.Key())
		}
	})
}
