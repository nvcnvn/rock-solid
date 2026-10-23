package lsm

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// joinPieces encodes a run's pieces as one fuzz input, each piece after its u16 length.
func joinPieces(ps [][]byte) []byte {
	var b []byte
	for _, p := range ps {
		b = binary.LittleEndian.AppendUint16(b, uint16(len(p)))
		b = append(b, p...)
	}
	return b
}

func splitPieces(b []byte) [][]byte {
	var ps [][]byte
	for len(b) >= 2 {
		n := min(int(binary.LittleEndian.Uint16(b)), len(b)-2)
		ps = append(ps, b[2:2+n])
		b = b[2+n:]
	}
	return ps
}

// FuzzLoadRun feeds arbitrary bytes as a run's pieces, of entries, fragments and the filter:
// loading never panics, and a run it accepts iterates in strictly increasing key order, floors of
// its keys find them, gets and prefix floors find them unless the filter rules them out, and its
// fragments hide their keys.
func FuzzLoadRun(f *testing.F) {
	cfg := Config{MaxPiece: 512, Check: true, Filter: KeysAndPrefixes, Prefix: func(k []byte) int { return min(len(k), 1) }}
	tr, err := New(cfg)
	if err != nil {
		f.Fatal(err)
	}
	for i := range 30 {
		put(f, tr, nil, string(rune('a'+i%26))+string(rune('a'+i/26)), "v")
	}
	fl := newFakeLog()
	run := func(root disklog.BlobRef) [][]byte {
		refs, err := RootRefs(fl.blobs[root], nil)
		if err != nil {
			f.Fatal(err)
		}
		var ps [][]byte
		for _, r := range append(refs, root) {
			ps = append(ps, fl.blobs[r])
		}
		return ps
	}
	roots, _ := fl.checkpoint(f, tr, nil)
	ps := run(roots[0])
	f.Add(joinPieces(ps))
	f.Add(joinPieces(ps[:len(ps)-1]))
	f.Add(joinPieces([][]byte{ps[0], ps[len(ps)-1][:5]}))
	for _, r := range [][2]string{{"aa", "ab"}, {"ac", "zz"}} {
		if err := tr.DeleteRange([]byte(r[0]), []byte(r[1])); err != nil {
			f.Fatal(err)
		}
	}
	roots, _ = fl.checkpoint(f, tr, nil)
	f.Add(joinPieces(run(roots[len(roots)-1])))
	f.Fuzz(func(t *testing.T, in []byte) {
		root := disklog.BlobRef{Segment: 1, Block: 1, LSN: 1}
		pieces := splitPieces(in)
		if len(pieces) == 0 {
			return
		}
		if _, err := RootRefs(pieces[len(pieces)-1], nil); err != nil {
			return
		}
		tr, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if tr.LoadRun(root, pieces) != nil {
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
			r := tr.runs[0]
			if v, ok := tr.Get(it.Key()); r.mayContain(it.Key(), bloomHash(it.Key())) && (!ok || !bytes.Equal(v, it.Value())) {
				t.Fatalf("get %q = %q %v, want %q", it.Key(), v, ok, it.Value())
			}
			if k, v, ok := tr.Floor(it.Key()); !ok || !bytes.Equal(k, it.Key()) || !bytes.Equal(v, it.Value()) {
				t.Fatalf("floor %q = %q %q %v, want itself and %q", it.Key(), k, v, ok, it.Value())
			}
			k, _, ok := tr.FloorPrefix(it.Key())
			if r.mayContain(it.Key()[:1], bloomHash(it.Key()[:1])) && (!ok || !bytes.Equal(k, it.Key())) {
				t.Fatalf("prefix floor %q = %q %v, want itself", it.Key(), k, ok)
			}
		}
	})
}

// FuzzLoadBoundedRun feeds arbitrary bytes as the live pieces of a run a merge has consumed up to
// a key, the dead pieces before them as many as the root lists beyond those given: loading never
// panics, and a run it accepts holds only keys above the bound, in strictly increasing order.
func FuzzLoadBoundedRun(f *testing.F) {
	cfg := Config{MaxPiece: 256, Check: true}
	tr, err := New(cfg)
	if err != nil {
		f.Fatal(err)
	}
	for i := range 60 {
		put(f, tr, nil, fmt.Sprintf("k%02d", i), "v")
	}
	if err := tr.DeleteRange([]byte("k10"), []byte("k40")); err != nil {
		f.Fatal(err)
	}
	fl := newFakeLog()
	roots, _ := fl.checkpoint(f, tr, nil)
	refs, err := RootRefs(fl.blobs[roots[0]], nil)
	if err != nil {
		f.Fatal(err)
	}
	var ps [][]byte
	for _, r := range append(refs, roots[0]) {
		ps = append(ps, fl.blobs[r])
	}
	for _, c := range []struct {
		dead int
		key  string
	}{{0, "k00"}, {1, "k20"}, {2, "k45"}, {0, ""}, {1, "zz"}} {
		f.Add(joinPieces(ps[c.dead:]), []byte(c.key))
	}
	f.Fuzz(func(t *testing.T, in, key []byte) {
		root := disklog.BlobRef{Segment: 1, Block: 1, LSN: 1}
		pieces := splitPieces(in)
		if len(pieces) == 0 {
			return
		}
		refs, err := RootRefs(pieces[len(pieces)-1], nil)
		dead := len(refs) - len(pieces) + 1
		if err != nil || dead < 0 {
			return
		}
		tr, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if tr.LoadBoundedRun(disklog.MergeBound{Run: root, Key: key, Dead: uint32(dead)}, pieces) != nil {
			return
		}
		r := tr.runs[0]
		if !bytes.Equal(r.lo, key) || r.base < dead || r.bounded && bytes.Compare(r.first, key) <= 0 {
			t.Fatalf("run bounded at %q, base %d, first %q, loaded past %d dead pieces", r.lo, r.base, r.first, dead)
		}
		var prev []byte
		var it Iter
		tr.Iter(&it)
		for ok := it.SeekGE(nil); ok; ok = it.Next() {
			if bytes.Compare(it.Key(), key) <= 0 || prev != nil && bytes.Compare(it.Key(), prev) <= 0 {
				t.Fatalf("key %q after %q, bound %q", it.Key(), prev, key)
			}
			prev = append(prev[:0], it.Key()...)
		}
	})
}
