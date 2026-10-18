package lsm

import (
	"bytes"
	"maps"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/disklog"
)

func newTree(t testing.TB, maxPiece int) *Tree {
	t.Helper()
	tr, err := New(Config{MaxPiece: maxPiece})
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

// model is the map a tree must read as.
type model map[string]string

func (m model) clone() model { return maps.Clone(m) }

type kv struct{ k, v string }

func (m model) sorted() []kv {
	var out []kv
	for _, k := range slices.Sorted(maps.Keys(m)) {
		out = append(out, kv{k, m[k]})
	}
	return out
}

type reader interface {
	Get(k []byte) ([]byte, bool)
	Floor(k []byte) ([]byte, []byte, bool)
	Iter(it *Iter)
}

func dump(r reader) []kv {
	var it Iter
	r.Iter(&it)
	var out []kv
	for ok := it.SeekGE(nil); ok; ok = it.Next() {
		out = append(out, kv{string(it.Key()), string(it.Value())})
	}
	return out
}

// checkReads compares r with m: every entry in order, a get of every key and of probes, and the
// floor of each probe.
func checkReads(t testing.TB, r reader, m model, probes []string) {
	t.Helper()
	want := m.sorted()
	if got := dump(r); !slices.Equal(got, want) {
		t.Fatalf("entries\n got %d %v\nwant %d %v", len(got), clip(got), len(want), clip(want))
	}
	for _, e := range want {
		if v, ok := r.Get([]byte(e.k)); !ok || string(v) != e.v {
			t.Fatalf("get %q = %q %v, want %q", e.k, v, ok, e.v)
		}
	}
	for _, p := range probes {
		v, ok := r.Get([]byte(p))
		if mv, mok := m[p]; ok != mok || string(v) != mv {
			t.Fatalf("get %q = %q %v, want %q %v", p, v, ok, mv, mok)
		}
		i, _ := slices.BinarySearchFunc(want, p, func(e kv, p string) int { return bytes.Compare([]byte(e.k), []byte(p)) })
		if i < len(want) && want[i].k == p {
			i++
		}
		k, v, ok := r.Floor([]byte(p))
		if ok != (i > 0) || ok && (string(k) != want[i-1].k || string(v) != want[i-1].v) {
			t.Fatalf("floor %q = %q %q %v, want index %d", p, k, v, ok, i-1)
		}
	}
}

func clip(s []kv) []kv { return s[:min(len(s), 8)] }

// fakeLog keeps the pieces a flush writes, each under a reference of its own.
type fakeLog struct {
	next  uint64
	blobs map[disklog.BlobRef][]byte
}

func newFakeLog() *fakeLog { return &fakeLog{blobs: map[disklog.BlobRef][]byte{}} }

func (f *fakeLog) put(b []byte) disklog.BlobRef {
	f.next++
	ref := disklog.BlobRef{Segment: 1, Block: uint32(f.next), LSN: f.next}
	f.blobs[ref] = bytes.Clone(b)
	return ref
}

// checkpoint flushes tr into f, calling between after each piece is built, and returns the roots
// the manifest names and the pieces it reports dead.
func (f *fakeLog) checkpoint(t testing.TB, tr *Tree, between func()) ([]disklog.BlobRef, []disklog.BlobRef) {
	t.Helper()
	if _, err := tr.BeginFlush(); err != nil {
		t.Fatal(err)
	}
	roots := slices.Clone(tr.CarriedRuns())
	buf := make([]byte, tr.cfg.MaxPiece)
	for {
		n, root, err := tr.NextPiece(buf)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
		if n > tr.cfg.MaxPiece {
			t.Fatalf("piece of %d bytes, max %d", n, tr.cfg.MaxPiece)
		}
		ref := f.put(buf[:n])
		if between != nil {
			between()
		}
		if err := tr.PieceDurable(ref); err != nil {
			t.Fatal(err)
		}
		if root {
			roots = append(roots, ref)
		}
	}
	dead, err := tr.CheckpointDone()
	if err != nil {
		t.Fatal(err)
	}
	return roots, dead
}

// restore loads the runs at roots into a new tree.
func (f *fakeLog) restore(t testing.TB, maxPiece int, roots []disklog.BlobRef) *Tree {
	t.Helper()
	tr := newTree(t, maxPiece)
	for _, root := range roots {
		refs, err := RootRefs(f.blobs[root], nil)
		if err != nil {
			t.Fatal(err)
		}
		var pieces [][]byte
		for _, r := range append(refs, root) {
			pieces = append(pieces, f.blobs[r])
		}
		if err := tr.LoadRun(root, pieces); err != nil {
			t.Fatal(err)
		}
	}
	return tr
}

// checkPages checks the memtable's structure from root: keys sorted within each page and between
// pages, every inner key equal to the smallest key of its child, and no empty leaf but the root.
// It returns the number of pages reachable.
func checkPages(t testing.TB, m *memtable, root uint32) int {
	t.Helper()
	var walk func(id uint32, lo, hi []byte, isRoot bool) ([]byte, int)
	walk = func(id uint32, lo, hi []byte, isRoot bool) ([]byte, int) {
		p := m.pg.get(id)
		if p.used() > pageCap || p.free() < pageHdr+2*p.n() {
			t.Fatalf("page %d overfull: used %d, free %d, n %d", id, p.used(), p.free(), p.n())
		}
		for i := range p.n() {
			k := p.key(i)
			if (i > 0 && bytes.Compare(p.key(i-1), k) >= 0) || (lo != nil && bytes.Compare(k, lo) < 0) || (hi != nil && bytes.Compare(k, hi) >= 0) {
				t.Fatalf("page %d key %d %q out of order in [%q, %q)", id, i, k, lo, hi)
			}
		}
		if p.kind() == leafPage {
			if p.n() == 0 {
				if !isRoot {
					t.Fatalf("empty leaf %d", id)
				}
				return nil, 1
			}
			return p.key(0), 1
		}
		if p.kind() != innerPage {
			t.Fatalf("page %d has kind %d", id, p.kind())
		}
		first, pages := walk(p.left(), lo, firstOr(p, 0, hi), false)
		for i := range p.n() {
			next := hi
			if i+1 < p.n() {
				next = p.key(i + 1)
			}
			min, n := walk(p.child(i), p.key(i), next, false)
			if !bytes.Equal(min, p.key(i)) {
				t.Fatalf("page %d key %d is %q, its child's smallest %q", id, i, p.key(i), min)
			}
			pages += n
		}
		return first, pages + 1
	}
	_, n := walk(root, nil, nil, true)
	return n
}

func firstOr(p *page, i int, hi []byte) []byte {
	if p.n() > i {
		return p.key(i)
	}
	return hi
}
