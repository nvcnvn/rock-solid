package lsm

import (
	"bytes"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/disklog"
)

func newTree(t testing.TB, maxPiece int) *Tree {
	t.Helper()
	tr, err := New(Config{MaxPiece: maxPiece, Check: true})
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
	FloorPrefix(k []byte) ([]byte, []byte, bool)
	Iter(it *Iter)
}

func prefixFunc(r reader) func([]byte) int {
	switch r := r.(type) {
	case *Tree:
		return r.cfg.Prefix
	case *View:
		return r.st.prefix
	}
	return nil
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
// floor, prefix floor, scan and prefix scan from each probe.
func checkReads(t testing.TB, r reader, m model, probes []string) {
	t.Helper()
	if d := readsDiffer(r, m, probes); d != "" {
		t.Fatal(d)
	}
}

// readsDiffer describes the first read in which r differs from m, or returns "".
func readsDiffer(r reader, m model, probes []string) string {
	want := m.sorted()
	if got := dump(r); !slices.Equal(got, want) {
		return fmt.Sprintf("entries\n got %d %v\nwant %d %v", len(got), clip(got), len(want), clip(want))
	}
	for _, e := range want {
		if v, ok := r.Get([]byte(e.k)); !ok || string(v) != e.v {
			return fmt.Sprintf("get %q = %q %v, want %q", e.k, v, ok, e.v)
		}
	}
	for _, p := range probes {
		v, ok := r.Get([]byte(p))
		if mv, mok := m[p]; ok != mok || string(v) != mv {
			return fmt.Sprintf("get %q = %q %v, want %q %v", p, v, ok, mv, mok)
		}
		i, _ := slices.BinarySearchFunc(want, p, func(e kv, p string) int { return bytes.Compare([]byte(e.k), []byte(p)) })
		if i < len(want) && want[i].k == p {
			i++
		}
		k, v, ok := r.Floor([]byte(p))
		if ok != (i > 0) || ok && (string(k) != want[i-1].k || string(v) != want[i-1].v) {
			return fmt.Sprintf("floor %q = %q %q %v, want index %d", p, k, v, ok, i-1)
		}
		pfx := string(prefixOf(prefixFunc(r), []byte(p)))
		wk, wv, wok := bytes.Clone(k), bytes.Clone(v), ok && strings.HasPrefix(string(k), pfx)
		if k, v, ok := r.FloorPrefix([]byte(p)); ok != wok || ok && (!bytes.Equal(k, wk) || !bytes.Equal(v, wv)) {
			return fmt.Sprintf("prefix floor %q = %q %q %v, want %q %q %v", p, k, v, ok, wk, wv, wok)
		}
		if d := seekDiffers(r, want, p, false); d != "" {
			return d
		}
		if d := seekDiffers(r, want, p, true); d != "" {
			return d
		}
	}
	return floorsDiffer(r, want, probes)
}

// prefixFloor is the greatest of want's entries <= p with p's prefix.
func prefixFloor(r reader, want []kv, p string) (kv, bool) {
	i, found := slices.BinarySearchFunc(want, p, func(e kv, p string) int { return bytes.Compare([]byte(e.k), []byte(p)) })
	if found {
		i++
	}
	if i == 0 || !strings.HasPrefix(want[i-1].k, string(prefixOf(prefixFunc(r), []byte(p)))) {
		return kv{}, false
	}
	return want[i-1], true
}

// floorsDiffer describes the first prefix floor that differs from want, the model's entries in
// order, through one iterator over probes in order, a prefix seek before every fifth, then in
// reverse, or returns "".
func floorsDiffer(r reader, want []kv, probes []string) string {
	var it Iter
	r.Iter(&it)
	for i := range 2 * len(probes) {
		p := probes[i%len(probes)]
		if i >= len(probes) {
			p = probes[2*len(probes)-1-i]
		} else if i%5 == 0 {
			j, _ := slices.BinarySearchFunc(want, p, func(e kv, p string) int { return bytes.Compare([]byte(e.k), []byte(p)) })
			pfx := string(prefixOf(prefixFunc(r), []byte(p)))
			wok := j < len(want) && strings.HasPrefix(want[j].k, pfx)
			if ok := it.SeekPrefixGE([]byte(p)); ok != wok || ok && string(it.Key()) != want[j].k {
				return fmt.Sprintf("prefix seek %q between floors = %v, want %v", p, ok, wok)
			}
		}
		w, wok := prefixFloor(r, want, p)
		if k, v, ok := it.FloorPrefix([]byte(p)); ok != wok || ok && (string(k) != w.k || string(v) != w.v) {
			return fmt.Sprintf("stepping prefix floor %d %q = %q %q %v, want %q %q %v", i, p, k, v, ok, w.k, w.v, wok)
		}
	}
	return ""
}

// seekDiffers describes how a scan of r from SeekGE(p), or SeekPrefixGE(p) with inPrefix, differs
// from want, the model's entries in order, over its first 8 steps, or 12 in a prefix, which with
// fewer entries left must end the scan; it returns "" when they agree.
func seekDiffers(r reader, want []kv, p string, inPrefix bool) string {
	i, _ := slices.BinarySearchFunc(want, p, func(e kv, p string) int { return bytes.Compare([]byte(e.k), []byte(p)) })
	want = want[i:]
	limit := 8
	if inPrefix {
		pfx := string(prefixOf(prefixFunc(r), []byte(p)))
		n := 0
		for n < len(want) && strings.HasPrefix(want[n].k, pfx) {
			n++
		}
		want, limit = want[:n], 12
	}
	steps := len(want) + 1
	if len(want) >= limit {
		want, steps = want[:limit], limit
	}
	var it Iter
	r.Iter(&it)
	seek := it.SeekGE
	if inPrefix {
		seek = it.SeekPrefixGE
	}
	var got []kv
	for ok := seek([]byte(p)); ok && len(got) < steps; ok = it.Next() {
		got = append(got, kv{string(it.Key()), string(it.Value())})
	}
	if !slices.Equal(got, want) {
		return fmt.Sprintf("scan from %q in prefix %v\n got %v\nwant %v", p, inPrefix, got, want)
	}
	return ""
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
	if err := f.load(tr, roots); err != nil {
		t.Fatal(err)
	}
	return tr
}

// load loads the runs at roots into tr.
func (f *fakeLog) load(tr *Tree, roots []disklog.BlobRef) error {
	for _, root := range roots {
		refs, err := RootRefs(f.blobs[root], nil)
		if err != nil {
			return err
		}
		var pieces [][]byte
		for _, r := range append(refs, root) {
			pieces = append(pieces, f.blobs[r])
		}
		if err := tr.LoadRun(root, pieces); err != nil {
			return err
		}
	}
	return nil
}

// checkPages checks the memtable's structure from root: keys sorted within each page and between
// pages, every inner key equal to the smallest key of its child, and no empty leaf but the root.
// It returns the number of pages reachable.
func checkPages(t testing.TB, m *memtable, root uint32) int {
	t.Helper()
	return checkTree(t, m, root, true)
}

// checkTree is checkPages, with inner keys only at or below their child's smallest key unless
// exact, as a tree that removes keys leaves them.
func checkTree(t testing.TB, m *memtable, root uint32, exact bool) int {
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
			if c := bytes.Compare(min, p.key(i)); c < 0 || exact && c != 0 {
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
