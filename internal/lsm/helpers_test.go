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

// fakeLog keeps the pieces a flush writes, each under a reference of its own, the newest
// manifest's roots and bounds, and the merge worker of every piece.
type fakeLog struct {
	next    uint64
	blobs   map[disklog.BlobRef][]byte
	roots   []disklog.BlobRef
	bounds  []disklog.MergeBound
	workers []int
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
	roots, bounds := slices.Clone(tr.CarriedRuns()), slices.Clone(tr.Bounds())
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
		f.workers = append(f.workers, tr.PieceWorker())
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
	f.roots, f.bounds = roots, bounds
	return roots, dead
}

// drop forgets dead pieces, so a restore that reads one fails.
func (f *fakeLog) drop(dead []disklog.BlobRef) {
	for _, r := range dead {
		delete(f.blobs, r)
	}
}

// restoreManifest loads the newest manifest's runs, bounded ones from their first live piece, into a tree of cfg.
func (f *fakeLog) restoreManifest(t testing.TB, cfg Config) *Tree {
	t.Helper()
	tr, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, root := range f.roots {
		refs, err := RootRefs(f.blobs[root], nil)
		if err != nil {
			t.Fatal(err)
		}
		b := slices.IndexFunc(f.bounds, func(b disklog.MergeBound) bool { return b.Run == root })
		if b >= 0 {
			refs = refs[f.bounds[b].Dead:]
		}
		var pieces [][]byte
		for _, r := range append(refs, root) {
			p, ok := f.blobs[r]
			if !ok {
				t.Fatalf("the manifest needs dead piece %+v", r)
			}
			pieces = append(pieces, p)
		}
		if b >= 0 {
			err = tr.LoadBoundedRun(f.bounds[b], pieces)
		} else {
			err = tr.LoadRun(root, pieces)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	return tr
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

// pickSpan starts a merge of runs [i, j) by rule, whatever the picker would choose.
func (t *Tree) pickSpan(i, j int, r rule) *Compaction { return t.start(t.compaction(i, j, r)) }

// filterBytesOf is the bytes of r's filter partitions.
func filterBytesOf(r *run) int {
	n := 0
	for _, p := range r.parts {
		n += len(p.enc)
	}
	return n
}
