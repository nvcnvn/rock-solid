package lsm

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/disklog"
)

func put(t testing.TB, tr *Tree, m model, k, v string) {
	t.Helper()
	if err := tr.Put([]byte(k), []byte(v)); err != nil {
		t.Fatal(err)
	}
	if m != nil {
		m[k] = v
	}
}

func del(t testing.TB, tr *Tree, m model, k string) {
	t.Helper()
	if err := tr.Delete([]byte(k)); err != nil {
		t.Fatal(err)
	}
	delete(m, k)
}

// mutate applies a random put or, one time in four, a removal, keeping keys and values within
// the lengths given.
func mutate(t testing.TB, r *rand.Rand, tr *Tree, m model, maxKey, maxValue, alphabet int) {
	t.Helper()
	k := randKey(r, 1+r.IntN(maxKey), alphabet)
	if r.IntN(4) == 0 {
		del(t, tr, m, k)
		return
	}
	put(t, tr, m, k, strings.Repeat("v", r.IntN(maxValue+1)))
}

func probes(r *rand.Rand, n, maxKey, alphabet int) []string {
	out := []string{""}
	for range n {
		out = append(out, randKey(r, 1+r.IntN(maxKey), alphabet))
	}
	return out
}

func TestGetReplaceAndPrefixScan(t *testing.T) {
	tr := newTree(t, 1<<20)
	for _, k := range []string{"ab", "a", "abc", "b", "aa"} {
		put(t, tr, nil, k, "v"+k)
	}
	scan := func(prefix string) []string {
		var it Iter
		tr.Iter(&it)
		var out []string
		for ok := it.SeekGE([]byte(prefix)); ok && strings.HasPrefix(string(it.Key()), prefix); ok = it.Next() {
			out = append(out, string(it.Key()))
		}
		return out
	}
	if got := scan("a"); !slices.Equal(got, []string{"a", "aa", "ab", "abc"}) {
		t.Fatalf("scan a: %q", got)
	}
	if got := scan("ab"); len(got) != 2 {
		t.Fatalf("scan ab: %q", got)
	}
	put(t, tr, nil, "b", "again")
	if v, ok := tr.Get([]byte("b")); !ok || string(v) != "again" || len(dump(tr)) != 5 || tr.mem.count != 5 {
		t.Fatalf("b = %q %v after replacing, %d entries", v, ok, len(dump(tr)))
	}
}

// [property] Random puts and removes, with keys up to MaxKey and values up to MaxValue so pages
// split at every level, read back as the model does at every step, and the page structure holds.
func TestMemtableMatchesModel(t *testing.T) {
	for seed := range 12 {
		r := rand.New(rand.NewPCG(uint64(seed), 1))
		tr := newTree(t, 1<<20)
		m := model{}
		maxKey, maxValue, alphabet := 12, 40, 6
		if seed%3 == 2 {
			maxKey, maxValue, alphabet = MaxKey, MaxValue, 26
		}
		ops := 2000 + r.IntN(4000)
		for i := range ops {
			mutate(t, r, tr, m, maxKey, maxValue, alphabet)
			if i%997 == 0 || i == ops-1 {
				checkReads(t, tr, m, probes(r, 50, maxKey, alphabet))
				if n := checkPages(t, tr.mem, tr.mem.root); n != tr.pg.inUse {
					t.Fatalf("seed %d: %d pages reachable, %d in use", seed, n, tr.pg.inUse)
				}
			}
		}
	}
}

// [property] Views opened at random points read the tree as it was then while writes continue,
// closing them frees every page they alone held, and only pages a view could reach are copied.
func TestViewsCopyOnWrite(t *testing.T) {
	for seed := range 6 {
		r := rand.New(rand.NewPCG(uint64(seed), 3))
		tr := newTree(t, 1<<20)
		m := model{}
		type open struct {
			v *View
			m model
		}
		var views []open
		closeOne := func(i int) {
			checkReads(t, views[i].v, views[i].m, probes(r, 20, 8, 6))
			views[i].v.Close()
			views = slices.Delete(views, i, i+1)
		}
		for i := range 15000 {
			mutate(t, r, tr, m, 8, 30, 6)
			if r.IntN(400) == 0 {
				views = append(views, open{tr.View(), m.clone()})
			}
			if len(views) > 0 && r.IntN(600) == 0 {
				closeOne(r.IntN(len(views)))
			}
			if i%3001 == 0 {
				checkReads(t, tr, m, probes(r, 20, 8, 6))
			}
		}
		for len(views) > 0 {
			closeOne(r.IntN(len(views)))
		}
		if n := checkPages(t, tr.mem, tr.mem.root); n != tr.pg.inUse || len(tr.mem.stale) != 0 {
			t.Fatalf("seed %d: %d pages reachable, %d in use, %d stale", seed, n, tr.pg.inUse, len(tr.mem.stale))
		}
		before := tr.pg.inUse
		mutate(t, r, tr, m, 8, 30, 6)
		if tr.pg.inUse != before {
			t.Fatalf("a write with no view open copied pages: %d to %d", before, tr.pg.inUse)
		}
	}
}

// Keys inserted in random order, as files are created, leave leaves well filled because a full
// leaf hands entries to a neighbour before it splits; the Go memtable spike measured 63% without.
func TestLeafRedistribution(t *testing.T) {
	r := rand.New(rand.NewPCG(5, 5))
	tr := newTree(t, 1<<20)
	for range 50000 {
		put(t, tr, nil, fmt.Sprintf("fs/%016x/a", r.Uint64()), strings.Repeat("x", 40))
	}
	used, leaves := 0, 0
	for id := range tr.pg.next {
		if p := tr.pg.get(id); p.kind() == leafPage {
			used += p.used()
			leaves++
		}
	}
	fill := float64(used) / float64(leaves*pageCap)
	t.Logf("%d leaves, %.1f%% full", leaves, 100*fill)
	if fill < 0.75 {
		t.Fatalf("leaves %.1f%% full", 100*fill)
	}
}

// flushChecked runs a checkpoint like fakeLog.checkpoint but tolerates ErrTooLarge from a run that
// needs more pieces than its root can list, checking that it really does.
func flushChecked(t *testing.T, f *fakeLog, tr *Tree, between func()) ([]disklog.BlobRef, bool) {
	t.Helper()
	if _, err := tr.BeginFlush(); err != nil {
		t.Fatal(err)
	}
	roots := slices.Clone(tr.CarriedRuns())
	buf := make([]byte, tr.cfg.MaxPiece)
	pieces := 0
	for {
		n, root, err := tr.NextPiece(buf)
		if errors.Is(err, ErrTooLarge) {
			if rootOverhead(pieces)+emptyBlockSize <= tr.cfg.MaxPiece {
				t.Fatalf("ErrTooLarge after %d pieces of at most %d bytes", pieces, tr.cfg.MaxPiece)
			}
			tr.FlushFailed()
			return nil, false
		}
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
		block, _, seq, refs, err := splitPiece(buf[:n], nil)
		if err != nil || n > tr.cfg.MaxPiece || len(block)+rootOverhead(len(refs)) != n || seq == 0 || root && len(refs) != pieces || !root && len(refs) != 0 {
			t.Fatalf("piece %d of %d bytes: block %d, refs %d, seq %d, root %v: %v", pieces, n, len(block), len(refs), seq, root, err)
		}
		ref := f.put(buf[:n])
		pieces++
		if between != nil {
			between()
		}
		if err := tr.PieceDurable(ref); err != nil {
			t.Fatal(err)
		}
		if root {
			roots = append(roots, ref)
			pieces = 0
		}
	}
	if _, err := tr.CheckpointDone(); err != nil {
		t.Fatal(err)
	}
	return roots, true
}

// [property] A flush at any piece size writes pieces within the limit and sized exactly, and
// loading them as a run into an empty tree reads as the tree did.
func TestFlushAtAnyPieceSize(t *testing.T) {
	r := rand.New(rand.NewPCG(11, 0))
	ok := 0
	for trial := range 50 {
		maxPiece := 123 + r.IntN(4105-123+1)
		tr := newTree(t, maxPiece)
		m := model{}
		for range r.IntN(201) {
			k := randKey(r, 1+r.IntN(40), 26)
			if r.IntN(4) == 0 {
				del(t, tr, m, k)
			} else {
				put(t, tr, m, k, strings.Repeat("v", r.IntN(60)))
			}
		}
		f := newFakeLog()
		roots, done := flushChecked(t, f, tr, nil)
		if !done {
			continue
		}
		ok++
		restored := f.restore(t, maxPiece, roots)
		checkReads(t, restored, m, probes(r, 30, 40, 26))
		checkReads(t, tr, m, probes(r, 30, 40, 26))
		if len(m) == 0 && len(roots) != 0 {
			t.Fatalf("trial %d: an empty tree wrote %d runs", trial, len(roots))
		}
	}
	if ok < 35 {
		t.Fatalf("only %d of 50 trials fit their runs in pieces", ok)
	}
}

// [property] Writes between a flush's pieces go to the new memtable, on keys before and after the
// cursor; the run holds the tree as of the freeze, the frozen pages are freed after the last piece,
// and the tree still reads as the model.
func TestFlushKeepsFrozenView(t *testing.T) {
	r := rand.New(rand.NewPCG(12, 0))
	for trial := range 200 {
		maxPiece := 256 + r.IntN(1800)
		tr := newTree(t, maxPiece)
		m := model{}
		for range r.IntN(301) {
			mutate(t, r, tr, m, 3, 20, 5)
		}
		frozen := m.clone()
		f := newFakeLog()
		roots, done := flushChecked(t, f, tr, func() {
			for range r.IntN(21) {
				mutate(t, r, tr, m, 3, 20, 5)
			}
		})
		if !done {
			t.Fatalf("trial %d: run did not fit %d-byte pieces", trial, maxPiece)
		}
		checkReads(t, f.restore(t, maxPiece, roots), frozen, probes(r, 20, 3, 5))
		if tr.Frozen() || checkPages(t, tr.mem, tr.mem.root) != tr.pg.inUse {
			t.Fatalf("trial %d: %d pages in use after the flush, frozen %v", trial, tr.pg.inUse, tr.Frozen())
		}
		checkReads(t, tr, m, probes(r, 20, 3, 5))
	}
}

// An empty memtable writes no run, nor does one holding only removals no older run holds the key
// of; a durable run is named again without being rewritten.
func TestEmptyTreeWritesNoRun(t *testing.T) {
	tr := newTree(t, 256)
	f := newFakeLog()
	if froze, err := tr.BeginFlush(); froze || err != nil {
		t.Fatalf("empty tree froze %v: %v", froze, err)
	}
	if n, _, err := tr.NextPiece(make([]byte, 256)); n != 0 || err != nil {
		t.Fatalf("empty tree built a piece of %d bytes: %v", n, err)
	}
	if dead, err := tr.CheckpointDone(); len(dead) != 0 || err != nil || tr.Runs() != 0 {
		t.Fatalf("dead %v, %d runs: %v", dead, tr.Runs(), err)
	}
	put(t, tr, nil, "a", "v")
	del(t, tr, nil, "a")
	del(t, tr, nil, "b")
	if roots, dead := f.checkpoint(t, tr, nil); len(roots) != 0 || len(dead) != 0 || len(f.blobs) != 0 || tr.Runs() != 0 {
		t.Fatalf("removals with nothing to hide wrote roots %v, %d pieces, dead %v", roots, len(f.blobs), dead)
	}
	if tr.Frozen() || tr.pg.inUse != checkPages(t, tr.mem, tr.mem.root) {
		t.Fatalf("frozen %v, %d pages in use", tr.Frozen(), tr.pg.inUse)
	}
	checkReads(t, tr, model{}, []string{"a", "b"})
	put(t, tr, nil, "c", "v")
	first, _ := f.checkpoint(t, tr, nil)
	written := len(f.blobs)
	del(t, tr, nil, "d")
	if roots, dead := f.checkpoint(t, tr, nil); !slices.Equal(roots, first) || len(dead) != 0 || len(f.blobs) != written || tr.Runs() != 1 {
		t.Fatalf("roots %v after %v, %d pieces after %d, dead %v", roots, first, len(f.blobs), written, dead)
	}
}

// A flush holds the frozen tree, the one piece being built, the run it becomes and its filter,
// and nothing more: the frozen pages are never copied, the run grows by each piece's block, a
// second piece is refused until the first is durable, and the heap grows only by the run.
func TestFlushPeakMemory(t *testing.T) {
	const maxPiece = 4096
	tr, err := New(Config{MaxPiece: maxPiece})
	if err != nil {
		t.Fatal(err)
	}
	fileModel(t, tr, 2000, 3)
	frozenPages := tr.pg.inUse
	if froze, err := tr.BeginFlush(); !froze || err != nil {
		t.Fatalf("froze %v: %v", froze, err)
	}
	memPages := checkPages(t, tr.mem, tr.mem.root)
	filterBytes := len(tr.fl.filter.blob) + partHdr + partTrailer
	buf, spare := make([]byte, maxPiece), make([]byte, maxPiece)
	refs := make([]disklog.BlobRef, 0, maxPiece/disklog.BlobRefSize)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	held, pieces, peak := 0, 0, 0
	for {
		n, _, err := tr.NextPiece(buf)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
		pieces++
		block, kind, _, _, err := splitPiece(buf[:n], refs[:0])
		if err != nil {
			t.Fatal(err)
		}
		if kind != filterBlock && len(block) > emptyBlockSize {
			held += len(block)
		}
		if got := tr.fl.frozen.bytes - filterBytesOf(tr.fl.frozen); tr.pg.inUse != frozenPages+memPages || got != held {
			t.Fatalf("piece %d: %d pages in use, want %d; run holds %d bytes, its pieces' blocks %d", pieces, tr.pg.inUse, frozenPages+memPages, got, held)
		}
		if _, _, err := tr.NextPiece(spare); !errors.Is(err, ErrNoFlush) {
			t.Fatalf("piece %d: a second piece handed out before the first is durable: %v", pieces, err)
		}
		peak = max(peak, tr.pg.inUse*pageSize+held+filterBytes+maxPiece)
		if err := tr.PieceDurable(disklog.BlobRef{Segment: 1, Block: uint32(pieces), LSN: uint64(pieces)}); err != nil {
			t.Fatal(err)
		}
	}
	runtime.ReadMemStats(&after)
	if _, err := tr.CheckpointDone(); err != nil {
		t.Fatal(err)
	}
	if tr.Runs() != 1 || tr.RunBytes() != held+filterBytes || tr.pg.inUse != memPages {
		t.Fatalf("%d runs of %d bytes, want 1 of %d; %d pages in use, want %d", tr.Runs(), tr.RunBytes(), held+filterBytes, tr.pg.inUse, memPages)
	}
	if entries, _ := runKinds(tr.runs[0]); entries != 2*2000 {
		t.Fatalf("run holds %d entries, want %d", entries, 2*2000)
	}
	if bound := (frozenPages+memPages)*pageSize + maxPiece + held + filterBytes; peak > bound {
		t.Fatalf("peak %d bytes over the frozen tree, a piece, the run and its filter, %d", peak, bound)
	}
	alloc := int(after.TotalAlloc - before.TotalAlloc)
	t.Logf("%d pieces; frozen tree %d B, run %d B, heap allocated %d B", pieces, frozenPages*pageSize, held, alloc)
	if limit := held + held/8 + 16<<10; alloc > limit {
		t.Fatalf("flush allocated %d bytes for a run of %d, limit %d", alloc, held, limit)
	}
}

func TestMemtableOverRuns(t *testing.T) {
	tr := newTree(t, 1<<20)
	m := model{}
	put(t, tr, m, "a", "1")
	put(t, tr, m, "b", "2")
	put(t, tr, m, "c", "3")
	f := newFakeLog()
	f.checkpoint(t, tr, nil)
	put(t, tr, m, "b", "20")
	del(t, tr, m, "c")
	put(t, tr, m, "d", "4")
	checkReads(t, tr, m, []string{"a", "b", "c", "d", "e", ""})
	if got := dump(tr); !slices.Equal(got, []kv{{"a", "1"}, {"b", "20"}, {"d", "4"}}) {
		t.Fatalf("scan %v", got)
	}
}

// A get searches the memtable, the frozen one during a flush, and each run whose filter lets the
// key through, by its fence pointers and one block; a run whose bounds leave the key out is not
// searched. Every run's filter is probed once.
func TestGetTouchesEachLayerOnce(t *testing.T) {
	tr := newTree(t, 1<<20)
	f := newFakeLog()
	for i := range 3 {
		put(t, tr, nil, "a", "v")
		put(t, tr, nil, fmt.Sprint("k", i), "v")
		put(t, tr, nil, "z", "v")
		f.checkpoint(t, tr, nil)
	}
	put(t, tr, nil, "k9", "v")
	if _, err := tr.BeginFlush(); err != nil {
		t.Fatal(err)
	}
	put(t, tr, nil, "k8", "v")
	noFalsePositives(t, tr, "missing", "k0", "k2", "zz", "")
	for _, c := range []struct {
		k      string
		probes int
	}{{"missing", 2}, {"k0", 3}, {"k2", 3}, {"zz", 2}, {"", 2}} {
		before := tr.Searches()
		tr.Get([]byte(c.k))
		if n := tr.Searches() - before; n != c.probes {
			t.Fatalf("get %q probed %d layers, want %d", c.k, n, c.probes)
		}
	}
}

// runKinds counts the entries and removals a run holds.
func runKinds(r *run) (entries, removals int) {
	for _, b := range r.blocks {
		var it blockIter
		it.init(b)
		for ok := it.first(); ok; ok = it.advance() {
			entries++
			if it.val[0] == kindDel {
				removals++
			}
		}
	}
	return entries, removals
}

// Four equal runs with overlapping keys merge by size amplification into their merge, the newest
// value winning and, since they include the oldest run, removed keys dropped; a view opened before
// keeps reading the old runs, and the next checkpoint reports the old runs dead.
func TestCompactionMergesFourRuns(t *testing.T) {
	tr := newTree(t, 512)
	m := model{}
	f := newFakeLog()
	var oldPieces []disklog.BlobRef
	for i := range 4 {
		for j := range 30 {
			put(t, tr, m, fmt.Sprintf("k%02d", (j*7+i*5)%40), fmt.Sprint(i, "-", j))
		}
		del(t, tr, m, fmt.Sprintf("k%02d", i*3))
		f.checkpoint(t, tr, nil)
	}
	for _, r := range tr.runs {
		oldPieces = append(oldPieces, r.pieces...)
	}
	before := m.clone()
	view := tr.View()
	c := tr.PickCompaction()
	if c == nil || c.rule != ruleSizeAmp || len(c.inputs) != 4 || len(c.older) != 0 {
		t.Fatalf("compaction %+v", c)
	}
	if c := tr.PickCompaction(); c != nil {
		t.Fatalf("picked %v over runs the running merge holds", c.rule)
	}
	done := make(chan struct{})
	go func() {
		c.Run()
		close(done)
	}()
	for j := range 20 {
		put(t, tr, m, fmt.Sprintf("n%02d", j), "new")
	}
	<-done
	if err := tr.Install(c); err != nil {
		t.Fatal(err)
	}
	if tr.Runs() != 1 {
		t.Fatalf("%d runs after compaction", tr.Runs())
	}
	if n, dels := runKinds(tr.runs[0]); n != len(before) || dels != 0 {
		t.Fatalf("merged run holds %d entries, %d removals; want %d, 0", n, dels, len(before))
	}
	checkReads(t, tr, m, []string{"k00", "k03", "k39", "n00"})
	checkReads(t, view, before, []string{"k00", "k03", "k39", "n00"})
	if len(view.st.runs) != 4 {
		t.Fatalf("view holds %d runs", len(view.st.runs))
	}
	view.Close()
	roots, dead := f.checkpoint(t, tr, nil)
	slices.SortFunc(dead, cmpRef)
	slices.SortFunc(oldPieces, cmpRef)
	if len(roots) != 2 || !slices.Equal(dead, oldPieces) {
		t.Fatalf("roots %v, dead %v, want the %d old pieces", roots, dead, len(oldPieces))
	}
	checkReads(t, f.restore(t, 512, roots), m, []string{"k00", "n00"})
}

func cmpRef(a, b disklog.BlobRef) int { return int(a.Block) - int(b.Block) }

// Size ratio takes four small runs and stops at a run over 1 MiB; the merge leaves that run out
// and keeps the removals, which still hide its keys.
func TestCompactionKeepsRemovalsAboveOlderRuns(t *testing.T) {
	tr := newTree(t, 1<<20)
	m := model{}
	f := newFakeLog()
	for i := range 30000 {
		put(t, tr, m, fmt.Sprintf("base%06d", i), strings.Repeat("b", 30))
	}
	f.checkpoint(t, tr, nil)
	if tr.runs[0].bytes < 1<<20 {
		t.Fatalf("base run of %d bytes is not over 1 MiB", tr.runs[0].bytes)
	}
	for i := range 4 {
		del(t, tr, m, fmt.Sprintf("base%06d", i))
		put(t, tr, m, fmt.Sprint("small", i), "s")
		f.checkpoint(t, tr, nil)
	}
	c := tr.PickCompaction()
	if c == nil || c.rule != ruleSizeRatio || len(c.inputs) != 4 || len(c.older) != 1 {
		t.Fatalf("compaction %+v", c)
	}
	c.Run()
	if err := tr.Install(c); err != nil {
		t.Fatal(err)
	}
	if _, dels := runKinds(tr.runs[0]); tr.Runs() != 2 || dels != 4 {
		t.Fatalf("%d runs, newest holds %d removals", tr.Runs(), dels)
	}
	checkReads(t, tr, m, []string{"base000000", "base000004", "small3"})
}

// Restore is runs plus replay: after six flushes, a compaction and more writes, loading the
// manifest's runs and replaying the writes after it reads as the live tree.
func TestRestoreIsRunsPlusReplay(t *testing.T) {
	r := rand.New(rand.NewPCG(13, 0))
	tr := newTree(t, 700)
	m := model{}
	f := newFakeLog()
	var roots []disklog.BlobRef
	for range 6 {
		for range 100 {
			mutate(t, r, tr, m, 4, 30, 6)
		}
		roots, _ = f.checkpoint(t, tr, nil)
	}
	if c := tr.PickCompaction(); c != nil {
		c.Run()
		if err := tr.Install(c); err != nil {
			t.Fatal(err)
		}
	}
	type op struct {
		k, v string
		del  bool
	}
	var replay []op
	for range 150 {
		k := randKey(r, 1+r.IntN(4), 6)
		if r.IntN(4) == 0 {
			del(t, tr, m, k)
			replay = append(replay, op{k: k, del: true})
		} else {
			put(t, tr, m, k, "after")
			replay = append(replay, op{k: k, v: "after"})
		}
	}
	restored := f.restore(t, 700, roots)
	for _, o := range replay {
		if o.del {
			del(t, restored, nil, o.k)
		} else {
			put(t, restored, nil, o.k, o.v)
		}
	}
	if got, want := dump(restored), dump(tr); !slices.Equal(got, want) {
		t.Fatalf("restored %d entries, live %d", len(got), len(want))
	}
	checkReads(t, restored, m, probes(r, 50, 4, 6))
}

func TestLoadRunRejects(t *testing.T) {
	tr := newTree(t, 256)
	for i := range 40 {
		put(t, tr, nil, fmt.Sprint("key", i), "value")
	}
	f := newFakeLog()
	roots, _ := f.checkpoint(t, tr, nil)
	root := f.blobs[roots[0]]
	refs, err := RootRefs(root, nil)
	if err != nil || len(refs) == 0 {
		t.Fatalf("refs %v: %v", refs, err)
	}
	pieces := func() [][]byte {
		var ps [][]byte
		for _, r := range append(refs, roots[0]) {
			ps = append(ps, slices.Clone(f.blobs[r]))
		}
		return ps
	}
	cases := map[string]func([][]byte) [][]byte{
		"truncated piece":          func(ps [][]byte) [][]byte { ps[0] = ps[0][:len(ps[0])-1]; return ps },
		"truncated root":           func(ps [][]byte) [][]byte { ps[len(ps)-1] = ps[len(ps)-1][:len(ps[len(ps)-1])-1]; return ps },
		"missing piece":            func(ps [][]byte) [][]byte { return ps[1:] },
		"pieces swapped":           func(ps [][]byte) [][]byte { ps[0], ps[1] = ps[1], ps[0]; return ps },
		"other run's seq":          func(ps [][]byte) [][]byte { ps[0][len(ps[0])-10]++; return ps },
		"first key above the next": func(ps [][]byte) [][]byte { ps[0][5] ^= 0x11; return ps },
	}
	for name, mod := range cases {
		err := newTree(t, 256).LoadRun(roots[0], mod(pieces()))
		if !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: %v, want ErrMalformed", name, err)
		}
	}
	ps := pieces()
	ps[0][0]++
	if err := newTree(t, 256).LoadRun(roots[0], ps); !errors.Is(err, ErrBadVersion) {
		t.Errorf("other version: %v", err)
	}
	twice := newTree(t, 256)
	if err := twice.LoadRun(roots[0], pieces()); err != nil {
		t.Fatal(err)
	}
	if err := twice.LoadRun(roots[0], pieces()); !errors.Is(err, ErrMalformed) {
		t.Errorf("one run loaded twice: %v", err)
	}
}

func TestEntryLargerThanPiece(t *testing.T) {
	tr := newTree(t, 128)
	if err := tr.Put([]byte(strings.Repeat("k", 60)), []byte(strings.Repeat("v", 60))); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("entry over a piece: %v", err)
	}
	if err := tr.Put([]byte(strings.Repeat("k", 50)), []byte(strings.Repeat("v", 50))); err != nil {
		t.Fatalf("entry within a piece: %v", err)
	}
	big := newTree(t, 1<<20)
	if err := big.Put(make([]byte, MaxKey+1), nil); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("key over MaxKey: %v", err)
	}
	if err := big.Put(make([]byte, MaxKey), make([]byte, MaxValue+1)); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("value over MaxValue: %v", err)
	}
	if err := big.Put(make([]byte, MaxKey), make([]byte, MaxValue)); err != nil {
		t.Fatalf("largest entry: %v", err)
	}
	if _, err := New(Config{MaxPiece: MinPiece - 1}); !errors.Is(err, ErrBadConfig) {
		t.Fatalf("piece below MinPiece: %v", err)
	}
}

// A failed checkpoint keeps the frozen memtable, reports the pieces it wrote of it, and the next
// flush writes that memtable again without freezing the writes made since.
func TestFlushFailedKeepsFrozen(t *testing.T) {
	tr := newTree(t, 256)
	m := model{}
	for i := range 40 {
		put(t, tr, m, fmt.Sprint("key", i), "value")
	}
	frozen := m.clone()
	if froze, err := tr.BeginFlush(); !froze || err != nil {
		t.Fatalf("froze %v: %v", froze, err)
	}
	f := newFakeLog()
	buf := make([]byte, 256)
	n, root, err := tr.NextPiece(buf)
	if err != nil || root || n == 0 {
		t.Fatalf("first piece %d root %v: %v", n, root, err)
	}
	ref := f.put(buf[:n])
	if _, _, err := tr.NextPiece(buf); !errors.Is(err, ErrNoFlush) {
		t.Fatalf("piece before the last is durable: %v", err)
	}
	if err := tr.PieceDurable(ref); err != nil {
		t.Fatal(err)
	}
	if dead := tr.FlushFailed(); !slices.Equal(dead, []disklog.BlobRef{ref}) || !tr.Frozen() {
		t.Fatalf("dead %v, frozen %v", dead, tr.Frozen())
	}
	put(t, tr, m, "later", "x")
	checkReads(t, tr, m, []string{"key1", "later"})
	if froze, err := tr.BeginFlush(); froze || err != nil {
		t.Fatalf("second flush froze %v: %v", froze, err)
	}
	tr.FlushFailed()
	roots, _ := f.checkpoint(t, tr, nil)
	checkReads(t, f.restore(t, 256, roots), frozen, []string{"later"})
	roots, _ = f.checkpoint(t, tr, nil)
	if len(roots) != 2 {
		t.Fatalf("roots %v", roots)
	}
	checkReads(t, f.restore(t, 256, roots), m, []string{"later"})
}

// A view opened while a flush writes the frozen memtable keeps reading it after the flush
// finishes, and the frozen pages are freed only once the view closes.
func TestViewDuringFlush(t *testing.T) {
	tr := newTree(t, 256)
	m := model{}
	for i := range 200 {
		put(t, tr, m, fmt.Sprint("key", i), "old")
	}
	if _, err := tr.BeginFlush(); err != nil {
		t.Fatal(err)
	}
	put(t, tr, m, "key7", "new")
	seen := m.clone()
	view := tr.View()
	frozenPages := checkPages(t, tr.frozen, tr.frozen.root)
	f := newFakeLog()
	buf := make([]byte, 256)
	for {
		n, _, err := tr.NextPiece(buf)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
		put(t, tr, m, "key8", "newer")
		if err := tr.PieceDurable(f.put(buf[:n])); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tr.CheckpointDone(); err != nil {
		t.Fatal(err)
	}
	live := checkPages(t, tr.mem, tr.mem.root)
	if tr.Frozen() || tr.pg.inUse < live+frozenPages {
		t.Fatalf("frozen %v, %d pages in use, %d live and %d frozen", tr.Frozen(), tr.pg.inUse, live, frozenPages)
	}
	checkReads(t, view, seen, []string{"key7", "key8"})
	view.Close()
	if tr.pg.inUse != checkPages(t, tr.mem, tr.mem.root) {
		t.Fatalf("%d pages in use after the view closed, %d live", tr.pg.inUse, live)
	}
	checkReads(t, tr, m, []string{"key7", "key8"})
}

// A flush keeps a removal only when an older run holds its key.
func TestFlushDropsRemovalsWithNothingToHide(t *testing.T) {
	tr := newTree(t, 1<<20)
	m := model{}
	f := newFakeLog()
	put(t, tr, m, "old", "1")
	f.checkpoint(t, tr, nil)
	put(t, tr, m, "new", "2")
	del(t, tr, m, "new")
	del(t, tr, m, "old")
	del(t, tr, m, "never")
	f.checkpoint(t, tr, nil)
	if n, dels := runKinds(tr.runs[0]); n != 1 || dels != 1 {
		t.Fatalf("newest run holds %d entries, %d removals; want only the removal of old", n, dels)
	}
	checkReads(t, tr, m, []string{"old", "new", "never"})
}

// A compaction installed before it ran is Stale and the next is still picked; a span whose output
// a root could not list is never picked: size amplification and size ratio both pick four runs
// of 400 entries in 512-byte pieces, the picker goes on to the next rule each time it refuses the
// span, and nothing is picked.
func TestCompactionGuards(t *testing.T) {
	tr := newTree(t, 512)
	f := newFakeLog()
	for i := range 4 {
		put(t, tr, nil, fmt.Sprint("k", i), "v")
		f.checkpoint(t, tr, nil)
	}
	c := tr.PickCompaction()
	if err := tr.Install(c); !errors.Is(err, ErrStale) {
		t.Fatalf("install before run: %v", err)
	}
	if tr.PickCompaction() == nil {
		t.Fatal("no compaction picked after one was installed before it ran")
	}
	big := newTree(t, 512)
	for i := range 4 {
		for j := range 400 {
			put(t, big, nil, fmt.Sprintf("k%d-%04d", i, j), strings.Repeat("v", 20))
		}
		f.checkpoint(t, big, nil)
	}
	runs := make([]pickRun, big.Runs())
	for i, r := range big.runs {
		runs[i].bytes = r.bytes
	}
	if i, j, ok := pickSizeAmp(runs, maxSizeAmp); !ok || i != 0 || j != 4 {
		t.Fatalf("size amplification picks [%d, %d) %v", i, j, ok)
	}
	if i, j, ok := pickSizeRatio(runs, sizeRatio, minMergeWidth, len(runs)); !ok || i != 0 || j != 4 {
		t.Fatalf("size ratio picks [%d, %d) %v", i, j, ok)
	}
	if c := big.PickCompaction(); c != nil {
		t.Fatalf("picked %v over %d bytes, more than a root of 512-byte pieces lists (%d)", c.rule, big.RunBytes(), big.maxRunBytes())
	}

	stepped, err := New(Config{MaxPiece: 512, StepBytes: 512, Check: true})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 4 {
		for j := range 5 {
			put(t, stepped, nil, fmt.Sprintf("k%d-%d", i, j), strings.Repeat("v", 270))
		}
		f.checkpoint(t, stepped, nil)
	}
	for c := stepped.PickCompaction(); c != nil; c = stepped.PickCompaction() {
		c.Run()
		if err := stepped.Install(c); err != nil {
			t.Fatal(err)
		}
		f.checkpoint(t, stepped, nil)
	}
}

// A compaction installed while a flush writes one of its inputs leaves that flush's manifest
// consistent: it names the inputs it wrote, and the next one names the result and reports them dead.
func TestCompactionDuringFlush(t *testing.T) {
	tr := newTree(t, 512)
	m := model{}
	f := newFakeLog()
	for i := range 4 {
		put(t, tr, m, fmt.Sprint("a", i), "v")
		f.checkpoint(t, tr, nil)
	}
	c := tr.PickCompaction()
	c.Run()
	if err := tr.Install(c); err != nil {
		t.Fatal(err)
	}
	for i := range 4 {
		put(t, tr, m, fmt.Sprint("b", i), "v")
		f.checkpoint(t, tr, nil)
	}
	put(t, tr, m, "c", "v")
	var installed bool
	var pending *Compaction
	roots, _ := f.checkpoint(t, tr, func() {
		if !installed {
			installed = true
			pending = tr.PickCompaction()
			pending.Run()
			if err := tr.Install(pending); err != nil {
				t.Fatal(err)
			}
		}
	})
	if pending == nil || len(roots) != 6 {
		t.Fatalf("compaction %v, roots %v", pending, roots)
	}
	checkReads(t, f.restore(t, 512, roots), m, []string{"a0", "b3", "c"})
	roots, dead := f.checkpoint(t, tr, nil)
	if len(roots) != 2 || len(dead) == 0 {
		t.Fatalf("roots %v, dead %v", roots, dead)
	}
	checkReads(t, f.restore(t, 512, roots), m, []string{"a0", "b3", "c"})
}

// A checkpoint that fails after writing a compaction's result whole carries that run next time.
func TestFlushFailedAfterARunCompleted(t *testing.T) {
	tr := newTree(t, 512)
	m := model{}
	f := newFakeLog()
	for i := range 4 {
		put(t, tr, m, fmt.Sprint("a", i), "v")
		f.checkpoint(t, tr, nil)
	}
	c := tr.PickCompaction()
	c.Run()
	if err := tr.Install(c); err != nil {
		t.Fatal(err)
	}
	put(t, tr, m, "b", "v")
	if _, err := tr.BeginFlush(); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 512)
	for root := false; !root; {
		n, r, err := tr.NextPiece(buf)
		if err != nil || n == 0 {
			t.Fatalf("compacted run's piece of %d bytes: %v", n, err)
		}
		if err := tr.PieceDurable(f.put(buf[:n])); err != nil {
			t.Fatal(err)
		}
		root = r
	}
	if dead := tr.FlushFailed(); len(dead) != 0 || !tr.Frozen() {
		t.Fatalf("dead %v, frozen %v", dead, tr.Frozen())
	}
	if _, err := tr.BeginFlush(); err != nil {
		t.Fatal(err)
	}
	if len(tr.CarriedRuns()) != 1 {
		t.Fatalf("carried %v, want the compacted run", tr.CarriedRuns())
	}
	tr.FlushFailed()
	roots, _ := f.checkpoint(t, tr, nil)
	checkReads(t, f.restore(t, 512, roots), m, []string{"a0", "b"})
}
