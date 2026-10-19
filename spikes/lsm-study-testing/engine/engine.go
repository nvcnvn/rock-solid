// Package engine drives internal/lsm for the metamorphic harness. plant.sh
// copies this file next to each planted-bug copy of internal/lsm, rewriting
// the import below.
package engine

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/nvcnvn/rock-solid/internal/disklog"
	lsm "github.com/nvcnvn/rock-solid/internal/lsm"

	"github.com/nvcnvn/rock-solid/spikes/lsm-study-testing/meta"
)

type write struct {
	k, v string
	del  bool
}

type manifest struct {
	roots  []disklog.BlobRef
	replay int
}

// Tree is an lsm.Tree over a fake log that keeps every live piece. Its op log
// of writes stands in for the disk log's records: a restart loads the newest
// manifest's runs and replays the writes after its replay point.
type Tree struct {
	piece    int
	tr       *lsm.Tree
	blobs    map[disklog.BlobRef][]byte
	next     uint64
	writes   []write
	man      manifest
	frozenAt int
	views    map[int]*lsm.View
	pending  *lsm.Compaction
	st       stats
}

type stats struct {
	flushes, failed, tooLarge, pieces, compactions, stale, restarts, replayed, maxRuns, maxBlobs int
}

// New makes an empty tree with o's piece size.
func New(o meta.Options) meta.Engine {
	t := &Tree{piece: o.Piece, blobs: map[disklog.BlobRef][]byte{}, views: map[int]*lsm.View{}}
	t.tr = t.newTree()
	return t
}

func (t *Tree) newTree() *lsm.Tree {
	tr, err := lsm.New(lsm.Config{MaxPiece: t.piece})
	if err != nil {
		panic(err)
	}
	return tr
}

func (t *Tree) Put(k, v string) error {
	t.writes = append(t.writes, write{k: k, v: v})
	return t.tr.Put([]byte(k), []byte(v))
}

func (t *Tree) Delete(k string) error {
	t.writes = append(t.writes, write{k: k, del: true})
	return t.tr.Delete([]byte(k))
}

func (t *Tree) Get(k string) (string, bool) {
	v, ok := t.tr.Get([]byte(k))
	return string(v), ok
}

func (t *Tree) Floor(k string) (string, string, bool) {
	fk, v, ok := t.tr.Floor([]byte(k))
	return string(fk), string(v), ok
}

func scan(it *lsm.Iter, lo string, n int) []string {
	out := []string{}
	for ok := it.SeekGE([]byte(lo)); ok && len(out) < n; ok = it.Next() {
		out = append(out, string(it.Key())+"="+string(it.Value()))
	}
	return out
}

func (t *Tree) Scan(lo string, n int) []string {
	var it lsm.Iter
	t.tr.Iter(&it)
	return scan(&it, lo, n)
}

func (t *Tree) ViewOpen(id int) { t.views[id] = t.tr.View() }

func (t *Tree) ViewGet(id int, k string) (string, bool, bool) {
	v, ok := t.views[id]
	if !ok {
		return "", false, false
	}
	val, found := v.Get([]byte(k))
	return string(val), found, true
}

func (t *Tree) ViewScan(id int, lo string, n int) ([]string, bool) {
	v, ok := t.views[id]
	if !ok {
		return nil, false
	}
	var it lsm.Iter
	v.Iter(&it)
	return scan(&it, lo, n), true
}

func (t *Tree) ViewClose(id int) {
	if v, ok := t.views[id]; ok {
		v.Close()
		delete(t.views, id)
	}
}

func (t *Tree) put(b []byte) disklog.BlobRef {
	t.next++
	ref := disklog.BlobRef{Segment: 1, Block: uint32(t.next), LSN: t.next}
	t.blobs[ref] = bytes.Clone(b)
	t.st.maxBlobs = max(t.st.maxBlobs, len(t.blobs))
	return ref
}

// begin starts a checkpoint and returns the replay point its manifest will name.
func (t *Tree) begin() (int, error) {
	froze, err := t.tr.BeginFlush()
	if err != nil {
		return 0, err
	}
	switch {
	case froze:
		t.frozenAt = len(t.writes)
		return t.frozenAt, nil
	case t.tr.Frozen():
		return t.frozenAt, nil
	}
	return len(t.writes), nil
}

// pieces writes the flush's pieces, stopping after limit pieces when limit > 0;
// it reports the roots and whether every piece was written.
func (t *Tree) pieces(limit int, between func()) ([]disklog.BlobRef, bool, error) {
	roots := slices.Clone(t.tr.CarriedRuns())
	buf := make([]byte, t.piece)
	for n := 0; limit == 0 || n < limit; n++ {
		size, root, err := t.tr.NextPiece(buf)
		if err != nil {
			return nil, false, err
		}
		if size == 0 {
			return roots, true, nil
		}
		ref := t.put(buf[:size])
		t.st.pieces++
		if between != nil {
			between()
		}
		if err := t.tr.PieceDurable(ref); err != nil {
			return nil, false, err
		}
		if root {
			roots = append(roots, ref)
		}
	}
	return nil, false, nil
}

func (t *Tree) fail() {
	for _, r := range t.tr.FlushFailed() {
		delete(t.blobs, r)
	}
	t.st.failed++
}

func (t *Tree) done(roots []disklog.BlobRef, replay int) error {
	dead, err := t.tr.CheckpointDone()
	if err != nil {
		return err
	}
	for _, r := range dead {
		delete(t.blobs, r)
	}
	t.man = manifest{roots: roots, replay: replay}
	t.st.flushes++
	t.st.maxRuns = max(t.st.maxRuns, t.tr.Runs())
	return nil
}

func (t *Tree) Flush(between func()) error {
	replay, err := t.begin()
	if err != nil {
		return err
	}
	roots, _, err := t.pieces(0, between)
	if errors.Is(err, lsm.ErrTooLarge) {
		t.st.tooLarge++
		t.fail()
		return nil
	}
	if err != nil {
		return err
	}
	return t.done(roots, replay)
}

func (t *Tree) FailFlush(limit int) error {
	replay, err := t.begin()
	if err != nil {
		return err
	}
	roots, complete, err := t.pieces(limit, nil)
	if errors.Is(err, lsm.ErrTooLarge) {
		t.st.tooLarge++
		t.fail()
		return nil
	}
	if err != nil {
		return err
	}
	if complete {
		return t.done(roots, replay)
	}
	t.fail()
	return nil
}

func (t *Tree) install(c *lsm.Compaction) error {
	c.Run()
	err := t.tr.Install(c)
	if errors.Is(err, lsm.ErrStale) {
		t.st.stale++
		return nil
	}
	if err == nil {
		t.st.compactions++
	}
	return err
}

func (t *Tree) CompactNow() (bool, error) {
	c := t.tr.PickCompaction()
	if c == nil {
		return false, nil
	}
	return true, t.install(c)
}

func (t *Tree) CompactStart() {
	if t.pending == nil {
		t.pending = t.tr.PickCompaction()
	}
}

// CompactFinish runs the held compaction only now, after later writes and
// flushes, since a compaction's Run must not depend on the tree it came from.
func (t *Tree) CompactFinish() error {
	c := t.pending
	t.pending = nil
	if c == nil {
		return nil
	}
	return t.install(c)
}

func (t *Tree) Restart() error {
	for id, v := range t.views {
		v.Close()
		delete(t.views, id)
	}
	t.pending = nil
	tr := t.newTree()
	for _, root := range t.man.roots {
		refs, err := lsm.RootRefs(t.blobs[root], nil)
		if err != nil {
			return fmt.Errorf("root refs: %w", err)
		}
		var ps [][]byte
		for _, r := range append(refs, root) {
			p, ok := t.blobs[r]
			if !ok {
				return fmt.Errorf("manifest names dead piece %+v", r)
			}
			ps = append(ps, p)
		}
		if err := tr.LoadRun(root, ps); err != nil {
			return fmt.Errorf("load run: %w", err)
		}
	}
	for _, w := range t.writes[t.man.replay:] {
		var err error
		if w.del {
			err = tr.Delete([]byte(w.k))
		} else {
			err = tr.Put([]byte(w.k), []byte(w.v))
		}
		if err != nil {
			return err
		}
	}
	t.st.replayed += len(t.writes) - t.man.replay
	t.tr = tr
	t.st.restarts++
	return nil
}

func (t *Tree) Stats() string {
	s := t.st
	return fmt.Sprintf("flushes=%d failed=%d tooLarge=%d pieces=%d compactions=%d stale=%d restarts=%d replayed=%d maxRuns=%d",
		s.flushes, s.failed, s.tooLarge, s.pieces, s.compactions, s.stale, s.restarts, s.replayed, s.maxRuns)
}

// CRCShare builds runs runs of about bytes bytes each from random 24-byte keys,
// and returns the time of one compaction of them and of CRC32C over every piece
// it reads, each the fastest of reps, and the bytes those pieces hold.
func CRCShare(runs, bytes, piece, reps int) (time.Duration, time.Duration, int) {
	tab := crc32.MakeTable(crc32.Castagnoli)
	merge, crc, total := time.Duration(1<<62), time.Duration(1<<62), 0
	for range reps {
		r := rand.New(rand.NewPCG(1, 2))
		t := New(meta.Options{Piece: piece}).(*Tree)
		key := make([]byte, 24)
		for range runs {
			for range bytes / 40 {
				for i := range key {
					key[i] = 'a' + byte(r.IntN(26))
				}
				_ = t.tr.Put(key, key[:12])
			}
			if err := t.Flush(nil); err != nil {
				panic(err)
			}
		}
		total = 0
		for _, b := range t.blobs {
			total += len(b)
		}
		s := time.Now()
		c := t.tr.PickCompaction()
		if c == nil {
			panic("no compaction due")
		}
		c.Run()
		merge = min(merge, time.Since(s))
		var sum uint32
		s = time.Now()
		for _, b := range t.blobs {
			sum ^= crc32.Checksum(b, tab)
		}
		crc = min(crc, time.Since(s))
		_ = sum
	}
	return merge, crc, total
}
