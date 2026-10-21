package chunkstore

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/lsm"
)

func newIndexWith(t testing.TB, cfg lsm.Config, pointRemovals bool) *Index {
	t.Helper()
	tree, err := lsm.New(TreeConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	x := NewIndex(tree, 0)
	x.pointRemovals = pointRemovals
	return x
}

func compactAll(t testing.TB, tree *lsm.Tree) {
	t.Helper()
	for c := tree.PickCompaction(); c != nil; c = tree.PickCompaction() {
		c.Run()
		if err := tree.Install(c); err != nil {
			t.Fatal(err)
		}
	}
}

// applyBoth applies ev to an index with range removals and one with point removals; both must
// then hold the same entries.
func applyBoth(t testing.TB, x, p *Index, ev *disklog.Event) {
	t.Helper()
	apply(t, x, ev)
	apply(t, p, ev)
	if !slices.Equal(dumpIndex(x), dumpIndex(p)) {
		t.Fatalf("record at %d: range and point removals read different indexes", ev.LBA)
	}
}

// The chunk index uses of the range removals scenarios: a write over two or more whole extents
// removes their keys with one range removal, over one with a point removal; a whole-chunk punch
// keeps the entry and empties the live map; releasing frozen version v removes (…, v) .. (…, v+1);
// releasing the absent version removes no keys.
func TestRangeRemovalUses(t *testing.T) {
	x, p := newIndexWith(t, lsm.Config{MaxPiece: 1 << 20, Check: true}, false), newIndexWith(t, lsm.Config{MaxPiece: 1 << 20, Check: true}, true)
	for i := range uint16(5) {
		applyBoth(t, x, p, record(100+core.LBA(10*i), disklog.WriteEntry(1, chunk(7), 1+uint64(i), 2*i, 2)))
	}
	applyBoth(t, x, p, record(200, disklog.WriteEntry(1, chunk(7), 6, 1, 6)))
	want := []Extent{{0, 1, 1, 101, 100}, {1, 6, 6, 201, 200}, {7, 1, 4, 132, 130}, {8, 2, 5, 141, 140}}
	if got := extents(x, chunk(7), 0); !slices.Equal(got, want) {
		t.Fatalf("after a write over two whole extents: %+v, want %+v", got, want)
	}
	before := x.Tree().Searches()
	if e, ok := x.Lookup(1, chunk(7), 3); !ok || e.Version != 6 || x.Tree().Searches()-before != 1 {
		t.Fatalf("block 3 reads %+v %v in %d searches", e, ok, x.Tree().Searches()-before)
	}

	for i := range uint16(3) {
		applyBoth(t, x, p, record(300+core.LBA(10*i), disklog.WriteEntry(1, chunk(8), 1+uint64(i), 2*i, 2)))
	}
	applyBoth(t, x, p, record(400, disklog.WriteEntry(1, chunk(8), 4, 1, 3)))
	want = []Extent{{0, 1, 1, 301, 300}, {1, 3, 4, 401, 400}, {4, 2, 3, 321, 320}}
	if got := extents(x, chunk(8), 0); !slices.Equal(got, want) {
		t.Fatalf("after a write over one whole extent: %+v, want %+v", got, want)
	}

	c := chunk(9)
	applyBoth(t, x, p, record(500, disklog.WriteEntry(1, c, 1, 0, 4)))
	applyBoth(t, x, p, record(600, disklog.FreezeEntry(1, c, 1, 1), disklog.WriteEntry(1, c, 2, 1, 1)))
	applyBoth(t, x, p, record(700, disklog.FreezeEntry(1, c, 2, 2), disklog.WriteEntry(1, c, 3, 2, 1)))
	applyBoth(t, x, p, record(800, disklog.PunchEntry(1, c, 4, 0, core.BlocksPerChunk)))
	if got := extents(x, c, 0); len(got) != 0 {
		t.Fatalf("live map after a whole-chunk punch: %+v", got)
	}
	if ch, ok := x.Chunk(1, c); !ok || ch.Version != 4 {
		t.Fatalf("chunk entry after the punch %+v %v, want the tombstone at v4", ch, ok)
	}
	if e, ok := x.Lookup(1, c, 1); !ok || e.LBA != 0 {
		t.Fatalf("punched block 1 reads %+v %v, want zeros", e, ok)
	}
	if e, ok := x.LookupAt(1, c, 1, 1); !ok || e.Version != 1 || e.BlockLBA(1) != 502 {
		t.Fatalf("block 1 at epoch 1 reads %+v %v, want v1 at 502", e, ok)
	}
	if x.Live(500) != 4+3 || x.Live(600) != 1 || x.Live(700) != 0 {
		t.Fatalf("live counts %d %d %d after the punch, want the frozen maps' 7 1 0", x.Live(500), x.Live(600), x.Live(700))
	}

	x.Release(1, c, 1)
	p.Release(1, c, 1)
	if !slices.Equal(dumpIndex(x), dumpIndex(p)) {
		t.Fatal("releasing v1 with a range removal and with point removals differ")
	}
	if got := x.frozen(nil, 1, c); !slices.Equal(got, []uint64{2, 2}) {
		t.Fatalf("frozen versions after releasing v1: %v", got)
	}
	if got := extents(x, c, 1); len(got) != 0 || x.Live(500) != 3 || x.Live(600) != 1 {
		t.Fatalf("v1's map %+v, live counts %d %d", got, x.Live(500), x.Live(600))
	}
	if e, ok := x.LookupAt(1, c, 1, 2); !ok || e.Version != 2 {
		t.Fatalf("v2 block 1 reads %+v %v", e, ok)
	}
	if err := x.Check(); err != nil {
		t.Fatal(err)
	}
	before2 := dumpIndex(x)
	x.Release(1, c, 1)
	x.Release(1, c, 0)
	if !slices.Equal(dumpIndex(x), before2) {
		t.Fatal("releasing a released version or the absent version changed the index")
	}
}

// Releasing the absent version removes no keys: the live map reads as before, and the record with
// that freeze of version 0, replayed after the release, changes nothing and is not refused.
func TestReleaseAbsentVersion(t *testing.T) {
	x := newIndex(t)
	c := chunk(9)
	ev := record(100, disklog.FreezeEntry(1, c, 0, 1), disklog.WriteEntry(1, c, 1, 0, 5))
	apply(t, x, ev)
	before := dumpIndex(x)
	x.Release(1, c, 0)
	if !slices.Equal(dumpIndex(x), before) {
		t.Fatal("releasing the absent version changed the index")
	}
	if e, ok := x.Lookup(1, c, 3); !ok || e.BlockLBA(3) != 104 {
		t.Fatalf("live block 3 reads %+v %v", e, ok)
	}
	apply(t, x, ev)
	if !slices.Equal(dumpIndex(x), before) {
		t.Fatal("the freeze of version 0 replayed after the release changed the index")
	}
}

// Chunk 7 written as 1,024 one-block extents, checkpointed, then rewritten in one 1,024-block write:
// a lookup of every block takes at most two searches before the checkpoint, after it and once the
// writes that follow have merged every run into one, where point removals step over the old
// extents; a 1-block write then splits the new extent.
func TestRewriteOver1024Extents(t *testing.T) {
	cfg := lsm.Config{MaxPiece: replayMaxPiece, RunLimit: 1, Check: true}
	x, p := newIndexWith(t, cfg, false), newIndexWith(t, cfg, true)
	sx, sp := &runStore{blobs: map[disklog.BlobRef][]byte{}}, &runStore{blobs: map[disklog.BlobRef][]byte{}}
	lba := core.LBA(100)
	for b := range uint16(core.BlocksPerChunk) {
		ev := record(lba, disklog.WriteEntry(1, chunk(7), 1+uint64(b), b, 1))
		x.must(x.Apply(ev))
		p.must(p.Apply(ev))
		lba += 2
	}
	sx.checkpoint(t, x.Tree())
	sp.checkpoint(t, p.Tree())
	applyBoth(t, x, p, record(lba, disklog.WriteEntry(1, chunk(7), 2000, 0, core.BlocksPerChunk)))
	lba += 1 + core.BlocksPerChunk
	lookups := func(x *Index, when string, limit int) int {
		t.Helper()
		worst := 0
		for b := range uint16(core.BlocksPerChunk) {
			before := x.Tree().Searches()
			if e, ok := x.Lookup(1, chunk(7), b); !ok || e.Version != 2000 {
				t.Fatalf("%s: block %d reads %+v %v", when, b, e, ok)
			}
			worst = max(worst, x.Tree().Searches()-before)
		}
		if limit > 0 && worst > limit {
			t.Fatalf("%s: a lookup took %d searches", when, worst)
		}
		return worst
	}
	lookups(x, "in the memtable", 2)
	if n := lookups(p, "point removals in the memtable", 0); n < 100 {
		t.Fatalf("point removals took at most %d searches; the scenario no longer tells them apart", n)
	}
	sx.checkpoint(t, x.Tree())
	sp.checkpoint(t, p.Tree())
	lookups(x, "flushed", 2)
	if n := lookups(p, "point removals flushed", 0); n < 100 {
		t.Fatalf("flushed point removals took at most %d searches", n)
	}
	for i := 0; x.Tree().Runs() != 1; i++ {
		if i == 40 {
			t.Fatalf("%d runs after %d checkpoints and compactions", x.Tree().Runs(), i)
		}
		for b := range uint16(core.BlocksPerChunk / 2) {
			ev := record(lba, disklog.WriteEntry(1, chunk(8), uint64(i+1), 2*b, 1))
			x.must(x.Apply(ev))
			p.must(p.Apply(ev))
			lba += 2
		}
		sx.checkpoint(t, x.Tree())
		compactAll(t, x.Tree())
	}
	lookups(x, "merged", 2)

	applyBoth(t, x, p, record(lba, disklog.WriteEntry(1, chunk(7), 2001, 700, 1)))
	got := extents(x, chunk(7), 0)
	if len(got) != 3 || got[0].First != 0 || got[0].Count != 700 || got[1].First != 700 || got[1].Version != 2001 ||
		got[2].First != 701 || got[2].Count != core.BlocksPerChunk-701 || got[2].Version != 2000 {
		t.Fatalf("after a 1-block write at 700: %+v", got)
	}
}

// [property] Seeds 0-5, 1,500 random writes, punches, freezes, releases and whole rewrites over 4
// chunks, with checkpoints and compactions: every block, live and at every snapshot epoch, reads
// as an index built with point removals does, through Lookup and through a Reader, and the
// invariants hold.
func TestRangeRemovalsMatchPointRemovals(t *testing.T) {
	const chunks = 4
	for seed := range uint64(6) {
		r := rand.New(rand.NewPCG(seed, 30))
		cfg := lsm.Config{MaxPiece: replayMaxPiece, Check: true}
		x, p := newIndexWith(t, cfg, false), newIndexWith(t, cfg, true)
		sx, sp := &runStore{blobs: map[disklog.BlobRef][]byte{}}, &runStore{blobs: map[disklog.BlobRef][]byte{}}
		var epoch, version uint64
		var frozen [][2]uint64
		lba := core.LBA(100)
		var rx, rp Reader
		compare := func(step int) {
			t.Helper()
			for c := range uint32(chunks) {
				x.Open(&rx, 1, chunk(c))
				p.Open(&rp, 1, chunk(c))
				for b := range uint16(core.BlocksPerChunk) {
					ex, okx := x.Lookup(1, chunk(c), b)
					ep, okp := p.Lookup(1, chunk(c), b)
					if ex != ep || okx != okp {
						t.Fatalf("seed %d step %d chunk %d block %d: %+v %v, point removals %+v %v", seed, step, c, b, ex, okx, ep, okp)
					}
					if rex, rokx := rx.Lookup(b); rex != ex || rokx != okx {
						t.Fatalf("seed %d step %d chunk %d block %d: reader %+v %v, lookup %+v %v", seed, step, c, b, rex, rokx, ex, okx)
					}
					if rep, rokp := rp.Lookup(b); rep != ep || rokp != okp {
						t.Fatalf("seed %d step %d chunk %d block %d: point removals' reader %+v %v, lookup %+v %v", seed, step, c, b, rep, rokp, ep, okp)
					}
				}
				for s := uint64(1); s <= epoch; s++ {
					x.OpenAt(&rx, 1, chunk(c), s)
					for range 16 {
						b := uint16(r.IntN(core.BlocksPerChunk))
						ex, okx := x.LookupAt(1, chunk(c), b, s)
						ep, okp := p.LookupAt(1, chunk(c), b, s)
						if ex != ep || okx != okp {
							t.Fatalf("seed %d step %d chunk %d block %d epoch %d: %+v %v, point removals %+v %v", seed, step, c, b, s, ex, okx, ep, okp)
						}
						if rex, rokx := rx.Lookup(b); rex != ex || rokx != okx {
							t.Fatalf("seed %d step %d chunk %d block %d epoch %d: reader %+v %v, lookup %+v %v", seed, step, c, b, s, rex, rokx, ex, okx)
						}
					}
				}
			}
		}
		for step := range 1500 {
			c := chunk(uint32(r.IntN(chunks)))
			switch op := r.IntN(100); {
			case op < 4:
				epoch++
				continue
			case op < 8:
				sx.checkpoint(t, x.Tree())
				sp.checkpoint(t, p.Tree())
				compactAll(t, x.Tree())
				compactAll(t, p.Tree())
				continue
			case op < 10:
				if len(frozen) > 0 {
					i := r.IntN(len(frozen))
					f := frozen[i]
					frozen = slices.Delete(frozen, i, i+1)
					x.Release(1, chunk(uint32(f[0])), f[1])
					p.Release(1, chunk(uint32(f[0])), f[1])
					if err := x.Check(); err != nil {
						t.Fatalf("seed %d step %d: %v", seed, step, err)
					}
				}
				continue
			}
			var entries []disklog.Entry
			if ch, _ := x.Chunk(1, c); epoch > ch.Epoch {
				entries = append(entries, disklog.FreezeEntry(1, c, ch.Version, epoch))
				if ch.Version != 0 {
					frozen = append(frozen, [2]uint64{uint64(c.Index), ch.Version})
				}
			}
			version++
			first := uint16(r.IntN(core.BlocksPerChunk))
			count := uint16(1 + r.IntN(min(8, core.BlocksPerChunk-int(first))))
			switch op := r.IntN(100); {
			case op < 10:
				entries = append(entries, disklog.WriteEntry(1, c, version, 0, core.BlocksPerChunk))
				count = core.BlocksPerChunk
			case op < 15:
				entries = append(entries, disklog.PunchEntry(1, c, version, 0, core.BlocksPerChunk))
				count = 0
			case op < 30:
				entries = append(entries, disklog.WriteEntry(1, c, version, first/64*64, 64))
				count = 64
			case op < 40:
				entries = append(entries, disklog.PunchEntry(1, c, version, first, count))
				count = 0
			default:
				entries = append(entries, disklog.WriteEntry(1, c, version, first, count))
			}
			applyBoth(t, x, p, record(lba, entries...))
			lba += 1 + core.LBA(count)
			if step%500 == 499 {
				compare(step)
			}
		}
		compare(1500)
		if x.Tree().Runs() == 0 || epoch == 0 {
			t.Fatalf("seed %d: %d runs, %d epochs", seed, x.Tree().Runs(), epoch)
		}
	}
}

// BenchmarkLookupRewritten is BenchmarkLookup's 20,000 one-block extents over 64 chunks, flushed,
// with every chunk then rewritten whole, so each lookup meets a range removal.
func BenchmarkLookupRewritten(b *testing.B) {
	x := newIndexWith(b, lsm.Config{MaxPiece: replayMaxPiece}, false)
	r := rand.New(rand.NewPCG(1, 1))
	lba := core.LBA(100)
	for range 20000 {
		first := uint16(r.IntN(core.BlocksPerChunk))
		apply(b, x, record(lba, disklog.WriteEntry(1, chunk(uint32(r.IntN(64))), 1, first, 1)))
		lba += 2
	}
	(&runStore{blobs: map[disklog.BlobRef][]byte{}}).checkpoint(b, x.Tree())
	for c := range uint32(64) {
		apply(b, x, record(lba, disklog.WriteEntry(1, chunk(c), 2, 0, core.BlocksPerChunk)))
		lba += 1 + core.BlocksPerChunk
	}
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		if e, ok := x.Lookup(1, chunk(uint32(i%64)), uint16(i*7919%core.BlocksPerChunk)); !ok || e.Version != 2 {
			b.Fatalf("lookup %d: %+v %v", i, e, ok)
		}
		i++
	}
}

// BenchmarkLookupAfterRewrite times a lookup of chunk 7 rewritten whole over 1,024 flushed one-block
// extents, in the memtable and flushed, with range removals and with point removals.
func BenchmarkLookupAfterRewrite(b *testing.B) {
	for _, points := range []bool{false, true} {
		for _, flushed := range []bool{false, true} {
			b.Run(fmt.Sprintf("points=%v/flushed=%v", points, flushed), func(b *testing.B) {
				x := newIndexWith(b, lsm.Config{MaxPiece: replayMaxPiece}, points)
				s := &runStore{blobs: map[disklog.BlobRef][]byte{}}
				lba := core.LBA(100)
				for blk := range uint16(core.BlocksPerChunk) {
					x.must(x.Apply(record(lba, disklog.WriteEntry(1, chunk(7), 1+uint64(blk), blk, 1))))
					lba += 2
				}
				s.checkpoint(b, x.Tree())
				x.must(x.Apply(record(lba, disklog.WriteEntry(1, chunk(7), 2000, 0, core.BlocksPerChunk))))
				if flushed {
					s.checkpoint(b, x.Tree())
				}
				b.ReportAllocs()
				i := 0
				for b.Loop() {
					if e, ok := x.Lookup(1, chunk(7), uint16(i*7919%core.BlocksPerChunk)); !ok || e.Version != 2000 {
						b.Fatalf("lookup %d: %+v %v", i, e, ok)
					}
					i++
				}
			})
		}
	}
}
