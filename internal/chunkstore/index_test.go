package chunkstore

import (
	"bytes"
	"cmp"
	"errors"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/lsm"
)

func newIndex(t testing.TB) *Index {
	t.Helper()
	tree, err := lsm.New(lsm.Config{MaxPiece: 1 << 20, Check: true})
	if err != nil {
		t.Fatal(err)
	}
	return NewIndex(tree)
}

// record is a durable record of entries at LBA lba with one header block.
func record(lba core.LBA, entries ...disklog.Entry) *disklog.Event {
	ev := &disklog.Event{Kind: disklog.Durable, LBA: lba, HeaderBlocks: 1, Entries: entries}
	for i := range entries {
		ev.Payloads = append(ev.Payloads, disklog.PayloadOffset(entries, i))
	}
	return ev
}

func apply(t testing.TB, x *Index, ev *disklog.Event) {
	t.Helper()
	if err := x.Apply(ev); err != nil {
		t.Fatal(err)
	}
	if err := x.Check(); err != nil {
		t.Fatal(err)
	}
}

func chunk(i uint32) core.ChunkID { return core.ChunkID{Volume: 1, Inode: 2, Index: i} }

func extents(x *Index, c core.ChunkID, version uint64) []Extent {
	return x.Extents(nil, 1, c, version)
}

func dumpIndex(x *Index) [][2]string {
	var it lsm.Iter
	x.Tree().Iter(&it)
	var out [][2]string
	for ok := it.SeekGE(nil); ok; ok = it.Next() {
		out = append(out, [2]string{string(it.Key()), string(it.Value())})
	}
	return out
}

// [property] Comparing encoded keys orders them as their tuples (pool, hash of chunk, chunk,
// version, first block) do; keys round-trip, and a chunk's entry sorts before its live map,
// which sorts before its frozen maps.
func TestKeyOrder(t *testing.T) {
	r := rand.New(rand.NewPCG(21, 0))
	randKey := func() Key {
		return Key{
			Pool:    core.PoolID(r.IntN(3)),
			Chunk:   core.ChunkID{Volume: core.VolumeID(r.IntN(3)), Inode: core.InodeID(r.IntN(3)), Index: uint32(r.IntN(3))},
			Version: uint64(r.IntN(3)),
			First:   uint16(r.IntN(3)) * 300,
		}
	}
	compare := func(a, b Key) int {
		ab, bb := a.Chunk.Bytes(), b.Chunk.Bytes()
		return cmp.Or(cmp.Compare(a.Pool, b.Pool), cmp.Compare(a.Chunk.Hash(), b.Chunk.Hash()),
			bytes.Compare(ab[:], bb[:]), cmp.Compare(a.Version, b.Version), cmp.Compare(a.First, b.First))
	}
	for range 5000 {
		a, b := randKey(), randKey()
		ea, eb := AppendKey(nil, a), AppendKey(nil, b)
		if len(ea) != KeySize || bytes.Compare(ea, eb) != compare(a, b) {
			t.Fatalf("%+v vs %+v: bytes compare %d, tuples %d", a, b, bytes.Compare(ea, eb), compare(a, b))
		}
		if got, err := DecodeKey(ea); err != nil || got != a {
			t.Fatalf("round trip %+v: %+v %v", a, got, err)
		}
	}
	c := chunk(7)
	entry := appendChunkPrefix(nil, 1, c)
	live := AppendKey(nil, Key{Pool: 1, Chunk: c, Version: 0, First: 1023})
	marker := AppendKey(nil, Key{Pool: 1, Chunk: c, Version: 1})[:frozenKeySize]
	frozen := AppendKey(nil, Key{Pool: 1, Chunk: c, Version: 1, First: 0})
	if !slices.IsSortedFunc([][]byte{entry, live, marker, frozen}, bytes.Compare) {
		t.Fatal("chunk entry, live map, frozen marker and frozen map out of order")
	}
	bad := AppendKey(nil, Key{Pool: 1, Chunk: c})
	bad[5] ^= 1
	if _, err := DecodeKey(bad); err == nil {
		t.Fatal("a key whose hash names another chunk decoded")
	}
}

// The scenarios of the chunk index section of docs/test-scenarios.md, in order: apply, trim and
// split, freeze, rebuild, cleaner moves.
func TestIndexScenarios(t *testing.T) {
	x := newIndex(t)
	var log []*disklog.Event
	step := func(ev *disklog.Event) {
		log = append(log, ev)
		apply(t, x, ev)
	}

	step(record(100,
		disklog.WriteEntry(1, chunk(7), 1, 0, 2),
		disklog.BlobEntry(disklog.BlobRaftEntries, 3, 100),
		disklog.WriteEntry(1, chunk(7), 2, 5, 1),
		disklog.PunchEntry(1, chunk(8), 3, 0, core.BlocksPerChunk)))
	want := []Extent{{First: 0, Count: 2, Version: 1, LBA: 101, Header: 100}, {First: 5, Count: 1, Version: 2, LBA: 103, Header: 100}}
	if got := extents(x, chunk(7), 0); !slices.Equal(got, want) {
		t.Fatalf("chunk 7 live map %+v, want %+v", got, want)
	}
	if got := extents(x, chunk(8), 0); len(got) != 0 {
		t.Fatalf("chunk 8 live map %+v, want empty", got)
	}
	if e, ok := x.Lookup(1, chunk(8), 700); !ok || e.LBA != 0 {
		t.Fatalf("block 700 of the punched chunk 8 reads %+v %v, want zeros", e, ok)
	}
	if c7, _ := x.Chunk(1, chunk(7)); c7.Version != 2 {
		t.Fatalf("chunk 7 entry %+v", c7)
	}
	if c8, _ := x.Chunk(1, chunk(8)); c8.Version != 3 {
		t.Fatalf("chunk 8 entry %+v", c8)
	}
	if e, ok := x.Lookup(1, chunk(7), 1); !ok || e.BlockLBA(1) != 102 {
		t.Fatalf("block 1 of chunk 7 at %+v %v", e, ok)
	}
	if e, ok := x.Lookup(1, chunk(7), 2); !ok || e != (Extent{First: 2, Count: 1}) {
		t.Fatalf("unwritten block 2 of chunk 7 reads %+v %v, want zeros", e, ok)
	}
	if e, ok := x.Lookup(1, chunk(9), 2); ok {
		t.Fatalf("chunk 9, which has no entry, reads %+v", e)
	}
	if x.Live(100) != 3 {
		t.Fatalf("record 100 refers to %d blocks, want 3", x.Live(100))
	}

	step(record(200, disklog.WriteEntry(1, chunk(7), 3, 1, 1)))
	want = []Extent{{0, 1, 1, 101, 100}, {1, 1, 3, 201, 200}, {5, 1, 2, 103, 100}}
	if got := extents(x, chunk(7), 0); !slices.Equal(got, want) {
		t.Fatalf("after the split %+v, want %+v", got, want)
	}
	step(record(300, disklog.WriteEntry(1, chunk(7), 4, 0, 4)))
	want = []Extent{{0, 4, 4, 301, 300}, {5, 1, 2, 103, 100}}
	if got := extents(x, chunk(7), 0); !slices.Equal(got, want) {
		t.Fatalf("after the trim %+v, want %+v", got, want)
	}
	if x.Live(100) != 1 || x.Live(200) != 0 || x.Live(300) != 4 {
		t.Fatalf("live counts %d %d %d, want 1 0 4", x.Live(100), x.Live(200), x.Live(300))
	}

	step(record(400, disklog.FreezeEntry(1, chunk(7), 4, 2), disklog.WriteEntry(1, chunk(7), 5, 2, 1)))
	if got := extents(x, chunk(7), 4); !slices.Equal(got, want) {
		t.Fatalf("frozen v4 map %+v, want %+v", got, want)
	}
	if e, ok := x.LookupAt(1, chunk(7), 2, 1); !ok || e.Version != 4 || e.BlockLBA(2) != 303 {
		t.Fatalf("block 2 at epoch 1: %+v %v", e, ok)
	}
	if e, ok := x.Lookup(1, chunk(7), 2); !ok || e.Version != 5 || e.BlockLBA(2) != 401 {
		t.Fatalf("live block 2: %+v %v", e, ok)
	}
	if e, ok := x.LookupAt(1, chunk(7), 2, 2); !ok || e.Version != 4 {
		t.Fatalf("block 2 at epoch 2, whose cut the freezing write followed: %+v %v", e, ok)
	}
	if e, ok := x.LookupAt(1, chunk(7), 2, 3); !ok || e.Version != 5 {
		t.Fatalf("block 2 at epoch 3: %+v %v", e, ok)
	}
	if c7, _ := x.Chunk(1, chunk(7)); c7 != (Chunk{Version: 5, Epoch: 2}) {
		t.Fatalf("chunk 7 entry %+v", c7)
	}
	if x.Live(300) != 4+3 {
		t.Fatalf("record 300 refers to %d blocks, want 7: 4 frozen, 3 live", x.Live(300))
	}
	before := dumpIndex(x)
	apply(t, x, record(400, disklog.FreezeEntry(1, chunk(7), 4, 2)))
	if !slices.Equal(dumpIndex(x), before) {
		t.Fatal("a freeze applied again changed the index")
	}

	rebuilt := newIndex(t)
	for _, ev := range log {
		apply(t, rebuilt, ev)
	}
	if !slices.Equal(dumpIndex(rebuilt), dumpIndex(x)) {
		t.Fatal("rebuilding from the records in LSN order gives another index")
	}

	if n := x.Move(1, chunk(7), 5, 1, 103, 501, 500); n != 2 {
		t.Fatalf("move of block 5 changed %d maps, want the live and frozen one sharing it: 2", n)
	}
	if e, _ := x.Lookup(1, chunk(7), 5); e.BlockLBA(5) != 501 || e.Header != 500 {
		t.Fatalf("moved block 5 at %+v", e)
	}
	if e, _ := x.LookupAt(1, chunk(7), 5, 1); e.BlockLBA(5) != 501 {
		t.Fatalf("frozen block 5 stayed: %+v", e)
	}
	if n := x.Move(1, chunk(7), 5, 1, 103, 601, 600); n != 0 || x.Live(100) != 0 {
		t.Fatalf("a stale move changed %d maps; record 100 refers to %d blocks", n, x.Live(100))
	}
	if err := x.Check(); err != nil {
		t.Fatal(err)
	}
}

func TestMoves(t *testing.T) {
	x := newIndex(t)
	apply(t, x, record(100, disklog.WriteEntry(1, chunk(7), 1, 0, 4)))
	apply(t, x, record(200, disklog.FreezeEntry(1, chunk(7), 1, 1), disklog.WriteEntry(1, chunk(7), 2, 3, 1)))
	if n := x.Move(1, chunk(7), 1, 2, 102, 301, 300); n != 2 {
		t.Fatalf("move shared by the live and frozen map changed %d maps", n)
	}
	want := []Extent{{0, 1, 1, 101, 100}, {1, 2, 1, 301, 300}, {3, 1, 2, 201, 200}}
	if got := extents(x, chunk(7), 0); !slices.Equal(got, want) {
		t.Fatalf("live map %+v, want %+v", got, want)
	}
	if x.Live(100) != 1+2 || x.Live(300) != 4 {
		t.Fatalf("live counts 100: %d, 300: %d", x.Live(100), x.Live(300))
	}
	if n := x.Move(1, chunk(7), 1, 2, 102, 401, 400); n != 0 {
		t.Fatalf("a move from a location no map points at changed %d maps", n)
	}
	if n := x.Move(1, chunk(7), 3, 1, 201, 401, 400); n != 1 {
		t.Fatalf("a move of a block only the live map holds changed %d maps", n)
	}
	apply(t, x, record(500, disklog.WriteEntry(1, chunk(7), 3, 0, 1)))
	if n := x.Move(1, chunk(7), 0, 1, 101, 601, 600); n != 1 {
		t.Fatalf("a move after a write replaced the live block changed %d maps, want the frozen one", n)
	}
	if err := x.Check(); err != nil {
		t.Fatal(err)
	}
}

func TestApplyRejectsReservedPool(t *testing.T) {
	x := newIndex(t)
	ev := record(100, disklog.WriteEntry(1, chunk(1), 1, 0, 1), disklog.WriteEntry(RecordPool, chunk(1), 1, 0, 1))
	if err := x.Apply(ev); err == nil {
		t.Fatal("a write to RecordPool applied")
	}
	if len(dumpIndex(x)) != 0 {
		t.Fatal("a rejected record left entries")
	}
}

func TestApplyRejectsFreezeOfLiveMap(t *testing.T) {
	x := newIndex(t)
	apply(t, x, record(100, disklog.WriteEntry(1, chunk(1), 1, 0, 4)))
	before := dumpIndex(x)
	err := x.Apply(record(200, disklog.WriteEntry(1, chunk(2), 1, 0, 1), disklog.FreezeEntry(1, chunk(1), 0, 1)))
	if !errors.Is(err, ErrFrozenLive) {
		t.Fatalf("a freeze of version 0 of a chunk that exists with no absent version: %v", err)
	}
	if !slices.Equal(dumpIndex(x), before) {
		t.Fatal("a refused record changed the index")
	}
	apply(t, x, record(300, disklog.FreezeEntry(1, chunk(3), 0, 1), disklog.WriteEntry(1, chunk(3), 2, 0, 1)))
}

func TestCheckFindsOverlap(t *testing.T) {
	x := newIndex(t)
	apply(t, x, record(100, disklog.WriteEntry(1, chunk(1), 1, 0, 4)))
	x.put(x.extentKey(1, chunk(1), 0, 2), Extent{First: 2, Count: 1, Version: 9, LBA: 900, Header: 100})
	if err := x.Check(); err == nil {
		t.Fatal("two extents holding block 2 passed the check")
	}
}

// [property] Random writes, punches, freezes and cleaner moves keep every invariant, and a read of
// each block at every snapshot epoch finds the version and location a model of whole-map copies holds.
func TestIndexMatchesModel(t *testing.T) {
	const blocks = 64
	type blockState struct{ version, lba uint64 }
	r := rand.New(rand.NewPCG(22, 0))
	for trial := range 20 {
		x := newIndex(t)
		live := map[uint32][]blockState{}
		type frozenCopy struct {
			epoch uint64
			state []blockState
		}
		frozen := map[uint32][]frozenCopy{}
		version, epoch := uint64(0), uint64(0)
		lba := core.LBA(100)
		for range 300 {
			c := uint32(r.IntN(3))
			if live[c] == nil {
				live[c] = make([]blockState, blocks)
			}
			first := uint16(r.IntN(blocks))
			count := uint16(1 + r.IntN(min(8, blocks-int(first))))
			version++
			var entries []disklog.Entry
			if r.IntN(6) == 0 {
				if ch, ok := x.Chunk(1, chunk(c)); ok {
					epoch++
					entries = append(entries, disklog.FreezeEntry(1, chunk(c), ch.Version, epoch))
					frozen[c] = append(frozen[c], frozenCopy{epoch, slices.Clone(live[c])})
				}
			}
			if r.IntN(5) == 0 {
				entries = append(entries, disklog.PunchEntry(1, chunk(c), version, first, count))
				for b := first; b < first+count; b++ {
					live[c][b] = blockState{version, 0}
				}
			} else {
				entries = append(entries, disklog.WriteEntry(1, chunk(c), version, first, count))
				for b := first; b < first+count; b++ {
					live[c][b] = blockState{version, uint64(lba) + 1 + uint64(b-first)}
				}
			}
			ev := record(lba, entries...)
			apply(t, x, ev)
			lba += core.LBA(1 + int(count))
			if r.IntN(4) == 0 {
				b := uint16(r.IntN(blocks))
				from := live[c][b].lba
				if r.IntN(3) == 0 && len(frozen[c]) > 0 {
					from = frozen[c][r.IntN(len(frozen[c]))].state[b].lba
				}
				if from == 0 {
					continue
				}
				want := 0
				to := uint64(lba) + 1
				for _, st := range append([][]blockState{live[c]}, func() (out [][]blockState) {
					for _, f := range frozen[c] {
						out = append(out, f.state)
					}
					return
				}()...) {
					if st[b].lba == from {
						st[b].lba = to
						want++
					}
				}
				if n := x.Move(1, chunk(c), b, 1, core.LBA(from), core.LBA(to), lba); n != want {
					t.Fatalf("trial %d: move of chunk %d block %d changed %d maps, want %d", trial, c, b, n, want)
				}
				if err := x.Check(); err != nil {
					t.Fatal(err)
				}
				lba += 2
			}
		}
		for c, state := range live {
			check := func(e Extent, ok bool, want blockState, b uint16, at string) {
				got := blockState{}
				if ok {
					got = blockState{e.Version, uint64(e.BlockLBA(b))}
				}
				if got != want {
					t.Fatalf("trial %d chunk %d block %d %s: %+v, want %+v", trial, c, b, at, got, want)
				}
			}
			for b := range uint16(blocks) {
				e, ok := x.Lookup(1, chunk(c), b)
				check(e, ok, state[b], b, "live")
				for s := uint64(0); s <= epoch; s++ {
					want := state[b]
					for _, f := range frozen[c] {
						if f.epoch >= s {
							want = f.state[b]
							break
						}
					}
					e, ok := x.LookupAt(1, chunk(c), b, s)
					check(e, ok, want, b, "at an epoch")
				}
			}
		}
	}
}

func BenchmarkLookup(b *testing.B) {
	x := newIndex(b)
	r := rand.New(rand.NewPCG(1, 1))
	lba := core.LBA(100)
	for range 20000 {
		first := uint16(r.IntN(core.BlocksPerChunk))
		apply(b, x, record(lba, disklog.WriteEntry(1, chunk(uint32(r.IntN(64))), 1, first, 1)))
		lba += 2
	}
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		x.Lookup(1, chunk(uint32(i%64)), uint16(i*7919%core.BlocksPerChunk))
		i++
	}
}

func BenchmarkApplyWrite(b *testing.B) {
	x := newIndex(b)
	ev := record(100, disklog.WriteEntry(1, chunk(1), 1, 0, 1))
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		ev.Entries[0].Chunk.Index = uint32(i % 64)
		ev.Entries[0].First = uint16(i * 7919 % core.BlocksPerChunk)
		ev.LBA = core.LBA(100 + 2*(i%4096))
		if err := x.Apply(ev); err != nil {
			b.Fatal(err)
		}
		i++
	}
}
