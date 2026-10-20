package chunkstore

import (
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

const (
	epochChunks = 4
	epochBlocks = 16
)

// epochWriter is a primary of one volume. Writes carry the volume's snapshot epoch; the first
// write to a chunk whose entry holds an older epoch puts a freeze of the chunk's current version
// in its record, version 0 when the chunk has no entry.
type epochWriter struct {
	x       *Index
	epoch   uint64
	version uint64
	lba     core.LBA
	records []*disklog.Event
	absents int
}

func (w *epochWriter) write(t *testing.T, c core.ChunkID, first, count uint16, punch bool) core.LBA {
	t.Helper()
	ch, _ := w.x.Chunk(1, c)
	var entries []disklog.Entry
	if w.epoch > ch.Epoch {
		entries = append(entries, disklog.FreezeEntry(1, c, ch.Version, w.epoch))
		if ch.Version == 0 {
			w.absents++
		}
	}
	w.version++
	if punch {
		entries = append(entries, disklog.PunchEntry(1, c, w.version, first, count))
	} else {
		entries = append(entries, disklog.WriteEntry(1, c, w.version, first, count))
	}
	lba := w.lba
	ev := record(lba, entries...)
	w.records = append(w.records, ev)
	w.lba += core.LBA(1 + int(count))
	apply(t, w.x, ev)
	return lba
}

type modelBlock struct {
	ok      bool
	version uint64
	lba     core.LBA
}

// volumeModel holds each block's version and location, and whether each chunk exists: a block a
// chunk that exists never wrote reads as zeros at version 0, one of a chunk that does not as a miss.
type volumeModel struct {
	blocks [epochChunks][epochBlocks]modelBlock
	exists [epochChunks]bool
}

func (m *volumeModel) write(c int, first, count uint16, version uint64, lba core.LBA, punch bool) {
	m.exists[c] = true
	for b := first; b < first+count; b++ {
		m.blocks[c][b] = modelBlock{ok: true, version: version}
		if !punch {
			m.blocks[c][b].lba = lba + 1 + core.LBA(b-first)
		}
	}
}

func (m *volumeModel) read(c int, b uint16) modelBlock {
	if m.blocks[c][b].ok || !m.exists[c] {
		return m.blocks[c][b]
	}
	return modelBlock{ok: true}
}

// snapshotMismatches counts the blocks a read at each snapshot epoch finds otherwise than snaps
// holds them, snaps[s-1] being snapshot s, and describes the first.
func snapshotMismatches(x *Index, snaps []volumeModel) (int, string) {
	n, first := 0, ""
	for s, snap := range snaps {
		for c := range epochChunks {
			for b := range uint16(epochBlocks) {
				want := snap.read(c, b)
				e, ok := x.LookupAt(1, chunk(uint32(c)), b, uint64(s+1))
				got := modelBlock{ok: ok}
				if ok {
					got.version, got.lba = e.Version, e.BlockLBA(b)
				}
				if got != want {
					if n == 0 {
						first = fmt.Sprintf("snapshot %d chunk %d block %d: %+v, want %+v", s+1, c, b, got, want)
					}
					n++
				}
			}
		}
	}
	return n, first
}

// E1: a chunk first written after cut 1 and written again before cut 2 is absent from snapshot 1.
func TestAbsentFreezeE1(t *testing.T) {
	w := &epochWriter{x: newIndex(t), epoch: 1, lba: 100}
	w.write(t, chunk(0), 0, 1, false)
	w.write(t, chunk(0), 1, 1, false)
	if e, ok := w.x.LookupAt(1, chunk(0), 0, 1); ok {
		t.Fatalf("snapshot 1 block 0 reads %+v, written after its cut", e)
	}
}

// E2: a chunk first written after cut 1, then cut 2 and a write: snapshot 1 reads nothing and
// snapshot 2 reads version 1.
func TestAbsentFreezeE2(t *testing.T) {
	w := &epochWriter{x: newIndex(t), epoch: 1, lba: 100}
	w.write(t, chunk(0), 0, 1, false)
	w.epoch = 2
	w.write(t, chunk(0), 0, 1, false)
	if e, ok := w.x.LookupAt(1, chunk(0), 0, 1); ok {
		t.Fatalf("snapshot 1 block 0 reads %+v, written after its cut", e)
	}
	if e, ok := w.x.LookupAt(1, chunk(0), 0, 2); !ok || e.Version != 1 {
		t.Fatalf("snapshot 2 block 0 reads %+v %v, want version 1", e, ok)
	}
}

// E3 [property]: random cuts, writes and punches over 4 chunks of 16 blocks; every block of every
// snapshot reads as a model of whole-volume copies holds it, and a rebuild from the records, and
// the records applied twice, give the same index.
func TestAbsentFreezeE3(t *testing.T) {
	const seeds, steps = 50, 400
	badSeeds, absents := 0, 0
	for seed := range uint64(seeds) {
		r := rand.New(rand.NewPCG(seed, 10))
		w := &epochWriter{x: newIndex(t), lba: 100}
		var live volumeModel
		var snaps []volumeModel
		for range steps {
			if r.IntN(10) == 0 {
				w.epoch++
				snaps = append(snaps, live)
				continue
			}
			c := r.IntN(epochChunks)
			first := uint16(r.IntN(epochBlocks))
			count := uint16(1 + r.IntN(min(4, epochBlocks-int(first))))
			punch := r.IntN(6) == 0
			lba := w.write(t, chunk(uint32(c)), first, count, punch)
			live.write(c, first, count, w.version, lba, punch)
		}
		absents += w.absents
		n, first := snapshotMismatches(w.x, snaps)
		if n > 0 {
			badSeeds++
			t.Errorf("seed %d: %d snapshot block reads wrong; first %s", seed, n, first)
		}
		rebuilt := newIndex(t)
		for _, ev := range w.records {
			apply(t, rebuilt, ev)
		}
		if !slices.Equal(dumpIndex(rebuilt), dumpIndex(w.x)) {
			t.Fatalf("seed %d: rebuild from the records differs", seed)
		}
		for _, ev := range w.records {
			apply(t, rebuilt, ev)
		}
		if !slices.Equal(dumpIndex(rebuilt), dumpIndex(w.x)) {
			t.Fatalf("seed %d: the records applied twice differ", seed)
		}
	}
	if badSeeds > 0 {
		t.Errorf("%d of %d seeds read a snapshot wrong", badSeeds, seeds)
	}
	if absents == 0 {
		t.Fatal("no seed froze an absent chunk")
	}
}

// The scenarios of the absent freeze section of docs/test-scenarios.md.
func TestAbsentFreezeScenarios(t *testing.T) {
	x := newIndex(t)
	c := chunk(9)
	var log []*disklog.Event
	step := func(ev *disklog.Event) {
		log = append(log, ev)
		apply(t, x, ev)
		before := dumpIndex(x)
		apply(t, x, ev)
		if !slices.Equal(dumpIndex(x), before) {
			t.Fatalf("record at %d applied again changed the index", ev.LBA)
		}
	}
	readAt := func(b uint16, epoch uint64) (Extent, bool) { return x.LookupAt(1, c, b, epoch) }

	step(record(100, disklog.FreezeEntry(1, c, 0, 1), disklog.WriteEntry(1, c, 1, 0, 5)))
	if ch, _ := x.Chunk(1, c); ch != (Chunk{Version: 1, Epoch: 1}) {
		t.Fatalf("chunk 9 entry %+v, want v1 at epoch 1", ch)
	}
	marker := binary.BigEndian.AppendUint64(appendChunkPrefix(nil, 1, c), 0)
	if v, ok := x.Tree().Get(marker); !ok || binary.LittleEndian.Uint64(v) != 1 {
		t.Fatalf("absent version under (…, 0) holds %x %v, want epoch 1", v, ok)
	}
	if v, ok := x.FrozenAt(1, c, 1); !ok || v != 0 {
		t.Fatalf("snapshot 1 reads version %d %v, want the absent version 0", v, ok)
	}
	if e, ok := readAt(0, 1); ok {
		t.Fatalf("a read at epoch 1 found %+v", e)
	}
	if e, ok := readAt(7, 1); ok {
		t.Fatalf("a read at epoch 1 of an unwritten block found %+v", e)
	}
	if x.Live(100) != 5 {
		t.Fatalf("record 100 refers to %d blocks, want 5: the absent version holds none", x.Live(100))
	}

	step(record(200, disklog.WriteEntry(1, c, 2, 1, 1)))
	step(record(300, disklog.FreezeEntry(1, c, 2, 2), disklog.WriteEntry(1, c, 3, 0, 1)))
	if e, ok := readAt(1, 1); ok {
		t.Fatalf("after cut 2 a read at epoch 1 found %+v", e)
	}
	if e, ok := readAt(1, 2); !ok || e.Version != 2 || e.BlockLBA(1) != 201 {
		t.Fatalf("a read at epoch 2 found %+v %v, want v2 at LBA 201", e, ok)
	}
	if got := x.frozen(nil, 1, c); !slices.Equal(got, []uint64{0, 1, 2, 2}) {
		t.Fatalf("frozen versions and epochs %v, want the absent version first: [0 1 2 2]", got)
	}

	if n := x.Move(1, c, 3, 1, 104, 501, 500); n != 2 {
		t.Fatalf("move of block 3 changed %d maps, want the live map and v2", n)
	}
	if e, ok := readAt(3, 1); ok {
		t.Fatalf("after the move a read at epoch 1 found %+v", e)
	}
	if e, _ := readAt(3, 2); e.BlockLBA(3) != 501 {
		t.Fatalf("frozen v2 block 3 at %+v, want the cleaner's copy", e)
	}
	if e, _ := x.Lookup(1, c, 3); e.BlockLBA(3) != 501 {
		t.Fatalf("live block 3 at %+v, want the cleaner's copy", e)
	}
	if err := x.Check(); err != nil {
		t.Fatal(err)
	}

	before := dumpIndex(x)
	apply(t, x, record(100, disklog.FreezeEntry(1, c, 0, 1)))
	if !slices.Equal(dumpIndex(x), before) {
		t.Fatal("a freeze of version 0 replayed for a chunk holding the absent version changed the index")
	}

	rebuilt := newIndex(t)
	for _, ev := range log {
		apply(t, rebuilt, ev)
	}
	if n := rebuilt.Move(1, c, 3, 1, 104, 501, 500); n != 2 || !slices.Equal(dumpIndex(rebuilt), dumpIndex(x)) {
		t.Fatalf("rebuilding from the records gives another index (move changed %d maps)", n)
	}
}

func TestAbsentFreezeOfPunch(t *testing.T) {
	x := newIndex(t)
	apply(t, x, record(100, disklog.FreezeEntry(1, chunk(9), 0, 1), disklog.PunchEntry(1, chunk(9), 1, 0, 4)))
	if e, ok := x.LookupAt(1, chunk(9), 0, 1); ok {
		t.Fatalf("a read at epoch 1 found %+v", e)
	}
	if e, ok := x.Lookup(1, chunk(9), 0); !ok || e.LBA != 0 || e.Version != 1 {
		t.Fatalf("a live read found %+v %v, want zeros at v1", e, ok)
	}
}

// A chunk with a tombstone has a version, so the first write after a cut freezes it, not version 0.
func TestFreezeOfTombstone(t *testing.T) {
	x := newIndex(t)
	apply(t, x, record(100, disklog.PunchEntry(1, chunk(9), 1, 0, core.BlocksPerChunk)))
	if err := x.Apply(record(200, disklog.FreezeEntry(1, chunk(9), 0, 1), disklog.WriteEntry(1, chunk(9), 2, 0, 1))); err == nil {
		t.Fatal("a freeze of version 0 of a punched chunk applied")
	}
	apply(t, x, record(200, disklog.FreezeEntry(1, chunk(9), 1, 1), disklog.WriteEntry(1, chunk(9), 2, 0, 1)))
	if e, ok := x.LookupAt(1, chunk(9), 0, 1); !ok || e.LBA != 0 {
		t.Fatalf("a read at epoch 1 found %+v %v, want zeros", e, ok)
	}
}

func TestCheckFindsFreezeNewerThanEntry(t *testing.T) {
	x := newIndex(t)
	apply(t, x, record(100, disklog.FreezeEntry(1, chunk(9), 0, 1), disklog.WriteEntry(1, chunk(9), 1, 0, 1)))
	if err := x.Tree().Put(binary.BigEndian.AppendUint64(appendChunkPrefix(nil, 1, chunk(9)), 0), binary.LittleEndian.AppendUint64(nil, 2)); err != nil {
		t.Fatal(err)
	}
	if err := x.Check(); err == nil {
		t.Fatal("an absent version frozen at epoch 2 under a chunk entry at epoch 1 passed the check")
	}
}

// A freeze of version 0 after the chunk's first write in the same record copies nothing: the
// absent version's key range is the live map's.
func TestAbsentFreezeAfterWriteCopiesNothing(t *testing.T) {
	x := newIndex(t)
	apply(t, x, record(100, disklog.WriteEntry(1, chunk(9), 1, 0, 4), disklog.FreezeEntry(1, chunk(9), 0, 1)))
	if x.Live(100) != 4 {
		t.Fatalf("record 100 refers to %d blocks, want 4", x.Live(100))
	}
}
