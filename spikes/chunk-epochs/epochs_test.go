package chunkepochs

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// writer is a primary of one volume. Writes carry the volume's snapshot epoch; the first write
// to a chunk whose entry holds an older epoch puts a freeze of the chunk's current version in
// its record. With absentFreeze, a write to a chunk that does not exist yet does too, freezing
// version 0, the absent chunk.
type writer struct {
	ix           index
	absentFreeze bool
	epoch        uint64
	version      uint64
	lba          core.LBA
	records      []*disklog.Event
	freezes      int
	absents      int
}

func (w *writer) write(c core.ChunkID, first, count uint16, punch bool) error {
	ver, ep, ok := w.ix.Chunk(c)
	var entries []disklog.Entry
	if w.epoch > ep {
		switch {
		case ok:
			entries = append(entries, disklog.FreezeEntry(pool, c, ver, w.epoch))
			w.freezes++
		case w.absentFreeze:
			entries = append(entries, disklog.FreezeEntry(pool, c, 0, w.epoch))
			w.absents++
		}
	}
	w.version++
	if punch {
		entries = append(entries, disklog.PunchEntry(pool, c, w.version, first, count))
	} else {
		entries = append(entries, disklog.WriteEntry(pool, c, w.version, first, count))
	}
	ev := record(w.lba, entries...)
	w.records = append(w.records, ev)
	w.lba += core.LBA(1 + int(count))
	if err := w.ix.Apply(ev); err != nil {
		return err
	}
	return w.ix.Check()
}

// model holds every block of every chunk as the live version and as each snapshot holds it.
type model struct {
	live  map[core.ChunkID]map[uint16]block
	snaps []map[core.ChunkID]map[uint16]block // snaps[s-1] is snapshot s
}

func newModel() *model { return &model{live: map[core.ChunkID]map[uint16]block{}} }

func (m *model) write(c core.ChunkID, first, count uint16, version uint64, lba core.LBA, punch bool) {
	if m.live[c] == nil {
		m.live[c] = map[uint16]block{}
	}
	for b := first; b < first+count; b++ {
		if punch {
			m.live[c][b] = block{version, 0}
		} else {
			m.live[c][b] = block{version, lba + 1 + core.LBA(b-first)}
		}
	}
}

func (m *model) cut() {
	s := map[core.ChunkID]map[uint16]block{}
	for c, bs := range m.live {
		s[c] = maps.Clone(bs)
	}
	m.snaps = append(m.snaps, s)
}

// mismatches counts every block of chunks 0..chunks-1, blocks 0..blocks-1, that a read at a
// snapshot epoch finds differently from the model, and how many of those are in a chunk the
// snapshot does not hold, and returns the first one.
func (m *model) mismatches(ix index, chunks int, blocks uint16) (int, int, string) {
	n, notThere, first := 0, 0, ""
	for s := 1; s <= len(m.snaps); s++ {
		for ci := range chunks {
			c := chunk(ci)
			for b := range blocks {
				want, wok := m.snaps[s-1][c][b]
				got, gok := ix.LookupAt(c, b, uint64(s))
				if wok != gok || (wok && got != want) {
					if m.snaps[s-1][c] == nil {
						notThere++
					}
					if n == 0 {
						first = fmt.Sprintf("snapshot %d chunk %d block %d: got=%v %+v want=%v %+v", s, ci, b, gok, got, wok, want)
					}
					n++
				}
			}
		}
	}
	return n, notThere, first
}

func report(t *testing.T, ok bool, name, detail string) {
	t.Helper()
	tag := "[OK ]"
	if !ok {
		tag = "[STALE]"
	}
	t.Logf("%s %s %s", tag, name, detail)
}

// E1: a chunk first written after cut 1 is written again before cut 2. The second write sees the
// chunk entry's epoch 0 and freezes version 1, which snapshot 1 never held.
// E2: a chunk first written after cut 1, then cut 2 and a write: the freeze at epoch 2 of a
// version written after cut 1 is what snapshot 1 reads.
func TestE1E2FirstWriteAfterCut(t *testing.T) {
	for _, impl := range []struct {
		name   string
		mk     func() index
		absent bool
	}{
		{"current", func() index { return newCurrent(nil) }, false},
		{"absent-freeze", func() index { return newProto(nil) }, true},
	} {
		w := &writer{ix: impl.mk(), absentFreeze: impl.absent, lba: 100}
		w.epoch = 1 // cut 1: the chunk does not exist yet
		must(t, w.write(chunk(0), 0, 1, false))
		must(t, w.write(chunk(0), 1, 1, false))
		got, ok := w.ix.LookupAt(chunk(0), 0, 1)
		report(t, !ok, "E1 "+impl.name,
			fmt.Sprintf("snapshot 1 block 0: found=%v %+v want=absent (freezes %d, absent freezes %d)", ok, got, w.freezes, w.absents))

		w = &writer{ix: impl.mk(), absentFreeze: impl.absent, lba: 100}
		w.epoch = 1
		must(t, w.write(chunk(0), 0, 1, false))
		w.epoch = 2
		must(t, w.write(chunk(0), 0, 1, false))
		got, ok = w.ix.LookupAt(chunk(0), 0, 1)
		got2, ok2 := w.ix.LookupAt(chunk(0), 0, 2)
		report(t, !ok, "E2 "+impl.name,
			fmt.Sprintf("snapshot 1 block 0: found=%v %+v want=absent; snapshot 2: found=%v %+v want=v1", ok, got, ok2, got2))
		if !ok2 || got2.Version != 1 {
			t.Fatalf("%s: snapshot 2 lost version 1", impl.name)
		}
	}
}

// E3 [property]: random cuts, writes and punches over 4 chunks; after each step every block of
// every snapshot must read as the model holds it. A rebuild from the records, and the records
// applied twice, must give the same index.
func TestE3SnapshotModel(t *testing.T) {
	const chunks, blocks, steps, seeds = 4, 16, 400, 50
	for _, impl := range []struct {
		name   string
		mk     func() index
		absent bool
	}{
		{"current", func() index { return newCurrent(nil) }, false},
		{"absent-freeze", func() index { return newProto(nil) }, true},
	} {
		badSeeds, badReads, notThere, freezes, absents := 0, 0, 0, 0, 0
		var first string
		for seed := range seeds {
			r := rand.New(rand.NewPCG(uint64(seed), 10))
			w := &writer{ix: impl.mk(), absentFreeze: impl.absent, lba: 100}
			m := newModel()
			for range steps {
				if r.IntN(10) == 0 {
					w.epoch++
					m.cut()
					continue
				}
				c := chunk(r.IntN(chunks))
				first := uint16(r.IntN(blocks))
				count := uint16(1 + r.IntN(min(4, blocks-int(first))))
				punch := r.IntN(6) == 0
				must(t, w.write(c, first, count, punch))
				m.write(c, first, count, w.version, w.lba-core.LBA(1+int(count)), punch)
			}
			n, nt, f := m.mismatches(w.ix, chunks, blocks)
			if n > 0 {
				badSeeds++
				badReads += n
				notThere += nt
				if first == "" {
					first = fmt.Sprintf("seed %d %s", seed, f)
				}
			}
			freezes += w.freezes
			absents += w.absents
			rebuilt := impl.mk()
			for _, ev := range w.records {
				must(t, rebuilt.Apply(ev))
			}
			if !slices.Equal(dump(rebuilt.Tree()), dump(w.ix.Tree())) {
				t.Fatalf("%s seed %d: rebuild differs", impl.name, seed)
			}
			for _, ev := range w.records {
				must(t, rebuilt.Apply(ev))
			}
			must(t, rebuilt.Check())
			if n2, _, _ := m.mismatches(rebuilt, chunks, blocks); n2 != n {
				t.Fatalf("%s seed %d: records applied twice read %d mismatches, once %d", impl.name, seed, n2, n)
			}
		}
		report(t, badSeeds == 0, "E3 "+impl.name, fmt.Sprintf("%d of %d seeds read a snapshot wrong, %d block reads, %d of them in a chunk created after the cut; %d freezes, %d absent freezes; first: %s",
			badSeeds, seeds, badReads, notThere, freezes, absents, first))
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
