package chunkepochs

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// unit is one write or punch of a record, with the freeze that precedes it if any: what a
// replicated primary maps into its index when the write commits.
type unit struct {
	lsn     int
	chunk   core.ChunkID
	entries []disklog.Entry
	offsets []uint64
}

// replicated is a primary whose writes commit some rounds after their record is durable, and
// which maps a write only when it commits, as docs/design.md "Reads see only acknowledged
// writes" requires. ordered keeps a chunk's commits in version order.
type replicated struct {
	r        *rand.Rand
	ix       index
	ordered  bool
	epoch    uint64
	version  uint64
	lba      core.LBA
	moveLBA  core.LBA
	records  []*disklog.Event
	pending  []unit
	exists   map[core.ChunkID]uint64 // chunk -> newest version submitted
	epochOf  map[core.ChunkID]uint64 // chunk -> snapshot epoch of its newest write submitted
	origin   map[core.LBA]core.LBA   // a cleaner copy's location -> the block it copied
	ckTree   [][2]string
	ckFrom   int
	ckHeld   int // records at or after ckFrom that the checkpoint's runs already held
	commits  int
	moves    int
	ckpoints int
}

func newReplicated(r *rand.Rand, ordered bool) *replicated {
	return &replicated{r: r, ix: newCurrent(nil), ordered: ordered, lba: 100, moveLBA: 1 << 40,
		exists: map[core.ChunkID]uint64{}, epochOf: map[core.ChunkID]uint64{}, origin: map[core.LBA]core.LBA{}}
}

func (p *replicated) submit(chunks, blocks int) {
	lsn := len(p.records)
	var entries []disklog.Entry
	var units []unit
	for range 1 + p.r.IntN(3) {
		c := chunk(p.r.IntN(chunks))
		u := unit{lsn: lsn, chunk: c}
		start := len(entries)
		if v, ok := p.exists[c]; ok && p.epoch > p.epochOf[c] {
			entries = append(entries, disklog.FreezeEntry(pool, c, v, p.epoch))
		}
		p.version++
		first := uint16(p.r.IntN(blocks))
		count := uint16(1 + p.r.IntN(min(4, blocks-int(first))))
		switch p.r.IntN(8) {
		case 0:
			entries = append(entries, disklog.PunchEntry(pool, c, p.version, 0, core.BlocksPerChunk))
		case 1:
			entries = append(entries, disklog.PunchEntry(pool, c, p.version, first, count))
		default:
			entries = append(entries, disklog.WriteEntry(pool, c, p.version, first, count))
		}
		p.exists[c], p.epochOf[c] = p.version, p.epoch
		for i := start; i < len(entries); i++ {
			u.entries = append(u.entries, entries[i])
			u.offsets = append(u.offsets, uint64(i))
		}
		units = append(units, u)
	}
	ev := record(p.lba, entries...)
	for i := range units {
		for j, idx := range units[i].offsets {
			units[i].offsets[j] = ev.Payloads[idx]
		}
		units[i].entries = append([]disklog.Entry(nil), units[i].entries...)
	}
	data := uint64(0)
	for _, e := range entries {
		if e.Kind == disklog.EntryWrite {
			data += uint64(e.Count)
		}
	}
	p.records = append(p.records, ev)
	p.lba += core.LBA(1 + data)
	p.pending = append(p.pending, units...)
}

// commit maps one pending write: any of them, or with ordered the oldest pending write of a
// chunk.
func (p *replicated) commit(t *testing.T) {
	var eligible []int
	seen := map[core.ChunkID]bool{}
	for i, u := range p.pending {
		if !p.ordered || !seen[u.chunk] {
			eligible = append(eligible, i)
		}
		seen[u.chunk] = true
	}
	if len(eligible) == 0 {
		return
	}
	i := eligible[p.r.IntN(len(eligible))]
	u := p.pending[i]
	p.pending = append(p.pending[:i], p.pending[i+1:]...)
	must(t, p.ix.Apply(event(p.records[u.lsn].LBA, u.entries, u.offsets)))
	p.commits++
}

// move copies one block a live or frozen map holds to a new location, as a cleaner does.
func (p *replicated) move(t *testing.T, chunks, blocks int) {
	c := chunk(p.r.IntN(chunks))
	b := uint16(p.r.IntN(blocks))
	var bl block
	var ok bool
	if p.epoch > 0 && p.r.IntN(2) == 0 {
		bl, ok = p.ix.LookupAt(c, b, 1+p.r.Uint64N(p.epoch))
	} else {
		bl, ok = p.ix.Lookup(c, b)
	}
	if !ok || bl.LBA == 0 {
		return
	}
	header := p.moveLBA
	to := header + 1
	p.moveLBA += 2
	if p.ix.Move(c, b, bl.LBA, to, header) > 0 {
		p.origin[to] = p.originOf(bl.LBA)
		p.moves++
	}
	must(t, p.ix.Check())
}

func (p *replicated) originOf(l core.LBA) core.LBA {
	if o, ok := p.origin[l]; ok {
		return o
	}
	return l
}

// checkpoint takes the index as a flush would write it, with the replay point at the oldest
// pending write's record, or past the last record.
func (p *replicated) checkpoint() {
	p.ckTree = dump(p.ix.Tree())
	p.ckFrom = len(p.records)
	for _, u := range p.pending {
		p.ckFrom = min(p.ckFrom, u.lsn)
	}
	committed := map[int]bool{}
	for _, u := range p.pending {
		committed[u.lsn] = false
	}
	p.ckHeld = 0
	for lsn := p.ckFrom; lsn < len(p.records); lsn++ {
		if _, isPending := committed[lsn]; !isPending {
			p.ckHeld++
		}
	}
	p.ckpoints++
}

// restart loads the checkpoint's runs and replays every record from its replay point in LSN
// order, mapping each write as a restarted primary does before it peers.
func (p *replicated) restart(t *testing.T) index {
	tree := newTree()
	for _, kv := range p.ckTree {
		must(t, tree.Put([]byte(kv[0]), []byte(kv[1])))
	}
	ix := newCurrent(tree)
	for _, ev := range p.records[p.ckFrom:] {
		must(t, ix.Apply(ev))
	}
	must(t, ix.Check())
	return ix
}

// differ compares two indexes block by block, live and at every snapshot epoch, by version and
// by the block a location holds a copy of.
func differ(a, b index, originA func(core.LBA) core.LBA, chunks, blocks int, epochs uint64) string {
	for ci := range chunks {
		c := chunk(ci)
		for bl := range uint16(blocks) {
			for s := uint64(0); s <= epochs; s++ {
				var x, y block
				var xo, yo bool
				if s == 0 {
					x, xo = a.Lookup(c, bl)
					y, yo = b.Lookup(c, bl)
				} else {
					x, xo = a.LookupAt(c, bl, s)
					y, yo = b.LookupAt(c, bl, s)
				}
				x.LBA = originA(x.LBA)
				if xo != yo || x != y {
					return fmt.Sprintf("chunk %d block %d epoch %d (0 = live): %v %+v, rebuild %v %+v", ci, bl, s, xo, x, yo, y)
				}
			}
		}
	}
	return ""
}

// R1, R2 [property]: a replicated primary maps writes at commit, checkpoints with its replay point
// at its oldest pending write, moves blocks as a cleaner, and crashes. Its restart (the
// checkpoint's index plus every record from the replay point, in LSN order, applied again where
// the runs already hold it) must read every block, live and at every snapshot, as a rebuild
// that applies each record once does. R1 commits a chunk's writes in version order; R2 does not.
// Both also compare the index that never crashed, once its pending writes commit, with the rebuild.
func TestR1R2ReplayPastPending(t *testing.T) {
	const chunks, blocks, steps, seeds = 4, 16, 300, 200
	for _, mode := range []struct {
		name    string
		ordered bool
	}{{"R1 per-chunk commit order", true}, {"R2 any commit order", false}} {
		bad, liveBad, held, replayed, commits, moves, ckpoints := 0, 0, 0, 0, 0, 0, 0
		var first string
		for seed := range seeds {
			p := newReplicated(rand.New(rand.NewPCG(uint64(seed), 11)), mode.ordered)
			for step := range steps {
				if p.r.IntN(12) == 0 {
					p.epoch++
				}
				p.submit(chunks, blocks)
				for range p.r.IntN(4) {
					p.commit(t)
				}
				if p.r.IntN(3) == 0 {
					p.move(t, chunks, blocks)
				}
				if p.r.IntN(20) == 0 || step == 0 {
					p.checkpoint()
				}
			}
			restarted := p.restart(t)
			rebuilt := newCurrent(nil)
			for _, ev := range p.records {
				must(t, rebuilt.Apply(ev))
			}
			for len(p.pending) > 0 {
				p.commit(t)
			}
			if differ(p.ix, rebuilt, p.originOf, chunks, blocks, p.epoch) != "" {
				liveBad++
			}
			if d := differ(restarted, rebuilt, p.originOf, chunks, blocks, p.epoch); d != "" {
				bad++
				if first == "" {
					first = fmt.Sprintf("seed %d: %s", seed, d)
				}
			}
			held += p.ckHeld
			replayed += len(p.records) - p.ckFrom
			commits += p.commits
			moves += p.moves
			ckpoints += p.ckpoints
		}
		report(t, bad == 0, mode.name, fmt.Sprintf("%d of %d seeds differ from a rebuild (the index that never crashed, its pending writes committed: %d); replay applied %d records of which the runs already held %d; %d commits, %d cleaner moves, %d checkpoints; first: %s",
			bad, seeds, liveBad, replayed, held, commits, moves, ckpoints, first))
	}
}

// R3: a write the primary logged but peering dropped, followed by a committed write to other
// blocks of the chunk. A checkpoint taken while the dropped write was pending replays it.
// R3b: the primary logs the recovery copy of the authoritative blocks under a new version, as
// docs/design.md "Epoch changes" recovers a dropped version, so the replay converges.
func TestR3DroppedWrite(t *testing.T) {
	for _, recoveryRecord := range []bool{false, true} {
		live := newCurrent(nil)
		c := chunk(0)
		var records []*disklog.Event
		rec := func(lba core.LBA, apply bool, entries ...disklog.Entry) {
			ev := record(lba, entries...)
			records = append(records, ev)
			if apply {
				must(t, live.Apply(ev))
			}
		}
		rec(100, true, disklog.WriteEntry(pool, c, 1, 0, 4))
		rec(200, false, disklog.WriteEntry(pool, c, 2, 0, 2))
		ckTree, ckFrom := dump(live.Tree()), 1
		origin := map[core.LBA]core.LBA{}
		if recoveryRecord {
			rec(300, true, disklog.WriteEntry(pool, c, 3, 0, 2))
			origin[301], origin[302] = 101, 102
		}
		rec(400, true, disklog.WriteEntry(pool, c, 4, 2, 2))
		tree := newTree()
		for _, kv := range ckTree {
			must(t, tree.Put([]byte(kv[0]), []byte(kv[1])))
		}
		restarted := newCurrent(tree)
		for _, ev := range records[ckFrom:] {
			must(t, restarted.Apply(ev))
		}
		of := func(l core.LBA) core.LBA {
			if o, ok := origin[l]; ok {
				return o
			}
			return l
		}
		got, _ := restarted.Lookup(c, 0)
		want, _ := live.Lookup(c, 0)
		name := "R3 dropped write, no recovery record"
		if recoveryRecord {
			name = "R3b dropped write superseded by a recovery record"
		}
		report(t, of(got.LBA) == of(want.LBA), name, fmt.Sprintf("block 0 after restart: v%d at LBA %d (copy of %d); live index: v%d at LBA %d (copy of %d)",
			got.Version, got.LBA, of(got.LBA), want.Version, want.LBA, of(want.LBA)))
	}
}
