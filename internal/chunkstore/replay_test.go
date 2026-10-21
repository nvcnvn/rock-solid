package chunkstore

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/lsm"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

const replayMaxPiece = 4096

// runStore keeps the run pieces checkpoints write, where the disk log would.
type runStore struct {
	blobs map[disklog.BlobRef][]byte
	lsn   uint64
}

// checkpoint flushes tree a piece at a time and returns the roots of the runs a manifest would name.
func (s *runStore) checkpoint(t testing.TB, tree *lsm.Tree) []disklog.BlobRef {
	t.Helper()
	if _, err := tree.BeginFlush(); err != nil {
		t.Fatal(err)
	}
	roots := slices.Clone(tree.CarriedRuns())
	buf := make([]byte, replayMaxPiece)
	for {
		n, root, err := tree.NextPiece(buf)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
		s.lsn++
		ref := disklog.BlobRef{Segment: 1, Block: uint32(s.lsn), LSN: s.lsn}
		s.blobs[ref] = slices.Clone(buf[:n])
		if err := tree.PieceDurable(ref); err != nil {
			t.Fatal(err)
		}
		if root {
			roots = append(roots, ref)
		}
	}
	if _, err := tree.CheckpointDone(); err != nil {
		t.Fatal(err)
	}
	return roots
}

func (s *runStore) restore(t *testing.T, roots []disklog.BlobRef) *Index {
	t.Helper()
	tree, err := lsm.New(TreeConfig(lsm.Config{MaxPiece: replayMaxPiece, Check: true}))
	if err != nil {
		t.Fatal(err)
	}
	for _, root := range roots {
		refs, err := lsm.RootRefs(s.blobs[root], nil)
		if err != nil {
			t.Fatal(err)
		}
		var pieces [][]byte
		for _, r := range append(refs, root) {
			pieces = append(pieces, s.blobs[r])
		}
		if err := tree.LoadRun(root, pieces); err != nil {
			t.Fatal(err)
		}
	}
	return NewIndex(tree)
}

// pendingWrite is one write or punch of a record, with the freeze before it if any: what a
// replicated primary maps when the write commits.
type pendingWrite struct {
	lsn      int
	chunk    core.ChunkID
	entries  []disklog.Entry
	payloads []uint64
}

// replica is a primary whose writes commit some rounds after their record is durable, mapping each
// at commit and a chunk's writes in version order.
type replica struct {
	r       *rand.Rand
	ix      *Index
	store   runStore
	epoch   uint64
	version uint64
	lba     core.LBA
	moveLBA core.LBA
	records []*disklog.Event
	pending []pendingWrite
	newest  map[core.ChunkID]uint64
	// newestEpoch is the snapshot epoch of the newest write submitted to a chunk.
	newestEpoch map[core.ChunkID]uint64
	// origin maps a cleaner copy's location to the block it is a copy of.
	origin map[core.LBA]core.LBA

	ckRuns []disklog.BlobRef
	ckFrom int
	ckHeld int

	commits, moves, checkpoints, compactions int
}

func newReplica(t *testing.T, seed uint64) *replica {
	tree, err := lsm.New(TreeConfig(lsm.Config{MaxPiece: replayMaxPiece, Check: true}))
	if err != nil {
		t.Fatal(err)
	}
	return &replica{
		r: rand.New(rand.NewPCG(seed, 11)), ix: NewIndex(tree), store: runStore{blobs: map[disklog.BlobRef][]byte{}},
		lba: 100, moveLBA: 1 << 40,
		newest: map[core.ChunkID]uint64{}, newestEpoch: map[core.ChunkID]uint64{}, origin: map[core.LBA]core.LBA{},
	}
}

func (p *replica) submit(chunks, blocks int) {
	lsn := len(p.records)
	var entries []disklog.Entry
	var writes []pendingWrite
	var at [][]int
	for range 1 + p.r.IntN(3) {
		c := chunk(uint32(p.r.IntN(chunks)))
		start := len(entries)
		if v, ok := p.newest[c]; ok && p.epoch > p.newestEpoch[c] {
			entries = append(entries, disklog.FreezeEntry(1, c, v, p.epoch))
		}
		p.version++
		first := uint16(p.r.IntN(blocks))
		count := uint16(1 + p.r.IntN(min(4, blocks-int(first))))
		switch p.r.IntN(8) {
		case 0:
			entries = append(entries, disklog.PunchEntry(1, c, p.version, 0, core.BlocksPerChunk))
		case 1:
			entries = append(entries, disklog.PunchEntry(1, c, p.version, first, count))
		default:
			entries = append(entries, disklog.WriteEntry(1, c, p.version, first, count))
		}
		p.newest[c], p.newestEpoch[c] = p.version, p.epoch
		writes = append(writes, pendingWrite{lsn: lsn, chunk: c})
		var idx []int
		for i := start; i < len(entries); i++ {
			idx = append(idx, i)
		}
		at = append(at, idx)
	}
	ev := record(p.lba, entries...)
	data := uint64(0)
	for i, w := range writes {
		for _, j := range at[i] {
			w.entries = append(w.entries, entries[j])
			w.payloads = append(w.payloads, ev.Payloads[j])
		}
		writes[i] = w
	}
	for _, e := range entries {
		if e.Kind == disklog.EntryWrite {
			data += uint64(e.Count)
		}
	}
	p.records = append(p.records, ev)
	p.lba += core.LBA(1 + data)
	p.pending = append(p.pending, writes...)
}

// commit maps the oldest pending write of a chunk chosen at random.
func (p *replica) commit(t *testing.T) {
	var eligible []int
	seen := map[core.ChunkID]bool{}
	for i, w := range p.pending {
		if !seen[w.chunk] {
			eligible = append(eligible, i)
		}
		seen[w.chunk] = true
	}
	if len(eligible) == 0 {
		return
	}
	i := eligible[p.r.IntN(len(eligible))]
	w := p.pending[i]
	p.pending = slices.Delete(p.pending, i, i+1)
	ev := &disklog.Event{Kind: disklog.Durable, LBA: p.records[w.lsn].LBA, HeaderBlocks: 1, Entries: w.entries, Payloads: w.payloads}
	if err := p.ix.Apply(ev); err != nil {
		t.Fatal(err)
	}
	p.commits++
}

// move copies one block a live or frozen map holds to a new location, as the cleaner does.
func (p *replica) move(t *testing.T, chunks, blocks int) {
	c := chunk(uint32(p.r.IntN(chunks)))
	b := uint16(p.r.IntN(blocks))
	var e Extent
	var ok bool
	if p.epoch > 0 && p.r.IntN(2) == 0 {
		e, ok = p.ix.LookupAt(1, c, b, 1+p.r.Uint64N(p.epoch))
	} else {
		e, ok = p.ix.Lookup(1, c, b)
	}
	if !ok || e.LBA == 0 {
		return
	}
	from := e.BlockLBA(b)
	header := p.moveLBA
	to := header + 1
	p.moveLBA += 2
	if p.ix.Move(1, c, b, 1, from, to, header) > 0 {
		p.origin[to] = p.originOf(from)
		p.moves++
	}
	if err := p.ix.Check(); err != nil {
		t.Fatal(err)
	}
}

func (p *replica) originOf(l core.LBA) core.LBA {
	if o, ok := p.origin[l]; ok {
		return o
	}
	return l
}

// checkpoint flushes the index's tree, with the replay point at the oldest pending write's record
// or past the last record, then compacts its runs when a compaction is due.
func (p *replica) checkpoint(t *testing.T) {
	tree := p.ix.Tree()
	p.ckRuns = p.store.checkpoint(t, tree)
	p.ckFrom = len(p.records)
	pendingLSN := map[int]bool{}
	for _, w := range p.pending {
		p.ckFrom = min(p.ckFrom, w.lsn)
		pendingLSN[w.lsn] = true
	}
	p.ckHeld = 0
	for lsn := p.ckFrom; lsn < len(p.records); lsn++ {
		if !pendingLSN[lsn] {
			p.ckHeld++
		}
	}
	p.checkpoints++
	if c := tree.PickCompaction(); c != nil {
		c.Run()
		if err := tree.Install(c); err != nil {
			t.Fatal(err)
		}
		p.compactions++
	}
}

// restart loads the checkpoint's runs and replays every record from its replay point in LSN order.
func (p *replica) restart(t *testing.T) *Index {
	ix := p.store.restore(t, p.ckRuns)
	for _, ev := range p.records[p.ckFrom:] {
		if err := ix.Apply(ev); err != nil {
			t.Fatal(err)
		}
	}
	if err := ix.Check(); err != nil {
		t.Fatal(err)
	}
	return ix
}

// differ compares two indexes block by block, live and at every snapshot epoch up to epochs, by
// version and by the block a location of a holds a copy of.
func differ(a, b *Index, originA func(core.LBA) core.LBA, chunks, blocks int, epochs uint64) string {
	type block struct {
		version uint64
		lba     core.LBA
	}
	mapAt := func(x *Index, c core.ChunkID, epoch uint64) uint64 {
		if v, ok := x.FrozenAt(1, c, epoch); ok && epoch > 0 {
			return v
		}
		return 0
	}
	for ci := range uint32(chunks) {
		c := chunk(ci)
		for s := uint64(0); s <= epochs; s++ {
			va, vb := mapAt(a, c, s), mapAt(b, c, s)
			for bl := range uint16(blocks) {
				x, xo := a.lookup(1, c, va, bl)
				y, yo := b.lookup(1, c, vb, bl)
				xb := block{x.Version, originA(x.BlockLBA(bl))}
				yb := block{y.Version, y.BlockLBA(bl)}
				if xo != yo || xb != yb {
					return fmt.Sprintf("chunk %d block %d epoch %d (0 = live): %v %+v, rebuild %v %+v", ci, bl, s, xo, xb, yo, yb)
				}
			}
		}
	}
	return ""
}

// [property] A primary that maps writes at commit, a chunk's in version order, moves blocks as the
// cleaner and checkpoints with its replay point at its oldest pending write restarts from the
// checkpoint's runs plus every record from the replay point, and reads every block, live and at
// every snapshot epoch, as a rebuild does; so does the index that never restarted once its pending
// writes commit.
func TestReplayPastPendingWrites(t *testing.T) {
	const chunks, blocks, rounds = 4, 16, 300
	n := 200
	if testing.Short() {
		n = 20
	}
	var replayed, held, commits, moves, checkpoints, compactions int
	seeds.Run(t, 0, n, func(seed uint64) {
		p := newReplica(t, seed)
		for round := range rounds {
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
			if p.r.IntN(20) == 0 || round == 0 {
				p.checkpoint(t)
			}
		}
		restarted := p.restart(t)
		rebuilt := newIndex(t)
		for _, ev := range p.records {
			if err := rebuilt.Apply(ev); err != nil {
				t.Fatal(err)
			}
		}
		if err := rebuilt.Check(); err != nil {
			t.Fatal(err)
		}
		for len(p.pending) > 0 {
			p.commit(t)
		}
		if d := differ(restarted, rebuilt, p.originOf, chunks, blocks, p.epoch); d != "" {
			t.Fatalf("seed %d: restarted index %s", seed, d)
		}
		if d := differ(p.ix, rebuilt, p.originOf, chunks, blocks, p.epoch); d != "" {
			t.Fatalf("seed %d: live index %s", seed, d)
		}
		replayed += len(p.records) - p.ckFrom
		held += p.ckHeld
		commits += p.commits
		moves += p.moves
		checkpoints += p.checkpoints
		compactions += p.compactions
	})
	if !seeds.Default() {
		return
	}
	t.Logf("%d seeds: replay applied %d records, %d of them already in the runs; %d commits, %d cleaner moves, %d checkpoints, %d compactions",
		n, replayed, held, commits, moves, checkpoints, compactions)
	if held == 0 || moves == 0 || compactions == 0 {
		t.Fatal("the workload missed a case")
	}
}
