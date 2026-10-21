package chunkstore

import (
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/lsm"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

const (
	indexOwner = 1
	diskBytes  = 1 << 30
)

// node runs a chunk index over a disk log on a simulated disk, executing each action at once. It
// is the owner a ChunkStore will be: it maps durable records, checkpoints the index's tree a piece
// at a time and restores it from the manifest's runs plus replay.
type node struct {
	t    *testing.T
	pool *sim.Pool
	ctx  core.Context
	disk *sim.Disk
	cfg  disklog.Config
	log  *disklog.Log
	rec  *disklog.Recovery
	out  disklog.Output
	ix   *Index
	// maxBlob is the log's MaxBlobBytes, which a restore needs before the log resumes.
	maxBlob int

	checkpointing bool
	piece         disklog.Ticket
	inPiece       bool
	replayLSN     uint64
	checkpoints   int
	carried       int
	pieces        int
	crashWhen     func(*disklog.Event) bool
	crashed       bool

	mode      disklog.Mode
	loading   map[disklog.BlobRef]*runLoad
	pieceOf   map[disklog.BlobRef]disklog.BlobRef
	replayed  int
	recovered bool
}

type runLoad struct {
	refs []disklog.BlobRef
	got  map[disklog.BlobRef][]byte
	root []byte
}

func testLogConfig() disklog.Config {
	cfg := disklog.DefaultConfig()
	cfg.MaxRecordBytes = 16 * core.BlockSize
	cfg.MaxReplayBytes = 96 * core.BlockSize
	cfg.Check = true
	cfg.NoReserve = true
	return cfg
}

func newNode(t *testing.T) *node {
	n := &node{t: t, pool: sim.NewPool(64<<20, 0), disk: sim.NewDisk(diskBytes / core.BlockSize), cfg: testLogConfig()}
	n.ctx = core.Context{Now: 1, Raw: 2, Pool: n.pool}
	geo, err := disklog.GeometryFor(diskBytes)
	if err != nil {
		t.Fatal(err)
	}
	n.log, err = disklog.Format(&n.ctx, n.cfg, disklog.Superblock{LogID: 0x5eed, Geometry: geo}, 1000, &n.out)
	if err != nil {
		t.Fatal(err)
	}
	n.maxBlob = int(n.log.MaxBlobBytes())
	n.ix = n.newIndex()
	n.settle()
	return n
}

func (n *node) newIndex() *Index {
	tree, err := lsm.New(TreeConfig(lsm.Config{MaxPiece: n.maxBlob, Check: true}))
	if err != nil {
		n.t.Fatal(err)
	}
	return NewIndex(tree)
}

func copyEvent(ev disklog.Event) disklog.Event {
	ev.Entries, ev.Tickets, ev.Payloads, ev.Data = slices.Clone(ev.Entries), slices.Clone(ev.Tickets), slices.Clone(ev.Payloads), slices.Clone(ev.Data)
	return ev
}

// settle executes actions and handles events until nothing is left, or the node crashes.
func (n *node) settle() {
	for !n.crashed {
		evs := make([]disklog.Event, len(n.out.Events))
		for i, ev := range n.out.Events {
			evs[i] = copyEvent(ev)
		}
		acts := slices.Clone(n.out.Actions)
		n.out.Reset()
		if len(evs) == 0 && len(acts) == 0 {
			return
		}
		for i := range evs {
			if n.handle(&evs[i]); n.crashed {
				return
			}
		}
		for _, a := range acts {
			done := n.disk.Execute(n.pool, &a)
			if n.rec != nil {
				n.rec.OnEvent(&n.ctx, &done, &n.out)
			} else {
				n.log.OnEvent(&n.ctx, &done, &n.out)
			}
		}
	}
}

func (n *node) crash() {
	n.crashed = true
	n.disk.Crash(func(int, core.LBA) bool { return false })
}

func (n *node) handle(ev *disklog.Event) {
	if n.crashWhen != nil && n.crashWhen(ev) {
		n.crash()
		return
	}
	tree := n.ix.Tree()
	switch ev.Kind {
	case disklog.Durable:
		if err := n.ix.Apply(ev); err != nil {
			n.t.Fatal(err)
		}
		for i, tk := range ev.Tickets {
			if n.inPiece && tk == n.piece {
				n.inPiece = false
				ref := disklog.BlobRef{Segment: ev.Segment, Block: ev.Block, LSN: ev.LSN, Entry: uint16(i)}
				if err := tree.PieceDurable(ref); err != nil {
					n.t.Fatal(err)
				}
				n.nextPiece()
			}
		}
	case disklog.CheckpointDone:
		if _, err := tree.CheckpointDone(); err != nil {
			n.t.Fatal(err)
		}
		n.checkpointing = false
		n.checkpoints++
	case disklog.CheckpointFailed, disklog.Failed, disklog.InDoubt, disklog.RecoveryFailed:
		n.t.Fatalf("log event %+v", *ev)
	case disklog.ManifestLoaded:
		n.loadRuns()
	case disklog.BlobRead:
		n.blobRead(ev)
	case disklog.Replayed:
		if ev.LSN > n.replayLSN {
			if err := n.ix.Apply(ev); err != nil {
				n.t.Fatal(err)
			}
			n.replayed++
		}
	case disklog.Recovered:
		log, err := n.rec.Log(&n.ctx)
		if err != nil {
			n.t.Fatal(err)
		}
		n.log, n.rec, n.recovered = log, nil, true
	}
}

func (n *node) checkpoint() {
	tree := n.ix.Tree()
	froze, err := tree.BeginFlush()
	if err != nil {
		n.t.Fatal(err)
	}
	if froze || !tree.Frozen() {
		n.replayLSN = n.log.DurableLSN()
	}
	idx := []disklog.CheckpointIndex{{Owner: indexOwner, ReplayLSN: n.replayLSN, Runs: tree.CarriedRuns()}}
	n.carried += len(tree.CarriedRuns())
	if err := n.log.BeginCheckpoint(idx); err != nil {
		n.t.Fatal(err)
	}
	n.checkpointing = true
	n.nextPiece()
}

func (n *node) nextPiece() {
	buf, err := n.pool.Alloc(n.maxBlob)
	if err != nil {
		n.t.Fatal(err)
	}
	length, root, err := n.ix.Tree().NextPiece(n.pool.Bytes(buf))
	if err != nil {
		n.t.Fatal(err)
	}
	if length == 0 {
		n.pool.Free(buf)
		if err := n.log.EndCheckpoint(&n.ctx, &n.out); err != nil {
			n.t.Fatal(err)
		}
		return
	}
	if n.piece, err = n.log.CheckpointPiece(&n.ctx, indexOwner, buf, uint64(length), root, &n.out); err != nil {
		n.t.Fatal(err)
	}
	n.inPiece = true
	n.pieces++
}

// recover restarts the node on its disk: recovery, the index's runs, replay, then group commit.
func (n *node) recover(mode disklog.Mode) {
	n.log, n.out, n.crashed, n.crashWhen = nil, disklog.Output{}, false, nil
	n.checkpointing, n.inPiece, n.recovered, n.replayed, n.replayLSN = false, false, false, 0, 0
	n.ix = n.newIndex()
	n.mode = mode
	n.loading, n.pieceOf = map[disklog.BlobRef]*runLoad{}, map[disklog.BlobRef]disklog.BlobRef{}
	var err error
	if n.rec, err = disklog.Recover(&n.ctx, n.cfg, n.disk.Blocks(), mode, 5000, &n.out); err != nil {
		n.t.Fatal(err)
	}
	n.settle()
	if !n.recovered {
		n.t.Fatalf("recovery stopped: %v", n.rec.Err())
	}
}

func (n *node) loadRuns() {
	if m, ok := n.rec.Manifest(); ok && n.mode == disklog.ModeOpen {
		for _, ix := range m.Indexes {
			if ix.Owner != indexOwner {
				continue
			}
			n.replayLSN = ix.ReplayLSN
			for _, root := range ix.Runs {
				n.loading[root] = &runLoad{got: map[disklog.BlobRef][]byte{}}
				if err := n.rec.ReadBlob(&n.ctx, root, &n.out); err != nil {
					n.t.Fatal(err)
				}
			}
		}
	}
	n.replayIfLoaded()
}

func (n *node) blobRead(ev *disklog.Event) {
	root := ev.Ref
	l, isRoot := n.loading[root]
	if isRoot && l.root == nil {
		l.root = ev.Data
		refs, err := lsm.RootRefs(ev.Data, nil)
		if err != nil {
			n.t.Fatal(err)
		}
		l.refs = refs
		for _, r := range refs {
			n.pieceOf[r] = root
			if err := n.rec.ReadBlob(&n.ctx, r, &n.out); err != nil {
				n.t.Fatal(err)
			}
		}
	} else {
		root = n.pieceOf[ev.Ref]
		l = n.loading[root]
		l.got[ev.Ref] = ev.Data
	}
	if len(l.got) == len(l.refs) {
		var pieces [][]byte
		for _, r := range l.refs {
			pieces = append(pieces, l.got[r])
		}
		if err := n.ix.Tree().LoadRun(root, append(pieces, l.root)); err != nil {
			n.t.Fatal(err)
		}
		delete(n.loading, root)
		n.replayIfLoaded()
	}
}

func (n *node) replayIfLoaded() {
	if len(n.loading) > 0 {
		return
	}
	if err := n.rec.Replay(&n.ctx, &n.out); err != nil {
		n.t.Fatal(err)
	}
}

// workload submits random writes, punches and freezes over a few chunks, checkpointing whenever
// one is due and compacting the index's runs when a compaction is.
type workload struct {
	r           *rand.Rand
	version     uint64
	epoch       uint64
	compactions int
	freezes     int
}

func (w *workload) run(n *node, ops int) {
	for range ops {
		for range 1 + w.r.IntN(4) {
			n.submit(w.item(n))
		}
		n.settle()
		if n.crashed {
			return
		}
		tree := n.ix.Tree()
		if !n.checkpointing && n.log.CheckpointDue() {
			n.checkpoint()
			n.settle()
		}
		if c := tree.PickCompaction(); c != nil {
			c.Run()
			if err := tree.Install(c); err != nil {
				n.t.Fatal(err)
			}
			w.compactions++
		}
		if err := n.ix.Check(); err != nil {
			n.t.Fatal(err)
		}
	}
}

func (w *workload) item(n *node) disklog.Item {
	c := core.ChunkID{Volume: 1, Inode: core.InodeID(1 + w.r.IntN(3)), Index: uint32(w.r.IntN(4))}
	if w.r.IntN(8) == 0 {
		if ch, ok := n.ix.Chunk(1, c); ok {
			w.epoch++
			w.freezes++
			return disklog.Item{Entry: disklog.FreezeEntry(1, c, ch.Version, w.epoch)}
		}
	}
	w.version++
	first := uint16(w.r.IntN(core.BlocksPerChunk))
	count := uint16(1 + w.r.IntN(min(6, core.BlocksPerChunk-int(first))))
	switch w.r.IntN(10) {
	case 0:
		return disklog.Item{Entry: disklog.PunchEntry(1, c, w.version, 0, core.BlocksPerChunk)}
	case 1, 2:
		return disklog.Item{Entry: disklog.PunchEntry(1, c, w.version, first, count)}
	}
	buf, err := n.pool.Alloc(int(count) * core.BlockSize)
	if err != nil {
		n.t.Fatal(err)
	}
	b := n.pool.Bytes(buf)
	for i := range b {
		b[i] = byte(w.version)
	}
	return disklog.Item{Entry: disklog.WriteEntry(1, c, w.version, first, count), Buf: buf}
}

func (n *node) submit(it disklog.Item) {
	if _, err := n.log.Submit(&n.ctx, disklog.Hot, it, &n.out); err != nil {
		n.t.Fatal(err)
	}
}

func (n *node) equal(other *Index, what string) {
	n.t.Helper()
	if err := other.Check(); err != nil {
		n.t.Fatalf("%s: %v", what, err)
	}
	if got, want := dumpIndex(other), dumpIndex(n.ix); !slices.Equal(got, want) {
		n.t.Fatalf("%s holds %d entries, the live index %d", what, len(got), len(want))
	}
}

// Replaying the records in LSN order, from the manifest's runs or from the first record, gives the
// live index exactly, across restarts.
func TestRecoveryAndRebuildEqualLiveIndex(t *testing.T) {
	seeds.Run(t, 0, 4, func(seed uint64) {
		n := newNode(t)
		w := &workload{r: rand.New(rand.NewPCG(uint64(seed), 7))}
		for generation := range 3 {
			w.run(n, 150)
			for n.checkpointing {
				n.settle()
			}
			live := n.ix
			n.recover(disklog.ModeRebuild)
			n.equal(live, "rebuilt index")
			n.recover(disklog.ModeOpen)
			n.equal(live, "recovered index")
			if n.ix.Tree().Runs() == 0 || n.replayed == 0 {
				t.Fatalf("seed %d generation %d: %d runs loaded, %d records replayed", seed, generation, n.ix.Tree().Runs(), n.replayed)
			}
		}
		t.Logf("seed %d: %d checkpoints of %d pieces, %d runs carried, %d compactions, %d freezes",
			seed, n.checkpoints, n.pieces, n.carried, w.compactions, w.freezes)
		if n.pieces <= n.checkpoints || n.carried == 0 || w.compactions == 0 || w.freezes == 0 {
			t.Fatalf("seed %d: the workload missed a case", seed)
		}
	})
}

// A crash after a checkpoint's runs are durable but before its manifest is recovers from an
// earlier manifest a segment header names plus replay, even when the runs it wrote came from a
// compaction.
func TestCrashBetweenRunAndManifest(t *testing.T) {
	seeds.Run(t, 0, 4, func(seed uint64) {
		n := newNode(t)
		w := &workload{r: rand.New(rand.NewPCG(uint64(seed), 8))}
		for n.checkpoints < 3 {
			w.run(n, 10)
		}
		w.run(n, 5)
		for n.checkpointing {
			n.settle()
		}
		if c := n.ix.Tree().PickCompaction(); c != nil {
			c.Run()
			if err := n.ix.Tree().Install(c); err != nil {
				t.Fatal(err)
			}
		}
		previous := n.log.Manifest()
		live := n.ix
		n.checkpoint()
		n.crashWhen = func(ev *disklog.Event) bool {
			return ev.Kind == disklog.Durable && slices.ContainsFunc(ev.Entries, func(e disklog.Entry) bool {
				return e.Kind == disklog.EntryBlob && e.BlobKind == disklog.BlobManifest
			})
		}
		n.settle()
		if !n.crashed {
			t.Fatalf("seed %d: no manifest written", seed)
		}
		n.recover(disklog.ModeOpen)
		if got := n.log.Manifest(); got.IsNone() || got.LSN > previous.LSN {
			t.Fatalf("seed %d: recovered from manifest %+v, past the last durable one %+v", seed, got, previous)
		}
		n.equal(live, "index recovered past the lost manifest")
		w.run(n, 40)
		for n.checkpointing {
			n.settle()
		}
		live = n.ix
		n.recover(disklog.ModeOpen)
		n.equal(live, "index after the next checkpoint")
	})
}
