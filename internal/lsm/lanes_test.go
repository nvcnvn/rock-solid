package lsm

import (
	"fmt"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// laneOwner is a disk's owner over a disk log on a simulated disk, executing each action at once:
// it checkpoints its trees, handing a merge step's pieces to the merge worker's lane through
// MergePiece and the frozen memtables' to the checkpoint's through CheckpointPiece, and records
// what each segment holds.
type laneOwner struct {
	t     *testing.T
	pool  *sim.Pool
	ctx   core.Context
	disk  *sim.Disk
	log   *disklog.Log
	out   disklog.Output
	trees []*Tree
	refs  map[disklog.Ticket]disklog.BlobRef
	kinds map[disklog.Ticket]string
	segs  map[uint32][]string
	// mergeSegs is the segment of each merge piece, in order; held the bytes of entry and fragment
	// blocks each worker handed over since the newest durable manifest, maxHeld their most;
	// heldFilter and maxFilter the same for filter partitions, and filterPieces the merge filter
	// pieces of each checkpoint.
	mergeSegs    []uint32
	held         map[int]int
	maxHeld      int
	heldFilter   map[int]int
	maxFilter    int
	filterPieces []int
	done         bool
}

func newLaneOwner(t *testing.T, trees int, cfg Config) *laneOwner {
	o := &laneOwner{t: t, pool: sim.NewPool(64<<20, 0), disk: sim.NewDisk((256 << 20) / core.BlockSize),
		refs: map[disklog.Ticket]disklog.BlobRef{}, kinds: map[disklog.Ticket]string{}, segs: map[uint32][]string{}, held: map[int]int{}, heldFilter: map[int]int{}}
	o.ctx = core.Context{Now: 1, Raw: 2, Pool: o.pool}
	geo, err := disklog.GeometryFor(256 << 20)
	if err != nil {
		t.Fatal(err)
	}
	lcfg := disklog.DefaultConfig()
	lcfg.Check, lcfg.NoReserve = true, true
	if o.log, err = disklog.Format(&o.ctx, lcfg, disklog.Superblock{LogID: 0x5eed, Geometry: geo}, 1000, &o.out); err != nil {
		t.Fatal(err)
	}
	o.settle()
	for range trees {
		tr, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		o.trees = append(o.trees, tr)
	}
	return o
}

func (o *laneOwner) settle() {
	for len(o.out.Events)+len(o.out.Actions) > 0 {
		evs, acts := slices.Clone(o.out.Events), slices.Clone(o.out.Actions)
		o.out.Reset()
		for _, ev := range evs {
			switch ev.Kind {
			case disklog.Durable:
				for i, tk := range ev.Tickets {
					kind, ok := o.kinds[tk]
					if !ok && (ev.Entries[i].BlobKind == disklog.BlobManifest || ev.Entries[i].BlobKind == disklog.BlobManifestPiece) {
						kind = "manifest"
					}
					o.segs[ev.Segment] = append(o.segs[ev.Segment], kind)
					o.refs[tk] = disklog.BlobRef{Segment: ev.Segment, Block: ev.Block, LSN: ev.LSN, Entry: uint16(i)}
					if kind == "merge" {
						o.mergeSegs = append(o.mergeSegs, ev.Segment)
					}
				}
			case disklog.CheckpointDone:
				o.done = true
			case disklog.CheckpointFailed, disklog.Failed, disklog.InDoubt:
				o.t.Fatalf("log event %+v", ev)
			}
		}
		for _, a := range acts {
			done := o.disk.Execute(o.pool, &a)
			o.log.OnEvent(&o.ctx, &done, &o.out)
		}
	}
}

// checkpoint flushes every tree, a piece at a time, and waits for the manifest.
func (o *laneOwner) checkpoint() {
	var idx []disklog.CheckpointIndex
	for i, tr := range o.trees {
		if _, err := tr.BeginFlush(); err != nil {
			o.t.Fatal(err)
		}
		idx = append(idx, disklog.CheckpointIndex{Owner: uint32(i + 1), ReplayLSN: o.log.DurableLSN(), Runs: tr.CarriedRuns(), Bounds: tr.Bounds()})
	}
	if err := o.log.BeginCheckpoint(idx); err != nil {
		o.t.Fatal(err)
	}
	pieces := 0
	for i, tr := range o.trees {
		for {
			buf, err := o.pool.Alloc(int(o.log.MaxBlobBytes()))
			if err != nil {
				o.t.Fatal(err)
			}
			n, root, err := tr.NextPiece(o.pool.Bytes(buf))
			if err != nil {
				o.t.Fatal(err)
			}
			if n == 0 {
				o.pool.Free(buf)
				break
			}
			var tk disklog.Ticket
			if w := tr.PieceWorker(); w == 0 {
				tk, err = o.log.CheckpointPiece(&o.ctx, uint32(i+1), buf, uint64(n), root, &o.out)
				o.kinds[tk] = "flush"
			} else {
				block, kind, _, _, perr := splitPiece(o.pool.Bytes(buf)[:n], nil)
				if perr != nil {
					o.t.Fatal(perr)
				}
				if kind == filterBlock {
					o.heldFilter[w] += len(block)
					o.maxFilter = max(o.maxFilter, o.heldFilter[w])
					pieces++
				} else if len(block) > emptyBlockSize {
					o.held[w] += len(block)
					o.maxHeld = max(o.maxHeld, o.held[w])
				}
				tk, err = o.log.MergePiece(&o.ctx, w, uint32(i+1), buf, uint64(n), root, &o.out)
				o.kinds[tk] = "merge"
			}
			if err != nil {
				o.t.Fatal(err)
			}
			o.settle()
			if err := tr.PieceDurable(o.refs[tk]); err != nil {
				o.t.Fatal(err)
			}
		}
	}
	o.done = false
	if err := o.log.EndCheckpoint(&o.ctx, &o.out); err != nil {
		o.t.Fatal(err)
	}
	o.settle()
	if !o.done {
		o.t.Fatalf("the manifest never became durable: in flight %d, queued %d, err %v, %d segments used, stalled %v, seg1 %v seg2 %v seg3 %v", o.log.InFlight(), o.log.Queued(), o.log.Err(), len(o.segs), o.log.Stalled(), o.segs[1], o.segs[2], o.segs[3])
	}
	clear(o.held)
	clear(o.heldFilter)
	o.filterPieces = append(o.filterPieces, pieces)
	for _, tr := range o.trees {
		if _, err := tr.CheckpointDone(); err != nil {
			o.t.Fatal(err)
		}
	}
}

// Merge output fills its merge worker's lane: with two indexes merging in steps on one worker and
// checkpoint flushes going on, no merge piece shares a segment with a flush piece or a manifest,
// the worker's next merge continues the segment the first one left open, every key reads the
// same throughout, and a merge never holds more than a step of entries and fragments beyond the manifests.
func TestMergeOutputFillsWorkerLane(t *testing.T) {
	const piece, step = 4096, 4096
	o := newLaneOwner(t, 2, stepConfig(piece, step))
	s, err := NewScheduler(SchedulerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	models := []model{{}, {}}
	for i, tr := range o.trees {
		s.Add(tr)
		for run := range 4 {
			for k := range 150 {
				put(t, tr, models[i], fmt.Sprintf("i%d-%03d", i, (k*7+run)%300), fmt.Sprintf("r%d%040d", run, k))
			}
			o.checkpoint()
		}
	}
	var lastOf []uint32
	var firstOf []uint32
	var prev *Compaction
	extra, steps := 0, 0
	for {
		c := s.Next()
		if c == nil && s.Running() == 0 {
			break
		}
		if c != nil {
			if c != prev {
				if len(firstOf) == 2 {
					break
				}
				if prev != nil {
					lastOf = append(lastOf, o.mergeSegs[len(o.mergeSegs)-1])
				}
				firstOf = append(firstOf, uint32(len(o.mergeSegs)))
				prev = c
			}
			if c.Worker() != 1 {
				t.Fatalf("merge on worker %d of 1", c.Worker())
			}
			if len(firstOf) == 1 {
				steps++
			}
			c.Run()
			if err := s.Install(c); err != nil {
				t.Fatal(err)
			}
		}
		for i, tr := range o.trees {
			put(t, tr, models[i], fmt.Sprintf("i%d-new%03d", i, extra), "x")
			checkReads(t, tr, models[i], []string{fmt.Sprintf("i%d-100", i)})
		}
		extra++
		o.checkpoint()
	}
	if len(firstOf) != 2 || len(lastOf) != 1 || steps < 3 {
		t.Fatalf("%d merges ran, the first in %d steps", len(firstOf), steps)
	}
	if second := o.mergeSegs[firstOf[1]]; second != lastOf[0] {
		t.Fatalf("the second merge opened segment %d, not the first one's open segment %d", second, lastOf[0])
	}
	for seg, kinds := range o.segs {
		if slices.Contains(kinds, "merge") && slices.ContainsFunc(kinds, func(k string) bool { return k != "merge" }) {
			t.Fatalf("segment %d holds %v", seg, kinds)
		}
	}
	if o.maxHeld == 0 || o.maxHeld > step+piece {
		t.Fatalf("a merge held %d bytes of entries beyond the manifests, step %d", o.maxHeld, step)
	}
	for i, tr := range o.trees {
		checkReads(t, tr, models[i], []string{fmt.Sprintf("i%d-100", i)})
	}
}

// A merge whose steps' filter partitions span several pieces holds at most a step of entries and
// fragments and its own partition beyond the manifests, the last step included, and its output reads
// like the map through the partitions, prefix lookups included.
func TestMergeStepFilterSpansPieces(t *testing.T) {
	const piece, step = 512, 2048
	cfg := Config{MaxPiece: piece, StepBytes: step, Check: true, Filter: KeysAndPrefixes, Prefix: func(k []byte) int { return min(len(k), 2) }}
	fill := func(tr *Tree, m model, n int, checkpoint func()) {
		for run := range 4 {
			for k := range n {
				put(t, tr, m, fmt.Sprintf("%02x%x", (k*7+run)%256, run), "v")
			}
			checkpoint()
		}
	}
	keys := 200
	for ; keys < 256; keys++ {
		dry, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		f, dm := newFakeLog(), model{}
		fill(dry, dm, keys, func() { f.checkpoint(t, dry, nil) })
		mergeAll(t, dry, f, dm, nil)
		if parts := dry.runs[0].parts; len(parts) >= 3 && len(parts[len(parts)-1].enc) > piece-pieceOverhead {
			break
		}
	}
	o := newLaneOwner(t, 1, cfg)
	tr, m := o.trees[0], model{}
	fill(tr, m, keys, o.checkpoint)
	for c := tr.pickSpan(0, 4, ruleNone); c != nil; c = tr.PickCompaction() {
		c.Run()
		if err := tr.Install(c); err != nil {
			t.Fatal(err)
		}
		o.checkpoint()
	}
	out := tr.runs[0]
	biggest := 0
	for _, p := range out.parts {
		biggest = max(biggest, len(p.enc))
	}
	last := o.filterPieces[len(o.filterPieces)-1]
	if tr.Runs() != 1 || len(out.parts) < 3 || last < 2 {
		t.Fatalf("%d runs, %d partitions, the last step's filter in %d pieces", tr.Runs(), len(out.parts), last)
	}
	if o.maxHeld > step+piece || o.maxFilter > biggest {
		t.Fatalf("a step held %d bytes of entries and %d of filter beyond the manifests; step %d, largest partition %d", o.maxHeld, o.maxFilter, step, biggest)
	}
	checkReads(t, tr, m, []string{"00", "7f", "7f1", "ff", "ff3", "zz"})
}
