package disklog_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// A snapshot carries each index's merge bounds, with the pieces each leaves dead, and an edit
// replaces them; a bound on a run the index does not hold is Malformed.
func TestManifestBounds(t *testing.T) {
	run := disklog.BlobRef{Segment: 4, Block: 2, LSN: 78, Entry: 3}
	m := sampleManifest()
	m.Indexes[0].Bounds = []disklog.MergeBound{{Run: run, Key: []byte("bound"), Dead: 7}}
	b := disklog.AppendManifest(nil, &m)
	if len(b) != disklog.ManifestSize(&m) {
		t.Fatalf("encoded %d bytes, size says %d", len(b), disklog.ManifestSize(&m))
	}
	if got, err := disklog.DecodeManifest(b); err != nil || !manifestsEqual(got, m) {
		t.Fatalf("decoded %+v, %v", got, err)
	}
	none := sampleManifest()
	if n := len(b) - len(disklog.AppendManifest(nil, &none)); n != 18+4+2+5 {
		t.Fatalf("a bound with a 5-byte key adds %d bytes", n)
	}
	bad := m
	bad.Indexes = slices.Clone(m.Indexes)
	bad.Indexes[1].Bounds = m.Indexes[0].Bounds
	if _, err := disklog.DecodeManifest(disklog.AppendManifest(nil, &bad)); !errors.Is(err, disklog.ErrMalformed) {
		t.Fatalf("a bound on a run the index does not hold: %v", err)
	}

	moved := disklog.ManifestEdit{BaseLSN: 80, NextSeq: 9, Indexes: []disklog.IndexEdit{
		{Owner: 1, ReplayLSN: 70, Bounds: []disklog.MergeBound{{Run: run, Key: []byte("later"), Dead: 9}}},
		{Owner: 2, ReplayLSN: 77},
	}}
	got, err := disklog.ApplyManifestEdit(&m, &moved)
	if err != nil || len(got.Indexes[0].Bounds) != 1 || string(got.Indexes[0].Bounds[0].Key) != "later" || got.Indexes[0].Bounds[0].Dead != 9 {
		t.Fatalf("moved bound applied as %+v, %v", got.Indexes, err)
	}
	ended := moved
	ended.Indexes = []disklog.IndexEdit{{Owner: 1, ReplayLSN: 70, Removed: []disklog.BlobRef{run}}, {Owner: 2, ReplayLSN: 77}}
	if got, err := disklog.ApplyManifestEdit(&m, &ended); err != nil || len(got.Indexes[0].Bounds) != 0 {
		t.Fatalf("an edit without bounds left %+v, %v", got.Indexes, err)
	}
	ended.Indexes[0].Bounds = moved.Indexes[0].Bounds
	if _, err := disklog.ApplyManifestEdit(&m, &ended); !errors.Is(err, disklog.ErrMalformed) {
		t.Fatalf("a bound on a removed run: %v", err)
	}
}

// mergeCheckpoint checkpoints index runOwner carrying runs with bounds, its pieces handed to merge
// worker w's lane, the last a root, and returns the manifest and the pieces' Durable events.
func (h *harness) mergeCheckpoint(w int, runs []disklog.BlobRef, bounds []disklog.MergeBound, sizes ...int) (disklog.Manifest, []disklog.Event) {
	h.t.Helper()
	if err := h.log.BeginCheckpoint([]disklog.CheckpointIndex{{Owner: runOwner, ReplayLSN: h.log.DurableLSN(), Runs: runs, Bounds: bounds}}); err != nil {
		h.t.Fatal(err)
	}
	var evs []disklog.Event
	for i, n := range sizes {
		it := h.blob(disklog.BlobRun, runOwner, n, uint64(i))
		tk, err := h.log.MergePiece(&h.ctx, w, runOwner, it.Buf, uint64(n), i == len(sizes)-1, &h.out)
		if err != nil {
			h.t.Fatal(err)
		}
		h.absorb()
		h.run()
		evs = append(evs, h.durableOf(tk))
	}
	if err := h.log.EndCheckpoint(&h.ctx, &h.out); err != nil {
		h.t.Fatal(err)
	}
	h.absorb()
	h.run()
	if done := h.eventsOf(disklog.CheckpointDone); len(done) == 0 || done[len(done)-1].Manifest != h.log.Manifest() {
		h.t.Fatalf("checkpoint did not finish: %+v", h.eventsOf(disklog.CheckpointFailed))
	}
	m, err := disklog.LoadManifest(h.readChain(posEvent(h, h.log.Manifest())).bodies)
	if err != nil {
		h.t.Fatal(err)
	}
	return m, evs
}

func refOf(ev disklog.Event, tk disklog.Ticket) disklog.BlobRef {
	return disklog.BlobRef{Segment: ev.Segment, Block: ev.Block, LSN: ev.LSN, Entry: uint16(slices.Index(ev.Tickets, tk))}
}

// A merge step's pieces handed over during a checkpoint go to the worker's lane and belong to
// the checkpoint: its manifest waits for them and names the root, with the bounds the checkpoint
// began with; the next manifest's bounds replace them. A root outside a checkpoint is
// NoCheckpoint, and a bound on a run the checkpoint does not carry is Malformed.
func TestMergeStepPiecesJoinTheCheckpoint(t *testing.T) {
	h := formatted(t, gib, mergeConfig(2))
	input := h.checkpointRuns(nil, 1).runs[0]
	it := h.blob(disklog.BlobRun, runOwner, 10, 1)
	if _, err := h.log.MergePiece(&h.ctx, 2, runOwner, it.Buf, 10, true, &h.out); !errors.Is(err, disklog.ErrNoCheckpoint) {
		t.Fatalf("a root outside a checkpoint: %v", err)
	}
	h.pool.Free(it.Buf)
	if err := h.log.BeginCheckpoint([]disklog.CheckpointIndex{{Owner: runOwner, Bounds: []disklog.MergeBound{{Run: input, Key: []byte("k")}}}}); !errors.Is(err, disklog.ErrMalformed) {
		t.Fatalf("a bound on a run not carried: %v", err)
	}
	bounds := []disklog.MergeBound{{Run: input, Key: []byte("k"), Dead: 1}}
	m, evs := h.mergeCheckpoint(2, []disklog.BlobRef{input}, bounds, 3*core.BlockSize, 100)
	root := refOf(evs[1], evs[1].Tickets[len(evs[1].Tickets)-1])
	if ix := m.Indexes[0]; !slices.Equal(ix.Runs, []disklog.BlobRef{input, root}) || !boundsEqual(ix.Bounds, bounds) {
		t.Fatalf("manifest index %+v, want the input bounded and the step's root", ix)
	}
	for _, ev := range evs {
		h.wantLane(ev.Segment, disklog.Meta, 2)
	}
	m, _ = h.mergeCheckpoint(2, []disklog.BlobRef{root}, nil, 100)
	if ix := m.Indexes[0]; len(ix.Bounds) != 0 || len(ix.Runs) != 2 {
		t.Fatalf("the merge's last step left %+v", ix)
	}
	h.close()
}

// A merge step finishes on a disk filled to its reserve: with only the reserve free and a write
// held, a checkpoint writes the step into a reserve segment of the worker's lane and its manifest
// is durable.
func TestMergeStepFinishesInTheReserve(t *testing.T) {
	cfg := disklog.DefaultConfig()
	cfg.MaxReplayBytes, cfg.CompactionStepBytes = 8*core.BlockSize, core.BlockSize
	h := formatted(t, 64*core.BlockSize, cfg)
	for i := range 6 {
		h.put(disklog.Hot, h.write(uint32(i), 1, uint64(i)))
	}
	h.checkpointRuns(nil, 0)
	h.victim = true
	h.submit(disklog.Hot, h.write(6, 1, 6))
	if !h.log.Stalled() || disklog.FreeSegments(h.log) > int(h.log.Reserve()) {
		t.Fatalf("%d free segments, reserve %d, stalled %v", disklog.FreeSegments(h.log), h.log.Reserve(), h.log.Stalled())
	}
	m, evs := h.mergeCheckpoint(1, nil, nil, core.BlockSize)
	h.wantLane(evs[0].Segment, disklog.Meta, 1)
	if free := disklog.FreeSegments(h.log); len(m.Indexes[0].Runs) != 1 || free >= int(h.log.Reserve()) {
		t.Fatalf("manifest %+v with %d free segments", m.Indexes, free)
	}
	h.log.FailStalled(&h.ctx, &h.out)
	h.absorb()
	h.victim = false
	h.close()
}
