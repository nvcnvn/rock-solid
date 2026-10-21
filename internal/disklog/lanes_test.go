package disklog_test

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

func mergeConfig(workers int) disklog.Config {
	cfg := testConfig(0)
	cfg.MergeWorkers = workers
	return cfg
}

func (h *harness) mergePiece(w int, owner uint32, n int, seed uint64) disklog.Ticket {
	h.t.Helper()
	it := h.blob(disklog.BlobRun, owner, n, seed)
	tk, err := h.log.MergePiece(&h.ctx, w, owner, it.Buf, uint64(n), &h.out)
	if err != nil {
		h.t.Fatalf("merge piece on lane %d: %v", w, err)
	}
	h.absorb()
	return tk
}

func (h *harness) putMerge(w int, owner uint32, n int, seed uint64) disklog.Event {
	h.t.Helper()
	tk := h.mergePiece(w, owner, n, seed)
	h.run()
	return h.durableOf(tk)
}

func (h *harness) durableOf(tk disklog.Ticket) disklog.Event {
	h.t.Helper()
	for _, ev := range h.eventsOf(disklog.Durable) {
		if slices.Contains(ev.Tickets, tk) {
			return ev
		}
	}
	h.t.Fatalf("ticket %d never became durable", tk)
	return disklog.Event{}
}

func (h *harness) wantLane(seg uint32, s disklog.Stream, lane uint8) {
	h.t.Helper()
	if sh := h.segmentHeader(seg); sh.Stream != s || sh.Lane != lane {
		h.t.Fatalf("segment %d is stream %d lane %d, want stream %d lane %d", seg, sh.Stream, sh.Lane, s, lane)
	}
}

func TestLanesTakeTurns(t *testing.T) {
	h := formatted(t, gib, testConfig(0))
	a := h.submit(disklog.Hot, h.write(0, 1, 1))
	b := h.submit(disklog.Hot, h.write(1, 1, 2))
	c := h.submit(disklog.Cold, h.blob(disklog.BlobRaftEntries, 5, 10, 3))
	if err := h.log.BeginCheckpoint([]disklog.CheckpointIndex{{Owner: runOwner}}); err != nil {
		t.Fatal(err)
	}
	it := h.blob(disklog.BlobRun, runOwner, 10, 4)
	m, err := h.log.CheckpointPiece(&h.ctx, runOwner, it.Buf, 10, true, &h.out)
	if err != nil {
		t.Fatal(err)
	}
	h.absorb()
	h.round()
	h.wantTrace("write 64+3 io104", "fsync io105", "write 128+3 io106", "fsync io107")
	d := h.submit(disklog.Hot, h.write(2, 1, 4))
	h.run()
	h.wantTrace("write 64+3 io104", "fsync io105", "write 128+3 io106", "fsync io107", "write 192+3 io108", "fsync io109", "write 67+3 io110", "fsync io111")
	want := [][]disklog.Ticket{{a}, {c}, {m}, {b, d}}
	for i, ev := range h.eventsOf(disklog.Durable) {
		if ev.LSN != uint64(i+1) || !slices.Equal(ev.Tickets, want[i]) {
			t.Fatalf("round %d: %+v", i+1, ev)
		}
	}
	h.wantLane(2, disklog.Cold, 0)
	h.wantLane(3, disklog.Meta, 0)
	if sh := h.segmentHeader(3); sh.Seq != 3 {
		t.Fatalf("segment 3 header %+v", sh)
	}
}

// Two merge workers running and a checkpoint give three open metadata segments, each lane
// appending only to its own.
func TestMergeLanesOpenSegmentsOfTheirOwn(t *testing.T) {
	h := formatted(t, gib, mergeConfig(2))
	h.put(disklog.Hot, h.write(0, 1, 1))
	a := h.putMerge(1, runOwner, 100, 1)
	b := h.putMerge(2, runOwner, 100, 2)
	cp := h.checkpointRuns(nil, 1)
	a2 := h.putMerge(1, runOwner+1, 100, 3)
	cp2 := h.checkpointRuns(cp.runs, 1)
	for _, c := range []struct {
		name string
		seg  uint32
		lane uint8
	}{
		{"worker 1's first piece", a.Segment, 1},
		{"worker 2's piece", b.Segment, 2},
		{"the flush piece", cp.runs[0].Segment, 0},
		{"the manifest", cp.last.Segment, 0},
		{"worker 1's second piece", a2.Segment, 1},
		{"the second flush piece", cp2.runs[1].Segment, 0},
	} {
		h.wantLane(c.seg, disklog.Meta, c.lane)
	}
	if a.Segment != a2.Segment || cp.runs[0].Segment != cp2.runs[1].Segment || a.Segment == b.Segment || a.Segment == cp.last.Segment {
		t.Fatalf("worker 1 in segments %d and %d, worker 2 in %d, flush pieces in %d and %d", a.Segment, a2.Segment, b.Segment, cp.runs[0].Segment, cp2.runs[1].Segment)
	}
	for _, w := range []int{0, 3} {
		it := h.blob(disklog.BlobRun, runOwner, 10, 9)
		if _, err := h.log.MergePiece(&h.ctx, w, runOwner, it.Buf, 10, &h.out); !errors.Is(err, disklog.ErrMalformed) {
			t.Fatalf("merge piece on lane %d of 2: %v", w, err)
		}
		h.pool.Free(it.Buf)
	}
	it := h.blob(disklog.BlobRaftEntries, 9, 10, 9)
	if _, err := h.log.Submit(&h.ctx, disklog.Meta, it, &h.out); !errors.Is(err, disklog.ErrMalformed) {
		t.Fatalf("submit to the metadata stream: %v", err)
	}
	h.pool.Free(it.Buf)
	h.wantLive(0)
	h.close()
}

// A worker's segment stays open across its merges, of one index or another, and after a
// restart. Merge records are delivered to no index, but count for the durable LSN.
func TestAMergeLaneContinuesItsSegment(t *testing.T) {
	h := formatted(t, gib, mergeConfig(1))
	a := h.putMerge(1, 5, 100, 1)
	b := h.putMerge(1, 6, 100, 2)
	h.put(disklog.Hot, h.write(0, 1, 3))
	if a.Segment != b.Segment || b.Block != a.Block+2 {
		t.Fatalf("index A's piece at %d/%d, index B's at %d/%d", a.Segment, a.Block, b.Segment, b.Block)
	}
	h.close()
	res := h.reopen()
	h.wantLSNs(res, []uint64{3})
	c := h.putMerge(1, 6, 100, 4)
	if c.Segment != a.Segment || c.Block != b.Block+2 || c.LSN != 4 {
		t.Fatalf("after a restart the lane appends at %d/%d LSN %d, want %d/%d LSN 4", c.Segment, c.Block, c.LSN, a.Segment, b.Block+2)
	}
	cp := h.checkpointRuns(nil, 1)
	if cp.runs[0].Segment == a.Segment {
		t.Fatal("a flush piece landed in the merge lane")
	}
	h.close()
}

// mergeAcrossTheReplayPoint stages "Recovery reads each lane from the last of its segments
// whose first record is at or before the first record to replay": merge worker 1 opens its
// segment before the checkpoints' shared metadata segment and takes pieces on both sides of
// the replay point. It returns the replay LSN and the LSNs of the merge pieces past it; hot
// writes follow them unless newest.
func mergeAcrossTheReplayPoint(t *testing.T, newest bool) (*harness, uint64, []uint64) {
	h := formatted(t, gib, mergeConfig(1))
	h.put(disklog.Hot, h.write(0, 1, 1))
	h.putMerge(1, runOwner, 100, 1)
	cp := h.checkpointRuns(nil, 1)
	h.put(disklog.Hot, h.write(1, 1, 2))
	h.putMerge(1, runOwner, 100, 2)
	cp = h.checkpointRuns(cp.runs, 1)
	h.nameManifest()
	var merges []uint64
	for i := range 2 {
		merges = append(merges, h.putMerge(1, runOwner, 100, uint64(3+i)).LSN)
	}
	if !newest {
		h.put(disklog.Hot, h.write(2, 1, 5))
	}
	h.close()
	return h, cp.base, merges
}

func TestRecoverReadsEachLaneFromItsOwnStart(t *testing.T) {
	for _, newest := range []bool{false, true} {
		h, base, merges := mergeAcrossTheReplayPoint(t, newest)
		durable := h.log.DurableLSN()
		var want []uint64
		for lsn := base + 1; lsn <= durable; lsn++ {
			if !slices.Contains(merges, lsn) {
				want = append(want, lsn)
			}
		}
		res := h.reopen()
		if got := res.lsns(); !slices.Equal(got, want) || res.durable != durable {
			t.Fatalf("newest %v: replayed %v to LSN %d, want %v to %d", newest, got, res.durable, want, durable)
		}
		h.close()

		h.plant = disklog.ReplayPerStream
		res = h.recover(disklog.ModeOpen)
		h.plant = nil
		if !newest {
			wantCorrupt(t, res, merges[0])
			continue
		}
		if res.err != nil || res.durable != merges[0]-1 {
			t.Fatalf("per stream, merge records newest: %v at LSN %d, want the tail cut at %d", res.err, res.durable, merges[0]-1)
		}
		h.put(disklog.Hot, h.write(3, 1, 6))
		h.close()
		wantCorrupt(t, h.recover(disklog.ModeOpen), merges[0])
	}
}

func TestRecoverWithoutMergeLanesIsCorrupt(t *testing.T) {
	h, _, merges := mergeAcrossTheReplayPoint(t, false)
	h.plant = disklog.SkipMergeLanes
	wantCorrupt(t, h.recover(disklog.ModeOpen), merges[0])
}

func TestRecoverTornMergeRecordAtTheTail(t *testing.T) {
	h := formatted(t, gib, mergeConfig(1))
	h.putMerge(1, runOwner, 100, 1)
	h.mergePiece(1, runOwner, 3*core.BlockSize, 2)
	h.wantTrace("write 64+3 io104", "fsync io105", "write 67+4 io106", "fsync io107")
	h.step(0)
	h.crashLanding(67, 68, 69)
	res := h.reopen()
	if res.durable != 1 || !isZeroBlock(h.read(67, 1)) {
		t.Fatalf("recovered LSN %d; the torn merge record's header %s", res.durable, map[bool]string{true: "erased", false: "kept"}[isZeroBlock(h.read(67, 1))])
	}
	if ev := h.putMerge(1, runOwner, 100, 3); ev.LSN != 2 || ev.Segment != 1 || ev.Block != 3 {
		t.Fatalf("next merge piece %+v, want LSN 2 at 1/3", ev)
	}
	h.close()
	if res := h.reopen(); res.durable != 2 {
		t.Fatalf("recovered LSN %d, want 2", res.durable)
	}
}

func TestRecoverCrashInTheRoundThatOpensAMergeSegment(t *testing.T) {
	for _, header := range []bool{false, true} {
		h := formatted(t, gib, mergeConfig(1))
		h.put(disklog.Hot, h.write(0, 1, 1))
		h.trace = nil
		h.mergePiece(1, runOwner, 100, 2)
		h.wantTrace("write 128+3 io106", "fsync io107")
		h.step(0)
		landed := []core.LBA{129, 130}
		if header {
			landed = append(landed, 128)
		}
		h.crashLanding(landed...)
		res := h.reopen()
		last := uint64(1)
		if header {
			last = 2
		}
		if res.durable != last {
			t.Fatalf("header %v: recovered LSN %d, want %d", header, res.durable, last)
		}
		if !header && !isZeroBlock(h.read(129, 1)) {
			t.Fatal("the record of a merge segment whose header never landed was not erased")
		}
		ev := h.putMerge(1, runOwner, 100, 3)
		wantSeq, wantBlock := uint64(3), uint32(1)
		if header {
			wantSeq, wantBlock = 2, 3
		}
		if ev.Segment != 2 || ev.Block != wantBlock || h.segmentHeader(2).Seq != wantSeq {
			t.Fatalf("header %v: next merge piece at %d/%d in seq %d", header, ev.Segment, ev.Block, h.segmentHeader(2).Seq)
		}
		h.wantLane(2, disklog.Meta, 1)
		h.close()
		if res := h.reopen(); res.durable != last+1 {
			t.Fatalf("header %v: recovered LSN %d, want %d", header, res.durable, last+1)
		}
	}
}

// windowReads is the segments a recovery's trace reads past their header.
func (h *harness) windowReads(trace []string) []uint32 {
	var segs []uint32
	for _, line := range trace {
		var lba, n, io uint64
		if _, err := fmt.Sscanf(line, "read %d+%d io%d", &lba, &n, &io); err != nil {
			continue
		}
		if s := uint32(lba / uint64(h.geo.SegmentBlocks)); s != 0 && lba%uint64(h.geo.SegmentBlocks) != 0 && !slices.Contains(segs, s) {
			segs = append(segs, s)
		}
	}
	return segs
}

// A merge worker configured away leaves its lane: its last segment is read at every start until
// the cleaner frees it, and no record goes to it.
func TestALaneWhoseWorkerIsGone(t *testing.T) {
	h := formatted(t, gib, mergeConfig(2))
	h.put(disklog.Hot, h.write(0, 1, 1))
	gone := h.putMerge(2, runOwner, 100, 1)
	h.close()
	h.cfg.MergeWorkers = 1
	for range 2 {
		res := h.reopen()
		if res.durable < gone.LSN || !slices.Contains(h.windowReads(res.trace), gone.Segment) {
			t.Fatalf("recovered LSN %d, reading segments %v; want LSN %d and segment %d read", res.durable, h.windowReads(res.trace), gone.LSN, gone.Segment)
		}
		if !slices.ContainsFunc(disklog.SegmentUsageOf(h.log), func(u disklog.SegmentUsage) bool { return u.Segment == gone.Segment && u.Live == 1 }) {
			t.Fatalf("usage %+v lost lane 2's segment", disklog.SegmentUsageOf(h.log))
		}
		it := h.blob(disklog.BlobRun, runOwner, 10, 9)
		if _, err := h.log.MergePiece(&h.ctx, 2, runOwner, it.Buf, 10, &h.out); !errors.Is(err, disklog.ErrMalformed) {
			t.Fatalf("merge piece on a lane past the merge workers: %v", err)
		}
		h.pool.Free(it.Buf)
		if ev := h.putMerge(1, runOwner, 100, 2); ev.Segment == gone.Segment {
			t.Fatal("worker 1 wrote into worker 2's lane")
		}
		h.put(disklog.Hot, h.write(1, 1, 3))
		h.put(disklog.Cold, h.write(2, 1, 4))
		for _, ev := range h.eventsOf(disklog.Durable) {
			if ev.Segment == gone.Segment {
				t.Fatalf("%+v went to a lane whose worker is gone", ev)
			}
		}
		h.close()
	}
}

// Replay reads at most one segment per lane that holds no record past the replay point: the
// hot and merge lanes each fill three segments before it and open a fourth after it.
func TestReplayReadsOneSegmentPerLaneBeforeTheReplayPoint(t *testing.T) {
	h := formatted(t, gib, mergeConfig(1))
	for i := range 3 {
		h.put(disklog.Hot, h.write(uint32(i), 62, uint64(i)))
		h.putMerge(1, runOwner, int(h.log.MaxBlobBytes()), uint64(i))
	}
	cp := h.checkpointRuns(nil, 1)
	h.nameManifest()
	h.putMerge(1, runOwner, int(h.log.MaxBlobBytes()), 3)
	after := map[uint32]bool{}
	for _, ev := range h.eventsOf(disklog.Durable) {
		if ev.LSN > cp.base {
			after[ev.Segment] = true
		}
	}
	h.close()
	res := h.reopen()
	before := map[[2]uint8]int{}
	for _, s := range h.windowReads(res.trace) {
		if sh := h.segmentHeader(s); !after[s] {
			before[[2]uint8{uint8(sh.Stream), sh.Lane}]++
		}
	}
	for lane, n := range before {
		if n > 1 {
			t.Fatalf("replay read %d segments of stream %d lane %d before the replay point: %v", n, lane[0], lane[1], h.windowReads(res.trace))
		}
	}
	if len(before) != 2 {
		t.Fatalf("segments read before the replay point by lane: %v, want one hot and one merge", before)
	}
	h.close()
}

func TestAMergeLaneAppendsOnlyOnceItsOpeningRoundIsDurable(t *testing.T) {
	cfg := mergeConfig(1)
	cfg.CommitDepth = 2
	h := formatted(t, gib, cfg)
	h.mergePiece(1, runOwner, 100, 1)
	h.mergePiece(1, runOwner, 100, 2)
	h.wantTrace("write 64+3 io104", "fsync io105")
	h.round()
	h.wantTrace("write 64+3 io104", "fsync io105", "write 67+2 io106", "fsync io107")
	h.run()
	h.close()
}

// Merge output is not ordinary: it goes out while ordinary items are held behind a checkpoint,
// and it may open a segment of the reserve, which holds a step of each worker.
func TestMergePiecesGoPastTheHoldAndIntoTheReserve(t *testing.T) {
	cfg := mergeConfig(1)
	cfg.MaxReplayBytes = 4 * core.BlockSize
	h := formatted(t, gib, cfg)
	h.put(disklog.Hot, h.write(0, 3, 0))
	h.put(disklog.Hot, h.write(1, 3, 1))
	held := h.submit(disklog.Hot, h.write(2, 1, 2))
	m := h.putMerge(1, runOwner, 100, 3)
	if h.log.Queued() != 1 || slices.ContainsFunc(h.eventsOf(disklog.Durable), func(ev disklog.Event) bool { return slices.Contains(ev.Tickets, held) }) {
		t.Fatalf("the hold let the write out or kept the merge piece (LSN %d): %+v", m.LSN, h.events)
	}
	h.checkpointRuns(nil, 1)
	h.durableOf(held)
	h.close()

	cfg = mergeConfig(1)
	cfg.NoReserve, cfg.Reserve = false, 2
	h = formatted(t, 32*core.BlockSize, cfg)
	h.victim = true
	for i := range 5 {
		h.put(disklog.Hot, h.write(uint32(i), 2, uint64(i)))
	}
	held = h.submit(disklog.Hot, h.write(5, 1, 5))
	if m := h.putMerge(1, runOwner, 100, 6); m.Segment != 6 || !h.log.Stalled() {
		t.Fatalf("merge piece in segment %d, write stalled %v: want the reserve's segment 6 and the write held", m.Segment, h.log.Stalled())
	}
	h.log.FailStalled(&h.ctx, &h.out)
	h.absorb()
	if failed := h.eventsOf(disklog.Failed); len(failed) != 1 || !slices.Equal(failed[0].Tickets, []disklog.Ticket{held}) {
		t.Fatalf("no victim: %+v", h.events)
	}
	h.close()
}

// Replay takes a window per lane past the hot and cold ones; a pool too small for them fails
// recovery and leaves no buffer allocated.
func TestRecoveryFailsWhenALaneWindowDoesNotFit(t *testing.T) {
	h := formatted(t, gib, mergeConfig(1))
	h.put(disklog.Hot, h.write(0, 1, 1))
	h.putMerge(1, runOwner, 100, 2)
	h.checkpointRuns(nil, 1)
	h.close()
	window := int(h.cfg.MaxRecordBytes)
	h.poolBytes = h.cfg.ReadDepth*2*core.BlockSize + 3*window
	if res := h.recover(disklog.ModeOpen); !errors.Is(res.err, core.ErrPoolExhausted) {
		t.Fatalf("a pool with room for one lane window of two: %v", res.err)
	}
	h.poolBytes += window
	h.reopen()
	h.close()
}
