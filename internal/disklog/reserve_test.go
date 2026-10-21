package disklog_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

func TestReserveSegments(t *testing.T) {
	const mib = 1 << 20
	for _, c := range []struct {
		segmentBytes uint64
		workers      int
		want         uint32
	}{
		{64 * mib, 1, 8},
		{64 * mib, 2, 8},
		{32 * mib, 1, 10},
		{256 << 10, 1, 772},
	} {
		if got := disklog.ReserveSegments(64*mib, 64*mib, c.workers, uint32(c.segmentBytes/core.BlockSize)); got != c.want {
			t.Errorf("%d-byte segments, %d merge workers: reserve %d, want %d", c.segmentBytes, c.workers, got, c.want)
		}
	}
}

func TestReserveLeavingNoSegmentIsRefused(t *testing.T) {
	cfg := testConfig(0)
	cfg.Reserve = 3
	h := blankHarness(t, 16*core.BlockSize, 1<<20, cfg)
	if _, err := disklog.Format(&h.ctx, h.cfg, disklog.Superblock{LogID: testLogID, Geometry: h.geo}, firstIO, &h.out); !errors.Is(err, disklog.ErrBadConfig) {
		t.Fatalf("format with a reserve of every usable segment: %v", err)
	}
	if h.pool.Live() != 0 {
		t.Fatalf("%d buffers left allocated", h.pool.Live())
	}

	h = formatted(t, 16*core.BlockSize, testConfig(0))
	h.put(disklog.Hot, h.write(0, 2, 1))
	h.close()
	h.cfg.Reserve = 3
	if res := h.recover(disklog.ModeOpen); !errors.Is(res.err, disklog.ErrBadConfig) {
		t.Fatalf("open with a reserve of every usable segment: %v", res.err)
	}
}

// reserveDisk is a 16-block disk of 3 usable 4-block segments with a reserve of one, whose two
// other segments two 2-block writes fill (LBAs 4 and 8), and a third write held for room.
func reserveDisk(t *testing.T) (*harness, disklog.Ticket) {
	cfg := testConfig(0)
	cfg.Reserve = 1
	h := formatted(t, 16*core.BlockSize, cfg)
	h.victim = true
	for i := range 2 {
		h.put(disklog.Hot, h.write(uint32(i), 2, uint64(i)))
	}
	held := h.submit(disklog.Hot, h.write(2, 2, 2))
	h.wantTrace("write 4+4 io104", "fsync io105", "write 8+4 io106", "fsync io107")
	if !h.log.Stalled() || len(h.eventsOf(disklog.Failed)) != 0 {
		t.Fatalf("stalled %v, events %+v", h.log.Stalled(), h.events)
	}
	h.wantLive(1)
	return h, held
}

func TestFullDiskWaits(t *testing.T) {
	h, held := reserveDisk(t)
	if err := h.log.BeginCheckpoint([]disklog.CheckpointIndex{{Owner: 5, ReplayLSN: 2}}); err != nil {
		t.Fatal(err)
	}
	it := h.blob(disklog.BlobRun, 5, 10, 1)
	piece, err := h.log.CheckpointPiece(&h.ctx, 5, it.Buf, 10, true, &h.out)
	if err != nil {
		t.Fatal(err)
	}
	h.absorb()
	if err := h.log.EndCheckpoint(&h.ctx, &h.out); err != nil {
		t.Fatal(err)
	}
	h.absorb()
	h.run()
	h.wantTrace("write 4+4 io104", "fsync io105", "write 8+4 io106", "fsync io107",
		"write 12+3 io108", "fsync io109")
	if d := h.eventsOf(disklog.Durable); len(d) != 3 || !slices.Equal(d[2].Tickets, []disklog.Ticket{piece}) || d[2].Segment != 3 {
		t.Fatalf("the piece is not in the reserve segment: %+v", d)
	}
	if !h.log.Stalled() || len(h.eventsOf(disklog.CheckpointDone)) != 0 {
		t.Fatalf("the manifest did not wait for room: %+v", h.events)
	}

	disklog.FreeSegment(h.log, 1, true)
	disklog.FreeSegment(h.log, 2, true)
	disklog.StartRounds(h.log, &h.ctx, &h.out)
	h.absorb()
	h.run()
	h.wantTrace("write 4+4 io104", "fsync io105", "write 8+4 io106", "fsync io107",
		"write 12+3 io108", "fsync io109",
		"write 4+4 io110", "fsync io111", "write 8+3 io112", "fsync io113")
	d := h.eventsOf(disklog.Durable)
	if len(d) != 5 || !slices.Equal(d[3].Tickets, []disklog.Ticket{held}) || len(h.eventsOf(disklog.CheckpointDone)) != 1 {
		t.Fatalf("events %+v", h.events)
	}
	if h.log.Stalled() || len(h.eventsOf(disklog.Failed)) != 0 {
		t.Fatalf("events %+v", h.events)
	}
	h.close()
}

func TestCheckpointFinishesInTheReserve(t *testing.T) {
	cfg := testConfig(0)
	cfg.Reserve = 2
	h := formatted(t, 32*core.BlockSize, cfg)
	h.victim = true
	for i := range 5 {
		h.put(disklog.Hot, h.write(uint32(i), 2, uint64(i)))
	}
	held := h.submit(disklog.Hot, h.write(5, 1, 5))
	if !h.log.Stalled() || len(h.pending) != 0 {
		t.Fatalf("a write opened a reserve segment: %v", h.trace)
	}
	if err := h.log.BeginCheckpoint([]disklog.CheckpointIndex{{Owner: 5, ReplayLSN: 5}}); err != nil {
		t.Fatal(err)
	}
	it := h.blob(disklog.BlobRun, 5, 10, 1)
	if _, err := h.log.CheckpointPiece(&h.ctx, 5, it.Buf, 10, true, &h.out); err != nil {
		t.Fatal(err)
	}
	h.absorb()
	if err := h.log.EndCheckpoint(&h.ctx, &h.out); err != nil {
		t.Fatal(err)
	}
	h.absorb()
	h.run()
	done := h.eventsOf(disklog.CheckpointDone)
	if len(done) != 1 || done[0].Manifest.Segment != 7 {
		t.Fatalf("checkpoint in the reserve: %+v", h.events)
	}
	if !h.log.Stalled() || slices.ContainsFunc(h.eventsOf(disklog.Durable), func(ev disklog.Event) bool { return slices.Contains(ev.Tickets, held) }) {
		t.Fatalf("the held write went into the reserve: %+v", h.events)
	}

	punch := h.submit(disklog.Cold, disklog.Item{Entry: disklog.PunchEntry(1, testChunk, 7, 0, core.BlocksPerChunk)})
	if len(h.pending) != 0 {
		t.Fatalf("a punch appended to the reserve segment the manifest left a block free in: %v", h.trace)
	}
	h.log.FailStalled(&h.ctx, &h.out)
	h.absorb()
	failed := h.eventsOf(disklog.Failed)
	if len(failed) != 2 || !errors.Is(failed[0].Err, disklog.ErrNoSpace) || !slices.Equal(failed[0].Tickets, []disklog.Ticket{held}) ||
		!errors.Is(failed[1].Err, disklog.ErrNoSpace) || !slices.Equal(failed[1].Tickets, []disklog.Ticket{punch}) {
		t.Fatalf("no victim: %+v", h.events)
	}
	h.wantLive(0)
	h.close()
}

func TestCheckpointWaitsUntilNoVictim(t *testing.T) {
	h, held := reserveDisk(t)
	if err := h.log.BeginCheckpoint([]disklog.CheckpointIndex{{Owner: 5, ReplayLSN: 2}}); err != nil {
		t.Fatal(err)
	}
	var pieces []disklog.Ticket
	for i := range 2 {
		it := h.blob(disklog.BlobRun, 5, 2*core.BlockSize, uint64(i))
		piece, err := h.log.CheckpointPiece(&h.ctx, 5, it.Buf, 2*core.BlockSize, false, &h.out)
		if err != nil {
			t.Fatal(err)
		}
		h.absorb()
		h.run()
		pieces = append(pieces, piece)
	}
	h.wantTrace("write 4+4 io104", "fsync io105", "write 8+4 io106", "fsync io107", "write 12+4 io108", "fsync io109")
	if !h.log.Stalled() || len(h.eventsOf(disklog.Failed)) != 0 {
		t.Fatalf("a piece with no segment free: %+v", h.events)
	}
	h.log.FailStalled(&h.ctx, &h.out)
	h.absorb()
	failed, cpFailed := h.eventsOf(disklog.Failed), h.eventsOf(disklog.CheckpointFailed)
	if len(cpFailed) != 1 || !errors.Is(cpFailed[0].Err, disklog.ErrNoSpace) || len(failed) != 2 ||
		!slices.Equal(failed[0].Tickets, []disklog.Ticket{held}) || !slices.Equal(failed[1].Tickets, pieces[1:]) {
		t.Fatalf("no victim: %+v", h.events)
	}
	if err := h.log.EndCheckpoint(&h.ctx, &h.out); !errors.Is(err, disklog.ErrNoCheckpoint) {
		t.Fatalf("end after failure: %v", err)
	}
	h.wantLive(0)
	h.close()
}

func TestStallEndsWhenASegmentFrees(t *testing.T) {
	cfg := testConfig(0)
	cfg.Reserve = 2
	h := formatted(t, 32*core.BlockSize, cfg)
	h.victim = true
	for i := range 4 {
		h.put(disklog.Hot, h.write(uint32(i), 2, uint64(i)))
	}
	h.put(disklog.Cold, h.write(4, 1, 4))
	held := h.submit(disklog.Hot, h.write(5, 2, 5))
	if !h.log.Stalled() {
		t.Fatal("a write with only the reserve free is not stalled")
	}
	h.submit(disklog.Cold, disklog.Item{Entry: disklog.PunchEntry(1, testChunk, 6, 0, core.BlocksPerChunk)})
	if h.log.InFlight() != 1 {
		t.Fatalf("the punch did not take the round slot: %v", h.trace)
	}
	disklog.FreeSegment(h.log, 1, true)
	disklog.StartRounds(h.log, &h.ctx, &h.out)
	h.absorb()
	if h.log.Stalled() {
		t.Fatal("stalled with a segment free outside the reserve")
	}
	h.log.FailStalled(&h.ctx, &h.out)
	h.absorb()
	h.run()
	d := h.eventsOf(disklog.Durable)
	if len(h.eventsOf(disklog.Failed)) != 0 || !slices.Equal(d[len(d)-1].Tickets, []disklog.Ticket{held}) || d[len(d)-1].Segment != 1 {
		t.Fatalf("events %+v", h.events)
	}
	h.close()
}

func TestReserveSegmentSurvivesARestart(t *testing.T) {
	cfg := testConfig(0)
	cfg.Reserve = 2
	h := formatted(t, 32*core.BlockSize, cfg)
	for i := range 5 {
		h.put(disklog.Hot, h.write(uint32(i), 2, uint64(i)))
	}
	if err := h.log.BeginCheckpoint([]disklog.CheckpointIndex{{Owner: 5, ReplayLSN: 5}}); err != nil {
		t.Fatal(err)
	}
	if err := h.log.EndCheckpoint(&h.ctx, &h.out); err != nil {
		t.Fatal(err)
	}
	h.absorb()
	h.run()
	if done := h.eventsOf(disklog.CheckpointDone); len(done) != 1 || done[0].Manifest.Segment != 6 {
		t.Fatalf("manifest %+v", done)
	}
	h.close()
	h.reopen()
	h.victim = true
	h.trace = nil
	h.submit(disklog.Cold, disklog.Item{Entry: disklog.PunchEntry(1, testChunk, 7, 0, core.BlocksPerChunk)})
	if len(h.trace) != 0 || !h.log.Stalled() {
		t.Fatalf("after a restart a punch appended to the reserve segment: %v", h.trace)
	}
	h.log.FailStalled(&h.ctx, &h.out)
	h.absorb()
	h.close()
}

func TestCloseFailsItemsWaitingForRoom(t *testing.T) {
	h, held := reserveDisk(t)
	if err := h.log.Close(&h.ctx, &h.out); err != nil {
		t.Fatal(err)
	}
	h.absorb()
	if failed := h.eventsOf(disklog.Failed); len(failed) != 1 || !errors.Is(failed[0].Err, disklog.ErrClosed) ||
		!slices.Equal(failed[0].Tickets, []disklog.Ticket{held}) || h.pool.Live() != 0 {
		t.Fatalf("close with a write waiting for room: %+v, %d buffers live", h.events, h.pool.Live())
	}
}

// The checkpoint's pieces lead the cold stream, so while they fill a segment of the reserve an
// ordinary cold item waits behind them even with segments free outside it; once their queue
// empties, the item opens a segment of its own.
func TestColdItemsWaitForTheCheckpointInTheReserve(t *testing.T) {
	cfg := testConfig(0)
	cfg.Reserve = 4093
	h := formatted(t, gib, cfg)
	h.put(disklog.Cold, h.write(0, 62, 0))
	h.put(disklog.Cold, h.write(1, 62, 1))
	if err := h.log.BeginCheckpoint([]disklog.CheckpointIndex{{Owner: 5, ReplayLSN: 2}}); err != nil {
		t.Fatal(err)
	}
	piece := func(seed uint64) disklog.Ticket {
		it := h.blob(disklog.BlobRun, 5, 100, seed)
		tk, err := h.log.CheckpointPiece(&h.ctx, 5, it.Buf, 100, false, &h.out)
		if err != nil {
			t.Fatal(err)
		}
		h.absorb()
		return tk
	}
	first := piece(1)
	disklog.FreeSegment(h.log, 1, true)
	disklog.FreeSegment(h.log, 2, true)
	cold := h.submit(disklog.Cold, h.write(2, 1, 2))
	second := piece(2)
	h.step(0)
	h.step(0)
	if h.log.Queued() != 1 || h.log.Stalled() {
		t.Fatalf("behind the second piece: %d queued, stalled %v", h.log.Queued(), h.log.Stalled())
	}
	h.run()
	d := h.eventsOf(disklog.Durable)
	got := func(i int) (disklog.Ticket, uint32) { return d[len(d)-3+i].Tickets[0], d[len(d)-3+i].Segment }
	for i, want := range []struct {
		tk  disklog.Ticket
		seg uint32
	}{{first, 3}, {second, 3}, {cold, 1}} {
		if tk, seg := got(i); tk != want.tk || seg != want.seg {
			t.Fatalf("round %d is ticket %d in segment %d, want %d in %d: %+v", i, tk, seg, want.tk, want.seg, d)
		}
	}
	h.close()
}
