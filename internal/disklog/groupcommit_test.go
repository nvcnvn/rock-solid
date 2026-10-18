package disklog_test

import (
	"bytes"
	"errors"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

func TestFormatWritesBothSuperblocks(t *testing.T) {
	h := newHarness(t, gib, 1<<20, testConfig(0))
	h.wantTrace("write 0+1 io100", "fsync io101")
	h.step(0)
	h.wantTrace("write 0+1 io100", "fsync io101")
	h.step(0)
	h.wantTrace("write 0+1 io100", "fsync io101", "write 63+1 io102", "fsync io103")
	h.run()
	if len(h.trace) != 4 || len(h.events) != 0 {
		t.Fatalf("not idle after format: %q %+v", h.trace, h.events)
	}
	h.wantLive(0)
	want := disklog.Superblock{LogID: testLogID, Geometry: h.geo}
	for _, lba := range []core.LBA{0, 63} {
		sb, err := disklog.DecodeSuperblock(h.read(lba, 1))
		if err != nil || sb != want {
			t.Fatalf("superblock at LBA %d: %+v, %v", lba, sb, err)
		}
	}
	h.close()
}

func TestFormatRefusesBadSettingsAndASmallPool(t *testing.T) {
	geo, _ := disklog.GeometryFor(gib)
	sb := disklog.Superblock{LogID: testLogID, Geometry: geo}
	var out disklog.Output
	for _, cfg := range []disklog.Config{{MaxRecordBytes: 2 * core.BlockSize, CommitDepth: 1}, {MaxRecordBytes: 1 << 20}} {
		pool := sim.NewPool(1<<20, 0)
		if _, err := disklog.Format(&core.Context{Pool: pool}, cfg, sb, 1, &out); !errors.Is(err, disklog.ErrBadConfig) {
			t.Fatalf("%+v: %v", cfg, err)
		}
	}
	pool := sim.NewPool(100*core.BlockSize, 0)
	if _, err := disklog.Format(&core.Context{Pool: pool}, testConfig(0), sb, 1, &out); !errors.Is(err, core.ErrPoolExhausted) || pool.Live() != 0 {
		t.Fatalf("pool of 100 blocks for 127: %v, %d live", err, pool.Live())
	}
}

func TestOneWriteAndOneFlushPerRound(t *testing.T) {
	h := formatted(t, gib, testConfig(0))
	a := h.submit(disklog.Hot, h.write(0, 1, 1))
	h.wantTrace("write 64+3 io104", "fsync io105")
	b := h.submit(disklog.Hot, h.write(1, 2, 2))
	c := h.submit(disklog.Hot, h.blob(disklog.BlobRaftEntries, 7, 100, 3))
	d := h.submit(disklog.Hot, h.blob(disklog.BlobRaftEntries, 7, 5000, 4))
	e := h.submit(disklog.Hot, disklog.Item{Entry: disklog.PunchEntry(1, testChunk, 5, 0, core.BlocksPerChunk)})
	h.wantTrace("write 64+3 io104", "fsync io105")
	h.run()
	h.wantTrace("write 64+3 io104", "fsync io105", "write 67+5 io106", "fsync io107")
	durable := h.eventsOf(disklog.Durable)
	if len(durable) != 2 || len(h.events) != 2 {
		t.Fatalf("events %+v", h.events)
	}
	if durable[0].LSN != 1 || !slices.Equal(durable[0].Tickets, []disklog.Ticket{a}) {
		t.Fatalf("round 1: %+v", durable[0])
	}
	if durable[1].LSN != 2 || !slices.Equal(durable[1].Tickets, []disklog.Ticket{b, c, d, e}) {
		t.Fatalf("round 2: %+v", durable[1])
	}
	if !slices.Equal(durable[1].Payloads, []uint64{0, 8192, 8292, 13292}) {
		t.Fatalf("round 2 payload offsets %v", durable[1].Payloads)
	}
	h.wantLive(0)

	if sh := h.segmentHeader(1); sh.Seq != 1 || sh.Stream != disklog.Hot || sh.Index != 1 || !sh.Manifest.IsNone() {
		t.Fatalf("segment 1 header %+v", sh)
	}
	r1 := h.record(durable[0])
	if durable[0].Segment != 1 || durable[0].Block != 1 || durable[0].LBA != 65 {
		t.Fatalf("record 1 at %+v", durable[0])
	}
	if !bytes.Equal(r1.Payload(0), pattern(1, core.BlockSize)) {
		t.Fatal("record 1's data is not the submitted bytes")
	}
	r2 := h.record(durable[1])
	if durable[1].Block != 3 || len(r2.Entries) != 4 {
		t.Fatalf("record 2 at %+v with %d entries", durable[1], len(r2.Entries))
	}
	for i, want := range [][]byte{pattern(2, 2*core.BlockSize), pattern(3, 100), pattern(4, 5000), nil} {
		if !bytes.Equal(r2.Payload(i), want) {
			t.Fatalf("record 2 entry %d payload differs", i)
		}
	}
}

func TestStreamsAlternate(t *testing.T) {
	h := formatted(t, gib, testConfig(0))
	a := h.submit(disklog.Hot, h.write(0, 1, 1))
	b := h.submit(disklog.Hot, h.write(1, 1, 2))
	c := h.submit(disklog.Cold, h.blob(disklog.BlobRun, 5, 10, 3))
	h.round()
	h.wantTrace("write 64+3 io104", "fsync io105", "write 128+3 io106", "fsync io107")
	d := h.submit(disklog.Hot, h.write(2, 1, 4))
	h.run()
	h.wantTrace("write 64+3 io104", "fsync io105", "write 128+3 io106", "fsync io107", "write 67+3 io108", "fsync io109")
	durable := h.eventsOf(disklog.Durable)
	want := [][]disklog.Ticket{{a}, {c}, {b, d}}
	for i, ev := range durable {
		if ev.LSN != uint64(i+1) || !slices.Equal(ev.Tickets, want[i]) {
			t.Fatalf("round %d: %+v", i+1, ev)
		}
	}
	if sh := h.segmentHeader(2); sh.Stream != disklog.Cold || sh.Seq != 2 {
		t.Fatalf("segment 2 header %+v", sh)
	}
}

func TestRoundsStopAtTheMaximumRecord(t *testing.T) {
	h := formatted(t, gib, testConfig(3))
	big := h.write(0, 3, 1)
	if _, err := h.log.Submit(&h.ctx, disklog.Hot, big, &h.out); !errors.Is(err, disklog.ErrTooLarge) {
		t.Fatalf("3-block write in 3-block records: %v", err)
	}
	h.pool.Free(big.Buf)
	if h.log.MaxBlobBytes() != core.BlockSize {
		t.Fatalf("largest blob %d", h.log.MaxBlobBytes())
	}
	tooBig := h.blob(disklog.BlobRaftEntries, 1, core.BlockSize+1, 9)
	if _, err := h.log.Submit(&h.ctx, disklog.Hot, tooBig, &h.out); !errors.Is(err, disklog.ErrTooLarge) {
		t.Fatalf("blob past the largest: %v", err)
	}
	h.pool.Free(tooBig.Buf)
	t1 := h.submit(disklog.Hot, h.write(0, 1, 1))
	t2 := h.submit(disklog.Hot, h.write(1, 1, 2))
	t3 := h.submit(disklog.Hot, h.write(2, 1, 3))
	t4 := h.submit(disklog.Hot, h.write(3, 1, 4))
	h.run()
	want := [][]disklog.Ticket{{t1}, {t2, t3}, {t4}}
	durable := h.eventsOf(disklog.Durable)
	if len(durable) != 3 {
		t.Fatalf("events %+v", h.events)
	}
	for i, ev := range durable {
		if !slices.Equal(ev.Tickets, want[i]) {
			t.Fatalf("LSN %d tickets %v, want %v", ev.LSN, ev.Tickets, want[i])
		}
	}
	h.wantLive(0)
}

func TestFullSegmentRollsAndFullDiskFails(t *testing.T) {
	h := formatted(t, 16*core.BlockSize, testConfig(0))
	for i := range 3 {
		h.submit(disklog.Hot, h.write(uint32(i), 2, uint64(i)))
		h.run()
	}
	h.wantTrace("write 4+4 io104", "fsync io105", "write 8+4 io106", "fsync io107", "write 12+4 io108", "fsync io109")
	last := h.submit(disklog.Hot, h.write(9, 1, 9))
	failed := h.eventsOf(disklog.Failed)
	if len(failed) != 1 || !errors.Is(failed[0].Err, disklog.ErrNoSpace) || !slices.Equal(failed[0].Tickets, []disklog.Ticket{last}) {
		t.Fatalf("events %+v", h.events)
	}
	if len(h.pending) != 0 {
		t.Fatalf("disk actions on a full disk: %+v", h.pending)
	}
	h.close()
}

func TestBadExtentIsRefused(t *testing.T) {
	h := formatted(t, gib, testConfig(0))
	for _, e := range []disklog.Entry{
		disklog.WriteEntry(1, testChunk, 1, 0, 0),
		disklog.PunchEntry(1, testChunk, 1, 0, 0),
		disklog.PunchEntry(1, testChunk, 1, 1000, 25),
	} {
		if _, err := h.log.Submit(&h.ctx, disklog.Hot, disklog.Item{Entry: e}, &h.out); !errors.Is(err, disklog.ErrBadExtent) {
			t.Fatalf("%+v: %v", e, err)
		}
	}
	if len(h.out.Actions) != 0 || len(h.out.Events) != 0 {
		t.Fatalf("refusals produced %+v", h.out)
	}
}

func TestDiskErrorFailsTheLog(t *testing.T) {
	h := formatted(t, gib, testConfig(0))
	a := h.submit(disklog.Hot, h.write(0, 1, 1))
	b := h.submit(disklog.Hot, h.write(1, 1, 2))
	c := h.submit(disklog.Cold, h.blob(disklog.BlobRun, 5, 10, 3))
	h.failIO[firstIO+4] = core.DiskIO
	h.run()
	inDoubt, failed := h.eventsOf(disklog.InDoubt), h.eventsOf(disklog.Failed)
	if len(h.events) != 2 || len(inDoubt) != 1 || len(failed) != 1 ||
		!errors.Is(inDoubt[0].Err, core.DiskIO) || !slices.Equal(inDoubt[0].Tickets, []disklog.Ticket{a}) ||
		!errors.Is(failed[0].Err, core.DiskIO) || !slices.Equal(failed[0].Tickets, []disklog.Ticket{b, c}) {
		t.Fatalf("events %+v", h.events)
	}
	h.wantLive(0)
	it := h.write(2, 1, 4)
	if _, err := h.log.Submit(&h.ctx, disklog.Hot, it, &h.out); !errors.Is(err, core.DiskIO) {
		t.Fatalf("submit after failure: %v", err)
	}
	h.pool.Free(it.Buf)
	if !errors.Is(h.log.Err(), core.DiskIO) {
		t.Fatalf("log error %v", h.log.Err())
	}
	h.close()
}

func TestDiskErrorWithRoundsInFlight(t *testing.T) {
	cfg := testConfig(0)
	cfg.CommitDepth = 2
	h := formatted(t, gib, cfg)
	a := h.submit(disklog.Hot, h.write(0, 1, 1))
	b := h.submit(disklog.Cold, h.write(1, 1, 2))
	c := h.submit(disklog.Hot, h.write(2, 1, 3))
	h.failIO[firstIO+6] = core.DiskIO
	h.step(2)
	inDoubt, failed := h.eventsOf(disklog.InDoubt), h.eventsOf(disklog.Failed)
	if len(inDoubt) != 1 || !slices.Equal(inDoubt[0].Tickets, []disklog.Ticket{a, b}) ||
		len(failed) != 1 || !slices.Equal(failed[0].Tickets, []disklog.Ticket{c}) {
		t.Fatalf("events %+v", h.events)
	}
	if err := h.log.Close(&h.ctx); !errors.Is(err, disklog.ErrBusy) {
		t.Fatalf("close with a write in flight: %v", err)
	}
	h.wantLive(1)
	h.run()
	if len(h.events) != 2 {
		t.Fatalf("events after the failure: %+v", h.events)
	}
	h.close()
}

func TestAFlushBeforeItsWriteFailsTheLog(t *testing.T) {
	h := formatted(t, gib, testConfig(0))
	a := h.submit(disklog.Hot, h.write(0, 1, 1))
	h.pending[1].linked = false
	h.step(1)
	inDoubt := h.eventsOf(disklog.InDoubt)
	if len(inDoubt) != 1 || !errors.Is(inDoubt[0].Err, disklog.ErrFlushBeforeWrite) || !slices.Equal(inDoubt[0].Tickets, []disklog.Ticket{a}) {
		t.Fatalf("events %+v", h.events)
	}
	h.wantLive(1)
	h.run()
	h.close()
}

func TestAnErrorOnALaterFlushFailsTheLog(t *testing.T) {
	cfg := testConfig(0)
	cfg.CommitDepth = 2
	h := formatted(t, gib, cfg)
	h.submit(disklog.Hot, h.write(0, 1, 1))
	h.submit(disklog.Cold, h.write(1, 1, 2))
	h.step(0)
	h.step(1)
	h.failIO[firstIO+5] = core.DiskIO
	h.step(1)
	if len(h.eventsOf(disklog.Durable)) != 2 || h.log.Err() != nil {
		t.Fatalf("events %+v", h.events)
	}
	if err := h.log.Close(&h.ctx); !errors.Is(err, disklog.ErrBusy) {
		t.Fatalf("close with a flush outstanding: %v", err)
	}
	h.step(0)
	if !errors.Is(h.log.Err(), core.DiskIO) {
		t.Fatalf("a failed earlier flush left the log at %v", h.log.Err())
	}
	h.close()
}

func TestAFullPoolNeverStallsARound(t *testing.T) {
	const own = 64 + 63
	h := newHarness(t, gib, (own+8)*core.BlockSize, testConfig(0))
	h.run()
	var tickets []disklog.Ticket
	for i := range 8 {
		tickets = append(tickets, h.submit(disklog.Hot, h.write(uint32(i), 1, uint64(i))))
	}
	if h.pool.Available() != 0 {
		t.Fatalf("%d bytes left in the pool", h.pool.Available())
	}
	h.run()
	var durable []disklog.Ticket
	for _, ev := range h.eventsOf(disklog.Durable) {
		durable = append(durable, ev.Tickets...)
	}
	if !slices.Equal(durable, tickets) {
		t.Fatalf("durable %v, want %v", durable, tickets)
	}
	h.close()
}

func TestCommitDepthWaitsForEarlierWritesBeforeAFlush(t *testing.T) {
	cfg := testConfig(0)
	cfg.CommitDepth = 2
	h := formatted(t, gib, cfg)
	a := h.submit(disklog.Hot, h.write(0, 1, 1))
	b := h.submit(disklog.Cold, h.write(1, 1, 2))
	h.wantTrace("write 64+3 io104", "fsync io105", "write 128+3 io106")
	h.step(2)
	h.wantTrace("write 64+3 io104", "fsync io105", "write 128+3 io106")
	h.step(0)
	h.wantTrace("write 64+3 io104", "fsync io105", "write 128+3 io106", "fsync io107")
	h.step(1)
	durable := h.eventsOf(disklog.Durable)
	if len(durable) != 2 || !slices.Equal(durable[0].Tickets, []disklog.Ticket{a}) || !slices.Equal(durable[1].Tickets, []disklog.Ticket{b}) {
		t.Fatalf("a flush after both writes makes both rounds durable: %+v", h.events)
	}
	h.run()
	if len(h.events) != 2 || h.writes != h.fsyncs {
		t.Fatalf("events %+v, %d writes and %d flushes", h.events, h.writes, h.fsyncs)
	}
	h.close()
}

// Seed 1 of TestEveryAcceptedItemDecodes once flushed a round as soon as the rounds before
// it were written, before its own write completed, so the flush covered nothing of it.
func TestAFlushSentLaterWaitsForItsOwnWrite(t *testing.T) {
	cfg := testConfig(0)
	cfg.CommitDepth = 2
	h := formatted(t, gib, cfg)
	h.submit(disklog.Hot, h.write(0, 1, 1))
	h.submit(disklog.Cold, h.write(1, 1, 2))
	h.step(0)
	h.wantTrace("write 64+3 io104", "fsync io105", "write 128+3 io106")
	h.step(1)
	h.wantTrace("write 64+3 io104", "fsync io105", "write 128+3 io106", "fsync io107")
	h.run()
	if len(h.eventsOf(disklog.Durable)) != 2 {
		t.Fatalf("events %+v", h.events)
	}
}

func TestARoundAppendsOnlyOnceItsSegmentsOpeningRoundIsDurable(t *testing.T) {
	cfg := testConfig(0)
	cfg.CommitDepth = 2
	h := formatted(t, gib, cfg)
	h.submit(disklog.Hot, h.chunkWrite(0, 0, 1, 1))
	h.submit(disklog.Hot, h.chunkWrite(1, 0, 1, 2))
	h.wantTrace("write 64+3 io104", "fsync io105")
	h.submit(disklog.Cold, h.chunkWrite(2, 0, 1, 3))
	h.wantTrace("write 64+3 io104", "fsync io105", "write 128+3 io106")
	h.round()
	h.wantTrace("write 64+3 io104", "fsync io105", "write 128+3 io106", "write 67+2 io108")
	h.run()
	if d := h.eventsOf(disklog.Durable); len(d) != 3 {
		t.Fatalf("events %+v", h.events)
	}
	h.close()
}

func TestCheckpointEndToEnd(t *testing.T) {
	cfg := testConfig(0)
	cfg.MaxReplayBytes = 8 * core.BlockSize
	h := formatted(t, gib, cfg)
	h.submit(disklog.Hot, h.write(0, 3, 1))
	h.run()
	if h.log.CheckpointDue() {
		t.Fatal("due after 4 blocks")
	}
	h.submit(disklog.Hot, h.write(1, 3, 2))
	h.run()
	if !h.log.CheckpointDue() {
		t.Fatal("not due after 8 blocks")
	}
	third := h.submit(disklog.Hot, h.write(2, 3, 3))
	indexes := []disklog.CheckpointIndex{{Owner: 5, ReplayLSN: 2}, {Owner: 6, ReplayLSN: 2}}
	if err := h.log.BeginCheckpoint(indexes); err != nil {
		t.Fatal(err)
	}
	if err := h.log.BeginCheckpoint(indexes); !errors.Is(err, disklog.ErrBusy) {
		t.Fatalf("second begin: %v", err)
	}
	if h.log.CheckpointDue() {
		t.Fatal("due while a checkpoint runs")
	}
	var pieces []disklog.Ticket
	for i := range 2 {
		it := h.blob(disklog.BlobRun, 5, 100, uint64(10+i))
		p, err := h.log.CheckpointPiece(&h.ctx, 5, it.Buf, 100, true, &h.out)
		if err != nil {
			t.Fatal(err)
		}
		h.absorb()
		pieces = append(pieces, p)
	}
	if err := h.log.EndCheckpoint(&h.ctx, &h.out); err != nil {
		t.Fatal(err)
	}
	h.absorb()
	h.run()

	durable := h.eventsOf(disklog.Durable)[2:]
	if len(durable) != 3 {
		t.Fatalf("events %+v", h.events)
	}
	if durable[0].LSN != 3 || !slices.Equal(durable[0].Tickets, []disklog.Ticket{third}) {
		t.Fatalf("LSN 3: %+v", durable[0])
	}
	if durable[1].LSN != 4 || durable[1].Segment != 2 || durable[1].Block != 1 || !slices.Equal(durable[1].Tickets, pieces) {
		t.Fatalf("LSN 4: %+v", durable[1])
	}
	done := h.eventsOf(disklog.CheckpointDone)
	wantPos := disklog.ManifestPos{Segment: 2, Block: durable[2].Block, LSN: 5}
	if durable[2].LSN != 5 || len(done) != 1 || done[0].Manifest != wantPos || h.log.Manifest() != wantPos {
		t.Fatalf("manifest: %+v, done %+v, newest %+v", durable[2], done, h.log.Manifest())
	}
	got := h.readManifest(durable[2])
	want := disklog.Manifest{
		BaseLSN: 2,
		Indexes: []disklog.ManifestIndex{
			{Owner: 5, ReplayLSN: 2, Runs: []disklog.BlobRef{{Segment: 2, Block: 1, LSN: 4, Entry: 0}, {Segment: 2, Block: 1, LSN: 4, Entry: 1}}},
			{Owner: 6, ReplayLSN: 2},
		},
	}
	if !manifestsEqual(got, want) {
		t.Fatalf("manifest\n got %+v\nwant %+v", got, want)
	}
	if h.log.ReplayBytes() != 8*core.BlockSize {
		t.Fatalf("%d bytes to replay after the manifest's base, want the 8 record blocks of LSNs 3 to 5", h.log.ReplayBytes())
	}
	h.wantLive(0)

	for i := range 2 {
		h.submit(disklog.Hot, h.write(uint32(10+i), 40, uint64(20+i)))
		h.run()
	}
	if sh := h.segmentHeader(3); sh.Stream != disklog.Hot || sh.Manifest != wantPos {
		t.Fatalf("segment 3 header %+v, want manifest %+v", sh, wantPos)
	}
}

func TestManifestLargerThanARecordGoesInPieces(t *testing.T) {
	h := formatted(t, gib, testConfig(3))
	const runs = 500
	if err := h.log.BeginCheckpoint([]disklog.CheckpointIndex{{Owner: 5}}); err != nil {
		t.Fatal(err)
	}
	for i := range runs {
		it := h.blob(disklog.BlobRun, 5, 1, uint64(i))
		if _, err := h.log.CheckpointPiece(&h.ctx, 5, it.Buf, 1, true, &h.out); err != nil {
			t.Fatal(err)
		}
		h.absorb()
	}
	if err := h.log.EndCheckpoint(&h.ctx, &h.out); err != nil {
		t.Fatal(err)
	}
	h.absorb()
	h.run()
	done := h.eventsOf(disklog.CheckpointDone)
	if len(done) != 1 {
		t.Fatalf("events %+v", h.events)
	}
	var last disklog.Event
	for _, ev := range h.eventsOf(disklog.Durable) {
		if ev.LSN == done[0].Manifest.LSN {
			last = ev
		}
	}
	d := h.record(last)
	earlier, _, err := disklog.DecodeManifestPiece(d.Payload(0), nil)
	if err != nil || len(earlier) == 0 {
		t.Fatalf("last piece names %d earlier pieces: %v", len(earlier), err)
	}
	m := h.readManifest(last)
	if len(m.Indexes) != 1 || len(m.Indexes[0].Runs) != runs {
		t.Fatalf("manifest holds %+v", m.Indexes)
	}
	for _, ev := range h.eventsOf(disklog.Durable) {
		for i, e := range ev.Entries {
			if e.BlobKind == disklog.BlobManifest || e.BlobKind == disklog.BlobManifestPiece {
				if ev.Tickets[i] != disklog.NoTicket {
					t.Fatalf("manifest piece reported with ticket %d", ev.Tickets[i])
				}
			}
		}
	}
	h.submit(disklog.Hot, h.write(0, 1, 1))
	h.run()
	if sh := h.segmentHeader(last.Segment + 1); sh.Stream != disklog.Hot || sh.Manifest != done[0].Manifest {
		t.Fatalf("next segment header %+v, want manifest %+v", sh, done[0].Manifest)
	}
	h.close()
}

func TestCheckpointOnAFullDiskFails(t *testing.T) {
	h := formatted(t, 16*core.BlockSize, testConfig(0))
	for i := range 3 {
		h.submit(disklog.Hot, h.write(uint32(i), 2, uint64(i)))
		h.run()
	}
	if err := h.log.BeginCheckpoint([]disklog.CheckpointIndex{{Owner: 5, ReplayLSN: 3}}); err != nil {
		t.Fatal(err)
	}
	it := h.blob(disklog.BlobRun, 5, 10, 1)
	piece, err := h.log.CheckpointPiece(&h.ctx, 5, it.Buf, 10, true, &h.out)
	if err != nil {
		t.Fatal(err)
	}
	h.absorb()
	failed, cpFailed := h.eventsOf(disklog.Failed), h.eventsOf(disklog.CheckpointFailed)
	if len(failed) != 1 || !slices.Equal(failed[0].Tickets, []disklog.Ticket{piece}) || !errors.Is(failed[0].Err, disklog.ErrNoSpace) ||
		len(cpFailed) != 1 || !errors.Is(cpFailed[0].Err, disklog.ErrNoSpace) {
		t.Fatalf("events %+v", h.events)
	}
	if err := h.log.EndCheckpoint(&h.ctx, &h.out); !errors.Is(err, disklog.ErrNoCheckpoint) {
		t.Fatalf("end after failure: %v", err)
	}
	if err := h.log.BeginCheckpoint([]disklog.CheckpointIndex{{Owner: 5, ReplayLSN: 3}}); err != nil {
		t.Fatalf("begin after failure: %v", err)
	}
	h.close()
}

func TestBeginCheckpointRefusesDuplicateOwnersAndLateReplay(t *testing.T) {
	h := formatted(t, gib, testConfig(0))
	for _, ixs := range [][]disklog.CheckpointIndex{{{Owner: 5}, {Owner: 5}}, {{Owner: 5, ReplayLSN: 1}}} {
		if err := h.log.BeginCheckpoint(ixs); !errors.Is(err, disklog.ErrMalformed) {
			t.Fatalf("%+v: %v", ixs, err)
		}
	}
}

func TestSubmitRefusesBadBuffersAndReservedKinds(t *testing.T) {
	h := formatted(t, gib, testConfig(0))
	two := h.alloc(2 * core.BlockSize)
	for _, it := range []disklog.Item{
		{Entry: disklog.WriteEntry(1, testChunk, 1, 0, 1), Buf: two},
		{Entry: disklog.WriteEntry(1, testChunk, 1, 0, 1)},
		{Entry: disklog.BlobEntry(disklog.BlobRun, 1, 3*core.BlockSize), Buf: two},
		{Entry: disklog.PunchEntry(1, testChunk, 1, 0, 1), Buf: two},
	} {
		if _, err := h.log.Submit(&h.ctx, disklog.Hot, it, &h.out); !errors.Is(err, disklog.ErrBadBuffer) {
			t.Fatalf("%+v: %v", it.Entry, err)
		}
	}
	for _, k := range []disklog.BlobKind{disklog.BlobManifest, disklog.BlobManifestPiece} {
		it := disklog.Item{Entry: disklog.BlobEntry(k, 0, 10), Buf: two}
		if _, err := h.log.Submit(&h.ctx, disklog.Cold, it, &h.out); !errors.Is(err, disklog.ErrReservedKind) {
			t.Fatalf("blob kind %d: %v", k, err)
		}
	}
	h.pool.Free(two)
	h.close()
}

// readManifest reads the manifest whose last piece is the blob of a Durable record,
// following its references to the earlier pieces.
func (h *harness) readManifest(last disklog.Event) disklog.Manifest {
	h.t.Helper()
	d := h.record(last)
	i := slices.IndexFunc(d.Entries, func(e disklog.Entry) bool { return e.BlobKind == disklog.BlobManifest })
	if i < 0 {
		h.t.Fatalf("LSN %d holds no manifest", last.LSN)
	}
	earlier, rest, err := disklog.DecodeManifestPiece(d.Payload(i), nil)
	if err != nil {
		h.t.Fatal(err)
	}
	var body []byte
	for _, ref := range earlier {
		ev := disklog.Event{LSN: ref.LSN, Segment: ref.Segment, Block: ref.Block, LBA: h.geo.LBA(ref.Segment, ref.Block)}
		p := h.record(ev)
		if p.Entries[ref.Entry].BlobKind != disklog.BlobManifestPiece {
			h.t.Fatalf("reference %+v names a %+v", ref, p.Entries[ref.Entry])
		}
		body = append(body, p.Payload(int(ref.Entry))...)
	}
	m, err := disklog.DecodeManifest(append(body, rest...))
	if err != nil {
		h.t.Fatal(err)
	}
	return m
}

func manifestsEqual(a, b disklog.Manifest) bool {
	return a.BaseLSN == b.BaseLSN &&
		slices.EqualFunc(a.Indexes, b.Indexes, func(x, y disklog.ManifestIndex) bool {
			return x.Owner == y.Owner && x.ReplayLSN == y.ReplayLSN && slices.Equal(x.Runs, y.Runs)
		})
}
