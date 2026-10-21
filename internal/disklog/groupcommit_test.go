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

func TestNoBlockIsCopied(t *testing.T) {
	h := formatted(t, gib, testConfig(0))
	first := h.write(0, 1, 1)
	h.submit(disklog.Hot, first)
	write := h.write(1, 2, 2)
	h.submit(disklog.Hot, write)
	small := h.blob(disklog.BlobRaftEntries, 7, 100, 3)
	h.submit(disklog.Hot, small)
	large := h.blob(disklog.BlobRaftEntries, 7, 5000, 4)
	h.submit(disklog.Hot, large)
	h.submit(disklog.Hot, disklog.Item{Entry: disklog.PunchEntry(1, testChunk, 5, 0, core.BlocksPerChunk)})
	if vec := h.pending[0].act.Vec; len(vec) != 2 || vec[1] != (core.IOVec{Buf: first.Buf, Len: core.BlockSize}) {
		t.Fatalf("round 1 writes %+v, want its header and the producer's buffer %+v", vec, first.Buf)
	}
	h.round()
	h.wantTrace("write 64+3 io104", "fsync io105", "write 67+5 io106", "fsync io107")
	vec := h.pending[0].act.Vec
	if len(vec) != 3 || vec[0].Len != core.BlockSize || vec[1] != (core.IOVec{Buf: write.Buf, Len: 2 * core.BlockSize}) || vec[2].Len != 2*core.BlockSize {
		t.Fatalf("round 2 writes %+v, want a header block, the producer's buffer %+v and two blob blocks", vec, write.Buf)
	}
	header, packed := vec[0].Buf, vec[2].Buf
	for _, own := range []core.IOBuf{header, packed} {
		if own == first.Buf || own == write.Buf || own == small.Buf || own == large.Buf {
			t.Fatalf("round 2 writes producer buffer %+v as the log's own", own)
		}
	}
	if header == packed {
		t.Fatal("round 2 writes its header and its blobs from one buffer")
	}
	blobs := h.pool.Bytes(packed)[:2*core.BlockSize]
	if want := append(pattern(3, 100), pattern(4, 5000)...); !bytes.Equal(blobs[:len(want)], want) || !isZeroBlock(blobs[len(want):]) {
		t.Fatal("the blob buffer does not hold the two blobs packed back to back")
	}
	h.run()
	h.wantLive(0)
	h.close()
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

func TestNoSpaceFailsTheStreamsWholeQueue(t *testing.T) {
	h := formatted(t, 16*core.BlockSize, testConfig(0))
	h.victim = true
	for i := range 2 {
		h.put(disklog.Hot, h.write(uint32(i), 2, uint64(i)))
	}
	last := h.submit(disklog.Hot, h.write(2, 1, 2))
	head := h.submit(disklog.Hot, h.write(3, 1, 3))
	fits := h.submit(disklog.Hot, disklog.Item{Entry: disklog.PunchEntry(1, testChunk, 4, 0, core.BlocksPerChunk)})
	behind := h.submit(disklog.Hot, h.write(4, 1, 4))
	h.wantTrace("write 4+4 io104", "fsync io105", "write 8+4 io106", "fsync io107", "write 12+3 io108", "fsync io109")
	h.run()
	h.log.FailStalled(&h.ctx, &h.out)
	h.absorb()
	h.wantTrace("write 4+4 io104", "fsync io105", "write 8+4 io106", "fsync io107", "write 12+3 io108", "fsync io109")
	durable, failed := h.eventsOf(disklog.Durable), h.eventsOf(disklog.Failed)
	if len(durable) != 3 || !slices.Equal(durable[2].Tickets, []disklog.Ticket{last}) {
		t.Fatalf("durable %+v", durable)
	}
	if len(failed) != 1 || !errors.Is(failed[0].Err, disklog.ErrNoSpace) || !slices.Equal(failed[0].Tickets, []disklog.Ticket{head, fits, behind}) {
		t.Fatalf("a 2-block head with a 1-block punch behind it and one block left: events %+v", h.events)
	}
	h.wantLive(0)
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
	c := h.submit(disklog.Cold, h.blob(disklog.BlobRaftEntries, 5, 10, 3))
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
	if err := h.log.Close(&h.ctx, &h.out); !errors.Is(err, disklog.ErrBusy) {
		t.Fatalf("close with a write in flight: %v", err)
	}
	h.wantLive(1)
	h.run()
	if len(h.events) != 2 {
		t.Fatalf("events after the failure: %+v", h.events)
	}
	h.close()
}

// A kept write's buffer stays with its submitter once the item is Durable or Failed, and the log
// frees it for an item in doubt once its write completes.
func TestKeptBuffers(t *testing.T) {
	h := formatted(t, gib, testConfig(0))
	kept := h.write(0, 1, 1)
	kept.Keep = true
	h.submit(disklog.Hot, kept)
	h.run()
	h.wantLive(1)
	h.pool.Free(kept.Buf)
	h.wantLive(0)
	h.close()

	cfg := testConfig(0)
	cfg.CommitDepth = 2
	h = formatted(t, gib, cfg)
	var items [3]disklog.Item
	for i, stream := range []disklog.Stream{disklog.Hot, disklog.Cold, disklog.Hot} {
		items[i] = h.write(uint32(i), 1, uint64(i+1))
		items[i].Keep = true
		h.submit(stream, items[i])
	}
	h.failIO[firstIO+6] = core.DiskIO
	h.step(2)
	if len(h.eventsOf(disklog.InDoubt)) != 1 || len(h.eventsOf(disklog.Failed)) != 1 {
		t.Fatalf("events %+v", h.events)
	}
	h.run()
	h.wantLive(1)
	h.pool.Free(items[2].Buf)
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
	if err := h.log.Close(&h.ctx, &h.out); !errors.Is(err, disklog.ErrBusy) {
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
	if sh := h.segmentHeader(2); sh.Stream != disklog.Meta || sh.Lane != 0 {
		t.Fatalf("the checkpoint's segment 2 header %+v, want the metadata stream's lane 0", sh)
	}
	done := h.eventsOf(disklog.CheckpointDone)
	wantPos := disklog.ManifestPos{Segment: 2, Block: durable[2].Block, LSN: 5}
	if durable[2].LSN != 5 || len(done) != 1 || done[0].Manifest != wantPos || h.log.Manifest() != wantPos {
		t.Fatalf("manifest: %+v, done %+v, newest %+v", durable[2], done, h.log.Manifest())
	}
	got := h.readManifest(durable[2])
	want := disklog.Manifest{
		BaseLSN: 2,
		NextSeq: 2,
		Indexes: []disklog.ManifestIndex{
			{Owner: 5, ReplayLSN: 2, Runs: []disklog.BlobRef{{Segment: 2, Block: 1, LSN: 4, Entry: 0}, {Segment: 2, Block: 1, LSN: 4, Entry: 1}}},
			{Owner: 6, ReplayLSN: 2},
		},
		Segments: []disklog.SegmentUsage{{Segment: 1, State: disklog.SegmentUsed, Live: 6, Newest: 2}},
	}
	if !manifestsEqual(got, want) {
		t.Fatalf("manifest\n got %+v\nwant %+v", got, want)
	}
	if h.log.ReplayBytes() != 8*core.BlockSize {
		t.Fatalf("%d bytes to replay after the manifest's base, want the 8 record blocks of LSNs 3 to 5", h.log.ReplayBytes())
	}
	h.wantLive(0)

	h.submit(disklog.Hot, h.write(10, 60, 20))
	h.run()
	if sh := h.segmentHeader(3); sh.Stream != disklog.Hot || sh.Manifest != wantPos {
		t.Fatalf("segment 3 header %+v, want manifest %+v", sh, wantPos)
	}
}

func TestCheckpointCarriesDurableRuns(t *testing.T) {
	h := formatted(t, gib, testConfig(0))
	h.submit(disklog.Hot, h.write(0, 1, 1))
	h.run()
	carried := disklog.BlobRef{Segment: 1, Block: 1, LSN: 1, Entry: 0}
	for _, bad := range []disklog.BlobRef{{Block: 1, LSN: 1}, {Segment: 1, Block: 1, LSN: 2}} {
		if err := h.log.BeginCheckpoint([]disklog.CheckpointIndex{{Owner: 5, Runs: []disklog.BlobRef{bad}}}); !errors.Is(err, disklog.ErrMalformed) {
			t.Fatalf("carrying %+v: %v", bad, err)
		}
	}
	runs := []disklog.BlobRef{carried}
	if err := h.log.BeginCheckpoint([]disklog.CheckpointIndex{{Owner: 5, ReplayLSN: 1, Runs: runs}}); err != nil {
		t.Fatal(err)
	}
	runs[0].Block = 9
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
	durable := h.eventsOf(disklog.Durable)
	m := h.readManifest(durable[len(durable)-1])
	want := []disklog.BlobRef{carried, {Segment: 2, Block: 1, LSN: 2, Entry: 0}}
	if len(m.Indexes) != 1 || !slices.Equal(m.Indexes[0].Runs, want) {
		t.Fatalf("manifest indexes %+v, want runs %+v", m.Indexes, want)
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
	lists, _, err := disklog.DecodeManifestPiece(d.Payload(0))
	if err != nil || len(lists) != 1 || len(lists[0]) == 0 {
		t.Fatalf("last piece lists %v: %v", lists, err)
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
	h.put(disklog.Hot, h.write(0, 1, 1))
	run := disklog.BlobRef{Segment: 1, Block: 1, LSN: 1}
	for _, ixs := range [][]disklog.CheckpointIndex{{{Owner: 5}, {Owner: 5}}, {{Owner: 5, ReplayLSN: 2}}, {{Owner: 5, Runs: []disklog.BlobRef{run, run}}}} {
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
		{Entry: disklog.BlobEntry(disklog.BlobRaftEntries, 1, 3*core.BlockSize), Buf: two},
		{Entry: disklog.PunchEntry(1, testChunk, 1, 0, 1), Buf: two},
	} {
		if _, err := h.log.Submit(&h.ctx, disklog.Hot, it, &h.out); !errors.Is(err, disklog.ErrBadBuffer) {
			t.Fatalf("%+v: %v", it.Entry, err)
		}
	}
	for _, k := range []disklog.BlobKind{disklog.BlobRun, disklog.BlobManifest, disklog.BlobManifestPiece} {
		for _, s := range []disklog.Stream{disklog.Hot, disklog.Cold} {
			it := disklog.Item{Entry: disklog.BlobEntry(k, 5, 10), Buf: two}
			if _, err := h.log.Submit(&h.ctx, s, it, &h.out); !errors.Is(err, disklog.ErrReservedKind) {
				t.Fatalf("blob kind %d on stream %d: %v", k, s, err)
			}
		}
	}
	h.wantLive(1)
	if len(h.pending) != 0 || h.log.Queued() != 0 {
		t.Fatalf("a refused item went to the disk: %v, %d queued", h.trace, h.log.Queued())
	}
	if err := h.log.BeginCheckpoint([]disklog.CheckpointIndex{{Owner: 5}}); err != nil {
		t.Fatal(err)
	}
	piece, err := h.log.CheckpointPiece(&h.ctx, 5, two, 10, true, &h.out)
	if err != nil {
		t.Fatalf("the same run piece through CheckpointPiece: %v", err)
	}
	h.absorb()
	if err := h.log.EndCheckpoint(&h.ctx, &h.out); err != nil {
		t.Fatal(err)
	}
	h.absorb()
	h.run()
	if done := h.eventsOf(disklog.CheckpointDone); len(done) != 1 || !slices.Contains(h.eventsOf(disklog.Durable)[0].Tickets, piece) {
		t.Fatalf("checkpoint with the piece: events %+v", h.events)
	}
	h.close()
}

func TestCloseIsBusyUntilFormatFinishes(t *testing.T) {
	h := newHarness(t, gib, 64<<20, testConfig(0))
	for len(h.pending) > 0 {
		if err := h.log.Close(&h.ctx, &h.out); !errors.Is(err, disklog.ErrBusy) {
			t.Fatalf("close with %v left of format: %v", h.trace[len(h.trace)-len(h.pending):], err)
		}
		h.wantLive(0)
		h.step(0)
	}
	if h.log.Err() != nil {
		t.Fatal(h.log.Err())
	}
	h.close()
}

// readManifest reads the manifest whose last piece is the blob of a Durable record: the
// snapshot and edits its list names, applied in order.
func (h *harness) readManifest(last disklog.Event) disklog.Manifest {
	h.t.Helper()
	m, err := disklog.LoadManifest(h.readChain(last).bodies)
	if err != nil {
		h.t.Fatalf("manifest at LSN %d: %v", last.LSN, err)
	}
	return m
}

// chain is a manifest as its last piece lists it: the body of the snapshot and of each edit
// since, the bytes each one's blobs hold, and where they are.
type chain struct {
	bodies [][]byte
	sizes  []uint64
	pieces [][]disklog.BlobRef
}

func (h *harness) blobAt(ref disklog.BlobRef) (disklog.BlobKind, []byte) {
	h.t.Helper()
	d := h.record(disklog.Event{LSN: ref.LSN, Segment: ref.Segment, Block: ref.Block, LBA: h.geo.LBA(ref.Segment, ref.Block)})
	return d.Entries[ref.Entry].BlobKind, d.Payload(int(ref.Entry))
}

func (h *harness) readChain(last disklog.Event) chain {
	h.t.Helper()
	d := h.record(last)
	i := slices.IndexFunc(d.Entries, func(e disklog.Entry) bool { return e.BlobKind == disklog.BlobManifest })
	if i < 0 {
		h.t.Fatalf("LSN %d holds no manifest", last.LSN)
	}
	lists, rest, err := disklog.DecodeManifestPiece(d.Payload(i))
	if err != nil {
		h.t.Fatal(err)
	}
	c := chain{bodies: make([][]byte, len(lists)), sizes: make([]uint64, len(lists)), pieces: lists}
	for m, list := range lists {
		for j, ref := range list {
			kind, b := h.blobAt(ref)
			c.sizes[m] += uint64(len(b))
			if m < len(lists)-1 && j == len(list)-1 {
				if kind != disklog.BlobManifest {
					h.t.Fatalf("manifest %d ends in %+v, a blob of kind %d", m, ref, kind)
				}
				if _, b, err = disklog.DecodeManifestPiece(b); err != nil {
					h.t.Fatal(err)
				}
			} else if kind != disklog.BlobManifestPiece {
				h.t.Fatalf("reference %+v names a blob of kind %d", ref, kind)
			}
			c.bodies[m] = append(c.bodies[m], b...)
		}
	}
	n := len(lists) - 1
	c.bodies[n] = append(c.bodies[n], rest...)
	c.sizes[n] += uint64(len(d.Payload(i)))
	return c
}

func boundsEqual(a, b []disklog.MergeBound) bool {
	return slices.EqualFunc(a, b, func(p, q disklog.MergeBound) bool {
		return p.Run == q.Run && p.Dead == q.Dead && bytes.Equal(p.Key, q.Key)
	})
}

func manifestsEqual(a, b disklog.Manifest) bool {
	return a.BaseLSN == b.BaseLSN && a.NextSeq == b.NextSeq &&
		slices.EqualFunc(a.Indexes, b.Indexes, func(x, y disklog.ManifestIndex) bool {
			return x.Owner == y.Owner && x.ReplayLSN == y.ReplayLSN && slices.Equal(x.Runs, y.Runs) && boundsEqual(x.Bounds, y.Bounds)
		}) &&
		slices.Equal(a.Segments, b.Segments)
}

func TestEachRecordNamesTheDurableLSNOfItsRound(t *testing.T) {
	cfg := testConfig(0)
	cfg.CommitDepth = 2
	h := formatted(t, gib, cfg)
	h.put(disklog.Hot, h.chunkWrite(0, 0, 1, 1))
	h.put(disklog.Hot, h.chunkWrite(1, 0, 1, 2))
	h.submit(disklog.Hot, h.chunkWrite(2, 0, 1, 3))
	h.submit(disklog.Hot, h.chunkWrite(3, 0, 1, 4))
	h.run()
	durable := h.eventsOf(disklog.Durable)
	want := []uint64{0, 1, 2, 2}
	if len(durable) != len(want) {
		t.Fatalf("%d records durable, want %d", len(durable), len(want))
	}
	for i, ev := range durable {
		if got := h.record(ev).Record.Durable; got != want[i] {
			t.Fatalf("LSN %d names %d durable, want %d", ev.LSN, got, want[i])
		}
	}
}
