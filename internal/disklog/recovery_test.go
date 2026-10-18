package disklog_test

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

func (h *harness) put(s disklog.Stream, it disklog.Item) {
	h.t.Helper()
	h.submit(s, it)
	h.run()
}

func (h *harness) wantLSNs(res *recovered, want []uint64) {
	h.t.Helper()
	if got := res.lsns(); !slices.Equal(got, want) {
		h.t.Fatalf("replayed LSNs %v, want %v", got, want)
	}
	if n := uint64(len(want)); n > 0 && res.durable != want[n-1] {
		h.t.Fatalf("durable LSN %d, want %d", res.durable, want[n-1])
	}
}

func wantCorrupt(t testing.TB, res *recovered, lsn uint64) {
	t.Helper()
	var ce *disklog.CorruptError
	if !errors.As(res.err, &ce) || ce.LSN != lsn {
		t.Fatalf("recovery: %v, want Corrupt(LSN %d)", res.err, lsn)
	}
}

// reformat formats the harness's disk again as another log, as reusing a disk does.
func (h *harness) reformat(logID uint64) {
	h.t.Helper()
	h.resetIO()
	h.logID = logID
	var err error
	h.log, err = disklog.Format(&h.ctx, h.cfg, disklog.Superblock{LogID: logID, Geometry: h.geo}, firstIO, &h.out)
	if err != nil {
		h.t.Fatal(err)
	}
	h.own = h.pool.Live()
	h.absorb()
	h.run()
	h.trace = nil
}

// twoBlockWrites writes n 2-block records, each one round, to chunks 0.. with versions 1...
func (h *harness) twoBlockWrites(n int) {
	h.t.Helper()
	for i := range n {
		h.put(disklog.Hot, h.chunkWrite(uint32(i), 0, 2, uint64(i+1)))
	}
	h.trace = nil
}

func TestRecoverEmptyLog(t *testing.T) {
	h := formatted(t, gib, testConfig(0))
	res := h.reopen()
	if len(res.replayed) != 0 || res.durable != 0 || !res.manifest.IsNone() || len(res.index) != 0 {
		t.Fatalf("empty log recovered %+v", res)
	}
	h.put(disklog.Hot, h.chunkWrite(0, 0, 1, 1))
	if d := h.eventsOf(disklog.Durable); len(d) != 1 || d[0].LSN != 1 {
		t.Fatalf("first write after recovery: %+v", d)
	}
	h.close()
}

func TestRecoverAllZeroDiskIsNotFormatted(t *testing.T) {
	h := blankHarness(t, gib, 1<<20, testConfig(0))
	if res := h.recover(disklog.ModeOpen); !errors.Is(res.err, disklog.ErrNotFormatted) {
		t.Fatalf("all-zero disk: %v", res.err)
	}
}

func TestRecoverReformattedDisk(t *testing.T) {
	h := formatted(t, gib, testConfig(0))
	h.logID = 3
	h.twoBlockWrites(6)
	h.put(disklog.Cold, h.blob(disklog.BlobRun, 9, 100, 1))
	h.reformat(4)
	n := newNode(h, 0)
	h.put(disklog.Hot, h.chunkWrite(0, 0, 1, 1))
	res := h.reopen()
	h.wantLSNs(res, []uint64{1})
	if h.rec != nil || h.log.Superblock().LogID != 4 {
		t.Fatalf("recovered log %+v", h.log.Superblock())
	}
	if free := disklog.FreeSegments(h.log); free != int(h.geo.SegmentCount)-2 {
		t.Fatalf("%d of %d segments free, want all but segment 0 and the new log's", free, h.geo.SegmentCount-1)
	}
	h.wantModel("index", res.index, n.live)
	h.readBack(res.index)
}

func TestRecoverTornTail(t *testing.T) {
	for _, landed := range [][]core.LBA{{}, {74}, {74, 75}, {75, 76}, {74, 76}, {74, 75, 76}} {
		h := formatted(t, gib, testConfig(0))
		n := newNode(h, 0)
		h.twoBlockWrites(3)
		before := n.live.clone()
		h.submit(disklog.Hot, h.chunkWrite(3, 0, 2, 4))
		h.wantTrace("write 74+3 io110", "fsync io111")
		h.step(0)
		h.crashLanding(landed...)
		res := h.reopen()
		if len(landed) == 3 {
			h.wantLSNs(res, lsnRange(1, 4))
			if got := res.replayed[3].Entries; !slices.Equal(got, []disklog.Entry{disklog.WriteEntry(1, chunkOf(3), 4, 0, 2)}) {
				t.Fatalf("kept unflushed record holds %+v", got)
			}
			before.apply(res.replayed[3])
		} else {
			h.wantLSNs(res, lsnRange(1, 3))
		}
		h.wantModel("index", res.index, before)
		h.readBack(res.index)
	}
}

func xorBlock(h *harness, lba core.LBA, mask byte) {
	b := h.read(lba, 1)
	for i := range b {
		b[i] ^= mask
	}
	h.disk.Put(lba, b)
}

func TestRecoverDamageInTheMiddleIsCorruption(t *testing.T) {
	cases := []struct {
		name   string
		damage func(h *harness)
		lsn    uint64
	}{
		{"record 2's header, every byte", func(h *harness) { xorBlock(h, 68, 0x5a) }, 2},
		{"a bit of record 3's header", func(h *harness) { h.disk.FlipBit(71, 60, 1) }, 3},
		{"a bit of record 3's data", func(h *harness) { h.disk.FlipBit(72, 100, 0x80) }, 3},
		{"a bit of record 4's header, the last but one", func(h *harness) { h.disk.FlipBit(74, 60, 1) }, 4},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := formatted(t, gib, testConfig(0))
			h.twoBlockWrites(5)
			c.damage(h)
			wantCorrupt(t, h.recover(disklog.ModeOpen), c.lsn)
			wantCorrupt(t, h.recover(disklog.ModeRebuild), c.lsn)
		})
	}
}

func TestRecoverDamageInTheLastRecordIsACrash(t *testing.T) {
	h := formatted(t, gib, testConfig(0))
	h.twoBlockWrites(5)
	h.disk.FlipBit(78, 7, 4)
	h.wantLSNs(h.reopen(), lsnRange(1, 4))
}

func TestRecoverOverwritesATornTail(t *testing.T) {
	h := formatted(t, gib, testConfig(0))
	n := newNode(h, 0)
	h.twoBlockWrites(10)
	h.submit(disklog.Hot, h.chunkWrite(10, 0, 4, 11))
	h.wantTrace("write 95+5 io124", "fsync io125")
	h.step(0)
	h.crashLanding(95, 97, 99)
	res := h.reopen()
	h.wantLSNs(res, lsnRange(1, 10))
	if !isZeroBlock(h.read(95, 1)) {
		t.Fatal("the torn record's header was not erased")
	}
	n.live = res.index
	for i := range 5 {
		h.put(disklog.Hot, h.chunkWrite(uint32(20+i), 0, 1, uint64(100+i)))
	}
	res = h.reopen()
	h.wantLSNs(res, lsnRange(1, 15))
	h.wantModel("index", res.index, n.live)
	h.readBack(res.index)
}

func isZeroBlock(b []byte) bool { return !slices.ContainsFunc(b, func(c byte) bool { return c != 0 }) }

func TestRecoverExactlyFullSegment(t *testing.T) {
	h := formatted(t, gib, testConfig(0))
	n := newNode(h, 0)
	h.twoBlockWrites(21)
	if last := h.eventsOf(disklog.Durable)[20]; last.Segment != 1 || last.Block != 61 {
		t.Fatalf("record 21 at segment %d block %d, want segment 1 block 61", last.Segment, last.Block)
	}
	n.live = h.reopen().index
	h.put(disklog.Hot, h.chunkWrite(21, 0, 2, 22))
	if d := h.eventsOf(disklog.Durable); len(d) != 1 || d[0].LSN != 22 || d[0].Segment != 2 {
		t.Fatalf("record 22: %+v", d)
	}
	for i := range 8 {
		h.put(disklog.Hot, h.chunkWrite(uint32(22+i), 0, 2, uint64(23+i)))
	}
	res := h.reopen()
	h.wantLSNs(res, lsnRange(1, 30))
	h.wantModel("index", res.index, n.live)
}

func TestRecoverCrashInTheRoundThatOpensASegment(t *testing.T) {
	for _, header := range []bool{false, true} {
		for _, record := range []bool{false, true} {
			h := formatted(t, gib, testConfig(0))
			n := newNode(h, 0)
			h.twoBlockWrites(21)
			h.submit(disklog.Hot, h.chunkWrite(21, 0, 2, 22))
			h.wantTrace("write 128+4 io146", "fsync io147")
			h.step(0)
			var landed []core.LBA
			if header {
				landed = append(landed, 128)
			}
			if record {
				landed = append(landed, 129, 130, 131)
			}
			h.crashLanding(landed...)
			res := h.reopen()
			last := uint64(21)
			if header && record {
				last = 22
			}
			h.wantLSNs(res, lsnRange(1, last))
			if record && !header && !isZeroBlock(h.read(129, 1)) {
				t.Fatal("the record of a segment whose header never landed was not erased")
			}
			n.live = res.index
			h.put(disklog.Hot, h.chunkWrite(30, 0, 1, 99))
			wantSeq := uint64(2)
			if record && !header {
				wantSeq = 3
			}
			if sh := h.segmentHeader(2); sh.Seq != wantSeq {
				t.Fatalf("header %v, record %v: segment 2 has seq %d, want %d", header, record, sh.Seq, wantSeq)
			}
			res = h.reopen()
			h.wantLSNs(res, lsnRange(1, last+1))
			h.wantModel("index", res.index, n.live)
			h.readBack(res.index)
		}
	}
}

func TestRecoverTornRecordLeftInTheOtherStream(t *testing.T) {
	h := formatted(t, gib, testConfig(0))
	n := newNode(h, 0)
	h.twoBlockWrites(2)
	h.submit(disklog.Cold, h.blob(disklog.BlobRun, 9, 3*core.BlockSize, 1))
	h.wantTrace("write 128+5 io108", "fsync io109")
	h.step(0)
	h.crashLanding(128, 129)
	res := h.reopen()
	h.wantLSNs(res, lsnRange(1, 2))
	if !isZeroBlock(h.read(129, 1)) {
		t.Fatal("the torn cold record's header was not erased")
	}
	n.live = res.index
	h.put(disklog.Hot, h.chunkWrite(5, 0, 1, 5))
	if d := h.eventsOf(disklog.Durable); len(d) != 1 || d[0].LSN != 3 || d[0].Segment != 1 {
		t.Fatalf("next hot write: %+v", d)
	}
	res = h.reopen()
	h.wantLSNs(res, lsnRange(1, 3))
	h.wantModel("open", res.index, n.live)
	reb := h.recover(disklog.ModeRebuild)
	h.wantLSNs(reb, lsnRange(1, 3))
	h.wantModel("rebuild", reb.index, n.live)
}

func TestRecoverReadErrorsOnBothSuperblocks(t *testing.T) {
	h := formatted(t, gib, testConfig(0))
	h.twoBlockWrites(1)
	h.disk.FailReads(0, 63)
	if res := h.recover(disklog.ModeOpen); !errors.Is(res.err, core.DiskIO) {
		t.Fatalf("both superblocks unreadable: %v", res.err)
	}
}

func TestRecoverReadErrorInASegmentFails(t *testing.T) {
	h := formatted(t, gib, testConfig(0))
	h.twoBlockWrites(3)
	h.disk.FailReads(h.geo.LBA(1, 0))
	if res := h.recover(disklog.ModeOpen); !errors.Is(res.err, core.DiskIO) {
		t.Fatalf("unreadable segment header: %v", res.err)
	}
}

func TestRecoverManySegmentsBothStreams(t *testing.T) {
	h := formatted(t, 1<<16*core.BlockSize, testConfig(4))
	n := newNode(h, 0)
	r := core.NewRand(7)
	for i := range 200 {
		h.put(disklog.Hot, h.chunkWrite(uint32(r.Below(32)), uint16(r.Below(1000)), uint16(r.Range(1, 2)), uint64(i+1)))
		if i%5 == 4 {
			h.put(disklog.Cold, h.blob(disklog.BlobRun, 9, int(r.Range(1, h.log.MaxBlobBytes())), uint64(i)))
		}
	}
	durable := h.log.DurableLSN()
	if last := h.eventsOf(disklog.Durable); last[len(last)-1].Segment < 20 {
		t.Fatalf("200 writes reached only segment %d", last[len(last)-1].Segment)
	}
	res := h.reopen()
	h.wantLSNs(res, lsnRange(1, durable))
	h.wantModel("open", res.index, n.live)
	reb := h.recover(disklog.ModeRebuild)
	h.wantModel("rebuild", reb.index, n.live)
	h.readBack(res.index)
}

func TestRecoverUsesTheNewestNamedManifest(t *testing.T) {
	for _, limit := range []int{0, 512} {
		cfg := testConfig(0)
		cfg.MaxReplayBytes = 40 * core.BlockSize
		h := formatted(t, 1<<16*core.BlockSize, cfg)
		n := newNode(h, limit)
		r := core.NewRand(11)
		for i := range 120 {
			chunk := uint32(r.Below(16))
			if i%7 == 6 {
				h.submit(disklog.Hot, disklog.Item{Entry: disklog.PunchEntry(1, chunkOf(chunk), uint64(1000+i), 0, core.BlocksPerChunk)})
			} else {
				h.submit(disklog.Hot, h.chunkWrite(chunk, 0, uint16(r.Range(1, 4)), uint64(i+1)))
			}
			for {
				h.run()
				if !n.step() {
					break
				}
			}
		}
		if n.checkpoints < 5 {
			t.Fatalf("%d checkpoints", n.checkpoints)
		}
		res := h.reopen()
		if res.manifest.IsNone() || (res.pieces > 1) != (limit > 0) {
			t.Fatalf("piece limit %d: manifest %+v loaded in %d pieces", limit, res.manifest, res.pieces)
		}
		bound := h.cfg.MaxReplayBytes + 3*h.geo.SegmentBytes()
		if got := res.replayedBytes; got > bound {
			t.Fatalf("replayed %d bytes, bound %d", got, bound)
		}
		if reads := res.replayReads; reads > bound+4*h.geo.SegmentBytes() {
			t.Fatalf("replay read %d bytes, bound %d plus a segment per stream before the replay point and the rest of each open one", reads, bound)
		}
		h.wantModel("open", res.index, n.live)
		reb := h.recover(disklog.ModeRebuild)
		h.wantModel("rebuild", reb.index, n.live)
		h.readBack(res.index)
	}
}

func TestRecoverManifestInPieces(t *testing.T) {
	h := formatted(t, gib, testConfig(3))
	for i := range 3 {
		h.put(disklog.Hot, h.chunkWrite(uint32(i), 0, 1, uint64(i+1)))
	}
	checkpoint := func(runs int) {
		if err := h.log.BeginCheckpoint([]disklog.CheckpointIndex{{Owner: indexOwner, ReplayLSN: h.log.DurableLSN()}}); err != nil {
			t.Fatal(err)
		}
		for i := range runs {
			it := h.blob(disklog.BlobRun, indexOwner, 1, uint64(i))
			if _, err := h.log.CheckpointPiece(&h.ctx, indexOwner, it.Buf, 1, true, &h.out); err != nil {
				t.Fatal(err)
			}
			h.absorb()
		}
		if err := h.log.EndCheckpoint(&h.ctx, &h.out); err != nil {
			t.Fatal(err)
		}
		h.absorb()
	}
	checkpoint(500)
	h.run()
	first := h.log.Manifest()
	if first.IsNone() {
		t.Fatal("no manifest")
	}
	written := h.readManifestAt(first)
	for i := range 40 {
		h.put(disklog.Hot, h.chunkWrite(uint32(10+i), 0, 1, uint64(10+i)))
	}
	durable := h.log.DurableLSN()
	res := h.reopen()
	if res.manifest != first || !manifestsEqual(res.loaded, written) || len(res.loaded.Indexes[0].Runs) != 500 {
		t.Fatalf("loaded manifest at %+v with %d runs, want %+v", res.manifest, len(res.loaded.Indexes[0].Runs), first)
	}
	h.wantLSNs(res, lsnRange(written.Indexes[0].ReplayLSN+1, durable))

	checkpoint(500)
	for len(h.pending) > 0 {
		if a := h.pending[0].act; a.Kind == core.ActDiskWrite && h.lastManifestPieceIn(a) {
			break
		}
		h.step(0)
	}
	h.crashLanding()
	res = h.reopen()
	if res.manifest != first || !manifestsEqual(res.loaded, written) {
		t.Fatalf("a crash before the last piece recovered from %+v, want %+v", res.manifest, first)
	}
}

// lastManifestPieceIn reports whether a write holds the record with a manifest's last piece.
func (h *harness) lastManifestPieceIn(a core.Action) bool {
	img := h.gather(a)
	for off := 0; off < len(img); off += core.BlockSize {
		at := a.LBA + core.LBA(off/core.BlockSize)
		seg, block := uint32(uint64(at)/uint64(h.geo.SegmentBlocks)), uint32(uint64(at)%uint64(h.geo.SegmentBlocks))
		if block == 0 {
			continue
		}
		hdr, err := disklog.DecodeHeader(img[off:], disklog.Place{LogID: h.logID, Seq: h.seqOf(a, seg, img), Offset: block}, h.geo.SegmentBlocks-block, nil)
		if err == nil && slices.ContainsFunc(hdr.Entries, func(e disklog.Entry) bool { return e.BlobKind == disklog.BlobManifest }) {
			return true
		}
	}
	return false
}

func (h *harness) gather(a core.Action) []byte {
	var img []byte
	for _, v := range a.Vec {
		img = append(img, h.pool.Bytes(v.Buf)[:v.Len]...)
	}
	return img
}

// seqOf is the segment sequence a write's records carry: from the segment header it
// starts with, or from the header already on disk.
func (h *harness) seqOf(a core.Action, seg uint32, img []byte) uint64 {
	if a.LBA == h.geo.LBA(seg, 0) {
		if sh, err := disklog.DecodeSegmentHeader(img); err == nil {
			return sh.Seq
		}
	}
	return h.segmentSeq(seg)
}

func TestRecoverReportsWhatIsLeftToReplay(t *testing.T) {
	cfg := testConfig(0)
	cfg.MaxReplayBytes = 8 * core.BlockSize
	h := formatted(t, gib, cfg)
	h.put(disklog.Hot, h.chunkWrite(0, 0, 3, 1))
	h.put(disklog.Hot, h.chunkWrite(1, 0, 3, 2))
	h.reopen()
	if !h.log.CheckpointDue() || h.log.ReplayBytes() != 8*core.BlockSize {
		t.Fatalf("after recovery: due %v with %d bytes to replay, want 8 blocks", h.log.CheckpointDue(), h.log.ReplayBytes())
	}
}

func TestRecoveryRefusesCallsOutOfStep(t *testing.T) {
	h := formatted(t, gib, testConfig(0))
	h.twoBlockWrites(2)
	h.resetIO()
	var out disklog.Output
	if _, err := disklog.Recover(&h.ctx, disklog.Config{MaxRecordBytes: 1 << 20, CommitDepth: 1}, h.disk.Blocks(), disklog.ModeOpen, 1, &out); !errors.Is(err, disklog.ErrBadConfig) {
		t.Fatalf("no read depth: %v", err)
	}
	if _, err := disklog.Recover(&h.ctx, h.cfg, h.disk.Blocks(), 0, 1, &out); !errors.Is(err, disklog.ErrBadConfig) {
		t.Fatalf("no mode: %v", err)
	}
	rec, err := disklog.Recover(&h.ctx, h.cfg, h.disk.Blocks(), disklog.ModeOpen, firstIO, &h.out)
	if err != nil {
		t.Fatal(err)
	}
	h.rec = rec
	h.absorb()
	if err := rec.Replay(&h.ctx, &h.out); !errors.Is(err, disklog.ErrRecovering) {
		t.Fatalf("replay before the manifest: %v", err)
	}
	if _, err := rec.Log(&h.ctx); !errors.Is(err, disklog.ErrRecovering) {
		t.Fatalf("log before recovery ends: %v", err)
	}
	if err := rec.Close(&h.ctx); !errors.Is(err, disklog.ErrBusy) {
		t.Fatalf("close with a read in flight: %v", err)
	}
	h.run()
	if err := rec.ReadBlob(&h.ctx, disklog.BlobRef{Segment: 1, Block: 1, LSN: 2}, &h.out); err != nil {
		t.Fatal(err)
	}
	h.absorb()
	h.run()
	failed := h.eventsOf(disklog.RecoveryFailed)
	var ce *disklog.CorruptError
	if len(failed) != 1 || !errors.As(failed[0].Err, &ce) || ce.LSN != 2 {
		t.Fatalf("a reference to the wrong record: %+v", failed)
	}
	if err := rec.Replay(&h.ctx, &h.out); !errors.Is(err, disklog.ErrCorrupt) {
		t.Fatalf("replay after failing: %v", err)
	}
	if h.pool.Live() != 0 {
		t.Fatalf("%d buffers live after a failed recovery", h.pool.Live())
	}
}

func TestCommitDepth2KeepsBothLandedRounds(t *testing.T) {
	cfg := testConfig(0)
	cfg.CommitDepth = 2
	h := formatted(t, gib, cfg)
	n := newNode(h, 0)
	h.put(disklog.Hot, h.chunkWrite(0, 0, 1, 1))
	h.submit(disklog.Hot, h.chunkWrite(1, 0, 2, 2))
	h.submit(disklog.Cold, h.chunkWrite(2, 0, 1, 3))
	h.wantTrace("write 64+3 io104", "fsync io105", "write 67+3 io106", "fsync io107", "write 128+3 io108")
	h.step(2)
	h.step(0)
	h.crashLanding(67, 68, 69, 128, 129, 130)
	res := h.reopen()
	h.wantLSNs(res, lsnRange(1, 3))
	want := n.live.clone()
	want.apply(res.replayed[1])
	want.apply(res.replayed[2])
	h.wantModel("index", res.index, want)
	h.readBack(res.index)
}

func TestCommitDepth2DropsRoundsAfterAMissingOne(t *testing.T) {
	cfg := testConfig(0)
	cfg.CommitDepth = 2
	h := formatted(t, gib, cfg)
	h.put(disklog.Hot, h.chunkWrite(0, 0, 1, 1))
	h.submit(disklog.Hot, h.chunkWrite(1, 0, 2, 2))
	h.submit(disklog.Hot, h.chunkWrite(2, 0, 1, 3))
	h.wantTrace("write 64+3 io104", "fsync io105", "write 67+3 io106", "fsync io107", "write 70+2 io108")
	h.step(2)
	h.step(0)
	h.crashLanding(68, 69, 70, 71)
	res := h.reopen()
	h.wantLSNs(res, lsnRange(1, 1))
	if !isZeroBlock(h.read(70, 1)) {
		t.Fatal("the landed record after the missing one was not erased")
	}
	n := newNode(h, 0)
	n.live = res.index
	h.put(disklog.Hot, h.chunkWrite(3, 0, 1, 4))
	if d := h.eventsOf(disklog.Durable); len(d) != 1 || d[0].LSN != 2 || d[0].LBA != 67 {
		t.Fatalf("next write: %+v", d)
	}
	res = h.reopen()
	h.wantLSNs(res, lsnRange(1, 2))
	h.wantModel("index", res.index, n.live)
}

func TestRecoverFlushesBeforeReportingRecovered(t *testing.T) {
	h := formatted(t, gib, testConfig(0))
	h.twoBlockWrites(3)
	h.submit(disklog.Hot, h.chunkWrite(3, 0, 2, 4))
	h.step(0)
	res := h.reopen()
	if res.durable != 4 || h.disk.Unflushed() != 0 {
		t.Fatalf("restart without a crash recovered LSN %d with %d blocks unflushed", res.durable, h.disk.Unflushed())
	}
	if last := res.trace[len(res.trace)-1]; !strings.HasPrefix(last, "fsync ") {
		t.Fatalf("recovery ended with %q, want a flush", last)
	}
	h.crashLanding()
	h.wantLSNs(h.reopen(), lsnRange(1, 4))
}

func TestRecoverRejectsAnImplausibleHeaderLength(t *testing.T) {
	h := newHarness(t, 256*gib, 64<<20, testConfig(0))
	h.run()
	for i := range 3 {
		h.put(disklog.Hot, h.chunkWrite(uint32(i), 0, 1, uint64(i+1)))
	}
	last := h.eventsOf(disklog.Durable)[2]
	h.disk.FlipBit(last.LBA, 7, 0x40)
	res := h.reopen()
	h.wantLSNs(res, lsnRange(1, 2))
}

func TestRecoverReportsADuplicateLSN(t *testing.T) {
	cfg := testConfig(0)
	cfg.CommitDepth = 2
	h := formatted(t, gib, cfg)
	h.twoBlockWrites(21)
	disklog.AppendAnywhere(h.log)
	h.submit(disklog.Hot, h.chunkWrite(30, 0, 2, 30))
	h.submit(disklog.Hot, h.chunkWrite(31, 0, 1, 31))
	h.wantTrace("write 128+4 io146", "fsync io147", "write 132+2 io148")
	h.step(2)
	h.crashLanding(132, 133)
	h.wantLSNs(h.reopen(), lsnRange(1, 21))
	h.put(disklog.Hot, h.chunkWrite(32, 0, 1, 32))
	h.put(disklog.Cold, h.chunkWrite(33, 0, 1, 33))
	if sh := h.segmentHeader(2); sh.Seq != 2 {
		t.Fatalf("segment 2 reopened with seq %d", sh.Seq)
	}
	wantCorrupt(t, h.recover(disklog.ModeOpen), 23)
}

func TestRecoverRefusesToDropADurableManifest(t *testing.T) {
	cfg := testConfig(0)
	cfg.CommitDepth = 2
	h := formatted(t, gib, cfg)
	h.twoBlockWrites(21)
	if err := h.log.BeginCheckpoint([]disklog.CheckpointIndex{{Owner: indexOwner, ReplayLSN: 21}}); err != nil {
		t.Fatal(err)
	}
	if err := h.log.EndCheckpoint(&h.ctx, &h.out); err != nil {
		t.Fatal(err)
	}
	h.absorb()
	h.run()
	m := h.log.Manifest()
	if m.LSN != 22 {
		t.Fatalf("manifest %+v", m)
	}
	h.submit(disklog.Hot, h.chunkWrite(30, 0, 2, 30))
	h.step(0)
	h.crashLanding(h.geo.LBA(3, 0))
	h.disk.FlipBit(126, 10, 1)
	wantCorrupt(t, h.recover(disklog.ModeRebuild), 21)
	res := h.reopen()
	if res.durable != 22 || res.manifest != m {
		t.Fatalf("open recovered LSN %d from %+v", res.durable, res.manifest)
	}
}

func TestRecoverReadsSegmentHeadersManyAtATime(t *testing.T) {
	for _, depth := range []int{1, 4, 64} {
		cfg := testConfig(0)
		cfg.ReadDepth = depth
		h := formatted(t, gib, cfg)
		h.twoBlockWrites(2)
		h.reopen()
		if h.maxPending != depth {
			t.Fatalf("read depth %d: %d reads in flight at most", depth, h.maxPending)
		}
	}
}

func TestRecoveryFailsOnAFailedWriteOrFlush(t *testing.T) {
	cases := []struct {
		name  string
		setup func(h *harness)
		fail  func(a core.Action) bool
	}{
		{"superblock repair", func(h *harness) { h.disk.FlipBit(63, 17, 1) }, func(a core.Action) bool {
			return a.Kind == core.ActDiskWrite && a.LBA == 63
		}},
		{"erase", func(h *harness) {
			h.submit(disklog.Hot, h.chunkWrite(9, 0, 2, 9))
			h.step(0)
			h.crashLanding(68)
		}, func(a core.Action) bool { return a.Kind == core.ActDiskWrite && a.LBA == 68 }},
		{"final flush", func(*harness) {}, func(a core.Action) bool { return a.Kind == core.ActDiskFsync }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := formatted(t, gib, testConfig(0))
			h.twoBlockWrites(1)
			c.setup(h)
			h.failWhen = func(a core.Action) core.DiskError {
				if c.fail(a) {
					return core.DiskIO
				}
				return core.DiskOK
			}
			if res := h.recover(disklog.ModeOpen); !errors.Is(res.err, core.DiskIO) {
				t.Fatalf("recovery: %v, want Disk(Io)", res.err)
			}
		})
	}
}
