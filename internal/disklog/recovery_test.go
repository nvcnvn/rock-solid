package disklog_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
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
	h.put(disklog.Cold, h.blob(disklog.BlobRaftEntries, 9, 100, 1))
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
			wantUsage := []disklog.SegmentUsage{{Segment: 1, State: disklog.SegmentUsed, Live: 42, Newest: 21}}
			if header {
				wantUsage = append(wantUsage, disklog.SegmentUsage{Segment: 2, State: disklog.SegmentUsed})
				if record {
					wantUsage[1].Live, wantUsage[1].Newest = 2, 22
				}
			}
			if got := disklog.SegmentUsageOf(h.log); !slices.Equal(got, wantUsage) {
				t.Fatalf("header %v, record %v: usage %+v, want %+v", header, record, got, wantUsage)
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
	h.submit(disklog.Cold, h.blob(disklog.BlobRaftEntries, 9, 3*core.BlockSize, 1))
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
			h.put(disklog.Cold, h.blob(disklog.BlobRaftEntries, 9, int(r.Range(1, h.log.MaxBlobBytes())), uint64(i)))
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

// TestRecoverManifestInPieces loads a snapshot and an edit written in pieces, and recovers
// from the manifest before one whose last piece never landed or that no segment header names.
func TestRecoverManifestInPieces(t *testing.T) {
	h := formatted(t, gib, testConfig(3))
	for i := range 3 {
		h.put(disklog.Hot, h.chunkWrite(uint32(i), 0, 1, uint64(i+1)))
	}
	begin := func(carried []disklog.BlobRef, runs int) {
		if err := h.log.BeginCheckpoint([]disklog.CheckpointIndex{{Owner: runOwner, ReplayLSN: h.log.DurableLSN(), Runs: carried}}); err != nil {
			t.Fatal(err)
		}
		for i := range runs {
			it := h.blob(disklog.BlobRun, runOwner, 1, uint64(i))
			if _, err := h.log.CheckpointPiece(&h.ctx, runOwner, it.Buf, 1, true, &h.out); err != nil {
				t.Fatal(err)
			}
			h.absorb()
		}
		if err := h.log.EndCheckpoint(&h.ctx, &h.out); err != nil {
			t.Fatal(err)
		}
		h.absorb()
	}
	crashBeforeLastPiece := func() {
		for len(h.pending) > 0 {
			if a := h.pending[0].act; a.Kind == core.ActDiskWrite && h.lastManifestPieceIn(a) {
				break
			}
			h.step(0)
		}
		h.crashLanding()
	}
	wantLoaded := func(name string, pos disklog.ManifestPos, written disklog.Manifest) *recovered {
		t.Helper()
		res := h.reopen()
		if res.manifest != pos || !manifestsEqual(res.loaded, written) {
			t.Fatalf("%s: recovered from %+v with %d runs, want %+v with %d", name, res.manifest, len(res.loaded.Indexes[0].Runs), pos, len(written.Indexes[0].Runs))
		}
		return res
	}

	begin(nil, 500)
	h.run()
	first := h.log.Manifest()
	written := h.readManifestAt(first)
	if c := h.readChain(posEvent(h, first)); len(c.bodies) != 1 || len(c.pieces[0]) == 0 {
		t.Fatalf("first manifest: %d manifests, %d earlier pieces; want a snapshot in pieces", len(c.bodies), len(c.pieces[0]))
	}
	for i := range 40 {
		h.put(disklog.Hot, h.chunkWrite(uint32(10+i), 0, 1, uint64(10+i)))
	}
	durable := h.log.DurableLSN()
	res := wantLoaded("snapshot", first, written)
	h.wantLSNs(res, lsnRange(written.Indexes[0].ReplayLSN+1, durable))

	begin(written.Indexes[0].Runs, 300)
	crashBeforeLastPiece()
	wantLoaded("a crash before an edit's last piece", first, written)

	begin(written.Indexes[0].Runs, 300)
	h.run()
	edit := h.log.Manifest()
	if c := h.readChain(posEvent(h, edit)); len(c.bodies) != 2 || len(c.pieces[1]) == 0 {
		t.Fatalf("second manifest: %d manifests, %d earlier pieces of its own; want an edit in pieces", len(c.bodies), len(c.pieces[1]))
	}
	h.nameManifest()
	written = h.readManifestAt(edit)
	wantLoaded("snapshot and edit", edit, written)

	h.log.RequestSnapshot()
	begin(written.Indexes[0].Runs, 10)
	h.run()
	if c := h.readChain(posEvent(h, h.log.Manifest())); len(c.bodies) != 1 {
		t.Fatalf("requested snapshot lists %d manifests", len(c.bodies))
	}
	h.crashLanding()
	wantLoaded("a crash before a segment header names a snapshot", edit, written)
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

func TestRecoverReportsADuplicateLSNInOneStream(t *testing.T) {
	h := formatted(t, gib, testConfig(0))
	h.twoBlockWrites(4)
	durable := h.eventsOf(disklog.Durable)
	second, last := durable[1], durable[3]
	img := h.read(second.LBA, 3)
	binary.LittleEndian.PutUint32(img[12:], last.Block+3)
	disklog.FixRecordCRC(img)
	h.disk.Put(last.LBA+3, img)
	wantCorrupt(t, h.recover(disklog.ModeOpen), 2)
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

func TestRecoveryThatEndsCorruptWritesNothing(t *testing.T) {
	cases := []struct {
		name   string
		damage func(h *harness)
		lsn    uint64
	}{
		{"none", func(*harness) {}, 0},
		{"a bit of record 2's data", func(h *harness) { h.disk.FlipBit(10, 100, 0x80) }, 2},
		{"a bit of segment 3's header", func(h *harness) { h.disk.FlipBit(12, 30, 1) }, 3},
	}
	modes := []struct {
		name string
		mode disklog.Mode
	}{{"open", disklog.ModeOpen}, {"rebuild", disklog.ModeRebuild}}
	for _, c := range cases {
		for _, m := range modes {
			t.Run(c.name+", "+m.name, func(t *testing.T) {
				const blocks = 1024
				h := formatted(t, blocks*core.BlockSize, testConfig(0))
				for i := range 5 {
					h.put(disklog.Hot, h.chunkWrite(uint32(i), 0, 1, uint64(i+1)))
				}
				h.trace = nil
				h.submit(disklog.Hot, h.chunkWrite(5, 0, 1, 6))
				h.wantTrace("write 24+3 io114", "fsync io115")
				h.step(0)
				h.crashLanding(24, 25)
				c.damage(h)
				before := h.read(0, blocks)
				res := h.recover(m.mode)
				if c.lsn == 0 {
					if res.err != nil || !slices.ContainsFunc(res.trace, func(s string) bool { return strings.HasPrefix(s, "write ") }) {
						t.Fatalf("undamaged recovery: %v, trace %q; want it to erase the torn record", res.err, res.trace)
					}
					return
				}
				wantCorrupt(t, res, c.lsn)
				if i := slices.IndexFunc(res.trace, func(s string) bool { return !strings.HasPrefix(s, "read ") }); i >= 0 {
					t.Fatalf("a recovery that ends Corrupt sent %q", res.trace[i])
				}
				if h.disk.Unflushed() != 0 || !bytes.Equal(h.read(0, blocks), before) {
					t.Fatal("a recovery that ends Corrupt changed the disk")
				}
			})
		}
	}
}

// wantNoRecordIn checks that no block from lba to the end of its segment decodes as a record
// of this log at that place.
func (h *harness) wantNoRecordIn(lba core.LBA, n uint64) {
	h.t.Helper()
	for at := lba; at < lba+core.LBA(n); at++ {
		seg, block := uint32(uint64(at)/uint64(h.geo.SegmentBlocks)), uint32(uint64(at)%uint64(h.geo.SegmentBlocks))
		left := h.geo.SegmentBlocks - block
		place := disklog.Place{LogID: h.logID, Seq: h.segmentSeq(seg), Offset: block}
		if hdr, err := disklog.DecodeHeader(h.read(at, uint64(left)), place, left, nil); err == nil {
			h.t.Fatalf("LBA %d reads as LSN %d", at, hdr.Record.LSN)
		}
	}
}

func TestRecoverAfterAFailedWrite(t *testing.T) {
	for _, depth := range []int{1, 2} {
		for _, headerLanded := range []bool{false, true} {
			t.Run(fmt.Sprintf("depth %d, header landed %v", depth, headerLanded), func(t *testing.T) {
				cfg := testConfig(0)
				cfg.CommitDepth = depth
				h := formatted(t, gib, cfg)
				n := newNode(h, 0)
				h.twoBlockWrites(3)
				a := h.submit(disklog.Hot, h.chunkWrite(3, 0, 2, 4))
				b := h.submit(disklog.Cold, h.chunkWrite(4, 0, 1, 5))
				c := h.submit(disklog.Hot, h.chunkWrite(5, 0, 1, 6))
				inDoubt, failed := []disklog.Ticket{a}, []disklog.Ticket{c, b}
				if depth == 2 {
					h.wantTrace("write 74+3 io110", "fsync io111", "write 128+3 io112")
					inDoubt, failed = []disklog.Ticket{a, b}, []disklog.Ticket{c}
				} else {
					h.wantTrace("write 74+3 io110", "fsync io111")
				}
				h.failWhen = func(act core.Action) core.DiskError {
					if act.IO != 110 {
						return core.DiskOK
					}
					if headerLanded {
						head := act
						head.Vec = act.Vec[:1]
						head.Len = uint64(act.Vec[0].Len)
						h.disk.Execute(h.pool, &head)
					}
					return core.DiskIO
				}
				h.run()
				h.failWhen = nil
				if d := h.eventsOf(disklog.Durable); len(d) != 3 {
					t.Fatalf("acknowledged %+v after a failed write", d[3:])
				}
				gotDoubt, gotFailed := h.eventsOf(disklog.InDoubt), h.eventsOf(disklog.Failed)
				if len(gotDoubt) != 1 || !slices.Equal(gotDoubt[0].Tickets, inDoubt) || len(gotFailed) != 1 || !slices.Equal(gotFailed[0].Tickets, failed) {
					t.Fatalf("events %+v", h.events)
				}
				res := h.reopen()
				h.wantLSNs(res, lsnRange(1, 3))
				h.wantModel("index", res.index, n.live)
				h.wantNoRecordIn(74, 3)
				h.put(disklog.Hot, h.chunkWrite(9, 0, 1, 9))
				if d := h.eventsOf(disklog.Durable); len(d) != 1 || d[0].LSN != 4 {
					t.Fatalf("next write after recovery: %+v", d)
				}
				res = h.reopen()
				h.wantLSNs(res, lsnRange(1, 4))
				h.wantModel("index", res.index, n.live)
				h.readBack(res.index)
			})
		}
	}
}

// failedFlush builds a 1,024-block log of three 1-block writes, then one round per commit
// depth whose write lands unflushed and whose first flush fails.
func failedFlush(t *testing.T, depth int) (*harness, *node) {
	t.Helper()
	cfg := testConfig(0)
	cfg.CommitDepth = depth
	h := formatted(t, 1024*core.BlockSize, cfg)
	n := newNode(h, 0)
	for i := range 3 {
		h.put(disklog.Hot, h.chunkWrite(uint32(i), 0, 1, uint64(i+1)))
	}
	h.trace = nil
	h.submit(disklog.Hot, h.chunkWrite(3, 0, 1, 4))
	h.submit(disklog.Cold, h.chunkWrite(4, 0, 1, 5))
	h.failIO[111] = core.DiskIO
	if depth == 2 {
		h.wantTrace("write 16+3 io110", "fsync io111", "write 20+3 io112")
		h.step(0)
		h.step(1)
		h.wantTrace("write 16+3 io110", "fsync io111", "write 20+3 io112", "fsync io113")
	} else {
		h.wantTrace("write 16+3 io110", "fsync io111")
		h.step(0)
	}
	h.step(0)
	clear(h.failIO)
	return h, n
}

func TestRecoverAfterAFailedFlush(t *testing.T) {
	for _, depth := range []int{1, 2} {
		h, _ := failedFlush(t, depth)
		if d := h.eventsOf(disklog.Durable); len(d) != 3 || len(h.eventsOf(disklog.InDoubt)) != 1 {
			t.Fatalf("depth %d: events after a failed flush %+v", depth, h.events)
		}
		for i := range depth {
			chunk, lba := chunkOf(uint32(3+i)), core.LBA(18+4*i)
			if !bytes.Equal(h.read(lba, 1), blockData(chunk, uint64(4+i), 0)) {
				t.Fatalf("depth %d: reads do not see round %d's data at LBA %d", depth, 4+i, lba)
			}
		}
		unflushed := h.disk.Unflushed()
		if unflushed != 3*depth {
			t.Fatalf("depth %d: %d blocks unflushed", depth, unflushed)
		}
		for mask := range 1 << unflushed {
			h, n := failedFlush(t, depth)
			h.disk.Crash(func(i int, _ core.LBA) bool { return mask>>i&1 == 1 })
			res := h.reopen()
			if res.durable < 3 || res.durable > uint64(3+depth) {
				t.Fatalf("depth %d, landed %b: recovered LSN %d", depth, mask, res.durable)
			}
			if mask == 1<<unflushed-1 && res.durable != uint64(3+depth) {
				t.Fatalf("depth %d: every block landed, recovered LSN %d", depth, res.durable)
			}
			h.wantLSNs(res, lsnRange(1, res.durable))
			for lsn := uint64(4); lsn <= res.durable; lsn++ {
				if round := mask >> (3 * (lsn - 4)) & 7; round != 7 {
					t.Fatalf("depth %d, landed %b: kept LSN %d with only blocks %b of it", depth, mask, lsn, round)
				}
			}
			want := n.live.clone()
			for _, ev := range res.replayed[3:] {
				want.apply(ev)
			}
			h.wantModel("index", res.index, want)
			h.readBack(res.index)
		}
	}
}

const (
	ownerIndexA  = 1
	ownerIndexB  = 2
	ownerReplica = 3
)

type fact struct {
	lsn   uint64
	entry disklog.Entry
}

// owners stands in for two chunk indexes and a shard replica sharing one log: index A owns
// the writes of pool 1, index B those of pool 2, and the replica its Raft entries. A run
// holds the number of facts its owner had applied.
type owners struct {
	h    *harness
	seed uint64
	live map[uint32][]fact
	refs map[disklog.Ticket]disklog.BlobRef
}

func newOwners(h *harness) *owners {
	o := &owners{h: h, live: map[uint32][]fact{}, refs: map[disklog.Ticket]disklog.BlobRef{}}
	h.onEvent = func(ev disklog.Event) {
		if ev.Kind != disklog.Durable {
			return
		}
		appendFacts(o.live, ev, nil)
		for i, t := range ev.Tickets {
			o.refs[t] = disklog.BlobRef{Segment: ev.Segment, Block: ev.Block, LSN: ev.LSN, Entry: uint16(i)}
		}
	}
	return o
}

func ownerOf(e disklog.Entry) (uint32, bool) {
	switch {
	case e.Kind == disklog.EntryWrite:
		return uint32(e.Pool), true
	case e.Kind == disklog.EntryBlob && e.BlobKind == disklog.BlobRaftEntries:
		return e.Owner, true
	}
	return 0, false
}

// appendFacts gives each owner the entries of a record past its replay LSN.
func appendFacts(facts map[uint32][]fact, ev disklog.Event, replayLSN map[uint32]uint64) {
	for _, e := range ev.Entries {
		if o, ok := ownerOf(e); ok && ev.LSN > replayLSN[o] {
			facts[o] = append(facts[o], fact{ev.LSN, e})
		}
	}
}

func (o *owners) item(owner uint32) disklog.Item {
	o.seed++
	if owner == ownerReplica {
		return o.h.blob(disklog.BlobRaftEntries, owner, 200, o.seed)
	}
	it := o.h.chunkWrite(uint32(o.seed), 0, 1, o.seed)
	it.Entry.Pool = core.PoolID(owner)
	return it
}

func (o *owners) put(owners ...uint32) {
	for _, owner := range owners {
		o.h.submit(disklog.Hot, o.item(owner))
	}
	o.h.run()
}

// checkpoint writes a manifest of indexes with a new run from each owner that flushed, and
// returns where each new run sits.
func (o *owners) checkpoint(indexes []disklog.CheckpointIndex, flushed ...uint32) map[uint32]disklog.BlobRef {
	h := o.h
	h.t.Helper()
	if err := h.log.BeginCheckpoint(indexes); err != nil {
		h.t.Fatal(err)
	}
	tickets := map[uint32]disklog.Ticket{}
	for _, owner := range flushed {
		buf := h.alloc(8)
		binary.LittleEndian.PutUint64(h.pool.Bytes(buf), uint64(len(o.live[owner])))
		t, err := h.log.CheckpointPiece(&h.ctx, owner, buf, 8, true, &h.out)
		if err != nil {
			h.t.Fatal(err)
		}
		h.absorb()
		tickets[owner] = t
	}
	if err := h.log.EndCheckpoint(&h.ctx, &h.out); err != nil {
		h.t.Fatal(err)
	}
	h.absorb()
	h.run()
	if len(h.eventsOf(disklog.CheckpointDone)) == 0 {
		h.t.Fatalf("checkpoint did not complete: %+v", h.events)
	}
	h.events = nil
	runs := map[uint32]disklog.BlobRef{}
	for owner, t := range tickets {
		runs[owner] = o.refs[t]
	}
	return runs
}

// recover runs an open recovery in which each owner loads its newest run and takes the
// records past its own replay LSN, and resumes the log.
func (o *owners) recover() (uint64, map[uint32][]fact) {
	h := o.h
	h.t.Helper()
	h.resetIO()
	got := map[uint32][]fact{}
	replayLSN := map[uint32]uint64{}
	reading := map[disklog.BlobRef]uint32{}
	var durable uint64
	replay := func() {
		if err := h.rec.Replay(&h.ctx, &h.out); err != nil {
			h.t.Fatalf("replay: %v", err)
		}
		h.absorb()
	}
	saved := h.onEvent
	h.onEvent = func(ev disklog.Event) {
		switch ev.Kind {
		case disklog.ManifestLoaded:
			m, _ := h.rec.Manifest()
			for _, ix := range m.Indexes {
				replayLSN[ix.Owner] = ix.ReplayLSN
				if len(ix.Runs) > 0 {
					reading[ix.Runs[len(ix.Runs)-1]] = ix.Owner
				}
			}
			if len(reading) == 0 {
				replay()
				return
			}
			for _, ix := range m.Indexes {
				if len(ix.Runs) > 0 {
					if err := h.rec.ReadBlob(&h.ctx, ix.Runs[len(ix.Runs)-1], &h.out); err != nil {
						h.t.Fatalf("read run: %v", err)
					}
				}
			}
			h.absorb()
		case disklog.BlobRead:
			owner := reading[ev.Ref]
			delete(reading, ev.Ref)
			got[owner] = slices.Clone(o.live[owner][:binary.LittleEndian.Uint64(ev.Data)])
			if len(reading) == 0 {
				replay()
			}
		case disklog.Replayed:
			appendFacts(got, ev, replayLSN)
		case disklog.Recovered:
			durable = ev.LSN
		case disklog.RecoveryFailed:
			h.t.Fatalf("recovery: %v", ev.Err)
		}
	}
	rec, err := disklog.Recover(&h.ctx, h.cfg, h.disk.Blocks(), disklog.ModeOpen, firstIO, &h.out)
	if err != nil {
		h.t.Fatalf("recover: %v", err)
	}
	h.rec = rec
	h.absorb()
	h.run()
	h.onEvent = saved
	if h.log, err = h.rec.Log(&h.ctx); err != nil {
		h.t.Fatalf("resume: %v", err)
	}
	h.rec = nil
	h.own = h.pool.Live()
	h.trace = nil
	return durable, got
}

func TestOneCheckpointOverTwoIndexesAndAShardReplica(t *testing.T) {
	for _, landed := range []bool{false, true} {
		h := formatted(t, gib, testConfig(0))
		o := newOwners(h)
		all := []uint32{ownerIndexA, ownerIndexB, ownerReplica}
		for range 3 {
			o.put(all...)
		}
		first := h.log.DurableLSN()
		runs := o.checkpoint([]disklog.CheckpointIndex{
			{Owner: ownerIndexA, ReplayLSN: first},
			{Owner: ownerIndexB, ReplayLSN: first},
			{Owner: ownerReplica, ReplayLSN: first},
		}, all...)

		for range 3 {
			o.put(ownerIndexA, ownerReplica)
		}
		oldestA := o.live[ownerIndexA][slices.IndexFunc(o.live[ownerIndexA], func(f fact) bool { return f.lsn > first })].lsn
		second := h.log.DurableLSN()
		o.checkpoint([]disklog.CheckpointIndex{
			{Owner: ownerIndexA, ReplayLSN: first, Runs: []disklog.BlobRef{runs[ownerIndexA]}},
			{Owner: ownerIndexB, ReplayLSN: second, Runs: []disklog.BlobRef{runs[ownerIndexB]}},
			{Owner: ownerReplica, ReplayLSN: second, Runs: []disklog.BlobRef{runs[ownerReplica]}},
		}, ownerReplica)
		m := h.readManifestAt(h.log.Manifest())
		if m.BaseLSN != second || len(m.Indexes) != 3 {
			t.Fatalf("manifest %+v", m)
		}
		for _, ix := range m.Indexes {
			switch ix.Owner {
			case ownerIndexA:
				if ix.ReplayLSN >= oldestA || len(ix.Runs) != 1 {
					t.Fatalf("index A, whose flush failed, replays after LSN %d with runs %+v; its oldest unflushed write is LSN %d", ix.ReplayLSN, ix.Runs, oldestA)
				}
			case ownerIndexB:
				if ix.ReplayLSN != second || len(ix.Runs) != 1 {
					t.Fatalf("index B, with no new writes, replays after LSN %d with runs %+v", ix.ReplayLSN, ix.Runs)
				}
			case ownerReplica:
				if ix.ReplayLSN != second || len(ix.Runs) != 2 {
					t.Fatalf("replica replays after LSN %d with runs %+v", ix.ReplayLSN, ix.Runs)
				}
			}
		}

		o.put(all...)
		h.submit(disklog.Hot, o.item(ownerIndexA))
		inFlight := []disklog.Item{o.item(ownerIndexB), o.item(ownerReplica), o.item(ownerIndexA)}
		for _, it := range inFlight {
			h.submit(disklog.Hot, it)
		}
		h.round()
		h.step(0)
		acked := h.log.DurableLSN()
		want := map[uint32][]fact{}
		for owner, facts := range o.live {
			want[owner] = slices.Clone(facts)
		}
		if landed {
			for _, it := range inFlight {
				owner, _ := ownerOf(it.Entry)
				want[owner] = append(want[owner], fact{acked + 1, it.Entry})
			}
		}
		h.disk.Crash(func(int, core.LBA) bool { return landed })
		durable, got := o.recover()
		wantLSN := acked
		if landed {
			wantLSN++
		}
		if durable != wantLSN {
			t.Fatalf("landed %v: recovered LSN %d, acknowledged %d", landed, durable, acked)
		}
		for _, owner := range all {
			if !slices.Equal(got[owner], want[owner]) {
				t.Fatalf("landed %v: owner %d recovered %d facts to LSN %d, want %d", landed, owner, len(got[owner]), durable, len(want[owner]))
			}
		}
	}
}

// headerAt decodes the record header at lba as a record of this log at that place.
func (h *harness) headerAt(lba core.LBA) (disklog.RecordHeader, error) {
	seg, block := uint32(uint64(lba)/uint64(h.geo.SegmentBlocks)), uint32(uint64(lba)%uint64(h.geo.SegmentBlocks))
	left := h.geo.SegmentBlocks - block
	place := disklog.Place{LogID: h.logID, Seq: h.segmentSeq(seg), Offset: block}
	return disklog.DecodeHeader(h.read(lba, uint64(left)), place, left, nil)
}

// stepWrites executes every pending write and none of the flushes.
func (h *harness) stepWrites() {
	h.t.Helper()
	for i := 0; i < len(h.pending); {
		if h.pending[i].act.Kind == core.ActDiskWrite {
			h.step(i)
			continue
		}
		i++
	}
}

func TestRecoverEndsTheLogOnlyWhereNoLaterHeaderNamesItDurable(t *testing.T) {
	const lsn4, lsn5 = core.LBA(74), core.LBA(77)
	inFlight := func(durable int) func(h *harness) {
		return func(h *harness) {
			h.twoBlockWrites(durable)
			for i := durable; i < 5; i++ {
				h.submit(disklog.Hot, h.chunkWrite(uint32(i), 0, 2, uint64(i+1)))
			}
			h.stepWrites()
		}
	}
	allDurable := func(h *harness) { h.twoBlockWrites(5) }
	cases := []struct {
		name         string
		depth, open  int
		write        func(h *harness)
		lose         func(h *harness)
		names        map[core.LBA]uint64
		unreadable   []core.LBA
		lsns         []uint64
		corrupt      uint64
		zeroedHeader []core.LBA
	}{
		{
			name: "depth 1, record 5 torn", depth: 1, open: 1, write: inFlight(4),
			lose:  func(h *harness) { h.crashLanding(77, 78) },
			names: map[core.LBA]uint64{lsn5: 4}, lsns: lsnRange(1, 4),
		},
		{
			name: "depth 1, record 4 damaged", depth: 1, open: 1, write: allDurable,
			lose:  func(h *harness) { h.disk.FlipBit(75, 100, 0x80) },
			names: map[core.LBA]uint64{lsn5: 4}, corrupt: 4,
		},
		{
			name: "depth 1, record 3 damaged", depth: 1, open: 1, write: allDurable,
			lose:  func(h *harness) { h.disk.FlipBit(72, 100, 0x80) },
			names: map[core.LBA]uint64{lsn4: 3, lsn5: 4}, corrupt: 3,
		},
		{
			name: "depth 2, round 5 begun after round 4 was durable, record 4 damaged", depth: 2, open: 2, write: allDurable,
			lose:  func(h *harness) { h.disk.FlipBit(75, 100, 0x80) },
			names: map[core.LBA]uint64{lsn5: 4}, corrupt: 4,
		},
		{
			name: "depth 2, rounds 4 and 5 in flight, record 4 missing", depth: 2, open: 2, write: inFlight(3),
			lose:  func(h *harness) { h.crashLanding(77, 78, 79) },
			names: map[core.LBA]uint64{lsn5: 3}, lsns: lsnRange(1, 3), zeroedHeader: []core.LBA{lsn5},
		},
		{
			name: "depth 2, rounds 4 and 5 in flight, record 5 missing", depth: 2, open: 2, write: inFlight(3),
			lose:  func(h *harness) { h.crashLanding(74, 75, 76) },
			names: map[core.LBA]uint64{lsn4: 3}, lsns: lsnRange(1, 4),
		},
		{
			name: "written at depth 1, opened at depth 2, record 4 damaged", depth: 1, open: 2, write: allDurable,
			lose:  func(h *harness) { h.disk.FlipBit(75, 100, 0x80) },
			names: map[core.LBA]uint64{lsn5: 4}, corrupt: 4,
		},
		{
			name: "depth 3, rounds 3, 4 and 5 in flight, record 3 torn", depth: 3, open: 3, write: inFlight(2),
			lose:  func(h *harness) { h.crashLanding(71, 72, 74, 75, 76, 77, 78, 79) },
			names: map[core.LBA]uint64{71: 2, lsn4: 2, lsn5: 2}, lsns: lsnRange(1, 2), zeroedHeader: []core.LBA{lsn4, lsn5},
		},
		{
			name: "depth 2, rounds 4 and 5 in flight, record 4 damaged, record 5's prelude torn", depth: 2, open: 2, write: inFlight(3),
			lose: func(h *harness) {
				h.crashLanding(74, 75, 77, 78, 79)
				b := h.read(lsn5, 1)
				clear(b[40:])
				h.disk.Put(lsn5, b)
			},
			names: map[core.LBA]uint64{lsn4: 3}, unreadable: []core.LBA{lsn5}, lsns: lsnRange(1, 3),
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := testConfig(0)
			cfg.CommitDepth = c.depth
			h := formatted(t, gib, cfg)
			c.write(h)
			c.lose(h)
			for lba, want := range c.names {
				hdr, err := h.headerAt(lba)
				if err != nil || hdr.Record.Durable != want {
					t.Fatalf("header at LBA %d: %+v, %v; want it to name %d durable", lba, hdr.Record, err, want)
				}
			}
			for _, lba := range c.unreadable {
				if hdr, err := h.headerAt(lba); err == nil {
					t.Fatalf("header at LBA %d reads as LSN %d", lba, hdr.Record.LSN)
				}
			}
			h.cfg.CommitDepth = c.open
			if c.corrupt != 0 {
				wantCorrupt(t, h.recover(disklog.ModeOpen), c.corrupt)
				wantCorrupt(t, h.recover(disklog.ModeRebuild), c.corrupt)
				return
			}
			h.wantLSNs(h.reopen(), c.lsns)
			for _, lba := range c.zeroedHeader {
				if !isZeroBlock(h.read(lba, 1)) {
					t.Fatalf("header at LBA %d was not erased", lba)
				}
			}
			last := c.lsns[len(c.lsns)-1]
			h.put(disklog.Hot, h.chunkWrite(9, 0, 1, 9))
			d := h.eventsOf(disklog.Durable)
			if len(d) != 1 || d[0].LSN != last+1 {
				t.Fatalf("next write after recovering LSN %d: %+v", last, d)
			}
			if got := h.record(d[0]).Record.Durable; got != last {
				t.Fatalf("LSN %d names %d durable, want the recovered %d", d[0].LSN, got, last)
			}
		})
	}
}
