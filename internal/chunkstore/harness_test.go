package chunkstore

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/lsm"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

const testPool core.PoolID = 1

// harness runs a store over a simulated disk. Actions queue in the order the store emits them and
// run one at a time, oldest first, so operations pipeline behind the rounds in flight; replies
// are kept by request ID.
type harness struct {
	t       testing.TB
	pool    *sim.Pool
	ctx     core.Context
	disk    *sim.Disk
	cfg     Config
	s       *Store
	out     Output
	queue   []core.Action
	replies map[uint64]Reply
	order   []uint64
	nextID  uint64
	acts    []core.Action
	seqOf   map[uint32]uint64
	// slotData is each chunk slot's data as its newest write sent it, by the slot's first LBA.
	slotData map[core.LBA][]byte
	records  []writtenRecord
	oracle   *sim.Oracle[writtenRecord, *storeModel]
	// recovered is what the oracle expects of the last recovery.
	recovered *storeModel
	acked     uint64
	// executed counts the actions run since format; the step that would run action crashAt panics with errCrash instead.
	executed int
	crashAt  int
	// crashIf, when set, crashes as crashAt does before an oldest queued action it accepts;
	// failIf, when set, completes with Io action k, counting from 0, if it accepts it; and onStep,
	// when set, sees action k once the store has taken its completion.
	crashIf func(a *core.Action) bool
	failIf  func(k int, a *core.Action) bool
	onStep  func(k int, a *core.Action)
	// plant is the bug planted in every store a recovery starts.
	plant plants
	// changes is the chunk of each write and punch, by request ID.
	changes map[uint64]core.ChunkID
	// abandoned is the pool buffers left live by stores the harness dropped at a crash, and own
	// those a settled store holds for its log once formatted.
	abandoned, own int
	// merging runs the index's due merge steps after every action, as the disk's owner does;
	// steps counts each merge's installed steps, merges in the order they started.
	merging bool
	steps   map[*lsm.Compaction]int
	merges  []*lsm.Compaction
	// laneOf is each metadata segment's lane, and stepHeld the run bytes each merge worker's lane
	// wrote since the last manifest, which never pass one step and its partition and root.
	laneOf      map[uint32]uint8
	stepHeld    map[uint8]uint64
	maxStepHeld uint64
	// stepBlocks counts the blocks of the records merge workers' lanes and manifests take, with the
	// segment headers they open.
	stepBlocks uint64
	// trims is every TRIM executed, timerAt the time the log's TRIM timer was last armed for, and
	// admitAt the store's AdmitTimer.
	trims            []core.Action
	timerAt, admitAt core.Tick
	// shuffle, when set, runs a queued action at random rather than the oldest, as an NVMe queue
	// may complete the commands it holds in any order but a flush.
	shuffle *core.Rand
	// maxCleanReads is the most reads a cleaner pass has had in flight, and maxMerging the most
	// merges begun and not finished at once.
	maxCleanReads, maxMerging int
	// held is the cleaner jobs taken from the store and not yet run: with late set, each runs
	// after a random number of later actions, as a worker may finish after the owner has taken
	// other events, and with hold set, only when a test runs it.
	held []*CleanJob
	late *core.Rand
	hold bool
	// timed moves the clock on by each disk action's simulated time as it completes, and light
	// keeps neither the actions, the records written nor the merges run, for runs that write a
	// disk many times.
	timed, light bool
	// wrote counts the blocks of every disk write run, and wroteMeta those in the metadata stream.
	wrote, wroteMeta uint64
	// recoveredLSN is the LSN the last recovery found, and loaded the manifest it loaded.
	recoveredLSN uint64
	loaded       disklog.ManifestPos
}

// advance moves the clock on by d, firing the log's TRIM timer if it falls due, and settles.
func (h *harness) advance(d core.Tick) {
	h.t.Helper()
	h.tick(d)
	h.settle()
}

// tick moves the clock on by d and fires the log's TRIM timer and the store's AdmitTimer if they
// fall due.
func (h *harness) tick(d core.Tick) {
	h.ctx.Now += d
	h.ctx.Raw += d
	if h.timerAt != 0 && h.timerAt <= h.ctx.Now {
		h.timerAt = 0
		h.s.OnEvent(&h.ctx, &core.Event{Kind: core.EvTimer, Timer: disklog.TrimTimer}, &h.out)
		h.take()
	}
	if h.admitAt != 0 && h.admitAt <= h.ctx.Now {
		h.admitAt = 0
		h.s.OnEvent(&h.ctx, &core.Event{Kind: core.EvTimer, Timer: AdmitTimer}, &h.out)
		h.take()
	}
}

var errCrash = errors.New("crash")

// writtenRecord is a record a round wrote: its LSN, entries and each entry's payload.
type writtenRecord struct {
	lsn      uint64
	entries  []disklog.Entry
	payloads [][]byte
}

func testConfig() Config {
	cfg := Config{Log: disklog.DefaultConfig(), Pool: testPool, Check: true}
	cfg.Log.MaxReplayBytes = 1 << 40
	cfg.Log.Check = true
	cfg.Log.NoReserve = true
	return cfg
}

func newHarness(t testing.TB, cfg Config) *harness {
	geo, err := disklog.GeometryFor(diskBytes)
	if err != nil {
		t.Fatal(err)
	}
	return newHarnessOn(t, cfg, geo)
}

// slotGeometry is a disk of 40 segments of 16 MiB, each holding 4 chunk slots.
var slotGeometry = disklog.Geometry{SegmentBlocks: 4096, SegmentCount: 40}

// newHarnessOn is newHarness on a disk cut as geo.
func newHarnessOn(t testing.TB, cfg Config, geo disklog.Geometry) *harness {
	h := &harness{t: t, pool: sim.NewPool(32<<20, 0), disk: sim.NewDisk(geo.DiskBlocks()), cfg: cfg,
		replies: map[uint64]Reply{}, seqOf: map[uint32]uint64{}, steps: map[*lsm.Compaction]int{}, laneOf: map[uint32]uint8{}, stepHeld: map[uint8]uint64{}, slotData: map[core.LBA][]byte{}, crashAt: -1}
	h.ctx = core.Context{Now: 1, Raw: 2, Pool: h.pool}
	var err error
	if h.s, err = Format(&h.ctx, cfg, disklog.Superblock{LogID: 0x5eed, Geometry: geo}, &h.out); err != nil {
		t.Fatal(err)
	}
	h.take()
	h.settle()
	h.executed, h.own = 0, h.pool.Live()
	return h
}

// take moves the store's actions to the queue and its replies to the reply book.
func (h *harness) take() {
	h.queue = append(h.queue, h.out.Actions...)
	if !h.light {
		h.acts = append(h.acts, h.out.Actions...)
	}
	for _, r := range h.out.Replies {
		if _, dup := h.replies[r.ID]; dup {
			h.t.Fatalf("request %d answered twice", r.ID)
		}
		h.replies[r.ID] = r
		h.order = append(h.order, r.ID)
	}
	h.out.Reset()
}

// step runs the oldest queued action and hands its completion to the store, checking the
// open sequences on disk when that ends a recovery.
func (h *harness) step() bool {
	if len(h.queue) == 0 {
		if h.hold || len(h.held) == 0 {
			return false
		}
		h.runClean(0)
		return true
	}
	if h.executed == h.crashAt || h.crashIf != nil && h.crashIf(&h.queue[0]) {
		panic(errCrash)
	}
	h.executed++
	a := h.queue[0]
	if h.shuffle != nil {
		a = h.unordered()
	} else {
		h.queue = h.queue[1:]
	}
	if a.Kind == core.ActSetTimer && a.Timer == AdmitTimer {
		h.admitAt = a.At
		return true
	}
	if a.Kind == core.ActSetTimer {
		h.timerAt = a.At
		return true
	}
	if a.Kind == core.ActDiskTrim {
		h.trims = append(h.trims, a)
	}
	if a.Kind == core.ActDiskWrite {
		h.wrote += a.Len / core.BlockSize
		if h.s.Serving() {
			if _, st := h.s.Log().Usage(uint32(uint64(a.LBA) / uint64(h.s.sb.SegmentBlocks))); st == disklog.Meta {
				h.wroteMeta += a.Len / core.BlockSize
			}
		}
		if !h.light {
			h.noteWrite(&a)
		}
	}
	if h.failIf != nil && h.failIf(h.executed-1, &a) {
		io := a.IO
		h.disk.Inject(sim.Fault{Effect: sim.FailIO, When: sim.Once(func(op sim.DiskOp, _ *core.Rand) bool { return op.IO == io })})
	}
	ev, took := h.disk.ExecuteTimed(h.pool, &a)
	if h.timed {
		h.tick(took)
	}
	recovering := !h.s.Serving()
	h.s.OnEvent(&h.ctx, &ev, &h.out)
	if recovering && h.s.Serving() {
		checkOpenSeqs(h.t, h.disk, h.s.Log())
		clear(h.stepHeld)
	}
	if recovering && h.s.Serving() && h.oracle != nil {
		m, err := h.oracle.Recovered(h.s.Log().DurableLSN())
		if err != nil {
			h.t.Fatal(err)
		}
		h.recovered, h.acked = m, h.s.Log().DurableLSN()
		h.recoveredLSN, h.loaded = h.acked, h.s.Log().Manifest()
	}
	h.take()
	h.noteDurable()
	h.noteCleanReads()
	for c := h.mergeDue(); c != nil; c = h.mergeDue() {
		c.Run()
		if err := h.s.InstallMerge(&h.ctx, c, &h.out); err != nil {
			h.t.Fatal(err)
		}
		if h.light {
			h.take()
			continue
		}
		if h.steps[c] == 0 {
			h.merges = append(h.merges, c)
		}
		h.steps[c]++
		running := 0
		for _, m := range h.merges[max(0, len(h.merges)-8):] {
			if !m.Finished() {
				running++
			}
		}
		h.maxMerging = max(h.maxMerging, running)
		h.take()
	}
	for j := h.s.CleanDue(); j != nil; j = h.s.CleanDue() {
		h.held = append(h.held, j)
	}
	for i := 0; i < len(h.held) && !h.hold; {
		if h.late != nil && h.late.Below(4) != 0 {
			i++
			continue
		}
		h.runClean(i)
	}
	if h.onStep != nil {
		h.onStep(h.executed-1, &a)
	}
	return true
}

// runClean runs the held cleaner job i, as its worker does, and hands it back to the store.
func (h *harness) runClean(i int) {
	j := h.held[i]
	h.held = slices.Delete(h.held, i, i+1)
	j.Run(h.pool)
	h.s.InstallClean(&h.ctx, j, &h.out)
	h.take()
}

// unordered takes a queued action at random, except that a flush completes after the writes
// queued before it, as a driver's flush command does.
func (h *harness) unordered() core.Action {
	k := int(h.shuffle.Below(uint64(len(h.queue))))
	if h.queue[k].Kind == core.ActDiskFsync && slices.ContainsFunc(h.queue[:k], func(a core.Action) bool { return a.Kind == core.ActDiskWrite }) {
		k = 0
	}
	a := h.queue[k]
	h.queue = slices.Delete(h.queue, k, k+1)
	return a
}

// noteCleanReads fails when a cleaner pass has more reads in flight than the cleaner's read
// depth, and keeps the most it has seen.
func (h *harness) noteCleanReads() {
	for _, p := range h.s.cl.passes {
		if p.job.reads > cleanReadDepth {
			h.t.Fatalf("a cleaner pass has %d reads in flight, above its depth of %d", p.job.reads, cleanReadDepth)
		}
		h.maxCleanReads = max(h.maxCleanReads, p.job.reads)
	}
}

func (h *harness) mergeDue() *lsm.Compaction {
	if !h.merging {
		return nil
	}
	return h.s.MergeDue()
}

// checkOpenSeqs fails unless log opens its next segment past the open sequence of every segment
// header of its own on disk, so that open sequences stay unique and increasing.
func checkOpenSeqs(t testing.TB, disk *sim.Disk, log *disklog.Log) {
	t.Helper()
	sb := log.Superblock()
	for s := uint32(1); s < sb.SegmentCount; s++ {
		sh, err := disklog.DecodeSegmentHeader(disk.Read(sb.LBA(s, 0), 1))
		if err == nil && sh.LogID == sb.LogID && sh.Index == s && sh.Seq >= log.NextOpenSeq() {
			t.Fatalf("the recovered log opens its next segment at sequence %d, and segment %d's header on disk is at %d", log.NextOpenSeq(), s, sh.Seq)
		}
	}
}

// noteSteps adds the run pieces a record on a merge worker's lane holds to that worker's step,
// and starts every worker's next step at a manifest.
func (h *harness) noteSteps(segment uint32, entries []disklog.Entry) {
	lane, ok := h.laneOf[segment]
	for _, e := range entries {
		switch {
		case e.Kind != disklog.EntryBlob:
		case e.BlobKind == disklog.BlobManifest:
			clear(h.stepHeld)
		case e.BlobKind == disklog.BlobRun && ok && lane > 0:
			h.stepHeld[lane] += e.Length
			h.maxStepHeld = max(h.maxStepHeld, h.stepHeld[lane])
			piece := h.s.Log().MaxBlobBytes()
			if most := max(h.cfg.Log.CompactionStepBytes, piece) + 2*piece; h.stepHeld[lane] > most {
				h.t.Fatalf("merge worker %d wrote %d bytes of runs since the last manifest, above a step's %d", lane, h.stepHeld[lane], most)
			}
		}
	}
}

// settleSteps bounds a settle, so that a store that never goes quiet fails rather than runs away.
const settleSteps = 100_000

func (h *harness) settle() {
	for n := 0; h.step(); n++ {
		if n == settleSteps {
			h.t.Fatalf("the store did not settle within %d actions: %+v", settleSteps, h.s.CleanStats())
		}
	}
	h.checkUsage()
}

// checkUsage checks that every segment outside the metadata stream counts as many live blocks
// as the index's records in it refer to, and the index as many slot-held ones as its slots' counts.
func (h *harness) checkUsage() {
	h.t.Helper()
	if !h.s.Serving() {
		return
	}
	geo := h.s.sb.Geometry
	want := make([]uint32, geo.SegmentCount)
	h.s.Index().SegmentLive(geo.SegmentBlocks, want)
	held := make([]uint32, geo.SegmentCount)
	h.s.Index().SegmentSlotHeld(held)
	for s := uint32(1); s < geo.SegmentCount; s++ {
		u, stream := h.s.Log().Usage(s)
		if stream != disklog.Meta && u.State == disklog.SegmentUsed && u.Live != want[s] {
			h.t.Fatalf("segment %d counts %d live blocks, the index's records in it %d", s, u.Live, want[s])
		}
		if got := h.s.Index().SlotHeld(s); got != held[s] || got > want[s] {
			h.t.Fatalf("segment %d holds %d slot blocks' headers, the slots' counts %d, of %d live", s, got, held[s], want[s])
		}
	}
}

func (h *harness) noteDurable() {
	if h.oracle == nil || h.s.Log() == nil || h.s.Log().DurableLSN() <= h.acked {
		return
	}
	h.acked = h.s.Log().DurableLSN()
	if err := h.oracle.Acked(h.acked); err != nil {
		h.t.Fatal(err)
	}
}

// noteWrite decodes the record a round writes, taking each segment's open sequence from the
// write that opened it.
func (h *harness) noteWrite(a *core.Action) {
	var img []byte
	for _, v := range a.Vec {
		img = append(img, h.pool.Bytes(v.Buf)[:v.Len]...)
	}
	geo := h.s.sb.Geometry
	if h.s.sb.LogID == 0 || len(img) < core.BlockSize {
		return
	}
	if a.Len == core.ChunkSize {
		if h.oracle != nil {
			h.slotData[a.LBA] = img
		}
		return
	}
	lba := a.LBA
	segment := uint32(uint64(lba) / uint64(geo.SegmentBlocks))
	if sh, err := disklog.DecodeSegmentHeader(img); err == nil && sh.LogID == h.s.sb.LogID {
		h.seqOf[segment] = sh.Seq
		if sh.Stream == disklog.Meta {
			h.laneOf[segment] = sh.Lane
		} else {
			delete(h.laneOf, segment)
		}
		img, lba = img[core.BlockSize:], lba+1
	}
	place := disklog.Place{LogID: h.s.sb.LogID, Seq: h.seqOf[segment], Offset: uint32(uint64(lba) % uint64(geo.SegmentBlocks))}
	d, err := disklog.DecodeRecord(img, place, math.MaxUint32, nil)
	if err != nil {
		return
	}
	if lane, ok := h.laneOf[segment]; ok && (lane > 0 || slices.ContainsFunc(d.Entries, func(e disklog.Entry) bool {
		return e.Kind == disklog.EntryBlob && (e.BlobKind == disklog.BlobManifest || e.BlobKind == disklog.BlobManifestPiece)
	})) {
		h.stepBlocks += a.Len / core.BlockSize
	}
	rec := writtenRecord{lsn: d.Record.LSN, entries: slices.Clone(d.Entries)}
	for i, e := range d.Entries {
		p := slices.Clone(d.Payload(i))
		if e.Kind == disklog.EntrySlot {
			p = h.slotData[geo.SlotLBA(e.Segment, e.Slot)]
		}
		rec.payloads = append(rec.payloads, p)
	}
	h.records = append(h.records, rec)
	h.noteSteps(segment, d.Entries)
	if h.oracle != nil {
		if err := h.oracle.Written(rec.lsn, rec); err != nil {
			h.t.Fatal(err)
		}
	}
}

func (h *harness) id() uint64 {
	h.nextID++
	return h.nextID
}

// change is a new request ID for a write or punch of chunk c.
func (h *harness) change(c core.ChunkID) uint64 {
	if h.changes == nil {
		h.changes = map[uint64]core.ChunkID{}
	}
	id := h.id()
	h.changes[id] = c
	return id
}

// writeBuf lays data out at off in a buffer of the blocks it touches, the rest poisoned.
func (h *harness) writeBuf(off uint32, data []byte) core.IOBuf {
	first, last := spanOf(off, uint32(len(data)))
	buf, err := h.pool.Alloc((int(last) - int(first) + 1) * core.BlockSize)
	if err != nil {
		h.t.Fatal(err)
	}
	copy(h.pool.Bytes(buf)[off%core.BlockSize:], data)
	return buf
}

func (h *harness) write(c core.ChunkID, off uint32, data []byte, epoch uint64) uint64 {
	id := h.change(c)
	h.s.Write(&h.ctx, Request{ID: id, Chunk: c, Off: off, Len: uint32(len(data)), Buf: h.writeBuf(off, data), Epoch: epoch}, &h.out)
	h.take()
	return id
}

func (h *harness) writeExpect(c core.ChunkID, off uint32, data []byte, ver uint64) uint64 {
	id := h.change(c)
	h.s.Write(&h.ctx, Request{ID: id, Chunk: c, Off: off, Len: uint32(len(data)), Buf: h.writeBuf(off, data), HasVer: true, Ver: ver}, &h.out)
	h.take()
	return id
}

func (h *harness) punch(c core.ChunkID, off, n uint32, epoch uint64) uint64 {
	id := h.change(c)
	h.s.Punch(&h.ctx, Request{ID: id, Chunk: c, Off: off, Len: n, Epoch: epoch}, &h.out)
	h.take()
	return id
}

func (h *harness) read(c core.ChunkID, off, n uint32) uint64 {
	id := h.id()
	h.s.Read(&h.ctx, Request{ID: id, Chunk: c, Off: off, Len: n}, &h.out)
	h.take()
	return id
}

func (h *harness) readAt(c core.ChunkID, off, n uint32, ver uint64) uint64 {
	id := h.id()
	h.s.Read(&h.ctx, Request{ID: id, Chunk: c, Off: off, Len: n, HasVer: true, Ver: ver}, &h.out)
	h.take()
	return id
}

// reply returns the reply to id, which must have come.
func (h *harness) reply(id uint64) Reply {
	h.t.Helper()
	r, ok := h.replies[id]
	if !ok {
		h.t.Fatalf("request %d not answered", id)
	}
	return r
}

// sync settles, then returns the version request id got.
func (h *harness) sync(id uint64) uint64 {
	h.t.Helper()
	h.settle()
	return h.ok(id)
}

func (h *harness) ok(id uint64) uint64 {
	h.t.Helper()
	r := h.reply(id)
	if r.Err != nil {
		h.t.Fatalf("request %d: %v", id, r.Err)
	}
	return r.Version
}

// bytesOf returns a read reply's bytes and frees its buffer.
func (h *harness) bytesOf(id uint64, off, n uint32) []byte {
	h.t.Helper()
	r := h.reply(id)
	if r.Err != nil {
		h.t.Fatalf("read %d: %v", id, r.Err)
	}
	b := slices.Clone(h.pool.Bytes(r.Buf)[off%core.BlockSize:][:n])
	h.pool.Free(r.Buf)
	return b
}

// readNow reads through the store and returns the bytes.
func (h *harness) readNow(c core.ChunkID, off, n uint32) []byte {
	h.t.Helper()
	id := h.read(c, off, n)
	h.settle()
	return h.bytesOf(id, off, n)
}

// block reads block b of the chunk from the index and the disk, as the snapshot of epoch reads it
// when epoch is not 0.
func (h *harness) block(c core.ChunkID, b uint16, epoch uint64) []byte {
	ix := h.s.Index()
	e, ok := ix.Lookup(testPool, c, b)
	if epoch != 0 {
		e, ok = ix.LookupAt(testPool, c, b, epoch)
	}
	if !ok || e.LBA == 0 {
		return make([]byte, core.BlockSize)
	}
	return h.disk.Read(e.BlockLBA(b), 1)
}

// actions counts the store's actions of a kind since mark, reads of blocks only.
func (h *harness) actions(mark int, kind core.ActionKind) int {
	n := 0
	for _, a := range h.acts[mark:] {
		if a.Kind == kind {
			n++
		}
	}
	return n
}

// logged lists the entries of the records written since mark that name chunk c, in log order.
func (h *harness) logged(mark int, c core.ChunkID) []string {
	var out []string
	for _, r := range h.records[mark:] {
		for _, e := range r.entries {
			if e.Kind == disklog.EntryBlob || e.Chunk != c {
				continue
			}
			switch e.Kind {
			case disklog.EntryWrite:
				out = append(out, fmt.Sprintf("write %d+%d v%#x", e.First, e.Count, e.Version))
			case disklog.EntrySlot:
				out = append(out, fmt.Sprintf("slot %d of %d v%#x", e.Slot, e.Segment, e.Version))
			case disklog.EntryPunch:
				out = append(out, fmt.Sprintf("punch %d+%d v%#x", e.First, e.Count, e.Version))
			case disklog.EntryFreeze:
				out = append(out, fmt.Sprintf("freeze v%#x e%d", e.Version, e.Epoch))
			}
		}
	}
	return out
}

// byteOracle applies every acknowledged write's and punch's bytes in version order, and keeps
// each snapshot epoch's bytes as the last write before its cut left them.
type byteOracle struct {
	chunks map[core.ChunkID]*oracleChunk
}

type oracleChunk struct {
	version uint64
	epoch   uint64
	live    []byte
	snaps   map[uint64][]byte
}

func newByteOracle() *byteOracle { return &byteOracle{chunks: map[core.ChunkID]*oracleChunk{}} }

func (o *byteOracle) chunk(c core.ChunkID) *oracleChunk {
	ch, ok := o.chunks[c]
	if !ok {
		ch = &oracleChunk{live: make([]byte, core.ChunkSize), snaps: map[uint64][]byte{}}
		o.chunks[c] = ch
	}
	return ch
}

// apply records an acknowledged write (data) or punch (nil data) of version at epoch.
func (o *byteOracle) apply(t testing.TB, c core.ChunkID, off, n uint32, data []byte, version, epoch uint64) {
	t.Helper()
	ch := o.chunk(c)
	if version <= ch.version {
		t.Fatalf("chunk %v acknowledged version %#x after %#x", c, version, ch.version)
	}
	for e := ch.epoch + 1; e <= epoch; e++ {
		ch.snaps[e] = slices.Clone(ch.live)
	}
	ch.version, ch.epoch = version, max(ch.epoch, epoch)
	if data == nil {
		clear(ch.live[off : off+n])
	} else {
		copy(ch.live[off:], data)
	}
}

// at is the chunk's bytes as the snapshot of epoch reads them, or live for epoch 0.
func (ch *oracleChunk) at(epoch uint64) []byte {
	if epoch == 0 || epoch > ch.epoch {
		return ch.live
	}
	return ch.snaps[epoch]
}

// diff compares blocks [first, last] of every chunk's live bytes with the store and describes the first difference.
func (o *byteOracle) diff(h *harness, first, last uint16) string {
	for c, ch := range o.chunks {
		for b := first; b <= last; b++ {
			got, want := h.block(c, b, 0), ch.live[int(b)*core.BlockSize:][:core.BlockSize]
			if i := firstDiff(got, want); i < core.BlockSize {
				return fmt.Sprintf("chunk %v block %d byte %d is %#x, the oracle holds %#x", c, b, i, got[i], want[i])
			}
		}
	}
	return ""
}

// check compares blocks [first, last] of every chunk, live and at each epoch up to maxEpoch, with the store's index and disk.
func (o *byteOracle) check(t testing.TB, h *harness, first, last uint16, maxEpoch uint64) {
	t.Helper()
	for c, ch := range o.chunks {
		for epoch := range maxEpoch + 1 {
			want := ch.at(epoch)
			for b := first; b <= last; b++ {
				got := h.block(c, b, epoch)
				if w := want[int(b)*core.BlockSize:][:core.BlockSize]; !bytes.Equal(got, w) {
					i := 0
					for got[i] == w[i] {
						i++
					}
					t.Fatalf("chunk %v block %d at epoch %d: byte %d is %#x, the oracle holds %#x", c, b, epoch, i, got[i], w[i])
				}
			}
		}
	}
}
