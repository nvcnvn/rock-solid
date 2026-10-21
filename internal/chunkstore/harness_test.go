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
	records []writtenRecord
	oracle  *sim.Oracle[writtenRecord, *blockModel]
	// recovered is what the oracle expects of the last recovery.
	recovered *blockModel
	acked     uint64
	// executed counts the actions run since format; the step that would run action crashAt panics with errCrash instead.
	executed int
	crashAt  int
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
	h := &harness{t: t, pool: sim.NewPool(32<<20, 0), disk: sim.NewDisk(diskBytes / core.BlockSize), cfg: cfg,
		replies: map[uint64]Reply{}, seqOf: map[uint32]uint64{}, crashAt: -1}
	h.ctx = core.Context{Now: 1, Raw: 2, Pool: h.pool}
	geo, err := disklog.GeometryFor(diskBytes)
	if err != nil {
		t.Fatal(err)
	}
	if h.s, err = Format(&h.ctx, cfg, disklog.Superblock{LogID: 0x5eed, Geometry: geo}, &h.out); err != nil {
		t.Fatal(err)
	}
	h.take()
	h.settle()
	h.executed = 0
	return h
}

// take moves the store's actions to the queue and its replies to the reply book.
func (h *harness) take() {
	h.queue = append(h.queue, h.out.Actions...)
	h.acts = append(h.acts, h.out.Actions...)
	for _, r := range h.out.Replies {
		if _, dup := h.replies[r.ID]; dup {
			h.t.Fatalf("request %d answered twice", r.ID)
		}
		h.replies[r.ID] = r
		h.order = append(h.order, r.ID)
	}
	h.out.Reset()
}

// step runs the oldest queued action and hands its completion to the store.
func (h *harness) step() bool {
	if len(h.queue) == 0 {
		return false
	}
	if h.executed == h.crashAt {
		panic(errCrash)
	}
	h.executed++
	a := h.queue[0]
	h.queue = h.queue[1:]
	if a.Kind == core.ActDiskWrite {
		h.noteWrite(&a)
	}
	ev := h.disk.Execute(h.pool, &a)
	recovering := !h.s.Serving()
	h.s.OnEvent(&h.ctx, &ev, &h.out)
	if recovering && h.s.Serving() && h.oracle != nil {
		m, err := h.oracle.Recovered(h.s.Log().DurableLSN())
		if err != nil {
			h.t.Fatal(err)
		}
		h.recovered, h.acked = m, h.s.Log().DurableLSN()
	}
	h.take()
	h.noteDurable()
	return true
}

func (h *harness) settle() {
	for h.step() {
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
	lba := a.LBA
	segment := uint32(uint64(lba) / uint64(geo.SegmentBlocks))
	if sh, err := disklog.DecodeSegmentHeader(img); err == nil && sh.LogID == h.s.sb.LogID {
		h.seqOf[segment] = sh.Seq
		img, lba = img[core.BlockSize:], lba+1
	}
	place := disklog.Place{LogID: h.s.sb.LogID, Seq: h.seqOf[segment], Offset: uint32(uint64(lba) % uint64(geo.SegmentBlocks))}
	d, err := disklog.DecodeRecord(img, place, math.MaxUint32, nil)
	if err != nil {
		return
	}
	rec := writtenRecord{lsn: d.Record.LSN, entries: slices.Clone(d.Entries)}
	for i := range d.Entries {
		rec.payloads = append(rec.payloads, slices.Clone(d.Payload(i)))
	}
	h.records = append(h.records, rec)
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
	id := h.id()
	h.s.Write(&h.ctx, Request{ID: id, Chunk: c, Off: off, Len: uint32(len(data)), Buf: h.writeBuf(off, data), Epoch: epoch}, &h.out)
	h.take()
	return id
}

func (h *harness) writeExpect(c core.ChunkID, off uint32, data []byte, ver uint64) uint64 {
	id := h.id()
	h.s.Write(&h.ctx, Request{ID: id, Chunk: c, Off: off, Len: uint32(len(data)), Buf: h.writeBuf(off, data), HasVer: true, Ver: ver}, &h.out)
	h.take()
	return id
}

func (h *harness) punch(c core.ChunkID, off, n uint32, epoch uint64) uint64 {
	id := h.id()
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
