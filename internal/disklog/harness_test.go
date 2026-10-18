package disklog_test

import (
	"encoding/binary"
	"fmt"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

const (
	testLogID  = 0x0123456789abcdef
	gib        = 1 << 30
	firstIO    = 100
	retryTimer = 4
	retryDelay = 50
)

type pendingIO struct {
	act    core.Action
	linked bool
	link   core.IOID
}

// harness drives a Log against an in-memory disk: it executes the log's actions when told
// to, feeds back their completions, and records the action trace and every event.
type harness struct {
	t       testing.TB
	pool    *sim.Pool
	ctx     core.Context
	geo     disklog.Geometry
	log     *disklog.Log
	out     disklog.Output
	blocks  map[core.LBA][]byte
	pending []pendingIO
	trace   []string
	events  []disklog.Event
	failIO  map[core.IOID]core.DiskError
	timer   bool

	completed map[core.IOID]bool
	durable   map[core.IOID]bool
	writeAt   map[core.LBA]core.IOID
	writes    int
	fsyncs    int
	unlinked  int
}

func newHarness(t testing.TB, diskBytes uint64, poolBytes int, cfg disklog.Config) *harness {
	t.Helper()
	geo, err := disklog.GeometryFor(diskBytes)
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{
		t:         t,
		pool:      sim.NewPool(poolBytes, 0),
		geo:       geo,
		blocks:    map[core.LBA][]byte{},
		failIO:    map[core.IOID]core.DiskError{},
		completed: map[core.IOID]bool{},
		durable:   map[core.IOID]bool{},
		writeAt:   map[core.LBA]core.IOID{},
	}
	h.ctx = core.Context{Now: 1000, Pool: h.pool}
	cfg.RetryTimer = retryTimer
	cfg.RetryDelay = retryDelay
	cfg.Check = true
	h.log, err = disklog.Format(&h.ctx, cfg, disklog.Superblock{LogID: testLogID, Geometry: geo}, firstIO, &h.out)
	if err != nil {
		t.Fatal(err)
	}
	h.absorb()
	return h
}

func testConfig(maxRecordBlocks uint64) disklog.Config {
	cfg := disklog.DefaultConfig()
	if maxRecordBlocks > 0 {
		cfg.MaxRecordBytes = maxRecordBlocks * core.BlockSize
	}
	return cfg
}

// formatted is a harness whose format writes have completed and been dropped from the trace.
func formatted(t testing.TB, diskBytes uint64, cfg disklog.Config) *harness {
	t.Helper()
	h := newHarness(t, diskBytes, 64<<20, cfg)
	h.run()
	h.trace = nil
	return h
}

func (h *harness) absorb() {
	for i, a := range h.out.Actions {
		switch a.Kind {
		case core.ActDiskWrite:
			p := pendingIO{act: a}
			p.act.Bufs = slices.Clone(a.Bufs)
			h.pending = append(h.pending, p)
			blocks := a.Len / core.BlockSize
			h.trace = append(h.trace, fmt.Sprintf("write %d+%d io%d", a.LBA, blocks, a.IO))
			h.writeAt[a.LBA] = a.IO
			h.writes++
		case core.ActDiskFsync:
			p := pendingIO{act: a}
			if i > 0 && h.out.Actions[i-1].Kind == core.ActDiskWrite {
				p.linked, p.link = true, h.out.Actions[i-1].IO
			} else {
				h.unlinked++
			}
			h.pending = append(h.pending, p)
			h.trace = append(h.trace, fmt.Sprintf("fsync io%d", a.IO))
			h.fsyncs++
		case core.ActSetTimer:
			h.trace = append(h.trace, fmt.Sprintf("timer %d at %d", a.Timer, a.At))
			h.timer = true
		default:
			h.t.Fatalf("unexpected action %+v", a)
		}
	}
	for _, ev := range h.out.Events {
		ev.Entries = slices.Clone(ev.Entries)
		ev.Tickets = slices.Clone(ev.Tickets)
		h.events = append(h.events, ev)
	}
	h.out.Reset()
}

func (h *harness) eligible(i int) bool {
	p := h.pending[i]
	return !p.linked || h.completed[p.link]
}

// step executes pending action i and feeds its completion to the log.
func (h *harness) step(i int) {
	h.t.Helper()
	if !h.eligible(i) {
		h.t.Fatalf("action %+v runs before its linked write", h.pending[i].act)
	}
	p := h.pending[i]
	h.pending = slices.Delete(h.pending, i, i+1)
	derr := h.failIO[p.act.IO]
	switch p.act.Kind {
	case core.ActDiskWrite:
		if derr == core.DiskOK {
			h.applyWrite(p.act)
		}
		h.completed[p.act.IO] = true
	case core.ActDiskFsync:
		if derr == core.DiskOK {
			for io := range h.completed {
				h.durable[io] = true
			}
		}
	}
	h.feed(core.Event{Kind: core.EvDiskDone, IO: p.act.IO, Err: derr})
}

func (h *harness) applyWrite(a core.Action) {
	var img []byte
	if a.Buf != core.NoBuf {
		img = append(img, h.pool.Bytes(a.Buf)...)
	}
	for _, b := range a.Bufs {
		img = append(img, h.pool.Bytes(b)...)
	}
	if uint64(len(img)) != a.Len {
		h.t.Fatalf("write io%d gathers %d bytes, says %d", a.IO, len(img), a.Len)
	}
	for i := 0; i < len(img); i += core.BlockSize {
		h.blocks[a.LBA+core.LBA(i/core.BlockSize)] = slices.Clone(img[i : i+core.BlockSize])
	}
}

func (h *harness) feed(ev core.Event) {
	h.log.OnEvent(&h.ctx, &ev, &h.out)
	h.absorb()
}

func (h *harness) fireTimer() {
	h.timer = false
	h.feed(core.Event{Kind: core.EvTimer, Timer: retryTimer})
}

// run executes pending actions oldest first until none is left.
func (h *harness) run() {
	h.t.Helper()
	for len(h.pending) > 0 {
		h.step(0)
	}
}

// round executes the oldest round's write and flush.
func (h *harness) round() {
	h.t.Helper()
	h.step(0)
	h.step(0)
}

func (h *harness) read(lba core.LBA, n uint64) []byte {
	var b []byte
	for i := range n {
		blk, ok := h.blocks[lba+core.LBA(i)]
		if !ok {
			blk = make([]byte, core.BlockSize)
		}
		b = append(b, blk...)
	}
	return b
}

// record reads back and decodes the record a Durable event names.
func (h *harness) record(ev disklog.Event) disklog.DecodedRecord {
	h.t.Helper()
	seq := h.segmentSeq(ev.Segment)
	left := h.geo.SegmentBlocks - ev.Block
	place := disklog.Place{LogID: testLogID, Seq: seq, Offset: ev.Block}
	headerBlocks := uint64(binary.LittleEndian.Uint16(h.read(ev.LBA, 1)[6:]))
	hdr, err := disklog.DecodeHeader(h.read(ev.LBA, headerBlocks), place, left, nil)
	if err != nil {
		h.t.Fatalf("LSN %d header at LBA %d: %v", ev.LSN, ev.LBA, err)
	}
	if ev.HeaderBlocks != 0 && ev.HeaderBlocks != hdr.Layout.HeaderBlocks {
		h.t.Fatalf("LSN %d has %d header blocks, event says %d", ev.LSN, hdr.Layout.HeaderBlocks, ev.HeaderBlocks)
	}
	d, err := disklog.DecodeRecord(h.read(ev.LBA, hdr.Layout.Blocks()), place, left, nil)
	if err != nil {
		h.t.Fatalf("LSN %d at LBA %d: %v", ev.LSN, ev.LBA, err)
	}
	if d.Record.LSN != ev.LSN {
		h.t.Fatalf("record at LBA %d has LSN %d, event says %d", ev.LBA, d.Record.LSN, ev.LSN)
	}
	return d
}

func (h *harness) segmentHeader(seg uint32) disklog.SegmentHeader {
	h.t.Helper()
	sh, err := disklog.DecodeSegmentHeader(h.read(h.geo.LBA(seg, 0), 1))
	if err != nil {
		h.t.Fatalf("segment %d header: %v", seg, err)
	}
	return sh
}

func (h *harness) segmentSeq(seg uint32) uint64 { return h.segmentHeader(seg).Seq }

func (h *harness) eventsOf(kind disklog.EventKind) []disklog.Event {
	var evs []disklog.Event
	for _, ev := range h.events {
		if ev.Kind == kind {
			evs = append(evs, ev)
		}
	}
	return evs
}

func (h *harness) alloc(n int) core.IOBuf {
	h.t.Helper()
	b, err := h.pool.Alloc(n)
	if err != nil {
		h.t.Fatal(err)
	}
	return b
}

func pattern(seed uint64, n int) []byte {
	b := make([]byte, n)
	core.NewRand(seed).Fill(b)
	return b
}

var testChunk = core.ChunkID{Volume: 2, Inode: 3, Index: 4}

// write builds a write item of count blocks of chunk index idx, filled from seed.
func (h *harness) write(idx uint32, count uint16, seed uint64) disklog.Item {
	buf := h.alloc(int(count) * core.BlockSize)
	copy(h.pool.Bytes(buf), pattern(seed, int(count)*core.BlockSize))
	chunk := testChunk
	chunk.Index = idx
	return disklog.Item{Entry: disklog.WriteEntry(1, chunk, uint64(seed), 0, count), Buf: buf}
}

func (h *harness) blob(kind disklog.BlobKind, owner uint32, n int, seed uint64) disklog.Item {
	buf := h.alloc(n)
	copy(h.pool.Bytes(buf), pattern(seed, n))
	return disklog.Item{Entry: disklog.BlobEntry(kind, owner, uint64(n)), Buf: buf}
}

func (h *harness) submit(s disklog.Stream, it disklog.Item) disklog.Ticket {
	h.t.Helper()
	t, err := h.log.Submit(&h.ctx, s, it, &h.out)
	if err != nil {
		h.t.Fatalf("submit %+v: %v", it.Entry, err)
	}
	h.absorb()
	return t
}

func (h *harness) wantTrace(want ...string) {
	h.t.Helper()
	if !slices.Equal(h.trace, want) {
		h.t.Fatalf("trace\n got %q\nwant %q", h.trace, want)
	}
}

func (h *harness) wantLive(n int) {
	h.t.Helper()
	if h.pool.Live() != n {
		h.t.Fatalf("%d pool buffers live, want %d", h.pool.Live(), n)
	}
}
