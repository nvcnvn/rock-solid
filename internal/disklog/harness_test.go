package disklog_test

import (
	"encoding/binary"
	"fmt"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/trace"
)

const (
	testLogID = 0x0123456789abcdef
	gib       = 1 << 30
	firstIO   = 100
)

type pendingIO struct {
	act    core.Action
	linked bool
	link   core.IOID
}

// harness drives a Log, or a Recovery, against a simulated disk: it executes their actions
// when told to, feeds back the completions, and records the action trace and every event.
type harness struct {
	t       testing.TB
	pool    *sim.Pool
	ctx     core.Context
	geo     disklog.Geometry
	cfg     disklog.Config
	logID   uint64
	log     *disklog.Log
	rec     *disklog.Recovery
	out     disklog.Output
	disk    *sim.Disk
	pending []pendingIO
	trace   []string
	events  []disklog.Event
	failIO  map[core.IOID]core.DiskError
	own     int
	onEvent func(disklog.Event)
	onWrite func(core.Action)
	// failWhen, when set, chooses the result of each action instead of executing it.
	failWhen func(core.Action) core.DiskError
	// ioRand, when set, makes recovery complete its actions in random order.
	ioRand     *core.Rand
	readBytes  uint64
	maxPending int
	// raw sets ctx.Raw from ctx.Now at every completion.
	raw func(now core.Tick) core.Tick

	completed map[core.IOID]bool
	done      []core.IOID
	durable   map[core.IOID]bool
	writeAt   map[core.LBA]core.IOID
	writes    int
	fsyncs    int
	unlinked  int

	// sched, when set, executes the actions on node's loop instead, each when step chooses it.
	sched   *sim.Scheduler
	node    *sim.Node
	loop    core.NodeID
	inPass  bool
	passOut []core.Action
	roundOf map[core.IOID]uint64
	// ignoreErrors hands the log every completion as a success, as a log without its error path would see it.
	ignoreErrors bool
	// victim stands for a cleaner with a victim, so stalled items wait; otherwise the harness
	// reports no victim and they fail with NoSpace.
	victim bool
}

func newHarness(t testing.TB, diskBytes uint64, poolBytes int, cfg disklog.Config) *harness {
	t.Helper()
	h := blankHarness(t, diskBytes, poolBytes, cfg)
	var err error
	h.log, err = disklog.Format(&h.ctx, h.cfg, disklog.Superblock{LogID: testLogID, Geometry: h.geo}, firstIO, &h.out)
	if err != nil {
		t.Fatal(err)
	}
	h.own = h.pool.Live()
	h.absorb()
	return h
}

// schedHarness is newHarness on a node of a scheduler made from seed, whose disk fails the
// actions failIO names.
func schedHarness(t testing.TB, seed, diskBytes uint64, poolBytes int, cfg disklog.Config) *harness {
	t.Helper()
	h := blankHarness(t, diskBytes, poolBytes, cfg)
	set := sim.DefaultSettings()
	set.PoolBytes = poolBytes
	h.sched = sim.New(seed, set)
	h.node = h.sched.AddNode(sim.DefaultClock, []sim.DiskSpec{{Blocks: diskBytes / core.BlockSize, Settings: sim.DefaultDiskSettings()}}, func(n *sim.Node) {
		h.loop = n.Start(logMachine{h}, n.Disk(0))
		h.pool = n.Pool()
		h.ctx = n.Context()
	})
	h.disk = h.node.Disk(0)
	h.disk.Inject(sim.Fault{Effect: sim.FailIO, When: func(op sim.DiskOp, _ *core.Rand) bool { return h.failIO[op.IO] != core.DiskOK }})
	h.roundOf = map[core.IOID]uint64{}
	var err error
	h.log, err = disklog.Format(&h.ctx, h.cfg, disklog.Superblock{LogID: testLogID, Geometry: h.geo}, firstIO, &h.out)
	if err != nil {
		t.Fatal(err)
	}
	h.own = h.pool.Live()
	h.absorb()
	return h
}

// logMachine runs the harness's Log or Recovery as a scheduler's machine.
type logMachine struct{ h *harness }

func (m logMachine) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	h := m.h
	h.ctx = *ctx
	h.inPass, h.passOut = true, out
	i := slices.IndexFunc(h.pending, func(p pendingIO) bool { return p.act.IO == ev.IO })
	if ev.Kind != core.EvDiskDone || i < 0 {
		h.t.Fatalf("event %+v for no pending action", *ev)
	}
	h.finish(i, *ev)
	h.inPass = false
	return h.passOut
}

func (m logMachine) Round(a *core.Action) uint64 { return m.h.roundOf[a.IO] }

// blankHarness is a harness over an all-zero disk with no log on it.
func blankHarness(t testing.TB, diskBytes uint64, poolBytes int, cfg disklog.Config) *harness {
	t.Helper()
	geo, err := disklog.GeometryFor(diskBytes)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Check = true
	h := &harness{
		t:         t,
		pool:      sim.NewPool(poolBytes, 0),
		geo:       geo,
		cfg:       cfg,
		logID:     testLogID,
		disk:      sim.NewDisk(diskBytes / core.BlockSize),
		failIO:    map[core.IOID]core.DiskError{},
		completed: map[core.IOID]bool{},
		durable:   map[core.IOID]bool{},
		writeAt:   map[core.LBA]core.IOID{},
	}
	h.raw = func(now core.Tick) core.Tick { return now*1226/1000 + 7 }
	h.ctx = core.Context{Now: 1000, Raw: h.raw(1000), Pool: h.pool}
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
	if !h.victim && h.rec == nil && h.log != nil && h.log.Stalled() {
		h.log.FailStalled(&h.ctx, &h.out)
	}
	if h.sched != nil {
		h.noteRounds()
	}
	for i, a := range h.out.Actions {
		switch a.Kind {
		case core.ActDiskRead:
			h.pending = append(h.pending, pendingIO{act: a})
			h.trace = append(h.trace, fmt.Sprintf("read %d+%d io%d", a.LBA, a.Len/core.BlockSize, a.IO))
			h.readBytes += a.Len
			h.maxPending = max(h.maxPending, len(h.pending))
		case core.ActDiskWrite:
			p := pendingIO{act: a}
			p.act.Vec = slices.Clone(a.Vec)
			h.pending = append(h.pending, p)
			blocks := a.Len / core.BlockSize
			h.trace = append(h.trace, fmt.Sprintf("write %d+%d io%d", a.LBA, blocks, a.IO))
			h.writeAt[a.LBA] = a.IO
			h.writes++
			if h.onWrite != nil {
				h.onWrite(a)
			}
		case core.ActDiskFsync:
			p := pendingIO{act: a}
			if a.Linked {
				if i == 0 || h.out.Actions[i-1].Kind != core.ActDiskWrite {
					h.t.Fatalf("linked flush io%d does not follow a write", a.IO)
				}
				p.linked, p.link = true, h.out.Actions[i-1].IO
			} else {
				h.unlinked++
			}
			h.pending = append(h.pending, p)
			h.trace = append(h.trace, fmt.Sprintf("fsync io%d", a.IO))
			h.fsyncs++
		default:
			h.t.Fatalf("unexpected action %+v", a)
		}
	}
	if h.sched != nil {
		h.handOver()
	}
	from := len(h.events)
	for _, ev := range h.out.Events {
		ev.Entries = slices.Clone(ev.Entries)
		ev.Tickets = slices.Clone(ev.Tickets)
		ev.Payloads = slices.Clone(ev.Payloads)
		ev.Data = slices.Clone(ev.Data)
		h.events = append(h.events, ev)
		if h.sched != nil && ev.Kind == disklog.Durable {
			h.node.Output(h.loop, trace.Record{Name: trace.OutDurable, Fields: [trace.MaxFields]uint64{ev.LSN, ev.LSN}})
		}
	}
	h.out.Reset()
	to := len(h.events)
	if h.onEvent != nil {
		for i := from; i < to; i++ {
			h.onEvent(h.events[i])
		}
	}
}

// noteRounds numbers the log's round writes and flushes by their record's LSN: the writes a
// call emits start its newest rounds, and a round's flush takes the IO after its write's.
func (h *harness) noteRounds() {
	if h.rec != nil || h.log == nil {
		return
	}
	writes := 0
	for _, a := range h.out.Actions {
		if a.Kind == core.ActDiskWrite {
			writes++
		}
	}
	if writes > h.log.InFlight() {
		return
	}
	next := h.log.DurableLSN() + uint64(h.log.InFlight()-writes) + 1
	for _, a := range h.out.Actions {
		switch a.Kind {
		case core.ActDiskWrite:
			h.roundOf[a.IO] = next
			next++
		case core.ActDiskFsync:
			h.roundOf[a.IO] = h.roundOf[a.IO-1]
		}
	}
}

// handOver gives the scheduler the actions absorbed: as the current pass's when an event is
// being handled, otherwise as a pass of their own.
func (h *harness) handOver() {
	if len(h.out.Actions) == 0 {
		return
	}
	if h.inPass {
		for _, a := range h.out.Actions {
			a.Vec = slices.Clone(a.Vec)
			h.passOut = append(h.passOut, a)
		}
		return
	}
	acts := slices.Clone(h.out.Actions)
	h.node.Call(h.loop, func(_ *core.Context, out []core.Action) []core.Action { return append(out, acts...) })
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
	if h.sched != nil {
		h.take(p.act.IO)
		return
	}
	ev := core.Event{Kind: core.EvDiskDone, IO: p.act.IO, Err: h.failIO[p.act.IO]}
	if h.failWhen != nil && ev.Err == core.DiskOK {
		ev.Err = h.failWhen(p.act)
	}
	if ev.Err == core.DiskOK {
		ev = h.disk.Execute(h.pool, &p.act)
	}
	h.finish(i, ev)
}

func (h *harness) take(io core.IOID) {
	h.t.Helper()
	for _, c := range h.sched.Choices() {
		if c.Kind == sim.ChooseDisk && c.Node == h.node.ID() && c.Action.IO == io {
			h.sched.Take(c)
			return
		}
	}
	h.t.Fatalf("io%d is not among the scheduler's choices", io)
}

// finish records pending action i's completion ev and feeds it to the log.
func (h *harness) finish(i int, ev core.Event) {
	p := h.pending[i]
	h.pending = slices.Delete(h.pending, i, i+1)
	switch p.act.Kind {
	case core.ActDiskWrite:
		h.completed[p.act.IO] = true
		h.done = append(h.done, p.act.IO)
	case core.ActDiskFsync:
		if ev.Err == core.DiskOK {
			for _, io := range h.done {
				h.durable[io] = true
			}
		}
	}
	h.feed(ev)
}

func (h *harness) feed(ev core.Event) {
	if h.sched == nil {
		h.ctx.Now += 10
		h.ctx.Raw = h.raw(h.ctx.Now)
	}
	if h.ignoreErrors {
		ev.Err = core.DiskOK
	}
	if h.rec != nil {
		h.rec.OnEvent(&h.ctx, &ev, &h.out)
	} else {
		h.log.OnEvent(&h.ctx, &ev, &h.out)
	}
	h.absorb()
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

func (h *harness) read(lba core.LBA, n uint64) []byte { return h.disk.Read(lba, n) }

// record reads back and decodes the record a Durable event names.
func (h *harness) record(ev disklog.Event) disklog.DecodedRecord {
	h.t.Helper()
	seq := h.segmentSeq(ev.Segment)
	left := h.geo.SegmentBlocks - ev.Block
	place := disklog.Place{LogID: h.logID, Seq: seq, Offset: ev.Block}
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

// wantLive checks the pool buffers live besides the log's own.
func (h *harness) wantLive(n int) {
	h.t.Helper()
	if got := h.pool.Live() - h.own; got != n {
		h.t.Fatalf("%d pool buffers live besides the log's %d, want %d", got, h.own, n)
	}
}

// close closes the log and checks every buffer is back in the pool.
func (h *harness) close() {
	h.t.Helper()
	if err := h.log.Close(&h.ctx); err != nil {
		h.t.Fatalf("close: %v", err)
	}
	if h.pool.Live() != 0 {
		h.t.Fatalf("%d pool buffers live after close", h.pool.Live())
	}
}
