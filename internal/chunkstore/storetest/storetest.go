// Package storetest is the shared ChunkStore suite: the store's behaviours, checked against a
// byte-range model, written once against the Disk interface and run on every disk driver.
package storetest

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/raftshard"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// Disk is a disk driver under the suite, with the buffer pool the store's I/O uses.
type Disk interface {
	// Pool is the buffer pool of the process the store runs in.
	Pool() core.BufPool
	// Blocks is the disk's size in blocks.
	Blocks() uint64
	// Execute queues a read, write, flush or trim.
	Execute(a *core.Action)
	// Complete submits what Execute queued and appends at least one completion, waiting for it.
	Complete(evs []core.Event) []core.Event
	// Restart ends the process with nothing in flight and starts another over the same disk, with
	// a new pool: a clean close and reopen.
	Restart()
}

// Host is a Disk whose completions a loop that owns the store's log hands the store, through the
// Rig's OnEvent and OnLog, as a storage node's device loop does.
type Host interface {
	Disk
	// Serve hands the loop the store's log, nil while it recovers, after each call into the store.
	Serve(log *disklog.Log)
	// Step runs the loop until it handed r's store something; it reports false with nothing to do.
	Step(r *Rig) bool
}

// DiskBytes is the size of the disk each case runs on.
const DiskBytes = 256 << 20

// PoolBytes is the size of the buffer pool each case's store allocates from.
const PoolBytes = 64 << 20

const pool core.PoolID = 1

// Config is the store's configuration in the suite: every check on, no reserve.
func Config() chunkstore.Config {
	cfg := chunkstore.Config{Log: disklog.DefaultConfig(), Pool: pool, Check: true}
	cfg.Log.Check, cfg.Log.NoReserve = true, true
	return cfg
}

// Case is one behaviour of the suite.
type Case struct {
	Name string
	Run  func(r *Rig)
}

// Run runs every case of the suite, each on a fresh disk from newDisk.
func Run(t *testing.T, newDisk func(t *testing.T) Disk) {
	for _, c := range Cases {
		t.Run(c.Name, func(t *testing.T) {
			c.Run(NewRig(t, newDisk(t), Config()))
		})
	}
}

// Rig runs a store over a Disk: it executes the store's actions, hands it their completions and
// runs its merges and cleaner jobs inline, and keeps a model of the acknowledged bytes.
type Rig struct {
	T   testing.TB
	D   Disk
	Cfg chunkstore.Config
	S   *chunkstore.Store
	ctx core.Context
	out chunkstore.Output
	evs []core.Event
	// inflight counts the disk actions executed and not completed.
	inflight int
	nextID   uint64
	replies  map[uint64]chunkstore.Reply
	answered []uint64
	// model is each chunk's bytes as its acknowledged changes left them, chunks the chunks it
	// holds, and sent the changes not yet answered.
	model  map[core.ChunkID][]byte
	chunks []core.ChunkID
	sent   map[uint64]change
	// trimmed is the first block of every range the store trimmed.
	trimmed []core.LBA
	// Opened, where set, is called with each store Format or Open makes, before any event, as a
	// node adds its shards; Mail is handed the store's Mail, for the shards on the round driver.
	Opened func(*chunkstore.Store)
	Mail   func(*raftshard.Mail)
}

type change struct {
	c    core.ChunkID
	off  uint32
	data []byte
	n    uint32
}

// NewRig formats a store of configuration cfg on d.
func NewRig(t testing.TB, d Disk, cfg chunkstore.Config) *Rig {
	t.Helper()
	r := newRig(t, d, cfg)
	r.Format(0x5eed)
	r.Settle()
	return r
}

// StartRig recovers a store of configuration cfg from d, as a storage node starts a device, and
// returns what the node does with it and, for chunkstore.StartFormat, the log ID to Format it under.
func StartRig(t testing.TB, d Disk, cfg chunkstore.Config, rng *core.Rand) (*Rig, chunkstore.DeviceStart, uint64) {
	t.Helper()
	r := newRig(t, d, cfg)
	r.open()
	act, log := chunkstore.StartDevice(rng, r.S.Err())
	return r, act, log
}

// Format starts formatting the disk under log; Settle finishes it.
func (r *Rig) Format(log uint64) {
	r.T.Helper()
	geo, err := disklog.GeometryFor(r.D.Blocks() * core.BlockSize)
	if err != nil {
		r.T.Fatal(err)
	}
	if r.S, err = chunkstore.Format(&r.ctx, r.Cfg, disklog.Superblock{LogID: log, Geometry: geo}, &r.out); err != nil {
		r.T.Fatal(err)
	}
	r.opened()
	r.take()
}

// OpenRig recovers a store of configuration cfg from d, a disk a store was formatted on.
func OpenRig(t testing.TB, d Disk, cfg chunkstore.Config) *Rig {
	t.Helper()
	r := newRig(t, d, cfg)
	r.Reopen()
	return r
}

func newRig(t testing.TB, d Disk, cfg chunkstore.Config) *Rig {
	r := &Rig{T: t, D: d, Cfg: cfg, replies: map[uint64]chunkstore.Reply{}, model: map[core.ChunkID][]byte{}, sent: map[uint64]change{}}
	r.ctx = core.Context{Now: 1, Raw: 2, Pool: d.Pool()}
	return r
}

// Pool is the buffer pool of the store's process.
func (r *Rig) Pool() core.BufPool { return r.ctx.Pool }

func (r *Rig) opened() {
	if r.Opened != nil {
		r.Opened(r.S)
	}
}

// take executes the store's actions and books its replies.
func (r *Rig) take() {
	for i := range r.out.Mail {
		if r.Mail == nil {
			r.T.Fatalf("the store mailed %+v with no shard to take it", r.out.Mail[i])
		}
		r.Mail(&r.out.Mail[i])
	}
	if h, ok := r.D.(Host); ok {
		h.Serve(r.S.Log())
	}
	for i := range r.out.Actions {
		a := &r.out.Actions[i]
		switch a.Kind {
		case core.ActSetTimer:
		case core.ActDiskRead, core.ActDiskWrite, core.ActDiskFsync, core.ActDiskTrim:
			if a.Kind == core.ActDiskTrim {
				r.trimmed = append(r.trimmed, a.LBA)
			}
			r.D.Execute(a)
			r.inflight++
		default:
			r.T.Fatalf("the store asked for action %+v", *a)
		}
	}
	for _, p := range r.out.Replies {
		if _, dup := r.replies[p.ID]; dup {
			r.T.Fatalf("request %d answered twice", p.ID)
		}
		r.replies[p.ID] = p
		r.answered = append(r.answered, p.ID)
		if ch, ok := r.sent[p.ID]; ok {
			delete(r.sent, p.ID)
			if p.Err == nil {
				r.apply(ch)
			}
		}
	}
	r.out.Reset()
	for c := r.S.MergeDue(); c != nil; c = r.S.MergeDue() {
		c.Run()
		if err := r.S.InstallMerge(&r.ctx, c, &r.out); err != nil {
			r.T.Fatal(err)
		}
		r.take()
	}
	for j := r.S.CleanDue(); j != nil; j = r.S.CleanDue() {
		j.Run(r.ctx.Pool)
		r.S.InstallClean(&r.ctx, j, &r.out)
		r.take()
	}
}

// Step waits for the disk's next completions and hands them to the store; it reports false with
// nothing in flight.
func (r *Rig) Step() bool {
	if h, ok := r.D.(Host); ok {
		return h.Step(r)
	}
	if r.inflight == 0 {
		return false
	}
	r.evs = r.D.Complete(r.evs[:0])
	for i := range r.evs {
		r.inflight--
		r.S.OnEvent(&r.ctx, &r.evs[i], &r.out)
		r.take()
	}
	return true
}

// OnEvent hands the store a disk completion or timer from a Host's loop, on that loop's clocks.
func (r *Rig) OnEvent(ctx *core.Context, ev *core.Event) {
	r.ctx.Now, r.ctx.Raw = ctx.Now, ctx.Raw
	r.S.OnEvent(&r.ctx, ev, &r.out)
	r.take()
}

// OnLog hands the store an event of its log from a Host's loop, on that loop's clocks.
func (r *Rig) OnLog(ctx *core.Context, ev *disklog.Event) {
	r.ctx.Now, r.ctx.Raw = ctx.Now, ctx.Raw
	r.S.OnLog(&r.ctx, ev, &r.out)
	r.take()
}

// Settle runs the disk until nothing is in flight.
func (r *Rig) Settle() {
	r.T.Helper()
	for n := 0; r.Step(); n++ {
		if n == 1_000_000 {
			r.T.Fatal("the store did not settle")
		}
	}
}

// Restart settles, ends the store's process and recovers a store from the same disk.
func (r *Rig) Restart() {
	r.T.Helper()
	r.Settle()
	r.D.Restart()
	r.Reopen()
}

// Reopen recovers a store from the disk, dropping the old one and the replies it gave.
func (r *Rig) Reopen() {
	r.T.Helper()
	r.open()
	if !r.S.Serving() {
		r.T.Fatalf("recovery ended without serving: %v", r.S.Err())
	}
}

func (r *Rig) open() {
	r.T.Helper()
	r.ctx.Pool, r.inflight = r.D.Pool(), 0
	clear(r.replies)
	clear(r.sent)
	r.answered = r.answered[:0]
	var err error
	if r.S, err = chunkstore.Open(&r.ctx, r.Cfg, r.D.Blocks(), &r.out); err != nil {
		r.T.Fatal(err)
	}
	r.opened()
	r.take()
	r.Settle()
}

func (r *Rig) id() uint64 {
	r.nextID++
	return r.nextID
}

func (r *Rig) apply(ch change) {
	b, ok := r.model[ch.c]
	if !ok {
		b = make([]byte, core.ChunkSize)
		r.model[ch.c] = b
		r.chunks = append(r.chunks, ch.c)
	}
	if ch.data == nil {
		clear(b[ch.off : ch.off+ch.n])
		return
	}
	copy(b[ch.off:], ch.data)
}

func (r *Rig) buf(off uint32, data []byte) core.IOBuf {
	first, last := off/core.BlockSize, (off+uint32(len(data))-1)/core.BlockSize
	b, err := r.ctx.Pool.Alloc(int(last-first+1) * core.BlockSize)
	if err != nil {
		r.T.Fatal(err)
	}
	copy(r.ctx.Pool.Bytes(b)[off%core.BlockSize:], data)
	return b
}

// Write sends a write of data at off in chunk c of epoch, unanswered until the disk runs.
func (r *Rig) Write(c core.ChunkID, off uint32, data []byte, epoch uint64) uint64 {
	return r.write(chunkstore.Request{Chunk: c, Off: off, Len: uint32(len(data)), Epoch: epoch}, data)
}

// WriteExpect sends a write that expects the chunk at version ver.
func (r *Rig) WriteExpect(c core.ChunkID, off uint32, data []byte, ver uint64) uint64 {
	return r.write(chunkstore.Request{Chunk: c, Off: off, Len: uint32(len(data)), HasVer: true, Ver: ver}, data)
}

func (r *Rig) write(q chunkstore.Request, data []byte) uint64 {
	q.ID, q.Buf = r.id(), r.buf(q.Off, data)
	r.sent[q.ID] = change{c: q.Chunk, off: q.Off, data: slices.Clone(data), n: q.Len}
	r.S.Write(&r.ctx, q, &r.out)
	r.take()
	return q.ID
}

// Punch sends a punch of n bytes at off in chunk c of epoch.
func (r *Rig) Punch(c core.ChunkID, off, n uint32, epoch uint64) uint64 {
	id := r.id()
	r.sent[id] = change{c: c, off: off, n: n}
	r.S.Punch(&r.ctx, chunkstore.Request{ID: id, Chunk: c, Off: off, Len: n, Epoch: epoch}, &r.out)
	r.take()
	return id
}

// Flush sends a flush of chunks cs.
func (r *Rig) Flush(cs ...core.ChunkID) uint64 {
	id := r.id()
	r.S.Flush(&r.ctx, id, cs, &r.out)
	r.take()
	return id
}

// Read sends a read; q's ID is set.
func (r *Rig) Read(q chunkstore.Request) uint64 {
	q.ID = r.id()
	r.S.Read(&r.ctx, q, &r.out)
	r.take()
	return q.ID
}

// Reply is request id's reply, if it came.
func (r *Rig) Reply(id uint64) (chunkstore.Reply, bool) {
	p, ok := r.replies[id]
	return p, ok
}

// InOrder fails unless every request of ids was answered, in the order of ids.
func (r *Rig) InOrder(ids ...uint64) {
	r.T.Helper()
	last := -1
	for k, id := range ids {
		i := slices.Index(r.answered, id)
		if i < 0 {
			r.T.Fatalf("request %d not answered", id)
		}
		if i < last {
			r.T.Fatalf("request %d answered before request %d", id, ids[k-1])
		}
		last = i
	}
}

// OK settles and returns the version request id was answered with, failing on an error.
func (r *Rig) OK(id uint64) uint64 {
	r.T.Helper()
	r.Settle()
	p, ok := r.replies[id]
	if !ok {
		r.T.Fatalf("request %d not answered", id)
	}
	if p.Err != nil {
		r.T.Fatalf("request %d: %v", id, p.Err)
	}
	return p.Version
}

// Fails settles and checks that request id was answered with err.
func (r *Rig) Fails(id uint64, err error) {
	r.T.Helper()
	r.Settle()
	p, ok := r.replies[id]
	if !ok || !errors.Is(p.Err, err) {
		r.T.Fatalf("request %d answered %+v %v, want %v", id, p, ok, err)
	}
}

// ReadNow reads n bytes at off of chunk c as q asks, and returns them with the version read.
func (r *Rig) ReadNow(q chunkstore.Request) ([]byte, uint64, error) {
	r.T.Helper()
	id := r.Read(q)
	r.Settle()
	p, ok := r.replies[id]
	if !ok {
		r.T.Fatalf("read %d not answered", id)
	}
	if p.Err != nil {
		return nil, 0, p.Err
	}
	b := slices.Clone(r.ctx.Pool.Bytes(p.Buf)[q.Off%core.BlockSize:][:q.Len])
	r.ctx.Pool.Free(p.Buf)
	return b, p.Version, nil
}

// Chunk reads the whole of chunk c's live image.
func (r *Rig) Chunk(c core.ChunkID) []byte {
	r.T.Helper()
	b, _, err := r.ReadNow(chunkstore.Request{Chunk: c, Len: core.ChunkSize})
	if err != nil {
		r.T.Fatalf("read of chunk %v: %v", c, err)
	}
	return b
}

// Model is chunk c as its acknowledged changes left it.
func (r *Rig) Model(c core.ChunkID) []byte {
	if b, ok := r.model[c]; ok {
		return b
	}
	return make([]byte, core.ChunkSize)
}

// Check reads every chunk the model holds and compares it with the model.
func (r *Rig) Check() {
	r.T.Helper()
	for _, c := range r.chunks {
		if err := Diff(r.Chunk(c), r.model[c]); err != nil {
			r.T.Fatalf("chunk %v: %v", c, err)
		}
	}
}

// Diff describes where got first differs from want.
func Diff(got, want []byte) error {
	if bytes.Equal(got, want) {
		return nil
	}
	if len(got) != len(want) {
		return fmt.Errorf("read %d bytes, want %d", len(got), len(want))
	}
	i := 0
	for got[i] == want[i] {
		i++
	}
	return fmt.Errorf("byte %d (block %d) is %#x, want %#x", i, i/core.BlockSize, got[i], want[i])
}

// SimDisk is the simulated disk under the suite; it alone runs the cases that need fault
// injection. It runs one queued action per Complete, so a crash can fall between any two of them.
type SimDisk struct {
	D     *sim.Disk
	pool  *sim.Pool
	queue []core.Action
	// Unflushed counts the unflushed block writes the disk held at its crashes.
	Unflushed int
}

// NewSimDisk makes a simulated disk of DiskBytes, tearing sectors on a crash.
func NewSimDisk(seed uint64) *SimDisk { return NewSimDiskOf(seed, DiskBytes) }

// NewSimDiskOf is NewSimDisk on a disk of the given size.
func NewSimDiskOf(seed, bytes uint64) *SimDisk {
	s := sim.DefaultDiskSettings()
	s.Seed, s.TearSectors = seed, true
	return &SimDisk{D: sim.NewDiskWith(bytes/core.BlockSize, s), pool: sim.NewPool(PoolBytes, 0)}
}

// Pool is the process's buffer pool.
func (d *SimDisk) Pool() core.BufPool { return d.pool }

// Blocks is the disk's size.
func (d *SimDisk) Blocks() uint64 { return d.D.Blocks() }

// Execute queues the action.
func (d *SimDisk) Execute(a *core.Action) { d.queue = append(d.queue, *a) }

// Complete runs the oldest queued action and appends its completion.
func (d *SimDisk) Complete(evs []core.Event) []core.Event {
	a := d.queue[0]
	d.queue = d.queue[1:]
	return append(evs, d.D.Execute(d.pool, &a))
}

// Restart starts a new process's pool; the disk keeps what it was written, flushed or not.
func (d *SimDisk) Restart() {
	d.queue, d.pool = nil, sim.NewPool(PoolBytes, 0)
}

// Crash cuts the power: every queued action is lost, and each unflushed block, or sector of it,
// lands or not as r says.
func (d *SimDisk) Crash(r *core.Rand) {
	d.Unflushed += d.D.Unflushed()
	d.D.CrashRandom(r)
	d.Restart()
}

// SlotDiskBytes is the size of the disk FreedSlotReadsAsZeros runs on, the smallest whose segments
// hold a chunk slot.
const SlotDiskBytes = 16 << 30

// FreedSlotReadsAsZeros writes chunk 1 whole twice on d, a disk of SlotDiskBytes, so the first
// write's slot segment frees with its only slot, and reads that segment's first block back from d
// once its TRIM ran: zeros, which recovery takes for a free segment, since the trim zeroes it.
func FreedSlotReadsAsZeros(t *testing.T, d Disk) {
	r := NewRig(t, d, Config())
	r.OK(r.Write(Chunk(1), 0, Fill(core.ChunkSize, 1), 0))
	r.trimmed = nil
	r.OK(r.Write(Chunk(1), 0, Fill(core.ChunkSize, 2), 0))
	r.OK(r.Write(Chunk(2), 0, Fill(core.BlockSize, 3), 0))
	if len(r.trimmed) != 1 {
		t.Fatalf("trimmed %v, want the first slot's segment", r.trimmed)
	}
	buf, err := r.ctx.Pool.Alloc(core.BlockSize)
	if err != nil {
		t.Fatal(err)
	}
	r.D.Execute(&core.Action{Kind: core.ActDiskRead, IO: 1 << 62, LBA: r.trimmed[0], Buf: buf, Len: core.BlockSize})
	if evs := r.D.Complete(nil); len(evs) != 1 || evs[0].Err != core.DiskOK {
		t.Fatalf("read of the trimmed segment's first block: %+v", evs)
	}
	if !bytes.Equal(r.ctx.Pool.Bytes(buf)[:core.BlockSize], make([]byte, core.BlockSize)) {
		t.Fatal("the freed slot segment's first block does not read as zeros")
	}
	r.ctx.Pool.Free(buf)
	r.Restart()
	r.Check()
}
