package looptest

import (
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/chunkstore/storetest"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/loop"
	"github.com/nvcnvn/rock-solid/internal/meta"
	"github.com/nvcnvn/rock-solid/internal/node"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// storeLoop runs the suite's store on a stepped loop that owns its log, as a storage node's
// device loop does: the log takes its disk completions and TRIM timer, and the store its log's
// events through OnLog.
type storeLoop struct {
	t  testing.TB
	sd *storetest.SimDisk
	// pool is the loop's, the disk's until a crash or restart starts another process.
	pool core.BufPool
	s    *sim.Scheduler
	ring *Disk
	lg   *loop.Log
	r    *storetest.Rig
	// logged counts the log events the loop handed the store.
	logged int
}

func newStoreLoop(t *testing.T) storetest.Disk { return newStoreLoopOn(t, storetest.NewSimDisk(1)) }

func newStoreLoopOn(t *testing.T, sd *storetest.SimDisk) *storeLoop {
	sl := &storeLoop{t: t, sd: sd}
	sl.start()
	t.Cleanup(func() {
		if sl.logged == 0 {
			t.Error("no event of the store's log came through the loop")
		}
	})
	return sl
}

func (sl *storeLoop) start() {
	sl.s = sim.New(1, sim.Settings{PoolBytes: 1 << 20})
	node := sl.s.AddNode(sim.DefaultClock, nil, func(*sim.Node) {})
	k := NewKernel(sl.s, node.ID())
	pool := sl.sd.Pool()
	sl.pool = pool
	dr, err := NewDisk(k, sl.sd.D, pool)
	if err != nil {
		sl.t.Fatal(err)
	}
	sl.t.Cleanup(dr.Close)
	l := loop.New(loop.Config{Node: node.ID(), Pool: pool}, k, storeMachine{sl}, diskDriver{dr})
	if err := l.AttachDisk(dr); err != nil {
		sl.t.Fatal(err)
	}
	sl.ring, sl.lg = dr, l.AttachLog(nil, storetest.Config().Log, chunkstore.FirstLogIO, chunkstore.EndLogIO, 1)
	AddWithDisk(node, k, l, dr)
}

func (sl *storeLoop) Pool() core.BufPool     { return sl.sd.Pool() }
func (sl *storeLoop) Blocks() uint64         { return sl.sd.Blocks() }
func (sl *storeLoop) Execute(a *core.Action) { sl.ring.Execute(a) }
func (sl *storeLoop) Complete([]core.Event) []core.Event {
	panic("looptest: the loop hands the store its completions")
}

// Restart starts a new process, whose loop starts with the next call into the store, as after a crash.
func (sl *storeLoop) Restart() { sl.sd.Restart() }

func (sl *storeLoop) Serve(log *disklog.Log) {
	if sl.pool != sl.sd.Pool() {
		sl.start()
	}
	if sl.lg.Log() != log {
		sl.lg.Serve(log)
	}
}

func (sl *storeLoop) Step(r *storetest.Rig) bool {
	sl.r = r
	if sl.s.Step() {
		return true
	}
	if age := r.S.Oldest(1 << 62); age != 0 {
		sl.t.Fatalf("a store with nothing in flight has an I/O of age %d", age)
	}
	return false
}

type storeMachine struct{ sl *storeLoop }

func (m storeMachine) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	m.sl.r.OnEvent(ctx, ev)
	return out
}

func (m storeMachine) OnLog(ctx *core.Context, _ *loop.Log, ev *disklog.Event, out []core.Action) []core.Action {
	m.sl.logged++
	m.sl.r.OnLog(ctx, ev)
	return out
}

// The ChunkStore suite holds with the store on a loop that owns its log.
func TestSuiteOnALoopThatOwnsTheLog(t *testing.T) {
	storetest.Run(t, newStoreLoop)
}

// A crash, a failed write and a flipped bit leave the store on a loop that owns its log as they
// leave one that drives its log itself.
func TestFaultsOnALoopThatOwnsTheLog(t *testing.T) {
	for seed := uint64(1); seed <= 4; seed++ {
		for _, c := range storetest.SimCases {
			t.Run(c.Name, func(t *testing.T) {
				d := storetest.NewSimDisk(seed)
				c.Run(storetest.NewRig(t, newStoreLoopOn(t, d), storetest.Config()), d, core.NewRand(seed))
			})
		}
	}
}

// recovering is an owner machine that recovers its disk's log itself: on timer 1 it reads IO 1,
// one of the log's IO IDs, and on timer 2 submits a record to log.
type recovering struct {
	log                 *disklog.Log
	pool                core.BufPool
	out                 disklog.Output
	ticket              disklog.Ticket
	read, durable, nils int
}

func (m *recovering) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	switch {
	case ev.Kind == core.EvDiskDone && ev.IO == 1:
		m.read++
	case ev.Kind == core.EvTimer && ev.Timer == 1:
		out = append(out, core.Action{Kind: core.ActDiskRead, IO: 1})
	case ev.Kind == core.EvTimer && ev.Timer == 2:
		buf, err := m.pool.Alloc(proposal)
		if err != nil {
			panic(err)
		}
		if m.ticket, err = m.log.Submit(ctx, disklog.Hot, disklog.Item{Entry: disklog.BlobEntry(99, 0, proposal), Buf: buf}, &m.out); err != nil {
			panic(err)
		}
		out = append(out, m.out.Actions...)
		m.out.Reset()
	}
	return out
}

func (m *recovering) OnLog(_ *core.Context, _ *loop.Log, ev *disklog.Event, out []core.Action) []core.Action {
	switch {
	case ev == nil:
		m.nils++
	case ev.Kind == disklog.Durable && slices.Contains(ev.Tickets, m.ticket):
		m.durable++
	}
	return out
}

// While a loop serves no log, as while its disk recovers, the completions of the log's IO IDs go
// to its machine; once it serves the log the machine recovered, they go to the log, and the
// machine hears its events and a nil after each completion.
func TestALoopServesTheLogItsMachineRecovered(t *testing.T) {
	m := &recovering{}
	in := newInstant(t, m)
	in.lg.Serve(nil)
	in.l.Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: 1})
	in.run()
	if m.read != 1 || m.nils != 0 {
		t.Fatalf("with no log served the machine got %d reads and %d nil log events", m.read, m.nils)
	}
	m.log, m.pool = in.log, in.pool
	in.lg.Serve(in.log)
	in.l.Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: 2})
	in.run()
	if m.read != 1 || m.durable != 1 || m.nils == 0 {
		t.Fatalf("with the log served the machine got %d reads, %d durables and %d nil log events", m.read, m.durable, m.nils)
	}
}

// deviceOnLoop runs a node.Device as a storage node's device loop does: the first event starts it,
// and after every call the loop serves the store's log.
type deviceOnLoop struct {
	cfg            node.DeviceConfig
	lg             *loop.Log
	d              *node.Device
	out            node.DeviceOutput
	opened, failed int
	report         meta.OpenedDisk
	// ask is the newest session table ask the device made.
	ask uint64
}

func (m *deviceOnLoop) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	m.out.Reset()
	if m.d == nil {
		m.d = node.NewDevice(ctx, m.cfg, &m.out)
	} else {
		m.d.OnEvent(ctx, ev, &m.out)
	}
	return m.done(out)
}

func (m *deviceOnLoop) OnLog(ctx *core.Context, _ *loop.Log, ev *disklog.Event, out []core.Action) []core.Action {
	m.out.Reset()
	m.d.OnLog(ctx, ev, &m.out)
	return m.done(out)
}

func (m *deviceOnLoop) done(out []core.Action) []core.Action {
	var log *disklog.Log
	if st := m.d.Store(); st != nil {
		log = st.Log()
	}
	if m.lg.Log() != log {
		m.lg.Serve(log)
	}
	if m.out.Opened != nil {
		m.opened, m.report = m.opened+1, *m.out.Opened
	}
	if m.out.Failed {
		m.failed++
	}
	m.ask = max(m.ask, m.out.Ask)
	return append(out, m.out.Actions...)
}

// A device on a blank disk formats it on its loop and reports it opened, though the format's
// writes and flushes, which the loop hands the log, end with no log event.
func TestADeviceOnALoopReportsTheDiskItFormattedOpened(t *testing.T) {
	const blocks = 1 << 18
	s := sim.New(1, sim.Settings{PoolBytes: 1 << 20})
	n := s.AddNode(sim.DefaultClock, nil, func(*sim.Node) {})
	k := NewKernel(s, n.ID())
	pool := sim.NewPool(16<<20, 0)
	dr, err := NewDisk(k, sim.NewDisk(blocks), pool)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(dr.Close)
	m := &deviceOnLoop{cfg: node.DeviceConfig{Device: 7, Blocks: blocks, Store: storetest.Config(), Seed: 1}}
	l := loop.New(loop.Config{Node: n.ID(), Pool: pool}, k, m, diskDriver{dr})
	if err := l.AttachDisk(dr); err != nil {
		t.Fatal(err)
	}
	m.lg = l.AttachLog(nil, storetest.Config().Log, chunkstore.FirstLogIO, chunkstore.EndLogIO, 1)
	AddWithDisk(n, k, l, dr)
	l.Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: 1})
	for s.Step() {
	}
	if m.opened != 1 || m.failed != 0 || m.lg.Log() == nil {
		t.Fatalf("the device reported %d opened and %d failed, serving a log: %v", m.opened, m.failed, m.lg.Log() != nil)
	}
}
