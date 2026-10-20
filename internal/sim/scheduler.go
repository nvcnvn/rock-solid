package sim

import (
	"bytes"
	"fmt"
	"slices"
	"strconv"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/trace"
)

// Machine is a sans-IO state machine the scheduler drives on one loop of a node.
type Machine interface {
	OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action
}

// MachineFunc adapts a function, such as a wrapper over a machine with its own output type, to Machine.
type MachineFunc func(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action

// OnEvent calls f.
func (f MachineFunc) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	return f(ctx, ev, out)
}

// Rounder is a Machine whose disk writes and flushes belong to numbered rounds, which their trace records carry.
type Rounder interface {
	Round(a *core.Action) uint64
}

// Actor is a client, worker job or event loop the scheduler steps on behalf of one loop, interleaved with the machines' events.
type Actor interface {
	// Ready reports whether the actor has an operation to take now.
	Ready() bool
	// Next takes the actor's next operation, appending the actions its loop's machine emits for it.
	Next(ctx *core.Context, out []core.Action) []core.Action
}

// Boot starts a node's machines and actors, at its first start and at every restart.
type Boot func(n *Node)

// Clock is where a node's CLOCK_BOOTTIME and CLOCK_MONOTONIC_RAW readings start, in ticks.
type Clock struct {
	Now core.Tick
	Raw core.Tick
}

// DefaultClock starts Now at 1,000 and Raw at 1,226, so a machine that mixes the clocks up reads the wrong one.
var DefaultClock = Clock{Now: 1000, Raw: 1226}

// Settings are the scheduler's tunables; each chance is num/den, and a den of 0 means never.
type Settings struct {
	PoolBytes int
	PoolLive  int
	NetDelay  core.Tick
	DropNum   uint64
	DropDen   uint64
	StaleNum  uint64
	StaleDen  uint64
	CrashNum  uint64
	CrashDen  uint64
	MsgType   func(m *core.Msg) uint64
}

// DefaultSettings are a 4 MiB pool per node, 50 µs network delay, no drops or crashes, and a stale Timer for one replaced or fired timer in 8.
func DefaultSettings() Settings {
	return Settings{PoolBytes: 4 << 20, NetDelay: 50, StaleNum: 1, StaleDen: 8}
}

// ChoiceKind is what a step does.
type ChoiceKind uint8

// Choice kinds.
const (
	ChooseDisk ChoiceKind = iota + 1
	ChooseDeliver
	ChooseTimer
	ChooseActor
	ChooseRestart
)

// Choice is one step the scheduler could take now: a disk action's completion, a message delivery, a due timer, an actor's operation or a node's restart.
type Choice struct {
	Kind   ChoiceKind
	Node   uint32
	Loop   uint32
	From   core.NodeID
	Action core.Action
	Stale  bool
	ref    int
}

// Scheduler runs simulated nodes, their disks, actors and messages in one goroutine from one seed, in simulated time.
type Scheduler struct {
	set     Settings
	root    *core.Rand
	rand    *core.Rand
	now     core.Tick
	nodes   []*Node
	items   []item
	choices []Choice
	out     []core.Action
	trace   []byte
	steps   uint64
}

type item struct {
	kind   ChoiceKind
	node   uint32
	loop   uint32
	gen    uint64
	act    core.Action
	issued core.Tick
	at     core.Tick
	disk   *Disk
	linked bool
	link   core.IOID
	failed bool
	stale  bool
	from   core.NodeID
	data   []byte
}

// Node is one simulated node: its clocks, disks and buffer pool, and the loops and actors of its current incarnation.
type Node struct {
	s      *Scheduler
	id     uint32
	rand   *core.Rand
	boot   Boot
	clock  Clock
	up     bool
	gen    uint64
	pool   *Pool
	disks  []*Disk
	loops  []loop
	passes []uint64
	actors []actor
}

type loop struct {
	m    Machine
	disk *Disk
}

type actor struct {
	loop uint32
	a    Actor
}

// New makes a scheduler whose source and every node's are forked from seed's, the scheduler's first.
func New(seed uint64, set Settings) *Scheduler {
	if set.PoolBytes <= 0 {
		panic(fmt.Sprintf("sim: pool of %d bytes", set.PoolBytes))
	}
	root := core.NewRand(seed)
	return &Scheduler{set: set, root: root, rand: root.Fork()}
}

// DiskSpec is a disk a node is made with, which outlives its crashes; its seed is drawn from the node's source in place of Settings.Seed.
type DiskSpec struct {
	Blocks   uint64
	Settings DiskSettings
}

// AddNode adds a node with the next ID, forking its source from the run's, makes its disks and boots it.
func (s *Scheduler) AddNode(c Clock, disks []DiskSpec, boot Boot) *Node {
	n := &Node{s: s, id: uint32(len(s.nodes)), rand: s.root.Fork(), boot: boot, clock: c}
	s.nodes = append(s.nodes, n)
	for _, d := range disks {
		set := d.Settings
		set.Seed = n.rand.Uint64()
		n.disks = append(n.disks, NewDiskWith(d.Blocks, set))
	}
	n.start()
	return n
}

// Node returns node id.
func (s *Scheduler) Node(id uint32) *Node { return s.nodes[id] }

// Time is the run's simulated time, which starts at 0 and only steps advance.
func (s *Scheduler) Time() core.Tick { return s.now }

// Steps is the number of steps taken.
func (s *Scheduler) Steps() uint64 { return s.steps }

// Trace is the run's trace, one trace.Record line per event delivered and action emitted.
func (s *Scheduler) Trace() []byte { return s.trace }

// Choices lists the steps the scheduler could take now, in a fixed order; the list is valid until the next step.
func (s *Scheduler) Choices() []Choice {
	s.choices = s.collect(s.choices[:0])
	if len(s.choices) > 0 {
		return s.choices
	}
	next, ok := s.nextTimer()
	if !ok {
		return s.choices
	}
	s.now = next
	s.choices = s.collect(s.choices[:0])
	return s.choices
}

func (s *Scheduler) collect(cs []Choice) []Choice {
	for i := range s.items {
		it := &s.items[i]
		if it.kind == ChooseTimer && it.at > s.now {
			continue
		}
		if it.kind == ChooseDisk && it.linked && s.writePending(it) {
			continue
		}
		cs = append(cs, Choice{Kind: it.kind, Node: it.node, Loop: it.loop, From: it.from, Action: it.act, Stale: it.stale, ref: i})
	}
	for _, n := range s.nodes {
		if !n.up {
			cs = append(cs, Choice{Kind: ChooseRestart, Node: n.id})
			continue
		}
		for i, a := range n.actors {
			if a.a.Ready() {
				cs = append(cs, Choice{Kind: ChooseActor, Node: n.id, Loop: a.loop, ref: i})
			}
		}
	}
	return cs
}

func (s *Scheduler) writePending(fsync *item) bool {
	for i := range s.items {
		it := &s.items[i]
		if it.kind == ChooseDisk && it.act.Kind == core.ActDiskWrite && it.node == fsync.node && it.loop == fsync.loop && it.act.IO == fsync.link {
			return true
		}
	}
	return false
}

func (s *Scheduler) nextTimer() (core.Tick, bool) {
	var next core.Tick
	found := false
	for i := range s.items {
		if it := &s.items[i]; it.kind == ChooseTimer && (!found || it.at < next) {
			next, found = it.at, true
		}
	}
	return next, found
}

// Step takes one step chosen from the run's source, a crash with the settings' chance, and reports false once nothing is pending.
func (s *Scheduler) Step() bool {
	if s.set.CrashDen > 0 && s.rand.Chance(s.set.CrashNum, s.set.CrashDen) {
		var up []uint32
		for _, n := range s.nodes {
			if n.up {
				up = append(up, n.id)
			}
		}
		if len(up) > 0 {
			s.CrashRandom(up[s.rand.Below(uint64(len(up)))])
			return true
		}
	}
	cs := s.Choices()
	if len(cs) == 0 {
		return false
	}
	s.Take(cs[s.rand.Below(uint64(len(cs)))])
	return true
}

// Run takes up to max steps and returns how many it took.
func (s *Scheduler) Run(max int) int {
	for i := range max {
		if !s.Step() {
			return i
		}
	}
	return max
}

// Take takes a step from the latest Choices.
func (s *Scheduler) Take(c Choice) {
	s.steps++
	switch c.Kind {
	case ChooseDisk:
		s.complete(s.remove(c.ref))
	case ChooseDeliver:
		s.deliver(s.remove(c.ref))
	case ChooseTimer:
		s.fire(s.remove(c.ref))
	case ChooseActor:
		n := s.nodes[c.Node]
		a := n.actors[c.ref]
		n.pass(a.loop, nil, a.a.Next)
	case ChooseRestart:
		s.Restart(c.Node)
	default:
		panic(fmt.Sprintf("sim: choice %+v", c))
	}
}

func (s *Scheduler) remove(i int) item {
	it := s.items[i]
	s.items = slices.Delete(s.items, i, i+1)
	return it
}

func (s *Scheduler) complete(it item) {
	n := s.nodes[it.node]
	var ev core.Event
	lat := core.Tick(0)
	if it.failed {
		ev = core.Event{Kind: core.EvDiskDone, IO: it.act.IO, Err: core.DiskIO}
	} else {
		ev, lat = it.disk.ExecuteTimed(n.pool, &it.act)
	}
	s.now = max(s.now, it.issued+lat)
	if it.act.Kind == core.ActDiskWrite && ev.Err != core.DiskOK {
		for i := range s.items {
			f := &s.items[i]
			if f.kind == ChooseDisk && f.linked && f.node == it.node && f.loop == it.loop && f.link == it.act.IO {
				f.failed = true
			}
		}
	}
	n.feed(it.loop, &ev)
}

func (s *Scheduler) drop(from, to core.NodeID, m *core.Msg) {
	n := s.nodes[from.Node]
	s.record(n, from.Loop, n.Context().Now, trace.Record{Name: trace.EvDrop, Fields: [trace.MaxFields]uint64{trace.Peer(to), msgType(m, s.set.MsgType)}})
}

func (s *Scheduler) deliver(it item) {
	s.now = max(s.now, it.at)
	n := s.nodes[it.node]
	to := core.NodeID{Node: it.node, Loop: it.loop}
	if !n.up || n.gen != it.gen || int(it.loop) >= len(n.loops) {
		s.drop(it.from, to, &it.act.Msg)
		return
	}
	ev := core.Event{Kind: core.EvNetRecv, Peer: it.from, Msg: it.act.Msg}
	ev.Msg.Buf = core.NoBuf
	if it.data != nil {
		b, err := n.pool.Alloc(len(it.data))
		if err != nil {
			s.drop(it.from, to, &it.act.Msg)
			return
		}
		copy(n.pool.Bytes(b), it.data)
		ev.Msg.Buf = b
	}
	n.feed(it.loop, &ev)
}

func (s *Scheduler) fire(it item) {
	if !it.stale && s.set.StaleDen > 0 && s.rand.Chance(s.set.StaleNum, s.set.StaleDen) {
		dup := it
		dup.stale = true
		s.items = append(s.items, dup)
	}
	ev := core.Event{Kind: core.EvTimer, Timer: it.act.Timer}
	s.nodes[it.node].feed(it.loop, &ev)
}

// Crash crashes node id, dropping its loops, actors, timers and pending disk actions, and lands the i-th block write of a disk, completed and unflushed then issued and not completed, only if landed says so.
func (s *Scheduler) Crash(id uint32, landed func(disk, i int, lba core.LBA) bool) {
	n := s.down(id)
	for d, disk := range n.disks {
		disk.Crash(func(i int, lba core.LBA) bool { return landed(d, i, lba) })
	}
}

// CrashRandom crashes node id with each block write unflushed or not yet completed landing from the scheduler's source.
func (s *Scheduler) CrashRandom(id uint32) {
	n := s.down(id)
	for _, disk := range n.disks {
		disk.CrashRandom(s.rand)
	}
}

func (s *Scheduler) down(id uint32) *Node {
	n := s.nodes[id]
	if !n.up {
		panic(fmt.Sprintf("sim: crashing node %d, which is down", id))
	}
	s.steps++
	for i := range s.items {
		if it := &s.items[i]; it.kind == ChooseDisk && it.node == id && it.act.Kind == core.ActDiskWrite {
			it.disk.inFlight(n.pool, &it.act)
		}
	}
	now := n.Context().Now
	for l := range n.loops {
		n.passes[l]++
		s.record(n, uint32(l), now, trace.Record{Name: trace.EvCrash})
	}
	s.items = slices.DeleteFunc(s.items, func(it item) bool {
		return it.node == id && it.kind != ChooseDeliver
	})
	n.up = false
	n.gen++
	n.loops = nil
	n.actors = nil
	n.pool = nil
	return n
}

// Restart boots node id again with a fresh pool, its disks as the crash left them and its clocks running on.
func (s *Scheduler) Restart(id uint32) {
	n := s.nodes[id]
	if n.up {
		panic(fmt.Sprintf("sim: restarting node %d, which is up", id))
	}
	n.start()
}

func (n *Node) start() {
	n.up = true
	n.pool = NewPool(n.s.set.PoolBytes, n.s.set.PoolLive)
	n.boot(n)
}

// ID is the node's number, its place in node order.
func (n *Node) ID() uint32 { return n.id }

// Up reports whether the node is running.
func (n *Node) Up() bool { return n.up }

// Pool is the buffer pool of the node's current incarnation.
func (n *Node) Pool() *Pool { return n.pool }

// Fork returns the next child of the node's source, one per machine or actor in the order the node creates them.
func (n *Node) Fork() *core.Rand { return n.rand.Fork() }

// Disk returns the node's i-th disk.
func (n *Node) Disk(i int) *Disk { return n.disks[i] }

// Context is the node's context now: its two clocks and its pool.
func (n *Node) Context() core.Context {
	return core.Context{Now: n.clock.Now + n.s.now, Raw: n.clock.Raw + n.s.now, Pool: n.pool}
}

// Start runs m on the node's next loop, its disk actions going to disk, and returns the loop's ID.
func (n *Node) Start(m Machine, disk *Disk) core.NodeID {
	n.loops = append(n.loops, loop{m: m, disk: disk})
	l := uint32(len(n.loops) - 1)
	if len(n.loops) > len(n.passes) {
		n.passes = append(n.passes, 0)
	}
	if n.gen > 0 {
		n.passes[l]++
		n.s.record(n, l, n.Context().Now, trace.Record{Name: trace.EvRestart})
	}
	return core.NodeID{Node: n.id, Loop: l}
}

// AddActor adds an actor whose operations the scheduler takes on behalf of loop id, until the node crashes.
func (n *Node) AddActor(id core.NodeID, a Actor) {
	n.checkLoop(id)
	n.actors = append(n.actors, actor{loop: id.Loop, a: a})
}

// Call runs f now as one pass of loop id, as a driver calling into the machine, and executes the actions it appends.
func (n *Node) Call(id core.NodeID, f func(ctx *core.Context, out []core.Action) []core.Action) {
	n.checkLoop(id)
	n.pass(id.Loop, nil, f)
}

// Feed hands loop id an event now, outside any pending step.
func (n *Node) Feed(id core.NodeID, ev core.Event) {
	n.checkLoop(id)
	n.feed(id.Loop, &ev)
}

// Output records r, an output the machine on loop id hands its owner, in the loop's current pass.
func (n *Node) Output(id core.NodeID, r trace.Record) {
	n.checkLoop(id)
	if r.Name.Kind() != trace.KindOutput {
		panic(fmt.Sprintf("sim: output record %v", r.Name))
	}
	n.s.record(n, id.Loop, n.Context().Now, r)
}

func (n *Node) checkLoop(id core.NodeID) {
	if !n.up || id.Node != n.id || int(id.Loop) >= len(n.loops) {
		panic(fmt.Sprintf("sim: node %d has no running loop %+v", n.id, id))
	}
}

func (n *Node) feed(l uint32, ev *core.Event) {
	n.pass(l, ev, nil)
}

func (n *Node) pass(l uint32, ev *core.Event, f func(ctx *core.Context, out []core.Action) []core.Action) {
	s := n.s
	n.passes[l]++
	ctx := n.Context()
	if ev != nil {
		s.record(n, l, ctx.Now, eventRecord(ev, s.set.MsgType))
		s.out = n.loops[l].m.OnEvent(&ctx, ev, s.out[:0])
	} else {
		s.out = f(&ctx, s.out[:0])
	}
	rounder, _ := n.loops[l].m.(Rounder)
	for i := range s.out {
		r := actionRecord(&s.out[i], s.set.MsgType)
		if rounder != nil && (r.Name == trace.ActDiskWrite || r.Name == trace.ActDiskFsync) {
			r.Fields[0] = rounder.Round(&s.out[i])
		}
		s.record(n, l, ctx.Now, r)
	}
	n.absorb(l, s.out)
}

func (n *Node) absorb(l uint32, acts []core.Action) {
	s := n.s
	for i := range acts {
		a := acts[i]
		it := item{node: n.id, loop: l, gen: n.gen, act: a, issued: s.now}
		switch a.Kind {
		case core.ActDiskRead, core.ActDiskWrite, core.ActDiskFsync, core.ActDiskTrim:
			it.kind = ChooseDisk
			it.disk = n.loops[l].disk
			if it.disk == nil {
				panic(fmt.Sprintf("sim: loop %d.%d has no disk for %+v", n.id, l, a))
			}
			it.act.Vec = slices.Clone(a.Vec)
			if a.Kind == core.ActDiskFsync && a.Linked {
				if i == 0 || acts[i-1].Kind != core.ActDiskWrite {
					panic(fmt.Sprintf("sim: loop %d.%d links flush io%d to no write", n.id, l, a.IO))
				}
				it.linked, it.link = true, acts[i-1].IO
			}
		case core.ActNetSend:
			if int(a.Peer.Node) >= len(s.nodes) || s.set.DropDen > 0 && s.rand.Chance(s.set.DropNum, s.set.DropDen) {
				s.drop(core.NodeID{Node: n.id, Loop: l}, a.Peer, &a.Msg)
				continue
			}
			to := s.nodes[a.Peer.Node]
			it.kind = ChooseDeliver
			it.from = core.NodeID{Node: n.id, Loop: l}
			it.node, it.loop, it.gen = to.id, a.Peer.Loop, to.gen
			it.at = s.now + s.set.NetDelay
			if a.Msg.Buf != core.NoBuf {
				it.data = slices.Clone(n.pool.Bytes(a.Msg.Buf))
			}
		case core.ActSetTimer:
			it.kind = ChooseTimer
			it.at = n.deadline(a.At)
			s.replaceTimer(n.id, l, a.Timer)
		default:
			panic(fmt.Sprintf("sim: loop %d.%d emits %+v", n.id, l, a))
		}
		s.items = append(s.items, it)
	}
}

func (n *Node) deadline(at core.Tick) core.Tick {
	if at <= n.clock.Now {
		return 0
	}
	return at - n.clock.Now
}

func (s *Scheduler) replaceTimer(node, l uint32, t core.TimerID) {
	for i := range s.items {
		it := &s.items[i]
		if it.kind != ChooseTimer || it.stale || it.node != node || it.loop != l || it.act.Timer != t {
			continue
		}
		if s.set.StaleDen > 0 && s.rand.Chance(s.set.StaleNum, s.set.StaleDen) {
			it.stale = true
		} else {
			s.items = slices.Delete(s.items, i, i+1)
		}
		return
	}
}

func (s *Scheduler) record(n *Node, l uint32, now core.Tick, r trace.Record) {
	r.Tick = now
	r.Pass = n.passes[l]
	r.Node = n.id
	r.Loop = uint16(l)
	r.Sim = true
	s.trace = r.Append(s.trace)
	s.trace = append(s.trace, '\n')
}

func msgType(m *core.Msg, f func(*core.Msg) uint64) uint64 {
	if f != nil {
		return f(m)
	}
	if m.Len == 0 {
		return 0
	}
	return uint64(m.Body[0])
}

func eventRecord(ev *core.Event, mt func(*core.Msg) uint64) trace.Record {
	var r trace.Record
	switch ev.Kind {
	case core.EvNetRecv:
		r.Name = trace.EvNetRecv
		r.Fields = [trace.MaxFields]uint64{trace.Peer(ev.Peer), msgType(&ev.Msg, mt)}
	case core.EvPeerLost:
		r.Name = trace.EvPeerLost
		r.Fields[0] = trace.Peer(ev.Peer)
	case core.EvDiskDone:
		r.Name = trace.EvDiskDone
		r.Fields = [trace.MaxFields]uint64{uint64(ev.IO), uint64(ev.Err)}
	case core.EvTimer:
		r.Name = trace.EvTimer
		r.Fields[0] = uint64(ev.Timer)
	case core.EvResumed:
		r.Name = trace.EvResumed
	default:
		panic(fmt.Sprintf("sim: event %+v", *ev))
	}
	return r
}

func actionRecord(a *core.Action, mt func(*core.Msg) uint64) trace.Record {
	var r trace.Record
	switch a.Kind {
	case core.ActNetSend:
		r.Name = trace.ActNetSend
		r.Fields = [trace.MaxFields]uint64{trace.Peer(a.Peer), msgType(&a.Msg, mt)}
	case core.ActDiskRead:
		r.Name = trace.ActDiskRead
		r.Fields = [trace.MaxFields]uint64{uint64(a.IO), uint64(a.LBA), a.Len}
	case core.ActDiskWrite:
		r.Name = trace.ActDiskWrite
		r.Fields = [trace.MaxFields]uint64{0, uint64(a.IO), uint64(a.LBA), a.Len, uint64(max(len(a.Vec), 1))}
	case core.ActDiskFsync:
		r.Name = trace.ActDiskFsync
		r.Fields = [trace.MaxFields]uint64{0, uint64(a.IO), 0}
		if a.Linked {
			r.Fields[2] = 1
		}
	case core.ActDiskTrim:
		r.Name = trace.ActDiskTrim
		r.Fields = [trace.MaxFields]uint64{uint64(a.IO), uint64(a.LBA), a.Len}
	case core.ActSetTimer:
		r.Name = trace.ActSetTimer
		r.Fields = [trace.MaxFields]uint64{uint64(a.Timer), uint64(a.At)}
	default:
		panic(fmt.Sprintf("sim: action %+v", *a))
	}
	return r
}

// TraceDiffError names the first line where two traces differ, numbered from 1.
type TraceDiffError struct {
	Line int
	A    string
	B    string
}

// Error quotes the line from each trace.
func (e *TraceDiffError) Error() string {
	return "traces differ at line " + strconv.Itoa(e.Line) + ":\n  " + e.A + "\n  " + e.B
}

// CompareTraces returns nil for byte-identical traces, or a TraceDiffError naming the first differing line.
func CompareTraces(a, b []byte) error {
	if bytes.Equal(a, b) {
		return nil
	}
	la := bytes.SplitAfter(a, []byte{'\n'})
	lb := bytes.SplitAfter(b, []byte{'\n'})
	for i := 0; ; i++ {
		if i >= len(la) || i >= len(lb) || !bytes.Equal(la[i], lb[i]) {
			return &TraceDiffError{Line: i + 1, A: lineAt(la, i), B: lineAt(lb, i)}
		}
	}
}

func lineAt(lines [][]byte, i int) string {
	if i >= len(lines) || len(lines[i]) == 0 {
		return "(end of trace)"
	}
	return string(bytes.TrimSuffix(lines[i], []byte{'\n'}))
}
