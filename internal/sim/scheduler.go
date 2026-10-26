package sim

import (
	"bytes"
	"fmt"
	"slices"
	"strconv"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim/clock"
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

const (
	second = core.Tick(1_000_000)
	// heartbeat is the least time a rebooted node stays down.
	heartbeat = second
	// rateWindow is how long a node's clocks run before its raw rate may step, so that holders have bounded it first.
	rateWindow = 10 * second
	maxPause   = 5 * second
	maxDrift   = clock.Unit / 25
	// rebootDrift is the most a reboot changes the raw rate by.
	rebootDrift = clock.Unit / 50
	// maxStep is lease_drift / 2 at the default lease_drift of 1%.
	maxStep    = clock.Unit / 200
	maxSlew    = 226_000_000
	chronySlew = 83_333_333
)

// Settings are the scheduler's tunables; each chance is num/den, and a den of 0 means never.
type Settings struct {
	PoolBytes int
	PoolLive  int
	// NetDelay is every message's least delay and NetJitter the most a seeded draw adds per message.
	NetDelay  core.Tick
	NetJitter core.Tick
	// Drop loses a message and breaks its connection; Dup delivers a message twice in a row.
	DropNum uint64
	DropDen uint64
	DupNum  uint64
	DupDen  uint64
	// FailAfter is the most a broken connection takes to fail with PeerLost at both ends; 0 means it
	// never does, so a pair whose connection a drop or partition broke stays cut, Heal or not, and the
	// buffers sent on it stay held, until one end restarts.
	FailAfter core.Tick
	StaleNum  uint64
	StaleDen  uint64
	// Each step, with these chances, a node crashes, reboots or pauses, a partition is cut or healed, or a node's slew or raw rate changes.
	CrashNum     uint64
	CrashDen     uint64
	RebootNum    uint64
	RebootDen    uint64
	PauseNum     uint64
	PauseDen     uint64
	PartitionNum uint64
	PartitionDen uint64
	// With ClockDen above 0 every node's raw clock also runs within ±4% of real time.
	ClockNum uint64
	ClockDen uint64
	MsgType  func(m *core.Msg) uint64
}

// DefaultSettings are a 4 MiB pool per node, 50 µs network delay, connections that fail within 1 ms of breaking, no faults, and a stale Timer for one replaced or fired timer in 8.
func DefaultSettings() Settings {
	return Settings{PoolBytes: 4 << 20, NetDelay: 50, FailAfter: 1000, StaleNum: 1, StaleDen: 8}
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
	ChoosePeerLost
	ChooseResume
)

// Choice is one step the scheduler could take now: a disk action's completion, a message delivery, a due timer, an actor's operation, a connection failure reaching one end, or a node's restart or resume.
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
	set      Settings
	root     *core.Rand
	rand     *core.Rand
	now      core.Tick
	nodes    []*Node
	items    []item
	conns    []conn
	connID   uint64
	cuts     []link
	queues   []link
	choices  []Choice
	out      []core.Action
	trace    []byte
	steps    uint64
	contract error
	history  *History
}

type item struct {
	kind    ChoiceKind
	node    uint32
	loop    uint32
	act     core.Action
	issued  core.Tick
	at      core.Tick
	disk    *Disk
	linked  bool
	link    core.IOID
	failed  bool
	stale   bool
	from    core.NodeID
	fromGen uint64
	conn    uint64
	data    []byte
}

// Node is one simulated node: its clocks, disks and buffer pool, and the loops and actors of its current incarnation.
type Node struct {
	s        *Scheduler
	id       uint32
	rand     *core.Rand
	boot     Boot
	clock    *clock.Model
	clockAt  core.Tick
	stepped  bool
	rebooted bool
	up       bool
	until    core.Tick
	paused   bool
	lossy    bool
	gen      uint64
	pool     *Pool
	disks    []*Disk
	loops    []loop
	passes   []uint64
	actors   []actor
}

type loop struct {
	m    Machine
	disk *Disk
	last core.Clocks
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

// AddNode adds a node with the next ID, forking its source from the run's, makes its disks and boots it; with clock faults on, its raw rate is drawn from the scheduler's source.
func (s *Scheduler) AddNode(c Clock, disks []DiskSpec, boot Boot) *Node {
	rate := uint64(clock.Unit)
	if s.set.ClockDen > 0 {
		rate = clock.Unit - maxDrift + s.rand.Below(2*maxDrift+1)
	}
	n := &Node{s: s, id: uint32(len(s.nodes)), rand: s.root.Fork(), boot: boot, clockAt: s.now}
	n.clock = clock.New(s.now, core.Clocks{Boot: c.Now, Raw: c.Raw, Mono: c.Now}, rate)
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

// DriverContract is the first time a machine broke the driver contract the network checks: it freed a send's buffer before the network was done with it.
func (s *Scheduler) DriverContract() error { return s.contract }

// Choices lists the steps the scheduler could take now, in a fixed order, first moving time to the next one when none is due; the list is valid until the next step.
func (s *Scheduler) Choices() []Choice {
	for {
		var wake core.Tick
		s.choices, wake = s.collect(s.choices[:0])
		if len(s.choices) > 0 || wake == 0 {
			return s.choices
		}
		s.now = wake
	}
}

// Next is the time the next step falls due, now if one is due, without moving time; 0 when nothing is pending.
func (s *Scheduler) Next() core.Tick {
	var wake core.Tick
	s.choices, wake = s.collect(s.choices[:0])
	if len(s.choices) > 0 {
		return s.now
	}
	return wake
}

// Advance moves time to t, which no pending step falls due before.
func (s *Scheduler) Advance(t core.Tick) {
	if next := s.Next(); next != 0 && next < t || t < s.now {
		panic(fmt.Sprintf("sim: advancing to %d past a step due at %d, now %d", t, next, s.now))
	}
	s.now = t
}

// Idle reports, without moving time, whether nothing is pending but timers periodic accepts: no message, disk action, other timer, restart, resume or ready actor.
func (s *Scheduler) Idle(periodic func(core.TimerID) bool) bool {
	for i := range s.items {
		if it := &s.items[i]; it.kind != ChooseTimer || !periodic(it.act.Timer) {
			return false
		}
	}
	for _, n := range s.nodes {
		if !n.up || n.paused || slices.ContainsFunc(n.actors, func(a actor) bool { return a.a.Ready() }) {
			return false
		}
	}
	return true
}

// collect lists the choices due now and the earliest later time one falls due, 0 for none.
func (s *Scheduler) collect(cs []Choice) ([]Choice, core.Tick) {
	var wake core.Tick
	later := func(t core.Tick) {
		if wake == 0 || t < wake {
			wake = t
		}
	}
	for i := range s.items {
		it := &s.items[i]
		n := s.nodes[it.node]
		if n.paused {
			continue
		}
		switch it.kind {
		case ChooseTimer:
			if n.clock.Read(s.now).Boot < it.act.At {
				if t, ok := n.clock.When(s.now, it.act.At); ok {
					later(t)
				}
				continue
			}
		case ChooseDisk:
			if it.linked && s.writePending(it) {
				continue
			}
		case ChooseDeliver, ChoosePeerLost:
			if !s.head(i) {
				continue
			}
			if it.at > s.now {
				later(it.at)
				continue
			}
		}
		cs = append(cs, Choice{Kind: it.kind, Node: it.node, Loop: it.loop, From: it.from, Action: it.act, Stale: it.stale, ref: i})
	}
	for _, n := range s.nodes {
		switch {
		case !n.up || n.paused:
			if n.until > s.now {
				later(n.until)
				continue
			}
			kind := ChooseRestart
			if n.up {
				kind = ChooseResume
			}
			cs = append(cs, Choice{Kind: kind, Node: n.id})
		default:
			for i, a := range n.actors {
				if a.a.Ready() {
					cs = append(cs, Choice{Kind: ChooseActor, Node: n.id, Loop: a.loop, ref: i})
				}
			}
		}
	}
	return cs, wake
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

// Step takes one step chosen from the run's source, or a fault with the settings' chances, and reports false once nothing is pending.
func (s *Scheduler) Step() bool {
	if s.fault() {
		return true
	}
	cs := s.Choices()
	if len(cs) == 0 {
		return false
	}
	s.Take(cs[s.rand.Below(uint64(len(cs)))])
	return true
}

func (s *Scheduler) chance(num, den uint64) bool { return den > 0 && s.rand.Chance(num, den) }

// fault draws the settings' faults, reporting true for one that is a step of its own.
func (s *Scheduler) fault() bool {
	if s.chance(s.set.CrashNum, s.set.CrashDen) {
		if id, ok := s.pick(false); ok {
			s.CrashRandom(id)
			return true
		}
	}
	if s.chance(s.set.RebootNum, s.set.RebootDen) {
		if id, ok := s.pick(false); ok {
			s.Reboot(id)
			return true
		}
	}
	if s.chance(s.set.PauseNum, s.set.PauseDen) {
		if id, ok := s.pick(true); ok {
			stop := clock.Stop(1 + s.rand.Below(uint64(clock.HeldMigration)))
			s.Pause(id, stop, 1+core.Tick(s.rand.Below(uint64(maxPause))), s.rand.Chance(1, 2))
			return true
		}
	}
	if s.chance(s.set.PartitionNum, s.set.PartitionDen) && len(s.nodes) > 1 {
		if len(s.cuts) > 0 && s.rand.Chance(1, 2) {
			c := s.cuts[s.rand.Below(uint64(len(s.cuts)))]
			s.Heal(c.from, c.to)
		} else {
			a := uint32(s.rand.Below(uint64(len(s.nodes))))
			b := (a + 1 + uint32(s.rand.Below(uint64(len(s.nodes)-1)))) % uint32(len(s.nodes))
			s.Partition(a, b, s.rand.Chance(1, 2))
		}
	}
	if s.chance(s.set.ClockNum, s.set.ClockDen) {
		if id, ok := s.pick(false); ok {
			s.clockFault(s.nodes[id])
		}
	}
	return false
}

// pick draws a running node, one not paused when unpaused is set.
func (s *Scheduler) pick(unpaused bool) (uint32, bool) {
	var ids []uint32
	for _, n := range s.nodes {
		if n.up && !(unpaused && n.paused) {
			ids = append(ids, n.id)
		}
	}
	if len(ids) == 0 {
		return 0, false
	}
	return ids[s.rand.Below(uint64(len(ids)))], true
}

// clockFault steps the node's raw rate once per boot, after rateWindow, or else gives its time daemon a new slew: none, chrony's 8.33% either way, or any up to ±22.6%.
func (s *Scheduler) clockFault(n *Node) {
	if !n.stepped && s.now-n.clockAt >= rateWindow && s.rand.Chance(1, 2) {
		n.stepped = true
		d := 1 + s.rand.Below(maxStep)
		r := n.clock.Rate()
		if s.rand.Chance(1, 2) {
			r += d
		} else {
			r -= d
		}
		n.clock.SetRate(s.now, r)
		return
	}
	var slew int64
	switch s.rand.Below(3) {
	case 1:
		slew = chronySlew
		if s.rand.Chance(1, 2) {
			slew = -slew
		}
	case 2:
		slew = int64(s.rand.Below(2*maxSlew+1)) - maxSlew
	}
	n.clock.SetSlew(s.now, slew)
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
	case ChoosePeerLost:
		s.peerLost(s.remove(c.ref))
	case ChooseTimer:
		s.fire(s.remove(c.ref))
	case ChooseActor:
		n := s.nodes[c.Node]
		a := n.actors[c.ref]
		n.pass(a.loop, nil, a.a.Next)
	case ChooseRestart:
		s.Restart(c.Node)
	case ChooseResume:
		s.resume(s.nodes[c.Node])
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

func (s *Scheduler) fire(it item) {
	if !it.stale && s.set.StaleDen > 0 && s.rand.Chance(s.set.StaleNum, s.set.StaleDen) {
		dup := it
		dup.stale = true
		s.items = append(s.items, dup)
	}
	ev := core.Event{Kind: core.EvTimer, Timer: it.act.Timer}
	s.nodes[it.node].feed(it.loop, &ev)
}

// Crash crashes node id, dropping its loops, actors, timers, pending disk actions and connections, and lands the i-th block write of a disk, completed and unflushed then issued and not completed, only if landed says so.
func (s *Scheduler) Crash(id uint32, landed func(disk, i int, lba core.LBA) bool) {
	n := s.down(id, true)
	for d, disk := range n.disks {
		disk.Crash(func(i int, lba core.LBA) bool { return landed(d, i, lba) })
	}
}

// CrashRandom crashes node id with each block write unflushed or not yet completed landing from the scheduler's source.
func (s *Scheduler) CrashRandom(id uint32) { s.crashRandom(id, true) }

func (s *Scheduler) crashRandom(id uint32, sent bool) *Node {
	n := s.down(id, sent)
	for _, disk := range n.disks {
		disk.CrashRandom(s.rand)
	}
	return n
}

// Reboot crashes node id as CrashRandom does, losing what it sent that is still in the network as a power loss or kernel panic loses its socket buffers, and keeps it down for at least a heartbeat; it comes back with its clocks restarted near 0 and its raw rate changed by up to 2%.
func (s *Scheduler) Reboot(id uint32) {
	n := s.crashRandom(id, false)
	n.rebooted = true
	n.until = s.now + heartbeat
}

// down stops node id; with sent, its connections still deliver what it sent, as its kernel sends a crashed process's socket data.
func (s *Scheduler) down(id uint32, sent bool) *Node {
	n := s.nodes[id]
	if !n.up {
		panic(fmt.Sprintf("sim: crashing node %d, which is down", id))
	}
	s.steps++
	for i := range s.items {
		it := &s.items[i]
		if it.kind == ChooseDisk && it.node == id && it.act.Kind == core.ActDiskWrite {
			it.disk.inFlight(n.pool, &it.act)
		}
		if sent && it.kind == ChooseDeliver && it.from.Node == id && it.fromGen == n.gen && it.data == nil && n.pool.Valid(it.act.Msg.Buf) {
			it.data = slices.Clone(n.pool.Bytes(it.act.Msg.Buf))
		}
	}
	now := n.Context().Now
	for l := range n.loops {
		n.passes[l]++
		s.record(n, uint32(l), now, trace.Record{Name: trace.EvCrash})
	}
	if n.paused {
		n.clock.Resume(s.now)
	}
	n.up, n.paused, n.lossy, n.until = false, false, false, s.now
	s.closeConns(id, sent)
	s.items = slices.DeleteFunc(s.items, func(it item) bool { return it.node == id })
	n.gen++
	n.loops = nil
	n.actors = nil
	n.pool = nil
	return n
}

// Restart boots node id again with a fresh pool and its disks as the crash left them; a process restart keeps its clocks running, a reboot restarts them.
func (s *Scheduler) Restart(id uint32) {
	n := s.nodes[id]
	if n.up || s.now < n.until {
		panic(fmt.Sprintf("sim: restarting node %d, up %v, at %d before %d", id, n.up, s.now, n.until))
	}
	n.start()
}

func (n *Node) start() {
	s := n.s
	if n.rebooted {
		r := n.clock.Rate() + s.rand.Below(2*rebootDrift+1) - rebootDrift
		r = min(max(r, clock.Unit-maxDrift), clock.Unit+maxDrift)
		boot := second + core.Tick(s.rand.Below(uint64(second)))
		start := core.Clocks{Boot: boot, Raw: second + core.Tick(s.rand.Below(uint64(second))), Mono: boot}
		n.clock = clock.New(s.now, start, r)
		n.clockAt, n.stepped, n.rebooted = s.now, false, false
	}
	n.up = true
	n.pool = NewPool(s.set.PoolBytes, s.set.PoolLive)
	n.boot(n)
}

// Pause stands node id still for d from now, its clocks as stop says; the messages sent to it wait for it, or with lost are lost and break their connections.
func (s *Scheduler) Pause(id uint32, stop clock.Stop, d core.Tick, lost bool) {
	n := s.nodes[id]
	if !n.up || n.paused || stop == clock.Running {
		panic(fmt.Sprintf("sim: pausing node %d, up %v, paused %v, with stop %d", id, n.up, n.paused, stop))
	}
	s.steps++
	r := trace.Record{Name: trace.EvPause, Fields: [trace.MaxFields]uint64{uint64(stop), 0, uint64(d)}}
	if lost {
		r.Fields[1] = 1
	}
	now := n.Context().Now
	for l := range n.loops {
		n.passes[l]++
		s.record(n, uint32(l), now, r)
	}
	n.paused, n.lossy, n.until = true, lost, s.now+d
	n.clock.Stop(s.now, stop)
	if lost {
		s.breakConns(id)
	}
}

// resume ends the node's pause and, as its loop's first pass would, delivers every message that arrived while it stood still, in arrival order.
func (s *Scheduler) resume(n *Node) {
	n.clock.Resume(s.now)
	n.paused, n.lossy = false, false
	now := n.Context().Now
	for l := range n.loops {
		n.passes[l]++
		s.record(n, uint32(l), now, trace.Record{Name: trace.EvResume})
	}
	for n.up && !n.paused {
		i := s.arrived(n.id)
		if i < 0 {
			return
		}
		s.Take(Choice{Kind: s.items[i].kind, ref: i})
	}
}

// ID is the node's number, its place in node order.
func (n *Node) ID() uint32 { return n.id }

// Up reports whether the node is running.
func (n *Node) Up() bool { return n.up }

// Pool is the buffer pool of the node's current incarnation.
func (n *Node) Pool() *Pool { return n.pool }

// Clock is the model of the node's clocks, which a test may slew or step; a reboot replaces it.
func (n *Node) Clock() *clock.Model { return n.clock }

// Fork returns the next child of the node's source, one per machine or actor in the order the node creates them.
func (n *Node) Fork() *core.Rand { return n.rand.Fork() }

// Disk returns the node's i-th disk.
func (n *Node) Disk(i int) *Disk { return n.disks[i] }

// Context is the node's context now: its two clocks and its pool.
func (n *Node) Context() core.Context {
	c := n.clock.Read(n.s.now)
	return core.Context{Now: c.Boot, Raw: c.Raw, Pool: n.pool}
}

// Start runs m on the node's next loop, its disk actions going to disk, and returns the loop's ID.
func (n *Node) Start(m Machine, disk *Disk) core.NodeID {
	n.loops = append(n.loops, loop{m: m, disk: disk, last: n.clock.Read(n.s.now)})
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

// pass is one pass of loop l: Resumed first if core.Resumed sees a suspend or marker change since the loop's last pass, then ev or f.
func (n *Node) pass(l uint32, ev *core.Event, f func(ctx *core.Context, out []core.Action) []core.Action) {
	s := n.s
	n.passes[l]++
	ctx := n.Context()
	lp := &n.loops[l]
	s.out = s.out[:0]
	cur := n.clock.Read(s.now)
	if core.Resumed(lp.last, cur, clock.Threshold) {
		resumed := core.Event{Kind: core.EvResumed}
		s.record(n, l, ctx.Now, eventRecord(&resumed, s.set.MsgType))
		s.out = lp.m.OnEvent(&ctx, &resumed, s.out)
	}
	lp.last = cur
	if ev != nil {
		s.record(n, l, ctx.Now, eventRecord(ev, s.set.MsgType))
		s.out = lp.m.OnEvent(&ctx, ev, s.out)
	} else {
		s.out = f(&ctx, s.out)
	}
	rounder, _ := lp.m.(Rounder)
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
		it := item{node: n.id, loop: l, act: a, issued: s.now}
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
			if s.history != nil {
				s.history.sent(&a)
			}
			s.send(n, l, &a)
			continue
		case core.ActSetTimer:
			it.kind = ChooseTimer
			s.replaceTimer(n.id, l, a.Timer)
		default:
			panic(fmt.Sprintf("sim: loop %d.%d emits %+v", n.id, l, a))
		}
		s.items = append(s.items, it)
	}
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
