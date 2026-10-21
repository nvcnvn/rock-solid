package sim_test

import (
	"bytes"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
	"github.com/nvcnvn/rock-solid/internal/sim/toy"
	"github.com/nvcnvn/rock-solid/internal/trace"
)

const traceDirEnv = "SIM_SEED_TRACE_DIR"

const processSeeds = 20

// Run under GOMAXPROCS=1 and 8 and with -race too; TestSeedTracesMatchInOtherProcesses does
// the first two from whatever binary runs it, so a -race run checks its own build.
func TestTwoRunsOfASeedGiveByteIdenticalTraces(t *testing.T) {
	seeds.Run(t, 0, 50, func(seed uint64) {
		a := seedTrace(seed)
		if err := trace.Check(a); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		if err := sim.CompareTraces(a, seedTrace(seed)); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
	})
}

func TestSeedsZeroTo99GivePairwiseDifferentTraces(t *testing.T) {
	seen := map[uint64]uint64{}
	for seed := range uint64(100) {
		h := fnv.New64a()
		h.Write(seedTrace(seed))
		sum := h.Sum64()
		if other, ok := seen[sum]; ok {
			t.Fatalf("seeds %d and %d give one trace", other, seed)
		}
		seen[sum] = seed
	}
}

func TestWriteSeedTraces(t *testing.T) {
	dir := os.Getenv(traceDirEnv)
	if dir == "" {
		t.Skip("writes traces only for TestSeedTracesMatchInOtherProcesses")
	}
	for seed := range uint64(processSeeds) {
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("seed-%d", seed)), seedTrace(seed), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// The child runs every test of the package in shuffled order, so the traces are written
// before and after other tests, in a process with its own map hash seeds.
func TestSeedTracesMatchInOtherProcesses(t *testing.T) {
	if os.Getenv(traceDirEnv) != "" {
		t.Skip("inside the child process")
	}
	if testing.Short() {
		t.Skip("runs the package's tests twice more")
	}
	for _, procs := range []string{"1", "8"} {
		dir := t.TempDir()
		cmd := exec.Command(os.Args[0], "-test.run=.", "-test.shuffle=on")
		cmd.Env = append(os.Environ(), traceDirEnv+"="+dir, "GOMAXPROCS="+procs)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("GOMAXPROCS=%s: %v\n%s", procs, err, out)
		}
		for seed := range uint64(processSeeds) {
			got, err := os.ReadFile(filepath.Join(dir, fmt.Sprintf("seed-%d", seed)))
			if err != nil {
				t.Fatal(err)
			}
			if err := sim.CompareTraces(seedTrace(seed), got); err != nil {
				t.Fatalf("GOMAXPROCS=%s seed %d: %v", procs, seed, err)
			}
		}
	}
}

func TestCompareTracesNamesTheFirstDifferingLine(t *testing.T) {
	err := sim.CompareTraces([]byte("a\nb\nc\n"), []byte("a\nb\nd\n"))
	var d *sim.TraceDiffError
	if !errors.As(err, &d) || d.Line != 3 || d.A != "c" || d.B != "d" {
		t.Fatalf("%v", err)
	}
	err = sim.CompareTraces([]byte("a\n"), []byte("a\nb\n"))
	if !errors.As(err, &d) || d.Line != 2 || d.A != "(end of trace)" || d.B != "b" {
		t.Fatalf("%v", err)
	}
	if err := sim.CompareTraces([]byte("a\n"), []byte("a\n")); err != nil {
		t.Fatal(err)
	}
}

func TestTheToyProtocolMakesProgressUnderChaos(t *testing.T) {
	acked := 0
	for seed := range uint64(20) {
		c := toy.NewCluster(seed, chaosSettings(), 40, toy.NoBug)
		c.Scheduler().Run(3000)
		acked += c.Acked()
	}
	if acked < 100 {
		t.Fatalf("%d writes acknowledged over 20 seeds", acked)
	}
}

// recorder is a machine that logs its events and emits what the test queues for the next call.
type recorder struct {
	events  []core.Event
	at      []core.Context
	next    [][]core.Action
	unknown []core.IOID
	known   []core.IOID
}

func (r *recorder) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	r.events = append(r.events, *ev)
	r.at = append(r.at, *ctx)
	if ev.Kind == core.EvDiskDone && !slices.Contains(r.known, ev.IO) {
		r.unknown = append(r.unknown, ev.IO)
	}
	if len(r.next) == 0 {
		return out
	}
	acts := r.next[0]
	r.next = r.next[1:]
	return append(out, acts...)
}

func (r *recorder) emit(acts ...core.Action) func(*core.Context, []core.Action) []core.Action {
	for _, a := range acts {
		if a.IO != 0 {
			r.known = append(r.known, a.IO)
		}
	}
	return func(_ *core.Context, out []core.Action) []core.Action { return append(out, acts...) }
}

type single struct {
	s    *sim.Scheduler
	n    *sim.Node
	m    *recorder
	loop core.NodeID
}

func newSingle(seed uint64, set sim.Settings) *single {
	x := &single{s: sim.New(seed, set)}
	x.n = x.s.AddNode(sim.DefaultClock, []sim.DiskSpec{{Blocks: 64, Settings: sim.DefaultDiskSettings()}}, func(n *sim.Node) {
		x.m = &recorder{}
		x.loop = n.Start(x.m, n.Disk(0))
	})
	return x
}

func (x *single) block(fill byte) core.IOBuf {
	b, err := x.n.Pool().Alloc(core.BlockSize)
	if err != nil {
		panic(err)
	}
	for i := range x.n.Pool().Bytes(b) {
		x.n.Pool().Bytes(b)[i] = fill
	}
	return b
}

func write(io core.IOID, lba core.LBA, buf core.IOBuf) core.Action {
	return core.Action{Kind: core.ActDiskWrite, IO: io, LBA: lba, Buf: buf, Len: core.BlockSize}
}

func (x *single) take(t *testing.T, kind sim.ChoiceKind, io core.IOID) {
	t.Helper()
	for _, c := range x.s.Choices() {
		if c.Kind == kind && c.Action.IO == io {
			x.s.Take(c)
			return
		}
	}
	t.Fatalf("no choice %d for io%d in %+v", kind, io, x.s.Choices())
}

func TestAnIdleHourCostsOneStepAndBothClocksMoveTogether(t *testing.T) {
	x := newSingle(1, sim.DefaultSettings())
	hour := core.TickFromDuration(3600e9)
	at := x.n.Context().Now + hour
	x.n.Call(x.loop, x.m.emit(core.Action{Kind: core.ActSetTimer, Timer: 7, At: at}))
	if !x.s.Step() || x.s.Step() {
		t.Fatalf("an hour's timer took %d steps", x.s.Steps())
	}
	ctx := x.m.at[0]
	if x.m.events[0].Kind != core.EvTimer || ctx.Now != at || ctx.Raw-ctx.Now != sim.DefaultClock.Raw-sim.DefaultClock.Now {
		t.Fatalf("event %+v at %+v, want the timer at Now %d", x.m.events[0], ctx, at)
	}
}

func TestADiskCompletionArrivesNoSoonerThanItsLatency(t *testing.T) {
	x := newSingle(1, sim.DefaultSettings())
	start := x.n.Context().Now
	x.n.Call(x.loop, x.m.emit(core.Action{Kind: core.ActDiskFsync, IO: 1}))
	x.s.Run(10)
	if got := x.m.at[0].Now - start; got != sim.DefaultDiskLatency.Flush {
		t.Fatalf("flush completed after %d ticks", got)
	}
}

func TestCompletionsReorder(t *testing.T) {
	reordered := 0
	for seed := range uint64(20) {
		x := newSingle(seed, sim.DefaultSettings())
		x.n.Call(x.loop, x.m.emit(write(1, 1, x.block(1)), write(2, 2, x.block(2)), write(3, 3, x.block(3))))
		x.s.Run(10)
		var order []core.IOID
		for _, ev := range x.m.events {
			order = append(order, ev.IO)
		}
		if len(order) != 3 {
			t.Fatalf("seed %d: completions %v", seed, order)
		}
		if !slices.IsSorted(order) {
			reordered++
		}
	}
	if reordered == 0 {
		t.Fatal("no seed completed writes out of issue order")
	}
}

func TestALinkedFlushWaitsForItsWrite(t *testing.T) {
	x := newSingle(1, sim.DefaultSettings())
	x.n.Call(x.loop, x.m.emit(write(1, 1, x.block(1)), core.Action{Kind: core.ActDiskFsync, IO: 2, Linked: true}))
	for _, c := range x.s.Choices() {
		if c.Action.IO == 2 {
			t.Fatalf("the linked flush is a choice before its write completes: %+v", x.s.Choices())
		}
	}
	x.take(t, sim.ChooseDisk, 1)
	x.take(t, sim.ChooseDisk, 2)
}

func TestALinkedFlushAfterAFailedWriteFails(t *testing.T) {
	x := newSingle(1, sim.DefaultSettings())
	x.n.Disk(0).Inject(sim.Fault{Effect: sim.FailIO, When: sim.OnKind(core.ActDiskWrite)})
	x.n.Call(x.loop, x.m.emit(write(1, 1, x.block(1)), core.Action{Kind: core.ActDiskFsync, IO: 2, Linked: true}))
	x.s.Run(10)
	if len(x.m.events) != 2 || x.m.events[0].Err != core.DiskIO || x.m.events[1].Err != core.DiskIO {
		t.Fatalf("events %+v", x.m.events)
	}
}

// w2ReadsAsBefore is the depth-2 case: W1 and W2 issued, W1 completes, an unlinked flush runs,
// a crash lands nothing; W1 must be durable and W2's block must read as before.
func w2ReadsAsBefore(d *sim.Disk) bool {
	return d.Read(1, 1)[0] == 1 && d.Read(2, 1)[0] == 0
}

func TestAnUnlinkedFlushCoversOnlyWritesCompletedBeforeIt(t *testing.T) {
	x := newSingle(1, sim.DefaultSettings())
	x.n.Call(x.loop, x.m.emit(write(1, 1, x.block(1)), write(2, 2, x.block(2))))
	x.take(t, sim.ChooseDisk, 1)
	x.n.Call(x.loop, x.m.emit(core.Action{Kind: core.ActDiskFsync, IO: 3}))
	x.take(t, sim.ChooseDisk, 3)
	disk := x.n.Disk(0)
	x.s.Crash(0, func(int, int, core.LBA) bool { return false })
	if !w2ReadsAsBefore(disk) {
		t.Fatalf("W1 %x, W2 %x", disk.Read(1, 1)[0], disk.Read(2, 1)[0])
	}

	atIssue := sim.NewDisk(64)
	pool := sim.NewPool(1<<20, 0)
	for i, fill := range []byte{1, 2} {
		b, _ := pool.Alloc(core.BlockSize)
		for j := range pool.Bytes(b) {
			pool.Bytes(b)[j] = fill
		}
		w := write(core.IOID(i+1), core.LBA(i+1), b)
		atIssue.Execute(pool, &w)
	}
	atIssue.Execute(pool, &core.Action{Kind: core.ActDiskFsync, IO: 3})
	atIssue.Crash(func(int, core.LBA) bool { return false })
	if w2ReadsAsBefore(atIssue) {
		t.Fatal("the case passes a scheduler that applies writes at issue")
	}
}

func TestStaleTimersArriveAndAreIgnored(t *testing.T) {
	set := sim.DefaultSettings()
	set.StaleNum, set.StaleDen = 1, 1
	x := newSingle(1, set)
	now := x.n.Context().Now
	x.n.Call(x.loop, x.m.emit(core.Action{Kind: core.ActSetTimer, Timer: 1, At: now + 100}))
	x.n.Call(x.loop, x.m.emit(core.Action{Kind: core.ActSetTimer, Timer: 1, At: now + 500}))
	x.s.Run(10)
	var at []core.Tick
	for i, ev := range x.m.events {
		if ev.Kind != core.EvTimer || ev.Timer != 1 {
			t.Fatalf("event %+v", ev)
		}
		at = append(at, x.m.at[i].Now-now)
	}
	if !slices.Equal(at, []core.Tick{100, 500, 500}) {
		t.Fatalf("timer 1 delivered at %v after arming, want the replaced deadline, the live one and a stale repeat", at)
	}
	m := toy.New(core.NewRand(1), toy.Config{})
	ctx := core.Context{Now: 1000, Pool: sim.NewPool(core.BlockSize, 0)}
	m.Propose(&ctx, 1, nil)
	m.OnEvent(&ctx, &core.Event{Kind: core.EvTimer, Timer: toy.RetryTimer}, nil)
	if m.Stale() != 1 || !m.Armed() {
		t.Fatalf("toy took a timer before its deadline: stale %d, armed %v", m.Stale(), m.Armed())
	}
}

func TestMessagesDropAndReorder(t *testing.T) {
	set := sim.DefaultSettings()
	set.DropNum, set.DropDen = 1, 4
	s := sim.New(3, set)
	var to *recorder
	var from core.NodeID
	sender := s.AddNode(sim.DefaultClock, nil, func(n *sim.Node) { from = n.Start(&recorder{}, nil) })
	s.AddNode(sim.DefaultClock, nil, func(n *sim.Node) { to = &recorder{}; n.Start(to, nil) })
	const sent = 40
	sender.Call(from, func(_ *core.Context, out []core.Action) []core.Action {
		for i := range sent {
			out = append(out, core.Action{Kind: core.ActNetSend, Peer: core.NodeID{Node: 1}, Msg: writeMsg(uint64(i))})
		}
		return out
	})
	s.Run(100)
	var got []uint64
	for _, ev := range to.events {
		if ev.Kind != core.EvNetRecv || ev.Peer != from {
			t.Fatalf("event %+v", ev)
		}
		got = append(got, uint64(ev.Msg.Body[9]))
	}
	if len(got) == 0 || len(got) == sent || slices.IsSorted(got) {
		t.Fatalf("received %v of %d: want some dropped and the rest reordered", got, sent)
	}
}

func TestAMessageToACrashedIncarnationIsDropped(t *testing.T) {
	s := sim.New(1, sim.DefaultSettings())
	var from core.NodeID
	sender := s.AddNode(sim.DefaultClock, nil, func(n *sim.Node) { from = n.Start(&recorder{}, nil) })
	var to *recorder
	s.AddNode(sim.DefaultClock, nil, func(n *sim.Node) { to = &recorder{}; n.Start(to, nil) })
	sender.Call(from, func(_ *core.Context, out []core.Action) []core.Action {
		return append(out, core.Action{Kind: core.ActNetSend, Peer: core.NodeID{Node: 1}, Msg: writeMsg(1)})
	})
	s.Crash(1, func(int, int, core.LBA) bool { return true })
	s.Restart(1)
	s.Run(10)
	if len(to.events) != 0 {
		t.Fatalf("the restarted node got %+v", to.events)
	}
}

func TestCrashesAndRestartsAreSteps(t *testing.T) {
	set := chaosSettings()
	set.CrashNum, set.CrashDen = 1, 20
	c := toy.NewCluster(5, set, 40, toy.NoBug)
	c.Scheduler().Run(400)
	if b := c.Boots(); b[0]+b[1]+b[2] <= 3 {
		t.Fatalf("boots %v: no node crashed and restarted", b)
	}
}

func TestAnEventOutsideTheContractIsData(t *testing.T) {
	c := toy.NewCluster(1, sim.DefaultSettings(), 0, toy.NoBug)
	c.Scheduler().Node(1).Feed(core.NodeID{Node: 1}, core.Event{Kind: core.EvDiskDone, IO: 999})
	if c.Node(1).Unknown() != 1 {
		t.Fatalf("unknown %d", c.Node(1).Unknown())
	}
	if !bytes.Contains(c.Scheduler().Trace(), []byte("1000 1.0 event pass=1 DiskDone io=999 err=ok\n")) {
		t.Fatalf("trace:\n%s", c.Scheduler().Trace())
	}
}

func draws(r *core.Rand) []uint64 {
	return []uint64{r.Uint64(), r.Uint64(), r.Uint64()}
}

// forkStreams boots node 0 with machines A and B, extra more of its own, and node 1 with C,
// A drawing once more at start when aExtra, and returns B's and C's streams.
func forkStreams(aExtra bool, extra int) (b, c []uint64) {
	s := sim.New(9, sim.DefaultSettings())
	s.AddNode(sim.DefaultClock, nil, func(n *sim.Node) {
		a := n.Fork()
		if aExtra {
			a.Uint64()
		}
		b = draws(n.Fork())
		for range extra {
			n.Fork()
		}
	})
	s.AddNode(sim.DefaultClock, nil, func(n *sim.Node) { c = draws(n.Fork()) })
	return b, c
}

func TestADrawInOneMachineShiftsNoOtherStream(t *testing.T) {
	b, c := forkStreams(false, 0)
	b2, c2 := forkStreams(true, 0)
	if !slices.Equal(b, b2) || !slices.Equal(c, c2) {
		t.Fatal("a draw in A shifted B's or C's stream")
	}
	_, c3 := forkStreams(false, 3)
	if !slices.Equal(c, c3) {
		t.Fatal("machines added on node 0 shifted node 1's stream")
	}
	if slices.Equal(b, c) {
		t.Fatal("two machines share a stream")
	}
}

func TestARestartForksNewStreams(t *testing.T) {
	s := sim.New(9, sim.DefaultSettings())
	var streams [][]uint64
	s.AddNode(sim.DefaultClock, nil, func(n *sim.Node) { streams = append(streams, draws(n.Fork())) })
	s.CrashRandom(0)
	s.Restart(0)
	if len(streams) != 2 || slices.Equal(streams[0], streams[1]) {
		t.Fatalf("streams %v", streams)
	}
}

// outputMachine stands for a machine with its own output type, as disklog's Log is.
type outputMachine struct {
	pending core.IOID
	done    int
}

type output struct {
	Actions []core.Action
	Done    int
}

func (m *outputMachine) OnEvent(_ *core.Context, ev *core.Event, out *output) {
	if ev.Kind == core.EvDiskDone && ev.IO == m.pending {
		out.Done++
	}
}

func TestAMachineWithItsOwnOutputPlugsInThroughMachineFunc(t *testing.T) {
	m := &outputMachine{pending: 5}
	var o output
	adapter := sim.MachineFunc(func(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
		o.Actions = o.Actions[:0]
		m.OnEvent(ctx, ev, &o)
		m.done += o.Done
		o.Done = 0
		return append(out, o.Actions...)
	})
	s := sim.New(1, sim.DefaultSettings())
	n := s.AddNode(sim.DefaultClock, []sim.DiskSpec{{Blocks: 8, Settings: sim.DefaultDiskSettings()}}, func(n *sim.Node) {
		n.Start(adapter, n.Disk(0))
	})
	n.Call(core.NodeID{}, func(_ *core.Context, out []core.Action) []core.Action {
		return append(out, core.Action{Kind: core.ActDiskFsync, IO: 5})
	})
	s.Run(10)
	if m.done != 1 {
		t.Fatalf("done %d", m.done)
	}
}

func TestACrashDecidesWritesNotYetCompleted(t *testing.T) {
	landed, lost := 0, 0
	for seed := range uint64(32) {
		x := newSingle(seed, sim.DefaultSettings())
		x.n.Call(x.loop, x.m.emit(write(1, 1, x.block(1)), write(2, 2, x.block(2))))
		disk := x.n.Disk(0)
		if disk.Read(1, 1)[0] != 0 {
			t.Fatal("a write issued and not completed is visible to reads")
		}
		x.s.CrashRandom(0)
		switch disk.Read(1, 1)[0] {
		case 1:
			landed++
		case 0:
			lost++
		default:
			t.Fatalf("seed %d: W1's block reads %x", seed, disk.Read(1, 1)[0])
		}
	}
	if landed == 0 || lost == 0 {
		t.Fatalf("W1 landed under %d seeds and was lost under %d; want both", landed, lost)
	}
}

func TestACrashLandsWritesNotYetCompletedWhenToldTo(t *testing.T) {
	x := newSingle(1, sim.DefaultSettings())
	x.n.Call(x.loop, x.m.emit(write(1, 1, x.block(1)), write(2, 2, x.block(2))))
	x.take(t, sim.ChooseDisk, 1)
	var decided []core.LBA
	x.s.Crash(0, func(_, _ int, lba core.LBA) bool { decided = append(decided, lba); return true })
	if !slices.Equal(decided, []core.LBA{1, 2}) || x.n.Disk(0).Read(2, 1)[0] != 2 {
		t.Fatalf("decided %v, W2 reads %x", decided, x.n.Disk(0).Read(2, 1)[0])
	}
}

func TestTheTraceShowsCrashesRestartsAndDrops(t *testing.T) {
	set := sim.DefaultSettings()
	set.DropNum, set.DropDen = 1, 1
	s := toy.NewCluster(1, set, 1, toy.NoBug).Scheduler()
	s.Run(5)
	s.Crash(1, func(int, int, core.LBA) bool { return false })
	s.Restart(1)
	tr := s.Trace()
	for _, want := range []string{" 0.0 event pass=1 Drop peer=1 msg=write\n", " 1.0 event pass=1 Crash\n", " 1.0 event pass=2 Restart\n"} {
		if !bytes.Contains(tr, []byte(want)) {
			t.Fatalf("no %q in\n%s", want, tr)
		}
	}
	if err := trace.Check(tr); err != nil {
		t.Fatal(err)
	}
}

func machineStream(s *sim.Scheduler) []uint64 {
	var got []uint64
	s.AddNode(sim.DefaultClock, nil, func(n *sim.Node) { got = draws(n.Fork()) })
	return got
}

func TestSchedulerDrawsShiftNoNodeStream(t *testing.T) {
	quiet := sim.New(4, sim.DefaultSettings())
	quiet.AddNode(sim.DefaultClock, nil, func(n *sim.Node) { n.Start(&recorder{}, nil) })
	want := machineStream(quiet)

	set := sim.DefaultSettings()
	set.DropNum, set.DropDen = 1, 2
	busy := sim.New(4, set)
	sender := busy.AddNode(sim.DefaultClock, nil, func(n *sim.Node) { n.Start(&recorder{}, nil) })
	sender.Call(core.NodeID{}, func(_ *core.Context, out []core.Action) []core.Action {
		for i := range 20 {
			out = append(out, core.Action{Kind: core.ActNetSend, Peer: core.NodeID{}, Msg: writeMsg(uint64(i))})
		}
		return out
	})
	busy.Run(50)
	if got := machineStream(busy); !slices.Equal(got, want) {
		t.Fatalf("scheduler draws shifted node 1's stream: %v, want %v", got, want)
	}

	root := core.NewRand(4)
	root.Fork()
	root.Fork()
	if derived := draws(root.Fork().Fork()); !slices.Equal(derived, want) {
		t.Fatalf("node 1's first machine draws %v, want the run's third child's first child %v", want, derived)
	}
}

// roundMachine numbers each disk write and flush it emits with the round set before the call.
type roundMachine struct {
	recorder
	rounds map[core.IOID]uint64
}

func (m *roundMachine) Round(a *core.Action) uint64 { return m.rounds[a.IO] }

func TestARoundersWritesAndFlushesCarryTheirRound(t *testing.T) {
	s := sim.New(1, sim.DefaultSettings())
	m := &roundMachine{rounds: map[core.IOID]uint64{1: 7, 2: 7}}
	var loop core.NodeID
	n := s.AddNode(sim.DefaultClock, []sim.DiskSpec{{Blocks: 8, Settings: sim.DefaultDiskSettings()}}, func(n *sim.Node) {
		loop = n.Start(m, n.Disk(0))
	})
	buf, _ := n.Pool().Alloc(core.BlockSize)
	n.Call(loop, m.emit(write(1, 1, buf), core.Action{Kind: core.ActDiskFsync, IO: 2, Linked: true}, write(3, 2, buf)))
	n.Output(loop, trace.Record{Name: trace.OutDurable, Fields: [trace.MaxFields]uint64{7, 7}})
	want := "1000 0.0 action pass=1 DiskWrite round=7 io=1 lba=1 len=4096 vec=1\n" +
		"1000 0.0 action pass=1 DiskFsync round=7 io=2 linked=1\n" +
		"1000 0.0 action pass=1 DiskWrite round=0 io=3 lba=2 len=4096 vec=1\n" +
		"1000 0.0 output pass=1 Durable round=7 lsn=7\n"
	if got := string(s.Trace()); got != want {
		t.Fatalf("trace\n%s\nwant\n%s", got, want)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("an action record was accepted as an output")
		}
	}()
	n.Output(loop, trace.Record{Name: trace.ActDiskWrite})
}

// trimReader writes a block, flushes it, trims it and reads it back, assuming a trimmed block reads as zeros.
type trimReader struct {
	buf    core.IOBuf
	zeros  bool
	reread bool
}

func (m *trimReader) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	switch ev.IO {
	case 2:
		return append(out, core.Action{Kind: core.ActDiskTrim, IO: 3, LBA: 1, Len: core.BlockSize})
	case 3:
		return append(out, core.Action{Kind: core.ActDiskRead, IO: 4, LBA: 1, Buf: m.buf, Len: core.BlockSize})
	case 4:
		m.reread = true
		m.zeros = !slices.ContainsFunc(ctx.Pool.Bytes(m.buf), func(b byte) bool { return b != 0 })
	}
	return out
}

func TestAMachineRelyingOnTrimmedBlocksReadingZerosFailsUnderSomeSeed(t *testing.T) {
	held, failed := 0, 0
	for seed := range uint64(32) {
		s := sim.New(seed, sim.DefaultSettings())
		m := &trimReader{}
		var loop core.NodeID
		n := s.AddNode(sim.DefaultClock, []sim.DiskSpec{{Blocks: 8, Settings: sim.DefaultDiskSettings()}}, func(n *sim.Node) {
			loop = n.Start(m, n.Disk(0))
		})
		m.buf, _ = n.Pool().Alloc(core.BlockSize)
		for i := range n.Pool().Bytes(m.buf) {
			n.Pool().Bytes(m.buf)[i] = 1
		}
		n.Call(loop, func(_ *core.Context, out []core.Action) []core.Action {
			return append(out, write(1, 1, m.buf), core.Action{Kind: core.ActDiskFsync, IO: 2, Linked: true})
		})
		s.Run(100)
		if !m.reread {
			t.Fatalf("seed %d: the machine never read its trimmed block", seed)
		}
		if m.zeros {
			held++
		} else {
			failed++
		}
	}
	if held == 0 || failed == 0 {
		t.Fatalf("the zeros assumption held under %d seeds and failed under %d; want both", held, failed)
	}
}
