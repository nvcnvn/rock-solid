package looptest

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/loop"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
	"github.com/nvcnvn/rock-solid/internal/trace"
	"golang.org/x/sys/unix"
)

const budget = 512

// gossip sends messages that hop between loops, over connections and through inboxes, so a run
// exercises every path of a pass.
type gossip struct {
	self   int
	ids    []core.NodeID
	loops  []*loop.Loop
	rand   *core.Rand
	rounds int
	pad    int
	burst  int
	sent   int
	got    int
	sum    uint64
	post   core.Event
}

func (g *gossip) pick() core.NodeID { return g.ids[g.rand.Below(uint64(len(g.ids)))] }

func (g *gossip) send(out []core.Action, to core.NodeID, hops byte, v uint64) []core.Action {
	a := core.Action{Kind: core.ActNetSend, Peer: to}
	var body [core.MaxInlineMsg]byte
	body[0] = hops
	binary.LittleEndian.PutUint64(body[1:], v)
	_ = a.Msg.SetInline(body[:9+g.pad])
	g.sent++
	return append(out, a)
}

func (g *gossip) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	switch ev.Kind {
	case core.EvTimer:
		if g.rounds == 0 {
			return out
		}
		g.rounds--
		if g.burst > 0 {
			to := g.pick()
			for range g.burst {
				out = g.send(out, to, byte(1+g.rand.Below(4)), g.rand.Uint64())
			}
		} else {
			for range 1 + g.rand.Below(6) {
				out = g.send(out, g.pick(), byte(1+g.rand.Below(4)), g.rand.Uint64())
			}
		}
		return append(out, core.Action{Kind: core.ActSetTimer, Timer: 1, At: ctx.Now + 50 + core.Tick(g.rand.Below(500))})
	case core.EvNetRecv:
		g.got++
		b := ev.Msg.Inline()
		v := binary.LittleEndian.Uint64(b[1:])
		g.sum = g.sum*31 + v
		if b[0] == 0 {
			return out
		}
		if g.rand.Chance(1, 4) {
			g.post = core.Event{Kind: core.EvNetRecv, Peer: g.ids[g.self]}
			_ = g.post.Msg.SetInline(b)
			g.post.Msg.Body[0]--
			to := g.loops[g.rand.Below(uint64(len(g.loops)))]
			if to.Inbox().Post(&g.post) {
				g.sent++
			}
			return out
		}
		return g.send(out, g.pick(), b[0]-1, v^uint64(g.self))
	}
	panic(fmt.Sprintf("gossip: %+v", *ev))
}

type run struct {
	traces [][]byte
	sent   int
	got    int
	sums   []uint64
	wakes  uint64
}

type cluster struct {
	s        *sim.Scheduler
	loops    []*loop.Loop
	wires    []*Wire
	machines []*gossip
	rings    []*trace.Ring
	kernels  []*Kernel
}

// shape is how hard a run pushes its loops: pad bytes added to every message, a burst of messages
// to one loop on each timer instead of a few to random loops, and a socket buffer of sndbuf bytes,
// 0 for the kernel's default.
type shape struct {
	pad    int
	burst  int
	sndbuf int
}

// backpressure fills a 512-byte read budget with two frames, and a small socket buffer with one burst.
var backpressure = shape{pad: 240, burst: 24, sndbuf: 2048}

// runLoops steps n loops, two to a node, over a socketpair per pair of loops, and one from each
// loop to itself, until nothing is left to do.
func runLoops(t *testing.T, seed uint64, n int, plant int, sh shape) (run, error) {
	t.Helper()
	c := build(t, seed, n, plant, sh)
	for i := range n {
		c.loops[i].Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: 1})
	}
	for c.s.Step() {
	}
	return c.result(t)
}

func build(t *testing.T, seed uint64, n int, plant int, sh shape) *cluster {
	t.Helper()
	s := sim.New(seed, sim.Settings{PoolBytes: 1 << 20, ClockNum: 1, ClockDen: 20})
	ids := make([]core.NodeID, n)
	for i := range ids {
		ids[i] = core.NodeID{Node: uint32(i / 2), Loop: uint32(i % 2)}
	}
	var nodes []*sim.Node
	for i := 0; i < n; i += 2 {
		nodes = append(nodes, s.AddNode(sim.DefaultClock, nil, func(*sim.Node) {}))
	}
	loops := make([]*loop.Loop, n)
	wires := make([]*Wire, n)
	machines := make([]*gossip, n)
	rings := make([]*trace.Ring, n)
	kernels := make([]*Kernel, n)
	for i := range n {
		node := nodes[i/2]
		ring, err := trace.NewRing(make([]byte, 1<<15*trace.RecordSize))
		if err != nil {
			t.Fatal(err)
		}
		rings[i] = ring
		machines[i] = &gossip{self: i, ids: ids, loops: loops, rand: node.Fork(), rounds: 10, pad: sh.pad, burst: sh.burst}
		wires[i] = &Wire{}
		kernels[i] = NewKernel(s, node.ID())
		cfg := loop.Config{Node: node.ID(), ID: uint16(i % 2), Pool: sim.NewPool(1<<20, 0), ReadBudget: budget, Trace: ring, PlantBlock: i == plant}
		loops[i] = loop.New(cfg, kernels[i], machines[i], wires[i])
		wires[i].L = loops[i]
		Add(node, kernels[i], loops[i])
	}
	var fds []int
	t.Cleanup(func() {
		for _, fd := range fds {
			_ = unix.Close(fd)
		}
	})
	for i := range n {
		for j := i; j < n; j++ {
			p, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
			if err != nil {
				t.Fatal(err)
			}
			fds = append(fds, p[0], p[1])
			for k, end := range []struct{ at, to int }{{i, j}, {j, i}} {
				if err := unix.SetNonblock(p[k], true); err != nil {
					t.Fatal(err)
				}
				if sh.sndbuf > 0 {
					// Linux bounds a unix stream by the sender's send buffer, macOS by the receiver's receive buffer.
					for _, opt := range []int{unix.SO_SNDBUF, unix.SO_RCVBUF} {
						if err := unix.SetsockoptInt(p[k], unix.SOL_SOCKET, opt, sh.sndbuf); err != nil {
							t.Fatal(err)
						}
					}
				}
				c, err := loops[end.at].AddConn(p[k], true)
				if err != nil {
					t.Fatal(err)
				}
				wires[end.at].Connect(c, ids[end.to])
			}
		}
	}
	return &cluster{s: s, loops: loops, wires: wires, machines: machines, rings: rings, kernels: kernels}
}

func (c *cluster) result(t *testing.T) (run, error) {
	t.Helper()
	n := len(c.loops)
	loops, wires, machines, rings, kernels := c.loops, c.wires, c.machines, c.rings, c.kernels
	var r run
	for i := range n {
		if err := kernels[i].Err(); err != nil {
			return r, err
		}
	}
	for i := range n {
		if rings[i].Added() > uint64(rings[i].Len()) {
			t.Fatalf("loop %d overflowed its trace ring", i)
		}
		if p := loops[i].Pending(); p.Work {
			t.Fatalf("loop %d left work pending: %+v", i, p)
		}
		if wires[i].Lost > 0 {
			t.Fatalf("loop %d lost %d sends", i, wires[i].Lost)
		}
		var recs []trace.Record
		for k := range rings[i].Len() {
			recs = append(recs, rings[i].At(k))
		}
		if err := trace.CheckRecords(recs, trace.OneEpollWaitPerPass(), trace.OneWritevPerFD(), trace.ReadBudget(budget)); err != nil {
			t.Fatalf("loop %d: %v", i, err)
		}
		r.traces = append(r.traces, rings[i].Dump(nil))
		r.sent += machines[i].sent
		r.got += machines[i].got
		r.sums = append(r.sums, machines[i].sum)
		r.wakes += loops[i].Inbox().Wakes()
	}
	return r, nil
}

func TestSteppedLoopsReplayFromTheirSeed(t *testing.T) {
	for _, c := range []struct {
		name string
		n    int
		sh   shape
	}{{"loops=1", 1, shape{}}, {"loops=8", 8, shape{}}, {"loops=8,backpressure", 8, backpressure}} {
		n := c.n
		t.Run(c.name, func(t *testing.T) {
			seeds.Run(t, 1, 50, func(seed uint64) {
				a, err := runLoops(t, seed, n, -1, c.sh)
				if err != nil {
					t.Fatal(err)
				}
				b, err := runLoops(t, seed, n, -1, c.sh)
				if err != nil {
					t.Fatal(err)
				}
				if a.got != a.sent || a.got == 0 {
					t.Fatalf("seed %d: %d messages sent, %d received", seed, a.sent, a.got)
				}
				for i := range a.traces {
					if err := sim.CompareTraces(a.traces[i], b.traces[i]); err != nil {
						t.Fatalf("seed %d, loop %d: %v", seed, i, err)
					}
				}
				if fmt.Sprint(a.sums) != fmt.Sprint(b.sums) {
					t.Fatalf("seed %d: results %v then %v", seed, a.sums, b.sums)
				}
			})
		})
	}
}

func TestSteppedRunFailsALoopThatBlocksWithWorkPending(t *testing.T) {
	_, err := runLoops(t, 1, 8, 3, shape{})
	if err == nil {
		t.Fatal("a loop that blocks with work pending passed")
	}
	t.Log(err)
}

func TestSteppedTraceNamesConnectionsNotDescriptors(t *testing.T) {
	a, err := runLoops(t, 7, 8, -1, shape{})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(a.traces[0], []byte(" read fd=0 ")) || !bytes.Contains(a.traces[0], []byte(" writev fd=")) {
		t.Fatalf("trace of loop 0 lacks reads or writes by connection ID:\n%.2000s", a.traces[0])
	}
}

func TestASteppedPassAllocatesNothing(t *testing.T) {
	c := build(t, 3, 2, -1, shape{})
	var ev core.Event
	pass := func() {
		ev = core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{Loop: 1}}
		_ = ev.Msg.SetInline([]byte{3, 1, 2, 3, 4, 5, 6, 7, 8})
		c.loops[0].Inbox().Post(&ev)
		for range 4 {
			c.loops[0].Pass()
			c.loops[1].Pass()
		}
	}
	pass()
	if n := testing.AllocsPerRun(200, pass); n != 0 {
		t.Fatalf("%v allocations per round of passes", n)
	}
	if c.machines[0].got+c.machines[1].got < 200 {
		t.Fatalf("only %d messages handled", c.machines[0].got+c.machines[1].got)
	}
}

// clientAndPeer is one stepped loop running m, gossip when nil, through the driver d makes of
// its Wire, the Wire itself when nil, with a client connection, added first, and a peer one. It
// returns the loop, its trace and the test's ends of the client and the peer connection, which
// the test may close and set to -1.
func clientAndPeer(t *testing.T, queueCap int, m loop.Machine, d func(*Wire) loop.Driver) (*loop.Loop, *trace.Ring, []int) {
	t.Helper()
	if m == nil {
		m = &gossip{}
	}
	s := sim.New(1, sim.Settings{PoolBytes: 1 << 20, ClockNum: 1, ClockDen: 20})
	node := s.AddNode(sim.DefaultClock, nil, func(*sim.Node) {})
	ring, err := trace.NewRing(make([]byte, 1<<10*trace.RecordSize))
	if err != nil {
		t.Fatal(err)
	}
	k := NewKernel(s, node.ID())
	w := &Wire{}
	var drv loop.Driver = w
	if d != nil {
		drv = d(w)
	}
	l := loop.New(loop.Config{Node: node.ID(), Pool: sim.NewPool(1<<20, 0), ReadBudget: budget, QueueCap: queueCap, Trace: ring}, k, m, drv)
	w.L = l
	k.Attach(l)
	ends := []int{-1, -1}
	t.Cleanup(func() {
		for i, fd := range ends {
			l.Close(loop.ConnID(i))
			if fd >= 0 {
				_ = unix.Close(fd)
			}
		}
	})
	for i, peer := range []bool{false, true} {
		p, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
		if err != nil {
			t.Fatal(err)
		}
		ends[i] = p[1]
		for _, fd := range p {
			if err := unix.SetNonblock(fd, true); err != nil {
				t.Fatal(err)
			}
		}
		c, err := l.AddConn(p[0], peer)
		if err != nil {
			t.Fatal(err)
		}
		w.Connect(c, core.NodeID{Node: uint32(9 + i)})
	}
	return l, ring, ends
}

// sendLast writes a frame of a message with no hops left to fd.
func sendLast(t *testing.T, fd int) {
	t.Helper()
	if _, err := unix.Write(fd, []byte{9, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8}); err != nil {
		t.Fatal(err)
	}
}

// readsIn is the connections read in pass, in order.
func readsIn(r *trace.Ring, pass uint64) []uint64 {
	var conns []uint64
	for i := range r.Len() {
		if rec := r.At(i); rec.Name == trace.SysRead && rec.Pass == pass {
			conns = append(conns, rec.Fields[0])
		}
	}
	return conns
}

func TestSteppedPassReadsPeersBeforeClients(t *testing.T) {
	l, ring, ends := clientAndPeer(t, 0, nil, nil)
	sendLast(t, ends[0])
	sendLast(t, ends[1])
	l.Pass()
	if got := readsIn(ring, 1); fmt.Sprint(got) != "[1 0]" {
		t.Fatalf("pass 1 read connections %v; want the peer, 1, before the client, 0\n%s", got, ring.Dump(nil))
	}
}

func TestSteppedLoopStopsReadingClientsAtTheQueueCap(t *testing.T) {
	const queueCap = 64
	l, ring, ends := clientAndPeer(t, queueCap, nil, nil)
	pool := l.Context().Pool
	b, err := pool.Alloc(queueCap)
	if err != nil {
		t.Fatal(err)
	}
	if !l.Queue(1, b, queueCap, true) {
		t.Fatal("queue refused")
	}
	sendLast(t, ends[0])
	sendLast(t, ends[1])
	l.Pass()
	if got := readsIn(ring, 1); fmt.Sprint(got) != "[1]" {
		t.Fatalf("with %d bytes queued, pass 1 read connections %v; want the peer, 1, alone\n%s", queueCap, got, ring.Dump(nil))
	}
	if l.Queued() != 0 {
		t.Fatalf("%d bytes still queued after pass 1", l.Queued())
	}
	l.Pass()
	if got := readsIn(ring, 2); fmt.Sprint(got) != "[0]" {
		t.Fatalf("below the cap, pass 2 read connections %v; want the client, 0\n%s", got, ring.Dump(nil))
	}
}

// sendOnClose is Wire, except that a closed connection makes it send a frame on another, as a
// TCP driver that reports PeerLost to a machine that then sends elsewhere does.
type sendOnClose struct {
	*Wire
	to loop.ConnID
}

func (d *sendOnClose) Closed(c loop.ConnID) {
	d.Wire.Closed(c)
	pool := d.L.Context().Pool
	b, err := pool.Alloc(11)
	if err != nil {
		panic(err)
	}
	copy(pool.Bytes(b), []byte{9, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8})
	if !d.L.Queue(d.to, b, 11, true) {
		panic("sendOnClose: queue refused")
	}
}

func TestSteppedFrameQueuedWhileFlushingIsWritten(t *testing.T) {
	l, ring, ends := clientAndPeer(t, 1, nil, func(w *Wire) loop.Driver { return &sendOnClose{Wire: w, to: 1} })
	pool := l.Context().Pool
	b, err := pool.Alloc(64)
	if err != nil {
		t.Fatal(err)
	}
	if !l.Queue(0, b, 64, true) {
		t.Fatal("queue refused")
	}
	_ = unix.Close(ends[0])
	ends[0] = -1
	for range 3 {
		l.Pass()
	}
	buf := make([]byte, 64)
	if n, err := unix.Read(ends[1], buf); n != 11 {
		t.Fatalf("the peer read %d bytes (%v) of the frame queued when the client's write failed; %d bytes still queued\n%s", n, err, l.Queued(), ring.Dump(nil))
	}
	if l.Queued() != 0 {
		t.Fatalf("%d bytes still queued", l.Queued())
	}
}

// rearm arms timer 1 again at ctx.Now each time it fires, up to n times.
type rearm struct{ n int }

func (m *rearm) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	if ev.Kind == core.EvTimer && m.n < 100 {
		m.n++
		return append(out, core.Action{Kind: core.ActSetTimer, Timer: 1, At: ctx.Now})
	}
	return out
}

func TestSteppedTimerArmedAgainAtNowFiresInTheNextPass(t *testing.T) {
	m := &rearm{}
	l, _, _ := clientAndPeer(t, 0, m, nil)
	l.Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: 1})
	for pass := 1; pass <= 3; pass++ {
		l.Pass()
		if m.n != pass {
			t.Fatalf("after pass %d the timer fired %d times; want once per pass", pass, m.n)
		}
	}
}

func TestQueueRefusesAnEmptyOrOversizedFrame(t *testing.T) {
	l, _, _ := clientAndPeer(t, 0, nil, nil)
	pool := l.Context().Pool
	b, err := pool.Alloc(8)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []int{-1, 0, len(pool.Bytes(b)) + 1} {
		if l.Queue(1, b, n, false) {
			t.Fatalf("Queue of %d bytes from a %d-byte buffer accepted", n, len(pool.Bytes(b)))
		}
	}
	l.Pass()
}

func TestAddConnRefusesABlockingDescriptor(t *testing.T) {
	l, _, _ := clientAndPeer(t, 0, nil, nil)
	p, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(p[0])
	defer unix.Close(p[1])
	if _, err := l.AddConn(p[0], false); err == nil {
		t.Fatal("a blocking descriptor was taken")
	}
}
