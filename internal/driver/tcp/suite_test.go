package tcp

import (
	"net/netip"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/loop"
	"github.com/nvcnvn/rock-solid/internal/loop/looptest"
	"github.com/nvcnvn/rock-solid/internal/loop/nettest"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// callTimer is the timer whose event runs a suite Call in the loop's next pass.
const callTimer = ^core.TimerID(0)

const (
	// suiteRounds bounds a Run; rest is quietRounds rounds and quietFor with no event and no
	// loop's work pending, enough for a dial and a frame to cross loopback.
	suiteRounds = 2_000_000
	quietRounds = 100
	quietFor    = 20 * time.Millisecond
	// readBudget is a quarter of the default, so a pass leaves more of a connection unread than
	// loopback's socket buffers hold, as the restart case needs to see a connection replaced.
	readBudget = 64 << 10
)

// suiteNet is the suite's Net on this driver over loopback TCP: stepped loops, each with its own
// listener, sharing one simulated clock.
type suiteNet struct {
	t      testing.TB
	s      *sim.Scheduler
	nodes  []*suiteNode
	events int
}

type suiteNode struct {
	n      *suiteNet
	i      int
	id     core.NodeID
	k      *looptest.Kernel
	l      *loop.Loop
	d      *Driver
	pool   *loop.Pool
	m      loop.Machine
	call   func(ctx *core.Context, out []core.Action) []core.Action
	port   int
	starts uint64
	up     bool
	held   bool
}

func newSuiteNet(t *testing.T) nettest.Net {
	n := &suiteNet{t: t, s: sim.New(1, sim.Settings{PoolBytes: 1 << 20})}
	t.Cleanup(func() {
		for i, nd := range n.nodes {
			if nd.up {
				n.Stop(i)
			}
		}
	})
	return n
}

func (nd *suiteNode) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	if ev.Kind == core.EvTimer && ev.Timer == callTimer {
		f := nd.call
		nd.call = nil
		return f(ctx, out)
	}
	nd.n.events++
	return nd.m.OnEvent(ctx, ev, out)
}

func (n *suiteNet) Start(i int, m loop.Machine) core.NodeID {
	if i == len(n.nodes) {
		node := n.s.AddNode(sim.DefaultClock, nil, func(*sim.Node) {}).ID()
		n.nodes = append(n.nodes, &suiteNode{n: n, i: i, id: core.NodeID{Node: uint32(i + 1), Loop: uint32(i % 2)}, k: looptest.NewKernel(n.s, node)})
	}
	nd := n.nodes[i]
	nd.starts++
	nd.m, nd.up, nd.held = m, true, false
	var err error
	if nd.pool, err = loop.NewPool(64<<20, 0); err != nil {
		n.t.Fatal(err)
	}
	nd.d = New(Config{Self: nd.id, Seed: uint64(i)<<32 | nd.starts, Dial: n.dial})
	nd.l = loop.New(loop.Config{Node: nd.id.Node, ID: uint16(nd.id.Loop), Pool: nd.pool, ReadBudget: readBudget}, nd.k, nd, nd.d)
	nd.d.L = nd.l
	nd.k.Attach(nd.l)
	lfd, port, err := ListenTCP(netip.MustParseAddrPort("127.0.0.1:0"))
	if err != nil {
		n.t.Fatal(err)
	}
	nd.port = port
	if err := nd.d.Listen(lfd); err != nil {
		n.t.Fatal(err)
	}
	return nd.id
}

// dial reaches a peer's listener, or is refused for a peer that is down or was never up.
func (n *suiteNet) dial(p core.NodeID) (int, error) {
	for _, nd := range n.nodes {
		if nd.id == p && nd.up {
			return DialTCP(netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(nd.port)))
		}
	}
	return -1, unix.ECONNREFUSED
}

// Stop closes the node's listener and connections, losing what its loop had not yet written, as
// a process's exit does.
func (n *suiteNet) Stop(i int) {
	nd := n.nodes[i]
	for _, cs := range nd.d.conns {
		nd.l.Close(cs.id)
	}
	nd.up = false
	if err := nd.pool.Close(); err != nil {
		n.t.Fatal(err)
	}
}

func (n *suiteNet) Pool(i int) nettest.Pool { return n.nodes[i].pool }

func (n *suiteNet) Call(i int, f func(ctx *core.Context, out []core.Action) []core.Action) {
	nd := n.nodes[i]
	nd.call = f
	if !nd.l.Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: callTimer}) {
		n.t.Fatal("inbox full")
	}
	nd.l.Pass()
	if nd.call != nil {
		n.t.Fatal("a pass did not run the call")
	}
}

func (n *suiteNet) Hold(i int) { n.nodes[i].held = true }

// Run passes every running loop not held, round robin; at rest it lets the held loops read
// again, and the network rests once none is held.
func (n *suiteNet) Run(done func() bool) bool {
	defer n.kernelErr()
	quiet, since := 0, time.Now()
	for range suiteRounds {
		if done != nil && done() {
			return true
		}
		before, work := n.events, false
		for _, nd := range n.nodes {
			if nd.up && !nd.held {
				nd.l.Pass()
				work = work || nd.l.Pending().Work
			}
		}
		if n.events != before || work {
			quiet, since = 0, time.Now()
			continue
		}
		if quiet++; quiet < quietRounds || time.Since(since) < quietFor {
			continue
		}
		released := false
		for _, nd := range n.nodes {
			released = released || nd.held
			nd.held = false
		}
		if !released {
			return done == nil || done()
		}
		quiet, since = 0, time.Now()
	}
	return false
}

func (n *suiteNet) kernelErr() {
	for _, nd := range n.nodes {
		if err := nd.k.Err(); err != nil {
			n.t.Fatal(err)
		}
	}
}

func (n *suiteNet) Sleep(d core.Tick) {
	n.Run(nil)
	n.s.Advance(n.s.Time() + d)
}

func (n *suiteNet) Backoff() core.Tick { return maxBackoff }

func TestTheNetworkSuiteOnLoopbackTCP(t *testing.T) {
	nettest.Run(t, newSuiteNet)
	nettest.SkipSimCases(t)
}
