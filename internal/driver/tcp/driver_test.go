package tcp

import (
	"bytes"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/loop"
	"github.com/nvcnvn/rock-solid/internal/loop/looptest"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// goTimer is the timer whose event makes a recorder emit the sends the test queued.
const goTimer core.TimerID = 1

// recorder is a test machine: it keeps every network event and, on goTimer, emits sends.
type recorder struct {
	sends []core.Action
	recvs []core.Event
	lost  []core.NodeID
	// keep, when set, keeps received data buffers instead of freeing them.
	keep bool
	// discard, when set, frees every message's data and only counts it.
	discard bool
	got     int
	data    [][]byte
	// hook, when set, sees every event first: when it returns true its actions stand in for the recorder's.
	hook func(ctx *core.Context, ev *core.Event, out []core.Action) ([]core.Action, bool)
}

func (r *recorder) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	if r.hook != nil {
		if o, ok := r.hook(ctx, ev, out); ok {
			return o
		}
	}
	switch ev.Kind {
	case core.EvTimer:
		if ev.Timer == goTimer {
			out = append(out, r.sends...)
			r.sends = r.sends[:0]
		}
	case core.EvNetRecv:
		if r.discard {
			r.got++
			if ev.Msg.Buf != core.NoBuf {
				ctx.Pool.Free(ev.Msg.Buf)
			}
			break
		}
		r.recvs = append(r.recvs, *ev)
		if ev.Msg.Buf != core.NoBuf {
			r.data = append(r.data, bytes.Clone(ctx.Pool.Bytes(ev.Msg.Buf)))
			if !r.keep {
				ctx.Pool.Free(ev.Msg.Buf)
			}
		}
	case core.EvPeerLost:
		r.lost = append(r.lost, ev.Peer)
	}
	return out
}

// node is one stepped loop with its driver, machine and listener path.
type node struct {
	id    core.NodeID
	l     *loop.Loop
	d     *Driver
	m     *recorder
	pool  *loop.Pool
	path  string
	port  int
	dials int
}

// rig is a set of stepped loops on one simulated node clock, each listening on a unix socket or,
// for tcp, on loopback TCP.
type rig struct {
	t     testing.TB
	s     *sim.Scheduler
	nodes []*node
	tcp   bool
	// refuse makes every dial fail.
	refuse bool
}

func newRig(t *testing.T, n int, poolBytes int) *rig { return newRigOn(t, n, poolBytes, false) }

func newRigOn(t testing.TB, n int, poolBytes int, tcp bool) *rig {
	t.Helper()
	s := sim.New(1, sim.Settings{PoolBytes: 1 << 20})
	r := &rig{t: t, s: s, tcp: tcp}
	// t.TempDir's path can pass the 104 bytes a unix socket's path holds on macOS.
	dir, err := os.MkdirTemp("", "tcp")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	for i := range n {
		k := looptest.NewKernel(s, s.AddNode(sim.DefaultClock, nil, func(*sim.Node) {}).ID())
		pool, err := loop.NewPool(poolBytes, 0)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = pool.Close() })
		nd := &node{id: core.NodeID{Node: uint32(i + 1), Loop: uint32(i % 2)}, m: &recorder{}, pool: pool}
		nd.path = filepath.Join(dir, fmt.Sprintf("l%d", i))
		nd.d = New(Config{Self: nd.id, Seed: uint64(i + 1), Dial: func(p core.NodeID) (int, error) {
			nd.dials++
			if r.refuse {
				return -1, unix.ECONNREFUSED
			}
			if r.tcp {
				port := r.byID(p).port
				if port == 0 {
					return -1, unix.ECONNREFUSED
				}
				return DialTCP(netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(port)))
			}
			return dialUnix(r.byID(p).path)
		}})
		nd.l = loop.New(loop.Config{Node: nd.id.Node, ID: uint16(nd.id.Loop), Pool: nd.pool}, k, nd.m, nd.d)
		nd.d.L = nd.l
		k.Attach(nd.l)
		var lfd int
		if tcp {
			lfd, nd.port, err = ListenTCP(netip.MustParseAddrPort("127.0.0.1:0"))
		} else {
			lfd, err = listenUnix(nd.path)
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := nd.d.Listen(lfd); err != nil {
			t.Fatal(err)
		}
		r.nodes = append(r.nodes, nd)
	}
	t.Cleanup(func() {
		for _, nd := range r.nodes {
			for _, cs := range nd.d.conns {
				nd.l.Close(cs.id)
			}
		}
	})
	return r
}

func (r *rig) byID(id core.NodeID) *node {
	for _, nd := range r.nodes {
		if nd.id == id {
			return nd
		}
	}
	return &node{path: "/nonexistent/tcp-test"}
}

func dialUnix(path string) (int, error) {
	fd, err := unix.Socket(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		return -1, err
	}
	if err := unix.SetNonblock(fd, true); err != nil {
		_ = unix.Close(fd)
		return -1, err
	}
	if err := unix.Connect(fd, &unix.SockaddrUnix{Name: path}); err != nil && err != unix.EINPROGRESS {
		_ = unix.Close(fd)
		return -1, err
	}
	return fd, nil
}

func listenUnix(path string) (int, error) {
	fd, err := unix.Socket(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		return -1, err
	}
	if err := unix.Bind(fd, &unix.SockaddrUnix{Name: path}); err != nil {
		return -1, err
	}
	if err := unix.Listen(fd, 64); err != nil {
		return -1, err
	}
	return fd, unix.SetNonblock(fd, true)
}

// send queues a NetSend from nd to peer carrying inline and, when data is set, a pool buffer of it.
func (nd *node) send(peer core.NodeID, inline string, data []byte, reply bool) {
	a := core.Action{Kind: core.ActNetSend, Peer: peer, Reply: reply}
	if err := a.Msg.SetInline([]byte(inline)); err != nil {
		panic(err)
	}
	if data != nil {
		b, err := nd.pool.Alloc(len(data))
		if err != nil {
			panic(err)
		}
		copy(nd.pool.Bytes(b), data)
		a.Msg.Buf = b
	}
	nd.m.sends = append(nd.m.sends, a)
}

// fire makes nd's machine emit its queued sends in its next pass.
func (nd *node) fire() {
	if !nd.l.Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: goTimer}) {
		panic("inbox full")
	}
}

// step passes every loop n times, round robin.
func (r *rig) step(n int) {
	for range n {
		for _, nd := range r.nodes {
			nd.l.Pass()
		}
	}
}

// links is the number of open connections nd's driver holds, listeners aside.
func (nd *node) links() int {
	n := 0
	for _, c := range nd.d.conns {
		if c.open && !c.listen {
			n++
		}
	}
	return n
}

func TestMessagesCrossOneConnectionBothWays(t *testing.T) {
	r := newRig(t, 2, 4<<20)
	a, b := r.nodes[0], r.nodes[1]
	block := bytes.Repeat([]byte{7}, core.BlockSize)
	for i := range 100 {
		var data []byte
		if i%3 == 0 {
			data = block
		}
		a.send(b.id, fmt.Sprintf("req %d", i), data, true)
	}
	a.fire()
	r.step(100)
	if len(b.m.recvs) != 100 {
		t.Fatalf("b got %d messages, want 100", len(b.m.recvs))
	}
	for i, ev := range b.m.recvs {
		if ev.Peer != a.id || string(ev.Msg.Inline()) != fmt.Sprintf("req %d", i) || (i%3 == 0) != (ev.Msg.Buf != core.NoBuf) {
			t.Fatalf("message %d: %+v %q", i, ev.Peer, ev.Msg.Inline())
		}
	}
	for _, d := range b.m.data {
		if !bytes.Equal(d, block) {
			t.Fatal("data differs")
		}
	}
	b.send(a.id, "reply", nil, true)
	b.fire()
	r.step(20)
	if len(a.m.recvs) != 1 || a.m.recvs[0].Peer != b.id {
		t.Fatalf("a got %+v", a.m.recvs)
	}
	if a.links() != 1 || b.links() != 1 || a.dials != 1 || b.dials != 0 {
		t.Fatalf("links %d and %d, dials %d and %d; want one connection, dialed by a", a.links(), b.links(), a.dials, b.dials)
	}
	if a.pool.Live() != 0 || b.pool.Live() != 0 || len(a.m.lost)+len(b.m.lost) != 0 {
		t.Fatalf("live buffers %d and %d, lost %v %v", a.pool.Live(), b.pool.Live(), a.m.lost, b.m.lost)
	}
}

func TestLoopsDialingEachOtherAtOnceKeepOneConnection(t *testing.T) {
	r := newRig(t, 2, 1<<20)
	a, b := r.nodes[0], r.nodes[1]
	a.send(b.id, "a1", nil, true)
	b.send(a.id, "b1", nil, true)
	a.fire()
	b.fire()
	r.step(20)
	if a.links() != 1 || b.links() != 1 {
		t.Fatalf("links %d and %d, want 1 each", a.links(), b.links())
	}
	// b, the higher address, gave up its own dial: one PeerLost there for what it sent on it.
	if len(a.m.lost) != 0 || len(b.m.lost) != 1 {
		t.Fatalf("lost at a %v, at b %v", a.m.lost, b.m.lost)
	}
	a.send(b.id, "a2", nil, true)
	b.send(a.id, "b2", nil, true)
	a.fire()
	b.fire()
	r.step(20)
	if got := string(b.m.recvs[len(b.m.recvs)-1].Msg.Inline()); got != "a2" {
		t.Fatalf("b's last message %q", got)
	}
	if got := string(a.m.recvs[len(a.m.recvs)-1].Msg.Inline()); got != "b2" {
		t.Fatalf("a's last message %q", got)
	}
	if a.links() != 1 || b.links() != 1 || len(b.m.lost) != 1 {
		t.Fatalf("links %d and %d, lost at b %v", a.links(), b.links(), b.m.lost)
	}
}

func TestEverySendToAnUnreachableLoopEndsInPeerLost(t *testing.T) {
	r := newRig(t, 1, 1<<20)
	a := r.nodes[0]
	gone := core.NodeID{Node: 9, Loop: 3}
	for round := range 3 {
		a.send(gone, "x", []byte{1}, true)
		a.fire()
		r.step(2)
		if len(a.m.lost) != round+1 || a.m.lost[round] != gone {
			t.Fatalf("round %d: lost %v", round, a.m.lost)
		}
	}
	// The first dial failed at once and the next two sends came within its backoff.
	if a.dials != 1 || a.pool.Live() != 0 {
		t.Fatalf("dials %d, live buffers %d", a.dials, a.pool.Live())
	}
	r.s.Advance(r.s.Time() + maxBackoff)
	a.send(gone, "x", nil, true)
	a.fire()
	r.step(2)
	if a.dials != 2 || len(a.m.lost) != 4 {
		t.Fatalf("after the backoff: dials %d, lost %v", a.dials, a.m.lost)
	}
}

func TestADroppedConnectionIsOnePeerLostThenReconnects(t *testing.T) {
	r := newRig(t, 2, 1<<20)
	a, b := r.nodes[0], r.nodes[1]
	a.send(b.id, "one", nil, true)
	a.fire()
	r.step(20)
	// b's end goes away under it, as a peer that closes does.
	b.dropAll()
	r.step(20)
	if len(a.m.lost) != 1 || a.m.lost[0] != b.id || len(b.m.lost) != 1 {
		t.Fatalf("lost at a %v, at b %v; want one each", a.m.lost, b.m.lost)
	}
	a.send(b.id, "two", nil, true)
	a.fire()
	r.step(20)
	if got := b.m.recvs[len(b.m.recvs)-1]; string(got.Msg.Inline()) != "two" || a.dials != 2 || len(a.m.lost) != 1 {
		t.Fatalf("after reconnecting: %q, dials %d, lost %v", got.Msg.Inline(), a.dials, a.m.lost)
	}
}

func TestAReplyBufferSentToThreePeersIsFreedOnceWhenOneIsLost(t *testing.T) {
	r := newRig(t, 3, 1<<20)
	a := r.nodes[0]
	gone := core.NodeID{Node: 9}
	a.send(r.nodes[1].id, "fan", []byte("data"), true)
	for _, p := range []core.NodeID{r.nodes[2].id, gone} {
		s := a.m.sends[0]
		s.Peer = p
		a.m.sends = append(a.m.sends, s)
	}
	a.fire()
	r.step(20)
	if a.pool.Live() != 0 || len(a.m.lost) != 1 || len(r.nodes[1].m.recvs) != 1 || len(r.nodes[2].m.recvs) != 1 {
		t.Fatalf("live %d, lost %v, received %d and %d", a.pool.Live(), a.m.lost, len(r.nodes[1].m.recvs), len(r.nodes[2].m.recvs))
	}
}
