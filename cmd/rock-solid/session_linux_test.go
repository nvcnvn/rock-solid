package main

import (
	"fmt"
	"net/netip"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/driver/tcp"
	"github.com/nvcnvn/rock-solid/internal/loop"
	"github.com/nvcnvn/rock-solid/internal/meta"
)

const clientTimer core.TimerID = 2

// client is a client's loop as far as the review needs: it starts sessions at the control plane
// through the session-start kind and writes one block to a chunk's primary, keeping the answers.
type client struct {
	t       *testing.T
	l       *loop.Loop
	ctl     core.NodeID
	pending []func(ctx *core.Context) core.Action
	started map[uint64]uint64
	acks    map[core.RequestID]chunkstore.Response
	data    map[core.RequestID]byte
	seq     uint64
}

func (c *client) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	switch ev.Kind {
	case core.EvTimer:
		for _, f := range c.pending {
			out = append(out, f(ctx))
		}
		c.pending = c.pending[:0]
		out = append(out, core.Action{Kind: core.ActSetTimer, Timer: clientTimer, At: ctx.Now + 100_000})
	case core.EvNetRecv:
		defer func() {
			if ev.Msg.Buf != core.NoBuf {
				ctx.Pool.Free(ev.Msg.Buf)
			}
		}()
		if ev.Msg.Group() {
			group, kind, token, ok := meta.Peek(&ev.Msg)
			if ok && group == ctlGroup && kind == meta.KindStarted {
				if id, err := meta.DecodeStarted(ctx.Pool, &ev.Msg); err == nil {
					c.started[token] = id
				}
			}
			return out
		}
		frame, data := chunkstore.Frame(ctx.Pool, &ev.Msg)
		var rp chunkstore.Response
		if chunkstore.DecodeResponse(frame, data, &rp) == nil {
			c.acks[rp.ID] = rp
			if data != 0 {
				c.data[rp.ID] = ctx.Pool.Bytes(ev.Msg.Buf)[0]
			}
		}
	}
	return out
}

func newClient(t *testing.T, ctlPort, nodeBase int) *client {
	k, err := loop.NewEpoll("/nonexistent/vmclock0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(k.Close)
	pool, err := loop.NewPool(16<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	c := &client{t: t, ctl: ctlNode, started: map[uint64]uint64{}, acks: map[core.RequestID]chunkstore.Response{}, data: map[core.RequestID]byte{}}
	lo := netip.MustParseAddr("127.0.0.1")
	td := tcp.New(tcp.Config{Self: core.NodeID{Node: 3}, Seed: 3, Dial: func(p core.NodeID) (int, error) {
		if p == ctlNode {
			return tcp.DialTCP(netip.AddrPortFrom(lo, uint16(ctlPort)))
		}
		return tcp.DialTCP(netip.AddrPortFrom(lo, uint16(nodeBase+int(p.Loop))))
	}})
	c.l = loop.New(loop.Config{Node: 3, Pool: pool}, k, c, td)
	td.L = c.l
	return c
}

// send queues f's message for the loop's next pass.
func (c *client) send(f func(ctx *core.Context) core.Action) {
	c.pending = append(c.pending, f)
	c.l.Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: clientTimer})
}

// start starts a session through the session-start kind, asking again every second, and returns
// its ID.
func (c *client) start(within time.Duration) (uint64, bool) {
	c.seq++
	req := c.seq
	next := time.Now()
	for end := time.Now().Add(within); time.Now().Before(end); c.l.Pass() {
		if id, ok := c.started[req]; ok {
			return id, true
		}
		if time.Now().After(next) {
			next = time.Now().Add(time.Second)
			c.send(func(ctx *core.Context) core.Action {
				a := core.Action{Kind: core.ActNetSend, Peer: c.ctl}
				if err := meta.EncodeSessionStart(ctx.Pool, ctlGroup, req, &a.Msg); err != nil {
					c.t.Error(err)
				}
				return a
			})
		}
	}
	return 0, false
}

// write sends one block of chunk ch, in u's PG, as session to the loop at, and waits for its
// answer.
func (c *client) write(u chunkstore.Update, at core.NodeID, within time.Duration) (chunkstore.Response, bool) {
	c.seq++
	u.Kind, u.ID, u.Incarnation, u.Off, u.Len = chunkstore.KindWrite, core.RequestID(c.seq), 1, 0, core.BlockSize
	c.send(func(ctx *core.Context) core.Action {
		a := core.Action{Kind: core.ActNetSend, Peer: at}
		a.Msg.Len = uint16(copy(a.Msg.Body[:], chunkstore.AppendUpdate(nil, &u)))
		buf, err := ctx.Pool.AllocZeroed(core.BlockSize)
		if err != nil {
			c.t.Error(err)
		}
		ctx.Pool.Bytes(buf)[0] = byte(u.ID)
		a.Msg.Buf = buf
		return a
	})
	for end := time.Now().Add(within); time.Now().Before(end); c.l.Pass() {
		if rp, ok := c.acks[u.ID]; ok {
			return rp, true
		}
	}
	return chunkstore.Response{}, false
}

// read reads the first block of u's chunk from the loop at, and waits for its answer and the
// block's first byte.
func (c *client) read(u chunkstore.Update, at core.NodeID, within time.Duration) (chunkstore.Response, byte, bool) {
	c.seq++
	rd := chunkstore.Read{ID: core.RequestID(c.seq), PG: u.PG, Chunk: u.Chunk, Len: core.BlockSize}
	c.send(func(ctx *core.Context) core.Action {
		a := core.Action{Kind: core.ActNetSend, Peer: at}
		a.Msg.Len = uint16(copy(a.Msg.Body[:], chunkstore.AppendRead(nil, &rd)))
		return a
	})
	for end := time.Now().Add(within); time.Now().Before(end); c.l.Pass() {
		if rp, ok := c.acks[rd.ID]; ok {
			return rp, c.data[rd.ID], true
		}
	}
	return chunkstore.Response{}, 0, false
}

// pgPrimary is PG 0 of pool 1 as the committed table has it, once its interval is activated with
// a member up: the write's PG fields and the loop of the member's disk, devices[i] at loop i+1.
func pgPrimary(c *meta.Control, devices []string) (chunkstore.Update, core.NodeID, bool) {
	t := c.Table()
	if len(t.Pools) == 0 || len(t.Pools[0].PGs) == 0 {
		return chunkstore.Update{}, core.NodeID{}, false
	}
	e := t.Pools[0].PGs[0]
	if e.Activated != e.Interval || len(e.Members) == 0 || e.Members[0].Mark != chunkstore.MarkUp {
		return chunkstore.Update{}, core.NodeID{}, false
	}
	pg := core.PGID{Pool: 1}
	u := chunkstore.Update{PG: chunkstore.PGFields{Epoch: t.Epoch, PG: pg, Interval: e.Interval}}
	for i := uint32(0); ; i++ {
		ch := core.ChunkID{Volume: 1, Inode: 7, Index: i}
		if core.PGIndexOf(ch.Hash(), uint32(len(t.Pools[0].PGs))) == 0 {
			u.Chunk = ch
			break
		}
	}
	for _, d := range c.Disks() {
		for k, path := range devices {
			if d.ID == e.Members[0].Disk && d.Device == deviceID(path) {
				return u, core.NodeID{Node: 2, Loop: uint32(k + 1)}, true
			}
		}
	}
	return chunkstore.Update{}, core.NodeID{}, false
}

// fenced reports whether the committed session table fences session id.
func fenced(c *meta.Control, id uint64) bool {
	st := c.SessionTable().Storage()
	return st.Fenced(id)
}

// writeOK writes as session to PG 0 of pool 1 until a write is acknowledged.
func writeOK(t *testing.T, p *ctl, cl *client, devices []string, session uint64, within time.Duration) (chunkstore.Response, bool) {
	return untilOK(t, p, devices, within, func(u chunkstore.Update, to core.NodeID) (chunkstore.Response, bool) {
		u.Session = session
		return cl.write(u, to, 5*time.Second)
	})
}

// untilOK runs op on PG 0 of pool 1 until it is answered OK, each time at the primary the
// committed table names.
func untilOK(t *testing.T, p *ctl, devices []string, within time.Duration, op func(u chunkstore.Update, to core.NodeID) (chunkstore.Response, bool)) (chunkstore.Response, bool) {
	var rp chunkstore.Response
	for end := time.Now().Add(within); time.Now().Before(end); {
		type at struct {
			u  chunkstore.Update
			to core.NodeID
			ok bool
		}
		w := onCtl(t, p, func(_ *core.Context, c *meta.Control) at {
			if c == nil {
				return at{}
			}
			u, to, ok := pgPrimary(c, devices)
			return at{u, to, ok}
		})
		if !w.ok {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		var ok bool
		if rp, ok = op(w.u, w.to); ok && rp.Status == chunkstore.StatusOK {
			return rp, true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return rp, false
}

// Over TCP, with the control plane in the test process and the storage node a child: a client
// starts a session through the session-start kind, and its write, from a session newer than the
// node's table, waits for the node's fetch and succeeds. The node is killed and started again at
// once over its opened stores, within node_down_after, so the control plane never marks its PG's
// member down: the block written before the kill reads back, and a new session's write to the same
// PG succeeds, once the PG moves to the one new interval the restarted replica asks for and the
// replica activates it. Once the first session's
// lease runs out unrenewed and the leader fences it, its write is refused.
func TestASessionWritesOverTCPAndARestartedNodeWritesToTheSamePGAgain(t *testing.T) {
	dir := testDir(t)
	p := runCtl(t, dir)
	devices := []string{filepath.Join(dir, "sess-dev-a"), filepath.Join(dir, "sess-dev-b")}
	for _, d := range devices {
		device(t, d, false)
	}
	base := freeBase(t, 3)
	flags := fmt.Sprintf("-node 2 -ctl 1=127.0.0.1:%d -group %d -listen 127.0.0.1:%d -device %s -device %s", p.Port, ctlGroup, base, devices[0], devices[1])
	var out output
	cmd := startNode(t, flags, &out)
	if !within(60*time.Second, func() bool {
		return onCtl(t, p, func(_ *core.Context, c *meta.Control) bool { return c != nil && len(upDisks(c, 2)) == 2 })
	}) {
		t.Fatalf("the node's disks never came up; the node said:\n%s", out.String())
	}
	replicas, pgs := 1, uint32(4)
	if err := onCtl(t, p, func(_ *core.Context, c *meta.Control) error {
		return c.SetPool(1, chunkstore.PoolSpec{Replicas: &replicas, PGCount: &pgs}, c.AppendDisks(nil))
	}); err != nil {
		t.Fatal(err)
	}
	if !within(30*time.Second, func() bool {
		return onCtl(t, p, func(_ *core.Context, c *meta.Control) bool {
			if c == nil {
				return false
			}
			_, _, ok := pgPrimary(c, devices)
			return ok
		})
	}) {
		t.Fatal("PG 0 never activated")
	}
	cl := newClient(t, p.Port, base)
	s1, ok := cl.start(10 * time.Second)
	if !ok {
		t.Fatal("no session started through the session-start kind")
	}
	written, ok := writeOK(t, p, cl, devices, s1, 8*time.Second)
	if !ok {
		t.Fatalf("session %d's write was never acknowledged, the last answer %+v; the node said:\n%s", s1, written, out.String())
	}
	before := onCtl(t, p, func(_ *core.Context, c *meta.Control) uint32 { return c.Table().Pools[0].PGs[0].Interval })

	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	var again output
	startNode(t, flags, &again)
	var got byte
	if rp, ok := untilOK(t, p, devices, 15*time.Second, func(u chunkstore.Update, to core.NodeID) (chunkstore.Response, bool) {
		rp, b, ok := cl.read(u, to, 5*time.Second)
		got = b
		return rp, ok
	}); !ok || got != byte(written.ID) {
		t.Fatalf("the block written before the kill read back %#x, want %#x, the last answer %+v; the node said:\n%s", got, byte(written.ID), rp, again.String())
	}
	s2, ok := cl.start(10 * time.Second)
	if !ok {
		t.Fatal("no session started after the restart")
	}
	if rp, ok := writeOK(t, p, cl, devices, s2, 15*time.Second); !ok {
		t.Fatalf("session %d's write after the restart was never acknowledged, the last answer %+v; the node said:\n%s", s2, rp, again.String())
	}
	if e := onCtl(t, p, func(_ *core.Context, c *meta.Control) chunkstore.PGEntry { return c.Table().Pools[0].PGs[0] }); e.Interval != before+1 || e.Activated != e.Interval || e.Members[0].Mark != chunkstore.MarkUp {
		t.Fatalf("after interval %d and a restart within node_down_after, PG 0 is at interval %d, activated %d, its member %v", before, e.Interval, e.Activated, e.Members[0].Mark)
	}
	if !within(2*meta.DeadWait.Duration(), func() bool {
		return onCtl(t, p, func(_ *core.Context, c *meta.Control) bool { return c != nil && fenced(c, s1) })
	}) {
		t.Fatalf("session %d, never renewed, was never fenced", s1)
	}
	type at struct {
		u  chunkstore.Update
		to core.NodeID
	}
	w := onCtl(t, p, func(_ *core.Context, c *meta.Control) at {
		u, to, _ := pgPrimary(c, devices)
		return at{u, to}
	})
	w.u.Session = s1
	if rp, ok := cl.write(w.u, w.to, 5*time.Second); !ok || rp.Status == chunkstore.StatusOK {
		t.Fatalf("the fenced session's write was answered %v: %+v", ok, rp)
	}
}

// A client-only node, `rock-solid node -role client` with no device, starts a session at the
// control plane over TCP and renews it: the one session it started is still live past dead_wait
// from its start, the longest the leader waits for a session that never renews.
func TestAClientOnlyNodeRenewsItsSessionOverTCP(t *testing.T) {
	p := runCtl(t, testDir(t))
	var out output
	startNode(t, fmt.Sprintf("-node 2 -ctl 1=127.0.0.1:%d -group %d -listen 127.0.0.1:%d -role client", p.Port, ctlGroup, freeBase(t, 1)), &out)
	live := func() []uint64 {
		return onCtl(t, p, func(_ *core.Context, c *meta.Control) []uint64 {
			if c == nil {
				return nil
			}
			var ids []uint64
			for _, e := range c.SessionTable().Sessions {
				if e.Node == 2 && e.Fenced == 0 {
					ids = append(ids, e.ID)
				}
			}
			return ids
		})
	}
	var first []uint64
	if !within(30*time.Second, func() bool { first = live(); return len(first) != 0 }) {
		t.Fatalf("the node started no session; it said:\n%s", out.String())
	}
	time.Sleep(time.Duration(meta.DeadWait)*time.Microsecond + 2*time.Second)
	if now := live(); len(first) != 1 || len(now) != 1 || now[0] != first[0] {
		t.Fatalf("live sessions %v at the start, %v past dead_wait; the node said:\n%s", first, now, out.String())
	}
}
