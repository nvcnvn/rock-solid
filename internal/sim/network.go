package sim

import (
	"fmt"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/trace"
)

// conn is a connection between two loops, as TCP keeps one: each direction delivers in order, and once
// a message on it is lost it loses everything until it fails, which each end learns from one PeerLost.
type conn struct {
	id     uint64
	ends   [2]core.NodeID
	done   [2]bool
	broken bool
}

// link is a one-way path between two nodes: a partition when in cuts, an added delay when in queues.
type link struct {
	from, to uint32
	d        core.Tick
}

func (c *conn) end(id core.NodeID) int {
	switch id {
	case c.ends[0]:
		return 0
	case c.ends[1]:
		return 1
	}
	return -1
}

// Partition loses every message node a sends to node b from now, and every one b sends to a unless oneWay, until Heal.
func (s *Scheduler) Partition(a, b uint32, oneWay bool) {
	s.cuts = append(s.cuts, link{from: a, to: b})
	if !oneWay {
		s.cuts = append(s.cuts, link{from: b, to: a})
	}
}

// Heal ends every partition between nodes a and b; a connection already broken stays broken until it fails.
func (s *Scheduler) Heal(a, b uint32) {
	s.cuts = slices.DeleteFunc(s.cuts, func(c link) bool { return c.from == a && c.to == b || c.from == b && c.to == a })
}

// Queue holds every message node from sends to node to for d beyond the network's delay, one way only, as a congested queue on that path does.
func (s *Scheduler) Queue(from, to uint32, d core.Tick) {
	s.queues = slices.DeleteFunc(s.queues, func(q link) bool { return q.from == from && q.to == to })
	if d > 0 {
		s.queues = append(s.queues, link{from: from, to: to, d: d})
	}
}

func (s *Scheduler) cut(from, to uint32) bool {
	return slices.ContainsFunc(s.cuts, func(c link) bool { return c.from == from && c.to == to })
}

func (s *Scheduler) queued(from, to uint32) core.Tick {
	for _, q := range s.queues {
		if q.from == from && q.to == to {
			return q.d
		}
	}
	return 0
}

// send puts a NetSend from loop l of n on its connection, or loses it.
func (s *Scheduler) send(n *Node, l uint32, a *core.Action) {
	if a.Msg.Buf != core.NoBuf {
		n.pool.Bytes(a.Msg.Buf)
	}
	from := core.NodeID{Node: n.id, Loop: l}
	it := item{kind: ChooseDeliver, node: a.Peer.Node, loop: a.Peer.Loop, act: *a, issued: s.now, from: from, fromGen: n.gen}
	if int(a.Peer.Node) >= len(s.nodes) {
		s.lose(&it)
		return
	}
	to := s.nodes[a.Peer.Node]
	c := s.connect(from, a.Peer)
	it.conn = c.id
	switch {
	case c.broken:
		s.lose(&it)
	case !to.reachable() || int(a.Peer.Loop) >= len(to.loops) || s.cut(n.id, to.id) || s.chance(s.set.DropNum, s.set.DropDen):
		s.lose(&it)
		s.breakConn(c)
	default:
		it.at = s.now + s.set.NetDelay + core.Tick(s.rand.Below(uint64(s.set.NetJitter)+1)) + s.queued(n.id, to.id)
		s.items = append(s.items, it)
		if s.chance(s.set.DupNum, s.set.DupDen) {
			if a.Msg.Buf != core.NoBuf {
				it.data = slices.Clone(n.pool.Bytes(a.Msg.Buf))
			}
			it.act.Reply = false
			s.items = append(s.items, it)
		}
	}
}

// connect is the connection from's sends to to go on, opening one if from has none; a new one fails first, at to, any older one to has not seen fail, as the peer's driver does when the restarted side connects again.
func (s *Scheduler) connect(from, to core.NodeID) *conn {
	for i := range s.conns {
		c := &s.conns[i]
		if e := c.end(from); e >= 0 && c.ends[1-e] == to && !c.done[e] {
			return c
		}
	}
	for i := range s.conns {
		c := &s.conns[i]
		if e := c.end(to); e >= 0 && c.ends[1-e] == from && !c.done[e] {
			s.failAt(c, e, s.now)
		}
	}
	s.connID++
	s.conns = append(s.conns, conn{id: s.connID, ends: [2]core.NodeID{from, to}, done: [2]bool{false, !s.nodes[to.Node].reachable()}})
	return &s.conns[len(s.conns)-1]
}

// reachable reports whether the node takes messages: it runs, and no pause that loses them stands it still.
func (n *Node) reachable() bool { return n.up && !(n.paused && n.lossy) }

func (s *Scheduler) connIndex(id uint64) int {
	return slices.IndexFunc(s.conns, func(c conn) bool { return c.id == id })
}

// breakConn loses every message on c and makes each live end's PeerLost due after a seeded delay up to FailAfter, or never.
func (s *Scheduler) breakConn(c *conn) { s.breakConnKeeping(c, -1) }

// breakConnKeeping breaks c but delivers what end keep already sent, as a crashed process's kernel sends its socket's data before closing it; a keep of -1 keeps nothing.
func (s *Scheduler) breakConnKeeping(c *conn, keep int) {
	if c.broken {
		return
	}
	c.broken = true
	for {
		i := slices.IndexFunc(s.items, func(it item) bool {
			return it.kind == ChooseDeliver && it.conn == c.id && (keep < 0 || it.from != c.ends[keep])
		})
		if i < 0 {
			break
		}
		it := s.remove(i)
		s.lose(&it)
	}
	if s.set.FailAfter == 0 {
		return
	}
	at := s.now + 1 + core.Tick(s.rand.Below(uint64(s.set.FailAfter)))
	for e := range c.ends {
		if !c.done[e] {
			s.failAt(c, e, at)
		}
	}
}

// failAt makes end e's PeerLost for c due by at.
func (s *Scheduler) failAt(c *conn, e int, at core.Tick) {
	c.broken = true
	to := c.ends[e]
	for i := range s.items {
		if it := &s.items[i]; it.kind == ChoosePeerLost && it.conn == c.id && it.node == to.Node && it.loop == to.Loop {
			it.at = min(it.at, at)
			return
		}
	}
	s.items = append(s.items, item{kind: ChoosePeerLost, node: to.Node, loop: to.Loop, gen: s.nodes[to.Node].gen, issued: s.now, at: at, from: c.ends[1-e], conn: c.id})
}

// closeConns ends node id's side of every connection when it crashes, breaking those still up after what it already sent.
func (s *Scheduler) closeConns(id uint32) {
	for i := range s.conns {
		c := &s.conns[i]
		for e, end := range c.ends {
			if end.Node == id && !c.done[e] {
				c.done[e] = true
				s.breakConnKeeping(c, e)
			}
		}
	}
	s.conns = slices.DeleteFunc(s.conns, func(c conn) bool { return c.done[0] && c.done[1] })
}

// breakConns breaks every connection node id has not seen fail.
func (s *Scheduler) breakConns(id uint32) {
	for i := range s.conns {
		c := &s.conns[i]
		for e, end := range c.ends {
			if end.Node == id && !c.done[e] {
				s.breakConn(c)
			}
		}
	}
}

// head reports whether item i is the first message or PeerLost still on its way from its sender loop to its receiver loop, so each direction delivers in order.
func (s *Scheduler) head(i int) bool {
	it := &s.items[i]
	for j := range i {
		o := &s.items[j]
		if (o.kind == ChooseDeliver || o.kind == ChoosePeerLost) && o.from == it.from && o.node == it.node && o.loop == it.loop {
			return false
		}
	}
	return true
}

// arrived is the first message or PeerLost due at node id that its connection can deliver, or -1.
func (s *Scheduler) arrived(id uint32) int {
	for i := range s.items {
		it := &s.items[i]
		if it.node == id && (it.kind == ChooseDeliver || it.kind == ChoosePeerLost) && it.at <= s.now && s.head(i) {
			return i
		}
	}
	return -1
}

// deliver copies the message's data out of the sender's buffer into one of the receiver's and hands it over.
func (s *Scheduler) deliver(it item) {
	s.now = max(s.now, it.at)
	from, to := s.nodes[it.from.Node], s.nodes[it.node]
	ev := core.Event{Kind: core.EvNetRecv, Peer: it.from, Msg: it.act.Msg}
	ev.Msg.Buf = core.NoBuf
	if b := it.act.Msg.Buf; b != core.NoBuf {
		data := it.data
		if data == nil {
			if !from.up || from.gen != it.fromGen || !from.pool.Valid(b) {
				s.violate(&it)
				return
			}
			data = from.pool.Bytes(b)
		}
		rb, err := to.pool.Alloc(len(data))
		if err != nil {
			s.lose(&it)
			s.breakConn(&s.conns[s.connIndex(it.conn)])
			return
		}
		copy(to.pool.Bytes(rb), data)
		ev.Msg.Buf = rb
	}
	s.release(&it)
	to.feed(it.loop, &ev)
}

// peerLost tells one end its connection failed; the connection is gone once both ends know.
func (s *Scheduler) peerLost(it item) {
	s.now = max(s.now, it.at)
	i := s.connIndex(it.conn)
	c := &s.conns[i]
	c.done[c.end(core.NodeID{Node: it.node, Loop: it.loop})] = true
	if c.done[0] && c.done[1] {
		s.conns = slices.Delete(s.conns, i, i+1)
	}
	ev := core.Event{Kind: core.EvPeerLost, Peer: it.from}
	s.nodes[it.node].feed(it.loop, &ev)
}

// lose records a message lost on its way, if its sender still runs, and frees it as sent.
func (s *Scheduler) lose(it *item) {
	n := s.nodes[it.from.Node]
	if !n.up || n.gen != it.fromGen {
		return
	}
	to := core.NodeID{Node: it.node, Loop: it.loop}
	s.record(n, it.from.Loop, n.Context().Now, trace.Record{Name: trace.EvDrop, Fields: [trace.MaxFields]uint64{trace.Peer(to), msgType(&it.act.Msg, s.set.MsgType)}})
	s.release(it)
}

// release frees a reply's buffer, as the driver does once the reply is sent, when no copy of it is left in the network.
func (s *Scheduler) release(it *item) {
	b := it.act.Msg.Buf
	n := s.nodes[it.from.Node]
	if !it.act.Reply || b == core.NoBuf || !n.up || n.gen != it.fromGen {
		return
	}
	for i := range s.items {
		o := &s.items[i]
		if o.kind == ChooseDeliver && o.act.Reply && o.act.Msg.Buf == b && o.from.Node == it.from.Node && o.fromGen == it.fromGen {
			return
		}
	}
	p := n.pool
	if !p.Valid(b) {
		s.violate(it)
		return
	}
	p.Free(b)
}

func (s *Scheduler) violate(it *item) {
	if s.contract == nil {
		s.contract = fmt.Errorf("loop %d.%d freed buffer %+v of a send to %d.%d while the network still held it", it.from.Node, it.from.Loop, it.act.Msg.Buf, it.node, it.loop)
	}
}
