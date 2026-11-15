// Package nettest is the shared network suite: the network driver contract, checked against a
// model of the messages sent, written once against the Net interface and run on every network
// driver.
package nettest

import (
	"fmt"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/loop"
)

// Net is a network driver under the suite: nodes of one loop each, numbered from 0, every loop
// running a machine the suite gives it. Nothing in it is particular to one transport, so a driver
// joins the suite by implementing it.
type Net interface {
	// Start starts node i's process with its loop running m and returns the loop's ID, the same on
	// every start of i. The suite starts nodes 0, 1, … in order, and starts a node again only after
	// Stop.
	Start(i int, m loop.Machine) core.NodeID
	// Stop ends node i's process: its connections close, and what it sent may or may not arrive.
	Stop(i int)
	// Pool is the buffer pool of node i's process, with room for 64 MiB out at once.
	Pool(i int) Pool
	// Call runs f as one pass of node i's loop and executes the actions it appends.
	Call(i int, f func(ctx *core.Context, out []core.Action) []core.Action)
	// Hold stops node i's loop reading until nothing else on the network moves.
	Hold(i int)
	// Run moves the network until done reports true, or with a nil done until nothing moves; it
	// reports whether done held, or with a nil done whether the network came to rest.
	Run(done func() bool) bool
	// Sleep lets d pass on every node's clocks once the network is at rest.
	Sleep(d core.Tick)
	// Backoff is the longest the driver refuses to dial a peer again after failing to reach it.
	Backoff() core.Tick
}

// Pool is a process's buffer pool as the suite sees it.
type Pool interface {
	core.BufPool
	Live() int
	Valid(b core.IOBuf) bool
}

// Case is one behaviour of the suite.
type Case struct {
	Name string
	Run  func(r *Rig)
}

// Run runs every case of the suite, each on a fresh network from newNet.
func Run(t *testing.T, newNet func(t *testing.T) Net) {
	for _, c := range Cases {
		t.Run(c.Name, func(t *testing.T) {
			r := NewRig(t, newNet(t))
			c.Run(r)
			r.Check()
		})
	}
}

// SkipSimCases lists each simulator-only case as skipped, for a driver whose faults cannot be
// injected the way the simulated network's are.
func SkipSimCases(t *testing.T) {
	for _, c := range SimCases {
		t.Run(c.Name, func(t *testing.T) { t.Skip("needs the simulated network's faults") })
	}
}

// Rig runs Peers on a Net and keeps the model: every message sent, by whom, to whom, and the
// PeerLost events its sender had seen for that peer when it went out.
type Rig struct {
	T     testing.TB
	N     Net
	peers []*Peer
	ids   []core.NodeID
	up    []bool
	past  []*Peer
	seq   uint64
	sent  []sent
}

type sent struct {
	from    *Peer
	to      core.NodeID
	inline  int
	data    int
	dataSeq uint64
	buf     core.IOBuf
	lostWas int
	got     bool
}

// NewRig makes a rig on n; StartNodes starts its nodes.
func NewRig(t testing.TB, n Net) *Rig { return &Rig{T: t, N: n} }

// StartNodes starts k more nodes, each running a fresh Peer.
func (r *Rig) StartNodes(k int) {
	for range k {
		r.ids = append(r.ids, core.NodeID{})
		r.peers = append(r.peers, nil)
		r.up = append(r.up, false)
		r.Start(len(r.ids)-1, &Peer{})
	}
}

// Start starts node i, which is down, running p, and returns p.
func (r *Rig) Start(i int, p *Peer) *Peer {
	if r.up[i] {
		r.T.Fatalf("node %d is already up", i)
	}
	if r.peers[i] != nil {
		r.past = append(r.past, r.peers[i])
	}
	r.peers[i], r.up[i], p.r = p, true, r
	r.ids[i] = r.N.Start(i, p)
	p.ID = r.ids[i]
	return p
}

// Stop stops node i.
func (r *Rig) Stop(i int) {
	r.N.Stop(i)
	r.up[i] = false
}

// ID is node i's loop.
func (r *Rig) ID(i int) core.NodeID { return r.ids[i] }

// Peer is node i's current machine.
func (r *Rig) Peer(i int) *Peer { return r.peers[i] }

// Msg is a message a case sends: its inline length, at least 8 and at most core.MaxInlineMsg, and
// the length of its pool buffer, 0 for none.
type Msg struct {
	To     core.NodeID
	Inline int
	Data   int
}

// Send sends msgs from node i in one pass, each a Reply whose buffer the driver frees once sent.
func (r *Rig) Send(i int, msgs ...Msg) {
	r.N.Call(i, func(ctx *core.Context, out []core.Action) []core.Action {
		p := r.peers[i]
		for _, m := range msgs {
			r.seq++
			a := core.Action{Kind: core.ActNetSend, Peer: m.To, Reply: true}
			a.Msg.Len = uint16(m.Inline)
			fill(a.Msg.Body[:m.Inline], r.seq)
			if m.Data > 0 {
				b, err := ctx.Pool.Alloc(m.Data)
				if err != nil {
					r.T.Fatalf("node %d's pool: %v", i, err)
				}
				fill(ctx.Pool.Bytes(b), r.seq)
				a.Msg.Buf = b
			}
			r.sent = append(r.sent, sent{from: p, to: m.To, inline: m.Inline, data: m.Data, dataSeq: r.seq, buf: a.Msg.Buf, lostWas: p.LostFrom(m.To)})
			out = append(out, a)
		}
		return out
	})
}

// SendShared sends one pool buffer of data bytes from node i to each of to in one pass, a Reply
// the driver frees once it is sent to all.
func (r *Rig) SendShared(i int, data int, to ...core.NodeID) {
	r.N.Call(i, func(ctx *core.Context, out []core.Action) []core.Action {
		p := r.peers[i]
		b, err := ctx.Pool.Alloc(data)
		if err != nil {
			r.T.Fatalf("node %d's pool: %v", i, err)
		}
		dataSeq := r.seq + 1
		fill(ctx.Pool.Bytes(b), dataSeq)
		for _, peer := range to {
			r.seq++
			a := core.Action{Kind: core.ActNetSend, Peer: peer, Reply: true, Msg: core.Msg{Len: 8, Buf: b}}
			fill(a.Msg.Body[:8], r.seq)
			r.sent = append(r.sent, sent{from: p, to: peer, inline: 8, data: data, dataSeq: dataSeq, lostWas: p.LostFrom(peer)})
			out = append(out, a)
		}
		return out
	})
}

// fill writes seq into b's first 8 bytes and a pattern of it after, or only the pattern into a
// shorter b.
func fill(b []byte, seq uint64) {
	for j := range b {
		b[j] = byte(seq*7 + uint64(j))
	}
	if len(b) >= 8 {
		for j := range 8 {
			b[j] = byte(seq >> (8 * j))
		}
	}
}

func seqOf(b []byte) uint64 {
	var s uint64
	for j := range 8 {
		s |= uint64(b[j]) << (8 * j)
	}
	return s
}

func whole(b []byte, seq uint64) bool {
	for j := 8; j < len(b); j++ {
		if b[j] != byte(seq*7+uint64(j)) {
			return false
		}
	}
	return true
}

func wholeData(b []byte, seq uint64) bool {
	for j := range b {
		want := byte(seq*7 + uint64(j))
		if len(b) >= 8 && j < 8 {
			want = byte(seq >> (8 * j))
		}
		if b[j] != want {
			return false
		}
	}
	return true
}

// Run moves the network until done holds, failing the case if it never does, after the end
// check when the model shows why.
func (r *Rig) Run(what string, done func() bool) {
	r.T.Helper()
	if !r.N.Run(done) {
		r.Check()
		r.T.Fatalf("the network came to rest before %s", what)
	}
}

// Settle moves the network until nothing moves.
func (r *Rig) Settle() {
	r.T.Helper()
	if !r.N.Run(nil) {
		r.T.Fatal("the network never came to rest")
	}
}

// Check settles the network and checks the model: every message arrived at most once, whole,
// naming its sender, and in the order sent between each two processes; between two PeerLost a
// process was handed for a peer, the messages it got from that peer are consecutive among those
// the peer sent it, and none comes from a later start of the peer; every message a live process
// sent that never arrived was followed by a PeerLost for its peer, and every buffer it sent alone
// to a peer was freed before that PeerLost; every live process's pool holds nothing.
func (r *Rig) Check() {
	r.T.Helper()
	r.Settle()
	type pair struct{ from, to *Peer }
	type route struct {
		from *Peer
		to   core.NodeID
	}
	last := map[pair]uint64{}
	nth, sentOn := make([]int, len(r.sent)), map[route]int{}
	for i := range r.sent {
		s := &r.sent[i]
		s.got = false
		nth[i] = sentOn[route{s.from, s.to}]
		sentOn[route{s.from, s.to}]++
	}
	for _, p := range r.allPeers() {
		prev := map[core.NodeID]uint64{}
		for _, e := range p.Events {
			if e.Lost {
				if e.Held != 0 {
					r.T.Fatalf("%v was handed PeerLost(%v) while its pool held %d buffers it had sent there", p.ID, e.From, e.Held)
				}
				delete(prev, e.From)
				continue
			}
			if e.Seq == 0 || e.Seq > uint64(len(r.sent)) {
				r.T.Fatalf("%v got a message the suite never sent: %+v", p.ID, e)
			}
			s := &r.sent[e.Seq-1]
			switch {
			case s.got:
				r.T.Fatalf("message %d arrived twice", e.Seq)
			case e.From != s.from.ID || s.to != p.ID:
				r.T.Fatalf("message %d from %v to %v arrived at %v naming %v", e.Seq, s.from.ID, s.to, p.ID, e.From)
			case !e.Whole || e.Inline != s.inline || e.Data != blocks(s.data) || s.data > 0 && e.DataSeq != s.dataSeq:
				r.T.Fatalf("message %d of %d inline and %d data bytes arrived with %d and %d, carrying message %d's data, whole %v", e.Seq, s.inline, s.data, e.Inline, e.Data, e.DataSeq, e.Whole)
			case e.Seq <= last[pair{s.from, p}]:
				r.T.Fatalf("message %d from %v arrived after message %d", e.Seq, e.From, last[pair{s.from, p}])
			}
			if q := prev[e.From]; q != 0 && r.sent[q-1].from != s.from {
				r.T.Fatalf("message %d from one start of %v arrived at %v after message %d of another, with no PeerLost between", e.Seq, e.From, p.ID, q)
			} else if q != 0 && nth[e.Seq-1] != nth[q-1]+1 {
				r.T.Fatalf("message %d from %v arrived at %v after message %d, with a message sent between them missing and no PeerLost", e.Seq, e.From, p.ID, q)
			}
			s.got = true
			last[pair{s.from, p}] = e.Seq
			prev[e.From] = e.Seq
		}
	}
	for i := range r.sent {
		s := &r.sent[i]
		if !s.got && r.live(s.from) && s.from.LostFrom(s.to) <= s.lostWas {
			r.T.Fatalf("message %d from %v to %v never arrived and its sender saw no PeerLost", i+1, s.from.ID, s.to)
		}
	}
	for i, up := range r.up {
		if up && r.N.Pool(i).Live() != 0 {
			r.T.Fatalf("node %d's pool holds %d buffers at rest", i, r.N.Pool(i).Live())
		}
	}
}

// blocks is n rounded up to whole blocks, as a pool buffer of n bytes travels.
func blocks(n int) int { return (n + core.BlockSize - 1) / core.BlockSize * core.BlockSize }

func (r *Rig) live(p *Peer) bool {
	for i, q := range r.peers {
		if q == p {
			return r.up[i]
		}
	}
	return false
}

// allPeers is every Peer the rig has started, the stopped ones too.
func (r *Rig) allPeers() []*Peer { return append(slices.Clone(r.past), r.peers...) }

// Event is a message or PeerLost a Peer was handed.
type Event struct {
	Lost   bool
	From   core.NodeID
	Seq    uint64
	Inline int
	Data   int
	// DataSeq is the message whose pattern its data carries, Seq's but for a buffer sent to several.
	DataSeq uint64
	Whole   bool
	// Held is the buffers the Peer had sent alone to From that its pool still held when it was
	// handed a PeerLost.
	Held int
}

// Peer is the machine the suite runs on every loop: it records each message and PeerLost it is
// handed and frees what it receives, unless Serve, when set, takes the event instead.
type Peer struct {
	ID     core.NodeID
	Events []Event
	r      *Rig
	// Serve, when set, handles every event in the Peer's place.
	Serve func(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action
}

// OnEvent records ev, or hands it to Serve.
func (p *Peer) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	if p.Serve != nil {
		return p.Serve(ctx, ev, out)
	}
	switch ev.Kind {
	case core.EvNetRecv:
		e := Event{From: ev.Peer, Inline: int(ev.Msg.Len), Whole: true}
		in := ev.Msg.Inline()
		if len(in) >= 8 {
			e.Seq = seqOf(in)
			e.Whole = whole(in, e.Seq)
		}
		if ev.Msg.Buf != core.NoBuf {
			b := ctx.Pool.Bytes(ev.Msg.Buf)
			e.Data, e.DataSeq = len(b), e.Seq
			if len(b) >= 8 {
				e.DataSeq = seqOf(b)
			}
			e.Whole = e.Whole && wholeData(b, e.DataSeq)
			ctx.Pool.Free(ev.Msg.Buf)
		}
		p.Events = append(p.Events, e)
	case core.EvPeerLost:
		p.Events = append(p.Events, Event{Lost: true, From: ev.Peer, Held: p.held(ctx.Pool.(Pool), ev.Peer)})
	}
	return out
}

// held counts every buffer still out to peer, so one already queued on a newer connection to it
// when an older connection's PeerLost arrives would count too; no case sends that way.
func (p *Peer) held(pool Pool, peer core.NodeID) int {
	n := 0
	for _, s := range p.r.sent {
		if s.from == p && s.to == peer && s.buf != core.NoBuf && pool.Valid(s.buf) {
			n++
		}
	}
	return n
}

// LostFrom is the number of PeerLost events p was handed for peer.
func (p *Peer) LostFrom(peer core.NodeID) int {
	n := 0
	for _, e := range p.Events {
		if e.Lost && e.From == peer {
			n++
		}
	}
	return n
}

// Got is the number of messages p received from peer.
func (p *Peer) Got(peer core.NodeID) int {
	n := 0
	for _, e := range p.Events {
		if !e.Lost && e.From == peer {
			n++
		}
	}
	return n
}

func (e Event) String() string {
	if e.Lost {
		return fmt.Sprintf("PeerLost(%v)", e.From)
	}
	return fmt.Sprintf("%d from %v", e.Seq, e.From)
}
