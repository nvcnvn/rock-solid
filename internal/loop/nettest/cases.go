package nettest

import (
	"bytes"
	"slices"
	"testing"

	"go.etcd.io/raft/v3"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/meta"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// Cases are the suite's behaviours that every driver shows.
var Cases = []Case{
	{"MessagesArriveWholeInOrderNamingTheirSender", messagesArriveWhole},
	{"ABufferSentToThreePeersIsFreedOnceAllHaveIt", fanOut},
	{"ATxnSentToTheWrongNodeIsAnsweredByTheLeaderItNames", txnRoundTrip},
	{"APeerLostMidRequestFailsEveryTxnSentToIt", peerLostMidRequest},
	{"ARestartedPeerIsANewConnection", restartIsNewConnection},
	{"TwoLoopsDialingEachOtherAtOnceLoseNothingSilently", dialAtOnce},
	{"ASendToAStoppedOrUnknownPeerEndsInPeerLost", sendToStoppedPeer},
	{"AReaderThatStopsLosesNothingSilently", readerThatStops},
}

// SimCase is a case that injects the simulated network's faults.
type SimCase struct {
	Name string
	Run  func(t *testing.T, seed uint64)
}

// SimCases are the cases only the simulated network can run.
var SimCases = []SimCase{
	{"APartitionFailsTheConnectionAtBothEnds", partition},
	{"EveryDroppedMessageEndsInPeerLostAtItsSender", drops},
}

// sizes are the inline and data lengths messages cycle through: the largest inline message, and
// data of one byte, one past the inline limit, a block and 1 MiB.
var sizes = []Msg{{Inline: 8}, {Inline: core.MaxInlineMsg}, {Inline: 100, Data: 1}, {Inline: 8, Data: core.MaxInlineMsg + 1}, {Inline: 64, Data: core.BlockSize}, {Inline: 8, Data: 1 << 20}}

func noLost(r *Rig, nodes int) {
	r.T.Helper()
	for i := range nodes {
		for _, e := range r.Peer(i).Events {
			if e.Lost {
				r.T.Fatalf("%v was handed %v", r.ID(i), e)
			}
		}
	}
}

// Each lower node opens its connections first: two loops that dial each other at once keep one
// dial, and the other's sends end in PeerLost.
func messagesArriveWhole(r *Rig) {
	r.StartNodes(3)
	r.Send(0, Msg{To: r.ID(1), Inline: 8}, Msg{To: r.ID(2), Inline: 8})
	r.Send(1, Msg{To: r.ID(2), Inline: 8})
	r.Run("the connections are up", func() bool {
		return r.Peer(1).Got(r.ID(0)) == 1 && r.Peer(2).Got(r.ID(0)) == 1 && r.Peer(2).Got(r.ID(1)) == 1
	})
	for round := range 3 {
		for i := range 3 {
			var msgs []Msg
			for k := range 20 {
				m := sizes[(round+k)%len(sizes)]
				m.To = r.ID((i + 1 + k%2) % 3)
				msgs = append(msgs, m)
			}
			r.Send(i, msgs...)
		}
	}
	r.Run("every message arrived", func() bool {
		return r.Peer(0).Got(r.ID(1))+r.Peer(0).Got(r.ID(2)) == 60 &&
			r.Peer(1).Got(r.ID(0))+r.Peer(1).Got(r.ID(2)) == 61 &&
			r.Peer(2).Got(r.ID(0))+r.Peer(2).Got(r.ID(1)) == 62
	})
	r.Check()
	noLost(r, 3)
}

func fanOut(r *Rig) {
	r.StartNodes(5)
	r.Stop(4)
	r.SendShared(0, core.BlockSize, r.ID(1), r.ID(2), r.ID(3), r.ID(4))
	r.Run("three peers got the buffer and the stopped one was lost", func() bool {
		return r.Peer(1).Got(r.ID(0)) == 1 && r.Peer(2).Got(r.ID(0)) == 1 && r.Peer(3).Got(r.ID(0)) == 1 && r.Peer(0).LostFrom(r.ID(4)) == 1
	})
	r.Check()
}

// txnServer answers each Txn it is sent with answer's Answer for it, or holds it when answer
// returns false, and keeps every Txn it heard.
type txnServer struct {
	heard  []meta.Txn
	tokens []uint64
	answer func(token uint64) (meta.Answer, bool)
	txn    meta.Txn
}

func (s *txnServer) serve(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	if ev.Kind != core.EvNetRecv {
		return out
	}
	group, kind, token, ok := meta.Peek(&ev.Msg)
	if !ok || kind != meta.KindTxn || meta.DecodeTxn(ctx.Pool, &ev.Msg, &s.txn) != nil {
		panic("nettest: a server got no Txn")
	}
	t := meta.Txn{}
	for _, op := range s.txn.Ops {
		t.Ops = append(t.Ops, meta.Op{Kind: op.Kind, Key: bytes.Clone(op.Key), Value: bytes.Clone(op.Value)})
	}
	if ev.Msg.Buf != core.NoBuf {
		ctx.Pool.Free(ev.Msg.Buf)
	}
	s.heard, s.tokens = append(s.heard, t), append(s.tokens, token)
	a, ok := s.answer(token)
	if !ok {
		return out
	}
	act := core.Action{Kind: core.ActNetSend, Peer: ev.Peer, Reply: true}
	if err := meta.EncodeAnswer(ctx.Pool, group, &a, &act.Msg); err != nil {
		panic(err)
	}
	return append(out, act)
}

func committed(token uint64) (meta.Answer, bool) {
	return meta.Answer{Token: token, Result: meta.Result{FailedCheck: -1}}, true
}

func holds(uint64) (meta.Answer, bool) { return meta.Answer{}, false }

// txnClient is a meta.Client on a loop, keeping every Answer it settles.
type txnClient struct {
	cl      *meta.Client
	out     meta.RouterOutput
	answers []meta.Answer
}

const dataGroup = 5

// newTxnClient routes the data group, whose voters are on nodes, to their loops.
func newTxnClient(r *Rig, inc uint64, nodes ...int) *txnClient {
	m := meta.ShardMap{Groups: []meta.Group{{ID: 1}, {ID: dataGroup, Start: []byte{meta.SpaceCtl + 1}}}}
	for k, i := range nodes {
		m.Groups[1].Voters = append(m.Groups[1].Voters, meta.Replica{ID: uint64(k) + 1, Node: r.ID(i).Node})
	}
	rt, err := meta.NewRouter(&m, 1)
	if err != nil {
		r.T.Fatal(err)
	}
	c := &txnClient{cl: meta.NewClient(rt, inc)}
	for _, i := range nodes {
		c.cl.SetLoop(r.ID(i).Node, dataGroup, r.ID(i).Loop)
	}
	return c
}

func (c *txnClient) take(out []core.Action) []core.Action {
	c.answers = append(c.answers, c.out.Answers...)
	out = append(out, c.out.Actions...)
	c.out.Reset()
	return out
}

func (c *txnClient) serve(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	c.cl.OnEvent(ctx, ev, &c.out)
	return c.take(out)
}

// send sends t from node i, which stays unchanged until its Answer, and returns its token.
func (c *txnClient) send(r *Rig, i int, t *meta.Txn) uint64 {
	var tok uint64
	r.N.Call(i, func(ctx *core.Context, out []core.Action) []core.Action {
		var err error
		if tok, err = c.cl.Txn(ctx, t, &c.out); err != nil {
			r.T.Fatal(err)
		}
		return c.take(out)
	})
	return tok
}

func put(r *Rig, inode uint64, value string) meta.Txn {
	k, err := meta.AppendKey(nil, &meta.Key{Space: meta.SpaceFS, Volume: 1, Inode: inode, Tag: meta.InodeAttr})
	if err != nil {
		r.T.Fatal(err)
	}
	return meta.Txn{Ops: []meta.Op{meta.Put(k, []byte(value))}}
}

func sameTxn(a, b *meta.Txn) bool {
	if len(a.Ops) != len(b.Ops) {
		return false
	}
	for i := range a.Ops {
		if a.Ops[i].Kind != b.Ops[i].Kind || !bytes.Equal(a.Ops[i].Key, b.Ops[i].Key) || !bytes.Equal(a.Ops[i].Value, b.Ops[i].Value) {
			return false
		}
	}
	return true
}

// The client guesses node 1, which answers NotLeader naming node 2; the client sends the Txn on
// to node 2, whose answer settles it under the client's token.
func txnRoundTrip(r *Rig) {
	r.StartNodes(3)
	c := newTxnClient(r, 1, 1, 2)
	follower := &txnServer{answer: func(token uint64) (meta.Answer, bool) {
		return meta.Answer{Token: token, Result: meta.Result{FailedCheck: -1}, Err: meta.NotLeader{Group: dataGroup, Leader: 2, Node: r.ID(2).Node, Term: 1}}, true
	}}
	leader := &txnServer{answer: committed}
	r.Peer(0).Serve, r.Peer(1).Serve, r.Peer(2).Serve = c.serve, follower.serve, leader.serve
	t := put(r, 7, "v")
	tok := c.send(r, 0, &t)
	r.Run("the Txn was answered", func() bool { return len(c.answers) > 0 })
	switch a := c.answers[0]; {
	case len(c.answers) != 1 || a.Token != tok || a.Err != nil || a.Result.FailedCheck != -1:
		r.T.Fatalf("answers %+v, want one committed under token %d", c.answers, tok)
	case len(follower.heard) != 1 || len(leader.heard) != 1 || !sameTxn(&follower.heard[0], &t) || !sameTxn(&leader.heard[0], &t):
		r.T.Fatalf("the follower heard %+v and the leader %+v, want %+v each", follower.heard, leader.heard, t)
	case follower.tokens[0] != tok || leader.tokens[0] != tok:
		r.T.Fatalf("tokens %v and %v, want %d", follower.tokens, leader.tokens, tok)
	}
}

// Three Txns wait at a server that stops: the client is handed one PeerLost, with every buffer
// it sent already freed, and settles all three as dropped; a Txn to the server's next start is
// answered.
func peerLostMidRequest(r *Rig) {
	r.StartNodes(2)
	c := newTxnClient(r, 1, 1)
	server := &txnServer{answer: holds}
	r.Peer(1).Serve = server.serve
	lost := 0
	r.Peer(0).Serve = func(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
		if ev.Kind == core.EvPeerLost {
			lost++
			if live := ctx.Pool.(Pool).Live(); live != 0 {
				r.T.Fatalf("the client's pool holds %d buffers when it is handed PeerLost", live)
			}
		}
		return c.serve(ctx, ev, out)
	}
	ts := []meta.Txn{put(r, 1, "a"), put(r, 2, "b"), put(r, 3, "c")}
	var toks []uint64
	for i := range ts {
		toks = append(toks, c.send(r, 0, &ts[i]))
	}
	r.Run("the server heard three Txns", func() bool { return len(server.heard) == 3 })
	r.Stop(1)
	r.Run("every Txn was settled", func() bool { return len(c.answers) == 3 })
	var got []uint64
	for _, a := range c.answers {
		if a.Err != raft.ErrProposalDropped {
			r.T.Fatalf("answers %+v, want tokens %v dropped", c.answers, toks)
		}
		got = append(got, a.Token)
	}
	if slices.Sort(got); !slices.Equal(got, toks) {
		r.T.Fatalf("answers %+v, want tokens %v dropped", c.answers, toks)
	}
	r.Settle()
	if lost != 1 {
		r.T.Fatalf("the client was handed %d PeerLost, want 1", lost)
	}
	again := &txnServer{answer: committed}
	r.Start(1, &Peer{Serve: again.serve})
	t := put(r, 4, "d")
	tok := c.send(r, 0, &t)
	r.Run("the Txn after the restart was answered", func() bool { return len(c.answers) == 4 })
	if a := c.answers[3]; a.Token != tok || a.Err != nil || len(again.heard) != 1 {
		r.T.Fatalf("after the restart: %+v, the server heard %d", a, len(again.heard))
	}
}

// A peer stops with messages on their way and starts again at once, sending more, twice: first
// while the other end reads, then while it does not, so the new connection arrives with more of
// the old one's left to read than a pass reads. Each start's messages arrive as one connection's,
// with one PeerLost between two starts.
func restartIsNewConnection(r *Rig) {
	r.StartNodes(2)
	a, b := r.ID(0), r.ID(1)
	r.Send(1, Msg{To: a, Inline: 8}, Msg{To: a, Inline: 8, Data: core.BlockSize})
	r.Run("the first connection is up", func() bool { return r.Peer(0).Got(b) == 2 })
	msgs := make([]Msg, 64)
	for k := range msgs {
		msgs[k] = Msg{To: a, Inline: 8, Data: 16 << 10}
	}
	var ends []uint64
	for _, hold := range []bool{false, true} {
		if hold {
			r.N.Hold(0)
		}
		r.Send(1, msgs...)
		ends = append(ends, r.seq)
		r.Stop(1)
		r.Start(1, &Peer{})
		r.Send(1, msgs...)
		from := r.seq - uint64(len(msgs))
		r.Run("every message of the new start arrived", func() bool {
			n := 0
			for _, e := range r.Peer(0).Events {
				if !e.Lost && e.Seq > from {
					n++
				}
			}
			return n == len(msgs)
		})
	}
	r.Check()
	lost := 0
	for _, e := range r.Peer(0).Events {
		switch {
		case e.Lost:
			lost++
		case lost > len(ends) || lost < len(ends) && e.Seq > ends[lost] || lost > 0 && e.Seq <= ends[lost-1]:
			r.T.Fatalf("a got message %d after %d PeerLost, the starts ending at messages %v: %v", e.Seq, lost, ends, r.Peer(0).Events)
		}
	}
	if lost != len(ends) {
		r.T.Fatalf("a was handed %d PeerLost for %d restarts: %v", lost, len(ends), r.Peer(0).Events)
	}
}

// A send to a peer that is down, or was never up, ends in PeerLost with its buffer freed; once
// the peer is up again, and the sender's backoff over, a send reaches it.
func sendToStoppedPeer(r *Rig) {
	r.StartNodes(2)
	a, b := r.ID(0), r.ID(1)
	unknown := core.NodeID{Node: 99, Loop: 1}
	r.Stop(1)
	for range 3 {
		r.Send(0, Msg{To: b, Inline: 8, Data: 100}, Msg{To: unknown, Inline: 8, Data: 100})
		r.Settle()
	}
	if n := r.Peer(0).LostFrom(b); n < 1 || n > 3 || r.Peer(0).LostFrom(unknown) < 1 {
		r.T.Fatalf("a was handed %v", r.Peer(0).Events)
	}
	r.Check()
	r.Start(1, &Peer{})
	r.N.Sleep(r.N.Backoff())
	r.Send(0, Msg{To: b, Inline: 8, Data: 100})
	r.Run("the send after the restart arrived", func() bool { return r.Peer(1).Got(a) == 1 })
}

// A reader stops while 40 MiB are sent to it: what arrives is in order, whatever does not ends
// in one PeerLost at the sender, and a send after reaches the reader.
func readerThatStops(r *Rig) {
	r.StartNodes(2)
	a, b := r.ID(0), r.ID(1)
	r.Send(0, Msg{To: b, Inline: 8})
	r.Run("the connection is up", func() bool { return r.Peer(1).Got(a) == 1 })
	r.N.Hold(1)
	for range 10 {
		msgs := make([]Msg, 4)
		for k := range msgs {
			msgs[k] = Msg{To: b, Inline: 8, Data: 1 << 20}
		}
		r.Send(0, msgs...)
	}
	r.Check()
	if n := r.Peer(0).LostFrom(b); n > 1 || n == 0 && r.Peer(1).Got(a) != 41 {
		r.T.Fatalf("b got %d of 41 messages, a was handed %d PeerLost", r.Peer(1).Got(a), n)
	}
	r.T.Logf("b got %d of 41 messages, a was handed %d PeerLost", r.Peer(1).Got(a), r.Peer(0).LostFrom(b))
	got := r.Peer(1).Got(a)
	r.Send(0, Msg{To: b, Inline: 8})
	r.Run("the send after arrived", func() bool { return r.Peer(1).Got(a) == got+1 })
}

// Every node sends to both others before any connection is up, so each pair dials each other at
// once; whichever dial a driver keeps, every send is delivered or reported lost.
func dialAtOnce(r *Rig) {
	r.StartNodes(3)
	for range 2 {
		for i := range 3 {
			r.Send(i, Msg{To: r.ID((i + 1) % 3), Inline: 8}, Msg{To: r.ID((i + 2) % 3), Inline: 8, Data: 100})
		}
		r.Settle()
	}
}

func partition(t *testing.T, seed uint64) {
	n := NewSimNet(t, seed, sim.DefaultSettings())
	r := NewRig(t, n)
	r.StartNodes(2)
	a, b := r.ID(0), r.ID(1)
	r.Send(0, Msg{To: b, Inline: 8})
	r.Run("the connection is up", func() bool { return r.Peer(1).Got(a) == 1 })
	n.S.Partition(0, 1, false)
	r.Send(0, Msg{To: b, Inline: 8}, Msg{To: b, Inline: 8, Data: 300})
	r.Run("both ends were handed PeerLost", func() bool { return r.Peer(0).LostFrom(b) == 1 && r.Peer(1).LostFrom(a) == 1 })
	if r.Peer(1).Got(a) != 1 {
		t.Fatalf("b got %d messages across the partition", r.Peer(1).Got(a)-1)
	}
	n.S.Heal(0, 1)
	r.Send(0, Msg{To: b, Inline: 8})
	r.Run("the send after Heal arrived", func() bool { return r.Peer(1).Got(a) == 2 })
	r.Check()
}

func drops(t *testing.T, seed uint64) {
	set := sim.DefaultSettings()
	set.DropNum, set.DropDen = 1, 10
	r := NewRig(t, NewSimNet(t, seed, set))
	r.StartNodes(3)
	for round := range 10 {
		for i := range 3 {
			var msgs []Msg
			for k := range 10 {
				m := sizes[(round+k)%len(sizes)]
				m.To = r.ID((i + 1 + k%2) % 3)
				msgs = append(msgs, m)
			}
			r.Send(i, msgs...)
		}
		r.Settle()
	}
	r.Check()
	lost := 0
	for i := range 3 {
		for j := range 3 {
			lost += r.Peer(i).LostFrom(r.ID(j))
		}
	}
	if lost == 0 {
		t.Fatal("no message was dropped")
	}
}
