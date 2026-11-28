package meta

import (
	"bytes"
	"errors"
	"slices"
	"testing"

	"go.etcd.io/raft/v3"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/lease"
	"github.com/nvcnvn/rock-solid/internal/raftshard"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// wireBytes is a message's inline bytes and then its body in a pool buffer, if any.
func wireBytes(pool core.BufPool, msg *core.Msg) []byte {
	out := bytes.Clone(msg.Inline())
	if msg.Buf != core.NoBuf {
		out = append(out, pool.Bytes(msg.Buf)[:le.Uint32(msg.Inline()[msgHead:])]...)
	}
	return out
}

// wireMsg is the message whose inline bytes are inline and whose body, if any, is in a pool buffer.
func wireMsg(pool core.BufPool, inline, body []byte) (core.Msg, bool) {
	var m core.Msg
	if m.SetInline(inline) != nil {
		return m, false
	}
	if len(body) > 0 {
		b, err := pool.Alloc(len(body))
		if err != nil {
			return m, false
		}
		copy(pool.Bytes(b), body)
		m.Buf = b
	}
	return m, true
}

func TestTxnsAndAnswersCrossTheWireWhole(t *testing.T) {
	pool := sim.NewPool(1<<20, 0)
	big := Txn{}
	for i := range 40 {
		big.Ops = append(big.Ops, Put(attr(1, 2, uint64(i)+1), bytes.Repeat(b("v"), 50)))
	}
	for _, tx := range []Txn{{Ops: []Op{Put(attr(1, 2, 1), b("v"))}}, big} {
		var m core.Msg
		if err := EncodeTxn(pool, 5, 9, &tx, &m); err != nil {
			t.Fatal(err)
		}
		var got Txn
		g, k, tok, ok := Peek(&m)
		if err := DecodeTxn(pool, &m, &got); err != nil || !ok || g != 5 || k != KindTxn || tok != 9 || !bytes.Equal(appendTxn(nil, &got), appendTxn(nil, &tx)) {
			t.Fatalf("decoded %v %d %d %d: %+v", err, g, k, tok, got)
		}
		if (m.Buf != core.NoBuf) != (len(tx.Ops) > 1) {
			t.Fatalf("a Txn of %d ops travels in a buffer: %v", len(tx.Ops), m.Buf != core.NoBuf)
		}
		if m.Buf != core.NoBuf {
			pool.Free(m.Buf)
		}
	}
	nl := NotLeader{Group: 5, Leader: 2, Node: 1, Term: 7}
	for _, a := range []Answer{
		{Token: 1, Result: Result{FailedCheck: -1, Updated: []uint64{4, 5}}},
		{Token: 2, Result: Result{FailedCheck: 0, Actual: b("old")}},
		{Token: 3, Result: Result{FailedCheck: 1, Actual: []byte{}}},
		{Token: 4, Result: Result{FailedCheck: -1, Actual: bytes.Repeat(b("x"), MaxValue)}},
		{Token: 5, Result: Result{FailedCheck: -1}, Err: nl},
		{Token: 6, Result: Result{FailedCheck: -1}, Err: ErrHalted},
		{Token: 7, Result: Result{FailedCheck: -1}, Err: ErrReplayTagged},
		{Token: 8, Result: Result{FailedCheck: -1}, Err: raft.ErrProposalDropped},
		{Token: 9, Result: Result{FailedCheck: -1}, Err: ErrOverloaded},
	} {
		var m core.Msg
		if err := EncodeAnswer(pool, 5, &a, &m); err != nil {
			t.Fatal(err)
		}
		var got Answer
		if err := DecodeAnswer(pool, &m, &got); err != nil || got.Token != a.Token || got.Err != a.Err || got.Result.FailedCheck != a.Result.FailedCheck ||
			!bytes.Equal(got.Result.Actual, a.Result.Actual) || (got.Result.Actual == nil) != (a.Result.Actual == nil) || len(got.Result.Updated) != len(a.Result.Updated) {
			t.Fatalf("sent %+v, got %+v, %v", a, got, err)
		}
		if m.Buf != core.NoBuf {
			pool.Free(m.Buf)
		}
	}
	for code, e := range wireErrs {
		var m core.Msg
		var got Answer
		if err := EncodeAnswer(pool, 5, &Answer{Token: 1, Result: Result{FailedCheck: -1}, Err: e}, &m); code != codeNotLeader && (err != nil ||
			DecodeAnswer(pool, &m, &got) != nil || got.Err != e) {
			t.Fatalf("code %d: sent %v, got %v", code, e, got.Err)
		}
	}
	var m core.Msg
	var got Answer
	if err := EncodeAnswer(pool, 5, &Answer{Token: 9, Result: Result{FailedCheck: -1}, Err: errors.New("raft: some other error")}, &m); err != nil ||
		DecodeAnswer(pool, &m, &got) != nil || got.Err != raft.ErrProposalDropped {
		t.Fatalf("an unknown error travels as %v", got.Err)
	}
	if pool.Live() != 0 {
		t.Fatalf("%d buffers live", pool.Live())
	}
}

func fuzzWire(f *testing.F, kind byte, decode func(pool core.BufPool, m *core.Msg) (core.Msg, bool)) {
	pool := sim.NewPool(1<<20, 0)
	for _, v := range [][]byte{b("v"), bytes.Repeat(b("w"), 300)} {
		tx := Txn{Ops: []Op{Put(attr(1, 2, 1), v), CheckAbsent(b("k"), true)}, Replay: replayKey(1, 1)}
		var m core.Msg
		switch kind {
		case KindTxn:
			_ = EncodeTxn(pool, 5, 9, &tx, &m)
		case KindAnswer:
			_ = EncodeAnswer(pool, 5, &Answer{Token: 9, Result: Result{FailedCheck: 0, Actual: v, Updated: []uint64{1}}, Err: NotLeader{Group: 5, Leader: 1}}, &m)
		case KindRead:
			_ = EncodeRead(pool, 5, 9, &Read{Start: v, End: append(v, 0), Limit: 3, Reg: byte(len(v)) % 5, Session: 7, Mount: 2, Term: 4}, &m)
		case KindReadAnswer:
			_ = EncodeReadAnswer(pool, 5, &ReadAnswer{Token: 9, KVs: []KV{{Key: b("k"), Value: v}, {Key: v, Value: nil}}, More: true}, &m)
		}
		all := wireBytes(pool, &m)
		f.Add(all[:m.Len], all[m.Len:])
	}
	f.Fuzz(func(t *testing.T, inline, body []byte) {
		pool := sim.NewPool(1<<20, 0)
		m, ok := wireMsg(pool, inline, body)
		if !ok {
			return
		}
		if again, ok := decode(pool, &m); ok {
			if out, in := wireBytes(pool, &again), wireBytes(pool, &m); !bytes.Equal(out, in) {
				t.Fatalf("decoded and encoded again:\n%x\nwas\n%x", out, in)
			}
			if again.Buf != core.NoBuf {
				pool.Free(again.Buf)
			}
		}
		if m.Buf != core.NoBuf {
			pool.Free(m.Buf)
		}
		if pool.Live() != 0 {
			t.Fatalf("%d buffers live", pool.Live())
		}
	})
}

func FuzzDecodeTxnMsg(f *testing.F) {
	fuzzWire(f, KindTxn, func(pool core.BufPool, m *core.Msg) (core.Msg, bool) {
		var tx Txn
		var again core.Msg
		g, _, tok, _ := Peek(m)
		if DecodeTxn(pool, m, &tx) != nil {
			return again, false
		}
		if err := EncodeTxn(pool, g, tok, &tx, &again); err != nil {
			panic(err)
		}
		return again, true
	})
}

func FuzzDecodeAnswerMsg(f *testing.F) {
	fuzzWire(f, KindAnswer, func(pool core.BufPool, m *core.Msg) (core.Msg, bool) {
		var a Answer
		var again core.Msg
		g, _, _, _ := Peek(m)
		if DecodeAnswer(pool, m, &a) != nil {
			return again, false
		}
		if err := EncodeAnswer(pool, g, &a, &again); err != nil {
			panic(err)
		}
		return again, true
	})
}

func FuzzDecodeReadMsg(f *testing.F) {
	fuzzWire(f, KindRead, func(pool core.BufPool, m *core.Msg) (core.Msg, bool) {
		var rd Read
		var again core.Msg
		g, _, tok, _ := Peek(m)
		if DecodeRead(pool, m, &rd) != nil {
			return again, false
		}
		if err := EncodeRead(pool, g, tok, &rd, &again); err != nil {
			panic(err)
		}
		return again, true
	})
}

func FuzzDecodeReadAnswerMsg(f *testing.F) {
	fuzzWire(f, KindReadAnswer, func(pool core.BufPool, m *core.Msg) (core.Msg, bool) {
		var a ReadAnswer
		var again core.Msg
		g, _, _, _ := Peek(m)
		if DecodeReadAnswer(pool, m, &a) != nil {
			return again, false
		}
		if err := EncodeReadAnswer(pool, g, &a, &again); err != nil {
			panic(err)
		}
		return again, true
	})
}

// Reads and their answers decode as they were sent, a nil end apart from an empty one, inline or
// in a buffer, every error code included, and a limit above MaxScanBytes goes as MaxScanBytes.
func TestReadsAndTheirAnswersCrossTheWireWhole(t *testing.T) {
	pool := sim.NewPool(1<<20, 0)
	free := func(m *core.Msg) {
		if m.Buf != core.NoBuf {
			pool.Free(m.Buf)
		}
	}
	long := bytes.Repeat(b("k"), MaxKey+1)
	for _, rd := range []Read{{Start: b("a"), End: b("b"), Limit: 1}, {Start: nil, End: nil, Limit: MaxScanBytes}, {Start: b("a"), End: []byte{}, Limit: 2},
		{Start: long, End: long, Limit: 7}, {Start: b("a"), Limit: 1 << 30}, {Start: b("a"), Limit: 1, Reg: RegOpen, Session: 1 << 40, Mount: 3},
		{Start: long, End: long, Limit: 1, Reg: RegRelease, Session: 2, Mount: 1 << 31}} {
		var m core.Msg
		if err := EncodeRead(pool, 5, 9, &rd, &m); err != nil {
			t.Fatal(err)
		}
		var got Read
		g, k, tok, _ := Peek(&m)
		if err := DecodeRead(pool, &m, &got); err != nil || g != 5 || k != KindRead || tok != 9 || !bytes.Equal(got.Start, rd.Start) ||
			!bytes.Equal(got.End, rd.End) || (got.End == nil) != (rd.End == nil) || got.Limit != min(rd.Limit, MaxScanBytes) ||
			got.Reg != rd.Reg || got.Session != rd.Session || got.Mount != rd.Mount {
			t.Fatalf("sent %+v, got %+v, %v", rd, got, err)
		}
		free(&m)
	}
	for _, rd := range []Read{{Start: b("a"), Limit: 0}, {Start: append(long, 0), Limit: 1}, {End: append(long, 0), Limit: 1}} {
		var m core.Msg
		if err := EncodeRead(pool, 5, 9, &rd, &m); err != ErrLimit && err != ErrTooLarge {
			t.Fatalf("encoded %+v: %v", rd, err)
		}
	}
	var many []KV
	for i := range 300 {
		many = append(many, KV{Key: attr(1, 2, uint64(i)+1), Value: bytes.Repeat(b("v"), i)})
	}
	for _, a := range []ReadAnswer{{Token: 1}, {Token: 2, KVs: []KV{{Key: b("k"), Value: []byte{}}}, More: true}, {Token: 3, KVs: many},
		{Token: 4, Err: NotLeader{Group: 5, Leader: 2, Node: 1, Term: 7}}, {Token: 5, Err: ErrHalted}, {Token: 6, Err: raft.ErrProposalDropped}} {
		var m core.Msg
		if err := EncodeReadAnswer(pool, 5, &a, &m); err != nil {
			t.Fatal(err)
		}
		var got ReadAnswer
		if err := DecodeReadAnswer(pool, &m, &got); err != nil || got.Token != a.Token || got.Err != a.Err || got.More != a.More ||
			!slices.EqualFunc(got.KVs, a.KVs, func(x, y KV) bool { return bytes.Equal(x.Key, y.Key) && bytes.Equal(x.Value, y.Value) }) {
			t.Fatalf("sent %+v, got %+v, %v", a, got, err)
		}
		if (m.Buf != core.NoBuf) != (len(a.KVs) > 1) {
			t.Fatalf("an answer of %d KVs travels in a buffer: %v", len(a.KVs), m.Buf != core.NoBuf)
		}
		free(&m)
	}
	if pool.Live() != 0 {
		t.Fatalf("%d buffers live", pool.Live())
	}
}

// wired is a group's replicas each with a Server, and a Client on node 3 whose router starts at
// the voter on node first.
type wired struct {
	c       *group
	cl      *Client
	servers []*Server
	ctx     core.Context
	out     RouterOutput
	// timer is whether the client set its router's timer.
	timer bool
}

func newWired(t *testing.T, first int) *wired {
	w := &wired{c: newGroup(t, 3, false), ctx: core.Context{Pool: sim.NewPool(1<<20, 0)}}
	m := oneShard(first)
	r, err := NewRouter(&m, 1)
	if err != nil {
		t.Fatal(err)
	}
	w.cl = NewClient(r, 1)
	for range 3 {
		w.servers = append(w.servers, NewServer())
	}
	return w
}

// run carries the client's NetSends to the replicas' servers and their answers back, until none
// is left, and returns the loops the Txns went to.
func (w *wired) run(t *testing.T) []core.NodeID {
	var to []core.NodeID
	client := core.NodeID{Node: 3}
	for len(w.out.Actions) > 0 {
		acts := w.out.Actions
		w.out.Actions = nil
		for _, a := range acts {
			if a.Kind == core.ActSetTimer {
				w.timer = true
				continue
			}
			if a.Kind != core.ActNetSend {
				t.Fatalf("the client asked for %+v", a)
			}
			to = append(to, a.Peer)
			n := int(a.Peer.Node)
			ev := core.Event{Kind: core.EvNetRecv, Peer: client, Msg: wireCopy(w.ctx.Pool, w.c.n[n].pool, &a.Msg)}
			if a.Msg.Buf != core.NoBuf {
				w.ctx.Pool.Free(a.Msg.Buf)
			}
			var replies []core.Action
			_, kind, _, _ := Peek(&ev.Msg)
			w.c.call(n, func(ctx *core.Context, o *Output) {
				if kind == KindRead {
					replies = w.servers[n].ServeRead(ctx, &ev, w.c.n[n].s, nil)
					return
				}
				replies = w.servers[n].Serve(ctx, &ev, w.c.n[n].s, o, nil)
				replies = w.servers[n].Reply(ctx, o.Answers, replies)
			})
			if len(replies) == 0 {
				w.c.settle()
				ans := w.c.answer(n, ServerToken|w.servers[n].next)
				ctx := w.c.ctx(n)
				replies = w.servers[n].Reply(&ctx, []Answer{ans}, nil)
			}
			for _, rp := range replies {
				if rp.Peer != client {
					t.Fatalf("an answer went to %v", rp.Peer)
				}
				back := core.Event{Kind: core.EvNetRecv, Peer: a.Peer, Msg: wireCopy(w.c.n[n].pool, w.ctx.Pool, &rp.Msg)}
				if rp.Msg.Buf != core.NoBuf {
					w.c.n[n].pool.Free(rp.Msg.Buf)
				}
				w.cl.OnEvent(&w.ctx, &back, &w.out)
			}
		}
	}
	return to
}

// wireCopy is msg as it arrives in another node's pool.
func wireCopy(from, to core.BufPool, msg *core.Msg) core.Msg {
	m := *msg
	if msg.Buf != core.NoBuf {
		b, err := to.Alloc(len(from.Bytes(msg.Buf)))
		if err != nil {
			panic(err)
		}
		copy(to.Bytes(b), from.Bytes(msg.Buf))
		m.Buf = b
	}
	return m
}

// A Txn sent to a follower is answered NotLeader over the wire, resent to the leader's loop and
// answered there, under the token the client gave it, and every buffer is freed.
func TestATxnCrossesTheWireToTheLeader(t *testing.T) {
	w := newWired(t, 0)
	l := w.c.elect()
	f := (l + 1) % 3
	m := oneShard(f)
	if err := w.cl.R.SetMap(&m, &w.out); err != nil {
		t.Fatal(err)
	}
	w.cl.SetLoop(uint32(l), dataGroup, 2)
	tx := Txn{Ops: []Op{Put(attr(1, 2, 1), b("v"))}, Replay: replayKey(1, 1)}
	tok, err := w.cl.Txn(&w.ctx, &tx, &w.out)
	if err != nil {
		t.Fatal(err)
	}
	to := w.run(t)
	if len(to) != 2 || to[0] != (core.NodeID{Node: uint32(f)}) || to[1] != (core.NodeID{Node: uint32(l), Loop: 2}) {
		t.Fatalf("the Txn went to %v; the leader is %d on loop 2", to, l)
	}
	if len(w.out.Answers) != 1 || w.out.Answers[0].Token != tok || w.out.Answers[0].Err != nil || w.out.Answers[0].Result.FailedCheck != -1 {
		t.Fatalf("the client answered %+v", w.out.Answers)
	}
	tok2, err := w.cl.Txn(&w.ctx, &Txn{Ops: []Op{Put(attr(1, 2, 2), b("v"))}}, &w.out)
	if err != nil || tok2 == tok {
		t.Fatalf("second token %d after %d, %v", tok2, tok, err)
	}
	if w.ctx.Pool.(*sim.Pool).Live() != 0 {
		t.Fatal("the client holds a buffer")
	}
}

// A Read sent to a follower is answered NotLeader, resent to the leader and answered there from its
// state under its lease; a leader without its lease refuses it NotLeader naming none, and the
// client holds it for an election timeout before it tries a voter again.
func TestAReadCrossesTheWireToTheLeaderUnderItsLease(t *testing.T) {
	w := newWired(t, 0)
	w.c.renewLeases()
	l := w.c.elect()
	w.c.heartbeat()
	f := (l + 1) % 3
	m := oneShard(f)
	if err := w.cl.R.SetMap(&m, &w.out); err != nil {
		t.Fatal(err)
	}
	w.c.txn(l, Txn{Ops: []Op{Put(attr(1, 2, 1), b("v"))}}, 1)
	w.c.settle()
	ctx := w.c.ctx(l)
	if !w.c.n[l].s.Raft().LeaseRead(&ctx) {
		t.Fatal("the leader holds no lease")
	}
	rd := Read{Start: attr(1, 2, 1), End: attr(1, 2, 2), Limit: 5}
	tok, err := w.cl.Read(&w.ctx, &rd, &w.out)
	if err != nil {
		t.Fatal(err)
	}
	to := w.run(t)
	if len(to) != 2 || to[0].Node != uint32(f) || to[1].Node != uint32(l) {
		t.Fatalf("the Read went to %v; the leader is %d", to, l)
	}
	if len(w.out.Reads) != 1 || w.out.Reads[0].Token != tok || w.out.Reads[0].Err != nil || len(w.out.Reads[0].KVs) != 1 ||
		!bytes.Equal(w.out.Reads[0].KVs[0].Value, b("v")) {
		t.Fatalf("the client answered %+v", w.out.Reads)
	}
	w.out.Reset()
	w.c.cut[l] = true
	for range 2 * lease.DefaultConfig.Lease / raftshard.TickInterval {
		w.c.tick()
	}
	w.c.renewLeases()
	w.c.heartbeat()
	w.c.cut[l] = false
	ctx = w.c.ctx(l)
	if w.c.n[l].s.Raft().Leader() != uint64(l)+1 || w.c.n[l].s.Raft().LeaseRead(&ctx) {
		t.Fatal("the cut-off leader does not lead without its lease")
	}
	if _, err := w.cl.Read(&w.ctx, &rd, &w.out); err != nil {
		t.Fatal(err)
	}
	if to := w.run(t); len(to) != 1 || to[0].Node != uint32(l) || len(w.out.Reads) != 0 || !w.timer {
		t.Fatalf("sent to %v, answered %+v; timer set %v", to, w.out.Reads, w.timer)
	}
	for range 3 {
		if len(w.out.Reads) == 0 {
			w.cl.OnEvent(&w.ctx, &core.Event{Kind: core.EvTimer, Timer: 1}, &w.out)
			w.run(t)
		}
	}
	if len(w.out.Reads) != 1 || w.out.Reads[0].Err != nil || len(w.out.Reads[0].KVs) != 1 || w.ctx.Pool.(*sim.Pool).Live() != 0 {
		t.Fatalf("after the election wait the client answered %+v", w.out.Reads)
	}
}

// A node with no replica of the group answers NotLeader naming none, and a body that does not
// decode is answered ErrMalformed: every Txn the server hears gets an answer.
func TestEveryTxnTheServerHearsIsAnswered(t *testing.T) {
	pool := sim.NewPool(1<<20, 0)
	ctx := core.Context{Pool: pool}
	sv := NewServer()
	peer := core.NodeID{Node: 3, Loop: 1}
	tx := Txn{Ops: []Op{Put(attr(1, 2, 1), b("v"))}}
	ev := core.Event{Kind: core.EvNetRecv, Peer: peer}
	if err := EncodeTxn(pool, dataGroup, 4, &tx, &ev.Msg); err != nil {
		t.Fatal(err)
	}
	bad := ev
	bad.Msg.Body[msgHead] = 0x7f
	var a Answer
	for _, c := range []struct {
		ev   core.Event
		want error
	}{{ev, NotLeader{Group: dataGroup}}, {bad, ErrMalformed}} {
		acts := sv.Serve(&ctx, &c.ev, nil, nil, nil)
		if len(acts) != 1 || acts[0].Peer != peer || DecodeAnswer(pool, &acts[0].Msg, &a) != nil || a.Token != 4 || a.Err != c.want {
			t.Fatalf("answered %+v, want %v", a, c.want)
		}
	}
}

// A lost peer fails the client's Txn sent to it as dropped, and the client sends to the loop
// SetLoop names.
func TestAClientTxnToALostPeerIsDropped(t *testing.T) {
	m := oneShard(1)
	r, err := NewRouter(&m, 1)
	if err != nil {
		t.Fatal(err)
	}
	cl := NewClient(r, 1)
	cl.SetLoop(1, dataGroup, 6)
	ctx := core.Context{Pool: sim.NewPool(1<<20, 0)}
	var out RouterOutput
	tok, err := cl.Txn(&ctx, &Txn{Ops: []Op{Put(attr(1, 2, 1), b("v"))}}, &out)
	if err != nil || len(out.Actions) != 1 || out.Actions[0].Peer != (core.NodeID{Node: 1, Loop: 6}) {
		t.Fatalf("sent %+v, %v", out.Actions, err)
	}
	cl.OnEvent(&ctx, &core.Event{Kind: core.EvPeerLost, Peer: core.NodeID{Node: 1, Loop: 6}}, &out)
	if len(out.Answers) != 1 || out.Answers[0].Token != tok || out.Answers[0].Err != raft.ErrProposalDropped {
		t.Fatalf("answered %+v", out.Answers)
	}
}

// The client routes by the shard map the control-plane group committed under ctl/.
func TestTheClientRoutesByTheShardMapUnderCtl(t *testing.T) {
	r := newCtlRun(t)
	if err := r.ctl.SetShardMap(twoGroups()); err != nil {
		t.Fatal(err)
	}
	r.step(&ControlInputs{})
	m := oneShard(0)
	rt, err := NewRouter(&m, 1)
	if err != nil {
		t.Fatal(err)
	}
	cl := NewClient(rt, 1)
	var out RouterOutput
	if err := cl.SetMap(r.c.n[(r.l+1)%3].s.State(), &out); err != nil {
		t.Fatal(err)
	}
	if g, err := cl.R.m.Route(&Txn{Ops: []Op{Put(attr(1, 2, 1), b("v"))}}); err != nil || g != 2 {
		t.Fatalf("routed to %d, %v", g, err)
	}
}

// An answer that promises nothing about whether its Txn applied, as an error with no code of its
// own, an answer to an applied Txn the server's pool had no buffer for, one whose body does not
// decode, or a lost peer, settles the client's Txn as dropped and is never resent, so no Txn
// applies twice.
func TestAnAnswerOfUnknownOutcomeIsNeverResent(t *testing.T) {
	for _, c := range []string{"no code", "no buffer", "no decode", "peer lost"} {
		m := oneShard(1)
		r, err := NewRouter(&m, 1)
		if err != nil {
			t.Fatal(err)
		}
		cl := NewClient(r, 1)
		ctx := core.Context{Pool: sim.NewPool(1<<20, 0)}
		var out RouterOutput
		tok, err := cl.Txn(&ctx, &Txn{Ops: []Op{Put(attr(1, 2, 1), b("v"))}}, &out)
		if err != nil || len(out.Actions) != 1 || out.Actions[0].Msg.Buf != core.NoBuf {
			t.Fatalf("sent %+v, %v", out.Actions, err)
		}
		ev := core.Event{Kind: core.EvNetRecv, Peer: out.Actions[0].Peer}
		out.Actions = nil
		switch c {
		case "no code":
			err = EncodeAnswer(ctx.Pool, dataGroup, &Answer{Token: tok, Result: Result{FailedCheck: -1}, Err: errors.New("other")}, &ev.Msg)
		case "no buffer":
			full := sim.NewPool(1<<20, 1)
			if _, err = full.Alloc(1); err != nil {
				t.Fatal(err)
			}
			sv := NewServer()
			sv.waits[ServerToken|1] = wait{group: dataGroup, peer: core.NodeID{Node: 3}, token: tok}
			applied := Answer{Token: ServerToken | 1, Result: Result{FailedCheck: 0, Actual: bytes.Repeat(b("x"), 1000)}}
			acts := sv.Reply(&core.Context{Pool: full}, []Answer{applied}, nil)
			if len(acts) != 1 || acts[0].Msg.Buf != core.NoBuf {
				t.Fatalf("replied %+v", acts)
			}
			ev.Msg = acts[0].Msg
		case "no decode":
			err = EncodeAnswer(ctx.Pool, dataGroup, &Answer{Token: tok, Result: Result{FailedCheck: -1}}, &ev.Msg)
			ev.Msg.Body[msgHead] = 0xff
		case "peer lost":
			ev = core.Event{Kind: core.EvPeerLost, Peer: ev.Peer}
		}
		if err != nil {
			t.Fatal(err)
		}
		cl.OnEvent(&ctx, &ev, &out)
		if len(out.Actions) != 0 || len(out.Answers) != 1 || out.Answers[0].Token != tok || out.Answers[0].Err != raft.ErrProposalDropped {
			t.Fatalf("%s: sent %+v, answered %+v", c, out.Actions, out.Answers)
		}
	}
}

// A shard refuses a token with ServerToken set from any proposer but a Server.
func TestOnlyAServerProposesUnderAServerToken(t *testing.T) {
	c := newGroup(t, 3, false)
	l := c.elect()
	var err error
	c.call(l, func(ctx *core.Context, o *Output) {
		err = c.n[l].s.Txn(ctx, &Txn{Ops: []Op{Put(attr(1, 2, 1), b("v"))}}, ServerToken|1, o)
	})
	if err != ErrServerToken {
		t.Fatalf("a Txn under a server token: %v", err)
	}
}

// A restarted client's tokens differ from its earlier start's, so an answer to a Txn the earlier
// start sent cannot settle one the new start sent.
func TestARestartedClientIgnoresItsEarlierStartsAnswers(t *testing.T) {
	ctx := core.Context{Pool: sim.NewPool(1<<20, 0)}
	var toks [2]uint64
	var cl *Client
	for start := range toks {
		m := oneShard(1)
		r, err := NewRouter(&m, 1)
		if err != nil {
			t.Fatal(err)
		}
		cl = NewClient(r, uint64(start)+7)
		var out RouterOutput
		if toks[start], err = cl.Txn(&ctx, &Txn{Ops: []Op{Put(attr(1, 2, 1), b("v"))}}, &out); err != nil {
			t.Fatal(err)
		}
		if toks[start]&ServerToken != 0 {
			t.Fatalf("token %x has ServerToken set", toks[start])
		}
	}
	var out RouterOutput
	ev := core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{Node: 1}}
	if err := EncodeAnswer(ctx.Pool, dataGroup, &Answer{Token: toks[0], Result: Result{FailedCheck: 0}}, &ev.Msg); err != nil {
		t.Fatal(err)
	}
	cl.OnEvent(&ctx, &ev, &out)
	if toks[0] == toks[1] || len(out.Answers) != 0 {
		t.Fatalf("tokens %x and %x; the new start took %+v", toks[0], toks[1], out.Answers)
	}
}

func FuzzDecodeReclaim(f *testing.F) {
	fuzzNodeWire(f, func(pool core.BufPool, m *core.Msg) { _ = EncodeReclaim(pool, 5, 7, 1<<40, m) }, func(pool core.BufPool, m *core.Msg) (core.Msg, bool) {
		var again core.Msg
		g, s, term, err := DecodeReclaim(pool, m)
		if err != nil {
			return again, false
		}
		if err := EncodeReclaim(pool, g, s, term, &again); err != nil {
			panic(err)
		}
		return again, true
	})
}
