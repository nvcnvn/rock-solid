package node

import (
	"testing"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/meta"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

var ctl = core.NodeID{Node: 9, Loop: 1}

const group = 1

type run struct {
	t   *testing.T
	n   *Node
	ctx core.Context
	out Output
}

func newRun(t *testing.T, seed uint64, devices ...uint64) *run {
	r := &run{t: t, n: New(Config{Node: 4, Release: 1, Ctls: []core.NodeID{ctl}, Group: group, Seed: seed}), ctx: core.Context{Now: 1, Pool: sim.NewPool(1<<20, 0)}}
	for _, d := range devices {
		r.n.Opened(meta.OpenedDisk{Device: d, Log: d << 8}, &r.out)
	}
	return r
}

// sends are the messages to the control plane of kind in the last call's output, their buffers freed.
func (r *run) sends(kind byte) []core.Msg {
	var ms []core.Msg
	for _, a := range r.out.Actions {
		if a.Kind != core.ActNetSend {
			continue
		}
		if a.Peer != ctl {
			r.t.Fatalf("sent to %v", a.Peer)
		}
		if _, k, _, _ := meta.Peek(&a.Msg); k == kind {
			ms = append(ms, a.Msg)
		} else if a.Msg.Buf != core.NoBuf {
			r.ctx.Pool.Free(a.Msg.Buf)
		}
	}
	return ms
}

// fetch is the token of the one fetch the last call sent, or 0 if it sent none.
func (r *run) fetch() uint64 {
	ms := r.sends(meta.KindSessionFetch)
	if len(ms) > 1 {
		r.t.Fatalf("%d fetches in one call", len(ms))
	}
	if len(ms) == 0 {
		return 0
	}
	_, _, tok, _ := meta.Peek(&ms[0])
	return tok
}

func (r *run) ask(device, ask uint64) uint64 {
	r.out.Reset()
	r.n.Ask(&r.ctx, device, ask, &r.out)
	return r.fetch()
}

// table hands the node table t answering token, and returns the hands it made by device.
func (r *run) table(token uint64, t chunkstore.SessionTable) map[uint64]Hand {
	ev := core.Event{Kind: core.EvNetRecv, Peer: ctl}
	if err := meta.EncodeSessions(r.ctx.Pool, group, token, &t, &ev.Msg); err != nil {
		r.t.Fatal(err)
	}
	r.out.Reset()
	r.n.OnEvent(&r.ctx, &ev, &r.out)
	hands := map[uint64]Hand{}
	for _, h := range r.out.Hands {
		if _, dup := hands[h.Device]; dup {
			r.t.Fatalf("device %d handed twice", h.Device)
		}
		hands[h.Device] = h
	}
	return hands
}

func (r *run) event(ev core.Event) {
	r.out.Reset()
	r.n.OnEvent(&r.ctx, &ev, &r.out)
}

// report is the NodeReport of a heartbeat at the current time.
func (r *run) report() meta.NodeReport {
	r.event(core.Event{Kind: core.EvTimer, Timer: HeartbeatTimer})
	ms := r.sends(meta.KindNodeReport)
	if len(ms) != 1 {
		r.t.Fatalf("%d reports on a heartbeat", len(ms))
	}
	var nr meta.NodeReport
	if err := meta.DecodeNodeReport(r.ctx.Pool, &ms[0], &nr); err != nil {
		r.t.Fatal(err)
	}
	if ms[0].Buf != core.NoBuf {
		r.ctx.Pool.Free(ms[0].Buf)
	}
	return nr
}

func TestAPushedTableGoesToEveryReplicaUnasked(t *testing.T) {
	r := newRun(t, 1, 10, 11)
	v2 := chunkstore.SessionTable{Version: 2, Next: 5, Live: []uint64{3}}
	hs := r.table(0, v2)
	if len(hs) != 2 || hs[10].Answers != 0 || hs[11].Answers != 0 || hs[10].Table.Version != 2 {
		t.Fatalf("a push handed %+v", hs)
	}
	if hs := r.table(0, v2); len(hs) != 0 {
		t.Fatalf("the same table again handed %+v", hs)
	}
	if hs := r.table(0, chunkstore.SessionTable{Version: 1, Next: 9}); len(hs) != 0 {
		t.Fatalf("an older table handed %+v", hs)
	}
	if hs := r.table(0, chunkstore.SessionTable{Version: 2, Next: 6, Live: []uint64{3, 5}}); len(hs) != 2 {
		t.Fatalf("a table with a later start handed %+v", hs)
	}
	r.out.Reset()
	r.n.Opened(meta.OpenedDisk{Device: 12}, &r.out)
	if len(r.out.Hands) != 1 || r.out.Hands[0].Table.Next != 6 || r.out.Hands[0].Answers != 0 {
		t.Fatalf("a replica opened later got %+v", r.out.Hands)
	}
}

// A session table from any peer but the control plane is dropped: one with a huge version would
// otherwise pin the node's table and every store's, since no real table would count as newer.
func TestATableFromAnotherPeerIsDropped(t *testing.T) {
	r := newRun(t, 1, 10)
	tok := r.ask(10, 1)
	ev := core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{Node: 3}}
	if err := meta.EncodeSessions(r.ctx.Pool, group, tok, &chunkstore.SessionTable{Version: 1 << 60, Next: 1 << 60}, &ev.Msg); err != nil {
		t.Fatal(err)
	}
	r.event(ev)
	if len(r.out.Hands) != 0 {
		t.Fatalf("another peer's table handed %+v", r.out.Hands)
	}
	if hs := r.table(tok, chunkstore.SessionTable{Version: 2, Next: 5}); hs[10].Answers != 1 || hs[10].Table.Version != 2 {
		t.Fatalf("the control plane's answer handed %+v", hs)
	}
}

func TestTheHeartbeatReportsTheVersionEveryReplicaHolds(t *testing.T) {
	r := newRun(t, 1, 10, 11)
	r.table(0, chunkstore.SessionTable{Version: 2, Next: 5})
	if nr := r.report(); nr.Sessions != 0 || nr.Node != 4 || nr.Release != 1 || len(nr.Opened) != 2 {
		t.Fatalf("a table no replica holds yet reported as %+v", nr)
	}
	r.n.Held(10, 2)
	if nr := r.report(); nr.Sessions != 0 {
		t.Fatalf("a table one replica of two holds reported as version %d", nr.Sessions)
	}
	r.n.Held(11, 2)
	if nr := r.report(); nr.Sessions != 2 {
		t.Fatalf("a table every replica holds reported as version %d", nr.Sessions)
	}
	r.table(0, chunkstore.SessionTable{Version: 3, Next: 5})
	r.n.Held(10, 3)
	if nr := r.report(); nr.Sessions != 2 {
		t.Fatalf("version 3 held by one replica reported as %d", nr.Sessions)
	}
	if a := r.out.Actions[len(r.out.Actions)-1]; a.Kind != core.ActSetTimer || a.Timer != HeartbeatTimer || a.At != r.ctx.Now+Interval {
		t.Fatalf("the heartbeat arms %+v", a)
	}

	bare := newRun(t, 1)
	bare.table(0, chunkstore.SessionTable{Version: 4, Next: 5})
	if nr := bare.report(); nr.Sessions != 4 {
		t.Fatalf("a node with no replica reports version %d", nr.Sessions)
	}
}

func TestAnAnswerStandsOnlyForTheAsksMadeBeforeItsFetch(t *testing.T) {
	r := newRun(t, 1, 10, 11)
	tok := r.ask(10, 1)
	if tok == 0 {
		t.Fatal("an ask sent no fetch")
	}
	if r.ask(11, 1) != 0 || r.ask(10, 2) != 0 {
		t.Fatal("asks while a fetch is in flight sent another")
	}
	st := chunkstore.SessionTable{Version: 1, Next: 8, Live: []uint64{7}}
	hs := r.table(tok, st)
	if hs[10].Answers != 1 || hs[11].Answers != 0 || len(hs) != 2 {
		t.Fatalf("the answer handed %+v, the asks after its fetch answered", hs)
	}
	again := r.fetch()
	if again == 0 || again == tok {
		t.Fatalf("asks after the fetch fetched again under %#x", again)
	}
	hs = r.table(again, st)
	if hs[10].Answers != 2 || hs[11].Answers != 1 {
		t.Fatalf("the second answer handed %+v", hs)
	}
	if r.fetch() != 0 {
		t.Fatal("a fetch with every ask answered")
	}
	if r.ask(10, 2) != 0 {
		t.Fatal("an ask answered already fetched again")
	}
}

// A fetch unanswered after Interval goes again under a new token. A lost connection sends nothing:
// while the driver holds the control plane in backoff, every send fails at once with another loss,
// and a resend on each would spin the loop.
func TestAnUnansweredFetchIsSentAgainOnlyByItsTimer(t *testing.T) {
	r := newRun(t, 1, 10)
	first := r.ask(10, 1)
	for range 100 {
		r.event(core.Event{Kind: core.EvPeerLost, Peer: ctl})
		if r.fetch() != 0 {
			t.Fatal("a lost connection fetched again")
		}
	}
	r.ctx.Now += Interval - 1
	r.event(core.Event{Kind: core.EvTimer, Timer: FetchTimer})
	if r.fetch() != 0 {
		t.Fatal("a fetch sent again before Interval")
	}
	r.ctx.Now++
	r.event(core.Event{Kind: core.EvTimer, Timer: FetchTimer})
	second := r.fetch()
	if second == 0 || second == first {
		t.Fatalf("an unanswered fetch fetched again under %#x", second)
	}
	if hs := r.table(first, chunkstore.SessionTable{Version: 1, Next: 3}); hs[10].Answers != 0 {
		t.Fatalf("the earlier fetch's late answer handed %+v", hs)
	}
	if hs := r.table(second, chunkstore.SessionTable{Version: 1, Next: 3}); hs[10].Answers != 1 {
		t.Fatalf("the answer handed %+v", hs)
	}
}

func TestAnEarlierStartsAnswerIsNotTakenForAFetch(t *testing.T) {
	before, after := newRun(t, 1, 10), newRun(t, 2, 10)
	tok := before.ask(10, 1)
	if got := after.ask(10, 1); got == tok {
		t.Fatalf("two starts fetch under the same token %#x", tok)
	}
	if hs := after.table(tok, chunkstore.SessionTable{Version: 1, Next: 3}); hs[10].Answers != 0 {
		t.Fatalf("an earlier start's answer handed %+v", hs)
	}
}

func TestTheStartReportsTheDevicesOpenedAndFailed(t *testing.T) {
	r := newRun(t, 1)
	if s, _ := r.n.Recovered(1, nil); s != chunkstore.StartOpened {
		t.Fatalf("a recovered device: %v", s)
	}
	s, log := r.n.Recovered(2, disklog.ErrNotFormatted)
	if s != chunkstore.StartFormat || log == 0 {
		t.Fatalf("an unformatted device: %v under %#x", s, log)
	}
	if _, other := newRun(t, 2).n.Recovered(2, disklog.ErrNotFormatted); other == log {
		t.Fatalf("two starts format under the same log ID %#x", log)
	}
	if s, _ := r.n.Recovered(3, disklog.ErrCorrupt); s != chunkstore.StartFailed {
		t.Fatalf("a corrupt device: %v", s)
	}
	r.n.Failed(4)
	r.n.Opened(meta.OpenedDisk{Device: 1, Log: 0x1a}, &r.out)
	r.n.Opened(meta.OpenedDisk{Device: 2, Log: log}, &r.out)
	r.out.Reset()
	r.n.Start(&r.ctx, &r.out)
	ms := r.sends(meta.KindNodeReport)
	var nr meta.NodeReport
	if len(ms) != 1 || meta.DecodeNodeReport(r.ctx.Pool, &ms[0], &nr) != nil {
		t.Fatalf("the start sent %d reports", len(ms))
	}
	if len(nr.Opened) != 2 || nr.Opened[1].Log != log || len(nr.Failed) != 2 || nr.Failed[0] != 3 || nr.Failed[1] != 4 {
		t.Fatalf("the start reported %+v", nr)
	}
}

var other = core.NodeID{Node: 8, Loop: 2}

// three is a node configured with three voters, ctl first.
func three(t *testing.T) *run {
	r := newRun(t, 1, 1)
	r.n.cfg.Ctls = []core.NodeID{ctl, other, {Node: 7}}
	return r
}

func (r *run) from(peer core.NodeID, encode func(pool core.BufPool, m *core.Msg) error) {
	r.t.Helper()
	ev := core.Event{Kind: core.EvNetRecv, Peer: peer}
	if err := encode(r.ctx.Pool, &ev.Msg); err != nil {
		r.t.Fatal(err)
	}
	r.event(ev)
}

func leader(at core.NodeID, known bool) func(pool core.BufPool, m *core.Msg) error {
	return func(pool core.BufPool, m *core.Msg) error { return meta.EncodeLeader(pool, group, at, known, m) }
}

func TestANodeMovesToTheLeaderItsVoterNames(t *testing.T) {
	r := three(t)
	r.from(other, leader(core.NodeID{Node: 7}, true))
	if r.out.Moved || r.n.Ctl() != ctl {
		t.Fatal("moved on a leader named by a voter the node does not talk to")
	}
	r.from(ctl, leader(other, true))
	if !r.out.Moved || r.n.Ctl() != other {
		t.Fatalf("talks to %v after its voter named %v", r.n.Ctl(), other)
	}
	r.from(other, leader(core.NodeID{}, false))
	if !r.out.Moved || r.n.Ctl() != (core.NodeID{Node: 7}) {
		t.Fatalf("talks to %v after its voter knew no leader", r.n.Ctl())
	}
	r.from(core.NodeID{Node: 7}, leader(core.NodeID{Node: 5}, true))
	if r.n.Ctl() != ctl {
		t.Fatalf("talks to %v after its voter named a node that is no voter", r.n.Ctl())
	}
	r.event(core.Event{Kind: core.EvPeerLost, Peer: ctl})
	if !r.out.Moved || r.n.Ctl() != other {
		t.Fatalf("talks to %v after losing %v", r.n.Ctl(), ctl)
	}
	r.event(core.Event{Kind: core.EvTimer, Timer: HeartbeatTimer})
	if len(r.out.Actions) == 0 || r.out.Actions[0].Peer != other {
		t.Fatalf("heartbeat went to %+v", r.out.Actions)
	}
	free(&r.ctx, &r.out.Actions[0].Msg)
}

func TestAPlacementTableFromAVoterIsHandedOn(t *testing.T) {
	r := three(t)
	tbl := chunkstore.Table{Epoch: 4}
	r.from(core.NodeID{Node: 5}, func(pool core.BufPool, m *core.Msg) error { return meta.EncodeTable(pool, group, &tbl, m) })
	if r.out.Table != nil {
		t.Fatal("took a table from a node that is no voter")
	}
	r.from(other, func(pool core.BufPool, m *core.Msg) error { return meta.EncodeTable(pool, group, &tbl, m) })
	if r.out.Table == nil || r.out.Table.Epoch != 4 {
		t.Fatalf("handed %+v", r.out.Table)
	}
	d := chunkstore.Delta{Epoch: 5}
	r.from(ctl, func(pool core.BufPool, m *core.Msg) error { return meta.EncodeDelta(pool, group, &d, m) })
	if r.out.Delta == nil || r.out.Delta.Epoch != 5 || r.out.Table != nil {
		t.Fatalf("handed %+v %+v", r.out.Delta, r.out.Table)
	}
	if r.ctx.Pool.(*sim.Pool).Live() != 0 {
		t.Fatal("a buffer left")
	}
}

func TestATableFetchGoesOutOnceAnIntervalUntilATableArrives(t *testing.T) {
	r := newRun(t, 1, 1)
	fetches := func() int {
		r.out.Reset()
		r.n.FetchTable(&r.ctx, &r.out)
		return len(r.sends(meta.KindTableFetch))
	}
	if n := fetches() + fetches(); n != 1 {
		t.Fatalf("two asks in a pass sent %d fetches", n)
	}
	r.ctx.Now += Interval
	if fetches() != 1 {
		t.Fatal("no fetch an interval later")
	}
	tbl := chunkstore.Table{Epoch: 4}
	r.from(ctl, func(pool core.BufPool, m *core.Msg) error { return meta.EncodeTable(pool, group, &tbl, m) })
	if fetches() != 1 {
		t.Fatal("no fetch at once after a table arrived")
	}
}
