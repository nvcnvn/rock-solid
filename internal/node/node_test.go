package node

import (
	"slices"
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
	var ds []meta.DiskAddr
	for i, d := range devices {
		r.n.Opened(meta.OpenedDisk{Device: d, Log: d << 8}, &r.out)
		ds = append(ds, meta.DiskAddr{Disk: uint32(i + 1), Log: d << 8})
	}
	if len(ds) != 0 {
		r.from(ctl, func(pool core.BufPool, m *core.Msg) error {
			return meta.EncodeDisks(pool, group, &meta.Membership{Disks: ds}, m)
		})
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

// A node holds metadata whatever its role, so its heartbeat lets the leader place voters on its
// disks; a role meta node also has its disks drained.
func TestTheHeartbeatReportsTheNodesRole(t *testing.T) {
	if nr := newRun(t, 1, 10).report(); !nr.Meta || nr.MetaOnly {
		t.Fatalf("a storage node reported %+v", nr)
	}
	r := newRun(t, 1, 10)
	r.n.cfg.MetaOnly = true
	if nr := r.report(); !nr.Meta || !nr.MetaOnly {
		t.Fatalf("a role meta node reported %+v", nr)
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
	r.ctx.Now += Interval
	r.from(other, leader(core.NodeID{}, false))
	if !r.out.Moved || r.n.Ctl() != (core.NodeID{Node: 7}) {
		t.Fatalf("talks to %v after its voter knew no leader", r.n.Ctl())
	}
	r.ctx.Now += Interval
	r.from(core.NodeID{Node: 7}, leader(core.NodeID{Node: 5}, true))
	if r.n.Ctl() != ctl {
		t.Fatalf("talks to %v after its voter named a node that is no voter", r.n.Ctl())
	}
	r.ctx.Now += Interval
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

// A node fetches the placement table at its start: its replicas start with none, and a leader that
// sent the node the whole table before a restart within node_down_after sends it only deltas.
func TestAStartFetchesThePlacementTable(t *testing.T) {
	r := newRun(t, 1)
	r.out.Reset()
	r.n.Start(&r.ctx, &r.out)
	if n := len(r.sends(meta.KindTableFetch)); n != 1 {
		t.Fatalf("the start sent %d table fetches", n)
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

// With one voter the address never moves: a move re-points every replica, each sending a report
// at once, and while the driver backs off each send fails with another loss.
func TestALostSoleVoterMovesNothing(t *testing.T) {
	r := newRun(t, 1, 1)
	for range 5 {
		r.event(core.Event{Kind: core.EvPeerLost, Peer: ctl})
		if r.out.Moved {
			t.Fatal("a loss of the only voter moved the node")
		}
	}
	r.from(ctl, leader(core.NodeID{}, false))
	if r.out.Moved {
		t.Fatal("the only voter knowing no leader moved the node")
	}
}

func TestANodeMovesAtMostOnceAnInterval(t *testing.T) {
	r := three(t)
	moves := 0
	for range 9 {
		r.event(core.Event{Kind: core.EvPeerLost, Peer: r.n.Ctl()})
		if r.out.Moved {
			moves++
		}
		r.from(r.n.Ctl(), leader(core.NodeID{}, false))
		if r.out.Moved {
			moves++
		}
	}
	if moves != 1 || r.n.Ctl() != other {
		t.Fatalf("losses and redirects in one interval moved %d times, to %v", moves, r.n.Ctl())
	}
	r.ctx.Now += Interval
	r.event(core.Event{Kind: core.EvPeerLost, Peer: other})
	if !r.out.Moved || r.n.Ctl() != (core.NodeID{Node: 7}) {
		t.Fatalf("talks to %v an interval later", r.n.Ctl())
	}
	r.ctx.Now += Interval
	r.from(r.n.Ctl(), leader(r.n.Ctl(), true))
	if r.out.Moved {
		t.Fatal("a voter naming itself moved the node")
	}
}

// The heartbeat reports the oldest placement epoch the replicas hold, so after a failover the
// leader resends the whole table only to a node behind it (Control.Heartbeat).
func TestTheHeartbeatReportsTheEpochEveryReplicaHolds(t *testing.T) {
	r := newRun(t, 1, 10, 11)
	tbl := chunkstore.Table{Epoch: 5}
	r.from(ctl, func(pool core.BufPool, m *core.Msg) error { return meta.EncodeTable(pool, group, &tbl, m) })
	if nr := r.report(); nr.Epoch != 0 {
		t.Fatalf("a table no replica holds yet reported as epoch %d", nr.Epoch)
	}
	r.n.HeldEpoch(10, 5)
	r.n.HeldEpoch(11, 4)
	if nr := r.report(); nr.Epoch != 4 {
		t.Fatalf("epochs 5 and 4 held reported as %d", nr.Epoch)
	}
	r.n.HeldEpoch(11, 5)
	r.n.HeldEpoch(11, 3)
	if nr := r.report(); nr.Epoch != 5 {
		t.Fatalf("epoch 5 held by every replica reported as %d", nr.Epoch)
	}

	bare := newRun(t, 1)
	d := chunkstore.Delta{Epoch: 6}
	bare.from(ctl, func(pool core.BufPool, m *core.Msg) error { return meta.EncodeDelta(pool, group, &d, m) })
	if nr := bare.report(); nr.Epoch != 6 {
		t.Fatalf("a node with no replica reports epoch %d", nr.Epoch)
	}
}

// A fetch in flight when the node moves goes to the new voter at once under a new token, rather
// than waiting out FetchTimer; moves come at most once an Interval, so this sends at most that often.
func TestAMoveSendsTheFetchInFlightToTheNewVoter(t *testing.T) {
	r := three(t)
	first := r.ask(1, 1)
	r.from(ctl, leader(other, true))
	var fetches []core.Action
	for _, a := range r.out.Actions {
		if _, k, _, _ := meta.Peek(&a.Msg); a.Kind == core.ActNetSend && k == meta.KindSessionFetch {
			fetches = append(fetches, a)
		}
	}
	if len(fetches) != 1 || fetches[0].Peer != other {
		t.Fatalf("a move sent fetches %+v", fetches)
	}
	if _, _, tok, _ := meta.Peek(&fetches[0].Msg); tok == first {
		t.Fatalf("the fetch went again under its old token %#x", tok)
	}
}

// A table fetch whose answer gets lost goes again on the heartbeat and on a move, until a table
// arrives; then neither sends it.
func TestALostTableFetchGoesAgainUntilATableArrives(t *testing.T) {
	r := three(t)
	r.out.Reset()
	r.n.Start(&r.ctx, &r.out)
	r.sends(meta.KindTableFetch)
	tableFetches := func() []core.NodeID {
		var to []core.NodeID
		for _, a := range r.out.Actions {
			if _, k, _, _ := meta.Peek(&a.Msg); a.Kind == core.ActNetSend && k == meta.KindTableFetch {
				to = append(to, a.Peer)
			}
			free(&r.ctx, &a.Msg)
		}
		return to
	}
	r.ctx.Now += Interval
	r.event(core.Event{Kind: core.EvTimer, Timer: HeartbeatTimer})
	if to := tableFetches(); len(to) != 1 || to[0] != ctl {
		t.Fatalf("the heartbeat after a lost table fetch sent it to %v", to)
	}
	r.from(ctl, leader(other, true))
	if to := tableFetches(); len(to) != 1 || to[0] != other {
		t.Fatalf("a move with no table sent the fetch to %v", to)
	}
	tbl := chunkstore.Table{Epoch: 4}
	r.from(other, func(pool core.BufPool, m *core.Msg) error { return meta.EncodeTable(pool, group, &tbl, m) })
	r.ctx.Now += Interval
	r.event(core.Event{Kind: core.EvTimer, Timer: HeartbeatTimer})
	if to := tableFetches(); len(to) != 0 {
		t.Fatalf("the heartbeat after a table sent a fetch to %v", to)
	}
	r.ctx.Now += Interval
	r.from(other, leader(ctl, true))
	if to := tableFetches(); len(to) != 0 {
		t.Fatalf("a move after a table sent a fetch to %v", to)
	}
}

// A delta ends a lost table fetch's resend as a table does: the replicas take it or, before one
// runs, fetch the table themselves.
func TestADeltaEndsTheTableFetchResend(t *testing.T) {
	r := three(t)
	r.out.Reset()
	r.n.Start(&r.ctx, &r.out)
	r.sends(meta.KindTableFetch)
	dl := chunkstore.Delta{Epoch: 2}
	r.from(ctl, func(pool core.BufPool, m *core.Msg) error { return meta.EncodeDelta(pool, group, &dl, m) })
	r.ctx.Now += Interval
	r.event(core.Event{Kind: core.EvTimer, Timer: HeartbeatTimer})
	if len(r.sends(meta.KindTableFetch)) != 0 {
		t.Fatal("the heartbeat after a delta fetched the table")
	}
}

// A replica removed, as a device opened again in the same process, gets no more tables and leaves
// the report, and the device opened again is reported once; a replica not yet removed, whatever
// its store, keeps getting tables and its version pins the report.
func TestARemovedReplicaGetsNoTableAndLeavesTheReport(t *testing.T) {
	r := newRun(t, 1, 10, 11)
	r.table(0, chunkstore.SessionTable{Version: 2, Next: 5})
	r.n.Held(10, 2)
	if hs := r.table(0, chunkstore.SessionTable{Version: 3, Next: 5}); len(hs) != 2 {
		t.Fatalf("a table before the removal handed %+v", hs)
	}
	if nr := r.report(); nr.Sessions != 0 || len(nr.Opened) != 2 {
		t.Fatalf("a replica holding nothing yet reported as %+v", nr)
	}
	r.n.Remove(11)
	if hs := r.table(0, chunkstore.SessionTable{Version: 4, Next: 5}); len(hs) != 1 || hs[10].Table.Version != 4 {
		t.Fatalf("a table after the removal handed %+v", hs)
	}
	r.n.Held(11, 4)
	if nr := r.report(); nr.Sessions != 2 || len(nr.Opened) != 1 || nr.Opened[0].Device != 10 {
		t.Fatalf("after the removal reported %+v", nr)
	}
	r.out.Reset()
	r.n.Opened(meta.OpenedDisk{Device: 11, Log: 0x99}, &r.out)
	if nr := r.report(); len(nr.Opened) != 2 || nr.Opened[1].Log != 0x99 || nr.Sessions != 2 {
		t.Fatalf("the device opened again, its log not yet named, reported %+v", nr)
	}
	ds := []meta.DiskAddr{{Disk: 1, Log: 10 << 8}, {Disk: 3, Log: 0x99}}
	r.from(ctl, func(pool core.BufPool, m *core.Msg) error {
		return meta.EncodeDisks(pool, group, &meta.Membership{Disks: ds}, m)
	})
	if nr := r.report(); nr.Sessions != 0 {
		t.Fatalf("the device opened again, its log named, reported %+v", nr)
	}
}

func TestTheDiskMembershipIsHandedOn(t *testing.T) {
	r := newRun(t, 1, 10)
	ds := []meta.DiskAddr{{Disk: 1, Log: 10 << 8, At: core.NodeID{Node: 4, Loop: 2}}, {Disk: 2, Log: 7, At: core.NodeID{Node: 5}}}
	r.from(core.NodeID{Node: 5}, func(pool core.BufPool, m *core.Msg) error {
		return meta.EncodeDisks(pool, group, &meta.Membership{Disks: ds}, m)
	})
	if r.out.Disks != nil {
		t.Fatal("took a membership from a node that is no voter")
	}
	r.from(ctl, func(pool core.BufPool, m *core.Msg) error {
		return meta.EncodeDisks(pool, group, &meta.Membership{Disks: ds}, m)
	})
	if !slices.Equal(r.out.Disks, ds) {
		t.Fatalf("handed %+v", r.out.Disks)
	}
	if r.ctx.Pool.(*sim.Pool).Live() != 0 {
		t.Fatal("a buffer left")
	}
}

// The heartbeat carries the metadata replicas each device last reported, and a delete from the
// voter the node talks to reaches the loop only for a replica that report showed had learned its
// removal.
func TestADeleteReachesOnlyAReplicaThatLearnedItsRemoval(t *testing.T) {
	r := newRun(t, 1)
	reps := []meta.ReplicaReport{{Group: 5, ID: 3, ConfIndex: 4, Joint: true, Removed: true, Voters: []uint64{1, 2, 4}}, {Group: 5, ID: 7, ConfIndex: 4}, {Group: 6, ID: 3}}
	r.n.Replicas(1, reps[2:])
	r.n.Replicas(2, reps[:1])
	r.n.Replicas(1, reps[1:])
	reps[0].Voters[0] = 9
	if nr := r.report(); len(nr.Replicas) != 3 || nr.Replicas[0].Voters[0] != 1 || !nr.Replicas[0].Removed || nr.Replicas[1].ConfIndex != 4 {
		t.Fatalf("the heartbeat reports %+v", nr.Replicas)
	}
	del := func(pool core.BufPool, m *core.Msg) error {
		return meta.EncodeDelete(pool, group, []meta.ReplicaRef{{Group: 5, ID: 3}, {Group: 5, ID: 7}, {Group: 6, ID: 3}, {Group: 5, ID: 9}}, m)
	}
	r.from(core.NodeID{Node: 5}, del)
	if len(r.out.Deletes) != 0 {
		t.Fatalf("took a delete from a node that is no voter: %v", r.out.Deletes)
	}
	r.from(ctl, del)
	if !slices.Equal(r.out.Deletes, []meta.ReplicaRef{{Group: 5, ID: 3}}) {
		t.Fatalf("handed the loop deletes %v, want replica 3 of group 5 alone", r.out.Deletes)
	}
	if r.ctx.Pool.(*sim.Pool).Live() != 0 {
		t.Fatal("a buffer left")
	}
}

// A membership, even an empty one, is handed on, with its shard map's plans unless an answer
// carried a newer map before, so a stale plan that still names a replica a later map dropped, and
// a delete then removed, never places it again; a clone keeps the plans and the deletes once the next messages overwrite the
// node's buffers.
func TestAPlanOlderThanTheNewestMapIsNotHandedOn(t *testing.T) {
	r := newRun(t, 1)
	plan := func(v uint64, ids ...uint64) func(pool core.BufPool, m *core.Msg) error {
		g := meta.Group{ID: 5, LastID: 4, Boot: 3}
		for _, id := range ids {
			g.Voters = append(g.Voters, meta.Replica{ID: id, Node: 4, Disk: 1})
		}
		return func(pool core.BufPool, m *core.Msg) error {
			return meta.EncodeDisks(pool, group, &meta.Membership{Disks: []meta.DiskAddr{{Disk: 1, Log: 7}}, MapV: v, Plans: []meta.Group{g}}, m)
		}
	}
	r.from(ctl, func(pool core.BufPool, m *core.Msg) error {
		return meta.EncodeDisks(pool, group, &meta.Membership{}, m)
	})
	if c, ok := r.out.Clone(); !ok || !c.Member {
		t.Fatal("an empty membership, the first a node gets before its disks open, was not handed on")
	}
	r.n.Replicas(10, []meta.ReplicaReport{{Group: 5, ID: 3, Removed: true}})
	r.from(ctl, plan(2, 4))
	planned, _ := r.out.Clone()
	r.from(ctl, func(pool core.BufPool, m *core.Msg) error {
		return meta.EncodeDelete(pool, group, []meta.ReplicaRef{{Group: 5, ID: 3}}, m)
	})
	deleted, ok := r.out.Clone()
	r.from(ctl, plan(1, 3))
	if !r.out.Member || r.out.Plans != nil {
		t.Fatalf("an answer at an older map handed the loop plans %+v", r.out.Plans)
	}
	r.from(ctl, plan(2, 4))
	if len(r.out.Plans) != 1 || r.out.Plans[0].Voters[0].ID != 4 {
		t.Fatalf("an answer at the newest map handed the loop plans %+v", r.out.Plans)
	}
	if !ok || !slices.Equal(deleted.Deletes, []meta.ReplicaRef{{Group: 5, ID: 3}}) || len(planned.Plans) != 1 || planned.Plans[0].Voters[0].ID != 4 {
		t.Fatalf("the clones hold deletes %v and plans %+v", deleted.Deletes, planned.Plans)
	}
	if r.ctx.Pool.(*sim.Pool).Live() != 0 {
		t.Fatal("a buffer left")
	}
}

// A clone of the node's output keeps its membership and placement table once the next messages
// overwrite the node's buffers, as the node's loop hands it to the device loops.
func TestACloneOutlivesTheNodesBuffers(t *testing.T) {
	r := newRun(t, 1, 10)
	ds := []meta.DiskAddr{{Disk: 1, Log: 10 << 8, At: core.NodeID{Node: 4, Loop: 2}}}
	r.from(ctl, func(pool core.BufPool, m *core.Msg) error {
		return meta.EncodeDisks(pool, group, &meta.Membership{Disks: ds}, m)
	})
	disks, ok := r.out.Clone()
	pg := chunkstore.PGEntry{Members: []chunkstore.Member{{Disk: 1}}, Interval: 1}
	tbl := chunkstore.Table{Epoch: 4, Pools: []chunkstore.PoolTable{{ID: 1, Replicas: 1, MinLive: 1, PGs: []chunkstore.PGEntry{pg}}}}
	r.from(ctl, func(pool core.BufPool, m *core.Msg) error { return meta.EncodeTable(pool, group, &tbl, m) })
	placed, _ := r.out.Clone()
	ds2 := []meta.DiskAddr{{Disk: 2, Log: 7}}
	r.from(ctl, func(pool core.BufPool, m *core.Msg) error {
		return meta.EncodeDisks(pool, group, &meta.Membership{Disks: ds2}, m)
	})
	tbl.Epoch, tbl.Pools[0].PGs[0].Members[0].Disk = 5, 2
	r.from(ctl, func(pool core.BufPool, m *core.Msg) error { return meta.EncodeTable(pool, group, &tbl, m) })
	if !ok || !slices.Equal(disks.Disks, []meta.DiskAddr{{Disk: 1, Log: 10 << 8, At: core.NodeID{Node: 4, Loop: 2}}}) {
		t.Fatalf("the membership's clone reads %+v", disks.Disks)
	}
	if placed.Table == nil || placed.Table.Epoch != 4 || placed.Table.Pools[0].PGs[0].Members[0].Disk != 1 {
		t.Fatalf("the table's clone reads %+v", placed.Table)
	}
	r.out.Reset()
	if _, ok := r.out.Clone(); ok {
		t.Fatal("an empty output's clone holds something for a device")
	}
}

// What a device's machine outputs reaches the node: its opened disk joins the report, its ask
// fetches the session table, its table fetch fetches the placement table, and the version it
// holds is what the report acknowledges.
func TestADevicesOutputFeedsTheNode(t *testing.T) {
	r := newRun(t, 1)
	r.table(0, chunkstore.SessionTable{Version: 3, Next: 5})
	r.out.Reset()
	r.n.Take(&r.ctx, 10, &DeviceOutput{Opened: &meta.OpenedDisk{Device: 10, Log: 10 << 8}}, &r.out)
	if len(r.out.Hands) != 1 || r.out.Hands[0].Device != 10 || r.out.Hands[0].Table.Version != 3 {
		t.Fatalf("the device opened was handed %+v", r.out.Hands)
	}
	r.from(ctl, func(pool core.BufPool, m *core.Msg) error {
		return meta.EncodeDisks(pool, group, &meta.Membership{Disks: []meta.DiskAddr{{Disk: 1, Log: 10 << 8}}}, m)
	})
	r.out.Reset()
	r.n.Take(&r.ctx, 10, &DeviceOutput{Ask: 1, Fetch: true, Held: 3, Epoch: 2}, &r.out)
	if r.fetch() == 0 {
		t.Fatal("the device's ask fetched no session table")
	}
	r.out.Reset()
	r.n.Take(&r.ctx, 10, &DeviceOutput{Fetch: true}, &r.out)
	if len(r.sends(meta.KindTableFetch)) != 0 {
		t.Fatal("a table fetch went out twice in an interval")
	}
	r.ctx.Now += Interval
	r.out.Reset()
	r.n.Take(&r.ctx, 10, &DeviceOutput{Fetch: true}, &r.out)
	if len(r.sends(meta.KindTableFetch)) != 1 {
		t.Fatal("the device's table fetch went out other than once")
	}
	r.out.Reset()
	r.n.Take(&r.ctx, 11, &DeviceOutput{Failed: true}, &r.out)
	nr := r.report()
	if nr.Sessions != 3 || nr.Epoch != 0 || len(nr.Opened) != 1 || !slices.Equal(nr.Failed, []uint64{11}) {
		t.Fatalf("reported %+v", nr)
	}
}

// A client-only node, with no device, starts its session, sending the same start again until it is
// answered, renews it rate_gap after its first answer and every second after, holds its lease from
// its second answer until it lapses, takes it back on the next answer, ends it on Resumed, and
// starts a new session under a new request once an answer fences it.
func TestAClientOnlyNodeHoldsASession(t *testing.T) {
	r := &run{t: t, n: New(Config{Node: 4, Release: 1, Ctls: []core.NodeID{ctl}, Group: group, Seed: 3, Client: true}), ctx: core.Context{Now: 1, Raw: 1, Pool: sim.NewPool(1<<20, 0)}}
	at := func(d core.Tick) { r.ctx.Now, r.ctx.Raw = r.ctx.Now+d, r.ctx.Raw+d }
	start := func() uint64 {
		t.Helper()
		ms := r.sends(meta.KindSessionStart)
		if len(ms) != 1 {
			t.Fatalf("sent %d starts", len(ms))
		}
		_, _, req, _ := meta.Peek(&ms[0])
		return req
	}
	beat := func() meta.SessionBeat {
		t.Helper()
		ms := r.sends(meta.KindSessionBeat)
		if len(ms) != 1 {
			t.Fatalf("sent %d heartbeats at %v", len(ms), r.ctx.Now)
		}
		var b meta.SessionBeat
		if err := meta.DecodeSessionBeat(r.ctx.Pool, &ms[0], &b); err != nil {
			t.Fatal(err)
		}
		return b
	}
	timer := func() { r.event(core.Event{Kind: core.EvTimer, Timer: SessionTimer}) }
	answer := func(b meta.SessionBeat, st meta.SessionStatus) {
		r.from(ctl, func(pool core.BufPool, m *core.Msg) error {
			return meta.EncodeRenewed(pool, group, 7, &meta.SessionAnswer{Request: b.Request, Raw: r.ctx.Raw, Status: st}, m)
		})
	}
	leased := func() bool { _, ok := r.n.Session(&r.ctx); return ok }

	r.n.Start(&r.ctx, &r.out)
	req := start()
	at(Interval)
	r.out.Reset()
	timer()
	if again := start(); again != req {
		t.Fatalf("start sent again as %d, first %d", again, req)
	}
	r.from(ctl, func(pool core.BufPool, m *core.Msg) error { return meta.EncodeStarted(pool, group, req, 5, m) })
	b := beat()
	if b.Session != 5 {
		t.Fatalf("heartbeat %+v", b)
	}
	answer(b, meta.SessionRenewed)
	if leased() {
		t.Fatal("leased on the first answer")
	}
	at(100_000)
	r.out.Reset()
	timer()
	b2 := beat()
	if b2.Request <= b.Request || b2.Answered != b.Request {
		t.Fatalf("second heartbeat %+v after %+v", b2, b)
	}
	answer(b2, meta.SessionRenewed)
	if !leased() {
		t.Fatal("no lease on the second answer")
	}
	for range 9 {
		at(Interval)
		r.out.Reset()
		timer()
		beat()
	}
	if leased() {
		t.Fatal("leased 9 s after the last answered renewal")
	}
	at(Interval)
	r.out.Reset()
	timer()
	answer(beat(), meta.SessionRenewed)
	if !leased() {
		t.Fatal("no lease on an answer after it lapsed")
	}
	r.event(core.Event{Kind: core.EvResumed})
	if leased() {
		t.Fatal("leased after Resumed")
	}
	answer(beat(), meta.SessionFenced)
	req2 := start()
	if id, _ := r.n.Session(&r.ctx); id != 0 || req2 == req {
		t.Fatalf("after the fence: session %d", id)
	}
	r.from(ctl, func(pool core.BufPool, m *core.Msg) error { return meta.EncodeStarted(pool, group, req2, 6, m) })
	r.n.Fenced(&r.ctx, 5, &r.out)
	if id, _ := r.n.Session(&r.ctx); id != 6 {
		t.Fatalf("a refusal of the old session's write moved the node to session %d", id)
	}
	r.out.Reset()
	r.n.Fenced(&r.ctx, 6, &r.out)
	if id, _ := r.n.Session(&r.ctx); id != 0 || start() == req2 {
		t.Fatalf("a refused write of the session left session %d", id)
	}
	if r.report().Meta {
		t.Fatal("a client-only node reports holding metadata")
	}
}
