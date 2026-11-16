package meta

import (
	"slices"
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/lease"
	"github.com/nvcnvn/rock-solid/internal/lsm"
	"github.com/nvcnvn/rock-solid/internal/raftshard"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// hostGroup is 257, whose ID begins with the bytes a chunkstore frame of kind 1 begins with.
const hostGroup = 257

var hostNode = core.NodeID{Node: 3, Loop: 1}

// hostRun drives a single-voter control-plane host as its loop would, on node 0.
type hostRun struct {
	t    *testing.T
	h    *CtlHost
	pool *sim.Pool
	now  core.Tick
	out  CtlHostOutput
	// sent is every message the host sent, by peer, decoded later.
	sent []core.Action
}

func newHostRun(t *testing.T) *hostRun {
	cfg := Config{Raft: raftshard.Config{Group: hostGroup, ID: 1, Storage: bootStorage(t, raftpb.ConfState{Voters: []uint64{1}}), Timer: 1,
		Peers: []raftshard.Peer{{ID: 1}}, Holder: lease.NewHolder(lease.DefaultConfig), Grantor: lease.NewGrantor(lease.DefaultConfig, 1, lease.Time{})},
		Tree: lsm.Config{MaxPiece: 64 << 10, Check: true}}
	h, err := NewCtlHost(cfg)
	if err != nil {
		t.Fatal(err)
	}
	r := &hostRun{t: t, h: h, pool: sim.NewPool(1<<20, 0), now: 5_000_000}
	r.call(func(ctx *core.Context, out *CtlHostOutput) { h.Start(ctx, out) })
	return r
}

func (r *hostRun) call(f func(ctx *core.Context, out *CtlHostOutput)) *CtlHostOutput {
	r.t.Helper()
	ctx := core.Context{Now: r.now, Raw: r.now, Pool: r.pool}
	r.out.Reset()
	f(&ctx, &r.out)
	for _, err := range r.out.Errs {
		r.t.Fatal(err)
	}
	for _, a := range r.out.Actions {
		if a.Kind == core.ActNetSend {
			r.sent = append(r.sent, a)
		}
	}
	return &r.out
}

func (r *hostRun) event(ev core.Event) *CtlHostOutput {
	return r.call(func(ctx *core.Context, out *CtlHostOutput) { r.h.OnEvent(ctx, &ev, out) })
}

func (r *hostRun) recv(from core.NodeID, msg core.Msg) *CtlHostOutput {
	return r.event(core.Event{Kind: core.EvNetRecv, Peer: from, Msg: msg})
}

// run moves the clock on by d, 10 ms at a time, firing the group's timer and the step timer.
func (r *hostRun) run(d core.Tick) {
	for end := r.now + d; r.now < end; {
		r.now += raftshard.TickInterval
		r.event(core.Event{Kind: core.EvTimer, Timer: 1})
		r.event(core.Event{Kind: core.EvTimer, Timer: StepTimer})
	}
}

// lead runs the host until its Control can answer a fetch: it leads, leases and has committed a
// session Txn of its own.
func (r *hostRun) lead() {
	r.t.Helper()
	for range 400 {
		r.run(raftshard.TickInterval)
		if c := r.h.Control(); c != nil {
			if _, ok := c.FetchAnswer(lease.Time{Boot: r.now, Raw: r.now}, true); ok {
				return
			}
		}
	}
	r.t.Fatal("the lone voter never led with a current session table")
}

func (r *hostRun) report(nr *NodeReport) {
	r.t.Helper()
	var m core.Msg
	if err := EncodeNodeReport(r.pool, hostGroup, nr, &m); err != nil {
		r.t.Fatal(err)
	}
	r.recv(core.NodeID{Node: nr.Node, Loop: 1}, m)
}

// tables decodes the session tables the host sent since the last call, by peer and token.
func (r *hostRun) tables() map[core.NodeID][]uint64 {
	r.t.Helper()
	got := map[core.NodeID][]uint64{}
	for _, a := range r.sent {
		var st chunkstore.SessionTable
		if err := DecodeSessions(r.pool, &a.Msg, &st); err != nil {
			r.t.Fatalf("sent %v: %v", a.Msg.Inline(), err)
		}
		_, _, token, _ := Peek(&a.Msg)
		got[a.Peer] = append(got[a.Peer], token, st.Version)
		free(&core.Context{Pool: r.pool}, &a.Msg)
	}
	r.sent = r.sent[:0]
	return got
}

func TestASingleVoterHostLeadsAndAnswersAFetchUnderItsToken(t *testing.T) {
	r := newHostRun(t)
	r.lead()
	r.report(&NodeReport{Node: hostNode.Node})
	r.h.Control().StartSession(hostNode.Node, 1)
	var started []SessionStart
	for range 10 {
		r.run(StepInterval)
		started = append(started, r.out.Started...)
	}
	if len(started) != 1 || started[0].ID == 0 {
		t.Fatalf("started %+v", started)
	}
	r.tables()
	var m core.Msg
	if err := EncodeSessionFetch(r.pool, hostGroup, 77, &m); err != nil {
		t.Fatal(err)
	}
	r.recv(hostNode, m)
	if len(r.sent) != 1 || r.sent[0].Peer != hostNode || !r.sent[0].Reply {
		t.Fatalf("answered %+v", r.sent)
	}
	var st chunkstore.SessionTable
	if err := DecodeSessions(r.pool, &r.sent[0].Msg, &st); err != nil || !slices.Contains(st.Live, started[0].ID) {
		t.Fatalf("answered %+v, %v", st, err)
	}
	if _, _, token, _ := Peek(&r.sent[0].Msg); token != 77 {
		t.Fatalf("answered under token %d", token)
	}
}

func TestAFencePushesTheSessionTableToEveryNodeHeardFrom(t *testing.T) {
	r := newHostRun(t)
	r.lead()
	for n := range uint32(3) {
		r.report(&NodeReport{Node: n + 2})
	}
	r.h.Control().StartSession(2, 1)
	r.run(StepInterval * 3)
	r.tables()
	pushed := map[core.NodeID][]uint64{}
	for range 300 {
		r.run(StepInterval)
		for p, v := range r.tables() {
			pushed[p] = append(pushed[p], v...)
		}
		if len(pushed) > 0 {
			break
		}
	}
	for n := range uint32(3) {
		if got := pushed[core.NodeID{Node: n + 2, Loop: 1}]; len(got) != 2 || got[0] != 0 || got[1] != 1 {
			t.Errorf("node %d got %v, want version 1 under token 0", n+2, got)
		}
	}
	r.report(&NodeReport{Node: 2, Sessions: 0})
	r.report(&NodeReport{Node: 3, Sessions: 1})
	if got := r.tables(); len(got) != 1 || len(got[core.NodeID{Node: 2, Loop: 1}]) != 2 {
		t.Fatalf("a heartbeat behind and one current got %v; only the one behind gets the table", got)
	}
}

func TestAReportFromAnotherNodeIsDropped(t *testing.T) {
	r := newHostRun(t)
	r.lead()
	var m core.Msg
	if err := EncodeNodeReport(r.pool, hostGroup, &NodeReport{Node: 4}, &m); err != nil {
		t.Fatal(err)
	}
	r.recv(hostNode, m)
	if len(r.h.nodes) != 0 {
		t.Fatalf("a report naming node 4 from node 3 was taken: %v", r.h.nodes)
	}
}

func TestATxnToTheGroupIsAnswered(t *testing.T) {
	r := newHostRun(t)
	r.lead()
	r.tables()
	var m core.Msg
	tx := Txn{Ops: []Op{Put(attr(1, 2, 1), b("v"))}}
	if err := EncodeTxn(r.pool, hostGroup, 9, &tx, &m); err != nil {
		t.Fatal(err)
	}
	r.recv(hostNode, m)
	var a Answer
	if len(r.sent) != 1 || DecodeAnswer(r.pool, &r.sent[0].Msg, &a) != nil || a.Token != 9 || a.Err != nil {
		t.Fatalf("answered %+v: %+v", r.sent, a)
	}
}

// Before it leads, the host has no Control: a chunkstore frame is dropped and its buffer freed,
// and a fetch goes unanswered.
func TestAHostThatDoesNotLeadAnswersNoFetchAndFreesFrames(t *testing.T) {
	r := newHostRun(t)
	var m core.Msg
	if err := chunkstore.Attach(r.pool, &m, make([]byte, core.MaxInlineMsg+1), core.NoBuf); err != nil {
		t.Fatal(err)
	}
	r.recv(hostNode, m)
	if err := EncodeSessionFetch(r.pool, hostGroup, 1, &m); err != nil {
		t.Fatal(err)
	}
	r.recv(hostNode, m)
	if r.h.Control() != nil || len(r.sent) != 0 || r.pool.Live() != 0 {
		t.Fatalf("sent %v, %d buffers live", r.sent, r.pool.Live())
	}
}
