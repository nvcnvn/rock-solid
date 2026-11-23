package meta

import (
	"bytes"
	"encoding/binary"
	"errors"
	"slices"
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/lease"
	"github.com/nvcnvn/rock-solid/internal/loop/looptest"
	"github.com/nvcnvn/rock-solid/internal/lsm"
	"github.com/nvcnvn/rock-solid/internal/raftshard"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// hostGroup is 257, whose ID begins with the bytes a chunkstore frame of kind 1 begins with.
const hostGroup = 257

var hostNode = core.NodeID{Node: 3, Loop: 1}

// voterNode is the node of the voter whose ID is id, well past the storage nodes the tests name.
func voterNode(id uint64) core.NodeID { return core.NodeID{Node: 100 + uint32(id)} }

// ctlGeometry is a disk of 64 segments of 1 MiB.
var ctlGeometry = disklog.Geometry{SegmentBlocks: 256, SegmentCount: 64}

func ctlLogConfig() disklog.Config {
	cfg := disklog.DefaultConfig()
	cfg.MaxRecordBytes, cfg.ReadDepth, cfg.NoReserve, cfg.NoTrim, cfg.Check = 64<<10, 4, true, true, true
	return cfg
}

// hostRun drives a control-plane group of hosts, each on a disk of its own, as their loops would:
// a call into a host runs every disk action and voter message it causes before it returns. Host
// 0 is h, and the storage nodes' messages go to it.
type hostRun struct {
	t *testing.T
	// pages are each host's page slabs; a host that restarts closes the last, which must give every one back.
	pages []*looptest.Pages
	lc    disklog.Config
	h     *CtlHost
	hs    []*CtlHost
	disks []*sim.Disk
	pool  *sim.Pool
	now   core.Tick
	// out gathers what the hosts handed out during the last call; its actions are in sent and work.
	out CtlHostOutput
	// sent is every message a host sent outside the group, decoded later.
	sent []core.Action
	work []hostWork
	// failing lets a host hand out errors, which out gathers, without failing the test.
	failing   bool
	geo       disklog.Geometry
	stepBytes int
	// keep lands a crash's unflushed writes, which a restart otherwise loses.
	keep bool
	// states is, per host, its state's data at each index it applied, which its state at that
	// index must equal across every crash; nil checks nothing.
	states   []map[uint64][]byte
	diverged int
	// plant plants a bug in each host a restart opens.
	plant func(h *CtlHost)
	// boot is the group's boot configuration, every host a voter when empty; a fresh host is
	// formatted with no configuration, as a voter the group has yet to add.
	boot  raftpb.ConfState
	fresh []bool
	// Each host's node-pair lease holder and grantor; leasing has run renew the leases among the
	// hosts not down every heartbeat. A down host takes no timer and loses every message to or from
	// it; a paused one takes no timer of its replica, though its replica still ticks on each event
	// it takes, a message included. pass, when set, may edit a message between hosts or refuse it,
	// which loses it.
	holders  []*lease.Holder
	grantors []*lease.Grantor
	leaseID  uint64
	leasing  bool
	down     []bool
	paused   []bool
	pass     func(w *hostWork) bool
}

// hostWork is a disk action of host at, or a message from host from to host at.
type hostWork struct {
	from, at int
	a        core.Action
}

func newHostRun(t *testing.T) *hostRun { return newHostGroup(t, 1) }

func newHostGroup(t *testing.T, voters int) *hostRun {
	return newHostGroupOn(t, voters, ctlLogConfig())
}

// newHostGroupOn is newHostGroup, each host's disk log on lc.
func newHostGroupOn(t *testing.T, voters int, lc disklog.Config) *hostRun {
	return newHostGroupOnDisks(t, voters, lc, ctlGeometry, 0, nil)
}

// newHostGroupWith is newHostGroupOn, setup changing the run before any host is formatted.
func newHostGroupWith(t *testing.T, voters int, lc disklog.Config, setup func(r *hostRun)) *hostRun {
	return newHostGroupOnDisks(t, voters, lc, ctlGeometry, 0, setup)
}

// newHostGroupOnDisks is newHostGroupWith, each host's disk of geometry geo and its tree merging in
// steps of stepBytes.
func newHostGroupOnDisks(t *testing.T, voters int, lc disklog.Config, geo disklog.Geometry, stepBytes int, setup func(r *hostRun)) *hostRun {
	r := &hostRun{t: t, lc: lc, geo: geo, stepBytes: stepBytes, hs: make([]*CtlHost, voters), pool: sim.NewPool(16<<20, 0), now: 5_000_000,
		fresh: make([]bool, voters), down: make([]bool, voters), paused: make([]bool, voters),
		holders: make([]*lease.Holder, voters), grantors: make([]*lease.Grantor, voters)}
	if setup != nil {
		setup(r)
	}
	for i := range voters {
		r.disks = append(r.disks, sim.NewDisk(geo.DiskBlocks()))
		sb := disklog.Superblock{LogID: uint64(i) + 1, Geometry: geo}
		r.do(i, func(ctx *core.Context, out *CtlHostOutput) {
			h, err := FormatCtlHost(ctx, r.config(i), r.lc, sb, out)
			if err != nil {
				t.Fatal(err)
			}
			r.hs[i] = h
		})
	}
	r.h = r.hs[0]
	r.drain()
	return r
}

func (r *hostRun) config(i int) Config {
	cs := r.boot
	var peers []raftshard.Peer
	for id := range uint64(len(r.hs)) {
		if len(r.boot.Voters) == 0 {
			cs.Voters = append(cs.Voters, id+1)
		}
		peers = append(peers, raftshard.Peer{ID: id + 1, Node: voterNode(id + 1)})
	}
	st := bootStorage(r.t, cs)
	if r.fresh[i] {
		st = raftshard.NewStorage()
	}
	r.holders[i], r.grantors[i] = lease.NewHolder(lease.DefaultConfig), lease.NewGrantor(lease.DefaultConfig, uint64(i)+1, lease.Time{})
	return Config{Raft: raftshard.Config{Group: hostGroup, ID: uint64(i) + 1, Storage: st, Seed: uint64(i), Timer: 1, Peers: peers,
		Holder: r.holders[i], Grantor: r.grantors[i]},
		Tree: lsm.Config{MaxPiece: 32 << 10, StepBytes: r.stepBytes, Check: true, Pages: r.pagesOf(i)}}
}

// pagesOf is host i's page slabs.
func (r *hostRun) pagesOf(i int) *looptest.Pages {
	for len(r.pages) <= i {
		r.pages = append(r.pages, &looptest.Pages{})
	}
	return r.pages[i]
}

// restart crashes host i, losing what its disk did not flush, and recovers it from its disk.
func (r *hostRun) restart(i int) { r.restartWith(i, r.config(i)) }

// restartWith is restart, the host recovering on cfg.
func (r *hostRun) restartWith(i int, cfg Config) {
	r.t.Helper()
	r.disks[i].Crash(func(int, core.LBA) bool { return r.keep })
	r.work = slices.DeleteFunc(r.work, func(w hostWork) bool { return w.at == i || w.from == i })
	if r.hs[i] != nil {
		r.hs[i].Close()
	}
	if out := r.pagesOf(i).Out; out != 0 {
		r.t.Fatalf("host %d kept %d bytes of page slabs once closed", i, out)
	}
	r.do(i, func(ctx *core.Context, out *CtlHostOutput) {
		h, err := OpenCtlHost(ctx, cfg, r.lc, r.geo.DiskBlocks(), out)
		if err != nil {
			r.t.Fatal(err)
		}
		if r.plant != nil {
			r.plant(h)
		}
		r.hs[i] = h
	})
	r.h = r.hs[0]
	r.drain()
}

// checkState records host i's state at the index it applied, or fails if it differs from the
// state recorded there before; with a bug planted it counts the difference in diverged.
func (r *hostRun) checkState(i int) {
	r.t.Helper()
	h := r.hs[i]
	if r.states == nil || h == nil || h.Shard() == nil {
		return
	}
	for len(r.states) <= i {
		r.states = append(r.states, map[uint64][]byte{})
	}
	st := h.Shard().State()
	data := st.SnapshotData(st.Applied())
	if was, ok := r.states[i][st.Applied()]; ok && !bytes.Equal(was, data) {
		if r.diverged++; r.plant == nil {
			r.t.Fatalf("host %d's state at index %d is not what it was before the crash", i, st.Applied())
		}
	}
	r.states[i][st.Applied()] = data
}

// do calls into host i and sorts what it hands out.
func (r *hostRun) do(i int, f func(ctx *core.Context, out *CtlHostOutput)) {
	r.t.Helper()
	ctx := core.Context{Now: r.now, Raw: r.now, Pool: r.pool}
	var o CtlHostOutput
	f(&ctx, &o)
	r.checkState(i)
	if h := r.hs[i]; h != nil && h.err == nil && h.rec == nil && h.disk.Log.Stalled() && !h.cp.running {
		r.t.Fatalf("host %d's log stalled with no checkpoint to free a segment: its disk is full", i)
	}
	for _, err := range o.Errs {
		if !r.failing {
			r.t.Fatal(err)
		}
	}
	r.out.Errs = append(r.out.Errs, o.Errs...)
	r.out.Started = append(r.out.Started, o.Started...)
	for _, a := range o.Actions {
		switch {
		case a.Kind == core.ActNetSend && a.Peer.Node >= voterNode(1).Node:
			r.work = append(r.work, hostWork{from: i, at: int(a.Peer.Node - voterNode(1).Node), a: a})
		case a.Kind == core.ActNetSend:
			r.sent = append(r.sent, a)
		case a.Kind != core.ActSetTimer:
			r.work = append(r.work, hostWork{from: i, at: i, a: a})
		}
	}
}

// drain runs the disk actions and delivers the voters' messages, oldest first, until none is left.
func (r *hostRun) drain() {
	r.t.Helper()
	for len(r.work) > 0 {
		w := r.work[0]
		r.work = r.work[1:]
		if r.down[w.at] || r.down[w.from] || w.a.Kind == core.ActNetSend && r.pass != nil && !r.pass(&w) {
			free(&core.Context{Pool: r.pool}, &w.a.Msg)
			continue
		}
		ev := core.Event{Kind: core.EvNetRecv, Peer: voterNode(uint64(w.from) + 1), Msg: w.a.Msg}
		if w.a.Kind != core.ActNetSend {
			ev = r.disks[w.at].Execute(r.pool, &w.a)
		}
		r.do(w.at, func(ctx *core.Context, out *CtlHostOutput) { r.hs[w.at].OnEvent(ctx, &ev, out) })
	}
}

func (r *hostRun) recv(from core.NodeID, msg core.Msg) *CtlHostOutput { return r.recvAt(0, from, msg) }

// recvAt hands host i a message from a storage node.
func (r *hostRun) recvAt(i int, from core.NodeID, msg core.Msg) *CtlHostOutput {
	r.t.Helper()
	r.out.Reset()
	r.do(i, func(ctx *core.Context, out *CtlHostOutput) {
		r.hs[i].OnEvent(ctx, &core.Event{Kind: core.EvNetRecv, Peer: from, Msg: msg}, out)
	})
	r.drain()
	return &r.out
}

// run moves the clock on by d, 10 ms at a time, firing every host's group timer and step timer.
func (r *hostRun) run(d core.Tick) {
	r.out.Reset()
	for end := r.now + d; r.now < end; {
		r.now += raftshard.TickInterval
		if r.leasing && r.now%(raftshard.HeartbeatTick*raftshard.TickInterval) == 0 {
			r.renewLeases()
		}
		for i, h := range r.hs {
			for _, timer := range []core.TimerID{1, StepTimer} {
				if r.down[i] || r.paused[i] && timer == 1 {
					continue
				}
				r.do(i, func(ctx *core.Context, out *CtlHostOutput) {
					h.OnEvent(ctx, &core.Event{Kind: core.EvTimer, Timer: timer}, out)
				})
				r.drain()
			}
		}
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

// dropDisks frees the disk memberships sent and drops them from sent.
func (r *hostRun) dropDisks() {
	r.sent = slices.DeleteFunc(r.sent, func(a core.Action) bool {
		if _, k, _, _ := Peek(&a.Msg); k == KindDisks {
			free(&core.Context{Pool: r.pool}, &a.Msg)
			return true
		}
		return false
	})
}

// tables decodes the session tables the host sent since the last call, by peer and token, and
// frees the disk memberships.
func (r *hostRun) tables() map[core.NodeID][]uint64 {
	r.t.Helper()
	got := map[core.NodeID][]uint64{}
	r.dropDisks()
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

// A client's session start, sent twice before it commits, starts one session, whose ID the client
// hears under its request; a node's fetch then gets a table with the session live.
func TestASingleVoterHostLeadsAndAnswersAFetchUnderItsToken(t *testing.T) {
	r := newHostRun(t)
	r.lead()
	r.report(&NodeReport{Node: hostNode.Node})
	r.tables()
	client := core.NodeID{Node: 9, Loop: 2}
	for range 2 {
		var m core.Msg
		if err := EncodeSessionStart(r.pool, hostGroup, 5, &m); err != nil {
			t.Fatal(err)
		}
		r.recv(client, m)
	}
	var started []SessionStart
	for range 10 {
		r.run(StepInterval)
		started = append(started, r.out.Started...)
	}
	if len(started) != 1 || started[0].ID == 0 || started[0].Node != client.Node {
		t.Fatalf("started %+v", started)
	}
	if len(r.sent) != 1 || r.sent[0].Peer != client {
		t.Fatalf("the start answered %+v", r.sent)
	}
	if id, err := DecodeStarted(r.pool, &r.sent[0].Msg); err != nil || id != started[0].ID {
		t.Fatalf("the client heard session %d, %v; started %d", id, err, started[0].ID)
	}
	if _, _, token, _ := Peek(&r.sent[0].Msg); token != 5 {
		t.Fatalf("the start answered under token %d", token)
	}
	r.sent = r.sent[:0]
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

// start sends the host a client's session start under request and runs a second, returning the
// sessions started meanwhile.
func (r *hostRun) start(client core.NodeID, request uint64) []SessionStart {
	r.t.Helper()
	var m core.Msg
	if err := EncodeSessionStart(r.pool, hostGroup, request, &m); err != nil {
		r.t.Fatal(err)
	}
	r.recv(client, m)
	var started []SessionStart
	for range 10 {
		r.run(StepInterval)
		started = append(started, r.out.Started...)
	}
	r.dropDisks()
	for _, a := range r.sent {
		free(&core.Context{Pool: r.pool}, &a.Msg)
	}
	r.sent = r.sent[:0]
	return started
}

// A start that reaches a leader whose Control does not serve yet is never handed to it; the
// client's start sent again once it serves is.
func TestAStartBeforeTheControlServesIsNotTaken(t *testing.T) {
	r := newHostRun(t)
	for range 400 {
		if r.run(raftshard.TickInterval); r.h.Control() != nil {
			break
		}
	}
	if _, ok := r.h.Control().FetchAnswer(lease.Time{Boot: r.now, Raw: r.now}, true); ok {
		t.Fatal("the Control served as soon as it was read")
	}
	client := core.NodeID{Node: 9, Loop: 2}
	if started := r.start(client, 5); len(started) != 0 {
		t.Fatalf("a start before the Control served started %+v", started)
	}
	if started := r.start(client, 5); len(started) != 1 || started[0].Request != 5 {
		t.Fatalf("the start sent again started %+v", started)
	}
}

// A start waiting when the Control drops, as on a term change, is forgotten with it: the client's
// start sent again to the next Control is taken and answered.
func TestAStartWaitingWhenTheControlDropsIsTakenAgain(t *testing.T) {
	r := newHostRun(t)
	r.lead()
	client := core.NodeID{Node: 9, Loop: 2}
	var m core.Msg
	if err := EncodeSessionStart(r.pool, hostGroup, 5, &m); err != nil {
		t.Fatal(err)
	}
	r.recv(client, m)
	r.h.term++
	r.lead()
	if started := r.start(client, 5); len(started) != 1 || started[0].Request != 5 {
		t.Fatalf("the start sent again after the Control dropped started %+v", started)
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
	for i := range 300 {
		if i%10 == 0 {
			for n := range uint32(3) {
				r.report(&NodeReport{Node: n + 2, Sessions: r.h.Control().SessionTable().Version})
			}
		}
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

// A node's report is answered with the committed membership: a disk it reports is named, with its
// log and the loop the report names for it, only once the record holding it commits, and no
// longer once a report leaves it out.
func TestAReportIsAnsweredWithTheCommittedDisks(t *testing.T) {
	r := newHostRun(t)
	r.lead()
	nr := NodeReport{Node: hostNode.Node, Opened: []OpenedDisk{{Device: 1, Log: 0x55, Blocks: 1 << 20, Loop: 4}}}
	disksSent := func() []DiskAddr {
		var got []DiskAddr
		for _, a := range r.sent {
			if _, k, _, _ := Peek(&a.Msg); k == KindDisks && a.Peer == hostNode {
				ds, err := DecodeDisks(r.pool, &a.Msg, nil)
				if err != nil {
					t.Fatal(err)
				}
				got = ds
			}
			free(&core.Context{Pool: r.pool}, &a.Msg)
		}
		r.sent = r.sent[:0]
		return got
	}
	r.report(&nr)
	if got := disksSent(); len(got) != 0 {
		t.Fatalf("before the record commits the report got %+v", got)
	}
	r.run(StepInterval * 3)
	r.report(&nr)
	want := []DiskAddr{{Disk: 1, Log: 0x55, At: core.NodeID{Node: hostNode.Node, Loop: 4}}}
	if got := disksSent(); !slices.Equal(got, want) {
		t.Fatalf("after the record commits the report got %+v, want %+v", got, want)
	}
	r.report(&NodeReport{Node: hostNode.Node})
	if got := disksSent(); len(got) != 0 {
		t.Fatalf("a report without the disk got %+v", got)
	}
	replaced := NodeReport{Node: hostNode.Node, Opened: []OpenedDisk{{Device: 1, Log: 0x66, Blocks: 1 << 20, Loop: 4}}}
	r.report(&replaced)
	r.run(StepInterval * 3)
	disksSent()
	replaced.Opened = append(replaced.Opened, OpenedDisk{Device: 2, Log: 0x55, Blocks: 1 << 20, Loop: 5})
	r.failing = true
	r.report(&replaced)
	want = []DiskAddr{{Disk: 2, Log: 0x66, At: core.NodeID{Node: hostNode.Node, Loop: 4}}}
	if got := disksSent(); !slices.Equal(got, want) {
		t.Fatalf("a gone disk's log reported again got %+v, want %+v", got, want)
	}
}

// A node reporting a replica its group's plan dropped is answered with the replica's deletion
// once every voter of the plan reported having applied its leave and the replica learned its
// removal, and the others with nothing to delete.
func TestAReportIsAnsweredWithTheReplicasToDelete(t *testing.T) {
	r := newHostRun(t)
	r.lead()
	r.h.Control().smap = deleting(1, 2, 4).smap
	plan := []uint64{1, 2, 4}
	deleted := func(node uint32) []ReplicaRef {
		var got []ReplicaRef
		for _, a := range r.sent {
			if _, k, _, _ := Peek(&a.Msg); k == KindDelete && a.Peer.Node == node {
				rs, err := DecodeDelete(r.pool, &a.Msg, nil)
				if err != nil {
					t.Fatal(err)
				}
				got = append(got, rs...)
			}
			free(&core.Context{Pool: r.pool}, &a.Msg)
		}
		r.sent = r.sent[:0]
		return got
	}
	gone := NodeReport{Node: 3, Replicas: []ReplicaReport{{Group: 5, ID: 3, ConfIndex: 3, Joint: true, Removed: true, Voters: plan}}}
	for _, id := range []uint64{1, 2} {
		r.report(&NodeReport{Node: uint32(id), Replicas: []ReplicaReport{{Group: 5, ID: id, ConfIndex: 5, Voters: plan}}})
	}
	r.report(&gone)
	if got := deleted(3); len(got) != 0 {
		t.Fatalf("before D reported the leave applied, node 3 is told to delete %v", got)
	}
	r.report(&NodeReport{Node: 4, Replicas: []ReplicaReport{{Group: 5, ID: 4, ConfIndex: 5, Voters: plan}}})
	if got := deleted(4); len(got) != 0 {
		t.Fatalf("node 4 is told to delete %v", got)
	}
	r.report(&gone)
	if got := deleted(3); !slices.Equal(got, []ReplicaRef{{Group: 5, ID: 3}}) {
		t.Fatalf("once the plan is complete, node 3 is told to delete %v, want C", got)
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
// and a fetch is answered with no leader known, so the node moves to its next address.
func TestAHostWithNoLeaderFreesFramesAndRedirectsAFetch(t *testing.T) {
	r := newHostRun(t)
	live := r.pool.Live()
	var m core.Msg
	if err := chunkstore.Attach(r.pool, &m, make([]byte, core.MaxInlineMsg+1), core.NoBuf); err != nil {
		t.Fatal(err)
	}
	r.recv(hostNode, m)
	if err := EncodeSessionFetch(r.pool, hostGroup, 1, &m); err != nil {
		t.Fatal(err)
	}
	r.recv(hostNode, m)
	if r.h.Control() != nil || len(r.sent) != 1 || r.pool.Live() != live {
		t.Fatalf("sent %v, %d buffers live, %d before", r.sent, r.pool.Live(), live)
	}
	if _, known, err := DecodeLeader(r.pool, &r.sent[0].Msg); err != nil || known {
		t.Fatalf("answered a leader known %v: %v", known, err)
	}
}

// A restart of the lone voter replays its group's log from its disk: the placement table it
// committed is there, a new session gets a later ID than any handed out before, and the next
// placement epoch follows the last.
func TestARestartedHostKeepsEveryCommittedTxn(t *testing.T) {
	r := newHostRun(t)
	r.lead()
	r.report(&NodeReport{Node: hostNode.Node})
	r.h.Control().StartSession(hostNode.Node, 1)
	if err := r.h.Control().SetPool(1, spec(1, 16), disks(2)); err != nil {
		t.Fatal(err)
	}
	var started []SessionStart
	for range 10 {
		r.run(StepInterval)
		started = append(started, r.out.Started...)
	}
	epoch := r.h.Control().Table().Epoch
	if len(started) != 1 || epoch == 0 {
		t.Fatalf("started %+v, epoch %d", started, epoch)
	}
	r.restart(0)
	r.lead()
	c := r.h.Control()
	if got := c.Table(); got.Epoch != epoch || len(got.Pools) != 1 {
		t.Fatalf("after a restart the table is at epoch %d with %d pools, was %d with 1", got.Epoch, len(got.Pools), epoch)
	}
	r.report(&NodeReport{Node: hostNode.Node})
	c.StartSession(hostNode.Node, 2)
	if err := c.SetPool(2, spec(1, 16), disks(2)); err != nil {
		t.Fatal(err)
	}
	var again []SessionStart
	for range 10 {
		r.run(StepInterval)
		again = append(again, r.out.Started...)
	}
	if len(again) != 1 || again[0].ID <= started[0].ID {
		t.Fatalf("after a restart started %+v; before, %+v", again, started)
	}
	if c.Table().Epoch <= epoch {
		t.Fatalf("after a restart a pool committed at epoch %d, not past %d", c.Table().Epoch, epoch)
	}
}

// A Txn is answered only once the disk holds its round.
func TestATxnIsAnsweredOnlyOnceItsRoundIsOnDisk(t *testing.T) {
	r := newHostRun(t)
	r.lead()
	r.sent = r.sent[:0]
	var m core.Msg
	tx := Txn{Ops: []Op{Put(attr(1, 2, 1), b("v"))}}
	if err := EncodeTxn(r.pool, hostGroup, 9, &tx, &m); err != nil {
		t.Fatal(err)
	}
	r.do(0, func(ctx *core.Context, out *CtlHostOutput) {
		r.h.OnEvent(ctx, &core.Event{Kind: core.EvNetRecv, Peer: hostNode, Msg: m}, out)
	})
	if len(r.sent) != 0 || len(r.work) == 0 {
		t.Fatalf("before its disk writes ran the Txn got %+v, with %d disk actions waiting", r.sent, len(r.work))
	}
	r.drain()
	if len(r.sent) != 1 {
		t.Fatalf("once its round is on disk the Txn got %+v", r.sent)
	}
}

// Three voters, each on its own disk log, elect a leader and commit a Txn on every replica.
func TestSeveralVotersOnDiskLogsElectAndCommit(t *testing.T) {
	r := newHostGroup(t, 3)
	leader := -1
	for range 500 {
		r.run(raftshard.TickInterval)
		for i, h := range r.hs {
			if h.Shard().Raft().Leader() == uint64(i)+1 {
				leader = i
			}
		}
		if leader >= 0 {
			break
		}
	}
	if leader < 0 {
		t.Fatal("three voters elected no leader")
	}
	var m core.Msg
	tx := Txn{Ops: []Op{Put(attr(1, 2, 1), b("v"))}}
	if err := EncodeTxn(r.pool, hostGroup, 9, &tx, &m); err != nil {
		t.Fatal(err)
	}
	r.recvAt(leader, hostNode, m)
	var a Answer
	if len(r.sent) != 1 || DecodeAnswer(r.pool, &r.sent[0].Msg, &a) != nil || a.Token != 9 || a.Err != nil {
		t.Fatalf("answered %+v: %+v", r.sent, a)
	}
	r.run(raftshard.TickInterval * 20)
	want := r.hs[leader].Shard().State().Applied()
	for i, h := range r.hs {
		if got := h.Shard().State().Applied(); got != want {
			t.Errorf("voter %d applied through %d, the leader through %d", i+1, got, want)
		}
	}
}

// A crash between a round's write and its Durable keeps the round or drops it whole: a Txn
// answered before the crash is applied after it, and the restarted host leads and answers.
func TestACrashMidRoundKeepsEveryAnsweredTxn(t *testing.T) {
	for _, keep := range []bool{false, true} {
		for steps := range 4 {
			r := newHostRun(t)
			r.lead()
			var m core.Msg
			tx := Txn{Ops: []Op{Put(attr(1, 2, 1), b("v"))}}
			if err := EncodeTxn(r.pool, hostGroup, 9, &tx, &m); err != nil {
				t.Fatal(err)
			}
			r.sent = r.sent[:0]
			r.do(0, func(ctx *core.Context, out *CtlHostOutput) {
				r.h.OnEvent(ctx, &core.Event{Kind: core.EvNetRecv, Peer: hostNode, Msg: m}, out)
			})
			for range steps {
				if len(r.work) == 0 {
					break
				}
				w := r.work[0]
				r.work = r.work[1:]
				ev := r.disks[w.at].Execute(r.pool, &w.a)
				r.do(w.at, func(ctx *core.Context, out *CtlHostOutput) { r.hs[w.at].OnEvent(ctx, &ev, out) })
			}
			answered := len(r.sent) > 0
			r.disks[0].Crash(func(int, core.LBA) bool { return keep })
			r.work = r.work[:0]
			r.hs[0].Close()
			r.do(0, func(ctx *core.Context, out *CtlHostOutput) {
				h, err := OpenCtlHost(ctx, r.config(0), r.lc, ctlGeometry.DiskBlocks(), out)
				if err != nil {
					t.Fatal(err)
				}
				r.hs[0] = h
			})
			r.h = r.hs[0]
			r.drain()
			r.lead()
			a := r.ask(0, 10, &Txn{Ops: []Op{Check(attr(1, 2, 1), b("v"), false)}})
			if a == nil || a.Err != nil {
				t.Fatalf("keep %v, %d disk actions: after the crash a Txn got %+v", keep, steps, a)
			}
			if answered && a.Result.FailedCheck != -1 {
				t.Fatalf("keep %v, %d disk actions: a Txn answered before the crash is lost", keep, steps)
			}
		}
	}
}

// A crash cuts format short after each of its disk actions. A restart finds no superblock, which
// -format formats again, or opens the log; a log holding no round at all, as a crash after the
// superblocks and before the boot round leaves it, gets the boot round from the voters given, as
// format writes it, and the host leads and answers.
func TestARestartAfterAFormatCutShortWritesTheBootRound(t *testing.T) {
	empty := 0
	for _, keep := range []bool{false, true} {
		for steps := 0; ; steps++ {
			r := &hostRun{t: t, lc: ctlLogConfig(), geo: ctlGeometry, hs: make([]*CtlHost, 1), pool: sim.NewPool(16<<20, 0), now: 5_000_000,
				fresh: make([]bool, 1), down: make([]bool, 1), paused: make([]bool, 1), holders: make([]*lease.Holder, 1), grantors: make([]*lease.Grantor, 1)}
			r.disks = append(r.disks, sim.NewDisk(ctlGeometry.DiskBlocks()))
			r.do(0, func(ctx *core.Context, out *CtlHostOutput) {
				h, err := FormatCtlHost(ctx, r.config(0), r.lc, disklog.Superblock{LogID: 1, Geometry: ctlGeometry}, out)
				if err != nil {
					t.Fatal(err)
				}
				r.hs[0] = h
			})
			for range steps {
				if len(r.work) == 0 {
					break
				}
				w := r.work[0]
				r.work = r.work[1:]
				ev := r.disks[0].Execute(r.pool, &w.a)
				r.do(0, func(ctx *core.Context, out *CtlHostOutput) { r.hs[0].OnEvent(ctx, &ev, out) })
			}
			settled := r.hs[0].Shard() != nil
			r.disks[0].Crash(func(int, core.LBA) bool { return keep })
			r.work = r.work[:0]
			r.failing = true
			r.hs[0].Close()
			r.do(0, func(ctx *core.Context, out *CtlHostOutput) {
				h, err := OpenCtlHost(ctx, r.config(0), r.lc, ctlGeometry.DiskBlocks(), out)
				if err != nil {
					t.Fatal(err)
				}
				r.hs[0] = h
			})
			r.h = r.hs[0]
			r.drain()
			if errors.Is(r.h.Err(), disklog.ErrNotFormatted) {
				continue
			}
			if r.h.Err() != nil {
				t.Fatalf("keep %v, %d disk actions: the restart stopped with %v", keep, steps, r.h.Err())
			}
			if !keep && !settled {
				empty++
			}
			r.failing = false
			r.lead()
			if a := r.txnAt(0, 9, "v"); a == nil || a.Err != nil {
				t.Fatalf("keep %v, %d disk actions: after the restart a Txn got %+v", keep, steps, a)
			}
			r.restart(0)
			if r.h.Err() != nil {
				t.Fatalf("keep %v, %d disk actions: the second restart stopped with %v", keep, steps, r.h.Err())
			}
			r.lead()
			if a := r.txnAt(0, 10, "w"); a == nil || a.Err != nil {
				t.Fatalf("keep %v, %d disk actions: after the second restart a Txn got %+v", keep, steps, a)
			}
			if settled {
				break
			}
		}
	}
	if empty == 0 {
		t.Fatal("no crash left a log holding no round")
	}
}

// A voter restarted with voters other than its log holds, here itself alone, takes its log's
// configuration, since -voters names the voters only at the first boot: it does not stop, does not
// elect itself alone, and applies what the group commits.
func TestAVoterRestartedWithOtherVotersTakesItsLogsConfiguration(t *testing.T) {
	r := newHostGroup(t, 3)
	l := r.leader()
	if a := r.txnAt(l, 9, "x"); a == nil || a.Err != nil {
		t.Fatalf("first Txn answered %+v", a)
	}
	alone := r.config(l)
	alone.Raft.Storage = bootStorage(t, raftpb.ConfState{Voters: []uint64{uint64(l) + 1}})
	alone.Raft.Peers = alone.Raft.Peers[l : l+1]
	r.restartWith(l, alone)
	if err := r.hs[l].Err(); err != nil || r.hs[l].Shard() == nil {
		t.Fatalf("restarted alone, the voter has error %v and replica %v", err, r.hs[l].Shard())
	}
	if _, cs := r.hs[l].Shard().Applied(); !slices.Equal(cs.Voters, []uint64{1, 2, 3}) {
		t.Fatalf("restarted alone, the voter is in %+v", cs)
	}
	o := r.leader()
	if a := r.txnAt(o, 11, "z"); a == nil || a.Err != nil {
		t.Fatalf("voter %d answered %+v", o+1, a)
	}
	r.run(raftshard.TickInterval * 50)
	if got, want := r.hs[l].Shard().State().Applied(), r.hs[o].Shard().State().Applied(); got != want {
		t.Fatalf("the restarted voter applied through %d, the leader through %d", got, want)
	}
}

// leader runs the group until a voter leads, and returns it.
func (r *hostRun) leader() int {
	r.t.Helper()
	for range 500 {
		r.run(raftshard.TickInterval)
		for i, h := range r.hs {
			if h.Err() == nil && h.Shard() != nil && h.Shard().Raft().Leader() == uint64(i)+1 {
				return i
			}
		}
	}
	r.t.Fatal("no voter leads")
	return -1
}

// txnAt hands voter i a Txn from a storage node putting v, and returns its answer, or nil.
func (r *hostRun) txnAt(i int, token uint64, v string) *Answer {
	return r.ask(i, token, &Txn{Ops: []Op{Put(attr(1, 2, 1), b(v))}})
}

// ask hands voter i tx from a storage node, and returns its answer, or nil.
func (r *hostRun) ask(i int, token uint64, tx *Txn) *Answer {
	r.t.Helper()
	var m core.Msg
	if err := EncodeTxn(r.pool, hostGroup, token, tx, &m); err != nil {
		r.t.Fatal(err)
	}
	r.sent = r.sent[:0]
	r.recvAt(i, hostNode, m)
	for _, a := range r.sent {
		var ans Answer
		if DecodeAnswer(r.pool, &a.Msg, &ans) == nil && ans.Token == token {
			return &ans
		}
	}
	return nil
}

// A disk that fails stops the host: it reports the error, drops its Control and takes nothing more,
// not a Txn, a fetch or a timer.
func TestAHostWhoseDiskFailsStops(t *testing.T) {
	r := newHostRun(t)
	r.lead()
	r.disks[0].Inject(sim.Fault{Effect: sim.FailIO, When: sim.OnKind(core.ActDiskWrite)})
	r.failing = true
	var m core.Msg
	tx := Txn{Ops: []Op{Put(attr(1, 2, 1), b("v"))}}
	if err := EncodeTxn(r.pool, hostGroup, 9, &tx, &m); err != nil {
		t.Fatal(err)
	}
	r.sent = r.sent[:0]
	out := r.recv(hostNode, m)
	if len(out.Errs) == 0 || r.h.Err() == nil || r.h.Control() != nil {
		t.Fatalf("a failed write gave errors %v, host error %v", out.Errs, r.h.Err())
	}
	if err := EncodeTxn(r.pool, hostGroup, 10, &tx, &m); err != nil {
		t.Fatal(err)
	}
	var fetch core.Msg
	if err := EncodeSessionFetch(r.pool, hostGroup, 11, &fetch); err != nil {
		t.Fatal(err)
	}
	r.now += StepInterval
	for _, ev := range []core.Event{
		{Kind: core.EvNetRecv, Peer: hostNode, Msg: m},
		{Kind: core.EvNetRecv, Peer: hostNode, Msg: fetch},
		{Kind: core.EvTimer, Timer: 1},
		{Kind: core.EvTimer, Timer: StepTimer},
	} {
		var out CtlHostOutput
		r.h.OnEvent(&core.Context{Now: r.now, Raw: r.now, Pool: r.pool}, &ev, &out)
		if len(out.Actions) != 0 || len(out.Errs) != 0 {
			t.Fatalf("a stopped host took event %d, timer %d, and handed out %d actions, errors %v", ev.Kind, ev.Timer, len(out.Actions), out.Errs)
		}
	}
}

// A follower of a three-voter group answers a node's report, session fetch, table fetch and
// session start and an operator's command with the leader's node.
func TestAFollowerRedirectsANodeToTheLeader(t *testing.T) {
	c := newGroup(t, 3, false)
	l := c.elect()
	f := (l + 1) % 3
	h := &CtlHost{shard: c.n[f].s, sv: NewServer(), id: uint64(f) + 1, group: 5}
	var msgs [5]core.Msg
	_ = EncodeNodeReport(c.n[f].pool, 5, &NodeReport{Node: hostNode.Node}, &msgs[0])
	_ = EncodeSessionFetch(c.n[f].pool, 5, 1, &msgs[1])
	_ = EncodeTableFetch(c.n[f].pool, 5, &msgs[2])
	_ = EncodeSessionStart(c.n[f].pool, 5, 1, &msgs[3])
	_ = EncodeAdmin(c.n[f].pool, 5, 1, &Admin{Op: AdminFailed}, &msgs[4])
	for i := range msgs {
		var out CtlHostOutput
		ctx := c.ctx(f)
		h.OnEvent(&ctx, &core.Event{Kind: core.EvNetRecv, Peer: hostNode, Msg: msgs[i]}, &out)
		var sent []core.Action
		for _, a := range out.Actions {
			if a.Kind == core.ActNetSend && a.Peer == hostNode {
				sent = append(sent, a)
			}
		}
		if len(sent) != 1 {
			t.Fatalf("message %d answered %+v", i, sent)
		}
		at, known, err := DecodeLeader(c.n[f].pool, &sent[0].Msg)
		if err != nil || !known || at != (core.NodeID{Node: uint32(l)}) {
			t.Fatalf("message %d answered leader %v known %v, %v; leader is %d", i, at, known, err, l)
		}
	}
}

// A committed table change sends its delta to every node that reported, and a node's table fetch
// gets the whole table.
func TestACommittedTableReachesEveryNode(t *testing.T) {
	r := newHostRun(t)
	r.lead()
	for n := range uint32(2) {
		r.report(&NodeReport{Node: n + 2})
	}
	r.tables()
	if err := r.h.Control().SetPool(1, spec(1, 16), disks(2)); err != nil {
		t.Fatal(err)
	}
	r.run(StepInterval * 3)
	var to []core.NodeID
	for _, a := range r.sent {
		if d, err := DecodeDelta(r.pool, &a.Msg); err == nil && d.Epoch == r.h.Control().Table().Epoch {
			to = append(to, a.Peer)
		}
		free(&core.Context{Pool: r.pool}, &a.Msg)
	}
	r.sent = r.sent[:0]
	if want := []core.NodeID{{Node: 2, Loop: 1}, {Node: 3, Loop: 1}}; !slices.Equal(to, want) {
		t.Fatalf("delta sent to %v, want %v", to, want)
	}
	var m core.Msg
	if err := EncodeTableFetch(r.pool, hostGroup, &m); err != nil {
		t.Fatal(err)
	}
	r.recv(hostNode, m)
	if len(r.sent) != 1 || r.sent[0].Peer != hostNode {
		t.Fatalf("a table fetch answered %+v", r.sent)
	}
	got, err := DecodeTable(r.pool, &r.sent[0].Msg)
	if err != nil || got.Epoch != r.h.Control().Table().Epoch || len(got.Pools) != 1 {
		t.Fatalf("a table fetch got %+v, %v", got, err)
	}
	free(&core.Context{Pool: r.pool}, &r.sent[0].Msg)
	r.sent = r.sent[:0]
	r.report(&NodeReport{Node: 2})
	r.report(&NodeReport{Node: 3, Epoch: got.Epoch})
	r.dropDisks()
	if len(r.sent) != 1 || r.sent[0].Peer != (core.NodeID{Node: 2, Loop: 1}) {
		t.Fatalf("heartbeats behind and current after a failover got %+v; only the one behind gets the table", r.sent)
	}
	if got, err := DecodeTable(r.pool, &r.sent[0].Msg); err != nil || got.Epoch != r.h.Control().Table().Epoch {
		t.Fatalf("the node behind got %+v, %v", got, err)
	}
}

// groupMsg frames a Raft message for group as raftshard's wire does.
func groupMsg(t *testing.T, group uint64, m raftpb.Message) core.Msg {
	var msg core.Msg
	msg.Body[0] = core.ProtoGroup
	binary.LittleEndian.PutUint64(msg.Body[1:], group)
	n, err := m.MarshalTo(msg.Body[10:])
	if err != nil {
		t.Fatal(err)
	}
	msg.Len, msg.Buf = uint16(10+n), core.NoBuf
	return msg
}

// A replica that stops leading drops its Control: a heartbeat of a later term deposes the lone
// voter, and a fetch then gets the leader it knows instead of an answer.
func TestADeposedLeaderDropsItsControl(t *testing.T) {
	r := newHostRun(t)
	r.lead()
	r.recv(core.NodeID{Node: 7}, groupMsg(t, hostGroup, raftpb.Message{Type: raftpb.MsgHeartbeat, From: 2, To: 1, Term: r.h.shard.Raft().Term() + 1}))
	if r.h.Control() != nil {
		t.Fatal("a deposed leader kept its Control")
	}
	r.sent = r.sent[:0]
	var m core.Msg
	if err := EncodeSessionFetch(r.pool, hostGroup, 1, &m); err != nil {
		t.Fatal(err)
	}
	r.recv(hostNode, m)
	if len(r.sent) != 1 || r.sent[0].Peer != hostNode {
		t.Fatalf("a fetch at a deposed leader answered %+v", r.sent)
	}
	if _, _, err := DecodeLeader(r.pool, &r.sent[0].Msg); err != nil {
		t.Fatalf("a fetch at a deposed leader got no leader: %v", err)
	}
}

// A leader whose lease does not run gets no Control: the helper's group leads without leases.
func TestALeaderWithoutItsLeaseHasNoControl(t *testing.T) {
	c := newGroup(t, 3, false)
	l := c.elect()
	c.heartbeat()
	h := &CtlHost{shard: c.n[l].s, sv: NewServer(), id: uint64(l) + 1, group: 5}
	ctx := c.ctx(l)
	if h.shard.Raft().Leader() != h.id || h.shard.Raft().LeaseRead(&ctx) {
		t.Fatal("replica does not lead, or has a lease")
	}
	var m core.Msg
	_ = EncodeSessionFetch(c.n[l].pool, 5, 1, &m)
	var out CtlHostOutput
	h.OnEvent(&ctx, &core.Event{Kind: core.EvNetRecv, Peer: hostNode, Msg: m}, &out)
	if h.Control() != nil {
		t.Fatal("a leader without its lease read a Control")
	}
}

// A replica's report reaches the Control: its disk's fill is taken.
func TestAReplicaReportReachesTheControl(t *testing.T) {
	r := newHostRun(t)
	r.lead()
	r.report(&NodeReport{Node: hostNode.Node, Opened: []OpenedDisk{{Device: 1, Log: 0x77, Blocks: 1 << 20}}})
	r.run(StepInterval * 3)
	var m core.Msg
	if err := chunkstore.Attach(r.pool, &m, chunkstore.AppendReport(nil, &chunkstore.Report{ID: 1, Disks: []chunkstore.DiskState{{Log: 0x77, Fill: 1234}}}), core.NoBuf); err != nil {
		t.Fatal(err)
	}
	r.recv(hostNode, m)
	c := r.h.Control()
	if id := c.diskOfLog(0x77); id == 0 || c.diskFill(id).live != 1234 {
		t.Fatalf("the report's fill was not taken: disk %d", id)
	}
}

// A pool over the disks nodes reported opened places every PG with a primary on them.
func TestAPoolOverReportedDisksPlacesItsPGs(t *testing.T) {
	r := newHostRun(t)
	r.lead()
	r.report(&NodeReport{Node: 2, Opened: []OpenedDisk{{Device: 1, Log: 0x77, Blocks: 1 << 20}}})
	r.run(StepInterval * 3)
	if err := r.h.Control().SetPool(1, spec(1, 4), []chunkstore.Disk{{ID: 1, Node: 2, Blocks: 1 << 20}}); err != nil {
		t.Fatal(err)
	}
	r.run(StepInterval * 3)
	tb := r.h.Control().Table()
	if len(tb.Pools) != 1 || len(tb.Pools[0].PGs) != 4 {
		t.Fatalf("table %+v", tb)
	}
	for i, pg := range tb.Pools[0].PGs {
		if len(pg.Members) != 1 || pg.Members[0].Disk != 1 {
			t.Fatalf("PG %d placed on %+v, want disk 1", i, pg.Members)
		}
	}
}

// A replica's ask over the host reaches Control.Ask, from a node heard from alone: the PG moves to
// a new interval, whose delta goes to the node; its activation, in a replica's report, reaches
// Control.Report, which a later table folds in.
func TestAReplicasAskAndActivationReachTheControl(t *testing.T) {
	r := newHostRun(t)
	r.lead()
	r.report(&NodeReport{Node: 2, Opened: []OpenedDisk{{Device: 1, Log: 0x77, Blocks: 1 << 20, Loop: 3}}})
	r.run(StepInterval * 3)
	if err := r.h.Control().SetPool(1, spec(1, 1), []chunkstore.Disk{{ID: 1, Node: 2, Blocks: 1 << 20}}); err != nil {
		t.Fatal(err)
	}
	r.run(StepInterval * 3)
	tb := r.h.Control().Table()
	pg := tb.Pools[0].PGs[0]
	ask := []chunkstore.PGFields{{Epoch: tb.Epoch, PG: core.PGID{Pool: 1}, Interval: pg.Interval}}
	replica := core.NodeID{Node: 2, Loop: 3}
	send := func(from core.NodeID, encode func(m *core.Msg) error) (delta bool) {
		t.Helper()
		var m core.Msg
		if err := encode(&m); err != nil {
			t.Fatal(err)
		}
		r.recv(from, m)
		r.run(StepInterval * 3)
		r.dropDisks()
		for _, a := range r.sent {
			_, k, _, _ := Peek(&a.Msg)
			delta = delta || k == KindDelta && a.Peer == core.NodeID{Node: 2, Loop: 1}
			free(&core.Context{Pool: r.pool}, &a.Msg)
		}
		r.sent = r.sent[:0]
		return delta
	}
	send(core.NodeID{Node: 5, Loop: 3}, func(m *core.Msg) error { return EncodeAsks(r.pool, hostGroup, ask, m) })
	if got := r.h.Control().Table().Pools[0].PGs[0]; got.Interval != pg.Interval {
		t.Fatalf("an ask from a node not heard from moved the PG to interval %d", got.Interval)
	}
	delta := send(replica, func(m *core.Msg) error { return EncodeAsks(r.pool, hostGroup, ask, m) })
	next := r.h.Control().Table().Pools[0].PGs[0]
	if next.Interval <= pg.Interval || !delta {
		t.Fatalf("the ask left the PG at interval %d, a delta sent to the node: %v", next.Interval, delta)
	}
	act := chunkstore.Report{ID: 1, Activations: []chunkstore.PGReport{{PGFields: chunkstore.PGFields{Epoch: r.h.Control().Table().Epoch, PG: core.PGID{Pool: 1}, Interval: next.Interval}, PGCount: uint32(len(r.h.Control().Table().Pools[0].PGs))}}}
	send(replica, func(m *core.Msg) error {
		return chunkstore.Attach(r.pool, m, chunkstore.AppendReport(nil, &act), core.NoBuf)
	})
	if got := r.h.Control().Table().Pools[0].PGs[0]; got.Activated != next.Interval {
		t.Fatalf("the activation of interval %d left the PG at %+v", next.Interval, got)
	}
}

// After a restart, a disk whose node has not reported yet keeps its PGs and takes no new one:
// placement reads it at size 0, not as having left its pools.
func TestARestartKeepsThePGsOfADiskNotReportedYet(t *testing.T) {
	r := newHostRun(t)
	r.lead()
	rep2 := &NodeReport{Node: 2, Opened: []OpenedDisk{{Device: 1, Log: 0x77, Blocks: 1 << 20}}}
	rep3 := &NodeReport{Node: 3, Opened: []OpenedDisk{{Device: 1, Log: 0x88, Blocks: 1 << 20}}}
	r.report(rep2)
	r.report(rep3)
	r.run(StepInterval * 3)
	both := []chunkstore.Disk{{ID: 1, Node: 2, Blocks: 1 << 20}, {ID: 2, Node: 3, Blocks: 1 << 20}}
	if err := r.h.Control().SetPool(1, spec(1, 8), both); err != nil {
		t.Fatal(err)
	}
	r.run(StepInterval * 3)
	before := r.h.Control().Table()
	r.restart(0)
	r.lead()
	r.report(rep2)
	if err := r.h.Control().SetPool(2, spec(1, 8), both); err != nil {
		t.Fatal(err)
	}
	r.run(StepInterval * 3)
	mid := r.h.Control().Table()
	if len(mid.Pools) != 2 {
		t.Fatalf("pool 2 not placed: %+v", mid.Pools)
	}
	for i, pg := range mid.Pools[1].PGs {
		if slices.ContainsFunc(pg.Members, func(m chunkstore.Member) bool { return m.Disk == 2 }) {
			t.Errorf("pool 2's PG %d placed on disk 2, not reported since the restart: %+v", i, pg.Members)
		}
	}
	r.report(rep3)
	r.run(StepInterval * 3)
	after := r.h.Control().Table()
	for i := range after.Pools[0].PGs {
		if !slices.Equal(after.Pools[0].PGs[i].Members, before.Pools[0].PGs[i].Members) {
			t.Errorf("PG %d moved from %+v to %+v", i, before.Pools[0].PGs[i].Members, after.Pools[0].PGs[i].Members)
		}
	}
}

// A node's message for another group is dropped, its node not recorded.
func TestAMessageOfAnotherGroupIsDropped(t *testing.T) {
	r := newHostRun(t)
	r.lead()
	var m core.Msg
	if err := EncodeNodeReport(r.pool, hostGroup+1, &NodeReport{Node: hostNode.Node}, &m); err != nil {
		t.Fatal(err)
	}
	r.recv(hostNode, m)
	if len(r.h.nodes) != 0 {
		t.Fatalf("a report for another group was taken: %v", r.h.nodes)
	}
}

// A node not heard from for NodeDownAfter gets no more deltas or pushes, so one gone for good
// costs the host no dial.
func TestANodeSilentForNodeDownAfterIsSentNothing(t *testing.T) {
	r := newHostRun(t)
	r.lead()
	r.report(&NodeReport{Node: 2})
	r.run(chunkstore.NodeDownAfter)
	r.report(&NodeReport{Node: 3})
	for _, a := range r.sent {
		free(&core.Context{Pool: r.pool}, &a.Msg)
	}
	r.sent = r.sent[:0]
	if err := r.h.Control().SetPool(1, spec(1, 16), disks(2)); err != nil {
		t.Fatal(err)
	}
	r.run(StepInterval * 3)
	var to []core.NodeID
	for _, a := range r.sent {
		if _, err := DecodeDelta(r.pool, &a.Msg); err == nil {
			to = append(to, a.Peer)
		}
		free(&core.Context{Pool: r.pool}, &a.Msg)
	}
	if want := []core.NodeID{{Node: 3, Loop: 1}}; !slices.Equal(to, want) {
		t.Fatalf("delta sent to %v, want %v", to, want)
	}
}
