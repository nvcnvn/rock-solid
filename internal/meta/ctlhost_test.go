package meta

import (
	"encoding/binary"
	"errors"
	"slices"
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/lease"
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
	t     *testing.T
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
	failing bool
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
	r := &hostRun{t: t, lc: lc, hs: make([]*CtlHost, voters), pool: sim.NewPool(16<<20, 0), now: 5_000_000}
	for i := range voters {
		r.disks = append(r.disks, sim.NewDisk(ctlGeometry.DiskBlocks()))
		sb := disklog.Superblock{LogID: uint64(i) + 1, Geometry: ctlGeometry}
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
	cs := raftpb.ConfState{}
	var peers []raftshard.Peer
	for id := range uint64(len(r.hs)) {
		cs.Voters = append(cs.Voters, id+1)
		peers = append(peers, raftshard.Peer{ID: id + 1, Node: voterNode(id + 1)})
	}
	return Config{Raft: raftshard.Config{Group: hostGroup, ID: uint64(i) + 1, Storage: bootStorage(r.t, cs), Seed: uint64(i), Timer: 1, Peers: peers,
		Holder: lease.NewHolder(lease.DefaultConfig), Grantor: lease.NewGrantor(lease.DefaultConfig, uint64(i)+1, lease.Time{})},
		Tree: lsm.Config{MaxPiece: 64 << 10, Check: true}}
}

// restart crashes host i, losing what its disk did not flush, and recovers it from its disk.
func (r *hostRun) restart(i int) { r.restartWith(i, r.config(i)) }

// restartWith is restart, the host recovering on cfg.
func (r *hostRun) restartWith(i int, cfg Config) {
	r.t.Helper()
	r.disks[i].Crash(func(int, core.LBA) bool { return false })
	r.work = slices.DeleteFunc(r.work, func(w hostWork) bool { return w.at == i || w.from == i })
	r.do(i, func(ctx *core.Context, out *CtlHostOutput) {
		h, err := OpenCtlHost(ctx, cfg, r.lc, ctlGeometry.DiskBlocks(), out)
		if err != nil {
			r.t.Fatal(err)
		}
		r.hs[i] = h
	})
	r.h = r.hs[0]
	r.drain()
}

// do calls into host i and sorts what it hands out.
func (r *hostRun) do(i int, f func(ctx *core.Context, out *CtlHostOutput)) {
	r.t.Helper()
	ctx := core.Context{Now: r.now, Raw: r.now, Pool: r.pool}
	var o CtlHostOutput
	f(&ctx, &o)
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
		for i, h := range r.hs {
			for _, timer := range []core.TimerID{1, StepTimer} {
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
			r := &hostRun{t: t, lc: ctlLogConfig(), hs: make([]*CtlHost, 1), pool: sim.NewPool(16<<20, 0), now: 5_000_000}
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
			if settled {
				break
			}
		}
	}
	if empty == 0 {
		t.Fatal("no crash left a log holding no round")
	}
}

// A voter restarted with voters other than those its disk was formatted with stops: alone, it
// would elect itself and acknowledge commits that no quorum of the group holds.
func TestAVoterRestartedWithOtherVotersStops(t *testing.T) {
	r := newHostGroup(t, 3)
	l := r.leader()
	if a := r.txnAt(l, 9, "x"); a == nil || a.Err != nil {
		t.Fatalf("first Txn answered %+v", a)
	}
	alone := r.config(l)
	alone.Raft.Storage = bootStorage(t, raftpb.ConfState{Voters: []uint64{uint64(l) + 1}})
	alone.Raft.Peers = alone.Raft.Peers[l : l+1]
	r.failing = true
	r.restartWith(l, alone)
	if !errors.Is(r.hs[l].Err(), ErrVoters) || r.hs[l].Shard() != nil {
		t.Fatalf("restarted alone, the voter has error %v and replica %v", r.hs[l].Err(), r.hs[l].Shard())
	}
	r.run(raftshard.TickInterval * 300)
	if a := r.txnAt(l, 10, "y"); a != nil {
		t.Fatalf("the stopped voter answered %+v", a)
	}
	o := r.leader()
	if o == l {
		t.Fatalf("the stopped voter leads")
	}
	if a := r.txnAt(o, 11, "z"); a == nil || a.Err != nil {
		t.Fatalf("the other two voters answered %+v", a)
	}
}

// A log that reaches max_replay_bytes, which nothing checkpoints yet, stops the host with
// ErrLogFull, so its process exits rather than keep a Control that never commits again, and
// stops again once restarted.
func TestAHostWhoseLogFillsStops(t *testing.T) {
	lc := ctlLogConfig()
	lc.MaxReplayBytes = 64 << 10
	r := newHostGroupOn(t, 1, lc)
	r.lead()
	r.failing = true
	for k := range 200 {
		if a := r.txnAt(0, uint64(100+k), "v"); a == nil {
			break
		}
	}
	if !errors.Is(r.h.Err(), ErrLogFull) || r.h.Control() != nil {
		t.Fatalf("a full log left the host with error %v and Control %v", r.h.Err(), r.h.Control())
	}
	r.restart(0)
	r.run(raftshard.TickInterval * 400)
	if !errors.Is(r.h.Err(), ErrLogFull) {
		t.Fatalf("restarted on a full log, the host has error %v", r.h.Err())
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

// A follower of a three-voter group answers a node's report, session fetch and table fetch with
// the leader's node.
func TestAFollowerRedirectsANodeToTheLeader(t *testing.T) {
	c := newGroup(t, 3, false)
	l := c.elect()
	f := (l + 1) % 3
	h := &CtlHost{shard: c.n[f].s, sv: NewServer(), id: uint64(f) + 1, group: 5}
	var msgs [3]core.Msg
	_ = EncodeNodeReport(c.n[f].pool, 5, &NodeReport{Node: hostNode.Node}, &msgs[0])
	_ = EncodeSessionFetch(c.n[f].pool, 5, 1, &msgs[1])
	_ = EncodeTableFetch(c.n[f].pool, 5, &msgs[2])
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
