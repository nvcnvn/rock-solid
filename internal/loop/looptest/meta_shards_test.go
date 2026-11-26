package looptest

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/chunkstore/storetest"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/lease"
	"github.com/nvcnvn/rock-solid/internal/loop"
	"github.com/nvcnvn/rock-solid/internal/lsm"
	"github.com/nvcnvn/rock-solid/internal/meta"
	"github.com/nvcnvn/rock-solid/internal/raftshard"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// metaShards runs a store of configuration cfg on a device loop, ID 1, that owns its log, with a
// MetaStore replica (meta.RoundShard) of a group of its own on the round driver of each loop
// listed in on, known to the store, which hears their replies, kept in replies; loop 0 is another loop of the
// node; joined puts them in one group instead, whose voter is the replica on loop 0, and the one on
// loop 1 a learner that joins it with empty storage. Each new process starts its loops and opens
// each replica to restore from the store's mail. Raft timers reach the replicas only while ticking, so the rig settles; Txns go to a
// replica from its loop's machine, and answers and refusals are kept by token.
type metaShards struct {
	t       *testing.T
	cfg     chunkstore.Config
	sd      *storetest.SimDisk
	pool    core.BufPool
	s       *sim.Scheduler
	node    uint32
	ring    *Disk
	loops   [2]*loop.Loop
	lg      *loop.Log
	r       *storetest.Rig
	maxBlob uint64
	on      []int
	reps    [2]*metaRep
	ticking bool
	joined  bool
	replies []loop.Reply
	// answered, if set, takes each replica's answers too.
	answered func(*core.Context, []meta.Answer, []core.Action) []core.Action
}

// metaRep is the replica on a loop: Txns waiting for its loop, the answer to each Txn by token,
// nil for one applied, the key each token wrote, the tokens refused overloaded, and the other
// refusals of a learner.
type metaRep struct {
	a       *meta.RoundShard
	group   uint64
	pending []uint64
	answers map[uint64]error
	keys    map[uint64][]byte
	refused []uint64
	errs    []error
	next    uint64
}

const (
	metaRaftTimer core.TimerID = 50
	metaTxnTimer  core.TimerID = 51
)

func newMetaShards(t *testing.T, cfg chunkstore.Config, on ...int) *metaShards {
	return (&metaShards{t: t, cfg: cfg, on: on}).format()
}

func (sh *metaShards) format() *metaShards {
	sh.sd = storetest.NewSimDiskOf(1, storetest.DiskBytes)
	sh.start()
	r := storetest.NewRig(sh.t, sh, sh.cfg)
	r.Opened, r.Mail = func(st *chunkstore.Store) { sh.opened(st, true) }, sh.mail
	sh.r, sh.maxBlob = r, r.S.Log().MaxBlobBytes()
	sh.opened(r.S, false)
	return sh
}

func (sh *metaShards) opened(st *chunkstore.Store, open bool) {
	if sh.pool != sh.sd.Pool() {
		sh.start()
	}
	for _, i := range sh.on {
		rep := &metaRep{group: uint64(10 + i), answers: map[uint64]error{}, keys: map[uint64][]byte{}}
		if old := sh.reps[i]; old != nil {
			rep.keys, rep.answers, rep.refused, rep.next = old.keys, old.answers, old.refused, old.next
		}
		sh.reps[i] = rep
		id, cs := uint64(1), raftpb.ConfState{Voters: []uint64{1}}
		peers := []raftshard.Peer{{ID: 1, Node: core.NodeID{Node: sh.node, Loop: uint32(i)}}}
		if sh.joined {
			rep.group, id, cs.Learners = 10, uint64(1+i), []uint64{2}
			peers = []raftshard.Peer{{ID: 1, Node: core.NodeID{Node: sh.node, Loop: 0}}, {ID: 2, Node: core.NodeID{Node: sh.node, Loop: 1}}}
		}
		storage := raftshard.NewStorage()
		if id == 1 {
			if err := storage.ApplySnapshot(raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 1, Term: 1, ConfState: cs}}); err != nil {
				sh.t.Fatal(err)
			}
		}
		cfg := meta.RoundConfig{
			Shard: meta.Config{
				Raft: raftshard.Config{Group: rep.group, ID: id, Storage: storage, Seed: uint64(i), Timer: metaRaftTimer,
					Peers:  peers,
					Holder: lease.NewHolder(lease.DefaultConfig), Grantor: lease.NewGrantor(lease.DefaultConfig, 1, lease.Time{})},
				Tree: lsm.Config{MaxPiece: 4 << 10, StepBytes: 4 << 10, Check: true},
			},
			Owner: metaOwner(i), MaxBlob: sh.maxBlob, Open: open,
			Answered: func(ctx *core.Context, as []meta.Answer, out []core.Action) []core.Action {
				for _, an := range as {
					rep.answers[an.Token] = an.Err
				}
				if sh.answered != nil {
					out = sh.answered(ctx, as, out)
				}
				return out
			},
		}
		rep.a = meta.NewRoundShard(sh.loops[i], sh.lg, cfg)
		st.AddRemote(metaOwner(i), i, uint32(rep.a.ID()))
		sh.loops[i].Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: metaRaftTimer})
	}
}

func metaOwner(i int) uint32 { return 1001 + uint32(i) }

func (sh *metaShards) start() {
	sh.s = sim.New(1, sim.Settings{PoolBytes: 1 << 20})
	node := sh.s.AddNode(sim.DefaultClock, nil, func(*sim.Node) {})
	sh.node, sh.pool = node.ID(), sh.sd.Pool()
	for i := range sh.loops {
		k := NewKernel(sh.s, node.ID())
		if i == 0 {
			sh.loops[i] = loop.New(loop.Config{Node: node.ID(), ID: 0, Pool: sim.NewPool(4<<20, 0)}, k, metaMachine{sh, 0}, metaDriver{sh: sh})
			Add(node, k, sh.loops[i])
			continue
		}
		dr, err := NewDisk(k, sh.sd.D, sh.pool)
		if err != nil {
			sh.t.Fatal(err)
		}
		sh.t.Cleanup(dr.Close)
		l := loop.New(loop.Config{Node: node.ID(), ID: 1, Pool: sh.pool}, k, metaStoreMachine{metaMachine{sh, 1}}, metaDriver{diskDriver{dr}, sh, 1})
		if err := l.AttachDisk(dr); err != nil {
			sh.t.Fatal(err)
		}
		sh.loops[i], sh.ring = l, dr
		sh.lg = l.AttachLog(nil, sh.cfg.Log, chunkstore.FirstLogIO, chunkstore.EndLogIO, 2)
		AddWithDisk(node, k, l, dr)
	}
}

// metaDriver hands a NetSend of loop i to the loop it names as a NetRecv, its buffer moved into
// that loop's pool, and its disk actions to its disk.
type metaDriver struct {
	diskDriver
	sh *metaShards
	i  int
}

func (d metaDriver) Execute(a *core.Action) {
	if a.Kind != core.ActNetSend {
		d.diskDriver.Execute(a)
		return
	}
	ev := core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{Node: d.sh.node, Loop: uint32(d.i)}, Msg: a.Msg}
	if b := a.Msg.Buf; b != core.NoBuf {
		from, to := d.sh.loops[d.i].Context().Pool, d.sh.loops[a.Peer.Loop].Context().Pool
		rb, err := to.Alloc(len(from.Bytes(b)))
		if err != nil {
			d.sh.t.Fatal(err)
		}
		copy(to.Bytes(rb), from.Bytes(b))
		from.Free(b)
		ev.Msg.Buf = rb
	}
	d.sh.loops[a.Peer.Loop].Inbox().Post(&ev)
}

func (sh *metaShards) mail(m *raftshard.Mail) {
	sh.lg.Send(m.Loop, loop.Mail{Shard: loop.ShardID(m.Shard), Kind: mailKinds[m.Kind], Owner: m.Owner, Ref: m.Ref, Blob: m.Blob, Data: m.Data, Pieces: m.Pieces, Base: m.Base, Share: m.Share, Bound: m.Bound})
}

func (sh *metaShards) Pool() core.BufPool     { return sh.sd.Pool() }
func (sh *metaShards) Blocks() uint64         { return sh.sd.Blocks() }
func (sh *metaShards) Execute(a *core.Action) { sh.ring.Execute(a) }
func (sh *metaShards) Complete([]core.Event) []core.Event {
	panic("looptest: the loop hands the store its completions")
}
func (sh *metaShards) Restart() { sh.sd.Restart() }

func (sh *metaShards) Serve(log *disklog.Log) {
	if sh.lg.Log() != log {
		sh.lg.Serve(log)
	}
}

func (sh *metaShards) Step(r *storetest.Rig) bool {
	sh.r = r
	return sh.s.Step()
}

// tick runs the replicas' clocks until each leads its group, then stops them.
func (sh *metaShards) tick() {
	sh.ticking = true
	for _, i := range sh.on {
		sh.loops[i].Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: metaRaftTimer})
	}
	for n := 0; ; n++ {
		led := true
		for _, i := range sh.on {
			s := sh.reps[i].a.Shard()
			led = led && s != nil && s.Raft().Leader() == 1
		}
		if led {
			break
		}
		if n == 1_000_000 || !sh.s.Step() {
			for _, i := range sh.on {
				a := sh.reps[i].a
				if a.Shard() != nil {
					st := a.Shard().Raft()
					sh.t.Logf("DBG %d err=%v lead=%d term=%d base=%+v", i, a.Err(), st.Leader(), st.Term(), a.Storage().Base())
				} else {
					sh.t.Logf("DBG %d not started err=%v", i, a.Err())
				}
			}
			sh.t.Fatal("the replicas never led")
		}
	}
	sh.ticking = false
	sh.r.Settle()
}

// propose has the replica on loop i propose n Txns, each putting a value of size bytes under a
// key of its own, and settles.
func (sh *metaShards) propose(i, n, size int) {
	rep := sh.reps[i]
	for range n {
		rep.next++
		rep.keys[rep.next] = []byte(fmt.Sprintf("\x01k%06d", rep.next))
		rep.pending = append(rep.pending, rep.next)
	}
	sh.loops[i].Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: metaTxnTimer, IO: core.IOID(size)})
	sh.r.Settle()
}

// check fails unless every Txn of the replica on loop i that was answered applied, and its state
// holds each applied Txn's value and no refused one's; a Txn a crash left unanswered may or may
// not have applied.
func (sh *metaShards) check(i int) {
	rep := sh.reps[i]
	tree := rep.a.Shard().State().Tree()
	refused := map[uint64]bool{}
	for _, tok := range rep.refused {
		refused[tok] = true
	}
	for tok, key := range rep.keys {
		v, ok := tree.Get(key)
		err, answered := rep.answers[tok]
		switch {
		case refused[tok] && ok:
			sh.t.Fatalf("replica %d applied Txn %d, refused overloaded", i, tok)
		case refused[tok]:
		case !answered:
		case err != nil:
			sh.t.Fatalf("replica %d answered Txn %d with %v", i, tok, err)
		case !ok || !bytes.HasPrefix(v, key):
			sh.t.Fatalf("replica %d holds %q under Txn %d's key, applied", i, v, tok)
		}
	}
}

type metaMachine struct {
	sh *metaShards
	i  int
}

func (m metaMachine) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	rep := m.sh.reps[m.i]
	switch {
	case ev.Kind == core.EvTimer && ev.Timer == metaRaftTimer:
		if m.sh.ticking && rep != nil {
			out = rep.a.OnEvent(ctx, ev, out)
		}
	case ev.Kind == core.EvTimer && ev.Timer == metaTxnTimer:
		for _, tok := range rep.pending {
			key := rep.keys[tok]
			t := meta.Txn{Ops: []meta.Op{meta.Put(key, append(append([]byte(nil), key...), make([]byte, int(ev.IO))...))}}
			var err error
			if out, err = rep.a.Txn(ctx, &t, tok, out); errors.Is(err, meta.ErrOverloaded) {
				rep.refused = append(rep.refused, tok)
			} else if err != nil && m.sh.joined && m.i == 1 {
				rep.errs = append(rep.errs, err)
			} else if err != nil {
				m.sh.t.Errorf("replica %d refused Txn %d: %v", m.i, tok, err)
			}
		}
		rep.pending = rep.pending[:0]
	case ev.Kind == core.EvNetRecv && rep != nil:
		out = rep.a.OnEvent(ctx, ev, out)
	case m.i == 1:
		m.sh.r.OnEvent(ctx, ev)
	}
	return out
}

type metaStoreMachine struct{ metaMachine }

func (m metaStoreMachine) OnLog(ctx *core.Context, _ *loop.Log, ev *disklog.Event, out []core.Action) []core.Action {
	m.sh.r.OnLog(ctx, ev)
	return out
}

func (m metaStoreMachine) OnReply(_ *loop.Log, r loop.Reply) {
	s := m.sh.r.S
	m.sh.replies = append(m.sh.replies, r)
	switch r.Kind {
	case loop.ReplyReplay:
		s.RemoteReplay(r.Owner, r.LSN)
	case loop.ReplyFull:
		s.RemoteFull()
	case loop.ReplyPiece:
		s.RemotePiece(r.Owner, r.LSN, r.Data, r.Root)
	case loop.ReplyRuns:
		s.RemoteRuns(r.Owner, r.LSN, r.Refs, r.Bounds)
	case loop.ReplyDead:
		s.RemoteDead(r.Owner, r.Refs)
	case loop.ReplyCosts:
		s.RemoteCosts(r.Owner, r.Costs)
	case loop.ReplyRemoved:
		s.RemoveRemote(r.Owner, r.LSN)
	}
}

// TestMetaShardsOnTheRoundDriverRecoverTheirState runs a MetaStore replica on the device loop and
// one on another loop, each the voter of a group of its own, through enough Txns that checkpoints
// flush their trees and compact their logs. Idle, each has asked for a checkpoint to name every
// merge step it installed, replied its runs as the cleaner weighs them, and reported dead the
// pieces its merges left out, which the log then drops; chunk writes move the replay floor past
// every blob they held when they went idle. Mailed to rewrite the runs in a segment, a replica
// rewrites them and asks for a checkpoint to name the rewrite. A restart restores each from the runs and blobs its
// store mails it, on the base a checkpoint compacted it to, with every Txn it answered, and it
// replies those runs as the cleaner weighs them before a checkpoint flushes it.
func TestMetaShardsOnTheRoundDriverRecoverTheirState(t *testing.T) {
	sh := newMetaShards(t, shardReplay(), 0, 1)
	sh.tick()
	for range 20 {
		for _, i := range sh.on {
			sh.propose(i, 8, 600)
		}
	}
	var gone []disklog.BlobRef
	for _, i := range sh.on {
		tree := sh.reps[i].a.Shard().State().Tree()
		if tree.StepUnnamed() {
			t.Fatalf("replica %d holds a merge step no manifest names", i)
		}
		var costs []disklog.RunCost
		for _, r := range sh.replies {
			if r.Owner == metaOwner(i) && r.Kind == loop.ReplyCosts {
				costs = r.Costs
			}
			if r.Owner == metaOwner(i) && r.Kind == loop.ReplyDead {
				gone = append(gone, r.Refs...)
			}
		}
		if want := tree.RunCosts(nil); len(want) == 0 || !reflect.DeepEqual(costs, want) {
			t.Fatalf("replica %d last replied runs %+v, its tree holds %+v", i, costs, want)
		}
	}
	if len(gone) == 0 {
		t.Fatal("no merge left a piece out")
	}
	idle := sh.r.S.Log().DurableLSN()
	for i := range 400 {
		sh.r.Write(storetest.Chunk(uint32(i%8)), 0, storetest.Fill(core.BlockSize, byte(i)), 0)
		sh.r.Settle()
	}
	if floor := sh.r.S.Log().ReplayFloor(); floor < idle {
		t.Fatalf("the idle replicas hold the replay floor at %d, before %d, where they went idle", floor, idle)
	}
	for _, p := range gone {
		if err := sh.r.S.Log().PiecesDead([]disklog.BlobRef{p}); err == nil {
			t.Fatalf("the log still holds piece %+v a merge left out", p)
		}
	}
	tree := sh.reps[0].a.Shard().State().Tree()
	seg := tree.RunCosts(nil)[0].Pieces[0].Segment
	sh.lg.Send(0, loop.Mail{Shard: sh.reps[0].a.ID(), Kind: loop.MailRewrite, Ref: disklog.BlobRef{Segment: seg}})
	sh.loops[1].Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: 1})
	sh.r.Settle()
	for _, c := range tree.RunCosts(nil) {
		if slices.ContainsFunc(c.Pieces, func(p disklog.BlobRef) bool { return p.Segment == seg }) || tree.StepUnnamed() {
			t.Fatalf("replica 0 holds run %+v in segment %d it was mailed to rewrite, a step unnamed %v", c, seg, tree.StepUnnamed())
		}
	}
	type state struct {
		applied, last uint64
		hs            raftpb.HardState
	}
	var before [2]state
	for _, i := range sh.on {
		if rep := sh.reps[i]; len(rep.answers) != len(rep.keys) {
			t.Fatalf("replica %d answered %d Txns of %d", i, len(rep.answers), len(rep.keys))
		}
		sh.check(i)
		s := sh.reps[i].a.Shard()
		applied, _ := s.Applied()
		last, _ := sh.reps[i].a.Storage().LastIndex()
		before[i] = state{applied, last, sh.reps[i].a.Storage().HardState()}
		if base := sh.reps[i].a.Storage().Base().Index; base <= 1 {
			t.Fatalf("replica %d never compacted: its base is %d", i, base)
		}
	}
	sh.replies = nil
	sh.r.Restart()
	for _, i := range sh.on {
		var first loop.ReplyKind
		for _, r := range sh.replies {
			if r.Owner == metaOwner(i) && (r.Kind == loop.ReplyCosts || r.Kind == loop.ReplyRuns) {
				first = r.Kind
				break
			}
		}
		if first != loop.ReplyCosts {
			t.Fatalf("replica %d replied its restored runs to the cleaner only after its first flush began", i)
		}
	}
	sh.tick()
	for _, i := range sh.on {
		s := sh.reps[i].a.Shard()
		if s == nil {
			t.Fatalf("replica %d never started after the restart: %v", i, sh.reps[i].a.Err())
		}
		last, _ := sh.reps[i].a.Storage().LastIndex()
		if base := sh.reps[i].a.Storage().Base().Index; base <= 1 || last < before[i].last {
			t.Fatalf("replica %d restored base %d and entries to %d, held entries to %d", i, base, last, before[i].last)
		}
		if applied, _ := s.Applied(); applied < before[i].applied {
			t.Fatalf("replica %d applied %d after the restart, %d before", i, applied, before[i].applied)
		}
		sh.check(i)
	}
	for _, i := range sh.on {
		sh.propose(i, 4, 600)
		sh.check(i)
	}
}

// TestAMetaShardHeldFullByAStuckCheckpoint has the store learn of a shard on another loop before
// that loop adds it, so every due checkpoint waits for its pieces, while a MetaStore replica on
// that loop takes Txns: once it retains its share it refuses them overloaded, applying none of
// them, and the store goes on serving. Once the shard is added the checkpoint ends, compacts the
// replica, and it takes Txns again; a restart finds every Txn it answered and none it refused.
func TestAMetaShardHeldFullByAStuckCheckpoint(t *testing.T) {
	sh, held, base := fillBehindAStuckCheckpoint(t)
	rep := sh.reps[0]
	term := rep.a.Shard().Raft().Term()
	sh.loops[0].Inbox().Post(&core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{Node: sh.node, Loop: 1}, Msg: appendFrom(t, rep.group, 2, term+5)})
	sh.r.Settle()
	if got := rep.a.Shard().Raft().Term(); got != term {
		t.Fatalf("the full replica took an append of term %d: its term is %d", term+5, got)
	}
	rr := addRaftRounds(sh.loops[0], sh.loops[0].Context().Pool, sh.lg, 2001, sh.maxBlob, 0)
	rr.reply = true
	if uint32(rr.id) != held {
		t.Fatalf("the held shard was added as %d, the store knows it as %d", rr.id, held)
	}
	sh.loops[0].Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: 1})
	sh.r.Settle()
	if got := rep.a.Storage().Base().Index; got <= base || rr.checkpointed == 0 {
		t.Fatalf("the replica's base is %d, %d before, with %d checkpoints ended", got, base, rr.checkpointed)
	}
	refused := len(rep.refused)
	sh.propose(0, 4, 900)
	if len(rep.refused) != refused {
		t.Fatal("the replica refused Txns once a checkpoint compacted it")
	}
	sh.check(0)
	sh.r.Restart()
	sh.tick()
	sh.check(0)
}

// fillBehindAStuckCheckpoint has the store of a replica on loop 0 learn of a shard on that loop
// that it never adds, and proposes Txns until the replica refuses some, overloaded; it returns the
// rig, the ShardID the store knows the shard by and the replica's base before.
func fillBehindAStuckCheckpoint(t *testing.T) (*metaShards, uint32, uint64) {
	sh := newMetaShards(t, shardReplay(), 0)
	sh.tick()
	rep := sh.reps[0]
	held := uint32(rep.a.ID()) + 1
	sh.r.S.AddRemote(2001, 0, held)
	sh.r.Settle()
	base := rep.a.Storage().Base().Index
	for n := 0; len(rep.refused) < 4; n++ {
		if n == 200 {
			t.Fatalf("200 rounds of 36 Txns, %d refused, with no checkpoint ending", len(rep.refused))
		}
		sh.propose(0, 36, 900)
		if !sh.r.S.Serving() {
			t.Fatalf("the store stopped: %v", sh.r.S.Err())
		}
	}
	if got := rep.a.Storage().Base().Index; got != base {
		t.Fatalf("the replica compacted to %d, from %d, while a checkpoint was held", got, base)
	}
	sh.check(0)
	return sh, held, base
}

// TestAMetaShardHeldFullRestartsWithItsCommittedTxns restarts the process while a stuck
// checkpoint holds a MetaStore replica full: the replica restores what no checkpoint compacted
// from the blobs its store mails it, every Txn it answered among them.
func TestAMetaShardHeldFullRestartsWithItsCommittedTxns(t *testing.T) {
	sh, _, _ := fillBehindAStuckCheckpoint(t)
	last, _ := sh.reps[0].a.Storage().LastIndex()
	sh.r.Restart()
	sh.tick()
	if got, _ := sh.reps[0].a.Storage().LastIndex(); got < last {
		t.Fatalf("the replica restarted with entries to %d, below the %d it held", got, last)
	}
	sh.check(0)
}

// appendFrom is a Raft append of group, from replica from to replica 1 in term, framed as the
// group's messages are: core.ProtoGroup, the group, a Raft kind byte, the message.
func appendFrom(t *testing.T, group, from, term uint64) core.Msg {
	return raftFrom(t, raftpb.Message{Type: raftpb.MsgApp, From: from, To: 1, Term: term, Index: 1, LogTerm: 1, Commit: 1}, group)
}

// raftFrom is m framed as group's messages are.
func raftFrom(t *testing.T, m raftpb.Message, group uint64) core.Msg {
	b := append(binary.LittleEndian.AppendUint64([]byte{core.ProtoGroup}, group), 0)
	b = append(b, make([]byte, m.Size())...)
	_, err := m.MarshalTo(b[10:])
	var msg core.Msg
	if err == nil {
		err = msg.SetInline(b)
	}
	if err != nil {
		t.Fatal(err)
	}
	return msg
}

// TestANewMetaShardCrashedBeforeItsBootRoundStartsOnRestart crashes the process before a new
// replica's boot round, its first, is written: the restart mails it no run and no blob, and it
// writes its group's boot state again, starts once that is durable and leads.
func TestANewMetaShardCrashedBeforeItsBootRoundStartsOnRestart(t *testing.T) {
	sh := newMetaShards(t, shardReplay(), 0)
	sh.sd.Restart()
	sh.r.Reopen()
	sh.tick()
	sh.propose(0, 4, 600)
	sh.check(0)
}

// TestAMetaShardNotYetStartedAnswersAnAskForItsPieces mails a new replica an ask for its pieces
// before its boot round is durable: it has no tree to flush yet and replies at once that it has
// written all it has, so the checkpoint does not wait on it.
func TestAMetaShardNotYetStartedAnswersAnAskForItsPieces(t *testing.T) {
	sh := newMetaShards(t, shardReplay(), 0)
	rep := sh.reps[0]
	sh.lg.Send(0, loop.Mail{Shard: rep.a.ID(), Kind: loop.MailFlush, Owner: metaOwner(0), Base: 7})
	sh.r.Settle()
	if rep.a.Shard() == nil {
		t.Fatal("the replica never started")
	}
	for _, r := range sh.replies {
		if r.Kind == loop.ReplyPiece && r.Owner == metaOwner(0) && r.LSN == 7 && r.Data == nil {
			return
		}
	}
	t.Fatalf("the replica never answered the ask for its pieces: replies %+v", sh.replies)
}

// TestAFullMetaShardGetsACheckpointFromAnIdleStore makes a MetaStore replica full with a share
// mail while its store is idle and holds entries it applied: it asks for a checkpoint, which
// compacts it.
func TestAFullMetaShardGetsACheckpointFromAnIdleStore(t *testing.T) {
	sh := newMetaShards(t, storetest.Config(), 0)
	sh.tick()
	sh.propose(0, 8, 600)
	rep := sh.reps[0]
	base := rep.a.Storage().Base().Index
	if applied, _ := rep.a.Shard().Applied(); applied <= base {
		t.Fatalf("the replica applied %d, its base already %d", applied, base)
	}
	sh.lg.Send(0, loop.Mail{Shard: rep.a.ID(), Kind: loop.MailShare, Share: 1})
	sh.loops[1].Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: 1})
	sh.r.Settle()
	if got := rep.a.Storage().Base().Index; got <= base {
		t.Fatalf("the full replica's base is %d, %d before", got, base)
	}
}

// TestAMetaShardThatJoinsWithEmptyStorageRestarts has a learner join a MetaStore group with empty
// storage, as a replacement does. The process crashes before the learner's boot round, its first,
// is durable, and the restart mails it nothing; it writes its boot state again and starts. A
// checkpoint then flushes its empty tree, marked with its base at index 0, before its leader's
// snapshot reaches it; it restarts from that run and catches up with every Txn its leader applied.
func TestAMetaShardThatJoinsWithEmptyStorageRestarts(t *testing.T) {
	sh := (&metaShards{t: t, cfg: shardReplay(), on: []int{0, 1}, joined: true}).format()
	sh.sd.Restart()
	sh.r.Reopen()
	learner := sh.reps[1]
	if learner.a.Shard() == nil {
		t.Fatalf("the learner never started after the crash: %v", learner.a.Err())
	}
	flushes := func() (n int) {
		for _, r := range sh.replies {
			if r.Kind == loop.ReplyRuns && r.Owner == metaOwner(1) {
				n++
			}
		}
		return n
	}
	log, named := sh.r.S.Log(), uint64(0)
	for i := 0; named == 0 || log.NamedLSN() < named; i++ {
		if i == 200 {
			t.Fatalf("200 chunk writes, %d checkpoints flushed the learner, and no segment header named one", flushes())
		}
		sh.r.Write(storetest.Chunk(uint32(i%8)), 0, storetest.Fill(4*core.BlockSize, byte(i)), 0)
		sh.r.Settle()
		if named == 0 && flushes() >= 2 {
			named = log.Manifest().LSN
		}
	}
	sh.r.Restart()
	learner = sh.reps[1]
	if learner.a.Shard() == nil {
		t.Fatalf("the learner never started after the restart: %v", learner.a.Err())
	}
	sh.tick()
	sh.propose(0, 4, 600)
	sh.check(0)
	tree := learner.a.Shard().State().Tree()
	for tok, key := range sh.reps[0].keys {
		if v, ok := tree.Get(key); !ok || !bytes.HasPrefix(v, key) {
			t.Fatalf("the learner holds %q under Txn %d's key, which its leader applied", v, tok)
		}
	}
}

// TestAFullMetaShardTakesEveryMessageButAnAppend sends a MetaStore replica held full by a stuck
// checkpoint a heartbeat of a later term: it drops appends alone, so the heartbeat reaches its
// Raft replica, which follows the sender.
func TestAFullMetaShardTakesEveryMessageButAnAppend(t *testing.T) {
	sh, _, _ := fillBehindAStuckCheckpoint(t)
	rep := sh.reps[0]
	term := rep.a.Shard().Raft().Term() + 5
	hb := raftpb.Message{Type: raftpb.MsgHeartbeat, From: 2, To: 1, Term: term}
	sh.loops[0].Inbox().Post(&core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{Node: sh.node, Loop: 1}, Msg: raftFrom(t, hb, rep.group)})
	sh.r.Settle()
	if got, lead := rep.a.Shard().Raft().Term(), rep.a.Shard().Raft().Leader(); got != term || lead != 2 {
		t.Fatalf("the full replica dropped a heartbeat of term %d: its term is %d, its leader %d", term, got, lead)
	}
}

// TestAFullMetaFollowerNamesItsLeader holds a learner full behind a stuck checkpoint: it refuses a
// Txn as any follower does, naming its leader, not overloaded.
func TestAFullMetaFollowerNamesItsLeader(t *testing.T) {
	sh := (&metaShards{t: t, cfg: shardReplay(), on: []int{0, 1}, joined: true}).format()
	sh.tick()
	sh.propose(0, 8, 600)
	learner := sh.reps[1]
	sh.r.S.AddRemote(2001, 0, uint32(sh.reps[0].a.ID())+1)
	sh.r.Settle()
	sh.propose(0, 8, 600)
	sh.lg.Send(1, loop.Mail{Shard: learner.a.ID(), Kind: loop.MailShare, Share: 1})
	sh.loops[1].Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: 1})
	sh.r.Settle()
	sh.propose(1, 1, 600)
	var nl meta.NotLeader
	if len(learner.refused) != 0 || len(learner.errs) != 1 || !errors.As(learner.errs[0], &nl) || nl.Leader != 1 || nl.Node != sh.node {
		t.Fatalf("the full learner refused %d Txns overloaded, and %v", len(learner.refused), learner.errs)
	}
}

// TestANewMetaShardShortOfBuffersWritesItsBootRoundOnItsNextEvent has a new replica's loop pool
// hold no buffer when its boot round is ready, and its store is idle, so mails it nothing: once
// buffers return, the next message to the replica, not yet started, has the round written, and
// the replica starts and leads.
func TestANewMetaShardShortOfBuffersWritesItsBootRoundOnItsNextEvent(t *testing.T) {
	sh := newMetaShards(t, shardReplay())
	pool := sh.loops[0].Context().Pool
	var held []core.IOBuf
	for {
		b, err := pool.Alloc(core.BlockSize)
		if err != nil {
			break
		}
		held = append(held, b)
	}
	sh.on = []int{0}
	sh.opened(sh.r.S, false)
	sh.r.Settle()
	rep := sh.reps[0]
	if rep.a.Shard() != nil {
		t.Fatal("the replica started with no buffer for its boot round")
	}
	for _, b := range held {
		pool.Free(b)
	}
	sh.loops[0].Inbox().Post(&core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{Node: sh.node, Loop: 1}, Msg: appendFrom(t, rep.group, 2, 1)})
	sh.r.Settle()
	if rep.a.Shard() == nil {
		t.Fatalf("the replica never started once buffers returned: %v", rep.a.Err())
	}
	sh.tick()
}
