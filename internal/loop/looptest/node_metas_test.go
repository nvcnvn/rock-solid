package looptest

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"testing"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/chunkstore/storetest"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/lease"
	"github.com/nvcnvn/rock-solid/internal/loop"
	"github.com/nvcnvn/rock-solid/internal/lsm"
	"github.com/nvcnvn/rock-solid/internal/meta"
	"github.com/nvcnvn/rock-solid/internal/node"
	"github.com/nvcnvn/rock-solid/internal/raftshard"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// nodeMetas runs a node.Device on a device loop over a simulated disk, as the storage node's process
// does, hosting MetaStore replicas, each the one voter of its group; a client's Txns reach the loop
// over the wire from peer, and the device's answers are kept by token, with the group each token
// was sent to. Raft timers reach the device only while ticking, so the loop settles.
type nodeMetas struct {
	t       *testing.T
	sd      *storetest.SimDisk
	s       *sim.Scheduler
	l       *loop.Loop
	m       *metaDevice
	node    uint32
	blocks  uint64
	ticking bool
	answers map[uint64]error
	keys    map[uint64][]byte
	groups  map[uint64]uint64
	next    uint64
	gone    int
	// askFirst has start ask about replica 1 as the device starts, before its store serves.
	askFirst bool
}

const nodeMetaGroup = 10

var nodeMetaPeer = core.NodeID{Node: 77}

// nodeMetaAsker is a node that asks the device about replicas; the driver counts its answers Gone.
var nodeMetaAsker = core.NodeID{Node: 78}

type metaDevice struct {
	deviceOnLoop
	nm *nodeMetas
}

func (m *metaDevice) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	if ev.Kind == core.EvTimer && ev.Timer >= node.MetaTimer && !m.nm.ticking {
		return out
	}
	return m.deviceOnLoop.OnEvent(ctx, ev, out)
}

func (m *metaDevice) OnReply(_ *loop.Log, r loop.Reply) { m.d.OnReply(r) }

// metaDeviceDriver keeps the device's answers to the client and hands its disk actions to its disk.
type metaDeviceDriver struct {
	diskDriver
	nm *nodeMetas
}

func (d metaDeviceDriver) Execute(a *core.Action) {
	if a.Kind != core.ActNetSend {
		d.diskDriver.Execute(a)
		return
	}
	pool := d.nm.l.Context().Pool
	var an meta.Answer
	if a.Peer == nodeMetaPeer && meta.DecodeAnswer(pool, &a.Msg, &an) == nil {
		d.nm.answers[an.Token] = an.Err
	}
	if in := a.Msg.Inline(); a.Peer == nodeMetaAsker && len(in) > 9 && in[9] == raftKindGone {
		d.nm.gone++
	}
	if a.Msg.Buf != core.NoBuf {
		pool.Free(a.Msg.Buf)
	}
}

// newNodeMetas starts a process on a new disk whose device restores the replicas of groups.
func newNodeMetas(t *testing.T, groups ...uint64) *nodeMetas {
	nm := &nodeMetas{t: t, sd: storetest.NewSimDiskOf(1, storetest.DiskBytes), answers: map[uint64]error{}, keys: map[uint64][]byte{}, groups: map[uint64]uint64{}}
	var metas []meta.RoundConfig
	for _, g := range groups {
		metas = append(metas, nm.replicaOf(g))
	}
	nm.start(metas)
	return nm
}

// replica is the configuration of nodeMetaGroup's one voter.
func (nm *nodeMetas) replica() meta.RoundConfig { return nm.replicaOf(nodeMetaGroup) }

// replicaOf is the configuration of group's one voter, booting from its configuration.
func (nm *nodeMetas) replicaOf(group uint64) meta.RoundConfig {
	storage := raftshard.NewStorage()
	cs := raftpb.ConfState{Voters: []uint64{1}}
	if err := storage.ApplySnapshot(raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 1, Term: 1, ConfState: cs}}); err != nil {
		nm.t.Fatal(err)
	}
	return meta.RoundConfig{
		Shard: meta.Config{
			Raft: raftshard.Config{Group: group, ID: 1, Storage: storage, Seed: 1,
				Peers:  []raftshard.Peer{{ID: 1, Node: core.NodeID{Node: nm.node, Loop: 1}}},
				Holder: lease.NewHolder(lease.DefaultConfig), Grantor: lease.NewGrantor(lease.DefaultConfig, 1, lease.Time{})},
			Tree: lsm.Config{MaxPiece: 4 << 10, StepBytes: 4 << 10, Check: true},
		},
		Owner: 991 + uint32(group),
	}
}

// start starts a process whose device restores metas, and settles.
func (nm *nodeMetas) start(metas []meta.RoundConfig) {
	nm.s = sim.New(1, sim.Settings{PoolBytes: 1 << 20})
	n := nm.s.AddNode(sim.DefaultClock, nil, func(*sim.Node) {})
	nm.node = n.ID()
	k := NewKernel(nm.s, n.ID())
	dr, err := NewDisk(k, nm.sd.D, nm.sd.Pool())
	if err != nil {
		nm.t.Fatal(err)
	}
	nm.t.Cleanup(dr.Close)
	cfg := shardReplay()
	nm.m = &metaDevice{nm: nm, deviceOnLoop: deviceOnLoop{cfg: node.DeviceConfig{Device: 7, Blocks: nm.sd.Blocks(), Loop: 1, Store: cfg, Seed: 1}}}
	if nm.blocks != 0 {
		nm.m.cfg.Blocks = nm.blocks
	}
	nm.l = loop.New(loop.Config{Node: n.ID(), ID: 1, Pool: nm.sd.Pool()}, k, nm.m, metaDeviceDriver{diskDriver{dr}, nm})
	if err := nm.l.AttachDisk(dr); err != nil {
		nm.t.Fatal(err)
	}
	nm.m.lg = nm.l.AttachLog(nil, cfg.Log, chunkstore.FirstLogIO, chunkstore.EndLogIO, 2)
	nm.m.cfg.Host, nm.m.cfg.Rounds = nm.l, nm.m.lg
	for _, c := range metas {
		c.Shard.Raft.Peers = []raftshard.Peer{{ID: 1, Node: core.NodeID{Node: n.ID(), Loop: 1}}}
		c.Shard.Raft.Holder, c.Shard.Raft.Grantor = lease.NewHolder(lease.DefaultConfig), lease.NewGrantor(lease.DefaultConfig, 1, lease.Time{})
		nm.m.cfg.Metas = append(nm.m.cfg.Metas, c)
	}
	AddWithDisk(n, k, nm.l, dr)
	nm.l.Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: 1})
	if nm.askFirst {
		nm.postAsk(1)
	}
	nm.settle()
}

func (nm *nodeMetas) settle() {
	for nm.s.Step() {
	}
}

// lead runs the clocks of the replicas of groups, hosted in that order, until each leads, then
// stops them; with no groups, nodeMetaGroup's.
func (nm *nodeMetas) lead(groups ...uint64) {
	if len(groups) == 0 {
		groups = []uint64{nodeMetaGroup}
	}
	nm.ticking = true
	for i := range groups {
		nm.l.Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: node.MetaTimer + core.TimerID(i)})
	}
	for n := 0; ; n++ {
		led := true
		for _, g := range groups {
			a := nm.m.d.Meta(g)
			led = led && a != nil && a.Shard() != nil && a.Shard().Raft().Leader() == 1
		}
		if led {
			break
		}
		if n == 1_000_000 || !nm.s.Step() {
			nm.t.Fatal("the replica never led")
		}
	}
	nm.ticking = false
	nm.settle()
}

// send sends n Txns to nodeMetaGroup's replica, as sendTo.
func (nm *nodeMetas) send(n, size int) { nm.sendTo(nodeMetaGroup, n, size) }

// sendTo sends n Txns for group to the device at once, each putting a value of size bytes under a
// key of its own, and settles.
func (nm *nodeMetas) sendTo(group uint64, n, size int) {
	nm.post(group, n, size)
	nm.settle()
}

// post posts n Txns for group to the device's loop, as sendTo, without settling.
func (nm *nodeMetas) post(group uint64, n, size int) {
	pool := nm.l.Context().Pool
	for range n {
		nm.next++
		key := []byte(fmt.Sprintf("\x01k%06d", nm.next))
		nm.keys[nm.next], nm.groups[nm.next] = key, group
		t := meta.Txn{Ops: []meta.Op{meta.Put(key, append(append([]byte(nil), key...), make([]byte, size)...))}}
		ev := core.Event{Kind: core.EvNetRecv, Peer: nodeMetaPeer}
		if err := meta.EncodeTxn(pool, group, nm.next, &t, &ev.Msg); err != nil {
			nm.t.Fatal(err)
		}
		if !nm.l.Inbox().Post(&ev) {
			nm.t.Fatal("the loop's inbox is full")
		}
	}
}

// check fails unless the state of the replica of each Txn's group holds the value of every Txn
// answered as applied.
func (nm *nodeMetas) check() {
	applied := 0
	for tok, key := range nm.keys {
		err, answered := nm.answers[tok]
		if !answered || err != nil {
			continue
		}
		tree := nm.m.d.Meta(nm.groups[tok]).Shard().State().Tree()
		applied++
		if v, ok := tree.Get(key); !ok || !bytes.HasPrefix(v, key) {
			nm.t.Fatalf("the replica holds %q under Txn %d's key, applied", v, tok)
		}
	}
	if applied == 0 {
		nm.t.Fatal("no Txn applied")
	}
}

// A device hosts a MetaStore replica on its loop's round driver: Txns sent to it over the wire are
// answered, more than its server holds at once and enough that its store checkpoints the
// replica's tree through the mail it sends and the replies it takes, and a restart that names the
// replica restores every Txn answered.
func TestADeviceHostsAMetaStoreReplicaAcrossARestart(t *testing.T) {
	nm := newNodeMetas(t)
	cfg := nm.replica()
	if err := nm.m.d.AddMeta(cfg); err != nil {
		t.Fatal(err)
	}
	nm.lead()
	for range 60 {
		nm.send(20, 900)
	}
	for tok := range nm.keys {
		if err, ok := nm.answers[tok]; !ok || err != nil {
			t.Fatalf("Txn %d answered %v, %v", tok, ok, err)
		}
	}
	if nm.m.d.Store().Log().Manifest().LSN == 0 {
		t.Fatal("no checkpoint ended")
	}
	nm.check()
	if err := nm.m.d.AddMeta(cfg); err != node.ErrHosted {
		t.Fatalf("a second replica of the group was hosted: %v", err)
	}

	nm.sd.Restart()
	nm.start([]meta.RoundConfig{cfg})
	nm.lead()
	nm.check()
	nm.send(1, 10)
	if err := nm.answers[nm.next]; err != nil {
		t.Fatalf("the restored replica answered %v", err)
	}
}

// A Txn for a group the device hosts no replica of is answered NotLeader over the wire.
func TestADeviceAnswersATxnForAGroupItDoesNotHost(t *testing.T) {
	nm := newNodeMetas(t)
	nm.send(1, 10)
	if _, ok := nm.answers[1].(meta.NotLeader); !ok {
		t.Fatalf("the device answered %v", nm.answers[1])
	}
}

// The Txns a server hands a replica on the round driver while its round is in flight gather
// unanswered, which the disk does not bound; past the server's Held of them it refuses the next
// with ErrOverloaded, over the wire.
func TestTheServerBoundsTheTxnsAReplicaGathers(t *testing.T) {
	sh := newMetaShards(t, shardReplay(), 1)
	sh.tick()
	sv := meta.NewServer()
	sv.Held = 2
	ctx := sh.loops[1].Context()
	client := core.NodeID{Node: 99}
	var acts []core.Action
	for tok := uint64(1); tok <= 3; tok++ {
		ev := core.Event{Kind: core.EvNetRecv, Peer: client}
		txn := meta.Txn{Ops: []meta.Op{meta.Put([]byte(fmt.Sprintf("\x01k%d", tok)), []byte("v"))}}
		if err := meta.EncodeTxn(ctx.Pool, sh.reps[1].group, tok, &txn, &ev.Msg); err != nil {
			t.Fatal(err)
		}
		acts = sv.ServeRound(ctx, &ev, sh.reps[1].a, acts)
	}
	var refused []uint64
	for _, a := range acts {
		var an meta.Answer
		if a.Kind == core.ActNetSend && a.Peer == client && meta.DecodeAnswer(ctx.Pool, &a.Msg, &an) == nil {
			if an.Err != meta.ErrOverloaded {
				t.Fatalf("Txn %d answered %v", an.Token, an.Err)
			}
			refused = append(refused, an.Token)
		}
		if a.Kind == core.ActNetSend && a.Msg.Buf != core.NoBuf {
			ctx.Pool.Free(a.Msg.Buf)
		}
	}
	if len(refused) != 1 || refused[0] != 3 {
		t.Fatalf("the server refused %v", refused)
	}
}

// A device hosts the replicas of two groups: each leads on its own Raft timer and takes the Txns
// of its group alone.
func TestADeviceHostsTheReplicasOfTwoGroups(t *testing.T) {
	nm := newNodeMetas(t)
	for _, g := range []uint64{nodeMetaGroup, nodeMetaGroup + 1} {
		if err := nm.m.d.AddMeta(nm.replicaOf(g)); err != nil {
			t.Fatal(err)
		}
	}
	nm.lead(nodeMetaGroup, nodeMetaGroup+1)
	nm.sendTo(nodeMetaGroup+1, 5, 10)
	nm.sendTo(nodeMetaGroup, 5, 10)
	for tok := range nm.keys {
		if err, ok := nm.answers[tok]; !ok || err != nil {
			t.Fatalf("Txn %d to group %d answered %v, %v", tok, nm.groups[tok], ok, err)
		}
	}
	nm.check()
}

// A device whose store finds no log formats its disk, which takes the replicas it restored with
// the log: their groups are hosted again as new replicas.
func TestADeviceThatFormatsHostsTheGroupsItRestoredAgain(t *testing.T) {
	nm := newNodeMetas(t, nodeMetaGroup)
	if err := nm.m.d.AddMeta(nm.replica()); err != nil {
		t.Fatalf("the formatted device refused the group it had restored: %v", err)
	}
	nm.lead()
	nm.send(1, 10)
	if err, ok := nm.answers[nm.next]; !ok || err != nil {
		t.Fatalf("the new replica answered %v, %v", ok, err)
	}
}

// A disk whose log was formatted at another size than the device has now, as a grown volume's,
// fails at open: the replicas it restores took the blob bound of a log of the device's size.
func TestADeviceFailsALogFormattedAtAnotherSize(t *testing.T) {
	nm := newNodeMetas(t)
	nm.sd.Restart()
	nm.blocks = 2 * nm.sd.Blocks()
	nm.start(nil)
	if nm.m.failed != 1 || nm.m.opened != 0 {
		t.Fatalf("the device opened %d times and failed %d", nm.m.opened, nm.m.failed)
	}
}

// A replica whose disk fails its round answers each Txn it holds dropped, as it applies nothing
// more, and refuses the next NotLeader.
func TestAReplicaWhoseRoundFailsAnswersTheTxnsItHolds(t *testing.T) {
	nm := newNodeMetas(t)
	if err := nm.m.d.AddMeta(nm.replica()); err != nil {
		t.Fatal(err)
	}
	nm.lead()
	nm.sd.D.Inject(sim.Fault{Effect: sim.FailIO, When: sim.OnKind(core.ActDiskWrite)})
	nm.send(8, 10)
	dropped := 0
	for tok := range nm.keys {
		err, ok := nm.answers[tok]
		var nl meta.NotLeader
		switch {
		case !ok:
			t.Fatalf("Txn %d was never answered", tok)
		case errors.Is(err, raft.ErrProposalDropped):
			dropped++
		case !errors.As(err, &nl):
			t.Fatalf("Txn %d answered %v", tok, err)
		}
	}
	if dropped == 0 {
		t.Fatal("no Txn was proposed before the round failed")
	}
	nm.send(1, 10)
	if _, ok := nm.answers[nm.next].(meta.NotLeader); !ok {
		t.Fatalf("the failed replica answered %v", nm.answers[nm.next])
	}
}

// A leader transferring its leadership drops each Txn proposed to it: the server answers one,
// though the replica answers it within the call that proposes it, and holds it no more.
func TestAServerAnswersATxnItsLeaderDropsMidTransfer(t *testing.T) {
	sh := (&metaShards{t: t, cfg: shardReplay(), on: []int{0, 1}, joined: true}).format()
	sv := meta.NewServer()
	sv.Held, sh.answered = 1, sv.Reply
	sh.tick()
	rep := sh.reps[0]
	rs := rep.a.Shard().Raft()
	rs.SetPlan([]raftshard.Peer{{ID: 1, Node: core.NodeID{Node: sh.node, Loop: 0}}, {ID: 2, Node: core.NodeID{Node: sh.node, Loop: 1}}})
	for i := 0; !slices.Contains(rep.a.Shard().Report(nil).Voters, 2); i++ {
		if i == 50 {
			t.Fatal("the learner was never promoted")
		}
		sh.propose(0, 1, 10)
	}
	ctx := sh.loops[0].Context()
	tr := raftpb.Message{Type: raftpb.MsgTransferLeader, From: 2, To: 1, Term: rs.Term()}
	ev := core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{Node: sh.node, Loop: 1}, Msg: raftFrom(t, tr, rep.group)}
	acts := rep.a.OnEvent(ctx, &ev, nil)
	if rs.Leader() != 1 {
		t.Fatal("the leader stepped down at the transfer")
	}
	client := core.NodeID{Node: 99}
	for tok := uint64(1); tok <= 2; tok++ {
		acts = serveTxn(t, sv, ctx, rep, client, tok, acts)
	}
	if got := answersTo(ctx, client, acts); len(got) != 2 || !errors.Is(got[1], raft.ErrProposalDropped) || !errors.Is(got[2], raft.ErrProposalDropped) {
		t.Fatalf("the server answered %v", got)
	}
}

// A replica stopped by a failed round refuses a Txn NotLeader, which its router sends elsewhere,
// even while the server holds as many of its group's Txns as it bounds.
func TestAServerRefusesATxnForAFailedReplicaNotLeader(t *testing.T) {
	sh := newMetaShards(t, shardReplay(), 1)
	sh.tick()
	sv := meta.NewServer()
	sv.Held = 1
	rep := sh.reps[1]
	ctx := sh.loops[1].Context()
	client := core.NodeID{Node: 99}
	sh.sd.D.Inject(sim.Fault{Effect: sim.FailIO, When: sim.OnKind(core.ActDiskWrite)})
	acts := serveTxn(t, sv, ctx, rep, client, 1, nil)
	sh.r.Settle()
	if rep.a.Err() == nil {
		t.Fatal("the replica's round did not fail")
	}
	acts = serveTxn(t, sv, ctx, rep, client, 2, acts)
	if _, ok := answersTo(ctx, client, acts)[2].(meta.NotLeader); !ok {
		t.Fatalf("the server answered %v", answersTo(ctx, client, acts))
	}
}

// serveTxn has sv serve a Txn from client under tok, putting a key of its own to rep's group.
func serveTxn(t *testing.T, sv *meta.Server, ctx *core.Context, rep *metaRep, client core.NodeID, tok uint64, acts []core.Action) []core.Action {
	ev := core.Event{Kind: core.EvNetRecv, Peer: client}
	txn := meta.Txn{Ops: []meta.Op{meta.Put([]byte(fmt.Sprintf("\x01s%d", tok)), []byte("v"))}}
	if err := meta.EncodeTxn(ctx.Pool, rep.group, tok, &txn, &ev.Msg); err != nil {
		t.Fatal(err)
	}
	return sv.ServeRound(ctx, &ev, rep.a, acts)
}

// answersTo is the answer to each Txn of client's that acts send, by token; it frees every message.
func answersTo(ctx *core.Context, client core.NodeID, acts []core.Action) map[uint64]error {
	got := map[uint64]error{}
	for i := range acts {
		a := &acts[i]
		if a.Kind != core.ActNetSend {
			continue
		}
		var an meta.Answer
		if a.Peer == client && meta.DecodeAnswer(ctx.Pool, &a.Msg, &an) == nil {
			got[an.Token] = an.Err
		}
		if a.Msg.Buf != core.NoBuf {
			ctx.Pool.Free(a.Msg.Buf)
			a.Msg.Buf = core.NoBuf
		}
	}
	return got
}

// raftKindGone is raftshard's message kind of an answer Gone, raftKindAsk of the ask it answers.
const (
	raftKindAsk  = 5
	raftKindGone = 6
)

// ask sends the device an ask, from nodeMetaAsker, about replica id of the group, and settles.
func (nm *nodeMetas) ask(id uint64) {
	nm.postAsk(id)
	nm.settle()
}

func (nm *nodeMetas) postAsk(id uint64) {
	body := binary.LittleEndian.AppendUint64([]byte{core.ProtoGroup}, nodeMetaGroup)
	body = append(body, raftKindAsk)
	for _, v := range []uint64{2, id, 0, 0, 0} {
		body = binary.LittleEndian.AppendUint64(body, v)
	}
	ev := core.Event{Kind: core.EvNetRecv, Peer: nodeMetaAsker}
	if err := ev.Msg.SetInline(body); err != nil {
		nm.t.Fatal(err)
	}
	nm.l.Inbox().Post(&ev)
}

// A restart restores the MetaStore replicas its disk's manifest names, not those the device has
// settings for: a replica that answered Txns before a crash comes back holding every one, and
// another group's settings host nothing. A disk holding a replica of a group the device has no
// settings for leaves the device failed rather than forget it. An ask about the replica before the
// store serves, so before it is restored, is not answered Gone.
func TestARestartRestoresTheReplicasItsDiskHolds(t *testing.T) {
	nm := newNodeMetas(t)
	cfg := nm.replica()
	if err := nm.m.d.AddMeta(cfg); err != nil {
		t.Fatal(err)
	}
	nm.lead()
	nm.send(20, 100)
	other := nm.replica()
	other.Shard.Raft.Group, other.Owner = nodeMetaGroup+1, 1002

	nm.sd.Crash(core.NewRand(3))
	nm.askFirst = true
	nm.start([]meta.RoundConfig{other, cfg})
	nm.askFirst = false
	if nm.m.d.Meta(other.Shard.Raft.Group) != nil {
		t.Fatal("the device hosted a replica its disk does not hold")
	}
	if nm.gone != 0 {
		t.Fatal("the device answered an ask about a replica it holds Gone before its store served")
	}
	nm.lead()
	nm.check()

	nm.sd.Restart()
	nm.start([]meta.RoundConfig{other})
	if nm.m.d.Store() != nil {
		t.Fatal("a disk holding a replica the device has no settings for served")
	}
}

// A new replica starts, and so can acknowledge anything, only once a durable manifest names it,
// which a restart then restores it from: crashed at every step from its add to its start, the
// disk brings it back whenever it had started, and otherwise may not, its device then answering
// an ask about it Gone.
func TestANewReplicaStartsOnlyOnceARestartWouldRestoreIt(t *testing.T) {
	lost, started := 0, 0
	for k := 0; started < 3; k++ {
		nm := newNodeMetas(t)
		if err := nm.m.d.AddMeta(nm.replica()); err != nil {
			t.Fatal(err)
		}
		nm.ticking = true
		nm.l.Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: node.MetaTimer})
		for range k {
			if !nm.s.Step() {
				break
			}
		}
		up := nm.m.d.Meta(nodeMetaGroup).Shard() != nil
		nm.ticking = false
		nm.sd.Crash(core.NewRand(uint64(k)))
		nm.start([]meta.RoundConfig{nm.replica()})
		restored := nm.m.d.Meta(nodeMetaGroup) != nil
		switch {
		case up && !restored:
			t.Fatalf("crashed after %d steps, a replica that had started was not restored", k)
		case up:
			started++
		case !restored:
			lost++
			nm.ask(1)
			if nm.gone != 1 {
				t.Fatalf("crashed after %d steps, the device answered an ask about a replica it does not hold Gone %d times", k, nm.gone)
			}
		}
	}
	if lost == 0 {
		t.Fatal("no crash came before a manifest named the replica")
	}
}
