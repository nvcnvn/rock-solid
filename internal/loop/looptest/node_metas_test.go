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
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/lease"
	"github.com/nvcnvn/rock-solid/internal/loop"
	"github.com/nvcnvn/rock-solid/internal/lsm"
	"github.com/nvcnvn/rock-solid/internal/meta"
	"github.com/nvcnvn/rock-solid/internal/node"
	"github.com/nvcnvn/rock-solid/internal/ops"
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
	// reads are the device's answers to the client's Reads, in order.
	reads  []meta.ReadAnswer
	keys   map[uint64][]byte
	groups map[uint64]uint64
	next   uint64
	gone   int
	// removing is the group whose replica the device removes at nodeMetaRemove, removed whether
	// that ran and removeErr what RemoveMeta returned.
	removing  uint64
	removed   bool
	removeErr error
	// slots is the number of the device's replica timer slots that a timer fired in.
	slots core.TimerID
	// stall drops each piece the replica of owner stallOwner replies for a checkpoint, which then
	// never ends; stalled counts them.
	stall      bool
	stallOwner uint32
	stalled    int
	// askFirst has start ask about replica 1 as the device starts, before its store serves.
	askFirst bool
	// cr is the crash run's, nil elsewhere: the store's settings, a voter on loop 0 and what it
	// watches the device by.
	cr *metaCrash
	// taking is what the device takes at nodeMetaTake, as from the node's loop, and reported the
	// replicas it reported then.
	taking   node.Output
	reported []meta.ReplicaReport
	// asked are the loops the device's chunk replica asked for a lease since its last take.
	asked []core.NodeID
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
		if m.nm.cr != nil {
			m.nm.cr.parked = append(m.nm.cr.parked, ev.Timer)
		}
		return out
	}
	if ev.Kind == core.EvTimer && ev.Timer >= node.MetaTimer {
		m.nm.slots = max(m.nm.slots, ev.Timer-node.MetaTimer+1)
	}
	if ev.Kind == core.EvTimer && ev.Timer == nodeMetaTake {
		m.out.Reset()
		m.d.Take(ctx, &m.nm.taking, &m.out)
		m.nm.reported = slices.Clone(m.out.Metas)
		m.nm.asked = nil
		return m.done(out)
	}
	if ev.Kind == core.EvTimer && ev.Timer == nodeMetaRemove {
		m.out.Reset()
		m.nm.removed, m.nm.removeErr = true, m.d.RemoveMeta(ctx, m.nm.removing, &m.out)
		return m.done(out)
	}
	if m.nm.cr != nil && ev.Kind == core.EvTimer && ev.Timer == crashWriteTimer {
		m.out.Reset()
		m.nm.cr.write(ctx, ev, &m.out)
		return m.done(out)
	}
	fresh := m.d == nil
	out = m.deviceOnLoop.OnEvent(ctx, ev, out)
	if fresh && m.nm.cr != nil {
		m.nm.cr.opened()
	}
	return out
}

func (m *metaDevice) OnReply(_ *loop.Log, r loop.Reply) {
	if m.nm.stall && r.Kind == loop.ReplyPiece && r.Owner == m.nm.stallOwner {
		m.nm.stalled++
		return
	}
	if m.nm.cr != nil {
		m.nm.cr.replied(r)
	}
	m.d.OnReply(r)
}

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
	if d.nm.cr != nil && d.nm.cr.forward(a, 1) {
		return
	}
	var ra meta.ReadAnswer
	if a.Peer == nodeMetaPeer && meta.DecodeReadAnswer(pool, &a.Msg, &ra) == nil {
		for i, kv := range ra.KVs {
			ra.KVs[i] = meta.KV{Key: bytes.Clone(kv.Key), Value: bytes.Clone(kv.Value)}
		}
		d.nm.reads = append(d.nm.reads, ra)
	}
	var an meta.Answer
	if a.Peer == nodeMetaPeer && meta.DecodeAnswer(pool, &a.Msg, &an) == nil {
		d.nm.answers[an.Token] = an.Err
		if d.nm.cr != nil && an.Err == nil {
			d.nm.cr.answered(an.Token)
		}
	}
	var lr chunkstore.LeaseRequest
	if chunkstore.DecodeLeaseRequest(a.Msg.Inline(), 0, &lr) == nil && !slices.Contains(d.nm.asked, a.Peer) {
		d.nm.asked = append(d.nm.asked, a.Peer)
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
	return meta.RoundConfig{
		Shard: meta.Config{
			Raft: raftshard.Config{Group: group, ID: 1, Storage: bootAt(nm.t, 1), Seed: 1,
				Peers:  []raftshard.Peer{{ID: 1, Node: core.NodeID{Node: nm.node, Loop: 1}}},
				Holder: lease.NewHolder(lease.DefaultConfig), Grantor: lease.NewGrantor(lease.DefaultConfig, 1, lease.Time{})},
			Tree: lsm.Config{MaxPiece: 4 << 10, StepBytes: 4 << 10, Check: true},
		},
		Owner: 991 + uint32(group),
	}
}

// metaTemplate is the settings a device gives the replicas its plans place, its tree's and leases.
func metaTemplate() meta.RoundConfig {
	return meta.RoundConfig{Shard: meta.Config{
		Raft: raftshard.Config{Seed: 1, Holder: lease.NewHolder(lease.DefaultConfig), Grantor: lease.NewGrantor(lease.DefaultConfig, 1, lease.Time{})},
		Tree: lsm.Config{MaxPiece: 4 << 10, StepBytes: 4 << 10, Check: true},
	}}
}

// bootAt is the storage of a group's one voter whose configuration is at index.
func bootAt(t *testing.T, index uint64) *raftshard.Storage {
	return bootOf(t, index, raftpb.ConfState{Voters: []uint64{1}})
}

// bootOf is the storage of a group's voter whose configuration cs is at index.
func bootOf(t *testing.T, index uint64, cs raftpb.ConfState) *raftshard.Storage {
	storage := raftshard.NewStorage()
	if err := storage.ApplySnapshot(raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: index, Term: 1, ConfState: cs}}); err != nil {
		t.Fatal(err)
	}
	return storage
}

// start starts a process whose device restores metas, and settles.
func (nm *nodeMetas) start(metas []meta.RoundConfig) {
	nm.boot(metas)
	nm.settle()
}

// boot starts a process whose device restores metas, without settling.
func (nm *nodeMetas) boot(metas []meta.RoundConfig) {
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
	if nm.cr != nil {
		cfg = nm.cr.cfg
	}
	nm.m = &metaDevice{nm: nm, deviceOnLoop: deviceOnLoop{cfg: node.DeviceConfig{Device: 7, Blocks: nm.sd.Blocks(), Loop: 1, Store: cfg, Seed: 1, Meta: metaTemplate()}}}
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
		c.Shard.Raft.Peers = slices.Clone(c.Shard.Raft.Peers)
		for i := range c.Shard.Raft.Peers {
			c.Shard.Raft.Peers[i].Node.Node = n.ID()
		}
		c.Shard.Raft.Holder, c.Shard.Raft.Grantor = lease.NewHolder(lease.DefaultConfig), lease.NewGrantor(lease.DefaultConfig, 1, lease.Time{})
		nm.m.cfg.Metas = append(nm.m.cfg.Metas, c)
	}
	AddWithDisk(n, k, nm.l, dr)
	if nm.cr != nil {
		nm.cr.boot(n)
	}
	nm.l.Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: 1})
	if nm.askFirst {
		nm.postAsk(1)
	}
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

// A Read of a group the device hosts the leader of is answered from its state over the wire, and
// one of a group it hosts no replica of NotLeader naming none.
func TestADeviceServesReadsOfTheGroupsItLeads(t *testing.T) {
	nm := newNodeMetas(t)
	if err := nm.m.d.AddMeta(nm.replica()); err != nil {
		t.Fatal(err)
	}
	nm.lead()
	nm.send(1, 10)
	pool := nm.l.Context().Pool
	for _, group := range []uint64{nodeMetaGroup, nodeMetaGroup + 1} {
		ev := core.Event{Kind: core.EvNetRecv, Peer: nodeMetaPeer}
		if err := meta.EncodeRead(pool, group, 100+group, &meta.Read{Start: nm.keys[1], End: append(slices.Clone(nm.keys[1]), 0), Limit: 1}, &ev.Msg); err != nil {
			t.Fatal(err)
		}
		nm.l.Inbox().Post(&ev)
		nm.settle()
	}
	if len(nm.reads) != 2 || nm.reads[0].Err != nil || len(nm.reads[0].KVs) != 1 || !bytes.Equal(nm.reads[0].KVs[0].Key, nm.keys[1]) ||
		nm.reads[1].Err != (meta.NotLeader{Group: nodeMetaGroup + 1}) {
		t.Fatalf("the device answered %+v", nm.reads)
	}
}

// A device's server takes the session tables its node hands it and frees an orphan at its last
// close: a fenced session's open closes at its acknowledgment, and the last mount's at its release.
func TestADeviceFreesAnOrphanAtItsLastClose(t *testing.T) {
	nm := newNodeMetas(t)
	if err := nm.m.d.AddMeta(nm.replica()); err != nil {
		t.Fatal(err)
	}
	nm.lead()
	const file = 5
	a := meta.Attr{Mode: meta.ModeFile | 0o644, Nlink: 1, AllocEnd: 1}
	fs := meta.FS{Volume: 1}
	ops := []func() error{
		func() error { return fs.Root(&meta.Attr{Mode: meta.ModeDir | 0o755, Nlink: 2}) },
		func() error { return fs.Create(meta.RootInode, meta.RootInode, []byte("f"), file, &a, nil, 1) },
	}
	for i, op := range ops {
		if err := op(); err != nil {
			t.Fatal(err)
		}
		nm.postTxn(uint64(i)+1, &fs.Txn)
	}
	attr := key(t, &meta.Key{Space: meta.SpaceFS, Volume: 1, Inode: file, Tag: meta.InodeAttr})
	for session := uint64(1); session <= 2; session++ {
		nm.postRead(&meta.Read{Start: attr, End: append(slices.Clone(attr), 0), Limit: 1, Reg: meta.RegOpen, Session: session, Mount: 1})
	}
	if err := fs.Unlink(meta.RootInode, []byte("f"), file, &a, 2); err != nil {
		t.Fatal(err)
	}
	nm.postTxn(3, &fs.Txn)
	purge := key(t, &meta.Key{Space: meta.SpaceFS, Volume: 1, Tag: meta.SlotPurge, ID: file})
	freed := func() bool {
		v, ok := nm.m.d.Meta(nodeMetaGroup).Shard().State().Tree().Get(purge)
		return ok && v != nil
	}
	for tok := uint64(1); tok <= 3; tok++ {
		if err, ok := nm.answers[tok]; !ok || err != nil {
			t.Fatalf("Txn %d was answered %v, %v", tok, ok, err)
		}
	}
	nm.take(node.Output{Hands: []node.Hand{{Device: 7, Table: chunkstore.SessionTable{Version: 1, Acked: 1, Next: 3, Live: []uint64{1}}}}})
	if freed() {
		t.Fatal("an orphan session 1 has open was freed")
	}
	nm.postRead(&meta.Read{Start: attr, End: append(slices.Clone(attr), 0), Limit: 1, Reg: meta.RegRelease, Session: 1, Mount: 1})
	if !freed() {
		t.Fatal("the orphan was not freed at its last release, after its other opener's fence was acknowledged")
	}
}

func key(t *testing.T, k *meta.Key) []byte {
	b, err := meta.AppendKey(nil, k)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// postTxn sends t to nodeMetaGroup's replica under token, and settles.
func (nm *nodeMetas) postTxn(token uint64, t *meta.Txn) {
	ev := core.Event{Kind: core.EvNetRecv, Peer: nodeMetaPeer}
	if err := meta.EncodeTxn(nm.l.Context().Pool, nodeMetaGroup, token, t, &ev.Msg); err != nil {
		nm.t.Fatal(err)
	}
	nm.l.Inbox().Post(&ev)
	nm.settle()
}

// postRead sends rd to nodeMetaGroup's replica, and settles.
func (nm *nodeMetas) postRead(rd *meta.Read) {
	ev := core.Event{Kind: core.EvNetRecv, Peer: nodeMetaPeer}
	if err := meta.EncodeRead(nm.l.Context().Pool, nodeMetaGroup, 100, rd, &ev.Msg); err != nil {
		nm.t.Fatal(err)
	}
	nm.l.Inbox().Post(&ev)
	nm.settle()
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

// A device whose disk grew since its log was formatted, as a resized volume's, restores its replica
// under the blob bound the log keeps from its format and reports the size its log was formatted
// with: it holds every Txn it answered, leads and takes Txns gathered into a round past that bound.
func TestADeviceRestoresItsReplicasOnAGrownVolume(t *testing.T) {
	nm := newNodeMetas(t)
	if err := nm.m.d.AddMeta(nm.replica()); err != nil {
		t.Fatal(err)
	}
	nm.lead()
	nm.send(20, 900)
	formatted := nm.m.d.Store().Log().Superblock().DiskBlocks()
	nm.sd.Restart()
	nm.blocks = 2 * nm.sd.Blocks()
	nm.start([]meta.RoundConfig{nm.replica()})
	if nm.m.failed != 0 || nm.m.opened != 1 || nm.m.report.Blocks != formatted {
		t.Fatalf("the device opened %d times, reporting %d blocks of the %d formatted, and failed %d", nm.m.opened, nm.m.report.Blocks, formatted, nm.m.failed)
	}
	nm.lead()
	nm.send(100, 900)
	for tok := range nm.keys {
		if err, ok := nm.answers[tok]; !ok || err != nil {
			t.Fatalf("Txn %d answered %v, %v", tok, ok, err)
		}
	}
	nm.check()
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
	ev := nm.askOf(nodeMetaGroup, id)
	nm.l.Inbox().Post(&ev)
}

// askOf is an ask, from nodeMetaAsker, about replica id of group.
func (nm *nodeMetas) askOf(group, id uint64) core.Event {
	body := binary.LittleEndian.AppendUint64([]byte{core.ProtoGroup}, group)
	body = append(body, raftKindAsk)
	for _, v := range []uint64{2, id, 0, 0, 0} {
		body = binary.LittleEndian.AppendUint64(body, v)
	}
	ev := core.Event{Kind: core.EvNetRecv, Peer: nodeMetaAsker}
	if err := ev.Msg.SetInline(body); err != nil {
		nm.t.Fatal(err)
	}
	return ev
}

// goneNow hands the device, between the loop's passes, an ask about replica id of group, and
// reports whether it answered Gone.
func (nm *nodeMetas) goneNow(group, id uint64) bool {
	ev := nm.askOf(group, id)
	var out node.DeviceOutput
	nm.m.d.OnEvent(nm.l.Context(), &ev, &out)
	before := nm.gone
	nm.execute(out.Actions)
	return nm.gone > before
}

// execute hands the client's driver the messages acts send.
func (nm *nodeMetas) execute(acts []core.Action) {
	for i := range acts {
		metaDeviceDriver{nm: nm}.Execute(&acts[i])
	}
}

// nodeMetaRemove is the timer at which the device removes its replica of nodeMetas.removing,
// within a pass of its loop, as a message from the control plane would have it.
const nodeMetaRemove core.TimerID = 0x72656d

// remove has the device remove its replica of group at its loop's next pass.
func (nm *nodeMetas) remove(group uint64) {
	nm.removing, nm.removed = group, false
	nm.l.Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: nodeMetaRemove})
}

// removeNow has the device remove its replica of group, stepping until it has, and fails unless
// RemoveMeta returned want.
func (nm *nodeMetas) removeNow(group uint64, want error) {
	nm.remove(group)
	for !nm.removed {
		if !nm.s.Step() {
			nm.t.Fatal("the removal never ran")
		}
	}
	if nm.removeErr != want {
		nm.t.Fatalf("RemoveMeta returned %v", nm.removeErr)
	}
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
}

// A restart whose disk holds a replica of a group the shard map no longer names serves: the device
// keeps the replica closed, taking no tick or Raft message, so never leading, and deletes nothing, so a restart whose map names the
// group again restores every Txn it answered.
func TestARestartKeepsAReplicaOfAGroupTheMapDroppedClosed(t *testing.T) {
	nm := newNodeMetas(t)
	cfg := nm.replica()
	if err := nm.m.d.AddMeta(cfg); err != nil {
		t.Fatal(err)
	}
	nm.lead()
	nm.send(20, 100)

	nm.sd.Restart()
	nm.start(nil)
	if nm.m.d.Store() == nil {
		t.Fatal("a disk holding a replica of a group the map dropped failed its start")
	}
	if nm.m.d.Meta(nodeMetaGroup) == nil || !slices.Equal(nm.m.d.Unmapped(), []disklog.Replica{{Group: nodeMetaGroup, ID: 1}}) {
		t.Fatalf("the replica is not kept closed: unmapped %v", nm.m.d.Unmapped())
	}
	nm.ticking = true
	nm.l.Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: node.MetaTimer})
	for i := 0; i < 1_000_000 && nm.s.Step(); i++ {
	}
	nm.ticking = false
	a := nm.m.d.Meta(nodeMetaGroup)
	if a.Shard() == nil || a.Shard().Raft().Leader() != 0 {
		t.Fatal("the closed replica is not open, or led")
	}
	now := raftpb.Message{Type: raftpb.MsgTimeoutNow, From: 2, To: 1, Term: a.Shard().Raft().Term()}
	nm.l.Inbox().Post(&core.Event{Kind: core.EvNetRecv, Peer: nodeMetaAsker, Msg: raftFrom(t, now, nodeMetaGroup)})
	nm.settle()
	if a.Shard().Raft().Leader() != 0 {
		t.Fatal("the closed replica campaigned on a MsgTimeoutNow")
	}

	nm.sd.Restart()
	nm.start([]meta.RoundConfig{cfg})
	nm.lead()
	nm.check()
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

// Removing a replica answers every Txn it holds, as applied or dropped, and frees its group for
// a new replica of it.
func TestRemovingAReplicaAnswersTheTxnsItHolds(t *testing.T) {
	nm := newNodeMetas(t)
	if err := nm.m.d.AddMeta(nm.replica()); err != nil {
		t.Fatal(err)
	}
	nm.lead()
	nm.post(nodeMetaGroup, 8, 10)
	for range 3 {
		nm.s.Step()
	}
	nm.removeNow(nodeMetaGroup, nil)
	nm.settle()
	dropped := 0
	for tok := range nm.keys {
		switch err, ok := nm.answers[tok]; {
		case !ok:
			t.Fatalf("Txn %d was never answered", tok)
		case errors.Is(err, raft.ErrProposalDropped):
			dropped++
		case err != nil:
			t.Fatalf("Txn %d answered %v", tok, err)
		}
	}
	if dropped == 0 {
		t.Fatal("the replica held no Txn when removed")
	}
	cfg := nm.replica()
	cfg.Owner++
	if err := nm.m.d.AddMeta(cfg); err != nil {
		t.Fatalf("the device refused a new replica of the group it removed one of: %v", err)
	}
	nm.lead()
	nm.send(1, 10)
	if err, ok := nm.answers[nm.next]; !ok || err != nil {
		t.Fatalf("the new replica answered %v, %v", ok, err)
	}
}

// The device soon answers an ask about a replica it removed Gone, though nothing else writes on its
// disk: the removal has the store begin a checkpoint whose manifest leaves the replica out, and
// once that manifest is durable the store writes a round of its own, which opens a segment whose
// header names that manifest, the one a restart then loads.
func TestARemovedReplicaIsSoonGone(t *testing.T) {
	nm := newNodeMetas(t)
	if err := nm.m.d.AddMeta(nm.replica()); err != nil {
		t.Fatal(err)
	}
	nm.lead()
	nm.send(4, 100)
	nm.removeNow(nodeMetaGroup, nil)
	nm.settle()
	if !nm.goneNow(nodeMetaGroup, 1) {
		t.Fatal("the device never answered Gone for the replica it removed")
	}
}

// The device refuses a new replica of a group until its store has taken the removal of the one it
// removed, which waits for that one's rounds in flight: a checkpoint begun before then names the
// removed replica still, and naming a new one of the group too would leave a restart that loads
// it two replicas of one group. Crashed at every step after the new replica's add, the device
// starts again.
func TestARemovedReplicasGroupTakesANewReplicaOnlyOnceTheStoreTookTheRemoval(t *testing.T) {
	refused := 0
	for k := 0; k < 12; k++ {
		nm := newNodeMetas(t)
		cfg := nm.replica()
		if err := nm.m.d.AddMeta(cfg); err != nil {
			t.Fatal(err)
		}
		nm.lead()
		nm.send(20, 100)
		nm.post(nodeMetaGroup, 4, 100)
		nm.s.Step()
		nm.removeNow(nodeMetaGroup, nil)
		again := nm.replica()
		again.Owner = cfg.Owner + 1
		for {
			err := nm.m.d.AddMeta(again)
			if err == nil {
				break
			}
			if err != node.ErrHosted {
				t.Fatal(err)
			}
			refused++
			if !nm.s.Step() {
				t.Fatal("the store never took the removal")
			}
		}
		for range k {
			if !nm.s.Step() {
				break
			}
		}
		nm.sd.Crash(core.NewRand(uint64(k)))
		nm.start([]meta.RoundConfig{nm.replica()})
		if nm.m.d.Store() == nil {
			t.Fatalf("crashed %d steps after the new replica's add, the device did not start", k)
		}
	}
	if refused == 0 {
		t.Fatal("no new replica came while the store had yet to take the removal")
	}
}

// A device removes no replica before its store serves, as it hosts none then: a removal asked
// during a restart's replay is refused ErrNotServing and the restored replica keeps its Txns.
func TestADeviceRemovesNoReplicaBeforeItServes(t *testing.T) {
	nm := newNodeMetas(t)
	cfg := nm.replica()
	if err := nm.m.d.AddMeta(cfg); err != nil {
		t.Fatal(err)
	}
	nm.lead()
	nm.send(20, 100)

	nm.sd.Restart()
	nm.boot([]meta.RoundConfig{cfg})
	for nm.m.d == nil || nm.m.d.Meta(nodeMetaGroup) == nil {
		if !nm.s.Step() {
			t.Fatal("the replica was not restored")
		}
	}
	if nm.m.d.Store().Serving() {
		t.Fatal("the store served before the removal could come during the replay")
	}
	nm.removeNow(nodeMetaGroup, node.ErrNotServing)
	nm.settle()
	nm.lead()
	nm.check()
}

// Removing a replica and adding a new one of its group, over and over, leaves the device holding
// what it holds for one: each removed replica's timer slot goes to the next, and the device
// forgets each removal once a restart would no longer restore its replica.
func TestRemovingAndAddingReplicasKeepsTheDevicesTablesBounded(t *testing.T) {
	nm := newNodeMetas(t)
	cfg := nm.replica()
	for i := range 20 {
		if err := nm.m.d.AddMeta(cfg); err != nil {
			t.Fatalf("add %d: %v", i, err)
		}
		if n := nm.m.d.Removing(); n != 0 {
			t.Fatalf("add %d: the device keeps %d removals", i, n)
		}
		nm.lead()
		nm.send(2, 10)
		nm.removeNow(nodeMetaGroup, nil)
		nm.settle()
		cfg.Owner++
	}
	if nm.slots != 1 {
		t.Fatalf("the replicas took %d timer slots", nm.slots)
	}
}

// A removed replica's owner key goes to no new replica while a manifest a restart may load names
// the removed one, as a restart would restore it under that key and hand it the new replica's
// blobs: with the checkpoint running at the removal stalled, so that none leaving the removed
// replica out begins, the key is refused, and a crash restores the removed replica whole, holding
// no blob of another.
func TestARemovedOwnerWaitsForAManifestLeavingItsReplicaOut(t *testing.T) {
	nm := newNodeMetas(t)
	other := uint64(nodeMetaGroup + 1)
	cfgs := []meta.RoundConfig{nm.replicaOf(other), nm.replicaOf(nodeMetaGroup)}
	for _, c := range cfgs {
		if err := nm.m.d.AddMeta(c); err != nil {
			t.Fatal(err)
		}
	}
	nm.lead(other, nodeMetaGroup)
	nm.send(20, 100)
	nm.stall, nm.stallOwner = true, cfgs[0].Owner
	for nm.stalled == 0 {
		nm.sendTo(other, 20, 900)
	}
	nm.removeNow(nodeMetaGroup, nil)
	nm.settle()
	if owner := cfgs[1].Owner; nm.m.d.Store().Reusable(owner) {
		next := nm.replicaOf(nodeMetaGroup + 2)
		next.Owner, next.Shard.Raft.Storage = owner, bootAt(t, 100)
		if err := nm.m.d.AddMeta(next); err != nil {
			t.Fatal(err)
		}
		nm.settle()
		cfgs = append(cfgs, next)
	}
	nm.stall = false
	nm.sd.Crash(core.NewRand(1))
	nm.start(cfgs)
	back := nm.m.d.Meta(nodeMetaGroup)
	if back == nil {
		t.Fatal("the removed replica was not restored, though no manifest leaving it out began")
	}
	if last, _ := back.Storage().LastIndex(); back.Storage().Base().Index >= 100 || last >= 100 {
		t.Fatalf("the restored replica holds another's blobs: base %d, last %d", back.Storage().Base().Index, last)
	}
	nm.lead(other, nodeMetaGroup)
	nm.check()
}

// A restart after a removal never brings back a replica its device answered Gone, and never hands
// a removed replica's blobs to a new replica its owner key went to: crashed at every step from the
// removal of a replica that answered Txns, through another group's Txns, to a new replica under
// the removed one's owner key starting, the disk restores the removed replica, holding every Txn
// it answered, only while the device would not answer an ask about it Gone, and restores the new
// replica, holding none of the removed one's keys, whenever it had started.
func TestARestartAfterARemovalNeverRestoresTheRemovedReplicaGone(t *testing.T) {
	other, reuse := uint64(nodeMetaGroup+1), uint64(nodeMetaGroup+2)
	var restoredRemoved, goneCrashes, reusedStarted int
	for k := 0; goneCrashes < 3 || reusedStarted < 3; k++ {
		if k == 2000 {
			t.Fatalf("after %d crashes: %d after Gone, %d with the new replica started", k, goneCrashes, reusedStarted)
		}
		nm := newNodeMetas(t)
		cfgs := []meta.RoundConfig{nm.replicaOf(other), nm.replicaOf(nodeMetaGroup)}
		for _, c := range cfgs {
			if err := nm.m.d.AddMeta(c); err != nil {
				t.Fatal(err)
			}
		}
		nm.lead(other, nodeMetaGroup)
		nm.send(20, 100)
		nm.post(nodeMetaGroup, 4, 100)
		nm.s.Step()
		removedKeys := len(nm.keys)
		next := nm.replicaOf(reuse)
		next.Owner = cfgs[1].Owner
		next.Shard.Raft.Storage = bootAt(t, 100)
		cfgs = append(cfgs, next)

		nm.remove(nodeMetaGroup)
		gone, reused := false, false
		for step := 0; step < k; step++ {
			stepped := nm.s.Step()
			gone = gone || nm.goneNow(nodeMetaGroup, 1)
			if !reused && nm.m.d.Store().Reusable(next.Owner) {
				if err := nm.m.d.AddMeta(next); err != nil {
					t.Fatal(err)
				}
				reused = true
			}
			switch {
			case stepped:
			case reused && nm.m.d.Meta(reuse).Shard() != nil:
				step = k
			default:
				nm.post(other, 2, 10)
			}
		}
		up := reused && nm.m.d.Meta(reuse).Shard() != nil
		nm.sd.Crash(core.NewRand(uint64(k)))
		nm.start(cfgs)
		back, renewed := nm.m.d.Meta(nodeMetaGroup), nm.m.d.Meta(reuse)
		if nm.m.d.Store() == nil {
			t.Fatalf("crashed %d steps after the removal, the device failed", k)
		}
		var restored []uint64
		for _, g := range []uint64{other, nodeMetaGroup, reuse} {
			if nm.m.d.Meta(g) != nil {
				restored = append(restored, g)
			}
		}
		nm.lead(restored...)
		switch {
		case gone && back != nil:
			t.Fatalf("crashed %d steps after the removal, the replica the device answered Gone was restored", k)
		case up && renewed == nil:
			t.Fatalf("crashed %d steps after the removal, the new replica that had started was not restored", k)
		}
		for tok, key := range nm.keys {
			if err, ok := nm.answers[tok]; !ok || err != nil {
				continue
			}
			if renewed != nil && renewed.Shard() != nil && tok <= uint64(removedKeys) {
				if _, ok := renewed.Shard().State().Tree().Get(key); ok {
					t.Fatalf("crashed %d steps after the removal, the new replica under its owner key holds the removed one's Txn %d", k, tok)
				}
			}
			a := nm.m.d.Meta(nm.groups[tok])
			if a == nil && nm.groups[tok] == nodeMetaGroup {
				continue
			}
			if a == nil || a.Err() != nil || a.Shard() == nil {
				t.Fatalf("crashed %d steps after the removal, the replica of group %d did not restore", k, nm.groups[tok])
			}
			if v, ok := a.Shard().State().Tree().Get(key); !ok || !bytes.HasPrefix(v, key) {
				t.Fatalf("crashed %d steps after the removal, the replica of group %d holds %q under Txn %d's key", k, nm.groups[tok], v, tok)
			}
		}
		if renewed != nil && renewed.Err() != nil {
			t.Fatalf("crashed %d steps after the removal, the new replica did not restore: %v", k, renewed.Err())
		}
		if back != nil {
			restoredRemoved++
		}
		if gone {
			goneCrashes++
		}
		if up {
			reusedStarted++
		}
	}
	if restoredRemoved == 0 {
		t.Fatal("no crash came before a named manifest left the removed replica out")
	}
}

const nodeMetaTake core.TimerID = 0x74616b

// take has the device take o at its loop's next pass, as the node's loop hands it a membership
// with the shard map's plans, or deletes, and settles.
func (nm *nodeMetas) take(o node.Output) {
	nm.taking = o
	nm.l.Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: nodeMetaTake})
	nm.settle()
}

// disks is the membership naming the device's disk, disk 1, at its loop.
func (nm *nodeMetas) disks() []meta.DiskAddr {
	return []meta.DiskAddr{{Disk: 1, Log: nm.m.d.Store().Log().Superblock().LogID, At: core.NodeID{Node: nm.node, Loop: 1}}}
}

// planOf is group's shard map entry: Boot boot, and voter id on the device's disk.
func planOf(group, boot, id uint64) meta.Group {
	return meta.Group{ID: group, LastID: max(boot, id), Boot: boot, Voters: []meta.Replica{{ID: id, Disk: 1}}}
}

// placed is the membership with plans, as the node hands it on.
func (nm *nodeMetas) placed(plans ...meta.Group) node.Output {
	return node.Output{Disks: nm.disks(), Plans: plans, Member: true}
}

// A plan that places a replica on the device's disk hosts it once: a group's first voter boots
// with its Boot voters' configuration and leads, a voter a later plan added joins with empty
// storage, the same plans again host nothing more, and the device reports both. A restart whose
// settings are the plans' restores each with its own, the voter its configuration and the joiner
// none, and the plans then host nothing more.
func TestAPlanHostsItsReplicaOnceAcrossARepeatAndARestart(t *testing.T) {
	nm := newNodeMetas(t)
	joiner := uint64(nodeMetaGroup + 1)
	plans := []meta.Group{planOf(nodeMetaGroup, 1, 1), planOf(joiner, 1, 2)}
	nm.take(nm.placed(plans...))
	a, j := nm.m.d.Meta(nodeMetaGroup), nm.m.d.Meta(joiner)
	if a == nil || a.ReplicaID() != 1 || j == nil || j.ReplicaID() != 2 || a.Owner() == j.Owner() {
		t.Fatalf("the plans hosted %v and %v", a, j)
	}
	if base := j.Storage().Base(); base.Index != 0 || len(base.ConfState.Voters) != 0 {
		t.Fatalf("a joining replica booted from %+v", base)
	}
	nm.lead()
	nm.send(4, 100)
	nm.take(nm.placed(plans...))
	if nm.m.d.Meta(nodeMetaGroup) != a || nm.m.d.Meta(joiner) != j || nm.m.d.Removing() != 0 {
		t.Fatal("the same plans again changed the replicas hosted")
	}
	if len(nm.reported) != 2 || nm.reported[0].Group != nodeMetaGroup || nm.reported[0].ID != 1 || !slices.Equal(nm.reported[0].Voters, []uint64{1}) || nm.reported[1].ID != 2 {
		t.Fatalf("the device reported %+v", nm.reported)
	}
	if at, ok := a.Shard().Raft().Node(1); !ok || at != (core.NodeID{Node: nm.node, Loop: 1}) {
		t.Fatalf("the replica's peer is at %v, %v", at, ok)
	}

	ds := nm.disks()
	nm.sd.Restart()
	nm.start(node.MetaSettingsOf(metaTemplate(), 0, plans, ds))
	a, j = nm.m.d.Meta(nodeMetaGroup), nm.m.d.Meta(joiner)
	if a == nil || a.ReplicaID() != 1 || j == nil || j.ReplicaID() != 2 {
		t.Fatalf("the restart restored %v and %v", a, j)
	}
	if base := j.Storage().Base(); base.Index != 0 || len(base.ConfState.Voters) != 0 {
		t.Fatalf("the restored joiner holds %+v", base)
	}
	nm.lead()
	nm.check()
	nm.take(nm.placed(plans...))
	if nm.m.d.Meta(nodeMetaGroup) != a || nm.m.d.Meta(joiner) != j {
		t.Fatal("the plans hosted another replica after the restart")
	}
	nm.send(1, 10)
	if err := nm.answers[nm.next]; err != nil {
		t.Fatalf("the restored replica answered %v", err)
	}
}

// A delete removes the replica it names, and one naming a replica the device never hosted, or
// another replica of a group it hosts, removes nothing. A delete wins over a plan: one taken with
// it, or a stale one after the removal is done, never hosts the deleted replica again.
func TestADeleteRemovesItsReplicaForGood(t *testing.T) {
	nm := newNodeMetas(t)
	plan := planOf(nodeMetaGroup, 1, 1)
	nm.take(nm.placed(plan))
	nm.lead()
	nm.send(4, 100)
	nm.take(node.Output{Deletes: []meta.ReplicaRef{{Group: nodeMetaGroup + 5, ID: 1}, {Group: nodeMetaGroup, ID: 2}}})
	if nm.m.d.Meta(nodeMetaGroup) == nil || nm.m.d.Removing() != 0 {
		t.Fatal("a delete of a replica the device does not host removed one")
	}
	o := nm.placed(plan)
	o.Deletes = []meta.ReplicaRef{{Group: nodeMetaGroup, ID: 1}}
	nm.take(o)
	if nm.m.d.Meta(nodeMetaGroup) != nil {
		t.Fatal("the deleted replica is hosted")
	}
	for nm.m.d.Removing() != 0 {
		if !nm.s.Step() {
			t.Fatal("the removal never ended")
		}
	}
	nm.take(nm.placed(plan))
	if nm.m.d.Meta(nodeMetaGroup) != nil {
		t.Fatal("a stale plan hosted the deleted replica again")
	}
	if !nm.goneNow(nodeMetaGroup, 1) {
		t.Fatal("the device never answered Gone for the replica it deleted")
	}
	nm.take(nm.placed(planOf(nodeMetaGroup, 1, 3)))
	if a := nm.m.d.Meta(nodeMetaGroup); a == nil || a.ReplicaID() != 3 {
		t.Fatalf("a plan placing a new replica of the group hosted %v", a)
	}
}

// A device's chunk replica asks for leases every loop hosting a member of a running replica's
// configuration, outgoing voters and learners among them, and stops asking a loop once the
// replica naming it is deleted. Each member of the replicas here runs on the loop of its ID.
func TestADeviceAsksLeasesOfTheLoopsItsReplicasNameOnly(t *testing.T) {
	nm := newNodeMetas(t)
	groups := map[uint64]raftpb.ConfState{
		nodeMetaGroup:     {Voters: []uint64{1, 2}, VotersOutgoing: []uint64{1, 3}, Learners: []uint64{4}},
		nodeMetaGroup + 1: {Voters: []uint64{1, 5}},
	}
	for _, g := range []uint64{nodeMetaGroup, nodeMetaGroup + 1} {
		cfg := nm.replicaOf(g)
		cfg.Shard.Raft.Storage = bootOf(t, 1, groups[g])
		cfg.Shard.Raft.Peers = nil
		for id := uint64(1); id <= 5; id++ {
			cfg.Shard.Raft.Peers = append(cfg.Shard.Raft.Peers, raftshard.Peer{ID: id, Node: core.NodeID{Node: nm.node, Loop: uint32(id)}})
		}
		if err := nm.m.d.AddMeta(cfg); err != nil {
			t.Fatal(err)
		}
	}
	nm.settle()
	asked := func(o node.Output) []uint32 {
		nm.taking = o
		nm.l.Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: nodeMetaTake})
		end := nm.s.Time() + 6_000_000
		for nm.s.Time() < end && nm.s.Step() {
		}
		var loops []uint32
		for _, at := range nm.asked {
			loops = append(loops, at.Loop)
		}
		slices.Sort(loops)
		return loops
	}
	if got := asked(nm.placed()); !slices.Equal(got, []uint32{2, 3, 4, 5}) {
		t.Fatalf("the device asked leases of loops %v", got)
	}
	o := nm.placed()
	o.Deletes = []meta.ReplicaRef{{Group: nodeMetaGroup + 1, ID: 1}}
	if got := asked(o); !slices.Equal(got, []uint32{2, 3, 4}) {
		t.Fatalf("after the delete the device asked leases of loops %v", got)
	}
}

// A device's facts copy the MetaStore replica it hosts and, while the replica leads, each
// follower's match index, a learner's among them, into arrays a refill reuses.
func TestFactsOfAHostedLeaderAllocateNothing(t *testing.T) {
	nm := newNodeMetas(t)
	cfg := nm.replica()
	cfg.Shard.Raft.Storage = bootOf(t, 1, raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{2}})
	cfg.Shard.Raft.Peers = append(cfg.Shard.Raft.Peers, raftshard.Peer{ID: 2, Node: core.NodeID{Node: 79}})
	if err := nm.m.d.AddMeta(cfg); err != nil {
		t.Fatal(err)
	}
	nm.lead()
	var f ops.Facts
	fill := func() {
		f.Reset()
		nm.m.d.Facts(nm.l.Context(), &f)
	}
	fill()
	if len(f.Replicas) != 1 || f.Replicas[0].Leader != 1 || len(f.Replicas[0].Followers) != 1 || f.Replicas[0].Followers[0].ID != 2 {
		t.Fatalf("the device's replicas %+v", f.Replicas)
	}
	if n := testing.AllocsPerRun(100, fill); n != 0 {
		t.Fatalf("a device's facts allocate %v times per copy", n)
	}
}
