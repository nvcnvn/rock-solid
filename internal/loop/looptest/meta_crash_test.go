package looptest

import (
	"bytes"
	"encoding/binary"
	"fmt"
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
	"github.com/nvcnvn/rock-solid/internal/node"
	"github.com/nvcnvn/rock-solid/internal/raftshard"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

// metaCrash is a crash run's view of a nodeMetas device: beside the device's single voters,
// group crashGroup's voter runs on loop 0, a meta.RoundShard known to the device's store, and its
// learner, which joins with empty storage, on the device. It keeps, for each Txn by token, the
// span of applied indexes its key appeared between (absent at lo, present at hi), each group's
// applied index at its newest Txn answered applied, the highest index the learner acknowledged
// and the highest term each replica of crashGroup sent, each removal and whether the device
// answered an ask about it Gone, and each owner's part of a checkpoint.
type metaCrash struct {
	t      *testing.T
	nm     *nodeMetas
	cfg    chunkstore.Config
	rnd    *core.Rand
	l0     *loop.Loop
	voter  *meta.RoundShard
	open   bool
	cut    bool
	parked []core.TimerID
	// vpending is the voter's Txns waiting for loop 0, vsize their values' size.
	vpending []uint64
	vsize    int
	spans    map[uint64]*span
	unseen   map[uint64][]uint64
	at       map[*meta.RoundShard]uint64
	ack      map[uint64]uint64
	acked    uint64
	sent     [2]uint64
	snaps    uint64
	// snapSeen is the newest snapshot the learner was seen holding, countdown the steps until the
	// crash that follows it, and crashNow that it is due.
	snapSeen  uint64
	countdown int
	crashNow  bool
	groups    []uint64
	removed   []removal
	next      uint64
	writes    uint64
	owner     uint32
	flushes   map[uint32]*flushPart
	dead      map[uint32]bool
	c         metaCrashCounts
}

type span struct{ lo, hi uint64 }

type removal struct {
	group, id uint64
	owner     uint32
	gone      bool
}

// flushPart is an owner's part of a checkpoint: phase 0 with no flush of it running, 1 from its
// first reply for checkpoint cp, 2 once it replied its flush's end and until a manifest newer than
// manifest is durable.
type flushPart struct {
	cp, manifest uint64
	phase        int
}

// metaCrashCounts is what a crash run did: crashes with an owner in each phase, restarts that
// restored a replica on a compacted base and the learner on a snapshot it was sent, snapshots
// delivered, removals answered Gone, replicas added and those of them under a removed replica's
// owner key, merges' dead pieces and segments the cleaner freed that held them.
type metaCrashCounts struct {
	phases                                                     [3]int
	compacted, onSnap, snaps, gone, added, reused, dead, freed int
}

func (c *metaCrashCounts) add(r metaCrashCounts) {
	for i := range c.phases {
		c.phases[i] += r.phases[i]
	}
	c.compacted, c.onSnap, c.snaps, c.gone = c.compacted+r.compacted, c.onSnap+r.onSnap, c.snaps+r.snaps, c.gone+r.gone
	c.added, c.reused, c.dead, c.freed = c.added+r.added, c.reused+r.reused, c.dead+r.dead, c.freed+r.freed
}

// below reports whether any count is below floor's.
func (c *metaCrashCounts) below(floor metaCrashCounts) bool {
	for i := range c.phases {
		if c.phases[i] < floor.phases[i] {
			return true
		}
	}
	return c.compacted < floor.compacted || c.onSnap < floor.onSnap || c.snaps < floor.snaps || c.gone < floor.gone ||
		c.added < floor.added || c.reused < floor.reused || c.dead < floor.dead || c.freed < floor.freed
}

const (
	crashGroup             = 30
	crashVoterOwner uint32 = 2001
)

func (c *metaCrash) peers() []raftshard.Peer {
	return []raftshard.Peer{{ID: 1, Node: core.NodeID{Node: c.nm.node, Loop: 0}}, {ID: 2, Node: core.NodeID{Node: c.nm.node, Loop: 1}}}
}

// learner is the settings of crashGroup's learner on the device, with empty storage.
func (c *metaCrash) learner() meta.RoundConfig {
	return meta.RoundConfig{
		Shard: meta.Config{
			Raft: raftshard.Config{Group: crashGroup, ID: 2, Storage: raftshard.NewStorage(), Seed: 2, Peers: c.peers(),
				Holder: lease.NewHolder(lease.DefaultConfig), Grantor: lease.NewGrantor(lease.DefaultConfig, 1, lease.Time{})},
			Tree: lsm.Config{MaxPiece: 4 << 10, StepBytes: 4 << 10, Check: true},
		},
		Owner: 991 + crashGroup,
	}
}

// boot adds loop 0 to the node of a new process.
func (c *metaCrash) boot(n *sim.Node) {
	k := NewKernel(c.nm.s, n.ID())
	c.l0 = loop.New(loop.Config{Node: n.ID(), ID: 0, Pool: sim.NewPool(4<<20, 0)}, k, crashVoterMachine{c}, crashVoterDriver{c: c})
	Add(n, k, c.l0)
	c.voter, c.parked = nil, c.parked[:0]
}

// opened puts the voter back on loop 0 as the device's store starts a restart.
func (c *metaCrash) opened() {
	if c.open && c.nm.m.d.Store() != nil {
		c.newVoter(true)
	}
}

func (c *metaCrash) newVoter(open bool) {
	st := raftshard.NewStorage()
	cs := raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{2}}
	if err := st.ApplySnapshot(raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 1, Term: 1, ConfState: cs}}); err != nil {
		c.t.Fatal(err)
	}
	g, err := disklog.GeometryFor(c.nm.sd.Blocks() * core.BlockSize)
	if err != nil {
		c.t.Fatal(err)
	}
	cfg := meta.RoundConfig{
		Shard: meta.Config{
			Raft: raftshard.Config{Group: crashGroup, ID: 1, Storage: st, Seed: 1, Timer: metaRaftTimer, Peers: c.peers(),
				Holder: lease.NewHolder(lease.DefaultConfig), Grantor: lease.NewGrantor(lease.DefaultConfig, 1, lease.Time{})},
			Tree: lsm.Config{MaxPiece: 4 << 10, StepBytes: 4 << 10, Check: true},
		},
		Owner: crashVoterOwner, MaxBlob: disklog.MaxBlobFor(c.cfg.Log, g), Open: open,
		Answered: func(_ *core.Context, as []meta.Answer, out []core.Action) []core.Action {
			for _, an := range as {
				c.nm.answers[an.Token] = an.Err
				if an.Err == nil {
					c.answered(an.Token)
				}
			}
			return out
		},
	}
	c.voter = meta.NewRoundShard(c.l0, c.nm.m.lg, cfg)
	c.nm.m.d.Store().AddRemote(crashVoterOwner, 0, uint32(c.voter.ID()), disklog.Replica{})
}

// replicas is the started replicas of group.
func (c *metaCrash) replicas(group uint64) []*meta.RoundShard {
	var out []*meta.RoundShard
	if group == crashGroup && c.voter != nil {
		out = append(out, c.voter)
	}
	if a := c.nm.m.d.Meta(group); a != nil {
		out = append(out, a)
	}
	return slices.DeleteFunc(out, func(a *meta.RoundShard) bool { return a.Err() != nil || a.Shard() == nil })
}

// answered keeps the applied index of the group of a Txn answered applied.
func (c *metaCrash) answered(tok uint64) {
	g := c.nm.groups[tok]
	a := c.voter
	if g != crashGroup {
		a = c.nm.m.d.Meta(g)
	}
	if a != nil && a.Shard() != nil {
		c.ack[g] = max(c.ack[g], a.Shard().State().Applied())
	}
}

// replied follows each owner's part of a checkpoint and the pieces its merges left out.
func (c *metaCrash) replied(r loop.Reply) {
	f := c.flushes[r.Owner]
	if f == nil {
		f = &flushPart{}
		c.flushes[r.Owner] = f
	}
	switch r.Kind {
	case loop.ReplyRuns, loop.ReplyPiece:
		if f.phase == 0 || f.cp != r.LSN {
			f.cp, f.phase = r.LSN, 1
		}
		if r.Kind == loop.ReplyPiece && len(r.Data) == 0 {
			f.phase, f.manifest = 2, c.nm.m.d.Store().Log().Manifest().LSN
		}
	case loop.ReplyDead:
		for _, ref := range r.Refs {
			if !c.dead[ref.Segment] {
				c.dead[ref.Segment] = true
				c.c.dead++
			}
		}
	}
}

func (c *metaCrash) phase(f *flushPart) int {
	if f.phase == 2 && c.nm.m.d.Store().Log().Manifest().LSN != f.manifest {
		f.phase = 0
	}
	return f.phase
}

// forward hands a message between loop 0 and the device's loop, as the network would, dropping
// those to the learner while cut, and watches crashGroup's Raft messages.
func (c *metaCrash) forward(a *core.Action, from uint32) bool {
	if a.Kind != core.ActNetSend || a.Peer.Node != c.nm.node {
		return false
	}
	src, dst := c.loopOf(from), c.loopOf(a.Peer.Loop)
	c.watch(&a.Msg, from, src.Context().Pool)
	if c.cut && a.Peer.Loop == 1 || dst == nil {
		if a.Msg.Buf != core.NoBuf {
			src.Context().Pool.Free(a.Msg.Buf)
		}
		return true
	}
	ev := core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{Node: c.nm.node, Loop: from}, Msg: a.Msg}
	if b := a.Msg.Buf; b != core.NoBuf {
		fp, tp := src.Context().Pool, dst.Context().Pool
		rb, err := tp.Alloc(len(fp.Bytes(b)))
		if err != nil {
			c.t.Fatal(err)
		}
		copy(tp.Bytes(rb), fp.Bytes(b))
		fp.Free(b)
		ev.Msg.Buf = rb
	}
	dst.Inbox().Post(&ev)
	return true
}

func (c *metaCrash) loopOf(id uint32) *loop.Loop {
	if id == 0 {
		return c.l0
	}
	return c.nm.l
}

// watch keeps the highest term a replica of crashGroup sent, the highest index the learner
// acknowledged, and the snapshots the voter sent it.
func (c *metaCrash) watch(msg *core.Msg, from uint32, pool core.BufPool) {
	in := msg.Inline()
	if len(in) < 10 || in[0] != core.ProtoGroup || binary.LittleEndian.Uint64(in[1:]) != crashGroup || in[9] != 0 {
		return
	}
	body := in[10:]
	if msg.Buf != core.NoBuf {
		body = pool.Bytes(msg.Buf)[:binary.LittleEndian.Uint32(body)]
	}
	var m raftpb.Message
	if m.Unmarshal(body) != nil {
		return
	}
	c.sent[from] = max(c.sent[from], m.Term)
	switch {
	case from == 1 && m.Type == raftpb.MsgAppResp && !m.Reject:
		c.acked = max(c.acked, m.Index)
	case from == 0 && m.Type == raftpb.MsgSnap && !c.cut:
		c.snaps = max(c.snaps, m.Snapshot.Metadata.Index)
		c.c.snaps++
	}
}

type crashVoterMachine struct{ c *metaCrash }

func (m crashVoterMachine) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	c := m.c
	switch {
	case ev.Kind == core.EvTimer && ev.Timer == metaRaftTimer && !c.nm.ticking:
		c.parked = append(c.parked, metaRaftTimer)
	case ev.Kind == core.EvTimer && ev.Timer == metaRaftTimer && c.voter != nil:
		out = c.voter.OnEvent(ctx, ev, out)
	case ev.Kind == core.EvTimer && ev.Timer == metaTxnTimer:
		for _, tok := range c.vpending {
			key := c.nm.keys[tok]
			t := meta.Txn{Ops: []meta.Op{meta.Put(key, append(append([]byte(nil), key...), make([]byte, c.vsize)...))}}
			var err error
			if out, err = c.voter.Txn(ctx, &t, tok, out); err != nil {
				c.nm.answers[tok] = err
			}
		}
		c.vpending = c.vpending[:0]
	case ev.Kind == core.EvNetRecv && c.voter != nil:
		out = c.voter.OnEvent(ctx, ev, out)
	case ev.Kind == core.EvNetRecv && ev.Msg.Buf != core.NoBuf:
		ctx.Pool.Free(ev.Msg.Buf)
	}
	return out
}

type crashVoterDriver struct {
	diskDriver
	c *metaCrash
}

func (d crashVoterDriver) Execute(a *core.Action) { d.c.forward(a, 0) }

// crashWriteTimer has the device's store take a chunk write of the event's IO blocks.
const crashWriteTimer core.TimerID = 0x777269

// write hands the device's store a chunk write within a pass of its loop, as its replica would,
// and its output to the loop and to the store's shards.
func (c *metaCrash) write(ctx *core.Context, ev *core.Event, out *node.DeviceOutput) {
	st := c.nm.m.d.Store()
	n := int(ev.IO)
	b, err := ctx.Pool.Alloc(n * core.BlockSize)
	if err != nil {
		c.t.Fatal(err)
	}
	copy(ctx.Pool.Bytes(b), storetest.Fill(n*core.BlockSize, byte(c.writes)))
	c.writes++
	var o chunkstore.Output
	st.Write(ctx, chunkstore.Request{ID: c.writes, Chunk: storetest.Chunk(uint32(c.writes % 8)), Len: uint32(n * core.BlockSize), Buf: b}, &o)
	for _, m := range o.Mail {
		c.nm.m.lg.Send(m.Loop, loop.Mail{Shard: loop.ShardID(m.Shard), Kind: mailKinds[m.Kind], Owner: m.Owner, Ref: m.Ref, Blob: m.Blob, Data: m.Data, Pieces: m.Pieces, Base: m.Base, Share: m.Share, Bound: m.Bound})
	}
	out.Actions = append(out.Actions, o.Actions...)
}

// propose has the voter propose n Txns of size bytes, each under a key of its own.
func (c *metaCrash) propose(n, size int) {
	if c.voter == nil || c.voter.Shard() == nil {
		return
	}
	for range n {
		c.nm.next++
		tok := c.nm.next
		c.nm.keys[tok], c.nm.groups[tok] = []byte(fmt.Sprintf("\x01k%06d", tok)), crashGroup
		c.track(tok)
		c.vpending = append(c.vpending, tok)
	}
	c.vsize = size
	c.l0.Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: metaTxnTimer})
}

// post sends the device n Txns of size bytes for group.
func (c *metaCrash) post(group uint64, n, size int) {
	first := c.nm.next + 1
	c.nm.post(group, n, size)
	for tok := first; tok <= c.nm.next; tok++ {
		c.track(tok)
	}
}

func (c *metaCrash) track(tok uint64) {
	c.spans[tok] = &span{}
	g := c.nm.groups[tok]
	c.unseen[g] = append(c.unseen[g], tok)
}

// observe narrows the span of each Txn not yet seen applied by the state of each replica of its
// group whose applied index moved.
func (c *metaCrash) observe() {
	for g, toks := range c.unseen {
		for _, a := range c.replicas(g) {
			st := a.Shard().State()
			applied := st.Applied()
			if c.at[a] == applied {
				continue
			}
			c.at[a] = applied
			toks = slices.DeleteFunc(toks, func(tok uint64) bool {
				sp := c.spans[tok]
				if _, ok := st.Tree().Get(c.nm.keys[tok]); ok {
					sp.hi = applied
					return true
				}
				sp.lo = max(sp.lo, applied)
				return false
			})
		}
		c.unseen[g] = toks
	}
}

// asks asks the device about each replica it removed, as a peer would, and keeps each answer Gone.
func (c *metaCrash) asks() {
	for i := range c.removed {
		if rm := &c.removed[i]; !rm.gone && c.nm.goneNow(rm.group, rm.id) {
			rm.gone = true
			c.c.gone++
		}
	}
}

// step steps the process, and once the learner holds a snapshot it was sent, has the process
// crash within 100 steps.
func (c *metaCrash) step() bool {
	stepped := c.nm.s.Step()
	c.observe()
	c.asks()
	c.freed()
	if l := c.nm.m.d.Meta(crashGroup); c.snaps > c.snapSeen && l != nil && l.Storage().Base().Index >= c.snaps {
		c.snapSeen, c.countdown = c.snaps, 1+int(c.rnd.Below(100))
	}
	if c.countdown > 0 {
		c.countdown--
		c.crashNow = c.countdown == 0
	}
	return stepped
}

// freed counts the segments that held pieces merges left out once the cleaner frees them.
func (c *metaCrash) freed() {
	log := c.nm.m.d.Store().Log()
	for seg := range c.dead {
		if u, _ := log.Usage(seg); u.State == disklog.SegmentFree {
			delete(c.dead, seg)
			c.c.freed++
		}
	}
}

// tick runs every replica's Raft timer for n steps.
func (c *metaCrash) tick(n int) {
	c.unpark()
	for range n {
		if c.crashNow {
			break
		}
		c.step()
	}
	c.nm.ticking = false
}

func (c *metaCrash) unpark() {
	c.nm.ticking = true
	for _, tm := range c.parked {
		l := c.nm.l
		if tm == metaRaftTimer {
			l = c.l0
		}
		l.Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: tm})
	}
	c.parked = c.parked[:0]
}

// hosted is the device's single voters.
func (c *metaCrash) hosted() []uint64 {
	var out []uint64
	for _, g := range c.groups {
		if a := c.nm.m.d.Meta(g); a != nil && a.Err() == nil {
			out = append(out, g)
		}
	}
	return out
}

// add has the device host a new single voter of a new group, under the owner key of a replica it
// removed when its store allows.
func (c *metaCrash) add() {
	cfg := c.nm.replicaOf(c.next)
	cfg.Owner = c.owner
	for _, rm := range c.removed {
		if c.nm.m.d.Store().Reusable(rm.owner) && c.nm.m.d.Meta(rm.group) == nil {
			cfg.Owner = rm.owner
		}
	}
	if err := c.nm.m.d.AddMeta(cfg); err != nil {
		return
	}
	if cfg.Owner == c.owner {
		c.owner++
	} else {
		c.c.reused++
	}
	c.groups = append(c.groups, c.next)
	c.next++
	c.c.added++
}

// remove has the device remove one of its single voters.
func (c *metaCrash) remove() {
	hosted := c.hosted()
	if len(hosted) == 0 || c.nm.removing != 0 && !c.nm.removed {
		return
	}
	g := hosted[c.rnd.Below(uint64(len(hosted)))]
	a := c.nm.m.d.Meta(g)
	c.removed = append(c.removed, removal{group: g, id: a.ReplicaID(), owner: a.Owner()})
	c.nm.remove(g)
}

// metas is the device's settings for every group it may hold a replica of.
func (c *metaCrash) metas() []meta.RoundConfig {
	out := []meta.RoundConfig{c.learner()}
	for _, g := range c.groups {
		out = append(out, c.nm.replicaOf(g))
	}
	return out
}

// lead runs the Raft timers until the voter and every single voter lead and the learner follows,
// then settles.
func (c *metaCrash) lead() {
	c.unpark()
	for n := 0; !c.led(); n++ {
		if n == 1_000_000 || !c.step() {
			c.t.Fatal("the replicas never led")
		}
	}
	c.nm.ticking = false
	c.nm.settle()
	c.observe()
}

func (c *metaCrash) led() bool {
	leads := func(a *meta.RoundShard) bool { return a != nil && a.Shard() != nil && a.Shard().Raft().Leader() == 1 }
	if !leads(c.voter) || !leads(c.nm.m.d.Meta(crashGroup)) {
		return false
	}
	for _, g := range c.hosted() {
		if !leads(c.nm.m.d.Meta(g)) {
			return false
		}
	}
	return true
}

// crash cuts the power, unflushed writes landing at random, starts a new process and checks what
// it restored: every replica started and not removed is back, every replica's state is its group's at its applied index, no replica of a group
// holds another's key, a replica answered Gone is not back, each replica of crashGroup holds a
// term at least the highest it sent and the learner every index it acknowledged; once they lead,
// each voter has applied at least the index at its group's newest Txn answered applied.
func (c *metaCrash) crash(when string) {
	nm := c.nm
	for _, f := range c.flushes {
		c.c.phases[c.phase(f)]++
	}
	var started []uint64
	for _, g := range c.groups {
		if a := nm.m.d.Meta(g); a != nil && a.Shard() != nil && !slices.ContainsFunc(c.removed, func(rm removal) bool { return rm.group == g }) {
			started = append(started, g)
		}
	}
	nm.sd.Crash(c.rnd)
	c.open, c.cut, nm.removing, nm.removed, nm.ticking = true, false, 0, true, false
	c.countdown, c.crashNow = 0, false
	clear(c.flushes)
	clear(c.at)
	nm.start(c.metas())
	d := nm.m.d
	if d.Store() == nil || !d.Store().Serving() {
		c.t.Fatalf("%s: the device did not restart", when)
	}
	for _, g := range started {
		if a := d.Meta(g); a == nil || a.Err() != nil || a.Shard() == nil {
			c.t.Fatalf("%s: the started replica of group %d, not removed, did not restart", when, g)
		}
	}
	c.check(when)
	for _, rm := range c.removed {
		if rm.gone && d.Meta(rm.group) != nil {
			c.t.Fatalf("%s: replica %d of group %d, answered Gone, was restored", when, rm.id, rm.group)
		}
	}
	learner := d.Meta(crashGroup)
	switch {
	case c.voter == nil || learner == nil || c.voter.Shard() == nil || learner.Shard() == nil:
		c.t.Fatalf("%s: crashGroup's replicas did not restart", when)
	case c.voter.Storage().HardState().Term < c.sent[0] || learner.Storage().HardState().Term < c.sent[1]:
		c.t.Fatalf("%s: crashGroup restarted with terms %d and %d, after sending %v", when, c.voter.Storage().HardState().Term, learner.Storage().HardState().Term, c.sent)
	}
	if last, _ := learner.Storage().LastIndex(); last < c.acked {
		c.t.Fatalf("%s: the learner acknowledged %d and restarted holding to %d", when, c.acked, last)
	}
	if c.snaps != 0 && learner.Storage().Base().Index >= c.snaps {
		c.c.onSnap++
	}
	for _, g := range append(c.hosted(), crashGroup) {
		for _, a := range c.replicas(g) {
			if a.Storage().Base().Index > 1 {
				c.c.compacted++
			}
		}
	}
	c.lead()
	c.countdown, c.crashNow, c.snapSeen = 0, false, c.snaps
	for _, g := range append(c.hosted(), crashGroup) {
		for _, a := range c.replicas(g) {
			if a != learner && a.Shard().State().Applied() < c.ack[g] {
				c.t.Fatalf("%s: replica %d of group %d applied to %d, below the %d it answered at", when, a.ReplicaID(), g, a.Shard().State().Applied(), c.ack[g])
			}
		}
	}
	c.check(when + ", led")
}

// check fails unless every started replica holds its group's state at its applied index: each
// key seen applied at or before it, none seen absent at or after it, and no other group's key.
func (c *metaCrash) check(when string) {
	for _, g := range append([]uint64{crashGroup}, c.groups...) {
		for _, a := range c.replicas(g) {
			st := a.Shard().State()
			applied := st.Applied()
			for tok, key := range c.nm.keys {
				v, ok := st.Tree().Get(key)
				sp := c.spans[tok]
				switch {
				case c.nm.groups[tok] != g && ok:
					c.t.Fatalf("%s: replica %d of group %d holds Txn %d of group %d", when, a.ReplicaID(), g, tok, c.nm.groups[tok])
				case c.nm.groups[tok] != g:
				case sp.hi != 0 && applied >= sp.hi && (!ok || !bytes.HasPrefix(v, key)):
					c.t.Fatalf("%s: replica %d of group %d at %d holds %q under Txn %d, applied by %d", when, a.ReplicaID(), g, applied, v, tok, sp.hi)
				case applied <= sp.lo && ok:
					c.t.Fatalf("%s: replica %d of group %d at %d holds Txn %d, not applied at %d", when, a.ReplicaID(), g, applied, tok, sp.lo)
				}
			}
		}
	}
}

// metaCrashRun runs ops operations on a device hosting MetaStore replicas: chunk writes, Txns to
// its single voters and to crashGroup's voter on loop 0, Raft ticks, the learner cut off and
// reconnected, a replica removed and a new one added, each followed by up to 29 steps, and
// crashes the process every 40, a third of the time once it settles, and within 100 steps of the
// learner taking a snapshot.
func metaCrashRun(t *testing.T, seed uint64, ops int) metaCrashCounts {
	cfg := shardReplay()
	geo, err := disklog.GeometryFor(storetest.DiskBytes)
	if err != nil {
		t.Fatal(err)
	}
	cfg.CleanFree = int(geo.SegmentCount) - 8
	nm := &nodeMetas{t: t, sd: storetest.NewSimDiskOf(1, storetest.DiskBytes), answers: map[uint64]error{}, keys: map[uint64][]byte{}, groups: map[uint64]uint64{}}
	c := &metaCrash{t: t, nm: nm, cfg: cfg, rnd: core.NewRand(seed), spans: map[uint64]*span{}, unseen: map[uint64][]uint64{},
		at: map[*meta.RoundShard]uint64{}, ack: map[uint64]uint64{}, flushes: map[uint32]*flushPart{}, dead: map[uint32]bool{}, next: 40, owner: 3001}
	nm.cr = c
	nm.start(nil)
	c.newVoter(false)
	if err := nm.m.d.AddMeta(c.learner()); err != nil {
		t.Fatal(err)
	}
	c.add()
	c.add()
	c.c.added = 0
	nm.settle()
	c.lead()
	rnd := c.rnd
	for op := range ops {
		switch r := rnd.Below(20); {
		case r < 3:
			nm.l.Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: crashWriteTimer, IO: core.IOID(1 + rnd.Below(4))})
		case r < 8:
			if hosted := c.hosted(); len(hosted) > 0 {
				c.post(hosted[rnd.Below(uint64(len(hosted)))], 1+int(rnd.Below(6)), 100+int(rnd.Below(800)))
			}
		case r < 13:
			c.propose(1+int(rnd.Below(6)), 100+int(rnd.Below(800)))
		case r < 16:
			c.tick(int(rnd.Below(300)))
		case r == 16:
			c.cut = !c.cut
		case r == 17:
			c.remove()
		default:
			if len(c.hosted()) < 2 {
				c.add()
			}
		}
		for range rnd.Below(30) {
			if c.crashNow {
				break
			}
			c.step()
		}
		if !c.crashNow && op%40 != 39 {
			continue
		}
		if !c.crashNow && rnd.Below(3) == 0 {
			nm.settle()
			c.observe()
		}
		c.crash(fmt.Sprintf("seed %d, crash after op %d", seed, op))
	}
	return c.c
}

// [property] MetaStore replicas on a device loop, single voters and the learner of a group whose
// voter is on another loop, take Txns while due checkpoints flush and compact them and merge
// their trees, the learner cut off and reconnected restores its voter's snapshots as runs,
// replicas are removed and new ones added, under removed ones' owner keys when allowed, and the
// cleaner frees segments, and the process crashes every 40 operations and soon after the learner
// takes a snapshot, with unflushed writes landing at random. Each restart restores every replica
// that started and was not removed, never one the device answered Gone, each replica to its
// group's state at its applied index, the voter and the learner with terms at least those they
// sent and the learner with every index it acknowledged; once they lead, each voter has applied
// every Txn it answered. Over its own seeds each count is at least about a quarter of what they
// reach, so a path that thins out fails as one that stops does.
func TestMetaReplicasOnADeviceCheckpointCompactAndRecover(t *testing.T) {
	var c metaCrashCounts
	seeds.Run(t, 0, 4, func(seed uint64) { c.add(metaCrashRun(t, seed, 400)) })
	t.Logf("%+v", c)
	floor := metaCrashCounts{phases: [3]int{36, 8, 16}, compacted: 43, onSnap: 12, snaps: 12, gone: 16, added: 16, reused: 14, dead: 300, freed: 160}
	if seeds.Default() && c.below(floor) {
		t.Fatalf("over the seeds: %+v, below %+v", c, floor)
	}
}
