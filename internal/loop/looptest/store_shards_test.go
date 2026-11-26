package looptest

import (
	"fmt"
	"reflect"
	"slices"
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/chunkstore/storetest"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/loop"
	"github.com/nvcnvn/rock-solid/internal/raftshard"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

// storeShards runs a store of configuration cfg on a device loop, ID 1, that owns its log, with a
// Raft shard on the round driver of that loop and one on a producer loop, ID 0, both known to the
// store, which hears their replies; each new process starts its loops, and its shards empty, to
// restore from the runs the store mails them, when the store opens, their merges paused with
// pause. removed is the ReplyRemoved replies the store heard, reusable whether it found each
// one's owner reusable at once, and manifests the newest durable manifest's LSN as the store mailed
// the shard on loop 1 each MailCheckpointed. sizes, bounded and late are each shard's, as
// raftRounds has them, and seed the simulator's.
type storeShards struct {
	t         *testing.T
	cfg       chunkstore.Config
	sd        *storetest.SimDisk
	pool      core.BufPool
	s         *sim.Scheduler
	ring      *Disk
	loops     [2]*loop.Loop
	lg        *loop.Log
	r         *storetest.Rig
	maxBlob   uint64
	rounds    int
	shards    [2]*raftRounds
	pause     bool
	removed   []loop.Reply
	reusable  []bool
	manifests []uint64
	sizes     *core.Rand
	bounded   bool
	late      bool
	seed      uint64
	// fds is the descriptors of the newest ring, which the test's cleanup closes: a cleanup
	// holding the ring would keep its pool live for the whole test, over every seed.
	fds *[2]int
}

func newStoreShards(t *testing.T, cfg chunkstore.Config, rounds int) *storeShards {
	return newStoreShardsOf(t, cfg, rounds, storetest.DiskBytes)
}

// newStoreShardsOf is newStoreShards on a disk of the given size.
func newStoreShardsOf(t *testing.T, cfg chunkstore.Config, rounds int, diskBytes uint64) *storeShards {
	return (&storeShards{t: t, cfg: cfg, sd: storetest.NewSimDiskOf(1, diskBytes), rounds: rounds, seed: 1}).format()
}

// format formats the store and adds its shards.
func (sh *storeShards) format() *storeShards {
	t, cfg := sh.t, sh.cfg
	sh.start()
	r := storetest.NewRig(t, sh, cfg)
	r.Opened, r.Mail = sh.opened, sh.mail
	sh.r, sh.maxBlob = r, r.S.Log().MaxBlobBytes()
	sh.opened(r.S)
	return sh
}

func (sh *storeShards) opened(st *chunkstore.Store) {
	if sh.pool != sh.sd.Pool() {
		sh.start()
	}
	for i, l := range sh.loops {
		sh.shards[i] = addRaftRounds(l, l.Context().Pool, sh.lg, 1001+uint32(i), sh.maxBlob, sh.rounds)
		rr := sh.shards[i]
		rr.reply, rr.pause, rr.sizes, rr.bounded, rr.late = true, sh.pause, sh.sizes, sh.bounded, sh.late
		st.AddRemote(1001+uint32(i), i, uint32(sh.shards[i].id), disklog.Replica{})
		l.Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: 1})
	}
}

func (sh *storeShards) start() {
	sh.s = sim.New(sh.seed, sim.Settings{PoolBytes: 1 << 20})
	node := sh.s.AddNode(sim.DefaultClock, nil, func(*sim.Node) {})
	sh.pool = sh.sd.Pool()
	for i := range sh.loops {
		k := NewKernel(sh.s, node.ID())
		if i == 0 {
			sh.loops[i] = loop.New(loop.Config{Node: node.ID(), ID: 0, Pool: sim.NewPool(1<<20, 0)}, k, actMachine{sh}, &diskDriver{})
			Add(node, k, sh.loops[i])
			continue
		}
		dr, err := NewDisk(k, sh.sd.D, sh.pool)
		if err != nil {
			sh.t.Fatal(err)
		}
		if sh.ring != nil {
			sh.ring.Close()
		}
		if sh.fds == nil {
			fds := new([2]int)
			sh.t.Cleanup(func() { (&Disk{fds: *fds}).Close() })
			sh.fds = fds
		}
		*sh.fds = dr.fds
		l := loop.New(loop.Config{Node: node.ID(), ID: 1, Pool: sh.pool}, k, storeShardsMachine{sh}, diskDriver{dr})
		if err := l.AttachDisk(dr); err != nil {
			sh.t.Fatal(err)
		}
		sh.loops[i], sh.ring = l, dr
		sh.lg = l.AttachLog(nil, sh.cfg.Log, chunkstore.FirstLogIO, chunkstore.EndLogIO, 2)
		AddWithDisk(node, k, l, dr)
	}
}

// close closes the newest ring, which the test's cleanup then leaves alone.
func (sh *storeShards) close() {
	sh.ring.Close()
	*sh.fds = [2]int{-1, -1}
}

// mailKinds is the loop's kind of each raftshard.MailKind.
var mailKinds = [...]loop.MailKind{
	raftshard.MailReplayed:     loop.MailReplayed,
	raftshard.MailRecovered:    loop.MailRecovered,
	raftshard.MailCheckpointed: loop.MailCheckpointed,
	raftshard.MailShare:        loop.MailShare,
	raftshard.MailFlush:        loop.MailFlush,
	raftshard.MailPiece:        loop.MailPiece,
	raftshard.MailFlushFailed:  loop.MailFlushFailed,
	raftshard.MailRun:          loop.MailRun,
	raftshard.MailRewrite:      loop.MailRewrite,
}

func (sh *storeShards) mail(m *raftshard.Mail) {
	if m.Kind == raftshard.MailCheckpointed && m.Loop == 1 {
		sh.manifests = append(sh.manifests, sh.r.S.Log().Manifest().LSN)
	}
	sh.lg.Send(m.Loop, loop.Mail{Shard: loop.ShardID(m.Shard), Kind: mailKinds[m.Kind], Owner: m.Owner, Ref: m.Ref, Blob: m.Blob, Data: m.Data, Pieces: m.Pieces, Base: m.Base, Share: m.Share, Bound: m.Bound})
}

func (sh *storeShards) Pool() core.BufPool     { return sh.sd.Pool() }
func (sh *storeShards) Blocks() uint64         { return sh.sd.Blocks() }
func (sh *storeShards) Execute(a *core.Action) { sh.ring.Execute(a) }
func (sh *storeShards) Complete([]core.Event) []core.Event {
	panic("looptest: the loop hands the store its completions")
}
func (sh *storeShards) Restart() { sh.sd.Restart() }

func (sh *storeShards) Serve(log *disklog.Log) {
	if sh.lg.Log() != log {
		sh.lg.Serve(log)
	}
}

func (sh *storeShards) Step(r *storetest.Rig) bool {
	sh.r = r
	return sh.s.Step()
}

type storeShardsMachine struct{ sh *storeShards }

func (m storeShardsMachine) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	if ev.Kind == core.EvTimer && ev.Timer == actTimer {
		m.sh.shards[1].act()
		return out
	}
	m.sh.r.OnEvent(ctx, ev)
	return out
}

// actTimer is the event act posts to a shard's loop.
const actTimer core.TimerID = 1 << 20

// actMachine runs loop 0, whose shard acts on actTimer.
type actMachine struct{ sh *storeShards }

func (m actMachine) OnEvent(_ *core.Context, ev *core.Event, out []core.Action) []core.Action {
	if ev.Kind == core.EvTimer && ev.Timer == actTimer {
		m.sh.shards[0].act()
	}
	return out
}

// act adds rounds rounds to shard i's burst and has it act on its loop.
func (sh *storeShards) act(i, rounds int) {
	sh.shards[i].left += rounds
	sh.loops[i].Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: actTimer})
}

func (m storeShardsMachine) OnLog(ctx *core.Context, _ *loop.Log, ev *disklog.Event, out []core.Action) []core.Action {
	m.sh.r.OnLog(ctx, ev)
	return out
}

func (m storeShardsMachine) OnReply(_ *loop.Log, r loop.Reply) {
	switch r.Kind {
	case loop.ReplyReplay:
		m.sh.r.S.RemoteReplay(r.Owner, r.LSN)
	case loop.ReplyFull:
		m.sh.r.S.RemoteFull()
	case loop.ReplyPiece:
		m.sh.r.S.RemotePiece(r.Owner, r.LSN, r.Data, r.Root)
	case loop.ReplyRuns:
		m.sh.r.S.RemoteRuns(r.Owner, r.LSN, r.Refs, r.Bounds)
	case loop.ReplyDead:
		m.sh.r.S.RemoteDead(r.Owner, r.Refs)
	case loop.ReplyCosts:
		m.sh.r.S.RemoteCosts(r.Owner, r.Costs)
	case loop.ReplyRemoved:
		m.sh.r.S.RemoveRemote(r.Owner, r.LSN)
		m.sh.removed = append(m.sh.removed, r)
		m.sh.reusable = append(m.sh.reusable, m.sh.r.S.Reusable(r.Owner))
	}
}

// shardState is a shard's storage, the index its state machine applied and its state there.
type shardState struct {
	hs          raftpb.HardState
	first, last uint64
	ents        []raftpb.Entry
	applied     uint64
	state       [raftKeys][]byte
}

func (sh *storeShards) storages() (out [2]shardState) {
	for i, rr := range sh.shards {
		if rr.err != nil {
			sh.t.Fatalf("shard %d: %v", i, rr.err)
		}
		out[i] = rr.state()
	}
	return out
}

func (rr *raftRounds) state() shardState {
	first, _ := rr.st.FirstIndex()
	last, _ := rr.st.LastIndex()
	ents, err := rr.st.Entries(first, last+1, 1<<62)
	rr.fail(err)
	out := shardState{hs: rr.st.HardState(), first: first, last: last, ents: ents, applied: rr.applied}
	for k := range uint64(raftKeys) {
		out.state[k], _ = rr.tree.Get(raftKey(k))
	}
	return out
}

// after is b once its storage takes round rd and its state machine applies it.
func (b shardState) after(rd *raftshard.Round) shardState {
	b.hs, b.ents, b.applied = rd.HardState, append(slices.Clip(b.ents), rd.Entries...), rd.HardState.Commit
	b.last = rd.Entries[len(rd.Entries)-1].Index
	for _, e := range rd.Entries {
		b.state[e.Index%raftKeys] = e.Data[:64]
	}
	return b
}

// sameAs reports how a, a recovered shard's state, differs from b, its state before: its hard
// state, its entries up to the same last index from the first b held, or from an older base its
// runs recorded, and its state at the same applied index.
func (a shardState) sameAs(b shardState) error {
	if a.hs != b.hs || a.first > b.first || a.last != b.last || !slices.EqualFunc(a.ents[b.first-a.first:], b.ents, func(x, y raftpb.Entry) bool { return reflect.DeepEqual(x, y) }) {
		return fmt.Errorf("recovered %+v and entries %d to %d, held %+v and %d to %d", a.hs, a.first, a.last, b.hs, b.first, b.last)
	}
	if a.applied != b.applied || !reflect.DeepEqual(a.state, b.state) {
		return fmt.Errorf("recovered its state at %d, held it at %d", a.applied, b.applied)
	}
	return nil
}

// restart ends the process and recovers the store, its shards making no new rounds, and checks
// that each of shards recovered equal to its state before: the same hard state, entries up to
// the same last index from the first it held, or from the base its runs recorded, before the
// compaction their manifest made durable, and the same state at the same applied index.
func (sh *storeShards) restart(shards ...int) {
	before := sh.storages()
	sh.rounds = 0
	sh.r.Restart()
	after := sh.storages()
	for _, i := range shards {
		if !sh.shards[i].recovered {
			sh.t.Fatalf("shard %d was never told the replay ended", i)
		}
		if err := after[i].sameAs(before[i]); err != nil {
			sh.t.Fatalf("shard %d %v", i, err)
		}
	}
}

// oldestLive is the LSN of the oldest blob of rr not reported dead.
func (rr *raftRounds) oldestLive() uint64 { return rr.refs[len(rr.dead)].LSN }

// TestRaftShardsOnTwoLoopsRecoverTheirStorage writes rounds of a Raft shard on the device loop and
// one on another loop, with no checkpoint due, and restarts the process: recovery mails each shard
// its blobs, and each equals its state before, with every blob of its rounds live in the log.
func TestRaftShardsOnTwoLoopsRecoverTheirStorage(t *testing.T) {
	sh := newStoreShards(t, storetest.Config(), 7)
	sh.r.Settle()
	for i, rr := range sh.shards {
		if rr.done != 7 || rr.flushes != 0 {
			t.Fatalf("shard %d made %d rounds of 7 and flushed %d times", i, rr.done, rr.flushes)
		}
	}
	live := sh.r.S.Log().LiveBlocks()
	sh.restart(0, 1)
	if got := sh.r.S.Log().LiveBlocks(); got != live {
		t.Fatalf("the recovered log holds %d live blocks, %d before", got, live)
	}
}

// shardReplay makes a checkpoint due every few rounds of a shard, yet lets two idle shards
// settle: a checkpoint's manifest and the hard-state round each writes again stay under it.
func shardReplay() chunkstore.Config {
	cfg := storetest.Config()
	cfg.Log.MaxReplayBytes = 32 << 10
	return cfg
}

// smallReplay makes a checkpoint due every few rounds of a shard.
func smallReplay() chunkstore.Config {
	cfg := storetest.Config()
	cfg.Log.MaxReplayBytes = 16 << 10
	return cfg
}

// TestCheckpointsNameTheShardsOnTheRoundDriver makes checkpoints due while Raft shards on the
// device loop and on another loop write rounds, and then, the shards idle, chunk writes make more
// due and merge steps run: each checkpoint, due or a merge step's, names both with the replay LSN
// they reported, so the replay floor moves yet never passes the oldest blob either still needs,
// though the index's replay LSN does. Idle, each shard compacts as due checkpoints' manifests are
// mailed durable and writes its hard state again, so the floor passes every blob it held when it
// went idle; and a restart recovers each storage whole.
func TestCheckpointsNameTheShardsOnTheRoundDriver(t *testing.T) {
	cfg := storetest.Config()
	cfg.Log.MaxReplayBytes = 64 << 10
	sh := newStoreShards(t, cfg, 30)
	step := func() bool {
		more := sh.r.Step()
		floor := sh.r.S.Log().ReplayFloor()
		for i, rr := range sh.shards {
			if len(rr.refs) > len(rr.dead) && floor >= rr.oldestLive() {
				t.Fatalf("the replay floor %d passed shard %d's live blob at %d", floor, i, rr.oldestLive())
			}
		}
		return more
	}
	for step() {
	}
	if sh.r.S.Log().ReplayFloor() == 0 {
		t.Fatal("no checkpoint moved the replay floor")
	}
	var idle [2]uint64
	for i, rr := range sh.shards {
		idle[i] = rr.refs[len(rr.refs)-1].LSN
	}
	for i := range 400 {
		sh.r.Write(storetest.Chunk(uint32(i%8)), 0, storetest.Fill(core.BlockSize, byte(i)), 0)
		for step() {
		}
	}
	for i, rr := range sh.shards {
		if floor := sh.r.S.Log().ReplayFloor(); floor < idle[i] || rr.rewrote == 0 {
			t.Fatalf("idle shard %d rewrote %d rounds and holds the replay floor at %d, at or before its blob at %d", i, rr.rewrote, floor, idle[i])
		}
	}
	sh.restart(0, 1)
}

// TestAFullShardGetsACheckpointFromAnIdleStore makes the Raft shard on each loop full with a
// share mail, so that it asks its idle store for a checkpoint from its Mail: the store, which no
// event wakes otherwise, takes one, has each shard flush its tree under it and mails each its
// manifest, for the shard on the device loop as for the one on another loop. The shard on the
// device loop does so alone too, its pieces' replies, mail and writes then the only events.
func TestAFullShardGetsACheckpointFromAnIdleStore(t *testing.T) {
	for _, c := range []struct {
		name  string
		full  int
		alone bool
	}{{"another loop", 0, false}, {"the device loop", 1, false}, {"the device loop alone", 1, true}} {
		t.Run(c.name, func(t *testing.T) {
			sh := newStoreShards(t, storetest.Config(), 3)
			sh.r.Settle()
			shards := sh.shards[:]
			if c.alone {
				sh.shards[0].remove()
				sh.r.Settle()
				shards = shards[1:]
			}
			rr := sh.shards[c.full]
			var before []int
			for _, o := range shards {
				before = append(before, o.checkpointed)
			}
			sh.lg.Send(c.full, loop.Mail{Shard: rr.id, Kind: loop.MailShare, Share: 1})
			sh.loops[1].Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: 1})
			sh.r.Settle()
			for k, o := range shards {
				if o.checkpointed != before[k]+1 {
					t.Fatalf("shard %d was mailed %d checkpoints after shard %d asked, %d before", o.owner, o.checkpointed, rr.owner, before[k])
				}
			}
		})
	}
}

// TestMailForAShardNotYetOnItsLoopWaitsForIt has the store learn of two shards on another loop
// before that loop adds them, as a node registers a shard with its store first, and chunk writes
// make it mail each its share as it changes: the mail waits on the loop, the newest share standing
// for the one before, and reaches the first shard once its loop adds it.
func TestMailForAShardNotYetOnItsLoopWaitsForIt(t *testing.T) {
	sh := newStoreShards(t, storetest.Config(), 3)
	sh.r.Settle()
	for i, owner := range []uint32{2001, 2002} {
		sh.r.S.AddRemote(owner, 0, uint32(1+i), disklog.Replica{})
		sh.r.Write(storetest.Chunk(uint32(i)), 0, storetest.Fill(core.BlockSize, byte(i)), 0)
		sh.r.Settle()
	}
	l := sh.loops[0]
	late := addRaftRounds(l, l.Context().Pool, sh.lg, 2001, sh.maxBlob, 0)
	l.Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: 1})
	sh.r.Settle()
	shares := sh.shards[0].shares
	if want := shares[len(shares)-1:]; late.id != 1 || !reflect.DeepEqual(late.shares, want) {
		t.Fatalf("shard %d added late was mailed the shares %v, want %v of the %v its loop's first shard was", late.id, late.shares, want, shares)
	}
}

// TestARemovedShardsOwnerOnTheRoundDriverWaitsForTheFloor removes the Raft shard on another loop
// with a round in flight, and then the one on the device loop: the store gives neither owner to a
// new shard until checkpoints move the replay floor past the newest blob of each, the round in
// flight's included, and a new shard given an owner then restarts with its own rounds alone.
func TestARemovedShardsOwnerOnTheRoundDriverWaitsForTheFloor(t *testing.T) {
	sh := newStoreShards(t, smallReplay(), 10)
	gone := sh.shards[0]
	for gone.done < 3 || gone.flush == gone.done || gone.rs.HasReady() {
		if !sh.r.Step() {
			t.Fatal("no round of shard 0 went in flight")
		}
	}
	gone.remove()
	sh.r.Settle()
	sh.shards[1].remove()
	sh.r.Settle()
	if len(sh.removed) != 2 || sh.removed[0].Owner != 1001 || sh.removed[1].Owner != 1002 {
		t.Fatalf("the store heard removals %+v", sh.removed)
	}
	if sh.reusable[0] {
		t.Fatal("the owner of a shard removed with a round in flight was reusable at once")
	}
	if newest := gone.refs[len(gone.refs)-1].LSN; sh.removed[0].LSN <= newest {
		t.Fatalf("shard 0 was removed with its newest blob at %d, past none of its round in flight's, after %d", sh.removed[0].LSN, newest)
	}
	for i := 0; !sh.r.S.Reusable(1001) || !sh.r.S.Reusable(1002); i++ {
		if floor := sh.r.S.Log().ReplayFloor(); floor >= sh.removed[0].LSN != sh.r.S.Reusable(1001) {
			t.Fatalf("owner 1001 reusable %v with the floor at %d and its newest blob at %d", sh.r.S.Reusable(1001), floor, sh.removed[0].LSN)
		}
		if i == 2000 {
			t.Fatal("the removed shards' owners never became reusable")
		}
		sh.r.Write(storetest.Chunk(uint32(i%8)), 0, storetest.Fill(4*core.BlockSize, byte(i)), 0)
		sh.r.Settle()
	}
	fresh := addRaftRounds(sh.loops[0], sh.loops[0].Context().Pool, sh.lg, 1001, sh.maxBlob, 3)
	fresh.reply = true
	sh.r.S.AddRemote(1001, 0, uint32(fresh.id), disklog.Replica{})
	sh.shards[0] = fresh
	sh.r.Settle()
	if fresh.done != 3 {
		t.Fatalf("the new shard made %d rounds of 3", fresh.done)
	}
	sh.restart(0)
}

// TestARemovedShardRepliesNothingMore removes the Raft shard on another loop: a replay LSN it
// reports after, or a second removal, could reach a new shard given its owner, so Reply panics.
func TestARemovedShardRepliesNothingMore(t *testing.T) {
	sh := newStoreShards(t, shardReplay(), 3)
	sh.r.Settle()
	gone := sh.shards[0]
	gone.remove()
	sh.r.Settle()
	for _, k := range []loop.ReplyKind{loop.ReplyReplay, loop.ReplyRemoved} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("a removed shard replied kind %d with no panic", k)
				}
			}()
			gone.l.Reply(gone.id, loop.Reply{Kind: k, Owner: gone.owner, LSN: 1})
		}()
	}
}

// TestAShardOnTheRoundDriverRestoresFromItsRunsAfterItCompacts has the Raft shard on each loop
// flush its tree under due checkpoints and compact to the index it applied at its mark as each
// manifest is mailed durable, and restarts the process after the last compaction, before any
// checkpoint follows: the replay no longer holds the rounds compacted, so each shard restores its
// state and its storage's base from the runs the manifest names, and then its rounds from the log.
func TestAShardOnTheRoundDriverRestoresFromItsRunsAfterItCompacts(t *testing.T) {
	sh := newStoreShards(t, shardReplay(), 10)
	sh.r.Settle()
	for i, rr := range sh.shards {
		if rr.checkpointed == 0 || rr.flushes != rr.checkpointed || rr.st.Base().Index == 0 || rr.tree.Runs() == 0 {
			t.Fatalf("shard %d flushed %d times, was mailed %d checkpoints, compacted to %d and holds %d runs", i, rr.flushes, rr.checkpointed, rr.st.Base().Index, rr.tree.Runs())
		}
	}
	sh.restart(0, 1)
}

// TestARemovedShardsRunsStayLiveUntilAManifestLeavesItOut takes the Raft shard on another loop
// off it part way through its tree's flush under a checkpoint, and tells the store only after the
// next checkpoint, as its loop holds the ReplyRemoved while its batches wait behind checkpoint
// backpressure: both checkpoints, which ask it for pieces it can no longer reply, still end, and
// the segments holding its runs' pieces keep every live block through them, as their manifests
// still name them, and so does the one running once the store hears of the removal.
func TestARemovedShardsRunsStayLiveUntilAManifestLeavesItOut(t *testing.T) {
	sh := newStoreShards(t, shardReplay(), 30)
	gone, other := sh.shards[0], sh.shards[1]
	for gone.tree.Runs() == 0 || !gone.tree.Frozen() {
		if !sh.r.Step() {
			t.Fatal("shard 0 never flushed under a checkpoint with runs to keep, part of it written")
		}
	}
	live := map[uint32]disklog.SegmentUsage{}
	for _, p := range gone.tree.Pieces(nil) {
		live[p.Segment], _ = sh.r.S.Log().Usage(p.Segment)
	}
	keepLive := func(until int, what string) {
		for other.checkpointed < until {
			for seg, was := range live {
				if u, _ := sh.r.S.Log().Usage(seg); u.Live < was.Live || u.State == disklog.SegmentFree {
					t.Fatalf("segment %d holding the removed shard's pieces fell to %d live blocks from %d before a manifest left it out", seg, u.Live, was.Live)
				}
			}
			if !sh.r.Step() {
				t.Fatalf("%s never ended: shard 1 was mailed %d checkpoints of %d", what, other.checkpointed, until)
			}
		}
	}
	gone.l.RemoveShard(gone.id)
	keepLive(other.checkpointed+2, "the checkpoints running at the removal and after")
	newest := gone.rs.Delete()
	gone.bury()
	gone.l.Reply(gone.id, loop.Reply{Kind: loop.ReplyRemoved, Owner: gone.owner, LSN: newest})
	keepLive(other.checkpointed+1, "the checkpoint after the store heard of the removal")
}

func TestAShardOnTheRoundDriverKeepsItsLoadedRunsAcrossASecondRestart(t *testing.T) {
	sh := newStoreShards(t, shardReplay(), 10)
	sh.r.Settle()
	sh.restart(0, 1)
	before := sh.shards[1].checkpointed
	for i := 0; i < 8; i++ {
		sh.r.Write(storetest.Chunk(uint32(i%8)), 0, storetest.Fill(4*core.BlockSize, byte(i)), 0)
		sh.r.Settle()
	}
	if sh.shards[1].checkpointed <= before {
		t.Fatalf("no checkpoint after the restart: %d, %d", sh.shards[1].checkpointed, before)
	}
	sh.restart(0, 1)
}

// TestAShardOnTheRoundDriverMergesItsRuns has the Raft shard on each loop merge its tree's runs in
// steps on its own loop as due checkpoints flush them, until a manifest names a merge part way,
// where both shards stop, and chunk writes then make more checkpoints due: each manifest names the
// runs the shard's flush carries, so every piece its merges left out is dead in the log once a
// checkpoint begun after its report ends, and a restart mails it the runs, and bounds, that the
// newest manifest it was mailed durable named, and recovers each storage whole.
func TestAShardOnTheRoundDriverMergesItsRuns(t *testing.T) {
	sh := newStoreShards(t, shardReplay(), 40)
	for sh.shards[0].named().bounds == nil && sh.shards[1].named().bounds == nil {
		if !sh.r.Step() {
			t.Fatal("no manifest named a merge part way")
		}
	}
	var gone []disklog.BlobRef
	sh.pause = true
	for _, rr := range sh.shards {
		rr.pause, rr.left = true, 0
	}
	sh.r.Settle()
	for i, rr := range sh.shards {
		if rr.steps == 0 || len(rr.gone) == 0 || rr.tree.Runs() >= rr.flushes {
			t.Fatalf("shard %d installed %d merge steps, reported %d pieces dead and holds %d runs after %d flushes", i, rr.steps, len(rr.gone), rr.tree.Runs(), rr.flushes)
		}
		gone = append(gone, rr.gone...)
	}
	before, log := sh.shards[1].checkpointed, sh.r.S.Log()
	var reported uint64
	for i := 0; reported == 0 || log.NamedLSN() < reported; i++ {
		if i == 200 {
			t.Fatalf("chunk writes made %d checkpoints, and no segment header named the manifest at %d", sh.shards[1].checkpointed-before, reported)
		}
		sh.r.Write(storetest.Chunk(uint32(i%8)), 0, storetest.Fill(4*core.BlockSize, byte(i)), 0)
		sh.r.Settle()
		if reported == 0 && sh.shards[1].checkpointed >= before+2 {
			reported = log.Manifest().LSN
		}
	}
	for _, p := range gone {
		if err := log.PiecesDead([]disklog.BlobRef{p}); err == nil {
			t.Fatalf("the log still holds piece %+v a merge left out", p)
		}
	}
	named := [2]runSet{sh.shards[0].named(), sh.shards[1].named()}
	if named[0].bounds == nil && named[1].bounds == nil {
		t.Fatal("no manifest names a merge part way")
	}
	sh.restart(0, 1)
	for i, rr := range sh.shards {
		if !rr.loaded.equal(named[i]) {
			t.Fatalf("shard %d was mailed runs %v bounded by %+v, the manifest it was mailed durable named %v bounded by %+v", i, rr.loaded.runs, rr.loaded.bounds, named[i].runs, named[i].bounds)
		}
	}
}

// TestACrashRightAfterAManifestNamesAShardsMergedRunsRestoresThem has the Raft shard on each loop
// merge its tree's runs as due checkpoints flush them, and stops both shards once a flush carries
// other runs than the manifest before named: the process crashes as soon as a segment header
// names that flush's manifest, before any later manifest is durable, and the restart mails each
// shard exactly the runs, and bounds, its flush named in it.
func TestACrashRightAfterAManifestNamesAShardsMergedRunsRestoresThem(t *testing.T) {
	sh := newStoreShards(t, shardReplay(), 40)
	rr := sh.shards[1]
	for rr.moved == 0 || rr.moved != len(rr.sets) {
		if !sh.r.Step() {
			t.Fatal("no flush carried other runs than the manifest before named")
		}
	}
	sh.pause, sh.rounds = true, 0
	for _, s := range sh.shards {
		s.pause, s.left = true, 0
	}
	for rr.checkpointed < rr.moved {
		if !sh.r.Step() {
			t.Fatalf("the checkpoint of flush %d never ended", rr.moved)
		}
	}
	log, m := sh.r.S.Log(), sh.manifests[rr.moved-1]
	for i := 0; log.NamedLSN() < m; i++ {
		if log.Manifest().LSN != m {
			t.Fatalf("a manifest at %d was durable before a segment header named the one at %d", log.Manifest().LSN, m)
		}
		if !sh.r.Step() {
			sh.r.Write(storetest.Chunk(uint32(i%8)), 0, storetest.Fill(4*core.BlockSize, byte(i)), 0)
		}
	}
	if log.Manifest().LSN != m {
		t.Fatalf("a manifest at %d was durable as a segment header named the one at %d", log.Manifest().LSN, m)
	}
	named := rr.sets[rr.moved-1]
	sh.r.D.Restart()
	sh.r.Reopen()
	sh.r.Settle()
	if got := sh.shards[1].loaded; !got.equal(named) {
		t.Fatalf("the crash restored runs %v bounded by %+v, where the flush the manifest names named %v bounded by %+v", got.runs, got.bounds, named.runs, named.bounds)
	}
}

// TestAnIdleStoreRewritesTheRunsOfAShardOnTheRoundDriver has the Raft shard on each loop flush
// and merge its tree under due checkpoints, and chunk writes then make more due, until segments
// that hold only the shards' run pieces are behind the replay floor. The process restarts with the
// cleaner keeping more segments free than the disk has, the shards making no rounds and nothing
// writing chunks: no checkpoint is due, and those segments are the only metadata victims that can
// free room. The cleaner weighs them from the runs each shard replied and mails the shard to
// rewrite them, and each step asks for a checkpoint, so the rewrites end and the shards report
// every old piece dead with nothing written but the checkpoints. Their segments free only once a
// segment header names the manifest after, which chunk writes then open; the log holds none of
// them, and the storages recover whole.
func TestAnIdleStoreRewritesTheRunsOfAShardOnTheRoundDriver(t *testing.T) {
	sh := newStoreShardsOf(t, shardReplay(), 300, 4<<30)
	sh.r.Settle()
	var old []disklog.BlobRef
	for i := 0; len(old) == 0; i++ {
		if i == 2000 {
			t.Fatal("no segment behind the replay floor held only the shards' run pieces")
		}
		sh.r.Write(storetest.Chunk(0), 0, storetest.Fill(4*core.BlockSize, byte(i)), 0)
		sh.r.Settle()
		old = sh.onlyShardPieces()
	}
	sh.r.Cfg.CleanFree = 1 << 20
	sh.restart(0, 1)
	for _, p := range old {
		if !slices.Contains(sh.shards[0].gone, p) && !slices.Contains(sh.shards[1].gone, p) {
			t.Fatalf("idle, the shards rewrote %d and %d runs the cleaner marked and never reported piece %+v dead", sh.shards[0].rewrites, sh.shards[1].rewrites, p)
		}
	}
	log := sh.r.S.Log()
	m := log.Manifest().LSN
	for i := 0; log.NamedLSN() < m; i++ {
		if i == 200 {
			t.Fatalf("no segment header named the manifest at %d", m)
		}
		sh.r.Write(storetest.Chunk(0), 0, storetest.Fill(4*core.BlockSize, byte(i)), 0)
		sh.r.Settle()
	}
	for _, p := range old {
		if u, _ := log.Usage(p.Segment); u.State != disklog.SegmentFree && u.Newest <= m {
			t.Fatalf("segment %d of only the shards' old pieces was not freed: %+v", p.Segment, u)
		}
		if err := log.PiecesDead([]disklog.BlobRef{p}); err == nil {
			t.Fatalf("the log still holds piece %+v in a segment of only the shards' pieces", p)
		}
	}
	sh.r.Cfg.CleanFree = 0
	sh.restart(0, 1)
}

// onlyShardPieces is the shards' run pieces in segments the cleaner may take that hold no piece
// of the chunk index or of a manifest.
func (sh *storeShards) onlyShardPieces() []disklog.BlobRef {
	log := sh.r.S.Log()
	index := map[uint32]bool{}
	for _, p := range sh.r.S.Index().Tree().Pieces(nil) {
		index[p.Segment] = true
	}
	var only []disklog.BlobRef
	for _, rr := range sh.shards {
		for _, p := range rr.tree.Pieces(nil) {
			if !index[p.Segment] && !log.HoldsManifestPiece(p.Segment) && log.Cleanable(p.Segment) {
				only = append(only, p)
			}
		}
	}
	return only
}

// crash cuts the power, unflushed writes landing as rnd says, and recovers the store, its shards
// making no new rounds. It reports how a shard's state when its replay ended differs from its
// state before the crash, or from that state and the round it had in flight, which may land.
func (sh *storeShards) crash(rnd *core.Rand) error {
	before := sh.storages()
	var flying [2]*raftshard.Round
	for i, rr := range sh.shards {
		if rr.flush > rr.done {
			flying[i] = &rr.round
		}
	}
	sh.sd.Crash(rnd)
	sh.r.Reopen()
	sh.r.Settle()
	for i, rr := range sh.shards {
		if !rr.recovered {
			return fmt.Errorf("shard %d was never told the replay ended", i)
		}
		b := before[i]
		if flying[i] != nil && rr.atRecovery.hs != b.hs {
			b = b.after(flying[i])
		}
		if err := rr.atRecovery.sameAs(b); err != nil {
			return fmt.Errorf("shard %d %w", i, err)
		}
	}
	return nil
}

// crashCounts is what a crash run of shards on the round driver did: restarts that restored a
// shard on a compacted base, and those whose replay began past a blob it reported dead before;
// merge steps its shards installed, and runs they rewrote as the cleaner marked them; segments the
// cleaner freed that held a Raft blob, or a run piece, reported dead; and phases, each shard at
// each crash by its phase.
type crashCounts struct {
	restored, skipped, steps, rewrites, freedBlobs, freedPieces int
	phases                                                      [3]int
}

// phase is where the shard's part of a checkpoint stands: 0 with no flush of it running, 1 with
// a piece of it in flight, 2 with every piece durable and the manifest not yet mailed durable.
func (rr *raftRounds) phase() int {
	switch {
	case rr.flushes == rr.checkpointed:
		return 0
	case !rr.ended:
		return 1
	}
	return 2
}

func (c *crashCounts) add(r crashCounts) {
	c.restored, c.skipped, c.steps, c.rewrites = c.restored+r.restored, c.skipped+r.skipped, c.steps+r.steps, c.rewrites+r.rewrites
	c.freedBlobs, c.freedPieces = c.freedBlobs+r.freedBlobs, c.freedPieces+r.freedPieces
	for i := range c.phases {
		c.phases[i] += r.phases[i]
	}
}

// shardCrashRun has the Raft shards on both loops make ops bursts of 1 to 4 rounds of seed's
// sizes, or chunk writes, a third of the time, stepping up to 29 times after each, and crashes the
// process every 50, a third of the time once it settles. The disk's 256 KiB segments are large
// enough that rewriting a shard's runs frees room, and the cleaner wants all but 8 free.
func shardCrashRun(t *testing.T, seed uint64, ops int) (c crashCounts) {
	cfg := storetest.Config()
	cfg.Log.MaxReplayBytes = 64 << 10
	const diskBytes = 1 << 30
	geo, err := disklog.GeometryFor(diskBytes)
	if err != nil {
		t.Fatal(err)
	}
	cfg.CleanFree = int(geo.SegmentCount) - 8
	sh := (&storeShards{t: t, cfg: cfg, sd: storetest.NewSimDiskOf(1, diskBytes), seed: seed, sizes: core.NewRand(^seed)}).format()
	rnd := core.NewRand(seed)
	var inv sim.Invariants
	var recovered error
	inv.Register("a shard recovered from manifest and log equals its state before the crash", func() error { return recovered })
	dead, gone := map[uint32]bool{}, map[uint32]bool{}
	var seen [2][2]int
	note := func() {
		for i, rr := range sh.shards {
			for _, ref := range rr.dead[seen[i][0]:] {
				dead[ref.Segment] = true
			}
			for _, ref := range rr.gone[seen[i][1]:] {
				gone[ref.Segment] = true
			}
			seen[i] = [2]int{len(rr.dead), len(rr.gone)}
		}
		log := sh.r.S.Log()
		for _, f := range []struct {
			segs map[uint32]bool
			n    *int
		}{{dead, &c.freedBlobs}, {gone, &c.freedPieces}} {
			for seg := range f.segs {
				if u, _ := log.Usage(seg); u.State == disklog.SegmentFree {
					*f.n++
					delete(f.segs, seg)
				}
			}
		}
	}
	for op := range ops {
		if rnd.Below(3) == 0 {
			sh.r.Write(storetest.Chunk(uint32(rnd.Below(8))), 0, storetest.Fill(int(1+rnd.Below(4))*core.BlockSize, byte(op)), 0)
		} else {
			sh.act(int(rnd.Below(2)), 1+int(rnd.Below(4)))
		}
		for range rnd.Below(30) {
			sh.r.Step()
		}
		note()
		if op%50 != 49 {
			continue
		}
		if rnd.Below(3) == 0 {
			sh.r.Settle()
			note()
		}
		old := sh.shards
		for _, rr := range old {
			c.phases[rr.phase()]++
		}
		recovered, seen = sh.crash(rnd), [2][2]int{}
		if err := inv.Check(); err != nil {
			t.Fatalf("crash after op %d: %v", op, err)
		}
		for i, rr := range sh.shards {
			c.steps, c.rewrites = c.steps+old[i].steps, c.rewrites+old[i].rewrites
			if rr.atRecovery.first > 1 {
				c.restored++
			}
			if len(old[i].dead) > 0 && (rr.replayed == 0 || rr.replayed > old[i].dead[0].LSN) {
				c.skipped++
			}
		}
	}
	sh.r.Settle()
	note()
	for _, rr := range sh.shards {
		c.steps, c.rewrites = c.steps+rr.steps, c.rewrites+rr.rewrites
	}
	sh.storages()
	sh.close()
	return c
}

// [property] Raft shards on the round driver, on the device loop and on another, make bursts of
// rounds beside chunk writes while due checkpoints flush their trees and their merges' steps and
// mail them to compact, and the cleaner frees segments and marks their runs to rewrite, and the
// process crashes every 50 bursts or writes with unflushed writes landing at random. Each
// recovered shard, restored from its manifest's runs and base and replayed from its replay LSN,
// equals its state before the crash with or without the round it had in flight. Over the seeds
// restarts restore compacted bases and replay past blobs reported dead, a shard is at crashes
// with no flush of it running, with a piece in flight and with every piece durable and the
// manifest not yet mailed durable, the shards merge and rewrite runs the cleaner marked, and the
// cleaner frees segments that held their dead blobs and dead run pieces.
func TestShardsOnTheRoundDriverCheckpointCompactAndRecover(t *testing.T) {
	var c crashCounts
	seeds.Run(t, 0, 4, func(seed uint64) {
		c.add(shardCrashRun(t, seed, 1000))
	})
	t.Logf("%+v", c)
	if c.restored == 0 || c.skipped == 0 || c.steps == 0 || c.rewrites == 0 || c.freedBlobs == 0 || c.freedPieces == 0 || slices.Contains(c.phases[:], 0) {
		t.Fatalf("over the seeds: %+v", c)
	}
}

// shardBoundRun has the Raft shards on both loops make bursts of 1 to 6 rounds of seed's sizes,
// each proposed only while the shard is not full, beside chunk writes, ops times, with each round
// told durable reaching its storage only when the test has the shard act, and checks at every
// step that each retains at most its share of twice max_replay_bytes and a round: its storage's
// entries and those of the rounds it proposed that its storage has not taken.
func shardBoundRun(t *testing.T, seed uint64, ops int) {
	cfg := storetest.Config()
	cfg.Log.MaxReplayBytes = 256 << 10
	sh := (&storeShards{t: t, cfg: cfg, sd: storetest.NewSimDiskOf(1, storetest.DiskBytes), seed: seed, sizes: core.NewRand(^seed), bounded: true, late: true}).format()
	rnd := core.NewRand(seed)
	share := cfg.Log.MaxReplayBytes
	const round = 8 * 8000
	bounded := func(op int) {
		for i, rr := range sh.shards {
			if n := len(rr.shares); n > 0 && rr.shares[n-1] != share {
				t.Fatalf("op %d: shard %d was mailed shares %v, not %d", op, i, rr.shares, share)
			}
			if b := rr.st.Bytes() + rr.held(); b > share+round {
				t.Fatalf("op %d: shard %d retains %d bytes, past its share %d and a round", op, i, b, share)
			}
		}
	}
	for op := range ops {
		switch rnd.Below(4) {
		case 0:
			sh.r.Write(storetest.Chunk(uint32(rnd.Below(8))), 0, storetest.Fill(int(1+rnd.Below(4))*core.BlockSize, byte(op)), 0)
		case 1:
			sh.act(int(rnd.Below(2)), 0)
		default:
			sh.act(int(rnd.Below(2)), 1+int(rnd.Below(6)))
		}
		for range rnd.Below(30) {
			sh.r.Step()
			bounded(op)
		}
	}
	sh.act(0, 0)
	sh.act(1, 0)
	sh.r.Settle()
	bounded(ops)
	for i, rr := range sh.shards {
		if rr.err != nil || rr.refused == 0 || rr.wrote < 4*share {
			t.Fatalf("shard %d proposed %d bytes, refused %d bursts while full, err %v", i, rr.wrote, rr.refused, rr.err)
		}
	}
	if !sh.r.S.Serving() {
		t.Fatalf("the store stopped: %v", sh.r.S.Err())
	}
	sh.close()
}

// [property] Raft shards on the round driver, on the device loop and on another, retain at most
// their share of twice max_replay_bytes and a round under bursts of proposals, rounds told
// durable reaching their storage late, and chunk writes beside them.
func TestShardsOnTheRoundDriverRetainAtMostTheirShare(t *testing.T) {
	seeds.Run(t, 0, 4, func(seed uint64) { shardBoundRun(t, seed, 600) })
}
