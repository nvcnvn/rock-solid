package looptest

import (
	"reflect"
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/chunkstore/storetest"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/loop"
	"github.com/nvcnvn/rock-solid/internal/raftshard"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// storeShards runs a store of configuration cfg on a device loop, ID 1, that owns its log, with a
// Raft shard on the round driver of that loop and one on a producer loop, ID 0, both known to the
// store, which hears their replies; each new process starts its loops, and its shards empty, to
// restore from the runs the store mails them, when the store opens. removed is the ReplyRemoved replies the
// store heard, and reusable whether it found each one's owner reusable at once.
type storeShards struct {
	t        *testing.T
	cfg      chunkstore.Config
	sd       *storetest.SimDisk
	pool     core.BufPool
	s        *sim.Scheduler
	ring     *Disk
	loops    [2]*loop.Loop
	lg       *loop.Log
	r        *storetest.Rig
	maxBlob  uint64
	rounds   int
	shards   [2]*raftRounds
	removed  []loop.Reply
	reusable []bool
}

func newStoreShards(t *testing.T, cfg chunkstore.Config, rounds int) *storeShards {
	sh := &storeShards{t: t, cfg: cfg, sd: storetest.NewSimDisk(1), rounds: rounds}
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
		sh.shards[i].reply = true
		st.AddRemote(1001+uint32(i), i, uint32(sh.shards[i].id))
		l.Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: 1})
	}
}

func (sh *storeShards) start() {
	sh.s = sim.New(1, sim.Settings{PoolBytes: 1 << 20})
	node := sh.s.AddNode(sim.DefaultClock, nil, func(*sim.Node) {})
	sh.pool = sh.sd.Pool()
	for i := range sh.loops {
		k := NewKernel(sh.s, node.ID())
		if i == 0 {
			sh.loops[i] = loop.New(loop.Config{Node: node.ID(), ID: 0, Pool: sim.NewPool(1<<20, 0)}, k, &proposer{}, &diskDriver{})
			Add(node, k, sh.loops[i])
			continue
		}
		dr, err := NewDisk(k, sh.sd.D, sh.pool)
		if err != nil {
			sh.t.Fatal(err)
		}
		sh.t.Cleanup(dr.Close)
		l := loop.New(loop.Config{Node: node.ID(), ID: 1, Pool: sh.pool}, k, storeShardsMachine{sh}, diskDriver{dr})
		if err := l.AttachDisk(dr); err != nil {
			sh.t.Fatal(err)
		}
		sh.loops[i], sh.ring = l, dr
		sh.lg = l.AttachLog(nil, sh.cfg.Log, chunkstore.FirstLogIO, chunkstore.EndLogIO, 2)
		AddWithDisk(node, k, l, dr)
	}
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
}

func (sh *storeShards) mail(m *raftshard.Mail) {
	sh.lg.Send(m.Loop, loop.Mail{Shard: loop.ShardID(m.Shard), Kind: mailKinds[m.Kind], Owner: m.Owner, Ref: m.Ref, Blob: m.Blob, Data: m.Data, Pieces: m.Pieces, Base: m.Base, Share: m.Share})
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
	m.sh.r.OnEvent(ctx, ev)
	return out
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
		first, _ := rr.st.FirstIndex()
		last, _ := rr.st.LastIndex()
		ents, err := rr.st.Entries(first, last+1, 1<<62)
		if err != nil {
			sh.t.Fatal(err)
		}
		out[i] = shardState{hs: rr.st.HardState(), first: first, last: last, ents: ents, applied: rr.applied}
		for k := range uint64(raftKeys) {
			out[i].state[k], _ = rr.tree.Get(raftKey(k))
		}
	}
	return out
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
		a, b := after[i], before[i]
		if !sh.shards[i].recovered {
			sh.t.Fatalf("shard %d was never told the replay ended", i)
		}
		if a.hs != b.hs || a.first > b.first || a.last != b.last || !reflect.DeepEqual(a.ents[b.first-a.first:], b.ents) {
			sh.t.Fatalf("shard %d recovered %+v and entries %d to %d, held %+v and %d to %d", i, a.hs, a.first, a.last, b.hs, b.first, b.last)
		}
		if a.applied != b.applied || !reflect.DeepEqual(a.state, b.state) {
			sh.t.Fatalf("shard %d recovered its state at %d, held it at %d", i, a.applied, b.applied)
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
		sh.r.S.AddRemote(owner, 0, uint32(1+i))
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
	sh.r.S.AddRemote(1001, 0, uint32(fresh.id))
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
