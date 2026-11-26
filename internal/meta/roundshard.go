package meta

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/loop"
	"github.com/nvcnvn/rock-solid/internal/raftshard"
)

// ErrOverloaded refuses a Txn at a leader whose disk holds its share of retained entries; it is
// not proposed, and the client may send it again once a checkpoint compacts the replica.
var ErrOverloaded = errors.New("meta: the replica retains its share of entries on its disk")

// bootRound is the number the round of a new replica's boot state goes out under, which no
// round of the replica's own takes.
const bootRound = math.MaxUint64

// nameRound is the number a new replica writes its boot state again under once a due checkpoint's
// manifest naming it is durable: the store has asked the log to name that manifest in a segment
// header (disklog.Log.NameNewest), so once the round is durable a restart restores the replica.
const nameRound = math.MaxUint64 - 1

// RoundConfig is a replica on the round driver's settings.
type RoundConfig struct {
	// Shard is the replica's, its storage's base the group's boot state: the voters' configuration.
	Shard Config
	// Owner is the owner key its blobs carry on its disk's log, and MaxBlob the log's MaxBlobBytes.
	Owner   uint32
	MaxBlob uint64
	// Open restores the replica from its disk's restart, which mails it its runs, its blobs and
	// the end of the replay; without it the replica is new.
	Open bool
	// Answered takes the answers to Txns the replica proposed, valid until it returns.
	Answered func(ctx *core.Context, as []Answer, out []core.Action) []core.Action
}

// RoundShard is a MetaStore replica on a storage node's loop, whose rounds go to its disk's log
// through the round driver: the loop.Shard that composes the replica, its state machine and its
// raftshard.Rounds, and answers its disk's owner's mail. A new replica writes the group's boot
// state as its first round, asks for a due checkpoint, and starts once that round is durable and
// so is a due checkpoint's manifest naming it (MailCheckpointed), as a restart restores only the
// replicas its manifest names, so it acknowledges nothing a crash would make it forget; one its
// disk restores starts once
// the replay ends, from the runs and blobs mailed to it, or with none mailed writes its boot
// state as a new one does. While Rounds reports it full it drops the appends sent to it and
// refuses Txns with ErrOverloaded.
type RoundShard struct {
	l     *loop.Loop
	id    loop.ShardID
	cfg   RoundConfig
	boot  raftpb.SnapshotMetadata
	st    *raftshard.Storage
	rs    *raftshard.Rounds
	state *State
	shard *Shard
	out   Output
	err   error
	// replayed says the restart mailed a run or a blob, restored that the runs' base is the
	// storage's; share is the share last mailed, and dead the blobs being reported dead.
	replayed, restored bool
	share              uint64
	// booted says the boot round of a new replica is durable, manifested that a due checkpoint's
	// manifest naming it is, and named that its name round is, so a restart restores it.
	booted, manifested, named bool
	dead                      []disklog.BlobRef
	// flushing says the running checkpoint began the tree's flush, cp is its number, and mark and
	// conf the index applied at its mark, which the log compacts to once its manifest is durable.
	flushing bool
	cp, mark uint64
	conf     raftpb.ConfState
	costs    []disklog.RunCost
}

// NewRoundShard puts the replica cfg describes on l, its rounds going to lg; the disk's store
// knows it by its owner and the ShardID it returns (chunkstore.Store.AddRemote) within the same
// pass, before its first round can be durable.
func NewRoundShard(l *loop.Loop, lg *loop.Log, cfg RoundConfig) *RoundShard {
	a := &RoundShard{l: l, cfg: cfg, boot: cfg.Shard.Raft.Storage.Base(), named: cfg.Open}
	a.st = cfg.Shard.Raft.Storage
	if cfg.Open {
		a.st = raftshard.NewStorage()
	}
	a.rs = raftshard.NewRounds(cfg.Owner, a.st, cfg.MaxBlob)
	a.id = l.AddShard(a, lg)
	if !cfg.Open {
		a.writeBoot()
		a.l.Reply(a.id, loop.Reply{Kind: loop.ReplyFull, Owner: cfg.Owner})
	}
	return a
}

// ReplicaID is the replica's ID in its group.
func (a *RoundShard) ReplicaID() uint64 { return a.cfg.Shard.Raft.ID }

// Owner is the owner key its blobs carry on its disk's log.
func (a *RoundShard) Owner() uint32 { return a.cfg.Owner }

// ID is the replica's ShardID on its loop.
func (a *RoundShard) ID() loop.ShardID { return a.id }

// Shard is the replica once it has started, or nil.
func (a *RoundShard) Shard() *Shard { return a.shard }

// Storage is the replica's Raft storage.
func (a *RoundShard) Storage() *raftshard.Storage { return a.st }

// Err is why the replica stopped for good: its disk failed its round, or its runs or blobs do
// not restore.
func (a *RoundShard) Err() error { return a.err }

func (a *RoundShard) writeBoot() {
	a.rs.Write(bootRound, &raftshard.Round{Snapshot: raftpb.Snapshot{Metadata: a.boot}})
	a.l.Mark(a.id)
}

func (a *RoundShard) writeName() {
	a.rs.Write(nameRound, &raftshard.Round{Snapshot: raftpb.Snapshot{Metadata: a.boot}})
	a.l.Mark(a.id)
}

func (a *RoundShard) fail(err error) {
	if err != nil && a.err == nil {
		a.err = err
	}
}

// OnEvent hands the replica a timer or a message of its group; a replica not yet started, or
// stopped, takes none, though one not started is marked again for a boot round its pool had no
// buffers for. While full it drops a Raft append, which its leader sends again.
func (a *RoundShard) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	if a.shard == nil || a.err != nil || ev.Kind == core.EvNetRecv && a.rs.Full() && raftshard.Appends(ctx.Pool, &ev.Msg) {
		if ev.Kind == core.EvNetRecv {
			free(ctx, &ev.Msg)
		}
		if a.HasReady() {
			a.l.Mark(a.id)
		}
		return out
	}
	a.shard.OnEvent(ctx, ev, &a.out)
	return a.after(ctx, out)
}

// Txn proposes t, as Shard.Txn, or refuses it with ErrOverloaded while the replica leads full, or
// with a NotLeader naming no leader before it starts.
func (a *RoundShard) Txn(ctx *core.Context, t *Txn, token uint64, out []core.Action) ([]core.Action, error) {
	if token&ServerToken != 0 {
		return out, ErrServerToken
	}
	return a.submit(ctx, t, token, out)
}

func (a *RoundShard) submit(ctx *core.Context, t *Txn, token uint64, out []core.Action) ([]core.Action, error) {
	switch {
	case a.shard == nil || a.err != nil:
		return out, NotLeader{Group: a.cfg.Shard.Raft.Group}
	case a.rs.Full() && a.shard.Raft().Leader() == a.cfg.Shard.Raft.ID:
		return out, ErrOverloaded
	}
	err := a.shard.submit(ctx, t, token, &a.out)
	return a.after(ctx, out), err
}

// after writes the replica's new round, hands over its answers and actions, and tells the loop
// and the disk's owner what its rounds changed.
func (a *RoundShard) after(ctx *core.Context, out []core.Action) []core.Action {
	o := &a.out
	if o.Raft.Flush != 0 {
		a.rs.Write(o.Raft.Flush, &o.Raft.Round)
	}
	out = a.answer(ctx, out)
	out = append(out, o.Raft.Actions...)
	o.Reset()
	a.bury()
	return out
}

// answer hands the replica's answers to Answered, with every Txn it holds answered dropped once
// it stopped, as it applies nothing more.
func (a *RoundShard) answer(ctx *core.Context, out []core.Action) []core.Action {
	o := &a.out
	if a.err != nil && a.shard != nil {
		a.shard.drop(o)
	}
	if len(o.Answers) > 0 && a.cfg.Answered != nil {
		out = a.cfg.Answered(ctx, o.Answers, out)
	}
	o.Answers = o.Answers[:0]
	return out
}

// bury reports the blobs that died and the replay LSN as it moves, asks for a checkpoint once
// full with a commit applied, and marks the replica while blobs wait.
func (a *RoundShard) bury() {
	if a.dead = a.rs.Dead(a.dead[:0]); len(a.dead) > 0 {
		a.l.Dead(a.id, a.dead)
	}
	if lsn, ok := a.rs.ReplayLSN(); ok {
		a.l.Reply(a.id, loop.Reply{Kind: loop.ReplyReplay, Owner: a.cfg.Owner, LSN: lsn})
	}
	if a.shard != nil {
		if i, _ := a.shard.Applied(); a.rs.FullDue(i) {
			a.l.Reply(a.id, loop.Reply{Kind: loop.ReplyFull, Owner: a.cfg.Owner})
		}
	}
	if a.rs.HasReady() {
		a.l.Mark(a.id)
	}
}

// HasReady implements loop.Shard.
func (a *RoundShard) HasReady() bool { return a.err == nil && a.rs.HasReady() }

// Ready implements loop.Shard.
func (a *RoundShard) Ready(items []disklog.Item) []disklog.Item {
	items, err := a.rs.Ready(a.l.Context().Pool, items)
	a.fail(err)
	return items
}

// Durable implements loop.Shard: the replica takes its round, or starts once its boot round is
// durable if a durable manifest names it; a round of its the log failed stops it, and one of Rounds' own a later Rewrite writes
// again.
func (a *RoundShard) Durable(ctx *core.Context, err error, refs []disklog.BlobRef, out []core.Action) []core.Action {
	round, err := a.rs.Durable(err, refs)
	switch {
	case round == 0:
	case err != nil:
		a.fail(fmt.Errorf("meta: round %d of shard %d: %w", round, a.cfg.Owner, err))
	case a.err != nil:
	case round == bootRound && a.named:
		a.start(ctx)
	case round == bootRound:
		if a.booted = true; a.manifested {
			a.writeName()
		}
	case round == nameRound:
		a.named = true
		a.start(ctx)
	default:
		a.shard.Durable(ctx, round, &a.out)
	}
	return a.after(ctx, out)
}

// start starts the replica on its storage and state.
func (a *RoundShard) start(ctx *core.Context) {
	cfg := a.cfg.Shard
	cfg.Raft.Storage, cfg.State = a.st, a.state
	s, err := NewShard(cfg)
	if err != nil {
		a.state = nil
		a.fail(err)
		return
	}
	a.shard, a.state = s, s.State()
	s.Start(ctx, &a.out)
}

// ErrRemoved is why a replica taken off its loop by Remove stopped.
var ErrRemoved = errors.New("meta: the replica was removed from its disk")

// Remove takes the replica off its loop for good and deletes its state: every Txn it holds is
// answered dropped, every blob of it reported dead, and its disk's owner told, with the newest
// blob's LSN, that the replica is gone (loop.ReplyRemoved). It gives its page slabs back, and may
// not be used after.
func (a *RoundShard) Remove(ctx *core.Context, out []core.Action) []core.Action {
	a.l.RemoveShard(a.id)
	newest := a.rs.Delete()
	a.fail(ErrRemoved)
	out = a.answer(ctx, out)
	if a.dead = a.rs.Dead(a.dead[:0]); len(a.dead) > 0 {
		a.l.Dead(a.id, a.dead)
	}
	a.l.Reply(a.id, loop.Reply{Kind: loop.ReplyRemoved, Owner: a.cfg.Owner, LSN: newest})
	a.Close()
	return out
}

// Close gives the replica's page slabs back; it may not be used after.
func (a *RoundShard) Close() {
	if a.state != nil {
		a.state.Close()
	}
}

// Mail implements loop.Shard: it answers every ask for a checkpoint's piece, with none from a
// replica that has no tree yet or stopped, so no checkpoint waits on it.
func (a *RoundShard) Mail(ctx *core.Context, m *loop.Mail, out []core.Action) []core.Action {
	switch m.Kind {
	case loop.MailRun:
		a.loadRun(m)
	case loop.MailReplayed:
		a.replayed = true
		a.restore()
		a.fail(a.rs.Replay(m.Ref, m.Blob, m.Data))
	case loop.MailRecovered:
		a.recovered(ctx)
	case loop.MailFlush:
		a.beginFlush(m.Base)
	case loop.MailPiece:
		if a.flushing {
			a.fail(a.state.Tree().PieceDurable(m.Ref))
		}
		a.piece()
	case loop.MailFlushFailed:
		if !a.manifested && !a.named {
			a.l.Reply(a.id, loop.Reply{Kind: loop.ReplyFull, Owner: a.cfg.Owner})
		}
		if a.flushing {
			a.flushing = false
			a.state.Tree().FlushFailed()
		}
	case loop.MailCheckpointed:
		if !a.named && !a.manifested {
			if a.manifested = true; a.booted {
				a.writeName()
			}
		}
		a.checkpointed(m.Base)
	case loop.MailRewrite:
		if a.shard != nil {
			a.state.Tree().MarkRewrite(m.Ref.Segment)
			a.merge()
		}
	case loop.MailShare:
		a.share = m.Share
		a.rs.SetShare(m.Share)
	}
	return a.after(ctx, out)
}

// loadRun loads a run the manifest a restart loaded names into the state's tree.
func (a *RoundShard) loadRun(m *loop.Mail) {
	a.replayed = true
	if a.state == nil {
		var err error
		if a.state, err = NewState(a.cfg.Shard.Tree, 0); err != nil {
			a.fail(err)
			return
		}
	}
	if m.Bound != nil {
		a.fail(a.state.Tree().LoadBoundedRun(*m.Bound, m.Pieces))
	} else {
		a.fail(a.state.Tree().LoadRun(m.Ref, m.Pieces))
	}
}

// restore makes the base the loaded runs' mark records the storage's, once, before the replay.
func (a *RoundShard) restore() {
	if a.restored || a.state == nil || a.err != nil {
		a.restored = true
		return
	}
	a.restored = true
	base, err := a.state.Loaded()
	if err == nil && base.Index > 0 {
		err = a.st.ApplySnapshot(raftpb.Snapshot{Metadata: base})
	}
	a.fail(err)
	a.rs = raftshard.NewRounds(a.cfg.Owner, a.st, a.cfg.MaxBlob)
	a.rs.SetShare(a.share)
}

// recovered ends the replay: the replica starts on what it restored, or, with nothing mailed,
// writes its boot state as a new replica does, which a crash before that round was durable left
// unwritten.
func (a *RoundShard) recovered(ctx *core.Context) {
	a.restore()
	a.rs.Replayed()
	if a.err != nil {
		return
	}
	if !a.replayed {
		if a.boot.Index > 0 {
			if err := a.st.ApplySnapshot(raftpb.Snapshot{Metadata: a.boot}); err != nil {
				a.fail(err)
				return
			}
		}
		a.rs = raftshard.NewRounds(a.cfg.Owner, a.st, a.cfg.MaxBlob)
		a.rs.SetShare(a.share)
		a.writeBoot()
		return
	}
	a.start(ctx)
	a.merge()
}

// beginFlush marks the tree with the storage's base and the index applied, begins its flush for
// checkpoint cp, names the runs it carries and replies its first piece. A replica not yet started
// writes nothing: its manifest names it with the runs the one before named, and the replay LSN
// it last reported, or the durable LSN when its disk learned of it, at or before its boot round.
func (a *RoundShard) beginFlush(cp uint64) {
	a.cp = cp
	if a.shard == nil || a.err != nil {
		a.piece()
		return
	}
	a.mark, a.conf = a.shard.Applied()
	a.state.Mark(a.st.Base())
	tree := a.state.Tree()
	if _, err := tree.BeginFlush(); err != nil {
		a.fail(err)
		a.piece()
		return
	}
	a.flushing = true
	a.l.Reply(a.id, loop.Reply{Kind: loop.ReplyRuns, Owner: a.cfg.Owner, LSN: cp, Refs: slices.Clone(tree.CarriedRuns()), Bounds: slices.Clone(tree.Bounds())})
	a.piece()
}

// piece replies the flush's next piece for the running checkpoint, or none once it has written
// all, or cannot.
func (a *RoundShard) piece() {
	r := loop.Reply{Kind: loop.ReplyPiece, Owner: a.cfg.Owner, LSN: a.cp}
	if a.flushing && a.err == nil {
		buf := make([]byte, a.cfg.Shard.Tree.MaxPiece)
		n, root, err := a.state.Tree().NextPiece(buf)
		a.fail(err)
		if n > 0 && err == nil {
			r.Data, r.Root = buf[:n], root
		}
	}
	a.l.Reply(a.id, r)
}

// checkpointed takes the manifest of the checkpoint it flushed under as durable: it reports the
// pieces its tree no longer names, compacts its log to the mark, writes again what holds the
// replay floor at base back, and goes on merging.
func (a *RoundShard) checkpointed(base uint64) {
	if !a.flushing || a.err != nil {
		return
	}
	a.flushing = false
	dead, err := a.state.Tree().CheckpointDone()
	if a.fail(err); err != nil {
		return
	}
	if len(dead) > 0 {
		a.l.Reply(a.id, loop.Reply{Kind: loop.ReplyDead, Owner: a.cfg.Owner, Refs: dead})
	}
	if a.mark > a.st.Base().Index {
		a.fail(a.rs.Compact(a.mark, a.conf))
	}
	a.rs.Rewrite(base)
	a.merge()
}

// merge installs each merge step the tree has due, asks for a checkpoint to write those no
// manifest names, and replies the tree's runs as the cleaner weighs them when they changed.
func (a *RoundShard) merge() {
	if a.err != nil {
		return
	}
	tree, steps := a.state.Tree(), 0
	for c := tree.PickCompaction(); c != nil; c = tree.PickCompaction() {
		c.Run()
		if err := tree.Install(c); err != nil {
			a.fail(err)
			return
		}
		steps++
	}
	if steps > 0 && tree.StepUnnamed() {
		a.l.Reply(a.id, loop.Reply{Kind: loop.ReplyFull, Owner: a.cfg.Owner})
	}
	if costs := tree.RunCosts(nil); !reflect.DeepEqual(costs, a.costs) {
		a.costs = costs
		a.l.Reply(a.id, loop.Reply{Kind: loop.ReplyCosts, Owner: a.cfg.Owner, Costs: slices.Clone(costs)})
	}
}
