package looptest

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"fmt"
	"reflect"
	"slices"
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/loop"
	"github.com/nvcnvn/rock-solid/internal/lsm"
	"github.com/nvcnvn/rock-solid/internal/raftshard"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// raftRounds is a Raft shard's log on the round driver, as its replica drives it: each round holds
// a few entries and a hard state committing them, its storage takes it once durable and its state
// machine, a tree putting each entry's head under its index mod raftKeys, applies it. With
// selfCompact every third round compacts to the commit of the round before, as a checkpoint
// would. With reply it tells the disk's owner its replay LSN as it moves, asks for a checkpoint
// when full, and flushes its tree under each due checkpoint, marked with its storage's base; once
// the checkpoint's manifest is durable it compacts to the index it applied at the mark and
// rewrites what holds the replay floor back; its tree then merges its runs in steps, asking for
// a due checkpoint to write each, rewrites the runs a MailRewrite names, and replies its runs as
// the cleaner weighs them whenever they change. A restart restores it from the runs the owner mails.
type raftRounds struct {
	l           *loop.Loop
	id          loop.ShardID
	owner       uint32
	reply       bool
	selfCompact bool
	pool        core.BufPool
	maxBlob     uint64
	st          *raftshard.Storage
	rs          *raftshard.Rounds
	round       raftshard.Round
	flush       uint64
	done        uint64
	left        int
	err         error
	// tree is the state machine, applied the index it applied, mark the index it had applied
	// when the running checkpoint marked it, cp that checkpoint's number, and restored says a
	// restart restored it from the runs mailed before its replay.
	tree     *lsm.Tree
	applied  uint64
	mark, cp uint64
	restored bool
	flushes  int
	// pause stops its tree's merges, steps counts the merge steps it installed, gone the pieces
	// it reported its tree no longer names, sets the runs each flush's manifest names, moved the
	// number of the last flush that carried other runs than the flush before it named, root says
	// the piece it replied last is a run's root, and loaded is the runs a restart mailed it.
	pause  bool
	steps  int
	gone   []disklog.BlobRef
	sets   []runSet
	moved  int
	root   bool
	loaded runSet
	// refs are the places of every blob told durable, dead those reported dead; recovered says
	// a restart's replay ended.
	refs, dead []disklog.BlobRef
	recovered  bool
	// rewrote counts the rounds of Rounds' own made durable, flushes the due checkpoints that
	// asked for its tree, checkpointed those mailed durable, and shares are the shares mailed.
	rewrote, checkpointed int
	shares                []uint64
	// rewrites counts the runs the cleaner's MailRewrite marked, and costs is the runs it replied
	// last as the cleaner weighs them.
	rewrites int
	costs    []disklog.RunCost
}

func newRaftRounds(l *loop.Loop, pool core.BufPool, lg *loop.Log, log *disklog.Log, rounds int) *raftRounds {
	rr := addRaftRounds(l, pool, lg, 7, log.MaxBlobBytes(), 0)
	rr.selfCompact, rr.left = true, rounds
	if rounds > 0 {
		rr.next()
	}
	return rr
}

// addRaftRounds puts on l a shard whose blobs carry owner, its storage empty, and makes its first
// of rounds rounds.
func addRaftRounds(l *loop.Loop, pool core.BufPool, lg *loop.Log, owner uint32, maxBlob uint64, rounds int) *raftRounds {
	st := raftshard.NewStorage()
	tree, err := lsm.New(lsm.Config{MaxPiece: raftPiece, StepBytes: raftPiece, Check: true})
	if err != nil {
		panic(err)
	}
	rr := &raftRounds{l: l, owner: owner, pool: pool, maxBlob: maxBlob, st: st, rs: raftshard.NewRounds(owner, st, maxBlob), left: rounds, tree: tree}
	rr.id = l.AddShard(rr, lg)
	if rounds > 0 {
		rr.next()
	}
	return rr
}

// raftPiece is a shard's tree's largest piece, small so that a flush writes several.
const raftPiece = 2048

// raftKeys is how many keys a shard's state machine holds.
const raftKeys = 40

var (
	raftMark = []byte{0}
	raftConf = raftpb.ConfState{Voters: []uint64{1}}
)

func raftKey(i uint64) []byte { return []byte(fmt.Sprintf("k%03d", i%raftKeys)) }

// apply applies the committed entries past the applied index.
func (rr *raftRounds) apply() {
	last, _ := rr.st.LastIndex()
	commit := min(rr.st.HardState().Commit, last)
	if commit <= rr.applied {
		return
	}
	ents, err := rr.st.Entries(rr.applied+1, commit+1, 1<<62)
	if err != nil {
		panic(err)
	}
	for _, e := range ents {
		if err := rr.tree.Put(raftKey(e.Index), e.Data[:64]); err != nil {
			panic(err)
		}
	}
	rr.applied = commit
}

// fail records the shard's first error.
func (rr *raftRounds) fail(err error) {
	if err != nil && rr.err == nil {
		rr.err = err
	}
}

// piece replies the tree flush's next piece for the running checkpoint, or none once it is all
// written.
func (rr *raftRounds) piece() {
	buf := make([]byte, raftPiece)
	n, root, err := rr.tree.NextPiece(buf)
	rr.fail(err)
	r := loop.Reply{Kind: loop.ReplyPiece, Owner: rr.owner, LSN: rr.cp, Root: root}
	rr.root = root
	if n > 0 {
		r.Data = buf[:n]
	}
	rr.l.Reply(rr.id, r)
}

// runSet is the runs of a shard's tree a manifest names, by their roots, with the merge bounds on
// some of them.
type runSet struct {
	runs   []disklog.BlobRef
	bounds []disklog.MergeBound
}

// equal reports whether s and o name the same runs and bounds, in any order.
func (s runSet) equal(o runSet) bool {
	byRef := func(a, b disklog.BlobRef) int {
		return cmp.Or(cmp.Compare(a.LSN, b.LSN), cmp.Compare(a.Entry, b.Entry))
	}
	byRun := func(a, b disklog.MergeBound) int { return byRef(a.Run, b.Run) }
	sr, or := slices.SortedFunc(slices.Values(s.runs), byRef), slices.SortedFunc(slices.Values(o.runs), byRef)
	sb, ob := slices.SortedFunc(slices.Values(s.bounds), byRun), slices.SortedFunc(slices.Values(o.bounds), byRun)
	return slices.Equal(sr, or) && slices.EqualFunc(sb, ob, func(a, b disklog.MergeBound) bool {
		return a.Run == b.Run && a.Dead == b.Dead && bytes.Equal(a.Key, b.Key)
	})
}

// named is the runs the newest manifest it was mailed durable names.
func (rr *raftRounds) named() runSet {
	if rr.checkpointed == 0 {
		return runSet{}
	}
	return rr.sets[rr.checkpointed-1]
}

// merge installs each merge step its tree has due, as a merge worker would, and with reply asks
// for a due checkpoint to write the steps it installed, and tells the owner its runs as the
// cleaner weighs them.
func (rr *raftRounds) merge() {
	n := rr.steps
	for c := rr.tree.PickCompaction(); c != nil && !rr.pause; c = rr.tree.PickCompaction() {
		c.Run()
		rr.fail(rr.tree.Install(c))
		rr.steps++
	}
	if !rr.reply {
		return
	}
	if rr.steps > n && rr.tree.StepUnnamed() {
		rr.l.Reply(rr.id, loop.Reply{Kind: loop.ReplyFull, Owner: rr.owner})
	}
	if costs := rr.tree.RunCosts(nil); !reflect.DeepEqual(costs, rr.costs) {
		rr.costs = costs
		rr.l.Reply(rr.id, loop.Reply{Kind: loop.ReplyCosts, Owner: rr.owner, Costs: slices.Clone(costs)})
	}
}

// restore makes the base the mark in the loaded runs records its storage's base, once, before the
// replay: Rounds keeps a storage already on its base.
func (rr *raftRounds) restore() {
	if rr.restored {
		return
	}
	rr.restored = true
	v, ok := rr.tree.Get(raftMark)
	if !ok {
		return
	}
	rr.applied = binary.BigEndian.Uint64(v)
	md := raftpb.SnapshotMetadata{Index: binary.BigEndian.Uint64(v[8:]), Term: binary.BigEndian.Uint64(v[16:]), ConfState: raftConf}
	if md.Index > 0 {
		rr.fail(rr.st.ApplySnapshot(raftpb.Snapshot{Metadata: md}))
	}
	rr.rs = raftshard.NewRounds(rr.owner, rr.st, rr.maxBlob)
}

func (rr *raftRounds) next() {
	rr.left--
	rr.flush++
	last, _ := rr.st.LastIndex()
	ents := make([]raftpb.Entry, 3)
	for i := range ents {
		ents[i] = raftpb.Entry{Index: last + 1 + uint64(i), Term: 1, Data: binary.LittleEndian.AppendUint64(make([]byte, 0, 1000), last+1+uint64(i))[:1000]}
	}
	rr.round = raftshard.Round{Entries: ents, HardState: raftpb.HardState{Term: 1, Vote: 1, Commit: last + 3}}
	rr.rs.Write(rr.flush, &rr.round)
	rr.l.Mark(rr.id)
}

func (rr *raftRounds) HasReady() bool { return rr.rs.HasReady() }

func (rr *raftRounds) Ready(items []disklog.Item) []disklog.Item {
	items, err := rr.rs.Ready(rr.pool, items)
	if err != nil {
		panic(err)
	}
	return items
}

func (rr *raftRounds) Durable(ctx *core.Context, err error, refs []disklog.BlobRef, out []core.Action) []core.Action {
	round, err := rr.rs.Durable(err, refs)
	if err != nil || round != 0 && round != rr.flush {
		rr.err = fmt.Errorf("round %d durable with %v, %d in flight", round, err, rr.flush)
		return out
	}
	rr.refs = append(rr.refs, refs...)
	if round == 0 {
		rr.rewrote++
		rr.bury()
		return out
	}
	rr.done = round
	before, _ := rr.st.LastIndex()
	rr.st.Append(rr.round.Entries)
	rr.st.SetHardState(rr.round.HardState)
	rr.st.Took(round)
	rr.apply()
	if rr.selfCompact && round%3 == 0 && before > rr.st.Base().Index {
		rr.fail(rr.rs.Compact(before, raftConf))
	}
	rr.bury()
	if rr.left > 0 {
		rr.next()
	}
	return out
}

func (rr *raftRounds) bury() {
	n := len(rr.dead)
	if rr.dead = rr.rs.Dead(rr.dead); len(rr.dead) > n {
		rr.l.Dead(rr.id, rr.dead[n:])
	}
	if lsn, ok := rr.rs.ReplayLSN(); ok && rr.reply {
		rr.l.Reply(rr.id, loop.Reply{Kind: loop.ReplyReplay, Owner: rr.owner, LSN: lsn})
	}
	if rr.reply && rr.rs.FullDue(rr.st.HardState().Commit) {
		rr.l.Reply(rr.id, loop.Reply{Kind: loop.ReplyFull, Owner: rr.owner})
	}
	if rr.rs.HasReady() {
		rr.l.Mark(rr.id)
	}
}

// remove takes the shard off its loop and deletes its state, as its driver does once the replica
// is removed, and tells the owner.
func (rr *raftRounds) remove() {
	rr.l.RemoveShard(rr.id)
	newest := rr.rs.Delete()
	rr.bury()
	rr.l.Reply(rr.id, loop.Reply{Kind: loop.ReplyRemoved, Owner: rr.owner, LSN: newest})
}

func (rr *raftRounds) Mail(_ *core.Context, m *loop.Mail, out []core.Action) []core.Action {
	switch m.Kind {
	case loop.MailRun:
		rr.loaded.runs = append(rr.loaded.runs, m.Ref)
		if m.Bound != nil {
			rr.loaded.bounds = append(rr.loaded.bounds, *m.Bound)
			rr.fail(rr.tree.LoadBoundedRun(*m.Bound, m.Pieces))
		} else {
			rr.fail(rr.tree.LoadRun(m.Ref, m.Pieces))
		}
	case loop.MailReplayed:
		rr.restore()
		rr.fail(rr.rs.Replay(m.Ref, m.Blob, m.Data))
	case loop.MailRecovered:
		rr.restore()
		rr.rs.Replayed()
		rr.apply()
		rr.recovered = true
		rr.merge()
	case loop.MailFlush:
		rr.flushes++
		rr.cp, rr.mark = m.Base, rr.applied
		b := rr.st.Base()
		v := binary.BigEndian.AppendUint64(nil, rr.applied)
		v = binary.BigEndian.AppendUint64(v, b.Index)
		v = binary.BigEndian.AppendUint64(v, b.Term)
		if was, _ := rr.tree.Get(raftMark); !bytes.Equal(was, v) {
			rr.fail(rr.tree.Put(raftMark, v))
		}
		_, err := rr.tree.BeginFlush()
		rr.fail(err)
		set := runSet{runs: slices.Clone(rr.tree.CarriedRuns()), bounds: slices.Clone(rr.tree.Bounds())}
		if len(rr.sets) > 0 && !set.equal(rr.sets[len(rr.sets)-1]) {
			rr.moved = len(rr.sets) + 1
		}
		rr.sets = append(rr.sets, set)
		rr.l.Reply(rr.id, loop.Reply{Kind: loop.ReplyRuns, Owner: rr.owner, LSN: rr.cp, Refs: set.runs, Bounds: set.bounds})
		rr.piece()
	case loop.MailPiece:
		rr.fail(rr.tree.PieceDurable(m.Ref))
		if rr.root {
			set := &rr.sets[len(rr.sets)-1]
			set.runs = append(slices.Clip(set.runs), m.Ref)
		}
		rr.piece()
	case loop.MailFlushFailed:
		rr.tree.FlushFailed()
	case loop.MailCheckpointed:
		rr.checkpointed++
		dead, err := rr.tree.CheckpointDone()
		rr.fail(err)
		if len(dead) > 0 {
			rr.gone = append(rr.gone, dead...)
			rr.l.Reply(rr.id, loop.Reply{Kind: loop.ReplyDead, Owner: rr.owner, Refs: dead})
		}
		if rr.mark > rr.st.Base().Index {
			rr.fail(rr.rs.Compact(rr.mark, raftConf))
		}
		rr.rs.Rewrite(m.Base)
		rr.merge()
	case loop.MailRewrite:
		rr.rewrites += rr.tree.MarkRewrite(m.Ref.Segment)
		rr.merge()
	case loop.MailShare:
		rr.rs.SetShare(m.Share)
		rr.shares = append(rr.shares, m.Share)
	}
	rr.bury()
	return out
}

// TestRoundsOfARaftShardDieAsItCompacts runs a Raft shard's rounds through the round driver, on a
// producer loop and on the owner: every round becomes durable, and the blobs of the rounds a
// compaction and a newer hard state leave behind leave the owner's log, the rest staying.
func TestRoundsOfARaftShardDieAsItCompacts(t *testing.T) {
	for _, n := range []int{0, 1} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			r := newRoundRig(t, 1, n, 0, 0, 2000, 0)
			defer r.close()
			rr := newRaftRounds(r.loops[0], r.pools[0], r.lg, r.log, 10)
			r.run(t)
			if rr.err != nil || rr.flush != 10 || rr.rs.HasReady() {
				t.Fatalf("round %d last made, err %v", rr.flush, rr.err)
			}
			if len(rr.dead) == 0 {
				t.Fatal("no blob died over three compactions")
			}
			for _, ref := range rr.dead {
				if err := r.log.RaftBlobsDead([]disklog.BlobRef{ref}); err == nil {
					t.Fatalf("the log still holds blob %+v, reported dead", ref)
				}
			}
			live := rr.refs[len(rr.dead):]
			if rr.refs[len(rr.dead)-1] != rr.dead[len(rr.dead)-1] {
				t.Fatalf("the dead blobs %v are not the oldest of %v", rr.dead, rr.refs)
			}
			if err := r.log.RaftBlobsDead(live); err != nil {
				t.Fatalf("the log dropped a live blob: %v", err)
			}
		})
	}
}

// TestRoundsOfARemovedRaftShardAllDie takes a Raft shard off its loop with a round in flight and
// deletes its state: the round driver reports the round's blobs, Delete every other, and the log
// holds none of them.
func TestRoundsOfARemovedRaftShardAllDie(t *testing.T) {
	for _, n := range []int{0, 1} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			r := newRoundRig(t, 1, n, 0, 0, 2000, 0)
			defer r.close()
			before := r.pools[0].(*sim.Pool).Available()
			rr := newRaftRounds(r.loops[0], r.pools[0], r.lg, r.log, 5)
			for rr.flush < 5 || rr.rs.HasReady() {
				if !r.s.Step() {
					t.Fatal("the last round was never collected")
				}
			}
			r.loops[0].RemoveShard(rr.id)
			rr.rs.Delete()
			r.loops[0].Dead(rr.id, rr.rs.Dead(nil))
			r.loops[len(r.loops)-1].Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: 1})
			r.run(t)
			if rr.err != nil || len(rr.refs) == 0 {
				t.Fatalf("err %v, %d blobs durable", rr.err, len(rr.refs))
			}
			if live := r.log.LiveBlocks(); live != 0 {
				t.Fatalf("the log holds %d live blocks of a deleted shard", live)
			}
			if got := r.pools[0].(*sim.Pool).Available(); got != before {
				t.Fatalf("the shard's pool has %d bytes free, %d before", got, before)
			}
		})
	}
}

// TestRoundOfARaftShardWaitsOutAFullPool collects a Raft shard whose loop's pool cannot hold its
// round: Ready hands over nothing and the round is kept, and once the pool frees and the shard is
// marked again, it and every round after it become durable.
func TestRoundOfARaftShardWaitsOutAFullPool(t *testing.T) {
	r := newRoundRig(t, 1, 1, 0, 0, 2000, 0)
	defer r.close()
	pool := r.pools[0].(*sim.Pool)
	hog, err := pool.Alloc(pool.Available())
	if err != nil {
		t.Fatal(err)
	}
	rr := newRaftRounds(r.loops[0], pool, r.lg, r.log, 4)
	r.run(t)
	if rr.done != 0 || !rr.rs.HasReady() {
		t.Fatalf("round %d durable, round waiting %v, with the pool full", rr.done, rr.rs.HasReady())
	}
	pool.Free(hog)
	r.loops[0].Mark(rr.id)
	r.loops[0].Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: 1})
	r.run(t)
	if rr.err != nil || rr.done != 4 || rr.rs.HasReady() {
		t.Fatalf("round %d durable of 4, err %v", rr.done, rr.err)
	}
}
