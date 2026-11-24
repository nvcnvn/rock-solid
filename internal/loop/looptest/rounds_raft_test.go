package looptest

import (
	"fmt"
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/loop"
	"github.com/nvcnvn/rock-solid/internal/raftshard"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// raftRounds is a Raft shard's log on the round driver, as its replica drives it: each round holds
// a few entries and a hard state committing them, its storage takes it once durable, and every
// third round compacts to the commit of the round before, as a checkpoint would.
type raftRounds struct {
	l     *loop.Loop
	id    loop.ShardID
	pool  core.BufPool
	st    *raftshard.Storage
	rs    *raftshard.Rounds
	round raftshard.Round
	flush uint64
	done  uint64
	left  int
	err   error
	// refs are the places of every blob told durable, dead those reported dead.
	refs, dead []disklog.BlobRef
}

func newRaftRounds(l *loop.Loop, pool core.BufPool, lg *loop.Log, log *disklog.Log, rounds int) *raftRounds {
	st := raftshard.NewStorage()
	rr := &raftRounds{l: l, pool: pool, st: st, rs: raftshard.NewRounds(7, st, log.MaxBlobBytes()), left: rounds}
	rr.id = l.AddShard(rr, lg)
	rr.next()
	return rr
}

func (rr *raftRounds) next() {
	rr.left--
	rr.flush++
	last, _ := rr.st.LastIndex()
	ents := make([]raftpb.Entry, 3)
	for i := range ents {
		ents[i] = raftpb.Entry{Index: last + 1 + uint64(i), Term: 1, Data: make([]byte, 1000)}
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
	if err != nil || round != rr.flush {
		rr.err = fmt.Errorf("round %d durable with %v, %d in flight", round, err, rr.flush)
		return out
	}
	rr.refs = append(rr.refs, refs...)
	rr.done = round
	before, _ := rr.st.LastIndex()
	rr.st.Append(rr.round.Entries)
	rr.st.SetHardState(rr.round.HardState)
	rr.st.Took(round)
	if round%3 == 0 {
		if err := rr.rs.Compact(before, raftpb.ConfState{Voters: []uint64{1}}); err != nil {
			rr.err = err
		}
	}
	n := len(rr.dead)
	if rr.dead = rr.rs.Dead(rr.dead); len(rr.dead) > n {
		rr.l.Dead(rr.id, rr.dead[n:])
	}
	if rr.left > 0 {
		rr.next()
	}
	return out
}

func (rr *raftRounds) Mail(_ *core.Context, _ *loop.Mail, out []core.Action) []core.Action {
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
