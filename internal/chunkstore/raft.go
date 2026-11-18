package chunkstore

import (
	"slices"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/lsm"
	"github.com/nvcnvn/rock-solid/internal/raftshard"
)

// RaftShard is a Raft shard the store hosts on its disk: its rounds share the log with the chunk
// writes, and its state's tree joins the store's checkpoints and merges.
type RaftShard interface {
	// Tree is the shard's state machine's tree.
	Tree() *lsm.Tree
	// Mark records in Tree the storage's base and returns the applied index, with its
	// configuration, that the shard's log compacts to once Tree's flush is durable.
	Mark() (uint64, raftpb.ConfState)
	// Restore makes the base recorded by the runs a restart loaded into Tree the storage's base.
	Restore() error
}

// raftShard is a hosted shard, the index and configuration a running checkpoint compacts it to,
// and the replay LSN the last due checkpoint named it with, if one did.
type raftShard struct {
	owner   uint32
	st      *raftshard.Storage
	sh      RaftShard
	compact uint64
	conf    raftpb.ConfState
	replay  uint64
	named   bool
}

// AddRaft hosts the Raft shard whose blobs carry owner and whose storage is st: after Format or
// Open returns and before any event, so a restart restores it from the manifest's runs and
// replays its rounds, or while the store serves, with owner Reusable on the log.
func (s *Store) AddRaft(owner uint32, st *raftshard.Storage, sh RaftShard) {
	if s.raft == nil {
		s.raft = raftshard.NewHost(s.log)
	}
	s.raft.Add(owner, st)
	s.shards = append(s.shards, raftShard{owner: owner, st: st, sh: sh})
	if s.sched != nil {
		s.sched.Add(sh.Tree())
	}
}

// WriteRound submits round flush of owner's shard, whose contents r stay valid until
// Output.Rounds names it durable.
func (s *Store) WriteRound(ctx *core.Context, owner uint32, flush uint64, r *raftshard.Round, out *Output) error {
	if s.phase != serving {
		return ErrUnavailable
	}
	err := s.raft.Write(ctx, owner, flush, r, &s.lout)
	s.pump(ctx, out)
	return err
}

// tree is the tree whose pieces a checkpoint writes k-th: the index's, then each shard's.
func (s *Store) tree(k int) (*lsm.Tree, uint32) {
	if k == 0 {
		return s.ix.Tree(), indexOwnerID
	}
	return s.shards[k-1].sh.Tree(), s.shards[k-1].owner
}

func (s *Store) shardOf(owner uint32) int {
	return slices.IndexFunc(s.shards, func(r raftShard) bool { return r.owner == owner })
}

func (s *Store) stepUnnamed() bool {
	return s.ix.Tree().StepUnnamed() || slices.ContainsFunc(s.shards, func(r raftShard) bool { return r.sh.Tree().StepUnnamed() })
}

// beginShards begins each shard tree's part of a checkpoint whose base is durable and appends
// its index. A due one first writes again the hard state of each idle shard the last manifest's
// base passed, then marks each tree with its storage's base and flushes its memtable, which the
// shard compacts to once durable unless the flush writes instead a memtable a failed one froze.
// A step one writes only merge steps, so it names each shard with the replay LSN of the runs it
// carries, the last due checkpoint's, and leaves out a shard no due checkpoint named: its rounds
// all follow that checkpoint's base, where the replay floor stays.
func (s *Store) beginShards(ctx *core.Context, due bool, durable uint64, idx []disklog.CheckpointIndex) ([]disklog.CheckpointIndex, error) {
	if s.raft == nil {
		return idx, nil
	}
	s.cpShards = len(s.shards)
	if !due {
		for i := range s.shards {
			r := &s.shards[i]
			t := r.sh.Tree()
			if err := t.BeginStepFlush(); err != nil {
				panic("chunkstore: a shard's flush begun while one runs: " + err.Error())
			}
			if r.compact = 0; r.named {
				idx = append(idx, disklog.CheckpointIndex{Owner: r.owner, ReplayLSN: r.replay, Runs: t.CarriedRuns(), Bounds: t.Bounds()})
			}
		}
		return idx, nil
	}
	if err := s.raft.Rewrite(ctx, s.lastBase, &s.lout); err != nil {
		return idx, err
	}
	s.cpBase = durable
	for i := range s.shards {
		r := &s.shards[i]
		r.compact, r.conf = r.sh.Mark()
		froze, err := r.sh.Tree().BeginFlush()
		if err != nil {
			panic("chunkstore: a shard's flush begun while one runs: " + err.Error())
		}
		if !froze && r.sh.Tree().Frozen() {
			r.compact = 0
		}
	}
	n := len(idx)
	idx = s.raft.Indexes(idx, durable)
	for k := n; k < len(idx); k++ {
		r := &s.shards[s.shardOf(idx[k].Owner)]
		t := r.sh.Tree()
		idx[k].Runs, idx[k].Bounds = t.CarriedRuns(), t.Bounds()
		r.replay, r.named = idx[k].ReplayLSN, true
	}
	return idx, nil
}

// shardsCheckpointed ends each shard tree's flush once the manifest is durable, reports the
// pieces it made dead and compacts each shard to the index its tree was marked at.
func (s *Store) shardsCheckpointed(ctx *core.Context, out *Output) {
	for i := range s.cpShards {
		r := &s.shards[i]
		dead, err := r.sh.Tree().CheckpointDone()
		if err != nil {
			panic("chunkstore: a shard's checkpoint done with no flush running: " + err.Error())
		}
		s.piecesDead(dead)
		if r.compact > r.st.Base().Index {
			if err := s.raft.Compact(r.owner, r.compact, r.conf); err != nil {
				s.stop(ctx, err, out)
				return
			}
		}
	}
	if s.cpBase != 0 {
		s.lastBase, s.cpBase = s.cpBase, 0
	}
}

func (s *Store) shardFlushesFailed() {
	for i := range s.cpShards {
		s.piecesDead(s.shards[i].sh.Tree().FlushFailed())
	}
	s.cpBase = 0
}

// restoreShards restores each hosted shard the loaded manifest names from the runs its tree
// loaded, and hands the host the manifest, before the replay.
func (s *Store) restoreShards(m *disklog.Manifest) error {
	if s.raft == nil {
		return nil
	}
	for _, mi := range m.Indexes {
		if k := s.shardOf(mi.Owner); k >= 0 {
			if err := s.shards[k].sh.Restore(); err != nil {
				return err
			}
			s.shards[k].replay, s.shards[k].named = mi.ReplayLSN, true
		}
	}
	s.raft.Loaded(m)
	s.lastBase = m.BaseLSN
	return nil
}
