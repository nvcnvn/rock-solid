package chunkstore

import (
	"errors"
	"fmt"
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
	// Applied is the index the shard's state machine applied.
	Applied() uint64
}

// raftShard is a hosted shard, the index and configuration a running checkpoint compacts it to,
// the replay LSN the last due checkpoint that ended named it with, if one did, and the one a
// running due checkpoint names it with.
type raftShard struct {
	owner    uint32
	st       *raftshard.Storage
	sh       RaftShard
	compact  uint64
	conf     raftpb.ConfState
	replay   uint64
	named    bool
	cpReplay uint64
	cpNamed  bool
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

// AddRemote knows the shard whose blobs carry owner as shard id on the round driver of loop,
// which writes its rounds to this store's log: a restart's replay hands its blobs to Output's
// Mail, every checkpoint names it with rep, the replica it holds the state of, which a restart
// hands Config.Replicas, and with the replay LSN it reports (RemoteReplay), and a broken
// index stops the store rather than rebuilding it under the driver. Output's Mail also tells it
// its share of twice max_replay_bytes as the shards on the disk come and go, and each due
// checkpoint whose manifest is durable, after which it compacts and writes again what holds the
// replay floor back (raftshard.Rounds.Rewrite). Each due checkpoint asks it to flush its tree
// under it, a piece at a time (RemotePiece), and waits for its last piece; the runs its flush
// carries, which its merges change (RemoteRuns), those it writes and their mark of its storage's
// base join the manifest, and a restart mails them to it before its replay; the pieces its merges
// leave out die once a checkpoint begun after it reports them ends (RemoteDead), and the store
// begins one while such pieces wait. The cleaner weighs its runs as it replies them (RemoteCosts)
// and mails it to rewrite those in a victim. It is called as AddRaft is, before the shard writes
// a blob, and to restore it from the manifest's runs, from Config.Replicas or, for a shard of
// no replica, after Open and before any event.
func (s *Store) AddRemote(owner uint32, loop int, id uint32, rep disklog.Replica) {
	if s.raft == nil {
		s.raft = raftshard.NewHost(s.log)
	}
	s.raft.AddRemote(owner, loop, id, rep)
}

// ErrReplicaNotRestored stops a restart whose manifest names a replica no Config.Replicas takes.
var ErrReplicaNotRestored = errors.New("chunkstore: the disk holds a replica nothing restores")

// restoreReplicas hands Config.Replicas each replica the loaded manifest names, before its runs
// are read: a replica left out would be taken for dead, so with no Replicas it stops the restart.
func (s *Store) restoreReplicas(m *disklog.Manifest) error {
	for _, mi := range m.Indexes {
		if mi.Replica == (disklog.Replica{}) {
			continue
		}
		if s.cfg.Replicas == nil {
			return fmt.Errorf("%w: replica %d of group %d", ErrReplicaNotRestored, mi.Replica.ID, mi.Replica.Group)
		}
		if err := s.cfg.Replicas(mi.Owner, mi.Replica); err != nil {
			return err
		}
	}
	return nil
}

// RemoteReplay takes the replay LSN owner's shard on the round driver reported: a restart needs
// none of its blobs at or before lsn.
func (s *Store) RemoteReplay(owner uint32, lsn uint64) { s.raft.RemoteReplay(owner, lsn) }

// RemotePiece takes the next piece owner's shard on the round driver replied for checkpoint cp
// (loop.ReplyPiece), data, root marking a run's last, or with no data the end of its flush; the
// store writes it under the checkpoint and mails the shard its place once durable.
func (s *Store) RemotePiece(owner uint32, cp uint64, data []byte, root bool) {
	s.raft.RemotePiece(owner, cp, data, root)
}

// RemoteRuns takes the runs, with merge bounds, that owner's shard on the round driver replied its
// flush for checkpoint cp carries (loop.ReplyRuns), before its first piece: the manifest names
// them, in place of those the newest manifest named, and the roots its pieces write.
func (s *Store) RemoteRuns(owner uint32, cp uint64, runs []disklog.BlobRef, bounds []disklog.MergeBound) {
	s.raft.RemoteRuns(owner, cp, runs, bounds)
}

// RemoteName takes the ask of owner's shard on the round driver that the manifest of checkpoint cp
// be the one a restart loads before any later round of the shard is durable (loop.ReplyName).
func (s *Store) RemoteName(owner uint32, cp uint64) { s.raft.RemoteName(owner, cp) }

// RemoteDead takes the pieces owner's shard on the round driver replied its tree no longer names
// (loop.ReplyDead), its merges' inputs once a durable manifest names their output: they are
// reported dead once a checkpoint begun after now ends.
func (s *Store) RemoteDead(owner uint32, refs []disklog.BlobRef) { s.raft.RemoteDead(owner, refs) }

// RemoteCosts takes the runs of owner's tree, a shard on the round driver, as the cleaner weighs
// them, replied whenever they change (loop.ReplyCosts): its cost query reads them, and a victim
// holding a piece of one mails the shard to rewrite it (raftshard.MailRewrite). The cleaner looks
// for a victim again, though the durable LSN has not moved, as an idle store's never would.
func (s *Store) RemoteCosts(owner uint32, costs []disklog.RunCost) {
	s.raft.RemoteCosts(owner, costs)
	s.cl.tried = false
}

// RemoteFull takes a shard on the round driver's ask for a due checkpoint: full, it applied a
// commit no checkpoint compacted, as raftshard.Rounds.FullDue reports it, or its tree installed a
// merge step no manifest names.
func (s *Store) RemoteFull() { s.remoteFull = true }

// RemoveRemote forgets owner's shard on the round driver, taken off its loop with every blob of it
// reported dead, the newest at lsn: its owner goes to no new shard while that blob can replay.
func (s *Store) RemoveRemote(owner uint32, lsn uint64) { s.raft.RemoveRemote(owner, lsn) }

// LeftOut reports whether every manifest a restart may load leaves out the removed shards of owner
// on the round driver.
func (s *Store) LeftOut(owner uint32) bool { return s.raft == nil || s.raft.LeftOut(owner) }

// Reusable reports whether owner may be given to a new shard, hosted or on the round driver.
func (s *Store) Reusable(owner uint32) bool { return s.raft == nil || s.raft.Reusable(owner) }

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

// ErrNotHosted is RemoveRaft's refusal of an owner whose shard the store does not host.
var ErrNotHosted = errors.New("chunkstore: no Raft shard of that owner hosted")

// RemoveRaft stops hosting owner's shard, as the driver does once the shard is deleted. Once no
// checkpoint runs, every Raft blob of it is reported dead and it leaves the checkpoints and the
// merges; its tree's pieces are reported dead once the next checkpoint, whose manifest leaves it
// out, ends. A restart that does not add it again skips its runs and takes its blobs for dead,
// and its owner goes to no new shard while they can replay. ErrNotHosted for an owner the store
// does not host or is already removing.
func (s *Store) RemoveRaft(ctx *core.Context, owner uint32, out *Output) error {
	if s.phase != serving {
		return ErrUnavailable
	}
	if s.shardOf(owner) < 0 || slices.Contains(s.removing, owner) {
		return ErrNotHosted
	}
	s.removing = append(s.removing, owner)
	s.pump(ctx, out)
	return nil
}

func (s *Store) removeShards() {
	for _, owner := range s.removing {
		s.raft.Remove(owner)
		k := s.shardOf(owner)
		t := s.shards[k].sh.Tree()
		s.removedPieces = t.Pieces(s.removedPieces)
		s.sched.Remove(t)
		s.shards = slices.Delete(s.shards, k, k+1)
	}
	s.removing = s.removing[:0]
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
// base passed, then marks each tree with its storage's base and flushes its memtable, and the
// shard compacts to its mark once the manifest is durable.
// A step one writes only merge steps, so it names each shard with the replay LSN of the runs it
// carries, that of the last due checkpoint that ended, and leaves out a shard none named: its rounds
// all follow that checkpoint's base, where the replay floor stays.
// Either names each shard on the round driver with the replay LSN it last reported, as its rounds
// may predate any checkpoint's base, and the runs the newest manifest names; a due one asks it to
// flush its tree (beginRemote) once the log has begun the checkpoint.
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
		return s.raft.RemoteIndexes(idx, durable), nil
	}
	if err := s.raft.Rewrite(ctx, s.lastBase, &s.lout); err != nil {
		return idx, err
	}
	s.cpBase = durable
	for i := range s.shards {
		r := &s.shards[i]
		r.compact, r.conf = r.sh.Mark()
		r.cpNamed = false
		if _, err := r.sh.Tree().BeginFlush(); err != nil {
			panic("chunkstore: a shard's flush begun while one runs: " + err.Error())
		}
	}
	n := len(idx)
	idx = s.raft.Indexes(idx, durable)
	for k := n; k < len(idx); k++ {
		r := &s.shards[s.shardOf(idx[k].Owner)]
		t := r.sh.Tree()
		idx[k].Runs, idx[k].Bounds = t.CarriedRuns(), t.Bounds()
		r.cpReplay, r.cpNamed = idx[k].ReplayLSN, true
	}
	return s.raft.RemoteIndexes(idx, durable), nil
}

// shardsCheckpointed ends each shard tree's flush once the manifest is durable, reports the
// pieces it made dead, those of removed shards' trees included, and compacts each shard to the
// index its tree was marked at; after a due one, it mails each shard on the round driver that
// flushed under it to do the same.
func (s *Store) shardsCheckpointed(ctx *core.Context, out *Output) {
	for i := range s.cpShards {
		r := &s.shards[i]
		dead, err := r.sh.Tree().CheckpointDone()
		if err != nil {
			panic("chunkstore: a shard's checkpoint done with no flush running: " + err.Error())
		}
		s.piecesDead(dead)
		if r.cpNamed {
			r.replay, r.named, r.cpNamed = r.cpReplay, true, false
		}
		if r.compact > r.st.Base().Index {
			if err := s.raft.Compact(r.owner, r.compact, r.conf); err != nil {
				s.stop(ctx, err, out)
				return
			}
		}
	}
	s.piecesDead(s.removedPieces)
	s.removedPieces = s.removedPieces[:0]
	if s.raft != nil {
		s.raft.Checkpointed(s.lastBase)
		out.Mail = s.raft.Mail(out.Mail)
	}
	if s.cpBase != 0 {
		s.lastBase, s.cpBase = s.cpBase, 0
	}
}

func (s *Store) shardFlushesFailed() {
	for i := range s.cpShards {
		s.piecesDead(s.shards[i].sh.Tree().FlushFailed())
		s.shards[i].cpNamed = false
	}
	if s.raft != nil {
		s.piecesDead(s.raft.FlushFailed(nil))
	}
	s.cpBase = 0
}

// beginRemote asks each shard on the round driver to flush its tree under the due checkpoint the
// log has just begun, and takes the pieces of removed shards' trees, which that checkpoint leaves
// out, as dead once it ends.
func (s *Store) beginRemote(due bool, out *Output) {
	if s.raft == nil {
		return
	}
	s.removedPieces = s.raft.Gone(s.removedPieces)
	if due {
		s.raft.BeginFlush(s.cpBase)
		out.Mail = s.raft.Mail(out.Mail)
	}
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

// RaftFull reports whether owner's shard retains its share of twice max_replay_bytes in entries,
// past which it takes no new ones: as a follower it drops the appends sent to it, which Raft takes
// as lost messages, and as a leader it refuses proposals as overloaded. Its committed entries are
// never dropped; a checkpoint compacts what it applied.
func (s *Store) RaftFull(owner uint32) bool {
	return s.raft.Retained(owner) >= s.raft.Share(s.raftBytes())
}

// raftBytes is what the shards on the disk, hosted and on the round driver, share of retained
// entries: twice max_replay_bytes.
func (s *Store) raftBytes() uint64 { return 2 * s.cfg.Log.MaxReplayBytes }

// fullDue reports whether a full shard applied a commit no checkpoint begun for it saw, and records
// that commit: one checkpoint compacts what it applied, and another would compact nothing more
// until its commit moves, as a follower's whose leader is gone does not. A shard on the round
// driver asks through RemoteFull.
func (s *Store) fullDue() bool {
	if s.raft == nil {
		return false
	}
	if s.remoteFull {
		s.remoteFull = false
		return true
	}
	share := s.raft.Share(s.raftBytes())
	for i := range s.shards {
		if r := &s.shards[i]; s.raft.FullDue(r.owner, r.sh.Applied(), share) {
			return true
		}
	}
	return false
}
