package raftshard

import (
	"errors"
	"fmt"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// Rounds is one shard's rounds on the round driver, kept on the shard's own loop beside its
// storage: the driver hands it each round the replica makes, collects the round's blobs with Ready
// and hands Durable the places the log wrote them. It keeps what Host keeps of a hosted shard, so
// the blobs a restart no longer needs gather in Dead, for the driver to report to the disk's owner.
// It writes rounds of its own too, as Host does, when Rewrite finds the shard holding the replay
// floor back.
type Rounds struct {
	hd      hosted
	maxBlob uint64
	rec     []byte
	// flush is the replica's round waiting for Ready, 0 for none, and round its contents; out is
	// the replica's round whose blobs are in flight, 0 for none, with its summary and entry payload
	// bytes.
	flush, out uint64
	round      Round
	sum        summary
	bytes      uint64
	// own is a round of the shard's hard state Rewrite queued, if queued; owning says blobs of
	// Rounds' own, that round's or a full rewrite's, are in flight, and ownLast that they end
	// their round, whose summary is sum.
	own                     Round
	queued, owning, ownLast bool
	dead                    []disklog.BlobRef
	deleted                 bool
	// told is the LSN of the oldest blob a restart needs that ReplayLSN last reported, 0 for none.
	// runs is the replica's round whose snapshot's data is in its runs, 0 for none, until a later
	// round of the replica's is durable: a restart on runs older than the snapshot drops it and
	// needs the rounds its hard state freed, so ReplayLSN holds meanwhile.
	told, runs uint64
	// share is the share of twice max_replay_bytes the disk's owner last mailed, 0 for none yet.
	share uint64
}

// NewRounds keeps the rounds of the shard whose blobs carry owner and whose storage is st, already
// on the base its runs restore, cutting each into blobs of at most maxBlob bytes, the log's
// MaxBlobBytes.
func NewRounds(owner uint32, st *Storage, maxBlob uint64) *Rounds {
	return &Rounds{hd: hosted{owner: owner, st: st, newest: -1, compacted: st.Base().Index}, maxBlob: maxBlob}
}

// Write queues round flush, whose contents r stay valid until Durable returns it, for Ready, after
// any round of Rounds' own; a round 0, or one while another of the replica's is queued or in
// flight, is a broken invariant of the caller. After a round whose snapshot's data is in the
// replica's runs, the replica's next round may be durable only once every manifest a restart may
// load names runs holding that snapshot.
func (r *Rounds) Write(flush uint64, rd *Round) {
	if flush == 0 || r.flush != 0 || r.out != 0 || r.deleted {
		panic(fmt.Sprintf("raftshard: round %d written with round %d queued, %d in flight, deleted %v", flush, r.flush, r.out, r.deleted))
	}
	r.flush, r.round = flush, *rd
	r.hd.flying += payloadBytes(rd)
	if rd.Runs {
		r.runs = flush
	}
}

// HasReady reports whether blobs wait for Ready: nothing is in flight, and a round of Rounds' own
// or the replica's is queued, or a full rewrite is going out.
func (r *Rounds) HasReady() bool {
	return r.out == 0 && !r.owning && (r.hd.rw != nil || r.queued || r.flush != 0)
}

// Ready appends the waiting blobs to items, their buffers from pool: the next few of a full
// rewrite, else a round of Rounds' own, else the replica's round. When pool has no buffers for
// them it appends none, and they wait: the shard's loop.Shard marks the shard again later, as
// Host's driver calls Retry, since the round driver collects only shards marked since its last
// round.
func (r *Rounds) Ready(pool core.BufPool, items []disklog.Item) ([]disklog.Item, error) {
	if !r.HasReady() {
		return items, nil
	}
	if r.hd.rw != nil {
		return r.rewriteItems(pool, items)
	}
	rd := &r.round
	if r.queued {
		rd = &r.own
	}
	r.rec = AppendRound(r.rec[:0], rd)
	items, err := RoundItems(pool, r.hd.owner, r.maxBlob, r.rec, items)
	if errors.Is(err, core.ErrPoolExhausted) {
		return items, nil
	}
	if err != nil {
		return items, err
	}
	r.sum = summarize(rd)
	if r.queued {
		r.owning, r.ownLast, r.queued, r.own = true, true, false, Round{}
		return items, nil
	}
	r.out, r.flush = r.flush, 0
	r.bytes = payloadBytes(&r.round)
	r.round = Round{}
	return items, nil
}

// rewriteItems appends the next blobs of the full rewrite, up to rewriteBlobs of them, and once its
// last is cut, compacts as the compaction that waited for it asks.
func (r *Rounds) rewriteItems(pool core.BufPool, items []disklog.Item) ([]disklog.Item, error) {
	n := len(items)
	for len(items)-n < rewriteBlobs && r.hd.rw != nil {
		rw := r.hd.rw
		it, err := r.hd.nextRewrite(pool, r.maxBlob)
		if errors.Is(err, core.ErrPoolExhausted) {
			break
		}
		if err != nil {
			return items, err
		}
		items = append(items, it)
		if r.hd.rw == nil {
			r.sum, r.ownLast = rw.sum, true
		}
	}
	r.owning = len(items) > n
	if r.hd.rw != nil || r.hd.cut == 0 {
		return items, nil
	}
	i := r.hd.cut
	r.hd.cut = 0
	return items, r.Compact(i, r.hd.cutConf)
}

// Durable takes the places of the blobs in flight, in Ready order, or the error the log failed
// them with, and returns the replica's round they end, which the replica is then told durable, or
// 0 for blobs of Rounds' own; Rounds' own failing drops their round, which a later Rewrite writes
// again. With nothing in flight, or after Delete, it is a broken invariant of the caller.
func (r *Rounds) Durable(err error, refs []disklog.BlobRef) (uint64, error) {
	if r.out == 0 && !r.owning || r.deleted {
		panic(fmt.Sprintf("raftshard: Durable with round %d in flight, own blobs %v, deleted %v", r.out, r.owning, r.deleted))
	}
	if r.owning {
		r.owning = false
		if err != nil {
			return 0, r.drop(err)
		}
		r.hd.refs = append(r.hd.refs, refs...)
		if r.ownLast {
			r.ownLast = false
			r.dead = r.hd.close(r.sum, r.dead)
		}
		return 0, nil
	}
	round := r.out
	r.out = 0
	if err != nil {
		return round, err
	}
	if round != r.runs {
		r.runs = 0
	}
	r.hd.refs = append(r.hd.refs, refs...)
	if r.bytes > 0 {
		r.hd.landed = append(r.hd.taken(), landedRound{round, r.bytes})
	}
	r.dead = r.hd.close(r.sum, r.dead)
	r.hd.done = round
	return round, nil
}

// drop gives up the round of Rounds' own that failed with err: its blobs already durable die, and
// the rounds it would have replaced stay live until a later Rewrite.
func (r *Rounds) drop(err error) error {
	r.dead = append(r.dead, r.hd.refs[r.hd.closed:]...)
	r.hd.refs, r.hd.rw, r.ownLast = r.hd.refs[:r.hd.closed], nil, false
	if i := r.hd.cut; i != 0 {
		r.hd.cut = 0
		err = errors.Join(err, r.Compact(i, r.hd.cutConf))
	}
	return err
}

// Compact compacts the storage to index i, whose configuration is cs, as Host.Compact does: while a
// full rewrite goes out, once it has read the entries the compaction drops.
func (r *Rounds) Compact(i uint64, cs raftpb.ConfState) error {
	if r.hd.rw != nil {
		r.hd.cut, r.hd.cutConf = i, cs
		return nil
	}
	var err error
	r.dead, err = r.hd.compact(i, cs, r.dead)
	return err
}

// Retained is the entry payload bytes the shard holds, as Host.Retained counts them.
func (r *Rounds) Retained() uint64 { return r.hd.retained() }

// SetShare takes the shard's share of twice max_replay_bytes the disk's owner mailed.
func (r *Rounds) SetShare(share uint64) { r.share = share }

// Full reports whether the shard retains its share, as Store.RaftFull does for a hosted shard: it
// then takes no new entries. Until the owner mails a share, it is not full.
func (r *Rounds) Full() bool { return r.share > 0 && r.hd.retained() >= r.share }

// FullDue reports whether the shard is full and, having applied index applied, applied a commit
// FullDue has not reported yet, as Host.FullDue does: the shard then asks the disk's owner for a
// checkpoint, which compacts what it can.
func (r *Rounds) FullDue(applied uint64) bool {
	return r.share > 0 && r.hd.fullDue(applied, r.share)
}

// Rewrite writes again, as Host.Rewrite does, what the shard keeps at or before lsn, the base the
// owner mailed with a due checkpoint's durable manifest, that of the due manifest before it, once
// the shard has compacted to what that checkpoint recorded: an idle shard's hard state in a round
// of Rounds' own, or a full rewrite of a shard whose oldest round holds an entry past its commit. A shard with a round of the
// replica's queued or in flight, or blobs of its own, is left as it is. The shard's loop.Shard
// marks the shard once HasReady.
func (r *Rounds) Rewrite(lsn uint64) {
	if r.flush != 0 || r.out != 0 || r.owning || r.queued || r.hd.rw != nil || r.deleted {
		return
	}
	r.own, r.queued = r.hd.rewrite(lsn)
}

// Replay takes a blob of the shard a restart's replay found at ref, as recovery's mail hands it
// over, past the shard's replay LSN.
func (r *Rounds) Replay(ref disklog.BlobRef, kind disklog.BlobKind, payload []byte) error {
	var err error
	r.dead, err = r.hd.replay(ref, kind, payload, r.dead)
	return err
}

// Replayed ends the replay: a round torn at the log's tail, missing its last blob, is dropped.
func (r *Rounds) Replayed() { r.dead = r.hd.recovered(r.dead) }

// ReplayLSN reports, when it moved since the last call, the LSN a restart replays the shard after:
// the one before its oldest blob a restart still needs. The shard tells the disk's owner, whose
// checkpoints name it with that LSN.
func (r *Rounds) ReplayLSN() (uint64, bool) {
	if r.runs != 0 || len(r.hd.refs) == 0 || r.hd.refs[0].LSN == r.told {
		return 0, false
	}
	r.told = r.hd.refs[0].LSN
	return r.told - 1, true
}

// Delete gives up the shard's state, every blob of it that Durable or Replay took now dead, and
// returns the LSN of the newest of them, 0 for none, which the disk's owner keeps the shard's
// owner from a new shard until the replay floor passes. A blob of a round still in flight is not
// among them: the round driver reports those of a shard taken off its loop, and raises that LSN
// to theirs.
func (r *Rounds) Delete() uint64 {
	var newest uint64
	if n := len(r.hd.refs); n > 0 {
		newest = r.hd.refs[n-1].LSN
	}
	r.deleted, r.queued, r.own, r.hd.rw, r.hd.cut = true, false, Round{}, nil, 0
	r.dead = append(r.dead, r.hd.refs...)
	r.hd.refs, r.hd.rounds, r.hd.closed, r.hd.newest = r.hd.refs[:0], r.hd.rounds[:0], 0, -1
	return newest
}

// Dead appends to dst the blobs that died since the last call, which the driver reports dead to
// the disk's owner, and forgets them.
func (r *Rounds) Dead(dst []disklog.BlobRef) []disklog.BlobRef {
	dst = append(dst, r.dead...)
	r.dead = r.dead[:0]
	return dst
}
