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
type Rounds struct {
	hd      hosted
	maxBlob uint64
	rec     []byte
	// flush is the round waiting for Ready, 0 for none, and round its contents; out is the round
	// whose blobs are in flight, 0 for none, with its summary and entry payload bytes.
	flush, out uint64
	round      Round
	sum        summary
	bytes      uint64
	dead       []disklog.BlobRef
	deleted    bool
}

// NewRounds keeps the rounds of the shard whose blobs carry owner and whose storage is st, already
// on the base its runs restore, cutting each into blobs of at most maxBlob bytes, the log's
// MaxBlobBytes.
func NewRounds(owner uint32, st *Storage, maxBlob uint64) *Rounds {
	return &Rounds{hd: hosted{owner: owner, st: st, newest: -1, compacted: st.Base().Index}, maxBlob: maxBlob}
}

// Write queues round flush, whose contents r stay valid until Durable returns it, for Ready; a
// round 0, or one while another is queued or in flight, is a broken invariant of the caller.
func (r *Rounds) Write(flush uint64, rd *Round) {
	if flush == 0 || r.flush != 0 || r.out != 0 || r.deleted {
		panic(fmt.Sprintf("raftshard: round %d written with round %d queued, %d in flight, deleted %v", flush, r.flush, r.out, r.deleted))
	}
	r.flush, r.round = flush, *rd
	r.hd.flying += payloadBytes(rd)
}

// HasReady reports whether a round waits for Ready: one is queued and none is in flight.
func (r *Rounds) HasReady() bool { return r.flush != 0 && r.out == 0 }

// Ready appends the waiting round's blobs to items, their buffers from pool. When pool has no
// buffers for them all it appends none, and the round waits: the shard's loop.Shard marks the
// shard again later, as Host's driver calls Retry, since the round driver collects only shards
// marked since its last round.
func (r *Rounds) Ready(pool core.BufPool, items []disklog.Item) ([]disklog.Item, error) {
	if !r.HasReady() {
		return items, nil
	}
	r.rec = AppendRound(r.rec[:0], &r.round)
	items, err := RoundItems(pool, r.hd.owner, r.maxBlob, r.rec, items)
	if errors.Is(err, core.ErrPoolExhausted) {
		return items, nil
	}
	if err != nil {
		return items, err
	}
	r.out, r.flush = r.flush, 0
	r.sum, r.bytes = summarize(&r.round), payloadBytes(&r.round)
	r.round = Round{}
	return items, nil
}

// Durable takes the places of the round in flight's blobs, in Ready order, or the error the log
// failed them with, and returns that round, which the replica is then told durable. With no round
// in flight, or after Delete, it is a broken invariant of the caller.
func (r *Rounds) Durable(err error, refs []disklog.BlobRef) (uint64, error) {
	if r.out == 0 || r.deleted {
		panic(fmt.Sprintf("raftshard: Durable with round %d in flight, deleted %v", r.out, r.deleted))
	}
	round := r.out
	r.out = 0
	if err != nil {
		return round, err
	}
	r.hd.refs = append(r.hd.refs, refs...)
	if r.bytes > 0 {
		r.hd.landed = append(r.hd.taken(), landedRound{round, r.bytes})
	}
	r.dead = r.hd.close(r.sum, r.dead)
	r.hd.done = round
	return round, nil
}

// Compact compacts the storage to index i, whose configuration is cs, as Host.Compact does.
func (r *Rounds) Compact(i uint64, cs raftpb.ConfState) error {
	var err error
	r.dead, err = r.hd.compact(i, cs, r.dead)
	return err
}

// Retained is the entry payload bytes the shard holds, as Host.Retained counts them.
func (r *Rounds) Retained() uint64 { return r.hd.retained() }

// Replay takes a blob of the shard a restart's replay found at ref, as recovery's mail hands it
// over, past the shard's replay LSN.
func (r *Rounds) Replay(ref disklog.BlobRef, kind disklog.BlobKind, payload []byte) error {
	var err error
	r.dead, err = r.hd.replay(ref, kind, payload, r.dead)
	return err
}

// Replayed ends the replay: a round torn at the log's tail, missing its last blob, is dropped.
func (r *Rounds) Replayed() { r.dead = r.hd.recovered(r.dead) }

// Delete gives up the shard's state, every blob of it that Durable or Replay took now dead. A blob
// of a round still in flight is not among them: the round driver reports those of a shard taken
// off its loop.
func (r *Rounds) Delete() {
	r.deleted = true
	r.dead = append(r.dead, r.hd.refs...)
	r.hd.refs, r.hd.rounds, r.hd.closed, r.hd.newest = r.hd.refs[:0], r.hd.rounds[:0], 0, -1
}

// Dead appends to dst the blobs that died since the last call, which the driver reports dead to
// the disk's owner, and forgets them.
func (r *Rounds) Dead(dst []disklog.BlobRef) []disklog.BlobRef {
	dst = append(dst, r.dead...)
	r.dead = r.dead[:0]
	return dst
}
