package raftshard

import (
	"errors"
	"fmt"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// Host carries the rounds of the shards on one disk to its log. It writes each round as blobs of
// its shard's owner on the hot stream, so a round joins whatever record the log writes next, tells
// which rounds each Durable event completes, and reports a round's blobs dead once a restart no
// longer needs them: every index the round holds is compacted, and a later durable round holds a
// newer hard state, else the shard's term and vote would be lost. A round whose blobs the pool has
// no buffers for waits until a later Durable event or the driver's Retry. A log that fails takes
// its shards with it.
type Host struct {
	// Log is the disk's log: set before any Write, and by Recovered after a restart's replay.
	Log     *disklog.Log
	at      map[uint32]int
	shards  []hosted
	flights []flight
	head    int
	waiting []uint32
	rec     []byte
	items   []disklog.Item
	dead    []disklog.BlobRef
}

// hosted is one shard: its storage, the index of its last compaction, the round waiting for pool
// buffers (0 for none) and its contents, the durable rounds a restart may still need, oldest
// first, newest the last of them holding a hard state (-1 for none), and refs, their blobs in
// order and then those of a round not yet whole, from closed on.
type hosted struct {
	owner     uint32
	st        *Storage
	compacted uint64
	wait      uint64
	pending   Round
	rounds    []liveRound
	newest    int
	refs      []disklog.BlobRef
	closed    int
}

type liveRound struct {
	summary
	blobs int
}

// flight is a blob submitted and not yet durable: round is the round its last blob completes, 0 for an earlier blob.
type flight struct {
	ticket disklog.Ticket
	owner  uint32
	round  uint64
	sum    summary
}

// Done is a shard's round a Durable event made durable, which the driver reports to the shard with Durable.
type Done struct {
	Owner uint32
	Round uint64
}

// NewHost is a host writing to log, nil until a restart's replay ends.
func NewHost(log *disklog.Log) *Host { return &Host{Log: log, at: map[uint32]int{}} }

// Add hosts the shard whose blobs carry owner and whose storage is st; an owner hosted or in flight already is a broken invariant of the caller.
func (h *Host) Add(owner uint32, st *Storage) {
	if _, ok := h.at[owner]; ok {
		panic(fmt.Sprintf("raftshard: owner %d hosted twice", owner))
	}
	for _, f := range h.flights[h.head:] {
		if f.owner == owner {
			panic(fmt.Sprintf("raftshard: owner %d hosted again with a round of its removed shard in flight", owner))
		}
	}
	h.at[owner] = len(h.shards)
	h.shards = append(h.shards, hosted{owner: owner, st: st, newest: -1})
}

func (h *Host) shard(owner uint32) *hosted {
	i, ok := h.at[owner]
	if !ok {
		panic(fmt.Sprintf("raftshard: owner %d not hosted", owner))
	}
	return &h.shards[i]
}

// Write submits round flush of owner's shard, whose contents r stay valid until the shard is told
// it is durable, as a Ready's do.
func (h *Host) Write(ctx *core.Context, owner uint32, flush uint64, r *Round, out *disklog.Output) error {
	hd := h.shard(owner)
	ok, err := h.submit(ctx, hd, flush, r, out)
	if err == nil && !ok {
		hd.wait, hd.pending = flush, *r
		h.waiting = append(h.waiting, owner)
	}
	return err
}

func (h *Host) submit(ctx *core.Context, hd *hosted, flush uint64, r *Round, out *disklog.Output) (bool, error) {
	h.rec = AppendRound(h.rec[:0], r)
	items, err := RoundItems(ctx.Pool, hd.owner, h.Log.MaxBlobBytes(), h.rec, h.items[:0])
	h.items = items[:0]
	if errors.Is(err, core.ErrPoolExhausted) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	sum := summarize(r)
	for k, it := range items {
		t, err := h.Log.Submit(ctx, disklog.Hot, it, out)
		if err != nil {
			for _, rest := range items[k:] {
				ctx.Pool.Free(rest.Buf)
			}
			return false, err
		}
		f := flight{ticket: t, owner: hd.owner}
		if k == len(items)-1 {
			f.round, f.sum = flush, sum
		}
		h.flights = append(h.flights, f)
	}
	return true, nil
}

// Durable takes a Durable event of the log, appending to dst each round it completes, then
// submits the rounds waiting for pool buffers that now fit.
func (h *Host) Durable(ctx *core.Context, ev *disklog.Event, out *disklog.Output, dst []Done) ([]Done, error) {
	for i, t := range ev.Tickets {
		if h.head == len(h.flights) || h.flights[h.head].ticket != t {
			continue
		}
		f := h.flights[h.head]
		h.head++
		ref := disklog.BlobRef{Segment: ev.Segment, Block: ev.Block, LSN: ev.LSN, Entry: uint16(i)}
		k, ok := h.at[f.owner]
		if !ok {
			h.dead = append(h.dead, ref)
			continue
		}
		hd := &h.shards[k]
		hd.refs = append(hd.refs, ref)
		if f.round != 0 {
			h.close(hd, f.sum)
			dst = append(dst, Done{Owner: f.owner, Round: f.round})
		}
	}
	if h.head == len(h.flights) {
		h.flights, h.head = h.flights[:0], 0
	}
	h.bury()
	return dst, h.Retry(ctx, out)
}

// Retry submits the rounds waiting for pool buffers that now fit; the driver calls it on its timer.
func (h *Host) Retry(ctx *core.Context, out *disklog.Output) error {
	n := 0
	for _, owner := range h.waiting {
		k, ok := h.at[owner]
		if !ok || h.shards[k].wait == 0 {
			n++
			continue
		}
		hd := &h.shards[k]
		sent, err := h.submit(ctx, hd, hd.wait, &hd.pending, out)
		if err != nil {
			return err
		}
		if !sent {
			break
		}
		hd.wait, hd.pending = 0, Round{}
		n++
	}
	h.waiting = append(h.waiting[:0], h.waiting[n:]...)
	return nil
}

// close makes the blobs since the last whole round one round. A hard state in it frees the round
// that held the newest one before, once every index that round holds is compacted, and no other:
// a compaction frees every other round it covers.
func (h *Host) close(hd *hosted, sum summary) {
	hd.rounds = append(hd.rounds, liveRound{summary: sum, blobs: len(hd.refs) - hd.closed})
	hd.closed = len(hd.refs)
	if !sum.hs {
		return
	}
	was := hd.newest
	hd.newest = len(hd.rounds) - 1
	if was >= 0 && hd.rounds[was].last <= hd.compacted {
		h.collect(hd)
	}
}

// collect drops the rounds a restart no longer needs, keeping their blobs to report dead.
func (h *Host) collect(hd *hosted) {
	keep, from, to, newest := 0, 0, 0, -1
	for j, r := range hd.rounds {
		refs := hd.refs[from : from+r.blobs]
		from += r.blobs
		if r.last <= hd.compacted && (!r.hs || j < hd.newest) {
			h.dead = append(h.dead, refs...)
			continue
		}
		if r.hs {
			newest = keep
		}
		hd.rounds[keep] = r
		keep++
		to += copy(hd.refs[to:], refs)
	}
	hd.closed = to
	to += copy(hd.refs[to:], hd.refs[from:])
	hd.rounds, hd.refs, hd.newest = hd.rounds[:keep], hd.refs[:to], newest
}

// bury reports the dead blobs to the log once there is one.
func (h *Host) bury() {
	if h.Log == nil || len(h.dead) == 0 {
		return
	}
	if err := h.Log.RaftBlobsDead(h.dead); err != nil {
		panic(fmt.Sprintf("raftshard: the log does not hold a Raft blob it made durable: %v", err))
	}
	h.dead = h.dead[:0]
}

// Compact compacts owner's storage to index i, whose configuration is cs, as its state machine
// does once the memtable that applied i is durable, and reports dead the rounds that frees.
func (h *Host) Compact(owner uint32, i uint64, cs raftpb.ConfState) error {
	hd := h.shard(owner)
	if err := hd.st.Compact(i, cs); err != nil {
		return err
	}
	hd.compacted = i
	h.collect(hd)
	h.bury()
	return nil
}

// Remove stops hosting owner's shard, as the driver does once the shard reports itself removed,
// and reports every blob of it dead, those still in flight once they are durable.
func (h *Host) Remove(owner uint32) {
	k := h.at[owner]
	h.dead = append(h.dead, h.shard(owner).refs...)
	last := len(h.shards) - 1
	h.shards[k] = h.shards[last]
	h.at[h.shards[k].owner] = k
	h.shards[last] = hosted{}
	h.shards = h.shards[:last]
	delete(h.at, owner)
	h.bury()
}

// Replayed takes a Replayed event of a restart's recovery, before Recovered: each hosted shard's
// rounds go to its storage, and the blobs of an owner not hosted, or of a round a later one's
// first blob shows was torn, are dead.
func (h *Host) Replayed(ev *disklog.Event) error {
	for i := range ev.Entries {
		e := &ev.Entries[i]
		if e.Kind != disklog.EntryBlob || e.BlobKind != disklog.BlobRaftEntries && e.BlobKind != disklog.BlobRaftState {
			continue
		}
		ref := disklog.BlobRef{Segment: ev.Segment, Block: ev.Block, LSN: ev.LSN, Entry: uint16(i)}
		k, ok := h.at[e.Owner]
		if !ok {
			h.dead = append(h.dead, ref)
			continue
		}
		hd := &h.shards[k]
		b := ev.Blob(i)
		if len(b) > 0 && b[0] == 1 {
			h.dead = append(h.dead, hd.refs[hd.closed:]...)
			hd.refs = hd.refs[:hd.closed]
		}
		hd.refs = append(hd.refs, ref)
		sum, err := hd.st.replay(e.BlobKind, b)
		if err != nil {
			return err
		}
		if sum.done {
			h.close(hd, sum)
		}
	}
	return nil
}

// Recovered ends a restart's replay with the log recovery handed over: a round torn at the log's
// tail, missing its last blob, is dropped and its blobs are dead.
func (h *Host) Recovered(log *disklog.Log) {
	h.Log = log
	for k := range h.shards {
		hd := &h.shards[k]
		h.dead = append(h.dead, hd.refs[hd.closed:]...)
		hd.refs = hd.refs[:hd.closed]
		hd.st.pending = hd.st.pending[:0]
	}
	h.bury()
}
