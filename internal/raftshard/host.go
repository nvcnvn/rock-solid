package raftshard

import (
	"errors"
	"fmt"
	"slices"

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
// its shards with it. The disk's checkpoint names each shard with the LSN its replay starts after,
// and no shard is given the owner of a removed one while that one's blobs can still replay. A
// shard on the round driver writes its own rounds through its loop; the host knows it to mail it
// a restart's replay, to name it in checkpoints with the replay LSN it reports, and to keep its
// owner once it is removed.
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
	// retired is the owners of removed shards with the LSN of each one's newest blob, kept until
	// the log's replay floor reaches it.
	retired []retiredOwner
	// unnamed is the owners of shards removed from the round driver that a manifest a restart may
	// load can still name.
	unnamed []unnamedOwner
	// leaving says the running checkpoint is the first to leave out a removed shard.
	leaving bool
	// remote is the shards on the round driver, remoteAt each one's place by owner, and mail
	// what a restart's replay sends them.
	remote   []remoteShard
	remoteAt map[uint32]int
	mail     []Mail
	// cp numbers the running due checkpoint the shards on the round driver flush their trees
	// under, 0 for none; pieces are their pieces in flight, and gone those of removed shards'
	// trees, dead once a checkpoint begun after the removal ends.
	cp     uint64
	pieces []remotePiece
	gone   []disklog.BlobRef
}

// remoteShard is a shard on the round driver: its owner, its loop and ShardID there, the LSN at
// or before which a restart's replay skips its blobs, the replay LSN a checkpoint names it with,
// at or before every blob of it a restart still needs, and the share it was last mailed, 0 for
// none; runs are the roots of its tree's runs that the newest manifest names, with their merge
// bounds, pieces every live piece of its tree, and flush its part of the running due checkpoint,
// if it has one.
type remoteShard struct {
	owner uint32
	rep   disklog.Replica
	// named says a manifest naming the shard was durable when it was mailed or loaded.
	named  bool
	loop   int
	id     uint32
	from   uint64
	replay uint64
	share  uint64
	runs   []disklog.BlobRef
	bounds []disklog.MergeBound
	pieces []disklog.BlobRef
	costs  []disklog.RunCost
	flush  *remoteFlush
}

// remoteFlush is a shard on the round driver's part of a due checkpoint: the piece it replied
// that waits for the log, if any, root if that is a run's last, the pieces written and the roots
// among them, done once it has written all it has, and named once it has named the runs its
// flush carries, with their bounds.
type remoteFlush struct {
	next    []byte
	root    bool
	pieces  []disklog.BlobRef
	roots   []disklog.BlobRef
	done    bool
	named   bool
	carried []disklog.BlobRef
	bounds  []disklog.MergeBound
}

type remotePiece struct {
	ticket disklog.Ticket
	owner  uint32
	root   bool
}

// MailKind tags a Mail, as loop.MailKind does.
type MailKind uint8

// Mail kinds.
const (
	// MailReplayed is a blob a restart's replay found past the shard's replay LSN.
	MailReplayed MailKind = iota + 1
	// MailRecovered ends the replay.
	MailRecovered
	// MailCheckpointed says the manifest of a due checkpoint is durable, Base the base of the due
	// manifest before it.
	MailCheckpointed
	// MailShare gives the shard its Share of twice max_replay_bytes in entries.
	MailShare
	// MailFlush begins due checkpoint Base: the shard marks its tree and replies its first piece.
	MailFlush
	// MailPiece says the shard's last piece is durable at Ref: it replies its next.
	MailPiece
	// MailFlushFailed says the checkpoint failed.
	MailFlushFailed
	// MailRun is a run the loaded manifest names, its root at Ref and its pieces, root last,
	// those past Bound's dead ones if a merge bounds it.
	MailRun
	// MailRewrite asks the shard to rewrite its runs with a piece in the segment of Ref, a victim
	// of the cleaner.
	MailRewrite
)

// Mail is what the disk's owner sends a shard on the round driver, at Shard on the loop of ID
// Loop, Owner its owner key on mail that asks it for a piece: a blob at Ref of kind Blob and bytes
// Data, or a run's Pieces and Bound, as Kind says.
type Mail struct {
	Kind   MailKind
	Loop   int
	Shard  uint32
	Owner  uint32
	Base   uint64
	Share  uint64
	Ref    disklog.BlobRef
	Blob   disklog.BlobKind
	Data   []byte
	Pieces [][]byte
	Bound  *disklog.MergeBound
}

type retiredOwner struct {
	owner uint32
	lsn   uint64
}

// unnamedOwner is a removed shard's owner and the durable LSN of the first checkpoint begun
// after the removal, 0 until one begins: a manifest past it leaves the owner out.
type unnamedOwner struct {
	owner uint32
	after uint64
}

// hosted is one shard: its storage, the index of its last compaction, the round waiting for pool
// buffers (0 for none) and its contents, the durable rounds a restart may still need, oldest
// first, newest the last of them holding a hard state (-1 for none), and refs, their blobs in
// order and then those of a round not yet whole, from closed on; from is the LSN at or before
// which a restart's replay skips its blobs, inflight counts its blobs not yet durable, flying
// the entry payload bytes of its rounds written and not yet taken into its storage, landed its
// rounds durable and not yet taken, with their payload bytes, done its newest round reported
// durable, rw its full rewrite going out, if one is, and cut the compaction, with its
// configuration, waiting for that rewrite to read the entries it drops (0 for none); kickedAt is
// the commit at which fullDue last found it full, if kicked.
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
	from      uint64
	inflight  int
	flying    uint64
	landed    []landedRound
	done      uint64
	rw        *rewriting
	cut       uint64
	cutConf   raftpb.ConfState
	kicked    bool
	kickedAt  uint64
}

type landedRound struct{ round, bytes uint64 }

// rewriting is a full rewrite's round going out a blob at a time, the storage left as it was when
// it began: its record is head, the encodings of entries next to end, off bytes of next's already
// out, then tail; left counts the record's bytes not yet out, and sum is the round's summary.
type rewriting struct {
	head, tail []byte
	next, end  uint64
	off        int
	left       uint64
	begun      bool
	sum        summary
}

// rewriteBlobs is how many blobs of a full rewrite may be in flight, so that it takes a few
// records' worth of the pool, never the whole of a shard's share at once.
const rewriteBlobs = 4

func newRewriting(st *Storage, newest *liveRound) *rewriting {
	r := Round{HardState: newest.state, Removed: newest.removed, Deleted: newest.deleted}
	rec := AppendRound(nil, &r)
	rw := &rewriting{head: rec[:1:1], tail: rec[1:], next: st.base.Index + 1, end: st.last(), sum: summarize(&r)}
	if rw.end >= rw.next {
		rw.head[0] |= flagEntries
		rw.head = le.AppendUint32(le.AppendUint64(rw.head, rw.next), uint32(rw.end-rw.next+1))
		rw.sum.last = rw.end
	}
	rw.sum.full = true
	rw.left = uint64(len(rw.head) + len(rw.tail))
	for i := range st.ents {
		rw.left += entryHeadBytes + uint64(st.ents[i].n)
	}
	return rw
}

// fill copies the record's next len(b) bytes into b.
func (rw *rewriting) fill(st *Storage, b []byte) {
	for len(b) > 0 {
		switch {
		case len(rw.head) > 0:
			n := copy(b, rw.head)
			rw.head, b = rw.head[n:], b[n:]
		case rw.next > rw.end:
			n := copy(b, rw.tail)
			rw.tail, b = rw.tail[n:], b[n:]
		default:
			e := st.entry(rw.next)
			var head [entryHeadBytes]byte
			enc := appendEntryHead(head[:0], &e)
			var n int
			if rw.off < len(enc) {
				n = copy(b, enc[rw.off:])
			} else {
				n = copy(b, e.Data[rw.off-len(enc):])
			}
			b, rw.off = b[n:], rw.off+n
			if rw.off == len(enc)+len(e.Data) {
				rw.next, rw.off = rw.next+1, 0
			}
		}
	}
}

type liveRound struct {
	summary
	blobs int
}

// flight is a blob submitted and not yet durable, after the durable LSN at its submit: a round's
// last blob carries its summary and its entries' payload bytes, and round, the shard's round it
// completes, is 0 for a round the host wrote itself.
type flight struct {
	ticket disklog.Ticket
	owner  uint32
	round  uint64
	sum    summary
	bytes  uint64
	after  uint64
}

// Done is a shard's round a Durable event made durable, which the driver reports to the shard with Durable.
type Done struct {
	Owner uint32
	Round uint64
}

// NewHost is a host writing to log, nil until a restart's replay ends.
func NewHost(log *disklog.Log) *Host {
	return &Host{Log: log, at: map[uint32]int{}, remoteAt: map[uint32]int{}}
}

// Add hosts the shard whose blobs carry owner and whose storage is st; an owner Reusable does not
// allow is a broken invariant of the caller.
func (h *Host) Add(owner uint32, st *Storage) {
	if !h.Reusable(owner) {
		panic(fmt.Sprintf("raftshard: owner %d hosted, in flight or with blobs that can replay", owner))
	}
	h.at[owner] = len(h.shards)
	h.shards = append(h.shards, hosted{owner: owner, st: st, newest: -1})
}

// AddRemote knows the shard whose blobs carry owner as shard id on the round driver of loop, so a
// restart's replay mails it its blobs, as Add does for a hosted shard, and checkpoints name it,
// with rep, the replica it holds the state of.
// It is added before it writes a blob: until it reports a replay LSN, it is named with the log's
// durable LSN when added, or with the one the loaded manifest names it with.
func (h *Host) AddRemote(owner uint32, loop int, id uint32, rep disklog.Replica) {
	if !h.Reusable(owner) {
		panic(fmt.Sprintf("raftshard: owner %d hosted, in flight or with blobs that can replay", owner))
	}
	rs := remoteShard{owner: owner, rep: rep, loop: loop, id: id}
	if h.Log != nil {
		rs.replay = h.Log.DurableLSN()
	}
	h.remoteAt[owner] = len(h.remote)
	h.remote = append(h.remote, rs)
}

func (h *Host) remoteShard(owner uint32) int {
	r, ok := h.remoteAt[owner]
	if !ok {
		panic(fmt.Sprintf("raftshard: owner %d not on the round driver", owner))
	}
	return r
}

// RemoteReplay takes the replay LSN owner's shard on the round driver reported: a restart needs
// none of its blobs at or before lsn.
func (h *Host) RemoteReplay(owner uint32, lsn uint64) {
	h.remote[h.remoteShard(owner)].replay = lsn
}

// RemoveRemote forgets owner's shard on the round driver, taken off its loop with every blob of it
// reported dead, the newest at lsn: owner goes to no new shard while that blob can replay or a
// manifest a restart may load names it, and the pieces of its tree are dead once a checkpoint
// begun after now ends (Gone), the running one's manifest still naming them.
func (h *Host) RemoveRemote(owner uint32, lsn uint64) {
	r := h.remoteShard(owner)
	if lsn > 0 {
		h.retire(owner, lsn)
	}
	h.unnamed = append(h.unnamed, unnamedOwner{owner: owner})
	h.gone = append(h.gone, h.remote[r].pieces...)
	if f := h.remote[r].flush; f != nil {
		h.gone = append(h.gone, f.pieces...)
	}
	last := len(h.remote) - 1
	h.remote[r] = h.remote[last]
	h.remoteAt[h.remote[r].owner] = r
	h.remote = h.remote[:last]
	delete(h.remoteAt, owner)
}

// Mail appends to dst what the replay sent the shards on the round driver since the last call, in
// the order sent, and forgets it.
func (h *Host) Mail(dst []Mail) []Mail {
	dst = append(dst, h.mail...)
	clear(h.mail)
	h.mail = h.mail[:0]
	return dst
}

// Checkpointed mails each shard on the round driver that flushed under the running due checkpoint
// that its manifest is durable, naming the runs it wrote, base the base of the due manifest before
// it: the shard compacts to what its mark recorded, and writes again what it keeps at or before
// base, as Rewrite does for a hosted shard when the next checkpoint begins; its tree then
// reports the pieces the manifest left out (RemoteDead). The first manifest naming a replica asks
// the log to name it in a segment header (disklog.Log.NameNewest), as only one a header names is
// the one a restart loads, so a round the shard starts after the mail is durable only once a
// restart restores the shard; so does the first manifest leaving out a removed one, so a restart
// soon restores it no more.
func (h *Host) Checkpointed(base uint64) {
	h.cp = 0
	if h.leaving {
		h.leaving = false
		h.Log.NameNewest()
	}
	for i := range h.remote {
		rs := &h.remote[i]
		if rs.flush == nil {
			continue
		}
		if !rs.named && rs.rep != (disklog.Replica{}) {
			rs.named = true
			h.Log.NameNewest()
		}
		rs.keep(rs.flush)
		h.mail = append(h.mail, Mail{Kind: MailCheckpointed, Loop: rs.loop, Shard: rs.id, Base: base})
	}
}

// keep makes what f wrote, and the runs it named as carried, rs's runs, as a manifest naming them
// does, and ends f.
func (rs *remoteShard) keep(f *remoteFlush) {
	if f.named {
		rs.runs, rs.bounds = f.carried, f.bounds
	}
	rs.runs, rs.pieces, rs.flush = append(slices.Clip(rs.runs), f.roots...), append(rs.pieces, f.pieces...), nil
}

// RemoteRuns takes the runs, with bounds, that owner's shard on the round driver replied its flush
// for checkpoint cp carries (loop.ReplyRuns), before its first piece, and has the checkpoint's
// manifest name them in place of the runs the newest manifest named, as its merges since may have
// replaced some. A reply for a checkpoint no longer running, or from a shard no longer known, is
// stale and dropped.
func (h *Host) RemoteRuns(owner uint32, cp uint64, runs []disklog.BlobRef, bounds []disklog.MergeBound) {
	r, ok := h.remoteAt[owner]
	if !ok || cp != h.cp || h.remote[r].flush == nil {
		return
	}
	if err := h.Log.CheckpointRuns(owner, runs, bounds); err != nil {
		panic(fmt.Sprintf("raftshard: shard %d named runs %v, bounds %v, for checkpoint %d: %v", owner, runs, bounds, cp, err))
	}
	f := h.remote[r].flush
	f.named, f.carried, f.bounds = true, runs, bounds
}

// RemoteDead takes the pieces owner's shard on the round driver replied its tree no longer names
// (loop.ReplyDead), once the manifest that leaves them out is durable: they are dead once a
// checkpoint begun after now ends (Gone), as no later manifest names them. A reply from a shard no
// longer known is dropped, as its removal took every piece of its for Gone.
func (h *Host) RemoteDead(owner uint32, refs []disklog.BlobRef) {
	r, ok := h.remoteAt[owner]
	if !ok {
		return
	}
	rs := &h.remote[r]
	rs.pieces = slices.DeleteFunc(rs.pieces, func(p disklog.BlobRef) bool {
		if slices.Contains(refs, p) {
			h.gone = append(h.gone, p)
			return true
		}
		return false
	})
}

// RemoteCosts takes the runs of owner's tree, a shard on the round driver, as the cleaner weighs
// them (loop.ReplyCosts), in place of those it replied before. A reply from a shard no longer
// known is dropped.
func (h *Host) RemoteCosts(owner uint32, costs []disklog.RunCost) {
	if r, ok := h.remoteAt[owner]; ok {
		h.remote[r].costs = costs
	}
}

// RewriteIn adds to the cleaner's weighing of segment, as disklog.RewriteIn does, the runs of the
// shards on the round driver as they last replied them: a stale reply only misweighs a victim,
// since the log frees no segment holding a live piece.
func (h *Host) RewriteIn(segment uint32, pieces []disklog.BlobRef, steps int, extra uint64) (_ []disklog.BlobRef, held bool, _ int, _ uint64) {
	for i := range h.remote {
		var hd bool
		pieces, hd, steps, extra = disklog.RewriteIn(h.remote[i].costs, segment, pieces, steps, extra)
		held = held || hd
	}
	return pieces, held, steps, extra
}

// MarkRewrite mails each shard on the round driver with a run no merge holds with a piece in
// segment, as it last replied its runs, to rewrite those runs (MailRewrite), and returns how
// many there are.
func (h *Host) MarkRewrite(segment uint32) int {
	n := 0
	for i := range h.remote {
		rs := &h.remote[i]
		k := 0
		for _, c := range rs.costs {
			if !c.Held && slices.ContainsFunc(c.Pieces, func(p disklog.BlobRef) bool { return p.Segment == segment }) {
				k++
			}
		}
		if k > 0 {
			n += k
			h.mail = append(h.mail, Mail{Kind: MailRewrite, Loop: rs.loop, Shard: rs.id, Ref: disklog.BlobRef{Segment: segment}})
		}
	}
	return n
}

// GoneWaiting reports whether pieces wait for a checkpoint to begin before they can die (Gone).
func (h *Host) GoneWaiting() bool { return len(h.gone) > 0 }

// BeginFlush asks each shard on the round driver for its part of due checkpoint cp, numbered by
// its base, a durable LSN: the shard marks its tree with its storage's base and flushes it, one
// piece at a time (RemotePiece, WritePieces), and the checkpoint ends only once none is Flushing.
func (h *Host) BeginFlush(cp uint64) {
	h.cp = cp
	for i := range h.remote {
		rs := &h.remote[i]
		rs.flush = &remoteFlush{}
		h.mail = append(h.mail, Mail{Kind: MailFlush, Loop: rs.loop, Shard: rs.id, Owner: rs.owner, Base: cp})
	}
}

// RemotePiece takes the next piece owner's shard on the round driver replied for checkpoint cp,
// data, root marking a run's last, or with no data the end of its flush. A piece for a checkpoint
// no longer running, or from a shard no longer known, is stale and dropped.
func (h *Host) RemotePiece(owner uint32, cp uint64, data []byte, root bool) {
	r, ok := h.remoteAt[owner]
	if !ok || cp != h.cp || h.remote[r].flush == nil {
		return
	}
	f := h.remote[r].flush
	if len(data) == 0 {
		f.done = true
		return
	}
	f.next, f.root = data, root
}

// Flushing reports whether a shard on the round driver has yet to write all of its part of the
// running checkpoint.
func (h *Host) Flushing() bool {
	for i := range h.remote {
		if f := h.remote[i].flush; f != nil && !f.done {
			return true
		}
	}
	return false
}

// WritePieces hands the log, as pieces of the running checkpoint, those the shards on the round
// driver replied; one the pool has no buffer for waits for a later call. Once durable, Durable
// mails its place to its shard, which replies its next.
func (h *Host) WritePieces(ctx *core.Context, out *disklog.Output) error {
	for i := range h.remote {
		rs := &h.remote[i]
		f := rs.flush
		if f == nil || f.next == nil {
			continue
		}
		buf, err := ctx.Pool.Alloc(len(f.next))
		if errors.Is(err, core.ErrPoolExhausted) {
			return nil
		}
		if err != nil {
			return err
		}
		copy(ctx.Pool.Bytes(buf), f.next)
		t, err := h.Log.CheckpointPiece(ctx, rs.owner, buf, uint64(len(f.next)), f.root, out)
		if err != nil {
			ctx.Pool.Free(buf)
			return err
		}
		h.pieces = append(h.pieces, remotePiece{t, rs.owner, f.root})
		f.next = nil
	}
	return nil
}

// pieceDurable takes the place of a piece in flight, if ticket t is one, and reports whether it was.
func (h *Host) pieceDurable(t disklog.Ticket, ref disklog.BlobRef) bool {
	k := slices.IndexFunc(h.pieces, func(p remotePiece) bool { return p.ticket == t })
	if k < 0 {
		return false
	}
	p := h.pieces[k]
	h.pieces = slices.Delete(h.pieces, k, k+1)
	r, ok := h.remoteAt[p.owner]
	if !ok || h.remote[r].flush == nil {
		h.gone = append(h.gone, ref)
		return true
	}
	rs := &h.remote[r]
	rs.flush.pieces = append(rs.flush.pieces, ref)
	if p.root {
		rs.flush.roots = append(rs.flush.roots, ref)
	}
	h.mail = append(h.mail, Mail{Kind: MailPiece, Loop: rs.loop, Shard: rs.id, Owner: rs.owner, Ref: ref, Base: h.cp})
	return true
}

// FlushFailed ends the shards on the round driver's part of a due checkpoint that failed and
// appends to dead the pieces no run holds. A shard that wrote all it had keeps the runs it wrote,
// as its tree does, and the next checkpoint names them; another's pieces are dead.
func (h *Host) FlushFailed(dead []disklog.BlobRef) []disklog.BlobRef {
	h.cp = 0
	for i := range h.remote {
		rs := &h.remote[i]
		f := rs.flush
		if f == nil {
			continue
		}
		if f.done {
			rs.keep(f)
		} else {
			dead = append(dead, f.pieces...)
		}
		rs.flush = nil
		h.mail = append(h.mail, Mail{Kind: MailFlushFailed, Loop: rs.loop, Shard: rs.id})
	}
	return dead
}

// Gone appends to dst the pieces of removed shards' trees since the last call, which a checkpoint
// beginning now leaves out: dead once it ends.
func (h *Host) Gone(dst []disklog.BlobRef) []disklog.BlobRef {
	dst = append(dst, h.gone...)
	h.gone = h.gone[:0]
	return dst
}

// Remote reports whether owner's shard is on the round driver.
func (h *Host) Remote(owner uint32) bool {
	_, ok := h.remoteAt[owner]
	return ok
}

// LoadedRun takes a run of owner's tree, a shard on the round driver, that the manifest a restart
// loaded names: refs its pieces in order and root last, pieces their bytes. It mails the run to
// the shard, before any blob the replay mails it.
func (h *Host) LoadedRun(owner uint32, refs []disklog.BlobRef, pieces [][]byte, bound *disklog.MergeBound) {
	rs := &h.remote[h.remoteShard(owner)]
	rs.pieces = append(rs.pieces, refs...)
	h.mail = append(h.mail, Mail{Kind: MailRun, Loop: rs.loop, Shard: rs.id, Ref: refs[len(refs)-1], Pieces: pieces, Bound: bound})
}

// Share is each shard's share of total, divided among the shards hosted and on the round driver.
func (h *Host) Share(total uint64) uint64 {
	return total / uint64(max(1, len(h.shards)+len(h.remote)))
}

// MailShares mails each shard on the round driver its share of total, as Share divides it, when
// that differs from the share it was mailed last.
func (h *Host) MailShares(total uint64) {
	share := h.Share(total)
	for i := range h.remote {
		if rs := &h.remote[i]; rs.share != share {
			rs.share = share
			h.mail = append(h.mail, Mail{Kind: MailShare, Loop: rs.loop, Shard: rs.id, Share: share})
		}
	}
}

// FullDue reports whether owner's shard, which applied index applied, retains share or more and
// applied a commit FullDue has not reported for it yet: a checkpoint then compacts what it can,
// and none more is taken for it until its commit moves.
func (h *Host) FullDue(owner uint32, applied, share uint64) bool {
	return h.shard(owner).fullDue(applied, share)
}

func (hd *hosted) fullDue(applied, share uint64) bool {
	c := hd.st.HardState().Commit
	if hd.kicked && hd.kickedAt == c || applied < c || hd.retained() < share {
		return false
	}
	hd.kicked, hd.kickedAt = true, c
	return true
}

func (h *Host) shard(owner uint32) *hosted {
	i, ok := h.at[owner]
	if !ok {
		panic(fmt.Sprintf("raftshard: owner %d not hosted", owner))
	}
	return &h.shards[i]
}

// Write submits round flush of owner's shard, whose contents r stay valid until the shard is told
// it is durable, as a Ready's do; it waits behind the shard's full rewrite, if one is going out.
func (h *Host) Write(ctx *core.Context, owner uint32, flush uint64, r *Round, out *disklog.Output) error {
	hd := h.shard(owner)
	hd.flying += payloadBytes(r)
	var ok bool
	var err error
	if hd.rw == nil {
		ok, err = h.submit(ctx, hd, flush, r, out)
	}
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
		f := flight{ticket: t, owner: hd.owner, after: h.Log.DurableLSN()}
		hd.inflight++
		if k == len(items)-1 {
			f.round, f.sum = flush, sum
			if flush != 0 {
				f.bytes = payloadBytes(r)
			}
		}
		h.flights = append(h.flights, f)
	}
	return true, nil
}

// Durable takes a Durable event of the log, appending to dst each round it completes, then
// submits the rounds waiting for pool buffers that now fit.
func (h *Host) Durable(ctx *core.Context, ev *disklog.Event, out *disklog.Output, dst []Done) ([]Done, error) {
	for i, t := range ev.Tickets {
		ref := disklog.BlobRef{Segment: ev.Segment, Block: ev.Block, LSN: ev.LSN, Entry: uint16(i)}
		if len(h.pieces) > 0 && h.pieceDurable(t, ref) {
			continue
		}
		if h.head == len(h.flights) || h.flights[h.head].ticket != t {
			continue
		}
		f := h.flights[h.head]
		h.head++
		k, ok := h.at[f.owner]
		if !ok {
			h.retire(f.owner, ref.LSN)
			h.dead = append(h.dead, ref)
			continue
		}
		hd := &h.shards[k]
		hd.refs = append(hd.refs, ref)
		hd.inflight--
		if f.bytes > 0 {
			hd.landed = append(hd.taken(), landedRound{f.round, f.bytes})
		}
		if f.sum.done {
			h.dead = hd.close(f.sum, h.dead)
			if f.round != 0 {
				hd.done = f.round
				dst = append(dst, Done{Owner: f.owner, Round: f.round})
			}
		}
	}
	if h.head == len(h.flights) {
		h.flights, h.head = h.flights[:0], 0
	}
	h.bury()
	return dst, h.Retry(ctx, out)
}

func payloadBytes(r *Round) uint64 {
	var n uint64
	for i := range r.Entries {
		n += uint64(len(r.Entries[i].Data))
	}
	return n
}

// Retained is the entry payload bytes owner's shard holds in its storage, as Storage.Bytes counts
// them, and in its rounds written and not yet taken into it, durable or not.
func (h *Host) Retained(owner uint32) uint64 {
	return h.shard(owner).retained()
}

func (hd *hosted) retained() uint64 {
	hd.landed = hd.taken()
	return hd.st.Bytes() + hd.flying
}

// taken drops the landed rounds the storage took from flying and returns those left.
func (hd *hosted) taken() []landedRound {
	k := 0
	for _, r := range hd.landed {
		if r.round <= hd.st.took {
			hd.flying -= r.bytes
			continue
		}
		hd.landed[k] = r
		k++
	}
	return hd.landed[:k]
}

// Retry sends the next blobs of the full rewrites going out, then submits the rounds waiting for
// pool buffers that now fit, in order, but for those waiting behind their shard's rewrite; the
// driver calls it on its timer.
func (h *Host) Retry(ctx *core.Context, out *disklog.Output) error {
	for k := range h.shards {
		if err := h.step(ctx, &h.shards[k], out); err != nil {
			return err
		}
	}
	keep, full := 0, false
	for _, owner := range h.waiting {
		k, ok := h.at[owner]
		if !ok || h.shards[k].wait == 0 {
			continue
		}
		hd := &h.shards[k]
		if !full && hd.rw == nil {
			sent, err := h.submit(ctx, hd, hd.wait, &hd.pending, out)
			if err != nil {
				return err
			}
			if sent {
				hd.wait, hd.pending = 0, Round{}
				continue
			}
			full = true
		}
		h.waiting[keep] = owner
		keep++
	}
	h.waiting = h.waiting[:keep]
	return nil
}

// step sends the next blobs of hd's full rewrite while the pool has buffers for them, at most
// rewriteBlobs in flight, and once its last is out, the compaction that waited for it.
func (h *Host) step(ctx *core.Context, hd *hosted, out *disklog.Output) error {
	for rw := hd.rw; rw != nil && hd.inflight < rewriteBlobs; rw = hd.rw {
		it, err := hd.nextRewrite(ctx.Pool, h.Log.MaxBlobBytes())
		if errors.Is(err, core.ErrPoolExhausted) {
			return nil
		}
		if err != nil {
			return err
		}
		t, err := h.Log.Submit(ctx, disklog.Hot, it, out)
		if err != nil {
			ctx.Pool.Free(it.Buf)
			return err
		}
		f := flight{ticket: t, owner: hd.owner, after: h.Log.DurableLSN()}
		if hd.rw == nil {
			f.sum = rw.sum
		}
		hd.inflight++
		h.flights = append(h.flights, f)
	}
	if hd.rw != nil || hd.cut == 0 {
		return nil
	}
	i := hd.cut
	hd.cut = 0
	return h.Compact(hd.owner, i, hd.cutConf)
}

// nextRewrite cuts the next blob of hd's full rewrite, of at most maxBlob bytes, into a buffer
// from pool, and drops the rewrite once its last blob is cut.
func (hd *hosted) nextRewrite(pool core.BufPool, maxBlob uint64) (disklog.Item, error) {
	rw := hd.rw
	k := min(rw.left, maxBlob-1)
	buf, err := pool.Alloc(int(k + 1))
	if err != nil {
		return disklog.Item{}, err
	}
	b := pool.Bytes(buf)[:k+1]
	b[0] = 0
	if !rw.begun {
		b[0] = 1
	}
	rw.fill(hd.st, b[1:])
	rw.begun, rw.left = true, rw.left-k
	kind := disklog.BlobRaftEntries
	if rw.left == 0 {
		kind, hd.rw = disklog.BlobRaftState, nil
	}
	return disklog.Item{Entry: disklog.BlobEntry(kind, hd.owner, k+1), Buf: buf}, nil
}

// close makes the blobs since the last whole round one round, appending to dead the blobs that
// frees. A hard state in it frees the round that held the newest one before, once every index that
// round holds is compacted, and no other: a compaction frees every other round it covers.
func (hd *hosted) close(sum summary, dead []disklog.BlobRef) []disklog.BlobRef {
	if sum.full {
		dead = append(dead, hd.refs[:hd.closed]...)
		hd.refs = append(hd.refs[:0], hd.refs[hd.closed:]...)
		hd.rounds, hd.closed, hd.newest = hd.rounds[:0], 0, -1
	}
	hd.rounds = append(hd.rounds, liveRound{summary: sum, blobs: len(hd.refs) - hd.closed})
	hd.closed = len(hd.refs)
	if !sum.hs {
		return dead
	}
	was := hd.newest
	hd.newest = len(hd.rounds) - 1
	if was >= 0 && hd.rounds[was].last <= hd.compacted {
		dead = hd.collect(dead)
	}
	return dead
}

// collect drops the rounds a restart no longer needs, appending their blobs to dead.
func (hd *hosted) collect(dead []disklog.BlobRef) []disklog.BlobRef {
	keep, from, to, newest := 0, 0, 0, -1
	for j, r := range hd.rounds {
		refs := hd.refs[from : from+r.blobs]
		from += r.blobs
		if r.last <= hd.compacted && (!r.hs || j < hd.newest) {
			dead = append(dead, refs...)
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
	return dead
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
// does once the memtable that applied i is durable, and reports dead the rounds that frees; while
// the shard's full rewrite goes out, it waits for that rewrite to read the entries it drops.
func (h *Host) Compact(owner uint32, i uint64, cs raftpb.ConfState) error {
	hd := h.shard(owner)
	if hd.rw != nil {
		hd.cut, hd.cutConf = i, cs
		return nil
	}
	var err error
	h.dead, err = hd.compact(i, cs, h.dead)
	h.bury()
	return err
}

func (hd *hosted) compact(i uint64, cs raftpb.ConfState, dead []disklog.BlobRef) ([]disklog.BlobRef, error) {
	if err := hd.st.Compact(i, cs); err != nil {
		return dead, err
	}
	hd.compacted = i
	return hd.collect(dead), nil
}

// Remove stops hosting owner's shard, as the driver does once the shard reports itself removed,
// and reports every blob of it dead, those still in flight once they are durable.
func (h *Host) Remove(owner uint32) {
	k := h.at[owner]
	refs := h.shard(owner).refs
	if n := len(refs); n > 0 {
		h.retire(owner, refs[n-1].LSN)
	}
	h.dead = append(h.dead, refs...)
	last := len(h.shards) - 1
	h.shards[k] = h.shards[last]
	h.at[h.shards[k].owner] = k
	h.shards[last] = hosted{}
	h.shards = h.shards[:last]
	delete(h.at, owner)
	h.bury()
}

// Replayed takes a Replayed event of a restart's recovery, before Recovered: each hosted shard's
// rounds after its replay LSN go to its storage, each blob of a shard on the round driver after
// its replay LSN to Mail, and the blobs at or before a shard's replay LSN, the blobs of an owner
// neither hosted nor on the round driver, and those of a round a later one's first blob shows was
// torn, are dead.
func (h *Host) Replayed(ev *disklog.Event) error {
	for i := range ev.Entries {
		e := &ev.Entries[i]
		if e.Kind != disklog.EntryBlob || e.BlobKind != disklog.BlobRaftEntries && e.BlobKind != disklog.BlobRaftState {
			continue
		}
		ref := disklog.BlobRef{Segment: ev.Segment, Block: ev.Block, LSN: ev.LSN, Entry: uint16(i)}
		if r, ok := h.remoteAt[e.Owner]; ok {
			if rs := &h.remote[r]; ev.LSN > rs.from {
				h.mail = append(h.mail, Mail{Kind: MailReplayed, Loop: rs.loop, Shard: rs.id, Ref: ref, Blob: e.BlobKind, Data: append([]byte(nil), ev.Blob(i)...)})
			} else {
				h.dead = append(h.dead, ref)
			}
			continue
		}
		k, ok := h.at[e.Owner]
		if !ok {
			h.retire(e.Owner, ref.LSN)
		}
		if !ok || ev.LSN <= h.shards[k].from {
			h.dead = append(h.dead, ref)
			continue
		}
		var err error
		if h.dead, err = h.shards[k].replay(ref, e.BlobKind, ev.Blob(i), h.dead); err != nil {
			return err
		}
	}
	return nil
}

// replay takes a replayed blob at ref, appending to dead those of a round its first blob shows torn.
func (hd *hosted) replay(ref disklog.BlobRef, kind disklog.BlobKind, b []byte, dead []disklog.BlobRef) ([]disklog.BlobRef, error) {
	if len(b) > 0 && b[0] == 1 {
		dead = append(dead, hd.refs[hd.closed:]...)
		hd.refs = hd.refs[:hd.closed]
	}
	hd.refs = append(hd.refs, ref)
	sum, err := hd.st.replay(kind, b)
	if err != nil {
		return dead, err
	}
	if sum.done {
		dead = hd.close(sum, dead)
	}
	return dead, nil
}

// Recovered ends a restart's replay with the log recovery handed over: a round torn at the log's
// tail, missing its last blob, is dropped and its blobs are dead, as are those of the rounds the
// base covers whose hard state a later round replaced; each shard on the round driver is mailed
// the end of the replay, to do the same.
func (h *Host) Recovered(log *disklog.Log) {
	h.Log = log
	for k := range h.shards {
		h.dead = h.shards[k].recovered(h.dead)
	}
	for _, rs := range h.remote {
		h.mail = append(h.mail, Mail{Kind: MailRecovered, Loop: rs.loop, Shard: rs.id})
	}
	h.bury()
}

// recovered drops a round torn at the log's tail, appending its blobs to dead with those of the
// rounds a restart no longer needs.
func (hd *hosted) recovered(dead []disklog.BlobRef) []disklog.BlobRef {
	dead = append(dead, hd.refs[hd.closed:]...)
	hd.refs = hd.refs[:hd.closed]
	hd.st.pending = nil
	return hd.collect(dead)
}

// Loaded takes the manifest a restart's recovery loaded, before its replay and after every shard
// it names is added on the base its runs restore: such a shard is compacted to that base, and
// skips its blobs at or before its replay LSN, which earlier compactions and hard states made dead;
// a shard on the round driver keeps its runs, which LoadedRun mailed it, for the next checkpoint.
func (h *Host) Loaded(m *disklog.Manifest) {
	for _, ix := range m.Indexes {
		if r, ok := h.remoteAt[ix.Owner]; ok {
			rs := &h.remote[r]
			rs.from, rs.replay, rs.runs, rs.bounds, rs.named = ix.ReplayLSN, ix.ReplayLSN, slices.Clone(ix.Runs), slices.Clone(ix.Bounds), true
		}
		if k, ok := h.at[ix.Owner]; ok {
			hd := &h.shards[k]
			hd.from, hd.compacted = ix.ReplayLSN, hd.st.base.Index
		}
	}
}

// Indexes appends each hosted shard's index for a checkpoint whose base is the durable LSN
// durable, with no runs: its owner, and its replay LSN, the LSN before its oldest round a restart
// still needs or before a blob of it in flight, else durable. The memtable that applied an entry
// starts no earlier, since a round's entries are compacted only once that memtable is durable.
// That holds only if Indexes is called in the handler that captures the runs it is checkpointed
// with, whose base is at least the last Compact's index, and if the checkpoint carries each
// shard's base, the one it was started on included.
func (h *Host) Indexes(dst []disklog.CheckpointIndex, durable uint64) []disklog.CheckpointIndex {
	n := len(dst)
	for k := range h.shards {
		hd := &h.shards[k]
		lsn := durable
		if len(hd.refs) > 0 {
			lsn = min(lsn, hd.refs[0].LSN-1)
		}
		dst = append(dst, disklog.CheckpointIndex{Owner: hd.owner, ReplayLSN: lsn})
	}
	for _, f := range h.flights[h.head:] {
		if k, ok := h.at[f.owner]; ok {
			dst[n+k].ReplayLSN = min(dst[n+k].ReplayLSN, f.after)
		}
	}
	return dst
}

// RemoteIndexes appends each shard on the round driver's index for a checkpoint whose base is the
// durable LSN durable: its owner and replica, the replay LSN it reported, at most durable, and the runs the
// newest manifest names; a due checkpoint's flush adds those it writes. The replay LSN reported
// before the shard marks its tree is at or before every round its mark's base leaves live. The
// checkpoint leaves out the shards removed before it, and checkpoints run one at a time, so every
// manifest past durable leaves them out.
func (h *Host) RemoteIndexes(dst []disklog.CheckpointIndex, durable uint64) []disklog.CheckpointIndex {
	for i := range h.unnamed {
		if h.unnamed[i].after == 0 {
			h.unnamed[i].after, h.leaving = durable, true
		}
	}
	for _, rs := range h.remote {
		dst = append(dst, disklog.CheckpointIndex{Owner: rs.owner, Replica: rs.rep, ReplayLSN: min(rs.replay, durable), Runs: rs.runs, Bounds: rs.bounds})
	}
	return dst
}

// Rewrite writes again, in a round of its own, what each shard whose oldest round a restart still
// needs lies at or before lsn keeps there, as a checkpoint does with lsn its last manifest's base,
// so no shard holds the replay floor back for good: the hard state and removal flag of an idle
// shard, whose oldest round is the one holding its newest hard state and holds no entry past the
// compaction; and every entry past the base, with the newest hard state, of a shard whose oldest
// round holds an entry past its commit, as a follower's whose leader is gone does, once its storage
// took every round reported durable. That full rewrite goes out a few blobs at a time, as Retry
// finds buffers for them, the shard's own rounds waiting behind it, and once durable makes every
// round before it dead. A shard with a round in flight or waiting for buffers is left to that
// round, and one with entries committed but not yet compacted to its compaction.
func (h *Host) Rewrite(ctx *core.Context, lsn uint64, out *disklog.Output) error {
	for k := range h.shards {
		hd := &h.shards[k]
		if hd.rw != nil || hd.inflight > 0 || hd.wait != 0 {
			continue
		}
		r, ok := hd.rewrite(lsn)
		if hd.rw != nil {
			if err := h.step(ctx, hd, out); err != nil {
				return err
			}
		}
		if ok {
			if _, err := h.submit(ctx, hd, 0, &r, out); err != nil {
				return err
			}
		}
	}
	return nil
}

// rewrite decides what of hd, with no round of its own in flight or waiting and no full rewrite
// going out, a checkpoint whose last manifest's base is lsn writes again: a round of its hard
// state, returned, or a full rewrite, which it starts in hd.rw.
func (hd *hosted) rewrite(lsn uint64) (Round, bool) {
	if hd.newest < 0 || hd.refs[0].LSN > lsn {
		return Round{}, false
	}
	newest := &hd.rounds[hd.newest]
	switch {
	case hd.rounds[0].last > newest.state.Commit:
		if hd.st.took == hd.done {
			hd.rw = newRewriting(hd.st, newest)
		}
	case hd.newest == 0 && hd.rounds[0].last <= hd.compacted:
		return Round{HardState: newest.state, Removed: newest.removed, Deleted: newest.deleted}, true
	}
	return Round{}, false
}

// Reusable reports whether owner may be given to a new shard: no shard holds it, hosted or on the
// round driver, no blob of it is in flight, no blob of a removed shard of it lies past the log's
// replay floor, and no manifest a restart may load names a removed shard of it on the round
// driver, which a restart would restore under owner and mail the new shard's blobs.
func (h *Host) Reusable(owner uint32) bool {
	if _, ok := h.at[owner]; ok {
		return false
	}
	if _, ok := h.remoteAt[owner]; ok {
		return false
	}
	for _, f := range h.flights[h.head:] {
		if f.owner == owner {
			return false
		}
	}
	var floor uint64
	if h.Log != nil {
		floor = h.Log.ReplayFloor()
	}
	ok := h.LeftOut(owner)
	keep := 0
	for _, r := range h.retired {
		if r.lsn > floor {
			ok = ok && r.owner != owner
			h.retired[keep] = r
			keep++
		}
	}
	h.retired = h.retired[:keep]
	return ok
}

// LeftOut reports whether every manifest a restart may load leaves out the removed shards of owner
// on the round driver, so a restart restores none of them.
func (h *Host) LeftOut(owner uint32) bool {
	var named uint64
	if h.Log != nil {
		named = h.Log.NamedLSN()
	}
	keep, ok := 0, true
	for _, u := range h.unnamed {
		if u.after == 0 || named <= u.after {
			ok = ok && u.owner != owner
			h.unnamed[keep] = u
			keep++
		}
	}
	h.unnamed = h.unnamed[:keep]
	return ok
}

func (h *Host) retire(owner uint32, lsn uint64) {
	for i := range h.retired {
		if h.retired[i].owner == owner {
			h.retired[i].lsn = max(h.retired[i].lsn, lsn)
			return
		}
	}
	h.retired = append(h.retired, retiredOwner{owner, lsn})
}
