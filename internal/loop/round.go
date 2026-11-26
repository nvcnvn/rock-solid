package loop

import (
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/trace"
)

// Shard is what a round collects records from: a MetaStore shard replica's Ready, or a store's
// chunk writes.
type Shard interface {
	// HasReady reports whether the shard has records to make durable.
	HasReady() bool
	// Ready appends the shard's records to items, their buffers from its loop's pool; only a shard
	// on the disk's owner loop may set Keep.
	Ready(items []disklog.Item) []disklog.Item
	// Durable reports the records of the shard's oldest Ready not yet reported durable at refs, in
	// Ready order, or never to be with err, and appends the actions that follow; Readies are
	// reported in the order they were made.
	Durable(ctx *core.Context, err error, refs []disklog.BlobRef, out []core.Action) []core.Action
	// Mail takes what the disk's owner sent the shard and appends the actions that follow.
	Mail(ctx *core.Context, m *Mail, out []core.Action) []core.Action
}

// MailKind tags a Mail.
type MailKind uint8

// Mail kinds.
const (
	// MailReplayed is a blob of the shard's that recovery replayed: its place, kind and bytes.
	MailReplayed MailKind = iota + 1
	// MailRewrite asks the shard to rewrite its runs with pieces in the segment of Ref, a victim of
	// the cleaner.
	MailRewrite
	// MailRecovered ends the replay: every MailReplayed of the restart came before it.
	MailRecovered
	// MailCheckpointed says the manifest of a due checkpoint is durable, Base the base of the due
	// manifest before it: the shard compacts to what the checkpoint recorded for it, and writes
	// again what it keeps at or before Base, so its rounds hold the replay floor back no longer.
	MailCheckpointed
	// MailShare gives the shard its Share of twice max_replay_bytes in retained entries.
	MailShare
	// MailFlush begins a due checkpoint, numbered Base: the shard marks its tree with its
	// storage's base, begins its flush and replies its first piece (ReplyPiece).
	MailFlush
	// MailPiece says the shard's last piece is durable at Ref: it replies its next.
	MailPiece
	// MailFlushFailed says the checkpoint failed: the shard's flush fails, and the owner reports
	// dead the pieces of it that no run holds.
	MailFlushFailed
	// MailRun is a run of the shard's tree that the manifest a restart loaded names, its root at
	// Ref and its pieces in order, root last, those past Bound's dead ones if a merge bounds it;
	// every one comes before the restart's first MailReplayed, and the shard restores its
	// storage's base from them before that.
	MailRun
)

// Mail is what a disk's owner sends one of its shards, off the data path, Owner the shard's owner
// key on mail that asks it for a piece; Data and Pieces are the shard's to keep.
type Mail struct {
	Shard  ShardID
	Kind   MailKind
	Owner  uint32
	Ref    disklog.BlobRef
	Blob   disklog.BlobKind
	Data   []byte
	Pieces [][]byte
	Base   uint64
	Share  uint64
	Bound  *disklog.MergeBound
}

// ShardID names a shard on its loop.
type ShardID uint32

// ReplyKind tags a Reply.
type ReplyKind uint8

// Reply kinds.
const (
	// ReplyReplay says a restart replays the shard after LSN: none of its blobs at or before it is
	// needed.
	ReplyReplay ReplyKind = iota + 1
	// ReplyRemoved says the shard is off its loop for good with every Raft blob of it reported
	// dead, the newest at LSN, 0 for none.
	ReplyRemoved
	// ReplyFull asks for a due checkpoint: the shard retains its share and applied a commit no
	// checkpoint compacted, or its tree installed a merge step no manifest names.
	ReplyFull
	// ReplyPiece is the next piece of the shard's flush for checkpoint LSN, Data, Root marking a
	// run's last; with no Data the flush has written all it has.
	ReplyPiece
	// ReplyRuns names the runs, Refs, with merge Bounds, that the shard's flush for checkpoint LSN
	// carries, before its first ReplyPiece: the manifest names them and the roots its pieces write.
	ReplyRuns
	// ReplyDead is the pieces, Refs, that the shard's tree no longer names, once the manifest that
	// leaves them out is durable.
	ReplyDead
	// ReplyCosts is the runs of the shard's tree as the cleaner weighs them, Costs, whenever they
	// change: the owner's cleaner reads a victim's cost from the last it heard, and mails the
	// shard a MailRewrite for a victim holding a piece of one.
	ReplyCosts
)

// Reply is what a shard tells its disk's owner of its blobs, which carry Owner, off the data path;
// Data, Refs, Bounds and Costs are the owner's to keep.
type Reply struct {
	Kind   ReplyKind
	Owner  uint32
	LSN    uint64
	Data   []byte
	Root   bool
	Refs   []disklog.BlobRef
	Bounds []disklog.MergeBound
	Costs  []disklog.RunCost
}

// ReplyMachine is an owner loop's machine that hears its shards' replies (Loop.Reply), after the
// dead blobs each reported before; it only records them, so it takes no actions, and acts on them
// at the nil event a LogMachine gets next.
type ReplyMachine interface {
	OnReply(lg *Log, r Reply)
}

// LogMachine is an owner loop's machine that calls into its logs besides the round driver, as a
// chunk store's reads, a checkpoint or the cleaner do: it gets every event of each log the loop
// owns once the round driver has counted its own tickets, and ignores the tickets it did not
// submit; after each disk completion or trim timer a log takes, after the events of any other
// call into it, and by the end of a pass in which a shard replied, it gets a nil ev, as a machine
// that drives its log itself looks at it after every call.
type LogMachine interface {
	OnLog(ctx *core.Context, lg *Log, ev *disklog.Event, out []core.Action) []core.Action
}

// Log is a disk's log as a node's loops share it: the loop that owns it and each loop's hand-off
// to it. Only the owner reads log; the other loops read the commit depth and record cap the log is
// configured with, which every log it serves shares.
type Log struct {
	owner     *Loop
	disk      uint32
	log       *disklog.Log
	depth     int
	maxRecord uint64
	ios       [2]core.IOID
	lm        LogMachine
	rm        ReplyMachine
	handoffs  []*handoff
	out       disklog.Output
	// batches are the batches submitted and not yet durable, in ticket order; those before taken
	// have their first record in a round, those before head are durable.
	batches     []batch
	head, taken int
	notify      []*handoff
	// queued are the hand-offs whose batch waits, in arrival order, for room in the owner's pool.
	queued  []*handoff
	copies  []core.IOBuf
	dead    []disklog.BlobRef
	replies []Reply
	replied bool
	// unburied are the Raft blobs reported dead while no log is served, which the next log served,
	// the one recovery replayed them into, takes.
	unburied []disklog.BlobRef
}

// A handoff carries one loop's batches of records for one disk to its owner. The loop sends a
// batch only once the owner has taken its last into a round, its first record in one, so the
// owner reads items between the inbox event and its next taken count, and the loop writes them
// only after. A batch the round's cap splits still lets the next one share the following round.
type handoff struct {
	lg   *Log
	from *Loop
	pool core.BufPool
	idx  int

	items         []disklog.Item
	marked        []ShardID
	slots         []slot
	sent, noticed uint64
	unposted      bool

	recv, lsn                  uint64
	notify, unnoticed, starved bool

	taken, durable atomic.Uint64
	waiting        atomic.Bool

	// mu guards mail, from the owner to h's loop, and dead and replies, from h's loop to the
	// owner.
	mu       sync.Mutex
	mail     []Mail
	dead     []disklog.BlobRef
	replies  []Reply
	hasMail  atomic.Bool
	unmailed bool
	got      []Mail
	// gone is h's removed shards whose ReplyRemoved waits for their batches in flight.
	gone []ShardID
}

// A slot is a batch in flight: its shards, where each one's records end, and the producer's
// buffers the owner copied, and which records are Raft blobs; and the outcome and the records' places, which the owner writes
// before it counts the batch durable.
type slot struct {
	shards []ShardID
	ends   []int
	bufs   []core.IOBuf
	raft   []bool
	err    error
	refs   []disklog.BlobRef
}

type batch struct {
	h          *handoff
	seq        uint64
	first, end disklog.Ticket
	left       int
	err        error
}

// A shard taken off its loop keeps gone, one past the last batch that may hold its records,
// newest, the LSN of the newest Raft blob of them, and reply, its ReplyRemoved once given.
type shard struct {
	s      Shard
	h      *handoff
	marked bool
	gone   uint64
	newest uint64
	reply  Reply
}

// AttachLog makes l the owner of log, a formatted or recovered log of configuration cfg on its
// disk ring whose IO IDs run from firstIO up to endIO, which loops of IDs 0 to loops-1 hand records
// to; log is nil while the disk recovers, until Serve. The log gets the disk completions of its IO IDs and the trim
// timer; the loop's machine gets the rest, and every completion while there is no log. It runs
// before the loops start. The logs of a loop share the trim timer, and every log takes each firing
// and arms it again, so a log's paced TRIM may wait for the time another log armed it to.
func (l *Loop) AttachLog(log *disklog.Log, cfg disklog.Config, firstIO, endIO core.IOID, loops int) *Log {
	for _, o := range l.logs {
		if firstIO < o.ios[1] && o.ios[0] < endIO {
			panic(fmt.Sprintf("loop: log IO IDs %d to %d overlap another log's %d to %d", firstIO, endIO, o.ios[0], o.ios[1]))
		}
	}
	lg := &Log{owner: l, disk: uint32(len(l.logs)), depth: cfg.CommitDepth, maxRecord: cfg.MaxRecordBytes, ios: [2]core.IOID{firstIO, endIO}, handoffs: make([]*handoff, loops)}
	lg.lm, _ = l.m.(LogMachine)
	lg.rm, _ = l.m.(ReplyMachine)
	l.logs = append(l.logs, lg)
	lg.Serve(log)
	return lg
}

// Serve makes log, or nil while the disk recovers again, the disk's log from now on, as the
// machine that recovers it says; it runs on lg's owner, with no batch in flight. While no log is
// served, the owner's marked shards stay marked, other loops' batches wait in its queue, and the
// Raft blobs reported dead wait for the log recovery replayed them into.
func (lg *Log) Serve(log *disklog.Log) {
	if len(lg.batches) > 0 {
		panic(fmt.Sprintf("loop: disk %d's log changed with batches in flight", lg.disk))
	}
	if log != nil && (log.CommitDepth() != lg.depth || log.MaxRecordBytes() != lg.maxRecord) {
		panic(fmt.Sprintf("loop: disk %d served a log of commit depth %d and record cap %d, attached with %d and %d", lg.disk, log.CommitDepth(), log.MaxRecordBytes(), lg.depth, lg.maxRecord))
	}
	lg.log = log
	if log != nil && len(lg.unburied) > 0 {
		lg.bury(lg.unburied)
		lg.unburied = lg.unburied[:0]
	}
}

// Log is the disk's log, nil while it recovers.
func (lg *Log) Log() *disklog.Log { return lg.log }

// logOf is the served log whose IO IDs hold io, or nil.
func (l *Loop) logOf(io core.IOID) *Log {
	for _, lg := range l.logs {
		if lg.log != nil && io >= lg.ios[0] && io < lg.ios[1] {
			return lg
		}
	}
	return nil
}

// AddShard puts s on l, its records going to lg. When s is l's first shard on lg and l is not lg's
// owner it makes their handoff, which the owner reads, so it runs before l and the owner start;
// otherwise it runs on l. Mail that came for
// the ID it returns before it, which l held, reaches s by the end of the pass.
func (l *Loop) AddShard(s Shard, lg *Log) ShardID {
	h := lg.handoffs[l.cfg.ID]
	if h == nil {
		h = &handoff{lg: lg, from: l, pool: l.ctx.Pool, idx: len(l.handoffs), slots: make([]slot, lg.depth+1)}
		lg.handoffs[l.cfg.ID] = h
		l.handoffs = append(l.handoffs, h)
	}
	l.shards = append(l.shards, shard{s: s, h: h})
	l.added = len(l.held) > 0
	return ShardID(len(l.shards) - 1)
}

// RemoveShard takes shard id off l for good, its ID never given again: mail for it is dropped, a
// checkpoint's ask for its next piece answered with none, and its batches in flight are written
// with no Durable call, their Raft blobs reported dead. It runs on l.
func (l *Loop) RemoveShard(id ShardID) {
	sh := &l.shards[id]
	sh.s, sh.gone = nil, sh.h.sent
}

// Mark asks the next round to collect shard id; a round visits only the shards marked since the last.
func (l *Loop) Mark(id ShardID) {
	sh := &l.shards[id]
	if !sh.marked {
		sh.marked = true
		sh.h.marked = append(sh.h.marked, id)
	}
}

func itemBytes(e *disklog.Entry) uint64 {
	if e.HasBlocks() {
		return uint64(e.Count) * core.BlockSize
	}
	return e.Length
}

// collect gathers the Ready of h's marked shards in the order they were marked into h.items and s,
// until the batch reaches the log's max_group_commit_bytes; the rest stay marked, first in line for
// the next round.
func (l *Loop) collect(h *handoff, s *slot) {
	h.items = h.items[:0]
	limit := h.lg.maxRecord
	var bytes uint64
	n := 0
	for n < len(h.marked) && bytes < limit {
		id := h.marked[n]
		n++
		sh := &l.shards[id]
		sh.marked = false
		if sh.s == nil || !sh.s.HasReady() {
			continue
		}
		from := len(h.items)
		h.items = sh.s.Ready(h.items)
		if len(h.items) == from {
			continue
		}
		for i := from; i < len(h.items); i++ {
			it := &h.items[i]
			bytes += itemBytes(&it.Entry)
			s.raft = append(s.raft, it.Entry.IsRaft())
			if h.lg.owner == l || it.Buf == core.NoBuf {
				continue
			}
			if it.Keep {
				panic(fmt.Sprintf("loop: shard %d kept the buffer of a record for another loop's disk", id))
			}
			s.bufs = append(s.bufs, it.Buf)
		}
		s.shards, s.ends = append(s.shards, id), append(s.ends, len(h.items))
	}
	h.marked = h.marked[:copy(h.marked, h.marked[n:])]
}

// sendable reports whether h may send a batch now: shards are marked, a slot is free, the owner
// has taken h's last batch into a round, and, on the owner, its disk serves a log.
func (h *handoff) sendable() bool {
	return len(h.marked) > 0 && h.sent-h.noticed < uint64(len(h.slots)) && h.taken.Load() == h.sent && (h.from != h.lg.owner || h.lg.log != nil)
}

// wait asks the owner to wake h's loop when it takes h's last batch into a round, if shards wait
// for that, and reports whether it took it meanwhile, as the owner wakes only a loop that asked.
func (h *handoff) wait() bool {
	if len(h.marked) == 0 || h.taken.Load() == h.sent {
		return false
	}
	h.waiting.Store(true)
	return h.sendable()
}

// endRounds runs at the end of a pass: the logs this loop owns take the batches waiting for room
// in its pool, before its own shards' Readies take that room, each disk whose owner took this
// loop's last batch into a round gets one with what the shards marked since have ready, and the
// logs this loop owns, held since the pass began, start the rounds the pass filled: its machine's
// own records share them with the hand-offs.
func (l *Loop) endRounds() {
	l.unhold()
	for _, lg := range l.logs {
		lg.takeQueued()
	}
	for _, h := range l.handoffs {
		if h.unposted {
			l.post(h.lg.owner, core.Event{Kind: core.EvHandoff, IO: core.IOID(h.lg.disk), Timer: core.TimerID(l.cfg.ID)}, &h.unposted)
		}
		if h.unmailed {
			l.post(h.lg.owner, core.Event{Kind: core.EvMail, IO: core.IOID(h.lg.disk), Timer: core.TimerID(l.cfg.ID)}, &h.unmailed)
		}
		if h.sendable() || h.wait() {
			l.send(h)
		}
	}
	for _, lg := range l.logs {
		if l.unposted > 0 {
			for _, h := range lg.handoffs {
				if h != nil && h.unnoticed {
					l.post(h.from, core.Event{Kind: core.EvNotice, Timer: core.TimerID(h.idx)}, &h.unnoticed)
				}
			}
		}
		if lg.log != nil {
			lg.log.Release(&l.ctx, &lg.out)
		}
		lg.drain(false)
	}
}

// send hands h's owner a batch of what h's marked shards have ready.
func (l *Loop) send(h *handoff) {
	s := &h.slots[h.sent%uint64(len(h.slots))]
	l.collect(h, s)
	if len(h.items) == 0 {
		return
	}
	h.sent++
	owner := h.lg.owner
	l.record(trace.Record{Name: trace.OutHandoff, Fields: [trace.MaxFields]uint64{uint64(owner.cfg.ID), uint64(h.lg.disk), uint64(len(h.items)), h.sent}})
	if owner == l {
		h.lg.take(h)
		return
	}
	l.post(owner, core.Event{Kind: core.EvHandoff, IO: core.IOID(h.lg.disk), Timer: core.TimerID(l.cfg.ID)}, &h.unposted)
}

// post puts ev in to's inbox; a full inbox sets *refused, and the caller posts again at the end of
// each pass until it clears.
func (l *Loop) post(to *Loop, ev core.Event, refused *bool) {
	ok := to.inbox.Post(&ev)
	if ok == !*refused {
		return
	}
	*refused = !ok
	if ok {
		l.unposted--
	} else {
		l.unposted++
	}
}

// handoff handles a hand-off, notice or mail from l's inbox.
func (l *Loop) handoff(ev *core.Event) {
	switch ev.Kind {
	case core.EvHandoff:
		lg := l.logs[ev.IO]
		lg.queued = append(lg.queued, lg.handoffs[ev.Timer])
		lg.takeQueued()
	case core.EvMail:
		lg := l.logs[ev.IO]
		h := lg.handoffs[ev.Timer]
		h.mu.Lock()
		lg.dead, h.dead = h.dead, lg.dead[:0]
		lg.replies, h.replies = h.replies, lg.replies[:0]
		h.mu.Unlock()
		lg.bury(lg.dead)
		for _, r := range lg.replies {
			lg.reply(r)
		}
	default:
		l.noticed(l.handoffs[ev.Timer])
	}
}

// Send hands m to its shard on the loop of ID to, which takes it at the end of the owner's pass
// if it is the owner, else on its next pass, or holds it until it adds the shard; it runs on lg's
// owner, and m.Data becomes the shard's, the caller keeping no hold on it. Mail to any loop with no
// shard on lg, the owner included, is a broken invariant of the caller.
func (lg *Log) Send(to int, m Mail) {
	h := lg.handoffs[to]
	if h == nil {
		panic(fmt.Sprintf("loop: mail for loop %d, which has no shard on disk %d", to, lg.disk))
	}
	h.mu.Lock()
	h.mail = append(h.mail, m)
	h.mu.Unlock()
	h.hasMail.Store(true)
	lg.wake(h)
}

// Dead reports Raft blobs of shard id it no longer needs to its disk's owner; every Raft blob
// comes here once, so the calls arrive at the write rate.
func (l *Loop) Dead(id ShardID, refs []disklog.BlobRef) {
	h := l.shards[id].h
	if h.lg.owner == l {
		h.lg.bury(refs)
		return
	}
	h.mu.Lock()
	first := len(h.dead) == 0 && len(h.replies) == 0
	h.dead = append(h.dead, refs...)
	h.mu.Unlock()
	if first {
		l.post(h.lg.owner, core.Event{Kind: core.EvMail, IO: core.IOID(h.lg.disk), Timer: core.TimerID(l.cfg.ID)}, &h.unmailed)
	}
}

// Reply tells shard id's disk owner r, after the dead blobs id reported before it. A ReplyRemoved
// follows RemoveShard and waits until the batches id had in flight are written, its LSN raised
// to the newest Raft blob among them. A ReplyReplay comes only while id is on its loop, and nothing
// after its ReplyRemoved.
func (l *Loop) Reply(id ShardID, r Reply) {
	sh := &l.shards[id]
	if sh.reply.Kind == ReplyRemoved || (sh.s == nil) != (r.Kind == ReplyRemoved) {
		panic(fmt.Sprintf("loop: shard %d replied %+v, on its loop %v, removed reply given %v", id, r, sh.s != nil, sh.reply.Kind == ReplyRemoved))
	}
	if r.Kind != ReplyRemoved {
		l.reply(sh.h, r)
		return
	}
	sh.reply = r
	sh.h.gone = append(sh.h.gone, id)
	l.replyGone(sh.h)
}

func (l *Loop) reply(h *handoff, r Reply) {
	if h.lg.owner == l {
		h.lg.reply(r)
		return
	}
	h.mu.Lock()
	first := len(h.dead) == 0 && len(h.replies) == 0
	h.replies = append(h.replies, r)
	h.mu.Unlock()
	if first {
		l.post(h.lg.owner, core.Event{Kind: core.EvMail, IO: core.IOID(h.lg.disk), Timer: core.TimerID(l.cfg.ID)}, &h.unmailed)
	}
}

// replyGone sends the ReplyRemoved of each of h's removed shards whose batches are all written.
func (l *Loop) replyGone(h *handoff) {
	keep := 0
	for _, id := range h.gone {
		sh := &l.shards[id]
		if h.noticed < sh.gone {
			h.gone[keep] = id
			keep++
			continue
		}
		sh.reply.LSN = max(sh.reply.LSN, sh.newest)
		l.reply(h, sh.reply)
	}
	h.gone = h.gone[:keep]
}

func (lg *Log) reply(r Reply) {
	if lg.rm == nil {
		panic(fmt.Sprintf("loop: a shard replied %+v to disk %d's owner, which is not a ReplyMachine", r, lg.disk))
	}
	lg.rm.OnReply(lg, r)
	lg.replied = true
}

func (lg *Log) bury(refs []disklog.BlobRef) {
	if lg.log == nil {
		lg.unburied = append(lg.unburied, refs...)
		return
	}
	if err := lg.log.RaftBlobsDead(refs); err != nil {
		panic(fmt.Sprintf("loop: a shard reported dead a Raft blob the log does not hold: %v", err))
	}
}

// takeQueued takes the queued batches in order until one finds the owner's pool full; that one
// and those behind it wait for a later pass, as the owner's buffers free when rounds complete, or
// for a log to serve.
func (lg *Log) takeQueued() {
	if lg.log == nil {
		return
	}
	n := 0
	for n < len(lg.queued) && lg.take(lg.queued[n]) {
		n++
	}
	if n < len(lg.queued) && !lg.queued[n].starved {
		h := lg.queued[n]
		h.starved = true
		lg.owner.record(trace.Record{Name: trace.EvPoolWait, Fields: [trace.MaxFields]uint64{uint64(h.from.cfg.ID), uint64(lg.disk), uint64(len(h.items)), h.recv + 1}})
	}
	lg.queued = lg.queued[:copy(lg.queued, lg.queued[n:])]
}

// take submits h's batch to the log, which holds its rounds until the end of the pass, and
// reports false, having submitted nothing, when the owner's pool cannot hold its copy. A record
// from another loop is copied into the owner's pool, since a buffer goes back only to the pool it
// came from; that loop frees its own once the batch is durable.
func (lg *Log) take(h *handoff) bool {
	l := lg.owner
	ctx := &l.ctx
	if !lg.copyIn(h) {
		return false
	}
	h.starved = false
	lg.log.Hold()
	b := batch{h: h, seq: h.recv, first: ^disklog.Ticket(0)}
	h.recv++
	l.record(trace.Record{Name: trace.EvTake, Fields: [trace.MaxFields]uint64{uint64(h.from.cfg.ID), uint64(lg.disk), uint64(len(h.items)), h.recv}})
	s := &h.slots[b.seq%uint64(len(h.slots))]
	s.refs = s.refs[:0]
	c := 0
	for i := range h.items {
		it := h.items[i]
		if h.from != l && it.Buf != core.NoBuf {
			it.Buf = lg.copies[c]
			c++
		}
		t, err := lg.log.Submit(ctx, disklog.Hot, it, &lg.out)
		if err != nil {
			if it.Buf != core.NoBuf && !it.Keep {
				ctx.Pool.Free(it.Buf)
			}
			b.err = err
			continue
		}
		b.first, b.end = min(b.first, t), t+1
		b.left++
		s.refs = append(s.refs, disklog.BlobRef{})
	}
	lg.batches = append(lg.batches, b)
	return true
}

// copyIn copies the records of a batch from another loop into lg.copies, from the owner's pool,
// and reports false, holding none, when the pool cannot hold them all.
func (lg *Log) copyIn(h *handoff) bool {
	if h.from == lg.owner {
		return true
	}
	pool := lg.owner.ctx.Pool
	lg.copies = lg.copies[:0]
	for i := range h.items {
		it := &h.items[i]
		if it.Buf == core.NoBuf {
			continue
		}
		n := itemBytes(&it.Entry)
		buf, err := pool.Alloc(int(n))
		if err != nil {
			for _, b := range lg.copies {
				pool.Free(b)
			}
			return false
		}
		copy(pool.Bytes(buf), h.pool.Bytes(it.Buf)[:n])
		lg.copies = append(lg.copies, buf)
	}
	return true
}

// onDisk hands the log a disk completion or its trim timer.
func (lg *Log) onDisk(ev *core.Event) {
	l := lg.owner
	lg.log.OnEvent(&l.ctx, ev, &lg.out)
	lg.drain(true)
}

// drain executes the log's actions, counts its outcomes against the batches they hold, hands
// every event to the loop's machine if it is a LogMachine, and reports the batches taken and
// durable; then it hands a LogMachine a nil event after a completion or trim timer, any event, or
// a shard's reply, and delivers the mail it sent then, again while shards on the owner reply.
// An event the round driver does not consume, with no LogMachine to take it, is a broken
// invariant: something calls the log with nothing to hear its answer.
func (lg *Log) drain(completion bool) {
	l := lg.owner
	l.out = append(l.out[:0], lg.out.Actions...)
	l.execute()
	for i := range lg.out.Events {
		ev := &lg.out.Events[i]
		if !lg.count(ev) && lg.lm == nil {
			panic(fmt.Sprintf("loop: disk %d's log reported event kind %d to an owner that is not a LogMachine", lg.disk, ev.Kind))
		}
		if lg.lm != nil {
			l.out = lg.lm.OnLog(&l.ctx, lg, ev, l.out[:0])
			l.execute()
		}
	}
	completion = completion || len(lg.out.Events) > 0
	lg.out.Reset()
	lg.progress()
	for lg.lm != nil && (completion || lg.replied) {
		completion, lg.replied = false, false
		l.out = lg.lm.OnLog(&l.ctx, lg, nil, l.out[:0])
		l.execute()
		lg.progress()
	}
}

// count counts the outcome of ev against the batches holding its tickets, and reports whether
// they held every ticket ev names.
func (lg *Log) count(ev *disklog.Event) bool {
	var err error
	switch ev.Kind {
	case disklog.Durable:
	case disklog.Failed, disklog.InDoubt:
		err = ev.Err
	default:
		return false
	}
	all := true
	for k, t := range ev.Tickets {
		held := false
		for j := lg.head; j < len(lg.batches); j++ {
			if b := &lg.batches[j]; t >= b.first && t < b.end {
				b.left--
				if err != nil {
					b.err = err
				} else if b.err == nil {
					b.h.slots[b.seq%uint64(len(b.h.slots))].refs[t-b.first] = disklog.BlobRef{Segment: ev.Segment, Block: ev.Block, LSN: ev.LSN, Entry: uint16(k)}
				}
				held = true
				break
			}
		}
		all = all && (held || t == disklog.NoTicket)
	}
	return all
}

// progress publishes each batch now in a round as taken, and each batch whose records all
// resolved, in order, as durable, then wakes each loop told something once.
func (lg *Log) progress() {
	l := lg.owner
	for ; lg.taken < len(lg.batches) && (lg.batches[lg.taken].end == 0 || lg.batches[lg.taken].first < lg.log.Unstarted()); lg.taken++ {
		b := &lg.batches[lg.taken]
		b.h.taken.Store(b.seq + 1)
		if b.h.waiting.Swap(false) {
			lg.wake(b.h)
		}
	}
	for ; lg.head < lg.taken && lg.batches[lg.head].left == 0; lg.head++ {
		b := &lg.batches[lg.head]
		s := &b.h.slots[b.seq%uint64(len(b.h.slots))]
		b.h.lsn = lg.log.DurableLSN()
		s.err = b.err
		b.h.durable.Store(b.seq + 1)
		lg.wake(b.h)
	}
	if lg.head > 64 && lg.head > len(lg.batches)/2 || lg.head == len(lg.batches) {
		n := copy(lg.batches, lg.batches[lg.head:])
		lg.batches, lg.head, lg.taken = lg.batches[:n], 0, lg.taken-lg.head
	}
	for i := 0; i < len(lg.notify); i++ {
		h := lg.notify[i]
		h.notify = false
		l.record(trace.Record{Name: trace.OutNotice, Fields: [trace.MaxFields]uint64{uint64(h.from.cfg.ID), uint64(lg.disk), h.lsn, h.durable.Load()}})
		h.lsn = 0
		if h.from == l {
			l.noticed(h)
		} else {
			l.post(h.from, core.Event{Kind: core.EvNotice, Timer: core.TimerID(h.idx)}, &h.unnoticed)
		}
	}
	lg.notify = lg.notify[:0]
}

func (lg *Log) wake(h *handoff) {
	if !h.notify {
		h.notify = true
		lg.notify = append(lg.notify, h)
	}
}

// noticed tells the shards of h's newly durable batches the outcome and frees their buffers, then
// hands its shards the owner's mail, holding the mail for an ID not yet added.
func (l *Loop) noticed(h *handoff) {
	d := h.durable.Load()
	for ; h.noticed < d; h.noticed++ {
		s := &h.slots[h.noticed%uint64(len(h.slots))]
		from := 0
		for k, id := range s.shards {
			var refs []disklog.BlobRef
			if s.err == nil {
				refs = s.refs[from:s.ends[k]]
			}
			raft := s.raft[from:s.ends[k]]
			from = s.ends[k]
			if l.shards[id].s == nil {
				l.buryRemoved(id, refs, raft)
				continue
			}
			l.out = l.shards[id].s.Durable(&l.ctx, s.err, refs, l.out[:0])
			l.execute()
		}
		for _, b := range s.bufs {
			l.ctx.Pool.Free(b)
		}
		s.shards, s.ends, s.bufs, s.raft = s.shards[:0], s.ends[:0], s.bufs[:0], s.raft[:0]
	}
	if len(h.gone) > 0 {
		l.replyGone(h)
	}
	if !h.hasMail.Swap(false) {
		return
	}
	h.mu.Lock()
	h.got, h.mail = h.mail, h.got[:0]
	h.mu.Unlock()
	l.unhold()
	for i := range h.got {
		if m := &h.got[i]; int(m.Shard) < len(l.shards) {
			l.deliver(m)
		} else {
			l.hold(m)
		}
	}
	clear(h.got)
}

// deliver hands m to its shard. A removed shard's checkpoint waits on none of its pieces: mail
// asking it for one gets the reply that its flush has written all it has.
func (l *Loop) deliver(m *Mail) {
	sh := &l.shards[m.Shard]
	switch {
	case sh.s != nil:
		l.out = sh.s.Mail(&l.ctx, m, l.out[:0])
		l.execute()
	case m.Kind == MailFlush || m.Kind == MailPiece:
		l.reply(sh.h, Reply{Kind: ReplyPiece, Owner: m.Owner, LSN: m.Base})
	}
}

// hold keeps m until its shard is added. A share or checkpoint mail supersedes the one before it,
// so l holds at most one of each per ID besides a restart's replay.
func (l *Loop) hold(m *Mail) {
	if m.Kind == MailShare || m.Kind == MailCheckpointed {
		l.held = slices.DeleteFunc(l.held, func(o Mail) bool { return o.Shard == m.Shard && o.Kind == m.Kind })
	}
	l.held = append(l.held, *m)
}

// unhold hands each shard added since l held its mail that mail, in arrival order.
func (l *Loop) unhold() {
	if !l.added {
		return
	}
	l.added = false
	keep := 0
	for i := range l.held {
		if int(l.held[i].Shard) < len(l.shards) {
			l.deliver(&l.held[i])
			continue
		}
		l.held[keep] = l.held[i]
		keep++
	}
	clear(l.held[keep:])
	l.held = l.held[:keep]
}

// buryRemoved reports dead the Raft blobs among refs, a removed shard's durable records, which it
// can no longer report itself.
func (l *Loop) buryRemoved(id ShardID, refs []disklog.BlobRef, raft []bool) {
	n := 0
	for i, ref := range refs {
		if raft[i] {
			refs[n] = ref
			n++
			l.shards[id].newest = max(l.shards[id].newest, ref.LSN)
		}
	}
	if n > 0 {
		l.Dead(id, refs[:n])
	}
}

// sendable reports whether one of l's hand-offs may send a batch now.
func (l *Loop) sendable() bool {
	for _, h := range l.handoffs {
		if h.sendable() {
			return true
		}
	}
	return false
}
