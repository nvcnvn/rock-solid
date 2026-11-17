package loop

import (
	"fmt"
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
)

// Mail is what a disk's owner sends one of its shards, off the data path; Data is the shard's to
// keep.
type Mail struct {
	Shard ShardID
	Kind  MailKind
	Ref   disklog.BlobRef
	Blob  disklog.BlobKind
	Data  []byte
}

// ShardID names a shard on its loop.
type ShardID uint32

// LogMachine is an owner loop's machine that calls into its logs besides the round driver, as a
// chunk store's reads, a checkpoint or the cleaner do: it gets every event of each log the loop
// owns once the round driver has counted its own tickets, and ignores the tickets it did not
// submit.
type LogMachine interface {
	OnLog(ctx *core.Context, lg *Log, ev *disklog.Event, out []core.Action) []core.Action
}

// Log is a disk's log as a node's loops share it: the loop that owns it and each loop's hand-off to it.
type Log struct {
	owner    *Loop
	disk     uint32
	log      *disklog.Log
	ios      [2]core.IOID
	lm       LogMachine
	handoffs []*handoff
	out      disklog.Output
	// batches are the batches submitted and not yet durable, in ticket order; those before taken
	// have their first record in a round, those before head are durable.
	batches     []batch
	head, taken int
	notify      []*handoff
	// queued are the hand-offs whose batch waits, in arrival order, for room in the owner's pool.
	queued []*handoff
	copies []core.IOBuf
	dead   []disklog.BlobRef
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

	// mu guards mail, from the owner to h's loop, and dead, from h's loop to the owner.
	mu       sync.Mutex
	mail     []Mail
	dead     []disklog.BlobRef
	hasMail  atomic.Bool
	unmailed bool
	got      []Mail
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

type shard struct {
	s      Shard
	h      *handoff
	marked bool
}

// AttachLog makes l the owner of log, a formatted or recovered log on its disk ring whose IO IDs
// run from firstIO up to endIO, which loops of IDs 0 to loops-1 hand records to. The log gets the
// disk completions of its IO IDs and the trim timer; the loop's machine gets the rest. It runs
// before the loops start. The logs of a loop share the trim timer, and every log takes each firing
// and arms it again, so a log's paced TRIM may wait for the time another log armed it to.
func (l *Loop) AttachLog(log *disklog.Log, firstIO, endIO core.IOID, loops int) *Log {
	for _, o := range l.logs {
		if firstIO < o.ios[1] && o.ios[0] < endIO {
			panic(fmt.Sprintf("loop: log IO IDs %d to %d overlap another log's %d to %d", firstIO, endIO, o.ios[0], o.ios[1]))
		}
	}
	lg := &Log{owner: l, disk: uint32(len(l.logs)), log: log, ios: [2]core.IOID{firstIO, endIO}, handoffs: make([]*handoff, loops)}
	lg.lm, _ = l.m.(LogMachine)
	l.logs = append(l.logs, lg)
	return lg
}

// logOf is the log whose IO IDs hold io, or nil.
func (l *Loop) logOf(io core.IOID) *Log {
	for _, lg := range l.logs {
		if io >= lg.ios[0] && io < lg.ios[1] {
			return lg
		}
	}
	return nil
}

// AddShard puts s on l, its records going to lg. When s is l's first shard on lg it makes their
// handoff, so it runs before l and lg's owner start; otherwise it runs on l.
func (l *Loop) AddShard(s Shard, lg *Log) ShardID {
	h := lg.handoffs[l.cfg.ID]
	if h == nil {
		h = &handoff{lg: lg, from: l, pool: l.ctx.Pool, idx: len(l.handoffs), slots: make([]slot, lg.log.CommitDepth()+1)}
		lg.handoffs[l.cfg.ID] = h
		l.handoffs = append(l.handoffs, h)
	}
	l.shards = append(l.shards, shard{s: s, h: h})
	return ShardID(len(l.shards) - 1)
}

// RemoveShard takes shard id off l for good, its ID never given again: mail for it is dropped,
// and its batches in flight are written with no Durable call, their Raft blobs reported dead. It
// runs on l.
func (l *Loop) RemoveShard(id ShardID) { l.shards[id].s = nil }

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
	limit := h.lg.log.MaxRecordBytes()
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

// sendable reports whether h may send a batch now: shards are marked, a slot is free, and the
// owner has taken h's last batch into a round.
func (h *handoff) sendable() bool {
	return len(h.marked) > 0 && h.sent-h.noticed < uint64(len(h.slots)) && h.taken.Load() == h.sent
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
// logs this loop owns start the rounds the pass's hand-offs filled.
func (l *Loop) endRounds() {
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
		lg.log.Release(&l.ctx, &lg.out)
		lg.drain()
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
		h.mu.Unlock()
		lg.bury(lg.dead)
	default:
		l.noticed(l.handoffs[ev.Timer])
	}
}

// Send hands m to its shard on the loop of ID to, which takes it at the end of the owner's pass
// if it is the owner, else on its next pass; it runs on lg's owner.
func (lg *Log) Send(to int, m Mail) {
	h := lg.handoffs[to]
	if h == nil {
		panic(fmt.Sprintf("loop: mail for loop %d, which has no shard on disk %d", to, lg.disk))
	}
	m.Data = append([]byte(nil), m.Data...)
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
	first := len(h.dead) == 0
	h.dead = append(h.dead, refs...)
	h.mu.Unlock()
	if first {
		l.post(h.lg.owner, core.Event{Kind: core.EvMail, IO: core.IOID(h.lg.disk), Timer: core.TimerID(l.cfg.ID)}, &h.unmailed)
	}
}

func (lg *Log) bury(refs []disklog.BlobRef) {
	if err := lg.log.RaftBlobsDead(refs); err != nil {
		panic(fmt.Sprintf("loop: a shard reported dead a Raft blob the log does not hold: %v", err))
	}
}

// takeQueued takes the queued batches in order until one finds the owner's pool full; that one
// and those behind it wait for a later pass, as the owner's buffers free when rounds complete.
func (lg *Log) takeQueued() {
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
	lg.log.OnEvent(&lg.owner.ctx, ev, &lg.out)
	lg.drain()
}

// drain executes the log's actions, counts its outcomes against the batches they hold, hands
// every event to the loop's machine if it is a LogMachine, and reports the batches taken and
// durable. An event the round driver does not consume, with no LogMachine to take it, is a broken
// invariant: something calls the log with nothing to hear its answer.
func (lg *Log) drain() {
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
	lg.out.Reset()
	lg.progress()
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
	start := lg.log.Unstarted()
	for ; lg.taken < len(lg.batches) && (lg.batches[lg.taken].end == 0 || lg.batches[lg.taken].first < start); lg.taken++ {
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
// hands its shards the owner's mail.
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
	if !h.hasMail.Swap(false) {
		return
	}
	h.mu.Lock()
	h.got, h.mail = h.mail, h.got[:0]
	h.mu.Unlock()
	for i := range h.got {
		m := &h.got[i]
		sh := l.shards[m.Shard].s
		if sh == nil {
			continue
		}
		l.out = sh.Mail(&l.ctx, m, l.out[:0])
		l.execute()
	}
	clear(h.got)
}

// buryRemoved reports dead the Raft blobs among refs, a removed shard's durable records, which it
// can no longer report itself.
func (l *Loop) buryRemoved(id ShardID, refs []disklog.BlobRef, raft []bool) {
	n := 0
	for i, ref := range refs {
		if raft[i] {
			refs[n] = ref
			n++
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
