package chunkstore

import (
	"slices"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/trace"
)

// maxRecoveries is how many chunks a replica recovers at once across the PGs it leads: recovery's
// throttle.
const maxRecoveries = 4

// maxHeld is how many client writes a lead holds before it refuses a partial-block write to a chunk
// whose kept version it lacks, which waits for that chunk's recovery.
const maxHeld = 64

// recoverChunk is a chunk the lead's peering left some up member of its interval lacking: its kept
// version, the members to recover it to by their place in the interval's members, and whether a
// recovery of it is under way.
type recoverChunk struct {
	chunk   core.ChunkID
	kept    uint64
	targets uint8
	busy    bool
	// wait is set once a read of the chunk failed, until the next retry; failed is the members,
	// by place, a read failed at, which are tried last.
	wait   bool
	failed uint8
}

// recovery is a chunk being recovered: its image read from a source, by store request read or by
// network request rid from the loop at source, then its recovery write, store request req, ordered
// and sent like any write.
type recovery struct {
	pg       core.PGID
	interval uint32
	todo     int
	src      int
	read     uint64
	rid      core.RequestID
	source   core.NodeID
	req      uint64
}

// planRecovery lists, as the lead activates, each chunk its decision left a member lacking, from
// the missing lists it had the members persist and its own copy, which lacks the kept version where
// a merge on it waits, and starts tracking the blocks the interval's writes cover in each.
func (r *Replica) planRecovery(l *lead, pr *peering, e *PGEntry) {
	l.todo, l.cursor, l.listed, l.reported = l.todo[:0], 0, [maxMembers]int{}, false
	slot := func(disk uint32) int {
		return slices.IndexFunc(e.Members, func(m Member) bool { return m.Disk == disk })
	}
	for _, rp := range pr.replies {
		s := slot(rp.disk)
		if s < 0 || s >= maxMembers || !upIn(e, rp.disk, e.Interval) || r.plant.recoverSelfOnly && rp.disk != r.cfg.Disk {
			continue
		}
		for _, cv := range rp.list {
			l.todo = append(l.todo, recoverChunk{chunk: cv.Chunk, kept: cv.Version, targets: 1 << s})
		}
	}
	if s := slot(r.cfg.Disk); s >= 0 && s < maxMembers {
		for _, k := range pr.kept {
			if k.lacking {
				l.todo = append(l.todo, recoverChunk{chunk: k.chunk, kept: k.kept, targets: 1 << s})
			}
		}
	}
	slices.SortFunc(l.todo, func(a, b recoverChunk) int { return compareChunk(a.chunk, b.chunk) })
	n := 0
	for _, t := range l.todo {
		if n > 0 && l.todo[n-1].chunk == t.chunk {
			l.todo[n-1].targets |= t.targets
			continue
		}
		l.todo[n] = t
		n++
	}
	l.todo = l.todo[:n]
	for i := range l.todo {
		t := &l.todo[i]
		for s := range maxMembers {
			if t.targets&(1<<s) != 0 {
				l.listed[s]++
			}
		}
		r.st.Cover(t.chunk, l.interval)
	}
}

// degraded is how far the lead's PG is from whole: its members not up and those still lacking a
// chunk.
func (r *Replica) degraded(l *lead, e *PGEntry) int {
	n := 0
	for s, m := range e.Members {
		if m.Mark != MarkUp || s < maxMembers && l.listed[s] > 0 {
			n++
		}
	}
	return n
}

// next is the lead's first chunk to recover, from where the pass of recoverWork stands, that no
// recovery has under way, or -1.
func (l *lead) next() int {
	for j := max(l.cursor, l.scan); j < len(l.todo); j++ {
		switch t := &l.todo[j]; {
		case t.targets == 0:
			if j == l.cursor {
				l.cursor++
			}
		case !t.busy && !t.wait:
			return j
		}
	}
	return -1
}

// recoverWork starts recoveries while fewer than maxRecoveries run: each time the next chunk of
// the active lead whose PG is most degraded, the lower PG first among equals.
func (r *Replica) recoverWork(ctx *core.Context, out *ReplicaOutput) {
	for i := range r.leads {
		r.leads[i].scan = 0
	}
	for len(r.recs) < maxRecoveries && !r.plant.noRecovery {
		best, bestN, bestJ := -1, 0, -1
		for i := range r.leads {
			l := &r.leads[i]
			_, e := r.table.entry(l.pg)
			if !l.active || l.asking || e == nil || e.Interval != l.interval {
				continue
			}
			if j := l.next(); j >= 0 {
				if n := r.degraded(l, e); best < 0 || n > bestN {
					best, bestN, bestJ = i, n, j
				}
			}
		}
		if best < 0 {
			return
		}
		r.leads[best].scan = bestJ + 1
		r.startRecovery(ctx, best, bestJ, out)
	}
}

// startRecovery reads chunk j of lead i's list from a source, an up member of the interval that
// holds the kept version and lacks nothing of it: the primary itself if it can, even one that lists
// the chunk only for versions of its own its index never mapped, else the first such member, those
// a read already failed at last. A chunk no member keeps anything of needs no source, and one no member can be a source of
// is left listed for this interval; one whose sources the replica cannot reach yet is tried again
// at the next retry.
func (r *Replica) startRecovery(ctx *core.Context, i, j int, out *ReplicaOutput) {
	l := &r.leads[i]
	_, e := r.table.entry(l.pg)
	t := &l.todo[j]
	rc := recovery{pg: l.pg, interval: l.interval, todo: j}
	if t.kept == 0 {
		r.recoverEmpty(ctx, t, rc)
		return
	}
	src, clean := -1, false
	for s, m := range e.Members {
		if s >= maxMembers || m.Mark != MarkUp || t.targets&(1<<s) != 0 && (m.Disk != r.cfg.Disk || l.lacks(t.chunk) && !r.plant.recoverFromListed) {
			continue
		}
		clean = true
		if m.Disk != r.cfg.Disk && !r.reachable(m.Disk) {
			continue
		}
		if better := src < 0 || t.failed&(1<<src) != 0 && t.failed&(1<<s) == 0 || m.Disk == r.cfg.Disk && t.failed&(1<<s) == 0; better {
			src = s
		}
	}
	switch {
	case src < 0 && r.plant.clearNoSource:
		r.recoverEmpty(ctx, t, rc)
		return
	case !clean:
		t.targets = 0
		r.st.Uncover(t.chunk)
		return
	case src < 0:
		t.wait, r.waits = true, true
		return
	}
	t.busy, rc.src = true, src
	if r.plant.coverAtRead {
		r.st.freezeCover(t.chunk)
	}
	if d := e.Members[src].Disk; d == r.cfg.Disk {
		rc.read = r.newReq(storeReq{kind: reqRecoverRead, pg: l.pg, chunk: t.chunk})
		r.recs = append(r.recs, rc)
		r.st.Read(ctx, Request{ID: rc.read, Chunk: t.chunk, Len: core.ChunkSize, Recover: true}, &r.sout)
	} else {
		r.nextID++
		rc.rid, rc.source = core.RequestID(r.nextID), r.addr(d)
		r.recs = append(r.recs, rc)
		rd := Read{ID: rc.rid, PG: PGFields{Epoch: r.table.Epoch, PG: l.pg, Interval: l.interval}, Chunk: t.chunk, Len: core.ChunkSize, Recover: true}
		r.frame = AppendRead(r.frame[:0], &rd)
		a := core.Action{Kind: core.ActNetSend, Peer: rc.source}
		a.Msg.Len = uint16(copy(a.Msg.Body[:], r.frame))
		out.Actions = append(out.Actions, a)
	}
}

// recoverEmpty orders a recovery write of an image with every block a hole, which needs no source.
func (r *Replica) recoverEmpty(ctx *core.Context, t *recoverChunk, rc recovery) {
	buf, err := ctx.Pool.Alloc(core.BlockSize)
	if err != nil {
		t.wait, r.waits = true, true
		return
	}
	var m blockMap
	for w := range m.holes {
		m.holes[w] = ^uint64(0)
	}
	m.put(ctx.Pool.Bytes(buf))
	t.busy = true
	r.recs = append(r.recs, rc)
	r.orderRecovery(ctx, len(r.recs)-1, buf)
}

// recoveryRead takes the image recovery k read, in buf, or its failure: the chunk is tried again
// at the next retry.
func (r *Replica) recoveryRead(ctx *core.Context, k int, buf core.IOBuf, ok bool) {
	if !ok {
		freeBuf(ctx, buf)
		rc := r.recs[k]
		r.endRecovery(k, false)
		if i := r.lead(rc.pg); i >= 0 && r.leads[i].interval == rc.interval {
			t := &r.leads[i].todo[rc.todo]
			t.wait, t.failed, r.waits = true, t.failed|1<<rc.src, true
		}
		return
	}
	r.orderRecovery(ctx, k, buf)
}

// retryReads lets the chunks whose reads failed be tried again.
func (r *Replica) retryReads() {
	if !r.waits {
		return
	}
	r.waits = false
	for i := range r.leads {
		for j := range r.leads[i].todo {
			r.leads[i].todo[j].wait = false
		}
	}
}

// recoveryAsked serves a recovery read from the PG's primary in the PG's current interval, if this
// disk is a member of it: the chunk's live image as a recovery buffer, or StatusMissing while the
// chunk is on this disk's missing list. A disk formatted again at a member's loop answers no read
// for the member's old disk ID.
func (r *Replica) recoveryAsked(ctx *core.Context, ev *core.Event, rd *Read, out *ReplicaOutput) {
	_, e := r.table.entry(rd.PG.PG)
	ok := e != nil && r.lead(rd.PG.PG) < 0 && rd.PG.Interval == e.Interval && slices.ContainsFunc(e.Members, func(m Member) bool { return m.Disk == r.cfg.Disk })
	if ok {
		d, has := e.primary()
		ok = has && r.addr(d) == ev.Peer
	}
	if !ok {
		rp := r.refusal(rd.PG.PG)
		rp.Kind = KindReadReply
		r.answer(ctx, ev.Peer, rd.ID, &rp, core.NoBuf, out)
		return
	}
	if ix := r.st.Index(); ix == nil || listedIn(ix, r.st.cfg.Pool, rd.Chunk) != 0 {
		rp := Response{Kind: KindReadReply, Status: StatusMissing}
		r.answer(ctx, ev.Peer, rd.ID, &rp, core.NoBuf, out)
		return
	}
	id := r.newReq(storeReq{kind: reqRead, peer: ev.Peer, msgID: rd.ID, pg: rd.PG.PG, chunk: rd.Chunk})
	r.st.Read(ctx, Request{ID: id, Chunk: rd.Chunk, Len: core.ChunkSize, Recover: true}, &r.sout)
}

func listedIn(ix *Index, pool core.PoolID, c core.ChunkID) uint32 {
	x, _ := ix.Chunk(pool, c)
	return x.ListedIn
}

// recoveryWritten takes the store's answer to a recovery write: mapped, its chunk is recovered;
// failed, even before the store logged any of it, the interval ends, since the store no longer
// tracks what the interval covered in the chunk.
func (r *Replica) recoveryWritten(ctx *core.Context, rp Reply, out *ReplicaOutput) {
	if i := r.versionOf(rp.ID); i >= 0 {
		r.vers[i].done = true
		r.release(ctx, i)
	}
	if rp.Err != nil {
		freeBuf(ctx, rp.Buf)
		if k := slices.IndexFunc(r.recs, func(rc recovery) bool { return rc.req == rp.ID }); k >= 0 {
			if i := r.lead(r.recs[k].pg); i >= 0 {
				r.leads[i].asking = true
			}
		}
	}
	if v := r.recoveryOf(rp.ID); rp.Err == nil && v != nil {
		out.note(trace.OutRecovered, v.Hash(), rp.Version, 0, 0, 0)
	}
	r.recoveryDone(ctx, rp.ID, rp.Err == nil, out)
}

// recoveryOf is the chunk recovery write req is for, or nil.
func (r *Replica) recoveryOf(req uint64) *core.ChunkID {
	k := slices.IndexFunc(r.recs, func(rc recovery) bool { return rc.req == req })
	if k < 0 {
		return nil
	}
	l := r.lead(r.recs[k].pg)
	if l < 0 || r.recs[k].todo >= len(r.leads[l].todo) {
		return nil
	}
	return &r.leads[l].todo[r.recs[k].todo].chunk
}

// orderRecovery hands the store recovery k's write of image buf, which the store orders under the
// version of the interval's last write of the chunk before it, or the kept version, leaving out what
// those writes cover, and the primary then sends to the members that list the chunk alone. The
// primary logs the image only if it lacks the chunk itself.
func (r *Replica) orderRecovery(ctx *core.Context, k int, buf core.IOBuf) {
	rc := &r.recs[k]
	i := r.lead(rc.pg)
	if i < 0 || r.leads[i].interval != rc.interval || !r.leads[i].active || r.leads[i].asking {
		freeBuf(ctx, buf)
		r.endRecovery(k, false)
		return
	}
	l := &r.leads[i]
	_, e := r.table.entry(rc.pg)
	t := &l.todo[rc.todo]
	self := slices.IndexFunc(e.Members, func(m Member) bool { return m.Disk == r.cfg.Disk })
	rc.read, rc.rid = 0, 0
	rc.req = r.newReq(storeReq{kind: reqRecover, pg: rc.pg, chunk: t.chunk})
	u := Update{Kind: KindRecover, PG: PGFields{Epoch: r.table.Epoch, PG: rc.pg, Interval: rc.interval}, Chunk: t.chunk, Len: core.ChunkSize}
	r.vers = append(r.vers, version{req: rc.req, epoch: r.table.Epoch, interval: rc.interval, u: u, recover: true})
	r.st.Write(ctx, Request{ID: rc.req, Chunk: t.chunk, Len: core.ChunkSize, Buf: buf, Interval: rc.interval, Gated: true, Recover: true, Ver: t.kept,
		Relay: self < 0 || t.targets&(1<<self) == 0}, &r.sout)
}

// recoveryTarget reports whether recovery write v goes to disk: a member that lacks its chunk.
func (r *Replica) recoveryTarget(v *version, disk uint32) bool {
	k := slices.IndexFunc(r.recs, func(rc recovery) bool { return rc.req == v.req })
	if k < 0 {
		return false
	}
	rc := &r.recs[k]
	i := r.lead(rc.pg)
	if i < 0 {
		return false
	}
	_, e := r.table.entry(rc.pg)
	s := slices.IndexFunc(e.Members, func(m Member) bool { return m.Disk == disk })
	return s >= 0 && s < maxMembers && r.leads[i].todo[rc.todo].targets&(1<<s) != 0
}

// listedIn is the interval whose peering listed chunk c on this disk, 0 for none.
func (r *Replica) listedIn(c core.ChunkID) uint32 {
	if r.st.Index() == nil {
		return 0
	}
	return listedIn(r.st.Index(), r.st.cfg.Pool, c)
}

// recoveryDone ends the recovery whose write is store request req: once mapped, the chunk's members
// no longer lack it, and the client writes held on it go ahead.
func (r *Replica) recoveryDone(ctx *core.Context, req uint64, ok bool, out *ReplicaOutput) {
	k := slices.IndexFunc(r.recs, func(rc recovery) bool { return rc.req == req })
	if k < 0 {
		return
	}
	rc := r.recs[k]
	r.endRecovery(k, ok)
	if i := r.lead(rc.pg); ok && i >= 0 {
		r.releaseHeld(ctx, i, r.leads[i].todo[rc.todo].chunk, out)
	}
}

// endRecovery drops recovery k: done, its chunk's members lack it no longer; failed before its
// write was ordered, the chunk is tried again. A write that fails ends its interval, and the store
// no longer tracks what the interval covered in the chunk, so the chunk stays listed until the next
// interval peers.
func (r *Replica) endRecovery(k int, ok bool) {
	rc := r.recs[k]
	r.recs = slices.Delete(r.recs, k, k+1)
	i := r.lead(rc.pg)
	if i < 0 || r.leads[i].interval != rc.interval {
		return
	}
	l := &r.leads[i]
	t := &l.todo[rc.todo]
	t.busy = false
	if !ok && rc.req != 0 {
		t.targets = 0
		return
	}
	if !ok {
		l.cursor = min(l.cursor, rc.todo)
		return
	}
	for s := range maxMembers {
		if t.targets&(1<<s) != 0 {
			l.listed[s]--
		}
	}
	t.targets = 0
	if j, found := slices.BinarySearchFunc(l.kept, t.chunk, func(k keptChunk, c core.ChunkID) int { return compareChunk(k.chunk, c) }); found {
		l.kept[j].lacking = false
	}
}

// releaseHeld orders again the client writes lead i holds on chunk c, which waited for the
// primary to hold its kept version.
func (r *Replica) releaseHeld(ctx *core.Context, i int, c core.ChunkID, out *ReplicaOutput) {
	l := &r.leads[i]
	r.released = r.released[:0]
	l.held = slices.DeleteFunc(l.held, func(h heldMsg) bool {
		frame, data := Frame(ctx.Pool, &h.msg)
		var u Update
		if DecodeUpdate(frame, data, &u) != nil || u.Chunk != c {
			return false
		}
		r.released = append(r.released, h)
		return true
	})
	was := r.replaying
	r.replaying = true
	for _, h := range r.released {
		ev := core.Event{Kind: core.EvNetRecv, Peer: h.peer, Msg: h.msg}
		r.recv(ctx, &ev, out)
	}
	r.replaying = was
}

// recoverReply takes a member's answer to a recovery read, reporting whether it was one; it takes
// buf.
func (r *Replica) recoverReply(ctx *core.Context, peer core.NodeID, rp *Response, buf core.IOBuf) bool {
	k := slices.IndexFunc(r.recs, func(rc recovery) bool { return rc.rid != 0 && rc.rid == rp.ID && rc.source == peer })
	if k < 0 {
		return false
	}
	var m blockMap
	ok := rp.Status == StatusOK && buf != core.NoBuf && decodeBlockMap(ctx.Pool.Bytes(buf), &m) == nil
	r.recoveryRead(ctx, k, buf, ok)
	return true
}

// recoverLost fails the recovery reads asked of a peer whose connection broke.
func (r *Replica) recoverLost(ctx *core.Context, peer core.NodeID) {
	for k := len(r.recs) - 1; k >= 0; k-- {
		if rc := r.recs[k]; rc.rid != 0 && rc.source == peer {
			r.recoveryRead(ctx, k, core.NoBuf, false)
		}
	}
}

// dropRecoveries drops the recoveries and lists of the PGs whose interval this disk no longer leads
// in, as a table changes: the next interval peers and lists again.
func (r *Replica) dropRecoveries(old []lead) {
	for _, l := range old {
		if i := r.lead(l.pg); i >= 0 && r.leads[i].interval == l.interval {
			continue
		}
		for _, t := range l.todo {
			if t.targets != 0 {
				r.st.Uncover(t.chunk)
			}
		}
	}
	r.recs = slices.DeleteFunc(r.recs, func(rc recovery) bool {
		i := r.lead(rc.pg)
		return i < 0 || r.leads[i].interval != rc.interval
	})
}

// recovered reports whether the lead's interval is recovered: it activated, and every member but a
// leaving one is up and lacks nothing.
func (r *Replica) recovered(l *lead, e *PGEntry) bool {
	if !l.active || e == nil || e.Interval != l.interval {
		return false
	}
	for s, m := range e.Members {
		if !m.Leaving && (m.Mark != MarkUp || s < maxMembers && l.listed[s] > 0) {
			return false
		}
	}
	return true
}

// reportRecovered reports each lead whose interval has recovered and is not yet recorded so in the
// table: at once the first time, and with every retry after.
func (r *Replica) reportRecovered(again bool, out *ReplicaOutput) {
	for i := range r.leads {
		l := &r.leads[i]
		_, e := r.table.entry(l.pg)
		if (again || !l.reported) && r.recovered(l, e) && !e.Recovered {
			if !l.reported {
				out.note(trace.OutReported, pgField(l.pg), uint64(l.interval), 0, 0, 0)
			}
			l.reported = true
			out.Recovered = append(out.Recovered, PGFields{Epoch: r.table.Epoch, PG: l.pg, Interval: l.interval})
		}
	}
}

// unrecorded reports whether a lead has a recovered interval the table has not recorded, which
// RetryTimer stays armed for.
func (r *Replica) unrecorded() bool {
	return slices.ContainsFunc(r.leads, func(l lead) bool {
		_, e := r.table.entry(l.pg)
		return r.recovered(&l, e) && !e.Recovered
	})
}
