package chunkstore

import (
	"slices"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/lease"
)

// PeerTimer is the timer on which a member answers the peering it waited on: the end of its restart wait or of a grant to a primary the interval moves away from.
const PeerTimer core.TimerID = 0x70656572

// persistBit marks the store requests that persist a missing list, numbered apart from the others.
const persistBit = 1 << 63

// peering is a primary's peering of its lead's interval: the request ID its messages carry, the replies it reached, and once it decided, each chunk's kept version, in chunk order, and the up members that persisted their lists.
type peering struct {
	interval uint32
	id       core.RequestID
	replies  []peerReply
	decided  bool
	kept     []keptChunk
	acked    []uint32
	all      []replyChunk
}

// peerAsk is a peering request a member has yet to answer: from a primary's loop, or from this disk's own lead when local.
type peerAsk struct {
	peer     core.NodeID
	id       core.RequestID
	pg       core.PGID
	interval uint32
	local    bool
}

// persistReq is a missing list the store is persisting, for a primary's loop or, when local, this disk's own lead.
type persistReq struct {
	id       uint64
	peer     core.NodeID
	msgID    core.RequestID
	pg       core.PGID
	interval uint32
	local    bool
}

// seenEpoch is the newest table epoch a holder reported in its lease requests.
type seenEpoch struct {
	peer  core.NodeID
	epoch uint64
}

// see records the table epoch holder peer reported.
func (r *Replica) see(peer core.NodeID, epoch uint64) {
	i, ok := slices.BinarySearchFunc(r.seen, peer, func(s seenEpoch, p core.NodeID) int { return compareNode(s.peer, p) })
	if !ok {
		r.seen = slices.Insert(r.seen, i, seenEpoch{peer: peer})
	}
	r.seen[i].epoch = max(r.seen[i].epoch, epoch)
}

func (r *Replica) seenEpoch(peer core.NodeID) uint64 {
	if i, ok := slices.BinarySearchFunc(r.seen, peer, func(s seenEpoch, p core.NodeID) int { return compareNode(s.peer, p) }); ok {
		return r.seen[i].epoch
	}
	return 0
}

// peerWork moves peering on after any event: the members' answers it waited to give and each lead's peering, started, decided or activated.
func (r *Replica) peerWork(ctx *core.Context, out *ReplicaOutput) {
	for i := range r.leads {
		l := &r.leads[i]
		p, e := r.table.entry(l.pg)
		if l.active || l.asking || e.live() < int(p.MinLive) {
			continue
		}
		if l.peer == nil {
			r.peerID++
			l.peer = &peering{interval: l.interval, id: core.RequestID(r.peerID)}
			r.askPeers(i, false, out)
		}
	}
	r.answerPeers(ctx, out)
	for i := range r.leads {
		if l := &r.leads[i]; !l.active && l.peer != nil && l.peer.decided {
			r.tryActivate(ctx, i, out)
		}
	}
}

// askPeers sends peering's request to every up member of the lead's interval and of the past intervals since its last activated one, its own disk asked locally; with again, only to those that have not answered.
func (r *Replica) askPeers(i int, again bool, out *ReplicaOutput) {
	l := &r.leads[i]
	_, e := r.table.entry(l.pg)
	ask := func(disk uint32) {
		if again && slices.ContainsFunc(l.peer.replies, func(p peerReply) bool { return p.disk == disk }) {
			return
		}
		if disk == r.cfg.Disk {
			r.queueAsk(peerAsk{id: l.peer.id, pg: l.pg, interval: l.interval, local: true})
			return
		}
		if !r.reachable(disk) {
			return
		}
		r.frame = AppendPeering(r.frame[:0], &Peering{Kind: KindPeer, ID: l.peer.id, PG: PGFields{Epoch: r.table.Epoch, PG: l.pg, Interval: l.interval}})
		r.sendFrame(r.addr(disk), out)
	}
	r.peerDisks = peerDisks(e, r.peerDisks[:0])
	for _, d := range r.peerDisks {
		ask(d)
	}
}

// peerDisks appends the disks peering of e's interval asks: the up members of the interval and of its past intervals since the last activated one.
func peerDisks(e *PGEntry, dst []uint32) []uint32 {
	add := func(ms []Member) {
		for _, m := range ms {
			if m.Mark == MarkUp && !slices.Contains(dst, m.Disk) {
				dst = append(dst, m.Disk)
			}
		}
	}
	add(e.Members)
	for _, p := range e.Past {
		if p.Interval >= e.Activated && p.Interval < e.Interval {
			add(p.Members)
		}
	}
	return dst
}

func (r *Replica) queueAsk(a peerAsk) {
	for i := range r.answers {
		if q := &r.answers[i]; q.peer == a.peer && q.local == a.local && q.pg == a.pg {
			*q = a
			return
		}
	}
	r.answers = append(r.answers, a)
}

// peerAsked takes a primary's peering request: one for an interval newer than the replica's table waits for that table, one for an older interval is ignored, as the primary learns of the newer table from the control plane.
func (r *Replica) peerAsked(ctx *core.Context, ev *core.Event, p *Peering, out *ReplicaOutput) bool {
	_, e := r.table.entry(p.PG.PG)
	switch {
	case e == nil:
	case p.PG.Interval > e.Interval && p.PG.Epoch > r.table.Epoch:
		r.park(ev, p.PG.Epoch, out)
		return true
	case p.PG.Interval == e.Interval:
		r.queueAsk(peerAsk{peer: ev.Peer, id: p.ID, pg: p.PG.PG, interval: p.PG.Interval})
	}
	return false
}

// answerPeers answers each peering request it may: in the PG's current interval, once the restart wait is over, no grant to a primary the interval moves away from runs, and the store has resolved every write of the PG and every list it persists.
func (r *Replica) answerPeers(ctx *core.Context, out *ReplicaOutput) {
	for i := 0; i < len(r.answers); {
		a := r.answers[i]
		p, e := r.table.entry(a.pg)
		if e == nil || e.Interval != a.interval {
			r.answers = slices.Delete(r.answers, i, i+1)
			continue
		}
		if !r.mayPeer(ctx, e, out) {
			i++
			continue
		}
		chunks, decided, err := r.st.PeerState(a.pg.Index, uint32(len(p.PGs)), r.peerChunks[:0])
		r.peerChunks = chunks
		if err != nil {
			i++
			continue
		}
		r.answers = slices.Delete(r.answers, i, i+1)
		if a.local {
			r.peerReply(ctx, r.cfg.Disk, a.pg, a.interval, a.id, decided, chunks, out)
			continue
		}
		r.frame = AppendPeering(r.frame[:0], &Peering{Kind: KindPeerReply, ID: a.id, PG: PGFields{Epoch: r.table.Epoch, PG: a.pg, Interval: a.interval}, Decided: decided, Chunks: chunks})
		r.sendLong(ctx, a.peer, out)
	}
}

// mayPeer reports whether this disk may join peering of e's interval now: its restart wait is over, and each primary of an interval since the last activated one that e moves the primary away from holds no grant from it that still runs, unless it reported a table at least as new as this disk's, and so adopted the interval. Otherwise it arms PeerTimer for the wait's end.
func (r *Replica) mayPeer(ctx *core.Context, e *PGEntry, out *ReplicaOutput) bool {
	t := lease.Now(ctx)
	if r.grantor.Waiting(t) {
		r.armPeer(ctx, r.grantor.WaitEnd(t), out)
		return false
	}
	self, next := r.addr(r.cfg.Disk), core.NodeID{}
	if d, ok := e.primary(); ok {
		next = r.addr(d)
	}
	for _, p := range e.Past {
		if p.Interval < e.Activated || p.Interval >= e.Interval {
			continue
		}
		past := PGEntry{Members: p.Members}
		d, ok := past.primary()
		if !ok {
			continue
		}
		if x := r.addr(d); r.reachable(d) && x != self && x != next && r.grantor.Live(x, t) && r.seenEpoch(x) < r.table.Epoch {
			r.armPeer(ctx, r.grantor.Until(x, t), out)
			return false
		}
	}
	return true
}

// armPeer arms PeerTimer for at unless it is armed sooner.
func (r *Replica) armPeer(ctx *core.Context, at core.Tick, out *ReplicaOutput) {
	if r.peerAt != 0 && r.peerAt <= at {
		return
	}
	r.peerAt = max(at, ctx.Now)
	out.Actions = append(out.Actions, core.Action{Kind: core.ActSetTimer, Timer: PeerTimer, At: r.peerAt})
}

// peerTick takes PeerTimer; a stale firing does nothing.
func (r *Replica) peerTick(ctx *core.Context) {
	if r.peerAt != 0 && ctx.Now >= r.peerAt {
		r.peerAt = 0
	}
}

// sendLong sends the frame in r.frame, in a pool buffer the driver frees once sent if it is too long to go inline.
func (r *Replica) sendLong(ctx *core.Context, peer core.NodeID, out *ReplicaOutput) {
	a := core.Action{Kind: core.ActNetSend, Peer: peer}
	if err := Attach(ctx.Pool, &a.Msg, r.frame, core.NoBuf); err != nil {
		return
	}
	a.Reply = a.Msg.Buf != core.NoBuf
	out.Actions = append(out.Actions, a)
}

// peerReplied takes a member's answer to peering.
func (r *Replica) peerReplied(ctx *core.Context, peer core.NodeID, p *Peering, out *ReplicaOutput) {
	i := r.lead(p.PG.PG)
	if i < 0 {
		return
	}
	l := &r.leads[i]
	if l.peer == nil {
		return
	}
	r.peerDisks = peerDisks(r.entryOf(l.pg), r.peerDisks[:0])
	for _, d := range r.peerDisks {
		if r.reachable(d) && r.addr(d) == peer && d != r.cfg.Disk {
			r.peerReply(ctx, d, p.PG.PG, p.PG.Interval, p.ID, p.Decided, p.Chunks, out)
			return
		}
	}
}

func (r *Replica) entryOf(pg core.PGID) *PGEntry {
	_, e := r.table.entry(pg)
	return e
}

// peerReply records disk's answer to the lead's peering and decides once every up member of the interval answered and a member up in each past interval that could have taken writes did: those since the last activated one with min_live live members and one up.
func (r *Replica) peerReply(ctx *core.Context, disk uint32, pg core.PGID, interval uint32, id core.RequestID, decided uint32, chunks []ChunkPeer, out *ReplicaOutput) {
	i := r.lead(pg)
	if i < 0 {
		return
	}
	l := &r.leads[i]
	pr := l.peer
	if pr == nil || pr.decided || pr.interval != interval || pr.id != id || l.interval != interval {
		return
	}
	rp := peerReply{disk: disk, decided: decided, chunks: slices.Clone(chunks)}
	if j := slices.IndexFunc(pr.replies, func(p peerReply) bool { return p.disk == disk }); j >= 0 {
		pr.replies[j] = rp
	} else {
		pr.replies = append(pr.replies, rp)
	}
	_, e := r.table.entry(pg)
	answered := func(ms []Member, all bool) bool {
		for _, m := range ms {
			got := m.Mark == MarkUp && slices.ContainsFunc(pr.replies, func(p peerReply) bool { return p.disk == m.Disk })
			if all && m.Mark == MarkUp && !got {
				return false
			}
			if !all && got {
				return true
			}
		}
		return all
	}
	if !answered(e.Members, true) {
		return
	}
	for _, past := range e.Past {
		x := PGEntry{Members: past.Members}
		if _, up := x.primary(); past.Interval >= e.Activated && past.Interval < e.Interval && up && x.live() >= int(past.MinLive) && !answered(past.Members, false) {
			return
		}
	}
	r.decide(ctx, i, out)
}

// decide applies the keep rule to the replies and has every up member of the interval persist its missing list, this disk's own included.
func (r *Replica) decide(ctx *core.Context, i int, out *ReplicaOutput) {
	l := &r.leads[i]
	pr := l.peer
	pr.all, pr.kept = keep(r.entryOf(l.pg), pr.replies, pr.all, pr.kept[:0])
	pr.decided = true
	r.persistLists(ctx, i, out)
}

// persistLists sends each up member of the interval that has not persisted its list the list again, persisting this disk's own unless it is in flight.
func (r *Replica) persistLists(ctx *core.Context, i int, out *ReplicaOutput) {
	l := &r.leads[i]
	pr := l.peer
	p, e := r.table.entry(l.pg)
	for _, rp := range pr.replies {
		if !upIn(e, rp.disk, e.Interval) || slices.Contains(pr.acked, rp.disk) {
			continue
		}
		if rp.disk != r.cfg.Disk {
			r.frame = AppendPeering(r.frame[:0], &Peering{Kind: KindPersist, ID: pr.id, PG: PGFields{Epoch: r.table.Epoch, PG: l.pg, Interval: l.interval}, Missing: rp.list})
			r.sendLong(ctx, r.addr(rp.disk), out)
			continue
		}
		if !slices.ContainsFunc(r.persists, func(q persistReq) bool { return q.local && q.pg == l.pg && q.interval == l.interval }) {
			r.persist(ctx, persistReq{pg: l.pg, interval: l.interval, local: true}, uint32(len(p.PGs)), rp.list)
		}
	}
}

// persist has the store persist a missing list for q.
func (r *Replica) persist(ctx *core.Context, q persistReq, pgs uint32, list []ChunkVersion) {
	r.peerID++
	q.id = persistBit | r.peerID
	r.persists = append(r.persists, q)
	r.st.Persist(ctx, q.id, q.pg.Index, pgs, q.interval, list, &r.sout)
}

// persistAsked takes a primary's missing list for the PG's current interval, persisting it; one for a newer interval waits for that table, and one for an older interval is refused.
func (r *Replica) persistAsked(ctx *core.Context, ev *core.Event, p *Peering, out *ReplicaOutput) bool {
	pt, e := r.table.entry(p.PG.PG)
	switch {
	case e != nil && p.PG.Interval > e.Interval && p.PG.Epoch > r.table.Epoch:
		r.park(ev, p.PG.Epoch, out)
		return true
	case e == nil || p.PG.Interval != e.Interval:
		rp := r.refusal(p.PG.PG)
		rp.Kind = KindPersistAck
		r.answer(ctx, ev.Peer, p.ID, &rp, core.NoBuf, out)
	default:
		r.persist(ctx, persistReq{peer: ev.Peer, msgID: p.ID, pg: p.PG.PG, interval: p.PG.Interval}, uint32(len(pt.PGs)), p.Missing)
	}
	return false
}

// persisted takes the store's answer to a missing list it persisted: acknowledged to the primary that sent it, or counted toward this disk's own lead's activation.
func (r *Replica) persisted(ctx *core.Context, rp Reply, out *ReplicaOutput) {
	j := slices.IndexFunc(r.persists, func(q persistReq) bool { return q.id == rp.ID })
	if j < 0 {
		return
	}
	q := r.persists[j]
	r.persists = slices.Delete(r.persists, j, j+1)
	if q.local {
		if rp.Err == nil {
			r.persistAcked(q.pg, q.interval, r.cfg.Disk)
		}
		return
	}
	ack := Response{Kind: KindPersistAck, Status: StatusOK}
	if rp.Err != nil {
		ack = r.refusal(q.pg)
		ack.Kind = KindPersistAck
	}
	r.answer(ctx, q.peer, q.msgID, &ack, core.NoBuf, out)
}

// persistAck takes a member's acknowledgment of its missing list.
func (r *Replica) persistAck(peer core.NodeID, rp *Response, out *ReplicaOutput) {
	if rp.Status != StatusOK {
		if rp.Epoch > r.table.Epoch {
			r.fetch(rp.Epoch, false, out)
		}
		return
	}
	for i := range r.leads {
		l := &r.leads[i]
		if l.peer == nil || l.peer.id != rp.ID {
			continue
		}
		for _, rpl := range l.peer.replies {
			if r.reachable(rpl.disk) && r.addr(rpl.disk) == peer && rpl.disk != r.cfg.Disk {
				r.persistAcked(l.pg, l.interval, rpl.disk)
			}
		}
	}
}

func (r *Replica) persistAcked(pg core.PGID, interval uint32, disk uint32) {
	i := r.lead(pg)
	if i < 0 || r.leads[i].peer == nil || !r.leads[i].peer.decided || r.leads[i].interval != interval || slices.Contains(r.leads[i].peer.acked, disk) {
		return
	}
	r.leads[i].peer.acked = append(r.leads[i].peer.acked, disk)
}

// tryActivate activates the lead once every up member of its interval persisted its list: it hands the store the decision on each chunk with versions pending from earlier intervals, then activates and reports it.
func (r *Replica) tryActivate(ctx *core.Context, i int, out *ReplicaOutput) {
	l := &r.leads[i]
	pr := l.peer
	_, e := r.table.entry(l.pg)
	if l.asking {
		return
	}
	for _, m := range e.Members {
		if m.Mark == MarkUp && !slices.Contains(pr.acked, m.Disk) {
			return
		}
	}
	r.unsettled = r.Unsettled(l.pg, r.unsettled[:0])
	for _, c := range r.unsettled {
		var kept uint64
		if j, ok := slices.BinarySearchFunc(pr.kept, c, func(k keptChunk, c core.ChunkID) int { return compareChunk(k.chunk, c) }); ok {
			kept = pr.kept[j].kept
		}
		if err := r.st.Settle(ctx, c, kept, &r.sout); err != nil {
			return
		}
		for j := range r.vers {
			if v := &r.vers[j]; v.u.Chunk == c && v.u.Version != 0 && v.u.Version <= kept {
				v.commit = true
			}
		}
	}
	var own []ChunkPeer
	if j := slices.IndexFunc(pr.replies, func(p peerReply) bool { return p.disk == r.cfg.Disk }); j >= 0 {
		own = pr.replies[j].chunks
	}
	for j := range pr.kept {
		k := &pr.kept[j]
		x, _ := r.st.Index().Chunk(r.st.cfg.Pool, k.chunk)
		k.lacking = x.Version != k.kept || x.Whole != k.kept || slices.ContainsFunc(own, func(c ChunkPeer) bool { return c.Chunk == k.chunk && c.Listed })
	}
	l.kept, pr.kept = pr.kept, l.kept[:0]
	pg, k := l.pg, l.interval
	if r.Activate(ctx, pg, k, out) != nil {
		return
	}
	if i = r.lead(pg); i < 0 || !r.leads[i].active {
		return
	}
	out.Activations = append(out.Activations, PGFields{Epoch: r.table.Epoch, PG: pg, Interval: k})
}

// peerRetry sends again, every RetryAfter, what peering has not had answered: requests before the decision, lists after it, and the activations no table has recorded.
func (r *Replica) peerRetry(ctx *core.Context, out *ReplicaOutput) {
	for i := range r.leads {
		l := &r.leads[i]
		_, e := r.table.entry(l.pg)
		switch {
		case l.active && e.Activated < l.interval:
			out.Activations = append(out.Activations, PGFields{Epoch: r.table.Epoch, PG: l.pg, Interval: l.interval})
		case l.peer != nil && !l.peer.decided:
			r.askPeers(i, true, out)
		case l.peer != nil:
			r.persistLists(ctx, i, out)
		}
	}
}

// peering reports whether a lead peers or has an activation to report, for which RetryTimer stays armed.
func (r *Replica) peering() bool {
	return slices.ContainsFunc(r.leads, func(l lead) bool {
		_, e := r.table.entry(l.pg)
		return l.peer != nil || l.active && e.Activated < l.interval
	})
}

// isUnfound reports whether the lead's last peering found no member to serve chunk c from.
func (l *lead) isUnfound(c core.ChunkID) bool {
	k, ok := l.decision(c)
	return ok && k.unfound
}

// lacks reports whether the primary's copy of chunk c may differ from the version the lead's last peering kept, which a partial-block write waits for.
func (l *lead) lacks(c core.ChunkID) bool {
	k, ok := l.decision(c)
	return ok && k.lacking
}

func (l *lead) decision(c core.ChunkID) (keptChunk, bool) {
	j, ok := slices.BinarySearchFunc(l.kept, c, func(k keptChunk, c core.ChunkID) int { return compareChunk(k.chunk, c) })
	if !ok {
		return keptChunk{}, false
	}
	return l.kept[j], true
}
