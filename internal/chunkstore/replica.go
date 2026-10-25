package chunkstore

import (
	"cmp"
	"errors"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// maxMembers is the widest PG a replica primary fans out to: rep3 with a leaving member.
const maxMembers = 8

// ReplicaConfig is a disk's place in the cluster.
type ReplicaConfig struct {
	// Disk is the disk whose store the replica runs, by its ID in the placement table.
	Disk uint32
}

// ReplicaOutput is what a call into a Replica asks of its loop: actions, and of the control plane a table at least as new as Fetch, whole if Whole.
type ReplicaOutput struct {
	Actions []core.Action
	Fetch   uint64
	Whole   bool
}

// Reset empties the output for reuse.
func (o *ReplicaOutput) Reset() { o.Actions, o.Fetch, o.Whole = o.Actions[:0], 0, false }

func (o *ReplicaOutput) fetch(epoch uint64, whole bool) {
	o.Fetch = max(o.Fetch, epoch)
	o.Whole = o.Whole || whole
}

// lead is a PG this disk is primary of: its interval in the replica's table, the interval its term began in, whether it has peered in the interval, and the client writes it holds until it has.
type lead struct {
	pg       core.PGID
	interval uint32
	since    uint32
	active   bool
	held     []heldMsg
}

type heldMsg struct {
	peer core.NodeID
	msg  core.Msg
	hold uint32
}

type parkedMsg struct {
	epoch uint64
	peer  core.NodeID
	msg   core.Msg
}

type reqKind uint8

const (
	reqClient reqKind = iota + 1
	reqMember
	reqRead
)

// storeReq is a store request in flight: who asked, and for a client's write or punch its version's state.
type storeReq struct {
	id    uint64
	kind  reqKind
	peer  core.NodeID
	msgID core.RequestID
	pg    core.PGID
	chunk core.ChunkID
}

// version is a client write or punch the primary ordered and fans out: the members that must hold it, those that do, and those whose send has neither a reply nor a PeerLost yet.
type version struct {
	seq     core.RequestID
	req     uint64
	client  core.NodeID
	msgID   core.RequestID
	epoch   uint64
	u       Update
	buf     core.IOBuf
	members [maxMembers]uint32
	n       int
	acked   uint8
	out     uint8
	logged  bool
	commit  bool
	done    bool
	status  Status
	refEp   uint64
	refIv   uint32
}

// Replica runs a disk's store as a member of the PGs the placement table puts on it: primary of those it leads, ordering client writes, fanning them out and committing them once every live member holds them, and a replica of the rest.
type Replica struct {
	cfg    ReplicaConfig
	st     *Store
	table  Table
	addrs  []core.NodeID
	sout   Output
	leads  []lead
	reqs   []storeReq
	vers   []version
	parked []parkedMsg
	dups   []dup
	bases  []baseFetch
	nextID uint64
	seq    core.RequestID
	frame  []byte
}

// NewReplica runs st, a serving store, as disk cfg.Disk.
func NewReplica(cfg ReplicaConfig, st *Store) *Replica {
	return &Replica{cfg: cfg, st: st}
}

// Store is the replica's store.
func (r *Replica) Store() *Store { return r.st }

// Table is the placement table the replica holds.
func (r *Replica) Table() *Table { return &r.table }

// SetAddr records the loop that owns disk, as the membership record publishes it.
func (r *Replica) SetAddr(disk uint32, addr core.NodeID) {
	if int(disk) >= len(r.addrs) {
		r.addrs = append(r.addrs, make([]core.NodeID, int(disk)+1-len(r.addrs))...)
	}
	r.addrs[disk] = addr
}

func (r *Replica) diskAt(addr core.NodeID) (uint32, bool) {
	for d, a := range r.addrs {
		if a == addr {
			return uint32(d), true
		}
	}
	return 0, false
}

// SetTable adopts t if it is newer than the table the replica holds.
func (r *Replica) SetTable(ctx *core.Context, t Table, out *ReplicaOutput) {
	if t.Epoch > r.table.Epoch {
		r.adopt(ctx, t, out)
	}
}

// ApplyDelta adopts the table d makes of the one the replica holds, or asks for the whole table after a gap or a CRC mismatch.
func (r *Replica) ApplyDelta(ctx *core.Context, d *Delta, out *ReplicaOutput) {
	if d.Epoch <= r.table.Epoch {
		return
	}
	t, err := r.table.Apply(d)
	if err != nil {
		out.fetch(d.Epoch, true)
		return
	}
	r.adopt(ctx, t, out)
}

func (t *Table) entry(pg core.PGID) (*PoolTable, *PGEntry) {
	p := t.pool(pg.Pool)
	if p == nil || int(pg.Index) >= len(p.PGs) {
		return nil, nil
	}
	return p, &p.PGs[pg.Index]
}

// primary is the PG's first up member, which orders its writes; an interval whose live members are all in maintenance has none.
func (e *PGEntry) primary() (uint32, bool) {
	for _, m := range e.Members {
		if m.Mark == MarkUp {
			return m.Disk, true
		}
	}
	return 0, false
}

func (e *PGEntry) live() int {
	n := 0
	for _, m := range e.Members {
		if m.Mark != MarkDown {
			n++
		}
	}
	return n
}

func (r *Replica) lead(pg core.PGID) int {
	i, ok := slices.BinarySearchFunc(r.leads, pg, func(l lead, pg core.PGID) int { return comparePG(l.pg, pg) })
	if !ok {
		return -1
	}
	return i
}

func comparePG(a, b core.PGID) int {
	return cmp.Or(cmp.Compare(a.Pool, b.Pool), cmp.Compare(a.Index, b.Index))
}

// adopt takes t as the replica's table. A PG this disk leads in t keeps its term if no other primary led it in between, as the PG's listed past intervals show, and holds its client writes until it has peered in its new interval; a PG it no longer leads refuses the writes it held.
func (r *Replica) adopt(ctx *core.Context, t Table, out *ReplicaOutput) {
	r.table = t
	kept := r.leads[:0:0]
	for pi := range t.Pools {
		p := &t.Pools[pi]
		for i := range p.PGs {
			e := &p.PGs[i]
			if d, ok := e.primary(); !ok || d != r.cfg.Disk {
				continue
			}
			pg := core.PGID{Pool: p.ID, Index: uint32(i)}
			l := lead{pg: pg, interval: e.Interval, since: e.Interval}
			if j := r.lead(pg); j >= 0 {
				old := r.leads[j]
				l.held = old.held
				r.leads[j].held = nil
				switch {
				case old.interval == e.Interval:
					l = old
				case r.keptPrimary(e, old.interval):
					l.since = old.since
				}
			}
			kept = append(kept, l)
		}
	}
	for i := range r.leads {
		for _, h := range r.leads[i].held {
			r.refuseHeld(ctx, h, out)
		}
	}
	r.leads = kept
	parked := r.parked
	r.parked = nil
	for _, p := range parked {
		if p.epoch > t.Epoch {
			r.parked = append(r.parked, p)
			continue
		}
		ev := core.Event{Kind: core.EvNetRecv, Peer: p.peer, Msg: p.msg}
		r.recv(ctx, &ev, out)
	}
	r.drain(ctx, out)
}

// keptPrimary reports whether this disk was the primary of every interval of e after from: e's past intervals list them back to its last activated one.
func (r *Replica) keptPrimary(e *PGEntry, from uint32) bool {
	for k := from + 1; k < e.Interval; k++ {
		i := slices.IndexFunc(e.Past, func(p PastInterval) bool { return p.Interval == k })
		if i < 0 {
			return false
		}
		past := PGEntry{Members: e.Past[i].Members}
		if d, ok := past.primary(); !ok || d != r.cfg.Disk {
			return false
		}
	}
	return from < e.Interval
}

// Activate is peering's end: the PG this disk leads is now active in interval, and the client writes it held are ordered again, unless another primary led the PG since the interval they were held in.
func (r *Replica) Activate(ctx *core.Context, pg core.PGID, interval uint32, out *ReplicaOutput) {
	i := r.lead(pg)
	if i < 0 || r.leads[i].interval != interval {
		return
	}
	r.leads[i].active = true
	held := r.leads[i].held
	r.leads[i].held = nil
	for _, h := range held {
		if h.hold < r.leads[i].since {
			r.refuseHeld(ctx, h, out)
			continue
		}
		ev := core.Event{Kind: core.EvNetRecv, Peer: h.peer, Msg: h.msg}
		r.recv(ctx, &ev, out)
	}
	r.drain(ctx, out)
}

func (r *Replica) refuseHeld(ctx *core.Context, h heldMsg, out *ReplicaOutput) {
	frame, data := Frame(ctx.Pool, &h.msg)
	var u Update
	if DecodeUpdate(frame, data, &u) == nil {
		r.reply(ctx, h.peer, u.ID, r.refusal(u.PG.PG), out)
	}
	if h.msg.Buf != core.NoBuf {
		ctx.Pool.Free(h.msg.Buf)
	}
}

// OnEvent takes a message, a lost peer, a disk completion or one of the store's timers.
func (r *Replica) OnEvent(ctx *core.Context, ev *core.Event, out *ReplicaOutput) {
	switch ev.Kind {
	case core.EvNetRecv:
		r.recv(ctx, ev, out)
	case core.EvPeerLost:
		r.peerLost(ev.Peer)
	case core.EvDiskDone, core.EvTimer:
		r.st.OnEvent(ctx, ev, &r.sout)
	}
	r.drain(ctx, out)
}

func (r *Replica) recv(ctx *core.Context, ev *core.Event, out *ReplicaOutput) {
	frame, data := Frame(ctx.Pool, &ev.Msg)
	buf := ev.Msg.Buf
	kind, err := Peek(frame)
	if err != nil {
		freeBuf(ctx, buf)
		return
	}
	switch kind {
	case KindHello:
		var h Hello
		if DecodeHello(frame, data, &h) == nil {
			r.st.SeeIncarnation(h.Session, h.Incarnation)
		}
	case KindWrite, KindPunch:
		var u Update
		if DecodeUpdate(frame, data, &u) != nil {
			break
		}
		r.clientUpdate(ctx, ev, &u, out)
		return
	case KindReplicaWrite, KindReplicaPunch:
		var u Update
		if DecodeUpdate(frame, data, &u) != nil {
			break
		}
		r.memberUpdate(ctx, ev, &u, out)
		return
	case KindRead:
		var rd Read
		if DecodeRead(frame, data, &rd) == nil {
			r.read(ctx, ev, &rd, out)
		}
	case KindAck:
		var rp Response
		if DecodeResponse(frame, data, &rp) == nil {
			r.ack(ctx, ev.Peer, &rp, out)
		}
	case KindReadReply:
		var rp Response
		if DecodeResponse(frame, data, &rp) == nil && r.baseReply(ctx, &rp, buf, out) {
			return
		}
	}
	freeBuf(ctx, buf)
}

// askBase sends the next up member of the PG a read of the base block at its version, or fails the write once none is left to ask.
func (r *Replica) askBase(ctx *core.Context, i int, out *ReplicaOutput) {
	f := &r.bases[i]
	_, e := r.table.entry(f.pg)
	for e != nil && f.tried < len(e.Members) {
		m := e.Members[f.tried]
		f.tried++
		if m.Mark != MarkUp || m.Disk == r.cfg.Disk {
			continue
		}
		rd := Read{ID: core.RequestID(f.ID), PG: PGFields{Epoch: r.table.Epoch, PG: f.pg, Interval: e.Interval}, Chunk: f.Chunk,
			Off: uint32(f.Block) * core.BlockSize, Len: core.BlockSize, HasAt: true, At: f.Version}
		r.frame = AppendRead(r.frame[:0], &rd)
		a := core.Action{Kind: core.ActNetSend, Peer: r.addr(m.Disk)}
		a.Msg.Len = uint16(copy(a.Msg.Body[:], r.frame))
		out.Actions = append(out.Actions, a)
		return
	}
	id := f.ID
	r.bases = slices.Delete(r.bases, i, i+1)
	r.st.SupplyBase(ctx, id, core.NoBuf, disklog.ErrBadChecksum, &r.sout)
}

// baseReply takes a member's answer to a base fetch, reporting whether it was one; it takes buf.
func (r *Replica) baseReply(ctx *core.Context, rp *Response, buf core.IOBuf, out *ReplicaOutput) bool {
	i := slices.IndexFunc(r.bases, func(f baseFetch) bool { return core.RequestID(f.ID) == rp.ID })
	if i < 0 {
		return false
	}
	if rp.Status != StatusOK {
		freeBuf(ctx, buf)
		r.askBase(ctx, i, out)
		return true
	}
	id := r.bases[i].ID
	r.bases = slices.Delete(r.bases, i, i+1)
	r.st.SupplyBase(ctx, id, buf, nil, &r.sout)
	return true
}

func freeBuf(ctx *core.Context, b core.IOBuf) {
	if b != core.NoBuf {
		ctx.Pool.Free(b)
	}
}

// refusal is the refusal of a request about pg: this replica's table epoch and pg's interval in it.
func (r *Replica) refusal(pg core.PGID) Response {
	rp := Response{Status: StatusRefused, Epoch: r.table.Epoch, Interval: 1}
	if _, e := r.table.entry(pg); e != nil {
		rp.Interval = e.Interval
	}
	return rp
}

// park keeps a message tagged with a newer table than the replica holds until it has one at least as new as the sender's.
func (r *Replica) park(ev *core.Event, epoch uint64, out *ReplicaOutput) {
	r.parked = append(r.parked, parkedMsg{epoch: epoch, peer: ev.Peer, msg: ev.Msg})
	out.fetch(epoch, false)
}

// clientUpdate takes a client's write or punch at the PG's primary: it is refused unless this disk leads the PG with at least min_live members live and the client's interval falls within the primary's term, held while the PG peers, and otherwise ordered.
func (r *Replica) clientUpdate(ctx *core.Context, ev *core.Event, u *Update, out *ReplicaOutput) {
	r.st.SeeIncarnation(u.Session, u.Incarnation)
	p, e := r.table.entry(u.PG.PG)
	i := r.lead(u.PG.PG)
	switch {
	case e != nil && i < 0 && u.PG.Interval > e.Interval && u.PG.Epoch > r.table.Epoch:
		r.park(ev, u.PG.Epoch, out)
		return
	case i < 0 || u.PG.Interval < r.leads[i].since || e.live() < int(p.MinLive):
	case u.PG.Interval > r.leads[i].interval:
		if u.PG.Epoch > r.table.Epoch {
			r.park(ev, u.PG.Epoch, out)
			return
		}
	case !r.leads[i].active:
		l := &r.leads[i]
		l.held = append(l.held, heldMsg{peer: ev.Peer, msg: ev.Msg, hold: l.interval})
		return
	default:
		r.order(ctx, ev, u, r.leads[i].interval)
		return
	}
	freeBuf(ctx, ev.Msg.Buf)
	r.reply(ctx, ev.Peer, u.ID, r.refusal(u.PG.PG), out)
}

func (r *Replica) order(ctx *core.Context, ev *core.Event, u *Update, interval uint32) {
	id := r.newReq(storeReq{kind: reqClient, peer: ev.Peer, msgID: u.ID, pg: u.PG.PG, chunk: u.Chunk})
	req := Request{ID: id, Chunk: u.Chunk, Off: u.Off, Len: u.Len, Buf: ev.Msg.Buf, HasVer: u.HasExpect, Ver: u.Expect,
		Epoch: u.Snapshot, Interval: interval, Gated: true, Session: u.Session, Incarnation: u.Incarnation}
	r.seq++
	r.vers = append(r.vers, version{seq: r.seq, req: id, client: ev.Peer, msgID: u.ID, epoch: r.table.Epoch, u: *u})
	if u.Kind == KindWrite {
		r.st.Write(ctx, req, &r.sout)
	} else {
		freeBuf(ctx, ev.Msg.Buf)
		r.st.Punch(ctx, req, &r.sout)
	}
}

// memberUpdate takes a write or punch the PG's primary ordered: accepted only in the PG's current interval, by an up member; a version the store already holds is answered once it is durable.
func (r *Replica) memberUpdate(ctx *core.Context, ev *core.Event, u *Update, out *ReplicaOutput) {
	_, e := r.table.entry(u.PG.PG)
	if e != nil && u.PG.Interval > e.Interval && u.PG.Epoch > r.table.Epoch {
		r.park(ev, u.PG.Epoch, out)
		return
	}
	if e == nil || u.PG.Interval != e.Interval || !slices.ContainsFunc(e.Members, func(m Member) bool { return m.Disk == r.cfg.Disk && m.Mark == MarkUp }) {
		freeBuf(ctx, ev.Msg.Buf)
		rp := r.refusal(u.PG.PG)
		rp.Kind = KindAck
		r.answer(ctx, ev.Peer, u.ID, &rp, core.NoBuf, out)
		return
	}
	id := r.newReq(storeReq{kind: reqMember, peer: ev.Peer, msgID: u.ID, pg: u.PG.PG, chunk: u.Chunk})
	if u.Version <= r.st.Newest(u.Chunk) {
		freeBuf(ctx, ev.Msg.Buf)
		r.dups = append(r.dups, dup{id: id, version: u.Version})
		r.st.Flush(ctx, id, []core.ChunkID{u.Chunk}, &r.sout)
		return
	}
	req := Request{ID: id, Chunk: u.Chunk, Off: u.Off, Len: u.Len, Buf: ev.Msg.Buf, Epoch: u.Snapshot, Version: u.Version}
	if u.Kind == KindReplicaWrite {
		r.st.Write(ctx, req, &r.sout)
	} else {
		r.st.Punch(ctx, req, &r.sout)
	}
}

// baseFetch is a base block the primary asks another member for, tried from member tried of the PG.
type baseFetch struct {
	BaseFetch
	pg    core.PGID
	tried int
}

// dup is a resent write a member already holds, answered with its version once the chunk's flush completes.
type dup struct {
	id      uint64
	version uint64
}

// read serves a client's read at the PG's primary, a read of a frozen version at any member, and at a member a base its primary fetches, the blocks at the version the primary's index names.
func (r *Replica) read(ctx *core.Context, ev *core.Event, rd *Read, out *ReplicaOutput) {
	_, e := r.table.entry(rd.PG.PG)
	i := r.lead(rd.PG.PG)
	primary := i >= 0 && r.leads[i].active && rd.PG.Interval >= r.leads[i].since && rd.PG.Interval <= r.leads[i].interval
	base := false
	if e != nil && i < 0 && rd.HasAt && rd.PG.Interval == e.Interval {
		d, ok := e.primary()
		base = ok && r.addr(d) == ev.Peer
	}
	if e == nil || i >= 0 && !primary {
		rp := r.refusal(rd.PG.PG)
		rp.Kind = KindReadReply
		r.answer(ctx, ev.Peer, rd.ID, &rp, core.NoBuf, out)
		return
	}
	id := r.newReq(storeReq{kind: reqRead, peer: ev.Peer, msgID: rd.ID, pg: rd.PG.PG, chunk: rd.Chunk})
	r.st.Read(ctx, Request{ID: id, Chunk: rd.Chunk, Off: rd.Off, Len: rd.Len, HasVer: rd.HasAt, Ver: rd.At, FrozenOnly: !primary && !base, Base: base}, &r.sout)
}

func (r *Replica) newReq(q storeReq) uint64 {
	r.nextID++
	q.id = r.nextID
	r.reqs = append(r.reqs, q)
	return q.id
}

func (r *Replica) takeReq(id uint64) (storeReq, bool) {
	i, ok := slices.BinarySearchFunc(r.reqs, id, func(q storeReq, id uint64) int { return cmp.Compare(q.id, id) })
	if !ok {
		return storeReq{}, false
	}
	q := r.reqs[i]
	r.reqs = slices.Delete(r.reqs, i, i+1)
	return q, true
}

func (r *Replica) versionOf(req uint64) int {
	return slices.IndexFunc(r.vers, func(v version) bool { return v.req == req })
}

func (r *Replica) versionBySeq(seq core.RequestID) int {
	i, ok := slices.BinarySearchFunc(r.vers, seq, func(v version, seq core.RequestID) int { return cmp.Compare(v.seq, seq) })
	if !ok {
		return -1
	}
	return i
}

// drain handles what the store answered until it has nothing more to say: the versions it ordered, those durable on its disk and the requests it finished.
func (r *Replica) drain(ctx *core.Context, out *ReplicaOutput) {
	for o, l, p, b := 0, 0, 0, 0; o < len(r.sout.Ordered) || l < len(r.sout.Logged) || p < len(r.sout.Replies) || b < len(r.sout.Bases); {
		for ; b < len(r.sout.Bases); b++ {
			f := baseFetch{BaseFetch: r.sout.Bases[b]}
			if i, ok := slices.BinarySearchFunc(r.reqs, f.ID, func(q storeReq, id uint64) int { return cmp.Compare(q.id, id) }); ok {
				f.pg = r.reqs[i].pg
			}
			r.bases = append(r.bases, f)
			r.askBase(ctx, len(r.bases)-1, out)
		}
		for ; o < len(r.sout.Ordered); o++ {
			r.ordered(ctx, r.sout.Ordered[o], out)
		}
		for ; l < len(r.sout.Logged); l++ {
			r.logged(ctx, r.sout.Logged[l], out)
		}
		for ; p < len(r.sout.Replies); p++ {
			r.finished(ctx, r.sout.Replies[p], out)
		}
	}
	out.Actions = append(out.Actions, r.sout.Actions...)
	r.sout.Reset()
}

// ordered fans a version out to every up member of its interval but this disk, in parallel with the store logging it.
func (r *Replica) ordered(ctx *core.Context, o Reply, out *ReplicaOutput) {
	i := r.versionOf(o.ID)
	v := &r.vers[i]
	v.u.Version, v.buf = o.Version, o.Buf
	_, e := r.table.entry(v.u.PG.PG)
	for _, m := range e.Members {
		if m.Mark != MarkUp || m.Disk == r.cfg.Disk {
			continue
		}
		if v.n == maxMembers {
			panic("chunkstore: a PG wider than a replica primary fans out to")
		}
		v.members[v.n] = m.Disk
		v.n++
	}
	for k := range v.n {
		r.send(v, k, out)
	}
}

func (r *Replica) send(v *version, k int, out *ReplicaOutput) {
	u := v.u
	u.ID, u.HasExpect, u.Expect = v.seq, false, 0
	u.PG.Epoch, u.PG.Interval = v.epoch, uint32(v.u.Version>>32)
	if u.Kind == KindWrite {
		first, last := spanOf(u.Off, u.Len)
		u.Kind, u.Off, u.Len = KindReplicaWrite, uint32(first)*core.BlockSize, uint32(last-first+1)*core.BlockSize
	} else {
		u.Kind = KindReplicaPunch
	}
	r.frame = AppendUpdate(r.frame[:0], &u)
	a := core.Action{Kind: core.ActNetSend, Peer: r.addr(v.members[k])}
	a.Msg.Len, a.Msg.Buf = uint16(copy(a.Msg.Body[:], r.frame)), v.buf
	out.Actions = append(out.Actions, a)
	v.out |= 1 << k
}

func (r *Replica) addr(disk uint32) core.NodeID {
	if int(disk) < len(r.addrs) {
		return r.addrs[disk]
	}
	return core.NodeID{}
}

func (r *Replica) logged(ctx *core.Context, l Reply, out *ReplicaOutput) {
	i := r.versionOf(l.ID)
	if i < 0 {
		return
	}
	if l.Err != nil {
		r.fail(ctx, i, statusOf(l.Err), 0, 0, out)
		return
	}
	r.vers[i].logged = true
	r.tryCommit(ctx, i)
}

func statusOf(err error) Status {
	switch {
	case err == nil:
		return StatusOK
	case errors.Is(err, disklog.ErrNoSpace):
		return StatusNoSpace
	case errors.Is(err, ErrVersion):
		return StatusMismatch
	}
	return StatusRefused
}

// ack takes a member's acknowledgment of a version.
func (r *Replica) ack(ctx *core.Context, peer core.NodeID, rp *Response, out *ReplicaOutput) {
	i := r.versionBySeq(rp.ID)
	if i < 0 {
		return
	}
	v := &r.vers[i]
	k := -1
	for j := range v.n {
		if r.addr(v.members[j]) == peer {
			k = j
		}
	}
	if k < 0 {
		return
	}
	v.out &^= 1 << k
	switch {
	case rp.Status == StatusOK && rp.Version == v.u.Version:
		v.acked |= 1 << k
		r.tryCommit(ctx, i)
	case rp.Status != StatusOK:
		if rp.Status == StatusRefused && rp.Epoch > r.table.Epoch {
			out.fetch(rp.Epoch, false)
		}
		r.fail(ctx, i, rp.Status, rp.Epoch, rp.Interval, out)
	}
	r.release(ctx, i)
}

func (r *Replica) tryCommit(ctx *core.Context, i int) {
	v := &r.vers[i]
	if v.status != StatusOK || !v.logged || v.commit || v.acked != 1<<v.n-1 {
		return
	}
	v.commit = true
	r.st.Commit(ctx, v.u.Chunk, v.u.Version, &r.sout)
}

// fail fails the version and every later one of its chunk, which may hold its bytes, so none of them commits.
func (r *Replica) fail(ctx *core.Context, i int, st Status, epoch uint64, interval uint32, out *ReplicaOutput) {
	v := &r.vers[i]
	if v.status != StatusOK || v.commit {
		return
	}
	c, from := v.u.Chunk, v.u.Version
	for j := range r.vers {
		w := &r.vers[j]
		if w.u.Chunk == c && w.u.Version >= from && w.u.Version != 0 && !w.commit && w.status == StatusOK {
			w.status, w.refEp, w.refIv = st, epoch, interval
		}
	}
	r.st.Abort(ctx, c, from, &r.sout)
}

// finished answers a request the store completed: a client's write or punch once its version is mapped or failed, a member's once durable, a read with its blocks.
func (r *Replica) finished(ctx *core.Context, rp Reply, out *ReplicaOutput) {
	q, ok := r.takeReq(rp.ID)
	if !ok {
		return
	}
	switch q.kind {
	case reqClient:
		resp := Response{Kind: KindReply, Status: statusOf(rp.Err), Version: rp.Version}
		ref := r.refusal(q.pg)
		if i := r.versionOf(rp.ID); i >= 0 {
			v := &r.vers[i]
			if v.status != StatusOK {
				resp.Status = v.status
			}
			if v.refEp > ref.Epoch {
				ref.Epoch, ref.Interval = v.refEp, v.refIv
			}
			v.done = true
			r.release(ctx, i)
		}
		if resp.Status != StatusOK {
			resp.Version = 0
		}
		if resp.Status == StatusRefused {
			resp.Epoch, resp.Interval = ref.Epoch, ref.Interval
		}
		r.answer(ctx, q.peer, q.msgID, &resp, core.NoBuf, out)
	case reqMember:
		resp := Response{Kind: KindAck, Status: statusOf(rp.Err), Version: rp.Version}
		if j := slices.IndexFunc(r.dups, func(d dup) bool { return d.id == rp.ID }); j >= 0 {
			resp.Version = r.dups[j].version
			r.dups = slices.Delete(r.dups, j, j+1)
		}
		if resp.Status == StatusMismatch {
			resp.Status = StatusRefused
		}
		if resp.Status != StatusOK {
			ref := r.refusal(q.pg)
			resp.Version, resp.Epoch, resp.Interval = 0, ref.Epoch, ref.Interval
		}
		r.answer(ctx, q.peer, q.msgID, &resp, core.NoBuf, out)
	case reqRead:
		resp := Response{Kind: KindReadReply, Version: rp.Version}
		if rp.Err != nil {
			ref := r.refusal(q.pg)
			resp.Status, resp.Version, resp.Epoch, resp.Interval = StatusRefused, 0, ref.Epoch, ref.Interval
		}
		r.answer(ctx, q.peer, q.msgID, &resp, rp.Buf, out)
	}
}

// release drops a finished version once no member's send of its buffer is outstanding.
func (r *Replica) release(ctx *core.Context, i int) {
	if v := &r.vers[i]; v.done && v.out == 0 {
		freeBuf(ctx, v.buf)
		r.vers = slices.Delete(r.vers, i, i+1)
	}
}

func (r *Replica) peerLost(peer core.NodeID) {
	for i := len(r.vers) - 1; i >= 0; i-- {
		v := &r.vers[i]
		for k := range v.n {
			if r.addr(v.members[k]) == peer {
				v.out &^= 1 << k
			}
		}
	}
}

// reply answers a client's write or punch.
func (r *Replica) reply(ctx *core.Context, peer core.NodeID, id core.RequestID, rp Response, out *ReplicaOutput) {
	rp.Kind = KindReply
	r.answer(ctx, peer, id, &rp, core.NoBuf, out)
}

func (r *Replica) answer(ctx *core.Context, peer core.NodeID, id core.RequestID, rp *Response, buf core.IOBuf, out *ReplicaOutput) {
	rp.ID = id
	r.frame = AppendResponse(r.frame[:0], rp)
	a := core.Action{Kind: core.ActNetSend, Peer: peer, Reply: true}
	a.Msg.Len, a.Msg.Buf = uint16(copy(a.Msg.Body[:], r.frame)), buf
	out.Actions = append(out.Actions, a)
}
