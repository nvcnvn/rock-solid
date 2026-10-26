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

// ResendTimer is the timer on which a primary sends its members again, in order, the writes a lost connection left unacknowledged.
const ResendTimer core.TimerID = 0x72736e64

// ResendAfter is how long after a PeerLost the primary sends again: 10 ms.
const ResendAfter = core.Tick(10_000)

// ReplicaConfig is a disk's place in the cluster.
type ReplicaConfig struct {
	// Disk is the disk whose store the replica runs, by its ID in the placement table.
	Disk uint32
}

// ReplicaOutput is what a call into a Replica asks of its loop: actions, and of the control plane a table at least as new as Fetch, whole if Whole, and a new interval of each PG in Asks, whose interval a spent version counter or a failed version ended.
type ReplicaOutput struct {
	Actions []core.Action
	Fetch   uint64
	Whole   bool
	Asks    []PGFields
}

// Reset empties the output for reuse.
func (o *ReplicaOutput) Reset() {
	o.Actions, o.Fetch, o.Whole, o.Asks = o.Actions[:0], 0, false, o.Asks[:0]
}

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
	// asking is set once a chunk of the PG spent its counter or a version failed, which ends the interval: the primary orders nothing more in it and asks for the next once none of its versions is pending, setting asked. spent counts the held writes the store refused for a spent counter, which go ahead of those held since.
	asking bool
	asked  bool
	spent  int
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
	// version is the version a member's write or punch was handed.
	version uint64
}

// version is a client write or punch the primary took in interval and fans out once its store orders it: the members that must hold it, those that do, and those whose latest send, numbered in sends, has neither a reply nor a PeerLost yet; done once its client is answered.
type version struct {
	seq      core.RequestID
	req      uint64
	client   core.NodeID
	msgID    core.RequestID
	epoch    uint64
	interval uint32
	u        Update
	buf      core.IOBuf
	members  [maxMembers]uint32
	sends    [maxMembers]uint8
	n        int
	acked    uint8
	out      uint8
	logged   bool
	commit   bool
	done     bool
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
	failed []failedVer
	bases  []baseFetch
	// lost is the peers a PeerLost left with writes to send again, which new writes to them wait behind.
	lost      []core.NodeID
	unsettled []core.ChunkID
	nextID    uint64
	seq       core.RequestID
	frame     []byte
	armed     bool
	plant     replicaPlants
	// disks is each disk's size and the live blocks it last reported, by disk ID; pools the pools'
	// settings and outstanding grants; slots each pool's member slots on each disk in the table.
	disks  []diskInfo
	pools  []poolInfo
	slots  []diskSlots
	live   uint64
	liveAt core.Tick
}

type diskInfo struct {
	blocks uint64
	live   uint64
}

type poolInfo struct {
	id     core.PoolID
	pool   Pool
	grants uint64
}

type diskSlots struct {
	pool core.PoolID
	disk uint32
	n    uint64
}

// fillEvery is how often a replica sums its disk's live blocks for the fill it reports: 100 ms.
const fillEvery = core.Tick(100_000)

func (r *Replica) disk(d uint32) *diskInfo {
	if int(d) >= len(r.disks) {
		r.disks = append(r.disks, make([]diskInfo, int(d)+1-len(r.disks))...)
	}
	return &r.disks[d]
}

func (r *Replica) pool(id core.PoolID) *poolInfo {
	for i := range r.pools {
		if r.pools[i].id == id {
			return &r.pools[i]
		}
	}
	r.pools = append(r.pools, poolInfo{id: id})
	return &r.pools[len(r.pools)-1]
}

// SetDisk records a disk's size in blocks, as the membership record publishes it.
func (r *Replica) SetDisk(disk uint32, blocks uint64) { r.disk(disk).blocks = blocks }

// MemberFill records the live blocks a member disk reported on its heartbeat.
func (r *Replica) MemberFill(disk uint32, live uint64) { r.disk(disk).live = live }

// SetPool records a pool's settings; a pool never set takes each disk's default write_limit.
func (r *Replica) SetPool(id core.PoolID, p Pool) { r.pool(id).pool = p }

// SetGrants records the space grants outstanding on a pool, in blocks, as the leader's heartbeat reply carries them.
func (r *Replica) SetGrants(id core.PoolID, blocks uint64) { r.pool(id).grants = blocks }

// fill is this disk's live blocks, summed again once fillEvery has passed while the store serves.
func (r *Replica) fill(ctx *core.Context) uint64 {
	if r.st.Serving() && (r.liveAt == 0 || ctx.Now-r.liveAt >= fillEvery) {
		r.live, r.liveAt = r.st.Log().LiveBlocks(), max(ctx.Now, 1)
	}
	return r.live
}

// full reports whether a disk holding one of the PG's members has reached its pool's write_limit: its live blocks plus the pool's outstanding grants times the disk's share, rounded up, against ⌊its size in blocks × write_limit⌋.
func (r *Replica) full(ctx *core.Context, pg core.PGID, p *PoolTable, e *PGEntry) bool {
	pi := r.pool(pg.Pool)
	for _, m := range e.Members {
		d := r.disk(m.Disk)
		if d.blocks == 0 {
			continue
		}
		live := d.live
		if m.Disk == r.cfg.Disk {
			live = r.fill(ctx)
		}
		var slots uint64
		if j, ok := slices.BinarySearchFunc(r.slots, diskSlots{pool: pg.Pool, disk: m.Disk}, compareSlots); ok {
			slots = r.slots[j].n
		}
		if live+mulDivUp(pi.grants, slots, uint64(len(p.PGs))) >= d.blocks*uint64(pi.pool.limitPercent(d.blocks))/100 {
			return true
		}
	}
	return false
}

// countSlots counts each pool's member slots on each disk in the table, in (pool, disk) order.
func (r *Replica) countSlots() {
	r.slots = r.slots[:0]
	for pi := range r.table.Pools {
		p := &r.table.Pools[pi]
		for i := range p.PGs {
			for _, m := range p.PGs[i].Members {
				key := diskSlots{pool: p.ID, disk: m.Disk}
				j, ok := slices.BinarySearchFunc(r.slots, key, compareSlots)
				if !ok {
					r.slots = slices.Insert(r.slots, j, key)
				}
				r.slots[j].n++
			}
		}
	}
}

func compareSlots(a, b diskSlots) int {
	return cmp.Or(cmp.Compare(a.pool, b.pool), cmp.Compare(a.disk, b.disk))
}

type replicaPlants struct {
	// resendNewestFirst sends a member a lost connection's writes again newest first.
	resendNewestFirst bool
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
	r.countSlots()
	r.failed = slices.DeleteFunc(r.failed, func(f failedVer) bool {
		_, e := r.table.entry(f.pg)
		return e == nil || e.Interval != uint32(f.version>>32)
	})
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
	r.dropWaiting(ctx, out)
	r.demote(ctx, out)
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

// demote refuses the clients of the versions this disk ordered as the primary of a PG it no longer leads, since peering decides whether those versions stay, and has the store map them as a member maps what it logs, so the PG's new primary finds the disk a member like any other.
func (r *Replica) demote(ctx *core.Context, out *ReplicaOutput) {
	for i := len(r.vers) - 1; i >= 0; i-- {
		v := &r.vers[i]
		if v.done || v.u.Version == 0 || r.lead(v.u.PG.PG) >= 0 {
			continue
		}
		v.done = true
		r.takeReq(v.req)
		r.reply(ctx, v.client, v.msgID, r.refusal(v.u.PG.PG), out)
		r.release(ctx, i)
	}
	r.unsettled = r.st.Unsettled(r.unsettled[:0])
	for _, c := range r.unsettled {
		if pg, ok := r.pgOf(c); !ok || r.lead(pg) < 0 {
			r.st.Ungate(ctx, c, &r.sout)
		}
	}
}

// pgOf is the PG chunk c belongs to in the replica's table.
func (r *Replica) pgOf(c core.ChunkID) (core.PGID, bool) {
	p := r.table.pool(r.st.cfg.Pool)
	if p == nil {
		return core.PGID{}, false
	}
	return core.PGID{Pool: p.ID, Index: core.PGIndexOf(c.Hash(), uint32(len(p.PGs)))}, true
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

// Activate is peering's end: the PG this disk leads is now active in interval, and the client writes it held are ordered again, unless another primary led the PG since the interval they were held in. It returns ErrUnresolved, activating nothing, while a chunk of the PG has versions peering has not settled.
func (r *Replica) Activate(ctx *core.Context, pg core.PGID, interval uint32, out *ReplicaOutput) error {
	i := r.lead(pg)
	if i < 0 || r.leads[i].interval != interval {
		return nil
	}
	if r.unsettled = r.Unsettled(pg, r.unsettled[:0]); len(r.unsettled) > 0 {
		return ErrUnresolved
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
	return nil
}

// Unsettled appends to dst the chunks of pg with versions pending in the store, which, before the PG activates, are an earlier interval's for peering to settle: no member may hold them, so only this disk can name them.
func (r *Replica) Unsettled(pg core.PGID, dst []core.ChunkID) []core.ChunkID {
	n := len(dst)
	all := r.st.Unsettled(dst)
	dst = all[:n]
	for _, c := range all[n:] {
		if p, ok := r.pgOf(c); ok && p == pg {
			dst = append(dst, c)
		}
	}
	return dst
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
		r.peerLost(ctx, ev.Peer, out)
	case core.EvTimer:
		if ev.Timer == ResendTimer {
			r.resend(out)
			break
		}
		r.st.OnEvent(ctx, ev, &r.sout)
	case core.EvDiskDone:
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

// askBase sends the next up member of the PG a read of the base block at its version, or fails the write once none is left to ask: after a member's refusal or a lost connection to it.
func (r *Replica) askBase(ctx *core.Context, i int, out *ReplicaOutput) {
	f := &r.bases[i]
	_, e := r.table.entry(f.pg)
	for e != nil && f.tried < len(e.Members) {
		m := e.Members[f.tried]
		f.tried++
		if m.Mark != MarkUp || m.Disk == r.cfg.Disk {
			continue
		}
		rd := Read{ID: f.rid, PG: PGFields{Epoch: r.table.Epoch, PG: f.pg, Interval: e.Interval}, Chunk: f.Chunk,
			Off: uint32(f.Block) * core.BlockSize, Len: core.BlockSize, HasAt: true, At: f.Version}
		r.frame = AppendRead(r.frame[:0], &rd)
		a := core.Action{Kind: core.ActNetSend, Peer: r.addr(m.Disk)}
		a.Msg.Len = uint16(copy(a.Msg.Body[:], r.frame))
		out.Actions = append(out.Actions, a)
		f.asked = a.Peer
		return
	}
	id, b := f.ID, f.Block
	r.bases = slices.Delete(r.bases, i, i+1)
	r.st.SupplyBase(ctx, id, b, core.NoBuf, disklog.ErrBadChecksum, &r.sout)
}

// baseReply takes a member's answer to a base fetch, reporting whether it was one; it takes buf.
func (r *Replica) baseReply(ctx *core.Context, rp *Response, buf core.IOBuf, out *ReplicaOutput) bool {
	i := slices.IndexFunc(r.bases, func(f baseFetch) bool { return f.rid == rp.ID })
	if i < 0 {
		return false
	}
	if rp.Status != StatusOK {
		freeBuf(ctx, buf)
		r.askBase(ctx, i, out)
		return true
	}
	id, b := r.bases[i].ID, r.bases[i].Block
	r.bases = slices.Delete(r.bases, i, i+1)
	r.st.SupplyBase(ctx, id, b, buf, nil, &r.sout)
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
	case !r.leads[i].active || r.leads[i].asking:
		l := &r.leads[i]
		l.held = append(l.held, heldMsg{peer: ev.Peer, msg: ev.Msg, hold: l.interval})
		return
	case u.Kind == KindWrite && !u.Granted && r.full(ctx, u.PG.PG, p, e) && !r.st.Overwrites(u.Chunk, u.Off, u.Len, u.Snapshot):
		freeBuf(ctx, ev.Msg.Buf)
		r.reply(ctx, ev.Peer, u.ID, Response{Status: StatusNoSpace}, out)
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
	r.vers = append(r.vers, version{seq: r.seq, req: id, client: ev.Peer, msgID: u.ID, epoch: r.table.Epoch, interval: interval, u: *u})
	if u.Kind == KindWrite {
		r.st.Write(ctx, req, &r.sout)
	} else {
		freeBuf(ctx, ev.Msg.Buf)
		r.st.Punch(ctx, req, &r.sout)
	}
}

// memberUpdate takes a write or punch the PG's primary ordered: accepted only in the PG's current interval, by an up member, and refused once an earlier version of its chunk in the interval failed here, so the member logs a chunk's versions without a gap. A version sent again while its first copy is in flight is answered with that copy's outcome; one whose copy is done is refused, since after a failure or a restart the member may hold only some of its records.
func (r *Replica) memberUpdate(ctx *core.Context, ev *core.Event, u *Update, out *ReplicaOutput) {
	_, e := r.table.entry(u.PG.PG)
	if e != nil && u.PG.Interval > e.Interval && u.PG.Epoch > r.table.Epoch {
		r.park(ev, u.PG.Epoch, out)
		return
	}
	refuse := e == nil || u.PG.Interval != e.Interval || !slices.ContainsFunc(e.Members, func(m Member) bool { return m.Disk == r.cfg.Disk && m.Mark == MarkUp }) ||
		slices.ContainsFunc(r.failed, func(f failedVer) bool {
			return f.chunk == u.Chunk && f.version>>32 == u.Version>>32 && f.version <= u.Version
		})
	if !refuse && u.Version <= r.st.Newest(u.Chunk) {
		if j := slices.IndexFunc(r.reqs, func(q storeReq) bool { return q.kind == reqMember && q.chunk == u.Chunk && q.version == u.Version }); j >= 0 {
			freeBuf(ctx, ev.Msg.Buf)
			r.dups = append(r.dups, dup{orig: r.reqs[j].id, peer: ev.Peer, msgID: u.ID})
			return
		}
		r.failed = append(r.failed, failedVer{pg: u.PG.PG, chunk: u.Chunk, version: u.Version})
		refuse = true
	}
	if refuse {
		freeBuf(ctx, ev.Msg.Buf)
		rp := r.refusal(u.PG.PG)
		rp.Kind, rp.Fill = KindAck, r.fill(ctx)
		r.answer(ctx, ev.Peer, u.ID, &rp, core.NoBuf, out)
		return
	}
	id := r.newReq(storeReq{kind: reqMember, peer: ev.Peer, msgID: u.ID, pg: u.PG.PG, chunk: u.Chunk, version: u.Version})
	req := Request{ID: id, Chunk: u.Chunk, Off: u.Off, Len: u.Len, Buf: ev.Msg.Buf, Epoch: u.Snapshot, Version: u.Version}
	if u.Kind == KindReplicaWrite {
		r.st.Write(ctx, req, &r.sout)
	} else {
		r.st.Punch(ctx, req, &r.sout)
	}
}

// baseFetch is a base block the primary asks another member for, tried from member tried of the PG, under read ID rid, of the loop at asked.
type baseFetch struct {
	BaseFetch
	pg    core.PGID
	tried int
	rid   core.RequestID
	asked core.NodeID
}

// dup is a write sent again to a member while its first copy, store request orig, is in flight; it is answered as that copy is.
type dup struct {
	orig  uint64
	peer  core.NodeID
	msgID core.RequestID
}

// failedVer is a version of a chunk a member failed to log, after which it refuses the chunk's later versions in that interval.
type failedVer struct {
	pg      core.PGID
	chunk   core.ChunkID
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

// versionOf finds a version by its store request, which, like seq, grows in the order r.vers holds them.
func (r *Replica) versionOf(req uint64) int {
	i, ok := slices.BinarySearchFunc(r.vers, req, func(v version, req uint64) int { return cmp.Compare(v.req, req) })
	if !ok {
		return -1
	}
	return i
}

func (r *Replica) versionBySeq(seq core.RequestID) int {
	i, ok := slices.BinarySearchFunc(r.vers, seq, func(v version, seq core.RequestID) int { return cmp.Compare(v.seq, seq) })
	if !ok {
		return -1
	}
	return i
}

// drain handles what the store answered until it has nothing more to say: the versions it ordered, those durable on its disk and the requests it finished. A rebuild of the store's index ends the interval of every PG this disk leads, as a restart would: the rebuilt index maps what the log holds, versions peering has yet to decide included.
func (r *Replica) drain(ctx *core.Context, out *ReplicaOutput) {
	for o, l, p, b := 0, 0, 0, 0; o < len(r.sout.Ordered) || l < len(r.sout.Logged) || p < len(r.sout.Replies) || b < len(r.sout.Bases); {
		for ; b < len(r.sout.Bases); b++ {
			r.nextID++
			f := baseFetch{BaseFetch: r.sout.Bases[b], rid: core.RequestID(r.nextID)}
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
	if r.sout.Rebuilt {
		for i := range r.leads {
			r.leads[i].active, r.leads[i].asking = false, true
		}
	}
	out.Actions = append(out.Actions, r.sout.Actions...)
	r.sout.Reset()
	r.ask(out)
}

// ordered fans a version out to every up member of its interval but this disk, in parallel with the store logging it, unless the interval has ended since the client's write was taken or the PG is wider than maxMembers: then the version fails at once. A member a resend is due to gets it with the resend.
func (r *Replica) ordered(ctx *core.Context, o Reply, out *ReplicaOutput) {
	i := r.versionOf(o.ID)
	v := &r.vers[i]
	v.u.Version, v.buf = o.Version, o.Buf
	l := r.lead(v.u.PG.PG)
	if l < 0 || r.leads[l].asking || r.leads[l].interval != uint32(o.Version>>32) {
		r.fail(ctx, i, Response{}, out)
		return
	}
	_, e := r.table.entry(v.u.PG.PG)
	for _, m := range e.Members {
		if m.Mark != MarkUp || m.Disk == r.cfg.Disk {
			continue
		}
		if v.n == maxMembers {
			r.fail(ctx, i, Response{}, out)
			return
		}
		v.members[v.n] = m.Disk
		v.n++
	}
	for k := range v.n {
		if !slices.Contains(r.lost, r.addr(v.members[k])) {
			r.send(v, k, out)
		}
	}
}

// sendShift places a send's number in the ID of the update it carries, above the version's sequence, so an answer to a send a lost connection left behind is told from the answer to the send after it, whose buffer is still in the network.
const sendShift = 56

func (r *Replica) send(v *version, k int, out *ReplicaOutput) {
	v.sends[k]++
	u := v.u
	u.ID, u.HasExpect, u.Expect, u.Granted = v.seq|core.RequestID(v.sends[k])<<sendShift, false, 0, false
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
	if r.vers[i].u.Version == 0 {
		r.vers[i].u.Version = l.Version
	}
	if l.Err != nil {
		r.fail(ctx, i, Response{}, out)
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

// ack takes a member's acknowledgment of a version, which ends its send only if it answers the latest.
func (r *Replica) ack(ctx *core.Context, peer core.NodeID, rp *Response, out *ReplicaOutput) {
	seq := rp.ID & (1<<sendShift - 1)
	i := r.versionBySeq(seq)
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
	if uint8(rp.ID>>sendShift) == v.sends[k] {
		v.out &^= 1 << k
	}
	r.disk(v.members[k]).live = rp.Fill
	switch {
	case rp.Status == StatusOK && rp.Version == v.u.Version:
		v.acked |= 1 << k
		r.tryCommit(ctx, i)
	case rp.Status != StatusOK:
		if rp.Status == StatusRefused && rp.Epoch > r.table.Epoch {
			out.fetch(rp.Epoch, false)
		}
		r.fail(ctx, i, *rp, out)
	}
	if i = r.versionBySeq(seq); i >= 0 {
		r.release(ctx, i)
	}
}

func (r *Replica) tryCommit(ctx *core.Context, i int) {
	v := &r.vers[i]
	if v.done || !v.logged || v.commit || v.acked != 1<<v.n-1 {
		return
	}
	v.commit = true
	r.st.Commit(ctx, v.u.Chunk, v.u.Version, &r.sout)
}

// fail ends a version that failed and every later one of its chunk, which may hold its bytes, so none of them commits: their clients are refused, with ref's table if it is newer, since peering decides whether they stay, and the PG's interval ends, as for a spent counter. The store keeps them pending until Settle.
func (r *Replica) fail(ctx *core.Context, i int, ref Response, out *ReplicaOutput) {
	v := &r.vers[i]
	if v.done || v.commit {
		return
	}
	c, from, pg := v.u.Chunk, v.u.Version, v.u.PG.PG
	if l := r.lead(pg); l >= 0 && r.leads[l].interval == uint32(from>>32) {
		r.leads[l].asking = true
	}
	if own := r.refusal(pg); ref.Epoch <= own.Epoch {
		ref = own
	}
	ref.Kind, ref.Status, ref.Version, ref.Fill = KindReply, StatusRefused, 0, 0
	for j := len(r.vers) - 1; j >= 0; j-- {
		w := &r.vers[j]
		if w.u.Chunk != c || w.u.Version < from || w.done || w.commit {
			continue
		}
		w.done = true
		r.takeReq(w.req)
		r.answer(ctx, w.client, w.msgID, &ref, core.NoBuf, out)
		r.release(ctx, j)
	}
	r.dropWaiting(ctx, out)
}

// dropWaiting refuses the client writes and punches still waiting in the store, for a base read or behind one, whose PG's interval has ended since they were taken, and has the store drop them, so none is ordered in an interval that ended. One the store has already ordered or answered is left to that answer.
func (r *Replica) dropWaiting(ctx *core.Context, out *ReplicaOutput) {
	for i := len(r.vers) - 1; i >= 0; i-- {
		v := &r.vers[i]
		if v.done || v.u.Version != 0 {
			continue
		}
		if l := r.lead(v.u.PG.PG); l >= 0 && !r.leads[l].asking && r.leads[l].interval == v.interval || !r.st.Cancel(ctx, v.u.Chunk, v.req, &r.sout) {
			continue
		}
		v.done = true
		r.takeReq(v.req)
		for j := len(r.bases) - 1; j >= 0; j-- {
			if f := r.bases[j]; f.ID == v.req {
				r.bases = slices.Delete(r.bases, j, j+1)
				r.st.SupplyBase(ctx, f.ID, f.Block, core.NoBuf, ErrDropped, &r.sout)
			}
		}
		r.reply(ctx, v.client, v.msgID, r.refusal(v.u.PG.PG), out)
		r.release(ctx, i)
	}
}

// finished answers a request the store completed: a client's write or punch once its version is mapped or failed, a member's once durable, a read with its blocks.
func (r *Replica) finished(ctx *core.Context, rp Reply, out *ReplicaOutput) {
	q, ok := r.takeReq(rp.ID)
	if !ok {
		return
	}
	switch q.kind {
	case reqClient:
		i := r.versionOf(rp.ID)
		if i >= 0 && errors.Is(rp.Err, ErrCounter) {
			r.counterSpent(ctx, i, rp.Buf, out)
			return
		}
		resp := Response{Kind: KindReply, Status: statusOf(rp.Err), Version: rp.Version}
		if resp.Status != StatusOK {
			resp.Version = 0
		}
		if resp.Status == StatusRefused {
			ref := r.refusal(q.pg)
			resp.Epoch, resp.Interval = ref.Epoch, ref.Interval
		}
		if i >= 0 {
			r.vers[i].done = true
			r.release(ctx, i)
		}
		r.answer(ctx, q.peer, q.msgID, &resp, core.NoBuf, out)
	case reqMember:
		resp := Response{Kind: KindAck, Status: statusOf(rp.Err), Version: rp.Version, Fill: r.fill(ctx)}
		if resp.Status == StatusMismatch {
			resp.Status = StatusRefused
		}
		if resp.Status != StatusOK {
			ref := r.refusal(q.pg)
			resp.Version, resp.Epoch, resp.Interval = 0, ref.Epoch, ref.Interval
			r.failed = append(r.failed, failedVer{pg: q.pg, chunk: q.chunk, version: q.version})
		}
		r.answer(ctx, q.peer, q.msgID, &resp, core.NoBuf, out)
		for j := 0; j < len(r.dups); {
			if d := r.dups[j]; d.orig == q.id {
				r.answer(ctx, d.peer, d.msgID, &resp, core.NoBuf, out)
				r.dups = slices.Delete(r.dups, j, j+1)
				continue
			}
			j++
		}
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

// counterSpent holds a client write whose chunk spent its counter in the PG's interval, its buffer handed back, until the PG has a new interval: the PG's next writes are held behind it, and once its pending writes are done the primary asks for the interval. A write of a PG this disk no longer leads is refused.
func (r *Replica) counterSpent(ctx *core.Context, i int, buf core.IOBuf, out *ReplicaOutput) {
	v := r.vers[i]
	r.vers = slices.Delete(r.vers, i, i+1)
	l := r.lead(v.u.PG.PG)
	if l < 0 {
		freeBuf(ctx, buf)
		r.reply(ctx, v.client, v.msgID, r.refusal(v.u.PG.PG), out)
		return
	}
	h := heldMsg{peer: v.client, hold: r.leads[l].interval}
	h.msg.Len, h.msg.Buf = uint16(copy(h.msg.Body[:], AppendUpdate(r.frame[:0], &v.u))), buf
	ld := &r.leads[l]
	ld.held = slices.Insert(ld.held, ld.spent, h)
	ld.spent++
	ld.asking = true
	r.dropWaiting(ctx, out)
}

// ask asks for the next interval of each PG whose interval ended, once none of its versions waits for a member or its own log: a committed version may wait behind a failed one, which only Settle resolves.
func (r *Replica) ask(out *ReplicaOutput) {
	for i := range r.leads {
		l := &r.leads[i]
		if !l.asking || l.asked || slices.ContainsFunc(r.vers, func(v version) bool { return v.u.PG.PG == l.pg && !v.done && !v.commit }) {
			continue
		}
		l.asked = true
		out.Asks = append(out.Asks, PGFields{Epoch: r.table.Epoch, PG: l.pg, Interval: l.interval})
	}
}

// peerLost ends every send to the peer and has the writes it left unacknowledged sent again after ResendAfter, new writes to it waiting behind them; a base asked of it is asked of the next member.
func (r *Replica) peerLost(ctx *core.Context, peer core.NodeID, out *ReplicaOutput) {
	for i := len(r.bases) - 1; i >= 0; i-- {
		if r.bases[i].asked == peer {
			r.askBase(ctx, i, out)
		}
	}
	lost := false
	for i := len(r.vers) - 1; i >= 0; i-- {
		v := &r.vers[i]
		for k := range v.n {
			if r.addr(v.members[k]) == peer {
				v.out &^= 1 << k
				lost = lost || v.acked&(1<<k) == 0 && !v.done
			}
		}
		r.release(ctx, i)
	}
	if lost && !slices.Contains(r.lost, peer) {
		r.lost = append(r.lost, peer)
	}
	if lost && !r.armed {
		r.armed = true
		out.Actions = append(out.Actions, core.Action{Kind: core.ActSetTimer, Timer: ResendTimer, At: ctx.Now + ResendAfter})
	}
}

// resend sends each member, in the order they were ordered, so a chunk's in version order, the writes of the PGs' current intervals it has neither acknowledged nor has a send of outstanding; a write of an interval that ended is left to the next interval's peering.
func (r *Replica) resend(out *ReplicaOutput) {
	r.armed, r.lost = false, r.lost[:0]
	for n := range r.vers {
		i := n
		if r.plant.resendNewestFirst {
			i = len(r.vers) - 1 - n
		}
		v := &r.vers[i]
		_, e := r.table.entry(v.u.PG.PG)
		if v.done || v.u.Version == 0 || e == nil || uint32(v.u.Version>>32) != e.Interval {
			continue
		}
		for k := range v.n {
			if v.acked&(1<<k) == 0 && v.out&(1<<k) == 0 {
				r.send(v, k, out)
			}
		}
	}
}

// ErrActive reports a Settle of a chunk whose PG this disk leads and has activated in its current interval, where the chunk's next versions could take the numbers of those it drops.
var ErrActive = errors.New("chunkstore: settle of a chunk its primary has activated")

// Settle is peering's decision on chunk c, made before this disk activates as primary in a new interval: the chunk's pending versions up to keep are mapped and their clients answered, and the later ones are dropped and their clients refused. It settles nothing and returns ErrActive once the PG is active, or ErrUnresolved while one of the chunk's writes waits to be ordered or its records here are neither durable nor failed, which peering waits for, since a version whose local write then failed would be kept torn.
func (r *Replica) Settle(ctx *core.Context, c core.ChunkID, keep uint64, out *ReplicaOutput) error {
	if pg, ok := r.pgOf(c); ok {
		if l := r.lead(pg); l >= 0 && r.leads[l].active {
			return ErrActive
		}
	}
	if err := r.st.Settle(ctx, c, keep, &r.sout); err != nil {
		return err
	}
	for i := range r.vers {
		if v := &r.vers[i]; v.u.Chunk == c && v.u.Version != 0 && v.u.Version <= keep {
			v.commit = true
		}
	}
	r.drain(ctx, out)
	return nil
}

// reply answers a client's write or punch.
func (r *Replica) reply(ctx *core.Context, peer core.NodeID, id core.RequestID, rp Response, out *ReplicaOutput) {
	rp.Kind = KindReply
	r.answer(ctx, peer, id, &rp, core.NoBuf, out)
}

func (r *Replica) answer(ctx *core.Context, peer core.NodeID, id core.RequestID, rp *Response, buf core.IOBuf, out *ReplicaOutput) {
	rp.ID = id
	r.frame = AppendResponse(r.frame[:0], rp)
	a := core.Action{Kind: core.ActNetSend, Peer: peer, Reply: rp.Kind != KindAck}
	a.Msg.Len, a.Msg.Buf = uint16(copy(a.Msg.Body[:], r.frame)), buf
	out.Actions = append(out.Actions, a)
}
