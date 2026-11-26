package chunkstore

import (
	"fmt"
	"maps"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/trace"
)

const (
	invLateLog       = "no member logs a write tagged with an earlier interval of its PG than its table's"
	invSameInterval  = "no write is refused for its interval while its PG's interval has not changed"
	invForward       = "no node's table epoch or PG interval number goes backwards"
	invActivations   = "no PG's last activated interval goes down, and every activation a table records happened"
	invDiskIDs       = "no disk ID names two log IDs, and no log ID is up under two disk IDs"
	invKeptAfterDrop = "no version peering keeps follows a dropped version of its chunk"
	invActivates     = "once membership stops changing, every PG whose current interval has at least min_live live members on running nodes activates it"
	invAckCopies     = "no write is acknowledged on fewer than min_live disks that list nothing of its chunk, a member in maintenance counting as one"
	invHasSource     = "an activated primary has a source of each chunk it kept, or calls it unfound"
	invLeftGated     = "no running disk keeps a write gated of a PG its table names it in no interval of since the last activated"
	invHeldOrdered   = "once the run settles, no active primary holds a client write"
	invAsks          = "once the run settles, no running primary still leads an interval a failed version or a restart ended"
)

// heldWrites is the first active lead on a running node that holds a client write (property 22, checked once the run settles).
func (c *rcluster) heldWrites() error {
	for n, r := range c.reps {
		if r == nil || !c.s.Node(uint32(n)).Up() {
			continue
		}
		for i := range r.leads {
			if l := &r.leads[i]; l.active && len(l.held) > 0 {
				return fmt.Errorf("%s: disk %d holds %d client writes of %v", invHeldOrdered, c.ids[n], len(l.held), l.pg)
			}
		}
	}
	return nil
}

// leftGated is the first running disk whose store keeps gated a write of a PG the cluster's table names it in no
// interval of since the last activated, but one whose writes there are still unresolved.
func (c *rcluster) leftGated() error {
	for n, rep := range c.reps {
		if rep == nil || !c.s.Node(uint32(n)).Up() || !rep.st.Serving() {
			continue
		}
		p := c.table.pool(rep.st.cfg.Pool)
		if p == nil {
			continue
		}
		for _, ch := range rep.st.Unsettled(nil) {
			i := core.PGIndexOf(ch.Hash(), uint32(len(p.PGs)))
			if !unresolved(rep.st, ch) && !namesDisk(&p.PGs[i], c.ids[n]) {
				return fmt.Errorf("%s: disk %d keeps %v of PG %d gated", invLeftGated, c.ids[n], ch, i)
			}
		}
	}
	return nil
}

// namesDisk reports whether e names disk in its current interval or in a past one since its last activated.
func namesDisk(e *PGEntry, disk uint32) bool {
	in := func(ms []Member) bool { return slices.ContainsFunc(ms, func(m Member) bool { return m.Disk == disk }) }
	return in(e.Members) || slices.ContainsFunc(e.Past, func(p PastInterval) bool { return p.Interval >= e.Activated && in(p.Members) })
}

// unresolved reports whether a write or punch of chunk c in st waits to be ordered or has records neither durable nor failed.
func unresolved(st *Store, c core.ChunkID) bool {
	ci, ok := st.chunkOf[c]
	if !ok {
		return false
	}
	cs := &st.chunks[ci]
	return len(cs.waiting) > 0 || slices.ContainsFunc(cs.pending, func(oi int32) bool { return st.ops[oi].left > 0 })
}

// inactive is the first PG of c's table with min_live live members whose primary runs but has not activated its current interval.
func (c *rcluster) inactive() error {
	for pi := range c.table.Pools {
		p := &c.table.Pools[pi]
		for i := range p.PGs {
			e := &p.PGs[i]
			d, ok := e.primary()
			n := c.node(d)
			if !ok || e.live() < int(p.MinLive) || n < 0 || !c.s.Node(uint32(n)).Up() || !c.reps[n].Store().Serving() {
				continue
			}
			r := c.reps[n]
			if j := r.lead(core.PGID{Pool: p.ID, Index: uint32(i)}); j < 0 || r.leads[j].interval != e.Interval || !r.leads[j].active {
				return fmt.Errorf("%s: PG %d of pool %d has not activated interval %d on disk %d", invActivates, i, p.ID, e.Interval, d)
			}
		}
	}
	return nil
}

// askOpen reports whether a running primary asked for the interval after one its table still has.
func (c *rcluster) askOpen() bool {
	for n, r := range c.reps {
		if !c.s.Node(uint32(n)).Up() {
			continue
		}
		for _, l := range r.leads {
			if _, e := c.table.entry(l.pg); l.asking && l.asked && e != nil && e.Interval == l.interval {
				return true
			}
		}
	}
	return false
}

// stillAsking is the first running primary of c's table still leading the interval of a PG a failed version ended: it never asked
// for the next, or its ask was never answered.
func (c *rcluster) stillAsking() error {
	for n, r := range c.reps {
		if !c.s.Node(uint32(n)).Up() {
			continue
		}
		for _, l := range r.leads {
			if _, e := c.table.entry(l.pg); l.asking && e != nil && e.Interval == l.interval {
				return fmt.Errorf("%s: disk %d still leads interval %d of PG %d, asked %v", invAsks, c.ids[n], l.interval, l.pg.Index, l.asked)
			}
		}
	}
	return nil
}

// epochInvariants are the checks epochChecks finds broken as replicas take events.
var epochInvariants = []string{invLateLog, invSameInterval, invForward, invKeptAfterDrop, invAckCopies}

// epochChecks holds a cluster to the epoch change model's properties 3, 5, 7, 9 and 17 and to the disk record's identities, after every step.
type epochChecks struct {
	c   *rcluster
	rec *DiskRecord
	// seen is the member update each replica is taking, with its state for the update's PG before.
	seen []memberSeen
	// epochs and intervals are the newest table epoch and PG interval numbers each node held; activated the last activated interval of each PG the cluster's tables recorded; logs each disk ID's log ID.
	epochs    []uint64
	intervals map[nodePG]uint32
	activated map[core.PGID]uint32
	logs      map[uint32]uint64
	// decisions is each activated interval's kept version of each chunk, by PG.
	decisions map[core.PGID]map[uint32][]keptChunk
	errs      map[string]error
	// writes is the chunk of each client write or punch a primary took, updates of each member update a node took; held the disks that acknowledged each version, which a disk restarting cannot show by its index.
	writes  map[core.RequestID]core.ChunkID
	updates map[updateAt]core.ChunkID
	held    map[heldAt]bool
	// acked is the newest version of each chunk acknowledged to a client; clean the newest each disk held listing nothing of the chunk as it was acknowledged or as a recovery brought it.
	acked map[core.ChunkID]uint64
	clean map[diskChunk]uint64
	// prims is the primary disk of each PG interval the cluster's table recorded.
	prims map[pgInterval]uint32
	// tears is set for a run whose crashes leave versions in part, where every copy can be torn and property 19 is not claimed.
	tears bool
}

type pgInterval struct {
	pg       core.PGID
	interval uint32
}

type diskChunk struct {
	disk  uint32
	chunk core.ChunkID
}

type nodePG struct {
	node int
	pg   core.PGID
}

type updateAt struct {
	node int
	id   core.RequestID
}

type heldAt struct {
	chunk   core.ChunkID
	version uint64
	node    int
}

type memberSeen struct {
	u        Update
	ok       bool
	interval uint32
	up       bool
	failed   bool
	newest   uint64
	next     uint64
	logging  bool
}

// watchEpochs registers the checks with c's invariants, those of rec's identities if rec is set.
func watchEpochs(c *rcluster, rec *DiskRecord) *epochChecks {
	x := &epochChecks{c: c, rec: rec, seen: make([]memberSeen, len(c.reps)), epochs: make([]uint64, len(c.reps)),
		intervals: map[nodePG]uint32{}, activated: map[core.PGID]uint32{}, logs: map[uint32]uint64{},
		decisions: map[core.PGID]map[uint32][]keptChunk{}, errs: map[string]error{},
		writes: map[core.RequestID]core.ChunkID{}, updates: map[updateAt]core.ChunkID{}, held: map[heldAt]bool{}, acked: map[core.ChunkID]uint64{}, clean: map[diskChunk]uint64{}, prims: map[pgInterval]uint32{}}
	for _, inv := range epochInvariants {
		c.inv.Register(inv, func() error { return x.errs[inv] })
	}
	c.inv.Register(invActivations, x.activations)
	c.inv.Register(invHasSource, x.sources)
	if rec != nil {
		c.inv.Register(invDiskIDs, x.identities)
	}
	c.onEvent = x.event
	return x
}

func (x *epochChecks) fail(inv string, err error) {
	if x.errs[inv] == nil {
		x.errs[inv] = fmt.Errorf("%s: %w", inv, err)
	}
}

// logging reports whether replica r has a store request for member update u under way.
func logging(r *Replica, u *Update) bool {
	return slices.ContainsFunc(r.reqs, func(q storeReq) bool {
		return q.kind == reqMember && q.chunk == u.Chunk && q.version == u.Version && !q.recover
	})
}

// event notes a replicated write or punch before replica d takes it and judges what it did with it after, and checks the replica's table and decisions after every event.
func (x *epochChecks) event(d int, ev *core.Event, done bool) {
	r := x.c.reps[d]
	if !done {
		x.seen[d] = memberSeen{}
		if ev.Kind != core.EvNetRecv || ev.Msg.Len < headSize {
			return
		}
		k := Kind(ev.Msg.Body[1])
		member := k == KindReplicaWrite || k == KindReplicaPunch
		if !member && k != KindWrite && k != KindPunch {
			return
		}
		var u Update
		data := 0
		if ev.Msg.Buf != core.NoBuf {
			data = len(x.c.s.Node(uint32(d)).Pool().Bytes(ev.Msg.Buf))
		}
		if DecodeUpdate(ev.Msg.Inline(), data, &u) != nil {
			return
		}
		if !member {
			x.writes[u.ID] = u.Chunk
			return
		}
		x.updates[updateAt{d, u.ID}] = u.Chunk
		s := memberSeen{u: u, ok: true, newest: r.st.Newest(u.Chunk), next: r.nextID, logging: logging(r, &u)}
		if _, e := r.table.entry(u.PG.PG); e != nil {
			s.interval = e.Interval
			s.up = slices.ContainsFunc(e.Members, func(m Member) bool { return m.Disk == r.cfg.Disk && m.Mark == MarkUp })
		}
		s.failed = slices.ContainsFunc(r.failed, func(f failedVer) bool {
			return f.chunk == u.Chunk && uint32(f.version>>32) == u.PG.Interval && f.version <= u.Version
		})
		x.seen[d] = s
		return
	}
	if s := x.seen[d]; s.ok {
		x.judge(d, &s)
	}
	x.forward(d)
	x.decided(r)
	x.answers(d)
	x.recovered(d)
}

// recovered notes each acknowledged chunk node d's last pass recovered as clean on d's disk at the version the recovery brought, while its index lists nothing of it.
func (x *epochChecks) recovered(d int) {
	o := x.c.reps[d]
	for _, n := range x.c.notes[d] {
		if n.Name != trace.OutRecovered || !o.Store().Serving() {
			continue
		}
		for ch := range x.acked {
			if ch.Hash() != n.Fields[0] {
				continue
			}
			if c, _ := o.Store().Index().Chunk(o.st.cfg.Pool, ch); c.ListedIn == 0 && c.Whole >= n.Fields[1] {
				k := diskChunk{o.cfg.Disk, ch}
				x.clean[k] = max(x.clean[k], n.Fields[1])
			}
		}
	}
}

// answers notes the versions node d acknowledged as a member in its last pass, and checks the disks holding each version it acknowledged to a client.
func (x *epochChecks) answers(d int) {
	passes := x.c.passes[d]
	if len(passes) == 0 {
		return
	}
	for _, a := range passes[len(passes)-1] {
		if a.Kind != core.ActNetSend || a.Msg.Len < headSize {
			continue
		}
		var rp Response
		k := Kind(a.Msg.Body[1])
		if k != KindAck && k != KindReply || DecodeResponse(a.Msg.Inline(), 0, &rp) != nil || rp.Status != StatusOK || rp.Version == 0 {
			continue
		}
		if ch, ok := x.updates[updateAt{d, rp.ID}]; ok && k == KindAck {
			x.held[heldAt{ch, rp.Version, d}] = true
		}
		if ch, ok := x.writes[rp.ID]; ok && k == KindReply {
			x.acked[ch] = max(x.acked[ch], rp.Version)
			x.copies(d, ch, rp.Version)
		}
	}
}

// copies checks that version v of ch, which node d acknowledged, is on min_live disks: d's, whose store may have stopped since it logged v, those that acknowledged it or whose index holds it, each only while its index lists nothing of the chunk or it persisted the decision of a later interval than the one answering v (as AckIv has it: v's own once every up member of it logged v, the one d's lead activated in otherwise), and those in maintenance in its interval or d's. The min_live is the lower of those two intervals', for a version acknowledged late across a change of it.
func (x *epochChecks) copies(d int, ch core.ChunkID, v uint64) {
	r := x.c.reps[d]
	p, ok := r.pgOf(ch)
	if !ok {
		return
	}
	pool, e := r.table.entry(p)
	if e == nil {
		return
	}
	need := int(pool.MinLive)
	disks := map[uint32]bool{}
	maint := func(ms []Member) {
		for _, m := range ms {
			if m.Mark == MarkMaint {
				disks[m.Disk] = true
			}
		}
	}
	maint(e.Members)
	own, logged := e.Members, e.Interval == intervalOf(v)
	if j := slices.IndexFunc(e.Past, func(q PastInterval) bool { return q.Interval == intervalOf(v) }); j >= 0 {
		need = min(need, int(e.Past[j].MinLive))
		maint(e.Past[j].Members)
		own, logged = e.Past[j].Members, true
	}
	acked := map[uint32]bool{r.cfg.Disk: true}
	for n, o := range x.c.reps {
		if o != nil && x.held[heldAt{ch, v, n}] {
			acked[o.cfg.Disk] = true
		}
	}
	for _, m := range own {
		logged = logged && (m.Mark != MarkUp || acked[m.Disk])
	}
	answer := intervalOf(v)
	if l := r.lead(p); l >= 0 && r.leads[l].active && !logged {
		answer = max(answer, r.leads[l].interval)
	}
	for n, o := range x.c.reps {
		if o == nil {
			continue
		}
		holds := x.held[heldAt{ch, v, n}] || n == d
		if o.Store().Serving() {
			c, _ := o.Store().Index().Chunk(r.st.cfg.Pool, ch)
			holds = (holds || c.Whole >= v) && (c.ListedIn == 0 || o.Store().Index().Decided(p.Pool, p.Index, uint32(len(pool.PGs))) > answer)
			if k := (diskChunk{o.cfg.Disk, ch}); holds && c.ListedIn == 0 {
				x.clean[k] = max(x.clean[k], v)
			}
		}
		if holds {
			disks[o.cfg.Disk] = true
		}
	}
	if len(disks) < need {
		x.fail(invAckCopies, fmt.Errorf("disk %d acknowledged %#x of %v on disks %v, min_live %d", r.cfg.Disk, v, ch, slices.Sorted(maps.Keys(disks)), need))
	}
}

// judge checks what replica d did with the member update it took: it logs none tagged with an interval below its table's, and refuses none before its store sees it while its table holds the update's interval, it is up in it, no earlier version of the chunk failed there and its session is not fenced.
func (x *epochChecks) judge(d int, s *memberSeen) {
	r, u := x.c.reps[d], &s.u
	if !s.logging && logging(r, u) && u.PG.Interval < s.interval {
		x.fail(invLateLog, fmt.Errorf("disk %d logs %#x of %v tagged with interval %d, its table at %d", r.cfg.Disk, u.Version, u.Chunk, u.PG.Interval, s.interval))
	}
	if u.PG.Interval != s.interval || !s.up || s.failed || u.Version <= s.newest || r.nextID != s.next || r.st.Sessions().Fenced(u.Session) {
		return
	}
	passes := x.c.passes[d]
	if len(passes) == 0 {
		return
	}
	for _, a := range passes[len(passes)-1] {
		var rp Response
		if a.Kind != core.ActNetSend || a.Msg.Len < headSize || Kind(a.Msg.Body[1]) != KindAck || DecodeResponse(a.Msg.Inline(), 0, &rp) != nil {
			continue
		}
		if rp.ID == u.ID && rp.Status != StatusOK && rp.Status != StatusNoSpace {
			x.fail(invSameInterval, fmt.Errorf("disk %d refused %#x of %v sent under epoch %d in interval %d, its table at epoch %d holding that interval", r.cfg.Disk, u.Version, u.Chunk, u.PG.Epoch, u.PG.Interval, r.table.Epoch))
		}
	}
}

// forward checks that node d's table epoch and PG interval numbers never went down, across its restarts too.
func (x *epochChecks) forward(d int) {
	t := &x.c.reps[d].table
	if t.Epoch < x.epochs[d] {
		x.fail(invForward, fmt.Errorf("node %d holds table epoch %d after %d", d, t.Epoch, x.epochs[d]))
	}
	x.epochs[d] = max(x.epochs[d], t.Epoch)
	for _, p := range t.Pools {
		for i, e := range p.PGs {
			k := nodePG{d, core.PGID{Pool: p.ID, Index: uint32(i)}}
			if e.Interval < x.intervals[k] {
				x.fail(invForward, fmt.Errorf("node %d holds %v at interval %d after %d", d, k.pg, e.Interval, x.intervals[k]))
			}
			x.intervals[k] = max(x.intervals[k], e.Interval)
		}
	}
}

// decided notes each decision r's leads activated and checks it against every earlier one: a version below an earlier activated interval that the earlier decision dropped is never kept.
func (x *epochChecks) decided(r *Replica) {
	for i := range r.leads {
		l := &r.leads[i]
		if !l.active {
			continue
		}
		byPG := x.decisions[l.pg]
		if byPG == nil {
			byPG = map[uint32][]keptChunk{}
			x.decisions[l.pg] = byPG
		}
		if _, ok := byPG[l.interval]; ok {
			continue
		}
		byPG[l.interval] = slices.Clone(l.kept)
		for _, k := range slices.Sorted(maps.Keys(byPG)) {
			earlier := byPG[k]
			if k >= l.interval {
				continue
			}
			for _, kc := range l.kept {
				j := slices.IndexFunc(earlier, func(e keptChunk) bool { return e.chunk == kc.chunk })
				if intervalOf(kc.kept) < k && (j < 0 && kc.kept != 0 || j >= 0 && kc.kept > earlier[j].kept) {
					x.fail(invKeptAfterDrop, fmt.Errorf("interval %d of %v keeps %#x of %v, which interval %d dropped", l.interval, l.pg, kc.kept, kc.chunk, k))
				}
			}
		}
	}
}

// activations checks the cluster's table: no PG's last activated interval went down, and each is one a primary activated.
func (x *epochChecks) activations() error {
	for _, r := range x.c.reps {
		x.decided(r)
	}
	for _, p := range x.c.table.Pools {
		for i, e := range p.PGs {
			pg := core.PGID{Pool: p.ID, Index: uint32(i)}
			if d, ok := e.primary(); ok {
				x.prims[pgInterval{pg, e.Interval}] = d
			}
			if e.Activated < x.activated[pg] {
				return fmt.Errorf("%s: %v last activated %d after %d", invActivations, pg, e.Activated, x.activated[pg])
			}
			if _, ok := x.decisions[pg][e.Activated]; e.Activated > x.activated[pg] && !ok {
				return fmt.Errorf("%s: %v records interval %d activated, which no primary activated", invActivations, pg, e.Activated)
			}
			x.activated[pg] = e.Activated
		}
	}
	return nil
}

// sources checks each activated lead of its table's interval: a chunk it kept that it lacks, and does not call unfound, has an up member listing nothing, or one not running to say; while the disk record has lost no disk, an unfound one with an acknowledged write has a member of any mark that held the newest clean when it was acknowledged or since a recovery and lists nothing, or only a version above it that it lacks as that version's primary, or one not running to say (property 19).
func (x *epochChecks) sources() error {
	claimed := !x.tears && x.rec != nil && !slices.ContainsFunc(x.rec.Disks(), func(d RecordedDisk) bool { return d.Gone })
	for _, r := range x.c.reps {
		if r == nil {
			continue
		}
		for i := range r.leads {
			l := &r.leads[i]
			_, e := r.table.entry(l.pg)
			if !l.active || e == nil || e.Interval != l.interval {
				continue
			}
			for _, k := range l.kept {
				if k.kept != 0 && k.lacking && !k.unfound && !slices.ContainsFunc(e.Members, func(m Member) bool { return x.canSource(m, l.pg, k.chunk, 0) }) {
					return fmt.Errorf("%s: disk %d keeps %#x of %v, every up member lists it", invHasSource, r.cfg.Disk, k.kept, k.chunk)
				}
				if a := x.acked[k.chunk]; k.unfound && claimed && a != 0 && !slices.ContainsFunc(e.Members, func(m Member) bool { return x.canSource(m, l.pg, k.chunk, a) }) {
					return fmt.Errorf("%s: disk %d calls %v unfound with every disk kept, no member holding its acknowledged %#x", invHasSource, r.cfg.Disk, k.chunk, a)
				}
			}
		}
	}
	return nil
}

// canSource reports whether member m can serve chunk ch of PG pg: if acked is 0, an up member that lists nothing of it or runs no store to say; otherwise one of any mark that held version acked clean when it was acknowledged or since a recovery and lists nothing, lists only a version above it that it lacks though it was that version's primary, or runs no store to say.
func (x *epochChecks) canSource(m Member, pg core.PGID, ch core.ChunkID, acked uint64) bool {
	if m.Mark != MarkUp && acked == 0 {
		return false
	}
	held := x.clean[diskChunk{m.Disk, ch}] >= acked
	n := x.c.node(m.Disk)
	if n < 0 || x.c.reps[n] == nil || !x.c.reps[n].Store().Serving() {
		return held
	}
	o := x.c.reps[n]
	c, _ := o.Store().Index().Chunk(o.st.cfg.Pool, ch)
	if acked == 0 {
		return c.ListedIn == 0
	}
	return held && (c.ListedIn == 0 || c.Listed > acked && c.Version < c.Listed && x.prims[pgInterval{pg, intervalOf(c.Listed)}] == m.Disk)
}

// identities checks the disk record and the disks the nodes run: each disk ID keeps the log ID it was first given, and no log ID is up under two disk IDs.
func (x *epochChecks) identities() error {
	up := map[uint64]uint32{}
	for _, d := range x.rec.Disks() {
		if l, ok := x.logs[d.ID]; ok && l != d.Log {
			return fmt.Errorf("%s: disk %d names log %#x and %#x", invDiskIDs, d.ID, l, d.Log)
		}
		x.logs[d.ID] = d.Log
		if d.Gone || !d.Up {
			continue
		}
		if o, ok := up[d.Log]; ok {
			return fmt.Errorf("%s: log %#x is up under disks %d and %d", invDiskIDs, d.Log, o, d.ID)
		}
		up[d.Log] = d.ID
	}
	ran := map[uint64]uint32{}
	for n, r := range x.c.reps {
		if r == nil || !x.c.s.Node(uint32(n)).Up() {
			continue
		}
		if l, ok := x.logs[r.cfg.Disk]; ok && l != r.cfg.Log {
			return fmt.Errorf("%s: disk %d runs log %#x, named %#x", invDiskIDs, r.cfg.Disk, r.cfg.Log, l)
		}
		x.logs[r.cfg.Disk] = r.cfg.Log
		if o, ok := ran[r.cfg.Log]; ok {
			return fmt.Errorf("%s: log %#x runs under disks %d and %d", invDiskIDs, r.cfg.Log, o, r.cfg.Disk)
		}
		ran[r.cfg.Log] = r.cfg.Disk
	}
	return nil
}
