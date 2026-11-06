package chunkstore

import (
	"fmt"
	"maps"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/core"
)

const (
	invLateLog       = "no member logs a write tagged with an earlier interval of its PG than its table's"
	invSameInterval  = "no write is refused for its interval while its PG's interval has not changed"
	invForward       = "no node's table epoch or PG interval number goes backwards"
	invActivations   = "no PG's last activated interval goes down, and every activation a table records happened"
	invDiskIDs       = "no disk ID names two log IDs, and no log ID is up under two disk IDs"
	invKeptAfterDrop = "no version peering keeps follows a dropped version of its chunk"
	invActivates     = "once membership stops changing, every PG whose current interval has at least min_live live members on running nodes activates it"
	invAckCopies     = "no write is acknowledged on fewer than min_live disks, a member in maintenance counting as one"
)

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
		writes: map[core.RequestID]core.ChunkID{}, updates: map[updateAt]core.ChunkID{}, held: map[heldAt]bool{}}
	for _, inv := range epochInvariants {
		c.inv.Register(inv, func() error { return x.errs[inv] })
	}
	c.inv.Register(invActivations, x.activations)
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
			x.copies(d, ch, rp.Version)
		}
	}
}

// copies checks that version v of ch, which node d acknowledged, is on min_live disks: those that acknowledged it or whose index holds it, and those in maintenance in its interval or d's. The min_live is the lower of those two intervals', for a version acknowledged late across a change of it.
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
	if j := slices.IndexFunc(e.Past, func(q PastInterval) bool { return q.Interval == intervalOf(v) }); j >= 0 {
		need = min(need, int(e.Past[j].MinLive))
		maint(e.Past[j].Members)
	}
	for n, o := range x.c.reps {
		if o == nil {
			continue
		}
		holds := x.held[heldAt{ch, v, n}]
		if o.Store().Serving() {
			c, _ := o.Store().Index().Chunk(r.st.cfg.Pool, ch)
			holds = holds || c.Whole >= v
		}
		if holds {
			disks[o.cfg.Disk] = true
		}
	}
	if len(disks) < need {
		x.fail(invAckCopies, fmt.Errorf("disk %d acknowledged %#x of %v on disks %v, min_live %d", r.cfg.Disk, v, ch, slices.Sorted(maps.Keys(disks)), need))
	}
}

// judge checks what replica d did with the member update it took: it logs none tagged with an interval below its table's, and refuses none before its store sees it while its table holds the update's interval, it is up in it and no earlier version of the chunk failed there.
func (x *epochChecks) judge(d int, s *memberSeen) {
	r, u := x.c.reps[d], &s.u
	if !s.logging && logging(r, u) && u.PG.Interval < s.interval {
		x.fail(invLateLog, fmt.Errorf("disk %d logs %#x of %v tagged with interval %d, its table at %d", r.cfg.Disk, u.Version, u.Chunk, u.PG.Interval, s.interval))
	}
	if u.PG.Interval != s.interval || !s.up || s.failed || u.Version <= s.newest || r.nextID != s.next {
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
