package meta

import (
	"cmp"
	"math"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
)

// Space grants are design.md's "Space grants" in "Client design".

// WritebackBuffer is writeback_buffer in blocks: the most one grant gives.
const WritebackBuffer = uint64(256<<20) / core.BlockSize

// ReuseBudget is k, the grants' worth of flushes a daemon reuses its grant for between answered heartbeats.
const ReuseBudget = chunkstore.ReuseBudget

// FillCover is how long after the leader hears a flush reported a disk's fill it receives must
// arrive to include it: a fill is summed at most 100 ms before its heartbeat is sent.
const FillCover = core.Tick(1_000_000)

// PoolBeat is what a session heartbeat says of one pool, in blocks: the node's ask, what it holds
// buffered, the grant it holds, and its cumulative count of blocks flushed.
type PoolBeat struct {
	Pool                         core.PoolID
	Ask, Buffered, Held, Flushed uint64
}

// Issuer is the leader lease that answers session heartbeats: the control-plane group's term and
// the lease's start on its leader's CLOCK_MONOTONIC_RAW, which only grows within a term.
type Issuer struct {
	Term  uint64
	Start core.Tick
}

// PoolGrant is a pool's grant in a session heartbeat's answer, in blocks.
type PoolGrant struct {
	Pool  core.PoolID
	Grant uint64
}

// grantEntry is a session's grant in a pool: its last ask and buffered blocks, the grant last
// sent, what the leader counts it at, and the flush count last heard.
type grantEntry struct {
	session                              uint64
	pool                                 core.PoolID
	ask, buffered, grant, count, flushed uint64
}

// poolGrants is a pool's room for grants as of the last Step, its flushes not yet in the fills
// the leader holds, and its capacity figures and conditions.
type poolGrants struct {
	id    core.PoolID
	ready bool
	// room is the pool's available figure before grants, gap its smallest g_d, level its water
	// level, and cover the oldest fill of its disks.
	room, gap, level uint64
	cover            core.Tick
	flushes          []flushAt
	figures          chunkstore.Figures
	cond             chunkstore.Conditions
}

type flushAt struct {
	at     core.Tick
	blocks uint64
}

// diskFill is what the leader heard of a disk: its live blocks, in all and by pool, and when, and
// its size and log reserve in blocks from its node's report.
type diskFill struct {
	live, blocks, reserve uint64
	pools                 []chunkstore.PoolLive
	at                    core.Tick
	heard                 bool
}

// own is pool id's live blocks on the disk, never above its live blocks or its size.
func (f *diskFill) own(id core.PoolID, blocks uint64) uint64 {
	i, ok := slices.BinarySearchFunc(f.pools, id, func(p chunkstore.PoolLive, id core.PoolID) int { return cmp.Compare(p.Pool, id) })
	if !ok {
		return 0
	}
	return min(f.pools[i].Live, f.live, blocks)
}

// grantPlants are bugs a test plants: a new leader counting a session it has not heard from at
// nothing and one it has at the grant it says it holds, a lapsed session's grant dropped once
// it is fenced, grants capped by half of available only and not by the smallest gap, and a
// shrunk grant counted at its new value before a heartbeat echoes it.
type grantPlants struct {
	unheardZero, dropLapsed, noGapCap, shrinkAtOnce bool
}

// Granted is the total of grants the leader counts outstanding on pool id, which every heartbeat
// reply to a storage node carries for write_limit admission.
func (c *Control) Granted(id core.PoolID) uint64 { return c.counted(id, 0) }

// Capacity is pool id's capacity figures and conditions as of the last Step, false before the
// leader has a fill of every disk holding a member of it.
func (c *Control) Capacity(id core.PoolID) (*chunkstore.Figures, *chunkstore.Conditions, bool) {
	pg := c.poolGrant(id)
	if pg == nil || !pg.ready {
		return nil, nil, false
	}
	return &pg.figures, &pg.cond, true
}

func (c *Control) poolGrant(id core.PoolID) *poolGrants {
	i, ok := slices.BinarySearchFunc(c.pgrants, id, func(p poolGrants, id core.PoolID) int { return cmp.Compare(p.id, id) })
	if !ok {
		return nil
	}
	return &c.pgrants[i]
}

func cmpGrant(e grantEntry, k grantEntry) int {
	return cmp.Or(cmp.Compare(e.session, k.session), cmp.Compare(e.pool, k.pool))
}

func (c *Control) heardFrom(session uint64) bool {
	_, ok := slices.BinarySearch(c.heard, session)
	return ok
}

func (c *Control) hear(session uint64) {
	if i, ok := slices.BinarySearch(c.heard, session); !ok {
		c.heard = slices.Insert(c.heard, i, session)
	}
}

// each calls f with every session of the table and its entry in pool id, nil if it has none, in
// one pass over both.
func (c *Control) each(id core.PoolID, f func(s *SessionEntry, e *grantEntry)) {
	j := 0
	for i := range c.sess.Sessions {
		s := &c.sess.Sessions[i]
		for j < len(c.grants) && cmpGrant(c.grants[j], grantEntry{session: s.ID, pool: id}) < 0 {
			j++
		}
		if j < len(c.grants) && c.grants[j].session == s.ID && c.grants[j].pool == id {
			f(s, &c.grants[j])
		} else {
			f(s, nil)
		}
	}
}

// counted is the sum, over the sessions of the table but skip, of what the leader counts each at
// in pool id: its grant's count; for one this leader has not heard from, which may hold a grant of
// an earlier leader, writeback_buffer; a fenced one until its fence is acknowledged and it leaves
// the table, since a primary may still apply its granted flushes until then.
func (c *Control) counted(id core.PoolID, skip uint64) uint64 {
	var sum uint64
	c.each(id, func(s *SessionEntry, e *grantEntry) {
		switch {
		case s.ID == skip || s.Fenced != 0 && c.gplant.dropLapsed:
		case e != nil:
			sum += e.count
		case !c.heardFrom(s.ID) && !c.gplant.unheardZero:
			sum += WritebackBuffer
		}
	})
	return sum
}

// flushed is what the pool's nodes reported flushed that the fills the leader holds may not include.
func (pg *poolGrants) flushed() uint64 {
	var f uint64
	for _, x := range pg.flushes {
		f += x.blocks
	}
	return f
}

func (pg *poolGrants) flush(at core.Tick, blocks uint64) {
	if blocks == 0 {
		return
	}
	if n := len(pg.flushes); n > 0 && pg.flushes[n-1].at == at {
		pg.flushes[n-1].blocks += blocks
		return
	}
	pg.flushes = append(pg.flushes, flushAt{at, blocks})
}

// limit is the most pg's grants may total, in blocks: half its available figure before grants
// and, so (1 + k) × Σ + F fits every disk's gap, its smallest gap over 1 + k, each less F.
func (c *Control) limit(pg *poolGrants) uint64 {
	if !pg.ready {
		return 0
	}
	f := pg.flushed()
	half := (pg.room - min(f, pg.room)) / 2
	if c.gplant.noGapCap {
		return half
	}
	return min(half, (pg.gap-min(f, pg.gap))/(1+ReuseBudget))
}

// grant answers b's pools into a at t: each flush reported since the last heartbeat counts in F
// until fills cover it; a pool the session first names is counted at writeback_buffer until it
// echoes a grant of this lease, and an echoed grant at its value or what the session holds
// buffered if more; a new grant is divided by water-filling at the last step's level, never past
// the room the others leave under the pool's limit, and counted at the larger of it and the old
// count until echoed. Only a renewed session is granted anything.
func (c *Control) grant(t core.Tick, b *SessionBeat, live, renewed bool, a *SessionAnswer) {
	for i := range b.Pools {
		pb := &b.Pools[i]
		g := uint64(0)
		if pg := c.poolGrant(pb.Pool); pg != nil && live {
			g = c.grantPool(t, b.Session, b.Issuer, pb, pg, renewed)
		}
		a.Grants = append(a.Grants, PoolGrant{Pool: pb.Pool, Grant: g})
	}
	if live {
		c.hear(b.Session)
	}
}

func (c *Control) grantPool(t core.Tick, session uint64, issuer Issuer, pb *PoolBeat, pg *poolGrants, renewed bool) uint64 {
	k1 := uint64(1 + ReuseBudget)
	key := grantEntry{session: session, pool: pb.Pool}
	i, ok := slices.BinarySearchFunc(c.grants, key, cmpGrant)
	if !ok {
		// What it may have landed in the window an earlier lease's grant opened, which no fill
		// this lease holds includes yet; and until it echoes a grant of this lease, an earlier
		// lease's late reply may still reach it.
		pg.flush(t, k1*pb.Held)
		key.count, key.flushed = WritebackBuffer, pb.Flushed
		if c.gplant.unheardZero {
			key.count = pb.Held
		}
		c.grants = slices.Insert(c.grants, i, key)
	}
	e := &c.grants[i]
	if pb.Flushed >= e.flushed {
		pg.flush(t, pb.Flushed-e.flushed)
	} else {
		// A daemon that restarted without its count: what its window allowed counts until fills cover it.
		pg.flush(t, k1*e.count)
	}
	e.flushed = pb.Flushed
	if pb.Held == e.grant && issuer == c.issuer {
		e.count = e.grant
	}
	e.ask, e.buffered = min(pb.Ask, WritebackBuffer), min(pb.Buffered, WritebackBuffer)
	// It buffered under its old grant until the reply shrinking it arrived.
	e.count = max(e.count, e.buffered)
	g := uint64(0)
	if renewed {
		lim := c.limit(pg)
		others := c.counted(pb.Pool, session)
		g = min(e.ask, max(e.buffered, pg.level), lim-min(lim, others))
	}
	prev := e.grant
	e.grant, e.count = g, max(e.count, g)
	if c.gplant.shrinkAtOnce && g < prev {
		e.count = max(g, e.buffered)
	}
	return g
}

// resetGrants forgets every grant and session heard, and readies no pool until fills after the
// next step under a lease cover what was flushed before it.
func (c *Control) resetGrants() {
	c.grants, c.heard, c.grantsFrom.set = c.grants[:0], c.heard[:0], false
	for i := range c.pgrants {
		c.pgrants[i].ready = false
	}
}

// fixed is what pool id counts for the sessions that ask nothing of it on this leader: those not
// heard from and the fenced.
func (c *Control) fixed(id core.PoolID) uint64 {
	var sum uint64
	c.each(id, func(s *SessionEntry, e *grantEntry) {
		switch {
		case e != nil && s.Fenced != 0 && !c.gplant.dropLapsed:
			sum += e.count
		case e == nil && !c.heardFrom(s.ID) && !c.gplant.unheardZero:
			sum += WritebackBuffer
		}
	})
	return sum
}

// level is the water level L of pool id under target: the highest at which the live sessions'
// grants min(ask, max(buffered, L)) total at most target, 0 when their floors alone exceed it.
func (c *Control) level(id core.PoolID, target uint64) uint64 {
	fill := func(l uint64) uint64 {
		var sum uint64
		c.each(id, func(s *SessionEntry, e *grantEntry) {
			if e != nil && s.Fenced == 0 {
				sum += min(e.ask, max(e.buffered, l))
			}
		})
		return sum
	}
	lo, hi := uint64(0), WritebackBuffer
	if fill(hi) <= target {
		return hi
	}
	for lo < hi {
		mid := hi - (hi-lo)/2
		if fill(mid) <= target {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	return lo
}

// fill takes a disk heartbeat's fills at now.
func (c *Control) fill(now core.Tick, rp *chunkstore.Report) {
	for _, s := range rp.Disks {
		if id := c.diskOfLog(s.Log); id != 0 {
			f := c.diskFill(id)
			f.live, f.pools, f.at, f.heard = s.Fill, append(f.pools[:0], s.Pools...), now, true
		}
	}
}

// AppendDisks appends to dst every recorded disk not gone, as Step takes its disks: at the size and
// fill its node last reported, or at size 0 until it reports.
func (c *Control) AppendDisks(dst []chunkstore.Disk) []chunkstore.Disk {
	for _, d := range c.rec.Disks() {
		if !d.Gone {
			f := c.diskFill(d.ID)
			dst = append(dst, chunkstore.Disk{ID: d.ID, Blocks: f.blocks, Live: f.live})
		}
	}
	return dst
}

func (c *Control) diskOfLog(log uint64) uint32 {
	for _, d := range c.rec.Disks() {
		if d.Log == log && !d.Gone {
			return d.ID
		}
	}
	return 0
}

func (c *Control) diskFill(id uint32) *diskFill {
	for int(id) > len(c.fills) {
		c.fills = append(c.fills, diskFill{})
	}
	return &c.fills[id-1]
}

// grantStep sets each committed pool's room for grants and water level at now from the fills and
// asks heard, with its capacity figures and conditions over ds and the committed table; moved lists the pools whose
// table this step changes. A pool is ready once every up disk holding a member of it reported its
// size and a fill at least FillCover after this leader's first step under its lease, when no
// earlier leader answers any more, so its fills include every flush an earlier leader heard. It
// drops the flushes every such disk's fill covers, and the state of sessions that left the table.
func (c *Control) grantStep(now core.Tick, leased bool, ds []chunkstore.Disk, moved []core.PoolID) {
	if !c.grantsFrom.set && leased {
		c.grantsFrom.set, c.grantsFrom.at = true, now
	}
	in := func(id uint64) bool {
		_, ok := slices.BinarySearchFunc(c.sess.Sessions, id, cmpSession)
		return ok
	}
	c.grants = slices.DeleteFunc(c.grants, func(e grantEntry) bool { return !in(e.session) })
	c.heard = slices.DeleteFunc(c.heard, func(id uint64) bool { return !in(id) })
	var next []poolGrants
	for _, cp := range c.pools {
		pg := poolGrants{id: cp.ID}
		if old := c.poolGrant(cp.ID); old != nil {
			pg = *old
		}
		j, ok := c.tablePool(cp.ID)
		if !ok {
			continue
		}
		pt := &c.table.Pools[j]
		disks := poolInputs(cp, ds).Disks
		for k := range disks {
			disks[k].Slots = 0
		}
		for _, e := range pt.PGs {
			for _, m := range e.Members {
				if k := slices.IndexFunc(disks, func(d chunkstore.Disk) bool { return d.ID == m.Disk }); k >= 0 {
					disks[k].Slots++
				}
			}
		}
		ready, cover, gap := true, core.Tick(math.MaxUint64), uint64(math.MaxUint64)
		for k := range disks {
			d := &disks[k]
			f := c.diskFill(d.ID)
			if f.blocks != 0 {
				d.Blocks = f.blocks
			}
			if f.heard {
				d.Live = f.live
			}
			d.Own = f.own(cp.ID, d.Blocks)
			if d.Slots == 0 || d.Mark != chunkstore.MarkUp {
				continue
			}
			if !c.grantsFrom.set || !f.heard || f.blocks == 0 || f.at < c.grantsFrom.at+FillCover {
				ready = false
				continue
			}
			cover = min(cover, f.at)
		}
		pg.figures = cp.Pool.Capacity(disks, c.counted(cp.ID, 0), now)
		for _, df := range pg.figures.Disks {
			if f := c.diskFill(df.ID); slices.ContainsFunc(disks, func(d chunkstore.Disk) bool { return d.ID == df.ID && d.Mark == chunkstore.MarkUp }) {
				gap = min(gap, df.Blocks-min(max(df.Live, df.Limit)+f.reserve, df.Blocks))
			}
		}
		pg.ready, pg.room, pg.gap, pg.cover = ready && gap != math.MaxUint64, pg.figures.Ungranted, gap, cover
		if pg.ready {
			pg.flushes = slices.DeleteFunc(pg.flushes, func(x flushAt) bool { return cover >= x.at+FillCover })
		}
		pg.cond.Update(&pg.figures, !slices.Contains(moved, cp.ID) && pt.Recovered())
		c.watch.Watch(now, pt, &pg.cond)
		lim := c.limit(&pg)
		pg.level = c.level(cp.ID, lim-min(lim, c.fixed(cp.ID)))
		next = append(next, pg)
	}
	c.pgrants = next
	c.watch.Forget(&c.table)
}
