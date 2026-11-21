package chunkstore

import (
	"errors"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/core"
)

var (
	// ErrGiveUpCandidate reports a give-up of a PG with a member down or in maintenance whose disk is not gone, which may hold what the PG lacks.
	ErrGiveUpCandidate = errors.New("chunkstore: a member down or in maintenance may still return")
	// ErrNothingLost reports a give-up of a PG whose current interval is neither stopped nor left chunks unfound.
	ErrNothingLost = errors.New("chunkstore: the PG is neither stopped nor has unfound chunks")
)

// CheckGiveUp is the give-up of PG pg's current interval as the leader commits it, from e, pg's committed entry, gone, which
// tells the disks the record has gone, and lead, the latest report of pg's primary or nil: every chunk of the PG if the
// interval never activated and is stopped, else the unfound chunks lead names, which every node drops.
func CheckGiveUp(pg core.PGID, e *PGEntry, gone func(disk uint32) bool, lead *LeadState) (PGGiveUp, error) {
	return checkGiveUp(pg, e, gone, lead, placementPlants{})
}

func checkGiveUp(pg core.PGID, e *PGEntry, gone func(uint32) bool, lead *LeadState, plant placementPlants) (PGGiveUp, error) {
	if !plant.giveUpWithCandidate && slices.ContainsFunc(e.Members, func(m Member) bool { return m.Mark != MarkUp && !gone(m.Disk) }) {
		return PGGiveUp{}, ErrGiveUpCandidate
	}
	active := lead != nil && lead.Interval == e.Interval && lead.Active
	activated := active || e.Activated == e.Interval
	g := PGGiveUp{PG: pg, GiveUp: GiveUp{Interval: e.Interval}}
	switch {
	case !activated && !e.stopped(gone) && !plant.giveUpNotStopped, activated && !plant.giveUpFound && (!active || len(lead.Chunks) == 0):
		return PGGiveUp{}, ErrNothingLost
	case !activated:
		g.All = true
	case plant.giveUpFound:
		g.All, g.Drop = true, true
	default:
		g.Drop, g.Chunks = true, slices.Clone(lead.Chunks)
	}
	return g, nil
}

// Entry is PG pg's entry in t, nil if t lacks it.
func (t *Table) Entry(pg core.PGID) *PGEntry {
	_, e := t.entry(pg)
	return e
}

// stopped reports whether e's current interval never activated and an interval its peering needs, one since the last
// activated and the newest give-up with min_live live members and one up, has no up member left.
func (e *PGEntry) stopped(gone func(uint32) bool) bool {
	if e.Activated == e.Interval {
		return false
	}
	for _, p := range e.Past {
		x := PGEntry{Members: p.Members}
		if _, up := x.primary(); up && p.Interval >= e.Activated && p.Interval > e.givenUp() && x.live() >= int(p.MinLive) &&
			!slices.ContainsFunc(p.Members, func(m Member) bool { return m.Mark == MarkUp && !gone(m.Disk) }) {
			return true
		}
	}
	return false
}

// dropBit marks the store requests that drop a chunk a give-up records, numbered apart from the others.
const dropBit = 1 << 62

// giveUpDrop is a chunk this disk drops for a give-up of its PG's intervals up to interval: id is the store's request while it runs.
type giveUpDrop struct {
	pg       core.PGID
	chunk    core.ChunkID
	interval uint32
	id       uint64
}

// noteDrops queues the chunks of each give-up t records that dropping and old does not, as a node that adopts t after old
// drops them, whether or not it saw the table that gave up. A restart's first table queues every one; holdsGivenUp then
// leaves out a chunk dropped already.
func (r *Replica) noteDrops(old, t *Table) {
	p := t.pool(r.st.cfg.Pool)
	if p == nil {
		return
	}
	op := old.pool(p.ID)
	for i := range p.PGs {
		e := &p.PGs[i]
		var seen uint32
		if op != nil && len(op.PGs) == len(p.PGs) {
			seen = op.PGs[i].givenUp()
		}
		pg := core.PGID{Pool: p.ID, Index: uint32(i)}
		for _, g := range e.GiveUps {
			if !g.Drop || g.Interval <= seen {
				continue
			}
			cs := g.Chunks
			if g.All && r.st.ix != nil {
				r.peerChunks = r.st.ix.PGChunks(r.peerChunks[:0], p.ID, pg.Index, uint32(len(p.PGs)))
				cs = nil
				for _, c := range r.peerChunks {
					cs = append(cs, c.Chunk)
				}
			}
			for _, c := range cs {
				if !slices.ContainsFunc(r.drops, func(d giveUpDrop) bool { return d.chunk == c && d.interval >= g.Interval }) {
					r.drops = append(r.drops, giveUpDrop{pg: pg, chunk: c, interval: g.Interval})
				}
			}
		}
	}
}

// dropGivenUp has the store drop each queued chunk it holds anything of from no later than its give-up's interval, once
// it serves; a chunk that holds nothing of those intervals, as after its drop or with a write since, leaves the queue.
func (r *Replica) dropGivenUp(ctx *core.Context) {
	if !r.st.Serving() {
		return
	}
	r.drops = slices.DeleteFunc(r.drops, func(d giveUpDrop) bool { return d.id == 0 && !r.holdsGivenUp(d) })
	for i := range r.drops {
		if d := &r.drops[i]; d.id == 0 {
			r.peerID++
			d.id = dropBit | r.peerID
			r.st.Drop(ctx, d.id, d.chunk, d.interval, &r.sout)
		}
	}
}

// holdsGivenUp reports whether the store holds or lists anything of d's chunk, all of it from no later than d's interval.
func (r *Replica) holdsGivenUp(d giveUpDrop) bool {
	_, cached := r.st.chunkOf[d.chunk]
	x, indexed := r.st.ix.Chunk(r.st.cfg.Pool, d.chunk)
	return (cached || indexed) && intervalOf(r.st.Newest(d.chunk)) <= d.interval && !x.after(d.interval)
}

// dropped takes the store's answer to a drop: done, or asked again on the next drain.
func (r *Replica) dropped(rp Reply) {
	if j := slices.IndexFunc(r.drops, func(d giveUpDrop) bool { return d.id == rp.ID }); j >= 0 {
		if rp.Err == nil {
			r.drops = slices.Delete(r.drops, j, j+1)
		} else {
			r.drops[j].id = 0
		}
	}
}

// dropping reports whether a chunk of pg waits for its drop, before which this disk answers no peering of pg.
func (r *Replica) dropping(pg core.PGID) bool {
	return slices.ContainsFunc(r.drops, func(d giveUpDrop) bool { return d.pg == pg })
}
