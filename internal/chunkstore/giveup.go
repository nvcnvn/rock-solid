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
