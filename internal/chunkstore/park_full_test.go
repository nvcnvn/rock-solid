package chunkstore

import (
	"bytes"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// X is acknowledged on both members of a rep2 PG on five disks. The leader takes the second member's disk at its log reserve and the PG Stuck with that member short, so a third disk joins the PG and the full member is parked beside it, with no recovered report. The first member's disk is declared lost, and replaced, before the third copies anything: the parked member still serves X.
func TestAFullMemberParkedKeepsWhatItHolds(t *testing.T) {
	if err := fullParkRun(t, nil, true); err != nil {
		t.Fatal(err)
	}
}

// fullParkRun runs that scenario under plant, if any, checking the park only if strict, and returns what the simulator's invariants find.
func fullParkRun(t *testing.T, plant func(*authority), strict bool) error {
	r := newSoleRun(t, 2, 2, 5)
	c := r.c
	if plant != nil {
		plant(r.a)
		c.planted = true
	}
	ch := chunkIn(pg0, 1, 0)
	id := c.write(pg0, ch, 0, bytes.Repeat([]byte{0x11}, core.BlockSize))
	c.until(id)
	r.acked[ch] = c.cl.replies[id].Version
	c.settle()
	ms := r.members()
	d1, d2 := ms[0].Disk, ms[1].Disk
	for n := range c.reps {
		c.reps[n].plant.noRecovery = true
	}
	r.a.full = []uint32{d2}
	r.tick(2_000_000)
	if got := r.members(); strict && (len(got) != 3 || got[0].Disk != d1 || got[1].Leaving || got[2] != (Member{Disk: d2, Leaving: true})) {
		t.Fatalf("PG 0 went from %v to %v: disk %d parked beside a new member expected", ms, got, d2)
	}
	r.lose(d1)
	r.tick(1_000_000)
	r.declare(t, d1)
	r.tick(5_000_000)
	if c.found != nil || !strict {
		return c.found
	}
	if got := r.members(); !slices.Contains(got, Member{Disk: d2, Leaving: true}) {
		t.Fatalf("PG 0 is %v once disk %d was lost: disk %d leaving expected", got, d1, d2)
	}
	return nil
}

// Both members of a rep2 PG on two disks, X acknowledged on both, are at their log reserve and short in a Stuck PG. No disk is eligible to replace either, so neither moves.
func TestFullMembersWithNoEligibleDiskStay(t *testing.T) {
	if err := fullUnpairedRun(t, nil); err != nil {
		t.Fatal(err)
	}
}

// fullUnpairedRun runs that scenario under plant, if any, and returns what the simulator's invariants find.
func fullUnpairedRun(t *testing.T, plant func(*authority)) error {
	r := newSoleRun(t, 2, 1, 2)
	c := r.c
	if plant != nil {
		plant(r.a)
		c.planted = true
	}
	ch := chunkIn(pg0, 1, 0)
	id := c.write(pg0, ch, 0, bytes.Repeat([]byte{0x11}, core.BlockSize))
	c.until(id)
	r.acked[ch] = c.cl.replies[id].Version
	c.settle()
	ms := slices.Clone(r.members())
	r.a.full = []uint32{ms[0].Disk, ms[1].Disk}
	r.tick(5_000_000)
	if c.found != nil {
		return c.found
	}
	if got := r.members(); !slices.Equal(got, ms) {
		t.Fatalf("PG 0 went from %v to %v with no eligible disk", ms, got)
	}
	return nil
}
