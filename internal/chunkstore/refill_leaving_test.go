package chunkstore

import (
	"bytes"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// No write in flight: X was acknowledged on both disks of a rep2 pool. One disk is out long enough to be refilled, the other goes before the refill member has copied X and waits, the PG having a leaving member. Both old disks come back whole, both still members, X reads, and the recovered report ends the wider PG.
func TestARefillKeepsTheLastSourceOfAPG(t *testing.T) {
	if err := lastSourceRun(t, nil, true); err != nil {
		t.Fatal(err)
	}
}

// lastSourceRun runs that scenario under plant, if any, checking what the rule keeps only if strict, and returns what the simulator's invariants find.
func lastSourceRun(t *testing.T, plant func(*authority), strict bool) error {
	r := newSoleRun(t, 2, 2, 5)
	c := r.c
	if plant != nil {
		plant(r.a)
		c.planted = true
	}
	ch := chunkIn(pg0, 1, 0)
	x := bytes.Repeat([]byte{0x11}, core.BlockSize)
	id := c.write(pg0, ch, 0, x)
	c.until(id)
	r.acked[ch] = c.cl.replies[id].Version
	c.settle()
	ms := r.members()
	d1, d2 := ms[0].Disk, ms[1].Disk
	for n := range c.reps {
		c.reps[n].plant.noRecovery = true
	}
	r.lose(d2)
	r.tick(DegradedRefillAfter + 2_000_000)
	ms = r.members()
	if j := slices.IndexFunc(ms, func(m Member) bool { return m.Disk == d2 }); strict && (j < 0 || !ms[j].Leaving || len(ms) != 3) {
		t.Fatalf("after the first refill: members %v", ms)
	}
	r.lose(d1)
	r.tick(DegradedRefillAfter + 2_000_000)
	if ms = r.members(); strict && (!slices.ContainsFunc(ms, func(m Member) bool { return m.Disk == d1 && !m.Leaving }) || len(ms) != 3) {
		t.Fatalf("the second member went with one leaving: members %v", ms)
	}
	r.back(d1)
	r.back(d2)
	for n := range c.reps {
		c.reps[n].plant.noRecovery = false
	}
	r.tick(5_000_000)
	if c.found != nil || !strict {
		return c.found
	}
	if rp, got := r.read(ch); rp.Status != StatusOK || !bytes.Equal(got, x) {
		t.Fatalf("X read back %+v with two whole copies on disks %d and %d, members %v", rp, d1, d2, r.members())
	}
	if ms = r.members(); len(ms) != 2 || slices.ContainsFunc(ms, func(m Member) bool { return m.Disk == d2 }) {
		t.Fatalf("the recovered report left members %v", ms)
	}
	return nil
}

// The stale Stay: disk 2 is refilled and returns, lacking Y, acknowledged on disk 1 and on the refill member, which lists the chunk. Disk 1 goes and stays out for refill_after, and the leaving disk 2 takes its place: disk 1 stays, leaving, the only source of Y.
func TestAStayKeepsTheLastSource(t *testing.T) {
	if err := staySourceRun(t, nil); err != nil {
		t.Fatal(err)
	}
}

// staySourceRun runs that scenario under plant, if any, and returns what the simulator's invariants find.
func staySourceRun(t *testing.T, plant func(*authority)) error {
	r := newSoleRun(t, 2, 2, 5)
	c := r.c
	if plant != nil {
		plant(r.a)
		c.planted = true
	}
	ch := chunkIn(pg0, 1, 0)
	x := bytes.Repeat([]byte{0x11}, core.BlockSize)
	c.until(c.write(pg0, ch, 0, x))
	c.settle()
	ms := r.members()
	d1, d2 := ms[0].Disk, ms[1].Disk
	for n := range c.reps {
		c.reps[n].plant.noRecovery = true
	}
	r.lose(d2)
	r.tick(DegradedRefillAfter + 2_000_000)
	rp, ok := r.answered(c.write(pg0, ch, 0, bytes.Repeat([]byte{0x22}, core.BlockSize)), 5_000_000)
	if !ok || rp.Status != StatusOK {
		t.Fatalf("Y answered %+v, %v, members %v", rp, ok, r.members())
	}
	r.acked[ch] = rp.Version
	r.back(d2)
	r.tick(2_000_000)
	r.lose(d1)
	r.tick(RefillAfter + 2_000_000)
	if c.found != nil {
		return c.found
	}
	if ms = r.members(); !slices.ContainsFunc(ms, func(m Member) bool { return m.Disk == d1 && m.Leaving }) {
		t.Fatalf("members %v: disk %d leaving expected", ms, d1)
	}
	return nil
}

// leavingPG is a rep2 PG on 4 nodes with min_live 2 and members a and b, b refilled after degraded_refill_after, so that it leaves beside c.
func leavingPG(t *testing.T) (l *layout, a, b, c uint32) {
	p := rep(2, 1)
	p.MinLive = 2
	l = placed(t, nodesOf(p, 4, 1, 2))
	e := l.pools().PGs[0]
	a, b = e.Members[0].Disk, e.Members[1].Disk
	l.mark(MarkDown, b)
	l.step(t, l.inputs(0))
	l.now += DegradedRefillAfter
	l.step(t, l.inputs(0))
	ms := l.pools().PGs[0].Members
	if len(ms) != 3 || !ms[1].Leaving || ms[1].Disk != b || ms[1].Mark != MarkDown {
		t.Fatalf("after the refill of %d: %v", b, ms)
	}
	return l, a, b, ms[2].Disk
}

// A member merely down is never deleted: it leaves beside its replacement however long it is down, and goes on the recovered report for the current interval or when its disk leaves the pool.
func TestARefilledMemberLeaves(t *testing.T) {
	l, a, b, c := leavingPG(t)
	l.now += 60 * minute
	l.step(t, l.inputs(0))
	if got := disksOf(&l.pools().PGs[0]); !slices.Equal(got, []uint32{a, b, c}) {
		t.Fatalf("an hour on: %v", got)
	}
	in := l.inputs(0)
	in.Recovered = []PGInterval{{PG: core.PGID{Pool: l.pools().ID}, Interval: l.pools().PGs[0].Interval}}
	l.step(t, in)
	if got := disksOf(&l.pools().PGs[0]); !slices.Equal(got, []uint32{a, c}) {
		t.Fatalf("after the recovered report: %v", got)
	}

	l, a, b, c = leavingPG(t)
	l.remove(b)
	l.step(t, l.inputs(0))
	if got := disksOf(&l.pools().PGs[0]); !slices.Equal(got, []uint32{a, c}) {
		t.Fatalf("after its disk left the pool: %v", got)
	}
}

// A second member missing while one leaves stays a member and the PG waits, below min_live here, however long; once a disk leaves the pool the entry moves on: the leaving one goes and the waiting one leaves, or the waiting one is replaced.
func TestASecondMissingMemberWaits(t *testing.T) {
	second := func(t *testing.T) (*layout, uint32, uint32, uint32) {
		l, a, b, c := leavingPG(t)
		l.mark(MarkDown, a)
		l.step(t, l.inputs(0))
		l.now += 60 * minute
		l.step(t, l.inputs(0))
		if got := disksOf(&l.pools().PGs[0]); !slices.Equal(got, []uint32{a, b, c}) {
			t.Fatalf("an hour on: %v", got)
		}
		return l, a, b, c
	}
	l, a, b, c := second(t)
	l.remove(b)
	l.step(t, l.inputs(0))
	e := l.pools().PGs[0]
	if len(e.Members) != 3 || e.Members[0].Disk != a || !e.Members[0].Leaving || e.Members[1].Disk != c {
		t.Fatalf("leaving member's disk left the pool: %v", e.Members)
	}
	l, a, b, c = second(t)
	l.remove(a)
	l.step(t, l.inputs(0))
	e = l.pools().PGs[0]
	if len(e.Members) != 3 || e.Members[0].Disk == a || !e.Members[1].Leaving || e.Members[1].Disk != b || e.Members[2].Disk != c {
		t.Fatalf("waiting member's disk left the pool: %v", e.Members)
	}
}

// A leaving member that is up takes the place of a member merely down, which leaves in turn: the PG keeps it as a source, one member wider, until the recovered report.
func TestStayKeepsTheMissingMember(t *testing.T) {
	l, a, b, c := leavingPG(t)
	l.mark(MarkDown, a)
	l.mark(MarkUp, b)
	l.step(t, l.inputs(0))
	l.now += RefillAfter
	l.step(t, l.inputs(0))
	ms := l.pools().PGs[0].Members
	if len(ms) != 3 || ms[0].Disk != a || !ms[0].Leaving || ms[1].Disk != b || ms[1].Leaving || ms[2].Disk != c {
		t.Fatalf("members %v", ms)
	}
}

// The two old disks of a rep2 pool out of reach with the PG still serving, by a min_live of 1: X, whole on both, reads unfound, and the pool names both, the leaving one too, for an operator to bring back. With min_live 2 the PG stops instead and names them once stuck.
func TestABlockedPGNamesItsDisks(t *testing.T) {
	for _, minLive := range []int{1, 2} {
		r := newSoleRun(t, 2, minLive, 5)
		c := r.c
		ch := chunkIn(pg0, 1, 0)
		c.until(c.write(pg0, ch, 0, bytes.Repeat([]byte{0x11}, core.BlockSize)))
		c.settle()
		ms := r.members()
		d1, d2 := ms[0].Disk, ms[1].Disk
		for n := range c.reps {
			c.reps[n].plant.noRecovery = true
		}
		r.lose(d2)
		r.tick(RefillAfter + 2_000_000)
		r.lose(d1)
		r.tick(10_000_000)
		want, status := uint32(1), StatusUnfound
		if minLive == 2 {
			want, status = 0, StatusRefused
		}
		if rp, _ := r.read(ch); rp.Status != status {
			t.Fatalf("min_live %d: X read %+v, members %v", minLive, rp, r.members())
		}
		var cond Conditions
		cond.Watch(c.s.Time(), []Copying{{Unfound: c.reps[c.primary(pg0)].Unfound(pg0)}}, &c.table.Pools[0])
		if len(cond.Blocked) != 1 || cond.Blocked[0].Unfound != want || !slices.Equal(cond.Blocked[0].Disks, []uint32{d1, d2}) {
			t.Fatalf("min_live %d: %+v with %d and %d down, members %v", minLive, cond, d1, d2, r.members())
		}
	}
}

// A PG names only its down members, none where no disk is left to return, and a disk that left the pool is no member to name.
func TestBlockedNamesDownMembers(t *testing.T) {
	var c Conditions
	tbl := PoolTable{PGs: []PGEntry{{Members: []Member{{Disk: 1, Mark: MarkDown, Leaving: true}, {Disk: 2}, {Disk: 3, Mark: MarkDown}}}, {Members: []Member{{Disk: 2}, {Disk: 4}}}}}
	c.Watch(0, []Copying{{PG: 0, Unfound: 2}, {PG: 1, Unfound: 1}}, &tbl)
	if len(c.Blocked) != 2 || !slices.Equal(c.Blocked[0].Disks, []uint32{1, 3}) || c.Blocked[0].Unfound != 2 || c.Blocked[1].Disks != nil {
		t.Fatalf("blocked %+v", c.Blocked)
	}
	c.Watch(0, nil, &tbl)
	if len(c.Blocked) != 0 {
		t.Fatalf("blocked %+v with nothing reported", c.Blocked)
	}
}
