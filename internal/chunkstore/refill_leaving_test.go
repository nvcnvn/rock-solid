package chunkstore

import (
	"bytes"
	"slices"
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/trace"
)

// No write in flight: X was acknowledged on both disks of a rep2 pool. One disk is out long enough to be refilled, the other goes before the refill member has copied X and is refilled too. Both old disks come back whole, both still members, X reads, and the recovered report ends the wider PG.
func TestARefillKeepsTheLastSourceOfAPG(t *testing.T) {
	if err := lastSourceRun(t, nil, true, ""); err != nil {
		t.Fatal(err)
	}
}

// lastSourceRun runs that scenario under plant, if any, checking what the rule keeps only if strict, keeping its conformance trace if named, and returns what the simulator's invariants find.
func lastSourceRun(t *testing.T, plant func(*authority), strict bool, name string) error {
	r := newSoleRun(t, 2, 2, 5)
	c := r.c
	if plant != nil {
		plant(r.a)
		c.planted = true
	}
	if name != "" {
		c.conformBegin()
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
	if ms = r.members(); strict && (!slices.ContainsFunc(ms, func(m Member) bool { return m.Disk == d1 && m.Leaving }) || len(ms) != 4) {
		t.Fatalf("after the second refill: members %v", ms)
	}
	r.back(d1)
	r.publish()
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
	if ms = r.members(); len(ms) != 2 || slices.ContainsFunc(ms, func(m Member) bool { return m.Disk == d1 || m.Disk == d2 }) {
		t.Fatalf("the recovered report left members %v", ms)
	}
	if name != "" {
		conformWrite(t, name, c)
	}
	return nil
}

// The stale Stay: disk 2 is refilled and returns, lacking Y, acknowledged on disk 1 and on the refill member, which lists the chunk. Disk 1 goes and stays out for refill_after, and the leaving disk 2 takes its place: disk 1 stays, leaving, the only source of Y.
func TestAStayKeepsTheLastSource(t *testing.T) {
	if err := staySourceRun(t, nil, ""); err != nil {
		t.Fatal(err)
	}
}

// staySourceRun runs that scenario under plant, if any, keeping its conformance trace if named, and returns what the simulator's invariants find.
func staySourceRun(t *testing.T, plant func(*authority), name string) error {
	r := newSoleRun(t, 2, 2, 5)
	c := r.c
	if plant != nil {
		plant(r.a)
		c.planted = true
	}
	if name != "" {
		c.conformBegin()
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
	if name != "" {
		conformWrite(t, name, c)
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
	if len(ms) != 3 || !ms[2].Leaving || ms[2].Disk != b || ms[2].Mark != MarkDown {
		t.Fatalf("after the refill of %d: %v", b, ms)
	}
	return l, a, b, ms[1].Disk
}

// A member merely down is never deleted: it leaves beside its replacement however long it is down, and goes on the recovered report for the current interval or when its disk leaves the pool.
func TestARefilledMemberLeaves(t *testing.T) {
	l, a, b, c := leavingPG(t)
	l.now += 60 * minute
	l.step(t, l.inputs(0))
	if got := disksOf(&l.pools().PGs[0]); !slices.Equal(got, []uint32{a, c, b}) {
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

// A second member missing while one leaves is refilled the same way, so a PG holds as many leaving members as went missing; each goes when its own disk leaves the pool, the other staying a source.
func TestASecondMissingMemberLeavesToo(t *testing.T) {
	second := func(t *testing.T) (*layout, uint32, uint32, uint32) {
		l, a, b, c := leavingPG(t)
		l.mark(MarkDown, a)
		l.step(t, l.inputs(0))
		l.now += RefillAfter
		l.step(t, l.inputs(0))
		e := l.pools().PGs[0]
		if len(e.Members) != 4 || leavingIn(e.Members) != 2 || e.Members[0].Disk != c || !e.Members[1].Leaving || e.Members[1].Disk != b || !e.Members[3].Leaving || e.Members[3].Disk != a {
			t.Fatalf("after the second refill: %v", e.Members)
		}
		return l, a, b, c
	}
	l, a, b, c := second(t)
	l.remove(b)
	l.step(t, l.inputs(0))
	e := l.pools().PGs[0]
	if len(e.Members) != 3 || e.Members[0].Disk != c || e.Members[2].Disk != a || !e.Members[2].Leaving {
		t.Fatalf("first leaving disk left the pool: %v", e.Members)
	}
	l, a, b, c = second(t)
	l.remove(a)
	l.step(t, l.inputs(0))
	e = l.pools().PGs[0]
	if len(e.Members) != 3 || e.Members[0].Disk != c || e.Members[1].Disk != b || !e.Members[1].Leaving {
		t.Fatalf("second leaving disk left the pool: %v", e.Members)
	}
}

// Refills widen a PG up to maxMembers; a member missing past that stays an ordinary member until a report or a lost disk makes room.
func TestRefillStopsAtMaxMembers(t *testing.T) {
	l := placed(t, nodesOf(rep(3, 1), 12, 1, 2))
	for range 7 {
		ms := l.pools().PGs[0].Members
		l.mark(MarkDown, ms[slices.IndexFunc(ms, func(m Member) bool { return !m.Leaving && m.Mark == MarkUp })].Disk)
		l.step(t, l.inputs(0))
		l.now += RefillAfter
		l.step(t, l.inputs(0))
	}
	if ms := l.pools().PGs[0].Members; len(ms) != maxMembers || leavingIn(ms) != maxMembers-3 {
		t.Fatalf("members %v", ms)
	}
}

// refillRun is a PG of replicas copies on disks disks holding X, acknowledged, with recovery going.
func refillRun(t *testing.T, replicas, minLive, disks int, plant func(*authority), name string) (r *soleRun, ch core.ChunkID, v uint64) {
	r = newSoleRun(t, replicas, minLive, disks)
	if plant != nil {
		plant(r.a)
		r.c.planted = true
	}
	if name != "" {
		r.c.conformBegin()
	}
	ch = chunkIn(pg0, 1, 0)
	id := r.c.write(pg0, ch, 0, bytes.Repeat([]byte{0x11}, core.BlockSize))
	r.c.until(id)
	v = r.c.cl.replies[id].Version
	r.acked[ch] = v
	r.c.settle()
	return r, ch, v
}

func (r *soleRun) recovery(on bool) {
	for n := range r.c.reps {
		r.c.reps[n].plant.noRecovery = !on
	}
}

// declare has an operator declare disk lost, as the record takes it.
func (r *soleRun) declare(t *testing.T, disk uint32) {
	t.Helper()
	if err := r.a.rec.Lost(disk); err != nil {
		t.Fatal(err)
	}
	if r.c.declared == nil {
		r.c.declared = map[uint32]bool{}
	}
	r.c.declared[disk] = true
	if r.c.conforming {
		r.c.s.Node(r.c.client.Node).Output(r.c.client, trace.Record{Name: trace.OutLost, Fields: [trace.MaxFields]uint64{uint64(disk)}})
	}
}

// repaired fails unless the PG ended on width members none leaving, each holding version v of ch, and the leaving disks went.
func (r *soleRun) repaired(t *testing.T, width int, ch core.ChunkID, v uint64) {
	t.Helper()
	ms := r.members()
	if len(ms) != width || leavingIn(ms) != 0 || len(r.holders(ch, v)) != width {
		t.Fatalf("members %v, holders of X %v, found %v", ms, r.holders(ch, v), r.c.found)
	}
}

// A second disk lost while a first is refilled, or the refill member lost, or both lost together: repair finishes, to three copies, with no disk returning.
func TestASecondFailureFinishesRepair(t *testing.T) {
	last := func(ms []Member, i int) uint32 { return ms[len(ms)-1-i].Disk }
	for name, run := range map[string]func(r *soleRun){
		"during the copy": func(r *soleRun) {
			r.recovery(false)
			ms := r.members()
			r.lose(last(ms, 0))
			r.tick(RefillAfter + 10_000_000)
			r.lose(last(ms, 1))
			r.recovery(true)
		},
		"together": func(r *soleRun) {
			ms := r.members()
			r.lose(last(ms, 0))
			r.lose(last(ms, 1))
		},
		"the refill member": func(r *soleRun) {
			r.recovery(false)
			r.lose(last(r.members(), 0))
			r.tick(RefillAfter + 10_000_000)
			r.lose(r.members()[len(r.members())-2].Disk)
			r.recovery(true)
		},
	} {
		t.Run(name, func(t *testing.T) {
			r, ch, v := refillRun(t, 3, 2, 6, nil, "")
			run(r)
			r.tick(RefillAfter + 30_000_000)
			r.repaired(t, 3, ch, v)
		})
	}
}

// A drain goes on when another member dies during it: it does not wait for the dead disk to return.
func TestADrainFinishesPastAFailure(t *testing.T) {
	r, ch, v := refillRun(t, 3, 2, 6, nil, "")
	r.a.moves = true
	ms := r.members()
	a, b := ms[0].Disk, ms[2].Disk
	r.recovery(false)
	r.a.drain[a] = true
	r.tick(10_000_000)
	r.lose(b)
	r.recovery(true)
	r.tick(RefillAfter + 30_000_000)
	r.repaired(t, 3, ch, v)
	if slices.ContainsFunc(r.members(), func(m Member) bool { return m.Disk == a || m.Disk == b }) {
		t.Fatalf("members %v kept a disk of %d, %d", r.members(), a, b)
	}
}

// A leaving member with no room in its domain does not hold up the refill of another member.
func TestALeavingMemberWithNoRoomLetsARefillPass(t *testing.T) {
	p := rep(2, 1)
	p.MinLive = 2
	l := placed(t, nodesOf(p, 4, 2, 2))
	e := &l.pools().PGs[0]
	a, b := e.Members[0].Disk, e.Members[1].Disk
	c := l.onNode(l.disk(a).Node)[0]
	if c == a {
		c = l.onNode(l.disk(a).Node)[1]
	}
	e.Members = []Member{{Disk: a, Leaving: true}, {Disk: b}, {Disk: c}}
	l.mark(MarkDown, b)
	l.step(t, l.inputs(0))
	l.now += RefillAfter
	l.step(t, l.inputs(0))
	if ms := l.pools().PGs[0].Members; len(ms) != 4 || leavingIn(ms) != 2 || !slices.ContainsFunc(ms, func(m Member) bool { return m.Disk == a && m.Leaving }) {
		t.Fatalf("members %v", ms)
	}
}

// Rep2 with both old disks gone before the copy: new writes are answered within a minute or two, X stays unfound while the disks may return, and a returning disk heals it.
func TestRep2ServesPastTwoFailures(t *testing.T) {
	r, ch, v := refillRun(t, 2, 2, 5, nil, "")
	r.recovery(false)
	ms := r.members()
	d1, d2 := ms[0].Disk, ms[1].Disk
	r.lose(d2)
	r.tick(RefillAfter + 10_000_000)
	r.lose(d1)
	r.recovery(true)
	for n := uint32(0); ; n++ {
		if rp, ok := r.answered(r.c.write(pg0, chunkIn(pg0, 2, n), 0, bytes.Repeat([]byte{0x22}, core.BlockSize)), 3_000_000); ok && rp.Status == StatusOK {
			break
		}
		if n == 40 {
			t.Fatalf("no new write answered in two minutes, members %v", r.members())
		}
		r.tick(2_000_000)
	}
	if rp, _ := r.read(ch); rp.Status != StatusUnfound {
		t.Fatalf("X read %+v, members %v", rp, r.members())
	}
	if ms = r.members(); len(ms) != 4 || leavingIn(ms) != 2 {
		t.Fatalf("members %v", ms)
	}
	r.back(d1)
	r.tick(30_000_000)
	r.repaired(t, 2, ch, v)
}

// lostRun has both old disks of a rep2 pool refilled, recovery off, with Y acknowledged after the first went, so that the second holds it alone, and declares the first lost. Under plant, if any, it keeps its conformance trace if named, and returns what the simulator's invariants find.
func lostRun(t *testing.T, plant func(*authority), name string) error {
	r, ch, _ := refillRun(t, 2, 2, 5, plant, name)
	c := r.c
	ms := r.members()
	d1, d2 := ms[0].Disk, ms[1].Disk
	r.recovery(false)
	r.lose(d2)
	r.tick(DegradedRefillAfter + 2_000_000)
	rp, ok := r.answered(c.write(pg0, ch, 0, bytes.Repeat([]byte{0x22}, core.BlockSize)), 5_000_000)
	if !ok || rp.Status != StatusOK {
		t.Fatalf("Y answered %+v, %v, members %v", rp, ok, r.members())
	}
	r.acked[ch] = rp.Version
	r.lose(d1)
	r.tick(DegradedRefillAfter + 2_000_000)
	r.declare(t, d2)
	r.tick(2_000_000)
	if c.found != nil {
		return c.found
	}
	ms = r.members()
	if len(ms) != 3 || !slices.ContainsFunc(ms, func(m Member) bool { return m.Disk == d1 && m.Leaving }) || slices.ContainsFunc(ms, func(m Member) bool { return m.Disk == d2 }) {
		t.Fatalf("after disk %d was declared lost: members %v", d2, ms)
	}
	r.back(d1)
	r.recovery(true)
	r.tick(10_000_000)
	r.repaired(t, 2, ch, rp.Version)
	if name != "" {
		conformWrite(t, name, c)
	}
	return nil
}

// A disk declared lost goes with its leaving member at once; the other leaving member stays the source it is, and heals the PG when it returns.
func TestADiskDeclaredLostLeavesThePool(t *testing.T) {
	if err := lostRun(t, nil, ""); err != nil {
		t.Fatal(err)
	}
}

// With dropAllLeaving, one leaving disk declared lost takes the other leaving member too: Y stays on a disk no member, with nothing lost but the first disk.
func TestDropAllLeavingStrandsAWrite(t *testing.T) {
	err := lostRun(t, func(a *authority) { a.plant.dropAllLeaving = true }, "")
	if err == nil || !strings.Contains(err.Error(), invStranded) {
		t.Fatalf("found %v", err)
	}
}

// Both old disks declared lost: repair ends with the chunk they alone held unfound for good, and the PG serves the rest.
func TestAChunkOnlyLostDisksHeldStaysUnfound(t *testing.T) {
	r, ch, _ := refillRun(t, 2, 2, 5, nil, "")
	r.recovery(false)
	ms := r.members()
	d1, d2 := ms[0].Disk, ms[1].Disk
	r.lose(d2)
	r.tick(DegradedRefillAfter + 2_000_000)
	r.lose(d1)
	r.recovery(true)
	r.tick(DegradedRefillAfter + 2_000_000)
	r.declare(t, d1)
	r.declare(t, d2)
	r.tick(10_000_000)
	if ms = r.members(); len(ms) != 2 || leavingIn(ms) != 0 {
		t.Fatalf("members %v", ms)
	}
	if rp, _ := r.read(ch); rp.Status != StatusUnfound {
		t.Fatalf("X read %+v", rp)
	}
	if rp, ok := r.answered(r.c.write(pg0, chunkIn(pg0, 1, 1), 0, bytes.Repeat([]byte{0x22}, core.BlockSize)), 5_000_000); !ok || rp.Status != StatusOK {
		t.Fatalf("a new write answered %+v, %v", rp, ok)
	}
	if rp, ok := r.answered(r.c.write(pg0, ch, 0, bytes.Repeat([]byte{0x22}, core.BlockSize)), 5_000_000); !ok || rp.Status != StatusUnfound {
		t.Fatalf("a write of the unfound chunk answered %+v, %v", rp, ok)
	}
}

// A primary refilled while down comes back behind the others: the member that took over keeps leading and serving.
func TestARefilledPrimaryReturnsBehindTheOthers(t *testing.T) {
	r, ch, v := refillRun(t, 3, 2, 6, nil, "")
	p := r.members()[0].Disk
	r.recovery(false)
	r.lose(p)
	r.tick(RefillAfter + 5_000_000)
	_, e := r.c.table.entry(pg0)
	took, _ := e.primary()
	r.back(p)
	r.publish()
	_, e = r.c.table.entry(pg0)
	if d, _ := e.primary(); d != took || d == p {
		t.Fatalf("primary %d with %d back, %d before, members %v", d, p, took, e.Members)
	}
	start := r.c.s.Time()
	for {
		rp, _ := r.read(ch)
		if rp.Status == StatusOK {
			t.Logf("X reads %d us after the return", r.c.s.Time()-start)
			break
		}
		if r.c.s.Time()-start > 15_000_000 {
			t.Fatalf("X read %+v, members %v", rp, e.Members)
		}
		r.tick(500_000)
	}
	r.recovery(true)
	r.tick(10_000_000)
	r.repaired(t, 3, ch, v)
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
	if len(ms) != 3 || ms[0].Disk != c || ms[1].Disk != b || ms[1].Leaving || ms[2].Disk != a || !ms[2].Leaving {
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
