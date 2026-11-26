package meta

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

var operator = core.NodeID{Node: 50}

// admin sends the host an operator's command numbered request.
func (r *hostRun) admin(request uint64, a Admin) {
	r.t.Helper()
	var m core.Msg
	if err := EncodeAdmin(r.pool, hostGroup, request, &a, &m); err != nil {
		r.t.Fatal(err)
	}
	r.recv(operator, m)
}

// answers decodes the answers the operator got since the last call, by request, and frees every
// message sent.
func (r *hostRun) answers() map[uint64][]AdminAnswer {
	r.t.Helper()
	got := map[uint64][]AdminAnswer{}
	for _, a := range r.sent {
		if _, k, request, _ := Peek(&a.Msg); a.Peer == operator && k == KindAdminAnswer {
			an, err := DecodeAdminAnswer(r.pool, &a.Msg)
			if err != nil {
				r.t.Fatal(err)
			}
			got[request] = append(got[request], an)
		}
		free(&core.Context{Pool: r.pool}, &a.Msg)
	}
	r.sent = r.sent[:0]
	return got
}

// A pool command sent twice is answered once, only once the pool has committed with its PGs on
// the disks nodes reported; the same command again, which changes nothing, is answered at once.
func TestAPoolCommandIsAnsweredOnceItCommits(t *testing.T) {
	r := newHostRun(t)
	r.lead()
	r.report(&NodeReport{Node: 2, Opened: []OpenedDisk{{Device: 1, Log: 0x77, Blocks: 1 << 20}, {Device: 2, Log: 0x78, Blocks: 1 << 20}}})
	r.run(StepInterval * 3)
	r.answers()
	cmd := Admin{Op: AdminPool, Pool: 1, Spec: spec(1, 8)}
	r.admin(7, cmd)
	r.admin(7, cmd)
	if got := r.answers(); len(got) != 0 {
		t.Fatalf("answered before the pool committed: %+v", got)
	}
	r.run(StepInterval * 5)
	if got := r.answers(); len(got[7]) != 1 || got[7][0].Refused != "" {
		t.Fatalf("answered %+v", got)
	}
	tb := r.h.Control().Table()
	if len(tb.Pools) != 1 || len(tb.Pools[0].PGs) != 8 {
		t.Fatalf("table %+v", tb)
	}
	for i, pg := range tb.Pools[0].PGs {
		if len(pg.Members) != 1 || pg.Members[0].Disk != 1 && pg.Members[0].Disk != 2 {
			t.Fatalf("PG %d placed on %+v", i, pg.Members)
		}
	}
	r.admin(8, cmd)
	if got := r.answers(); len(got[8]) != 1 || got[8][0].Refused != "" {
		t.Fatalf("an unchanged pool answered %+v", got)
	}
}

// Commands the leader refuses are answered at once with why: a declaration for an up disk and for
// an unknown one, a removal of a disk not drained, a pool the pool rules refuse and a command that
// does not decode.
func TestARefusedCommandIsAnsweredWithItsReason(t *testing.T) {
	r := newHostRun(t)
	r.lead()
	r.report(&NodeReport{Node: 2, Opened: []OpenedDisk{{Device: 1, Log: 0x77, Blocks: 1 << 20}}})
	r.run(StepInterval * 3)
	r.answers()
	data := 4
	for i, c := range []struct {
		a    Admin
		want string
	}{
		{Admin{Op: AdminLost, Disk: 1}, chunkstore.ErrLostRefused.Error()},
		{Admin{Op: AdminLost, Disk: 9}, ErrUnknownDisk.Error()},
		{Admin{Op: AdminRemove, Disk: 1}, ErrNotDrained.Error()},
		{Admin{Op: AdminPool, Pool: 1, Spec: chunkstore.PoolSpec{Data: &data}}, "data"},
	} {
		r.admin(uint64(i+1), c.a)
		if got := r.answers(); len(got[uint64(i+1)]) != 1 || !strings.Contains(got[uint64(i+1)][0].Refused, c.want) {
			t.Fatalf("%+v answered %+v, want a refusal naming %q", c.a, got, c.want)
		}
	}
	var m core.Msg
	if err := encodeBody(r.pool, hostGroup, KindAdmin, 9, []byte{byte(AdminFailed), 0}, &m); err != nil {
		t.Fatal(err)
	}
	r.recv(operator, m)
	if got := r.answers(); len(got[9]) != 1 || got[9][0].Refused == "" {
		t.Fatalf("a malformed command answered %+v", got)
	}
	if len(r.h.Control().Pools()) != 0 || r.h.Control().Disks()[0].Gone {
		t.Fatal("a refused command took effect")
	}
}

// A declaration that a dead node's disk is lost is answered once it commits, the disk gone; the
// same declaration again is answered at once.
func TestALostDiskIsAnsweredOnceGone(t *testing.T) {
	r := newHostRun(t)
	r.lead()
	r.report(&NodeReport{Node: 2, Opened: []OpenedDisk{{Device: 1, Log: 0x77, Blocks: 1 << 20}}})
	r.report(&NodeReport{Node: 3, Opened: []OpenedDisk{{Device: 1, Log: 0x88, Blocks: 1 << 20}}})
	r.run(StepInterval * 3)
	r.admin(1, Admin{Op: AdminPool, Pool: 1, Spec: chunkstore.PoolSpec{Replicas: new(2), MinLive: new(1), PGCount: new(uint32(8))}})
	r.run(StepInterval * 5)
	r.answers()
	for range 2 * chunkstore.NodeDownAfter / StepInterval {
		r.report(&NodeReport{Node: 2, Opened: []OpenedDisk{{Device: 1, Log: 0x77, Blocks: 1 << 20}}})
		r.run(StepInterval)
	}
	r.answers()
	r.admin(2, Admin{Op: AdminLost, Disk: 2})
	if got := r.answers(); len(got) != 0 {
		t.Fatalf("answered before the declaration committed: %+v", got)
	}
	r.run(StepInterval * 5)
	if got := r.answers(); len(got[2]) != 1 || got[2][0].Refused != "" {
		t.Fatalf("answered %+v", got)
	}
	if d := r.h.Control().Disks()[1]; !d.Gone {
		t.Fatalf("disk 2 declared lost: %+v", d)
	}
	r.admin(3, Admin{Op: AdminLost, Disk: 2})
	if got := r.answers(); len(got[3]) != 1 || got[3][0].Refused != "" {
		t.Fatalf("a repeated declaration answered %+v", got)
	}
}

// A drain over the wire is answered once it commits, the disk drained, and the disk is then removed
// by a removal; an undrain is answered once the disk's drain has ended, and a drain of an unknown
// disk at once with ErrUnknownDisk.
func TestADrainOverTheWireIsAnsweredOnceItCommits(t *testing.T) {
	r := newHostRun(t)
	r.lead()
	r.report(&NodeReport{Node: 2, Opened: []OpenedDisk{{Device: 1, Log: 0x77, Blocks: 1 << 20}, {Device: 2, Log: 0x78, Blocks: 1 << 20}}})
	r.run(StepInterval * 3)
	r.answers()
	r.admin(1, Admin{Op: AdminDrain, Disk: 9})
	if got := r.answers(); len(got[1]) != 1 || !strings.Contains(got[1][0].Refused, ErrUnknownDisk.Error()) {
		t.Fatalf("a drain of an unknown disk answered %+v", got)
	}
	for _, d := range []uint32{1, 2} {
		r.admin(uint64(d+1), Admin{Op: AdminDrain, Disk: d})
	}
	if got := r.answers(); len(got) != 0 {
		t.Fatalf("answered before the drains committed: %+v", got)
	}
	r.run(StepInterval * 5)
	if got := r.answers(); len(got[2]) != 1 || got[2][0].Refused != "" || len(got[3]) != 1 || got[3][0].Refused != "" {
		t.Fatalf("answered %+v", got)
	}
	if ds := r.h.Control().Disks(); !ds[0].Drain || !ds[1].Drain {
		t.Fatalf("disks after the drains: %+v", ds)
	}
	r.admin(4, Admin{Op: AdminUndrain, Disk: 2})
	r.admin(5, Admin{Op: AdminRemove, Disk: 1})
	r.run(StepInterval * 5)
	if got := r.answers(); len(got[4]) != 1 || got[4][0].Refused != "" || len(got[5]) != 1 || got[5][0].Refused != "" {
		t.Fatalf("answered %+v", got)
	}
	if ds := r.h.Control().Disks(); !ds[0].Gone || ds[1].Drain || ds[1].Gone {
		t.Fatalf("disks after the undrain and the removal: %+v", ds)
	}
}

// A pin, an edit and an unpin over the wire are each answered once the table holds the PG as asked: a pin with no disks
// keeps the PG's members, a pin with disks reorders them too, an unpin keeps them and an edit reorders them unpinned. An
// edit of an unknown PG or disk, or naming other disks than the members, is refused at once.
func TestAnEditOverTheWireIsAnsweredOnceItCommits(t *testing.T) {
	r, pg, a, b := editRun(t)
	for i, c := range []struct {
		a     Admin
		disks []uint32
		label string
	}{
		{Admin{Op: AdminEdit, Pool: 1, Pin: true}, []uint32{a, b}, "a pin"},
		{Admin{Op: AdminEdit, Pool: 1, Pin: true, Disks: []uint32{b, a}}, []uint32{b, a}, "a pin with disks"},
		{Admin{Op: AdminEdit, Pool: 1}, []uint32{b, a}, "an unpin"},
		{Admin{Op: AdminEdit, Pool: 1, Disks: []uint32{a, b}}, []uint32{a, b}, "an edit"},
	} {
		req := uint64(i + 2)
		r.admin(req, c.a)
		if got := r.answers(); len(got) != 0 {
			t.Fatalf("%s answered before it committed: %+v", c.label, got)
		}
		r.run(StepInterval * 5)
		e := r.h.Control().Table().Entry(pg)
		if got := r.answers(); len(got[req]) != 1 || got[req][0].Refused != "" || e.Pinned != c.a.Pin || !slices.Equal(memberDisks(e), c.disks) {
			t.Fatalf("%s answered %+v, entry %+v", c.label, got, e)
		}
	}
	for i, c := range []struct {
		a    Admin
		want string
	}{
		{Admin{Op: AdminEdit, Pool: 1, PG: 8, Pin: true}, ErrUnknownPG.Error()},
		{Admin{Op: AdminEdit, Pool: 1, Disks: []uint32{9}}, ErrUnknownDisk.Error()},
		{Admin{Op: AdminEdit, Pool: 1, Pin: true, Disks: []uint32{a, 6 - a - b}}, ErrEditMembers.Error()},
		{Admin{Op: AdminEdit, Pool: 1, Disks: []uint32{b}}, ErrEditMembers.Error()},
	} {
		req := uint64(i + 10)
		r.admin(req, c.a)
		if got := r.answers(); len(got[req]) != 1 || !strings.Contains(got[req][0].Refused, c.want) {
			t.Fatalf("%+v answered %+v, want a refusal naming %q", c.a, got, c.want)
		}
	}
}

// An edit that commits overtaken is refused: a reorder another edit in its table undoes, though the PG keeps its disks
// and pin, and an edit keeping the order of a PG a drain in its table moves. An edit of a moving PG is refused at once.
func TestAnEditOvertakenInItsTableIsRefused(t *testing.T) {
	r, _, a, b := editRun(t)
	r.admin(2, Admin{Op: AdminEdit, Pool: 1, Disks: []uint32{b, a}})
	r.admin(3, Admin{Op: AdminEdit, Pool: 1, Disks: []uint32{a, b}})
	r.run(StepInterval * 5)
	got := r.answers()
	if len(got[2]) != 1 || !strings.Contains(got[2][0].Refused, "overtook") || len(got[3]) != 1 || got[3][0].Refused != "" {
		t.Fatalf("two reorders in one table answered %+v", got)
	}
	r.activate(chunkstore.Report{ID: 1, Disks: []chunkstore.DiskState{{Log: 0x77}, {Log: 0x78}, {Log: 0x79}}})
	for i, e := range r.h.Control().Table().Pools[0].PGs {
		r.admin(uint64(10+i), Admin{Op: AdminEdit, Pool: 1, PG: uint32(i), Disks: memberDisks(&e)})
	}
	r.admin(4, Admin{Op: AdminDrain, Disk: a})
	r.run(StepInterval * 5)
	got, moved := r.answers(), -1
	for i, e := range r.h.Control().Table().Pools[0].PGs {
		an := got[uint64(10+i)]
		if slices.ContainsFunc(e.Members, func(m chunkstore.Member) bool { return m.Leaving }) {
			moved = i
			if len(an) != 1 || !strings.Contains(an[0].Refused, "overtook") {
				t.Fatalf("an edit of PG %d, which a drain moved, answered %+v", i, an)
			}
		} else if len(an) != 1 || an[0].Refused != "" {
			t.Fatalf("an edit of PG %d answered %+v", i, an)
		}
	}
	if moved < 0 {
		t.Fatal("the drain moved no PG")
	}
	r.admin(5, Admin{Op: AdminEdit, Pool: 1, PG: uint32(moved), Pin: true})
	if got := r.answers(); len(got[5]) != 1 || !strings.Contains(got[5][0].Refused, ErrPGMoving.Error()) {
		t.Fatalf("a pin of a moving PG answered %+v", got)
	}
}

// editRun is a led host with a rep2 pool of 8 PGs, one disk per failure domain, on disks 1 to 3 of node 2: it returns
// PG 1.0 and its members.
func editRun(t *testing.T) (*hostRun, core.PGID, uint32, uint32) {
	r := newHostRun(t)
	r.lead()
	r.report(&NodeReport{Node: 2, Opened: []OpenedDisk{{Device: 1, Log: 0x77, Blocks: 1 << 20}, {Device: 2, Log: 0x78, Blocks: 1 << 20}, {Device: 3, Log: 0x79, Blocks: 1 << 20}}})
	r.run(StepInterval * 3)
	s := spec(2, 8)
	s.FailureDomain = "disk"
	r.admin(1, Admin{Op: AdminPool, Pool: 1, Spec: s})
	r.run(StepInterval * 5)
	r.answers()
	pg := core.PGID{Pool: 1}
	ms := memberDisks(r.h.Control().Table().Entry(pg))
	if len(ms) != 2 {
		t.Fatalf("PG 1.0 has members %v", ms)
	}
	return r, pg, ms[0], ms[1]
}

// activate sends rp from node 2's loop with every PG of pool 1 activated and recovered at its interval.
func (r *hostRun) activate(rp chunkstore.Report) {
	r.t.Helper()
	tb := r.h.Control().Table()
	for i, e := range tb.Pools[0].PGs {
		f := chunkstore.PGReport{PGFields: chunkstore.PGFields{Epoch: tb.Epoch, PG: core.PGID{Pool: 1, Index: uint32(i)}, Interval: e.Interval}, PGCount: uint32(len(tb.Pools[0].PGs))}
		rp.Activations, rp.Recovered = append(rp.Activations, f), append(rp.Recovered, f)
	}
	var m core.Msg
	if err := chunkstore.Attach(r.pool, &m, chunkstore.AppendReport(nil, &rp), core.NoBuf); err != nil {
		r.t.Fatal(err)
	}
	r.recv(core.NodeID{Node: 2, Loop: 1}, m)
}

func memberDisks(e *chunkstore.PGEntry) []uint32 {
	var ds []uint32
	for _, m := range e.Members {
		ds = append(ds, m.Disk)
	}
	return ds
}

// A drain change that a race overtakes before it commits is refused with what happened: a drain
// of a down disk declared lost with it, an undrain of a disk removed with it, and a drain undone
// by an undrain, which is answered.
func TestADrainChangeOvertakenIsRefusedWithWhy(t *testing.T) {
	r := newHostRun(t)
	r.lead()
	node := NodeReport{Node: 2, Opened: []OpenedDisk{{Device: 1, Log: 0x77, Blocks: 1 << 20}, {Device: 2, Log: 0x78, Blocks: 1 << 20}}}
	r.report(&node)
	r.report(&NodeReport{Node: 3, Opened: []OpenedDisk{{Device: 1, Log: 0x88, Blocks: 1 << 20}}})
	r.run(StepInterval * 3)
	for range 2 * chunkstore.NodeDownAfter / StepInterval {
		r.report(&node)
		r.run(StepInterval)
	}
	r.admin(1, Admin{Op: AdminDrain, Disk: 1})
	r.run(StepInterval * 5)
	r.answers()
	r.admin(2, Admin{Op: AdminDrain, Disk: 3})
	r.admin(3, Admin{Op: AdminLost, Disk: 3})
	r.admin(4, Admin{Op: AdminUndrain, Disk: 1})
	r.admin(5, Admin{Op: AdminRemove, Disk: 1})
	r.admin(6, Admin{Op: AdminDrain, Disk: 2})
	r.admin(7, Admin{Op: AdminUndrain, Disk: 2})
	r.run(StepInterval * 5)
	got := r.answers()
	for i, want := range []string{"declared lost or removed", "", "declared lost or removed", "", "another command changed its drain", ""} {
		req := uint64(i + 2)
		if len(got[req]) != 1 || want == "" && got[req][0].Refused != "" || !strings.Contains(got[req][0].Refused, want) {
			t.Fatalf("request %d answered %+v, want a refusal naming %q", req, got[req], want)
		}
	}
}

// A drained disk that holds members empties through balance moves, never more than max_moves at
// once; its removal is refused with how many PGs still name it until then, and answered after.
func TestADrainedDiskWithMembersEmptiesAndIsRemoved(t *testing.T) {
	r := newHostRun(t)
	r.lead()
	node := NodeReport{Node: 2, Opened: []OpenedDisk{{Device: 1, Log: 0x77, Blocks: 1 << 20}, {Device: 2, Log: 0x78, Blocks: 1 << 20}}}
	r.report(&node)
	r.run(StepInterval * 3)
	r.admin(1, Admin{Op: AdminPool, Pool: 1, Spec: spec(1, 8)})
	r.run(StepInterval * 5)
	r.admin(2, Admin{Op: AdminDrain, Disk: 1})
	r.run(StepInterval * 5)
	r.answers()
	if named(r.h.Control().Table(), 1, true) == 0 {
		t.Fatal("disk 1 holds no member")
	}
	r.admin(3, Admin{Op: AdminRemove, Disk: 1})
	if got := r.answers(); len(got[3]) != 1 || !strings.Contains(got[3][0].Refused, "PGs still name it") {
		t.Fatalf("a removal of a disk still named answered %+v", got)
	}
	for steps := 0; named(r.h.Control().Table(), 1, true) > 0; steps++ {
		tb := r.h.Control().Table()
		if steps > 100 {
			t.Fatalf("disk 1 is still named by %d PGs", named(tb, 1, true))
		}
		if n := moving(tb); n > MaxMoves {
			t.Fatalf("%d moves in flight", n)
		}
		r.activate(chunkstore.Report{ID: 1, Disks: []chunkstore.DiskState{{Log: 0x77}, {Log: 0x78}}})
		r.report(&node)
		r.run(StepInterval)
	}
	r.answers()
	r.admin(4, Admin{Op: AdminRemove, Disk: 1})
	r.run(StepInterval * 5)
	if got := r.answers(); len(got[4]) != 1 || got[4][0].Refused != "" || !r.h.Control().Disks()[0].Gone {
		t.Fatalf("a removal of the emptied disk answered %+v", got)
	}
}

// A give-up is refused for a PG the table lacks, for an interval not current and for one with nothing lost; once its
// primary reports chunks unfound, which a read of the PG gives with its interval, it is answered when it commits, with
// the interval it gave up and the chunks it dropped. Sent again after a newer interval leaves chunks unfound, it is
// answered with the same give-up and gives up nothing more.
func TestAGiveUpIsAnsweredWithWhatItDropped(t *testing.T) {
	r := newHostRun(t)
	r.lead()
	r.report(&NodeReport{Node: 2, Opened: []OpenedDisk{{Device: 1, Log: 0x77, Blocks: 1 << 20}}})
	r.run(StepInterval * 3)
	r.admin(1, Admin{Op: AdminPool, Pool: 1, Spec: chunkstore.PoolSpec{Replicas: new(1), MinLive: new(1), PGCount: new(uint32(8))}})
	r.run(StepInterval * 5)
	r.answers()
	pg := core.PGID{Pool: 1, Index: 3}
	k := r.h.Control().Table().Entry(pg).Interval
	for i, c := range []struct {
		a    Admin
		want error
	}{
		{Admin{Op: AdminGiveUp, Pool: 1, PG: 8, Interval: k}, ErrUnknownPG},
		{Admin{Op: AdminPG, Pool: 1, PG: 8}, ErrUnknownPG},
		{Admin{Op: AdminGiveUp, Pool: 1, PG: 3, Interval: k + 1}, ErrIntervalMoved},
		{Admin{Op: AdminGiveUp, Pool: 1, PG: 3, Interval: k}, chunkstore.ErrNothingLost},
	} {
		r.admin(uint64(i+2), c.a)
		if got := r.answers(); len(got[uint64(i+2)]) != 1 || !strings.Contains(got[uint64(i+2)][0].Refused, c.want.Error()) {
			t.Fatalf("%+v answered %+v, want a refusal naming %q", c.a, got, c.want)
		}
	}
	lost := []core.ChunkID{{Volume: 1, Index: 4}, {Volume: 1, Index: 12}}
	var m core.Msg
	if err := chunkstore.Attach(nil, &m, chunkstore.AppendReport(nil, &chunkstore.Report{ID: 1, Leads: []chunkstore.LeadState{{PG: pg, PGCount: uint32(len(r.h.Control().Table().Pools[0].PGs)), Interval: k, Active: true, Unfound: 2, Chunks: lost}}}), core.NoBuf); err != nil {
		t.Fatal(err)
	}
	r.recv(core.NodeID{Node: 2, Loop: 1}, m)
	r.admin(9, Admin{Op: AdminPG, Pool: 1, PG: 3})
	if got := r.answers(); len(got[9]) != 1 || got[9][0].GiveUp.Interval != k || got[9][0].Unfound != 2 {
		t.Fatalf("a read of PG 1.3 answered %+v", got)
	}
	r.admin(10, Admin{Op: AdminGiveUp, Pool: 1, PG: 3, Interval: k})
	if got := r.answers(); len(got) != 0 {
		t.Fatalf("answered before the give-up committed: %+v", got)
	}
	r.run(StepInterval * 5)
	want := chunkstore.GiveUp{Interval: k, Drop: true, Chunks: lost}
	if got := r.answers(); len(got[10]) != 1 || got[10][0].Refused != "" || !reflect.DeepEqual(got[10][0].GiveUp, want) {
		t.Fatalf("answered %+v, want %+v", got, want)
	}
	if e := r.h.Control().Table().Entry(pg); e.Interval != k+1 {
		t.Fatalf("after the give-up: %+v", e)
	}
	if err := chunkstore.Attach(nil, &m, chunkstore.AppendReport(nil, &chunkstore.Report{ID: 2, Leads: []chunkstore.LeadState{{PG: pg, PGCount: uint32(len(r.h.Control().Table().Pools[0].PGs)), Interval: k + 1, Active: true, Unfound: 2, Chunks: lost}}}), core.NoBuf); err != nil {
		t.Fatal(err)
	}
	r.recv(core.NodeID{Node: 2, Loop: 1}, m)
	r.admin(10, Admin{Op: AdminGiveUp, Pool: 1, PG: 3, Interval: k})
	r.run(StepInterval * 5)
	if got := r.answers(); len(got[10]) != 1 || got[10][0].Refused != "" || !reflect.DeepEqual(got[10][0].GiveUp, want) {
		t.Fatalf("sent again, answered %+v, want %+v", got, want)
	}
	if e := r.h.Control().Table().Entry(pg); e.Interval != k+1 || len(e.GiveUps) != 1 {
		t.Fatalf("a give-up of %d sent again after %d left chunks unfound: %+v", k, k+1, e)
	}
}

// A pool change keeps every setting it leaves out as the pool has it, not at its default.
func TestAPoolChangeKeepsTheSettingsItLeavesOut(t *testing.T) {
	r := newHostRun(t)
	r.lead()
	for n := uint32(2); n <= 4; n++ {
		r.report(&NodeReport{Node: n, Opened: []OpenedDisk{{Device: 1, Log: 0x70 + uint64(n), Blocks: 1 << 20}}})
	}
	r.run(StepInterval * 3)
	r.admin(1, Admin{Op: AdminPool, Pool: 1, Spec: chunkstore.PoolSpec{Replicas: new(2), MinLive: new(1), Overcommit: new(false), PGCount: new(uint32(8)), FailureDomain: chunkstore.DomainDisk}})
	r.run(StepInterval * 5)
	r.answers()
	r.admin(2, Admin{Op: AdminPool, Pool: 1, Spec: chunkstore.PoolSpec{WriteLimit: new(uint8(50))}})
	r.run(StepInterval * 5)
	if got := r.answers(); len(got[2]) != 1 || got[2][0].Refused != "" {
		t.Fatalf("answered %+v", got)
	}
	want := chunkstore.Pool{Replicas: 2, MinLive: 1, FailureDomain: chunkstore.DomainDisk, MaxPerDomain: 1, Locality: chunkstore.LocalitySpread, WriteLimit: 50, PGCount: 8}
	if p := r.h.Control().Pools()[0].Pool; p != want {
		t.Fatalf("a write_limit change left the pool %+v, want %+v", p, want)
	}
}

// The failed devices command lists every device the nodes heard from left failed.
func TestTheFailedDevicesCommandListsThem(t *testing.T) {
	r := newHostRun(t)
	r.lead()
	r.report(&NodeReport{Node: 2, Failed: []uint64{0xbad}})
	r.answers()
	r.admin(1, Admin{Op: AdminFailed})
	if got := r.answers(); len(got[1]) != 1 || !slices.Equal(got[1][0].Failed, []FailedDevice{{Node: 2, Device: 0xbad}}) {
		t.Fatalf("answered %+v", got)
	}
}

// A command waiting when the leader drops its Control is never answered: the command line sends
// it again to the leader it then finds.
func TestACommandWaitingWhenTheControlDropsIsNotAnswered(t *testing.T) {
	r := newHostRun(t)
	r.lead()
	r.report(&NodeReport{Node: 2, Opened: []OpenedDisk{{Device: 1, Log: 0x77, Blocks: 1 << 20}}})
	r.run(StepInterval * 3)
	r.answers()
	r.admin(1, Admin{Op: AdminPool, Pool: 1, Spec: spec(1, 8)})
	r.h.term = 0
	r.h.lead(&core.Context{Now: r.now, Raw: r.now, Pool: r.pool}, &r.out)
	if len(r.h.admins) != 0 {
		t.Fatalf("a dropped Control left commands waiting: %+v", r.h.admins)
	}
}

// write commits ops to the group as another leader's Txn would, outside the Control.
func (r *hostRun) write(ops ...Op) {
	r.t.Helper()
	var m core.Msg
	if err := EncodeTxn(r.pool, hostGroup, 1, &Txn{Ops: ops}, &m); err != nil {
		r.t.Fatal(err)
	}
	r.recv(hostNode, m)
}

// A pool change whose Txn fails its check, since another leader committed the pool at another
// pg_count, is dropped after the reload and answered with a refusal, not as set.
func TestAPoolChangeDroppedBeforeItCommitsIsRefused(t *testing.T) {
	r := newHostRun(t)
	r.failing = true
	r.lead()
	r.report(&NodeReport{Node: 2, Opened: []OpenedDisk{{Device: 1, Log: 0x77, Blocks: 1 << 20}, {Device: 2, Log: 0x78, Blocks: 1 << 20}}})
	r.run(StepInterval * 3)
	r.admin(1, Admin{Op: AdminPool, Pool: 1, Spec: spec(1, 8)})
	r.run(StepInterval * 5)
	r.answers()
	r.admin(2, Admin{Op: AdminPool, Pool: 1, Spec: chunkstore.PoolSpec{WriteLimit: new(uint8(50))}})
	other := r.h.Control().Pools()[0].Pool
	other.PGCount = 16
	r.write(Put(poolKey(1), appendPool(nil, &other)))
	r.answers()
	r.run(StepInterval * 5)
	if got := r.answers(); len(got[2]) != 1 || !strings.Contains(got[2][0].Refused, "dropped") {
		t.Fatalf("answered %+v", got)
	}
	if p := r.h.Control().Pools()[0].Pool; p.WriteLimit == 50 {
		t.Fatalf("the dropped change committed: %+v", p)
	}
}

// A declaration whose Txn fails its check, since another leader committed the disk up, is
// dropped after the reload and answered with the refusal, not as declared lost.
func TestADeclarationDroppedBeforeItCommitsIsRefused(t *testing.T) {
	r := newHostRun(t)
	r.failing = true
	r.lead()
	r.report(&NodeReport{Node: 2, Opened: []OpenedDisk{{Device: 1, Log: 0x77, Blocks: 1 << 20}}})
	r.report(&NodeReport{Node: 3, Opened: []OpenedDisk{{Device: 1, Log: 0x88, Blocks: 1 << 20}}})
	r.run(StepInterval * 3)
	r.admin(1, Admin{Op: AdminPool, Pool: 1, Spec: chunkstore.PoolSpec{Replicas: new(2), MinLive: new(1), PGCount: new(uint32(8))}})
	r.run(StepInterval * 5)
	r.answers()
	var up ScanResult
	if err := r.h.Shard().State().Scan(ctlKey(CtlMembership, nil), PrefixEnd(ctlKey(CtlMembership, nil)), 1<<16, &up); err != nil {
		t.Fatal(err)
	}
	var ops []Op
	for _, kv := range up.KVs {
		ops = append(ops, Put(slices.Clone(kv.Key), slices.Clone(kv.Value)))
	}
	for range 2 * chunkstore.NodeDownAfter / StepInterval {
		r.report(&NodeReport{Node: 2, Opened: []OpenedDisk{{Device: 1, Log: 0x77, Blocks: 1 << 20}}})
		r.run(StepInterval)
	}
	r.answers()
	r.admin(2, Admin{Op: AdminLost, Disk: 2})
	r.write(ops...)
	if got := r.answers(); len(got) != 0 {
		t.Fatalf("answered before the declaration's Txn: %+v", got)
	}
	r.run(StepInterval * 5)
	if got := r.answers(); len(got[2]) != 1 || !strings.Contains(got[2][0].Refused, "refused") {
		t.Fatalf("answered %+v", got)
	}
	if r.h.Control().Disks()[1].Gone {
		t.Fatal("disk 2 is gone")
	}
}

// A command at a leader whose lease does not run is neither answered nor taken, the failed list
// included: the command line sends it again. The helper's group leads without leases.
func TestACommandAtALeaderWithoutItsLeaseIsDropped(t *testing.T) {
	c := newGroup(t, 3, false)
	l := c.elect()
	c.heartbeat()
	h := &CtlHost{shard: c.n[l].s, sv: NewServer(), id: uint64(l) + 1, group: 5, term: c.n[l].s.Raft().Term()}
	var err error
	if h.ctl, err = NewControl(h.shard.State(), h.term); err != nil {
		t.Fatal(err)
	}
	ctx := c.ctx(l)
	for _, a := range []Admin{{Op: AdminFailed}, {Op: AdminPool, Pool: 1, Spec: spec(1, 8)}} {
		var m core.Msg
		if err := EncodeAdmin(c.n[l].pool, 5, 1, &a, &m); err != nil {
			t.Fatal(err)
		}
		var out CtlHostOutput
		h.OnEvent(&ctx, &core.Event{Kind: core.EvNetRecv, Peer: operator, Msg: m}, &out)
		if h.Control() == nil || len(out.Actions) != 0 || len(h.admins) != 0 || len(h.ctl.staged) != 0 {
			t.Fatalf("%+v without the lease: Control %v, sent %+v, waits %+v", a, h.Control() != nil, out.Actions, h.admins)
		}
	}
}

func TestAdminCrossesTheWireWhole(t *testing.T) {
	pool := sim.NewPool(1<<20, 0)
	full := chunkstore.PoolSpec{Replicas: new(3), MinLive: new(2), Data: new(4), Parity: new(2), EncodeOnWrite: new(true), MaxPerDomain: new(1),
		Overcommit: new(false), WriteLimit: new(uint8(90)), PGCount: new(uint32(1 << 31)), Sealed: "ec", FailureDomain: "disk", Locality: "node", DiskClass: "nvme"}
	for _, a := range []Admin{{Op: AdminLost, Disk: 1 << 31}, {Op: AdminRemove, Disk: 1}, {Op: AdminFailed}, {Op: AdminMembership}, {Op: AdminPool, Pool: 7}, {Op: AdminPool, Pool: 1, Spec: full}, {Op: AdminGiveUp, Pool: 1 << 31, PG: 1<<32 - 1, Interval: 1<<32 - 1}, {Op: AdminPG, Pool: 2, PG: 5}, {Op: AdminDrain, Disk: 1<<32 - 1}, {Op: AdminUndrain, Disk: 4},
		{Op: AdminEdit, Pool: 1 << 31, PG: 1<<32 - 1, Pin: true}, {Op: AdminEdit, Pool: 3, PG: 2, Disks: []uint32{1<<32 - 1, 1, 7}}} {
		var m core.Msg
		if err := EncodeAdmin(pool, 1, 5, &a, &m); err != nil {
			t.Fatal(err)
		}
		got, err := DecodeAdmin(pool, &m)
		if _, _, tok, _ := Peek(&m); err != nil || tok != 5 || string(appendSpec(nil, &got.Spec)) != string(appendSpec(nil, &a.Spec)) || got.Op != a.Op || got.Disk != a.Disk || got.Pool != a.Pool || got.PG != a.PG || got.Interval != a.Interval ||
			got.Pin != a.Pin || !slices.Equal(got.Disks, a.Disks) {
			t.Fatalf("sent %+v, decoded %+v, %v", a, got, err)
		}
	}
	var fs []FailedDevice
	for i := range 100 {
		fs = append(fs, FailedDevice{Node: uint32(i) << 24, Device: uint64(i) << 56})
	}
	lost := []core.ChunkID{{Volume: 1 << 31, Inode: 1 << 63, Index: 1<<32 - 1}, {Volume: 1, Index: 2}}
	for _, a := range []AdminAnswer{{}, {Refused: strings.Repeat("x", 300)}, {Failed: fs},
		{Failed: fs[:1], GiveUp: chunkstore.GiveUp{Interval: 1<<32 - 1, All: true}}, {GiveUp: chunkstore.GiveUp{Interval: 4, Drop: true, Chunks: lost}}, {GiveUp: chunkstore.GiveUp{Interval: 9}, Unfound: 1<<32 - 1}} {
		var m core.Msg
		if err := EncodeAdminAnswer(pool, 1, 5, &a, &m); err != nil {
			t.Fatal(err)
		}
		got, err := DecodeAdminAnswer(pool, &m)
		if err != nil || got.Refused != a.Refused || !slices.Equal(got.Failed, a.Failed) || !reflect.DeepEqual(got.GiveUp, a.GiveUp) || got.Unfound != a.Unfound {
			t.Fatalf("sent %+v, decoded %+v, %v", a, got, err)
		}
		free(&core.Context{Pool: pool}, &m)
	}
}

func FuzzDecodeAdmin(f *testing.F) {
	for _, a := range []Admin{{Op: AdminLost, Disk: 2}, {Op: AdminRemove, Disk: 3}, {Op: AdminFailed}, {Op: AdminMembership}, {Op: AdminGiveUp, Pool: 1, PG: 7, Interval: 3}, {Op: AdminPG, Pool: 1, PG: 7}, {Op: AdminDrain, Disk: 4}, {Op: AdminUndrain, Disk: 5},
		{Op: AdminEdit, Pool: 1, PG: 7, Pin: true}, {Op: AdminEdit, Pool: 1, PG: 7, Disks: []uint32{3, 1, 2}}} {
		addWire(f, func(pool core.BufPool, m *core.Msg) { _ = EncodeAdmin(pool, 1, 4, &a, m) })
	}
	fuzzNodeWire(f, func(pool core.BufPool, m *core.Msg) {
		_ = EncodeAdmin(pool, 1, 4, &Admin{Op: AdminPool, Pool: 1, Spec: chunkstore.PoolSpec{Replicas: new(2), Overcommit: new(true), DiskClass: "ssd"}}, m)
	}, func(pool core.BufPool, m *core.Msg) (core.Msg, bool) {
		var again core.Msg
		g, _, tok, _ := Peek(m)
		a, err := DecodeAdmin(pool, m)
		if err != nil {
			return again, false
		}
		if err := EncodeAdmin(pool, g, tok, &a, &again); err != nil {
			panic(err)
		}
		return again, true
	})
}

func FuzzDecodeAdminAnswer(f *testing.F) {
	for _, a := range []AdminAnswer{{}, {Refused: "disk 2: chunkstore: disk not down"}, {Failed: []FailedDevice{{Node: 2, Device: 9}, {Node: 3, Device: 1 << 40}}},
		{GiveUp: chunkstore.GiveUp{Interval: 3, All: true}}, {GiveUp: chunkstore.GiveUp{Interval: 9, Drop: true, Chunks: []core.ChunkID{{Volume: 1, Inode: 2, Index: 3}}}, Unfound: 70}} {
		addWire(f, func(pool core.BufPool, m *core.Msg) { _ = EncodeAdminAnswer(pool, 1, 4, &a, m) })
	}
	fuzzNodeWire(f, func(pool core.BufPool, m *core.Msg) {
		_ = EncodeAdminAnswer(pool, 1, 4, &AdminAnswer{Refused: "no", Failed: []FailedDevice{{Node: 2, Device: 9}}}, m)
	}, func(pool core.BufPool, m *core.Msg) (core.Msg, bool) {
		var again core.Msg
		g, _, tok, _ := Peek(m)
		a, err := DecodeAdminAnswer(pool, m)
		if err != nil {
			return again, false
		}
		if err := EncodeAdminAnswer(pool, g, tok, &a, &again); err != nil {
			panic(err)
		}
		return again, true
	})
}

// An edit staged while the commit in flight moves its PG is dropped by the next step: neither a reorder nor a pin naming
// no disk, which would take the members of the table it was staged on, undoes the move.
func TestAnEditOfAPGMovedInFlightIsDropped(t *testing.T) {
	r, in := poolRun(t)
	r.heal(true)
	r.settle(in)
	in.Moves = 16
	if err := r.ctl.Drain(1, true); err != nil {
		t.Fatal(err)
	}
	in.Now = r.now
	if !r.propose(in) {
		t.Fatal("nothing proposed")
	}
	for i, e := range r.ctl.Table().Pools[0].PGs {
		ed := chunkstore.Edit{PG: core.PGID{Pool: 1, Index: uint32(i)}, Pinned: i%2 == 1}
		if i%2 == 0 {
			ed.Disks = memberDisks(&e)
			slices.Reverse(ed.Disks)
		}
		if err := r.ctl.Edit(ed); err != nil {
			t.Fatalf("edit %+v: %v", ed, err)
		}
	}
	if a := r.answer(); a.Err != nil || a.Result.FailedCheck >= 0 {
		t.Fatalf("the drain's commit answered %+v", a)
	}
	r.c.agree()
	r.settle(in)
	moved := 0
	for i, e := range r.ctl.Table().Pools[0].PGs {
		if slices.ContainsFunc(e.Members, func(m chunkstore.Member) bool { return m.Leaving }) {
			moved++
			if e.Pinned || !slices.Contains(e.Members, chunkstore.Member{Disk: 1, Leaving: true}) {
				t.Fatalf("an edit undid the move of PG %d: %+v", i, e)
			}
		}
	}
	if moved == 0 {
		t.Fatal("the drain moved no PG")
	}
	if len(r.ctl.edits) != 0 {
		t.Fatalf("edits still staged: %+v", r.ctl.edits)
	}
}

// A step its table's budget cuts keeps the edit of a PG it left out, which the next table applies; a commit that fails
// its checks keeps the edits it took, which the step after the reload applies.
func TestAnEditWaitsOutACutAndAFailedCommit(t *testing.T) {
	r := newCtlRun(t)
	in := &ControlInputs{Disks: disks(0, 1, 2, 3)}
	for id := range core.PoolID(4) {
		if err := r.ctl.SetPool(id+1, spec(3, 1<<13), in.Disks); err != nil {
			t.Fatal(err)
		}
		r.step(in)
	}
	pg := core.PGID{Pool: 4, Index: 1<<13 - 1}
	was := memberDisks(r.ctl.Table().Entry(pg))
	want := slices.Clone(was)
	slices.Reverse(want)
	if err := r.ctl.Edit(chunkstore.Edit{PG: pg, Disks: want}); err != nil {
		t.Fatal(err)
	}
	r.fail(1)
	r.step(in)
	if got := memberDisks(r.ctl.Table().Entry(pg)); !slices.Equal(got, was) || len(r.ctl.edits) != 1 {
		t.Fatalf("after the cut table, PG %v has members %v and %d edits staged", pg, got, len(r.ctl.edits))
	}
	for r.settle(in) {
	}
	if got := memberDisks(r.ctl.Table().Entry(pg)); !slices.Equal(got, want) || len(r.ctl.edits) != 0 {
		t.Fatalf("PG %v has members %v, %d edits staged", pg, got, len(r.ctl.edits))
	}
	stale := r.load(r.l)
	if err := r.ctl.Drain(4, true); err != nil {
		t.Fatal(err)
	}
	r.settle(in)
	r.ctl = stale
	if err := r.ctl.Edit(chunkstore.Edit{PG: pg, Disks: was}); err != nil {
		t.Fatal(err)
	}
	if !r.propose(in) {
		t.Fatal("nothing proposed")
	}
	if a := r.answer(); a.Result.FailedCheck < 0 || len(r.ctl.edits) != 1 {
		t.Fatalf("a racing commit answered %+v with %d edits staged", a, len(r.ctl.edits))
	}
	for r.settle(in) {
	}
	if got := memberDisks(r.ctl.Table().Entry(pg)); !slices.Equal(got, was) {
		t.Fatalf("after the reload, PG %v has members %v", pg, got)
	}
}

// The membership command is answered at once with what a node's report gets: here the shard map
// the leader committed once node 2 reported, its control-plane group's entry this host's
// configuration and its data group's voter on node 2's disk, and the loop of that disk.
func TestTheMembershipCommandAnswersTheShardMapAndTheDisks(t *testing.T) {
	r := newHostRun(t)
	r.lead()
	r.report(&NodeReport{Node: 2, Meta: true, Opened: []OpenedDisk{{Device: 1, Log: 0x77, Blocks: 1 << 20, Loop: 1}}})
	r.run(StepInterval * 5)
	r.answers()
	r.admin(1, Admin{Op: AdminMembership})
	var m Membership
	n := 0
	for _, a := range r.sent {
		if _, k, _, _ := Peek(&a.Msg); a.Peer == operator && k == KindDisks {
			if err := DecodeDisks(r.pool, &a.Msg, &m); err != nil {
				t.Fatal(err)
			}
			n++
		}
	}
	r.answers()
	if n != 1 || len(m.Disks) != 1 || m.Disks[0].At != (core.NodeID{Node: 2, Loop: 1}) {
		t.Fatalf("answered %d memberships, the last %+v", n, m)
	}
	want := []Group{{ID: hostGroup, Voters: []Replica{{ID: 1, Node: voterNode(1).Node}}, LastID: 1},
		{ID: hostGroup + 1, Start: ctlEnd, Voters: []Replica{{ID: 1, Node: 2, Disk: m.Disks[0].Disk}}, LastID: 1, Boot: 1}}
	if len(m.Plans) != 2 || string(appendGroup(nil, &m.Plans[0])) != string(appendGroup(nil, &want[0])) || string(appendGroup(nil, &m.Plans[1])) != string(appendGroup(nil, &want[1])) {
		t.Fatalf("the map is %+v, want %+v", m.Plans, want)
	}
}
