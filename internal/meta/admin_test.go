package meta

import (
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

func TestAdminCrossesTheWireWhole(t *testing.T) {
	pool := sim.NewPool(1<<20, 0)
	full := chunkstore.PoolSpec{Replicas: new(3), MinLive: new(2), Data: new(4), Parity: new(2), EncodeOnWrite: new(true), MaxPerDomain: new(1),
		Overcommit: new(false), WriteLimit: new(uint8(90)), PGCount: new(uint32(1 << 31)), Sealed: "ec", FailureDomain: "disk", Locality: "node", DiskClass: "nvme"}
	for _, a := range []Admin{{Op: AdminLost, Disk: 1 << 31}, {Op: AdminRemove, Disk: 1}, {Op: AdminFailed}, {Op: AdminPool, Pool: 7}, {Op: AdminPool, Pool: 1, Spec: full}} {
		var m core.Msg
		if err := EncodeAdmin(pool, 1, 5, &a, &m); err != nil {
			t.Fatal(err)
		}
		got, err := DecodeAdmin(pool, &m)
		if _, _, tok, _ := Peek(&m); err != nil || tok != 5 || string(appendSpec(nil, &got.Spec)) != string(appendSpec(nil, &a.Spec)) || got.Op != a.Op || got.Disk != a.Disk || got.Pool != a.Pool {
			t.Fatalf("sent %+v, decoded %+v, %v", a, got, err)
		}
	}
	var fs []FailedDevice
	for i := range 100 {
		fs = append(fs, FailedDevice{Node: uint32(i) << 24, Device: uint64(i) << 56})
	}
	for _, a := range []AdminAnswer{{}, {Refused: strings.Repeat("x", 300)}, {Failed: fs}} {
		var m core.Msg
		if err := EncodeAdminAnswer(pool, 1, 5, &a, &m); err != nil {
			t.Fatal(err)
		}
		got, err := DecodeAdminAnswer(pool, &m)
		if err != nil || got.Refused != a.Refused || !slices.Equal(got.Failed, a.Failed) {
			t.Fatalf("sent %+v, decoded %+v, %v", a, got, err)
		}
		free(&core.Context{Pool: pool}, &m)
	}
}

func FuzzDecodeAdmin(f *testing.F) {
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
