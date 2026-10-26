package chunkstore

import (
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/lease"
)

// moveOff is interval 2 of PG 0 moving its primary from disk 1, marked down, to disk 2.
func moveOff(t *Table) {
	setInterval(t, pg0, 2)
	_, e := t.entry(pg0)
	e.Members = []Member{{Disk: 2}, {Disk: 3}, {Disk: 1, Mark: MarkDown}}
}

// Interval 2 moves PG 0's primary off disk 1, which disks 2 and 3 grant a lease. If disk 1 adopts
// the table too, its next lease request reports it and peering waits at most a heartbeat; if it
// does not, the grantors join peering only once their grants to it end, 3 to 4 s later.
func TestAGrantorJoinsNoPeeringMovingThePrimaryOffAHolder(t *testing.T) {
	for _, adopted := range []bool{true, false} {
		c := newRCluster(t, rtable(4, 2))
		next := c.next(moveOff)
		start := c.s.Time()
		if adopted {
			c.publish(next, true)
		} else {
			c.table = next
			for d := 1; d <= 2; d++ {
				c.call(d, func(ctx *core.Context, r *Replica, out *ReplicaOutput) { r.SetTable(ctx, next, out) })
			}
			c.untilActive()
		}
		took := c.s.Time() - start
		if adopted && took > lease.DefaultConfig.Heartbeat+200_000 || !adopted && (took < 2_900_000 || took > lease.DefaultConfig.Lease+200_000) {
			t.Fatalf("adopted %v: peering took %d µs", adopted, took)
		}
	}
}

// A member that restarted joins no peering until both its clocks show one lease: interval 2 of PG
// 0, published half a second after disk 3 restarted, activates only once disk 3's wait is over.
func TestARestartedMemberJoinsNoPeeringDuringItsWait(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	c.s.CrashRandom(2)
	c.s.Restart(2)
	for !c.s.Node(2).Up() && c.step() {
	}
	start := c.s.Time()
	c.runFor(500_000)
	c.publish(c.next(func(t *Table) { setInterval(t, pg0, 2) }), true)
	if took := c.s.Time() - start; took < 3_800_000 {
		t.Fatalf("peering ended %d µs after disk 3 restarted", took)
	}
}

// A primary reports each activation on every heartbeat until a table records it, then stops.
func TestAnActivationIsReportedUntilATableRecordsIt(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	want := PGFields{Epoch: 1, PG: pg0, Interval: 1}
	count := func() int {
		return len(slices.DeleteFunc(slices.Clone(c.activations), func(a PGFields) bool { return a != want }))
	}
	before := count()
	c.runFor(2*RetryAfter + 100_000)
	if before == 0 || count() < before+2 {
		t.Fatalf("reported %d times at boot and %d since", before, count()-before)
	}
	c.publish(c.next(func(*Table) {}), false)
	c.runFor(RetryAfter)
	before = count()
	c.runFor(2 * RetryAfter)
	if count() != before {
		t.Fatalf("reported %d times after a table recorded it", count()-before)
	}
}

// Disk 3, down in interval 1, becomes PG 0's primary in interval 2 and lacks 1.1, which peering
// keeps from disk 2: disk 3 lists it, a partial-block write to the chunk waits, while a whole-block
// write to it and a partial write to another chunk go ahead.
func TestAPartialWriteWaitsForTheKeptVersion(t *testing.T) {
	tbl := rtable(4, 2)
	_, e := tbl.entry(pg0)
	e.Members[2].Mark = MarkDown
	c := newRCluster(t, tbl)
	ch, other := chunkIn(pg0, 4, 0), chunkIn(pg0, 4, 1)
	c.until(c.write(pg0, ch, 0, []byte{1}))
	c.publish(c.next(func(t *Table) {
		setInterval(t, pg0, 2)
		_, e := t.entry(pg0)
		e.Members = []Member{{Disk: 3}, {Disk: 1, Mark: MarkDown}, {Disk: 2}}
	}), true)
	if x, _ := c.reps[2].Store().Index().Chunk(testPool, ch); x.ListedIn != 2 || x.Listed != MakeVersion(1, 1) {
		t.Fatalf("disk 3 lists %+v", x)
	}
	wait := c.write(pg0, ch, 10, []byte{2})
	whole := c.write(pg0, ch, core.BlockSize, make([]byte, core.BlockSize))
	elsewhere := c.write(pg0, other, 10, []byte{3})
	c.settle()
	if _, ok := c.cl.replies[wait]; ok {
		t.Fatal("the partial write went ahead")
	}
	for _, w := range []core.RequestID{whole, elsewhere} {
		if rp := c.reply(w); rp.Status != StatusOK {
			t.Fatalf("answered %+v", rp)
		}
	}
}

// Interval 2 of PG 0 runs under min_live 1 with only disk 1 live and takes a write; interval 3
// raises min_live to 2 and moves the primary to disk 2. Peering judges interval 2 by the min_live it
// ran under, so it waits for disk 1, crashed and then in its restart wait, and keeps 2.1 once disk 1
// answers.
func TestPeeringJudgesAPastIntervalByItsOwnMinLive(t *testing.T) {
	c := newRCluster(t, rtable(4, 1))
	ch := chunkIn(pg0, 4, 0)
	c.publish(c.next(func(t *Table) {
		mark(t, pg0, 2, MarkDown)
		_, e := t.entry(pg0)
		e.Members[2].Mark = MarkDown
	}), true)
	c.until(c.write(pg0, ch, 0, []byte{1}))
	c.s.CrashRandom(0)
	c.table = c.next(func(t *Table) {
		setInterval(t, pg0, 3)
		t.Pools[0].MinLive = 2
		_, e := t.entry(pg0)
		e.Members = []Member{{Disk: 2}, {Disk: 3}, {Disk: 1, Mark: MarkDown}}
	})
	for d := 1; d <= 2; d++ {
		c.call(d, func(ctx *core.Context, r *Replica, out *ReplicaOutput) { r.SetTable(ctx, c.table, out) })
	}
	c.runFor(2 * RetryAfter)
	if l := c.reps[1].leads[c.reps[1].lead(pg0)]; l.active {
		t.Fatal("interval 3 activated without disk 1, the only live member of interval 2")
	}
	if !c.s.Node(0).Up() {
		c.s.Restart(0)
	}
	c.untilActive()
	if x, _ := c.reps[1].Store().Index().Chunk(testPool, ch); x.ListedIn != 3 || x.Listed != MakeVersion(2, 1) {
		t.Fatalf("disk 2 lists %+v", x)
	}
}
