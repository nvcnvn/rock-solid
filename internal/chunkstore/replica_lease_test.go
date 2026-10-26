package chunkstore

import (
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/lease"
	"github.com/nvcnvn/rock-solid/internal/sim/clock"
)

// readStatus reads a byte of ch through pg's primary and returns the reply's status.
func (c *rcluster) readStatus(pg core.PGID, ch core.ChunkID) Status {
	c.t.Helper()
	id := c.read(pg, ch, 0, 1, c.primary(pg))
	c.until(id)
	return c.reply(id).Status
}

// A grantor that adopted a newer table refuses to renew a holder that reports an older one, so the holder's lease on it lapses within a lease and its primary serves no reads, while it still takes writes; once the holder holds the table, the grantor renews and reads resume.
func TestAPrimaryReadsOnlyWithALeaseFromEveryUpMember(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	ch := chunkIn(pg0, 4, 0)
	if st := c.readStatus(pg0, ch); st != StatusOK {
		t.Fatalf("a read with every lease: %v", st)
	}
	newer := c.next(func(t *Table) {})
	c.call(2, func(ctx *core.Context, r *Replica, out *ReplicaOutput) { r.SetTable(ctx, newer, out) })
	c.runFor(lease.DefaultConfig.Lease + 100_000)
	if st := c.readStatus(pg0, ch); st != StatusRefused {
		t.Fatalf("a read without disk 3's lease: %v", st)
	}
	if c.fetch[0] != newer.Epoch {
		t.Fatalf("the refused holder fetched epoch %d", c.fetch[0])
	}
	w := c.write(pg0, ch, 0, []byte{7})
	c.until(w)
	if rp := c.reply(w); rp.Status != StatusOK {
		t.Fatalf("a write without disk 3's lease: %+v", rp)
	}
	c.publish(newer, false)
	c.runFor(lease.DefaultConfig.Heartbeat + 200_000)
	if st := c.readStatus(pg0, ch); st != StatusOK {
		t.Fatalf("a read once the holder holds the table: %v", st)
	}
}

// A member marked down in a new interval is no longer a grantor the primary needs once it has re-peered.
func TestAMemberMarkedDownGrantsNoLeaseThePrimaryNeeds(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	ch := chunkIn(pg0, 4, 0)
	c.s.Partition(0, 2, false)
	c.runFor(lease.DefaultConfig.Lease + 100_000)
	if st := c.readStatus(pg0, ch); st != StatusRefused {
		t.Fatalf("a read with disk 3 cut off and up: %v", st)
	}
	c.publish(c.next(func(t *Table) { mark(t, pg0, 3, MarkDown) }), true)
	if st := c.readStatus(pg0, ch); st != StatusOK {
		t.Fatalf("a read once disk 3 is down: %v", st)
	}
}

// A primary serves no reads in an interval with fewer than min_live live members, though it needs no lease there; maintenance counts as live.
func TestAPrimaryBelowMinLiveServesNoReads(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	ch := chunkIn(pg0, 4, 0)
	c.publish(c.next(func(t *Table) { mark(t, pg0, 2, MarkDown); mark(t, pg0, 3, MarkMaint) }), true)
	if st := c.readStatus(pg0, ch); st != StatusOK {
		t.Fatalf("a read with a member in maintenance: %v", st)
	}
	c.publish(c.next(func(t *Table) { mark(t, pg0, 3, MarkDown) }), true)
	if st := c.readStatus(pg0, ch); st != StatusRefused {
		t.Fatalf("a read with one live member under min_live 2: %v", st)
	}
}

// A primary stopped in place with its clocks frozen sees no Resumed; the heartbeats queued for it arrive ahead of a read sent after them, trip the step check and end its leases, so the read is refused rather than served on a lease its grantors no longer keep.
func TestHeartbeatsQueuedBehindAFrozenPauseEndTheLeasesBeforeAReadBehindThem(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	ch := chunkIn(pg0, 4, 0)
	if st := c.readStatus(pg0, ch); st != StatusOK {
		t.Fatalf("a read before the pause: %v", st)
	}
	c.s.Pause(0, clock.PauseFrozen, 10_000_000, false)
	c.runFor(9_000_000)
	id := c.read(pg0, ch, 0, 1, 0)
	c.until(id)
	if st := c.reply(id).Status; st != StatusRefused {
		t.Fatalf("a read queued behind the heartbeats: %v", st)
	}
	c.runFor(lease.DefaultConfig.RateGap + 50_000)
	if st := c.readStatus(pg0, ch); st != StatusOK {
		t.Fatalf("a read after the new window's bound: %v", st)
	}
}

// A node that restarts grants nothing until both its clocks show one lease, so its PGs' primaries lose their leases on it until then; Resumed ends every lease a primary holds, and it reads again about rate_gap later.
func TestTheRestartWaitAndResumedStopReads(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	ch := chunkIn(pg0, 4, 0)
	c.s.CrashRandom(1)
	c.s.Restart(1)
	c.runFor(lease.DefaultConfig.Lease - 10_000)
	if !c.reps[1].grantor.Waiting(lease.Now(&core.Context{Now: c.s.Node(1).Context().Now, Raw: c.s.Node(1).Context().Raw})) {
		t.Fatal("the restarted node's wait is over before a lease")
	}
	c.runFor(30_000)
	if st := c.readStatus(pg0, ch); st != StatusRefused {
		t.Fatalf("a read leaning on the restarted node's forgotten grant: %v", st)
	}
	c.runFor(lease.DefaultConfig.Heartbeat + 200_000)
	if st := c.readStatus(pg0, ch); st != StatusOK {
		t.Fatalf("a read after the restart wait: %v", st)
	}
	c.s.Node(0).Feed(c.loops[0], core.Event{Kind: core.EvResumed})
	if st := c.readStatus(pg0, ch); st != StatusRefused {
		t.Fatalf("a read after Resumed: %v", st)
	}
	c.runFor(lease.DefaultConfig.RateGap + 10_000)
	if st := c.readStatus(pg0, ch); st != StatusOK {
		t.Fatalf("a read rate_gap after Resumed: %v", st)
	}
}

// Interval 2 of PG 0 keeps disk 1 as primary, and disk 3's acknowledgments of its missing list are
// lost, so disk 1 does not activate: it holds a write but goes on serving reads of committed versions,
// answering a chunk the last peering found unfound as unfound.
func TestAPrimaryThatKeepsLeadingServesReadsWhileItPeers(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	ch, lost := chunkIn(pg0, 4, 0), chunkIn(pg0, 4, 1)
	c.until(c.write(pg0, ch, 0, []byte{1}))
	l := &c.reps[0].leads[c.reps[0].lead(pg0)]
	l.kept = []keptChunk{{chunk: lost, kept: MakeVersion(1, 9), unfound: true}}
	c.drop = func(d int, a *core.Action) bool {
		return d == 2 && a.Kind == core.ActNetSend && a.Msg.Len >= headSize && Kind(a.Msg.Body[1]) == KindPersistAck
	}
	c.publish(c.next(func(t *Table) { setInterval(t, pg0, 2) }), false)
	w := c.write(pg0, ch, 0, []byte{2})
	c.runFor(RetryAfter)
	if c.reps[0].leads[c.reps[0].lead(pg0)].active {
		t.Fatal("disk 1 activated interval 2")
	}
	if got := c.readNow(pg0, ch, 0, 1); got[0] != 1 {
		t.Fatalf("read %v", got)
	}
	unfound := c.read(pg0, lost, 0, 1, 0)
	c.until(unfound)
	if rp := c.reply(unfound); rp.Status != StatusUnfound {
		t.Fatalf("the unfound chunk read %+v", rp)
	}
	if rp, ok := c.cl.replies[w]; ok {
		t.Fatalf("the write was answered %+v", rp)
	}
}

// Disk 1 adopts a table while it still places disk 3 at disk 2's loop, then learns disk 3's loop:
// it asks disk 3's loop for leases from then on and goes on serving reads.
func TestAPrimaryAsksAMemberLearnedAfterItsTableForALease(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	newer := c.next(func(*Table) {})
	c.call(0, func(ctx *core.Context, r *Replica, out *ReplicaOutput) {
		r.SetAddr(3, c.loops[1])
		r.SetTable(ctx, newer, out)
		r.SetAddr(3, c.loops[2])
	})
	c.publish(newer, false)
	c.runFor(lease.DefaultConfig.Lease + 500_000)
	if st := c.readStatus(pg0, chunkIn(pg0, 4, 0)); st != StatusOK {
		t.Fatalf("a read: %v", st)
	}
}
