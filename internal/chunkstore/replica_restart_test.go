package chunkstore

import (
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// A primary whose store was opened again asks for a new interval of each PG it leads in its first table, holds writes and serves no reads in its old interval, even if told it activated there, and serves once it has peered in the new one; a replica on a freshly formatted disk asks nothing.
func TestARestartedPrimaryTakesANewIntervalBeforeItServes(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	if len(c.asks) != 0 {
		t.Fatalf("fresh disks asked %+v", c.asks)
	}
	ch := chunkIn(pg0, 4, 0)
	w := c.write(pg0, ch, 0, []byte{1})
	c.until(w)
	c.s.CrashRandom(0)
	c.s.Restart(0)
	c.settle()
	pg3 := core.PGID{Pool: testPool, Index: 3}
	for _, pg := range []core.PGID{pg0, pg3} {
		if !slices.Contains(c.asks, PGFields{Epoch: 1, PG: pg, Interval: 1}) {
			t.Fatalf("no ask for %v: %+v", pg, c.asks)
		}
	}
	if len(c.asks) != 2 {
		t.Fatalf("asks %+v", c.asks)
	}
	c.call(0, func(ctx *core.Context, r *Replica, out *ReplicaOutput) { r.Activate(ctx, pg0, 1, out) })
	held := c.write(pg0, ch, 1, []byte{2})
	if st := c.readStatus(pg0, ch); st != StatusRefused {
		t.Fatalf("a read in the old interval: %v", st)
	}
	if _, ok := c.cl.replies[held]; ok {
		t.Fatal("a write answered in the old interval")
	}
	c.publish(c.next(func(t *Table) { setInterval(t, pg0, 2); setInterval(t, pg3, 2) }), true)
	c.until(held)
	if rp := c.reply(held); rp.Status != StatusOK || rp.Version != MakeVersion(2, 1) {
		t.Fatalf("the held write answered %+v", rp)
	}
	c.runFor(500_000)
	if st := c.readStatus(pg0, ch); st != StatusOK {
		t.Fatalf("a read in the new interval: %v", st)
	}
}
