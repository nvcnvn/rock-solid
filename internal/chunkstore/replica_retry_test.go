package chunkstore

import (
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// A primary's ask for a new interval and a replica's fetch of a newer table are sent again every RetryAfter until a table answers them, and no more once one has.
func TestAsksAndFetchesAreSentAgainUntilATableAnswers(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	ch := chunkIn(pg0, 4, 0)
	restore := c.refuse()
	c.write(pg0, ch, 10, []byte{1})
	c.settle()
	restore()
	if len(c.asks) != 1 {
		t.Fatalf("asks %+v", c.asks)
	}
	c.runFor(3 * RetryAfter)
	if len(c.asks) < 4 || len(c.asks) > 5 || slices.ContainsFunc(c.asks, func(a PGFields) bool { return a != c.asks[0] }) {
		t.Fatalf("after 3 s, asks %+v", c.asks)
	}
	c.publish(c.next(func(t *Table) { setInterval(t, pg0, 2) }), false)
	n := len(c.asks)
	c.runFor(3 * RetryAfter)
	if len(c.asks) != n {
		t.Fatalf("asks after a table answered: %+v", c.asks)
	}

	next := c.next(func(t *Table) {})
	gap := c.next(func(t *Table) { t.Epoch += 2 })
	g := MakeDelta(&next, &gap)
	c.call(1, func(ctx *core.Context, r *Replica, out *ReplicaOutput) { r.ApplyDelta(ctx, &g, out) })
	before := c.fetches[1]
	c.runFor(2 * RetryAfter)
	if n := c.fetches[1] - before; n < 2 || n > 3 || c.fetch[1] != gap.Epoch || !c.whole[1] {
		t.Fatalf("fetches %d after %d, for epoch %d whole %v", c.fetches[1], before, c.fetch[1], c.whole[1])
	}
	c.publish(gap, false)
	n = c.fetches[1]
	c.runFor(3 * RetryAfter)
	if c.fetches[1] != n {
		t.Fatalf("fetches after the table arrived: %d", c.fetches[1])
	}
}
