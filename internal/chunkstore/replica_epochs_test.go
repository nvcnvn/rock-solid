package chunkstore

import (
	"bytes"
	"math"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// Writes a cut-off member missed are sent again once it is reachable, in the order they were ordered; the resendNewestFirst plant breaks the send-order invariant.
func TestWritesAreSentAgainInOrderAfterAReconnect(t *testing.T) {
	for _, plant := range []bool{false, true} {
		c := newRCluster(t, rtable(4, 2))
		c.reps[0].plant.resendNewestFirst = plant
		if plant {
			c.inv.Off(invSendOrder)
		}
		a, b := chunkIn(pg0, 4, 0), chunkIn(pg0, 4, 1)
		c.s.Partition(0, 2, false)
		ws := []core.RequestID{c.write(pg0, a, 0, []byte{1}), c.write(pg0, a, core.BlockSize, []byte{2}), c.write(pg0, b, 0, []byte{3})}
		c.runFor(50_000)
		c.s.Heal(0, 2)
		c.settle()
		for _, w := range ws {
			if rp := c.reply(w); rp.Status != StatusOK {
				t.Fatalf("plant %v: write answered %+v", plant, rp)
			}
		}
		if c.newest(2, a) != MakeVersion(1, 2) || c.newest(2, b) != MakeVersion(1, 1) {
			t.Fatalf("plant %v: the member that missed them holds %#x and %#x", plant, c.newest(2, a), c.newest(2, b))
		}
		if plant != (c.orderErr != nil) {
			t.Fatalf("plant %v: %v", plant, c.orderErr)
		}
	}
}

// PG 7 is led by disk 2. Its writes are held while it re-peers; they are ordered again in the new interval if disk 2 led every interval since, and refused otherwise.
func TestHeldWritesAreOrderedAgainOnlyIfNoOtherPrimaryLed(t *testing.T) {
	pg7 := core.PGID{Pool: testPool, Index: 7}
	for _, kept := range []bool{true, false} {
		c := newRCluster(t, rtable(8, 2))
		ch := chunkIn(pg7, 8, 0)
		c.publish(c.next(func(t *Table) { mark(t, pg7, 3, MarkDown) }), false)
		w := c.write(pg7, ch, 0, []byte{1})
		c.settle()
		if _, ok := c.cl.replies[w]; ok {
			t.Fatal("a write was taken before the primary re-peered")
		}
		jump := c.next(func(t *Table) {
			_, e := t.entry(pg7)
			past := []Member{{Disk: 2}, {Disk: 3}, {Disk: 1}}
			if !kept {
				past = []Member{{Disk: 3}, {Disk: 1}, {Disk: 2}}
			}
			e.Activated, e.Interval = 1, 4
			e.Past = []PastInterval{{Interval: 2, Members: e.Members}, {Interval: 3, Members: past}}
			e.Members = []Member{{Disk: 2}, {Disk: 3}, {Disk: 1}}
		})
		jump.Epoch += 2
		c.publish(jump, true)
		c.settle()
		rp := c.reply(w)
		if kept && (rp.Status != StatusOK || rp.Version != MakeVersion(4, 1)) || !kept && (rp.Status != StatusRefused || rp.Interval != 4) {
			t.Fatalf("kept %v: the held write answered %+v", kept, rp)
		}
	}
}

// A chunk at 7.(2³² − 3) takes 5 writes: 1 and 2 get 7.(2³² − 2) and 7.(2³² − 1); 3 waits until the PG's pending writes commit and interval 8 is peered, then 3 to 5 get 8.1 to 8.3. Other PGs see no refusal.
func TestASpentCounterAsksForTheNextInterval(t *testing.T) {
	tbl := rtable(4, 2)
	setInterval(&tbl, pg0, 7)
	c := newRCluster(t, tbl)
	ch := chunkIn(pg0, 4, 0)
	for d := range 3 {
		c.call(d, func(ctx *core.Context, r *Replica, out *ReplicaOutput) {
			buf, _ := ctx.Pool.AllocZeroed(core.BlockSize)
			r.Store().Write(ctx, Request{ID: 1 << 60, Chunk: ch, Len: core.BlockSize, Buf: buf, Version: MakeVersion(7, math.MaxUint32-2)}, &r.sout)
			r.drain(ctx, out)
		})
	}
	c.settle()
	var ws []core.RequestID
	for i := range 5 {
		ws = append(ws, c.write(pg0, ch, uint32(i), []byte{byte(i + 1)}))
	}
	pg1 := core.PGID{Pool: testPool, Index: 1}
	other := c.write(pg1, chunkIn(pg1, 4, 0), 0, []byte{9})
	c.settle()
	if rp := c.reply(other); rp.Status != StatusOK {
		t.Fatalf("another PG's write answered %+v", rp)
	}
	for i, v := range []uint64{MakeVersion(7, math.MaxUint32-1), MakeVersion(7, math.MaxUint32)} {
		if rp := c.reply(ws[i]); rp.Status != StatusOK || rp.Version != v {
			t.Fatalf("write %d answered %+v", i+1, rp)
		}
	}
	if _, ok := c.cl.replies[ws[2]]; ok || len(c.asks) != 1 || c.asks[0].PG != pg0 || c.asks[0].Interval != 7 {
		t.Fatalf("write 3 answered before a new interval; asks %+v", c.asks)
	}
	c.publish(c.next(func(t *Table) { setInterval(t, pg0, 8) }), true)
	c.settle()
	for i := 2; i < 5; i++ {
		if rp := c.reply(ws[i]); rp.Status != StatusOK || rp.Version != MakeVersion(8, uint32(i-1)) {
			t.Fatalf("write %d answered %+v", i+1, rp)
		}
	}
	if got := c.readNow(pg0, ch, 0, 5); !bytes.Equal(got, []byte{1, 2, 3, 4, 5}) {
		t.Fatalf("read % x", got)
	}
}

func TestTablesAreAppliedAsDeltasAndFetchedWholeAfterAGap(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	next := c.next(func(t *Table) { mark(t, pg0, 3, MarkDown) })
	d := MakeDelta(&c.table, &next)
	c.call(0, func(ctx *core.Context, r *Replica, out *ReplicaOutput) { r.ApplyDelta(ctx, &d, out) })
	if got := c.reps[0].Table(); got.Epoch != next.Epoch || got.CRC() != next.CRC() || c.fetch[0] != 0 {
		t.Fatalf("after the delta: epoch %d, fetch %d", got.Epoch, c.fetch[0])
	}
	c.table = next
	after := c.next(func(t *Table) {})
	gap := c.next(func(t *Table) { t.Epoch++ })
	g := MakeDelta(&after, &gap)
	c.call(1, func(ctx *core.Context, r *Replica, out *ReplicaOutput) { r.ApplyDelta(ctx, &g, out) })
	if c.fetch[1] != gap.Epoch || !c.whole[1] || c.reps[1].Table().Epoch != 1 {
		t.Fatalf("a gap: fetch %d whole %v", c.fetch[1], c.whole[1])
	}
	bad := MakeDelta(&c.table, &after)
	bad.CRC++
	c.call(0, func(ctx *core.Context, r *Replica, out *ReplicaOutput) { r.ApplyDelta(ctx, &bad, out) })
	if c.fetch[0] != after.Epoch || !c.whole[0] || c.reps[0].Table().Epoch != next.Epoch {
		t.Fatalf("a CRC mismatch: fetch %d whole %v", c.fetch[0], c.whole[0])
	}
}
