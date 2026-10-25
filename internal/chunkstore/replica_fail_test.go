package chunkstore

import (
	"errors"
	"math"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// replicaWrite sends disk node+1 a primary's whole-block write of block 0 of ch at version v, as if from its primary.
func (c *rcluster) replicaWrite(node int, ch core.ChunkID, v uint64) core.RequestID {
	c.nextID++
	id := c.nextID
	u := Update{Kind: KindReplicaWrite, ID: id, PG: c.pgFields(pg0), Chunk: ch, Len: core.BlockSize, Version: v}
	c.s.Node(c.client.Node).Call(c.client, func(ctx *core.Context, acts []core.Action) []core.Action {
		a := core.Action{Kind: core.ActNetSend, Peer: core.NodeID{Node: uint32(node)}}
		a.Msg.Len = uint16(copy(a.Msg.Body[:], AppendUpdate(nil, &u)))
		a.Msg.Buf, _ = ctx.Pool.AllocZeroed(core.BlockSize)
		c.cl.sent[id] = a.Msg.Buf
		return append(acts, a)
	})
	return id
}

// refuse makes disk 3 refuse pg0's writes, its table marking it in maintenance in the same interval, until the returned func restores the cluster's table.
func (c *rcluster) refuse() func() {
	refusing := cloneTable(c.table)
	_, e := refusing.entry(pg0)
	e.Members[2].Mark = MarkMaint
	c.call(2, func(ctx *core.Context, r *Replica, out *ReplicaOutput) { r.table = refusing })
	return func() { c.call(2, func(ctx *core.Context, r *Replica, out *ReplicaOutput) { r.table = c.table }) }
}

// A write ordered while a member's resend is due waits behind it: 1.2 never reaches disk 3 before 1.1.
func TestANewWriteToAMemberWaitsBehindTheResend(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	ch := chunkIn(pg0, 4, 0)
	c.s.Partition(0, 2, false)
	w1 := c.write(pg0, ch, 0, []byte{1})
	key := [2]core.NodeID{c.loops[0], c.loops[2]}
	for c.lost[key] == 0 && c.step() {
	}
	c.s.Heal(0, 2)
	w2 := c.write(pg0, ch, core.BlockSize, []byte{2})
	c.settle()
	for _, w := range []core.RequestID{w1, w2} {
		if rp := c.reply(w); rp.Status != StatusOK {
			t.Fatalf("answered %+v", rp)
		}
	}
	if b0, b1 := c.diskBlock(2, ch, 0), c.diskBlock(2, ch, 1); b0[0] != 1 || b1[0] != 2 {
		t.Fatalf("disk 3 maps block 0 as %x and block 1 as %x", b0[0], b1[0])
	}
}

// Disk 2 gets 1.1 twice and its disk fails the first: the second is answered with the first's failure, not acknowledged.
func TestADuplicateIsAnsweredWithItsOriginalsOutcome(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	ch := chunkIn(pg0, 4, 0)
	c.s.Node(1).Disk(0).Inject(sim.Fault{Effect: sim.FailIO, When: sim.OnKind(core.ActDiskWrite)})
	first, again := c.replicaWrite(1, ch, MakeVersion(1, 1)), c.replicaWrite(1, ch, MakeVersion(1, 1))
	c.settle()
	if rp := c.reply(first); rp.Status == StatusOK {
		t.Fatalf("a write the disk failed answered %+v", rp)
	}
	if rp := c.reply(again); rp.Status == StatusOK {
		t.Fatalf("its duplicate answered %+v", rp)
	}
}

// A write that waited behind a base read is ordered after a table that drops its pool: it is refused, not sent.
func TestAWriteOrderedAfterItsPGLeftTheTableIsRefused(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	ch := chunkIn(pg0, 4, 0)
	c.writeWhole(pg0, ch, 0, 1, 0xaa)
	w := c.write(pg0, ch, 10, []byte{1})
	for len(c.reps[0].vers) == 0 && c.step() {
	}
	gone := c.next(func(t *Table) { t.Pools = nil })
	c.call(0, func(ctx *core.Context, r *Replica, out *ReplicaOutput) { r.SetTable(ctx, gone, out) })
	c.settle()
	if rp := c.reply(w); rp.Status != StatusRefused {
		t.Fatalf("answered %+v", rp)
	}
}

// Both edge blocks of a write fail their CRC32C at the primary: both come from another member.
func TestTwoRottenEdgeBlocksAreBothFetched(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	ch := chunkIn(pg0, 4, 0)
	c.writeWhole(pg0, ch, 2, 2, 0xaa)
	for _, b := range []uint16{2, 3} {
		e, _ := c.reps[0].Store().Index().Lookup(testPool, ch, b)
		c.s.Node(0).Disk(0).FlipBit(e.BlockLBA(b), 4000, 1)
	}
	data := make([]byte, core.BlockSize)
	for i := range data {
		data[i] = 1
	}
	w := c.write(pg0, ch, 2*core.BlockSize+10, data)
	c.settle()
	if rp := c.reply(w); rp.Status != StatusOK {
		t.Fatalf("answered %+v", rp)
	}
	for d := range 3 {
		b2, b3 := c.diskBlock(d, ch, 2), c.diskBlock(d, ch, 3)
		if b2[9] != 0xaa || b2[10] != 1 || b3[9] != 1 || b3[10] != 0xaa || b3[4000] != 0xaa {
			t.Fatalf("disk %d merged % x and % x", d+1, b2[9:11], []byte{b3[9], b3[10], b3[4000]})
		}
	}
}

// Disk 3 refuses 1.1 of an absent chunk: the client hears a refusal, the primary asks for interval 2, orders nothing more in 1 and writes nothing over the absent chunk. Peering, standing in, drops 1.1, and the write held meanwhile is 2.1 on every member, merged on no trace of 1.1.
func TestAFailedVersionEndsTheInterval(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	ch := chunkIn(pg0, 4, 0)
	restore := c.refuse()
	w := c.write(pg0, ch, 10, []byte{1, 2, 3})
	c.settle()
	if rp := c.reply(w); rp.Status != StatusRefused {
		t.Fatalf("answered %+v", rp)
	}
	if _, ok := c.reps[0].Store().Index().Chunk(testPool, ch); ok {
		t.Fatal("the primary maps the absent chunk")
	}
	if len(c.asks) != 1 || c.asks[0] != (PGFields{Epoch: 1, PG: pg0, Interval: 1}) {
		t.Fatalf("asks %+v", c.asks)
	}
	restore()
	next := c.write(pg0, ch, 20, []byte{4})
	c.settle()
	if rp, ok := c.cl.replies[next]; ok {
		t.Fatalf("a write after the failure answered %+v in interval 1", rp)
	}
	c.publish(c.next(func(t *Table) { setInterval(t, pg0, 2) }), false)
	c.call(0, func(ctx *core.Context, r *Replica, out *ReplicaOutput) {
		r.Settle(ctx, ch, 0, out)
		r.Activate(ctx, pg0, 2, out)
	})
	c.settle()
	if rp := c.reply(next); rp.Status != StatusOK || rp.Version != MakeVersion(2, 1) {
		t.Fatalf("the held write answered %+v once peering dropped 1.1", rp)
	}
	for d := range 3 {
		if got := c.diskBlock(d, ch, 0); got[10] != 0 || got[20] != 4 {
			t.Fatalf("disk %d maps % x", d+1, []byte{got[10], got[20]})
		}
	}
}

// 1.1 waits for cut-off disk 3 when interval 2 marks it down: peering, standing in, keeps 1.1, which is then mapped and acknowledged.
func TestPeeringKeepsAPendingVersionOfAnEndedInterval(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	ch := chunkIn(pg0, 4, 0)
	c.s.Partition(0, 2, false)
	w := c.write(pg0, ch, 0, []byte{1})
	c.runFor(100_000)
	c.publish(c.next(func(t *Table) { mark(t, pg0, 3, MarkDown) }), false)
	c.settle()
	if _, ok := c.cl.replies[w]; ok {
		t.Fatal("answered before peering")
	}
	c.call(0, func(ctx *core.Context, r *Replica, out *ReplicaOutput) {
		r.Settle(ctx, ch, MakeVersion(1, 1), out)
		r.Activate(ctx, pg0, 2, out)
	})
	c.settle()
	if rp := c.reply(w); rp.Status != StatusOK || rp.Version != MakeVersion(1, 1) {
		t.Fatalf("answered %+v", rp)
	}
	if got := c.readNow(pg0, ch, 0, 1); got[0] != 1 {
		t.Fatalf("read %d", got[0])
	}
}

// Disk 2 is in maintenance, so disk 3 is the only member 1.1 is sent to, and it refuses it: the refusal ends the version without the primary failing.
func TestARefusalFromTheOnlyMemberSentToEndsTheVersion(t *testing.T) {
	tbl := rtable(4, 2)
	_, e := tbl.entry(pg0)
	e.Members[1].Mark = MarkMaint
	c := newRCluster(t, tbl)
	ch := chunkIn(pg0, 4, 0)
	c.refuse()
	w := c.write(pg0, ch, 0, []byte{1})
	c.settle()
	if rp := c.reply(w); rp.Status != StatusRefused {
		t.Fatalf("answered %+v", rp)
	}
}

// Disk 2 logged 1.1 and acknowledged it. Sent again once that copy is done, 1.1 is refused, since after a failure or a restart the member may hold only some of its records, and 1.2 after it is refused too.
func TestAVersionSentAgainAfterItsCopyIsDoneIsRefused(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	ch := chunkIn(pg0, 4, 0)
	first := c.replicaWrite(1, ch, MakeVersion(1, 1))
	c.settle()
	if rp := c.reply(first); rp.Status != StatusOK {
		t.Fatalf("1.1 answered %+v", rp)
	}
	again := c.replicaWrite(1, ch, MakeVersion(1, 1))
	c.settle()
	next := c.replicaWrite(1, ch, MakeVersion(1, 2))
	c.settle()
	for _, w := range []core.RequestID{again, next} {
		if rp := c.reply(w); rp.Status != StatusRefused {
			t.Fatalf("answered %+v", rp)
		}
	}
}

// A partial write waits for its base read when interval 2 begins, and peering, standing in, settles the chunk at 1.1 and activates before the read completes: the waiting write is refused and never ordered, and the next write is 2.1 on every member.
func TestAWriteWaitingWhenItsIntervalEndsIsRefused(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	ch := chunkIn(pg0, 4, 0)
	c.writeWhole(pg0, ch, 0, 1, 0xaa)
	w := c.write(pg0, ch, 10, []byte{1})
	for len(c.reps[0].vers) == 0 && c.step() {
	}
	c.publish(c.next(func(t *Table) { setInterval(t, pg0, 2) }), false)
	c.call(0, func(ctx *core.Context, r *Replica, out *ReplicaOutput) {
		r.Settle(ctx, ch, MakeVersion(1, 1), out)
		r.Activate(ctx, pg0, 2, out)
	})
	next := c.write(pg0, ch, 20, []byte{2})
	c.settle()
	if rp := c.reply(w); rp.Status != StatusRefused {
		t.Fatalf("the waiting write answered %+v", rp)
	}
	if rp := c.reply(next); rp.Status != StatusOK || rp.Version != MakeVersion(2, 1) {
		t.Fatalf("the next write answered %+v", rp)
	}
	for d := range 3 {
		if got := c.diskBlock(d, ch, 0); got[10] != 0xaa || got[20] != 2 {
			t.Fatalf("disk %d maps % x", d+1, []byte{got[10], got[20]})
		}
	}
}

// A partial write of chunk A waits for its base read when a write to chunk B spends B's counter, which ends the interval: the waiting write is refused at once.
func TestAWriteWaitingWhenAnotherChunkEndsTheIntervalIsRefused(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	a, b := chunkIn(pg0, 4, 0), chunkIn(pg0, 4, 1)
	c.writeWhole(pg0, a, 0, 1, 0xaa)
	c.call(0, func(ctx *core.Context, r *Replica, out *ReplicaOutput) {
		buf, _ := ctx.Pool.AllocZeroed(core.BlockSize)
		r.Store().Write(ctx, Request{ID: 1 << 60, Chunk: b, Len: core.BlockSize, Buf: buf, Version: MakeVersion(1, math.MaxUint32)}, &r.sout)
		r.drain(ctx, out)
	})
	c.settle()
	w := c.write(pg0, a, 10, []byte{1})
	for len(c.reps[0].vers) == 0 && c.step() {
	}
	c.nextID++
	spend := c.nextID
	c.call(0, func(ctx *core.Context, r *Replica, out *ReplicaOutput) {
		u := Update{Kind: KindWrite, ID: spend, PG: c.pgFields(pg0), Session: 1, Incarnation: 1, Chunk: b, Len: core.BlockSize}
		ev := core.Event{Kind: core.EvNetRecv, Peer: c.client}
		ev.Msg.Len = uint16(copy(ev.Msg.Body[:], AppendUpdate(nil, &u)))
		ev.Msg.Buf, _ = ctx.Pool.AllocZeroed(core.BlockSize)
		r.OnEvent(ctx, &ev, out)
	})
	if slices.ContainsFunc(c.reps[0].vers, func(v version) bool { return v.msgID == w && !v.done }) {
		t.Fatal("the waiting write is still pending once the interval ended")
	}
	c.settle()
	if rp := c.reply(w); rp.Status != StatusRefused {
		t.Fatalf("the waiting write answered %+v", rp)
	}
	if v := c.newest(1, a); v != MakeVersion(1, 1) {
		t.Fatalf("disk 2 holds %#x", v)
	}
}

// Settle is refused once the primary has activated the PG's interval, since the chunk's next versions could then take the numbers of those it drops.
func TestSettleIsRefusedOnceThePGIsActive(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	ch := chunkIn(pg0, 4, 0)
	c.writeWhole(pg0, ch, 0, 1, 1)
	var err error
	c.call(0, func(ctx *core.Context, r *Replica, out *ReplicaOutput) { err = r.Settle(ctx, ch, 0, out) })
	if !errors.Is(err, ErrActive) {
		t.Fatalf("Settle in an active interval: %v", err)
	}
}
