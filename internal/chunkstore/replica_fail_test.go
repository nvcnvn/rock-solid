package chunkstore

import (
	"errors"
	"math"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/lsm"
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

// The primary's copy of a base is rotten and disk 2, which it asks first, is cut off: once the connection is lost it asks disk 3.
func TestABaseIsAskedOfTheNextMemberWhenOneIsLost(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	ch := chunkIn(pg0, 4, 0)
	c.writeWhole(pg0, ch, 2, 1, 0xaa)
	e, _ := c.reps[0].Store().Index().Lookup(testPool, ch, 2)
	c.s.Node(0).Disk(0).FlipBit(e.BlockLBA(2), 4000, 1)
	c.s.Partition(0, 1, false)
	w := c.write(pg0, ch, 2*core.BlockSize, []byte{1})
	c.runFor(50_000)
	c.s.Heal(0, 1)
	c.settle()
	if rp := c.reply(w); rp.Status != StatusOK {
		t.Fatalf("answered %+v", rp)
	}
	for d := range 3 {
		if got := c.diskBlock(d, ch, 2); got[0] != 1 || got[4000] != 0xaa {
			t.Fatalf("disk %d merged on % x", d+1, []byte{got[0], got[4000]})
		}
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

// Disk 2 fails to log 1.1, a punch whose edge block finds its pool full: 1.2, sent after it, is refused, so the member never holds 1.2 without 1.1.
func TestAMemberRefusesTheVersionsAfterOneItFailed(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	ch := chunkIn(pg0, 4, 0)
	var hog []core.IOBuf
	c.call(1, func(ctx *core.Context, r *Replica, out *ReplicaOutput) {
		for {
			b, err := ctx.Pool.Alloc(core.BlockSize)
			if err != nil {
				break
			}
			hog = append(hog, b)
		}
		ctx.Pool.Free(hog[len(hog)-1])
		hog = hog[:len(hog)-1]
	})
	c.nextID++
	punch := c.nextID
	u := Update{Kind: KindReplicaPunch, ID: punch, PG: c.pgFields(pg0), Chunk: ch, Off: 10, Len: 100, Version: MakeVersion(1, 1)}
	c.s.Node(c.client.Node).Call(c.client, func(ctx *core.Context, acts []core.Action) []core.Action {
		a := core.Action{Kind: core.ActNetSend, Peer: core.NodeID{Node: 1}}
		a.Msg.Len = uint16(copy(a.Msg.Body[:], AppendUpdate(nil, &u)))
		a.Msg.Buf, _ = ctx.Pool.AllocZeroed(core.BlockSize)
		c.cl.sent[punch] = a.Msg.Buf
		return append(acts, a)
	})
	c.until(punch)
	c.call(1, func(ctx *core.Context, r *Replica, out *ReplicaOutput) {
		for _, b := range hog {
			ctx.Pool.Free(b)
		}
	})
	next := c.replicaWrite(1, ch, MakeVersion(1, 2))
	c.settle()
	for _, w := range []core.RequestID{punch, next} {
		if rp := c.reply(w); rp.Status != StatusRefused {
			t.Fatalf("answered %+v", rp)
		}
	}
	if v := c.newest(1, ch); v != 0 {
		t.Fatalf("disk 2 holds %#x", v)
	}
}

// Disk 2 restarts and gets 1.1 while its store still recovers: it refuses it.
func TestARecoveringMemberRefusesWrites(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	ch := chunkIn(pg0, 4, 0)
	c.s.CrashRandom(1)
	for !c.s.Node(1).Up() && c.step() {
	}
	if c.reps[1].Store().Serving() {
		t.Fatal("the store serves as it restarts")
	}
	w := c.replicaWrite(1, ch, MakeVersion(1, 1))
	c.settle()
	if rp := c.reply(w); rp.Status != StatusRefused {
		t.Fatalf("answered %+v", rp)
	}
}

// Disk 3, cut off, acknowledges 1.2 and then refuses 1.1, as a member does that logged both and refuses 1.1 sent again after the acknowledgment was lost: 1.2 has committed and waits behind 1.1, and the primary still asks for the next interval, whose peering settles both.
func TestAFailureBelowACommittedVersionStillAsks(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	ch := chunkIn(pg0, 4, 0)
	c.s.Partition(0, 2, false)
	w1, w2 := c.write(pg0, ch, 0, []byte{1}), c.write(pg0, ch, 1, []byte{2})
	c.runFor(5_000)
	from3 := func(rp Response) {
		ev := core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{Node: 2}}
		ev.Msg.Len = uint16(copy(ev.Msg.Body[:], AppendResponse(nil, &rp)))
		c.s.Node(0).Feed(c.loops[0], ev)
	}
	from3(Response{Kind: KindAck, ID: 2, Version: MakeVersion(1, 2)})
	if !c.reps[0].vers[1].commit {
		t.Fatal("1.2 did not commit")
	}
	from3(Response{Kind: KindAck, ID: 1, Status: StatusRefused, Epoch: 1, Interval: 1})
	c.s.Heal(0, 2)
	c.settle()
	if len(c.asks) != 1 {
		t.Fatalf("asks %+v", c.asks)
	}
	c.publish(c.next(func(t *Table) { setInterval(t, pg0, 2) }), false)
	c.call(0, func(ctx *core.Context, r *Replica, out *ReplicaOutput) {
		if err := r.Settle(ctx, ch, 0, out); err != nil {
			t.Fatal(err)
		}
		r.Activate(ctx, pg0, 2, out)
	})
	c.settle()
	for _, w := range []core.RequestID{w1, w2} {
		if rp := c.reply(w); rp.Status != StatusRefused {
			t.Fatalf("answered %+v", rp)
		}
	}
}

// Disk 2's acknowledgment of 1.1's first send, which a lost connection left unanswered, arrives once 1.1 has gone out again: the primary keeps 1.1's buffer until the second send has its answer too.
func TestALateAcknowledgmentKeepsTheBufferOfASendAgain(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	ch := chunkIn(pg0, 4, 0)
	c.s.Partition(0, 1, false)
	c.write(pg0, ch, 0, []byte{1})
	for c.lost[[2]core.NodeID{c.loops[0], c.loops[1]}] == 0 && c.step() {
	}
	c.s.Heal(0, 1)
	for len(c.reps[0].vers) > 0 && c.reps[0].vers[0].out&1 == 0 && c.step() {
	}
	if len(c.reps[0].vers) == 0 {
		t.Fatal("1.1 was not sent again")
	}
	ev := core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{Node: 1}}
	ev.Msg.Len = uint16(copy(ev.Msg.Body[:], AppendResponse(nil, &Response{Kind: KindAck, ID: 1, Version: MakeVersion(1, 1)})))
	c.s.Node(0).Feed(c.loops[0], ev)
	c.settle()
}

// Disk 1's index rebuilds while 1.1 waits for cut-off disk 3. The rebuilt index maps 1.1, as a restart's would, so disk 1 refuses 1.1's client, refuses pg0's reads and asks for interval 2, to peer again as after a restart.
func TestAnIndexRebuildEndsTheIntervalsItsDiskLeads(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	ch := chunkIn(pg0, 4, 0)
	c.writeWhole(pg0, ch, 0, 1, 1)
	c.s.Partition(0, 2, false)
	w := c.write(pg0, ch, 0, []byte{9})
	c.runFor(100_000)
	c.call(0, func(ctx *core.Context, r *Replica, out *ReplicaOutput) {
		r.st.indexBroken(ctx, lsm.ErrCheck, &r.sout)
		r.st.pump(ctx, &r.sout)
		r.drain(ctx, out)
	})
	c.s.Heal(0, 2)
	c.settle()
	if rp := c.reply(w); rp.Status != StatusRefused {
		t.Fatalf("1.1 answered %+v after the rebuild", rp)
	}
	if !slices.Contains(c.asks, PGFields{Epoch: 1, PG: pg0, Interval: 1}) {
		t.Fatalf("asks %+v", c.asks)
	}
	rd := c.read(pg0, ch, 0, 1, 0)
	c.until(rd)
	if rp := c.reply(rd); rp.Status != StatusRefused {
		t.Fatalf("a read after the rebuild answered %+v", rp)
	}
}

// Disk 1 orders 1.1 while disk 3 is cut off, and interval 2 makes disk 2 the primary with disk 1 an up member. Disk 1 refuses 1.1's client and maps 1.1 as a member maps what it logs, so the new primary's 2.1 is answered once the cut heals, and disk 1 frees 1.1's buffer.
func TestADemotedPrimaryMapsItsPendingVersionsAsAMember(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	ch := chunkIn(pg0, 4, 0)
	c.s.Partition(0, 2, false)
	w1 := c.write(pg0, ch, 0, []byte{1})
	c.runFor(100_000)
	moved := c.next(func(t *Table) {
		_, e := t.entry(pg0)
		e.Members = []Member{{Disk: 2}, {Disk: 3}, {Disk: 1}}
		e.Interval = 2
	})
	c.s.Heal(0, 2)
	c.publish(moved, true)
	c.settle()
	if rp := c.reply(w1); rp.Status != StatusRefused {
		t.Fatalf("1.1 answered %+v once disk 1 lost its lead", rp)
	}
	w2 := c.write(pg0, ch, 1, []byte{2})
	c.settle()
	if rp, ok := c.cl.replies[w2]; !ok || rp.Status != StatusOK || rp.Version != MakeVersion(2, 1) {
		t.Fatalf("the new primary's write answered %+v %v", rp, ok)
	}
	if n := len(c.reps[0].vers); n != 0 {
		t.Fatalf("disk 1 holds %d versions", n)
	}
}

// 1.1 of an absent chunk goes only to disk 3, disk 2 being in maintenance, and disk 3 refuses it. No member holds 1.1, so only the primary can name the chunk to peering: Activate refuses while 1.1 is pending and Unsettled lists the chunk. Once peering drops 1.1 the PG activates, and the next write is 2.1 on every member, merged on no trace of 1.1.
func TestActivateWaitsForEveryPendingVersionToBeSettled(t *testing.T) {
	tbl := rtable(4, 2)
	_, e := tbl.entry(pg0)
	e.Members[1].Mark = MarkMaint
	c := newRCluster(t, tbl)
	ch := chunkIn(pg0, 4, 0)
	restore := c.refuse()
	w := c.write(pg0, ch, 0, []byte{1})
	c.settle()
	if rp := c.reply(w); rp.Status != StatusRefused {
		t.Fatalf("answered %+v", rp)
	}
	restore()
	c.publish(c.next(func(t *Table) { setInterval(t, pg0, 2) }), false)
	c.call(0, func(ctx *core.Context, r *Replica, out *ReplicaOutput) {
		if err := r.Activate(ctx, pg0, 2, out); !errors.Is(err, ErrUnresolved) {
			t.Fatalf("Activate with 1.1 pending: %v", err)
		}
		if got := r.Unsettled(pg0, nil); !slices.Equal(got, []core.ChunkID{ch}) {
			t.Fatalf("unsettled %v, want %v", got, ch)
		}
		if err := r.Settle(ctx, ch, 0, out); err != nil {
			t.Fatal(err)
		}
		if err := r.Activate(ctx, pg0, 2, out); err != nil {
			t.Fatalf("Activate once 1.1 was dropped: %v", err)
		}
	})
	next := c.write(pg0, ch, 1, []byte{2})
	c.settle()
	if rp := c.reply(next); rp.Status != StatusOK || rp.Version != MakeVersion(2, 1) {
		t.Fatalf("the write after activation answered %+v", rp)
	}
	for d := range 3 {
		if got := c.diskBlock(d, ch, 0); d != 1 && (got[0] != 0 || got[1] != 2) {
			t.Fatalf("disk %d maps % x", d+1, got[:2])
		}
	}
}

// A write whose chunk spent its counter after its PG's lead moved away is refused rather than dropped unanswered.
func TestASpentCounterWithNoLeadRefusesTheWrite(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	pg1 := core.PGID{Pool: testPool, Index: 1}
	c.call(0, func(ctx *core.Context, r *Replica, out *ReplicaOutput) {
		r.vers = append(r.vers, version{seq: 1, client: c.client, msgID: 99, u: Update{Kind: KindWrite, ID: 99, PG: PGFields{PG: pg1}, Chunk: chunkIn(pg1, 4, 0)}})
		r.counterSpent(ctx, len(r.vers)-1, core.NoBuf, out)
	})
	c.settle()
	if rp := c.reply(99); rp.Status != StatusRefused {
		t.Fatalf("answered %+v", rp)
	}
}
