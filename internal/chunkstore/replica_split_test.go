package chunkstore

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

// split is t with its pool's pg_count doubled, as placement splits it: PG i is a half of PG i/2,
// with its parent's members and past intervals, starting the parent's interval plus one.
func split(t Table) Table {
	t.Pools = slices.Clone(t.Pools)
	p := &t.Pools[0]
	pgs := make([]PGEntry, 2*len(p.PGs))
	for i := range pgs {
		e := p.PGs[i/2]
		pgs[i] = PGEntry{Members: e.Members, Interval: e.Interval + 1, Activated: e.Activated,
			Past: append(slices.Clone(e.Past), PastInterval{Interval: e.Interval, MinLive: p.MinLive, Members: e.Members})}
	}
	p.PGs = pgs
	t.Epoch++
	p.Split = t.Epoch
	return t
}

// collidingHalves is a cluster of disks 1 to 4 with 4 PGs, PG 1 on PG 0's members one interval above
// it, and PG 3 on disks 4, 2 and 3; and the split of its table, which also moves new PG 2, a half of
// PG 1, to disk 4 as primary. New PG 1, a half of PG 0, then starts interval 2 on disk 1, as old PG 1
// was: a message naming old PG 1 at interval 2 names new PG 1 by number and interval.
func collidingHalves(t *testing.T) (*rcluster, Table) {
	tbl := rtable(4, 2)
	tbl.Pools[0].PGs[1].Members = slices.Clone(tbl.Pools[0].PGs[0].Members)
	tbl.Pools[0].PGs[3].Members = []Member{{Disk: 4}, {Disk: 2}, {Disk: 3}}
	c := newRCluster(t, tbl)
	c.untilActive()
	next := cloneTable(c.table)
	next.Epoch = 2
	e := &next.Pools[0].PGs[1]
	e.Past, e.Interval = []PastInterval{{Interval: 1, MinLive: 2, Members: e.Members}}, 2
	c.publish(next, true)
	s := cloneTable(split(c.table))
	s.Pools[0].PGs[2].Members = []Member{{Disk: 4}, {Disk: 2}, {Disk: 3}}
	return c, s
}

var (
	pg1 = core.PGID{Pool: testPool, Index: 1}
	pg2 = core.PGID{Pool: testPool, Index: 2}
)

// A client one table behind writes a chunk of old PG 1, now of new PG 2, naming PG 1 at interval 2:
// disk 1, new PG 1's primary at that interval, must refuse it with the split's epoch, not order it
// on new PG 1's members and acknowledge a write PG 2's primary cannot read.
func TestAClientOneTableBehindIsRefusedAtAHalf(t *testing.T) {
	c, s := collidingHalves(t)
	ch := chunkIn(pg2, 8, 0)
	old := c.pgFields(pg1)
	c.publish(s, true)
	c.runFor(10_000_000)
	w := c.update(old, ch, 0, bytes.Repeat([]byte{0xAA}, core.BlockSize), 0, nil)
	c.until(w)
	if rp := c.reply(w); rp.Status != StatusRefused || rp.Epoch != s.Epoch {
		t.Fatalf("a write from before the split answered %+v", rp)
	}
}

// A client one table behind reads a chunk of new PG 2 naming old PG 1 at interval 2: disk 1 must
// refuse it rather than serve new PG 1's copy, which it does not hold.
func TestAReadOneTableBehindIsRefusedAtAHalf(t *testing.T) {
	c, s := collidingHalves(t)
	ch := chunkIn(pg2, 8, 0)
	before := cloneTable(c.table)
	c.publish(s, true)
	c.runFor(10_000_000)
	c.until(c.write(pg2, ch, 0, bytes.Repeat([]byte{0xAA}, core.BlockSize)))
	c.table = before
	id := c.read(pg1, ch, 0, core.BlockSize, c.primary(pg1))
	c.table = s
	c.until(id)
	if rp := c.reply(id); rp.Status != StatusRefused || rp.Epoch != s.Epoch {
		t.Fatalf("a read from before the split answered %+v, %x", rp, c.cl.data[id][:4])
	}
}

// Old PG 1's primary, disk 1, is cut off with a write W unsent when the split comes, and stays on
// the table before it; disk 4 activates new PG 2, W's chunk's PG, without W. After the heal, W's
// resend names old PG 1 at interval 2: disks 2 and 3, members of new PG 1 at that interval, must
// refuse it, or disk 1 acknowledges a write PG 2's primary cannot read.
func TestAStalePrimarysResendIsRefusedAtAHalf(t *testing.T) {
	c, s := collidingHalves(t)
	ch := chunkIn(pg2, 8, 0)
	p := c.primary(pg1)
	for d := range c.reps {
		if d != p {
			c.s.Partition(uint32(p), uint32(d), false)
		}
	}
	w := c.write(pg1, ch, 0, bytes.Repeat([]byte{0xAA}, core.BlockSize))
	c.runFor(2_000)
	for i := range s.Pools[0].PGs {
		for j := range s.Pools[0].PGs[i].Members {
			if m := &s.Pools[0].PGs[i].Members[j]; m.Disk == c.ids[p] {
				m.Mark = MarkDown
			}
		}
	}
	c.table = s
	for d := range c.reps {
		if d != p {
			c.call(d, func(ctx *core.Context, r *Replica, out *ReplicaOutput) { r.SetTable(ctx, s, out) })
		}
	}
	c.runFor(10_000_000)
	for d := range c.reps {
		if d != p {
			c.s.Heal(uint32(p), uint32(d))
		}
	}
	c.runFor(10_000_000)
	if rp, ok := c.cl.replies[w]; ok && rp.Status == StatusOK {
		if got := c.readNow(pg2, ch, 0, core.BlockSize); got[0] != 0xAA {
			t.Fatalf("W acknowledged, and PG 2 reads %#x", got[0])
		}
	}
}

// PG 0's primary holds a client write while it peers a new interval, cut off from its members, when
// the split comes: it refuses the write under the split's table, so the client fetches that table
// rather than send the write again by the old number.
func TestAWriteHeldAcrossASplitIsRefusedUnderTheSplitTable(t *testing.T) {
	c := newRCluster(t, rtable(2, 2))
	c.untilActive()
	p := c.primary(pg0)
	for d := range c.reps {
		if d != p {
			c.s.Partition(uint32(p), uint32(d), false)
		}
	}
	c.publish(c.next(func(t *Table) { setInterval(t, pg0, 2) }), false)
	w := c.write(pg0, chunkIn(pg0, 2, 0), 0, []byte{1})
	c.runFor(2_000)
	if i := c.reps[p].lead(pg0); i < 0 || len(c.reps[p].leads[i].held) != 1 {
		t.Fatal("the write is not held")
	}
	s := split(c.table)
	c.publish(s, false)
	c.until(w)
	if rp := c.reply(w); rp.Status != StatusRefused || rp.Epoch != s.Epoch {
		t.Fatalf("the held write answered %+v", rp)
	}
}

// A primary on the table before the split asks disk 2 to peer old PG 1 at interval 2: disk 2, a
// member of new PG 1 at that interval, must not answer with new PG 1's chunks.
func TestAPeeringAskFromBeforeTheSplitIsNotAnswered(t *testing.T) {
	c, s := collidingHalves(t)
	old := c.pgFields(pg1)
	c.publish(s, true)
	c.settle()
	answered := 0
	c.drop = func(_ int, a *core.Action) bool {
		if a.Kind == core.ActNetSend && a.Peer == c.client && Kind(a.Msg.Body[1]) == KindPeerReply {
			answered++
			return true
		}
		return false
	}
	c.s.Node(c.client.Node).Call(c.client, func(ctx *core.Context, acts []core.Action) []core.Action {
		a := core.Action{Kind: core.ActNetSend, Peer: c.loops[c.node(2)]}
		a.Msg.Len = uint16(copy(a.Msg.Body[:], AppendPeering(nil, &Peering{Kind: KindPeer, ID: 77, PG: old, Disk: 2})))
		return append(acts, a)
	})
	c.runFor(10_000_000)
	if answered != 0 {
		t.Fatalf("disk 2 answered a peering ask from before the split %d times", answered)
	}
}

// A primary on the table before the split hands disk 2 an empty missing list for old PG 1 at
// interval 2: it passes the store's range check, so disk 2, a member of new PG 1 at that interval,
// must refuse it with the split's epoch rather than record a decision of new PG 1.
func TestAMissingListFromBeforeTheSplitIsRefused(t *testing.T) {
	c, s := collidingHalves(t)
	old := c.pgFields(pg1)
	c.publish(s, true)
	c.settle()
	var acks []Peering
	c.drop = func(n int, a *core.Action) bool {
		if a.Kind != core.ActNetSend || a.Peer != c.client || Kind(a.Msg.Body[1]) != KindPersistAck {
			return false
		}
		var p Peering
		frame, data := Frame(c.s.Node(uint32(n)).Pool(), &a.Msg)
		if err := DecodePeering(frame, data, &p); err != nil {
			t.Fatal(err)
		}
		acks = append(acks, p)
		return true
	}
	c.s.Node(c.client.Node).Call(c.client, func(ctx *core.Context, acts []core.Action) []core.Action {
		a := core.Action{Kind: core.ActNetSend, Peer: c.loops[c.node(2)]}
		a.Msg.Len = uint16(copy(a.Msg.Body[:], AppendPeering(nil, &Peering{Kind: KindPersist, ID: 77, PG: old, Disk: 2})))
		return append(acts, a)
	})
	c.runFor(10_000_000)
	if len(acks) != 1 || acks[0].Decided != 0 || acks[0].PG.Epoch != s.Epoch {
		t.Fatalf("disk 2 answered a missing list from before the split with %+v", acks)
	}
}

// ahead names new PG 1 at interval 2 in the split table s, which old PG 1 holds by number and interval.
func ahead(s Table) PGFields { return PGFields{Epoch: s.Epoch, PG: pg1, Interval: 2} }

// Disk 2, still on the table before the split, takes a member write naming new PG 1 at interval 2
// for a chunk of new PG 1, which its own table puts in old PG 0: it must wait for the split table
// rather than log the write under old PG 1's checks, then log it once it has that table.
func TestAMemberBehindASplitWaitsForItWithAWrite(t *testing.T) {
	c, s := collidingHalves(t)
	ch, d, v := chunkIn(pg1, 8, 0), c.node(2), uint64(2)<<32|1
	id := c.replicaWrite(d, ch, v, func(u *Update) { u.PG = ahead(s) })
	c.runFor(100_000)
	if _, ok := c.cl.replies[id]; ok || c.newest(d, ch) != 0 || c.fetch[d] != s.Epoch {
		t.Fatalf("before the split table: answered %v, newest %#x, fetched %d", ok, c.newest(d, ch), c.fetch[d])
	}
	c.call(d, func(ctx *core.Context, r *Replica, out *ReplicaOutput) { r.SetTable(ctx, s, out) })
	c.until(id)
	if rp := c.reply(id); rp.Status != StatusOK || c.newest(d, ch) != v {
		t.Fatalf("under the split table: answered %+v, newest %#x", rp, c.newest(d, ch))
	}
}

// Disk 1, old PG 1's primary at interval 2 and still on the table before the split, takes a read
// naming new PG 1 at interval 2 for a chunk its own table puts in old PG 0: it must wait for the
// split table rather than serve it under old PG 1's lead, then answer it under that table.
func TestAPrimaryBehindASplitWaitsForItWithARead(t *testing.T) {
	c, s := collidingHalves(t)
	ch, d := chunkIn(pg1, 8, 0), c.node(1)
	c.until(c.write(pg0, ch, 0, bytes.Repeat([]byte{0xAA}, core.BlockSize)))
	c.runFor(10_000_000)
	before := c.table
	c.table = s
	id := c.read(pg1, ch, 0, core.BlockSize, d)
	c.table = before
	c.runFor(100_000)
	if _, ok := c.cl.replies[id]; ok || c.fetch[d] != s.Epoch {
		t.Fatalf("before the split table: answered %v, fetched %d", ok, c.fetch[d])
	}
	c.publish(s, true)
	c.until(id)
	if rp := c.reply(id); rp.Status != StatusRefused || rp.Epoch != s.Epoch {
		t.Fatalf("under the split table, before new PG 1 serves: answered %+v", rp)
	}
	c.runFor(10_000_000)
	if got := c.readNow(pg1, ch, 0, core.BlockSize); got[0] != 0xAA {
		t.Fatalf("new PG 1 reads %#x", got[0])
	}
}

// Disk 2, still on the table before the split, takes a peering ask naming new PG 1 at interval 2:
// it must wait for the split table rather than answer with old PG 1's chunks, then answer with new
// PG 1's.
func TestAMemberBehindASplitWaitsForItWithAPeeringAsk(t *testing.T) {
	c, s := collidingHalves(t)
	mine, other, d := chunkIn(pg1, 8, 0), chunkIn(pg2, 8, 0), c.node(2)
	c.until(c.write(pg0, mine, 0, []byte{1}))
	c.until(c.write(pg1, other, 0, []byte{2}))
	var replies [][]ChunkPeer
	c.drop = func(n int, a *core.Action) bool {
		if a.Kind != core.ActNetSend || a.Peer != c.client || Kind(a.Msg.Body[1]) != KindPeerReply {
			return false
		}
		pool := c.s.Node(uint32(n)).Pool()
		var p Peering
		frame, data := Frame(pool, &a.Msg)
		if err := DecodePeering(frame, data, &p); err != nil {
			t.Fatal(err)
		}
		replies = append(replies, slices.Clone(p.Chunks))
		if a.Msg.Buf != core.NoBuf {
			pool.Free(a.Msg.Buf)
		}
		return true
	}
	c.s.Node(c.client.Node).Call(c.client, func(ctx *core.Context, acts []core.Action) []core.Action {
		a := core.Action{Kind: core.ActNetSend, Peer: c.loops[d]}
		a.Msg.Len = uint16(copy(a.Msg.Body[:], AppendPeering(nil, &Peering{Kind: KindPeer, ID: 77, PG: ahead(s), Disk: 2})))
		return append(acts, a)
	})
	c.runFor(100_000)
	if len(replies) != 0 || c.fetch[d] != s.Epoch {
		t.Fatalf("before the split table: answered %v, fetched %d", replies, c.fetch[d])
	}
	c.call(d, func(ctx *core.Context, r *Replica, out *ReplicaOutput) { r.SetTable(ctx, s, out) })
	c.runFor(10_000_000)
	has := func(ch core.ChunkID) bool {
		return len(replies) == 1 && slices.ContainsFunc(replies[0], func(p ChunkPeer) bool { return p.Chunk == ch })
	}
	if !has(mine) || has(other) {
		t.Fatalf("under the split table: answered %v", replies)
	}
}

// Disk 2, still on the table of 4 PGs, takes a peering ask and a missing list naming new PG 5,
// tagged with the split table: it must wait for that table rather than ignore the ask or refuse the
// list for a PG its table lacks, then answer each under it.
func TestAMemberBehindASplitWaitsForItWithAPGPastItsCount(t *testing.T) {
	for _, kind := range []Kind{KindPeer, KindPersist} {
		c, s := collidingHalves(t)
		d, pg5 := c.node(2), core.PGID{Pool: testPool, Index: 5}
		var replies []Peering
		c.drop = func(n int, a *core.Action) bool {
			if a.Kind != core.ActNetSend || a.Peer != c.client || Kind(a.Msg.Body[1]) != kind+1 {
				return false
			}
			pool := c.s.Node(uint32(n)).Pool()
			var p Peering
			frame, data := Frame(pool, &a.Msg)
			if err := DecodePeering(frame, data, &p); err != nil {
				t.Fatal(err)
			}
			replies = append(replies, p)
			if a.Msg.Buf != core.NoBuf {
				pool.Free(a.Msg.Buf)
			}
			return true
		}
		f := PGFields{Epoch: s.Epoch, PG: pg5, Interval: s.Entry(pg5).Interval}
		c.s.Node(c.client.Node).Call(c.client, func(ctx *core.Context, acts []core.Action) []core.Action {
			a := core.Action{Kind: core.ActNetSend, Peer: c.loops[d]}
			a.Msg.Len = uint16(copy(a.Msg.Body[:], AppendPeering(nil, &Peering{Kind: kind, ID: 77, PG: f, Disk: 2})))
			return append(acts, a)
		})
		c.runFor(100_000)
		if len(replies) != 0 || c.fetch[d] != s.Epoch {
			t.Fatalf("kind %d before the split table: answered %+v, fetched %d", kind, replies, c.fetch[d])
		}
		c.call(d, func(ctx *core.Context, r *Replica, out *ReplicaOutput) { r.SetTable(ctx, s, out) })
		c.runFor(10_000_000)
		if len(replies) != 1 || replies[0].PG.PG != pg5 || kind == KindPersist && replies[0].Decided != f.Interval {
			t.Fatalf("kind %d under the split table: answered %+v", kind, replies)
		}
	}
}

// A primary retries a missing list every RetryAfter to disk 2, still on the table before the split:
// each retry replaces the parked copy, so the member holds one parked message and one pool buffer
// however often it is sent, and persists the list once it has the split table.
func TestAMemberBehindASplitParksOneCopyOfARetriedMissingList(t *testing.T) {
	c, s := collidingHalves(t)
	d := c.node(2)
	var missing []ChunkVersion
	for i := range uint32(12) {
		missing = append(missing, ChunkVersion{Chunk: chunkIn(pg1, 8, i), Version: 1<<32 | 1})
	}
	frame := AppendPeering(nil, &Peering{Kind: KindPersist, ID: 77, PG: ahead(s), Disk: 2, Missing: missing})
	if len(frame) <= core.MaxInlineMsg {
		t.Fatalf("a %d-byte missing list travels inline", len(frame))
	}
	acks := 0
	c.drop = func(n int, a *core.Action) bool {
		if a.Kind != core.ActNetSend || a.Peer != c.client || Kind(a.Msg.Body[1]) != KindPersistAck {
			return false
		}
		acks++
		if a.Msg.Buf != core.NoBuf {
			c.s.Node(uint32(n)).Pool().Free(a.Msg.Buf)
		}
		return true
	}
	pool, live := c.s.Node(uint32(d)).Pool(), 0
	for i := range 5 {
		c.s.Node(c.client.Node).Call(c.client, func(ctx *core.Context, acts []core.Action) []core.Action {
			a := core.Action{Kind: core.ActNetSend, Peer: c.loops[d]}
			if err := Attach(ctx.Pool, &a.Msg, frame, core.NoBuf); err != nil {
				t.Fatal(err)
			}
			return append(acts, a)
		})
		c.runFor(RetryAfter)
		if i == 0 {
			live = pool.Live()
		}
		if r := c.reps[d]; len(r.parked) != 1 || pool.Live() != live {
			t.Fatalf("after %d sends: %d parked, %d buffers live, %d after the first", i+1, len(r.parked), pool.Live(), live)
		}
	}
	c.call(d, func(ctx *core.Context, r *Replica, out *ReplicaOutput) { r.SetTable(ctx, s, out) })
	c.runFor(10_000_000)
	if acks != 1 || len(c.reps[d].parked) != 0 {
		t.Fatalf("under the split table: %d acks, %d parked", acks, len(c.reps[d].parked))
	}
}

// A disk leading PGs 0 and 1, PG 1 one interval above PG 0, adopts a split: new PG 1, a half of PG
// 0, starts PG 1's old interval, and the disk must peer it rather than keep PG 1's active lead.
func TestAHalfDoesNotKeepALeadOfItsNumberFromBeforeTheSplit(t *testing.T) {
	tbl := rtable(2, 2)
	tbl.Pools[0].PGs[1].Members = slices.Clone(tbl.Pools[0].PGs[0].Members)
	c := newRCluster(t, tbl)
	c.untilActive()
	next := c.table
	next.Epoch, next.Pools = 2, slices.Clone(next.Pools)
	next.Pools[0].PGs = slices.Clone(next.Pools[0].PGs)
	e := &next.Pools[0].PGs[1]
	e.Past, e.Interval = []PastInterval{{Interval: 1, MinLive: 2, Members: e.Members}}, 2
	c.publish(next, true)
	s := split(c.table)
	c.publish(s, false)
	pg1 := core.PGID{Pool: testPool, Index: 1}
	r := c.reps[c.primary(pg1)]
	if i := r.lead(pg1); i < 0 || r.leads[i].active || r.leads[i].interval != 2 {
		t.Fatalf("disk %d leads %v after the split as %+v", r.cfg.Disk, pg1, r.leads)
	}
	c.untilActive()
}

// Disk 1 decides old PG 1 at interval 3 with a chunk of new PG 2 in its image; the split starts new
// PG 1, a half of PG 0, at interval 2, below what that image was checked up to. The disk must drop
// the image rather than judge new PG 1's copies against old PG 1's chunks.
func TestAHalfDoesNotKeepAnImageOfItsNumberFromBeforeTheSplit(t *testing.T) {
	c, _ := collidingHalves(t)
	other := chunkIn(pg2, 8, 0)
	c.until(c.write(pg1, other, 0, []byte{2}))
	next := cloneTable(c.table)
	next.Epoch++
	e := &next.Pools[0].PGs[1]
	e.Past, e.Interval = append(e.Past, PastInterval{Interval: e.Interval, MinLive: 2, Members: e.Members}), e.Interval+1
	c.publish(next, true)
	r := c.reps[c.primary(pg1)]
	if _, ok := r.imaged(pg1, other); !ok {
		t.Fatalf("disk %d's image of old PG 1 does not hold %v", r.cfg.Disk, other)
	}
	c.publish(cloneTable(split(c.table)), false)
	for _, r := range c.reps {
		for _, m := range r.images {
			for _, cv := range m.chunks {
				if core.PGIndexOf(cv.Chunk.Hash(), 8) != m.pg.Index {
					t.Fatalf("disk %d's image of new PG %d holds %v, a chunk of PG %d", r.cfg.Disk, m.pg.Index, cv.Chunk, core.PGIndexOf(cv.Chunk.Hash(), 8))
				}
			}
		}
	}
}

const invSplitActivations = "every activation a table records happened, one reported before a split naming both halves of its PG"

// Old PG 1 at interval 2 is active with chunk x unfound, and the split gives number 1 to a half of
// PG 0 at interval 2. The leader's watcher must drop old PG 1's lead, and a node still on the table
// before the split reporting it again, so a give-up of new PG 1 does not drop x, a chunk of new PG 3.
func TestAGiveUpAfterASplitDropsNoChunkOfAnotherPG(t *testing.T) {
	ms := []Member{{Disk: 1}, {Disk: 2}, {Disk: 3}}
	old := Table{Epoch: 2, Pools: []PoolTable{{ID: testPool, Replicas: 3, MinLive: 2, PGs: []PGEntry{
		{Interval: 1, Activated: 1, Members: ms}, {Interval: 2, Activated: 2, Members: ms}}}}}
	var w Watcher
	var cond Conditions
	w.Watch(0, &old.Pools[0], &cond)
	x := chunkIn(pg1, 2, 0)
	stale := []LeadState{{PG: pg1, PGCount: 2, Interval: 2, Active: true, Unfound: 1, Chunks: []core.ChunkID{x}}}
	w.Hear(&old, stale)
	s := split(old)
	for _, step := range []string{"the split", "a stale report after it"} {
		w.Watch(0, &s.Pools[0], &cond)
		w.Forget(&s)
		g, err := CheckGiveUp(pg1, s.Entry(pg1), func(uint32) bool { return false }, w.Lead(&s, pg1))
		if err == nil && slices.Contains(g.Chunks, x) {
			t.Fatalf("after %s, a give-up of new PG 1 drops chunk x of new PG %d", step, core.PGIndexOf(x.Hash(), 4))
		}
		if len(cond.Blocked) != 0 {
			t.Fatalf("after %s, old PG 1's lead blocks %+v", step, cond.Blocked)
		}
		w.Hear(&s, stale)
	}
	w.Hear(&s, []LeadState{{PG: pg1, PGCount: 4, Interval: 2, Active: true}})
	w.Hear(&s, stale)
	if l := w.Lead(&s, pg1); l == nil || l.Unfound != 0 {
		t.Fatalf("a stale report replaced new PG 1's: %+v", l)
	}
}

// watchSplits registers with c's invariants that every activation its table records is one its
// primaries reported, one sent under half the pool's pg_count, before a split, naming both halves.
func watchSplits(c *rcluster) {
	c.inv.Register(invSplitActivations, func() error {
		for _, p := range c.table.Pools {
			n := uint32(len(p.PGs))
			for i, e := range p.PGs {
				if e.Activated != 0 && !slices.ContainsFunc(c.activations, func(a PGReport) bool {
					return a.PG.Pool == p.ID && a.Interval == e.Activated &&
						(a.PGCount == n && a.PG.Index == uint32(i) || 2*a.PGCount == n && a.PG.Index == uint32(i)/2)
				}) {
					return fmt.Errorf("PG %d records interval %d activated, which no primary activated", i, e.Activated)
				}
			}
		}
		return nil
	})
}

// neg-report-across-split: disk 1 leads PGs 0 and 1, and PG 1's interval 2 activates; the pool
// splits while disk 1 is cut off from the other members, so new PG 1, a half of PG 0 starting
// interval 2, cannot activate. The leader then folds PG 1's report from before the split: under the
// rule it drops it, and with the bug it records new PG 1's interval 2 activated, which never came,
// and its next peering would start from a decision no member persisted.
func TestPlantedReportAcrossSplitRecordsAnActivationThatNeverCame(t *testing.T) {
	for _, planted := range []bool{false, true} {
		tbl := rtable(2, 2)
		tbl.Pools[0].PGs[1].Members = slices.Clone(tbl.Pools[0].PGs[0].Members)
		c := newRCluster(t, tbl)
		watchSplits(c)
		c.planted = planted
		c.untilActive()
		next := c.table
		next.Epoch, next.Pools = 2, slices.Clone(next.Pools)
		next.Pools[0].PGs = slices.Clone(next.Pools[0].PGs)
		e := &next.Pools[0].PGs[1]
		e.Past, e.Interval = []PastInterval{{Interval: 1, MinLive: 2, Members: e.Members}}, 2
		c.publish(next, true)
		p := uint32(c.node(1))
		for _, d := range []uint32{2, 3} {
			c.s.Partition(p, uint32(c.node(d)), false)
		}
		c.publish(split(c.table), false)
		c.runFor(100_000)
		a := &authority{pool: Pool{Replicas: 3, MinLive: 2, FailureDomain: DomainNode, MaxPerDomain: 1, Locality: LocalitySpread, Overcommit: true, WriteLimit: 90, PGCount: 4},
			plant: placementPlants{reportAcrossSplit: planted}}
		for n := range c.ids {
			if _, err := a.rec.Report(uint32(n), 0, c.logs[n], ""); err != nil {
				t.Fatal(err)
			}
		}
		next, _, _ = Next(&c.table, a.inputs(c))
		c.publish(next, false)
		c.step()
		var v *sim.Violation
		if planted && (!errors.As(c.found, &v) || v.Name != invSplitActivations) {
			t.Fatalf("the planted bug found %v", c.found)
		}
	}
}

// P7: a pool's pg_count doubles from 64 to 128 while writes to chunks of PG 37 are in flight. PG 37
// sits on PG 18's members one interval above it, so new PG 37, a half of PG 18, starts PG 37's
// interval with its primary, and the split table moves PGs 74 and 75, PG 37's halves, to disk 4 as
// primary. A write the split catches is refused and sent again under the new table; PGs 74 and 75
// start one interval above 37, every chunk's acknowledged versions keep growing, and each reads
// back through its half's primary as its last acknowledged write.
func TestP7SplitsAPoolWithWritesInFlight(t *testing.T) {
	seeds.Run(t, 0, 8, func(seed uint64) {
		tbl := rtable(64, 2)
		tbl.Pools[0].PGs[37].Members = slices.Clone(tbl.Pools[0].PGs[18].Members)
		tbl.Pools[0].PGs[63].Members = []Member{{Disk: 4}, {Disk: 2}, {Disk: 3}}
		c := newRClusterOn(t, tbl, sim.New(seed, sim.DefaultSettings()))
		watchSplits(c)
		c.untilActive()
		pg := core.PGID{Pool: testPool, Index: 37}
		c.publish(c.next(func(t *Table) { setInterval(t, pg, 2) }), true)
		var chunks [4]core.ChunkID
		for i := range chunks {
			chunks[i] = chunkIn(pg, 64, uint32(i))
		}
		rng := core.NewRand(seed)
		at := int(rng.Below(6))
		acked, last := map[core.ChunkID]uint64{}, map[core.ChunkID]byte{}
		for round := range 12 {
			ids := map[core.RequestID]core.ChunkID{}
			datas := map[core.RequestID]byte{}
			for i, ch := range chunks {
				b := byte(round*len(chunks) + i + 1)
				id := c.write(core.PGID{Pool: testPool, Index: core.PGIndexOf(ch.Hash(), uint32(len(c.table.Pools[0].PGs)))}, ch, 0, bytes.Repeat([]byte{b}, core.BlockSize))
				ids[id], datas[id] = ch, b
			}
			if round == at {
				_, parent := c.table.entry(pg)
				c.runFor(core.Tick(rng.Below(2_000)))
				s := cloneTable(split(c.table))
				s.Pools[0].PGs[74].Members = []Member{{Disk: 4}, {Disk: 2}, {Disk: 3}}
				s.Pools[0].PGs[75].Members = []Member{{Disk: 4}, {Disk: 3}, {Disk: 2}}
				c.publish(s, false)
				for _, half := range []uint32{74, 75} {
					if _, e := c.table.entry(core.PGID{Pool: testPool, Index: half}); e.Interval != parent.Interval+1 {
						t.Fatalf("seed %d: PG %d starts interval %d after PG 37's %d", seed, half, e.Interval, parent.Interval)
					}
				}
				if _, e := c.table.entry(pg); e.Interval != parent.Interval {
					t.Fatalf("seed %d: new PG 37 starts interval %d, not old PG 37's %d", seed, e.Interval, parent.Interval)
				}
			}
			for id, ch := range ids {
				c.until(id)
				rp := c.reply(id)
				if rp.Status != StatusOK {
					continue
				}
				if rp.Version <= acked[ch] {
					t.Fatalf("seed %d: %v acknowledged %#x after %#x", seed, ch, rp.Version, acked[ch])
				}
				acked[ch], last[ch] = rp.Version, datas[id]
			}
			if round == at {
				c.untilActive()
			}
		}
		c.runFor(10_000_000)
		for _, ch := range chunks {
			if last[ch] == 0 || acked[ch]>>32 < 3 {
				t.Fatalf("seed %d: %v acknowledged nothing after the split: %#x", seed, ch, acked[ch])
			}
			h := core.PGID{Pool: testPool, Index: core.PGIndexOf(ch.Hash(), 128)}
			if got := c.readNow(h, ch, 0, core.BlockSize); !bytes.Equal(got, bytes.Repeat([]byte{last[ch]}, core.BlockSize)) {
				t.Fatalf("seed %d: %v reads %#x, last acknowledged %#x", seed, ch, got[0], last[ch])
			}
		}
	})
}
