package chunkstore

import (
	"bytes"
	"maps"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/lsm"
	"github.com/nvcnvn/rock-solid/internal/sim/clock"
	"github.com/nvcnvn/rock-solid/internal/trace"
)

// recSend is a recovery write a primary sent a member: its update, and when it carried a buffer,
// the buffer's map and each block's first byte.
type recSend struct {
	from, to int
	u        Update
	carries  bool
	m        blockMap
	bytes    map[uint16]byte
}

// watchRecoveries records every recovery write the replicas send, as they send it.
func (c *rcluster) watchRecoveries() *[]recSend {
	var sends []recSend
	prev := c.onPass
	c.onPass = func(d int, acts []core.Action) {
		if prev != nil {
			prev(d, acts)
		}
		for i := range acts {
			a := &acts[i]
			if a.Kind != core.ActNetSend || a.Msg.Len < headSize || Kind(a.Msg.Body[1]) != KindRecover {
				continue
			}
			var buf []byte
			if a.Msg.Buf != core.NoBuf {
				buf = c.s.Node(uint32(d)).Pool().Bytes(a.Msg.Buf)
			}
			rs := recSend{from: d, to: int(a.Peer.Node), carries: buf != nil, bytes: map[uint16]byte{}}
			if DecodeUpdate(a.Msg.Inline(), len(buf), &rs.u) != nil {
				c.t.Fatalf("disk %d sent a recovery write that does not decode", d+1)
			}
			if buf != nil {
				if decodeBlockMap(buf, &rs.m) != nil {
					c.t.Fatalf("disk %d sent a recovery buffer that does not decode", d+1)
				}
				k := 1
				rs.m.data.runs(func(first, n uint16) {
					for b := first; b < first+n; b++ {
						rs.bytes[b] = buf[k*core.BlockSize]
						k++
					}
				})
			}
			sends = append(sends, rs)
		}
	}
	return &sends
}

func (c *rcluster) listed(d int, ch core.ChunkID) Chunk {
	x, _ := c.reps[d].Store().Index().Chunk(testPool, ch)
	return x
}

// blockIs checks that block b of ch on disk d is filled with v.
func (c *rcluster) blockIs(d int, ch core.ChunkID, b uint16, v byte) {
	c.t.Helper()
	if got := c.diskBlock(d, ch, b); !bytes.Equal(got, bytes.Repeat([]byte{v}, core.BlockSize)) {
		c.t.Fatalf("disk %d holds block %d of %v as %#x, want %#x", c.ids[d], b, ch, got[0], v)
	}
}

// Disk 3 misses a write to chunk a while down; marked up again, it lists a alone, and recovery
// copies a's blocks to it alone, at the version they were written at, never touching chunk b, which
// it missed nothing of, nor disk 2. The primary then reports the interval recovered, and again on each retry
// until a table records it.
func TestAReturningMemberGetsOnlyTheChunksItMissed(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	sends := c.watchRecoveries()
	a, b := chunkIn(pg0, 4, 0), chunkIn(pg0, 4, 1)
	c.writeWhole(pg0, b, 0, 1, 7)
	c.publish(c.next(func(t *Table) { mark(t, pg0, 3, MarkDown) }), true)
	c.writeWhole(pg0, a, 0, 2, 5)
	c.reps[0].plant.noRecovery = true
	c.publish(c.next(func(t *Table) { mark(t, pg0, 3, MarkUp) }), true)
	c.settle()
	want := PGFields{Epoch: c.table.Epoch, PG: pg0, Interval: 3}
	count := func() int {
		return len(slices.DeleteFunc(slices.Clone(c.recovered), func(r PGFields) bool { return r != want }))
	}
	if x := c.listed(2, a); x.ListedIn != 3 || len(*sends) != 0 || count() != 0 {
		t.Fatalf("before recovery disk 3 holds %+v, %d recovery writes, recovered %+v", x, len(*sends), c.recovered)
	}
	c.reps[0].plant.noRecovery = false
	c.runFor(RetryAfter / 2)
	c.settle()
	if len(*sends) != 1 {
		t.Fatalf("%d recovery writes", len(*sends))
	}
	if s := (*sends)[0]; s.u.Chunk != a || s.to != 2 || s.u.Version != MakeVersion(2, 1) || !s.carries || len(s.bytes) != 2 || s.bytes[0] != 5 || s.bytes[1] != 5 || s.m.holes.count() != core.BlocksPerChunk-2 {
		t.Fatalf("recovery wrote %+v", s)
	}
	for d := range 3 {
		if x := c.listed(d, a); x.ListedIn != 0 || x.Whole != (*sends)[0].u.Version {
			t.Fatalf("disk %d holds a as %+v", d+1, x)
		}
	}
	c.blockIs(2, a, 1, 5)
	if x := c.listed(2, b); x.Version != MakeVersion(1, 1) {
		t.Fatalf("disk 3 holds b as %+v", x)
	}
	if count() == 0 {
		t.Fatalf("recovered %+v", c.recovered)
	}
	before := count()
	c.runFor(2*RetryAfter + 100_000)
	if count() < before+2 {
		t.Fatalf("reported %d times and %d since", before, count()-before)
	}
	c.publish(c.next(func(*Table) {}), false)
	c.runFor(RetryAfter)
	before = count()
	c.runFor(2 * RetryAfter)
	if count() != before {
		t.Fatalf("reported %d times after a table recorded it", count()-before)
	}
}

// 1.2 lands on disks 1 and 2 while disk 3 is cut off, and interval 2's peering drops it: recovery
// supersedes it on both, the primary included, so a restart's replay of disk 1's log ends at the
// recovered block rather than bringing 1.2 back (chunk epochs spike R3b). Without recovery the
// replay maps 1.2 again (R3).
func TestRecoverySupersedesADroppedVersionOnEveryMemberThatLoggedIt(t *testing.T) {
	for _, recover := range []bool{true, false} {
		c := newRCluster(t, rtable(4, 2))
		c.reps[0].plant.noRecovery = !recover
		ch := chunkIn(pg0, 4, 0)
		c.writeWhole(pg0, ch, 0, 2, 1)
		c.s.Partition(0, 2, false)
		w := c.write(pg0, ch, core.BlockSize, bytes.Repeat([]byte{9}, core.BlockSize))
		c.runFor(100_000)
		c.publish(c.next(func(t *Table) { setInterval(t, pg0, 2) }), false)
		c.s.Heal(0, 2)
		c.untilActive()
		c.settle()
		if rp := c.reply(w); rp.Status != StatusRefused {
			t.Fatalf("1.2 answered %+v", rp)
		}
		for d := range 2 {
			if x := c.listed(d, ch); recover != (x.ListedIn == 0) {
				t.Fatalf("recovery %v: disk %d holds %+v", recover, d+1, x)
			}
		}
		if !recover {
			c.s.CrashRandom(0)
			c.s.Restart(0)
			c.settle()
			c.blockIs(0, ch, 1, 9)
			continue
		}
		for d := range 3 {
			c.blockIs(d, ch, 1, 1)
		}
		c.s.CrashRandom(0)
		c.s.Restart(0)
		c.settle()
		c.blockIs(0, ch, 1, 1)
	}
}

// Kept 1.1 is on disk 2 only; disk 3, the new primary, takes a whole-block write to block 0, then
// recovers the chunk from disk 2: the recovery write leaves block 0 out, carrying block 1 alone, so
// both lacking members end with the new write in block 0. The blocks are chosen when the recovery
// write is ordered: with the whole-block write ordered between the read of disk 2 and the write, it
// still leaves block 0 out, while the planted variant that chooses them at the read copies 1.1's
// block 0 over the acknowledged write (the epoch change model's neg-recovery-covers-at-read).
func TestRecoveryLeavesOutTheBlocksTheIntervalsWritesCover(t *testing.T) {
	for _, tc := range []struct {
		name       string
		atRead     bool
		betweenOps bool
	}{{"before the read", false, false}, {"between read and write", false, true}, {"planted, chosen at the read", true, true}} {
		tbl := rtable(4, 1)
		_, e := tbl.entry(pg0)
		e.Members = []Member{{Disk: 2}, {Disk: 1, Mark: MarkDown}, {Disk: 3, Mark: MarkDown}}
		c := newRCluster(t, tbl)
		ch := chunkIn(pg0, 4, 0)
		c.writeWhole(pg0, ch, 0, 2, 1)
		sends := c.watchRecoveries()
		c.reps[2].plant.noRecovery = !tc.betweenOps
		c.reps[2].plant.coverAtRead = tc.atRead
		var w core.RequestID
		if tc.betweenOps {
			prev := c.onPass
			c.onPass = func(d int, acts []core.Action) {
				prev(d, acts)
				for i := range acts {
					if a := &acts[i]; d == 2 && w == 0 && a.Kind == core.ActNetSend && a.Msg.Len >= headSize && Kind(a.Msg.Body[1]) == KindRecoverRead {
						w = c.write(pg0, ch, 0, bytes.Repeat([]byte{8}, core.BlockSize))
					}
				}
			}
		}
		c.publish(c.next(func(t *Table) {
			setInterval(t, pg0, 2)
			_, e := t.entry(pg0)
			e.Members = []Member{{Disk: 3}, {Disk: 1}, {Disk: 2}}
		}), true)
		if !tc.betweenOps {
			w = c.write(pg0, ch, 0, bytes.Repeat([]byte{8}, core.BlockSize))
			c.until(w)
			c.reps[2].plant.noRecovery = false
		}
		c.runFor(RetryAfter / 2)
		c.settle()
		if rp := c.reply(w); rp.Status != StatusOK {
			t.Fatalf("%s: the write answered %+v", tc.name, rp)
		}
		var toDisk1 *recSend
		for i := range *sends {
			if s := &(*sends)[i]; s.to == 0 {
				toDisk1 = s
			}
		}
		if toDisk1 == nil || toDisk1.u.Version != c.reply(w).Version {
			t.Fatalf("%s: recovery writes %+v, the write before them %#x", tc.name, *sends, c.reply(w).Version)
		}
		if tc.atRead {
			if c.diskBlock(0, ch, 0)[0] != 1 {
				t.Fatalf("%s: the planted variant kept the write", tc.name)
			}
			continue
		}
		if toDisk1.m.data.has(0) || toDisk1.m.holes.has(0) || len(toDisk1.bytes) != 1 || toDisk1.bytes[1] != 1 {
			t.Fatalf("%s: disk 1 got blocks %v", tc.name, toDisk1.bytes)
		}
		for _, d := range []int{0, 2} {
			c.blockIs(d, ch, 0, 8)
			c.blockIs(d, ch, 1, 1)
			if x := c.listed(d, ch); x.ListedIn != 0 {
				t.Fatalf("%s: disk %d holds %+v", tc.name, d+1, x)
			}
		}
	}
}

// Recovery held off in interval 3 is cut short by interval 4: disk 3's entry survives, raised to
// interval 4's decision, and interval 4's recovery brings it the kept version.
func TestRecoveryCutShortByTheNextIntervalLeavesTheListsInPlace(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	ch := chunkIn(pg0, 4, 0)
	c.writeWhole(pg0, ch, 0, 1, 1)
	c.publish(c.next(func(t *Table) { mark(t, pg0, 3, MarkDown) }), true)
	c.writeWhole(pg0, ch, 0, 1, 2)
	c.reps[0].plant.noRecovery = true
	c.publish(c.next(func(t *Table) { mark(t, pg0, 3, MarkUp) }), true)
	if x := c.listed(2, ch); x.ListedIn != 3 {
		t.Fatalf("disk 3 holds %+v", x)
	}
	c.publish(c.next(func(t *Table) { setInterval(t, pg0, 4) }), true)
	if x := c.listed(2, ch); x.ListedIn != 4 || x.Listed != MakeVersion(2, 1) {
		t.Fatalf("disk 3 holds %+v after interval 4's peering", x)
	}
	c.reps[0].plant.noRecovery = false
	c.runFor(RetryAfter / 2)
	c.settle()
	if x := c.listed(2, ch); x.ListedIn != 0 {
		t.Fatalf("disk 3 holds %+v after recovery", x)
	}
	c.blockIs(2, ch, 0, 2)
}

// Disk 2 holds dropped 1.2, which wrote block 1, and lists 1.1; 2.1, writing block 2, lands on it
// before recovery reaches it. The next peering keeps 2.1, whose newest copy disk 2 holds, and raises
// disk 2's entry to 2.1 rather than clearing it; recovery then brings disk 2 block 1 of 1.1's
// history.
func TestOnlyRecoveryRemovesAMissingListEntry(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	ch := chunkIn(pg0, 4, 0)
	c.writeWhole(pg0, ch, 0, 3, 1)
	c.s.Partition(0, 2, false)
	c.write(pg0, ch, core.BlockSize, bytes.Repeat([]byte{9}, core.BlockSize))
	c.runFor(100_000)
	c.reps[0].plant.noRecovery = true
	c.publish(c.next(func(t *Table) { setInterval(t, pg0, 2) }), false)
	c.s.Heal(0, 2)
	c.untilActive()
	c.writeWhole(pg0, ch, 2, 1, 4)
	if x := c.listed(1, ch); x.ListedIn != 2 || x.Listed != MakeVersion(1, 1) || x.Whole != MakeVersion(2, 1) {
		t.Fatalf("disk 2 holds %+v", x)
	}
	c.publish(c.next(func(t *Table) { setInterval(t, pg0, 3) }), true)
	if x := c.listed(1, ch); x.ListedIn != 3 || x.Listed != MakeVersion(2, 1) {
		t.Fatalf("disk 2 holds %+v after interval 3's peering", x)
	}
	c.reps[0].plant.noRecovery = false
	c.runFor(RetryAfter / 2)
	c.settle()
	for d := range 3 {
		if x := c.listed(d, ch); x.ListedIn != 0 {
			t.Fatalf("disk %d holds %+v", d+1, x)
		}
		c.blockIs(d, ch, 1, 1)
		c.blockIs(d, ch, 2, 4)
	}
}

// Disk 1, down in interval 2, misses 2.1 and lists the chunk as it leads interval 3, where disk 2
// is down and disk 3 is its only source. Node 3's disk is then formatted again and comes back as
// disk 4 while the table still lists disk 3 up there; disk 4 answers no recovery read for disk 3,
// so disk 1 keeps the chunk listed rather than taking disk 4's empty image over 2.1 (the
// primary-change run's seed 34 found it).
func TestADiskFormattedAgainAnswersNoRecoveryReadForItsOldID(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	ch := chunkIn(pg0, 4, 0)
	c.writeWhole(pg0, ch, 0, 1, 1)
	c.publish(c.next(func(t *Table) {
		setInterval(t, pg0, 2)
		_, e := t.entry(pg0)
		e.Members = []Member{{Disk: 2}, {Disk: 3}, {Disk: 1, Mark: MarkDown}}
	}), true)
	c.writeWhole(pg0, ch, 0, 1, 2)
	c.reps[0].plant.noRecovery = true
	c.publish(c.next(func(t *Table) {
		setInterval(t, pg0, 3)
		_, e := t.entry(pg0)
		e.Members = []Member{{Disk: 1}, {Disk: 3}, {Disk: 2, Mark: MarkDown}}
	}), true)
	c.logs[2], c.reformat[2] = 0xf0f0, true
	c.onFormat = func(int) uint32 { return 4 }
	c.s.CrashRandom(2)
	c.s.Restart(2)
	for !c.s.Node(2).Up() && c.step() {
	}
	c.reps[0].plant.noRecovery = false
	c.runFor(2 * RetryAfter)
	if x := c.listed(0, ch); x.ListedIn == 0 {
		t.Fatalf("disk 1 recovered the chunk while the table still lists disk 3, holding block 0 as %#x", c.diskBlock(0, ch, 0)[0])
	}
}

// Recovery writes pass write_limit: with disk 3 at 91% under a 90% limit, a client write adding
// data is refused, while recovery still copies disk 3 what it missed.
func TestRecoveryWritesPassWriteLimit(t *testing.T) {
	c := admitCluster(t, 90)
	ch, other := chunkIn(pg0, 4, 0), chunkIn(pg0, 4, 1)
	c.publish(c.next(func(t *Table) { mark(t, pg0, 3, MarkDown) }), true)
	c.writeWhole(pg0, ch, 0, 2, 3)
	c.reps[0].plant.noRecovery = true
	c.publish(c.next(func(t *Table) { mark(t, pg0, 3, MarkUp) }), true)
	c.xFills(910)
	if st := c.status(c.write(pg0, other, 0, []byte{1})); st != StatusNoSpace {
		t.Fatalf("a write adding data with disk 3 at 91%%: %v", st)
	}
	c.xFills(910)
	c.reps[0].plant.noRecovery = false
	c.runFor(RetryAfter / 2)
	c.settle()
	if x := c.listed(2, ch); x.ListedIn != 0 {
		t.Fatalf("disk 3 holds %+v", x)
	}
	c.blockIs(2, ch, 1, 3)
}

// PG 12, led by disk 1 like PGs 4 and 9, misses two members and they one each: recovery's first
// write is PG 12's though it is the highest PG, and PG 9, last among the equally degraded, starts
// only once PG 12's have. PG 12 lacks maxRecoveries chunks, so its recoveries alone fill the first
// window whatever order their reads complete in.
func TestTheMostDegradedPGRecoversFirst(t *testing.T) {
	tbl := rtable(16, 1)
	pgs := []core.PGID{{Pool: testPool, Index: 12}, {Pool: testPool, Index: 4}, {Pool: testPool, Index: 9}}
	for _, pg := range pgs {
		_, e := tbl.entry(pg)
		e.Members = []Member{{Disk: 1}, {Disk: 2}, {Disk: 3}}
	}
	c := newRCluster(t, tbl)
	c.reps[0].plant.noRecovery = true
	c.publish(c.next(func(t *Table) {
		mark(t, pgs[0], 2, MarkDown)
		mark(t, pgs[0], 3, MarkDown)
		_, e := t.entry(pgs[0])
		e.Interval--
		e.Past = e.Past[:len(e.Past)-1]
		mark(t, pgs[1], 3, MarkDown)
		mark(t, pgs[2], 3, MarkDown)
	}), true)
	chunks := func(pg core.PGID) uint32 {
		if pg == pgs[0] {
			return maxRecoveries
		}
		return 2
	}
	for _, pg := range pgs {
		for n := range chunks(pg) {
			c.writeWhole(pg, chunkIn(pg, 16, n), 0, 1, byte(1+n))
		}
	}
	sends := c.watchRecoveries()
	c.publish(c.next(func(t *Table) {
		for _, pg := range pgs {
			_, e := t.entry(pg)
			setInterval(t, pg, e.Interval+1)
			for i := range e.Members {
				e.Members[i].Mark = MarkUp
			}
		}
	}), true)
	c.reps[0].plant.noRecovery = false
	c.runFor(RetryAfter / 2)
	c.settle()
	var order []uint32
	last12, first9 := -1, -1
	for _, s := range *sends {
		pg := core.PGIndexOf(s.u.Chunk.Hash(), 16)
		order = append(order, pg)
		if pg == 12 {
			last12 = len(order) - 1
		}
		if pg == 9 && first9 < 0 {
			first9 = len(order) - 1
		}
	}
	if len(order) == 0 || order[0] != 12 || first9 < last12 {
		t.Fatalf("recovered PGs in the order %v", order)
	}
	for _, pg := range pgs {
		for n := range chunks(pg) {
			if x := c.listed(2, chunkIn(pg, 16, n)); x.ListedIn != 0 {
				t.Fatalf("disk 3 holds %+v of PG %d", x, pg.Index)
			}
		}
	}
}

// A primary lacking a chunk holds partial writes to it up to maxHeld client writes, and refuses
// the next.
func TestHeldPartialWritesAreBounded(t *testing.T) {
	tbl := rtable(4, 2)
	_, e := tbl.entry(pg0)
	e.Members[2].Mark = MarkDown
	c := newRCluster(t, tbl)
	ch := chunkIn(pg0, 4, 0)
	c.until(c.write(pg0, ch, 0, []byte{1}))
	c.reps[2].plant.noRecovery = true
	c.publish(c.next(func(t *Table) {
		setInterval(t, pg0, 2)
		_, e := t.entry(pg0)
		e.Members = []Member{{Disk: 3}, {Disk: 1, Mark: MarkDown}, {Disk: 2}}
	}), true)
	var ids []core.RequestID
	for i := range maxHeld + 1 {
		ids = append(ids, c.write(pg0, ch, uint32(i), []byte{2}))
	}
	c.settle()
	for i, id := range ids {
		if _, ok := c.cl.replies[id]; ok != (i == maxHeld) {
			t.Fatalf("write %d answered %v", i, ok)
		}
	}
	if rp := c.reply(ids[maxHeld]); rp.Status != StatusRefused {
		t.Fatalf("the write past the bound answered %+v", rp)
	}
	c.reps[2].plant.noRecovery = false
	for _, id := range ids[:maxHeld] {
		c.until(id)
		if rp := c.reply(id); rp.Status != StatusOK {
			t.Fatalf("a held write answered %+v", rp)
		}
	}
}

// Disk 3, the new primary, lacks the chunk and asks disk 2 for its image; its index rebuilds while
// the read is out, ending its leads and the covers its store kept. The reply then arrives and is
// dropped: the recovery ended with the rebuild, rather than ordering a write of a chunk the store
// no longer covers.
func TestARebuildEndsTheRecoveriesUnderWay(t *testing.T) {
	tbl := rtable(4, 1)
	_, e := tbl.entry(pg0)
	e.Members = []Member{{Disk: 2}, {Disk: 1, Mark: MarkDown}, {Disk: 3, Mark: MarkDown}}
	c := newRCluster(t, tbl)
	ch := chunkIn(pg0, 4, 0)
	c.writeWhole(pg0, ch, 0, 2, 1)
	c.reps[2].plant.noRecovery = true
	c.publish(c.next(func(t *Table) {
		setInterval(t, pg0, 2)
		_, e := t.entry(pg0)
		e.Members = []Member{{Disk: 3}, {Disk: 1}, {Disk: 2}}
	}), true)
	sent := false
	c.onPass = func(d int, acts []core.Action) {
		for i := range acts {
			if a := &acts[i]; d == 2 && a.Kind == core.ActNetSend && a.Msg.Len >= headSize && Kind(a.Msg.Body[1]) == KindRecoverRead {
				sent = true
			}
		}
	}
	c.reps[2].plant.noRecovery = false
	c.call(2, func(ctx *core.Context, r *Replica, out *ReplicaOutput) { r.drain(ctx, out) })
	for range 100_000 {
		if sent {
			break
		}
		c.step()
	}
	if !sent {
		t.Fatal("no recovery read")
	}
	c.s.Pause(1, clock.PauseFrozen, 2_000_000, false)
	c.call(2, func(ctx *core.Context, r *Replica, out *ReplicaOutput) {
		r.st.indexBroken(ctx, lsm.ErrCheck, &r.sout)
		r.st.pump(ctx, &r.sout)
		r.drain(ctx, out)
	})
	if n := len(c.reps[2].recs); n != 0 {
		t.Fatalf("%d recoveries outlived the rebuild", n)
	}
	c.runFor(5_000_000)
}

// Disk 1, the primary, orders a recovery write of disk 3's chunk, and its store fails it before
// logging any of it, as it does when its pool has no buffer for the blocks the members get: the
// primary asks for the next interval, whose recovery brings disk 3 the chunk, rather than leaving
// it listed with its cover spent.
func TestARecoveryWriteThatFailsEndsItsInterval(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	a := chunkIn(pg0, 4, 0)
	c.publish(c.next(func(t *Table) { mark(t, pg0, 3, MarkDown) }), true)
	c.writeWhole(pg0, a, 0, 2, 5)
	c.reps[0].plant.noRecovery = true
	c.publish(c.next(func(t *Table) { mark(t, pg0, 3, MarkUp) }), true)
	c.settle()
	c.reps[0].plant.noRecovery = false
	c.call(0, func(ctx *core.Context, r *Replica, out *ReplicaOutput) { r.drain(ctx, out) })
	for len(c.reps[0].recs) > 0 && c.reps[0].recs[0].req == 0 && c.step() {
	}
	if len(c.reps[0].recs) != 1 {
		t.Fatalf("recoveries %+v", c.reps[0].recs)
	}
	req := c.reps[0].recs[0].req
	c.call(0, func(ctx *core.Context, r *Replica, out *ReplicaOutput) {
		r.finished(ctx, Reply{ID: req, Err: core.ErrPoolExhausted}, out)
		r.drain(ctx, out)
	})
	c.settle()
	if !slices.ContainsFunc(c.asks, func(f PGFields) bool { return f.PG == pg0 && f.Interval == 3 }) {
		t.Fatalf("no ask for interval 3's successor: %+v", c.asks)
	}
}

// A partial-block write to a chunk the last peering found unfound is answered as unfound at once,
// not held for a recovery no member can source, nor refused as if the interval had ended.
func TestAPartialWriteToAnUnfoundChunkIsAnsweredUnfound(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	ch, lost := chunkIn(pg0, 4, 0), chunkIn(pg0, 4, 1)
	c.until(c.write(pg0, ch, 0, []byte{1}))
	l := &c.reps[0].leads[c.reps[0].lead(pg0)]
	l.kept = []keptChunk{{chunk: lost, kept: MakeVersion(1, 9), unfound: true, lacking: true}}
	w := c.write(pg0, lost, 0, []byte{2})
	c.until(w)
	if rp := c.reply(w); rp.Status != StatusUnfound || len(l.held) != 0 {
		t.Fatalf("the write answered %+v with %d held", rp, len(l.held))
	}
}

// Recovery allocates nothing once warm: rounds of disk 3 missing writes to 6 chunks and recovering
// them, the last round's events making no heap allocation.
func TestRecoveryAllocatesNothing(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	for round := range 4 {
		c.publish(c.next(func(t *Table) { mark(t, pg0, 3, MarkDown) }), true)
		for k := range uint32(6) {
			c.writeWhole(pg0, chunkIn(pg0, 4, k), 0, 2, byte(round+1))
		}
		c.reps[0].plant.noRecovery = true
		c.publish(c.next(func(t *Table) { mark(t, pg0, 3, MarkUp) }), true)
		c.settle()
		c.mallocs, c.counting = 0, round == 3
		c.reps[0].plant.noRecovery = false
		c.runFor(RetryAfter / 2)
		c.settle()
		c.counting = false
		for k := range uint32(6) {
			if x := c.listed(2, chunkIn(pg0, 4, k)); x.ListedIn != 0 {
				t.Fatalf("round %d: chunk %d still listed %+v", round, k, x)
			}
		}
	}
	if c.mallocs != 0 {
		t.Fatalf("%d heap allocations over a warm round of recovery", c.mallocs)
	}
}

// Primary 3 and member 1 both lack the chunk disk 2 holds, so primary 3 reads it from disk 2, and each
// one's record of its copy names disk 2, the member's by the recovery write.
func TestARecoveredRecordNamesItsSource(t *testing.T) {
	tbl := rtable(4, 1)
	_, e := tbl.entry(pg0)
	e.Members = []Member{{Disk: 2}, {Disk: 1, Mark: MarkDown}, {Disk: 3, Mark: MarkDown}}
	c := newRCluster(t, tbl)
	ch := chunkIn(pg0, 4, 0)
	c.writeWhole(pg0, ch, 0, 2, 1)
	c.conformBegin()
	c.publish(c.next(func(t *Table) {
		setInterval(t, pg0, 2)
		_, e := t.entry(pg0)
		e.Members = []Member{{Disk: 3}, {Disk: 1}, {Disk: 2}}
	}), true)
	c.settle()
	got := map[uint32]uint64{}
	for line := range bytes.Lines(c.s.Trace()) {
		r, err := trace.Parse(bytes.TrimSuffix(line, []byte("\n")))
		if err != nil {
			t.Fatal(err)
		}
		if r.Name == trace.OutRecovered {
			got[r.Node+1] = r.Fields[2]
		}
	}
	if want := (map[uint32]uint64{1: 2, 3: 2}); !maps.Equal(got, want) {
		t.Fatalf("recovered records by disk name sources %v, want %v", got, want)
	}
}

// Primary 3's recovery write of the chunk it lacks waits on its member, then a table moves the
// primary: the store's answer still makes its record, which the spec's check of a late write reads.
func TestALeadThatLeftStillRecordsItsRecovery(t *testing.T) {
	tbl := rtable(4, 1)
	_, e := tbl.entry(pg0)
	e.Members = []Member{{Disk: 2}, {Disk: 1, Mark: MarkDown}, {Disk: 3, Mark: MarkDown}}
	c := newRCluster(t, tbl)
	ch := chunkIn(pg0, 4, 0)
	c.writeWhole(pg0, ch, 0, 2, 1)
	c.conformBegin()
	c.drop = func(d int, a *core.Action) bool {
		return a.Kind == core.ActNetSend && a.Msg.Len >= headSize && Kind(a.Msg.Body[1]) == KindRecover
	}
	c.publish(c.next(func(t *Table) {
		setInterval(t, pg0, 2)
		_, e := t.entry(pg0)
		e.Members = []Member{{Disk: 3}, {Disk: 1}, {Disk: 2}}
	}), true)
	c.settle()
	records := func() (n int) {
		for line := range bytes.Lines(c.s.Trace()) {
			r, err := trace.Parse(bytes.TrimSuffix(line, []byte("\n")))
			if err != nil {
				t.Fatal(err)
			}
			if r.Name == trace.OutRecovered && r.Node == 2 {
				n++
			}
		}
		return n
	}
	if n := records(); n != 0 {
		t.Fatalf("primary 3 recorded %d recoveries with no member's answer", n)
	}
	c.publish(c.next(func(t *Table) {
		setInterval(t, pg0, 3)
		_, e := t.entry(pg0)
		e.Members = []Member{{Disk: 2}, {Disk: 1}, {Disk: 3}}
	}), false)
	c.settle()
	if n := records(); n != 1 {
		t.Fatalf("primary 3 recorded %d recoveries after it left the lead, want 1", n)
	}
}
