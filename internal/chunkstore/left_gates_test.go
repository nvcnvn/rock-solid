package chunkstore

import (
	"bytes"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/lsm"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// leftRun is a rep2 PG that drained a member to a third disk with Z written once the move's interval recovered, so the
// leaving member logged Z; that member crashed, the table removing it was published, and it restarted, its replay gating Z.
type leftRun struct {
	*soleRun
	ch      core.ChunkID
	z       uint64
	leaving uint32
	n       int
}

// newLeftRun builds a leftRun; the restarted member's first table is the one removing it, which still names it in the
// interval the move ran in, its last activated.
func newLeftRun(t *testing.T, seed uint64, plant func(*replicaPlants)) *leftRun {
	set := sim.DefaultSettings()
	set.PoolBytes = 64 << 20
	r := seededSoleRunOn(t, seed, 2, 3, set, slotSeedGeometry)
	c := r.c
	plantAll(c, plant)
	ms := r.members()
	l := &leftRun{soleRun: r, ch: chunkIn(pg0, 1, 0), leaving: ms[1].Disk, n: c.node(ms[1].Disk)}
	r.a.moves = true
	r.a.drain[l.leaving] = true
	r.publish()
	if ms = r.members(); len(ms) != 3 {
		t.Fatalf("seed %d: no move: members %v", seed, ms)
	}
	c.untilActive()
	c.settle()
	_, e := c.table.entry(pg0)
	if !slices.ContainsFunc(c.recovered, func(f PGFields) bool { return f.Interval == e.Interval }) {
		t.Fatalf("seed %d: interval %d not reported recovered: %+v", seed, e.Interval, c.recovered)
	}
	id := c.write(pg0, l.ch, 0, bytes.Repeat([]byte{0x33}, core.ChunkSize))
	c.until(id)
	l.z = c.reply(id).Version
	c.settle()
	if got := c.listed(l.n, l.ch); got.Version != l.z {
		t.Fatalf("seed %d: the leaving member holds %+v, not Z %#x", seed, got, l.z)
	}
	c.s.CrashRandom(uint32(l.n))
	r.isolated[l.leaving] = true
	r.publish()
	_, e = c.table.entry(pg0)
	if slices.ContainsFunc(e.Members, func(m Member) bool { return m.Disk == l.leaving }) ||
		!slices.ContainsFunc(e.Past, func(p PastInterval) bool {
			return slices.ContainsFunc(p.Members, func(m Member) bool { return m.Disk == l.leaving })
		}) {
		t.Fatalf("seed %d: the table does not name the leaving disk in a past interval alone: %+v", seed, e)
	}
	c.s.Restart(uint32(l.n))
	delete(r.isolated, l.leaving)
	c.runFor(1_000_000)
	return l
}

// gated is the restarted member's store and the LSN and segment of the oldest record of a version it keeps gated, 0 for none.
func (l *leftRun) gated() (*Store, uint64, uint32) {
	st := l.c.reps[l.n].Store()
	at, seg := heldRecord(st)
	return st, at, seg
}

// heldRecord is the LSN and segment of the oldest record of a version st keeps gated, 0 for none.
func heldRecord(st *Store) (uint64, uint32) {
	oldest, seg := uint64(0), uint32(0)
	for i := range st.ops {
		if o := &st.ops[i]; o.kind != 0 && o.req.Gated && !o.dropped {
			for j := range o.items {
				if it := &o.items[j]; it.state == itemDurable && (oldest == 0 || it.lsn < oldest) {
					oldest, seg = it.lsn, uint32(uint64(it.lba)/uint64(st.sb.SegmentBlocks))
				}
			}
		}
	}
	return oldest, seg
}

// A leaving member down when its move completes and then restarted keeps Z, a whole chunk in a slot, gated while a table
// names it in an interval since the last activated one, which peering may still reach: Z holds the replay point short of
// its record, so its segment from the cleaner, and pins its slot. Once the interval without the member activates and a
// table names it in none, it drops Z, which holds neither any more.
func TestALeftMemberDropsItsGatedWritesOnceNoIntervalNamesIt(t *testing.T) {
	for seed := uint64(1); seed <= 3; seed++ {
		l := newLeftRun(t, seed, noPlant)
		st, at, seg := l.gated()
		u, _ := st.Log().Usage(seg)
		if !slices.Contains(st.Unsettled(nil), l.ch) || at == 0 || len(st.pinned) == 0 || len(st.Log().SlotsInUse(nil)) != 1 {
			t.Fatalf("seed %d: replay did not gate Z in a slot, or the first table dropped it: %v gated, %v pinned", seed, st.Unsettled(nil), st.pinned)
		}
		if st.replayPoint() >= at || u.Newest <= st.replayPoint() {
			t.Fatalf("seed %d: replay point %d past gated Z at LSN %d in segment %+v", seed, st.replayPoint(), at, u)
		}
		l.tick(10_000_000)
		_, e := l.c.table.entry(pg0)
		if l.c.reps[l.n].names(pg0) || len(e.Past) != 0 {
			t.Fatalf("seed %d: the table still names the left disk: %+v", seed, e)
		}
		st, left, _ := l.gated()
		u, _ = st.Log().Usage(seg)
		if got := st.Unsettled(nil); len(got) != 0 || left != 0 || len(st.pinned) != 0 {
			t.Fatalf("seed %d: the left member keeps %v gated, %v pinned", seed, got, st.pinned)
		}
		if got := st.replayPoint(); got < at || u.Newest > got {
			t.Fatalf("seed %d: replay point %d short of dropped Z at LSN %d in segment %+v", seed, got, at, u)
		}
		if got := st.Log().SlotsInUse(nil); len(got) != 0 {
			t.Fatalf("seed %d: the left member keeps chunk slots %v in use", seed, got)
		}
	}
}

func TestALeftMemberThatNeverSettlesKeepsItsGatedWrites(t *testing.T) {
	l := newLeftRun(t, 1, func(p *replicaPlants) { p.leftNeverSettles = true })
	l.tick(10_000_000)
	if st, at, _ := l.gated(); !slices.Contains(st.Unsettled(nil), l.ch) || st.replayPoint() >= at {
		t.Fatalf("leftNeverSettles: the left member keeps %v gated, replay point %d, Z at %d", st.Unsettled(nil), st.replayPoint(), at)
	}
}

func TestALeftMemberSettledEarlyDropsWhatAPastIntervalNames(t *testing.T) {
	l := newLeftRun(t, 1, func(p *replicaPlants) { p.settleLeftEarly = true })
	if st, _, _ := l.gated(); slices.Contains(st.Unsettled(nil), l.ch) {
		t.Fatalf("settleLeftEarly: the first table left Z gated")
	}
}

// The store's half of a left PG's drop: a version Z of the interval the disk last decided, replayed gated, holds the
// replay point short of its record and that record's segment from the cleaner. Settled to nothing, as a replica drops a
// left PG's gated writes, Z holds neither: once later writes roll its segment closed and a checkpoint names a replay
// point past it, the cleaner frees the segment.
func TestADroppedGatedWriteLeavesItsSegmentToTheCleaner(t *testing.T) {
	h := newHarness(t, cleanConfig())
	h.oracle = sim.NewOracle[writtenRecord](0, newStoreModel())
	z := chunk(2)
	h.persist(0, 1, 2, nil)
	h.sync(h.memberWrite(z, 0, 4, MakeVersion(2, 1), 0x7e))
	restart(h)
	at, seg := heldRecord(h.s)
	if !slices.Contains(h.s.Unsettled(nil), z) || h.s.replayPoint() >= at {
		t.Fatalf("replay left %v gated, replay point %d, Z at LSN %d", h.s.Unsettled(nil), h.s.replayPoint(), at)
	}
	h.settleChunk(z, 0)
	if got := h.s.Unsettled(nil); len(got) != 0 || h.s.replayPoint() < at {
		t.Fatalf("dropped Z: %v gated, replay point %d, Z at LSN %d", got, h.s.replayPoint(), at)
	}
	for i := range 300 {
		h.sync(h.write(chunk(1), 0, fill(64*core.BlockSize, byte(i)), 0))
	}
	h.clean(h.s.Log().FreeOutsideReserve() + 1)
	if u, _ := h.s.Log().Usage(seg); u.State != disklog.SegmentFree {
		t.Fatalf("segment %d of dropped Z's record is %+v, cleanable %v", seg, u, h.s.Log().Cleanable(seg))
	}
	h.holds()
}

// A rebuild of the left member's index after the drop replays Z and gates it again, as a restart would; with the table
// unchanged, the replica drops it again.
func TestALeftMemberDropsItsGatedWritesAgainAfterARebuild(t *testing.T) {
	l := newLeftRun(t, 1, noPlant)
	l.tick(10_000_000)
	if st, _, _ := l.gated(); len(st.Unsettled(nil)) != 0 {
		t.Fatalf("the left member keeps %v gated before the rebuild", st.Unsettled(nil))
	}
	epoch := l.c.table.Epoch
	l.c.call(l.n, func(ctx *core.Context, rep *Replica, out *ReplicaOutput) {
		rep.st.indexBroken(ctx, lsm.ErrCheck, &rep.sout)
		rep.st.pump(ctx, &rep.sout)
		rep.drain(ctx, out)
	})
	l.tick(10_000_000)
	if l.c.table.Epoch != epoch {
		t.Fatalf("a table was published after the rebuild: epoch %d -> %d", epoch, l.c.table.Epoch)
	}
	if st := l.c.reps[l.n].Store(); !st.Serving() {
		t.Fatal("the rebuilt store is not serving")
	}
	if err := l.c.leftGated(); err != nil {
		t.Fatal(err)
	}
}
