package chunkstore

import (
	"bytes"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

const strandSeeds = 20

// strandedChunk is a chunk whose acknowledged write X sits under Y, a write never acknowledged, on every member, with a primary that restarted since Y.
type strandedChunk struct {
	*soleRun
	ch                      core.ChunkID
	x                       uint64
	primary, leaving, added uint32
}

// seededSoleRun is newSoleRun over the scheduler of seed.
func seededSoleRun(t *testing.T, seed uint64, replicas, disks int) *soleRun {
	a := &authority{pool: Pool{Replicas: replicas, MinLive: 2, FailureDomain: DomainNode, MaxPerDomain: 1, Locality: LocalitySpread, Overcommit: true, WriteLimit: 90, PGCount: 1},
		down: map[uint32]core.Tick{}, drain: map[uint32]bool{}}
	for n := range disks {
		if _, err := a.rec.Report(uint32(n), 0, 0x5eed+uint64(n)); err != nil {
			t.Fatal(err)
		}
	}
	var empty Table
	tbl, _ := Next(&empty, &Inputs{Pools: a.pools()})
	sized := cloneTable(tbl)
	_, e := tbl.entry(pg0)
	for d := uint32(1); d <= uint32(disks); d++ {
		if !slices.ContainsFunc(e.Members, func(m Member) bool { return m.Disk == d }) {
			sized.Pools[0].PGs = append(sized.Pools[0].PGs, PGEntry{Members: []Member{{Disk: d}}, Interval: 1})
		}
	}
	c := newRClusterWith(t, sized, sim.New(seed, sim.DefaultSettings()), nil)
	if len(sized.Pools[0].PGs) > 1 {
		next := cloneTable(tbl)
		next.Epoch = sized.Epoch + 1
		c.publish(next, true)
	}
	return &soleRun{c: c, a: a, isolated: map[uint32]bool{}}
}

// strandedMove is the model's NoStranded counterexample on real placement, with no disk lost: a rep2 PG drains a member to a third disk, the new member recovers X and the primary reports the interval recovered. Y then reaches the primary and the new member; its send to the leaving member dies with the primary, as the table removing that member commits. The primary comes back, its replay maps Y, and it asks for a new interval, whose peering still reaches the leaving member through a past interval. That member lacks Y, so peering keeps X and drops Y, and each member lists the chunk.
func strandedMove(t *testing.T, seed uint64, yOff uint32) *strandedChunk {
	r := seededSoleRun(t, seed, 2, 3)
	c := r.c
	s := &strandedChunk{soleRun: r, ch: chunkIn(pg0, 1, 0)}
	id := c.write(pg0, s.ch, 0, bytes.Repeat([]byte{0x11}, core.BlockSize))
	c.until(id)
	s.x = c.reply(id).Version
	c.settle()
	ms := r.members()
	s.primary, s.leaving = ms[0].Disk, ms[1].Disk
	r.a.moves = true
	r.a.drain[s.leaving] = true
	r.publish()
	if ms = r.members(); len(ms) != 3 {
		t.Fatalf("no move: members %v", ms)
	}
	s.added = ms[2].Disk
	c.untilActive()
	c.settle()
	_, e := c.table.entry(pg0)
	if !slices.ContainsFunc(c.recovered, func(f PGFields) bool { return f.Interval == e.Interval }) {
		t.Fatalf("interval %d not reported recovered: %+v", e.Interval, c.recovered)
	}

	p, l := uint32(c.node(s.primary)), uint32(c.node(s.leaving))
	c.s.Partition(p, l, true)
	c.write(pg0, s.ch, yOff, bytes.Repeat([]byte{0x22}, core.BlockSize))
	c.runFor(5_000)
	c.s.CrashRandom(p)
	r.isolated[s.primary] = true
	r.publish()
	if ms = r.members(); slices.ContainsFunc(ms, func(m Member) bool { return m.Disk == s.leaving }) {
		t.Fatalf("no removal: members %v", ms)
	}
	c.s.Heal(p, l)
	c.s.Restart(p)
	delete(r.isolated, s.primary)
	r.tick(10_000_000)
	return s
}

// disk is what the store of disk holds of the chunk, by its index.
func (s *strandedChunk) disk(d uint32) Chunk { return s.c.listed(s.c.node(d), s.ch) }

// block is the first byte of block b of the chunk as disk maps it.
func (s *strandedChunk) block(d uint32, b uint16) byte { return s.c.diskBlock(s.c.node(d), s.ch, b)[0] }

// FAILS ON PURPOSE until the bug it reproduces is fixed; see strandedMove. X was acknowledged, no disk was lost and a whole copy exists, yet no read of X succeeds.
func TestKnownBugAcknowledgedWriteReadsAfterAMoveAndARestart(t *testing.T) {
	for _, tc := range []struct {
		name string
		yOff uint32
	}{{"Y beside X", core.BlockSize}, {"Y over X", 0}} {
		t.Run(tc.name, func(t *testing.T) {
			s := strandedMove(t, 1, tc.yOff)
			if rp, _ := s.read(s.ch); rp.Status != StatusOK {
				t.Fatalf("acknowledged X read %+v with no disk lost, whole on disk %d %+v, members %v", rp, s.leaving, s.disk(s.leaving), s.members())
			}
		})
	}
}

// What the bug leaves, on every seed tried: both members list the chunk, so a read gets StatusMissing, and the decision does not call it unfound, since the removed disk's reply was clean. The removed disk keeps X, in its index and on its disk, for as long as the run lasts, nothing removes or heals anything, and Y's blocks hide X's on the members wherever they overlap. This pins today's behaviour for the fix to change.
func TestAStrandedChunkStaysOnTheRemovedDisk(t *testing.T) {
	for seed := uint64(1); seed <= strandSeeds; seed++ {
		for _, yOff := range []uint32{core.BlockSize, 0} {
			s := strandedMove(t, seed, yOff)
			c := s.c
			check := func() {
				t.Helper()
				if a, b := s.disk(s.primary), s.disk(s.added); a.ListedIn == 0 || b.ListedIn == 0 || a.Listed != s.x || b.Listed != s.x {
					t.Fatalf("seed %d: members hold %+v and %+v", seed, a, b)
				}
				if x := s.disk(s.leaving); x.Whole != s.x || x.Version != s.x || x.ListedIn != 0 || s.block(s.leaving, 0) != 0x11 {
					t.Fatalf("seed %d: removed disk holds %+v", seed, x)
				}
				if rp, _ := s.read(s.ch); rp.Status != StatusMissing {
					t.Fatalf("seed %d: read %+v", seed, rp)
				}
			}
			check()
			want := byte(0x11)
			if yOff == 0 {
				want = 0x22
			}
			for _, d := range []uint32{s.primary, s.added} {
				if got := s.block(d, 0); got != want {
					t.Fatalf("seed %d: member %d holds X's block as %#x", seed, d, got)
				}
			}
			n := c.primary(pg0)
			if l := &c.reps[n].leads[c.reps[n].lead(pg0)]; l.isUnfound(s.ch) {
				t.Fatalf("seed %d: decision calls the chunk unfound", seed)
			}
			before := c.table.Epoch
			s.tick(120_000_000)
			check()
			if c.table.Epoch != before {
				t.Fatalf("seed %d: the table moved on from %d to %d", seed, before, c.table.Epoch)
			}
		}
	}
}

// Without the restart the same steps heal: the primary still holds Y gated, peering hears the removed disk and drops Y, and the primary's own image of X is the source for the new member. The new member's replies are held back so that the removed disk's reply comes first, as a restart's wait lets it.
func TestAPrimaryThatDidNotRestartRecoversTheMove(t *testing.T) {
	r := seededSoleRun(t, 1, 2, 3)
	c := r.c
	ch := chunkIn(pg0, 1, 0)
	id := c.write(pg0, ch, 0, bytes.Repeat([]byte{0x11}, core.BlockSize))
	c.until(id)
	x := c.reply(id).Version
	c.settle()
	ms := r.members()
	primary, leaving := ms[0].Disk, ms[1].Disk
	r.a.moves = true
	r.a.drain[leaving] = true
	r.publish()
	added := r.members()[2].Disk
	c.untilActive()
	c.settle()

	p, l, n := uint32(c.node(primary)), uint32(c.node(leaving)), uint32(c.node(added))
	c.s.Partition(p, l, true)
	c.write(pg0, ch, core.BlockSize, bytes.Repeat([]byte{0x22}, core.BlockSize))
	c.runFor(5_000)
	c.s.Heal(p, l)
	c.s.Queue(n, p, 50_000)
	r.publish()
	r.tick(10_000_000)
	if rp, got := r.read(ch); rp.Status != StatusOK || rp.Version != x || got[0] != 0x11 {
		t.Fatalf("X read %+v, %#x, members %v", rp, got, r.members())
	}
	for _, d := range []uint32{primary, added} {
		if got := c.listed(c.node(d), ch); got.ListedIn != 0 {
			t.Fatalf("disk %d still lists the chunk: %+v", d, got)
		}
	}
}

// An operator's edit naming the removed disk heals the chunk if no write landed on it first: the disk is a clean source of X, and Y is gone.
func TestAnEditNamingTheRemovedDiskHealsAStrandedChunk(t *testing.T) {
	for _, yOff := range []uint32{core.BlockSize, 0} {
		s := strandedMove(t, 1, yOff)
		s.a.edits = append(s.a.edits, Edit{PG: pg0, Disks: []uint32{s.primary, s.leaving}})
		s.tick(10_000_000)
		if rp, got := s.read(s.ch); rp.Status != StatusOK || rp.Version != s.x || got[0] != 0x11 {
			t.Fatalf("X read %+v, %#x, members %v", rp, got, s.members())
		}
	}
}

// Writes on a stranded chunk: a partial block waits for a recovery that gave up, unanswered, while a whole block hears OK and reads StatusMissing. Once one is acknowledged, the next interval keeps it, no disk holds it clean, the chunk turns unfound, and the removed disk, which lacks it, no longer heals it: X is out of reach for good, with its bytes on healthy disks.
func TestAWriteOnAStrandedChunkEndsTheWayBack(t *testing.T) {
	s := strandedMove(t, 1, core.BlockSize)
	c := s.c
	if rp, ok := s.answered(c.write(pg0, s.ch, 10, []byte{0x33}), 5_000_000); ok {
		t.Fatalf("a partial write answered %+v", rp)
	}
	z := c.write(pg0, s.ch, 2*core.BlockSize, bytes.Repeat([]byte{0x44}, core.BlockSize))
	if rp, ok := s.answered(z, 5_000_000); !ok || rp.Status != StatusOK {
		t.Fatalf("a whole-block write answered %+v, %v", rp, ok)
	}
	if rp, _ := s.read(s.ch); rp.Status != StatusMissing {
		t.Fatalf("read %+v after an OK", rp)
	}

	_, e := c.table.entry(pg0)
	s.a.asks = append(s.a.asks, PGInterval{PG: pg0, Interval: e.Interval})
	s.tick(10_000_000)
	if rp, _ := s.read(s.ch); rp.Status != StatusUnfound {
		t.Fatalf("read %+v in the next interval", rp)
	}

	clear(s.a.drain)
	s.a.edits = append(s.a.edits, Edit{PG: pg0, Disks: []uint32{s.primary, s.leaving}})
	s.tick(120_000_000)
	if !slices.ContainsFunc(s.members(), func(m Member) bool { return m.Disk == s.leaving }) {
		t.Fatalf("members %v", s.members())
	}
	if rp, _ := s.read(s.ch); rp.Status != StatusUnfound {
		t.Fatalf("read %+v with the removed disk back", rp)
	}
	if got := s.block(s.leaving, 0); got != 0x11 {
		t.Fatalf("removed disk holds X's block as %#x", got)
	}
	if got := s.block(s.primary, 0); got != 0x11 {
		t.Fatalf("primary holds X's block as %#x", got)
	}
}

// The same hole with no move and no removal: on a rep3 PG a disk comes back and lists the chunk, and before recovery reaches it Y lands on the other two, not on it, and the primary restarts. The disk that lacks Y lists the chunk, so it is no source, yet it alone drops Y, which leaves every member listing it: X is unfound for good with every disk up, and held under Y by two of them. noRecovery stands in for recovery that has not reached the chunk. This pins today's behaviour for the fix to change.
func TestARestartDuringRecoveryLeavesNoMemberASource(t *testing.T) {
	r := seededSoleRun(t, 1, 3, 3)
	c := r.c
	ch := chunkIn(pg0, 1, 0)
	c.until(c.write(pg0, ch, 0, bytes.Repeat([]byte{0x11}, core.BlockSize)))
	c.settle()
	ms := r.members()
	a, b, d := ms[0].Disk, ms[1].Disk, ms[2].Disk
	r.lose(d)
	r.publish()
	c.untilActive()
	c.settle()
	id := c.write(pg0, ch, 0, bytes.Repeat([]byte{0x33}, core.BlockSize))
	c.until(id)
	x := c.reply(id).Version
	c.settle()
	for n := range c.reps {
		c.reps[n].plant.noRecovery = true
	}
	r.back(d)
	r.publish()
	c.untilActive()
	c.settle()
	if got := c.listed(c.node(d), ch); got.Listed != x || got.ListedIn == 0 {
		t.Fatalf("returned disk holds %+v", got)
	}

	an, dn := uint32(c.node(a)), uint32(c.node(d))
	c.s.Partition(an, dn, true)
	c.write(pg0, ch, core.BlockSize, bytes.Repeat([]byte{0x22}, core.BlockSize))
	c.runFor(5_000)
	c.s.CrashRandom(an)
	c.s.Heal(an, dn)
	c.s.Restart(an)
	r.tick(20_000_000)
	for _, m := range []uint32{a, b, d} {
		if got := c.listed(c.node(m), ch); got.Listed != x || got.ListedIn == 0 {
			t.Fatalf("disk %d holds %+v", m, got)
		}
	}
	for _, m := range []uint32{a, b} {
		if got := c.diskBlock(c.node(m), ch, 0)[0]; got != 0x33 {
			t.Fatalf("disk %d holds X's block as %#x", m, got)
		}
	}
	r.tick(120_000_000)
	if rp, _ := r.read(ch); rp.Status != StatusUnfound {
		t.Fatalf("read %+v", rp)
	}
}
