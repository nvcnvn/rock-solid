package chunkstore

import (
	"bytes"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// soleRun drives a rep-replica cluster of disks disks through real placement: every table comes from Next, with the authority's marks and the sim's clock.
type soleRun struct {
	c        *rcluster
	a        *authority
	isolated map[uint32]bool
}

func newSoleRun(t *testing.T, replicas, minLive, disks int) *soleRun {
	a := &authority{pool: Pool{Replicas: replicas, MinLive: minLive, FailureDomain: DomainNode, MaxPerDomain: 1, Locality: LocalitySpread, Overcommit: true, WriteLimit: 90, PGCount: 1},
		down: map[uint32]core.Tick{}, drain: map[uint32]bool{}}
	for n := range disks {
		if _, err := a.rec.Report(uint32(n), 0, 0x5eed+uint64(n)); err != nil {
			t.Fatal(err)
		}
	}
	var empty Table
	tbl, _ := Next(&empty, &Inputs{Pools: a.pools()})
	c := newSoleCluster(t, tbl, disks)
	return &soleRun{c: c, a: a, isolated: map[uint32]bool{}}
}

// newSoleCluster starts a node per disk under tbl, the disks tbl leaves out as sole members of extra PGs below min_live, which the next table drops.
func newSoleCluster(t *testing.T, tbl Table, disks int) *rcluster {
	sized := cloneTable(tbl)
	_, e := tbl.entry(pg0)
	for d := uint32(1); d <= uint32(disks); d++ {
		if !slices.ContainsFunc(e.Members, func(m Member) bool { return m.Disk == d }) {
			sized.Pools[0].PGs = append(sized.Pools[0].PGs, PGEntry{Members: []Member{{Disk: d}}, Interval: 1})
		}
	}
	c := newRClusterWith(t, sized, sim.New(1, sim.DefaultSettings()), nil)
	if len(sized.Pools[0].PGs) > 1 {
		next := cloneTable(tbl)
		next.Epoch = sized.Epoch + 1
		c.publish(next, true)
	}
	return c
}

func (r *soleRun) members() []Member {
	_, e := r.c.table.entry(pg0)
	return e.Members
}

// publish computes the next table from the marks, the reports and the clock, and hands it to the running replicas.
func (r *soleRun) publish() {
	c := r.c
	next, _ := Next(&c.table, r.a.inputs(c))
	if d := MakeDelta(&c.table, &next); len(d.Entries) == 0 {
		return
	}
	c.noteTable(&next)
	c.table = next
	for n := range c.reps {
		if !r.isolated[uint32(n)+1] {
			c.call(n, func(ctx *core.Context, rep *Replica, out *ReplicaOutput) { rep.SetTable(ctx, next, out) })
		}
	}
}

// lose cuts disk off from every node, as a power cut would, and marks it down; its disk stays as it was.
func (r *soleRun) lose(disk uint32) {
	r.isolated[disk] = true
	r.a.down[disk] = r.c.s.Time()
	n := uint32(r.c.node(disk))
	for o := range uint32(len(r.c.reps)) + 1 {
		if o != n {
			r.c.s.Partition(n, o, false)
		}
	}
}

// back reconnects disk, whole as it was cut off, marks it up and hands it the newest table.
func (r *soleRun) back(disk uint32) {
	delete(r.isolated, disk)
	delete(r.a.down, disk)
	n := uint32(r.c.node(disk))
	for o := range uint32(len(r.c.reps)) + 1 {
		if o != n && !r.isolated[uint32(o)+1] {
			r.c.s.Heal(n, o)
		}
	}
	t := r.c.table
	r.c.call(int(n), func(ctx *core.Context, rep *Replica, out *ReplicaOutput) { rep.SetTable(ctx, t, out) })
}

// tick lets d pass, publishing tables and recording what the primaries report every second.
func (r *soleRun) tick(d core.Tick) {
	for end := r.c.s.Time() + d; r.c.s.Time() < end; {
		r.c.runFor(1_000_000)
		r.publish()
	}
}

// answered steps the cluster until the client has heard of id, at most for d, publishing every second.
func (r *soleRun) answered(id core.RequestID, d core.Tick) (Response, bool) {
	c := r.c
	end := c.s.Time() + d
	nextPub := c.s.Time()
	for c.s.Time() < end {
		if rp, ok := c.cl.replies[id]; ok {
			return rp, true
		}
		if c.s.Time() >= nextPub {
			r.publish()
			nextPub = c.s.Time() + 1_000_000
		}
		if !c.step() {
			c.runFor(10_000)
		}
	}
	rp, ok := c.cl.replies[id]
	return rp, ok
}

// read is one read of ch at the PG's primary, asked again while it is refused, for up to 5 s.
func (r *soleRun) read(ch core.ChunkID) (Response, []byte) {
	c := r.c
	for range 50 {
		id := c.read(pg0, ch, 0, 1, c.primary(pg0))
		if rp, ok := r.answered(id, 100_000); ok && rp.Status != StatusRefused {
			return rp, c.cl.data[id]
		}
		r.publish()
	}
	return Response{Status: StatusRefused}, nil
}

// holders are the members holding version v of ch: their index has it and their missing list does not name the chunk.
func (r *soleRun) holders(ch core.ChunkID, v uint64) []uint32 {
	var hs []uint32
	for _, m := range r.members() {
		n := r.c.node(m.Disk)
		if n < 0 || !r.c.s.Node(uint32(n)).Up() || m.Mark != MarkUp {
			continue
		}
		x, _ := r.c.reps[n].Store().Index().Chunk(testPool, ch)
		if x.ListedIn == 0 && x.Whole >= v {
			hs = append(hs, m.Disk)
		}
	}
	return hs
}

// A rep3 pool with min_live 2, no refill: disk 2 is down, W reaches disk 1 and is on its way to disk 3 when disk 3 goes too and disk 2 comes back. The interval of disks 1 and 2 keeps W, since disk 2 was not live in W's interval, but disk 1 is its only copy, so the client is refused. Its resend lands on both, disk 1 is then lost for good, and W reads from the others.
func TestAWriteAcknowledgedAtMinLiveSurvivesTheLossOfOneDisk(t *testing.T) {
	r := newSoleRun(t, 3, 2, 3)
	c := r.c
	ch, old := chunkIn(pg0, 1, 0), chunkIn(pg0, 1, 1)
	x := bytes.Repeat([]byte{0x11}, core.BlockSize)
	c.until(c.write(pg0, old, 0, x))
	c.until(c.write(pg0, ch, 0, x))
	c.settle()
	ms := r.members()
	d1, d2, d3 := ms[0].Disk, ms[1].Disk, ms[2].Disk

	r.lose(d2)
	r.publish()
	c.untilActive()
	c.settle()

	r.lose(d3)
	w := bytes.Repeat([]byte{0x22}, core.BlockSize)
	id := c.write(pg0, ch, 0, w)
	c.runFor(5_000)
	if rp, ok := c.cl.replies[id]; ok {
		t.Fatalf("W answered %+v on the primary alone", rp)
	}
	first := c.newest(c.node(d1), ch)

	r.back(d2)
	r.publish()
	if rp, ok := r.answered(id, 100_000_000); !ok || rp.Status != StatusRefused {
		t.Fatalf("W, on disk %d alone, answered %+v, %v", d1, rp, ok)
	}
	if h := r.holders(ch, first); len(h) != 1 || h[0] != d1 {
		t.Fatalf("refused W, kept, is held by %v", h)
	}

	retry := c.write(pg0, ch, 0, w)
	rp, ok := r.answered(retry, 100_000_000)
	if !ok || rp.Status != StatusOK || rp.Version <= first {
		t.Fatalf("W sent again answered %+v, %v, above %#x", rp, ok, first)
	}
	r.tick(2_000_000)
	if h := r.holders(ch, rp.Version); len(h) != 2 {
		t.Fatalf("W sent again is held by %v", h)
	}

	r.lose(d1)
	r.back(d3)
	r.tick(5_000_000)
	if orp, odata := r.read(old); orp.Status != StatusOK || !bytes.Equal(odata, x) {
		t.Fatalf("X, on two of three disks, read %+v", orp)
	}
	if grp, got := r.read(ch); grp.Status != StatusOK || !bytes.Equal(got, w) {
		t.Fatalf("W, acknowledged OK as %#x, read back %+v after the loss of one disk of a rep3 pool with min_live 2", rp.Version, grp)
	}
}

// The model's counterexample on real placement: every other member is cut off while W is on its way, so W is on the primary alone; the PG below min_live waits degraded_refill_after, and a refill brings it back to min_live with a member that lists W. Peering keeps W but its client is refused. The resend lands on both, so when the primary is cut off and a second refill removes it, W reads from the member left.
func TestARefillKeepsTheOnlyCopyOfAnAcknowledgedWrite(t *testing.T) {
	for _, shape := range []struct {
		name              string
		replicas, minLive int
		// out is how long the primary stays out: on rep3 a refill first replaces the member down longest, which brings the PG back to min_live, so the primary goes only after refill_after.
		out core.Tick
	}{{"rep3", 3, 2, RefillAfter + 5_000_000}, {"rep2", 2, 2, DegradedRefillAfter + 5_000_000}} {
		t.Run(shape.name, func(t *testing.T) {
			r := newSoleRun(t, shape.replicas, shape.minLive, shape.replicas+3)
			c := r.c
			ch := chunkIn(pg0, 1, 0)
			ms := r.members()
			first := ms[0].Disk
			others := make([]uint32, 0, 2)
			for _, m := range ms[1:] {
				others = append(others, m.Disk)
				r.lose(m.Disk)
			}
			w := bytes.Repeat([]byte{0x22}, core.BlockSize)
			id := c.write(pg0, ch, 0, w)
			c.runFor(5_000)
			r.publish()
			if rp, ok := r.answered(id, 2*DegradedRefillAfter); !ok || rp.Status != StatusRefused {
				t.Fatalf("W, on the primary alone, answered %+v, %v", rp, ok)
			}
			if ms := r.members(); slices.ContainsFunc(ms, func(m Member) bool { return m.Disk == others[0] && m.Mark == MarkUp }) {
				t.Fatalf("no refill: members %v", ms)
			}

			retry := c.write(pg0, ch, 0, w)
			rp, ok := r.answered(retry, 2*DegradedRefillAfter)
			if !ok || rp.Status != StatusOK {
				t.Fatalf("W sent again answered %+v, %v", rp, ok)
			}
			r.tick(2_000_000)
			if h := r.holders(ch, rp.Version); len(h) != shape.minLive {
				t.Fatalf("W sent again is held by %v, members %v", h, r.members())
			}

			r.lose(first)
			r.tick(shape.out)
			for _, d := range append(others, first) {
				r.back(d)
			}
			r.tick(5_000_000)
			if slices.ContainsFunc(r.members(), func(m Member) bool { return m.Disk == first }) {
				t.Fatalf("disk %d is a member again: %v", first, r.members())
			}
			if grp, got := r.read(ch); grp.Status != StatusOK || !bytes.Equal(got, w) {
				t.Fatalf("W read back %+v after a refill removed disk %d", grp, first)
			}
		})
	}
}

// loneWrite leaves W on the first member's log alone and hands the PG to the first two: the second comes back as the third goes.
func (r *soleRun) loneWrite(ch core.ChunkID) core.RequestID {
	c := r.c
	ms := r.members()
	r.lose(ms[1].Disk)
	r.publish()
	c.untilActive()
	c.settle()
	r.lose(ms[2].Disk)
	id := c.write(pg0, ch, 0, bytes.Repeat([]byte{0x22}, core.BlockSize))
	c.runFor(5_000)
	r.back(ms[1].Disk)
	r.publish()
	return id
}

// At min_live 1 the same write has copies enough: W, on disk 1 alone, keeps its OK.
func TestALoneCopyIsEnoughAtMinLiveOne(t *testing.T) {
	r := newSoleRun(t, 3, 1, 3)
	ch := chunkIn(pg0, 1, 0)
	r.c.until(r.c.write(pg0, ch, 0, bytes.Repeat([]byte{0x11}, core.BlockSize)))
	r.c.settle()
	if rp, ok := r.answered(r.loneWrite(ch), 100_000_000); !ok || rp.Status != StatusOK {
		t.Fatalf("W answered %+v, %v", rp, ok)
	}
}

// The limit of keeping a refused version: its one holder dies, so its chunk is unfound though X, below it, is on the others. No acknowledged write is lost, and the chunk reads again once the disk returns.
func TestARefusedVersionLeavesItsChunkUnfoundWithItsHolder(t *testing.T) {
	r := newSoleRun(t, 3, 2, 3)
	c := r.c
	ch := chunkIn(pg0, 1, 0)
	c.until(c.write(pg0, ch, 0, bytes.Repeat([]byte{0x11}, core.BlockSize)))
	c.settle()
	ms := r.members()
	for n := range c.reps {
		c.reps[n].plant.noRecovery = true
	}
	if rp, ok := r.answered(r.loneWrite(ch), 100_000_000); !ok || rp.Status != StatusRefused {
		t.Fatalf("W answered %+v, %v", rp, ok)
	}
	r.lose(ms[0].Disk)
	r.back(ms[2].Disk)
	r.tick(5_000_000)
	if rp, _ := r.read(ch); rp.Status != StatusUnfound {
		t.Fatalf("chunk read %+v with W's holder gone", rp)
	}
	r.back(ms[0].Disk)
	r.tick(5_000_000)
	if rp, got := r.read(ch); rp.Status != StatusOK || got[0] != 0x22 {
		t.Fatalf("chunk read %+v with W's holder back", rp)
	}
}

// Only what a decision proves counts: an up member it left off the list and a member in maintenance, not a listed member, a down one, or an up one that gave no reply.
func TestCopiesCountsWhatTheDecisionProves(t *testing.T) {
	c, other := chunkIn(pg0, 1, 0), chunkIn(pg0, 1, 1)
	listed := []ChunkVersion{{Chunk: c, Version: 7}}
	for _, tc := range []struct {
		name    string
		members []Member
		replies []peerReply
		want    int
	}{
		{"clean", []Member{{Disk: 1}, {Disk: 2}}, []peerReply{{disk: 1}, {disk: 2}}, 2},
		{"listed", []Member{{Disk: 1}, {Disk: 2}}, []peerReply{{disk: 1}, {disk: 2, list: listed}}, 1},
		{"listed for another chunk", []Member{{Disk: 1}, {Disk: 2}}, []peerReply{{disk: 1}, {disk: 2, list: []ChunkVersion{{Chunk: other}}}}, 2},
		{"maintenance", []Member{{Disk: 1}, {Disk: 2, Mark: MarkMaint}}, []peerReply{{disk: 1}}, 2},
		{"down, reached in a past interval", []Member{{Disk: 1}, {Disk: 2, Mark: MarkDown}}, []peerReply{{disk: 1}, {disk: 2}}, 1},
		{"reached, no longer a member", []Member{{Disk: 1}}, []peerReply{{disk: 1}, {disk: 2}}, 1},
		{"up, no reply", []Member{{Disk: 1}, {Disk: 2}}, []peerReply{{disk: 1}}, 1},
	} {
		if got := copies(&PGEntry{Members: tc.members}, tc.replies, c); got != tc.want {
			t.Errorf("%s: %d copies, want %d", tc.name, got, tc.want)
		}
	}
}
