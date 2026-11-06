package chunkstore

import (
	"bytes"
	"os"
	"slices"
	"strconv"
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

// A rep3 pool with min_live 2, no refill: disk 2 is down, W reaches disk 1 and is on its way to disk 3 when disk 3 goes too and disk 2 comes back. The interval of disks 1 and 2 keeps W, since disk 2 was not live in W's interval, and the primary answers W OK on one copy. Disk 1 is then lost for good and disks 2 and 3 are whole again: X, on them both, reads; W, acknowledged, is gone.
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
	t.Logf("members %v", ms)

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
	t.Logf("W pending: disk %d newest %#x, disk %d newest %#x", d1, c.newest(c.node(d1), ch), d3, c.newest(c.node(d3), ch))

	r.back(d2)
	r.publish()
	rp, ok := r.answered(id, 100_000_000)
	if !ok {
		t.Fatal("W unanswered")
	}
	if rp.Status != StatusOK {
		t.Skipf("W answered %+v: a refused write promises nothing", rp)
	}
	t.Logf("t=%d W answered %+v; members %v; real holders %v", c.s.Time(), rp, r.members(), r.holders(ch, rp.Version))

	r.lose(d1)
	r.back(d3)
	r.tick(5_000_000)
	_, e := c.table.entry(pg0)
	t.Logf("t=%d members %v interval %d activated %d", c.s.Time(), r.members(), e.Interval, e.Activated)

	orp, odata := r.read(old)
	if orp.Status != StatusOK || !bytes.Equal(odata, x) {
		t.Fatalf("X, on two of three disks, read %+v", orp)
	}
	grp, got := r.read(ch)
	t.Logf("W read %+v", grp)
	if grp.Status != StatusOK || !bytes.Equal(got, w) {
		t.Fatalf("W, acknowledged OK as %#x, read back %+v after the loss of one disk of a rep3 pool with min_live 2", rp.Version, grp)
	}
}

// The model's counterexample on real placement: every other member is cut off while W is on its way, so W is on the primary alone; the PG below min_live waits degraded_refill_after, a refill brings it back to min_live with a member that lists W, peering keeps W and the primary answers it OK. The primary is then cut off, a second refill removes it, and every old disk comes back whole. W is on a healthy disk that is no member, and the PG reports it unfound. Putting that disk back by an operator's edit serves it again.
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
			t.Logf("members %v", ms)
			others := make([]uint32, 0, 2)
			for _, m := range ms[1:] {
				others = append(others, m.Disk)
				r.lose(m.Disk)
			}
			w := bytes.Repeat([]byte{0x22}, core.BlockSize)
			id := c.write(pg0, ch, 0, w)
			c.runFor(5_000)
			r.publish()
			start := c.s.Time()
			rp, ok := r.answered(id, 2*DegradedRefillAfter)
			if !ok {
				t.Fatal("W unanswered")
			}
			if rp.Status != StatusOK {
				t.Skipf("W answered %+v: a refused write promises nothing", rp)
			}
			t.Logf("W answered OK after %d s: members %v, real holders %v", (c.s.Time()-start)/1_000_000, r.members(), r.holders(ch, rp.Version))
			if h := r.holders(ch, rp.Version); len(h) != 1 || h[0] != first {
				t.Fatalf("holders %v", h)
			}

			r.lose(first)
			r.tick(shape.out)
			_, e := c.table.entry(pg0)
			t.Logf("after the second refill: members %v interval %d activated %d past %d", r.members(), e.Interval, e.Activated, len(e.Past))
			for _, d := range append(others, first) {
				r.back(d)
			}
			r.tick(5_000_000)
			t.Logf("every old disk back: members %v", r.members())
			grp, got := r.read(ch)
			t.Logf("W read %+v", grp)
			if slices.ContainsFunc(r.members(), func(m Member) bool { return m.Disk == first }) {
				t.Fatalf("disk %d is a member again: %v", first, r.members())
			}
			defer func() {
				if grp.Status != StatusOK || !bytes.Equal(got, w) {
					t.Errorf("W read back %+v with its only copy whole on disk %d, a disk the refill removed", grp, first)
				}
			}()
			ids := []uint32{first}
			for _, m := range r.members() {
				if len(ids) < shape.replicas {
					ids = append(ids, m.Disk)
				}
			}
			r.a.edits = append(r.a.edits, Edit{PG: pg0, Disks: ids})
			r.tick(5_000_000)
			after, data := r.read(ch)
			t.Logf("after an operator's edit puts disk %d back: members %v, W read %+v, same bytes %v", first, r.members(), after, bytes.Equal(data, w))
		})
	}
}

// No write in flight: X was acknowledged on both disks of a rep2 pool. One disk is out long enough to be refilled, the other goes before the refill member has copied X, and is refilled in turn. Both old disks come back whole, neither a member, and X, on both, is reported unfound.
func TestARefillKeepsTheLastSourceOfAPG(t *testing.T) {
	r := newSoleRun(t, 2, 2, 5)
	c := r.c
	ch := chunkIn(pg0, 1, 0)
	x := bytes.Repeat([]byte{0x11}, core.BlockSize)
	c.until(c.write(pg0, ch, 0, x))
	c.settle()
	ms := r.members()
	d1, d2 := ms[0].Disk, ms[1].Disk
	for n := range c.reps {
		c.reps[n].plant.noRecovery = true
	}
	r.lose(d2)
	r.tick(DegradedRefillAfter + 2_000_000)
	t.Logf("after the first refill: members %v", r.members())
	r.lose(d1)
	r.tick(DegradedRefillAfter + 2_000_000)
	t.Logf("after the second refill: members %v", r.members())
	r.back(d1)
	r.back(d2)
	for n := range c.reps {
		c.reps[n].plant.noRecovery = false
	}
	r.tick(5_000_000)
	t.Logf("both old disks back: members %v", r.members())
	rp, got := r.read(ch)
	t.Logf("X read %+v", rp)
	if rp.Status != StatusOK || !bytes.Equal(got, x) {
		t.Fatalf("X read back %+v with two whole copies on disks %d and %d, neither a member", rp, d1, d2)
	}
}

// soleDiag counts, over a seeded run, acknowledgments on fewer than min_live disks and activations with no clean holder of an acknowledged version. It breaks nothing.
type soleDiag struct {
	c       *rcluster
	chunkOf func(core.RequestID) (core.ChunkID, bool)
	acked   map[core.ChunkID]uint64
	// member is every version some member acknowledged holding, of either chunk: versions of the two chunks can collide, which only hides a sole copy.
	member  map[uint64]bool
	acts    int
	soleAck int
	noCopy  int
	noBytes int
}

var soleWatch *soleDiag

func (d *soleDiag) watch(c *rcluster, chunkOf func(core.RequestID) (core.ChunkID, bool)) {
	d.c, d.chunkOf, d.acked, d.member = c, chunkOf, map[core.ChunkID]uint64{}, map[uint64]bool{}
	prev := c.onPass
	c.onPass = func(n int, acts []core.Action) {
		if prev != nil {
			prev(n, acts)
		}
		d.pass(acts)
	}
}

func (d *soleDiag) whole(ch core.ChunkID, v uint64, clean bool) int {
	c, n := d.c, 0
	_, e := c.table.entry(pg0)
	for _, m := range e.Members {
		i := c.node(m.Disk)
		if i < 0 || m.Mark != MarkUp || !c.s.Node(uint32(i)).Up() || c.reps[i] == nil || !c.reps[i].Store().Serving() {
			continue
		}
		x, _ := c.reps[i].Store().Index().Chunk(testPool, ch)
		if x.Whole >= v && (!clean || x.ListedIn == 0) {
			n++
		}
	}
	return n
}

func (d *soleDiag) pass(acts []core.Action) {
	c := d.c
	for i := range acts {
		a := &acts[i]
		if a.Kind != core.ActNetSend || a.Msg.Len < headSize {
			continue
		}
		k := Kind(a.Msg.Body[1])
		if k != KindReply && k != KindAck {
			continue
		}
		var rp Response
		if DecodeResponse(a.Msg.Inline(), 0, &rp) != nil || rp.Status != StatusOK || rp.Version == 0 {
			continue
		}
		if k == KindAck {
			d.member[rp.Version] = true
			continue
		}
		if ch, ok := d.chunkOf(rp.ID); ok {
			d.acked[ch] = max(d.acked[ch], rp.Version)
			if !d.member[rp.Version] {
				d.soleAck++
			}
		}
	}
	if len(c.activations) > d.acts {
		d.acts = len(c.activations)
		for ch, v := range d.acked {
			if d.whole(ch, v, true) == 0 {
				d.noCopy++
			}
			if d.whole(ch, v, false) == 0 {
				d.noBytes++
			}
		}
	}
}

// The counts of the runs' unplanted seeds; set SOLE_SEEDS to run them.
func TestDiagSoleCopySeeds(t *testing.T) {
	n, _ := strconv.Atoi(os.Getenv("SOLE_SEEDS"))
	if n == 0 {
		t.Skip("set SOLE_SEEDS")
	}
	for _, run := range []struct {
		name string
		f    func(seed uint64) error
	}{
		{"primaries", func(seed uint64) error { return runPrimaries(t, seed, nil) }},
		{"faults", func(seed uint64) error { return runFaults(t, seed, 2, 24, nil) }},
	} {
		var sole, none, bytesGone, soleTotal, noneTotal int
		for seed := range uint64(n) {
			d := &soleDiag{}
			soleWatch = d
			if err := run.f(seed); err != nil {
				t.Fatalf("%s seed %d: %v", run.name, seed, err)
			}
			soleWatch = nil
			if d.soleAck > 0 {
				sole++
				soleTotal += d.soleAck
			}
			if d.noCopy > 0 {
				none++
				noneTotal += d.noCopy
			}
			if d.noBytes > 0 {
				bytesGone++
				t.Logf("%s seed %d: no up member holds an acknowledged version", run.name, seed)
			}
			if d.soleAck > 0 && sole <= 3 {
				t.Logf("%s seed %d: %d acknowledgments below min_live copies, %d activations with no clean holder", run.name, seed, d.soleAck, d.noCopy)
			}
		}
		t.Logf("%s over %d seeds: %d seeds acknowledged on fewer than 2 disks (%d writes); %d seeds activated with no up member clean for an acknowledged version (%d activations); %d seeds with no up member holding its bytes at all", run.name, n, sole, soleTotal, none, noneTotal, bytesGone)
	}
}
