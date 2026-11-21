package chunkstore

import (
	"bytes"
	"fmt"
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
	acked    map[core.ChunkID]uint64
	// w, once watch starts it, is what the replicas' reports say of the PG, and cond the pool's conditions it gives each second.
	w    *Watcher
	cond Conditions
	// lostErr is what the first give-up that broke invOnlyLost found.
	lostErr error
	// ivs is the members of each interval of PG 0 a table named.
	ivs map[uint32][]Member
}

// watch starts an authority on a node of its own that every replica heartbeats from each start and that hears their reports in w.
func (r *soleRun) watch() {
	c := r.c
	r.w = &Watcher{}
	var rp Report
	c.s.AddNode(sim.DefaultClock, nil, func(n *sim.Node) {
		c.auth = n.Start(sim.MachineFunc(func(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
			if ev.Kind != core.EvNetRecv {
				return out
			}
			frame, data := Frame(ctx.Pool, &ev.Msg)
			defer freeBuf(ctx, ev.Msg.Buf)
			if k, err := Peek(frame); err == nil && k == KindReport && DecodeReport(frame, data, &rp) == nil {
				r.w.Hear(rp.Leads)
			}
			return out
		}), nil)
	})
	c.reporting = true
	for k := range c.reps {
		c.call(k, func(ctx *core.Context, rep *Replica, out *ReplicaOutput) { rep.SetAuthority(ctx, c.auth, out) })
	}
}

func newSoleRun(t *testing.T, replicas, minLive, disks int) *soleRun {
	a := &authority{pool: Pool{Replicas: replicas, MinLive: minLive, FailureDomain: DomainNode, MaxPerDomain: 1, Locality: LocalitySpread, Overcommit: true, WriteLimit: 90, PGCount: 1},
		down: map[uint32]core.Tick{}, drain: map[uint32]bool{}}
	for n := range disks {
		if _, err := a.rec.Report(uint32(n), 0, 0x5eed+uint64(n), ""); err != nil {
			t.Fatal(err)
		}
	}
	var empty Table
	tbl, _ := Next(&empty, &Inputs{Pools: a.pools()})
	c := newSoleCluster(t, tbl, disks)
	r := &soleRun{c: c, a: a, isolated: map[uint32]bool{}, acked: map[core.ChunkID]uint64{}}
	r.noteIntervals()
	c.inv.Register(invStranded, func() error {
		_, e := c.table.entry(pg0)
		for ch, v := range r.acked {
			if err := c.stranded(e, ch, v); err != nil {
				return err
			}
		}
		return nil
	})
	c.inv.Register(invOnlyLost, func() error { return r.lostErr })
	return r
}

// stranded reports a disk that can serve version v of ch, holding it and listing nothing of it, in a PG none of whose members in e can, up or down, nor carries, running, an image of ch at v or above; a crashed member says nothing.
func (c *rcluster) stranded(e *PGEntry, ch core.ChunkID, v uint64) error {
	for n := range c.reps {
		if !slices.ContainsFunc(e.Members, func(m Member) bool { return m.Disk == c.ids[n] }) || c.reps[n].Store().Index() == nil {
			continue
		}
		if pg, ok := c.reps[n].pgOf(ch); ok {
			if iv, ok := c.reps[n].imaged(pg, ch); ok && iv >= v {
				return nil
			}
		}
	}
	held := uint32(0)
	for n := range c.reps {
		member := slices.ContainsFunc(e.Members, func(m Member) bool { return m.Disk == c.ids[n] })
		idx := c.reps[n].Store().Index()
		if c.declared[c.ids[n]] {
			continue
		}
		if idx == nil {
			if member {
				return nil
			}
			continue
		}
		if x, _ := idx.Chunk(testPool, ch); x.ListedIn == 0 && x.Whole >= v {
			if member {
				return nil
			}
			held = c.ids[n]
		}
	}
	if held != 0 {
		return fmt.Errorf("%#x of %v is on disk %d and on no member of %v", v, ch, held, e.Members)
	}
	return nil
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
	r.noteIntervals()
	for n := range c.reps {
		if !r.isolated[uint32(n)+1] {
			c.call(n, func(ctx *core.Context, rep *Replica, out *ReplicaOutput) { rep.SetTable(ctx, next, out) })
		}
	}
}

func (r *soleRun) noteIntervals() {
	if r.ivs == nil {
		r.ivs = map[uint32][]Member{}
	}
	if e := r.c.table.Entry(pg0); r.ivs[e.Interval] == nil {
		r.ivs[e.Interval] = slices.Clone(e.Members)
	}
}

// removed reports whether disk d left PG 0 as the model's Removed has it: leaving in the last interval naming it, which
// is not the current one, and every member of the interval after it declared lost.
func (r *soleRun) removed(d uint32) bool {
	var last uint32
	for iv, ms := range r.ivs {
		if iv > last && slices.ContainsFunc(ms, func(m Member) bool { return m.Disk == d }) {
			last = iv
		}
	}
	next, ok := r.ivs[last+1]
	return last != 0 && ok && slices.ContainsFunc(r.ivs[last], func(m Member) bool { return m.Disk == d && m.Leaving }) &&
		!slices.ContainsFunc(next, func(m Member) bool { return !r.c.declared[m.Disk] })
}

// lose cuts disk off from every node, the client's and a watch's authority's included, as a power cut would, and marks it down; its disk stays as it was.
func (r *soleRun) lose(disk uint32) {
	r.isolated[disk] = true
	r.a.down[disk] = r.c.s.Time()
	r.a.rec.Down(disk, r.c.s.Time())
	n := uint32(r.c.node(disk))
	for o := range uint32(len(r.c.reps)) + 2 {
		if o != n {
			r.c.s.Partition(n, o, false)
		}
	}
}

// back reconnects disk, whole as it was cut off, marks it up and hands it the newest table.
func (r *soleRun) back(disk uint32) {
	delete(r.isolated, disk)
	delete(r.a.down, disk)
	r.a.rec.Up(disk)
	n := uint32(r.c.node(disk))
	for o := range uint32(len(r.c.reps)) + 2 {
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
		if r.w != nil {
			r.w.Watch(r.c.s.Time(), &r.c.table.Pools[0], &r.cond)
		}
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
func (r *soleRun) read(ch core.ChunkID) (Response, []byte) { return r.readLen(ch, 1) }

func (r *soleRun) readLen(ch core.ChunkID, n uint32) (Response, []byte) {
	c := r.c
	for range 50 {
		id := c.read(pg0, ch, 0, n, c.primary(pg0))
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

// A rep3 pool with min_live 2, no refill: disk 2 is down, W reaches disk 1 and is on its way to disk 3 when disk 3 goes too and disk 2 comes back. The interval of disks 1 and 2 keeps W, since disk 2 was not live in W's interval, but disk 1 is its only copy, so the client is refused. Its resend lands on both, recovery lets disk 2 serve it, disk 1 is then lost for good, and W reads from the others.
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

// The model's counterexample on real placement: every other member is cut off while W is on its way, so W is on the primary alone; the PG below min_live waits degraded_refill_after, and a refill brings it back to min_live with a member that lists W. Peering keeps W but its client is refused. The resend lands on both and recovery lets the refill serve it, so when the primary is cut off, W reads from the member left: on rep2 a second refill removes the primary on the recovered report, on rep3 the third member, down, holds that report back and the primary stays a member.
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
			if shape.replicas == 2 && slices.ContainsFunc(r.members(), func(m Member) bool { return m.Disk == first }) {
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

// The limit of keeping a refused version: its one holder dies, so its chunk is unfound though X, below it, was acknowledged and is on the others. The chunk reads again once the disk returns; were it lost for good, X would stay unfound.
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

// oneCleanCopy leaves chunk ch with one clean copy, on disk 1: W1 is on disks 1 and 2, disk 3 returns listing the chunk and disk 2 goes down. noRecovery, standing in for recovery that is slow, keeps disk 3 listing it.
func oneCleanCopy(t *testing.T, set func(*replicaPlants)) (r *soleRun, ch core.ChunkID, d1, d2, d3 uint32) {
	r = newSoleRun(t, 3, 2, 3)
	c := r.c
	for n := range c.reps {
		set(&c.reps[n].plant)
	}
	ch = chunkIn(pg0, 1, 0)
	c.until(c.write(pg0, ch, 0, bytes.Repeat([]byte{0x11}, core.BlockSize)))
	c.settle()
	ms := r.members()
	d1, d2, d3 = ms[0].Disk, ms[1].Disk, ms[2].Disk
	r.lose(d3)
	r.publish()
	c.untilActive()
	c.settle()
	c.until(c.write(pg0, ch, 0, bytes.Repeat([]byte{0x33}, core.BlockSize)))
	c.settle()

	for n := range c.reps {
		c.reps[n].plant.noRecovery = true
	}
	r.lose(d2)
	r.back(d3)
	r.publish()
	c.untilActive()
	c.settle()
	return r, ch, d1, d2, d3
}

// W2 to a chunk whose one clean copy is disk 1 waits until recovery reaches disk 3, so its OK rests on two clean copies and losing disk 1 leaves the chunk readable.
func TestAWriteToAChunkWithOneCleanCopyWaitsForRecovery(t *testing.T) {
	r, ch, d1, d2, d3 := oneCleanCopy(t, func(*replicaPlants) {})
	c := r.c
	w2 := bytes.Repeat([]byte{0x44}, core.BlockSize)
	id := c.write(pg0, ch, 0, w2)
	if rp, ok := r.answered(id, 20_000_000); ok {
		t.Fatalf("W2 answered %+v with one clean copy", rp)
	}
	for n := range c.reps {
		c.reps[n].plant.noRecovery = false
	}
	rp, ok := r.answered(id, 100_000_000)
	if !ok || rp.Status != StatusOK {
		t.Fatalf("W2 answered %+v, %v", rp, ok)
	}
	if h := r.holders(ch, rp.Version); !slices.Contains(h, d1) || !slices.Contains(h, d3) {
		t.Fatalf("W2, OK, has clean copies on %v", h)
	}

	r.lose(d1)
	r.back(d2)
	r.tick(10_000_000)
	if grp, got := r.read(ch); grp.Status != StatusOK || !bytes.Equal(got, w2) {
		t.Fatalf("W2 read back %+v with disk %d gone", grp, d1)
	}
}

// With noDegradedHold, W2 is answered OK on disk 1 and on disk 3, whose missing list names the chunk: no clean copy but disk 1's, so losing disk 1 leaves W2 unfound though disk 3 holds its bytes, until disk 1 returns.
func TestNoDegradedHoldLeavesAnOKUnfoundWithItsOneCleanCopy(t *testing.T) {
	r, ch, d1, d2, d3 := oneCleanCopy(t, func(p *replicaPlants) { p.noDegradedHold = true })
	c := r.c
	w2 := bytes.Repeat([]byte{0x44}, core.BlockSize)
	rp, ok := r.answered(c.write(pg0, ch, 0, w2), 100_000_000)
	if !ok || rp.Status != StatusOK {
		t.Fatalf("W2 answered %+v, %v", rp, ok)
	}
	if h := r.holders(ch, rp.Version); len(h) != 1 || h[0] != d1 {
		t.Fatalf("W2, OK, has clean copies on %v", h)
	}
	if x, _ := c.reps[c.node(d3)].Store().Index().Chunk(testPool, ch); x.Whole != rp.Version || x.ListedIn == 0 {
		t.Fatalf("disk %d, which logged W2 %#x, holds %+v", d3, rp.Version, x)
	}

	r.lose(d1)
	r.back(d2)
	r.tick(10_000_000)
	if grp, _ := r.read(ch); grp.Status != StatusUnfound {
		t.Fatalf("chunk read %+v with W2's clean copy gone", grp)
	}
	r.back(d1)
	r.tick(5_000_000)
	if grp, got := r.read(ch); grp.Status != StatusOK || !bytes.Equal(got, w2) {
		t.Fatalf("W2 read back %+v with disk %d back", grp, d1)
	}
}

// listerPrimary has the primary of a rep3 PG at min_live 1 list chunk ch beside one clean member: Y lands on the primary and the second member, not on the third, and peering drops it, so the two that logged it list the chunk and the third holds X clean, which noRecovery keeps. It leaves the third cut off from the primary and returns what the second holds.
func listerPrimary(t *testing.T, set func(*replicaPlants)) (r *soleRun, ch core.ChunkID, a, b, d uint32, held Chunk) {
	r = newSoleRun(t, 3, 1, 3)
	c := r.c
	plantAll(c, set)
	ch = chunkIn(pg0, 1, 0)
	c.until(c.write(pg0, ch, 0, bytes.Repeat([]byte{0x11}, core.BlockSize)))
	c.settle()
	ms := r.members()
	a, b, d = ms[0].Disk, ms[1].Disk, ms[2].Disk
	plantAll(c, func(p *replicaPlants) { p.noRecovery = true })
	an, dn := uint32(c.node(a)), uint32(c.node(d))
	c.s.Partition(an, dn, true)
	y := c.write(pg0, ch, core.BlockSize, bytes.Repeat([]byte{0x22}, core.BlockSize))
	c.runFor(5_000)
	_, e := c.table.entry(pg0)
	r.a.asks = append(r.a.asks, PGInterval{PG: pg0, Interval: e.Interval})
	r.publish()
	c.s.Heal(an, dn)
	if rp, ok := r.answered(y, 20_000_000); !ok || rp.Status == StatusOK {
		t.Fatalf("Y answered %+v, %v", rp, ok)
	}
	r.tick(5_000_000)
	for _, m := range []uint32{a, b, d} {
		if got := c.listed(c.node(m), ch); (got.ListedIn != 0) != (m != d) {
			t.Fatalf("disk %d holds %+v", m, got)
		}
	}
	c.s.Partition(an, dn, true)
	return r, ch, a, b, d, c.listed(c.node(b), ch)
}

// W, a whole block, waits while the primary lists the chunk, though the third member is a clean copy and min_live is 1: the one copy W surely lands on is the primary's. Once recovery clears the primary's list, W is ordered and its OK rests on a clean primary.
func TestAWriteWaitsWhileThePrimaryListsTheChunk(t *testing.T) {
	r, ch, a, b, d, held := listerPrimary(t, noPlant)
	c := r.c
	w := c.write(pg0, ch, 0, bytes.Repeat([]byte{0x44}, core.BlockSize))
	if rp, ok := r.answered(w, 20_000_000); ok {
		t.Fatalf("W answered %+v with the primary listing the chunk", rp)
	}
	if got := c.listed(c.node(b), ch); got.Whole != held.Whole {
		t.Fatalf("W ordered with the primary listing the chunk: disk %d holds %+v, held %+v", b, got, held)
	}
	c.s.Heal(uint32(c.node(a)), uint32(c.node(d)))
	plantAll(c, func(p *replicaPlants) { p.noRecovery = false })
	rp, ok := r.answered(w, 100_000_000)
	if !ok || rp.Status != StatusOK {
		t.Fatalf("W answered %+v, %v", rp, ok)
	}
	if h := r.holders(ch, rp.Version); !slices.Contains(h, a) {
		t.Fatalf("W, OK, has clean copies on %v", h)
	}
}

// With listerOrders, W lands on the primary and the second member, which both list the chunk, and not on the third, its one clean copy. The third goes down, the interval of the other two keeps W, and once the third is back it lacks W and lists the chunk too: every disk is up and X, acknowledged, reads unfound.
func TestListerOrdersLeavesAChunkUnfoundWithEveryDiskUp(t *testing.T) {
	r, ch, _, b, d, held := listerPrimary(t, func(p *replicaPlants) { p.listerOrders = true })
	c := r.c
	c.write(pg0, ch, 0, bytes.Repeat([]byte{0x44}, core.BlockSize))
	c.runFor(5_000)
	if got := c.listed(c.node(b), ch); got.Whole == held.Whole {
		t.Fatalf("W not ordered: disk %d holds %+v", b, got)
	}
	r.lose(d)
	r.publish()
	c.untilActive()
	r.back(d)
	r.publish()
	r.tick(10_000_000)
	for _, m := range r.members() {
		if m.Mark != MarkUp || c.listed(c.node(m.Disk), ch).ListedIn == 0 {
			t.Fatalf("members %v, disk %d holds %+v", r.members(), m.Disk, c.listed(c.node(m.Disk), ch))
		}
	}
	if rp, _ := r.read(ch); rp.Status != StatusUnfound {
		t.Fatalf("chunk read %+v with every disk up", rp)
	}
}

// W is on disks 1 and 2 when disk 3 goes: the decision proves two copies, and the client hears OK at activation.
func TestAVersionOnMinLiveCopiesHearsOKAtActivation(t *testing.T) {
	r := newSoleRun(t, 3, 2, 3)
	c := r.c
	ch := chunkIn(pg0, 1, 0)
	ms := r.members()
	r.lose(ms[2].Disk)
	id := c.write(pg0, ch, 0, bytes.Repeat([]byte{0x22}, core.BlockSize))
	c.runFor(5_000)
	if rp, ok := c.cl.replies[id]; ok {
		t.Fatalf("W answered %+v before activation", rp)
	}
	r.publish()
	if rp, ok := r.answered(id, 100_000_000); !ok || rp.Status != StatusOK {
		t.Fatalf("W, on two disks, answered %+v, %v", rp, ok)
	}
}

// rep2 with the second member cut off and then in maintenance: W on the primary alone has two copies, as it would on a write in that interval.
func TestAMemberInMaintenanceCountsTowardAKeptVersionsCopies(t *testing.T) {
	r := newSoleRun(t, 2, 2, 2)
	c := r.c
	ch := chunkIn(pg0, 1, 0)
	second := r.members()[1].Disk
	r.lose(second)
	id := c.write(pg0, ch, 0, bytes.Repeat([]byte{0x22}, core.BlockSize))
	c.runFor(5_000)
	if rp, ok := c.cl.replies[id]; ok {
		t.Fatalf("W answered %+v before activation", rp)
	}
	c.publish(c.next(func(t *Table) { mark(t, pg0, second, MarkMaint) }), true)
	c.settle()
	if rp := c.reply(id); rp.Status != StatusOK {
		t.Fatalf("W answered %+v", rp)
	}
}

// rep1: W has no member to wait for, so it commits on its own log, and an interval that ends under it leaves it its OK.
func TestAOneDiskPoolsVersionHearsOKAcrossItsInterval(t *testing.T) {
	r := newSoleRun(t, 1, 1, 1)
	c := r.c
	ch := chunkIn(pg0, 1, 0)
	id := c.write(pg0, ch, 0, bytes.Repeat([]byte{0x22}, core.BlockSize))
	for len(c.reps[0].vers) == 0 && c.step() {
	}
	c.publish(c.next(func(t *Table) { _, e := t.entry(pg0); setInterval(t, pg0, e.Interval+1) }), true)
	c.settle()
	if rp := c.reply(id); rp.Status != StatusOK {
		t.Fatalf("W answered %+v", rp)
	}
}

// At min_live 1 too, a primary that lists the chunk is no copy: W, sent as the other two go, waits while the primary lists the chunk, and with no up member a source the next interval answers it unfound. With listerOrders, W is ordered on the primary alone, which counts none at activation, so its client is refused, and the resend answers unfound.
func TestAPrimaryThatListsTheChunkIsNoCopyAtMinLiveOne(t *testing.T) {
	for _, plant := range []bool{false, true} {
		r := newSoleRun(t, 3, 1, 3)
		plantAll(r.c, func(p *replicaPlants) { p.listerOrders = plant })
		first := StatusUnfound
		if plant {
			first = StatusRefused
		}
		primaryListsAtMinLiveOne(t, r, first)
	}
}

func primaryListsAtMinLiveOne(t *testing.T, r *soleRun, first Status) {
	c := r.c
	ch := chunkIn(pg0, 1, 0)
	c.until(c.write(pg0, ch, 0, bytes.Repeat([]byte{0x11}, core.BlockSize)))
	c.settle()
	ms := r.members()
	r.lose(ms[0].Disk)
	r.publish()
	c.untilActive()
	c.settle()
	c.until(c.write(pg0, ch, 0, bytes.Repeat([]byte{0x33}, core.BlockSize)))
	c.settle()
	for n := range c.reps {
		c.reps[n].plant.noRecovery = true
	}
	r.back(ms[0].Disk)
	r.publish()
	c.untilActive()
	c.settle()
	r.lose(ms[1].Disk)
	r.lose(ms[2].Disk)
	w := bytes.Repeat([]byte{0x22}, core.BlockSize)
	id := c.write(pg0, ch, 0, w)
	c.runFor(5_000)
	r.publish()
	if rp, ok := r.answered(id, 100_000_000); !ok || rp.Status != first {
		t.Fatalf("W answered %+v, %v", rp, ok)
	}
	if rp, ok := r.answered(c.write(pg0, ch, 0, w), 100_000_000); !ok || rp.Status != StatusUnfound {
		t.Fatalf("W sent again answered %+v, %v", rp, ok)
	}
}

// Disk 3 acknowledges 1.2 and then refuses 1.1, so 1.2 commits behind a failed version. The next interval proves one copy, disk 1, below min_live, and 1.2 still hears OK: every member it was sent to took it.
func TestACommittedVersionBehindAFailedOneKeepsItsOK(t *testing.T) {
	r := newSoleRun(t, 3, 2, 4)
	c := r.c
	ch := chunkIn(pg0, 1, 0)
	ms := r.members()
	d1, d2, d3 := ms[0].Disk, ms[1].Disk, ms[2].Disk
	spare := uint32(1)
	for slices.ContainsFunc(ms, func(m Member) bool { return m.Disk == spare }) {
		spare++
	}
	n1, n2, n3 := c.node(d1), uint32(c.node(d2)), uint32(c.node(d3))
	c.s.Partition(n3, uint32(n1), true)
	w1, w2 := c.write(pg0, ch, 0, []byte{1}), c.write(pg0, ch, 1, []byte{2})
	c.runFor(5_000)
	from3 := func(rp Response) {
		ev := core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{Node: n3}}
		ev.Msg.Len = uint16(copy(ev.Msg.Body[:], AppendResponse(nil, &rp)))
		c.s.Node(uint32(n1)).Feed(c.loops[n1], ev)
	}
	from3(Response{Kind: KindAck, ID: 2, Version: MakeVersion(1, 2)})
	if !c.reps[n1].vers[1].commit {
		t.Fatal("1.2 did not commit")
	}
	from3(Response{Kind: KindAck, ID: 1, Status: StatusRefused, Epoch: 1, Interval: 1})
	c.settle()
	if _, ok := c.cl.replies[w2]; ok {
		t.Fatalf("1.2 answered behind 1.1: %+v", c.cl.replies[w2])
	}
	c.reps[n1].plant.noRecovery = true
	c.s.Partition(uint32(n1), n2, false)
	c.s.Partition(uint32(n1), n3, false)
	c.publish(c.next(members(Member{Disk: d1}, Member{Disk: spare}, Member{Disk: d3, Mark: MarkDown})), true)
	c.settle()
	if h := r.holders(ch, MakeVersion(1, 2)); len(h) != 1 || h[0] != d1 {
		t.Fatalf("1.2 is held by %v", h)
	}
	if rp := c.reply(w1); rp.Status != StatusRefused {
		t.Fatalf("1.1 answered %+v", rp)
	}
	if rp := c.reply(w2); rp.Status != StatusOK {
		t.Fatalf("1.2 answered %+v", rp)
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
		{"the primary lists it, alone", []Member{{Disk: 1}}, []peerReply{{disk: 1, list: listed}}, 0},
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
