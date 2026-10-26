package chunkstore

import (
	"fmt"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/clock"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

const invMarkedFaulty = "the detector marks down only a disk whose node was cut off, whose I/O stalled or slowed, or whose log failed"

// dcluster is a replica cluster whose stand-in authority runs the detector on a simulated node of its own: the replicas heartbeat it and report the members that keep them waiting over the simulated network, it probes their loops, and the authority publishes a table computed with Next whenever its marks change or a replica asks for an interval or reports an activation.
type dcluster struct {
	*rcluster
	a       *authority
	det     *Detector
	changes uint64
	// faulty is the disks the run made fail; downAt when each disk was first marked down.
	faulty map[uint32]bool
	downAt map[uint32]core.Tick
}

func newDCluster(t *testing.T, seed uint64, set sim.Settings) *dcluster {
	s := sim.New(seed, set)
	a := &authority{pool: Pool{Replicas: 3, MinLive: 2, FailureDomain: DomainNode, MaxPerDomain: 1, Locality: LocalitySpread, Overcommit: true, WriteLimit: 90, PGCount: 4}, down: map[uint32]core.Tick{}}
	for n := range 3 {
		if _, err := a.rec.Report(uint32(n), 0, 0x5eed+uint64(n)); err != nil {
			t.Fatal(err)
		}
	}
	var empty Table
	tbl, _ := Next(&empty, &Inputs{Pools: a.pools()})
	c := newRClusterOn(t, tbl, s)
	d := &dcluster{rcluster: c, a: a, det: c.detect(a), faulty: map[uint32]bool{}, downAt: map[uint32]core.Tick{}}
	c.inv.Register(invMarkedFaulty, func() error {
		for _, r := range a.rec.Disks() {
			if m, since := d.det.Mark(r.ID); m == MarkDown && !d.faulty[r.ID] {
				return fmt.Errorf("disk %d marked down at %d", r.ID, since)
			}
		}
		return nil
	})
	return d
}

// detect starts a detector on a node of its own as a's marks, which every replica heartbeats from each start.
func (c *rcluster) detect(a *authority) *Detector {
	var det *Detector
	node := c.s.AddNode(sim.DefaultClock, nil, func(n *sim.Node) {
		c.auth = n.Start(sim.MachineFunc(func(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
			return det.OnEvent(ctx, ev, out)
		}), nil)
	})
	det = NewDetector(&a.rec, node.Context().Now)
	a.det = det
	det.View(&c.table, a.pools())
	c.reporting = true
	for k := range c.reps {
		c.call(k, func(ctx *core.Context, r *Replica, out *ReplicaOutput) { r.SetAuthority(ctx, c.auth, out) })
	}
	return det
}

// tend publishes a table when the detector's marks changed or a replica asked or activated, noting when each disk was first marked down.
func (d *dcluster) tend() {
	c := d.rcluster
	if d.det.Changes() == d.changes && len(c.asks) == d.a.seen && len(c.activations) == d.a.acts {
		return
	}
	d.changes = d.det.Changes()
	for _, r := range d.a.rec.Disks() {
		if m, _ := d.det.Mark(r.ID); m == MarkDown && d.downAt[r.ID] == 0 {
			d.downAt[r.ID] = c.s.Time()
		}
	}
	next, _ := Next(&c.table, d.a.inputs(c))
	if len(MakeDelta(&c.table, &next).Entries) > 0 {
		c.table = next
		for n := range c.reps {
			if c.s.Node(uint32(n)).Up() {
				c.call(n, func(ctx *core.Context, r *Replica, out *ReplicaOutput) { r.SetTable(ctx, next, out) })
			}
		}
	}
	d.det.View(&c.table, d.a.pools())
}

// run steps the cluster for dur of simulated time, tending the table.
func (d *dcluster) run(dur core.Tick) {
	end := d.s.Time() + dur
	for d.s.Time() < end {
		d.tend()
		if next := d.s.Next(); next == 0 || next > end {
			d.s.Advance(end)
			return
		}
		d.step()
	}
}

// dclient writes a block at a time to the chunks of every PG, with up to pipe writes unanswered, abandoning one after abandonAfter.
type dclient struct {
	c    *rcluster
	rng  *core.Rand
	pipe int
	at   map[core.RequestID]core.Tick
	ok   int
}

func (dc *dclient) Ready() bool {
	out := 0
	for id, at := range dc.at {
		if rp, done := dc.c.cl.replies[id]; done {
			if rp.Status == StatusOK {
				dc.ok++
			}
			delete(dc.at, id)
		} else if dc.c.s.Time()-at < abandonAfter {
			out++
		}
	}
	return out < dc.pipe
}

func (dc *dclient) Next(ctx *core.Context, acts []core.Action) []core.Action {
	c := dc.c
	pg := core.PGID{Pool: testPool, Index: uint32(dc.rng.Below(4))}
	ch := chunkIn(pg, 4, uint32(dc.rng.Below(4)))
	_, e := c.table.entry(pg)
	lead, ok := e.primary()
	if !ok {
		return acts
	}
	c.nextID++
	id := c.nextID
	dc.at[id] = c.s.Time()
	u := Update{Kind: KindWrite, ID: id, PG: c.pgFields(pg), Session: 1, Incarnation: 1, Chunk: ch, Off: uint32(dc.rng.Below(8)) * core.BlockSize, Len: core.BlockSize}
	a := core.Action{Kind: core.ActNetSend, Peer: core.NodeID{Node: uint32(c.node(lead))}}
	a.Msg.Len = uint16(copy(a.Msg.Body[:], AppendUpdate(nil, &u)))
	buf, err := ctx.Pool.AllocZeroed(core.BlockSize)
	if err != nil {
		panic(err)
	}
	ctx.Pool.Bytes(buf)[0] = byte(id)
	a.Msg.Buf = buf
	c.cl.sent[id] = buf
	return append(acts, a)
}

type diskFault uint8

const (
	faultCut diskFault = iota
	faultHang
	faultLog
)

func (f diskFault) String() string {
	return [...]string{"node cut off", "hang", "failed log"}[f]
}

// noisy is the settings of a run with slewed and drifting clocks and jittered delivery.
func noisy() sim.Settings {
	set := sim.DefaultSettings()
	set.ClockNum, set.ClockDen, set.NetJitter = 1, 200, 20_000
	return set
}

// runDetection loads every PG with writes, then fails disk with fault and runs on: the detector marks it down within the fault's bound, a quarter wider either way with noise, which slews clocks by up to 22.6%, and only once, marks no other disk, and the PGs take writes again. With noise, clocks slew and drift, delivery is jittered, and the detector's own node stops for up to 4.5 s, counted by its clocks, before the fault.
func runDetection(t *testing.T, seed uint64, fault diskFault, disk uint32, noise bool) {
	set := sim.DefaultSettings()
	if noise {
		set = noisy()
	}
	d := newDCluster(t, seed, set)
	cl := &dclient{c: d.rcluster, rng: core.NewRand(seed ^ 0xdec), pipe: 6, at: map[core.RequestID]core.Tick{}}
	d.s.Node(d.client.Node).AddActor(d.client, cl)
	if noise {
		rng := core.NewRand(seed ^ 0x9a05e)
		d.run(sec + core.Tick(rng.Below(uint64(sec))))
		stop := core.Tick(rng.Below(4_500_000))
		d.s.Pause(uint32(len(d.reps)+1), clock.PauseCounted, stop, false)
		d.run(stop)
	}
	d.run(3 * sec)
	if cl.ok == 0 || !slices.ContainsFunc(d.det.disks, func(w watched) bool { return w.lat > 0 }) {
		t.Fatalf("seed %d: before the fault, %d writes completed and no primary reported a member's latency", seed, cl.ok)
	}
	n := uint32(d.node(disk))
	d.faulty[disk] = true
	from := d.s.Time()
	var earliest, latest core.Tick
	switch fault {
	case faultCut:
		for k := range uint32(5) {
			if k != n {
				d.s.Partition(n, k, false)
			}
		}
		earliest, latest = MemberTimeoutAfter, NodeDownAfter+100_000
	case faultHang:
		d.s.Node(n).Disk(0).Hang(true)
		earliest, latest = MemberTimeoutAfter, 2*MemberTimeoutAfter+500_000
	case faultLog:
		d.s.Node(n).Disk(0).Inject(sim.Fault{Effect: sim.FailIO, When: sim.Once(sim.OnKind(core.ActDiskWrite))})
		earliest, latest = 0, RetryAfter+500_000
	}
	if noise {
		earliest, latest = earliest*3/4, latest*5/4
	}
	d.run(latest + sec)
	at := d.downAt[disk]
	if at == 0 || at-from < earliest || at-from > latest {
		t.Fatalf("seed %d: %v of disk %d at %d: marked down at %d, want within [%d, %d] of the fault", seed, fault, disk, from, at, earliest, latest)
	}
	if d.det.Changes() != 1 {
		t.Fatalf("seed %d: %d marks changed, want the one of disk %d", seed, d.det.Changes(), disk)
	}
	before := cl.ok
	d.run(5 * sec)
	if cl.ok == before {
		t.Fatalf("seed %d: no write completed after disk %d was marked down", seed, disk)
	}
}

// Each seed fails one disk of a loaded rep3 cluster: its node is cut off from every other, its I/O hangs or its log fails, on a clean network and clocks or, every other nine seeds, on noisy ones with a stop of the detector's node. The detector marks it down within the bound of its fault and no other disk, and the PGs take writes again. Fail-slow is judged on the latencies primaries report (TestAFailSlowDiskIsMarkedDown): the simulator advances its one clock to each completion it takes, so a slow disk delays the other disks' completions in flight as much as its own.
func TestTheDetectorMarksAFailedDiskDown(t *testing.T) {
	seeds.Run(t, 0, 9, func(seed uint64) {
		runDetection(t, seed, diskFault(seed%3), uint32(1+seed/3%3), seed/9%2 == 1)
	})
}

// A primary waiting on a member whose disk hangs reports it and the member's loop answers the probe from memory; the member is marked down within 2 s of the write, by the probe or, when the hung write was not yet member_timeout old as the probe arrived, by the member's next heartbeat.
func TestAHungMemberIsMarkedDownOnItsPrimarysReport(t *testing.T) {
	d := newDCluster(t, 3, sim.DefaultSettings())
	d.run(3 * sec)
	_, e := d.table.entry(pg0)
	member := e.Members[1].Disk
	d.faulty[member] = true
	d.s.Node(uint32(d.node(member))).Disk(0).Hang(true)
	from := d.s.Time()
	id := d.write(pg0, chunkIn(pg0, 4, 0), 0, fill(core.BlockSize, 7))
	var probed bool
	d.onPass = func(n int, acts []core.Action) {
		for _, a := range acts {
			if k, _ := Peek(a.Msg.Inline()); a.Kind == core.ActNetSend && k == KindProbeReply && n == d.node(member) {
				probed = true
			}
		}
	}
	d.run(2*MemberTimeoutAfter + 500_000)
	if !probed || d.downAt[member] == 0 || d.downAt[member]-from > 2*MemberTimeoutAfter+100_000 {
		t.Fatalf("hung member %d: probed %v, marked down %d after the write", member, probed, d.downAt[member]-from)
	}
	d.run(sec)
	if _, ok := d.cl.replies[id]; !ok {
		t.Fatal("the write waiting on the hung member got no reply")
	}
	if d.reps[d.primary(pg0)].disk(member).lat.n > 0 {
		t.Fatal("a member that never acknowledged has a latency")
	}
}

// Once warm, a replicated write allocates nothing in any replica's events with reporting on: the acknowledgment latencies, the waits watched and the heartbeats included.
func TestReportingAllocatesNothingPerWrite(t *testing.T) {
	d := newDCluster(t, 5, sim.DefaultSettings())
	ch := chunkIn(pg0, 4, 0)
	write := func(i int) {
		w := d.write(pg0, ch, uint32(i%8)*core.BlockSize, fill(core.BlockSize, byte(i)))
		d.until(w)
		if d.reply(w).Status != StatusOK {
			t.Fatal(d.reply(w))
		}
	}
	for i := range 200 {
		write(i)
	}
	d.run(12 * sec)
	for i := range 200 {
		write(i)
	}
	d.counting = true
	for i := range 100 {
		write(i)
	}
	d.run(3 * sec)
	if d.mallocs != 0 {
		t.Fatalf("%d heap allocations over 100 writes' events and 3 heartbeats", d.mallocs)
	}
}

// A node cut off for 2.5 s while writes wait on it is marked down when its probe goes unanswered, as silent, and rejoins at its first heartbeat after the cut heals.
func TestADiskCutOffBrieflyRejoinsAfterTheHeal(t *testing.T) {
	seeds.Run(t, 0, 6, func(seed uint64) {
		d := newDCluster(t, seed, sim.DefaultSettings())
		cl := &dclient{c: d.rcluster, rng: core.NewRand(seed ^ 0xdec), pipe: 6, at: map[core.RequestID]core.Tick{}}
		d.s.Node(d.client.Node).AddActor(d.client, cl)
		d.run(3 * sec)
		disk := uint32(1 + seed%3)
		n := uint32(d.node(disk))
		d.faulty[disk] = true
		for k := range uint32(5) {
			if k != n {
				d.s.Partition(n, k, false)
			}
		}
		d.run(2_500_000)
		for k := range uint32(5) {
			if k != n {
				d.s.Heal(n, k)
			}
		}
		d.run(2 * sec)
		if m, since := d.det.Mark(disk); m == MarkDown {
			t.Fatalf("seed %d: disk %d cut off 2.5 s, still down 2 s after the heal, since %d", seed, disk, since)
		}
	})
}

// A primary cut off from one member, which still answers the detector's probes, reports it once per member_timeout for as long as its write waits, not once.
func TestAPrimaryReportsAWaitEveryMemberTimeout(t *testing.T) {
	d := newDCluster(t, 3, sim.DefaultSettings())
	d.run(3 * sec)
	_, e := d.table.entry(pg0)
	p, m := uint32(d.node(e.Members[0].Disk)), uint32(d.node(e.Members[1].Disk))
	reports := 0
	d.onPass = func(n int, acts []core.Action) {
		for _, a := range acts {
			if k, _ := Peek(a.Msg.Inline()); a.Kind == core.ActNetSend && k == KindMemberTimeout && uint32(n) == p {
				reports++
			}
		}
	}
	d.s.Partition(p, m, false)
	d.write(pg0, chunkIn(pg0, 4, 0), 0, fill(core.BlockSize, 7))
	d.run(3*MemberTimeoutAfter + 500_000)
	if reports != 3 {
		t.Fatalf("%d member_timeout reports over 3.5 s of waiting", reports)
	}
}

// A version sent again after its member's connection broke gives no acknowledgment latency: the wait for the reconnect is not the disk's.
func TestAResentVersionGivesNoLatency(t *testing.T) {
	d := newDCluster(t, 3, sim.DefaultSettings())
	d.run(3 * sec)
	_, e := d.table.entry(pg0)
	member := e.Members[1].Disk
	p, m := uint32(d.node(e.Members[0].Disk)), uint32(d.node(member))
	d.s.Partition(p, m, false)
	id := d.write(pg0, chunkIn(pg0, 4, 0), 0, fill(core.BlockSize, 7))
	d.run(500_000)
	d.s.Heal(p, m)
	d.run(2 * sec)
	if d.reply(id).Status != StatusOK {
		t.Fatal(d.reply(id))
	}
	if w := d.det.disks[member-1]; w.latAt != 0 {
		t.Fatalf("disk %d reported a latency of %d at %d", member, w.lat, w.latAt)
	}
}
