package chunkstore

import (
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

const sec = core.Tick(1_000_000)

// dharness feeds a Detector events on simulated time and computes tables from its marks with Next, as the stand-in authority does.
type dharness struct {
	t       *testing.T
	rec     DiskRecord
	d       *Detector
	ctx     core.Context
	pool    Pool
	table   Table
	timerAt core.Tick
	probes  []Probe
}

// newDetector records disks disks, disk i on node i-1, and starts a detector at start over a pool of pool.
func newDetector(t *testing.T, disks int, pool Pool, start core.Tick) *dharness {
	h := &dharness{t: t, pool: pool, ctx: core.Context{Now: start, Raw: start, Pool: sim.NewPool(1<<20, 64)}}
	for n := range disks {
		if _, err := h.rec.Report(uint32(n), 0, logOf(uint32(n+1))); err != nil {
			t.Fatal(err)
		}
	}
	h.d = NewDetector(&h.rec, start)
	var empty Table
	h.table, _ = Next(&empty, &Inputs{Now: start, Pools: h.pools()})
	h.d.View(&h.table, h.pools())
	return h
}

func logOf(disk uint32) uint64 { return 0x10 + uint64(disk) }

func (h *dharness) pools() []PoolInputs {
	var disks []Disk
	for _, r := range h.rec.Disks() {
		dk := Disk{ID: r.ID, Node: r.Node, Blocks: tb(1)}
		if h.d != nil {
			dk.Mark, dk.DownSince = h.d.Mark(r.ID)
		}
		disks = append(disks, dk)
	}
	return []PoolInputs{{ID: testPool, Pool: h.pool, Disks: disks}}
}

// publish computes the next table from the detector's marks, as the authority does when they change.
func (h *dharness) publish() Table {
	prev := h.table
	h.table, _ = Next(&prev, &Inputs{Now: h.ctx.Now, Pools: h.pools()})
	h.d.View(&h.table, h.pools())
	return prev
}

// at moves time to t, firing DetectTimer on the way.
func (h *dharness) at(t core.Tick) {
	for h.timerAt != 0 && h.timerAt <= t {
		h.ctx.Now, h.timerAt = h.timerAt, 0
		h.feed(&core.Event{Kind: core.EvTimer, Timer: DetectTimer})
	}
	h.ctx.Now = max(h.ctx.Now, t)
}

func (h *dharness) feed(ev *core.Event) {
	for _, a := range h.d.OnEvent(&h.ctx, ev, nil) {
		switch a.Kind {
		case core.ActSetTimer:
			h.timerAt = a.At
		case core.ActNetSend:
			var p Probe
			if err := DecodeProbe(a.Msg.Inline(), 0, &p); err != nil {
				h.t.Fatal(err)
			}
			h.probes = append(h.probes, p)
		}
	}
}

func (h *dharness) send(frame []byte, from uint32) {
	ev := core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{Node: from}}
	if err := Attach(h.ctx.Pool, &ev.Msg, frame, core.NoBuf); err != nil {
		h.t.Fatal(err)
	}
	h.feed(&ev)
}

// beat is disk's heartbeat at t with its oldest unfinished I/O's age and whether its log failed, carrying lats as a primary.
func (h *dharness) beat(t core.Tick, disk uint32, oldest core.Tick, failed bool, lats ...Latency) {
	h.at(t)
	h.send(AppendReport(nil, &Report{ID: 1, Disks: []DiskState{{Log: logOf(disk), Oldest: oldest, Failed: failed}}, Latencies: lats}), disk-1)
}

// beats is a normal heartbeat of every disk but those in skip each second from t to end, both included.
func (h *dharness) beats(t, end core.Tick, skip ...uint32) {
	for ; t <= end; t += sec {
		for _, r := range h.rec.Disks() {
			if !slices.Contains(skip, r.ID) {
				h.beat(t, r.ID, 0, false)
			}
		}
	}
}

func (h *dharness) timeout(t core.Tick, disk uint32) {
	h.at(t)
	h.send(AppendMemberTimeout(nil, &MemberTimeout{ID: 1, PG: PGFields{Epoch: 1, PG: core.PGID{Pool: testPool}, Interval: 1}, Disk: disk}), 0)
}

func (h *dharness) reply(t core.Tick, p Probe, log uint64, oldest core.Tick, failed bool) {
	h.at(t)
	h.send(AppendProbeReply(nil, &ProbeReply{ID: p.ID, Log: log, Oldest: oldest, Failed: failed}), p.Disk-1)
}

func (h *dharness) down(disk uint32) (bool, core.Tick) {
	m, since := h.d.Mark(disk)
	return m == MarkDown, since
}

func (h *dharness) wantDown(disk uint32, since core.Tick) {
	h.t.Helper()
	if down, s := h.down(disk); !down || s != since {
		h.t.Fatalf("at %d: disk %d down %v since %d, want down since %d", h.ctx.Now, disk, down, s, since)
	}
}

func (h *dharness) wantUp(disk uint32) {
	h.t.Helper()
	if down, s := h.down(disk); down {
		h.t.Fatalf("at %d: disk %d down since %d, want up", h.ctx.Now, disk, s)
	}
}

var (
	rep3Pool     = Pool{Replicas: 3, MinLive: 2, FailureDomain: DomainNode, MaxPerDomain: 1, Locality: LocalitySpread, Overcommit: true, WriteLimit: 90, PGCount: 8}
	failSlowPool = Pool{Replicas: 3, MinLive: 2, FailureDomain: DomainNode, MaxPerDomain: 1, Locality: LocalitySpread, Overcommit: true, WriteLimit: 90, PGCount: 16}
)

// A disk whose heartbeats stop is marked down node_down_after after the last, in a table that starts a new interval of every PG with a member on it; one heartbeat before that restarts the count.
func TestADiskSilentForNodeDownAfterIsMarkedDown(t *testing.T) {
	h := newDetector(t, 3, rep3Pool, sec)
	h.beats(sec, 10*sec)
	h.beats(11*sec, 14*sec, 3)
	h.at(15*sec - 1)
	h.wantUp(3)
	h.at(15 * sec)
	h.wantDown(3, 15*sec)
	prev := h.publish()
	for i, e := range h.table.Pools[0].PGs {
		on := slices.ContainsFunc(prev.Pools[0].PGs[i].Members, func(m Member) bool { return m.Disk == 3 })
		if raised := e.Interval > prev.Pools[0].PGs[i].Interval; raised != on {
			t.Errorf("PG %d: interval raised %v, a member on disk 3 %v", i, raised, on)
		}
	}

	h = newDetector(t, 3, rep3Pool, sec)
	h.beats(sec, 10*sec)
	h.beats(11*sec, 14*sec, 3)
	h.beat(14*sec+900_000, 3, 0, false)
	h.beats(15*sec, 19*sec, 3)
	h.at(19*sec + 899_999)
	h.wantUp(3)
	h.at(19*sec + 900_000)
	h.wantDown(3, 19*sec+900_000)
}

// A detector that may have stood still, on Resumed or when its last event is more than two heartbeats old, gives every disk one more heartbeat before it counts as silent: heartbeats queued behind a stop of its own node mark nothing down, while every disk falling silent at once is marked down within a heartbeat after node_down_after.
func TestADetectorThatStoodStillGivesEveryDiskAHeartbeat(t *testing.T) {
	for _, timerFirst := range []bool{false, true} {
		h := newDetector(t, 3, rep3Pool, sec)
		h.beats(sec, 3*sec)
		h.timerAt, h.ctx.Now = 0, 8*sec+500_000
		if timerFirst {
			h.feed(&core.Event{Kind: core.EvTimer, Timer: DetectTimer})
		}
		h.beats(8*sec+500_000, 8*sec+500_000)
		for id := uint32(1); id <= 3; id++ {
			h.wantUp(id)
		}
	}

	h := newDetector(t, 3, rep3Pool, sec)
	h.beats(sec, 3*sec)
	h.beats(4*sec, 7*sec, 1)
	h.at(7*sec + 500_000)
	h.feed(&core.Event{Kind: core.EvResumed})
	h.beats(8*sec, 8*sec, 1)
	h.at(8*sec + 499_999)
	h.wantUp(1)
	h.at(8*sec + 500_000)
	h.wantDown(1, 8*sec+500_000)

	h = newDetector(t, 3, rep3Pool, sec)
	h.beats(sec, 10*sec)
	h.at(15*sec - 1)
	h.wantUp(1)
	h.at(16 * sec)
	for id := uint32(1); id <= 3; id++ {
		if down, _ := h.down(id); !down {
			t.Fatalf("disk %d up a heartbeat after node_down_after of every disk's silence", id)
		}
	}
}

// A new control-plane leader counts node_down_after from its own leader lease: a disk last heard from before it is marked down at the lease's start plus node_down_after.
func TestANewLeaderCountsNodeDownAfterFromItsLease(t *testing.T) {
	const start = 30 * sec
	h := newDetector(t, 3, rep3Pool, start)
	h.beats(start, start+4*sec, 1)
	h.at(start + NodeDownAfter - 1)
	h.wantUp(1)
	h.at(start + NodeDownAfter)
	h.wantDown(1, start+NodeDownAfter)
}

// A primary's member_timeout report is answered by a probe of the member's loop: a failed log, an I/O older than member_timeout, another log or no answer within member_timeout marks it down at once, and an answer with no I/O that old leaves it up. A disk whose loop did not answer rejoins at its next normal heartbeat, as a silent one does; the others wait slow_disk_window.
func TestAMemberTimeoutReportIsAnsweredByAProbe(t *testing.T) {
	for _, c := range []struct {
		name   string
		answer bool
		log    uint64
		oldest core.Tick
		failed bool
		down   bool
	}{
		{"probe fails", true, logOf(2), 0, true, true},
		{"a write 1.2 s old", true, logOf(2), 1_200_000, false, true},
		{"another log", true, 0x99, 0, false, true},
		{"no answer", false, 0, 0, false, true},
		{"healthy", true, logOf(2), 900_000, false, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := newDetector(t, 3, rep3Pool, sec)
			h.beats(sec, 3*sec)
			h.timeout(3*sec+100, 2)
			if len(h.probes) != 1 || h.probes[0].Disk != 2 {
				t.Fatalf("probes %+v", h.probes)
			}
			h.timeout(3*sec+200, 2)
			if len(h.probes) != 1 {
				t.Fatalf("a second report while a probe is out sent another: %+v", h.probes)
			}
			at := 3*sec + 100 + MemberTimeoutAfter
			if c.answer {
				at = 3*sec + 5_000
				h.reply(at, h.probes[0], c.log, c.oldest, c.failed)
			}
			h.at(at)
			if c.down {
				h.wantDown(2, at)
				h.beats(at+sec, at+sec)
				if down, _ := h.down(2); down != c.answer {
					t.Fatalf("at %d, after a normal heartbeat: disk 2 down %v", h.ctx.Now, down)
				}
			} else {
				h.at(4*sec - 1)
				h.wantUp(2)
			}
		})
	}
	h := newDetector(t, 3, rep3Pool, sec)
	h.beats(sec, 2*sec)
	h.timeout(2*sec+1, 9)
	h.reply(2*sec+2, Probe{ID: 77, Disk: 1}, 0, 5*sec, true)
	if len(h.probes) != 0 {
		t.Fatalf("a report of an unknown disk sent %+v", h.probes)
	}
	h.wantUp(1)
}

// A member_timeout report that reaches a new leader before the disk's first heartbeat sends no probe: the heartbeat, due within a second, stands for it.
func TestAReportBeforeAFirstHeartbeatSendsNoProbe(t *testing.T) {
	h := newDetector(t, 3, rep3Pool, 10*sec)
	h.timeout(10*sec+100, 2)
	h.beats(10*sec+500_000, 10*sec+500_000)
	h.at(11*sec + 200_000)
	if len(h.probes) != 0 {
		t.Fatalf("probes sent: %+v", h.probes)
	}
	h.wantUp(2)
}

// A new control-plane leader keeps the marks of the leader before, as the disk record holds them: a disk down stays down from when it was marked, and rejoins as a disk that failed.
func TestANewLeaderKeepsTheRecordedMarks(t *testing.T) {
	h := newDetector(t, 3, rep3Pool, sec)
	h.beats(sec, 2*sec)
	h.beat(3*sec, 1, MemberTimeoutAfter, false)
	h.wantDown(1, 3*sec)
	h.publish()
	if r := h.rec.Disks()[0]; r.Up || r.DownSince != 3*sec {
		t.Fatalf("the record has %+v", r)
	}
	h.d = NewDetector(&h.rec, 10*sec)
	h.d.View(&h.table, h.pools())
	h.timerAt, h.ctx.Now = 0, 10*sec
	h.wantDown(1, 3*sec)
	h.beats(10*sec, 69*sec)
	h.wantDown(1, 3*sec)
	h.beats(70*sec, 70*sec)
	h.wantUp(1)
	if !h.rec.Disks()[0].Up {
		t.Fatal("the record still has disk 1 down")
	}
}

// A heartbeat naming a disk whose oldest unfinished I/O passed member_timeout, or whose log failed, marks it down at once, even when that leaves a PG below min_live; it rejoins only once a whole slow_disk_window passes normal.
func TestAStalledOrFailedDiskIsMarkedDownFromItsHeartbeat(t *testing.T) {
	for _, failed := range []bool{false, true} {
		h := newDetector(t, 3, rep3Pool, sec)
		h.beats(sec, 2*sec)
		h.beat(2*sec+10, 1, 0, false)
		h.beat(3*sec, 1, MemberTimeoutAfter, failed)
		h.wantDown(1, 3*sec)
		h.publish()
		h.beats(3*sec+1, 53*sec+1)
		h.wantDown(1, 3*sec)
		h.beats(54*sec+1, 64*sec+1)
		h.wantUp(1)
	}

	h := newDetector(t, 3, Pool{Replicas: 3, MinLive: 3, FailureDomain: DomainNode, MaxPerDomain: 1, Locality: LocalitySpread, Overcommit: true, WriteLimit: 90, PGCount: 8}, sec)
	h.beats(sec, 2*sec)
	h.beat(3*sec, 2, 2*sec, false)
	h.wantDown(2, 3*sec)
}

// A disk marked down as stalled rejoins at its first normal heartbeat while a PG it belongs to is below min_live without it, which stops that PG anyway: a 1 s stall on rep2 copies nothing away, though a third disk could take its members.
func TestAStalledDiskRejoinsAPGStoppedWithoutIt(t *testing.T) {
	rep2 := Pool{Replicas: 2, MinLive: 2, FailureDomain: DomainNode, MaxPerDomain: 1, Locality: LocalitySpread, Overcommit: true, WriteLimit: 90, PGCount: 8}
	h := newDetector(t, 3, rep2, sec)
	on := func() []int {
		var pgs []int
		for i, e := range h.table.Pools[0].PGs {
			if slices.ContainsFunc(e.Members, func(m Member) bool { return m.Disk == 1 && !m.Leaving }) {
				pgs = append(pgs, i)
			}
		}
		return pgs
	}
	h.beats(sec, 2*sec)
	before := on()
	h.beat(3*sec, 1, MemberTimeoutAfter, false)
	h.wantDown(1, 3*sec)
	h.publish()
	h.beats(4*sec, 4*sec)
	h.wantUp(1)
	h.publish()
	if !slices.Equal(on(), before) {
		t.Fatalf("a 1 s stall moved disk 1's PGs from %v to %v", before, on())
	}
}

// A probe or heartbeat of a disk marked stalled restarts its normal window.
func TestAStallWhileDownRestartsTheWindow(t *testing.T) {
	h := newDetector(t, 3, rep3Pool, sec)
	h.beats(sec, 2*sec)
	h.beat(3*sec, 1, MemberTimeoutAfter, false)
	h.beats(4*sec, 62*sec)
	h.beat(62*sec+1, 1, MemberTimeoutAfter, false)
	h.beats(63*sec, 122*sec)
	h.wantDown(1, 3*sec)
	h.beats(123*sec, 123*sec)
	h.wantUp(1)
}

// A disk marked down flap_count times within flap_window stays down until its heartbeats have been normal for a whole flap_window, from the mark that held it; never refilled meanwhile in rep3 or rep2, but refilled once down for refill_after.
func TestAFlappingDiskIsHeldDown(t *testing.T) {
	for _, pool := range []Pool{rep3Pool, {Replicas: 2, MinLive: 2, FailureDomain: DomainNode, MaxPerDomain: 1, Locality: LocalitySpread, Overcommit: true, WriteLimit: 90, PGCount: 8}} {
		h := newDetector(t, 4, pool, sec)
		on := func() []int {
			var pgs []int
			for i, e := range h.table.Pools[0].PGs {
				if slices.ContainsFunc(e.Members, func(m Member) bool { return m.Disk == 4 }) {
					pgs = append(pgs, i)
				}
			}
			return pgs
		}
		before := on()
		intervals := func() uint32 {
			var n uint32
			for _, i := range before {
				n += h.table.Pools[0].PGs[i].Interval
			}
			return n
		}
		t0 := intervals()
		at := sec
		for flap := range FlapCount {
			h.beats(at, at+4*sec)
			h.beats(at+5*sec, at+10*sec, 4)
			h.at(at + 10*sec)
			h.wantDown(4, at+9*sec)
			h.publish()
			at += 10 * sec
			h.beats(at, at)
			if flap < FlapCount-1 {
				h.wantUp(4)
				h.publish()
				if got := intervals() - t0; got != 2*uint32(len(before))*uint32(flap+1) {
					t.Fatalf("after flap %d: %d intervals started", flap+1, got)
				}
			}
			if !slices.Equal(on(), before) {
				t.Fatalf("flap %d refilled disk 4: %v, was %v", flap+1, on(), before)
			}
			at += sec
		}
		damped := at - 2*sec
		h.wantDown(4, damped)
		for ; at < damped+9*60*sec; at += 10 * sec {
			h.beats(at, at+9*sec)
			h.publish()
		}
		h.wantDown(4, damped)
		for ; at <= damped+FlapWindow+sec; at += sec {
			h.beats(at, at)
			h.publish()
		}
		h.wantUp(4)
		if pool.Replicas == 3 && slices.Equal(on(), before) {
			t.Fatalf("held down past refill_after, disk 4 keeps its members %v", before)
		}
	}
}

// A disk marked down as stalled flap_count times within flap_window stays down until its I/O ages have been normal for a whole flap_window.
func TestADiskStalledFlapCountTimesIsHeldDown(t *testing.T) {
	h := newDetector(t, 3, rep3Pool, sec)
	at := sec
	for range FlapCount {
		h.beats(at, at)
		h.beat(at+sec, 1, MemberTimeoutAfter, false)
		h.wantDown(1, at+sec)
		at += 2 * sec
		h.beats(at, at+SlowDiskWindow)
		at += SlowDiskWindow + sec
	}
	held := at - SlowDiskWindow - 2*sec
	h.wantDown(1, held)
	h.beats(at, held+FlapWindow+sec)
	h.wantUp(1)
}

// A damped disk is released at its first normal heartbeat when a PG it belongs to would otherwise stay below min_live with no disk to refill onto: rep2 on two storage nodes.
func TestADampedDiskNeededForMinLiveIsReleased(t *testing.T) {
	h := newDetector(t, 2, Pool{Replicas: 2, MinLive: 2, FailureDomain: DomainNode, MaxPerDomain: 1, Locality: LocalitySpread, Overcommit: true, WriteLimit: 90, PGCount: 4}, sec)
	at := sec
	for range FlapCount {
		h.beats(at, at+sec)
		h.beats(at+2*sec, at+7*sec, 2)
		h.at(at + 7*sec)
		h.publish()
		at += 8 * sec
	}
	h.wantDown(2, at-2*sec)
	h.beat(at, 2, 0, false)
	h.wantUp(2)
}

// A damped disk peering waits for is released at its first normal heartbeat, though another disk could take its place: on rep2 with min_live 1, the PGs it ran alone after their other member went down cannot activate without it, and placement does not refill a member peering waits for.
func TestADampedDiskPeeringWaitsForIsReleased(t *testing.T) {
	h := newDetector(t, 3, Pool{Replicas: 2, MinLive: 1, FailureDomain: DomainNode, MaxPerDomain: 1, Locality: LocalitySpread, Overcommit: true, WriteLimit: 90, PGCount: 8}, sec)
	alone := slices.ContainsFunc(h.table.Pools[0].PGs, func(e PGEntry) bool {
		return slices.ContainsFunc(e.Members, func(m Member) bool { return m.Disk == 1 }) && slices.ContainsFunc(e.Members, func(m Member) bool { return m.Disk == 2 })
	})
	if !alone {
		t.Fatal("no PG on disks 1 and 2")
	}
	h.beats(sec, 2*sec)
	h.beats(3*sec, 7*sec, 2)
	h.wantDown(2, 7*sec)
	h.publish()
	at := 8 * sec
	for range FlapCount {
		h.beats(at, at+sec, 2)
		h.beats(at+2*sec, at+7*sec, 1, 2)
		h.at(at + 7*sec)
		h.publish()
		at += 8 * sec
	}
	h.wantDown(1, at-2*sec)
	h.beat(at, 1, 0, false)
	h.wantUp(1)
}

// Each primary's member latencies are judged against the pool: a disk above slow_disk_factor times the median of the others for a whole slow_disk_window is marked down unless a PG would drop below min_live, and rejoins after a whole window normal.
func TestAFailSlowDiskIsMarkedDown(t *testing.T) {
	const base = core.Tick(2_000)
	// run feeds a second of latencies, X's at ratio times the others', from a primary that is not X.
	run := func(h *dharness, from, to core.Tick, ratio func(t core.Tick) core.Tick) {
		for t := from; t <= to; t += sec {
			var lats []Latency
			for id := uint32(2); id <= 6; id++ {
				lats = append(lats, Latency{Disk: id, Latency: base + core.Tick(id)})
			}
			lats = append(lats, Latency{Disk: 1, Latency: base * ratio(t) / 10})
			for id := uint32(1); id <= 6; id++ {
				h.beat(t, id, 0, false)
			}
			h.beat(t, 6, 0, false, lats...)
		}
	}
	steady := func(r core.Tick) func(core.Tick) core.Tick { return func(core.Tick) core.Tick { return r } }
	pool6 := failSlowPool

	h := newDetector(t, 6, pool6, sec)
	run(h, sec, 60*sec, steady(120))
	h.wantUp(1)
	run(h, 61*sec, 61*sec, steady(120))
	h.wantDown(1, 61*sec)
	prev := h.publish()
	run(h, 62*sec, 121*sec, steady(10))
	h.wantDown(1, 61*sec)
	run(h, 122*sec, 122*sec, steady(10))
	h.wantUp(1)
	if h.publish(); h.table.Pools[0].PGs[0].Interval == prev.Pools[0].PGs[0].Interval && slices.ContainsFunc(prev.Pools[0].PGs[0].Members, func(m Member) bool { return m.Disk == 1 }) {
		t.Fatal("rejoining started no interval")
	}

	h = newDetector(t, 6, pool6, sec)
	run(h, sec, 60*sec, steady(120))
	run(h, 61*sec, 120*sec, steady(10))
	h.wantUp(1)

	h = newDetector(t, 6, pool6, sec)
	run(h, sec, 600*sec, steady(90))
	h.wantUp(1)

	h = newDetector(t, 6, pool6, sec)
	run(h, sec, 600*sec, func(t core.Tick) core.Tick {
		if (t/sec)%60 < 50 {
			return 120
		}
		return 10
	})
	h.wantUp(1)

	h = newDetector(t, 6, Pool{Replicas: 3, MinLive: 3, FailureDomain: DomainNode, MaxPerDomain: 1, Locality: LocalitySpread, Overcommit: true, WriteLimit: 90, PGCount: 16}, sec)
	run(h, sec, 120*sec, steady(120))
	h.wantUp(1)
	if !slices.Equal(h.d.Slow(), []uint32{1}) {
		t.Fatalf("slow disks kept up for min_live: %v", h.d.Slow())
	}
	h.beat(121*sec, 6, 0, false, Latency{Disk: 42, Latency: sec})
}

// A disk is marked down as fail-slow only once it has been slow for a whole slow_disk_window: two slow figures a window apart with none between leave it up.
func TestFailSlowNeedsAWholeSlowWindow(t *testing.T) {
	const base = core.Tick(2_000)
	h := newDetector(t, 6, failSlowPool, sec)
	for at := sec; at <= 61*sec; at += sec {
		var lats []Latency
		for id := uint32(2); id <= 5; id++ {
			lats = append(lats, Latency{Disk: id, Latency: base})
		}
		if at == sec || at == 61*sec {
			lats = append(lats, Latency{Disk: 1, Latency: 12 * base})
		}
		for id := uint32(1); id <= 6; id++ {
			h.beat(at, id, 0, false)
		}
		h.beat(at, 6, 0, false, lats...)
	}
	h.wantUp(1)
}

// Two disks slow at once are judged against the marks the detector made, not only the table's: the second is kept up where marking both would leave a PG below min_live.
func TestTwoSlowDisksKeepMinLive(t *testing.T) {
	const base = core.Tick(2_000)
	h := newDetector(t, 6, failSlowPool, sec)
	for at := sec; at <= 61*sec; at += sec {
		var lats []Latency
		for id := uint32(3); id <= 5; id++ {
			lats = append(lats, Latency{Disk: id, Latency: base})
		}
		lats = append(lats, Latency{Disk: 1, Latency: 12 * base}, Latency{Disk: 2, Latency: 12 * base})
		for id := uint32(1); id <= 6; id++ {
			h.beat(at, id, 0, false)
		}
		h.beat(at, 6, 0, false, lats...)
	}
	for i, e := range h.table.Pools[0].PGs {
		live := 0
		for _, m := range e.Members {
			if down, _ := h.down(m.Disk); !down {
				live++
			}
		}
		if live < int(failSlowPool.MinLive) {
			t.Fatalf("PG %d members %v: %d live by the detector's marks", i, e.Members, live)
		}
	}
	if len(h.d.Slow()) != 1 {
		t.Fatalf("slow disks kept up: %v", h.d.Slow())
	}
}

// The primary reports each member's 99th-percentile acknowledgment latency from buckets of half an octave, at the bucket's lower bound.
func TestLatencyBuckets(t *testing.T) {
	prev := -1
	for v := core.Tick(0); v < 1<<20; v += 1 + v/64 {
		b := latBucket(v)
		if b < prev || b >= latBuckets || latFloor(b) > v || v-latFloor(b) > latFloor(b)/2 {
			t.Fatalf("latency %d: bucket %d, floor %d, after %d", v, b, latFloor(b), prev)
		}
		prev = b
	}
	if latBucket(1<<40) != latBuckets-1 {
		t.Fatal("a latency past the last bucket")
	}
	var m latHist
	for i := range 1000 {
		took := core.Tick(100)
		if i%100 == 0 {
			took = 50_000
		}
		m.hist[latBucket(took)]++
		m.n++
	}
	if got := m.p99(); got != latFloor(latBucket(100)) {
		t.Fatalf("p99 of 1000 with 10 slow: %d", got)
	}
	m.hist[latBucket(50_000)]++
	m.n++
	if got := m.p99(); got != latFloor(latBucket(50_000)) {
		t.Fatalf("p99 of 1001 with 11 slow: %d", got)
	}
}
