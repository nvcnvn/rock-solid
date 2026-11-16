package meta

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

const sec = core.Tick(1_000_000)

// run moves time on by d a second at a time, each second every node's last report again and a
// normal heartbeat of every node 0 to 4 but those in skip, then the detector's timer.
func (r *ctlRun) run(d core.Tick, skip ...uint32) {
	for end := r.now + d; r.now < end; {
		r.now += sec
		for n := range uint32(5) {
			if nr, ok := r.reports[n]; ok {
				r.heartbeat(&nr)
			}
			if !slices.Contains(skip, n) {
				r.beat(n, 0, false)
			}
		}
		r.event(0, nil)
	}
}

// settle commits whatever the machine proposes, if anything, and reports whether it did.
func (r *ctlRun) settle(in *ControlInputs) bool {
	r.t.Helper()
	in.Now = r.now
	if !r.propose(in) {
		return false
	}
	if a := r.answer(); a.Err != nil || a.Result.FailedCheck >= 0 {
		r.t.Fatalf("the commit answered %+v", a)
	}
	r.c.agree()
	return true
}

// failover moves the group's leadership to another replica and starts a new machine there.
func (r *ctlRun) failover() {
	old := r.l
	r.c.cut[old] = true
	r.l = r.c.elect()
	r.c.cut[old] = false
	r.c.heartbeat()
	r.ctl = r.load(r.l)
}

// disk is disk id as last committed.
func (r *ctlRun) disk(id uint32) DiskEntry { return r.ctl.Disks()[id-1] }

// members counts the PGs with a member on disk id that is not leaving.
func members(t *chunkstore.Table, id uint32) int {
	n := 0
	for _, e := range t.Pools[0].PGs {
		if slices.ContainsFunc(e.Members, func(m chunkstore.Member) bool { return m.Disk == id && !m.Leaving }) {
			n++
		}
	}
	return n
}

// poolRun is a ctlRun with a rep3 pool of 16 PGs on disks 1 to 5 and the two-group shard map committed.
func poolRun(t *testing.T) (*ctlRun, *ControlInputs) {
	r := newCtlRun(t)
	in := &ControlInputs{Disks: disks(0, 1, 2, 3, 4)}
	if err := r.ctl.SetPool(1, spec(3, 16), in.Disks); err != nil {
		t.Fatal(err)
	}
	if err := r.ctl.SetShardMap(twoGroups()); err != nil {
		t.Fatal(err)
	}
	r.settle(in)
	return r, in
}

// A disk silent for node_down_after is marked down in one table, which starts a new interval of
// every PG with a member on it and no other; down for refill_after, its members are refilled and
// its voters replaced.
func TestASilentDiskIsMarkedDownThenRefilled(t *testing.T) {
	r, in := poolRun(t)
	prev := *r.ctl.Table()
	r.run(chunkstore.NodeDownAfter-sec, 0)
	r.settle(in)
	if !r.disk(1).Up || r.ctl.Table().Epoch != prev.Epoch {
		t.Fatalf("disk 1 %+v before node_down_after, table %d", r.disk(1), r.ctl.Table().Epoch)
	}
	r.run(sec, 0)
	r.settle(in)
	if r.disk(1).Up || r.ctl.Table().Epoch != prev.Epoch+1 {
		t.Fatalf("disk 1 %+v after node_down_after, table %d", r.disk(1), r.ctl.Table().Epoch)
	}
	for i, e := range r.ctl.Table().Pools[0].PGs {
		on := slices.ContainsFunc(e.Members, func(m chunkstore.Member) bool { return m.Disk == 1 })
		if (e.Interval != prev.Pools[0].PGs[i].Interval) != on {
			t.Fatalf("PG %d with a member on disk 1 %v went from interval %d to %d", i, on, prev.Pools[0].PGs[i].Interval, e.Interval)
		}
	}
	r.run(chunkstore.RefillAfter-sec, 0)
	r.settle(in)
	if members(r.ctl.Table(), 1) == 0 {
		t.Fatal("refilled before refill_after")
	}
	r.run(2*sec, 0)
	r.settle(in)
	if n := members(r.ctl.Table(), 1); n != 0 {
		t.Fatalf("%d PGs keep disk 1 after refill_after", n)
	}
	for _, g := range r.ctl.ShardMap().Groups {
		if len(g.Voters) != Voters || slices.ContainsFunc(g.Voters, func(v Replica) bool { return v.Disk == 1 }) {
			t.Fatalf("group %d keeps voters %+v", g.ID, g.Voters)
		}
	}
}

// A disk marked down for the 5th time within flap_window is held down until it has been normal
// for a whole flap_window.
func TestAFlappingDiskIsHeldDown(t *testing.T) {
	r, in := poolRun(t)
	for range chunkstore.FlapCount {
		r.run(chunkstore.NodeDownAfter, 0)
		r.settle(in)
		if r.disk(1).Up {
			t.Fatal("a silent disk stayed up")
		}
		r.run(sec)
		r.settle(in)
	}
	if r.disk(1).Up {
		t.Fatal("a disk marked down five times came back at its next heartbeat")
	}
	r.run(chunkstore.FlapWindow - 2*sec)
	r.settle(in)
	if r.disk(1).Up {
		t.Fatal("a damped disk came back before flap_window")
	}
	r.run(2 * sec)
	r.settle(in)
	if !r.disk(1).Up {
		t.Fatal("a damped disk stayed down after a whole flap_window normal")
	}
}

// A primary's member_timeout report sends a probe: a probe answer showing a stall marks the disk
// down at once, one with no stall does not. A heartbeat showing a stall marks it down at once.
func TestAStallIsMarkedDownAtOnce(t *testing.T) {
	for _, stalled := range []bool{true, false} {
		r, in := poolRun(t)
		r.run(sec)
		acts := r.event(3, chunkstore.AppendMemberTimeout(nil, &chunkstore.MemberTimeout{ID: 1, PG: chunkstore.PGFields{Epoch: 1, PG: core.PGID{Pool: 1}, Interval: 1}, Disk: 1}))
		var p chunkstore.Probe
		i := slices.IndexFunc(acts, func(a core.Action) bool { return a.Kind == core.ActNetSend })
		if i < 0 || acts[i].Peer.Node != 0 || chunkstore.DecodeProbe(acts[i].Msg.Inline(), 0, &p) != nil {
			t.Fatalf("no probe of disk 1 in %+v", acts)
		}
		oldest := core.Tick(0)
		if stalled {
			oldest = chunkstore.MemberTimeoutAfter
		}
		r.event(0, chunkstore.AppendProbeReply(nil, &chunkstore.ProbeReply{ID: p.ID, Log: logOf(0), Oldest: oldest}))
		r.settle(in)
		if r.disk(1).Up == stalled {
			t.Fatalf("a probe answered stalled %v left disk 1 %+v", stalled, r.disk(1))
		}
	}
	r, in := poolRun(t)
	r.beat(2, chunkstore.MemberTimeoutAfter, false)
	r.settle(in)
	if r.disk(3).Up {
		t.Fatal("a stalled heartbeat left its disk up")
	}
}

// A disk whose latency stays above slow_disk_factor times its pool's median for slow_disk_window
// is marked down, unless that drops a PG below min_live; back to normal, it rejoins only after a
// whole slow_disk_window.
func TestAFailSlowDiskIsMarkedDownAndRejoinsAfterAWindow(t *testing.T) {
	lats := func(r *ctlRun, d core.Tick) {
		for end := r.now + d; r.now < end; {
			r.run(sec)
			var ls []chunkstore.Latency
			for id := range uint32(5) {
				l := core.Tick(100)
				if id == 0 {
					l = 2000
				}
				ls = append(ls, chunkstore.Latency{Disk: id + 1, Latency: l})
			}
			r.event(4, chunkstore.AppendReport(nil, &chunkstore.Report{ID: 1, Latencies: ls}))
		}
	}
	r, in := poolRun(t)
	r.fail(2)
	r.settle(in)
	lats(r, chunkstore.SlowDiskWindow+2*sec)
	r.settle(in)
	if !r.disk(1).Up {
		t.Fatal("a slow disk was marked down though a PG would drop below min_live")
	}
	r, in = poolRun(t)
	// The window runs from the second report: the first has no other disk's latency to compare with.
	lats(r, chunkstore.SlowDiskWindow-sec)
	r.settle(in)
	if !r.disk(1).Up {
		t.Fatal("a slow disk was marked down before slow_disk_window")
	}
	lats(r, 3*sec)
	r.settle(in)
	if r.disk(1).Up {
		t.Fatal("a fail-slow disk stayed up")
	}
	r.run(50 * sec)
	r.settle(in)
	if r.disk(1).Up {
		t.Fatal("a fail-slow disk rejoined after 50 s normal")
	}
	r.run(11 * sec)
	r.settle(in)
	if !r.disk(1).Up {
		t.Fatal("a fail-slow disk stayed down after a whole slow_disk_window normal")
	}
}

// A node of role meta holds voters and no PG member.
func TestAMetaOnlyNodeHoldsVotersAndNoMember(t *testing.T) {
	r := newCtlRun(t)
	for n := range uint32(3) {
		r.heartbeat(&NodeReport{Node: n})
	}
	r.heartbeat(&NodeReport{Node: 5, Meta: true, MetaOnly: true, Opened: []OpenedDisk{{Device: devOf(5), Log: logOf(5)}}})
	in := &ControlInputs{Disks: disks(0, 1, 2, 3, 4, 5)}
	if err := r.ctl.SetPool(1, spec(3, 16), in.Disks); err != nil {
		t.Fatal(err)
	}
	if err := r.ctl.SetShardMap(twoGroups()); err != nil {
		t.Fatal(err)
	}
	r.settle(in)
	if n := members(r.ctl.Table(), 6); n != 0 {
		t.Fatalf("%d PGs have a member on the role meta node", n)
	}
	for _, g := range r.ctl.ShardMap().Groups {
		if ns := voterNodes(&g); !slices.Contains(ns, 5) || slices.ContainsFunc(ns, func(n uint32) bool { return n < 3 }) {
			t.Fatalf("group %d has voters on nodes %v", g.ID, ns)
		}
	}
}

// The format level is raised only above the committed one, to a release every node heard runs,
// and once every node with a disk up has been heard. A node on an older release that heartbeats
// after it is raised is not marked up, and is named.
func TestANodeBelowTheFormatLevelIsNotMarkedUp(t *testing.T) {
	r, in := poolRun(t)
	if err := r.ctl.SetFormat(2); !errors.Is(err, ErrFormatLevel) {
		t.Fatalf("a level above every node's release answered %v", err)
	}
	for n := range uint32(5) {
		r.heartbeat(&NodeReport{Node: n, Meta: true, Release: 2})
	}
	if err := r.ctl.SetFormat(2); err != nil {
		t.Fatal(err)
	}
	r.settle(in)
	if r.ctl.Format() != 2 || r.load(r.l).Format() != 2 {
		t.Fatalf("format level %d committed", r.ctl.Format())
	}
	if err := r.ctl.SetFormat(2); !errors.Is(err, ErrFormatLevel) {
		t.Fatalf("the same level again answered %v", err)
	}
	r.run(chunkstore.NodeDownAfter, 0)
	r.heartbeat(&NodeReport{Node: 0, Meta: true, Release: 1})
	r.run(30 * sec)
	r.settle(in)
	if r.disk(1).Up || !slices.Equal(r.ctl.OldReleases(), []uint32{0}) {
		t.Fatalf("disk 1 %+v on an old release, nodes named %v", r.disk(1), r.ctl.OldReleases())
	}
	r.heartbeat(&NodeReport{Node: 0, Meta: true, Release: 2})
	r.run(sec)
	r.settle(in)
	if !r.disk(1).Up || len(r.ctl.OldReleases()) != 0 {
		t.Fatalf("disk 1 %+v after its upgrade, nodes named %v", r.disk(1), r.ctl.OldReleases())
	}
	r.ctl = r.load(r.l)
	for n := range uint32(4) {
		r.heartbeat(&NodeReport{Node: n, Meta: true, Release: 3})
	}
	if err := r.ctl.SetFormat(3); !errors.Is(err, ErrFormatLevel) {
		t.Fatalf("a new leader that has not heard node 4 raised the level: %v", err)
	}
	r.heartbeat(&NodeReport{Node: 4, Meta: true, Release: 3})
	if err := r.ctl.SetFormat(3); err != nil {
		t.Fatal(err)
	}
}

// A disk is known by its log ID on a stable device identity: disks renumbered on their node keep
// their IDs, a new log on a device replaces only the disk last reported on that device, and a log
// up on another device is refused.
func TestADiskIsKnownByItsLogOnItsDevice(t *testing.T) {
	r := newCtlRun(t)
	a, b := OpenedDisk{Device: 0xa, Log: 0x1a}, OpenedDisk{Device: 0xb, Log: 0x1b}
	r.heartbeat(&NodeReport{Node: 7, Opened: []OpenedDisk{a, b}})
	r.settle(&ControlInputs{})
	r.heartbeat(&NodeReport{Node: 7, Opened: []OpenedDisk{b, a}})
	r.settle(&ControlInputs{})
	if len(r.ctl.Disks()) != 7 || r.disk(6).Log != a.Log || r.disk(7).Log != b.Log || !r.disk(6).Up || !r.disk(7).Up {
		t.Fatalf("renumbered disks recorded as %+v", r.ctl.Disks()[5:])
	}
	r.heartbeat(&NodeReport{Node: 7, Opened: []OpenedDisk{{Device: b.Device, Log: 0x2b}, a}})
	r.settle(&ControlInputs{})
	if d := r.ctl.Disks(); len(d) != 8 || !d[6].Gone || d[5].Gone || !d[5].Up || d[7].Log != 0x2b {
		t.Fatalf("a new log on device b gave %+v", d[5:])
	}
	var out ControlOutput
	if err := r.ctl.Heartbeat(r.now, &NodeReport{Node: 8, Opened: []OpenedDisk{{Device: 0xc, Log: a.Log}}}, &out); !errors.Is(err, chunkstore.ErrLogRefused) {
		t.Fatalf("a cloned drive's log answered %v", err)
	}
	if got := r.ctl.FailedDevices(); !slices.Equal(got, []FailedDevice{{Node: 8, Device: 0xc}}) {
		t.Fatalf("a refused log's device shown as %+v", got)
	}
}

// A down mark commits as an age, rewritten each AgeStep: a new leader whose clock reads nothing
// like the old one's refills the disk refill_after after it went down, within an AgeStep, rather
// than refill_after after the failover; one whose clock reads less than the age counts from its
// clock's zero.
func TestADownMarkCarriesItsAgeAcrossAFailover(t *testing.T) {
	for _, clock := range []core.Tick{3 * 60 * AgeStep, 2 * AgeStep} {
		r, in := poolRun(t)
		r.fail(1)
		r.settle(in)
		r.run(AgeStep-sec, 0)
		if in.Now = r.now; r.propose(in) {
			t.Fatal("a down disk's age was written before an AgeStep")
		}
		r.run(sec, 0)
		r.settle(in)
		if r.disk(1).Age != AgeStep {
			t.Fatalf("the committed age is %d", r.disk(1).Age)
		}
		r.run(4*AgeStep, 0)
		r.settle(in)
		r.c.cut[r.l] = true
		r.l = r.c.elect()
		r.ctl, r.now = r.load(r.l), clock
		r.hearAll()
		since := clock - min(5*AgeStep, clock)
		r.run(since+chunkstore.RefillAfter-sec-r.now, 0)
		r.settle(in)
		if members(r.ctl.Table(), 1) == 0 {
			t.Fatalf("refilled early on a clock at %d", clock)
		}
		r.run(2*sec, 0)
		r.settle(in)
		if n := members(r.ctl.Table(), 1); n != 0 {
			t.Fatalf("on a clock at %d, %d PGs keep disk 1 refill_after after it went down", clock, n)
		}
	}
}

// A disk whose log is gone commits no age, so its entry is not written again each AgeStep.
func TestAGoneDiskIsNotWrittenAgain(t *testing.T) {
	r, in := poolRun(t)
	r.run(2 * AgeStep)
	const newLog = 0x99
	r.heartbeat(&NodeReport{Node: 0, Meta: true, Opened: []OpenedDisk{{Device: devOf(0), Log: newLog}}})
	run := func(d core.Tick) {
		for end := r.now + d; r.now < end; {
			r.run(sec, 0)
			r.event(0, chunkstore.AppendReport(nil, &chunkstore.Report{ID: 1, Disks: []chunkstore.DiskState{{Log: newLog}}}))
		}
	}
	run(sec)
	r.settle(in)
	if !r.disk(1).Gone || members(r.ctl.Table(), 1) != 0 {
		t.Fatalf("disk 1 %+v keeps %d PGs after a new log on its device", r.disk(1), members(r.ctl.Table(), 1))
	}
	for range 3 {
		run(AgeStep)
		if in.Now = r.now; r.propose(in) {
			t.Fatalf("an AgeStep later the record wrote %+v", r.txn.Ops)
		}
	}
}

// A report in which two disks share a device identity, as drives with no WWN can, is refused for
// both and retires neither; a disk whose log was replaced is refused afterwards.
func TestADeviceIdentityTwiceInAReportIsRefused(t *testing.T) {
	r := newCtlRun(t)
	twins := []OpenedDisk{{Device: 0, Log: 0x1a}, {Device: 0, Log: 0x1b}}
	var out ControlOutput
	for range 2 {
		if err := r.ctl.Heartbeat(r.now, &NodeReport{Node: 7, Opened: twins}, &out); !errors.Is(err, chunkstore.ErrLogRefused) {
			t.Fatalf("two disks on one device answered %v", err)
		}
	}
	r.settle(&ControlInputs{})
	if len(r.ctl.Disks()) != 5 {
		t.Fatalf("two disks on one device recorded as %+v", r.ctl.Disks()[5:])
	}
	r.heartbeat(&NodeReport{Node: 7, Opened: twins[:1]})
	r.heartbeat(&NodeReport{Node: 7, Opened: []OpenedDisk{{Device: 0, Log: 0x2a}}})
	if err := r.ctl.Heartbeat(r.now, &NodeReport{Node: 7, Opened: twins[:1]}, &out); !errors.Is(err, chunkstore.ErrLogRefused) {
		t.Fatalf("a replaced log answered %v", err)
	}
	r.settle(&ControlInputs{})
	if d := r.ctl.Disks(); len(d) != 7 || !d[5].Gone || !d[6].Up {
		t.Fatalf("a reformat recorded %+v", d[5:])
	}
}

// A format level staged before a node is heard on an older release is dropped at the next Step.
func TestAStagedFormatLevelIsCheckedAgainAtTheStep(t *testing.T) {
	r, in := poolRun(t)
	for n := range uint32(5) {
		r.heartbeat(&NodeReport{Node: n, Meta: true, Release: 2})
	}
	if err := r.ctl.SetFormat(2); err != nil {
		t.Fatal(err)
	}
	r.heartbeat(&NodeReport{Node: 3, Meta: true, Release: 1})
	var out ControlOutput
	if err := r.ctl.Step(&ControlInputs{Now: r.now, Disks: in.Disks}, 99, &out); !errors.Is(err, ErrFormatLevel) || out.Propose {
		t.Fatalf("a step after an older release was heard answered %v, proposed %v", err, out.Propose)
	}
	r.settle(in)
	if r.ctl.Format() != 0 || r.load(r.l).Format() != 0 {
		t.Fatalf("format level %d committed", r.ctl.Format())
	}
}

// After a failover, a disk whose node has not reported to the new leader is not marked up,
// however long it heartbeats, since the leader does not yet know the node's release.
func TestADiskIsNotMarkedUpBeforeItsNodeReports(t *testing.T) {
	r, in := poolRun(t)
	r.fail(1)
	r.settle(in)
	r.c.cut[r.l] = true
	r.l = r.c.elect()
	r.ctl = r.load(r.l)
	delete(r.reports, 0)
	for n := range uint32(4) {
		r.heartbeat(&NodeReport{Node: n + 1, Meta: true, Opened: []OpenedDisk{{Device: devOf(n + 1), Log: logOf(n + 1)}}})
	}
	r.run(2 * chunkstore.SlowDiskWindow)
	r.settle(in)
	if r.disk(1).Up {
		t.Fatal("a disk whose node never reported was marked up")
	}
	r.heartbeat(&NodeReport{Node: 0, Meta: true, Opened: []OpenedDisk{{Device: devOf(0), Log: logOf(0)}}})
	r.run(2 * chunkstore.SlowDiskWindow)
	r.settle(in)
	if !r.disk(1).Up {
		t.Fatal("a disk stayed down after its node reported")
	}
}

// A device a node reports failed, with no log ID, is shown to the operator for as long as the node
// reports it, and never joins the record.
func TestAFailedDeviceIsShownAndNeverRecorded(t *testing.T) {
	r := newCtlRun(t)
	disks := r.ctl.Disks()
	r.heartbeat(&NodeReport{Node: 2, Meta: true, Opened: []OpenedDisk{{Device: devOf(2), Log: logOf(2)}}, Failed: []uint64{0xbad}})
	r.heartbeat(&NodeReport{Node: 9, Failed: []uint64{0xbad, 0xbad + 1}})
	want := []FailedDevice{{Node: 2, Device: 0xbad}, {Node: 9, Device: 0xbad}, {Node: 9, Device: 0xbad + 1}}
	if got := r.ctl.FailedDevices(); !slices.Equal(got, want) {
		t.Fatalf("failed devices %+v, want %+v", got, want)
	}
	if r.propose(&ControlInputs{}) || !slices.Equal(r.ctl.Disks(), disks) {
		t.Fatalf("a failed device changed the record: %+v", r.ctl.Disks())
	}
	r.heartbeat(&NodeReport{Node: 2, Meta: true, Opened: []OpenedDisk{{Device: devOf(2), Log: logOf(2)}}})
	if got := r.ctl.FailedDevices(); !slices.Equal(got, want[1:]) {
		t.Fatalf("after node 2 stopped reporting its failed device: %+v", got)
	}
}

// A node not heard from for node_down_after is forgotten: the older release it ran no longer
// holds back the format level.
func TestANodeSilentForNodeDownAfterIsForgotten(t *testing.T) {
	r := newCtlRun(t)
	for n := range uint32(5) {
		r.heartbeat(&NodeReport{Node: n, Meta: true, Release: 2})
	}
	r.heartbeat(&NodeReport{Node: 9, Release: 1})
	if err := r.ctl.SetFormat(2); !errors.Is(err, ErrFormatLevel) {
		t.Fatalf("a level above a node's release answered %v", err)
	}
	r.run(chunkstore.NodeDownAfter)
	if err := r.ctl.SetFormat(2); err != nil {
		t.Fatalf("a node silent for node_down_after still holds back the level: %v", err)
	}
}

// A voter halted at an entry it cannot decode is replaced once its node has reported it halted for
// refill_after, by the node's own count of the halt's age.
func TestAHaltIsCountedFromTheAgeItsNodeReports(t *testing.T) {
	r, in := poolRun(t)
	// A leader counts a halt from no earlier than its own clock's zero.
	r.run(chunkstore.RefillAfter)
	v := r.ctl.ShardMap().Groups[0].Voters[0]
	halt := func(age core.Tick) {
		r.heartbeat(&NodeReport{Node: v.Node, Meta: true, Halts: []HaltReport{{Group: 1, ID: v.ID, Index: 7, Age: age}}})
		r.settle(in)
	}
	halt(chunkstore.RefillAfter - sec)
	if g := r.ctl.ShardMap().Groups[0]; g.Voters[0] != v {
		t.Fatalf("a voter halted short of refill_after was replaced: %+v", g.Voters)
	}
	r.run(sec)
	halt(chunkstore.RefillAfter)
	if g := r.ctl.ShardMap().Groups[0]; slices.Contains(g.Voters, v) || slices.Contains(voterNodes(&g), v.Node) {
		t.Fatalf("a voter halted for refill_after stays: %+v", g.Voters)
	}
}

// After a failover or an answer that is an error, every committed shard-map entry is handed out
// again, and the whole table goes once to each node whose heartbeat shows it behind.
func TestALostAnswerIsHandedOutAgain(t *testing.T) {
	r, in := poolRun(t)
	r.c.cut[r.l] = true
	r.l = r.c.elect()
	r.ctl = r.load(r.l)
	var out ControlOutput
	if err := r.ctl.Step(&ControlInputs{Disks: in.Disks}, 99, &out); err != nil || out.Propose || len(out.Plans) != 2 {
		t.Fatalf("a new leader's first step answered %v and handed out %+v", err, out.Plans)
	}
	behind := &NodeReport{Node: 2, Meta: true, Epoch: r.ctl.Table().Epoch - 1}
	if out := r.heartbeat(behind); !slices.Equal(out.Tables, []uint32{2}) {
		t.Fatalf("a node behind was sent tables %v", out.Tables)
	}
	if out := r.heartbeat(behind); len(out.Tables) != 0 {
		t.Fatalf("the table went again to %v", out.Tables)
	}
	if out := r.heartbeat(&NodeReport{Node: 3, Meta: true, Epoch: r.ctl.Table().Epoch}); len(out.Tables) != 0 {
		t.Fatalf("a node up to date was sent the table")
	}
	other := r.load(r.l)
	r.fail(1)
	r.settle(in)
	r.ctl = other
	r.fail(2)
	if !r.propose(in) {
		t.Fatal("nothing proposed")
	}
	r.c.heartbeat()
	a := r.c.answer(r.l, r.token)
	out.Reset()
	if err := r.ctl.Answer(r.c.n[r.l].s.State(), &a, &out); err != nil || a.Result.FailedCheck < 0 || len(out.Plans) != 2 {
		t.Fatalf("a failed commit answered %+v, %v, handed out %+v", a, err, out.Plans)
	}
	if out := r.heartbeat(behind); !slices.Equal(out.Tables, []uint32{2}) {
		t.Fatalf("after an error answer a node behind was sent tables %v", out.Tables)
	}
}

// An operator's drain commits in the disk record, and ends; a drain of an unknown disk is refused.
func TestADrainCommits(t *testing.T) {
	r, in := poolRun(t)
	if err := r.ctl.Drain(9, true); !errors.Is(err, ErrUnknownDisk) {
		t.Fatalf("a drain of an unknown disk answered %v", err)
	}
	for _, on := range []bool{true, false} {
		if err := r.ctl.Drain(2, on); err != nil {
			t.Fatal(err)
		}
		r.settle(in)
		if got := r.load(r.l).disks[1].Drain; got != on {
			t.Fatalf("drain %v read back as %v", on, got)
		}
	}
}

const (
	invRecordIDs   = "a disk ID keeps the log ID it was first given, and no log ID is under two disk IDs"
	invRecordReads = "the leader's state holds the disk record and format level last committed"
	invRecordGone  = "a disk's log is gone only once another log was reported on its device of its node after it, or an operator declared it lost"
	invGoneStays   = "a disk committed gone stays gone"
	invFormatLevel = "a format level commits only above no release of a node with a disk committed up"
)

// drive is a drive in the record test: the node it sits in, its stable identity and its log.
type drive struct {
	node     uint32
	dev, log uint64
}

// clone is the device a cloned drive is reported on in the record test.
const clone = 0xc10e

// releaseOf is the release node n runs in the record test: node 5 runs an older one.
func releaseOf(n uint32) uint32 {
	if n == 5 {
		return 2
	}
	return 3
}

// The disk record keeps its identities through restarts that renumber a node's disks, reformats,
// drives moved or cloned to other nodes, failed disks, operators' declarations and failovers, and
// every new leader reads it back as last committed.
func TestTheDiskRecordKeepsItsIdentitiesAcrossFailovers(t *testing.T) {
	seeds.Run(t, 0, 20, func(seed uint64) { recordRun(t, seed) })
}

func recordRun(t *testing.T, seed uint64) {
	rng := rand.New(rand.NewPCG(seed, 3))
	r := newCtlRun(t)
	var drives []drive
	for n := range uint32(5) {
		drives = append(drives, drive{n, devOf(n), logOf(n)}, drive{n, 0x100 + uint64(n), 0x1000 + uint64(n)})
	}
	nextLog := uint64(0x2000)
	committed, format := r.ctl.Disks(), r.ctl.Format()
	var reported []drive
	var raisedOver []DiskEntry
	var declared []uint32
	gone := map[uint32]bool{}
	logs := map[uint32]uint64{}
	var inv sim.Invariants
	inv.Register(invRecordIDs, func() error {
		ids := map[uint64]uint32{}
		for _, d := range r.ctl.Disks() {
			if l, ok := logs[d.ID]; ok && l != d.Log {
				return fmt.Errorf("disk %d names log %#x and %#x", d.ID, l, d.Log)
			}
			logs[d.ID] = d.Log
			if o, ok := ids[d.Log]; ok {
				return fmt.Errorf("log %#x is under disks %d and %d", d.Log, o, d.ID)
			}
			ids[d.Log] = d.ID
		}
		if len(r.ctl.Disks()) < len(logs) {
			return fmt.Errorf("the record lost disks %d to %d", len(r.ctl.Disks())+1, len(logs))
		}
		return nil
	})
	inv.Register(invRecordReads, func() error {
		got := r.load(r.l)
		if !slices.Equal(got.Disks(), committed) || got.Format() != format {
			return fmt.Errorf("read back %+v at level %d, committed %+v at level %d", got.Disks(), got.Format(), committed, format)
		}
		return nil
	})
	inv.Register(invRecordGone, func() error {
		for _, d := range r.ctl.Disks() {
			// A cloned drive's log is reported again after it is gone, and refused.
			if !d.Gone || slices.Contains(declared, d.ID) || slices.ContainsFunc(reported, func(x drive) bool { return x.log == d.Log && x.dev == clone }) {
				continue
			}
			at := func(x drive) bool { return x.node == d.Node && x.dev == d.Device }
			last := -1
			for i, x := range reported {
				if x.log == d.Log && at(x) {
					last = i
				}
			}
			if !slices.ContainsFunc(reported[last+1:], func(x drive) bool { return x.log != d.Log && at(x) }) {
				return fmt.Errorf("disk %d is gone with no other log reported on device %#x of node %d after its own", d.ID, d.Device, d.Node)
			}
		}
		return nil
	})
	inv.Register(invGoneStays, func() error {
		for _, d := range committed {
			if gone[d.ID] && !d.Gone {
				return fmt.Errorf("disk %d committed gone came back as %+v", d.ID, d)
			}
			gone[d.ID] = d.Gone
		}
		return nil
	})
	inv.Register(invFormatLevel, func() error {
		for _, d := range raisedOver {
			if d.Up && releaseOf(d.Node) < r.ctl.Format() {
				return fmt.Errorf("level %d committed with disk %d up on node %d at release %d", r.ctl.Format(), d.ID, d.Node, releaseOf(d.Node))
			}
		}
		return nil
	})
	report := func(n uint32, opened []OpenedDisk) {
		var out ControlOutput
		if releaseOf(n) >= r.ctl.Format() {
			for _, o := range opened {
				reported = append(reported, drive{n, o.Device, o.Log})
			}
		}
		if err := r.ctl.Heartbeat(r.now, &NodeReport{Node: n, Meta: true, Release: releaseOf(n), Opened: opened}, &out); err != nil && !errors.Is(err, chunkstore.ErrLogRefused) {
			t.Fatal(err)
		}
	}
	in := &ControlInputs{}
	for range 150 {
		d := &drives[rng.IntN(len(drives))]
		switch rng.IntN(11) {
		case 0, 1:
			n := uint32(rng.IntN(6))
			var opened []OpenedDisk
			for _, x := range drives {
				if x.node == n {
					opened = append(opened, OpenedDisk{Device: x.dev, Log: x.log})
				}
			}
			rng.Shuffle(len(opened), func(i, j int) { opened[i], opened[j] = opened[j], opened[i] })
			report(n, opened)
		case 2:
			d.log, nextLog = nextLog, nextLog+1
		case 3:
			d.node = uint32(rng.IntN(6))
		case 4:
			report(uint32(rng.IntN(6)), []OpenedDisk{{Device: clone, Log: d.log}})
		case 5, 6:
			r.event(d.node, chunkstore.AppendReport(nil, &chunkstore.Report{ID: 1, Disks: []chunkstore.DiskState{{Log: d.log, Failed: rng.IntN(4) == 0}}}))
		case 7:
			r.now += core.Tick(rng.IntN(20)) * sec
			r.event(0, nil)
		case 8:
			raisedOver = nil
			if rng.IntN(4) == 0 && r.ctl.SetFormat(r.ctl.Format()+1) == nil {
				format, raisedOver = r.ctl.Format()+1, committed
			}
			r.settle(in)
			committed = r.ctl.Disks()
		case 9:
			r.settle(in)
			committed = r.ctl.Disks()
			r.failover()
		case 10:
			id := uint32(rng.IntN(len(r.ctl.Disks()) + 2))
			switch err := r.ctl.Lost(r.now, id); {
			case err == nil:
				declared = append(declared, id)
				r.settle(in)
				committed = r.ctl.Disks()
			case !errors.Is(err, ErrUnknownDisk) && !errors.Is(err, chunkstore.ErrLostRefused):
				t.Fatal(err)
			}
		}
		if err := inv.Check(); err != nil {
			t.Fatal(err)
		}
	}
}
