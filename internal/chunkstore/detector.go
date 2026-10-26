package chunkstore

import (
	"slices"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// NodeDownAfter is node_down_after: how long the control plane waits for a disk's heartbeat before it marks the disk down.
const NodeDownAfter = core.Tick(5_000_000)

// SlowDiskFactor is slow_disk_factor: how many times its pool's median a disk's acknowledgment latency must reach to count as slow.
const SlowDiskFactor = 10

// SlowDiskWindow is slow_disk_window: how long a disk must stay slow before it is marked down, and stay normal before a disk marked down as slow or stalled rejoins.
const SlowDiskWindow = core.Tick(60_000_000)

// FlapCount is flap_count: the marks within FlapWindow that hold a disk down.
const FlapCount = 5

// FlapWindow is flap_window: the window FlapCount marks are counted in, and how long a disk they hold down must stay normal to rejoin.
const FlapWindow = core.Tick(600_000_000)

// DetectTimer is the timer on which the detector marks down the disks whose heartbeats or probe answers are late.
const DetectTimer core.TimerID = 0x64746374

// latencyFresh is how recent the other disks' latencies must be to judge a disk against them: two heartbeats.
const latencyFresh = 2 * RetryAfter

// downCause is why a disk is marked down, which sets how long it must stay normal to rejoin.
type downCause uint8

const (
	// causeSilent: no heartbeat for node_down_after; it rejoins at its next normal one.
	causeSilent downCause = iota + 1
	// causeFailed: stalled, a failed log, a failed probe or fail-slow; it rejoins after a whole slow_disk_window normal.
	causeFailed
)

// watched is the detector's view of one disk.
type watched struct {
	addr  core.NodeID
	known bool
	heard core.Tick
	down  bool
	since core.Tick
	cause downCause
	// damped holds the disk down until a whole flap_window passes normal; marks are its last FlapCount marks, oldest first.
	damped bool
	marks  []core.Tick
	// normal is set while its heartbeats have been normal since normalFrom; slow while its latency has been slow since slowFrom.
	normal     bool
	normalFrom core.Tick
	slow       bool
	slowFrom   core.Tick
	lat        core.Tick
	latAt      core.Tick
	probe      core.RequestID
	probeAt    core.Tick
}

// Detector is the control-plane leader's failure detection: it marks a disk down when its heartbeats stop for node_down_after, when its oldest unfinished I/O passes member_timeout or its log fails, when a probe sent on a primary's member_timeout report finds it so or goes unanswered for member_timeout, all at once, and when its acknowledgment latency stays above slow_disk_factor times its pool's median for slow_disk_window unless a PG would drop below min_live; it marks it up once it has been normal long enough, holding a flapping disk down.
type Detector struct {
	rec      *DiskRecord
	disks    []watched
	table    *Table
	pools    []PoolInputs
	nextID   uint64
	timerAt  core.Tick
	changes  uint64
	slowKept []uint32
	rep      Report
}

// NewDetector watches the disks of rec from start, the beginning of this leader's lease: a disk not heard from by start plus node_down_after is marked down then.
func NewDetector(rec *DiskRecord, start core.Tick) *Detector {
	d := &Detector{rec: rec}
	d.sync(start)
	return d
}

// View gives the detector the table and pool inputs its min_live rules read.
func (d *Detector) View(t *Table, pools []PoolInputs) { d.table, d.pools = t, pools }

// Mark is the live mark of disk id and, when down, since when.
func (d *Detector) Mark(id uint32) (Mark, core.Tick) {
	if w := d.watch(id); w != nil && w.down {
		return MarkDown, w.since
	}
	return MarkUp, 0
}

// Changes counts the marks the detector has changed.
func (d *Detector) Changes() uint64 { return d.changes }

// Slow lists the disks slow for slow_disk_window that stay up because marking them down would leave a PG below min_live; the pool reports them.
func (d *Detector) Slow() []uint32 { return d.slowKept }

// sync watches every disk the record gained, heard from at now.
func (d *Detector) sync(now core.Tick) {
	for len(d.disks) < len(d.rec.Disks()) {
		d.disks = append(d.disks, watched{heard: now})
	}
}

// watch is disk id's view, nil if the record does not know it or its log is gone.
func (d *Detector) watch(id uint32) *watched {
	if id == 0 || int(id) > len(d.disks) || d.rec.Disks()[id-1].Gone {
		return nil
	}
	return &d.disks[id-1]
}

func (d *Detector) byLog(log uint64) (uint32, *watched) {
	i := slices.IndexFunc(d.rec.Disks(), func(r RecordedDisk) bool { return r.Log == log })
	if i < 0 {
		return 0, nil
	}
	return uint32(i + 1), d.watch(uint32(i + 1))
}

// OnEvent takes a disk's heartbeat, a primary's member_timeout report, a probe's answer or DetectTimer, sends probes and arms DetectTimer for the next deadline.
func (d *Detector) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	d.sync(ctx.Now)
	switch ev.Kind {
	case core.EvNetRecv:
		out = d.recv(ctx, ev, out)
	case core.EvTimer:
		if ev.Timer == DetectTimer {
			d.timerAt = 0
		}
	}
	return d.check(ctx.Now, out)
}

func (d *Detector) recv(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	frame, data := Frame(ctx.Pool, &ev.Msg)
	defer freeBuf(ctx, ev.Msg.Buf)
	kind, err := Peek(frame)
	if err != nil {
		return out
	}
	switch kind {
	case KindReport:
		if DecodeReport(frame, data, &d.rep) == nil {
			d.report(ctx.Now, ev.Peer, &d.rep)
		}
	case KindMemberTimeout:
		var mt MemberTimeout
		if DecodeMemberTimeout(frame, data, &mt) == nil {
			out = d.timedOut(ctx.Now, &mt, out)
		}
	case KindProbeReply:
		var pr ProbeReply
		if DecodeProbeReply(frame, data, &pr) == nil {
			d.probed(ctx.Now, &pr)
		}
	}
	return out
}

// report takes a heartbeat from the loop at peer: each disk it names is heard from, marked down if stalled or failed, and judged for rejoining; each member latency it carries is judged against the pool.
func (d *Detector) report(now core.Tick, peer core.NodeID, rp *Report) {
	for _, s := range rp.Disks {
		id, w := d.byLog(s.Log)
		if w == nil {
			continue
		}
		w.heard, w.addr, w.known = now, peer, true
		if s.Failed || s.Oldest >= MemberTimeoutAfter {
			w.normal = false
			d.markDown(id, w, now, causeFailed)
			continue
		}
		if !w.normal {
			w.normal, w.normalFrom = true, now
		}
		d.rejoin(id, w, now)
	}
	for _, l := range rp.Latencies {
		if w := d.watch(l.Disk); w != nil {
			w.lat, w.latAt = l.Latency, now
			d.judge(l.Disk, w, now)
		}
	}
}

// timedOut probes the disk a primary waited on past member_timeout, unless it is down or a probe of it is out.
func (d *Detector) timedOut(now core.Tick, mt *MemberTimeout, out []core.Action) []core.Action {
	w := d.watch(mt.Disk)
	if w == nil || w.down || w.probe != 0 {
		return out
	}
	d.nextID++
	w.probe, w.probeAt = core.RequestID(d.nextID), now
	if !w.known {
		return out
	}
	a := core.Action{Kind: core.ActNetSend, Peer: w.addr}
	a.Msg.Len = uint16(copy(a.Msg.Body[:], AppendProbe(nil, &Probe{ID: w.probe, Disk: mt.Disk})))
	return append(out, a)
}

// probed takes a probe's answer: a disk stalled, failed or holding another log is marked down at once.
func (d *Detector) probed(now core.Tick, pr *ProbeReply) {
	for i := range d.disks {
		id := uint32(i + 1)
		w := d.watch(id)
		if w == nil || w.probe != pr.ID || pr.ID == 0 {
			continue
		}
		w.probe = 0
		if pr.Failed || pr.Oldest >= MemberTimeoutAfter || pr.Log != d.rec.Disks()[i].Log {
			d.markDown(id, w, now, causeFailed)
		}
		return
	}
}

// check marks down every disk silent for node_down_after or whose probe went unanswered for member_timeout, and arms DetectTimer for the next such deadline.
func (d *Detector) check(now core.Tick, out []core.Action) []core.Action {
	next := core.Tick(0)
	soonest := func(t core.Tick) {
		if next == 0 || t < next {
			next = t
		}
	}
	for i := range d.disks {
		id := uint32(i + 1)
		w := d.watch(id)
		if w == nil {
			continue
		}
		if w.probe != 0 {
			if now-w.probeAt >= MemberTimeoutAfter {
				w.probe = 0
				d.markDown(id, w, now, causeFailed)
			} else {
				soonest(w.probeAt + MemberTimeoutAfter)
			}
		}
		if now-w.heard >= NodeDownAfter {
			w.normal = false
			d.markDown(id, w, now, causeSilent)
		} else if !w.down {
			soonest(w.heard + NodeDownAfter)
		}
	}
	if next != 0 && (d.timerAt == 0 || next < d.timerAt) {
		d.timerAt = next
		out = append(out, core.Action{Kind: core.ActSetTimer, Timer: DetectTimer, At: next})
	}
	return out
}

// markDown marks disk id down for cause at now, holding it down if this is its FlapCount-th mark within FlapWindow; a disk already down keeps its since and takes the longer cause.
func (d *Detector) markDown(id uint32, w *watched, now core.Tick, cause downCause) {
	if w.down {
		w.cause = max(w.cause, cause)
		return
	}
	w.down, w.since, w.cause, w.slow, w.normal, w.probe = true, now, cause, false, false, 0
	if len(w.marks) == FlapCount {
		w.marks = w.marks[1:]
	}
	w.marks = append(w.marks, now)
	if len(w.marks) == FlapCount && now-w.marks[0] <= FlapWindow {
		w.damped = true
	}
	d.slowKept = slices.DeleteFunc(d.slowKept, func(x uint32) bool { return x == id })
	d.rec.Down(id)
	d.changes++
}

// rejoin marks a down disk up once it has been normal for its cause's wait, or for flap_window if damped, unless a PG it belongs to would otherwise stay below min_live with no disk to refill onto, which lifts the damping.
func (d *Detector) rejoin(id uint32, w *watched, now core.Tick) {
	if !w.down || !w.normal {
		return
	}
	if w.damped && now-w.normalFrom < FlapWindow && !d.needed(id, now) {
		return
	}
	if w.cause == causeFailed && now-w.normalFrom < SlowDiskWindow {
		return
	}
	w.down, w.damped, w.cause = false, false, 0
	d.rec.disks[id-1].Up = true
	d.changes++
}

// judge marks an up disk down once its latency has stayed above SlowDiskFactor times the median of its pool's other disks for SlowDiskWindow, unless that would leave a PG below min_live.
func (d *Detector) judge(id uint32, w *watched, now core.Tick) {
	if w.down {
		return
	}
	if !d.slower(id, w, now) {
		w.slow = false
		d.slowKept = slices.DeleteFunc(d.slowKept, func(x uint32) bool { return x == id })
		return
	}
	if !w.slow {
		w.slow, w.slowFrom = true, now
	}
	if now-w.slowFrom < SlowDiskWindow {
		return
	}
	if d.keepsMinLive(id) {
		d.markDown(id, w, now, causeFailed)
	} else if !slices.Contains(d.slowKept, id) {
		d.slowKept = append(d.slowKept, id)
	}
}

// slower reports whether w's latency is above SlowDiskFactor times the lower median of the fresh latencies of the other up disks of some pool it is in.
func (d *Detector) slower(id uint32, w *watched, now core.Tick) bool {
	var lats []core.Tick
	for _, p := range d.pools {
		if !slices.ContainsFunc(p.Disks, func(x Disk) bool { return x.ID == id }) {
			continue
		}
		lats = lats[:0]
		for _, x := range p.Disks {
			if o := d.watch(x.ID); x.ID != id && o != nil && !o.down && o.latAt != 0 && now-o.latAt <= latencyFresh {
				lats = append(lats, o.lat)
			}
		}
		if len(lats) == 0 {
			continue
		}
		slices.Sort(lats)
		if w.lat > SlowDiskFactor*lats[(len(lats)-1)/2] {
			return true
		}
	}
	return false
}

// keepsMinLive reports whether marking disk id down leaves every PG it is a live member of with at least min_live live members.
func (d *Detector) keepsMinLive(id uint32) bool {
	if d.table == nil {
		return true
	}
	for pi := range d.table.Pools {
		p := &d.table.Pools[pi]
		for i := range p.PGs {
			e := &p.PGs[i]
			if slices.ContainsFunc(e.Members, func(m Member) bool { return m.Disk == id && !m.Leaving && m.Mark != MarkDown }) && e.live()-1 < int(p.MinLive) {
				return false
			}
		}
	}
	return true
}

// needed reports whether a PG disk id belongs to is below min_live with no eligible disk to refill it onto.
func (d *Detector) needed(id uint32, now core.Tick) bool {
	if d.table == nil {
		return false
	}
	for pi := range d.table.Pools {
		p := &d.table.Pools[pi]
		in := d.poolInputs(p.ID)
		for i := range p.PGs {
			e := &p.PGs[i]
			j := slices.IndexFunc(e.Members, func(m Member) bool { return m.Disk == id && !m.Leaving })
			if j >= 0 && e.live() < int(p.MinLive) && (in == nil || !refillable(in, e, j, now)) {
				return true
			}
		}
	}
	return false
}

func (d *Detector) poolInputs(id core.PoolID) *PoolInputs {
	for i := range d.pools {
		if d.pools[i].ID == id {
			return &d.pools[i]
		}
	}
	return nil
}
