package chunkstore

import (
	"math/bits"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// MemberTimeoutAfter is member_timeout: how long a disk's oldest unfinished I/O, or a primary's wait for a member's acknowledgment, may last before the disk counts as stalled.
const MemberTimeoutAfter = core.Tick(1_000_000)

// ReportTimer is the timer on which a replica sends the authority its heartbeat.
const ReportTimer core.TimerID = 0x72707274

// WaitTimer is the timer on which a primary looks for members that kept it waiting past member_timeout.
const WaitTimer core.TimerID = 0x77616974

// latBuckets is the acknowledgment-latency histogram's buckets: 2 per octave of microseconds, up to 2^34.
const latBuckets = 68

// latHist is the acknowledgments a member disk gave since the last report, by latency bucket.
type latHist struct {
	n    uint32
	hist [latBuckets]uint32
}

func latBucket(t core.Tick) int {
	v := uint64(min(t, 1<<34-1))
	if v < 4 {
		return int(v)
	}
	e := bits.Len64(v) - 2
	return e*2 + int(v>>e)
}

func latFloor(i int) core.Tick {
	if i < 4 {
		return core.Tick(i)
	}
	return core.Tick(uint64(i%2+2) << (i/2 - 1))
}

// p99 is the lower bound of the bucket holding the 99th percentile.
func (m *latHist) p99() core.Tick {
	rank := (99*uint64(m.n) + 99) / 100
	var sum uint64
	for i, c := range m.hist {
		if sum += uint64(c); sum >= rank {
			return latFloor(i)
		}
	}
	return latFloor(latBuckets - 1)
}

// SetAuthority has the replica heartbeat the control plane at addr every RetryAfter, from now on, with its disk's state and, as a primary, its members' acknowledgment latencies, and report to it every member that keeps it waiting past member_timeout.
func (r *Replica) SetAuthority(ctx *core.Context, addr core.NodeID, out *ReplicaOutput) {
	r.auth, r.reporting = addr, true
	r.report(ctx, out)
}

// diskState is what the owner loop holds in memory of its disk.
func (r *Replica) diskState(ctx *core.Context) DiskState {
	return DiskState{Log: r.cfg.Log, Fill: r.fill(ctx), Oldest: r.st.Oldest(ctx.Now), Failed: r.st.Err() != nil}
}

// report sends the authority a heartbeat and arms the next.
func (r *Replica) report(ctx *core.Context, out *ReplicaOutput) {
	rp := &r.rep
	r.reportID++
	rp.ID, rp.Disks, rp.Latencies = core.RequestID(r.reportID), append(rp.Disks[:0], r.diskState(ctx)), rp.Latencies[:0]
	for id := range r.disks {
		if m := &r.disks[id].lat; m.n > 0 {
			rp.Latencies = append(rp.Latencies, Latency{Disk: uint32(id), Latency: m.p99()})
			*m = latHist{}
		}
	}
	r.frame = AppendReport(r.frame[:0], rp)
	a := core.Action{Kind: core.ActNetSend, Peer: r.auth}
	if Attach(ctx.Pool, &a.Msg, r.frame, core.NoBuf) == nil {
		out.Actions = append(out.Actions, a)
	}
	out.Actions = append(out.Actions, core.Action{Kind: core.ActSetTimer, Timer: ReportTimer, At: ctx.Now + RetryAfter})
}

// probe answers the authority's probe from memory, never waiting on the disk.
func (r *Replica) probe(ctx *core.Context, peer core.NodeID, p *Probe, out *ReplicaOutput) {
	s := r.diskState(ctx)
	r.frame = AppendProbeReply(r.frame[:0], &ProbeReply{ID: p.ID, Log: s.Log, Oldest: s.Oldest, Failed: s.Failed})
	a := core.Action{Kind: core.ActNetSend, Peer: peer}
	a.Msg.Len = uint16(copy(a.Msg.Body[:], r.frame))
	out.Actions = append(out.Actions, a)
}

// noteAck records how long a member disk took to acknowledge a version.
func (r *Replica) noteAck(disk uint32, took core.Tick) {
	if r.reporting {
		m := &r.disk(disk).lat
		m.n++
		m.hist[latBucket(took)]++
	}
}

// armWait arms WaitTimer for when a version just sent would keep its primary waiting past member_timeout, unless it is armed.
func (r *Replica) armWait(ctx *core.Context, out *ReplicaOutput) {
	if r.reporting && r.waitAt == 0 {
		r.waitAt = ctx.Now + MemberTimeoutAfter
		out.Actions = append(out.Actions, core.Action{Kind: core.ActSetTimer, Timer: WaitTimer, At: r.waitAt})
	}
}

// waited reports to the authority each member disk that has kept a version waiting for its acknowledgment past member_timeout, once per member_timeout for as long as it does, and arms WaitTimer for the next such deadline.
func (r *Replica) waited(ctx *core.Context, out *ReplicaOutput) {
	if r.waitAt == 0 || ctx.Now < r.waitAt {
		return
	}
	r.waitAt = 0
	next := core.Tick(0)
	for i := range r.vers {
		v := &r.vers[i]
		if v.done || v.sentAt == 0 {
			continue
		}
		for k := range v.n {
			if v.acked&(1<<k) != 0 {
				continue
			}
			due := v.sentAt + MemberTimeoutAfter
			if due <= ctx.Now {
				r.timedOut(ctx, v, v.members[k], out)
				due = r.disk(v.members[k]).waited + MemberTimeoutAfter
			}
			if next == 0 || due < next {
				next = due
			}
		}
	}
	if next != 0 {
		r.waitAt = next
		out.Actions = append(out.Actions, core.Action{Kind: core.ActSetTimer, Timer: WaitTimer, At: next})
	}
}

func (r *Replica) timedOut(ctx *core.Context, v *version, disk uint32, out *ReplicaOutput) {
	di := r.disk(disk)
	if di.waited != 0 && ctx.Now-di.waited < MemberTimeoutAfter {
		return
	}
	di.waited = ctx.Now
	r.reportID++
	mt := MemberTimeout{ID: core.RequestID(r.reportID), PG: PGFields{Epoch: r.table.Epoch, PG: v.u.PG.PG, Interval: uint32(v.u.Version >> 32)}, Disk: disk}
	r.frame = AppendMemberTimeout(r.frame[:0], &mt)
	a := core.Action{Kind: core.ActNetSend, Peer: r.auth}
	a.Msg.Len = uint16(copy(a.Msg.Body[:], r.frame))
	out.Actions = append(out.Actions, a)
}
