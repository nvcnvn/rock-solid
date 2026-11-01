package chunkstore

import (
	"cmp"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/lease"
)

// noteGrantors lists the loops this disk needs leases from: the other up members of each PG it leads.
func (r *Replica) noteGrantors() {
	r.grantors = r.grantors[:0]
	for _, l := range r.leads {
		_, e := r.table.entry(l.pg)
		for _, m := range e.Members {
			if m.Mark != MarkUp || m.Disk == r.cfg.Disk {
				continue
			}
			a := r.addr(m.Disk)
			if i, ok := slices.BinarySearchFunc(r.grantors, a, compareNode); !ok {
				r.grantors = slices.Insert(r.grantors, i, a)
			}
		}
	}
}

func compareNode(a, b core.NodeID) int {
	return cmp.Or(cmp.Compare(a.Node, b.Node), cmp.Compare(a.Loop, b.Loop))
}

// readable reports whether this disk, primary of the PG in e, may serve its reads now: the interval has min_live live members, maintenance counting, and the disk holds a lease from every other up member.
func (r *Replica) readable(ctx *core.Context, p *PoolTable, e *PGEntry) bool {
	if e.live() < int(p.MinLive) && !r.plant.readBelowMinLive {
		return false
	}
	if r.plant.noLeaseCheck {
		return true
	}
	t := lease.Now(ctx)
	for _, m := range e.Members {
		if m.Mark == MarkUp && m.Disk != r.cfg.Disk && !r.holder.Valid(r.addr(m.Disk), t) {
			return false
		}
	}
	return true
}

// leaseRequest answers a holder's request as a grantor: a grant, renewing the pair's support epoch while the last grant runs, if the holder reported a table at least as new and the restart wait is over, else a refusal naming this disk's table epoch.
func (r *Replica) leaseRequest(ctx *core.Context, peer core.NodeID, lr *LeaseRequest, out *ReplicaOutput) {
	r.see(peer, lr.Epoch)
	t := lease.Now(ctx)
	g := Grant{ID: lr.ID, Status: StatusRefused, Epoch: r.table.Epoch, Run: r.grantor.Run(), Raw: ctx.Raw}
	reported := lr.Epoch
	if r.plant.freshGrantNoCheck && !r.grantor.Live(peer, t) {
		reported = max(reported, r.table.Epoch)
	}
	if sup, ok := r.grantor.Grant(peer, reported, r.table.Epoch, t); ok {
		g.Status, g.Epoch, g.Support = StatusOK, 0, sup
		if r.beatAt == 0 {
			r.beatAt = ctx.Now + r.lcfg.Heartbeat
		}
	}
	if lr.Epoch > r.table.Epoch {
		r.fetch(lr.Epoch, false, out)
	}
	r.frame = AppendGrant(r.frame[:0], &g)
	r.sendFrame(peer, out)
	r.armLease(ctx, out)
}

// grant takes a grantor's answer: a grant feeds the holder's window and lease, a refusal naming a newer table fetches it.
func (r *Replica) grant(ctx *core.Context, peer core.NodeID, g *Grant, out *ReplicaOutput) {
	if g.Status != StatusOK {
		if g.Epoch > r.table.Epoch {
			r.fetch(g.Epoch, false, out)
		}
		return
	}
	r.holder.OnGrant(peer, lease.Grant{ID: g.ID, Run: g.Run, Raw: g.Raw, Support: g.Support}, lease.Now(ctx))
	r.armLease(ctx, out)
}

// heartbeat takes a grantor's heartbeat: the holder's step check, and a new run's or a lower raw reading's end of the lease.
func (r *Replica) heartbeat(ctx *core.Context, peer core.NodeID, hb *Heartbeat, out *ReplicaOutput) {
	r.holder.OnHeartbeat(peer, hb.Run, hb.Raw, lease.Now(ctx))
	if hb.Epoch > r.table.Epoch {
		r.fetch(hb.Epoch, false, out)
	}
	r.armLease(ctx, out)
}

// resumed ends every lease this disk holds and draws a new run ID for its grants, which stand.
func (r *Replica) resumed(ctx *core.Context, out *ReplicaOutput) {
	r.holder.Resumed()
	r.grantor.Resumed(r.rng.Uint64())
	r.armLease(ctx, out)
}

// leaseTick asks each grantor due a request, none during the restart wait, and, once a heartbeat is due, heartbeats every holder whose grant runs; a stale firing does nothing.
func (r *Replica) leaseTick(ctx *core.Context, out *ReplicaOutput) {
	if r.leaseAt == 0 || ctx.Now < r.leaseAt {
		return
	}
	r.leaseAt = 0
	t := lease.Now(ctx)
	for _, g := range r.grantors {
		if r.grantor.Waiting(t) || !r.holder.Due(g, t) {
			continue
		}
		r.leaseID++
		id := core.RequestID(r.leaseID)
		r.holder.Ask(g, id, t)
		r.frame = AppendLeaseRequest(r.frame[:0], &LeaseRequest{ID: id, Epoch: r.table.Epoch})
		r.sendFrame(g, out)
	}
	if r.beatAt != 0 && ctx.Now >= r.beatAt {
		r.holders = r.grantor.Granting(r.holders[:0], t)
		r.beatAt = 0
		if len(r.holders) > 0 {
			r.beatAt = ctx.Now + r.lcfg.Heartbeat
		}
		for _, h := range r.holders {
			r.frame = AppendHeartbeat(r.frame[:0], &Heartbeat{Epoch: r.table.Epoch, Run: r.grantor.Run(), Raw: ctx.Raw})
			r.sendFrame(h, out)
		}
	}
	r.armLease(ctx, out)
}

// armLease arms LeaseTimer for the earliest of the grantors' next asks, no earlier than the end of the restart wait, and the next heartbeat, unless it is armed sooner.
func (r *Replica) armLease(ctx *core.Context, out *ReplicaOutput) {
	t := lease.Now(ctx)
	at := r.beatAt
	for _, g := range r.grantors {
		n := r.holder.Next(g, t)
		if r.grantor.Waiting(t) {
			n = max(n, r.grantor.WaitEnd(t))
		}
		if at == 0 || n < at {
			at = n
		}
	}
	if at == 0 || r.leaseAt != 0 && r.leaseAt <= at {
		return
	}
	r.leaseAt = at
	out.Actions = append(out.Actions, core.Action{Kind: core.ActSetTimer, Timer: LeaseTimer, At: at})
}

func (r *Replica) sendFrame(peer core.NodeID, out *ReplicaOutput) {
	a := core.Action{Kind: core.ActNetSend, Peer: peer}
	a.Msg.Len = uint16(copy(a.Msg.Body[:], r.frame))
	out.Actions = append(out.Actions, a)
}
