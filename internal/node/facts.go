package node

import (
	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/lease"
	"github.com/nvcnvn/rock-solid/internal/meta"
	"github.com/nvcnvn/rock-solid/internal/ops"
)

// grow extends s by one element, keeping the arrays of the one a reset left in that slot, so a
// part filled again allocates nothing.
func grow[T any](s []T) ([]T, *T) {
	if len(s) < cap(s) {
		s = s[:len(s)+1]
	} else {
		var zero T
		s = append(s, zero)
	}
	return s, &s[len(s)-1]
}

// Facts copies the node machine's part of the node's facts into f: the node, its cluster map and session.
func (n *Node) Facts(ctx *core.Context, f *ops.Facts) {
	f.Node.ID, f.Node.Release, f.Node.BootUs, f.Node.RawUs = n.cfg.Node, n.cfg.Release, ctx.Now, ctx.Raw
	switch {
	case n.cfg.Client:
		f.Node.Roles = append(f.Node.Roles, "client")
	case n.cfg.MetaOnly:
		f.Node.Roles = append(f.Node.Roles, "meta")
	default:
		f.Node.Roles = append(f.Node.Roles, "storage")
	}
	if n.cfg.Session && !n.cfg.Client {
		f.Node.Roles = append(f.Node.Roles, "mount")
	}
	f.Map = ops.Map{Epoch: n.epoch, Sessions: n.table.Version, ShardMap: n.mapV, ReportsTo: n.Ctl().Node}
	if n.sess != nil {
		inc, left, age := n.sess.Held(lease.Now(ctx))
		f.Session = ops.Session{ID: n.sess.Session(), Incarnation: inc, LeaseLeftUs: left, RenewalAgeUs: age}
	}
}

var phases = [...]string{recovering: "recovering", formatting: "formatting", opened: "open", failed: "failed"}

// Facts copies the device's part of the node's facts into f: its disk, PG members and MetaStore replicas.
func (d *Device) Facts(ctx *core.Context, f *ops.Facts) {
	var k *ops.Disk
	f.Disks, k = grow(f.Disks)
	*k = ops.Disk{Loop: d.cfg.Loop, Device: d.cfg.Device, State: phases[d.phase], Log: d.opened.Log, Blocks: d.opened.Blocks,
		Reserve: d.opened.Reserve, Epoch: d.epoch, Sessions: d.held, MemberAckUs: k.MemberAckUs[:0]}
	if d.st != nil {
		k.OldestIOUs = d.st.Oldest(ctx.Now)
	}
	if r := d.rep; r != nil {
		k.Disk, k.Fill, k.Run = r.Disk(), r.Fill(), r.Run()
		for _, l := range r.Reported().Latencies {
			k.MemberAckUs = append(k.MemberAckUs, ops.Latency{Disk: l.Disk, Us: l.Latency})
		}
		f.Members = members(r, f.Members, func(core.PGID) bool { return true })
	}
	for i := range d.metas {
		if sh := d.running(i); sh != nil {
			f.Replicas = replicaFacts(ctx, sh, d.cfg.Loop, d.halts, f.Replicas)
		}
	}
}

// PGFacts copies into p the device's member of PG p.Pool.p.PG, false when its table holds no such PG.
func (d *Device) PGFacts(p *ops.PGFacts) bool {
	r := d.rep
	if r == nil {
		return false
	}
	pg := core.PGID{Pool: core.PoolID(p.Pool), Index: p.PG}
	p.Members = members(r, p.Members, func(x core.PGID) bool { return x == pg })
	for _, m := range p.Members {
		for _, c := range r.UnfoundChunks(pg, m.Interval, nil) {
			p.Unfound = append(p.Unfound, ops.Chunk{Volume: uint32(c.Volume), Inode: uint64(c.Inode), Index: c.Index})
		}
	}
	for _, t := range r.Table().Pools {
		if t.ID == pg.Pool {
			return int(pg.Index) < len(t.PGs)
		}
	}
	return false
}

// members appends the PG members r's table names its disk in, of the PGs want takes, each with
// what r holds of it as primary.
func members(r *chunkstore.Replica, dst []ops.Member, want func(core.PGID) bool) []ops.Member {
	disk := r.Disk()
	for _, p := range r.Table().Pools {
		for i := range p.PGs {
			pg := core.PGID{Pool: p.ID, Index: uint32(i)}
			if !want(pg) {
				continue
			}
			e := &p.PGs[i]
			for _, m := range e.Members {
				if m.Disk != disk {
					continue
				}
				mf := ops.Member{Disk: disk, Pool: uint32(p.ID), PG: uint32(i), Interval: e.Interval, Role: "member"}
				if m.Leaving {
					mf.Role = "leaving"
				}
				if l, ok := r.Lead(pg, e.Interval); ok {
					mf.Role, mf.Active, mf.Unfound, mf.Lacking = "primary", l.Active, l.Unfound, l.Lacking
				}
				dst = append(dst, mf)
			}
		}
	}
	return dst
}

// replicaFacts appends replica sh's facts, hosted on loop, its halt's age from halts.
func replicaFacts(ctx *core.Context, sh *meta.Shard, loop uint32, halts []meta.Halt, dst []ops.Replica) []ops.Replica {
	var r *ops.Replica
	dst, r = grow(dst)
	group, id := sh.Name()
	st := sh.Raft().Status()
	*r = ops.Replica{Group: group, ID: id, Loop: loop, Term: st.Term, Leader: st.Lead, Commit: st.Commit, Applied: st.Applied,
		Lease: sh.Raft().LeaseRead(ctx), HaltIndex: sh.Halted(), Followers: r.Followers[:0]}
	for _, h := range halts {
		if h.Group == group && h.ID == id && h.Index == r.HaltIndex {
			r.HaltAgeUs = ctx.Now - min(h.Since, ctx.Now)
		}
	}
	sh.Raft().Matches(func(id, match uint64) { r.Followers = append(r.Followers, ops.Follower{ID: id, Match: match}) })
	return dst
}

// CtlFacts copies a control-plane voter's facts into f: its replica and, while it leads, its cluster map.
func CtlFacts(ctx *core.Context, h *meta.CtlHost, node, release uint32, f *ops.Facts) {
	f.Node.ID, f.Node.Release, f.Node.BootUs, f.Node.RawUs = node, release, ctx.Now, ctx.Raw
	f.Node.Roles = append(f.Node.Roles, "control-plane")
	if h == nil {
		return
	}
	if sh := h.Shard(); sh != nil {
		f.Replicas = replicaFacts(ctx, sh, 0, nil, f.Replicas)
	}
	if c := h.Control(); c != nil {
		f.Map = ops.Map{Epoch: c.Table().Epoch, Sessions: c.SessionTable().Version, Format: c.Format(), ReportsTo: node}
	}
}
