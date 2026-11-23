package chunkstore_test

import (
	"slices"
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/lease"
	"github.com/nvcnvn/rock-solid/internal/loop/looptest"
	"github.com/nvcnvn/rock-solid/internal/lsm"
	"github.com/nvcnvn/rock-solid/internal/meta"
	"github.com/nvcnvn/rock-solid/internal/raftshard"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

func init() {
	chunkstore.NewControlPlane = func(t *testing.T) chunkstore.ControlPlane { return newCtlGroup(t) }
}

// ctlGroup is meta.Control at the leader of a three-replica control-plane group, which it runs by
// hand as their loops would: each message between replicas arrives the simulator's least network
// delay after it is sent, each flush takes a disk's write and flush, each replica ticks every
// TickInterval, and their node-pair leases renew every HeartbeatTick. On the control plane's node
// the group's clock runs off from the node's, and the Control steps every StepInterval, holding
// its lease as the leader does, so its commits take the group's time.
type ctlGroup struct {
	t       *testing.T
	now     core.Tick
	off     core.Tick
	tickAt  core.Tick
	renewAt core.Tick
	stepAt  core.Tick
	n       []*ctlReplica
	work    []ctlWork
	l       int
	ctl     *meta.Control
	token   uint64
	// waiting is the token of the commit in flight, 0 for none, and sent the edits its step took.
	waiting uint64
	sent    []chunkstore.Edit
	edits   []chunkstore.Edit
	moves   int
	leaseID uint64
	out     meta.ControlOutput
	asks    []chunkstore.PGFields
}

type ctlReplica struct {
	s       *meta.Shard
	pages   looptest.Pages
	pool    *sim.Pool
	out     meta.Output
	answers map[uint64]meta.Answer
	holder  *lease.Holder
	grantor *lease.Grantor
}

// ctlWork is a message from replica from to replica to, or to's flush of round flush, due at at.
type ctlWork struct {
	at       core.Tick
	from, to int
	msg      core.Msg
	flush    uint64
}

const ctlGroupID = 5

var (
	netDelay   = sim.DefaultSettings().NetDelay
	flushDelay = sim.DefaultDiskLatency.Write + sim.DefaultDiskLatency.Flush
)

// diskBlocks is the size each disk reports, 1 TB.
const diskBlocks = uint64(1e12) / core.BlockSize

func newCtlGroup(t *testing.T) *ctlGroup {
	g := &ctlGroup{t: t, now: 5_000_000}
	g.tickAt, g.renewAt = g.now+raftshard.TickInterval, g.now
	cs := raftpb.ConfState{Voters: []uint64{1, 2, 3}}
	for range 3 {
		g.n = append(g.n, &ctlReplica{pool: sim.NewPool(64<<20, 0), answers: map[uint64]meta.Answer{}})
	}
	for i, r := range g.n {
		st := raftshard.NewStorage()
		if err := st.ApplySnapshot(raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 1, Term: 1, ConfState: cs}}); err != nil {
			t.Fatal(err)
		}
		cfg := meta.Config{Raft: raftshard.Config{Group: ctlGroupID, ID: uint64(i) + 1, Storage: st, Seed: uint64(i), Timer: 1}, Tree: lsm.Config{MaxPiece: 64 << 10, Check: true, Pages: &r.pages}}
		for j := range g.n {
			cfg.Raft.Peers = append(cfg.Raft.Peers, raftshard.Peer{ID: uint64(j) + 1, Node: core.NodeID{Node: uint32(j)}})
		}
		r.holder, r.grantor = lease.NewHolder(lease.DefaultConfig), lease.NewGrantor(lease.DefaultConfig, uint64(i)+1, lease.Time{})
		cfg.Raft.Holder, cfg.Raft.Grantor = r.holder, r.grantor
		var err error
		if r.s, err = meta.NewShard(cfg); err != nil {
			t.Fatal(err)
		}
		g.call(i, func(ctx *core.Context, out *meta.Output) { r.s.Start(ctx, out) })
	}
	g.l = -1
	for range 1000 {
		g.advance(g.now + raftshard.TickInterval)
		if g.l < 0 {
			for i, r := range g.n {
				if r.s.Raft().Leader() == uint64(i)+1 {
					g.l = i
				}
			}
		}
		if g.l >= 0 && g.leased() {
			var err error
			if g.ctl, err = meta.NewControl(g.n[g.l].s.State(), g.n[g.l].s.Raft().Term()); err != nil {
				t.Fatal(err)
			}
			return g
		}
	}
	t.Fatal("the control-plane group elected no leader holding its lease")
	return nil
}

func (g *ctlGroup) ctx(i int) core.Context {
	return core.Context{Now: g.now, Raw: g.now, Pool: g.n[i].pool}
}

func (g *ctlGroup) leased() bool {
	ctx := g.ctx(g.l)
	return g.n[g.l].s.Raft().LeaseRead(&ctx)
}

func (g *ctlGroup) call(i int, f func(ctx *core.Context, out *meta.Output)) {
	r := g.n[i]
	ctx := g.ctx(i)
	r.out.Reset()
	f(&ctx, &r.out)
	for _, a := range r.out.Answers {
		r.answers[a.Token] = a
	}
	if round := r.out.Raft.Flush; round != 0 {
		g.add(ctlWork{at: g.now + flushDelay, from: i, to: i, flush: round})
	}
	for _, a := range r.out.Raft.Actions {
		if a.Kind != core.ActNetSend {
			continue
		}
		w := ctlWork{at: g.now + netDelay, from: i, to: int(a.Peer.Node), msg: a.Msg}
		if a.Msg.Buf != core.NoBuf {
			to := g.n[w.to].pool
			buf, err := to.Alloc(len(r.pool.Bytes(a.Msg.Buf)))
			if err != nil {
				g.t.Fatal(err)
			}
			copy(to.Bytes(buf), r.pool.Bytes(a.Msg.Buf))
			r.pool.Free(a.Msg.Buf)
			w.msg.Buf = buf
		}
		g.add(w)
	}
}

func (g *ctlGroup) add(w ctlWork) {
	i := slices.IndexFunc(g.work, func(o ctlWork) bool { return o.at > w.at })
	if i < 0 {
		i = len(g.work)
	}
	g.work = slices.Insert(g.work, i, w)
}

// advance runs the group to its time to: each message, flush, tick and lease renewal at its time,
// and the Control's answer as soon as the leader gives it.
func (g *ctlGroup) advance(to core.Tick) {
	for {
		at := g.tickAt
		if len(g.work) > 0 {
			at = min(at, g.work[0].at)
		}
		if at > to {
			break
		}
		g.now = at
		if len(g.work) > 0 && g.work[0].at == at {
			w := g.work[0]
			g.work = g.work[1:]
			g.call(w.to, func(ctx *core.Context, out *meta.Output) {
				if w.flush != 0 {
					g.n[w.to].s.Durable(ctx, w.flush, out)
				} else {
					g.n[w.to].s.OnEvent(ctx, &core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{Node: uint32(w.from)}, Msg: w.msg}, out)
				}
			})
		} else {
			g.tickAt += raftshard.TickInterval
			if g.now >= g.renewAt {
				g.renew()
			}
			for i := range g.n {
				g.call(i, func(ctx *core.Context, out *meta.Output) {
					g.n[i].s.OnEvent(ctx, &core.Event{Kind: core.EvTimer, Timer: 1}, out)
				})
			}
		}
		g.answer()
	}
	g.now = max(g.now, to)
}

// renew has every replica renew its node-pair lease from every other, twice rate_gap apart the
// first time.
func (g *ctlGroup) renew() {
	g.renewAt = g.now + raftshard.HeartbeatTick*raftshard.TickInterval
	at := []core.Tick{g.now}
	if g.leaseID == 0 {
		at = []core.Tick{g.now - lease.DefaultConfig.RateGap, g.now}
	}
	for _, now := range at {
		t := lease.Time{Boot: now, Raw: now}
		for h, hr := range g.n {
			for k, gr := range g.n {
				if h == k {
					continue
				}
				g.leaseID++
				id := core.RequestID(g.leaseID)
				hr.holder.Ask(core.NodeID{Node: uint32(k)}, id, t)
				if sup, ok := gr.grantor.Grant(core.NodeID{Node: uint32(h)}, 1, 1, t); ok {
					hr.holder.OnGrant(core.NodeID{Node: uint32(k)}, lease.Grant{ID: id, Run: gr.grantor.Run(), Raw: t.Raw, Support: sup}, t)
				}
			}
		}
	}
}

// propose steps the Control at now, the control plane node's clocks, with the edits and moves
// queued, and proposes its Txn, unless a commit is in flight; it reports whether it proposed.
func (g *ctlGroup) propose(now, raw core.Tick) bool {
	if g.waiting != 0 {
		return false
	}
	g.token++
	g.out.Reset()
	in := meta.ControlInputs{Now: now, Raw: raw, Leased: g.leased(), Disks: g.ctl.AppendDisks(nil), Edits: g.edits, Moves: g.moves}
	if err := g.ctl.Step(&in, g.token, &g.out); err != nil {
		g.t.Fatalf("the control plane's step: %v", err)
	}
	if !g.out.Propose {
		g.edits = nil
		return false
	}
	g.waiting, g.sent, g.edits = g.out.Token, g.edits, nil
	t := g.out.Txn
	g.call(g.l, func(ctx *core.Context, out *meta.Output) {
		if err := g.n[g.l].s.Txn(ctx, &t, g.waiting, out); err != nil {
			g.t.Fatalf("the leader refused its machine's Txn: %v", err)
		}
	})
	return true
}

// answer hands the Control the leader's answer to the commit in flight, once given. A session
// turn takes no edit, so the edits its step was given wait for the next.
func (g *ctlGroup) answer() {
	if g.waiting == 0 {
		return
	}
	a, ok := g.n[g.l].answers[g.waiting]
	if !ok {
		return
	}
	delete(g.n[g.l].answers, g.waiting)
	g.waiting = 0
	g.out.Reset()
	if err := g.ctl.Answer(g.n[g.l].s.State(), &a, &g.out); err != nil {
		g.t.Fatalf("the control plane's answer: %v", err)
	}
	if a.Err != nil || a.Result.FailedCheck >= 0 {
		g.t.Fatalf("the control plane's commit answered %+v", a)
	}
	if !g.out.Push {
		g.edits = append(g.sent, g.edits...)
	}
	g.sent = nil
}

func (g *ctlGroup) Start(ctx *core.Context, out []core.Action) []core.Action {
	g.off, g.stepAt = g.now-ctx.Now, ctx.Now
	return append(out, core.Action{Kind: core.ActSetTimer, Timer: chunkstore.ControlTimer, At: ctx.Now})
}

func (g *ctlGroup) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	g.advance(ctx.Now + g.off)
	switch {
	case ev.Kind == core.EvTimer && ev.Timer == chunkstore.ControlTimer:
		if ctx.Now >= g.stepAt {
			g.stepAt = ctx.Now + meta.StepInterval
			g.propose(ctx.Now, ctx.Raw)
		}
		at := g.stepAt
		if g.waiting != 0 && len(g.work) > 0 {
			at = min(at, g.work[0].at-g.off)
		}
		return append(out, core.Action{Kind: core.ActSetTimer, Timer: chunkstore.ControlTimer, At: at})
	case ev.Kind == core.EvNetRecv && ev.Msg.Group():
		var err error
		if _, k, _, ok := meta.Peek(&ev.Msg); ok && k == meta.KindAsks {
			if g.asks, err = meta.DecodeAsks(ctx.Pool, &ev.Msg, g.asks); err == nil {
				g.ctl.Asked(ev.Peer.Node, g.asks)
			}
		}
		if ev.Msg.Buf != core.NoBuf {
			ctx.Pool.Free(ev.Msg.Buf)
		}
		return out
	}
	return g.ctl.OnEvent(ctx, ev, out)
}

func (g *ctlGroup) EncodeAsks(pool core.BufPool, fs []chunkstore.PGFields, msg *core.Msg) error {
	return meta.EncodeAsks(pool, ctlGroupID, fs, msg)
}

func (g *ctlGroup) Heartbeat(now core.Tick, node uint32, opened []chunkstore.OpenedLog) error {
	nr := meta.NodeReport{Node: node}
	for _, o := range opened {
		nr.Opened = append(nr.Opened, meta.OpenedDisk{Device: o.Device, Log: o.Log, Blocks: diskBlocks})
	}
	var out meta.ControlOutput
	return g.ctl.Heartbeat(now, &nr, &out)
}

func (g *ctlGroup) Ask(f chunkstore.PGFields) { g.ctl.Ask(f) }

func (g *ctlGroup) Edit(edits []chunkstore.Edit, moves int) {
	g.edits, g.moves = append(g.edits, edits...), moves
}

func (g *ctlGroup) Drain(id uint32, on bool) error { return g.ctl.Drain(id, on) }

func (g *ctlGroup) Lost(now core.Tick, id uint32) error { return g.ctl.Lost(now, id) }

func (g *ctlGroup) GiveUp(now core.Tick, pg core.PGID, interval uint32) error {
	return g.ctl.GiveUp(now, pg, interval)
}

// SetPool stages a pool over the committed disks, at the nodes the record has them on.
func (g *ctlGroup) SetPool(id core.PoolID, spec chunkstore.PoolSpec) error {
	rec := g.ctl.Disks()
	ds := g.ctl.AppendDisks(nil)
	for i := range ds {
		if j := slices.IndexFunc(rec, func(e meta.DiskEntry) bool { return e.ID == ds[i].ID }); j >= 0 {
			ds[i].Node = rec[j].Node
		}
	}
	return g.ctl.SetPool(id, spec, ds)
}

// Commit waits out the commit in flight, then steps the Control at now until it proposes nothing,
// waiting out each commit, while no time passes on the control plane's node, whose clocks read now
// and raw.
func (g *ctlGroup) Commit(now, raw core.Tick) {
	for range 16 {
		for range 10_000 {
			if g.waiting == 0 {
				break
			}
			g.advance(g.now + netDelay)
		}
		if g.waiting != 0 {
			g.t.Fatal("the control plane's commit never answered")
		}
		if !g.propose(now, raw) {
			g.off = max(g.off, g.now-now)
			return
		}
	}
	g.t.Fatal("the control plane still proposed after 16 commits")
}

func (g *ctlGroup) Table() (*chunkstore.Table, []chunkstore.RecordedDisk) {
	var rec []chunkstore.RecordedDisk
	for _, e := range g.ctl.Disks() {
		rec = append(rec, e.RecordedDisk)
	}
	return g.ctl.Table(), rec
}
