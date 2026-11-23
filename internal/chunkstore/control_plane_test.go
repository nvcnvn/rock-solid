package chunkstore

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

// ControlPlane is the real control plane a seeded run's authority stands on in place of Next: the
// leader's machine of a real three-replica control-plane group. Package chunkstore_test supplies it
// (ctlgroup_test.go), since the machine's package imports this one.
type ControlPlane interface {
	// Start arms the control plane's timer on its node, whose clocks ctx reads.
	Start(ctx *core.Context, out []core.Action) []core.Action
	// OnEvent takes a replica's heartbeat, asks, activations and recovered reports, a member_timeout
	// report, a probe's answer, the detector's timer or ControlTimer, on which it steps.
	OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action
	// EncodeAsks writes a node's asks for new intervals into msg, as a frame OnEvent takes.
	EncodeAsks(pool core.BufPool, fs []PGFields, msg *core.Msg) error
	// Heartbeat is node's report at now of the logs its devices opened.
	Heartbeat(now core.Tick, node uint32, opened []OpenedLog) error
	// Ask is the run's own ask, which no node sends.
	Ask(f PGFields)
	// Edit queues edits and sets the balance moves for the steps to come.
	Edit(edits []Edit, moves int)
	Drain(id uint32, on bool) error
	Lost(now core.Tick, id uint32) error
	GiveUp(now core.Tick, pg core.PGID, interval uint32) error
	SetPool(id core.PoolID, spec PoolSpec) error
	// Commit waits out the commit in flight and steps the machine until it proposes nothing,
	// committing each Txn, at once on the clocks now and raw of the control plane's node.
	Commit(now, raw core.Tick)
	// Table is the committed table, which the caller must not change, and disk record.
	Table() (*Table, []RecordedDisk)
}

// ControlTimer is the timer the real control plane steps on.
const ControlTimer core.TimerID = 0x63747270

// OpenedLog is a log a node's device opened, as its heartbeat names it.
type OpenedLog struct{ Device, Log uint64 }

// NewControlPlane starts a real control plane.
var NewControlPlane func(t *testing.T) ControlPlane

// realControl has a commit its tables through a real control plane, which learns disks 1 to nodes
// from node 0 to nodes-1's heartbeats, each naming log 0x5eed plus its node as the stand-in's
// record does, and takes a's pool through the operator's call; it returns the first table.
func realControl(t *testing.T, a *authority, nodes int) Table {
	a.t, a.cp, a.cut, a.told = t, NewControlPlane(t), map[uint32]bool{}, map[uint32]bool{}
	for n := range uint32(nodes) {
		if err := a.cp.Heartbeat(0, n, []OpenedLog{{0, 0x5eed + uint64(n)}}); err != nil {
			t.Fatal(err)
		}
	}
	a.cp.Commit(0, 0)
	p := a.pool
	spec := PoolSpec{Replicas: &p.Replicas, MinLive: &p.MinLive, FailureDomain: p.FailureDomain, MaxPerDomain: &p.MaxPerDomain,
		Locality: p.Locality, Overcommit: &p.Overcommit, WriteLimit: &p.WriteLimit, PGCount: &p.PGCount}
	if err := a.cp.SetPool(testPool, spec); err != nil {
		t.Fatal(err)
	}
	a.cp.Commit(0, 0)
	return a.committed()
}

// attach starts the control plane's node, which every replica heartbeats from each start. A
// heartbeat stands for its node's report too, naming the log the replica runs and any other a's
// opened gives the node. A replica's asks, activations and recovered reports go to it over the
// network, as its node sends them.
func (a *authority) attach(c *rcluster) {
	var rp Report
	a.c = c
	c.s.AddNode(sim.DefaultClock, nil, func(n *sim.Node) {
		c.auth = n.Start(sim.MachineFunc(func(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
			if ev.Kind == core.EvNetRecv {
				frame, data := Frame(ctx.Pool, &ev.Msg)
				if k, err := Peek(frame); err == nil && k == KindReport && DecodeReport(frame, data, &rp) == nil && len(rp.Disks) > 0 {
					node := ev.Peer.Node
					if err := a.cp.Heartbeat(ctx.Now, node, append([]OpenedLog{{0, rp.Disks[0].Log}}, a.opened[node]...)); err != nil {
						a.refused = append(a.refused, err)
					}
				}
			}
			return a.cp.OnEvent(ctx, ev, out)
		}), nil)
	})
	c.s.Node(c.auth.Node).Call(c.auth, func(ctx *core.Context, out []core.Action) []core.Action { return a.cp.Start(ctx, out) })
	c.reporting, c.forward = true, a.forward
	for k := range c.reps {
		c.call(k, func(ctx *core.Context, r *Replica, out *ReplicaOutput) { r.SetAuthority(ctx, c.auth, out) })
	}
}

// forward sends the control plane disk d's asks, and its activations and recovered reports in a report.
func (a *authority) forward(d int, out *ReplicaOutput) {
	pool := a.c.s.Node(uint32(d)).Pool()
	if len(out.Asks) > 0 {
		act := core.Action{Kind: core.ActNetSend, Peer: a.c.auth}
		if err := a.cp.EncodeAsks(pool, out.Asks, &act.Msg); err != nil {
			a.t.Fatal(err)
		}
		out.Actions = append(out.Actions, act)
	}
	if len(out.Activations)+len(out.Recovered) > 0 {
		a.reports++
		frame := AppendReport(nil, &Report{ID: core.RequestID(a.reports), Activations: out.Activations, Recovered: out.Recovered})
		act := core.Action{Kind: core.ActNetSend, Peer: a.c.auth}
		if err := Attach(pool, &act.Msg, frame, core.NoBuf); err != nil {
			a.t.Fatal(err)
		}
		out.Actions = append(out.Actions, act)
	}
}

// now is the control plane's clocks.
func (a *authority) now() (core.Tick, core.Tick) {
	ctx := a.c.s.Node(a.c.auth.Node).Context()
	return ctx.Now, ctx.Raw
}

// committed is the real control plane's committed table, its record mirrored in a.rec.
func (a *authority) committed() Table {
	t, rec := a.cp.Table()
	a.rec = NewDiskRecord(rec)
	return cloneTable(*t)
}

// next is the table after c's reports and the run's levers: Next over them, or the last the real
// control plane committed, which takes the levers at its next steps. There a disk down is one whose
// node is cut off from the control plane, which marks it down once it stays silent, and up once it
// heartbeats again.
func (a *authority) next(c *rcluster) Table {
	if a.cp == nil {
		t, _, _ := Next(&c.table, a.inputs(c))
		return t
	}
	cut := map[uint32]bool{}
	for d := range a.down {
		if n := c.node(d); n >= 0 {
			cut[uint32(n)] = true
		}
	}
	for _, n := range slices.Sorted(maps.Keys(cut)) {
		if !a.cut[n] {
			c.s.Partition(n, c.auth.Node, false)
		}
	}
	for _, n := range slices.Sorted(maps.Keys(a.cut)) {
		if !cut[n] {
			c.s.Heal(n, c.auth.Node)
		}
	}
	a.cut = cut
	for _, id := range slices.Sorted(maps.Keys(a.drain)) {
		if a.drain[id] && !a.told[id] {
			if err := a.cp.Drain(id, true); err != nil {
				a.t.Fatalf("drain of disk %d: %v", id, err)
			}
			a.told[id] = true
		}
	}
	for _, id := range slices.Sorted(maps.Keys(a.told)) {
		if !a.drain[id] {
			_ = a.cp.Drain(id, false) // a disk formatted again since is gone, which ends its drain
			delete(a.told, id)
		}
	}
	for _, x := range a.asks {
		a.cp.Ask(PGFields{Epoch: c.table.Epoch, PG: x.PG, Interval: x.Interval})
	}
	a.acts, a.rcv, a.seen = len(c.activations), len(c.recovered), len(c.asks)
	moves := 0
	if a.moves {
		moves = 1
	}
	a.cp.Edit(a.edits, moves)
	a.edits, a.asks = nil, nil
	return a.committed()
}

// due reports whether the real control plane committed a table c has not been handed, mirroring
// its committed record in a.rec.
func (a *authority) due() bool {
	if a.cp == nil {
		return false
	}
	t, rec := a.cp.Table()
	a.rec = NewDiskRecord(rec)
	return t.Epoch != a.c.table.Epoch
}

// report is node n's disk reporting log on device 0, and the disk ID the record gives it: at
// once, or once the real control plane commits it.
func (a *authority) report(n int, log uint64) (uint32, error) {
	if a.cp == nil {
		return a.rec.Report(uint32(n), 0, log, "")
	}
	now, raw := a.now()
	if err := a.cp.Heartbeat(now, uint32(n), []OpenedLog{{0, log}}); err != nil {
		return 0, err
	}
	a.cp.Commit(now, raw)
	a.committed()
	i := slices.IndexFunc(a.rec.Disks(), func(d RecordedDisk) bool { return d.Log == log && !d.Gone })
	if i < 0 {
		return 0, fmt.Errorf("the control plane committed no disk for log %#x", log)
	}
	return uint32(i + 1), nil
}

// rejoin heals every cut and steps the cluster for two heartbeats, so the real control plane
// marks each disk of a running node up again; a disk still damped stays down.
func (a *authority) rejoin(c *rcluster, tend func()) {
	if a.cp == nil {
		return
	}
	clear(a.down)
	for _, n := range slices.Sorted(maps.Keys(a.cut)) {
		c.s.Heal(n, c.auth.Node)
	}
	clear(a.cut)
	end := c.s.Time() + 2*RetryAfter
	for c.s.Time() < end && c.step() {
		tend()
	}
}

// publish hands every running replica the table the authority gives next, with the address of
// every disk ID, if it changes any entry.
func (a *authority) publish(c *rcluster) {
	next := a.next(c)
	if len(MakeDelta(&c.table, &next).Entries) == 0 {
		return
	}
	c.table = next
	for n := range c.reps {
		if c.s.Node(uint32(n)).Up() {
			c.call(n, func(ctx *core.Context, rep *Replica, out *ReplicaOutput) {
				for k, id := range c.ids {
					rep.SetAddr(id, core.NodeID{Node: uint32(k)})
				}
				rep.SetTable(ctx, next, out)
			})
		}
	}
}

// cloneRun is clients writing to every PG of four disks on the real control plane while, at a
// random time, a node opens a copy of another node's disk's log on its second device, which its
// heartbeats name from then on: in even seeds at once, the original up, and in odd seeds once the
// original's node, cut off from the control plane, has its disk marked down. It returns whether
// the control plane refused the clone and the run's first broken invariant, with plant the record
// accepting the clone and only "no disk ID names two log IDs" checked.
func cloneRun(t *testing.T, seed uint64, plant bool) (bool, error) {
	s := sim.New(seed, sim.DefaultSettings())
	a := &authority{pool: Pool{Replicas: 3, MinLive: 2, FailureDomain: DomainNode, MaxPerDomain: 1, Locality: LocalitySpread, Overcommit: true, WriteLimit: 90, PGCount: 4},
		down: map[uint32]core.Tick{}, opened: map[uint32][]OpenedLog{}}
	c := newRClusterOn(t, realControl(t, a, 4), s)
	a.attach(c)
	watchEpochs(c, &a.rec)
	if plant {
		plantCloneJoins, c.planted = true, true
		defer func() { plantCloneJoins = false }()
		c.inv.Only(invDiskIDs)
	}
	rng := core.NewRand(seed ^ 0xc10e)
	cl := &dclient{c: c, rng: rng.Fork(), pipe: 6, at: map[core.RequestID]core.Tick{}}
	s.Node(c.client.Node).AddActor(c.client, cl)
	at, end := s.Time()+sec+core.Tick(rng.Below(uint64(4*sec))), s.Time()+8*sec
	down := seed%2 == 1
	if down {
		end += NodeDownAfter
	}
	var d uint32
	var n int
	var clone uint64
	for s.Time() < end && c.found == nil {
		if d == 0 && s.Time() >= at {
			_, e := c.table.entry(core.PGID{Pool: testPool, Index: uint32(rng.Below(4))})
			d = e.Members[rng.Below(uint64(len(e.Members)))].Disk
			n = (c.node(d) + 1 + int(rng.Below(3))) % 4
			if down {
				a.down[d] = s.Time()
				a.publish(c)
			}
		}
		if d != 0 && clone == 0 && (!down || !a.rec.Disks()[d-1].Up) {
			clone = a.rec.Disks()[d-1].Log
			a.opened[uint32(n)] = []OpenedLog{{1, clone}}
		}
		if len(c.asks) > a.seen || len(c.activations) > a.acts || a.due() {
			a.publish(c)
		}
		if !c.step() {
			break
		}
	}
	if clone == 0 {
		t.Fatalf("seed %d: disk %d was never marked down", seed, d)
	}
	refused := slices.ContainsFunc(a.refused, func(err error) bool { return errors.Is(err, ErrLogRefused) })
	if c.found != nil {
		return refused, c.found
	}
	return refused, c.inv.Check()
}

// A node opening a copy of a member disk's log on its second device, at a random time while
// clients write, the original up or down: the control plane refuses the copy each time the node's
// heartbeat names it, and no disk ID names two log IDs, nor is a log ID up under two disk IDs.
func TestAClonedDiskIsRefused(t *testing.T) {
	seeds.Run(t, 0, 10, func(seed uint64) {
		refused, err := cloneRun(t, seed, false)
		if err == nil && !refused {
			err = errors.New("the control plane took the clone")
		}
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
	})
}

// A record that takes the clone as a new disk has its log up under two disk IDs.
func TestPlantedCloneJoinsBreaksTheDiskIDs(t *testing.T) {
	if _, err := cloneRun(t, 0, true); err == nil || !strings.Contains(err.Error(), invDiskIDs) {
		t.Fatalf("a record taking the clone: %v", err)
	}
}

// A node cut off from the control plane, whose disk the detector has yet to mark down: its
// restarted replica's asks for new intervals of the PGs it is a member of never reach the leader,
// and reach it once the cut heals.
func TestACutNodesAsksDoNotReachTheControlPlane(t *testing.T) {
	s := sim.New(1, sim.DefaultSettings())
	a := &authority{pool: Pool{Replicas: 3, MinLive: 2, FailureDomain: DomainNode, MaxPerDomain: 1, Locality: LocalitySpread, Overcommit: true, WriteLimit: 90, PGCount: 4},
		down: map[uint32]core.Tick{}, opened: map[uint32][]OpenedLog{}}
	c := newRClusterOn(t, realControl(t, a, 4), s)
	a.attach(c)
	run := func(d core.Tick) {
		for end := s.Time() + d; s.Time() < end && c.step(); {
			if len(c.asks) > a.seen || len(c.activations) > a.acts || a.due() {
				a.publish(c)
			}
		}
	}
	intervals := func() map[core.PGID]uint32 {
		m := map[core.PGID]uint32{}
		for i := range uint32(4) {
			pg := core.PGID{Pool: testPool, Index: i}
			if _, e := c.table.entry(pg); slices.ContainsFunc(e.Members[1:], func(m Member) bool { return m.Disk == 1 }) {
				m[pg] = e.Interval
			}
		}
		return m
	}
	run(sec)
	was, asked := intervals(), len(c.asks)
	a.down[1] = s.Time()
	a.publish(c)
	c.s.CrashRandom(0)
	run(2 * sec)
	if !slices.ContainsFunc(c.asks[asked:], func(f PGFields) bool { _, ok := was[f.PG]; return ok }) {
		t.Fatalf("the restarted replica asked nothing: %+v", c.asks[asked:])
	}
	if now := intervals(); !maps.Equal(now, was) {
		t.Fatalf("a cut node's asks reached the control plane: intervals %v, then %v", was, now)
	}
	delete(a.down, 1)
	a.publish(c)
	run(2 * sec)
	for pg, iv := range intervals() {
		if iv <= was[pg] {
			t.Fatalf("PG %v kept interval %d once the cut healed", pg, iv)
		}
	}
}
