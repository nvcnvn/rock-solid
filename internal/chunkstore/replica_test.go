package chunkstore

import (
	"bytes"
	"fmt"
	"runtime"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/lease"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/trace"
)

// rcluster is replicas on simulated nodes, disk d on node d − 1, a client on the last node, and the test as the control plane: it publishes tables and ends peering.
type rcluster struct {
	t      *testing.T
	s      *sim.Scheduler
	reps   []*Replica
	loops  []core.NodeID
	table  Table
	client core.NodeID
	cl     *rclient
	nextID core.RequestID
	// fetch is the newest table epoch each replica asked for, and whole whether it asked for the whole table.
	fetch   []uint64
	whole   []bool
	fetches []int
	asks    []PGFields
	// activations is every activation a primary reported, which next records in its table, and
	// recovered every recovered interval.
	activations []PGFields
	recovered   []PGFields
	inv         sim.Invariants
	// sentVer is the newest version of each chunk a primary sent each member on its current connection, boots counting each node's starts and lost the connections each lost; orderErr is the first send out of order.
	sentVer map[sendKey]uint64
	boots   []int
	lost    map[[2]core.NodeID]int
	// conforming is set once the run records a conformance trace, noted once its first table was recorded.
	conforming, noted bool
	orderErr          error
	// passes is each pass of each replica's loop: the actions it emitted.
	passes [][][]core.Action
	// counting adds the heap allocations each replica's OnEvent makes to mallocs.
	counting bool
	mallocs  uint64
	// onPass, when set, sees the actions of each pass of each replica's loop, and drop, when set, removes those it reports.
	onPass func(d int, acts []core.Action)
	drop   func(d int, a *core.Action) bool
	// onEvent, when set, sees each event before each replica takes it, and again after with done.
	onEvent func(d int, ev *core.Event, done bool)
	// ids is the disk ID of node d's disk and logs its log ID; reformat has node d's next start format its disk with the log ID in logs, and onFormat, when set, then gives its disk ID.
	ids      []uint32
	logs     []uint64
	reformat []bool
	onFormat func(d int) uint32
	// storePlants and replicaPlants are planted in every store and replica the cluster starts.
	storePlants   plants
	replicaPlants replicaPlants
	// planted has step note the first broken invariant in found rather than fail the test.
	planted bool
	found   error
	// auth, once reporting is set, is the authority every replica heartbeats, from each start.
	auth      core.NodeID
	reporting bool
	// hist, once set, records every client write, punch and read.
	hist *chunkHist
}

type sendKey struct {
	from, to   core.NodeID
	boot, conn int
	chunk      core.ChunkID
}

const invSendOrder = "a primary sends a chunk's writes to each member in version order, again after a reconnect"

type rclient struct {
	replies map[core.RequestID]Response
	data    map[core.RequestID][]byte
	sent    map[core.RequestID]core.IOBuf
	// lost, when set, sees each peer whose connection broke.
	lost func(peer core.NodeID)
}

func (c *rclient) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	if ev.Kind == core.EvPeerLost && c.lost != nil {
		c.lost(ev.Peer)
	}
	if ev.Kind != core.EvNetRecv {
		return out
	}
	frame, data := Frame(ctx.Pool, &ev.Msg)
	var rp Response
	if err := DecodeResponse(frame, data, &rp); err != nil {
		panic(err)
	}
	if _, dup := c.replies[rp.ID]; dup {
		panic("a request answered twice")
	}
	c.replies[rp.ID] = rp
	if ev.Msg.Buf != core.NoBuf {
		c.data[rp.ID] = slices.Clone(ctx.Pool.Bytes(ev.Msg.Buf))
		ctx.Pool.Free(ev.Msg.Buf)
	}
	if b, ok := c.sent[rp.ID]; ok {
		ctx.Pool.Free(b)
		delete(c.sent, rp.ID)
	}
	return out
}

// rtable is one pool of pgs rep3 PGs over disks 1 to 3, PG i led by disk i mod 3 + 1, all in interval 1.
func rtable(pgs uint32, minLive uint8) Table {
	p := PoolTable{ID: testPool, Replicas: 3, MinLive: minLive}
	for i := range pgs {
		var ms []Member
		for k := range uint32(3) {
			ms = append(ms, Member{Disk: (i+k)%3 + 1})
		}
		p.PGs = append(p.PGs, PGEntry{Members: ms, Interval: 1})
	}
	return Table{Epoch: 1, Pools: []PoolTable{p}}
}

func newRCluster(t *testing.T, tbl Table) *rcluster {
	return newRClusterOn(t, tbl, sim.New(1, sim.DefaultSettings()))
}

// newRClusterOn is newRCluster on scheduler s.
func newRClusterOn(t *testing.T, tbl Table, s *sim.Scheduler) *rcluster {
	return newRClusterWith(t, tbl, s, nil)
}

// newRClusterWith is newRClusterOn with each store's settings edited by edit, if set, and a node for each disk up to the highest the table names.
func newRClusterWith(t *testing.T, tbl Table, s *sim.Scheduler, edit func(*Config)) *rcluster {
	geo, err := disklog.GeometryFor(diskBytes)
	if err != nil {
		t.Fatal(err)
	}
	c := &rcluster{t: t, s: s, table: tbl, cl: &rclient{replies: map[core.RequestID]Response{}, data: map[core.RequestID][]byte{}, sent: map[core.RequestID]core.IOBuf{}},
		sentVer: map[sendKey]uint64{}, lost: map[[2]core.NodeID]int{}}
	c.inv.Register(invSendOrder, func() error { return c.orderErr })
	disks := 0
	for _, p := range tbl.Pools {
		for _, e := range p.PGs {
			for _, m := range e.Members {
				disks = max(disks, int(m.Disk))
			}
		}
	}
	c.reps, c.loops, c.fetch, c.whole, c.passes = make([]*Replica, disks), make([]core.NodeID, disks), make([]uint64, disks), make([]bool, disks), make([][][]core.Action, disks)
	c.fetches = make([]int, disks)
	c.ids, c.logs, c.reformat, c.boots = make([]uint32, disks), make([]uint64, disks), make([]bool, disks), make([]int, disks)
	for d := range disks {
		c.ids[d], c.logs[d] = uint32(d+1), 0x5eed+uint64(d)
	}
	for d := range disks {
		c.s.AddNode(sim.DefaultClock, []sim.DiskSpec{{Blocks: geo.DiskBlocks(), Settings: sim.DefaultDiskSettings()}}, func(n *sim.Node) {
			out := ReplicaOutput{Trace: make([]trace.Record, 0, 256)}
			m := sim.MachineFunc(func(ctx *core.Context, ev *core.Event, acts []core.Action) []core.Action {
				out.Reset()
				if ev.Kind == core.EvPeerLost {
					c.lost[[2]core.NodeID{c.loops[d], ev.Peer}]++
				}
				if c.onEvent != nil {
					c.onEvent(d, ev, false)
					defer c.onEvent(d, ev, true)
				}
				if c.counting {
					var before, after runtime.MemStats
					runtime.ReadMemStats(&before)
					c.reps[d].OnEvent(ctx, ev, &out)
					runtime.ReadMemStats(&after)
					c.mallocs += after.Mallocs - before.Mallocs
				} else {
					c.reps[d].OnEvent(ctx, ev, &out)
				}
				return c.took(d, &out, acts)
			})
			c.loops[d] = n.Start(m, n.Disk(0))
			seed := n.Fork().Uint64()
			n.Call(c.loops[d], func(ctx *core.Context, acts []core.Action) []core.Action {
				var sout Output
				cfg := testConfig()
				cfg.MergeCache = DefaultMergeCache
				if edit != nil {
					edit(&cfg)
				}
				c.boots[d]++
				first := c.reps[d] == nil
				fresh := first || c.reformat[d]
				var st *Store
				var err error
				if fresh {
					st, err = Format(ctx, cfg, disklog.Superblock{LogID: c.logs[d], Geometry: geo}, &sout)
				} else {
					st, err = Open(ctx, cfg, geo.DiskBlocks(), &sout)
				}
				if err != nil {
					t.Fatal(err)
				}
				if c.reformat[d] && c.onFormat != nil {
					c.ids[d] = c.onFormat(d)
				}
				c.reformat[d] = false
				st.plant = c.storePlants
				c.reps[d] = NewReplica(ReplicaConfig{Disk: c.ids[d], Seed: seed, Start: lease.Now(ctx), Log: c.logs[d], Restarted: !fresh}, st)
				c.reps[d].plant = c.replicaPlants
				for k := range disks {
					c.reps[d].SetAddr(c.ids[k], core.NodeID{Node: uint32(k)})
				}
				acts = append(acts, sout.Actions...)
				if first && !c.reporting {
					return acts
				}
				var out ReplicaOutput
				if c.reporting {
					c.reps[d].SetAuthority(ctx, c.auth, &out)
				}
				if !first {
					c.reps[d].SetTable(ctx, c.table, &out)
				}
				return c.took(d, &out, acts)
			})
		})
	}
	c.s.AddNode(sim.DefaultClock, nil, func(n *sim.Node) { c.client = n.Start(c.cl, nil) })
	c.settle()
	c.publish(tbl, false)
	c.runFor(lease.DefaultConfig.Lease + 500_000)
	c.untilActive()
	return c
}

func (c *rcluster) took(d int, out *ReplicaOutput, acts []core.Action) []core.Action {
	if c.drop != nil {
		out.Actions = slices.DeleteFunc(out.Actions, func(a core.Action) bool { return c.drop(d, &a) })
	}
	c.fetch[d] = max(c.fetch[d], out.Fetch)
	c.whole[d] = c.whole[d] || out.Whole
	if out.Fetch != 0 {
		c.fetches[d]++
	}
	c.asks = append(c.asks, out.Asks...)
	c.activations = append(c.activations, out.Activations...)
	c.recovered = append(c.recovered, out.Recovered...)
	for i := range out.Trace {
		c.s.Node(uint32(d)).Output(c.loops[d], out.Trace[i])
	}
	c.passes[d] = append(c.passes[d], slices.Clone(out.Actions))
	if c.onPass != nil {
		c.onPass(d, out.Actions)
	}
	if c.hist != nil {
		c.hist.ordered(c.reps[d])
	}
	for i := range out.Actions {
		a := &out.Actions[i]
		var u Update
		if k := Kind(a.Msg.Body[1]); a.Kind != core.ActNetSend || a.Msg.Len < headSize || k != KindReplicaWrite && k != KindReplicaPunch {
			continue
		}
		var data int
		if a.Msg.Buf != core.NoBuf {
			data = len(c.s.Node(uint32(d)).Pool().Bytes(a.Msg.Buf))
		}
		if DecodeUpdate(a.Msg.Inline(), data, &u) != nil || u.Version == 0 {
			continue
		}
		k := sendKey{from: c.loops[d], to: a.Peer, boot: c.boots[d], conn: c.lost[[2]core.NodeID{c.loops[d], a.Peer}], chunk: u.Chunk}
		if u.Version <= c.sentVer[k] && c.orderErr == nil {
			c.orderErr = fmt.Errorf("disk %d sent %#x of %v to %v after %#x on one connection", d+1, u.Version, u.Chunk, a.Peer, c.sentVer[k])
		}
		c.sentVer[k] = u.Version
	}
	return append(acts, out.Actions...)
}

// call runs f on disk d's loop as one pass.
func (c *rcluster) call(d int, f func(ctx *core.Context, r *Replica, out *ReplicaOutput)) {
	c.s.Node(uint32(d)).Call(c.loops[d], func(ctx *core.Context, acts []core.Action) []core.Action {
		var out ReplicaOutput
		f(ctx, c.reps[d], &out)
		return c.took(d, &out, acts)
	})
}

// publish hands every replica t and, with activate, runs the cluster until every PG a running replica leads has peered.
func (c *rcluster) publish(t Table, activate bool) {
	c.noteTable(&t)
	c.table = t
	for d := range c.reps {
		c.call(d, func(ctx *core.Context, r *Replica, out *ReplicaOutput) { r.SetTable(ctx, t, out) })
	}
	if activate {
		c.untilActive()
	}
}

// untilActive steps the cluster until every PG that a running replica leads in the cluster's table, with min_live members live, is active in its interval.
func (c *rcluster) untilActive() {
	c.t.Helper()
	for range 1_000_000 {
		if c.active() {
			return
		}
		if !c.step() {
			break
		}
	}
	c.t.Fatal("peering did not end")
}

func (c *rcluster) active() bool {
	for pi := range c.table.Pools {
		p := &c.table.Pools[pi]
		for i := range p.PGs {
			e := &p.PGs[i]
			d, ok := e.primary()
			n := c.node(d)
			if !ok || e.live() < int(p.MinLive) || n < 0 || !c.s.Node(uint32(n)).Up() {
				continue
			}
			r := c.reps[n]
			j := r.lead(core.PGID{Pool: p.ID, Index: uint32(i)})
			if j < 0 || r.leads[j].interval != e.Interval || !r.leads[j].active && !r.leads[j].asking {
				return false
			}
		}
	}
	return true
}

// settle steps the cluster until nothing but the replicas' periodic timers is pending.
func (c *rcluster) settle() {
	c.t.Helper()
	for range 1_000_000 {
		if c.quiet() || !c.step() {
			return
		}
	}
	c.t.Fatal("the cluster did not settle")
}

// quiet reports whether every step pending is a periodic timer: a replica asking the control plane again.
func (c *rcluster) quiet() bool {
	return c.s.Idle(func(id core.TimerID) bool {
		return id == RetryTimer || id == LeaseTimer || id == ReportTimer || id == DetectTimer
	})
}

func (c *rcluster) step() bool {
	c.t.Helper()
	more := c.s.Step()
	if err := c.inv.Check(); err != nil {
		if !c.planted {
			c.t.Fatal(err)
		}
		if c.found == nil {
			c.found = err
		}
	}
	if err := c.s.DriverContract(); err != nil {
		c.t.Fatal(err)
	}
	return more
}

// runFor steps the cluster until simulated time d has passed or nothing is pending.
func (c *rcluster) runFor(d core.Tick) {
	c.t.Helper()
	end := c.s.Time() + d
	for {
		if next := c.s.Next(); next == 0 || next > end {
			c.s.Advance(max(end, c.s.Time()))
			return
		}
		c.step()
	}
}

func chunkIn(pg core.PGID, count uint32, n uint32) core.ChunkID {
	for i := uint32(0); ; i++ {
		ch := core.ChunkID{Volume: 1, Inode: 7, Index: i}
		if core.PGIndexOf(ch.Hash(), count) == pg.Index {
			if n == 0 {
				return ch
			}
			n--
		}
	}
}

func (c *rcluster) pgFields(pg core.PGID) PGFields {
	_, e := c.table.entry(pg)
	return PGFields{Epoch: c.table.Epoch, PG: pg, Interval: e.Interval}
}

// primary is the node of pg's primary.
func (c *rcluster) primary(pg core.PGID) int {
	_, e := c.table.entry(pg)
	d, _ := e.primary()
	return c.node(d)
}

// node is the node whose disk is disk.
func (c *rcluster) node(disk uint32) int {
	return slices.Index(c.ids, disk)
}

// update sends a client's write (data set) or punch of chunk ch to the PG's primary, tagged as pgf.
func (c *rcluster) update(pgf PGFields, ch core.ChunkID, off uint32, data []byte, n uint32, edit func(*Update)) core.RequestID {
	c.nextID++
	id := c.nextID
	_, e := c.table.entry(pgf.PG)
	lead, _ := e.primary()
	u := Update{Kind: KindPunch, ID: id, PG: pgf, Session: 1, Incarnation: 1, Chunk: ch, Off: off, Len: n}
	if data != nil {
		u.Kind, u.Len = KindWrite, uint32(len(data))
	}
	if edit != nil {
		edit(&u)
	}
	c.s.Node(c.client.Node).Call(c.client, func(ctx *core.Context, acts []core.Action) []core.Action {
		a := core.Action{Kind: core.ActNetSend, Peer: core.NodeID{Node: lead - 1}}
		a.Msg.Len = uint16(copy(a.Msg.Body[:], AppendUpdate(nil, &u)))
		if u.Kind == KindWrite {
			buf, err := ctx.Pool.AllocZeroed(dataBytes(u.Off, u.Len))
			if err != nil {
				c.t.Fatal(err)
			}
			copy(ctx.Pool.Bytes(buf)[u.Off%core.BlockSize:], data)
			a.Msg.Buf = buf
			c.cl.sent[id] = buf
		}
		c.hist.invoke(ctx, 0, &a)
		c.noteSends([]core.Action{a})
		return append(acts, a)
	})
	return id
}

func (c *rcluster) write(pg core.PGID, ch core.ChunkID, off uint32, data []byte) core.RequestID {
	return c.update(c.pgFields(pg), ch, off, data, 0, nil)
}

func (c *rcluster) read(pg core.PGID, ch core.ChunkID, off, n uint32, to int) core.RequestID {
	c.nextID++
	id := c.nextID
	rd := Read{ID: id, PG: c.pgFields(pg), Chunk: ch, Off: off, Len: n}
	c.s.Node(c.client.Node).Call(c.client, func(ctx *core.Context, acts []core.Action) []core.Action {
		a := core.Action{Kind: core.ActNetSend, Peer: core.NodeID{Node: uint32(to)}}
		a.Msg.Len = uint16(copy(a.Msg.Body[:], AppendRead(nil, &rd)))
		c.hist.invoke(ctx, 0, &a)
		return append(acts, a)
	})
	return id
}

func (c *rcluster) reply(id core.RequestID) Response {
	c.t.Helper()
	rp, ok := c.cl.replies[id]
	if !ok {
		c.t.Fatalf("request %d not answered", id)
	}
	return rp
}

// readNow reads through disk to's replica and returns the bytes.
func (c *rcluster) readNow(pg core.PGID, ch core.ChunkID, off, n uint32) []byte {
	c.t.Helper()
	id := c.read(pg, ch, off, n, c.primary(pg))
	c.until(id)
	rp := c.reply(id)
	if rp.Status != StatusOK {
		c.t.Fatalf("read answered %+v", rp)
	}
	return c.cl.data[id][off%core.BlockSize:][:n]
}

func (c *rcluster) newest(d int, ch core.ChunkID) uint64 { return c.reps[d].Store().Newest(ch) }

// until steps the cluster until request id is answered.
func (c *rcluster) until(id core.RequestID) {
	c.t.Helper()
	for range 1_000_000 {
		if _, ok := c.cl.replies[id]; ok || !c.step() {
			return
		}
	}
	c.t.Fatalf("request %d not answered", id)
}

var pg0 = core.PGID{Pool: testPool, Index: 0}

func TestAWriteCommitsOnEveryMemberAndIsAcknowledged(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	ch := chunkIn(pg0, 4, 0)
	w := c.write(pg0, ch, 100, []byte("hello"))
	c.settle()
	if rp := c.reply(w); rp.Status != StatusOK || rp.Version != MakeVersion(1, 1) {
		t.Fatalf("answered %+v, want 1.1", rp)
	}
	for d := range 3 {
		if v := c.newest(d, ch); v != MakeVersion(1, 1) {
			t.Fatalf("disk %d holds %#x", d+1, v)
		}
	}
	if got := c.readNow(pg0, ch, 99, 7); !bytes.Equal(got, []byte("\x00hello\x00")) {
		t.Fatalf("read % x", got)
	}
	w = c.write(pg0, ch, 102, []byte("LL"))
	c.settle()
	if rp := c.reply(w); rp.Version != MakeVersion(1, 2) {
		t.Fatalf("second write answered %+v", rp)
	}
	if got := c.readNow(pg0, ch, 100, 5); string(got) != "heLLo" {
		t.Fatalf("merged read %q", got)
	}
}
