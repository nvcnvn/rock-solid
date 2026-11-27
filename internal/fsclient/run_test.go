package fsclient

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/chunkstore/storetest"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/fuseproto"
	"github.com/nvcnvn/rock-solid/internal/lease"
	"github.com/nvcnvn/rock-solid/internal/lsm"
	"github.com/nvcnvn/rock-solid/internal/meta"
	nodepkg "github.com/nvcnvn/rock-solid/internal/node"
	"github.com/nvcnvn/rock-solid/internal/raftshard"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

const (
	invCloseToOpen = "a read in a handle opened after a close sees the bytes that close covers"
	invInodeNew    = "an inode ID is never handed out twice"
	invMtime       = "no file's mtime goes backwards"
	invSlot        = "a file's entry is in its directory's slot"
	invAncestor    = "no directory is its own ancestor"
	invErrno       = "every system call of the run succeeds"
	invLive        = "once faults heal, no system call or request waits longer than liveWait"
)

const (
	runGroup   = 5
	runSession = 1
	runBlocks  = 1 << 18
	runFor     = core.Tick(30_000_000)
	// quietFor is the run's time without faults after them, long enough for an election and the
	// router's wait to retry a Txn it held for one.
	quietFor = core.Tick(3_000_000)
	// faultGap is the most time between two daemon crashes, and between two reboots.
	faultGap = core.Tick(8_000_000)
	// The client loop's timers: its router's and the held reads' retry.
	routerTimer core.TimerID = 0x66737274
	readTimer   core.TimerID = 0x66737264
	readRetry                = core.Tick(1000)
	// realtime is the wall clock's reading at CLOCK_BOOTTIME 0, in nanoseconds.
	realtime = int64(1_700_000_000_000_000_000)
	// liveWait is how long, at the end of the quiet time, a system call or a request in a machine
	// may have waited.
	liveWait = core.Tick(100_000)
)

// fsRun is one node's mount against a three-replica MetaStore shard and three chunk devices.
// Nodes 0 to 2 each run a meta.Shard replica, served over the meta wire; nodes 3 to 5 each run a
// node.Device, whose placement table puts four PGs of one member on them. The daemon on node 6 runs
// a Machine per mount, a meta.Client and a chunkstore.Client, behind a sim.Mount whose processes
// open the root directory's two files by name, read, read through a mapping, write the whole file
// and fsync, one writer of a file at a time. Faults: partitions, and up to faultGap apart until
// rebootsEnd a replica's node rebooted, so leaders change, and the daemon crashed, also within 3 ms of a chunk
// write it calls, half the time for a file's first and one time in 32 for another; it restarts as
// a new mount that, before its processes start, creates each file not yet made and, with even
// odds, unlinks and creates each other again. The devices do not crash: a restarted primary asks
// the control plane, which the run does not have, for a new interval.
//
// MetaStore reads have no wire yet: the daemon serves each from the state of an up replica that
// has applied every entry any replica has, which a leader's lease read stands for, and holds it
// for readRetry while none has.
type fsRun struct {
	t     testing.TB
	s     *sim.Scheduler
	rng   *core.Rand
	plant plants
	hist  *sim.History
	inv   sim.Invariants
	reps  [3]*metaRep
	devs  [3]*devRep
	d     *daemon
	files [2]runFile
	// incs counts the daemon's starts, clients the history's clients and ids the writing handles.
	incs, ids  uint64
	clients    uint32
	maxApplied uint64
	// last is, per inode, the last chunk the daemon called a write of.
	last    map[uint64]uint64
	creates map[uint64]string
	mtimes  map[uint64]int64
	// crashAt is when the daemon crashes next, rebootAt when a replica's node reboots.
	crashAt, rebootAt core.Tick
	crashes           int
	// ended is set for the check at the end of the quiet time.
	ended bool
	// idErr, errnoErr and purgeErr are the first inode handed out twice, system call failed and
	// chunk a crash left past alloc_end and the purge entry.
	idErr, errnoErr, purgeErr error
	scan                      meta.ScanResult
}

// runFile is a name of the root directory: its inode, the most any writer of it wrote, and the
// handle writing it.
type runFile struct {
	name   string
	ino    uint64
	size   int64
	writer *runProc
}

// rebootsEnd is when replicas' nodes stop rebooting: one rebooted then is down a second and
// grants no vote for its restart wait, so a quorum votes again from runFor.
var rebootsEnd = runFor - 1_000_000 - lease.DefaultConfig.Lease

// clientNode is the daemon's node; nodes 0 to 2 run the shard's replicas.
const clientNode = 6

// devAt is the loop of chunk device i.
func devAt(i int) core.NodeID { return core.NodeID{Node: uint32(3 + i)} }

// storageClock starts the replicas' nodes' clocks past a lease, so a new group's grantors, which
// start at 0, have no restart wait left.
var storageClock = sim.Clock{Now: 5_000_000, Raw: 5_000_226}

func newFSRun(t testing.TB, seed uint64, p plants) *fsRun {
	set := sim.DefaultSettings()
	set.PoolBytes = 64 << 20
	set.PartitionNum, set.PartitionDen = 1, 4000
	r := &fsRun{t: t, s: sim.New(seed, set), rng: core.NewRand(seed), plant: p, last: map[uint64]uint64{},
		creates: map[uint64]string{}, mtimes: map[uint64]int64{}}
	r.files = [2]runFile{{name: "a"}, {name: "b"}}
	r.rebootAt = core.Tick(r.rng.Below(uint64(faultGap)))
	r.hist = sim.NewHistory(r.s, 1<<14)
	r.inv.Register("driver contract", r.s.DriverContract)
	r.inv.Register(invInodeNew, func() error { return r.idErr })
	r.inv.Register(invErrno, func() error { return r.errnoErr })
	r.inv.Register(invMtime, r.mtimeKept)
	r.inv.Register(invSlot, r.slotsKept)
	r.inv.Register(invAncestor, r.noCycle)
	r.inv.Register(invPurgeMisses, func() error { return r.purgeErr })
	r.inv.Register(invLive, r.live)
	r.inv.Register(invCloseToOpen, func() error {
		if i, broken := sim.CloseToOpen(r.hist.Ops()); broken {
			return fmt.Errorf("operation %d: %+v", i, r.hist.Ops()[i])
		}
		return nil
	})
	for i := range 3 {
		r.reps[i] = &metaRep{r: r, i: i, sv: meta.NewServer()}
		r.s.AddNode(storageClock, []sim.DiskSpec{{Blocks: 16, Settings: sim.DefaultDiskSettings()}}, r.reps[i].boot)
	}
	for i := range 3 {
		r.devs[i] = &devRep{r: r, i: i}
		r.s.AddNode(sim.DefaultClock, []sim.DiskSpec{{Blocks: runBlocks, Settings: sim.DefaultDiskSettings()}}, func(n *sim.Node) {
			r.devs[i].boot(n)
			n.Start(sim.MachineFunc(sink), nil)
		})
	}
	r.s.AddNode(sim.DefaultClock, nil, r.bootDaemon)
	return r
}

// sink is the loop the devices report to, which drops what it gets.
func sink(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	if ev.Kind == core.EvNetRecv && ev.Msg.Buf != core.NoBuf {
		ctx.Pool.Free(ev.Msg.Buf)
	}
	return out
}

// run steps the run under its faults for runFor, then heals every partition and runs quietFor
// more without faults, checking the invariants every 2,000 steps and at the end.
func (r *fsRun) run() error {
	for steps := 0; ; steps++ {
		now := r.s.Time()
		if now >= runFor+quietFor {
			break
		}
		if now >= runFor {
			for a := range uint32(clientNode + 1) {
				for b := range uint32(clientNode + 1) {
					r.s.Heal(a, b)
				}
			}
		} else {
			r.faults()
		}
		if !r.s.Step() {
			break
		}
		r.tend()
		if steps%2000 == 1999 {
			if err := r.inv.Check(); err != nil {
				return err
			}
		}
	}
	r.ended = true
	return r.inv.Check()
}

// live checks, at the end of the quiet time, that no process's system call and no request in
// the daemon's machines has waited longer than liveWait.
func (r *fsRun) live() error {
	d := r.d
	if !r.ended || !d.n.Up() {
		return nil
	}
	now := r.s.Time()
	for _, pr := range d.procs {
		if !pr.p.Busy() {
			continue
		}
		if op := r.hist.Ops()[pr.op]; now-op.Invoke > liveWait {
			return fmt.Errorf("client %d's system call %+v has waited %d µs", pr.client, op, now-op.Invoke)
		}
	}
	at := realtime + int64(d.n.Context().Now)*1000
	for _, m := range []*Machine{d.setup, d.m} {
		for _, o := range m.ops {
			if o.req.Opcode < opReserveSlots && at-o.now > int64(liveWait)*1000 {
				return fmt.Errorf("request %+v at step %d has waited %d µs, with %d Txns in flight", o.req, o.step, (at-o.now)/1000, len(d.txns))
			}
		}
	}
	return nil
}

// faults crashes the daemon at crashAt, checking the chunks it wrote first, and before rebootsEnd
// reboots a replica's node at rebootAt, each drawn again up to faultGap later.
func (r *fsRun) faults() {
	now := r.s.Time()
	if c := r.s.Node(clientNode); c.Up() && now >= r.crashAt {
		if err := r.purged(); err != nil && r.purgeErr == nil {
			r.purgeErr = err
		}
		// A setup request the crash left in flight may still apply, after the next start's: the
		// file goes on under a new name, which only the next start touches.
		if d := r.d; !d.ready && d.step > 0 {
			for i := range r.files {
				if f := &r.files[i]; f.name == string(d.steps[d.step-1].Name) {
					f.name, f.ino, f.size = f.name+"'", 0, 0
				}
			}
		}
		r.crashes++
		r.s.CrashRandom(clientNode)
	}
	if now >= r.rebootAt && now < rebootsEnd {
		r.rebootAt = now + core.Tick(r.rng.Below(uint64(faultGap)))
		if i := uint32(r.rng.Below(3)); r.s.Node(i).Up() {
			r.s.Reboot(i)
		}
	}
}

// runTable places pool 1's four PGs, of one member each, on disks 1, 2, 3 and 1.
func runTable() chunkstore.Table {
	t := chunkstore.Table{Epoch: 2, Pools: []chunkstore.PoolTable{{ID: 1, Replicas: 1, MinLive: 1}}}
	for i := range 4 {
		t.Pools[0].PGs = append(t.Pools[0].PGs, chunkstore.PGEntry{Members: []chunkstore.Member{{Disk: uint32(i%3) + 1}}, Interval: 1, Activated: 1})
	}
	return t
}

// tend hands each opened device that has not had them since it started the membership of all
// three, its session table and the placement table.
func (r *fsRun) tend() {
	for _, dv := range r.devs {
		if dv.log == 0 {
			return
		}
	}
	ds := make([]meta.DiskAddr, 3)
	tbl := runTable()
	for i, dv := range r.devs {
		ds[i] = meta.DiskAddr{Disk: uint32(i) + 1, Log: dv.log, At: devAt(i), Blocks: runBlocks, Reserve: dv.reserve}
	}
	for i, dv := range r.devs {
		n := r.s.Node(devAt(i).Node)
		if !n.Up() || !dv.opened || dv.handed {
			continue
		}
		dv.handed = true
		o := nodepkg.Output{Member: true, Disks: ds, Table: &tbl,
			Hands: []nodepkg.Hand{{Device: uint64(i) + 1, Table: chunkstore.SessionTable{Version: 1, Next: runSession + 1, Live: []uint64{runSession}}}}}
		n.Call(dv.loop, func(ctx *core.Context, acts []core.Action) []core.Action {
			dv.out.Reset()
			dv.d.Take(ctx, &o, &dv.out)
			return dv.finish(acts)
		})
	}
}

// current is the state of an up replica that has applied every entry any replica has, if any.
func (r *fsRun) current() *meta.Shard {
	for _, m := range r.reps {
		if m.s == nil || !r.s.Node(uint32(m.i)).Up() {
			continue
		}
		if idx, _ := m.s.Applied(); idx >= r.maxApplied {
			return m.s
		}
	}
	return nil
}

// purged checks the chunks the daemon called writes of against the current state.
func (r *fsRun) purged() error {
	s := r.current()
	if s == nil {
		return nil
	}
	return purgeMisses(s.Scan, r.last)
}

// each calls f with each of the current state's keys decoded, as long as f returns nil.
func (r *fsRun) each(f func(k *meta.Key, v []byte) error) error {
	s := r.current()
	if s == nil {
		return nil
	}
	if err := s.Scan(nil, nil, 1<<20, &r.scan); err != nil {
		return err
	}
	for _, kv := range r.scan.KVs {
		var k meta.Key
		if meta.DecodeKey(kv.Key, &k) != nil || k.Space != meta.SpaceFS {
			continue
		}
		if err := f(&k, kv.Value); err != nil {
			return err
		}
	}
	return nil
}

func (r *fsRun) mtimeKept() error {
	return r.each(func(k *meta.Key, v []byte) error {
		if k.Tag != meta.InodeAttr || k.Inode&(1<<meta.SeqBits-1) == 0 {
			return nil
		}
		var a meta.Attr
		if err := meta.DecodeAttr(v, &a); err != nil {
			return err
		}
		if old, ok := r.mtimes[k.Inode]; ok && a.Mtime < old {
			return fmt.Errorf("inode %x's mtime went from %d to %d", k.Inode, old, a.Mtime)
		}
		r.mtimes[k.Inode] = a.Mtime
		return nil
	})
}

func (r *fsRun) slotsKept() error {
	return r.each(func(k *meta.Key, v []byte) error {
		if k.Tag != meta.InodeEntry {
			return nil
		}
		ino, mode, err := meta.DecodeEntry(v)
		if err != nil {
			return err
		}
		if mode != meta.ModeDir && ino>>meta.SeqBits != k.Inode>>meta.SeqBits {
			return fmt.Errorf("file %x has its entry %q in directory %x", ino, k.Name, k.Inode)
		}
		return nil
	})
}

func (r *fsRun) noCycle() error {
	up := map[uint64]uint64{}
	if err := r.each(func(k *meta.Key, v []byte) error {
		if k.Tag != meta.InodeParent {
			return nil
		}
		p, err := meta.DecodeParent(v)
		up[k.Inode] = p
		return err
	}); err != nil {
		return err
	}
	for _, dir := range slices.Sorted(maps.Keys(up)) {
		d := dir
		for range len(up) + 1 {
			if d = up[d]; d == dir {
				return fmt.Errorf("directory %x is its own ancestor", dir)
			}
		}
	}
	return nil
}

// metaRep is a storage node's MetaStore replica: its shard, the server answering the daemon's
// Txns, and the rounds its disk made durable, which a restart replays.
type metaRep struct {
	r      *fsRun
	i      int
	loop   core.NodeID
	s      *meta.Shard
	sv     *meta.Server
	out    meta.Output
	recs   [][]byte
	flight []byte
}

func (m *metaRep) boot(n *sim.Node) {
	if m.s != nil {
		m.s.Close()
	}
	st := raftshard.NewStorage()
	if err := st.ApplySnapshot(raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 1, Term: 1, ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}}}}); err != nil {
		m.r.t.Fatal(err)
	}
	for _, rec := range m.recs {
		if err := st.Replay(disklog.BlobRaftState, append([]byte{1}, rec...)); err != nil {
			m.r.t.Fatal(err)
		}
	}
	rng := n.Fork()
	first := m.s == nil
	m.flight, m.sv = nil, meta.NewServer()
	m.loop = n.Start(sim.MachineFunc(m.onEvent), n.Disk(0))
	n.Call(m.loop, func(ctx *core.Context, acts []core.Action) []core.Action {
		// A new group's grantors have granted nothing, so they start with no restart wait.
		start := lease.Now(ctx)
		if first {
			start = lease.Time{}
		}
		cfg := meta.Config{Raft: raftshard.Config{Group: runGroup, ID: uint64(m.i) + 1, Storage: st, Seed: rng.Uint64(), Timer: 1,
			Holder: lease.NewHolder(lease.DefaultConfig), Grantor: lease.NewGrantor(lease.DefaultConfig, rng.Uint64(), start)},
			Tree: lsm.Config{MaxPiece: 64 << 10}}
		for j := range 3 {
			cfg.Raft.Peers = append(cfg.Raft.Peers, raftshard.Peer{ID: uint64(j) + 1, Node: core.NodeID{Node: uint32(j)}})
		}
		var err error
		if m.s, err = meta.NewShard(cfg); err != nil {
			m.r.t.Fatal(err)
		}
		m.out.Reset()
		m.s.Start(ctx, &m.out)
		return m.finish(ctx, acts)
	})
}

func (m *metaRep) onEvent(ctx *core.Context, ev *core.Event, acts []core.Action) []core.Action {
	m.out.Reset()
	switch {
	case ev.Kind == core.EvNetRecv && !ev.Msg.Group():
		return sink(ctx, ev, acts)
	case ev.Kind == core.EvNetRecv && len(ev.Msg.Inline()) > 9 && ev.Msg.Inline()[9] == meta.KindTxn:
		acts = m.sv.Serve(ctx, ev, m.s, &m.out, acts)
	case ev.Kind == core.EvDiskDone:
		if ev.Err != core.DiskOK {
			panic(ev.Err)
		}
		m.recs, m.flight = append(m.recs, m.flight), nil
		m.s.Durable(ctx, uint64(ev.IO), &m.out)
	default:
		m.s.OnEvent(ctx, ev, &m.out)
	}
	return m.finish(ctx, acts)
}

func (m *metaRep) finish(ctx *core.Context, acts []core.Action) []core.Action {
	acts = m.sv.Reply(ctx, m.out.Answers, acts)
	acts = append(acts, m.out.Raft.Actions...)
	if f := m.out.Raft.Flush; f != 0 {
		m.flight = raftshard.AppendRound(nil, &m.out.Raft.Round)
		acts = append(acts, core.Action{Kind: core.ActDiskFsync, IO: core.IOID(f)})
	}
	if idx, _ := m.s.Applied(); idx > m.r.maxApplied {
		m.r.maxApplied = idx
	}
	return acts
}

// devRep is a storage node's chunk device, its log and reserve noted once it opened.
type devRep struct {
	r       *fsRun
	i       int
	loop    core.NodeID
	d       *nodepkg.Device
	out     nodepkg.DeviceOutput
	log     uint64
	reserve uint64
	opened  bool
	handed  bool
}

func (dv *devRep) boot(n *sim.Node) {
	dv.opened, dv.handed = false, false
	rng := n.Fork()
	dv.loop = n.Start(sim.MachineFunc(dv.onEvent), n.Disk(0))
	n.Call(dv.loop, func(ctx *core.Context, acts []core.Action) []core.Action {
		cfg := storetest.Config()
		cfg.Log.NoReserve = false
		dv.out.Reset()
		dv.d = nodepkg.NewDevice(ctx, nodepkg.DeviceConfig{Device: uint64(dv.i) + 1, Blocks: runBlocks, Class: "ssd", Loop: 0, Store: cfg,
			Seed: rng.Uint64(), Group: 1, Ctl: core.NodeID{Node: devAt(dv.i).Node, Loop: 1}}, &dv.out)
		return dv.finish(acts)
	})
}

func (dv *devRep) onEvent(ctx *core.Context, ev *core.Event, acts []core.Action) []core.Action {
	dv.out.Reset()
	dv.d.OnEvent(ctx, ev, &dv.out)
	return dv.finish(acts)
}

func (dv *devRep) finish(acts []core.Action) []core.Action {
	if o := dv.out.Opened; o != nil {
		dv.log, dv.reserve, dv.opened = o.Log, o.Reserve, true
	}
	if dv.out.Failed {
		panic(fmt.Sprintf("device %d failed", dv.i))
	}
	return append(acts, dv.out.Actions...)
}

// daemon is one start of the daemon on node 3: setup, a mount that unlinks and creates the files
// again before the processes start, then m serving the sim.Mount.
type daemon struct {
	r      *fsRun
	n      *sim.Node
	loop   core.NodeID
	setup  *Machine
	m      *Machine
	mc     *meta.Client
	rout   meta.RouterOutput
	cl     *chunkstore.Client
	cout   chunkstore.ClientOutput
	mount  *sim.Mount
	frames [][]byte
	ready  bool
	procs  []*runProc
	// steps are setup's requests in order, and step the next to send.
	steps []fuseproto.Request
	step  int
	root  meta.FS
	txns  map[uint64]sentTxn
	held  []heldRead
	armed bool
	ctx   *core.Context
	acts  []core.Action
}

type sentTxn struct {
	m     *Machine
	token uint64
}

type heldRead struct {
	m  *Machine
	rd Read
}

func (r *fsRun) bootDaemon(n *sim.Node) {
	r.incs++
	r.crashAt = r.s.Time() + core.Tick(r.rng.Below(uint64(faultGap)))
	cfg := Config{Volume: vol, Pool: 1, Session: runSession, Mount: uint32(2 * r.incs), SlotLimit: 1 << 20}
	d := &daemon{r: r, n: n, setup: New(cfg), txns: map[uint64]sentTxn{}}
	cfg.Mount++
	d.m = New(cfg)
	d.setup.plant, d.m.plant = r.plant, r.plant
	router, err := meta.NewRouter(&meta.ShardMap{Groups: []meta.Group{{ID: 1}, {ID: runGroup, Start: []byte{meta.SpaceCtl + 1}, Voters: []meta.Replica{{ID: 1, Node: 0}, {ID: 2, Node: 1}, {ID: 3, Node: 2}}}}}, routerTimer)
	if err != nil {
		r.t.Fatal(err)
	}
	d.mc = meta.NewClient(router, r.incs)
	d.cl = chunkstore.NewClient(chunkstore.ClientConfig{Session: runSession, Incarnation: r.incs})
	d.mount = sim.NewMount(r.s.Time, r.hist)
	r.d = d
	for i := range r.files {
		f := &r.files[i]
		f.writer = nil
		if f.ino == 0 || r.rng.Chance(1, 2) {
			if f.ino != 0 {
				d.steps = append(d.steps, fuseproto.Request{Opcode: fuseproto.OpUnlink, NodeID: meta.RootInode, Name: []byte(f.name)})
			}
			d.steps = append(d.steps, fuseproto.Request{Opcode: fuseproto.OpCreate, NodeID: meta.RootInode, Name: []byte(f.name), Mode: uint32(meta.ModeFile | 0o644)})
		}
	}
	d.loop = n.Start(sim.MachineFunc(d.onEvent), nil)
	n.AddActor(d.loop, d)
	for range 4 {
		r.clients++
		pr := &runProc{d: d, p: d.mount.Process(r.clients - 1), client: r.clients - 1}
		d.procs = append(d.procs, pr)
		n.AddActor(d.loop, pr)
	}
	n.Call(d.loop, func(ctx *core.Context, acts []core.Action) []core.Action {
		return d.pass(ctx, acts, func() {
			for i := range 3 {
				d.cl.SetAddr(uint32(i)+1, devAt(i))
			}
			d.chunked(func(ctx *core.Context, o *chunkstore.ClientOutput) { d.cl.SetTable(ctx, runTable(), o) })
			d.next()
		})
	})
}

// pass runs f as one pass of the daemon's loop and returns the actions it took.
func (d *daemon) pass(ctx *core.Context, acts []core.Action, f func()) []core.Action {
	d.ctx, d.acts = ctx, acts
	f()
	for fr, ok := d.mount.Read(); ok; fr, ok = d.mount.Read() {
		d.frames = append(d.frames, fr)
	}
	return d.acts
}

// next sends setup's next request, or once none is left lets the processes start.
func (d *daemon) next() {
	if d.step == 0 && d.root.Txn.Ops == nil {
		d.root.Volume = vol
		if err := d.root.Root(&rootAttr); err != nil {
			panic(err)
		}
		d.sendRoot()
		return
	}
	if d.step == len(d.steps) {
		d.ready = true
		return
	}
	d.step++
	rq := d.steps[d.step-1]
	rq.Unique = 2 * uint64(d.step)
	d.feed(d.setup, func(o *Output) { d.setup.Request(d.ctx, d.realtime(), &rq, o) })
}

// sendRoot sends the Txn that creates the volume's root once, whose failed check on a later
// start says it is there.
func (d *daemon) sendRoot() {
	var tok uint64
	var err error
	d.routed(func(o *meta.RouterOutput) { tok, err = d.mc.Txn(d.ctx, &d.root.Txn, o) })
	if err != nil {
		panic(err)
	}
	d.txns[tok] = sentTxn{}
}

func (d *daemon) realtime() int64 { return realtime + int64(d.ctx.Now)*1000 }

func (d *daemon) onEvent(ctx *core.Context, ev *core.Event, acts []core.Action) []core.Action {
	return d.pass(ctx, acts, func() {
		switch {
		case ev.Kind == core.EvNetRecv && ev.Msg.Group():
			d.routed(func(o *meta.RouterOutput) { d.mc.OnEvent(ctx, ev, o) })
		case ev.Kind == core.EvNetRecv:
			d.chunked(func(ctx *core.Context, o *chunkstore.ClientOutput) { d.cl.OnEvent(ctx, ev, o) })
		case ev.Kind == core.EvTimer && ev.Timer == routerTimer:
			d.routed(func(o *meta.RouterOutput) { d.mc.OnEvent(ctx, ev, o) })
		case ev.Kind == core.EvTimer && ev.Timer == chunkstore.ClientTimer:
			d.chunked(func(ctx *core.Context, o *chunkstore.ClientOutput) { d.cl.OnEvent(ctx, ev, o) })
		case ev.Kind == core.EvTimer && ev.Timer == readTimer:
			d.armed = false
			held := d.held
			d.held = nil
			for _, h := range held {
				d.read(h.m, h.rd)
			}
		case ev.Kind == core.EvPeerLost:
			d.routed(func(o *meta.RouterOutput) { d.mc.OnEvent(ctx, ev, o) })
			d.chunked(func(ctx *core.Context, o *chunkstore.ClientOutput) { d.cl.OnEvent(ctx, ev, o) })
		}
	})
}

// feed runs f on machine m and makes the calls its output asks for, the chunk calls first, as
// their data aliases the machine's memory only until they are made.
func (d *daemon) feed(m *Machine, f func(o *Output)) {
	var o Output
	f(&o)
	for _, c := range o.Chunks {
		d.chunk(c)
	}
	for _, s := range o.Txns {
		d.txn(m, s)
	}
	for b := o.Replies; len(b) > 0; b = b[binary.LittleEndian.Uint32(b):] {
		d.reply(m, b[:binary.LittleEndian.Uint32(b)])
	}
	for _, rd := range o.Reads {
		d.read(m, rd)
	}
}

func (d *daemon) chunk(c ChunkCall) {
	var err error
	if c.Read {
		d.chunked(func(ctx *core.Context, o *chunkstore.ClientOutput) {
			err = d.cl.Read(ctx, c.Token, c.File, c.Off, c.N, o)
		})
	} else {
		r := d.r
		last := (c.Off + uint64(len(c.Data)) - 1) / core.ChunkSize
		l, ok := r.last[uint64(c.File.Inode)]
		if !ok || l < last {
			r.last[uint64(c.File.Inode)] = last
		}
		if r.rng.Chance(1, 32) || !ok && r.rng.Chance(1, 2) {
			r.crashAt = min(r.crashAt, r.s.Time()+core.Tick(r.rng.Below(3000)))
		}
		d.chunked(func(ctx *core.Context, o *chunkstore.ClientOutput) {
			err = d.cl.Write(ctx, c.Token, c.File, c.Off, c.Data, false, o)
		})
	}
	if err != nil {
		panic(fmt.Sprintf("chunk call %+v: %v", c, err))
	}
}

// chunked runs f on the chunk client and hands each completion to the mount's machine, as
// setup makes no chunk calls.
func (d *daemon) chunked(f func(ctx *core.Context, o *chunkstore.ClientOutput)) {
	d.cout.Reset()
	ctx := *d.ctx
	ctx.Pool = detachPool{BufPool: d.ctx.Pool, s: d.r.s, node: clientNode}
	f(&ctx, &d.cout)
	d.acts = append(d.acts, d.cout.Actions...)
	done := slices.Clone(d.cout.Done)
	for i := range done {
		d.feed(d.m, func(o *Output) { d.m.ChunkDone(d.ctx, &done[i], o) })
	}
}

func (d *daemon) txn(m *Machine, s TxnSend) {
	r := d.r
	if s.Txn.Replay != nil {
		for _, op := range s.Txn.Ops {
			var k meta.Key
			if op.Kind != meta.OpPut || meta.DecodeKey(op.Key, &k) != nil || k.Tag != meta.InodeEntry {
				continue
			}
			ino, _, err := meta.DecodeEntry(op.Value)
			if prev, ok := r.creates[ino]; err == nil && ok && prev != string(s.Txn.Replay) && r.idErr == nil {
				r.idErr = fmt.Errorf("inode %x created by request %x and by %x", ino, prev, s.Txn.Replay)
			}
			r.creates[ino] = string(s.Txn.Replay)
		}
	}
	var tok uint64
	var err error
	d.routed(func(o *meta.RouterOutput) { tok, err = d.mc.Txn(d.ctx, s.Txn, o) })
	if err != nil {
		panic(fmt.Sprintf("the router refused %+v: %v", *s.Txn, err))
	}
	d.txns[tok] = sentTxn{m, s.Token}
}

// routed runs f on the meta client and hands each answer to the machine that sent its Txn.
func (d *daemon) routed(f func(o *meta.RouterOutput)) {
	d.rout.Reset()
	f(&d.rout)
	d.acts = append(d.acts, d.rout.Actions...)
	answers := slices.Clone(d.rout.Answers)
	for i := range answers {
		a := &answers[i]
		s, ok := d.txns[a.Token]
		if !ok {
			continue
		}
		delete(d.txns, a.Token)
		if s.m == nil {
			if errResend(a.Err) {
				d.sendRoot()
			} else {
				d.next()
			}
			continue
		}
		d.feed(s.m, func(o *Output) { s.m.TxnDone(d.ctx, s.token, &a.Result, a.Err, o) })
	}
}

// read serves rd from the current state, or holds it for readRetry while none is.
func (d *daemon) read(m *Machine, rd Read) {
	s := d.r.current()
	if s == nil {
		rd.Start, rd.End = bytes.Clone(rd.Start), bytes.Clone(rd.End)
		d.held = append(d.held, heldRead{m, rd})
		if !d.armed {
			d.armed = true
			d.acts = append(d.acts, core.Action{Kind: core.ActSetTimer, Timer: readTimer, At: d.ctx.Now + readRetry})
		}
		return
	}
	var res meta.ScanResult
	if err := s.Scan(rd.Start, rd.End, rd.Limit, &res); err != nil {
		panic(err)
	}
	d.feed(m, func(o *Output) { m.ReadDone(d.ctx, rd.Token, res.KVs, o) })
}

// reply writes a reply to the mount, or takes a setup reply and sends setup's next request.
func (d *daemon) reply(m *Machine, frame []byte) {
	if m == d.m {
		if err := d.mount.Write(frame); err != nil {
			panic(fmt.Sprintf("the mount refused a reply: %v", err))
		}
		return
	}
	h, body, err := fuseproto.DecodeReply(frame)
	if err != nil {
		panic(err)
	}
	rq := d.steps[d.step-1]
	if rq.Opcode == fuseproto.OpCreate {
		if h.Error != 0 {
			panic(fmt.Sprintf("setup's create of %q failed with errno %d", rq.Name, -h.Error))
		}
		e, err := fuseproto.DecodeEntry(body[:min(len(body), 128)])
		if err != nil {
			panic(err)
		}
		for i := range d.r.files {
			if f := &d.r.files[i]; f.name == string(rq.Name) && f.ino != e.NodeID {
				f.ino, f.size = e.NodeID, 0
			}
		}
	}
	d.next()
}

// Ready reports a request the kernel queued, which the daemon reads in order.
func (d *daemon) Ready() bool { return len(d.frames) > 0 }

func (d *daemon) Next(ctx *core.Context, acts []core.Action) []core.Action {
	return d.pass(ctx, acts, func() {
		fr := d.frames[0]
		d.frames = d.frames[1:]
		var rq fuseproto.Request
		if err := fuseproto.Decode(fr, &rq); err != nil {
			panic(err)
		}
		if rq.Opcode == fuseproto.OpInit {
			flags := uint64(fuseproto.FlagAsyncRead | fuseproto.FlagBigWrites | fuseproto.FlagAutoInvalData | fuseproto.FlagParallelDirops | fuseproto.FlagMaxPages)
			if err := d.mount.Write(fuseproto.AppendInit(nil, rq.Unique, &fuseproto.InitOut{Major: 7, Minor: 45, Flags: flags, MaxWrite: 128 << 10, MaxPages: 32})); err != nil {
				panic(err)
			}
			return
		}
		d.feed(d.m, func(o *Output) { d.m.Request(ctx, d.realtime(), &rq, o) })
	})
}

// runProc is a process of the mount: it opens a file, reads it, reads it through a mapping, or as
// the file's one writer writes it whole and fsyncs, then closes it, idle up to 2 ms between calls.
type runProc struct {
	d             *daemon
	p             *sim.Process
	open, closing bool
	file          int
	id            uint64
	written, size int64
	left          int
	wake          core.Tick
	// client is its number in the history, op the index of its last call, and failed set once a
	// call failed, after which it stops.
	client uint32
	op     int
	failed bool
}

func (pr *runProc) Ready() bool {
	return pr.d.ready && !pr.failed && !pr.p.Busy() && pr.d.n.Context().Now >= pr.wake
}

func (pr *runProc) Wake() (core.Tick, bool) { return pr.wake, pr.d.ready && !pr.failed && !pr.p.Busy() }

func (pr *runProc) Next(ctx *core.Context, acts []core.Action) []core.Action {
	return pr.d.pass(ctx, acts, func() {
		before := len(pr.d.r.hist.Ops())
		pr.next()
		if len(pr.d.r.hist.Ops()) > before {
			pr.op = before
		}
		pr.wake = ctx.Now + core.Tick(pr.d.r.rng.Below(2000))
	})
}

func (pr *runProc) next() {
	r, p := pr.d.r, pr.p
	if p.Errno != 0 {
		if r.errnoErr == nil {
			r.errnoErr = fmt.Errorf("client %d's system call %+v failed with errno %d", pr.client, r.hist.Ops()[pr.op], p.Errno)
		}
		pr.failed = true
		return
	}
	if pr.closing {
		if f := &r.files[pr.file]; f.writer == pr {
			f.writer = nil
		}
		pr.open, pr.closing, pr.id = false, false, 0
	}
	rng := r.rng
	switch f := &r.files[pr.file]; {
	case !pr.open:
		pr.file, pr.open, pr.left = int(rng.Below(2)), true, int(rng.Range(1, 4))
		f = &r.files[pr.file]
		flags := uint32(0)
		if f.writer == nil && rng.Chance(1, 2) {
			r.ids++
			f.writer, pr.id, pr.written, flags = pr, r.ids, 0, 2
			pr.size = max(f.size, 8*int64(rng.Range(1, 1536)))
		}
		p.Open(f.name, flags)
	case pr.id != 0 && pr.written < pr.size && (pr.left == 0 || rng.Chance(3, 4)):
		n := min(pr.size-pr.written, 8*int64(rng.Range(1, 1024)))
		p.Write(pr.written, words(pr.id, pr.written, int(n)))
		pr.written += n
		f.size = max(f.size, pr.written)
	case pr.left > 0:
		pr.left--
		off, n := 8*int64(rng.Below(uint64(f.size+sim.PageSize)/8)), 8*int(rng.Range(1, 1024))
		switch k := rng.Below(5); {
		case k == 0 && pr.id != 0:
			p.Fsync()
		case k < 2:
			p.MapRead(off, n)
		default:
			p.Read(off, n)
		}
	default:
		p.Close()
		pr.closing = true
	}
}

// words is n bytes at off as a handle id writes them for CloseToOpen.
func words(id uint64, off int64, n int) []byte {
	b := make([]byte, n)
	for i := 0; i+8 <= n; i += 8 {
		binary.LittleEndian.PutUint64(b[i:], id<<32|uint64(off+int64(i))/8)
	}
	return b
}

// detachPool frees a buffer only after copying it into each send of it still in the network.
type detachPool struct {
	core.BufPool
	s    *sim.Scheduler
	node uint32
}

func (p detachPool) Free(b core.IOBuf) {
	p.s.Detach(p.node, b)
	p.BufPool.Free(b)
}

// The mount's processes on one node, against a three-replica shard and three chunk replicas,
// across partitions, storage nodes rebooted and the daemon crashed and restarted: histories of
// opens by path keep close-to-open, every system call succeeds, no inode is handed out twice, no
// mtime goes backwards, every file's entry is in its directory's slot, no directory is its own
// ancestor, at each daemon crash every chunk it wrote lies below alloc_end or in its purge
// entry, and at the end of the quiet time no system call or request has waited past liveWait.
func TestAMountsHistoriesKeepTheirInvariantsAcrossCrashes(t *testing.T) {
	seeds.Run(t, 1, 10, func(seed uint64) {
		r := newFSRun(t, seed, plants{})
		if err := r.run(); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		if n := len(r.hist.Ops()); n < 200 || r.crashes == 0 {
			t.Fatalf("seed %d: %d operations, %d daemon crashes", seed, n, r.crashes)
		}
	})
}

// Planted, a raise of alloc_end after the chunk write past it leaves, at a daemon crash between
// them, a chunk that neither alloc_end nor the purge entry of its file covers; seed 1 catches it,
// as each of the first 30 seeds does.
func TestARaiseAfterItsChunkWriteFailsTheMountsRun(t *testing.T) {
	r := newFSRun(t, 1, plants{raiseAfterWrite: true})
	r.inv.Only(invPurgeMisses)
	if err := r.run(); err == nil || !strings.Contains(err.Error(), invPurgeMisses) {
		t.Fatalf("the planted run gave %v", err)
	}
}
