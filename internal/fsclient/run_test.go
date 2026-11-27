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
	"github.com/nvcnvn/rock-solid/internal/lease/leasetest"
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
	invErrno       = "every system call of the run succeeds, or fails EIO under a fault injected while it ran"
	invLive        = "once faults heal, no system call or request waits longer than liveWait"
	invIDs         = "a fenced session's chunk client numbers its requests above every ID the old one took"
	invReadErr     = "a Read answered with an error that lasts fails its request, and one answered with any other goes again"
)

const (
	runGroup  = 5
	runBlocks = 1 << 18
	runFor    = core.Tick(30_000_000)
	// quietFor is the run's time without faults after them, long enough for an election and the
	// router's wait to retry a Txn it held for one.
	quietFor = core.Tick(3_000_000)
	// faultGap is the most time between two daemon crashes, and between two reboots, and fenceGap
	// between two fences.
	faultGap = core.Tick(8_000_000)
	fenceGap = core.Tick(1_000_000)
	// routerTimer is the client loop's router's timer.
	routerTimer core.TimerID = 0x66737274
	// readTimer is the client loop's timer for held reads, and readLatency the most a read is held
	// before it is sent, drawn for each.
	readTimer   core.TimerID = 0x66737264
	readLatency              = core.Tick(1000)
	// realtime is the wall clock's reading at CLOCK_BOOTTIME 0, in nanoseconds.
	realtime = int64(1_700_000_000_000_000_000)
	// liveWait is how long, at the end of the quiet time, a system call or a request in a machine
	// may have waited.
	liveWait = core.Tick(100_000)
)

// fsRun is one node's mount against a three-replica MetaStore shard and three chunk devices.
// Nodes 0 to 2 each run a meta.Shard replica on loop 1, its Txns and Reads served over the meta
// wire, and renew node-pair leases with each other; nodes 3 to 5 each run a node.Device, whose
// placement table puts four PGs of one member on them. The daemon on node 6 runs the Mount that
// ships, behind a sim.Mount whose processes open the root directory's two files by name, read,
// read through a mapping, write the whole file and fsync, one writer of a file at a time. Faults:
// partitions, and up to faultGap apart until rebootsEnd a replica's node rebooted, so leaders
// change, and the daemon crashed, also within 3 ms of a chunk write it sends, half the time for a
// file's first and one time in 32 for another; it restarts in a new session that, before its
// processes start, creates each file not yet made and, with even odds, unlinks and creates each
// other again. While the processes run, until runFor: the session fenced up to fenceGap apart, and
// within 1 ms of one write in 16; one allocation in allocOdds refused; and one Read in
// readErrOdds answered with an error, half of them lasting. A system call such a fault overlaps
// may fail EIO, a read may come back short, and the process goes on. The devices do not crash: a
// restarted primary asks the control plane, which the run does not have, for a new interval.
//
// The daemon holds each read's send for up to readLatency, so requests arrive during fetches and
// the machine's stale-attribute rule runs.
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
	// crashAt is when the daemon crashes next, fenceAt when its session is fenced and rebootAt
	// when a replica's node reboots.
	crashAt, fenceAt, rebootAt core.Tick
	crashes                    int
	// session is the daemon's node's: each start and each fence takes the next.
	session uint64
	// faultAt are the times of the faults that may fail a system call in flight with EIO: fences,
	// refused allocations and Reads answered with an error that lasts.
	faultAt []core.Tick
	// abandons counts fences with chunk I/O in flight, lasting and transient the Read errors the
	// mount took of each kind.
	abandons, lasting, transient int
	// ended is set for the check at the end of the quiet time.
	ended bool
	// idErr, errnoErr, purgeErr, readErr and idsErr are the first inode handed out twice, system
	// call failed, chunk a crash left past alloc_end and the purge entry, Read error handled
	// against its kind and fenced session's chunk client numbering below the old one's.
	idErr, errnoErr, purgeErr, readErr, idsErr error
	scan                                       meta.ScanResult
}

// runFile is a name of the root directory: its inode, the most any writer of it wrote, and the
// handle writing it.
type runFile struct {
	name   string
	ino    uint64
	size   int64
	writer *runProc
}

// rebootsEnd is when replicas' nodes stop rebooting: one rebooted then is down a heartbeat and
// grants no vote for its restart wait, so a quorum votes again from runFor.
var rebootsEnd = runFor - sim.Heartbeat - lease.DefaultConfig.Lease

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
	r.inv.Register(invReadErr, func() error { return r.readErr })
	r.inv.Register(invIDs, func() error { return r.idsErr })
	r.inv.Register(invCloseToOpen, func() error {
		if i, broken := sim.CloseToOpen(r.unfaulted()); broken {
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
// more without faults, Step's own partitions included, checking the invariants every 2,000 steps and at the end.
func (r *fsRun) run() error {
	for steps := 0; ; steps++ {
		now := r.s.Time()
		if now >= runFor+quietFor {
			break
		}
		if now < runFor {
			r.faults()
			if !r.s.Step() {
				break
			}
		} else {
			for a := range uint32(clientNode + 1) {
				for b := range uint32(clientNode + 1) {
					r.s.Heal(a, b)
				}
			}
			cs := r.s.Choices()
			if len(cs) == 0 {
				break
			}
			r.s.Take(cs[r.rng.Below(uint64(len(cs)))])
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
	for _, o := range d.g.m.ops {
		if o.req.Opcode < opReserveSlots && at-o.now > int64(liveWait)*1000 {
			return fmt.Errorf("request %+v at step %d has waited %d µs, with %d Txns in flight", o.req, o.step, (at-o.now)/1000, len(d.g.txns))
		}
	}
	return nil
}

// faults crashes the daemon at crashAt, checking the chunks it wrote first, fences its session at
// fenceAt while the run injects faults, and before rebootsEnd reboots a replica's node at
// rebootAt, each drawn again up to faultGap, or fenceGap, later.
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
	if c := r.s.Node(clientNode); c.Up() && r.injects() && now >= r.fenceAt {
		r.fenceAt = now + core.Tick(r.rng.Below(uint64(fenceGap)))
		r.fence()
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

// tend hands each opened device that has not had them since it started or the daemon's node took
// its session the membership of all three, a session table with only that session live, and the
// placement table.
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
		if !n.Up() || !dv.opened || dv.session == r.session {
			continue
		}
		dv.session = r.session
		o := nodepkg.Output{Member: true, Disks: ds, Table: &tbl,
			Hands: []nodepkg.Hand{{Device: uint64(i) + 1, Table: chunkstore.SessionTable{Version: r.session, Next: r.session + 1, Live: []uint64{r.session}}}}}
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
	pair   *leasetest.Pair
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
	// Loop 0 of a node hosts no MetaStore replica.
	n.Start(sim.MachineFunc(sink), nil)
	m.loop = n.Start(sim.MachineFunc(m.onEvent), n.Disk(0))
	n.Call(m.loop, func(ctx *core.Context, acts []core.Action) []core.Action {
		// A new group's grantors have granted nothing, so they start with no restart wait.
		start := lease.Now(ctx)
		if first {
			start = lease.Time{}
		}
		h, g := lease.NewHolder(lease.DefaultConfig), lease.NewGrantor(lease.DefaultConfig, rng.Uint64(), start)
		cfg := meta.Config{Raft: raftshard.Config{Group: runGroup, ID: uint64(m.i) + 1, Storage: st, Seed: rng.Uint64(), Timer: 1, Holder: h, Grantor: g},
			Tree: lsm.Config{MaxPiece: 64 << 10}}
		var peers []core.NodeID
		for j := range 3 {
			at := core.NodeID{Node: uint32(j), Loop: 1}
			cfg.Raft.Peers = append(cfg.Raft.Peers, raftshard.Peer{ID: uint64(j) + 1, Node: at})
			if j != m.i {
				peers = append(peers, at)
			}
		}
		m.pair = leasetest.New(h, g, peers, rng)
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
	acts, isLease := m.pair.OnEvent(ctx, ev, acts)
	_, kind, _, _ := meta.Peek(&ev.Msg)
	switch {
	case isLease:
		return acts
	case ev.Kind == core.EvNetRecv && !ev.Msg.Group():
		return sink(ctx, ev, acts)
	case ev.Kind == core.EvNetRecv && kind == meta.KindTxn:
		acts = m.sv.Serve(ctx, ev, m.s, &m.out, acts)
	case ev.Kind == core.EvNetRecv && kind == meta.KindRead && m.r.injects() && m.r.rng.Chance(1, readErrOdds):
		acts = m.r.answerErr(ctx, ev, acts)
	case ev.Kind == core.EvNetRecv && kind == meta.KindRead:
		acts = m.sv.ServeRead(ctx, ev, m.s, acts)
	case ev.Kind == core.EvDiskDone:
		if ev.Err != core.DiskOK {
			panic(ev.Err)
		}
		m.recs, m.flight = append(m.recs, m.flight), nil
		m.s.Durable(ctx, uint64(ev.IO), &m.out)
	default:
		if ev.Kind == core.EvTimer {
			acts = m.pair.Tick(ctx, acts)
		}
		m.s.OnEvent(ctx, ev, &m.out)
	}
	return m.finish(ctx, acts)
}

// readErrOdds is the odds against a replica answering a Read of the processes' with an error.
const readErrOdds = 200

// readErrs are the errors a Read may be answered with, the first two lasting.
var readErrs = [...]error{meta.ErrMalformed, meta.ErrTooLarge, meta.ErrOverloaded, meta.ErrHalted}

// answerErr answers Read ev with one of readErrs.
func (r *fsRun) answerErr(ctx *core.Context, ev *core.Event, acts []core.Action) []core.Action {
	group, _, token, _ := meta.Peek(&ev.Msg)
	err := readErrs[r.rng.Below(uint64(len(readErrs)))]
	if lasting(err) {
		r.faultAt = append(r.faultAt, r.s.Time())
	}
	if ev.Msg.Buf != core.NoBuf {
		ctx.Pool.Free(ev.Msg.Buf)
	}
	a := core.Action{Kind: core.ActNetSend, Peer: ev.Peer, Reply: true}
	if err := meta.EncodeReadAnswer(ctx.Pool, group, &meta.ReadAnswer{Token: token, Err: err}, &a.Msg); err != nil {
		panic(err)
	}
	return append(acts, a)
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
	// session is the one the device was handed live.
	session uint64
}

func (dv *devRep) boot(n *sim.Node) {
	dv.opened, dv.session = false, 0
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

// daemon is one start of the daemon on node 6: the Mount serving the sim.Mount, whose processes
// start once setup's requests, sent through the same Mount after the Txn that creates the root,
// have unlinked and created the files again.
type daemon struct {
	r      *fsRun
	n      *sim.Node
	loop   core.NodeID
	g      *Mount
	out    MountOutput
	mount  *sim.Mount
	frames [][]byte
	ready  bool
	procs  []*runProc
	// steps are setup's requests in order, step the next to send, and advance set once its last
	// reply is in.
	steps   []fuseproto.Request
	step    int
	advance bool
	frame   []byte
	// rc sends the root's Txn under rootTok, from a client of its own on the same loop.
	rc      *meta.Client
	rout    meta.RouterOutput
	root    meta.FS
	rootTok uint64
	ans     meta.ReadAnswer
	txn     meta.Txn
	upd     chunkstore.Update
	held    []heldSend
	// armed is set while readTimer is set, for armedAt.
	armed   bool
	armedAt core.Tick
	ctx     *core.Context
	acts    []core.Action
}

// heldSend is a Read's send the daemon holds until at.
type heldSend struct {
	a  core.Action
	at core.Tick
}

// setupUnique is where setup's request uniques start, above any the kernel takes.
const setupUnique = 1 << 40

// metaDisk is the disk of replica i's node, which the shard map names its voter by.
func metaDisk(i int) uint32 { return uint32(11 + i) }

func (r *fsRun) bootDaemon(n *sim.Node) {
	r.incs++
	r.session++
	r.crashAt = r.s.Time() + core.Tick(r.rng.Below(uint64(faultGap)))
	r.fenceAt = r.s.Time() + core.Tick(r.rng.Below(uint64(fenceGap)))
	d := &daemon{r: r, n: n, g: NewMount(Config{Volume: vol, Pool: 1, Mount: 1, SlotLimit: 1 << 20}, 2*r.incs)}
	router, err := meta.NewRouter(&meta.ShardMap{Groups: []meta.Group{{ID: 1}, {ID: runGroup, Start: []byte{meta.SpaceCtl + 1}, Voters: []meta.Replica{{ID: 1, Node: 0}, {ID: 2, Node: 1}, {ID: 3, Node: 2}}}}}, routerTimer)
	if err != nil {
		r.t.Fatal(err)
	}
	d.rc = meta.NewClient(router, 2*r.incs+1)
	plan := meta.Group{ID: runGroup, Start: []byte{meta.SpaceCtl + 1}}
	disks := make([]meta.DiskAddr, 0, 6)
	for i := range 3 {
		d.rc.SetLoop(uint32(i), runGroup, 1)
		plan.Voters = append(plan.Voters, meta.Replica{ID: uint64(i) + 1, Node: uint32(i), Disk: metaDisk(i)})
		disks = append(disks, meta.DiskAddr{Disk: uint32(i) + 1, At: devAt(i)}, meta.DiskAddr{Disk: metaDisk(i), At: core.NodeID{Node: uint32(i), Loop: 1}})
	}
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
			d.g.SetSession(d.ctx, r.session, &d.out)
			d.g.m.plant, d.g.plant = r.plant, r.plant
			d.g.SetMembership(d.ctx, []meta.Group{{ID: 1}, plan}, disks, &d.out)
			tbl := runTable()
			d.g.SetTable(d.ctx, &tbl, nil, &d.out)
			d.root.Volume = vol
			if err := d.root.Root(&rootAttr); err != nil {
				panic(err)
			}
			d.sendRoot()
		})
	})
}

// pass runs f as one pass of the daemon's loop, with the pool the mount allocates from, flushes
// what the mount emitted and returns the actions it took.
func (d *daemon) pass(ctx *core.Context, acts []core.Action, f func()) []core.Action {
	c := *ctx
	c.Pool = runPool{BufPool: ctx.Pool, d: d}
	d.ctx, d.acts = &c, acts
	d.out.Reset()
	f()
	d.flush()
	for d.advance {
		d.advance = false
		d.out.Reset()
		d.next()
		d.flush()
	}
	for fr, ok := d.mount.Read(); ok; fr, ok = d.mount.Read() {
		d.frames = append(d.frames, fr)
	}
	return d.acts
}

// flush hands each opener the credentials of a process that holds only its request's, writes the
// mount's replies and makes its actions.
func (d *daemon) flush() {
	for i := 0; i < len(d.out.Openers); i++ {
		d.g.OpenerDone(d.ctx, d.out.Openers[i].Token, &Creds{}, &d.out)
	}
	for b := d.out.Replies; len(b) > 0; b = b[binary.LittleEndian.Uint32(b):] {
		d.reply(b[:binary.LittleEndian.Uint32(b)])
	}
	for _, buf := range d.out.Data {
		b := d.ctx.Pool.Bytes(buf)
		d.reply(b[:binary.LittleEndian.Uint32(b)])
		d.ctx.Pool.Free(buf)
	}
	for i := range d.out.Actions {
		d.act(&d.out.Actions[i])
	}
}

// act makes action a, holding a Read's send for up to readLatency, and notes the inodes a Txn
// creates and the chunks a write goes to.
func (d *daemon) act(a *core.Action) {
	if a.Kind == core.ActNetSend {
		switch _, kind, _, ok := meta.Peek(&a.Msg); {
		case ok && kind == meta.KindRead && a.Msg.Buf == core.NoBuf:
			d.hold(heldSend{*a, d.ctx.Now + core.Tick(d.r.rng.Below(uint64(readLatency)))})
			return
		case ok && kind == meta.KindTxn:
			d.created(&a.Msg)
		case !ok && a.Peer.Node >= devAt(0).Node && a.Peer.Node < clientNode:
			d.wrote(&a.Msg)
		}
	}
	d.acts = append(d.acts, *a)
}

// created checks that no inode a Txn creates was created by a request of another replay key.
func (d *daemon) created(msg *core.Msg) {
	r := d.r
	if err := meta.DecodeTxn(d.ctx.Pool, msg, &d.txn); err != nil {
		panic(err)
	}
	if d.txn.Replay == nil {
		return
	}
	for _, op := range d.txn.Ops {
		var k meta.Key
		if op.Kind != meta.OpPut || meta.DecodeKey(op.Key, &k) != nil || k.Tag != meta.InodeEntry {
			continue
		}
		ino, _, err := meta.DecodeEntry(op.Value)
		if prev, ok := r.creates[ino]; err == nil && ok && prev != string(d.txn.Replay) && r.idErr == nil {
			r.idErr = fmt.Errorf("inode %x created by request %x and by %x", ino, prev, d.txn.Replay)
		}
		r.creates[ino] = string(d.txn.Replay)
	}
}

// wrote notes the chunk a write goes to and, half the time for a file's first and one time in 32
// for another, crashes the daemon within 3 ms; one time in 16 it fences its session within 1 ms,
// while the write is in flight.
func (d *daemon) wrote(msg *core.Msg) {
	r := d.r
	frame, data := chunkstore.Frame(d.ctx.Pool, msg)
	if kind, err := chunkstore.Peek(frame); err != nil || kind != chunkstore.KindWrite {
		return
	}
	if err := chunkstore.DecodeUpdate(frame, data, &d.upd); err != nil {
		panic(err)
	}
	ino, last := uint64(d.upd.Chunk.Inode), uint64(d.upd.Chunk.Index)
	l, ok := r.last[ino]
	if !ok || l < last {
		r.last[ino] = last
	}
	if r.rng.Chance(1, 32) || !ok && r.rng.Chance(1, 2) {
		r.crashAt = min(r.crashAt, r.s.Time()+core.Tick(r.rng.Below(3000)))
	}
	if r.rng.Chance(1, 16) {
		r.fenceAt = min(r.fenceAt, r.s.Time()+core.Tick(r.rng.Below(1000)))
	}
}

// hold keeps h until its time, arming readTimer for it if the timer is not armed sooner.
func (d *daemon) hold(h heldSend) {
	d.held = append(d.held, h)
	if !d.armed || h.at < d.armedAt {
		d.armed, d.armedAt = true, h.at
		d.acts = append(d.acts, core.Action{Kind: core.ActSetTimer, Timer: readTimer, At: h.at})
	}
}

// sendRoot sends the Txn that creates the volume's root, whose failed check on a later start says
// it is there.
func (d *daemon) sendRoot() {
	d.rout.Reset()
	tok, err := d.rc.Txn(d.ctx, &d.root.Txn, &d.rout)
	if err != nil {
		panic(err)
	}
	d.rootTok = tok
	d.routedRoot()
}

// routedRoot makes the root client's actions and, once the root's Txn is answered, sends setup's
// first request, or the Txn again for an answer that promises nothing.
func (d *daemon) routedRoot() {
	d.acts = append(d.acts, d.rout.Actions...)
	for _, a := range d.rout.Answers {
		switch {
		case a.Token != d.rootTok:
		case errResend(a.Err):
			d.sendRoot()
		default:
			d.advance = true
		}
	}
}

// next sends setup's next request, or once none is left lets the processes start.
func (d *daemon) next() {
	if d.step == len(d.steps) {
		d.ready = true
		return
	}
	d.step++
	rq := d.steps[d.step-1]
	rq.Unique = setupUnique + 2*uint64(d.step)
	d.frame = fuseproto.AppendRequest(d.frame[:0], &rq)
	if err := d.g.Request(d.ctx, d.realtime(), d.frame, &d.out); err != nil {
		panic(err)
	}
}

func (d *daemon) realtime() int64 { return realtime + int64(d.ctx.Now)*1000 }

func (d *daemon) onEvent(ctx *core.Context, ev *core.Event, acts []core.Action) []core.Action {
	return d.pass(ctx, acts, func() {
		_, _, tok, _ := meta.Peek(&ev.Msg)
		switch {
		case ev.Kind == core.EvTimer && ev.Timer == readTimer:
			d.armed = false
			held := d.held
			d.held = nil
			for _, h := range held {
				if h.at > d.ctx.Now {
					d.hold(h)
				} else {
					d.acts = append(d.acts, h.a)
				}
			}
		case ev.Kind == core.EvTimer && ev.Timer == routerTimer, ev.Kind == core.EvNetRecv && ev.Msg.Group() && tok>>32 == d.rootTok>>32:
			d.rout.Reset()
			d.rc.OnEvent(d.ctx, ev, &d.rout)
			d.routedRoot()
		case ev.Kind == core.EvPeerLost:
			d.rout.Reset()
			d.rc.OnEvent(d.ctx, ev, &d.rout)
			d.routedRoot()
			d.g.OnEvent(d.ctx, ev, &d.out)
		default:
			d.answer(ev, tok)
		}
	})
}

// answer hands the mount ev, checking that a Read answered with an error that lasts fails its
// request and one answered with any other stays in hand to go again.
func (d *daemon) answer(ev *core.Event, tok uint64) {
	r := d.r
	_, kind, _, _ := meta.Peek(&ev.Msg)
	h, sent := d.g.reads[tok]
	if ev.Kind != core.EvNetRecv || kind != meta.KindReadAnswer || !sent || meta.DecodeReadAnswer(d.ctx.Pool, &ev.Msg, &d.ans) != nil || d.ans.Err == nil {
		d.g.OnEvent(d.ctx, ev, &d.out)
		return
	}
	mt, err := h.token, d.ans.Err
	d.g.OnEvent(d.ctx, ev, &d.out)
	if _, still := d.g.reads[tok]; still {
		return
	}
	_, waits := d.g.m.waits[mt]
	switch {
	case waits == lasting(err) && r.readErr == nil:
		r.readErr = fmt.Errorf("a Read answered %v left its request waiting: %v", err, waits)
	case waits:
		r.transient++
	default:
		r.lasting++
	}
}

// reply writes a reply to the mount, or takes a setup reply and sends setup's next request.
func (d *daemon) reply(frame []byte) {
	if binary.LittleEndian.Uint64(frame[8:]) < setupUnique {
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
	d.advance = true
}

// Ready reports a request the kernel queued, which the daemon reads in order.
func (d *daemon) Ready() bool { return len(d.frames) > 0 }

func (d *daemon) Next(ctx *core.Context, acts []core.Action) []core.Action {
	return d.pass(ctx, acts, func() {
		fr := d.frames[0]
		d.frames = d.frames[1:]
		if err := d.g.Request(d.ctx, d.realtime(), fr, &d.out); err != nil {
			panic(err)
		}
	})
}

// injects reports whether faults that fail system calls are injected now: while the processes
// run, before the quiet time.
func (r *fsRun) injects() bool { return r.d.ready && r.s.Time() < runFor }

// fence starts a new session on the daemon's node, which tend hands the devices, fencing the
// old one.
func (r *fsRun) fence() {
	d := r.d
	r.session++
	r.faultAt = append(r.faultAt, r.s.Time())
	d.n.Call(d.loop, func(ctx *core.Context, acts []core.Action) []core.Action {
		return d.pass(ctx, acts, func() {
			if d.g.cl.Pending() > 0 {
				r.abandons++
			}
			old := d.g.cl.LastID()
			d.g.SetSession(d.ctx, r.session, &d.out)
			if d.g.cl.LastID() < old && r.idsErr == nil {
				r.idsErr = fmt.Errorf("session %d's chunk client starts at ID %d, below the old one's %d", r.session, d.g.cl.LastID(), old)
			}
		})
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
	// writing is set while its last call is a write, whose bytes that did not land go again.
	writing bool
	left    int
	wake    core.Tick
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
	if pr.writing {
		op := r.hist.Ops()[pr.op]
		pr.written, pr.writing = int64(op.Input[1]), false
		if p.Errno == 0 {
			pr.written += int64(op.Output[1])
		}
	}
	if p.Errno != 0 {
		op := r.hist.Ops()[pr.op]
		if p.Errno != int32(EIO) || !r.faulted(op.Invoke, op.Complete) {
			if r.errnoErr == nil {
				r.errnoErr = fmt.Errorf("client %d's system call %+v failed with errno %d", pr.client, op, p.Errno)
			}
			pr.failed = true
			return
		}
		pr.failedUnder(op)
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
		pr.written, pr.writing = pr.written+n, true
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

// failedUnder takes back an open that failed under a fault: the process holds no file.
func (pr *runProc) failedUnder(op sim.Op) {
	if op.Input[0]&0xff == sim.FileOpen {
		if f := &pr.d.r.files[pr.file]; f.writer == pr {
			f.writer = nil
		}
		pr.open, pr.id = false, 0
	}
}

// unfaulted is the history with each read a fault overlapped that succeeded asking for what it
// got, as a fault may cut it short; every word it returned is still checked.
func (r *fsRun) unfaulted() []sim.Op {
	ops := slices.Clone(r.hist.Ops())
	for i := range ops {
		if k := ops[i].Input[0] & 0xff; (k == sim.FileRead || k == sim.FileMapRead) && ops[i].Output[0] == 0 && r.faulted(ops[i].Invoke, ops[i].Complete) {
			ops[i].Input[2] = ops[i].Output[1]
		}
	}
	return ops
}

// faulted reports whether a fault that may fail a system call was injected from invoke to complete.
func (r *fsRun) faulted(invoke, complete core.Tick) bool {
	i, _ := slices.BinarySearch(r.faultAt, invoke)
	return i < len(r.faultAt) && r.faultAt[i] <= complete
}

// words is n bytes at off as a handle id writes them for CloseToOpen.
func words(id uint64, off int64, n int) []byte {
	b := make([]byte, n)
	for i := 0; i+8 <= n; i += 8 {
		binary.LittleEndian.PutUint64(b[i:], id<<32|uint64(off+int64(i))/8)
	}
	return b
}

// runPool is the pool the daemon's mount allocates from. While the run injects faults it refuses
// one allocation in allocOdds, and it frees a buffer only after copying it into each send of it still
// in the network.
type runPool struct {
	core.BufPool
	d *daemon
}

// allocOdds is the odds against an allocation being refused.
const allocOdds = 100

func (p runPool) Alloc(n int) (core.IOBuf, error) {
	if r := p.d.r; r.injects() && r.rng.Chance(1, allocOdds) {
		r.faultAt = append(r.faultAt, r.s.Time())
		return core.NoBuf, &core.PoolExhaustedError{Requested: n}
	}
	return p.BufPool.Alloc(n)
}

func (p runPool) Free(b core.IOBuf) {
	p.d.r.s.Detach(clientNode, b)
	p.BufPool.Free(b)
}

// The mount's processes on one node, against a three-replica shard and three chunk replicas,
// across partitions, storage nodes rebooted and the daemon crashed and restarted: histories of
// opens by path keep close-to-open, every system call succeeds, no inode is handed out twice, no
// mtime goes backwards, every file's entry is in its directory's slot, no directory is its own
// ancestor, at each daemon crash every chunk it wrote lies below alloc_end or in its purge
// entry, and at the end of the quiet time no system call or request has waited past liveWait.
func TestAMountsHistoriesKeepTheirInvariantsAcrossCrashes(t *testing.T) {
	var abandons, lasting, transient int
	seeds.Run(t, 1, 10, func(seed uint64) {
		r := newFSRun(t, seed, plants{})
		if err := r.run(); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		if n := len(r.hist.Ops()); n < 200 || r.crashes == 0 {
			t.Fatalf("seed %d: %d operations, %d daemon crashes", seed, n, r.crashes)
		}
		abandons, lasting, transient = abandons+r.abandons, lasting+r.lasting, transient+r.transient
	})
	if seeds.Default() && (abandons < 100 || lasting < 100 || transient < 100) {
		t.Fatalf("%d fences abandoned chunk I/O in flight, %d Reads failed and %d went again", abandons, lasting, transient)
	}
}

// Planted, each of the mount's bugs under the fault that reaches it fails the run: a fenced
// session's chunk client numbering from 0, whose requests late answers to the old one's match;
// the old client's parts left in flight, and the chunks of a call the chunk client refused left
// uncompleted, either leaving a request waiting; and a Read answered with an error that lasts sent
// again.
func TestPlantedMountBugsFailTheMountsRun(t *testing.T) {
	for _, c := range []struct {
		p    plants
		seed uint64
		inv  string
	}{
		{plants{noIDBase: true}, 1, invIDs},
		{plants{noAbandon: true}, 1, invLive},
		{plants{dropRefused: true}, 3, invLive},
		{plants{retryLasting: true}, 1, invReadErr},
	} {
		r := newFSRun(t, c.seed, c.p)
		r.inv.Only(c.inv)
		if err := r.run(); err == nil || !strings.Contains(err.Error(), c.inv) {
			t.Errorf("planted %+v, seed %d: %v", c.p, c.seed, err)
		}
	}
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

// A router output that carries both a Read's answer and a Txn's hands out both, though the Read's
// answer, an UNLINK's entry found, moves the UNLINK on to a call whose own pass through the router
// takes an output of its own.
func TestOneRouterOutputHandsOutAReadsAnswerAndATxns(t *testing.T) {
	r := newFSRun(t, 1, plants{})
	var tok, ino uint64
	var key []byte
	for tok == 0 {
		if r.s.Time() >= runFor {
			t.Fatal("no setup UNLINK read its entry")
		}
		r.faults()
		r.s.Step()
		r.tend()
		g := r.d.g
		for _, k := range slices.Sorted(maps.Keys(g.reads)) {
			h := g.reads[k]
			if w, ok := g.m.waits[h.token]; ok && w.o.req.Opcode == fuseproto.OpUnlink {
				for _, f := range r.files {
					if f.name == string(w.o.req.Name) {
						tok, ino, key = k, f.ino, bytes.Clone(h.rd.Start)
					}
				}
			}
		}
	}
	const other = ^uint64(0)
	d := r.d
	g := d.g
	g.txns[other] = other
	before := len(g.reads) + len(g.txns)
	d.n.Call(d.loop, func(ctx *core.Context, acts []core.Action) []core.Action {
		return d.pass(ctx, acts, func() {
			g.routed(d.ctx, func(o *meta.RouterOutput) {
				o.Reads = append(o.Reads, meta.ReadAnswer{Token: tok, KVs: []meta.KV{{Key: key, Value: meta.AppendEntry(nil, ino, meta.ModeFile)}}})
				o.Answers = append(o.Answers, meta.Answer{Token: other, Result: meta.Result{FailedCheck: -1}})
			})
		})
	})
	if _, ok := g.txns[other]; ok {
		t.Fatal("the Txn's answer was lost")
	}
	if after := len(g.reads) + len(g.txns); after < before-1 {
		t.Fatalf("the UNLINK went on to nothing: %d reads and Txns in flight, %d before", after, before)
	}
}
