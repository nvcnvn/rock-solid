package meta

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/lease"
	"github.com/nvcnvn/rock-solid/internal/lease/leasetest"
	"github.com/nvcnvn/rock-solid/internal/lsm"
	"github.com/nvcnvn/rock-solid/internal/raftshard"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

const invOrphanOpen = "an orphaned inode is never freed while a live session has it open"

const (
	openClients = 3
	openFor     = core.Tick(20_000_000)
	openGap     = core.Tick(20_000)
	// openCrashGap is the most time between two client crashes; a crashed client's session is
	// fenced up to openFenceLag later and acknowledged up to openAckLag after that.
	openCrashGap = core.Tick(1_500_000)
	openFenceLag = core.Tick(300_000)
	openAckLag   = core.Tick(1_000_000)
	// openCutGap is the most time between two cuts of a client from the leader, each openCut long.
	openCutGap = core.Tick(2_000_000)
	openCut    = core.Tick(400_000)
	// openMoveAt is when the leader first moves, and openMoveGap the most time between two moves,
	// each a crash of its node or a transfer of its leadership to another replica.
	openMoveAt  = core.Tick(4_000_000)
	openMoveGap = core.Tick(3_000_000)
	// openMoveCut is when, after a move, a client is cut off from whichever replica then leads;
	// openLease is how long a grace period waits on a session before evicting it.
	openMoveCut = core.Tick(200_000)
	openLease   = core.Tick(1_500_000)
	openTick    = core.TimerID(0x6f70656e)
)

// openRun runs a three-replica shard on nodes 0 to 2, each serving Txns, Reads and the open
// registry through a Server, and three clients on nodes 3 to 5 that each, every openGap, look up
// the root directory's file f, create it under a new inode when it is missing, open it, and then
// release it or unlink it while it is open; each re-registers its open file and ends its
// re-registration when a new owner asks. The run is the control plane: a client's start takes a
// session, and a crashed or evicted client's session is fenced, then acknowledged, the table handed
// to every replica's server each time and whenever one asks. Faults: clients crashed up to
// openCrashGap apart and cut off from the leader for openCut up to openCutGap apart, and the leader
// moved up to openMoveGap apart, its node crashed or its leadership handed to another replica, whose
// new owner has an empty registry and a client cut off from it openMoveCut later.
type openRun struct {
	t     testing.TB
	s     *sim.Scheduler
	rng   *core.Rand
	plant regPlants
	inv   sim.Invariants
	reps  [3]*openRep
	cls   [openClients]*openClient
	tab   SessionTable
	// opens are the inodes each session has open, as its client knows; fences the sessions
	// whose fence is due, committed or not, and when.
	opens                          []openHeld
	fences                         []openFence
	next                           uint64
	freed                          int
	crashAt, cutAt, healAt, moveAt core.Tick
	cut                            [2]uint32
	err                            error
}

type openHeld struct{ inode, session uint64 }

type openFence struct {
	session uint64
	at      core.Tick
	version uint64
}

func newOpenRun(t testing.TB, seed uint64, p regPlants) *openRun {
	set := sim.DefaultSettings()
	set.PoolBytes = 16 << 20
	r := &openRun{t: t, s: sim.New(seed, set), rng: core.NewRand(seed), plant: p, next: 2, tab: SessionTable{Next: 1}}
	r.crashAt = 2_000_000 + core.Tick(r.rng.Below(uint64(openCrashGap)))
	r.cutAt = core.Tick(r.rng.Below(uint64(openCutGap)))
	r.moveAt = openMoveAt + core.Tick(r.rng.Below(uint64(openMoveGap)))
	r.inv.Register("driver contract", r.s.DriverContract)
	r.inv.Register(invOrphanOpen, r.check)
	for i := range 3 {
		r.reps[i] = &openRep{r: r, i: i}
		r.s.AddNode(storageClock, []sim.DiskSpec{{Blocks: 16, Settings: sim.DefaultDiskSettings()}}, r.reps[i].boot)
	}
	for i := range openClients {
		r.cls[i] = &openClient{r: r, i: i}
		r.s.AddNode(sim.DefaultClock, nil, r.cls[i].boot)
	}
	return r
}

func (r *openRun) run() error {
	for steps := 0; r.s.Time() < openFor; steps++ {
		r.faults()
		if !r.s.Step() {
			break
		}
		if steps%10 == 9 {
			if err := r.inv.Check(); err != nil {
				return err
			}
		}
	}
	if err := r.inv.Check(); err != nil {
		return err
	}
	if r.freed == 0 {
		return errors.New("no orphan was freed")
	}
	return nil
}

// check fails once a replica holds a purge entry, made only by freeing an orphan, for an inode
// that a session still in the table has open.
func (r *openRun) check() error {
	if r.err != nil {
		return r.err
	}
	freed := 0
	for _, m := range r.reps {
		if m.s == nil || !r.s.Node(uint32(m.i)).Up() {
			continue
		}
		var sc ScanResult
		start := mustKey(&Key{Space: SpaceFS, Volume: 1, Tag: SlotPurge, ID: 0})
		end := mustKey(&Key{Space: SpaceFS, Volume: 1, Tag: SlotPurge + 1})
		if err := m.s.Scan(start, end, 1<<20, &sc); err != nil {
			return err
		}
		freed = max(freed, len(sc.KVs))
		for _, kv := range sc.KVs {
			var k Key
			if err := DecodeKey(kv.Key, &k); err != nil {
				return err
			}
			for _, h := range r.opens {
				if h.inode == k.ID {
					return fmt.Errorf("at %d replica %d freed inode %d, which session %d has open", r.s.Time(), m.i, k.ID, h.session)
				}
			}
		}
	}
	r.freed = freed
	return nil
}

func (r *openRun) leader() int {
	for i, m := range r.reps {
		if m.s != nil && r.s.Node(uint32(i)).Up() && m.s.Raft().Leader() == uint64(i)+1 {
			return i
		}
	}
	return -1
}

// move crashes the leader's node, or has it hand its leadership to another replica.
func (r *openRun) move() {
	l := r.leader()
	if l < 0 {
		return
	}
	if r.healAt == 0 {
		r.cutAt = r.s.Time() + openMoveCut
	}
	if r.rng.Chance(1, 2) {
		r.s.Reboot(uint32(l))
		return
	}
	m, to := r.reps[l], (l+1+int(r.rng.Below(2)))%3
	msg := groupMsg(r.t, dataGroup, raftpb.Message{Type: raftpb.MsgTransferLeader, From: uint64(to) + 1, To: uint64(l) + 1, Term: m.s.Raft().Term()})
	r.s.Node(uint32(l)).Call(m.loop, func(ctx *core.Context, acts []core.Action) []core.Action {
		m.out.Reset()
		m.s.OnEvent(ctx, &core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{Node: uint32(to)}, Msg: msg}, &m.out)
		return m.finish(ctx, acts)
	})
}

func (r *openRun) faults() {
	now := r.s.Time()
	if r.healAt != 0 && now >= r.healAt {
		r.s.Heal(r.cut[0], r.cut[1])
		r.healAt, r.cutAt = 0, now+core.Tick(r.rng.Below(uint64(openCutGap)))
	}
	if r.healAt == 0 && now >= r.cutAt {
		if l := r.leader(); l >= 0 {
			r.cut = [2]uint32{uint32(l), uint32(3 + r.rng.Below(openClients))}
			r.s.Partition(r.cut[0], r.cut[1], false)
			r.healAt = now + openCut
		}
	}
	if now >= r.moveAt {
		r.moveAt = now + core.Tick(r.rng.Below(uint64(openMoveGap)))
		r.move()
	}
	if now >= r.crashAt {
		r.crashAt = now + core.Tick(r.rng.Below(uint64(openCrashGap)))
		r.crash(r.cls[r.rng.Below(openClients)])
	}
	changed := false
	for i := 0; i < len(r.fences); i++ {
		f := &r.fences[i]
		if now < f.at {
			continue
		}
		changed = true
		if f.version == 0 {
			r.tab.Version++
			f.version, f.at = r.tab.Version, now+core.Tick(r.rng.Below(uint64(openAckLag)))
			j, _ := slices.BinarySearchFunc(r.tab.Sessions, f.session, cmpSession)
			r.tab.Sessions[j].Fenced = f.version
			continue
		}
		r.tab.Acked = max(r.tab.Acked, f.version)
		r.tab.Sessions = slices.DeleteFunc(r.tab.Sessions, func(e SessionEntry) bool { return e.Fenced != 0 && e.Fenced <= r.tab.Acked })
		r.opens = slices.DeleteFunc(r.opens, func(h openHeld) bool {
			_, live := slices.BinarySearchFunc(r.tab.Sessions, h.session, cmpSession)
			return !live && h.session < r.tab.Next
		})
		r.fences = slices.DeleteFunc(r.fences, func(f openFence) bool { return f.version != 0 && f.version <= r.tab.Acked })
		i = -1
	}
	if changed {
		r.tell()
	}
}

// crash reboots c's node, whose session is fenced up to openFenceLag later.
func (r *openRun) crash(c *openClient) {
	if r.s.Node(uint32(3 + c.i)).Up() {
		r.fences = append(r.fences, openFence{session: c.session, at: r.s.Time() + core.Tick(r.rng.Below(uint64(openFenceLag)))})
		r.s.Reboot(uint32(3 + c.i))
	}
}

// evict takes an owner's ask to evict session id: it stops renewing it, so its client, cut off,
// crashes and its session is fenced.
func (r *openRun) evict(id uint64) {
	for _, c := range r.cls {
		if c.session == id {
			r.crash(c)
		}
	}
}

// tell hands the session table to every replica's server.
func (r *openRun) tell() {
	tab := r.tab.Storage()
	for _, m := range r.reps {
		if m.s == nil || !r.s.Node(uint32(m.i)).Up() {
			continue
		}
		r.s.Node(uint32(m.i)).Call(m.loop, func(ctx *core.Context, acts []core.Action) []core.Action {
			m.out.Reset()
			m.sv.SetSessions(&tab, 0)
			return m.finish(ctx, acts)
		})
	}
}

// openRep is a replica's node: its shard, its server, its node-pair leases, and the rounds its
// disk made durable, which a restart replays.
type openRep struct {
	r      *openRun
	i      int
	loop   core.NodeID
	s      *Shard
	sv     *Server
	pair   *leasetest.Pair
	out    Output
	recs   [][]byte
	flight []byte
}

func (m *openRep) boot(n *sim.Node) {
	st := bootStorage(m.r.t, raftpb.ConfState{Voters: []uint64{1, 2, 3}})
	for _, rec := range m.recs {
		if err := st.Replay(disklog.BlobRaftState, append([]byte{1}, rec...)); err != nil {
			m.r.t.Fatal(err)
		}
	}
	rng := n.Fork()
	m.flight, m.sv = nil, NewServer()
	m.sv.reg.plant, m.sv.Lease = m.r.plant, openLease
	m.loop = n.Start(sim.MachineFunc(m.onEvent), n.Disk(0))
	n.Call(m.loop, func(ctx *core.Context, acts []core.Action) []core.Action {
		h, g := lease.NewHolder(lease.DefaultConfig), lease.NewGrantor(lease.DefaultConfig, rng.Uint64(), lease.Time{})
		cfg := Config{Raft: raftshard.Config{Group: dataGroup, ID: uint64(m.i) + 1, Storage: st, Seed: rng.Uint64(), Timer: 1, Holder: h, Grantor: g},
			Tree: lsm.Config{MaxPiece: 64 << 10}}
		var peers []core.NodeID
		for j := range 3 {
			cfg.Raft.Peers = append(cfg.Raft.Peers, raftshard.Peer{ID: uint64(j) + 1, Node: core.NodeID{Node: uint32(j)}})
			if j != m.i {
				peers = append(peers, core.NodeID{Node: uint32(j)})
			}
		}
		m.pair = leasetest.New(h, g, peers, rng)
		var err error
		if m.s, err = NewShard(cfg); err != nil {
			m.r.t.Fatal(err)
		}
		m.out.Reset()
		m.s.Start(ctx, &m.out)
		return m.finish(ctx, acts)
	})
}

func (m *openRep) onEvent(ctx *core.Context, ev *core.Event, acts []core.Action) []core.Action {
	m.out.Reset()
	acts, isLease := m.pair.OnEvent(ctx, ev, acts)
	_, kind, _, _ := Peek(&ev.Msg)
	switch {
	case isLease:
		return acts
	case ev.Kind == core.EvNetRecv && kind == KindTxn:
		acts = m.sv.Serve(ctx, ev, m.s, &m.out, acts)
	case ev.Kind == core.EvNetRecv && kind == KindRead:
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

// finish replies the answers, which may leave orphans to free, answers the server's ask for a
// session table, then proposes the frees, whose answers a later call replies, and takes its asks
// to evict.
func (m *openRep) finish(ctx *core.Context, acts []core.Action) []core.Action {
	acts = m.sv.Reply(ctx, m.out.Answers, acts)
	m.out.Answers = m.out.Answers[:0]
	if ask := m.sv.FetchSessions(); ask != 0 {
		tab := m.r.tab.Storage()
		m.sv.SetSessions(&tab, ask)
	}
	acts = m.sv.Free(ctx, m.s, &m.out, acts)
	for _, id := range m.sv.Evictions(nil) {
		m.r.evict(id)
	}
	acts = m.sv.Reply(ctx, m.out.Answers, acts)
	acts = append(acts, m.out.Raft.Actions...)
	if f := m.out.Raft.Flush; f != 0 {
		m.flight = raftshard.AppendRound(nil, &m.out.Raft.Round)
		acts = append(acts, core.Action{Kind: core.ActDiskFsync, IO: core.IOID(f)})
	}
	return acts
}

// openClient is a client node's daemon: its session, the step it is at, the inode it looked up
// or has open and what it read of it, and the token it waits on; and the newest owner that asked
// it to re-register since it last started to, the term it re-registers with, the token it waits on
// for that, and whether its open in flight registers again once answered, as the mount's does: an
// owner that has since failed may have answered it.
type openClient struct {
	r       *openRun
	i       int
	c       *Client
	out     RouterOutput
	session uint64
	step    int
	inode   uint64
	attr    Attr
	open    bool
	token   uint64
	fs      FS
	rd      Read
	asked   bool
	term    uint64
	rtoken  uint64
	again   bool
	rrd     Read
}

const (
	stepIdle = iota
	stepLookup
	stepCreate
	stepOpen
	stepRelease
	stepAttr
	stepUnlink
	stepRoot
)

func (c *openClient) boot(n *sim.Node) {
	r := c.r
	c.session, c.step, c.open, c.token = r.tab.Next, stepIdle, false, 0
	c.asked, c.term, c.rtoken, c.again = false, 0, 0, false
	c.out.Reset()
	r.tab.Next++
	r.tab.Sessions = append(r.tab.Sessions, SessionEntry{ID: c.session, Node: uint32(3 + c.i)})
	r.tell()
	m := oneShard(0)
	rt, err := NewRouter(&m, readRouter)
	if err != nil {
		r.t.Fatal(err)
	}
	c.c = NewClient(rt, r.rng.Uint64()>>34+1)
	c.fs = FS{Volume: 1}
	loop := n.Start(sim.MachineFunc(c.onEvent), nil)
	n.Call(loop, func(ctx *core.Context, acts []core.Action) []core.Action {
		if c.i == 0 && c.session == 1 {
			c.root(ctx)
		}
		return append(append(acts, c.out.Actions...), core.Action{Kind: core.ActSetTimer, Timer: openTick, At: ctx.Now + openGap})
	})
}

func (c *openClient) onEvent(ctx *core.Context, ev *core.Event, acts []core.Action) []core.Action {
	c.out.Reset()
	if ev.Kind == core.EvTimer && ev.Timer == openTick {
		if c.token == 0 {
			c.act(ctx)
		}
		acts = append(acts, core.Action{Kind: core.ActSetTimer, Timer: openTick, At: ctx.Now + openGap})
	} else if _, kind, _, _ := Peek(&ev.Msg); ev.Kind == core.EvNetRecv && kind == KindReclaim {
		if _, session, term, err := DecodeReclaim(ctx.Pool, &ev.Msg); err == nil && session == c.session {
			c.asked, c.term = true, max(c.term, term)
			c.reclaim(ctx)
		}
	} else {
		c.c.OnEvent(ctx, ev, &c.out)
		c.reclaimed(ctx)
		c.settle(ctx)
	}
	return append(acts, c.out.Actions...)
}

// reclaim starts the re-registration the newest owner asked for, unless one is in flight: the
// file the client has open, then the end of it, an open in flight going again once answered. An
// owner asks again until it hears the end, so the client starts over at each ask.
func (c *openClient) reclaim(ctx *core.Context) {
	if c.rtoken != 0 || !c.asked {
		return
	}
	c.asked, c.again = false, c.again || c.step == stepOpen
	reg, start := byte(RegReclaimed), c.key(RootInode, InodeAttr, nil)
	if c.open {
		reg, start = RegReopen, c.key(c.inode, InodeAttr, nil)
	}
	c.rrd = Read{Start: start, End: append(slices.Clip(start), 0), Limit: 1, Reg: reg, Session: c.session, Mount: 1, Term: c.term}
	tok, err := c.c.Read(ctx, &c.rrd, &c.out)
	if err != nil {
		c.r.t.Fatal(err)
	}
	c.rtoken = tok
}

// reclaimed takes the answer to the re-registration step in flight, and starts the next.
func (c *openClient) reclaimed(ctx *core.Context) {
	i := slices.IndexFunc(c.out.Reads, func(a ReadAnswer) bool { return a.Token == c.rtoken })
	if c.rtoken == 0 || i < 0 {
		return
	}
	err := c.out.Reads[i].Err
	c.out.Reads = slices.Delete(c.out.Reads, i, i+1)
	c.rtoken = 0
	switch {
	case err == ErrFenced:
		return
	case err == nil && c.rrd.Reg == RegReopen:
		c.rrd = Read{Start: c.key(RootInode, InodeAttr, nil), Limit: 1, Reg: RegReclaimed, Session: c.session, Mount: 1, Term: c.term}
		c.rrd.End = append(slices.Clip(c.rrd.Start), 0)
		tok, err := c.c.Read(ctx, &c.rrd, &c.out)
		if err != nil {
			c.r.t.Fatal(err)
		}
		c.rtoken = tok
		return
	}
	c.reclaim(ctx)
}

// root creates the volume's root, until a Txn of it is answered with no error.
func (c *openClient) root(ctx *core.Context) {
	if err := c.fs.Root(&Attr{Mode: ModeDir | 0o755, Nlink: 2}); err != nil {
		c.r.t.Fatal(err)
	}
	c.txn(ctx, stepRoot)
}

func (c *openClient) key(inode uint64, tag byte, name []byte) []byte {
	return mustKey(&Key{Space: SpaceFS, Volume: 1, Inode: inode, Tag: tag, Name: name})
}

func (c *openClient) read(ctx *core.Context, start []byte, reg byte, step int) {
	c.rd = Read{Start: start, End: append(slices.Clip(start), 0), Limit: 1, Reg: reg, Session: c.session, Mount: 1}
	tok, err := c.c.Read(ctx, &c.rd, &c.out)
	if err != nil {
		c.r.t.Fatal(err)
	}
	c.token, c.step = tok, step
}

func (c *openClient) txn(ctx *core.Context, step int) {
	tok, err := c.c.Txn(ctx, &c.fs.Txn, &c.out)
	if err != nil {
		c.r.t.Fatal(err)
	}
	c.token, c.step = tok, step
}

// act starts the client's next step: a lookup of f when it has nothing open, and otherwise a
// release or, one time in two, an unlink of the file it has open.
func (c *openClient) act(ctx *core.Context) {
	switch {
	case !c.open:
		c.read(ctx, c.key(RootInode, InodeEntry, []byte("f")), RegNone, stepLookup)
	case c.r.rng.Chance(1, 2):
		c.r.opens = slices.DeleteFunc(c.r.opens, func(h openHeld) bool { return h == openHeld{c.inode, c.session} })
		c.open = false
		c.read(ctx, c.key(c.inode, InodeAttr, nil), RegRelease, stepRelease)
	default:
		c.read(ctx, c.key(c.inode, InodeAttr, nil), RegNone, stepAttr)
	}
}

// settle takes the answer to the step the client waits on, and starts the step it leads to.
func (c *openClient) settle(ctx *core.Context) {
	var kvs []KV
	var err error
	got := false
	for _, a := range c.out.Reads {
		if a.Token == c.token {
			kvs, err, got = a.KVs, a.Err, true
		}
	}
	for _, a := range c.out.Answers {
		if a.Token == c.token {
			err, got = a.Err, true
			if err == nil && a.Result.FailedCheck >= 0 {
				err = errors.New("a check failed")
			}
		}
	}
	if !got {
		return
	}
	c.out.Reads, c.out.Answers, c.token = c.out.Reads[:0], c.out.Answers[:0], 0
	step := c.step
	c.step = stepIdle
	if step == stepRoot && err != nil && err.Error() != "a check failed" {
		c.root(ctx)
		return
	}
	if step == stepOpen {
		again := c.again
		c.again = false
		if again && err == nil {
			c.read(ctx, c.key(c.inode, InodeAttr, nil), RegOpen, stepOpen)
			return
		}
	}
	if err != nil {
		return
	}
	switch step {
	case stepLookup:
		if len(kvs) == 0 {
			c.r.next++
			a := Attr{Mode: ModeFile | 0o644, Nlink: 1, AllocEnd: 1}
			if err := c.fs.Create(RootInode, RootInode, []byte("f"), c.r.next, &a, nil, int64(ctx.Now)); err != nil {
				c.r.t.Fatal(err)
			}
			c.txn(ctx, stepCreate)
			return
		}
		inode, _, err := DecodeEntry(kvs[0].Value)
		if err != nil {
			c.r.t.Fatal(err)
		}
		c.inode = inode
		c.read(ctx, c.key(inode, InodeAttr, nil), RegOpen, stepOpen)
	case stepOpen:
		if len(kvs) == 1 && DecodeAttr(kvs[0].Value, &c.attr) == nil && c.attr.Nlink > 0 {
			c.open = true
			c.r.opens = append(c.r.opens, openHeld{c.inode, c.session})
		}
	case stepAttr:
		if len(kvs) == 1 && DecodeAttr(kvs[0].Value, &c.attr) == nil && c.attr.Nlink > 0 {
			if err := c.fs.Unlink(RootInode, []byte("f"), c.inode, &c.attr, int64(ctx.Now)); err != nil {
				c.r.t.Fatal(err)
			}
			c.txn(ctx, stepUnlink)
		}
	}
}

// An orphaned inode is never freed while a session still in the table has it open, with clients
// crashed, their sessions fenced and acknowledged late, clients cut off from the leader, and the
// leader crashed or handing its leadership over.
func TestAnOrphanIsNeverFreedWhileALiveSessionHasItOpen(t *testing.T) {
	seeds.Run(t, 1, 20, func(seed uint64) {
		if err := newOpenRun(t, seed, regPlants{}).run(); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
	})
}

// An unlink that frees an open file at once (seed 1), a fenced session's opens closed when its
// fence commits rather than once it is acknowledged (seed 2), a new owner that frees orphans in
// its grace period (seed 3), and one whose grace period ends by time alone (seed 8) each free an
// orphan a live session has open.
func TestPlantedRegistryBugsFreeAnOpenOrphan(t *testing.T) {
	for _, c := range []struct {
		p    regPlants
		seed uint64
	}{
		{regPlants{unlinkOpen: true}, 1},
		{regPlants{releaseOnCommit: true}, 2},
		{regPlants{orphanInGrace: true}, 3},
		{regPlants{graceByTime: true}, 8},
	} {
		var v *sim.Violation
		if err := newOpenRun(t, c.seed, c.p).run(); !errors.As(err, &v) || v.Name != invOrphanOpen {
			t.Errorf("plant %+v, seed %d: %v", c.p, c.seed, err)
		}
	}
}
