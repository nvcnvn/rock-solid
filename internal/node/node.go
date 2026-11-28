// Package node is a storage node's machine beside its replicas: its start, its heartbeat to the
// control plane and its side of session fencing (design.md's "Sessions and fencing").
package node

import (
	"slices"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/lease"
	"github.com/nvcnvn/rock-solid/internal/meta"
)

// Interval is how often a node heartbeats the control plane, and how long it waits on a fetch
// before sending it again.
const Interval = core.Tick(1_000_000)

// The node's timers.
const (
	HeartbeatTimer core.TimerID = 0x6e626561
	FetchTimer     core.TimerID = 0x6e666574
	SessionTimer   core.TimerID = 0x6e736573
)

// Config is a node's place in the cluster: its ID, the release its format level is, the loops of
// the control-plane group's voters and its group ID, the seed of the source its start draws
// from, which the daemon takes from the operating system at each start, its role: meta, holding
// metadata and no PG member, client, holding neither, or else storage, holding both; and whether
// it holds a client session, as a node that mounts a volume does.
type Config struct {
	Node     uint32
	Release  uint32
	Ctls     []core.NodeID
	Group    uint64
	Seed     uint64
	MetaOnly bool
	Client   bool
	Session  bool
}

// Hand is a session table for the replica on Device, answering its ask Answers, 0 when unasked.
type Hand struct {
	Device  uint64
	Table   chunkstore.SessionTable
	Answers uint64
}

// Output is what a call into the node asks of its loop: actions; session tables to hand
// replicas; a placement table or delta to hand every replica, with SetTable or ApplyDelta; the
// committed disk membership and the pools' grants outstanding to hand every device; whether the node moved to another
// control-plane voter, Ctl, which every replica then reports to with SetAuthority; the metadata
// replicas to delete, each of which its last report showed had learned its removal; and with the
// membership, Member, the shard map's plans, unless an answer carried a newer map before, for
// Device.Plans.
type Output struct {
	Actions []core.Action
	Hands   []Hand
	Table   *chunkstore.Table
	Delta   *chunkstore.Delta
	Disks   []meta.DiskAddr
	Grants  []meta.PoolGrant
	Plans   []meta.Group
	Member  bool
	Moved   bool
	Ctl     core.NodeID
	Deletes []meta.ReplicaRef
}

// Reset empties the output for reuse.
func (o *Output) Reset() {
	*o = Output{Actions: o.Actions[:0], Hands: o.Hands[:0], Deletes: o.Deletes[:0]}
}

// Clone is o without its actions, copied whole, since its table, delta, membership and deletes
// alias the node's buffers, which the next message overwrites; false when it holds nothing for a
// device.
func (o *Output) Clone() (Output, bool) {
	c := Output{Disks: slices.Clone(o.Disks), Grants: slices.Clone(o.Grants), Plans: slices.Clone(o.Plans), Member: o.Member, Moved: o.Moved, Ctl: o.Ctl,
		Deletes: slices.Clone(o.Deletes)}
	for _, h := range o.Hands {
		h.Table.Live = slices.Clone(h.Table.Live)
		c.Hands = append(c.Hands, h)
	}
	if o.Table != nil {
		t, err := chunkstore.DecodeTable(chunkstore.AppendTable(nil, o.Table))
		if err != nil {
			panic(err)
		}
		c.Table = &t
	}
	if o.Delta != nil {
		d, err := chunkstore.DecodeDelta(chunkstore.AppendDelta(nil, o.Delta))
		if err != nil {
			panic(err)
		}
		c.Delta = &d
	}
	return c, c.Hands != nil || c.Table != nil || c.Delta != nil || c.Member || c.Moved || c.Deletes != nil
}

// replica is a device opened: its log, whether its replica runs, as it may once the node has
// handed it a membership naming its log, the session version its store holds, the placement epoch
// it holds, the newest ask it made, the ask the fetch in flight covers and the newest ask answered.
type replica struct {
	device, log              uint64
	runs                     bool
	held, epoch              uint64
	asked, covered, answered uint64
}

// Node is a storage node's machine. Its heartbeat reports the session version every running
// replica's store holds, which the control plane takes as their acknowledgement, and the placement
// epoch every running replica holds; a table only received does not count. A device whose log no
// membership names, as one the control plane refused, runs no replica and never pins the report. A replica's ask for the session table is one fetch from the control plane at a time,
// and an answer stands for the asks made before its fetch went out, which it hands back by number.
type Node struct {
	cfg      Config
	rng      *core.Rand
	start    uint64
	opened   []meta.OpenedDisk
	failed   []uint64
	reps     []replica
	table    chunkstore.SessionTable
	fetching bool
	fetchSeq uint64
	fetchAt  core.Tick
	report   meta.NodeReport
	// ctl is the voter the node talks to, moveAt when it may move to another, tableAt when it may
	// fetch the placement table again, wanted whether a fetch has had no table or delta since, and
	// placed and delta the last placement table and delta it received, and epoch the newest epoch
	// received.
	ctl     int
	moveAt  core.Tick
	tableAt core.Tick
	wanted  bool
	placed  chunkstore.Table
	delta   chunkstore.Delta
	epoch   uint64
	disks   meta.Membership
	// metas are the metadata replicas as each device last reported them, reps their report, halts
	// and haltRep those of them halted, dels a delete received, and mapV the newest shard map
	// version received.
	metas   []heldMeta
	metaRep []meta.ReplicaReport
	halts   []heldHalt
	haltRep []meta.HaltReport
	dels    []meta.ReplicaRef
	mapV    uint64
	// sess is the client session, if the node holds one, startAt when its start request sent may go
	// again, and beat and ans a heartbeat and an answer.
	sess    *meta.SessionClient
	startAt core.Tick
	sent    uint64
	beat    meta.SessionBeat
	ans     meta.SessionAnswer
}

// Ctl is the control-plane voter the node talks to.
func (n *Node) Ctl() core.NodeID { return n.cfg.Ctls[n.ctl] }

// move talks to the one at, if it is a voter, or else to the next, and sends it the fetch in
// flight. It moves at most once an Interval: a move re-points every replica, each sending at once,
// and while the driver backs off each send fails with another loss.
func (n *Node) move(ctx *core.Context, at core.NodeID, known bool, out *Output) {
	i := slices.Index(n.cfg.Ctls, at)
	if !known || i < 0 {
		i = (n.ctl + 1) % len(n.cfg.Ctls)
	}
	if i == n.ctl || ctx.Now < n.moveAt {
		return
	}
	n.ctl, n.moveAt = i, ctx.Now+Interval
	out.Moved, out.Ctl = true, n.Ctl()
	n.startAt = 0
	n.session(ctx, out)
	if n.fetching {
		n.fetching = false
		n.fetch(ctx, out)
	}
	if n.wanted {
		n.tableAt = 0
		n.FetchTable(ctx, out)
	}
}

// FetchTable takes a replica's ReplicaOutput.Fetch or Whole and fetches the whole placement table,
// at most once an Interval, and again on each heartbeat and move until a table or delta arrives,
// since a replica with no table never asks for one.
func (n *Node) FetchTable(ctx *core.Context, out *Output) {
	n.wanted = true
	if ctx.Now < n.tableAt {
		return
	}
	a := core.Action{Kind: core.ActNetSend, Peer: n.Ctl()}
	if meta.EncodeTableFetch(ctx.Pool, n.cfg.Group, &a.Msg) == nil {
		n.tableAt = ctx.Now + Interval
		out.Actions = append(out.Actions, a)
	}
}

// New is a node at its start, which talks to its first voter.
func New(cfg Config) *Node {
	n := &Node{cfg: cfg, rng: core.NewRand(cfg.Seed)}
	n.start = uint64(n.rng.Uint32()) << 32
	if cfg.Session {
		n.sess = meta.NewSessionClient(meta.ClientLease, cfg.Node, n.rng)
	}
	return n
}

// Recovered takes how device's recovery ended and says what to do with it: open it, format it
// under the log ID given, or leave it failed, which the node reports from then on.
func (n *Node) Recovered(device uint64, err error) (chunkstore.DeviceStart, uint64) {
	s, log := chunkstore.StartDevice(n.rng, err)
	if s == chunkstore.StartFailed {
		n.Failed(device)
	}
	return s, log
}

// Failed reports device failed from then on, as a format that ended in an error leaves it.
func (n *Node) Failed(device uint64) {
	if !slices.Contains(n.failed, device) {
		n.failed = append(n.failed, device)
	}
}

// Opened reports disk opened from then on and hands its device the newest session table the node
// has; its replica counts in the report once a membership names its log.
func (n *Node) Opened(d meta.OpenedDisk, out *Output) {
	n.opened = append(n.opened, d)
	n.reps = append(n.reps, replica{device: d.Device, log: d.Log})
	if n.table.Version != 0 || n.table.Next != 0 {
		out.Hands = append(out.Hands, Hand{Device: d.Device, Table: n.table})
	}
}

// Remove takes device's replica away, as a device opened again in the same process or a failed
// store's replica stopped leaves it: the node hands it nothing more and its report names it no
// longer, so its version never pins the report. Until then a failed store's replica is handed
// every table and counts in the report like any other.
func (n *Node) Remove(device uint64) {
	n.reps = slices.DeleteFunc(n.reps, func(r replica) bool { return r.device == device })
	n.opened = slices.DeleteFunc(n.opened, func(d meta.OpenedDisk) bool { return d.Device == device })
	n.metas = slices.DeleteFunc(n.metas, func(h heldMeta) bool { return h.device == device })
	n.halts = slices.DeleteFunc(n.halts, func(h heldHalt) bool { return h.device == device })
}

// heldHalt is a halted metadata replica a device reported.
type heldHalt struct {
	device uint64
	halt   meta.Halt
}

// Halts reports the halted metadata replicas on device, each since when the device found it
// halted, which the node's heartbeats carry from then on.
func (n *Node) Halts(device uint64, hs []meta.Halt) {
	n.halts = slices.DeleteFunc(n.halts, func(h heldHalt) bool { return h.device == device })
	for _, h := range hs {
		n.halts = append(n.halts, heldHalt{device: device, halt: h})
	}
}

// heldMeta is a metadata replica a device reported.
type heldMeta struct {
	device uint64
	rep    meta.ReplicaReport
}

// Replicas reports the metadata replicas on device, which the node's heartbeats carry from then on.
func (n *Node) Replicas(device uint64, rs []meta.ReplicaReport) {
	n.metas = slices.DeleteFunc(n.metas, func(h heldMeta) bool { return h.device == device })
	for _, r := range rs {
		r.Voters = slices.Clone(r.Voters)
		n.metas = append(n.metas, heldMeta{device: device, rep: r})
	}
}

// Start sends the first heartbeat, arms the next and fetches the placement table, which the
// node's replicas start without.
func (n *Node) Start(ctx *core.Context, out *Output) {
	n.heartbeat(ctx, out)
	n.FetchTable(ctx, out)
	n.session(ctx, out)
}

// Session is the node's client session and whether it holds its lease now; every request waits
// until it does.
func (n *Node) Session(ctx *core.Context) (uint64, bool) {
	if n.sess == nil {
		return 0, false
	}
	return n.sess.Session(), n.sess.Leased(lease.Now(ctx))
}

// Fenced takes a write refused because session is fenced: if it is the node's, the node starts a
// new session.
func (n *Node) Fenced(ctx *core.Context, session uint64, out *Output) {
	if n.sess != nil {
		n.sess.Fenced(session)
		n.session(ctx, out)
	}
}

// session sends the session's start, again each Interval until answered, or its heartbeat to the
// voter the node talks to when one is due, and arms SessionTimer for the next.
func (n *Node) session(ctx *core.Context, out *Output) {
	if n.sess == nil {
		return
	}
	var at core.Tick
	a := core.Action{Kind: core.ActNetSend, Peer: n.Ctl()}
	if req, ok := n.sess.Starting(); ok {
		if (ctx.Now >= n.startAt || req != n.sent) && meta.EncodeSessionStart(ctx.Pool, n.cfg.Group, req, &a.Msg) == nil {
			n.startAt, n.sent = ctx.Now+Interval, req
			out.Actions = append(out.Actions, a)
		}
		at = n.startAt
	} else {
		t := lease.Now(ctx)
		if n.sess.Beat(n.Ctl(), t, &n.beat) && meta.EncodeSessionBeat(ctx.Pool, n.cfg.Group, &n.beat, &a.Msg) == nil {
			out.Actions = append(out.Actions, a)
		}
		at = n.sess.Next(n.Ctl(), t)
	}
	out.Actions = append(out.Actions, core.Action{Kind: core.ActSetTimer, Timer: SessionTimer, At: max(at, ctx.Now+1)})
}

// Held records that the store on device holds session version v, as its replica's SetSessions
// left it.
func (n *Node) Held(device, v uint64) {
	if r := n.replica(device); r != nil {
		r.held = max(r.held, v)
	}
}

// HeldEpoch records that the replica on device holds placement epoch e, as its SetTable or
// ApplyDelta left it.
func (n *Node) HeldEpoch(device, e uint64) {
	if r := n.replica(device); r != nil {
		r.epoch = max(r.epoch, e)
	}
}

// Ask takes a replica's FetchSessions, ask, and fetches the table unless a fetch is in flight;
// that one's answer leaves ask unanswered, and the node fetches again.
func (n *Node) Ask(ctx *core.Context, device, ask uint64, out *Output) {
	r := n.replica(device)
	if r == nil || ask <= r.asked {
		return
	}
	r.asked = ask
	if !n.fetching {
		n.fetch(ctx, out)
	}
}

// OnEvent takes a session or placement table or a delete from a control-plane voter, dropping one from any
// other peer, the leader the voter it talks to names, or a timer. A lost connection to that voter
// moves the node to the next; a fetch goes again only on a move or FetchTimer, both at most once an
// Interval, since while the driver backs off each send fails at once with another loss.
func (n *Node) OnEvent(ctx *core.Context, ev *core.Event, out *Output) {
	switch ev.Kind {
	case core.EvPeerLost:
		if ev.Peer == n.Ctl() {
			n.move(ctx, core.NodeID{}, false, out)
		}
	case core.EvNetRecv:
		n.recv(ctx, ev, out)
	case core.EvResumed:
		if n.sess != nil {
			n.sess.Resumed()
			n.session(ctx, out)
		}
	case core.EvTimer:
		switch {
		case ev.Timer == SessionTimer:
			n.session(ctx, out)
		case ev.Timer == HeartbeatTimer:
			n.heartbeat(ctx, out)
			if n.wanted {
				n.FetchTable(ctx, out)
			}
		case ev.Timer == FetchTimer && n.fetching && ctx.Now >= n.fetchAt:
			n.fetching = false
			n.fetch(ctx, out)
		}
	}
}

func (n *Node) recv(ctx *core.Context, ev *core.Event, out *Output) {
	defer free(ctx, &ev.Msg)
	group, kind, token, ok := meta.Peek(&ev.Msg)
	if !ok || !slices.Contains(n.cfg.Ctls, ev.Peer) || group != n.cfg.Group {
		return
	}
	var err error
	switch kind {
	case meta.KindLeader:
		if at, known, err := meta.DecodeLeader(ctx.Pool, &ev.Msg); err == nil && ev.Peer == n.Ctl() {
			n.move(ctx, at, known, out)
		}
		return
	case meta.KindTable:
		if n.placed, err = meta.DecodeTable(ctx.Pool, &ev.Msg); err == nil {
			out.Table, n.tableAt, n.wanted, n.epoch = &n.placed, 0, false, max(n.epoch, n.placed.Epoch)
		}
		return
	case meta.KindDelta:
		if n.delta, err = meta.DecodeDelta(ctx.Pool, &ev.Msg); err == nil {
			out.Delta, n.tableAt, n.wanted, n.epoch = &n.delta, 0, false, max(n.epoch, n.delta.Epoch)
		}
		return
	case meta.KindDelete:
		if n.dels, err = meta.DecodeDelete(ctx.Pool, &ev.Msg, n.dels); err == nil {
			for _, d := range n.dels {
				if slices.ContainsFunc(n.metas, func(h heldMeta) bool { return h.rep.Group == d.Group && h.rep.ID == d.ID && h.rep.Removed }) {
					out.Deletes = append(out.Deletes, d)
				}
			}
		}
		return
	case meta.KindStarted:
		if id, err := meta.DecodeStarted(ctx.Pool, &ev.Msg); err == nil && n.sess != nil && n.sess.Started(token, id) {
			n.session(ctx, out)
		}
		return
	case meta.KindRenewed:
		if run, err := meta.DecodeRenewed(ctx.Pool, &ev.Msg, &n.ans); err == nil && n.sess != nil {
			n.sess.Answer(ev.Peer, run, &n.ans, lease.Now(ctx))
			n.session(ctx, out)
		}
		return
	case meta.KindDisks:
		if err = meta.DecodeDisks(ctx.Pool, &ev.Msg, &n.disks); err == nil {
			out.Disks, out.Grants, out.Member = n.disks.Disks, n.disks.Grants, true
			if n.disks.MapV >= n.mapV {
				n.mapV, out.Plans = n.disks.MapV, n.disks.Plans
			}
			for i := range n.reps {
				r := &n.reps[i]
				r.runs = r.runs || slices.ContainsFunc(n.disks.Disks, func(a meta.DiskAddr) bool { return a.Log == r.log })
			}
		}
		return
	}
	var t chunkstore.SessionTable
	if kind != meta.KindSessions || meta.DecodeSessions(ctx.Pool, &ev.Msg, &t) != nil {
		return
	}
	newer := t.Version > n.table.Version || t.Version == n.table.Version && t.Next > n.table.Next
	if newer {
		n.table = t
	}
	answer := n.fetching && token == n.token()
	for i := range n.reps {
		r := &n.reps[i]
		h := Hand{Device: r.device, Table: t}
		if answer {
			h.Answers, r.answered = r.covered, r.covered
		}
		if newer || h.Answers != 0 {
			out.Hands = append(out.Hands, h)
		}
	}
	if answer {
		n.fetching = false
		n.fetch(ctx, out)
	}
}

// fetch asks the control plane for the session table if a replica has an ask not yet answered.
func (n *Node) fetch(ctx *core.Context, out *Output) {
	if !slices.ContainsFunc(n.reps, func(r replica) bool { return r.asked > r.answered }) {
		return
	}
	n.fetchSeq++
	a := core.Action{Kind: core.ActNetSend, Peer: n.Ctl()}
	if meta.EncodeSessionFetch(ctx.Pool, n.cfg.Group, n.token(), &a.Msg) != nil {
		return
	}
	for i := range n.reps {
		n.reps[i].covered = n.reps[i].asked
	}
	n.fetching, n.fetchAt = true, ctx.Now+Interval
	out.Actions = append(out.Actions, a, core.Action{Kind: core.ActSetTimer, Timer: FetchTimer, At: n.fetchAt})
}

// token names the fetch in flight, its high bits the start's, so no earlier start's answer can
// stand for it.
func (n *Node) token() uint64 { return n.start | n.fetchSeq&0xffffffff }

// Take hands the node what device's machine output, o, asks of it.
func (n *Node) Take(ctx *core.Context, device uint64, o *DeviceOutput, out *Output) {
	switch {
	case o.Opened != nil:
		n.Opened(*o.Opened, out)
	case o.Failed:
		n.Failed(device)
	}
	if o.Ask != 0 {
		n.Ask(ctx, device, o.Ask, out)
	}
	if len(o.Evicts) != 0 {
		a := core.Action{Kind: core.ActNetSend, Peer: n.Ctl()}
		if meta.EncodeEvict(ctx.Pool, n.cfg.Group, o.Evicts, &a.Msg) == nil {
			out.Actions = append(out.Actions, a)
		}
	}
	if o.Fetch {
		n.FetchTable(ctx, out)
	}
	n.Held(device, o.Held)
	n.HeldEpoch(device, o.Epoch)
	if o.Reported {
		n.Replicas(device, o.Metas)
		n.Halts(device, o.Halts)
	}
}

// heartbeat sends the control plane the node's report and arms the next.
func (n *Node) heartbeat(ctx *core.Context, out *Output) {
	nr := &n.report
	// With no replica running this reports the version only received, safe while a store's fence
	// table starts empty in memory: a replica that starts later is handed n.table before any write.
	n.metaRep = n.metaRep[:0]
	for _, h := range n.metas {
		n.metaRep = append(n.metaRep, h.rep)
	}
	n.haltRep = n.haltRep[:0]
	for _, h := range n.halts {
		n.haltRep = append(n.haltRep, meta.HaltReport{Group: h.halt.Group, ID: h.halt.ID, Index: h.halt.Index, Age: ctx.Now - min(h.halt.Since, ctx.Now)})
	}
	*nr = meta.NodeReport{Node: n.cfg.Node, Release: n.cfg.Release, Meta: !n.cfg.Client, MetaOnly: n.cfg.MetaOnly, Epoch: n.epoch, Sessions: n.table.Version, Opened: n.opened, Failed: n.failed,
		Halts: n.haltRep, Replicas: n.metaRep}
	for _, r := range n.reps {
		if r.runs {
			nr.Sessions, nr.Epoch = min(nr.Sessions, r.held), min(nr.Epoch, r.epoch)
		}
	}
	a := core.Action{Kind: core.ActNetSend, Peer: n.Ctl(), Reply: true}
	if meta.EncodeNodeReport(ctx.Pool, n.cfg.Group, nr, &a.Msg) == nil {
		out.Actions = append(out.Actions, a)
	}
	out.Actions = append(out.Actions, core.Action{Kind: core.ActSetTimer, Timer: HeartbeatTimer, At: ctx.Now + Interval})
}

func (n *Node) replica(device uint64) *replica {
	if i := slices.IndexFunc(n.reps, func(r replica) bool { return r.device == device }); i >= 0 {
		return &n.reps[i]
	}
	return nil
}

func free(ctx *core.Context, m *core.Msg) {
	if m.Buf != core.NoBuf {
		ctx.Pool.Free(m.Buf)
	}
}
