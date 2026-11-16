// Package node is a storage node's machine beside its replicas: its start, its heartbeat to the
// control plane and its side of session fencing (design.md's "Sessions and fencing").
package node

import (
	"slices"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/meta"
)

// Interval is how often a node heartbeats the control plane, and how long it waits on a fetch
// before sending it again.
const Interval = core.Tick(1_000_000)

// The node's timers.
const (
	HeartbeatTimer core.TimerID = 0x6e626561
	FetchTimer     core.TimerID = 0x6e666574
)

// Config is a node's place in the cluster: its ID, the release its format level is, the loops of
// the control-plane group's voters and its group ID, and the seed of the source its start draws
// from, which the daemon takes from the operating system at each start.
type Config struct {
	Node    uint32
	Release uint32
	Ctls    []core.NodeID
	Group   uint64
	Seed    uint64
}

// Hand is a session table for the replica on Device, answering its ask Answers, 0 when unasked.
type Hand struct {
	Device  uint64
	Table   chunkstore.SessionTable
	Answers uint64
}

// Output is what a call into the node asks of its loop: actions; session tables to hand
// replicas; a placement table or delta to hand every replica, with SetTable or ApplyDelta; and
// whether the node moved to another control-plane voter, which every replica then reports to
// with SetAuthority at Ctl.
type Output struct {
	Actions []core.Action
	Hands   []Hand
	Table   *chunkstore.Table
	Delta   *chunkstore.Delta
	Moved   bool
}

// Reset empties the output for reuse.
func (o *Output) Reset() {
	*o = Output{Actions: o.Actions[:0], Hands: o.Hands[:0]}
}

// replica is a device whose replica runs: the session version its store holds, the placement
// epoch it holds, the newest ask it made, the ask the fetch in flight covers and the newest ask
// answered.
type replica struct {
	device                   uint64
	held, epoch              uint64
	asked, covered, answered uint64
}

// Node is a storage node's machine. Its heartbeat reports the session version every replica's
// store holds, which the control plane takes as their acknowledgement, and the placement epoch
// every replica holds; a table only received does not count. A replica's ask for the session table is one fetch from the control plane at a time,
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
	// fetch the placement table again, and placed and delta the last placement table and delta it
	// received, and epoch the newest epoch received.
	ctl     int
	moveAt  core.Tick
	tableAt core.Tick
	placed  chunkstore.Table
	delta   chunkstore.Delta
	epoch   uint64
}

// Ctl is the control-plane voter the node talks to.
func (n *Node) Ctl() core.NodeID { return n.cfg.Ctls[n.ctl] }

// move talks to the one at, if it is a voter, or else to the next, at most once an Interval: a
// move re-points every replica, each sending at once, and while the driver backs off each send
// fails with another loss.
func (n *Node) move(ctx *core.Context, at core.NodeID, known bool, out *Output) {
	i := slices.Index(n.cfg.Ctls, at)
	if !known || i < 0 {
		i = (n.ctl + 1) % len(n.cfg.Ctls)
	}
	if i == n.ctl || ctx.Now < n.moveAt {
		return
	}
	n.ctl, n.moveAt = i, ctx.Now+Interval
	out.Moved = true
}

// FetchTable takes a replica's ReplicaOutput.Fetch or Whole and fetches the whole placement table,
// at most once an Interval until a table or delta arrives.
func (n *Node) FetchTable(ctx *core.Context, out *Output) {
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

// Opened reports disk opened from then on, its replica running, and hands the replica the newest
// session table the node has.
func (n *Node) Opened(d meta.OpenedDisk, out *Output) {
	n.opened = append(n.opened, d)
	n.reps = append(n.reps, replica{device: d.Device})
	if n.table.Version != 0 || n.table.Next != 0 {
		out.Hands = append(out.Hands, Hand{Device: d.Device, Table: n.table})
	}
}

// Start sends the first heartbeat and arms the next.
func (n *Node) Start(ctx *core.Context, out *Output) { n.heartbeat(ctx, out) }

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

// OnEvent takes a session or placement table from a control-plane voter, dropping one from any
// other peer, the leader the voter it talks to names, or a timer. A lost connection to that voter
// moves the node to the next and leaves the fetch to FetchTimer: while the driver backs off, each
// send fails at once with another loss, and a resend on each would spin the loop.
func (n *Node) OnEvent(ctx *core.Context, ev *core.Event, out *Output) {
	switch ev.Kind {
	case core.EvPeerLost:
		if ev.Peer == n.Ctl() {
			n.move(ctx, core.NodeID{}, false, out)
		}
	case core.EvNetRecv:
		n.recv(ctx, ev, out)
	case core.EvTimer:
		switch {
		case ev.Timer == HeartbeatTimer:
			n.heartbeat(ctx, out)
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
			out.Table, n.tableAt, n.epoch = &n.placed, 0, max(n.epoch, n.placed.Epoch)
		}
		return
	case meta.KindDelta:
		if n.delta, err = meta.DecodeDelta(ctx.Pool, &ev.Msg); err == nil {
			out.Delta, n.tableAt, n.epoch = &n.delta, 0, max(n.epoch, n.delta.Epoch)
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

// heartbeat sends the control plane the node's report and arms the next.
func (n *Node) heartbeat(ctx *core.Context, out *Output) {
	nr := &n.report
	// With no replica this reports the version only received, safe while a store's fence table
	// starts empty in memory: a replica opened later is handed n.table before any write.
	*nr = meta.NodeReport{Node: n.cfg.Node, Release: n.cfg.Release, Epoch: n.epoch, Sessions: n.table.Version, Opened: n.opened, Failed: n.failed}
	for _, r := range n.reps {
		nr.Sessions, nr.Epoch = min(nr.Sessions, r.held), min(nr.Epoch, r.epoch)
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
