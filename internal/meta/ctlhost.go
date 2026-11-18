package meta

import (
	"cmp"
	"errors"
	"fmt"
	"slices"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/lease"
	"github.com/nvcnvn/rock-solid/internal/lsm"
	"github.com/nvcnvn/rock-solid/internal/raftshard"
)

// StepTimer is the timer a CtlHost steps its Control on, every StepInterval.
const (
	StepTimer    core.TimerID = 0x63737470
	StepInterval core.Tick    = 100_000
)

// CtlHostOutput is what one call into a CtlHost hands its loop: actions, the sessions whose start
// committed, and the errors its Control gave, for the log.
type CtlHostOutput struct {
	Actions []core.Action
	Started []SessionStart
	Errs    []error
}

// Reset empties the output for reuse.
func (o *CtlHostOutput) Reset() {
	o.Actions, o.Started, o.Errs = o.Actions[:0], o.Started[:0], o.Errs[:0]
}

// CtlHost is one replica of the control-plane group and, while it leads with an entry of its term
// applied, the Control beside it. It takes the group's messages, the sharded API's Txns to the
// group, each storage node's report, session fetch and table fetch, its replicas' asks for new
// intervals, and the chunkstore frames of replicas (reports, member timeouts, probe replies); it steps its Control every StepInterval and
// after each answer, proposing what the Control asks, and sends the nodes that reported the
// placement tables and session tables the Control hands out, a session table pushed with token 0.
// Without a Control it answers a node's report or fetch with the leader it knows. It keeps its
// group's log on a disk log of its own and tells the replica a round is durable only once the
// disk is. Once max_replay_bytes of records follow the last manifest it checkpoints the group's
// tree and compacts the log to what the tree's runs hold; a restart loads the runs and replays the
// log after the group's replay LSN before the replica starts. A disk or checkpoint that fails stops it.
// Its replica reports itself every second, to its Control or to the other members of its
// configuration; a leader drives the group toward the plan its Control hands out for it, and a
// delete naming the replica, once it learned its removal, stops the host for good.
type CtlHost struct {
	shard *Shard
	// cfg is the replica's settings until the replica starts.
	cfg Config
	// boot is the boot state a restart was given, which only a log holding no round takes, as its
	// first round.
	boot raftpb.SnapshotMetadata
	disk *raftshard.Host
	rec  *disklog.Recovery
	// replayed is set once a restart's replay gave a record.
	replayed bool
	lout     disklog.Output
	// seen counts lout's events taken.
	seen  int
	dones []raftshard.Done
	err   error
	sv    *Server
	id    uint64
	group uint64
	ctl   *Control
	// term is the term ctl was read for, or the term whose read failed.
	term  uint64
	token uint64
	// nodes are the nodes heard from within NodeDownAfter, by ID, each at the loop it last
	// reported from.
	nodes []heardNode
	// starting are the session starts handed to the Control and not yet answered, each with the
	// loop that asked.
	starting []starter
	// admins are the operator's commands the Control took and the host has not yet answered.
	admins  []adminWait
	addrs   []DiskAddr
	sout    Output
	cout    ControlOutput
	answers []Answer
	nr      NodeReport
	asks    []chunkstore.PGFields
	disks   []chunkstore.Disk
	dels    []ReplicaRef
	stepAt  core.Tick
	cp      ctlCheckpoint
	// own is the replica's report, sent at reportAt; plan is the group's plan as its replica takes it.
	own      NodeReport
	reportAt core.Tick
	plan     []raftshard.Peer
	members  []uint64
}

// reportInterval is how often a voter reports its replica, as a node's heartbeat does.
const reportInterval = core.Tick(1_000_000)

// ErrDeleted reports a host whose replica the group's leader deleted: the replica learned its
// removal and the plan that dropped it is complete.
var ErrDeleted = errors.New("meta: the control-plane group dropped this voter and its plan is complete")

// ctlOwner is the owner the group's blobs carry on the host's disk log.
const ctlOwner = 1

// heardNode is a node heard from: the loop it reported from, when, and the loop of each disk its
// last report opened.
type heardNode struct {
	at    core.NodeID
	heard core.Tick
	loops []diskLoop
}

type diskLoop struct {
	log  uint64
	loop uint32
}

type starter struct {
	at      core.NodeID
	request uint64
}

// adminWait is an operator's command the Control took: who sent it under which request, the
// command, and for a pool, the settings it waits to see committed.
type adminWait struct {
	starter
	a    Admin
	want chunkstore.Pool
}

// FormatCtlHost starts a host on a new disk, which it formats with sb. Its first round is the
// group's boot state on cfg's storage, the voters' configuration, and the replica starts on that
// storage once the round is durable. A tree whose MaxPiece passes the log's MaxBlobBytes is
// lsm.ErrBadConfig here, and stops a restart once its log is recovered.
func FormatCtlHost(ctx *core.Context, cfg Config, lc disklog.Config, sb disklog.Superblock, out *CtlHostOutput) (*CtlHost, error) {
	h := newCtlHost(cfg)
	log, err := disklog.Format(ctx, lc, sb, 1, &h.lout)
	if err != nil {
		return nil, err
	}
	if uint64(cfg.Tree.MaxPiece) > log.MaxBlobBytes() {
		return nil, lsm.ErrBadConfig
	}
	h.disk.Log = log
	if err := h.writeBoot(ctx, cfg.Raft.Storage.Base()); err != nil {
		return nil, err
	}
	h.after(ctx, out)
	return h, nil
}

// writeBoot writes the group's boot state as the log's first round.
func (h *CtlHost) writeBoot(ctx *core.Context, md raftpb.SnapshotMetadata) error {
	boot := raftshard.Round{Snapshot: raftpb.Snapshot{Metadata: md}}
	return h.disk.Write(ctx, ctlOwner, 1, &boot, &h.lout)
}

// OpenCtlHost starts recovering a host from its disk of diskBlocks blocks: the log's rounds
// replay onto an empty storage, from the boot state FormatCtlHost wrote first, and the replica
// starts once the replay ends, in the configuration its log holds, whatever voters cfg's storage
// names. A log holding no round, as a crash after format's superblocks and before its boot round
// leaves it, gets cfg's boot state written as format writes it, and the replica starts once that
// round is durable.
func OpenCtlHost(ctx *core.Context, cfg Config, lc disklog.Config, diskBlocks uint64, out *CtlHostOutput) (*CtlHost, error) {
	boot := cfg.Raft.Storage.Base()
	cfg.Raft.Storage = raftshard.NewStorage()
	h := newCtlHost(cfg)
	h.boot = boot
	rec, err := disklog.Recover(ctx, lc, diskBlocks, disklog.ModeOpen, 1, &h.lout)
	if err != nil {
		return nil, err
	}
	h.rec = rec
	h.after(ctx, out)
	return h, nil
}

func newCtlHost(cfg Config) *CtlHost {
	h := &CtlHost{cfg: cfg, disk: raftshard.NewHost(nil), sv: NewServer(), id: cfg.Raft.ID, group: cfg.Raft.Group}
	h.disk.Add(ctlOwner, cfg.Raft.Storage)
	return h
}

// start starts the replica on what the log replayed, its clock and the step timer.
func (h *CtlHost) start(ctx *core.Context, out *CtlHostOutput) error {
	s, err := NewShard(h.cfg)
	if err != nil {
		return err
	}
	h.shard, h.cfg = s, Config{}
	h.shard.Start(ctx, &h.sout)
	h.arm(ctx, out)
	return nil
}

// Close gives the replica's page slabs back; the host may not be used after.
func (h *CtlHost) Close() {
	if h.shard != nil {
		h.shard.Close()
		h.shard = nil
	}
	if h.cfg.State != nil {
		h.cfg.State.Close()
		h.cfg.State = nil
	}
}

// Err is why the host stopped for good, or nil.
func (h *CtlHost) Err() error { return h.err }

// Shard is the host's replica of the control-plane group, nil while a restart replays its log.
func (h *CtlHost) Shard() *Shard { return h.shard }

// Control is the leader's Control, or nil while the host does not lead with an entry of its term
// applied.
func (h *CtlHost) Control() *Control { return h.ctl }

// OnEvent takes a message, a timer, a disk completion, a lost peer or a resume. A host that
// stopped, or replays its log, drops every message.
func (h *CtlHost) OnEvent(ctx *core.Context, ev *core.Event, out *CtlHostOutput) {
	switch {
	case h.err != nil || h.shard == nil && ev.Kind != core.EvDiskDone:
		if ev.Kind == core.EvNetRecv {
			free(ctx, &ev.Msg)
		}
		return
	case ev.Kind == core.EvDiskDone && h.rec != nil:
		h.rec.OnEvent(ctx, ev, &h.lout)
	case ev.Kind == core.EvDiskDone || ev.Kind == core.EvTimer && ev.Timer == disklog.TrimTimer:
		h.disk.Log.OnEvent(ctx, ev, &h.lout)
	case ev.Kind == core.EvNetRecv && ev.Msg.Group():
		h.recvGroup(ctx, ev, out)
	case ev.Kind == core.EvNetRecv && h.ctl != nil:
		out.Actions = h.ctl.OnEvent(ctx, ev, out.Actions)
	case ev.Kind == core.EvNetRecv:
		free(ctx, &ev.Msg)
	case ev.Kind == core.EvTimer && ev.Timer == StepTimer:
		if ctx.Now >= h.stepAt {
			h.arm(ctx, out)
			h.fail(h.disk.Retry(ctx, &h.lout), out)
			if h.cp.pieceWait {
				h.fail(h.nextPiece(ctx), out)
			}
			h.reportOwn(ctx, out)
			h.step(ctx, out)
		}
	default:
		h.shard.OnEvent(ctx, ev, &h.sout)
		if h.ctl != nil {
			out.Actions = h.ctl.OnEvent(ctx, ev, out.Actions)
		}
	}
	h.after(ctx, out)
}

func (h *CtlHost) arm(ctx *core.Context, out *CtlHostOutput) {
	h.stepAt = ctx.Now + StepInterval
	out.Actions = append(out.Actions, core.Action{Kind: core.ActSetTimer, Timer: StepTimer, At: h.stepAt})
}

func (h *CtlHost) recvGroup(ctx *core.Context, ev *core.Event, out *CtlHostOutput) {
	if in := ev.Msg.Inline(); len(in) <= kindAt || in[kindAt] < KindTxn {
		if group, id, ok := raftshard.Asked(&ev.Msg); ok && (group != h.group || id != h.id) {
			var a core.Action
			if raftshard.Gone(&ev.Msg, ev.Peer, &a) {
				out.Actions = append(out.Actions, a)
			}
			return
		}
		h.shard.OnEvent(ctx, ev, &h.sout)
		return
	}
	group, kind, token, ok := Peek(&ev.Msg)
	switch {
	case !ok || group != h.group:
	case kind == KindTxn:
		out.Actions = h.sv.Serve(ctx, ev, h.shard, &h.sout, out.Actions)
		return
	case kind == KindNodeReport:
		h.report(ctx, ev, out)
	case kind == KindDelete:
		var err error
		if h.dels, err = DecodeDelete(ctx.Pool, &ev.Msg, h.dels[:0]); err == nil {
			h.deleted(h.dels, out)
		}
	case kind == KindAsks:
		var err error
		if h.asks, err = DecodeAsks(ctx.Pool, &ev.Msg, h.asks); err == nil && h.ctl != nil {
			h.ctl.Asked(ev.Peer.Node, h.asks)
		}
	case kind != KindSessionFetch && kind != KindTableFetch && kind != KindSessionStart && kind != KindAdmin:
	case h.ctl == nil:
		h.redirect(ctx, ev.Peer, out)
	case kind == KindTableFetch:
		h.send(ctx, ev.Peer, KindTable, out)
	case kind == KindAdmin:
		h.admin(ctx, ev, token, out)
	case kind == KindSessionStart:
		h.startSession(ctx, ev.Peer, token)
	default:
		if t, ok := h.ctl.FetchAnswer(lease.Now(ctx), h.leased(ctx)); ok {
			h.push(ctx, ev.Peer, token, &t, out)
		}
	}
	free(ctx, &ev.Msg)
}

// redirect answers a node's report or fetch with the leader this replica knows, or none, so the
// node moves to it or to its next address; a replica that leads but has no Control yet answers
// nothing, and the node asks again.
func (h *CtlHost) redirect(ctx *core.Context, to core.NodeID, out *CtlHostOutput) {
	r := h.shard.Raft()
	id := r.Leader()
	if id == h.id {
		return
	}
	at, known := r.Node(id)
	a := core.Action{Kind: core.ActNetSend, Peer: to, Reply: true}
	if EncodeLeader(ctx.Pool, h.group, at, known, &a.Msg) == nil {
		out.Actions = append(out.Actions, a)
	}
}

// report takes a node's report from the loop it names, which pushes then go to, and answers it
// with the replicas the node is to delete, if any, and the disk membership. A report that names a
// replica of this group is a voter's, taken by voterReport.
func (h *CtlHost) report(ctx *core.Context, ev *core.Event, out *CtlHostOutput) {
	if DecodeNodeReport(ctx.Pool, &ev.Msg, &h.nr) != nil || h.nr.Node != ev.Peer.Node {
		return
	}
	if slices.ContainsFunc(h.nr.Replicas, func(r ReplicaReport) bool { return r.Group == h.group }) {
		if h.ctl != nil {
			h.voterReport(ctx, &h.nr, ev.Peer, false, out)
		}
		return
	}
	i, ok := slices.BinarySearchFunc(h.nodes, ev.Peer.Node, func(n heardNode, id uint32) int { return cmp.Compare(n.at.Node, id) })
	if !ok {
		h.nodes = slices.Insert(h.nodes, i, heardNode{})
	}
	n := &h.nodes[i]
	n.at, n.heard, n.loops = ev.Peer, ctx.Now, n.loops[:0]
	for _, o := range h.nr.Opened {
		n.loops = append(n.loops, diskLoop{log: o.Log, loop: o.Loop})
	}
	if h.ctl == nil {
		h.redirect(ctx, ev.Peer, out)
		return
	}
	h.cout.Reset()
	if err := h.ctl.Heartbeat(ctx.Now, &h.nr, &h.cout); err != nil {
		out.Errs = append(out.Errs, err)
	}
	h.handOut(ctx, out)
	if h.dels = h.ctl.Deletes(h.nr.Node, h.leased(ctx), h.dels[:0]); len(h.dels) > 0 {
		a := core.Action{Kind: core.ActNetSend, Peer: ev.Peer, Reply: true}
		if EncodeDelete(ctx.Pool, h.group, h.dels, &a.Msg) == nil {
			out.Actions = append(out.Actions, a)
		}
	}
	h.sendDisks(ctx, ev.Peer, out)
}

// reportOwn reports the host's replica every reportInterval: to its Control while it has one, or
// else to every other member of its applied configuration, since the leader it knows may be stale:
// a halted replica takes no Raft message, and the next leader of a configuration that dropped the
// replica sends it none.
func (h *CtlHost) reportOwn(ctx *core.Context, out *CtlHostOutput) {
	r := h.shard.Raft()
	at, ok := r.Node(h.id)
	if !ok || h.err != nil || ctx.Now < h.reportAt {
		return
	}
	h.reportAt = ctx.Now + reportInterval
	var voters []uint64
	if len(h.own.Replicas) > 0 {
		voters = h.own.Replicas[0].Voters
	}
	h.own.Node, h.own.Replicas = at.Node, append(h.own.Replicas[:0], h.shard.Report(voters))
	if h.ctl != nil {
		h.voterReport(ctx, &h.own, at, true, out)
		return
	}
	_, cs := h.shard.Applied()
	h.members = append(append(append(append(h.members[:0], cs.Voters...), cs.VotersOutgoing...), cs.Learners...), cs.LearnersNext...)
	slices.Sort(h.members)
	for _, id := range slices.Compact(h.members) {
		to, known := r.Node(id)
		if id == h.id || !known {
			continue
		}
		a := core.Action{Kind: core.ActNetSend, Peer: to}
		if EncodeNodeReport(ctx.Pool, h.group, &h.own, &a.Msg) == nil {
			out.Actions = append(out.Actions, a)
		}
	}
}

// voterReport hands the Control a voter's report, its own when self, and answers it with the
// replicas the voter is to delete. A voter is sent no table, session table or disk membership.
func (h *CtlHost) voterReport(ctx *core.Context, nr *NodeReport, from core.NodeID, self bool, out *CtlHostOutput) {
	h.ctl.VoterReport(ctx.Now, nr.Node, nr.Replicas)
	if h.dels = h.ctl.Deletes(nr.Node, h.leased(ctx), h.dels[:0]); len(h.dels) == 0 {
		return
	}
	if self {
		h.deleted(h.dels, out)
		return
	}
	a := core.Action{Kind: core.ActNetSend, Peer: from, Reply: true}
	if EncodeDelete(ctx.Pool, h.group, h.dels, &a.Msg) == nil {
		out.Actions = append(out.Actions, a)
	}
}

// deleted stops the host for good when refs name its replica and the replica learned its removal.
func (h *CtlHost) deleted(refs []ReplicaRef, out *CtlHostOutput) {
	if removed, _ := h.shard.Raft().Removed(); removed && slices.Contains(refs, ReplicaRef{Group: h.group, ID: h.id}) {
		h.fail(ErrDeleted, out)
	}
}

// sendDisks answers a node's report with the committed membership: each disk not gone that its
// node's last report opened, at the loop that report names for it, so the node learns its own
// disks' IDs and its replicas the loops of their members.
func (h *CtlHost) sendDisks(ctx *core.Context, to core.NodeID, out *CtlHostOutput) {
	h.addrs = h.addrs[:0]
	for _, d := range h.ctl.disks {
		i, ok := slices.BinarySearchFunc(h.nodes, d.Node, func(n heardNode, id uint32) int { return cmp.Compare(n.at.Node, id) })
		if !ok || d.Gone {
			continue
		}
		if j := slices.IndexFunc(h.nodes[i].loops, func(l diskLoop) bool { return l.log == d.Log }); j >= 0 {
			h.addrs = append(h.addrs, DiskAddr{Disk: d.ID, Log: d.Log, At: core.NodeID{Node: d.Node, Loop: h.nodes[i].loops[j].loop}})
		}
	}
	a := core.Action{Kind: core.ActNetSend, Peer: to, Reply: true}
	if EncodeDisks(ctx.Pool, h.group, h.addrs, &a.Msg) == nil {
		out.Actions = append(out.Actions, a)
	}
}

// startSession hands the Control a client's session start while it serves, once per request the client
// is still waiting on; the client asks again for one it never hears of.
func (h *CtlHost) startSession(ctx *core.Context, at core.NodeID, request uint64) {
	s := starter{at: at, request: request}
	if slices.Contains(h.starting, s) || !h.ctl.Serves(lease.Now(ctx), h.leased(ctx)) {
		return
	}
	h.starting = append(h.starting, s)
	h.ctl.StartSession(at.Node, request)
}

// admin hands the Control an operator's command while the leader holds its lease, once per request
// the command line still waits on. A refusal is answered at once; a command taken is answered once
// nothing of it is staged, by settle.
func (h *CtlHost) admin(ctx *core.Context, ev *core.Event, request uint64, out *CtlHostOutput) {
	w := adminWait{starter: starter{at: ev.Peer, request: request}}
	if !h.leased(ctx) || slices.ContainsFunc(h.admins, func(o adminWait) bool { return o.starter == w.starter }) {
		return
	}
	c := h.ctl
	var err error
	switch w.a, err = DecodeAdmin(ctx.Pool, &ev.Msg); {
	case err != nil:
	case w.a.Op == AdminFailed:
		h.answerAdmin(ctx, &w, &AdminAnswer{Failed: c.FailedDevices()}, out)
		return
	case w.a.Op == AdminPool:
		c.begin(ctx.Now)
		if err = c.SetPool(w.a.Pool, w.a.Spec, c.poolDisks(c.AppendDisks(h.disks[:0]))); err == nil {
			w.want = c.nextPool(w.a.Pool)
		}
		err = wrap(err, "pool %d", w.a.Pool)
	case w.a.Op == AdminLost:
		err = wrap(c.Lost(ctx.Now, w.a.Disk), "disk %d", w.a.Disk)
	default:
		err = wrap(c.Remove(ctx.Now, w.a.Disk), "disk %d", w.a.Disk)
	}
	if err != nil {
		h.answerAdmin(ctx, &w, &AdminAnswer{Refused: err.Error()}, out)
		return
	}
	h.admins = append(h.admins, w)
}

func wrap(err error, format string, arg any) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf(format+": %w", arg, err)
}

// settle answers each operator's command the Control no longer stages: one that took effect, a
// pool committed with its settings or a disk gone, with no refusal; one the Control dropped, as a
// change the committed state refused after a reload, with that.
func (h *CtlHost) settle(ctx *core.Context, out *CtlHostOutput) {
	c := h.ctl
	h.admins = slices.DeleteFunc(h.admins, func(w adminWait) bool {
		var refused string
		if w.a.Op == AdminPool {
			if slices.ContainsFunc(c.staged, func(s PoolRecord) bool { return s.ID == w.a.Pool }) {
				return false
			}
			if i, ok := slices.BinarySearchFunc(c.pools, w.a.Pool, cmpPool); !ok || c.pools[i].Pool != w.want {
				refused = fmt.Sprintf("pool %d: the change was dropped before it committed", w.a.Pool)
			}
		} else {
			if slices.Contains(c.exits, diskExit{w.a.Disk, w.a.Op == AdminRemove}) {
				return false
			}
			if !c.disks[w.a.Disk-1].Gone {
				refused = fmt.Sprintf("disk %d: the committed state refused it before it committed", w.a.Disk)
			}
		}
		h.answerAdmin(ctx, &w, &AdminAnswer{Refused: refused}, out)
		return true
	})
}

func (h *CtlHost) answerAdmin(ctx *core.Context, w *adminWait, an *AdminAnswer, out *CtlHostOutput) {
	a := core.Action{Kind: core.ActNetSend, Peer: w.at, Reply: true}
	if EncodeAdminAnswer(ctx.Pool, h.group, w.request, an, &a.Msg) == nil {
		out.Actions = append(out.Actions, a)
	}
}

func (h *CtlHost) leased(ctx *core.Context) bool { return h.shard.Raft().LeaseRead(ctx) }

// step steps the Control and proposes its Txn; one the replica refuses at once is its answer.
func (h *CtlHost) step(ctx *core.Context, out *CtlHostOutput) {
	if h.ctl == nil {
		return
	}
	h.token++
	h.cout.Reset()
	h.disks = h.ctl.AppendDisks(h.disks[:0])
	if err := h.ctl.Step(&ControlInputs{Now: ctx.Now, Raw: ctx.Raw, Leased: h.leased(ctx), Disks: h.disks}, h.token, &h.cout); err != nil {
		out.Errs = append(out.Errs, err)
	}
	h.handOut(ctx, out)
	if !h.cout.Propose {
		return
	}
	if err := h.shard.Txn(ctx, &h.cout.Txn, h.cout.Token, &h.sout); err != nil {
		h.answer(ctx, &Answer{Token: h.cout.Token, Result: Result{FailedCheck: -1}, Err: err}, out)
	}
}

func (h *CtlHost) answer(ctx *core.Context, a *Answer, out *CtlHostOutput) {
	h.cout.Reset()
	if err := h.ctl.Answer(h.shard.State(), a, &h.cout); err != nil {
		out.Errs = append(out.Errs, err)
	}
	h.handOut(ctx, out)
}

// handOut turns the Control's output into the host's: its group's plan to its replica, a voter of
// the group on its node's loop 0; the sessions started; a committed table's
// delta sent to every node heard from within NodeDownAfter, and the whole table to each node named
// that is; and the session table pushed to every such node, or to each node named that is.
func (h *CtlHost) handOut(ctx *core.Context, out *CtlHostOutput) {
	for _, g := range h.cout.Plans {
		if g.ID != h.group {
			continue
		}
		h.plan = h.plan[:0]
		for _, v := range g.Voters {
			h.plan = append(h.plan, raftshard.Peer{ID: v.ID, Node: core.NodeID{Node: v.Node}})
		}
		h.shard.Raft().SetPlan(h.plan)
	}
	out.Started = append(out.Started, h.cout.Started...)
	for _, s := range h.cout.Started {
		i := slices.IndexFunc(h.starting, func(w starter) bool { return w.at.Node == s.Node && w.request == s.Request })
		if i < 0 {
			continue
		}
		a := core.Action{Kind: core.ActNetSend, Peer: h.starting[i].at, Reply: true}
		if EncodeStarted(ctx.Pool, h.group, s.Request, s.ID, &a.Msg) == nil {
			out.Actions = append(out.Actions, a)
		}
		h.starting = slices.Delete(h.starting, i, i+1)
	}
	h.nodes = slices.DeleteFunc(h.nodes, func(n heardNode) bool { return ctx.Now-min(n.heard, ctx.Now) >= chunkstore.NodeDownAfter })
	for _, n := range h.nodes {
		if h.cout.Push {
			h.send(ctx, n.at, KindDelta, out)
		}
		if slices.Contains(h.cout.Tables, n.at.Node) {
			h.send(ctx, n.at, KindTable, out)
		}
	}
	if !h.cout.SessionPush && len(h.cout.SessionTables) == 0 {
		return
	}
	t := h.ctl.SessionTable().Storage()
	for _, n := range h.nodes {
		if h.cout.SessionPush || slices.Contains(h.cout.SessionTables, n.at.Node) {
			h.push(ctx, n.at, 0, &t, out)
		}
	}
}

// send sends a node the committed placement table whole, or the delta just committed. One the
// pool has no buffer for is not sent: the node's replicas find the gap and fetch the table.
func (h *CtlHost) send(ctx *core.Context, to core.NodeID, kind byte, out *CtlHostOutput) {
	a := core.Action{Kind: core.ActNetSend, Peer: to, Reply: true}
	var err error
	if kind == KindDelta {
		err = EncodeDelta(ctx.Pool, h.group, &h.cout.Delta, &a.Msg)
	} else {
		err = EncodeTable(ctx.Pool, h.group, h.ctl.Table(), &a.Msg)
	}
	if err == nil {
		out.Actions = append(out.Actions, a)
	}
}

// push sends t to a node under token. One the pool has no buffer for is not sent: the node's next
// heartbeat shows it behind, or it fetches again.
func (h *CtlHost) push(ctx *core.Context, to core.NodeID, token uint64, t *chunkstore.SessionTable, out *CtlHostOutput) {
	a := core.Action{Kind: core.ActNetSend, Peer: to, Reply: true}
	if EncodeSessions(ctx.Pool, h.group, token, t, &a.Msg) == nil {
		out.Actions = append(out.Actions, a)
	}
}

// after takes the log's events, writes the replica's round to the log, follows the replica's
// leadership, hands each answer to the Txn's proposer, the Server or the Control, which steps
// again, and gathers the replica's and the log's actions. A checkpoint with no room fails, which
// stops the host: only a checkpoint frees a segment.
func (h *CtlHost) after(ctx *core.Context, out *CtlHostOutput) {
	for {
		if h.cp.running && h.err == nil && h.disk.Log.CheckpointStalled() {
			h.disk.Log.FailStalled(ctx, &h.lout)
		}
		h.logEvents(ctx, out)
		if r := h.sout.Raft.Flush; r != 0 && h.err == nil {
			h.sout.Raft.Flush = 0
			h.fail(h.disk.Write(ctx, ctlOwner, r, &h.sout.Raft.Round, &h.lout), out)
			if h.shard != nil && h.err == nil && !h.cp.running && h.disk.Log.CheckpointDue() {
				h.fail(h.checkpoint(ctx), out)
			}
		}
		if h.shard == nil || h.err != nil {
			break
		}
		h.lead(ctx, out)
		if len(h.sout.Answers) == 0 {
			break
		}
		h.answers = append(h.answers[:0], h.sout.Answers...)
		h.sout.Answers = h.sout.Answers[:0]
		out.Actions = h.sv.Reply(ctx, h.answers, out.Actions)
		for i := range h.answers {
			if h.answers[i].Token&ServerToken == 0 && h.ctl != nil {
				h.answer(ctx, &h.answers[i], out)
				h.step(ctx, out)
			}
		}
	}
	if h.ctl != nil {
		h.settle(ctx, out)
	}
	out.Actions = append(out.Actions, h.sout.Raft.Actions...)
	out.Actions = append(out.Actions, h.lout.Actions...)
	h.sout.Reset()
	h.lout.Reset()
	h.seen = 0
}

// logEvents takes the log's events: a durable round goes to the replica, a restart's replay to
// its storage, and the replica starts once the replay ends.
func (h *CtlHost) logEvents(ctx *core.Context, out *CtlHostOutput) {
	for ; h.seen < len(h.lout.Events) && h.err == nil; h.seen++ {
		ev := h.lout.Events[h.seen]
		var err error
		switch ev.Kind {
		case disklog.Durable:
			if err = h.pieceDurable(ctx, &ev); err != nil {
				break
			}
			h.dones, err = h.disk.Durable(ctx, &ev, &h.lout, h.dones[:0])
			for _, d := range h.dones {
				if h.shard == nil {
					err = h.start(ctx, out)
				} else {
					h.shard.Durable(ctx, d.Round, &h.sout)
				}
			}
		case disklog.ManifestLoaded:
			err = h.loadManifest(ctx)
		case disklog.BlobRead:
			err = h.blobRead(ctx, &ev)
		case disklog.CheckpointDone:
			err = h.checkpointDone(ctx)
		case disklog.Replayed:
			h.replayed = true
			err = h.disk.Replayed(&ev)
		case disklog.Recovered:
			var log *disklog.Log
			if log, err = h.rec.Log(ctx); err == nil && uint64(h.cfg.Tree.MaxPiece) > log.MaxBlobBytes() {
				err = lsm.ErrBadConfig
			} else if err == nil {
				h.disk.Recovered(log)
				h.rec = nil
				st := h.cfg.Raft.Storage
				if !h.replayed {
					if err = st.ApplySnapshot(raftpb.Snapshot{Metadata: h.boot}); err == nil {
						err = h.writeBoot(ctx, h.boot)
					}
				} else {
					err = h.start(ctx, out)
				}
			}
		case disklog.Failed, disklog.InDoubt, disklog.RecoveryFailed, disklog.CheckpointFailed:
			err = ev.Err
		}
		h.fail(err, out)
	}
}

// fail stops the host for good on err, a failed disk or a log that does not replay.
func (h *CtlHost) fail(err error, out *CtlHostOutput) {
	if err != nil && h.err == nil {
		h.err, h.ctl, h.admins = err, nil, nil
		out.Errs = append(out.Errs, err)
	}
}

// lead drops the Control of a replica no longer leading its term, and reads one for a leader
// whose lease runs, which needs an entry of its term applied.
func (h *CtlHost) lead(ctx *core.Context, out *CtlHostOutput) {
	r := h.shard.Raft()
	if h.ctl != nil && (r.Leader() != h.id || r.Term() != h.term) {
		h.ctl, h.starting, h.admins = nil, h.starting[:0], h.admins[:0]
	}
	if h.ctl != nil || h.term == r.Term() || !r.LeaseRead(ctx) {
		return
	}
	h.term = r.Term()
	c, err := NewControl(h.shard.State(), h.term)
	if err != nil {
		out.Errs = append(out.Errs, err)
		return
	}
	h.ctl = c
}

func free(ctx *core.Context, m *core.Msg) {
	if m.Buf != core.NoBuf {
		ctx.Pool.Free(m.Buf)
	}
}
