package meta

import (
	"cmp"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/lease"
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
// group, each storage node's report, session fetch and table fetch, and the chunkstore frames of
// replicas (reports, member timeouts, probe replies); it steps its Control every StepInterval and
// after each answer, proposing what the Control asks, and sends the nodes that reported the
// placement tables and session tables the Control hands out, a session table pushed with token 0.
// Without a Control it answers a node's report or fetch with the leader it knows. It keeps its
// group's log on a disk log of its own and tells the replica a round is durable only once the
// disk is; a restart replays the log before the replica starts. A disk that fails stops it.
type CtlHost struct {
	shard *Shard
	// cfg is the replica's settings until a restart's replay ends and the replica starts.
	cfg  Config
	disk *raftshard.Host
	rec  *disklog.Recovery
	lout disklog.Output
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
	nodes   []heardNode
	sout    Output
	cout    ControlOutput
	answers []Answer
	nr      NodeReport
	stepAt  core.Tick
}

// ctlOwner is the owner the group's blobs carry on the host's disk log.
const ctlOwner = 1

type heardNode struct {
	at    core.NodeID
	heard core.Tick
}

// FormatCtlHost starts a host on a new disk, which it formats with sb, its replica on cfg's
// storage, the group's boot state.
func FormatCtlHost(ctx *core.Context, cfg Config, lc disklog.Config, sb disklog.Superblock, out *CtlHostOutput) (*CtlHost, error) {
	h := newCtlHost(cfg)
	log, err := disklog.Format(ctx, lc, sb, 1, &h.lout)
	if err != nil {
		return nil, err
	}
	h.disk.Log = log
	if err := h.start(ctx, out); err != nil {
		return nil, err
	}
	h.after(ctx, out)
	return h, nil
}

// OpenCtlHost starts recovering a host from its disk of diskBlocks blocks: the log's rounds
// replay onto cfg's storage, the group's boot state as FormatCtlHost was given it, and the replica
// starts once the replay ends.
func OpenCtlHost(ctx *core.Context, cfg Config, lc disklog.Config, diskBlocks uint64, out *CtlHostOutput) (*CtlHost, error) {
	h := newCtlHost(cfg)
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
	case kind != KindSessionFetch && kind != KindTableFetch:
	case h.ctl == nil:
		h.redirect(ctx, ev.Peer, out)
	case kind == KindTableFetch:
		h.send(ctx, ev.Peer, KindTable, out)
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

// report takes a node's report from the loop it names, which pushes then go to.
func (h *CtlHost) report(ctx *core.Context, ev *core.Event, out *CtlHostOutput) {
	if DecodeNodeReport(ctx.Pool, &ev.Msg, &h.nr) != nil || h.nr.Node != ev.Peer.Node {
		return
	}
	i, ok := slices.BinarySearchFunc(h.nodes, ev.Peer.Node, func(n heardNode, id uint32) int { return cmp.Compare(n.at.Node, id) })
	if !ok {
		h.nodes = slices.Insert(h.nodes, i, heardNode{})
	}
	h.nodes[i] = heardNode{at: ev.Peer, heard: ctx.Now}
	if h.ctl == nil {
		h.redirect(ctx, ev.Peer, out)
		return
	}
	h.cout.Reset()
	if err := h.ctl.Heartbeat(ctx.Now, &h.nr, &h.cout); err != nil {
		out.Errs = append(out.Errs, err)
	}
	h.handOut(ctx, out)
}

func (h *CtlHost) leased(ctx *core.Context) bool { return h.shard.Raft().LeaseRead(ctx) }

// step steps the Control and proposes its Txn; one the replica refuses at once is its answer.
func (h *CtlHost) step(ctx *core.Context, out *CtlHostOutput) {
	if h.ctl == nil {
		return
	}
	h.token++
	h.cout.Reset()
	if err := h.ctl.Step(&ControlInputs{Now: ctx.Now, Raw: ctx.Raw, Leased: h.leased(ctx)}, h.token, &h.cout); err != nil {
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

// handOut turns the Control's output into the host's: the sessions started; a committed table's
// delta sent to every node heard from within NodeDownAfter, and the whole table to each node named
// that is; and the session table pushed to every such node, or to each node named that is.
func (h *CtlHost) handOut(ctx *core.Context, out *CtlHostOutput) {
	out.Started = append(out.Started, h.cout.Started...)
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
// again, and gathers the replica's and the log's actions.
func (h *CtlHost) after(ctx *core.Context, out *CtlHostOutput) {
	for {
		h.logEvents(ctx, out)
		if r := h.sout.Raft.Flush; r != 0 && h.err == nil {
			h.sout.Raft.Flush = 0
			h.fail(h.disk.Write(ctx, ctlOwner, r, &h.sout.Raft.Round, &h.lout), out)
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
			h.dones, err = h.disk.Durable(ctx, &ev, &h.lout, h.dones[:0])
			for _, d := range h.dones {
				h.shard.Durable(ctx, d.Round, &h.sout)
			}
		case disklog.ManifestLoaded:
			err = h.rec.Replay(ctx, &h.lout)
		case disklog.Replayed:
			err = h.disk.Replayed(&ev)
		case disklog.Recovered:
			var log *disklog.Log
			if log, err = h.rec.Log(ctx); err == nil {
				h.disk.Recovered(log)
				h.rec = nil
				err = h.start(ctx, out)
			}
		case disklog.Failed, disklog.InDoubt, disklog.RecoveryFailed:
			err = ev.Err
		}
		h.fail(err, out)
	}
}

// fail stops the host for good on err, a failed disk or a log that does not replay.
func (h *CtlHost) fail(err error, out *CtlHostOutput) {
	if err != nil && h.err == nil {
		h.err, h.ctl = err, nil
		out.Errs = append(out.Errs, err)
	}
}

// lead drops the Control of a replica no longer leading its term, and reads one for a leader
// whose lease runs, which needs an entry of its term applied.
func (h *CtlHost) lead(ctx *core.Context, out *CtlHostOutput) {
	r := h.shard.Raft()
	if h.ctl != nil && (r.Leader() != h.id || r.Term() != h.term) {
		h.ctl = nil
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
