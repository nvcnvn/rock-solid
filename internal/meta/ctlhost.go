package meta

import (
	"cmp"
	"errors"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/lease"
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
// Without a Control it answers a node's report or fetch with the leader it knows. A round of its group is durable as soon as it
// is taken: its log lives in memory only.
type CtlHost struct {
	shard *Shard
	sv    *Server
	id    uint64
	group uint64
	ctl   *Control
	// term is the term ctl was read for, or the term whose read failed.
	term  uint64
	token uint64
	// nodes are the nodes heard from, by ID, each at the loop it last reported from.
	nodes   []core.NodeID
	sout    Output
	cout    ControlOutput
	answers []Answer
	nr      NodeReport
	stepAt  core.Tick
}

// ErrInMemoryVoters refuses a control-plane group of more than one voter while its log lives in
// memory: a voter restarted empty could vote twice in a term and elect a second leader.
var ErrInMemoryVoters = errors.New("meta: a control-plane group with its log in memory has more than one voter")

// NewCtlHost starts a host on the replica cfg sets up, refusing a group of more than one voter.
func NewCtlHost(cfg Config) (*CtlHost, error) {
	if _, cs, err := cfg.Raft.Storage.InitialState(); err != nil {
		return nil, err
	} else if len(cs.Voters) > 1 || len(cs.VotersOutgoing) > 1 {
		return nil, ErrInMemoryVoters
	}
	s, err := NewShard(cfg)
	if err != nil {
		return nil, err
	}
	return &CtlHost{shard: s, sv: NewServer(), id: cfg.Raft.ID, group: cfg.Raft.Group}, nil
}

// Shard is the host's replica of the control-plane group.
func (h *CtlHost) Shard() *Shard { return h.shard }

// Control is the leader's Control, or nil while the host does not lead with an entry of its term
// applied.
func (h *CtlHost) Control() *Control { return h.ctl }

// Start starts the replica's clock and the step timer.
func (h *CtlHost) Start(ctx *core.Context, out *CtlHostOutput) {
	h.shard.Start(ctx, &h.sout)
	h.arm(ctx, out)
	h.after(ctx, out)
}

// OnEvent takes a message, a timer, a lost peer or a resume.
func (h *CtlHost) OnEvent(ctx *core.Context, ev *core.Event, out *CtlHostOutput) {
	switch {
	case ev.Kind == core.EvNetRecv && ev.Msg.Group():
		h.recvGroup(ctx, ev, out)
	case ev.Kind == core.EvNetRecv && h.ctl != nil:
		out.Actions = h.ctl.OnEvent(ctx, ev, out.Actions)
	case ev.Kind == core.EvNetRecv:
		free(ctx, &ev.Msg)
	case ev.Kind == core.EvTimer && ev.Timer == StepTimer:
		if ctx.Now >= h.stepAt {
			h.arm(ctx, out)
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
	i, ok := slices.BinarySearchFunc(h.nodes, ev.Peer.Node, func(n core.NodeID, id uint32) int { return cmp.Compare(n.Node, id) })
	if !ok {
		h.nodes = slices.Insert(h.nodes, i, ev.Peer)
	}
	h.nodes[i] = ev.Peer
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
// delta sent to every node heard from, and the whole table to each node named that is; and the
// session table pushed to every node heard from, or to each node named that is.
func (h *CtlHost) handOut(ctx *core.Context, out *CtlHostOutput) {
	out.Started = append(out.Started, h.cout.Started...)
	for _, n := range h.nodes {
		if h.cout.Push {
			h.send(ctx, n, KindDelta, out)
		}
		if slices.Contains(h.cout.Tables, n.Node) {
			h.send(ctx, n, KindTable, out)
		}
	}
	if !h.cout.SessionPush && len(h.cout.SessionTables) == 0 {
		return
	}
	t := h.ctl.SessionTable().Storage()
	for _, n := range h.nodes {
		if h.cout.SessionPush || slices.Contains(h.cout.SessionTables, n.Node) {
			h.push(ctx, n, 0, &t, out)
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

// after makes each round durable at once, follows the replica's leadership, hands each answer to
// the Txn's proposer, the Server or the Control, which steps again, and gathers the replica's
// actions.
func (h *CtlHost) after(ctx *core.Context, out *CtlHostOutput) {
	for {
		for r := h.sout.Raft.Flush; r != 0; r = h.sout.Raft.Flush {
			h.sout.Raft.Flush = 0
			h.shard.Durable(ctx, r, &h.sout)
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
	h.sout.Reset()
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
