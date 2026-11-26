package chunkstore

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

// invClientAnswers is the run's check of what a client tells its caller.
const invClientAnswers = "a client completes each part of a request once, as its primary answered it"

// clientIDSpan is how far apart the read run's chunk clients take their request IDs.
const clientIDSpan = core.RequestID(1) << 40

// clientNode is the read run's two writers as chunk clients on the client node: the session's first
// incarnation, which dies at its stop, and its successor, which starts with the first's writes left
// without a final answer. Each takes the newest table up to 200 ms after it appears.
type clientNode struct {
	r   *readRun
	ws  [2]*clientWriter
	out ClientOutput
	err error
}

// clientPart is the range of a file one request ID of a client names.
type clientPart struct {
	off uint64
	n   uint32
}

// clientWriter writes, and in one request of four reads, blocks and parts of blocks of two chunks
// of every PG through its client, with up to 6 parts of requests without a final answer.
type clientWriter struct {
	n        *clientNode
	i        int
	cl       *Client
	rng      *core.Rand
	start    core.Tick
	stop     core.Tick
	hist     uint32
	token    uint64
	ops      map[uint64]faultOp
	left     map[uint64]uint32
	parts    map[core.RequestID]clientPart
	reissue  []faultOp
	reissued bool
	idle     core.Tick
	given    uint64
	tableAt  core.Tick
	seen     uint64
}

func newClientNode(r *readRun, first, second *incWriter) *clientNode {
	c := r.d.rcluster
	n := &clientNode{r: r}
	for i, iw := range []*incWriter{first, second} {
		w := &clientWriter{n: n, i: i, rng: iw.rng, start: iw.start, stop: iw.stop, ops: map[uint64]faultOp{}, left: map[uint64]uint32{}, parts: map[core.RequestID]clientPart{}, reissued: i == 0}
		w.cl = NewClient(ClientConfig{Session: iw.session, Incarnation: iw.inc, IDBase: core.RequestID(i+1) * clientIDSpan})
		w.cl.plant = r.clientPlant
		if x := c.hist; x != nil {
			w.hist = x.clients
			x.clients++
		}
		n.ws[i] = w
		c.s.Node(c.client.Node).AddActor(c.client, w)
	}
	c.cl.route = n.route
	c.inv.Register(invClientAnswers, func() error { return n.err })
	return n
}

func (w *clientWriter) alive(now core.Tick) bool { return now >= w.start && now < w.stop }

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

// call runs f on w's client, with rp the answer f hands it if any, and returns its actions, its
// timer numbered for w, each send recorded and each completion checked against rp.
func (w *clientWriter) call(ctx *core.Context, acts []core.Action, rp *Response, f func(ctx *core.Context, out *ClientOutput)) []core.Action {
	c := w.n.r.d.rcluster
	cctx := *ctx
	cctx.Pool = detachPool{BufPool: ctx.Pool, s: c.s, node: c.client.Node}
	out := &w.n.out
	out.Reset()
	f(&cctx, out)
	for _, a := range out.Actions {
		switch a.Kind {
		case core.ActSetTimer:
			a.Timer += core.TimerID(w.i + 1)
		case core.ActNetSend:
			var u Update
			var rd Read
			switch Kind(a.Msg.Body[1]) {
			case KindWrite:
				if DecodeUpdate(a.Msg.Inline(), dataLen(ctx.Pool, a.Msg.Buf), &u) == nil {
					w.n.r.writes[u.ID] = incWrite{inc: uint64(w.i + 1), chunk: u.Chunk}
					w.parts[u.ID] = clientPart{off: uint64(u.Chunk.Index)*core.ChunkSize + uint64(u.Off), n: u.Len}
				}
			case KindRead:
				if DecodeRead(a.Msg.Inline(), 0, &rd) == nil {
					w.parts[rd.ID] = clientPart{off: uint64(rd.Chunk.Index)*core.ChunkSize + uint64(rd.Off), n: rd.Len}
				}
			}
			c.hist.invoke(ctx, w.hist, &a)
			c.noteSends([]core.Action{a})
		}
		acts = append(acts, a)
	}
	for _, d := range out.Done {
		if err := w.answers(rp, d); err != nil {
			w.n.err = cmp.Or(w.n.err, err)
			continue
		}
		if w.left[d.Token] -= d.Len; w.left[d.Token] == 0 {
			delete(w.left, d.Token)
			delete(w.ops, d.Token)
		}
		freeBuf(ctx, d.Buf)
	}
	return acts
}

// answers reports how completion d breaks the client's word to its caller: d is the part rp
// answers, with rp's status and version, and its token has that part outstanding.
func (w *clientWriter) answers(rp *Response, d Completion) error {
	var p clientPart
	if rp != nil {
		p = w.parts[rp.ID]
	}
	switch {
	case rp == nil || p != (clientPart{off: d.Off, n: d.Len}):
		return fmt.Errorf("%s: client %d completed %d bytes at %d of request %d, an answer to none of them", invClientAnswers, w.i, d.Len, d.Off, d.Token)
	case d.Status != rp.Status || d.Version != rp.Version:
		return fmt.Errorf("%s: client %d completed request %d %v at version %#x, its primary answered %v at %#x", invClientAnswers, w.i, d.Token, d.Status, d.Version, rp.Status, rp.Version)
	case d.Len > w.left[d.Token]:
		return fmt.Errorf("%s: client %d completed %d bytes of request %d, of which %d were outstanding", invClientAnswers, w.i, d.Len, d.Token, w.left[d.Token])
	}
	return nil
}

// route hands a client its answers, its timer and lost connections while it lives.
func (n *clientNode) route(ctx *core.Context, ev *core.Event, out []core.Action) ([]core.Action, bool) {
	now := n.r.d.s.Time()
	for _, w := range n.ws {
		switch ev.Kind {
		case core.EvNetRecv:
			id := core.RequestID(le.Uint64(ev.Msg.Body[2:]))
			if (id&^readIDs)/clientIDSpan != core.RequestID(w.i+1) {
				continue
			}
			if !w.alive(now) {
				freeBuf(ctx, ev.Msg.Buf)
				return out, true
			}
			var rp Response
			frame, data := Frame(ctx.Pool, &ev.Msg)
			if DecodeResponse(frame, data, &rp) != nil {
				return w.call(ctx, out, nil, func(ctx *core.Context, o *ClientOutput) { w.cl.OnEvent(ctx, ev, o) }), true
			}
			return w.call(ctx, out, &rp, func(ctx *core.Context, o *ClientOutput) { w.cl.OnEvent(ctx, ev, o) }), true
		case core.EvTimer:
			if ev.Timer == ClientTimer+core.TimerID(w.i+1) && w.alive(now) {
				t := core.Event{Kind: core.EvTimer, Timer: ClientTimer}
				out = w.call(ctx, out, nil, func(ctx *core.Context, o *ClientOutput) { w.cl.OnEvent(ctx, &t, o) })
			}
		case core.EvPeerLost:
			if w.alive(now) {
				out = w.call(ctx, out, nil, func(ctx *core.Context, o *ClientOutput) { w.cl.OnEvent(ctx, ev, o) })
			}
		}
	}
	return out, ev.Kind != core.EvNetRecv
}

// tend hands each live client the newest table once its delay has passed, and the second the
// first's writes left without a final answer once it starts.
func (n *clientNode) tend() {
	c := n.r.d.rcluster
	now := c.s.Time()
	for _, w := range n.ws {
		if !w.alive(now) {
			continue
		}
		if e := c.table.Epoch; e > w.seen {
			w.seen, w.tableAt = e, now+core.Tick(w.rng.Below(200_000))
		}
		if w.given < c.table.Epoch && now >= w.tableAt {
			t := cloneTable(c.table)
			w.given = t.Epoch
			c.s.Node(c.client.Node).Call(c.client, func(ctx *core.Context, acts []core.Action) []core.Action {
				for k, id := range c.ids {
					w.cl.SetAddr(id, core.NodeID{Node: uint32(k)})
				}
				return w.call(ctx, acts, nil, func(ctx *core.Context, o *ClientOutput) { w.cl.SetTable(ctx, t, o) })
			})
		}
	}
	if second := n.ws[1]; !second.reissued && now >= second.start {
		second.reissued = true
		first := n.ws[0]
		for _, tok := range slices.Sorted(maps.Keys(first.ops)) {
			second.reissue = append(second.reissue, first.ops[tok])
		}
	}
}

func (w *clientWriter) Ready() bool {
	now := w.n.r.d.s.Time()
	return w.alive(now) && now >= w.idle && w.reissued && w.cl.Pending() < 6
}

func (w *clientWriter) Next(ctx *core.Context, acts []core.Action) []core.Action {
	var op faultOp
	read := false
	if len(w.reissue) > 0 {
		op, w.reissue = w.reissue[0], w.reissue[1:]
	} else {
		pg := core.PGID{Pool: testPool, Index: uint32(w.rng.Below(4))}
		op = faultOp{c: chunkIn(pg, 4, uint32(w.rng.Below(2))), off: uint32(w.rng.Below(4)) * core.BlockSize, n: core.BlockSize}
		if w.rng.Chance(1, 4) {
			op.off, op.n = uint32(w.rng.Below(4*core.BlockSize-300)), 1+uint32(w.rng.Below(300))
		}
		read = w.rng.Chance(1, 4)
		if !read {
			op.data = make([]byte, op.n)
			for i := range op.data {
				op.data[i] = byte(w.rng.Below(255) + 1)
			}
		}
	}
	w.token++
	tok := w.token
	f := File{Pool: testPool, Volume: op.c.Volume, Inode: op.c.Inode}
	off := uint64(op.c.Index)*core.ChunkSize + uint64(op.off)
	var err error
	acts = w.call(ctx, acts, nil, func(ctx *core.Context, o *ClientOutput) {
		if read {
			err = w.cl.Read(ctx, tok, f, off, op.n, o)
		} else {
			err = w.cl.Write(ctx, tok, f, off, op.data, false, o)
		}
	})
	if err != nil {
		w.idle = w.n.r.d.s.Time() + 1_000
		return acts
	}
	w.left[tok] = op.n
	if !read {
		w.ops[tok] = op
	}
	return acts
}

// The read run with its writers as chunk clients, the second a new session that starts once every
// node with a disk up holds the fence of the first: across primary changes, table changes and the
// fence, no acknowledged write is lost, no fenced session's write is applied, and every chunk's
// history is linearizable.
func TestChunkClientsWriteAcrossPrimaryChangesAndAFence(t *testing.T) {
	seeds.Run(t, 0, 10, func(seed uint64) {
		if err := runReadRun(t, seed, true, true, nil); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
	})
}

// Planted in the clients of the run, a write sent again under a new request ID (4 of the first 30
// seeds), a refusal taken as a write's final answer (1 of 30) and a write naming its own ID as the
// oldest outstanding (26 of 30) each have a primary answer a write stale before its client heard
// a final answer to it: the client has named as final an ID it never heard answered. A refused
// write completed OK and sent again tells its caller what no primary answered, and a write sent
// again after its final answer completes twice (each 10 of the first 10).
func TestPlantedClientBugsBreakTheClientRun(t *testing.T) {
	for _, p := range []struct {
		name  string
		seed  uint64
		plant clientPlants
		inv   string
	}{
		{"a write sent again under a new ID", 12, clientPlants{resendNewID: true}, invStaleOnlyHeard},
		{"a refusal taken as final", 24, clientPlants{refusedFinal: true}, invStaleOnlyHeard},
		{"a write naming its own ID as the oldest", 0, clientPlants{oldestOwn: true}, invStaleOnlyHeard},
		{"a refused write completed OK", 0, clientPlants{refusedOK: true}, invClientAnswers},
		{"a write sent again after its final answer", 0, clientPlants{keepAnswered: true}, invClientAnswers},
	} {
		err := runReadRun(t, p.seed, true, true, &readPlant{inv: p.inv, set: func(r *readRun) { r.clientPlant = p.plant }})
		if err == nil || !strings.Contains(err.Error(), p.inv) {
			t.Errorf("%s at seed %d: %v", p.name, p.seed, err)
		}
	}
}
