package fsclient

import (
	"encoding/binary"
	"errors"
	"fmt"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/fuseproto"
	"github.com/nvcnvn/rock-solid/internal/meta"
)

// MountTimer is a Mount's meta router's timer.
const MountTimer core.TimerID = 0x66757365

// MountOutput is what one call into a Mount emits: Replies, FUSE reply frames back to back, each
// starting with its length; Data, each a frame at the start of a pool buffer; the Openers whose
// credentials the machine waits for; and Actions. The driver hands it to Flush and then executes
// Actions.
type MountOutput struct {
	Actions []core.Action
	Replies []byte
	Data    []core.IOBuf
	Openers []Opener
}

// Reset empties the output for reuse.
func (o *MountOutput) Reset() {
	o.Actions, o.Replies, o.Data, o.Openers = o.Actions[:0], o.Replies[:0], o.Data[:0], o.Openers[:0]
}

// Mount is the half of one FUSE mount's daemon that does no I/O: it serves the kernel's request
// frames with a Machine and makes the calls that machine asks for, its Txns and Reads through a
// meta.Client and its chunk I/O through the chunk client of the session it holds, one client per
// session. It runs on a loop that hosts no MetaStore replica, so it never sends to its own ID.
type Mount struct {
	cfg     Config
	m       *Machine
	mc      *meta.Client
	cl      *chunkstore.Client
	session uint64
	table   chunkstore.Table
	disks   []meta.DiskAddr
	mapped  bool
	txns    map[uint64]uint64
	reads   map[uint64]*sentRead
	free    []*sentRead
	rq      fuseproto.Request
	fetch   uint64
	out     *MountOutput
	plant   plants
	outs    stack[Output]
	routs   stack[meta.RouterOutput]
	couts   stack[chunkstore.ClientOutput]
}

// sentRead is a Read in flight, kept by the mount since the router keeps a pointer to it.
type sentRead struct {
	token uint64
	rd    meta.Read
}

// stack hands each nested call an output of its own, as answers handed out inline call back in.
type stack[T any] struct {
	s []*T
	n int
}

func (s *stack[T]) get() *T {
	if s.n == len(s.s) {
		s.s = append(s.s, new(T))
	}
	s.n++
	return s.s[s.n-1]
}

func (s *stack[T]) put() { s.n-- }

// NewMount is a mount of cfg, whose Session SetSession sets, in start inc of the process, which
// differs between starts (meta.NewClient); it has no session, shard map or table yet.
func NewMount(cfg Config, inc uint64) *Mount {
	r, err := meta.NewRouter(&meta.ShardMap{Groups: []meta.Group{{ID: 1}, {ID: 2, Start: []byte{meta.SpaceCtl + 1}}}}, MountTimer)
	if err != nil {
		panic(err)
	}
	return &Mount{cfg: cfg, mc: meta.NewClient(r, inc), txns: map[uint64]uint64{}, reads: map[uint64]*sentRead{}}
}

// Ready reports whether the mount can serve: it holds a session and a shard map that places every
// voter's loop.
func (g *Mount) Ready() bool { return g.m != nil && g.mapped }

// WantsTable reports that the mount holds no placement table, or its chunk client waits for a newer one.
func (g *Mount) WantsTable() bool { return g.table.Epoch == 0 || g.fetch > g.table.Epoch }

// SetSession takes the node's session, starting the machine on the first and a chunk client for
// each new one: a fence starts a new session, whose incarnation is 1. The old session's chunk I/O
// in flight fails with EIO, and the new client numbers its requests above the old one's, whose late
// answers arrive on the same connections. It comes first in a pass, before anything that could
// make the old client send.
func (g *Mount) SetSession(ctx *core.Context, session uint64, out *MountOutput) {
	if session == 0 || session == g.session {
		return
	}
	g.out, g.session = out, session
	if g.m == nil {
		cfg := g.cfg
		cfg.Session = session
		g.m = New(cfg)
	}
	g.m.SetSession(session)
	old := g.cl
	var base core.RequestID
	if old != nil && !g.plant.noIDBase {
		base = old.LastID()
	}
	g.cl = chunkstore.NewClient(chunkstore.ClientConfig{Session: session, Incarnation: 1, IDBase: base})
	g.setAddrs()
	if g.table.Epoch != 0 {
		t := g.table
		g.chunked(ctx, func(o *chunkstore.ClientOutput) { g.cl.SetTable(ctx, t, o) })
	}
	if old != nil && !g.plant.noAbandon {
		g.chunked(ctx, func(o *chunkstore.ClientOutput) { old.Abandon(ctx, o) })
	}
}

// SetTable hands the chunk client placement table t, or the table delta makes of the one held. A
// delta it cannot apply, past a gap or failing its check, makes it want the whole table.
func (g *Mount) SetTable(ctx *core.Context, t *chunkstore.Table, delta *chunkstore.Delta, out *MountOutput) {
	g.out = out
	switch {
	case t != nil && t.Epoch > g.table.Epoch:
		g.table = *t
	case delta != nil && delta.Epoch > g.table.Epoch:
		next, err := g.table.Apply(delta)
		if err != nil {
			g.fetch = max(g.fetch, delta.Epoch)
			return
		}
		g.table = next
	default:
		return
	}
	if g.cl != nil {
		t := g.table
		g.chunked(ctx, func(o *chunkstore.ClientOutput) { g.cl.SetTable(ctx, t, o) })
	}
}

// SetMembership takes the disks' loops and, when not nil, the shard map's groups. A map is taken
// only once every voter's disk has a loop, since a send to a replica of no known loop would go to
// loop 0, the mount's own.
func (g *Mount) SetMembership(ctx *core.Context, plans []meta.Group, disks []meta.DiskAddr, out *MountOutput) {
	g.out = out
	if disks != nil {
		g.disks = slices.Clone(disks)
		g.setAddrs()
	}
	if plans == nil {
		return
	}
	smap := meta.ShardMap{Groups: slices.Clone(plans)}
	if smap.CheckRanges() != nil || len(smap.Groups) < 2 {
		return
	}
	for _, gr := range smap.Groups[1:] {
		for _, v := range gr.Voters {
			i := slices.IndexFunc(g.disks, func(a meta.DiskAddr) bool { return a.Disk == v.Disk })
			if i < 0 || g.disks[i].At.Loop == 0 {
				return
			}
			g.mc.SetLoop(v.Node, gr.ID, g.disks[i].At.Loop)
		}
	}
	g.routed(ctx, func(o *meta.RouterOutput) {
		if err := g.mc.R.SetMap(&smap, o); err != nil {
			panic(fmt.Sprintf("fsclient: shard map: %v", err))
		}
	})
	g.mapped = true
}

func (g *Mount) setAddrs() {
	if g.cl == nil {
		return
	}
	for _, a := range g.disks {
		g.cl.SetAddr(a.Disk, a.At)
	}
}

// OnEvent takes a meta answer, a chunk reply, either client's timer or a lost peer.
func (g *Mount) OnEvent(ctx *core.Context, ev *core.Event, out *MountOutput) {
	g.out = out
	switch {
	case ev.Kind == core.EvNetRecv && ev.Msg.Group(), ev.Kind == core.EvTimer && ev.Timer == MountTimer:
		g.routed(ctx, func(o *meta.RouterOutput) { g.mc.OnEvent(ctx, ev, o) })
	case ev.Kind == core.EvNetRecv && g.cl != nil, ev.Kind == core.EvTimer && ev.Timer == chunkstore.ClientTimer && g.cl != nil:
		g.chunked(ctx, func(o *chunkstore.ClientOutput) { g.cl.OnEvent(ctx, ev, o) })
	case ev.Kind == core.EvNetRecv && ev.Msg.Buf != core.NoBuf:
		ctx.Pool.Free(ev.Msg.Buf)
	case ev.Kind == core.EvPeerLost:
		g.routed(ctx, func(o *meta.RouterOutput) { g.mc.OnEvent(ctx, ev, o) })
		if g.cl != nil {
			g.chunked(ctx, func(o *chunkstore.ClientOutput) { g.cl.OnEvent(ctx, ev, o) })
		}
	}
}

// Request serves one request frame read from the device at wall-clock time now, in nanoseconds:
// INIT here, a frame that does not decode with ENOSYS for an unknown opcode and EIO otherwise, and
// every other request through the machine. An INIT of another major version is answered EPROTO
// and returns why, since the mount cannot go on.
func (g *Mount) Request(ctx *core.Context, now int64, frame []byte, out *MountOutput) error {
	g.out = out
	err := fuseproto.Decode(frame, &g.rq)
	switch {
	case err != nil && len(frame) >= 16:
		errno := fuseproto.EIO
		if errors.Is(err, fuseproto.ErrOpcode) {
			errno = fuseproto.ENOSYS
		}
		out.Replies = fuseproto.AppendError(out.Replies, binary.LittleEndian.Uint64(frame[8:]), errno)
	case err != nil:
	case g.rq.Opcode == fuseproto.OpInit:
		o, err := Negotiate(&g.rq.Init)
		if err != nil {
			out.Replies = fuseproto.AppendError(out.Replies, g.rq.Unique, EPROTO)
			return fmt.Errorf("fsclient: %w: %d.%d", err, g.rq.Init.Major, g.rq.Init.Minor)
		}
		out.Replies = fuseproto.AppendInit(out.Replies, g.rq.Unique, &o)
	default:
		g.feed(ctx, func(o *Output) { g.m.Request(ctx, now, &g.rq, o) })
	}
	return nil
}

// Flush hands the machine each of out's Openers' credentials as creds reads them, nil when it
// cannot, which may add more to out, then writes each of out's Replies and Data frames with write,
// freeing the Data buffers.
func (g *Mount) Flush(ctx *core.Context, out *MountOutput, creds func(pid uint32) *Creds, write func(frame []byte)) {
	g.out = out
	for i := 0; i < len(out.Openers); i++ {
		token, c := out.Openers[i].Token, creds(out.Openers[i].PID)
		g.feed(ctx, func(o *Output) { g.m.OpenerDone(ctx, token, c, o) })
	}
	for b := out.Replies; len(b) > 0; b = b[binary.LittleEndian.Uint32(b):] {
		write(b[:binary.LittleEndian.Uint32(b)])
	}
	for _, buf := range out.Data {
		b := ctx.Pool.Bytes(buf)
		write(b[:binary.LittleEndian.Uint32(b)])
		ctx.Pool.Free(buf)
	}
}

// feed runs f on the machine and makes the calls its output asks for, the chunk calls first, as
// their data aliases the request's frame or the machine's memory only until they are made.
func (g *Mount) feed(ctx *core.Context, f func(o *Output)) {
	o := g.outs.get()
	defer g.outs.put()
	o.Reset()
	f(o)
	for i := range o.Chunks {
		g.chunk(ctx, &o.Chunks[i])
	}
	for _, s := range o.Txns {
		g.txn(ctx, s)
	}
	g.out.Replies = append(g.out.Replies, o.Replies...)
	g.out.Data = append(g.out.Data, o.Data...)
	for _, rd := range o.Reads {
		g.read(ctx, rd)
	}
	g.out.Openers = append(g.out.Openers, o.Openers...)
}

// chunk makes one chunk-client call. One the client cannot take, as with its pool exhausted,
// completes each chunk it covers with an error, which the machine answers EIO.
func (g *Mount) chunk(ctx *core.Context, c *ChunkCall) {
	var err error
	g.chunked(ctx, func(o *chunkstore.ClientOutput) {
		if c.Read {
			err = g.cl.Read(ctx, c.Token, c.File, c.Off, c.N, o)
		} else {
			err = g.cl.Write(ctx, c.Token, c.File, c.Off, c.Data, false, o)
		}
	})
	if err == nil || g.plant.dropRefused {
		return
	}
	n := uint64(c.N)
	if !c.Read {
		n = uint64(len(c.Data))
	}
	for off := c.Off; off < c.Off+n; off = (off/core.ChunkSize + 1) * core.ChunkSize {
		done := chunkstore.Completion{Token: c.Token, Off: off, Status: chunkstore.StatusMissing, Buf: core.NoBuf}
		g.feed(ctx, func(o *Output) { g.m.ChunkDone(ctx, &done, o) })
	}
}

// chunked runs f on the chunk client and hands each completion to the machine.
func (g *Mount) chunked(ctx *core.Context, f func(o *chunkstore.ClientOutput)) {
	o := g.couts.get()
	defer g.couts.put()
	o.Reset()
	f(o)
	g.out.Actions = append(g.out.Actions, o.Actions...)
	g.fetch = max(g.fetch, o.Fetch)
	for i := range o.Done {
		g.feed(ctx, func(out *Output) { g.m.ChunkDone(ctx, &o.Done[i], out) })
	}
}

// txn sends a Txn, recording its token before the router's answers are handed out, since the
// router may answer it inline. A Txn the router refuses is a broken invariant: the mount routes
// only by a map it checked, and a refusal handed back would be built and refused again forever.
func (g *Mount) txn(ctx *core.Context, s TxnSend) {
	g.routed(ctx, func(o *meta.RouterOutput) {
		tok, err := g.mc.Txn(ctx, s.Txn, o)
		if err != nil {
			panic(fmt.Sprintf("fsclient: the router refused a Txn: %v", err))
		}
		g.txns[tok] = s.Token
	})
}

// read sends a Read under a copy the mount keeps until its answer.
func (g *Mount) read(ctx *core.Context, rd Read) {
	var h *sentRead
	if n := len(g.free); n > 0 {
		h, g.free = g.free[n-1], g.free[:n-1]
	} else {
		h = new(sentRead)
	}
	h.token = rd.Token
	h.rd = meta.Read{Start: append(h.rd.Start[:0], rd.Start...), End: append(h.rd.End[:0], rd.End...), Limit: rd.Limit, Reg: rd.Reg, Session: rd.Session, Mount: rd.Mount}
	if rd.End == nil {
		h.rd.End = nil
	}
	g.send(ctx, h, false)
}

// send sends h's Read, after the router's election wait when later.
func (g *Mount) send(ctx *core.Context, h *sentRead, later bool) {
	g.routed(ctx, func(o *meta.RouterOutput) {
		var tok uint64
		var err error
		if later {
			tok, err = g.mc.ReadLater(ctx, &h.rd, o)
		} else {
			tok, err = g.mc.Read(ctx, &h.rd, o)
		}
		if err != nil {
			panic(fmt.Sprintf("fsclient: the router refused a Read: %v", err))
		}
		g.reads[tok] = h
	})
}

// lasting reports a Read error that asking again gets again.
func lasting(err error) bool {
	return errors.Is(err, meta.ErrMalformed) || errors.Is(err, meta.ErrTooLarge) || errors.Is(err, meta.ErrFenced)
}

// routed runs f on the meta client and hands each answer to the machine. A Read answered with an
// error that lasts fails; one answered any other error goes again after the router's election
// wait, as a halted group's replacement may answer it, and at once would spin on an error that lasts.
func (g *Mount) routed(ctx *core.Context, f func(o *meta.RouterOutput)) {
	o := g.routs.get()
	defer g.routs.put()
	o.Reset()
	f(o)
	g.out.Actions = append(g.out.Actions, o.Actions...)
	for i := range o.Reads {
		a := &o.Reads[i]
		h, ok := g.reads[a.Token]
		if !ok {
			continue
		}
		delete(g.reads, a.Token)
		if a.Err != nil && (!lasting(a.Err) || g.plant.retryLasting) {
			g.send(ctx, h, true)
			continue
		}
		tok := h.token
		g.free = append(g.free, h)
		if a.Err != nil && g.plant.lastingMissing {
			g.feed(ctx, func(out *Output) { g.m.ReadDone(ctx, tok, nil, out) })
			continue
		}
		if a.Err != nil {
			g.feed(ctx, func(out *Output) { g.m.ReadFailed(ctx, tok, out) })
			continue
		}
		g.feed(ctx, func(out *Output) { g.m.ReadDone(ctx, tok, a.KVs, out) })
	}
	for i := range o.Answers {
		a := &o.Answers[i]
		tok, ok := g.txns[a.Token]
		if !ok {
			continue
		}
		delete(g.txns, a.Token)
		g.feed(ctx, func(out *Output) { g.m.TxnDone(ctx, tok, &a.Result, a.Err, out) })
	}
}
