package chunkstore

import (
	"errors"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// ClientTimer is the timer on which a chunk client sends again a request that a refusal held back
// or that went unanswered.
const ClientTimer core.TimerID = 0x636c6e74

const (
	// ClientRetryAfter is how long a client waits to send again a request whose PG has no primary it
	// can reach, and the first wait of one a primary refused without naming a newer table or
	// answered unfound: 100 ms.
	ClientRetryAfter = core.Tick(100_000)
	// ClientRetryMax is the longest wait of a request refused or answered unfound again and again,
	// each wait doubling the last: 3.2 s.
	ClientRetryMax = 32 * ClientRetryAfter
	// ClientResendAfter is how long a client waits for an answer before it sends a request again: 2 s.
	ClientResendAfter = core.Tick(2_000_000)
)

// readIDs is the bit a client's reads and hellos set in their request IDs, which keeps them out of
// the writes' window.
const readIDs = core.RequestID(1) << 63

// ErrClientRange reports a client request of no bytes or past the last chunk a file may have.
var ErrClientRange = errors.New("chunkstore: client request of no bytes or past a file's last chunk")

// ClientConfig names a client's session and incarnation, and IDBase the request ID below its
// first. A session incarnation has one Client: its request IDs and the resend table's trim by the
// oldest outstanding are the incarnation's, so IDBase sets apart only clients of different
// incarnations, as a test keeping one history of several does.
type ClientConfig struct {
	Session     uint64
	Incarnation uint64
	IDBase      core.RequestID
}

// File names a file's chunks: its volume's pool, the volume and the inode.
type File struct {
	Pool   core.PoolID
	Volume core.VolumeID
	Inode  core.InodeID
}

// Completion is the final answer to one chunk's part of a client request: the request's token, the
// part's range in the file, and the status and version its primary answered with. A read answered
// OK hands the caller Buf, which holds the part's bytes from Off % core.BlockSize; StatusNoSpace is
// ENOSPC. A refused request, and a write answered unfound, is sent again rather than completed.
type Completion struct {
	Token   uint64
	Off     uint64
	Len     uint32
	Status  Status
	Version uint64
	Buf     core.IOBuf
}

// ClientOutput is what a client emits: actions, completions and, when not 0, the table epoch it
// waits for, which SetTable hands it.
type ClientOutput struct {
	Actions []core.Action
	Done    []Completion
	Fetch   uint64
}

// Reset empties the output for reuse.
func (o *ClientOutput) Reset() { o.Actions, o.Done, o.Fetch = o.Actions[:0], o.Done[:0], 0 }

// clientOp is one chunk's part of a request: id 0 until a write is given one; sent once it went to
// peer, due again at at, or once the client holds table epoch wait; retry its last wait after a
// refusal.
type clientOp struct {
	token   uint64
	id      core.RequestID
	kind    Kind
	pool    core.PoolID
	chunk   core.ChunkID
	off, n  uint32
	file    uint64
	buf     core.IOBuf
	granted bool
	sent    bool
	peer    core.NodeID
	at      core.Tick
	retry   core.Tick
	wait    uint64
}

// clientPlants are bugs planted in a client, each breaking one rule of its request IDs.
type clientPlants struct {
	// resendNewID sends a write again under a new request ID; oldestOwn names a write's own ID as
	// the oldest outstanding; oldestSent names the lowest sent one, past IDs assigned and not yet
	// sent; refusedFinal takes a refusal as a write's final answer; keepAnswered sends a write
	// again after its final answer; pastWindow assigns IDs past RequestWindow over the oldest;
	// refusedOK completes a refused write OK and sends it again, completing it no more.
	resendNewID, oldestOwn, oldestSent, refusedFinal, keepAnswered, pastWindow, refusedOK bool
}

// Client is the client side of chunk I/O for one session incarnation, its only one: it sends each chunk's
// writes, punches and reads to the primary the table it holds names for the chunk's PG, after a
// hello naming the session and incarnation on each connection. Writes take request IDs in order,
// each named by every send of its write; a write names the lowest ID with no final answer, and no
// ID is taken more than RequestWindow past it, so a write waits for an ID rather than run ahead.
// A refused request goes again once the client holds the table the refusal names, or after
// ClientRetryAfter, and one unanswered after ClientResendAfter, or after its connection is lost.
type Client struct {
	cfg     ClientConfig
	table   Table
	addrs   []core.NodeID
	known   []bool
	greeted []core.NodeID
	// next is the last request ID a write took and nextRead a read's or hello's count; writes holds
	// the writes with an ID and no final answer in ID order, queued those waiting for one.
	next, nextRead uint64
	writes, queued []clientOp
	reads          []clientOp
	armed          core.Tick
	frame          []byte
	plant          clientPlants
}

// NewClient makes a client of cfg's session incarnation.
func NewClient(cfg ClientConfig) *Client { return &Client{cfg: cfg} }

// SetAddr names the loop that owns disk.
func (c *Client) SetAddr(disk uint32, addr core.NodeID) {
	if int(disk) >= len(c.addrs) {
		c.addrs = append(c.addrs, make([]core.NodeID, int(disk)+1-len(c.addrs))...)
		c.known = append(c.known, make([]bool, int(disk)+1-len(c.known))...)
	}
	c.addrs[disk], c.known[disk] = addr, true
}

// Epoch is the epoch of the table the client holds.
func (c *Client) Epoch() uint64 { return c.table.Epoch }

// Pending is the parts of requests with no final answer.
func (c *Client) Pending() int { return len(c.writes) + len(c.queued) + len(c.reads) }

// LastID is the highest request ID the client has taken, without the read bit: a client that
// replaces it on the same connections takes it as its IDBase, so no answer to this one matches
// a request of its own.
func (c *Client) LastID() core.RequestID {
	return c.cfg.IDBase + core.RequestID(max(c.next, c.nextRead))
}

// Abandon completes every part with no final answer StatusMissing, frees its buffer and forgets
// it, as for a fenced session, whose writes can no longer land. The parts' sends must have been
// executed, as the actions of an earlier output are.
func (c *Client) Abandon(ctx *core.Context, out *ClientOutput) {
	for _, ops := range [][]clientOp{c.writes, c.queued, c.reads} {
		for _, op := range ops {
			out.Done = append(out.Done, Completion{Token: op.token, Off: op.file, Len: op.n, Status: StatusMissing, Buf: core.NoBuf})
			freeBuf(ctx, op.buf)
		}
	}
	c.writes, c.queued, c.reads = c.writes[:0], c.queued[:0], c.reads[:0]
}

// SetTable adopts t if it is newer than the client's table, sending at once what waited for it, what
// has not gone yet and what went to a primary the table no longer names.
func (c *Client) SetTable(ctx *core.Context, t Table, out *ClientOutput) {
	if t.Epoch <= c.table.Epoch {
		return
	}
	c.table = t
	for _, ops := range [][]clientOp{c.writes, c.reads} {
		for i := range ops {
			op := &ops[i]
			if op.wait != 0 && op.wait <= t.Epoch {
				op.wait, op.at = 0, 0
			}
			if to, ok := c.primary(op); !op.sent || !ok || to != op.peer {
				op.at, op.retry = 0, 0
			}
		}
	}
	c.pump(ctx, out)
}

// Write writes data at off of file f, one write per chunk it covers, each completing on its own;
// with granted, under the node's space grant. It copies data into pool buffers and fails, issuing
// nothing, when the pool has no room.
func (c *Client) Write(ctx *core.Context, token uint64, f File, off uint64, data []byte, granted bool, out *ClientOutput) error {
	return c.update(ctx, token, f, off, uint64(len(data)), data, granted, out)
}

// Punch zeroes n bytes at off of file f, one punch per chunk it covers, each completing on its own.
func (c *Client) Punch(ctx *core.Context, token uint64, f File, off, n uint64, out *ClientOutput) error {
	return c.update(ctx, token, f, off, n, nil, false, out)
}

func (c *Client) update(ctx *core.Context, token uint64, f File, off, n uint64, data []byte, granted bool, out *ClientOutput) error {
	if !inFile(off, n) {
		return ErrClientRange
	}
	first := len(c.queued)
	for done := uint64(0); done < n; {
		op := part(token, f, off+done, n-done)
		op.granted = granted
		if data != nil {
			op.kind = KindWrite
			buf, err := ctx.Pool.Alloc(dataBytes(op.off, op.n))
			if err != nil {
				for _, q := range c.queued[first:] {
					ctx.Pool.Free(q.buf)
				}
				c.queued = c.queued[:first]
				return err
			}
			b, h := ctx.Pool.Bytes(buf), int(op.off%core.BlockSize)
			clear(b[:h])
			clear(b[h+copy(b[h:], data[done:done+uint64(op.n)]):])
			op.buf = buf
		}
		c.queued = append(c.queued, op)
		done += uint64(op.n)
	}
	c.pump(ctx, out)
	return nil
}

// Read reads the committed bytes of n at off of file f, one read per chunk it covers, each
// completing on its own.
func (c *Client) Read(ctx *core.Context, token uint64, f File, off uint64, n uint32, out *ClientOutput) error {
	if !inFile(off, uint64(n)) {
		return ErrClientRange
	}
	for done := uint64(0); done < uint64(n); {
		op := part(token, f, off+done, uint64(n)-done)
		op.kind, op.id = KindRead, c.readID()
		c.reads = append(c.reads, op)
		done += uint64(op.n)
	}
	c.pump(ctx, out)
	return nil
}

func inFile(off, n uint64) bool { return n > 0 && off+n >= off && off+n <= core.ChunkSize<<32 }

// part is the punch of the first chunk of n bytes at file offset off: up to the chunk's end.
func part(token uint64, f File, off, n uint64) clientOp {
	in := uint32(off % core.ChunkSize)
	return clientOp{token: token, kind: KindPunch, pool: f.Pool, chunk: core.ChunkID{Volume: f.Volume, Inode: f.Inode, Index: uint32(off / core.ChunkSize)},
		off: in, n: uint32(min(n, core.ChunkSize-uint64(in))), file: off}
}

func (c *Client) readID() core.RequestID {
	c.nextRead++
	return readIDs | (c.cfg.IDBase + core.RequestID(c.nextRead))
}

// OnEvent takes an answer from a primary, a lost connection or ClientTimer.
func (c *Client) OnEvent(ctx *core.Context, ev *core.Event, out *ClientOutput) {
	switch ev.Kind {
	case core.EvNetRecv:
		c.recv(ctx, ev, out)
	case core.EvPeerLost:
		if i := slices.Index(c.greeted, ev.Peer); i >= 0 {
			c.greeted = slices.Delete(c.greeted, i, i+1)
		}
		for _, ops := range [][]clientOp{c.writes, c.reads} {
			for i := range ops {
				if ops[i].sent && ops[i].peer == ev.Peer {
					ops[i].at = min(ops[i].at, ctx.Now+ResendAfter)
				}
			}
		}
	case core.EvTimer:
		if ev.Timer != ClientTimer {
			return
		}
		if ctx.Now >= c.armed {
			c.armed = 0
		}
	}
	c.pump(ctx, out)
}

func (c *Client) recv(ctx *core.Context, ev *core.Event, out *ClientOutput) {
	frame, data := Frame(ctx.Pool, &ev.Msg)
	var rp Response
	if DecodeResponse(frame, data, &rp) != nil || rp.Kind == KindAck {
		freeBuf(ctx, ev.Msg.Buf)
		return
	}
	ops := &c.writes
	if rp.Kind == KindReadReply {
		ops = &c.reads
	}
	// ponytail: a linear scan per answer, fine while a client keeps a few dozen requests in flight;
	// a map from ID if it keeps thousands.
	i := slices.IndexFunc(*ops, func(op clientOp) bool { return op.id == rp.ID })
	if i < 0 {
		freeBuf(ctx, ev.Msg.Buf)
		return
	}
	op := &(*ops)[i]
	switch {
	case rp.Status == StatusRefused && !(c.plant.refusedFinal && rp.Kind == KindReply) || rp.Kind == KindReply && rp.Status == StatusUnfound:
		if ev.Peer != op.peer {
			break
		}
		if c.plant.refusedOK && rp.Kind == KindReply && op.token != 0 {
			out.Done = append(out.Done, Completion{Token: op.token, Off: op.file, Len: op.n, Status: StatusOK})
			op.token = 0
		}
		if rp.Status == StatusRefused && rp.Epoch > c.table.Epoch {
			op.wait = rp.Epoch
			out.Fetch = max(out.Fetch, rp.Epoch)
		} else {
			op.retry = min(max(2*op.retry, ClientRetryAfter), ClientRetryMax)
			op.at = ctx.Now + op.retry
		}
	case c.plant.keepAnswered && rp.Kind == KindReply:
		out.Done = append(out.Done, Completion{Token: op.token, Off: op.file, Len: op.n, Status: rp.Status, Version: rp.Version})
		op.at = ctx.Now + ClientRetryAfter
	default:
		cp := Completion{Token: op.token, Off: op.file, Len: op.n, Status: rp.Status, Version: rp.Version}
		if rp.Kind == KindReadReply && rp.Status == StatusOK {
			cp.Buf, ev.Msg.Buf = ev.Msg.Buf, core.NoBuf
		}
		if !c.plant.refusedOK || op.token != 0 {
			out.Done = append(out.Done, cp)
		}
		freeBuf(ctx, op.buf)
		*ops = slices.Delete(*ops, i, i+1)
	}
	freeBuf(ctx, ev.Msg.Buf)
}

// pump gives queued writes their IDs while the window allows, sends every request due, and arms
// ClientTimer for the next one due.
//
// ponytail: a walk of every request per event, and a queue drained from its front, fine at a few
// dozen requests in flight; a heap by due time and a ring for the queue if a client keeps thousands.
func (c *Client) pump(ctx *core.Context, out *ClientOutput) {
	for len(c.queued) > 0 && (c.plant.pastWindow || len(c.writes) == 0 || c.next+1-uint64(c.writes[0].id-c.cfg.IDBase) <= RequestWindow) {
		c.next++
		op := c.queued[0]
		op.id = c.cfg.IDBase + core.RequestID(c.next)
		c.writes = append(c.writes, op)
		c.queued = slices.Delete(c.queued, 0, 1)
	}
	next := core.Tick(0)
	for _, ops := range [][]clientOp{c.writes, c.reads} {
		for i := range ops {
			op := &ops[i]
			if op.wait > c.table.Epoch {
				continue
			}
			if op.at <= ctx.Now && !c.send(ctx, op, out) {
				op.at = ctx.Now + ClientRetryAfter
				out.Fetch = max(out.Fetch, c.table.Epoch+1)
			}
			if next == 0 || op.at < next {
				next = op.at
			}
		}
	}
	if next != 0 && (c.armed == 0 || next < c.armed) {
		c.armed = next
		out.Actions = append(out.Actions, core.Action{Kind: core.ActSetTimer, Timer: ClientTimer, At: next})
	}
}

// primary is the loop of the primary the client's table names for op's chunk, and its PG.
func (c *Client) primary(op *clientOp) (core.NodeID, bool) {
	to, _, _, ok := c.route(op)
	return to, ok
}

func (c *Client) route(op *clientOp) (core.NodeID, core.PGID, *PGEntry, bool) {
	p := c.table.pool(op.pool)
	if p == nil || len(p.PGs) == 0 {
		return core.NodeID{}, core.PGID{}, nil, false
	}
	pg := core.PGID{Pool: op.pool, Index: core.PGIndexOf(op.chunk.Hash(), uint32(len(p.PGs)))}
	e := &p.PGs[pg.Index]
	disk, ok := e.primary()
	if !ok || int(disk) >= len(c.known) || !c.known[disk] {
		return core.NodeID{}, pg, e, false
	}
	return c.addrs[disk], pg, e, true
}

// send sends op to its chunk's primary, after a hello if the connection has none, or reports that
// the table names no primary the client can reach.
func (c *Client) send(ctx *core.Context, op *clientOp, out *ClientOutput) bool {
	to, pg, e, ok := c.route(op)
	if !ok {
		return false
	}
	if !slices.Contains(c.greeted, to) {
		c.greeted = append(c.greeted, to)
		c.frame = AppendHello(c.frame[:0], &Hello{ID: c.readID(), Session: c.cfg.Session, Incarnation: c.cfg.Incarnation})
		c.emit(to, core.NoBuf, out)
	}
	if c.plant.resendNewID && op.sent && op.kind != KindRead {
		c.next++
		op.id = c.cfg.IDBase + core.RequestID(c.next)
	}
	f := PGFields{Epoch: c.table.Epoch, PG: pg, Interval: e.Interval}
	if op.kind == KindRead {
		c.frame = AppendRead(c.frame[:0], &Read{ID: op.id, PG: f, Chunk: op.chunk, Off: op.off, Len: op.n})
	} else {
		c.frame = AppendUpdate(c.frame[:0], &Update{Kind: op.kind, ID: op.id, PG: f, Session: c.cfg.Session, Incarnation: c.cfg.Incarnation,
			Chunk: op.chunk, Off: op.off, Len: op.n, Granted: op.granted, Oldest: uint64(c.oldest(op))})
	}
	c.emit(to, op.buf, out)
	op.sent, op.peer, op.at = true, to, ctx.Now+ClientResendAfter
	return true
}

// oldest is the lowest request ID with no final answer, which op's send names.
func (c *Client) oldest(op *clientOp) core.RequestID {
	switch {
	case c.plant.oldestOwn:
		return op.id
	case c.plant.oldestSent:
		for _, w := range c.writes {
			if w.sent || w.id == op.id {
				return w.id
			}
		}
	}
	return c.writes[0].id
}

func (c *Client) emit(to core.NodeID, buf core.IOBuf, out *ClientOutput) {
	a := core.Action{Kind: core.ActNetSend, Peer: to}
	a.Msg.Len = uint16(copy(a.Msg.Body[:], c.frame))
	a.Msg.Buf = buf
	out.Actions = append(out.Actions, a)
}
