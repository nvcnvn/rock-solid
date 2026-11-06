package chunkstore

import (
	"fmt"
	"hash/fnv"
	"slices"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/lincheck"
)

const invLinearizable = "every chunk's history of client writes, punches and reads is linearizable"

// checkTimeout bounds one run's Porcupine check, which a history it cannot split finely enough can make exponential.
const checkTimeout = 30 * time.Second

const (
	histWrite = iota + 1
	histPunch
	histRead
)

// histOp is what an operation's words leave out: its PG, chunk, range and bytes, the version it
// expects or reads at, the version a primary ordered a write or punch under, 0 for none, and, for
// one with no completion, its fate: kept or dropped by a later peering, or neither while none decided
// it.
type histOp struct {
	kind      uint64
	pg        core.PGID
	chunk     core.ChunkID
	off, n    uint32
	data      []byte
	expect    uint64
	hasExpect bool
	at        uint64
	hasAt     bool
	version   uint64
	kept      bool
	dropped   bool
}

// chunkHist records the client writes, punches and reads of a cluster's run in the scheduler's
// history: an operation's input is {kind, chunk key, off<<32 | n, its index in ops}, a write's or
// punch's output {version, status} and a read's {version, status, FNV-1a of the bytes}. Each attempt
// of a write sent again is its own operation, until the open question on resent chunk writes is
// settled.
type chunkHist struct {
	c       *rcluster
	h       *sim.History
	ops     []histOp
	byID    map[core.RequestID]int
	chunks  []core.ChunkID
	clients uint32
	// acked is the versions each chunk's writes and punches were answered OK with, in order;
	// activated each interval of a PG a primary activated, and decided the version of each chunk its
	// peering kept, 0 for a chunk no reached member held.
	acked     [][]uint64
	activated map[PGInterval]bool
	decided   map[chunkInterval]uint64
}

type chunkInterval struct {
	chunk    core.ChunkID
	interval uint32
}

// record starts recording c's client operations.
func (c *rcluster) record() *chunkHist {
	x := &chunkHist{c: c, h: sim.NewHistory(c.s, 1024), byID: map[core.RequestID]int{}, activated: map[PGInterval]bool{}, decided: map[chunkInterval]uint64{}}
	x.h.Replies(x.reply)
	c.hist = x
	return x
}

// addClient adds a client actor, whose requests are recorded once the cluster records.
func (c *rcluster) addClient(a sim.Actor) {
	if x := c.hist; x != nil {
		a = recorded{Actor: a, x: x, client: x.clients}
		x.clients++
	}
	c.s.Node(c.client.Node).AddActor(c.client, conformActor{a, c})
}

type recorded struct {
	sim.Actor
	x      *chunkHist
	client uint32
}

func (r recorded) Next(ctx *core.Context, acts []core.Action) []core.Action {
	n := len(acts)
	acts = r.Actor.Next(ctx, acts)
	for i := n; i < len(acts); i++ {
		r.x.invoke(ctx, r.client, &acts[i])
	}
	return acts
}

func (x *chunkHist) key(ch core.ChunkID) uint64 {
	i := slices.Index(x.chunks, ch)
	if i < 0 {
		i = len(x.chunks)
		x.chunks = append(x.chunks, ch)
	}
	return uint64(i)
}

// invoke records the client write, punch or read a carries, if any.
func (x *chunkHist) invoke(ctx *core.Context, client uint32, a *core.Action) {
	if x == nil || a.Kind != core.ActNetSend || a.Msg.Len < headSize {
		return
	}
	frame, n := Frame(ctx.Pool, &a.Msg)
	var op histOp
	var id core.RequestID
	switch Kind(a.Msg.Body[1]) {
	case KindWrite, KindPunch:
		var u Update
		if DecodeUpdate(frame, n, &u) != nil {
			return
		}
		op = histOp{kind: histPunch, pg: u.PG.PG, chunk: u.Chunk, off: u.Off, n: u.Len, expect: u.Expect, hasExpect: u.HasExpect}
		if u.Kind == KindWrite {
			op.kind, op.data = histWrite, slices.Clone(ctx.Pool.Bytes(a.Msg.Buf)[u.Off%core.BlockSize:][:u.Len])
		}
		id = u.ID
	case KindRead:
		var rd Read
		if DecodeRead(frame, n, &rd) != nil || rd.Recover {
			return
		}
		op, id = histOp{kind: histRead, pg: rd.PG.PG, chunk: rd.Chunk, off: rd.Off, n: rd.Len, at: rd.At, hasAt: rd.HasAt}, rd.ID
	default:
		return
	}
	x.byID[id] = x.h.Invoke(client, [sim.OpWords]uint64{op.kind, x.key(op.chunk), uint64(op.off)<<32 | uint64(op.n), uint64(len(x.ops))})
	x.ops = append(x.ops, op)
}

// reply completes an operation from a replica's reply to the client: a write, punch or read
// answered OK. Any other answer leaves the result unknown, so the operation stays without a
// completion; a refusal for an expected version among them, since the version it was compared with
// may be pending and later dropped.
func (x *chunkHist) reply(a *core.Action, data []byte) (int, [sim.OpWords]uint64, bool) {
	var rp Response
	if a.Peer != x.c.client || a.Msg.Len < headSize || DecodeResponse(a.Msg.Inline(), len(data), &rp) != nil {
		return 0, [sim.OpWords]uint64{}, false
	}
	i, ok := x.byID[rp.ID]
	if !ok {
		return 0, [sim.OpWords]uint64{}, false
	}
	op := &x.ops[i]
	switch {
	case op.kind == histRead && rp.Kind == KindReadReply && rp.Status == StatusOK:
		return i, [sim.OpWords]uint64{rp.Version, uint64(rp.Status), hashOf(data[op.off%core.BlockSize:][:op.n])}, true
	case op.kind != histRead && rp.Kind == KindReply && rp.Status == StatusOK:
		return i, [sim.OpWords]uint64{rp.Version, uint64(rp.Status)}, true
	}
	return 0, [sim.OpWords]uint64{}, false
}

// ordered notes the version a primary ordered each of the client's writes and punches under, which
// an operation with no completion takes if it took effect.
func (x *chunkHist) ordered(r *Replica) {
	for i := range r.vers {
		v := &r.vers[i]
		if v.ordered && !v.recover && v.client == x.c.client {
			if k, ok := x.byID[v.msgID]; ok {
				x.ops[k].version = v.u.Version
			}
		}
	}
	for i := range r.leads {
		l := &r.leads[i]
		if k := (PGInterval{l.pg, l.interval}); l.active && !x.activated[k] {
			x.activated[k] = true
			for _, kc := range l.kept {
				x.decided[chunkInterval{kc.chunk, l.interval}] = kc.kept
			}
		}
	}
}

// settle gives each operation with no completion the fate the first peering after its version's
// interval gave the version, if one has.
func (x *chunkHist) settle(ops []sim.Op) {
	top := uint32(0)
	for k := range x.activated {
		top = max(top, k.Interval)
	}
	for i, o := range ops {
		op := &x.ops[i]
		if o.Done || op.kind == histRead || op.version == 0 {
			continue
		}
		for k := uint32(op.version>>32) + 1; k <= top; k++ {
			if x.activated[PGInterval{op.pg, k}] {
				op.kept = op.version <= x.decided[chunkInterval{op.chunk, k}]
				op.dropped = !op.kept
				break
			}
		}
	}
}

// readBack reads the first n bytes of each chunk at its PG's primary, recorded, once the run has
// settled: it steps the cluster until each read is answered or 100,000 steps pass, and reads again
// every 200 ms, up to 20 times, until one is answered OK or unfound, the open question on recovery
// sources across overlapping faults. It returns the chunks none was, which the check leaves
// unchecked.
func (x *chunkHist) readBack(n uint32, chunks ...core.ChunkID) []core.ChunkID {
	c := x.c
	var unread []core.ChunkID
	for _, ch := range chunks {
		pg := core.PGID{Pool: testPool, Index: core.PGIndexOf(ch.Hash(), uint32(len(c.table.Pools[0].PGs)))}
		read := false
		for try := 0; try < 20 && !read; try++ {
			if try > 0 {
				c.runFor(200_000)
			}
			if p := c.primary(pg); p >= 0 {
				id := c.read(pg, ch, 0, n, p)
				for range 100_000 {
					if _, ok := c.cl.replies[id]; ok || !c.step() {
						break
					}
				}
				rp, ok := c.cl.replies[id]
				read = ok && (rp.Status == StatusOK || rp.Status == StatusUnfound)
			}
		}
		if !read {
			unread = append(unread, ch)
		}
	}
	return unread
}

func hashOf(b []byte) uint64 {
	h := fnv.New64a()
	h.Write(b)
	return h.Sum64()
}

// modelState is a chunk at a version: the write or punch that made it, nil for the absent chunk, on
// the state before it. Its bytes, those past data zero, are built only once a read needs them, since
// the search makes far more states than it reads; a read at a version finds that version down the
// chain.
type modelState struct {
	ver  uint64
	op   *histOp
	prev *modelState
	data *[]byte
}

func (s *modelState) content() []byte {
	if s.op == nil {
		return nil
	}
	if s.data == nil {
		b := slices.Clone(s.prev.content())
		end := int(s.op.off + s.op.n)
		switch {
		case s.op.kind == histWrite:
			b = append(b, make([]byte, max(0, end-len(b)))...)
			copy(b[s.op.off:], s.op.data)
		case int(s.op.off) < len(b):
			clear(b[s.op.off:min(end, len(b))])
		}
		s.data = &b
	}
	return *s.data
}

func (s *modelState) bytes(off, n uint32) []byte {
	b, data := make([]byte, n), s.content()
	if int(off) < len(data) {
		copy(b, data[off:])
	}
	return b
}

func sameState(a, b *modelState) bool {
	for ; a != b; a, b = a.prev, b.prev {
		if a == nil || b == nil || a.ver != b.ver || a.op != b.op {
			return false
		}
	}
	return true
}

// model is the per-chunk model: a chunk's 4 MiB of bytes and its version. A write or punch answered
// OK takes effect at its version, and at its expected version if it names one. Its version must
// follow the chunk's: the next of the chunk's interval, or k.1 of a later interval k, since a
// primary numbers a chunk's versions from its interval and peering keeps every version below a kept
// one; and it never passes over a version another write was answered OK with, which could then
// never take effect. A punch answered OK with no version, as one of an absent chunk is, finds the
// chunk at version 0 and changes nothing. One with no completion whose version a later peering kept takes
// effect at it like one answered OK, one whose version it dropped is left out, and one no peering
// decided takes effect at its version if it can there, and is otherwise no operation, so it may take
// effect or never: placed after a later version of its chunk, or last. A read returns the chunk's
// version and its bytes, a read at a version the bytes of that version. Operations that can show
// nothing, a read with no answer and a write never ordered, are left out of the chunk's partition.
//
// An operation that may take effect or never doubles the orders to search, and a few dozen refused
// as their primary failed would exhaust it, so a peering's decision settles each it can. One left is
// concurrent with everything after its call; once an operation showing a later version of its chunk
// has completed it can only be no operation, so its return is moved there, admitting the same
// orders.
func (x *chunkHist) model() porcupine.Model {
	return porcupine.Model{
		Partition: func(h []porcupine.Operation) [][]porcupine.Operation {
			parts := make([][]porcupine.Operation, len(x.chunks))
			for _, o := range h {
				in, done := o.Input.([sim.OpWords]uint64), o.Output.(lincheck.Result).Done
				if op := &x.ops[in[3]]; !done && (op.kind == histRead || op.version == 0 || op.dropped) {
					continue
				}
				parts[in[1]] = append(parts[in[1]], o)
			}
			for _, part := range parts {
				for i := range part {
					o := &part[i]
					if o.Output.(lincheck.Result).Done {
						continue
					}
					v := x.ops[o.Input.([sim.OpWords]uint64)[3]].version
					for _, q := range part {
						if r := q.Output.(lincheck.Result); r.Done && !x.ops[q.Input.([sim.OpWords]uint64)[3]].hasAt && r.Output[0] > v && q.Return < o.Return {
							o.Return = q.Return
						}
					}
				}
			}
			return slices.DeleteFunc(parts, func(p []porcupine.Operation) bool { return len(p) == 0 })
		},
		Init: func() any { return &modelState{} },
		Step: func(state, input, output any) (bool, any) {
			s, in, out := state.(*modelState), input.([sim.OpWords]uint64), output.(lincheck.Result)
			op := &x.ops[in[3]]
			if op.kind == histRead {
				at := s
				if op.hasAt {
					for at != nil && at.ver != op.at {
						at = at.prev
					}
				} else if out.Output[0] != s.ver {
					return false, s
				}
				return at != nil && hashOf(at.bytes(op.off, op.n)) == out.Output[2], s
			}
			expected := !op.hasExpect || op.expect == s.ver
			v := op.version
			if out.Done {
				v = out.Output[0]
			}
			if out.Done && v == 0 {
				return expected && s.ver == 0, s
			}
			acked := x.acked[in[1]]
			follows := v == s.ver+1 || uint32(v) == 1 && v>>32 > s.ver>>32
			if j, _ := slices.BinarySearch(acked, s.ver+1); !expected || !follows || j < len(acked) && acked[j] < v {
				return !out.Done && !op.kept, s
			}
			return true, &modelState{ver: v, op: op, prev: s}
		},
		Equal: func(a, b any) bool { return sameState(a.(*modelState), b.(*modelState)) },
		Hash:  func(s any) uint64 { return s.(*modelState).ver },
	}
}

// prepare settles the fates of ops, the recorded operations, and gathers the versions each chunk's
// writes must take.
func (x *chunkHist) prepare(ops []sim.Op) {
	x.settle(ops)
	x.acked = make([][]uint64, len(x.chunks))
	for i, o := range ops {
		switch {
		case o.Done && x.ops[i].kind != histRead && o.Output[0] != 0:
			x.acked[o.Input[1]] = append(x.acked[o.Input[1]], o.Output[0])
		case x.ops[i].kept:
			x.acked[o.Input[1]] = append(x.acked[o.Input[1]], x.ops[i].version)
		}
	}
	for _, a := range x.acked {
		slices.Sort(a)
	}
}

// check looks for a linearizable order of every chunk's history.
func (x *chunkHist) check() error {
	x.prepare(x.h.Ops())
	i, res := lincheck.Illegal(x.model(), x.h, checkTimeout)
	switch res {
	case porcupine.Illegal:
		o, op := x.h.Ops()[i], x.ops[i]
		return fmt.Errorf("%s: no order fits operation %d, kind %d of %v at %d+%d, invoked at %d, done %v at %d with %#x", invLinearizable, i, op.kind, op.chunk, op.off, op.n, o.Invoke, o.Done, o.Complete, o.Output[:2])
	case porcupine.Unknown:
		return fmt.Errorf("%s: the check of %d operations ran past %v", invLinearizable, len(x.ops), checkTimeout)
	}
	return nil
}
