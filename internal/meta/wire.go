package meta

import (
	"bytes"
	"encoding/binary"
	"errors"

	"go.etcd.io/raft/v3"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// A sharded API message is core.ProtoGroup, its group's ID, its kind and its token, little-endian,
// then its body:
// inline when all of it fits in core.MaxInlineMsg bytes, or else the body's length inline and the
// body in a pool buffer. A Txn's body is its entry encoding. An Answer's is its error's code, a
// NotLeader's group, leader, node and term, FailedCheck+1 as a uvarint, Actual as a presence byte
// and its bytes, and Updated as a count and 8 bytes each.
const (
	// KindTxn and KindAnswer follow a message's group ID as raftshard's kinds do, above all of them.
	KindTxn      byte = 0x40
	KindAnswer   byte = 0x41
	kindAt            = 1 + 8
	msgHead           = kindAt + 1 + 8
	notLeaderLen      = 8 + 8 + 4 + 8
)

var le = binary.LittleEndian

// wireErrs are the errors an Answer carries, by code; code 1 is a NotLeader. Any other error
// travels as raft.ErrProposalDropped, which promises nothing about whether the Txn applied.
var wireErrs = [...]error{nil, nil, raft.ErrProposalDropped, ErrMalformed, ErrHalted, ErrNoOps, ErrTooLarge,
	ErrEmptyRange, ErrBadOp, ErrBadReplay, ErrReplayTagged, ErrOverloaded}

const (
	codeNotLeader = 1
	codeDropped   = 2
)

func errCode(err error) byte {
	if _, ok := err.(NotLeader); ok {
		return codeNotLeader
	}
	for i, e := range wireErrs {
		if i != codeNotLeader && e == err {
			return byte(i)
		}
	}
	return codeDropped
}

// frameMsg writes a message's head into msg and returns where its body of n bytes goes, empty
// with room for n: msg's inline bytes, or a pool buffer msg then holds.
func frameMsg(pool core.BufPool, group uint64, kind byte, token uint64, n int, msg *core.Msg) ([]byte, error) {
	b := msg.Body[:]
	b[0] = core.ProtoGroup
	le.PutUint64(b[1:], group)
	b[kindAt] = kind
	le.PutUint64(b[kindAt+1:], token)
	if n > core.MaxMsgData {
		return nil, ErrTooLarge
	}
	if msgHead+n <= core.MaxInlineMsg {
		msg.Len, msg.Buf = uint16(msgHead+n), core.NoBuf
		return b[msgHead:msgHead:core.MaxInlineMsg], nil
	}
	buf, err := pool.Alloc(n)
	if err != nil {
		return nil, err
	}
	le.PutUint32(b[msgHead:], uint32(n))
	msg.Len, msg.Buf = msgHead+4, buf
	return pool.Bytes(buf)[:0:n], nil
}

// Peek is a message's group, kind and token, or false for one too short to hold them or of
// another protocol.
func Peek(msg *core.Msg) (group uint64, kind byte, token uint64, ok bool) {
	in := msg.Inline()
	if len(in) < msgHead || in[0] != core.ProtoGroup {
		return 0, 0, 0, false
	}
	return le.Uint64(in[1:]), in[kindAt], le.Uint64(in[kindAt+1:]), true
}

// msgBody is the body of a message of kind, aliasing msg or its pool buffer, or ErrMalformed.
func msgBody(pool core.BufPool, kind byte, msg *core.Msg) ([]byte, error) {
	in := msg.Inline()
	if _, k, _, ok := Peek(msg); !ok || k != kind {
		return nil, ErrMalformed
	}
	if msg.Buf == core.NoBuf {
		return in[msgHead:], nil
	}
	if len(in) != msgHead+4 {
		return nil, ErrMalformed
	}
	n, b := int(le.Uint32(in[msgHead:])), pool.Bytes(msg.Buf)
	if n > len(b) || msgHead+n <= core.MaxInlineMsg {
		return nil, ErrMalformed
	}
	return b[:n], nil
}

// EncodeTxn writes t, sent to group under token, into msg, or returns Check's error or the pool's.
func EncodeTxn(pool core.BufPool, group, token uint64, t *Txn, msg *core.Msg) error {
	n, err := t.Check()
	if err != nil {
		return err
	}
	dst, err := frameMsg(pool, group, KindTxn, token, n, msg)
	if err != nil {
		return err
	}
	if len(appendTxn(dst, t)) != n {
		panic("meta: a Txn's encoding is not the size Check gives")
	}
	return nil
}

// DecodeTxn decodes a Txn message's body into t, reusing its ops, whose keys and values alias
// msg's bytes; one that does not decode is ErrMalformed. Peek gives its group and token.
func DecodeTxn(pool core.BufPool, msg *core.Msg, t *Txn) error {
	b, err := msgBody(pool, KindTxn, msg)
	if err != nil {
		return err
	}
	if rest, err := decodeTxn(b, t); err != nil || len(rest) != 0 {
		return ErrMalformed
	}
	return nil
}

// EncodeAnswer writes a, about a Txn of group, into msg, or returns the pool's error.
func EncodeAnswer(pool core.BufPool, group uint64, a *Answer, msg *core.Msg) error {
	code := errCode(a.Err)
	r := &a.Result
	n := 1 + uvarintLen(uint64(r.FailedCheck+1)) + 1 + uvarintLen(uint64(len(r.Updated))) + 8*len(r.Updated)
	if code == codeNotLeader {
		n += notLeaderLen
	}
	if r.Actual != nil {
		n += bytesLen(r.Actual)
	}
	dst, err := frameMsg(pool, group, KindAnswer, a.Token, n, msg)
	if err != nil {
		return err
	}
	dst = append(dst, code)
	if nl, ok := a.Err.(NotLeader); ok {
		dst = le.AppendUint64(le.AppendUint64(dst, nl.Group), nl.Leader)
		dst = le.AppendUint64(le.AppendUint32(dst, nl.Node), nl.Term)
	}
	dst = binary.AppendUvarint(dst, uint64(r.FailedCheck+1))
	if r.Actual == nil {
		dst = append(dst, 0)
	} else {
		dst = appendBytes(append(dst, 1), r.Actual)
	}
	dst = binary.AppendUvarint(dst, uint64(len(r.Updated)))
	for _, u := range r.Updated {
		dst = le.AppendUint64(dst, u)
	}
	if len(dst) != n {
		panic("meta: an Answer's encoding is not the size counted")
	}
	return nil
}

// DecodeAnswer decodes an Answer message into a, its Actual and Updated fresh copies; one that
// does not decode, or that no EncodeAnswer gives, is ErrMalformed.
func DecodeAnswer(pool core.BufPool, msg *core.Msg, a *Answer) error {
	b, err := msgBody(pool, KindAnswer, msg)
	if err != nil {
		return err
	}
	_, _, a.Token, _ = Peek(msg)
	if len(b) == 0 || int(b[0]) >= len(wireErrs) {
		return ErrMalformed
	}
	code := b[0]
	a.Err, b = wireErrs[code], b[1:]
	if code == codeNotLeader {
		if len(b) < notLeaderLen {
			return ErrMalformed
		}
		a.Err = NotLeader{Group: le.Uint64(b), Leader: le.Uint64(b[8:]), Node: le.Uint32(b[16:]), Term: le.Uint64(b[20:])}
		b = b[notLeaderLen:]
	}
	d := decoder{b: b}
	f := d.uvarint()
	present := d.byte()
	var actual []byte
	if present == 1 {
		actual = d.bytes()
	}
	count := d.uvarint()
	if d.bad || f > MaxCtlTxnBytes || present > 1 || len(actual) > MaxValue || count*8 != uint64(len(d.b)) {
		return ErrMalformed
	}
	r := &a.Result
	r.FailedCheck, r.Actual, r.Updated = int(f)-1, nil, nil
	if present == 1 {
		r.Actual = bytes.Clone(actual)
		if r.Actual == nil {
			r.Actual = []byte{}
		}
	}
	for b = d.b; len(b) > 0; b = b[8:] {
		r.Updated = append(r.Updated, le.Uint64(b))
	}
	return nil
}

// ShardMapOf is the shard map the control-plane group's applied state st commits under ctl/.
func ShardMapOf(st *State) (ShardMap, error) {
	var c Control
	if err := c.load(st); err != nil {
		return ShardMap{}, err
	}
	return c.smap, nil
}

// Client is a node's Router on the network: it gives each Txn a token unique across its process's
// starts, sends
// each Txn to the loop hosting its replica, and feeds the router the answers, its timer and lost
// peers. Its calls leave every send in out.Actions, as NetSends, and none in out.Sends.
type Client struct {
	R     *Router
	next  uint64
	loops map[replicaAt]uint32
	ans   Answer
}

type replicaAt struct {
	node  uint32
	group uint64
}

// NewClient wires r to the network. inc names this start of the process and must differ between
// starts, as the network driver's seed does: it fills bits 32 to 62 of every token, so a server
// that answers a Txn of an earlier start cannot settle one of this start's.
func NewClient(r *Router, inc uint64) *Client {
	return &Client{R: r, next: inc << 32 &^ ServerToken, loops: map[replicaAt]uint32{}}
}

// SetLoop names the loop of node hosting group's replica, which is loop 0 until named. A loop that
// no longer hosts it forwards what it gets, so this is a hint.
func (c *Client) SetLoop(node uint32, group uint64, loop uint32) {
	c.loops[replicaAt{node, group}] = loop
}

// SetMap routes later Txns by the shard map the control-plane group's applied state st commits.
func (c *Client) SetMap(st *State, out *RouterOutput) error {
	m, err := ShardMapOf(st)
	if err != nil {
		return err
	}
	return c.R.SetMap(&m, out)
}

// Txn sends t, which stays the caller's and unchanged until an Answer settles it, and returns the
// token its Answer carries, or Route's error.
func (c *Client) Txn(ctx *core.Context, t *Txn, out *RouterOutput) (uint64, error) {
	c.next++
	if err := c.R.Txn(ctx, t, c.next, out); err != nil {
		return 0, err
	}
	c.send(ctx, out)
	return c.next, nil
}

// OnEvent takes an Answer message, the router's timer or a lost peer; it frees a message's buffer
// and drops one that is no Answer. An Answer whose body does not decode settles its Txn as
// dropped, since the router has no timeout.
func (c *Client) OnEvent(ctx *core.Context, ev *core.Event, out *RouterOutput) {
	if ev.Kind == core.EvNetRecv {
		err := DecodeAnswer(ctx.Pool, &ev.Msg, &c.ans)
		if ev.Msg.Buf != core.NoBuf {
			ctx.Pool.Free(ev.Msg.Buf)
		}
		if _, kind, token, ok := Peek(&ev.Msg); err != nil && ok && kind == KindAnswer {
			c.ans = Answer{Token: token, Result: Result{FailedCheck: -1}, Err: raft.ErrProposalDropped}
			err = nil
		}
		if err == nil {
			c.R.Answer(ctx, &c.ans, out)
		}
	} else {
		c.R.OnEvent(ctx, ev, out)
	}
	c.send(ctx, out)
}

// send turns the router's sends into NetSends; a Txn the pool has no buffer for is answered with
// the pool's error, sent nowhere.
func (c *Client) send(ctx *core.Context, out *RouterOutput) {
	for _, s := range out.Sends {
		a := core.Action{Kind: core.ActNetSend, Peer: core.NodeID{Node: s.Node, Loop: c.loops[replicaAt{s.Node, s.Group}]}, Reply: true}
		if err := EncodeTxn(ctx.Pool, s.Group, s.Token, s.Txn, &a.Msg); err != nil {
			c.ans = Answer{Token: s.Token, Result: Result{FailedCheck: -1}, Err: err}
			c.R.Answer(ctx, &c.ans, out)
			continue
		}
		out.Actions = append(out.Actions, a)
	}
	out.Sends = out.Sends[:0]
}

// ServerToken is set in every token a Server proposes under, and in no other proposer's.
const ServerToken = 1 << 63

// ErrServerToken reports a Txn handed a shard under a token with ServerToken set.
var ErrServerToken = errors.New("meta: token has ServerToken set")

// Server answers the Txns peers' routers send to a node's replicas: it proposes each under a
// token of its own, answers at once with any error Shard.Txn returns, and sends each answer the
// replica gives back to the loop the Txn came from. A replica on the round driver gathers the Txns
// that arrive while backpressure holds its round in flight, which the disk does not bound, so the
// server refuses one with ErrOverloaded while Held of its group's are proposed and unanswered.
type Server struct {
	Held  int
	next  uint64
	waits map[uint64]wait
	held  map[uint64]int
	t     Txn
	a     Answer
}

// ServerHeld is a new Server's Held.
const ServerHeld = 1024

// wait is a Txn proposed: its group, and the loop and token it came under.
type wait struct {
	group uint64
	peer  core.NodeID
	token uint64
}

// NewServer makes a server.
func NewServer() *Server {
	return &Server{Held: ServerHeld, waits: map[uint64]wait{}, held: map[uint64]int{}}
}

// Serve takes ev, a Txn message, and proposes it on s, the replica of its group Peek names, or
// nil when the node hosts none, which answers NotLeader naming no leader. out is s's output for
// the call, and Serve frees the message's buffer.
func (sv *Server) Serve(ctx *core.Context, ev *core.Event, s *Shard, out *Output, acts []core.Action) []core.Action {
	group, _, token, ok := Peek(&ev.Msg)
	err := DecodeTxn(ctx.Pool, &ev.Msg, &sv.t)
	switch {
	case !ok:
	case err != nil:
	case s == nil:
		err = NotLeader{Group: group}
	default:
		sv.next++
		local := ServerToken | sv.next
		if err = s.submit(ctx, &sv.t, local, out); err == nil {
			sv.waits[local] = wait{group, ev.Peer, token}
		}
	}
	return sv.refuse(ctx, ev, group, token, ok, err, acts)
}

// ServeRound is Serve for a, a replica on the round driver, or nil when the node hosts none of
// the group. The Txn waits before it is proposed, as the replica may answer it within the call.
func (sv *Server) ServeRound(ctx *core.Context, ev *core.Event, a *RoundShard, acts []core.Action) []core.Action {
	group, _, token, ok := Peek(&ev.Msg)
	err := DecodeTxn(ctx.Pool, &ev.Msg, &sv.t)
	switch {
	case !ok:
	case err != nil:
	case a == nil || a.Err() != nil:
		err = NotLeader{Group: group}
	case sv.held[group] >= sv.Held:
		err = ErrOverloaded
	default:
		sv.next++
		local := ServerToken | sv.next
		sv.waits[local] = wait{group, ev.Peer, token}
		sv.held[group]++
		if acts, err = a.submit(ctx, &sv.t, local, acts); err != nil {
			sv.release(local)
		}
	}
	return sv.refuse(ctx, ev, group, token, ok, err, acts)
}

// refuse frees ev's buffer and answers err, if any, to a Txn that Peek could read.
func (sv *Server) refuse(ctx *core.Context, ev *core.Event, group, token uint64, ok bool, err error, acts []core.Action) []core.Action {
	if ev.Msg.Buf != core.NoBuf {
		ctx.Pool.Free(ev.Msg.Buf)
	}
	if !ok || err == nil {
		return acts
	}
	sv.a = Answer{Token: token, Result: Result{FailedCheck: -1}, Err: err}
	return sv.answer(ctx, group, ev.Peer, acts)
}

// Reply sends each of answers, a replica's, under a token Serve proposed under to the loop the
// Txn came from, and leaves the others, which are their proposers'.
func (sv *Server) Reply(ctx *core.Context, answers []Answer, acts []core.Action) []core.Action {
	for i := range answers {
		w, ok := sv.release(answers[i].Token)
		if !ok {
			continue
		}
		sv.a = answers[i]
		sv.a.Token = w.token
		acts = sv.answer(ctx, w.group, w.peer, acts)
	}
	return acts
}

// release forgets the Txn proposed under token, if it waits, and returns its wait.
func (sv *Server) release(token uint64) (wait, bool) {
	w, ok := sv.waits[token]
	if !ok {
		return w, false
	}
	delete(sv.waits, token)
	if sv.held[w.group]--; sv.held[w.group] <= 0 {
		delete(sv.held, w.group)
	}
	return w, true
}

// answer sends sv.a to peer. An answer the pool has no buffer for goes inline as dropped, which
// promises nothing about the Txn, as the router has no timeout and must hear one.
func (sv *Server) answer(ctx *core.Context, group uint64, peer core.NodeID, acts []core.Action) []core.Action {
	a := core.Action{Kind: core.ActNetSend, Peer: peer, Reply: true}
	if EncodeAnswer(ctx.Pool, group, &sv.a, &a.Msg) != nil {
		sv.a = Answer{Token: sv.a.Token, Result: Result{FailedCheck: -1}, Err: raft.ErrProposalDropped}
		_ = EncodeAnswer(ctx.Pool, group, &sv.a, &a.Msg)
	}
	return append(acts, a)
}
