package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"slices"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/lease"
	"github.com/nvcnvn/rock-solid/internal/raftshard"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// Maelstrom error codes.
const (
	errUnavailable  = 11
	errNoKey        = 20
	errPrecondition = 22
)

type message struct {
	Src  string `json:"src"`
	Dest string `json:"dest"`
	Body body   `json:"body"`
}

// body is every field a lin-kv node reads or writes; keys and values stay raw JSON, compared as text.
type body struct {
	Type      string          `json:"type"`
	MsgID     uint64          `json:"msg_id,omitempty"`
	InReplyTo uint64          `json:"in_reply_to,omitempty"`
	NodeID    string          `json:"node_id,omitempty"`
	NodeIDs   []string        `json:"node_ids,omitempty"`
	Key       json.RawMessage `json:"key,omitempty"`
	Value     json.RawMessage `json:"value,omitempty"`
	From      json.RawMessage `json:"from,omitempty"`
	To        json.RawMessage `json:"to,omitempty"`
	Code      int             `json:"code,omitempty"`
	Text      string          `json:"text,omitempty"`
	Inline    []byte          `json:"inline,omitempty"`
	Buf       []byte          `json:"buf,omitempty"`
}

// command is a client's write or cas as a log entry, naming the node that proposed it and its proposal ID there.
type command struct {
	Node string          `json:"node"`
	ID   uint64          `json:"id"`
	Op   string          `json:"op"`
	Key  json.RawMessage `json:"key"`
	Val  json.RawMessage `json:"value,omitempty"`
	From json.RawMessage `json:"from,omitempty"`
	To   json.RawMessage `json:"to,omitempty"`
}

// client is a request to answer.
type client struct {
	src   string
	msgID uint64
	key   json.RawMessage
}

// read is a client read waiting for its ReadIndex: confirmed once a quorum confirmed the index it is served at.
type read struct {
	client
	index     uint64
	confirmed bool
}

// node is one Maelstrom node running one replica of a single Raft group, with a map as its state machine.
type node struct {
	id    string
	ids   []string
	shard *raftshard.Shard
	ctx   core.Context
	out   raftshard.Output
	// wake is when the shard's tick timer fires, on ctx.Now.
	wake core.Tick
	kv   map[string]json.RawMessage
	// applied is the index of the last entry applied to kv.
	applied uint64
	nextID  uint64
	writes  map[uint64]client
	reads   map[uint64]*read
	// resent are the client requests this follower resent to its leader, by the ID it resent them under.
	resent map[uint64]client
	// stale serves reads from kv at once, the planted bug the checker must catch.
	stale bool
	// sent is what the node sends, which its driver writes out after each call.
	sent []message
}

func newNode(stale bool) *node {
	return &node{kv: map[string]json.RawMessage{}, writes: map[uint64]client{}, resent: map[uint64]client{}, reads: map[uint64]*read{}, stale: stale}
}

// handle takes one message from Maelstrom's network at now.
func (n *node) handle(now core.Tick, m *message) {
	n.ctx.Now, n.ctx.Raw = now, now
	if m.Body.Type == "init" {
		n.init(m)
		return
	}
	if n.shard == nil {
		return
	}
	n.out.Reset()
	c := client{src: m.Src, msgID: m.Body.MsgID, key: m.Body.Key}
	switch m.Body.Type {
	case "raft":
		n.recv(m)
	case "read":
		n.read(c)
	case "write", "cas":
		n.propose(c, &m.Body)
	case "write_ok", "cas_ok", "error":
		n.relay(m.Body)
	default:
		n.fail(c, 10, "unsupported: "+m.Body.Type)
	}
	n.drain()
}

// tick runs the shard's timer at now.
func (n *node) tick(now core.Tick) {
	if n.shard == nil {
		return
	}
	n.ctx.Now, n.ctx.Raw = now, now
	n.out.Reset()
	n.shard.OnEvent(&n.ctx, &core.Event{Kind: core.EvTimer, Timer: 1}, &n.out)
	n.drain()
}

// init boots every node with the same configuration, all of Maelstrom's nodes as voters. The
// leases are never renewed, so no leader is fortified and reads go through ReadIndex; the grantor
// still holds the restart wait, so no vote is granted in the first lease after start.
func (n *node) init(m *message) {
	n.id, n.ids = m.Body.NodeID, m.Body.NodeIDs
	me := slices.Index(n.ids, n.id)
	cs := raftpb.ConfState{}
	cfg := raftshard.Config{Group: 1, ID: uint64(me) + 1, Seed: uint64(me), Timer: 1}
	for i := range n.ids {
		cs.Voters = append(cs.Voters, uint64(i)+1)
		cfg.Peers = append(cfg.Peers, raftshard.Peer{ID: uint64(i) + 1, Node: core.NodeID{Node: uint32(i)}})
	}
	cfg.Storage = raftshard.NewStorage()
	if err := cfg.Storage.ApplySnapshot(raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 1, Term: 1, ConfState: cs}}); err != nil {
		panic(err)
	}
	n.applied = 1
	at := lease.Now(&n.ctx)
	cfg.Holder = lease.NewHolder(lease.DefaultConfig)
	cfg.Grantor = lease.NewGrantor(lease.DefaultConfig, uint64(me)+1, at)
	s, err := raftshard.New(cfg)
	if err != nil {
		panic(err)
	}
	n.shard = s
	n.ctx.Pool = sim.NewPool(64<<20, 0)
	n.reply(client{src: m.Src, msgID: m.Body.MsgID}, body{Type: "init_ok"})
	n.out.Reset()
	s.Start(&n.ctx, &n.out)
	n.drain()
}

func (n *node) recv(m *message) {
	from := slices.Index(n.ids, m.Src)
	if from < 0 {
		return
	}
	ev := core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{Node: uint32(from)}}
	if err := ev.Msg.SetInline(m.Body.Inline); err != nil {
		return
	}
	if len(m.Body.Buf) > 0 {
		b, err := n.ctx.Pool.Alloc(len(m.Body.Buf))
		if err != nil {
			return
		}
		copy(n.ctx.Pool.Bytes(b), m.Body.Buf)
		ev.Msg.Buf = b
	}
	n.shard.OnEvent(&n.ctx, &ev, &n.out)
}

func (n *node) read(c client) {
	if n.stale {
		n.serve(c)
		return
	}
	n.nextID++
	n.reads[n.nextID] = &read{client: c}
	n.shard.ReadIndex(&n.ctx, binary.LittleEndian.AppendUint64(nil, n.nextID), &n.out)
}

// propose appends a write or cas to the log if this node leads. A follower resends it to the
// leader it names, as a daemon resends a Txn on NotLeader, and relays the answer; a node that
// neither leads nor knows a leader, or was itself resent the request, answers error 11, a definite
// failure, since it never proposed it.
func (n *node) propose(c client, b *body) {
	n.nextID++
	data, err := json.Marshal(command{Node: n.id, ID: n.nextID, Op: b.Type, Key: b.Key, Val: b.Value, From: b.From, To: b.To})
	if err != nil {
		panic(err)
	}
	if err := n.shard.Propose(&n.ctx, data, &n.out); err == nil {
		n.writes[n.nextID] = c
		return
	}
	lead := n.shard.Leader()
	if lead == 0 || n.ids[lead-1] == n.id || slices.Contains(n.ids, c.src) {
		n.fail(c, errUnavailable, "not the leader")
		return
	}
	n.resent[n.nextID] = c
	rb := *b
	rb.MsgID = n.nextID
	n.sent = append(n.sent, message{Src: n.id, Dest: n.ids[lead-1], Body: rb})
}

// relay passes the leader's answer to a resent request on to its client.
func (n *node) relay(b body) {
	if c, ok := n.resent[b.InReplyTo]; ok {
		delete(n.resent, b.InReplyTo)
		n.reply(c, b)
	}
}

// drain carries out the shard's output, reporting each round durable at once: the storage is the
// process's memory, which Maelstrom's lin-kv workload never crashes.
func (n *node) drain() {
	for {
		if !raft.IsEmptySnap(n.out.Snapshot) || n.out.Removed {
			panic("maelstrom: a snapshot or removal in a group that never compacts or reconfigures")
		}
		for i := range n.out.Actions {
			n.act(&n.out.Actions[i])
		}
		for i := range n.out.Committed {
			n.apply(&n.out.Committed[i])
		}
		for _, rs := range n.out.Reads {
			if r := n.reads[binary.LittleEndian.Uint64(rs.RequestCtx)]; r != nil {
				r.index, r.confirmed = rs.Index, true
			}
		}
		for id, r := range n.reads {
			if r.confirmed && r.index <= n.applied {
				delete(n.reads, id)
				n.serve(r.client)
			}
		}
		f := n.out.Flush
		n.out.Reset()
		if f == 0 {
			return
		}
		n.shard.Durable(&n.ctx, f, &n.out)
	}
}

func (n *node) act(a *core.Action) {
	switch a.Kind {
	case core.ActSetTimer:
		n.wake = a.At
	case core.ActNetSend:
		b := body{Type: "raft", Inline: slices.Clone(a.Msg.Inline())}
		if a.Msg.Buf != core.NoBuf {
			b.Buf = slices.Clone(n.ctx.Pool.Bytes(a.Msg.Buf))
			n.ctx.Pool.Free(a.Msg.Buf)
		}
		n.sent = append(n.sent, message{Src: n.id, Dest: n.ids[a.Peer.Node], Body: b})
	}
}

// apply applies a committed write or cas to kv, and answers its client if this node proposed it.
func (n *node) apply(e *raftpb.Entry) {
	n.applied = e.Index
	if e.Type != raftpb.EntryNormal || len(e.Data) == 0 {
		return
	}
	var cmd command
	if err := json.Unmarshal(e.Data, &cmd); err != nil {
		panic(fmt.Sprintf("maelstrom: committed entry %d does not decode: %v", e.Index, err))
	}
	k := string(cmd.Key)
	cur, ok := n.kv[k]
	code, res := 0, body{Type: cmd.Op + "_ok"}
	switch {
	case cmd.Op == "write":
		n.kv[k] = cmd.Val
	case !ok:
		code = errNoKey
	case string(cur) != string(cmd.From):
		code = errPrecondition
	default:
		n.kv[k] = cmd.To
	}
	c, mine := n.writes[cmd.ID]
	if cmd.Node != n.id || !mine {
		return
	}
	delete(n.writes, cmd.ID)
	if code != 0 {
		n.fail(c, code, "cas on "+k)
		return
	}
	n.reply(c, res)
}

func (n *node) serve(c client) {
	v, ok := n.kv[string(c.key)]
	if !ok {
		n.fail(c, errNoKey, "no key "+string(c.key))
		return
	}
	n.reply(c, body{Type: "read_ok", Value: v})
}

func (n *node) fail(c client, code int, text string) {
	n.reply(c, body{Type: "error", Code: code, Text: text})
}

func (n *node) reply(c client, b body) {
	b.InReplyTo = c.msgID
	n.sent = append(n.sent, message{Src: n.id, Dest: c.src, Body: b})
}
