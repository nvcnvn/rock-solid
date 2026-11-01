package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"testing"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/raftshard"
)

// cluster routes messages in memory, dropping those between nodes when either is cut, and collects replies to client c1.
type cluster struct {
	t       *testing.T
	now     core.Tick
	nodes   map[string]*node
	ids     []string
	cut     map[string]bool
	replies map[uint64]body
	msgID   uint64
}

func newCluster(t *testing.T, size int) *cluster {
	c := &cluster{t: t, nodes: map[string]*node{}, cut: map[string]bool{}, replies: map[uint64]body{}}
	for i := range size {
		c.ids = append(c.ids, fmt.Sprintf("n%d", i))
	}
	for _, id := range c.ids {
		c.nodes[id] = newNode(false)
		c.deliver(&message{Src: "c0", Dest: id, Body: body{Type: "init", MsgID: 1, NodeID: id, NodeIDs: c.ids}})
	}
	return c
}

// deliver hands m to its node and then every message that follows from it, at once.
func (c *cluster) deliver(m *message) {
	q := []message{*m}
	for len(q) > 0 {
		m := q[0]
		q = q[1:]
		switch n := c.nodes[m.Dest]; {
		case m.Dest == "c1":
			c.replies[m.Body.InReplyTo] = m.Body
		case n != nil && !(c.nodes[m.Src] != nil && (c.cut[m.Src] || c.cut[m.Dest])):
			n.handle(c.now, &m)
			q = append(q, n.sent...)
			n.sent = n.sent[:0]
		}
	}
}

// run moves the clock by d, firing each node's timer every tick.
func (c *cluster) run(d core.Tick) {
	for end := c.now + d; c.now < end; {
		c.now += raftshard.TickInterval
		for _, id := range c.ids {
			if n := c.nodes[id]; n.wake <= c.now {
				n.tick(c.now)
				sent := n.sent
				n.sent = nil
				for i := range sent {
					c.deliver(&sent[i])
				}
			}
		}
	}
}

// leader runs the cluster until a node takes a write, and returns it.
func (c *cluster) leader() string {
	for range 1000 {
		c.run(raftshard.TickInterval * 10)
		for _, id := range c.ids {
			if !c.cut[id] && c.call(id, `{"type":"write","key":0,"value":0}`).Type == "write_ok" {
				return id
			}
		}
	}
	c.t.Fatal("no leader")
	return ""
}

// send has client c1 send a request to node id and returns its message ID.
func (c *cluster) send(id, req string) uint64 {
	var b body
	if err := json.Unmarshal([]byte(req), &b); err != nil {
		c.t.Fatal(err)
	}
	c.msgID++
	b.MsgID = c.msgID
	c.deliver(&message{Src: "c1", Dest: id, Body: b})
	return b.MsgID
}

// call sends a request and runs the cluster a second for its reply.
func (c *cluster) call(id, req string) body {
	m := c.send(id, req)
	c.run(core.TickFromDuration(1e9))
	return c.replies[m]
}

func (c *cluster) expect(id, req string, want body) {
	c.t.Helper()
	got := c.call(id, req)
	if got.Type != want.Type || got.Code != want.Code || string(got.Value) != string(want.Value) {
		c.t.Fatalf("%s to %s: got %+v, want %+v", req, id, got, want)
	}
}

func TestLinKV(t *testing.T) {
	c := newCluster(t, 3)
	c.expect(c.ids[0], `{"type":"write","key":1,"value":1}`, body{Type: "error", Code: errUnavailable})
	lead := c.leader()
	follower := c.ids[0]
	if follower == lead {
		follower = c.ids[1]
	}
	c.expect(lead, `{"type":"cas","key":1,"from":1,"to":2}`, body{Type: "error", Code: errNoKey})
	c.expect(lead, `{"type":"write","key":1,"value":1}`, body{Type: "write_ok"})
	c.expect(lead, `{"type":"cas","key":1,"from":3,"to":2}`, body{Type: "error", Code: errPrecondition})
	c.expect(lead, `{"type":"cas","key":1,"from":1,"to":2}`, body{Type: "cas_ok"})
	c.expect(follower, `{"type":"cas","key":1,"from":2,"to":5}`, body{Type: "cas_ok"})
	c.expect(follower, `{"type":"read","key":1}`, body{Type: "read_ok", Value: json.RawMessage("5")})
	c.expect(follower, `{"type":"cas","key":1,"from":2,"to":6}`, body{Type: "error", Code: errPrecondition})
	c.expect(lead, `{"type":"read","key":2}`, body{Type: "error", Code: errNoKey})
}

// An old leader cut off from its peers takes a write it cannot commit, and neither answers it nor
// any read; the new leader's log replaces the write, which it never answers after the cut heals.
func TestOrphanedWrite(t *testing.T) {
	c := newCluster(t, 3)
	old := c.leader()
	c.cut[old] = true
	w := c.send(old, `{"type":"write","key":1,"value":1}`)
	r := c.send(old, `{"type":"read","key":0}`)
	lead := c.leader()
	c.expect(lead, `{"type":"write","key":1,"value":2}`, body{Type: "write_ok"})
	c.cut[old] = false
	c.run(core.TickFromDuration(5e9))
	c.expect(old, `{"type":"read","key":1}`, body{Type: "read_ok", Value: json.RawMessage("2")})
	for _, m := range []uint64{w, r} {
		if b, ok := c.replies[m]; ok {
			t.Fatalf("request %d to the old leader was answered: %+v", m, b)
		}
	}
}

func TestAReadWaitsUntilItsIndexApplies(t *testing.T) {
	n := newNode(false)
	n.id, n.applied = "n0", 1
	n.kv["1"] = json.RawMessage("7")
	n.reads[5] = &read{client: client{src: "c1", msgID: 9, key: json.RawMessage("1")}}
	n.out.Reads = append(n.out.Reads, raft.ReadState{Index: 2, RequestCtx: binary.LittleEndian.AppendUint64(nil, 5)})
	n.drain()
	if len(n.sent) != 0 {
		t.Fatalf("read served at applied index 1 before its read index 2: %+v", n.sent)
	}
	n.out.Committed = append(n.out.Committed, raftpb.Entry{Index: 2})
	n.drain()
	if len(n.sent) != 1 || n.sent[0].Body.Type != "read_ok" || n.sent[0].Body.InReplyTo != 9 || string(n.sent[0].Body.Value) != "7" {
		t.Fatalf("got %+v, want read_ok of 7 to request 9", n.sent)
	}
}
