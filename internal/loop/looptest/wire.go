package looptest

import (
	"encoding/binary"
	"fmt"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/loop"
)

// Wire is the smallest driver the loop tests need: a frame is a 2-byte length and a message's
// inline bytes, sent on the connection its peer was last connected on. The TCP driver replaces it.
type Wire struct {
	L *loop.Loop
	// Lost counts NetSends a full connection refused and connections closed under it.
	Lost  int
	peers map[core.NodeID]loop.ConnID
	conns []wireConn
	ev    core.Event
}

type wireConn struct {
	peer  core.NodeID
	carry [2 + core.MaxInlineMsg]byte
	n     int
}

// Connect names c's peer and routes sends to that peer over c.
func (w *Wire) Connect(c loop.ConnID, peer core.NodeID) {
	if w.peers == nil {
		w.peers = make(map[core.NodeID]loop.ConnID)
	}
	w.peers[peer] = c
	for len(w.conns) <= c.Slot() {
		w.conns = append(w.conns, wireConn{})
	}
	w.conns[c.Slot()] = wireConn{peer: peer}
}

// Read decodes whole frames into NetRecv events, carrying a partial frame to the next read.
func (w *Wire) Read(c loop.ConnID, in []byte) {
	wc := &w.conns[c.Slot()]
	for len(in) > 0 {
		if wc.n < 2 {
			k := copy(wc.carry[wc.n:2], in)
			wc.n += k
			in = in[k:]
			continue
		}
		size := 2 + int(binary.LittleEndian.Uint16(wc.carry[:2]))
		if size > len(wc.carry) {
			panic(fmt.Sprintf("looptest: a %d-byte frame on connection %d", size, c))
		}
		k := copy(wc.carry[wc.n:size], in)
		wc.n += k
		in = in[k:]
		if wc.n == size {
			w.ev = core.Event{Kind: core.EvNetRecv, Peer: wc.peer}
			w.ev.Msg.Len = uint16(copy(w.ev.Msg.Body[:], wc.carry[2:size]))
			wc.n = 0
			w.L.Deliver(&w.ev)
		}
	}
}

// Closed counts the connection lost.
func (w *Wire) Closed(loop.ConnID) { w.Lost++ }

// Accept panics: the wire's tests hand loops connected sockets.
func (w *Wire) Accept(c loop.ConnID) {
	panic(fmt.Sprintf("looptest: the wire accepts nothing, listener %d", c))
}

// Flush defers nothing.
func (w *Wire) Flush() bool { return false }

// Execute frames a NetSend into a pool buffer and queues it.
func (w *Wire) Execute(a *core.Action) {
	if a.Kind != core.ActNetSend {
		panic(fmt.Sprintf("looptest: the wire executes no %+v", *a))
	}
	c, ok := w.peers[a.Peer]
	pool := w.L.Context().Pool
	if !ok {
		panic(fmt.Sprintf("looptest: no connection to %+v", a.Peer))
	}
	n := 2 + int(a.Msg.Len)
	b, err := pool.Alloc(n)
	if err != nil {
		w.Lost++
		return
	}
	p := pool.Bytes(b)
	binary.LittleEndian.PutUint16(p, a.Msg.Len)
	copy(p[2:], a.Msg.Inline())
	if !w.L.Queue(c, b, n, true) {
		pool.Free(b)
		w.Lost++
	}
}
