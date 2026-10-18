package core

import (
	"cmp"
	"fmt"
	"slices"
)

// RequestID names one request to a peer, chosen by the machine that sends it.
type RequestID uint64

// Reason says why a request failed.
type Reason uint8

// Failure reasons.
const (
	ReasonTimeout Reason = iota + 1
	ReasonPeerLost
)

func (r Reason) String() string {
	switch r {
	case ReasonTimeout:
		return "timeout"
	case ReasonPeerLost:
		return "peer lost"
	}
	return fmt.Sprintf("reason %d", uint8(r))
}

// Request is one outstanding request to a peer.
type Request struct {
	ID       RequestID
	Peer     NodeID
	Timer    TimerID
	Deadline Tick
	Buf      IOBuf
}

// Failure reports a request that timed out or whose peer was lost.
type Failure struct {
	ID       RequestID
	Peer     NodeID
	Timer    TimerID
	Deadline Tick
	Buf      IOBuf
	Reason   Reason
}

type tracked struct {
	Request
	timedOut bool
}

func (t *tracked) failure(reason Reason) Failure {
	return Failure{ID: t.ID, Peer: t.Peer, Timer: t.Timer, Deadline: t.Deadline, Buf: t.Buf, Reason: reason}
}

// PeerRequests tracks the requests outstanding to peers, each with its timer, deadline
// and data buffer, in request-ID order.
type PeerRequests struct {
	reqs []tracked
}

// NewPeerRequests returns an empty table.
func NewPeerRequests() *PeerRequests { return &PeerRequests{} }

// Send records the request and appends two actions: the message to the peer, carrying
// buf as its data, then the timer armed at now + timeout. An id or timer already in
// flight is a programming error.
func (p *PeerRequests) Send(ctx *Context, id RequestID, peer NodeID, timer TimerID, timeout Tick, msg *Msg, buf IOBuf, out []Action) []Action {
	i, found := p.find(id)
	if found {
		panic(fmt.Sprintf("core: request %d is already in flight", id))
	}
	for j := range p.reqs {
		if r := &p.reqs[j]; r.Timer == timer && !r.timedOut {
			panic(fmt.Sprintf("core: timer %d already guards request %d", timer, r.ID))
		}
	}
	deadline := ctx.Now + timeout
	p.reqs = append(p.reqs, tracked{})
	copy(p.reqs[i+1:], p.reqs[i:])
	p.reqs[i] = tracked{Request: Request{ID: id, Peer: peer, Timer: timer, Deadline: deadline, Buf: buf}}
	out = append(out, Action{Kind: ActNetSend, Peer: peer, Msg: *msg})
	out[len(out)-1].Msg.Buf = buf
	return append(out, Action{Kind: ActSetTimer, Timer: timer, At: deadline})
}

// OnTimer reports the request the timer guards once its deadline has passed, with reason
// timeout, and keeps it tracked. A fire before the current request's deadline, for a
// timer guarding nothing, or for a request already reported, reports nothing.
func (p *PeerRequests) OnTimer(ctx *Context, timer TimerID, out []Failure) []Failure {
	for i := range p.reqs {
		r := &p.reqs[i]
		if r.Timer != timer || r.timedOut {
			continue
		}
		if ctx.Now >= r.Deadline {
			r.timedOut = true
			out = append(out, r.failure(ReasonTimeout))
		}
		return out
	}
	return out
}

// OnPeerLost fails every request outstanding to the peer, in request-ID order, and
// stops tracking them.
func (p *PeerRequests) OnPeerLost(peer NodeID, out []Failure) []Failure {
	kept := p.reqs[:0]
	for i := range p.reqs {
		r := &p.reqs[i]
		if r.Peer == peer {
			out = append(out, r.failure(ReasonPeerLost))
			continue
		}
		kept = append(kept, *r)
	}
	clear(p.reqs[len(kept):])
	p.reqs = kept
	return out
}

// Complete stops tracking the request and returns it, once.
func (p *PeerRequests) Complete(id RequestID) (Request, bool) {
	i, found := p.find(id)
	if !found {
		return Request{}, false
	}
	r := p.reqs[i].Request
	p.reqs = slices.Delete(p.reqs, i, i+1)
	return r, true
}

// InFlight is the number of requests tracked, timed-out ones included.
func (p *PeerRequests) InFlight() int { return len(p.reqs) }

func (p *PeerRequests) find(id RequestID) (int, bool) {
	return slices.BinarySearchFunc(p.reqs, id, func(r tracked, id RequestID) int { return cmp.Compare(r.ID, id) })
}
