// Package leasetest renews a test loop's node-pair leases with its peers' loops over the network,
// as a storage node's loop renews its own for its replicas.
package leasetest

import (
	"encoding/binary"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/lease"
)

// A lease frame is core.ProtoGroup, Group, a kind, then a request ID, the grantor's run ID, its raw
// reading and the support epoch, little-endian.
const (
	// Group is the group ID lease frames carry, which no test's Raft group takes.
	Group     = ^uint64(0)
	kindAsk   = 'a'
	kindGrant = 'g'
	kindBeat  = 'h'
	kindAt    = 1 + 8
	frameLen  = kindAt + 1 + 4*8
)

var le = binary.LittleEndian

// Pair is one loop's side of its node-pair leases: it asks each peer a request is due from, grants
// each ask its grantor may, and heartbeats each holder it grants to once a heartbeat is due.
type Pair struct {
	Holder  *lease.Holder
	Grantor *lease.Grantor
	Peers   []core.NodeID
	rng     *core.Rand
	next    uint64
	beatAt  core.Tick
}

// New is a pair over h and g with peers, drawing a new run ID for g from rng on every resume.
func New(h *lease.Holder, g *lease.Grantor, peers []core.NodeID, rng *core.Rand) *Pair {
	return &Pair{Holder: h, Grantor: g, Peers: peers, rng: rng}
}

// OnEvent takes a lease frame, reporting true, and on EvResumed ends the holder's leases and gives
// the grantor a new run, reporting false so the event goes on to the loop's shards too.
func (p *Pair) OnEvent(ctx *core.Context, ev *core.Event, acts []core.Action) ([]core.Action, bool) {
	t := lease.Now(ctx)
	if ev.Kind == core.EvResumed {
		p.Holder.Resumed()
		p.Grantor.Resumed(p.rng.Uint64())
		return acts, false
	}
	b := ev.Msg.Inline()
	if ev.Kind != core.EvNetRecv || ev.Msg.Buf != core.NoBuf || len(b) != frameLen || b[0] != core.ProtoGroup || le.Uint64(b[1:]) != Group {
		return acts, false
	}
	id, run, raw, support := le.Uint64(b[kindAt+1:]), le.Uint64(b[kindAt+9:]), core.Tick(le.Uint64(b[kindAt+17:])), le.Uint64(b[kindAt+25:])
	switch b[kindAt] {
	case kindAsk:
		if sup, ok := p.Grantor.Grant(ev.Peer, 1, 1, t); ok {
			acts = send(acts, kindGrant, ev.Peer, id, p.Grantor.Run(), ctx.Raw, sup)
			if p.beatAt == 0 {
				p.beatAt = ctx.Now + lease.DefaultConfig.Heartbeat
			}
		}
	case kindGrant:
		p.Holder.OnGrant(ev.Peer, lease.Grant{ID: core.RequestID(id), Run: run, Raw: raw, Support: support}, t)
	case kindBeat:
		p.Holder.OnHeartbeat(ev.Peer, run, raw, t)
	}
	return acts, true
}

// Tick asks every peer due a request, none during the grantor's restart wait, and heartbeats every
// holder whose grant runs once a heartbeat is due; the loop calls it every few milliseconds.
func (p *Pair) Tick(ctx *core.Context, acts []core.Action) []core.Action {
	t := lease.Now(ctx)
	for _, g := range p.Peers {
		if p.Grantor.Waiting(t) || !p.Holder.Due(g, t) {
			continue
		}
		p.next++
		p.Holder.Ask(g, core.RequestID(p.next), t)
		acts = send(acts, kindAsk, g, p.next, 0, 0, 0)
	}
	if p.beatAt == 0 || ctx.Now < p.beatAt {
		return acts
	}
	p.beatAt = 0
	for _, h := range p.Grantor.Granting(nil, t) {
		acts = send(acts, kindBeat, h, 0, p.Grantor.Run(), ctx.Raw, 0)
		p.beatAt = ctx.Now + lease.DefaultConfig.Heartbeat
	}
	return acts
}

func send(acts []core.Action, kind byte, to core.NodeID, id, run uint64, raw core.Tick, support uint64) []core.Action {
	a := core.Action{Kind: core.ActNetSend, Peer: to}
	b := le.AppendUint64([]byte{core.ProtoGroup}, Group)
	b = append(b, kind)
	for _, v := range [...]uint64{id, run, uint64(raw), support} {
		b = le.AppendUint64(b, v)
	}
	a.Msg.Len = uint16(copy(a.Msg.Body[:], b))
	return append(acts, a)
}
