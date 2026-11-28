package meta

import (
	"slices"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/lease"
)

// ClientLease is a session's lease as its client holds it: session_lease, renewed every second,
// rate_gap, rate_window, lease_drift, max_margin and core.ResumeThreshold.
var ClientLease = lease.Config{Lease: SessionLease, Heartbeat: 1_000_000, RateGap: 100_000, Window: 10_000_000,
	Drift: lease.DefaultConfig.Drift, MaxMargin: lease.DefaultConfig.MaxMargin, Resume: core.ResumeThreshold}

// SessionClient is a client daemon's side of its session: it starts the session, renews it with
// the control-plane leader it is pointed at and counts the lease each leader's answers give, from
// the send of the last answered renewal on both its clocks, within the margin its bound on that
// leader's raw rate needs (lease.Holder), ending every lease on Resumed. Requests wait while it
// holds no lease. A fence, from an answer or a refused write, starts a new session.
type SessionClient struct {
	cfg  lease.Config
	h    *lease.Holder
	node uint32
	// id is the session, 0 while it starts under the start request start, drawn from rng.
	id, start uint64
	rng       *core.Rand
	// inc is the session's incarnation, the high bits of its requests, seq their count, and first
	// the session's first request: answers below it are an earlier session's.
	inc, seq, first, last uint64
	// answered is the request of the last answer and arrived its arrival on CLOCK_MONOTONIC_RAW.
	answered uint64
	arrived  core.Tick
	grantors []core.NodeID
	plant    clientPlants
}

// clientPlants are bugs a test plants in a session's client, each the sessions model's
// counterexample of the same name: a lease counted from an answer's arrival, one that does not end
// lease_drift early, leases kept across Resumed, the step check at 1 + lease_drift / 2, and one
// rate window per leader node across its runs.
type clientPlants struct {
	countFromReply, noDrift, ignoreResumed, fixedStepCheck, windowPerNode bool
}

// NewSessionClient is the session client of a daemon on node, holding leases under cfg, which
// draws each start request from rng.
func NewSessionClient(cfg lease.Config, node uint32, rng *core.Rand) *SessionClient {
	s := &SessionClient{cfg: cfg, node: node, rng: rng}
	s.over()
	return s
}

// over starts a new session: no lease, requests numbered above every earlier one, and a new start
// request of 64 random bits, so that no earlier daemon's start on the node, whose session may still
// be in the table, is taken for it.
func (s *SessionClient) over() {
	s.inc = 1
	s.id, s.start, s.first, s.last = 0, s.rng.Uint64(), s.inc<<32|(s.seq+1), 0
	s.answered, s.arrived, s.grantors = 0, 0, s.grantors[:0]
	c := s.cfg
	c.Plants = lease.Plants{CountFromReply: s.plant.countFromReply, NoDrift: s.plant.noDrift, HalfStep: s.plant.fixedStepCheck}
	s.h = lease.NewHolder(c)
}

// Starting is the start request to send while the session starts, the same each time it is sent
// again, so that the leader starts one session however often it arrives.
func (s *SessionClient) Starting() (uint64, bool) { return s.start, s.id == 0 }

// Started takes the answer to a start request, and reports whether it was the one outstanding.
func (s *SessionClient) Started(request, id uint64) bool {
	if s.id != 0 || request != s.start || id == 0 {
		return false
	}
	s.id = id
	return true
}

// Session is the session's ID, 0 while it starts.
func (s *SessionClient) Session() uint64 { return s.id }

// Leased reports whether the client holds a lease at t, which every request waits for.
func (s *SessionClient) Leased(t lease.Time) bool {
	return s.id != 0 && slices.ContainsFunc(s.grantors, func(g core.NodeID) bool { return s.h.Valid(g, t) })
}

// Held is the session's incarnation, its longest lease's time left at t and its last renewal's age, 0 before one.
func (s *SessionClient) Held(t lease.Time) (inc uint64, left, age core.Tick) {
	for _, g := range s.grantors {
		left = max(left, s.h.Left(g, t))
	}
	if s.arrived != 0 {
		age = t.Raw - min(s.arrived, t.Raw)
	}
	return s.inc, left, age
}

// Next is the boot time at which the next heartbeat to leader may be due.
func (s *SessionClient) Next(leader core.NodeID, t lease.Time) core.Tick { return s.h.Next(leader, t) }

// Beat fills b with a heartbeat to leader if one is due at t, under a request above every
// earlier one, a resend included.
func (s *SessionClient) Beat(leader core.NodeID, t lease.Time, b *SessionBeat) bool {
	if s.id == 0 || !s.h.Due(leader, t) {
		return false
	}
	s.seq++
	req := s.inc<<32 | s.seq&0xffffffff
	s.last = req
	s.h.Ask(leader, core.RequestID(req), t)
	if !slices.Contains(s.grantors, leader) {
		s.grantors = append(s.grantors, leader)
	}
	*b = SessionBeat{Session: s.id, Request: req, Sent: t.Raw, Answered: s.answered, Arrived: s.arrived, Pools: b.Pools[:0]}
	return true
}

// Answer takes leader's answer a, from its loop's run ID run, arriving at t. A renewal is a grant to
// the lease; a fence starts a new session. It reports whether the grant tripped the step check,
// which ended every lease.
func (s *SessionClient) Answer(leader core.NodeID, run uint64, a *SessionAnswer, t lease.Time) (tripped bool) {
	if s.id == 0 || a.Request < s.first || a.Request > s.last {
		return false
	}
	s.answered, s.arrived = a.Request, t.Raw
	switch a.Status {
	case SessionRenewed:
		if s.plant.windowPerNode {
			run = 0
		}
		return s.h.OnGrant(leader, lease.Grant{ID: core.RequestID(a.Request), Run: run, Raw: a.Raw}, t)
	case SessionFenced:
		s.over()
	}
	return false
}

// Fenced takes a write refused because session is fenced: if it is this client's, it starts over.
func (s *SessionClient) Fenced(session uint64) {
	if session != 0 && session == s.id {
		s.over()
	}
}

// Resumed ends every lease, since the client's clocks may have stood still.
func (s *SessionClient) Resumed() {
	if !s.plant.ignoreResumed {
		s.h.Resumed()
	}
}
