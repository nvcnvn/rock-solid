package meta

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/lease"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

// at is the leader's two clocks.
func (r *ctlRun) at() lease.Time { return lease.Time{Boot: r.now, Raw: r.raw} }

// tick moves both of the leader's clocks on by d.
func (r *ctlRun) tick(d core.Tick) { r.now, r.raw = r.now+d, r.raw+d }

// sessStep steps the control plane at its clocks, holding its lease, and commits what it proposes.
func (r *ctlRun) sessStep() *ControlOutput {
	r.t.Helper()
	r.settle(&ControlInputs{Raw: r.raw, Leased: true})
	return &r.out
}

// sessClient is a client daemon's side of its session's heartbeats.
type sessClient struct {
	id, req, answered uint64
	arrived           core.Tick
}

// startSessions starts n sessions, of daemons on nodes 0 to 4 in turn, and commits them.
func (r *ctlRun) startSessions(n int) []*sessClient {
	r.t.Helper()
	for i := range n {
		r.ctl.StartSession(uint32(i%5), uint64(i)+1)
	}
	var cs []*sessClient
	for _, s := range r.sessStep().Started {
		cs = append(cs, &sessClient{id: s.ID})
	}
	if len(cs) != n {
		r.t.Fatalf("started %d sessions of %d", len(cs), n)
	}
	return cs
}

// renew sends c's heartbeat, sent when the client's raw clock read sent, to the leader holding its
// lease; the answer arrives at once.
func (r *ctlRun) renew(c *sessClient, sent core.Tick) SessionStatus {
	r.t.Helper()
	c.req++
	var a SessionAnswer
	if !r.ctl.Renew(r.at(), true, &SessionBeat{Session: c.id, Request: c.req, Sent: sent, Answered: c.answered, Arrived: c.arrived}, &a) {
		r.t.Fatal("no answer under the leader lease")
	}
	c.answered, c.arrived = a.Request, sent
	return a.Status
}

// fenced reports whether session id is fenced or has left the table.
func (r *ctlRun) fenced(id uint64) bool {
	s := r.ctl.SessionTable()
	i, ok := slices.BinarySearchFunc(s.Sessions, id, cmpSession)
	return !ok || s.Sessions[i].Fenced != 0
}

// A daemon's start commits one key with an ID from a counter; 100 heartbeats renewing 10 sessions
// commit nothing; the sessions due together are fenced as one version, which every node with a
// disk up then holds, so it is acknowledged and the sessions leave the table. A later start does
// not reuse an ID.
func TestASessionStartsRenewsInMemoryAndIsFencedThenAcknowledged(t *testing.T) {
	r := newCtlRun(t)
	cs := r.startSessions(10)
	if cs[0].id != 1 || cs[9].id != 10 {
		t.Fatalf("started IDs %d to %d", cs[0].id, cs[9].id)
	}
	if got := r.load(r.l).SessionTable(); got.Next != 11 || len(got.Sessions) != 10 {
		t.Fatalf("read back %+v", got)
	}
	for i := range 100 {
		r.tick(sec / 10)
		if st := r.renew(cs[i%10], r.now); st != SessionRenewed {
			t.Fatalf("answer %d", st)
		}
		if r.propose(&ControlInputs{Now: r.now, Raw: r.raw, Leased: true}) {
			t.Fatal("a renewal proposed a Txn")
		}
	}
	for r.ctl.SessionTable().Version == 0 {
		r.tick(sec)
		if r.now > 2*DeadWait {
			t.Fatal("never fenced")
		}
		r.hearAll()
		r.sessStep()
	}
	s := r.ctl.SessionTable()
	if s.Version != 1 || slices.ContainsFunc(s.Sessions, func(e SessionEntry) bool { return e.Fenced != 1 }) || !r.out.SessionPush {
		t.Fatalf("fenced %+v push %v", s, r.out.SessionPush)
	}
	for n := range uint32(5) {
		nr := r.reports[n]
		nr.Sessions = 1
		r.heartbeat(&nr)
	}
	r.sessStep()
	if s := r.ctl.SessionTable(); s.Acked != 1 || len(s.Sessions) != 0 {
		t.Fatalf("after the acknowledgment %+v", s)
	}
	r.ctl.StartSession(2, 11)
	if out := r.sessStep(); out.Started[0].ID != 11 {
		t.Fatalf("reused an ID: %+v", out.Started)
	}
}

// A version waits for every node with a disk committed up; a node whose disk is committed down
// counts as holding it, with no heartbeat of its own.
func TestANodeMarkedDownCountsAsAcknowledging(t *testing.T) {
	r := newCtlRun(t)
	r.startSessions(1)
	for r.ctl.SessionTable().Version == 0 {
		r.tick(sec)
		r.hearAll()
		r.sessStep()
	}
	for n := range uint32(4) {
		nr := r.reports[n]
		nr.Sessions = 1
		r.heartbeat(&nr)
	}
	r.tick(sec)
	r.sessStep()
	if r.ctl.SessionTable().Acked != 0 {
		t.Fatal("acknowledged with node 4 behind and its disk up")
	}
	r.fail(5)
	for i := 0; r.ctl.SessionTable().Acked == 0; i++ {
		if i == 4 {
			t.Fatalf("not acknowledged with disk 5 %+v", r.disk(5))
		}
		r.sessStep()
	}
	if r.disk(5).Up || len(r.ctl.SessionTable().Sessions) != 0 {
		t.Fatalf("acknowledged with disk 5 %+v and table %+v", r.disk(5), r.ctl.SessionTable())
	}
}

// A new leader answers no heartbeat before its leader lease starts, and from that start gives every
// live session a full count: dead_wait for one it has not measured, which includes one that sent it
// a single heartbeat, and session_lease times its bound once two heartbeats measured it.
func TestANewLeaderGivesEverySessionAFullLease(t *testing.T) {
	r := newCtlRun(t)
	cs := r.startSessions(3)
	for range 5 {
		r.tick(sec)
		for _, c := range cs {
			r.renew(c, r.now)
		}
		r.sessStep()
	}
	r.failover()
	for range 3 {
		r.tick(sec)
		var a SessionAnswer
		if r.ctl.Renew(r.at(), false, &SessionBeat{Session: cs[0].id, Request: 99, Sent: r.now}, &a) {
			t.Fatal("answered before its leader lease")
		}
		if r.settle(&ControlInputs{Raw: r.raw}) && r.ctl.SessionTable().Version != 0 {
			t.Fatal("fenced before its leader lease")
		}
	}
	r.tick(sec)
	start := r.now
	want := []core.Tick{start + 2*sec + SessionLease, start + sec + DeadWait, start + DeadWait}
	for r.now <= start+DeadWait+2*sec {
		r.sessStep()
		switch r.now - start {
		case sec:
			r.renew(cs[0], r.now)
			r.renew(cs[1], r.now)
		case 2 * sec:
			r.renew(cs[0], r.now)
		}
		for i, c := range cs {
			if r.fenced(c.id) != (r.now >= want[i]) {
				t.Fatalf("session %d fenced %v %v after the lease began", c.id, r.fenced(c.id), r.now-start)
			}
		}
		r.tick(sec)
	}
}

// The leader side of a client whose CLOCK_BOOTTIME runs 22.6% fast: the leader crashes 1 s after
// its last renewal of C, and a new leader whose own CLOCK_BOOTTIME runs 22.6% fast answers 8 s, or
// 5 s, after the crash. C renews with it from then and is never fenced; D, whose daemon died with
// the old leader, is fenced once dead_wait has passed on the new leader's raw clock as well, not
// sooner on its fast boot clock.
func TestAFastBootClockFencesNoSessionEarly(t *testing.T) {
	for _, gap := range []core.Tick{8 * sec, 5 * sec} {
		r := newCtlRun(t)
		cs := r.startSessions(2)
		c, d := cs[0], cs[1]
		for range 5 {
			r.tick(sec)
			r.renew(c, r.raw)
			r.renew(d, r.raw)
			r.sessStep()
		}
		r.tick(sec)
		r.failover()
		fast := func(dt core.Tick) { r.now, r.raw = r.now+dt*1226/1000, r.raw+dt }
		for range gap / sec {
			fast(sec)
			r.settle(&ControlInputs{Raw: r.raw})
		}
		start := r.raw
		for r.raw < start+DeadWait+3*sec {
			r.sessStep()
			r.renew(c, r.raw)
			if r.fenced(c.id) || r.fenced(d.id) != (r.raw-start >= DeadWait) {
				t.Fatalf("new leader %v after the crash: C fenced %v, D fenced %v, %v into its lease", gap, r.fenced(c.id), r.fenced(d.id), r.raw-start)
			}
			fast(sec)
		}
	}
}

// A leader whose inputs stop for more than stallAfter, or that resumes from a pause, restarts every
// session's count, which only delays a fence.
func TestAStallOrAResumeRestartsEveryCount(t *testing.T) {
	for _, resume := range []bool{false, true} {
		r := newCtlRun(t)
		c := r.startSessions(1)[0]
		for range 2 {
			r.tick(sec)
			r.renew(c, r.now)
		}
		for range 5 {
			r.tick(sec)
			r.sessStep()
		}
		if resume {
			r.tick(sec)
			r.ctl.OnEvent(&core.Context{Now: r.now, Raw: r.raw}, &core.Event{Kind: core.EvResumed}, nil)
		} else {
			r.tick(3 * sec)
		}
		from := r.now
		for r.now <= from+SessionLease+sec {
			r.sessStep()
			if r.fenced(c.id) != (r.now >= from+SessionLease) {
				t.Fatalf("resume %v: fenced %v %v after the restart", resume, r.fenced(c.id), r.now-from)
			}
			r.tick(sec)
		}
	}
}

// An eviction, an owner's unanswered recall or an operator's, only stops renewals: the client's
// heartbeats are answered without renewing it, and it is fenced once the lease of its last renewal
// runs out, never at once.
func TestAnEvictionOnlyStopsRenewals(t *testing.T) {
	r := newCtlRun(t)
	c := r.startSessions(1)[0]
	if err := r.ctl.Evict(c.id + 1); !errors.Is(err, ErrUnknownSession) {
		t.Fatalf("evicting an unknown session: %v", err)
	}
	for range 2 {
		r.tick(sec)
		r.renew(c, r.now)
	}
	last := r.now
	if err := r.ctl.Evict(c.id); err != nil {
		t.Fatal(err)
	}
	for r.now <= last+SessionLease+sec {
		r.tick(sec)
		r.sessStep()
		want := SessionHeld
		if r.now >= last+SessionLease {
			want = SessionFenced
		}
		if r.fenced(c.id) != (want == SessionFenced) {
			t.Fatalf("fenced %v %v after the last renewal", r.fenced(c.id), r.now-last)
		}
		if st := r.renew(c, r.now); st != want {
			t.Fatalf("answer %d %v after the last renewal", st, r.now-last)
		}
	}
}

// A leader whose raw clock runs 5% fast over its clients' measures it from two heartbeats and
// fences 10.5 s after the last renewal; a heartbeat that echoes an answer older than the leader's
// last, as after a lost answer, leaves its session unmeasured again, fenced only after dead_wait.
func TestAFenceStretchesByTheMeasuredRate(t *testing.T) {
	r := newCtlRun(t)
	cs := r.startSessions(2)
	fast := func(d core.Tick) { r.now, r.raw = r.now+d, r.raw+d*105/100 }
	for range 3 {
		fast(sec)
		r.sessStep()
		r.renew(cs[0], r.now)
		r.renew(cs[1], r.now)
	}
	fast(sec)
	r.sessStep()
	r.renew(cs[0], r.now)
	lost := *cs[1]
	r.renew(cs[1], r.now)
	cs[1].answered, cs[1].arrived = lost.answered, lost.arrived
	last0 := r.now
	fast(sec)
	r.sessStep()
	r.renew(cs[1], r.now)
	last1 := r.now
	for r.now < last1+DeadWait+sec {
		fast(sec / 10)
		r.sessStep()
		if r.fenced(cs[0].id) != (r.now >= last0+SessionLease*105/100) || r.fenced(cs[1].id) != (r.now >= last1+DeadWait) {
			t.Fatalf("fenced %v and %v at %v and %v after their last renewals", r.fenced(cs[0].id), r.fenced(cs[1].id), r.now-last0, r.now-last1)
		}
	}
}

// A heartbeat the leader processes twice, as after a resend or a redelivery, or an older one
// arriving late, leaves the pair at the first answer to the highest request: a leader whose raw
// clock runs 5% fast still fences 10.5 s after the last renewal, where a pair from the later answer
// would bound its rate below 1 and fence after 10 s.
func TestARepeatedHeartbeatIsMeasuredFromItsFirstAnswer(t *testing.T) {
	for _, late := range []bool{false, true} {
		r := newCtlRun(t)
		c := r.startSessions(1)[0]
		fast := func(d core.Tick) { r.now, r.raw = r.now+d, r.raw+d*105/100 }
		fast(sec)
		r.sessStep()
		first := *c
		r.renew(c, r.now)
		second := *c
		r.renew(c, r.now)
		after := *c
		fast(sec / 2)
		*c = second
		if late {
			*c = first
		}
		r.renew(c, r.now)
		*c = after
		fast(sec / 2)
		r.renew(c, r.now)
		last := r.now
		for r.now < last+DeadWait {
			fast(sec / 10)
			r.sessStep()
			if r.fenced(c.id) != (r.now >= last+SessionLease*105/100) {
				t.Fatalf("late %v: fenced %v %v after the last renewal", late, r.fenced(c.id), r.now-last)
			}
		}
	}
}

// A measured bound stretches the lease, but never past dead_wait: only a client that stood still
// between an answer and its next heartbeat measures more than dead_wait assumes.
func TestAStretchLiesBetweenTheLeaseAndDeadWait(t *testing.T) {
	for _, c := range []struct{ dl, dc, want core.Tick }{
		{sec, 2 * sec, SessionLease},
		{sec * 105 / 100, sec, SessionLease * 105 / 100},
		{sec + 1, sec, SessionLease + SessionLease/sec},
		{30 * sec, sec, DeadWait},
		{1 << 62, 1, DeadWait},
	} {
		if got := stretch(c.dl, c.dc); got != c.want {
			t.Errorf("stretch(%v, %v) = %v, want %v", c.dl, c.dc, got, c.want)
		}
	}
}

const (
	invSessionLease = "no session is fenced while its client's own lease still runs"
	invSessionReads = "a new leader reads the session table back as committed"
	invSessionIDs   = "no session ID is given twice"
	invSessionAcks  = "a version is acknowledged only once every node with a disk up holds it"
)

// sessClock is a node's two clocks in the session run, which read off + T × rate / PPM at real
// time T.
type sessClock struct {
	off             core.Tick
	bootPPM, rawPPM uint64
}

func (c *sessClock) at(t core.Tick) lease.Time {
	return lease.Time{Boot: c.off + core.Tick(uint64(t)*c.bootPPM/lease.PPM), Raw: c.off + core.Tick(uint64(t)*c.rawPPM/lease.PPM)}
}

// runClient is a client daemon in the session run: its clocks and lease holder, the control-plane
// node it renews with and when it learns the leader, its heartbeats' state, and whether it died, was
// fenced, or staged a new session under start.
type runClient struct {
	clk           sessClock
	h             *lease.Holder
	id            uint64
	to            int
	learn         core.Tick
	req, answered uint64
	arrived       core.Tick
	dead, fenced  bool
	start, starts uint64
}

// sessMsg is a heartbeat to control-plane node, or the answer from node's run, arriving at at.
type sessMsg struct {
	at     core.Tick
	client int
	node   int
	run    uint64
	reply  bool
	beat   SessionBeat
	ans    SessionAnswer
}

// No session is fenced while its client's own lease, counted by lease.Holder on clocks at their own
// rates, still runs: through failovers, deposed leaders answering until their leases end, stalls,
// lost and delayed messages, evictions and daemons that die. Every new leader reads the table back
// as committed, no ID is given twice, and a dead daemon's session is fenced and acknowledged.
func TestNoSessionIsFencedWhileItsClientsLeaseRuns(t *testing.T) {
	seeds.Run(t, 0, 20, func(seed uint64) {
		if err := sessionRun(t, seed, sessionPlants{}); err != nil {
			t.Fatal(err)
		}
	})
}

// A leader whose clocks run 10% fast over its clients' takes over the moment the old leader
// renewed C: C's lease from the old leader still runs 9.9 s, and the new leader fences C no sooner,
// whether C dies at once, unmeasured, or after two heartbeats that measure it, or after two
// heartbeats between which the new leader stood still for 200 ms with its clocks frozen.
func TestAFastNewLeaderFencesNoSessionEarly(t *testing.T) {
	for _, c := range []struct {
		measured bool
		stop     core.Tick
	}{{false, 0}, {true, 0}, {true, 200_000}} {
		if err := fastLeaderRun(t, c.measured, c.stop, sessionPlants{}); err != nil {
			t.Errorf("measured %v, stop %v: %v", c.measured, c.stop, err)
		}
	}
}

// Each planted leader-side bug of the sessions model fences a session whose client's lease runs:
// on seed 38 of the seeded run, and every time in the fast new leader's scenario that exposes it.
func TestPlantedSessionBugsFenceEarly(t *testing.T) {
	for _, p := range []struct {
		name string
		seed uint64
		pl   sessionPlants
	}{
		{"session_lease for an unmeasured session", 38, sessionPlants{deadWaitLease: true}},
		{"session_lease on the leader's own clock", 38, sessionPlants{countOnOwnClock: true}},
	} {
		if err := sessionRun(t, p.seed, p.pl); err == nil || !strings.Contains(err.Error(), invSessionLease) {
			t.Errorf("%s at seed %d: %v", p.name, p.seed, err)
		}
	}
	for _, p := range []struct {
		name     string
		measured bool
		pl       sessionPlants
	}{
		{"session_lease for an unmeasured session", false, sessionPlants{deadWaitLease: true}},
		{"session_lease on the leader's own clock", true, sessionPlants{countOnOwnClock: true}},
	} {
		if err := fastLeaderRun(t, p.measured, 0, p.pl); err == nil || !strings.Contains(err.Error(), invSessionLease) {
			t.Errorf("%s, fast new leader: %v", p.name, err)
		}
	}
}

// fastLeaderRun renews client C's session with a leader on clocks at C's rate for 5 s, then fails
// over, right after a renewal, to a leader whose clocks run 10% fast and whose leader lease starts
// at once. C dies then, or after two heartbeats to the new leader, the second measuring it, with
// the new leader standing still for stop between them and then getting Resumed; the run fails if
// C is fenced while a lease it holds runs, or is not fenced within 25 s.
func fastLeaderRun(t *testing.T, measured bool, stop core.Tick, pl sessionPlants) error {
	const step = core.Tick(10_000)
	cfg := lease.Config{Lease: SessionLease, Heartbeat: sec, RateGap: 100_000, Window: 10 * sec, Drift: lease.DefaultConfig.Drift, MaxMargin: lease.DefaultConfig.MaxMargin}
	r := newCtlRun(t)
	r.ctl.plant = pl
	c := r.startSessions(1)[0]
	h := lease.NewHolder(cfg)
	gid := func() core.NodeID { return core.NodeID{Node: 100 + uint32(r.l)} }
	oldG := gid()
	var now core.Tick
	ct := func() lease.Time { return lease.Time{Boot: sec + now, Raw: sec + now} }
	ask := func() bool {
		g := gid()
		if !h.Due(g, ct()) {
			return false
		}
		c.req++
		h.Ask(g, core.RequestID(c.req), ct())
		var a SessionAnswer
		if r.ctl.Renew(r.at(), true, &SessionBeat{Session: c.id, Request: c.req, Sent: ct().Raw, Answered: c.answered, Arrived: c.arrived}, &a) {
			c.answered, c.arrived = a.Request, ct().Raw
			if a.Status == SessionRenewed {
				h.OnGrant(g, lease.Grant{ID: core.RequestID(a.Request), Run: 1, Raw: a.Raw}, ct())
			}
		}
		return true
	}
	for now = step; !ask() || now < 5*sec; now += step {
		r.tick(step)
		if now%(sec/10) == 0 {
			r.sessStep()
		}
	}
	r.failover()
	r.ctl.plant = pl
	r.sessStep()
	asks := 0
	if measured {
		asks = 2
	}
	var stood core.Tick
	for end := now + 25*sec; now < end; now += step {
		if asks == 1 && stood < stop {
			if stood += step; stood == stop {
				r.ctl.OnEvent(&core.Context{Now: r.now, Raw: r.raw}, &core.Event{Kind: core.EvResumed}, nil)
			}
			continue
		}
		r.now, r.raw = r.now+step*11/10, r.raw+step*11/10
		if now%(sec/10) == 0 {
			r.sessStep()
		}
		if asks > 0 && ask() {
			asks--
		}
		if r.fenced(c.id) && (h.Valid(oldG, ct()) || h.Valid(gid(), ct())) {
			return fmt.Errorf("%s: session %d fenced at %v", invSessionLease, c.id, now)
		}
	}
	if !r.fenced(c.id) {
		return fmt.Errorf("session %d never fenced", c.id)
	}
	return nil
}

func sessionRun(t *testing.T, seed uint64, pl sessionPlants) error {
	const step = core.Tick(10_000)
	const faultsEnd, end = 90 * sec, 140 * sec
	rng := rand.New(rand.NewPCG(seed, 7))
	lag := rand.New(rand.NewPCG(seed, 8))
	ticks := func(n int) core.Tick { return core.Tick(rng.IntN(n+1)) * step }
	clock := func() sessClock {
		c := sessClock{off: ticks(100), rawPPM: 900_000 + rng.Uint64N(200_001)}
		c.bootPPM = c.rawPPM
		if rng.IntN(4) == 0 {
			c.bootPPM = c.rawPPM * (774_000 + rng.Uint64N(452_001)) / lease.PPM
		}
		return c
	}
	cfg := lease.Config{Lease: SessionLease, Heartbeat: sec, RateGap: 100_000, Window: 10 * sec, Drift: lease.DefaultConfig.Drift, MaxMargin: lease.DefaultConfig.MaxMargin}
	gid := func(n int) core.NodeID { return core.NodeID{Node: 100 + uint32(n)} }

	r := newCtlRun(t)
	r.ctl.plant = pl
	nodes := []sessClock{clock(), clock(), clock()}
	runs := []uint64{1, 1, 1}
	var now core.Tick
	setNow := func() lease.Time {
		lt := nodes[r.l].at(now)
		r.now, r.raw = lt.Boot, lt.Raw
		return lt
	}
	var leaseFrom, stallTo core.Tick
	var old *Control
	oldNode, oldTo := -1, core.Tick(0)

	clients := make([]*runClient, 4)
	for i := range clients {
		clients[i] = &runClient{clk: clock(), h: lease.NewHolder(cfg), start: uint64(i) + 1, starts: 1}
		r.ctl.StartSession(uint32(i), clients[i].start)
	}
	given := map[uint64]bool{}
	fencedIDs := map[uint64]bool{}
	var dup uint64
	dirty := false
	commit := func(in *ControlInputs) {
		if !r.settle(in) {
			return
		}
		dirty = true
		for _, s := range r.out.Started {
			if given[s.ID] {
				dup = s.ID
			}
			given[s.ID] = true
			for _, c := range clients {
				if c.start == s.Request {
					c.id, c.start, c.fenced, c.answered, c.arrived, c.h = s.ID, 0, false, 0, 0, lease.NewHolder(cfg)
				}
			}
		}
		for _, e := range r.ctl.SessionTable().Sessions {
			if e.Fenced != 0 {
				fencedIDs[e.ID] = true
			}
		}
	}
	setNow()
	commit(&ControlInputs{Raw: r.raw, Leased: true})
	if slices.ContainsFunc(clients, func(c *runClient) bool { return c.id == 0 }) {
		t.Fatal("the first step started no session")
	}

	var inv sim.Invariants
	inv.Register(invSessionLease, func() error {
		for i, c := range clients {
			if !fencedIDs[c.id] {
				continue
			}
			for n := range nodes {
				if c.h.Valid(gid(n), c.clk.at(now)) {
					return fmt.Errorf("client %d's session %d is fenced while its lease from node %d runs, at %v", i, c.id, n, now)
				}
			}
		}
		return nil
	})
	inv.Register(invSessionReads, func() error {
		if !dirty {
			return nil
		}
		dirty = false
		got, want := r.load(r.l).SessionTable(), r.ctl.SessionTable()
		if got.Version != want.Version || got.Acked != want.Acked || got.Next != want.Next || !slices.Equal(got.Sessions, want.Sessions) {
			return fmt.Errorf("read back %+v, committed %+v", got, want)
		}
		return nil
	})
	inv.Register(invSessionIDs, func() error {
		if dup != 0 {
			return fmt.Errorf("session ID %d given twice", dup)
		}
		return nil
	})
	var held [5]uint64
	inv.Register(invSessionAcks, func() error {
		acked := r.ctl.SessionTable().Acked
		for _, d := range r.ctl.Disks() {
			if d.Up && held[d.Node] < acked {
				return fmt.Errorf("version %d acknowledged while node %d holds %d", acked, d.Node, held[d.Node])
			}
		}
		return nil
	})

	nextFail := 5*sec + ticks(2000)
	deaths := 0
	var msgs, due []sessMsg
	for now = step; now < end; now += step {
		faults := now < faultsEnd
		if old != nil && now >= oldTo {
			old, oldNode = nil, -1
		}
		if faults && now >= nextFail && old == nil && now >= leaseFrom && now >= stallTo {
			nextFail = now + 3*sec + ticks(1200)
			oldTo = now
			if rng.IntN(2) == 0 {
				runs[r.l]++
			} else {
				old, oldNode, oldTo = r.ctl, r.l, now+ticks(300)
			}
			r.failover()
			r.ctl.plant = pl
			leaseFrom = oldTo + ticks(100)
			for _, c := range clients {
				c.learn, c.start = now+ticks(1500), 0
			}
		}
		if faults && now >= stallTo && rng.IntN(3000) == 0 {
			stallTo = now + 250*step + ticks(250)
		}
		if faults && deaths < 2 && rng.IntN(4000) == 0 {
			clients[rng.IntN(len(clients))].dead = true
			deaths++
		}
		if faults && rng.IntN(4000) == 0 {
			_ = r.ctl.Evict(clients[rng.IntN(len(clients))].id)
		}
		stalled := now < stallTo

		due, msgs = append(due[:0], msgs...), msgs[:0]
		for _, m := range due {
			if m.at > now || !m.reply && m.node == r.l && stalled {
				msgs = append(msgs, m)
				continue
			}
			c := clients[m.client]
			if m.reply {
				if c.dead || c.fenced {
					continue
				}
				ct := c.clk.at(now)
				c.answered, c.arrived = m.ans.Request, ct.Raw
				switch m.ans.Status {
				case SessionRenewed:
					c.h.OnGrant(gid(m.node), lease.Grant{ID: core.RequestID(m.ans.Request), Run: m.run, Raw: m.ans.Raw}, ct)
				case SessionFenced:
					c.fenced = true
				}
				continue
			}
			var a SessionAnswer
			ok := false
			switch m.node {
			case r.l:
				ok = r.ctl.Renew(setNow(), now >= leaseFrom, &m.beat, &a)
			case oldNode:
				ok = old.Renew(nodes[oldNode].at(now), true, &m.beat, &a)
			}
			if ok && rng.IntN(20) != 0 {
				msgs = append(msgs, sessMsg{at: now + ticks(3), client: m.client, node: m.node, run: runs[m.node], reply: true, ans: a})
			}
		}

		for i, c := range clients {
			if c.dead {
				continue
			}
			if c.fenced {
				if c.start == 0 && !stalled {
					c.start, c.starts = uint64(i+1)<<32|(c.starts+1), c.starts+1
					r.ctl.StartSession(uint32(i), c.start)
				}
				continue
			}
			if now >= c.learn {
				c.to = r.l
			}
			ct := c.clk.at(now)
			if !c.h.Due(gid(c.to), ct) {
				continue
			}
			c.req++
			c.h.Ask(gid(c.to), core.RequestID(c.req), ct)
			if rng.IntN(20) != 0 {
				msgs = append(msgs, sessMsg{at: now + ticks(3), client: i, node: c.to, beat: SessionBeat{Session: c.id, Request: c.req, Sent: ct.Raw, Answered: c.answered, Arrived: c.arrived}})
			}
		}

		if !stalled && now%(sec/10) == 0 {
			setNow()
			if now%sec == 0 {
				for n := range uint32(5) {
					if !faults || lag.IntN(3) == 0 {
						held[n] = r.ctl.SessionTable().Version
					}
					r.heartbeat(&NodeReport{Node: n, Meta: true, Opened: []OpenedDisk{{Device: devOf(n), Log: logOf(n)}}, Sessions: held[n]})
				}
			}
			commit(&ControlInputs{Raw: r.raw, Leased: now >= leaseFrom})
		}
		if err := inv.Check(); err != nil {
			return fmt.Errorf("seed %d: %w", seed, err)
		}
	}
	s := r.ctl.SessionTable()
	for i, c := range clients {
		if c.dead && slices.ContainsFunc(s.Sessions, func(e SessionEntry) bool { return e.ID == c.id }) {
			return fmt.Errorf("seed %d: dead client %d's session %d is still in the table %+v", seed, i, c.id, s)
		}
	}
	if s.Acked != s.Version {
		return fmt.Errorf("seed %d: version %d never acknowledged, %d is", seed, s.Version, s.Acked)
	}
	return nil
}
