package meta

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"errors"
	"math"
	"math/bits"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/lease"
)

// The sessions protocol is design.md's "Sessions and fencing" and models/sessions/sessions.tla.

// SessionLease is session_lease, what a renewal gives a session on the leader's clocks before the
// leader stretches it by its measured rate over the client's.
const SessionLease = core.Tick(10_000_000)

// DeadWait is dead_wait, what the leader waits on a session it has not measured.
const DeadWait = 2 * SessionLease

// stallAfter is the gap between two inputs that shows the leader's loop fell behind by more than a
// heartbeat interval.
const stallAfter = core.Tick(2_000_000)

// maxSessionOps is the most session keys one Txn writes; what is left commits in the next.
const maxSessionOps = 8192

// ErrUnknownSession reports an eviction of a session that is not live.
var ErrUnknownSession = errors.New("meta: unknown session")

// SessionStatus is what the leader answers a session heartbeat.
type SessionStatus uint8

// The answers to a session heartbeat: renewed; answered without a renewal, while the session is
// being evicted; or fenced, also for a session the table does not hold, after which the client
// starts a new session.
const (
	SessionRenewed SessionStatus = iota + 1
	SessionHeld
	SessionFenced
)

// SessionBeat is a client daemon's session heartbeat: its session, this heartbeat's request ID,
// above every earlier send of the session's, and its CLOCK_MONOTONIC_RAW when it sent this heartbeat;
// with the request ID of the last answer it got and that answer's arrival on the same clock, the
// issuer of the grants it holds, and what it says of each pool.
type SessionBeat struct {
	Session, Request uint64
	Sent             core.Tick
	Answered         uint64
	Arrived          core.Tick
	Issuer           Issuer
	Pools            []PoolBeat
}

// SessionAnswer is the leader's answer to the heartbeat Request: its CLOCK_MONOTONIC_RAW when it
// processed it, its status, the leader lease that answered, and a grant for each pool the
// heartbeat named.
type SessionAnswer struct {
	Request uint64
	Raw     core.Tick
	Status  SessionStatus
	Issuer  Issuer
	Grants  []PoolGrant
}

// SessionEntry is a session as the table commits it: its node, its newest incarnation, and the
// table version that fenced it, 0 while it is live.
type SessionEntry struct {
	ID          uint64
	Node        uint32
	Incarnation uint64
	Fenced      uint64
}

// SessionTable is the committed session table: its version, which counts fence commits, the
// version every storage node acknowledged, the next session ID, and the sessions in ID order,
// those fenced at or below Acked having left it.
type SessionTable struct {
	Version, Acked, Next uint64
	Sessions             []SessionEntry
}

// Storage is t as a storage node holds it: its version, next session ID and live sessions.
func (t *SessionTable) Storage() chunkstore.SessionTable {
	s := chunkstore.SessionTable{Version: t.Version, Next: t.Next}
	for _, e := range t.Sessions {
		if e.Fenced == 0 {
			s.Live = append(s.Live, e.ID)
		}
	}
	return s
}

// SessionStart is a daemon's request to start a session on Node, and the ID it got once committed.
type SessionStart struct {
	Node        uint32
	Request, ID uint64
}

// sessTimer is the leader's count for a live session: when it last renewed it, or started counting
// it; its wait, 0 until measured; the highest request it answered and the raw time of its first
// answer to it; and an eviction.
type sessTimer struct {
	id    uint64
	at    lease.Time
	wait  core.Tick
	req   uint64
	aRaw  core.Tick
	evict bool
}

// SessionTable is the committed session table.
func (c *Control) SessionTable() *SessionTable { return &c.sess }

// StartSession stages a session for a daemon on node, which the next Step commits with a new ID.
func (c *Control) StartSession(node uint32, request uint64) {
	c.starts = append(c.starts, SessionStart{Node: node, Request: request})
}

// FetchAnswer is the session table at t as storage nodes hold it, for a node's fetch, or false
// while the leader does not serve.
func (c *Control) FetchAnswer(t lease.Time, leased bool) (chunkstore.SessionTable, bool) {
	if !c.Serves(t, leased) {
		return chunkstore.SessionTable{}, false
	}
	return c.sess.Storage(), true
}

// Serves reports whether this leader answers a node's session fetch or a client's session start at
// t: not while it does not hold its leader lease, has not yet stepped under it, or has not had a
// session Txn of its own commit, since a new leader's applied table can lag a start the old leader
// committed, and a node would refuse a live session's writes.
func (c *Control) Serves(t lease.Time, leased bool) bool {
	c.sessClock(t, leased)
	return leased && c.grantsFrom.set && c.current
}

// Renew answers a session heartbeat at t into a, unless this leader does not hold its leader lease,
// has not yet stepped under it, or has not seen the session start (its table lags), and renews a live session not being evicted, granting it space
// in each pool it names. Before that step the leader knows no pool's room, and an answer granting 0
// would replace the grant whose window the daemon's next heartbeat must count. It measures the leader's raw rate over the client's
// from the heartbeat and the answer it names, when that is this leader's first answer to the highest
// request of the session: a later answer to the same request would measure the rate too low.
func (c *Control) Renew(t lease.Time, leased bool, b *SessionBeat, a *SessionAnswer) bool {
	c.sessClock(t, leased)
	if !c.plant.answerWithoutLease && (!leased || !c.grantsFrom.set) || b.Session >= c.sess.Next {
		return false
	}
	*a = SessionAnswer{Request: b.Request, Raw: t.Raw, Status: SessionFenced, Issuer: c.issuer, Grants: a.Grants[:0]}
	if !c.live(b.Session) {
		_, inTable := slices.BinarySearchFunc(c.sess.Sessions, b.Session, cmpSession)
		c.grant(t.Boot, b, inTable, false, a)
		return true
	}
	tm := c.timer(b.Session, t)
	wait := core.Tick(0)
	if b.Answered != 0 && b.Answered == tm.req && b.Sent > b.Arrived && t.Raw > tm.aRaw {
		wait = stretch(t.Raw-tm.aRaw, b.Sent-b.Arrived)
	}
	if b.Request > tm.req {
		tm.req, tm.aRaw = b.Request, t.Raw
	}
	a.Status = SessionHeld
	if !tm.evict {
		tm.at, tm.wait, a.Status = t, wait, SessionRenewed
	}
	c.grant(t.Boot, b, true, !tm.evict, a)
	return true
}

// stretch is the wait after a renewal on a leader whose raw clock ran dl while the client's ran dc:
// session_lease times the larger of 1 and the bound dl / dc, rounded up. A bound above 2 counts as
// 2, which dead_wait already takes no voter's raw clock to exceed over a client's; only a client
// that stood still between the two can measure more.
func stretch(dl, dc core.Tick) core.Tick {
	hi, lo := bits.Mul64(uint64(SessionLease), uint64(dl))
	if hi >= uint64(dc) {
		return DeadWait
	}
	q, r := bits.Div64(hi, lo, uint64(dc))
	if r != 0 {
		q++
	}
	return core.Tick(min(max(q, uint64(SessionLease)), uint64(DeadWait)))
}

// Evict stops renewing session id, which is fenced once its lease runs out on the leader's clocks.
func (c *Control) Evict(id uint64) error {
	if !c.live(id) {
		return ErrUnknownSession
	}
	c.timer(id, c.seen).evict = true
	return nil
}

func (c *Control) live(id uint64) bool {
	i, ok := slices.BinarySearchFunc(c.sess.Sessions, id, cmpSession)
	return ok && c.sess.Sessions[i].Fenced == 0
}

func cmpSession(e SessionEntry, id uint64) int { return cmp.Compare(e.ID, id) }

// timer is session id's count, which starts at t if the leader holds none.
func (c *Control) timer(id uint64, t lease.Time) *sessTimer {
	i, ok := slices.BinarySearchFunc(c.timers, id, func(x sessTimer, id uint64) int { return cmp.Compare(x.id, id) })
	if !ok {
		c.timers = slices.Insert(c.timers, i, sessTimer{id: id, at: t})
	}
	return &c.timers[i]
}

// sessClock restarts every count at t after a stall, and at the start of a leader lease: a new
// leader's first, since renewals lived only in the old leader's memory, or one regained, since no
// session could renew in between. Grants start over at a lease lost or regained, under a new issuer.
func (c *Control) sessClock(t lease.Time, leased bool) {
	c.stall(t)
	if leased != c.leased {
		c.resetGrants()
	}
	if leased && !c.leased {
		if !c.plant.noFullLease {
			c.restart(t)
		}
		c.issuer.Start = max(t.Raw, c.issuer.Start+1)
	}
	c.leased = leased
}

// stall restarts every count at t when the last input is more than stallAfter old.
func (c *Control) stall(t lease.Time) {
	if c.seen != (lease.Time{}) && t.Boot-min(c.seen.Boot, t.Boot) > stallAfter {
		c.restart(t)
	}
	c.seen = t
}

func (c *Control) restart(t lease.Time) {
	for i := range c.timers {
		c.timers[i].at = t
	}
}

// resume restarts every count at t and forgets every session's last answer, since a pair across
// the leader's own stop leaves the stop out of its time and measures its rate too low.
func (c *Control) resume(t lease.Time) {
	c.restart(t)
	for i := range c.timers {
		c.timers[i].req = 0
	}
}

// sessionPlants are bugs a test plants, each the sessions model's counterexample of the same name:
// a session not measured waiting only session_lease, every session waiting session_lease on the
// leader's own clock, renewals answered without the leader lease or the grants step only it starts,
// an eviction fenced at once, a leader lease that starts no count over, so a new leader counts
// from its first step, which can come before the old leader's last renewal, a fenced session's
// locks released at its fence's commit, and an acknowledged version that counts only the nodes
// this leader heard from or waits for nodes whose disks are marked down, and a disk marked up
// while its node holds an older table.
type sessionPlants struct {
	deadWaitLease, countOnOwnClock, answerWithoutLease, evictAtOnce, noFullLease bool
	releaseOnCommit, ackReachableOnly, ackWaitsDown, upWithOldTable              bool
	// noHeadCheck is not the model's: a session Txn without its check of the table's head.
	noHeadCheck bool
}

// due reports whether the session's wait has passed on both of the leader's clocks at t.
func (c *Control) due(tm *sessTimer, t lease.Time) bool {
	w := tm.wait
	switch {
	case c.plant.evictAtOnce && tm.evict:
		return true
	case c.plant.countOnOwnClock:
		w = SessionLease
	case w == 0 && c.plant.deadWaitLease:
		w = SessionLease
	case w == 0:
		w = DeadWait
	}
	return t.Boot-min(tm.at.Boot, t.Boot) >= w && t.Raw-min(tm.at.Raw, t.Raw) >= w
}

// acknowledged reports whether every node with a disk committed up holds session-table version v.
func (c *Control) acknowledged(v uint64) bool {
	for _, e := range c.disks {
		if !e.Up && (!c.plant.ackWaitsDown || e.Gone) {
			continue
		}
		i := slices.IndexFunc(c.nodes, func(n nodeState) bool { return n.id == e.Node })
		if i < 0 && c.plant.ackReachableOnly {
			continue
		}
		if i < 0 || c.nodes[i].sessV < v {
			return false
		}
	}
	return true
}

// Holding reports whether session id still holds its locks, which is until it leaves the table, once
// every storage node holds its fence; an owner frees a lock key whose session does not. An ID the
// table has not handed out yet holds: a new leader's table can lag a start the old leader committed.
func (c *Control) Holding(id uint64) bool {
	if id >= c.sess.Next {
		return true
	}
	i, ok := slices.BinarySearchFunc(c.sess.Sessions, id, cmpSession)
	return ok && (c.sess.Sessions[i].Fenced == 0 || !c.plant.releaseOnCommit)
}

// sessionStep proposes under token, while this leader holds its lease, one Txn of the session
// table: every session due at t fenced as one new version, the version every storage node holds
// committed as acknowledged with the sessions it fenced removed, and the staged starts; or, with
// head and until one of this machine's has committed, a Txn of the head's checks alone.
func (c *Control) sessionStep(t lease.Time, token uint64, head bool, out *ControlOutput) (bool, error) {
	timers := make([]sessTimer, 0, len(c.timers))
	j := 0
	for _, e := range c.sess.Sessions {
		if e.Fenced != 0 {
			continue
		}
		for j < len(c.timers) && c.timers[j].id < e.ID {
			j++
		}
		tm := sessTimer{id: e.ID, at: t}
		if j < len(c.timers) && c.timers[j].id == e.ID {
			tm = c.timers[j]
		}
		timers = append(timers, tm)
	}
	c.timers = timers
	if !c.leased {
		return false, nil
	}
	p := &commit{token: token, next: c.ctlState, session: true}
	n := &p.next.sess
	n.Sessions = slices.Clone(c.sess.Sessions)
	var ops []Op
	for _, tm := range c.timers {
		if len(ops) < maxSessionOps && c.due(&tm, t) {
			i, _ := slices.BinarySearchFunc(n.Sessions, tm.id, cmpSession)
			n.Sessions[i].Fenced, p.fenced = c.sess.Version+1, true
			ops = append(ops, Put(sessionKey(tm.id), appendSession(nil, &n.Sessions[i])))
		}
	}
	if p.fenced {
		n.Version++
	}
	if c.sess.Version > c.sess.Acked && c.acknowledged(c.sess.Version) {
		n.Acked = c.sess.Version
	}
	n.Sessions = slices.DeleteFunc(n.Sessions, func(e SessionEntry) bool {
		left := e.Fenced != 0 && e.Fenced <= n.Acked && len(ops) < maxSessionOps
		if left {
			ops = append(ops, Delete(sessionKey(e.ID)))
		}
		return left
	})
	for _, s := range c.starts {
		if len(ops) >= maxSessionOps {
			break
		}
		s.ID = max(n.Next, 1)
		n.Next = s.ID + 1
		e := SessionEntry{ID: s.ID, Node: s.Node, Incarnation: 1}
		n.Sessions = append(n.Sessions, e)
		p.started = append(p.started, s)
		ops = append(ops, Put(sessionKey(s.ID), appendSession(nil, &e)))
	}
	if len(ops) == 0 && n.Acked == c.sess.Acked && (c.current || !head) {
		return false, nil
	}
	tx := Txn{Ops: append([]Op{
		guard(sessionKey(0), c.sess.Next != 0, appendSessionHead(nil, &c.sess)),
		guard(ctlKey(CtlMembership, nil), c.diskV != 0, bigEndian.AppendUint64(nil, c.diskV)),
	}, ops...)}
	if n.Next != 0 {
		tx.Ops = slices.Insert(tx.Ops, 2, Put(sessionKey(0), appendSessionHead(nil, n)))
	}
	if c.plant.noHeadCheck {
		tx.Ops = tx.Ops[1:]
	}
	if _, err := tx.Check(); err != nil {
		return false, err
	}
	c.pending = p
	out.Propose, out.Txn, out.Token = true, tx, token
	return true, nil
}

func sessionKey(id uint64) []byte { return ctlKey(CtlSession, bigEndian.AppendUint64(nil, id)) }

// The session table's head, under session ID 0: the next ID, the version and the acknowledged
// version, 8 bytes each.
func appendSessionHead(dst []byte, t *SessionTable) []byte {
	return bigEndian.AppendUint64(bigEndian.AppendUint64(bigEndian.AppendUint64(dst, t.Next), t.Version), t.Acked)
}

// A session's entry: its node, its incarnation and the version that fenced it, as uvarints.
func appendSession(dst []byte, e *SessionEntry) []byte {
	return binary.AppendUvarint(binary.AppendUvarint(binary.AppendUvarint(dst, uint64(e.Node)), e.Incarnation), e.Fenced)
}

// decodeSession decodes session id's entry; ErrMalformed for a node over 32 bits, incarnation 0,
// or anything appendSession would not write.
func decodeSession(id uint64, b []byte) (SessionEntry, error) {
	d := decoder{b: b}
	node, inc, fenced := d.uvarint(), d.uvarint(), d.uvarint()
	e := SessionEntry{ID: id, Node: uint32(node), Incarnation: inc, Fenced: fenced}
	if d.bad || len(d.b) != 0 || node > math.MaxUint32 || inc == 0 || !bytes.Equal(appendSession(nil, &e), b) {
		return SessionEntry{}, ErrMalformed
	}
	return e, nil
}
