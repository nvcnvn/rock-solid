package meta

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"sort"

	"go.etcd.io/raft/v3"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/raftshard"
)

var (
	// ErrCrossShard reports a Txn or Scan whose keys span two Raft groups, refused until phase 13's cross-shard transactions; a *CrossShard names the groups.
	ErrCrossShard = errors.New("meta: keys span Raft groups")
	// ErrReplayTagged reports a client Txn, one with a replay key, that writes or removes a replay-tagged key.
	ErrReplayTagged = errors.New("meta: client txn writes a replay-tagged key")
	// ErrRanges reports a shard map whose ranges a router cannot route by.
	ErrRanges = errors.New("meta: bad shard map ranges")
	// ErrTokenInFlight reports a Txn handed a router under the token of one still in flight.
	ErrTokenInFlight = errors.New("meta: token in flight")
	// ErrGroupGone reports a Txn held for an election whose group left the shard map; it was never proposed, so it may be routed again.
	ErrGroupGone = errors.New("meta: txn's group left the shard map")
)

// CrossShard is the refusal of a Txn or Scan whose keys span Groups, sorted.
type CrossShard struct {
	Groups []uint64
}

func (e *CrossShard) Error() string { return fmt.Sprintf("meta: keys span Raft groups %v", e.Groups) }

func (e *CrossShard) Unwrap() error { return ErrCrossShard }

// NotLeader is a follower's or candidate's answer to a Txn, which it proposes nowhere: its group,
// its term, and the leader it heard in that term, by replica ID and node, or Leader 0 for none.
type NotLeader struct {
	Group  uint64
	Leader uint64
	Node   uint32
	Term   uint64
}

func (e NotLeader) Error() string {
	if e.Leader == 0 {
		return fmt.Sprintf("meta: not the leader of group %d in term %d; no leader is known", e.Group, e.Term)
	}
	return fmt.Sprintf("meta: not the leader of group %d in term %d; replica %d on node %d leads", e.Group, e.Term, e.Leader, e.Node)
}

// ctlEnd is PrefixEnd(ctl/), where the control-plane group's range ends.
var ctlEnd = []byte{SpaceCtl + 1}

// CheckRanges reports whether a router can route by m: the control-plane group first, from the
// empty key to PrefixEnd(ctl/), each later group from a Start above the one before, and each
// group one range. A split inside the control-plane group's range fails it.
func (m *ShardMap) CheckRanges() error {
	g := m.Groups
	if len(g) < 2 || len(g[0].Start) != 0 || !bytes.Equal(g[1].Start, ctlEnd) {
		return ErrRanges
	}
	seen := map[uint64]bool{g[0].ID: true}
	for i := 1; i < len(g); i++ {
		if seen[g[i].ID] || bytes.Compare(g[i-1].Start, g[i].Start) >= 0 {
			return ErrRanges
		}
		seen[g[i].ID] = true
	}
	return nil
}

// Route is the ID of the one group whose range holds every key t reads or writes, its replay key
// included, or t.Check's error, or a *CrossShard. m must pass CheckRanges.
func (m *ShardMap) Route(t *Txn) (uint64, error) {
	if _, err := t.Check(); err != nil {
		return 0, err
	}
	g, cross := -1, false
	if t.Replay != nil {
		g = m.group(t.Replay)
	}
	for i := range t.Ops {
		op := &t.Ops[i]
		k := m.group(op.Key)
		cross = cross || g >= 0 && k != g || op.End != nil && m.spans(k, op.End)
		g = k
	}
	if cross {
		return 0, m.crossShard(t)
	}
	return m.Groups[g].ID, nil
}

// RouteScan is the ID of the group whose range holds [start, end), a nil end the rest of the key
// space, or a *CrossShard, since one point-in-time view covers one group.
func (m *ShardMap) RouteScan(start, end []byte) (uint64, error) {
	k := m.group(start)
	if m.spans(k, end) {
		ids := m.cover(nil, start, end)
		slices.Sort(ids)
		return 0, &CrossShard{Groups: ids}
	}
	return m.Groups[k].ID, nil
}

// cover appends to ids the IDs of the groups whose ranges [start, end) reaches, a nil end the rest of the key space.
func (m *ShardMap) cover(ids []uint64, start, end []byte) []uint64 {
	for k := m.group(start); ; k++ {
		ids = append(ids, m.Groups[k].ID)
		if !m.spans(k, end) {
			return ids
		}
	}
}

// group is the index of the group whose range holds key.
func (m *ShardMap) group(key []byte) int {
	return sort.Search(len(m.Groups), func(i int) bool { return bytes.Compare(m.Groups[i].Start, key) > 0 }) - 1
}

// spans reports whether a range starting in group k and ending at end, nil for none, reaches the next group.
func (m *ShardMap) spans(k int, end []byte) bool {
	return k+1 < len(m.Groups) && (end == nil || bytes.Compare(m.Groups[k+1].Start, end) < 0)
}

func (m *ShardMap) crossShard(t *Txn) error {
	var ids []uint64
	if t.Replay != nil {
		ids = append(ids, m.Groups[m.group(t.Replay)].ID)
	}
	for i := range t.Ops {
		if op := &t.Ops[i]; op.End != nil {
			ids = m.cover(ids, op.Key, op.End)
		} else {
			ids = append(ids, m.Groups[m.group(op.Key)].ID)
		}
	}
	slices.Sort(ids)
	return &CrossShard{Groups: slices.Compact(ids)}
}

// writesReplayTagged reports whether t puts, deletes or updates a replay-tagged key or removes a range holding one.
func writesReplayTagged(t *Txn) bool {
	for i := range t.Ops {
		switch op := &t.Ops[i]; op.Kind {
		case OpPut, OpDelete, OpUpdate:
			if replayTagged(op.Key) {
				return true
			}
		case OpDeleteRange:
			if replayIn(op.Key, op.End) {
				return true
			}
		}
	}
	return false
}

// replayPrefix is the length of the prefix a replay-tagged key starts with: fs/, a volume, a slot at sequence 0, and the replay tag.
const replayPrefix = 14

// replayTagged reports whether k is an fs/ key at sequence 0 under the replay tag, whether or not the rest of it decodes.
func replayTagged(k []byte) bool {
	return len(k) >= replayPrefix && k[0] == SpaceFS && k[8]|k[9]|k[10]|k[11]|k[12] == 0 && k[13] == SlotReplay
}

// replayIn reports whether [start, end) holds a replay-tagged key: start is one, or the least one above it is below end.
func replayIn(start, end []byte) bool {
	if replayTagged(start) {
		return true
	}
	var p [replayPrefix]byte
	switch {
	case len(start) == 0 || start[0] < SpaceFS:
	case start[0] > SpaceFS:
		return false
	default:
		copy(p[:], start)
		// p[1:8] is the volume and slot; the least replay-tagged key above start is that slot's
		// prefix if start sorts before it, or else the next slot's.
		if p[8]|p[9]|p[10]|p[11]|p[12] != 0 || p[13] > SlotReplay {
			i := 7
			for ; i > 0 && p[i] == 0xff; i-- {
				p[i] = 0
			}
			if i == 0 {
				return false
			}
			p[i]++
		}
	}
	p[0], p[8], p[9], p[10], p[11], p[12], p[13] = SpaceFS, 0, 0, 0, 0, 0, SlotReplay
	return bytes.Compare(p[:], end) < 0
}

// electionWait is how long a router holds a Txn whose group has no known leader before it tries a voter again.
const electionWait = raftshard.ElectionTick * raftshard.TickInterval

// Send is a Txn for its driver to send to Node, which hosts a replica of Group, under Token.
type Send struct {
	Token uint64
	Group uint64
	Node  uint32
	Txn   *Txn
}

// RouterOutput is what one call into a Router hands its driver: the Txns to send, the answers
// final for their callers, and its timer.
type RouterOutput struct {
	Sends   []Send
	Answers []Answer
	Actions []core.Action
}

// Reset empties the output for reuse.
func (o *RouterOutput) Reset() {
	o.Sends, o.Answers, o.Actions = o.Sends[:0], o.Answers[:0], o.Actions[:0]
}

// Router is a daemon's side of the sharded API: it routes each Txn through the shard map to its
// group's leader from its route cache, learns the leader from a NotLeader answer and resends,
// and, when no leader is known, tries the group's voters in turn, one per election timeout.
type Router struct {
	m     ShardMap
	timer core.TimerID
	armed bool
	// routes is the route cache, and cached each group's index in it.
	routes  []route
	cached  map[uint64]int
	flights []flight
	at      map[uint64]int
}

// route is the cache's entry for a group: the newest term heard, the leader's node if known in
// it, and how many guesses at a voter it took.
type route struct {
	group   uint64
	term    uint64
	node    uint32
	known   bool
	guesses int
}

// flight is a Txn sent and not yet answered, or parked until the timer.
type flight struct {
	token  uint64
	group  uint64
	node   uint32
	txn    *Txn
	parked bool
}

// NewRouter is a router over m, arming timer while it holds Txns for an election.
func NewRouter(m *ShardMap, timer core.TimerID) (*Router, error) {
	r := &Router{timer: timer, cached: map[uint64]int{}, at: map[uint64]int{}}
	return r, r.SetMap(m, nil)
}

// SetMap routes later Txns by a copy of m, keeping the route cache, and fails each Txn held for
// an election whose group m no longer holds with ErrGroupGone; none of them was proposed.
func (r *Router) SetMap(m *ShardMap, out *RouterOutput) error {
	if err := m.CheckRanges(); err != nil {
		return err
	}
	r.m.Groups = slices.Clone(m.Groups)
	for i := range r.m.Groups {
		g := &r.m.Groups[i]
		g.Start, g.Voters = slices.Clone(g.Start), slices.Clone(g.Voters)
	}
	for i := len(r.flights) - 1; i >= 0; i-- {
		if f := &r.flights[i]; f.parked && r.m.index(f.group) < 0 {
			r.finish(i, &Answer{Token: f.token, Result: Result{FailedCheck: -1}, Err: ErrGroupGone}, out)
		}
	}
	return nil
}

// index is the index of group id in m, or -1.
func (m *ShardMap) index(id uint64) int {
	return slices.IndexFunc(m.Groups, func(g Group) bool { return g.ID == id })
}

// route is group's entry in the route cache, added if missing; it stays valid until the next one is added.
func (r *Router) route(group uint64) *route {
	k, ok := r.cached[group]
	if !ok {
		k = len(r.routes)
		r.cached[group] = k
		r.routes = append(r.routes, route{group: group})
	}
	return &r.routes[k]
}

// Txn sends t, which stays the caller's and unchanged until an Answer under token settles it, to
// its group's leader, or refuses it with Route's error before sending anything.
func (r *Router) Txn(ctx *core.Context, t *Txn, token uint64, out *RouterOutput) error {
	if _, ok := r.at[token]; ok {
		return ErrTokenInFlight
	}
	g, err := r.m.Route(t)
	if err != nil {
		return err
	}
	r.at[token] = len(r.flights)
	r.flights = append(r.flights, flight{token: token, group: g, node: noNode, txn: t})
	r.send(ctx, len(r.flights)-1, out)
	return nil
}

// Answer takes a node's answer to a Txn sent: a NotLeader updates the route cache of the Txn's
// group and resends the Txn, which no replica proposed, and any other answer is final. An answer
// to no Txn in flight, or to one held for an election, is dropped.
func (r *Router) Answer(ctx *core.Context, a *Answer, out *RouterOutput) {
	i, ok := r.at[a.Token]
	if !ok || r.flights[i].parked {
		return
	}
	nl, ok := a.Err.(NotLeader)
	if !ok {
		r.finish(i, a, out)
		return
	}
	f := &r.flights[i]
	c := r.route(f.group)
	// The cached leader saying it does not lead is newer than the cache, even within its term.
	if c.known && c.node == f.node {
		c.known = false
	}
	if nl.Term > c.term || nl.Term == c.term && !c.known {
		c.term, c.node, c.known = nl.Term, nl.Node, nl.Leader != 0
	}
	r.send(ctx, i, out)
}

// OnEvent handles the router's timer, which resends the Txns held for an election, and a lost
// peer, which fails the Txns sent to it as dropped, since each may or may not have applied, and
// is forgotten as the leader of every group.
func (r *Router) OnEvent(ctx *core.Context, ev *core.Event, out *RouterOutput) {
	switch {
	case ev.Kind == core.EvTimer && ev.Timer == r.timer:
		r.armed = false
		for i := range r.flights {
			if r.flights[i].parked {
				r.flights[i].parked = false
				r.send(ctx, i, out)
			}
		}
	case ev.Kind == core.EvPeerLost:
		for i := range r.routes {
			if c := &r.routes[i]; c.known && c.node == ev.Peer.Node {
				c.known = false
			}
		}
		for i := len(r.flights) - 1; i >= 0; i-- {
			if f := &r.flights[i]; !f.parked && f.node == ev.Peer.Node {
				r.finish(i, &Answer{Token: f.token, Result: Result{FailedCheck: -1}, Err: raft.ErrProposalDropped}, out)
			}
		}
	}
}

// send sends flight i to its group's cached leader, or to its next voter in turn when none is
// known and the Txn has not yet been tried on a guess since the last election wait, or parks it.
func (r *Router) send(ctx *core.Context, i int, out *RouterOutput) {
	f := &r.flights[i]
	c := r.route(f.group)
	switch {
	case c.known:
		f.node = c.node
	case f.node == noNode:
		var voters []Replica
		if k := r.m.index(f.group); k >= 0 {
			voters = r.m.Groups[k].Voters
		}
		if len(voters) == 0 {
			r.park(ctx, f, out)
			return
		}
		f.node = voters[c.guesses%len(voters)].Node
		c.guesses++
	default:
		r.park(ctx, f, out)
		return
	}
	out.Sends = append(out.Sends, Send{Token: f.token, Group: f.group, Node: f.node, Txn: f.txn})
}

func (r *Router) park(ctx *core.Context, f *flight, out *RouterOutput) {
	f.parked, f.node = true, noNode
	if !r.armed {
		r.armed = true
		out.Actions = append(out.Actions, core.Action{Kind: core.ActSetTimer, Timer: r.timer, At: ctx.Now + electionWait})
	}
}

// noNode marks a flight not yet sent since it was routed or parked.
const noNode = ^uint32(0)

// finish hands a's copy out as flight i's final answer and forgets the flight.
func (r *Router) finish(i int, a *Answer, out *RouterOutput) {
	out.Answers = append(out.Answers, *a)
	delete(r.at, r.flights[i].token)
	last := len(r.flights) - 1
	if i != last {
		r.flights[i] = r.flights[last]
		r.at[r.flights[i].token] = i
	}
	r.flights = r.flights[:last]
}
