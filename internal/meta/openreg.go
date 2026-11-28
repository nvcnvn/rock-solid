package meta

import (
	"bytes"
	"errors"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"go.etcd.io/raft/v3"
)

// The open registry is the inode owner's of docs/design.md's "Inode owner" and models/purge's
// Open, Close, FreeOrphan, FenceAck, OwnerFailover, Reregister and EndGrace: a Server keeps it for
// the groups whose leader it serves.

// ErrFenced answers an open from a session the owner's session table shows not live: no open is
// registered, so the mount must not hand out a handle.
var ErrFenced = errors.New("meta: the session is fenced")

// ErrGrace answers an open a new owner holds in its grace period, which the router sends again.
var ErrGrace = errors.New("meta: the owner is in its grace period")

// inodeAt names an inode of a volume in a group.
type inodeAt struct {
	group  uint64
	volume uint32
	inode  uint64
}

// opener is a mount that has a file open.
type opener struct {
	session uint64
	mount   uint32
}

// openKey is a mount's open of an inode.
type openKey struct {
	at  inodeAt
	who opener
}

// openRow is an open, and the version of the first table that showed its session not live, which
// its fence is at or below, or 0.
type openRow struct {
	openKey
	fenced uint64
}

// registry is who has each file open: a row per inode and mount, in no order, each row's index by
// its key and the rows of each inode and of each group; the orphans to look at once their last
// opener left, and the set of them; the free Txns proposed, by token; the newest session table;
// the groups this replica owns, the asks for a table they made, and the sessions to evict.
type registry struct {
	rows    []openRow
	index   map[openKey]int
	opens   map[inodeAt]int
	groups  map[uint64]int
	frees   []inodeAt
	listed  map[inodeAt]struct{}
	spare   []inodeAt
	freeing map[uint64]inodeAt
	sess    chunkstore.SessionTable
	known   bool
	owners  []ownership
	asks    uint64
	evicts  []uint64
	fs      FS
	key     []byte
	scan    ScanResult
	plant   regPlants
}

// ownership is this replica's ownership of a group's inodes: the term it leads in, whether its
// grace period runs, the session table ask whose answer lists the sessions it waits on, 0 once
// taken, those sessions, the version of the table it took them from, which fenced the sessions
// not live in it, those whose re-registration ended before it took them, when it took them,
// whether it has, when it last asked them to re-register, and whether it asked to evict them.
type ownership struct {
	group, term   uint64
	grace         bool
	ask           uint64
	wait          []waiting
	fenced        uint64
	early         []uint64
	since, asked  core.Tick
	timed, evicts bool
}

// waiting is a session a new owner waits on to re-register: its ID, its node, and the version of
// the first table that showed it not live, or 0.
type waiting struct {
	id     uint64
	node   uint32
	fenced uint64
}

// ReclaimGap is how often a new owner asks the sessions it waits on to re-register.
const ReclaimGap = core.Tick(100_000)

// regPlants are bugs a test plants: an orphan freed while open, as an unlink freeing an open file
// at once; a fenced session's opens closed once its fence commits rather than once it is
// acknowledged; a grace period that ends on time alone; a new owner that takes new opens in its
// grace period; and one that frees orphans in it.
type regPlants struct {
	unlinkOpen, releaseOnCommit, graceByTime, openInGrace, orphanInGrace bool
}

func newRegistry() registry {
	return registry{index: map[openKey]int{}, opens: map[inodeAt]int{}, groups: map[uint64]int{}, listed: map[inodeAt]struct{}{}, freeing: map[uint64]inodeAt{}}
}

// open reports whether any mount has at open.
func (g *registry) open(at inodeAt) bool { return g.opens[at] > 0 }

func (g *registry) insert(r openRow) {
	g.index[r.openKey] = len(g.rows)
	g.rows = append(g.rows, r)
	g.opens[r.at]++
	g.groups[r.at.group]++
}

// remove deletes row i, moving the last row into its place.
func (g *registry) remove(i int) {
	k := g.rows[i].openKey
	last := len(g.rows) - 1
	g.rows[i] = g.rows[last]
	g.index[g.rows[i].openKey] = i
	g.rows = g.rows[:last]
	delete(g.index, k)
	g.closed(k.at)
}

// closed counts one fewer open of at.
func (g *registry) closed(at inodeAt) {
	if g.opens[at]--; g.opens[at] <= 0 {
		delete(g.opens, at)
	}
	if g.groups[at.group]--; g.groups[at.group] <= 0 {
		delete(g.groups, at.group)
	}
}

// retain keeps the rows keep reports true for and closes the others, looking at an inode it
// leaves no open of when look is set.
func (g *registry) retain(keep func(*openRow) bool, look bool) {
	j := 0
	for _, r := range g.rows {
		if !keep(&r) {
			delete(g.index, r.openKey)
			g.closed(r.at)
			if look && !g.open(r.at) {
				g.look(r.at)
			}
			continue
		}
		g.rows[j] = r
		g.index[r.openKey] = j
		j++
	}
	g.rows = g.rows[:j]
}

// holds reports whether session id may register an open: before any table, while live, and for
// an ID the table has not handed out.
func (g *registry) holds(id uint64) bool {
	_, live := slices.BinarySearch(g.sess.Live, id)
	return !g.known || live || id >= g.sess.Next
}

func (g *registry) look(at inodeAt) {
	if _, ok := g.listed[at]; !ok {
		g.listed[at] = struct{}{}
		g.frees = append(g.frees, at)
	}
}

// own returns s's ownership of its group while s leads it, and nil otherwise. A replica that does
// not lead drops the group's opens, which the next leader serves; one that starts to lead in a
// term drops those of an older one, asks for a session table, and starts its grace period.
func (g *registry) own(s *Shard) *ownership {
	i := slices.IndexFunc(g.owners, func(o ownership) bool { return o.group == s.group })
	leads, term := s.raft.Leader() == s.id, s.raft.Term()
	if i >= 0 && leads && g.owners[i].term == term {
		return &g.owners[i]
	}
	if g.groups[s.group] > 0 {
		g.retain(func(r *openRow) bool { return r.at.group != s.group }, false)
	}
	if !leads {
		if i >= 0 {
			g.owners = slices.Delete(g.owners, i, i+1)
		}
		return nil
	}
	if i < 0 {
		i = len(g.owners)
		g.owners = append(g.owners, ownership{})
	}
	g.asks++
	g.owners[i] = ownership{group: s.group, term: term, grace: true, ask: g.asks, wait: g.owners[i].wait[:0], early: g.owners[i].early[:0]}
	if g.plant.orphanInGrace {
		g.scanOrphans(s)
	}
	return &g.owners[i]
}

// reclaimed counts session id's re-registration with o as ended.
func (o *ownership) reclaimed(id uint64) {
	if o.ask != 0 {
		o.early = append(o.early, id)
		return
	}
	o.wait = slices.DeleteFunc(o.wait, func(w waiting) bool { return w.id == id })
}

// list takes the sessions o waits on from t, a table read after o's ask: its live sessions, and
// until t's version is acknowledged, those it shows fenced.
func (o *ownership) list(t *chunkstore.SessionTable) {
	o.fenced = t.Version
	for i, id := range t.Live {
		if !slices.Contains(o.early, id) {
			o.wait = append(o.wait, waiting{id: id, node: t.Nodes[i]})
		}
	}
	o.ask, o.early = 0, o.early[:0]
}

// progress runs o's grace period on s: it ends once every session o waits on has re-registered or
// left the table, when the orphan list is scanned; until then o asks each to re-register every
// ReclaimGap, and asks the control plane once to evict those still waited on after lease.
func (g *registry) progress(ctx *core.Context, s *Shard, o *ownership, lease core.Tick, acts []core.Action) []core.Action {
	if !o.grace || o.ask != 0 {
		return acts
	}
	if !o.timed {
		o.since, o.timed = ctx.Now, true
	}
	if len(o.wait) == 0 && g.sess.Acked >= o.fenced || g.plant.graceByTime && ctx.Now-o.since >= 2*ReclaimGap {
		o.grace = false
		g.scanOrphans(s)
		return acts
	}
	if !o.evicts && ctx.Now-o.since >= lease {
		o.evicts = true
		for _, w := range o.wait {
			g.evicts = append(g.evicts, w.id)
		}
	}
	if o.asked != 0 && ctx.Now-o.asked < ReclaimGap {
		return acts
	}
	o.asked = ctx.Now
	for _, w := range o.wait {
		a := core.Action{Kind: core.ActNetSend, Peer: core.NodeID{Node: w.node}}
		if EncodeReclaim(ctx.Pool, o.group, w.id, o.term, &a.Msg) == nil {
			acts = append(acts, a)
		}
	}
	return acts
}

// scanOrphans looks at every orphan s's state lists.
// ponytail: one pass over all the group's keys per failover; seek from slot to slot if shards
// grow large enough for it to stall the loop.
func (g *registry) scanOrphans(s *Shard) {
	var k Key
	var from []byte
	for {
		if s.Scan(from, nil, MaxScanBytes, &g.scan) != nil {
			return
		}
		for _, kv := range g.scan.KVs {
			if DecodeKey(kv.Key, &k) == nil && k.Space == SpaceFS && k.Tag == SlotOrphan {
				g.look(inodeAt{s.group, k.Volume, k.ID})
			}
		}
		if !g.scan.More || len(g.scan.KVs) == 0 {
			return
		}
		from = append(append(from[:0], g.scan.KVs[len(g.scan.KVs)-1].Key...), 0)
	}
}

// register serves rd, an open, a release or a re-registration of the file whose attributes Start
// names, to which kvs, from the leader's state, answer, at o, s's ownership of group, nil when s
// does not lead it. An open from a session that may not register is refused ErrFenced, and one in
// o's grace period ErrGrace; otherwise it registers its mount while the file has a link. A
// re-registration registers it whatever the links, and a release deregisters it, leaving an
// orphan no mount has open to be freed. The end of a session's re-registration counts it as
// re-registered in o's grace period, if it is the one of rd's term.
func (g *registry) register(group uint64, rd *Read, kvs []KV, o *ownership) error {
	if rd.Reg != RegRelease && !g.holds(rd.Session) {
		return ErrFenced
	}
	if rd.Reg == RegReclaimed {
		if o != nil && o.grace && o.term == rd.Term {
			o.reclaimed(rd.Session)
		}
		return nil
	}
	var k Key
	if DecodeKey(rd.Start, &k) != nil || k.Space != SpaceFS || k.Tag != InodeAttr {
		return ErrMalformed
	}
	var a Attr
	found := len(kvs) > 0 && bytes.Equal(kvs[0].Key, rd.Start) && DecodeAttr(kvs[0].Value, &a) == nil
	key := openKey{inodeAt{group, k.Volume, k.Inode}, opener{rd.Session, rd.Mount}}
	i, there := g.index[key]
	switch {
	case rd.Reg == RegOpen && o != nil && o.grace && !g.plant.openInGrace:
		return ErrGrace
	case o == nil || there:
	case rd.Reg == RegOpen && found && a.Nlink > 0, rd.Reg == RegReopen && found:
		g.insert(openRow{openKey: key})
	}
	if rd.Reg == RegRelease && there {
		g.remove(i)
		if found && a.Nlink == 0 && !g.open(key.at) {
			g.look(key.at)
		}
	}
	return nil
}

// orphans returns the inode t makes an orphan, if any.
func orphans(group uint64, t *Txn) (inodeAt, bool) {
	var k Key
	for i := range t.Ops {
		if op := &t.Ops[i]; op.Kind == OpPut && DecodeKey(op.Key, &k) == nil && k.Space == SpaceFS && k.Tag == SlotOrphan {
			return inodeAt{group, k.Volume, k.ID}, true
		}
	}
	return inodeAt{}, false
}

// answered takes the answer to a free Txn the registry proposed under token, and looks again at
// an orphan whose free did not apply.
func (g *registry) answered(a *Answer) bool {
	at, ok := g.freeing[a.Token]
	if !ok {
		return false
	}
	delete(g.freeing, a.Token)
	if a.Err != nil || a.Result.FailedCheck >= 0 {
		g.look(at)
	}
	return true
}

// SetSessions takes t, the committed session table as storage nodes hold it, if no older than the
// last, read after the FetchSessions answers names, or 0 for a table sent unasked: the opens of a
// session no longer live close once t acknowledges a version at or above the one that first showed
// it so, which frees each orphan it was the last to have open, and a grace period stops waiting on
// such a session; one whose ask t answers takes the sessions it waits on.
func (sv *Server) SetSessions(t *chunkstore.SessionTable, answers uint64) {
	g := &sv.reg
	if !g.known || t.Version > g.sess.Version || t.Version == g.sess.Version && t.Acked >= g.sess.Acked {
		g.sess.Version, g.sess.Acked, g.sess.Next = t.Version, t.Acked, t.Next
		g.sess.Live = append(g.sess.Live[:0], t.Live...)
		g.sess.Nodes = append(g.sess.Nodes[:0], t.Nodes...)
		for len(g.sess.Nodes) < len(g.sess.Live) {
			g.sess.Nodes = append(g.sess.Nodes, 0)
		}
		g.known = true
	}
	t = &g.sess
	g.retain(func(r *openRow) bool {
		switch {
		case g.holds(r.who.session):
		case r.fenced == 0:
			r.fenced = t.Version
		}
		return r.fenced == 0 || r.fenced > t.Acked && !g.plant.releaseOnCommit
	}, true)
	for i := range g.owners {
		o := &g.owners[i]
		if o.ask != 0 && answers >= o.ask {
			o.list(t)
		}
		j := 0
		for _, w := range o.wait {
			if w.fenced == 0 && !g.holds(w.id) {
				w.fenced = t.Version
			}
			if w.fenced == 0 || w.fenced > t.Acked {
				o.wait[j] = w
				j++
			}
		}
		o.wait = o.wait[:j]
	}
}

// FetchSessions is the newest ask for a session table not yet answered, or 0: its host fetches one
// from the control plane and hands it to SetSessions as answering every ask made before the fetch
// went out.
func (sv *Server) FetchSessions() uint64 {
	for _, o := range sv.reg.owners {
		if o.ask != 0 {
			return sv.reg.asks
		}
	}
	return 0
}

// Evictions appends to dst, and forgets, the sessions a grace period waited on for a lease, for
// its host to ask the control plane to evict.
func (sv *Server) Evictions(dst []uint64) []uint64 {
	dst = append(dst, sv.reg.evicts...)
	sv.reg.evicts = sv.reg.evicts[:0]
	return dst
}

// Free proposes on s, as Serve does into out, a Txn per orphan of its group that no mount has
// open, moving it to the purge list, and runs s's grace period, appending to acts its asks to
// re-register. Its host calls it after each call into s or the server: a frees may be left by a
// release served, an answer replied or a session table taken, and a grace period by any event.
func (sv *Server) Free(ctx *core.Context, s *Shard, out *Output, acts []core.Action) []core.Action {
	return sv.free(ctx, s, acts, func(t *Txn, token uint64) error { return s.submit(ctx, t, token, out) })
}

// FreeRound is Free for a, a replica on the round driver, or nil when the node hosts none; one
// not started yet frees nothing.
func (sv *Server) FreeRound(ctx *core.Context, a *RoundShard, acts []core.Action) []core.Action {
	if a == nil || a.Err() != nil || a.Shard() == nil {
		return acts
	}
	var out []core.Action
	acts = sv.free(ctx, a.Shard(), acts, func(t *Txn, token uint64) (err error) {
		out, err = a.submit(ctx, t, token, out)
		return err
	})
	return append(acts, out...)
}

// free runs s's ownership and proposes the frees of s's group, keeping to look at again an orphan
// whose proposal was refused for a reason that may pass. A replica that does not own the group, or
// owns it in its grace period, frees none, as its registry may miss the opens an earlier owner
// registered: the orphans stay listed for the scan at the grace period's end.
func (sv *Server) free(ctx *core.Context, s *Shard, acts []core.Action, submit func(*Txn, uint64) error) []core.Action {
	g := &sv.reg
	o := g.own(s)
	if o != nil {
		acts = g.progress(ctx, s, o, sv.Lease, acts)
	}
	if len(g.frees) == 0 {
		return acts
	}
	todo := g.frees
	g.frees = g.spare[:0]
	owns := o != nil && (!o.grace || g.plant.orphanInGrace)
	for _, at := range todo {
		if at.group != s.group {
			g.frees = append(g.frees, at)
			continue
		}
		delete(g.listed, at)
		if !owns || !g.freeTxn(s, at) {
			continue
		}
		sv.next++
		token := ServerToken | sv.next
		if err := submit(&g.fs.Txn, token); err != nil {
			if passing(err) {
				g.look(at)
			}
			continue
		}
		g.freeing[token] = at
	}
	g.spare = todo[:0]
	return acts
}

// passing reports a refusal of a proposal that may pass: an overloaded replica, or a leader change.
func passing(err error) bool {
	var nl NotLeader
	return errors.Is(err, ErrOverloaded) || errors.Is(err, raft.ErrProposalDropped) || errors.As(err, &nl)
}

// freeTxn builds in g.fs the Txn that frees at, from s's state, unless a mount has it open or it
// is no orphan.
func (g *registry) freeTxn(s *Shard, at inodeAt) bool {
	if g.open(at) && !g.plant.unlinkOpen {
		return false
	}
	var a Attr
	g.key, _ = AppendKey(g.key[:0], &Key{Space: SpaceFS, Volume: at.volume, Inode: at.inode, Tag: InodeAttr})
	if v, ok := g.get(s); !ok || DecodeAttr(v, &a) != nil || a.Nlink != 0 {
		return false
	}
	g.key, _ = AppendKey(g.key[:0], &Key{Space: SpaceFS, Volume: at.volume, Inode: at.inode >> SeqBits << SeqBits, Tag: SlotPurge, ID: at.inode})
	var pending *Purge
	v, ok := g.get(s)
	p, err := DecodePurge(v)
	if ok && err != nil {
		return false
	}
	if ok {
		pending = &p
	}
	g.fs.Volume, g.fs.Req = at.volume, nil
	return g.fs.Free(at.inode, &a, pending) == nil
}

// get reads the key in g.key from s's state.
func (g *registry) get(s *Shard) ([]byte, bool) {
	n := len(g.key)
	g.key = append(append(g.key, g.key...), 0)
	if s.Scan(g.key[:n:n], g.key[n:], 1, &g.scan) != nil || len(g.scan.KVs) == 0 || !bytes.Equal(g.scan.KVs[0].Key, g.key[:n]) {
		return nil, false
	}
	return g.scan.KVs[0].Value, true
}

// EncodeReclaim writes a new owner's ask to session that it re-register its opens of group, and
// end with a RegReclaimed Read of term, into msg.
func EncodeReclaim(pool core.BufPool, group, session, term uint64, msg *core.Msg) error {
	return encodeBody(pool, group, KindReclaim, session, le.AppendUint64(nil, term), msg)
}

// DecodeReclaim decodes a KindReclaim message into its group, session and term, or ErrMalformed.
func DecodeReclaim(pool core.BufPool, msg *core.Msg) (group, session, term uint64, err error) {
	b, err := msgBody(pool, KindReclaim, msg)
	if err != nil || len(b) != 8 {
		return 0, 0, 0, ErrMalformed
	}
	group, _, session, _ = Peek(msg)
	return group, session, le.Uint64(b), nil
}
