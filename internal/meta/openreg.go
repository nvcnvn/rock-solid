package meta

import (
	"bytes"
	"errors"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
)

// The open registry is the inode owner's of docs/design.md's "Inode owner" and models/purge's
// Open, Close, FreeOrphan and FenceAck: a Server keeps it for the groups whose leader it serves.

// ErrFenced answers an open from a session the owner's session table shows not live: no open is
// registered, so the mount must not hand out a handle.
var ErrFenced = errors.New("meta: the session is fenced")

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
// its key and the rows of each inode; the orphans to look at once their last opener left; the free
// Txns proposed, by token; and the newest session table.
type registry struct {
	rows    []openRow
	index   map[openKey]int
	opens   map[inodeAt]int
	frees   []inodeAt
	spare   []inodeAt
	freeing map[uint64]inodeAt
	sess    chunkstore.SessionTable
	known   bool
	fs      FS
	key     []byte
	scan    ScanResult
	plant   regPlants
}

// regPlants are bugs a test plants: an orphan freed while open, as an unlink freeing an open file
// at once; a fenced session's opens closed once its fence commits rather than once it is
// acknowledged; and a new owner that frees orphans, its registry missing the opens its
// predecessors registered.
type regPlants struct {
	unlinkOpen, releaseOnCommit, newOwnerFrees bool
}

func newRegistry() registry {
	return registry{index: map[openKey]int{}, opens: map[inodeAt]int{}, freeing: map[uint64]inodeAt{}}
}

// open reports whether any mount has at open.
func (g *registry) open(at inodeAt) bool { return g.opens[at] > 0 }

func (g *registry) insert(r openRow) {
	g.index[r.openKey] = len(g.rows)
	g.rows = append(g.rows, r)
	g.opens[r.at]++
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
}

// holds reports whether session id may register an open: before any table, while live, and for
// an ID the table has not handed out.
func (g *registry) holds(id uint64) bool {
	_, live := slices.BinarySearch(g.sess.Live, id)
	return !g.known || live || id >= g.sess.Next
}

func (g *registry) look(at inodeAt) {
	if !slices.Contains(g.frees, at) {
		g.frees = append(g.frees, at)
	}
}

// register serves rd, an open or a release of the file whose attributes Start names, to which
// kvs, from the leader's state, answer: an open registers its mount while the file has a link,
// or is refused ErrFenced from a session that may not register, and a release deregisters it,
// leaving an orphan no mount has open to be freed.
func (g *registry) register(group uint64, rd *Read, kvs []KV) error {
	var k Key
	if DecodeKey(rd.Start, &k) != nil || k.Space != SpaceFS || k.Tag != InodeAttr {
		return ErrMalformed
	}
	var a Attr
	found := len(kvs) > 0 && bytes.Equal(kvs[0].Key, rd.Start) && DecodeAttr(kvs[0].Value, &a) == nil
	key := openKey{inodeAt{group, k.Volume, k.Inode}, opener{rd.Session, rd.Mount}}
	i, there := g.index[key]
	switch {
	case rd.Reg == RegOpen && !g.holds(rd.Session):
		return ErrFenced
	case rd.Reg == RegOpen && !there && found && a.Nlink > 0:
		g.insert(openRow{openKey: key})
	case rd.Reg == RegRelease && there:
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
// last: the opens of a session no longer live close once t acknowledges a version at or above the
// one that first showed it so, which frees each orphan it was the last to have open.
func (sv *Server) SetSessions(t *chunkstore.SessionTable) {
	g := &sv.reg
	if g.known && (t.Version < g.sess.Version || t.Version == g.sess.Version && t.Acked < g.sess.Acked) {
		return
	}
	g.sess.Version, g.sess.Acked, g.sess.Next = t.Version, t.Acked, t.Next
	g.sess.Live = append(g.sess.Live[:0], t.Live...)
	g.known = true
	j := 0
	for _, r := range g.rows {
		switch {
		case g.holds(r.who.session):
		case r.fenced == 0:
			r.fenced = t.Version
		}
		if r.fenced != 0 && (r.fenced <= t.Acked || g.plant.releaseOnCommit) {
			delete(g.index, r.openKey)
			g.closed(r.at)
			g.look(r.at)
			continue
		}
		g.rows[j] = r
		g.index[r.openKey] = j
		j++
	}
	g.rows = g.rows[:j]
}

// Free proposes on s, as Serve does into out, a Txn per orphan of its group that no mount has
// open, moving it to the purge list. Its host calls it after each call that may have left one: a
// release served, an answer replied or a session table taken.
func (sv *Server) Free(ctx *core.Context, s *Shard, out *Output) {
	sv.free(s, func(t *Txn, token uint64) error { return s.submit(ctx, t, token, out) })
}

// FreeRound is Free for a, a replica on the round driver, or nil when the node hosts none.
func (sv *Server) FreeRound(ctx *core.Context, a *RoundShard, acts []core.Action) []core.Action {
	if a == nil || a.Err() != nil || len(sv.reg.frees) == 0 {
		return acts
	}
	sv.free(a.Shard(), func(t *Txn, token uint64) (err error) {
		acts, err = a.submit(ctx, t, token, acts)
		return err
	})
	return acts
}

// free proposes the frees of s's group, keeping to look at again an orphan whose proposal was
// refused. A replica that is not its group's sole leader since boot frees none, as its registry
// may miss the opens an earlier leader registered, and drops them: the orphans stay listed until
// Owner failover and grace period re-registers the opens and scans the orphan list.
func (sv *Server) free(s *Shard, submit func(*Txn, uint64) error) {
	g := &sv.reg
	todo := g.frees
	g.frees = g.spare[:0]
	sole := s.sole() || g.plant.newOwnerFrees
	for _, at := range todo {
		if at.group != s.group {
			g.frees = append(g.frees, at)
			continue
		}
		if !sole || !g.freeTxn(s, at) {
			continue
		}
		sv.next++
		token := ServerToken | sv.next
		if submit(&g.fs.Txn, token) != nil {
			g.frees = append(g.frees, at)
			continue
		}
		g.freeing[token] = at
	}
	g.spare = todo[:0]
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
