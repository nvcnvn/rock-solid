package meta

import (
	"bytes"
	"cmp"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// The open registry is the inode owner's of docs/design.md's "Inode owner" and models/purge's
// Open, Close, FreeOrphan and FenceAck: a Server keeps it for the groups whose leader it serves.

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

type openRow struct {
	at  inodeAt
	who opener
}

func cmpRow(a, b openRow) int {
	return cmp.Or(cmp.Compare(a.at.group, b.at.group), cmp.Compare(a.at.volume, b.at.volume), cmp.Compare(a.at.inode, b.at.inode),
		cmp.Compare(a.who.session, b.who.session), cmp.Compare(a.who.mount, b.who.mount))
}

// registry is who has each file open, a row per inode and mount in order; the orphans to look at
// once their last opener left; the free Txns proposed, by token; and the newest session table.
type registry struct {
	rows    []openRow
	frees   []inodeAt
	spare   []inodeAt
	freeing map[uint64]inodeAt
	sess    SessionTable
	known   bool
	fs      FS
	key     []byte
	scan    ScanResult
	plant   regPlants
}

// regPlants are bugs a test plants: an orphan freed while open, as an unlink freeing an open file
// at once, and a fenced session's opens closed once its fence commits rather than once it is
// acknowledged.
type regPlants struct {
	unlinkOpen, releaseOnCommit bool
}

// open reports whether any mount has at open.
func (g *registry) open(at inodeAt) bool {
	i, _ := slices.BinarySearchFunc(g.rows, openRow{at: at}, cmpRow)
	return i < len(g.rows) && g.rows[i].at == at
}

// holds reports whether session id's opens count, which is until it leaves the table, once every
// storage node holds its fence. Before any table, and for an ID the table has not handed out, it does.
func (g *registry) holds(id uint64) bool {
	if !g.known || id >= g.sess.Next {
		return true
	}
	i, ok := slices.BinarySearchFunc(g.sess.Sessions, id, cmpSession)
	return ok && (g.sess.Sessions[i].Fenced == 0 || !g.plant.releaseOnCommit)
}

func (g *registry) look(at inodeAt) {
	if !slices.Contains(g.frees, at) {
		g.frees = append(g.frees, at)
	}
}

// register serves rd, an open or a release of the file whose attributes Start names, to which
// kvs, from the leader's state, answer: an open registers its mount while the file has a link,
// and a release deregisters it, leaving an orphan no mount has open to be freed.
func (g *registry) register(group uint64, rd *Read, kvs []KV) error {
	var k Key
	if DecodeKey(rd.Start, &k) != nil || k.Space != SpaceFS || k.Tag != InodeAttr {
		return ErrMalformed
	}
	var a Attr
	found := len(kvs) > 0 && bytes.Equal(kvs[0].Key, rd.Start) && DecodeAttr(kvs[0].Value, &a) == nil
	row := openRow{inodeAt{group, k.Volume, k.Inode}, opener{rd.Session, rd.Mount}}
	i, there := slices.BinarySearchFunc(g.rows, row, cmpRow)
	switch {
	case rd.Reg == RegOpen && !there && found && a.Nlink > 0 && g.holds(rd.Session):
		g.rows = slices.Insert(g.rows, i, row)
	case rd.Reg == RegRelease && there:
		g.rows = slices.Delete(g.rows, i, i+1)
		if found && a.Nlink == 0 && !g.open(row.at) {
			g.look(row.at)
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

// SetSessions takes the committed session table t, a newer one than any before: the opens of a
// session that has left it close, which frees each orphan it was the last to have open.
func (sv *Server) SetSessions(t *SessionTable) {
	g := &sv.reg
	if g.known && (t.Version < g.sess.Version || t.Version == g.sess.Version && t.Acked < g.sess.Acked) {
		return
	}
	g.sess.Version, g.sess.Acked, g.sess.Next = t.Version, t.Acked, t.Next
	g.sess.Sessions = append(g.sess.Sessions[:0], t.Sessions...)
	g.known = true
	j := 0
	for _, r := range g.rows {
		if g.holds(r.who.session) {
			g.rows[j] = r
			j++
		} else {
			g.look(r.at)
		}
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

func (sv *Server) free(s *Shard, submit func(*Txn, uint64) error) {
	g := &sv.reg
	todo := g.frees
	g.frees = g.spare[:0]
	for _, at := range todo {
		if at.group != s.group {
			g.frees = append(g.frees, at)
			continue
		}
		if !g.freeTxn(s, at) {
			continue
		}
		sv.next++
		token := ServerToken | sv.next
		if submit(&g.fs.Txn, token) == nil {
			g.freeing[token] = at
		}
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
