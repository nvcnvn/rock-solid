package meta

import (
	"testing"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
)

// owner is a three-replica group's leader serving the open registry, as a host drives its Server.
type owner struct {
	t  *testing.T
	c  *group
	l  int
	sv *Server
	fs FS
}

const regFile = 5

var regAttr = Attr{Mode: ModeFile | 0o644, Nlink: 1, Size: 10, AllocEnd: 1}

func newOwner(t *testing.T, p regPlants) *owner {
	o := &owner{t: t, c: newGroup(t, 3, false), sv: NewServer(), fs: FS{Volume: 1}}
	o.sv.reg.plant = p
	o.c.renewLeases()
	o.l = o.c.elect()
	o.c.heartbeat()
	if err := o.fs.Root(&Attr{Mode: ModeDir | 0o755, Nlink: 2}); err != nil {
		t.Fatal(err)
	}
	o.c.txn(o.l, o.fs.Txn, 1)
	a := regAttr
	if err := o.fs.Create(RootInode, RootInode, []byte("f"), regFile, &a, nil, 1); err != nil {
		t.Fatal(err)
	}
	o.c.txn(o.l, o.fs.Txn, 2)
	o.c.settle()
	if ans := o.c.answer(o.l, 2); ans.Err != nil || ans.Result.FailedCheck >= 0 || !o.has(&Key{Space: SpaceFS, Volume: 1, Inode: regFile, Tag: InodeAttr}) {
		t.Fatalf("the create was answered %+v", ans)
	}
	return o
}

func (o *owner) attrKey(inode uint64) []byte {
	return mustKey(&Key{Space: SpaceFS, Volume: 1, Inode: inode, Tag: InodeAttr})
}

func (o *owner) has(k *Key) bool {
	var r ScanResult
	key := mustKey(k)
	if err := o.c.n[o.l].s.Scan(key, append(key, 0), 1, &r); err != nil {
		o.t.Fatal(err)
	}
	return len(r.KVs) == 1
}

// freed reports whether the file is off the orphan list with its keys gone and its purge entry made.
func (o *owner) freed() bool {
	gone := !o.has(&Key{Space: SpaceFS, Volume: 1, Inode: regFile, Tag: InodeAttr}) &&
		!o.has(&Key{Space: SpaceFS, Volume: 1, Inode: regFile >> SeqBits << SeqBits, Tag: SlotOrphan, ID: regFile})
	if gone != o.has(&Key{Space: SpaceFS, Volume: 1, Inode: regFile >> SeqBits << SeqBits, Tag: SlotPurge, ID: regFile}) {
		o.t.Fatal("the file's keys and its purge entry disagree")
	}
	return gone
}

// step runs f on the leader, then the host's Free, settling the group and replying every answer
// to the server until nothing more is proposed.
func (o *owner) step(f func(ctx *core.Context, out *Output)) {
	from := o.sv.next
	o.c.call(o.l, f)
	for {
		o.c.call(o.l, func(ctx *core.Context, out *Output) { o.sv.Free(ctx, o.c.n[o.l].s, out) })
		o.c.settle()
		to := o.sv.next
		if from == to {
			return
		}
		for tok := from + 1; tok <= to; tok++ {
			if a, ok := o.c.n[o.l].answers[ServerToken|tok]; ok {
				ctx := o.c.ctx(o.l)
				o.sv.Reply(&ctx, []Answer{a}, nil)
			}
		}
		from = to
	}
}

func (o *owner) msg(encode func(pool core.BufPool, m *core.Msg) error) core.Event {
	ev := core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{Node: 3}}
	if err := encode(o.c.n[o.l].pool, &ev.Msg); err != nil {
		o.t.Fatal(err)
	}
	return ev
}

// reg sends session's open or release of the file from mount 1 and returns the links it was answered.
func (o *owner) reg(reg byte, session uint64) uint64 {
	rd := Read{Start: o.attrKey(regFile), End: append(o.attrKey(regFile), 0), Limit: 1, Reg: reg, Session: session, Mount: 1}
	ev := o.msg(func(pool core.BufPool, m *core.Msg) error { return EncodeRead(pool, o.c.n[o.l].s.group, 7, &rd, m) })
	var acts []core.Action
	o.step(func(ctx *core.Context, out *Output) { acts = o.sv.ServeRead(ctx, &ev, o.c.n[o.l].s, nil) })
	var ra ReadAnswer
	if len(acts) != 1 || DecodeReadAnswer(o.c.n[o.l].pool, &acts[0].Msg, &ra) != nil || ra.Err != nil {
		o.t.Fatalf("the open or release was answered %+v", ra)
	}
	var a Attr
	if len(ra.KVs) == 0 || DecodeAttr(ra.KVs[0].Value, &a) != nil {
		return 0
	}
	return a.Nlink
}

// unlink sends a client's unlink of the file's one link, which makes it an orphan.
func (o *owner) unlink() {
	a := regAttr
	if err := o.fs.Unlink(RootInode, []byte("f"), regFile, &a, true, nil, 5); err != nil {
		o.t.Fatal(err)
	}
	ev := o.msg(func(pool core.BufPool, m *core.Msg) error {
		return EncodeTxn(pool, o.c.n[o.l].s.group, 8, &o.fs.Txn, m)
	})
	o.step(func(ctx *core.Context, out *Output) { o.sv.Serve(ctx, &ev, o.c.n[o.l].s, out, nil) })
	if ans := o.c.answer(o.l, ServerToken|1); ans.Err != nil || ans.Result.FailedCheck >= 0 {
		o.t.Fatalf("the unlink was answered %+v", ans)
	}
	ctx := o.c.ctx(o.l)
	o.sv.Reply(&ctx, []Answer{o.c.answer(o.l, ServerToken|1)}, nil)
	o.step(func(*core.Context, *Output) {})
}

func (o *owner) table(t chunkstore.SessionTable) {
	o.step(func(*core.Context, *Output) { o.sv.SetSessions(&t) })
}

// An unlinked file stays an orphan while any mount has it open, and its owner frees it at its last
// release; an orphan takes no new open.
func TestAnOrphanIsFreedAtItsLastRelease(t *testing.T) {
	o := newOwner(t, regPlants{})
	if o.reg(RegOpen, 1) != 1 || o.reg(RegOpen, 2) != 1 {
		t.Fatal("an open was not answered the file's attributes")
	}
	o.unlink()
	if o.freed() {
		t.Fatal("an open file was freed at its unlink")
	}
	o.reg(RegOpen, 3)
	o.reg(RegRelease, 1)
	if o.freed() {
		t.Fatal("the orphan was freed while a mount has it open")
	}
	o.reg(RegRelease, 2)
	if !o.freed() {
		t.Fatal("the orphan was not freed at its last release, though the open after its unlink did not register")
	}
}

// A fenced session's opens count until a table acknowledges a version at or above the first that
// showed it fenced; the planted bug closes them when its fence commits.
func TestAFencedSessionsOpensCloseOnlyOnceItsFenceIsAcknowledged(t *testing.T) {
	for _, p := range []regPlants{{}, {releaseOnCommit: true}} {
		o := newOwner(t, p)
		o.table(chunkstore.SessionTable{Version: 1, Next: 4, Live: []uint64{1, 2, 3}})
		o.reg(RegOpen, 2)
		o.reg(RegOpen, 3)
		o.unlink()
		o.table(chunkstore.SessionTable{Version: 3, Acked: 1, Next: 4, Live: []uint64{1, 3}})
		o.table(chunkstore.SessionTable{Version: 4, Acked: 2, Next: 4, Live: []uint64{1}})
		o.reg(RegRelease, 3)
		if o.freed() != p.releaseOnCommit {
			t.Fatalf("with %+v, freed before session 2's fence was acknowledged: %v", p, o.freed())
		}
		o.table(chunkstore.SessionTable{Version: 4, Acked: 3, Next: 4, Live: []uint64{1}})
		if !o.freed() {
			t.Fatal("the orphan was not freed once its holder's fence was acknowledged")
		}
	}
}

// An unlink of a file no mount has open frees it in a Txn of the owner's; the planted bug frees an
// open one too.
func TestAnUnlinkFreesAFileOnlyOnceNoMountHasItOpen(t *testing.T) {
	o := newOwner(t, regPlants{})
	o.unlink()
	if !o.freed() {
		t.Fatal("an unlinked file no mount had open was not freed")
	}
	o = newOwner(t, regPlants{unlinkOpen: true})
	o.reg(RegOpen, 1)
	o.unlink()
	if !o.freed() {
		t.Fatal("the planted bug did not free an open file")
	}
}

// A new owner frees no orphan, as its registry lacks the opens its predecessor registered; the
// planted bug frees one a mount has open at the old owner.
func TestANewOwnerFreesNoOrphan(t *testing.T) {
	for _, p := range []regPlants{{}, {newOwnerFrees: true}} {
		o := newOwner(t, p)
		o.reg(RegOpen, 1)
		o.c.cut[o.l] = true
		o.c.renewLeases()
		o.l, o.sv = o.c.elect(), NewServer()
		o.sv.reg.plant = p
		o.c.heartbeat()
		o.unlink()
		if o.freed() != p.newOwnerFrees {
			t.Fatalf("with %+v, the new owner freed the orphan: %v", p, o.freed())
		}
	}
}
