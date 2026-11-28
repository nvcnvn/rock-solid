package meta

import (
	"math"
	"testing"
	"time"

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

// ask sends session's open or release of the file from mount 1 and returns its answer.
func (o *owner) ask(reg byte, session uint64) ReadAnswer {
	rd := Read{Start: o.attrKey(regFile), End: append(o.attrKey(regFile), 0), Limit: 1, Reg: reg, Session: session, Mount: 1}
	ev := o.msg(func(pool core.BufPool, m *core.Msg) error { return EncodeRead(pool, o.c.n[o.l].s.group, 7, &rd, m) })
	var acts []core.Action
	o.step(func(ctx *core.Context, out *Output) { acts = o.sv.ServeRead(ctx, &ev, o.c.n[o.l].s, nil) })
	var ra ReadAnswer
	if len(acts) != 1 || DecodeReadAnswer(o.c.n[o.l].pool, &acts[0].Msg, &ra) != nil {
		o.t.Fatal("the open or release was not answered")
	}
	return ra
}

// serve serves session's open or release of the file from mount 1 with no free after it.
func (o *owner) serve(reg byte, session uint64) {
	rd := Read{Start: o.attrKey(regFile), End: append(o.attrKey(regFile), 0), Limit: 1, Reg: reg, Session: session, Mount: 1}
	ev := o.msg(func(pool core.BufPool, m *core.Msg) error { return EncodeRead(pool, o.c.n[o.l].s.group, 7, &rd, m) })
	o.c.call(o.l, func(ctx *core.Context, out *Output) { o.sv.ServeRead(ctx, &ev, o.c.n[o.l].s, nil) })
}

// reg is ask, failing on an error, and returns the links it was answered.
func (o *owner) reg(reg byte, session uint64) uint64 {
	ra := o.ask(reg, session)
	if ra.Err != nil {
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
	if err := o.fs.Unlink(RootInode, []byte("f"), regFile, &a, 5); err != nil {
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

// An open from a session the table shows fenced is refused with no attributes, and registers
// nothing, so the orphan is freed at once.
func TestAnOpenFromAFencedSessionIsRefused(t *testing.T) {
	o := newOwner(t, regPlants{})
	o.table(chunkstore.SessionTable{Version: 2, Acked: 1, Next: 4, Live: []uint64{1, 3}})
	if ra := o.ask(RegOpen, 2); ra.Err != ErrFenced || len(ra.KVs) != 0 {
		t.Fatalf("an open from a fenced session was answered %+v", ra)
	}
	o.unlink()
	if !o.freed() {
		t.Fatal("the refused open was registered")
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

// A free proposed by a leader cut off before it commits is answered dropped once the replica
// applies the next leader's entries, so no token waits forever and the orphan is looked at again.
func TestAFreeDroppedAtALeaderChangeIsAnswered(t *testing.T) {
	o := newOwner(t, regPlants{})
	o.reg(RegOpen, 1)
	o.unlink()
	old := o.l
	o.c.cut[old] = true
	o.reg(RegRelease, 1)
	if len(o.sv.reg.freeing) != 1 {
		t.Fatalf("%d frees in flight, want 1", len(o.sv.reg.freeing))
	}
	o.c.renewLeases()
	o.l = o.c.elect()
	o.c.heartbeat()
	o.c.cut[old] = false
	o.c.heartbeat()
	o.c.settle()
	ctx := o.c.ctx(old)
	o.sv.Reply(&ctx, []Answer{o.c.answer(old, ServerToken|o.sv.next)}, nil)
	if len(o.sv.reg.freeing) != 0 || len(o.sv.reg.frees) != 1 {
		t.Fatalf("%d frees in flight and %d orphans to look at after the drop", len(o.sv.reg.freeing), len(o.sv.reg.frees))
	}
}

// An orphan whose free the replica refuses, as when its round holds all it may, is looked at again.
func TestAFreeRefusedIsTriedAgain(t *testing.T) {
	o := newOwner(t, regPlants{})
	o.reg(RegOpen, 1)
	o.unlink()
	o.serve(RegRelease, 1)
	o.sv.free(o.c.n[o.l].s, func(*Txn, uint64) error { return ErrOverloaded })
	if len(o.sv.reg.frees) != 1 || len(o.sv.reg.freeing) != 0 {
		t.Fatalf("after a refused free, %d orphans to look at and %d frees in flight", len(o.sv.reg.frees), len(o.sv.reg.freeing))
	}
	o.step(func(*core.Context, *Output) {})
	if !o.freed() {
		t.Fatal("the orphan was not freed once the replica took its free")
	}
}

// An orphan whose free the replica refuses for good, as a halted replica does, is not looked at
// again.
func TestAFreeRefusedForGoodIsDropped(t *testing.T) {
	o := newOwner(t, regPlants{})
	o.reg(RegOpen, 1)
	o.unlink()
	o.serve(RegRelease, 1)
	o.sv.free(o.c.n[o.l].s, func(*Txn, uint64) error { return ErrHalted })
	if len(o.sv.reg.frees) != 0 || len(o.sv.reg.listed) != 0 {
		t.Fatalf("after a lasting refusal, %d orphans to look at", len(o.sv.reg.frees))
	}
}

// A leader that loses its group drops the opens it registered, which a later leader serves, at
// its next free; it registers none while it cannot become sole again.
func TestAnExLeaderDropsItsOpens(t *testing.T) {
	o := newOwner(t, regPlants{})
	o.reg(RegOpen, 1)
	old := o.l
	o.c.cut[old] = true
	o.c.renewLeases()
	o.l = o.c.elect()
	o.c.heartbeat()
	o.c.cut[old] = false
	o.c.heartbeat()
	o.c.settle()
	if len(o.sv.reg.rows) != 1 {
		t.Fatalf("%d opens before the ex-leader frees, want 1", len(o.sv.reg.rows))
	}
	o.c.call(old, func(ctx *core.Context, out *Output) { o.sv.Free(ctx, o.c.n[old].s, out) })
	if len(o.sv.reg.rows) != 0 || len(o.sv.reg.index) != 0 || len(o.sv.reg.opens) != 0 || len(o.sv.reg.groups) != 0 {
		t.Fatalf("the ex-leader kept %d opens", len(o.sv.reg.rows))
	}
	o.serve(RegOpen, 2)
	if len(o.sv.reg.rows) != 0 {
		t.Fatal("a leader that cannot become sole registered an open")
	}
}

// Closing a fenced session's opens takes time linear in them: five times the opens take well under
// twenty-five times as long.
func TestClosingAFencedSessionsOpensIsLinear(t *testing.T) {
	cost := func(n int) time.Duration {
		best := time.Duration(math.MaxInt64)
		for range 3 {
			sv := NewServer()
			sv.SetSessions(&chunkstore.SessionTable{Version: 1, Acked: 1, Next: 2, Live: []uint64{1}})
			for i := range n {
				sv.reg.insert(openRow{openKey: openKey{inodeAt{1, 1, uint64(i)}, opener{1, 1}}})
			}
			start := time.Now()
			sv.SetSessions(&chunkstore.SessionTable{Version: 2, Acked: 2, Next: 2})
			best = min(best, time.Since(start))
			if len(sv.reg.rows) != 0 || len(sv.reg.frees) != n {
				t.Fatalf("%d opens left and %d orphans to look at, want 0 and %d", len(sv.reg.rows), len(sv.reg.frees), n)
			}
		}
		return best
	}
	small, large := cost(10_000), cost(50_000)
	if large > 12*small {
		t.Fatalf("closing 10,000 opens took %v and 50,000 took %v", small, large)
	}
}

// A replica on the round driver that has not started its shard frees nothing and keeps the
// orphans and opens it holds for once it does.
func TestARoundReplicaNotStartedFreesNothing(t *testing.T) {
	sv := NewServer()
	sv.reg.insert(openRow{openKey: openKey{inodeAt{1, 1, 6}, opener{1, 1}}})
	sv.reg.look(inodeAt{1, 1, regFile})
	var ctx core.Context
	sv.FreeRound(&ctx, &RoundShard{}, nil)
	if len(sv.reg.frees) != 1 || len(sv.reg.rows) != 1 {
		t.Fatalf("%d orphans to look at and %d opens after a free on a replica not started", len(sv.reg.frees), len(sv.reg.rows))
	}
}
