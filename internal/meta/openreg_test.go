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
	t    *testing.T
	c    *group
	l    int
	sv   *Server
	fs   FS
	sent []core.Action
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
	o.table(chunkstore.SessionTable{Next: 1})
	return o
}

// failover cuts the leader off and elects another, whose server starts empty.
func (o *owner) failover(p regPlants) {
	o.c.cut[o.l] = true
	o.c.renewLeases()
	o.l, o.sv = o.c.elect(), NewServer()
	o.sv.reg.plant = p
	o.c.heartbeat()
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
		o.c.call(o.l, func(ctx *core.Context, out *Output) { o.sent = o.sv.Free(ctx, o.c.n[o.l].s, out, o.sent[:0]) })
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

// ask sends session's open, release or re-registration of the file from mount 1, or the end of its
// re-registration with the leader's term, and returns its answer.
func (o *owner) ask(reg byte, session uint64) ReadAnswer {
	rd := Read{Start: o.attrKey(regFile), End: append(o.attrKey(regFile), 0), Limit: 1, Reg: reg, Session: session, Mount: 1, Term: o.c.n[o.l].s.raft.Term()}
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

// table hands the server t, as read after every ask it made.
func (o *owner) table(t chunkstore.SessionTable) {
	o.step(func(*core.Context, *Output) {})
	o.step(func(*core.Context, *Output) { o.sv.SetSessions(&t, o.sv.FetchSessions()) })
}

// asked reports the sessions the last Free asked to re-register.
func (o *owner) asked() []uint64 {
	var ids []uint64
	for i := range o.sent {
		if _, session, term, err := DecodeReclaim(o.c.n[o.l].pool, &o.sent[i].Msg); err == nil && term == o.c.n[o.l].s.raft.Term() {
			ids = append(ids, session)
		}
	}
	return ids
}

// later moves the clock on by d, renewing the leader's lease.
func (o *owner) later(d core.Tick) {
	o.c.now += d
	o.c.renewLeases()
	o.c.heartbeat()
	o.step(func(*core.Context, *Output) {})
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

// A new owner frees no orphan in its grace period, which ends only once every live session has
// re-registered, an empty re-registration counting, never by time alone; then it scans the orphan
// list, and frees an orphan at its last release. The planted bugs free the orphan a live session
// has open but has not yet re-registered: one at the grace period's start, and one at its end by
// time.
func TestAGracePeriodEndsOnlyOnceEveryLiveSessionReregistered(t *testing.T) {
	for _, p := range []regPlants{{}, {orphanInGrace: true}, {graceByTime: true}} {
		o := newOwner(t, p)
		o.table(chunkstore.SessionTable{Version: 1, Acked: 1, Next: 3, Live: []uint64{1, 2}, Nodes: []uint32{3, 4}})
		o.reg(RegOpen, 1)
		o.unlink()
		o.failover(p)
		o.table(chunkstore.SessionTable{Version: 1, Acked: 1, Next: 3, Live: []uint64{1, 2}, Nodes: []uint32{3, 4}})
		if got := o.asked(); len(got) != 2 {
			t.Fatalf("with %+v, the new owner asked sessions %v to re-register, want 1 and 2", p, got)
		}
		o.reg(RegReclaimed, 2)
		o.later(SessionLease / 2)
		if o.freed() != (p != regPlants{}) {
			t.Fatalf("with %+v, the orphan session 1 has open was freed before it re-registered: %v", p, o.freed())
		}
		if p != (regPlants{}) {
			continue
		}
		if got := o.asked(); len(got) != 1 || got[0] != 1 {
			t.Fatalf("the new owner asked sessions %v to re-register, want 1", got)
		}
		o.reg(RegReopen, 1)
		o.reg(RegReclaimed, 1)
		if o.freed() || !o.sv.reg.open(inodeAt{o.c.n[o.l].s.group, 1, regFile}) {
			t.Fatal("the orphan was freed, or its re-registered open not counted, at the grace period's end")
		}
		o.reg(RegRelease, 1)
		if !o.freed() {
			t.Fatal("the orphan was not freed at its last release after the grace period")
		}
	}
}

// A session that does not re-register within a lease is evicted, asked again each lease since an
// ask may reach a voter that does not lead, and the grace period ends once its fence is
// acknowledged, not at its commit.
func TestAnUnregisteredSessionIsEvictedAndTheGraceEndsAtItsFence(t *testing.T) {
	o := newOwner(t, regPlants{})
	o.reg(RegOpen, 2)
	o.unlink()
	o.failover(regPlants{})
	o.table(chunkstore.SessionTable{Version: 1, Acked: 1, Next: 3, Live: []uint64{1, 2}, Nodes: []uint32{3, 4}})
	o.reg(RegReclaimed, 1)
	o.later(SessionLease / 2)
	if got := o.sv.Evictions(nil); len(got) != 0 {
		t.Fatalf("sessions %v evicted within a lease", got)
	}
	o.later(SessionLease / 2)
	if got := o.sv.Evictions(nil); len(got) != 1 || got[0] != 2 {
		t.Fatalf("sessions %v evicted, want 2", got)
	}
	o.later(SessionLease / 2)
	if got := o.sv.Evictions(nil); len(got) != 0 {
		t.Fatalf("sessions %v evicted again within a lease", got)
	}
	o.later(SessionLease / 2)
	if got := o.sv.Evictions(nil); len(got) != 1 || got[0] != 2 {
		t.Fatalf("a lease after the ask, sessions %v evicted, want 2", got)
	}
	o.table(chunkstore.SessionTable{Version: 2, Acked: 1, Next: 3, Live: []uint64{1}, Nodes: []uint32{3}})
	if o.freed() {
		t.Fatal("the grace period ended at the fence's commit")
	}
	o.table(chunkstore.SessionTable{Version: 2, Acked: 2, Next: 3, Live: []uint64{1}, Nodes: []uint32{3}})
	if !o.freed() {
		t.Fatal("the grace period did not end once the evicted session's fence was acknowledged")
	}
}

// A new owner holds new opens in its grace period, refusing them ErrGrace for the router to send
// again, while it takes re-registrations; the planted bug registers a new open in it.
func TestANewOpenWaitsOutTheGracePeriod(t *testing.T) {
	for _, p := range []regPlants{{}, {openInGrace: true}} {
		o := newOwner(t, p)
		o.failover(p)
		o.table(chunkstore.SessionTable{Version: 1, Acked: 1, Next: 3, Live: []uint64{1, 2}, Nodes: []uint32{3, 4}})
		if ra := o.ask(RegOpen, 1); (ra.Err == ErrGrace) == p.openInGrace {
			t.Fatalf("with %+v, an open in the grace period was answered %+v", p, ra)
		}
		o.reg(RegReclaimed, 1)
		o.reg(RegReclaimed, 2)
		if ra := o.ask(RegOpen, 2); ra.Err != nil || !o.sv.reg.open(inodeAt{o.c.n[o.l].s.group, 1, regFile}) {
			t.Fatalf("an open after the grace period was answered %+v", ra)
		}
	}
}

// A re-registration that ends at an older owner counts for none after it.
func TestAReregistrationForAnOlderOwnerDoesNotCount(t *testing.T) {
	o := newOwner(t, regPlants{})
	o.failover(regPlants{})
	o.table(chunkstore.SessionTable{Version: 1, Acked: 1, Next: 2, Live: []uint64{1}, Nodes: []uint32{3}})
	rd := Read{Start: o.attrKey(regFile), End: append(o.attrKey(regFile), 0), Limit: 1, Reg: RegReclaimed, Session: 1, Mount: 1, Term: o.c.n[o.l].s.raft.Term() - 1}
	ev := o.msg(func(pool core.BufPool, m *core.Msg) error { return EncodeRead(pool, o.c.n[o.l].s.group, 7, &rd, m) })
	o.step(func(ctx *core.Context, out *Output) { o.sv.ServeRead(ctx, &ev, o.c.n[o.l].s, nil) })
	if !o.sv.reg.owners[0].grace {
		t.Fatal("a re-registration with an older owner ended the grace period")
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
	ctx := o.c.ctx(o.l)
	o.sv.free(&ctx, o.c.n[o.l].s, nil, func(*Txn, uint64) error { return ErrOverloaded })
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
	ctx := o.c.ctx(o.l)
	o.sv.free(&ctx, o.c.n[o.l].s, nil, func(*Txn, uint64) error { return ErrHalted })
	if len(o.sv.reg.frees) != 0 || len(o.sv.reg.listed) != 0 {
		t.Fatalf("after a lasting refusal, %d orphans to look at", len(o.sv.reg.frees))
	}
}

// A leader that loses its group drops the opens it registered, which a later leader serves, at
// its next free.
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
	o.c.call(old, func(ctx *core.Context, out *Output) { o.sv.Free(ctx, o.c.n[old].s, out, nil) })
	if len(o.sv.reg.rows) != 0 || len(o.sv.reg.index) != 0 || len(o.sv.reg.opens) != 0 || len(o.sv.reg.groups) != 0 || len(o.sv.reg.owners) != 0 {
		t.Fatalf("the ex-leader kept %d opens", len(o.sv.reg.rows))
	}
}

// Closing a fenced session's opens takes time linear in them: five times the opens take well under
// twenty-five times as long.
func TestClosingAFencedSessionsOpensIsLinear(t *testing.T) {
	cost := func(n int) time.Duration {
		best := time.Duration(math.MaxInt64)
		for range 3 {
			sv := NewServer()
			sv.SetSessions(&chunkstore.SessionTable{Version: 1, Acked: 1, Next: 2, Live: []uint64{1}}, 0)
			for i := range n {
				sv.reg.insert(openRow{openKey: openKey{inodeAt{1, 1, uint64(i)}, opener{1, 1}}})
			}
			start := time.Now()
			sv.SetSessions(&chunkstore.SessionTable{Version: 2, Acked: 2, Next: 2}, 0)
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
