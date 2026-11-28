package fuse

import (
	"bytes"
	"errors"
	"slices"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/fsclient"
	"github.com/nvcnvn/rock-solid/internal/fuseproto"
	"github.com/nvcnvn/rock-solid/internal/meta"
)

// fakeKernel is the kernel's end of a datagram socket pair standing in for /dev/fuse, which keeps
// each request and reply whole as the device does, and a one-voter data group at node 2's loop 1
// whose answers the test writes.
type fakeKernel struct {
	t    testing.TB
	fd   int
	d    *Daemon
	ctx  core.Context
	acts []core.Action
	buf  []byte
	req  []byte
	a    core.Action
	ev   core.Event
	rd   meta.Read
	ans  meta.ReadAnswer
	kv   [1]meta.KV
	root []byte
}

func newFakeKernel(t testing.TB) *fakeKernel {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_DGRAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = unix.Close(fds[0]); _ = unix.Close(fds[1]) })
	if err := unix.SetNonblock(fds[1], true); err != nil {
		t.Fatal(err)
	}
	k := &fakeKernel{t: t, fd: fds[0], d: New(Config{Volume: 1, Pool: 1, Inc: 1}), buf: make([]byte, 4096),
		root: meta.AppendAttr(nil, &meta.Attr{Mode: meta.ModeDir | 0o755, Nlink: 2})}
	k.ctx.Pool = core.NewPool([][]byte{make([]byte, 4<<20)}, 0)
	if err := k.d.Attach(&k.ctx, Device{Fd: fds[1]}); err != nil {
		t.Fatal(err)
	}
	k.acts = k.d.SetSession(&k.ctx, 7, k.acts[:0])
	plans := []meta.Group{{ID: 1}, {ID: 2, Start: []byte{meta.SpaceCtl + 1}, Voters: []meta.Replica{{ID: 1, Node: 2, Disk: 1}}}}
	k.acts = k.d.SetMembership(&k.ctx, plans, []meta.DiskAddr{{Disk: 1, At: core.NodeID{Node: 2, Loop: 1}}}, k.acts[:0])
	if !k.d.Ready() {
		t.Fatal("the daemon is not ready with a session and a map")
	}
	return k
}

// call writes request r as the kernel would, serves it, answers each Read the daemon sends with
// the root's attributes, and returns the reply's header and body.
func (k *fakeKernel) call(r *fuseproto.Request) (fuseproto.OutHeader, []byte) {
	k.req = fuseproto.AppendRequest(k.req[:0], r)
	if _, err := unix.Write(k.fd, k.req); err != nil {
		k.t.Fatal(err)
	}
	acts, err := k.d.Serve(&k.ctx, k.acts[:0])
	if err != nil {
		k.t.Fatal(err)
	}
	k.acts = acts
	for len(k.acts) > 0 {
		k.a = k.acts[0]
		k.acts = k.acts[:0]
		a := &k.a
		if a.Kind != core.ActNetSend || a.Peer != (core.NodeID{Node: 2, Loop: 1}) {
			k.t.Fatalf("the daemon asked for %+v", a)
		}
		_, _, token, _ := meta.Peek(&a.Msg)
		if err := meta.DecodeRead(k.ctx.Pool, &a.Msg, &k.rd); err != nil {
			k.t.Fatal(err)
		}
		k.kv[0] = meta.KV{Key: k.rd.Start, Value: k.root}
		k.ans = meta.ReadAnswer{Token: token, KVs: k.kv[:]}
		k.ev = core.Event{Kind: core.EvNetRecv, Peer: a.Peer}
		if err := meta.EncodeReadAnswer(k.ctx.Pool, 2, &k.ans, &k.ev.Msg); err != nil {
			k.t.Fatal(err)
		}
		if a.Msg.Buf != core.NoBuf {
			k.ctx.Pool.Free(a.Msg.Buf)
		}
		k.acts = k.d.OnEvent(&k.ctx, &k.ev, k.acts[:0])
	}
	n, err := unix.Read(k.fd, k.buf)
	if err != nil {
		k.t.Fatalf("no reply: %v", err)
	}
	h, body, err := fuseproto.DecodeReply(k.buf[:n])
	if err != nil || h.Unique != r.Unique {
		k.t.Fatalf("reply %+v to request %d: %v", h, r.Unique, err)
	}
	return h, body
}

// The daemon answers INIT itself, an opcode the codec does not decode with ENOSYS, and GETATTR
// through the filesystem client's read of the attributes.
func TestTheDaemonAnswersInitAnUnknownOpcodeAndGetattrOnItsDevice(t *testing.T) {
	k := newFakeKernel(t)
	h, body := k.call(&fuseproto.Request{Opcode: fuseproto.OpInit, Unique: 2, Init: fuseproto.InitIn{Major: 7, Minor: 45, MaxReadahead: 1 << 17, Flags: 1<<64 - 1}})
	if o, err := fuseproto.DecodeInitOut(body); h.Error != 0 || err != nil || o.Flags&fuseproto.FlagParallelDirops == 0 {
		t.Fatalf("INIT answered %+v %+v, %v", h, o, err)
	}
	unknown := fuseproto.AppendRequest(nil, &fuseproto.Request{Opcode: fuseproto.OpGetattr, Unique: 4, NodeID: 1})
	unknown[4] = 200
	if _, err := unix.Write(k.fd, unknown); err != nil {
		t.Fatal(err)
	}
	if _, err := k.d.Serve(&k.ctx, nil); err != nil {
		t.Fatal(err)
	}
	n, _ := unix.Read(k.fd, k.buf)
	if h, _, err := fuseproto.DecodeReply(k.buf[:n]); err != nil || h.Unique != 4 || h.Error != -int32(fuseproto.ENOSYS) {
		t.Fatalf("opcode 200 answered %+v, %v", h, err)
	}
	h, body = k.call(&fuseproto.Request{Opcode: fuseproto.OpGetattr, Unique: 6, NodeID: meta.RootInode})
	if _, _, a, err := fuseproto.DecodeAttrOut(body); h.Error != 0 || err != nil || a.Mode != uint32(meta.ModeDir|0o755) || a.Ino != meta.RootInode {
		t.Fatalf("GETATTR answered %+v %+v, %v", h, a, err)
	}
}

// A GETATTR's whole path, from the device through the machine and the meta client and back to the
// device, allocates nothing.
func TestAGetattrsPathThroughTheDaemonAllocatesNothing(t *testing.T) {
	k := newFakeKernel(t)
	r := fuseproto.Request{Opcode: fuseproto.OpGetattr, Unique: 2, NodeID: meta.RootInode}
	k.call(&r)
	if n := testing.AllocsPerRun(100, func() {
		r.Unique += 2
		k.call(&r)
	}); n != 0 {
		t.Fatalf("%v allocations a GETATTR", n)
	}
}

func BenchmarkGetattrThroughTheDaemon(b *testing.B) {
	k := newFakeKernel(b)
	r := fuseproto.Request{Opcode: fuseproto.OpGetattr, Unique: 2, NodeID: meta.RootInode}
	b.ReportAllocs()
	for b.Loop() {
		r.Unique += 2
		k.call(&r)
	}
}

// A placement delta the daemon cannot apply, one that fails its check or one past a gap, makes it
// want the whole table.
func TestADeltaTheDaemonCannotApplyMakesItWantTheWholeTable(t *testing.T) {
	d := New(Config{Volume: 1, Pool: 1, Inc: 1})
	var ctx core.Context
	d.SetTable(&ctx, &chunkstore.Table{Epoch: 1}, nil, nil)
	if d.WantsTable() {
		t.Fatal("the daemon wants a table it holds")
	}
	d.SetTable(&ctx, nil, &chunkstore.Delta{Epoch: 2, CRC: 1}, nil)
	if !d.WantsTable() {
		t.Fatal("a delta whose CRC is wrong left the daemon content with table 1")
	}
	d.SetTable(&ctx, &chunkstore.Table{Epoch: 2}, nil, nil)
	d.SetTable(&ctx, nil, &chunkstore.Delta{Epoch: 4}, nil)
	if !d.WantsTable() {
		t.Fatal("a delta past a gap left the daemon content with table 2")
	}
}

// An INIT of a major version the daemon does not speak is answered EPROTO, and Serve returns why.
func TestAnInitOfAnotherMajorEndsServeWithItsError(t *testing.T) {
	k := newFakeKernel(t)
	k.req = fuseproto.AppendRequest(k.req[:0], &fuseproto.Request{Opcode: fuseproto.OpInit, Unique: 2, Init: fuseproto.InitIn{Major: 8}})
	if _, err := unix.Write(k.fd, k.req); err != nil {
		t.Fatal(err)
	}
	if _, err := k.d.Serve(&k.ctx, nil); !errors.Is(err, fsclient.ErrMajor) {
		t.Fatalf("Serve returned %v", err)
	}
	n, _ := unix.Read(k.fd, k.buf)
	if h, _, err := fuseproto.DecodeReply(k.buf[:n]); err != nil || h.Unique != 2 || h.Error != -int32(fsclient.EPROTO) {
		t.Fatalf("INIT 8 answered %+v, %v", h, err)
	}
}

// A process's supplementary groups and CAP_DAC_OVERRIDE come from its /proc status; one whose
// status cannot be read, or lacks either line, has no credentials.
func TestAnOpenersCredentialsComeFromItsProcStatus(t *testing.T) {
	var c fsclient.Creds
	status := "Name:\tcat\nUid:\t0\t0\t0\t0\nGroups:\t5 1000 \nCapEff:\t0000000000000002\n"
	if !parseStatus([]byte(status), &c) || !slices.Equal(c.Groups, []uint32{5, 1000}) || !c.Override {
		t.Fatalf("parsed %+v", c)
	}
	if !parseStatus([]byte("Groups:\t\nCapEff:\t000001fffffffffd\n"), &c) || len(c.Groups) != 0 || c.Override {
		t.Fatalf("parsed %+v from no groups and no CAP_DAC_OVERRIDE", c)
	}
	if parseStatus([]byte("Groups:\t5\n"), &c) {
		t.Fatal("a status without CapEff parsed")
	}
	if New(Config{}).opener(1<<31) != nil {
		t.Fatal("a process that does not exist has credentials")
	}
}

// getattrAnswered serves a GETATTR of the root and answers its Read with err, returning the
// daemon's actions.
func (k *fakeKernel) getattrAnswered(err error) []core.Action {
	k.req = fuseproto.AppendRequest(k.req[:0], &fuseproto.Request{Opcode: fuseproto.OpGetattr, Unique: 2, NodeID: meta.RootInode})
	if _, err := unix.Write(k.fd, k.req); err != nil {
		k.t.Fatal(err)
	}
	acts, serr := k.d.Serve(&k.ctx, nil)
	if serr != nil || len(acts) != 1 || acts[0].Kind != core.ActNetSend {
		k.t.Fatalf("GETATTR asked for %+v, %v", acts, serr)
	}
	_, _, token, _ := meta.Peek(&acts[0].Msg)
	k.ans = meta.ReadAnswer{Token: token, Err: err}
	k.ev = core.Event{Kind: core.EvNetRecv, Peer: acts[0].Peer}
	if err := meta.EncodeReadAnswer(k.ctx.Pool, 2, &k.ans, &k.ev.Msg); err != nil {
		k.t.Fatal(err)
	}
	return k.d.OnEvent(&k.ctx, &k.ev, nil)
}

// A Read answered with an error that may clear, as a halted group's replacement may answer it,
// goes again only after the router's election wait, rather than spinning on an error that lasts.
func TestAReadAnsweredWithAnErrorGoesAgainAfterTheRoutersWait(t *testing.T) {
	for _, e := range []error{meta.ErrHalted, meta.ErrOverloaded} {
		k := newFakeKernel(t)
		acts := k.getattrAnswered(e)
		if len(acts) != 1 || acts[0].Kind != core.ActSetTimer || acts[0].Timer != fsclient.MountTimer || acts[0].At <= k.ctx.Now {
			t.Fatalf("a Read answered %v asked for %+v", e, acts)
		}
		k.ctx.Now = acts[0].At
		k.ev = core.Event{Kind: core.EvTimer, Timer: fsclient.MountTimer}
		if acts = k.d.OnEvent(&k.ctx, &k.ev, nil); len(acts) != 1 || acts[0].Kind != core.ActNetSend {
			t.Fatalf("after %v the router's timer asked for %+v", e, acts)
		}
	}
}

// A Read answered ErrMalformed or ErrTooLarge, which asking again cannot change, answers its
// request EIO at once, rather than leave its process waiting for good.
func TestAReadAnsweredWithAnErrorThatLastsAnswersEIO(t *testing.T) {
	for _, e := range []error{meta.ErrMalformed, meta.ErrTooLarge} {
		k := newFakeKernel(t)
		if acts := k.getattrAnswered(e); len(acts) != 0 {
			t.Fatalf("a Read answered %v asked for %+v", e, acts)
		}
		n, err := unix.Read(k.fd, k.buf)
		if err != nil {
			t.Fatalf("no reply after %v: %v", e, err)
		}
		if h, _, err := fuseproto.DecodeReply(k.buf[:n]); err != nil || h.Unique != 2 || h.Error != -int32(fuseproto.EIO) {
			t.Fatalf("after %v the kernel got %+v, %v", e, h, err)
		}
	}
}

// A new owner's ask to re-register, sent to loop 0 of the session's node where the daemon runs,
// re-registers each file the mount has open in the owner's group, then ends with the owner's
// term at the group's start; an ask for another session asks nothing.
func TestTheDaemonReregistersItsOpensWhenANewOwnerAsks(t *testing.T) {
	k := newFakeKernel(t)
	if h, _ := k.call(&fuseproto.Request{Opcode: fuseproto.OpOpen, Unique: 2, NodeID: meta.RootInode}); h.Error != 0 {
		t.Fatalf("OPEN answered %+v", h)
	}
	ask := func(session uint64) []core.Action {
		k.ev = core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{Node: 2, Loop: 1}}
		if err := meta.EncodeReclaim(k.ctx.Pool, 2, session, 5, &k.ev.Msg); err != nil {
			t.Fatal(err)
		}
		return k.d.OnEvent(&k.ctx, &k.ev, nil)
	}
	if acts := ask(8); len(acts) != 0 {
		t.Fatalf("an ask for another session asked for %+v", acts)
	}
	attr, err := meta.AppendKey(nil, &meta.Key{Space: meta.SpaceFS, Volume: 1, Inode: meta.RootInode, Tag: meta.InodeAttr})
	if err != nil {
		t.Fatal(err)
	}
	want := []meta.Read{
		{Start: attr, Reg: meta.RegReopen, Session: 7},
		{Start: []byte{meta.SpaceCtl + 1}, Reg: meta.RegReclaimed, Session: 7, Term: 5},
	}
	acts := ask(7)
	for _, w := range want {
		if len(acts) != 1 || acts[0].Kind != core.ActNetSend || acts[0].Peer != (core.NodeID{Node: 2, Loop: 1}) {
			t.Fatalf("want %+v, the daemon asked for %+v", w, acts)
		}
		_, _, token, _ := meta.Peek(&acts[0].Msg)
		if err := meta.DecodeRead(k.ctx.Pool, &acts[0].Msg, &k.rd); err != nil || !bytes.Equal(k.rd.Start, w.Start) || k.rd.Reg != w.Reg || k.rd.Session != w.Session || k.rd.Term != w.Term {
			t.Fatalf("the daemon sent %+v, want %+v: %v", k.rd, w, err)
		}
		k.ans = meta.ReadAnswer{Token: token}
		k.ev = core.Event{Kind: core.EvNetRecv, Peer: acts[0].Peer}
		if err := meta.EncodeReadAnswer(k.ctx.Pool, 2, &k.ans, &k.ev.Msg); err != nil {
			t.Fatal(err)
		}
		acts = k.d.OnEvent(&k.ctx, &k.ev, nil)
	}
	if len(acts) != 0 {
		t.Fatalf("after the end the daemon asked for %+v", acts)
	}
}

// A file whose last close comes during a new owner's re-registration stays released there: the
// mount skips a file closed before its turn, and holds the release of one whose re-registration is
// in flight until that is answered, so neither registers the file after its release.
func TestAFileClosedDuringAReregistrationIsNotRegisteredAfter(t *testing.T) {
	k := newFakeKernel(t)
	var fhs [2]uint64
	for i, ino := range []uint64{meta.RootInode, 9} {
		h, body := k.call(&fuseproto.Request{Opcode: fuseproto.OpOpen, Unique: uint64(2 + 2*i), NodeID: ino})
		o, err := fuseproto.DecodeOpen(body)
		if h.Error != 0 || err != nil {
			t.Fatalf("OPEN of %d answered %+v, %v", ino, h, err)
		}
		fhs[i] = o.Fh
	}
	type sentRead struct {
		token uint64
		meta.Read
	}
	sent := func(acts []core.Action) []sentRead {
		var rds []sentRead
		for _, a := range acts {
			if a.Kind != core.ActNetSend {
				t.Fatalf("the daemon asked for %+v", a)
			}
			if err := meta.DecodeRead(k.ctx.Pool, &a.Msg, &k.rd); err != nil {
				t.Fatal(err)
			}
			_, _, token, _ := meta.Peek(&a.Msg)
			rd := k.rd
			rd.Start = slices.Clone(rd.Start)
			rds = append(rds, sentRead{token, rd})
		}
		return rds
	}
	answer := func(rd sentRead) []sentRead {
		k.kv[0] = meta.KV{Key: rd.Start, Value: k.root}
		k.ans = meta.ReadAnswer{Token: rd.token, KVs: k.kv[:]}
		k.ev = core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{Node: 2, Loop: 1}}
		if err := meta.EncodeReadAnswer(k.ctx.Pool, 2, &k.ans, &k.ev.Msg); err != nil {
			t.Fatal(err)
		}
		return sent(k.d.OnEvent(&k.ctx, &k.ev, nil))
	}
	release := func(i int) []sentRead {
		k.req = fuseproto.AppendRequest(k.req[:0], &fuseproto.Request{Opcode: fuseproto.OpRelease, Unique: uint64(10 + 2*i), NodeID: []uint64{meta.RootInode, 9}[i], Fh: fhs[i]})
		if _, err := unix.Write(k.fd, k.req); err != nil {
			t.Fatal(err)
		}
		acts, err := k.d.Serve(&k.ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		return sent(acts)
	}
	key := func(ino uint64) []byte {
		b, err := meta.AppendKey(nil, &meta.Key{Space: meta.SpaceFS, Volume: 1, Inode: ino, Tag: meta.InodeAttr})
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	k.ev = core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{Node: 2, Loop: 1}}
	if err := meta.EncodeReclaim(k.ctx.Pool, 2, 7, 5, &k.ev.Msg); err != nil {
		t.Fatal(err)
	}
	rds := sent(k.d.OnEvent(&k.ctx, &k.ev, nil))
	if len(rds) != 1 || rds[0].Reg != meta.RegReopen || !bytes.Equal(rds[0].Start, key(9)) {
		t.Fatalf("the ask sent %+v, want the re-registration of 9", rds)
	}
	reopen := rds[0]
	if rds := release(1); len(rds) != 0 {
		t.Fatalf("the last close of 9 sent %+v while its re-registration was in flight", rds)
	}
	rds = release(0)
	if len(rds) != 1 || rds[0].Reg != meta.RegRelease || !bytes.Equal(rds[0].Start, key(meta.RootInode)) {
		t.Fatalf("the last close of the root sent %+v", rds)
	}
	if rds := answer(rds[0]); len(rds) != 0 {
		t.Fatalf("the root's release answered sent %+v", rds)
	}
	rds = answer(reopen)
	if len(rds) != 2 || !slices.ContainsFunc(rds, func(rd sentRead) bool { return rd.Reg == meta.RegRelease && bytes.Equal(rd.Start, key(9)) }) ||
		!slices.ContainsFunc(rds, func(rd sentRead) bool { return rd.Reg == meta.RegReclaimed && rd.Term == 5 }) {
		t.Fatalf("the re-registration of 9 answered sent %+v, want 9's release and the end", rds)
	}
}
