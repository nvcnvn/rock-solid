package fuse

import (
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
	if _, err := k.d.Serve(&k.ctx, nil); !errors.Is(err, ErrMajor) {
		t.Fatalf("Serve returned %v", err)
	}
	n, _ := unix.Read(k.fd, k.buf)
	if h, _, err := fuseproto.DecodeReply(k.buf[:n]); err != nil || h.Unique != 2 || h.Error != -int32(eproto) {
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

// A Read answered with a final error goes again only after the router's election wait, rather
// than spinning on an error that lasts.
func TestAReadAnsweredWithAnErrorGoesAgainAfterTheRoutersWait(t *testing.T) {
	k := newFakeKernel(t)
	k.req = fuseproto.AppendRequest(k.req[:0], &fuseproto.Request{Opcode: fuseproto.OpGetattr, Unique: 2, NodeID: meta.RootInode})
	if _, err := unix.Write(k.fd, k.req); err != nil {
		t.Fatal(err)
	}
	acts, err := k.d.Serve(&k.ctx, nil)
	if err != nil || len(acts) != 1 || acts[0].Kind != core.ActNetSend {
		t.Fatalf("GETATTR asked for %+v, %v", acts, err)
	}
	_, _, token, _ := meta.Peek(&acts[0].Msg)
	k.ans = meta.ReadAnswer{Token: token, Err: meta.ErrOverloaded}
	k.ev = core.Event{Kind: core.EvNetRecv, Peer: acts[0].Peer}
	if err := meta.EncodeReadAnswer(k.ctx.Pool, 2, &k.ans, &k.ev.Msg); err != nil {
		t.Fatal(err)
	}
	acts = k.d.OnEvent(&k.ctx, &k.ev, nil)
	if len(acts) != 1 || acts[0].Kind != core.ActSetTimer || acts[0].Timer != RouterTimer || acts[0].At <= k.ctx.Now {
		t.Fatalf("a Read answered ErrOverloaded asked for %+v", acts)
	}
	k.ctx.Now = acts[0].At
	k.ev = core.Event{Kind: core.EvTimer, Timer: RouterTimer}
	if acts = k.d.OnEvent(&k.ctx, &k.ev, nil); len(acts) != 1 || acts[0].Kind != core.ActNetSend {
		t.Fatalf("the router's timer asked for %+v", acts)
	}
}
