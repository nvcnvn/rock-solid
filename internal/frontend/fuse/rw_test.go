package fuse

import (
	"encoding/binary"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/fuseproto"
	"github.com/nvcnvn/rock-solid/internal/meta"
)

// rwKernel extends fakeKernel: meta Reads get a regular file's attributes, chunk reads and writes
// to disk 1 get OK replies, or are held for the test to answer while hold is set.
type rwKernel struct {
	*fakeKernel
	file []byte
	cev  core.Event
	rp   chunkstore.Response
	pfd  []unix.PollFd
	q    []core.Action
	hold bool
	held []core.RequestID
	peer core.NodeID
}

func newRWKernel(t testing.TB) *rwKernel {
	k := &rwKernel{fakeKernel: newFakeKernel(t)}
	k.file = meta.AppendAttr(nil, &meta.Attr{Mode: meta.ModeFile | 0o644, Nlink: 1, Size: 1 << 20, AllocEnd: 4})
	tbl := chunkstore.Table{Epoch: 1, Pools: []chunkstore.PoolTable{{ID: 1, Replicas: 1, MinLive: 1, PGs: []chunkstore.PGEntry{{Members: []chunkstore.Member{{Disk: 1}}, Interval: 1}}}}}
	k.acts = k.d.SetTable(&k.ctx, &tbl, nil, k.acts[:0])
	k.drain()
	return k
}

func (k *rwKernel) drain() {
	for len(k.acts) > 0 {
		k.q = append(k.q[:0], k.acts[1:]...)
		k.a = k.acts[0]
		rest := append(k.acts[:0], k.q...)
		k.acts = rest
		a := &k.a
		if a.Kind != core.ActNetSend {
			continue
		}
		if _, _, token, ok := meta.Peek(&a.Msg); ok {
			if err := meta.DecodeRead(k.ctx.Pool, &a.Msg, &k.rd); err != nil {
				k.t.Fatal(err)
			}
			k.kv[0] = meta.KV{Key: k.rd.Start, Value: k.file}
			k.ans = meta.ReadAnswer{Token: token, KVs: k.kv[:]}
			k.ev = core.Event{Kind: core.EvNetRecv, Peer: a.Peer}
			if err := meta.EncodeReadAnswer(k.ctx.Pool, 2, &k.ans, &k.ev.Msg); err != nil {
				k.t.Fatal(err)
			}
			if a.Msg.Buf != core.NoBuf && a.Reply {
				k.ctx.Pool.Free(a.Msg.Buf)
			}
			k.acts = k.d.OnEvent(&k.ctx, &k.ev, rest)
			continue
		}
		frame, _ := chunkstore.Frame(k.ctx.Pool, &a.Msg)
		kind, err := chunkstore.Peek(frame)
		if err != nil {
			k.t.Fatal(err)
		}
		id := core.RequestID(binary.LittleEndian.Uint64(frame[2:]))
		k.cev = core.Event{Kind: core.EvNetRecv, Peer: a.Peer}
		switch {
		case k.hold && (kind == chunkstore.KindWrite || kind == chunkstore.KindRead):
			k.held, k.peer = append(k.held, id), a.Peer
			continue
		case kind == chunkstore.KindWrite:
			k.rp = chunkstore.Response{Kind: chunkstore.KindReply, ID: id, Version: chunkstore.MakeVersion(1, 1)}
		case kind == chunkstore.KindRead:
			k.rp = chunkstore.Response{Kind: chunkstore.KindReadReply, ID: id}
			b, err := k.ctx.Pool.Alloc(4096)
			if err != nil {
				k.t.Fatal(err)
			}
			k.cev.Msg.Buf = b
		default:
			if a.Msg.Buf != core.NoBuf && a.Reply {
				k.ctx.Pool.Free(a.Msg.Buf)
			}
			continue
		}
		if a.Msg.Buf != core.NoBuf && a.Reply {
			k.ctx.Pool.Free(a.Msg.Buf)
		}
		k.cev.Msg.Len = uint16(len(chunkstore.AppendResponse(k.cev.Msg.Body[:0], &k.rp)))
		k.acts = k.d.OnEvent(&k.ctx, &k.cev, rest)
	}
}

func (k *rwKernel) rw(r *fuseproto.Request) fuseproto.OutHeader {
	k.req = fuseproto.AppendRequest(k.req[:0], r)
	if _, err := unix.Write(k.fd, k.req); err != nil {
		k.t.Fatal(err)
	}
	acts, err := k.d.Serve(&k.ctx, k.acts[:0])
	if err != nil {
		k.t.Fatal(err)
	}
	k.acts = acts
	k.drain()
	if len(k.buf) < ReadBuffer {
		k.buf = make([]byte, ReadBuffer)
	}
	if k.pfd == nil {
		k.pfd = []unix.PollFd{{Fd: int32(k.fd), Events: unix.POLLIN}}
	}
	if c, _ := unix.Poll(k.pfd, 1000); c == 0 {
		k.t.Fatalf("no reply to op %d (refused %v)", r.Opcode, k.d.Refused())
	}
	n, err := unix.Read(k.fd, k.buf)
	if err != nil {
		k.t.Fatalf("no reply to %d: %v", r.Opcode, err)
	}
	h, _, err := fuseproto.DecodeReply(k.buf[:n])
	if err != nil || h.Unique != r.Unique {
		k.t.Fatalf("reply %+v to %d: %v", h, r.Unique, err)
	}
	return h
}

// A READ's and a WRITE's whole paths through the daemon allocate nothing and leave no pool buffer
// but the daemon's own read buffer behind. A unix datagram on macOS holds at most 2 KiB, so the
// I/O is 1 KiB.
func TestReadAndWriteThroughTheDaemonAllocateNothing(t *testing.T) {
	k := newRWKernel(t)
	data := make([]byte, 1024)
	g := fuseproto.Request{Opcode: fuseproto.OpGetattr, Unique: 2, NodeID: 5}
	if h := k.rw(&g); h.Error != 0 {
		t.Fatalf("getattr %+v", h)
	}
	rd := fuseproto.Request{Opcode: fuseproto.OpRead, Unique: 4, NodeID: 5, Offset: 0, Size: 1024}
	wr := fuseproto.Request{Opcode: fuseproto.OpWrite, Unique: 6, NodeID: 5, Offset: 0, Size: 1024, Data: data}
	for _, r := range []*fuseproto.Request{&rd, &wr} {
		if h := k.rw(r); h.Error != 0 {
			t.Fatalf("op %d answered %+v", r.Opcode, h)
		}
		if n := testing.AllocsPerRun(100, func() {
			r.Unique += 8
			if h := k.rw(r); h.Error != 0 {
				t.Fatalf("op %d answered %+v", r.Opcode, h)
			}
		}); n != 0 {
			t.Errorf("op %d: %v allocations", r.Opcode, n)
		}
	}
	if live := k.ctx.Pool.(*core.Pool).Live(); live != 1 {
		t.Errorf("%d pool buffers live after the run", live)
	}
}

func (k *rwKernel) send(r *fuseproto.Request) {
	k.req = fuseproto.AppendRequest(k.req[:0], r)
	if _, err := unix.Write(k.fd, k.req); err != nil {
		k.t.Fatal(err)
	}
	acts, err := k.d.Serve(&k.ctx, k.acts[:0])
	if err != nil {
		k.t.Fatal(err)
	}
	k.acts = acts
	k.drain()
}

// replies reads every reply the kernel holds: each unique's errno.
func (k *rwKernel) replies() map[uint64]int32 {
	if err := unix.SetNonblock(k.fd, true); err != nil {
		k.t.Fatal(err)
	}
	got := map[uint64]int32{}
	for {
		n, err := unix.Read(k.fd, k.buf)
		if err != nil {
			return got
		}
		h, _, err := fuseproto.DecodeReply(k.buf[:n])
		if err != nil {
			k.t.Fatal(err)
		}
		got[h.Unique] = h.Error
	}
}

// answerRead answers held chunk read id OK.
func (k *rwKernel) answerRead(id core.RequestID) {
	k.rp = chunkstore.Response{Kind: chunkstore.KindReadReply, ID: id}
	k.cev = core.Event{Kind: core.EvNetRecv, Peer: k.peer}
	b, err := k.ctx.Pool.Alloc(4096)
	if err != nil {
		k.t.Fatal(err)
	}
	k.cev.Msg.Buf = b
	k.cev.Msg.Len = uint16(len(chunkstore.AppendResponse(k.cev.Msg.Body[:0], &k.rp)))
	k.acts = k.d.OnEvent(&k.ctx, &k.cev, k.acts[:0])
	k.drain()
}

// answerWrite answers held chunk write id OK.
func (k *rwKernel) answerWrite(id core.RequestID) {
	k.rp = chunkstore.Response{Kind: chunkstore.KindReply, ID: id, Version: chunkstore.MakeVersion(1, 1)}
	k.cev = core.Event{Kind: core.EvNetRecv, Peer: k.peer}
	k.cev.Msg.Len = uint16(len(chunkstore.AppendResponse(k.cev.Msg.Body[:0], &k.rp)))
	k.acts = k.d.OnEvent(&k.ctx, &k.cev, k.acts[:0])
	k.drain()
}

func (k *rwKernel) getattr() {
	if h := k.rw(&fuseproto.Request{Opcode: fuseproto.OpGetattr, Unique: 2, NodeID: 5}); h.Error != 0 {
		k.t.Fatalf("getattr %+v", h)
	}
}

// A fence fails the READs and WRITEs in flight with EIO: none of the fenced session's answers can
// land, and a request left waiting would hold its process, its pool buffer and the mount's oldest
// request until the connection ends.
func TestAFenceFailsTheReadsAndWritesInFlight(t *testing.T) {
	k := newRWKernel(t)
	k.getattr()
	k.hold = true
	k.send(&fuseproto.Request{Opcode: fuseproto.OpRead, Unique: 4, NodeID: 5, Offset: 0, Size: 1024})
	k.send(&fuseproto.Request{Opcode: fuseproto.OpWrite, Unique: 6, NodeID: 5, Offset: 8192, Size: 1024, Data: make([]byte, 1024)})
	k.acts = k.d.SetSession(&k.ctx, 8, k.acts[:0])
	k.drain()
	if got := k.replies(); got[4] != -int32(fuseproto.EIO) || got[6] != -int32(fuseproto.EIO) {
		t.Fatalf("after the fence the kernel got %v, want EIO for 4 and 6", got)
	}
	if live := k.ctx.Pool.(*core.Pool).Live(); live != 1 {
		t.Errorf("%d pool buffers live after the fence", live)
	}
}

// A late reply to a fenced session's chunk read answers no request of the new session, whose
// chunk client numbers its requests above every ID the old one used.
func TestAFencedSessionsLateReplyAnswersNoRequestOfTheNext(t *testing.T) {
	k := newRWKernel(t)
	k.getattr()
	k.hold = true
	k.send(&fuseproto.Request{Opcode: fuseproto.OpRead, Unique: 4, NodeID: 5, Offset: 0, Size: 1024})
	k.acts = k.d.SetSession(&k.ctx, 8, k.acts[:0])
	k.drain()
	k.replies()
	k.send(&fuseproto.Request{Opcode: fuseproto.OpRead, Unique: 6, NodeID: 5, Offset: 8192, Size: 1024})
	k.hold = false
	k.answerRead(k.held[0])
	if got := k.replies(); len(got) != 0 {
		t.Fatalf("session 7's reply to chunk read %#x answered %v", k.held[0], got)
	}
	k.answerRead(k.held[1])
	if got, ok := k.replies()[6]; !ok || got != 0 {
		t.Fatalf("session 8's read answered %d, %v", got, ok)
	}
}

// A late reply to a fenced session's chunk write answers no write of the new session: the new
// client's write IDs start above the old one's as well as its read IDs.
func TestAFencedSessionsLateWriteReplyAnswersNoWriteOfTheNext(t *testing.T) {
	k := newRWKernel(t)
	k.getattr()
	k.hold = true
	for i, off := range []uint64{0, 8192, 16384} {
		k.send(&fuseproto.Request{Opcode: fuseproto.OpWrite, Unique: uint64(4 + 2*i), NodeID: 5, Offset: off, Size: 1024, Data: make([]byte, 1024)})
	}
	if len(k.held) != 3 {
		t.Fatalf("held %x", k.held)
	}
	k.acts = k.d.SetSession(&k.ctx, 8, k.acts[:0])
	k.drain()
	k.replies()
	k.send(&fuseproto.Request{Opcode: fuseproto.OpWrite, Unique: 20, NodeID: 5, Offset: 0, Size: 1024, Data: make([]byte, 1024)})
	k.hold = false
	for _, id := range k.held[:3] {
		k.answerWrite(id)
	}
	if got := k.replies(); len(got) != 0 {
		t.Fatalf("session 7's write replies answered %v (chunk requests %x)", got, k.held)
	}
}
