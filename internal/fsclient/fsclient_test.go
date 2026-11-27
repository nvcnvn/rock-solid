package fsclient

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"maps"
	"slices"
	"testing"

	"go.etcd.io/raft/v3"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/fuseproto"
	"github.com/nvcnvn/rock-solid/internal/lsm"
	"github.com/nvcnvn/rock-solid/internal/meta"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

const vol = 5

var rootAttr = meta.Attr{Mode: meta.ModeDir | 0o755, Nlink: 2, Mtime: 10, Ctime: 10}

// mount is one machine over a real State, its chunks in memory; it answers every read, Txn and
// chunk call at once unless held.
type mount struct {
	t      *testing.T
	m      *Machine
	s      *meta.State
	ctx    core.Context
	out    Output
	unique uint64
	now    int64
	data   map[uint64][]byte
	// held keeps the calls of the next requests for the test to answer.
	held    bool
	reads   []Read
	txns    []meta.Txn
	sent    []TxnSend
	chunks  []ChunkCall
	replies [][]byte
}

func newMount(t *testing.T, cfg Config) *mount {
	s, err := meta.NewState(lsm.Config{MaxPiece: 64 << 10}, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	var f meta.FS
	f.Volume = vol
	if err := f.Root(&rootAttr); err != nil {
		t.Fatal(err)
	}
	var r meta.Result
	if err := s.Apply(&f.Txn, &r); err != nil || r.FailedCheck != -1 {
		t.Fatal(err, r)
	}
	cfg.Volume, cfg.Session, cfg.Mount = vol, 7, 1
	if cfg.SlotLimit == 0 {
		cfg.SlotLimit = 1 << 20
	}
	return &mount{t: t, m: New(cfg), s: s, ctx: core.Context{Pool: sim.NewPool(64<<20, 1<<16)}, now: 1_000_000_000, data: map[uint64][]byte{}}
}

// apply applies t as a shard does, refusing it first if it does not pass Check.
func (h *mount) apply(t *meta.Txn) meta.Result {
	h.t.Helper()
	if _, err := t.Check(); err != nil {
		h.t.Fatalf("a shard refuses %+v: %v", *t, err)
	}
	var r meta.Result
	if err := h.s.Apply(t, &r); err != nil {
		h.t.Fatal(err)
	}
	return r
}

func (h *mount) answerRead(rd Read) {
	var r meta.ScanResult
	if err := h.s.Scan(rd.Start, rd.End, rd.Limit, &r); err != nil {
		h.t.Fatal(err)
	}
	h.m.ReadDone(&h.ctx, rd.Token, r.KVs, &h.out)
}

func (h *mount) answerTxn(s TxnSend) {
	r := h.apply(s.Txn)
	h.m.TxnDone(&h.ctx, s.Token, &r, nil, &h.out)
}

func (h *mount) answerChunk(c ChunkCall) {
	b := h.data[uint64(c.File.Inode)]
	if !c.Read {
		if end := c.Off + uint64(len(c.Data)); end > uint64(len(b)) {
			b = append(b, make([]byte, end-uint64(len(b)))...)
		}
		copy(b[c.Off:], c.Data)
		h.data[uint64(c.File.Inode)] = b
	}
	end := c.Off + uint64(c.N)
	if !c.Read {
		end = c.Off + uint64(len(c.Data))
	}
	for off := c.Off; off < end; off = (off/core.ChunkSize + 1) * core.ChunkSize {
		n := min(end, (off/core.ChunkSize+1)*core.ChunkSize) - off
		done := chunkstore.Completion{Token: c.Token, Off: off, Len: uint32(n), Status: chunkstore.StatusOK}
		if c.Read {
			buf, err := h.ctx.Pool.AllocZeroed(int(off%core.BlockSize + n))
			if err != nil {
				h.t.Fatal(err)
			}
			if off < uint64(len(b)) {
				copy(h.ctx.Pool.Bytes(buf)[off%core.BlockSize:], b[off:min(uint64(len(b)), off+n)])
			}
			done.Buf = buf
		}
		h.m.ChunkDone(&h.ctx, &done, &h.out)
	}
}

// serve answers what the machine emitted until it emits nothing more, or keeps it when held.
func (h *mount) serve() {
	for {
		o := h.out
		h.out = Output{}
		for b := o.Replies; len(b) > 0; b = b[binary.LittleEndian.Uint32(b):] {
			h.replies = append(h.replies, bytes.Clone(b[:binary.LittleEndian.Uint32(b)]))
		}
		for _, s := range o.Txns {
			h.txns = append(h.txns, *s.Txn)
		}
		if h.held {
			h.reads = append(h.reads, o.Reads...)
			h.sent = append(h.sent, o.Txns...)
			h.chunks = append(h.chunks, o.Chunks...)
			return
		}
		if len(o.Reads)+len(o.Txns)+len(o.Chunks) == 0 {
			return
		}
		for _, rd := range o.Reads {
			h.answerRead(rd)
		}
		for _, s := range o.Txns {
			h.answerTxn(s)
		}
		for _, c := range o.Chunks {
			h.answerChunk(c)
		}
	}
}

// send hands the machine r and serves it.
func (h *mount) send(r fuseproto.Request) uint64 {
	h.unique += 2
	r.Unique = h.unique
	h.now++
	h.m.Request(&h.ctx, h.now, &r, &h.out)
	h.serve()
	return r.Unique
}

// reply is the reply to unique: its errno and body.
func (h *mount) reply(unique uint64) (int32, []byte) {
	h.t.Helper()
	for _, b := range h.replies {
		if binary.LittleEndian.Uint64(b[8:]) == unique {
			return -int32(binary.LittleEndian.Uint32(b[4:])), b[fuseproto.OutHeaderSize:]
		}
	}
	h.t.Fatalf("no reply to %d", unique)
	return 0, nil
}

func (h *mount) ok(unique uint64) []byte {
	h.t.Helper()
	errno, b := h.reply(unique)
	if errno != 0 {
		h.t.Fatalf("request %d failed with errno %d", unique, errno)
	}
	return b
}

func (h *mount) errno(unique uint64, want fuseproto.Errno) {
	h.t.Helper()
	if errno, _ := h.reply(unique); errno != int32(want) {
		h.t.Fatalf("request %d: errno %d, want %d", unique, errno, want)
	}
}

func (h *mount) entry(unique uint64) fuseproto.Entry {
	h.t.Helper()
	b := h.ok(unique)
	e, err := fuseproto.DecodeEntry(b[:min(len(b), 128)])
	if err != nil {
		h.t.Fatal(err)
	}
	return e
}

func (h *mount) lookup(dir uint64, name string) uint64 {
	return h.send(fuseproto.Request{Opcode: fuseproto.OpLookup, NodeID: dir, Name: []byte(name)})
}

// create makes a file and returns its inode and handle.
func (h *mount) create(dir uint64, name string) (uint64, uint64) {
	h.t.Helper()
	u := h.send(fuseproto.Request{Opcode: fuseproto.OpCreate, NodeID: dir, Name: []byte(name), Mode: uint32(meta.ModeFile | 0o644)})
	b := h.ok(u)
	e, _ := fuseproto.DecodeEntry(b[:128])
	o, _ := fuseproto.DecodeOpen(b[128:])
	return e.NodeID, o.Fh
}

func (h *mount) mkdir(dir uint64, name string) uint64 {
	h.t.Helper()
	return h.entry(h.send(fuseproto.Request{Opcode: fuseproto.OpMkdir, NodeID: dir, Name: []byte(name), Mode: 0o755})).NodeID
}

func (h *mount) write(ino, fh, off uint64, data []byte) uint64 {
	return h.send(fuseproto.Request{Opcode: fuseproto.OpWrite, NodeID: ino, Fh: fh, Offset: off, Size: uint32(len(data)), Data: data})
}

func (h *mount) read(ino, fh, off uint64, n uint32) []byte {
	h.t.Helper()
	return h.ok(h.send(fuseproto.Request{Opcode: fuseproto.OpRead, NodeID: ino, Fh: fh, Offset: off, Size: n}))
}

func (h *mount) open(ino uint64) fuseproto.Open {
	h.t.Helper()
	o, err := fuseproto.DecodeOpen(h.ok(h.send(fuseproto.Request{Opcode: fuseproto.OpOpen, NodeID: ino})))
	if err != nil {
		h.t.Fatal(err)
	}
	return o
}

func (h *mount) attr(ino uint64) meta.Attr {
	h.t.Helper()
	k, _ := meta.AppendKey(nil, &meta.Key{Space: meta.SpaceFS, Volume: vol, Inode: ino, Tag: meta.InodeAttr})
	var r meta.ScanResult
	if err := h.s.Scan(k, append(bytes.Clone(k), 0), 1, &r); err != nil || len(r.KVs) != 1 {
		h.t.Fatalf("no attributes of %x: %v", ino, err)
	}
	var a meta.Attr
	if err := meta.DecodeAttr(r.KVs[0].Value, &a); err != nil {
		h.t.Fatal(err)
	}
	return a
}

func (h *mount) setAttr(ino uint64, b meta.Attr) {
	h.t.Helper()
	a := h.attr(ino)
	f := meta.FS{Volume: vol}
	if err := f.SetAttr(ino, &a, &b, nil); err != nil {
		h.t.Fatal(err)
	}
	if r := h.apply(&f.Txn); r.FailedCheck != -1 {
		h.t.Fatal(r)
	}
}

func TestLookupAnswersCurrentAttributesAndAMissingNameWithENOENT(t *testing.T) {
	h := newMount(t, Config{})
	h.errno(h.lookup(meta.RootInode, "f"), ENOENT)
	ino, _ := h.create(meta.RootInode, "f")
	e := h.entry(h.lookup(meta.RootInode, "f"))
	if e.NodeID != ino || e.EntryValid != 0 || e.AttrValid != 1 || e.Attr.Mode != uint32(meta.ModeFile|0o644) {
		t.Fatalf("file entry %+v", e)
	}
	b := h.attr(ino)
	b.Size, b.Mtime = 77, 5e9
	h.setAttr(ino, b)
	if e := h.entry(h.lookup(meta.RootInode, "f")); e.Attr.Size != 77 || e.Attr.Mtime != 5 {
		t.Fatalf("a lookup after another node's change answered %+v", e.Attr)
	}
	d := h.mkdir(meta.RootInode, "d")
	if e := h.entry(h.lookup(meta.RootInode, "d")); e.NodeID != d || e.EntryValid != 1 {
		t.Fatalf("directory entry %+v", e)
	}
	s := newMount(t, Config{Strict: true})
	s.mkdir(meta.RootInode, "d")
	if e := s.entry(s.lookup(meta.RootInode, "d")); e.EntryValid != 0 || e.AttrValid != 0 {
		t.Fatalf("strict entry %+v", e)
	}
}

func TestAnAttributeReplyFetchedBeforeALaterRequestIsFetchedAgain(t *testing.T) {
	for _, later := range []string{"getattr", "lookup"} {
		h := newMount(t, Config{})
		ino, _ := h.create(meta.RootInode, "f")
		h.held, h.replies = true, nil
		first := h.send(fuseproto.Request{Opcode: fuseproto.OpGetattr, NodeID: ino})
		if len(h.reads) != 1 {
			t.Fatalf("getattr read %d keys", len(h.reads))
		}
		rd := h.reads[0]
		h.reads = nil
		if later == "getattr" {
			h.send(fuseproto.Request{Opcode: fuseproto.OpGetattr, NodeID: ino})
		} else {
			h.lookup(meta.RootInode, "f")
		}
		b := h.attr(ino)
		b.Size = 99
		h.setAttr(ino, b)
		h.answerRead(rd)
		if len(h.out.Replies) != 0 || len(h.out.Reads) != 1 {
			t.Fatalf("%s: the first getattr answered with what it fetched before the later request", later)
		}
		h.held = false
		h.serve()
		for _, r := range h.reads {
			h.answerRead(r)
		}
		h.serve()
		va, _, a, err := fuseproto.DecodeAttrOut(h.ok(first))
		if err != nil || a.Size != 99 || va != 1 {
			t.Fatalf("%s: first getattr answered %+v %v", later, a, err)
		}
	}
}

func TestAnOpenAfterAChangeDropsTheCache(t *testing.T) {
	h := newMount(t, Config{})
	ino, _ := h.create(meta.RootInode, "f")
	if o := h.open(ino); o.Flags&fuseproto.OpenKeepCache == 0 {
		t.Fatal("an open of an unchanged file dropped the cache")
	}
	b := h.attr(ino)
	b.Ctime++
	h.setAttr(ino, b)
	if o := h.open(ino); o.Flags&fuseproto.OpenKeepCache != 0 {
		t.Fatal("an open after another node's change kept the cache")
	}
	if o := h.open(ino); o.Flags&fuseproto.OpenKeepCache == 0 {
		t.Fatal("a second open with no change between dropped the cache")
	}
}

func TestNamespaceRequestsAnswerTheirChecksErrnos(t *testing.T) {
	h := newMount(t, Config{})
	h.create(meta.RootInode, "f")
	d := h.mkdir(meta.RootInode, "d")
	h.create(d, "g")
	n := len(h.txns)
	h.errno(h.send(fuseproto.Request{Opcode: fuseproto.OpCreate, NodeID: meta.RootInode, Name: []byte("f"), Mode: uint32(meta.ModeFile | 0o644), Flags: oExcl}), EEXIST)
	h.errno(h.send(fuseproto.Request{Opcode: fuseproto.OpMkdir, NodeID: meta.RootInode, Name: []byte("d"), Mode: 0o755}), EEXIST)
	h.errno(h.send(fuseproto.Request{Opcode: fuseproto.OpRmdir, NodeID: meta.RootInode, Name: []byte("d")}), ENOTEMPTY)
	if len(h.txns) != n+3 {
		t.Fatalf("%d Txns for 3 failed requests", len(h.txns)-n)
	}
	n = len(h.txns)
	h.errno(h.send(fuseproto.Request{Opcode: fuseproto.OpRename, NodeID: meta.RootInode, Name: []byte("d"), NewDir: meta.RootInode, NewName: []byte("f")}), ENOTDIR)
	h.errno(h.send(fuseproto.Request{Opcode: fuseproto.OpRename, NodeID: meta.RootInode, Name: []byte("f"), NewDir: meta.RootInode, NewName: []byte("d")}), EISDIR)
	h.errno(h.send(fuseproto.Request{Opcode: fuseproto.OpRename, NodeID: meta.RootInode, Name: []byte("f"), NewDir: d, NewName: []byte("f")}), EXDEV)
	h.errno(h.send(fuseproto.Request{Opcode: fuseproto.OpUnlink, NodeID: meta.RootInode, Name: []byte("d")}), EISDIR)
	h.errno(h.send(fuseproto.Request{Opcode: fuseproto.OpRmdir, NodeID: meta.RootInode, Name: []byte("f")}), ENOTDIR)
	h.errno(h.send(fuseproto.Request{Opcode: fuseproto.OpUnlink, NodeID: meta.RootInode, Name: []byte("x")}), ENOENT)
	if len(h.txns) != n {
		t.Fatalf("%d Txns sent for requests refused before one", len(h.txns)-n)
	}
	h.ok(h.send(fuseproto.Request{Opcode: fuseproto.OpRename, NodeID: meta.RootInode, Name: []byte("f"), NewDir: meta.RootInode, NewName: []byte("h")}))
	h.errno(h.lookup(meta.RootInode, "f"), ENOENT)
	h.ok(h.send(fuseproto.Request{Opcode: fuseproto.OpUnlink, NodeID: d, Name: []byte("g")}))
	h.ok(h.send(fuseproto.Request{Opcode: fuseproto.OpRmdir, NodeID: meta.RootInode, Name: []byte("d")}))
	h.errno(h.lookup(meta.RootInode, "d"), ENOENT)
	for _, tx := range h.txns {
		if tx.Replay == nil && len(tx.Ops) > 0 && tx.Ops[0].Kind != meta.OpUpdate {
			t.Fatalf("a client request's Txn without its replay key: %+v", tx)
		}
	}
}

func TestTheFirstCreateInASlotWaitsForOneReservation(t *testing.T) {
	h := newMount(t, Config{})
	h.create(meta.RootInode, "a")
	if len(h.txns) != 2 || h.txns[0].Replay != nil || h.txns[1].Replay == nil {
		t.Fatalf("first create sent %d Txns", len(h.txns))
	}
	h.create(meta.RootInode, "b")
	if len(h.txns) != 3 {
		t.Fatalf("second create sent %d Txns", len(h.txns)-2)
	}
	d := h.mkdir(meta.RootInode, "d")
	if d>>meta.SeqBits == 0 || d&(1<<meta.SeqBits-1) != 1 {
		t.Fatalf("mkdir made %x, not sequence 1 of a fresh slot", d)
	}
	n := len(h.txns)
	h.create(d, "f")
	h.mkdir(d, "e")
	if len(h.txns) != n+2 {
		t.Fatalf("creates in a fresh slot sent %d Txns", len(h.txns)-n)
	}
}

func TestReaddirplusListsEveryNameAcrossCalls(t *testing.T) {
	h := newMount(t, Config{})
	names := map[string]bool{}
	for _, n := range []string{"a", "bb", "ccc", "dddd", "eeeee"} {
		h.create(meta.RootInode, n)
		names[n] = true
	}
	fh, _ := fuseproto.DecodeOpen(h.ok(h.send(fuseproto.Request{Opcode: fuseproto.OpOpendir, NodeID: meta.RootInode})))
	off := uint64(0)
	for range 10 {
		b := h.ok(h.send(fuseproto.Request{Opcode: fuseproto.OpReaddirplus, NodeID: meta.RootInode, Fh: fh.Fh, Offset: off, Size: 2 * 160}))
		if len(b) == 0 {
			break
		}
		for len(b) > 0 {
			nl := binary.LittleEndian.Uint32(b[128+16:])
			name := string(b[128+24 : 128+24+nl])
			if binary.LittleEndian.Uint64(b) != 0 || !names[name] {
				t.Fatalf("listed %q with node %d", name, binary.LittleEndian.Uint64(b))
			}
			delete(names, name)
			off = binary.LittleEndian.Uint64(b[128+8:])
			b = b[fuseproto.DirentPlusSize(int(nl)):]
		}
	}
	if len(names) != 0 {
		t.Fatalf("never listed %v", names)
	}
}

func TestWritesRaiseAllocEndFirstByDoublingSteps(t *testing.T) {
	h := newMount(t, Config{})
	ino, fh := h.create(meta.RootInode, "f")
	block := make([]byte, 4096)
	var ends []uint64
	written := uint64(0)
	for c := uint64(0); c < 40; c++ {
		h.ok(h.write(ino, fh, c*core.ChunkSize, block))
		if a := h.attr(ino); len(ends) == 0 || a.AllocEnd != ends[len(ends)-1] {
			ends = append(ends, a.AllocEnd)
			if a.Size != written {
				t.Fatalf("the raise to %d committed size %d, want %d written before it", a.AllocEnd, a.Size, written)
			}
		}
		written = c*core.ChunkSize + 4096
	}
	want := []uint64{1, 3, 7, 15, 31, 47}
	if len(ends) != len(want) {
		t.Fatalf("alloc_end went %v, want %v", ends, want)
	}
	for i := range want {
		if ends[i] != want[i] {
			t.Fatalf("alloc_end went %v, want %v", ends, want)
		}
	}
	h.held = true
	h.write(ino, fh, 60*core.ChunkSize, block)
	if len(h.chunks) != 0 || len(h.sent) != 1 {
		t.Fatalf("a write past alloc_end sent %d chunk writes and %d Txns before its raise answered", len(h.chunks), len(h.sent))
	}
}

func TestCloseCommitsSizeAndMtimeAndReadsAreClamped(t *testing.T) {
	h := newMount(t, Config{})
	ino, fh := h.create(meta.RootInode, "f")
	h.ok(h.write(ino, fh, 0, []byte("hello world")))
	if a := h.attr(ino); a.Size != 0 {
		t.Fatalf("size %d committed before close", a.Size)
	}
	if got := h.read(ino, fh, 6, 100); string(got) != "world" {
		t.Fatalf("read %q", got)
	}
	if got := h.read(ino, fh, 11, 100); len(got) != 0 {
		t.Fatalf("read past the size gave %q", got)
	}
	stored := h.attr(ino).Mtime
	h.now = stored - 100
	h.ok(h.write(ino, fh, 11, []byte("!")))
	h.ok(h.send(fuseproto.Request{Opcode: fuseproto.OpFlush, NodeID: ino, Fh: fh}))
	a := h.attr(ino)
	if a.Size != 12 || a.Mtime != stored+1 {
		t.Fatalf("close committed size %d mtime %d, want 12 and %d", a.Size, a.Mtime, stored+1)
	}
	h.now = 50e9
	h.ok(h.write(ino, fh, 0, []byte("H")))
	h.ok(h.send(fuseproto.Request{Opcode: fuseproto.OpFsync, NodeID: ino, Fh: fh}))
	if a := h.attr(ino); a.Size != 12 || a.Mtime != 50e9+1 {
		t.Fatalf("fsync committed size %d mtime %d", a.Size, a.Mtime)
	}
	n := len(h.txns)
	h.ok(h.send(fuseproto.Request{Opcode: fuseproto.OpFlush, NodeID: ino, Fh: fh}))
	if len(h.txns) != n {
		t.Fatal("a close with nothing written sent a Txn")
	}
	b := h.attr(ino)
	b.Size = 5
	h.setAttr(ino, b)
	h.open(ino)
	if got := h.read(ino, fh, 0, 100); string(got) != "Hello" {
		t.Fatalf("read after another node's truncate gave %q", got)
	}
}

func TestPerCallAnswersOfOpcodesTheKernelRemembers(t *testing.T) {
	h := newMount(t, Config{})
	ino, _ := h.create(meta.RootInode, "f")
	h.ok(h.send(fuseproto.Request{Opcode: fuseproto.OpAccess, NodeID: ino}))
	h.ok(h.send(fuseproto.Request{Opcode: fuseproto.OpFsyncdir, NodeID: meta.RootInode}))
	h.errno(h.send(fuseproto.Request{Opcode: fuseproto.OpRename2, NodeID: meta.RootInode}), EINVAL)
	h.errno(h.send(fuseproto.Request{Opcode: fuseproto.OpTmpfile, NodeID: meta.RootInode}), EOPNOTSUPP)
	h.errno(h.send(fuseproto.Request{Opcode: fuseproto.OpLink, NodeID: meta.RootInode}), EPERM)
	h.errno(h.send(fuseproto.Request{Opcode: fuseproto.OpGetxattr, NodeID: ino}), ENODATA)
	h.errno(h.send(fuseproto.Request{Opcode: fuseproto.OpRemovexattr, NodeID: ino}), ENODATA)
	h.errno(h.send(fuseproto.Request{Opcode: fuseproto.OpSetxattr, NodeID: ino}), EOPNOTSUPP)
	if b := h.ok(h.send(fuseproto.Request{Opcode: fuseproto.OpListxattr, NodeID: ino})); len(b) != 8 || binary.LittleEndian.Uint64(b) != 0 {
		t.Fatalf("listxattr's length %x", b)
	}
	if b := h.ok(h.send(fuseproto.Request{Opcode: fuseproto.OpListxattr, NodeID: ino, Size: 100})); len(b) != 0 {
		t.Fatalf("listxattr %x", b)
	}
	_, _, a, err := fuseproto.DecodeStatxOut(h.ok(h.send(fuseproto.Request{Opcode: fuseproto.OpStatx, NodeID: ino})))
	if err != nil || a.Ino != ino || a.Mode != uint32(meta.ModeFile|0o644) {
		t.Fatalf("statx %+v %v", a, err)
	}
}

func TestSetattrChangesModeAndTimesAndRefusesASizeChange(t *testing.T) {
	h := newMount(t, Config{})
	ino, _ := h.create(meta.RootInode, "f")
	u := h.send(fuseproto.Request{Opcode: fuseproto.OpSetattr, NodeID: ino, Setattr: fuseproto.Setattr{Valid: fuseproto.AttrMode | fuseproto.AttrMtime, Mode: 0o600, Mtime: 3}})
	if _, _, a, err := fuseproto.DecodeAttrOut(h.ok(u)); err != nil || a.Mode != uint32(meta.ModeFile|0o600) || a.Mtime != 3 {
		t.Fatalf("setattr answered %+v %v", a, err)
	}
	if a := h.attr(ino); a.Mode != meta.ModeFile|0o600 || a.Mtime != 3e9 {
		t.Fatalf("setattr stored %+v", a)
	}
	h.errno(h.send(fuseproto.Request{Opcode: fuseproto.OpSetattr, NodeID: ino, Setattr: fuseproto.Setattr{Valid: fuseproto.AttrSize, Size: 10}}), EOPNOTSUPP)
	h.ok(h.send(fuseproto.Request{Opcode: fuseproto.OpSetattr, NodeID: ino, Setattr: fuseproto.Setattr{Valid: fuseproto.AttrSize, Size: 0}}))
	d := h.mkdir(meta.RootInode, "d")
	h.ok(h.send(fuseproto.Request{Opcode: fuseproto.OpSetattr, NodeID: d, Setattr: fuseproto.Setattr{Valid: fuseproto.AttrMode, Mode: 0o700}}))
	if a := h.attr(d); a.Mode != meta.ModeDir|0o700 {
		t.Fatalf("directory setattr stored %+v", a)
	}
}

const invPurgeMisses = "a purge never misses a chunk written before a crash"

// scanner reads at most limit keys of [start, end) from a MetaStore state.
type scanner func(start, end []byte, limit int, r *meta.ScanResult) error

// purgeMisses checks that every chunk last names as holding data of an inode, up to and
// including its chunk index, lies below the inode's alloc_end, or, once the inode is removed,
// within its purge entry.
func purgeMisses(scan scanner, last map[uint64]uint64) error {
	for _, ino := range slices.Sorted(maps.Keys(last)) {
		chunk := last[ino]
		k, _ := meta.AppendKey(nil, &meta.Key{Space: meta.SpaceFS, Volume: vol, Inode: ino, Tag: meta.InodeAttr})
		var r meta.ScanResult
		if err := scan(k, append(bytes.Clone(k), 0), 1, &r); err != nil {
			return err
		}
		var a meta.Attr
		if len(r.KVs) == 1 && meta.DecodeAttr(r.KVs[0].Value, &a) == nil && a.AllocEnd > chunk {
			continue
		}
		p, _ := meta.AppendKey(nil, &meta.Key{Space: meta.SpaceFS, Volume: vol, Inode: ino >> meta.SeqBits << meta.SeqBits, Tag: meta.SlotPurge, ID: ino})
		if err := scan(p, append(bytes.Clone(p), 0), 1, &r); err != nil {
			return err
		}
		if len(r.KVs) == 1 {
			if e, err := meta.DecodePurge(r.KVs[0].Value); err == nil && e.Start == 0 && e.End > chunk {
				continue
			}
		}
		return fmt.Errorf("chunk %d of inode %x lies past its alloc_end and its purge entry", chunk, ino)
	}
	return nil
}

// purgeMisses checks the chunks the mount wrote against its state.
func (h *mount) purgeMisses() error {
	last := map[uint64]uint64{}
	for ino, b := range h.data {
		if len(b) > 0 {
			last[ino] = (uint64(len(b)) - 1) / core.ChunkSize
		}
	}
	return purgeMisses(h.s.Scan, last)
}

func TestAWriteBeforeItsRaiseLeavesAChunkThePurgeMisses(t *testing.T) {
	for _, planted := range []bool{false, true} {
		h := newMount(t, Config{})
		h.m.plant.raiseAfterWrite = planted
		ino, fh := h.create(meta.RootInode, "f")
		h.held = true
		h.write(ino, fh, 0, []byte("x"))
		for _, c := range h.chunks {
			h.answerChunk(c)
		}
		h.m, h.out, h.held = New(h.m.cfg), Output{}, false
		h.ok(h.send(fuseproto.Request{Opcode: fuseproto.OpUnlink, NodeID: meta.RootInode, Name: []byte("f")}))
		if err := h.purgeMisses(); (err != nil) != planted {
			t.Fatalf("planted %v: %v", planted, err)
		}
	}
}

// A CREATE without O_EXCL whose name another node created first opens that file, as open(2)
// would, and with O_TRUNC truncates it only when empty, as a size setattr does until Truncate;
// with O_EXCL it fails EEXIST.
func TestACreateWithoutExclOpensTheFileAnotherNodeCreatedFirst(t *testing.T) {
	h := newMount(t, Config{})
	other := *h
	other.m, other.out, other.replies = New(Config{Volume: vol, Session: 8, Mount: 1, SlotLimit: 1 << 20}), Output{}, nil
	ino, _ := other.create(meta.RootInode, "f")
	if got, fh := h.create(meta.RootInode, "f"); got != ino || fh == 0 {
		t.Fatalf("create opened inode %x handle %d, want inode %x", got, fh, ino)
	}
	h.errno(h.send(fuseproto.Request{Opcode: fuseproto.OpCreate, NodeID: meta.RootInode, Name: []byte("f"), Mode: uint32(meta.ModeFile | 0o644), Flags: oExcl}), EEXIST)
	h.ok(h.send(fuseproto.Request{Opcode: fuseproto.OpCreate, NodeID: meta.RootInode, Name: []byte("f"), Mode: uint32(meta.ModeFile | 0o644), Flags: oTrunc}))
	b := h.attr(ino)
	b.Size = 5
	h.setAttr(ino, b)
	h.errno(h.send(fuseproto.Request{Opcode: fuseproto.OpCreate, NodeID: meta.RootInode, Name: []byte("f"), Mode: uint32(meta.ModeFile | 0o644), Flags: oTrunc}), EOPNOTSUPP)
	other.mkdir(meta.RootInode, "d")
	h.errno(h.send(fuseproto.Request{Opcode: fuseproto.OpCreate, NodeID: meta.RootInode, Name: []byte("d"), Mode: uint32(meta.ModeFile | 0o644)}), EISDIR)
}

// A request served from answers the machine has seen before allocates nothing: lookups,
// attributes, an open, a write within alloc_end, its commit at flush, a read and a release.
func TestTheRequestPathAllocatesNothing(t *testing.T) {
	h := newMount(t, Config{})
	ino, fh := h.create(meta.RootInode, "f")
	h.write(ino, fh, 0, []byte("x"))
	h.ok(h.send(fuseproto.Request{Opcode: fuseproto.OpFlush, NodeID: ino, Fh: fh}))
	kvs := map[string][]meta.KV{}
	ok := meta.Result{FailedCheck: -1}
	data := make([]byte, 4096)
	reqs := []fuseproto.Request{
		{Opcode: fuseproto.OpLookup, NodeID: meta.RootInode, Name: []byte("f")},
		{Opcode: fuseproto.OpGetattr, NodeID: ino},
		{Opcode: fuseproto.OpOpen, NodeID: ino},
		{Opcode: fuseproto.OpWrite, NodeID: ino, Fh: fh, Offset: 0, Size: 4096, Data: data},
		{Opcode: fuseproto.OpFlush, NodeID: ino, Fh: fh},
		{Opcode: fuseproto.OpRead, NodeID: ino, Fh: fh, Offset: 0, Size: 4096},
		{Opcode: fuseproto.OpRelease, NodeID: ino},
	}
	var out, next Output
	buf, err := h.ctx.Pool.AllocZeroed(4096)
	if err != nil {
		t.Fatal(err)
	}
	cycle := func() {
		for i := range reqs {
			h.unique += 2
			reqs[i].Unique = h.unique
			if reqs[i].Opcode == fuseproto.OpRelease {
				reqs[i].Fh = h.m.nextFh
			}
			out.Reset()
			h.m.Request(&h.ctx, h.now, &reqs[i], &out)
			for len(out.Reads)+len(out.Txns)+len(out.Chunks) > 0 {
				next.Reset()
				for _, rd := range out.Reads {
					got, seen := kvs[string(rd.Start)]
					if !seen {
						var r meta.ScanResult
						if err := h.s.Scan(rd.Start, rd.End, rd.Limit, &r); err != nil {
							t.Fatal(err)
						}
						for _, kv := range r.KVs {
							got = append(got, meta.KV{Key: bytes.Clone(kv.Key), Value: bytes.Clone(kv.Value)})
						}
						kvs[string(bytes.Clone(rd.Start))] = got
					}
					h.m.ReadDone(&h.ctx, rd.Token, got, &next)
				}
				for _, s := range out.Txns {
					h.m.TxnDone(&h.ctx, s.Token, &ok, nil, &next)
				}
				for _, c := range out.Chunks {
					done := chunkstore.Completion{Token: c.Token, Off: c.Off, Len: c.N, Status: chunkstore.StatusOK, Buf: core.NoBuf}
					if c.Read {
						done.Buf, buf = buf, core.NoBuf
					} else {
						done.Len = uint32(len(c.Data))
					}
					h.m.ChunkDone(&h.ctx, &done, &next)
					if c.Read {
						buf, _ = h.ctx.Pool.AllocZeroed(4096)
					}
				}
				out, next = next, out
			}
		}
	}
	if n := testing.AllocsPerRun(100, cycle); n != 0 {
		t.Fatalf("%v allocations a cycle of %d requests", n, len(reqs))
	}
}

// A FLUSH that finds its writes committed by the FLUSH ahead of it ends without taking the inode,
// and still hands it on: a WRITE queued behind both raises alloc_end and is answered.
func TestARequestQueuedBehindTwoCommitsRuns(t *testing.T) {
	h := newMount(t, Config{})
	ino, fh := h.create(meta.RootInode, "f")
	h.ok(h.write(ino, fh, 0, make([]byte, 4096)))
	h.held = true
	f1 := h.send(fuseproto.Request{Opcode: fuseproto.OpFlush, NodeID: ino, Fh: fh})
	f2 := h.send(fuseproto.Request{Opcode: fuseproto.OpFlush, NodeID: ino, Fh: fh})
	w := h.write(ino, fh, 40*core.ChunkSize, make([]byte, 4096))
	if len(h.sent) != 1 {
		t.Fatalf("%d Txns sent while the first flush held the inode", len(h.sent))
	}
	h.held = false
	h.answerTxn(h.sent[0])
	h.serve()
	h.ok(f1)
	h.ok(f2)
	h.ok(w)
	if n := h.m.Pending(); n != 0 {
		t.Fatalf("%d requests still in hand", n)
	}
}

// A GETATTR whose fetches keep going stale, each answered after the next GETATTR is read, is
// fetched again once and then answered with an attribute timeout of 0.
func TestAStreamOfRequestsCannotStarveAGetattr(t *testing.T) {
	h := newMount(t, Config{})
	ino, _ := h.create(meta.RootInode, "f")
	h.held, h.replies = true, nil
	first := h.send(fuseproto.Request{Opcode: fuseproto.OpGetattr, NodeID: ino})
	for range 50 {
		h.send(fuseproto.Request{Opcode: fuseproto.OpGetattr, NodeID: ino})
		rd := h.reads[0]
		h.reads = h.reads[1:]
		h.answerRead(rd)
		h.serve()
	}
	if va, _, _, err := fuseproto.DecodeAttrOut(h.ok(first)); err != nil || va != 0 {
		t.Fatalf("the first getattr answered with attr_valid %d: %v", va, err)
	}
}

// An open's fetch read before a close committed the size, and answered after the commit, is
// fetched again rather than taken: a read in the handle then sees the whole file.
func TestAFetchFromBeforeTheMachinesOwnCommitIsNotTaken(t *testing.T) {
	h := newMount(t, Config{})
	ino, fh := h.create(meta.RootInode, "f")
	h.ok(h.write(ino, fh, 0, []byte("hello")))
	h.held = true
	flush := h.send(fuseproto.Request{Opcode: fuseproto.OpFlush, NodeID: ino, Fh: fh})
	h.send(fuseproto.Request{Opcode: fuseproto.OpOpen, NodeID: ino})
	if len(h.sent) != 1 || len(h.reads) != 1 {
		t.Fatalf("%d Txns and %d reads in flight", len(h.sent), len(h.reads))
	}
	var before meta.ScanResult
	if err := h.s.Scan(h.reads[0].Start, h.reads[0].End, h.reads[0].Limit, &before); err != nil {
		t.Fatal(err)
	}
	old := slices.Clone(before.KVs)
	h.held = false
	h.answerTxn(h.sent[0])
	h.m.ReadDone(&h.ctx, h.reads[0].Token, old, &h.out)
	h.serve()
	h.ok(flush)
	if got := h.read(ino, fh, 0, 100); string(got) != "hello" {
		t.Fatalf("read after the open gave %q", got)
	}
}

// With a case index, a CREATE whose name folds to another node's file answers EEXIST; one whose
// name another node made and unlinked before the create read it is made again.
func TestACreateThatLosesItsNameAnswersByWhatHoldsIt(t *testing.T) {
	h := newMount(t, Config{CaseIndex: true})
	other := *h
	other.m, other.out, other.replies = New(Config{Volume: vol, CaseIndex: true, Session: 8, Mount: 1, SlotLimit: 1 << 20}), Output{}, nil
	theirs, _ := other.create(meta.RootInode, "f")
	h.errno(h.send(fuseproto.Request{Opcode: fuseproto.OpCreate, NodeID: meta.RootInode, Name: []byte("F"), Mode: uint32(meta.ModeFile | 0o644)}), EEXIST)
	h.held = true
	u := h.send(fuseproto.Request{Opcode: fuseproto.OpCreate, NodeID: meta.RootInode, Name: []byte("f"), Mode: uint32(meta.ModeFile | 0o644)})
	h.answerTxn(h.sent[0])
	h.serve()
	if len(h.reads) != 1 {
		t.Fatalf("a create that lost its name sent %d reads", len(h.reads))
	}
	other.ok(other.send(fuseproto.Request{Opcode: fuseproto.OpUnlink, NodeID: meta.RootInode, Name: []byte("f")}))
	h.held = false
	h.answerRead(h.reads[0])
	h.serve()
	e := h.entry(u)
	if e.NodeID == theirs || h.entry(h.lookup(meta.RootInode, "f")).NodeID != e.NodeID {
		t.Fatalf("the create answered inode %x, the unlinked file's %x", e.NodeID, theirs)
	}
}

// A mutating request's replay key carries the mount's oldest request in hand, so a lower request
// whose answer was lost keeps its record past a later one's Txn: its resend, an unlink applied
// once, answers as the first did rather than ENOENT.
func TestAResendAfterALostAnswerHasOneEffect(t *testing.T) {
	h := newMount(t, Config{})
	h.create(meta.RootInode, "a")
	h.create(meta.RootInode, "b")
	h.held = true
	unlink := func(name string) (uint64, TxnSend) {
		u := h.send(fuseproto.Request{Opcode: fuseproto.OpUnlink, NodeID: meta.RootInode, Name: []byte(name)})
		for len(h.reads) > 0 {
			rd := h.reads[0]
			h.reads = h.reads[1:]
			h.answerRead(rd)
			h.serve()
		}
		if len(h.sent) != 1 {
			t.Fatalf("unlink of %s sent %d Txns", name, len(h.sent))
		}
		s := h.sent[0]
		h.sent = nil
		return u, s
	}
	ua, sa := unlink("a")
	ub, sb := unlink("b")
	if sb.Txn.Oldest != ua {
		t.Fatalf("the later unlink's Txn names oldest %d, want %d", sb.Txn.Oldest, ua)
	}
	if r := h.apply(sa.Txn); r.FailedCheck != -1 {
		t.Fatal(r)
	}
	h.held = false
	h.answerTxn(sb)
	h.m.TxnDone(&h.ctx, sa.Token, nil, raft.ErrProposalDropped, &h.out)
	h.serve()
	h.ok(ub)
	h.ok(ua)
}
