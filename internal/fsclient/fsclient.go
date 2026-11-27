// Package fsclient is the daemon's sans-IO machine for one FUSE mount: decoded requests in;
// MetaStore reads and Txns, chunk-client calls and FUSE replies out.
package fsclient

import (
	"encoding/binary"
	"errors"

	"go.etcd.io/raft/v3"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/fuseproto"
	"github.com/nvcnvn/rock-solid/internal/meta"
)

// The errnos the machine answers with.
const (
	EPERM        fuseproto.Errno = 1
	ENOENT       fuseproto.Errno = 2
	EIO          fuseproto.Errno = 5
	EEXIST       fuseproto.Errno = 17
	EXDEV        fuseproto.Errno = 18
	ENOTDIR      fuseproto.Errno = 20
	EISDIR       fuseproto.Errno = 21
	EINVAL       fuseproto.Errno = 22
	EFBIG        fuseproto.Errno = 27
	ENOSPC       fuseproto.Errno = 28
	ENAMETOOLONG fuseproto.Errno = 36
	ENOTEMPTY    fuseproto.Errno = 39
	ENODATA      fuseproto.Errno = 61
	EOPNOTSUPP   fuseproto.Errno = 95
)

// oExcl is open(2)'s O_EXCL.
const oExcl = 0o200

// NameMax is the longest name a directory entry takes.
const NameMax = 255

// maxRaise is the most chunks one raise of alloc_end adds.
const maxRaise = 16

// Config is a mount's volume and session.
type Config struct {
	Volume    uint32
	Pool      core.PoolID
	CaseIndex bool
	Session   uint64
	Mount     uint32
	// SlotLimit is where the volume's shard's slots end.
	SlotLimit uint32
	// Strict sets every timeout handed the kernel to 0.
	Strict bool
}

// Read asks for the keys of [Start, End), at most Limit, from the shard leader's lease reads.
type Read struct {
	Token      uint64
	Start, End []byte
	Limit      int
}

// TxnSend is a Txn to send, which stays unchanged until TxnDone settles Token.
type TxnSend struct {
	Token uint64
	Txn   *meta.Txn
}

// ChunkCall is a chunk-client call: Read N bytes at Off of File, or write Data there, which
// aliases the request's frame or the machine's memory until the call is made.
type ChunkCall struct {
	Token uint64
	Read  bool
	File  chunkstore.File
	Off   uint64
	N     uint32
	Data  []byte
}

// Output is what one call into the machine emits; the driver makes its calls in order.
type Output struct {
	// Replies holds FUSE reply frames back to back, each starting with its length.
	Replies []byte
	Reads   []Read
	Txns    []TxnSend
	Chunks  []ChunkCall
}

// Reset empties the output for reuse.
func (o *Output) Reset() {
	o.Replies, o.Reads, o.Txns, o.Chunks = o.Replies[:0], o.Reads[:0], o.Txns[:0], o.Chunks[:0]
}

// plants are bugs a test plants.
type plants struct {
	// raiseAfterWrite raises alloc_end after the chunk write past it rather than before.
	raiseAfterWrite bool
}

// Machine serves one mount. Each request names its inodes by FUSE node ID, which is the inode ID.
type Machine struct {
	cfg     Config
	alloc   meta.Allocator
	nodes   map[uint64]*node
	handles map[uint64]*handle
	freeFh  []*handle
	nextFh  uint64
	// ops are the requests in hand, the oldest first.
	ops     []*op
	free    []*op
	waits   map[uint64]wait
	next    uint64
	seq     uint64
	blocked []*op
	plant   plants
}

// node is what the machine knows of an inode.
type node struct {
	attr meta.Attr
	// parent is a directory's, read with its attributes.
	parent uint64
	// last is the sequence of the last request read for the inode.
	last    uint64
	lookups uint64
	opens   int
	// opened is the attributes its last open saw, which say whether the kernel's pages are current.
	opened meta.Attr
	// written is the end of the writes acknowledged since the size was last committed; wrote and
	// lastWrite say whether there were any and the wall-clock time of the last.
	written   uint64
	wrote     bool
	lastWrite int64
	step      uint64
	// busy is the request changing its attributes; queue waits for it.
	busy  *op
	queue []*op
}

type handle struct {
	ino uint64
	// count is the entries a directory handle returned, the last of them named last.
	count uint64
	last  []byte
}

type wait struct {
	o    *op
	slot int
}

// got is one read's answer: the first key's value, and every key and value for a scan.
type got struct {
	v  []byte
	ok bool
}

// The internal requests: inode and slot reservations, which no FUSE request names.
const (
	opReserveInodes fuseproto.Opcode = 1<<32 - 1 - iota
	opReadFree
	opReserveSlots
)

type op struct {
	req fuseproto.Request
	now int64
	seq uint64
	// step is where the request is; pending counts the answers it waits for.
	step    int
	pending int
	got     [4]got
	scan    []meta.KV
	vbuf    []byte
	names   []byte
	data    []byte
	reply   []byte
	fetch   uint64
	// resolved is a LOOKUP that has read its entry; again a request retried after a check failed.
	resolved bool
	again    bool
	// refetched is a request that fetched its attributes again once they went stale, and expire
	// one answering with stale attributes, which the kernel is told to hold for no time.
	refetched, expire bool
	over     uint64
	omode    uint64
	wtime    int64
	fields   [6]meta.Field
	child    uint64
	mode     uint64
	fresh    bool
	slot     uint32
	attr     meta.Attr
	pend     meta.Purge
	mreq     meta.Request
	fs       meta.FS
	errno    fuseproto.Errno
}

// New is a mount's machine.
func New(cfg Config) *Machine {
	m := &Machine{cfg: cfg, nodes: map[uint64]*node{}, handles: map[uint64]*handle{}, waits: map[uint64]wait{}}
	m.nodes[meta.RootInode] = &node{lookups: 1}
	return m
}

// Pending is the requests in hand.
func (m *Machine) Pending() int { return len(m.ops) }

func (m *Machine) token(o *op, slot int) uint64 {
	m.next++
	m.waits[m.next] = wait{o, slot}
	o.pending++
	return m.next
}

func (m *Machine) node(ino uint64) *node {
	n := m.nodes[ino]
	if n == nil {
		n = &node{}
		m.nodes[ino] = n
	}
	return n
}

// handle opens a handle of ino and returns its number.
func (m *Machine) handle(ino uint64) uint64 {
	var h *handle
	if k := len(m.freeFh); k > 0 {
		h, m.freeFh = m.freeFh[k-1], m.freeFh[:k-1]
		*h = handle{last: h.last[:0]}
	} else {
		h = &handle{}
	}
	h.ino = ino
	m.nextFh++
	m.handles[m.nextFh] = h
	return m.nextFh
}

func (m *Machine) newOp() *op {
	var o *op
	if k := len(m.free); k > 0 {
		o, m.free = m.free[k-1], m.free[:k-1]
	} else {
		o = &op{}
	}
	m.ops = append(m.ops, o)
	return o
}

// done ends o; its reply is already out.
func (m *Machine) done(o *op) {
	for i, p := range m.ops {
		if p == o {
			m.ops = append(m.ops[:i], m.ops[i+1:]...)
			break
		}
	}
	*o = op{scan: o.scan[:0], vbuf: o.vbuf[:0], names: o.names[:0], data: o.data[:0], reply: o.reply[:0], fs: o.fs}
	m.free = append(m.free, o)
}

func (m *Machine) fail(o *op, errno fuseproto.Errno, out *Output) {
	out.Replies = fuseproto.AppendError(out.Replies, o.req.Unique, errno)
	m.end(o, out)
}

func (m *Machine) empty(o *op, out *Output) {
	out.Replies = fuseproto.AppendEmpty(out.Replies, o.req.Unique)
	m.end(o, out)
}

// end ends o once its reply is out, handing its inode to the next request waiting for it.
func (m *Machine) end(o *op, out *Output) {
	m.unlock(o, out)
	m.done(o)
}

// lock makes o the one request changing its inode's attributes; false queues it behind the holder.
func (m *Machine) lock(o *op, n *node) bool {
	if n.busy == nil || n.busy == o {
		n.busy = o
		return true
	}
	n.queue = append(n.queue, o)
	return false
}

// unlock hands n's lock on until a queued request takes it: one may end without it, as a commit
// finding nothing left to commit does.
func (m *Machine) unlock(o *op, out *Output) {
	n := m.nodes[o.req.NodeID]
	if n == nil || n.busy != o {
		return
	}
	n.busy = nil
	for n.busy == nil && len(n.queue) > 0 {
		p := n.queue[0]
		n.queue = n.queue[:copy(n.queue, n.queue[1:])]
		m.run(p, out)
	}
}

// Request takes a decoded request read at wall-clock time now, in nanoseconds since the epoch;
// its frame need live only through the call.
func (m *Machine) Request(ctx *core.Context, now int64, r *fuseproto.Request, out *Output) {
	switch r.Opcode {
	case fuseproto.OpForget:
		m.forget(r.NodeID, r.NLookup)
		return
	case fuseproto.OpBatchForget:
		for i := range r.ForgetCount() {
			m.forget(r.Forget(i))
		}
		return
	}
	m.seq++
	o := m.newOp()
	o.req, o.now, o.seq = *r, now, m.seq
	o.names = append(append(o.names[:0], r.Name...), r.NewName...)
	o.req.Name, o.req.NewName = o.names[:len(r.Name):len(r.Name)], o.names[len(r.Name):]
	o.req.Data = nil
	if r.Opcode != fuseproto.OpLookup && r.NodeID != 0 {
		m.node(r.NodeID).last = m.seq
	}
	m.mutating(o)
	if r.Opcode == fuseproto.OpWrite {
		m.write(o, r.Data, out)
		return
	}
	m.run(o, out)
}

// mutating gives o's Txns its replay key.
func (m *Machine) mutating(o *op) {
	o.mreq = meta.Request{Session: m.cfg.Session, Mount: m.cfg.Mount, ID: o.req.Unique &^ fuseproto.UniqueResend}
	o.fs.Volume, o.fs.CaseIndex, o.fs.Req = m.cfg.Volume, m.cfg.CaseIndex, &o.mreq
}

func (m *Machine) forget(ino, n uint64) {
	nd := m.nodes[ino]
	if nd == nil || ino == meta.RootInode {
		return
	}
	nd.lookups -= min(n, nd.lookups)
	m.drop(ino, nd)
}

func (m *Machine) drop(ino uint64, n *node) {
	if n.lookups == 0 && n.opens == 0 && n.busy == nil && !n.wrote && ino != meta.RootInode {
		delete(m.nodes, ino)
	}
}

// oldest is the lowest request ID in hand.
func (m *Machine) oldest() uint64 {
	low := uint64(1<<64 - 1)
	for _, o := range m.ops {
		if o.req.Opcode < opReserveSlots {
			low = min(low, o.req.Unique&^fuseproto.UniqueResend)
		}
	}
	return low
}

func (m *Machine) sendTxn(o *op, out *Output) {
	if o.fs.Txn.Replay != nil {
		o.mreq.Oldest = m.oldest()
		o.fs.Txn.Oldest = o.mreq.Oldest
	}
	out.Txns = append(out.Txns, TxnSend{Token: m.token(o, 0), Txn: &o.fs.Txn})
}

// bound appends fs/{volume}/{inode}/{tag}, a range bound no key need equal.
func (m *Machine) bound(dst []byte, inode uint64, tag byte) []byte {
	return append(binary.BigEndian.AppendUint64(binary.BigEndian.AppendUint32(append(dst, meta.SpaceFS), m.cfg.Volume), inode), tag)
}

func (m *Machine) key(dst []byte, inode uint64, tag byte, name []byte) []byte {
	k := meta.Key{Space: meta.SpaceFS, Volume: m.cfg.Volume, Inode: inode, Tag: tag, Name: name}
	if tag >= meta.SlotNext {
		k.Inode, k.Name, k.ID = inode>>meta.SeqBits<<meta.SeqBits, nil, inode
	}
	dst, _ = meta.AppendKey(dst, &k)
	return dst
}

// get reads one key into o's slot.
func (m *Machine) get(o *op, slot int, inode uint64, tag byte, name []byte, out *Output) {
	start := len(o.vbuf)
	o.vbuf = m.key(o.vbuf, inode, tag, name)
	k := o.vbuf[start:len(o.vbuf):len(o.vbuf)]
	o.vbuf = append(append(o.vbuf, k...), 0)
	out.Reads = append(out.Reads, Read{Token: m.token(o, slot), Start: k, End: o.vbuf[start+len(k) : len(o.vbuf) : len(o.vbuf)], Limit: 1})
}

// ReadDone answers Read token with the keys found and their values, which need live only through the call.
func (m *Machine) ReadDone(ctx *core.Context, token uint64, kvs []meta.KV, out *Output) {
	w, ok := m.waits[token]
	if !ok {
		return
	}
	delete(m.waits, token)
	o := w.o
	o.pending--
	if w.slot < 0 {
		for _, kv := range kvs {
			start := len(o.vbuf)
			o.vbuf = append(append(o.vbuf, kv.Key...), kv.Value...)
			o.scan = append(o.scan, meta.KV{Key: o.vbuf[start : start+len(kv.Key)], Value: o.vbuf[start+len(kv.Key) : len(o.vbuf)]})
		}
	} else if len(kvs) > 0 {
		start := len(o.vbuf)
		o.vbuf = append(o.vbuf, kvs[0].Value...)
		o.got[w.slot] = got{o.vbuf[start:len(o.vbuf):len(o.vbuf)], true}
	} else {
		o.got[w.slot] = got{}
	}
	m.run(o, out)
}

// errResend reports a Txn answer that promises nothing about whether it applied, so it goes again.
func errResend(err error) bool {
	return errors.Is(err, raft.ErrProposalDropped) || errors.Is(err, meta.ErrOverloaded)
}

// TxnDone settles TxnSend token with its Result or error.
func (m *Machine) TxnDone(ctx *core.Context, token uint64, r *meta.Result, err error, out *Output) {
	w, ok := m.waits[token]
	if !ok {
		return
	}
	delete(m.waits, token)
	o := w.o
	o.pending--
	if errResend(err) {
		m.sendTxn(o, out)
		return
	}
	switch o.req.Opcode {
	case opReserveInodes:
		if err == nil && r.FailedCheck == -1 && len(r.Updated) == 1 {
			m.alloc.InodesReserved(o.slot, r.Updated[0])
		} else {
			m.alloc.InodesFailed(o.slot)
		}
		m.internalDone(o, out)
		return
	case opReserveSlots:
		if err == nil && r.FailedCheck == -1 && len(r.Updated) == 1 {
			m.alloc.SlotsReserved(r.Updated[0], m.cfg.SlotLimit)
		} else {
			m.alloc.SlotsFailed()
		}
		m.internalDone(o, out)
		return
	}
	if err != nil {
		m.fail(o, EIO, out)
		return
	}
	if f := r.FailedCheck; f >= 0 {
		c := &o.fs.Txn.Ops[f]
		switch {
		case !c.Final:
			o.step, o.again = 0, true
			m.run(o, out)
		case c.Kind == meta.OpCheckEmpty:
			m.fail(o, ENOTEMPTY, out)
		case c.Absent && o.req.Opcode == fuseproto.OpCreate && o.req.Flags&oExcl == 0:
			o.step = createOpen
			m.run(o, out)
		case c.Absent:
			m.fail(o, EEXIST, out)
		default:
			m.fail(o, ENOENT, out)
		}
		return
	}
	o.step = -1
	m.run(o, out)
}

func (m *Machine) internalDone(o *op, out *Output) {
	m.done(o)
	b := m.blocked
	m.blocked = nil
	for _, p := range b {
		m.run(p, out)
	}
}

func (m *Machine) internal(kind fuseproto.Opcode, slot uint32, out *Output) *op {
	o := m.newOp()
	o.req.Opcode, o.slot = kind, slot
	o.fs.Volume, o.fs.CaseIndex, o.fs.Req = m.cfg.Volume, false, nil
	switch kind {
	case opReserveInodes:
		_ = o.fs.ReserveInodes(slot)
		m.sendTxn(o, out)
	case opReadFree:
		m.get(o, 0, 0, meta.SlotFree, nil, out)
	case opReserveSlots:
		_ = o.fs.ReserveSlots(0)
		m.sendTxn(o, out)
	}
	return o
}

func (m *Machine) needSlots(out *Output) {
	if !m.alloc.NeedSlots() {
		return
	}
	if m.alloc.Free() {
		m.internal(opReserveSlots, 0, out)
	} else {
		m.internal(opReadFree, 0, out)
	}
}

// inode takes a new inode ID in dir's slot, or for a directory sequence 1 of a fresh slot;
// false holds the request until a reservation lands.
func (m *Machine) inode(o *op, dir bool, out *Output) bool {
	if dir {
		s, ok := m.alloc.Slot()
		m.needSlots(out)
		if ok {
			o.child, o.fresh, o.slot = uint64(s)<<meta.SeqBits|1, true, s
			return true
		}
		if !m.alloc.NoSlots() {
			m.blocked = append(m.blocked, o)
			return false
		}
	}
	slot := uint32(o.req.NodeID >> meta.SeqBits)
	id, ok := m.alloc.Inode(slot)
	if m.alloc.NeedInodes(slot) {
		m.internal(opReserveInodes, slot, out)
	}
	if !ok {
		m.blocked = append(m.blocked, o)
		return false
	}
	o.child = id
	return true
}

// run moves o on from its step, once every answer it waits for is in.
func (m *Machine) run(o *op, out *Output) {
	if o.pending > 0 {
		return
	}
	if o.step == 0 {
		o.vbuf, o.scan = o.vbuf[:0], o.scan[:0]
	}
	switch o.req.Opcode {
	case opReadFree:
		m.alloc.FreeRead(o.got[0].ok)
		if m.alloc.Free() {
			m.done(o)
			m.internal(opReserveSlots, 0, out)
		} else {
			m.internalDone(o, out)
		}
	case fuseproto.OpLookup:
		m.lookup(o, out)
	case fuseproto.OpGetattr, fuseproto.OpStatx:
		m.getattr(o, out)
	case fuseproto.OpOpen:
		m.open(o, out)
	case fuseproto.OpOpendir:
		out.Replies = fuseproto.AppendOpen(out.Replies, o.req.Unique, fuseproto.Open{Fh: m.handle(o.req.NodeID)})
		m.done(o)
	case fuseproto.OpReaddirplus:
		m.readdir(o, out)
	case fuseproto.OpRelease, fuseproto.OpReleasedir:
		if h := m.handles[o.req.Fh]; h != nil {
			delete(m.handles, o.req.Fh)
			m.freeFh = append(m.freeFh, h)
			if n := m.nodes[h.ino]; n != nil && o.req.Opcode == fuseproto.OpRelease {
				n.opens--
				defer m.drop(h.ino, n)
			}
		}
		m.empty(o, out)
	case fuseproto.OpStatfs:
		out.Replies = fuseproto.AppendStatfs(out.Replies, o.req.Unique, &fuseproto.Statfs{Blocks: 1 << 40, Bfree: 1 << 40, Bavail: 1 << 40, Files: 1 << 40, Ffree: 1 << 40, Bsize: core.BlockSize, NameLen: NameMax, Frsize: core.BlockSize})
		m.done(o)
	case fuseproto.OpCreate, fuseproto.OpMkdir:
		m.create(o, out)
	case fuseproto.OpUnlink, fuseproto.OpRmdir:
		m.unlink(o, out)
	case fuseproto.OpRename:
		m.rename(o, out)
	case fuseproto.OpSetattr:
		m.setattr(o, out)
	case fuseproto.OpRead:
		m.read(o, out)
	case fuseproto.OpWrite:
		m.raised(o, out)
	case fuseproto.OpFlush, fuseproto.OpFsync:
		m.commit(o, out)
	default:
		m.perCall(o, out)
	}
}

// perCall answers the opcodes whose ENOSYS the kernel would remember for the mount's life.
func (m *Machine) perCall(o *op, out *Output) {
	switch o.req.Opcode {
	case fuseproto.OpAccess, fuseproto.OpFsyncdir:
		m.empty(o, out)
	case fuseproto.OpRename2:
		m.fail(o, EINVAL, out)
	case fuseproto.OpTmpfile, fuseproto.OpSetxattr:
		m.fail(o, EOPNOTSUPP, out)
	case fuseproto.OpLink:
		m.fail(o, EPERM, out)
	case fuseproto.OpGetxattr, fuseproto.OpRemovexattr:
		m.fail(o, ENODATA, out)
	case fuseproto.OpListxattr:
		if o.req.Size == 0 {
			out.Replies = fuseproto.AppendXattrSize(out.Replies, o.req.Unique, 0)
			m.done(o)
		} else {
			m.empty(o, out)
		}
	default:
		m.fail(o, fuseproto.ENOSYS, out)
	}
}

// timeouts are an entry's and attributes' cache times for mode, in seconds.
func (m *Machine) timeouts(mode uint64) (entry, attr uint64) {
	if m.cfg.Strict {
		return 0, 0
	}
	if mode&meta.ModeType == meta.ModeFile {
		return 0, 1
	}
	return 1, 1
}

func split(t int64) (uint64, uint32) {
	s, ns := t/1e9, t%1e9
	if ns < 0 {
		s, ns = s-1, ns+1e9
	}
	return uint64(s), uint32(ns)
}

func fuseAttr(ino uint64, a *meta.Attr) fuseproto.Attr {
	f := fuseproto.Attr{Ino: ino, Size: a.Size, Blocks: (a.Size + 511) / 512, Mode: uint32(a.Mode), Nlink: uint32(a.Nlink), UID: uint32(a.UID), GID: uint32(a.GID), Blksize: core.BlockSize}
	f.Atime, f.AtimeNsec = split(a.Atime)
	f.Mtime, f.MtimeNsec = split(a.Mtime)
	f.Ctime, f.CtimeNsec = split(a.Ctime)
	return f
}

func (m *Machine) entry(ino uint64, a *meta.Attr) fuseproto.Entry {
	ev, av := m.timeouts(a.Mode)
	return fuseproto.Entry{NodeID: ino, EntryValid: ev, AttrValid: av, Attr: fuseAttr(ino, a)}
}
