// Package uring is the io_uring disk driver: a ring per loop over raw syscalls, its completions
// signalled on an eventfd in the loop's epoll set.
package uring

import (
	"unsafe"

	"github.com/nvcnvn/rock-solid/internal/core"
	"golang.org/x/sys/unix"
)

const (
	opWritev     = 2
	opFsync      = 3
	opReadFixed  = 4
	opWriteFixed = 5
	opRead       = 22
	opWrite      = 23
	opURingCmd   = 46

	sqeFixedFile  = 1 << 0
	sqeIOLink     = 1 << 2
	sqeIOHardlink = 1 << 3

	fsyncDatasync = 1
	rwfDsync      = 0x2
	// blockCmdDiscard is BLOCK_URING_CMD_DISCARD, _IO(0x12, 0), Linux 6.12 and later.
	blockCmdDiscard = 0x12 << 8

	// maxIOV is UIO_MAXIOV, the most iovecs one vectored write takes.
	maxIOV = 1024
	// maxIO is the largest read or write submitted, below Linux's MAX_RW_COUNT, past which a
	// write would come back short.
	maxIO = 1 << 30
)

// sqe is an io_uring submission queue entry as the kernel lays it out.
type sqe struct {
	opcode   uint8
	flags    uint8
	_        uint16
	fd       int32
	off      uint64
	addr     uint64
	len      uint32
	opFlags  uint32
	userData uint64
	bufIndex uint16
	_        uint16
	_        int32
	addr3    uint64
	_        uint64
}

// target is a disk as its submissions name it.
type target struct {
	// fd is the descriptor, or its index in the registered files when fixed.
	fd     int32
	fixed  bool
	blocks uint64
	// file is a regular file, where a trim completes at once and discards nothing.
	file bool
	// fua writes with FUA and issues no flush.
	fua bool
}

// regions are the pool's mapped regions, registered as fixed buffers when fixed.
type regions struct {
	base  []uintptr
	size  []uintptr
	fixed bool
	// zero is a block of zeros off the Go heap, written over the header of a segment a trim frees.
	zero uintptr
}

func newRegions(rs [][]byte) regions {
	var g regions
	for _, r := range rs {
		g.base = append(g.base, uintptr(unsafe.Pointer(&r[0])))
		g.size = append(g.size, uintptr(len(r)))
	}
	return g
}

func (g *regions) index(addr uintptr) uint16 {
	for i, b := range g.base {
		if addr >= b && addr-b < g.size[i] {
			return uint16(i)
		}
	}
	panic("uring: a pool buffer lies outside every registered region")
}

// op is one disk action from Execute to its completion.
type op struct {
	sqe  sqe
	io   core.IOID
	kind core.ActionKind
	// want is the bytes a read or write must move.
	want uint32
	// then is a flush linked behind this write under fua, which completes with it.
	then    core.IOID
	hasThen bool
	// quiet is a discard whose trim completes with the header write hard-linked behind it.
	quiet bool
	// live is an op between Execute and its completion.
	live bool
	// sent is an op copied into the submission queue, which the kernel may hold.
	sent bool
	// bufs are the pool buffers the kernel reads or writes.
	bufs []core.IOBuf
	iov  []unix.Iovec
	next int32
}

// held is the pool buffers the kernel may still read or write once the ring fails: those of an op
// copied into the submission queue.
func (o *op) held() []core.IOBuf {
	if !o.sent {
		return nil
	}
	return o.bufs
}

// on reports whether o's submission names t.
func (o *op) on(t *target) bool {
	return o.sqe.fd == t.fd && (o.sqe.flags&sqeFixedFile != 0) == t.fixed
}

// prepare fills o's submission for a on t. It returns false with the error a completes with at
// once: DiskInvalidRange for a range off the disk, a length not in whole blocks, over maxIO or
// past its buffer, or a vector whose lengths do not sum to Len or that is longer than the kernel
// takes; DiskOK for a flush under fua, which has nothing to submit. A trim of a file is the write
// of header; a trim of a device is a quiet discard, which header must follow.
func prepare(o *op, id uint64, a *core.Action, t *target, g *regions, pool core.BufPool) (bool, core.DiskError) {
	*o = op{io: a.IO, kind: a.Kind, live: true, bufs: o.bufs[:0], iov: o.iov[:0], next: o.next}
	s := &o.sqe
	s.userData = id
	s.fd = t.fd
	if t.fixed {
		s.flags = sqeFixedFile
	}
	if a.Kind == core.ActDiskFsync {
		if t.fua {
			return false, core.DiskOK
		}
		s.opcode, s.opFlags = opFsync, fsyncDatasync
		return true, core.DiskOK
	}
	bs := uint64(core.BlockSize)
	if a.Len == 0 || a.Len%bs != 0 || uint64(a.LBA) >= t.blocks || a.Len/bs > t.blocks-uint64(a.LBA) {
		return false, core.DiskInvalidRange
	}
	off := uint64(a.LBA) * bs
	switch a.Kind {
	case core.ActDiskTrim:
		if t.file {
			header(o, id, a, t, g)
			return true, core.DiskOK
		}
		s.opcode, s.flags, s.off, s.addr, s.addr3 = opURingCmd, s.flags|sqeIOHardlink, blockCmdDiscard, off, a.Len
		o.quiet = true
		return true, core.DiskOK
	case core.ActDiskRead, core.ActDiskWrite:
	default:
		return false, core.DiskInvalidRange
	}
	if a.Len > maxIO {
		return false, core.DiskInvalidRange
	}
	s.off, s.len, o.want = off, uint32(a.Len), uint32(a.Len)
	write := a.Kind == core.ActDiskWrite
	if write && t.fua {
		s.opFlags = rwfDsync
	}
	buf := a.Buf
	if write && len(a.Vec) > 0 {
		if len(a.Vec) > 1 {
			return vector(o, a, pool)
		}
		if uint64(a.Vec[0].Len) != a.Len {
			return false, core.DiskInvalidRange
		}
		buf = a.Vec[0].Buf
	}
	b := pool.Bytes(buf)
	if uint64(len(b)) < a.Len {
		return false, core.DiskInvalidRange
	}
	o.bufs = append(o.bufs, buf)
	addr := uintptr(unsafe.Pointer(&b[0]))
	s.addr = uint64(addr)
	switch {
	case g.fixed && write:
		s.opcode, s.bufIndex = opWriteFixed, g.index(addr)
	case g.fixed:
		s.opcode, s.bufIndex = opReadFixed, g.index(addr)
	case write:
		s.opcode = opWrite
	default:
		s.opcode = opRead
	}
	return true, core.DiskOK
}

// header fills o with the write of a block of zeros over the first block a trims, the segment
// header, so a freed segment reads as free whether or not a discard reads back as zeros.
func header(o *op, id uint64, a *core.Action, t *target, g *regions) {
	*o = op{io: a.IO, kind: core.ActDiskWrite, want: core.BlockSize, live: true, bufs: o.bufs[:0], iov: o.iov[:0], next: o.next}
	o.sqe = sqe{opcode: opWrite, fd: t.fd, off: uint64(a.LBA) * core.BlockSize, addr: uint64(g.zero), len: core.BlockSize, userData: id}
	if t.fixed {
		o.sqe.flags = sqeFixedFile
	}
}

// vector makes a write of several buffers one WRITEV, its iovecs held in o until it completes.
func vector(o *op, a *core.Action, pool core.BufPool) (bool, core.DiskError) {
	if len(a.Vec) > maxIOV {
		return false, core.DiskInvalidRange
	}
	var sum uint64
	for _, v := range a.Vec {
		b := pool.Bytes(v.Buf)
		if v.Len == 0 || v.Len%core.BlockSize != 0 || int(v.Len) > len(b) {
			return false, core.DiskInvalidRange
		}
		sum += uint64(v.Len)
		o.bufs = append(o.bufs, v.Buf)
		iv := unix.Iovec{Base: &b[0]}
		iv.SetLen(int(v.Len))
		o.iov = append(o.iov, iv)
	}
	if sum != a.Len {
		return false, core.DiskInvalidRange
	}
	o.sqe.opcode = opWritev
	o.sqe.addr = uint64(uintptr(unsafe.Pointer(&o.iov[0])))
	o.sqe.len = uint32(len(o.iov))
	return true, core.DiskOK
}

// complete appends the DiskDone for o's completion result res, and its linked flush's under fua: a
// negative errno, a read or write that moved other than the bytes asked, or a flush or trim with
// any result but 0, is DiskIO, and fails the linked flush too. A quiet op appends nothing.
func complete(evs []core.Event, o *op, res int32) []core.Event {
	if o.quiet {
		return evs
	}
	ev := core.Event{Kind: core.EvDiskDone, IO: o.io}
	rw := o.kind == core.ActDiskRead || o.kind == core.ActDiskWrite
	if res < 0 || rw && uint32(res) != o.want || !rw && res != 0 {
		ev.Err = core.DiskIO
	}
	evs = append(evs, ev)
	if o.hasThen {
		evs = append(evs, core.Event{Kind: core.EvDiskDone, IO: o.then, Err: ev.Err})
	}
	return evs
}
