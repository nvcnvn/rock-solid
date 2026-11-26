package fuseproto

import (
	"errors"
	"fmt"
)

// The kernel's side of the protocol, which the simulated kernel speaks: requests encoded, replies
// and notifications decoded.

// ErrReply is the error of a reply or notification whose length disagrees with its frame or layout.
var ErrReply = errors.New("fuseproto: reply does not match its layout")

// OutHeader is fuse_out_header: Error is 0 or a negated errno in a reply, the notification's code
// in a notification, whose Unique is 0.
type OutHeader struct {
	Error  int32
	Unique uint64
}

// Notification codes, an OutHeader's Error when its Unique is 0.
const (
	NotifyInvalInode = notifyInvalInode
	NotifyInvalEntry = notifyInvalEntry
	NotifyResend     = notifyResend
)

// AppendRequest appends r as the kernel sends it, for the opcodes a simulated kernel sends: INIT,
// LOOKUP, GETATTR, OPEN, READ, WRITE, FLUSH, RELEASE and FSYNC.
func AppendRequest(dst []byte, r *Request) []byte {
	start := len(dst)
	dst = le.AppendUint32(dst, 0)
	dst = le.AppendUint32(dst, uint32(r.Opcode))
	dst = le.AppendUint64(dst, r.Unique)
	dst = le.AppendUint64(dst, r.NodeID)
	dst = le.AppendUint32(dst, r.UID)
	dst = le.AppendUint32(dst, r.GID)
	dst = le.AppendUint32(dst, r.PID)
	dst = le.AppendUint32(dst, 0)
	switch r.Opcode {
	case OpInit:
		dst = le.AppendUint32(dst, r.Init.Major)
		dst = le.AppendUint32(dst, r.Init.Minor)
		dst = le.AppendUint32(dst, r.Init.MaxReadahead)
		dst = le.AppendUint32(dst, uint32(r.Init.Flags))
		dst = le.AppendUint32(dst, uint32(r.Init.Flags>>32))
		dst = append(dst, make([]byte, 44)...)
	case OpLookup:
		dst = append(append(dst, r.Name...), 0)
	case OpGetattr:
		dst = le.AppendUint64(dst, uint64(r.OpFlags))
		dst = le.AppendUint64(dst, r.Fh)
	case OpOpen:
		dst = le.AppendUint32(dst, r.Flags)
		dst = le.AppendUint32(dst, r.OpFlags)
	case OpRead, OpWrite:
		dst = le.AppendUint64(dst, r.Fh)
		dst = le.AppendUint64(dst, r.Offset)
		dst = le.AppendUint32(dst, r.Size)
		dst = le.AppendUint32(dst, r.OpFlags)
		dst = le.AppendUint64(dst, r.LockOwner)
		dst = le.AppendUint64(dst, uint64(r.Flags))
		dst = append(dst, r.Data...)
	case OpFlush:
		dst = le.AppendUint64(dst, r.Fh)
		dst = le.AppendUint64(dst, 0)
		dst = le.AppendUint64(dst, r.LockOwner)
	case OpRelease:
		dst = le.AppendUint64(dst, r.Fh)
		dst = le.AppendUint32(dst, r.Flags)
		dst = le.AppendUint32(dst, r.OpFlags)
		dst = le.AppendUint64(dst, r.LockOwner)
	case OpFsync:
		dst = le.AppendUint64(dst, r.Fh)
		dst = le.AppendUint64(dst, uint64(r.OpFlags))
	default:
		panic(fmt.Sprintf("fuseproto: no request encoder for opcode %d", r.Opcode))
	}
	le.PutUint32(dst[start:], uint32(len(dst)-start))
	return dst
}

// DecodeReply splits a frame the daemon writes into its header and body.
func DecodeReply(frame []byte) (OutHeader, []byte, error) {
	if len(frame) < OutHeaderSize || uint64(le.Uint32(frame)) != uint64(len(frame)) {
		return OutHeader{}, nil, ErrReply
	}
	return OutHeader{Error: int32(le.Uint32(frame[4:])), Unique: le.Uint64(frame[8:])}, frame[OutHeaderSize:], nil
}

// DecodeEntry decodes the body of a LOOKUP reply.
func DecodeEntry(b []byte) (Entry, error) {
	if len(b) != entrySize {
		return Entry{}, ErrReply
	}
	return decodeEntry(b), nil
}

// DecodeAttrOut decodes the body of a GETATTR reply: how long the attributes may be cached, and
// the attributes.
func DecodeAttrOut(b []byte) (valid uint64, validNsec uint32, a Attr, err error) {
	if len(b) != 16+attrSize {
		return 0, 0, Attr{}, ErrReply
	}
	return le.Uint64(b), le.Uint32(b[8:]), decodeAttr(b[16:]), nil
}

// DecodeOpen decodes the body of an OPEN reply.
func DecodeOpen(b []byte) (Open, error) {
	if len(b) != openSize {
		return Open{}, ErrReply
	}
	return Open{Fh: le.Uint64(b), Flags: le.Uint32(b[8:])}, nil
}

// DecodeWrite decodes the body of a WRITE reply, the bytes written.
func DecodeWrite(b []byte) (uint32, error) {
	if len(b) != 8 {
		return 0, ErrReply
	}
	return le.Uint32(b), nil
}

// DecodeInitOut decodes the body of an INIT reply.
func DecodeInitOut(b []byte) (InitOut, error) {
	if len(b) != initOutSize {
		return InitOut{}, ErrReply
	}
	return decodeInitOut(b), nil
}

// DecodeInvalInode decodes the body of an inval_inode notification.
func DecodeInvalInode(b []byte) (ino uint64, off, length int64, err error) {
	if len(b) != 24 {
		return 0, 0, 0, ErrReply
	}
	return le.Uint64(b), int64(le.Uint64(b[8:])), int64(le.Uint64(b[16:])), nil
}

func decodeAttr(b []byte) Attr {
	return Attr{
		Ino: le.Uint64(b), Size: le.Uint64(b[8:]), Blocks: le.Uint64(b[16:]),
		Atime: le.Uint64(b[24:]), Mtime: le.Uint64(b[32:]), Ctime: le.Uint64(b[40:]),
		AtimeNsec: le.Uint32(b[48:]), MtimeNsec: le.Uint32(b[52:]), CtimeNsec: le.Uint32(b[56:]),
		Mode: le.Uint32(b[60:]), Nlink: le.Uint32(b[64:]), UID: le.Uint32(b[68:]), GID: le.Uint32(b[72:]),
		Rdev: le.Uint32(b[76:]), Blksize: le.Uint32(b[80:]), Flags: le.Uint32(b[84:]),
	}
}

func decodeEntry(b []byte) Entry {
	return Entry{
		NodeID: le.Uint64(b), Generation: le.Uint64(b[8:]), EntryValid: le.Uint64(b[16:]), AttrValid: le.Uint64(b[24:]),
		EntryValidNsec: le.Uint32(b[32:]), AttrValidNsec: le.Uint32(b[36:]), Attr: decodeAttr(b[40:]),
	}
}

func decodeInitOut(o []byte) InitOut {
	return InitOut{
		Major:               le.Uint32(o),
		Minor:               le.Uint32(o[4:]),
		MaxReadahead:        le.Uint32(o[8:]),
		Flags:               uint64(le.Uint32(o[12:])) | uint64(le.Uint32(o[32:]))<<32,
		MaxBackground:       le.Uint16(o[16:]),
		CongestionThreshold: le.Uint16(o[18:]),
		MaxWrite:            le.Uint32(o[20:]),
		TimeGran:            le.Uint32(o[24:]),
		MaxPages:            le.Uint16(o[28:]),
		MapAlignment:        le.Uint16(o[30:]),
		MaxStackDepth:       le.Uint32(o[36:]),
		RequestTimeout:      le.Uint16(o[40:]),
	}
}
