// Package fuseproto encodes and decodes the /dev/fuse protocol of Linux 6.18, FUSE 7.45, on
// little-endian hosts: requests the kernel sends, the daemon's replies and notifications, and a
// mount's negotiated INIT parameters. Decoded names and data are slices of the caller's frame.
package fuseproto

import (
	"bytes"
	"encoding/binary"
	"errors"
)

var le = binary.LittleEndian

// Opcode is a request's operation.
type Opcode uint32

// Opcodes the codec decodes.
const (
	OpLookup      Opcode = 1
	OpForget      Opcode = 2
	OpGetattr     Opcode = 3
	OpSetattr     Opcode = 4
	OpMkdir       Opcode = 9
	OpUnlink      Opcode = 10
	OpRmdir       Opcode = 11
	OpRename      Opcode = 12
	OpLink        Opcode = 13
	OpOpen        Opcode = 14
	OpRead        Opcode = 15
	OpWrite       Opcode = 16
	OpStatfs      Opcode = 17
	OpRelease     Opcode = 18
	OpFsync       Opcode = 20
	OpSetxattr    Opcode = 21
	OpGetxattr    Opcode = 22
	OpListxattr   Opcode = 23
	OpRemovexattr Opcode = 24
	OpFlush       Opcode = 25
	OpInit        Opcode = 26
	OpOpendir     Opcode = 27
	OpReleasedir  Opcode = 29
	OpFsyncdir    Opcode = 30
	OpAccess      Opcode = 34
	OpCreate      Opcode = 35
	OpBatchForget Opcode = 42
	OpReaddirplus Opcode = 44
	OpRename2     Opcode = 45
	OpTmpfile     Opcode = 51
	OpStatx       Opcode = 52
)

// NoReply reports whether the kernel expects no reply to op.
func (op Opcode) NoReply() bool { return op == OpForget || op == OpBatchForget }

// Sizes of the fixed parts of frames.
const (
	InHeaderSize  = 40
	OutHeaderSize = 16
	forgetOneSize = 16
	initInSize    = 20
	// setxattrInSize is FUSE_COMPAT_SETXATTR_IN_SIZE, the body the kernel sends while the mount
	// does not negotiate FUSE_SETXATTR_EXT.
	setxattrInSize = 8
)

// UniqueResend is the bit of a request's unique the kernel sets when it resends the request
// after FUSE_NOTIFY_RESEND. A request's identity, its replay key, is Unique &^ UniqueResend; a
// reply echoes Unique whole.
const UniqueResend = 1 << 63

// Decode errors. Every error but ErrShortHeader leaves the request's header set, so the caller can
// answer it: ENOSYS for ErrOpcode, EIO for the rest.
var (
	ErrShortHeader = errors.New("fuseproto: frame shorter than a request header")
	ErrLength      = errors.New("fuseproto: header length disagrees with the frame")
	ErrBody        = errors.New("fuseproto: body does not match its opcode's layout")
	ErrName        = errors.New("fuseproto: name without its terminating zero")
	ErrOpcode      = errors.New("fuseproto: unknown opcode")
)

// Request is one decoded request; fields its opcode does not carry are zero.
type Request struct {
	Opcode Opcode
	Unique uint64
	NodeID uint64
	UID    uint32
	GID    uint32
	PID    uint32

	// Name is the entry LOOKUP, CREATE, TMPFILE, UNLINK, MKDIR, RMDIR and LINK act on, and the
	// old name of RENAME and RENAME2; for GETXATTR, SETXATTR and REMOVEXATTR, the attribute's name.
	Name    []byte
	NewName []byte
	NewDir  uint64
	// OldNodeID is the node LINK gives a new name.
	OldNodeID uint64
	// Data is WRITE's bytes or SETXATTR's value, in the frame where the kernel put them.
	Data []byte

	Fh     uint64
	Offset uint64
	// Size is the length of READ, READDIRPLUS, WRITE and SETXATTR, or the buffer GETXATTR and
	// LISTXATTR fill, 0 asking for the length only.
	Size uint32
	// Flags are the open(2) flags of OPEN, OPENDIR, CREATE, TMPFILE, READ, WRITE, RELEASE and
	// RELEASEDIR.
	Flags uint32
	// OpFlags are the opcode's own flags: FUSE_OPEN_*, FUSE_READ_*, FUSE_WRITE_*, FUSE_RELEASE_*,
	// FUSE_GETATTR_* (GETATTR, STATX), FUSE_FSYNC_* (FSYNC, FSYNCDIR), RENAME2's RENAME_* or SETXATTR's XATTR_*.
	OpFlags   uint32
	LockOwner uint64
	Mode      uint32
	Umask     uint32
	// Mask is ACCESS's MAY_* bits.
	Mask uint32
	// StatxFlags and StatxMask are statx(2)'s AT_STATX_* flags and STATX_* mask.
	StatxFlags uint32
	StatxMask  uint32

	NLookup uint64
	// Forgets holds BATCH_FORGET's records; Forget reads them.
	Forgets []byte

	Setattr Setattr
	Init    InitIn
}

// Setattr is the body of SETATTR; Valid's FATTR_* bits say which fields are set.
type Setattr struct {
	Valid     uint32
	Fh        uint64
	Size      uint64
	LockOwner uint64
	Atime     uint64
	Mtime     uint64
	Ctime     uint64
	AtimeNsec uint32
	MtimeNsec uint32
	CtimeNsec uint32
	Mode      uint32
	UID       uint32
	GID       uint32
}

// InitIn is what the kernel offers in INIT; Flags joins flags and flags2.
type InitIn struct {
	Major        uint32
	Minor        uint32
	MaxReadahead uint32
	Flags        uint64
}

// ForgetCount returns how many records BATCH_FORGET carries.
func (r *Request) ForgetCount() int { return len(r.Forgets) / forgetOneSize }

// Forget returns BATCH_FORGET's record i.
func (r *Request) Forget(i int) (nodeID, nlookup uint64) {
	b := r.Forgets[i*forgetOneSize:]
	return le.Uint64(b), le.Uint64(b[8:])
}

// Decode decodes frame, one whole read from /dev/fuse, into r.
func Decode(frame []byte, r *Request) error {
	*r = Request{}
	if len(frame) < InHeaderSize {
		return ErrShortHeader
	}
	r.Opcode = Opcode(le.Uint32(frame[4:]))
	r.Unique = le.Uint64(frame[8:])
	r.NodeID = le.Uint64(frame[16:])
	r.UID = le.Uint32(frame[24:])
	r.GID = le.Uint32(frame[28:])
	r.PID = le.Uint32(frame[32:])
	ext := int(le.Uint16(frame[36:])) * 8
	if uint64(le.Uint32(frame)) != uint64(len(frame)) || ext > len(frame)-InHeaderSize {
		return ErrLength
	}
	return r.decodeBody(frame[InHeaderSize : len(frame)-ext])
}

func (r *Request) decodeBody(b []byte) error {
	var err error
	switch r.Opcode {
	case OpLookup, OpUnlink, OpRmdir, OpRemovexattr:
		r.Name, err = lastName(b)
	case OpForget:
		if len(b) != 8 {
			return ErrBody
		}
		r.NLookup = le.Uint64(b)
	case OpBatchForget:
		if len(b) < 8 || uint64(le.Uint32(b))*forgetOneSize != uint64(len(b)-8) {
			return ErrBody
		}
		r.Forgets = b[8:]
	case OpGetattr:
		if len(b) != 16 {
			return ErrBody
		}
		r.OpFlags = le.Uint32(b)
		r.Fh = le.Uint64(b[8:])
	case OpSetattr:
		if len(b) != 88 {
			return ErrBody
		}
		r.Setattr = Setattr{
			Valid:     le.Uint32(b),
			Fh:        le.Uint64(b[8:]),
			Size:      le.Uint64(b[16:]),
			LockOwner: le.Uint64(b[24:]),
			Atime:     le.Uint64(b[32:]),
			Mtime:     le.Uint64(b[40:]),
			Ctime:     le.Uint64(b[48:]),
			AtimeNsec: le.Uint32(b[56:]),
			MtimeNsec: le.Uint32(b[60:]),
			CtimeNsec: le.Uint32(b[64:]),
			Mode:      le.Uint32(b[68:]),
			UID:       le.Uint32(b[76:]),
			GID:       le.Uint32(b[80:]),
		}
	case OpMkdir:
		if len(b) < 8 {
			return ErrBody
		}
		r.Mode = le.Uint32(b)
		r.Umask = le.Uint32(b[4:])
		r.Name, err = lastName(b[8:])
	case OpCreate, OpTmpfile:
		if len(b) < 16 {
			return ErrBody
		}
		r.Flags = le.Uint32(b)
		r.Mode = le.Uint32(b[4:])
		r.Umask = le.Uint32(b[8:])
		r.OpFlags = le.Uint32(b[12:])
		r.Name, err = lastName(b[16:])
	case OpRename:
		if len(b) < 8 {
			return ErrBody
		}
		r.NewDir = le.Uint64(b)
		err = r.decodeNames(b[8:])
	case OpRename2:
		if len(b) < 16 {
			return ErrBody
		}
		r.NewDir = le.Uint64(b)
		r.OpFlags = le.Uint32(b[8:])
		err = r.decodeNames(b[16:])
	case OpLink:
		if len(b) < 8 {
			return ErrBody
		}
		r.OldNodeID = le.Uint64(b)
		r.Name, err = lastName(b[8:])
	case OpGetxattr:
		if len(b) < 8 {
			return ErrBody
		}
		r.Size = le.Uint32(b)
		r.Name, err = lastName(b[8:])
	case OpListxattr:
		if len(b) != 8 {
			return ErrBody
		}
		r.Size = le.Uint32(b)
	case OpSetxattr:
		if len(b) < setxattrInSize {
			return ErrBody
		}
		r.Size = le.Uint32(b)
		r.OpFlags = le.Uint32(b[4:])
		if r.Name, r.Data, err = name(b[setxattrInSize:]); err == nil && uint64(r.Size) != uint64(len(r.Data)) {
			err = ErrBody
		}
	case OpAccess:
		if len(b) != 8 {
			return ErrBody
		}
		r.Mask = le.Uint32(b)
	case OpStatx:
		if len(b) != 24 {
			return ErrBody
		}
		r.OpFlags = le.Uint32(b)
		r.Fh = le.Uint64(b[8:])
		r.StatxFlags = le.Uint32(b[16:])
		r.StatxMask = le.Uint32(b[20:])
	case OpOpen, OpOpendir:
		if len(b) != 8 {
			return ErrBody
		}
		r.Flags = le.Uint32(b)
		r.OpFlags = le.Uint32(b[4:])
	case OpRead, OpReaddirplus:
		if len(b) != 40 {
			return ErrBody
		}
		r.decodeIO(b)
	case OpWrite:
		if len(b) < 40 {
			return ErrBody
		}
		r.decodeIO(b)
		if uint64(r.Size) != uint64(len(b)-40) {
			return ErrBody
		}
		r.Data = b[40:]
	case OpFlush:
		if len(b) != 24 {
			return ErrBody
		}
		r.Fh = le.Uint64(b)
		r.LockOwner = le.Uint64(b[16:])
	case OpRelease, OpReleasedir:
		if len(b) != 24 {
			return ErrBody
		}
		r.Fh = le.Uint64(b)
		r.Flags = le.Uint32(b[8:])
		r.OpFlags = le.Uint32(b[12:])
		r.LockOwner = le.Uint64(b[16:])
	case OpFsync, OpFsyncdir:
		if len(b) != 16 {
			return ErrBody
		}
		r.Fh = le.Uint64(b)
		r.OpFlags = le.Uint32(b[8:])
	case OpStatfs:
		if len(b) != 0 {
			return ErrBody
		}
	case OpInit:
		// INIT's length follows the kernel's version, not the minor this codec speaks.
		r.Init, err = decodeInitIn(b)
	default:
		return ErrOpcode
	}
	return err
}

// decodeNames decodes RENAME's and RENAME2's old and new names.
func (r *Request) decodeNames(b []byte) error {
	var rest []byte
	var err error
	if r.Name, rest, err = name(b); err == nil {
		r.NewName, err = lastName(rest)
	}
	return err
}

// decodeIO decodes fuse_read_in and fuse_write_in, which share their layout.
func (r *Request) decodeIO(b []byte) {
	r.Fh = le.Uint64(b)
	r.Offset = le.Uint64(b[8:])
	r.Size = le.Uint32(b[16:])
	r.OpFlags = le.Uint32(b[20:])
	r.LockOwner = le.Uint64(b[24:])
	r.Flags = le.Uint32(b[32:])
}

func decodeInitIn(b []byte) (InitIn, error) {
	if len(b) < 16 {
		return InitIn{}, ErrBody
	}
	in := InitIn{Major: le.Uint32(b), Minor: le.Uint32(b[4:]), MaxReadahead: le.Uint32(b[8:]), Flags: uint64(le.Uint32(b[12:]))}
	if in.Flags&FlagInitExt != 0 && len(b) >= initInSize {
		in.Flags |= uint64(le.Uint32(b[16:])) << 32
	}
	return in, nil
}

// name splits b at its first zero.
func name(b []byte) (n, rest []byte, err error) {
	i := bytes.IndexByte(b, 0)
	if i < 0 {
		return nil, nil, ErrName
	}
	return b[:i:i], b[i+1:], nil
}

// lastName returns the name that fills b, its zero included.
func lastName(b []byte) ([]byte, error) {
	n, rest, err := name(b)
	if err == nil && len(rest) != 0 {
		err = ErrBody
	}
	return n, err
}
