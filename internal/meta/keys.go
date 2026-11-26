package meta

import (
	"encoding/binary"
	"errors"
)

// A key is a one-byte space, then fixed-width big-endian fields and one-byte tags, then at most
// one variable last component, so byte order is logical order and a prefix scan is a byte-prefix
// scan. The tags are frozen disk format (testdata/key_golden.txt).

// The spaces, a key's first byte.
const (
	SpaceCtl byte = iota + 1
	SpaceVolume
	SpaceFS
	SpaceS3
)

// The tags under ctl/.
const (
	CtlPool byte = iota + 1
	CtlPlacement
	CtlMembership
	CtlSession
	CtlFormat
	CtlShardMap
)

// The tags under volume/{id}/; volume/{0}/record is the volume ID counter.
const (
	VolumeAttach byte = iota + 1
	VolumeSnap
	VolumeRecord
)

// An inode's tags, at sequence 1 and above: a, x, t, l, d, c and P.
const (
	InodeAttr byte = iota + 1
	InodeXattr
	InodeTarget
	InodeLock
	InodeEntry
	InodeFolded
	InodeParent
)

// A slot's tags, at sequence 0: n, u, o, p, r and f.
const (
	SlotNext byte = iota + 0x10
	SlotUsed
	SlotOrphan
	SlotPurge
	SlotReplay
	SlotFree
)

// PieceBit set on a value's tag is the tag of its pieces.
const PieceBit byte = 0x80

// The lock modes of an l/{mode}/{session} key.
const (
	LockShared byte = iota + 1
	LockExclusive
)

// An inode ID is a slot of SlotBits and a sequence of SeqBits.
const (
	SlotBits = 24
	SeqBits  = 40
	seqMask  = 1<<SeqBits - 1
)

var (
	// ErrBadKey reports a key that does not decode, or a Key or inode that does not encode.
	ErrBadKey = errors.New("meta: bad key")
	// ErrBadPiece reports a head value or piece that is not one a split value gives.
	ErrBadPiece = errors.New("meta: bad piece")
)

// Key is a key's components; which ones it has depends on its Space and Tag.
type Key struct {
	Space byte
	// Tag is the tag after the space's fixed fields; s3/ keys have none.
	Tag     byte
	Volume  uint32
	Inode   uint64
	Piece   byte
	Mode    byte
	Session uint64
	Mount   uint32
	// ID is a fixed 64-bit last component: a request, an orphan's or purge entry's inode, a snapshot epoch or a session's ID under ctl/.
	ID uint64
	// Name is the variable last component, which a decoded Key aliases from its bytes.
	Name []byte
}

// layout is what follows a key's tag, and its piece number for a piece key.
type layout uint8

const (
	bad    layout = iota
	empty         // nothing
	name          // a non-empty Name
	rest          // a Name, possibly empty, that the control-plane group gives its meaning
	id            // ID
	lock          // Mode, Session
	replay        // Session, Mount, ID
)

var (
	ctlLayouts    = [...]layout{CtlPool: rest, CtlPlacement: rest, CtlMembership: rest, CtlSession: id, CtlFormat: rest, CtlShardMap: rest}
	volumeLayouts = [...]layout{VolumeAttach: empty, VolumeSnap: id, VolumeRecord: empty}
	fsLayouts     = [...]layout{
		InodeAttr: empty, InodeXattr: name, InodeTarget: empty, InodeLock: lock, InodeEntry: name, InodeFolded: name, InodeParent: empty,
		SlotNext: empty, SlotUsed: empty, SlotOrphan: id, SlotPurge: id, SlotReplay: replay, SlotFree: empty,
	}
	// sizes is each layout's length after the tag and piece number, -1 for a variable one.
	sizes = [...]int{empty: 0, name: -1, rest: -1, id: 8, lock: 9, replay: 20}
	// heads is each space's length after its byte, through its tag.
	heads = [...]int{SpaceCtl: 1, SpaceVolume: 5, SpaceFS: 13, SpaceS3: 4}
)

func layoutOf(space, tag byte, inode uint64) layout {
	switch space {
	case SpaceCtl:
		if int(tag) < len(ctlLayouts) {
			return ctlLayouts[tag]
		}
	case SpaceVolume:
		if int(tag) < len(volumeLayouts) {
			return volumeLayouts[tag]
		}
	case SpaceS3:
		return name
	case SpaceFS:
		if tag == InodeXattr|PieceBit || tag == InodeTarget|PieceBit {
			tag &^= PieceBit
		}
		if int(tag) < len(fsLayouts) && (tag >= SlotNext) == (inode&seqMask == 0) {
			return fsLayouts[tag]
		}
	}
	return bad
}

// MakeInode is the ID of sequence seq in slot; ErrBadKey when either is out of range.
func MakeInode(slot uint32, seq uint64) (uint64, error) {
	if slot >= 1<<SlotBits || seq > seqMask {
		return 0, ErrBadKey
	}
	return uint64(slot)<<SeqBits | seq, nil
}

// AppendKey appends k's encoding to dst; ErrBadKey, and dst unchanged, when it would not decode.
func AppendKey(dst []byte, k *Key) ([]byte, error) {
	start := len(dst)
	dst = append(dst, k.Space)
	be := binary.BigEndian
	switch k.Space {
	case SpaceCtl:
		dst = append(dst, k.Tag)
	case SpaceVolume:
		dst = append(be.AppendUint32(dst, k.Volume), k.Tag)
	case SpaceFS:
		dst = append(be.AppendUint64(be.AppendUint32(dst, k.Volume), k.Inode), k.Tag)
		if k.Tag&PieceBit != 0 {
			dst = append(dst, k.Piece)
		}
	case SpaceS3:
		dst = be.AppendUint32(dst, k.Volume)
	}
	switch layoutOf(k.Space, k.Tag, k.Inode) {
	case name, rest:
		dst = append(dst, k.Name...)
	case id:
		dst = be.AppendUint64(dst, k.ID)
	case lock:
		dst = be.AppendUint64(append(dst, k.Mode), k.Session)
	case replay:
		dst = be.AppendUint64(be.AppendUint32(be.AppendUint64(dst, k.Session), k.Mount), k.ID)
	}
	var d Key
	if DecodeKey(dst[start:], &d) != nil {
		return dst[:start], ErrBadKey
	}
	return dst, nil
}

// DecodeKey decodes b into k, whose Name aliases b; ErrBadKey when b is no key.
func DecodeKey(b []byte, k *Key) error {
	*k = Key{}
	if len(b) == 0 || len(b) > MaxKey {
		return ErrBadKey
	}
	be := binary.BigEndian
	k.Space, b = b[0], b[1:]
	if int(k.Space) >= len(heads) || k.Space == 0 || len(b) < heads[k.Space] {
		return ErrBadKey
	}
	if k.Space != SpaceCtl {
		k.Volume = be.Uint32(b)
	}
	if k.Space == SpaceFS {
		k.Inode = be.Uint64(b[4:])
	}
	if k.Space != SpaceS3 {
		k.Tag = b[heads[k.Space]-1]
	}
	b = b[heads[k.Space]:]
	l := layoutOf(k.Space, k.Tag, k.Inode)
	if l == bad {
		return ErrBadKey
	}
	if k.Space == SpaceFS && k.Tag&PieceBit != 0 {
		if len(b) == 0 || b[0] == 0 {
			return ErrBadKey
		}
		k.Piece, b = b[0], b[1:]
	}
	if n := sizes[l]; l == name && len(b) == 0 || n >= 0 && len(b) != n {
		return ErrBadKey
	}
	switch l {
	case name, rest:
		if len(b) > 0 {
			k.Name = b
		}
	case id:
		k.ID = be.Uint64(b)
	case lock:
		if b[0] != LockShared && b[0] != LockExclusive {
			return ErrBadKey
		}
		k.Mode, k.Session = b[0], be.Uint64(b[1:])
	case replay:
		k.Session, k.Mount, k.ID = be.Uint64(b), be.Uint32(b[8:]), be.Uint64(b[12:])
	}
	return nil
}

// A value longer than MaxValue - 1 bytes, an xattr's or a symlink target, is split: its head,
// under its own key, is the count m of the pieces after it, one byte, then its first MaxValue - 1
// bytes, and pieces 1 … m under its piece keys hold MaxValue bytes each, the last 1 to MaxValue.
// A value that fits is a head with m = 0.
const headBytes = MaxValue - 1

// MaxSplit is the longest value a head and its pieces hold, Linux's XATTR_SIZE_MAX.
const MaxSplit = 64 << 10

// maxPieces is the count of pieces after the head of a MaxSplit value.
const maxPieces = (MaxSplit - headBytes + MaxValue - 1) / MaxValue

// Pieces is the number of pieces after the head of a value of n bytes.
func Pieces(n int) int { return (max(n-headBytes, 0) + MaxValue - 1) / MaxValue }

// AppendHead appends v's head to dst; ErrTooLarge over MaxSplit.
func AppendHead(dst, v []byte) ([]byte, error) {
	if len(v) > MaxSplit {
		return dst, ErrTooLarge
	}
	return append(append(dst, byte(Pieces(len(v)))), v[:min(len(v), headBytes)]...), nil
}

// Piece is piece i of v, counted from 1, aliasing v.
func Piece(v []byte, i int) []byte {
	lo := headBytes + (i-1)*MaxValue
	return v[lo:min(lo+MaxValue, len(v))]
}

// DecodeHead is a head's piece count and its first part, aliasing h; ErrBadPiece when h is no head.
func DecodeHead(h []byte) (int, []byte, error) {
	if len(h) == 0 || len(h) > MaxValue || h[0] > maxPieces || h[0] > 0 && len(h) != MaxValue {
		return 0, nil, ErrBadPiece
	}
	return int(h[0]), h[1:], nil
}

// AppendPiece appends p, read as piece i of m, to dst; ErrBadPiece when no split value has such a piece.
func AppendPiece(dst, p []byte, i, m int) ([]byte, error) {
	if i < 1 || i > m || m > maxPieces || len(p) == 0 || len(p) > MaxSplit-headBytes-(i-1)*MaxValue ||
		len(p) > MaxValue || i < m && len(p) != MaxValue {
		return dst, ErrBadPiece
	}
	return append(dst, p...), nil
}
