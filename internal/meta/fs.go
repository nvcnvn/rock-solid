package meta

import (
	"encoding/binary"
	"errors"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// The values of fs/ keys, hand-encoded with fixed widths so an Update can change one field of
// them in place. Their layouts are disk format.

// The offsets of an inode's attribute fields, each 8 bytes big-endian, under its a key.
const (
	AttrMode     uint16 = 8 * iota // file type and permission bits, as st_mode
	AttrNlink                      // links, a directory's 2 plus its subdirectories
	AttrUID                        // owner
	AttrGID                        // group
	AttrSize                       // size in bytes
	AttrAllocEnd                   // one past the highest chunk that may hold data
	AttrAtime                      // times as TimeField gives them
	AttrMtime
	AttrCtime
	// AttrBytes is the length of an attribute value.
	AttrBytes = int(8 * iota)
)

// The file types of an attribute's mode, as st_mode's S_IFMT bits; a directory entry carries
// the type shifted right by 12, as d_type does.
const (
	ModeType    uint64 = 0o170000
	ModeDir     uint64 = 0o040000
	ModeFile    uint64 = 0o100000
	ModeSymlink uint64 = 0o120000
)

// RootInode is a volume's root directory, sequence 1 of slot 0.
const RootInode uint64 = 1

// EntryBytes is the length of a directory entry's value: the inode, then its type.
const EntryBytes = 9

// PurgeBytes is the length of a purge entry's value: its start byte, then its end chunk.
const PurgeBytes = 16

// ErrBadValue reports an fs/ value that does not decode.
var ErrBadValue = errors.New("meta: bad fs value")

// Attr is an inode's attributes. Times are nanoseconds since the Unix epoch.
type Attr struct {
	Mode, Nlink, UID, GID, Size, AllocEnd uint64
	Atime, Mtime, Ctime                   int64
}

// TimeField is the stored form of time t: biased by 2^63, so times before 1970 order below later
// ones as unsigned fields, which Bump compares.
func TimeField(t int64) uint64 { return uint64(t) ^ 1<<63 }

func timeOf(f uint64) int64 { return int64(f ^ 1<<63) }

// AppendAttr appends a's encoding to dst.
func AppendAttr(dst []byte, a *Attr) []byte {
	be := binary.BigEndian
	for _, f := range [...]uint64{a.Mode, a.Nlink, a.UID, a.GID, a.Size, a.AllocEnd, TimeField(a.Atime), TimeField(a.Mtime), TimeField(a.Ctime)} {
		dst = be.AppendUint64(dst, f)
	}
	return dst
}

// DecodeAttr decodes b into a; ErrBadValue when b is not AttrBytes long.
func DecodeAttr(b []byte, a *Attr) error {
	if len(b) != AttrBytes {
		return ErrBadValue
	}
	be := binary.BigEndian
	*a = Attr{
		Mode: be.Uint64(b), Nlink: be.Uint64(b[AttrNlink:]), UID: be.Uint64(b[AttrUID:]), GID: be.Uint64(b[AttrGID:]),
		Size: be.Uint64(b[AttrSize:]), AllocEnd: be.Uint64(b[AttrAllocEnd:]),
		Atime: timeOf(be.Uint64(b[AttrAtime:])), Mtime: timeOf(be.Uint64(b[AttrMtime:])), Ctime: timeOf(be.Uint64(b[AttrCtime:])),
	}
	return nil
}

// AppendEntry appends a directory entry's value, inode and the type of mode, to dst.
func AppendEntry(dst []byte, inode, mode uint64) []byte {
	return append(binary.BigEndian.AppendUint64(dst, inode), byte(mode&ModeType>>12))
}

// DecodeEntry is a directory entry's inode and file type as mode bits; ErrBadValue when b is none.
func DecodeEntry(b []byte) (inode, mode uint64, err error) {
	if len(b) != EntryBytes || b[8] > byte(ModeType>>12) {
		return 0, 0, ErrBadValue
	}
	return binary.BigEndian.Uint64(b), uint64(b[8]) << 12, nil
}

// DecodeParent is a P key's value, the directory's parent; ErrBadValue when b is none.
func DecodeParent(b []byte) (uint64, error) {
	if len(b) != 8 {
		return 0, ErrBadValue
	}
	return binary.BigEndian.Uint64(b), nil
}

// Purge is a purge entry: every byte from Start to the end of chunk End - 1 is to be freed.
type Purge struct {
	Start, End uint64
}

// Chunks is the number of chunks that hold bytes 0 … size − 1.
func Chunks(size uint64) uint64 { return size/core.ChunkSize + min(size%core.ChunkSize, 1) }

// empty reports a range that frees nothing.
func (p Purge) empty() bool { return p.Start >= p.End*core.ChunkSize }

// Merge is the union of p and q. Every pending range lies past the file's size and every new one
// starts at or below it, so the union is one range.
func (p Purge) Merge(q Purge) Purge { return Purge{min(p.Start, q.Start), max(p.End, q.End)} }

// AppendPurge appends p's encoding to dst.
func AppendPurge(dst []byte, p Purge) []byte {
	return binary.BigEndian.AppendUint64(binary.BigEndian.AppendUint64(dst, p.Start), p.End)
}

// DecodePurge decodes a purge entry; ErrBadValue when b is none or frees nothing.
func DecodePurge(b []byte) (Purge, error) {
	if len(b) != PurgeBytes {
		return Purge{}, ErrBadValue
	}
	p := Purge{binary.BigEndian.Uint64(b), binary.BigEndian.Uint64(b[8:])}
	if p.End > (1<<64-1)/core.ChunkSize || p.empty() {
		return Purge{}, ErrBadValue
	}
	return p, nil
}

// Mtime is what a close or fsync stores: the latest of the writer's time of its last write, the
// stored mtime plus 1 ns, and the largest mtime the node reported for a write-shared file.
func Mtime(stored, write, reported int64) int64 { return max(write, stored+1, reported) }
