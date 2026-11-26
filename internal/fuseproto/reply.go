package fuseproto

// INIT flags, joined as InitIn.Flags and InitOut.Flags join flags and flags2.
const (
	FlagAsyncRead      = 1 << 0
	FlagBigWrites      = 1 << 5
	FlagAutoInvalData  = 1 << 12
	FlagDoReaddirplus  = 1 << 13
	FlagWritebackCache = 1 << 16
	FlagParallelDirops = 1 << 18
	FlagMaxPages       = 1 << 22
	FlagInitExt        = 1 << 30
	FlagHasResend      = 1 << 39
	FlagOverIOUring    = 1 << 41
	FlagRequestTimeout = 1 << 42
)

// Open flags of an OPEN or CREATE reply.
const (
	OpenDirectIO  = 1 << 0
	OpenKeepCache = 1 << 1
)

// Setattr.Valid bits.
const (
	AttrMode      = 1 << 0
	AttrUID       = 1 << 1
	AttrGID       = 1 << 2
	AttrSize      = 1 << 3
	AttrAtime     = 1 << 4
	AttrMtime     = 1 << 5
	AttrFh        = 1 << 6
	AttrAtimeNow  = 1 << 7
	AttrMtimeNow  = 1 << 8
	AttrLockOwner = 1 << 9
	AttrCtime     = 1 << 10
)

// Request.OpFlags bits.
const (
	GetattrFh      = 1 << 0
	ReleaseFlush   = 1 << 0
	FsyncFdatasync = 1 << 0
)

// ExpireOnly is the flag of an inval_entry that expires the entry rather than dropping it.
const ExpireOnly = 1 << 0

// Errno is a positive Linux error number.
type Errno int32

// Error numbers the codec's callers answer malformed requests with.
const (
	EIO    Errno = 5
	ENOSYS Errno = 38
)

const (
	notifyInvalInode = 2
	notifyInvalEntry = 3
	notifyResend     = 7

	attrSize     = 88
	entrySize    = 40 + attrSize
	openSize     = 16
	initOutSize  = 64
	statfsSize   = 80
	direntOffset = entrySize + 24
)

// Attr is fuse_attr, a node's attributes.
type Attr struct {
	Ino       uint64
	Size      uint64
	Blocks    uint64
	Atime     uint64
	Mtime     uint64
	Ctime     uint64
	AtimeNsec uint32
	MtimeNsec uint32
	CtimeNsec uint32
	Mode      uint32
	Nlink     uint32
	UID       uint32
	GID       uint32
	Rdev      uint32
	Blksize   uint32
	Flags     uint32
}

// Entry is fuse_entry_out, a name's node and how long the kernel may cache it.
type Entry struct {
	NodeID         uint64
	Generation     uint64
	EntryValid     uint64
	AttrValid      uint64
	EntryValidNsec uint32
	AttrValidNsec  uint32
	Attr           Attr
}

// Open is fuse_open_out, an opened handle.
type Open struct {
	Fh    uint64
	Flags uint32
}

// Statfs is fuse_kstatfs.
type Statfs struct {
	Blocks  uint64
	Bfree   uint64
	Bavail  uint64
	Files   uint64
	Ffree   uint64
	Bsize   uint32
	NameLen uint32
	Frsize  uint32
}

// InitOut is the daemon's INIT reply; Flags joins flags and flags2.
type InitOut struct {
	Major               uint32
	Minor               uint32
	MaxReadahead        uint32
	Flags               uint64
	MaxBackground       uint16
	CongestionThreshold uint16
	MaxWrite            uint32
	TimeGran            uint32
	MaxPages            uint16
	MapAlignment        uint16
	MaxStackDepth       uint32
	RequestTimeout      uint16
}

func appendOutHeader(dst []byte, n int, errno int32, unique uint64) []byte {
	dst = le.AppendUint32(dst, uint32(OutHeaderSize+n))
	dst = le.AppendUint32(dst, uint32(errno))
	return le.AppendUint64(dst, unique)
}

// AppendError appends the reply failing request unique with errno.
func AppendError(dst []byte, unique uint64, errno Errno) []byte {
	return appendOutHeader(dst, 0, -int32(errno), unique)
}

// AppendEmpty appends a success reply with no body.
func AppendEmpty(dst []byte, unique uint64) []byte {
	return appendOutHeader(dst, 0, 0, unique)
}

// PutDataReply writes the header of a success reply into frame's first OutHeaderSize bytes, the
// rest of frame being its body: READ's data or READDIRPLUS's records, already in place.
func PutDataReply(frame []byte, unique uint64) {
	appendOutHeader(frame[:0], len(frame)-OutHeaderSize, 0, unique)
}

// AppendEntry appends the reply of LOOKUP, MKDIR or LINK.
func AppendEntry(dst []byte, unique uint64, e *Entry) []byte {
	return appendEntry(appendOutHeader(dst, entrySize, 0, unique), e)
}

// AppendAttr appends the reply of GETATTR or SETATTR, cached for valid seconds and validNsec.
func AppendAttr(dst []byte, unique, valid uint64, validNsec uint32, a *Attr) []byte {
	dst = appendOutHeader(dst, 16+attrSize, 0, unique)
	dst = le.AppendUint64(dst, valid)
	dst = le.AppendUint64(dst, uint64(validNsec))
	return appendAttr(dst, a)
}

// AppendOpen appends the reply of OPEN or OPENDIR.
func AppendOpen(dst []byte, unique uint64, o Open) []byte {
	return appendOpen(appendOutHeader(dst, openSize, 0, unique), o)
}

// AppendCreate appends the reply of CREATE or TMPFILE.
func AppendCreate(dst []byte, unique uint64, e *Entry, o Open) []byte {
	return appendOpen(appendEntry(appendOutHeader(dst, entrySize+openSize, 0, unique), e), o)
}

// AppendWrite appends the reply of a WRITE that wrote size bytes.
func AppendWrite(dst []byte, unique uint64, size uint32) []byte {
	return le.AppendUint64(appendOutHeader(dst, 8, 0, unique), uint64(size))
}

// AppendXattrSize appends the reply of a GETXATTR or LISTXATTR asking for the length only.
func AppendXattrSize(dst []byte, unique uint64, size uint32) []byte {
	return le.AppendUint64(appendOutHeader(dst, 8, 0, unique), uint64(size))
}

// AppendStatfs appends the reply of STATFS.
func AppendStatfs(dst []byte, unique uint64, s *Statfs) []byte {
	dst = appendOutHeader(dst, statfsSize, 0, unique)
	for _, v := range [...]uint64{s.Blocks, s.Bfree, s.Bavail, s.Files, s.Ffree} {
		dst = le.AppendUint64(dst, v)
	}
	dst = le.AppendUint32(dst, s.Bsize)
	dst = le.AppendUint32(dst, s.NameLen)
	dst = le.AppendUint32(dst, s.Frsize)
	return append(dst, make([]byte, 28)...)
}

// AppendInit appends the reply of INIT.
func AppendInit(dst []byte, unique uint64, o *InitOut) []byte {
	return appendInitOut(appendOutHeader(dst, initOutSize, 0, unique), o)
}

// DirentPlusSize returns the length of a READDIRPLUS record whose name is nameLen bytes.
func DirentPlusSize(nameLen int) int { return (direntOffset + nameLen + 7) &^ 7 }

// AppendDirentPlus appends one READDIRPLUS record: e, the offset of the next entry, the entry's
// type (its mode's S_IFMT bits shifted right by 12) and its name.
func AppendDirentPlus(dst []byte, e *Entry, off uint64, typ uint32, name []byte) []byte {
	dst = appendEntry(dst, e)
	dst = le.AppendUint64(dst, e.Attr.Ino)
	dst = le.AppendUint64(dst, off)
	dst = le.AppendUint32(dst, uint32(len(name)))
	dst = le.AppendUint32(dst, typ)
	dst = append(dst, name...)
	return append(dst, make([]byte, DirentPlusSize(len(name))-direntOffset-len(name))...)
}

// AppendInvalInode appends a notification dropping the kernel's cached attributes of node ino and
// its cached data from off for length bytes, to its end if length is negative.
func AppendInvalInode(dst []byte, ino uint64, off, length int64) []byte {
	dst = appendOutHeader(dst, 24, notifyInvalInode, 0)
	dst = le.AppendUint64(dst, ino)
	dst = le.AppendUint64(dst, uint64(off))
	return le.AppendUint64(dst, uint64(length))
}

// AppendInvalEntry appends a notification dropping the kernel's cached entry name in directory
// parent, or with ExpireOnly expiring it.
func AppendInvalEntry(dst []byte, parent uint64, name []byte, flags uint32) []byte {
	dst = appendOutHeader(dst, 16+len(name)+1, notifyInvalEntry, 0)
	dst = le.AppendUint64(dst, parent)
	dst = le.AppendUint32(dst, uint32(len(name)))
	dst = le.AppendUint32(dst, flags)
	return append(append(dst, name...), 0)
}

// AppendResend appends FUSE_NOTIFY_RESEND, which makes the kernel send again every request it
// had handed to the daemon and not yet seen answered.
func AppendResend(dst []byte) []byte {
	return appendOutHeader(dst, 0, notifyResend, 0)
}

func appendAttr(dst []byte, a *Attr) []byte {
	for _, v := range [...]uint64{a.Ino, a.Size, a.Blocks, a.Atime, a.Mtime, a.Ctime} {
		dst = le.AppendUint64(dst, v)
	}
	for _, v := range [...]uint32{a.AtimeNsec, a.MtimeNsec, a.CtimeNsec, a.Mode, a.Nlink, a.UID, a.GID, a.Rdev, a.Blksize, a.Flags} {
		dst = le.AppendUint32(dst, v)
	}
	return dst
}

func appendEntry(dst []byte, e *Entry) []byte {
	for _, v := range [...]uint64{e.NodeID, e.Generation, e.EntryValid, e.AttrValid} {
		dst = le.AppendUint64(dst, v)
	}
	dst = le.AppendUint32(dst, e.EntryValidNsec)
	dst = le.AppendUint32(dst, e.AttrValidNsec)
	return appendAttr(dst, &e.Attr)
}

func appendOpen(dst []byte, o Open) []byte {
	dst = le.AppendUint64(dst, o.Fh)
	return le.AppendUint64(dst, uint64(o.Flags))
}

func appendInitOut(dst []byte, o *InitOut) []byte {
	dst = le.AppendUint32(dst, o.Major)
	dst = le.AppendUint32(dst, o.Minor)
	dst = le.AppendUint32(dst, o.MaxReadahead)
	dst = le.AppendUint32(dst, uint32(o.Flags))
	dst = le.AppendUint16(dst, o.MaxBackground)
	dst = le.AppendUint16(dst, o.CongestionThreshold)
	dst = le.AppendUint32(dst, o.MaxWrite)
	dst = le.AppendUint32(dst, o.TimeGran)
	dst = le.AppendUint16(dst, o.MaxPages)
	dst = le.AppendUint16(dst, o.MapAlignment)
	dst = le.AppendUint32(dst, uint32(o.Flags>>32))
	dst = le.AppendUint32(dst, o.MaxStackDepth)
	dst = le.AppendUint16(dst, o.RequestTimeout)
	return append(dst, make([]byte, 22)...)
}
