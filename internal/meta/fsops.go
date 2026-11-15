package meta

import (
	"bytes"
	"encoding/binary"
	"errors"
	"unicode"
	"unicode/utf8"
)

// The reservation batches: a daemon reserves InodeBatch sequence numbers of a slot, or SlotBatch
// slots of a shard, with one Txn, and reserves the next batch once only InodePrefetch or
// SlotPrefetch are left, about a tenth.
const (
	InodeBatch    = 64
	InodePrefetch = 6
	SlotBatch     = 16
	SlotPrefetch  = 2
)

var (
	// ErrLocked reports a lock a live session holds in a conflicting mode.
	ErrLocked = errors.New("meta: locked")
	// ErrLoop reports a rename that would move a directory under itself.
	ErrLoop = errors.New("meta: directory moved under itself")
	// ErrAncestry reports a rename whose ancestors do not chain from the new parent to the root.
	ErrAncestry = errors.New("meta: ancestors do not reach the root")
)

// Request names a client request, whose Txn carries its replay key.
type Request struct {
	Session uint64
	Mount   uint32
	ID      uint64
	// Oldest is the mount's oldest outstanding request.
	Oldest uint64
}

// FS builds the Txns of filesystem operations on one volume, each op as the design's
// transaction table gives it. Txn and its keys and values alias FS's buffer, which the next
// operation reuses.
type FS struct {
	Volume uint32
	// CaseIndex keeps the case-folded name of every entry under c/.
	CaseIndex bool
	// Req, when set, makes the next Txn a client request's, with its replay key in the slot it changes.
	Req  *Request
	Txn  Txn
	buf  []byte
	fold []byte
	err  error
	// resetRoot is a bug a test plants: Root without its check, so a resent root creation resets the volume's counters.
	resetRoot bool
}

// Rename is a rename's inputs, as the daemon read them.
type Rename struct {
	SrcDir, DstDir uint64
	Src, Dst       []byte
	// Child is the moved inode and ChildMode its type.
	Child, ChildMode uint64
	// Over is the inode the destination entry names, 0 when it is absent; OverAttr its attributes,
	// OverOpen whether it is open, and OverPending its purge entry, nil when it has none.
	Over        uint64
	OverAttr    *Attr
	OverOpen    bool
	OverPending *Purge
	// Ancestors are the new parent and, for a directory, each directory above it up to the root,
	// each with the parent it read; a move across directories needs them.
	Ancestors []Ancestor
}

// Ancestor is a directory and its parent.
type Ancestor struct {
	Dir, Parent uint64
}

// Field is an attribute field and the value to set it to.
type Field struct {
	Off uint16
	V   uint64
}

// Holder is a lock key a scan of the inode's l/ found, and whether its session is live.
type Holder struct {
	Mode    byte
	Session uint64
	Live    bool
}

func slotOf(inode uint64) uint32 { return uint32(inode >> SeqBits) }

func (f *FS) begin(slot uint32) {
	f.Txn = Txn{Ops: f.Txn.Ops[:0]}
	f.buf, f.err = f.buf[:0], nil
	if f.Req != nil {
		f.Txn.Replay = f.appendKey(&Key{Space: SpaceFS, Volume: f.Volume, Inode: uint64(slot) << SeqBits, Tag: SlotReplay, Session: f.Req.Session, Mount: f.Req.Mount, ID: f.Req.ID})
		f.Txn.Oldest = f.Req.Oldest
	}
}

func (f *FS) end() error {
	if f.err != nil {
		f.Txn.Ops = f.Txn.Ops[:0]
	}
	return f.err
}

func (f *FS) op(o Op) { f.Txn.Ops = append(f.Txn.Ops, o) }

func (f *FS) appendKey(k *Key) []byte {
	start := len(f.buf)
	var err error
	if f.buf, err = AppendKey(f.buf, k); err != nil && f.err == nil {
		f.err = err
	}
	return f.buf[start:len(f.buf):len(f.buf)]
}

func (f *FS) key(inode uint64, tag byte, name []byte) []byte {
	return f.appendKey(&Key{Space: SpaceFS, Volume: f.Volume, Inode: inode, Tag: tag, Name: name})
}

func (f *FS) slotKey(slot uint32, tag byte, id uint64) []byte {
	return f.appendKey(&Key{Space: SpaceFS, Volume: f.Volume, Inode: uint64(slot) << SeqBits, Tag: tag, ID: id})
}

func (f *FS) lockKey(inode uint64, mode byte, session uint64) []byte {
	return f.appendKey(&Key{Space: SpaceFS, Volume: f.Volume, Inode: inode, Tag: InodeLock, Mode: mode, Session: session})
}

// bound is fs/{volume}/{inode} and the tags given, a range bound no key need equal.
func (f *FS) bound(inode uint64, tags ...byte) []byte {
	start := len(f.buf)
	f.buf = append(binary.BigEndian.AppendUint64(binary.BigEndian.AppendUint32(append(f.buf, SpaceFS), f.Volume), inode), tags...)
	return f.buf[start:len(f.buf):len(f.buf)]
}

// val is what appends wrote to the buffer since start.
func (f *FS) val(start int) []byte { return f.buf[start:len(f.buf):len(f.buf)] }

func (f *FS) attr(a *Attr) []byte {
	start := len(f.buf)
	f.buf = AppendAttr(f.buf, a)
	return f.val(start)
}

func (f *FS) u64(n uint64) []byte {
	start := len(f.buf)
	f.buf = binary.BigEndian.AppendUint64(f.buf, n)
	return f.val(start)
}

func (f *FS) entry(inode, mode uint64) []byte {
	start := len(f.buf)
	f.buf = AppendEntry(f.buf, inode, mode)
	return f.val(start)
}

// times moves the parent's mtime and ctime to at least now, and each by at least 1 ns.
func (f *FS) times(dir uint64, now int64) {
	a := f.key(dir, InodeAttr, nil)
	f.op(Update(a, AttrMtime, Bump, TimeField(now)))
	f.op(Update(a, AttrCtime, Bump, TimeField(now)))
}

// folded is dir's c/ key of name, case-folded: each rune of valid UTF-8 replaced by the least rune
// of its unicode.SimpleFold orbit, so two names of valid UTF-8 fold alike exactly when
// strings.EqualFold matches them; other bytes as they are; nil without CaseIndex.
func (f *FS) folded(dir uint64, name []byte) []byte {
	if !f.CaseIndex {
		return nil
	}
	f.fold = f.fold[:0]
	for len(name) > 0 {
		r, n := utf8.DecodeRune(name)
		if r == utf8.RuneError && n == 1 {
			f.fold = append(f.fold, name[0])
		} else {
			f.fold = utf8.AppendRune(f.fold, foldRune(r))
		}
		name = name[n:]
	}
	return f.key(dir, InodeFolded, f.fold)
}

func foldRune(r rune) rune {
	least := r
	for o := unicode.SimpleFold(r); o != r; o = unicode.SimpleFold(o) {
		least = min(least, o)
	}
	return least
}

// split puts value v under inode's key of tag, in pieces past MaxValue − 1 bytes, and removes
// the pieces past its own of a value that had old pieces.
func (f *FS) split(inode uint64, tag byte, name, v []byte, old int) {
	k := f.key(inode, tag, name)
	start := len(f.buf)
	var err error
	if f.buf, err = AppendHead(f.buf, v); err != nil {
		f.err = err
		return
	}
	f.op(Put(k, f.val(start)))
	m := Pieces(len(v))
	for i := 1; i <= max(m, old); i++ {
		k := f.appendKey(&Key{Space: SpaceFS, Volume: f.Volume, Inode: inode, Tag: tag | PieceBit, Piece: byte(i), Name: name})
		if i <= m {
			f.op(Put(k, Piece(v, i)))
		} else {
			f.op(Delete(k))
		}
	}
}

// purge merges add into inode's purge entry, checking that its pending entry is still the one read.
func (f *FS) purge(inode uint64, pending *Purge, add Purge) {
	k := f.slotKey(slotOf(inode), SlotPurge, inode)
	if pending != nil {
		start := len(f.buf)
		f.buf = AppendPurge(f.buf, *pending)
		f.op(Check(k, f.val(start), false))
		add = add.Merge(*pending)
	} else {
		f.op(CheckAbsent(k, false))
	}
	if add.empty() {
		return
	}
	start := len(f.buf)
	f.buf = AppendPurge(f.buf, add)
	f.op(Put(k, f.val(start)))
}

// used adds delta to the byte counter of inode's slot.
func (f *FS) used(inode uint64, delta uint64) {
	if delta != 0 {
		f.op(Update(f.slotKey(slotOf(inode), SlotUsed, 0), 0, Add, delta))
	}
}

// remove ends inode's last link: its keys go, its chunks join its purge entry, and its slot's
// byte counter drops by a file's size. The check on its attributes fails if a link or a write raced.
func (f *FS) remove(inode uint64, a *Attr, pending *Purge) {
	f.op(Check(f.key(inode, InodeAttr, nil), f.attr(a), false))
	f.op(DeleteRange(f.bound(inode), f.bound(inode+1)))
	if pending != nil || a.AllocEnd > 0 {
		f.purge(inode, pending, Purge{0, a.AllocEnd})
	}
	if a.Mode&ModeType == ModeFile {
		f.used(inode, -a.Size)
	}
}

// unlink drops one of inode's links, which it had when read as a: the last link of an inode no
// one has open removes it, an open one becomes an orphan.
func (f *FS) unlink(inode uint64, a *Attr, open bool, pending *Purge, now int64) {
	if a.Nlink <= 1 && !open {
		f.remove(inode, a, pending)
		return
	}
	k := f.key(inode, InodeAttr, nil)
	f.op(Check(k, f.attr(a), false))
	b := *a
	b.Nlink, b.Ctime = b.Nlink-1, now
	f.op(Put(k, f.attr(&b)))
	if b.Nlink == 0 {
		f.op(Put(f.slotKey(slotOf(inode), SlotOrphan, inode), nil))
	}
}

// Root creates a volume's root directory and its slot counters, once.
func (f *FS) Root(a *Attr) error {
	f.begin(0)
	n := f.slotKey(0, SlotNext, 0)
	if !f.resetRoot {
		f.op(CheckAbsent(n, true))
	}
	f.op(Put(f.key(RootInode, InodeAttr, nil), f.attr(a)))
	f.op(Put(n, f.u64(2)))
	f.op(Put(f.slotKey(0, SlotFree, 0), f.u64(1)))
	return f.end()
}

// internal begins an internal Txn, one without a replay key.
func (f *FS) internal(slot uint32) {
	req := f.Req
	f.Req = nil
	f.begin(slot)
	f.Req = req
}

// ReserveInodes reserves InodeBatch sequence numbers of slot, an internal Txn whose Updated end
// the Allocator takes.
func (f *FS) ReserveInodes(slot uint32) error {
	f.internal(slot)
	f.op(Update(f.slotKey(slot, SlotNext, 0), 0, Add, InodeBatch))
	return f.end()
}

// ReserveSlots reserves SlotBatch slots from the f key at lowest, the shard's lowest slot.
func (f *FS) ReserveSlots(lowest uint32) error {
	f.internal(lowest)
	f.op(Update(f.slotKey(lowest, SlotFree, 0), 0, Add, SlotBatch))
	return f.end()
}

// live checks that directory dir still has up, its parent as read, which rmdir deletes and the
// root never has.
func (f *FS) live(dir, up uint64) {
	if dir != RootInode {
		f.op(Check(f.key(dir, InodeParent, nil), f.u64(up), false))
	}
}

// Create makes name in dir, whose parent was read as up, an entry of child, a new file, or a
// symlink to target when a's mode says so.
func (f *FS) Create(dir, up uint64, name []byte, child uint64, a *Attr, target []byte, now int64) error {
	f.begin(slotOf(dir))
	f.create(dir, up, name, child, a.Mode, now)
	f.op(Put(f.key(child, InodeAttr, nil), f.attr(a)))
	if a.Mode&ModeType == ModeSymlink {
		f.split(child, InodeTarget, nil, target, 0)
	}
	return f.end()
}

func (f *FS) create(dir, up uint64, name []byte, child, mode uint64, now int64) {
	d := f.key(dir, InodeEntry, name)
	f.op(CheckAbsent(d, true))
	f.live(dir, up)
	f.op(Put(d, f.entry(child, mode)))
	if c := f.folded(dir, name); c != nil {
		f.op(CheckAbsent(c, true))
		f.op(Put(c, name))
	}
	f.times(dir, now)
}

// Mkdir makes name in dir, whose parent was read as up, the directory child: a sequence of dir's
// slot or, when fresh, sequence 1 of a slot from the daemon's reservation, whose sequences
// 2 … InodeBatch + 1 the daemon keeps.
func (f *FS) Mkdir(dir, up uint64, name []byte, child uint64, a *Attr, fresh bool, now int64) error {
	f.begin(slotOf(dir))
	f.create(dir, up, name, child, a.Mode, now)
	f.op(Update(f.key(dir, InodeAttr, nil), AttrNlink, Add, 1))
	if fresh {
		n := f.slotKey(slotOf(child), SlotNext, 0)
		f.op(CheckAbsent(n, false))
		f.op(Put(n, f.u64(2+InodeBatch)))
	}
	f.op(Put(f.key(child, InodeAttr, nil), f.attr(a)))
	f.op(Put(f.key(child, InodeParent, nil), f.u64(dir)))
	return f.end()
}

// Link makes name in dir, whose parent was read as up, a further entry of child, read as a.
func (f *FS) Link(dir, up uint64, name []byte, child uint64, a *Attr, now int64) error {
	f.begin(slotOf(dir))
	f.create(dir, up, name, child, a.Mode, now)
	k := f.key(child, InodeAttr, nil)
	f.op(Check(k, f.attr(a), false))
	b := *a
	b.Nlink, b.Ctime = b.Nlink+1, now
	f.op(Put(k, f.attr(&b)))
	return f.end()
}

// Unlink removes name, child's entry in dir; child was read as a, with its purge entry pending.
func (f *FS) Unlink(dir uint64, name []byte, child uint64, a *Attr, open bool, pending *Purge, now int64) error {
	f.begin(slotOf(dir))
	f.drop(dir, name, child, a.Mode, now)
	f.unlink(child, a, open, pending, now)
	return f.end()
}

func (f *FS) drop(dir uint64, name []byte, child, mode uint64, now int64) {
	d := f.key(dir, InodeEntry, name)
	f.op(Check(d, f.entry(child, mode), true))
	f.op(Delete(d))
	if c := f.folded(dir, name); c != nil {
		f.op(Delete(c))
	}
	f.times(dir, now)
}

// Rmdir removes name, the empty directory child in dir.
func (f *FS) Rmdir(dir uint64, name []byte, child uint64, now int64) error {
	f.begin(slotOf(dir))
	f.drop(dir, name, child, ModeDir, now)
	f.op(CheckEmpty(f.bound(child, InodeEntry), f.bound(child, InodeEntry+1), true))
	f.op(DeleteRange(f.bound(child), f.bound(child+1)))
	f.op(Update(f.key(dir, InodeAttr, nil), AttrNlink, Add, 1<<64-1))
	return f.end()
}

// Rename moves r.Src in r.SrcDir to r.Dst in r.DstDir, replacing what the destination named; when
// both name the same inode, it changes nothing, as POSIX says.
func (f *FS) Rename(r *Rename, now int64) error {
	dir := r.ChildMode&ModeType == ModeDir
	across := r.SrcDir != r.DstDir
	switch {
	case dir && across:
		if err := ancestry(r); err != nil {
			return err
		}
	case across && r.DstDir != RootInode && (len(r.Ancestors) == 0 || r.Ancestors[0].Dir != r.DstDir):
		return ErrAncestry
	}
	f.begin(slotOf(r.DstDir))
	src := f.key(r.SrcDir, InodeEntry, r.Src)
	f.op(Check(src, f.entry(r.Child, r.ChildMode), true))
	dst := f.key(r.DstDir, InodeEntry, r.Dst)
	if r.Over == r.Child {
		f.op(Check(dst, f.entry(r.Child, r.ChildMode), false))
		return f.end()
	}
	f.op(Delete(src))
	cs := f.folded(r.SrcDir, r.Src)
	if cs != nil {
		f.op(Delete(cs))
	}
	c := f.folded(r.DstDir, r.Dst)
	switch {
	case r.Over == 0:
		f.op(CheckAbsent(dst, false))
		switch {
		case c == nil:
		case bytes.Equal(c, cs):
			f.op(Check(c, r.Src, true))
		default:
			f.op(CheckAbsent(c, true))
		}
	case r.OverAttr.Mode&ModeType == ModeDir:
		f.op(Check(dst, f.entry(r.Over, r.OverAttr.Mode), false))
		f.op(CheckEmpty(f.bound(r.Over, InodeEntry), f.bound(r.Over, InodeEntry+1), true))
		f.op(DeleteRange(f.bound(r.Over), f.bound(r.Over+1)))
		f.op(Update(f.key(r.DstDir, InodeAttr, nil), AttrNlink, Add, 1<<64-1))
	default:
		f.op(Check(dst, f.entry(r.Over, r.OverAttr.Mode), false))
		f.unlink(r.Over, r.OverAttr, r.OverOpen, r.OverPending, now)
	}
	f.op(Put(dst, f.entry(r.Child, r.ChildMode)))
	if c != nil {
		f.op(Put(c, r.Dst))
	}
	f.times(r.SrcDir, now)
	if !across {
		return f.end()
	}
	f.times(r.DstDir, now)
	if dir {
		f.op(Update(f.key(r.SrcDir, InodeAttr, nil), AttrNlink, Add, 1<<64-1))
		f.op(Update(f.key(r.DstDir, InodeAttr, nil), AttrNlink, Add, 1))
		f.op(Put(f.key(r.Child, InodeParent, nil), f.u64(r.DstDir)))
	}
	for _, a := range r.Ancestors {
		f.live(a.Dir, a.Parent)
	}
	return f.end()
}

// ancestry checks that r's ancestors chain from its new parent to the root without the moved directory.
func ancestry(r *Rename) error {
	next := r.DstDir
	for _, a := range r.Ancestors {
		if a.Dir == r.Child {
			return ErrLoop
		}
		if a.Dir != next {
			return ErrAncestry
		}
		next = a.Parent
	}
	switch {
	case next == r.Child || r.Child == RootInode:
		return ErrLoop
	case next != RootInode:
		return ErrAncestry
	}
	return nil
}

// SetDirAttr sets fields of directory dir's attributes, whose parent was read as up, and moves its
// ctime to at least now, by Updates alone, since every create in it changes them.
func (f *FS) SetDirAttr(dir, up uint64, now int64, set ...Field) error {
	f.begin(slotOf(dir))
	f.live(dir, up)
	k := f.key(dir, InodeAttr, nil)
	for _, s := range set {
		f.op(Update(k, s.Off, Set, s.V))
	}
	f.op(Update(k, AttrCtime, Bump, TimeField(now)))
	return f.end()
}

// SetAttr replaces inode's attributes, read as old, with b: a close, fsync, size change,
// alloc_end raise, truncate or setattr of a file. A shrink lowers alloc_end to the new size's
// chunks and merges the bytes from the new size on into the purge entry, pending as read.
func (f *FS) SetAttr(inode uint64, old, b *Attr, pending *Purge) error {
	f.begin(slotOf(inode))
	k := f.key(inode, InodeAttr, nil)
	f.op(Check(k, f.attr(old), false))
	n := *b
	if n.Size < old.Size {
		n.AllocEnd = min(n.AllocEnd, Chunks(n.Size))
		if cut := (Purge{n.Size, old.AllocEnd}); pending != nil || !cut.empty() {
			f.purge(inode, pending, cut)
		}
	}
	f.op(Put(k, f.attr(&n)))
	f.used(inode, n.Size-old.Size)
	return f.end()
}

// Lock takes a flock of mode on inode for session, given the holders a scan of the inode's l/
// found, in key order; a shared lock needs only l/x/. The keys of dead sessions and session's own
// go in the same Txn, and one a live session holds in a conflicting mode is ErrLocked.
func (f *FS) Lock(inode uint64, mode byte, session uint64, held []Holder) error {
	f.begin(slotOf(inode))
	from := f.bound(inode, InodeLock)
	if mode == LockShared {
		from = f.bound(inode, InodeLock, LockExclusive)
	}
	for _, h := range held {
		if mode == LockShared && h.Mode != LockExclusive {
			continue
		}
		if h.Live && h.Session != session {
			f.Txn.Ops = f.Txn.Ops[:0]
			return ErrLocked
		}
		k := f.lockKey(inode, h.Mode, h.Session)
		if bytes.Compare(from, k) < 0 {
			f.op(CheckEmpty(from, k, false))
		}
		f.op(Check(k, nil, false))
		f.op(Delete(k))
		start := len(f.buf)
		f.buf = append(append(f.buf, k...), 0)
		from = f.val(start)
	}
	f.op(CheckEmpty(from, f.bound(inode, InodeLock+1), false))
	f.op(Put(f.lockKey(inode, mode, session), nil))
	return f.end()
}

// Unlock drops session's lock of mode on inode.
func (f *FS) Unlock(inode uint64, mode byte, session uint64) error {
	f.begin(slotOf(inode))
	f.op(Delete(f.lockKey(inode, mode, session)))
	return f.end()
}

// SetXattr sets inode's xattr name to v, in pieces past MaxValue − 1 bytes, dropping the pieces
// past its own of a value that had old pieces. inode was read as a, with up as its parent if a
// directory.
func (f *FS) SetXattr(inode uint64, a *Attr, up uint64, name, v []byte, old int) error {
	f.begin(slotOf(inode))
	f.split(inode, InodeXattr, name, v, old)
	f.exists(inode, a, up)
	return f.end()
}

// RemoveXattr removes inode's xattr name, whose head counted pieces; inode was read as SetXattr's.
func (f *FS) RemoveXattr(inode uint64, a *Attr, up uint64, name []byte, pieces int) error {
	f.begin(slotOf(inode))
	f.split(inode, InodeXattr, name, nil, pieces)
	f.Txn.Ops[0] = Delete(f.Txn.Ops[0].Key)
	f.exists(inode, a, up)
	return f.end()
}

// exists checks that inode, read as a with up as its parent if a directory, was not removed: a
// directory by its P, a file by its attributes, so a write racing a setxattr makes it retry.
func (f *FS) exists(inode uint64, a *Attr, up uint64) {
	if a.Mode&ModeType == ModeDir {
		f.live(inode, up)
	} else {
		f.op(Check(f.key(inode, InodeAttr, nil), f.attr(a), false))
	}
}
