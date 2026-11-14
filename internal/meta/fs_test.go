package meta

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/fnv"
	"math/rand/v2"
	"strings"
	"testing"
	"unicode"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

const second = int64(1e9)

var (
	rootDir  = Attr{Mode: ModeDir | 0o755, Nlink: 2, Mtime: 10 * second, Ctime: 10 * second}
	plain    = Attr{Mode: ModeFile | 0o644, Nlink: 1, Mtime: 10 * second, Ctime: 10 * second}
	symlinkA = Attr{Mode: ModeSymlink | 0o777, Nlink: 1}
)

// fsT is volume 5 on a State, its operations built as a daemon builds them from what it read.
type fsT struct {
	t testing.TB
	s *State
	f FS
}

func newFS(t testing.TB) *fsT {
	h := &fsT{t: t, s: newTestState(t), f: FS{Volume: 5}}
	h.ok(h.f.Root(&rootDir))
	return h
}

// do applies the Txn the last build made.
func (h *fsT) do(err error) Result {
	h.t.Helper()
	if err != nil {
		h.t.Fatal(err)
	}
	return run(h.t, h.s, h.f.Txn)
}

// ok applies the last build and fails the test if a check failed.
func (h *fsT) ok(err error) Result {
	h.t.Helper()
	r := h.do(err)
	if r.FailedCheck != -1 {
		h.t.Fatalf("check %d %+v failed", r.FailedCheck, h.f.Txn.Ops[r.FailedCheck])
	}
	return r
}

// fails applies the last build and wants a check to fail, final or not.
func (h *fsT) fails(err error, final bool) {
	h.t.Helper()
	r := h.do(err)
	if r.FailedCheck == -1 || h.f.Txn.Ops[r.FailedCheck].Final != final {
		h.t.Fatalf("FailedCheck %d, want a check failed with final %v", r.FailedCheck, final)
	}
}

// built is a copy of the last build, which the next one overwrites.
func (h *fsT) built() Txn {
	var d Txn
	if _, err := decodeTxn(appendTxn(nil, &h.f.Txn), &d); err != nil {
		h.t.Fatal(err)
	}
	return d
}

func (h *fsT) key(inode uint64, tag byte, name string) []byte {
	k := Key{Space: SpaceFS, Volume: 5, Inode: inode, Tag: tag}
	if name != "" {
		k.Name = b(name)
	}
	return mustKey(&k)
}

func (h *fsT) slotKey(slot uint32, tag byte, id uint64) []byte {
	return mustKey(&Key{Space: SpaceFS, Volume: 5, Inode: ino(slot, 0), Tag: tag, ID: id})
}

func (h *fsT) has(k []byte) bool { _, ok := h.s.tree.Get(k); return ok }

func (h *fsT) attr(inode uint64) Attr {
	h.t.Helper()
	v, _ := h.s.tree.Get(h.key(inode, InodeAttr, ""))
	var a Attr
	if err := DecodeAttr(v, &a); err != nil {
		h.t.Fatalf("attributes of %x: %v", inode, err)
	}
	return a
}

// up is dir's parent, 0 for the root or a directory with no P.
func (h *fsT) up(dir uint64) uint64 {
	v, _ := h.s.tree.Get(h.key(dir, InodeParent, ""))
	p, _ := DecodeParent(v)
	return p
}

// read is inode's attributes, as a daemon read them.
func (h *fsT) read(inode uint64) *Attr {
	a := h.attr(inode)
	return &a
}

func (h *fsT) counter(slot uint32, tag byte) uint64 {
	return field(h.s, string(h.slotKey(slot, tag, 0)), 0)
}

func (h *fsT) purge(inode uint64) *Purge {
	v, ok := h.s.tree.Get(h.slotKey(slotOf(inode), SlotPurge, inode))
	if !ok {
		return nil
	}
	p, err := DecodePurge(v)
	if err != nil {
		h.t.Fatal(err)
	}
	return &p
}

// keysOf is every key of inode.
func (h *fsT) keysOf(inode uint64) []string {
	var lo, hi FS
	lo.Volume, hi.Volume = 5, 5
	ks, _ := scanKeys(h.t.(*testing.T), h.s, lo.bound(inode), hi.bound(inode+1), 1000)
	return ks
}

// reserveSlots runs a daemon's slot reservation after NeedSlots, reading f before the first.
func (h *fsT) reserveSlots(a *Allocator, lowest, limit uint32) {
	if !a.Free() {
		a.FreeRead(h.has(h.slotKey(lowest, SlotFree, 0)))
	}
	if a.Free() {
		a.SlotsReserved(h.ok(h.f.ReserveSlots(lowest)).Updated[0], limit)
	}
}

func (h *fsT) mkdir(dir uint64, name string, child uint64) {
	h.t.Helper()
	h.ok(h.f.Mkdir(dir, h.up(dir), b(name), child, &rootDir, child&seqMask == 1, 20*second))
}

func (h *fsT) create(dir uint64, name string, child uint64, a Attr) {
	h.t.Helper()
	h.ok(h.f.Create(dir, h.up(dir), b(name), child, &a, nil, 20*second))
}

func TestFSValuesRoundTrip(t *testing.T) {
	a := Attr{Mode: ModeFile | 0o640, Nlink: 3, UID: 1000, GID: 100, Size: 5000, AllocEnd: 1, Atime: -5 * second, Mtime: 1000*second + 5, Ctime: 1 << 62}
	var d Attr
	if err := DecodeAttr(AppendAttr(nil, &a), &d); err != nil || d != a {
		t.Fatalf("attributes %+v came back %+v, %v", a, d, err)
	}
	if TimeField(-1) >= TimeField(0) || TimeField(0) >= TimeField(1) {
		t.Fatal("a time before 1970 does not order below later ones")
	}
	if i, m, err := DecodeEntry(AppendEntry(nil, ino(2, 7), ModeSymlink|0o777)); err != nil || i != ino(2, 7) || m != ModeSymlink {
		t.Fatalf("entry came back %x %o %v", i, m, err)
	}
	p := Purge{5000, 4}
	if q, err := DecodePurge(AppendPurge(nil, p)); err != nil || q != p {
		t.Fatalf("purge entry came back %+v %v", q, err)
	}
	for _, bad := range []Purge{{0, 0}, {core.ChunkSize, 1}} {
		if _, err := DecodePurge(AppendPurge(nil, bad)); err == nil {
			t.Errorf("purge entry %+v, which frees nothing, decoded", bad)
		}
	}
	if Chunks(0) != 0 || Chunks(1) != 1 || Chunks(core.ChunkSize) != 1 || Chunks(core.ChunkSize+1) != 2 {
		t.Fatal("Chunks")
	}
}

func TestMtimeNeverGoesBackwards(t *testing.T) {
	for _, c := range []struct{ stored, write, reported, want int64 }{
		{1000*second + 5, 999*second + 9e8, 0, 1000*second + 6},               // the writer's clock behind
		{1000 * second, 1001 * second, 0, 1001 * second},                      // the writer's clock ahead
		{1000 * second, 999*second + 9e8, 1000*second + 40, 1000*second + 40}, // write-shared, the node reported later
	} {
		if got := Mtime(c.stored, c.write, c.reported); got != c.want {
			t.Errorf("Mtime(%d, %d, %d) = %d, want %d", c.stored, c.write, c.reported, got, c.want)
		}
	}
}

func TestReservations(t *testing.T) {
	h := newFS(t)
	var a Allocator
	if !a.NeedSlots() || a.NeedSlots() {
		t.Fatal("NeedSlots must ask once until answered")
	}
	h.ok(h.f.ReserveSlots(0))
	if len(h.f.Txn.Ops) != 1 || h.f.Txn.Replay != nil {
		t.Fatal("a slot reservation is one Update in a Txn of its own")
	}
	h.reserveSlots(&a, 0, 1<<SlotBits)
	if s, _ := a.Slot(); s != 17 {
		t.Fatalf("the second reservation's first slot is %d, want 17: the root's f starts at 1", s)
	}
	photos := ino(17, 1)
	h.mkdir(RootInode, "photos", photos)
	a.Adopt(17)
	if h.counter(17, SlotNext) != 66 || h.attr(RootInode).Nlink != 3 {
		t.Fatalf("n %d, root nlink %d after a mkdir in a fresh slot", h.counter(17, SlotNext), h.attr(RootInode).Nlink)
	}
	for want := uint64(2); want <= 65; want++ {
		i, ok := a.Inode(17)
		if !ok || i != ino(17, want) {
			t.Fatalf("inode %x %v, want sequence %d with no reservation round", i, ok, want)
		}
		if left := 65 - want; a.NeedInodes(17) != (left == InodePrefetch) {
			t.Fatalf("NeedInodes with %d left", left)
		}
		if left := 65 - want; left == InodePrefetch {
			r := h.ok(h.f.ReserveInodes(17))
			a.InodesReserved(17, r.Updated[0])
		}
	}
	if i, _ := a.Inode(17); i != ino(17, 66) {
		t.Fatalf("after the first batch %x, want the prefetched 17:66", i)
	}
	// A fresh slot's n set again is a counter reset: caught.
	h.fails(h.f.Mkdir(RootInode, 0, b("again"), photos, &rootDir, true, 0), false)

	// The last IDs of a slot.
	must(h.s.tree.Put(h.slotKey(2, SlotNext, 0), be(1<<SeqBits-1)))
	var c Allocator
	c.NeedInodes(2)
	c.InodesReserved(2, h.ok(h.f.ReserveInodes(2)).Updated[0])
	if i, ok := c.Inode(2); !ok || i != ino(2, 1<<SeqBits-1) {
		t.Fatalf("the slot's last ID came out %x %v", i, ok)
	}
	if _, ok := c.Inode(2); ok || !c.SlotFull(2) || c.NeedInodes(2) {
		t.Fatal("a slot whose IDs ran out must be full, so the next create goes to a fresh slot")
	}
	// A shard with no free slot left: mkdir shares its parent's slot.
	var d Allocator
	d.NeedSlots()
	d.SlotsReserved(1<<SlotBits+SlotBatch, 1<<SlotBits)
	if _, ok := d.Slot(); ok || !d.NoSlots() {
		t.Fatal("a shard past its last slot must have none")
	}
}

// A shard from slot 100 whose f no split wrote, reserved from by one fresh Allocator after
// another, as daemons and restarts would: each finds f absent, sends nothing and hands out nothing.
func TestShardWithoutFreeKeyHandsOutNoSlot(t *testing.T) {
	h := newFS(t)
	const lowest, limit = 100, 2000
	for i := 1; i <= 10; i++ {
		var a Allocator
		if !a.NeedSlots() {
			t.Fatal("a fresh Allocator must ask for slots")
		}
		h.reserveSlots(&a, lowest, limit)
		if s, ok := a.Slot(); ok || !a.NoSlots() || a.NeedSlots() {
			t.Fatalf("reservation %d on a shard with no f handed out slot %d", i, s)
		}
		if h.has(h.slotKey(lowest, SlotFree, 0)) {
			t.Fatalf("reservation %d on a shard with no f wrote f = %d", i, h.counter(lowest, SlotFree))
		}
	}
}

func TestCreateMkdirLinkSymlink(t *testing.T) {
	h := newFS(t)
	photos := ino(2, 1)
	h.mkdir(RootInode, "photos", photos)
	if p, _ := h.s.tree.Get(h.key(photos, InodeParent, "")); !bytes.Equal(p, be(RootInode)) {
		t.Fatal("a directory's P must name its parent")
	}
	beach := ino(2, 2)
	h.create(photos, "beach.jpg", beach, plain)
	if v, _ := h.s.tree.Get(h.key(photos, InodeEntry, "beach.jpg")); !bytes.Equal(v, AppendEntry(nil, beach, ModeFile)) {
		t.Fatalf("entry %x", v)
	}
	if pa := h.attr(photos); pa.Mtime != 20*second || pa.Ctime != 20*second {
		t.Fatalf("parent times %d %d", pa.Mtime, pa.Ctime)
	}
	h.fails(h.f.Create(photos, h.up(photos), b("beach.jpg"), ino(2, 3), &plain, nil, 5*second), true)

	// Two racing creates, both built before either applies, commit in one round, and a create from
	// a node whose clock is behind still moves the directory's times by 1 ns.
	h.f.Create(photos, h.up(photos), b("a"), ino(2, 4), &plain, nil, 5*second)
	one := h.built()
	h.f.Create(photos, h.up(photos), b("b"), ino(2, 5), &plain, nil, 5*second)
	two := h.f.Txn
	for _, x := range []Txn{one, two} {
		if r := run(t, h.s, x); r.FailedCheck != -1 {
			t.Fatal("a racing create failed")
		}
	}
	if pa := h.attr(photos); pa.Mtime != 20*second+2 {
		t.Fatalf("mtime %d after two creates from a clock behind, want 2 ns later", pa.Mtime)
	}

	// mkdir with no free slot shares its parent's slot.
	h.mkdir(photos, "sub", ino(2, 6))
	if h.attr(photos).Nlink != 3 || h.counter(2, SlotNext) != 66 {
		t.Fatal("mkdir in the parent's slot")
	}

	h.ok(h.f.Link(photos, h.up(photos), b("b2"), beach, &plain, 30*second))
	if a := h.attr(beach); a.Nlink != 2 || a.Ctime != 30*second {
		t.Fatalf("link left nlink %d ctime %d", a.Nlink, a.Ctime)
	}
	two2 := h.attr(beach)
	h.fails(h.f.Link(photos, h.up(photos), b("b2"), beach, &two2, 30*second), true)

	target := bytes.Repeat([]byte("t"), 4095)
	link := ino(2, 7)
	h.ok(h.f.Create(photos, h.up(photos), b("ln"), link, &symlinkA, target, 30*second))
	if n := len(h.keysOf(link)); n != 1+5 {
		t.Fatalf("a 4,095-byte target took %d values, want 5 and the attributes", n)
	}
	head, _ := h.s.tree.Get(h.key(link, InodeTarget, ""))
	m, got, err := DecodeHead(head)
	if err != nil {
		t.Fatal(err)
	}
	got = bytes.Clone(got)
	for i := 1; i <= m; i++ {
		p, _ := h.s.tree.Get(mustKey(&Key{Space: SpaceFS, Volume: 5, Inode: link, Tag: InodeTarget | PieceBit, Piece: byte(i)}))
		if got, err = AppendPiece(got, p, i, m); err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(got, target) {
		t.Fatal("readlink did not return the target")
	}
}

func TestUnlink(t *testing.T) {
	h := newFS(t)
	dir := RootInode
	f := ino(0, 2)
	a := plain
	a.Size, a.AllocEnd = 3, 1
	h.create(dir, "f", f, a)
	h.ok(h.f.SetXattr(f, h.read(f), h.up(f), b("user.x"), b("v"), 0))
	h.ok(h.f.Lock(f, LockExclusive, 7, nil))
	must(h.s.tree.Put(h.slotKey(0, SlotUsed, 0), be(10)))

	// Last link, not open: one Txn, and nothing of the inode stays.
	h.ok(h.f.Unlink(dir, b("f"), f, &a, false, nil, 30*second))
	if ks := h.keysOf(f); len(ks) != 0 || h.has(h.key(dir, InodeEntry, "f")) {
		t.Fatalf("keys %q left after the last unlink", ks)
	}
	if p := h.purge(f); p == nil || *p != (Purge{0, 1}) || h.counter(0, SlotUsed) != 7 {
		t.Fatalf("purge %v, used %d", p, h.counter(0, SlotUsed))
	}

	// Another link: only the entry goes.
	g := ino(0, 3)
	h.create(dir, "g", g, plain)
	h.ok(h.f.Link(dir, h.up(dir), b("g2"), g, &plain, 30*second))
	ga := h.attr(g)
	h.ok(h.f.Unlink(dir, b("g2"), g, &ga, false, nil, 40*second))
	if na := h.attr(g); na.Nlink != 1 || na.Ctime != 40*second || h.purge(g) != nil {
		t.Fatal("unlink of one of two links")
	}
	// A link racing the last unlink fails its check on the attributes, where the unlink would
	// otherwise remove an inode an entry still names.
	stale := h.attr(g)
	h.ok(h.f.Link(dir, h.up(dir), b("g3"), g, &stale, 50*second))
	h.fails(h.f.Unlink(dir, b("g"), g, &stale, false, nil, 50*second), false)

	// Still open: an orphan.
	o := ino(0, 4)
	h.create(dir, "o", o, plain)
	h.ok(h.f.Unlink(dir, b("o"), o, &plain, true, nil, 60*second))
	if !h.has(h.slotKey(0, SlotOrphan, o)) || h.attr(o).Nlink != 0 {
		t.Fatal("an open file's last unlink must leave an orphan")
	}

	// rmdir.
	d := ino(3, 1)
	h.mkdir(dir, "d", d)
	h.create(d, "x", ino(3, 2), plain)
	h.fails(h.f.Rmdir(dir, b("d"), d, 70*second), true)
	h.ok(h.f.Unlink(d, b("x"), ino(3, 2), &plain, false, nil, 70*second))
	nlink := h.attr(dir).Nlink
	h.ok(h.f.Rmdir(dir, b("d"), d, 70*second))
	if len(h.keysOf(d)) != 0 || h.attr(dir).Nlink != nlink-1 {
		t.Fatal("rmdir of an empty directory")
	}
}

func TestRename(t *testing.T) {
	h := newFS(t)
	root := RootInode
	f := ino(0, 2)
	h.create(root, "f", f, plain)
	h.ok(h.f.Rename(&Rename{SrcDir: root, DstDir: root, Src: b("f"), Dst: b("g"), Child: f, ChildMode: ModeFile}, 30*second))
	if h.has(h.key(root, InodeEntry, "f")) || !h.has(h.key(root, InodeEntry, "g")) {
		t.Fatal("rename within a directory")
	}
	h.fails(h.f.Rename(&Rename{SrcDir: root, DstDir: root, Src: b("f"), Dst: b("h"), Child: f, ChildMode: ModeFile}, 30*second), true)

	// Over a non-empty directory: ENOTEMPTY, nothing changes.
	d, e := ino(1, 1), ino(2, 1)
	h.mkdir(root, "d", d)
	h.mkdir(root, "e", e)
	h.create(e, "x", ino(2, 2), plain)
	ea := h.attr(e)
	h.fails(h.f.Rename(&Rename{SrcDir: root, DstDir: root, Src: b("d"), Dst: b("e"), Child: d, ChildMode: ModeDir, Over: e, OverAttr: &ea}, 30*second), true)

	// Over a file someone has open, and over one no one has.
	o, p := ino(0, 3), ino(0, 4)
	big := plain
	big.Size, big.AllocEnd = 100, 1
	h.create(root, "o", o, plain)
	h.create(root, "p", p, big)
	h.ok(h.f.Rename(&Rename{SrcDir: root, DstDir: root, Src: b("g"), Dst: b("o"), Child: f, ChildMode: ModeFile, Over: o, OverAttr: &plain, OverOpen: true}, 30*second))
	if !h.has(h.slotKey(0, SlotOrphan, o)) {
		t.Fatal("the replaced open file must be an orphan")
	}
	h.ok(h.f.Rename(&Rename{SrcDir: root, DstDir: root, Src: b("o"), Dst: b("p"), Child: f, ChildMode: ModeFile, Over: p, OverAttr: &big}, 30*second))
	if len(h.keysOf(p)) != 0 || *h.purge(p) != (Purge{0, 1}) {
		t.Fatal("the replaced file's keys must go and its chunks join the purge list")
	}
}

func TestRenameAcrossDirectories(t *testing.T) {
	h := newFS(t)
	root := RootInode
	a, bb, c := ino(1, 1), ino(2, 1), ino(3, 1)
	h.mkdir(root, "a", a)
	h.mkdir(a, "b", bb)
	h.mkdir(root, "c", c)
	move := Rename{SrcDir: a, DstDir: c, Src: b("b"), Dst: b("b"), Child: bb, ChildMode: ModeDir, Ancestors: []Ancestor{{c, root}}}
	h.f.Rename(&move, 30*second)
	built := h.built()
	h.create(c, "meanwhile", ino(3, 2), plain)
	if r := run(t, h.s, built); r.FailedCheck != -1 {
		t.Fatal("a create in the new parent failed the rename")
	}
	if h.attr(a).Nlink != 2 || h.attr(c).Nlink != 3 {
		t.Fatal("link counts after moving a directory")
	}
	if v, _ := h.s.tree.Get(h.key(bb, InodeParent, "")); !bytes.Equal(v, be(c)) {
		t.Fatal("the moved directory's P")
	}

	// /a into /a/b/c's place: a directory never moves under itself.
	cc := ino(4, 1)
	h.mkdir(c, "cc", cc)
	loop := Rename{SrcDir: root, DstDir: cc, Src: b("c"), Dst: b("x"), Child: c, ChildMode: ModeDir, Ancestors: []Ancestor{{cc, c}, {c, root}}}
	if err := h.f.Rename(&loop, 0); !errors.Is(err, ErrLoop) {
		t.Fatalf("a move under itself: %v", err)
	}
	if err := h.f.Rename(&Rename{SrcDir: root, DstDir: cc, Child: a, ChildMode: ModeDir, Src: b("a"), Dst: b("a"), Ancestors: []Ancestor{{cc, c}}}, 0); !errors.Is(err, ErrAncestry) {
		t.Fatalf("ancestors short of the root: %v", err)
	}

	// Moving /a under /c/cc while another node moves /c into /a: the other move commits first, and
	// this one's check on cc's ancestors fails, so it reads again and finds the loop.
	mine := Rename{SrcDir: root, DstDir: cc, Src: b("a"), Dst: b("a"), Child: a, ChildMode: ModeDir, Ancestors: []Ancestor{{cc, c}, {c, root}}}
	h.f.Rename(&mine, 40*second)
	built = h.built()
	h.ok(h.f.Rename(&Rename{SrcDir: root, DstDir: a, Src: b("c"), Dst: b("c"), Child: c, ChildMode: ModeDir, Ancestors: []Ancestor{{a, root}}}, 40*second))
	if r := run(t, h.s, built); r.FailedCheck == -1 || built.Ops[r.FailedCheck].Final {
		t.Fatal("a rename whose ancestors moved must fail a check that is not final")
	}
}

// An operation built before an rmdir or a last unlink commits after it: each fails a check that
// is not final, so the daemon reads again and finds the inode gone, and none leaves a key behind.
func TestNothingLandsOnARemovedInode(t *testing.T) {
	for _, c := range []struct {
		name  string
		build func(h *fsT, d, f uint64) error
	}{
		{"create", func(h *fsT, d, f uint64) error {
			return h.f.Create(d, RootInode, b("f"), ino(0, 9), &plain, nil, 30*second)
		}},
		{"symlink", func(h *fsT, d, f uint64) error {
			return h.f.Create(d, RootInode, b("s"), ino(0, 9), &symlinkA, b("t"), 30*second)
		}},
		{"mkdir", func(h *fsT, d, f uint64) error {
			return h.f.Mkdir(d, RootInode, b("m"), ino(0, 9), &rootDir, false, 30*second)
		}},
		{"fresh mkdir", func(h *fsT, d, f uint64) error {
			return h.f.Mkdir(d, RootInode, b("m"), ino(9, 1), &rootDir, true, 30*second)
		}},
		{"link", func(h *fsT, d, f uint64) error { return h.f.Link(d, RootInode, b("l"), f, &plain, 30*second) }},
		{"rename a file in", func(h *fsT, d, f uint64) error {
			return h.f.Rename(&Rename{SrcDir: RootInode, DstDir: d, Src: b("f"), Dst: b("f"), Child: f, ChildMode: ModeFile, Ancestors: []Ancestor{{d, RootInode}}}, 30*second)
		}},
		{"chmod", func(h *fsT, d, f uint64) error {
			return h.f.SetDirAttr(d, RootInode, 30*second, Field{AttrMode, ModeDir | 0o700})
		}},
		{"setxattr", func(h *fsT, d, f uint64) error { return h.f.SetXattr(d, &rootDir, RootInode, b("user.x"), b("v"), 0) }},
	} {
		name, build := c.name, c.build
		h := newFS(t)
		d, f := ino(0, 2), ino(0, 3)
		h.mkdir(RootInode, "d", d)
		h.create(RootInode, "f", f, plain)
		must(build(h, d, f))
		late := h.built()
		h.ok(h.f.Rmdir(RootInode, b("d"), d, 30*second))
		if r := run(t, h.s, late); r.FailedCheck == -1 || late.Ops[r.FailedCheck].Final {
			t.Errorf("%s committed under the removed directory, or failed final", name)
		}
		if ks := h.keysOf(d); len(ks) != 0 || h.has(h.slotKey(9, SlotNext, 0)) {
			t.Errorf("%s left %q", name, ks)
		}
	}

	h := newFS(t)
	f := ino(0, 2)
	h.create(RootInode, "f", f, plain)
	h.f.SetXattr(f, &plain, 0, b("user.x"), b("v"), 0)
	set := h.built()
	h.f.RemoveXattr(f, &plain, 0, b("user.x"), 0)
	remove := h.built()
	h.ok(h.f.Unlink(RootInode, b("f"), f, &plain, false, nil, 30*second))
	for _, late := range []Txn{set, remove} {
		if r := run(t, h.s, late); r.FailedCheck == -1 || len(h.keysOf(f)) != 0 {
			t.Fatalf("an xattr of a removed file left %q", h.keysOf(f))
		}
	}
}

// rename(x, x), and a rename between two links of one inode, change nothing and succeed.
func TestRenameOntoItselfChangesNothing(t *testing.T) {
	h := newFS(t)
	f := ino(0, 2)
	h.create(RootInode, "x", f, plain)
	h.ok(h.f.Link(RootInode, 0, b("y"), f, &plain, 30*second))
	before := h.keysOf(f)
	for _, dst := range []string{"x", "y"} {
		h.ok(h.f.Rename(&Rename{SrcDir: RootInode, DstDir: RootInode, Src: b("x"), Dst: b(dst), Child: f, ChildMode: ModeFile, Over: f, OverAttr: h.read(f)}, 40*second))
		if !h.has(h.key(RootInode, InodeEntry, "x")) || !h.has(h.key(RootInode, InodeEntry, "y")) || len(h.keysOf(f)) != len(before) || h.attr(f).Nlink != 2 {
			t.Fatalf("rename x to %s changed %q", dst, h.keysOf(f))
		}
	}
	h.fails(h.f.Rename(&Rename{SrcDir: RootInode, DstDir: RootInode, Src: b("z"), Dst: b("x"), Child: f, ChildMode: ModeFile, Over: f, OverAttr: h.read(f)}, 40*second), true)
}

// A symlink's size is its target's length, which the slot's byte counter does not count.
func TestSymlinksLeaveTheByteCounter(t *testing.T) {
	h := newFS(t)
	s := ino(0, 2)
	a := symlinkA
	a.Size = 5
	h.ok(h.f.Create(RootInode, 0, b("s"), s, &a, b("12345"), 20*second))
	h.ok(h.f.Unlink(RootInode, b("s"), s, &a, false, nil, 30*second))
	if u := h.counter(0, SlotUsed); u != 0 {
		t.Fatalf("u = %d after a symlink's create and unlink", int64(u))
	}
}

func TestAttributes(t *testing.T) {
	h := newFS(t)
	root := RootInode
	d := ino(1, 1)
	h.mkdir(root, "d", d)

	// chmod while 64 creates land: no check on the directory's attributes, so all commit.
	h.f.SetDirAttr(d, h.up(d), 30*second, Field{AttrMode, ModeDir | 0o700})
	chmod := h.built()
	for i := range uint64(64) {
		h.create(d, string(rune('A'+i)), ino(1, 2+i), plain)
	}
	if r := run(t, h.s, chmod); r.FailedCheck != -1 || h.attr(d).Mode != ModeDir|0o700 {
		t.Fatal("chmod")
	}

	// Size from 0 to 3: the slot's counter by Add.
	f := ino(1, 70)
	h.create(d, "file", f, plain)
	three := plain
	three.Size, three.AllocEnd = 3, 1
	h.ok(h.f.SetAttr(f, &plain, &three, nil))
	if h.counter(1, SlotUsed) != 3 {
		t.Fatal("used")
	}

	// A close whose check loses a race with another node's size change retries from a new read.
	four := three
	four.Size = 4
	h.ok(h.f.SetAttr(f, &three, &four, nil))
	h.fails(h.f.SetAttr(f, &three, &plain, nil), false)

	// Truncates.
	t16 := plain
	t16.Size, t16.AllocEnd = 16<<20, 4
	g := ino(1, 71)
	h.create(d, "big", g, t16)
	h.ok(h.f.SetAttr(g, &t16, &t16, nil))
	half := t16
	half.Size = 8 << 20
	h.ok(h.f.SetAttr(g, &t16, &half, nil))
	if a := h.attr(g); a.AllocEnd != 2 || *h.purge(g) != (Purge{8 << 20, 4}) {
		t.Fatalf("16 MiB to 8 MiB: alloc_end %d, purge %+v", a.AllocEnd, *h.purge(g))
	}
	cut := h.attr(g)
	small := cut
	small.Size = 5000
	h.ok(h.f.SetAttr(g, &cut, &small, h.purge(g)))
	if a := h.attr(g); a.AllocEnd != 1 || *h.purge(g) != (Purge{5000, 4}) {
		t.Fatalf("then to 5,000: alloc_end %d, purge %+v, want the union", a.AllocEnd, *h.purge(g))
	}
	// A truncate with a stale purge entry read fails on that entry, not final.
	h.fails(h.f.SetAttr(g, h.read(g), &plain, &Purge{5000, 2}), false)
	if r := h.do(h.f.SetAttr(g, h.read(g), &plain, &Purge{5000, 2})); !bytes.Equal(h.f.Txn.Ops[r.FailedCheck].Key, h.slotKey(1, SlotPurge, g)) {
		t.Fatal("the failed check must be the purge entry's")
	}
	end := h.attr(g)
	h.ok(h.f.Unlink(d, b("big"), g, &end, false, h.purge(g), 50*second))
	if *h.purge(g) != (Purge{0, 4}) {
		t.Fatalf("truncate, then unlink: purge %+v, want 0 .. 4", *h.purge(g))
	}
	tenK := plain
	tenK.Size, tenK.AllocEnd = 10000, 1
	k := ino(1, 72)
	h.create(d, "tenk", k, tenK)
	fiveK := tenK
	fiveK.Size = 5000
	h.ok(h.f.SetAttr(k, &tenK, &fiveK, nil))
	if *h.purge(k) != (Purge{5000, 1}) || h.attr(k).AllocEnd != 1 {
		t.Fatal("a truncate's range starts at the new size's byte offset")
	}

	// utimensat stores the time it is given, earlier or not.
	u := h.attr(k)
	back := u
	back.Mtime = 500 * second
	h.ok(h.f.SetAttr(k, &u, &back, nil))
	if h.attr(k).Mtime != 500*second {
		t.Fatal("utimensat")
	}
}

func TestLocks(t *testing.T) {
	h := newFS(t)
	f := ino(0, 2)
	h.create(RootInode, "f", f, plain)
	h.ok(h.f.Lock(f, LockExclusive, 7, nil))
	if !h.has(mustKey(&Key{Space: SpaceFS, Volume: 5, Inode: f, Tag: InodeLock, Mode: LockExclusive, Session: 7})) {
		t.Fatal("LOCK_EX")
	}
	if err := h.f.Lock(f, LockExclusive, 8, []Holder{{LockExclusive, 7, true}}); !errors.Is(err, ErrLocked) {
		t.Fatal("a live holder must refuse")
	}
	// Session 7 has left the session table: its key goes in the Txn that grants 8's.
	h.ok(h.f.Lock(f, LockExclusive, 8, []Holder{{LockExclusive, 7, false}}))
	h.ok(h.f.Unlock(f, LockExclusive, 8))

	h.ok(h.f.Lock(f, LockShared, 1, nil))
	h.ok(h.f.Lock(f, LockShared, 2, []Holder{{LockShared, 1, true}}))
	if err := h.f.Lock(f, LockExclusive, 3, []Holder{{LockShared, 1, true}, {LockShared, 2, true}}); !errors.Is(err, ErrLocked) {
		t.Fatal("LOCK_EX over two shared holders")
	}
	h.ok(h.f.Unlock(f, LockShared, 1))
	h.ok(h.f.Unlock(f, LockShared, 2))

	// Conversions: session 4's own key goes in the Txn that takes the other mode.
	h.ok(h.f.Lock(f, LockShared, 4, nil))
	h.ok(h.f.Lock(f, LockExclusive, 4, []Holder{{LockShared, 4, true}}))
	h.ok(h.f.Lock(f, LockShared, 4, []Holder{{LockExclusive, 4, true}}))
	if ks := h.keysOf(f); len(ks) != 2 || !h.has(mustKey(&Key{Space: SpaceFS, Volume: 5, Inode: f, Tag: InodeLock, Mode: LockShared, Session: 4})) {
		t.Fatalf("after LOCK_SH, LOCK_EX, LOCK_SH by one session: %q, want the attributes and l/s/4", ks)
	}
	h.ok(h.f.Unlock(f, LockShared, 4))

	// A key added in a gap after the scan fails that gap's CheckEmpty.
	h.ok(h.f.Lock(f, LockShared, 5, nil))
	h.f.Lock(f, LockExclusive, 9, []Holder{{LockShared, 5, false}})
	built := h.built()
	h.ok(h.f.Lock(f, LockShared, 6, nil))
	if r := run(t, h.s, built); r.FailedCheck == -1 {
		t.Fatal("two exclusive holders")
	}
}

func TestXattrsAndCaseIndex(t *testing.T) {
	h := newFS(t)
	f := ino(0, 2)
	h.create(RootInode, "f", f, plain)
	h.ok(h.f.SetXattr(f, h.read(f), h.up(f), b("user.DOSATTRIB"), b("0x20"), 0))
	var lo, hi FS
	lo.Volume, hi.Volume = 5, 5
	if ks, _ := scanKeys(t, h.s, lo.bound(f, InodeAttr), hi.bound(f, InodeXattr+1), 10); len(ks) != 2 {
		t.Fatalf("one scan returns %d keys, want the attributes and the xattr", len(ks))
	}
	big := bytes.Repeat([]byte("x"), MaxSplit)
	h.ok(h.f.SetXattr(f, h.read(f), h.up(f), b("user.fork"), big, 0))
	if n, _ := h.f.Txn.Check(); len(h.f.Txn.Ops) != 67 || n < 64<<10 || n > 84<<10 {
		t.Fatalf("a 64 KiB xattr: %d values in a Txn of %d bytes", len(h.f.Txn.Ops), n)
	}
	h.ok(h.f.SetXattr(f, h.read(f), h.up(f), b("user.fork"), b("small"), 65))
	if ks := h.keysOf(f); len(ks) != 3 {
		t.Fatalf("a shorter value left %d keys: its old pieces must go", len(ks))
	}
	h.ok(h.f.RemoveXattr(f, h.read(f), h.up(f), b("user.fork"), 0))
	if ks := h.keysOf(f); len(ks) != 2 {
		t.Fatal("removexattr")
	}

	h.f.CaseIndex = true
	h.create(RootInode, "Photo.JPG", ino(0, 3), plain)
	if v, _ := h.s.tree.Get(h.key(RootInode, InodeFolded, "PHOTO.JPG")); string(v) != "Photo.JPG" {
		t.Fatalf("c/ holds %q", v)
	}
	h.fails(h.f.Create(RootInode, 0, b("PHOTO.jpg"), ino(0, 4), &plain, nil, 0), true)
	h.ok(h.f.Unlink(RootInode, b("Photo.JPG"), ino(0, 3), &plain, false, nil, 0))
	if h.has(h.key(RootInode, InodeFolded, "PHOTO.JPG")) {
		t.Fatal("unlink must drop the folded name")
	}

	// A rename that changes only the case keeps the c/ key, now naming the new spelling.
	r := ino(0, 5)
	h.create(RootInode, "readme.txt", r, plain)
	h.ok(h.f.Rename(&Rename{SrcDir: RootInode, DstDir: RootInode, Src: b("readme.txt"), Dst: b("README.txt"), Child: r, ChildMode: ModeFile}, 0))
	if v, _ := h.s.tree.Get(h.key(RootInode, InodeFolded, "README.TXT")); string(v) != "README.txt" || h.has(h.key(RootInode, InodeEntry, "readme.txt")) {
		t.Fatalf("c/ holds %q after a case-only rename", v)
	}
}

func TestCaseFoldingMatchesEqualFold(t *testing.T) {
	h := newFS(t)
	h.f.CaseIndex = true
	for i, pair := range [][2]string{{"\u212Aelvin", "kelvin"}, {"\u017Fun", "SUN"}, {"\u03BF\u03C2", "\u039F\u03A3"}} {
		if !strings.EqualFold(pair[0], pair[1]) {
			t.Fatalf("%q and %q", pair[0], pair[1])
		}
		h.create(RootInode, pair[0], ino(0, uint64(3+2*i)), plain)
		h.fails(h.f.Create(RootInode, 0, b(pair[1]), ino(0, uint64(4+2*i)), &plain, nil, 0), true)
	}
	h.create(RootInode, "\u00DF", ino(0, 9), plain)
	h.create(RootInode, "ss", ino(0, 10), plain)
}

// TestCaseFoldIsPinned fails when a Go release changes a rune's fold, which would change the c/
// key of names already stored (tech-debts.md).
func TestCaseFoldIsPinned(t *testing.T) {
	h := fnv.New64a()
	for r := rune(0); r <= unicode.MaxRune; r++ {
		if f := foldRune(r); f != r {
			h.Write(binary.BigEndian.AppendUint32(binary.BigEndian.AppendUint32(nil, uint32(r)), uint32(f)))
		}
	}
	if got := h.Sum64(); got != 0x3788585c5fcc1dda {
		t.Fatalf("fold fingerprint %#x", got)
	}
}

func TestReplayedMkdir(t *testing.T) {
	h := newFS(t)
	h.f.Req = &Request{Session: 7, Mount: 1, ID: 42}
	h.mkdir(RootInode, "d", ino(0, 2))
	if rec := mustKey(&Key{Space: SpaceFS, Volume: 5, Tag: SlotReplay, Session: 7, Mount: 1, ID: 42}); !bytes.Equal(h.f.Txn.Replay, rec) || !h.has(rec) {
		t.Fatal("the record of request 42 is r/7/1/42 in the parent's slot")
	}
	h.mkdir(RootInode, "d", ino(0, 2))
	if h.attr(RootInode).Nlink != 3 {
		t.Fatal("a resent mkdir changed the state")
	}
	h.f.Req.ID = 43
	h.fails(h.f.Mkdir(RootInode, 0, b("d"), ino(0, 3), &rootDir, false, 0), true)
	h.fails(h.f.Mkdir(RootInode, 0, b("d"), ino(0, 3), &rootDir, false, 0), true)
}

func TestFSBuildsAllocateNothing(t *testing.T) {
	f := FS{Volume: 5, CaseIndex: true, Req: &Request{Session: 7, Mount: 1, ID: 9}}
	var a Allocator
	a.Adopt(2)
	pending := Purge{5000, 4}
	r := Rename{SrcDir: ino(2, 1), DstDir: ino(3, 1), Src: b("a"), Dst: b("b"), Child: ino(2, 5), ChildMode: ModeDir, Over: ino(3, 9), OverAttr: &plain, OverPending: &pending, Ancestors: []Ancestor{{ino(3, 1), RootInode}}}
	held := []Holder{{LockShared, 3, false}, {LockExclusive, 4, false}}
	small := plain
	small.Size = 5000
	big := plain
	big.Size, big.AllocEnd = 16<<20, 4
	name := b("beach.jpg")
	n := testing.AllocsPerRun(100, func() {
		a.Adopt(2)
		i, _ := a.Inode(2)
		_ = f.Create(ino(2, 1), RootInode, name, i, &plain, nil, 1)
		_ = f.Unlink(ino(2, 1), name, i, &big, false, &pending, 1)
		_ = f.Rename(&r, 1)
		_ = f.SetAttr(i, &big, &small, &pending)
		_ = f.Lock(i, LockExclusive, 5, held)
		_ = f.Unlock(i, LockExclusive, 5)
		_ = f.Mkdir(ino(2, 1), RootInode, name, ino(3, 1), &rootDir, true, 1)
		_ = f.Link(ino(2, 1), RootInode, name, i, &plain, 1)
		_ = f.Rmdir(ino(2, 1), name, ino(3, 1), 1)
		_ = f.SetDirAttr(ino(2, 1), RootInode, 1, Field{AttrMode, ModeDir})
		_ = f.SetXattr(i, &plain, 0, name, name, 2)
		_ = f.RemoveXattr(ino(2, 1), &rootDir, RootInode, name, 2)
	})
	if n != 0 {
		t.Fatalf("%v allocations per run", n)
	}
}

// FuzzDecodeFSValues feeds any bytes to the attribute, entry, parent and purge decoders: each
// refuses them or decodes a value that encodes back to them.
func FuzzDecodeFSValues(f *testing.F) {
	f.Add(AppendAttr(nil, &plain))
	f.Add(AppendEntry(nil, ino(2, 3), ModeDir))
	f.Add(AppendPurge(nil, Purge{5000, 4}))
	f.Fuzz(func(t *testing.T, v []byte) {
		var a Attr
		if DecodeAttr(v, &a) == nil && !bytes.Equal(AppendAttr(nil, &a), v) {
			t.Fatal("attributes")
		}
		if i, m, err := DecodeEntry(v); err == nil && !bytes.Equal(AppendEntry(nil, i, m), v) {
			t.Fatal("entry")
		}
		if p, err := DecodeParent(v); err == nil && !bytes.Equal(be(p), v) {
			t.Fatal("parent")
		}
		if p, err := DecodePurge(v); err == nil && (!bytes.Equal(AppendPurge(nil, p), v) || p.empty()) {
			t.Fatal("purge")
		}
	})
}

// BenchmarkBuildCreate builds a create from the daemon's reservation, as every create does.
func BenchmarkBuildCreate(b *testing.B) {
	f := FS{Volume: 5, Req: &Request{Session: 7, Mount: 1}}
	var a Allocator
	name := []byte("beach.jpg")
	b.ReportAllocs()
	for i := range b.N {
		if i%InodeBatch == 0 {
			a.Adopt(2)
		}
		in, _ := a.Inode(2)
		f.Req.ID = uint64(i)
		if err := f.Create(ino(2, 1), RootInode, name, in, &plain, nil, int64(i)); err != nil {
			b.Fatal(err)
		}
	}
}

// fsEntry is an entry a walk of the volume found.
type fsEntry struct {
	dir   uint64
	name  string
	inode uint64
	mode  uint64
}

// fsWork builds a random client operation on volume 5 from what it reads at each send, as a
// daemon rebuilds a resent request.
type fsWork struct {
	h    *fsT
	ino  uint64
	slot uint32
	now  int64
}

func (w *fsWork) scan(inode uint64, tags ...byte) []KV {
	var lo, hi FS
	lo.Volume, hi.Volume = 5, 5
	end := append([]byte{}, tags...)
	end[len(end)-1]++
	var r ScanResult
	if err := w.h.s.Scan(lo.bound(inode, tags...), hi.bound(inode, end...), 1<<20, &r); err != nil {
		w.h.t.Fatal(err)
	}
	return r.KVs
}

// walk is every directory and every entry reachable from the root.
func (w *fsWork) walk() (dirs []uint64, entries []fsEntry) {
	dirs = []uint64{RootInode}
	for i := 0; i < len(dirs); i++ {
		for _, kv := range w.scan(dirs[i], InodeEntry) {
			var k Key
			inode, mode, err := DecodeEntry(kv.Value)
			if err != nil || DecodeKey(kv.Key, &k) != nil {
				w.h.t.Fatalf("entry %x: %v", kv.Key, err)
			}
			entries = append(entries, fsEntry{dirs[i], string(k.Name), inode, mode})
			if mode == ModeDir {
				dirs = append(dirs, inode)
			}
		}
	}
	return dirs, entries
}

func (w *fsWork) attr(inode uint64) *Attr {
	v, ok := w.h.s.tree.Get(w.h.key(inode, InodeAttr, ""))
	if !ok {
		return nil
	}
	var a Attr
	if err := DecodeAttr(v, &a); err != nil {
		w.h.t.Fatalf("attributes of %x: %v", inode, err)
	}
	return &a
}

// fsOp is a client request's arguments, which a resend repeats: the directory and name, the
// entry, whose inode an unlink, rmdir or rename looks up again by its name and other operations
// keep, and a seed for the rest.
type fsOp struct {
	kind  int
	dir   uint64
	name  []byte
	e     fsEntry
	child uint64
	seed  uint64
}

// request picks a mutating operation and its arguments, and returns the builder of each of its
// sends, which reads the volume again as a daemon does for a resent request. Where the read
// answers with an errno, a request never built reports false and sends nothing, and one built
// before sends its first Txn again, whose replay key holds the result if it committed.
func (w *fsWork) request(rng *rand.Rand, req Request) func() (Txn, bool) {
	dirs, entries := w.walk()
	o := fsOp{kind: rng.IntN(12), dir: dirs[rng.IntN(len(dirs))], name: b([]string{"a", "b", "c"}[rng.IntN(3)]), seed: rng.Uint64()}
	if len(entries) > 0 {
		o.e = entries[rng.IntN(len(entries))]
	} else if o.kind != 1 && o.kind != 7 {
		o.kind = 0
	}
	o.child = ino(0, 2+w.ino)
	w.ino++
	if o.kind == 1 && rng.IntN(4) == 0 {
		w.slot++
		o.child = ino(w.slot, 1)
	}
	var first *Txn
	return func() (Txn, bool) {
		w.now += second
		w.h.f.Req = &req
		switch {
		case w.build(&o) == nil:
			if first == nil {
				t := w.h.built()
				first = &t
			}
			return w.h.f.Txn, true
		case first != nil:
			return *first, true
		}
		return Txn{}, false
	}
}

var errNoOp = errors.New("the daemon answers with an errno")

func (w *fsWork) build(o *fsOp) error {
	f, now, rng := &w.h.f, w.now, rand.New(rand.NewPCG(o.seed, 2))
	e, a := o.e, (*Attr)(nil)
	if e.inode != 0 {
		byName := o.kind == 3 || o.kind == 4 || o.kind == 5
		if v, ok := w.h.s.tree.Get(w.h.key(e.dir, InodeEntry, e.name)); ok && byName {
			e.inode, e.mode, _ = DecodeEntry(v)
		} else if byName {
			return errNoOp
		}
		a = w.attr(e.inode)
	}
	if w.attr(o.dir) == nil || a == nil && o.kind >= 2 && o.kind != 7 {
		return errNoOp
	}
	if e.mode == ModeDir && (o.kind == 2 || o.kind == 3 || o.kind == 6) || e.mode != ModeDir && o.kind == 4 {
		return errNoOp
	}
	switch o.kind {
	case 0:
		if rng.IntN(3) == 0 {
			return f.Create(o.dir, w.h.up(o.dir), o.name, o.child, &symlinkA, bytes.Repeat([]byte("t"), 1+rng.IntN(2500)), now)
		}
		return f.Create(o.dir, w.h.up(o.dir), o.name, o.child, &plain, nil, now)
	case 1:
		return f.Mkdir(o.dir, w.h.up(o.dir), o.name, o.child, &rootDir, o.child&seqMask == 1, now)
	case 2:
		return f.Link(o.dir, w.h.up(o.dir), o.name, e.inode, a, now)
	case 3:
		return f.Unlink(e.dir, b(e.name), e.inode, a, rng.IntN(4) == 0, w.h.purge(e.inode), now)
	case 4:
		return f.Rmdir(e.dir, b(e.name), e.inode, now)
	case 5:
		r := Rename{SrcDir: e.dir, DstDir: o.dir, Src: b(e.name), Dst: o.name, Child: e.inode, ChildMode: e.mode}
		if v, ok := w.h.s.tree.Get(w.h.key(o.dir, InodeEntry, string(o.name))); ok {
			over, mode, _ := DecodeEntry(v)
			if over == e.inode || (mode == ModeDir) != (e.mode == ModeDir) {
				return errNoOp
			}
			r.Over, r.OverAttr, r.OverOpen, r.OverPending = over, w.attr(over), rng.IntN(4) == 0, w.h.purge(over)
		}
		for d := o.dir; d != RootInode; {
			v, ok := w.h.s.tree.Get(w.h.key(d, InodeParent, ""))
			p, err := DecodeParent(v)
			if !ok || err != nil {
				return errNoOp
			}
			r.Ancestors = append(r.Ancestors, Ancestor{d, p})
			d = p
		}
		return f.Rename(&r, now)
	case 6:
		n := *a
		n.Size = rng.Uint64N(3 * core.ChunkSize)
		n.AllocEnd = max(n.AllocEnd, Chunks(n.Size))
		n.Mtime = Mtime(a.Mtime, now, 0)
		return f.SetAttr(e.inode, a, &n, w.h.purge(e.inode))
	case 7:
		return f.SetDirAttr(o.dir, w.h.up(o.dir), now, Field{AttrMode, ModeDir | uint64(rng.IntN(0o1000))})
	case 8:
		var held []Holder
		for _, kv := range w.scan(e.inode, InodeLock) {
			var k Key
			if err := DecodeKey(kv.Key, &k); err != nil {
				w.h.t.Fatal(err)
			}
			held = append(held, Holder{k.Mode, k.Session, true})
		}
		return f.Lock(e.inode, LockShared+byte(rng.IntN(2)), 7+rng.Uint64N(2), held)
	case 9:
		return f.Unlock(e.inode, LockShared+byte(rng.IntN(2)), 7+rng.Uint64N(2))
	}
	old := 0
	if v, ok := w.h.s.tree.Get(w.h.key(e.inode, InodeXattr, "user.x")); ok {
		var err error
		if old, _, err = DecodeHead(v); err != nil {
			w.h.t.Fatal(err)
		}
	} else if o.kind == 11 {
		return errNoOp
	}
	if o.kind == 11 {
		return f.RemoveXattr(e.inode, a, w.h.up(e.inode), b("user.x"), old)
	}
	return f.SetXattr(e.inode, a, w.h.up(e.inode), b("user.x"), bytes.Repeat([]byte("v"), rng.IntN(2500)), old)
}

func fsResendRun(t *testing.T, seed uint64) error {
	h := newFS(t)
	h.f.CaseIndex = seed%2 == 0
	w := &fsWork{h: h}
	rng := rand.New(rand.NewPCG(seed, 1))
	return resendRun(t, h.s, rng, 300, func(mount uint32, id, oldest uint64) func() (Txn, bool) {
		return w.request(rng, Request{Session: 7, Mount: mount, ID: id, Oldest: oldest})
	})
}

// TestResentFSOpsAreSent checks that the resend test sends unlink, rmdir and rename's first Txn
// again after it committed, when a new read no longer finds the name.
func TestResentFSOpsAreSent(t *testing.T) {
	resent := 0
	for seed := uint64(1); seed <= 20; seed++ {
		h := newFS(t)
		w := &fsWork{h: h}
		rng := rand.New(rand.NewPCG(seed, 1))
		err := resendRun(t, h.s, rng, 300, func(mount uint32, id, oldest uint64) func() (Txn, bool) {
			send := w.request(rng, Request{Session: 7, Mount: mount, ID: id, Oldest: oldest})
			var replay, first []byte
			return func() (Txn, bool) {
				committed := replay != nil && h.has(replay)
				tx, ok := send()
				if ok && replay == nil {
					replay, first = bytes.Clone(tx.Replay), appendTxn(nil, &tx)
				}
				drop := ok && len(tx.Ops) > 1 && tx.Ops[0].Final && !tx.Ops[0].Absent && tx.Ops[1].Kind == OpDelete
				if committed && drop && bytes.Equal(appendTxn(nil, &tx), first) {
					resent++
				}
				return tx, ok
			}
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if resent == 0 {
		t.Fatal("no unlink, rmdir or rename was resent after it committed")
	}
}

func TestResentFSOpsReturnTheirFirstFinalResult(t *testing.T) {
	seeds.Run(t, 1, 20, func(seed uint64) {
		if err := fsResendRun(t, seed); err != nil {
			t.Fatal(err)
		}
	})
}
