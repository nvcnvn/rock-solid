package sim

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/fuseproto"
)

// PageSize is the size of a page of a mount's page cache.
const PageSize = 4096

// File operations a mount's processes record, the low byte of an operation's first input word;
// the rest of that word is the file's node ID, which an open reports in its second output word.
// Inputs: open (flags), read and mapped read (offset, length), write (offset, length, the data's
// first 8 bytes as a little-endian word), fsync and close. Outputs begin with the errno; a write's
// go on with the bytes written, and a read's with the bytes returned, then with what its 8-byte
// words say when each holds id<<32 | its offset/8: the least ID among the words that are not zero,
// a word holding another offset counting as ID 0, and the offset of the first zero word, or of the
// read's end if none is.
const (
	FileOpen = iota + 1
	FileRead
	FileMapRead
	FileWrite
	FileFsync
	FileClose
)

const (
	enoent = 2
	eio    = 5
	enodev = 19
	enosys = 38
	estale = 116

	// maxPagesLimit is fuse_max_pages_limit's default.
	maxPagesLimit = 256
)

var (
	errUnique    = errors.New("sim: reply to no request the kernel is waiting on")
	errNoInode   = errors.New("sim: inval_inode of a node the kernel does not hold")
	errUnhandled = errors.New("sim: frame the kernel model does not handle")
)

// Mount is the kernel side of one FUSE mount, as fs/fuse of Linux 6.18.44 runs a mount without
// writeback_cache, for the simulator's file clients: its processes open, read, write, fsync and
// close files, recording each operation in a History, and it talks to its daemon only through
// /dev/fuse frames, Read giving the daemon the next request and Write taking a reply or
// notification. It holds the root directory's files; a process holds one of them open at a time.
// Each rule cites the function of fs/fuse (or mm/filemap.c) it follows.
type Mount struct {
	now     func() core.Tick
	hist    *History
	unique  uint64
	queue   [][]byte
	replies map[uint64]func(errno int32, body []byte) error
	blocked []func()

	ready, autoInval, bigWrites, noFlush, noFsync bool
	maxWrite, maxPages                            int64
	attrVersion                                   uint64
	dentries                                      map[string]dentry
	inodes                                        map[uint64]*inode
}

type dentry struct {
	ino    uint64
	expiry core.Tick
}

type inode struct {
	id, size     uint64
	mtime        uint64
	mtimeNsec    uint32
	expiry       core.Tick
	stale        bool
	sizeUnstable bool
	attrVersion  uint64
	pages        []*page
}

// A page that is not uptodate and not locked holds nothing a read uses, as if it were absent.
type page struct {
	data     [PageSize]byte
	uptodate bool
	locked   bool
	drop     bool
}

// NewMount mounts a FUSE filesystem whose CLOCK_BOOTTIME is now, recording its processes'
// operations in h. Its first request is INIT; a process's operation waits for INIT's reply.
func NewMount(now func() core.Tick, h *History) *Mount {
	m := &Mount{now: now, hist: h, replies: map[uint64]func(int32, []byte) error{},
		dentries: map[string]dentry{}, inodes: map[uint64]*inode{}}
	// fuse_send_init (inode.c): the flags this kernel offers that the codec names.
	m.send(&fuseproto.Request{Opcode: fuseproto.OpInit, Init: fuseproto.InitIn{Major: 7, Minor: 45, MaxReadahead: 128 << 10,
		Flags: fuseproto.FlagAsyncRead | fuseproto.FlagBigWrites | fuseproto.FlagAutoInvalData | fuseproto.FlagDoReaddirplus |
			fuseproto.FlagWritebackCache | fuseproto.FlagParallelDirops | fuseproto.FlagMaxPages | fuseproto.FlagInitExt |
			fuseproto.FlagHasResend | fuseproto.FlagRequestTimeout}}, m.initReply)
	return m
}

// initReply follows process_init_reply (inode.c).
func (m *Mount) initReply(errno int32, body []byte) error {
	o, err := fuseproto.DecodeInitOut(body)
	switch {
	case errno != 0 || err != nil || o.Major != 7:
		return fmt.Errorf("sim: INIT refused: errno %d, %+v, %v", errno, o, err)
	case o.Flags&fuseproto.FlagWritebackCache != 0:
		return fmt.Errorf("sim: INIT answered with writeback_cache, which the kernel model does not run: %w", errUnhandled)
	}
	m.ready = true
	m.autoInval = o.Flags&fuseproto.FlagAutoInvalData != 0
	m.bigWrites = o.Flags&fuseproto.FlagBigWrites != 0
	m.maxWrite = max(4096, int64(o.MaxWrite))
	m.maxPages = 32
	if o.Flags&fuseproto.FlagMaxPages != 0 {
		m.maxPages = min(maxPagesLimit, max(int64(o.MaxPages), 1))
	}
	return nil
}

// Read takes the oldest request the daemon has not read, as a read of /dev/fuse does.
func (m *Mount) Read() ([]byte, bool) {
	if len(m.queue) == 0 {
		return nil, false
	}
	f := m.queue[0]
	m.queue = m.queue[1:]
	return f, true
}

// Write hands the kernel a reply or notification, as a write to /dev/fuse does (fuse_dev_do_write,
// dev.c), and reports what such a write would fail with: a malformed frame, a reply to no request
// waiting, or a notification other than inval_inode, which the model does not handle.
func (m *Mount) Write(frame []byte) error {
	h, body, err := fuseproto.DecodeReply(frame)
	if err != nil {
		return err
	}
	if h.Unique == 0 {
		return m.notify(h.Error, body)
	}
	if h.Error > 0 || h.Error <= -512 || h.Error != 0 && len(body) != 0 {
		return fuseproto.ErrReply
	}
	then, ok := m.replies[h.Unique]
	if !ok {
		return errUnique
	}
	delete(m.replies, h.Unique)
	err = then(-h.Error, body)
	// Only a reply unlocks a page or completes INIT, which the blocked operations wait on.
	b := m.blocked
	m.blocked = nil
	for _, f := range b {
		f()
	}
	return err
}

// notify follows fuse_notify (dev.c) for inval_inode, fuse_reverse_inval_inode (inode.c): the
// attributes go stale, and the pages from off, all of them for a length of 0 or less, are dropped.
func (m *Mount) notify(code int32, body []byte) error {
	if code != fuseproto.NotifyInvalInode {
		return errUnhandled
	}
	ino, off, n, err := fuseproto.DecodeInvalInode(body)
	if err != nil {
		return err
	}
	in := m.inodes[ino]
	if in == nil {
		return errNoInode
	}
	m.attrVersion++
	in.attrVersion = m.attrVersion
	in.stale = true
	if off >= 0 {
		last := int64(-1)
		if n > 0 {
			last = (off + n - 1) / PageSize
		}
		in.invalidate(off/PageSize, last)
	}
	return nil
}

func (m *Mount) send(r *fuseproto.Request, then func(errno int32, body []byte) error) {
	m.unique += 2 // fuse_get_unique_locked (dev.c): FUSE_REQ_ID_STEP
	r.Unique = m.unique
	m.queue = append(m.queue, fuseproto.AppendRequest(nil, r))
	m.replies[r.Unique] = then
}

func (m *Mount) block(f func()) { m.blocked = append(m.blocked, f) }

// expiry follows fuse_time_to_jiffies (dir.c): 0 for a timeout of 0, which has always expired.
func (m *Mount) expiry(sec uint64, nsec uint32) core.Tick {
	if sec == 0 && nsec == 0 {
		return 0
	}
	return m.now() + core.Tick(sec)*second + core.Tick(min(nsec, 999_999_999)/1000)
}

// expired follows time_before64(expiry, jiffies); jiffies are never 0.
func (m *Mount) expired(expiry core.Tick) bool { return expiry == 0 || expiry < m.now() }

// changeAttributes follows fuse_change_attributes_i and fuse_change_attributes_common (inode.c):
// attributes from a request sent before a later local change are dropped, and a changed size, or
// with auto_inval_data a changed mtime, drops every page.
func (m *Mount) changeAttributes(in *inode, a *fuseproto.Attr, expiry core.Tick, version uint64) {
	if version != 0 && in.attrVersion > version || in.sizeUnstable {
		return
	}
	in.stale = false
	m.attrVersion++
	in.attrVersion = m.attrVersion
	in.expiry = expiry
	inval := in.size != a.Size || m.autoInval && (in.mtime != a.Mtime || in.mtimeNsec != a.MtimeNsec)
	in.size, in.mtime, in.mtimeNsec = a.Size, a.Mtime, a.MtimeNsec
	if inval {
		in.invalidate(0, -1)
	}
}

// updateSize follows fuse_write_update_attr (file.c), after a write of written bytes ending at end.
func (m *Mount) updateSize(in *inode, end uint64, written int64) {
	m.attrVersion++
	in.attrVersion = m.attrVersion
	if written > 0 && end > in.size {
		in.size = end
	}
	in.stale = true
}

// invalidate drops pages first to last, to the end if last is negative, as
// invalidate_inode_pages2_range does. That waits for a locked page, so a locked page is dropped as
// it unlocks instead, and the operation that locked it still gets its bytes.
func (in *inode) invalidate(first, last int64) {
	for i := first; i < int64(len(in.pages)) && (last < 0 || i <= last); i++ {
		if pg := in.pages[i]; pg != nil && pg.locked {
			pg.drop = true
		} else {
			in.pages[i] = nil
		}
	}
}

func (in *inode) page(i int64) *page {
	if i < int64(len(in.pages)) {
		return in.pages[i]
	}
	return nil
}

func (in *inode) setPage(i int64, pg *page) {
	for int64(len(in.pages)) <= i {
		in.pages = append(in.pages, nil)
	}
	in.pages[i] = pg
}

func (in *inode) unlock(i int64, pg *page) {
	pg.locked = false
	if pg.drop {
		in.pages[i] = nil
	}
}

// Process is a process of a mount, doing one system call at a time on the file it holds open.
type Process struct {
	m      *Mount
	client uint32
	busy   bool
	op     int
	file   *handle

	// Data is what the last read returned, Errno the last operation's error.
	Data  []byte
	Errno int32

	rbase      int64
	pos, end   int64
	wdata      []byte
	wbase      int64
	written    int64
	req        []*page
	lockedLast bool
	reqStart   int64
	reqCount   int64
}

type handle struct {
	in     *inode
	fh     uint64
	flags  uint32
	direct bool
	mapped bool
}

// Process starts a process recorded in the history as client.
func (m *Mount) Process(client uint32) *Process { return &Process{m: m, client: client} }

// Busy reports whether the process's system call has not returned.
func (p *Process) Busy() bool { return p.busy }

// Fh is the handle of the process's open file, zero when it has none.
func (p *Process) Fh() uint64 {
	if p.file == nil {
		return 0
	}
	return p.file.fh
}

// Open opens the root directory's file name with open(2) flags, which the daemon gets as they are.
func (p *Process) Open(name string, flags uint32) {
	if p.file != nil {
		panic("sim: process opens a second file")
	}
	p.start([OpWords]uint64{FileOpen, uint64(flags)})
	p.whenReady(func() { p.lookup(name, func(in *inode) { p.open(in, flags) }) })
}

// OpenInode opens the file the kernel holds as node ino without a LOOKUP, as a reopen through
// /proc/self/fd/N or open_by_handle_at does: a path that jumps through /proc is only weakly
// revalidated (complete_walk, fs/namei.c), which FUSE does not define, and fuse_get_dentry
// (inode.c) takes a held inode as it is. A node the kernel does not hold fails with ESTALE, as
// without export_support.
func (p *Process) OpenInode(ino uint64, flags uint32) {
	if p.file != nil {
		panic("sim: process opens a second file")
	}
	p.start([OpWords]uint64{FileOpen, uint64(flags)})
	p.whenReady(func() {
		if in := p.m.inodes[ino]; in != nil {
			p.open(in, flags)
		} else {
			p.finish([OpWords]uint64{estale})
		}
	})
}

// Read reads n bytes at off, through the page cache unless the handle was opened for direct I/O.
func (p *Process) Read(off int64, n int) { p.read(FileRead, off, n) }

// MapRead reads n bytes at off through a shared mapping of the file, whose pages are never
// revalidated (filemap_fault, mm/filemap.c, takes a page in the cache as it is). The handle's first
// mapped read maps the file after an fstat, as a process sizing its mapping does (fuse_getattr,
// dir.c), and the mapping lasts until close. A read is clamped to the size the kernel holds.
func (p *Process) MapRead(off int64, n int) { p.read(FileMapRead, off, n) }

// Write writes data at off.
func (p *Process) Write(off int64, data []byte) {
	var first [8]byte
	copy(first[:], data)
	p.start([OpWords]uint64{FileWrite | p.held(), uint64(off), uint64(len(data)), binary.LittleEndian.Uint64(first[:])})
	p.wdata, p.wbase, p.written = data, off, 0
	if p.file.direct {
		p.writeDirect()
		return
	}
	// fuse_perform_write (file.c): a write past the size holds off attribute changes until done.
	if in := p.file.in; in.size < uint64(off)+uint64(len(data)) {
		in.sizeUnstable = true
	}
	p.reqStart, p.reqCount = off, 0
	p.fill()
}

// Fsync sends FSYNC (fuse_fsync, file.c); a daemon answering ENOSYS gets none again.
func (p *Process) Fsync() {
	p.start([OpWords]uint64{FileFsync | p.held()})
	if p.m.noFsync {
		p.finish([OpWords]uint64{})
		return
	}
	p.m.send(&fuseproto.Request{Opcode: fuseproto.OpFsync, NodeID: p.file.in.id, PID: p.client, Fh: p.file.fh}, func(errno int32, _ []byte) error {
		if errno == enosys {
			p.m.noFsync, errno = true, 0
		}
		p.finish([OpWords]uint64{uint64(errno)})
		return nil
	})
}

// Close sends FLUSH and waits for it (fuse_flush, file.c), then RELEASE, which it does not wait for
// (fuse_release, file.c); a daemon answering FLUSH with ENOSYS gets none again.
func (p *Process) Close() {
	p.start([OpWords]uint64{FileClose | p.held()})
	h := p.file
	p.file = nil
	release := func(errno int32) {
		p.m.send(&fuseproto.Request{Opcode: fuseproto.OpRelease, NodeID: h.in.id, PID: p.client, Fh: h.fh, Flags: h.flags},
			func(int32, []byte) error { return nil })
		p.finish([OpWords]uint64{uint64(errno)})
	}
	if p.m.noFlush {
		release(0)
		return
	}
	p.m.send(&fuseproto.Request{Opcode: fuseproto.OpFlush, NodeID: h.in.id, PID: p.client, Fh: h.fh}, func(errno int32, _ []byte) error {
		if errno == enosys {
			p.m.noFlush, errno = true, 0
		}
		release(errno)
		return nil
	})
}

func (p *Process) held() uint64 { return p.file.in.id << 8 }

func (p *Process) start(in [OpWords]uint64) {
	if p.busy {
		panic("sim: process starts a system call before its last returned")
	}
	p.busy, p.Data = true, p.Data[:0]
	p.op = p.m.hist.Invoke(p.client, in)
}

func (p *Process) finish(out [OpWords]uint64) {
	p.busy, p.Errno = false, int32(out[0])
	p.m.hist.Complete(p.op, out)
}

func (p *Process) whenReady(f func()) {
	if p.m.ready {
		f()
		return
	}
	p.m.block(func() { p.whenReady(f) })
}

// lookup follows fuse_dentry_revalidate, fuse_lookup and fuse_lookup_name (dir.c): a name's entry
// is used until its timeout, then looked up again, and the reply's attributes are taken as from
// GETATTR. A reply of node ID 0 is a negative entry, kept for its timeout; an ENOENT error is not.
func (p *Process) lookup(name string, then func(*inode)) {
	m := p.m
	if d, ok := m.dentries[name]; ok && !m.expired(d.expiry) {
		if d.ino == 0 {
			p.finish([OpWords]uint64{enoent})
			return
		}
		then(m.inodes[d.ino])
		return
	}
	version := m.attrVersion
	m.send(&fuseproto.Request{Opcode: fuseproto.OpLookup, NodeID: 1, PID: p.client, Name: []byte(name)}, func(errno int32, body []byte) error {
		e, err := fuseproto.DecodeEntry(body)
		switch {
		case errno != 0:
			p.finish([OpWords]uint64{uint64(errno)})
			return nil
		case err != nil:
			p.finish([OpWords]uint64{eio})
			return err
		case e.NodeID == 0:
			m.dentries[name] = dentry{expiry: m.expiry(e.EntryValid, e.EntryValidNsec)}
			p.finish([OpWords]uint64{enoent})
			return nil
		}
		in := m.inodes[e.NodeID]
		if in == nil {
			in = &inode{id: e.NodeID} // fuse_iget (inode.c)
			m.inodes[e.NodeID] = in
		}
		m.changeAttributes(in, &e.Attr, m.expiry(e.AttrValid, e.AttrValidNsec), version)
		m.dentries[name] = dentry{ino: e.NodeID, expiry: m.expiry(e.EntryValid, e.EntryValidNsec)}
		then(in)
		return nil
	})
}

// open follows fuse_open (file.c): the reply's FOPEN_DIRECT_IO is the handle's for its life, and
// an open without FOPEN_KEEP_CACHE drops every page of the file.
func (p *Process) open(in *inode, flags uint32) {
	p.m.send(&fuseproto.Request{Opcode: fuseproto.OpOpen, NodeID: in.id, PID: p.client, Flags: flags}, func(errno int32, body []byte) error {
		o, err := fuseproto.DecodeOpen(body)
		switch {
		case errno != 0:
			p.finish([OpWords]uint64{uint64(errno)})
			return nil
		case err != nil:
			p.finish([OpWords]uint64{eio})
			return err
		}
		p.file = &handle{in: in, fh: o.Fh, flags: flags, direct: o.Flags&fuseproto.OpenDirectIO != 0}
		if o.Flags&fuseproto.OpenKeepCache == 0 {
			in.invalidate(0, -1)
		}
		p.finish([OpWords]uint64{0, in.id, uint64(o.Flags)})
		return nil
	})
}

// read follows fuse_file_read_iter (file.c): a direct handle reads from the daemon, a cached one
// through the page cache, first revalidating the attributes as fuse_cache_read_iter does, with
// auto_inval_data on every read and otherwise on a read past the size.
func (p *Process) read(kind uint64, off int64, n int) {
	p.start([OpWords]uint64{kind | p.held(), uint64(off), uint64(n)})
	p.rbase, p.pos, p.end = off, off, off+int64(n)
	in := p.file.in
	switch {
	case p.file.direct && kind == FileMapRead:
		// fuse_file_mmap (file.c): no shared mapping of a direct handle without FUSE_DIRECT_IO_ALLOW_MMAP.
		p.finishRead(enodev)
	case p.file.direct:
		p.readDirect()
	case kind == FileMapRead && !p.file.mapped:
		p.file.mapped = true
		p.updateAttributes(p.readCached)
	case kind == FileRead && (p.m.autoInval || uint64(p.end) > in.size):
		p.updateAttributes(p.readCached)
	default:
		p.readCached()
	}
}

// updateAttributes follows fuse_update_get_attr (dir.c): GETATTR when the attributes have expired
// or a local change marked them stale.
func (p *Process) updateAttributes(then func()) {
	m, in := p.m, p.file.in
	if !in.stale && !m.expired(in.expiry) {
		then()
		return
	}
	version := m.attrVersion
	m.send(&fuseproto.Request{Opcode: fuseproto.OpGetattr, NodeID: in.id, PID: p.client, OpFlags: fuseproto.GetattrFh, Fh: p.file.fh},
		func(errno int32, body []byte) error {
			valid, nsec, a, err := fuseproto.DecodeAttrOut(body)
			switch {
			case errno != 0:
				p.finishRead(errno)
				return nil
			case err != nil:
				p.finishRead(eio)
				return err
			}
			m.changeAttributes(in, &a, m.expiry(valid, nsec), version)
			then()
			return nil
		})
}

// readCached follows filemap_read (mm/filemap.c): page by page, never past the size, which it
// reads again once each page is uptodate; a locked page is waited for, and a page not uptodate
// read from the daemon. Readahead is not modelled: each page is its own READ.
func (p *Process) readCached() {
	in := p.file.in
	for p.pos < p.end && uint64(p.pos) < in.size {
		i := p.pos / PageSize
		pg := in.page(i)
		switch {
		case pg != nil && pg.locked:
			p.m.block(p.readCached)
			return
		case pg == nil || !pg.uptodate:
			p.readPage(i)
			return
		}
		p.copyPage(&pg.data)
	}
	p.finishRead(0)
}

func (p *Process) copyPage(data *[PageSize]byte) {
	i := p.pos / PageSize
	stop := min(p.end, int64(p.file.in.size), (i+1)*PageSize)
	if p.pos >= stop {
		p.end = p.pos
		return
	}
	p.Data = append(p.Data, data[p.pos-i*PageSize:stop-i*PageSize]...)
	p.pos = stop
}

// readPage follows fuse_read_folio and fuse_do_readfolio (file.c): the page is locked while its READ
// is out, a short reply leaves zeros after it and, unless the attributes changed since the READ was
// sent, cuts the size there (fuse_short_read, fuse_read_update_size).
func (p *Process) readPage(i int64) {
	m, h := p.m, p.file
	in := h.in
	pg := &page{locked: true}
	in.setPage(i, pg)
	version := m.attrVersion
	m.send(&fuseproto.Request{Opcode: fuseproto.OpRead, NodeID: in.id, PID: p.client, Fh: h.fh, Offset: uint64(i * PageSize), Size: PageSize, Flags: h.flags},
		func(errno int32, body []byte) error {
			var err error
			if errno == 0 && len(body) > PageSize {
				errno, err = eio, fuseproto.ErrReply
			}
			if errno == 0 {
				copy(pg.data[:], body)
				pg.uptodate = true
				if end := uint64(i*PageSize) + uint64(len(body)); len(body) < PageSize &&
					version >= in.attrVersion && end < in.size && !in.sizeUnstable {
					m.attrVersion++
					in.attrVersion = m.attrVersion
					in.size = end
				}
			}
			in.unlock(i, pg)
			if errno != 0 {
				p.finishRead(errno)
				return err
			}
			p.copyPage(&pg.data)
			p.readCached()
			return err
		})
}

// readDirect follows fuse_direct_io (file.c) for a read: READs of up to max_pages pages, ending at
// a short one, past the page cache and the size.
func (p *Process) readDirect() {
	if p.pos >= p.end {
		p.finishRead(0)
		return
	}
	n := min(p.end-p.pos, p.m.maxPages*PageSize)
	h := p.file
	p.m.send(&fuseproto.Request{Opcode: fuseproto.OpRead, NodeID: h.in.id, PID: p.client, Fh: h.fh, Offset: uint64(p.pos), Size: uint32(n), Flags: h.flags},
		func(errno int32, body []byte) error {
			if errno == 0 && int64(len(body)) > n {
				p.finishRead(eio)
				return fuseproto.ErrReply
			}
			if errno != 0 {
				p.finishRead(errno)
				return nil
			}
			p.Data = append(p.Data, body...)
			p.pos += int64(len(body))
			if int64(len(body)) < n {
				p.end = p.pos
			}
			p.readDirect()
			return nil
		})
}

// finishRead returns what was read, or errno if nothing was.
func (p *Process) finishRead(errno int32) {
	if len(p.Data) > 0 {
		errno = 0
	}
	least, zero := ^uint64(0), uint64(p.rbase)+uint64(len(p.Data))
	for i := 0; i < len(p.Data); i += 8 {
		var w [8]byte
		copy(w[:], p.Data[i:])
		at := uint64(p.rbase) + uint64(i)
		switch v := binary.LittleEndian.Uint64(w[:]); {
		case v == 0:
			zero = min(zero, at)
		case uint32(v) != uint32(at/8):
			least = 0
		default:
			least = min(least, v>>32)
		}
	}
	p.finish([OpWords]uint64{uint64(errno), uint64(len(p.Data)), least, zero})
}

// fill follows fuse_fill_write_pages (file.c): one WRITE's pages, at most max_write bytes, each
// copied into the page cache before it is sent. A whole page becomes uptodate; a part of a page
// not uptodate ends the WRITE there, the page locked until the reply. Without big_writes a WRITE
// holds one page. A locked page is waited for.
func (p *Process) fill() {
	m, in := p.m, p.file.in
	num := min(int64(len(p.wdata))-(p.reqStart-p.wbase), m.maxWrite) - p.reqCount
	for num > 0 && int64(len(p.req)) < m.maxPages {
		pos := p.reqStart + p.reqCount
		i, off := pos/PageSize, pos%PageSize
		n := min(PageSize-off, num)
		pg := in.page(i)
		if pg != nil && pg.locked {
			m.block(p.fill)
			return
		}
		if pg == nil {
			pg = &page{}
			in.setPage(i, pg)
		}
		copy(pg.data[off:off+n], p.wdata[pos-p.wbase:])
		if n == PageSize {
			pg.uptodate = true
		}
		p.req = append(p.req, pg)
		p.reqCount += n
		num -= n
		if !pg.uptodate {
			pg.locked, p.lockedLast = true, true
			break
		}
		if !m.bigWrites || (off+n)%PageSize != 0 {
			break
		}
	}
	p.sendWrite()
}

// sendWrite follows fuse_send_write_pages (file.c): a failed WRITE leaves its pages not uptodate,
// a short one the page it stopped in.
func (p *Process) sendWrite() {
	m, h := p.m, p.file
	in := h.in
	start, count := p.reqStart, p.reqCount
	m.send(&fuseproto.Request{Opcode: fuseproto.OpWrite, NodeID: in.id, PID: p.client, Fh: h.fh, Offset: uint64(start),
		Size: uint32(count), Flags: h.flags, Data: p.wdata[start-p.wbase : start-p.wbase+count]}, func(errno int32, body []byte) error {
		size, err := fuseproto.DecodeWrite(body)
		switch {
		case errno != 0:
			err = nil
		case err != nil || int64(size) > count:
			errno = eio
		}
		left, off := int64(size), start%PageSize
		for k, pg := range p.req {
			if errno != 0 {
				pg.uptodate = false
			} else if left >= PageSize-off {
				left -= PageSize - off
			} else {
				if int64(size) < count {
					pg.uptodate = false
				}
				left = 0
			}
			off = 0
			if p.lockedLast && k == len(p.req)-1 {
				in.unlock((start+count-1)/PageSize, pg)
			}
		}
		p.req, p.lockedLast = p.req[:0], false
		if errno != 0 {
			p.finishWrite(errno)
			return err
		}
		p.written += int64(size)
		switch {
		case int64(size) != count:
			p.finishWrite(eio)
		case p.written < int64(len(p.wdata)):
			p.reqStart, p.reqCount = start+count, 0
			p.fill()
		default:
			p.finishWrite(0)
		}
		return nil
	})
}

// writeDirect follows fuse_direct_write_iter and fuse_direct_io (file.c of 6.18.44): the pages
// written over are dropped before the WRITEs (file.c:1654), which go past the page cache, up to
// max_write bytes and max_pages pages each, ending at a short one, and again after them if any
// byte was written (file.c:1718), dropping what a cached read filled meanwhile. Torvalds' v6.18
// drops them only before.
func (p *Process) writeDirect() {
	in, h := p.file.in, p.file
	first, last := p.wbase/PageSize, (p.wbase+int64(len(p.wdata))-1)/PageSize
	in.invalidate(first, last)
	finish := func(errno int32) {
		if p.written > 0 {
			in.invalidate(first, last)
		}
		p.finishWrite(errno)
	}
	var next func()
	next = func() {
		left := int64(len(p.wdata)) - p.written
		if left == 0 {
			finish(0)
			return
		}
		n := min(left, p.m.maxWrite, p.m.maxPages*PageSize)
		pos := p.wbase + p.written
		p.m.send(&fuseproto.Request{Opcode: fuseproto.OpWrite, NodeID: in.id, PID: p.client, Fh: h.fh, Offset: uint64(pos),
			Size: uint32(n), Flags: h.flags, Data: p.wdata[p.written : p.written+n]}, func(errno int32, body []byte) error {
			size, err := fuseproto.DecodeWrite(body)
			switch {
			case errno != 0:
				err = nil
			case err != nil || int64(size) > n:
				errno = eio
			}
			if errno != 0 {
				finish(errno)
				return err
			}
			p.written += int64(size)
			if int64(size) < n {
				finish(0)
				return nil
			}
			next()
			return nil
		})
	}
	next()
}

// finishWrite returns the bytes written, or errno if none were, after fuse_write_update_attr.
func (p *Process) finishWrite(errno int32) {
	in := p.file.in
	p.m.updateSize(in, uint64(p.wbase+p.written), p.written)
	in.sizeUnstable = false
	if p.written > 0 {
		errno = 0
	}
	p.finish([OpWords]uint64{uint64(errno), uint64(p.written)})
}
