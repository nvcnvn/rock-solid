//go:build linux

package uring

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"unsafe"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/loop"
	"golang.org/x/sys/unix"
)

const (
	offSQRing = 0
	offSQEs   = 0x10000000

	featSingleMmap = 1 << 0
	featNoDrop     = 1 << 1

	regBuffers     = 0
	regFiles       = 2
	regEventfd     = 4
	regFilesUpdate = 6

	// maxDisks is the registered file table's size, the most disks one ring opens as fixed files.
	maxDisks = 16
)

type sqOffsets struct {
	head, tail, ringMask, ringEntries, flags, dropped, array, resv1 uint32
	userAddr                                                        uint64
}

type cqOffsets struct {
	head, tail, ringMask, ringEntries, overflow, cqes, flags, resv1 uint32
	userAddr                                                        uint64
}

type params struct {
	sqEntries, cqEntries, flags, sqThreadCPU, sqThreadIdle, features, wqFD uint32
	resv                                                                   [3]uint32
	sqOff                                                                  sqOffsets
	cqOff                                                                  cqOffsets
}

// cqe is an io_uring completion queue entry.
type cqe struct {
	userData uint64
	res      int32
	_        uint32
}

type filesUpdate struct {
	offset uint32
	resv   uint32
	fds    uint64
}

// Status is what a ring registered with the kernel, and why not.
type Status struct {
	// FixedBuffers is set when the pool is registered and reads and writes of one buffer use
	// READ_FIXED and WRITE_FIXED; otherwise BuffersErr says why, ENOMEM under the memory-lock limit.
	FixedBuffers bool
	BuffersErr   error
	// FixedFiles is set when the file table is registered and disks are opened into it.
	FixedFiles bool
	FilesErr   error
	// Failed is the io_uring_enter error that failed the ring: every operation in flight then and
	// every one executed since completes with DiskIO.
	Failed error
}

// Ring is one loop's io_uring: its disks' reads, writes, flushes and trims go through it, and it
// never waits in io_uring_enter, its completions arriving on an eventfd the loop watches.
type Ring struct {
	fd, efd     int
	mem, sqeMem []byte
	zero        []byte
	sqHead      *uint32
	sqTail      *uint32
	cqHead      *uint32
	cqTail      *uint32
	sqMask      uint32
	cqMask      uint32
	sqEntries   uint32
	cqEntries   uint32
	sqes        []sqe
	cqes        []cqe
	pool        *loop.Pool
	regions     regions
	files       int
	disks       []*Disk
	ops         []op
	free        int32
	queue       []int32
	qhead       int
	inflight    uint32
	done        []core.Event
	last        int32
	// after is the op whose completion the head of the queue, the rest of a link chain the kernel
	// cut, waits for, or -1.
	after  int32
	status Status
}

// New sets up a ring of entries submission entries, a power of two, for the loop that owns pool,
// and registers pool's regions as fixed buffers, falling back to plain reads and writes when the
// kernel refuses.
func New(pool *loop.Pool, entries uint32) (*Ring, error) {
	if entries < 2 {
		return nil, fmt.Errorf("uring: %d entries cannot hold a write and its linked flush", entries)
	}
	var p params
	fd, _, e := unix.Syscall(unix.SYS_IO_URING_SETUP, uintptr(entries), uintptr(unsafe.Pointer(&p)), 0)
	if e != 0 {
		return nil, fmt.Errorf("uring: io_uring_setup: %w", e)
	}
	r := &Ring{fd: int(fd), efd: -1, pool: pool, free: -1, last: -1, after: -1, regions: newRegions(pool.Regions())}
	if err := r.setup(&p); err != nil {
		r.Close()
		return nil, err
	}
	r.register(pool.Regions())
	return r, nil
}

func (r *Ring) setup(p *params) error {
	if p.features&featSingleMmap == 0 || p.features&featNoDrop == 0 {
		return fmt.Errorf("uring: kernel lacks IORING_FEAT_SINGLE_MMAP or IORING_FEAT_NODROP (features %#x)", p.features)
	}
	size := max(int(p.sqOff.array)+int(p.sqEntries)*4, int(p.cqOff.cqes)+int(p.cqEntries)*int(unsafe.Sizeof(cqe{})))
	var err error
	if r.mem, err = unix.Mmap(r.fd, offSQRing, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE); err != nil {
		return fmt.Errorf("uring: mapping the rings: %w", err)
	}
	if r.sqeMem, err = unix.Mmap(r.fd, offSQEs, int(p.sqEntries)*int(unsafe.Sizeof(sqe{})), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE); err != nil {
		return fmt.Errorf("uring: mapping the submission entries: %w", err)
	}
	at := func(off uint32) *uint32 { return (*uint32)(unsafe.Pointer(&r.mem[off])) }
	r.sqHead, r.sqTail, r.cqHead, r.cqTail = at(p.sqOff.head), at(p.sqOff.tail), at(p.cqOff.head), at(p.cqOff.tail)
	r.sqMask, r.cqMask = *at(p.sqOff.ringMask), *at(p.cqOff.ringMask)
	r.sqEntries, r.cqEntries = p.sqEntries, p.cqEntries
	r.sqes = unsafe.Slice((*sqe)(unsafe.Pointer(&r.sqeMem[0])), p.sqEntries)
	r.cqes = unsafe.Slice((*cqe)(unsafe.Pointer(&r.mem[p.cqOff.cqes])), p.cqEntries)
	if r.zero, err = unix.Mmap(-1, 0, core.BlockSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANON|unix.MAP_POPULATE); err != nil {
		return fmt.Errorf("uring: mapping a block of zeros: %w", err)
	}
	r.regions.zero = uintptr(unsafe.Pointer(&r.zero[0]))
	array := unsafe.Slice((*uint32)(unsafe.Pointer(&r.mem[p.sqOff.array])), p.sqEntries)
	for i := range array {
		array[i] = uint32(i)
	}
	if r.efd, err = unix.Eventfd(0, unix.EFD_NONBLOCK|unix.EFD_CLOEXEC); err != nil {
		return fmt.Errorf("uring: eventfd: %w", err)
	}
	efd := int32(r.efd)
	if err := r.reg(regEventfd, unsafe.Pointer(&efd), 1); err != nil {
		return fmt.Errorf("uring: registering the eventfd: %w", err)
	}
	return nil
}

func (r *Ring) register(regions [][]byte) {
	iov := make([]unix.Iovec, len(regions))
	for i, m := range regions {
		iov[i].Base = &m[0]
		iov[i].SetLen(len(m))
	}
	if err := r.reg(regBuffers, unsafe.Pointer(&iov[0]), len(iov)); err != nil {
		r.status.BuffersErr = fmt.Errorf("uring: registering %d pool regions: %w", len(iov), err)
	} else {
		r.regions.fixed, r.status.FixedBuffers = true, true
	}
	var fds [maxDisks]int32
	for i := range fds {
		fds[i] = -1
	}
	if err := r.reg(regFiles, unsafe.Pointer(&fds[0]), maxDisks); err != nil {
		r.status.FilesErr = fmt.Errorf("uring: registering the file table: %w", err)
	} else {
		r.status.FixedFiles = true
	}
}

func (r *Ring) reg(opcode int, arg unsafe.Pointer, n int) error {
	if _, _, e := unix.Syscall6(unix.SYS_IO_URING_REGISTER, uintptr(r.fd), uintptr(opcode), uintptr(arg), uintptr(n), 0, 0); e != 0 {
		return e
	}
	return nil
}

// Status is what the ring registered.
func (r *Ring) Status() Status { return r.status }

// Fd is the eventfd the kernel signals completions on.
func (r *Ring) Fd() int { return r.efd }

// Close unmaps the ring and closes it, its eventfd and its disks. I/O still in flight is abandoned,
// its buffers quarantined, as the kernel tears the ring down without waiting for it.
func (r *Ring) Close() {
	r.quarantine()
	r.ops = nil
	for _, d := range r.disks {
		_ = unix.Close(d.raw)
	}
	r.disks = nil
	for _, m := range [][]byte{r.sqeMem, r.mem, r.zero} {
		if m != nil {
			_ = unix.Munmap(m)
		}
	}
	r.sqeMem, r.mem, r.zero = nil, nil, nil
	if r.efd >= 0 {
		_ = unix.Close(r.efd)
		r.efd = -1
	}
	if r.fd >= 0 {
		_ = unix.Close(r.fd)
		r.fd = -1
	}
}

// Disk is one disk on a ring, a regular file or a raw block device opened O_DIRECT.
type Disk struct {
	r   *Ring
	raw int
	t   target
}

// Open opens the file or block device at path for the ring; fua makes it a durability: fua disk,
// whose writes carry FUA and whose flushes submit nothing.
func (r *Ring) Open(path string, fua bool) (*Disk, error) {
	var st unix.Stat_t
	if err := unix.Stat(path, &st); err != nil {
		return nil, fmt.Errorf("uring: %w", err)
	}
	flags := unix.O_RDWR | unix.O_DIRECT | unix.O_CLOEXEC
	if st.Mode&unix.S_IFMT == unix.S_IFBLK {
		flags |= unix.O_EXCL
	}
	return r.open(path, flags, fua)
}

func (r *Ring) open(path string, flags int, fua bool) (*Disk, error) {
	fd, err := unix.Open(path, flags, 0)
	if err != nil {
		return nil, fmt.Errorf("uring: opening %s: %w", path, err)
	}
	d := &Disk{r: r, raw: fd, t: target{fd: int32(fd), fua: fua}}
	size, err := d.size()
	if err != nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("uring: sizing %s: %w", path, err)
	}
	if size%core.BlockSize != 0 {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("uring: opening %s: %w: %d bytes", path, ErrNotBlocks, size)
	}
	d.t.blocks = size / core.BlockSize
	if r.status.FixedFiles && r.files < maxDisks {
		f := int32(fd)
		up := filesUpdate{offset: uint32(r.files), fds: uint64(uintptr(unsafe.Pointer(&f)))}
		if err := r.reg(regFilesUpdate, unsafe.Pointer(&up), 1); err == nil {
			d.t.fd, d.t.fixed = int32(r.files), true
			r.files++
		}
	}
	r.disks = append(r.disks, d)
	return d, nil
}

func (d *Disk) size() (uint64, error) {
	var st unix.Stat_t
	if err := unix.Fstat(d.raw, &st); err != nil {
		return 0, err
	}
	if st.Mode&unix.S_IFMT != unix.S_IFBLK {
		d.t.file = true
		return uint64(st.Size), nil
	}
	var n uint64
	if _, _, e := unix.Syscall(unix.SYS_IOCTL, uintptr(d.raw), unix.BLKGETSIZE64, uintptr(unsafe.Pointer(&n))); e != 0 {
		return 0, e
	}
	return n, nil
}

// Blocks is the disk's size in blocks.
func (d *Disk) Blocks() uint64 { return d.t.blocks }

// Fixed reports whether the disk's descriptor is registered as a fixed file.
func (d *Disk) Fixed() bool { return d.t.fixed }

// Execute queues a read, write, flush or trim for the ring's next Submit. An action refused and a
// flush under fua complete at the ring's next Reap without a submission, and so does every action
// on a failed ring, with DiskIO; a flush linked behind a refused write completes with DiskIO, as
// io_uring cancels a link. A trim of a device is a discard with the write of its segment header
// hard-linked behind it, which runs whatever the discard's result and completes the trim.
func (d *Disk) Execute(a *core.Action) {
	r := d.r
	last := r.last
	r.last = -1
	if r.status.Failed != nil {
		r.done = append(r.done, core.Event{Kind: core.EvDiskDone, IO: a.IO, Err: core.DiskIO})
		return
	}
	if a.Kind == core.ActDiskFsync && a.Linked {
		switch {
		case last < 0 || r.ops[last].sent:
			r.done = append(r.done, core.Event{Kind: core.EvDiskDone, IO: a.IO, Err: core.DiskIO})
			return
		case !r.ops[last].on(&d.t):
		case d.t.fua:
			r.ops[last].then, r.ops[last].hasThen = a.IO, true
			return
		default:
			r.ops[last].sqe.flags |= sqeIOLink
		}
	}
	i := r.alloc()
	o := &r.ops[i]
	ok, err := prepare(o, uint64(i), a, &d.t, &r.regions, r.pool)
	if !ok {
		r.release(i)
		r.done = append(r.done, core.Event{Kind: core.EvDiskDone, IO: a.IO, Err: err})
		return
	}
	r.queue = append(r.queue, i)
	switch {
	case a.Kind == core.ActDiskWrite:
		r.last = i
	case a.Kind == core.ActDiskTrim && !d.t.file:
		j := r.alloc()
		header(&r.ops[j], uint64(j), a, &d.t, &r.regions)
		r.queue = append(r.queue, j)
	}
}

func (r *Ring) alloc() int32 {
	if i := r.free; i >= 0 {
		r.free = r.ops[i].next
		return i
	}
	r.ops = append(r.ops, op{})
	return int32(len(r.ops) - 1)
}

func (r *Ring) release(i int32) {
	r.ops[i].live = false
	r.ops[i].next = r.free
	r.free = i
}

// chain is the entries of the link chain that starts at queue position q.
func (r *Ring) chain(q int) uint32 {
	n := uint32(1)
	for r.ops[r.queue[q]].sqe.flags&(sqeIOLink|sqeIOHardlink) != 0 {
		q++
		n++
	}
	return n
}

// Submit copies whole link chains from the queue into the submission queue while it and the
// completion queue have room for them, and submits them in one io_uring_enter that waits for
// nothing. In flight never exceeds the completion queue, so it never overflows.
func (r *Ring) Submit() loop.Enter {
	return r.enter(r.fill())
}

// fill copies whole link chains into the submission queue and returns the entries copied.
func (r *Ring) fill() uint32 {
	tail := *r.sqTail
	copied := uint32(0)
	for r.after < 0 && r.qhead < len(r.queue) {
		n := r.chain(r.qhead)
		if tail-atomic.LoadUint32(r.sqHead)+n > r.sqEntries || r.inflight+n > r.cqEntries {
			break
		}
		for range n {
			o := &r.ops[r.queue[r.qhead]]
			o.sent = true
			r.sqes[tail&r.sqMask] = o.sqe
			tail++
			r.qhead++
		}
		r.inflight += n
		copied += n
	}
	r.queue = append(r.queue[:0], r.queue[r.qhead:]...)
	r.qhead = 0
	atomic.StoreUint32(r.sqTail, tail)
	return copied
}

// enter submits n entries of the submission queue. An error other than EINTR, EAGAIN or EBUSY
// fails the ring; otherwise the entries the kernel did not take go back to the head of the queue.
func (r *Ring) enter(n uint32) loop.Enter {
	if n == 0 {
		return loop.Enter{}
	}
	e := loop.Enter{Submit: n}
	took, _, errno := unix.Syscall6(unix.SYS_IO_URING_ENTER, uintptr(r.fd), uintptr(e.Submit), uintptr(e.Wait), 0, 0, 0)
	if e.Errno = errno; errno == 0 {
		e.Took = uint32(took)
	}
	switch errno {
	case 0, unix.EINTR, unix.EAGAIN, unix.EBUSY:
		r.requeue(e.Took)
	default:
		r.fail(fmt.Errorf("uring: io_uring_enter submitting %d: %w", n, errno))
	}
	return e
}

// requeue moves the entries the kernel did not take back from the submission queue to the head of
// the queue; the kernel reads the submission queue only inside io_uring_enter, so lowering its
// tail withdraws them. The kernel stops early when it cannot allocate a request, even inside a
// link chain, and then runs the chain's part it took alone (io_submit_sqes and
// io_submit_state_end, Linux 6.12). So when the last entry taken links to the next, the rest of
// its chain waits at the head of the queue until it completes.
func (r *Ring) requeue(took uint32) {
	head, tail := atomic.LoadUint32(r.sqHead), *r.sqTail
	if head == tail {
		return
	}
	n := int(tail - head)
	r.queue = append(r.queue, make([]int32, n)...)
	copy(r.queue[n:], r.queue)
	for k := range n {
		i := int32(r.sqes[(head+uint32(k))&r.sqMask].userData)
		r.queue[k] = i
		r.ops[i].sent = false
	}
	r.inflight -= uint32(n)
	atomic.StoreUint32(r.sqTail, head)
	if s := &r.sqes[(head-1)&r.sqMask]; took > 0 && s.flags&(sqeIOLink|sqeIOHardlink) != 0 {
		r.after = int32(s.userData)
	}
}

// quarantine keeps the pool from handing out again the buffers of every op the kernel may hold.
func (r *Ring) quarantine() {
	for i := range r.ops {
		if o := &r.ops[i]; o.live {
			for _, b := range o.held() {
				r.pool.Quarantine(b)
			}
		}
	}
}

// fail marks the ring failed by err and completes every operation queued or in flight with
// DiskIO; the completion queue is never read again, as its entries name ops since reused. The
// kernel may still read or write the buffers of an operation copied into the submission queue, so
// they are quarantined: never handed out again while the pool lives.
func (r *Ring) fail(err error) {
	r.status.Failed = err
	r.quarantine()
	for i := range r.ops {
		if o := &r.ops[i]; o.live {
			r.done = complete(r.done, o, -int32(unix.EIO))
			r.release(int32(i))
		}
	}
	r.queue, r.qhead, r.inflight, r.after = r.queue[:0], 0, 0, -1
}

// Reap appends the completions refused or finished at once since the last Reap, then every
// completion in the completion queue. When the op the head of the queue waits for completes, the
// head may go to the kernel, unless that op failed a soft link, which the kernel would have
// cancelled the head for: it then completes with DiskIO.
func (r *Ring) Reap(evs []core.Event) []core.Event {
	evs = append(evs, r.done...)
	r.done = r.done[:0]
	if r.status.Failed != nil {
		return evs
	}
	head, tail := *r.cqHead, atomic.LoadUint32(r.cqTail)
	for ; head != tail; head++ {
		c := r.cqes[head&r.cqMask]
		i := int32(c.userData)
		o := &r.ops[i]
		evs = complete(evs, o, c.res)
		if i == r.after {
			r.after = -1
			if o.sqe.flags&sqeIOLink != 0 && evs[len(evs)-1].Err != core.DiskOK {
				j := r.queue[0]
				evs = complete(evs, &r.ops[j], -int32(unix.ECANCELED))
				r.release(j)
				r.queue = append(r.queue[:0], r.queue[1:]...)
			}
		}
		r.release(i)
		r.inflight--
	}
	atomic.StoreUint32(r.cqHead, head)
	return evs
}

// Waiting reports a completion to reap, or a queued chain with room for it in flight that waits
// for no completion.
func (r *Ring) Waiting() bool {
	if r.status.Failed != nil {
		return len(r.done) > 0
	}
	return len(r.done) > 0 || *r.cqHead != atomic.LoadUint32(r.cqTail) ||
		r.after < 0 && r.qhead < len(r.queue) && r.inflight+r.chain(r.qhead) <= r.cqEntries
}

// InFlight is the entries submitted and not yet reaped.
func (r *Ring) InFlight() int { return int(r.inflight) }

// ErrNotBlocks reports a disk file whose size is not whole blocks.
var ErrNotBlocks = errors.New("uring: size is not whole blocks")

// CreateFile makes a disk file of size bytes, every byte written, and makes it and its directory
// entry durable, so later writes into it are overwrites that commit no file system journal.
func CreateFile(path string, size int64) error {
	if size <= 0 || size%core.BlockSize != 0 {
		return fmt.Errorf("%w: %d bytes", ErrNotBlocks, size)
	}
	fd, err := unix.Open(path, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_DIRECT|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return fmt.Errorf("uring: creating %s: %w", path, err)
	}
	defer func() { _ = unix.Close(fd) }()
	zeros, err := unix.Mmap(-1, 0, 1<<20, unix.PROT_READ, unix.MAP_PRIVATE|unix.MAP_ANON)
	if err != nil {
		return fmt.Errorf("uring: mapping zeros: %w", err)
	}
	defer func() { _ = unix.Munmap(zeros) }()
	for off := int64(0); off < size; {
		n, err := unix.Pwrite(fd, zeros[:min(int64(len(zeros)), size-off)], off)
		if err != nil {
			return fmt.Errorf("uring: writing %s at %d: %w", path, off, err)
		}
		off += int64(n)
	}
	if err := unix.Fdatasync(fd); err != nil {
		return fmt.Errorf("uring: syncing %s: %w", path, err)
	}
	return syncDir(path)
}

func syncDir(path string) error {
	dir := filepath.Dir(path)
	fd, err := unix.Open(dir, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("uring: opening %s: %w", dir, err)
	}
	defer func() { _ = unix.Close(fd) }()
	if err := unix.Fsync(fd); err != nil {
		return fmt.Errorf("uring: syncing %s: %w", dir, err)
	}
	return nil
}
