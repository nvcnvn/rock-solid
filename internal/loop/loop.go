package loop

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/trace"
	"golang.org/x/sys/unix"
)

// Machine is the state machine a loop runs; one that owns several dispatches to them itself.
type Machine interface {
	OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action
}

// Driver turns the bytes a loop reads from a connection into events and executes the actions
// the loop does not itself, which are all but SetTimer.
type Driver interface {
	// Read consumes all of in, read from c, handing the loop each event it completes through Deliver.
	Read(c ConnID, in []byte)
	// Closed reports that c reached its end or failed and the loop closed it.
	Closed(c ConnID)
	// Execute runs a NetSend or disk action; it must not call Deliver, whose action buffer it is reading from.
	Execute(a *core.Action)
}

// Disk is a ring of disk I/O a loop drives: the loop watches Fd, collects completions every pass
// and submits once at the end of a pass.
type Disk interface {
	// Fd turns readable when completions arrive.
	Fd() int
	// Reap appends every completion waiting to evs.
	Reap(evs []core.Event) []core.Event
	// Submit hands the kernel what Execute queued since the last Submit, in one io_uring_enter
	// that never waits, and returns that call, Submit 0 when it made none.
	Submit() Enter
	// Waiting reports a completion to reap, or a queued submission the ring can take now,
	// either of which the loop must not block on.
	Waiting() bool
}

// Enter is one io_uring_enter: the entries and min_complete passed, the entries the kernel took
// and the error it returned.
type Enter struct {
	Submit, Wait, Took uint32
	Errno              unix.Errno
}

// ConnID is the number a loop gives a connection, the one its trace records name it by.
type ConnID uint32

// Config sets up a loop; zero fields take their defaults.
type Config struct {
	Node uint32
	ID   uint16
	Pool core.BufPool
	// ReadBudget is the most a pass reads from one connection, 256 KiB by default.
	ReadBudget int
	// InboxSize is the events the inbox holds, a power of two, 1,024 by default.
	InboxSize int
	// InboxBudget is the most inbox events a pass handles, 256 by default.
	InboxBudget int
	// QueueCap is the bytes queued for writing above which the loop stops reading clients, 16 MiB by default.
	QueueCap int
	// Trace, when set, receives a record of every event, action and syscall.
	Trace   *trace.Ring
	MsgType func(*core.Msg) uint64
	// PlantBlock plants a loop that blocks in Wait with work pending, for a run that must catch it.
	PlantBlock bool
}

// maxIOV is the most frames a connection holds queued, and so the most one writev gathers.
const maxIOV = 64

// Loop is one event loop: its kernel, its machine and driver, connections, timers and inbox.
type Loop struct {
	cfg     Config
	k       Kernel
	m       Machine
	d       Driver
	ctx     core.Context
	clocks  core.Clocks
	pass    uint64
	ready   []Ready
	rbuf    []byte
	conns   []conn
	dirty   []ConnID
	closing []ConnID
	hot     int
	queued  int
	capped  bool
	timers  core.Timers
	fire    []core.TimerID
	armed   core.Tick
	inbox   *Inbox
	disk    Disk
	evs     []core.Event
	drain   [8]byte
	ev      core.Event
	out     []core.Action
	iov     [maxIOV]unix.Iovec
	stopped atomic.Bool
}

type conn struct {
	fd      int
	open    bool
	peer    bool
	dirty   bool
	blocked bool
	out     bool
	muted   bool
	frames  [maxIOV]frame
	head    int
	n       int
}

type frame struct {
	buf  core.IOBuf
	off  uint32
	len  uint32
	free bool
}

// New makes a loop over k running m, whose connections' bytes go to d, and reads k's clocks: the
// first pass delivers Resumed if the node stood still since.
func New(cfg Config, k Kernel, m Machine, d Driver) *Loop {
	if cfg.ReadBudget == 0 {
		cfg.ReadBudget = 256 << 10
	}
	if cfg.InboxSize == 0 {
		cfg.InboxSize = 1024
	}
	if cfg.InboxBudget == 0 {
		cfg.InboxBudget = 256
	}
	if cfg.QueueCap == 0 {
		cfg.QueueCap = 16 << 20
	}
	return &Loop{
		cfg:    cfg,
		k:      k,
		m:      m,
		d:      d,
		ctx:    core.Context{Pool: cfg.Pool},
		clocks: k.Clocks(),
		ready:  make([]Ready, 0, 1024),
		rbuf:   make([]byte, cfg.ReadBudget),
		inbox:  newInbox(cfg.InboxSize, k),
		out:    make([]core.Action, 0, 64),
		fire:   make([]core.TimerID, 0, 64),
	}
}

// Inbox is the loop's inbox, which any goroutine may post to.
func (l *Loop) Inbox() *Inbox { return l.inbox }

// Context is the loop's context as of its latest pass.
func (l *Loop) Context() *core.Context { return &l.ctx }

// Passes is the number of passes the loop has made.
func (l *Loop) Passes() uint64 { return l.pass }

// AddConn takes over fd, a non-blocking stream to a peer loop or, without peer, a client, and
// returns its ID: the lowest one free.
func (l *Loop) AddConn(fd int, peer bool) (ConnID, error) {
	fl, err := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
	if err != nil {
		return 0, fmt.Errorf("loop: flags of fd %d: %w", fd, err)
	}
	if fl&unix.O_NONBLOCK == 0 {
		return 0, fmt.Errorf("loop: fd %d is blocking", fd)
	}
	id := len(l.conns)
	for i := range l.conns {
		if !l.conns[i].open {
			id = i
			break
		}
	}
	if id >= int(TokenDisk) {
		return 0, errors.New("loop: out of connection IDs")
	}
	if id == len(l.conns) {
		l.conns = append(l.conns, conn{})
	}
	l.syscall(trace.SysEpollCtl, uint64(id), 1, 0)
	if err := l.k.Watch(fd, uint32(id)); err != nil {
		return 0, err
	}
	l.conns[id] = conn{fd: fd, open: true, peer: peer}
	if cap(l.dirty) < len(l.conns) {
		l.dirty = append(make([]ConnID, 0, 2*len(l.conns)), l.dirty...)
		l.closing = append(make([]ConnID, 0, 2*len(l.conns)), l.closing...)
	}
	if !peer && l.capped {
		l.conns[id].muted = true
		l.interest(ConnID(id))
	}
	return ConnID(id), nil
}

// AttachDisk makes d the loop's disk ring, watched under TokenDisk; a loop has at most one.
func (l *Loop) AttachDisk(d Disk) error {
	if l.disk != nil {
		return errors.New("loop: a disk ring is attached already")
	}
	l.syscall(trace.SysEpollCtl, uint64(TokenDisk), 1, 0)
	if err := l.k.Watch(d.Fd(), TokenDisk); err != nil {
		return err
	}
	l.disk = d
	l.evs = make([]core.Event, 0, 256)
	return nil
}

// Queue queues the first n bytes of b to be written to c in this pass's writev, freeing b once
// written when free is set; false when c holds maxIOV frames already, is closed, or n is not in
// 1 to b's length.
func (l *Loop) Queue(c ConnID, b core.IOBuf, n int, free bool) bool {
	cn := &l.conns[c]
	if !cn.open || cn.n == maxIOV || n <= 0 || n > len(l.ctx.Pool.Bytes(b)) {
		return false
	}
	cn.frames[(cn.head+cn.n)%maxIOV] = frame{buf: b, len: uint32(n), free: free}
	cn.n++
	l.queued += n
	if !cn.dirty {
		cn.dirty = true
		l.dirty = append(l.dirty, c)
	}
	return true
}

// Queued is the bytes queued for writing over every connection.
func (l *Loop) Queued() int { return l.queued }

// Pending is what a loop still has to do.
type Pending struct {
	// Work is set while the loop must not block: an inbox event queued, a timer due, a frame
	// queued since the last flush, or a connection that filled its read budget in the last pass.
	Work bool
	// Deadline is the earliest timer, 0 when none is armed.
	Deadline core.Tick
}

// Pending reports what the loop still has to do.
func (l *Loop) Pending() Pending {
	var p Pending
	if at, ok := l.timers.Earliest(); ok {
		p.Deadline = at
		p.Work = at <= l.ctx.Now
	}
	p.Work = p.Work || l.hot > 0 || len(l.dirty) > 0 || l.inbox.ready() || l.disk != nil && l.disk.Waiting()
	return p
}

// Pass is one turn of the loop: wait, read the clocks, deliver Resumed if the node stood still
// since the last pass, fire the timers due, deliver disk completions, handle the inbox, then peer
// connections before clients, write each connection once, stop or resume reading clients at the
// queue cap, submit disk I/O and arm the timer. Now and Raw never go back: a reading below the last one leaves them where they were,
// and delivers Resumed.
func (l *Loop) Pass() {
	block := l.cfg.PlantBlock || !l.Pending().Work
	l.ready = l.k.Wait(l.ready[:0], block)
	c := l.k.Clocks()
	resumed := core.Resumed(l.clocks, c, core.ResumeThreshold)
	l.clocks = c
	l.ctx.Now, l.ctx.Raw = max(l.ctx.Now, c.Boot), max(l.ctx.Raw, c.Raw)
	l.pass++
	l.hot = 0
	l.syscall(trace.SysEpollWait, uint64(len(l.ready)), 0, 0)
	if resumed {
		l.ev = core.Event{Kind: core.EvResumed}
		l.Deliver(&l.ev)
	}
	l.fire = l.fire[:0]
	for {
		id, ok := l.timers.PopDue(l.ctx.Now)
		if !ok {
			break
		}
		l.fire = append(l.fire, id)
	}
	for _, id := range l.fire {
		// A timer an earlier one's handler armed again waits for its new deadline.
		if l.timers.Armed(id) {
			continue
		}
		l.ev = core.Event{Kind: core.EvTimer, Timer: id}
		l.Deliver(&l.ev)
	}
	if l.armed <= l.ctx.Now {
		l.armed = 0
	}
	for _, r := range l.ready {
		switch r.Token {
		case TokenWake:
			l.inbox.notified.Store(false)
		case TokenDisk:
			n, _ := rawRead(l.disk.Fd(), l.drain[:])
			l.syscall(trace.SysRead, uint64(TokenDisk), uint64(max(n, 0)), 0)
		}
	}
	if l.disk != nil {
		l.evs = l.disk.Reap(l.evs[:0])
		for i := range l.evs {
			l.Deliver(&l.evs[i])
		}
	}
	for range l.cfg.InboxBudget {
		if !l.inbox.pop(&l.ev) {
			break
		}
		l.Deliver(&l.ev)
	}
	l.handle(true)
	l.handle(false)
	l.flush()
	l.applyCap()
	if l.disk != nil {
		if e := l.disk.Submit(); e.Submit > 0 {
			l.record(trace.Record{Name: trace.SysIOUringEnter, Fields: [trace.MaxFields]uint64{uint64(e.Submit), uint64(e.Wait), uint64(e.Took), uint64(e.Errno)}})
		}
	}
	if at, ok := l.timers.Earliest(); ok && at != l.armed {
		l.armed = at
		l.syscall(trace.SysTimerfdSettime, uint64(at), 0, 0)
		l.k.Arm(at)
	}
}

// Run makes passes until Stop.
func (l *Loop) Run() {
	for !l.stopped.CompareAndSwap(true, false) {
		l.Pass()
	}
}

// Stop ends the current or next Run after its current pass; any goroutine may call it.
func (l *Loop) Stop() {
	l.stopped.Store(true)
	l.k.Wake()
}

// Deliver hands ev to the machine and executes the actions it emits.
func (l *Loop) Deliver(ev *core.Event) {
	l.record(trace.EventRecord(ev, l.cfg.MsgType))
	l.out = l.m.OnEvent(&l.ctx, ev, l.out[:0])
	for i := range l.out {
		a := &l.out[i]
		l.record(trace.ActionRecord(a, l.cfg.MsgType))
		if a.Kind == core.ActSetTimer {
			l.timers.Set(a.Timer, a.At)
		} else {
			l.d.Execute(a)
		}
	}
}

func (l *Loop) handle(peer bool) {
	for _, r := range l.ready {
		if r.Token >= TokenDisk {
			continue
		}
		cn := &l.conns[r.Token]
		if !cn.open || cn.peer != peer {
			continue
		}
		if r.Out && cn.blocked {
			cn.blocked = false
			if !cn.dirty {
				cn.dirty = true
				l.dirty = append(l.dirty, ConnID(r.Token))
			}
		}
		switch {
		case (r.In || r.Hup) && (peer || l.queued < l.cfg.QueueCap):
			l.read(ConnID(r.Token))
		case r.Hup && cn.muted:
			// A muted client is reported only for a hang-up or error, which no mask stops: it
			// would be reported every pass until the cap clears.
			l.Close(ConnID(r.Token))
			l.d.Closed(ConnID(r.Token))
		}
	}
}

func (l *Loop) read(c ConnID) {
	cn := &l.conns[c]
	n, err := rawRead(cn.fd, l.rbuf)
	l.syscall(trace.SysRead, uint64(c), uint64(max(n, 0)), 0)
	switch {
	case err == unix.EAGAIN || err == unix.EINTR:
	case err != 0 || n == 0:
		l.Close(c)
		l.d.Closed(c)
	default:
		if n == len(l.rbuf) {
			l.hot++
		}
		l.d.Read(c, l.rbuf[:n])
	}
}

// Close closes c, freeing what it still had queued.
func (l *Loop) Close(c ConnID) {
	cn := &l.conns[c]
	if !cn.open {
		return
	}
	for ; cn.n > 0; cn.n-- {
		f := &cn.frames[cn.head]
		l.queued -= int(f.len - f.off)
		if f.free {
			l.ctx.Pool.Free(f.buf)
		}
		cn.head = (cn.head + 1) % maxIOV
	}
	cn.open = false
	l.syscall(trace.SysEpollCtl, uint64(c), 0, 0)
	_ = l.k.Unwatch(cn.fd)
	l.syscall(trace.SysClose, uint64(c), 0, 0)
	_ = unix.Close(cn.fd)
}

// flush writes every dirty connection once. A connection a write closes is reported to the
// driver only after the walk, so what the driver queues then waits for the next pass.
func (l *Loop) flush() {
	for _, c := range l.dirty {
		cn := &l.conns[c]
		cn.dirty = false
		if cn.open && !cn.blocked && cn.n > 0 {
			l.write(c)
		}
	}
	l.dirty = l.dirty[:0]
	for _, c := range l.closing {
		l.d.Closed(c)
	}
	l.closing = l.closing[:0]
}

// applyCap mutes every client once the bytes queued reach the cap, and unmutes them below it:
// epoll is level-triggered, so a client left readable but unread would end every Wait at once.
func (l *Loop) applyCap() {
	over := l.queued >= l.cfg.QueueCap
	if over == l.capped {
		return
	}
	l.capped = over
	for i := range l.conns {
		cn := &l.conns[i]
		if cn.open && !cn.peer {
			cn.muted = over
			l.interest(ConnID(i))
		}
	}
}

func (l *Loop) interest(c ConnID) {
	cn := &l.conns[c]
	l.syscall(trace.SysEpollCtl, uint64(c), b2u(!cn.muted), b2u(cn.out))
	if err := l.k.Modify(cn.fd, uint32(c), !cn.muted, cn.out); err != nil {
		panic(fmt.Sprintf("loop: changing what connection %d is watched for: %v", c, err))
	}
}

func (l *Loop) write(c ConnID) {
	cn := &l.conns[c]
	total := 0
	for i := range cn.n {
		f := &cn.frames[(cn.head+i)%maxIOV]
		b := l.ctx.Pool.Bytes(f.buf)[f.off:f.len]
		l.iov[i].Base = &b[0]
		l.iov[i].SetLen(len(b))
		total += len(b)
	}
	n, err := rawWritev(cn.fd, l.iov[:cn.n])
	l.syscall(trace.SysWritev, uint64(c), uint64(cn.n), uint64(max(n, 0)))
	for i := range cn.n {
		l.iov[i] = unix.Iovec{}
	}
	switch {
	case err == unix.EAGAIN || err == unix.EINTR:
		n = 0
	case err != 0:
		l.Close(c)
		l.closing = append(l.closing, c)
		return
	}
	l.queued -= n
	for n > 0 {
		f := &cn.frames[cn.head]
		left := int(f.len - f.off)
		if n < left {
			f.off += uint32(n)
			break
		}
		n -= left
		if f.free {
			l.ctx.Pool.Free(f.buf)
		}
		cn.head = (cn.head + 1) % maxIOV
		cn.n--
	}
	cn.blocked = cn.n > 0
	if cn.blocked != cn.out {
		cn.out = cn.blocked
		l.interest(c)
	}
}

func b2u(b bool) uint64 {
	if b {
		return 1
	}
	return 0
}

func (l *Loop) syscall(name trace.Name, a, b, c uint64) {
	l.record(trace.Record{Name: name, Fields: [trace.MaxFields]uint64{a, b, c}})
}

func (l *Loop) record(r trace.Record) {
	if l.cfg.Trace == nil {
		return
	}
	r.Tick, r.Pass, r.Node, r.Loop = l.ctx.Now, l.pass, l.cfg.Node, l.cfg.ID
	l.cfg.Trace.Add(&r)
}
