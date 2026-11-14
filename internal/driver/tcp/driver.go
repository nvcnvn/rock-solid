package tcp

import (
	"cmp"
	"fmt"
	"slices"

	"golang.org/x/sys/unix"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/loop"
)

const (
	// batchBytes is the pool buffer a connection's frames are gathered into, one writev frame
	// each, once a full one waits. A connection with nothing waiting starts with firstBatch.
	batchBytes = 512 << 10
	firstBatch = 64 << 10
	// maxQueued is the bytes queued on one connection at which a send ends it, so it holds at most
	// that and one frame: above the loop's default 16 MiB queue cap and one pass's 4 MiB of client
	// reads, so a peer that keeps reading is never lost.
	maxQueued = 32 << 20
	// acceptBudget is the most connections one Accept takes.
	acceptBudget = 64
	// firstBackoff and maxBackoff bound the wait after a failed dial or accept, which doubles
	// per failure.
	firstBackoff = core.Tick(10_000)
	maxBackoff   = core.Tick(1_000_000)
)

// Config sets up a driver.
type Config struct {
	// Self is the loop the driver serves.
	Self core.NodeID
	// Dial opens a non-blocking stream to peer's loop, connected or connecting.
	Dial func(peer core.NodeID) (int, error)
	// Seed seeds the driver's backoff jitter and names its incarnation, so it must differ
	// between the loop's starts.
	Seed uint64
}

// Driver is a loop's network driver: one connection per peer loop, opened on demand by the first
// send to it or accepted from it, every message checksummed and gathered into the connection's
// batch buffer. Any transport error closes the connection and delivers one PeerLost for its
// peer, and a send to a peer that cannot be reached delivers one by the next pass.
type Driver struct {
	// L is the loop the driver serves, set before its first pass.
	L     *loop.Loop
	cfg   Config
	rand  *core.Rand
	inc   uint64
	index map[core.NodeID]int
	peers []peer
	conns []conn
	lost  []int
	frees []core.IOBuf
	// acceptFails counts the accepts failed in a row on a lasting error.
	acceptFails int
	ev          core.Event
	f           frame
	hello       [helloLen]byte
}

// peer is a peer loop: its connection while one is up, whether this side dialed it, the
// incarnation the peer named in its hello on it, if heard, and the backoff after failed dials.
type peer struct {
	id      core.NodeID
	conn    loop.ConnID
	up      bool
	dialed  bool
	known   bool
	inc     uint64
	fails   int
	retryAt core.Tick
	lost    bool
}

// conn is a connection's state: its ID, its peer's index once named, -1 before, and its decoder.
type conn struct {
	id     loop.ConnID
	open   bool
	listen bool
	fd     int
	peer   int
	dialed bool
	dec    decoder
}

// New makes a driver; set L before the loop's first pass.
func New(cfg Config) *Driver {
	r := core.NewRand(cfg.Seed)
	return &Driver{cfg: cfg, rand: r, inc: r.Uint64(), index: map[core.NodeID]int{}}
}

// Listen hands the loop fd, a non-blocking listening socket whose connections the driver accepts.
func (d *Driver) Listen(fd int) error {
	c, err := d.L.AddListener(fd)
	if err != nil {
		return err
	}
	d.add(c, conn{listen: true, fd: fd, peer: -1})
	return nil
}

// add records cs as c's state, c being a connection the loop just added.
func (d *Driver) add(c loop.ConnID, cs conn) {
	for len(d.conns) <= c.Slot() {
		d.conns = append(d.conns, conn{peer: -1})
	}
	cs.id, cs.open = c, true
	d.conns[c.Slot()] = cs
}

// conn is c's state, nil once c is closed, whether or not its slot holds a newer connection.
func (d *Driver) conn(c loop.ConnID) *conn {
	if c.Slot() >= len(d.conns) {
		return nil
	}
	cs := &d.conns[c.Slot()]
	if !cs.open || cs.id != c {
		return nil
	}
	return cs
}

func (d *Driver) peer(id core.NodeID) int {
	i, ok := d.index[id]
	if !ok {
		i = len(d.peers)
		d.index[id] = i
		d.peers = append(d.peers, peer{id: id})
	}
	return i
}

// accept is unix.Accept, which tests replace to fail it.
var accept = unix.Accept

// Accept takes the connections waiting on listener c, each nameless until its hello. An error
// that lasts while connections wait, such as running out of descriptors, pauses the listener
// for a backoff, as a failed dial does, rather than waking the loop every pass.
func (d *Driver) Accept(c loop.ConnID) {
	lfd := d.conns[c.Slot()].fd
	for range acceptBudget {
		fd, _, err := accept(lfd)
		switch err {
		case nil:
			d.acceptFails = 0
		case unix.EMFILE, unix.ENFILE, unix.ENOBUFS, unix.ENOMEM:
			d.L.Pause(c, d.wait(&d.acceptFails))
			return
		case unix.ECONNABORTED, unix.EINTR:
			continue
		default:
			return
		}
		unix.CloseOnExec(fd)
		// A unix socket, as stepped tests accept, has no Nagle to turn off.
		_ = unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_NODELAY, 1)
		if unix.SetNonblock(fd, true) != nil {
			_ = unix.Close(fd)
			continue
		}
		id, err := d.L.AddConn(fd, true)
		if err != nil {
			_ = unix.Close(fd)
			return
		}
		d.L.Own(id)
		d.add(id, conn{fd: fd, peer: -1})
	}
}

// Read reads c's frames until none wait or c's read budget for the pass is spent: a hello names
// its peer, and each message becomes a NetRecv. scratch is the read-ahead the decoder reads small
// frames into. A bad frame closes c.
func (d *Driver) Read(c loop.ConnID, scratch []byte) {
	pool := d.L.Context().Pool
	var in []byte
	for more := true; ; {
		cs := d.conn(c)
		if cs == nil {
			return
		}
		ok, err := cs.dec.take(pool, &in, &d.f)
		switch {
		case err != nil:
			d.fail(c)
			return
		case ok:
			d.onFrame(c)
			continue
		case !more:
			return
		}
		v, size := cs.dec.space(pool, scratch)
		n, ended := d.L.Readv(c, v)
		if ended {
			d.closed(c, true)
			return
		}
		in = scratch[:cs.dec.read(n)]
		more = n == size
	}
}

func (d *Driver) onFrame(c loop.ConnID) {
	cs := &d.conns[c.Slot()]
	switch {
	case d.f.kind == kindHello:
		d.named(c)
	case cs.peer < 0 || cs.dialed && !d.peers[cs.peer].known:
		if d.f.data != core.NoBuf {
			d.L.Context().Pool.Free(d.f.data)
		}
		d.fail(c)
	default:
		d.ev = core.Event{Kind: core.EvNetRecv, Peer: d.peers[cs.peer].id}
		d.ev.Msg.Len = uint16(copy(d.ev.Msg.Body[:], d.f.inline))
		d.ev.Msg.Buf = d.f.data
		d.deliver(&d.ev)
	}
}

// named takes c's hello. A dialed connection's must name the loop dialed. An accepted one makes c
// its peer's connection in place of an older one, whose PeerLost follows, unless both sides
// dialed at once and this side's address is the lower: then its own dial stands and c is
// refused, so both sides keep the same connection. A dial at once is one whose hello is not yet
// heard or named the same incarnation; a peer that restarted names another.
func (d *Driver) named(c loop.ConnID) {
	cs := &d.conns[c.Slot()]
	id, inc := getHello(d.f.inline)
	if cs.dialed {
		p := &d.peers[cs.peer]
		if p.id != id || p.known || p.conn != c {
			d.fail(c)
			return
		}
		p.known, p.inc, p.fails, p.retryAt = true, inc, 0, 0
		return
	}
	if cs.peer >= 0 {
		d.fail(c)
		return
	}
	i := d.peer(id)
	p := &d.peers[i]
	old, replace := p.conn, p.up
	if replace && p.dialed && (!p.known || p.inc == inc) && less(d.cfg.Self, id) {
		d.fail(c)
		return
	}
	cs.peer = i
	if !d.send(c, kindHello, putHello(d.hello[:], d.cfg.Self, d.inc), nil) {
		return
	}
	p = &d.peers[i]
	p.conn, p.up, p.dialed, p.known, p.inc, p.fails, p.retryAt = c, true, false, true, inc, 0, 0
	if replace {
		d.L.Close(old)
		d.conns[old.Slot()].dec.reset(d.L.Context().Pool)
		d.conns[old.Slot()] = conn{peer: -1}
		d.ev = core.Event{Kind: core.EvPeerLost, Peer: id}
		d.deliver(&d.ev)
	}
}

func less(a, b core.NodeID) bool {
	return a.Node < b.Node || a.Node == b.Node && a.Loop < b.Loop
}

// Closed reports c ended by the loop: an EOF, a reset or a failed write.
func (d *Driver) Closed(c loop.ConnID) { d.closed(c, true) }

// fail closes c for a transport error the driver found.
func (d *Driver) fail(c loop.ConnID) {
	d.L.Close(c)
	d.closed(c, true)
}

// closed forgets c, discarding a partial frame, and, when c was its peer's connection, delivers
// the peer's PeerLost now, or when deliver is false by the next pass. A dial that ends before
// its peer's hello starts or lengthens the peer's backoff.
func (d *Driver) closed(c loop.ConnID, deliver bool) {
	cs := d.conn(c)
	if cs == nil {
		return
	}
	cs.dec.reset(d.L.Context().Pool)
	i := cs.peer
	*cs = conn{peer: -1}
	if i < 0 {
		return
	}
	p := &d.peers[i]
	if !p.up || p.conn != c {
		return
	}
	p.up = false
	if p.dialed && !p.known {
		d.backoff(p)
	}
	if deliver {
		d.ev = core.Event{Kind: core.EvPeerLost, Peer: p.id}
		d.deliver(&d.ev)
		return
	}
	d.lose(i)
}

func (d *Driver) backoff(p *peer) { p.retryAt = d.wait(&p.fails) }

// wait counts one more failure in a row in fails and returns when to try again.
func (d *Driver) wait(fails *int) core.Tick {
	w := min(firstBackoff<<min(*fails, 16), maxBackoff)
	*fails++
	return d.L.Context().Now + w/2 + core.Tick(d.rand.Below(uint64(w/2)+1))
}

// lose queues one PeerLost for peer i, delivered by the next Flush.
func (d *Driver) lose(i int) {
	if !d.peers[i].lost {
		d.peers[i].lost = true
		d.lost = append(d.lost, i)
	}
}

// deliver hands ev to the loop once every Reply buffer sent is freed: no action of an output is
// left to execute when a driver may deliver.
func (d *Driver) deliver(ev *core.Event) {
	d.free()
	d.L.Deliver(ev)
}

func (d *Driver) free() {
	if len(d.frees) == 0 {
		return
	}
	slices.SortFunc(d.frees, func(a, b core.IOBuf) int {
		return cmp.Or(cmp.Compare(a.Slot, b.Slot), cmp.Compare(a.Gen, b.Gen))
	})
	pool := d.L.Context().Pool
	for _, b := range slices.Compact(d.frees) {
		pool.Free(b)
	}
	d.frees = d.frees[:0]
}

// Execute sends a NetSend on its peer's connection, dialing one if there is none and the peer is
// not in backoff; a send that cannot go out delivers its peer's PeerLost by the next pass. A
// Reply's buffer is freed once every action of its output has run, since one buffer may go to
// several peers. A send to the driver's own loop is its machine's bug.
func (d *Driver) Execute(a *core.Action) {
	if a.Kind != core.ActNetSend || a.Peer == d.cfg.Self {
		panic(fmt.Sprintf("tcp: loop %v's network driver executes no %+v", d.cfg.Self, *a))
	}
	pool := d.L.Context().Pool
	var data []byte
	if a.Msg.Buf != core.NoBuf {
		data = pool.Bytes(a.Msg.Buf)
		if a.Reply {
			d.frees = append(d.frees, a.Msg.Buf)
		}
	}
	i := d.peer(a.Peer)
	p := &d.peers[i]
	if !p.up && !d.dial(i) {
		d.lose(i)
		return
	}
	c := d.peers[i].conn
	if !d.send(c, kindMsg, a.Msg.Inline(), data) {
		d.lose(i)
	}
}

// dial opens a connection to peer i and queues its hello, unless the peer is in backoff.
func (d *Driver) dial(i int) bool {
	p := &d.peers[i]
	if d.L.Context().Now < p.retryAt {
		return false
	}
	fd, err := d.cfg.Dial(p.id)
	if err != nil {
		d.backoff(p)
		return false
	}
	c, err := d.L.AddConn(fd, true)
	if err != nil {
		_ = unix.Close(fd)
		d.backoff(p)
		return false
	}
	d.L.Own(c)
	d.add(c, conn{fd: fd, peer: i, dialed: true})
	p = &d.peers[i]
	p.conn, p.up, p.dialed, p.known = c, true, true, false
	return d.send(c, kindHello, putHello(d.hello[:], d.cfg.Self, d.inc), nil)
}

// send gathers a frame into c's last batch buffer, or a new one; a frame the pool or the
// connection's limits cannot take closes c, its PeerLost deferred, as Execute may not deliver.
func (d *Driver) send(c loop.ConnID, kind byte, inline, data []byte) bool {
	pool := d.L.Context().Pool
	n := frameLen(len(inline), len(data))
	if len(data) > MaxData || d.L.QueuedOn(c) >= maxQueued {
		d.L.Close(c)
		d.closed(c, false)
		return false
	}
	size := firstBatch
	if b, used, ok := d.L.Tail(c); ok {
		if buf := pool.Bytes(b); used+n <= len(buf) {
			putFrame(buf[used:], kind, inline, data)
			d.L.Grow(c, n)
			return true
		}
		size = batchBytes
	}
	b, err := pool.Alloc(max(n, size))
	if err == nil {
		putFrame(pool.Bytes(b), kind, inline, data)
		if d.L.Queue(c, b, n, true) {
			return true
		}
		pool.Free(b)
	}
	d.L.Close(c)
	d.closed(c, false)
	return false
}

// Flush frees the Reply buffers sent and delivers the PeerLost events Execute deferred; those
// their handlers defer wait for the next pass.
func (d *Driver) Flush() bool {
	d.free()
	n := len(d.lost)
	for _, i := range d.lost[:n] {
		d.peers[i].lost = false
		d.ev = core.Event{Kind: core.EvPeerLost, Peer: d.peers[i].id}
		d.deliver(&d.ev)
	}
	d.lost = d.lost[:copy(d.lost, d.lost[n:])]
	return len(d.lost) > 0
}
