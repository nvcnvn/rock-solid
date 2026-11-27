package fuse

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"slices"
	"time"

	"golang.org/x/sys/unix"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/fsclient"
	"github.com/nvcnvn/rock-solid/internal/fuseproto"
	"github.com/nvcnvn/rock-solid/internal/meta"
)

// The daemon's timers: RouterTimer is its meta router's, and ReadyTimer is never armed but
// delivered by the loop's driver when the device has requests to read.
const (
	RouterTimer core.TimerID = 0x66757365
	ReadyTimer  core.TimerID = 0x66726479
)

// batch is the most requests one Serve reads, so a busy mount leaves the loop its other work.
const batch = 64

// ErrGone is a device whose connection the kernel ended: the mount was unmounted or aborted.
var ErrGone = errors.New("fuse: the mount's connection is gone")

// Device is a mount's /dev/fuse descriptor, non-blocking: each read takes one request, each write
// one reply.
type Device struct{ Fd int }

// Read reads one request into b, 0 when none waits.
func (d Device) Read(b []byte) (int, error) {
	n, err := unix.Read(d.Fd, b)
	switch {
	case err == unix.EAGAIN || err == unix.EINTR:
		return 0, nil
	case err == unix.ENODEV:
		return 0, ErrGone
	case err != nil:
		return 0, err
	}
	return n, nil
}

// Write writes one reply. ENOENT, a reply to a request the kernel no longer waits for, as an
// interrupted one, is no error.
func (d Device) Write(b []byte) error {
	_, err := unix.Write(d.Fd, b)
	switch {
	case err == nil, err == unix.ENOENT:
		return nil
	case err == unix.ENODEV:
		return ErrGone
	}
	return err
}

// Config is a daemon's mount: its volume, the pool its chunks go to, and Inc, which names this
// start of the process and differs between starts (meta.NewClient).
type Config struct {
	Volume uint32
	Pool   core.PoolID
	Inc    uint64
}

// Daemon serves one mount on one event loop: it reads the device's requests into the filesystem
// client and makes the calls that machine asks for, its Txns and Reads through a meta.Client and
// its chunk I/O through the chunk client of the session it holds, one client per session
// incarnation. It runs on a loop that hosts no MetaStore replica, so it never sends to its own ID.
type Daemon struct {
	cfg     Config
	dev     Device
	buf     core.IOBuf
	m       *fsclient.Machine
	mc      *meta.Client
	cl      *chunkstore.Client
	session uint64
	table   chunkstore.Table
	disks   []meta.DiskAddr
	mapped  bool
	txns    map[uint64]uint64
	reads   map[uint64]*held
	free    []*held
	rq      fuseproto.Request
	acts    []core.Action
	fetch   uint64
	outs    stack[fsclient.Output]
	routs   stack[meta.RouterOutput]
	couts   stack[chunkstore.ClientOutput]
}

// held is a Read in flight, kept by the daemon since the router keeps a pointer to it.
type held struct {
	token uint64
	rd    meta.Read
}

// stack hands each nested call an output of its own, as answers handed out inline call back in.
type stack[T any] struct {
	s []*T
	n int
}

func (s *stack[T]) get() *T {
	if s.n == len(s.s) {
		s.s = append(s.s, new(T))
	}
	s.n++
	return s.s[s.n-1]
}

func (s *stack[T]) put() { s.n-- }

// New is a daemon of cfg's mount, with no session, shard map or device yet.
func New(cfg Config) *Daemon {
	r, err := meta.NewRouter(&meta.ShardMap{Groups: []meta.Group{{ID: 1}, {ID: 2, Start: []byte{meta.SpaceCtl + 1}}}}, RouterTimer)
	if err != nil {
		panic(err)
	}
	return &Daemon{cfg: cfg, mc: meta.NewClient(r, cfg.Inc), txns: map[uint64]uint64{}, reads: map[uint64]*held{}, buf: core.NoBuf}
}

// Ready reports whether the daemon can serve: it holds a session and a shard map that places
// every voter's loop.
func (d *Daemon) Ready() bool { return d.m != nil && d.mapped }

// WantsTable reports that the daemon holds no placement table, or its chunk client waits for a newer one.
func (d *Daemon) WantsTable() bool { return d.table.Epoch == 0 || d.fetch > d.table.Epoch }

// Volume is the mount's volume.
func (d *Daemon) Volume() uint32 { return d.cfg.Volume }

// Attach makes dev the device the daemon serves.
func (d *Daemon) Attach(dev Device) { d.dev = dev }

// SetSession takes the node's session, starting the filesystem client on the first and a chunk
// client for each new one: a fence starts a new session, whose incarnation is 1.
func (d *Daemon) SetSession(ctx *core.Context, session uint64, acts []core.Action) []core.Action {
	if session == 0 || session == d.session {
		return acts
	}
	d.acts, d.session = acts, session
	if d.m == nil {
		// The volume's one data group spans every slot.
		d.m = fsclient.New(fsclient.Config{Volume: d.cfg.Volume, Pool: d.cfg.Pool, Session: session, Mount: 1, SlotLimit: 1<<32 - 1})
	}
	d.cl = chunkstore.NewClient(chunkstore.ClientConfig{Session: session, Incarnation: 1})
	d.setAddrs()
	if d.table.Epoch != 0 {
		t := d.table
		d.chunked(ctx, func(o *chunkstore.ClientOutput) { d.cl.SetTable(ctx, t, o) })
	}
	return d.acts
}

// SetTable hands the chunk client placement table t, or the table delta makes of the one held.
func (d *Daemon) SetTable(ctx *core.Context, t *chunkstore.Table, delta *chunkstore.Delta, acts []core.Action) []core.Action {
	d.acts = acts
	switch {
	case t != nil && t.Epoch > d.table.Epoch:
		d.table = *t
	case delta != nil && delta.Epoch == d.table.Epoch+1:
		next, err := d.table.Apply(delta)
		if err != nil {
			log.Printf("fuse: placement delta %d: %v", delta.Epoch, err)
			return acts
		}
		d.table = next
	default:
		return acts
	}
	if d.cl != nil {
		t := d.table
		d.chunked(ctx, func(o *chunkstore.ClientOutput) { d.cl.SetTable(ctx, t, o) })
	}
	return d.acts
}

// SetMembership takes the disks' loops and, when not nil, the shard map's groups. A map is taken
// only once every voter's disk has a loop, since a send to a replica of no known loop would go to
// loop 0, the daemon's own.
func (d *Daemon) SetMembership(ctx *core.Context, plans []meta.Group, disks []meta.DiskAddr, acts []core.Action) []core.Action {
	d.acts = acts
	if disks != nil {
		d.disks = slices.Clone(disks)
		d.setAddrs()
	}
	if plans == nil {
		return acts
	}
	smap := meta.ShardMap{Groups: slices.Clone(plans)}
	if smap.CheckRanges() != nil || len(smap.Groups) < 2 {
		return acts
	}
	for _, g := range smap.Groups[1:] {
		for _, v := range g.Voters {
			i := slices.IndexFunc(d.disks, func(a meta.DiskAddr) bool { return a.Disk == v.Disk })
			if i < 0 || d.disks[i].At.Loop == 0 {
				return acts
			}
			d.mc.SetLoop(v.Node, g.ID, d.disks[i].At.Loop)
		}
	}
	d.routed(ctx, func(o *meta.RouterOutput) {
		if err := d.mc.R.SetMap(&smap, o); err != nil {
			panic(fmt.Sprintf("fuse: shard map: %v", err))
		}
	})
	d.mapped = true
	return d.acts
}

func (d *Daemon) setAddrs() {
	if d.cl == nil {
		return
	}
	for _, a := range d.disks {
		d.cl.SetAddr(a.Disk, a.At)
	}
}

// OnEvent takes a meta answer, a chunk reply, either client's timer or a lost peer.
func (d *Daemon) OnEvent(ctx *core.Context, ev *core.Event, acts []core.Action) []core.Action {
	d.acts = acts
	switch {
	case ev.Kind == core.EvNetRecv && ev.Msg.Group(), ev.Kind == core.EvTimer && ev.Timer == RouterTimer:
		d.routed(ctx, func(o *meta.RouterOutput) { d.mc.OnEvent(ctx, ev, o) })
	case ev.Kind == core.EvNetRecv && d.cl != nil, ev.Kind == core.EvTimer && ev.Timer == chunkstore.ClientTimer && d.cl != nil:
		d.chunked(ctx, func(o *chunkstore.ClientOutput) { d.cl.OnEvent(ctx, ev, o) })
	case ev.Kind == core.EvNetRecv && ev.Msg.Buf != core.NoBuf:
		ctx.Pool.Free(ev.Msg.Buf)
	case ev.Kind == core.EvPeerLost:
		d.routed(ctx, func(o *meta.RouterOutput) { d.mc.OnEvent(ctx, ev, o) })
		if d.cl != nil {
			d.chunked(ctx, func(o *chunkstore.ClientOutput) { d.cl.OnEvent(ctx, ev, o) })
		}
	}
	return d.acts
}

// Serve reads and serves up to batch requests from the device; ErrGone once the kernel ended the
// connection.
func (d *Daemon) Serve(ctx *core.Context, acts []core.Action) ([]core.Action, error) {
	d.acts = acts
	if d.buf == core.NoBuf {
		b, err := ctx.Pool.Alloc(ReadBuffer)
		if err != nil {
			return acts, nil
		}
		d.buf = b
	}
	for range batch {
		buf := ctx.Pool.Bytes(d.buf)[:ReadBuffer]
		n, err := d.dev.Read(buf)
		if err != nil {
			return d.acts, err
		}
		if n == 0 {
			break
		}
		if err := d.request(ctx, buf[:n]); err != nil {
			return d.acts, err
		}
	}
	return d.acts, nil
}

// request serves one frame: INIT here, every other request through the filesystem client.
func (d *Daemon) request(ctx *core.Context, frame []byte) error {
	err := fuseproto.Decode(frame, &d.rq)
	switch {
	case err != nil && len(frame) >= 16:
		var reply [fuseproto.OutHeaderSize]byte
		errno := fuseproto.EIO
		if errors.Is(err, fuseproto.ErrOpcode) {
			errno = fuseproto.ENOSYS
		}
		return d.dev.Write(fuseproto.AppendError(reply[:0], binary.LittleEndian.Uint64(frame[8:]), errno))
	case err != nil:
		return nil
	case d.rq.Opcode == fuseproto.OpInit:
		var reply [128]byte
		o, err := Negotiate(&d.rq.Init)
		if err != nil {
			log.Printf("fuse: %v: %d.%d", err, d.rq.Init.Major, d.rq.Init.Minor)
			return d.dev.Write(fuseproto.AppendError(reply[:0], d.rq.Unique, eproto))
		}
		return d.dev.Write(fuseproto.AppendInit(reply[:0], d.rq.Unique, &o))
	}
	d.feed(ctx, func(o *fsclient.Output) { d.m.Request(ctx, time.Now().UnixNano(), &d.rq, o) })
	return nil
}

const eproto fuseproto.Errno = 71

// feed runs f on the filesystem client and makes the calls its output asks for, the chunk calls
// first, as their data aliases the request's frame or the machine's memory only until they are made.
func (d *Daemon) feed(ctx *core.Context, f func(o *fsclient.Output)) {
	o := d.outs.get()
	defer d.outs.put()
	o.Reset()
	f(o)
	for i := range o.Chunks {
		d.chunk(ctx, &o.Chunks[i])
	}
	for _, s := range o.Txns {
		d.txn(ctx, s)
	}
	for b := o.Replies; len(b) > 0; {
		n := binary.LittleEndian.Uint32(b)
		d.write(b[:n])
		b = b[n:]
	}
	for _, buf := range o.Data {
		b := ctx.Pool.Bytes(buf)
		d.write(b[:binary.LittleEndian.Uint32(b)])
		ctx.Pool.Free(buf)
	}
	for _, rd := range o.Reads {
		d.read(ctx, rd)
	}
}

func (d *Daemon) write(frame []byte) {
	if err := d.dev.Write(frame); err != nil && !errors.Is(err, ErrGone) {
		log.Printf("fuse: a reply the kernel refused: %v", err)
	}
}

// chunk makes one chunk-client call. One the client cannot take, as with its pool exhausted,
// completes each chunk it covers with an error, which the machine answers EIO.
func (d *Daemon) chunk(ctx *core.Context, c *fsclient.ChunkCall) {
	var err error
	d.chunked(ctx, func(o *chunkstore.ClientOutput) {
		if c.Read {
			err = d.cl.Read(ctx, c.Token, c.File, c.Off, c.N, o)
		} else {
			err = d.cl.Write(ctx, c.Token, c.File, c.Off, c.Data, false, o)
		}
	})
	if err == nil {
		return
	}
	n := uint64(c.N)
	if !c.Read {
		n = uint64(len(c.Data))
	}
	for off := c.Off; off < c.Off+n; off = (off/core.ChunkSize + 1) * core.ChunkSize {
		done := chunkstore.Completion{Token: c.Token, Off: off, Status: chunkstore.StatusMissing, Buf: core.NoBuf}
		d.feed(ctx, func(o *fsclient.Output) { d.m.ChunkDone(ctx, &done, o) })
	}
}

// chunked runs f on the chunk client and hands each completion to the filesystem client.
func (d *Daemon) chunked(ctx *core.Context, f func(o *chunkstore.ClientOutput)) {
	o := d.couts.get()
	defer d.couts.put()
	o.Reset()
	f(o)
	d.acts = append(d.acts, o.Actions...)
	d.fetch = max(d.fetch, o.Fetch)
	for i := range o.Done {
		d.feed(ctx, func(out *fsclient.Output) { d.m.ChunkDone(ctx, &o.Done[i], out) })
	}
}

// txn sends a Txn, recording its token before the router's answers are handed out, since the
// router may answer it inline. A Txn the router refuses is a broken invariant: the daemon routes
// only by a map it checked, and a refusal handed back would be built and refused again forever.
func (d *Daemon) txn(ctx *core.Context, s fsclient.TxnSend) {
	d.routed(ctx, func(o *meta.RouterOutput) {
		tok, err := d.mc.Txn(ctx, s.Txn, o)
		if err != nil {
			panic(fmt.Sprintf("fuse: the router refused a Txn: %v", err))
		}
		d.txns[tok] = s.Token
	})
}

// read sends a Read under a copy the daemon keeps until its answer.
func (d *Daemon) read(ctx *core.Context, rd fsclient.Read) {
	var h *held
	if n := len(d.free); n > 0 {
		h, d.free = d.free[n-1], d.free[:n-1]
	} else {
		h = new(held)
	}
	h.token = rd.Token
	h.rd = meta.Read{Start: append(h.rd.Start[:0], rd.Start...), End: append(h.rd.End[:0], rd.End...), Limit: rd.Limit}
	if rd.End == nil {
		h.rd.End = nil
	}
	d.send(ctx, h)
}

func (d *Daemon) send(ctx *core.Context, h *held) {
	d.routed(ctx, func(o *meta.RouterOutput) {
		tok, err := d.mc.Read(ctx, &h.rd, o)
		if err != nil {
			panic(fmt.Sprintf("fuse: the router refused a Read: %v", err))
		}
		d.reads[tok] = h
	})
}

// routed runs f on the meta client and hands each answer to the filesystem client, sending again
// a Read answered with an error: the router has retried what it can, and the machine has no
// failed read.
func (d *Daemon) routed(ctx *core.Context, f func(o *meta.RouterOutput)) {
	o := d.routs.get()
	defer d.routs.put()
	o.Reset()
	f(o)
	d.acts = append(d.acts, o.Actions...)
	for i := range o.Reads {
		a := &o.Reads[i]
		h, ok := d.reads[a.Token]
		if !ok {
			continue
		}
		delete(d.reads, a.Token)
		if a.Err != nil {
			d.send(ctx, h)
			continue
		}
		tok := h.token
		d.free = append(d.free, h)
		d.feed(ctx, func(out *fsclient.Output) { d.m.ReadDone(ctx, tok, a.KVs, out) })
	}
	for i := range o.Answers {
		a := &o.Answers[i]
		tok, ok := d.txns[a.Token]
		if !ok {
			continue
		}
		delete(d.txns, a.Token)
		d.feed(ctx, func(out *fsclient.Output) { d.m.TxnDone(ctx, tok, &a.Result, a.Err, out) })
	}
}
