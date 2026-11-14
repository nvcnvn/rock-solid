package tcp

import (
	"cmp"
	"encoding/binary"
	"fmt"
	"net/netip"
	"os"
	"os/exec"
	"strings"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/loop"
)

// The open-loop latency spike's client protocol: a 24-byte header, the kind, the payload's
// length at 4, the request's ID at 8 and the payload's CRC32C at 16, then a 4 KiB payload; the
// answer is a header of kind ack, no payload, naming the ID.
const (
	lgHdr     = 24
	lgPayload = 4096
	lgWrite   = 1
	lgAck     = 3
	// clientNode marks a NodeID naming a client connection, by its ConnID in Loop: the load's
	// clients never reconnect, so no ID has a generation above its slot.
	clientNode = ^uint32(0)
	// seqRing bounds the writes in flight at the primary.
	seqRing = 1 << 18
)

// lgConn is a client connection's partial request.
type lgConn struct {
	hdr [lgHdr]byte
	hn  int
	buf core.IOBuf
	got int
}

// front is the primary's driver: the loadgen's clients on one listener, and the TCP driver
// for the replicas.
type front struct {
	*Driver
	ln      loop.ConnID
	clients map[loop.ConnID]*lgConn
	ev      core.Event
	bad     int
	maxQ    int
}

func (f *front) Accept(c loop.ConnID) {
	if c != f.ln {
		f.Driver.Accept(c)
		return
	}
	for {
		fd, _, err := unix.Accept4(f.Driver.conns[c].fd, unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC)
		if err != nil {
			return
		}
		_ = unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_NODELAY, 1)
		id, err := f.L.AddConn(fd, false)
		if err != nil {
			_ = unix.Close(fd)
			return
		}
		f.clients[id] = &lgConn{buf: core.NoBuf}
	}
}

func (f *front) Read(c loop.ConnID, in []byte) {
	lc, ok := f.clients[c]
	if !ok {
		f.Driver.Read(c, in)
		return
	}
	pool := f.L.Context().Pool
	for len(in) > 0 {
		if lc.hn < lgHdr {
			k := copy(lc.hdr[lc.hn:], in)
			lc.hn += k
			in = in[k:]
			if lc.hn < lgHdr {
				return
			}
			b, err := pool.Alloc(lgPayload)
			if err != nil || lc.hdr[0] != lgWrite || binary.LittleEndian.Uint32(lc.hdr[4:]) != lgPayload {
				panic(fmt.Sprintf("client request: %v %x", err, lc.hdr))
			}
			lc.buf, lc.got = b, 0
		}
		k := copy(pool.Bytes(lc.buf)[lc.got:lgPayload], in)
		lc.got += k
		in = in[k:]
		if lc.got < lgPayload {
			return
		}
		if core.CRC32C(pool.Bytes(lc.buf)[:lgPayload]) != binary.LittleEndian.Uint32(lc.hdr[16:]) {
			f.bad++
		}
		f.ev = core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{Node: clientNode, Loop: uint32(c)}}
		f.ev.Msg.Len = uint16(copy(f.ev.Msg.Body[:], lc.hdr[8:16]))
		f.ev.Msg.Buf = lc.buf
		lc.hn, lc.buf = 0, core.NoBuf
		f.Driver.deliver(&f.ev)
	}
}

func (f *front) Closed(c loop.ConnID) {
	if lc, ok := f.clients[c]; ok {
		if lc.buf != core.NoBuf {
			f.L.Context().Pool.Free(lc.buf)
		}
		delete(f.clients, c)
		return
	}
	f.Driver.Closed(c)
}

func (f *front) Execute(a *core.Action) {
	if a.Peer.Node != clientNode {
		f.Driver.Execute(a)
		return
	}
	c := loop.ConnID(a.Peer.Loop)
	if _, ok := f.clients[c]; !ok {
		return
	}
	pool := f.L.Context().Pool
	var ack [lgHdr]byte
	ack[0] = lgAck
	copy(ack[8:16], a.Msg.Inline())
	if b, used, ok := f.L.Tail(c); ok && used+lgHdr <= len(pool.Bytes(b)) {
		copy(pool.Bytes(b)[used:], ack[:])
		f.L.Grow(c, lgHdr)
		return
	}
	b, err := pool.Alloc(64 << 10)
	if err != nil {
		panic(err)
	}
	copy(pool.Bytes(b), ack[:])
	if !f.L.Queue(c, b, lgHdr, true) {
		pool.Free(b)
		f.L.Close(c)
		f.Closed(c)
	}
}

func (f *front) Flush() bool {
	f.maxQ = max(f.maxQ, f.L.Queued())
	return f.Driver.Flush()
}

// pwait is a write the primary waits on its replicas' acks for.
type pwait struct {
	client core.NodeID
	id     [8]byte
	acks   uint8
}

// primary stores each client write, sends it to both replicas and acks the client once both have.
type primary struct {
	replicas [2]core.NodeID
	next     uint64
	waits    []pwait
	store    []byte
	off      int
	lost     int
}

func (p *primary) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	switch {
	case ev.Kind == core.EvPeerLost:
		p.lost++
	case ev.Kind == core.EvNetRecv && ev.Peer.Node == clientNode:
		p.next++
		w := &p.waits[p.next%seqRing]
		if w.acks != 0 {
			panic("more writes in flight than the primary tracks")
		}
		w.client, w.acks = ev.Peer, 2
		copy(w.id[:], ev.Msg.Inline())
		p.off = storeAt(p.store, p.off, ctx.Pool.Bytes(ev.Msg.Buf)[:lgPayload])
		for _, r := range p.replicas {
			a := core.Action{Kind: core.ActNetSend, Peer: r, Reply: true}
			a.Msg.Len = 8
			binary.LittleEndian.PutUint64(a.Msg.Body[:], p.next)
			a.Msg.Buf = ev.Msg.Buf
			out = append(out, a)
		}
	case ev.Kind == core.EvNetRecv:
		w := &p.waits[binary.LittleEndian.Uint64(ev.Msg.Body[:])%seqRing]
		if w.acks--; w.acks == 0 {
			a := core.Action{Kind: core.ActNetSend, Peer: w.client}
			a.Msg.Len = uint16(copy(a.Msg.Body[:], w.id[:]))
			out = append(out, a)
		}
	}
	return out
}

func storeAt(store []byte, off int, p []byte) int {
	if off+len(p) > len(store) {
		off = 0
	}
	return off + copy(store[off:], p)
}

// replica stores each write and acks it.
type replica struct {
	store []byte
	off   int
	lost  int
}

func (r *replica) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	switch ev.Kind {
	case core.EvPeerLost:
		r.lost++
	case core.EvNetRecv:
		r.off = storeAt(r.store, r.off, ctx.Pool.Bytes(ev.Msg.Buf)[:lgPayload])
		ctx.Pool.Free(ev.Msg.Buf)
		a := core.Action{Kind: core.ActNetSend, Peer: ev.Peer}
		a.Msg.Len = uint16(copy(a.Msg.Body[:], ev.Msg.Inline()))
		out = append(out, a)
	}
	return out
}

func realLoop(t *testing.T, id core.NodeID, poolBytes int, m loop.Machine, d loop.Driver, dial func(core.NodeID) (int, error)) (*loop.Loop, *Driver, int) {
	t.Helper()
	k, err := loop.NewEpoll(loop.VMClockPath)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := loop.NewPool(poolBytes, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	td := New(Config{Self: id, Seed: uint64(id.Node), Dial: dial})
	if d == nil {
		d = td
	}
	l := loop.New(loop.Config{Node: id.Node, Pool: pool}, k, m, d)
	td.L = l
	lfd, port, err := ListenTCP(netip.MustParseAddrPort("127.0.0.1:0"))
	if err != nil {
		t.Fatal(err)
	}
	if err := td.Listen(lfd); err != nil {
		t.Fatal(err)
	}
	return l, td, port
}

// TestTheLoadGeneratorDropsNothing runs the open-loop latency spike's load generator, at
// RS_LOADGEN, against a primary and two replicas, each an event loop with this driver, at each
// rate of RS_LOAD_RATES, and fails if it drops a request or a connection is lost.
func TestTheLoadGeneratorDropsNothing(t *testing.T) {
	bin := os.Getenv("RS_LOADGEN")
	if bin == "" {
		t.Skip("RS_LOADGEN names no load generator")
	}
	rates := strings.Split(cmp.Or(os.Getenv("RS_LOAD_RATES"), "50000,100000"), ",")
	secs := cmp.Or(os.Getenv("RS_LOAD_SECS"), "10")
	var ports [2]int
	dial := func(p core.NodeID) (int, error) {
		return DialTCP(netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(ports[p.Node-2])))
	}
	var reps [2]*replica
	var loops []*loop.Loop
	for i := range 2 {
		reps[i] = &replica{store: make([]byte, 64<<20)}
		l, _, port := realLoop(t, core.NodeID{Node: uint32(i + 2)}, 256<<20, reps[i], nil, dial)
		ports[i] = port
		loops = append(loops, l)
	}
	p := &primary{replicas: [2]core.NodeID{{Node: 2}, {Node: 3}}, store: make([]byte, 64<<20)}
	p.waits = make([]pwait, seqRing)
	f := &front{clients: map[loop.ConnID]*lgConn{}}
	l, td, _ := realLoop(t, core.NodeID{Node: 1}, 512<<20, p, f, dial)
	f.Driver = td
	cfd, cport, err := ListenTCP(netip.MustParseAddrPort("127.0.0.1:0"))
	if err != nil {
		t.Fatal(err)
	}
	if f.ln, err = l.AddListener(cfd); err != nil {
		t.Fatal(err)
	}
	*td.conn(f.ln) = conn{open: true, listen: true, fd: cfd, peer: -1}
	loops = append(loops, l)
	done := make(chan struct{}, len(loops))
	for _, l := range loops {
		go func() { l.Run(); done <- struct{}{} }()
	}
	defer func() {
		for _, l := range loops {
			l.Stop()
			<-done
		}
	}()
	for _, rate := range rates {
		cmd := exec.Command(bin, fmt.Sprintf("port=%d", cport), fmt.Sprintf("pid=%d", os.Getpid()), "rate="+rate, "secs="+secs, "warm=1", "conns=16", "label=go-tcp")
		outb, err := cmd.CombinedOutput()
		line := strings.TrimSpace(string(outb))
		t.Log(line)
		if err != nil {
			t.Fatalf("loadgen at %s: %v", rate, err)
		}
		if !strings.Contains(line, " dropped=0 ") || !strings.Contains(line, " unanswered=0 ") {
			t.Errorf("at %s writes/s the load generator dropped requests", rate)
		}
	}
	for _, l := range loops {
		l.Stop()
		<-done
	}
	loops = nil
	t.Logf("peers lost: primary %d, replicas %d %d; bad CRCs %d; primary's most queued %d bytes", p.lost, reps[0].lost, reps[1].lost, f.bad, f.maxQ)
	if p.lost+reps[0].lost+reps[1].lost != 0 || f.bad != 0 {
		t.Error("a connection was lost or a payload corrupt")
	}
}
