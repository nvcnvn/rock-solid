package tcp

import (
	"bytes"
	"testing"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/loop"
)

// connTo is nd's connection to peer and its descriptor.
func (nd *node) connTo(t *testing.T, peer core.NodeID) (loop.ConnID, int) {
	t.Helper()
	i, ok := nd.d.index[peer]
	if !ok || !nd.d.peers[i].up {
		t.Fatalf("%v holds no connection to %v", nd.id, peer)
	}
	c := nd.d.peers[i].conn
	return c, nd.d.conns[c.Slot()].fd
}

// connected makes a and b exchange a message, so both hold their connection.
func connected(t *testing.T, r *rig, a, b *node) {
	t.Helper()
	a.send(b.id, "hi", nil, true)
	a.fire()
	r.step(20)
	if len(b.m.recvs) != 1 || a.links() != 1 || b.links() != 1 {
		t.Fatalf("b got %d messages; links %d and %d; lost %v, dials %d", len(b.m.recvs), a.links(), b.links(), a.m.lost, a.dials)
	}
}

// resetClose closes fd with a reset in place of a FIN.
func resetClose(t *testing.T, fd int) {
	t.Helper()
	if err := unix.SetsockoptLinger(fd, unix.SOL_SOCKET, unix.SO_LINGER, &unix.Linger{Onoff: 1, Linger: 0}); err != nil {
		t.Fatal(err)
	}
}

// rawPeer is a blocking loopback socket to port that names itself id in a hello, with a receive
// buffer of rcvbuf bytes unless 0; the caller closes it.
func rawPeer(t testing.TB, port int, id core.NodeID, rcvbuf int) int {
	t.Helper()
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_NODELAY, 1); err != nil {
		t.Fatal(err)
	}
	if rcvbuf > 0 {
		if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_RCVBUF, rcvbuf); err != nil {
			t.Fatal(err)
		}
	}
	if err := unix.Connect(fd, &unix.SockaddrInet4{Port: port, Addr: [4]byte{127, 0, 0, 1}}); err != nil {
		t.Fatal(err)
	}
	if _, err := unix.Write(fd, encode(kindHello, putHello(make([]byte, helloLen), id, 1), nil)); err != nil {
		t.Fatal(err)
	}
	return fd
}

func TestAResetIsOnePeerLostThenReconnects(t *testing.T) {
	r := newRigOn(t, 2, 4<<20, true)
	a, b := r.nodes[0], r.nodes[1]
	connected(t, r, a, b)
	c, fd := b.connTo(t, a.id)
	resetClose(t, fd)
	b.l.Close(c)
	b.d.Closed(c)
	r.step(20)
	if len(a.m.lost) != 1 || a.m.lost[0] != b.id || len(b.m.lost) != 1 {
		t.Fatalf("lost at a %v, at b %v; want one each", a.m.lost, b.m.lost)
	}
	a.send(b.id, "two", nil, true)
	a.fire()
	r.step(20)
	if got := b.m.recvs[len(b.m.recvs)-1]; string(got.Msg.Inline()) != "two" || a.dials != 2 || len(a.m.lost) != 1 {
		t.Fatalf("after reconnecting: %q, dials %d, lost %v", got.Msg.Inline(), a.dials, a.m.lost)
	}
}

func TestAnEOFIsOnePeerLostThenReconnects(t *testing.T) {
	r := newRigOn(t, 2, 4<<20, true)
	a, b := r.nodes[0], r.nodes[1]
	connected(t, r, a, b)
	_, fd := b.connTo(t, a.id)
	if err := unix.Shutdown(fd, unix.SHUT_WR); err != nil {
		t.Fatal(err)
	}
	r.step(20)
	if len(a.m.lost) != 1 || a.m.lost[0] != b.id || len(b.m.lost) != 1 {
		t.Fatalf("lost at a %v, at b %v; want one each", a.m.lost, b.m.lost)
	}
	a.send(b.id, "two", nil, true)
	a.fire()
	r.step(20)
	if got := b.m.recvs[len(b.m.recvs)-1]; string(got.Msg.Inline()) != "two" || len(a.m.lost) != 1 {
		t.Fatalf("after reconnecting: %q, lost %v", got.Msg.Inline(), a.m.lost)
	}
}

func TestAPartialFrameAtAResetIsNeverDelivered(t *testing.T) {
	r := newRigOn(t, 1, 4<<20, true)
	b := r.nodes[0]
	x := core.NodeID{Node: 9, Loop: 1}
	fd := rawPeer(t, b.port, x, 0)
	whole := encode(kindMsg, []byte("abc"), bytes.Repeat([]byte{5}, 64<<10))
	if _, err := unix.Write(fd, whole[:headerLen+3+1000]); err != nil {
		t.Fatal(err)
	}
	r.step(5)
	if c, _ := b.connTo(t, x); b.d.conns[c].dec.got != 1000 {
		t.Fatalf("b read %d bytes of the data, want 1000", b.d.conns[c].dec.got)
	}
	resetClose(t, fd)
	_ = unix.Close(fd)
	r.step(5)
	if len(b.m.recvs) != 0 || len(b.m.lost) != 1 || b.m.lost[0] != x || b.pool.Live() != 0 {
		t.Fatalf("received %d, lost %v, %d buffers live", len(b.m.recvs), b.m.lost, b.pool.Live())
	}
}

func TestAPeerThatStopsReadingIsOnePeerLost(t *testing.T) {
	r := newRigOn(t, 1, 128<<20, true)
	b := r.nodes[0]
	x := core.NodeID{Node: 9, Loop: 1}
	fd := rawPeer(t, b.port, x, 4096)
	defer unix.Close(fd)
	r.step(5)
	big := make([]byte, 600<<10)
	for round := 0; len(b.m.lost) == 0; round++ {
		if round == 40 {
			t.Fatalf("no PeerLost after %d sends; %d bytes queued", round*8, b.l.Queued())
		}
		for range 8 {
			b.send(x, "w", big, true)
		}
		b.fire()
		r.step(1)
	}
	r.step(2)
	if len(b.m.lost) != 1 || b.m.lost[0] != x || b.pool.Live() != 0 || b.l.Queued() != 0 {
		t.Fatalf("lost %v, %d buffers live, %d bytes queued", b.m.lost, b.pool.Live(), b.l.Queued())
	}
}

func TestDataIsReadStraightIntoItsAlignedBuffer(t *testing.T) {
	r := newRigOn(t, 2, 16<<20, true)
	a, b := r.nodes[0], r.nodes[1]
	b.m.keep = true
	connected(t, r, a, b)
	data := bytes.Repeat([]byte{7}, 4<<20)
	a.send(b.id, "w", data, true)
	a.fire()
	c, _ := b.connTo(t, a.id)
	into := core.NoBuf
	for range 200 {
		r.step(1)
		if dec := &b.d.conns[c].dec; dec.got > 0 && dec.got < dec.dlen {
			into = dec.data
			// The decoder's next read goes straight into the data buffer, the only way data enters it.
			if v, _ := dec.space(b.pool, nil); &v[0][0] != &b.pool.Bytes(into)[dec.got] {
				t.Fatal("the next read does not go into the data buffer")
			}
		}
		if len(b.m.recvs) == 2 {
			break
		}
	}
	if len(b.m.recvs) != 2 || into == core.NoBuf {
		t.Fatalf("b got %d messages; caught the data mid-read: %v", len(b.m.recvs), into != core.NoBuf)
	}
	got := b.m.recvs[1].Msg.Buf
	mem := b.pool.Bytes(got)
	if got != into || uintptr(unsafe.Pointer(&mem[0]))%core.BlockSize != 0 || !bytes.Equal(mem[:len(data)], data) {
		t.Fatalf("delivered %v, read into %v, at %p", got, into, &mem[0])
	}
	b.pool.Free(got)
}

// sendAction is a NetSend to peer carrying inline.
func sendAction(peer core.NodeID, inline string) core.Action {
	a := core.Action{Kind: core.ActNetSend, Peer: peer}
	if err := a.Msg.SetInline([]byte(inline)); err != nil {
		panic(err)
	}
	return a
}

// dropAll closes every connection nd holds, as its process going away would.
func (nd *node) dropAll() {
	for _, cs := range nd.d.conns {
		if cs.open && !cs.listen {
			nd.l.Close(cs.id)
			nd.d.Closed(cs.id)
		}
	}
}

func TestReadAheadIsNeverDeliveredAsTheNextConnectionsPeer(t *testing.T) {
	r := newRig(t, 3, 64<<20)
	a, b, c := r.nodes[0], r.nodes[1], r.nodes[2]
	connected(t, r, a, b)
	for _, s := range []string{"m0", "m1", "m2"} {
		b.send(a.id, s, nil, false)
	}
	big, err := a.pool.Alloc(MaxData + 1)
	if err != nil {
		t.Fatal(err)
	}
	defer a.pool.Free(big)
	done := false
	// On m0, a's send of an oversized frame closes the connection to b, and its send to c dials
	// a connection that takes the same slot while m1 and m2 wait in the read-ahead.
	a.m.hook = func(ctx *core.Context, ev *core.Event, out []core.Action) ([]core.Action, bool) {
		if ev.Kind != core.EvNetRecv || string(ev.Msg.Inline()) != "m0" || done {
			return out, false
		}
		done = true
		a.m.recvs = append(a.m.recvs, *ev)
		s := sendAction(b.id, "")
		s.Msg.Buf = big
		return append(out, s, sendAction(c.id, "z")), true
	}
	b.fire()
	r.step(10)
	for _, ev := range a.m.recvs {
		if ev.Peer != b.id {
			t.Errorf("a delivered %q as from %v", ev.Msg.Inline(), ev.Peer)
		}
	}
	if len(c.m.recvs) != 1 || c.m.recvs[0].Peer != a.id {
		t.Fatalf("c got %+v", c.m.recvs)
	}
}

func TestTwoWriteFailuresInAPassAreEachTheirOwnPeerLost(t *testing.T) {
	r := newRig(t, 5, 1<<20)
	a, b, c, d, e := r.nodes[0], r.nodes[1], r.nodes[2], r.nodes[3], r.nodes[4]
	a.send(b.id, "hi", nil, false)
	a.send(c.id, "hi", nil, false)
	a.fire()
	r.step(10)
	if a.links() != 2 {
		t.Fatalf("setup: a holds %d links", a.links())
	}
	fired := false
	// b and c go away inside a's pass, before a writes to both; b's PeerLost handler then dials d
	// and e, while c's failure is not yet reported to the driver.
	a.m.hook = func(ctx *core.Context, ev *core.Event, out []core.Action) ([]core.Action, bool) {
		switch {
		case ev.Kind == core.EvTimer && ev.Timer == goTimer && !fired:
			fired = true
			b.dropAll()
			c.dropAll()
			return append(out, sendAction(b.id, "x"), sendAction(c.id, "x")), true
		case ev.Kind == core.EvPeerLost && ev.Peer == b.id:
			a.m.lost = append(a.m.lost, ev.Peer)
			return append(out, sendAction(d.id, "to d"), sendAction(e.id, "to e")), true
		}
		return out, false
	}
	a.fire()
	r.step(10)
	if len(a.m.lost) != 2 || a.m.lost[0] != b.id || a.m.lost[1] != c.id {
		t.Fatalf("a lost %v, want b then c", a.m.lost)
	}
	if len(d.m.recvs) != 1 || len(e.m.recvs) != 1 || a.links() != 2 {
		t.Fatalf("d got %d, e got %d, a holds %d links", len(d.m.recvs), len(e.m.recvs), a.links())
	}
	a.send(c.id, "again", nil, false)
	a.fire()
	r.step(10)
	if got := c.m.recvs[len(c.m.recvs)-1]; string(got.Msg.Inline()) != "again" || len(e.m.recvs) != 1 {
		t.Fatalf("c's last message %q, e got %d", got.Msg.Inline(), len(e.m.recvs))
	}
}

func TestAPeerLostFromAWriteFailureIsDeliveredWithoutBlocking(t *testing.T) {
	r := newRig(t, 2, 1<<20)
	a, b := r.nodes[0], r.nodes[1]
	connected(t, r, a, b)
	gone := core.NodeID{Node: 9}
	fired := false
	a.m.hook = func(ctx *core.Context, ev *core.Event, out []core.Action) ([]core.Action, bool) {
		switch {
		case ev.Kind == core.EvTimer && ev.Timer == goTimer && !fired:
			fired = true
			b.dropAll()
			return append(out, sendAction(b.id, "x")), true
		case ev.Kind == core.EvPeerLost && ev.Peer == b.id:
			a.m.lost = append(a.m.lost, ev.Peer)
			return append(out, sendAction(gone, "y")), true
		}
		return out, false
	}
	a.fire()
	a.l.Pass()
	if len(a.m.lost) != 1 || !a.l.Pending().Work {
		t.Fatalf("lost %v; the loop would block holding %d deferred PeerLost", a.m.lost, len(a.d.lost))
	}
	a.l.Pass()
	if len(a.m.lost) != 2 || a.m.lost[1] != gone {
		t.Fatalf("lost %v", a.m.lost)
	}
}

func TestAMessageBeforeTheHelloOnADialIsRefused(t *testing.T) {
	r := newRig(t, 1, 1<<20)
	a := r.nodes[0]
	x := core.NodeID{Node: 9}
	path := a.path + "x"
	lfd, err := listenUnix(path)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(lfd)
	a.d.cfg.Dial = func(core.NodeID) (int, error) { return dialUnix(path) }
	a.send(x, "hi", nil, false)
	a.fire()
	r.step(2)
	fd, _, err := unix.Accept(lfd)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	if _, err := unix.Write(fd, encode(kindMsg, []byte("early"), nil)); err != nil {
		t.Fatal(err)
	}
	r.step(2)
	if len(a.m.recvs) != 0 || len(a.m.lost) != 1 || a.m.lost[0] != x || a.links() != 0 {
		t.Fatalf("received %d, lost %v, links %d", len(a.m.recvs), a.m.lost, a.links())
	}
}

func TestAConnectionQueuesAtMost32MiBAndOneFrame(t *testing.T) {
	r := newRigOn(t, 1, 128<<20, true)
	b := r.nodes[0]
	x := core.NodeID{Node: 9, Loop: 1}
	fd := rawPeer(t, b.port, x, 4096)
	defer unix.Close(fd)
	r.step(5)
	big := make([]byte, MaxData)
	for range 3 {
		b.send(x, "w", big, true)
	}
	b.fire()
	r.step(1)
	if len(b.m.lost) != 1 || b.m.lost[0] != x {
		t.Fatalf("lost %v with %d bytes queued", b.m.lost, b.l.Queued())
	}
	r.step(2)
	if b.pool.Live() != 0 || b.l.Queued() != 0 {
		t.Fatalf("%d buffers live, %d bytes queued", b.pool.Live(), b.l.Queued())
	}
}

func TestAnAcceptErrorThatLastsPausesTheListener(t *testing.T) {
	r := newRig(t, 2, 1<<20)
	a, b := r.nodes[0], r.nodes[1]
	calls := 0
	accept = func(int) (int, unix.Sockaddr, error) {
		calls++
		return -1, nil, unix.EMFILE
	}
	defer func() { accept = unix.Accept }()
	b.send(a.id, "hi", nil, false)
	b.fire()
	r.step(10)
	if calls != 1 || a.l.Pending().Deadline == 0 {
		t.Fatalf("%d accepts in 10 passes, pending %+v", calls, a.l.Pending())
	}
	accept = unix.Accept
	r.s.Advance(a.l.Pending().Deadline)
	r.step(5)
	if len(a.m.recvs) != 1 || a.m.recvs[0].Peer != b.id {
		t.Fatalf("a got %+v", a.m.recvs)
	}
}

func TestASendToTheLoopItselfPanics(t *testing.T) {
	r := newRig(t, 1, 1<<20)
	a := r.nodes[0]
	defer func() {
		if recover() == nil || a.dials != 0 {
			t.Fatalf("no panic, %d dials", a.dials)
		}
	}()
	act := sendAction(a.id, "self")
	a.d.Execute(&act)
}
