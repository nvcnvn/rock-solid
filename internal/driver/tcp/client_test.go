package tcp

import (
	"bytes"
	"net/netip"
	"testing"
	"time"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// A blocking Conn names itself with a hello and trades messages, inline bytes and data, with a
// loop's driver both ways.
func TestAConnTradesMessagesWithALoop(t *testing.T) {
	r := newRigOn(t, 1, 1<<20, true)
	a := r.nodes[0]
	x := core.NodeID{Node: 9}
	c, err := Dial(netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(a.port)), x, a.pool, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	data := bytes.Repeat([]byte{7}, core.BlockSize)
	var m core.Msg
	if err := m.SetInline([]byte("ask")); err != nil {
		t.Fatal(err)
	}
	if m.Buf, err = a.pool.Alloc(len(data)); err != nil {
		t.Fatal(err)
	}
	copy(a.pool.Bytes(m.Buf), data)
	if err := c.Send(&m); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100 && len(a.m.recvs) == 0; i++ {
		r.step(1)
		time.Sleep(time.Millisecond)
	}
	if len(a.m.recvs) != 1 || a.m.recvs[0].Peer != x || string(a.m.recvs[0].Msg.Inline()) != "ask" || !bytes.Equal(a.m.data[0], data) {
		t.Fatalf("the loop got %+v", a.m.recvs)
	}
	a.send(x, "answer", []byte{1, 2, 3}, true)
	a.fire()
	r.step(3)
	if err := c.Recv(time.Now().Add(5*time.Second), &m); err != nil {
		t.Fatal(err)
	}
	if string(m.Inline()) != "answer" || m.Buf == core.NoBuf || !bytes.Equal(a.pool.Bytes(m.Buf)[:3], []byte{1, 2, 3}) {
		t.Fatalf("the Conn got %q", m.Inline())
	}
	a.pool.Free(m.Buf)
}

// A loop forgets a peer that dialed in once its connection closes, if the loop never dialed it:
// each run of the operator's command line connects under a fresh node ID. A peer the loop dialed
// is kept, and its connection still names it after it moves into the forgotten peer's place.
func TestAPeerThatDialedInIsForgottenOnceItCloses(t *testing.T) {
	r := newRigOn(t, 2, 1<<20, true)
	a, b := r.nodes[0], r.nodes[1]
	x := core.NodeID{Node: 9}
	c, err := Dial(netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(a.port)), x, a.pool, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var m core.Msg
	if err := m.SetInline([]byte("ask")); err != nil {
		t.Fatal(err)
	}
	if err := c.Send(&m); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100 && len(a.m.recvs) == 0; i++ {
		r.step(1)
		time.Sleep(time.Millisecond)
	}
	a.send(b.id, "one", nil, true)
	a.fire()
	r.step(20)
	if len(a.d.peers) != 2 || a.d.peers[0].id != x {
		t.Fatalf("peers %+v", a.d.peers)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100 && len(a.m.lost) == 0; i++ {
		r.step(1)
		time.Sleep(time.Millisecond)
	}
	if len(a.d.peers) != 1 || len(a.d.index) != 1 || len(a.m.lost) != 1 || a.m.lost[0] != x {
		t.Fatalf("after %v closed: peers %+v, lost %v", x, a.d.peers, a.m.lost)
	}
	b.send(a.id, "back", nil, true)
	b.fire()
	r.step(20)
	if got := a.m.recvs[len(a.m.recvs)-1]; got.Peer != b.id || string(got.Msg.Inline()) != "back" {
		t.Fatalf("the loop got %q from %v", got.Msg.Inline(), got.Peer)
	}
	a.send(b.id, "two", nil, true)
	a.fire()
	r.step(20)
	if got := b.m.recvs[len(b.m.recvs)-1]; string(got.Msg.Inline()) != "two" || a.dials != 1 {
		t.Fatalf("the dialed peer got %q after %d dials", got.Msg.Inline(), a.dials)
	}
}

// A send that ends a dialed-in peer's connection, as one too large does, queues its PeerLost for
// the pass's end, and the peer is forgotten with it.
func TestAPeerThatDialedInIsForgottenWhenASendToItFails(t *testing.T) {
	r := newRigOn(t, 1, 32<<20, true)
	a := r.nodes[0]
	x := core.NodeID{Node: 9}
	c, err := Dial(netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(a.port)), x, a.pool, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	var m core.Msg
	if err := m.SetInline([]byte("ask")); err != nil {
		t.Fatal(err)
	}
	if err := c.Send(&m); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100 && len(a.m.recvs) == 0; i++ {
		r.step(1)
		time.Sleep(time.Millisecond)
	}
	if len(a.d.peers) != 1 {
		t.Fatalf("peers %+v", a.d.peers)
	}
	a.send(x, "big", make([]byte, MaxData+1), true)
	a.fire()
	r.step(3)
	if len(a.m.lost) != 1 || a.m.lost[0] != x || len(a.d.peers) != 0 || len(a.d.index) != 0 {
		t.Fatalf("after the failed send: lost %v, peers %+v", a.m.lost, a.d.peers)
	}
}

// A dialed-in peer whose hello the loop cannot answer, its pool full, is refused and not kept.
func TestAPeerWhoseHelloCannotBeAnsweredIsNotKept(t *testing.T) {
	r := newRigOn(t, 1, 1<<20, true)
	a := r.nodes[0]
	var held []core.IOBuf
	for {
		b, err := a.pool.Alloc(core.BlockSize)
		if err != nil {
			break
		}
		held = append(held, b)
	}
	defer func() {
		for _, b := range held {
			a.pool.Free(b)
		}
	}()
	c, err := Dial(netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(a.port)), core.NodeID{Node: 9}, a.pool, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	for i := 0; i < 100 && a.links() == 0; i++ {
		r.step(1)
		time.Sleep(time.Millisecond)
	}
	for i := 0; i < 100 && a.links() != 0; i++ {
		r.step(1)
		time.Sleep(time.Millisecond)
	}
	if a.links() != 0 || len(a.d.peers) != 0 || len(a.d.index) != 0 || len(a.m.lost) != 0 {
		t.Fatalf("links %d, peers %+v, lost %v", a.links(), a.d.peers, a.m.lost)
	}
}
