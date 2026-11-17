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
