package tcp

import (
	"bytes"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// sendOne is a loop sending one message of a block to a raw loopback peer per call, which drains it.
func sendOne(tb testing.TB) func() {
	r := newRigOn(tb, 1, 4<<20, true)
	nd := r.nodes[0]
	x := core.NodeID{Node: 9, Loop: 1}
	fd := rawPeer(tb, nd.port, x, 0)
	tb.Cleanup(func() { _ = unix.Close(fd) })
	if err := unix.SetNonblock(fd, true); err != nil {
		tb.Fatal(err)
	}
	r.step(5)
	block := bytes.Repeat([]byte{7}, core.BlockSize)
	drain := make([]byte, 256<<10)
	return func() {
		nd.send(x, "write", block, true)
		nd.fire()
		nd.l.Pass()
		for {
			if n, err := unix.Read(fd, drain); n <= 0 || err != nil {
				break
			}
		}
		if len(nd.m.lost) != 0 {
			tb.Fatalf("lost %v", nd.m.lost)
		}
	}
}

// receiveOne is a loop receiving one message of a block from a raw loopback peer per call.
func receiveOne(tb testing.TB) func() {
	r := newRigOn(tb, 1, 4<<20, true)
	nd := r.nodes[0]
	nd.m.discard = true
	fd := rawPeer(tb, nd.port, core.NodeID{Node: 9, Loop: 1}, 0)
	tb.Cleanup(func() { _ = unix.Close(fd) })
	r.step(5)
	msg := encode(kindMsg, []byte("write"), bytes.Repeat([]byte{7}, core.BlockSize))
	return func() {
		want := nd.m.got + 1
		if _, err := unix.Write(fd, msg); err != nil {
			tb.Fatal(err)
		}
		for range 10 {
			if nd.l.Pass(); nd.m.got == want {
				return
			}
		}
		tb.Fatalf("received %d, want %d", nd.m.got, want)
	}
}

func TestSendingAndReceivingAllocateNothing(t *testing.T) {
	for name, op := range map[string]func(testing.TB) func(){"send": sendOne, "receive": receiveOne} {
		if n := testing.AllocsPerRun(200, op(t)); n != 0 {
			t.Errorf("%s: %v allocations a message", name, n)
		}
	}
}

func BenchmarkSend(b *testing.B) {
	op := sendOne(b)
	b.ReportAllocs()
	b.SetBytes(core.BlockSize)
	for b.Loop() {
		op()
	}
}

func BenchmarkReceive(b *testing.B) {
	op := receiveOne(b)
	b.ReportAllocs()
	b.SetBytes(core.BlockSize)
	for b.Loop() {
		op()
	}
}
