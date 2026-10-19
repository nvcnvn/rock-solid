// Checks what go-fuse v2.12.0 does with a /dev/fuse descriptor that another process already
// initialized, and what its transport-free ProtocolServer costs per request.
//
//	gofuse adopt <holder-sock>   take the fd from the hofs holder, call fuse.NewServer("/dev/fd/N")
//	gofuse codec                 ProtocolServer: replayed INIT, unique echo, allocations per GETATTR
package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
)

type rootFS struct{ fuse.RawFileSystem }

func (rootFS) GetAttr(cancel <-chan struct{}, in *fuse.GetAttrIn, out *fuse.AttrOut) fuse.Status {
	out.Mode = syscall.S_IFDIR | 0o755
	out.Ino = 1
	out.Nlink = 2
	return fuse.OK
}

func ev(format string, args ...any) {
	fmt.Printf("EV %s\n", fmt.Sprintf(format, args...))
}

func getFromHolder(sock string) int {
	c, err := syscall.Socket(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		panic(err)
	}
	if err := syscall.Connect(c, &syscall.SockaddrUnix{Name: sock}); err != nil {
		panic(err)
	}
	if _, err := syscall.Write(c, []byte("G")); err != nil {
		panic(err)
	}
	state := make([]byte, 144)
	oob := make([]byte, syscall.CmsgSpace(8*4))
	n, oobn, _, _, err := syscall.Recvmsg(c, state, oob, syscall.MSG_WAITALL)
	if err != nil || n != len(state) {
		panic(fmt.Sprintf("recvmsg n=%d err=%v", n, err))
	}
	msgs, err := syscall.ParseSocketControlMessage(oob[:oobn])
	if err != nil {
		panic(err)
	}
	fds, err := syscall.ParseUnixRights(&msgs[0])
	if err != nil {
		panic(err)
	}
	syscall.Close(c)
	return fds[0]
}

func adopt(sock string) {
	fd := getFromHolder(sock)
	ev("GOT_FD fd=%d", fd)
	// The holder and the previous daemon share this open file description, so its O_NONBLOCK flag
	// is whatever the last process set; go-fuse's reads expect a blocking descriptor.
	if err := syscall.SetNonblock(fd, false); err != nil {
		panic(err)
	}
	done := make(chan error, 1)
	var srv *fuse.Server
	go func() {
		var err error
		srv, err = fuse.NewServer(rootFS{fuse.NewDefaultRawFileSystem()}, fmt.Sprintf("/dev/fd/%d", fd),
			&fuse.MountOptions{Debug: true})
		done <- err
	}()
	select {
	case err := <-done:
		ev("NEWSERVER_RETURNED err=%v", err)
	case <-time.After(2 * time.Second):
		ev("NEWSERVER_BLOCKED_2S")
		err := <-done
		ev("NEWSERVER_RETURNED err=%v", err)
	}
	if srv != nil {
		ev("KERNEL_SETTINGS major=%d minor=%d", srv.KernelSettings().Major, srv.KernelSettings().Minor)
		go srv.Serve()
	}
	time.Sleep(time.Hour)
}

func le(vs ...any) []byte {
	var b bytes.Buffer
	for _, v := range vs {
		if err := binary.Write(&b, binary.LittleEndian, v); err != nil {
			panic(err)
		}
	}
	return b.Bytes()
}

// request encodes a request whose go-fuse input struct embeds InHeader as its first field.
func request(op uint32, unique, node uint64, in any) []byte {
	body := le(in)[40:]
	h := fuse.InHeader{Length: uint32(40 + len(body)), Opcode: op, Unique: unique, NodeId: node}
	return append(le(h), body...)
}

func codec() {
	ps := fuse.NewProtocolServer(rootFS{fuse.NewDefaultRawFileSystem()}, &fuse.MountOptions{})
	out := make([]byte, 8192)
	init := fuse.InitIn{Major: 7, Minor: 45, MaxReadAhead: 131072}
	n, st := ps.HandleRequest([][]byte{request(26, 1, 0, init)}, [][]byte{out})
	ev("INIT_REPLAYED n=%d status=%v", n, st)
	resent := uint64(1)<<63 | 0x14a
	getattr := request(3, resent, 1, fuse.GetAttrIn{})
	n, st = ps.HandleRequest([][]byte{getattr}, [][]byte{out})
	echoed := binary.LittleEndian.Uint64(out[8:16])
	ev("GETATTR n=%d status=%v unique_in=%#x unique_out=%#x full_echo=%v", n, st, resent, echoed, echoed == resent)
	allocs := testing.AllocsPerRun(20000, func() {
		ps.HandleRequest([][]byte{getattr}, [][]byte{out})
	})
	r := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			ps.HandleRequest([][]byte{getattr}, [][]byte{out})
		}
	})
	ev("GETATTR_ALLOCS allocs_per_op=%.1f bench=%s %s", allocs, r.String(), r.MemString())
}

func main() {
	switch os.Args[1] {
	case "adopt":
		adopt(os.Args[2])
	case "codec":
		codec()
	}
}
