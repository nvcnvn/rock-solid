// Microbenchmarks of the pieces of replicate-go3's write path, each the same operation as in
// micro-rust: syscalls with and without the runtime's entersyscall/exitsyscall, epoll_wait,
// time.Now, frame parsing, header encoding, CRC32C, the 4 KiB copies, the map of pending requests, and
// indexed loops. The parsing, header and ring code is copied from replicate-go3. It sticks to Go
// 1.18, so gccgo 14 (Go 1.18 library, no generics) builds the same source with GCC's backend;
// appendUint32 and appendUint64 are encoding/binary's Go 1.19 AppendUint32 and AppendUint64.
//
// usage: micro-go [filter]   (runs the benchmarks whose name contains filter)
package main

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"net"
	"os"
	"sort"
	"strings"
	"syscall"
	"time"
	"unsafe"
)

const (
	hdrLen    = 24
	payload   = 4096
	kindWrite = 1
	kindRepl  = 2
	kindAck   = 3
	reps      = 7
)

var sink uint64

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func appendUint32(b []byte, v uint32) []byte {
	return append(b, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
}

func appendUint64(b []byte, v uint64) []byte {
	return append(b, byte(v), byte(v>>8), byte(v>>16), byte(v>>24), byte(v>>32), byte(v>>40), byte(v>>48),
		byte(v>>56))
}

func header(b []byte, kind byte, n int, id uint64, crc uint32) []byte {
	b = append(b, kind, 0, 0, 0)
	b = appendUint32(b, uint32(n))
	b = appendUint64(b, id)
	b = appendUint32(b, crc)
	return append(b, 0, 0, 0, 0)
}

type ring struct {
	mem []byte
	off int
}

func (r *ring) store(p []byte) {
	if r.off+len(p) > len(r.mem) {
		r.off = 0
	}
	copy(r.mem[r.off:], p)
	r.off += len(p)
}

type conn struct {
	rbuf []byte
	rlen int
	wbuf []byte
}

type frame struct {
	kind       byte
	id         uint64
	crc        uint32
	start, len int
}

//go:noinline
func parse(c *conn, frames []frame) []frame {
	for pos := 0; c.rlen-pos >= hdrLen; {
		h := c.rbuf[pos : pos+hdrLen]
		n := int(binary.LittleEndian.Uint32(h[4:]))
		if c.rlen-pos < hdrLen+n {
			break
		}
		frames = append(frames, frame{kind: h[0], id: binary.LittleEndian.Uint64(h[8:]),
			crc: binary.LittleEndian.Uint32(h[16:]), start: pos + hdrLen, len: n})
		pos += hdrLen + n
	}
	return frames
}

//go:noinline
func parseBCE(c *conn, frames []frame) []frame {
	buf := c.rbuf[:c.rlen]
	for len(buf) >= hdrLen {
		h := buf[:hdrLen]
		n := int(binary.LittleEndian.Uint32(h[4:8]))
		if len(buf)-hdrLen < n {
			break
		}
		frames = append(frames, frame{kind: h[0], id: binary.LittleEndian.Uint64(h[8:16]),
			crc: binary.LittleEndian.Uint32(h[16:20]), start: c.rlen - len(buf) + hdrLen, len: n})
		buf = buf[hdrLen+n:]
	}
	return frames
}

//go:noinline
func parseInPlace(c *conn, frames []frame) []frame {
	for pos := 0; c.rlen-pos >= hdrLen; {
		h := c.rbuf[pos : pos+hdrLen]
		n := int(binary.LittleEndian.Uint32(h[4:]))
		if c.rlen-pos < hdrLen+n {
			break
		}
		frames = append(frames, frame{})
		f := &frames[len(frames)-1]
		f.kind, f.id, f.crc = h[0], binary.LittleEndian.Uint64(h[8:]), binary.LittleEndian.Uint32(h[16:])
		f.start, f.len = pos+hdrLen, n
		pos += hdrLen + n
	}
	return frames
}

func fill(c *conn, frames, size int) {
	c.rlen = 0
	c.rbuf = make([]byte, frames*(hdrLen+size))
	b := c.rbuf[:0]
	for i := 0; i < frames; i++ {
		b = header(b, kindRepl, size, uint64(i), 0)
		b = append(b, make([]byte, size)...)
	}
	c.rlen = len(b)
}

type pend struct {
	tok  int
	cid  uint64
	acks uint8
}

func tcpPair() (int, int) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	must(err)
	a, err := net.Dial("tcp", ln.Addr().String())
	must(err)
	b, err := ln.Accept()
	must(err)
	fd := func(c net.Conn) int {
		f, err := c.(*net.TCPConn).File()
		must(err)
		d, err := syscall.Dup(int(f.Fd()))
		must(err)
		must(f.Close())
		must(c.Close())
		must(syscall.SetNonblock(d, true))
		must(syscall.SetsockoptInt(d, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1))
		return d
	}
	must(ln.Close())
	return fd(a), fd(b)
}

func rawRead(fd int, p []byte) (int, syscall.Errno) {
	n, _, e := syscall.RawSyscall(syscall.SYS_READ, uintptr(fd), uintptr(unsafe.Pointer(&p[0])), uintptr(len(p)))
	return int(n), e
}

func rawWrite(fd int, p []byte) (int, syscall.Errno) {
	n, _, e := syscall.RawSyscall(syscall.SYS_WRITE, uintptr(fd), uintptr(unsafe.Pointer(&p[0])), uintptr(len(p)))
	return int(n), e
}

func rawEpollWait(ep int, evs []syscall.EpollEvent) int {
	n, _, _ := syscall.RawSyscall6(syscall.SYS_EPOLL_WAIT, uintptr(ep), uintptr(unsafe.Pointer(&evs[0])),
		uintptr(len(evs)), 0, 0, 0)
	return int(n)
}

type bench struct {
	name  string
	iters int
	run   func(n int)
}

func benches() []bench {
	var bs []bench
	add := func(name string, iters int, run func(n int)) { bs = append(bs, bench{name, iters, run}) }

	add("getppid_raw", 2_000_000, func(n int) {
		for k := 0; k < n; k++ {
			r, _, _ := syscall.RawSyscall(syscall.SYS_GETPPID, 0, 0, 0)
			sink += uint64(r)
		}
	})
	add("getppid_syscall", 2_000_000, func(n int) {
		for k := 0; k < n; k++ {
			r, _, _ := syscall.Syscall(syscall.SYS_GETPPID, 0, 0, 0)
			sink += uint64(r)
		}
	})

	ea, eb := tcpPair()
	buf := make([]byte, 256<<10)
	add("read_eagain_std", 1_000_000, func(n int) {
		for k := 0; k < n; k++ {
			if _, err := syscall.Read(ea, buf); err != syscall.EAGAIN {
				panic(err)
			}
		}
	})
	add("read_eagain_raw", 1_000_000, func(n int) {
		for k := 0; k < n; k++ {
			if _, e := rawRead(ea, buf); e != syscall.EAGAIN {
				panic(e)
			}
		}
	})
	for _, size := range []int{hdrLen, hdrLen + payload} {
		msg := make([]byte, size)
		add(fmt.Sprintf("write_read_%d_std", size), 200_000, func(n int) {
			for k := 0; k < n; k++ {
				w, err := syscall.Write(ea, msg)
				must(err)
				for got := 0; got < w; {
					r, err := syscall.Read(eb, buf)
					if err == syscall.EAGAIN {
						continue
					}
					must(err)
					got += r
				}
			}
		})
		add(fmt.Sprintf("write_read_%d_raw", size), 200_000, func(n int) {
			for k := 0; k < n; k++ {
				w, e := rawWrite(ea, msg)
				if e != 0 {
					panic(e)
				}
				for got := 0; got < w; {
					r, e := rawRead(eb, buf)
					if e == syscall.EAGAIN {
						continue
					}
					if e != 0 {
						panic(e)
					}
					got += r
				}
			}
		})
	}

	ep, err := syscall.EpollCreate1(0)
	must(err)
	must(syscall.EpollCtl(ep, syscall.EPOLL_CTL_ADD, ea, &syscall.EpollEvent{Events: syscall.EPOLLIN, Fd: 0}))
	evs := make([]syscall.EpollEvent, 64)
	epollWait := func(want int) func(n int) {
		return func(n int) {
			for k := 0; k < n; k++ {
				got, err := syscall.EpollWait(ep, evs, 0)
				must(err)
				if got != want {
					panic("epoll")
				}
			}
		}
	}
	epollWaitRaw := func(want int) func(n int) {
		return func(n int) {
			for k := 0; k < n; k++ {
				if rawEpollWait(ep, evs) != want {
					panic("epoll")
				}
			}
		}
	}
	add("epoll_wait0_empty_std", 1_000_000, epollWait(0))
	add("epoll_wait0_empty_raw", 1_000_000, epollWaitRaw(0))
	ready := func(name string, run func(n int)) {
		add(name, 1_000_000, func(n int) {
			_, err := syscall.Write(eb, []byte{1})
			must(err)
			time.Sleep(time.Millisecond)
			run(n)
			for {
				if _, err := syscall.Read(ea, buf); err == syscall.EAGAIN {
					break
				}
			}
		})
	}
	ready("epoll_wait0_ready_std", epollWait(1))
	ready("epoll_wait0_ready_raw", epollWaitRaw(1))

	add("clock_now", 5_000_000, func(n int) {
		for k := 0; k < n; k++ {
			sink += uint64(time.Now().UnixNano())
		}
	})

	var repl, acks conn
	fill(&repl, 16, payload)
	fill(&acks, 64, 0)
	frames := make([]frame, 0, 64)
	add("parse_repl16", 2_000_000, func(n int) {
		for k := 0; k < n; k++ {
			frames = parse(&repl, frames[:0])
			sink += uint64(len(frames))
		}
	})
	add("parse_ack64", 500_000, func(n int) {
		for k := 0; k < n; k++ {
			frames = parse(&acks, frames[:0])
			sink += uint64(len(frames))
		}
	})
	add("parse_ack64_bce", 500_000, func(n int) {
		for k := 0; k < n; k++ {
			frames = parseBCE(&acks, frames[:0])
			sink += uint64(len(frames))
		}
	})
	add("parse_ack64_inplace", 500_000, func(n int) {
		for k := 0; k < n; k++ {
			frames = parseInPlace(&acks, frames[:0])
			sink += uint64(len(frames))
		}
	})
	var c conn
	add("header16", 1_000_000, func(n int) {
		for i := 0; i < n; i++ {
			c.wbuf = c.wbuf[:0]
			for j := 0; j < 16; j++ {
				c.wbuf = header(c.wbuf, kindAck, 0, uint64(i+j), 0)
			}
			sink += uint64(len(c.wbuf))
		}
	})
	data := make([]byte, payload)
	add("append_frame_4k", 2_000_000, func(n int) {
		for i := 0; i < n; i++ {
			c.wbuf = append(header(c.wbuf[:0], kindRepl, payload, uint64(i), 7), data...)
			sink += uint64(len(c.wbuf))
		}
	})
	castagnoli := crc32.MakeTable(crc32.Castagnoli)
	add("crc32c_4k", 1_000_000, func(n int) {
		for i := 0; i < n; i++ {
			data[0] = byte(i)
			sink += uint64(crc32.Checksum(data, castagnoli))
		}
	})
	for _, size := range []int{64 << 10, 64 << 20} {
		rg := &ring{mem: make([]byte, size)}
		for i := 0; i < size; i += 4096 {
			rg.mem[i] = 1
		}
		add(fmt.Sprintf("ring_store_%dk", size>>10), 1_000_000, func(n int) {
			for k := 0; k < n; k++ {
				rg.store(data)
			}
			sink += uint64(rg.off)
		})
	}

	pendingPtr := map[uint64]*pend{}
	pendingVal := map[uint64]pend{}
	var next uint64
	for next < 16 {
		next++
		pendingPtr[next] = &pend{tok: 2, cid: next}
		pendingVal[next] = pend{tok: 2, cid: next}
	}
	add("map_cycle_ptr", 5_000_000, func(n int) {
		for k := 0; k < n; k++ {
			next++
			pendingPtr[next] = &pend{tok: 2, cid: next}
			id := next - 16
			for k := 0; k < 2; k++ {
				e := pendingPtr[id]
				e.acks++
				if e.acks == 2 {
					delete(pendingPtr, id)
					sink += e.cid
				}
			}
		}
	})
	next = 16
	add("map_cycle_val", 5_000_000, func(n int) {
		for k := 0; k < n; k++ {
			next++
			pendingVal[next] = pend{tok: 2, cid: next}
			id := next - 16
			for k := 0; k < 2; k++ {
				e := pendingVal[id]
				e.acks++
				if e.acks == 2 {
					delete(pendingVal, id)
					sink += e.cid
				} else {
					pendingVal[id] = e
				}
			}
		}
	})

	bytes := make([]byte, payload)
	for i := range bytes {
		bytes[i] = byte(i * 7)
	}
	add("sum_bytes_4k", 500_000, func(n int) {
		for k := 0; k < n; k++ {
			var s uint64
			for i := 0; i < len(bytes); i++ {
				s += uint64(bytes[i])
			}
			sink += s
		}
	})
	idx := make([]uint32, 1024)
	for i := range idx {
		idx[i] = uint32(i*2654435761) % payload
	}
	add("gather_1k", 500_000, func(n int) {
		for k := 0; k < n; k++ {
			var s uint64
			for _, j := range idx {
				s += uint64(bytes[j])
			}
			sink += s
		}
	})
	return bs
}

func main() {
	filter := ""
	if len(os.Args) > 1 {
		filter = os.Args[1]
	}
	for _, b := range benches() {
		if !strings.Contains(b.name, filter) {
			continue
		}
		b.run(b.iters / 10)
		var ns []float64
		for k := 0; k < reps; k++ {
			t := time.Now()
			b.run(b.iters)
			ns = append(ns, float64(time.Since(t).Nanoseconds())/float64(b.iters))
		}
		sort.Float64s(ns)
		fmt.Printf("BENCH lang=go name=%s ns_per_op=%.1f min=%.1f max=%.1f\n", b.name, ns[reps/2], ns[0], ns[reps-1])
	}
	if sink == 42 {
		fmt.Println()
	}
}
