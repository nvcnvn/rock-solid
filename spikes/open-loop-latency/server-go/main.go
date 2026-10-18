// Replicated 4K write server: go-tuned/replicate-go3's primary and two replicas in one process,
// same protocol, CRC32C and batching (one goroutine per node running its own epoll loop on raw
// non-blocking sockets), with the primary serving external clients on a TCP port instead of an
// in-process client. It prints "PORT <n>" once listening, accepts conns clients, and exits when
// all of them have closed, printing its op count and process CPU. CPUPROFILE=<path> writes a CPU
// profile of the serving period, TRACE=<path> an execution trace.
//
// usage: server-go conns=N [drain=1]
//
// With drain=1 (the original harness) an event loop pass reads each readable connection until it
// would block; with drain=0 it makes one read of up to the 256 KiB read buffer, and level-triggered
// epoll reports the rest on the next pass.
package main

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"net"
	"os"
	"runtime/pprof"
	"runtime/trace"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	hdrLen    = 24
	payload   = 4096
	kindWrite = 1
	kindRepl  = 2
	kindAck   = 3
	ringSize  = 64 << 20
)

var (
	castagnoli = crc32.MakeTable(crc32.Castagnoli)
	drain      = true
	maxWbuf    atomic.Int64
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func header(b []byte, kind byte, n int, id uint64, crc uint32) []byte {
	b = append(b, kind, 0, 0, 0)
	b = binary.LittleEndian.AppendUint32(b, uint32(n))
	b = binary.LittleEndian.AppendUint64(b, id)
	b = binary.LittleEndian.AppendUint32(b, crc)
	return append(b, 0, 0, 0, 0)
}

type ring struct {
	mem []byte
	off int
}

func (r *ring) store(p []byte, want uint32) {
	if crc32.Checksum(p, castagnoli) != want {
		panic("crc")
	}
	if r.off+len(p) > ringSize {
		r.off = 0
	}
	copy(r.mem[r.off:], p)
	r.off += len(p)
}

type conn struct {
	fd    int
	rbuf  []byte
	rlen  int
	wbuf  []byte
	wpos  int
	armed bool
	eof   bool
}

type frame struct {
	kind       byte
	id         uint64
	crc        uint32
	start, len int
}

type loop struct {
	ep         int
	conns      []*conn
	closed     int
	closeLimit int
}

func newLoop() *loop {
	ep, err := syscall.EpollCreate1(0)
	must(err)
	return &loop{ep: ep, closeLimit: int(^uint(0) >> 1)}
}

func (l *loop) add(fd int) int {
	must(syscall.SetNonblock(fd, true))
	must(syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1))
	tok := len(l.conns)
	ev := syscall.EpollEvent{Events: syscall.EPOLLIN, Fd: int32(tok)}
	must(syscall.EpollCtl(l.ep, syscall.EPOLL_CTL_ADD, fd, &ev))
	l.conns = append(l.conns, &conn{fd: fd, rbuf: make([]byte, 256<<10)})
	return tok
}

func (l *loop) arm(tok int, out bool) {
	c := l.conns[tok]
	if c.armed == out || c.eof {
		return
	}
	c.armed = out
	ev := syscall.EpollEvent{Events: syscall.EPOLLIN, Fd: int32(tok)}
	if out {
		ev.Events |= syscall.EPOLLOUT
	}
	must(syscall.EpollCtl(l.ep, syscall.EPOLL_CTL_MOD, c.fd, &ev))
}

func (l *loop) flush(tok int) {
	c := l.conns[tok]
	if n := int64(len(c.wbuf)); n > maxWbuf.Load() {
		maxWbuf.Store(n)
	}
	for c.wpos < len(c.wbuf) {
		n, err := syscall.Write(c.fd, c.wbuf[c.wpos:])
		if err == syscall.EAGAIN {
			break
		}
		if err == syscall.ECONNRESET || err == syscall.EPIPE {
			c.wpos = len(c.wbuf)
			break
		}
		must(err)
		c.wpos += n
	}
	done := c.wpos == len(c.wbuf)
	if done {
		c.wbuf, c.wpos = c.wbuf[:0], 0
	}
	l.arm(tok, !done)
}

// read reads what is available, or with drain=0 at most one buffer's worth, and returns the
// complete frames, leaving their bytes in rbuf.
func (l *loop) read(tok int, frames []frame) []frame {
	c := l.conns[tok]
	for {
		if c.rlen == len(c.rbuf) {
			if !drain {
				break
			}
			c.rbuf = append(c.rbuf, make([]byte, len(c.rbuf))...)
		}
		n, err := syscall.Read(c.fd, c.rbuf[c.rlen:])
		if err == syscall.EAGAIN {
			break
		}
		if err == syscall.ECONNRESET || n == 0 {
			l.close(tok)
			return frames
		}
		must(err)
		c.rlen += n
		if !drain {
			break
		}
	}
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

func (l *loop) close(tok int) {
	c := l.conns[tok]
	if c.eof {
		return
	}
	c.eof = true
	c.wbuf, c.wpos = c.wbuf[:0], 0
	must(syscall.EpollCtl(l.ep, syscall.EPOLL_CTL_DEL, c.fd, nil))
	l.closed++
}

func (l *loop) consume(tok int, frames []frame) {
	if len(frames) == 0 {
		return
	}
	c := l.conns[tok]
	last := frames[len(frames)-1]
	end := last.start + last.len
	c.rlen = copy(c.rbuf, c.rbuf[end:c.rlen])
}

func (l *loop) run(stop *atomic.Bool, on func(l *loop, tok int, frames []frame)) {
	evs := make([]syscall.EpollEvent, 64)
	var frames []frame
	var dirty []int
	for !stop.Load() && l.closed < l.closeLimit {
		n, err := syscall.EpollWait(l.ep, evs, 10)
		if err == syscall.EINTR {
			continue
		}
		must(err)
		for _, ev := range evs[:n] {
			tok := int(ev.Fd)
			if ev.Events&syscall.EPOLLOUT != 0 {
				dirty = append(dirty, tok)
			}
			if ev.Events&syscall.EPOLLIN != 0 {
				frames = l.read(tok, frames[:0])
				on(l, tok, frames)
				l.consume(tok, frames)
			}
		}
		for t, c := range l.conns {
			if len(c.wbuf) > 0 {
				dirty = append(dirty, t)
			}
		}
		slices.Sort(dirty)
		for _, t := range slices.Compact(dirty) {
			l.flush(t)
		}
		dirty = dirty[:0]
	}
}

// rawFD returns a descriptor for c detached from Go's network poller.
func rawFD(c net.Conn) int {
	f, err := c.(*net.TCPConn).File()
	must(err)
	// The os.File's finalizer would close its descriptor, so keep a duplicate instead.
	d, err := syscall.Dup(int(f.Fd()))
	must(err)
	must(f.Close())
	must(c.Close())
	return d
}

func accept(ln net.Listener) int {
	c, err := ln.Accept()
	must(err)
	return rawFD(c)
}

// pair returns the raw descriptors of both ends of a loopback TCP connection.
func pair(ln net.Listener) (int, int) {
	a, err := net.Dial("tcp", ln.Addr().String())
	must(err)
	return rawFD(a), accept(ln)
}

func replica(fd int, stop *atomic.Bool, wg *sync.WaitGroup) {
	defer wg.Done()
	l := newLoop()
	l.add(fd)
	rg := &ring{mem: make([]byte, ringSize)}
	l.run(stop, func(l *loop, tok int, frames []frame) {
		c := l.conns[tok]
		for _, f := range frames {
			rg.store(c.rbuf[f.start:f.start+f.len], f.crc)
			c.wbuf = header(c.wbuf, kindAck, 0, f.id, 0)
		}
	})
}

type pend struct {
	tok  int
	cid  uint64
	acks uint8
}

// primary serves the clients until all of them have closed and returns the number of writes.
func primary(replicas, clients []int) uint64 {
	l := newLoop()
	for _, fd := range replicas {
		l.add(fd)
	}
	l.closeLimit = len(clients)
	for _, fd := range clients {
		l.add(fd)
	}
	rg := &ring{mem: make([]byte, ringSize)}
	pending := map[uint64]*pend{}
	var next uint64
	var never atomic.Bool
	l.run(&never, func(l *loop, tok int, frames []frame) {
		for _, f := range frames {
			if tok < 2 {
				e := pending[f.id]
				e.acks++
				if e.acks == 2 {
					delete(pending, f.id)
					l.conns[e.tok].wbuf = header(l.conns[e.tok].wbuf, kindAck, 0, e.cid, 0)
				}
				continue
			}
			next++
			p := l.conns[tok].rbuf[f.start : f.start+f.len]
			rg.store(p, f.crc)
			pending[next] = &pend{tok: tok, cid: f.id}
			for r := range 2 {
				c := l.conns[r]
				c.wbuf = append(header(c.wbuf, kindRepl, len(p), next, f.crc), p...)
			}
		}
	})
	return next
}

func cpu() time.Duration {
	var ru syscall.Rusage
	must(syscall.Getrusage(syscall.RUSAGE_SELF, &ru))
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

func main() {
	kv := map[string]int{"conns": 16, "drain": 1}
	for _, a := range os.Args[1:] {
		k, v, _ := strings.Cut(a, "=")
		n, err := strconv.Atoi(v)
		must(err)
		kv[k] = n
	}
	conns := kv["conns"]
	drain = kv["drain"] == 1
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	must(err)
	var stop atomic.Bool
	var wg sync.WaitGroup
	var toReplicas []int
	for range 2 {
		a, b := pair(ln)
		toReplicas = append(toReplicas, a)
		wg.Add(1)
		go replica(b, &stop, &wg)
	}
	fmt.Printf("PORT %d\n", ln.Addr().(*net.TCPAddr).Port)
	clients := make([]int, conns)
	for i := range clients {
		clients[i] = accept(ln)
	}
	if path := os.Getenv("CPUPROFILE"); path != "" {
		f, err := os.Create(path)
		must(err)
		must(pprof.StartCPUProfile(f))
	}
	if path := os.Getenv("TRACE"); path != "" {
		f, err := os.Create(path)
		must(err)
		must(trace.Start(f))
	}
	c0, t0 := cpu(), time.Now()
	ops := primary(toReplicas, clients)
	wall, used := time.Since(t0).Seconds(), cpu()-c0
	pprof.StopCPUProfile()
	trace.Stop()
	stop.Store(true)
	wg.Wait()
	fmt.Printf("SERVER lang=go conns=%d drain=%d | ops=%d cpu_s=%.3f wall_s=%.3f cpu_us_per_op=%.2f max_wbuf=%d\n",
		conns, kv["drain"], ops, used.Seconds(), wall, float64(used.Microseconds())/float64(max(ops, 1)), maxWbuf.Load())
}
