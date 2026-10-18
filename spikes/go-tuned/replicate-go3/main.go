// Replicated 4K write path in Go with replicate-rust's design: one epoll event loop per node on
// non-blocking sockets of its own, bypassing Go's network poller, with no locks or channels, and
// each loop pass writing every connection's frames in one write. Same protocol, client and checks
// as go-cpu-per-op/replicate-go and replicate-rust, so the three differ only in language and design.
//
// usage: replicate-go3 conns=N secs=S [lock=1]   (lock=1 pins each loop to its own OS thread)
package main

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"net"
	"os"
	"runtime"
	"runtime/pprof"
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
	castagnoli    = crc32.MakeTable(crc32.Castagnoli)
	reads, writes atomic.Int64
	lockThreads   bool
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
}

type frame struct {
	kind       byte
	id         uint64
	crc        uint32
	start, len int
}

type loop struct {
	ep    int
	conns []*conn
}

func newLoop() *loop {
	ep, err := syscall.EpollCreate1(0)
	must(err)
	return &loop{ep: ep}
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
	if c.armed == out {
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
	for c.wpos < len(c.wbuf) {
		writes.Add(1)
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

func (l *loop) read(tok int, frames []frame) []frame {
	c := l.conns[tok]
	for {
		if c.rlen == len(c.rbuf) {
			c.rbuf = append(c.rbuf, make([]byte, len(c.rbuf))...)
		}
		reads.Add(1)
		n, err := syscall.Read(c.fd, c.rbuf[c.rlen:])
		if err == syscall.EAGAIN {
			break
		}
		if err == syscall.ECONNRESET || n == 0 {
			return frames
		}
		must(err)
		c.rlen += n
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
	if lockThreads {
		runtime.LockOSThread()
	}
	evs := make([]syscall.EpollEvent, 64)
	var frames []frame
	var dirty []int
	for !stop.Load() {
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

// pair returns the raw descriptors of both ends of a loopback TCP connection, detached from Go's
// network poller.
func pair(ln net.Listener) (int, int) {
	a, err := net.Dial("tcp", ln.Addr().String())
	must(err)
	b, err := ln.Accept()
	must(err)
	fd := func(c net.Conn) int {
		f, err := c.(*net.TCPConn).File()
		must(err)
		// The os.File's finalizer would close its descriptor, so keep a duplicate instead.
		d, err := syscall.Dup(int(f.Fd()))
		must(err)
		must(f.Close())
		must(c.Close())
		return d
	}
	return fd(a), fd(b)
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

func primary(replicas, clients []int, stop *atomic.Bool, wg *sync.WaitGroup) {
	defer wg.Done()
	l := newLoop()
	for _, fd := range replicas {
		l.add(fd)
	}
	for _, fd := range clients {
		l.add(fd)
	}
	rg := &ring{mem: make([]byte, ringSize)}
	pending := map[uint64]*pend{}
	var next uint64
	l.run(stop, func(l *loop, tok int, frames []frame) {
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
}

func cpu() time.Duration {
	var ru syscall.Rusage
	must(syscall.Getrusage(syscall.RUSAGE_SELF, &ru))
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

func main() {
	kv := map[string]int{"conns": 1, "secs": 10, "lock": 1}
	for _, a := range os.Args[1:] {
		k, v, _ := strings.Cut(a, "=")
		n, err := strconv.Atoi(v)
		must(err)
		kv[k] = n
	}
	conns, secs := kv["conns"], kv["secs"]
	lockThreads = kv["lock"] == 1
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
	var clientEnds, serverEnds []int
	for range conns {
		a, b := pair(ln)
		clientEnds = append(clientEnds, a)
		serverEnds = append(serverEnds, b)
	}
	wg.Add(1)
	go primary(toReplicas, serverEnds, &stop, &wg)

	data := make([]byte, payload)
	for i := range data {
		data[i] = byte(i * 7)
	}
	crc := crc32.Checksum(data, castagnoli)
	l := newLoop()
	for _, fd := range clientEnds {
		l.add(fd)
	}
	sent := make([]time.Time, conns)
	lat := make([]time.Duration, 0, 1<<24)
	var ops float64
	send := func(l *loop, tok int, id uint64) {
		c := l.conns[tok]
		c.wbuf = append(header(c.wbuf, kindWrite, payload, id, crc), data...)
	}
	time.Sleep(200 * time.Millisecond)
	if path := os.Getenv("CPUPROFILE"); path != "" {
		f, err := os.Create(path)
		must(err)
		must(pprof.StartCPUProfile(f))
		defer pprof.StopCPUProfile()
	}
	for t := range conns {
		sent[t] = time.Now()
		send(l, t, 0)
		l.flush(t)
	}
	c0, t0 := cpu(), time.Now()
	r0, w0 := reads.Load(), writes.Load()
	end := t0.Add(time.Duration(secs) * time.Second)
	var clientStop atomic.Bool
	l.run(&clientStop, func(l *loop, tok int, frames []frame) {
		for _, f := range frames {
			now := time.Now()
			lat = append(lat, now.Sub(sent[tok]))
			ops++
			if now.After(end) {
				clientStop.Store(true)
				continue
			}
			sent[tok] = now
			send(l, tok, f.id+1)
		}
	})
	wall, used := time.Since(t0).Seconds(), cpu()-c0
	r1, w1 := reads.Load(), writes.Load()
	stop.Store(true)
	wg.Wait()
	slices.Sort(lat)
	p := func(q float64) float64 {
		return float64(lat[min(len(lat)-1, int(float64(len(lat))*q))].Nanoseconds()) / 1000
	}
	fmt.Printf("RUN lang=go3 conns=%d lock=%d | ops_per_s=%.0f cpu_us_per_op=%.2f cores=%.2f reads_per_op=%.2f "+
		"writes_per_op=%.2f lat_p50_us=%.1f lat_p99_us=%.1f lat_p999_us=%.1f secs=%.1f\n", conns, kv["lock"],
		ops/wall, float64(used.Microseconds())/ops, used.Seconds()/wall, float64(r1-r0)/ops, float64(w1-w0)/ops,
		p(0.5), p(0.99), p(0.999), wall)
}
