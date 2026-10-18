// Replicated 4K write path in Go, tuned: the protocol and client of go-cpu-per-op/replicate-go,
// with the primary rebuilt around atomics instead of a mutex-guarded map and a channel per request.
//
//   - Requests in flight live in a fixed table of slots indexed by request id, each with an atomic
//     acknowledgment count. No map, no mutex.
//   - The goroutine reading a replica's acknowledgments replies to the client itself when the count
//     reaches 2, so no request is handed back to the goroutine that received it.
//   - Each replica connection has a lock-free bounded queue (Vyukov's MPMC ring, used here by many
//     producers and one consumer) drained by a writer goroutine into one buffered write; before it
//     flushes, the writer yields once so producers that are running can join the batch.
//   - A replica flushes its acknowledgments only when no further frame is already buffered.
//
// usage: replicate-go2 conns=N secs=S [yield=1]
package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
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
	slots     = 1024
	queueSize = 1024
)

var (
	castagnoli    = crc32.MakeTable(crc32.Castagnoli)
	yieldBeforeFl bool
	reads, writes atomic.Int64
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

type countConn struct{ net.Conn }

func (c countConn) Read(b []byte) (int, error) {
	reads.Add(1)
	return c.Conn.Read(b)
}

func (c countConn) Write(b []byte) (int, error) {
	writes.Add(1)
	return c.Conn.Write(b)
}

// A frame is `kind u8 | 3 pad | len u32 | id u64 | crc32c u32 | 4 pad` and len payload bytes.
func header(b []byte, kind byte, n int, id uint64, crc uint32) []byte {
	b = append(b[:0], kind, 0, 0, 0)
	b = binary.LittleEndian.AppendUint32(b, uint32(n))
	b = binary.LittleEndian.AppendUint64(b, id)
	b = binary.LittleEndian.AppendUint32(b, crc)
	return append(b, 0, 0, 0, 0)
}

func readFrame(r *bufio.Reader, buf []byte) (uint64, uint32, []byte, error) {
	if _, err := io.ReadFull(r, buf[:hdrLen]); err != nil {
		return 0, 0, nil, err
	}
	n := int(binary.LittleEndian.Uint32(buf[4:]))
	id := binary.LittleEndian.Uint64(buf[8:])
	crc := binary.LittleEndian.Uint32(buf[16:])
	if _, err := io.ReadFull(r, buf[hdrLen:hdrLen+n]); err != nil {
		return 0, 0, nil, err
	}
	return id, crc, buf[hdrLen : hdrLen+n], nil
}

type ring struct {
	mem []byte
	off atomic.Uint64
}

func (r *ring) store(p []byte, want uint32) {
	if crc32.Checksum(p, castagnoli) != want {
		panic("crc")
	}
	o := (r.off.Add(uint64(len(p))) - uint64(len(p))) % (ringSize - payload)
	copy(r.mem[o:], p)
}

func replica(ln net.Listener) {
	rg := &ring{mem: make([]byte, ringSize)}
	for {
		nc, err := ln.Accept()
		if err != nil {
			return
		}
		c := countConn{nc}
		go func() {
			r := bufio.NewReaderSize(c, 64<<10)
			w := bufio.NewWriterSize(c, 64<<10)
			buf := make([]byte, hdrLen+payload)
			ack := make([]byte, 0, hdrLen)
			for {
				id, crc, p, err := readFrame(r, buf)
				if err != nil {
					return
				}
				rg.store(p, crc)
				w.Write(header(ack, kindAck, 0, id, 0))
				if r.Buffered() < hdrLen {
					must(w.Flush())
				}
			}
		}()
	}
}

// cell is one entry of the queue to a replica. Its data stays valid until both replicas have
// acknowledged the request, which happens after the writer has copied it into its buffer.
type cell struct {
	seq  atomic.Uint64
	id   uint64
	crc  uint32
	data []byte
}

// queue is a bounded lock-free ring: producers claim a position with one atomic add and publish the
// cell by storing its sequence number; the single consumer takes cells in order.
type queue struct {
	_     [64]byte
	tail  atomic.Uint64
	_     [56]byte
	head  uint64
	idle  atomic.Bool
	wake  chan struct{}
	cells [queueSize]cell
}

func newQueue() *queue {
	q := &queue{wake: make(chan struct{}, 1)}
	for i := range q.cells {
		q.cells[i].seq.Store(uint64(i))
	}
	return q
}

func (q *queue) push(id uint64, data []byte, crc uint32) {
	pos := q.tail.Add(1) - 1
	c := &q.cells[pos%queueSize]
	for c.seq.Load() != pos {
		runtime.Gosched()
	}
	c.id, c.crc, c.data = id, crc, data
	c.seq.Store(pos + 1)
	if q.idle.Load() && q.idle.CompareAndSwap(true, false) {
		q.wake <- struct{}{}
	}
}

// pop returns the next cell, or nil if none is published yet.
func (q *queue) pop() *cell {
	c := &q.cells[q.head%queueSize]
	if c.seq.Load() != q.head+1 {
		return nil
	}
	return c
}

func (q *queue) release(c *cell) {
	c.data = nil
	c.seq.Store(q.head + queueSize)
	q.head++
}

type peer struct {
	conn net.Conn
	q    *queue
}

func (p *peer) writer() {
	w := bufio.NewWriterSize(p.conn, 256<<10)
	hdr := make([]byte, 0, hdrLen)
	for {
		c := p.q.pop()
		if c == nil {
			if w.Buffered() > 0 {
				if yieldBeforeFl {
					runtime.Gosched()
					if p.q.pop() != nil {
						continue
					}
				}
				must(w.Flush())
				continue
			}
			p.q.idle.Store(true)
			if p.q.pop() != nil {
				if p.q.idle.CompareAndSwap(true, false) {
					continue
				}
			}
			<-p.q.wake
			continue
		}
		w.Write(header(hdr, kindRepl, len(c.data), c.id, c.crc))
		w.Write(c.data)
		p.q.release(c)
	}
}

type slot struct {
	acks   atomic.Int32
	client net.Conn
	cid    uint64
	ack    []byte
}

type primary struct {
	rg    *ring
	peers []*peer
	next  atomic.Uint64
	slots [slots]slot
}

// ackReader counts a replica's acknowledgments; the second for a request replies to its client.
// A client has one request in flight, so only one goroutine writes to it at a time.
func (pr *primary) ackReader(c net.Conn) {
	r := bufio.NewReaderSize(c, 64<<10)
	buf := make([]byte, hdrLen)
	for {
		id, _, _, err := readFrame(r, buf)
		if err != nil {
			return
		}
		s := &pr.slots[id%slots]
		if s.acks.Add(1) == int32(len(pr.peers)) {
			s.ack = header(s.ack, kindAck, 0, s.cid, 0)
			_, err := s.client.Write(s.ack)
			must(err)
		}
	}
}

func (pr *primary) serve(c net.Conn) {
	r := bufio.NewReaderSize(c, 64<<10)
	buf := make([]byte, hdrLen+payload)
	for {
		cid, crc, p, err := readFrame(r, buf)
		if err != nil {
			return
		}
		pr.rg.store(p, crc)
		id := pr.next.Add(1)
		s := &pr.slots[id%slots]
		s.acks.Store(0)
		s.client, s.cid = c, cid
		for _, pe := range pr.peers {
			pe.q.push(id, p, crc)
		}
	}
}

func cpu() time.Duration {
	var ru syscall.Rusage
	must(syscall.Getrusage(syscall.RUSAGE_SELF, &ru))
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

func main() {
	kv := map[string]int{"conns": 1, "secs": 10, "yield": 1}
	for _, a := range os.Args[1:] {
		k, v, _ := strings.Cut(a, "=")
		n, err := strconv.Atoi(v)
		must(err)
		kv[k] = n
	}
	if kv["conns"] > slots {
		panic("more connections than slots")
	}
	yieldBeforeFl = kv["yield"] == 1
	listen := func() net.Listener {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		must(err)
		return ln
	}
	pr := &primary{rg: &ring{mem: make([]byte, ringSize)}}
	for range 2 {
		ln := listen()
		go replica(ln)
		c, err := net.Dial("tcp", ln.Addr().String())
		must(err)
		pe := &peer{conn: countConn{c}, q: newQueue()}
		pr.peers = append(pr.peers, pe)
		go pr.ackReader(pe.conn)
		go pe.writer()
	}
	pln := listen()
	go func() {
		for {
			c, err := pln.Accept()
			if err != nil {
				return
			}
			go pr.serve(countConn{c})
		}
	}()

	conns, secs := kv["conns"], kv["secs"]
	data := make([]byte, payload)
	for i := range data {
		data[i] = byte(i * 7)
	}
	crc := crc32.Checksum(data, castagnoli)
	var stop atomic.Bool
	var ops atomic.Int64
	lats := make([][]time.Duration, conns)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range conns {
		nc, err := net.Dial("tcp", pln.Addr().String())
		must(err)
		c := countConn{nc}
		wg.Go(func() {
			r := bufio.NewReaderSize(c, 64<<10)
			frame := append(header(make([]byte, 0, hdrLen+payload), kindWrite, payload, 0, crc), data...)
			buf := make([]byte, hdrLen)
			lat := make([]time.Duration, 0, 1<<20)
			<-start
			for id := uint64(0); !stop.Load(); id++ {
				binary.LittleEndian.PutUint64(frame[8:], id)
				s := time.Now()
				_, err := c.Write(frame)
				must(err)
				_, _, _, err = readFrame(r, buf)
				must(err)
				if len(lat) < cap(lat) {
					lat = append(lat, time.Since(s))
				}
				ops.Add(1)
			}
			lats[i] = lat
		})
	}
	time.Sleep(200 * time.Millisecond)
	if path := os.Getenv("CPUPROFILE"); path != "" {
		f, err := os.Create(path)
		must(err)
		must(pprof.StartCPUProfile(f))
		defer pprof.StopCPUProfile()
	}
	c0, t0 := cpu(), time.Now()
	r0, w0 := reads.Load(), writes.Load()
	close(start)
	time.Sleep(time.Duration(secs) * time.Second)
	stop.Store(true)
	wg.Wait()
	wall, used := time.Since(t0).Seconds(), cpu()-c0
	r1, w1 := reads.Load(), writes.Load()
	all := slices.Concat(lats...)
	slices.Sort(all)
	p := func(q float64) float64 {
		return float64(all[min(len(all)-1, int(float64(len(all))*q))].Nanoseconds()) / 1000
	}
	n := float64(ops.Load())
	fmt.Printf("RUN lang=go2 conns=%d yield=%d | ops_per_s=%.0f cpu_us_per_op=%.2f cores=%.2f reads_per_op=%.2f "+
		"writes_per_op=%.2f lat_p50_us=%.1f lat_p99_us=%.1f lat_p999_us=%.1f secs=%.1f\n", conns, kv["yield"], n/wall,
		float64(used.Microseconds())/n, used.Seconds()/wall, float64(r1-r0)/n, float64(w1-w0)/n, p(0.5), p(0.99),
		p(0.999), wall)
}
