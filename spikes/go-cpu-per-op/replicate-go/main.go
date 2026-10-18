// Replicated 4K write path in idiomatic Go: a client sends a 4K write to a primary, which checks
// its CRC32C, appends it to an in-memory log, sends it to two replicas that do the same, and
// acknowledges once both replicas have. One goroutine per connection, over loopback TCP; no disk,
// as on a drive whose flush is nearly free.
//
// usage: replicate-go conns=N secs=S [coalesce=0]
//
// With coalesce=1 the primary sends to each replica through a writer goroutine that empties its
// queue into one buffered write, and a replica flushes its acknowledgments only when no further
// frame is already buffered, so frames that arrive together leave together as replicate-rust's
// event loop sends them.
package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"os"
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
	coalesce      bool
	reads, writes atomic.Int64
)

// countConn counts the socket reads and writes the program makes.
type countConn struct{ net.Conn }

func (c countConn) Read(b []byte) (int, error) {
	reads.Add(1)
	return c.Conn.Read(b)
}

func (c countConn) Write(b []byte) (int, error) {
	writes.Add(1)
	return c.Conn.Write(b)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

// A frame is `kind u8 | 3 pad | len u32 | id u64 | crc32c u32 | 4 pad` and len payload bytes.
func header(b []byte, kind byte, n int, id uint64, crc uint32) []byte {
	b = append(b[:0], kind, 0, 0, 0)
	b = binary.LittleEndian.AppendUint32(b, uint32(n))
	b = binary.LittleEndian.AppendUint64(b, id)
	b = binary.LittleEndian.AppendUint32(b, crc)
	return append(b, 0, 0, 0, 0)
}

// readFrame reads one frame into buf and returns its id, CRC and payload.
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

func (r *ring) append(p []byte) {
	o := (r.off.Add(uint64(len(p))) - uint64(len(p))) % (ringSize - payload)
	copy(r.mem[o:], p)
}

func store(rg *ring, p []byte, want uint32) {
	if crc32.Checksum(p, castagnoli) != want {
		panic("crc")
	}
	rg.append(p)
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
				store(rg, p, crc)
				if !coalesce {
					_, err = c.Write(header(ack, kindAck, 0, id, 0))
					must(err)
					continue
				}
				w.Write(header(ack, kindAck, 0, id, 0))
				if r.Buffered() < hdrLen {
					must(w.Flush())
				}
			}
		}()
	}
}

type peer struct {
	mu   sync.Mutex
	conn net.Conn
	out  []byte
	q    chan repl
}

type repl struct {
	id   uint64
	data []byte
	crc  uint32
}

// writer sends queued frames, as many as are queued in one write. A frame's data stays valid until
// its sender sees both acknowledgments, which come after this write.
func (p *peer) writer() {
	w := bufio.NewWriterSize(p.conn, 256<<10)
	hdr := make([]byte, 0, hdrLen)
	for f := range p.q {
		for {
			w.Write(header(hdr, kindRepl, len(f.data), f.id, f.crc))
			w.Write(f.data)
			select {
			case f = <-p.q:
				continue
			default:
			}
			break
		}
		must(w.Flush())
	}
}

func (p *peer) send(id uint64, data []byte, crc uint32) {
	if coalesce {
		p.q <- repl{id, data, crc}
		return
	}
	p.mu.Lock()
	b := append(header(p.out, kindRepl, len(data), id, crc), data...)
	p.out = b
	_, err := p.conn.Write(b)
	p.mu.Unlock()
	must(err)
}

type primary struct {
	rg      *ring
	peers   []*peer
	next    atomic.Uint64
	mu      sync.Mutex
	pending map[uint64]chan struct{}
}

func (pr *primary) ackReader(p *peer) {
	r := bufio.NewReaderSize(p.conn, 64<<10)
	buf := make([]byte, hdrLen)
	for {
		id, _, _, err := readFrame(r, buf)
		if err != nil {
			return
		}
		pr.mu.Lock()
		ch := pr.pending[id]
		pr.mu.Unlock()
		ch <- struct{}{}
	}
}

func (pr *primary) serve(c net.Conn) {
	r := bufio.NewReaderSize(c, 64<<10)
	buf := make([]byte, hdrLen+payload)
	ack := make([]byte, 0, hdrLen)
	done := make(chan struct{}, len(pr.peers))
	for {
		cid, crc, p, err := readFrame(r, buf)
		if err != nil {
			return
		}
		store(pr.rg, p, crc)
		id := pr.next.Add(1)
		pr.mu.Lock()
		pr.pending[id] = done
		pr.mu.Unlock()
		for _, pe := range pr.peers {
			pe.send(id, p, crc)
		}
		for range pr.peers {
			<-done
		}
		pr.mu.Lock()
		delete(pr.pending, id)
		pr.mu.Unlock()
		_, err = c.Write(header(ack, kindAck, 0, cid, 0))
		must(err)
	}
}

func cpu() time.Duration {
	var ru syscall.Rusage
	must(syscall.Getrusage(syscall.RUSAGE_SELF, &ru))
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

func main() {
	kv := map[string]int{"conns": 1, "secs": 10, "coalesce": 0}
	for _, a := range os.Args[1:] {
		k, v, _ := strings.Cut(a, "=")
		n, err := strconv.Atoi(v)
		must(err)
		kv[k] = n
	}
	coalesce = kv["coalesce"] == 1
	listen := func() net.Listener {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		must(err)
		return ln
	}
	pr := &primary{rg: &ring{mem: make([]byte, ringSize)}, pending: map[uint64]chan struct{}{}}
	for range 2 {
		ln := listen()
		go replica(ln)
		c, err := net.Dial("tcp", ln.Addr().String())
		must(err)
		pe := &peer{conn: countConn{c}, out: make([]byte, 0, hdrLen+payload), q: make(chan repl, 1024)}
		pr.peers = append(pr.peers, pe)
		go pr.ackReader(pe)
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
	fmt.Printf("RUN lang=go conns=%d coalesce=%d | ops_per_s=%.0f cpu_us_per_op=%.2f cores=%.2f reads_per_op=%.2f "+
		"writes_per_op=%.2f lat_p50_us=%.1f lat_p99_us=%.1f lat_p999_us=%.1f secs=%.1f\n", conns, kv["coalesce"], n/wall,
		float64(used.Microseconds())/n, used.Seconds()/wall, float64(r1-r0)/n, float64(w1-w0)/n, p(0.5), p(0.99), p(0.999), wall)
}
