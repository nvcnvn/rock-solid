package loop_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/loop"
	"github.com/nvcnvn/rock-solid/internal/loop/looptest"
	"github.com/nvcnvn/rock-solid/internal/trace"
	"golang.org/x/sys/unix"
)

const realBudget = 512

// echo answers each message whose first byte is 'e' to its sender, counts every message, and
// arms a timer at Wait ticks from now on an EvTimer of ID 0.
type echo struct {
	got   atomic.Int64
	fired atomic.Int64
	at    core.Tick
	wait  core.Tick
}

func (m *echo) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	switch ev.Kind {
	case core.EvNetRecv:
		m.got.Add(1)
		if ev.Msg.Len > 0 && ev.Msg.Body[0] == 'e' {
			return append(out, core.Action{Kind: core.ActNetSend, Peer: ev.Peer, Msg: ev.Msg})
		}
	case core.EvTimer:
		if ev.Timer == 0 {
			m.at = ctx.Now + m.wait
			return append(out, core.Action{Kind: core.ActSetTimer, Timer: 1, At: m.at})
		}
		m.fired.Add(1)
	}
	return out
}

// counted counts the calls a loop makes through the seam, and the reads Wait makes to drain the
// eventfd and the timerfd it reports.
type counted struct {
	*loop.Epoll
	ctl, unwatch, waits, drains, arms int
	// afterWait, when set, runs as each Wait returns.
	afterWait func()
}

func (c *counted) Watch(fd int, token uint32) error {
	c.ctl++
	return c.Epoll.Watch(fd, token)
}

func (c *counted) Modify(fd int, token uint32, in, out bool) error {
	c.ctl++
	return c.Epoll.Modify(fd, token, in, out)
}

func (c *counted) Unwatch(fd int) error {
	c.ctl++
	c.unwatch++
	return c.Epoll.Unwatch(fd)
}

func (c *counted) Wait(ready []loop.Ready, block bool) []loop.Ready {
	c.waits++
	n := len(ready)
	ready = c.Epoll.Wait(ready, block)
	if c.afterWait != nil {
		c.afterWait()
	}
	for _, r := range ready[n:] {
		if r.Token == loop.TokenWake || r.Token == loop.TokenTimer {
			c.drains++
		}
	}
	return ready
}

func (c *counted) Arm(deadline core.Tick) {
	c.arms++
	c.Epoll.Arm(deadline)
}

type realLoop struct {
	l    *loop.Loop
	k    *loop.Epoll
	c    *counted
	m    *echo
	w    *looptest.Wire
	ring *trace.Ring
	pool *loop.Pool
	ends []int
	fds  []int
}

func newRealLoop(t *testing.T, conns int) *realLoop {
	t.Helper()
	k, err := loop.NewEpoll(loop.VMClockPath)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := loop.NewPool(64<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	ring, err := trace.NewRing(make([]byte, 1<<17*trace.RecordSize))
	if err != nil {
		t.Fatal(err)
	}
	r := &realLoop{k: k, c: &counted{Epoll: k}, m: &echo{}, w: &looptest.Wire{}, ring: ring, pool: pool}
	r.l = loop.New(loop.Config{Pool: pool, ReadBudget: realBudget, Trace: ring}, r.c, r.m, r.w)
	r.w.L = r.l
	for i := range conns {
		p, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
		if err != nil {
			t.Fatal(err)
		}
		c, err := r.l.AddConn(p[0], i%2 == 0)
		if err != nil {
			t.Fatal(err)
		}
		r.w.Connect(c, core.NodeID{Node: uint32(i)})
		r.ends = append(r.ends, p[1])
		r.fds = append(r.fds, p[0])
	}
	t.Cleanup(func() {
		for _, fd := range r.ends {
			_ = unix.Close(fd)
		}
		for i := range conns {
			r.l.Close(loop.ConnID(i))
		}
		k.Close()
		_ = pool.Close()
	})
	return r
}

func frame(b []byte, v uint64) []byte {
	b = binary.LittleEndian.AppendUint16(b, 9)
	b = append(b, 'e')
	return binary.LittleEndian.AppendUint64(b, v)
}

// frameLen is the bytes of a frame of frame's.
const frameLen = 11

// drain reads every echo waiting at the test's ends and returns how many bytes it read.
func (r *realLoop) drain(buf []byte) int {
	n := 0
	for _, fd := range r.ends {
		for {
			k, err := unix.Read(fd, buf)
			if k <= 0 || err != nil {
				break
			}
			n += k
		}
	}
	return n
}

func (r *realLoop) records() []trace.Record {
	recs := make([]trace.Record, r.ring.Len())
	for i := range recs {
		recs[i] = r.ring.At(i)
	}
	return recs
}

func TestAPassUnder1000Connections(t *testing.T) {
	r := newRealLoop(t, 1000)
	var in []byte
	sent := 0
	for i, fd := range r.ends {
		frames := 3
		if i%100 == 0 {
			frames = 300
		}
		in = in[:0]
		for j := range frames {
			in = frame(in, uint64(i<<16|j))
		}
		if _, err := unix.Write(fd, in); err != nil {
			t.Fatal(err)
		}
		sent += frames
	}
	buf := make([]byte, 64<<10)
	got := 0
	for got < sent*frameLen {
		r.l.Pass()
		got += r.drain(buf)
	}
	if r.w.Lost > 0 || r.m.got.Load() != int64(sent) {
		t.Fatalf("%d of %d frames handled, %d lost", r.m.got.Load(), sent, r.w.Lost)
	}
	if r.ring.Added() > uint64(r.ring.Len()) {
		t.Fatal("trace ring overflowed")
	}
	recs := r.records()
	if err := trace.CheckRecords(recs, trace.OneEpollWaitPerPass(), trace.OneWritevPerFD(), trace.ReadBudget(realBudget)); err != nil {
		t.Fatal(err)
	}
	most := uint64(0)
	for _, rec := range recs {
		if rec.Name == trace.SysEpollWait {
			most = max(most, rec.Fields[0])
		}
	}
	if most < 1000 {
		t.Fatalf("no pass found 1,000 descriptors ready; most %d", most)
	}
	t.Logf("%d frames echoed in %d passes, at most %d descriptors ready in one", sent, r.l.Passes(), most)
}

// threadIO reads the read and write syscalls the calling thread has made from io, its open
// /proc/thread-self/io; each call is itself one read.
func threadIO(t *testing.T, io int) (reads, writes int) {
	t.Helper()
	var buf [512]byte
	n, err := unix.Pread(io, buf[:], 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(buf[:n]), "\n") {
		if v, ok := strings.CutPrefix(line, "syscr: "); ok {
			reads, _ = strconv.Atoi(v)
		}
		if v, ok := strings.CutPrefix(line, "syscw: "); ok {
			writes, _ = strconv.Atoi(v)
		}
	}
	return reads, writes
}

func TestNoSyscallOfAPassIsMissingFromTheTrace(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	io, err := unix.Open("/proc/thread-self/io", unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatalf("this check counts syscalls in /proc/thread-self/io: %v", err)
	}
	defer unix.Close(io)
	r := newRealLoop(t, 4)
	r.m.wait = 1000
	r0, w0 := threadIO(t, io)
	r1, w1 := threadIO(t, io)
	selfR, selfW := r1-r0, w1-w0
	var reads, writes int
	pass := func() {
		r0, w0 := threadIO(t, io)
		r.l.Pass()
		r1, w1 := threadIO(t, io)
		reads += r1 - r0 - selfR
		writes += w1 - w0 - selfW
	}
	var in []byte
	buf := make([]byte, 64<<10)
	r.l.Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: 0})
	for i := range 20 {
		in = in[:0]
		for j := range 1 + 40*(i%3) {
			in = frame(in, uint64(j))
		}
		if _, err := unix.Write(r.ends[i%3], in); err != nil {
			t.Fatal(err)
		}
		if i == 10 {
			time.Sleep(2 * time.Millisecond)
		}
		pass()
		r.drain(buf)
	}
	_ = unix.Close(r.ends[3])
	r.ends[3] = -1
	pass()
	var count [trace.SysClose + 1]int
	for _, rec := range r.records() {
		if rec.Name <= trace.SysClose {
			count[rec.Name]++
		}
	}
	got := fmt.Sprintf("read %d, writev %d, epoll_ctl %d, epoll_wait %d, timerfd_settime %d, close %d",
		reads-r.c.drains, writes, r.c.ctl, r.c.waits, r.c.arms, r.c.unwatch)
	traced := fmt.Sprintf("read %d, writev %d, epoll_ctl %d, epoll_wait %d, timerfd_settime %d, close %d",
		count[trace.SysRead], count[trace.SysWritev], count[trace.SysEpollCtl], count[trace.SysEpollWait], count[trace.SysTimerfdSettime], count[trace.SysClose])
	if got != traced {
		t.Fatalf("syscalls made: %s\nsyscalls traced: %s", got, traced)
	}
	if r.c.drains < 2 || r.c.arms == 0 || count[trace.SysClose] != 1 || r.m.fired.Load() != 1 {
		t.Fatalf("the run missed a path: %d drains, %d timers fired, %s", r.c.drains, r.m.fired.Load(), traced)
	}
	t.Logf("%d passes; made and traced: %s; plus %d eventfd and timerfd reads under epoll_wait", r.l.Passes(), traced, r.c.drains)
}

func TestGoroutinesDoNotGrowWithConnections(t *testing.T) {
	count := func(conns int) int {
		r := newRealLoop(t, conns)
		buf := make([]byte, 4096)
		for _, fd := range r.ends {
			if _, err := unix.Write(fd, frame(nil, 1)); err != nil {
				t.Fatal(err)
			}
		}
		for got := 0; got < conns*frameLen; got += r.drain(buf) {
			r.l.Pass()
		}
		return runtime.NumGoroutine()
	}
	one, many := count(1), count(1000)
	if one != many {
		t.Fatalf("%d goroutines with 1 connection, %d with 1,000", one, many)
	}
}

func TestARealPassAllocatesNothing(t *testing.T) {
	r := newRealLoop(t, 4)
	in := frame(nil, 7)
	buf := make([]byte, 4096)
	pass := func() {
		if _, err := unix.Write(r.ends[1], in); err != nil {
			t.Fatal(err)
		}
		r.l.Pass()
		if _, err := unix.Read(r.ends[1], buf); err != nil {
			t.Fatal(err)
		}
	}
	pass()
	if n := testing.AllocsPerRun(1000, pass); n != 0 {
		t.Fatalf("%v allocations per pass", n)
	}
}

func TestAnInboxBurstWakesTheLoopOnce(t *testing.T) {
	r := newRealLoop(t, 0)
	var ev core.Event
	ev.Kind = core.EvNetRecv
	ev.Msg.Len = 1
	for range 64 {
		if !r.l.Inbox().Post(&ev) {
			t.Fatal("inbox full")
		}
	}
	if w := r.l.Inbox().Wakes(); w != 1 {
		t.Fatalf("a burst of 64 woke the loop %d times", w)
	}
	r.l.Pass()
	if r.m.got.Load() != 64 || r.l.Passes() != 1 {
		t.Fatalf("%d events in %d passes", r.m.got.Load(), r.l.Passes())
	}
	if p := r.l.Pending(); p.Work {
		t.Fatalf("work left: %+v", p)
	}
}

func TestProducersPostToALoopThatBlocks(t *testing.T) {
	r := newRealLoop(t, 0)
	const producers, each = 4, 20000
	done := make(chan struct{})
	go func() {
		r.l.Run()
		close(done)
	}()
	var wg sync.WaitGroup
	for range producers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var ev core.Event
			ev.Kind = core.EvNetRecv
			for i := range each {
				if i%1000 == 0 {
					time.Sleep(time.Millisecond)
				}
				for !r.l.Inbox().Post(&ev) {
					runtime.Gosched()
				}
			}
		}()
	}
	wg.Wait()
	deadline := time.Now().Add(10 * time.Second)
	for r.m.got.Load() < producers*each && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	r.l.Stop()
	<-done
	if got := r.m.got.Load(); got != producers*each {
		t.Fatalf("%d of %d events handled", got, producers*each)
	}
	t.Logf("%d events in %d passes, %d wakes", producers*each, r.l.Passes(), r.l.Inbox().Wakes())
}

func TestALoopSleepsUntilItsTimer(t *testing.T) {
	r := newRealLoop(t, 0)
	r.m.wait = 50_000
	r.l.Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: 0})
	start := time.Now()
	for r.m.fired.Load() == 0 {
		r.l.Pass()
	}
	if el := time.Since(start); el < 50*time.Millisecond {
		t.Fatalf("timer fired after %v", el)
	}
	if r.l.Passes() > 4 {
		t.Fatalf("%d passes to wait 50 ms: the loop spun", r.l.Passes())
	}
	recs := r.records()
	var armed bool
	for _, rec := range recs {
		if rec.Name == trace.SysTimerfdSettime && core.Tick(rec.Fields[0]) == r.m.at {
			armed = true
		}
		if rec.Name == trace.EvTimer && rec.Fields[0] == 1 && rec.Tick < r.m.at {
			t.Fatalf("timer handled at %d, before its deadline %d", rec.Tick, r.m.at)
		}
	}
	if !armed {
		t.Fatalf("no timerfd_settime at %d in\n%s", r.m.at, r.ring.Dump(nil))
	}
}

// TestCgroupSizing runs in a container started with a CPU limit, which sets RS_WANT_LOOPS and
// RS_WANT_PROCS: docker run --cpus 2.5 -e RS_WANT_LOOPS=2 -e RS_WANT_PROCS=3 ...
func TestCgroupSizing(t *testing.T) {
	wantLoops, wantProcs := os.Getenv("RS_WANT_LOOPS"), os.Getenv("RS_WANT_PROCS")
	if wantLoops == "" {
		t.Skip("RS_WANT_LOOPS is not set; run in a container with a CPU limit")
	}
	s := loop.Size(runtime.GOMAXPROCS(0), runtime.NumCPU())
	if strconv.Itoa(s.Loops) != wantLoops || s.Procs != 0 || strconv.Itoa(runtime.GOMAXPROCS(0)) != wantProcs {
		t.Fatalf("on %d CPUs with GOMAXPROCS %d: %+v; want %s loops and GOMAXPROCS %s", runtime.NumCPU(), runtime.GOMAXPROCS(0), s, wantLoops, wantProcs)
	}
	loops := make([]*realLoop, s.Loops)
	var wg sync.WaitGroup
	for i := range loops {
		loops[i] = newRealLoop(t, 1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			loops[i].l.Run()
		}()
	}
	before := throttled(t)
	time.Sleep(3 * time.Second)
	after := throttled(t)
	for _, r := range loops {
		r.l.Stop()
	}
	wg.Wait()
	if after != before {
		t.Fatalf("idle loops throttled in %d periods", after-before)
	}
	t.Logf("%d CPUs: %d loops, GOMAXPROCS %d, no throttled period in 3 s idle", runtime.NumCPU(), s.Loops, runtime.GOMAXPROCS(0))
}

func throttled(t *testing.T) int {
	b, err := os.ReadFile("/sys/fs/cgroup/cpu.stat")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range bytes.Split(b, []byte{'\n'}) {
		if v, ok := strings.CutPrefix(string(line), "nr_throttled "); ok {
			n, err := strconv.Atoi(v)
			if err != nil {
				t.Fatal(err)
			}
			return n
		}
	}
	t.Fatalf("cpu.stat has no nr_throttled:\n%s", b)
	return 0
}

func TestAReadThatFindsNothingKeepsTheConnection(t *testing.T) {
	r := newRealLoop(t, 1)
	buf := make([]byte, 64)
	r.c.afterWait = func() {
		if _, err := unix.Read(r.fds[0], buf); err != nil {
			t.Errorf("taking the frame from under the loop: %v", err)
		}
		r.c.afterWait = nil
	}
	if _, err := unix.Write(r.ends[0], frame(nil, 1)); err != nil {
		t.Fatal(err)
	}
	r.l.Pass()
	if _, err := unix.Write(r.ends[0], frame(nil, 2)); err != nil {
		t.Fatal(err)
	}
	r.l.Pass()
	if n := r.drain(buf); n != frameLen || r.m.got.Load() != 1 || r.w.Lost != 0 {
		t.Fatalf("%d bytes echoed, %d frames handled, %d connections lost\n%s", n, r.m.got.Load(), r.w.Lost, r.ring.Dump(nil))
	}
}

// capped is a loop over one peer connection, 0, and one client, 1, with QueueCap bytes of cap,
// whose test ends are returned.
func capped(t *testing.T, queueCap int, sndbuf int) (*loop.Loop, *looptest.Wire, *echo, *trace.Ring, []int) {
	t.Helper()
	k, err := loop.NewEpoll(loop.VMClockPath)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := loop.NewPool(8<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	ring, err := trace.NewRing(make([]byte, 1<<17*trace.RecordSize))
	if err != nil {
		t.Fatal(err)
	}
	w, m := &looptest.Wire{}, &echo{}
	l := loop.New(loop.Config{Pool: pool, ReadBudget: realBudget, QueueCap: queueCap, Trace: ring}, k, m, w)
	w.L = l
	var ends []int
	for i, peer := range []bool{true, false} {
		p, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
		if err != nil {
			t.Fatal(err)
		}
		if sndbuf > 0 {
			if err := unix.SetsockoptInt(p[0], unix.SOL_SOCKET, unix.SO_SNDBUF, sndbuf); err != nil {
				t.Fatal(err)
			}
		}
		c, err := l.AddConn(p[0], peer)
		if err != nil {
			t.Fatal(err)
		}
		w.Connect(c, core.NodeID{Node: uint32(i)})
		ends = append(ends, p[1])
	}
	t.Cleanup(func() {
		for i, fd := range ends {
			l.Close(loop.ConnID(i))
			if fd >= 0 {
				_ = unix.Close(fd)
			}
		}
		k.Close()
		_ = pool.Close()
	})
	return l, w, m, ring, ends
}

// runFor runs l for d on another goroutine and returns the passes it made.
func runFor(l *loop.Loop, d time.Duration) uint64 {
	before := l.Passes()
	go func() {
		time.Sleep(d)
		l.Stop()
	}()
	l.Run()
	return l.Passes() - before
}

func TestALoopAtTheQueueCapSleeps(t *testing.T) {
	l, w, _, _, ends := capped(t, 64<<10, 0)
	pool := l.Context().Pool
	b, err := pool.Alloc(4 << 20)
	if err != nil {
		t.Fatal(err)
	}
	if !l.Queue(0, b, 4<<20, true) {
		t.Fatal("queue refused")
	}
	l.Pass()
	if l.Queued() < 64<<10 {
		t.Fatalf("only %d bytes left queued behind the peer", l.Queued())
	}
	if _, err := unix.Write(ends[1], frame(nil, 1)); err != nil {
		t.Fatal(err)
	}
	if n := runFor(l, 100*time.Millisecond); n > 10 {
		t.Fatalf("%d passes in 100 ms with %d bytes queued and a client frame waiting: the loop spun", n, l.Queued())
	}
	_ = unix.Close(ends[1])
	ends[1] = -1
	if n := runFor(l, 100*time.Millisecond); n > 10 || w.Lost != 1 {
		t.Fatalf("%d passes in 100 ms and %d connections closed after the muted client hung up", n, w.Lost)
	}
}

// bigFrame appends a frame of a bigLen-byte echo request carrying v.
func bigFrame(b []byte, v uint64) []byte {
	b = binary.LittleEndian.AppendUint16(b, bigLen-2)
	b = append(b, 'e')
	b = binary.LittleEndian.AppendUint64(b, v)
	return append(b, make([]byte, bigLen-11)...)
}

const bigLen = 202

func TestASlowReaderGetsEveryFrameInOrder(t *testing.T) {
	const frames = 3000
	// A 2304-byte send buffer is the kernel's least, 4,608 once doubled. The socket turns
	// writable once a quarter of that is in flight, and the cap lets a writev gather more than
	// the rest, so writes stop mid-frame.
	l, w, m, ring, ends := capped(t, 40*bigLen, 2304)
	client := ends[1]
	done := make(chan struct{})
	go func() {
		l.Run()
		close(done)
	}()
	var out []byte
	for i := range frames {
		out = bigFrame(out, uint64(i))
	}
	buf := make([]byte, 1500)
	var carry []byte
	next := uint64(0)
	start := time.Now()
	for next < frames && time.Since(start) < 10*time.Second {
		if len(out) > 0 {
			n, err := unix.Write(client, out)
			if err != nil && err != unix.EAGAIN {
				t.Fatal(err)
			}
			out = out[max(n, 0):]
		}
		if time.Since(start) < 50*time.Millisecond {
			time.Sleep(time.Millisecond)
			continue
		}
		time.Sleep(200 * time.Microsecond)
		n, err := unix.Read(client, buf)
		if err == unix.EAGAIN {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		carry = append(carry, buf[:n]...)
		for len(carry) >= bigLen {
			if v := binary.LittleEndian.Uint64(carry[3:11]); v != next {
				t.Fatalf("frame %d arrived where %d was due", v, next)
			}
			next++
			carry = carry[bigLen:]
		}
	}
	l.Stop()
	<-done
	if next != frames || w.Lost != 0 || m.got.Load() != frames {
		t.Fatalf("%d of %d frames echoed in order, %d handled, %d lost", next, frames, m.got.Load(), w.Lost)
	}
	var muted, unmuted, blocked, partial int
	for i := range ring.Len() {
		rec := ring.At(i)
		if rec.Name == trace.SysWritev && rec.Fields[2]%bigLen != 0 {
			partial++
		}
		if rec.Name != trace.SysEpollCtl || rec.Fields[0] != 1 {
			continue
		}
		if rec.Fields[1] == 0 {
			muted++
		} else if muted > 0 {
			unmuted++
		}
		if rec.Fields[2] == 1 {
			blocked++
		}
	}
	if muted == 0 || unmuted == 0 || blocked == 0 || partial == 0 {
		t.Fatalf("the reader was not slow enough: %d mutes, %d unmutes, %d blocked writes, %d partial writes", muted, unmuted, blocked, partial)
	}
	t.Logf("%d frames echoed in order in %d passes: %d mutes, %d unmutes, %d blocked writes, %d partial writes", frames, l.Passes(), muted, unmuted, blocked, partial)
}
