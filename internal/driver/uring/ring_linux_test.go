package uring

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"slices"
	"testing"
	"unsafe"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/loop"
	"github.com/nvcnvn/rock-solid/internal/trace"
	"golang.org/x/sys/unix"
)

// machine runs script on each kick, an EvTimer posted to the inbox, and keeps the DiskDones it
// gets.
type machine struct {
	script func(out []core.Action) []core.Action
	got    []core.Event
}

func (m *machine) OnEvent(_ *core.Context, ev *core.Event, out []core.Action) []core.Action {
	if ev.Kind == core.EvTimer {
		return m.script(out)
	}
	m.got = append(m.got, *ev)
	return out
}

func TestCompletionsMatchTheKernelLayout(t *testing.T) {
	if s := unsafe.Sizeof(cqe{}); s != 16 {
		t.Errorf("cqe is %d bytes, want 16", s)
	}
}

type driver struct{ d *Disk }

func (driver) Read(loop.ConnID, []byte) {}
func (driver) Closed(loop.ConnID)       {}
func (d driver) Execute(a *core.Action) { d.d.Execute(a) }
func (driver) Accept(loop.ConnID)       {}
func (driver) Flush() bool              { return false }

type rig struct {
	r     *Ring
	d     *Disk
	pool  *loop.Pool
	m     *machine
	l     *loop.Loop
	trace *trace.Ring
	kick  core.Event
}

func newRing(t testing.TB, poolBytes int, entries uint32) (*Ring, *loop.Pool) {
	t.Helper()
	pool, err := loop.NewPool(poolBytes, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	r, err := New(pool, entries)
	if errors.Is(err, unix.EPERM) || errors.Is(err, unix.ENOSYS) {
		t.Skipf("io_uring is not allowed here: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)
	return r, pool
}

// diskFile creates a disk file of blocks blocks under RS_URING_DIR, or the test's temporary
// directory, which must allow O_DIRECT.
func diskFile(t testing.TB, blocks int) string {
	t.Helper()
	dir := os.Getenv("RS_URING_DIR")
	if dir == "" {
		dir = t.TempDir()
	} else {
		var err error
		if dir, err = os.MkdirTemp(dir, "uring-"); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.RemoveAll(dir) })
	}
	path := filepath.Join(dir, "disk")
	if err := CreateFile(path, int64(blocks)*bs); err != nil {
		t.Fatal(err)
	}
	return path
}

func newRig(t testing.TB, r *Ring, pool *loop.Pool, d *Disk) *rig {
	t.Helper()
	k, err := loop.NewEpoll("/nonexistent/vmclock0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(k.Close)
	tr, err := trace.NewRing(make([]byte, 1<<12*trace.RecordSize))
	if err != nil {
		t.Fatal(err)
	}
	m := &machine{got: make([]core.Event, 0, 1024)}
	l := loop.New(loop.Config{Pool: pool, Trace: tr}, k, m, driver{d})
	if err := l.AttachDisk(r); err != nil {
		t.Fatal(err)
	}
	return &rig{r: r, d: d, pool: pool, m: m, l: l, trace: tr, kick: core.Event{Kind: core.EvTimer, Timer: 1}}
}

// run kicks the machine with script and makes passes until it holds want DiskDones; a pass with
// nothing to do blocks in epoll_wait until the ring's eventfd wakes it.
func (g *rig) run(t testing.TB, want int, script func(out []core.Action) []core.Action) []core.Event {
	g.m.got = g.m.got[:0]
	g.m.script = script
	if !g.l.Inbox().Post(&g.kick) {
		t.Fatal("inbox full")
	}
	for i := 0; len(g.m.got) < want; i++ {
		if i > 10000 {
			t.Fatalf("%d of %d completions after %d passes", len(g.m.got), want, i)
		}
		g.l.Pass()
	}
	return g.m.got
}

// enters is the entries each io_uring_enter in the trace submitted, each of which must wait for
// nothing and be taken whole.
func (g *rig) enters(t testing.TB) []int {
	t.Helper()
	var n []int
	for i := range g.trace.Len() {
		if rec := g.trace.At(i); rec.Name == trace.SysIOUringEnter {
			if f := rec.Fields; f[1] != 0 || f[2] != f[0] || f[3] != 0 {
				t.Errorf("io_uring_enter submit=%d wait=%d took=%d errno=%d", f[0], f[1], f[2], f[3])
			}
			n = append(n, int(rec.Fields[0]))
		}
	}
	return n
}

// drains counts the reads of the ring's eventfd in the trace.
func (g *rig) drains() int {
	n := 0
	for i := range g.trace.Len() {
		if rec := g.trace.At(i); rec.Name == trace.SysRead && rec.Fields[0] == uint64(loop.TokenDisk) && rec.Fields[1] == 8 {
			n++
		}
	}
	return n
}

// reap reaps r until it holds at least n completions and nothing is in flight.
func reap(t *testing.T, r *Ring, n int) []core.Event {
	t.Helper()
	var evs []core.Event
	var drain [8]byte
	for range 1000 {
		if evs = r.Reap(evs); len(evs) >= n && r.InFlight() == 0 {
			return evs
		}
		_, _ = unix.Poll([]unix.PollFd{{Fd: int32(r.Fd()), Events: unix.POLLIN}}, 10)
		_, _ = unix.Read(r.Fd(), drain[:])
	}
	t.Fatalf("completions %+v, %d in flight", evs, r.InFlight())
	return nil
}

// allocAll allocates every block-sized buffer pool hands out and returns their slots.
func allocAll(pool *loop.Pool) map[uint32]bool {
	handed := map[uint32]bool{}
	for {
		b, err := pool.Alloc(bs)
		if err != nil {
			return handed
		}
		handed[b.Slot] = true
	}
}

// breakRing puts /dev/null behind r's descriptor, so every io_uring_enter fails.
func breakRing(t *testing.T, r *Ring) {
	t.Helper()
	null, err := unix.Open("/dev/null", unix.O_RDWR|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unix.Close(null) }()
	if err := unix.Dup2(null, r.fd); err != nil {
		t.Fatal(err)
	}
}

func expect(t testing.TB, got []core.Event, want ...core.Event) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("completions %+v, want %+v", got, want)
	}
	for i := range want {
		want[i].Kind = core.EvDiskDone
		if got[i] != want[i] {
			t.Fatalf("completion %d: %+v, want %+v", i, got[i], want[i])
		}
	}
}

func fill(p core.BufPool, b core.IOBuf, v byte) {
	s := p.Bytes(b)
	for i := range s {
		s[i] = v + byte(i/bs)
	}
}

// roundTrip writes three blocks from two buffers with a linked flush, which go to the kernel in
// one io_uring_enter, then reads them back.
func roundTrip(t *testing.T, g *rig) {
	t.Helper()
	a, b, rd := alloc(t, g.pool.Pool, 2*bs), alloc(t, g.pool.Pool, bs), alloc(t, g.pool.Pool, 3*bs)
	fill(g.pool, a, 1)
	fill(g.pool, b, 7)
	got := g.run(t, 2, func(out []core.Action) []core.Action {
		return append(out,
			core.Action{Kind: core.ActDiskWrite, IO: 1, LBA: 5, Vec: []core.IOVec{{Buf: a, Len: 2 * bs}, {Buf: b, Len: bs}}, Len: 3 * bs},
			core.Action{Kind: core.ActDiskFsync, IO: 2, Linked: true})
	})
	expect(t, got, core.Event{IO: 1}, core.Event{IO: 2})
	if e := g.enters(t); len(e) != 1 || e[0] != 2 {
		t.Fatalf("io_uring_enter submissions %v, want one submitting 2", e)
	}
	expect(t, g.run(t, 1, func(out []core.Action) []core.Action {
		return append(out, core.Action{Kind: core.ActDiskRead, IO: 3, LBA: 5, Buf: rd, Len: 3 * bs})
	}), core.Event{IO: 3})
	want := append(append([]byte{}, g.pool.Bytes(a)...), g.pool.Bytes(b)...)
	if !bytes.Equal(g.pool.Bytes(rd), want) {
		t.Fatal("read back other bytes than written")
	}
}

func TestAWriteAndItsLinkedFlushGoInOneEnter(t *testing.T) {
	r, pool := newRing(t, 4<<20, 64)
	d, err := r.Open(diskFile(t, 64), false)
	if err != nil {
		t.Fatal(err)
	}
	if s := r.Status(); !s.FixedBuffers || !s.FixedFiles || !d.Fixed() {
		t.Fatalf("status %+v, disk fixed %v", s, d.Fixed())
	}
	g := newRig(t, r, pool, d)
	roundTrip(t, g)
	if g.drains() == 0 {
		t.Fatal("no read of the ring's eventfd in the trace")
	}
}

func TestTheRegistrationFallback(t *testing.T) {
	var lim unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_MEMLOCK, &lim); err != nil {
		t.Fatal(err)
	}
	low := lim
	low.Cur = min(lim.Max, 8<<20)
	if err := unix.Setrlimit(unix.RLIMIT_MEMLOCK, &low); err != nil {
		t.Fatal(err)
	}
	r, pool := newRing(t, 64<<20, 64)
	if err := unix.Setrlimit(unix.RLIMIT_MEMLOCK, &lim); err != nil {
		t.Fatal(err)
	}
	s := r.Status()
	if s.FixedBuffers {
		t.Skip("64 MiB registered under an 8 MiB memory-lock limit: the process has CAP_IPC_LOCK")
	}
	if !errors.Is(s.BuffersErr, unix.ENOMEM) {
		t.Fatalf("status %+v, want ENOMEM", s)
	}
	t.Logf("status: %v", s.BuffersErr)
	d, err := r.Open(diskFile(t, 64), false)
	if err != nil {
		t.Fatal(err)
	}
	roundTrip(t, newRig(t, r, pool, d))
}

func TestAFailedWriteCancelsItsLinkedFlush(t *testing.T) {
	r, pool := newRing(t, 1<<20, 64)
	d, err := r.open(diskFile(t, 16), unix.O_RDONLY|unix.O_DIRECT|unix.O_CLOEXEC, false)
	if err != nil {
		t.Fatal(err)
	}
	g := newRig(t, r, pool, d)
	b := alloc(t, pool.Pool, bs)
	expect(t, g.run(t, 2, func(out []core.Action) []core.Action {
		return append(out,
			core.Action{Kind: core.ActDiskWrite, IO: 1, Buf: b, Len: bs},
			core.Action{Kind: core.ActDiskFsync, IO: 2, Linked: true})
	}), core.Event{IO: 1, Err: core.DiskIO}, core.Event{IO: 2, Err: core.DiskIO})
}

func TestARefusedWriteFailsItsLinkedFlushAtOnce(t *testing.T) {
	r, pool := newRing(t, 1<<20, 64)
	d, err := r.Open(diskFile(t, 16), false)
	if err != nil {
		t.Fatal(err)
	}
	g := newRig(t, r, pool, d)
	b := alloc(t, pool.Pool, bs)
	expect(t, g.run(t, 2, func(out []core.Action) []core.Action {
		return append(out,
			core.Action{Kind: core.ActDiskWrite, IO: 1, LBA: 16, Buf: b, Len: bs},
			core.Action{Kind: core.ActDiskFsync, IO: 2, Linked: true})
	}), core.Event{IO: 1, Err: core.DiskInvalidRange}, core.Event{IO: 2, Err: core.DiskIO})
	if e := g.enters(t); len(e) != 0 {
		t.Fatalf("io_uring_enter submissions %v, want none", e)
	}
}

func TestAShortReadIsAnError(t *testing.T) {
	r, pool := newRing(t, 1<<20, 64)
	path := diskFile(t, 8)
	d, err := r.Open(path, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, 4*bs); err != nil {
		t.Fatal(err)
	}
	g := newRig(t, r, pool, d)
	b := alloc(t, pool.Pool, 4*bs)
	expect(t, g.run(t, 1, func(out []core.Action) []core.Action {
		return append(out, core.Action{Kind: core.ActDiskRead, IO: 1, LBA: 2, Buf: b, Len: 4 * bs})
	}), core.Event{IO: 1, Err: core.DiskIO})
}

func TestAFullRingDefersSubmissions(t *testing.T) {
	r, pool := newRing(t, 1<<20, 4)
	d, err := r.Open(diskFile(t, 64), false)
	if err != nil {
		t.Fatal(err)
	}
	g := newRig(t, r, pool, d)
	b := alloc(t, pool.Pool, bs)
	got := g.run(t, 64, func(out []core.Action) []core.Action {
		for i := range 64 {
			out = append(out, core.Action{Kind: core.ActDiskRead, IO: core.IOID(i + 100), LBA: core.LBA(i), Buf: b, Len: bs})
		}
		return out
	})
	seen := map[core.IOID]bool{}
	for _, ev := range got {
		if ev.Err != core.DiskOK || seen[ev.IO] {
			t.Fatalf("completion %+v", ev)
		}
		seen[ev.IO] = true
	}
	total := 0
	for _, n := range g.enters(t) {
		if n > 4 {
			t.Fatalf("an io_uring_enter submitted %d into a ring of 4", n)
		}
		total += n
	}
	if total != 64 || r.InFlight() != 0 {
		t.Fatalf("submitted %d, %d in flight", total, r.InFlight())
	}
}

func TestAFUADiskSubmitsNoFlush(t *testing.T) {
	r, pool := newRing(t, 1<<20, 64)
	d, err := r.Open(diskFile(t, 16), true)
	if err != nil {
		t.Fatal(err)
	}
	g := newRig(t, r, pool, d)
	b := alloc(t, pool.Pool, bs)
	expect(t, g.run(t, 3, func(out []core.Action) []core.Action {
		return append(out,
			core.Action{Kind: core.ActDiskWrite, IO: 1, Buf: b, Len: bs},
			core.Action{Kind: core.ActDiskFsync, IO: 2, Linked: true},
			core.Action{Kind: core.ActDiskFsync, IO: 3})
	}), core.Event{IO: 3}, core.Event{IO: 1}, core.Event{IO: 2})
	if e := g.enters(t); len(e) != 1 || e[0] != 1 {
		t.Fatalf("io_uring_enter submissions %v, want one submitting 1", e)
	}
}

func TestAFileIsWrittenInFullAndATrimZeroesOnlyTheHeader(t *testing.T) {
	r, pool := newRing(t, 1<<20, 64)
	path := diskFile(t, 1024)
	var before, after unix.Stat_t
	if err := unix.Stat(path, &before); err != nil {
		t.Fatal(err)
	}
	if before.Size != 1024*bs || before.Blocks*512 < before.Size {
		t.Fatalf("size %d, %d bytes allocated", before.Size, before.Blocks*512)
	}
	d, err := r.Open(path, false)
	if err != nil {
		t.Fatal(err)
	}
	g := newRig(t, r, pool, d)
	trimZeroesTheHeader(t, g, false)
	if err := unix.Stat(path, &after); err != nil {
		t.Fatal(err)
	}
	if after.Blocks != before.Blocks {
		t.Fatalf("a trim of a file changed its allocation from %d to %d blocks", before.Blocks, after.Blocks)
	}
}

func TestAFailedDiscardStillZeroesTheHeader(t *testing.T) {
	r, pool := newRing(t, 1<<20, 64)
	d, err := r.Open(diskFile(t, 64), false)
	if err != nil {
		t.Fatal(err)
	}
	d.t.file = false
	trimZeroesTheHeader(t, newRig(t, r, pool, d), false)
}

// trimZeroesTheHeader fills blocks 32 to 35, trims them and reads back zeros in block 32, and the
// other three as written or, when the discard may zero them, all zeros, which it reports.
func trimZeroesTheHeader(t *testing.T, g *rig, discards bool) (zeroTail bool) {
	t.Helper()
	b := alloc(t, g.pool.Pool, 4*bs)
	fill(g.pool, b, 1)
	expect(t, g.run(t, 1, func(out []core.Action) []core.Action {
		return append(out, core.Action{Kind: core.ActDiskWrite, IO: 1, LBA: 32, Buf: b, Len: 4 * bs})
	}), core.Event{IO: 1})
	before := len(g.enters(t))
	expect(t, g.run(t, 1, func(out []core.Action) []core.Action {
		return append(out, core.Action{Kind: core.ActDiskTrim, IO: 2, LBA: 32, Len: 4 * bs})
	}), core.Event{IO: 2})
	entries := 2
	if g.d.t.file {
		entries = 1
	}
	if e := g.enters(t); g.r.InFlight() != 0 || len(e) != before+1 || e[before] != entries {
		t.Fatalf("%d in flight after the trim, io_uring_enter submissions %v, want %d more submitting %d", g.r.InFlight(), e, 1, entries)
	}
	rd := alloc(t, g.pool.Pool, 4*bs)
	expect(t, g.run(t, 1, func(out []core.Action) []core.Action {
		return append(out, core.Action{Kind: core.ActDiskRead, IO: 3, LBA: 32, Buf: rd, Len: 4 * bs})
	}), core.Event{IO: 3})
	got, wrote := g.pool.Bytes(rd), g.pool.Bytes(b)
	zeroTail = discards && bytes.Equal(got[bs:], make([]byte, 3*bs))
	if !bytes.Equal(got[:bs], make([]byte, bs)) || !zeroTail && !bytes.Equal(got[bs:], wrote[bs:]) {
		t.Fatal("the trim did not zero the header block alone")
	}
	return zeroTail
}

// TestAFailedRingCompletesEveryOperationWithDiskIO fails a ring of two entries holding a read in
// the kernel, a write and its flush in the submission queue and a read that did not fit, and
// checks that only the buffers the kernel may hold are never handed out again.
func TestAFailedRingCompletesEveryOperationWithDiskIO(t *testing.T) {
	r, pool := newRing(t, 1<<20, 2)
	d, err := r.Open(diskFile(t, 16), false)
	if err != nil {
		t.Fatal(err)
	}
	b1, b2, b4, b5 := alloc(t, pool.Pool, bs), alloc(t, pool.Pool, bs), alloc(t, pool.Pool, bs), alloc(t, pool.Pool, bs)
	d.Execute(&core.Action{Kind: core.ActDiskRead, IO: 1, Buf: b1, Len: bs})
	if e := r.Submit(); e.Took != 1 {
		t.Fatalf("submitted %+v", e)
	}
	breakRing(t, r)
	d.Execute(&core.Action{Kind: core.ActDiskWrite, IO: 2, Buf: b2, Len: bs})
	d.Execute(&core.Action{Kind: core.ActDiskFsync, IO: 3, Linked: true})
	d.Execute(&core.Action{Kind: core.ActDiskRead, IO: 4, Buf: b4, Len: bs, LBA: 1})
	if e := r.Submit(); e.Took != 0 || e.Errno == 0 || r.Status().Failed == nil {
		t.Fatalf("submitted %+v into a broken ring, status %+v", e, r.Status())
	}
	t.Logf("status: %v", r.Status().Failed)
	d.Execute(&core.Action{Kind: core.ActDiskRead, IO: 5, Buf: b5, Len: bs})
	if !r.Waiting() {
		t.Fatal("failed completions wait unreported")
	}
	evs := r.Reap(nil)
	seen := map[core.IOID]int{}
	for _, ev := range evs {
		if ev.Err != core.DiskIO {
			t.Fatalf("completion %+v", ev)
		}
		seen[ev.IO]++
	}
	for io := core.IOID(1); io <= 5; io++ {
		if seen[io] != 1 {
			t.Fatalf("completions %+v", evs)
		}
	}
	if len(evs) != 5 || r.InFlight() != 0 || r.Waiting() || len(r.Reap(nil)) != 0 {
		t.Fatalf("completions %+v, %d in flight, waiting %v", evs, r.InFlight(), r.Waiting())
	}
	for _, b := range []core.IOBuf{b1, b2, b4, b5} {
		pool.Free(b)
	}
	handed := allocAll(pool)
	if handed[b1.Slot] || handed[b2.Slot] || !handed[b4.Slot] || !handed[b5.Slot] || len(handed) != 1<<20/bs-2 {
		t.Fatalf("handed out %d buffers after the failure: b1 %v, b2 %v, b4 %v, b5 %v",
			len(handed), handed[b1.Slot], handed[b2.Slot], handed[b4.Slot], handed[b5.Slot])
	}
}

func TestAFailedEnterIsTraced(t *testing.T) {
	r, pool := newRing(t, 1<<20, 64)
	d, err := r.Open(diskFile(t, 16), false)
	if err != nil {
		t.Fatal(err)
	}
	g := newRig(t, r, pool, d)
	breakRing(t, r)
	b := alloc(t, pool.Pool, bs)
	expect(t, g.run(t, 1, func(out []core.Action) []core.Action {
		return append(out, core.Action{Kind: core.ActDiskRead, IO: 1, Buf: b, Len: bs})
	}), core.Event{IO: 1, Err: core.DiskIO})
	var got []trace.Record
	for i := range g.trace.Len() {
		if rec := g.trace.At(i); rec.Name == trace.SysIOUringEnter {
			got = append(got, rec)
		}
	}
	if len(got) != 1 || got[0].Fields[0] != 1 || got[0].Fields[2] != 0 || got[0].Fields[3] != uint64(unix.EOPNOTSUPP) {
		t.Fatalf("io_uring_enter records %+v, want one submitting 1 that took 0 with EOPNOTSUPP", got)
	}
}

// cut executes a chain of two entries and lets the kernel take only the first, as it does when it
// cannot allocate a request for the second; the second must then wait in the driver.
func cut(t *testing.T, r *Ring, d *Disk, chain ...core.Action) {
	t.Helper()
	for i := range chain {
		d.Execute(&chain[i])
	}
	if n := r.fill(); n != 2 {
		t.Fatalf("copied %d entries, want a chain of 2", n)
	}
	if e := r.enter(1); e.Took != 1 || e.Errno != 0 {
		t.Fatalf("enter %+v", e)
	}
	if r.InFlight() != 1 || r.after < 0 || len(r.queue) != 1 {
		t.Fatalf("%d in flight, waiting on %d, queue %v", r.InFlight(), r.after, r.queue)
	}
	if e := r.Submit(); e.Submit != 0 {
		t.Fatalf("submitted %+v while the chain's head runs", e)
	}
}

func TestAFlushCutFromItsWriteWaitsForIt(t *testing.T) {
	r, pool := newRing(t, 1<<20, 64)
	d, err := r.Open(diskFile(t, 16), false)
	if err != nil {
		t.Fatal(err)
	}
	b := alloc(t, pool.Pool, bs)
	cut(t, r, d,
		core.Action{Kind: core.ActDiskWrite, IO: 1, Buf: b, Len: bs},
		core.Action{Kind: core.ActDiskFsync, IO: 2, Linked: true})
	expect(t, reap(t, r, 1), core.Event{IO: 1})
	if e := r.Submit(); e.Submit != 1 || e.Took != 1 {
		t.Fatalf("submitted %+v, want the flush alone", e)
	}
	expect(t, reap(t, r, 1), core.Event{IO: 2})
}

func TestAFlushCutFromAFailedWriteFails(t *testing.T) {
	r, pool := newRing(t, 1<<20, 64)
	d, err := r.open(diskFile(t, 16), unix.O_RDONLY|unix.O_DIRECT|unix.O_CLOEXEC, false)
	if err != nil {
		t.Fatal(err)
	}
	b := alloc(t, pool.Pool, bs)
	cut(t, r, d,
		core.Action{Kind: core.ActDiskWrite, IO: 1, Buf: b, Len: bs},
		core.Action{Kind: core.ActDiskFsync, IO: 2, Linked: true})
	expect(t, reap(t, r, 2), core.Event{IO: 1, Err: core.DiskIO}, core.Event{IO: 2, Err: core.DiskIO})
	if e := r.Submit(); e.Submit != 0 || len(r.queue) != 0 || r.Waiting() {
		t.Fatalf("submitted %+v, queue %v, waiting %v", e, r.queue, r.Waiting())
	}
}

func TestAHeaderWriteCutFromItsDiscardStillRuns(t *testing.T) {
	r, _ := newRing(t, 1<<20, 64)
	path := diskFile(t, 64)
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(bytes.Repeat([]byte{7}, 2*bs), 32*bs); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	d, err := r.Open(path, false)
	if err != nil {
		t.Fatal(err)
	}
	d.t.file = false
	cut(t, r, d, core.Action{Kind: core.ActDiskTrim, IO: 1, LBA: 32, Len: 2 * bs})
	if evs := reap(t, r, 0); len(evs) != 0 {
		t.Fatalf("the discard completed %+v", evs)
	}
	if e := r.Submit(); e.Submit != 1 || e.Took != 1 {
		t.Fatalf("submitted %+v, want the header write alone", e)
	}
	expect(t, reap(t, r, 1), core.Event{IO: 1})
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got[32*bs:33*bs], make([]byte, bs)) || !bytes.Equal(got[33*bs:34*bs], bytes.Repeat([]byte{7}, bs)) {
		t.Fatal("the trim did not zero the header block alone")
	}
}

func TestCloseQuarantinesWhatTheKernelHolds(t *testing.T) {
	r, pool := newRing(t, 1<<20, 2)
	d, err := r.Open(diskFile(t, 16), false)
	if err != nil {
		t.Fatal(err)
	}
	b1, b2 := alloc(t, pool.Pool, bs), alloc(t, pool.Pool, bs)
	d.Execute(&core.Action{Kind: core.ActDiskRead, IO: 1, Buf: b1, Len: bs})
	if e := r.Submit(); e.Took != 1 {
		t.Fatalf("submitted %+v", e)
	}
	d.Execute(&core.Action{Kind: core.ActDiskRead, IO: 2, Buf: b2, Len: bs, LBA: 1})
	r.Close()
	pool.Free(b1)
	pool.Free(b2)
	handed := allocAll(pool)
	if handed[b1.Slot] || !handed[b2.Slot] || len(handed) != 1<<20/bs-1 {
		t.Fatalf("handed out %d buffers after Close: b1 %v, b2 %v", len(handed), handed[b1.Slot], handed[b2.Slot])
	}
}

func TestALinkedFlushBehindAnotherDisksWriteFails(t *testing.T) {
	r, pool := newRing(t, 1<<20, 64)
	var ds [3]*Disk
	for i := range ds {
		var err error
		if ds[i], err = r.Open(diskFile(t, 16), i == 2); err != nil {
			t.Fatal(err)
		}
	}
	b := alloc(t, pool.Pool, bs)
	ds[0].Execute(&core.Action{Kind: core.ActDiskWrite, IO: 1, Buf: b, Len: bs})
	ds[1].Execute(&core.Action{Kind: core.ActDiskFsync, IO: 2, Linked: true})
	ds[0].Execute(&core.Action{Kind: core.ActDiskWrite, IO: 3, Buf: b, Len: bs, LBA: 1})
	ds[2].Execute(&core.Action{Kind: core.ActDiskFsync, IO: 4, Linked: true})
	if len(r.queue) != 2 || r.ops[r.queue[0]].sqe.flags&sqeIOLink != 0 || r.ops[r.queue[1]].hasThen {
		t.Fatalf("a flush joined a write on another disk: queue %v", r.queue)
	}
	if e := r.Submit(); e.Took != 2 {
		t.Fatalf("submitted %+v", e)
	}
	evs := reap(t, r, 4)
	// The two writes go to the kernel unlinked, so either may complete first.
	slices.SortFunc(evs[2:], func(a, b core.Event) int { return cmp.Compare(a.IO, b.IO) })
	expect(t, evs, core.Event{IO: 2, Err: core.DiskIO}, core.Event{IO: 4, Err: core.DiskIO}, core.Event{IO: 1}, core.Event{IO: 3})
}

// TestACutBetweenTwoChainsWaitsForNothing has the kernel take a write and its flush, a whole
// chain, of the three entries it was asked for: the read after them goes at the next Submit.
func TestACutBetweenTwoChainsWaitsForNothing(t *testing.T) {
	r, pool := newRing(t, 1<<20, 64)
	d, err := r.Open(diskFile(t, 16), false)
	if err != nil {
		t.Fatal(err)
	}
	b, rd := alloc(t, pool.Pool, bs), alloc(t, pool.Pool, bs)
	d.Execute(&core.Action{Kind: core.ActDiskWrite, IO: 1, Buf: b, Len: bs})
	d.Execute(&core.Action{Kind: core.ActDiskFsync, IO: 2, Linked: true})
	d.Execute(&core.Action{Kind: core.ActDiskRead, IO: 3, Buf: rd, Len: bs, LBA: 1})
	if n := r.fill(); n != 3 {
		t.Fatalf("copied %d entries, want 3", n)
	}
	if e := r.enter(2); e.Took != 2 || e.Errno != 0 {
		t.Fatalf("enter %+v", e)
	}
	if r.after >= 0 || len(r.queue) != 1 {
		t.Fatalf("waiting on %d, queue %v", r.after, r.queue)
	}
	if e := r.Submit(); e.Submit != 1 || e.Took != 1 {
		t.Fatalf("submitted %+v, want the read", e)
	}
	evs := reap(t, r, 3)
	if len(evs) != 3 {
		t.Fatalf("completions %+v", evs)
	}
	for _, ev := range evs {
		if ev.Err != core.DiskOK {
			t.Fatalf("completion %+v", ev)
		}
	}
}

// TestACutIsReadFromTheSubmissionQueueHead submits 1 of a write and its flush with the syscall's
// count thrown away, as an error replaces it: the flush still waits for its write.
func TestACutIsReadFromTheSubmissionQueueHead(t *testing.T) {
	r, pool := newRing(t, 1<<20, 64)
	d, err := r.Open(diskFile(t, 16), false)
	if err != nil {
		t.Fatal(err)
	}
	d.Execute(&core.Action{Kind: core.ActDiskWrite, IO: 1, Buf: alloc(t, pool.Pool, bs), Len: bs})
	d.Execute(&core.Action{Kind: core.ActDiskFsync, IO: 2, Linked: true})
	if n := r.fill(); n != 2 {
		t.Fatalf("copied %d entries, want 2", n)
	}
	start := *r.sqHead
	if _, _, e := unix.Syscall6(unix.SYS_IO_URING_ENTER, uintptr(r.fd), 1, 0, 0, 0, 0); e != 0 {
		t.Fatal(e)
	}
	r.requeue(start)
	if r.after < 0 || r.ops[r.after].io != 1 || len(r.queue) != 1 {
		t.Fatalf("waiting on %d, queue %v", r.after, r.queue)
	}
	expect(t, reap(t, r, 1), core.Event{IO: 1})
	r.Submit()
	expect(t, reap(t, r, 1), core.Event{IO: 2})
}

// TestEntriesTakenBackGoInFrontOfTheQueue fills a ring of 4 with 4 of 6 reads and has the kernel
// take 1: the 3 it left go back in front of the 2 that did not fit, in the order executed.
func TestEntriesTakenBackGoInFrontOfTheQueue(t *testing.T) {
	r, pool := newRing(t, 1<<20, 4)
	d, err := r.Open(diskFile(t, 16), false)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 6 {
		d.Execute(&core.Action{Kind: core.ActDiskRead, IO: core.IOID(1 + i), Buf: alloc(t, pool.Pool, bs), Len: bs, LBA: core.LBA(i)})
	}
	if n := r.fill(); n != 4 {
		t.Fatalf("copied %d entries, want 4", n)
	}
	if e := r.enter(1); e.Took != 1 || e.Errno != 0 {
		t.Fatalf("enter %+v", e)
	}
	var queued []core.IOID
	for _, i := range r.queue {
		queued = append(queued, r.ops[i].io)
	}
	if fmt.Sprint(queued) != "[2 3 4 5 6]" || r.InFlight() != 1 || r.after >= 0 {
		t.Fatalf("queue %v, %d in flight, waiting on %d", queued, r.InFlight(), r.after)
	}
	var evs []core.Event
	for len(evs) < 6 {
		r.Submit()
		evs = append(evs, reap(t, r, 1)...)
	}
	if len(evs) != 6 {
		t.Fatalf("completions %+v", evs)
	}
}

func TestOpenRefusesAFileNotInWholeBlocks(t *testing.T) {
	r, _ := newRing(t, 1<<20, 64)
	path := filepath.Join(t.TempDir(), "disk")
	if err := os.WriteFile(path, make([]byte, bs+512), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Open(path, false); !errors.Is(err, ErrNotBlocks) {
		t.Fatalf("Open = %v, want ErrNotBlocks", err)
	}
}

func TestARoundTripAllocatesNothing(t *testing.T) {
	r, pool := newRing(t, 1<<20, 64)
	d, err := r.Open(diskFile(t, 64), false)
	if err != nil {
		t.Fatal(err)
	}
	g := newRig(t, r, pool, d)
	write, single := writeAndFlush(t, g)
	g.run(t, 2, write)
	runtime.GC()
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	if n := testing.AllocsPerRun(100, func() { g.run(t, 2, write); g.run(t, 1, single) }); n != 0 {
		t.Fatalf("%v allocations per round trip", n)
	}
}

func writeAndFlush(t testing.TB, g *rig) (write, single func([]core.Action) []core.Action) {
	b := alloc(t, g.pool.Pool, bs)
	vec := []core.IOVec{{Buf: b, Len: bs}, {Buf: b, Len: bs}}
	write = func(out []core.Action) []core.Action {
		return append(out,
			core.Action{Kind: core.ActDiskWrite, IO: 1, LBA: 3, Vec: vec, Len: 2 * bs},
			core.Action{Kind: core.ActDiskFsync, IO: 2, Linked: true})
	}
	single = func(out []core.Action) []core.Action {
		return append(out, core.Action{Kind: core.ActDiskWrite, IO: 3, LBA: 7, Buf: b, Len: bs})
	}
	return write, single
}

func BenchmarkAWriteAndItsFlush(b *testing.B) {
	r, pool := newRing(b, 1<<20, 64)
	d, err := r.Open(diskFile(b, 64), false)
	if err != nil {
		b.Fatal(err)
	}
	g := newRig(b, r, pool, d)
	write, _ := writeAndFlush(b, g)
	b.ReportAllocs()
	for b.Loop() {
		g.run(b, 2, write)
	}
}
