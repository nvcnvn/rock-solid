package uring

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/loop"
	"golang.org/x/sys/unix"
)

const blkZeroOut = 0x127f

// rawDevice is the block device RS_RAW_DEVICE names, which the test overwrites; without it the
// test skips.
func rawDevice(t testing.TB) string {
	t.Helper()
	dev := os.Getenv("RS_RAW_DEVICE")
	if dev == "" {
		t.Skip("RS_RAW_DEVICE names no block device to overwrite")
	}
	return dev
}

// zeroRange writes zeros over n bytes of dev from off, as CreateFile does over a new file.
func zeroRange(t testing.TB, dev string, off, n uint64) {
	t.Helper()
	fd, err := unix.Open(dev, unix.O_RDWR|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unix.Close(fd) }()
	rng := [2]uint64{off, n}
	if _, _, e := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), blkZeroOut, uintptr(unsafe.Pointer(&rng))); e != 0 {
		t.Fatalf("BLKZEROOUT %s: %v", dev, e)
	}
}

// blockStat is field i of the block device's /sys/class/block/<name>/stat.
func blockStat(t testing.TB, dev string, i int) uint64 {
	t.Helper()
	real, err := filepath.EvalSymlinks(dev)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join("/sys/class/block", filepath.Base(real), "stat"))
	if err != nil {
		t.Fatal(err)
	}
	v, err := strconv.ParseUint(strings.Fields(string(b))[i], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

const statDiscardSectors = 13

func TestARawDeviceIsOpenedExclusively(t *testing.T) {
	dev := rawDevice(t)
	r, _ := newRing(t, 1<<20, 8)
	d, err := r.Open(dev, false)
	if err != nil {
		t.Fatal(err)
	}
	real, err := filepath.EvalSymlinks(dev)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join("/sys/class/block", filepath.Base(real), "size"))
	if err != nil {
		t.Fatal(err)
	}
	if sectors, _ := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64); d.Blocks()*bs != sectors*512 {
		t.Fatalf("%d blocks, the device holds %d sectors", d.Blocks(), sectors)
	}
	if fd, err := unix.Open(dev, unix.O_RDWR|unix.O_EXCL|unix.O_CLOEXEC, 0); !errors.Is(err, unix.EBUSY) {
		_ = unix.Close(fd)
		t.Fatalf("a second O_EXCL open: %v, want EBUSY", err)
	}
	r2, _ := newRing(t, 1<<20, 8)
	if _, err := r2.Open(dev, false); !errors.Is(err, unix.EBUSY) {
		t.Fatalf("a second ring's Open: %v, want EBUSY", err)
	}
	r.Close()
	if _, err := r2.Open(dev, false); err != nil {
		t.Fatalf("Open once the first ring is closed: %v", err)
	}
}

func TestATrimOfARawDeviceDiscardsItThenZeroesTheHeader(t *testing.T) {
	dev := rawDevice(t)
	r, pool := newRing(t, 1<<20, 64)
	d, err := r.Open(dev, false)
	if err != nil {
		t.Fatal(err)
	}
	before := blockStat(t, dev, statDiscardSectors)
	if zeroTail := trimZeroesTheHeader(t, newRig(t, r, pool, d), true); zeroTail {
		t.Log("the blocks discarded after the header read back as zeros")
	} else {
		t.Log("the blocks discarded after the header read back unchanged")
	}
	if n := blockStat(t, dev, statDiscardSectors) - before; n < 4*bs/512 {
		t.Fatalf("the device discarded %d sectors, want %d", n, 4*bs/512)
	}
}

// TestDurableWritesIntoAFileCommitNoJournal makes 1,000 4 KiB writes, each with its linked flush,
// into a file-backed disk under RS_URING_DIR, and checks that the file system's journal commits no
// more than its periodic commits (one per 5 s) across them; it logs their latency.
func TestDurableWritesIntoAFileCommitNoJournal(t *testing.T) {
	if os.Getenv("RS_URING_DIR") == "" {
		t.Skip("RS_URING_DIR names no directory on the drive to measure")
	}
	const n = 1000
	r, pool := newRing(t, 1<<20, 8)
	path := diskFile(t, n)
	d, err := r.Open(path, false)
	if err != nil {
		t.Fatal(err)
	}
	g := newRig(t, r, pool, d)
	b := alloc(t, pool.Pool, bs)
	fill(pool, b, 3)
	commits := jbd2Commits(t, path)
	lat := make([]time.Duration, n)
	start := time.Now()
	for i := range n {
		at := time.Now()
		expect(t, g.run(t, 2, func(out []core.Action) []core.Action {
			return append(out,
				core.Action{Kind: core.ActDiskWrite, IO: 1, LBA: core.LBA(i), Buf: b, Len: bs},
				core.Action{Kind: core.ActDiskFsync, IO: 2, Linked: true})
		}), core.Event{IO: 1}, core.Event{IO: 2})
		lat[i] = time.Since(at)
	}
	took := time.Since(start)
	slices.Sort(lat)
	t.Logf("%d durable 4 KiB writes in %v: p50 %v, p90 %v, p99 %v, max %v", n, took, lat[n/2], lat[n*9/10], lat[n*99/100], lat[n-1])
	if commits < 0 {
		t.Log("no jbd2 journal behind the file")
		return
	}
	got := jbd2Commits(t, path) - commits
	t.Logf("the journal committed %d times across them", got)
	if allowed := int64(took/(5*time.Second)) + 1; got > allowed {
		t.Fatalf("the journal committed %d times in %v, more than its periodic commits", got, took)
	}
}

// jbd2Commits is the transactions the jbd2 journal of path's file system has committed, or -1.
func jbd2Commits(t *testing.T, path string) int64 {
	t.Helper()
	var st unix.Stat_t
	if err := unix.Stat(path, &st); err != nil {
		t.Fatal(err)
	}
	dev, err := filepath.EvalSymlinks(fmt.Sprintf("/sys/dev/block/%d:%d", unix.Major(st.Dev), unix.Minor(st.Dev)))
	if err != nil {
		return -1
	}
	b, err := os.ReadFile(fmt.Sprintf("/proc/fs/jbd2/%s-8/info", filepath.Base(dev)))
	if err != nil {
		return -1
	}
	n, err := strconv.ParseInt(strings.Fields(string(b))[0], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

const benchWindow = 1 << 30

// BenchmarkRawDevice runs 4 KiB O_DIRECT reads and writes at random offsets in the first GiB of
// RS_RAW_DEVICE from random slots of a 64 MiB pool, at queue depths 1 and 32, plain, with a fixed
// buffer, and with a fixed buffer and file, as the registered buffers spike did; cpu-ns/op is the
// process's user and system time per I/O. register is one registration of a 1 GiB pool.
func BenchmarkRawDevice(b *testing.B) {
	dev := rawDevice(b)
	prefill(b, dev)
	for _, kind := range []core.ActionKind{core.ActDiskRead, core.ActDiskWrite} {
		for _, qd := range []int{1, 32} {
			for _, mode := range []string{"plain", "buffer", "buffer+file"} {
				name := fmt.Sprintf("%s/qd%d/%s", map[core.ActionKind]string{core.ActDiskRead: "read", core.ActDiskWrite: "write"}[kind], qd, mode)
				b.Run(name, func(b *testing.B) { benchIO(b, dev, kind, qd, mode != "plain", mode == "buffer+file") })
			}
		}
	}
	b.Run("register/1GiB", func(b *testing.B) {
		pool, err := loop.NewPool(1<<30, 0)
		if err != nil {
			b.Fatal(err)
		}
		defer func() { _ = pool.Close() }()
		for b.Loop() {
			r, err := New(pool, 8)
			if err != nil || !r.Status().FixedBuffers {
				b.Fatalf("New: %v, %+v", err, r.Status())
			}
			r.Close()
		}
	})
}

// prefill writes the benchmark's window once, so reads find written blocks.
func prefill(b *testing.B, dev string) {
	fd, err := unix.Open(dev, unix.O_RDWR|unix.O_DIRECT|unix.O_CLOEXEC, 0)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = unix.Close(fd) }()
	buf, err := unix.Mmap(-1, 0, 1<<20, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANON)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = unix.Munmap(buf) }()
	core.NewRand(1).Fill(buf)
	for off := int64(0); off < benchWindow; off += int64(len(buf)) {
		if _, err := unix.Pwrite(fd, buf, off); err != nil {
			b.Fatal(err)
		}
	}
}

func benchIO(b *testing.B, dev string, kind core.ActionKind, qd int, fixedBuf, fixedFile bool) {
	pool, err := loop.NewPool(64<<20, 0)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = pool.Close() }()
	r, err := New(pool, 64)
	if err != nil {
		b.Fatal(err)
	}
	defer r.Close()
	if fixedBuf && !r.status.FixedBuffers || fixedFile && !r.status.FixedFiles {
		b.Skipf("nothing to measure: %+v", r.status)
	}
	r.regions.fixed, r.status.FixedFiles = fixedBuf, fixedFile
	d, err := r.Open(dev, false)
	if err != nil {
		b.Fatal(err)
	}
	if d.Fixed() != fixedFile {
		b.Fatalf("fixed file %v", d.Fixed())
	}
	bufs := make([]core.IOBuf, 64<<20/bs)
	for i := range bufs {
		bufs[i] = alloc(b, pool.Pool, bs)
	}
	rnd := core.NewRand(2)
	evs := make([]core.Event, 0, 64)
	var drain [8]byte
	var before, after unix.Rusage
	_ = unix.Getrusage(unix.RUSAGE_SELF, &before)
	b.ReportAllocs()
	b.ResetTimer()
	for issued, done := 0, 0; done < b.N; {
		for ; issued < b.N && issued-done < qd; issued++ {
			d.Execute(&core.Action{Kind: kind, IO: core.IOID(issued), LBA: core.LBA(rnd.Below(benchWindow / bs)), Buf: bufs[rnd.Below(uint64(len(bufs)))], Len: bs})
		}
		r.Submit()
		if evs = r.Reap(evs[:0]); len(evs) == 0 {
			_, _ = unix.Poll([]unix.PollFd{{Fd: int32(r.Fd()), Events: unix.POLLIN}}, -1)
			_, _ = unix.Read(r.Fd(), drain[:])
			continue
		}
		for _, ev := range evs {
			if ev.Err != core.DiskOK {
				b.Fatalf("completion %+v", ev)
			}
		}
		done += len(evs)
	}
	b.StopTimer()
	_ = unix.Getrusage(unix.RUSAGE_SELF, &after)
	cpu := time.Duration(after.Utime.Nano() + after.Stime.Nano() - before.Utime.Nano() - before.Stime.Nano())
	b.ReportMetric(float64(cpu.Nanoseconds())/float64(b.N), "cpu-ns/op")
}

// dmRig is the crash test's disk on RS_RAW_DEVICE under device-mapper: /dev/mapper/rs-crash, a
// window of the raw device's first bytes behind one of the targets below. Its log-writes target
// logs every write and flush the window gets, with its data, into a second window, and cut
// replays onto a copy of the window kept in a third only what was flushed: the window as a power
// cut that loses every write not yet flushed leaves it.
type dmRig struct {
	t       testing.TB
	raw     string
	target  string
	sectors uint64
	// writes and flushes replayed, and writes completed but never flushed, which cut dropped.
	replayed, flushes, dropped int
}

const (
	dmName     = "rs-crash"
	dmPath     = "/dev/mapper/" + dmName
	dmBaseOff  = 1 << 30
	dmLogOff   = 2 << 30
	dmLogBytes = 4 << 30
)

func newDMRig(t testing.TB, raw, target string, bytes uint64) *dmRig {
	m := &dmRig{t: t, raw: raw, target: target, sectors: bytes / 512}
	m.remove()
	t.Cleanup(m.remove)
	zeroRange(t, raw, 0, bytes)
	if target == "log-writes" {
		m.dmsetup("create", "rs-data", "--table", m.table("linear"))
		m.dmsetup("create", "rs-log", "--table", fmt.Sprintf("0 %d linear %s %d", dmLogBytes/512, raw, dmLogOff/512))
		return m
	}
	if target == "error" || target == "flakey" {
		target = "linear"
	}
	m.dmsetup("create", dmName, "--table", m.table(target))
	return m
}

func (m *dmRig) table(target string) string {
	switch target {
	case "linear":
		return fmt.Sprintf("0 %d linear %s 0", m.sectors, m.raw)
	case "error":
		return fmt.Sprintf("0 %d error", m.sectors)
	case "flakey":
		return fmt.Sprintf("0 %d flakey %s 0 0 1 1 drop_writes", m.sectors, m.raw)
	case "delay":
		return fmt.Sprintf("0 %d delay %s 0 5", m.sectors, m.raw)
	case "log-writes":
		return fmt.Sprintf("0 %d log-writes /dev/mapper/rs-data /dev/mapper/rs-log", m.sectors)
	}
	m.t.Fatalf("RS_CRASH_TARGET %q: want linear, error, flakey, delay or log-writes", target)
	return ""
}

func (m *dmRig) dmsetup(args ...string) {
	m.t.Helper()
	if out, err := exec.Command("dmsetup", args...).CombinedOutput(); err != nil {
		m.t.Fatalf("dmsetup %s: %v\n%s", strings.Join(args, " "), err, out)
	}
}

func (m *dmRig) remove() {
	for _, n := range []string{dmName, "rs-data", "rs-log"} {
		_ = exec.Command("dmsetup", "remove", "--retry", n).Run()
	}
}

// swap puts target behind the window at once, every write in flight finished first.
func (m *dmRig) swap(target string) {
	m.dmsetup("suspend", dmName)
	m.dmsetup("load", dmName, "--table", m.table(target))
	m.dmsetup("resume", dmName)
}

// begin starts a log-writes cycle: it keeps a copy of the window and logs into an empty log.
func (m *dmRig) begin() {
	if m.target != "log-writes" {
		return
	}
	m.copyWindow(0, dmBaseOff)
	zeroRange(m.t, m.raw, dmLogOff, bs)
	m.dmsetup("create", dmName, "--table", m.table("log-writes"))
}

// cut ends a log-writes cycle once nothing has the window open: the window becomes the copy
// begin kept with the writes logged before the last flush on it.
func (m *dmRig) cut() {
	if m.target != "log-writes" {
		return
	}
	m.dmsetup("remove", "--retry", dmName)
	fd := m.open()
	defer func() { _ = unix.Close(fd) }()
	buf, err := unix.Mmap(-1, 0, 64<<20, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANON)
	if err != nil {
		m.t.Fatal(err)
	}
	defer func() { _ = unix.Munmap(buf) }()
	m.pread(fd, buf[:512], dmLogOff)
	if magic := binary.LittleEndian.Uint64(buf); magic != 0x6a736677736872 {
		m.t.Fatalf("log-writes super magic %#x", magic)
	}
	type entry struct{ sector, sectors, flags, at uint64 }
	n := binary.LittleEndian.Uint64(buf[16:])
	entries := make([]entry, 0, n)
	last := -1
	at := uint64(dmLogOff + 512)
	for range n {
		m.pread(fd, buf[:512], int64(at))
		e := entry{binary.LittleEndian.Uint64(buf), binary.LittleEndian.Uint64(buf[8:]), binary.LittleEndian.Uint64(buf[16:]), at + 512}
		at += 512
		if e.flags&logDiscard == 0 {
			at += e.sectors * 512
		}
		if e.flags&logFlush != 0 {
			last = len(entries)
			m.flushes++
		}
		entries = append(entries, e)
	}
	if len(entries) == 0 || entries[len(entries)-1].flags&logMark == 0 {
		m.t.Fatalf("the log of %d entries does not end with dm-log-writes' end mark: it ran out of room", n)
	}
	m.copyWindow(dmBaseOff, 0)
	for i, e := range entries {
		if e.flags&(logDiscard|logMark) != 0 || e.sectors == 0 {
			continue
		}
		if i > last && e.flags&logFUA == 0 {
			m.dropped++
			continue
		}
		if e.sectors*512 > uint64(len(buf)) {
			m.t.Fatalf("a logged write of %d sectors", e.sectors)
		}
		data := buf[:e.sectors*512]
		m.pread(fd, data, int64(e.at))
		if _, err := unix.Pwrite(fd, data, int64(e.sector*512)); err != nil {
			m.t.Fatal(err)
		}
		m.replayed++
	}
}

const (
	logFlush   = 1 << 0
	logFUA     = 1 << 1
	logDiscard = 1 << 2
	logMark    = 1 << 3
)

// open opens the raw device for direct I/O, past the page cache dm-log-writes writes under.
func (m *dmRig) open() int {
	fd, err := unix.Open(m.raw, unix.O_RDWR|unix.O_DIRECT|unix.O_CLOEXEC, 0)
	if err != nil {
		m.t.Fatal(err)
	}
	return fd
}

func (m *dmRig) pread(fd int, b []byte, off int64) {
	if n, err := unix.Pread(fd, b, off); err != nil || n != len(b) {
		m.t.Fatalf("reading %d bytes at %d: %d, %v", len(b), off, n, err)
	}
}

func (m *dmRig) copyWindow(from, to int64) {
	fd := m.open()
	defer func() { _ = unix.Close(fd) }()
	buf, err := unix.Mmap(-1, 0, 4<<20, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANON)
	if err != nil {
		m.t.Fatal(err)
	}
	defer func() { _ = unix.Munmap(buf) }()
	for off := int64(0); off < int64(m.sectors*512); off += int64(len(buf)) {
		m.pread(fd, buf, from+off)
		if _, err := unix.Pwrite(fd, buf, to+off); err != nil {
			m.t.Fatal(err)
		}
	}
}
