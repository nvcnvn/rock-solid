package main

import (
	"runtime"
	"syscall"
	"unsafe"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/lsm"
	"github.com/nvcnvn/rock-solid/spikes/lsm-runs/cix"
	"github.com/nvcnvn/rock-solid/spikes/lsm-runs/lsmx"
)

const maxPiece = 1 << 20

// flusher is the flush half of a tree, real or the spike's copy.
type flusher interface {
	BeginFlush() (bool, error)
	NextPiece(buf []byte) (int, bool, error)
	PieceDurable(ref disklog.BlobRef) error
	CheckpointDone() ([]disklog.BlobRef, error)
	Runs() int
	RunBytes() int
}

// written counts the bytes of run pieces a flush writes: all of them, and the frozen memtable's.
type written struct {
	all, mem int
	next     uint64
	buf      []byte
}

// checkpoint writes every piece the tree's flush asks for, each durable at once.
func (w *written) checkpoint(t flusher) {
	if w.buf == nil {
		w.buf = make([]byte, maxPiece)
	}
	froze, err := t.BeginFlush()
	must(err)
	var runBytes []int
	cur := 0
	for {
		n, root, err := t.NextPiece(w.buf)
		must(err)
		if n == 0 {
			break
		}
		w.next++
		cur += n
		must(t.PieceDurable(disklog.BlobRef{Segment: 1, Block: uint32(w.next), LSN: w.next}))
		if root {
			runBytes = append(runBytes, cur)
			cur = 0
		}
	}
	_, err = t.CheckpointDone()
	must(err)
	for _, b := range runBytes {
		w.all += b
	}
	if froze && len(runBytes) > 0 {
		w.mem += runBytes[len(runBytes)-1]
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func compactReal(t *lsm.Tree) int {
	n := 0
	for c := t.PickCompaction(); c != nil; c = t.PickCompaction() {
		c.Run()
		must(t.Install(c))
		n++
	}
	return n
}

func compactX(t *lsmx.Tree) int {
	n := 0
	for c := t.PickCompaction(); c != nil; c = t.PickCompaction() {
		c.Run()
		must(t.Install(c))
		n++
	}
	return n
}

// env is a chunk index over a tree, real or the spike's copy, behind closures.
type env struct {
	name     string
	apply    func(ev *disklog.Event)
	lookup   func(c core.ChunkID, b uint16) bool
	flush    func()
	compact  func() int
	runs     func() int
	runBytes func() int
	steps    func() int
	stats    func() lsmx.Stats
	w        written
}

func realEnv() *env {
	tree, err := lsm.New(lsm.Config{MaxPiece: maxPiece})
	must(err)
	x := chunkstore.NewIndex(tree)
	e := &env{name: "internal"}
	e.apply = func(ev *disklog.Event) { must(x.Apply(ev)) }
	e.lookup = func(c core.ChunkID, b uint16) bool { _, ok := x.Lookup(1, c, b); return ok }
	e.flush = func() { e.w.checkpoint(tree) }
	e.compact = func() int { return compactReal(tree) }
	e.runs, e.runBytes = tree.Runs, tree.RunBytes
	e.steps = func() int { return 0 }
	e.stats = func() lsmx.Stats { return lsmx.Stats{} }
	return e
}

func xEnv(name string, cfg lsmx.Config, rangeTrim int) *env {
	cfg.MaxPiece = maxPiece
	tree, err := lsmx.New(cfg)
	must(err)
	x := cix.NewIndex(tree)
	x.RangeTrim = rangeTrim
	e := &env{name: name}
	e.apply = func(ev *disklog.Event) { must(x.Apply(ev)) }
	e.lookup = func(c core.ChunkID, b uint16) bool { _, ok := x.Lookup(1, c, b); return ok }
	e.flush = func() { e.w.checkpoint(tree) }
	e.compact = func() int { return compactX(tree) }
	e.runs, e.runBytes = tree.Runs, tree.RunBytes
	e.steps = func() int { return tree.Stats.Steps }
	e.stats = func() lsmx.Stats { return tree.Stats }
	return e
}

// disk hands out record addresses in order, one header block per record.
type disk struct{ lba core.LBA }

func (d *disk) record(entries ...disklog.Entry) *disklog.Event {
	ev := &disklog.Event{Kind: disklog.Durable, LBA: d.lba + 1, HeaderBlocks: 1, Entries: entries}
	blocks := 0
	for i := range entries {
		ev.Payloads = append(ev.Payloads, disklog.PayloadOffset(entries, i))
		if entries[i].Kind == disklog.EntryWrite {
			blocks += int(entries[i].Count)
		}
	}
	d.lba += core.LBA(1 + blocks)
	return ev
}

func chunk(i uint32) core.ChunkID { return core.ChunkID{Volume: 1, Inode: 2, Index: i} }

// writeWhole writes chunk c whole in records of perRecord blocks.
func writeWhole(e *env, d *disk, c core.ChunkID, version uint64, perRecord int) {
	for first := 0; first < core.BlocksPerChunk; first += perRecord {
		e.apply(d.record(disklog.WriteEntry(1, c, version, uint16(first), uint16(perRecord))))
	}
}

// cost is the CPU time and allocations per call of fn, run n times on a locked thread.
type cost struct {
	ns     float64
	allocs float64
}

func measure(n int, fn func(i int)) cost {
	var ms0, ms1 runtime.MemStats
	runtime.ReadMemStats(&ms0)
	t0 := threadCPU()
	for i := range n {
		fn(i)
	}
	t1 := threadCPU()
	runtime.ReadMemStats(&ms1)
	return cost{ns: float64(t1-t0) / float64(n), allocs: float64(ms1.Mallocs-ms0.Mallocs) / float64(n)}
}

func threadCPU() int64 {
	var ts syscall.Timespec
	if _, _, errno := syscall.Syscall(syscall.SYS_CLOCK_GETTIME, 3, uintptr(unsafe.Pointer(&ts)), 0); errno != 0 {
		panic(errno)
	}
	return ts.Nano()
}

// The harness runs on one goroutine locked to its thread, so the thread's CPU clock is its own.
func init() { runtime.LockOSThread() }
