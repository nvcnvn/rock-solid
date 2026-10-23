package chunkstore

import (
	"bytes"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// The stale-header crash orders of the log segments study (docs/test-scenarios.md, Cleaner:
// Freed segments and NextSeq), staged by the cleaner itself: a store on 16-block segments, a
// checkpoint whenever 40 blocks follow the manifest, overwrites of block 0 of 16 chunks, and a
// pass over every chunk segment whose records all precede the replay point. Allocation takes the
// lowest free segment, so the segments opened after the pass take the lower victims first, and
// an order watches s, the highest, or the lowest when it needs s opened again early.
type stage struct {
	h       *harness
	s       uint32
	seq     uint64
	freeing uint64
	begun   bool
	was     bool
	n       int
}

func newStage(t *testing.T, noTrim, lowest bool) *stage {
	t.Helper()
	cfg := cleanConfig()
	cfg.Log.MaxReplayBytes = 40 * core.BlockSize
	cfg.Log.NoTrim = noTrim
	cfg.CleanFree = -1
	h := newHarnessOn(t, cfg, cleanGeo)
	h.oracle = sim.NewOracle[writtenRecord](0, newStoreModel())
	st := &stage{h: h}
	for range 150 {
		st.write()
		h.settle()
	}
	log := h.s.Log()
	var victims []uint32
	for seg := uint32(1); seg < cleanGeo.SegmentCount; seg++ {
		if _, stream := log.Usage(seg); stream != disklog.Meta && log.Cleanable(seg) {
			victims = append(victims, seg)
		}
	}
	if len(victims) < 6 {
		t.Fatalf("cleanable chunk segments %v", victims)
	}
	st.s = victims[len(victims)-1]
	if lowest {
		st.s = victims[0]
	}
	st.seq = h.seqOf[st.s]
	h.s.cl.passes = append(h.s.cl.passes, &cleanPass{victims: victims})
	return st
}

// write overwrites block 0 of the next of 16 chunks, as the study's log does.
func (st *stage) write() {
	st.n++
	st.h.write(chunk(uint32(st.n%16)), 0, fill(core.BlockSize, byte(st.n)), 0)
}

func (st *stage) free() bool {
	u, _ := st.h.s.Log().Usage(st.s)
	return u.State == disklog.SegmentFree
}

func (st *stage) reused() bool { return st.h.seqOf[st.s] != st.seq }

// until steps the store, writing on whenever it is idle, until cond holds. It notes the freeing
// manifest: the first durable once a checkpoint begun after s was freed ends.
func (st *stage) until(cond func() bool) {
	st.h.t.Helper()
	h := st.h
	for i := 0; !cond(); i++ {
		if i > 20000 {
			h.t.Fatal("the crash order never came")
		}
		freed := st.free() || st.reused()
		if freed && h.s.checkpointing && !st.was {
			st.begun = true
		}
		st.was = h.s.checkpointing
		before := h.s.Log().Manifest()
		h.tick(100)
		if !h.step() {
			st.write()
			continue
		}
		if st.begun && st.freeing == 0 && h.s.Log().Manifest() != before {
			st.freeing = h.s.Log().Manifest().LSN
		}
	}
}

func (st *stage) named() bool { return st.freeing != 0 && st.h.s.Log().NamedLSN() >= st.freeing }

// crash crashes the disk with the unflushed blocks land chooses landing and recovers, checking the
// store's invariants. It returns the reads recovery issued.
func (st *stage) crash(land func(core.LBA) bool) []core.Action {
	st.h.t.Helper()
	h := st.h
	h.disk.Crash(func(_ int, lba core.LBA) bool { return land(lba) })
	mark := len(h.acts)
	h.recoverStore(disklog.ModeOpen)
	return h.acts[mark:]
}

func none(core.LBA) bool { return false }

// readInside reports whether reads read a block of segment s past its header and block 1.
func readInside(reads []core.Action, s uint32) bool {
	lo, hi := cleanGeo.LBA(s, 2), cleanGeo.LBA(s+1, 0)
	for _, a := range reads {
		if a.Kind == core.ActDiskRead && a.LBA < hi && a.LBA+core.LBA(a.Len/core.BlockSize) > lo {
			return true
		}
	}
	return false
}

func (st *stage) usage() disklog.SegmentUsage {
	u, _ := st.h.s.Log().Usage(st.s)
	return u
}

// Freed in memory, the freeing manifest not durable: s is used with the older manifest's row,
// and the copy records replay into what its runs still map there.
func TestCrashOrderFreedInMemory(t *testing.T) {
	st := newStage(t, true, false)
	st.until(st.free)
	st.crash(none)
	if u := st.usage(); u.State != disklog.SegmentUsed {
		t.Fatalf("segment %d is %+v", st.s, u)
	}
}

// The freeing manifest durable, no segment header naming it yet: s is used, and the cleaner frees
// it again.
func TestCrashOrderFreeingManifestUnnamed(t *testing.T) {
	st := newStage(t, true, false)
	st.until(func() bool { return st.freeing != 0 && !st.named() && !st.reused() })
	st.crash(none)
	if u := st.usage(); u.State != disklog.SegmentUsed {
		t.Fatalf("segment %d is %+v", st.s, u)
	}
	st.h.s.cl.passes = append(st.h.s.cl.passes, &cleanPass{victims: []uint32{st.s}})
	st.h.clean(-1)
	if u := st.usage(); u.State != disklog.SegmentFree && !st.reused() {
		t.Fatalf("the cleaner left segment %d %+v", st.s, u)
	}
}

// A header naming the freeing manifest durable, s not reused and never trimmed, as on a disk that
// is a file: s is free, and recovery reads its header and block 1 and nothing more, at this start
// and the next.
func TestCrashOrderFreedAndNamed(t *testing.T) {
	st := newStage(t, true, false)
	st.until(func() bool { return st.named() && !st.reused() })
	for range 2 {
		if reads := st.crash(none); st.usage().State != disklog.SegmentFree || readInside(reads, st.s) {
			t.Fatalf("segment %d is %+v, read in full %v", st.s, st.usage(), readInside(reads, st.s))
		}
	}
}

// As above, the cleaner's TRIM run, reading back as zeros: s is free. Reading back as ones, its
// header is damage: recovery reads s in full, finds no record of this log past the replay point,
// and frees it, at every start until it is reused.
func TestCrashOrderTrimmed(t *testing.T) {
	for _, pattern := range []byte{0, 0xff} {
		st := newStage(t, false, false)
		st.until(func() bool {
			return slices.ContainsFunc(st.h.trims, func(a core.Action) bool { return a.LBA == cleanGeo.LBA(st.s, 0) }) && !st.reused()
		})
		st.h.disk.Put(cleanGeo.LBA(st.s, 0), bytes.Repeat([]byte{pattern}, int(cleanGeo.SegmentBlocks)*core.BlockSize))
		for range 2 {
			reads := st.crash(none)
			if st.usage().State != disklog.SegmentFree || readInside(reads, st.s) != (pattern != 0) {
				t.Fatalf("trimmed to %#x: segment %d is %+v, read in full %v", pattern, st.s, st.usage(), readInside(reads, st.s))
			}
		}
	}
}

// Reused before any header names the freeing manifest, its new header and records landed: s is
// used, its row from replay, the older manifest's row for it dropped.
func TestCrashOrderReusedBeforeNamed(t *testing.T) {
	st := newStage(t, true, true)
	st.until(func() bool { return st.reused() && !st.named() && st.usage().State == disklog.SegmentUsed })
	st.h.settle()
	newest := st.usage().Newest
	st.crash(none)
	if u := st.usage(); u.State != disklog.SegmentUsed || u.Newest != newest {
		t.Fatalf("segment %d is %+v, want used with newest LSN %d from its new records", st.s, u, newest)
	}
}

// Reused after a header names the freeing manifest: s is used.
func TestCrashOrderReusedAfterNamed(t *testing.T) {
	st := newStage(t, true, false)
	st.until(func() bool { return st.named() && st.reused() })
	st.h.settle()
	st.crash(none)
	if u := st.usage(); u.State != disklog.SegmentUsed {
		t.Fatalf("segment %d is %+v", st.s, u)
	}
}

// Reused by a round in flight: its record at block 1 landed, its header did not, with a header
// naming the freeing manifest durable or not yet. The record is erased, s is free or used under
// the older manifest's listing, and the next segment opened takes a sequence past the record's.
func TestCrashOrderStrayRecordUnderAStaleHeader(t *testing.T) {
	for _, named := range []bool{false, true} {
		st := newStage(t, true, !named)
		h := st.h
		st.until(func() bool {
			return st.named() == named && len(h.queue) > 0 && h.queue[0].Kind == core.ActDiskWrite && h.queue[0].LBA == cleanGeo.LBA(st.s, 0)
		})
		h.step()
		stray := h.seqOf[st.s]
		st.crash(func(lba core.LBA) bool { return lba > cleanGeo.LBA(st.s, 0) && lba < cleanGeo.LBA(st.s+1, 0) })
		if !bytes.Equal(h.disk.Read(cleanGeo.LBA(st.s, 1), 1), make([]byte, core.BlockSize)) {
			t.Fatalf("named %v: the record at segment %d block 1 was not erased", named, st.s)
		}
		if u := st.usage(); named && u.State != disklog.SegmentFree || !named && u.State != disklog.SegmentUsed {
			t.Fatalf("named %v: segment %d is %+v", named, st.s, u)
		}
		before := map[uint32]uint64{}
		for k, v := range h.seqOf {
			before[k] = v
		}
		for range 40 {
			st.write()
			h.settle()
		}
		for seg, seq := range h.seqOf {
			if before[seg] != seq && seq <= stray {
				t.Fatalf("named %v: segment %d opened at sequence %d, at or below the erased record's %d", named, seg, seq, stray)
			}
		}
	}
}
