package chunkstore

import (
	"bytes"
	"errors"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// Nine whole chunks whose record headers lie among overwrites of a small chunk, then new data
// until the disk is full: the segments holding the headers, which no copy frees, never crowd
// out the victims that free space, so a write fails with NoSpace only once no segment without
// a slot header would gain space.
func TestSlotHeadersDoNotStarveTheCleaner(t *testing.T) {
	cfg := cleanConfig()
	cfg.Log.MaxReplayBytes = 64 * core.BlockSize
	geo := slotGeometry
	h := newHarnessOn(t, cfg, geo)
	h.merging = true
	for k := range 9 {
		h.sync(h.write(core.ChunkID{Volume: 3, Inode: 1, Index: uint32(k)}, 0, pattern(uint64(k), core.ChunkSize), 0))
		for i := range 70 {
			id := h.write(chunk(1), 0, fill(64*core.BlockSize, byte(i)), 0)
			h.settle()
			h.ok(id)
		}
	}
	log := h.s.Log()
	for i := range 3000 {
		id := h.write(chunk(1), 0, fill(64*core.BlockSize, byte(i)), 0)
		h.settle()
		if err := h.reply(id).Err; err != nil {
			t.Fatalf("overwrite %d: %v; stats %+v; victims %v", i, err, h.s.CleanStats(), h.s.chooseVictims())
		}
	}
	// New data in 64-block writes, every other one later overwritten, so that victims are half live.
	n := 0
	for i := 0; ; i++ {
		c, off := chunk(uint32(100+i/16)), uint32(i%16)*64*core.BlockSize
		id := h.write(c, off, fill(64*core.BlockSize, byte(i)), 0)
		h.settle()
		if err := h.reply(id).Err; err != nil {
			if !errors.Is(err, disklog.ErrNoSpace) {
				t.Fatal(err)
			}
			break
		}
		if i%2 == 1 {
			id := h.write(c, off-64*core.BlockSize, fill(64*core.BlockSize, byte(i+1)), 0)
			h.settle()
			if err := h.reply(id).Err; err != nil {
				if !errors.Is(err, disklog.ErrNoSpace) {
					t.Fatal(err)
				}
				break
			}
		}
		n++
	}
	gain := 0
	for s := uint32(1); s < geo.SegmentCount; s++ {
		u, st := log.Usage(s)
		if st != disklog.Meta && log.Cleanable(s) && h.s.Index().SlotHeld(s) == 0 && log.CopyCost(uint64(u.Live))+1 < uint64(geo.SegmentBlocks) {
			gain++
		}
	}
	if gain > 0 {
		t.Fatalf("writes failed with NoSpace after %d while %d segments with no slot header would gain space", n, gain)
	}
}

// Whole chunks written among overwrites of a small chunk leave their record headers together, in
// the slot lane's segment, which holds nothing else; the overwrites' segments then hold no slot
// header and the cleaner frees them.
func TestSlotHeadersShareSegmentsOnlyWithSlotHeaders(t *testing.T) {
	cfg := cleanConfig()
	cfg.Log.MaxReplayBytes = 64 * core.BlockSize
	geo := slotGeometry
	h := newHarnessOn(t, cfg, geo)
	h.merging = true
	whole := make([]core.ChunkID, 6)
	for k := range whole {
		whole[k] = core.ChunkID{Volume: 3, Inode: 1, Index: uint32(k)}
		h.sync(h.write(whole[k], 0, pattern(uint64(k), core.ChunkSize), 0))
		for i := range 70 {
			id := h.write(chunk(1), 0, fill(64*core.BlockSize, byte(i)), 0)
			h.settle()
			h.ok(id)
		}
	}
	var segs []uint32
	for _, c := range whole {
		e, _ := h.s.Index().Lookup(testPool, c, 0)
		if seg := uint32(uint64(e.Header) / uint64(geo.SegmentBlocks)); !slices.Contains(segs, seg) {
			segs = append(segs, seg)
		}
	}
	if len(segs) != 1 {
		t.Fatalf("the whole chunks' headers lie in segments %v, want one", segs)
	}
	seg := segs[0]
	sh, err := disklog.DecodeSegmentHeader(h.disk.Read(geo.LBA(seg, 0), 1))
	if err != nil || sh.Stream != disklog.Hot || sh.Lane != 1 {
		t.Fatalf("segment %d of the headers: %+v, %v; want the hot stream's lane 1", seg, sh, err)
	}
	u, _ := h.s.Log().Usage(seg)
	if held := h.s.Index().SlotHeld(seg); u.Live != held || held != uint32(len(whole))*core.BlocksPerChunk {
		t.Fatalf("segment %d of the headers: %d live, %d slot-held", seg, u.Live, held)
	}
	h.clean(1000)
	for s := uint32(1); s < geo.SegmentCount; s++ {
		if s != seg && h.s.Index().SlotHeld(s) > 0 {
			t.Fatalf("segment %d holds %d slot blocks' headers", s, h.s.Index().SlotHeld(s))
		}
	}
	if h.s.CleanStats().Freed == 0 {
		t.Fatal("the cleaner freed nothing")
	}
	for k, c := range whole {
		if got := h.readNow(c, 0, core.ChunkSize); !bytes.Equal(got, pattern(uint64(k), core.ChunkSize)) {
			t.Fatalf("whole chunk %d does not read back after cleaning", k)
		}
	}
}
