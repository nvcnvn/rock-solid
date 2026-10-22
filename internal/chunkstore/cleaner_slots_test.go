package chunkstore

import (
	"errors"
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
