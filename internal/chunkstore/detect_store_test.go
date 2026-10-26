package chunkstore

import (
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// The store ages its oldest unfinished read, write or flush on its own clock, from when it issued it, and leaves trims out.
func TestOldestAgesTheOldestUnfinishedReadWriteOrFlush(t *testing.T) {
	h := newHarness(t, testConfig())
	h.settle()
	if got := h.s.Oldest(h.ctx.Now + 5_000_000); got != 0 {
		t.Fatalf("idle store: oldest %d", got)
	}
	issued := h.ctx.Now
	h.write(chunkC, 0, fill(core.BlockSize, 0x11), 0)
	if len(h.queue) == 0 {
		t.Fatal("the write issued no disk action")
	}
	if got := h.s.Oldest(issued + 1_200_000); got != 1_200_000 {
		t.Fatalf("a write issued 1.2 s ago: oldest %d", got)
	}
	h.settle()
	if got := h.s.Oldest(h.ctx.Now + 1_200_000); got != 0 {
		t.Fatalf("every action done: oldest %d", got)
	}

	h.s.lout.Actions = append(h.s.lout.Actions, core.Action{Kind: core.ActDiskTrim, IO: 1 << 40})
	h.s.pump(&h.ctx, &h.out)
	if got := h.s.Oldest(h.ctx.Now + 3_000_000); got != 0 {
		t.Fatalf("a trim unfinished for 3 s: oldest %d", got)
	}
}
