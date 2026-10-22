package chunkstore

import (
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// The share of the cleaner's rate the bucket hands out, by free segments outside the reserve
// against the default clean_free of 16: nothing charged above 8, and from 1 at 8 to 0.5 at none
// in equal steps.
func TestAdmitScaleByFreeSegments(t *testing.T) {
	for _, c := range []struct {
		free    int
		scale   float64
		charged bool
	}{
		{20, 1, false}, {16, 1, false}, {15, 1, false}, {14, 1, false}, {9, 1, false},
		{8, 1, true}, {4, 0.75, true}, {1, 0.5625, true}, {0, 0.5, true}, {-3, 0.5, true},
	} {
		scale, charged := admitScale(c.free, DefaultCleanFree)
		if scale != c.scale || charged != c.charged {
			t.Errorf("%d free: scale %v, charged %v; want %v, %v", c.free, scale, charged, c.scale, c.charged)
		}
	}
	if _, charged := admitScale(0, 0); charged {
		t.Error("a store whose cleaner runs only for waiting items charges writes")
	}
}

const second = admitWindow

// A cleaner that freed 2 segments of 64 MiB while its copies, checkpoints and merges took 1.5
// segments' worth in [0, 1 s) refills the bucket from 1 s at 32 MiB/s at 8 free segments and 24 MiB/s at 4, up to a window's worth. A
// write takes the bucket into debt, and the bucket says when it holds a token again. A window
// that freed nothing measures 0, which empties the bucket.
func TestTheBucketRefillsAtTheCleanersNetRate(t *testing.T) {
	const seg = 16384
	mib := func(blocks float64) float64 { return blocks * core.BlockSize / (1 << 20) }
	var b bucket
	b.tick(0, 0, 0, 1)
	b.tick(second/2, seg, seg/2, 1)
	if b.measured || b.tokens != 0 {
		t.Fatalf("half a window measured %v and refilled %v tokens", b.measured, b.tokens)
	}
	b.tick(second, 2*seg, 3*seg/2, 1)
	if got := mib(b.rate * float64(second)); !b.measured || got != 32 || b.tokens != 0 {
		t.Fatalf("the first window measured %v MiB/s, measured %v, and refilled %v", got, b.measured, b.tokens)
	}
	b.tick(second+second/4, 2*seg, 3*seg/2, 1)
	if got := mib(b.tokens); got != 8 {
		t.Fatalf("a quarter second at 8 free segments refilled %v MiB", got)
	}
	b.tick(second+second/2, 2*seg, 3*seg/2, 0.75)
	if got := mib(b.tokens); got != 8+6 {
		t.Fatalf("a quarter second more at 4 free segments refilled %v MiB in all", got-8)
	}
	b.tokens -= 18 << 20 / core.BlockSize
	if at := b.refilled(1); at != b.filledAt+second/8+1 {
		t.Fatalf("a bucket 4 MiB in debt refills %d after its last fill, not an eighth of a second", at-b.filledAt)
	}
	b.tick(2*second, 4*seg, 3*seg, 1)
	b.tick(3*second, 6*seg, 9*seg/2, 1)
	if got := mib(b.tokens); got != 32 {
		t.Fatalf("the bucket holds %v MiB, not a window's 32", got)
	}
	b.tick(4*second, 6*seg, 9*seg/2, 1)
	b.tick(4*second+1, 6*seg, 9*seg/2, 1)
	if b.rate != 0 || b.tokens != 0 {
		t.Fatalf("a window that freed nothing measured %v and left %v tokens", b.rate, b.tokens)
	}
	b.tokens--
	if at := b.refilled(1); at != b.windowAt+second {
		t.Fatalf("a bucket in debt at no rate refills at %d, not at the window's end %d", at, b.windowAt+second)
	}
}

// slowedStore is a store on the small disk, its cleaner off, written until 5 segments outside the
// reserve are free; clean_free then goes to 16, and a read starts passes whose jobs the harness
// holds, so the cleaner frees nothing more, and the bucket measures that over a whole window.
func slowedStore(t *testing.T) *harness {
	h, o := primed(t)
	r := core.NewRand(31)
	for h.s.Log().FreeOutsideReserve() > 5 {
		overwrite(h, o, r, []core.ChunkID{chunk(1), chunk(2)}, 64)
	}
	h.hold = true
	h.s.cfg.CleanFree = DefaultCleanFree
	h.s.cl.tried = false
	h.read(chunk(1), 0, 1)
	h.settle()
	for range 2 {
		h.tick(second)
		h.read(chunk(1), 0, 1)
		h.settle()
	}
	if f := h.s.Log().FreeOutsideReserve(); f < 1 || f > 8 || len(h.s.cl.passes) == 0 || len(h.held) == 0 || !h.s.adm.measured || h.s.adm.rate != 0 {
		t.Fatalf("%d free, passes %d, held jobs %d, measured %v at %v", f, len(h.s.cl.passes), len(h.held), h.s.adm.measured, h.s.adm.rate)
	}
	return h
}

// With the cleaner freeing nothing, chunk writes wait in the store, holding their buffers, in
// the order the bucket refused them, while a punch goes out and the timer is armed for the
// window's end. Once the cleaner frees a victim, the next window's rate refills the bucket and the
// writes go out in order as their tokens come, the free segments still at 8 or fewer.
func TestASlowedDiskHoldsChunkWritesUntilTheCleanerFrees(t *testing.T) {
	h := slowedStore(t)
	free := h.s.Log().FreeOutsideReserve()
	a := h.write(chunk(3), 0, fill(2*core.BlockSize, 'a'), 0)
	b := h.write(chunk(4), 0, fill(core.BlockSize, 'b'), 0)
	p := h.punch(chunk(1), 0, core.BlockSize, 0)
	h.settle()
	if _, ok := h.replies[a]; ok || h.s.Admitting() || len(h.s.adm.held) != 2 {
		t.Fatalf("at %d free segments and no rate, the writes went out: held %v", free, h.s.adm.held)
	}
	for _, ci := range h.s.adm.held {
		if o := &h.s.ops[h.s.chunks[ci].waiting[0]]; o.req.Buf == core.NoBuf {
			t.Fatalf("held write %d gave up its buffer", o.req.ID)
		}
	}
	h.ok(p)
	if h.admitAt != h.s.adm.windowAt+second {
		t.Fatalf("the admit timer is at %d, not at the window's end %d", h.admitAt, h.s.adm.windowAt+second)
	}
	freed := h.s.CleanStats().Freed
	h.runClean(0)
	h.settle()
	if h.s.CleanStats().Freed == freed {
		t.Fatal("the job run freed nothing")
	}
	start := h.ctx.Now
	for i := 0; !h.s.Admitting(); i++ {
		if i == 100 || h.admitAt == 0 {
			t.Fatalf("the bucket still holds %v after %d timers, at rate %v", h.s.adm.held, i, h.s.adm.rate)
		}
		h.advance(h.admitAt - h.ctx.Now)
	}
	if f := h.s.Log().FreeOutsideReserve(); f > 8 || h.ctx.Now < start+second/2 {
		t.Fatalf("the writes went out %d µs after the cleaner freed, with %d free segments", h.ctx.Now-start, f)
	}
	h.ok(a)
	h.ok(b)
	if ia, ib := slices.Index(h.order, a), slices.Index(h.order, b); ia > ib {
		t.Fatalf("the held writes were answered out of order: %d at %d, %d at %d", a, ia, b, ib)
	}
	if got := h.readNow(chunk(3), 0, 2*core.BlockSize); string(got) != string(fill(2*core.BlockSize, 'a')) {
		t.Fatal("the held write did not read back")
	}
}

// A held write waits only while a pass runs: once the cleaner has no pass, the bucket lets it go
// to the log.
func TestAHeldWriteGoesOutWhenNoPassRuns(t *testing.T) {
	h := slowedStore(t)
	a := h.write(chunk(3), 0, fill(core.BlockSize, 'a'), 0)
	h.settle()
	if h.s.Admitting() {
		t.Fatal("the write was not held")
	}
	for _, j := range h.held {
		h.s.freePieces(&h.ctx, j)
	}
	h.held = nil
	h.s.resetCleaner(&h.ctx)
	h.s.cfg.CleanFree = -1
	h.advance(second)
	h.ok(a)
}
