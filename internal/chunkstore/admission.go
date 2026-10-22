package chunkstore

import (
	"slices"

	"github.com/nvcnvn/rock-solid/internal/core"
)

const (
	// AdmitTimer is the timer the store arms for when its token bucket next admits a chunk write it
	// holds; its owner hands the store that timer's event.
	AdmitTimer  core.TimerID = 0x61646d74
	admitWindow              = core.Tick(1_000_000)
)

// bucket is write admission's token bucket, in blocks: refilled at the net rate the cleaner freed
// space over the last one-second window, the blocks of the segments it freed less those its
// copies, checkpoints and merges took, and holding at most a window's worth. A write is taken
// while a token is left and charged its blocks in full, so the bucket goes into debt and a write
// larger than a window's tokens still goes out. Before its first window ends it has no rate and
// charges nothing.
type bucket struct {
	started  bool
	measured bool
	windowAt core.Tick
	freed    uint64
	spent    uint64
	// rate is the net blocks freed per tick over the last window, never below 0.
	rate     float64
	tokens   float64
	filledAt core.Tick
}

// tick refills the bucket up to now at its rate scaled by scale, then closes the window once it
// has lasted a second, given the blocks the cleaner has freed and those spent on copies,
// checkpoints and merges, in all.
func (b *bucket) tick(now core.Tick, freed, spent uint64, scale float64) {
	if !b.started {
		b.started, b.windowAt, b.filledAt, b.freed, b.spent = true, now, now, freed, spent
		return
	}
	r := b.rate * scale
	b.tokens = min(b.tokens+r*float64(now-b.filledAt), r*float64(admitWindow))
	b.filledAt = now
	if now-b.windowAt >= admitWindow {
		b.rate = max(float64(freed-b.freed)-float64(spent-b.spent), 0) / float64(now-b.windowAt)
		b.measured, b.windowAt, b.freed, b.spent = true, now, freed, spent
	}
}

// refilled is when the bucket next holds a token at the rate scaled by scale, or when its window
// ends and the rate changes, whichever comes first.
func (b *bucket) refilled(scale float64) core.Tick {
	at := b.windowAt + admitWindow
	if r := b.rate * scale; r > 0 {
		if d := -b.tokens / r; d < float64(at-b.filledAt) {
			at = b.filledAt + core.Tick(d) + 1
		}
	}
	return at
}

// admitScale is how much of the cleaner's rate the bucket hands out with free segments outside
// the reserve, and whether it charges at all: from half of clean_free, 8 of the default 16, the
// share falls from 1 to 0.5 at none in equal steps.
func admitScale(free, cleanFree int) (float64, bool) {
	half := cleanFree / 2
	if half == 0 || free > half {
		return 1, false
	}
	return 0.5 + 0.5*float64(max(free, 0))/float64(half), true
}

// admission is the store's write admission: its bucket, the chunks whose next write the bucket
// holds back, in the order it refused them, and when the store's timer is armed for them.
type admission struct {
	bucket
	held    []int32
	timerAt core.Tick
}

// Admitting reports whether the store takes chunk writes at once: false while its token bucket
// holds one back, when the loop stops reading the connections that send them.
func (s *Store) Admitting() bool { return len(s.adm.held) == 0 }

// charging reports whether chunk writes take tokens, and at what share of the cleaner's rate:
// while the cleaner has a pass running, a rate measured and free segments outside the reserve at
// half of clean_free or fewer. With no pass running the cleaner frees nothing a write could wait
// for, so writes go to the log, which holds them at the reserve until NoSpace fails them.
func (s *Store) charging(now core.Tick) (float64, bool) {
	scale, slowed := admitScale(s.log.FreeOutsideReserve(), s.cleanFree())
	s.adm.tick(now, uint64(s.cl.stats.Freed)*uint64(s.sb.SegmentBlocks), s.log.BackgroundBlocks(), scale)
	return scale, slowed && s.adm.measured && len(s.cl.passes) > 0
}

// admitWrite charges the chunk's next write its blocks, or holds it back, keeping it first among
// the chunk's operations, when the bucket is empty or the write of a chunk held before it waits.
func (s *Store) admitWrite(ctx *core.Context, ci int32, o *op) bool {
	a := &s.adm
	_, charged := s.charging(ctx.Now)
	charged = charged && !s.plant.noAdmission
	if len(a.held) > 0 && a.held[0] != ci || charged && a.tokens <= 0 {
		if !slices.Contains(a.held, ci) {
			a.held = append(a.held, ci)
		}
		return false
	}
	if len(a.held) > 0 {
		a.held = a.held[:copy(a.held, a.held[1:])]
	}
	if charged {
		first, last := spanOf(o.req.Off, o.req.Len)
		a.tokens -= float64(last - first + 1)
	}
	return true
}

// admitHeld measures the cleaner, as the store does on every call, retries the held writes in the
// order the bucket refused them, and arms the timer for when it next holds a token if any is
// still held.
func (s *Store) admitHeld(ctx *core.Context, out *Output) {
	a := &s.adm
	scale, _ := s.charging(ctx.Now)
	for len(a.held) > 0 {
		ci := a.held[0]
		s.orderWaiting(ctx, ci, out)
		if len(a.held) > 0 && a.held[0] == ci {
			break
		}
	}
	if len(a.held) == 0 {
		return
	}
	if at := a.refilled(scale); at != a.timerAt {
		a.timerAt = at
		s.lout.Actions = append(s.lout.Actions, core.Action{Kind: core.ActSetTimer, Timer: AdmitTimer, At: at})
	}
}
