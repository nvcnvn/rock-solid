package chunkstore

import "github.com/nvcnvn/rock-solid/internal/core"

const (
	// AdmitTimer is the timer the store arms for when its token bucket next admits a chunk write it
	// holds; its owner hands the store that timer's event.
	AdmitTimer  core.TimerID = 0x61646d74
	admitWindow              = core.Tick(1_000_000)
)

// bucket is write admission's token bucket, in blocks: refilled at the net rate the cleaner freed
// space over the last one-second window, the blocks of the segments it freed less those its
// copies, checkpoints and merges took, and holding at most a window's worth. A window that spent
// more than it freed carries the difference into the next, so a victim copied across windows is
// credited only its net gain; a window in which the cleaner stopped carries nothing, so what
// merges spent while it was idle does not starve its next pass. A write is taken while a token is
// left and charged its blocks in full, so the bucket goes into debt and a write larger than a
// window's tokens still goes out. Before its first window ends it has no rate and charges nothing.
type bucket struct {
	started  bool
	measured bool
	windowAt core.Tick
	freed    uint64
	spent    uint64
	// carry is the net blocks, at most 0, earlier windows leave to the next, and stopped whether
	// the cleaner stopped during this one, which then carries nothing.
	carry   float64
	stopped bool
	// rate is the net blocks freed per tick over the last window, never below 0.
	rate     float64
	tokens   float64
	filledAt core.Tick
}

// tick refills the bucket up to now at its rate scaled by scale, then closes the window once it
// has lasted a second, given the blocks the cleaner has freed and those spent on copies,
// checkpoints and merges, in all, and whether the cleaner is short of clean_free.
func (b *bucket) tick(now core.Tick, freed, spent uint64, scale float64, cleaning bool) {
	if !b.started {
		b.started, b.stopped, b.windowAt, b.filledAt, b.freed, b.spent = true, !cleaning, now, now, freed, spent
		return
	}
	r := b.rate * scale
	b.tokens = min(b.tokens+r*float64(now-b.filledAt), r*float64(admitWindow))
	b.filledAt = now
	b.stopped = b.stopped || !cleaning
	if now-b.windowAt >= admitWindow {
		net := float64(freed-b.freed) - float64(spent-b.spent) + b.carry
		b.carry, b.rate = min(net, 0), max(net, 0)/float64(now-b.windowAt)
		if b.stopped {
			b.carry = 0
		}
		b.measured, b.stopped, b.windowAt, b.freed, b.spent = true, !cleaning, now, freed, spent
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
// holds back, in the order it refused them from heldHead on, and when the store's timer is armed
// for them.
type admission struct {
	bucket
	held     []int32
	heldHead int
	timerAt  core.Tick
}

// Admitting reports whether the store takes chunk writes at once: false while its token bucket
// holds one back, when the loop stops reading the connections that send them.
func (s *Store) Admitting() bool { return s.adm.heldHead == len(s.adm.held) }

func (a *admission) firstHeld() int32 {
	if a.heldHead == len(a.held) {
		return -1
	}
	return a.held[a.heldHead]
}

func (s *Store) dropHeld() {
	for _, ci := range s.adm.held[s.adm.heldHead:] {
		s.chunks[ci].held = false
	}
	s.adm.held, s.adm.heldHead = s.adm.held[:0], 0
}

// charging reports whether chunk writes take tokens, and at what share of the cleaner's rate:
// while the cleaner has a pass running or will start one, a rate measured and free segments
// outside the reserve at half of clean_free or fewer. A cleaner that found no victim at this LSN
// frees nothing a write could wait for, so writes go to the log, which holds them at the reserve
// until NoSpace fails them.
func (s *Store) charging(now core.Tick) (float64, bool) {
	free := s.log.FreeOutsideReserve()
	scale, slowed := admitScale(free, s.cleanFree())
	spent := s.log.BackgroundBlocks()
	if s.plant.copiesOnly {
		spent = s.cl.stats.Copied
	}
	s.adm.tick(now, uint64(s.cl.stats.Freed)*uint64(s.sb.SegmentBlocks), spent, scale, free < s.cleanFree())
	return scale, slowed && s.adm.measured && (len(s.cl.passes) > 0 || !s.cleanerIdle())
}

// admitWrite charges the chunk's next write its blocks, or holds it back, keeping it first among
// the chunk's operations, when the bucket is empty or the write of a chunk held before it waits.
func (s *Store) admitWrite(ctx *core.Context, ci int32, o *op) bool {
	a := &s.adm
	_, charged := s.charging(ctx.Now)
	charged = charged && !s.plant.noAdmission
	first := a.firstHeld()
	if first >= 0 && first != ci || charged && a.tokens <= 0 {
		if cs := &s.chunks[ci]; !cs.held {
			cs.held = true
			a.held = append(a.held, ci)
		}
		return false
	}
	if first >= 0 {
		s.chunks[ci].held = false
		a.heldHead++
		if a.heldHead > len(a.held)/2 {
			a.held = a.held[:copy(a.held, a.held[a.heldHead:])]
			a.heldHead = 0
		}
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
	for ci := a.firstHeld(); ci >= 0; {
		s.orderWaiting(ctx, ci, out)
		next := a.firstHeld()
		if next == ci {
			break
		}
		ci = next
	}
	if s.Admitting() {
		return
	}
	if at := a.refilled(scale); at != a.timerAt {
		a.timerAt = at
		s.lout.Actions = append(s.lout.Actions, core.Action{Kind: core.ActSetTimer, Timer: AdmitTimer, At: at})
	}
}
