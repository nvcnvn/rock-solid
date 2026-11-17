package chunkstore

import (
	"cmp"
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

// admitRun is a simulated run of closed-loop writers against a cleaner whose worker takes
// jobTime per victim, on a disk whose actions take sim.DefaultDiskLatency one at a time. The disk
// is filled to fill in order with writes of fillRun extents of extent blocks, then overwritten at
// random, extent-aligned, for overwrites disk capacities, within the first hot share of what was
// filled when hot is set; a writer sends its next write once its last is answered and the store is
// admitting, the writer that has waited longest first, as connections read in turn. record sets
// the largest record in bytes in place of 64 KiB.
type admitRun struct {
	geo        disklog.Geometry
	fill       float64
	extent     uint16
	writers    int
	jobTime    core.Tick
	overwrites float64
	hot        float64
	fillRun    int
	record     uint64
	noAdmit    bool
	copiesOnly bool
}

// admitResult is what the last three quarters of the overwrites measured: the time with no free
// segment outside the reserve and with half of clean_free or fewer, the slow-down band, against
// the whole; the longest a writer waited from its last answer to its next, each of those waits,
// the fewest free segments outside the reserve, and the writes NoSpace failed.
type admitResult struct {
	zero, slowed, total core.Tick
	slowest             core.Tick
	waits               []core.Tick
	fewest              int
	noSpace             int
}

func (r admitRun) run(t *testing.T, seed uint64) admitResult {
	t.Helper()
	cfg := testConfig()
	cfg.Check, cfg.Log.Check = false, false
	cfg.Log.NoReserve, cfg.Log.Reserve = false, 8
	cfg.Log.MaxRecordBytes = cmp.Or(r.record, 64<<10)
	cfg.Log.MaxReplayBytes = 16 << 20
	cfg.Log.CompactionStepBytes = uint64(r.geo.SegmentBlocks) * core.BlockSize
	h := newHarnessOn(t, cfg, r.geo)
	h.merging, h.hold, h.timed, h.light = true, true, true, true
	h.s.plant.noAdmission, h.s.plant.copiesOnly = r.noAdmit, r.copiesOnly
	rng := core.NewRand(seed)
	extents := uint64(core.BlocksPerChunk / r.extent)
	fill := uint64(r.fill*float64(r.geo.DiskBlocks())) / core.BlocksPerChunk * extents
	end := fill + uint64(r.overwrites*float64(r.geo.DiskBlocks()))/uint64(r.extent)
	run := uint64(max(r.fillRun, 1))
	data := make([]byte, run*uint64(r.extent)*core.BlockSize)

	hot := fill
	if r.hot > 0 {
		hot = uint64(r.hot * float64(fill))
	}
	var res admitResult
	ids, ready, idle := make([]uint64, r.writers), make([]core.Tick, r.writers), make([]int, 0, r.writers)
	for k := range r.writers {
		idle = append(idle, k)
	}
	var due []core.Tick
	next, sampled, free := uint64(0), core.Tick(0), 0
	for next < end || len(idle) < r.writers {
		for k, id := range ids {
			reply, ok := h.replies[id]
			if id == 0 || !ok {
				continue
			}
			delete(h.replies, id)
			switch {
			case errors.Is(reply.Err, disklog.ErrNoSpace):
				if err := h.found[invNoSpace]; err != nil {
					t.Fatal(err)
				}
				res.noSpace++
			case reply.Err != nil:
				t.Fatalf("write %d: %v", id, reply.Err)
			}
			if sampled != 0 {
				res.slowest = max(res.slowest, h.ctx.Now-ready[k])
				res.waits = append(res.waits, h.ctx.Now-ready[k])
			}
			ids[k], ready[k], idle = 0, h.ctx.Now, append(idle, k)
		}
		for len(idle) > 0 && next < end && h.s.Admitting() {
			e, n := next, uint64(1)
			switch {
			case next >= fill:
				e = rng.Below(hot)
				next++
			default:
				n = min(run, fill-next)
				next += n
			}
			c := core.ChunkID{Volume: 1, Inode: 2, Index: uint32(e / extents)}
			ids[idle[0]] = h.write(c, uint32(e%extents)*uint32(r.extent)*core.BlockSize, data[:n*uint64(r.extent)*core.BlockSize], 0)
			idle = idle[:copy(idle, idle[1:])]
		}
		h.order = h.order[:0]
		if sampled == 0 && next > fill+(end-fill)/4 {
			sampled, free = h.ctx.Now, h.s.Log().FreeOutsideReserve()
			res.fewest = free
		}
		for len(due) < len(h.held) {
			due = append(due, h.ctx.Now+r.jobTime)
		}
		ran := false
		for i := 0; i < len(due); {
			if due[i] > h.ctx.Now {
				i++
				continue
			}
			due = slices.Delete(due, i, i+1)
			h.runClean(i)
			ran = true
		}
		answered := slices.ContainsFunc(ids, func(id uint64) bool { _, ok := h.replies[id]; return id != 0 && ok })
		if !ran && !answered && !h.step() {
			var wake core.Tick
			for _, at := range append(due, h.admitAt, h.timerAt) {
				if at != 0 && (wake == 0 || at < wake) {
					wake = at
				}
			}
			if wake == 0 {
				if next == end && len(idle) == r.writers {
					break
				}
				t.Fatalf("the run stopped with writes outstanding, nothing queued and no timer armed")
			}
			h.tick(max(wake, h.ctx.Now+1) - h.ctx.Now)
		}
		if sampled != 0 {
			if free <= 0 {
				res.zero += h.ctx.Now - sampled
			}
			if free <= h.s.cleanFree()/2 {
				res.slowed += h.ctx.Now - sampled
			}
			res.total += h.ctx.Now - sampled
			free = h.s.Log().FreeOutsideReserve()
			sampled, res.fewest = h.ctx.Now, min(res.fewest, free)
		}
	}
	return res
}

// report logs what a run with admission and one without measured.
func (r admitRun) report(t *testing.T, with, without admitResult) {
	t.Logf("%.0f%% full: no free segment %.1f%% of %.1f s and the slowest write %.1f ms with admission; %.1f%% and %.1f ms without",
		100*r.fill, 100*float64(with.zero)/float64(with.total), float64(with.total)/1e6, float64(with.slowest)/1e3,
		100*float64(without.zero)/float64(without.total), float64(without.slowest)/1e3)
}

// With closed-loop writers outrunning a cleaner whose worker takes 20 ms a victim, at 50 and 65%
// full on 512 segments of 64 blocks, the most the cleaner's net-gain guard sustains on segments
// this small: without admission the disk sits with no free segment outside the reserve most of
// the time and some writer waits longer than two victims' cleaning, 40 ms; with it, free segments
// stay above zero all but 1% of the time, no write fails while the cleaner has a victim, and the
// slowest writer waits less than without.
func TestWriteAdmissionKeepsSegmentsFreeAheadOfASlowCleaner(t *testing.T) {
	for _, fill := range []float64{0.5, 0.65} {
		r := admitRun{geo: disklog.Geometry{SegmentBlocks: 64, SegmentCount: 512}, fill: fill, extent: 4, writers: 8, jobTime: 20_000, overwrites: 1}
		slow := r
		slow.noAdmit = true
		without, with := slow.run(t, 1), r.run(t, 1)
		r.report(t, with, without)
		bar := 2 * r.jobTime
		if without.zero < without.total/2 || without.slowest < bar {
			t.Fatalf("%.0f%% full: without admission the cleaner kept a free segment all but %d of %d µs and the slowest write took %d µs; it is not the slower", 100*fill, without.zero, without.total, without.slowest)
		}
		if with.zero > with.total/100 || with.slowest >= without.slowest || with.noSpace > 0 {
			t.Fatalf("%.0f%% full: no free segment for %d of %d µs, the slowest write %d µs, %d writes failed", 100*fill, with.zero, with.total, with.slowest, with.noSpace)
		}
	}
}

// longSimEnv names the variable that runs the simulations too large for the test suite.
const longSimEnv = "ROCKSOLID_LONG_SIM"

// At 85% full on 320 segments of 1,024 blocks, about the most the net-gain guard sustains on
// segments of that size, the same writers against a cleaner taking 20 ms a victim: admission
// keeps free segments above none for a larger share of the time than without it, and than a
// planted rate net of copied blocks alone, and no write fails while the cleaner has a victim.
// The three runs take about 2.5 minutes, so they run only with ROCKSOLID_LONG_SIM set; their
// figures feed the cleaning-debt question in design.md.
func TestWriteAdmissionAt85PercentFull(t *testing.T) {
	if os.Getenv(longSimEnv) == "" {
		t.Skip("set " + longSimEnv + " to run")
	}
	r := admitRun{geo: disklog.Geometry{SegmentBlocks: 1024, SegmentCount: 320}, fill: 0.85, extent: 4, writers: 8, jobTime: 20_000, overwrites: 1}
	slow, copies := r, r
	slow.noAdmit, copies.copiesOnly = true, true
	without, copied, with := slow.run(t, 1), copies.run(t, 1), r.run(t, 1)
	r.report(t, with, without)
	t.Logf("net of copied blocks alone: no free segment %.1f%% of the time, the slowest write %.1f ms", 100*float64(copied.zero)/float64(copied.total), float64(copied.slowest)/1e3)
	share := func(x admitResult) float64 { return float64(x.zero) / float64(x.total) }
	if share(with) >= share(without) || share(with) >= share(copied) || with.noSpace > 0 {
		t.Fatalf("no free segment %.1f%% of the time against %.1f%% without admission and %.1f%% net of copies alone, %d writes failed", 100*share(with), 100*share(without), 100*share(copied), with.noSpace)
	}
}

// fullRun is admitRun with data at fill of a 16 GiB disk of 1,024 segments of 16 MiB, filled in
// writes of 64 blocks whose records of 264 KiB pack a segment whole, then overwritten in 16 KiB
// writes within the first 30% of the data for a tenth of a disk capacity.
func fullRun(fill float64) admitRun {
	return admitRun{geo: disklog.Geometry{SegmentBlocks: 4096, SegmentCount: 1024}, fill: fill, extent: 4, fillRun: 16, record: 264 << 10,
		writers: 8, jobTime: 20_000, overwrites: 0.1, hot: 0.3}
}

// p99 is the 99th percentile of waits.
func p99(waits []core.Tick) core.Tick {
	w := slices.Clone(waits)
	slices.Sort(w)
	return w[len(w)*99/100]
}

// checkHeld fails the test unless run res reached the slow-down band, where admission kept a
// segment free outside the reserve all but 1% of the time, no write failed while the cleaner had
// a victim, and the writers' wait from one answer to the next stayed within 200 ms at the 99th
// percentile and one admission window, 1 s, at the most.
func (res admitResult) checkHeld(t *testing.T, run string) {
	t.Helper()
	const fullP99, fullSlowest = core.Tick(200_000), admitWindow
	switch {
	case res.fewest > DefaultCleanFree/2:
		t.Fatalf("%s: free segments outside the reserve never fell below %d, so the run never reached the slow-down band", run, res.fewest)
	case res.zero > res.total/100 || res.noSpace > 0:
		t.Fatalf("%s: no free segment for %d of %d µs, %d writes failed", run, res.zero, res.total, res.noSpace)
	case p99(res.waits) > fullP99 || res.slowest > fullSlowest:
		t.Fatalf("%s: writes waited %d µs at the 99th percentile and %d µs at the most", run, p99(res.waits), res.slowest)
	}
}

// [property] With data at 90 and 95% of the disk and overwrites in its first 30%, closed-loop
// writers outrun a cleaner whose worker takes 20 ms a victim, so free segments outside the reserve
// fall into the slow-down band, where admission holds as checkHeld checks. The runs take about a
// minute and 1.5 GB, so a plain go test runs them only with ROCKSOLID_LONG_SIM set, which also
// runs the default seed without admission, where no segment is free for a tenth of the time or
// more.
func TestWriteAdmissionHoldsAt90And95PercentFull(t *testing.T) {
	if seeds.Default() && os.Getenv(longSimEnv) == "" {
		t.Skip("set " + longSimEnv + " or a seed flag to run")
	}
	fills := []float64{0.9, 0.95}
	sum := make([]admitResult, len(fills))
	seeds.Run(t, 0, 1, func(seed uint64) {
		for i, fill := range fills {
			res := fullRun(fill).run(t, seed)
			res.checkHeld(t, fmt.Sprintf("seed %d, %.0f%% full", seed, 100*fill))
			s := &sum[i]
			if s.total == 0 || res.fewest < s.fewest {
				s.fewest = res.fewest
			}
			s.zero, s.slowed, s.total = s.zero+res.zero, s.slowed+res.slowed, s.total+res.total
			s.slowest, s.waits = max(s.slowest, res.slowest), append(s.waits, res.waits...)
		}
	})
	if !seeds.Default() {
		return
	}
	for i, fill := range fills {
		s := sum[i]
		t.Logf("%.0f%% full: %.1f s measured, %.1f%% of it in the slow-down band, as few as %d free; no free segment %.2f%%; writes waited %.1f ms at the 99th percentile, %.1f ms at the most",
			100*fill, float64(s.total)/1e6, 100*float64(s.slowed)/float64(s.total), s.fewest, 100*float64(s.zero)/float64(s.total), float64(p99(s.waits))/1e3, float64(s.slowest)/1e3)
		r := fullRun(fill)
		r.noAdmit = true
		without := r.run(t, 0)
		t.Logf("%.0f%% full without admission: no free segment %.1f%% of the time; writes waited %.1f ms at the 99th percentile, %.1f ms at the most",
			100*fill, 100*float64(without.zero)/float64(without.total), float64(p99(without.waits))/1e3, float64(without.slowest)/1e3)
		if without.zero < without.total/10 {
			t.Fatalf("%.0f%% full: without admission the cleaner kept a free segment all but %d of %d µs; it is not the slower", 100*fill, without.zero, without.total)
		}
	}
}

// Uniform overwrites at 90% full, the disk's fill counting the index's runs, on
// TestCleanerWriteAmplification's 2,048 segments of 8 MiB in records of 192 KiB: closed-loop
// writers of 16 KiB outrun a cleaner whose worker takes 20 ms a victim, for two disk capacities,
// and admission holds as checkHeld checks over the last three quarters of them.
func TestWriteAdmissionHoldsUniformOverwritesAt90PercentFull(t *testing.T) {
	if os.Getenv(longSimEnv) == "" {
		t.Skip("set " + longSimEnv + " to run")
	}
	r := admitRun{geo: disklog.Geometry{SegmentBlocks: 2048, SegmentCount: 2048}, fill: 0.9 - waIndex, extent: 4, fillRun: 8, record: 192 << 10,
		writers: 8, jobTime: 20_000, overwrites: 2}
	res := r.run(t, 1)
	t.Logf("%.1f s measured, %.1f%% of it in the slow-down band, as few as %d free; no free segment %.2f%%; writes waited %.1f ms at the 99th percentile, %.1f ms at the most",
		float64(res.total)/1e6, 100*float64(res.slowed)/float64(res.total), res.fewest, 100*float64(res.zero)/float64(res.total), float64(p99(res.waits))/1e3, float64(res.slowest)/1e3)
	res.checkHeld(t, "uniform 90% full")
}
