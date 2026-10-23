package chunkstore

import (
	"errors"
	"os"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// admitRun is a simulated run of closed-loop writers against a cleaner whose worker takes
// jobTime per victim, on a disk whose actions take sim.DefaultDiskLatency one at a time. The disk
// is filled to fill with writes of extent blocks, then overwritten at random, extent-aligned, for
// overwrites disk capacities; a writer sends its next write once its last is answered and the
// store is admitting, the writer that has waited longest first, as connections read in turn.
type admitRun struct {
	geo        disklog.Geometry
	fill       float64
	extent     uint16
	writers    int
	jobTime    core.Tick
	overwrites float64
	noAdmit    bool
	copiesOnly bool
}

// admitResult is what the last three quarters of the overwrites measured: the time with no free
// segment outside the reserve against the whole, the longest a writer waited from its last
// answer to its next, and the writes NoSpace failed.
type admitResult struct {
	zero, total core.Tick
	slowest     core.Tick
	noSpace     int
}

func (r admitRun) run(t *testing.T, seed uint64) admitResult {
	t.Helper()
	cfg := testConfig()
	cfg.Check, cfg.Log.Check = false, false
	cfg.Log.NoReserve, cfg.Log.Reserve = false, 8
	cfg.Log.MaxRecordBytes = 64 << 10
	cfg.Log.MaxReplayBytes = 16 << 20
	cfg.Log.CompactionStepBytes = uint64(r.geo.SegmentBlocks) * core.BlockSize
	h := newHarnessOn(t, cfg, r.geo)
	h.merging, h.hold, h.timed, h.light = true, true, true, true
	h.s.plant.noAdmission, h.s.plant.copiesOnly = r.noAdmit, r.copiesOnly
	rng := core.NewRand(seed)
	extents := uint64(core.BlocksPerChunk / r.extent)
	fill := uint64(r.fill*float64(r.geo.DiskBlocks())) / core.BlocksPerChunk * extents
	end := fill + uint64(r.overwrites*float64(r.geo.DiskBlocks()))/uint64(r.extent)
	data := make([]byte, int(r.extent)*core.BlockSize)

	var res admitResult
	ids, ready, idle := make([]uint64, r.writers), make([]core.Tick, r.writers), make([]int, 0, r.writers)
	for k := range r.writers {
		idle = append(idle, k)
	}
	var due []core.Tick
	next, sampled, zero := uint64(0), core.Tick(0), false
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
			}
			ids[k], ready[k], idle = 0, h.ctx.Now, append(idle, k)
		}
		for len(idle) > 0 && next < end && h.s.Admitting() {
			e := next
			if next >= fill {
				e = rng.Below(fill)
			}
			c := core.ChunkID{Volume: 1, Inode: 2, Index: uint32(e / extents)}
			ids[idle[0]] = h.write(c, uint32(e%extents)*uint32(r.extent)*core.BlockSize, data, 0)
			idle = idle[:copy(idle, idle[1:])]
			next++
		}
		h.order = h.order[:0]
		if sampled == 0 && next > fill+(end-fill)/4 {
			sampled = h.ctx.Now
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
			if zero {
				res.zero += h.ctx.Now - sampled
			}
			res.total += h.ctx.Now - sampled
			sampled, zero = h.ctx.Now, h.s.Log().FreeOutsideReserve() <= 0
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
// stay above zero all but 1% of the time, no write fails while the cleaner has a victim, and no
// writer waits 40 ms. The cleaner here frees many victims a window; how long writers wait when it
// frees fewer than one is the cleaning-debt question in design.md.
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
		if with.zero > with.total/100 || with.slowest >= bar || with.noSpace > 0 {
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
