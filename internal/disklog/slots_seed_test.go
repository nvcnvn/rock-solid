package disklog_test

import (
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
	"github.com/nvcnvn/rock-solid/internal/trace"
)

// [property] Rounds with chunk slot writes keep the disk log's trace rules on a node of the
// scheduler: whole chunks, small writes and slots freed at random, at commit_depth 1 to 3, with
// completions in random order, give one DiskFsync per round, which follows both of a slot round's
// writes, and report a round durable only once a flush covered both.
func TestSlotRoundsKeepTheTraceRules(t *testing.T) {
	var slotWrites int
	seeds.Run(t, 1, 20, func(seed uint64) {
		r := core.NewRand(seed)
		cfg := testConfig(r.Range(4, 64))
		cfg.CommitDepth = int(r.Range(1, 3))
		h := schedHarness(t, seed, 16*gib, 48<<20, cfg)
		h.run()
		data := chunkData(1, 1)
		for range 400 {
			switch {
			case r.Chance(1, 4) && h.log.Queued()+h.log.InFlight() < 6:
				h.submit(disklog.Hot, h.whole(uint32(r.Below(16)), r.Uint64(), data))
			case r.Chance(1, 4):
				h.submit(disklog.Stream(r.Below(2)), h.write(uint32(r.Below(16)), uint16(r.Range(1, 2)), r.Uint64()))
			case r.Chance(1, 4):
				if inUse := h.log.SlotsInUse(nil); len(inUse) > 0 {
					if err := h.log.FreeSlot(inUse[r.Below(uint64(len(inUse)))], &h.out); err != nil {
						t.Fatalf("seed %d: %v", seed, err)
					}
					h.absorb()
				}
			case len(h.pending) > 0:
				var ready []int
				for i := range h.pending {
					if h.eligible(i) {
						ready = append(ready, i)
					}
				}
				h.step(ready[r.Below(uint64(len(ready)))])
			}
		}
		h.run()
		for _, ev := range h.eventsOf(disklog.Durable) {
			for _, e := range ev.Entries {
				if e.Kind == disklog.EntrySlot {
					slotWrites++
				}
			}
		}
		if err := trace.Check(h.sched.Trace(), diskLogRules(cfg.CommitDepth)...); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
	})
	if seeds.Default() && slotWrites == 0 {
		t.Fatal("no seed wrote a slot")
	}
}
