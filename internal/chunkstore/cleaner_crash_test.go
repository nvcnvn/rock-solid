package chunkstore

import (
	"fmt"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

var cleanChunks = []core.ChunkID{chunk(1), chunk(2), chunk(3)}

// cleanerWorkload overwrites and punches blocks of three chunks, now and then a whole chunk or
// under a new snapshot epoch, with operations pipelined behind a few disk actions at a time, so writes land
// while the cleaner copies, and merges run as the daemon would run them.
func cleanerWorkload(h *harness, r *core.Rand, ops int) uint64 {
	var epoch uint64
	for range ops {
		c := cleanChunks[r.Below(uint64(len(cleanChunks)))]
		b := uint32(r.Below(48))
		switch r.Below(40) {
		case 0, 1, 2:
			h.punch(c, b*core.BlockSize, core.BlockSize, epoch)
		case 3:
			epoch++
		case 4:
			h.punch(c, 0, core.ChunkSize, epoch)
		default:
			data := make([]byte, core.BlockSize)
			r.Fill(data)
			h.write(c, b*core.BlockSize, data, epoch)
		}
		for range r.Below(4) {
			h.step()
		}
		if r.Chance(1, 8) {
			h.settle()
		}
	}
	h.settle()
	return epoch
}

// cleanerCrash runs the workload on the small disk, cleaning as it fills, crashes it at a seeded
// action with a seeded share of unflushed blocks landing, and recovers: the store must hold what
// the crash oracle holds, live and at every snapshot, with the usage table on the index's counts,
// and again once it has rebuilt its index, which must equal the opened one; a second rebuild,
// after the cleaner has moved blocks again, must equal the live index. A quarter of the seeds
// take the results of the cleaner's jobs late, after other events. It returns the difference, if
// any.
func cleanerCrash(t *testing.T, seed uint64, plant plants) (string, int) {
	r := core.NewRand(seed)
	h := newHarnessOn(t, cleanConfig(), cleanGeo)
	h.merging = true
	if seed%4 == 3 {
		h.late = core.NewRand(^seed)
	}
	h.oracle = sim.NewOracle[writtenRecord](0, newStoreModel())
	h.crashAt = int(r.Range(200, 4000))
	var epoch uint64
	func() {
		defer func() {
			if v := recover(); v != nil && v != errCrash {
				panic(v)
			}
		}()
		epoch = cleanerWorkload(h, r, 700)
		panic(errCrash)
	}()
	freed := h.s.CleanStats().Freed
	h.disk.CrashRandom(r)
	h.crashAt = -1
	h.cfg.CleanFree = 0
	h.queue, h.out = nil, Output{}
	h.abandoned, h.held = h.pool.Live(), nil
	h.s = newStore(h.cfg)
	h.s.plant = plant
	if err := h.s.recover(&h.ctx, h.disk.Blocks(), disklog.ModeOpen); err != nil {
		t.Fatal(err)
	}
	h.s.pump(&h.ctx, &h.out)
	h.take()
	h.settle()
	if !h.s.Serving() {
		t.Fatalf("seed %d: recovery stopped: %v", seed, h.s.Err())
	}
	h.checkUsage()
	m := h.recovered
	if msg := modelDiff(h, m, cleanChunks, epoch+1); msg != "" {
		return msg, freed
	}
	opened := dumpIndex(h.s.Index())
	if rebuilt := h.rebuild(); !slices.Equal(rebuilt, opened) {
		return fmt.Sprintf("rebuilt index has %d entries, opened %d", len(rebuilt), len(opened)), freed
	}
	if msg := modelDiff(h, m, cleanChunks, epoch+1); msg != "" {
		return "after a rebuild: " + msg, freed
	}
	live := dumpIndex(h.s.Index())
	if again := h.rebuild(); !slices.Equal(again, live) {
		return fmt.Sprintf("a second rebuild has %d entries, the live index after the first %d", len(again), len(live)), freed
	}
	return "", freed
}

// [property] Crashes at any action of a workload that the cleaner keeps writing on a small disk
// recover what the crash oracle holds: copies durable before their moves applied replay with the
// owner's check, freed and reused segments come back as the table and NextSeq say, and after
// every recovery the usage table equals the index's counts and the log opens its next segment
// past every open sequence on disk. A rebuild of the index after the recovery equals it.
func TestCleanerSurvivesCrashes(t *testing.T) {
	cleaned := 0
	seeds.Run(t, 0, 20, func(seed uint64) {
		msg, freed := cleanerCrash(t, seed, plants{})
		if msg != "" {
			t.Fatalf("seed %d: %s", seed, msg)
		}
		if freed > 0 {
			cleaned++
		}
	})
	if seeds.Default() && cleaned < 10 {
		t.Fatalf("the cleaner freed a segment before the crash in only %d of 20 seeds", cleaned)
	}
}

// Seed 329 of TestCleanerSurvivesCrashes: with the guard charging runs at their bytes rather
// than at the records their pieces take, the cleaner kept rewriting metadata segments into new
// ones as full after recovery, and the store never settled.
func TestCleanerCrashSeed329Settles(t *testing.T) {
	if msg, _ := cleanerCrash(t, 329, plants{}); msg != "" {
		t.Fatal(msg)
	}
}

// Seed 448 of TestCleanerSurvivesCrashes: with the guard not charging the record a segment's end
// cuts in two, copying victims of 13 live blocks filled a 16-block segment each, and the cleaner
// copied after recovery without end.
func TestCleanerCrashSeed448Settles(t *testing.T) {
	if msg, _ := cleanerCrash(t, 448, plants{}); msg != "" {
		t.Fatal(msg)
	}
}

// Planted bug: replay mapping a copy's blocks as a write, whether or not a map still stores them
// at the source, brings back a block a later write replaced; the crash oracle sees it within 50
// seeds.
func TestReplayingCopiesAsWritesFailsTheOracle(t *testing.T) {
	for seed := range uint64(50) {
		if msg, _ := cleanerCrash(t, seed, plants{replayCopyAsWrite: true}); msg != "" {
			return
		}
	}
	t.Fatal("50 seeds replayed copies as writes and the crash oracle saw nothing")
}
