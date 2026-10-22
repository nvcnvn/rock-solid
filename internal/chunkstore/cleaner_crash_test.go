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

// cleanerWorkload overwrites and punches blocks of three chunks, now and then under a new
// snapshot epoch, with operations pipelined behind a few disk actions at a time, so writes land
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
			h.merge()
		}
	}
	h.settle()
	return epoch
}

// cleanerCrash runs the workload on the small disk, cleaning as it fills, crashes it at a seeded
// action with a seeded share of unflushed blocks landing, and recovers: the store must hold what
// the crash oracle holds, live and at every snapshot, with the usage table on the index's counts,
// and again once it has rebuilt its index, which must equal the opened one. It returns the
// difference, if any.
func cleanerCrash(t *testing.T, seed uint64, plant plants) (string, int) {
	r := core.NewRand(seed)
	h := newHarnessOn(t, cleanConfig(), cleanGeo)
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
	h.s.cfg.CleanFree = -1
	h.rebuild()
	if rebuilt := dumpIndex(h.s.Index()); !slices.Equal(rebuilt, opened) {
		return fmt.Sprintf("rebuilt index has %d entries, opened %d", len(rebuilt), len(opened)), freed
	}
	return modelDiff(h, m, cleanChunks, epoch+1), freed
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
