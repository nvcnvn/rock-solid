package chunkstore

import (
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
func cleanerWorkload(h *harness, r *core.Rand, ops int) {
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
}

// cleanerCrash runs the workload on the small disk, cleaning as it fills, crashes it at a seeded
// action with a seeded share of unflushed blocks landing, recovers and rebuilds the
// index twice, the second time after the cleaner has moved blocks again, checking the store's
// invariants throughout. A quarter of the seeds take the results of the cleaner's jobs late,
// after other events. plant is planted throughout. It returns the blocks the cleaner freed before
// the crash and the first violation.
func cleanerCrash(t *testing.T, seed uint64, plant plants) (int, error) {
	return cleanerCrashLog(t, seed, plant, disklog.Plants{})
}

// cleanerCrashLog is cleanerCrash with the log's planted bugs logPlant too.
func cleanerCrashLog(t *testing.T, seed uint64, plant plants, logPlant disklog.Plants) (int, error) {
	r := core.NewRand(seed)
	cfg := cleanConfig()
	cfg.Log.Plants = logPlant
	h := newHarnessOn(t, cfg, cleanGeo)
	h.merging, h.catch = true, true
	h.s.plant, h.plant = plant, plant
	if seed%4 == 3 {
		h.late = core.NewRand(^seed)
	}
	h.oracle = sim.NewOracle[writtenRecord](0, newStoreModel())
	h.crashAt = int(r.Range(200, 4000))
	if err := violated(func() {
		crashes(func() {
			cleanerWorkload(h, r, 700)
			panic(errCrash)
		})
	}); err != nil {
		return 0, err
	}
	freed := h.s.CleanStats().Freed
	h.disk.CrashRandom(r)
	h.crashAt = -1
	h.cfg.CleanFree = 0
	return freed, violated(func() {
		if !h.reopen(disklog.ModeOpen) {
			t.Fatalf("seed %d: recovery stopped: %v", seed, h.s.Err())
		}
		h.rebuild()
		h.rebuild()
	})
}

// [property] Crashes at any action of a workload that the cleaner keeps writing on a small disk
// recover what the crash oracle holds: copies durable before their moves applied replay with the
// owner's check, freed and reused segments come back as the table and NextSeq say, and after
// every recovery the usage table equals the index's counts and the log opens its next segment
// past every open sequence on disk. A rebuild of the index after the recovery equals it.
func TestCleanerSurvivesCrashes(t *testing.T) {
	cleaned := 0
	seeds.Run(t, 0, 20, func(seed uint64) {
		freed, err := cleanerCrash(t, seed, plants{})
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
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
	if _, err := cleanerCrash(t, 329, plants{}); err != nil {
		t.Fatal(err)
	}
}

// Seed 448 of TestCleanerSurvivesCrashes: with the guard not charging the record a segment's end
// cuts in two, copying victims of 13 live blocks filled a 16-block segment each, and the cleaner
// copied after recovery without end.
func TestCleanerCrashSeed448Settles(t *testing.T) {
	if _, err := cleanerCrash(t, 448, plants{}); err != nil {
		t.Fatal(err)
	}
}

// Seed 277 of TestCleanerSurvivesCrashes: a write fails with NoSpace when the store finds no
// victim, and the event that failed it then starts a pass on segments its durable LSN made
// cleanable, so a pass running when the reply comes is no victim the write could have waited for.
func TestCleanerCrashSeed277FailsWritesWithNoVictim(t *testing.T) {
	if _, err := cleanerCrash(t, 277, plants{}); err != nil {
		t.Fatal(err)
	}
}

// Planted violation: a cleaner that counts as a victim only a pass it has just started fails
// writes waiting for room while an older pass still copies, which seed 277's writes 620 to 625 wait
// through, and invNoSpace names it.
func TestFailingWritesWhileAPassRunsBreaksNoSpace(t *testing.T) {
	_, err := cleanerCrash(t, 277, plants{newPassOnly: true})
	if violationOf(err) != invNoSpace {
		t.Fatalf("broke %v", err)
	}
	t.Log(err)
}

// Planted violations of the log's bookkeeping, each named by invNamed at the seed given, since the
// harness decodes the named manifest and each segment's records from the writes it saw rather
// than from the log: run pieces dropped before a header names the manifest that dropped them,
// the cleaner ignoring the replay points, the manifests a durable one replaces dropped with their
// pieces before a header names it, a live manifest keeping only the newest entry of its chain,
// and recovery forgetting each segment's newest LSN. Read from the log, the last two showed
// only as a corrupt log, at seeds 34 and 0.
func TestPlantedLogBugsBreakNamed(t *testing.T) {
	for _, c := range []struct {
		name  string
		plant disklog.Plants
		seed  uint64
	}{
		{"PiecesDeadAtOnce", disklog.Plants{PiecesDeadAtOnce: true}, 0},
		{"CleanableIgnoresReplay", disklog.Plants{CleanableIgnoresReplay: true}, 6},
		{"ChainPiecesDieAtOnce", disklog.Plants{ChainPiecesDieAtOnce: true}, 4},
		{"LiveChainNewestOnly", disklog.Plants{LiveChainNewestOnly: true}, 0},
		{"RecoveryForgetsNewest", disklog.Plants{RecoveryForgetsNewest: true}, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := cleanerCrashLog(t, c.seed, plants{}, c.plant)
			if violationOf(err) != invNamed {
				t.Fatalf("broke %v", err)
			}
			t.Log(err)
		})
	}
}

// Planted bug: replay mapping a copy's blocks as a write, whether or not a map still stores them
// at the source, brings back a block a later write replaced. A crash of the settled store
// recovers another index than the owner kept, which invReplay names within 50 seeds.
func TestReplayingCopiesAsWritesBreaksReplay(t *testing.T) {
	for seed := range uint64(50) {
		h := newHarnessOn(t, cleanConfig(), cleanGeo)
		h.merging, h.catch = true, true
		cleanerWorkload(h, core.NewRand(seed), 700)
		if h.s.CleanStats().Freed == 0 {
			continue
		}
		h.disk.Crash(func(int, core.LBA) bool { return false })
		h.plant = plants{replayCopyAsWrite: true}
		err := violated(func() { h.reopen(disklog.ModeOpen) })
		if err == nil {
			continue
		}
		if violationOf(err) != invReplay {
			t.Fatalf("seed %d: %v", seed, err)
		}
		t.Logf("seed %d: %v", seed, err)
		return
	}
	t.Fatal("50 seeds replayed copies as writes and no recovery broke an invariant")
}
