package chunkstore

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// everyActionChunks are the chunks the crash-at-every-action workload writes.
var everyActionChunks = []core.ChunkID{crashChunks[0], crashChunks[1], chunk(9)}

// everyActionWorkload runs on the small disk with merges running as the daemon runs them:
// unaligned writes of 1 byte to 3 blocks, an unaligned punch over a whole block, overwrites of a
// few blocks under snapshot epochs up to 2 until their older segments are mostly dead, which
// checkpoints and compacts, a whole-chunk punch, and a cleaner pass that frees every segment it
// can. Once the pass has read its first victim, a write lands on the last block it would move;
// once the copies are out and before their record is durable, a write, a punch of the whole block
// and a cut land on the first block they move. It reports whether they did.
func everyActionWorkload(h *harness) (windowed bool) {
	h.merging = true
	c, d, e := everyActionChunks[0], everyActionChunks[1], everyActionChunks[2]
	h.write(c, 100, fill(1, 0xa1), 0)
	h.write(c, 4000, fill(2*core.BlockSize, 0xa2), 0)
	h.write(e, 0, fill(2*core.BlockSize, 0xa3), 0)
	h.step()
	h.punch(c, 2*core.BlockSize+7, 2*core.BlockSize, 0)
	h.settle()
	for i := range 36 {
		h.write(d, uint32(i%6)*core.BlockSize, fill(core.BlockSize, byte(i)), uint64(i/12))
		if i%4 == 0 {
			h.settle()
		} else {
			h.step()
		}
	}
	h.punch(e, 0, core.ChunkSize, 2)
	h.settle()
	h.hold = true
	h.s.cfg.CleanFree = int(cleanGeo.SegmentCount)
	h.s.cl.tried = false
	h.read(chunk(1), 0, 1)
	for len(h.held) == 0 && h.step() {
	}
	if len(h.held) > 0 && len(h.held[0].pieces) > 0 {
		p := h.held[0].pieces[len(h.held[0].pieces)-1]
		h.write(p.chunk, uint32(p.first+p.count-1)*core.BlockSize, fill(core.BlockSize, 0xb0), 2)
		h.settle()
	}
	h.hold = false
	if len(h.held) > 0 {
		h.runClean(0)
		if i := slices.IndexFunc(h.s.cl.sent[h.s.cl.sentHead:], func(c cleanSent) bool { return c.pass != nil }); i >= 0 {
			sent := h.s.cl.sent[h.s.cl.sentHead+i]
			pc := sent.pass.job.pieces[sent.piece]
			off := uint32(pc.first) * core.BlockSize
			h.write(pc.chunk, off+5, fill(10, 0xb1), 2)
			h.punch(pc.chunk, off, core.BlockSize, 2)
			h.write(pc.chunk, off, fill(3, 0xb2), 3)
			windowed = true
		}
	}
	h.settle()
	return windowed
}

// everyActionCoverage is what the workload's full run reached: a state between a run's record
// and the manifest naming it, one inside a cleaner pass between its copy record and the owner
// applying the moves, a write that opens a segment and one of a manifest, each of which a crash
// or failure at some action meets; and, by action, whether it is a disk action rather than a timer.
type everyActionCoverage struct {
	unnamedRun, copyOut, opening, manifest bool
	disk                                   []bool
}

// watch records what each action of h's run reaches.
func (cv *everyActionCoverage) watch(h *harness) {
	unnamed, copied := false, false
	seen := len(h.records)
	h.onStep = func(k int, a *core.Action) {
		for len(cv.disk) < k {
			cv.disk = append(cv.disk, false)
		}
		cv.disk = append(cv.disk, true)
		for _, r := range h.records[seen:] {
			for _, e := range r.entries {
				switch {
				case e.Kind == disklog.EntryCopy:
					copied = true
				case e.Kind != disklog.EntryBlob:
				case e.BlobKind == disklog.BlobRun:
					unnamed = true
				case e.BlobKind == disklog.BlobManifest:
					unnamed, cv.manifest = false, true
				case e.BlobKind == disklog.BlobManifestPiece:
					cv.manifest = true
				}
			}
		}
		seen = len(h.records)
		cv.unnamedRun = cv.unnamedRun || unnamed
		cv.copyOut = cv.copyOut || copied && slices.ContainsFunc(h.s.cl.sent[h.s.cl.sentHead:], func(c cleanSent) bool { return c.pass != nil })
		cv.opening = cv.opening || a.Kind == core.ActDiskWrite && uint64(a.LBA)%uint64(h.s.sb.SegmentBlocks) == 0
	}
}

// startEveryAction is a store on the small disk under the crash oracle.
func startEveryAction(t *testing.T) *harness {
	cfg := cleanConfig()
	cfg.Log.MaxReplayBytes = 12 * core.BlockSize
	h := newHarnessOn(t, cfg, cleanGeo)
	h.oracle = sim.NewOracle[writtenRecord](0, newStoreModel())
	return h
}

// runEveryAction runs the workload until it ends or the harness crashes it.
func runEveryAction(h *harness) {
	defer func() {
		if r := recover(); r != nil && r != errCrash {
			panic(r)
		}
	}()
	everyActionWorkload(h)
}

// crashLanding crashes the disk with no unflushed or undecided block landing, a half seeded by
// seed, or all of them.
func crashLanding(h *harness, landing string, seed uint64) {
	h.disk.ClearFaults()
	switch landing {
	case "none":
		h.disk.Crash(func(int, core.LBA) bool { return false })
	case "half":
		h.disk.CrashRandom(core.NewRand(seed))
	case "all":
		h.disk.Crash(func(int, core.LBA) bool { return true })
	}
	h.crashAt, h.crashIf, h.failIf = -1, nil, nil
}

// checkEveryAction recovers the crashed store, then rebuilds its index, checking the store's
// invariants after each; it returns the recovered LSN, or the first violation.
func checkEveryAction(h *harness) (lsn uint64, err error) {
	h.t.Helper()
	h.catch = true
	err = violated(func() {
		h.recoverStore(disklog.ModeOpen)
		lsn = h.acked
		h.rebuild()
	})
	return lsn, err
}

// crashedAt is the workload crashed at action k with landing, recovered with plant.
func crashedAt(t *testing.T, k int, landing string, plant plants) *harness {
	h := startEveryAction(t)
	h.crashAt = k
	runEveryAction(h)
	crashLanding(h, landing, uint64(k))
	h.plant = plant
	return h
}

// sweepCleaning crashes the workload at every action with each landing, and fails each disk
// action with each landing, recovering with plant, and reports the first recovery that fails a
// check.
func sweepCleaning(t *testing.T, total int, disk []bool, plant plants) error {
	for k := 0; k <= total; k++ {
		for _, landing := range []string{"none", "half", "all"} {
			h := crashedAt(t, k, landing, plant)
			if _, err := checkEveryAction(h); err != nil {
				return fmt.Errorf("crash at action %d, %s landing: %w", k, landing, err)
			}
			if k == total || !disk[k] {
				continue
			}
			h = startEveryAction(t)
			var failed core.ActionKind
			var durable uint64
			h.failIf = func(i int, a *core.Action) bool {
				if i != k {
					return false
				}
				failed, durable = a.Kind, h.s.Log().DurableLSN()
				return true
			}
			runEveryAction(h)
			if (failed == core.ActDiskWrite || failed == core.ActDiskFsync) && (h.s.Log().Err() == nil || h.s.Log().DurableLSN() != durable) {
				t.Fatalf("action %d failed: the log's error %v, durable LSN %d, %d at the failure", k, h.s.Log().Err(), h.s.Log().DurableLSN(), durable)
			}
			crashLanding(h, landing, uint64(k))
			h.plant = plant
			if _, err := checkEveryAction(h); err != nil {
				return fmt.Errorf("action %d failed, %s landing: %w", k, landing, err)
			}
		}
	}
	return nil
}

// recoveryAction reports whether a is one of recovery's own writes or flushes, counting
// them in n, and is the j-th of them.
func recoveryAction(h *harness, a *core.Action, n *int, j int) bool {
	if h.s.Serving() || a.Kind != core.ActDiskWrite && a.Kind != core.ActDiskFsync {
		return false
	}
	*n++
	return *n-1 == j
}

// fullCleaning runs the workload to its end and returns its actions and which are disk actions,
// failing unless it compacted, checkpointed and cleaned, landed its operations while the cleaner's
// copies were out, and reached every rare case.
func fullCleaning(t *testing.T) (int, []bool) {
	h := startEveryAction(t)
	var cv everyActionCoverage
	cv.watch(h)
	if !everyActionWorkload(h) {
		t.Fatal("no write, punch and cut landed while the cleaner's copies were out")
	}
	if h.s.CleanStats().Freed == 0 || len(h.merges) == 0 || h.s.Log().Manifest().IsNone() {
		t.Fatalf("the workload freed %d segments, merged %d times, manifest %v", h.s.CleanStats().Freed, len(h.merges), h.s.Log().Manifest())
	}
	if !cv.unnamedRun || !cv.copyOut || !cv.opening || !cv.manifest {
		t.Fatalf("the workload missed a rare case: %+v", cv)
	}
	return h.executed, cv.disk
}

// [property] The single-node ChunkStore's workload with a checkpoint, a compaction and a cleaner
// pass, crashed at its k-th action for every k with no unflushed block landing, a seeded half and
// all of them, and separately failed at its k-th disk action with Io and then crashed the same
// ways, recovers a prefix of what was written that holds every acknowledged write, live and at
// every snapshot, and a rebuilt index equals the recovered one; a failed write or flush leaves no
// acknowledgment after it. A recovery crashed before each of its own writes and flushes recovers
// again to the LSN it would have found, and one whose write or flush fails stops with Io, writes
// nothing more, and the next recovery finds that LSN.
func TestCrashAtEveryActionWithCleaning(t *testing.T) {
	total, disk := fullCleaning(t)
	if err := sweepCleaning(t, total, disk, plants{}); err != nil {
		t.Fatal(err)
	}
	crashes, failures := 0, 0
	for k := 0; k <= total; k++ {
		lsn, err := checkEveryAction(crashedAt(t, k, "half", plants{}))
		if err != nil {
			t.Fatal(err)
		}
		for j := 0; ; j++ {
			h := crashedAt(t, k, "half", plants{})
			n := 0
			h.crashIf = func(a *core.Action) bool { return recoveryAction(h, a, &n, j) }
			func() {
				defer func() {
					if r := recover(); r != nil && r != errCrash {
						panic(r)
					}
				}()
				h.recoverStore(disklog.ModeOpen)
			}()
			if n <= j {
				break
			}
			crashes++
			crashLanding(h, "half", uint64(k)+1)
			if got, err := checkEveryAction(h); err != nil || got != lsn {
				t.Fatalf("crash at action %d, recovery crashed before its write or flush %d: recovered LSN %d, %d uncrashed: %v", k, j, got, lsn, err)
			}
			h = crashedAt(t, k, "half", plants{})
			n, mark := 0, 0
			h.failIf = func(_ int, a *core.Action) bool {
				if !recoveryAction(h, a, &n, j) {
					return false
				}
				mark = len(h.acts)
				return true
			}
			if h.reopen(disklog.ModeOpen) || !errors.Is(h.s.Err(), core.DiskIO) {
				t.Fatalf("crash at action %d, recovery's write or flush %d failed: serving %v, %v", k, j, h.s.Serving(), h.s.Err())
			}
			if i := slices.IndexFunc(h.acts[mark:], func(a core.Action) bool { return a.Kind == core.ActDiskWrite || a.Kind == core.ActDiskFsync }); i >= 0 {
				t.Fatalf("crash at action %d: recovery issued %+v after its write or flush %d failed", k, h.acts[mark+i], j)
			}
			failures++
			crashLanding(h, "half", uint64(k)+1)
			if got, err := checkEveryAction(h); err != nil || got != lsn {
				t.Fatalf("crash at action %d, recovery's write or flush %d failed: the next recovered LSN %d, %d unfailed: %v", k, j, got, lsn, err)
			}
		}
	}
	if crashes == 0 || failures == 0 {
		t.Fatalf("recoveries crashed %d times and failed %d times", crashes, failures)
	}
	t.Logf("%d actions; recoveries crashed %d times and failed %d times", total, crashes, failures)
}

// Planted bug: replay mapping a copy's blocks as a write, whether or not a map still stores them
// at the source, brings back a block the write that landed while the cleaner read it replaced.
func TestCrashAtEveryActionCatchesReplayingCopiesAsWrites(t *testing.T) {
	total, disk := fullCleaning(t)
	err := sweepCleaning(t, total, disk, plants{replayCopyAsWrite: true})
	if violationOf(err) != invAcked {
		t.Fatal(err)
	}
	t.Log(err)
}
