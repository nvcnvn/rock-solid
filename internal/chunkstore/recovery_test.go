package chunkstore

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/lsm"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

const diskBytes = 1 << 30

// recoveryChunks are the recovery workload's chunks: twelve it writes from the start, and two it
// first writes after a cut, whose first write freezes them absent.
var recoveryChunks = func() []core.ChunkID {
	var cs []core.ChunkID
	for inode := range 4 {
		for i := range 4 - 2*(inode/3) {
			cs = append(cs, core.ChunkID{Volume: 1, Inode: core.InodeID(1 + inode), Index: uint32(i)})
		}
	}
	return cs
}()

// newRecoveryHarness is a store on the default disk under the crash oracle, with 16-block records,
// a checkpoint due every 96 blocks of records, and merges running as the daemon runs them.
func newRecoveryHarness(t *testing.T) *harness {
	cfg := testConfig()
	cfg.Log.MaxRecordBytes = 16 * core.BlockSize
	cfg.Log.MaxReplayBytes = 96 * core.BlockSize
	h := newHarness(t, cfg)
	h.merging = true
	h.oracle = sim.NewOracle[writtenRecord](0, newStoreModel())
	return h
}

// workload sends random writes and punches over the recovery chunks, now and then after a cut.
type workload struct {
	r     *rand.Rand
	epoch uint64
	ops   int
}

// run sends 1 to 4 requests and settles, rounds times.
func (w *workload) run(h *harness, rounds int) {
	for range rounds {
		for range 1 + w.r.IntN(4) {
			w.request(h)
		}
		h.settle()
	}
}

// request cuts one time in 40, then sends a write of 1 byte to 6 blocks within a chunk's first 64
// blocks, unaligned one time in two, a punch of such a range two times in ten, or a punch of the
// whole chunk one in ten.
func (w *workload) request(h *harness) {
	if w.r.IntN(40) == 0 {
		w.epoch++
	}
	w.ops++
	c := recoveryChunks[w.r.IntN(len(recoveryChunks)-2)]
	if w.epoch > 0 {
		c = recoveryChunks[w.r.IntN(len(recoveryChunks))]
	}
	off := uint32(w.r.IntN(58)) * core.BlockSize
	n := uint32(1+w.r.IntN(6)) * core.BlockSize
	if w.r.IntN(2) == 0 {
		off += uint32(w.r.IntN(core.BlockSize))
		n = uint32(1 + w.r.IntN(int(n)))
	}
	switch w.r.IntN(10) {
	case 0:
		h.punch(c, 0, core.ChunkSize, w.epoch)
	case 1, 2:
		h.punch(c, off, n, w.epoch)
	default:
		h.write(c, off, fill(int(n), byte(1+w.ops%255)), w.epoch)
	}
}

// crashes runs f and reports whether the harness crashed it.
func crashes(f func()) (crashed bool) {
	defer func() {
		if r := recover(); r != nil {
			if r != errCrash {
				panic(r)
			}
			crashed = true
		}
	}()
	f()
	return false
}

// restart crashes the disk with no unflushed block landing and recovers the store on it, which
// must read as the crash oracle's model.
func restart(h *harness, w *workload) {
	h.t.Helper()
	h.crashIf = nil
	h.disk.Crash(func(int, core.LBA) bool { return false })
	h.recoverStore(disklog.ModeOpen)
	checkModel(h.t, h, h.recovered, recoveryChunks, w.epoch+1)
}

// written counts the run pieces and manifests of the records written, and gives the newest
// manifest's LSN.
func written(h *harness) (pieces, manifests int, newest uint64) {
	for _, r := range h.records {
		for _, e := range r.entries {
			switch {
			case e.Kind != disklog.EntryBlob:
			case e.BlobKind == disklog.BlobRun:
				pieces++
			case e.BlobKind == disklog.BlobManifest:
				manifests, newest = manifests+1, r.lsn
			}
		}
	}
	return pieces, manifests, newest
}

// [property] Rebuilding the index from every record header, and recovering it from the manifest's
// runs plus replay, both give the live index exactly, across restarts.
func TestRecoveryAndRebuildEqualLiveIndex(t *testing.T) {
	seeds.Run(t, 0, 4, func(seed uint64) {
		h := newRecoveryHarness(t)
		w := &workload{r: rand.New(rand.NewPCG(seed, 7))}
		carried := 0
		for generation := range 3 {
			w.run(h, 150)
			live := dumpIndex(h.s.Index())
			restart(h, w)
			if got := dumpIndex(h.s.Index()); !slices.Equal(got, live) {
				t.Fatalf("seed %d generation %d: the recovered index holds %d entries, the live one %d", seed, generation, len(got), len(live))
			}
			runs := h.s.Index().Tree().Runs()
			if runs == 0 || h.recoveredLSN <= h.loaded.LSN {
				t.Fatalf("seed %d generation %d: %d runs, recovered to LSN %d from the manifest at %d", seed, generation, runs, h.recoveredLSN, h.loaded.LSN)
			}
			carried = max(carried, runs-1)
			if got := h.rebuild(); !slices.Equal(got, live) {
				t.Fatalf("seed %d generation %d: the rebuilt index holds %d entries, the live one %d", seed, generation, len(got), len(live))
			}
		}
		pieces, manifests, _ := written(h)
		t.Logf("seed %d: %d manifests of %d run pieces, %d runs carried, %d merges, %d cuts", seed, manifests, pieces, carried, len(h.merges), w.epoch)
		if pieces <= manifests || carried == 0 || len(h.merges) == 0 || w.epoch == 0 {
			t.Fatalf("seed %d: the workload missed a case", seed)
		}
	})
}

// A crash once a checkpoint's manifest record is durable but before any segment header names it
// recovers from an earlier manifest plus replay, even when a merge's output is among its runs, and
// the next checkpoint's restart does too.
func TestCrashBetweenRunAndManifest(t *testing.T) {
	seeds.Run(t, 0, 4, func(seed uint64) {
		h := newRecoveryHarness(t)
		w := &workload{r: rand.New(rand.NewPCG(seed, 8))}
		for _, m, _ := written(h); m < 3 || !slices.ContainsFunc(h.merges, (*lsm.Compaction).Finished); _, m, _ = written(h) {
			w.run(h, 10)
		}
		_, _, before := written(h)
		var lost uint64
		h.crashIf = func(*core.Action) bool {
			_, _, newest := written(h)
			if newest > before && newest <= h.s.Log().DurableLSN() {
				lost = newest
			}
			return lost != 0
		}
		if !crashes(func() { w.run(h, 1000) }) {
			t.Fatalf("seed %d: no manifest written", seed)
		}
		restart(h, w)
		if h.loaded.IsNone() || h.loaded.LSN >= lost {
			t.Fatalf("seed %d: recovered from manifest %+v, not one before the manifest at LSN %d no header named", seed, h.loaded, lost)
		}
		w.run(h, 40)
		live := dumpIndex(h.s.Index())
		restart(h, w)
		if got := dumpIndex(h.s.Index()); h.loaded.LSN <= lost || !slices.Equal(got, live) {
			t.Fatalf("seed %d: the index recovered from manifest %+v holds %d entries, the live one %d", seed, h.loaded, len(got), len(live))
		}
	})
}

// partedFreeze settles, then writes maxWrite-1 blocks of chunk a and 2×maxWrite of chunk c at
// epoch 1 together, so that c's freeze goes in a record ahead of its blocks, crashes the disk
// before the k-th action after them, with nothing unflushed landing, and recovers; it returns c's
// entry.
func partedFreeze(h *harness, a, c core.ChunkID, k int) Chunk {
	h.t.Helper()
	h.settle()
	base, mw := h.executed, int(h.s.maxWrite)
	h.crashIf = func(*core.Action) bool { return h.executed == base+k }
	h.write(a, 0, fill((mw-1)*core.BlockSize, 2), 1)
	h.write(c, 0, fill(2*mw*core.BlockSize, 3), 1)
	crashes(h.settle)
	h.crashIf = nil
	h.disk.Crash(func(int, core.LBA) bool { return false })
	h.recoverStore(disklog.ModeOpen)
	got, _ := h.s.Index().Chunk(testPool, c)
	return got
}

// A chunk frozen absent and never written, which a crash between a write's freeze and its blocks
// leaves, recovers by replay, then from a run, as the crash oracle's model.
func TestAbsentOnlyChunkRecovers(t *testing.T) {
	a, c := recoveryChunks[0], core.ChunkID{Volume: 1, Inode: 9}
	for k := range 12 {
		h := newRecoveryHarness(t)
		h.write(a, 0, fill(core.BlockSize, 1), 0)
		if partedFreeze(h, a, c, k) != (Chunk{Epoch: 1}) {
			continue
		}
		w := &workload{r: rand.New(rand.NewPCG(0, 10)), epoch: 1}
		for restarts := 0; ; restarts++ {
			checkModel(t, h, h.recovered, []core.ChunkID{c}, 2)
			if got, ok := h.s.Index().Chunk(testPool, c); !ok || got != (Chunk{Epoch: 1}) {
				t.Fatalf("crash at action %d, restart %d: chunk %v entry %+v %v, want frozen absent at epoch 1", k, restarts, c, got, ok)
			}
			if restarts == 1 {
				return
			}
			_, before, _ := written(h)
			for _, m, _ := written(h); m < before+2; _, m, _ = written(h) {
				w.run(h, 10)
			}
			restart(h, w)
		}
	}
	t.Fatal("no crash left a chunk frozen absent without its blocks")
}

// oracleStats counts what the crash cycles reached.
type oracleStats struct {
	recoveries, kept, crashedRecoveries, erasedThenCrashed, withRuns, withFreezes int
	// absent counts recoveries with a chunk frozen absent, writtenAfterAbsent those with one
	// written after it.
	absent, writtenAfterAbsent int
}

// crashCycles runs the workload on a store crashed at random actions six times, each unflushed
// block landing at random, and recovers with plant after each crash; every other recovery is first
// crashed at its final flush and run again. It returns the first recovery the oracle rejects.
func crashCycles(t *testing.T, seed uint64, plant plants, st *oracleStats) error {
	t.Helper()
	h := newRecoveryHarness(t)
	h.plant = plant
	w := &workload{r: rand.New(rand.NewPCG(seed, 9))}
	r := core.NewRand(seed)
	for cycle := range 6 {
		h.crashIf = func(a *core.Action) bool { return a.Kind != core.ActDiskRead && r.Chance(1, 300) }
		crashes(func() { w.run(h, 120) })
		h.crashIf = nil
		h.disk.CrashRandom(r)
		acked, newest := h.oracle.Window()
		if cycle%2 == 1 {
			var found uint64
			h.crashIf = func(a *core.Action) bool {
				if h.s.rec == nil || a.Kind != core.ActDiskFsync || a.Linked {
					return false
				}
				found = h.s.rec.DurableLSN()
				if h.s.rec.Erased() > 0 {
					st.erasedThenCrashed++
				}
				return true
			}
			if !crashes(func() { h.reopen(disklog.ModeOpen) }) {
				t.Fatalf("seed %d cycle %d: recovery issued no final flush to crash at", seed, cycle)
			}
			h.crashIf = nil
			st.crashedRecoveries++
			if a, b := h.oracle.Window(); a != acked || b != newest {
				t.Fatalf("seed %d cycle %d: a crashed recovery moved the oracle's window from [%d, %d] to [%d, %d]", seed, cycle, acked, newest, a, b)
			}
			h.disk.CrashRandom(r)
			h.recoverStore(disklog.ModeOpen)
			if h.recoveredLSN != found {
				t.Fatalf("seed %d cycle %d: recovered to LSN %d, the crashed recovery found %d", seed, cycle, h.recoveredLSN, found)
			}
		} else {
			h.recoverStore(disklog.ModeOpen)
		}
		m := h.recovered
		if msg := modelDiff(h, m, recoveryChunks, w.epoch+1); msg != "" {
			return fmt.Errorf("store recovered to LSN %d: %s", h.recoveredLSN, msg)
		}
		st.recoveries++
		if h.recoveredLSN > acked {
			st.kept++
		}
		if h.s.Index().Tree().Runs() > 0 {
			st.withRuns++
		}
		absent, after, frozen := false, false, false
		for _, c := range recoveryChunks {
			frozen = frozen || len(m.frozen[c]) > 0
			if m.isFrozen(c, 0) {
				absent, after = true, after || m.chunks[c].Version != 0
			}
		}
		if frozen {
			st.withFreezes++
		}
		if absent {
			st.absent++
		}
		if after {
			st.writtenAfterAbsent++
		}
	}
	return nil
}

// [property] Seeds 0-7, six crashes each at random actions, each unflushed block landing at
// random: every recovery reports an LSN in the crash oracle's window and recovers a store that
// reads, chunk entries and blocks live and at every snapshot epoch, as the oracle's model at that
// LSN. A recovery crashed at its final flush leaves the window as it was, and the recovery run
// again reports the LSN it had found.
func TestRecoveriesMatchTheCrashOracle(t *testing.T) {
	var st oracleStats
	for seed := range uint64(8) {
		if err := crashCycles(t, seed, plants{}, &st); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
	}
	t.Logf("%+v", st)
	if st.kept == 0 || st.erasedThenCrashed == 0 || st.withRuns < st.recoveries/2 || st.withFreezes == 0 || st.absent == 0 || st.writtenAfterAbsent == 0 {
		t.Fatalf("the crashes missed a case: %+v", st)
	}
}

// Replay that leaves out the first record past the replay LSN that maps blocks, as a replay LSN
// one record too new would (RocksDB #11148), trips the crash oracle on some seed of 8.
func TestCrashOracleCatchesLateReplay(t *testing.T) {
	for seed := range uint64(8) {
		if err := crashCycles(t, seed, plants{replayLate: true}, &oracleStats{}); err != nil {
			if !strings.Contains(err.Error(), "the model holds") {
				t.Fatalf("seed %d: %v names no expected read", seed, err)
			}
			t.Logf("seed %d: %v", seed, err)
			return
		}
	}
	t.Fatal("no seed of 8 tripped the oracle")
}
