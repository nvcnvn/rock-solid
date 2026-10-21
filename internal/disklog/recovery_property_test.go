package disklog_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

const userBlobOwner = 7

type crashStats struct {
	keptInFlight, droppedInFlight, fromManifest, noSpace, depth2, depth3, erased, restarts, crashesInRecovery, gaps, gapsInsideTheWindow int
}

// crashSeed runs four cycles of random items, checkpoints and disk completions, each ended
// by a crash that lands every unflushed block with probability 1/2, and checks every
// recovery against what was acknowledged before the crash. It runs on a node of the
// scheduler, whose trace it returns with the harness.
func crashSeed(t *testing.T, seed uint64, st *crashStats) *harness {
	r := core.NewRand(seed)
	disk := [3]uint64{1024, 4096, 1 << 16}[r.Below(3)]
	cfg := testConfig(r.Range(3, 32))
	cfg.MaxReplayBytes = r.Range(4, 200) * core.BlockSize
	cfg.CommitDepth = int(r.Range(1, 3))
	switch cfg.CommitDepth {
	case 2:
		st.depth2++
	case 3:
		st.depth3++
	}
	h := schedHarness(t, seed, disk*core.BlockSize, 64<<20, cfg)
	h.run()
	h.trace = nil
	h.reformat(r.Uint64() | 1)
	pieceLimit := 0
	if r.Chance(1, 2) {
		pieceLimit = 512
	}
	n := newNode(h, pieceLimit)
	h.ioRand = r.Fork()
	durableAtWrite := map[core.LBA]uint64{}
	h.onWrite = func(a core.Action) {
		if h.log == nil || h.rec != nil {
			return
		}
		for b := range core.LBA(a.Len / core.BlockSize) {
			durableAtWrite[a.LBA+b] = h.log.DurableLSN()
		}
	}
	nodeEvent := h.onEvent
	h.onEvent = func(ev disklog.Event) {
		nodeEvent(ev)
		if ev.Kind != disklog.Durable {
			return
		}
		if named := h.record(ev).Record.Durable; named >= ev.LSN || named > durableAtWrite[ev.LBA] {
			t.Fatalf("seed %d: LSN %d names %d durable; the log's durable LSN was %d when its round was written", seed, ev.LSN, named, durableAtWrite[ev.LBA])
		}
	}
	writes := map[uint64]disklog.Entry{}
	blobs := map[uint64][]byte{}
	var version, blobID uint64

	submitOne := func() {
		maxWrite := min(cfg.MaxRecordBytes/core.BlockSize, uint64(h.geo.SegmentBlocks))
		maxBlob := h.log.MaxBlobBytes()
		var it disklog.Item
		stream := disklog.Hot
		switch k := r.Below(10); {
		case k < 6:
			count := uint16(r.Range(1, min(3, maxWrite-2)))
			version++
			chunk := uint32(r.Below(16))
			it = h.chunkWrite(chunk, uint16(r.Below(uint64(core.BlocksPerChunk-int(count))+1)), count, version)
			writes[version] = it.Entry
		case k < 7:
			version++
			it = disklog.Item{Entry: disklog.PunchEntry(1, chunkOf(uint32(r.Below(16))), version, 0, core.BlocksPerChunk)}
			writes[version] = it.Entry
		default:
			limit := uint64(3000)
			if k >= 9 {
				stream, limit = disklog.Cold, 6000
			}
			size := int(r.Range(8, min(limit, maxBlob)))
			blobID++
			payload := pattern(blobID, size)
			binary.LittleEndian.PutUint64(payload, blobID)
			blobs[blobID] = payload
			buf, err := h.pool.Alloc(size)
			if err != nil {
				return
			}
			copy(h.pool.Bytes(buf), payload)
			it = disklog.Item{Entry: disklog.BlobEntry(disklog.BlobRaftEntries, userBlobOwner, uint64(size)), Buf: buf}
		}
		if _, err := h.log.Submit(&h.ctx, stream, it, &h.out); err != nil {
			t.Fatalf("seed %d: submit %+v: %v", seed, it.Entry, err)
		}
		h.absorb()
	}

	for cycle := range 4 {
		for range 300 {
			for n.step() {
			}
			if h.log.Err() == nil && (len(h.pending) == 0 || r.Chance(2, 5)) {
				for range r.Range(1, 4) {
					submitOne()
				}
				continue
			}
			if len(h.pending) > 0 {
				var ready []int
				for i := range h.pending {
					if h.eligible(i) {
						ready = append(ready, i)
					}
				}
				h.step(ready[r.Below(uint64(len(ready)))])
			}
		}
		for _, ev := range h.eventsOf(disklog.Failed) {
			if errors.Is(ev.Err, disklog.ErrNoSpace) {
				st.noSpace++
				break
			}
		}
		acked, inFlight := h.log.DurableLSN(), uint64(h.log.InFlight())
		newest, replayBytes := h.log.Manifest(), h.log.ReplayBytes()
		usage := disklog.SegmentUsageOf(h.log)
		before := n.live.clone()
		if r.Chance(1, 4) {
			st.restarts++
		} else {
			h.crashRandom(r)
		}

		reb := h.recoverCrashing(disklog.ModeRebuild, func(a core.Action) bool {
			if a.Kind == core.ActDiskRead || !r.Chance(1, 4) {
				return false
			}
			st.crashesInRecovery++
			h.crashRandom(r)
			return true
		})
		if reb.err != nil {
			t.Fatalf("seed %d cycle %d: rebuild: %v", seed, cycle, reb.err)
		}
		rebuiltReplay := h.log.ReplayBytes()
		rebuiltUsage := disklog.SegmentUsageOf(h.log)
		res := h.recover(disklog.ModeOpen)
		if res.err != nil {
			t.Fatalf("seed %d cycle %d: open: %v", seed, cycle, res.err)
		}
		if h.disk.Unflushed() != 0 {
			t.Fatalf("seed %d cycle %d: recovered with %d blocks unflushed", seed, cycle, h.disk.Unflushed())
		}
		if got := disklog.SegmentUsageOf(h.log); !slices.Equal(got, rebuiltUsage) || (inFlight == 0 && !slices.Equal(got, usage)) {
			t.Fatalf("seed %d cycle %d: open's usage %+v, rebuild's %+v, before the crash %+v", seed, cycle, got, rebuiltUsage, usage)
		}
		if h.log.ReplayBytes() != rebuiltReplay {
			t.Fatalf("seed %d cycle %d: open leaves %d bytes to replay, rebuild %d", seed, cycle, h.log.ReplayBytes(), rebuiltReplay)
		}
		d := res.durable
		if d < acked || d > acked+inFlight || reb.durable != d {
			t.Fatalf("seed %d cycle %d: recovered LSN %d (rebuild %d), %d acknowledged and %d in flight", seed, cycle, d, reb.durable, acked, inFlight)
		}
		switch {
		case d > acked:
			st.keptInFlight++
		case inFlight > 0:
			st.droppedInFlight++
		}
		if !res.manifest.IsNone() {
			st.fromManifest++
			want, ok := n.expect[res.manifest]
			if !ok || !manifestsEqual(res.loaded, want) || !manifestsEqual(reb.loaded, want) {
				t.Fatalf("seed %d cycle %d: open loaded %+v, rebuild %+v; a snapshot at the same base is %+v", seed, cycle, res.loaded, reb.loaded, want)
			}
		}
		st.erased += reb.erased + res.erased
		if len(reb.replayed) != int(d) {
			t.Fatalf("seed %d cycle %d: rebuild replayed %d records of %d", seed, cycle, len(reb.replayed), d)
		}
		want := before
		for _, ev := range reb.replayed {
			for i, e := range ev.Entries {
				switch {
				case e.Kind == disklog.EntryWrite || e.Kind == disklog.EntryPunch:
					if writes[e.Version] != e {
						t.Fatalf("seed %d cycle %d: LSN %d entry %d is %+v, never submitted", seed, cycle, ev.LSN, i, e)
					}
				case e.Owner == userBlobOwner:
					b := ev.Blob(i)
					if uint64(len(b)) != e.Length || len(b) < 8 || !bytes.Equal(b, blobs[binary.LittleEndian.Uint64(b)]) {
						t.Fatalf("seed %d cycle %d: LSN %d blob %d is not what was submitted", seed, cycle, ev.LSN, i)
					}
				}
			}
			if ev.LSN > acked {
				want.apply(ev)
			}
		}
		if d == acked && res.manifest == newest && h.log.ReplayBytes() != replayBytes {
			t.Fatalf("seed %d cycle %d: %d bytes to replay after recovery, %d before the crash", seed, cycle, h.log.ReplayBytes(), replayBytes)
		}
		h.wantModel("open", res.index, want)
		h.wantModel("rebuild", reb.index, want)
		h.readBack(want)
		if gapNamedDurable(t, h, r, res, st) {
			if again := h.reopen(); again.durable != d {
				t.Fatalf("seed %d cycle %d: reopened at LSN %d after undoing the gap, want %d", seed, cycle, again.durable, d)
			}
		}
		n.live, n.running, n.failed = res.index, false, false
	}
	return h
}

// gapNamedDurable damages the header of a random replayed record that a later whole header
// names durable, checks that recovery ends Corrupt at it, and undoes the damage; it reports
// whether it found such a record.
func gapNamedDurable(t *testing.T, h *harness, r *core.Rand, res *recovered, st *crashStats) bool {
	t.Helper()
	var candidates []int
	for i, ev := range res.replayed {
		if ev.LSN > res.replayLSN && ev.LSN > res.manifest.LSN && i+1 < len(res.replayed) && !checkpointRecord(ev) {
			candidates = append(candidates, i)
		}
	}
	if len(candidates) == 0 {
		return false
	}
	i := candidates[len(candidates)-1]
	if r.Chance(1, 2) {
		i = candidates[r.Below(uint64(len(candidates)))]
	}
	gap := res.replayed[i]
	namer := slices.IndexFunc(res.replayed[i+1:], func(ev disklog.Event) bool { return h.record(ev).Record.Durable >= gap.LSN })
	if namer < 0 {
		return false
	}
	st.gaps++
	if res.durable < gap.LSN+uint64(h.cfg.CommitDepth) {
		st.gapsInsideTheWindow++
	}
	off := int(r.Below(56))
	h.disk.FlipBit(gap.LBA, off, 1)
	wantCorrupt(t, h.recover(disklog.ModeOpen), gap.LSN)
	h.disk.FlipBit(gap.LBA, off, 1)
	return true
}

func checkpointRecord(ev disklog.Event) bool {
	return slices.ContainsFunc(ev.Entries, func(e disklog.Entry) bool { return e.Kind == disklog.EntryBlob && e.Owner != userBlobOwner })
}

func TestRandomCrashesKeepAcknowledgedRecords(t *testing.T) {
	var st crashStats
	for seed := uint64(1); seed <= 50; seed++ {
		h := crashSeed(t, seed, &st)
		if err := checkIncarnations(h.sched.Trace(), diskLogRules(h.cfg.CommitDepth)...); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		if err := sim.CompareTraces(h.sched.Trace(), crashSeed(t, seed, &crashStats{}).sched.Trace()); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
	}
	if st.keptInFlight <= 5 || st.droppedInFlight <= 5 || st.fromManifest <= 20 || st.noSpace == 0 || st.depth2 == 0 || st.depth3 == 0 || st.gapsInsideTheWindow == 0 || st.erased == 0 || st.restarts == 0 || st.crashesInRecovery == 0 {
		t.Fatalf("seeds missed a case: %+v", st)
	}
	t.Logf("%+v", st)
}

// FuzzRecover damages bytes of a log written with checkpoints and checks that recovery
// either fails with an error or resumes a log that keeps a new write across a restart.
func FuzzRecover(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 0, 16, 0, 1})
	f.Add([]byte{5, 0, 0, 0, 0xff, 9, 0, 40, 0, 1})
	f.Add([]byte{4, 0, 0, 0, 1})
	f.Fuzz(func(t *testing.T, damage []byte) {
		cfg := testConfig(4)
		cfg.MaxReplayBytes = 12 * core.BlockSize
		h := formatted(t, 1024*core.BlockSize, cfg)
		n := newNode(h, 64)
		for i := range 40 {
			h.submit(disklog.Hot, h.chunkWrite(uint32(i%8), 0, uint16(1+i%2), uint64(i+1)))
			if i%5 == 0 {
				h.submit(disklog.Cold, h.blob(disklog.BlobRaftEntries, userBlobOwner, 100+i, uint64(i)))
			}
			for {
				h.run()
				if !n.step() {
					break
				}
			}
		}
		for ; len(damage) >= 5; damage = damage[5:] {
			lba := core.LBA(binary.LittleEndian.Uint16(damage) % 1024)
			h.disk.FlipBit(lba, int(binary.LittleEndian.Uint16(damage[2:])%core.BlockSize), damage[4]|1)
		}
		for _, mode := range []disklog.Mode{disklog.ModeRebuild, disklog.ModeOpen} {
			res := h.recover(mode)
			if res.err == nil {
				continue
			}
			var ce *disklog.CorruptError
			var co *disklog.CopyOnlyError
			if !errors.As(res.err, &ce) && !errors.As(res.err, &co) && !errors.Is(res.err, disklog.ErrNotFormatted) {
				t.Fatalf("recovery failed with %v", res.err)
			}
			return
		}
		durable := h.log.DurableLSN()
		h.put(disklog.Hot, h.chunkWrite(99, 0, 1, 999))
		if h.log.DurableLSN() != durable+1 {
			return
		}
		res := h.reopen()
		if res.durable != durable+1 {
			t.Fatalf("recovered LSN %d, want the new write's %d", res.durable, durable+1)
		}
	})
}
