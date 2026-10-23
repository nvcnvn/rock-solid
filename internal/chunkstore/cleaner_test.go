package chunkstore

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/lsm"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// cleanGeo is a disk of 64 segments of 16 blocks, small enough to fill many times over.
var cleanGeo = disklog.Geometry{SegmentBlocks: 16, SegmentCount: 64}

func cleanConfig() Config {
	cfg := testConfig()
	cfg.Log.MaxReplayBytes = 32 * core.BlockSize
	cfg.Log.NoReserve, cfg.Log.Reserve = false, 8
	cfg.CleanFree = 4
	cfg.Log.CompactionStepBytes = core.BlockSize
	return cfg
}

// overwrite writes one block of one of chunks at random, each block a new pattern, and records it.
func overwrite(h *harness, r *core.Rand, chunks []core.ChunkID, blocks uint16) {
	h.t.Helper()
	c := chunks[r.Below(uint64(len(chunks)))]
	b := uint16(r.Below(uint64(blocks)))
	data := make([]byte, core.BlockSize)
	r.Fill(data)
	id := h.write(c, uint32(b)*core.BlockSize, data, 0)
	h.settle()
	h.ok(id)
}

// Uniform overwrites of a quarter of a small disk write it over many times: the cleaner keeps
// segments free, so no write fails, every block reads back, the usage table keeps the index's
// counts, and a restart reads the same.
func TestCleanerKeepsAFullDiskWriting(t *testing.T) {
	h := newHarnessOn(t, cleanConfig(), cleanGeo)
	h.merging = true
	r := core.NewRand(7)
	chunks := []core.ChunkID{chunk(1), chunk(2)}
	for range 4 * int(cleanGeo.DiskBlocks()) / 2 {
		overwrite(h, r, chunks, 64)
	}
	h.holds()
	st := h.s.CleanStats()
	if st.Freed < int(cleanGeo.SegmentCount) {
		t.Fatalf("the cleaner freed %d segments over 4 disks of writes", st.Freed)
	}
	if st.PeakPasses != 2 {
		t.Fatalf("at most %d passes ran at once; a second joins two segments below clean_free", st.PeakPasses)
	}
	for range 20 {
		h.advance(core.Tick(time.Second / time.Microsecond))
	}
	if len(h.trims) == 0 {
		t.Fatal("no freed segment was trimmed")
	}
	h.recoverStore(disklog.ModeOpen)
	h.holds()
	for range 200 {
		overwrite(h, r, chunks, 64)
	}
	h.holds()
}

// chooseVictims is the victims the cleaner's next pass would take.
func (s *Store) chooseVictims() []uint32 {
	victims, _ := s.pickVictims()
	return victims
}

// freeEmpty runs passes over the victims holding nothing live, which a pass takes first, until
// there are none, so that the next victims are those cost-benefit ranks.
func (h *harness) freeEmpty() {
	h.t.Helper()
	for range 10 {
		var empty []uint32
		for _, v := range h.s.chooseVictims() {
			if u, _ := h.s.Log().Usage(v); u.Live == 0 {
				empty = append(empty, v)
			}
		}
		if len(empty) == 0 {
			return
		}
		h.s.cl.passes = append(h.s.cl.passes, &cleanPass{victims: empty})
		h.clean(-1)
	}
	h.t.Fatalf("victims %v still hold nothing live", h.s.chooseVictims())
}

// primed is a store on the small disk whose cleaner does not run until a test sets CleanFree:
// two chunks of 64 blocks written once and overwritten at random until several manifests are named.
func primed(t *testing.T) *harness { return primedWith(t, cleanConfig()) }

func primedWith(t *testing.T, cfg Config) *harness {
	cfg.CleanFree = -1
	h := newHarnessOn(t, cfg, cleanGeo)
	h.merging = true
	r := core.NewRand(5)
	chunks := []core.ChunkID{chunk(1), chunk(2)}
	for _, c := range chunks {
		for b := range uint16(64) {
			data := make([]byte, core.BlockSize)
			r.Fill(data)
			id := h.write(c, uint32(b)*core.BlockSize, data, 0)
			h.settle()
			h.ok(id)
		}
	}
	for range 120 {
		overwrite(h, r, chunks, 64)
	}
	return h
}

// clean runs the cleaner with clean_free at want: a write, which the store answers once its
// cleaning has caught up with it.
func (h *harness) clean(want int) {
	h.t.Helper()
	h.s.cfg.CleanFree = want
	h.s.cl.tried = false
	h.read(chunk(1), 0, 1)
	h.settle()
}

// steppedStore is a primed store whose pieces hold a block, so that its index's runs span
// several, overwritten further with merges off, two more chunks among the overwrites so that a
// run's rewrite outgrows a step; merges run in steps of a block.
func steppedStore(t *testing.T) (*harness, *core.Rand) { return steppedStoreOf(t, 0) }

// steppedStoreOf is steppedStore with workers merge workers, or the default for 0.
func steppedStoreOf(t *testing.T, workers int) (*harness, *core.Rand) {
	cfg := cleanConfig()
	cfg.Log.MaxRecordBytes = 3 * core.BlockSize
	cfg.Log.MergeWorkers = workers
	h := primedWith(t, cfg)
	h.merging = false
	r := core.NewRand(9)
	for range 100 {
		overwrite(h, r, []core.ChunkID{chunk(1), chunk(2), chunk(3), chunk(4)}, 64)
	}
	return h, r
}

// markRewrites runs a pass over the cleanable metadata segment whose live runs have the most
// pieces, which copies nothing and marks those runs for rewrites, and returns the segment and how
// many runs it marked; merges stay off.
func markRewrites(t *testing.T, h *harness) (uint32, int) {
	t.Helper()
	log := h.s.Log()
	var seg uint32
	most := 0
	for s := uint32(1); s < cleanGeo.SegmentCount; s++ {
		pieces, held := h.s.Index().Tree().RunsIn(s, nil)
		if _, stream := log.Usage(s); stream == disklog.Meta && log.Cleanable(s) && len(pieces) > most && !held && !log.HoldsManifestPiece(s) {
			seg, most = s, len(pieces)
		}
	}
	if seg == 0 {
		t.Fatal("no cleanable metadata segment holding a piece of a live run")
	}
	before := h.s.CleanStats()
	h.merging = false
	h.s.cl.passes = append(h.s.cl.passes, &cleanPass{victims: []uint32{seg}})
	h.clean(-1)
	if st := h.s.CleanStats(); st.Rewrites == before.Rewrites || st.Copied != before.Copied || !log.HoldsPiece(seg) {
		t.Fatalf("stats %+v after a pass over segment %d; it holds a piece %v", st, seg, log.HoldsPiece(seg))
	}
	return seg, int(h.s.CleanStats().Rewrites - before.Rewrites)
}

// A victim holding a piece of a live run is not copied: the run is marked for a rewrite, which
// the next merge writes elsewhere in steps, each worker holding at most a step beyond the
// manifests, and once the manifest naming its last step is named the segment holds nothing live
// and the next pass frees it.
func TestLiveRunPiecesAreRewrittenNotCopied(t *testing.T) {
	h, r := steppedStore(t)
	log := h.s.Log()
	seg, marked := markRewrites(t, h)
	h.merging = true
	started := len(h.merges)
	for i := 0; log.HoldsPiece(seg) && i < 400; i++ {
		overwrite(h, r, []core.ChunkID{chunk(1), chunk(2)}, 64)
	}
	if log.HoldsPiece(seg) {
		t.Fatalf("segment %d still holds a live piece after the rewrite", seg)
	}
	rewrites := h.merges[started : started+marked]
	if !slices.ContainsFunc(rewrites, func(c *lsm.Compaction) bool { return h.steps[c] > 1 }) || h.maxStepHeld == 0 {
		t.Fatalf("no rewrite of the %d runs ran in steps, and a worker held at most %d bytes beyond the manifests", marked, h.maxStepHeld)
	}
	h.s.cl.passes = append(h.s.cl.passes, &cleanPass{victims: []uint32{seg}})
	h.clean(-1)
	if u, _ := log.Usage(seg); u.State != disklog.SegmentFree {
		t.Fatalf("segment %d is %+v after its runs were rewritten", seg, u)
	}
	h.holds()
}

// A rewrite in steps of a block writes, for each step, a filter partition, a root and a manifest
// beside its blocks; the net-gain guard charges them, so the rewrite of a victim's runs writes no
// more blocks than the guard charged it.
func TestARewriteInStepsWritesNoMoreThanItsCost(t *testing.T) {
	h, _ := steppedStore(t)
	log := h.s.Log()
	costs := map[uint32]uint64{}
	for s := uint32(1); s < cleanGeo.SegmentCount; s++ {
		if pieces, _ := h.s.Index().Tree().RunsIn(s, nil); len(pieces) > 0 {
			steps, extra := h.s.Index().Tree().RewriteSteps(s)
			costs[s] = log.RewriteCost(pieces, steps, extra)
		}
	}
	seg, marked := markRewrites(t, h)
	written := h.stepBlocks
	var rewrites []*lsm.Compaction
	for c := h.s.MergeDue(); c != nil; c = h.s.MergeDue() {
		if !slices.Contains(rewrites, c) {
			if len(rewrites) == marked {
				break
			}
			rewrites = append(rewrites, c)
		}
		c.Run()
		if err := h.s.InstallMerge(&h.ctx, c, &h.out); err != nil {
			t.Fatal(err)
		}
		h.take()
		h.settle()
	}
	if slices.ContainsFunc(rewrites, func(c *lsm.Compaction) bool { return !c.Finished() }) {
		t.Fatalf("of the %d rewrites of segment %d's runs, one did not finish", marked, seg)
	}
	if written = h.stepBlocks - written; written > costs[seg] {
		t.Fatalf("the rewrite of segment %d's runs wrote %d blocks, charged %d", seg, written, costs[seg])
	}
	h.holds()
}

// runToMidStep runs the store's due merge steps until one installs a step its merge does not
// end with, and returns that merge once the manifest naming the step is durable.
func runToMidStep(t *testing.T, h *harness) *lsm.Compaction {
	t.Helper()
	for {
		c := h.s.MergeDue()
		if c == nil {
			t.Fatal("no merge ran in steps")
		}
		c.Run()
		if err := h.s.InstallMerge(&h.ctx, c, &h.out); err != nil {
			t.Fatal(err)
		}
		h.take()
		h.settle()
		if !c.Finished() {
			return c
		}
	}
}

// A crash between two steps of a rewrite recovers its input, bounded by the first step, and the
// output so far from the manifest naming that step; the rewrite resumes past the bound and the
// segment's pieces die once it ends.
func TestARewriteResumesAfterACrashBetweenSteps(t *testing.T) {
	h, r := steppedStore(t)
	seg, _ := markRewrites(t, h)
	runToMidStep(t, h)
	h.disk.Crash(func(int, core.LBA) bool { return true })
	h.recoverStore(disklog.ModeOpen)
	h.merging = true
	for i := 0; h.s.Log().HoldsPiece(seg) && i < 400; i++ {
		overwrite(h, r, []core.ChunkID{chunk(1), chunk(2)}, 64)
	}
	if h.s.Log().HoldsPiece(seg) {
		t.Fatalf("segment %d still holds a live piece after the resumed rewrite", seg)
	}
	h.holds()
}

// A disk filled to its reserve still finishes a rewrite in steps: each step's checkpoint may write
// to the merge worker's lane in the reserve, which ordinary writes never open.
func TestARewriteFinishesOnADiskFilledToItsReserve(t *testing.T) {
	h, r := steppedStore(t)
	for i := 0; h.s.Log().FreeOutsideReserve() > 0; i++ {
		if i == 2000 {
			t.Fatal("the disk never filled to its reserve")
		}
		overwrite(h, r, []core.ChunkID{chunk(1), chunk(2)}, 64)
	}
	markRewrites(t, h)
	if free := h.s.Log().FreeOutsideReserve(); free > 0 {
		t.Fatalf("the pass left %d segments outside the reserve free", free)
	}
	var merges []*lsm.Compaction
	stepped := false
	for c := h.s.MergeDue(); c != nil; c = h.s.MergeDue() {
		c.Run()
		if err := h.s.InstallMerge(&h.ctx, c, &h.out); err != nil {
			t.Fatal(err)
		}
		h.take()
		h.settle()
		stepped = stepped || slices.Contains(merges, c)
		if !slices.Contains(merges, c) {
			merges = append(merges, c)
		}
	}
	if !stepped || slices.ContainsFunc(merges, func(c *lsm.Compaction) bool { return !c.Finished() }) {
		t.Fatalf("of %d merges, none ran in steps or one did not finish", len(merges))
	}
	h.holds()
}

// With the cleaner running only for writes that wait, the disk fills to its reserve; a pass then
// copies into the reserve and the waiting writes go out, none failing while there is a victim.
func TestAPassFinishesOnADiskFilledToItsReserve(t *testing.T) {
	h := primed(t)
	r := core.NewRand(13)
	low, passes := h.s.Log().FreeOutsideReserve(), h.s.CleanStats().Passes
	for range 600 {
		overwrite(h, r, []core.ChunkID{chunk(1), chunk(2)}, 64)
		low = min(low, h.s.Log().FreeOutsideReserve())
	}
	if low > 0 || h.s.CleanStats().Passes == passes {
		t.Fatalf("free segments outside the reserve fell to %d; %d passes", low, h.s.CleanStats().Passes-passes)
	}
	h.holds()
}

// Overwrites of four chunks on a disk with a reserve of 4 segments and a cleaner that runs only for
// writes waiting for room: a pass can end with writes still waiting, its copies having taken what
// it freed, and the store then starts the next pass rather than failing them, so no write fails
// with NoSpace while a victim is left. Write 2,163 is the one that did.
func TestAPassEndingWithWritesWaitingStartsTheNext(t *testing.T) {
	cfg := cleanConfig()
	cfg.Log.Reserve, cfg.CleanFree = 4, -1
	h := newHarnessOn(t, cfg, cleanGeo)
	h.merging = true
	r := core.NewRand(1)
	chunks := []core.ChunkID{chunk(1), chunk(2), chunk(3), chunk(4)}
	for i := range 2200 {
		c, b := chunks[r.Below(4)], uint32(r.Below(64))
		if i < 4*64 {
			c, b = chunks[i/64], uint32(i%64)
		}
		data := make([]byte, core.BlockSize)
		r.Fill(data)
		id := h.write(c, b*core.BlockSize, data, 0)
		h.settle()
		if err := h.reply(id).Err; err != nil {
			t.Fatalf("write %d: %v, with victims %v", i, err, h.s.chooseVictims())
		}
	}
}

// Writes of new blocks fill the disk with live data until no victim gains space and writes waiting
// for room fail with NoSpace, whatever the reserve; invNoSpace checks none fails earlier.
func TestWritesFailOnlyWhenNoVictimGainsSpace(t *testing.T) {
	for _, reserve := range []uint32{8, 10, 12} {
		t.Run(fmt.Sprintf("reserve %d", reserve), func(t *testing.T) {
			cfg := cleanConfig()
			cfg.Log.Reserve = reserve
			h := newHarnessOn(t, cfg, cleanGeo)
			h.merging = true
			for i := 0; ; i++ {
				id := h.write(chunk(uint32(10+i/64)), uint32(i%64)*core.BlockSize, fill(core.BlockSize, byte(i)), 0)
				h.settle()
				if err := h.reply(id).Err; err != nil {
					if !errors.Is(err, disklog.ErrNoSpace) {
						t.Fatalf("write %d: %v", i, err)
					}
					return
				}
			}
		})
	}
}

// With a reserve of one segment, below what the formula asks, the cleaner's copies and the
// checkpoints take the disk to zero free segments once writes of new blocks fill it, after
// overwrites. A victim whose copies do not fit the room the cleaner's lane can reach is not taken,
// and a pass that frees nothing starts no other until the durable LSN moves, so a write that finds
// no room fails with NoSpace at once, no pass starting and nothing copied for it.
func TestAFullDiskWithNoFreeSegmentEndsInNoSpace(t *testing.T) {
	cfg := cleanConfig()
	cfg.Log.Reserve = 1
	h := primedWith(t, cfg)
	r := core.NewRand(12)
	log := h.s.Log()
	failed, empty := 0, false
	try := func(c core.ChunkID, b uint32, data []byte) {
		t.Helper()
		st := h.s.CleanStats()
		id := h.write(c, b*core.BlockSize, data, 0)
		h.settle()
		empty = empty || log.FreeOutsideReserve() < 0
		switch err := h.reply(id).Err; {
		case err == nil:
			h.ok(id)
		case !errors.Is(err, disklog.ErrNoSpace):
			t.Fatalf("write %d: %v", failed, err)
		case h.s.CleanStats().Copied != st.Copied:
			t.Fatalf("a write failed after the cleaner copied: %+v, then %+v", st, h.s.CleanStats())
		default:
			failed++
		}
	}
	chunks := []core.ChunkID{chunk(1), chunk(2), chunk(3), chunk(4)}
	for range 1500 {
		data := make([]byte, core.BlockSize)
		r.Fill(data)
		try(chunks[r.Below(4)], uint32(r.Below(64)), data)
	}
	for i := 0; failed < 20; i++ {
		if i == 2000 {
			t.Fatal("writes of new blocks never failed")
		}
		try(chunk(uint32(10+i/64)), uint32(i%64), fill(core.BlockSize, byte(i)))
	}
	if !empty {
		t.Fatal("the disk never ran out of free segments")
	}
	h.holds()
}

// A whole chunk's record header, which holds the checksums of its slot, stays live while the chunk
// does: the usage table counts the slot's blocks against the header's segment, so the cleaner
// never frees it, and the chunk reads back after every pass.
func TestTheCleanerKeepsTheHeadersOfLiveSlotWrites(t *testing.T) {
	cfg := cleanConfig()
	cfg.Log.MaxReplayBytes = 64 * core.BlockSize
	h := newHarnessOn(t, cfg, slotGeometry)
	whole := []core.ChunkID{{Volume: 3, Inode: 1, Index: 1}, {Volume: 3, Inode: 1, Index: 2}}
	for i, c := range whole {
		h.sync(h.write(c, 0, pattern(uint64(i), core.ChunkSize), 0))
	}
	for i := range 300 {
		data := fill(64*core.BlockSize, byte(i))
		id := h.write(chunk(1), 0, data, 0)
		h.settle()
		h.ok(id)
	}
	h.clean(1000)
	for i, c := range whole {
		if got := h.readNow(c, 0, core.ChunkSize); !bytes.Equal(got, pattern(uint64(i), core.ChunkSize)) {
			t.Fatalf("whole chunk %d does not read back after cleaning", i)
		}
	}
	if h.s.CleanStats().Freed == 0 {
		t.Fatal("the cleaner freed nothing")
	}
	h.holds()
}

// A slot lane segment whose live blocks are only a slot write its record header holds the
// checksums of is never freed while the chunk lives, so once the lane moves on it is never a
// victim, however short of clean_free, and a restart that loads the chunk's record from the
// index's runs counts it again. Rewrites of another whole chunk fill the lane past the segment.
func TestASegmentOfSlotHeadersAloneIsNoVictim(t *testing.T) {
	cfg := cleanConfig()
	cfg.Log.MaxReplayBytes = 64 * core.BlockSize
	h := newHarnessOn(t, cfg, slotGeometry)
	whole, other := core.ChunkID{Volume: 3, Inode: 1, Index: 1}, core.ChunkID{Volume: 3, Inode: 1, Index: 2}
	h.sync(h.write(whole, 0, pattern(0, core.ChunkSize), 0))
	e, _ := h.s.Index().Lookup(testPool, whole, 0)
	seg := uint32(uint64(e.Header) / uint64(slotGeometry.SegmentBlocks))
	for i := 0; ; i++ {
		if i == 5000 {
			t.Fatal("the slot lane never left the segment of the first chunk's header")
		}
		h.sync(h.write(other, 0, pattern(uint64(i+1), core.ChunkSize), 0))
		if o, _ := h.s.Index().Lookup(testPool, other, 0); uint32(uint64(o.Header)/uint64(slotGeometry.SegmentBlocks)) != seg {
			break
		}
	}
	for i := range 300 {
		id := h.write(chunk(1), 0, fill(64*core.BlockSize, byte(i)), 0)
		h.settle()
		h.ok(id)
	}
	h.clean(1000)
	if u, _ := h.s.Log().Usage(seg); u.State != disklog.SegmentUsed || u.Live != core.BlocksPerChunk || h.s.Index().SlotHeld(seg) != core.BlocksPerChunk {
		t.Fatalf("segment %d holding the whole chunk's header is %+v, %d slot-held", seg, u, h.s.Index().SlotHeld(seg))
	}
	if !h.s.Log().Cleanable(seg) || slices.Contains(h.s.chooseVictims(), seg) {
		t.Fatalf("segment %d: cleanable %v, victims %v", seg, h.s.Log().Cleanable(seg), h.s.chooseVictims())
	}
	h.recoverStore(disklog.ModeOpen)
	if got := h.s.Index().SlotHeld(seg); got != core.BlocksPerChunk || slices.Contains(h.s.chooseVictims(), seg) {
		t.Fatalf("after a restart segment %d holds %d slot blocks' headers; victims %v", seg, got, h.s.chooseVictims())
	}
	if got := h.readNow(whole, 0, core.ChunkSize); !bytes.Equal(got, pattern(0, core.ChunkSize)) {
		t.Fatal("the whole chunk does not read back")
	}
}

// A pass whose every victim holds a block that fails its checksum quarantines them and frees
// nothing, so the cleaner, one worker short of clean_free, starts no other until the durable LSN
// moves, however often the store wakes; a write moves it, and the next pass frees segments.
func TestAPassThatFreesNothingWaitsForTheLog(t *testing.T) {
	h := primed(t)
	log := h.s.Log()
	r := core.NewRand(9)
	copied := func(v uint32) bool { u, stream := log.Usage(v); return stream != disklog.Meta && u.Live > 0 }
	victims := h.s.chooseVictims()
	for i := 0; slices.ContainsFunc(victims, func(v uint32) bool { return !copied(v) }) && i < 1000; i++ {
		overwrite(h, r, []core.ChunkID{chunk(1), chunk(2)}, 64)
		victims = h.s.chooseVictims()
	}
	h.inv.Off(invReads)
	for _, v := range victims {
		lo := cleanGeo.LBA(v, 0)
		if !copied(v) {
			t.Fatalf("victim %d of %v has nothing to copy", v, victims)
		}
		rec := h.s.Index().Records(nil, lo, lo+core.LBA(cleanGeo.SegmentBlocks))[0]
		var at core.LBA
		for _, c := range []core.ChunkID{chunk(1), chunk(2)} {
			for b := range uint16(64) {
				if e, ok := h.s.Index().Lookup(testPool, c, b); ok && e.Header == rec.Header {
					at = e.BlockLBA(b)
				}
			}
		}
		corrupt := h.disk.Read(at, 1)
		corrupt[100] ^= 0xff
		h.disk.Put(at, corrupt)
	}
	was := h.s.CleanStats()
	h.clean(log.FreeOutsideReserve() + 1)
	st := h.s.CleanStats()
	if st.Passes != was.Passes+1 || st.Freed != was.Freed || st.Quarantined != len(victims) {
		t.Fatalf("stats %+v from %+v over victims %v", st, was, victims)
	}
	lsn := log.DurableLSN()
	for range 5 {
		h.read(chunk(1), 0, 1)
		h.settle()
	}
	if got := h.s.CleanStats().Passes; got != st.Passes || log.DurableLSN() != lsn {
		t.Fatalf("%d passes after one freed nothing, the durable LSN at %d of %d", got-st.Passes, log.DurableLSN(), lsn)
	}
	h.sync(h.write(chunk(1), 0, fill(core.BlockSize, 1), 0))
	if got := h.s.CleanStats(); got.Passes == st.Passes || got.Freed == st.Freed {
		t.Fatalf("stats %+v after a write", got)
	}
}

// A pass takes the 4 best victims by cost-benefit among the segments it may clean whose copy
// costs less than a segment, header blocks included, that of the segment it opens too.
func TestAPassTakesTheFourBestVictimsThatGainSpace(t *testing.T) {
	h := primed(t)
	log := h.s.Log()
	victims := h.s.chooseVictims()
	if len(victims) != disklog.VictimsPerPass {
		t.Fatalf("%d victims", len(victims))
	}
	score := func(seg uint32) float64 {
		u, _ := log.Usage(seg)
		return costBenefit(min(float64(u.Live)/float64(cleanGeo.SegmentBlocks-1), 1), log.Age(seg))
	}
	worst := score(victims[len(victims)-1])
	for seg := uint32(1); seg < cleanGeo.SegmentCount; seg++ {
		u, stream := log.Usage(seg)
		chosen := slices.Contains(victims, seg)
		gains := stream == disklog.Meta || log.CopyCost(uint64(u.Live))+1 < uint64(cleanGeo.SegmentBlocks)
		if chosen && (!log.Cleanable(seg) || !gains) {
			t.Fatalf("victim %d is not cleanable or costs more than it frees: %+v", seg, u)
		}
		if !chosen && log.Cleanable(seg) && gains && stream != disklog.Meta && score(seg) > worst {
			t.Fatalf("segment %d scores %.1f, above victim %d's %.1f, and was left", seg, score(seg), victims[len(victims)-1], worst)
		}
	}
}

// A used segment with nothing live costs nothing to free, so a pass takes such segments ahead of
// any with blocks to copy, however young. Cost-benefit alone scores one its age, which counts only
// ordinary writes: the primed disk's metadata segments emptied by merges ranked below segments
// with live blocks, and one the index writes while writes wait for room stays at age 0.
func TestVictimsWithNothingLiveComeFirst(t *testing.T) {
	h := primed(t)
	log := h.s.Log()
	empty := 0
	for seg := uint32(1); seg < cleanGeo.SegmentCount; seg++ {
		if u, _ := log.Usage(seg); u.State == disklog.SegmentUsed && u.Live == 0 && log.Cleanable(seg) {
			empty++
		}
	}
	if empty <= disklog.VictimsPerPass {
		t.Fatalf("%d cleanable segments hold nothing live", empty)
	}
	for _, v := range h.s.chooseVictims() {
		if u, _ := log.Usage(v); u.Live != 0 {
			t.Fatalf("victim %d holds %d live blocks, age %d, while %d segments hold none", v, u.Live, log.Age(v), empty)
		}
	}
}

// A used segment with no live block is freed without reading or copying anything.
func TestAVictimWithNothingLiveIsFreedWithoutCopying(t *testing.T) {
	h := primed(t)
	log := h.s.Log()
	var empty uint32
	for seg := uint32(1); seg < cleanGeo.SegmentCount && empty == 0; seg++ {
		if u, stream := log.Usage(seg); u.State == disklog.SegmentUsed && u.Live > 0 && stream != disklog.Meta && log.Cleanable(seg) {
			empty = seg
		}
	}
	r := core.NewRand(11)
	for _, c := range []core.ChunkID{chunk(1), chunk(2)} {
		for b := range uint16(64) {
			if e, ok := h.s.Index().Lookup(testPool, c, b); ok && uint32(uint64(e.Header)/uint64(cleanGeo.SegmentBlocks)) == empty {
				data := make([]byte, core.BlockSize)
				r.Fill(data)
				id := h.write(c, uint32(b)*core.BlockSize, data, 0)
				h.settle()
				h.ok(id)
			}
		}
	}
	if u, _ := log.Usage(empty); u.State != disklog.SegmentUsed || u.Live != 0 || !log.Cleanable(empty) {
		t.Fatalf("segment %d is %+v, cleanable %v, after its blocks were overwritten", empty, u, log.Cleanable(empty))
	}
	mark := len(h.acts)
	before := h.s.CleanStats()
	seq := h.seqOf[empty]
	h.clean(100)
	if u, _ := log.Usage(empty); u.State != disklog.SegmentFree && h.seqOf[empty] == seq {
		t.Fatalf("empty segment %d is %+v after a pass, neither free nor opened again", empty, u)
	}
	for _, a := range h.acts[mark:] {
		if uint64(a.LBA)/uint64(cleanGeo.SegmentBlocks) != uint64(empty) {
			continue
		}
		if a.Kind == core.ActDiskWrite {
			break
		}
		if a.Kind == core.ActDiskRead {
			t.Fatalf("the cleaner read %+v of the empty segment", a)
		}
	}
	if after := h.s.CleanStats(); after.Freed <= before.Freed {
		t.Fatalf("stats %+v after %+v", after, before)
	}
	h.holds()
}

// The worker groups a victim's blocks by chunk: copies go out in the index's key order, each
// chunk's pieces side by side and ascending.
func TestCopiesGoOutInKeyOrder(t *testing.T) {
	pool := sim.NewPool(1<<20, 0)
	j := cleanJob{}
	for _, p := range []struct {
		c core.ChunkID
		b uint16
	}{{chunk(7), 3}, {chunk(3), 1}, {chunk(7), 4}, {chunk(3), 2}} {
		buf, err := pool.AllocZeroed(core.BlockSize)
		if err != nil {
			t.Fatal(err)
		}
		j.pieces = append(j.pieces, cleanPiece{pool: 1, chunk: p.c, first: p.b, count: 1, crc: len(j.crcs), buf: buf})
		j.crcs = append(j.crcs, core.CRC32C(make([]byte, core.BlockSize)))
	}
	j.Run(pool)
	var keys [][]byte
	for _, p := range j.pieces {
		keys = append(keys, AppendKey(nil, Key{Pool: p.pool, Chunk: p.chunk, First: p.first}))
	}
	if j.bad || !slices.IsSortedFunc(keys, bytes.Compare) || j.pieces[0].chunk != j.pieces[1].chunk || j.pieces[2].chunk != j.pieces[3].chunk {
		t.Fatalf("pieces after Run: %+v", j.pieces)
	}
	pool.Bytes(j.pieces[2].buf)[9] ^= 1
	j.Run(pool)
	if !j.bad {
		t.Fatal("a block whose checksum fails passed")
	}
}

// A victim holding a block whose checksum fails is quarantined: nothing of it is copied, it is
// never a victim again, the other victims are cleaned, and reads of the block keep failing.
func TestAVictimWithABadBlockIsQuarantined(t *testing.T) {
	h := primed(t)
	log := h.s.Log()
	h.freeEmpty()
	victims := h.s.chooseVictims()
	var bad uint32
	var at core.LBA
	for _, v := range victims {
		if u, stream := log.Usage(v); stream != disklog.Meta && u.Live > 0 {
			bad = v
			break
		}
	}
	lo := cleanGeo.LBA(bad, 0)
	rec := h.s.Index().Records(nil, lo, lo+core.LBA(cleanGeo.SegmentBlocks))[0]
	for _, c := range []core.ChunkID{chunk(1), chunk(2)} {
		for b := range uint16(64) {
			if e, ok := h.s.Index().Lookup(testPool, c, b); ok && e.Header == rec.Header && at == 0 {
				at = e.BlockLBA(b)
				h.inv.Off(invReads)
				corrupt := h.disk.Read(at, 1)
				corrupt[100] ^= 0xff
				h.disk.Put(at, corrupt)
				id := h.read(c, uint32(b)*core.BlockSize, 1)
				h.settle()
				if err := h.reply(id).Err; !errors.Is(err, disklog.ErrBadChecksum) {
					t.Fatalf("read of the damaged block: %v", err)
				}
				defer func(c core.ChunkID, b uint16) {
					id := h.read(c, uint32(b)*core.BlockSize, 1)
					h.settle()
					if err := h.reply(id).Err; !errors.Is(err, disklog.ErrBadChecksum) {
						t.Fatalf("read of the damaged block after the pass: %v", err)
					}
				}(c, b)
			}
		}
	}
	if at == 0 {
		t.Fatalf("no live block of victim %d's record at %d", bad, rec.Header)
	}
	h.clean(100)
	if u, _ := log.Usage(bad); u.State != disklog.SegmentQuarantined || h.s.CleanStats().Quarantined != 1 {
		t.Fatalf("victim %d is %+v, stats %+v", bad, u, h.s.CleanStats())
	}
	if h.s.CleanStats().Freed == 0 || slices.Contains(h.s.chooseVictims(), bad) {
		t.Fatalf("stats %+v; the quarantined segment is a victim again: %v", h.s.CleanStats(), slices.Contains(h.s.chooseVictims(), bad))
	}
}

// Blocks whose record lies in a quarantined segment may still be overwritten and punched, which
// lowers its live count, before a restart and after one, which keeps it quarantined.
func TestAQuarantinedSegmentsBlocksAreOverwritten(t *testing.T) {
	h := primed(t)
	log := h.s.Log()
	h.freeEmpty()
	var bad uint32
	var most uint32
	for _, v := range h.s.chooseVictims() {
		if u, stream := log.Usage(v); stream != disklog.Meta && u.Live > most {
			bad, most = v, u.Live
		}
	}
	lo := cleanGeo.LBA(bad, 0)
	type block struct {
		c core.ChunkID
		b uint16
	}
	var held []block
	for _, c := range []core.ChunkID{chunk(1), chunk(2)} {
		for b := range uint16(64) {
			if e, ok := h.s.Index().Lookup(testPool, c, b); ok && e.Header >= lo && e.Header < lo+core.LBA(cleanGeo.SegmentBlocks) {
				held = append(held, block{c, b})
			}
		}
	}
	if len(held) < 2 {
		t.Fatalf("victim %d holds %d live blocks", bad, len(held))
	}
	at, _ := h.s.Index().Lookup(testPool, held[0].c, held[0].b)
	h.inv.Off(invReads)
	corrupt := h.disk.Read(at.BlockLBA(held[0].b), 1)
	corrupt[100] ^= 0xff
	h.disk.Put(at.BlockLBA(held[0].b), corrupt)
	h.clean(100)
	h.s.cfg.CleanFree = -1
	overwriteHeld := func(bl block) {
		data := fill(core.BlockSize, byte(bl.b))
		id := h.write(bl.c, uint32(bl.b)*core.BlockSize, data, 0)
		h.settle()
		h.ok(id)
	}
	quarantined := func() {
		t.Helper()
		if u, _ := log.Usage(bad); u.State != disklog.SegmentQuarantined {
			t.Fatalf("segment %d is %+v", bad, u)
		}
	}
	quarantined()
	overwriteHeld(held[0])
	h.inv.On(invReads)
	h.recoverStore(disklog.ModeOpen)
	h.s.cfg.CleanFree = -1
	log = h.s.Log()
	quarantined()
	for _, bl := range held[1:] {
		overwriteHeld(bl)
	}
	h.holds()
	quarantined()
}

// A victim holding 64 single blocks of one chunk, each between blocks of another since
// overwritten, and overwrites of a third that roll the log past it, is copied with no more than the read depth of its pieces read at once, and every
// block reads back.
func TestAVictimOfManyPiecesReadsThemAtTheReadDepth(t *testing.T) {
	h := newHarnessOn(t, cleanConfig(), disklog.Geometry{SegmentBlocks: 256, SegmentCount: 64})
	h.merging = true
	r := core.NewRand(13)
	put := func(c core.ChunkID, b uint16) {
		data := make([]byte, core.BlockSize)
		r.Fill(data)
		id := h.write(c, uint32(b)*core.BlockSize, data, 0)
		h.settle()
		h.ok(id)
	}
	for b := range uint16(64) {
		put(chunk(1), b)
		put(chunk(2), b)
	}
	for b := range uint16(64) {
		put(chunk(2), b)
	}
	for range 400 {
		put(chunk(3), uint16(r.Below(16)))
	}
	h.clean(1000)
	if h.s.CleanStats().Copied < 64 || h.maxCleanReads != cleanReadDepth {
		t.Fatalf("stats %+v; at most %d reads in flight", h.s.CleanStats(), h.maxCleanReads)
	}
	h.holds()
}

// The net-gain guard charges a chunk victim the index entries its moves write, one per moved
// block of 67 bytes at most, flushed and merged again: in 64 MiB segments of 1 MiB records a
// victim of 15,434 live blocks costs 15,877 blocks of copies and 505 of entries, 16,382, and is
// picked, while 15,435 cost 16,383 and are refused.
func TestTheGuardChargesTheIndexEntriesOfMoves(t *testing.T) {
	h := newHarnessOn(t, testConfig(), disklog.Geometry{SegmentBlocks: 16384, SegmentCount: 8})
	log := h.s.Log()
	if movedEntryBytes != 2*67 {
		t.Fatalf("a moved block's entry is charged %d bytes", movedEntryBytes)
	}
	for _, c := range []struct{ live, copies, entries uint64 }{{1, 8, 1}, {15434, 15877, 505}, {15435, 15878, 505}} {
		if got, entries := log.CopyCost(c.live), moveCost(c.live); got != c.copies || entries != c.entries {
			t.Fatalf("%d live blocks cost %d of copies and %d of entries, want %d and %d", c.live, got, entries, c.copies, c.entries)
		}
	}
}

// Planted violation: run pieces on a merge worker's lane past a step and its partition and root
// with no manifest between break invHeld, and a manifest between them does not.
func TestNoteStepsCatchesAStepPastItsBound(t *testing.T) {
	const seg = 5
	h := newHarnessOn(t, cleanConfig(), cleanGeo)
	piece := h.s.Log().MaxBlobBytes()
	run := disklog.Entry{Kind: disklog.EntryBlob, BlobKind: disklog.BlobRun, Length: piece}
	manifest := disklog.Entry{Kind: disklog.EntryBlob, BlobKind: disklog.BlobManifest, Length: 1}
	n := int((max(h.cfg.Log.CompactionStepBytes, piece)+2*piece)/piece) + 1
	held := func(entries []disklog.Entry) error {
		h := newHarnessOn(t, cleanConfig(), cleanGeo)
		h.laneOf[seg], h.catch = 1, true
		return violated(func() {
			h.noteSteps(seg, entries)
			h.holds()
		})
	}
	if err := held(slices.Repeat([]disklog.Entry{run}, n)); violationOf(err) != invHeld {
		t.Fatalf("%d run pieces of %d bytes past a manifest broke %v", n, piece, err)
	}
	if err := held(append(slices.Repeat([]disklog.Entry{run}, n-1), manifest, run)); err != nil {
		t.Fatal(err)
	}
}

// With two merge workers, the rewrites a cleaner pass marks run beside the merges overwrites
// make due, two at once, each on its own worker's lane within its step, and every block reads
// back, after a restart too.
func TestTwoMergesRunAtOnce(t *testing.T) {
	h, r := steppedStoreOf(t, 2)
	markRewrites(t, h)
	h.merging = true
	for i := 0; h.maxMerging < 2; i++ {
		if i == 2000 {
			t.Fatalf("no two of %d merges ran at once", len(h.merges))
		}
		overwrite(h, r, []core.ChunkID{chunk(1), chunk(2)}, 64)
	}
	workers := map[int]bool{}
	for _, c := range h.merges {
		workers[c.Worker()] = true
	}
	if len(workers) != 2 {
		t.Fatalf("merges ran on workers %v", workers)
	}
	h.holds()
	h.recoverStore(disklog.ModeOpen)
	h.holds()
}
