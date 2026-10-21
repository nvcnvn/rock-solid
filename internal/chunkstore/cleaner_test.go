package chunkstore

import (
	"bytes"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// cleanGeo is a disk of 64 segments of 16 blocks, small enough to fill many times over.
var cleanGeo = disklog.Geometry{SegmentBlocks: 16, SegmentCount: 64}

func cleanConfig() Config {
	cfg := testConfig()
	cfg.Log.MaxReplayBytes = 32 * core.BlockSize
	cfg.Log.NoReserve, cfg.Log.Reserve = false, 8
	cfg.CleanFree = 4
	return cfg
}

// merge runs the index's due merges, rewrites of runs the cleaner marked first, as the daemon
// does off the loop, each to its end.
func (h *harness) merge() {
	h.t.Helper()
	if h.noMerge {
		return
	}
	for c := h.s.MergeDue(); c != nil; c = h.s.MergeDue() {
		c.Run()
		if err := h.s.InstallMerge(&h.ctx, c, &h.out); err != nil {
			h.t.Fatal(err)
		}
		h.take()
		h.settle()
	}
}

// overwrite writes one block of one of chunks at random, each block a new pattern, and records it.
func overwrite(h *harness, o *byteOracle, r *core.Rand, chunks []core.ChunkID, blocks uint16) {
	h.t.Helper()
	c := chunks[r.Below(uint64(len(chunks)))]
	b := uint16(r.Below(uint64(blocks)))
	data := make([]byte, core.BlockSize)
	r.Fill(data)
	id := h.write(c, uint32(b)*core.BlockSize, data, 0)
	h.settle()
	h.merge()
	o.apply(h.t, c, uint32(b)*core.BlockSize, core.BlockSize, data, h.ok(id), 0)
}

// Uniform overwrites of a quarter of a small disk write it over many times: the cleaner keeps
// segments free, so no write fails, every block reads back, the usage table keeps the index's
// counts, and a restart reads the same.
func TestCleanerKeepsAFullDiskWriting(t *testing.T) {
	h := newHarnessOn(t, cleanConfig(), cleanGeo)
	r := core.NewRand(7)
	chunks := []core.ChunkID{chunk(1), chunk(2)}
	o := newByteOracle()
	for range 4 * int(cleanGeo.DiskBlocks()) / 2 {
		overwrite(h, o, r, chunks, 64)
	}
	o.check(t, h, 0, 63, 0)
	st := h.s.CleanStats()
	if st.Freed < int(cleanGeo.SegmentCount) {
		t.Fatalf("the cleaner freed %d segments over 4 disks of writes", st.Freed)
	}
	if st.PeakPasses != 2 {
		t.Fatalf("at most %d passes ran at once; a second joins two segments below clean_free", st.PeakPasses)
	}
	if per := float64(st.Copied) / float64(st.Freed); per >= float64(cleanGeo.SegmentBlocks-1) {
		t.Fatalf("the cleaner copied %.1f blocks per segment it freed, a segment's worth", per)
	}
	for range 20 {
		h.advance(core.Tick(time.Second / time.Microsecond))
	}
	if len(h.trims) == 0 {
		t.Fatal("no freed segment was trimmed")
	}
	h.recoverStore(disklog.ModeOpen)
	o.check(t, h, 0, 63, 0)
	for range 200 {
		overwrite(h, o, r, chunks, 64)
	}
	o.check(t, h, 0, 63, 0)
}

// primed is a store on the small disk whose cleaner does not run until a test sets CleanFree:
// two chunks of 64 blocks written once and overwritten at random until several manifests are named.
func primed(t *testing.T) (*harness, *byteOracle) {
	cfg := cleanConfig()
	cfg.CleanFree = -1
	h := newHarnessOn(t, cfg, cleanGeo)
	r := core.NewRand(5)
	o := newByteOracle()
	chunks := []core.ChunkID{chunk(1), chunk(2)}
	for _, c := range chunks {
		for b := range uint16(64) {
			data := make([]byte, core.BlockSize)
			r.Fill(data)
			id := h.write(c, uint32(b)*core.BlockSize, data, 0)
			h.settle()
			o.apply(t, c, uint32(b)*core.BlockSize, core.BlockSize, data, h.ok(id), 0)
		}
	}
	for range 120 {
		overwrite(h, o, r, chunks, 64)
	}
	return h, o
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

// A victim holding a piece of a live run is not copied: the run is marked for a rewrite, which
// the next merge writes elsewhere, and once the manifest naming the rewrite is named the segment
// holds nothing live and the next pass frees it.
func TestLiveRunPiecesAreRewrittenNotCopied(t *testing.T) {
	h, o := primed(t)
	h.noMerge = true
	r := core.NewRand(9)
	for range 100 {
		overwrite(h, o, r, []core.ChunkID{chunk(1), chunk(2)}, 64)
	}
	h.noMerge = false
	log := h.s.Log()
	var seg uint32
	for s := uint32(1); s < cleanGeo.SegmentCount && seg == 0; s++ {
		bytes, held := h.s.Index().Tree().RunsIn(s)
		if _, stream := log.Usage(s); stream == disklog.Meta && log.Cleanable(s) && bytes > 0 && !held && !log.HoldsManifestPiece(s) {
			seg = s
		}
	}
	if seg == 0 {
		t.Fatal("no cleanable metadata segment holding a piece of a live run")
	}
	copied := h.s.CleanStats().Copied
	h.s.cl.passes = append(h.s.cl.passes, &cleanPass{victims: []uint32{seg}})
	h.clean(-1)
	if st := h.s.CleanStats(); st.Rewrites == 0 || st.Copied != copied || !log.HoldsPiece(seg) {
		t.Fatalf("stats %+v after a pass over segment %d; it holds a piece %v", st, seg, log.HoldsPiece(seg))
	}
	for i := 0; log.HoldsPiece(seg) && i < 400; i++ {
		overwrite(h, o, r, []core.ChunkID{chunk(1), chunk(2)}, 64)
	}
	if log.HoldsPiece(seg) {
		t.Fatalf("segment %d still holds a live piece after the rewrite", seg)
	}
	h.s.cl.passes = append(h.s.cl.passes, &cleanPass{victims: []uint32{seg}})
	h.clean(-1)
	if u, _ := log.Usage(seg); u.State != disklog.SegmentFree {
		t.Fatalf("segment %d is %+v after its runs were rewritten", seg, u)
	}
	o.check(t, h, 0, 63, 0)
}

// With the cleaner running only for writes that wait, the disk fills to its reserve; a pass then
// copies into the reserve and the waiting writes go out, none failing while there is a victim.
func TestAPassFinishesOnADiskFilledToItsReserve(t *testing.T) {
	h, o := primed(t)
	r := core.NewRand(13)
	low, passes := h.s.Log().FreeOutsideReserve(), h.s.CleanStats().Passes
	for range 600 {
		overwrite(h, o, r, []core.ChunkID{chunk(1), chunk(2)}, 64)
		low = min(low, h.s.Log().FreeOutsideReserve())
	}
	if low > 0 || h.s.CleanStats().Passes == passes {
		t.Fatalf("free segments outside the reserve fell to %d; %d passes", low, h.s.CleanStats().Passes-passes)
	}
	o.check(t, h, 0, 63, 0)
}

// Writes of new blocks fill the disk with live data: no victim gains space, and only then do
// writes waiting for room fail with NoSpace.
func TestWritesFailOnlyWhenNoVictimGainsSpace(t *testing.T) {
	h := newHarnessOn(t, cleanConfig(), cleanGeo)
	for i := 0; ; i++ {
		id := h.write(chunk(uint32(10+i/64)), uint32(i%64)*core.BlockSize, fill(core.BlockSize, byte(i)), 0)
		h.settle()
		h.merge()
		if err := h.reply(id).Err; err != nil {
			if !errors.Is(err, disklog.ErrNoSpace) {
				t.Fatalf("write %d: %v", i, err)
			}
			if v := h.s.chooseVictims(); len(v) > 0 {
				t.Fatalf("write %d failed with victims %v left", i, v)
			}
			return
		}
	}
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
	o := newByteOracle()
	for i := range 300 {
		data := fill(64*core.BlockSize, byte(i))
		id := h.write(chunk(1), 0, data, 0)
		h.settle()
		h.merge()
		o.apply(t, chunk(1), 0, uint32(len(data)), data, h.ok(id), 0)
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
	o.check(t, h, 0, 63, 0)
}

// A pass takes the 4 best victims by cost-benefit among the segments it may clean whose copy
// costs less than a segment, header blocks included.
func TestAPassTakesTheFourBestVictimsThatGainSpace(t *testing.T) {
	h, _ := primed(t)
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
		gains := stream == disklog.Meta || log.CopyCost(uint64(u.Live)) < uint64(cleanGeo.SegmentBlocks)
		if chosen && (!log.Cleanable(seg) || !gains) {
			t.Fatalf("victim %d is not cleanable or costs more than it frees: %+v", seg, u)
		}
		if !chosen && log.Cleanable(seg) && gains && stream != disklog.Meta && score(seg) > worst {
			t.Fatalf("segment %d scores %.1f, above victim %d's %.1f, and was left", seg, score(seg), victims[len(victims)-1], worst)
		}
	}
}

// A used segment with no live block is freed without reading or copying anything.
func TestAVictimWithNothingLiveIsFreedWithoutCopying(t *testing.T) {
	h, o := primed(t)
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
				o.apply(t, c, uint32(b)*core.BlockSize, core.BlockSize, data, h.ok(id), 0)
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
		if a.Kind == core.ActDiskRead && uint64(a.LBA)/uint64(cleanGeo.SegmentBlocks) == uint64(empty) {
			t.Fatalf("the cleaner read %+v of the empty segment", a)
		}
	}
	if after := h.s.CleanStats(); after.Freed <= before.Freed {
		t.Fatalf("stats %+v after %+v", after, before)
	}
	o.check(t, h, 0, 63, 0)
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
	h, _ := primed(t)
	log := h.s.Log()
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
