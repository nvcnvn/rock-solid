package chunkstore

import (
	"cmp"
	"errors"
	"fmt"
	"math"
	"os"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/lsm"
)

// waRun is a simulated run of uniform random 4 KiB overwrites, as the cleaner spike ran them. A
// volume of fill less index times the disk, so that with the index's runs the disk's fill, its
// live blocks over its size, is fill, is written once in order in writes of waFillBlocks, each
// followed half the time by a 4 KiB overwrite of what is written so far, so that the cleaner finds
// dead blocks in the oldest segments as the disk fills; then it is overwritten at random for warm
// disk capacities of writes before one measured capacity, with 16 writes outstanding. Write
// admission paces them once the cleaner falls behind, and a round takes the writes waiting when it
// begins: 12, 8 and 7 at 80, 85 and 90% full, where the spike's records held 16.
// Records hold up to 256 KiB, where the net-gain guard lets victims of a 4 MiB segment pass up to
// 914 of 1,023 blocks live, 89.35%. A checkpoint falls every 4 MiB of log, the cleaner streams
// spike's stand-in for 64 MiB on a 16 GiB disk. check turns on the store's and the log's invariant
// checkers, cleanFree sets the store's clean_free, record sets the largest record in bytes in
// place of 256 KiB, and plant plants bugs in the store.
type waRun struct {
	geo       disklog.Geometry
	fill      float64
	index     float64
	warm      float64
	check     bool
	cleanFree int
	record    uint64
	plant     plants
}

// waResult is what the measured capacities saw: the user blocks acknowledged, the blocks the
// disk was asked to write, those in each stream, the blocks the cleaner copied and the log's own
// count; the chunk victims the cleaner took, their
// live blocks when taken, and the fullest over the whole run; the metadata stream's blocks by
// what wrote them, and the tails the cold and metadata streams left at segments' ends; and the
// disk's space, sampled. measured is set once the measured capacities begin, and ranOut when a
// write failed with NoSpace with no victim left.
type waResult struct {
	user, wrote, copied uint64
	in                  [disklog.Meta + 1]uint64
	metric              disklog.Writes
	victims, victimLive uint64
	victimMost          uint64
	flushed, merged     uint64
	rewritten, tails    uint64
	manifests           uint64
	space               waSpace
	measured, ranOut    bool
}

// waSpace sums samples of the disk's segments, free and used in each stream, and of the live
// blocks in each stream.
type waSpace struct {
	samples, free uint64
	used, live    [disklog.Meta + 1]uint64
}

func (w *waSpace) sample(l *disklog.Log, segments uint32) {
	w.samples++
	for seg := uint32(1); seg < segments; seg++ {
		u, st := l.Usage(seg)
		if u.State != disklog.SegmentUsed {
			if u.State == disklog.SegmentFree {
				w.free++
			}
			continue
		}
		w.used[st]++
		w.live[st] += uint64(u.Live)
	}
}

func (w waSpace) mean(n uint64) float64 { return float64(n) / float64(w.samples) }

func (w waResult) amplification() float64 { return float64(w.wrote) / float64(w.user) }

// chunkAmplification is the write amplification outside the metadata stream: user blocks, their
// records' headers, the cleaner's copies and their headers, and segment headers.
func (w waResult) chunkAmplification() float64 {
	return float64(w.wrote-w.in[disklog.Meta]) / float64(w.user)
}

// diskFill is the disk's mean fill over the measured capacities: its live blocks, the index's
// runs included, over its size.
func (w waResult) diskFill(geo disklog.Geometry) float64 {
	sp := w.space
	return sp.mean(sp.live[disklog.Hot]+sp.live[disklog.Cold]+sp.live[disklog.Meta]) / float64(geo.DiskBlocks())
}

// waWriters is the writes kept outstanding, and waFillBlocks the size of the writes that fill
// the volume, half a record, so that the volume starts with few record headers, as the spike's
// did with records of 256 blocks.
const (
	waWriters    = 16
	waFillBlocks = 32
)

func (r waRun) run(t testing.TB, seed uint64) waResult {
	t.Helper()
	cfg := testConfig()
	cfg.Check, cfg.Log.Check = r.check, r.check
	cfg.Log.NoReserve = false
	cfg.CleanFree = r.cleanFree
	cfg.Log.MaxRecordBytes = cmp.Or(r.record, 256<<10)
	cfg.Log.MaxReplayBytes = 4 << 20
	cfg.Log.CompactionStepBytes = uint64(r.geo.SegmentBlocks) * core.BlockSize
	h := newHarnessOn(t, cfg, r.geo)
	h.merging, h.light, h.timed = true, true, true
	h.s.plant = r.plant
	rng := core.NewRand(seed)
	blocks := uint64((r.fill-r.index)*float64(r.geo.DiskBlocks())) / core.BlocksPerChunk * core.BlocksPerChunk
	data := make([]byte, waFillBlocks*core.BlockSize)

	var res waResult
	var user uint64
	blocksOf := map[uint64]uint64{}
	next, outstanding := uint64(0), 0
	// upTo writes until next reaches limit, then until every write is answered and the store
	// has nothing left to do; it reports false once a write fails with NoSpace.
	lane, seq := map[uint32]uint8{}, map[uint32]uint64{}
	rewriting := map[int]bool{}
	h.onMerge = func(c *lsm.Compaction) {
		_, rewriting[c.Worker()] = c.Rewrite()
	}
	var img []byte
	h.onWrite = func(a *core.Action, st disklog.Stream) {
		if st != disklog.Meta {
			return
		}
		img = img[:0]
		for _, v := range a.Vec {
			img = append(img, h.pool.Bytes(v.Buf)[:v.Len]...)
		}
		seg, at := uint32(uint64(a.LBA)/uint64(r.geo.SegmentBlocks)), uint32(uint64(a.LBA)%uint64(r.geo.SegmentBlocks))
		rec := img
		if at == 0 {
			sh, err := disklog.DecodeSegmentHeader(img)
			if err != nil {
				t.Fatal(err)
			}
			lane[seg], seq[seg], rec, at = sh.Lane, sh.Seq, img[core.BlockSize:], 1
		}
		if !res.measured {
			return
		}
		n := a.Len / core.BlockSize
		if l := lane[seg]; l > 0 {
			res.merged += n
			if rewriting[int(l)] {
				res.rewritten += n
			}
			return
		}
		d, err := disklog.DecodeRecord(rec, disklog.Place{LogID: h.s.sb.LogID, Seq: seq[seg], Offset: at}, r.geo.SegmentBlocks-at, nil)
		if err != nil {
			t.Fatal(err)
		}
		if slices.ContainsFunc(d.Entries, func(e disklog.Entry) bool {
			return e.BlobKind == disklog.BlobManifest || e.BlobKind == disklog.BlobManifestPiece
		}) {
			res.manifests += n
			return
		}
		res.flushed += n
	}
	seen := map[*cleanPass]int{}
	sampleEvery, sampleAt := r.geo.DiskBlocks()/256, uint64(0)
	observe := func() {
		for _, p := range h.s.cl.passes {
			for _, v := range p.victims[seen[p]:] {
				if u, st := h.s.Log().Usage(v); st != disklog.Meta {
					res.victims++
					res.victimLive += uint64(u.Live)
					res.victimMost = max(res.victimMost, uint64(u.Live))
				}
			}
			seen[p] = len(p.victims)
		}
		if len(seen) > 64 {
			clear(seen)
			for _, p := range h.s.cl.passes {
				seen[p] = len(p.victims)
			}
		}
		if res.measured && user >= sampleAt {
			res.space.sample(h.s.Log(), r.geo.SegmentCount)
			sampleAt = user + sampleEvery
		}
	}
	upTo := func(limit uint64) bool {
		for {
			observe()
			for _, id := range h.order {
				err, n := h.replies[id].Err, blocksOf[id]
				delete(h.replies, id)
				delete(blocksOf, id)
				outstanding--
				switch {
				case errors.Is(err, disklog.ErrNoSpace):
					if err := h.found[invNoSpace]; err != nil {
						t.Fatal(err)
					}
					res.ranOut = true
				case err != nil:
					t.Fatalf("write %d: %v", id, err)
				default:
					user += n
				}
			}
			h.order = h.order[:0]
			for outstanding < waWriters && next < limit && !res.ranOut && h.s.Admitting() {
				b, n := next, uint64(waFillBlocks)
				switch {
				case next >= blocks:
					b, n = rng.Below(blocks), 1
				case next > 0 && rng.Chance(1, 2):
					b, n = rng.Below(next), 0
				}
				c := core.ChunkID{Volume: 1, Inode: 2, Index: uint32(b / core.BlocksPerChunk)}
				id := h.id()
				blocksOf[id] = max(n, 1)
				h.s.Write(&h.ctx, Request{ID: id, Chunk: c, Off: uint32(b%core.BlocksPerChunk) * core.BlockSize, Len: uint32(max(n, 1)) * core.BlockSize, Buf: h.writeBuf(0, data[:max(n, 1)*core.BlockSize])}, &h.out)
				h.take()
				outstanding++
				next += n
			}
			if len(h.order) > 0 || h.step() {
				continue
			}
			var wake core.Tick
			for _, at := range []core.Tick{h.admitAt, h.timerAt} {
				if at != 0 && (wake == 0 || at < wake) {
					wake = at
				}
			}
			switch {
			case outstanding == 0 && (next == limit || res.ranOut):
				return !res.ranOut
			case wake == 0:
				t.Fatalf("the run stopped with %d writes outstanding, %d to send, nothing queued and no timer armed", outstanding, limit-next)
			}
			h.tick(max(wake, h.ctx.Now+1) - h.ctx.Now)
		}
	}
	start := time.Now()
	limit := blocks + uint64(r.warm*float64(r.geo.DiskBlocks()))
	if !upTo(limit) {
		t.Logf("%.0f%% full: ran out of space after %d of %d blocks written, %v; the fullest victim %d blocks live",
			100*r.fill, user, limit, time.Since(start), res.victimMost)
		return res
	}
	user, res.measured, res.victims, res.victimLive = 0, true, 0, 0
	wrote, in, copied, metric, background := h.wrote, h.wroteIn, h.s.CleanStats().Copied, h.s.Log().Writes(), h.s.Log().BackgroundBlocks()
	if !upTo(limit + r.geo.DiskBlocks()) {
		t.Logf("%.0f%% full: ran out of space after %d measured blocks", 100*r.fill, user)
	}
	now := h.s.Log().Writes()
	res.user, res.wrote = user, h.wrote-wrote
	for st := range res.in {
		res.in[st] = h.wroteIn[st] - in[st]
	}
	res.copied = h.s.CleanStats().Copied - copied
	res.metric = disklog.Writes{Data: now.Data - metric.Data, Disk: now.Disk - metric.Disk}
	res.tails = h.s.Log().BackgroundBlocks() - background - res.in[disklog.Cold] - res.in[disklog.Meta]
	res.log(t, r, h.s.Log().Reserve(), time.Since(start))
	return res
}

// log reports the write amplification and its parts per user block: the user records' headers
// and segment headers in the hot stream, the blocks copied and the cold stream's headers, and the
// metadata stream; then the space, as mean segments free and used in each stream, the live share
// of the disk and of the chunk segments in use, and the chunk victims' mean live share.
func (w waResult) log(t testing.TB, r waRun, reserve uint32, took time.Duration) {
	t.Helper()
	u, sp := float64(w.user), w.space
	usable := float64(r.geo.SegmentBlocks - 1)
	chunkSegs := sp.mean(sp.used[disklog.Hot] + sp.used[disklog.Cold])
	t.Logf("%.0f%% full: write amplification %.3f, of chunk data %.3f, in %v", 100*r.fill, w.amplification(), w.chunkAmplification(), took)
	t.Logf("  per user block: user headers %.4f, copies %.4f, copy headers %.4f, metadata %.4f (flushes %.4f, merges %.4f of which rewrites %.4f, records with manifests %.4f)",
		float64(w.in[disklog.Hot]-w.user)/u, float64(w.copied)/u, float64(w.in[disklog.Cold]-w.copied)/u, float64(w.in[disklog.Meta])/u,
		float64(w.flushed)/u, float64(w.merged)/u, float64(w.rewritten)/u, float64(w.manifests)/u)
	t.Logf("  tails the cold and metadata streams left at segments' ends: %.4f per user block", float64(w.tails)/u)
	t.Logf("  segments of %d: free %.1f, hot %.1f, cold %.1f, meta %.1f; reserve %d", r.geo.SegmentCount,
		sp.mean(sp.free), sp.mean(sp.used[disklog.Hot]), sp.mean(sp.used[disklog.Cold]), sp.mean(sp.used[disklog.Meta]), reserve)
	chunkLive, disk := sp.mean(sp.live[disklog.Hot]+sp.live[disklog.Cold]), float64(r.geo.DiskBlocks())
	t.Logf("  live share of the disk: %.4f, chunk data %.4f, index %.4f; of chunk segments %.4f; %d chunk victims, %.4f live, the fullest %.4f",
		chunkLive/disk+sp.mean(sp.live[disklog.Meta])/disk, chunkLive/disk, sp.mean(sp.live[disklog.Meta])/disk,
		chunkLive/(chunkSegs*usable), w.victims, float64(w.victimLive)/float64(w.victims)/usable, float64(w.victimMost)/usable)
}

// checkMetric fails unless the log's write counters over the run's measured capacity equal the
// user blocks acknowledged and the blocks the disk was asked to write.
func (w waResult) checkMetric(t *testing.T) {
	t.Helper()
	if w.metric.Data != w.user || w.metric.Disk != w.wrote {
		t.Fatalf("the log counts %d data blocks and %d disk blocks; the run acknowledged %d and wrote %d", w.metric.Data, w.metric.Disk, w.user, w.wrote)
	}
}

// On a small disk the cleaner copies, the index writes runs, and the log's write counters over a
// capacity of overwrites equal what the run acknowledged and what the disk was asked to write,
// so the write amplification each disk exports is the measured one.
func TestWriteAmplificationMetricEqualsTheMeasuredFigure(t *testing.T) {
	w := waRun{geo: disklog.Geometry{SegmentBlocks: 256, SegmentCount: 128}, fill: 0.4, warm: 1, check: true}.run(t, 1)
	if w.ranOut || w.copied == 0 || w.in[disklog.Meta] == 0 {
		t.Fatalf("ran out %v, %d blocks copied, %d in the metadata stream", w.ranOut, w.copied, w.in[disklog.Meta])
	}
	w.checkMetric(t)
}

// Planted violation: a store that fails writes waiting for room while the cleaner has a victim
// fails the run on invNoSpace.
func TestWriteAmplificationRunChecksNoSpace(t *testing.T) {
	c := &fatalCatcher{TB: t}
	func() {
		defer func() {
			if v := recover(); v != nil && v != errCaught {
				panic(v)
			}
		}()
		waRun{geo: disklog.Geometry{SegmentBlocks: 256, SegmentCount: 128}, fill: 0.5, warm: 1, cleanFree: -1, plant: plants{noSpaceWithVictim: true}}.run(c, 1)
	}()
	if !strings.Contains(c.msg, "failed with NoSpace") {
		t.Fatalf("the run ended with %q", c.msg)
	}
	t.Log(c.msg)
}

// fatalCatcher records a Fatal instead of ending the test, unwinding with errCaught.
type fatalCatcher struct {
	testing.TB
	msg string
}

var errCaught = errors.New("caught")

func (c *fatalCatcher) Fatal(args ...any) {
	c.msg = fmt.Sprint(args...)
	panic(errCaught)
}

func (c *fatalCatcher) Fatalf(format string, args ...any) {
	c.msg = fmt.Sprintf(format, args...)
	panic(errCaught)
}

// waIndex is the share of the disk the chunk index's live runs hold under uniform 4 KiB
// overwrites, which a run's fill counts as the disk's fill does.
const waIndex = 0.0125

// Uniform overwrites at 80, 85 and 90% full, the disk's fill counting the index's runs, a subtest
// each, run alone: go test -run 'TestCleanerWriteAmplification/80'. The chunk data's write
// amplification, every block the disk was asked to write outside the metadata stream over the
// user blocks acknowledged, lands within 5% of the cleaner streams spike's run under the store's
// conditions (spikes/cleaner-streams, test-scenarios.md, "Cleaner write amplification in
// simulation"): 1,024 segments of 4 MiB, or at 90% 2,048 of 8 MiB, whose net-gain guard passes
// victims up to 92.6% live in records of 192 KiB, where 4 MiB segments pass at most 89.35% and
// run out of victims; 16 segments free, the reserve and the admission point write admission
// holds them at; and user records of as many writes as the store's rounds carried under
// admission, 12, 8 and 7 at 80, 85 and 90%. The index's own writes per user block stay within 10%
// of those measured, 0.56, 0.72 and 1.70. The run's disk fill is within half a point of the
// subtest's, the log's counters equal what it measured, and no write fails with NoSpace while
// the cleaner has a victim. The 80% run takes 2.3 minutes and 0.6 GB, 85% 3.5 minutes and
// 0.6 GB, and 90% 29 minutes and 2.5 GB.
func TestCleanerWriteAmplification(t *testing.T) {
	if os.Getenv(longSimEnv) == "" {
		t.Skip("set " + longSimEnv + " to run")
	}
	defer debug.SetGCPercent(debug.SetGCPercent(50))
	for _, c := range []struct {
		fill   float64
		geo    disklog.Geometry
		record uint64
		spike  float64
		meta   float64
	}{
		{0.80, disklog.Geometry{SegmentBlocks: 1024, SegmentCount: 1024}, 0, 4.03, 0.56},
		{0.85, disklog.Geometry{SegmentBlocks: 1024, SegmentCount: 1024}, 0, 6.03, 0.72},
		{0.90, disklog.Geometry{SegmentBlocks: 2048, SegmentCount: 2048}, 192 << 10, 10.50, 1.70},
	} {
		t.Run(strconv.Itoa(int(100*c.fill)), func(t *testing.T) {
			w := waRun{geo: c.geo, fill: c.fill, index: waIndex, warm: 1, record: c.record}.run(t, 1)
			if !w.measured || w.ranOut {
				t.Fatalf("%.0f%% full ran out of space", 100*c.fill)
			}
			w.checkMetric(t)
			if f := w.diskFill(c.geo); math.Abs(f-c.fill) > 0.005 {
				t.Fatalf("the disk's fill is %.4f, not %.2f", f, c.fill)
			}
			if chunk := w.chunkAmplification(); math.Abs(chunk/c.spike-1) > 0.05 {
				t.Fatalf("the chunk data's write amplification is %.3f, against the spike's %.2f", chunk, c.spike)
			}
			if meta := float64(w.in[disklog.Meta]) / float64(w.user); math.Abs(meta/c.meta-1) > 0.10 {
				t.Fatalf("the index writes %.3f blocks per user block, against the %.2f measured", meta, c.meta)
			}
		})
	}
}

// At the default write_limit of each band of segment sizes below 8 MiB, a disk of 1,024 of the
// band's smallest segments, in records of max_group_commit_bytes' default, 1 MiB, runs a measured
// capacity of uniform overwrites without NoSpace: 45% on 64 KiB segments, 50% on 128 KiB, 55% on
// 512 KiB and 75% on 4 MiB. Run alone: the 4 MiB subtest takes 2 minutes and 0.6 GB.
func TestDefaultWriteLimitBySegmentSize(t *testing.T) {
	if os.Getenv(longSimEnv) == "" {
		t.Skip("set " + longSimEnv + " to run")
	}
	defer debug.SetGCPercent(debug.SetGCPercent(50))
	for _, c := range []struct {
		segmentBlocks uint32
		limit         uint8
	}{{16, 45}, {32, 50}, {128, 55}, {1024, 75}} {
		t.Run(fmt.Sprintf("%d KiB", c.segmentBlocks*4), func(t *testing.T) {
			if got := DefaultWriteLimit(4096 * uint64(c.segmentBlocks) * core.BlockSize); got != c.limit {
				t.Fatalf("the default write_limit on %d KiB segments is %d%%, want %d%%", c.segmentBlocks*4, got, c.limit)
			}
			geo := disklog.Geometry{SegmentBlocks: c.segmentBlocks, SegmentCount: 1024}
			w := waRun{geo: geo, fill: float64(c.limit) / 100, warm: 1, record: 1 << 20}.run(t, 1)
			if !w.measured || w.ranOut {
				t.Fatalf("%d%% full on %d KiB segments ran out of space", c.limit, c.segmentBlocks*4)
			}
			w.checkMetric(t)
		})
	}
}

// On a disk with chunk slots, whole chunks written again and deleted at random among 4 KiB
// overwrites free slot segments, which the log discards and whose headers it zeroes, after a TRIM
// or, with TRIM off, by a write alone: the log's counters still equal the blocks acknowledged and
// those the disk was asked to write.
func TestWriteAmplificationMetricCountsChunkSlots(t *testing.T) {
	for _, noTrim := range []bool{false, true} {
		t.Run(fmt.Sprintf("no TRIM %v", noTrim), func(t *testing.T) {
			cfg := testConfig()
			cfg.Log.MaxReplayBytes = 1 << 20
			cfg.Log.NoTrim = noTrim
			h := newHarnessOn(t, cfg, disklog.Geometry{SegmentBlocks: 4096, SegmentCount: 24})
			r := core.NewRand(9)
			whole := pattern(9, core.ChunkSize)
			small := core.ChunkID{Volume: 2, Inode: 2}
			was, wrote, user := h.s.Log().Writes(), h.wrote, uint64(0)
			for range 200 {
				c := core.ChunkID{Volume: 2, Inode: 1, Index: uint32(r.Below(16))}
				switch {
				case r.Chance(1, 4):
					h.sync(h.punch(c, 0, core.ChunkSize, 0))
				case r.Chance(1, 2):
					h.sync(h.write(c, 0, whole, 0))
					user += core.BlocksPerChunk
				default:
					h.sync(h.write(small, uint32(r.Below(core.BlocksPerChunk))*core.BlockSize, fill(core.BlockSize, 1), 0))
					user++
				}
			}
			now := h.s.Log().Writes()
			if now.Data-was.Data != user || now.Disk-was.Disk != h.wrote-wrote {
				t.Fatalf("the log counts %d data blocks and %d disk blocks; the run acknowledged %d and wrote %d", now.Data-was.Data, now.Disk-was.Disk, user, h.wrote-wrote)
			}
		})
	}
}
