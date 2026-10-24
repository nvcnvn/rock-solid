package chunkstore

import (
	"cmp"
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// waRun is a simulated run of uniform random 4 KiB overwrites, as the cleaner spike ran them: a
// volume of fill times the disk, written once in order in writes of waFillBlocks, each followed
// half the time by a 4 KiB overwrite of what is written so far, so that the cleaner finds dead
// blocks in the oldest segments as the disk fills, then overwritten at random for warm disk
// capacities of writes before one measured capacity, with 16 writes outstanding, so that a
// record holds 16 of them as the spike's 64 KiB records did. Records hold up to 256 KiB, where
// the net-gain guard lets victims of a 4 MiB segment pass up to 914 of 1,023 blocks live, 89.35%;
// 128 KiB records let 930 pass, 90.9%, yet measured worse, 5.78 at 80% full against 5.09, and
// ran out of space sooner at 90%. A checkpoint falls every 4 MiB of log, the cleaner streams spike's stand-in for 64 MiB on a 16 GiB disk. check turns
// on the store's and the log's invariant checkers, cleanFree sets the store's clean_free, record
// sets the largest record in bytes in place of 256 KiB, and plant plants bugs in the store.
type waRun struct {
	geo       disklog.Geometry
	fill      float64
	warm      float64
	check     bool
	cleanFree int
	record    uint64
	plant     plants
}

// waResult is what the measured capacity saw: the user blocks acknowledged, the blocks the disk
// was asked to write, those in the metadata stream, the blocks the cleaner copied and the
// log's own count; measured is set once the measured capacity begins, and ranOut when a write
// failed with NoSpace with no victim left.
type waResult struct {
	user, wrote, meta, copied uint64
	metric                    disklog.Writes
	measured, ranOut          bool
}

func (w waResult) amplification() float64 { return float64(w.wrote) / float64(w.user) }

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
	blocks := uint64(r.fill*float64(r.geo.DiskBlocks())) / core.BlocksPerChunk * core.BlocksPerChunk
	data := make([]byte, waFillBlocks*core.BlockSize)

	var res waResult
	var user uint64
	blocksOf := map[uint64]uint64{}
	next, outstanding := uint64(0), 0
	// upTo writes until next reaches limit, then until every write is answered and the store
	// has nothing left to do; it reports false once a write fails with NoSpace.
	upTo := func(limit uint64) bool {
		for {
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
		t.Logf("%.0f%% full: ran out of space after %d of %d blocks written, %v", 100*r.fill, user, limit, time.Since(start))
		return res
	}
	user, res.measured = 0, true
	wrote, meta, copied, metric := h.wrote, h.wroteMeta, h.s.CleanStats().Copied, h.s.Log().Writes()
	limit += r.geo.DiskBlocks()
	ok := upTo(limit)
	now := h.s.Log().Writes()
	res.user, res.wrote, res.meta = user, h.wrote-wrote, h.wroteMeta-meta
	res.copied = h.s.CleanStats().Copied - copied
	res.metric = disklog.Writes{Data: now.Data - metric.Data, Disk: now.Disk - metric.Disk}
	u := float64(res.user)
	if !ok {
		t.Logf("%.0f%% full: ran out of space after %d of %d measured blocks", 100*r.fill, user, r.geo.DiskBlocks())
	}
	t.Logf("%.0f%% full: write amplification %.2f, of it metadata %.2f and copies %.2f per user block, in %v",
		100*r.fill, res.amplification(), float64(res.meta)/u, float64(res.copied)/u, time.Since(start))
	return res
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
	if w.ranOut || w.copied == 0 || w.meta == 0 {
		t.Fatalf("ran out %v, %d blocks copied, %d in the metadata stream", w.ranOut, w.copied, w.meta)
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

// Uniform overwrites at 80, 90 and 95% full on a 4 GiB disk of 1,024 segments of 4 MiB, a
// subtest each, run alone: go test -run 'TestCleanerWriteAmplification/80'. The spike's 16 GiB
// disk does not fit: the simulated disk keeps every block that is not zeros, and the index's dead
// runs alone hold gigabytes. The figures are logged for test-scenarios.md, "Cleaner write
// amplification in simulation"; the test fails if a write fails with NoSpace while the cleaner
// has a victim, or the metric differs from what the run measured, and skips a fill whose disk
// runs out of space before the measured capacity begins. The 80% run takes 3 minutes and 0.6 GB.
func TestCleanerWriteAmplification(t *testing.T) {
	if os.Getenv(longSimEnv) == "" {
		t.Skip("set " + longSimEnv + " to run")
	}
	defer debug.SetGCPercent(debug.SetGCPercent(50))
	for _, pct := range []int{80, 90, 95} {
		t.Run(strconv.Itoa(pct), func(t *testing.T) {
			geo := disklog.Geometry{SegmentBlocks: 1024, SegmentCount: 1024}
			w := waRun{geo: geo, fill: float64(pct) / 100, warm: 1}.run(t, 1)
			if !w.measured {
				t.Skipf("%d%% full: the disk ran out of space before the measured capacity, no victim passing the net-gain guard", pct)
			}
			w.checkMetric(t)
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
		limit         float64
	}{{16, 0.45}, {32, 0.50}, {128, 0.55}, {1024, 0.75}} {
		t.Run(fmt.Sprintf("%d KiB", c.segmentBlocks*4), func(t *testing.T) {
			geo := disklog.Geometry{SegmentBlocks: c.segmentBlocks, SegmentCount: 1024}
			w := waRun{geo: geo, fill: c.limit, warm: 1, record: 1 << 20}.run(t, 1)
			if !w.measured || w.ranOut {
				t.Fatalf("%.0f%% full on %d KiB segments ran out of space", 100*c.limit, c.segmentBlocks*4)
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
