package chunkstore

import (
	"errors"
	"os"
	"runtime/debug"
	"strconv"
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
// record holds 16 of them as the spike's 64 KiB records did. Records hold up to 256 KiB, the
// most the net-gain guard lets victims of 4 MiB segments near 90% live pass, and a checkpoint
// falls every 4 MiB of log, the cleaner streams spike's stand-in for 64 MiB on a 16 GiB disk.
type waRun struct {
	geo  disklog.Geometry
	fill float64
	warm float64
}

// waResult is what the measured capacity saw: the user blocks acknowledged, the blocks the disk
// was asked to write, those in the metadata stream, the blocks the cleaner copied and the
// log's own count; ranOut is set when a write failed with NoSpace with no victim left.
type waResult struct {
	user, wrote, meta, copied uint64
	metric                    disklog.Writes
	ranOut                    bool
}

func (w waResult) amplification() float64 { return float64(w.wrote) / float64(w.user) }

// waWriters is the writes kept outstanding, and waFillBlocks the size of the writes that fill
// the volume, half a record, so that the volume starts with few record headers, as the spike's
// did with records of 256 blocks.
const (
	waWriters    = 16
	waFillBlocks = 32
)

func (r waRun) run(t *testing.T, seed uint64) waResult {
	t.Helper()
	cfg := testConfig()
	cfg.Check, cfg.Log.Check = false, false
	cfg.Log.NoReserve = false
	cfg.Log.MaxRecordBytes = 256 << 10
	cfg.Log.MaxReplayBytes = 4 << 20
	cfg.Log.CompactionStepBytes = uint64(r.geo.SegmentBlocks) * core.BlockSize
	h := newHarnessOn(t, cfg, r.geo)
	h.merging, h.light, h.timed = true, true, true
	rng := core.NewRand(seed)
	blocks := uint64(r.fill*float64(r.geo.DiskBlocks())) / core.BlocksPerChunk * core.BlocksPerChunk
	data := make([]byte, waFillBlocks*core.BlockSize)

	var res waResult
	var user uint64
	next, outstanding := uint64(0), 0
	// upTo writes until next reaches limit, then until every write is answered and the store
	// has nothing left to do; it reports false once a write fails with NoSpace.
	upTo := func(limit uint64) bool {
		for {
			for _, id := range h.order {
				err := h.replies[id].Err
				delete(h.replies, id)
				outstanding--
				switch {
				case errors.Is(err, disklog.ErrNoSpace):
					if v := h.s.chooseVictims(); len(v) > 0 {
						t.Fatalf("a write failed with NoSpace while the cleaner had victims %v", v)
					}
					res.ranOut = true
				case err != nil:
					t.Fatalf("write %d: %v", id, err)
				default:
					user++
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
				h.s.Write(&h.ctx, Request{ID: h.id(), Chunk: c, Off: uint32(b%core.BlocksPerChunk) * core.BlockSize, Len: uint32(max(n, 1)) * core.BlockSize, Buf: h.writeBuf(0, data[:max(n, 1)*core.BlockSize])}, &h.out)
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
		t.Logf("%.0f%% full: ran out of space after %d of %d writes, %v", 100*r.fill, user, limit, time.Since(start))
		return res
	}
	user = 0
	wrote, meta, copied, metric := h.wrote, h.wroteMeta, h.s.CleanStats().Copied, h.s.Log().Writes()
	limit += r.geo.DiskBlocks()
	ok := upTo(limit)
	now := h.s.Log().Writes()
	res.user, res.wrote, res.meta = user, h.wrote-wrote, h.wroteMeta-meta
	res.copied = h.s.CleanStats().Copied - copied
	res.metric = disklog.Writes{Data: now.Data - metric.Data, Disk: now.Disk - metric.Disk}
	if !ok {
		t.Logf("%.0f%% full: ran out of space after %d measured writes, %v", 100*r.fill, user, time.Since(start))
		return res
	}
	u := float64(res.user)
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
	w := waRun{geo: disklog.Geometry{SegmentBlocks: 256, SegmentCount: 128}, fill: 0.4, warm: 1}.run(t, 1)
	if w.ranOut || w.copied == 0 || w.meta == 0 {
		t.Fatalf("ran out %v, %d blocks copied, %d in the metadata stream", w.ranOut, w.copied, w.meta)
	}
	w.checkMetric(t)
}

// Uniform overwrites at 80, 90 and 95% full on a 4 GiB disk of 1,024 segments of 4 MiB, a
// subtest each, run alone: go test -run 'TestCleanerWriteAmplification/80'. The spike's 16 GiB
// disk does not fit: the simulated disk keeps every block that is not zeros, and the index's dead
// runs alone hold gigabytes. The figures are logged for test-scenarios.md, "Cleaner write
// amplification in simulation"; the test fails if the metric differs from them or a write fails
// with NoSpace while the cleaner has a victim. The 80% run takes 5 minutes and 1.3 GB.
func TestCleanerWriteAmplification(t *testing.T) {
	if os.Getenv(longSimEnv) == "" {
		t.Skip("set " + longSimEnv + " to run")
	}
	defer debug.SetGCPercent(debug.SetGCPercent(50))
	for _, pct := range []int{80, 90, 95} {
		t.Run(strconv.Itoa(pct), func(t *testing.T) {
			geo := disklog.Geometry{SegmentBlocks: 1024, SegmentCount: 1024}
			waRun{geo: geo, fill: float64(pct) / 100, warm: 1}.run(t, 1).checkMetric(t)
		})
	}
}
