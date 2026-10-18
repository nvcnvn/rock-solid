package disklog_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

type submitted struct {
	entry   disklog.Entry
	payload []byte
	done    bool
}

// runSeed submits random items to random streams with completions interleaved at random,
// and checks that every accepted item ends durable in a record on disk that decodes to
// it, or fails for want of space; that durability is a prefix of the log; and that every
// buffer comes back.
type seedStats struct {
	noSpace, tooLarge, deferredFlushes, deepRuns int
}

func runSeed(t testing.TB, seed uint64, broken func(head []byte), stats *seedStats) {
	r := core.NewRand(seed)
	maxRec := r.Range(2, 64)
	disk := uint64(gib)
	if r.Chance(1, 4) {
		disk = 16 * core.BlockSize
	}
	punchShare := r.Range(0, 8)
	cfg := testConfig(maxRec)
	cfg.CommitDepth = int(r.Range(1, 3))
	h := newHarness(t, disk, 16<<20, cfg)
	if broken != nil {
		disklog.BreakEncoding(h.log, broken)
	}
	h.run()
	maxBlob := h.log.MaxBlobBytes()
	maxData := min(maxRec, uint64(h.geo.SegmentBlocks)-1) - 1

	items := map[disklog.Ticket]*submitted{}
	toSubmit := r.Range(1, 1000)
	var nextLSN uint64 = 1
	checkEvents := func(from int) {
		for _, ev := range h.events[from:] {
			switch ev.Kind {
			case disklog.Durable:
				if ev.LSN != nextLSN {
					t.Fatalf("seed %d: durable LSN %d, want %d", seed, ev.LSN, nextLSN)
				}
				nextLSN++
				io, ok := h.writeAt[ev.LBA]
				if !ok {
					io = h.writeAt[ev.LBA-1]
				}
				if !h.durable[io] {
					t.Fatalf("seed %d: LSN %d at LBA %d (io%d, ok %v) reported durable before a flush covered its write; depth %d trace %q", seed, ev.LSN, ev.LBA, io, ok, cfg.CommitDepth, h.trace[max(0, len(h.trace)-12):])
				}
				d := h.record(ev)
				for i, tk := range ev.Tickets {
					it := items[tk]
					if it == nil || it.done {
						t.Fatalf("seed %d: LSN %d names ticket %d", seed, ev.LSN, tk)
					}
					it.done = true
					if d.Entries[i] != it.entry || !bytes.Equal(d.Payload(i), it.payload) {
						t.Fatalf("seed %d: LSN %d entry %d is not what ticket %d submitted", seed, ev.LSN, i, tk)
					}
				}
			case disklog.Failed:
				if !errors.Is(ev.Err, disklog.ErrNoSpace) {
					t.Fatalf("seed %d: failed with %v", seed, ev.Err)
				}
				stats.noSpace++
				for _, tk := range ev.Tickets {
					if it := items[tk]; it == nil || it.done {
						t.Fatalf("seed %d: failure names ticket %d", seed, tk)
					}
					items[tk].done = true
				}
			default:
				t.Fatalf("seed %d: event %+v", seed, ev)
			}
		}
	}

	for toSubmit > 0 || len(h.pending) > 0 || h.timer {
		from := len(h.events)
		switch {
		case toSubmit > 0 && (len(h.pending) == 0 || r.Chance(2, 5)) && h.pool.Available() > 256*core.BlockSize:
			toSubmit--
			s := disklog.Stream(r.Below(2))
			near := r.Chance(1, 8)
			var it disklog.Item
			switch {
			case r.Below(8) < punchShare:
				count := uint16(r.Range(1, core.BlocksPerChunk))
				it = disklog.Item{Entry: disklog.PunchEntry(1, testChunk, r.Uint64(), uint16(core.BlocksPerChunk)-count, count)}
			case r.Chance(1, 2):
				count := r.Range(1, min(3, maxData))
				if near {
					count = maxData + r.Below(2)
				}
				it = h.write(uint32(r.Below(64)), uint16(count), r.Uint64())
			default:
				n := r.Range(1, min(5000, maxBlob))
				if near {
					n = maxBlob + r.Below(2)
				}
				it = h.blob(disklog.BlobRaftEntries, uint32(r.Below(4)), int(n), r.Uint64())
			}
			var payload []byte
			if it.Buf != core.NoBuf {
				payload = bytes.Clone(h.pool.Bytes(it.Buf)[:max(it.Entry.Length, uint64(it.Entry.Count)*core.BlockSize)])
			}
			tk, err := h.log.Submit(&h.ctx, s, it, &h.out)
			if err != nil {
				if !errors.Is(err, disklog.ErrTooLarge) || !near {
					t.Fatalf("seed %d: submit %+v: %v", seed, it.Entry, err)
				}
				if it.Buf != core.NoBuf {
					h.pool.Free(it.Buf)
				}
				stats.tooLarge++
				continue
			}
			items[tk] = &submitted{entry: it.Entry, payload: payload}
			h.absorb()
		case len(h.pending) > 0:
			var ready []int
			for i := range h.pending {
				if h.eligible(i) {
					ready = append(ready, i)
				}
			}
			h.step(ready[r.Below(uint64(len(ready)))])
		default:
			h.ctx.Now += retryDelay
			h.fireTimer()
		}
		checkEvents(from)
	}
	for tk, it := range items {
		if !it.done {
			t.Fatalf("seed %d: ticket %d neither durable nor failed", seed, tk)
		}
	}
	if h.writes != h.fsyncs {
		t.Fatalf("seed %d: %d writes and %d flushes", seed, h.writes, h.fsyncs)
	}
	h.wantLive(0)
	stats.deferredFlushes += h.unlinked
	if cfg.CommitDepth > 1 {
		stats.deepRuns++
	}
}

func TestEveryAcceptedItemDecodes(t *testing.T) {
	var stats seedStats
	for seed := uint64(1); seed <= 100; seed++ {
		runSeed(t, seed, nil, &stats)
	}
	if stats.noSpace == 0 || stats.tooLarge == 0 || stats.deferredFlushes == 0 || stats.deepRuns == 0 {
		t.Fatalf("seeds missed a case: %+v", stats)
	}
	t.Logf("%+v", stats)
}

func TestDecodeCheckCatchesABrokenEncoder(t *testing.T) {
	breakers := map[string]func(head []byte){
		"wrong version, checksum fixed": func(head []byte) {
			rec := head
			if string(head[:8]) == "RSOLIDSG" {
				rec = head[core.BlockSize:]
			}
			rec[48+21]++
			disklog.FixRecordCRC(rec)
		},
		"flipped bit": func(head []byte) { head[len(head)-1] ^= 1 },
	}
	for name, broken := range breakers {
		t.Run(name, func(t *testing.T) {
			defer func() {
				msg, _ := recover().(string)
				if !strings.Contains(msg, "decode check") {
					t.Fatalf("a broken encoder passed the decode check: %q", msg)
				}
			}()
			for seed := uint64(1); seed <= 100; seed++ {
				runSeed(t, seed, broken, &seedStats{})
			}
		})
	}
}
