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

type seedStats struct {
	noSpace, tooLarge, deferredFlushes, deepRuns, poolFull, inDoubt, errorRuns, checkpoints, checkpointsFailed, manifestPieces int
}

// runSeed submits random items to random streams, checkpoints whenever one is due and
// may fail one disk action, with completions interleaved at random. It checks that every
// accepted item ends durable in a record on disk that decodes to it, failed, or in doubt
// after a disk error; that durability is a prefix of the log; that every finished
// checkpoint's manifest reads back; and that every buffer comes back.
func runSeed(t testing.TB, seed uint64, broken func(head []byte), stats *seedStats) {
	r := core.NewRand(seed)
	maxRec := r.Range(3, 64)
	disk := uint64(gib)
	if r.Chance(1, 4) {
		disk = 16 * core.BlockSize
	}
	geo, _ := disklog.GeometryFor(disk)
	punchShare := r.Range(0, 8)
	cfg := testConfig(maxRec)
	cfg.CommitDepth = int(r.Range(1, 3))
	cfg.MaxReplayBytes = r.Range(4, 200) * core.BlockSize
	maxWrite := min(maxRec, uint64(geo.SegmentBlocks))
	own := uint64(cfg.CommitDepth) * (2*maxWrite - 1)
	h := newHarness(t, disk, int(own+r.Range(maxWrite, 512))*core.BlockSize, cfg)
	if broken != nil {
		disklog.BreakEncoding(h.log, broken)
	}
	h.run()
	if r.Chance(1, 8) {
		h.failIO[core.IOID(firstIO+4+r.Below(400))] = core.DiskIO
		stats.errorRuns++
	}
	maxBlob := h.log.MaxBlobBytes()
	maxData := maxWrite - 2

	items := map[disklog.Ticket]*submitted{}
	durableAt := map[uint64]disklog.Event{}
	running := false
	var cpBase uint64
	var cpRoots int
	toSubmit := r.Range(1, 1000)
	var nextLSN uint64 = 1
	settle := func(tk disklog.Ticket, what string) {
		it := items[tk]
		if it == nil || it.done {
			t.Fatalf("seed %d: %s names ticket %d", seed, what, tk)
		}
		it.done = true
	}
	checkEvents := func(from int) {
		for _, ev := range h.events[from:] {
			switch ev.Kind {
			case disklog.Durable:
				if ev.LSN != nextLSN {
					t.Fatalf("seed %d: durable LSN %d, want %d", seed, ev.LSN, nextLSN)
				}
				nextLSN++
				durableAt[ev.LSN] = ev
				io, ok := h.writeAt[ev.LBA]
				if !ok {
					io = h.writeAt[ev.LBA-1]
				}
				if !h.durable[io] {
					t.Fatalf("seed %d: LSN %d reported durable before a flush covered its write", seed, ev.LSN)
				}
				d := h.record(ev)
				for i, tk := range ev.Tickets {
					if ev.Payloads[i] != disklog.PayloadOffset(ev.Entries, i) {
						t.Fatalf("seed %d: LSN %d entry %d payload offset %d", seed, ev.LSN, i, ev.Payloads[i])
					}
					if tk == disklog.NoTicket {
						if k := ev.Entries[i].BlobKind; k != disklog.BlobManifest && k != disklog.BlobManifestPiece {
							t.Fatalf("seed %d: LSN %d entry %d has no ticket", seed, ev.LSN, i)
						}
						stats.manifestPieces++
						continue
					}
					settle(tk, "a durable record")
					if it := items[tk]; d.Entries[i] != it.entry || !bytes.Equal(d.Payload(i), it.payload) {
						t.Fatalf("seed %d: LSN %d entry %d is not what ticket %d submitted", seed, ev.LSN, i, tk)
					}
				}
			case disklog.Failed, disklog.InDoubt:
				if errors.Is(ev.Err, disklog.ErrNoSpace) && ev.Kind == disklog.Failed {
					stats.noSpace++
				} else if !errors.Is(ev.Err, core.DiskIO) {
					t.Fatalf("seed %d: %+v", seed, ev)
				}
				if ev.Kind == disklog.InDoubt {
					stats.inDoubt++
				}
				for _, tk := range ev.Tickets {
					if tk != disklog.NoTicket {
						settle(tk, "a failure")
					}
				}
			case disklog.CheckpointDone:
				running = false
				stats.checkpoints++
				m := h.readManifest(durableAt[ev.Manifest.LSN])
				if m.BaseLSN != cpBase || len(m.Indexes) != 2 || len(m.Indexes[0].Runs)+len(m.Indexes[1].Runs) != cpRoots {
					t.Fatalf("seed %d: manifest %+v, want base %d and %d runs", seed, m, cpBase, cpRoots)
				}
			case disklog.CheckpointFailed:
				running = false
				stats.checkpointsFailed++
			default:
				t.Fatalf("seed %d: event %+v", seed, ev)
			}
		}
	}

	for (toSubmit > 0 && h.log.Err() == nil) || len(h.pending) > 0 {
		from := len(h.events)
		switch {
		case !running && h.log.CheckpointDue():
			cpBase = h.log.DurableLSN()
			if err := h.log.BeginCheckpoint([]disklog.CheckpointIndex{{Owner: 1, ReplayLSN: cpBase}, {Owner: 2, ReplayLSN: cpBase}}); err != nil {
				t.Fatalf("seed %d: begin: %v", seed, err)
			}
			running, cpRoots = true, 0
			for range r.Below(4) {
				n := int(r.Range(1, maxBlob))
				buf, err := h.pool.Alloc(n)
				if err != nil {
					stats.poolFull++
					break
				}
				r.Fill(h.pool.Bytes(buf)[:n])
				payload := bytes.Clone(h.pool.Bytes(buf)[:n])
				owner, root := uint32(r.Range(1, 2)), r.Chance(1, 2)
				tk, err := h.log.CheckpointPiece(&h.ctx, owner, buf, uint64(n), root, &h.out)
				if err != nil {
					h.pool.Free(buf)
					if !errors.Is(err, disklog.ErrNoCheckpoint) && h.log.Err() == nil {
						t.Fatalf("seed %d: piece: %v", seed, err)
					}
					break
				}
				items[tk] = &submitted{entry: disklog.BlobEntry(disklog.BlobRun, owner, uint64(n)), payload: payload}
				if root {
					cpRoots++
				}
			}
			if err := h.log.EndCheckpoint(&h.ctx, &h.out); err != nil && !errors.Is(err, disklog.ErrNoCheckpoint) && h.log.Err() == nil {
				t.Fatalf("seed %d: end: %v", seed, err)
			}
			h.absorb()
		case toSubmit > 0 && h.log.Err() == nil && (len(h.pending) == 0 || r.Chance(2, 5)):
			toSubmit--
			s := disklog.Stream(r.Below(2))
			near := r.Chance(1, 8)
			var e disklog.Entry
			n := 0
			switch {
			case r.Below(8) < punchShare:
				count := uint16(r.Range(1, core.BlocksPerChunk))
				e = disklog.PunchEntry(1, testChunk, r.Uint64(), uint16(core.BlocksPerChunk)-count, count)
			case r.Chance(1, 2):
				count := r.Range(1, min(3, maxData))
				if near {
					count = maxData + r.Below(2)
				}
				e = disklog.WriteEntry(1, core.ChunkID{Index: uint32(r.Below(64))}, r.Uint64(), 0, uint16(count))
				n = int(count) * core.BlockSize
			default:
				l := r.Range(1, min(5000, maxBlob))
				if near {
					l = maxBlob + r.Below(2)
				}
				e = disklog.BlobEntry(disklog.BlobRaftEntries, uint32(r.Below(4)), l)
				n = int(l)
			}
			it := disklog.Item{Entry: e}
			var payload []byte
			if n > 0 {
				buf, err := h.pool.Alloc(n)
				if err != nil {
					stats.poolFull++
					continue
				}
				r.Fill(h.pool.Bytes(buf)[:n])
				it.Buf, payload = buf, bytes.Clone(h.pool.Bytes(buf)[:n])
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
			t.Fatalf("seed %d: stalled with %d bytes free in the pool", seed, h.pool.Available())
		}
		checkEvents(from)
	}
	for tk, it := range items {
		if !it.done {
			t.Fatalf("seed %d: ticket %d neither durable, failed nor in doubt", seed, tk)
		}
	}
	if h.log.Err() == nil && h.writes != h.fsyncs {
		t.Fatalf("seed %d: %d writes and %d flushes", seed, h.writes, h.fsyncs)
	}
	h.close()
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
	if stats.noSpace == 0 || stats.tooLarge == 0 || stats.deferredFlushes == 0 || stats.deepRuns == 0 ||
		stats.poolFull == 0 || stats.inDoubt == 0 || stats.checkpoints == 0 || stats.checkpointsFailed == 0 || stats.manifestPieces == 0 {
		t.Fatalf("seeds missed a case: %+v", stats)
	}
	t.Logf("%+v", stats)
}

func TestDecodeCheckCatchesABrokenEncoder(t *testing.T) {
	breakers := []struct {
		name   string
		broken func(head []byte)
	}{
		{"wrong version, checksum fixed", func(head []byte) {
			rec := head
			if string(head[:8]) == "RSOLIDSG" {
				rec = head[core.BlockSize:]
			}
			rec[48+21]++
			disklog.FixRecordCRC(rec)
		}},
		{"flipped bit", func(head []byte) { head[len(head)-1] ^= 1 }},
	}
	for _, b := range breakers {
		broken := b.broken
		t.Run(b.name, func(t *testing.T) {
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
