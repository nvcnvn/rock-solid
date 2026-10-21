package disklog_test

import (
	"bytes"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
	"github.com/nvcnvn/rock-solid/internal/trace"
)

type submitted struct {
	entry   disklog.Entry
	payload []byte
	done    bool
}

type seedStats struct {
	noSpace, tooLarge, deferredFlushes, deepRuns, poolFull, inDoubt, errorRuns, checkpoints, checkpointsFailed, manifestPieces, mergePieces int
}

// runSeed submits random items to random streams and merge lanes, checkpoints whenever one is due and
// may fail one disk action, with completions interleaved at random, on a node of the
// scheduler, whose trace it returns with the harness. It checks that every
// accepted item ends durable in a record on disk that decodes to it, failed, or in doubt
// after a disk error; that durability is a prefix of the log; that every finished
// checkpoint's manifest reads back; and that every buffer comes back.
func runSeed(t testing.TB, seed uint64, setup func(*harness), stats *seedStats) *harness {
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
	cfg.MergeWorkers = int(r.Range(1, 4))
	maxWrite := min(maxRec, uint64(geo.SegmentBlocks))
	own := uint64(cfg.CommitDepth) * (2*maxWrite - 1)
	h := schedHarness(t, seed, disk, int(own+r.Range(maxWrite, 512))*core.BlockSize, cfg)
	if setup != nil {
		setup(h)
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
	var cpBase, cpNextSeq uint64
	var cpRows []disklog.SegmentUsage
	var cpRoots int
	var retryFrom uint64
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
				if m.BaseLSN != cpBase || m.NextSeq != cpNextSeq || !slices.Equal(m.Segments, cpRows) || len(m.Indexes) != 2 || len(m.Indexes[0].Runs)+len(m.Indexes[1].Runs) != cpRoots {
					t.Fatalf("seed %d: manifest %+v, want base %d, NextSeq %d, rows %+v and %d runs", seed, m, cpBase, cpNextSeq, cpRows, cpRoots)
				}
			case disklog.CheckpointFailed:
				running, retryFrom = false, nextLSN
				stats.checkpointsFailed++
			default:
				t.Fatalf("seed %d: event %+v", seed, ev)
			}
		}
	}

	for steps := 0; (toSubmit > 0 || h.log.Queued() > 0) && h.log.Err() == nil || len(h.pending) > 0; steps++ {
		if steps == maxSeedSteps {
			t.Fatalf("seed %d: no end after %d steps, %d items left to submit", seed, steps, toSubmit)
		}
		from := len(h.events)
		switch {
		case !running && h.log.CheckpointDue() && nextLSN > retryFrom:
			cpBase, cpNextSeq, cpRows = h.log.DurableLSN(), disklog.NextSeqOf(h.log), disklog.SegmentUsageOf(h.log)
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
		case toSubmit > 0 && h.log.Err() == nil && (len(h.pending) == 0 || r.Chance(2, 5)) && r.Chance(1, 8):
			toSubmit--
			n := int(r.Range(1, min(5000, maxBlob)))
			buf, err := h.pool.Alloc(n)
			if err != nil {
				stats.poolFull++
				continue
			}
			r.Fill(h.pool.Bytes(buf)[:n])
			payload := bytes.Clone(h.pool.Bytes(buf)[:n])
			owner := uint32(r.Range(1, 2))
			tk, err := h.log.MergePiece(&h.ctx, int(r.Range(1, uint64(cfg.MergeWorkers))), owner, buf, uint64(n), &h.out)
			if err != nil {
				t.Fatalf("seed %d: merge piece: %v", seed, err)
			}
			items[tk] = &submitted{entry: disklog.BlobEntry(disklog.BlobRun, owner, uint64(n)), payload: payload}
			stats.mergePieces++
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
	return h
}

// maxSeedSteps bounds runSeed's loop, so a seed that stops making progress fails instead of
// growing its items without end.
const maxSeedSteps = 100_000

// Regression (seed 1957): on a 16-block disk with no free segment every checkpoint fails with
// NoSpace as it begins, and beginning the next one before a record became durable never ended.
func TestACheckpointFailedForWantOfSpaceWaitsForADurableRecord(t *testing.T) {
	runSeed(t, 1957, nil, &seedStats{})
}

func TestEveryAcceptedItemDecodes(t *testing.T) {
	var stats seedStats
	seeds.Run(t, 1, 100, func(seed uint64) {
		h := runSeed(t, seed, nil, &stats)
		if err := trace.Check(h.sched.Trace(), diskLogRules(h.cfg.CommitDepth)...); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		if err := sim.CompareTraces(h.sched.Trace(), runSeed(t, seed, nil, &seedStats{}).sched.Trace()); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
	})
	if !seeds.Default() {
		return
	}
	if stats.noSpace == 0 || stats.tooLarge == 0 || stats.deferredFlushes == 0 || stats.deepRuns == 0 ||
		stats.poolFull == 0 || stats.inDoubt == 0 || stats.checkpoints == 0 || stats.checkpointsFailed == 0 || stats.manifestPieces == 0 || stats.mergePieces == 0 {
		t.Fatalf("seeds missed a case: %+v", stats)
	}
	t.Logf("%+v", stats)
}

// randomEntry is an entry of any kind byte with random values, which half the time has one
// field set, its kind's or another's, beyond what its constructor sets.
func randomEntry(r *core.Rand, maxBlob uint64) disklog.Entry {
	pool, chunk, version := core.PoolID(r.Below(4)), core.ChunkID{Volume: core.VolumeID(r.Uint32()), Inode: core.InodeID(r.Uint64()), Index: r.Uint32()}, r.Uint64()
	first, count := uint16(r.Below(core.BlocksPerChunk+1)), uint16(r.Range(1, 3))
	if r.Chance(1, 4) {
		count = uint16(r.Below(core.BlocksPerChunk + 2))
	}
	var e disklog.Entry
	switch k := disklog.EntryKind(r.Below(6)); k {
	case disklog.EntryWrite:
		e = disklog.WriteEntry(pool, chunk, version, first, count)
	case disklog.EntryPunch:
		e = disklog.PunchEntry(pool, chunk, version, first, count)
	case disklog.EntryFreeze:
		e = disklog.FreezeEntry(pool, chunk, version, r.Uint64())
	case disklog.EntryBlob:
		e = disklog.BlobEntry(disklog.BlobKind(r.Below(256)), r.Uint32(), r.Below(maxBlob+2))
	default:
		e = disklog.Entry{Kind: k, Pool: pool, Chunk: chunk, Version: version}
	}
	if r.Chance(1, 2) {
		switch r.Below(9) {
		case 0:
			e.Pool = core.PoolID(r.Uint32() | 1)
		case 1:
			e.Chunk.Index = r.Uint32() | 1
		case 2:
			e.Version = r.Uint64() | 1
		case 3:
			e.First = uint16(r.Range(1, 3))
		case 4:
			e.Count = uint16(r.Range(1, 3))
		case 5:
			e.Epoch = r.Uint64() | 1
		case 6:
			e.BlobKind = disklog.BlobKind(r.Range(1, 3))
		case 7:
			e.Owner = r.Uint32() | 1
		case 8:
			e.Length = r.Range(1, 100)
		}
	}
	return e
}

// TestAnythingSubmitAcceptsDecodes submits random entries with buffers of their size, one
// off or missing, and checks that every item Submit accepts ends durable in a record that
// decodes to it, and that every refusal is one Submit documents.
func TestAnythingSubmitAcceptsDecodes(t *testing.T) {
	accepted := map[disklog.EntryKind]int{}
	refused := map[error]int{}
	known := []error{disklog.ErrMalformed, disklog.ErrBadExtent, disklog.ErrTooLarge, disklog.ErrBadBuffer, disklog.ErrReservedKind}
	seeds.Run(t, 1, 20, func(seed uint64) {
		r := core.NewRand(seed)
		cfg := testConfig(r.Range(3, 64))
		cfg.CommitDepth = int(r.Range(1, 3))
		h := formatted(t, gib, cfg)
		items := map[disklog.Ticket]*submitted{}
		for range 300 {
			e := randomEntry(r, h.log.MaxBlobBytes())
			n := int(e.Length)
			if e.Kind == disklog.EntryWrite {
				n = int(e.Count) * core.BlockSize
			}
			if r.Chance(1, 8) {
				n += (int(r.Below(3)) - 1) * core.BlockSize
			}
			it := disklog.Item{Entry: e}
			var payload []byte
			if n > 0 && n <= 64*core.BlockSize {
				it.Buf = h.alloc(n)
				b := h.pool.Bytes(it.Buf)
				r.Fill(b)
				payload = bytes.Clone(b)
				if e.Kind == disklog.EntryBlob {
					payload = payload[:min(uint64(len(b)), e.Length)]
				}
			}
			tk, err := h.log.Submit(&h.ctx, disklog.Stream(r.Below(2)), it, &h.out)
			if err != nil {
				i := slices.IndexFunc(known, func(k error) bool { return errors.Is(err, k) })
				if i < 0 {
					t.Fatalf("seed %d: submit %+v: %v", seed, e, err)
				}
				refused[known[i]]++
				if it.Buf != core.NoBuf {
					h.pool.Free(it.Buf)
				}
				continue
			}
			accepted[e.Kind]++
			items[tk] = &submitted{entry: e, payload: payload}
			h.absorb()
			if r.Chance(1, 4) {
				h.run()
			}
		}
		h.run()
		for _, ev := range h.eventsOf(disklog.Durable) {
			d := h.record(ev)
			for i, tk := range ev.Tickets {
				it := items[tk]
				if it == nil || it.done || d.Entries[i] != it.entry || !bytes.Equal(d.Payload(i), it.payload) {
					t.Fatalf("seed %d: LSN %d entry %d is not what ticket %d submitted", seed, ev.LSN, i, tk)
				}
				it.done = true
			}
		}
		for tk, it := range items {
			if !it.done {
				t.Fatalf("seed %d: ticket %d never became durable", seed, tk)
			}
		}
		h.close()
	})
	if !seeds.Default() {
		return
	}
	for _, k := range []disklog.EntryKind{disklog.EntryWrite, disklog.EntryPunch, disklog.EntryFreeze, disklog.EntryBlob} {
		if accepted[k] == 0 {
			t.Fatalf("no entry of kind %d accepted: %v", k, accepted)
		}
	}
	for _, k := range known {
		if refused[k] == 0 {
			t.Fatalf("no item refused with %v: %v", k, refused)
		}
	}
}

func TestDecodeCheckCatchesABrokenEncoder(t *testing.T) {
	record := func(head []byte) []byte {
		if string(head[:8]) == "RSOLIDSG" {
			return head[core.BlockSize:]
		}
		return head
	}
	header := func(f func(head []byte)) func(*disklog.Log) {
		return func(l *disklog.Log) { disklog.BreakEncoding(l, f) }
	}
	breakers := []struct {
		name   string
		broken func(*disklog.Log)
	}{
		{"wrong version, checksum fixed", header(func(head []byte) {
			rec := record(head)
			rec[48+21]++
			disklog.FixRecordCRC(rec)
		})},
		{"flipped bit", header(func(head []byte) { head[len(head)-1] ^= 1 })},
		{"segment header of the next stream, checksum fixed", header(func(head []byte) {
			if &record(head)[0] != &head[0] {
				head[10] = (head[10] + 1) % 3
				disklog.FixSegmentCRC(head)
			}
		})},
		{"segment header of the stream after next, checksum fixed", header(func(head []byte) {
			if &record(head)[0] != &head[0] {
				head[10] = (head[10] + 2) % 3
				disklog.FixSegmentCRC(head)
			}
		})},
		{"segment header of another lane, checksum fixed", header(func(head []byte) {
			if &record(head)[0] != &head[0] {
				head[11] ^= 1
				disklog.FixSegmentCRC(head)
			}
		})},
		{"payload offset misreported", disklog.MisreportPayloads},
		{"write longer than its record", disklog.PadWrites},
	}
	for _, b := range breakers {
		t.Run(b.name, func(t *testing.T) {
			for seed := uint64(1); seed <= 100; seed++ {
				func() {
					defer func() {
						if msg, _ := recover().(string); !strings.Contains(msg, "decode check") {
							t.Fatalf("seed %d: a broken encoder passed the decode check: %q", seed, msg)
						}
					}()
					runSeed(t, seed, func(h *harness) { b.broken(h.log) }, &seedStats{})
				}()
			}
		})
	}
}
