package disklog_test

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
	"github.com/nvcnvn/rock-solid/internal/trace"
)

// preludeChecked is how many leading bytes of a record header DecodeHeader checks before its
// checksum: the magic, the version and the header's length in blocks.
const preludeChecked = 8

func onIO(io core.IOID) sim.Pred {
	return func(op sim.DiskOp, _ *core.Rand) bool { return op.IO == io }
}

// roundWrite is the IO of round lsn's write in a log formatted at firstIO and never recovered.
func roundWrite(lsn uint64) core.IOID { return firstIO + 4 + 2*core.IOID(lsn-1) }

func punch(i uint32) disklog.Item {
	return disklog.Item{Entry: disklog.PunchEntry(1, chunkOf(i), uint64(i), 0, 1)}
}

// decodeAt decodes the record of n blocks at lba as if it sat at block of a segment of seq.
func (h *harness) decodeAt(lba core.LBA, n uint64, seq uint64, block uint32) error {
	_, err := disklog.DecodeRecord(h.read(lba, n), disklog.Place{LogID: h.logID, Seq: seq, Offset: block}, h.geo.SegmentBlocks-block, nil)
	return err
}

func firstDiff(a, b []byte) int {
	for i := range a {
		if a[i] != b[i] {
			return i
		}
	}
	return -1
}

func wantCodecError(t *testing.T, what string, err error, off int) {
	t.Helper()
	switch {
	case err == nil:
		t.Fatalf("%s decoded", what)
	case off >= preludeChecked && !errors.Is(err, disklog.ErrBadChecksum):
		t.Fatalf("%s, changed at byte %d: %v, want a checksum error", what, off, err)
	}
}

func TestADamagedReadOfARecordNeverDecodes(t *testing.T) {
	h := formatted(t, gib, testConfig(0))
	h.twoBlockWrites(5)
	rec := h.eventsOf(disklog.Durable)[2]
	place := disklog.Place{LogID: h.logID, Seq: h.segmentSeq(rec.Segment), Offset: rec.Block}
	want := h.read(rec.LBA, 3)
	buf := h.alloc(len(want))
	inHeader, inData := 0, 0
	for range 200 {
		h.disk.Inject(sim.Fault{Effect: sim.DamageRead, When: sim.Once(sim.OnKind(core.ActDiskRead))})
		if ev := h.disk.Execute(h.pool, &core.Action{Kind: core.ActDiskRead, IO: 1, LBA: rec.LBA, Buf: buf, Len: uint64(len(want))}); ev.Err != core.DiskOK {
			t.Fatalf("damaged read: %+v", ev)
		}
		got := h.pool.Bytes(buf)[:len(want)]
		off := firstDiff(got, want)
		if off < 0 {
			t.Fatal("a damaged read changed no byte")
		}
		_, err := disklog.DecodeRecord(got, place, h.geo.SegmentBlocks-rec.Block, nil)
		wantCodecError(t, fmt.Sprintf("record damaged at byte %d", off), err, off)
		if off < core.BlockSize {
			inHeader++
			_, err := disklog.DecodeHeader(got, place, h.geo.SegmentBlocks-rec.Block, nil)
			wantCodecError(t, fmt.Sprintf("header damaged at byte %d", off), err, off)
		} else {
			inData++
		}
		h.disk.ClearFaults()
	}
	if inHeader == 0 || inData == 0 {
		t.Fatalf("damage fell in the header %d times and in the data %d times", inHeader, inData)
	}
}

// driveRandomly submits items writes of 1 to 3 blocks and completes the log's actions in an
// order r chooses, and returns the tickets submitted.
func driveRandomly(h *harness, r *core.Rand, items int) []disklog.Ticket {
	h.t.Helper()
	var tickets []disklog.Ticket
	for items > 0 || len(h.pending) > 0 {
		if items > 0 && (len(h.pending) == 0 || r.Chance(2, 5)) {
			items--
			it := h.chunkWrite(uint32(r.Below(16)), 0, uint16(r.Range(1, 3)), uint64(items))
			tk, err := h.log.Submit(&h.ctx, disklog.Stream(r.Below(2)), it, &h.out)
			if err != nil {
				h.pool.Free(it.Buf)
				continue
			}
			tickets = append(tickets, tk)
			h.absorb()
			continue
		}
		var ready []int
		for i := range h.pending {
			if h.eligible(i) {
				ready = append(ready, i)
			}
		}
		h.step(ready[r.Below(uint64(len(ready)))])
	}
	return tickets
}

func TestAFailedWriteOrFlushIsFinalForTheOpenLog(t *testing.T) {
	failures := 0
	for _, kind := range []core.ActionKind{core.ActDiskWrite, core.ActDiskFsync} {
		seeds.Run(t, 1, 20, func(seed uint64) {
			r := core.NewRand(seed)
			cfg := testConfig(0)
			cfg.CommitDepth = int(r.Range(1, 3))
			h := schedHarness(t, seed, gib, 64<<20, cfg)
			h.run()
			failing := r.Range(2, 8)
			io := roundWrite(failing)
			if kind == core.ActDiskFsync {
				io++
			}
			h.disk.Inject(sim.Fault{Effect: sim.FailIO, When: onIO(io)})
			driveRandomly(h, r, 60)
			if h.log.Err() == nil {
				t.Fatalf("seed %d: the log outlived a failed io%d", seed, io)
			}
			text := h.sched.Trace()
			failed := bytes.Index(text, fmt.Appendf(nil, "DiskDone io=%d err=io", io))
			if failed < 0 {
				t.Fatalf("seed %d: io%d never failed", seed, io)
			}
			for _, what := range []string{"Durable", "DiskWrite", "DiskFsync"} {
				if bytes.Contains(text[failed:], []byte(what)) {
					t.Fatalf("seed %d: %s after io%d failed", seed, what, io)
				}
			}
			if h.log.DurableLSN() >= failing && !bytes.Contains(text[:failed], fmt.Appendf(nil, "Durable round=%d ", failing)) {
				t.Fatalf("seed %d: round %d acknowledged though its io%d failed", seed, failing, io)
			}
			failures++
			if err := trace.Check(text, diskLogRules(cfg.CommitDepth)...); err != nil {
				t.Fatalf("seed %d: %v", seed, err)
			}
			if _, err := h.log.Submit(&h.ctx, disklog.Hot, punch(1), &h.out); !errors.Is(err, core.DiskIO) {
				t.Fatalf("seed %d: submit after the failure: %v", seed, err)
			}
			acked := h.log.DurableLSN()
			h.disk.ClearFaults()
			res := h.reopen()
			if res.durable < acked || res.durable > acked+uint64(cfg.CommitDepth) {
				t.Fatalf("seed %d: recovered LSN %d, %d acknowledged", seed, res.durable, acked)
			}
			h.put(disklog.Hot, punch(2))
			if d := h.eventsOf(disklog.Durable); len(d) != 1 || d[0].LSN != res.durable+1 {
				t.Fatalf("seed %d: the recovered log's next record: %+v", seed, d)
			}
		})
	}
	if !seeds.Default() {
		return
	}
	if failures == 0 {
		t.Fatal("no seed failed an action")
	}
}

func TestTheFirstFlushFailedOnEverySeedKeepsTheFailureRule(t *testing.T) {
	failFirstFlush := func(h *harness) { h.failIO[roundWrite(1)+1] = core.DiskIO }
	seeds.Run(t, 1, 100, func(seed uint64) {
		h := runSeed(t, seed, failFirstFlush, &seedStats{})
		if !bytes.Contains(h.sched.Trace(), fmt.Appendf(nil, "DiskDone io=%d err=io", roundWrite(1)+1)) {
			t.Fatalf("seed %d: the first flush did not fail", seed)
		}
		if err := trace.Check(h.sched.Trace(), diskLogRules(h.cfg.CommitDepth)...); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
	})
}

func TestALogThatIgnoresAFailedFlushBreaksTheFailureRule(t *testing.T) {
	h := schedHarness(t, 1, gib, 64<<20, testConfig(0))
	h.run()
	h.ignoreErrors = true
	h.failIO[roundWrite(1)+1] = core.DiskIO
	for i := range 3 {
		h.put(disklog.Hot, punch(uint32(i)))
	}
	var v *trace.Violation
	if err := trace.Check(h.sched.Trace(), trace.NoDurableAfterFailure()); !errors.As(err, &v) || v.Rule != "no-durable-after-failure" {
		t.Fatalf("a log that acknowledges after a failed flush: %v", err)
	}
}

func TestAMisdirectedRecordIsMissingAtItsPlaceAndAMismatchAtTheOther(t *testing.T) {
	const freeSegment = 20
	cases := []struct {
		name     string
		depth    int
		record   uint64
		inFlight bool
		to       func(h *harness) core.LBA
		write    func(h *harness)
		lsns     []uint64
		corrupt  uint64
	}{
		{
			name: "record 5, into a free segment, crash before its flush", depth: 1, record: 5,
			to: func(h *harness) core.LBA { return h.geo.LBA(freeSegment, 0) },
			write: func(h *harness) {
				h.submit(disklog.Hot, punch(5))
				h.step(0)
				h.crashLanding(h.geo.LBA(freeSegment, 0))
			},
			lsns: lsnRange(1, 4),
		},
		{
			name: "record 3, later rounds built after it was durable", depth: 1, record: 3,
			to:      func(h *harness) core.LBA { return h.geo.LBA(freeSegment, 0) },
			write:   func(h *harness) { h.put(disklog.Hot, punch(5)) },
			corrupt: 3,
		},
		{
			name: "record 5 over record 2's header block", depth: 1, record: 5,
			to:      func(h *harness) core.LBA { return h.geo.LBA(1, 2) },
			write:   func(h *harness) { h.put(disklog.Hot, punch(5)) },
			corrupt: 2,
		},
		{
			name: "record 4 of 5, both rounds in flight together", depth: 2, record: 4, inFlight: true,
			to: func(h *harness) core.LBA { return h.geo.LBA(freeSegment, 0) },
			write: func(h *harness) {
				h.submit(disklog.Hot, punch(5))
				h.stepWrites()
				h.disk.Crash(func(int, core.LBA) bool { return true })
			},
			lsns: lsnRange(1, 3),
		},
		{
			name: "record 4, round 5 begun after 4 was durable", depth: 2, record: 4,
			to:      func(h *harness) core.LBA { return h.geo.LBA(freeSegment, 0) },
			write:   func(h *harness) { h.put(disklog.Hot, punch(5)) },
			corrupt: 4,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := testConfig(0)
			cfg.CommitDepth = c.depth
			h := formatted(t, gib, cfg)
			to := c.to(h)
			h.disk.Inject(sim.Fault{Effect: sim.Misdirect, When: onIO(roundWrite(c.record)), To: func(sim.DiskOp) core.LBA { return to }})
			for i := uint32(1); i <= 4; i++ {
				if c.inFlight && i == 4 {
					h.submit(disklog.Hot, punch(i))
					continue
				}
				h.put(disklog.Hot, punch(i))
			}
			c.write(h)
			h.disk.ClearFaults()
			if m := h.disk.Misdirected(); len(m) != 1 || m[0].IO != roundWrite(c.record) || m[0].Landed != to || m[0].Intended != h.geo.LBA(1, uint32(c.record)) {
				t.Fatalf("misdirected %+v", m)
			}
			seq := h.segmentSeq(1)
			if err := h.decodeAt(to, 1, seq, uint32(c.record)); err != nil {
				t.Fatalf("the misdirected block does not hold record %d: %v", c.record, err)
			}
			offset := uint32(uint64(to) % uint64(h.geo.SegmentBlocks))
			if err := h.decodeAt(to, 1, seq, offset); !errors.Is(err, disklog.ErrMismatch) {
				t.Fatalf("record %d at block %d of segment 1: %v, want a mismatch", c.record, offset, err)
			}
			if c.corrupt != 0 {
				wantCorrupt(t, h.recover(disklog.ModeOpen), c.corrupt)
				wantCorrupt(t, h.recover(disklog.ModeRebuild), c.corrupt)
				return
			}
			res := h.reopen()
			h.wantLSNs(res, c.lsns)
			if len(res.quarantined) != 0 {
				t.Fatalf("quarantined %v", res.quarantined)
			}
			h.put(disklog.Hot, punch(9))
			res = h.reopen()
			h.wantLSNs(res, lsnRange(1, c.lsns[len(c.lsns)-1]+1))
		})
	}
}

func TestBlocksAMisdirectedWriteOverwritesAreDamage(t *testing.T) {
	t.Run("a record's data block", func(t *testing.T) {
		h := formatted(t, gib, testConfig(0))
		h.twoBlockWrites(4)
		second := h.eventsOf(disklog.Durable)[1]
		h.disk.Inject(sim.Fault{Effect: sim.Misdirect, When: onIO(roundWrite(5)), To: func(sim.DiskOp) core.LBA { return second.LBA + 1 }})
		h.put(disklog.Hot, punch(5))
		h.disk.ClearFaults()
		if err := h.decodeAt(second.LBA, 3, h.segmentSeq(1), second.Block); !errors.Is(err, disklog.ErrBadChecksum) {
			t.Fatalf("record 2 with a data block overwritten: %v", err)
		}
		wantCorrupt(t, h.recover(disklog.ModeOpen), 2)
		wantCorrupt(t, h.recover(disklog.ModeRebuild), 2)
	})
	t.Run("a segment header", func(t *testing.T) {
		h := formatted(t, 1<<16*core.BlockSize, testConfig(0))
		for i := range 60 {
			h.put(disklog.Hot, h.chunkWrite(uint32(i), 0, 1, uint64(i+1)))
		}
		h.disk.Inject(sim.Fault{Effect: sim.Misdirect, When: onIO(roundWrite(61)), To: func(sim.DiskOp) core.LBA { return h.geo.LBA(1, 0) }})
		h.put(disklog.Hot, punch(61))
		h.disk.ClearFaults()
		wantCorrupt(t, h.recover(disklog.ModeOpen), 1)
	})
}

// ackedRecord is an acknowledged record and the durable LSN its header names.
type ackedRecord struct {
	lsn, named uint64
}

// lostAcknowledged returns the first acknowledged record a recovery to LSN durable lost that
// the single-disk invariant does not excuse: only a misdirected record no later acknowledged
// header names durable may be lost.
func lostAcknowledged(h *harness, acked []ackedRecord, durable uint64) error {
	for i, a := range acked {
		if a.lsn <= durable {
			continue
		}
		misdirected := slices.ContainsFunc(h.disk.Misdirected(), func(m sim.MisdirectedWrite) bool { return m.IO == roundWrite(a.lsn) })
		named := slices.ContainsFunc(acked[i+1:], func(later ackedRecord) bool { return later.named >= a.lsn })
		if !misdirected || named {
			return fmt.Errorf("acknowledged LSN %d lost (misdirected %v, named by a later header %v)", a.lsn, misdirected, named)
		}
	}
	return nil
}

func TestTheAcknowledgedWriteInvariantExcusesOnlyAnUnnamedMisdirectedRecord(t *testing.T) {
	setup := func(t *testing.T, misdirect bool) (*harness, []ackedRecord) {
		h := formatted(t, gib, testConfig(0))
		if misdirect {
			h.disk.Inject(sim.Fault{Effect: sim.Misdirect, When: onIO(roundWrite(5)), To: func(sim.DiskOp) core.LBA { return h.geo.LBA(20, 0) }})
		}
		var acked []ackedRecord
		for i := uint32(1); i <= 5; i++ {
			named := h.log.DurableLSN()
			h.put(disklog.Hot, punch(i))
			acked = append(acked, ackedRecord{lsn: h.log.DurableLSN(), named: named})
		}
		h.disk.ClearFaults()
		return h, acked
	}
	t.Run("the last record misdirected", func(t *testing.T) {
		h, acked := setup(t, true)
		res := h.reopen()
		h.wantLSNs(res, lsnRange(1, 4))
		if err := lostAcknowledged(h, acked, res.durable); err != nil {
			t.Fatal(err)
		}
		if err := lostAcknowledged(h, acked, 3); err == nil {
			t.Fatal("losing record 4, which record 5's header names, was excused")
		}
	})
	t.Run("the last record lost another way", func(t *testing.T) {
		h, acked := setup(t, false)
		h.disk.Put(h.geo.LBA(1, 5), make([]byte, core.BlockSize))
		res := h.reopen()
		h.wantLSNs(res, lsnRange(1, 4))
		if err := lostAcknowledged(h, acked, res.durable); err == nil {
			t.Fatal("a record lost without a misdirected write was excused")
		}
	})
}

// formattedOn is formatted on a disk with settings set.
func formattedOn(t *testing.T, diskBytes uint64, set sim.DiskSettings, cfg disklog.Config) *harness {
	t.Helper()
	h := blankHarness(t, diskBytes, 64<<20, cfg)
	h.disk = sim.NewDiskWith(diskBytes/core.BlockSize, set)
	var err error
	h.log, err = disklog.Format(&h.ctx, h.cfg, disklog.Superblock{LogID: testLogID, Geometry: h.geo}, firstIO, &h.out)
	if err != nil {
		t.Fatal(err)
	}
	h.own = h.pool.Live()
	h.absorb()
	h.run()
	h.trace = nil
	return h
}

func TestABitFlipInARecordFailsItsChecksum(t *testing.T) {
	inHeader, inData := 0, 0
	seeds.Run(t, 1, 20, func(seed uint64) {
		set := sim.DefaultDiskSettings()
		set.Seed = seed
		h := formattedOn(t, gib, set, testConfig(0))
		r := core.NewRand(seed)
		for i := range 30 {
			if r.Chance(1, 3) {
				h.put(disklog.Hot, h.blob(disklog.BlobRaftEntries, 1, int(r.Range(1, 9000)), uint64(i)))
				continue
			}
			h.put(disklog.Hot, h.chunkWrite(uint32(i), 0, uint16(r.Range(1, 3)), uint64(i+1)))
		}
		type placed struct {
			ev     disklog.Event
			seq    uint64
			blocks uint64
			image  []byte
		}
		var recs []placed
		for _, ev := range h.eventsOf(disklog.Durable) {
			d := h.record(ev)
			n := uint64(d.Layout.Blocks())
			recs = append(recs, placed{ev, d.Record.Place.Seq, n, h.read(ev.LBA, n)})
		}
		flipped := h.disk.FlipRandomBits(1, 8)
		for _, p := range recs {
			off := firstDiff(h.read(p.ev.LBA, p.blocks), p.image)
			err := h.decodeAt(p.ev.LBA, p.blocks, p.seq, p.ev.Block)
			if off < 0 {
				if err != nil {
					t.Fatalf("seed %d: LSN %d, not flipped: %v", seed, p.ev.LSN, err)
				}
				continue
			}
			wantCodecError(t, fmt.Sprintf("seed %d: LSN %d flipped at byte %d", seed, p.ev.LSN, off), err, off)
			if off < int(p.ev.HeaderBlocks)*core.BlockSize {
				inHeader++
			} else {
				inData++
			}
		}
		if len(flipped) == 0 {
			t.Fatalf("seed %d flipped nothing", seed)
		}
	})
	if !seeds.Default() {
		return
	}
	if inHeader == 0 || inData == 0 {
		t.Fatalf("flips hit %d headers and %d data areas", inHeader, inData)
	}
}

func TestEveryItemIsAcknowledgedOnAFailSlowDisk(t *testing.T) {
	run := func(seed uint64, slow uint64) (*harness, []disklog.Ticket) {
		r := core.NewRand(seed)
		cfg := testConfig(0)
		cfg.CommitDepth = int(r.Range(1, 3))
		h := schedHarness(t, seed, gib, 64<<20, cfg)
		h.disk.SetSlow(slow)
		h.run()
		return h, driveRandomly(h, r, 200)
	}
	seeds.Run(t, 1, 10, func(seed uint64) {
		healthy, _ := run(seed, 1)
		h, tickets := run(seed, 100)
		var acked []disklog.Ticket
		for _, ev := range h.eventsOf(disklog.Durable) {
			acked = append(acked, ev.Tickets...)
		}
		slices.Sort(acked)
		slices.Sort(tickets)
		if len(tickets) == 0 || !slices.Equal(acked, tickets) || h.log.Err() != nil {
			t.Fatalf("seed %d: %d of %d items acknowledged, log error %v", seed, len(acked), len(tickets), h.log.Err())
		}
		if bytes.Contains(h.sched.Trace(), []byte("err=io")) {
			t.Fatalf("seed %d: an action failed on a slow disk", seed)
		}
		if h.sched.Time() != 100*healthy.sched.Time() {
			t.Fatalf("seed %d: %d µs on the slow disk, %d on a healthy one", seed, h.sched.Time(), healthy.sched.Time())
		}
	})
}
