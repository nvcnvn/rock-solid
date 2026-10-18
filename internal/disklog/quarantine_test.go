package disklog_test

import (
	"bytes"
	"errors"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// usedSegments reads every segment header and returns the segments of this log, and the
// newest manifest they name.
func (h *harness) usedSegments() ([]uint32, disklog.ManifestPos) {
	var used []uint32
	var newest disklog.ManifestPos
	for s := uint32(1); s < h.geo.SegmentCount; s++ {
		sh, err := disklog.DecodeSegmentHeader(h.read(h.geo.LBA(s, 0), 1))
		if err == nil && sh.LogID == h.logID && sh.Index == s {
			used = append(used, s)
			if sh.Manifest.LSN > newest.LSN {
				newest = sh.Manifest
			}
		}
	}
	return used, newest
}

func TestQuarantinedSegmentsAreNeverReused(t *testing.T) {
	for seed := uint64(1); seed <= 10; seed++ {
		r := core.NewRand(seed)
		cfg := testConfig(4)
		cfg.MaxReplayBytes = 40 * core.BlockSize
		h := formatted(t, 4096*core.BlockSize, cfg)
		n := newNode(h, 0)
		lastLSN := map[uint32]uint64{}
		firstLSN := map[uint32]uint64{}
		h.onEvent = func(ev disklog.Event) {
			n.onEvent(ev)
			if ev.Kind == disklog.Durable {
				lastLSN[ev.Segment] = ev.LSN
				if _, ok := firstLSN[ev.Segment]; !ok {
					firstLSN[ev.Segment] = ev.LSN
				}
			}
		}
		for i := range 300 {
			h.submit(disklog.Hot, h.chunkWrite(uint32(r.Below(16)), 0, uint16(r.Range(1, 2)), uint64(i+1)))
			for {
				h.run()
				if !n.step() {
					break
				}
			}
		}
		used, named := h.usedSegments()
		replayLSN := h.readManifestAt(named).Indexes[0].ReplayLSN
		var old []uint32
		for _, s := range used {
			if lastLSN[s] <= replayLSN {
				old = append(old, s)
			}
		}
		r.Shuffle(len(old), func(i, j int) { old[i], old[j] = old[j], old[i] })
		damaged := slices.Sorted(slices.Values(old[:3]))
		saved := map[uint32][]byte{}
		for _, s := range damaged {
			h.disk.FlipBit(h.geo.LBA(s, 0), int(r.Below(52)), 1<<r.Below(8))
			saved[s] = h.read(h.geo.LBA(s, 0), uint64(h.geo.SegmentBlocks))
		}
		freeSeg := h.geo.SegmentCount - 1
		h.disk.FlipBit(h.geo.LBA(freeSeg, 0), int(r.Below(52)), 1<<r.Below(8))
		h.ioRand = r.Fork()

		res := h.reopen()
		if !slices.Equal(res.quarantined, damaged) {
			t.Fatalf("seed %d: quarantined %v, want %v", seed, res.quarantined, damaged)
		}
		if free := disklog.FreeSegments(h.log); free != int(h.geo.SegmentCount)-1-len(used) {
			t.Fatalf("seed %d: %d segments free, want %d", seed, free, int(h.geo.SegmentCount)-1-len(used))
		}
		h.wantModel("open", res.index, n.live)
		n.live = res.index
		h.onEvent = n.onEvent
		noSpace := false
		for i := 0; !noSpace; i++ {
			h.submit(disklog.Hot, h.chunkWrite(uint32(r.Below(16)), 0, 2, uint64(1000+i)))
			h.run()
			for _, ev := range h.eventsOf(disklog.Failed) {
				noSpace = noSpace || errors.Is(ev.Err, disklog.ErrNoSpace)
			}
		}
		for _, s := range damaged {
			if !bytes.Equal(h.read(h.geo.LBA(s, 0), uint64(h.geo.SegmentBlocks)), saved[s]) {
				t.Fatalf("seed %d: quarantined segment %d was written", seed, s)
			}
		}
		if sh, err := disklog.DecodeSegmentHeader(h.read(h.geo.LBA(freeSeg, 0), 1)); err != nil || sh.LogID != testLogID {
			t.Fatalf("seed %d: the free segment with a damaged header was never reused: %+v, %v", seed, sh, err)
		}
		res = h.reopen()
		h.wantModel("after filling", res.index, n.live)
		h.readBack(res.index)
		minLSN := firstLSN[damaged[0]]
		for _, s := range damaged {
			minLSN = min(minLSN, firstLSN[s])
		}
		wantCorrupt(t, h.recover(disklog.ModeRebuild), minLSN)
	}
}

// readManifestAt reads the manifest whose last piece sits at pos.
func (h *harness) readManifestAt(pos disklog.ManifestPos) disklog.Manifest {
	h.t.Helper()
	return h.readManifest(disklog.Event{LSN: pos.LSN, Segment: pos.Segment, Block: pos.Block, LBA: h.geo.LBA(pos.Segment, pos.Block)})
}

func TestQuarantineReplayThroughADamagedSegmentIsCorruption(t *testing.T) {
	oneBlockWrites := func(h *harness, n int) {
		for i := range n {
			h.put(disklog.Hot, h.chunkWrite(uint32(i), 0, 1, uint64(i+1)))
		}
	}
	t.Run("the first segment", func(t *testing.T) {
		h := formatted(t, 1<<16*core.BlockSize, testConfig(0))
		oneBlockWrites(h, 60)
		h.disk.FlipBit(h.geo.LBA(1, 0), 30, 1)
		wantCorrupt(t, h.recover(disklog.ModeOpen), 1)
	})
	t.Run("the newest segment with several records", func(t *testing.T) {
		h := formatted(t, 1<<16*core.BlockSize, testConfig(0))
		oneBlockWrites(h, 60)
		last := h.eventsOf(disklog.Durable)[59]
		first := slices.IndexFunc(h.eventsOf(disklog.Durable), func(ev disklog.Event) bool { return ev.Segment == last.Segment })
		h.disk.FlipBit(h.geo.LBA(last.Segment, 0), 30, 1)
		wantCorrupt(t, h.recover(disklog.ModeOpen), uint64(first+1))
	})
	t.Run("the newest segment with one record", func(t *testing.T) {
		h := formatted(t, 1<<16*core.BlockSize, testConfig(0))
		oneBlockWrites(h, 64)
		durable := h.eventsOf(disklog.Durable)
		last := durable[63]
		if durable[62].Segment == last.Segment || last.Block != 1 {
			t.Fatalf("record 64 at segment %d block %d does not open a segment", last.Segment, last.Block)
		}
		h.disk.FlipBit(h.geo.LBA(last.Segment, 0), 30, 1)
		segment := h.read(h.geo.LBA(last.Segment, 0), uint64(h.geo.SegmentBlocks))
		wantCorrupt(t, h.recover(disklog.ModeOpen), 64)
		if !bytes.Equal(h.read(h.geo.LBA(last.Segment, 0), uint64(h.geo.SegmentBlocks)), segment) {
			t.Fatal("recovery wrote into the damaged segment")
		}
	})
}
