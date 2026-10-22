package disklog_test

import (
	"errors"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// The net-gain guard rounds header blocks up per record: in 16,384-block (64 MiB) segments a
// copy record of one-block entries holds 253 data blocks under 3 header blocks, so 16,191 live
// blocks cost 16,383 and pass, while 16,192 cost 16,384 and are refused.
func TestCopyCostRoundsHeadersUpPerRecord(t *testing.T) {
	h := blankHarness(t, 256<<30, 8<<20, testConfig(0))
	if h.geo.SegmentBlocks != 16384 {
		t.Fatalf("%d-block segments", h.geo.SegmentBlocks)
	}
	log, err := disklog.Format(&h.ctx, h.cfg, disklog.Superblock{LogID: testLogID, Geometry: h.geo}, firstIO, &h.out)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct{ live, cost uint64 }{{1, 4}, {253, 256}, {254, 260}, {16191, 16383}, {16192, 16384}} {
		if got := log.CopyCost(c.live); got != c.cost {
			t.Fatalf("%d live blocks cost %d, want %d", c.live, got, c.cost)
		}
	}
}

// A rewrite costs the records holding its pieces as they lie, header blocks included and each
// record once, and the tail the largest leaves behind in a segment too short for it: without
// that, a run filling a segment would fill a new one but for a block no record fits in, and its
// rewrite would gain nothing.
func TestRewriteCostChargesRecordsAndTheTail(t *testing.T) {
	h := formatted(t, gib, testConfig(0))
	h.put(disklog.Hot, h.write(0, 1, 1))
	big := h.checkpointPieces(nil, 1, 3*core.BlockSize).runs
	if got := h.log.RewriteCost(big); got != 4+3 {
		t.Fatalf("a 3-block piece in a 4-block record costs %d, want 7", got)
	}
	shared := h.checkpointPieces(nil, 3, 100).runs[1:]
	if shared[0].LSN != shared[1].LSN {
		t.Fatalf("the two pieces went out in records %d and %d, want one", shared[0].LSN, shared[1].LSN)
	}
	if got := h.log.RewriteCost(shared); got != 2+1 {
		t.Fatalf("two pieces sharing a 2-block record cost %d, want 3", got)
	}
	if got := h.log.RewriteCost(append(big, shared...)); got != 4+2+3 {
		t.Fatalf("both records cost %d, want 9", got)
	}
	h.close()
}

// Only a used segment that no lane has open and whose records all precede the replay points of
// the oldest manifest recovery may load can be cleaned; before any manifest is named, none can.
// Free takes one with no live block and no piece, and makes it reusable at once.
func TestWhatTheCleanerMayFree(t *testing.T) {
	h, _, lsns, m := usedLog(t)
	floor := m.Indexes[0].ReplayLSN
	open := map[disklog.Stream]uint32{}
	newest := map[disklog.Stream]uint64{}
	for s, l := range lsns {
		st := h.segmentHeader(s).Stream
		if top := l[len(l)-1]; top > newest[st] {
			newest[st], open[st] = top, s
		}
	}
	var past uint32
	for s, l := range lsns {
		isOpen := open[h.segmentHeader(s).Stream] == s
		want := !isOpen && l[len(l)-1] <= floor
		if got := h.log.Cleanable(s); got != want {
			t.Fatalf("segment %d (LSNs %v, open %v, replay floor %d): cleanable %v, want %v", s, l, isOpen, floor, got, want)
		}
		if !isOpen && l[len(l)-1] > floor {
			past = s
		}
	}
	if err := h.log.Free(&h.ctx, open[disklog.Hot], &h.out); !errors.Is(err, disklog.ErrNotCleanable) {
		t.Fatalf("freeing the open segment: %v", err)
	}
	if err := h.log.Free(&h.ctx, 1, &h.out); !errors.Is(err, disklog.ErrNotCleanable) {
		t.Fatalf("freeing segment 1 with live blocks: %v", err)
	}
	u, _ := h.log.Usage(1)
	if err := h.log.AddLive(1, -int(u.Live)); err != nil {
		t.Fatal(err)
	}
	free := disklog.FreeSegments(h.log)
	if err := h.log.Free(&h.ctx, 1, &h.out); err != nil || disklog.FreeSegments(h.log) != free+1 {
		t.Fatalf("freeing an empty cleanable segment: %v", err)
	}
	if err := h.log.Quarantine(1); !errors.Is(err, disklog.ErrMalformed) {
		t.Fatalf("quarantining a free segment: %v", err)
	}
	if past != 0 {
		if err := h.log.Free(&h.ctx, past, &h.out); !errors.Is(err, disklog.ErrNotCleanable) {
			t.Fatalf("freeing segment %d past the replay point: %v", past, err)
		}
	}
	q := uint32(2)
	if err := h.log.Quarantine(q); err != nil || h.log.Cleanable(q) {
		t.Fatalf("quarantined segment %d: %v, cleanable %v", q, err, h.log.Cleanable(q))
	}
	if u, _ := h.log.Usage(q); u.State != disklog.SegmentQuarantined {
		t.Fatalf("segment %d is %+v", q, u)
	}

	fresh := formatted(t, gib, testConfig(0))
	fresh.put(disklog.Hot, fresh.write(0, 1, 1))
	fresh.put(disklog.Hot, fresh.write(1, 1, 2))
	if fresh.log.Cleanable(1) {
		t.Fatal("a segment is cleanable before any manifest is named")
	}
}
