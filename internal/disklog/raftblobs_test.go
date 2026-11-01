package disklog_test

import (
	"errors"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/disklog"
)

func blobRef(ev disklog.Event, tk disklog.Ticket) disklog.BlobRef {
	for i, t := range ev.Tickets {
		if t == tk {
			return disklog.BlobRef{Segment: ev.Segment, Block: ev.Block, LSN: ev.LSN, Entry: uint16(i)}
		}
	}
	panic("ticket not in its Durable event")
}

// A record's Raft blobs count live until their shard reports them dead, a block two of them share
// until both are, and an open recounts the blobs it replayed beside the owner's chunk counts.
func TestRaftBlobsStayLiveUntilTheirShardReportsThemDead(t *testing.T) {
	h := formatted(t, gib, testConfig(0))
	h.submit(disklog.Hot, h.write(1, 1, 1))
	w := h.submit(disklog.Hot, h.write(0, 1, 1))
	a := h.submit(disklog.Hot, h.blob(disklog.BlobRaftEntries, 7, 100, 2))
	b := h.submit(disklog.Hot, h.blob(disklog.BlobRaftState, 7, 100, 3))
	h.run()
	ev := h.durableOf(w)
	if h.durableOf(a).LSN != ev.LSN || h.durableOf(b).LSN != ev.LSN {
		t.Fatal("the write and the two blobs went out in different records")
	}
	seg, ra, rb := ev.Segment, blobRef(ev, a), blobRef(ev, b)
	if h.liveOf(seg) != 3 {
		t.Fatalf("segment %d counts %d live blocks, want the two writes' and the blobs' shared one", seg, h.liveOf(seg))
	}

	h.reopen()
	chunk := make([]uint32, h.geo.SegmentCount)
	chunk[seg] = 2
	h.log.Recount(chunk)
	if h.liveOf(seg) != 3 {
		t.Fatalf("after an open the recount left segment %d at %d live blocks, want 3", seg, h.liveOf(seg))
	}

	if err := h.log.RaftBlobsDead([]disklog.BlobRef{ra}); err != nil || h.liveOf(seg) != 3 {
		t.Fatalf("with one blob dead segment %d counts %d live blocks, want 3: %v", seg, h.liveOf(seg), err)
	}
	if err := h.log.RaftBlobsDead([]disklog.BlobRef{rb}); err != nil || h.liveOf(seg) != 2 {
		t.Fatalf("with both blobs dead segment %d counts %d live blocks, want 2: %v", seg, h.liveOf(seg), err)
	}
	if err := h.log.RaftBlobsDead([]disklog.BlobRef{rb}); !errors.Is(err, disklog.ErrMalformed) {
		t.Fatalf("a blob reported dead twice: %v", err)
	}
	h.log.Recount(chunk)
	if h.liveOf(seg) != 2 {
		t.Fatalf("a recount brought dead blobs back: segment %d counts %d live blocks", seg, h.liveOf(seg))
	}
	h.close()
}
