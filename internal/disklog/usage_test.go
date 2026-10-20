package disklog_test

import (
	"bytes"
	"fmt"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// usedLog writes 60 one-block hot writes on a 64K-block disk (16-block segments),
// checkpointing whenever 40 blocks follow the last manifest. It returns the LSNs each
// segment holds and the newest named manifest.
func usedLog(t *testing.T) (*harness, *node, map[uint32][]uint64, disklog.Manifest) {
	t.Helper()
	cfg := testConfig(0)
	cfg.MaxReplayBytes = 40 * core.BlockSize
	h := formatted(t, 1<<16*core.BlockSize, cfg)
	n := newNode(h, 0)
	lsns := map[uint32][]uint64{}
	h.onEvent = func(ev disklog.Event) {
		n.onEvent(ev)
		if ev.Kind == disklog.Durable {
			lsns[ev.Segment] = append(lsns[ev.Segment], ev.LSN)
		}
	}
	for i := range 60 {
		h.submit(disklog.Hot, h.chunkWrite(uint32(i%16), 0, 1, uint64(i+1)))
		for {
			h.run()
			if !n.step() {
				break
			}
		}
	}
	h.onEvent = n.onEvent
	_, named := h.usedSegments()
	return h, n, lsns, h.readManifestAt(named)
}

// readsInside reports whether a recovery trace reads any block of segment s but its header.
func (h *harness) readsInside(trace []string, s uint32) bool {
	for _, line := range trace {
		var lba core.LBA
		var n uint64
		if _, err := fmt.Sscanf(line, "read %d+%d", &lba, &n); err == nil && lba > h.geo.LBA(s, 0) && lba < h.geo.LBA(s+1, 0) {
			return true
		}
	}
	return false
}

// named is the newest manifest a segment header names, the one recovery loads.
func (h *harness) named() disklog.ManifestPos {
	_, pos := h.usedSegments()
	return pos
}

func TestUsageTableNeverFreesAUsedSegment(t *testing.T) {
	for name, damage := range map[string]func(h *harness){
		"zeroed header":  func(h *harness) { h.disk.Put(h.geo.LBA(1, 0), make([]byte, core.BlockSize)) },
		"zeroed segment": func(h *harness) { h.disk.Put(h.geo.LBA(1, 0), make([]byte, h.geo.SegmentBytes())) },
		"another log's header": func(h *harness) {
			b, err := disklog.AppendSegmentHeader(nil, disklog.SegmentHeader{LogID: testLogID + 1, Seq: 1, Index: 1})
			if err != nil {
				h.t.Fatal(err)
			}
			h.disk.Put(h.geo.LBA(1, 0), b)
		},
	} {
		t.Run(name, func(t *testing.T) {
			h, n, lsns, m := usedLog(t)
			if slices.Max(lsns[1]) > m.Indexes[0].ReplayLSN {
				t.Fatalf("segment 1 holds LSNs %v past the replay point %d", lsns[1], m.Indexes[0].ReplayLSN)
			}
			damage(h)
			segment := h.read(h.geo.LBA(1, 0), uint64(h.geo.SegmentBlocks))
			used, _ := h.usedSegments()
			res := h.reopen()
			if !slices.Equal(res.quarantined, []uint32{1}) {
				t.Fatalf("quarantined %v, want [1]", res.quarantined)
			}
			if free := disklog.FreeSegments(h.log); free != int(h.geo.SegmentCount)-2-len(used) {
				t.Fatalf("%d segments free, want %d", free, int(h.geo.SegmentCount)-2-len(used))
			}
			h.wantModel("open", res.index, n.live)
			n.live = res.index
			for i := range 16 {
				h.put(disklog.Hot, h.chunkWrite(uint32(i), 0, 1, uint64(1000+i)))
			}
			if !bytes.Equal(h.read(h.geo.LBA(1, 0), uint64(h.geo.SegmentBlocks)), segment) {
				t.Fatal("the zeroed segment was opened again")
			}
			res = h.reopen()
			if !slices.Equal(res.quarantined, []uint32{1}) {
				t.Fatalf("after reuse: quarantined %v, want [1]", res.quarantined)
			}
			h.wantModel("after reuse", res.index, n.live)
			h.readBack(res.index)
		})
	}
}

func TestUsageTableZeroedHeaderOverRecordsPastTheReplayPointIsCorruption(t *testing.T) {
	h, _, lsns, m := usedLog(t)
	replayLSN := m.Indexes[0].ReplayLSN
	for _, u := range m.Segments {
		i := slices.IndexFunc(lsns[u.Segment], func(lsn uint64) bool { return lsn > replayLSN })
		if i < 0 {
			continue
		}
		h.disk.Put(h.geo.LBA(u.Segment, 0), make([]byte, core.BlockSize))
		wantCorrupt(t, h.recover(disklog.ModeOpen), lsns[u.Segment][i])
		return
	}
	t.Fatalf("no segment the table lists holds a record past the replay point %d", replayLSN)
}

func TestUsageTableKeepsAQuarantine(t *testing.T) {
	h, n, _, _ := usedLog(t)
	header := h.read(h.geo.LBA(1, 0), 1)
	h.disk.FlipBit(h.geo.LBA(1, 0), 30, 1)
	res := h.reopen()
	if !slices.Equal(res.quarantined, []uint32{1}) || !h.readsInside(res.trace, 1) {
		t.Fatalf("quarantined %v, read in full %v; want [1] read in full", res.quarantined, h.readsInside(res.trace, 1))
	}
	n.live = res.index
	for i := 0; h.log.Manifest() == res.manifest || h.named() != h.log.Manifest(); i++ {
		h.submit(disklog.Hot, h.chunkWrite(uint32(i%16), 0, 1, uint64(2000+i)))
		for {
			h.run()
			if !n.step() {
				break
			}
		}
	}
	m := h.readManifestAt(h.log.Manifest())
	if i := slices.IndexFunc(m.Segments, func(u disklog.SegmentUsage) bool { return u.Segment == 1 }); i < 0 || m.Segments[i].State != disklog.SegmentQuarantined {
		t.Fatalf("the manifest's table %+v does not quarantine segment 1", m.Segments)
	}
	res = h.reopen()
	if !slices.Equal(res.quarantined, []uint32{1}) || h.readsInside(res.trace, 1) {
		t.Fatalf("quarantined %v, read in full %v; want [1] left unread", res.quarantined, h.readsInside(res.trace, 1))
	}
	n.live = res.index
	h.disk.Put(h.geo.LBA(1, 0), header)
	res = h.reopen()
	if !slices.Equal(res.quarantined, []uint32{1}) {
		t.Fatalf("with its header restored: quarantined %v, want [1]", res.quarantined)
	}
	h.wantModel("open", res.index, n.live)
	wantCorrupt(t, h.recover(disklog.ModeRebuild), 1)
}

func TestUsageTableZeroedHeaderOfASegmentOpenedAfterTheManifestIsCorruption(t *testing.T) {
	h, _, lsns, m := usedLog(t)
	last := h.eventsOf(disklog.Durable)
	seg := last[len(last)-1].Segment
	if slices.ContainsFunc(m.Segments, func(u disklog.SegmentUsage) bool { return u.Segment == seg }) || len(lsns[seg]) < 2 {
		t.Fatalf("segment %d with LSNs %v is listed or holds one record", seg, lsns[seg])
	}
	h.disk.Put(h.geo.LBA(seg, 0), make([]byte, core.BlockSize))
	wantCorrupt(t, h.recover(disklog.ModeOpen), lsns[seg][0])
}

func TestUsageTableNamingASegmentPastTheDiskIsCorruption(t *testing.T) {
	h, _, _, _ := usedLog(t)
	pos := h.named()
	ev := disklog.Event{LSN: pos.LSN, Segment: pos.Segment, Block: pos.Block, LBA: h.geo.LBA(pos.Segment, pos.Block)}
	d := h.record(ev)
	i := slices.IndexFunc(d.Entries, func(e disklog.Entry) bool { return e.BlobKind == disklog.BlobManifest })
	lists, rest, err := disklog.DecodeManifestPiece(d.Payload(i))
	if err != nil || len(lists[len(lists)-1]) != 0 {
		t.Fatalf("newest manifest in pieces %v: %v", lists, err)
	}
	var body []byte
	if m, err := disklog.DecodeManifest(rest); err == nil {
		m.Segments[len(m.Segments)-1].Segment = h.geo.SegmentCount
		body = disklog.AppendManifest(nil, &m)
	} else if e, err := disklog.DecodeManifestEdit(rest); err == nil {
		e.Segments[len(e.Segments)-1].Segment = h.geo.SegmentCount
		body = disklog.AppendManifestEdit(nil, &e)
	} else {
		t.Fatal(err)
	}
	piece, err := disklog.AppendManifestPiece(nil, lists, body)
	if err != nil {
		t.Fatal(err)
	}
	data := slices.Clone(d.Data)
	copy(data[disklog.PayloadOffset(d.Entries, i):], piece)
	img, err := disklog.AppendRecord(nil, d.Record, d.Entries, data)
	if err != nil {
		t.Fatal(err)
	}
	h.disk.Put(ev.LBA, img)
	wantCorrupt(t, h.recover(disklog.ModeOpen), pos.LSN)
}
