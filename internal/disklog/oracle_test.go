package disklog_test

import (
	"fmt"
	"maps"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// diskOracle is the crash oracle over the disk's one index, kept as the plain model.
type diskOracle = sim.Oracle[disklog.Event, model]

// oracleFeed is a crash oracle over this log's records, whatever model it keeps.
type oracleFeed interface {
	Written(lsn uint64, ev disklog.Event) error
	Acked(lsn uint64) error
}

// watch starts a crash oracle at the log's durable LSN, whose model there is m: from now on the
// harness hands it every round the log writes and every record it reports durable, and each
// recovery's result carries what it expects.
func (h *harness) watch(m model) *diskOracle {
	if h.log.InFlight() != 0 {
		h.t.Fatalf("watch with %d rounds in flight", h.log.InFlight())
	}
	o := sim.NewOracle[disklog.Event](h.log.DurableLSN(), m.Clone())
	h.oracle = o
	return o
}

// noteRound hands the oracle the record a round's write a carries, decoded from the bytes the
// write sends, as a Durable event for it would report it. Writes to segment 0 are the superblock's.
func (h *harness) noteRound(a core.Action) {
	h.t.Helper()
	seg, block := uint32(uint64(a.LBA)/uint64(h.geo.SegmentBlocks)), uint32(uint64(a.LBA)%uint64(h.geo.SegmentBlocks))
	if seg == 0 {
		return
	}
	var img []byte
	for _, v := range a.Vec {
		img = append(img, h.pool.Bytes(v.Buf)[:v.Len]...)
	}
	lba := a.LBA
	var seq uint64
	if block == 0 {
		sh, err := disklog.DecodeSegmentHeader(img)
		if err != nil {
			h.t.Fatalf("a round opens segment %d with a header that does not decode: %v", seg, err)
		}
		seq, img, block, lba = sh.Seq, img[core.BlockSize:], 1, lba+1
	} else {
		seq = h.segmentSeq(seg)
	}
	hdr, err := disklog.DecodeHeader(img, disklog.Place{LogID: h.logID, Seq: seq, Offset: block}, h.geo.SegmentBlocks-block, nil)
	if err != nil {
		h.t.Fatalf("round written at LBA %d: %v", lba, err)
	}
	ev := disklog.Event{Kind: disklog.Durable, LSN: hdr.Record.LSN, Segment: seg, Block: block, LBA: lba, HeaderBlocks: hdr.Layout.HeaderBlocks, Entries: hdr.Entries}
	for i := range ev.Entries {
		ev.Payloads = append(ev.Payloads, disklog.PayloadOffset(ev.Entries, i))
	}
	if err := h.oracle.Written(ev.LSN, ev); err != nil {
		h.t.Fatalf("oracle: %v", err)
	}
}

// oracleCheck reports how a recovery fails the crash oracle: an LSN outside its window, or an
// index other than its model at that LSN.
func oracleCheck(res *recovered) error {
	if res.oracleErr != nil {
		return res.oracleErr
	}
	if !maps.Equal(res.index, res.expect) {
		return fmt.Errorf("index at LSN %d differs from the oracle's: %s", res.durable, modelDiff(res.index, res.expect))
	}
	return nil
}

// modelDiff names the first block where got and want differ, and how many do.
func modelDiff(got, want model) string {
	all := want.Clone()
	maps.Copy(all, got)
	first, n := "", 0
	for _, k := range all.keys() {
		g, inGot := got[k]
		w, inWant := want[k]
		if inGot == inWant && g == w {
			continue
		}
		n++
		if first != "" {
			continue
		}
		switch {
		case !inGot:
			first = fmt.Sprintf("chunk %d block %d missing, want v%d at %d", k.chunk.Index, k.block, w.version, w.lba)
		case !inWant:
			first = fmt.Sprintf("chunk %d block %d v%d at %d, want none", k.chunk.Index, k.block, g.version, g.lba)
		default:
			first = fmt.Sprintf("chunk %d block %d v%d pool %d at %d, want v%d pool %d at %d", k.chunk.Index, k.block, g.version, g.pool, g.lba, w.version, w.pool, w.lba)
		}
	}
	if n > 1 {
		first += fmt.Sprintf(" (%d blocks differ)", n)
	}
	return first
}
