package disklog

import (
	"errors"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// ErrNotCleanable reports a segment the cleaner may not free: open, not used, still counting live
// blocks, holding a piece something needs, or a record past the replay points of a manifest
// recovery may load.
var ErrNotCleanable = errors.New("segment not cleanable")

// VictimsPerPass is how many victims a cleaner pass takes, which the reserve holds room for.
const VictimsPerPass = victimsPerPass

// Age is how many data blocks of ordinary records became durable since the newest record of
// segment did, at most a disk's worth: a segment from before the log was opened counts as old
// as that cap.
func (l *Log) Age(segment uint32) uint64 {
	return min(l.written-l.writtenAt[segment], l.sb.DiskBlocks())
}

// noteWritten counts the data blocks of an ordinary round as written, and dates the round's
// segment by its youngest block: a copied block keeps the date of the segment it was copied
// from, as the original log-structured file system keeps a moved block's write time, so copies
// of old data look old.
func (l *Log) noteWritten(r *round) {
	if r.ordinary > 0 {
		l.written += r.bytes/core.BlockSize - uint64(r.headerBlocks)
	}
	t := l.written
	if len(r.entries) > 0 && r.entries[0].Kind == EntryCopy {
		t = 0
		for i := range r.entries {
			t = max(t, l.writtenAt[uint64(r.entries[i].Source)/uint64(l.sb.SegmentBlocks)])
		}
	}
	if r.opened != 0 {
		l.writtenAt[r.segment] = t
		return
	}
	l.writtenAt[r.segment] = max(l.writtenAt[r.segment], t)
}

// CopiesStalled reports cleaner copies waiting with no segment free at all, the reserve included,
// so that a pass cannot finish until something else frees one.
func (l *Log) CopiesStalled() bool {
	if l.lanes[laneClean].q.len() == 0 {
		return false
	}
	_, p := l.place(laneClean)
	return p == placeNoRoom
}

// NamedLSN is the LSN of the newest manifest a durable segment header names, the one recovery would load.
func (l *Log) NamedLSN() uint64 { return l.namedLSN }

// FreeOutsideReserve is how many segments are free beyond the reserve, or below zero by how many the reserve lacks.
func (l *Log) FreeOutsideReserve() int { return int(l.freeCount()) - int(l.reserve) }

// MaxCopyBlocks is the most blocks one copy item holds.
func (l *Log) MaxCopyBlocks() uint16 {
	return uint16(min(maxDataBlocks(l.maxItemRec, copyEntrySize), core.BlocksPerChunk))
}

// CopyCost is what copying blocks live blocks costs the log in blocks at most: records of
// one-block copy entries in records of the largest size, header blocks rounded up per record, the
// header of one more record, since copies rolling into a new segment split their records between
// two, and the tail they leave behind in the segment they roll out of, which a record never spans:
// up to a block short of the largest copy among them and its header. In 1 MiB records, 253 data
// blocks and 3 header blocks each, and a copy of up to 254 blocks under 1.
func (l *Log) CopyCost(blocks uint64) uint64 {
	if blocks == 0 {
		return 0
	}
	per, header := copiesPerRecord(l.maxWrite)
	largest := min(blocks, uint64(l.MaxCopyBlocks()))
	tail := largest + (preludeSize+copyEntrySize+largest*core.CRC32CSize+core.BlockSize-1)/core.BlockSize - 1
	return blocks + header*((blocks+per-1)/per+1) + tail
}

// RewriteCost is what writing pieces again costs the log in blocks at most: the records holding
// them as they lie now, header blocks included, and the tail that the largest leaves behind in a
// segment too short for it, since a record never spans two segments. A piece the log does not
// hold counts as a record of the largest size.
func (l *Log) RewriteCost(pieces []BlobRef) uint64 {
	var cost, largest uint64
	for k, ref := range pieces {
		if slices.ContainsFunc(pieces[:k], func(p BlobRef) bool { return p.LSN == ref.LSN }) {
			continue
		}
		blocks := uint64(l.maxWrite)
		if i, ok := l.pieces.at[ref.LSN]; ok && l.pieces.recs[i].segment == ref.Segment {
			var end uint32
			for _, sp := range l.pieces.recs[i].spans {
				end = max(end, sp.hi)
			}
			blocks = uint64(end - ref.Block)
		}
		cost += blocks
		largest = max(largest, blocks)
	}
	if largest > 0 {
		cost += largest - 1
	}
	return cost
}

func copiesPerRecord(recordBlocks uint32) (per, header uint64) {
	for d := uint64(recordBlocks); d > 0; d-- {
		h := (preludeSize + d*(copyEntrySize+core.CRC32CSize) + core.BlockSize - 1) / core.BlockSize
		if d+h <= uint64(recordBlocks) {
			return d, h
		}
	}
	return 1, 1
}

// Cleanable reports whether the cleaner may take segment: used, not the open segment of any lane,
// and holding no record past the replay points of the oldest manifest recovery may still load,
// which replay reads before it applies anything. Before any manifest is named, nothing is.
func (l *Log) Cleanable(segment uint32) bool {
	if segment == 0 || segment >= l.sb.SegmentCount || l.usage[segment].State != SegmentUsed || len(l.live) == 0 {
		return false
	}
	for i := range l.lanes {
		if l.lanes[i].asm.segment == segment {
			return false
		}
	}
	return l.usage[segment].Newest <= l.live[0].replay
}

// Free frees segment once the cleaner has moved everything live out of it: it must be cleanable,
// count no live block and hold no piece. Its row turns free for the next checkpoint's edit, and
// it may be opened again at once, by the rounds waiting for room that it starts; its header stays
// until it is.
func (l *Log) Free(ctx *core.Context, segment uint32, out *Output) error {
	if !l.Cleanable(segment) || l.usage[segment].Live != 0 || l.HoldsPiece(segment) || l.HoldsManifestPiece(segment) {
		return ErrNotCleanable
	}
	l.usage[segment] = SegmentUsage{}
	l.changed[segment/64] |= 1 << (segment % 64)
	l.free[segment/64] |= 1 << (segment % 64)
	l.noteFreed(segment)
	l.now = ctx.Now
	l.startRounds(ctx, out)
	return nil
}

// Quarantine marks a used segment quarantined, as the cleaner does with a victim holding a block
// whose checksum fails: never a victim again, never reused, and reported for repair.
func (l *Log) Quarantine(segment uint32) error {
	if segment == 0 || segment >= l.sb.SegmentCount || l.usage[segment].State != SegmentUsed {
		return ErrMalformed
	}
	l.usage[segment].State = SegmentQuarantined
	l.changed[segment/64] |= 1 << (segment % 64)
	return nil
}

// replayFloor is the oldest LSN a manifest of the indexes replays from: its oldest replay LSN, or
// its base without one.
func replayFloor(m *Manifest) uint64 {
	if len(m.Indexes) == 0 {
		return m.BaseLSN
	}
	lsn := m.Indexes[0].ReplayLSN
	for _, ix := range m.Indexes[1:] {
		lsn = min(lsn, ix.ReplayLSN)
	}
	return lsn
}
