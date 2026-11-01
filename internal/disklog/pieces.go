package disklog

import (
	"fmt"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// pieceSpan is a live blob entry of a record and the blocks [lo, hi) of its segment its bytes touch.
type pieceSpan struct {
	entry  uint16
	lo, hi uint32
}

// pieceRecord is a record holding live run or manifest pieces.
type pieceRecord struct {
	lsn     uint64
	segment uint32
	spans   []pieceSpan
}

// pieceSet is the run and manifest pieces something still needs, by record, so that a block two
// pieces share stays live until both are dead.
type pieceSet struct {
	recs []pieceRecord
	at   map[uint64]int
	in   []uint32
}

func newPieceSet(segments uint32) pieceSet {
	return pieceSet{at: map[uint64]int{}, in: make([]uint32, segments)}
}

// spanOf is the blocks of its segment that entry i's payload touches, in a record at block whose
// data area starts headerBlocks later and holds the payload at byte offset off.
func spanOf(block, headerBlocks uint32, off, length uint64) (uint32, uint32) {
	start := uint64(block + headerBlocks)
	return uint32(start + off/core.BlockSize), uint32(start + (off+length+core.BlockSize-1)/core.BlockSize)
}

func (p *pieceSet) add(ref BlobRef, lo, hi uint32) {
	if lo >= hi || p.holds(ref) {
		return
	}
	i, ok := p.at[ref.LSN]
	if !ok {
		i = len(p.recs)
		p.at[ref.LSN] = i
		var spans []pieceSpan
		if i < cap(p.recs) {
			spans = p.recs[:i+1][i].spans[:0]
		}
		p.recs = append(p.recs, pieceRecord{lsn: ref.LSN, segment: ref.Segment, spans: spans})
	}
	p.recs[i].spans = append(p.recs[i].spans, pieceSpan{entry: ref.Entry, lo: lo, hi: hi})
	p.in[ref.Segment]++
}

func (p *pieceSet) holds(ref BlobRef) bool {
	i, ok := p.at[ref.LSN]
	return ok && p.recs[i].segment == ref.Segment && slices.ContainsFunc(p.recs[i].spans, func(s pieceSpan) bool { return s.entry == ref.Entry })
}

// drop removes the piece and returns how many of its blocks no other live piece of its record touches.
func (p *pieceSet) drop(ref BlobRef) uint32 {
	i, ok := p.at[ref.LSN]
	if !ok || p.recs[i].segment != ref.Segment {
		return 0
	}
	rec := &p.recs[i]
	k := slices.IndexFunc(rec.spans, func(s pieceSpan) bool { return s.entry == ref.Entry })
	if k < 0 {
		return 0
	}
	sp := rec.spans[k]
	rec.spans = slices.Delete(rec.spans, k, k+1)
	p.in[ref.Segment]--
	var freed uint32
	for b := sp.lo; b < sp.hi; b++ {
		if !slices.ContainsFunc(rec.spans, func(s pieceSpan) bool { return s.lo <= b && b < s.hi }) {
			freed++
		}
	}
	if len(rec.spans) == 0 {
		last := len(p.recs) - 1
		p.recs[i], p.recs[last] = p.recs[last], p.recs[i]
		p.at[p.recs[i].lsn] = i
		p.recs = p.recs[:last]
		delete(p.at, ref.LSN)
	}
	return freed
}

// coverage sets live[s] to the blocks of segment s some live piece touches, for every segment holding one.
func (p *pieceSet) coverage(live []uint32) {
	for i := range p.recs {
		rec := &p.recs[i]
		var lo, hi uint32
		for _, s := range sortedSpans(rec.spans) {
			if s.lo >= hi {
				live[rec.segment] += hi - lo
				lo, hi = s.lo, s.hi
				continue
			}
			hi = max(hi, s.hi)
		}
		live[rec.segment] += hi - lo
	}
}

func sortedSpans(spans []pieceSpan) []pieceSpan {
	out := slices.Clone(spans)
	slices.SortFunc(out, func(a, b pieceSpan) int { return int(a.lo) - int(b.lo) })
	return out
}

type dyingPiece struct {
	ref BlobRef
	lsn uint64
}

// Usage is segment's row in the usage table as of the durable LSN, and the stream it was opened in.
func (l *Log) Usage(segment uint32) (SegmentUsage, Stream) {
	u := l.usage[segment]
	u.Segment = segment
	return u, l.streams[segment]
}

// slotBlocksOf is the blocks the slot writes among entries put in chunk slots, which the segment
// of their record counts live too: its header holds their checksums.
func slotBlocksOf(entries []Entry) uint32 {
	var n uint64
	for i := range entries {
		n += entries[i].slotBlocks()
	}
	return uint32(n)
}

// AddLive changes segment's live blocks by delta, as the owner of the index whose records it holds reports their counts changing past the blocks each record wrote; ErrMalformed for a segment that is neither used nor quarantined or a count that would go below zero.
func (l *Log) AddLive(segment uint32, delta int) error {
	if segment == 0 || segment >= l.sb.SegmentCount {
		return ErrMalformed
	}
	u := &l.usage[segment]
	n := int64(u.Live) + int64(delta)
	if u.State != SegmentUsed && u.State != SegmentQuarantined || n < 0 {
		return ErrMalformed
	}
	u.Live = uint32(n)
	l.changed[segment/64] |= 1 << (segment % 64)
	return nil
}

// PiecesDead reports run pieces nothing names any more, as a checkpoint's end or failure leaves them: once a segment header names the newest durable manifest, which no longer needs them, their blocks no other live piece shares leave the usage table's live count, and their segments may be cleaned. ErrMalformed for a piece the log does not hold.
func (l *Log) PiecesDead(refs []BlobRef) error {
	for _, ref := range refs {
		if !l.pieces.holds(ref) {
			return ErrMalformed
		}
	}
	for _, ref := range refs {
		if l.manifest.LSN <= l.namedLSN || l.cfg.Plants.PiecesDeadAtOnce {
			l.dropPiece(ref)
			continue
		}
		l.dying = append(l.dying, dyingPiece{ref: ref, lsn: l.manifest.LSN})
	}
	return nil
}

func (l *Log) dropPiece(ref BlobRef) { l.dropFrom(&l.pieces, ref) }

func (l *Log) dropFrom(set *pieceSet, ref BlobRef) {
	if n := set.drop(ref); n > 0 {
		u := &l.usage[ref.Segment]
		if u.Live < n {
			panic(fmt.Sprintf("disklog: segment %d counts %d live blocks, fewer than the %d its dead piece %+v held", ref.Segment, u.Live, n, ref))
		}
		u.Live -= n
		l.changed[ref.Segment/64] |= 1 << (ref.Segment % 64)
	}
}

// RaftBlobsDead reports Raft blobs, of kinds BlobRaftEntries and BlobRaftState, their shard no
// longer needs: their blocks no other live Raft blob shares leave the usage table's live count at
// once, since Cleanable keeps a segment holding records past the replay points. ErrMalformed for a
// blob the log does not hold.
func (l *Log) RaftBlobsDead(refs []BlobRef) error {
	for _, ref := range refs {
		if !l.raft.holds(ref) {
			return ErrMalformed
		}
	}
	for _, ref := range refs {
		l.dropFrom(&l.raft, ref)
	}
	return nil
}

func isRaftKind(e *Entry) bool {
	return e.Kind == EntryBlob && (e.BlobKind == BlobRaftEntries || e.BlobKind == BlobRaftState)
}

// HoldsPiece reports whether segment holds a run or manifest piece that a manifest recovery may load, a running checkpoint or a running merge still needs; after a rebuild, which reads no run, every metadata segment does until a segment header names a manifest written since.
func (l *Log) HoldsPiece(segment uint32) bool {
	if segment >= uint32(len(l.pieces.in)) {
		return false
	}
	return l.pieces.in[segment] > 0 || l.pinnedLSN != 0 && l.streams[segment] == Meta
}

// registerBlobs adds the run and manifest pieces of a durable round to the live pieces: a run
// piece until its owner reports it dead, a manifest piece of the running checkpoint until a
// segment header names a manifest that no longer lists it, and drops at once a manifest piece of
// a checkpoint that failed meanwhile. Its Raft blobs stay live until their shard reports them dead.
func (l *Log) registerBlobs(r *round) {
	for i := range r.entries {
		e := &r.entries[i]
		raft := isRaftKind(e)
		if !raft && !isCheckpointKind(e) {
			continue
		}
		ref := BlobRef{Segment: r.segment, Block: r.block, LSN: r.lsn, Entry: uint16(i)}
		lo, hi := spanOf(r.block, r.headerBlocks, r.payloads[i], e.Length)
		if raft {
			l.raft.add(ref, lo, hi)
			continue
		}
		l.pieces.add(ref, lo, hi)
		if isManifestKind(e) && (l.cp == nil || r.tickets[i] != l.cp.piece) {
			l.dropPiece(ref)
		}
	}
}

// namedDurable notes a segment header naming named durable: the manifests before it can no
// longer be loaded, so the pieces only they listed and the run pieces reported dead before it
// was durable leave the live pieces.
func (l *Log) namedDurable(named ManifestPos) {
	l.namedLSN = max(l.namedLSN, named.LSN)
	l.trimNamed()
	if l.pinnedLSN != 0 && l.namedLSN > l.pinnedLSN {
		l.pinnedLSN = 0
		l.recount(nil, false)
	}
	l.dropChains(named.LSN)
	k := 0
	for _, d := range l.dying {
		if d.lsn <= l.namedLSN {
			l.dropPiece(d.ref)
			continue
		}
		l.dying[k] = d
		k++
	}
	clear(l.dying[k:])
	l.dying = l.dying[:k]
}

// dropChains drops the live manifests before lsn, which recovery can no longer load, and the
// pieces only they listed.
func (l *Log) dropChains(lsn uint64) {
	i := slices.IndexFunc(l.live, func(c liveChain) bool { return c.pos.LSN >= lsn })
	if i <= 0 {
		return
	}
	dropped := slices.Clone(l.live[:i])
	l.live = slices.Delete(l.live, 0, i)
	for _, c := range dropped {
		for _, e := range c.entries {
			for _, ref := range e.pieces {
				if !l.listsPiece(ref) {
					l.dropPiece(ref)
				}
			}
		}
	}
}

// listsPiece reports whether a manifest recovery may still load, or the running checkpoint, lists ref.
func (l *Log) listsPiece(ref BlobRef) bool {
	for _, c := range l.live {
		if slices.ContainsFunc(c.entries, func(e chainEntry) bool { return slices.Contains(e.pieces, ref) }) {
			return true
		}
	}
	return l.cp != nil && slices.Contains(l.cp.earlier, ref)
}

// Recount sets every used segment's live blocks from what still needs them, as the owner does at
// open once its index holds every record: a segment of the metadata stream counts the blocks of
// its live pieces, any other the record counts chunk names for it, nothing past its end, and the
// blocks of its live Raft blobs. The manifest's live counts are therefore only advice, and steps
// written but never named, or pieces reported dead before a crash, count nothing.
func (l *Log) Recount(chunk []uint32) { l.recount(chunk, true) }

func (l *Log) recount(chunk []uint32, all bool) {
	meta, raft := make([]uint32, l.sb.SegmentCount), make([]uint32, l.sb.SegmentCount)
	l.pieces.coverage(meta)
	l.raft.coverage(raft)
	for s := uint32(1); s < l.sb.SegmentCount; s++ {
		u := &l.usage[s]
		if u.State == SegmentFree || u.State == SegmentSlot {
			continue
		}
		live := meta[s]
		if l.streams[s] == Meta && l.pinnedLSN != 0 {
			continue
		}
		if l.streams[s] != Meta {
			if !all {
				continue
			}
			live = raft[s]
			if s < uint32(len(chunk)) {
				live += chunk[s]
			}
		}
		if u.Live != live {
			u.Live = live
			l.changed[s/64] |= 1 << (s % 64)
		}
	}
}
