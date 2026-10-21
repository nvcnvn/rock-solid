package disklog

import (
	"encoding/binary"
	"math/bits"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// BreakEncoding makes every round corrupt its header image with f before it is checked
// and written, as a deliberately broken encoder would.
func BreakEncoding(l *Log, f func(head []byte)) {
	l.breakRound = func(head []byte, _ *round) { f(head) }
}

// MisreportPayloads makes every round report its last entry's payload a byte late.
func MisreportPayloads(l *Log) {
	l.breakRound = func(_ []byte, r *round) { r.payloads[len(r.payloads)-1]++ }
}

// PadWrites makes every round write one block more than its record.
func PadWrites(l *Log) {
	l.breakRound = func(_ []byte, r *round) { r.vec = append(r.vec, core.IOVec{Buf: r.head, Len: core.BlockSize}) }
}

// SwapTickets makes every round of two or more items report its first two items' tickets swapped.
func SwapTickets(l *Log) {
	l.breakRound = func(_ []byte, r *round) {
		if len(r.tickets) >= 2 {
			r.tickets[0], r.tickets[1] = r.tickets[1], r.tickets[0]
		}
	}
}

// LoseNewest plants the bug of dropping the newest item queued on stream without an event: its
// ticket was handed out and its buffer is freed. It reports whether an item was queued.
func LoseNewest(l *Log, ctx *core.Context, stream Stream) bool {
	q := &l.lanes[laneOf(stream, 0)].q
	if q.len() == 0 {
		return false
	}
	if it := q.at(q.len() - 1); it.buf != core.NoBuf {
		ctx.Pool.Free(it.buf)
	}
	q.items = q.items[:len(q.items)-1]
	return true
}

// LoseOldestRound plants the bug of dropping the oldest round in flight without an event. It
// reports whether a round was in flight.
func LoseOldestRound(l *Log) bool {
	if l.count == 0 {
		return false
	}
	l.first = (l.first + 1) % len(l.rounds)
	l.count--
	return true
}

// QueuedTickets is the tickets of the items waiting for a round, as events would report them.
func QueuedTickets(l *Log) []Ticket {
	var ts []Ticket
	for i := range l.lanes {
		q := &l.lanes[i].q
		for j := range q.len() {
			ts = append(ts, visible(q.at(j).ticket, &q.at(j).entry))
		}
	}
	return ts
}

// InFlightTickets is the tickets of the rounds in flight that a disk error has not abandoned.
func InFlightTickets(l *Log) []Ticket {
	var ts []Ticket
	for i := range l.count {
		if r := l.round(i); !r.abandoned {
			for j, t := range r.tickets {
				ts = append(ts, visible(t, &r.entries[j]))
			}
		}
	}
	return ts
}

// RoundTickets is the tickets of the round in flight at lsn, as its Durable event would report them.
func RoundTickets(l *Log, lsn uint64) []Ticket {
	for i := range l.count {
		if r := l.round(i); r.lsn == lsn {
			ts := make([]Ticket, len(r.tickets))
			for j, t := range r.tickets {
				ts[j] = visible(t, &r.entries[j])
			}
			return ts
		}
	}
	return nil
}

// AppendAnywhere lets rounds append to a segment whose opening round is still in flight, as
// a log without that rule would.
func AppendAnywhere(l *Log) { l.appendAnywhere = true }

// FixRecordCRC recomputes the header checksum of the record header at the start of b.
func FixRecordCRC(b []byte) {
	n := int(binary.LittleEndian.Uint16(b[recHeaderBlocks:])) * core.BlockSize
	fixCRC(b[:n], recCRC)
}

// FixSegmentCRC recomputes the checksum of the segment header at the start of b.
func FixSegmentCRC(b []byte) { fixCRC(b[:core.BlockSize], sgCRC) }

// FreeSegments is the number of segments the log may open.
func FreeSegments(l *Log) int {
	n := 0
	for _, w := range l.free {
		n += bits.OnesCount64(w)
	}
	return n
}

// SegmentUsageOf is the log's segment usage table as of its durable LSN.
func SegmentUsageOf(l *Log) []SegmentUsage { return l.usageRows() }

// FreeSegment stands in for the cleaner freeing segment s: its row turns free for the next
// checkpoint's edit, and reuse lets a round open it again at once.
func FreeSegment(l *Log, s uint32, reuse bool) {
	l.usage[s] = SegmentUsage{}
	l.changed[s/64] |= 1 << (s % 64)
	if reuse {
		l.free[s/64] |= 1 << (s % 64)
	}
}

// NextSeqOf is the NextSeq a checkpoint begun now would record.
func NextSeqOf(l *Log) uint64 { return l.durableSeq }

// TakeNextSeqAtBegin plants the bug of recording the log's next sequence number when a
// checkpoint begins as its NextSeq, which counts segments rounds in flight have opened.
func TakeNextSeqAtBegin(l *Log) { l.seqAtBegin = true }

// StartRounds starts the rounds a freed segment makes room for, as the cleaner will once it frees one.
func StartRounds(l *Log, ctx *core.Context, out *Output) { l.startRounds(ctx, out) }

// HeldBytes is the bytes of records after the newest manifest that count toward the hold behind a checkpoint.
func HeldBytes(l *Log) uint64 { return l.heldBytes }

// ReplayPerStream plants the bug of reading each stream, not each lane, from the last of its
// segments whose first record is at or before the first to replay.
func ReplayPerStream(r *Recovery) { r.perStream = true }

// SkipMergeLanes plants the bug of leaving merge lanes out of replay.
func SkipMergeLanes(r *Recovery) { r.skipMerges = true }

// StopEarly plants the bug of reporting the LSN before the last one replay delivered.
func StopEarly(r *Recovery) { r.stopEarly = true }

// ReplayPastGaps plants the bug of delivering a record past a missing LSN instead of ending the log there.
func ReplayPastGaps(r *Recovery) { r.pastGaps = true }
