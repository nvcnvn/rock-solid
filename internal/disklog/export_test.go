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
