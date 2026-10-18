package disklog

import (
	"encoding/binary"
	"math/bits"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// BreakEncoding makes every round corrupt its header image with f before it is checked
// and written, as a deliberately broken encoder would.
func BreakEncoding(l *Log, f func(head []byte)) { l.breakEncoding = f }

// AppendAnywhere lets rounds append to a segment whose opening round is still in flight, as
// a log without that rule would.
func AppendAnywhere(l *Log) { l.appendAnywhere = true }

// FixRecordCRC recomputes the header checksum of the record header at the start of b.
func FixRecordCRC(b []byte) {
	n := int(binary.LittleEndian.Uint16(b[recHeaderBlocks:])) * core.BlockSize
	fixCRC(b[:n], recCRC)
}

// FreeSegments is the number of segments the log may open.
func FreeSegments(l *Log) int {
	n := 0
	for _, w := range l.free {
		n += bits.OnesCount64(w)
	}
	return n
}
