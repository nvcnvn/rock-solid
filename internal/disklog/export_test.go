package disklog

import (
	"encoding/binary"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// BreakEncoding makes every round corrupt its header image with f before it is checked
// and written, as a deliberately broken encoder would.
func BreakEncoding(l *Log, f func(head []byte)) { l.breakEncoding = f }

// FixRecordCRC recomputes the header checksum of the record header at the start of b.
func FixRecordCRC(b []byte) {
	n := int(binary.LittleEndian.Uint16(b[recHeaderBlocks:])) * core.BlockSize
	fixCRC(b[:n], recCRC)
}
