// Package tcp is the network driver: one stream per pair of event loops, carrying framed
// messages, checksummed and batched per connection.
package tcp

import (
	"encoding/binary"
	"errors"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// A frame is a 12-byte header, then the inline bytes, then the data. The header is the format
// version, the kind, the inline length (2 bytes), the data length (4 bytes) and a CRC32C over
// the header's first 8 bytes, the inline bytes and the data, little-endian.
const (
	version   = 1
	headerLen = 12
	kindHello = 1
	kindMsg   = 2
	// helloLen is a hello's inline bytes: the sender's node and loop and its incarnation.
	helloLen = 16
	// MaxData is the largest data buffer a frame carries.
	MaxData = 16 << 20
)

var (
	// ErrFrame reports a frame of an unknown version or kind or over a limit.
	ErrFrame = errors.New("tcp: bad frame")
	// ErrChecksum reports a frame whose CRC32C does not match.
	ErrChecksum = errors.New("tcp: frame checksum mismatch")
)

// frameLen is the bytes a frame with inline and data bytes takes.
func frameLen(inline, data int) int { return headerLen + inline + data }

// putFrame writes a frame of kind carrying inline and data at the start of dst, which holds
// frameLen of them, and returns its length.
func putFrame(dst []byte, kind byte, inline, data []byte) int {
	dst[0], dst[1] = version, kind
	binary.LittleEndian.PutUint16(dst[2:], uint16(len(inline)))
	binary.LittleEndian.PutUint32(dst[4:], uint32(len(data)))
	n := headerLen + copy(dst[headerLen:], inline)
	n += copy(dst[n:], data)
	crc := core.CRC32CUpdate(core.CRC32C(dst[:8]), dst[headerLen:n])
	core.PutCRC32C(dst[8:], crc)
	return n
}

func putHello(dst []byte, self core.NodeID, inc uint64) []byte {
	binary.LittleEndian.PutUint32(dst, self.Node)
	binary.LittleEndian.PutUint32(dst[4:], self.Loop)
	binary.LittleEndian.PutUint64(dst[8:], inc)
	return dst[:helloLen]
}

func getHello(b []byte) (core.NodeID, uint64) {
	return core.NodeID{Node: binary.LittleEndian.Uint32(b), Loop: binary.LittleEndian.Uint32(b[4:])}, binary.LittleEndian.Uint64(b[8:])
}

// frame is a decoded frame: its kind, its inline bytes, which alias the decoder's until the next
// call, and its data buffer, NoBuf for none, which the caller takes over.
type frame struct {
	kind   byte
	inline []byte
	data   core.IOBuf
}

// decoder reassembles frames from a stream. Each read fills the rest of the current frame's data
// straight into the frame's own pool buffer and reads ahead into a scratch buffer, from which the
// frames that follow are decoded, their headers and inline bytes copied into head and their data
// into their pool buffers. Read-ahead is aheadBytes after a small frame and copyMax after one
// whose data is larger, so a stream of large frames copies at most copyMax of each and reads the
// rest straight in. A partial frame's data buffer is the decoder's until the frame completes or
// reset frees it.
type decoder struct {
	head [headerLen + core.MaxInlineMsg]byte
	n    int
	// want is the header and inline bytes once the header is known, 0 before.
	want  int
	data  core.IOBuf
	dlen  int
	got   int
	large bool
	out   bool
	iov   [2][]byte
}

const (
	// aheadBytes is the most one read takes past the current frame's data: enough for a few hundred
	// small frames (heartbeats, lease grants, short appends) in one syscall.
	aheadBytes = 64 << 10
	// copyMax is the most data of a large frame copied out of the read-ahead: a frame with more
	// data is large, and after one the read-ahead shrinks to this.
	copyMax = 4 << 10
)

// space is where the next read goes, in order, and the bytes it holds: the rest of the current
// frame's data, if its inline bytes are in, then the read-ahead, a prefix of scratch.
func (d *decoder) space(pool core.BufPool, scratch []byte) ([][]byte, int) {
	v, size := d.iov[:0], 0
	if d.want > 0 && d.n == d.want && d.got < d.dlen {
		v = append(v, pool.Bytes(d.data)[d.got:d.dlen])
		size = d.dlen - d.got
	}
	ahead := aheadBytes
	if d.large {
		ahead = copyMax
	}
	ahead = min(ahead, len(scratch))
	return append(v, scratch[:ahead]), size + ahead
}

// read takes k bytes just read into space and returns how many of them went to the read-ahead.
func (d *decoder) read(k int) int {
	if d.want > 0 && d.n == d.want {
		a := min(k, d.dlen-d.got)
		d.got += a
		return k - a
	}
	return k
}

// take completes a frame into f from what was read straight in and from the read-ahead in, whose
// bytes it consumes, and returns true, or false when the frame needs more, with in empty. Its
// inline bytes alias the decoder's until the next call. A bad frame is ErrFrame, ErrChecksum or
// the pool's error, after which the decoder holds nothing and the stream is broken.
func (d *decoder) take(pool core.BufPool, in *[]byte, f *frame) (bool, error) {
	if d.out {
		d.n, d.want, d.dlen, d.got, d.out = 0, 0, 0, 0, false
	}
	b := *in
	if d.want == 0 {
		k := copy(d.head[d.n:headerLen], b)
		d.n += k
		b = b[k:]
		if d.n < headerLen {
			*in = b
			return false, nil
		}
		if err := d.header(pool); err != nil {
			d.reset(pool)
			return false, err
		}
	}
	k := copy(d.head[d.n:d.want], b)
	d.n += k
	b = b[k:]
	if d.n == d.want && d.got < d.dlen {
		k = copy(pool.Bytes(d.data)[d.got:d.dlen], b)
		d.got += k
		b = b[k:]
	}
	*in = b
	if d.n < d.want || d.got < d.dlen {
		return false, nil
	}
	crc := core.CRC32CUpdate(core.CRC32C(d.head[:8]), d.head[headerLen:d.want])
	if d.dlen > 0 {
		crc = core.CRC32CUpdate(crc, pool.Bytes(d.data)[:d.dlen])
	}
	if crc != core.GetCRC32C(d.head[8:]) {
		d.reset(pool)
		return false, ErrChecksum
	}
	*f = frame{kind: d.head[1], inline: d.head[headerLen:d.want], data: d.data}
	d.data, d.out = core.NoBuf, true
	return true, nil
}

func (d *decoder) header(pool core.BufPool) error {
	kind := d.head[1]
	inline := int(binary.LittleEndian.Uint16(d.head[2:]))
	dlen := binary.LittleEndian.Uint32(d.head[4:])
	switch {
	case d.head[0] != version, inline > core.MaxInlineMsg, dlen > MaxData,
		kind == kindHello && (inline != helloLen || dlen != 0), kind != kindHello && kind != kindMsg:
		return ErrFrame
	}
	if dlen > 0 {
		b, err := pool.Alloc(int(dlen))
		if err != nil {
			return err
		}
		d.data = b
	}
	d.want, d.dlen, d.large = headerLen+inline, int(dlen), dlen > copyMax
	return nil
}

// reset discards a partial frame, freeing its data buffer.
func (d *decoder) reset(pool core.BufPool) {
	if d.data != core.NoBuf {
		pool.Free(d.data)
	}
	*d = decoder{}
}
