package chunkstore

import (
	"encoding/binary"
	"math/bits"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// blockWords is the 64-bit words of a bitmap over a chunk's blocks.
const blockWords = core.BlocksPerChunk / 64

// blockBits is a bitmap over a chunk's blocks.
type blockBits [blockWords]uint64

func (b *blockBits) has(i uint16) bool { return b[i/64]&(1<<(i%64)) != 0 }
func (b *blockBits) set(i uint16)      { b[i/64] |= 1 << (i % 64) }

// setRange sets blocks [first, last].
func (b *blockBits) setRange(first, last uint16) {
	for i := first; i <= last; i++ {
		b.set(i)
	}
}

func (b *blockBits) count() int {
	n := 0
	for _, w := range b {
		n += bits.OnesCount64(w)
	}
	return n
}

// rank is how many blocks below i are set.
func (b *blockBits) rank(i uint16) int {
	n := 0
	for w := range int(i / 64) {
		n += bits.OnesCount64(b[w])
	}
	return n + bits.OnesCount64(b[i/64]&(1<<(i%64)-1))
}

// runs calls f for each run of set blocks [first, first+n).
func (b *blockBits) runs(f func(first, n uint16)) {
	for i := 0; i < core.BlocksPerChunk; {
		if !b.has(uint16(i)) {
			i++
			continue
		}
		j := i
		for j < core.BlocksPerChunk && b.has(uint16(j)) {
			j++
		}
		f(uint16(i), uint16(j-i))
		i = j
	}
}

// blockMap opens a recovery buffer: the chunk's blocks the buffer carries, whose bytes follow its
// head in block order, and those that are holes. A recovery write writes the one and punches the
// other, leaving every block in neither alone. The head goes on with a u32 count of resend records
// and the records, zero to the end of its last block; a zero record is one the primary left out.
type blockMap struct {
	data, holes blockBits
}

func (m *blockMap) empty() bool { return m.data == blockBits{} && m.holes == blockBits{} }

// strip leaves out the blocks set in cover.
func (m *blockMap) strip(cover *blockBits) {
	for w := range blockWords {
		m.data[w] &^= cover[w]
		m.holes[w] &^= cover[w]
	}
}

// put writes m into the first block of buf, zero past it.
func (m *blockMap) put(buf []byte) {
	clear(buf[:core.BlockSize])
	m.putMaps(buf)
}

func (m *blockMap) putMaps(buf []byte) {
	for w := range blockWords {
		binary.LittleEndian.PutUint64(buf[8*w:], m.data[w])
		binary.LittleEndian.PutUint64(buf[8*(blockWords+w):], m.holes[w])
	}
}

// decodeBlockMap reads a recovery buffer's map: no block both written and punched, the head zero
// past its records, and exactly the blocks it writes after it.
func decodeBlockMap(buf []byte, m *blockMap) error {
	if len(buf) < core.BlockSize || len(buf)%core.BlockSize != 0 {
		return ErrMalformed
	}
	for w := range blockWords {
		m.data[w] = binary.LittleEndian.Uint64(buf[8*w:])
		m.holes[w] = binary.LittleEndian.Uint64(buf[8*(blockWords+w):])
		if m.data[w]&m.holes[w] != 0 {
			return ErrMalformed
		}
	}
	n := recoveryRecords(buf)
	if n > maxResendRecords {
		return ErrMalformed
	}
	h := recoveryHead(n) * core.BlockSize
	if len(buf) < h || !allZero(buf[recordsAt+n*resendRecordSize:h]) || len(buf) != h+m.data.count()*core.BlockSize {
		return ErrMalformed
	}
	return nil
}

// recordsAt is where a recovery buffer's resend records start, after their count, and
// maxResendRecords bounds the count, so the head takes at most maxRecoveryHead blocks.
// ponytail: a source holding more of the chunk's records than that fails the read, and the chunk
// stays listed; send the records in more than one message if a PG's sessions grow past them.
const (
	recordsAt        = 16*blockWords + 4
	maxRecoveryHead  = 16
	maxResendRecords = (maxRecoveryHead*core.BlockSize - recordsAt) / resendRecordSize
)

// recoveryHead is the blocks a recovery buffer's map and its n resend records take.
func recoveryHead(n int) int {
	return (recordsAt + n*resendRecordSize + core.BlockSize - 1) / core.BlockSize
}

// recoveryRecords is the count of resend records recovery buffer buf gives.
func recoveryRecords(buf []byte) int { return int(binary.LittleEndian.Uint32(buf[16*blockWords:])) }

// resendRecord is record i of recovery buffer buf.
func resendRecord(buf []byte, i int) []byte {
	return buf[recordsAt+i*resendRecordSize:][:resendRecordSize]
}

// keepResendRecords zeroes the records of buf naming a version above kept, which the target does
// not take from the source.
func keepResendRecords(buf []byte, kept uint64) {
	for i := range recoveryRecords(buf) {
		if r := resendRecord(buf, i); binary.LittleEndian.Uint64(r[16:]) != 0 && binary.LittleEndian.Uint64(r[24:]) > kept {
			clear(r)
		}
	}
}

// cover is the blocks of a chunk that the gated writes and punches of an interval have covered so
// far, which a recovery write of the chunk in that interval leaves out.
type cover struct {
	interval uint32
	blocks   blockBits
	// frozen stops the cover growing, as the coverAtRead plant has it at the read.
	frozen bool
}

// Cover starts tracking the blocks of chunk c that the gated writes and punches of interval
// ordered from now on cover, which the chunk's recovery write in that interval leaves out. A
// primary starts it, for each chunk its peering left a member lacking, before it orders any write
// of the interval.
func (s *Store) Cover(c core.ChunkID, interval uint32) {
	if s.covers == nil {
		s.covers = map[core.ChunkID]cover{}
	}
	s.covers[c] = cover{interval: interval}
}

// Uncover stops tracking chunk c's covered blocks.
func (s *Store) Uncover(c core.ChunkID) { delete(s.covers, c) }

// freezeCover stops chunk c's cover growing, for the coverAtRead plant.
func (s *Store) freezeCover(c core.ChunkID) {
	if cv, ok := s.covers[c]; ok {
		cv.frozen = true
		s.covers[c] = cv
	}
}

// covered adds the blocks of gated op o to its chunk's cover, if tracked in the op's interval.
func (s *Store) covered(c core.ChunkID, o *op) {
	cv, ok := s.covers[c]
	if !ok || cv.interval != uint32(o.version>>32) || cv.frozen {
		return
	}
	first, last := spanOf(o.req.Off, o.req.Len)
	cv.blocks.setRange(first, last)
	s.covers[c] = cv
}

// recoveryBlocks is the blocks a recovery write's buffer carries.
func recoveryBlocks(pool core.BufPool, buf core.IOBuf) int {
	if buf == core.NoBuf {
		return 0
	}
	b := pool.Bytes(buf)
	return len(b)/core.BlockSize - recoveryHead(recoveryRecords(b))
}

// recoverItems builds a recovery write's items: unless it relays, a write of each run of the blocks
// its buffer carries and a punch of each run of holes, and then the entry that ends it. At the
// primary the blocks a gated write of the interval ordered before it covers are left out first,
// and the buffer that remains, for the members, is kept in o.buf: none when nothing is left.
func (s *Store) recoverItems(ctx *core.Context, o *op) error {
	pool, c := s.cfg.Pool, s.chunks[o.chunk].id
	var m blockMap
	var src []byte
	if o.req.Buf != core.NoBuf {
		src = ctx.Pool.Bytes(o.req.Buf)
		if err := decodeBlockMap(src, &m); err != nil {
			return err
		}
	}
	orig, head := m.data, 1
	if src != nil {
		head = recoveryHead(recoveryRecords(src))
	}
	if o.req.Gated {
		cv, ok := s.covers[c]
		if !ok || cv.interval != o.req.Interval {
			panic("chunkstore: a recovery write of a chunk whose covered blocks are not tracked")
		}
		delete(s.covers, c)
		m.strip(&cv.blocks)
	}
	at := func(b uint16) []byte { return src[(head+orig.rank(b))*core.BlockSize:][:core.BlockSize] }
	var err error
	if !o.req.Relay {
		m.data.runs(func(first, n uint16) {
			for b := first; b < first+n && err == nil; b += s.maxWrite {
				k := min(s.maxWrite, first+n-b)
				var buf core.IOBuf
				if buf, err = ctx.Pool.Alloc(int(k) * core.BlockSize); err != nil {
					return
				}
				dst := ctx.Pool.Bytes(buf)
				for i := range k {
					copy(dst[int(i)*core.BlockSize:], at(b+i))
				}
				o.items = append(o.items, item{entry: disklog.WriteEntry(pool, c, o.version, b, k), buf: buf})
			}
		})
		m.holes.runs(func(first, n uint16) {
			o.items = append(o.items, item{entry: disklog.PunchEntry(pool, c, o.version, first, n)})
		})
	}
	if err != nil {
		return err
	}
	if !o.req.Relay && src != nil {
		for i := range recoveryRecords(src) {
			if r := resendRecord(src, i); o.req.PGs != 0 && !allZero(r) {
				f := func(k int) uint64 { return binary.LittleEndian.Uint64(r[8*k:]) }
				ch := core.ChunkIDFromBytes([core.ChunkIDSize]byte(r[32:]))
				o.items = append(o.items, item{entry: disklog.ResendEntry(pool, ch, o.version, o.req.PGs, f(0), f(1), f(2), f(3))})
			}
		}
	}
	if !o.req.Relay {
		o.items = append(o.items, item{entry: disklog.RecoveredEntry(pool, c, o.version, o.req.Interval)})
	}
	if !o.req.Gated || m.empty() {
		return nil
	}
	n := m.data.count()
	buf, err := ctx.Pool.Alloc((head + n) * core.BlockSize)
	if err != nil {
		return err
	}
	dst := ctx.Pool.Bytes(buf)
	if src != nil {
		copy(dst[:head*core.BlockSize], src)
	}
	m.putMaps(dst)
	k := head
	m.data.runs(func(first, n uint16) {
		for b := first; b < first+n; b++ {
			copy(dst[k*core.BlockSize:], at(b))
			k++
		}
	})
	o.buf = buf
	return nil
}

// recoveryRead starts read op oi of the chunk's live image as a recovery buffer: the map of its
// written blocks and holes, then the written blocks' bytes, fetched into place.
func (s *Store) recoveryRead(ctx *core.Context, oi int32) error {
	o := &s.ops[oi]
	s.rexts = s.ix.Extents(s.rexts[:0], s.cfg.Pool, o.req.Chunk, 0)
	var m blockMap
	for i := range m.holes {
		m.holes[i] = ^uint64(0)
	}
	for _, e := range s.rexts {
		if e.LBA != 0 {
			for b := e.First; int(b) < e.end(); b++ {
				m.data.set(b)
				m.holes[b/64] &^= 1 << (b % 64)
			}
		}
	}
	s.recs = s.ix.appendResendRecords(s.recs[:0], s.cfg.Pool, o.req.Chunk, s.rexts)
	n := len(s.recs) / resendRecordSize
	if n > maxResendRecords {
		return ErrMalformed
	}
	buf, err := ctx.Pool.Alloc((recoveryHead(n) + m.data.count()) * core.BlockSize)
	if err != nil {
		return err
	}
	b := ctx.Pool.Bytes(buf)
	clear(b[:recoveryHead(n)*core.BlockSize])
	m.putMaps(b)
	binary.LittleEndian.PutUint32(b[16*blockWords:], uint32(n))
	copy(b[recordsAt:], s.recs)
	o.buf = buf
	for _, e := range s.rexts {
		if e.LBA == 0 {
			continue
		}
		if err := s.startFetch(ctx, oi, e.First, e.Count, e); err != nil {
			return err
		}
	}
	return nil
}

// recoveryAt is where block b's bytes go in recovery buffer buf.
func recoveryAt(buf []byte, b uint16) int {
	var m blockBits
	for w := range blockWords {
		m[w] = binary.LittleEndian.Uint64(buf[8*w:])
	}
	return (recoveryHead(recoveryRecords(buf)) + m.rank(b)) * core.BlockSize
}
