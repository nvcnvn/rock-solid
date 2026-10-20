package lsm

import (
	"bytes"
	"encoding/binary"
	"math/bits"
)

// A block is LevelDB's block format, which Pebble keeps (sstable/rowblk): entries of
// `shared uvarint | unshared uvarint | value_len uvarint | key[shared:] | value`, a restart
// every restartInterval entries whose key is stored whole, then each restart's offset and the
// restart count as little-endian u32s. A block always has at least one restart.
const restartInterval = 16

const emptyBlockSize = 8

// blockWriter builds a block; with record on it also keeps each entry added, a uvarint-prefixed
// key then value, in taken for the flush's decode check.
type blockWriter struct {
	buf        []byte
	restarts   []uint32
	prev       []byte
	n          int
	record     bool
	taken      []byte
	skipSecond bool
}

// reset starts a block in buf, which must have the capacity of every entry added to it, since
// the writer never grows it.
func (w *blockWriter) reset(buf []byte) {
	w.buf, w.restarts, w.prev, w.n, w.taken = buf[:0], w.restarts[:0], w.prev[:0], 0, w.taken[:0]
}

func (w *blockWriter) shared(k []byte) int {
	if w.n%restartInterval == 0 {
		return 0
	}
	n := min(len(k), len(w.prev))
	i := 0
	for i < n && k[i] == w.prev[i] {
		i++
	}
	return i
}

// sizeWith is the finished block's length were k and a value of vlen bytes added.
func (w *blockWriter) sizeWith(k []byte, vlen int) int {
	s := w.shared(k)
	r := max(len(w.restarts), 1)
	if w.n%restartInterval == 0 && w.n > 0 {
		r++
	}
	return len(w.buf) + uvarintLen(s) + uvarintLen(len(k)-s) + uvarintLen(vlen) + len(k) - s + vlen + 4*r + 4
}

func (w *blockWriter) add(k, v []byte) {
	if w.record {
		w.taken = append(binary.AppendUvarint(w.taken, uint64(len(k))), k...)
		w.taken = append(binary.AppendUvarint(w.taken, uint64(len(v))), v...)
		if w.skipSecond && w.n == 1 {
			w.n++
			return
		}
	}
	s := w.shared(k)
	if w.n%restartInterval == 0 {
		w.restarts = append(w.restarts, uint32(len(w.buf)))
	}
	w.buf = binary.AppendUvarint(w.buf, uint64(s))
	w.buf = binary.AppendUvarint(w.buf, uint64(len(k)-s))
	w.buf = binary.AppendUvarint(w.buf, uint64(len(v)))
	w.buf = append(w.buf, k[s:]...)
	w.buf = append(w.buf, v...)
	w.prev = append(w.prev[:0], k...)
	w.n++
}

func (w *blockWriter) finish() []byte {
	if w.n == 0 {
		w.restarts = append(w.restarts[:0], 0)
	}
	for _, r := range w.restarts {
		w.buf = binary.LittleEndian.AppendUint32(w.buf, r)
	}
	return binary.LittleEndian.AppendUint32(w.buf, uint32(len(w.restarts)))
}

func uvarintLen(x int) int { return (bits.Len64(uint64(x)|1) + 6) / 7 }

// blockIter reads a block that validateBlock accepted.
type blockIter struct {
	data     []byte
	restarts int
	count    int
	off      int
	next     int
	key      []byte
	alt      []byte
	val      []byte
}

func (it *blockIter) init(b []byte) {
	it.data = b
	it.count = int(binary.LittleEndian.Uint32(b[len(b)-4:]))
	it.restarts = len(b) - 4 - 4*it.count
	if it.restarts == 0 {
		it.count = 0
	}
	it.off, it.next = it.restarts, it.restarts
}

func (it *blockIter) restart(i int) int {
	return int(binary.LittleEndian.Uint32(it.data[it.restarts+4*i:]))
}

// restartKey is the whole key stored at restart i, sliced from the block.
func (it *blockIter) restartKey(i int) []byte {
	off := it.restart(i)
	_, n := binary.Uvarint(it.data[off:])
	off += n
	unshared, n := binary.Uvarint(it.data[off:])
	off += n
	_, n = binary.Uvarint(it.data[off:])
	off += n
	return it.data[off : off+int(unshared)]
}

// load decodes the entry at off, whose shared prefix is in it.key.
func (it *blockIter) load(off int) bool {
	if off >= it.restarts {
		it.off, it.next = it.restarts, it.restarts
		return false
	}
	shared, n := binary.Uvarint(it.data[off:])
	p := off + n
	unshared, n := binary.Uvarint(it.data[p:])
	p += n
	vlen, n := binary.Uvarint(it.data[p:])
	p += n
	it.key = append(it.key[:shared], it.data[p:p+int(unshared)]...)
	p += int(unshared)
	it.val = it.data[p : p+int(vlen)]
	it.off, it.next = off, p+int(vlen)
	return true
}

func (it *blockIter) valid() bool { return it.off < it.restarts }
func (it *blockIter) first() bool { return it.load(0) }
func (it *blockIter) advance() bool {
	return it.valid() && it.load(it.next)
}

// above is the first restart whose key is > k, or >= k when inclusive.
func (it *blockIter) above(k []byte, inclusive bool) int {
	lo, hi := 0, it.count
	for lo < hi {
		m := int(uint(lo+hi) >> 1)
		c := bytes.Compare(it.restartKey(m), k)
		if c < 0 || (c == 0 && !inclusive) {
			lo = m + 1
		} else {
			hi = m
		}
	}
	return lo
}

// seekGE moves to the first entry with key >= k.
func (it *blockIter) seekGE(k []byte) bool {
	i := it.above(k, false)
	start := 0
	if i > 0 {
		start = it.restart(i - 1)
	}
	for ok := it.load(start); ok; ok = it.advance() {
		if bytes.Compare(it.key, k) >= 0 {
			return true
		}
	}
	return false
}

// floor moves to the last entry with key <= k, or < k when strict.
func (it *blockIter) floor(k []byte, strict bool) bool {
	i := it.above(k, strict)
	if i == 0 {
		it.off, it.next = it.restarts, it.restarts
		return false
	}
	it.load(it.restart(i - 1))
	for {
		off, next, val := it.off, it.next, it.val
		it.alt = append(it.alt[:0], it.key...)
		c := 1
		if it.advance() {
			c = bytes.Compare(it.key, k)
		}
		if c > 0 || (c == 0 && strict) {
			it.key, it.alt = it.alt, it.key
			it.off, it.next, it.val = off, next, val
			return true
		}
	}
}

// validateBlock checks that b is an entry block whose keys are strictly increasing, and above prev
// when hasPrev, with keys of at most MaxKey bytes and every value a kind byte and at most MaxValue
// bytes. It returns the block's entry count and its last key, appended to prev[:0].
func validateBlock(b, prev []byte, hasPrev bool) (int, []byte, bool) {
	return walkBlock(b, prev, hasPrev, false)
}

// validateFragBlock checks that b is a fragment block whose fragments are sorted, never overlap
// one another or end above the next's start, nor start below prevEnd when hasPrev, with bounds of
// at most MaxBound bytes, each ending above its start. It returns the fragment count and the last
// end, appended to prevEnd[:0].
func validateFragBlock(b, prevEnd []byte, hasPrev bool) (int, []byte, bool) {
	return walkBlock(b, prevEnd, hasPrev, true)
}

func walkBlock(b, prev []byte, hasPrev, frags bool) (int, []byte, bool) {
	if len(b) < emptyBlockSize {
		return 0, prev, false
	}
	count := int(binary.LittleEndian.Uint32(b[len(b)-4:]))
	if count < 1 || count > (len(b)-4)/4 {
		return 0, prev, false
	}
	end := len(b) - 4 - 4*count
	restartAt := func(i int) int { return int(binary.LittleEndian.Uint32(b[end+4*i:])) }
	if restartAt(0) != 0 {
		return 0, prev, false
	}
	var key []byte
	n, ri := 0, 0
	for off := 0; off < end; n++ {
		isRestart := ri < count && restartAt(ri) == off
		if ri < count && restartAt(ri) < off {
			return 0, prev, false
		}
		shared, k := binary.Uvarint(b[off:end])
		if k <= 0 {
			return 0, prev, false
		}
		off += k
		unshared, k := binary.Uvarint(b[off:end])
		if k <= 0 {
			return 0, prev, false
		}
		off += k
		vlen, k := binary.Uvarint(b[off:end])
		if k <= 0 {
			return 0, prev, false
		}
		off += k
		if shared > uint64(len(key)) || (isRestart && shared != 0) || n == 0 && shared != 0 ||
			unshared > uint64(end-off) || vlen > uint64(end-off)-unshared || vlen == 0 {
			return 0, prev, false
		}
		key = append(key[:shared], b[off:off+int(unshared)]...)
		off += int(unshared)
		val := b[off : off+int(vlen)]
		off += int(vlen)
		after := hasPrev || n > 0
		if frags {
			if len(key) > MaxBound || len(val) > MaxBound || bytes.Compare(val, key) <= 0 ||
				after && bytes.Compare(key, prev) < 0 {
				return 0, prev, false
			}
			prev = append(prev[:0], val...)
		} else {
			if kind := val[0]; kind != kindSet && kind != kindDel || len(key) > MaxKey || len(val)-1 > MaxValue ||
				after && bytes.Compare(key, prev) <= 0 {
				return 0, prev, false
			}
			prev = append(prev[:0], key...)
		}
		if isRestart {
			ri++
		}
	}
	if ri != count && !(n == 0 && count == 1) {
		return 0, prev, false
	}
	return n, prev, true
}
