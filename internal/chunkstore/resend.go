package chunkstore

import (
	"encoding/binary"
	"math/bits"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// The resend table's keys are ResendPool u32 | pool u32 | log₂ of the pool's PG count u8 | PG u32 |
// session u64 | incarnation u64, all big-endian: that key holds the session's watermark in the PG,
// and with a request ID appended, the chunk and version the request's write took. A PG's table is
// one key range per PG count it had, since entries and watermarks logged before a split hold for
// both halves.
const (
	resendWatermarkSize = 4 + 4 + 1 + 4 + 8 + 8
	resendKeySize       = resendWatermarkSize + 8
	resendValueSize     = core.ChunkIDSize + 8
	// ResendPool is the pool ID the index keeps the resend table under; no chunk may use it.
	ResendPool core.PoolID = DecisionPool - 1
)

func (x *Index) resendPrefix(dst []byte, pool core.PoolID, level uint8, pg uint32) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(ResendPool))
	dst = binary.BigEndian.AppendUint32(dst, uint32(pool))
	return binary.BigEndian.AppendUint32(append(dst, level), pg)
}

func (x *Index) watermarkKey(pool core.PoolID, level uint8, pg uint32, session, inc uint64) []byte {
	x.key = x.resendPrefix(x.key[:0], pool, level, pg)
	return binary.BigEndian.AppendUint64(binary.BigEndian.AppendUint64(x.key, session), inc)
}

// lowestLevel is the smallest log₂ PG count of the pool's resend table, which a walk over a PG's
// ancestors stops at, or false when the pool has none.
func (x *Index) lowestLevel(pool core.PoolID) (uint8, bool) {
	x.lo = binary.BigEndian.AppendUint32(binary.BigEndian.AppendUint32(x.lo[:0], uint32(ResendPool)), uint32(pool))
	x.tree.Iter(&x.it)
	if !x.it.SeekGE(x.lo) {
		return 0, false
	}
	k := x.it.Key()
	if len(k) < resendWatermarkSize || core.PoolID(binary.BigEndian.Uint32(k)) != ResendPool || core.PoolID(binary.BigEndian.Uint32(k[4:])) != pool {
		return 0, false
	}
	return k[8], true
}

// ancestors calls f with chunk c's PG of the pool's pgs PGs and then each PG it split from, down to
// the lowest PG count the pool's table holds, until f returns false.
func (x *Index) ancestors(pool core.PoolID, c core.ChunkID, pgs uint32, f func(level uint8, pg uint32) bool) {
	lowest, ok := x.lowestLevel(pool)
	if !ok {
		return
	}
	top := uint8(bits.TrailingZeros32(pgs))
	pg := core.PGIndexOf(c.Hash(), pgs)
	for l := int(top); l >= int(lowest); l-- {
		if !f(uint8(l), pg>>(top-uint8(l))) {
			return
		}
	}
}

// watermark is the session's watermark in chunk c's PG and its ancestors, 0 for none.
func (x *Index) watermark(pool core.PoolID, c core.ChunkID, pgs uint32, session, inc uint64) uint64 {
	var wm uint64
	x.ancestors(pool, c, pgs, func(level uint8, pg uint32) bool {
		if v, ok := x.tree.Get(x.watermarkKey(pool, level, pg, session, inc)); ok {
			wm = max(wm, binary.LittleEndian.Uint64(v))
		}
		return true
	})
	return wm
}

// Resent looks a client write up in the resend table of chunk c's PG of the pool's pgs PGs: the
// version of c the request took, 0 for none, and whether the request is at or below its session's
// watermark, so its client already heard an earlier attempt's answer.
func (x *Index) Resent(pool core.PoolID, c core.ChunkID, pgs uint32, session, inc, request uint64) (version uint64, stale bool) {
	if request <= x.watermark(pool, c, pgs, session, inc) {
		return 0, true
	}
	x.ancestors(pool, c, pgs, func(level uint8, pg uint32) bool {
		k := binary.BigEndian.AppendUint64(x.watermarkKey(pool, level, pg, session, inc), request)
		if v, ok := x.tree.Get(k); ok && len(v) == resendValueSize && core.ChunkIDFromBytes([core.ChunkIDSize]byte(v)) == c {
			version = binary.LittleEndian.Uint64(v[core.ChunkIDSize:])
			return false
		}
		return true
	})
	return version, false
}

// writer applies a writer entry: the table names the request's version unless the request is at
// or below its session's watermark, and the session's entries in the PG below its oldest
// outstanding request go, the watermark rising to the one before it.
func (x *Index) writer(e *disklog.Entry) {
	pgs := uint32(e.Length)
	top := uint8(bits.TrailingZeros32(pgs))
	pg := core.PGIndexOf(e.Chunk.Hash(), pgs)
	wm := x.watermark(e.Pool, e.Chunk, pgs, e.Session, e.Incarnation)
	if e.Request > wm {
		k := binary.BigEndian.AppendUint64(x.watermarkKey(e.Pool, top, pg, e.Session, e.Incarnation), e.Request)
		x.val = binary.LittleEndian.AppendUint64(e.Chunk.AppendTo(x.val[:0]), e.Version)
		x.must(x.tree.Put(k, x.val))
	}
	oldest := e.Oldest
	if x.plant != nil && x.plant.trimOnAnswer {
		oldest = max(oldest, e.Request+1)
	}
	if oldest <= wm+1 {
		return
	}
	x.ancestors(e.Pool, e.Chunk, pgs, func(level uint8, pg uint32) bool {
		x.lo = append(x.lo[:0], x.watermarkKey(e.Pool, level, pg, e.Session, e.Incarnation)...)
		x.hi = binary.BigEndian.AppendUint64(append(x.hi[:0], x.lo...), oldest)
		x.must(x.tree.DeleteRange(x.lo, x.hi))
		return true
	})
	if x.plant != nil && x.plant.noWatermark {
		return
	}
	x.val = binary.LittleEndian.AppendUint64(x.val[:0], oldest-1)
	x.must(x.tree.Put(x.watermarkKey(e.Pool, top, pg, e.Session, e.Incarnation), x.val))
}
