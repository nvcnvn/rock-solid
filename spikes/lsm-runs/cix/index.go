// Package chunkstore is the data layer of one disk: the chunk index over the disk log, and later
// the replica primary, pools, placement, peering and failure detection.
package cix

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	lsm "github.com/nvcnvn/rock-solid/spikes/lsm-runs/lsmx"
)

// A chunk's keys are pool u32 | XXH3 of the chunk u64 | chunk ID, all big-endian, so a placement
// group's chunks are one key range. That prefix alone holds the chunk's entry; with a version
// appended (0 for the live map) and a first block, an extent of that map; a frozen version alone
// holds its freeze epoch, sorting before that version's extents. A record's live block count sits
// under RecordPool and its header's LBA, after every chunk.
const (
	chunkKeySize  = 4 + 8 + core.ChunkIDSize
	frozenKeySize = chunkKeySize + 8
	// KeySize is the length of an extent's key.
	KeySize    = frozenKeySize + 2
	recordSize = 4 + 8
	extentSize = 8 + 8 + 2 + 8
	// RecordPool is the pool ID the index keeps record counts under; no chunk may use it.
	RecordPool core.PoolID = math.MaxUint32
)

var (
	// ErrReservedPool reports a record entry for RecordPool.
	ErrReservedPool = errors.New("chunkstore: pool reserved for the index")
	// ErrFrozenLive reports a freeze of version 0, which names the live map.
	ErrFrozenLive = errors.New("chunkstore: freeze of version 0")
	// ErrBadKey reports a key that is not an extent's.
	ErrBadKey = errors.New("chunkstore: not an extent key")
)

// Key is an extent's key: the chunk, the frozen version whose map holds it (0 for the live map) and
// its first block.
type Key struct {
	Pool    core.PoolID
	Chunk   core.ChunkID
	Version uint64
	First   uint16
}

// AppendKey appends k's KeySize-byte encoding.
func AppendKey(dst []byte, k Key) []byte {
	dst = appendChunkPrefix(dst, k.Pool, k.Chunk)
	dst = binary.BigEndian.AppendUint64(dst, k.Version)
	return binary.BigEndian.AppendUint16(dst, k.First)
}

func appendChunkPrefix(dst []byte, pool core.PoolID, c core.ChunkID) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(pool))
	dst = binary.BigEndian.AppendUint64(dst, c.Hash())
	return c.AppendTo(dst)
}

// DecodeKey decodes an extent's key, checking its hash names its chunk.
func DecodeKey(b []byte) (Key, error) {
	if len(b) != KeySize || core.PoolID(binary.BigEndian.Uint32(b)) == RecordPool {
		return Key{}, ErrBadKey
	}
	k := Key{
		Pool:    core.PoolID(binary.BigEndian.Uint32(b)),
		Chunk:   core.ChunkIDFromBytes([core.ChunkIDSize]byte(b[12:chunkKeySize])),
		Version: binary.BigEndian.Uint64(b[chunkKeySize:]),
		First:   binary.BigEndian.Uint16(b[frozenKeySize:]),
	}
	if k.Chunk.Hash() != binary.BigEndian.Uint64(b[4:]) {
		return Key{}, ErrBadKey
	}
	return k, nil
}

// Extent is a run of a chunk's blocks written by one record: Count blocks from block First of the
// chunk, at version Version, stored from LBA on, or zeros when LBA is 0, in the record whose header
// is at Header.
type Extent struct {
	First   uint16
	Count   uint16
	Version uint64
	LBA     core.LBA
	Header  core.LBA
}

func (e Extent) end() int { return int(e.First) + int(e.Count) }

// BlockLBA is where block b of the chunk, which the extent covers, is stored, or 0 for zeros.
func (e Extent) BlockLBA(b uint16) core.LBA {
	if e.LBA == 0 {
		return 0
	}
	return e.LBA + core.LBA(b-e.First)
}

func (e Extent) shifted(first uint16) Extent {
	if e.LBA != 0 {
		e.LBA += core.LBA(first - e.First)
	}
	e.Count -= first - e.First
	e.First = first
	return e
}

func appendExtent(dst []byte, e Extent) []byte {
	dst = binary.LittleEndian.AppendUint64(dst, e.Version)
	dst = binary.LittleEndian.AppendUint64(dst, uint64(e.LBA))
	dst = binary.LittleEndian.AppendUint16(dst, e.Count)
	return binary.LittleEndian.AppendUint64(dst, uint64(e.Header))
}

func decodeExtent(key, v []byte) (Extent, bool) {
	if len(key) != KeySize || len(v) != extentSize {
		return Extent{}, false
	}
	return Extent{
		First:   binary.BigEndian.Uint16(key[frozenKeySize:]),
		Version: binary.LittleEndian.Uint64(v),
		LBA:     core.LBA(binary.LittleEndian.Uint64(v[8:])),
		Count:   binary.LittleEndian.Uint16(v[16:]),
		Header:  core.LBA(binary.LittleEndian.Uint64(v[18:])),
	}, true
}

// Chunk is a chunk's entry: its current version and the snapshot epoch of its last freeze.
type Chunk struct {
	Version uint64
	Epoch   uint64
}

// Index is one disk's chunk index, in an LSM tree: per chunk its entry, its live map, which never
// holds two extents for one block, a frozen map per version a snapshot froze, and per record the
// blocks the maps still refer to. Records apply once each, in LSN order.
type Index struct {
	// RangeTrim, when above 0, makes a write that removes at least that many extents' keys remove
	// them with one range removal instead of one removal each.
	RangeTrim int
	lo, hi    []byte
	tree      *lsm.Tree
	key       []byte
	val       []byte
	exts      []Extent
	vers      []uint64
	it        lsm.Iter
	check     []recordCount
}

// NewIndex makes an index over tree, which holds the index alone.
func NewIndex(tree *lsm.Tree) *Index { return &Index{tree: tree} }

// Tree is the LSM tree the index is in, for its owner to flush and restore.
func (x *Index) Tree() *lsm.Tree { return x.tree }

func (x *Index) extentKey(pool core.PoolID, c core.ChunkID, version uint64, first uint16) []byte {
	x.key = AppendKey(x.key[:0], Key{Pool: pool, Chunk: c, Version: version, First: first})
	return x.key
}

func (x *Index) put(key []byte, e Extent) {
	x.val = appendExtent(x.val[:0], e)
	x.must(x.tree.Put(key, x.val))
}

func (x *Index) must(err error) {
	if err != nil {
		panic(fmt.Sprintf("chunkstore: index entry rejected: %v", err))
	}
}

// Apply maps a durable record, a Durable or Replayed event of the disk log: writes and punches
// replace what they cover in the live map, and a freeze copies the live map under its version
// first, as the entries come in the record.
func (x *Index) Apply(ev *disklog.Event) error {
	for i := range ev.Entries {
		e := &ev.Entries[i]
		if e.Kind != disklog.EntryBlob && e.Pool == RecordPool {
			return ErrReservedPool
		}
		if e.Kind == disklog.EntryFreeze && e.Version == 0 {
			return ErrFrozenLive
		}
	}
	for i := range ev.Entries {
		e := &ev.Entries[i]
		switch e.Kind {
		case disklog.EntryWrite:
			lba := ev.LBA + core.LBA(ev.HeaderBlocks) + core.LBA(ev.Payloads[i]/core.BlockSize)
			x.insert(e.Pool, e.Chunk, Extent{First: e.First, Count: e.Count, Version: e.Version, LBA: lba, Header: ev.LBA})
		case disklog.EntryPunch:
			x.insert(e.Pool, e.Chunk, Extent{First: e.First, Count: e.Count, Version: e.Version, Header: ev.LBA})
		case disklog.EntryFreeze:
			x.freeze(e.Pool, e.Chunk, e.Version, e.Epoch)
		}
	}
	return nil
}

// insert maps ext into the chunk's live map, trimming and splitting the extents it covers.
func (x *Index) insert(pool core.PoolID, c core.ChunkID, ext Extent) {
	x.exts = x.overlapping(x.exts[:0], pool, c, 0, ext.First, ext.end())
	removed := 0
	for _, o := range x.exts {
		if o.First > ext.First {
			removed++
		}
	}
	ranged := x.RangeTrim > 0 && removed >= x.RangeTrim
	if ranged {
		x.lo = AppendKey(x.lo[:0], Key{Pool: pool, Chunk: c, First: ext.First + 1})
		x.hi = AppendKey(x.hi[:0], Key{Pool: pool, Chunk: c, First: uint16(ext.end())})
		x.must(x.tree.DeleteRange(x.lo, x.hi))
	}
	for _, o := range x.exts {
		lo, hi := max(int(o.First), int(ext.First)), min(o.end(), ext.end())
		if o.LBA != 0 {
			x.addLive(o.Header, -(hi - lo))
		}
		switch {
		case o.First < ext.First:
			left := o
			left.Count = ext.First - o.First
			x.put(x.extentKey(pool, c, 0, o.First), left)
		case o.First != ext.First && !ranged:
			x.must(x.tree.Delete(x.extentKey(pool, c, 0, o.First)))
		}
		if o.end() > ext.end() {
			x.put(x.extentKey(pool, c, 0, uint16(ext.end())), o.shifted(uint16(ext.end())))
		}
	}
	x.put(x.extentKey(pool, c, 0, ext.First), ext)
	if ext.LBA != 0 {
		x.addLive(ext.Header, int(ext.Count))
	}
	ch, _ := x.Chunk(pool, c)
	ch.Version = ext.Version
	x.putChunk(pool, c, ch)
}

// overlapping appends the extents of map version that cover any block in [first, end).
func (x *Index) overlapping(dst []Extent, pool core.PoolID, c core.ChunkID, version uint64, first uint16, end int) []Extent {
	k := x.extentKey(pool, c, version, first)
	if fk, v, ok := x.tree.Floor(k); ok && bytes.HasPrefix(fk, k[:frozenKeySize]) {
		if e, ok := decodeExtent(fk, v); ok && e.First < first && e.end() > int(first) {
			dst = append(dst, e)
		}
	}
	k = x.extentKey(pool, c, version, first)
	x.tree.Iter(&x.it)
	for ok := x.it.SeekGE(k); ok && bytes.HasPrefix(x.it.Key(), k[:frozenKeySize]); ok = x.it.Next() {
		e, ok := decodeExtent(x.it.Key(), x.it.Value())
		if !ok || int(e.First) >= end {
			break
		}
		dst = append(dst, e)
	}
	return dst
}

// freeze copies the live map under version, once: replaying a freeze already applied changes nothing.
func (x *Index) freeze(pool core.PoolID, c core.ChunkID, version, epoch uint64) {
	x.key = binary.BigEndian.AppendUint64(appendChunkPrefix(x.key[:0], pool, c), version)
	if _, ok := x.tree.Get(x.key); ok {
		return
	}
	x.exts = x.overlapping(x.exts[:0], pool, c, 0, 0, core.BlocksPerChunk)
	for _, e := range x.exts {
		x.put(x.extentKey(pool, c, version, e.First), e)
		if e.LBA != 0 {
			x.addLive(e.Header, int(e.Count))
		}
	}
	x.key = binary.BigEndian.AppendUint64(appendChunkPrefix(x.key[:0], pool, c), version)
	x.val = binary.LittleEndian.AppendUint64(x.val[:0], epoch)
	x.must(x.tree.Put(x.key, x.val))
	ch, ok := x.Chunk(pool, c)
	if !ok {
		ch.Version = version
	}
	ch.Epoch = max(ch.Epoch, epoch)
	x.putChunk(pool, c, ch)
}

func (x *Index) putChunk(pool core.PoolID, c core.ChunkID, ch Chunk) {
	x.key = appendChunkPrefix(x.key[:0], pool, c)
	x.val = binary.LittleEndian.AppendUint64(x.val[:0], ch.Version)
	x.val = binary.LittleEndian.AppendUint64(x.val, ch.Epoch)
	x.must(x.tree.Put(x.key, x.val))
}

func (x *Index) recordKey(header core.LBA) []byte {
	x.key = binary.BigEndian.AppendUint32(x.key[:0], uint32(RecordPool))
	x.key = binary.BigEndian.AppendUint64(x.key, uint64(header))
	return x.key
}

func (x *Index) addLive(header core.LBA, delta int) {
	n := int(x.Live(header)) + delta
	if n < 0 {
		panic(fmt.Sprintf("chunkstore: record at LBA %d refers to %d blocks", header, n))
	}
	k := x.recordKey(header)
	if n == 0 {
		x.must(x.tree.Delete(k))
		return
	}
	x.val = binary.LittleEndian.AppendUint32(x.val[:0], uint32(n))
	x.must(x.tree.Put(k, x.val))
}

// Live is how many blocks of the record whose header is at header the live and frozen maps refer
// to; a block both refer to counts twice, and a record at 0 holds nothing the index needs.
func (x *Index) Live(header core.LBA) uint32 {
	v, ok := x.tree.Get(x.recordKey(header))
	if !ok {
		return 0
	}
	return binary.LittleEndian.Uint32(v)
}

// Chunk returns the chunk's entry.
func (x *Index) Chunk(pool core.PoolID, c core.ChunkID) (Chunk, bool) {
	x.key = appendChunkPrefix(x.key[:0], pool, c)
	v, ok := x.tree.Get(x.key)
	if !ok {
		return Chunk{}, false
	}
	return Chunk{Version: binary.LittleEndian.Uint64(v), Epoch: binary.LittleEndian.Uint64(v[8:])}, true
}

// Lookup returns the live extent holding block b of the chunk.
func (x *Index) Lookup(pool core.PoolID, c core.ChunkID, b uint16) (Extent, bool) {
	return x.lookup(pool, c, 0, b)
}

func (x *Index) lookup(pool core.PoolID, c core.ChunkID, version uint64, b uint16) (Extent, bool) {
	k := x.extentKey(pool, c, version, b)
	fk, v, ok := x.tree.Floor(k)
	if !ok || !bytes.HasPrefix(fk, k[:frozenKeySize]) {
		return Extent{}, false
	}
	e, ok := decodeExtent(fk, v)
	if !ok || int(b) >= e.end() {
		return Extent{}, false
	}
	return e, true
}

// LookupAt returns the extent holding block b of the chunk as the snapshot of epoch reads it: from
// the frozen version with the smallest freeze epoch at or above epoch, since writes after that
// snapshot's cut carry its epoch, or the live map when none is.
func (x *Index) LookupAt(pool core.PoolID, c core.ChunkID, b uint16, epoch uint64) (Extent, bool) {
	if v, ok := x.FrozenAt(pool, c, epoch); ok {
		return x.lookup(pool, c, v, b)
	}
	return x.Lookup(pool, c, b)
}

// FrozenAt returns the frozen version a snapshot at epoch reads the chunk at, if any.
func (x *Index) FrozenAt(pool core.PoolID, c core.ChunkID, epoch uint64) (uint64, bool) {
	x.vers = x.frozen(x.vers[:0], pool, c)
	best, bestEpoch, found := uint64(0), uint64(0), false
	for i := 0; i < len(x.vers); i += 2 {
		if v, e := x.vers[i], x.vers[i+1]; e >= epoch && (!found || e < bestEpoch) {
			best, bestEpoch, found = v, e, true
		}
	}
	return best, found
}

// frozen appends each frozen version of the chunk and its freeze epoch.
func (x *Index) frozen(dst []uint64, pool core.PoolID, c core.ChunkID) []uint64 {
	x.key = binary.BigEndian.AppendUint64(appendChunkPrefix(x.key[:0], pool, c), 1)
	x.tree.Iter(&x.it)
	for ok := x.it.SeekGE(x.key); ok && bytes.HasPrefix(x.it.Key(), x.key[:chunkKeySize]); {
		k := x.it.Key()
		if len(k) != frozenKeySize {
			ok = x.it.Next()
			continue
		}
		v := binary.BigEndian.Uint64(k[chunkKeySize:])
		dst = append(dst, v, binary.LittleEndian.Uint64(x.it.Value()))
		if v == math.MaxUint64 {
			break
		}
		x.key = binary.BigEndian.AppendUint64(x.key[:chunkKeySize], v+1)
		ok = x.it.SeekGE(x.key)
	}
	return dst
}

// Extents appends the extents of the chunk's map at version, 0 for the live map, by first block.
func (x *Index) Extents(dst []Extent, pool core.PoolID, c core.ChunkID, version uint64) []Extent {
	return x.overlapping(dst, pool, c, version, 0, core.BlocksPerChunk)
}

// Move applies a cleaner's copy of count blocks from block first of the chunk, stored from LBA
// from on, to LBA to in the record whose header is at header. It changes each map, live or
// frozen, whose extent still stores exactly those blocks at from, so a write that landed while the
// cleaner copied wins, and returns how many maps it changed.
func (x *Index) Move(pool core.PoolID, c core.ChunkID, first, count uint16, from, to, header core.LBA) int {
	x.vers = append(x.frozen(x.vers[:0], pool, c), 0, 0)
	moved := 0
	for i := 0; i < len(x.vers); i += 2 {
		v := x.vers[i]
		e, ok := x.lookup(pool, c, v, first)
		if !ok || e.LBA == 0 || e.BlockLBA(first) != from || int(first)+int(count) > e.end() || count == 0 {
			continue
		}
		if e.First < first {
			left := e
			left.Count = first - e.First
			x.put(x.extentKey(pool, c, v, e.First), left)
		}
		mid := e.shifted(first)
		mid.Count, mid.LBA, mid.Header = count, to, header
		x.put(x.extentKey(pool, c, v, first), mid)
		if end := int(first) + int(count); e.end() > end {
			x.put(x.extentKey(pool, c, v, uint16(end)), e.shifted(uint16(end)))
		}
		x.addLive(e.Header, -int(count))
		x.addLive(header, int(count))
		moved++
	}
	return moved
}

type recordCount struct {
	header core.LBA
	n      uint32
}

// Check verifies the index's invariants: every map's extents lie in the chunk and never share a
// block, and each record's count equals the blocks the maps refer to in it.
func (x *Index) Check() error {
	x.check = x.check[:0]
	var stored []recordCount
	var prefix []byte
	var mapVersion, marker uint64
	prevEnd := 0
	x.tree.Iter(&x.it)
	for ok := x.it.SeekGE(nil); ok; ok = x.it.Next() {
		k, v := x.it.Key(), x.it.Value()
		switch {
		case len(k) == recordSize && core.PoolID(binary.BigEndian.Uint32(k)) == RecordPool:
			if len(v) != 4 || binary.LittleEndian.Uint32(v) == 0 {
				return fmt.Errorf("record %x: count %x", k, v)
			}
			stored = append(stored, recordCount{core.LBA(binary.BigEndian.Uint64(k[4:])), binary.LittleEndian.Uint32(v)})
		case len(k) == chunkKeySize:
			if len(v) != 16 {
				return fmt.Errorf("chunk %x: entry %x", k, v)
			}
			prefix = append(prefix[:0], k...)
			mapVersion, marker, prevEnd = math.MaxUint64, 0, 0
		case len(k) == frozenKeySize && bytes.HasPrefix(k, prefix) && len(prefix) > 0:
			if len(v) != 8 {
				return fmt.Errorf("frozen %x: epoch %x", k, v)
			}
			marker = binary.BigEndian.Uint64(k[chunkKeySize:])
		case len(k) == KeySize && bytes.HasPrefix(k, prefix) && len(prefix) > 0:
			key, err := DecodeKey(k)
			e, ok := decodeExtent(k, v)
			if err != nil || !ok || e.Count == 0 || e.end() > core.BlocksPerChunk || key.Version != 0 && key.Version != marker {
				return fmt.Errorf("extent %x: %x", k, v)
			}
			if key.Version != mapVersion {
				mapVersion, prevEnd = key.Version, 0
			}
			if int(e.First) < prevEnd {
				return fmt.Errorf("chunk %v map %d: block %d held twice", key.Chunk, key.Version, e.First)
			}
			prevEnd = e.end()
			if e.LBA != 0 {
				x.check = append(x.check, recordCount{e.Header, uint32(e.Count)})
			}
		default:
			return fmt.Errorf("key %x outside any chunk", k)
		}
	}
	slices.SortFunc(x.check, func(a, b recordCount) int { return cmp.Compare(a.header, b.header) })
	var want []recordCount
	for _, rc := range x.check {
		if n := len(want); n > 0 && want[n-1].header == rc.header {
			want[n-1].n += rc.n
		} else {
			want = append(want, rc)
		}
	}
	if !slices.Equal(stored, want) {
		return fmt.Errorf("record counts %v, the maps refer to %v", stored, want)
	}
	return nil
}
