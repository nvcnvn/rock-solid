// Package chunkstore is the data layer of one disk: the chunk index over the disk log, and later
// the replica primary, pools, placement, peering and failure detection.
package chunkstore

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
	"github.com/nvcnvn/rock-solid/internal/lsm"
)

// A chunk's keys are pool u32 | XXH3 of the chunk u64 | chunk ID, all big-endian, so a placement
// group's chunks are one key range. That prefix alone holds the chunk's entry; with a version
// appended (0 for the live map) and a first block, an extent of that map; a frozen version alone
// holds its freeze epoch, sorting before that version's extents. Version 0, the absent chunk, has
// no extents of its own: its marker sorts before the live map's. A record's live block count sits
// under RecordPool and its header's LBA, after every chunk, and a chunk slot's under its first
// LBA, which no record header shares: a slot segment holds no records.
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
	// ErrFrozenLive reports a freeze of version 0, the absent chunk, of a chunk that exists.
	ErrFrozenLive = errors.New("chunkstore: freeze of version 0 of an existing chunk")
	// ErrBadKey reports a key that is not an extent's.
	ErrBadKey = errors.New("chunkstore: not an extent key")
	// ErrNoSlots reports a slot write on a disk whose segments hold no chunk slot.
	ErrNoSlots = errors.New("chunkstore: slot write on a disk without chunk slots")
)

// Key is an extent's key: the chunk, the frozen version whose map holds it (0 for the live map) and its first block.
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

// Extent is Count blocks of a chunk from block First at Version, stored from LBA on (zeros when 0), in the record at Header.
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

// Index is one disk's chunk index in an LSM tree: per chunk its entry, live map and frozen maps, and per record its live blocks.
type Index struct {
	tree          *lsm.Tree
	segmentBlocks uint32
	// freed is the chunk slots whose count reached zero since the owner last took them.
	freed         []core.LBA
	key           []byte
	val           []byte
	lo            []byte
	hi            []byte
	exts          []Extent
	vers          []uint64
	it            lsm.Iter
	check         []recordCount
	pointRemovals bool
}

// Prefix is the length of an index key's prefix for run filters: its chunk's, or a record count's whole key.
func Prefix(k []byte) int {
	switch {
	case len(k) >= recordSize && core.PoolID(binary.BigEndian.Uint32(k)) == RecordPool:
		return recordSize
	case len(k) >= chunkKeySize:
		return chunkKeySize
	}
	return 0
}

// TreeConfig is cfg with run filters of each key's Prefix, which the index's lookups probe.
func TreeConfig(cfg lsm.Config) lsm.Config {
	cfg.Filter, cfg.Prefix = lsm.Prefixes, Prefix
	return cfg
}

// NewIndex makes an index over tree, which holds the index alone, configured by TreeConfig, of a disk whose segments are segmentBlocks long.
func NewIndex(tree *lsm.Tree, segmentBlocks uint32) *Index {
	return &Index{tree: tree, segmentBlocks: segmentBlocks}
}

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

// Apply maps a Durable or Replayed record's entries in order: writes and punches into the live map, freezes as copies of it.
func (x *Index) Apply(ev *disklog.Event) error {
	for i := range ev.Entries {
		e := &ev.Entries[i]
		if e.Kind != disklog.EntryBlob && e.Pool == RecordPool {
			return ErrReservedPool
		}
		if e.Kind == disklog.EntrySlot && x.segmentBlocks < core.BlocksPerChunk {
			return ErrNoSlots
		}
		if e.Kind == disklog.EntryFreeze && e.Version == 0 && !x.absent(e.Pool, e.Chunk) {
			if _, ok := x.Chunk(e.Pool, e.Chunk); ok {
				return ErrFrozenLive
			}
		}
	}
	for i := range ev.Entries {
		e := &ev.Entries[i]
		switch e.Kind {
		case disklog.EntryWrite:
			lba := ev.LBA + core.LBA(ev.HeaderBlocks) + core.LBA(ev.Payloads[i]/core.BlockSize)
			x.insert(e.Pool, e.Chunk, Extent{First: e.First, Count: e.Count, Version: e.Version, LBA: lba, Header: ev.LBA})
		case disklog.EntrySlot:
			lba := core.LBA(e.Segment)*core.LBA(x.segmentBlocks) + core.LBA(e.Slot)*core.BlocksPerChunk
			x.insert(e.Pool, e.Chunk, Extent{First: e.First, Count: e.Count, Version: e.Version, LBA: lba, Header: ev.LBA})
		case disklog.EntryPunch:
			if e.First == 0 && e.Count == core.BlocksPerChunk {
				x.punchWhole(e.Pool, e.Chunk, e.Version)
				continue
			}
			x.insert(e.Pool, e.Chunk, Extent{First: e.First, Count: e.Count, Version: e.Version, Header: ev.LBA})
		case disklog.EntryFreeze:
			x.freeze(e.Pool, e.Chunk, e.Version, e.Epoch)
		}
	}
	return nil
}

// insert maps ext into the chunk's live map, trimming and splitting the extents it covers, and
// removing the keys of those starting inside it with one range removal when there are two or more.
func (x *Index) insert(pool core.PoolID, c core.ChunkID, ext Extent) {
	x.exts = x.overlapping(x.exts[:0], pool, c, 0, ext.First, ext.end())
	inside := 0
	for _, o := range x.exts {
		if o.First > ext.First {
			inside++
		}
	}
	ranged := inside >= 2 && !x.pointRemovals
	if ranged {
		x.lo = AppendKey(x.lo[:0], Key{Pool: pool, Chunk: c, First: ext.First + 1})
		x.hi = AppendKey(x.hi[:0], Key{Pool: pool, Chunk: c, First: uint16(ext.end())})
		x.must(x.tree.DeleteRange(x.lo, x.hi))
	}
	for _, o := range x.exts {
		lo, hi := max(int(o.First), int(ext.First)), min(o.end(), ext.end())
		x.refer(o, -(hi - lo))
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
	x.refer(ext, int(ext.Count))
	ch, _ := x.Chunk(pool, c)
	ch.Version = ext.Version
	x.putChunk(pool, c, ch)
}

// punchWhole empties the chunk's live map with one range removal, keeping its entry, at version,
// as its tombstone.
func (x *Index) punchWhole(pool core.PoolID, c core.ChunkID, version uint64) {
	x.exts = x.overlapping(x.exts[:0], pool, c, 0, 0, core.BlocksPerChunk)
	x.removeMap(pool, c, 0, x.exts)
	ch, _ := x.Chunk(pool, c)
	ch.Version = version
	x.putChunk(pool, c, ch)
}

// removeMap removes map version of the chunk, whose extents are exts, with one range removal:
// the live map's extents for version 0, a frozen version's epoch and extents otherwise.
func (x *Index) removeMap(pool core.PoolID, c core.ChunkID, version uint64, exts []Extent) {
	for _, e := range exts {
		x.refer(e, -int(e.Count))
	}
	if x.pointRemovals {
		if version != 0 {
			x.must(x.tree.Delete(binary.BigEndian.AppendUint64(appendChunkPrefix(x.key[:0], pool, c), version)))
		}
		for _, e := range exts {
			x.must(x.tree.Delete(x.extentKey(pool, c, version, e.First)))
		}
		return
	}
	x.lo = binary.BigEndian.AppendUint64(appendChunkPrefix(x.lo[:0], pool, c), version)
	if version == 0 {
		x.lo = binary.BigEndian.AppendUint16(x.lo, 0)
	}
	x.hi = appendChunkPrefix(x.hi[:0], pool, c)
	if version == math.MaxUint64 {
		x.hi = binary.BigEndian.AppendUint64(x.hi, version)
		x.hi = append(x.hi, 0xff, 0xff, 0xff)
	} else {
		x.hi = binary.BigEndian.AppendUint64(x.hi, version+1)
	}
	x.must(x.tree.DeleteRange(x.lo, x.hi))
}

// Release removes a frozen version no snapshot reads any more with one range removal over its epoch and map; the absent version 0, whose empty map shares the live map's key range, removes no keys.
func (x *Index) Release(pool core.PoolID, c core.ChunkID, version uint64) {
	if version == 0 {
		return
	}
	x.key = binary.BigEndian.AppendUint64(appendChunkPrefix(x.key[:0], pool, c), version)
	if _, ok := x.tree.Get(x.key); !ok {
		return
	}
	x.exts = x.overlapping(x.exts[:0], pool, c, version, 0, core.BlocksPerChunk)
	x.removeMap(pool, c, version, x.exts)
}

// overlapping appends the extents of map version that cover any block in [first, end).
func (x *Index) overlapping(dst []Extent, pool core.PoolID, c core.ChunkID, version uint64, first uint16, end int) []Extent {
	k := x.extentKey(pool, c, version, first)
	if fk, v, ok := x.tree.FloorPrefix(k); ok && bytes.HasPrefix(fk, k[:frozenKeySize]) {
		if e, ok := decodeExtent(fk, v); ok && e.First < first && e.end() > int(first) {
			dst = append(dst, e)
		}
	}
	k = x.extentKey(pool, c, version, first)
	x.tree.Iter(&x.it)
	for ok := x.it.SeekPrefixGE(k); ok && bytes.HasPrefix(x.it.Key(), k[:frozenKeySize]); ok = x.it.Next() {
		e, ok := decodeExtent(x.it.Key(), x.it.Value())
		if !ok || int(e.First) >= end {
			break
		}
		dst = append(dst, e)
	}
	return dst
}

// freeze copies the live map under version, or records version 0 absent with an empty map, once:
// replaying a freeze already applied changes nothing.
func (x *Index) freeze(pool core.PoolID, c core.ChunkID, version, epoch uint64) {
	x.key = binary.BigEndian.AppendUint64(appendChunkPrefix(x.key[:0], pool, c), version)
	if _, ok := x.tree.Get(x.key); ok {
		return
	}
	if version != 0 {
		x.exts = x.overlapping(x.exts[:0], pool, c, 0, 0, core.BlocksPerChunk)
		for _, e := range x.exts {
			x.put(x.extentKey(pool, c, version, e.First), e)
			x.refer(e, int(e.Count))
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

// refer counts n more of the extent's blocks referred to, against its record and, for blocks in
// a chunk slot, its slot.
func (x *Index) refer(e Extent, n int) {
	if e.LBA == 0 {
		return
	}
	x.addLive(e.Header, n)
	if slot, ok := x.slotOf(e); ok {
		if x.addLive(slot, n) == 0 {
			x.freed = append(x.freed, slot)
		}
	}
}

// slotOf is the first LBA of the chunk slot holding the extent's blocks, if they lie in one:
// outside the segment of the record that wrote them, since a record's data follows its header.
func (x *Index) slotOf(e Extent) (core.LBA, bool) {
	if x.segmentBlocks < core.BlocksPerChunk || e.LBA == 0 {
		return 0, false
	}
	seg := core.LBA(x.segmentBlocks)
	return e.LBA - core.LBA(e.First), e.LBA/seg != e.Header/seg
}

// TakeFreed appends the chunk slots no map refers to any more and forgets them; a slot freed and
// referred to again since reads as live and is left out.
func (x *Index) TakeFreed(dst []core.LBA) []core.LBA {
	for _, slot := range x.freed {
		if x.Live(slot) == 0 && !slices.Contains(dst, slot) {
			dst = append(dst, slot)
		}
	}
	x.freed = x.freed[:0]
	return dst
}

func (x *Index) addLive(header core.LBA, delta int) int {
	n := int(x.Live(header)) + delta
	if n < 0 {
		panic(fmt.Sprintf("chunkstore: record at LBA %d refers to %d blocks", header, n))
	}
	k := x.recordKey(header)
	if n == 0 {
		x.must(x.tree.Delete(k))
		return 0
	}
	x.val = binary.LittleEndian.AppendUint32(x.val[:0], uint32(n))
	x.must(x.tree.Put(k, x.val))
	return n
}

// Live is how many blocks of the record at header, or of the chunk slot starting there, the live and frozen maps refer to, a block both refer to counting twice.
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

// Lookup returns the live extent holding block b of the chunk, a one-block extent of zeros at version 0 when the chunk has an entry and no extent holds b, or false when the chunk has no entry or only the absent version.
func (x *Index) Lookup(pool core.PoolID, c core.ChunkID, b uint16) (Extent, bool) {
	return x.lookup(pool, c, 0, b)
}

// lookup reads block b of map version with one floor.
func (x *Index) lookup(pool core.PoolID, c core.ChunkID, version uint64, b uint16) (Extent, bool) {
	k := x.extentKey(pool, c, version, b)
	fk, v, ok := x.tree.FloorPrefix(k)
	e, ok, entry := floorExtent(k, fk, v, ok, version, b)
	if !entry {
		return e, ok
	}
	if ch, ok := x.Chunk(pool, c); ok && ch.Version != 0 {
		return e, true
	}
	return Extent{}, false
}

// floorExtent reads block b of map version from fk and v, the floor of k, the block's extent key:
// an extent of that map, or, below its extents, the map's epoch for a frozen version and the
// chunk's entry or absent version for the live map. Below the absent version it reports entry,
// and the block reads as zeros when the chunk's entry has a version.
func floorExtent(k, fk, v []byte, ok bool, version uint64, b uint16) (Extent, bool, bool) {
	if !ok || !bytes.HasPrefix(fk, k[:chunkKeySize]) {
		return Extent{}, false, false
	}
	zeros := Extent{First: b, Count: 1}
	switch len(fk) {
	case KeySize:
		e, ok := decodeExtent(fk, v)
		if !ok || !bytes.HasPrefix(fk, k[:frozenKeySize]) {
			return Extent{}, false, false
		}
		if int(b) >= e.end() {
			return zeros, true, false
		}
		return e, true, false
	case frozenKeySize:
		if !bytes.Equal(fk, k[:frozenKeySize]) {
			return Extent{}, false, false
		}
		return zeros, version != 0, version == 0
	case chunkKeySize:
		if version != 0 || len(v) != 16 || binary.LittleEndian.Uint64(v) == 0 {
			return Extent{}, false, false
		}
		return zeros, true, false
	}
	return Extent{}, false, false
}

// LookupAt returns the extent holding block b of the chunk as the snapshot of epoch reads it.
func (x *Index) LookupAt(pool core.PoolID, c core.ChunkID, b uint16, epoch uint64) (Extent, bool) {
	if v, ok := x.FrozenAt(pool, c, epoch); ok {
		if v == 0 {
			return Extent{}, false
		}
		return x.lookup(pool, c, v, b)
	}
	return x.Lookup(pool, c, b)
}

// FrozenAt returns the frozen version a snapshot at epoch reads the chunk at, if any, 0 if the chunk was absent.
func (x *Index) FrozenAt(pool core.PoolID, c core.ChunkID, epoch uint64) (uint64, bool) {
	x.vers = x.frozen(x.vers[:0], pool, c)
	return frozenAt(x.vers, epoch)
}

// frozenAt picks from vers, as frozen lists them, the version a snapshot at epoch reads.
func frozenAt(vers []uint64, epoch uint64) (uint64, bool) {
	best, bestEpoch, found := uint64(0), uint64(0), false
	for i := 0; i < len(vers); i += 2 {
		if v, e := vers[i], vers[i+1]; e >= epoch && (!found || e < bestEpoch) {
			best, bestEpoch, found = v, e, true
		}
	}
	return best, found
}

func (x *Index) absent(pool core.PoolID, c core.ChunkID) bool {
	x.key = binary.BigEndian.AppendUint64(appendChunkPrefix(x.key[:0], pool, c), 0)
	_, ok := x.tree.Get(x.key)
	return ok
}

// frozen appends each frozen version of the chunk and its freeze epoch, the absent version 0 first.
func (x *Index) frozen(dst []uint64, pool core.PoolID, c core.ChunkID) []uint64 {
	x.tree.Iter(&x.it)
	dst, x.key = frozenIn(dst, &x.it, x.key, pool, c)
	return dst
}

// frozenIn is frozen through it, with key as its buffer, seeking from each map's first key to the
// next map's.
func frozenIn(dst []uint64, it *lsm.Iter, key []byte, pool core.PoolID, c core.ChunkID) ([]uint64, []byte) {
	key = binary.BigEndian.AppendUint64(appendChunkPrefix(key[:0], pool, c), 0)
	for ok := it.SeekPrefixGE(key); ok && bytes.HasPrefix(it.Key(), key[:chunkKeySize]); {
		k := it.Key()
		if len(k) < frozenKeySize {
			ok = it.Next()
			continue
		}
		v := binary.BigEndian.Uint64(k[chunkKeySize:])
		if len(k) == frozenKeySize {
			dst = append(dst, v, binary.LittleEndian.Uint64(it.Value()))
		}
		if v == math.MaxUint64 {
			break
		}
		key = binary.BigEndian.AppendUint64(key[:chunkKeySize], v+1)
		ok = it.SeekPrefixGE(key)
	}
	return dst, key
}

// Reader reads blocks of one chunk, ascending, until the index next changes; reuse it across reads.
type Reader struct {
	it      lsm.Iter
	key     []byte
	vers    []uint64
	pool    core.PoolID
	chunk   core.ChunkID
	version uint64
	absent  bool
}

// Open opens r on the chunk's live map, which its Lookup reads as Index.Lookup does.
func (x *Index) Open(r *Reader, pool core.PoolID, c core.ChunkID) {
	x.tree.Iter(&r.it)
	r.pool, r.chunk, r.version, r.absent = pool, c, 0, false
}

// OpenAt opens r on the chunk as the snapshot of epoch reads it, which its Lookup reads as Index.LookupAt does.
func (x *Index) OpenAt(r *Reader, pool core.PoolID, c core.ChunkID, epoch uint64) {
	x.Open(r, pool, c)
	r.vers, r.key = frozenIn(r.vers[:0], &r.it, r.key, pool, c)
	r.version, r.absent = frozenAt(r.vers, epoch)
	r.absent = r.absent && r.version == 0
}

// Lookup returns the extent holding block b of the map r was opened on.
func (r *Reader) Lookup(b uint16) (Extent, bool) {
	if r.absent {
		return Extent{}, false
	}
	r.key = AppendKey(r.key[:0], Key{Pool: r.pool, Chunk: r.chunk, Version: r.version, First: b})
	fk, v, ok := r.it.FloorPrefix(r.key)
	e, ok, entry := floorExtent(r.key, fk, v, ok, r.version, b)
	if !entry {
		return e, ok
	}
	if fk, v, ok := r.it.FloorPrefix(r.key[:chunkKeySize]); ok && len(fk) == chunkKeySize && len(v) == 16 && binary.LittleEndian.Uint64(v) != 0 {
		return e, true
	}
	return Extent{}, false
}

// Extents appends the extents of the chunk's map at version, 0 for the live map, by first block.
func (x *Index) Extents(dst []Extent, pool core.PoolID, c core.ChunkID, version uint64) []Extent {
	return x.overlapping(dst, pool, c, version, 0, core.BlocksPerChunk)
}

// Move repoints every map still storing the blocks at from to the cleaner's copy at to, and returns how many maps it changed.
func (x *Index) Move(pool core.PoolID, c core.ChunkID, first, count uint16, from, to, header core.LBA) int {
	x.vers = append(x.frozen(x.vers[:0], pool, c), 0, 0)
	moved := 0
	for i := 0; i < len(x.vers); i += 2 {
		v := x.vers[i]
		if v == 0 && i+2 < len(x.vers) {
			continue
		}
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
		mid.Count = count
		x.refer(mid, -int(count))
		mid.LBA, mid.Header = to, header
		x.put(x.extentKey(pool, c, v, first), mid)
		if end := int(first) + int(count); e.end() > end {
			x.put(x.extentKey(pool, c, v, uint16(end)), e.shifted(uint16(end)))
		}
		x.refer(mid, int(count))
		moved++
	}
	return moved
}

type recordCount struct {
	header core.LBA
	n      uint32
}

// Check verifies that no map's extents leave the chunk or share a block, that no freeze is newer than its chunk's entry, and that every record's and chunk slot's count is right.
func (x *Index) Check() error {
	x.check = x.check[:0]
	var stored []recordCount
	var prefix []byte
	var mapVersion, marker, chunkEpoch uint64
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
			chunkEpoch = binary.LittleEndian.Uint64(v[8:])
		case len(k) == frozenKeySize && bytes.HasPrefix(k, prefix) && len(prefix) > 0:
			if len(v) != 8 || binary.LittleEndian.Uint64(v) > chunkEpoch {
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
			if slot, ok := x.slotOf(e); ok {
				x.check = append(x.check, recordCount{slot, uint32(e.Count)})
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
