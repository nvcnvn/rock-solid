package disklog

import (
	"encoding/binary"
	"math"

	"github.com/nvcnvn/rock-solid/internal/core"
)

const (
	recordMagic = "RSRC"
	preludeSize = 56

	writeEntrySize    = 34
	copyEntrySize     = writeEntrySize + 8
	slotEntrySize     = writeEntrySize + 6
	freezeEntrySize   = 37
	blobEntrySize     = 14
	missingEntrySize  = 33
	decisionEntrySize = 17

	// lastFlag marks a write's, punch's or slot write's entry as the last of its version.
	lastFlag = 0x01

	recVersion      = 4
	recHeaderBlocks = 6
	recCRC          = 8
	recOffset       = 12
	recLogID        = 16
	recSeq          = 24
	recLSN          = 32
	recEntries      = 40
	recDataBlocks   = 44
	recDurable      = 48

	maxDataBytes = uint64(math.MaxUint32) * core.BlockSize
)

// EntryKind tags an Entry; zero is never a kind, so zero padding never reads as one.
type EntryKind uint8

// Entry kinds.
const (
	EntryWrite EntryKind = iota + 1
	EntryPunch
	EntryFreeze
	EntryBlob
	EntrySlot
	EntryCopy
	EntryMissing
	EntryDecision
	EntryRecovered
)

// BlobKind says what a blob payload holds; the codec carries it without checking it.
type BlobKind uint8

// Blob kinds.
const (
	BlobRaftEntries BlobKind = iota + 1
	BlobRaftState
	BlobRun
	BlobManifest
	BlobManifestPiece
)

// Entry is one item of a record header; only the fields of its kind are set.
type Entry struct {
	Kind     EntryKind
	Pool     core.PoolID
	Chunk    core.ChunkID
	Version  uint64
	First    uint16
	Count    uint16
	Epoch    uint64
	BlobKind BlobKind
	Owner    uint32
	Length   uint64
	Source   core.LBA
	Segment  uint32
	Slot     uint16
	// Last marks a write, punch or slot write as its version's last entry.
	Last     bool
	Interval uint32
}

// WriteEntry is a write of count blocks from block first of a chunk version.
func WriteEntry(pool core.PoolID, chunk core.ChunkID, version uint64, first, count uint16) Entry {
	return Entry{Kind: EntryWrite, Pool: pool, Chunk: chunk, Version: version, First: first, Count: count}
}

// CopyEntry is the cleaner's copy of count blocks from block first of a chunk version, read from source on, where the blocks were stored.
func CopyEntry(pool core.PoolID, chunk core.ChunkID, version uint64, first, count uint16, source core.LBA) Entry {
	return Entry{Kind: EntryCopy, Pool: pool, Chunk: chunk, Version: version, First: first, Count: count, Source: source}
}

// HasBlocks reports whether the entry carries chunk blocks in the record's data area: a write or a copy.
func (e *Entry) HasBlocks() bool { return e.Kind == EntryWrite || e.Kind == EntryCopy }

// SlotEntry is a write of a whole chunk version to chunk slot slot of segment, whose blocks lie there rather than in the record.
func SlotEntry(pool core.PoolID, chunk core.ChunkID, version uint64, segment uint32, slot uint16) Entry {
	return Entry{Kind: EntrySlot, Pool: pool, Chunk: chunk, Version: version, Count: core.BlocksPerChunk, Segment: segment, Slot: slot}
}

// PunchEntry frees count blocks from block first of a chunk version.
func PunchEntry(pool core.PoolID, chunk core.ChunkID, version uint64, first, count uint16) Entry {
	return Entry{Kind: EntryPunch, Pool: pool, Chunk: chunk, Version: version, First: first, Count: count}
}

// FreezeEntry pins a chunk version for the snapshot epoch.
func FreezeEntry(pool core.PoolID, chunk core.ChunkID, version, epoch uint64) Entry {
	return Entry{Kind: EntryFreeze, Pool: pool, Chunk: chunk, Version: version, Epoch: epoch}
}

// MissingEntry lists a chunk on a PG member's missing list: the member must receive version, which the peering of interval kept.
func MissingEntry(pool core.PoolID, chunk core.ChunkID, version uint64, interval uint32) Entry {
	return Entry{Kind: EntryMissing, Pool: pool, Chunk: chunk, Version: version, Interval: interval}
}

// RecoveredEntry ends a recovery write of a chunk in a PG's interval: the member holds the chunk whole at version, and a missing-list entry a peering of that interval or earlier made for the chunk is removed.
func RecoveredEntry(pool core.PoolID, chunk core.ChunkID, version uint64, interval uint32) Entry {
	return Entry{Kind: EntryRecovered, Pool: pool, Chunk: chunk, Version: version, Interval: interval}
}

// DecisionEntry marks the decision of the peering of interval persisted for PG pg of the pool's pgs PGs, held in Owner and Length.
func DecisionEntry(pool core.PoolID, pg, pgs, interval uint32) Entry {
	return Entry{Kind: EntryDecision, Pool: pool, Owner: pg, Length: uint64(pgs), Interval: interval}
}

// BlobEntry is a payload of length bytes of the given kind for owner.
func BlobEntry(kind BlobKind, owner uint32, length uint64) Entry {
	return Entry{Kind: EntryBlob, BlobKind: kind, Owner: owner, Length: length}
}

func (e *Entry) wireSize() (int, error) {
	var n int
	switch e.Kind {
	case EntryWrite, EntryPunch:
		if e.Count == 0 || int(e.First)+int(e.Count) > core.BlocksPerChunk {
			return 0, ErrBadExtent
		}
		n = writeEntrySize
	case EntryCopy:
		if e.Count == 0 || int(e.First)+int(e.Count) > core.BlocksPerChunk {
			return 0, ErrBadExtent
		}
		if e.Source == 0 {
			return 0, ErrMalformed
		}
		n = copyEntrySize
	case EntrySlot:
		if e.First != 0 || e.Count != core.BlocksPerChunk {
			return 0, ErrBadExtent
		}
		n = slotEntrySize
	case EntryFreeze:
		n = freezeEntrySize
	case EntryBlob:
		n = blobEntrySize
	case EntryMissing, EntryRecovered:
		n = missingEntrySize
	case EntryDecision:
		n = decisionEntrySize
	default:
		return 0, ErrMalformed
	}
	if !e.valid() || *e != e.ofKind() {
		return 0, ErrMalformed
	}
	return n, nil
}

// ofKind is the entry with only the fields its kind encodes.
func (e *Entry) ofKind() Entry {
	switch e.Kind {
	case EntryWrite, EntryPunch:
		return Entry{Kind: e.Kind, Pool: e.Pool, Chunk: e.Chunk, Version: e.Version, First: e.First, Count: e.Count, Last: e.Last}
	case EntryCopy:
		return CopyEntry(e.Pool, e.Chunk, e.Version, e.First, e.Count, e.Source)
	case EntrySlot:
		return Entry{Kind: e.Kind, Pool: e.Pool, Chunk: e.Chunk, Version: e.Version, First: e.First, Count: e.Count, Segment: e.Segment, Slot: e.Slot, Last: e.Last}
	case EntryFreeze:
		return FreezeEntry(e.Pool, e.Chunk, e.Version, e.Epoch)
	case EntryMissing:
		return MissingEntry(e.Pool, e.Chunk, e.Version, e.Interval)
	case EntryRecovered:
		return RecoveredEntry(e.Pool, e.Chunk, e.Version, e.Interval)
	case EntryDecision:
		return Entry{Kind: e.Kind, Pool: e.Pool, Owner: e.Owner, Length: e.Length, Interval: e.Interval}
	}
	return BlobEntry(e.BlobKind, e.Owner, e.Length)
}

// valid checks the fields a missing-list entry, recovery's end or decision marker carries: an
// interval, for recovery's end no earlier than its version's, and a PG among a power of two of
// them.
func (e *Entry) valid() bool {
	switch e.Kind {
	case EntryMissing:
		return e.Interval != 0
	case EntryRecovered:
		return e.Interval != 0 && uint32(e.Version>>32) <= e.Interval
	case EntryDecision:
		return e.Interval != 0 && e.Length != 0 && e.Length <= 1<<31 && e.Length&(e.Length-1) == 0 && uint64(e.Owner) < e.Length
	}
	return true
}

func (e *Entry) dataBytes() uint64 {
	switch e.Kind {
	case EntryWrite, EntryCopy:
		return uint64(e.Count) * core.BlockSize
	case EntryBlob:
		return e.Length
	}
	return 0
}

func (e *Entry) slotBlocks() uint64 {
	if e.Kind == EntrySlot {
		return uint64(e.Count)
	}
	return 0
}

// Place is where a record sits: its log, its segment's open sequence and its block offset in the segment.
type Place struct {
	LogID  uint64
	Seq    uint64
	Offset uint32
}

// Record identifies one record: its place, its log sequence number and the newest LSN durable when its round was built.
type Record struct {
	Place
	LSN     uint64
	Durable uint64
}

// Layout is the size of a record: its header bytes before padding, its header and data blocks, and the blocks its slot writes put in chunk slots, whose checksums follow the data blocks'.
type Layout struct {
	HeaderBytes  int
	HeaderBlocks uint32
	DataBlocks   uint32
	SlotBlocks   uint32
}

// Blocks is the length of the whole record.
func (l Layout) Blocks() uint64 { return uint64(l.HeaderBlocks) + uint64(l.DataBlocks) }

// LayoutOf sizes the record the entries describe and validates every entry.
func LayoutOf(entries []Entry) (Layout, error) {
	if uint64(len(entries)) > math.MaxUint32 {
		return Layout{}, ErrTooLarge
	}
	var acc layoutAcc
	for i := range entries {
		if err := acc.add(&entries[i]); err != nil {
			return Layout{}, err
		}
	}
	return acc.layout()
}

type layoutAcc struct {
	headerBytes uint64
	dataBytes   uint64
	slotBlocks  uint64
}

func (a *layoutAcc) add(e *Entry) error {
	n, err := e.wireSize()
	if err != nil {
		return err
	}
	add := e.dataBytes()
	if add > maxDataBytes-a.dataBytes {
		return ErrTooLarge
	}
	a.headerBytes += uint64(n)
	a.dataBytes += add
	a.slotBlocks += e.slotBlocks()
	return nil
}

func (a *layoutAcc) layout() (Layout, error) {
	dataBlocks := (a.dataBytes + core.BlockSize - 1) / core.BlockSize
	headerBytes := preludeSize + a.headerBytes + (dataBlocks+a.slotBlocks)*core.CRC32CSize
	headerBlocks := (headerBytes + core.BlockSize - 1) / core.BlockSize
	if headerBlocks > math.MaxUint16 {
		return Layout{}, ErrTooLarge
	}
	return Layout{HeaderBytes: int(headerBytes), HeaderBlocks: uint32(headerBlocks), DataBlocks: uint32(dataBlocks), SlotBlocks: uint32(a.slotBlocks)}, nil
}

// PayloadOffset is where entry i's payload starts in the data area: writes' and copies' blocks first, in order, then blobs' bytes.
func PayloadOffset(entries []Entry, i int) uint64 {
	var writeBytes, blobBytes uint64
	for j := range entries[:i] {
		switch {
		case entries[j].HasBlocks():
			writeBytes += entries[j].dataBytes()
		case entries[j].Kind == EntryBlob:
			blobBytes += entries[j].Length
		}
	}
	if entries[i].HasBlocks() {
		return writeBytes
	}
	for j := i; j < len(entries); j++ {
		if entries[j].HasBlocks() {
			writeBytes += entries[j].dataBytes()
		}
	}
	return writeBytes + blobBytes
}

// AppendHeader appends the record's header blocks; crcs holds one checksum per data block, in order, then one per block of each slot write, in entry order.
func AppendHeader(dst []byte, rec Record, entries []Entry, crcs []uint32) ([]byte, error) {
	lay, err := LayoutOf(entries)
	if err != nil {
		return dst, err
	}
	if uint64(len(crcs)) != uint64(lay.DataBlocks)+uint64(lay.SlotBlocks) || rec.Durable >= rec.LSN {
		return dst, ErrMalformed
	}
	start := len(dst)
	dst = appendPrelude(dst, rec, lay, len(entries))
	dst = appendEntries(dst, entries)
	for _, crc := range crcs {
		dst = binary.LittleEndian.AppendUint32(dst, crc)
	}
	return finishHeader(dst, start, lay), nil
}

// AppendRecord appends the header blocks and then data, which must be exactly the data area, of a record with no slot write.
func AppendRecord(dst []byte, rec Record, entries []Entry, data []byte) ([]byte, error) {
	lay, err := LayoutOf(entries)
	if err != nil {
		return dst, err
	}
	if uint64(len(data)) != uint64(lay.DataBlocks)*core.BlockSize || lay.SlotBlocks != 0 || rec.Durable >= rec.LSN {
		return dst, ErrMalformed
	}
	start := len(dst)
	dst = appendPrelude(dst, rec, lay, len(entries))
	dst = appendEntries(dst, entries)
	for i := 0; i < len(data); i += core.BlockSize {
		dst = core.AppendCRC32C(dst, data[i:i+core.BlockSize])
	}
	dst = finishHeader(dst, start, lay)
	return append(dst, data...), nil
}

func appendPrelude(dst []byte, rec Record, lay Layout, entries int) []byte {
	dst = appendZeros(dst, preludeSize)
	b := dst[len(dst)-preludeSize:]
	copy(b, recordMagic)
	binary.LittleEndian.PutUint16(b[recVersion:], FormatVersion)
	binary.LittleEndian.PutUint16(b[recHeaderBlocks:], uint16(lay.HeaderBlocks))
	binary.LittleEndian.PutUint32(b[recOffset:], rec.Offset)
	binary.LittleEndian.PutUint64(b[recLogID:], rec.LogID)
	binary.LittleEndian.PutUint64(b[recSeq:], rec.Seq)
	binary.LittleEndian.PutUint64(b[recLSN:], rec.LSN)
	binary.LittleEndian.PutUint32(b[recEntries:], uint32(entries))
	binary.LittleEndian.PutUint32(b[recDataBlocks:], lay.DataBlocks)
	binary.LittleEndian.PutUint64(b[recDurable:], rec.Durable)
	return dst
}

func appendEntries(dst []byte, entries []Entry) []byte {
	for i := range entries {
		e := &entries[i]
		dst = append(dst, byte(e.Kind))
		switch e.Kind {
		case EntryWrite, EntryPunch, EntrySlot, EntryCopy:
			dst = binary.LittleEndian.AppendUint32(dst, uint32(e.Pool))
			dst = e.Chunk.AppendTo(dst)
			dst = binary.LittleEndian.AppendUint64(dst, e.Version)
			dst = binary.LittleEndian.AppendUint16(dst, e.First)
			dst = binary.LittleEndian.AppendUint16(dst, e.Count)
			var flags byte
			if e.Last {
				flags = lastFlag
			}
			dst = append(dst, flags)
			switch e.Kind {
			case EntryCopy:
				dst = binary.LittleEndian.AppendUint64(dst, uint64(e.Source))
			case EntrySlot:
				dst = binary.LittleEndian.AppendUint32(dst, e.Segment)
				dst = binary.LittleEndian.AppendUint16(dst, e.Slot)
			}
		case EntryFreeze:
			dst = binary.LittleEndian.AppendUint32(dst, uint32(e.Pool))
			dst = e.Chunk.AppendTo(dst)
			dst = binary.LittleEndian.AppendUint64(dst, e.Version)
			dst = binary.LittleEndian.AppendUint64(dst, e.Epoch)
		case EntryBlob:
			dst = append(dst, byte(e.BlobKind))
			dst = binary.LittleEndian.AppendUint32(dst, e.Owner)
			dst = binary.LittleEndian.AppendUint64(dst, e.Length)
		case EntryMissing, EntryRecovered:
			dst = binary.LittleEndian.AppendUint32(dst, uint32(e.Pool))
			dst = e.Chunk.AppendTo(dst)
			dst = binary.LittleEndian.AppendUint64(dst, e.Version)
			dst = binary.LittleEndian.AppendUint32(dst, e.Interval)
		case EntryDecision:
			dst = binary.LittleEndian.AppendUint32(dst, uint32(e.Pool))
			dst = binary.LittleEndian.AppendUint32(dst, e.Owner)
			dst = binary.LittleEndian.AppendUint32(dst, uint32(e.Length))
			dst = binary.LittleEndian.AppendUint32(dst, e.Interval)
		}
	}
	return dst
}

func finishHeader(dst []byte, start int, lay Layout) []byte {
	end := start + int(lay.HeaderBlocks)*core.BlockSize
	dst = appendZeros(dst, end-len(dst))
	h := dst[start:end]
	core.PutCRC32C(h[recCRC:], headerCRC(h, recCRC))
	return dst
}

func decodeEntry(b []byte, e *Entry) (int, error) {
	if len(b) < 1 {
		return 0, ErrMalformed
	}
	*e = Entry{Kind: EntryKind(b[0])}
	switch e.Kind {
	case EntryWrite, EntryPunch, EntrySlot, EntryCopy:
		n := entrySize(e.Kind)
		if len(b) < n {
			return 0, ErrMalformed
		}
		e.Pool = core.PoolID(binary.LittleEndian.Uint32(b[1:]))
		e.Chunk = core.ChunkIDFromBytes([core.ChunkIDSize]byte(b[5:21]))
		e.Version = binary.LittleEndian.Uint64(b[21:])
		e.First = binary.LittleEndian.Uint16(b[29:])
		e.Count = binary.LittleEndian.Uint16(b[31:])
		if e.Count == 0 || int(e.First)+int(e.Count) > core.BlocksPerChunk {
			return 0, ErrBadExtent
		}
		if flags := b[33]; flags&^lastFlag != 0 || e.Kind == EntryCopy && flags != 0 {
			return 0, ErrMalformed
		}
		e.Last = b[33]&lastFlag != 0
		if e.Kind == EntryCopy {
			if e.Source = core.LBA(binary.LittleEndian.Uint64(b[34:])); e.Source == 0 {
				return 0, ErrMalformed
			}
		}
		if e.Kind == EntrySlot {
			e.Segment = binary.LittleEndian.Uint32(b[34:])
			e.Slot = binary.LittleEndian.Uint16(b[38:])
			if e.First != 0 || e.Count != core.BlocksPerChunk {
				return 0, ErrBadExtent
			}
			if e.Segment == 0 {
				return 0, ErrMalformed
			}
		}
		return n, nil
	case EntryFreeze:
		if len(b) < freezeEntrySize {
			return 0, ErrMalformed
		}
		e.Pool = core.PoolID(binary.LittleEndian.Uint32(b[1:]))
		e.Chunk = core.ChunkIDFromBytes([core.ChunkIDSize]byte(b[5:21]))
		e.Version = binary.LittleEndian.Uint64(b[21:])
		e.Epoch = binary.LittleEndian.Uint64(b[29:])
		return freezeEntrySize, nil
	case EntryBlob:
		if len(b) < blobEntrySize {
			return 0, ErrMalformed
		}
		e.BlobKind = BlobKind(b[1])
		e.Owner = binary.LittleEndian.Uint32(b[2:])
		e.Length = binary.LittleEndian.Uint64(b[6:])
		return blobEntrySize, nil
	case EntryMissing, EntryRecovered:
		if len(b) < missingEntrySize {
			return 0, ErrMalformed
		}
		e.Pool = core.PoolID(binary.LittleEndian.Uint32(b[1:]))
		e.Chunk = core.ChunkIDFromBytes([core.ChunkIDSize]byte(b[5:21]))
		e.Version = binary.LittleEndian.Uint64(b[21:])
		e.Interval = binary.LittleEndian.Uint32(b[29:])
	case EntryDecision:
		if len(b) < decisionEntrySize {
			return 0, ErrMalformed
		}
		e.Pool = core.PoolID(binary.LittleEndian.Uint32(b[1:]))
		e.Owner = binary.LittleEndian.Uint32(b[5:])
		e.Length = uint64(binary.LittleEndian.Uint32(b[9:]))
		e.Interval = binary.LittleEndian.Uint32(b[13:])
	default:
		return 0, ErrMalformed
	}
	if !e.valid() {
		return 0, ErrMalformed
	}
	return entrySize(e.Kind), nil
}

// RecordHeader is a decoded record header; its entries share the slice the decoder was given.
type RecordHeader struct {
	Record  Record
	Layout  Layout
	Entries []Entry
	crcs    []byte
}

// DataCRC is the stored checksum of data block i.
func (h *RecordHeader) DataCRC(i int) uint32 {
	return core.GetCRC32C(h.crcs[i*core.CRC32CSize:])
}

// CRCs appends every stored checksum: the data blocks', then each slot write's blocks', in entry order.
func (h *RecordHeader) CRCs(dst []uint32) []uint32 {
	for i := 0; i < len(h.crcs); i += core.CRC32CSize {
		dst = append(dst, core.GetCRC32C(h.crcs[i:]))
	}
	return dst
}

// SlotCRC is the stored checksum of block b of slot write entry i.
func (h *RecordHeader) SlotCRC(i int, b uint16) uint32 {
	at := uint64(h.Layout.DataBlocks)
	for j := range h.Entries[:i] {
		at += h.Entries[j].slotBlocks()
	}
	return core.GetCRC32C(h.crcs[(at+uint64(b))*core.CRC32CSize:])
}

// DecodeHeader verifies and decodes the header at the start of img, which must name place at and fit in blocksLeft.
func DecodeHeader(img []byte, at Place, blocksLeft uint32, entries []Entry) (RecordHeader, error) {
	if len(img) < len(recordMagic) {
		return RecordHeader{}, ErrTruncated
	}
	if string(img[:len(recordMagic)]) != recordMagic {
		return RecordHeader{}, ErrBadMagic
	}
	if len(img) < preludeSize {
		return RecordHeader{}, ErrTruncated
	}
	if binary.LittleEndian.Uint16(img[recVersion:]) != FormatVersion {
		return RecordHeader{}, ErrBadVersion
	}
	headerBlocks := uint32(binary.LittleEndian.Uint16(img[recHeaderBlocks:]))
	if headerBlocks == 0 {
		return RecordHeader{}, ErrMalformed
	}
	hlen := int(headerBlocks) * core.BlockSize
	if len(img) < hlen {
		return RecordHeader{}, ErrTruncated
	}
	h := img[:hlen]
	if core.GetCRC32C(h[recCRC:]) != headerCRC(h, recCRC) {
		return RecordHeader{}, ErrBadChecksum
	}
	rec := Record{
		Place: Place{
			LogID:  binary.LittleEndian.Uint64(h[recLogID:]),
			Seq:    binary.LittleEndian.Uint64(h[recSeq:]),
			Offset: binary.LittleEndian.Uint32(h[recOffset:]),
		},
		LSN:     binary.LittleEndian.Uint64(h[recLSN:]),
		Durable: binary.LittleEndian.Uint64(h[recDurable:]),
	}
	if rec.Durable >= rec.LSN {
		return RecordHeader{}, ErrMalformed
	}
	dataBlocks := binary.LittleEndian.Uint32(h[recDataBlocks:])
	if uint64(headerBlocks)+uint64(dataBlocks) > uint64(blocksLeft) {
		return RecordHeader{}, ErrBadLengthAtEnd
	}
	if rec.Place != at {
		return RecordHeader{}, ErrMismatch
	}
	count := binary.LittleEndian.Uint32(h[recEntries:])
	first := len(entries)
	pos := preludeSize
	for range count {
		var e Entry
		n, err := decodeEntry(h[pos:], &e)
		if err != nil {
			return RecordHeader{}, err
		}
		pos += n
		entries = append(entries, e)
	}
	lay, err := LayoutOf(entries[first:])
	if err != nil {
		return RecordHeader{}, err
	}
	if lay.HeaderBlocks != headerBlocks || lay.DataBlocks != dataBlocks {
		return RecordHeader{}, ErrMalformed
	}
	crcEnd := pos + int(dataBlocks+lay.SlotBlocks)*core.CRC32CSize
	if !isZero(h[crcEnd:]) {
		return RecordHeader{}, ErrMalformed
	}
	return RecordHeader{Record: rec, Layout: lay, Entries: entries[first:], crcs: h[pos:crcEnd]}, nil
}

// DecodedRecord is a record whose header and every data block were verified; a slot write's blocks lie outside it.
type DecodedRecord struct {
	RecordHeader
	Data []byte
}

// DecodeRecord decodes the record at the start of img as DecodeHeader does and checks every data block's checksum.
func DecodeRecord(img []byte, at Place, blocksLeft uint32, entries []Entry) (DecodedRecord, error) {
	h, err := DecodeHeader(img, at, blocksLeft, entries)
	if err != nil {
		return DecodedRecord{}, err
	}
	hlen := uint64(h.Layout.HeaderBlocks) * core.BlockSize
	total := h.Layout.Blocks() * core.BlockSize
	if uint64(len(img)) < total {
		return DecodedRecord{}, ErrTruncated
	}
	data := img[hlen:total]
	for i := range int(h.Layout.DataBlocks) {
		block := data[i*core.BlockSize : (i+1)*core.BlockSize]
		if core.CRC32C(block) != h.DataCRC(i) {
			return DecodedRecord{}, ErrBadChecksum
		}
	}
	return DecodedRecord{RecordHeader: h, Data: data}, nil
}

// Payload is entry i's bytes sliced from the image: a write's or copy's blocks or a blob's bytes, empty for any other entry.
func (d *DecodedRecord) Payload(i int) []byte {
	e := &d.Entries[i]
	switch e.Kind {
	case EntryWrite, EntryCopy, EntryBlob:
		start := PayloadOffset(d.Entries, i)
		return d.Data[start : start+e.dataBytes()]
	}
	return nil
}

// BlockCRC verifies the record header at the start of img, which must be logID's at offset in its segment, and returns the stored checksum of its data block i; when img holds less than the header, it returns ErrTruncated and the header's length in blocks.
func BlockCRC(img []byte, logID uint64, offset uint32, i uint32) (crc uint32, headerBlocks uint32, err error) {
	return storedCRC(img, logID, offset, func(entries []byte, n, dataBlocks uint32) (uint64, bool) {
		return uint64(i), i < dataBlocks
	})
}

// SlotBlockCRC is BlockCRC for block b of the record's write to slot slot of segment.
func SlotBlockCRC(img []byte, logID uint64, offset uint32, segment uint32, slot uint16, b uint16) (crc uint32, headerBlocks uint32, err error) {
	return storedCRC(img, logID, offset, func(entries []byte, n, dataBlocks uint32) (uint64, bool) {
		at := uint64(dataBlocks)
		for range n {
			if EntryKind(entries[0]) == EntrySlot {
				if binary.LittleEndian.Uint32(entries[34:]) == segment && binary.LittleEndian.Uint16(entries[38:]) == slot {
					return at + uint64(b), b < core.BlocksPerChunk
				}
				at += core.BlocksPerChunk
			}
			entries = entries[entrySize(EntryKind(entries[0])):]
		}
		return 0, false
	})
}

func entrySize(k EntryKind) int {
	switch k {
	case EntryWrite, EntryPunch:
		return writeEntrySize
	case EntrySlot:
		return slotEntrySize
	case EntryCopy:
		return copyEntrySize
	case EntryFreeze:
		return freezeEntrySize
	case EntryBlob:
		return blobEntrySize
	case EntryMissing, EntryRecovered:
		return missingEntrySize
	case EntryDecision:
		return decisionEntrySize
	}
	return 0
}

// storedCRC verifies the header as BlockCRC does and returns the checksum at the index pick
// finds from its n entries' bytes and its data block count.
func storedCRC(img []byte, logID uint64, offset uint32, pick func(entries []byte, n, dataBlocks uint32) (uint64, bool)) (uint32, uint32, error) {
	if len(img) < preludeSize {
		return 0, 0, ErrTruncated
	}
	if string(img[:len(recordMagic)]) != recordMagic {
		return 0, 0, ErrBadMagic
	}
	if binary.LittleEndian.Uint16(img[recVersion:]) != FormatVersion {
		return 0, 0, ErrBadVersion
	}
	headerBlocks := uint32(binary.LittleEndian.Uint16(img[recHeaderBlocks:]))
	if headerBlocks == 0 {
		return 0, 0, ErrMalformed
	}
	hlen := int(headerBlocks) * core.BlockSize
	if len(img) < hlen {
		return 0, headerBlocks, ErrTruncated
	}
	h := img[:hlen]
	if core.GetCRC32C(h[recCRC:]) != headerCRC(h, recCRC) {
		return 0, headerBlocks, ErrBadChecksum
	}
	if binary.LittleEndian.Uint64(h[recLogID:]) != logID || binary.LittleEndian.Uint32(h[recOffset:]) != offset {
		return 0, headerBlocks, ErrMismatch
	}
	n := binary.LittleEndian.Uint32(h[recEntries:])
	pos := preludeSize
	var crcs uint64
	for range n {
		size := 0
		if pos < len(h) {
			size = entrySize(EntryKind(h[pos]))
		}
		if size == 0 || pos+size > len(h) {
			return 0, headerBlocks, ErrMalformed
		}
		if EntryKind(h[pos]) == EntrySlot {
			crcs += core.BlocksPerChunk
		}
		pos += size
	}
	dataBlocks := binary.LittleEndian.Uint32(h[recDataBlocks:])
	crcs += uint64(dataBlocks)
	i, ok := pick(h[preludeSize:pos], n, dataBlocks)
	at := uint64(pos) + i*core.CRC32CSize
	if !ok || i >= crcs || at+core.CRC32CSize > uint64(len(h)) {
		return 0, headerBlocks, ErrMalformed
	}
	return core.GetCRC32C(h[at:]), headerBlocks, nil
}
