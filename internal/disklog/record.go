package disklog

import (
	"encoding/binary"
	"math"

	"github.com/nvcnvn/rock-solid/internal/core"
)

const (
	recordMagic = "RSRC"
	preludeSize = 56

	writeEntrySize  = 33
	freezeEntrySize = 37
	blobEntrySize   = 14

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
}

// WriteEntry is a write of count blocks from block first of a chunk version.
func WriteEntry(pool core.PoolID, chunk core.ChunkID, version uint64, first, count uint16) Entry {
	return Entry{Kind: EntryWrite, Pool: pool, Chunk: chunk, Version: version, First: first, Count: count}
}

// PunchEntry frees count blocks from block first of a chunk version.
func PunchEntry(pool core.PoolID, chunk core.ChunkID, version uint64, first, count uint16) Entry {
	return Entry{Kind: EntryPunch, Pool: pool, Chunk: chunk, Version: version, First: first, Count: count}
}

// FreezeEntry pins a chunk version for the snapshot epoch.
func FreezeEntry(pool core.PoolID, chunk core.ChunkID, version, epoch uint64) Entry {
	return Entry{Kind: EntryFreeze, Pool: pool, Chunk: chunk, Version: version, Epoch: epoch}
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
	case EntryFreeze:
		n = freezeEntrySize
	case EntryBlob:
		n = blobEntrySize
	default:
		return 0, ErrMalformed
	}
	if *e != e.ofKind() {
		return 0, ErrMalformed
	}
	return n, nil
}

// ofKind is the entry with only the fields its kind encodes.
func (e *Entry) ofKind() Entry {
	switch e.Kind {
	case EntryWrite, EntryPunch:
		return Entry{Kind: e.Kind, Pool: e.Pool, Chunk: e.Chunk, Version: e.Version, First: e.First, Count: e.Count}
	case EntryFreeze:
		return FreezeEntry(e.Pool, e.Chunk, e.Version, e.Epoch)
	}
	return BlobEntry(e.BlobKind, e.Owner, e.Length)
}

func (e *Entry) dataBytes() uint64 {
	switch e.Kind {
	case EntryWrite:
		return uint64(e.Count) * core.BlockSize
	case EntryBlob:
		return e.Length
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

// Layout is the size of a record: its header bytes before padding, and its header and data blocks.
type Layout struct {
	HeaderBytes  int
	HeaderBlocks uint32
	DataBlocks   uint32
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
	return nil
}

func (a *layoutAcc) layout() (Layout, error) {
	dataBlocks := (a.dataBytes + core.BlockSize - 1) / core.BlockSize
	headerBytes := preludeSize + a.headerBytes + dataBlocks*core.CRC32CSize
	headerBlocks := (headerBytes + core.BlockSize - 1) / core.BlockSize
	if headerBlocks > math.MaxUint16 {
		return Layout{}, ErrTooLarge
	}
	return Layout{HeaderBytes: int(headerBytes), HeaderBlocks: uint32(headerBlocks), DataBlocks: uint32(dataBlocks)}, nil
}

// PayloadOffset is where entry i's payload starts in the data area: writes' blocks first, in order, then blobs' bytes.
func PayloadOffset(entries []Entry, i int) uint64 {
	var writeBytes, blobBytes uint64
	for j := range entries[:i] {
		switch entries[j].Kind {
		case EntryWrite:
			writeBytes += entries[j].dataBytes()
		case EntryBlob:
			blobBytes += entries[j].Length
		}
	}
	if entries[i].Kind == EntryWrite {
		return writeBytes
	}
	for j := i; j < len(entries); j++ {
		if entries[j].Kind == EntryWrite {
			writeBytes += entries[j].dataBytes()
		}
	}
	return writeBytes + blobBytes
}

// AppendHeader appends the record's header blocks; crcs holds one checksum per data block, in order.
func AppendHeader(dst []byte, rec Record, entries []Entry, crcs []uint32) ([]byte, error) {
	lay, err := LayoutOf(entries)
	if err != nil {
		return dst, err
	}
	if uint64(len(crcs)) != uint64(lay.DataBlocks) || rec.Durable >= rec.LSN {
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

// AppendRecord appends the record's header blocks and then data, which must be exactly its data area.
func AppendRecord(dst []byte, rec Record, entries []Entry, data []byte) ([]byte, error) {
	lay, err := LayoutOf(entries)
	if err != nil {
		return dst, err
	}
	if uint64(len(data)) != uint64(lay.DataBlocks)*core.BlockSize || rec.Durable >= rec.LSN {
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
		case EntryWrite, EntryPunch:
			dst = binary.LittleEndian.AppendUint32(dst, uint32(e.Pool))
			dst = e.Chunk.AppendTo(dst)
			dst = binary.LittleEndian.AppendUint64(dst, e.Version)
			dst = binary.LittleEndian.AppendUint16(dst, e.First)
			dst = binary.LittleEndian.AppendUint16(dst, e.Count)
		case EntryFreeze:
			dst = binary.LittleEndian.AppendUint32(dst, uint32(e.Pool))
			dst = e.Chunk.AppendTo(dst)
			dst = binary.LittleEndian.AppendUint64(dst, e.Version)
			dst = binary.LittleEndian.AppendUint64(dst, e.Epoch)
		case EntryBlob:
			dst = append(dst, byte(e.BlobKind))
			dst = binary.LittleEndian.AppendUint32(dst, e.Owner)
			dst = binary.LittleEndian.AppendUint64(dst, e.Length)
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
	case EntryWrite, EntryPunch:
		if len(b) < writeEntrySize {
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
		return writeEntrySize, nil
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
	}
	return 0, ErrMalformed
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
	crcEnd := pos + int(dataBlocks)*core.CRC32CSize
	if !isZero(h[crcEnd:]) {
		return RecordHeader{}, ErrMalformed
	}
	return RecordHeader{Record: rec, Layout: lay, Entries: entries[first:], crcs: h[pos:crcEnd]}, nil
}

// DecodedRecord is a record whose header and every data block were verified.
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

// Payload is entry i's bytes sliced from the image: a write's blocks or a blob's bytes, empty for a punch or freeze.
func (d *DecodedRecord) Payload(i int) []byte {
	e := &d.Entries[i]
	switch e.Kind {
	case EntryWrite, EntryBlob:
		start := PayloadOffset(d.Entries, i)
		return d.Data[start : start+e.dataBytes()]
	}
	return nil
}
