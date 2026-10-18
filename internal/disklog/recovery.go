package disklog

import (
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// Mode says where recovery starts replaying.
type Mode uint8

// Recovery modes.
const (
	// ModeOpen loads the newest manifest and replays the records after its oldest replay LSN.
	ModeOpen Mode = iota + 1
	// ModeRebuild ignores manifests and replays every record from the start of the log.
	ModeRebuild
)

var (
	// ErrNotFormatted reports a disk with no valid superblock of this format.
	ErrNotFormatted = errors.New("disk holds no superblock of this format")
	// ErrCopyOnly matches any CopyOnlyError with errors.Is.
	ErrCopyOnly = errors.New("only the superblock copy is valid")
	// ErrCorrupt matches any CorruptError with errors.Is.
	ErrCorrupt = errors.New("log corrupt")
	// ErrRecovering reports a recovery call made at the wrong step.
	ErrRecovering = errors.New("recovery is not at that step")
)

// CopyOnlyError reports a block 0 of zeros or another format next to a valid copy, which only an operator may restore.
type CopyOnlyError struct {
	Block0 error
	Copy   Superblock
}

func (e *CopyOnlyError) Error() string {
	return fmt.Sprintf("block 0 is not a superblock (%v) but the copy at LBA %d is valid", e.Block0, e.Copy.SuperblockCopyLBA())
}

// Is lets errors.Is match ErrCopyOnly.
func (e *CopyOnlyError) Is(target error) bool { return target == ErrCopyOnly }

// CorruptError reports a missing or damaged record that more records follow than rounds can be in flight, or that a durable manifest relies on.
type CorruptError struct {
	LSN uint64
}

func (e *CorruptError) Error() string { return fmt.Sprintf("log corrupt at LSN %d", e.LSN) }

// Is lets errors.Is match ErrCorrupt.
func (e *CorruptError) Is(target error) bool { return target == ErrCorrupt }

type recPhase uint8

const (
	recSuperblock recPhase = iota + 1
	recHeaders
	recFollow
	recQuarantine
	recManifest
	recLoad
	recReplay
	recErase
	recDone
	recFailed
	recSpent
)

type segState uint8

const (
	segFree segState = iota
	segUsed
	segSuspect
	segQuarantined
)

type segInfo struct {
	state    segState
	stream   Stream
	hasFirst bool
	seq      uint64
	firstLSN uint64
}

type foundHeader struct {
	segment uint32
	block   uint32
	lsn     uint64
}

type window struct {
	buf   core.IOBuf
	cap   uint32
	seg   uint32
	start uint32
	n     uint32
	busy  bool
	valid bool
}

type tailRecord struct {
	lsn uint64
	end uint32
}

type streamScan struct {
	segs      []uint32
	i         int
	pos       uint32
	searching bool
	lastLSN   uint64
	done      bool
	hasHead   bool
	head      DecodedRecord
	headSeg   uint32
	headBlock uint32
	ents      []Entry
	tail      []tailRecord
}

type ioKind uint8

const (
	ioBlock0 ioKind = iota + 1
	ioCopy
	ioRepair
	ioRepairSync
	ioHeader
	ioWindow
	ioErase
	ioEraseSync
)

type pendingIO struct {
	io   core.IOID
	kind ioKind
	slot int
}

type seekState uint8

const (
	seekPending seekState = iota
	seekFound
	seekEnd
)

// Recovery reads a disk's log back after a restart, finds where the durable log ends, and hands over to a Log that appends after it.
type Recovery struct {
	cfg        Config
	mode       Mode
	diskBlocks uint64
	nextIO     core.IOID
	phase      recPhase
	err        error
	pending    []pendingIO

	sb      Superblock
	b0      Superblock
	b0Err   error
	copyLBA core.LBA

	maxHeader uint32
	hdrBufs   []core.IOBuf
	slotSeg   []uint32
	nextSeg   uint32
	hdrsDone  uint32
	win       [2]window
	scratch   []Entry

	segs     []segInfo
	follow   []uint32
	fi       int
	suspects []uint32
	qi       int
	qPos     uint32

	manifestPos ManifestPos
	manifest    Manifest
	hasManifest bool
	pieces      []BlobRef
	pi          int
	pieceStep   bool
	rest        []byte
	body        []byte
	blobs       []BlobRef
	bi          int

	streams     [2]streamScan
	minReplay   uint64
	expected    uint64
	gapped      bool
	maxKnown    uint64
	maxSeq      uint64
	found       []foundHeader
	replayFloor uint64
	replayBytes uint64
	replayed    uint64

	durable     uint64
	erase       []foundHeader
	eraseLeft   int
	resume      [2]cursor
	quarantined []uint32
}

// Recover starts recovering the log on a disk of diskBlocks blocks, with IO IDs counting up from firstIO, taking its read buffers from the pool at once.
func Recover(ctx *core.Context, cfg Config, diskBlocks uint64, mode Mode, firstIO core.IOID, out *Output) (*Recovery, error) {
	maxRec := cfg.MaxRecordBytes / core.BlockSize
	if cfg.CommitDepth < 1 || cfg.ReadDepth < 1 || maxRec < 3 || (mode != ModeOpen && mode != ModeRebuild) || diskBlocks == 0 {
		return nil, ErrBadConfig
	}
	r := &Recovery{cfg: cfg, mode: mode, diskBlocks: diskBlocks, nextIO: firstIO, phase: recSuperblock}
	r.hdrBufs = make([]core.IOBuf, cfg.ReadDepth)
	r.slotSeg = make([]uint32, cfg.ReadDepth)
	winBlocks := uint32(min(maxRec, MaxSegmentBytes/core.BlockSize))
	r.maxHeader = winBlocks
	var err error
	for i := range r.hdrBufs {
		if r.hdrBufs[i], err = ctx.Pool.Alloc(2 * core.BlockSize); err != nil {
			break
		}
	}
	for i := range r.win {
		if err != nil {
			break
		}
		r.win[i].cap = winBlocks
		r.win[i].buf, err = ctx.Pool.Alloc(int(winBlocks) * core.BlockSize)
	}
	if err != nil {
		r.release(ctx)
		return nil, err
	}
	r.issue(ioBlock0, 0, core.Action{Kind: core.ActDiskRead, LBA: 0, Buf: r.hdrBufs[0], Len: core.BlockSize}, out)
	return r, nil
}

func (r *Recovery) release(ctx *core.Context) {
	for i, b := range r.hdrBufs {
		if b != core.NoBuf {
			ctx.Pool.Free(b)
			r.hdrBufs[i] = core.NoBuf
		}
	}
	for i := range r.win {
		if r.win[i].buf != core.NoBuf {
			ctx.Pool.Free(r.win[i].buf)
			r.win[i].buf = core.NoBuf
		}
	}
}

// Close returns recovery's buffers without resuming a log; it refuses while a read or write is in flight.
func (r *Recovery) Close(ctx *core.Context) error {
	if len(r.pending) > 0 {
		return ErrBusy
	}
	r.release(ctx)
	r.phase = recSpent
	return nil
}

// Err is the error recovery failed with, or nil.
func (r *Recovery) Err() error { return r.err }

// Superblock is the log's identity and geometry, once read.
func (r *Recovery) Superblock() Superblock { return r.sb }

// Manifest is the manifest recovery loaded, if any.
func (r *Recovery) Manifest() (Manifest, bool) { return r.manifest, r.hasManifest }

// ManifestPos is the position of the newest manifest a segment header names.
func (r *Recovery) ManifestPos() ManifestPos { return r.manifestPos }

// DurableLSN is the LSN of the newest durable record, once recovered.
func (r *Recovery) DurableLSN() uint64 { return r.durable }

// ReplayedBytes is the size of the records replay delivered.
func (r *Recovery) ReplayedBytes() uint64 { return r.replayed }

// Quarantined is the segments whose damaged header covers records before the replay point; group commit does not open them.
func (r *Recovery) Quarantined() []uint32 { return r.quarantined }

// Erased is the number of record headers past the durable end that recovery erased.
func (r *Recovery) Erased() int { return len(r.erase) }

func (r *Recovery) issue(kind ioKind, slot int, a core.Action, out *Output) {
	a.IO = r.nextIO
	r.nextIO++
	r.pending = append(r.pending, pendingIO{io: a.IO, kind: kind, slot: slot})
	out.Actions = append(out.Actions, a)
}

func (r *Recovery) fail(ctx *core.Context, err error, out *Output) {
	if r.phase == recFailed {
		return
	}
	r.phase, r.err = recFailed, err
	out.Events = append(out.Events, Event{Kind: RecoveryFailed, Err: err})
	if len(r.pending) == 0 {
		r.release(ctx)
	}
}

// OnEvent advances recovery on disk completions.
func (r *Recovery) OnEvent(ctx *core.Context, ev *core.Event, out *Output) {
	if ev.Kind != core.EvDiskDone {
		return
	}
	i := slices.IndexFunc(r.pending, func(p pendingIO) bool { return p.io == ev.IO })
	if i < 0 {
		return
	}
	p := r.pending[i]
	r.pending = slices.Delete(r.pending, i, i+1)
	if r.phase == recFailed || r.phase == recSpent {
		if r.phase == recFailed && len(r.pending) == 0 {
			r.release(ctx)
		}
		return
	}
	switch p.kind {
	case ioBlock0:
		r.onBlock0(ctx, ev.Err, out)
		return
	case ioCopy:
		r.onCopy(ctx, ev.Err, out)
		return
	}
	if ev.Err != core.DiskOK {
		r.fail(ctx, ev.Err, out)
		return
	}
	switch p.kind {
	case ioRepairSync:
		r.startHeaders(ctx, out)
	case ioHeader:
		r.onHeader(ctx, p.slot, out)
	case ioWindow:
		r.win[p.slot].busy = false
		r.win[p.slot].valid = true
		r.pump(ctx, out)
	case ioErase:
		if r.eraseLeft--; r.eraseLeft == 0 {
			r.issue(ioEraseSync, 0, core.Action{Kind: core.ActDiskFsync}, out)
		}
	case ioEraseSync:
		r.finish(out)
	}
}

func (r *Recovery) onBlock0(ctx *core.Context, derr core.DiskError, out *Output) {
	r.b0Err = nil
	if derr != core.DiskOK {
		r.b0Err = derr
	} else if sb, err := DecodeSuperblock(ctx.Pool.Bytes(r.hdrBufs[0])); err != nil {
		r.b0Err = err
	} else if sb.DiskBlocks() > r.diskBlocks {
		r.b0Err = ErrMalformed
	} else {
		r.b0 = sb
	}
	if r.b0Err == nil {
		r.copyLBA = r.b0.SuperblockCopyLBA()
	} else {
		geo, err := GeometryFor(r.diskBlocks * core.BlockSize)
		if err != nil {
			r.failSuperblock(ctx, ErrNotFormatted, out)
			return
		}
		r.copyLBA = geo.SuperblockCopyLBA()
	}
	r.issue(ioCopy, 0, core.Action{Kind: core.ActDiskRead, LBA: r.copyLBA, Buf: r.win[0].buf, Len: core.BlockSize}, out)
}

func (r *Recovery) failSuperblock(ctx *core.Context, err error, out *Output) {
	var derr core.DiskError
	if errors.As(r.b0Err, &derr) {
		err = derr
	}
	r.fail(ctx, err, out)
}

// onCopy applies the table of "Superblock repair" in docs/design.md.
func (r *Recovery) onCopy(ctx *core.Context, derr core.DiskError, out *Output) {
	cp, cerr := Superblock{}, error(derr)
	if derr == core.DiskOK {
		cp, cerr = DecodeSuperblock(ctx.Pool.Bytes(r.win[0].buf))
		if cerr == nil && (cp.SuperblockCopyLBA() != r.copyLBA || cp.DiskBlocks() > r.diskBlocks) {
			cerr = ErrMalformed
		}
	}
	var derr0 core.DiskError
	switch {
	case r.b0Err == nil:
		r.sb = r.b0
		if cerr != nil || cp != r.b0 {
			r.repair(ctx, r.copyLBA, out)
			return
		}
		r.startHeaders(ctx, out)
	case errors.As(r.b0Err, &derr0) || errors.Is(r.b0Err, ErrBadChecksum) || errors.Is(r.b0Err, ErrMalformed):
		if cerr != nil {
			r.failSuperblock(ctx, ErrNotFormatted, out)
			return
		}
		r.sb = cp
		r.repair(ctx, r.sb.SuperblockLBA(), out)
	case cerr == nil:
		r.fail(ctx, &CopyOnlyError{Block0: r.b0Err, Copy: cp}, out)
	default:
		r.fail(ctx, ErrNotFormatted, out)
	}
}

func (r *Recovery) repair(ctx *core.Context, lba core.LBA, out *Output) {
	if _, err := AppendSuperblock(ctx.Pool.Bytes(r.hdrBufs[0])[:0], r.sb); err != nil {
		panic(fmt.Sprintf("disklog: a decoded superblock does not encode: %v", err))
	}
	r.issue(ioRepair, 0, core.Action{Kind: core.ActDiskWrite, LBA: lba, Buf: r.hdrBufs[0], Len: core.BlockSize}, out)
	r.issue(ioRepairSync, 0, core.Action{Kind: core.ActDiskFsync, Linked: true}, out)
}

func (r *Recovery) startHeaders(ctx *core.Context, out *Output) {
	r.phase = recHeaders
	r.segs = make([]segInfo, r.sb.SegmentCount)
	r.nextSeg = 1
	for slot := range r.hdrBufs {
		r.readHeader(slot, out)
	}
}

func (r *Recovery) readHeader(slot int, out *Output) {
	if r.nextSeg >= r.sb.SegmentCount {
		return
	}
	s := r.nextSeg
	r.nextSeg++
	r.slotSeg[slot] = s
	r.issue(ioHeader, slot, core.Action{Kind: core.ActDiskRead, LBA: r.sb.LBA(s, 0), Buf: r.hdrBufs[slot], Len: 2 * core.BlockSize}, out)
}

func (r *Recovery) onHeader(ctx *core.Context, slot int, out *Output) {
	r.classify(r.slotSeg[slot], ctx.Pool.Bytes(r.hdrBufs[slot])[:2*core.BlockSize])
	r.readHeader(slot, out)
	if r.hdrsDone++; r.hdrsDone == r.sb.SegmentCount-1 {
		r.startFollow(ctx, out)
	}
}

// classify reads block 1 along with the header: it holds the record written with the header,
// which locates a used segment for replay and, in a free segment, is a round in flight whose
// header never landed.
func (r *Recovery) classify(s uint32, img []byte) {
	h, err := DecodeSegmentHeader(img)
	info := &r.segs[s]
	switch {
	case err == nil && h.LogID == r.sb.LogID && h.Index == s:
		*info = segInfo{state: segUsed, stream: h.Stream, seq: h.Seq}
		r.maxSeq = max(r.maxSeq, h.Seq)
		if h.Manifest.LSN > r.manifestPos.LSN {
			r.manifestPos = h.Manifest
		}
		r.checkFirst(s, img[core.BlockSize:], h.Seq, false)
	case isZero(img[:core.BlockSize]) || (err == nil && h.LogID != r.sb.LogID):
		*info = segInfo{state: segFree}
		r.checkFirst(s, img[core.BlockSize:], 0, true)
	default:
		*info = segInfo{state: segSuspect}
		r.suspects = append(r.suspects, s)
	}
}

func (r *Recovery) checkFirst(s uint32, img []byte, seq uint64, anySeq bool) {
	p, ok := r.candidate(img, 1, seq, anySeq)
	if !ok {
		return
	}
	if p.headerBlocks > 1 {
		r.follow = append(r.follow, s)
		return
	}
	h, err := DecodeHeader(img, Place{LogID: r.sb.LogID, Seq: p.seq, Offset: 1}, r.sb.SegmentBlocks-1, r.scratch[:0])
	if err == nil {
		r.noteFirst(s, h)
	}
}

func (r *Recovery) noteFirst(s uint32, h RecordHeader) {
	if info := &r.segs[s]; info.state == segUsed {
		info.hasFirst, info.firstLSN = true, h.Record.LSN
		return
	}
	r.noteFound(s, 1, h)
}

func (r *Recovery) noteFound(s, block uint32, h RecordHeader) {
	r.found = append(r.found, foundHeader{segment: s, block: block, lsn: h.Record.LSN})
	r.maxKnown = max(r.maxKnown, h.Record.LSN)
	r.maxSeq = max(r.maxSeq, h.Record.Seq)
}

type prelude struct {
	headerBlocks uint32
	logID        uint64
	seq          uint64
	offset       uint32
}

func peekRecord(b []byte) (prelude, bool) {
	if len(b) < preludeSize || string(b[:len(recordMagic)]) != recordMagic {
		return prelude{}, false
	}
	return prelude{
		headerBlocks: uint32(binary.LittleEndian.Uint16(b[recHeaderBlocks:])),
		logID:        binary.LittleEndian.Uint64(b[recLogID:]),
		seq:          binary.LittleEndian.Uint64(b[recSeq:]),
		offset:       binary.LittleEndian.Uint32(b[recOffset:]),
	}, true
}

func (r *Recovery) candidate(b []byte, block uint32, seq uint64, anySeq bool) (prelude, bool) {
	p, ok := peekRecord(b)
	ok = ok && p.logID == r.sb.LogID && p.offset == block && p.headerBlocks > 0 && p.headerBlocks <= r.maxHeader && (anySeq || p.seq == seq)
	return p, ok
}

func (r *Recovery) need(ctx *core.Context, w int, seg, pos, n uint32, out *Output) ([]byte, bool) {
	win := &r.win[w]
	left := r.sb.SegmentBlocks - pos
	n = min(n, left)
	if win.valid && win.seg == seg && win.start <= pos && pos+n <= win.start+win.n {
		return ctx.Pool.Bytes(win.buf)[(pos-win.start)*core.BlockSize : win.n*core.BlockSize], true
	}
	if win.busy || r.phase == recFailed {
		return nil, false
	}
	if n > win.cap {
		ctx.Pool.Free(win.buf)
		var err error
		if win.buf, err = ctx.Pool.Alloc(int(n) * core.BlockSize); err != nil {
			win.buf, win.cap = core.NoBuf, 0
			r.fail(ctx, err, out)
			return nil, false
		}
		win.cap = n
	}
	count := max(n, min(win.cap, left))
	*win = window{buf: win.buf, cap: win.cap, seg: seg, start: pos, n: count, busy: true}
	r.issue(ioWindow, w, core.Action{Kind: core.ActDiskRead, LBA: r.sb.LBA(seg, pos), Buf: win.buf, Len: uint64(count) * core.BlockSize}, out)
	return nil, false
}

func (r *Recovery) seek(ctx *core.Context, w int, seg, pos uint32, seq uint64, anySeq bool, ents []Entry, out *Output) (uint32, RecordHeader, seekState) {
	for pos < r.sb.SegmentBlocks {
		img, ok := r.need(ctx, w, seg, pos, 1, out)
		if !ok {
			return pos, RecordHeader{}, seekPending
		}
		end := r.win[w].start + r.win[w].n
		for b := pos; b < end; b++ {
			blk := img[(b-pos)*core.BlockSize:]
			p, ok := r.candidate(blk, b, seq, anySeq)
			if !ok {
				continue
			}
			if b+min(p.headerBlocks, r.sb.SegmentBlocks-b) > end {
				r.need(ctx, w, seg, b, p.headerBlocks, out)
				return b, RecordHeader{}, seekPending
			}
			h, err := DecodeHeader(blk, Place{LogID: r.sb.LogID, Seq: p.seq, Offset: b}, r.sb.SegmentBlocks-b, ents[:0])
			if err == nil {
				return b, h, seekFound
			}
		}
		pos = end
	}
	return pos, RecordHeader{}, seekEnd
}

func (r *Recovery) pump(ctx *core.Context, out *Output) {
	switch r.phase {
	case recFollow:
		r.pumpFollow(ctx, out)
	case recQuarantine:
		r.pumpQuarantine(ctx, out)
	case recManifest:
		r.pumpManifest(ctx, out)
	case recLoad:
		r.pumpBlobs(ctx, out)
	case recReplay:
		r.pumpReplay(ctx, out)
	}
}

func (r *Recovery) startFollow(ctx *core.Context, out *Output) {
	r.phase = recFollow
	r.pumpFollow(ctx, out)
}

func (r *Recovery) pumpFollow(ctx *core.Context, out *Output) {
	for r.fi < len(r.follow) {
		s := r.follow[r.fi]
		img, ok := r.need(ctx, 0, s, 1, 1, out)
		if !ok {
			return
		}
		p, _ := peekRecord(img)
		if img, ok = r.need(ctx, 0, s, 1, p.headerBlocks, out); !ok {
			return
		}
		h, err := DecodeHeader(img, Place{LogID: r.sb.LogID, Seq: p.seq, Offset: 1}, r.sb.SegmentBlocks-1, r.scratch[:0])
		if err == nil {
			r.noteFirst(s, h)
		}
		r.fi++
	}
	r.phase, r.qPos = recQuarantine, 1
	r.pumpQuarantine(ctx, out)
}

// pumpQuarantine only collects what suspect segments hold; whether each is free or
// quarantined depends on where replay finds the durable log ends.
func (r *Recovery) pumpQuarantine(ctx *core.Context, out *Output) {
	for r.qi < len(r.suspects) {
		s := r.suspects[r.qi]
		for r.qPos < r.sb.SegmentBlocks {
			b, h, st := r.seek(ctx, 0, s, r.qPos, 0, true, r.scratch, out)
			r.qPos = b
			if st == seekPending {
				return
			}
			if st == seekEnd {
				break
			}
			r.noteFound(s, b, h)
			r.qPos = b + uint32(h.Layout.Blocks())
		}
		r.qi++
		r.qPos = 1
	}
	r.phase = recManifest
	r.pumpManifest(ctx, out)
}

type readState uint8

const (
	readPending readState = iota
	readOK
	readBad
)

// readRecord trusts the record's own segment sequence only outside used segments, where the
// segment header that would give it is damaged.
func (r *Recovery) readRecord(ctx *core.Context, seg, block uint32, out *Output) (DecodedRecord, readState) {
	if seg == 0 || seg >= r.sb.SegmentCount || block == 0 || block >= r.sb.SegmentBlocks {
		return DecodedRecord{}, readBad
	}
	img, ok := r.need(ctx, 0, seg, block, 1, out)
	if !ok {
		return DecodedRecord{}, readPending
	}
	p, ok := r.candidate(img, block, 0, true)
	if !ok {
		return DecodedRecord{}, readBad
	}
	place := Place{LogID: r.sb.LogID, Seq: p.seq, Offset: block}
	if info := r.segs[seg]; info.state == segUsed {
		place.Seq = info.seq
	}
	left := r.sb.SegmentBlocks - block
	if img, ok = r.need(ctx, 0, seg, block, p.headerBlocks, out); !ok {
		return DecodedRecord{}, readPending
	}
	h, err := DecodeHeader(img, place, left, r.scratch[:0])
	if err != nil {
		return DecodedRecord{}, readBad
	}
	if img, ok = r.need(ctx, 0, seg, block, uint32(h.Layout.Blocks()), out); !ok {
		return DecodedRecord{}, readPending
	}
	d, err := DecodeRecord(img, place, left, r.scratch[:0])
	if err != nil {
		return DecodedRecord{}, readBad
	}
	return d, readOK
}

func (r *Recovery) readBlob(ctx *core.Context, ref BlobRef, out *Output) ([]byte, BlobKind, readState) {
	d, st := r.readRecord(ctx, ref.Segment, ref.Block, out)
	if st != readOK {
		return nil, 0, st
	}
	if d.Record.LSN != ref.LSN || int(ref.Entry) >= len(d.Entries) || d.Entries[ref.Entry].Kind != EntryBlob {
		return nil, 0, readBad
	}
	return d.Payload(int(ref.Entry)), d.Entries[ref.Entry].BlobKind, readOK
}

func (r *Recovery) pumpManifest(ctx *core.Context, out *Output) {
	pos := r.manifestPos
	if pos.IsNone() {
		r.loaded(out)
		return
	}
	corrupt := &CorruptError{LSN: pos.LSN}
	if !r.pieceStep {
		d, st := r.readRecord(ctx, pos.Segment, pos.Block, out)
		if st == readPending {
			return
		}
		i := slices.IndexFunc(d.Entries, func(e Entry) bool { return e.Kind == EntryBlob && e.BlobKind == BlobManifest })
		if st == readBad || d.Record.LSN != pos.LSN || i < 0 {
			r.fail(ctx, corrupt, out)
			return
		}
		earlier, rest, err := DecodeManifestPiece(d.Payload(i), nil)
		if err != nil {
			r.fail(ctx, corrupt, out)
			return
		}
		r.pieces, r.rest, r.pieceStep = earlier, slices.Clone(rest), true
	}
	for r.pi < len(r.pieces) {
		b, kind, st := r.readBlob(ctx, r.pieces[r.pi], out)
		if st == readPending {
			return
		}
		if st == readBad || kind != BlobManifestPiece {
			r.fail(ctx, corrupt, out)
			return
		}
		r.body = append(r.body, b...)
		r.pi++
	}
	m, err := DecodeManifest(append(r.body, r.rest...))
	if err != nil {
		r.fail(ctx, corrupt, out)
		return
	}
	r.manifest, r.hasManifest = m, true
	r.body, r.rest, r.pieces = nil, nil, nil
	r.loaded(out)
}

func (r *Recovery) loaded(out *Output) {
	r.phase = recLoad
	out.Events = append(out.Events, Event{Kind: ManifestLoaded, Manifest: r.manifestPos})
}

// ReadBlob reads the blob at ref, such as a run piece the manifest names, between ManifestLoaded and Replay; a BlobRead event delivers it.
func (r *Recovery) ReadBlob(ctx *core.Context, ref BlobRef, out *Output) error {
	if r.phase != recLoad {
		return r.stepErr()
	}
	r.blobs = append(r.blobs, ref)
	r.pumpBlobs(ctx, out)
	return nil
}

func (r *Recovery) stepErr() error {
	if r.err != nil {
		return r.err
	}
	return ErrRecovering
}

func (r *Recovery) pumpBlobs(ctx *core.Context, out *Output) {
	for r.bi < len(r.blobs) {
		ref := r.blobs[r.bi]
		b, _, st := r.readBlob(ctx, ref, out)
		if st == readPending {
			return
		}
		if st == readBad {
			r.fail(ctx, &CorruptError{LSN: ref.LSN}, out)
			return
		}
		r.bi++
		out.emitBlob(ref, b)
	}
}

// Replay delivers the records after the manifest's oldest replay LSN, or all of them without one, as Replayed events in LSN order, then Recovered.
func (r *Recovery) Replay(ctx *core.Context, out *Output) error {
	if r.phase != recLoad {
		return r.stepErr()
	}
	if r.bi < len(r.blobs) {
		return ErrBusy
	}
	r.phase = recReplay
	switch {
	case r.mode == ModeRebuild:
	case r.hasManifest && len(r.manifest.Indexes) > 0:
		r.minReplay = r.manifest.Indexes[0].ReplayLSN
		for _, ix := range r.manifest.Indexes[1:] {
			r.minReplay = min(r.minReplay, ix.ReplayLSN)
		}
	case r.hasManifest:
		r.minReplay = r.manifest.BaseLSN
	}
	r.expected = r.minReplay + 1
	r.replayFloor = r.manifestPos.LSN
	if r.hasManifest {
		r.replayFloor = r.manifest.BaseLSN
	}
	for s := range r.streams {
		r.streams[s] = streamScan{segs: r.replaySegments(Stream(s)), pos: 1, ents: r.streams[s].ents}
	}
	r.pumpReplay(ctx, out)
	return nil
}

func (r *Recovery) replaySegments(s Stream) []uint32 {
	var segs []uint32
	for i := range r.segs {
		if r.segs[i].state == segUsed && r.segs[i].stream == s {
			segs = append(segs, uint32(i))
		}
	}
	slices.SortFunc(segs, func(a, b uint32) int {
		return cmp.Or(cmp.Compare(r.segs[a].seq, r.segs[b].seq), cmp.Compare(a, b))
	})
	start := 0
	for i, seg := range segs {
		if info := r.segs[seg]; info.hasFirst && info.firstLSN <= r.expected {
			start = i
		}
	}
	return segs[start:]
}

// pumpReplay stops delivering at the first missing LSN but scans both streams to their end,
// since the tail rule needs every record header past it.
func (r *Recovery) pumpReplay(ctx *core.Context, out *Output) {
	for r.phase == recReplay {
		for s := range r.streams {
			r.advance(ctx, s, out)
		}
		a, b := &r.streams[Hot], &r.streams[Cold]
		if (!a.hasHead && !a.done) || (!b.hasHead && !b.done) || r.phase != recReplay {
			return
		}
		if a.done && b.done {
			r.endReplay(ctx, out)
			return
		}
		x := a
		if !a.hasHead || (b.hasHead && b.head.Record.LSN < a.head.Record.LSN) {
			x = b
		}
		lsn := x.head.Record.LSN
		switch {
		case !r.gapped && lsn == r.expected:
			r.deliver(x, out)
			r.expected++
		case !r.gapped && lsn < r.expected:
			r.fail(ctx, &CorruptError{LSN: lsn}, out)
			return
		default:
			r.gapped = true
			r.found = append(r.found, foundHeader{segment: x.headSeg, block: x.headBlock, lsn: lsn})
		}
		x.hasHead = false
	}
}

// advance searches the rest of a segment after a block that holds no valid record, so damage
// cannot hide the records behind it.
func (r *Recovery) advance(ctx *core.Context, s int, out *Output) {
	st := &r.streams[s]
	for !st.hasHead && !st.done && r.phase == recReplay {
		if st.i >= len(st.segs) {
			st.done = true
			return
		}
		seg := st.segs[st.i]
		seq := r.segs[seg].seq
		if st.pos >= r.sb.SegmentBlocks {
			st.i, st.pos, st.searching = st.i+1, 1, false
			continue
		}
		if st.searching {
			b, _, state := r.seek(ctx, s, seg, st.pos, seq, false, st.ents, out)
			st.pos = b
			if state == seekPending {
				return
			}
			st.searching = false
			continue
		}
		pos, left := st.pos, r.sb.SegmentBlocks-st.pos
		place := Place{LogID: r.sb.LogID, Seq: seq, Offset: pos}
		img, ok := r.need(ctx, s, seg, pos, 1, out)
		if !ok {
			return
		}
		p, ok := r.candidate(img, pos, seq, false)
		if !ok {
			st.searching, st.pos = true, pos+1
			continue
		}
		if img, ok = r.need(ctx, s, seg, pos, p.headerBlocks, out); !ok {
			return
		}
		h, err := DecodeHeader(img, place, left, st.ents[:0])
		if err != nil {
			st.searching, st.pos = true, pos+1
			continue
		}
		n := uint32(h.Layout.Blocks())
		if img, ok = r.need(ctx, s, seg, pos, n, out); !ok {
			return
		}
		d, err := DecodeRecord(img, place, left, st.ents[:0])
		lsn := h.Record.LSN
		r.maxKnown = max(r.maxKnown, lsn)
		if seg == st.segs[len(st.segs)-1] {
			st.tail = append(st.tail, tailRecord{lsn: lsn, end: pos + n})
		}
		st.pos = pos + n
		switch {
		case err != nil || lsn <= st.lastLSN:
			r.found = append(r.found, foundHeader{segment: seg, block: pos, lsn: lsn})
		case lsn <= r.minReplay:
			st.lastLSN = lsn
		default:
			st.lastLSN = lsn
			st.head, st.headSeg, st.headBlock, st.hasHead = d, seg, pos, true
		}
	}
}

func (r *Recovery) deliver(x *streamScan, out *Output) {
	d := &x.head
	ev := Event{
		Kind:         Replayed,
		LSN:          d.Record.LSN,
		Segment:      x.headSeg,
		Block:        x.headBlock,
		LBA:          r.sb.LBA(x.headSeg, x.headBlock),
		HeaderBlocks: d.Layout.HeaderBlocks,
	}
	out.emitRecord(ev, d)
	blocks := d.Layout.Blocks()
	r.replayed += blocks * core.BlockSize
	if d.Record.LSN > r.replayFloor {
		r.replayBytes += blocks * core.BlockSize
	}
}

// endReplay applies the rules of "Recovery" in docs/design.md. A segment header fits in one
// sector, so a torn write leaves it whole or as it was, and a damaged one over records past
// the replay point is damage. The first missing LSN g ends the log only if every record header
// found past it lies within the commit_depth rounds that can be in flight, and no durable
// manifest lies past it.
func (r *Recovery) endReplay(ctx *core.Context, out *Output) {
	var damaged uint64
	for _, f := range r.found {
		if r.segs[f.segment].state != segSuspect && r.segs[f.segment].state != segQuarantined {
			continue
		}
		if f.lsn > r.minReplay && (damaged == 0 || f.lsn < damaged) {
			damaged = f.lsn
		}
		r.segs[f.segment].state = segQuarantined
	}
	g := r.expected
	switch {
	case damaged != 0:
		r.fail(ctx, &CorruptError{LSN: damaged}, out)
		return
	case r.maxKnown >= g+uint64(r.cfg.CommitDepth) || r.manifestPos.LSN >= g:
		r.fail(ctx, &CorruptError{LSN: g}, out)
		return
	}
	r.durable = g - 1
	for _, s := range r.suspects {
		if r.segs[s].state == segQuarantined {
			r.quarantined = append(r.quarantined, s)
		} else {
			r.segs[s].state = segFree
		}
	}
	slices.Sort(r.quarantined)
	for s := range r.streams {
		st := &r.streams[s]
		if len(st.segs) == 0 {
			continue
		}
		seg := st.segs[len(st.segs)-1]
		c := cursor{segment: seg, seq: r.segs[seg].seq, next: 1}
		for _, t := range st.tail {
			if t.lsn <= r.durable {
				c.next = max(c.next, t.end)
			}
		}
		r.resume[s] = c
	}
	for _, f := range r.found {
		if f.lsn > r.durable {
			r.erase = append(r.erase, f)
		}
	}
	r.phase = recErase
	if len(r.erase) == 0 {
		r.issue(ioEraseSync, 0, core.Action{Kind: core.ActDiskFsync}, out)
		return
	}
	zero := r.hdrBufs[0]
	clear(ctx.Pool.Bytes(zero))
	r.eraseLeft = len(r.erase)
	for _, f := range r.erase {
		r.issue(ioErase, 0, core.Action{Kind: core.ActDiskWrite, LBA: r.sb.LBA(f.segment, f.block), Buf: zero, Len: core.BlockSize}, out)
	}
}

func (r *Recovery) finish(out *Output) {
	r.phase = recDone
	out.Events = append(out.Events, Event{Kind: Recovered, LSN: r.durable})
}

// Log returns recovery's buffers and resumes group commit after the durable log, each stream appending to its newest segment.
func (r *Recovery) Log(ctx *core.Context) (*Log, error) {
	if r.phase != recDone {
		return nil, r.stepErr()
	}
	r.release(ctx)
	r.phase = recSpent
	l, err := newLog(ctx, r.cfg, r.sb, r.nextIO)
	if err != nil {
		return nil, err
	}
	l.phase = formatDone
	l.nextLSN = r.durable + 1
	l.durableLSN = r.durable
	l.nextSeq = r.maxSeq + 1
	l.manifest = r.manifestPos
	l.replayBytes = r.replayBytes
	l.asm = r.resume
	for s := uint32(1); s < r.sb.SegmentCount; s++ {
		if r.segs[s].state == segFree {
			l.free[s/64] |= 1 << (s % 64)
		}
	}
	return l, nil
}
