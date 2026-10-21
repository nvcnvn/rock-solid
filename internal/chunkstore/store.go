package chunkstore

import (
	"cmp"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/lsm"
)

var (
	// ErrRange reports an empty byte range or one running past the end of its chunk.
	ErrRange = errors.New("chunkstore: byte range empty or outside the chunk")
	// ErrVersion reports an expectVer other than the chunk's newest ordered version.
	ErrVersion = errors.New("chunkstore: chunk is not at the expected version")
	// ErrNoVersion reports an atVer that is neither the chunk's committed version nor one a snapshot froze.
	ErrNoVersion = errors.New("chunkstore: no such version to read")
	// ErrInDoubt reports a write whose record was in flight when the disk failed; recovery decides whether it is durable.
	ErrInDoubt = errors.New("chunkstore: write in doubt")
	// ErrUnavailable reports an operation while the store recovers, rebuilds its index or after its log failed.
	ErrUnavailable = errors.New("chunkstore: store unavailable")
	// ErrSeal reports a Seal, which the single-node store does not do.
	ErrSeal = errors.New("chunkstore: seal not supported")
)

const (
	indexOwnerID uint32 = 1
	fetchIOBit          = 1 << 63
)

// Config is a store's settings.
type Config struct {
	// Log is the disk log's group commit settings.
	Log disklog.Config
	// Pool is the pool every chunk of this store belongs to.
	Pool core.PoolID
	// Check makes the index's tree verify every piece and merge it builds.
	Check bool
}

// Request is a Write, Punch or Read of a chunk's bytes [Off, Off+Len).
type Request struct {
	ID    uint64
	Chunk core.ChunkID
	Off   uint32
	Len   uint32
	// Buf holds a write's bytes laid out from the start of the block Off falls in, exactly the blocks the range touches; the store takes it.
	Buf core.IOBuf
	// HasVer makes Ver a write's or punch's expectVer, or a read's atVer.
	HasVer bool
	Ver    uint64
	// Epoch is the snapshot epoch of the chunk's volume, which a write or punch carries.
	Epoch uint64
}

// Reply completes the request ID: the version written or read, and a read's blocks in Buf laid out as a write's, which the caller frees.
type Reply struct {
	ID      uint64
	Version uint64
	Buf     core.IOBuf
	Err     error
}

// Output is the actions and replies of calls into the store, reused by the caller.
type Output struct {
	Actions []core.Action
	Replies []Reply
}

// Reset empties the output for reuse.
func (o *Output) Reset() {
	clear(o.Replies)
	o.Actions, o.Replies = o.Actions[:0], o.Replies[:0]
}

type phase uint8

const (
	recovering phase = iota
	serving
	draining
	failed
)

type opKind uint8

const (
	opWrite opKind = iota + 1
	opPunch
	opRead
)

type itemState uint8

const (
	itemQueued itemState = iota
	itemDurable
	itemFailed
)

type item struct {
	entry        disklog.Entry
	buf          core.IOBuf
	ticket       disklog.Ticket
	state        itemState
	lba          core.LBA
	headerBlocks uint32
	payload      uint64
	lsn          uint64
}

type op struct {
	kind      opKind
	req       Request
	seq       uint64
	chunk     int32
	version   uint64
	prevVer   uint64
	prevEpoch uint64
	based     bool
	edges     [2]core.IOBuf
	items     []item
	left      int
	reads     int
	buf       core.IOBuf
	err       error
}

// chunkState is a chunk with operations not yet answered: those ordered, in version order, and those waiting behind a base read.
type chunkState struct {
	live    bool
	id      core.ChunkID
	version uint64
	epoch   uint64
	pending []int32
	waiting []int32
}

type sentItem struct {
	ticket disklog.Ticket
	op     int32
	item   int32
}

type fetch struct {
	op      int32
	first   uint16
	n       uint16
	lba     core.LBA
	header  core.LBA
	data    core.IOBuf
	hdr     core.IOBuf
	hdrLen  uint32
	waiting int
	err     error
}

type flushWait struct {
	id     uint64
	seq    uint64
	chunks []core.ChunkID
}

type loadingRun struct {
	refs []disklog.BlobRef
	got  map[disklog.BlobRef][]byte
	root []byte
}

type plants struct {
	mergeCommitted bool
	failCheck      bool
	failMerge      bool
}

// Store is the single-node ChunkStore of one disk: its log, its chunk index and the operations on them.
type Store struct {
	cfg      Config
	phase    phase
	err      error
	log      *disklog.Log
	rec      *disklog.Recovery
	mode     disklog.Mode
	lout     disklog.Output
	ix       *Index
	sb       disklog.Superblock
	maxWrite uint16
	interval uint32
	seq      uint64
	logGen   uint64

	ops        []op
	freeOps    []int32
	chunks     []chunkState
	freeChunks []int32
	chunkOf    map[core.ChunkID]int32
	sent       []sentItem
	sentHead   int
	fetches    []fetch
	freeFetch  []int32
	flushes    []flushWait

	checkpointing bool
	piece         disklog.Ticket
	inPiece       bool
	piecesOut     int
	replayLSN     uint64
	retryAfter    bool
	// pieceWait is a flush whose next piece waits for a pool buffer, tried again on every call.
	pieceWait bool

	loading map[disklog.BlobRef]*loadingRun
	pieceOf map[disklog.BlobRef]disklog.BlobRef
	freed   []core.LBA

	apply   disklog.Event
	entry   [1]disklog.Entry
	payload [1]uint64

	plant plants
}

func newStore(cfg Config) *Store {
	if cfg.Pool == RecordPool {
		panic("chunkstore: a store of the index's reserved pool")
	}
	return &Store{cfg: cfg, interval: 1, chunkOf: map[core.ChunkID]int32{}}
}

// Format starts a store on an empty disk.
func Format(ctx *core.Context, cfg Config, sb disklog.Superblock, out *Output) (*Store, error) {
	s := newStore(cfg)
	log, err := disklog.Format(ctx, cfg.Log, sb, s.logIO(), &s.lout)
	if err != nil {
		return nil, err
	}
	if err := s.resume(log); err != nil {
		return nil, err
	}
	s.pump(ctx, out)
	return s, nil
}

// Open starts recovering a store from its disk of diskBlocks blocks; it serves once Serving says so.
func Open(ctx *core.Context, cfg Config, diskBlocks uint64, out *Output) (*Store, error) {
	s := newStore(cfg)
	if err := s.recover(ctx, diskBlocks, disklog.ModeOpen); err != nil {
		return nil, err
	}
	s.pump(ctx, out)
	return s, nil
}

// Serving reports whether the store takes operations.
func (s *Store) Serving() bool { return s.phase == serving }

// Err is why the store stopped serving for good, or nil.
func (s *Store) Err() error { return s.err }

// Index is the store's chunk index, nil while it recovers.
func (s *Store) Index() *Index { return s.ix }

// Log is the store's disk log, nil while it recovers.
func (s *Store) Log() *disklog.Log { return s.log }

func (s *Store) logIO() core.IOID {
	s.logGen++
	return core.IOID(s.logGen << 40)
}

func (s *Store) recover(ctx *core.Context, diskBlocks uint64, mode disklog.Mode) error {
	s.log, s.ix, s.mode, s.replayLSN = nil, nil, mode, 0
	s.loading, s.pieceOf = map[disklog.BlobRef]*loadingRun{}, map[disklog.BlobRef]disklog.BlobRef{}
	rec, err := disklog.Recover(ctx, s.cfg.Log, diskBlocks, mode, s.logIO(), &s.lout)
	if err != nil {
		return err
	}
	s.rec, s.phase = rec, recovering
	return nil
}

func (s *Store) resume(log *disklog.Log) error {
	s.log, s.rec, s.sb, s.maxWrite = log, nil, log.Superblock(), log.MaxWriteBlocks()
	if s.ix == nil {
		ix, err := s.newIndex(log.MaxBlobBytes(), s.sb.SegmentBlocks)
		if err != nil {
			return err
		}
		s.ix = ix
	}
	s.phase, s.checkpointing, s.inPiece, s.piecesOut, s.pieceWait = serving, false, false, 0, false
	return nil
}

func (s *Store) newIndex(maxPiece uint64, segmentBlocks uint32) (*Index, error) {
	tree, err := lsm.New(TreeConfig(lsm.Config{MaxPiece: int(maxPiece), Check: s.cfg.Check}))
	if err != nil {
		return nil, err
	}
	return NewIndex(tree, segmentBlocks), nil
}

// Seal exists in the API only.
func (s *Store) Seal(core.ChunkID) error { return ErrSeal }

// Write writes r.Len bytes at r.Off, merging the blocks it covers in part against the chunk's newest ordered version.
func (s *Store) Write(ctx *core.Context, r Request, out *Output) {
	if ok := s.admit(ctx, r, opWrite, out); ok {
		s.pump(ctx, out)
	}
}

// Punch zeroes r.Len bytes at r.Off as a new version; punching the whole chunk deletes it and leaves a tombstone.
func (s *Store) Punch(ctx *core.Context, r Request, out *Output) {
	r.Buf = core.NoBuf
	if ok := s.admit(ctx, r, opPunch, out); ok {
		s.pump(ctx, out)
	}
}

func spanOf(off, n uint32) (first, last uint16) {
	return uint16(off / core.BlockSize), uint16((off + n - 1) / core.BlockSize)
}

func (s *Store) admit(ctx *core.Context, r Request, kind opKind, out *Output) bool {
	err := s.unavailable()
	if err == nil && (r.Len == 0 || r.Off >= core.ChunkSize || r.Len > core.ChunkSize-r.Off) {
		err = ErrRange
	}
	if err == nil && kind == opWrite {
		first, last := spanOf(r.Off, r.Len)
		if r.Buf == core.NoBuf || len(ctx.Pool.Bytes(r.Buf)) != (int(last)-int(first)+1)*core.BlockSize {
			err = disklog.ErrBadBuffer
		}
	}
	if err != nil {
		if r.Buf != core.NoBuf {
			ctx.Pool.Free(r.Buf)
		}
		out.Replies = append(out.Replies, Reply{ID: r.ID, Err: err})
		return false
	}
	o := s.newOp(kind, r)
	ci := s.chunkSlot(r.Chunk)
	s.ops[o].chunk = ci
	cs := &s.chunks[ci]
	cs.waiting = append(cs.waiting, o)
	if len(cs.waiting) == 1 {
		s.orderWaiting(ctx, ci, out)
	}
	return true
}

func (s *Store) unavailable() error {
	if s.phase == serving {
		return nil
	}
	return ErrUnavailable
}

func (s *Store) newOp(kind opKind, r Request) int32 {
	var i int32
	if n := len(s.freeOps); n > 0 {
		i = s.freeOps[n-1]
		s.freeOps = s.freeOps[:n-1]
	} else {
		s.ops = append(s.ops, op{})
		i = int32(len(s.ops) - 1)
	}
	o := &s.ops[i]
	s.seq++
	*o = op{kind: kind, req: r, seq: s.seq, chunk: -1, items: o.items[:0]}
	return i
}

func (s *Store) freeOp(i int32) {
	o := &s.ops[i]
	o.kind = 0
	s.freeOps = append(s.freeOps, i)
}

func (s *Store) chunkSlot(c core.ChunkID) int32 {
	if i, ok := s.chunkOf[c]; ok {
		return i
	}
	var i int32
	if n := len(s.freeChunks); n > 0 {
		i = s.freeChunks[n-1]
		s.freeChunks = s.freeChunks[:n-1]
	} else {
		s.chunks = append(s.chunks, chunkState{})
		i = int32(len(s.chunks) - 1)
	}
	cs := &s.chunks[i]
	cs.live, cs.id, cs.pending, cs.waiting = true, c, cs.pending[:0], cs.waiting[:0]
	cs.version, cs.epoch = 0, 0
	if ch, ok := s.ix.Chunk(s.cfg.Pool, c); ok {
		cs.version, cs.epoch = ch.Version, ch.Epoch
	}
	s.chunkOf[c] = i
	return i
}

func (s *Store) releaseChunk(i int32) {
	cs := &s.chunks[i]
	if !cs.live || len(cs.pending) > 0 || len(cs.waiting) > 0 {
		return
	}
	cs.live = false
	delete(s.chunkOf, cs.id)
	s.freeChunks = append(s.freeChunks, i)
}

func (s *Store) orderWaiting(ctx *core.Context, ci int32, out *Output) {
	for {
		cs := &s.chunks[ci]
		if len(cs.waiting) == 0 {
			s.releaseChunk(ci)
			return
		}
		oi := cs.waiting[0]
		o := &s.ops[oi]
		if o.reads > 0 {
			return
		}
		if o.err == nil && !o.based {
			if s.startBases(ctx, oi) {
				return
			}
			continue
		}
		cs.waiting = cs.waiting[:copy(cs.waiting, cs.waiting[1:])]
		if o.err != nil {
			s.finishUnordered(ctx, oi, out)
			continue
		}
		s.submit(ctx, oi, out)
	}
}

func (s *Store) finishUnordered(ctx *core.Context, oi int32, out *Output) {
	o := &s.ops[oi]
	s.freeBufs(ctx, oi)
	out.Replies = append(out.Replies, Reply{ID: o.req.ID, Version: o.version, Err: o.err})
	ci := o.chunk
	s.freeOp(oi)
	s.releaseChunk(ci)
	s.checkFlushes(out)
}

func (s *Store) freeBufs(ctx *core.Context, oi int32) {
	o := &s.ops[oi]
	if o.req.Buf != core.NoBuf {
		ctx.Pool.Free(o.req.Buf)
		o.req.Buf = core.NoBuf
	}
	for k, b := range o.edges {
		if b != core.NoBuf {
			ctx.Pool.Free(b)
			o.edges[k] = core.NoBuf
		}
	}
}

func partial(off, n uint32, b uint16) (lo, hi uint32, part bool) {
	start := uint32(b) * core.BlockSize
	lo = max(off, start) - start
	hi = min(off+n, start+core.BlockSize) - start
	return lo, hi, lo != 0 || hi != core.BlockSize
}

func (s *Store) startBases(ctx *core.Context, oi int32) bool {
	o := &s.ops[oi]
	cs := &s.chunks[o.chunk]
	o.based = true
	if o.req.HasVer && o.req.Ver != cs.version {
		o.err = ErrVersion
		return false
	}
	if o.kind == opPunch && cs.version == 0 {
		return false
	}
	first, last := spanOf(o.req.Off, o.req.Len)
	for k, b := range [2]uint16{first, last} {
		if k == 1 && last == first {
			break
		}
		lo, hi, part := partial(o.req.Off, o.req.Len, b)
		if !part {
			continue
		}
		var dst []byte
		if o.kind == opWrite {
			dst = ctx.Pool.Bytes(o.req.Buf)[int(b-first)*core.BlockSize:][:core.BlockSize]
		} else {
			buf, err := ctx.Pool.AllocZeroed(core.BlockSize)
			if err != nil {
				o.err = err
				return o.reads > 0
			}
			o.edges[k] = buf
			dst = ctx.Pool.Bytes(buf)
		}
		src, ext, read := s.base(ctx, cs, b)
		switch {
		case read:
			if err := s.startFetch(ctx, oi, b, 1, ext); err != nil {
				o.err = err
				return o.reads > 0
			}
		case src != nil:
			copy(dst[:lo], src[:lo])
			copy(dst[hi:], src[hi:])
		default:
			clear(dst[:lo])
			clear(dst[hi:])
		}
	}
	return o.reads > 0
}

// base finds block b's newest ordered bytes: a pending operation's block, zeros (nil), or a live extent to read.
func (s *Store) base(ctx *core.Context, cs *chunkState, b uint16) (src []byte, ext Extent, read bool) {
	for i := len(cs.pending) - 1; i >= 0 && !s.plant.mergeCommitted; i-- {
		o := &s.ops[cs.pending[i]]
		for j := len(o.items) - 1; j >= 0; j-- {
			it := &o.items[j]
			if it.state == itemFailed || it.entry.Kind == disklog.EntryFreeze || b < it.entry.First || int(b) >= int(it.entry.First)+int(it.entry.Count) {
				continue
			}
			if it.entry.Kind == disklog.EntryPunch {
				return nil, Extent{}, false
			}
			return ctx.Pool.Bytes(it.buf)[int(b-it.entry.First)*core.BlockSize:][:core.BlockSize], Extent{}, false
		}
	}
	e, ok := s.ix.Lookup(s.cfg.Pool, cs.id, b)
	if !ok || e.LBA == 0 {
		return nil, Extent{}, false
	}
	return nil, e, true
}

func (s *Store) submit(ctx *core.Context, oi int32, out *Output) {
	o := &s.ops[oi]
	ci := o.chunk
	cs := &s.chunks[ci]
	if o.kind == opPunch && cs.version == 0 {
		o.err = nil
		s.finishUnordered(ctx, oi, out)
		return
	}
	o.prevVer, o.prevEpoch = cs.version, cs.epoch
	o.version = s.nextVersion(cs.version)
	pool, c := s.cfg.Pool, cs.id
	if o.req.Epoch > cs.epoch {
		o.items = append(o.items, item{entry: disklog.FreezeEntry(pool, c, cs.version, o.req.Epoch)})
		cs.epoch = o.req.Epoch
	}
	cs.version = o.version
	first, last := spanOf(o.req.Off, o.req.Len)
	if o.kind == opWrite {
		if err := s.writeItems(ctx, o, first, last); err != nil {
			o.err = err
			s.freeBufs(ctx, oi)
		}
	} else {
		s.punchItems(o, first, last)
	}
	cs.pending = append(cs.pending, oi)
	o.left = len(o.items)
	for j := range o.items {
		it := &o.items[j]
		if o.err != nil {
			s.itemFailed(ctx, oi, int32(j), o.err)
			continue
		}
		t, err := s.log.Submit(ctx, disklog.Hot, disklog.Item{Entry: it.entry, Buf: it.buf, Keep: true}, &s.lout)
		if err != nil {
			s.itemFailed(ctx, oi, int32(j), err)
			continue
		}
		it.ticket = t
		s.sent = append(s.sent, sentItem{ticket: t, op: oi, item: int32(j)})
	}
	if s.ops[oi].left == 0 {
		s.mapResolved(ctx, ci, out)
	}
}

// writeItems cuts the write's blocks into items of at most one record each, or one item for a
// whole chunk the log writes to a chunk slot; a larger write is copied into a buffer per item,
// since a pool buffer cannot be split.
func (s *Store) writeItems(ctx *core.Context, o *op, first, last uint16) error {
	pool, c := s.cfg.Pool, s.chunks[o.chunk].id
	count := last - first + 1
	if count <= s.maxWrite || count == core.BlocksPerChunk && s.log.SlotAvailable() {
		o.items = append(o.items, item{entry: disklog.WriteEntry(pool, c, o.version, first, count), buf: o.req.Buf})
		o.req.Buf = core.NoBuf
		return nil
	}
	src := ctx.Pool.Bytes(o.req.Buf)
	for b := first; b <= last; b += s.maxWrite {
		n := min(s.maxWrite, last-b+1)
		buf, err := ctx.Pool.Alloc(int(n) * core.BlockSize)
		if err != nil {
			return err
		}
		copy(ctx.Pool.Bytes(buf), src[int(b-first)*core.BlockSize:])
		o.items = append(o.items, item{entry: disklog.WriteEntry(pool, c, o.version, b, n), buf: buf})
	}
	ctx.Pool.Free(o.req.Buf)
	o.req.Buf = core.NoBuf
	return nil
}

func (s *Store) punchItems(o *op, first, last uint16) {
	pool, c := s.cfg.Pool, s.chunks[o.chunk].id
	lo, hi := int(first), int(last)
	if o.edges[0] != core.NoBuf {
		o.items = append(o.items, item{entry: disklog.WriteEntry(pool, c, o.version, first, 1), buf: o.edges[0]})
		o.edges[0] = core.NoBuf
		lo++
	}
	tail := o.edges[1]
	if tail != core.NoBuf {
		o.edges[1] = core.NoBuf
		hi--
	}
	if lo <= hi {
		o.items = append(o.items, item{entry: disklog.PunchEntry(pool, c, o.version, uint16(lo), uint16(hi-lo+1))})
	}
	if tail != core.NoBuf {
		o.items = append(o.items, item{entry: disklog.WriteEntry(pool, c, o.version, last, 1), buf: tail})
	}
}

func (s *Store) nextVersion(newest uint64) uint64 {
	interval, counter := uint32(newest>>32), uint32(newest)
	if interval < s.interval {
		return uint64(s.interval)<<32 | 1
	}
	if counter == math.MaxUint32 {
		s.interval = interval + 1
		return uint64(s.interval)<<32 | 1
	}
	s.interval = interval
	return newest + 1
}

// Read reads r.Len bytes at r.Off of the chunk's committed version, or of version r.Ver when r.HasVer; it never waits for a writer.
func (s *Store) Read(ctx *core.Context, r Request, out *Output) {
	r.Buf = core.NoBuf
	err := s.unavailable()
	if err == nil && (r.Len == 0 || r.Off >= core.ChunkSize || r.Len > core.ChunkSize-r.Off) {
		err = ErrRange
	}
	pool := s.cfg.Pool
	var mapVer uint64
	var zeros bool
	if err == nil {
		ch, _ := s.ix.Chunk(pool, r.Chunk)
		mapVer, zeros, err = s.readable(r, ch.Version)
	}
	if err != nil {
		out.Replies = append(out.Replies, Reply{ID: r.ID, Err: err})
		return
	}
	first, last := spanOf(r.Off, r.Len)
	buf, err := ctx.Pool.AllocZeroed((int(last) - int(first) + 1) * core.BlockSize)
	if err != nil {
		out.Replies = append(out.Replies, Reply{ID: r.ID, Err: err})
		return
	}
	oi := s.newOp(opRead, r)
	o := &s.ops[oi]
	o.buf, o.version = buf, r.Ver
	if !r.HasVer {
		ch, _ := s.ix.Chunk(pool, r.Chunk)
		o.version = ch.Version
	}
	for b := int(first); b <= int(last) && !zeros; {
		e, ok := s.ix.lookup(pool, r.Chunk, mapVer, uint16(b))
		if !ok || e.LBA == 0 {
			b++
			continue
		}
		n := min(e.end(), int(last)+1) - b
		if err := s.startFetch(ctx, oi, uint16(b), uint16(n), e); err != nil {
			s.ops[oi].err = err
			break
		}
		b += n
	}
	if s.ops[oi].reads == 0 {
		s.finishRead(ctx, oi, out)
	}
	s.pump(ctx, out)
}

// readable is the map a read at r.Ver reads: the live map (0) for the committed version or a
// frozen version's map, or none when it reads the absent version 0.
func (s *Store) readable(r Request, committed uint64) (mapVer uint64, zeros bool, err error) {
	switch {
	case !r.HasVer || r.Ver == committed:
		return 0, false, nil
	case r.Ver == 0 && s.ix.absent(s.cfg.Pool, r.Chunk):
		return 0, true, nil
	case r.Ver != 0:
		if _, ok := s.ix.lookup(s.cfg.Pool, r.Chunk, r.Ver, 0); ok {
			return r.Ver, false, nil
		}
	}
	return 0, false, ErrNoVersion
}

func (s *Store) finishRead(ctx *core.Context, oi int32, out *Output) {
	o := &s.ops[oi]
	r := Reply{ID: o.req.ID, Version: o.version, Buf: o.buf, Err: o.err}
	if o.err != nil {
		ctx.Pool.Free(o.buf)
		r.Buf, r.Version = core.NoBuf, 0
	}
	out.Replies = append(out.Replies, r)
	s.freeOp(oi)
}

// Flush completes once every write and punch of the chunks cs submitted before it is answered: since writes are answered once durable, it adds no disk action.
func (s *Store) Flush(ctx *core.Context, id uint64, cs []core.ChunkID, out *Output) {
	if err := s.unavailable(); err != nil {
		out.Replies = append(out.Replies, Reply{ID: id, Err: err})
		return
	}
	s.seq++
	s.flushes = append(s.flushes, flushWait{id: id, seq: s.seq, chunks: s.flushChunks(cs)})
	s.checkFlushes(out)
}

// flushChunks copies cs into the chunk list a finished flush left past the end of s.flushes, if any.
func (s *Store) flushChunks(cs []core.ChunkID) []core.ChunkID {
	if n := len(s.flushes); n < cap(s.flushes) {
		return append(s.flushes[:n+1][n].chunks[:0], cs...)
	}
	return append([]core.ChunkID(nil), cs...)
}

func (s *Store) checkFlushes(out *Output) {
	for i := 0; i < len(s.flushes); {
		f := &s.flushes[i]
		if s.flushWaits(f) {
			i++
			continue
		}
		out.Replies = append(out.Replies, Reply{ID: f.id})
		last := len(s.flushes) - 1
		s.flushes[i], s.flushes[last] = s.flushes[last], s.flushes[i]
		s.flushes = s.flushes[:last]
	}
}

func (s *Store) flushWaits(f *flushWait) bool {
	for _, c := range f.chunks {
		ci, ok := s.chunkOf[c]
		if !ok {
			continue
		}
		cs := &s.chunks[ci]
		oldest := cs.waiting
		if len(cs.pending) > 0 {
			oldest = cs.pending
		}
		if len(oldest) > 0 && s.ops[oldest[0]].seq < f.seq {
			return true
		}
	}
	return false
}

func (s *Store) startFetch(ctx *core.Context, oi int32, first, n uint16, e Extent) error {
	data, err := ctx.Pool.Alloc(int(n) * core.BlockSize)
	if err != nil {
		return err
	}
	hdr, err := ctx.Pool.Alloc(core.BlockSize)
	if err != nil {
		ctx.Pool.Free(data)
		return err
	}
	var fi int32
	if k := len(s.freeFetch); k > 0 {
		fi = s.freeFetch[k-1]
		s.freeFetch = s.freeFetch[:k-1]
	} else {
		s.fetches = append(s.fetches, fetch{})
		fi = int32(len(s.fetches) - 1)
	}
	s.fetches[fi] = fetch{op: oi, first: first, n: n, lba: e.BlockLBA(first), header: e.Header, data: data, hdr: hdr, hdrLen: 1, waiting: 2}
	s.ops[oi].reads++
	s.lout.Actions = append(s.lout.Actions,
		core.Action{Kind: core.ActDiskRead, IO: fetchIO(fi, 0), LBA: e.BlockLBA(first), Buf: data, Len: uint64(n) * core.BlockSize},
		core.Action{Kind: core.ActDiskRead, IO: fetchIO(fi, 1), LBA: e.Header, Buf: hdr, Len: core.BlockSize})
	return nil
}

func fetchIO(fi int32, part uint64) core.IOID {
	return core.IOID(fetchIOBit | uint64(fi)<<1 | part)
}

// OnEvent takes a disk completion.
func (s *Store) OnEvent(ctx *core.Context, ev *core.Event, out *Output) {
	if ev.Kind != core.EvDiskDone {
		return
	}
	if ev.IO&fetchIOBit != 0 {
		s.fetchDone(ctx, int32(uint64(ev.IO)&^fetchIOBit>>1), ev.Err, out)
	} else if s.rec != nil {
		s.rec.OnEvent(ctx, ev, &s.lout)
	} else if s.log != nil {
		s.log.OnEvent(ctx, ev, &s.lout)
	}
	s.pump(ctx, out)
}

func (s *Store) fetchDone(ctx *core.Context, fi int32, derr core.DiskError, out *Output) {
	f := &s.fetches[fi]
	if derr != core.DiskOK && f.err == nil {
		f.err = derr
	}
	if f.waiting--; f.waiting > 0 {
		return
	}
	if f.err == nil {
		f.err = s.verify(ctx, fi)
		if f.waiting > 0 {
			return
		}
	}
	oi := f.op
	o := &s.ops[oi]
	if f.err != nil && o.err == nil {
		o.err = f.err
	}
	if f.err == nil {
		s.deliver(ctx, f, o)
	}
	ctx.Pool.Free(f.data)
	ctx.Pool.Free(f.hdr)
	f.lba = 0
	s.freeFetch = append(s.freeFetch, fi)
	if len(s.freed) > 0 && s.rec == nil {
		s.freeSlots(false)
	}
	if o.reads--; o.reads > 0 {
		return
	}
	if o.kind == opRead {
		s.finishRead(ctx, oi, out)
		return
	}
	s.orderWaiting(ctx, o.chunk, out)
}

func (s *Store) verify(ctx *core.Context, fi int32) error {
	f := &s.fetches[fi]
	offset := uint32(uint64(f.header) % uint64(s.sb.SegmentBlocks))
	hdr, data := ctx.Pool.Bytes(f.hdr)[:f.hdrLen*core.BlockSize], ctx.Pool.Bytes(f.data)
	_, hb, err := disklog.BlockCRC(hdr, s.sb.LogID, offset, 0)
	if errors.Is(err, disklog.ErrTruncated) && hb > f.hdrLen {
		return s.rereadHeader(ctx, fi, hb)
	}
	slot, inSlot := s.ix.slotOf(Extent{First: f.first, LBA: f.lba, Header: f.header})
	segment, index, _ := s.sb.SlotOf(slot)
	for k := range uint64(f.n) {
		var crc uint32
		if inSlot {
			crc, _, err = disklog.SlotBlockCRC(hdr, s.sb.LogID, offset, segment, index, f.first+uint16(k))
		} else {
			crc, _, err = disklog.BlockCRC(hdr, s.sb.LogID, offset, uint32(uint64(f.lba)+k-uint64(f.header)-uint64(hb)))
		}
		if err != nil {
			return err
		}
		if core.CRC32C(data[k*core.BlockSize:][:core.BlockSize]) != crc {
			return disklog.ErrBadChecksum
		}
	}
	return nil
}

func (s *Store) rereadHeader(ctx *core.Context, fi int32, blocks uint32) error {
	f := &s.fetches[fi]
	hdr, err := ctx.Pool.Alloc(int(blocks) * core.BlockSize)
	if err != nil {
		return err
	}
	ctx.Pool.Free(f.hdr)
	f.hdr, f.hdrLen, f.waiting = hdr, blocks, 1
	s.lout.Actions = append(s.lout.Actions, core.Action{Kind: core.ActDiskRead, IO: fetchIO(fi, 1), LBA: f.header, Buf: hdr, Len: uint64(blocks) * core.BlockSize})
	return nil
}

func (s *Store) deliver(ctx *core.Context, f *fetch, o *op) {
	src := ctx.Pool.Bytes(f.data)[:int(f.n)*core.BlockSize]
	first, last := spanOf(o.req.Off, o.req.Len)
	if o.kind == opRead {
		copy(ctx.Pool.Bytes(o.buf)[int(f.first-first)*core.BlockSize:], src)
		return
	}
	lo, hi, _ := partial(o.req.Off, o.req.Len, f.first)
	var dst []byte
	switch {
	case o.kind == opWrite:
		dst = ctx.Pool.Bytes(o.req.Buf)[int(f.first-first)*core.BlockSize:][:core.BlockSize]
	case f.first == first && o.edges[0] != core.NoBuf:
		dst = ctx.Pool.Bytes(o.edges[0])
	case f.first == last:
		dst = ctx.Pool.Bytes(o.edges[1])
	}
	copy(dst[:lo], src[:lo])
	copy(dst[hi:], src[hi:])
}

func (s *Store) pump(ctx *core.Context, out *Output) {
	i := 0
	for {
		for ; i < len(s.lout.Events); i++ {
			ev := s.lout.Events[i]
			s.logEvent(ctx, &ev, out)
		}
		was := s.phase
		s.housekeep(ctx, out)
		if i == len(s.lout.Events) && s.phase == was {
			break
		}
	}
	out.Actions = append(out.Actions, s.lout.Actions...)
	s.lout.Reset()
}

func (s *Store) housekeep(ctx *core.Context, out *Output) {
	switch s.phase {
	case serving:
		if s.log.Err() != nil {
			s.stop(ctx, s.log.Err(), out)
			return
		}
		// With no cleaner yet, nothing frees a segment, so items waiting for room fail at once.
		if s.log.Stalled() {
			s.log.FailStalled(ctx, &s.lout)
			return
		}
		if s.pieceWait {
			s.nextPiece(ctx, out)
		}
		if !s.checkpointing && !s.retryAfter && s.log.CheckpointDue() {
			s.beginCheckpoint(ctx, out)
		}
	case draining:
		s.drain(ctx, out)
	}
}

func (s *Store) logEvent(ctx *core.Context, ev *disklog.Event, out *Output) {
	switch ev.Kind {
	case disklog.Durable:
		s.durable(ctx, ev, out)
	case disklog.Failed, disklog.InDoubt:
		for _, t := range ev.Tickets {
			if s.inPiece && t == s.piece {
				s.inPiece, s.piecesOut = false, 0
				continue
			}
			if si, ok := s.popSent(t); ok {
				err := ev.Err
				if ev.Kind == disklog.InDoubt {
					err = ErrInDoubt
					s.ops[si.op].items[si.item].buf = core.NoBuf
				}
				if s.itemFailed(ctx, si.op, si.item, err) {
					s.mapResolved(ctx, s.ops[si.op].chunk, out)
				}
			}
		}
		if ev.Kind == disklog.InDoubt && s.phase == serving {
			s.stop(ctx, ErrInDoubt, out)
		}
	case disklog.CheckpointDone:
		if _, err := s.ix.Tree().CheckpointDone(); err != nil {
			panic("chunkstore: checkpoint done with no flush running: " + err.Error())
		}
		s.checkpointing = false
	case disklog.CheckpointFailed:
		s.ix.Tree().FlushFailed()
		s.checkpointing, s.inPiece, s.piecesOut, s.retryAfter, s.pieceWait = false, false, 0, true, false
	case disklog.ManifestLoaded:
		s.manifestLoaded(ctx)
	case disklog.BlobRead:
		s.blobRead(ctx, ev)
	case disklog.Replayed:
		if ev.LSN > s.replayLSN {
			if err := s.ix.Apply(ev); err != nil {
				s.recoveryFailed(ctx, err)
			}
		}
	case disklog.Recovered:
		log, err := s.rec.Log(ctx)
		if err != nil {
			s.recoveryFailed(ctx, err)
			return
		}
		if err := s.resume(log); err != nil {
			s.recoveryFailed(ctx, err)
			return
		}
		s.freeSlots(true)
	case disklog.RecoveryFailed:
		s.recoveryFailed(ctx, ev.Err)
	}
}

func (s *Store) recoveryFailed(ctx *core.Context, err error) {
	if s.rec != nil {
		_ = s.rec.Close(ctx)
	}
	s.phase, s.err = failed, err
}

// popSent finds the store's item under ticket t; the log answers a lane's items in submit order,
// but may fail queued items before rounds in flight become durable.
func (s *Store) popSent(t disklog.Ticket) (sentItem, bool) {
	live := s.sent[s.sentHead:]
	k, ok := slices.BinarySearchFunc(live, t, func(si sentItem, t disklog.Ticket) int { return cmp.Compare(si.ticket, t) })
	if !ok || live[k].op < 0 {
		return sentItem{}, false
	}
	si := live[k]
	live[k].op = -1
	for s.sentHead < len(s.sent) && s.sent[s.sentHead].op < 0 {
		s.sentHead++
	}
	if s.sentHead > len(s.sent)/2 {
		s.sent = s.sent[:copy(s.sent, s.sent[s.sentHead:])]
		s.sentHead = 0
	}
	return si, true
}

func (s *Store) durable(ctx *core.Context, ev *disklog.Event, out *Output) {
	s.retryAfter = false
	for i, t := range ev.Tickets {
		if s.inPiece && t == s.piece {
			s.inPiece, s.piecesOut = false, 0
			ref := disklog.BlobRef{Segment: ev.Segment, Block: ev.Block, LSN: ev.LSN, Entry: uint16(i)}
			if err := s.ix.Tree().PieceDurable(ref); err != nil {
				panic("chunkstore: piece durable out of turn: " + err.Error())
			}
			s.nextPiece(ctx, out)
			continue
		}
		si, ok := s.popSent(t)
		if !ok {
			continue
		}
		it := &s.ops[si.op].items[si.item]
		it.entry, it.state, it.lba, it.headerBlocks, it.payload, it.lsn = ev.Entries[i], itemDurable, ev.LBA, ev.HeaderBlocks, ev.Payloads[i], ev.LSN
		if s.itemDone(si.op) {
			s.mapResolved(ctx, s.ops[si.op].chunk, out)
		}
	}
}

func (s *Store) itemFailed(ctx *core.Context, oi, ii int32, err error) bool {
	o := &s.ops[oi]
	it := &o.items[ii]
	it.state = itemFailed
	if it.buf != core.NoBuf {
		ctx.Pool.Free(it.buf)
		it.buf = core.NoBuf
	}
	if o.err == nil {
		o.err = err
	}
	return s.itemDone(oi)
}

// itemDone counts an item of the op resolved and reports whether all are; a failed op gives back
// the chunk's version and epoch when none of its items, or not its freeze, became durable.
func (s *Store) itemDone(oi int32) bool {
	o := &s.ops[oi]
	if o.left--; o.left > 0 {
		return false
	}
	cs := &s.chunks[o.chunk]
	if o.err != nil {
		// The writes ordered after a failed one may hold its bytes; the log fails them too, since
		// it fails a lane's queued items together in submit order.
		if !o.holdsVersion() {
			cs.version = min(cs.version, o.prevVer)
		}
		if len(o.items) > 0 && o.items[0].entry.Kind == disklog.EntryFreeze && o.items[0].state != itemDurable {
			cs.epoch = min(cs.epoch, o.prevEpoch)
		}
	}
	return true
}

func (o *op) holdsVersion() bool {
	for j := range o.items {
		if it := &o.items[j]; it.state == itemDurable && it.entry.Kind != disklog.EntryFreeze {
			return true
		}
	}
	return false
}

func (s *Store) mapResolved(ctx *core.Context, ci int32, out *Output) {
	cs := &s.chunks[ci]
	for len(cs.pending) > 0 {
		oi := cs.pending[0]
		o := &s.ops[oi]
		if o.left > 0 {
			break
		}
		for j := range o.items {
			it := &o.items[j]
			if it.state == itemDurable && s.rec == nil {
				s.mapItem(it)
			}
			if it.buf != core.NoBuf {
				ctx.Pool.Free(it.buf)
				it.buf = core.NoBuf
			}
		}
		r := Reply{ID: o.req.ID, Version: o.version, Err: o.err}
		if o.err != nil && !o.holdsVersion() {
			r.Version = 0
		}
		out.Replies = append(out.Replies, r)
		cs.pending = cs.pending[:copy(cs.pending, cs.pending[1:])]
		s.freeOp(oi)
	}
	if s.rec == nil {
		s.freeSlots(false)
	}
	s.releaseChunk(ci)
	s.checkFlushes(out)
}

// freeSlots hands the log the chunk slots the index no longer refers to, now that the records
// that rewrote or deleted their chunks are durable. After a recovery some may already be free
// in the log, which the manifest recorded as of a later record than replay applied again.
// A slot a read in flight reads from waits until the read completes, so the log never reuses or
// TRIMs it under the read.
func (s *Store) freeSlots(recovered bool) {
	s.freed = s.ix.TakeFreed(s.freed)
	if s.log.Err() != nil {
		s.freed = s.freed[:0]
		return
	}
	waiting := s.freed[:0]
	for _, lba := range s.freed {
		switch {
		case s.ix.Live(lba) != 0:
		case s.reading(lba):
			waiting = append(waiting, lba)
		default:
			if err := s.log.FreeSlot(lba, &s.lout); err != nil && !recovered {
				panic("chunkstore: the log refused a chunk slot the index freed: " + err.Error())
			}
		}
	}
	s.freed = waiting
}

// reading reports whether a read in flight reads from the chunk slot starting at slot.
func (s *Store) reading(slot core.LBA) bool {
	for i := range s.fetches {
		f := &s.fetches[i]
		if at, ok := s.ix.slotOf(Extent{First: f.first, LBA: f.lba, Header: f.header}); f.lba != 0 && ok && at == slot {
			return true
		}
	}
	return false
}

// CheckSlots verifies that the chunk slots the log holds in use are exactly those the index's maps refer to.
func (s *Store) CheckSlots() error {
	var referred []core.LBA
	var it lsm.Iter
	s.ix.Tree().Iter(&it)
	for ok := it.SeekGE(nil); ok; ok = it.Next() {
		if e, ok := decodeExtent(it.Key(), it.Value()); ok {
			if slot, ok := s.ix.slotOf(e); ok {
				referred = append(referred, slot)
			}
		}
	}
	slices.Sort(referred)
	referred = slices.Compact(referred)
	if inUse := s.log.SlotsInUse(nil); !slices.Equal(inUse, referred) {
		return fmt.Errorf("chunkstore: chunk slots in use at %v, the maps refer to %v", inUse, referred)
	}
	return nil
}

func (s *Store) mapItem(it *item) {
	s.entry[0], s.payload[0] = it.entry, it.payload
	s.apply = disklog.Event{Kind: disklog.Durable, LSN: it.lsn, LBA: it.lba, HeaderBlocks: it.headerBlocks, Entries: s.entry[:], Payloads: s.payload[:]}
	if err := s.ix.Apply(&s.apply); err != nil {
		panic("chunkstore: the index refused the store's own entry: " + err.Error())
	}
}

// replayPoint is the newest LSN every record up to which the index holds: the durable LSN, or
// short of the oldest durable item the index has not mapped yet.
func (s *Store) replayPoint() uint64 {
	lsn := s.log.DurableLSN()
	for i := range s.ops {
		o := &s.ops[i]
		if o.kind == opRead || o.kind == 0 {
			continue
		}
		for j := range o.items {
			if it := &o.items[j]; it.state == itemDurable {
				lsn = min(lsn, it.lsn-1)
			}
		}
	}
	return lsn
}

func (s *Store) beginCheckpoint(ctx *core.Context, out *Output) {
	tree := s.ix.Tree()
	froze, err := tree.BeginFlush()
	if err != nil {
		panic("chunkstore: flush begun while one runs: " + err.Error())
	}
	if froze || !tree.Frozen() {
		s.replayLSN = s.replayPoint()
	}
	idx := [1]disklog.CheckpointIndex{{Owner: indexOwnerID, ReplayLSN: s.replayLSN, Runs: tree.CarriedRuns()}}
	if err := s.log.BeginCheckpoint(idx[:]); err != nil {
		tree.FlushFailed()
		return
	}
	s.checkpointing = true
	s.nextPiece(ctx, out)
}

// nextPiece hands the log the flush's next run piece, once the one before it is durable.
func (s *Store) nextPiece(ctx *core.Context, out *Output) {
	if s.phase != serving {
		return
	}
	buf, err := ctx.Pool.Alloc(int(s.log.MaxBlobBytes()))
	s.pieceWait = err != nil
	if err != nil {
		return
	}
	length, root, err := s.ix.Tree().NextPiece(ctx.Pool.Bytes(buf))
	if err == nil && s.plant.failCheck {
		s.plant.failCheck, err = false, lsm.ErrCheck
	}
	if err != nil {
		ctx.Pool.Free(buf)
		s.indexBroken(ctx, out)
		return
	}
	if length == 0 {
		ctx.Pool.Free(buf)
		if err := s.log.EndCheckpoint(ctx, &s.lout); err != nil {
			s.stop(ctx, err, out)
		}
		return
	}
	if s.piece, err = s.log.CheckpointPiece(ctx, indexOwnerID, buf, uint64(length), root, &s.lout); err != nil {
		ctx.Pool.Free(buf)
		s.stop(ctx, err, out)
		return
	}
	s.inPiece = true
	s.piecesOut++
}

// MergeDue returns the index's next merge, which may run off the loop, or nil.
func (s *Store) MergeDue() *lsm.Compaction {
	if s.phase != serving {
		return nil
	}
	return s.ix.Tree().PickCompaction()
}

// InstallMerge installs a merge MergeDue returned; one that fails its run check rebuilds the index from the log.
func (s *Store) InstallMerge(ctx *core.Context, c *lsm.Compaction, out *Output) error {
	if s.phase != serving {
		return ErrUnavailable
	}
	var err error
	if s.plant.failMerge {
		s.plant.failMerge, err = false, lsm.ErrCheck
	} else {
		err = s.ix.Tree().Install(c)
	}
	if errors.Is(err, lsm.ErrCheck) {
		s.indexBroken(ctx, out)
		s.pump(ctx, out)
	}
	return err
}

// indexBroken starts a rebuild of the index from the log: the store stops taking operations,
// lets the log's rounds in flight finish and restarts its log in rebuild mode.
func (s *Store) indexBroken(ctx *core.Context, out *Output) {
	s.phase = draining
	s.failWaiting(ctx, ErrUnavailable, out)
}

func (s *Store) drain(ctx *core.Context, out *Output) {
	if s.log.InFlight() > 0 || s.fetchesOut() > 0 {
		return
	}
	var closed disklog.Output
	if err := s.log.Close(ctx, &closed); err != nil {
		return
	}
	for i := range closed.Events {
		s.logEvent(ctx, &closed.Events[i], out)
	}
	s.phase = recovering
	if err := s.recover(ctx, s.sb.Geometry.DiskBlocks(), disklog.ModeRebuild); err != nil {
		s.phase, s.err = failed, err
	}
}

func (s *Store) fetchesOut() int { return len(s.fetches) - len(s.freeFetch) }

func (s *Store) stop(ctx *core.Context, err error, out *Output) {
	s.phase, s.err = failed, err
	s.failWaiting(ctx, err, out)
}

func (s *Store) failWaiting(ctx *core.Context, err error, out *Output) {
	for ci := range s.chunks {
		cs := &s.chunks[ci]
		for k := 0; k < len(cs.waiting); {
			oi := cs.waiting[k]
			if s.ops[oi].reads > 0 {
				s.ops[oi].err = err
				k++
				continue
			}
			cs.waiting = append(cs.waiting[:k], cs.waiting[k+1:]...)
			s.ops[oi].err = err
			s.finishUnordered(ctx, oi, out)
		}
	}
}

func (s *Store) manifestLoaded(ctx *core.Context) {
	ix, err := s.newIndex(s.rec.MaxBlobBytes(), s.rec.Superblock().SegmentBlocks)
	if err != nil {
		s.recoveryFailed(ctx, err)
		return
	}
	s.ix = ix
	if m, ok := s.rec.Manifest(); ok && s.mode == disklog.ModeOpen {
		for _, mi := range m.Indexes {
			if mi.Owner != indexOwnerID {
				continue
			}
			s.replayLSN = mi.ReplayLSN
			for _, root := range mi.Runs {
				s.loading[root] = &loadingRun{got: map[disklog.BlobRef][]byte{}}
				if err := s.rec.ReadBlob(ctx, root, &s.lout); err != nil {
					s.recoveryFailed(ctx, err)
					return
				}
			}
		}
	}
	s.replayIfLoaded(ctx)
}

func (s *Store) blobRead(ctx *core.Context, ev *disklog.Event) {
	root := ev.Ref
	l, isRoot := s.loading[root]
	if isRoot && l.root == nil {
		l.root = append([]byte(nil), ev.Data...)
		refs, err := lsm.RootRefs(l.root, nil)
		if err != nil {
			s.recoveryFailed(ctx, err)
			return
		}
		l.refs = refs
		for _, r := range refs {
			s.pieceOf[r] = root
			if err := s.rec.ReadBlob(ctx, r, &s.lout); err != nil {
				s.recoveryFailed(ctx, err)
				return
			}
		}
	} else {
		root = s.pieceOf[ev.Ref]
		l = s.loading[root]
		l.got[ev.Ref] = append([]byte(nil), ev.Data...)
	}
	if len(l.got) == len(l.refs) {
		pieces := make([][]byte, 0, len(l.refs)+1)
		for _, r := range l.refs {
			pieces = append(pieces, l.got[r])
		}
		if err := s.ix.Tree().LoadRun(root, append(pieces, l.root)); err != nil {
			s.recoveryFailed(ctx, err)
			return
		}
		delete(s.loading, root)
		s.replayIfLoaded(ctx)
	}
}

func (s *Store) replayIfLoaded(ctx *core.Context) {
	if len(s.loading) > 0 || s.phase != recovering {
		return
	}
	if err := s.rec.Replay(ctx, &s.lout); err != nil {
		s.recoveryFailed(ctx, err)
	}
}
