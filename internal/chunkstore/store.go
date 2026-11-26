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
	"github.com/nvcnvn/rock-solid/internal/raftshard"
)

var (
	// ErrRange reports an empty byte range or one running past the end of its chunk.
	ErrRange = errors.New("chunkstore: byte range empty or outside the chunk")
	// ErrVersion reports an expectVer other than the chunk's newest ordered version.
	ErrVersion = errors.New("chunkstore: chunk is not at the expected version")
	// ErrNoVersion reports an atVer that is neither the chunk's committed version nor one a snapshot froze.
	ErrNoVersion = errors.New("chunkstore: no such version to read")
	// ErrInDoubt reports a write whose record was in flight when the disk failed, or one still pending when the index was rebuilt; recovery decides whether it holds.
	ErrInDoubt = errors.New("chunkstore: write in doubt")
	// ErrUnavailable reports an operation while the store recovers, rebuilds its index or after its log failed.
	ErrUnavailable = errors.New("chunkstore: store unavailable")
	// ErrSeal reports a Seal, which the single-node store does not do.
	ErrSeal = errors.New("chunkstore: seal not supported")
	// ErrCounter reports a write or punch whose chunk has spent its version counter in the request's interval, or whose newest version is from a later one.
	ErrCounter = errors.New("chunkstore: chunk's version counter spent in this interval")
	// ErrFenced reports a write or punch of a session the session table fenced, or of a client incarnation older than the newest the store has seen of its session.
	ErrFenced = errors.New("chunkstore: write from a fenced session or an older incarnation of its session")
	// ErrDropped reports a gated write or punch that Settle dropped, or one Cancel dropped before it was ordered.
	ErrDropped = errors.New("chunkstore: write dropped")
	// ErrUnresolved reports a Settle of a chunk with a write or punch still waiting to be ordered or with records not yet durable or failed, or a PeerState of a PG with such a chunk or a missing list still being persisted.
	ErrUnresolved = errors.New("chunkstore: chunk has writes not yet resolved")
	// ErrMissing reports a read of a chunk on the missing list, whose kept version the disk may lack.
	ErrMissing = errors.New("chunkstore: chunk on the missing list")
)

const (
	indexOwnerID uint32 = 1
	fetchIOBit          = 1 << 63
)

// FirstLogIO and EndLogIO bound the IO IDs of the store's log and its recoveries, from every
// open, format and rebuild.
const FirstLogIO, EndLogIO core.IOID = 1 << 40, cleanIOBit

// Config is a store's settings.
type Config struct {
	// Log is the disk log's group commit settings.
	Log disklog.Config
	// Pool is the pool every chunk of this store belongs to.
	Pool core.PoolID
	// Check makes the index's tree verify every piece and merge it builds.
	Check bool
	// CleanFree is clean_free, the free segments outside the reserve the cleaner keeps; 0 is DefaultCleanFree, and below 0 the cleaner runs only for items waiting for room.
	CleanFree int
	// MergeCache is how many recently merged blocks the store keeps to merge on without a read; 0 keeps none.
	MergeCache int
	// Pages is where the index's tree takes its page slabs; nil takes them from the Go heap.
	Pages lsm.Pages
	// Replicas takes each Raft replica the manifest a restart loads names, with the owner its
	// blobs carry and the log's MaxBlobBytes from its superblock's geometry, before the replay: it
	// restores it as a shard on the round driver (AddRemote) before it returns, or returns why
	// not, which stops the restart.
	Replicas func(owner uint32, rep disklog.Replica, maxBlob uint64) error
}

// DefaultMergeCache is the merged-block cache of a replica primary's store: 64 blocks, 256 KiB per disk.
const DefaultMergeCache = 64

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
	// Interval, when set, numbers a write's or punch's version in the chunk's PG interval rather than the store's own.
	Interval uint32
	// Version, when set, is the version a PG's primary gave a write or punch, logged as is: a write's blocks are whole and a punch's Buf holds its merged edge blocks.
	Version uint64
	// Gated keeps a write or punch unmapped, its buffers kept, until Commit or Settle.
	Gated bool
	// FrozenOnly makes a read serve only a version a snapshot froze.
	FrozenOnly bool
	// Base makes a read serve its blocks of the live map only while they are at version Ver, as a primary fetches a base whose own copy failed its checksum.
	Base bool
	// Session and Incarnation name the client process a write or punch came from; a member's
	// replicated write, at the Version its primary ordered, is checked against the session table alone.
	Session     uint64
	Incarnation uint64
	// Request and Oldest are a client write's or punch's request ID and its session's oldest
	// outstanding one, logged with the version in a writer entry when Request and PGs, its pool's PG
	// count, are set, for the resend table.
	Request uint64
	Oldest  uint64
	PGs     uint32
	// Recover makes a Write a recovery write of the whole chunk in PG interval Interval, whose Buf is
	// a recovery buffer, or none when it only ends the chunk's listing, and a Read one of the chunk's
	// live image as a recovery buffer. A recovery write takes no version of its own: a member's is
	// at Ver, which HasVer marks, and a primary's, which is Gated, at the chunk's newest version of
	// the interval or else at Ver, the version peering kept. Relay has a primary's recovery write
	// log nothing, its buffer going to the members that lack the chunk.
	Recover bool
	Relay   bool
	// Lacking has a gated punch of a chunk the store holds nothing of ordered rather than answered
	// done: the primary lacks the chunk's kept version, which its members may hold.
	Lacking bool
}

// Reply completes the request ID: the version written or read, and a read's blocks in Buf laid out as a write's, which the caller frees; a write refused with ErrCounter hands its Buf back.
type Reply struct {
	ID      uint64
	Version uint64
	Buf     core.IOBuf
	Err     error
}

// BaseFetch asks the caller for block Block of a chunk at version Version from another member: the store's copy of a gated write's base failed its checksum, and request ID waits for SupplyBase.
type BaseFetch struct {
	ID      uint64
	Chunk   core.ChunkID
	Block   uint16
	Version uint64
}

// Output is the actions and replies of calls into the store, reused by the caller: Bases asks for gated writes' bases from other members; Ordered hands over each gated write's or punch's merged blocks as it is ordered (a write's whole blocks, a punch's merged edge blocks, none for a punch without them) in a buffer the caller frees, and Logged answers it once its records are durable or have failed; Mail is what a restart's replay sends the shards on the round driver, for the caller to hand each one's loop; Rebuilt reports that the index started a rebuild, after which the store holds what a restart would.
type Output struct {
	Actions []core.Action
	Replies []Reply
	Ordered []Reply
	Logged  []Reply
	Bases   []BaseFetch
	Rounds  []raftshard.Done
	Mail    []raftshard.Mail
	Rebuilt bool
}

// Reset empties the output for reuse.
func (o *Output) Reset() {
	clear(o.Replies)
	clear(o.Mail)
	o.Actions, o.Replies, o.Ordered, o.Logged, o.Bases, o.Rounds, o.Mail, o.Rebuilt = o.Actions[:0], o.Replies[:0], o.Ordered[:0], o.Logged[:0], o.Bases[:0], o.Rounds[:0], o.Mail[:0], false
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
	opPersist
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
	// answered is set once the ackEarly plant has replied.
	answered  bool
	committed bool
	dropped   bool
	// mapped is set once the mapOnCommit plant has mapped a gated op out of turn.
	mapped bool
	// torn marks a version replay gated without its last entry, which the crash lost: the disk
	// holds it in part.
	torn bool
	// remote counts the blocks of its base the op waits for from another member.
	remote int
}

// cachedBlock is a merged block the store keeps: block b of chunk c as version wrote it.
type cachedBlock struct {
	c       core.ChunkID
	b       uint16
	version uint64
}

// chunkState is a chunk with operations not yet answered: those ordered, in version order, and those waiting behind a base read.
type chunkState struct {
	live    bool
	id      core.ChunkID
	version uint64
	epoch   uint64
	pending []int32
	waiting []int32
	// held is whether write admission holds the chunk's next write back.
	held bool
	// failed is the lowest version a member's write or punch of the chunk failed at, after which the chunk's later versions of that interval are refused, so a member never logs one without the versions below it.
	failed uint64
	// ordered is the version of the chunk's last write or punch the store ordered, which a gated
	// one that fails before it is ordered leaves behind version.
	ordered uint64
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
	version uint64
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

// loadingRun is a run a restart loads into tree, or for owner's shard on the round driver, with
// tree nil.
type loadingRun struct {
	tree  *lsm.Tree
	owner uint32
	// bound is the merge bound the manifest holds on the run, whose pieces up to bound.Dead are dead and never read.
	bound *disklog.MergeBound
	refs  []disklog.BlobRef
	got   map[disklog.BlobRef][]byte
	root  []byte
}

type plants struct {
	// writerAtLog puts a gated version's writer into the resend table once logged rather than once
	// mapped, and removes no entry where its version goes, so a version peering drops keeps its
	// entry (the epoch change model's KeepDroppedIds).
	writerAtLog bool
	// trimOnAnswer has a disk trim a write's entry once it maps the write's version, whether or not
	// its client heard the answer; noWatermark trims a session's entries and keeps no watermark.
	trimOnAnswer bool
	noWatermark  bool
	// clearListed has a persisted missing list clear, in the index alone, every listing of its PG
	// from an earlier peering that it leaves out.
	clearListed bool
	// settleSubset has Settle drop the first gated version below the kept one, as a peering that drops any subset of pending versions rather than a suffix would (sub-block spike M7).
	settleSubset bool
	// settleByNumber has Settle keep every gated version up to the kept one by number, whatever interval it was ordered in.
	settleByNumber bool
	// mapOnReplay has replay map every write and punch, holding none back for a decision; replayForgetsDrops has a
	// decision marker map the versions replay held back that its decision dropped.
	mapOnReplay        bool
	replayForgetsDrops bool
	// replayCopyAsWrite replays a copy entry as a write of its blocks, mapping them whether or not a map still stores them at the source.
	replayCopyAsWrite bool
	mergeCommitted    bool
	failCheck         bool
	failMerge         bool
	// failChecks fails every flush's run check.
	failChecks bool
	// noAdmission admits every chunk write at once, however few segments are free, and
	// copiesOnly measures the cleaner's rate net of its copied blocks alone.
	noAdmission bool
	copiesOnly  bool
	// replayLate leaves out of replay the first record past the replay LSN that maps blocks, as a
	// replay LSN one record too new would (RocksDB #11148); skipped is set once it has.
	replayLate bool
	skipped    bool
	// forgetVersion starts a chunk taken up again at version 0 rather than its newest;
	// rebuildSkipsFreezes drops the freezes a rebuild replays; replayDropsPunches drops the punches
	// any recovery replays; punchKeepsHead leaves a punch's partial first block whole, its old bytes
	// past the punch's start kept.
	forgetVersion       bool
	rebuildSkipsFreezes bool
	replayDropsPunches  bool
	punchKeepsHead      bool
	// ackEarly answers a write or punch OK as it is submitted, before its records are durable;
	// freeNextSlot also hands the log the slot after each chunk slot it frees.
	ackEarly     bool
	freeNextSlot bool
	// noGainGuard takes victims whose copies cost a segment or more; moveOverWrites applies the
	// cleaner's copies as writes, wherever a map points; noSpaceWithVictim fails items waiting for
	// room with NoSpace while the cleaner has a victim; freeLive frees a chunk victim's live count
	// before copying, so the log frees it with its blocks live; newPassOnly counts as a victim only
	// a pass the cleaner just started, failing items waiting for room while an older one runs.
	noGainGuard       bool
	moveOverWrites    bool
	noSpaceWithVictim bool
	freeLive          bool
	newPassOnly       bool
	// rewriteNoSteps charges a metadata victim's rewrite as if it took no steps, leaving out each
	// step's record header, extra blobs and manifest.
	rewriteNoSteps bool
	// mapOnCommit maps a gated op when it commits, ahead of earlier versions still uncommitted.
	mapOnCommit bool
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
	sched    *lsm.Scheduler
	sb       disklog.Superblock
	maxWrite uint16
	interval uint32
	seq      uint64
	logGen   uint64
	// rebuilt is set from a rebuild of the index until a checkpoint completes.
	rebuilt bool

	ops        []op
	freeOps    []int32
	chunks     []chunkState
	freeChunks []int32
	chunkOf    map[core.ChunkID]int32
	sent       []sentItem
	sentHead   int
	// persists is the ops persisting missing lists, until their records are durable or failed.
	persists []int32
	// held is the replayed writes and punches, in log order, of an interval at or past their PG's
	// newest decision the disk persisted, which no later marker has settled yet.
	held []item
	// issued is every disk read, write and flush not yet done, in the order the store issued them, with when.
	issued    []issuedIO
	fetches   []fetch
	freeFetch []int32
	flushes   []flushWait

	checkpointing bool
	piece         disklog.Ticket
	inPiece       bool
	piecesOut     int
	replayLSN     uint64
	retryAfter    bool
	// pieceWait is a flush whose next piece waits for a pool buffer, tried again on every call.
	pieceWait bool
	// pieceTree is the tree whose pieces the checkpoint writes, as tree numbers it; piecesDone says
	// every tree's are written, the checkpoint waiting for the shards on the round driver.
	pieceTree  int
	piecesDone bool
	idx        []disklog.CheckpointIndex

	// raft hosts the Raft shards on the disk, shards in the order their trees checkpoint, the
	// first cpShards in the running checkpoint, removing the owners of those RemoveRaft removes
	// once it ends, removedPieces the pieces of their trees, dead once a checkpoint begun after the
	// removal ends; lastBase is the base of the last manifest a due checkpoint wrote, cpBase that
	// of the one running; remoteFull says a shard on the round driver asked for a checkpoint.
	raft             *raftshard.Host
	shards           []raftShard
	cpShards         int
	removing         []uint32
	removedPieces    []disklog.BlobRef
	lastBase, cpBase uint64
	remoteFull       bool

	loading map[disklog.BlobRef]*loadingRun
	pieceOf map[disklog.BlobRef]disklog.BlobRef
	freed   []core.LBA
	// pinned counts, for each chunk slot, the slot writes replay held that wait in a gated op for a
	// decision: the slot stays in use while any does.
	pinned map[core.LBA]int32

	apply   disklog.Event
	marks   []disklog.Entry
	entry   [1]disklog.Entry
	payload [1]uint64
	changes []LiveChange

	cl  cleaner
	adm admission
	// sessions is the newest incarnation seen of each client session.
	sessions map[uint64]uint64
	fence    SessionTable
	// covers is, for each chunk a recovery write waits on at this primary, the blocks the writes of
	// its interval ordered so far cover; rexts the extents a recovery read reads.
	covers map[core.ChunkID]cover
	rexts  []Extent
	// cache is the recently merged blocks, held in cacheBuf block for block, replaced in turn from cacheNext.
	cache     []cachedBlock
	cacheBuf  core.IOBuf
	cacheNext int

	plant plants
	// metaVictim, when set, sees each metadata victim the cleaner starts, before it marks the
	// victim's runs for a rewrite, as the tests that measure what the rewrite writes do.
	metaVictim func(segment uint32)
}

func newStore(cfg Config) *Store {
	if reserved(cfg.Pool) {
		panic("chunkstore: a store of the index's reserved pool")
	}
	return &Store{cfg: cfg, interval: 1, chunkOf: map[core.ChunkID]int32{}, sessions: map[uint64]uint64{}, pinned: map[core.LBA]int32{}}
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

// Index is the store's chunk index: nil until recovery loads the manifest, partial until Serving.
func (s *Store) Index() *Index { return s.ix }

// Log is the store's disk log, nil while it recovers.
func (s *Store) Log() *disklog.Log { return s.log }

func (s *Store) logIO() core.IOID {
	s.logGen++
	return core.IOID(s.logGen << 40)
}

func (s *Store) recover(ctx *core.Context, diskBlocks uint64, mode disklog.Mode) error {
	s.closeIndex()
	s.log, s.mode, s.replayLSN, s.held = nil, mode, 0, s.held[:0]
	s.loading, s.pieceOf = map[disklog.BlobRef]*loadingRun{}, map[disklog.BlobRef]disklog.BlobRef{}
	rec, err := disklog.Recover(ctx, s.cfg.Log, diskBlocks, mode, s.logIO(), &s.lout)
	if err != nil {
		return err
	}
	s.rec, s.phase = rec, recovering
	return nil
}

// resume serves on log. A recovered index holds every record, so the log's usage table takes
// each chunk segment's live blocks from the index's record counts, and the index reports every
// change to them from then on.
func (s *Store) resume(log *disklog.Log) error {
	s.log, s.rec, s.sb, s.maxWrite = log, nil, log.Superblock(), log.MaxWriteBlocks()
	if s.ix == nil {
		if err := s.newIndex(log.MaxBlobBytes(), s.sb.Geometry); err != nil {
			return err
		}
	} else {
		live := make([]uint32, s.sb.SegmentCount)
		s.ix.SegmentLive(s.sb.SegmentBlocks, live)
		log.Recount(live)
		s.ix.RecountSlotHeld()
		s.ix.EndReplay()
	}
	s.ix.TrackLive(true)
	s.adm.bucket = bucket{}
	s.phase, s.checkpointing, s.inPiece, s.piecesOut, s.pieceWait = serving, false, false, 0, false
	return nil
}

// newIndex starts an empty index and the merge scheduler over it, which hands out the merge
// workers the log keeps a lane for.
func (s *Store) newIndex(maxPiece uint64, geo disklog.Geometry) error {
	s.closeIndex()
	step := cmp.Or(s.cfg.Log.CompactionStepBytes, disklog.DefaultConfig().CompactionStepBytes)
	tree, err := lsm.New(TreeConfig(lsm.Config{MaxPiece: int(maxPiece), StepBytes: int(step), Check: s.cfg.Check, Pages: s.cfg.Pages}))
	if err != nil {
		return err
	}
	sched, err := lsm.NewScheduler(lsm.SchedulerConfig{MergeWorkers: s.cfg.Log.MergeWorkers})
	if err != nil {
		return err
	}
	sched.Add(tree)
	for _, r := range s.shards {
		sched.Add(r.sh.Tree())
	}
	s.ix, s.sched = NewIndex(tree, geo.SegmentBlocks, geo.SegmentCount), sched
	s.ix.plant = &s.plant
	return nil
}

// closeIndex gives the index's page slabs back and drops it.
func (s *Store) closeIndex() {
	if s.ix != nil {
		s.ix.Tree().Close()
		s.ix = nil
	}
}

// Close gives the index's page slabs back to Config.Pages; the store may not be used after.
func (s *Store) Close() { s.closeIndex() }

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
	if r.Version == 0 {
		r.Buf = core.NoBuf
	}
	if s.plant.punchKeepsHead && r.Off%core.BlockSize != 0 && r.Len > core.BlockSize {
		r.Off, r.Len = r.Off+core.BlockSize-r.Off%core.BlockSize, r.Len-(core.BlockSize-r.Off%core.BlockSize)
	}
	if ok := s.admit(ctx, r, opPunch, out); ok {
		s.pump(ctx, out)
	}
}

func spanOf(off, n uint32) (first, last uint16) {
	return uint16(off / core.BlockSize), uint16((off + n - 1) / core.BlockSize)
}

func (s *Store) admit(ctx *core.Context, r Request, kind opKind, out *Output) bool {
	err := s.unavailable()
	if err == nil && !validRange(r.Off, r.Len) {
		err = ErrRange
	}
	switch {
	case err != nil || kind != opWrite:
	case r.Recover:
		var m blockMap
		if r.Buf != core.NoBuf && decodeBlockMap(ctx.Pool.Bytes(r.Buf), &m) != nil || r.Buf == core.NoBuf && r.Gated {
			err = disklog.ErrBadBuffer
		}
	case r.Buf == core.NoBuf || len(ctx.Pool.Bytes(r.Buf)) != dataBytes(r.Off, r.Len):
		err = disklog.ErrBadBuffer
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
	cs.version, cs.epoch, cs.failed, cs.ordered = 0, 0, 0, 0
	if ch, ok := s.ix.Chunk(s.cfg.Pool, c); ok && !s.plant.forgetVersion {
		cs.version, cs.epoch, cs.ordered = ch.Version, ch.Epoch, ch.Version
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
		if o.err == nil && cs.failed != 0 && o.req.Version > cs.failed && o.req.Version>>32 == cs.failed>>32 {
			o.err = ErrVersion
		}
		if o.err == nil && o.kind == opWrite && !s.admitWrite(ctx, ci, o) {
			return
		}
		if o.err == nil && o.req.Session != 0 && (o.req.Version == 0 && o.req.Incarnation < s.sessions[o.req.Session] || s.fence.Fenced(o.req.Session)) {
			o.err = ErrFenced
		}
		cs.waiting = cs.waiting[:copy(cs.waiting, cs.waiting[1:])]
		if cs.held {
			s.unhold(ci)
		}
		if o.err != nil {
			s.finishUnordered(ctx, oi, out)
			continue
		}
		s.submit(ctx, oi, out)
	}
}

func (s *Store) finishUnordered(ctx *core.Context, oi int32, out *Output) {
	o := &s.ops[oi]
	var back core.IOBuf
	if errors.Is(o.err, ErrCounter) {
		back, o.req.Buf = o.req.Buf, core.NoBuf
	}
	s.freeBufs(ctx, oi)
	if o.err != nil && o.req.Version > s.chunks[o.chunk].version {
		s.memberFailed(o)
	}
	if !o.answered {
		out.Replies = append(out.Replies, Reply{ID: o.req.ID, Version: o.version, Err: o.err, Buf: back})
	}
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
	if o.req.Recover {
		if iv := uint32(o.req.Ver >> 32); o.req.HasVer && (iv == o.req.Interval && cs.version != o.req.Ver || iv < o.req.Interval && uint32(cs.version>>32) == o.req.Interval) {
			o.err = ErrVersion
		}
		return false
	}
	if o.req.Version != 0 {
		switch {
		case o.req.Version <= cs.version:
			o.err = ErrVersion
		case o.kind == opPunch:
			o.err = s.memberEdges(ctx, o)
		}
		return false
	}
	if o.req.HasVer && o.req.Ver != cs.version {
		o.err = ErrVersion
		return false
	}
	if o.kind == opPunch && cs.version == 0 && !o.req.Lacking {
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

// memberEdges splits a member's punch buffer into the merged edge blocks its primary sent, one per block the punch covers in part.
func (s *Store) memberEdges(ctx *core.Context, o *op) error {
	var src []byte
	if o.req.Buf != core.NoBuf {
		src = ctx.Pool.Bytes(o.req.Buf)
	}
	first, last := spanOf(o.req.Off, o.req.Len)
	n := 0
	for k, b := range [2]uint16{first, last} {
		if k == 1 && last == first {
			break
		}
		if _, _, part := partial(o.req.Off, o.req.Len, b); !part {
			continue
		}
		if len(src) < (n+1)*core.BlockSize {
			return disklog.ErrBadBuffer
		}
		buf, err := ctx.Pool.Alloc(core.BlockSize)
		if err != nil {
			return err
		}
		copy(ctx.Pool.Bytes(buf), src[n*core.BlockSize:])
		o.edges[k] = buf
		n++
	}
	if len(src) != n*core.BlockSize {
		return disklog.ErrBadBuffer
	}
	return nil
}

// base finds block b's newest ordered bytes: a pending operation's block, zeros (nil), or a live extent to read.
func (s *Store) base(ctx *core.Context, cs *chunkState, b uint16) (src []byte, ext Extent, read bool) {
	for i := len(cs.pending) - 1; i >= 0 && !s.plant.mergeCommitted; i-- {
		o := &s.ops[cs.pending[i]]
		if o.dropped || o.req.Gated && o.err != nil {
			continue
		}
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
	for i, k := range s.cache {
		if k.c == cs.id && k.b == b && k.version == e.Version {
			return ctx.Pool.Bytes(s.cacheBuf)[i*core.BlockSize:][:core.BlockSize], Extent{}, false
		}
	}
	return nil, e, true
}

// remember keeps the blocks op o merged, its edges that its range covers in part, once o is mapped.
func (s *Store) remember(ctx *core.Context, o *op) {
	if s.cfg.MergeCache == 0 || o.req.Version != 0 {
		return
	}
	if s.cacheBuf == core.NoBuf {
		buf, err := ctx.Pool.Alloc(s.cfg.MergeCache * core.BlockSize)
		if err != nil {
			return
		}
		s.cacheBuf = buf
	}
	c := s.chunks[o.chunk].id
	first, last := spanOf(o.req.Off, o.req.Len)
	for k, b := range [2]uint16{first, last} {
		if _, _, part := partial(o.req.Off, o.req.Len, b); !part || k == 1 && last == first {
			continue
		}
		for j := range o.items {
			it := &o.items[j]
			if it.state != itemDurable || it.entry.Kind != disklog.EntryWrite || b < it.entry.First || int(b) >= int(it.entry.First)+int(it.entry.Count) {
				continue
			}
			i := slices.IndexFunc(s.cache, func(k cachedBlock) bool { return k.c == c && k.b == b })
			if i < 0 && len(s.cache) < s.cfg.MergeCache {
				s.cache = append(s.cache, cachedBlock{})
				i = len(s.cache) - 1
			}
			if i < 0 {
				i = s.cacheNext
				s.cacheNext = (s.cacheNext + 1) % s.cfg.MergeCache
			}
			s.cache[i] = cachedBlock{c: c, b: b, version: o.version}
			copy(ctx.Pool.Bytes(s.cacheBuf)[i*core.BlockSize:][:core.BlockSize], ctx.Pool.Bytes(it.buf)[int(b-it.entry.First)*core.BlockSize:])
		}
	}
}

// SupplyBase hands the op waiting on request id the base block b another member sent, in buf, which the store frees, or fails it with ErrBadChecksum when no member could send it.
func (s *Store) SupplyBase(ctx *core.Context, id uint64, b uint16, buf core.IOBuf, err error, out *Output) {
	defer s.pump(ctx, out)
	for oi := range s.ops {
		o := &s.ops[oi]
		if o.kind == 0 || o.kind == opRead || o.req.ID != id || o.remote == 0 {
			continue
		}
		o.remote--
		if err == nil && buf != core.NoBuf && len(ctx.Pool.Bytes(buf)) >= core.BlockSize {
			s.deliver(ctx, &fetch{first: b, n: 1, data: buf}, o)
		} else if o.err == nil {
			o.err = disklog.ErrBadChecksum
		}
		if o.reads--; o.reads == 0 {
			s.orderWaiting(ctx, o.chunk, out)
		}
		break
	}
	if buf != core.NoBuf {
		ctx.Pool.Free(buf)
	}
}

func (s *Store) submit(ctx *core.Context, oi int32, out *Output) {
	o := &s.ops[oi]
	ci := o.chunk
	cs := &s.chunks[ci]
	if o.kind == opPunch && cs.version == 0 && o.req.Version == 0 && !o.req.Lacking {
		o.err = nil
		s.finishUnordered(ctx, oi, out)
		return
	}
	o.prevVer, o.prevEpoch = cs.version, cs.epoch
	switch {
	case o.req.Recover && (o.req.HasVer || uint32(cs.ordered>>32) != o.req.Interval):
		o.version = o.req.Ver
	case o.req.Recover:
		o.version = cs.ordered
	case o.req.Version != 0:
		o.version = o.req.Version
	case o.req.Interval != 0:
		v, ok := intervalVersion(cs.version, o.req.Interval)
		if !ok {
			o.err = ErrCounter
			s.finishUnordered(ctx, oi, out)
			return
		}
		o.version = v
	default:
		o.version = s.nextVersion(cs.version)
	}
	pool, c := s.cfg.Pool, cs.id
	if o.req.Epoch > cs.epoch {
		o.items = append(o.items, item{entry: disklog.FreezeEntry(pool, c, cs.version, o.req.Epoch)})
		cs.epoch = o.req.Epoch
	}
	writer := o.req.Request != 0 && o.req.PGs != 0 && o.req.Session != 0 && !o.req.Recover
	if writer {
		o.items = append(o.items, item{entry: disklog.WriterEntry(pool, c, o.version, o.req.PGs, o.req.Session, o.req.Incarnation, o.req.Request, o.req.Oldest)})
	}
	cs.version = o.version
	first, last := spanOf(o.req.Off, o.req.Len)
	switch {
	case o.req.Recover:
		if err := s.recoverItems(ctx, o); err != nil {
			o.err = err
			s.memberFailed(o)
		}
		s.freeBufs(ctx, oi)
	case o.kind == opWrite:
		if err := s.writeItems(ctx, o, first, last); err != nil {
			o.err = err
			s.freeBufs(ctx, oi)
			s.memberFailed(o)
		}
	default:
		s.punchItems(o, first, last)
	}
	if n := len(o.items); o.err == nil && n > 0 && !o.req.Recover {
		o.items[n-1].entry.Last = true
	}
	if o.req.Gated && o.err == nil && len(o.items) == 0 {
		out.Logged = append(out.Logged, Reply{ID: o.req.ID, Version: o.version})
	}
	if o.req.Gated && o.err == nil {
		r := Reply{ID: o.req.ID, Version: o.version}
		if o.req.Recover {
			r.Buf, o.buf = o.buf, core.NoBuf
		} else {
			r.Buf, o.err = s.mergedCopy(ctx, o)
		}
		if o.err == nil {
			out.Ordered = append(out.Ordered, r)
			if !o.req.Recover {
				cs.ordered = o.version
				s.covered(c, o)
			}
		}
	}
	cs.pending = append(cs.pending, oi)
	o.left = len(o.items)
	if s.plant.ackEarly && o.err == nil {
		o.answered = true
		out.Replies = append(out.Replies, Reply{ID: o.req.ID, Version: o.version})
	}
	// A writer entry rides in the round of its version's first data rather than in a record of
	// its own, which would cost each client write a round and a header block.
	held := true
	if writer {
		held = s.log.Hold()
	}
	for j := range o.items {
		it := &o.items[j]
		if o.err != nil {
			s.itemFailed(ctx, oi, int32(j), o.err, out)
			continue
		}
		t, err := s.log.Submit(ctx, disklog.Hot, disklog.Item{Entry: it.entry, Buf: it.buf, Keep: true}, &s.lout)
		if err != nil {
			s.itemFailed(ctx, oi, int32(j), err, out)
			continue
		}
		it.ticket = t
		s.sent = append(s.sent, sentItem{ticket: t, op: oi, item: int32(j)})
	}
	if !held {
		s.log.Release(ctx, &s.lout)
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
	if count <= s.maxWrite || count == core.BlocksPerChunk && s.log.SlotAvailable() && !o.req.Gated {
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

// mergedCopy copies the blocks o's items write, in order, into one buffer: a write's whole blocks or a punch's merged edges, none for a punch without them.
func (s *Store) mergedCopy(ctx *core.Context, o *op) (core.IOBuf, error) {
	n := 0
	for j := range o.items {
		if e := &o.items[j].entry; e.Kind == disklog.EntryWrite {
			n += int(e.Count)
		}
	}
	if n == 0 {
		return core.NoBuf, nil
	}
	buf, err := ctx.Pool.Alloc(n * core.BlockSize)
	if err != nil {
		return core.NoBuf, err
	}
	dst := ctx.Pool.Bytes(buf)
	for j := range o.items {
		if it := &o.items[j]; it.entry.Kind == disklog.EntryWrite {
			dst = dst[copy(dst, ctx.Pool.Bytes(it.buf)[:int(it.entry.Count)*core.BlockSize]):]
		}
	}
	return buf, nil
}

// intervalVersion is the version after newest in a PG's interval: interval.1 above an earlier interval's version, else newest plus one, unless the counter is spent or newest is from a later interval.
func intervalVersion(newest uint64, interval uint32) (uint64, bool) {
	switch k := uint32(newest >> 32); {
	case k < interval:
		return MakeVersion(interval, 1), true
	case k > interval || uint32(newest) == math.MaxUint32:
		return 0, false
	}
	return newest + 1, true
}

func (s *Store) nextVersion(newest uint64) uint64 {
	interval, counter := uint32(newest>>32), uint32(newest)
	if interval < s.interval {
		return MakeVersion(s.interval, 1)
	}
	if counter == math.MaxUint32 {
		s.interval = interval + 1
		return MakeVersion(s.interval, 1)
	}
	s.interval = interval
	return newest + 1
}

// Read reads r.Len bytes at r.Off of the chunk's committed version, or of version r.Ver when r.HasVer; it never waits for a writer.
func (s *Store) Read(ctx *core.Context, r Request, out *Output) {
	r.Buf = core.NoBuf
	err := s.unavailable()
	if err == nil && !validRange(r.Off, r.Len) {
		err = ErrRange
	}
	pool := s.cfg.Pool
	var mapVer uint64
	var zeros bool
	if err == nil {
		ch, _ := s.ix.Chunk(pool, r.Chunk)
		mapVer, zeros, err = s.readable(r, ch.Version)
		if ch.ListedIn != 0 && !r.Recover {
			err = ErrMissing
		}
	}
	if err != nil {
		out.Replies = append(out.Replies, Reply{ID: r.ID, Err: err})
		return
	}
	if r.Recover {
		oi := s.newOp(opRead, r)
		ch, _ := s.ix.Chunk(pool, r.Chunk)
		s.ops[oi].version = ch.Version
		if err := s.recoveryRead(ctx, oi); err != nil {
			s.ops[oi].err = err
		}
		if s.ops[oi].reads == 0 {
			s.finishRead(ctx, oi, out)
		}
		s.pump(ctx, out)
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
	case r.Base:
		first, last := spanOf(r.Off, r.Len)
		for b := first; b <= last; b++ {
			if e, ok := s.ix.Lookup(s.cfg.Pool, r.Chunk, b); !ok || e.Version != r.Ver {
				return 0, false, ErrNoVersion
			}
		}
		return 0, false, nil
	case r.FrozenOnly && (!r.HasVer || r.Ver == 0):
	case !r.FrozenOnly && (!r.HasVer || r.Ver == committed):
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
		if o.buf != core.NoBuf {
			ctx.Pool.Free(o.buf)
		}
		r.Buf, r.Version = core.NoBuf, 0
	}
	out.Replies = append(out.Replies, r)
	s.freeOp(oi)
}

// Commit lets the gated write or punch of chunk c that request id ordered be mapped, once the chunk's earlier versions are.
func (s *Store) Commit(ctx *core.Context, c core.ChunkID, id uint64, out *Output) {
	ci, ok := s.chunkOf[c]
	if !ok {
		return
	}
	for _, oi := range s.chunks[ci].pending {
		if o := &s.ops[oi]; o.req.Gated && o.req.ID == id {
			o.committed = true
			if s.plant.mapOnCommit && o.left == 0 && o.err == nil {
				for j := range o.items {
					if it := &o.items[j]; it.state == itemDurable {
						s.unmappedLive(it, 1)
						s.mapItem(it)
					}
				}
				o.mapped = true
			}
			break
		}
	}
	s.mapResolved(ctx, ci, out)
}

// Settle is peering's decision on chunk c's gated writes and punches not mapped yet, given as the version keep it kept: those on keep's chain, at keep or below it in keep's interval, and every one before such a one, are mapped in version order, and the others dropped, never mapped, even if committed, the chunk's version and snapshot epoch given back to what the first dropped one found. One ordered in an earlier interval than keep's with none of keep's interval after it is dropped even if keep is above it by number, since an interval this disk missed may have dropped it and built keep on an older version. It returns the newest version it keeps, 0 for none. Dropped records stay in the log until recovery supersedes their blocks, and the caller numbers the chunk's next versions from a later interval, since members may have logged the dropped ones. It returns ErrUnresolved, settling nothing, while a write or punch of the chunk waits to be ordered or has records neither durable nor failed.
func (s *Store) Settle(ctx *core.Context, c core.ChunkID, keep uint64, out *Output) (uint64, error) {
	return s.settle(ctx, c, keep, true, out)
}

// settle is Settle, which with commit false only drops, leaving the versions it keeps gated.
func (s *Store) settle(ctx *core.Context, c core.ChunkID, keep uint64, commit bool, out *Output) (uint64, error) {
	ci, ok := s.chunkOf[c]
	if !ok {
		return 0, nil
	}
	cs := &s.chunks[ci]
	if len(cs.waiting) > 0 || slices.ContainsFunc(cs.pending, func(oi int32) bool { return s.ops[oi].left > 0 }) {
		return 0, ErrUnresolved
	}
	top := keep
	if !s.plant.settleByNumber {
		top = 0
		for _, oi := range cs.pending {
			if o := &s.ops[oi]; o.req.Gated && !o.dropped && o.version <= keep && intervalOf(o.version) == intervalOf(keep) {
				top = max(top, o.version)
			}
		}
	}
	subset := s.plant.settleSubset
	for _, oi := range cs.pending {
		switch o := &s.ops[oi]; {
		case !o.req.Gated || o.dropped:
		case subset && o.version < top:
			o.dropped, subset = true, false
		case o.version <= top:
			o.committed = o.committed || commit
		default:
			o.dropped = true
			cs.version, cs.epoch = min(cs.version, o.prevVer), min(cs.epoch, o.prevEpoch)
		}
	}
	s.mapResolved(ctx, ci, out)
	return top, nil
}

// Ungate maps chunk c's gated writes and punches as a member maps what it logs, each once its records are durable, for a disk that no longer leads the chunk's PG; those whose records failed are never mapped.
func (s *Store) Ungate(ctx *core.Context, c core.ChunkID, out *Output) {
	ci, ok := s.chunkOf[c]
	if !ok {
		return
	}
	for _, oi := range s.chunks[ci].pending {
		if o := &s.ops[oi]; o.req.Gated && !o.dropped {
			o.committed = true
		}
	}
	s.mapResolved(ctx, ci, out)
}

// Unsettled appends to dst the chunks with gated writes or punches ordered and neither mapped nor dropped.
func (s *Store) Unsettled(dst []core.ChunkID) []core.ChunkID {
	for ci := range s.chunks {
		if cs := &s.chunks[ci]; cs.live && slices.ContainsFunc(cs.pending, func(oi int32) bool { return s.ops[oi].req.Gated }) {
			dst = append(dst, cs.id)
		}
	}
	return dst
}

// Persist logs the missing list the peering of interval decided for PG pg of the pool's pgs PGs,
// each chunk's kept version, with the image flag on the entries image holds, and after it the PG's decision marker, and answers id once all are
// durable, as the index takes them together: replay after a crash takes a list only with its
// marker, so a disk persists a decision whole or not at all. A PG count that is not a power of two,
// a PG or interval that does not exist or a chunk outside the PG is refused with ErrRange. Once
// the list is durable, each listed chunk's gated versions that its kept version does not keep are
// dropped, as replay drops them at the marker; those it keeps wait for Settle.
func (s *Store) Persist(ctx *core.Context, id uint64, pg, pgs, interval uint32, missing, image []ChunkVersion, out *Output) {
	err := s.unavailable()
	if err == nil && (core.CheckPGCount(pgs) != nil || pg >= pgs || interval == 0 ||
		slices.ContainsFunc(missing, func(m ChunkVersion) bool { return core.PGIndexOf(m.Chunk.Hash(), pgs) != pg })) {
		err = ErrRange
	}
	if err != nil {
		out.Replies = append(out.Replies, Reply{ID: id, Err: err})
		return
	}
	oi := s.newOp(opPersist, Request{ID: id})
	o := &s.ops[oi]
	for _, m := range missing {
		e := disklog.MissingEntry(s.cfg.Pool, m.Chunk, m.Version, interval)
		if slices.Contains(image, m) {
			e = disklog.ImageEntry(s.cfg.Pool, m.Chunk, m.Version, interval)
		}
		o.items = append(o.items, item{entry: e})
	}
	o.items = append(o.items, item{entry: disklog.DecisionEntry(s.cfg.Pool, pg, pgs, interval)})
	s.submitPersist(ctx, oi, out)
}

// Drop logs that chunk c, of a PG whose intervals up to interval an operator gave up, is dropped whole, its entry,
// listing and every map, and answers id once that is durable, when the index drops it. Its gated writes and punches
// are dropped first; while one of its writes or punches waits to be ordered or has records neither durable nor failed,
// or a member's waits to be mapped, it answers ErrUnresolved, dropping nothing.
func (s *Store) Drop(ctx *core.Context, id uint64, c core.ChunkID, interval uint32, out *Output) {
	err := s.unavailable()
	if err == nil && interval == 0 {
		err = ErrRange
	}
	if err == nil {
		if _, err = s.settle(ctx, c, 0, true, out); err == nil {
			if ci, ok := s.chunkOf[c]; ok && len(s.chunks[ci].pending) > 0 {
				err = ErrUnresolved
			}
		}
	}
	if err != nil {
		out.Replies = append(out.Replies, Reply{ID: id, Err: err})
		return
	}
	oi := s.newOp(opPersist, Request{ID: id})
	s.ops[oi].items = append(s.ops[oi].items, item{entry: disklog.DroppedEntry(s.cfg.Pool, c, interval)})
	s.submitPersist(ctx, oi, out)
}

// submitPersist logs the items of persist op oi.
func (s *Store) submitPersist(ctx *core.Context, oi int32, out *Output) {
	o := &s.ops[oi]
	o.left = len(o.items)
	s.persists = append(s.persists, oi)
	for j := range o.items {
		t, err := s.log.Submit(ctx, disklog.Hot, disklog.Item{Entry: s.ops[oi].items[j].entry}, &s.lout)
		if err != nil {
			s.ops[oi].items[j].state = itemFailed
			s.persisted(ctx, oi, err, out)
			continue
		}
		s.ops[oi].items[j].ticket = t
		s.sent = append(s.sent, sentItem{ticket: t, op: oi, item: int32(j)})
	}
	s.pump(ctx, out)
}

// persisted counts an item of persist oi resolved, with err if it failed; once all are, a persist
// whose items are all durable hands the index its list and marker, and its caller is answered.
func (s *Store) persisted(ctx *core.Context, oi int32, err error, out *Output) {
	o := &s.ops[oi]
	if o.err == nil {
		o.err = err
	}
	if o.left--; o.left > 0 {
		return
	}
	if o.err == nil {
		s.marks = s.marks[:0]
		for j := range o.items {
			s.marks = append(s.marks, o.items[j].entry)
		}
		s.apply = disklog.Event{Kind: disklog.Durable, Entries: s.marks}
		if err := s.ix.Apply(&s.apply); err != nil {
			panic("chunkstore: the index refused the store's own missing list: " + err.Error())
		}
		if last := o.items[len(o.items)-1].entry; last.Kind == disklog.EntryDropped {
			s.reportLive()
			if s.rec == nil {
				s.freeSlots(false)
			}
			if ci, ok := s.chunkOf[last.Chunk]; ok {
				cs := &s.chunks[ci]
				if ch, _ := s.ix.Chunk(last.Pool, last.Chunk); !(Chunk{Version: cs.version, Listed: ch.Listed, ListedIn: ch.ListedIn}).after(last.Interval) {
					cs.version, cs.epoch, cs.failed, cs.ordered = 0, 0, 0, 0
				}
			}
		} else if s.plant.clearListed {
			s.clearListed(last)
		}
		for j := range o.items[:len(o.items)-1] {
			e := &o.items[j].entry
			// No chunk is unresolved here: PeerState answered only once the PG's were, and nothing of
			// the PG is ordered again before activation.
			_, _ = s.settle(ctx, e.Chunk, e.Version, false, out)
		}
	}
	out.Replies = append(out.Replies, Reply{ID: o.req.ID, Err: o.err})
	s.persists = slices.DeleteFunc(s.persists, func(i int32) bool { return i == oi })
	s.freeOp(oi)
}

// clearListed clears the listings of decision d's PG from peerings before d's, for the clearListed plant.
func (s *Store) clearListed(d disklog.Entry) {
	for _, c := range s.ix.PGChunks(nil, d.Pool, d.Owner, uint32(d.Length)) {
		if ch, _ := s.ix.Chunk(d.Pool, c.Chunk); ch.ListedIn != 0 && ch.ListedIn < d.Interval {
			ch.Listed, ch.ListedIn, ch.Image = 0, 0, false
			s.ix.putChunk(d.Pool, c.Chunk, ch)
		}
	}
}

// PeerState appends to dst what a member reports to peering of PG pg of the pool's pgs PGs: each
// chunk of the PG it logged or lists, and the newest interval whose decision it persisted for the
// PG, 0 for none. A primary's gated writes and punches that wait for peering's decision count as
// logged, and as held whole up to the first whose records failed; its own recovery copy counts
// once it maps. Until every write and punch of the PG's chunks has its records durable or failed,
// those of a member mapped too, and every missing list being persisted is resolved, it returns
// ErrUnresolved, since what it reports would change after the member answered.
func (s *Store) PeerState(pg, pgs uint32, dst []ChunkPeer) ([]ChunkPeer, uint32, error) {
	if err := s.unavailable(); err != nil {
		return dst, 0, err
	}
	if core.CheckPGCount(pgs) != nil || pg >= pgs {
		return dst, 0, ErrRange
	}
	gated := false
	for i := range s.chunks {
		cs := &s.chunks[i]
		if !cs.live || core.PGIndexOf(cs.id.Hash(), pgs) != pg {
			continue
		}
		if len(cs.waiting) > 0 || slices.ContainsFunc(cs.pending, func(oi int32) bool { return s.ops[oi].left > 0 || !s.ops[oi].req.Gated }) {
			return dst, 0, ErrUnresolved
		}
		gated = gated || len(cs.pending) > 0
	}
	if len(s.persists) > 0 {
		return dst, 0, ErrUnresolved
	}
	n := len(dst)
	dst = s.ix.PGChunks(dst, s.cfg.Pool, pg, pgs)
	for i := range s.chunks {
		if cs := &s.chunks[i]; gated && cs.live && len(cs.pending) > 0 && core.PGIndexOf(cs.id.Hash(), pgs) == pg {
			dst = s.peerGated(dst, n, cs)
		}
	}
	return dst, s.ix.Decided(s.cfg.Pool, pg, pgs), nil
}

// peerGated folds chunk cs's gated writes and punches into its report among dst[from:], adding
// one for a chunk the index does not hold.
func (s *Store) peerGated(dst []ChunkPeer, from int, cs *chunkState) []ChunkPeer {
	j := slices.IndexFunc(dst[from:], func(c ChunkPeer) bool { return c.Chunk == cs.id })
	if j < 0 {
		dst = append(dst, ChunkPeer{Chunk: cs.id})
		j = len(dst) - 1
	} else {
		j += from
	}
	c := &dst[j]
	whole := true
	for _, oi := range cs.pending {
		o := &s.ops[oi]
		if o.dropped {
			continue
		}
		whole = whole && o.err == nil && !o.torn
		if o.req.Recover {
			// A primary's own recovery copy changes nothing it reports until it maps: the chunk stays listed until then.
			continue
		}
		if whole {
			c.Whole = max(c.Whole, o.version)
		}
		if o.holdsVersion() {
			c.Logged = max(c.Logged, o.version)
		}
	}
	return dst
}

// Cancel drops chunk c's write or punch of request id if it still waits to be ordered, reporting whether it did: it is never ordered, and is answered with ErrDropped once no base read of its own is in flight.
func (s *Store) Cancel(ctx *core.Context, c core.ChunkID, id uint64, out *Output) bool {
	ci, ok := s.chunkOf[c]
	if !ok {
		return false
	}
	for _, oi := range s.chunks[ci].waiting {
		if o := &s.ops[oi]; o.req.ID == id {
			if o.err == nil {
				o.err = ErrDropped
			}
			s.orderWaiting(ctx, ci, out)
			s.pump(ctx, out)
			return true
		}
	}
	return false
}

// Newest is chunk c's newest ordered version, counting the versions a member was handed and has yet to order and a primary's versions whose records failed, whose numbers are never given again: not the newest version the disk holds whole.
func (s *Store) Newest(c core.ChunkID) uint64 {
	if ci, ok := s.chunkOf[c]; ok {
		cs := &s.chunks[ci]
		v := cs.version
		for _, oi := range cs.waiting {
			v = max(v, s.ops[oi].req.Version)
		}
		return v
	}
	if s.ix == nil {
		return 0
	}
	ch, _ := s.ix.Chunk(s.cfg.Pool, c)
	return ch.Version
}

// Overwrites reports whether a write of bytes [off, off+n) of chunk c at snapshot epoch epoch adds no live data: every block it covers is in the chunk's live map, written after the chunk's latest freeze, and it needs no new freeze. It looks up the index once per run of blocks.
func (s *Store) Overwrites(c core.ChunkID, off, n uint32, epoch uint64) bool {
	if s.ix == nil || !validRange(off, n) {
		return false
	}
	ch, ok := s.ix.Chunk(s.cfg.Pool, c)
	if !ok || epoch > ch.Epoch {
		return false
	}
	var frozen uint64
	if ch.Epoch > 0 {
		frozen, _ = s.ix.FrozenAt(s.cfg.Pool, c, ch.Epoch)
	}
	first, last := spanOf(off, n)
	for b := int(first); b <= int(last); {
		e, ok := s.ix.Lookup(s.cfg.Pool, c, uint16(b))
		if !ok || e.LBA == 0 || e.Version <= frozen {
			return false
		}
		b = e.end()
	}
	return true
}

// SessionTable is the control plane's session table as a storage node checks writes against it:
// its version, the next session ID it hands out, and its live sessions in order.
type SessionTable struct {
	Version, Next uint64
	Live          []uint64
}

// Fenced reports whether session id was handed out and is no longer live.
func (t *SessionTable) Fenced(id uint64) bool {
	_, live := slices.BinarySearch(t.Live, id)
	return id != 0 && id < t.Next && !live
}

// Newer reports whether session id started after the table: its writes wait for a newer one.
func (t *SessionTable) Newer(id uint64) bool { return id != 0 && id >= t.Next }

// newer reports whether t is newer than u: a later version, or the same with later starts.
func (t *SessionTable) newer(u *SessionTable) bool {
	return t.Version > u.Version || t.Version == u.Version && t.Next > u.Next
}

// SetSessions applies t if it is newer than the table the store holds: from then on the store
// refuses, with ErrFenced when it orders them, the writes and punches of the sessions t fenced,
// those already queued included.
func (s *Store) SetSessions(t SessionTable) {
	if t.newer(&s.fence) {
		s.fence = t
	}
}

// Sessions is the session table the store holds, whose version its node's heartbeat reports.
func (s *Store) Sessions() *SessionTable { return &s.fence }

// SeeIncarnation records incarnation inc of a client session; the store refuses the older incarnations' writes and punches with ErrFenced when it orders them.
func (s *Store) SeeIncarnation(session, inc uint64) {
	if inc > s.sessions[session] {
		s.sessions[session] = inc
	}
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
	s.fetches[fi] = fetch{op: oi, first: first, n: n, version: e.Version, lba: e.BlockLBA(first), header: e.Header, data: data, hdr: hdr, hdrLen: 1, waiting: 2}
	s.ops[oi].reads++
	s.lout.Actions = append(s.lout.Actions,
		core.Action{Kind: core.ActDiskRead, IO: fetchIO(fi, 0), LBA: e.BlockLBA(first), Buf: data, Len: uint64(n) * core.BlockSize},
		core.Action{Kind: core.ActDiskRead, IO: fetchIO(fi, 1), LBA: e.Header, Buf: hdr, Len: core.BlockSize})
	return nil
}

func fetchIO(fi int32, part uint64) core.IOID {
	return core.IOID(fetchIOBit | uint64(fi)<<1 | part)
}

// OnEvent takes a disk completion, the log's TRIM timer or the store's AdmitTimer.
func (s *Store) OnEvent(ctx *core.Context, ev *core.Event, out *Output) {
	if ev.Kind == core.EvTimer && ev.Timer == AdmitTimer {
		s.adm.timerAt = 0
		s.pump(ctx, out)
		return
	}
	if ev.Kind == core.EvTimer && ev.Timer == disklog.TrimTimer && s.log != nil {
		s.log.OnEvent(ctx, ev, &s.lout)
		s.pump(ctx, out)
		return
	}
	if ev.Kind != core.EvDiskDone {
		return
	}
	s.completed(ev.IO)
	if ev.IO&fetchIOBit != 0 {
		s.fetchDone(ctx, int32(uint64(ev.IO)&^fetchIOBit>>1), ev.Err, out)
	} else if ev.IO&cleanIOBit != 0 {
		s.cleanReadDone(ctx, int(uint64(ev.IO)&^cleanIOBit), ev.Err)
	} else if s.rec != nil {
		s.rec.OnEvent(ctx, ev, &s.lout)
	} else if s.log != nil {
		s.log.OnEvent(ctx, ev, &s.lout)
	}
	s.pump(ctx, out)
}

// OnLog takes an event of the store's log from the loop that owns the log and hands it the log's
// disk completions and TRIM timer, which OnEvent then never sees; a nil ev follows each of them,
// and the store acts on the events of one completion only at that nil, as when it drives its log.
func (s *Store) OnLog(ctx *core.Context, ev *disklog.Event, out *Output) {
	if ev != nil {
		s.logEvent(ctx, ev, out)
		return
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
	remote := errors.Is(f.err, disklog.ErrBadChecksum) && o.kind != opRead && o.req.Gated && o.err == nil
	if remote {
		o.remote++
		out.Bases = append(out.Bases, BaseFetch{ID: o.req.ID, Chunk: s.chunks[o.chunk].id, Block: f.first, Version: f.version})
	}
	if f.err != nil && o.err == nil && !remote {
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
	if remote {
		return
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
	if o.kind == opRead && o.req.Recover {
		dst := ctx.Pool.Bytes(o.buf)
		copy(dst[recoveryAt(dst, f.first):], src)
		return
	}
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
	for i := range s.lout.Actions {
		if a := &s.lout.Actions[i]; (a.Kind == core.ActDiskRead || a.Kind == core.ActDiskWrite || a.Kind == core.ActDiskFsync) && (s.log == nil || a.IO >= EndLogIO) {
			s.issued = append(s.issued, issuedIO{a.IO, ctx.Now})
		}
	}
	out.Actions = append(out.Actions, s.lout.Actions...)
	s.lout.Reset()
}

type issuedIO struct {
	io core.IOID
	at core.Tick
}

// Oldest is the age at now of the oldest disk read, write or flush the store or its log issued that has not completed, 0 if none; trims are left out.
func (s *Store) Oldest(now core.Tick) core.Tick {
	var age core.Tick
	if len(s.issued) > 0 {
		age = now - min(s.issued[0].at, now)
	}
	if s.log != nil {
		age = max(age, s.log.Oldest(now))
	}
	return age
}

func (s *Store) completed(io core.IOID) {
	if i := slices.IndexFunc(s.issued, func(x issuedIO) bool { return x.io == io }); i >= 0 {
		s.issued = slices.Delete(s.issued, i, i+1)
	}
}

func (s *Store) housekeep(ctx *core.Context, out *Output) {
	switch s.phase {
	case serving:
		if s.log.Err() != nil {
			s.stop(ctx, s.log.Err(), out)
			return
		}
		victim := s.clean(ctx)
		s.admitHeld(ctx, out)
		if s.log.Stalled() && (!victim || s.log.CopiesStalled() || s.plant.noSpaceWithVictim) {
			s.log.FailStalled(ctx, &s.lout)
			if s.raft != nil {
				s.stop(ctx, disklog.ErrNoSpace, out)
			}
			return
		}
		if s.raft != nil {
			if err := s.raft.Retry(ctx, &s.lout); err != nil {
				s.stop(ctx, err, out)
				return
			}
			if err := s.raft.WritePieces(ctx, &s.lout); err != nil {
				s.stop(ctx, err, out)
				return
			}
			s.raft.MailShares(s.raftBytes())
			out.Mail = s.raft.Mail(out.Mail)
		}
		if s.pieceWait {
			s.nextPiece(ctx, out)
		}
		s.endCheckpoint(ctx, out)
		if !s.checkpointing {
			s.removeShards()
		}
		if !s.checkpointing && !s.retryAfter {
			if due := s.log.CheckpointDue() || s.fullDue(); due || s.stepUnnamed() || s.raft != nil && s.raft.GoneWaiting() {
				s.beginCheckpoint(ctx, due, out)
			}
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
			if s.copyResolved(t, ev, -1) {
				continue
			}
			if si, ok := s.popSent(t); ok {
				err := ev.Err
				if ev.Kind == disklog.InDoubt {
					err = ErrInDoubt
					s.ops[si.op].items[si.item].buf = core.NoBuf
				}
				if s.ops[si.op].kind == opPersist {
					s.ops[si.op].items[si.item].state = itemFailed
					s.persisted(ctx, si.op, err, out)
					continue
				}
				if s.itemFailed(ctx, si.op, si.item, err, out) {
					s.mapResolved(ctx, s.ops[si.op].chunk, out)
				}
			}
		}
		if ev.Kind == disklog.InDoubt && s.phase == serving {
			s.stop(ctx, ErrInDoubt, out)
		}
	case disklog.CheckpointDone:
		dead, err := s.ix.Tree().CheckpointDone()
		if err != nil {
			panic("chunkstore: checkpoint done with no flush running: " + err.Error())
		}
		s.piecesDead(dead)
		s.checkpointing, s.rebuilt = false, false
		s.shardsCheckpointed(ctx, out)
	case disklog.CheckpointFailed:
		s.piecesDead(s.ix.Tree().FlushFailed())
		s.shardFlushesFailed()
		s.checkpointing, s.inPiece, s.piecesOut, s.retryAfter, s.pieceWait, s.piecesDone = false, false, 0, true, false, false
	case disklog.ManifestLoaded:
		s.manifestLoaded(ctx)
	case disklog.BlobRead:
		s.blobRead(ctx, ev)
	case disklog.Replayed:
		if s.raft != nil {
			if err := s.raft.Replayed(ev); err != nil {
				s.recoveryFailed(ctx, err)
				return
			}
			out.Mail = s.raft.Mail(out.Mail)
		}
		if s.plant.replayCopyAsWrite {
			for i := range ev.Entries {
				if e := &ev.Entries[i]; e.Kind == disklog.EntryCopy {
					*e = disklog.WriteEntry(e.Pool, e.Chunk, e.Version, e.First, e.Count)
				}
			}
		}
		if ev.LSN > s.replayLSN && s.plant.replayLate && !s.plant.skipped && slices.ContainsFunc(ev.Entries, func(e disklog.Entry) bool { return e.Kind != disklog.EntryBlob }) {
			s.plant.skipped = true
			return
		}
		if s.plant.rebuildSkipsFreezes && s.rebuilt {
			ev.Entries = slices.DeleteFunc(ev.Entries, func(e disklog.Entry) bool { return e.Kind == disklog.EntryFreeze })
		}
		if s.plant.replayDropsPunches {
			ev.Entries = slices.DeleteFunc(ev.Entries, func(e disklog.Entry) bool { return e.Kind == disklog.EntryPunch })
		}
		if ev.LSN > s.replayLSN {
			if err := s.replay(ev); err != nil {
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
		// After resume's Recount: replay counted again the blobs that died before the manifest.
		if s.raft != nil {
			s.raft.Recovered(log)
			out.Mail = s.raft.Mail(out.Mail)
		}
		s.gateHeld()
		s.freeSlots(true)
	case disklog.RecoveryFailed:
		s.recoveryFailed(ctx, ev.Err)
	}
}

// replay applies a replayed record's entries in order, holding back each write and punch of an
// interval at or past its PG's newest persisted decision: that decision's peering, or a later
// one, may drop it, and only the next marker says which. A decision marker settles the held
// versions of its PG, and a drop drops those of its chunk.
func (s *Store) replay(ev *disklog.Event) error {
	for i := range ev.Entries {
		e := &ev.Entries[i]
		if s.gates(e) {
			s.held = append(s.held, item{entry: *e, state: itemDurable, lba: ev.LBA, headerBlocks: ev.HeaderBlocks, payload: ev.Payloads[i], lsn: ev.LSN})
			continue
		}
		if e.Kind == disklog.EntryRecovered {
			s.recoveredHeld(e)
		}
		if e.Kind == disklog.EntryDropped {
			s.held = slices.DeleteFunc(s.held, func(it item) bool {
				if it.entry.Pool != e.Pool || it.entry.Chunk != e.Chunk {
					return false
				}
				s.dropSlot(&it)
				return true
			})
		}
		s.apply = disklog.Event{Kind: ev.Kind, LSN: ev.LSN, LBA: ev.LBA, HeaderBlocks: ev.HeaderBlocks, Entries: ev.Entries[i : i+1], Payloads: ev.Payloads[i : i+1]}
		if err := s.ix.Apply(&s.apply); err != nil {
			return err
		}
		if e.Kind == disklog.EntryDecision {
			s.settleHeld(e)
		}
	}
	return nil
}

// gates reports whether replay holds entry e back: a write, punch or writer of an interval at or past the
// newest decision the disk persisted for a PG holding its chunk, at any PG count.
func (s *Store) gates(e *disklog.Entry) bool {
	if e.Kind != disklog.EntryWrite && e.Kind != disklog.EntryPunch && e.Kind != disklog.EntrySlot && e.Kind != disklog.EntryWriter || s.plant.mapOnReplay {
		return false
	}
	// ponytail: up to 32 index lookups per replayed entry; cache per PG if replay time matters.
	const finest = 1 << 31
	d := s.ix.Decided(e.Pool, core.PGIndexOf(e.Chunk.Hash(), finest), finest)
	return d != 0 && intervalOf(e.Version) >= d
}

// recoveredHeld marks the last held entry of recovery e's write as its version's last: a recovery
// write's entries carry no such mark, its recovered entry, logged after them, saying it is whole.
func (s *Store) recoveredHeld(e *disklog.Entry) {
	for j := len(s.held) - 1; j >= 0; j-- {
		if h := &s.held[j].entry; h.Pool == e.Pool && h.Chunk == e.Chunk && h.Version == e.Version {
			h.Last = true
			return
		}
	}
}

// settleHeld settles the held versions of decision marker d's PG as Settle does, by the version
// the marker's list keeps if it lists the chunk and by the newest held otherwise: those kept are
// mapped, the others dropped.
func (s *Store) settleHeld(d *disklog.Entry) {
	pgs := uint32(d.Length)
	var pg []item
	s.held = slices.DeleteFunc(s.held, func(it item) bool {
		if it.entry.Pool != d.Pool || core.PGIndexOf(it.entry.Chunk.Hash(), pgs) != d.Owner {
			return false
		}
		pg = append(pg, it)
		return true
	})
	slices.SortStableFunc(pg, func(a, b item) int { return compareChunk(a.entry.Chunk, b.entry.Chunk) })
	for len(pg) > 0 {
		c := pg[0].entry.Chunk
		n := 1 + slices.IndexFunc(pg[1:], func(it item) bool { return it.entry.Chunk != c })
		if n == 0 {
			n = len(pg)
		}
		run := pg[:n]
		pg = pg[n:]
		keep := uint64(0)
		if ch, _ := s.ix.Chunk(d.Pool, c); ch.ListedIn == d.Interval {
			keep = ch.Listed
		} else {
			for _, it := range run {
				keep = max(keep, it.entry.Version)
			}
		}
		top := keep
		if !s.plant.settleByNumber {
			top = 0
			for _, it := range run {
				if v := it.entry.Version; v <= keep && intervalOf(v) == intervalOf(keep) {
					top = max(top, v)
				}
			}
		}
		for k := range run {
			if run[k].entry.Version <= top || s.plant.replayForgetsDrops {
				s.mapItem(&run[k])
			} else {
				s.dropSlot(&run[k])
			}
		}
	}
}

// gateHeld hands the versions replay still holds to the store as gated writes and punches,
// whose records are durable and whose clients are gone, for the next decision to settle.
func (s *Store) gateHeld() {
	for k := range s.held {
		it := &s.held[k]
		ci := s.chunkSlot(it.entry.Chunk)
		cs := &s.chunks[ci]
		if n := len(cs.pending); n == 0 || s.ops[cs.pending[n-1]].version != it.entry.Version {
			kind := opWrite
			if it.entry.Kind == disklog.EntryPunch {
				kind = opPunch
			}
			oi := s.newOp(kind, Request{Chunk: it.entry.Chunk, Version: it.entry.Version, Gated: true})
			o := &s.ops[oi]
			o.chunk, o.version, o.prevVer, o.prevEpoch, o.answered, o.torn = ci, it.entry.Version, cs.version, cs.epoch, true, true
			cs.pending = append(cs.pending, oi)
			cs.version, cs.ordered = it.entry.Version, it.entry.Version
		}
		o := &s.ops[cs.pending[len(cs.pending)-1]]
		if len(o.items) == 1 && o.items[0].entry.Kind == disklog.EntryWriter && it.entry.Kind == disklog.EntryPunch {
			o.kind = opPunch
		}
		o.items = append(o.items, *it)
		if it.entry.Kind == disklog.EntrySlot {
			s.pinned[s.ix.slotLBA(&it.entry)]++
		}
		if it.entry.Last {
			o.torn = false
		}
	}
	s.held = s.held[:0]
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
			tree, _ := s.tree(s.pieceTree)
			if err := tree.PieceDurable(ref); err != nil {
				panic("chunkstore: piece durable out of turn: " + err.Error())
			}
			s.nextPiece(ctx, out)
			continue
		}
		if s.copyResolved(t, ev, i) {
			continue
		}
		si, ok := s.popSent(t)
		if !ok {
			continue
		}
		it := &s.ops[si.op].items[si.item]
		it.entry, it.state, it.lba, it.headerBlocks, it.payload, it.lsn = ev.Entries[i], itemDurable, ev.LBA, ev.HeaderBlocks, ev.Payloads[i], ev.LSN
		if s.ops[si.op].kind == opPersist {
			s.persisted(ctx, si.op, nil, out)
			continue
		}
		if s.ops[si.op].req.Gated {
			s.unmappedLive(it, -1)
			if s.plant.writerAtLog && it.entry.Kind == disklog.EntryWriter {
				whole := *it
				whole.entry.Last = true
				s.mapItem(&whole)
			}
		}
		if s.itemDone(si.op, out) {
			s.mapResolved(ctx, s.ops[si.op].chunk, out)
		}
	}
	if s.raft != nil {
		var err error
		if out.Rounds, err = s.raft.Durable(ctx, ev, &s.lout, out.Rounds); err != nil {
			s.stop(ctx, err, out)
		}
	}
}

func (s *Store) itemFailed(ctx *core.Context, oi, ii int32, err error, out *Output) bool {
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
	return s.itemDone(oi, out)
}

// itemDone counts an item of the op resolved and reports whether all are; a failed op gives back
// the chunk's epoch when its freeze did not become durable, and its version when none of its items
// did, unless it is gated: a primary may have sent a gated version to members, so it is not given
// back here. Its number can still return once releaseChunk forgets the chunk; the replica ends the
// interval, and the next numbers from a later one.
func (s *Store) itemDone(oi int32, out *Output) bool {
	o := &s.ops[oi]
	if o.left--; o.left > 0 {
		return false
	}
	if o.req.Gated {
		out.Logged = append(out.Logged, Reply{ID: o.req.ID, Version: o.version, Err: o.err})
	}
	cs := &s.chunks[o.chunk]
	if o.err != nil {
		s.memberFailed(o)
		// The writes ordered after a failed one may hold its bytes; the log fails them too, since
		// it fails a stalled lane's queued items together in submit order, both hot lanes' at once.
		if !o.holdsVersion() && !o.req.Gated {
			cs.version = min(cs.version, o.prevVer)
		}
		if len(o.items) > 0 && o.items[0].entry.Kind == disklog.EntryFreeze && o.items[0].state != itemDurable {
			cs.epoch = min(cs.epoch, o.prevEpoch)
		}
	}
	return true
}

// memberFailed notes that op o failed if it is a member's, so the chunk's later versions of its interval waiting behind it are refused.
func (s *Store) memberFailed(o *op) {
	if cs := &s.chunks[o.chunk]; o.req.Version != 0 && (cs.failed == 0 || o.req.Version < cs.failed) {
		cs.failed = o.req.Version
	}
}

func (o *op) holdsVersion() bool {
	for j := range o.items {
		if it := &o.items[j]; it.state == itemDurable && it.entry.Kind != disklog.EntryFreeze && it.entry.Kind != disklog.EntryWriter {
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
		if o.left > 0 || o.req.Gated && o.err == nil && !o.committed && !o.dropped {
			break
		}
		mapped := o.req.Gated && (o.err != nil || o.dropped || o.mapped)
		if !mapped && o.err == nil && s.rec == nil {
			s.remember(ctx, o)
		}
		for j := range o.items {
			it := &o.items[j]
			if it.state == itemDurable && s.rec == nil && !mapped {
				if o.req.Gated {
					s.unmappedLive(it, 1)
				}
				s.mapItem(it)
			}
			if o.dropped && !o.mapped {
				s.dropSlot(it)
			}
			if o.req.Gated && it.entry.Kind == disklog.EntrySlot {
				s.unpin(s.ix.slotLBA(&it.entry))
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
		if o.dropped && o.err == nil {
			r.Err = ErrDropped
		}
		if !o.answered {
			out.Replies = append(out.Replies, r)
		}
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
// TRIMs it under the read. A slot a held write names waits too: replay can hold both a version that
// overwrote the slot and another chunk's later write the log gave it to, and mapping the first takes
// away the index's last reference while the second still names it.
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
		case s.reading(lba) || s.pinned[lba] > 0:
			waiting = append(waiting, lba)
		default:
			if err := s.log.FreeSlot(lba, &s.lout); err != nil && !recovered {
				panic("chunkstore: the log refused a chunk slot the index freed: " + err.Error())
			}
			if s.plant.freeNextSlot {
				_ = s.log.FreeSlot(lba+core.BlocksPerChunk, &s.lout)
			}
		}
	}
	s.freed = waiting
}

// dropSlot queues for freeSlots the chunk slot of a durable slot write it dropped: no map ever
// refers to it, so the index never frees it. The slot may already wait in s.freed on this very
// write, and TakeFreed keeps out only the duplicates it would add itself, so dropSlot checks.
func (s *Store) dropSlot(it *item) {
	if it.entry.Kind != disklog.EntrySlot || it.state != itemDurable {
		return
	}
	if lba := s.ix.slotLBA(&it.entry); !slices.Contains(s.freed, lba) {
		s.freed = append(s.freed, lba)
	}
}

// unpin ends one held slot write's pin on slot.
func (s *Store) unpin(slot core.LBA) {
	if n := s.pinned[slot] - 1; n > 0 {
		s.pinned[slot] = n
	} else {
		delete(s.pinned, slot)
	}
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

// readsFrom reports whether a read in flight reads data or a record header from segment seg.
func (s *Store) readsFrom(seg uint32) bool {
	n := core.LBA(s.sb.SegmentBlocks)
	for i := range s.fetches {
		if f := &s.fetches[i]; f.lba != 0 && (uint32(f.lba/n) == seg || uint32(f.header/n) == seg) {
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
	s.reportLive()
}

// unmappedLive takes a gated item's blocks out of its segment's live count while the index does not map it (sign -1), or puts them back as it does (+1): the log counts a record's blocks live once durable.
func (s *Store) unmappedLive(it *item, sign int) {
	if it.entry.Kind != disklog.EntryWrite && it.entry.Kind != disklog.EntrySlot || it.entry.Count == 0 {
		return
	}
	if err := s.log.AddLive(uint32(uint64(it.lba)/uint64(s.sb.SegmentBlocks)), sign*int(it.entry.Count)); err != nil {
		panic("chunkstore: the usage table refused a gated item's count: " + err.Error())
	}
}

// reportLive hands the log's usage table the changes to record counts the index made.
func (s *Store) reportLive() {
	s.changes = s.ix.TakeChanges(s.changes[:0])
	for _, c := range s.changes {
		if err := s.log.AddLive(uint32(uint64(c.Header)/uint64(s.sb.SegmentBlocks)), int(c.Delta)); err != nil {
			panic("chunkstore: the usage table refused a record count change: " + err.Error())
		}
	}
}

// piecesDead hands the log the run pieces the index no longer names.
func (s *Store) piecesDead(refs []disklog.BlobRef) {
	if err := s.log.PiecesDead(refs); err != nil {
		panic("chunkstore: the log does not hold a piece the index wrote: " + err.Error())
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

// beginCheckpoint starts a checkpoint: one that is due flushes the memtable, and one begun only
// for a merge step writes the step and a manifest, keeping the replay LSN.
func (s *Store) beginCheckpoint(ctx *core.Context, due bool, out *Output) {
	tree := s.ix.Tree()
	var froze bool
	var err error
	if due {
		froze, err = tree.BeginFlush()
	} else {
		err = tree.BeginStepFlush()
	}
	if err != nil {
		panic("chunkstore: flush begun while one runs: " + err.Error())
	}
	if froze || due && !tree.Frozen() {
		s.replayLSN = s.replayPoint()
	}
	s.idx = append(s.idx[:0], disklog.CheckpointIndex{Owner: indexOwnerID, ReplayLSN: s.replayLSN, Runs: tree.CarriedRuns(), Bounds: tree.Bounds()})
	s.idx, err = s.beginShards(ctx, due, s.log.DurableLSN(), s.idx)
	if err != nil {
		s.piecesDead(tree.FlushFailed())
		s.shardFlushesFailed()
		s.stop(ctx, err, out)
		return
	}
	begin := s.log.BeginCheckpoint
	if !due {
		begin = s.log.BeginStepCheckpoint
	}
	if err := begin(s.idx); err != nil {
		s.piecesDead(tree.FlushFailed())
		s.shardFlushesFailed()
		return
	}
	s.checkpointing, s.pieceTree, s.piecesDone = true, 0, false
	s.beginRemote(due, out)
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
	tree, owner := s.tree(s.pieceTree)
	length, root, err := tree.NextPiece(ctx.Pool.Bytes(buf))
	for err == nil && length == 0 && s.pieceTree < s.cpShards {
		s.pieceTree++
		tree, owner = s.tree(s.pieceTree)
		length, root, err = tree.NextPiece(ctx.Pool.Bytes(buf))
	}
	if err == nil && (s.plant.failCheck || s.plant.failChecks) {
		s.plant.failCheck, err = false, lsm.ErrCheck
	}
	if err != nil {
		ctx.Pool.Free(buf)
		s.indexBroken(ctx, err, out)
		return
	}
	if length == 0 {
		ctx.Pool.Free(buf)
		s.piecesDone = true
		s.endCheckpoint(ctx, out)
		return
	}
	if w := tree.PieceWorker(); w > 0 {
		s.piece, err = s.log.MergePiece(ctx, w, owner, buf, uint64(length), root, &s.lout)
	} else {
		s.piece, err = s.log.CheckpointPiece(ctx, owner, buf, uint64(length), root, &s.lout)
	}
	if err != nil {
		ctx.Pool.Free(buf)
		s.stop(ctx, err, out)
		return
	}
	s.inPiece = true
	s.piecesOut++
}

// endCheckpoint ends the running checkpoint once its trees on this loop and the shards on the
// round driver have handed the log all their pieces.
func (s *Store) endCheckpoint(ctx *core.Context, out *Output) {
	if !s.piecesDone || s.raft != nil && s.raft.Flushing() {
		return
	}
	s.piecesDone = false
	if err := s.log.EndCheckpoint(ctx, &s.lout); err != nil {
		s.stop(ctx, err, out)
	}
}

// MergeDue returns the index's next merge step, whose Run may go to its merge worker off the
// loop, or nil: a running merge's next step once the manifest naming its last is durable, else a
// new merge on a free worker, a rewrite the cleaner marked first.
func (s *Store) MergeDue() *lsm.Compaction {
	if s.phase != serving {
		return nil
	}
	return s.sched.Next()
}

// InstallMerge installs a merge step MergeDue returned, which the next checkpoint writes to the
// merge worker's lane, beginning one if none is due; a step that fails its run check rebuilds the
// index from the log, and one of a shard RemoveRaft removed since is dropped with lsm.ErrStale.
func (s *Store) InstallMerge(ctx *core.Context, c *lsm.Compaction, out *Output) error {
	if s.phase != serving {
		return ErrUnavailable
	}
	var err error
	if s.plant.failMerge {
		s.plant.failMerge, err = false, lsm.ErrCheck
	} else {
		err = s.sched.Install(c)
	}
	if errors.Is(err, lsm.ErrCheck) {
		s.indexBroken(ctx, err, out)
	}
	s.pump(ctx, out)
	return err
}

// indexBroken starts a rebuild of the index from the log: the store stops taking operations,
// lets the log's rounds in flight finish and recovers its log again as an open does. A check that
// fails again before a checkpoint completes after the rebuild stops the store with err.
func (s *Store) indexBroken(ctx *core.Context, err error, out *Output) {
	if s.rebuilt || s.raft != nil {
		s.stop(ctx, err, out)
		return
	}
	s.phase, out.Rebuilt = draining, true
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
	s.phase, s.rebuilt = recovering, true
	s.resetCleaner(ctx)
	s.abandonPending(ctx, out)
	if err := s.recover(ctx, s.sb.Geometry.DiskBlocks(), disklog.ModeOpen); err != nil {
		s.phase, s.err = failed, err
	}
}

// abandonPending answers in doubt the writes and punches still pending once the log closed for a
// rebuild, gated ones waiting for a commit and those behind them: replay maps or gates every
// durable record again, as after a restart.
func (s *Store) abandonPending(ctx *core.Context, out *Output) {
	for ci := range s.chunks {
		cs := &s.chunks[ci]
		if !cs.live {
			continue
		}
		for _, oi := range cs.pending {
			s.freeBufs(ctx, oi)
			o := &s.ops[oi]
			for j := range o.items {
				if it := &o.items[j]; it.buf != core.NoBuf {
					ctx.Pool.Free(it.buf)
					it.buf = core.NoBuf
				}
			}
			r := Reply{ID: o.req.ID, Err: ErrInDoubt}
			if o.holdsVersion() {
				r.Version = o.version
			}
			if !o.answered {
				out.Replies = append(out.Replies, r)
			}
			s.freeOp(oi)
		}
		cs.pending = cs.pending[:0]
		s.releaseChunk(int32(ci))
	}
	for _, oi := range s.persists {
		out.Replies = append(out.Replies, Reply{ID: s.ops[oi].req.ID, Err: ErrInDoubt})
		s.freeOp(oi)
	}
	s.persists = s.persists[:0]
	clear(s.covers)
	clear(s.pinned)
	s.checkFlushes(out)
}

func (s *Store) fetchesOut() int { return len(s.fetches) - len(s.freeFetch) + s.cleanReadsOut() }

func (s *Store) stop(ctx *core.Context, err error, out *Output) {
	s.phase, s.err = failed, err
	s.failWaiting(ctx, err, out)
}

func (s *Store) failWaiting(ctx *core.Context, err error, out *Output) {
	s.dropHeld()
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
	if err := s.newIndex(s.rec.MaxBlobBytes(), s.rec.Superblock().Geometry); err != nil {
		s.recoveryFailed(ctx, err)
		return
	}
	if m, ok := s.rec.Manifest(); ok && s.mode == disklog.ModeOpen {
		if err := s.restoreReplicas(&m); err != nil {
			s.recoveryFailed(ctx, err)
			return
		}
		for _, mi := range m.Indexes {
			tree := s.ix.Tree()
			if k := s.shardOf(mi.Owner); k >= 0 {
				tree = s.shards[k].sh.Tree()
			} else if s.raft != nil && s.raft.Remote(mi.Owner) {
				tree = nil
			} else if mi.Owner != indexOwnerID {
				continue
			} else {
				s.replayLSN = mi.ReplayLSN
			}
			for _, root := range mi.Runs {
				l := &loadingRun{tree: tree, owner: mi.Owner, got: map[disklog.BlobRef][]byte{}}
				if k := slices.IndexFunc(mi.Bounds, func(b disklog.MergeBound) bool { return b.Run == root }); k >= 0 {
					l.bound = &mi.Bounds[k]
				}
				s.loading[root] = l
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
		if l.bound != nil {
			refs = refs[min(int(l.bound.Dead), len(refs)):]
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
		pieces = append(pieces, l.root)
		var err error
		if l.tree == nil {
			s.raft.LoadedRun(l.owner, append(l.refs, root), pieces, l.bound)
		} else if l.bound != nil {
			err = l.tree.LoadBoundedRun(*l.bound, pieces)
		} else {
			err = l.tree.LoadRun(root, pieces)
		}
		if err != nil {
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
	if err := s.ix.CheckChunks(); err != nil {
		s.recoveryFailed(ctx, err)
		return
	}
	if m, ok := s.rec.Manifest(); ok && s.mode == disklog.ModeOpen {
		if err := s.restoreShards(&m); err != nil {
			s.recoveryFailed(ctx, err)
			return
		}
	}
	if err := s.rec.Replay(ctx, &s.lout); err != nil {
		s.recoveryFailed(ctx, err)
	}
}
