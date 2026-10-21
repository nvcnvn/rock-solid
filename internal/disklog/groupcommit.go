package disklog

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/core"
)

const maxRecordEntries = math.MaxUint16

var (
	// ErrNoSpace reports an item no free segment is left to hold.
	ErrNoSpace = errors.New("no free segment")
	// ErrBusy reports a checkpoint begun while another runs.
	ErrBusy = errors.New("a checkpoint is already running")
	// ErrNoCheckpoint reports a checkpoint piece or end with no checkpoint taking pieces.
	ErrNoCheckpoint = errors.New("no checkpoint is taking pieces")
	// ErrUnknownOwner reports a checkpoint piece for an index the checkpoint did not name.
	ErrUnknownOwner = errors.New("index not named by the checkpoint")
	// ErrBadBuffer reports an item whose buffer does not match its entry.
	ErrBadBuffer = errors.New("buffer does not match its entry")
	// ErrReservedKind reports an item of a blob kind only a checkpoint writes.
	ErrReservedKind = errors.New("blob kind reserved for checkpoints")
	// ErrBadConfig reports settings the log cannot run with.
	ErrBadConfig = errors.New("bad group commit settings")
	// ErrFlushBeforeWrite reports a driver that completed a round's flush before its write.
	ErrFlushBeforeWrite = errors.New("flush completed before its write")
	// ErrClosed reports a log whose buffers were returned.
	ErrClosed = errors.New("log closed")
)

// Config is the group commit settings.
type Config struct {
	// MaxRecordBytes is max_group_commit_bytes, the most a round writes, segment header included; at least 3 blocks.
	MaxRecordBytes uint64
	// MaxReplayBytes is max_replay_bytes, the size of the records after the newest manifest's base that makes a checkpoint due.
	MaxReplayBytes uint64
	// CommitDepth is commit_depth, the most rounds in flight at once; at least 1.
	CommitDepth int
	// Check decodes every record a round encodes and panics unless it holds exactly what was submitted.
	Check bool
	// ReadDepth is the most segment-header reads recovery keeps in flight; at least 1.
	ReadDepth int
	// MergeWorkers is merge_workers as MergeWorkers resolves it; each worker writes to a lane of its own, and the reserve holds a step of each.
	MergeWorkers int
	// CompactionStepBytes is compaction_step_bytes, the most output a merge installs at once.
	CompactionStepBytes uint64
	// Reserve, if set, replaces the reserve ReserveSegments sizes from these settings and the disk's segments.
	Reserve uint32
	// NoReserve runs the log with no reserve, which only tests ask for.
	NoReserve bool
}

const (
	victimsPerPass = 4
	minReserve     = 8
)

const (
	// DefaultMergeWorkers is merge_workers when the setting is 0.
	DefaultMergeWorkers = 1
	// MaxMergeWorkers is the most merge_workers may be, since a segment header names a merge worker's lane in one byte.
	MaxMergeWorkers = math.MaxUint8
)

// MergeWorkers is the merges a disk runs at once for the merge_workers setting n, which the log and the merge scheduler both resolve here: DefaultMergeWorkers for 0, and not ok below 0 or above MaxMergeWorkers.
func MergeWorkers(n int) (int, bool) {
	switch {
	case n < 0 || n > MaxMergeWorkers:
		return 0, false
	case n == 0:
		return DefaultMergeWorkers, true
	}
	return n, true
}

// ReserveSegments is the reserve for segments of segmentBlocks blocks: a checkpoint at its largest, a step per merge worker and a cleaner pass, and at least 8.
func ReserveSegments(maxReplayBytes, stepBytes uint64, mergeWorkers int, segmentBlocks uint32) uint32 {
	seg := uint64(segmentBlocks) * core.BlockSize
	hi, steps := bits.Mul64(uint64(mergeWorkers), stepBytes)
	held, c1 := bits.Add64(maxReplayBytes, maxReplayBytes, 0)
	held, c2 := bits.Add64(held, steps, 0)
	held, c3 := bits.Add64(held, seg-1, 0)
	if hi|c1|c2|c3 != 0 {
		return math.MaxUint32
	}
	return uint32(min(max(held/seg+victimsPerPass, minReserve), math.MaxUint32))
}

// DefaultConfig is the default settings, with the decode check off.
func DefaultConfig() Config {
	return Config{MaxRecordBytes: 1 << 20, MaxReplayBytes: 64 << 20, CommitDepth: 1, ReadDepth: 64, CompactionStepBytes: 64 << 20}
}

// settled reports whether the merge workers are in range and the reserve settings name one
// reserve, before any disk's geometry is known.
func (c *Config) settled() bool {
	if _, ok := MergeWorkers(c.MergeWorkers); !ok {
		return false
	}
	switch {
	case c.NoReserve:
		return c.Reserve == 0
	case c.Reserve != 0:
		return true
	default:
		return c.CompactionStepBytes > 0
	}
}

// reserve is the free segments only checkpoints may open on a disk of sb's geometry, sized for a
// checkpoint, the merge steps and a cleaner pass, which must leave a segment for everything else.
func (c *Config) reserve(sb *Superblock) (uint32, error) {
	var n uint32
	switch {
	case !c.settled():
		return 0, ErrBadConfig
	case c.NoReserve:
	case c.Reserve != 0:
		n = c.Reserve
	default:
		workers, _ := MergeWorkers(c.MergeWorkers)
		n = ReserveSegments(c.MaxReplayBytes, c.CompactionStepBytes, workers, sb.SegmentBlocks)
	}
	if n >= sb.SegmentCount-1 {
		return 0, ErrBadConfig
	}
	return n, nil
}

// Ticket names one submitted item in the log's events; NoTicket marks the log's own manifest pieces.
type Ticket uint64

// NoTicket is the ticket events give the log's own items.
const NoTicket Ticket = 0

// Item is a write's Count blocks in a buffer of exactly that size, a blob's Length bytes at the start of its buffer, or a bufferless punch or freeze.
type Item struct {
	Entry Entry
	Buf   core.IOBuf
	// Keep leaves a write's buffer with its submitter, who frees it once the item is Durable or Failed; the log frees it for an item in doubt.
	Keep bool
}

// EventKind tags an Event.
type EventKind uint8

// Event kinds.
const (
	// Durable reports a durable record; Tickets[i] submitted Entries[i], whose payload starts Payloads[i] bytes into the data area.
	Durable EventKind = iota + 1
	// Failed reports items that were never written and never will be, and why.
	Failed
	// InDoubt reports items whose round was in flight when the disk failed; recovery decides whether they are durable.
	InDoubt
	// CheckpointDone reports the running checkpoint's manifest durable at Manifest.
	CheckpointDone
	// CheckpointFailed reports that the running checkpoint will never finish; a new one may begin.
	CheckpointFailed
	// ManifestLoaded reports that recovery read the newest manifest, if any; blob reads and Replay may follow.
	ManifestLoaded
	// BlobRead reports the blob at Ref, whose bytes are Data.
	BlobRead
	// Replayed reports one durable record in LSN order, as Durable does, with its blob bytes in Data.
	Replayed
	// Recovered reports that recovery finished with LSN the newest durable record; Log resumes group commit.
	Recovered
	// RecoveryFailed reports that recovery stopped with Err and wrote nothing more.
	RecoveryFailed
)

// Event is one outcome of the log; its slices stay valid until the Output carrying it is reset.
type Event struct {
	Kind         EventKind
	LSN          uint64
	Segment      uint32
	Block        uint32
	LBA          core.LBA
	HeaderBlocks uint32
	Entries      []Entry
	Tickets      []Ticket
	Payloads     []uint64
	Err          error
	Manifest     ManifestPos
	Ref          BlobRef
	Data         []byte
}

// Blob is the payload of blob entry i of a Replayed event.
func (ev *Event) Blob(i int) []byte {
	var writes uint64
	for j := range ev.Entries {
		if ev.Entries[j].Kind == EntryWrite {
			writes += ev.Entries[j].dataBytes()
		}
	}
	start := ev.Payloads[i] - writes
	return ev.Data[start : start+ev.Entries[i].Length]
}

// Output is the actions and events of calls into the log, reused by the caller so a call allocates nothing once it has grown.
type Output struct {
	Actions  []core.Action
	Events   []Event
	entries  []Entry
	tickets  []Ticket
	payloads []uint64
	data     []byte
}

// Reset empties the output for reuse.
func (o *Output) Reset() {
	o.Actions = o.Actions[:0]
	clear(o.Events)
	o.Events = o.Events[:0]
	o.entries = o.entries[:0]
	o.tickets = o.tickets[:0]
	o.payloads = o.payloads[:0]
	o.data = o.data[:0]
}

func (o *Output) emitDurable(ev Event, r *round) {
	es, ps := len(o.entries), len(o.payloads)
	o.entries = append(o.entries, r.entries...)
	o.payloads = append(o.payloads, r.payloads...)
	ev.Entries = o.entries[es:len(o.entries):len(o.entries)]
	ev.Payloads = o.payloads[ps:len(o.payloads):len(o.payloads)]
	ts := len(o.tickets)
	for i, t := range r.tickets {
		o.tickets = append(o.tickets, visible(t, &r.entries[i]))
	}
	ev.Tickets = o.tickets[ts:len(o.tickets):len(o.tickets)]
	o.Events = append(o.Events, ev)
}

func (o *Output) emitRecord(ev Event, d *DecodedRecord) {
	es, ps, ds := len(o.entries), len(o.payloads), len(o.data)
	o.entries = append(o.entries, d.Entries...)
	var writes uint64
	for i := range d.Entries {
		if d.Entries[i].Kind == EntryWrite {
			writes += d.Entries[i].dataBytes()
		}
	}
	var writeOff, blobOff uint64
	for i := range d.Entries {
		e := &d.Entries[i]
		if e.Kind == EntryWrite {
			o.payloads = append(o.payloads, writeOff)
			writeOff += e.dataBytes()
			continue
		}
		o.payloads = append(o.payloads, writes+blobOff)
		if e.Kind == EntryBlob {
			blobOff += e.Length
		}
	}
	o.data = append(o.data, d.Data[writes:writes+blobOff]...)
	ev.Entries = o.entries[es:len(o.entries):len(o.entries)]
	ev.Payloads = o.payloads[ps:len(o.payloads):len(o.payloads)]
	ev.Data = o.data[ds:len(o.data):len(o.data)]
	o.Events = append(o.Events, ev)
}

func (o *Output) emitBlob(ref BlobRef, b []byte) {
	ds := len(o.data)
	o.data = append(o.data, b...)
	o.Events = append(o.Events, Event{
		Kind:    BlobRead,
		LSN:     ref.LSN,
		Segment: ref.Segment,
		Block:   ref.Block,
		Ref:     ref,
		Data:    o.data[ds:len(o.data):len(o.data)],
	})
}

func (o *Output) emitTickets(kind EventKind, err error, from int) {
	if from == len(o.tickets) {
		return
	}
	o.Events = append(o.Events, Event{Kind: kind, Err: err, Tickets: o.tickets[from:len(o.tickets):len(o.tickets)]})
}

func visible(t Ticket, e *Entry) Ticket {
	if isManifestKind(e) {
		return NoTicket
	}
	return t
}

func isManifestKind(e *Entry) bool {
	return e.Kind == EntryBlob && (e.BlobKind == BlobManifest || e.BlobKind == BlobManifestPiece)
}

func isCheckpointKind(e *Entry) bool {
	return isManifestKind(e) || e.Kind == EntryBlob && e.BlobKind == BlobRun
}

// CheckpointIndex names an index a checkpoint covers, its replay LSN, the durable runs the manifest names again and the bounds its running merges set on them.
type CheckpointIndex struct {
	Owner     uint32
	ReplayLSN uint64
	Runs      []BlobRef
	Bounds    []MergeBound
}

type queued struct {
	ticket Ticket
	entry  Entry
	buf    core.IOBuf
	bytes  []byte
	keep   bool
}

func (q *queued) payload(pool core.BufPool) []byte {
	switch {
	case q.bytes != nil:
		return q.bytes
	case q.buf == core.NoBuf:
		return nil
	case q.entry.Kind == EntryBlob:
		return pool.Bytes(q.buf)[:q.entry.Length]
	}
	return pool.Bytes(q.buf)
}

type queue struct {
	items []queued
	head  int
}

func (q *queue) len() int              { return len(q.items) - q.head }
func (q *queue) at(i int) *queued      { return &q.items[q.head+i] }
func (q *queue) push(it queued)        { q.items = append(q.items, it) }
func (q *queue) window(n int) []queued { return q.items[q.head : q.head+n] }

func (q *queue) drop(n int) {
	clear(q.items[q.head : q.head+n])
	q.head += n
	switch {
	case q.head == len(q.items):
		q.items, q.head = q.items[:0], 0
	case q.head > 64 && q.head > len(q.items)/2:
		n := copy(q.items, q.items[q.head:])
		clear(q.items[n:])
		q.items, q.head = q.items[:n], 0
	}
}

type cursor struct {
	segment uint32
	seq     uint64
	next    uint32
}

// Lanes in the order rounds take turns: each stream's shared lane, then merge worker w's lane
// of the metadata stream at laneMeta + w. Only the hot and cold lanes take ordinary items; the
// metadata lane takes the checkpoint's pieces and manifest, and a merge lane its worker's output.
const (
	laneHot = iota
	laneCold
	laneMeta
)

func laneOf(s Stream, n uint8) int {
	switch s {
	case Hot:
		return laneHot
	case Cold:
		return laneCold
	}
	return laneMeta + int(n)
}

// lane is a sequence of segments that fill one after another, at most one open at a time, so
// LSNs rise along it.
type lane struct {
	stream Stream
	n      uint8
	q      queue
	asm    cursor
	opened uint64
}

type round struct {
	lsn          uint64
	writeIO      core.IOID
	flushIO      core.IOID
	writeDone    bool
	flushSent    bool
	abandoned    bool
	segment      uint32
	block        uint32
	headerBlocks uint32
	bytes        uint64
	ordinary     uint64
	head         core.IOBuf
	tail         core.IOBuf
	vec          []core.IOVec
	writeBufs    []core.IOBuf
	keepBufs     []core.IOBuf
	entries      []Entry
	tickets      []Ticket
	payloads     []uint64
	opened       uint64
	named        ManifestPos
}

type pendingPiece struct {
	ticket Ticket
	index  int
	root   bool
}

func (cp *checkpoint) owns(q queued) bool {
	return slices.ContainsFunc(cp.pending, func(p pendingPiece) bool { return p.ticket == q.ticket })
}

type checkpoint struct {
	manifest      Manifest
	editRows      []SegmentUsage
	changed       []uint64
	replayAtBegin uint64
	heldAtBegin   uint64
	pending       []pendingPiece
	ended         bool
	body          []byte
	snapshot      bool
	state         []ManifestIndex
	prior         [][]BlobRef
	written       int
	earlier       []BlobRef
	size          uint64
	piece         Ticket
	final         bool
}

// chainEntry is a snapshot or an edit a last piece lists: its pieces, its own last piece
// included, and the bytes they hold, which Pebble's rule weighs.
type chainEntry struct {
	pieces []BlobRef
	size   uint64
}

type liveChain struct {
	pos     ManifestPos
	entries []chainEntry
}

type formatPhase uint8

const (
	formatBlock0 formatPhase = iota + 1
	formatCopy
	formatDone
)

// Log appends a disk's records by group commit, one vectored write and one flush per round.
type Log struct {
	cfg        Config
	sb         Superblock
	reserve    uint32
	maxWrite   uint32
	maxItemRec uint32
	maxBlob    uint64
	maxWrites  uint16

	phase      formatPhase
	formatIO   core.IOID
	formatSync core.IOID

	nextIO     core.IOID
	nextTicket Ticket
	nextLSN    uint64
	nextSeq    uint64
	durableSeq uint64
	durableLSN uint64
	free       []uint64
	usage      []SegmentUsage
	changed    []uint64

	lanes     []lane
	cpNoSpace bool
	last      int

	rounds []round
	first  int
	count  int
	stale  []core.IOID

	err         error
	manifest    ManifestPos
	replayBytes uint64
	heldBytes   uint64
	cp          *checkpoint
	chain       []chainEntry
	state       []ManifestIndex
	live        []liveChain
	wantSnap    bool

	crcs     []uint32
	checkImg []byte
	checkEnt []Entry

	breakRound     func(head []byte, r *round)
	appendAnywhere bool
	seqAtBegin     bool
}

// Format starts a log on an empty disk, writing and flushing the superblock then its copy, with IO IDs counting up from firstIO, a range no other machine on the disk uses.
func Format(ctx *core.Context, cfg Config, sb Superblock, firstIO core.IOID, out *Output) (*Log, error) {
	if err := sb.check(); err != nil {
		return nil, err
	}
	l, err := newLog(ctx, cfg, sb, firstIO)
	if err != nil {
		return nil, err
	}
	if _, err := AppendSuperblock(ctx.Pool.Bytes(l.rounds[0].head)[:0], sb); err != nil {
		l.release(ctx)
		return nil, err
	}
	for s := uint32(1); s < sb.SegmentCount; s++ {
		l.free[s/64] |= 1 << (s % 64)
	}
	l.phase = formatBlock0
	l.writeFormat(sb.SuperblockLBA(), out)
	return l, nil
}

// newLog sizes the log and takes each round slot's header and blob buffers from the pool
// for the log's lifetime, so a pool filled by producers never stops a round.
func newLog(ctx *core.Context, cfg Config, sb Superblock, firstIO core.IOID) (*Log, error) {
	maxWrite := itemRecordBlocks(cfg, sb) + 1
	reserve, err := cfg.reserve(&sb)
	workers, _ := MergeWorkers(cfg.MergeWorkers)
	if err != nil || cfg.CommitDepth < 1 || maxWrite < 3 || cfg.MaxReplayBytes == 0 || cfg.MaxReplayBytes > math.MaxUint64/4 {
		return nil, ErrBadConfig
	}
	l := &Log{
		cfg:        cfg,
		sb:         sb,
		reserve:    reserve,
		maxWrite:   maxWrite,
		maxItemRec: maxWrite - 1,
		maxBlob:    maxBlobBytes(maxWrite - 1),
		maxWrites:  maxWriteBlocks(maxWrite - 1),
		nextIO:     firstIO,
		nextTicket: 1,
		nextLSN:    1,
		nextSeq:    1,
		durableSeq: 1,
		free:       make([]uint64, (sb.SegmentCount+63)/64),
		usage:      make([]SegmentUsage, sb.SegmentCount),
		changed:    make([]uint64, (sb.SegmentCount+63)/64),
		lanes:      make([]lane, laneMeta+1+workers),
		rounds:     make([]round, cfg.CommitDepth),
	}
	l.lanes[laneHot].stream, l.lanes[laneCold].stream = Hot, Cold
	for i := laneMeta; i < len(l.lanes); i++ {
		l.lanes[i].stream, l.lanes[i].n = Meta, uint8(i-laneMeta)
	}
	l.last = len(l.lanes) - 1
	for i := range l.rounds {
		r := &l.rounds[i]
		var err error
		if r.head, err = ctx.Pool.Alloc(int(maxWrite) * core.BlockSize); err == nil {
			r.tail, err = ctx.Pool.Alloc(int(maxWrite-1) * core.BlockSize)
		}
		if err != nil {
			l.release(ctx)
			return nil, err
		}
	}
	return l, nil
}

func (l *Log) release(ctx *core.Context) {
	for i := range l.rounds {
		r := &l.rounds[i]
		for _, b := range []*core.IOBuf{&r.head, &r.tail} {
			if *b != core.NoBuf {
				ctx.Pool.Free(*b)
				*b = core.NoBuf
			}
		}
	}
}

// Close fails the items still waiting, for room or for a manifest, and the running checkpoint with ErrClosed, and returns their buffers and the log's own to the pool; it refuses until format finishes and while a write or flush is in flight, and the log takes nothing more.
func (l *Log) Close(ctx *core.Context, out *Output) error {
	if l.count > 0 || l.phase != formatDone || len(l.stale) > 0 {
		return ErrBusy
	}
	if l.err != ErrClosed {
		ts := len(out.tickets)
		out.tickets = l.drainLanes(ctx, out.tickets)
		out.emitTickets(Failed, ErrClosed, ts)
		if l.cp != nil {
			l.dropCheckpoint(ErrClosed, out)
		}
		l.release(ctx)
		l.err = ErrClosed
	}
	return nil
}

func maxBlobBytes(maxRecBlocks uint32) uint64 {
	return uint64(maxDataBlocks(maxRecBlocks, blobEntrySize)) * core.BlockSize
}

func maxWriteBlocks(maxRecBlocks uint32) uint16 {
	return uint16(min(maxDataBlocks(maxRecBlocks, writeEntrySize), core.BlocksPerChunk))
}

// maxDataBlocks is the most data blocks a record of one entry of entrySize bytes holds in maxRecBlocks.
func maxDataBlocks(maxRecBlocks uint32, entrySize int) uint32 {
	for d := maxRecBlocks - 1; d > 0; d-- {
		headerBytes := uint64(preludeSize + entrySize + core.CRC32CSize*int(d))
		if (headerBytes+core.BlockSize-1)/core.BlockSize+uint64(d) <= uint64(maxRecBlocks) {
			return d
		}
	}
	return 0
}

// itemRecordBlocks is the longest record one item may take under cfg on a disk of sb's geometry.
func itemRecordBlocks(cfg Config, sb Superblock) uint32 {
	return uint32(min(cfg.MaxRecordBytes/core.BlockSize, uint64(sb.SegmentBlocks))) - 1
}

func (l *Log) io() core.IOID {
	io := l.nextIO
	l.nextIO++
	return io
}

func (l *Log) writeFormat(lba core.LBA, out *Output) {
	l.formatIO, l.formatSync = l.io(), l.io()
	r := &l.rounds[0]
	r.vec = append(r.vec[:0], core.IOVec{Buf: r.head, Len: core.BlockSize})
	out.Actions = append(out.Actions,
		core.Action{Kind: core.ActDiskWrite, IO: l.formatIO, LBA: lba, Vec: r.vec, Len: core.BlockSize},
		core.Action{Kind: core.ActDiskFsync, IO: l.formatSync, Linked: true})
}

// Superblock is the log's identity and geometry.
func (l *Log) Superblock() Superblock { return l.sb }

// Reserve is the free segments only checkpoints may open, sized for a checkpoint, the merge steps and a cleaner pass.
func (l *Log) Reserve() uint32 { return l.reserve }

// DurableLSN is the LSN of the newest durable record.
func (l *Log) DurableLSN() uint64 { return l.durableLSN }

// Manifest is the position of the newest durable manifest's last piece.
func (l *Log) Manifest() ManifestPos { return l.manifest }

// Err is the error that failed or closed the log, or nil.
func (l *Log) Err() error { return l.err }

// MaxBlobBytes is the largest blob one record holds.
func (l *Log) MaxBlobBytes() uint64 { return l.maxBlob }

// MaxWriteBlocks is the most blocks one write item holds.
func (l *Log) MaxWriteBlocks() uint16 { return l.maxWrites }

// ReplayBytes is the size of the records written after the newest manifest's base, which a restart replays.
func (l *Log) ReplayBytes() uint64 { return l.replayBytes }

// InFlight is the number of rounds written but not yet durable.
func (l *Log) InFlight() int { return l.count }

// Queued is the number of items submitted and not yet in a round.
func (l *Log) Queued() int {
	n := 0
	for i := range l.lanes {
		n += l.lanes[i].q.len()
	}
	return n
}

// Stalled reports items waiting for room: ordinary items with only the reserve free or held behind a checkpoint that failed for want of space, or the checkpoint's or a merge's items with no segment free.
func (l *Log) Stalled() bool {
	for i := range l.lanes {
		if l.lanes[i].q.len() > 0 {
			if _, p := l.place(i); p == placeNoRoom {
				return true
			}
		}
	}
	return false
}

// FailStalled fails the items waiting for room with NoSpace, each lane's in submit order, and a checkpoint among them, as the owner does when the cleaner has no victim.
func (l *Log) FailStalled(ctx *core.Context, out *Output) {
	for l.Stalled() {
		for i := range l.lanes {
			if l.lanes[i].q.len() == 0 {
				continue
			}
			if _, p := l.place(i); p != placeNoRoom {
				continue
			}
			if i == laneMeta {
				l.failCheckpoint(ctx, out)
				continue
			}
			if l.cp != nil && slices.ContainsFunc(l.lanes[i].q.window(l.lanes[i].q.len()), l.cp.owns) {
				l.failCheckpoint(ctx, out)
			}
			// The whole queue fails, even items behind its head that would fit, since writing
			// them first would break submit order, which a freeze before its write relies on.
			ts := len(out.tickets)
			out.tickets = l.drain(ctx, &l.lanes[i].q, out.tickets)
			out.emitTickets(Failed, ErrNoSpace, ts)
		}
		l.startRounds(ctx, out)
	}
}

// ordinaryBytes is the share of a record laid out as lay that the hold behind a checkpoint
// counts: none for a record of checkpoint items alone, else its header blocks and every data
// block an ordinary entry's payload touches, so a block shared with a piece counts whole.
func ordinaryBytes(entries []Entry, lay Layout) uint64 {
	var blocks, blobOff, counted uint64
	ordinary := false
	for i := range entries {
		e := &entries[i]
		own := isCheckpointKind(e)
		ordinary = ordinary || !own
		switch e.Kind {
		case EntryWrite:
			blocks += uint64(e.Count)
		case EntryBlob:
			lo, hi := blobOff/core.BlockSize, (blobOff+e.Length+core.BlockSize-1)/core.BlockSize
			blobOff += e.Length
			if !own && e.Length > 0 {
				blocks += hi - max(lo, counted)
				counted = hi
			}
		}
	}
	if !ordinary {
		return 0
	}
	return (uint64(lay.HeaderBlocks) + blocks) * core.BlockSize
}

// behind reports twice MaxReplayBytes of ordinary records, in flight or durable, after the
// newest manifest: ordinary items wait for a checkpoint, which bounds the memtables and Raft
// entries that replay must rebuild. The checkpoint's own records are left out, or one larger
// than the bound would hold ordinary items past every manifest.
func (l *Log) behind() bool {
	n := l.heldBytes
	for i := range l.count {
		n += l.round(i).ordinary
	}
	return n >= 2*l.cfg.MaxReplayBytes
}

// CheckpointDue reports whether MaxReplayBytes follow the newest manifest and no checkpoint is running.
func (l *Log) CheckpointDue() bool {
	return l.cp == nil && l.err == nil && l.replayBytes >= l.cfg.MaxReplayBytes
}

// Submit queues an item for the hot or the cold stream, taking its buffer; a refused item's buffer stays with the caller.
func (l *Log) Submit(ctx *core.Context, stream Stream, it Item, out *Output) (Ticket, error) {
	if l.err != nil {
		return 0, l.err
	}
	if stream > Cold {
		return 0, ErrMalformed
	}
	if isCheckpointKind(&it.Entry) {
		return 0, ErrReservedKind
	}
	if err := l.accept(ctx, &it); err != nil {
		return 0, err
	}
	t := l.enqueue(&l.lanes[laneOf(stream, 0)].q, queued{entry: it.Entry, buf: it.Buf, keep: it.Keep && it.Entry.Kind == EntryWrite})
	l.startRounds(ctx, out)
	return t, nil
}

// MergePiece queues merge worker w's next output piece for index owner, of length bytes, on the worker's lane of the metadata stream, which may open segments of the reserve; w counts from 1 to the configured merge workers, and the reserve holds one step of each, so a worker hands over a step only once the manifest naming its previous one is durable. While a checkpoint covering owner is open the piece is one of its pieces, root marking the run's last piece, which the manifest names; otherwise it goes out alone, and root is ErrNoCheckpoint.
func (l *Log) MergePiece(ctx *core.Context, w int, owner uint32, buf core.IOBuf, length uint64, root bool, out *Output) (Ticket, error) {
	if l.err != nil {
		return 0, l.err
	}
	if w < 1 || laneMeta+w >= len(l.lanes) {
		return 0, ErrMalformed
	}
	if l.cp != nil && !l.cp.ended && l.cpIndex(owner) >= 0 {
		return l.checkpointPiece(ctx, laneMeta+w, owner, buf, length, root, out)
	}
	if root {
		return 0, ErrNoCheckpoint
	}
	it := Item{Entry: BlobEntry(BlobRun, owner, length), Buf: buf}
	if err := l.accept(ctx, &it); err != nil {
		return 0, err
	}
	t := l.enqueue(&l.lanes[laneMeta+w].q, queued{entry: it.Entry, buf: it.Buf})
	l.startRounds(ctx, out)
	return t, nil
}

func (l *Log) accept(ctx *core.Context, it *Item) error {
	var acc layoutAcc
	if err := acc.add(&it.Entry); err != nil {
		return err
	}
	lay, err := acc.layout()
	if err != nil || lay.Blocks() > uint64(l.maxItemRec) {
		return ErrTooLarge
	}
	switch it.Entry.Kind {
	case EntryWrite:
		if it.Buf == core.NoBuf || len(ctx.Pool.Bytes(it.Buf)) != int(it.Entry.Count)*core.BlockSize {
			return ErrBadBuffer
		}
	case EntryBlob:
		if it.Entry.Length == 0 {
			if it.Buf != core.NoBuf {
				return ErrBadBuffer
			}
		} else if it.Buf == core.NoBuf || uint64(len(ctx.Pool.Bytes(it.Buf))) < it.Entry.Length {
			return ErrBadBuffer
		}
	default:
		if it.Buf != core.NoBuf {
			return ErrBadBuffer
		}
	}
	return nil
}

func (l *Log) enqueue(to *queue, q queued) Ticket {
	q.ticket = l.nextTicket
	l.nextTicket++
	to.push(q)
	return q.ticket
}

// BeginCheckpoint starts a checkpoint of indexes whose runs hold every record up to DurableLSN after each one's replay LSN; pieces and EndCheckpoint follow.
func (l *Log) BeginCheckpoint(indexes []CheckpointIndex) error {
	if l.err != nil {
		return l.err
	}
	if l.cp != nil {
		return ErrBusy
	}
	nextSeq := l.durableSeq
	if l.seqAtBegin {
		nextSeq = l.nextSeq
	}
	m := Manifest{BaseLSN: l.durableLSN, NextSeq: nextSeq, Indexes: make([]ManifestIndex, len(indexes))}
	for i, ix := range indexes {
		if ix.ReplayLSN > l.durableLSN || slices.ContainsFunc(indexes[:i], func(o CheckpointIndex) bool { return o.Owner == ix.Owner }) ||
			slices.ContainsFunc(ix.Runs, func(r BlobRef) bool { return r.Segment == 0 || r.LSN > l.durableLSN }) ||
			hasDuplicate(ix.Runs) || !validBounds(ix.Runs, ix.Bounds) {
			return ErrMalformed
		}
		var bounds []MergeBound
		for _, b := range ix.Bounds {
			bounds = append(bounds, MergeBound{Run: b.Run, Key: slices.Clone(b.Key), Dead: b.Dead})
		}
		m.Indexes[i] = ManifestIndex{Owner: ix.Owner, ReplayLSN: ix.ReplayLSN, Runs: slices.Clone(ix.Runs), Bounds: bounds}
	}
	m.Segments = l.usageRows()
	cp := &checkpoint{manifest: m, changed: slices.Clone(l.changed), replayAtBegin: l.replayBytes, heldAtBegin: l.heldBytes}
	for w, set := range l.changed {
		for ; set != 0; set &= set - 1 {
			s := w*64 + bits.TrailingZeros64(set)
			u := l.usage[s]
			u.Segment = uint32(s)
			cp.editRows = append(cp.editRows, u)
		}
	}
	clear(l.changed)
	l.cp, l.cpNoSpace = cp, false
	return nil
}

func hasDuplicate(refs []BlobRef) bool {
	for i, r := range refs {
		if slices.Contains(refs[i+1:], r) {
			return true
		}
	}
	return false
}

// RequestSnapshot makes the next checkpoint to choose its manifest write a snapshot, whatever Pebble's rule says.
func (l *Log) RequestSnapshot() { l.wantSnap = true }

// HoldsManifestPiece reports whether segment holds a piece of a manifest recovery may still load, or one the running checkpoint wrote.
func (l *Log) HoldsManifestPiece(segment uint32) bool {
	in := func(refs []BlobRef) bool {
		return slices.ContainsFunc(refs, func(r BlobRef) bool { return r.Segment == segment })
	}
	for _, c := range l.live {
		if slices.ContainsFunc(c.entries, func(e chainEntry) bool { return in(e.pieces) }) {
			return true
		}
	}
	return l.cp != nil && in(l.cp.earlier)
}

// CheckpointPiece queues an index's next run piece of length bytes on the metadata stream; root marks a run's last piece, the one the manifest names.
func (l *Log) CheckpointPiece(ctx *core.Context, owner uint32, buf core.IOBuf, length uint64, root bool, out *Output) (Ticket, error) {
	if l.err != nil {
		return 0, l.err
	}
	return l.checkpointPiece(ctx, laneMeta, owner, buf, length, root, out)
}

func (l *Log) cpIndex(owner uint32) int {
	return slices.IndexFunc(l.cp.manifest.Indexes, func(ix ManifestIndex) bool { return ix.Owner == owner })
}

func (l *Log) checkpointPiece(ctx *core.Context, lane int, owner uint32, buf core.IOBuf, length uint64, root bool, out *Output) (Ticket, error) {
	if l.cp == nil || l.cp.ended {
		return 0, ErrNoCheckpoint
	}
	index := l.cpIndex(owner)
	if index < 0 {
		return 0, ErrUnknownOwner
	}
	it := Item{Entry: BlobEntry(BlobRun, owner, length), Buf: buf}
	if err := l.accept(ctx, &it); err != nil {
		return 0, err
	}
	t := l.enqueue(&l.lanes[lane].q, queued{entry: it.Entry, buf: it.Buf})
	l.cp.pending = append(l.cp.pending, pendingPiece{ticket: t, index: index, root: root})
	l.startRounds(ctx, out)
	return t, nil
}

// EndCheckpoint marks every piece handed over; the manifest follows once they are all durable.
func (l *Log) EndCheckpoint(ctx *core.Context, out *Output) error {
	if l.err != nil {
		return l.err
	}
	if l.cp == nil || l.cp.ended {
		return ErrNoCheckpoint
	}
	l.cp.ended = true
	l.advanceManifest(out)
	l.startRounds(ctx, out)
	return nil
}

func (l *Log) advanceManifest(out *Output) {
	cp := l.cp
	if cp == nil || !cp.ended || len(cp.pending) > 0 || cp.piece != 0 {
		return
	}
	if cp.body == nil {
		l.chooseManifest(cp)
	}
	rest := cp.body[cp.written:]
	lists := append(slices.Clip(cp.prior), cp.earlier)
	if uint64(ManifestPieceSize(lists, len(rest))) <= l.maxBlob {
		piece, err := AppendManifestPiece(nil, lists, rest)
		if err == nil {
			cp.final = true
			cp.size += uint64(len(piece))
			cp.piece = l.enqueue(&l.lanes[laneMeta].q, queued{entry: BlobEntry(BlobManifest, 0, uint64(len(piece))), bytes: piece})
			return
		}
	} else if len(rest) > 0 && len(cp.earlier) < math.MaxUint16 {
		n := min(uint64(len(rest)), l.maxBlob)
		cp.size += n
		cp.piece = l.enqueue(&l.lanes[laneMeta].q, queued{entry: BlobEntry(BlobManifestPiece, 0, n), bytes: rest[:n]})
		cp.written += int(n)
		return
	}
	l.dropCheckpoint(ErrTooLarge, out)
}

// chooseManifest writes a snapshot of the state an edit would leave, runs in the order applying
// it gives, so loading either yields the same manifest.
func (l *Log) chooseManifest(cp *checkpoint) {
	snapshot := len(l.chain) == 0 || l.wantSnap
	state := cp.manifest.Indexes
	var edit ManifestEdit
	if !snapshot {
		edit = ManifestEdit{BaseLSN: cp.manifest.BaseLSN, NextSeq: cp.manifest.NextSeq, Indexes: diffIndexes(l.state, state), Segments: cp.editRows}
		var err error
		if state, err = applyIndexes(l.state, edit.Indexes); err != nil {
			panic(fmt.Sprintf("disklog: an edit diffed from its base does not apply: %v", err))
		}
	}
	snap := Manifest{BaseLSN: cp.manifest.BaseLSN, NextSeq: cp.manifest.NextSeq, Indexes: state, Segments: cp.manifest.Segments}
	cp.state = state
	if !snapshot {
		prior := make([][]BlobRef, len(l.chain))
		var since uint64
		for i, e := range l.chain {
			prior[i] = e.pieces
			if i > 0 {
				since += e.size
			}
		}
		next, ok := manifestBytes(ManifestSize(&snap), nil, l.maxBlob)
		if !ok {
			next = math.MaxUint64
		}
		if size, ok := manifestBytes(ManifestEditSize(&edit), prior, l.maxBlob); ok && !snapshotDue(l.chain[0].size, since, size, next) {
			cp.body, cp.prior = AppendManifestEdit(nil, &edit), prior
			return
		}
	}
	cp.body, cp.snapshot = AppendManifest(nil, &snap), true
}

// dropCheckpoint ends the running checkpoint unfinished; the segments it would have recorded
// changing stay marked for the next one.
func (l *Log) dropCheckpoint(err error, out *Output) {
	for w, set := range l.cp.changed {
		l.changed[w] |= set
	}
	l.cp = nil
	out.Events = append(out.Events, Event{Kind: CheckpointFailed, Err: err})
}

// OnEvent advances rounds on disk completions.
func (l *Log) OnEvent(ctx *core.Context, ev *core.Event, out *Output) {
	if ev.Kind == core.EvDiskDone {
		l.diskDone(ctx, ev.IO, ev.Err, out)
	}
}

func (l *Log) diskDone(ctx *core.Context, io core.IOID, derr core.DiskError, out *Output) {
	if l.phase != formatDone && (io == l.formatIO || io == l.formatSync) {
		l.formatDone(ctx, io, derr, out)
		return
	}
	for i := range l.count {
		r := l.round(i)
		switch io {
		case r.writeIO:
			if r.writeDone {
				return
			}
			r.writeDone = true
			l.freeWriteBufs(ctx, r)
			if r.abandoned {
				l.freeKeepBufs(ctx, r)
				l.dropAbandoned()
				return
			}
			if derr != core.DiskOK {
				l.fail(ctx, derr, out)
				return
			}
			l.sendFlushes(out)
			return
		case r.flushIO:
			if r.abandoned || !r.flushSent {
				return
			}
			if derr != core.DiskOK {
				l.fail(ctx, derr, out)
				return
			}
			if !r.writeDone {
				l.fail(ctx, ErrFlushBeforeWrite, out)
				return
			}
			for range i {
				if l.round(0).flushSent {
					l.stale = append(l.stale, l.round(0).flushIO)
				}
				l.retire(out)
			}
			l.retire(out)
			l.startRounds(ctx, out)
			return
		}
	}
	if j := slices.Index(l.stale, io); j >= 0 {
		l.stale = slices.Delete(l.stale, j, j+1)
		if derr != core.DiskOK && l.err == nil {
			l.fail(ctx, derr, out)
		}
	}
}

func (l *Log) formatDone(ctx *core.Context, io core.IOID, derr core.DiskError, out *Output) {
	if derr != core.DiskOK {
		l.phase = formatDone
		l.fail(ctx, derr, out)
		return
	}
	if io != l.formatSync {
		return
	}
	if l.phase == formatBlock0 {
		l.phase = formatCopy
		l.writeFormat(l.sb.SuperblockCopyLBA(), out)
		return
	}
	l.phase = formatDone
	l.startRounds(ctx, out)
}

func (l *Log) round(i int) *round { return &l.rounds[(l.first+i)%len(l.rounds)] }

// freeKeepBufs frees the kept buffers of a round in doubt once its write can no longer read them.
func (l *Log) freeKeepBufs(ctx *core.Context, r *round) {
	for _, b := range r.keepBufs {
		ctx.Pool.Free(b)
	}
	r.keepBufs = r.keepBufs[:0]
}

func (l *Log) freeWriteBufs(ctx *core.Context, r *round) {
	for _, b := range r.writeBufs {
		ctx.Pool.Free(b)
	}
	clear(r.writeBufs)
	r.writeBufs = r.writeBufs[:0]
}

// sendFlushes submits the flush of every round whose write and every earlier round's
// write have completed, so durability stays a prefix of the log; such a flush is not
// linked, since its write is already done.
func (l *Log) sendFlushes(out *Output) {
	for i := range l.count {
		r := l.round(i)
		if !r.writeDone {
			return
		}
		if !r.flushSent {
			r.flushSent = true
			out.Actions = append(out.Actions, core.Action{Kind: core.ActDiskFsync, IO: r.flushIO})
		}
	}
}

func (l *Log) earlierWritten() bool {
	for i := range l.count {
		if !l.round(i).writeDone {
			return false
		}
	}
	return true
}

// usageRows is the segment usage table as of the durable LSN: every segment that is not free.
func (l *Log) usageRows() []SegmentUsage {
	var rows []SegmentUsage
	for s := range l.usage {
		if u := l.usage[s]; u.State != SegmentFree {
			u.Segment = uint32(s)
			rows = append(rows, u)
		}
	}
	return rows
}

func (l *Log) retire(out *Output) {
	r := l.round(0)
	l.durableLSN = r.lsn
	r.keepBufs = r.keepBufs[:0]
	if r.opened != 0 {
		l.durableSeq = r.opened + 1
		l.headerDurable(r.named)
	}
	l.replayBytes += r.bytes
	l.heldBytes += r.ordinary
	l.changed[r.segment/64] |= 1 << (r.segment % 64)
	u := &l.usage[r.segment]
	u.State, u.Newest = SegmentUsed, r.lsn
	u.Live += uint32(r.bytes/core.BlockSize) - r.headerBlocks
	out.emitDurable(Event{
		Kind:         Durable,
		LSN:          r.lsn,
		Segment:      r.segment,
		Block:        r.block,
		LBA:          l.sb.LBA(r.segment, r.block),
		HeaderBlocks: r.headerBlocks,
	}, r)
	l.checkpointProgress(r, out)
	l.first = (l.first + 1) % len(l.rounds)
	l.count--
}

// headerDurable drops the manifests older than the one a now durable segment header names:
// recovery loads that one or a newer one.
func (l *Log) headerDurable(named ManifestPos) {
	i := slices.IndexFunc(l.live, func(c liveChain) bool { return c.pos.LSN >= named.LSN })
	if i > 0 {
		l.live = slices.Delete(l.live, 0, i)
	}
}

func (l *Log) checkpointProgress(r *round, out *Output) {
	cp := l.cp
	if cp == nil {
		return
	}
	for i, t := range r.tickets {
		ref := BlobRef{Segment: r.segment, Block: r.block, LSN: r.lsn, Entry: uint16(i)}
		k := slices.IndexFunc(cp.pending, func(p pendingPiece) bool { return p.ticket == t })
		switch {
		case k >= 0:
			p := cp.pending[k]
			cp.pending = slices.Delete(cp.pending, k, k+1)
			if p.root {
				ix := &cp.manifest.Indexes[p.index]
				ix.Runs = append(ix.Runs, ref)
			}
		case t == cp.piece && cp.final:
			l.manifest = ManifestPos{Segment: r.segment, Block: r.block, LSN: r.lsn}
			l.replayBytes -= cp.replayAtBegin
			l.heldBytes -= cp.heldAtBegin
			entry := chainEntry{pieces: append(cp.earlier, ref), size: cp.size}
			if cp.snapshot {
				l.chain, l.wantSnap = []chainEntry{entry}, false
			} else {
				l.chain = append(slices.Clip(l.chain), entry)
			}
			l.state = cp.state
			l.live = append(l.live, liveChain{pos: l.manifest, entries: l.chain})
			l.cp = nil
			out.Events = append(out.Events, Event{Kind: CheckpointDone, Manifest: l.manifest})
			return
		case t == cp.piece:
			cp.earlier = append(cp.earlier, ref)
			cp.piece = 0
		}
	}
	l.advanceManifest(out)
}

func (l *Log) fail(ctx *core.Context, err error, out *Output) {
	l.err = err
	ts := len(out.tickets)
	for i := range l.count {
		r := l.round(i)
		for j, t := range r.tickets {
			out.tickets = append(out.tickets, visible(t, &r.entries[j]))
		}
		r.abandoned = true
		if r.writeDone {
			l.freeKeepBufs(ctx, r)
		}
	}
	out.emitTickets(InDoubt, err, ts)
	ts = len(out.tickets)
	out.tickets = l.drainLanes(ctx, out.tickets)
	out.emitTickets(Failed, err, ts)
	if l.cp != nil {
		l.dropCheckpoint(err, out)
	}
	l.dropAbandoned()
}

func (l *Log) dropAbandoned() {
	for l.count > 0 && l.round(0).writeDone {
		l.first = (l.first + 1) % len(l.rounds)
		l.count--
	}
}

func (l *Log) drainLanes(ctx *core.Context, tickets []Ticket) []Ticket {
	for i := range l.lanes {
		tickets = l.drain(ctx, &l.lanes[i].q, tickets)
	}
	return tickets
}

func (l *Log) drain(ctx *core.Context, q *queue, tickets []Ticket) []Ticket {
	for i := range q.len() {
		it := q.at(i)
		tickets = append(tickets, visible(it.ticket, &it.entry))
		if it.buf != core.NoBuf && !it.keep {
			ctx.Pool.Free(it.buf)
		}
	}
	q.drop(q.len())
	return tickets
}

// failCheckpoint fails the checkpoint's queued items when not even the reserve has room for them.
func (l *Log) failCheckpoint(ctx *core.Context, out *Output) {
	if l.cp != nil {
		l.dropCheckpoint(ErrNoSpace, out)
	}
	ts := len(out.tickets)
	out.tickets = l.drain(ctx, &l.lanes[laneMeta].q, out.tickets)
	out.emitTickets(Failed, ErrNoSpace, ts)
	l.cpNoSpace = true
}

type target struct {
	lane    int
	segment uint32
	seq     uint64
	offset  uint32
	roll    bool
}

type placement uint8

const (
	placeReady placement = iota
	placeWait
	placeNoRoom
)

// target takes the lanes with work in turn, starting after the lane of the last round.
func (l *Log) target() (target, bool) {
	for k := 1; k <= len(l.lanes); k++ {
		i := (l.last + k) % len(l.lanes)
		if l.lanes[i].q.len() == 0 {
			continue
		}
		if t, p := l.place(i); p == placeReady {
			return t, true
		}
	}
	return target{}, false
}

// place places a round of lane i, or says why it waits. A round appends to the lane's segment
// only once the round that opened it is durable, so a segment whose header never landed holds
// at most the one record written with it, the record recovery checks at block 1. Ordinary
// items wait while twice MaxReplayBytes follow the newest manifest, and never open a segment of
// the reserve; the checkpoint's and the merges' items may, and an ordinary lane never shares
// their segments.
func (l *Log) place(i int) (target, placement) {
	ln := &l.lanes[i]
	ordinary := i < laneMeta
	if ordinary && l.behind() {
		if l.cpNoSpace {
			return target{}, placeNoRoom
		}
		return target{}, placeWait
	}
	var acc layoutAcc
	if err := acc.add(&ln.q.at(0).entry); err != nil {
		panic(fmt.Sprintf("disklog: queued entry no longer validates: %v", err))
	}
	lay, _ := acc.layout()
	c := ln.asm
	if c.segment != 0 && uint64(l.sb.SegmentBlocks-c.next) >= lay.Blocks() {
		if ln.opened > l.durableLSN && !l.appendAnywhere {
			return target{}, placeWait
		}
		return target{lane: i, segment: c.segment, seq: c.seq, offset: c.next}, placeReady
	}
	seg, ok := l.firstFree()
	if !ok || ordinary && l.freeCount() <= l.reserve {
		return target{}, placeNoRoom
	}
	return target{lane: i, segment: seg, seq: l.nextSeq, offset: 1, roll: true}, placeReady
}

func (l *Log) freeCount() uint32 {
	n := 0
	for _, w := range l.free {
		n += bits.OnesCount64(w)
	}
	return uint32(n)
}

func (l *Log) firstFree() (uint32, bool) {
	for w, bitsSet := range l.free {
		if bitsSet != 0 {
			return uint32(w*64 + bits.TrailingZeros64(bitsSet)), true
		}
	}
	return 0, false
}

func (l *Log) startRounds(ctx *core.Context, out *Output) {
	for l.err == nil && l.phase == formatDone && l.count < len(l.rounds) {
		t, ok := l.target()
		if !ok {
			return
		}
		l.startRound(ctx, t, out)
	}
}

func (l *Log) startRound(ctx *core.Context, t target, out *Output) {
	ln := &l.lanes[t.lane]
	lead := uint32(0)
	if t.roll {
		lead = 1
	}
	limit := uint64(min(l.sb.SegmentBlocks-t.offset, l.maxWrite-lead))
	var acc layoutAcc
	var lay Layout
	n, writeBlocks := 0, uint32(0)
	for n < ln.q.len() && n < maxRecordEntries {
		next := acc
		e := &ln.q.at(n).entry
		if err := next.add(e); err != nil {
			break
		}
		nl, err := next.layout()
		if err != nil || nl.Blocks() > limit {
			break
		}
		acc, lay = next, nl
		if e.Kind == EntryWrite {
			writeBlocks += uint32(e.Count)
		}
		n++
	}
	if n == 0 {
		panic("disklog: a round built a record with no entries")
	}
	items := ln.q.window(n)
	slot := (l.first + l.count) % len(l.rounds)
	r := &l.rounds[slot]
	tailBlocks := lay.DataBlocks - writeBlocks
	if tailBlocks > 0 {
		l.fillTail(ctx, items, ctx.Pool.Bytes(r.tail)[:tailBlocks*core.BlockSize])
	}

	headLen := (lead + lay.HeaderBlocks) * core.BlockSize
	r.entries = r.entries[:0]
	r.tickets = r.tickets[:0]
	r.payloads = r.payloads[:0]
	r.writeBufs = r.writeBufs[:0]
	r.keepBufs = r.keepBufs[:0]
	r.vec = append(r.vec[:0], core.IOVec{Buf: r.head, Len: headLen})
	l.crcs = l.crcs[:0]
	var writeOff, blobOff uint64
	for i := range items {
		it := &items[i]
		r.entries = append(r.entries, it.entry)
		r.tickets = append(r.tickets, it.ticket)
		switch it.entry.Kind {
		case EntryWrite:
			r.payloads = append(r.payloads, writeOff)
			writeOff += uint64(it.entry.Count) * core.BlockSize
			if it.keep {
				r.keepBufs = append(r.keepBufs, it.buf)
			} else {
				r.writeBufs = append(r.writeBufs, it.buf)
			}
			r.vec = append(r.vec, core.IOVec{Buf: it.buf, Len: uint32(it.entry.Count) * core.BlockSize})
			l.crcs = appendBlockCRCs(l.crcs, ctx.Pool.Bytes(it.buf))
		default:
			r.payloads = append(r.payloads, blobOff)
			blobOff += it.entry.Length
		}
	}
	for i := range items {
		if items[i].entry.Kind != EntryWrite {
			r.payloads[i] += writeOff
		}
	}
	if tailBlocks > 0 {
		r.vec = append(r.vec, core.IOVec{Buf: r.tail, Len: tailBlocks * core.BlockSize})
		l.crcs = appendBlockCRCs(l.crcs, ctx.Pool.Bytes(r.tail)[:tailBlocks*core.BlockSize])
	}

	rec := Record{Place: Place{LogID: l.sb.LogID, Seq: t.seq, Offset: t.offset}, LSN: l.nextLSN, Durable: l.durableLSN}
	img := ctx.Pool.Bytes(r.head)[:0]
	var err error
	if t.roll {
		img, err = AppendSegmentHeader(img, l.segmentHeader(t))
		if err != nil {
			panic(fmt.Sprintf("disklog: segment header does not encode: %v", err))
		}
	}
	if _, err = AppendHeader(img, rec, r.entries, l.crcs); err != nil {
		panic(fmt.Sprintf("disklog: record header does not encode: %v", err))
	}
	if l.breakRound != nil {
		l.breakRound(ctx.Pool.Bytes(r.head)[:headLen], r)
	}
	if l.cfg.Check {
		l.check(ctx, t, rec, r, items)
	}
	for i := range items {
		if it := &items[i]; it.entry.Kind == EntryBlob && it.buf != core.NoBuf {
			ctx.Pool.Free(it.buf)
		}
	}
	ln.q.drop(n)

	if t.roll {
		l.free[t.segment/64] &^= 1 << (t.segment % 64)
		l.nextSeq++
	}
	bytes := uint64(lead+uint32(lay.Blocks())) * core.BlockSize
	*r = round{
		lsn:          l.nextLSN,
		writeIO:      l.io(),
		flushIO:      l.io(),
		flushSent:    l.earlierWritten(),
		segment:      t.segment,
		block:        t.offset,
		headerBlocks: lay.HeaderBlocks,
		bytes:        lay.Blocks() * core.BlockSize,
		head:         r.head,
		tail:         r.tail,
		vec:          r.vec,
		writeBufs:    r.writeBufs,
		keepBufs:     r.keepBufs,
		entries:      r.entries,
		tickets:      r.tickets,
		payloads:     r.payloads,
	}
	r.ordinary = ordinaryBytes(r.entries, lay)
	if t.roll {
		r.opened, r.named = t.seq, l.manifest
	}
	ln.asm = cursor{segment: t.segment, seq: t.seq, next: t.offset + uint32(lay.Blocks())}
	if t.roll {
		ln.opened = l.nextLSN
	}
	l.nextLSN++
	l.last = t.lane
	l.count++
	out.Actions = append(out.Actions, core.Action{
		Kind: core.ActDiskWrite,
		IO:   r.writeIO,
		LBA:  l.sb.LBA(t.segment, t.offset-lead),
		Vec:  r.vec,
		Len:  bytes,
	})
	if r.flushSent {
		out.Actions = append(out.Actions, core.Action{Kind: core.ActDiskFsync, IO: r.flushIO, Linked: true})
	}
}

func (l *Log) segmentHeader(t target) SegmentHeader {
	ln := &l.lanes[t.lane]
	return SegmentHeader{LogID: l.sb.LogID, Seq: t.seq, Index: t.segment, Stream: ln.stream, Lane: ln.n, Manifest: l.manifest}
}

func (l *Log) fillTail(ctx *core.Context, items []queued, tail []byte) {
	n := 0
	for i := range items {
		if it := &items[i]; it.entry.Kind == EntryBlob {
			n += copy(tail[n:], it.payload(ctx.Pool))
		}
	}
	clear(tail[n:])
}

func appendBlockCRCs(crcs []uint32, b []byte) []uint32 {
	for i := 0; i < len(b); i += core.BlockSize {
		crcs = append(crcs, core.CRC32C(b[i:i+core.BlockSize]))
	}
	return crcs
}

func (l *Log) check(ctx *core.Context, t target, rec Record, r *round, items []queued) {
	img := l.checkImg[:0]
	for _, v := range r.vec {
		img = append(img, ctx.Pool.Bytes(v.Buf)[:v.Len]...)
	}
	l.checkImg = img
	if t.roll {
		h, err := DecodeSegmentHeader(img)
		if err != nil || h != l.segmentHeader(t) {
			panic(fmt.Sprintf("disklog: decode check: segment header decodes to %+v, %v; want %+v", h, err, l.segmentHeader(t)))
		}
		img = img[core.BlockSize:]
	}
	d, err := DecodeRecord(img, rec.Place, l.sb.SegmentBlocks-t.offset, l.checkEnt[:0])
	l.checkEnt = d.Entries[:0]
	if err != nil {
		panic(fmt.Sprintf("disklog: decode check: LSN %d does not decode: %v", rec.LSN, err))
	}
	if d.Record != rec || len(d.Entries) != len(items) || uint64(len(img)) != d.Layout.Blocks()*core.BlockSize {
		panic(fmt.Sprintf("disklog: decode check: LSN %d decodes as %+v with %d entries in %d blocks; want %+v with %d in %d bytes", rec.LSN, d.Record, len(d.Entries), d.Layout.Blocks(), rec, len(items), len(img)))
	}
	for i := range items {
		it := &items[i]
		if d.Entries[i] != it.entry {
			panic(fmt.Sprintf("disklog: decode check: LSN %d entry %d decodes as %+v; want %+v", rec.LSN, i, d.Entries[i], it.entry))
		}
		if off := PayloadOffset(d.Entries, i); r.payloads[i] != off {
			panic(fmt.Sprintf("disklog: decode check: LSN %d entry %d reported at offset %d; it decodes at %d", rec.LSN, i, r.payloads[i], off))
		}
		if !bytes.Equal(d.Payload(i), it.payload(ctx.Pool)) {
			panic(fmt.Sprintf("disklog: decode check: LSN %d entry %d payload differs", rec.LSN, i))
		}
	}
}
