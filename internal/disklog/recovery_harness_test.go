package disklog_test

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"maps"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

const indexOwner = 1

// blockData is the content a write of chunk version gives block: any mapped extent can be
// checked by reading it back.
func blockData(chunk core.ChunkID, version uint64, block uint16) []byte {
	seed := chunk.Hash() ^ version*0x9e3779b97f4a7c15 ^ uint64(block)<<48
	return pattern(seed, core.BlockSize)
}

func chunkOf(idx uint32) core.ChunkID {
	c := testChunk
	c.Index = idx
	return c
}

// chunkWrite builds a write of count blocks from block first of a chunk version, filled
// with blockData.
func (h *harness) chunkWrite(idx uint32, first, count uint16, version uint64) disklog.Item {
	buf := h.alloc(int(count) * core.BlockSize)
	b := h.pool.Bytes(buf)
	chunk := chunkOf(idx)
	for i := range count {
		copy(b[int(i)*core.BlockSize:], blockData(chunk, version, first+i))
	}
	return disklog.Item{Entry: disklog.WriteEntry(1, chunk, version, first, count), Buf: buf}
}

type blockKey struct {
	chunk core.ChunkID
	block uint16
}

type mapping struct {
	lba     core.LBA
	version uint64
}

// model is a stand-in for the chunk index: where each live block of each chunk sits.
type model map[blockKey]mapping

// apply maps a Durable or Replayed record's writes and drops its punched blocks.
func (m model) apply(ev disklog.Event) {
	for i, e := range ev.Entries {
		switch e.Kind {
		case disklog.EntryWrite:
			base := ev.LBA + core.LBA(ev.HeaderBlocks) + core.LBA(ev.Payloads[i]/core.BlockSize)
			for b := range e.Count {
				m[blockKey{e.Chunk, e.First + b}] = mapping{base + core.LBA(b), e.Version}
			}
		case disklog.EntryPunch:
			for b := range e.Count {
				delete(m, blockKey{e.Chunk, e.First + b})
			}
		}
	}
}

func (m model) keys() []blockKey {
	keys := slices.Collect(maps.Keys(m))
	slices.SortFunc(keys, func(a, b blockKey) int {
		return cmp.Or(bytes.Compare(a.chunk.AppendTo(nil), b.chunk.AppendTo(nil)), cmp.Compare(a.block, b.block))
	})
	return keys
}

func (m model) encode() []byte {
	var b []byte
	for _, k := range m.keys() {
		v := m[k]
		b = k.chunk.AppendTo(b)
		b = binary.LittleEndian.AppendUint16(b, k.block)
		b = binary.LittleEndian.AppendUint64(b, uint64(v.lba))
		b = binary.LittleEndian.AppendUint64(b, v.version)
	}
	return b
}

func decodeModel(b []byte) model {
	m := model{}
	for ; len(b) >= core.ChunkIDSize+18; b = b[core.ChunkIDSize+18:] {
		k := blockKey{core.ChunkIDFromBytes([core.ChunkIDSize]byte(b)), binary.LittleEndian.Uint16(b[core.ChunkIDSize:])}
		m[k] = mapping{core.LBA(binary.LittleEndian.Uint64(b[core.ChunkIDSize+2:])), binary.LittleEndian.Uint64(b[core.ChunkIDSize+10:])}
	}
	return m
}

func (m model) clone() model { return maps.Clone(m) }

// readBack checks that every mapped block reads back the data its version wrote.
func (h *harness) readBack(m model) {
	h.t.Helper()
	for _, k := range m.keys() {
		v := m[k]
		if !bytes.Equal(h.read(v.lba, 1), blockData(k.chunk, v.version, k.block)) {
			h.t.Fatalf("chunk %v block %d (version %d) at LBA %d does not read back", k.chunk, k.block, v.version, v.lba)
		}
	}
}

func (h *harness) wantModel(name string, got, want model) {
	h.t.Helper()
	if !maps.Equal(got, want) {
		h.t.Fatalf("%s: index of %d blocks, want %d", name, len(got), len(want))
	}
}

// node stands in for the chunk store that owns a disk log: it applies every durable record
// to its index and checkpoints the index whenever one is due, as a chain of run pieces
// each naming the one before, handing a piece only once the previous one is durable. After
// a failed checkpoint it begins the next once a record has become durable since.
type node struct {
	h          *harness
	live       model
	pieceLimit int
	failed     bool
	failedAt   uint64

	running bool
	body    []byte
	off     int
	prev    *disklog.BlobRef
	ticket  disklog.Ticket

	checkpoints, failedCheckpoints int
	// begun is the snapshot the running checkpoint would write, its run filled in once durable;
	// expect holds it for every finished checkpoint by where its manifest's last piece sits.
	begun  disklog.Manifest
	expect map[disklog.ManifestPos]disklog.Manifest
}

func newNode(h *harness, pieceLimit int) *node {
	n := &node{h: h, live: model{}, pieceLimit: pieceLimit, expect: map[disklog.ManifestPos]disklog.Manifest{}}
	h.onEvent = n.onEvent
	return n
}

func (n *node) onEvent(ev disklog.Event) {
	switch ev.Kind {
	case disklog.Durable:
		n.live.apply(ev)
		for i, t := range ev.Tickets {
			if n.running && n.ticket != disklog.NoTicket && t == n.ticket {
				n.prev = &disklog.BlobRef{Segment: ev.Segment, Block: ev.Block, LSN: ev.LSN, Entry: uint16(i)}
				n.ticket = 0
			}
		}
	case disklog.CheckpointDone:
		if n.running {
			n.begun.Indexes[0].Runs = []disklog.BlobRef{*n.prev}
			n.expect[ev.Manifest] = n.begun
		}
		n.running = false
		n.checkpoints++
	case disklog.CheckpointFailed:
		n.running, n.failed, n.failedAt = false, true, n.h.log.DurableLSN()
		n.failedCheckpoints++
	}
}

// step begins a checkpoint when one is due and hands its next piece once the previous one
// is durable; it reports whether it did anything.
func (n *node) step() bool {
	h := n.h
	if h.log.Err() != nil || n.failed && h.log.DurableLSN() == n.failedAt {
		return false
	}
	if !n.running {
		if !h.log.CheckpointDue() {
			return false
		}
		base := h.log.DurableLSN()
		n.begun = disklog.Manifest{BaseLSN: base, NextSeq: disklog.NextSeqOf(h.log), Indexes: []disklog.ManifestIndex{{Owner: indexOwner, ReplayLSN: base}}, Segments: disklog.SegmentUsageOf(h.log)}
		if err := h.log.BeginCheckpoint([]disklog.CheckpointIndex{{Owner: indexOwner, ReplayLSN: base}}); err != nil {
			h.t.Fatalf("begin checkpoint: %v", err)
		}
		n.running, n.body, n.off, n.prev, n.ticket = true, n.live.encode(), 0, nil, 0
	}
	if n.ticket != 0 || n.off > len(n.body) {
		return false
	}
	limit := int(h.log.MaxBlobBytes()) - 1 - disklog.BlobRefSize
	if n.pieceLimit > 0 {
		limit = min(limit, n.pieceLimit)
	}
	chunk := n.body[n.off:min(len(n.body), n.off+limit)]
	piece := []byte{0}
	if n.prev != nil {
		piece[0] = 1
		piece = appendRef(piece, *n.prev)
	}
	piece = append(piece, chunk...)
	buf, err := h.pool.Alloc(len(piece))
	if err != nil {
		return false
	}
	copy(h.pool.Bytes(buf), piece)
	root := n.off+len(chunk) == len(n.body)
	t, err := h.log.CheckpointPiece(&h.ctx, indexOwner, buf, uint64(len(piece)), root, &h.out)
	if err != nil {
		h.pool.Free(buf)
		h.t.Fatalf("checkpoint piece: %v", err)
	}
	n.ticket = t
	n.off += len(chunk)
	h.absorb()
	if root && n.running {
		n.off++
		if err := h.log.EndCheckpoint(&h.ctx, &h.out); err != nil {
			h.t.Fatalf("end checkpoint: %v", err)
		}
		h.absorb()
	}
	return true
}

func appendRef(b []byte, r disklog.BlobRef) []byte {
	b = binary.LittleEndian.AppendUint32(b, r.Segment)
	b = binary.LittleEndian.AppendUint32(b, r.Block)
	b = binary.LittleEndian.AppendUint64(b, r.LSN)
	return binary.LittleEndian.AppendUint16(b, r.Entry)
}

func decodeRef(b []byte) disklog.BlobRef {
	return disklog.BlobRef{
		Segment: binary.LittleEndian.Uint32(b),
		Block:   binary.LittleEndian.Uint32(b[4:]),
		LSN:     binary.LittleEndian.Uint64(b[8:]),
		Entry:   binary.LittleEndian.Uint16(b[16:]),
	}
}

// recovered is what one recovery delivered.
type recovered struct {
	err       error
	index     model
	replayLSN uint64
	pieces    int
	replayed  []disklog.Event
	manifest  disklog.ManifestPos
	durable   uint64

	replayedBytes uint64
	trace         []string
	quarantined   []uint32
	erased        int
	loaded        disklog.Manifest
	replayReads   uint64
}

func (r *recovered) lsns() []uint64 {
	var lsns []uint64
	for _, ev := range r.replayed {
		lsns = append(lsns, ev.LSN)
	}
	return lsns
}

// resetIO forgets everything in flight and starts a fresh pool, as a restarted process does;
// on a scheduler it restarts the node, crashing it first, if it is up, with every write landing.
func (h *harness) resetIO() {
	h.pending = nil
	h.trace = nil
	h.events = nil
	h.completed = map[core.IOID]bool{}
	h.durable = map[core.IOID]bool{}
	h.writeAt = map[core.LBA]core.IOID{}
	h.done = nil
	h.writes, h.fsyncs, h.unlinked, h.maxPending = 0, 0, 0, 0
	h.log, h.rec = nil, nil
	if h.sched == nil {
		h.pool = sim.NewPool(cmp.Or(h.poolBytes, 64<<20), 0)
		h.ctx.Pool = h.pool
		return
	}
	if h.node.Up() {
		h.sched.Crash(h.node.ID(), func(int, int, core.LBA) bool { return true })
	}
	h.roundOf = map[core.IOID]uint64{}
	h.sched.Restart(h.node.ID())
}

// crashRandom crashes the disk with each unflushed block write landing with probability 1/2
// from r; on a scheduler it crashes the node, and writes issued but not completed are decided too.
func (h *harness) crashRandom(r *core.Rand) {
	if h.sched == nil {
		h.disk.CrashRandom(r)
		return
	}
	h.sched.Crash(h.node.ID(), func(int, int, core.LBA) bool { return r.Chance(1, 2) })
}

// recover runs a recovery to its end, loading the index's run from the manifest in open
// mode and applying the records after its replay point, and resumes the log when it
// succeeds. Whatever is in flight is dropped first.
func (h *harness) recover(mode disklog.Mode) *recovered {
	h.t.Helper()
	return h.recoverCrashing(mode, nil)
}

// recoverCrashing is recover where crash, consulted before each of recovery's actions, may
// crash the disk instead; recovery then starts over.
func (h *harness) recoverCrashing(mode disklog.Mode, crash func(core.Action) bool) *recovered {
	h.t.Helper()
	for {
		if res, crashed := h.recoverOnce(mode, crash); !crashed {
			return res
		}
	}
}

func (h *harness) recoverOnce(mode disklog.Mode, crash func(core.Action) bool) (*recovered, bool) {
	h.t.Helper()
	h.resetIO()
	res := &recovered{index: model{}}
	var chain [][]byte
	replay := func() {
		if err := h.rec.Replay(&h.ctx, &h.out); err != nil {
			h.t.Fatalf("replay: %v", err)
		}
		h.absorb()
	}
	saved := h.onEvent
	h.onEvent = func(ev disklog.Event) {
		switch ev.Kind {
		case disklog.ManifestLoaded:
			res.manifest = ev.Manifest
			res.replayReads = h.readBytes
			m, ok := h.rec.Manifest()
			res.loaded = m
			if ok {
				for _, ix := range m.Indexes {
					if ix.Owner != indexOwner {
						continue
					}
					res.replayLSN = ix.ReplayLSN
					if len(ix.Runs) > 0 {
						if err := h.rec.ReadBlob(&h.ctx, ix.Runs[len(ix.Runs)-1], &h.out); err != nil {
							h.t.Fatalf("read run: %v", err)
						}
						h.absorb()
						return
					}
				}
			}
			replay()
		case disklog.BlobRead:
			res.pieces++
			chain = append(chain, ev.Data)
			if ev.Data[0] == 1 {
				if err := h.rec.ReadBlob(&h.ctx, decodeRef(ev.Data[1:]), &h.out); err != nil {
					h.t.Fatalf("read piece: %v", err)
				}
				h.absorb()
				return
			}
			var body []byte
			for i := len(chain) - 1; i >= 0; i-- {
				p := chain[i]
				if p[0] == 1 {
					p = p[1+disklog.BlobRefSize:]
				} else {
					p = p[1:]
				}
				body = append(body, p...)
			}
			res.index = decodeModel(body)
			replay()
		case disklog.Replayed:
			res.replayed = append(res.replayed, ev)
			if ev.LSN > res.replayLSN {
				res.index.apply(ev)
			}
		case disklog.Recovered:
			res.durable = ev.LSN
		case disklog.RecoveryFailed:
			res.err = ev.Err
		}
	}
	rec, err := disklog.Recover(&h.ctx, h.cfg, h.disk.Blocks(), mode, firstIO, &h.out)
	if err != nil {
		h.t.Fatalf("recover: %v", err)
	}
	h.rec = rec
	if h.plant != nil {
		h.plant(rec)
	}
	h.readBytes = 0
	h.absorb()
	for len(h.pending) > 0 {
		i := 0
		if h.ioRand != nil {
			var ready []int
			for j := range h.pending {
				if h.eligible(j) {
					ready = append(ready, j)
				}
			}
			i = ready[h.ioRand.Below(uint64(len(ready)))]
		}
		if crash != nil && crash(h.pending[i].act) {
			h.onEvent = saved
			return nil, true
		}
		h.step(i)
	}
	h.onEvent = saved
	res.replayReads = h.readBytes - res.replayReads
	res.trace = h.trace
	if res.err != nil {
		if h.pool.Live() != 0 {
			h.t.Fatalf("%d buffers live after a failed recovery", h.pool.Live())
		}
		return res, false
	}
	res.replayedBytes = h.rec.ReplayedBytes()
	res.quarantined = h.rec.Quarantined()
	res.erased = h.rec.Erased()
	if res.durable != h.rec.DurableLSN() {
		h.t.Fatalf("recovered LSN %d, recovery says %d", res.durable, h.rec.DurableLSN())
	}
	h.log, err = h.rec.Log(&h.ctx)
	if err != nil {
		h.t.Fatalf("resume: %v", err)
	}
	h.rec = nil
	h.own = h.pool.Live()
	h.trace = nil
	return res, false
}

// reopen recovers in open mode and fails the test unless recovery succeeds.
func (h *harness) reopen() *recovered {
	h.t.Helper()
	res := h.recover(disklog.ModeOpen)
	if res.err != nil {
		h.t.Fatalf("recovery: %v", res.err)
	}
	return res
}

// crashLanding crashes the disk with only the unflushed blocks at the given LBAs landing.
func (h *harness) crashLanding(lbas ...core.LBA) {
	h.disk.Crash(func(_ int, lba core.LBA) bool { return slices.Contains(lbas, lba) })
}

func lsnRange(from, to uint64) []uint64 {
	var l []uint64
	for i := from; i <= to; i++ {
		l = append(l, i)
	}
	return l
}
