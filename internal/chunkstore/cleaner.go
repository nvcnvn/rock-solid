package chunkstore

import (
	"cmp"
	"errors"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

const (
	// DefaultCleanFree is clean_free, the free segments outside the reserve the cleaner keeps.
	DefaultCleanFree = 16
	cleanIOBit       = 1 << 62
	cleanReadDepth   = 16
)

// cleanPiece is a run of live blocks of one chunk version that the cleaner copies: from block
// first, read from src on, their checksums at crc in the job's list.
type cleanPiece struct {
	pool    core.PoolID
	chunk   core.ChunkID
	version uint64
	first   uint16
	count   uint16
	src     core.LBA
	crc     int
	buf     core.IOBuf
}

// cleanJob is the copy of one victim: the pieces the owner listed from its records, each read
// into a buffer of its own. Run is the part a worker goroutine does, since it touches nothing but
// the job.
type cleanJob struct {
	segment uint32
	seq     uint64
	records []RecordLive
	next    int
	reads   int
	pieces  []cleanPiece
	crcs    []uint32
	issued  int
	bad     bool
	failed  bool
	copies  int
	phase   jobPhase
}

type jobPhase uint8

// A job reads its victim's segment header, then the headers of its live records, listing their
// live pieces, then the pieces' data; Run checks and sorts them, and the copies go out.
const (
	jobHeader jobPhase = iota
	jobRecords
	jobData
	jobCopies
)

// Run checks every block it read against the checksum its record header stored, and sorts the
// pieces into the index's key order, so a chunk's blocks go out side by side and its moves apply
// together; a failing block marks the job bad, and nothing of it is copied.
func (j *cleanJob) Run(pool core.BufPool) {
	for _, p := range j.pieces {
		b := pool.Bytes(p.buf)
		for k := range int(p.count) {
			if core.CRC32C(b[k*core.BlockSize:][:core.BlockSize]) != j.crcs[p.crc+k] {
				j.bad = true
				return
			}
		}
	}
	slices.SortFunc(j.pieces, func(a, b cleanPiece) int {
		ab, bb := a.chunk.Bytes(), b.chunk.Bytes()
		return cmp.Or(cmp.Compare(a.pool, b.pool), cmp.Compare(a.chunk.Hash(), b.chunk.Hash()),
			slices.Compare(ab[:], bb[:]), cmp.Compare(a.first, b.first))
	})
}

// cleanPass cleans its victims one after another.
type cleanPass struct {
	victims []uint32
	cur     int
	freed   int
	job     cleanJob
}

type cleanRead struct {
	pass   *cleanPass
	record int
	piece  int
	buf    core.IOBuf
	blocks uint32
	used   bool
}

type cleanSent struct {
	ticket disklog.Ticket
	pass   *cleanPass
	piece  int
}

// CleanStats counts the cleaner's work: passes begun, victims freed or quarantined, runs marked
// for a rewrite, the blocks it copied, and the most passes run at once.
type CleanStats struct {
	Passes      int
	Freed       int
	Quarantined int
	Rewrites    int
	Copied      uint64
	PeakPasses  int
}

// CleanStats is what the store's cleaner has done since the store started.
func (s *Store) CleanStats() CleanStats { return s.cl.stats }

// cleaner is the store's cleaner state machine: its passes, their reads and the copies sent, in
// ticket order from sentHead on, a resolved one's pass nil.
type cleaner struct {
	stats     CleanStats
	passes    []*cleanPass
	reads     []cleanRead
	freeReads []int
	sent      []cleanSent
	sentHead  int
	triedAt   uint64
	tried     bool
	ents      []disklog.Entry
	candidate []victimScore
	pieces    []disklog.BlobRef
	items     []disklog.Item
	tickets   []disklog.Ticket
}

type victimScore struct {
	segment uint32
	score   float64
}

func (s *Store) cleanFree() int {
	switch {
	case s.cfg.CleanFree < 0:
		return 0
	case s.cfg.CleanFree == 0:
		return DefaultCleanFree
	}
	return s.cfg.CleanFree
}

// cleanWorkers is how many passes run at once: one, and a second once the free segments
// outside the reserve fall two below clean_free, 14 of the default 16.
func (s *Store) cleanWorkers() int {
	if s.log.FreeOutsideReserve() <= s.cleanFree()-2 {
		return 2
	}
	return 1
}

// clean starts passes while free segments outside the reserve are short of clean_free, or while
// items wait for room, and drives the running ones. After a pass that frees nothing, as after
// finding no victim, it starts none until the durable LSN moves. It reports whether the cleaner
// has a victim.
func (s *Store) clean(ctx *core.Context) bool {
	for len(s.cl.passes) < s.cleanWorkers() && (s.log.FreeOutsideReserve() < s.cleanFree() || s.log.Stalled()) {
		if s.cl.tried && s.cl.triedAt == s.log.DurableLSN() {
			break
		}
		victims, snapshot := s.pickVictims()
		if snapshot {
			s.log.RequestSnapshot()
		}
		if len(victims) == 0 {
			s.cl.tried, s.cl.triedAt = true, s.log.DurableLSN()
			break
		}
		s.cl.tried = false
		s.cl.stats.Passes++
		s.cl.passes = append(s.cl.passes, &cleanPass{victims: victims})
		s.cl.stats.PeakPasses = max(s.cl.stats.PeakPasses, len(s.cl.passes))
	}
	for i := 0; i < len(s.cl.passes); {
		p := s.cl.passes[i]
		s.advancePass(ctx, p)
		if p.cur == len(p.victims) {
			if p.freed == 0 {
				s.cl.tried, s.cl.triedAt = true, s.log.DurableLSN()
			}
			s.cl.passes = slices.Delete(s.cl.passes, i, i+1)
			continue
		}
		i++
	}
	return len(s.cl.passes) > 0
}

func (s *Store) isVictim(seg uint32) bool {
	for _, p := range s.cl.passes {
		if slices.Contains(p.victims[p.cur:], seg) {
			return true
		}
	}
	return false
}

// pickVictims picks up to VictimsPerPass segments by cost-benefit, (1 − u) × age ÷ (1 + u)
// with u the live share and age capped at a disk's worth of writes, among those whose copy
// costs less than a segment less the header of the segment it lands in, header blocks and the
// index entries its moves write included, charging live run pieces at the records of the whole
// runs a rewrite writes again and what each of its steps writes beside them; a chunk segment's
// copies must also fit the room the cleaner's lane can reach, and it must hold no header of a
// live slot write, which no copy moves. A segment holding a piece of a manifest recovery may
// load waits for the next snapshot, which snapshot reports is wanted, and one holding only
// pieces about to die waits for them.
func (s *Store) pickVictims() (victims []uint32, snapshot bool) {
	geo := s.sb.Geometry
	room := s.log.CleanerRoom()
	s.cl.candidate = s.cl.candidate[:0]
	for seg := uint32(1); seg < geo.SegmentCount; seg++ {
		if !s.log.Cleanable(seg) || s.isVictim(seg) {
			continue
		}
		if s.log.HoldsManifestPiece(seg) {
			snapshot = true
			continue
		}
		u, stream := s.log.Usage(seg)
		var cost uint64
		if stream == disklog.Meta {
			var held bool
			s.cl.pieces, held = s.ix.Tree().RunsIn(seg, s.cl.pieces[:0])
			if held || len(s.cl.pieces) == 0 && s.log.HoldsPiece(seg) {
				continue
			}
			steps, extra := s.ix.Tree().RewriteSteps(seg)
			cost = s.log.RewriteCost(s.cl.pieces, steps, extra)
		} else if cost = s.log.CopyCost(uint64(u.Live)); cost > room || s.ix.SlotHeld(seg) > 0 {
			continue
		} else {
			cost += moveCost(uint64(u.Live))
		}
		if cost+1 >= uint64(geo.SegmentBlocks) {
			continue
		}
		share := min(float64(u.Live)/float64(geo.SegmentBlocks-1), 1)
		s.cl.candidate = append(s.cl.candidate, victimScore{seg, costBenefit(share, s.log.Age(seg))})
	}
	slices.SortStableFunc(s.cl.candidate, func(a, b victimScore) int { return cmp.Compare(b.score, a.score) })
	for _, c := range s.cl.candidate[:min(len(s.cl.candidate), disklog.VictimsPerPass)] {
		victims = append(victims, c.segment)
	}
	return victims, snapshot
}

// movedEntryBytes is what a moved block costs the index at most: its extent's key and value with
// a run block's three length varints, written once by the flush and again by a merge.
const movedEntryBytes = 2 * (KeySize + extentSize + 3)

// moveCost is the blocks the index entries of live moved blocks take at most.
func moveCost(live uint64) uint64 {
	return (live*movedEntryBytes + core.BlockSize - 1) / core.BlockSize
}

// costBenefit is a victim's score, free space times age over copy cost, as the original
// log-structured file system scores it: (1 − u) × age ÷ (1 + u) for live share u.
func costBenefit(u float64, age uint64) float64 { return (1 - u) * float64(age) / (1 + u) }

// startVictim begins the pass's current victim: a metadata segment marks the runs holding live
// pieces in it for a rewrite and asks for a snapshot when it holds a manifest's piece, and is
// freed once nothing it holds is live; a chunk segment's live records are listed for copying.
func (s *Store) startVictim(ctx *core.Context, p *cleanPass) bool {
	seg := p.victims[p.cur]
	u, stream := s.log.Usage(seg)
	if stream == disklog.Meta {
		s.cl.stats.Rewrites += s.ix.Tree().MarkRewrite(seg)
		if s.log.Free(ctx, seg, &s.lout) == nil {
			s.cl.stats.Freed++
			p.freed++
		}
		p.cur++
		return false
	}
	p.job = cleanJob{segment: seg, records: p.job.records[:0], pieces: p.job.pieces[:0], crcs: p.job.crcs[:0]}
	if u.Live == 0 {
		p.job.phase = jobCopies
		return true
	}
	lo := s.sb.LBA(seg, 0)
	p.job.records = s.ix.Records(p.job.records, lo, lo+core.LBA(s.sb.SegmentBlocks))
	return true
}

func (s *Store) advancePass(ctx *core.Context, p *cleanPass) {
	for p.cur < len(p.victims) {
		j := &p.job
		if j.segment != p.victims[p.cur] && !s.startVictim(ctx, p) {
			continue
		}
		switch j.phase {
		case jobHeader:
			if j.seq == 0 && j.reads == 0 && !j.failed && !s.cleanRead(ctx, p, -1, -1, s.sb.LBA(j.segment, 0), 1) {
				return
			}
			if j.reads > 0 {
				return
			}
			j.phase = jobRecords
		case jobRecords:
			for j.next < len(j.records) && j.reads < cleanReadDepth && !j.failed && !j.bad {
				if !s.cleanRead(ctx, p, j.next, -1, j.records[j.next].Header, 1) {
					return
				}
				j.next++
			}
			if j.reads > 0 || j.next < len(j.records) && !j.failed && !j.bad {
				return
			}
			j.phase = jobData
		case jobData:
			for j.issued < len(j.pieces) && j.reads < cleanReadDepth && !j.failed && !j.bad {
				if !s.cleanRead(ctx, p, -1, j.issued, j.pieces[j.issued].src, uint32(j.pieces[j.issued].count)) {
					return
				}
				j.issued++
			}
			if j.reads > 0 || j.issued < len(j.pieces) && !j.failed && !j.bad {
				return
			}
			if !j.failed && !j.bad {
				j.Run(ctx.Pool)
			}
			if j.bad {
				if err := s.log.Quarantine(j.segment); err != nil {
					panic("chunkstore: a victim the cleaner read is not used: " + err.Error())
				}
				s.cl.stats.Quarantined++
			}
			if !j.failed && !j.bad {
				s.sendCopies(ctx, p)
			}
			j.phase = jobCopies
		case jobCopies:
			if j.copies > 0 || s.readsFrom(j.segment) {
				return
			}
			s.finishVictim(ctx, p)
		}
	}
}

// cleanRead issues a read of blocks blocks at lba, for record r's header or piece pc's data, or
// the victim's segment header with neither; it reports false when the pool has no buffer for it
// yet, and the pass tries again on a later call.
func (s *Store) cleanRead(ctx *core.Context, p *cleanPass, r, pc int, lba core.LBA, blocks uint32) bool {
	var buf core.IOBuf
	if pc >= 0 && p.job.pieces[pc].buf != core.NoBuf {
		buf = p.job.pieces[pc].buf
	} else {
		var err error
		if buf, err = ctx.Pool.Alloc(int(blocks) * core.BlockSize); err != nil {
			return false
		}
		if pc >= 0 {
			p.job.pieces[pc].buf = buf
		}
	}
	var i int
	if k := len(s.cl.freeReads); k > 0 {
		i = s.cl.freeReads[k-1]
		s.cl.freeReads = s.cl.freeReads[:k-1]
	} else {
		s.cl.reads = append(s.cl.reads, cleanRead{})
		i = len(s.cl.reads) - 1
	}
	s.cl.reads[i] = cleanRead{pass: p, record: r, piece: pc, buf: buf, blocks: blocks, used: true}
	p.job.reads++
	s.lout.Actions = append(s.lout.Actions, core.Action{Kind: core.ActDiskRead, IO: core.IOID(cleanIOBit | uint64(i)), LBA: lba, Buf: buf, Len: uint64(blocks) * core.BlockSize})
	return true
}

func (s *Store) cleanReadsOut() int { return len(s.cl.reads) - len(s.cl.freeReads) }

// cleanReadDone takes a completed read: the victim's segment header gives its records' open
// sequence, a record header lists the record's live pieces, and a piece's data waits for Run.
func (s *Store) cleanReadDone(ctx *core.Context, i int, derr core.DiskError) {
	rd := s.cl.reads[i]
	s.cl.reads[i] = cleanRead{}
	s.cl.freeReads = append(s.cl.freeReads, i)
	j := &rd.pass.job
	j.reads--
	if derr != core.DiskOK || s.phase != serving {
		j.failed = true
	}
	if rd.piece >= 0 {
		return
	}
	defer ctx.Pool.Free(rd.buf)
	if j.failed {
		return
	}
	img := ctx.Pool.Bytes(rd.buf)[:rd.blocks*core.BlockSize]
	if rd.record < 0 {
		h, err := disklog.DecodeSegmentHeader(img)
		if err != nil || h.LogID != s.sb.LogID || h.Index != j.segment {
			j.failed = true
			return
		}
		j.seq = h.Seq
		return
	}
	header := j.records[rd.record].Header
	offset := uint32(uint64(header) % uint64(s.sb.SegmentBlocks))
	if _, hb, err := disklog.BlockCRC(img, s.sb.LogID, offset, 0); errors.Is(err, disklog.ErrTruncated) && hb > rd.blocks {
		if !s.cleanRead(ctx, rd.pass, rd.record, -1, header, hb) {
			j.failed = true
		}
		return
	}
	h, err := disklog.DecodeHeader(img, disklog.Place{LogID: s.sb.LogID, Seq: j.seq, Offset: offset}, s.sb.SegmentBlocks-offset, s.cl.ents[:0])
	if err != nil {
		j.bad = true
		return
	}
	s.cl.ents = h.Entries[:0]
	s.listPieces(j, header, &h)
}

// listPieces adds the runs of blocks of each write or copy in the record that a map still stores
// there, as pieces of at most one copy item each.
func (s *Store) listPieces(j *cleanJob, header core.LBA, h *disklog.RecordHeader) {
	maxCopy := s.log.MaxCopyBlocks()
	for i := range h.Entries {
		e := &h.Entries[i]
		if !e.HasBlocks() {
			continue
		}
		off := disklog.PayloadOffset(h.Entries, i) / core.BlockSize
		lba := header + core.LBA(h.Layout.HeaderBlocks) + core.LBA(off)
		var live [core.BlocksPerChunk / 64]uint64
		s.ix.LiveBlocks(e.Pool, e.Chunk, e.First, e.Count, lba, &live)
		for b := 0; b < int(e.Count); {
			if live[b/64]&(1<<(b%64)) == 0 {
				b++
				continue
			}
			n := 1
			for b+n < int(e.Count) && n < int(maxCopy) && live[(b+n)/64]&(1<<((b+n)%64)) != 0 {
				n++
			}
			j.pieces = append(j.pieces, cleanPiece{pool: e.Pool, chunk: e.Chunk, version: e.Version, first: e.First + uint16(b), count: uint16(n), src: lba + core.LBA(b), crc: len(j.crcs)})
			for k := range n {
				j.crcs = append(j.crcs, h.DataCRC(int(off)+b+k))
			}
			b += n
		}
	}
}

func (s *Store) freePieces(ctx *core.Context, j *cleanJob) {
	for k := range j.pieces {
		if j.pieces[k].buf != core.NoBuf {
			ctx.Pool.Free(j.pieces[k].buf)
			j.pieces[k].buf = core.NoBuf
		}
	}
}

// sendCopies hands the log the job's pieces, in key order, as copy items of the cleaner's lane.
func (s *Store) sendCopies(ctx *core.Context, p *cleanPass) {
	j := &p.job
	s.cl.items = s.cl.items[:0]
	for _, pc := range j.pieces {
		s.cl.items = append(s.cl.items, disklog.Item{Entry: disklog.CopyEntry(pc.pool, pc.chunk, pc.version, pc.first, pc.count, pc.src), Buf: pc.buf})
	}
	var err error
	s.cl.tickets, err = s.log.Copy(ctx, s.cl.items, s.cl.tickets[:0], &s.lout)
	for k, t := range s.cl.tickets {
		pc := &j.pieces[k]
		pc.buf = core.NoBuf
		j.copies++
		s.cl.stats.Copied += uint64(pc.count)
		s.cl.sent = append(s.cl.sent, cleanSent{ticket: t, pass: p, piece: k})
	}
	if err != nil {
		j.failed = true
		s.freePieces(ctx, j)
	}
}

// copyResolved takes a copy item the log resolved: durable, it applies as replay would, wherever a
// map still stores the copied blocks at their source; failed, it leaves its victim used.
func (s *Store) copyResolved(t disklog.Ticket, ev *disklog.Event, i int) bool {
	live := s.cl.sent[s.cl.sentHead:]
	k, ok := slices.BinarySearchFunc(live, t, func(c cleanSent, t disklog.Ticket) int { return cmp.Compare(c.ticket, t) })
	if !ok || live[k].pass == nil {
		return false
	}
	c := live[k]
	live[k].pass = nil
	for s.cl.sentHead < len(s.cl.sent) && s.cl.sent[s.cl.sentHead].pass == nil {
		s.cl.sentHead++
	}
	if s.cl.sentHead > len(s.cl.sent)/2 {
		s.cl.sent = s.cl.sent[:copy(s.cl.sent, s.cl.sent[s.cl.sentHead:])]
		s.cl.sentHead = 0
	}
	j := &c.pass.job
	j.copies--
	if ev.Kind != disklog.Durable {
		j.failed = true
		return true
	}
	s.entry[0], s.payload[0] = ev.Entries[i], ev.Payloads[i]
	s.apply = disklog.Event{Kind: disklog.Durable, LSN: ev.LSN, LBA: ev.LBA, HeaderBlocks: ev.HeaderBlocks, Entries: s.entry[:], Payloads: s.payload[:]}
	if err := s.ix.Apply(&s.apply); err != nil {
		panic("chunkstore: the index refused the cleaner's copy: " + err.Error())
	}
	s.reportLive()
	return true
}

// finishVictim frees the current victim once every copy applied and nothing it holds is live,
// and moves the pass on; a victim it cannot free yet stays used and may be chosen again.
func (s *Store) finishVictim(ctx *core.Context, p *cleanPass) {
	j := &p.job
	s.freePieces(ctx, j)
	if !j.failed && !j.bad && s.log.Free(ctx, j.segment, &s.lout) == nil {
		s.cl.stats.Freed++
		p.freed++
	}
	p.cur++
	j.segment = 0
}

// resetCleaner drops every pass and frees the blocks it read, as when the store's log stops; their
// reads have completed.
func (s *Store) resetCleaner(ctx *core.Context) {
	for _, p := range s.cl.passes {
		s.freePieces(ctx, &p.job)
	}
	s.cl.passes, s.cl.sent, s.cl.sentHead, s.cl.tried = nil, s.cl.sent[:0], 0, false
}
