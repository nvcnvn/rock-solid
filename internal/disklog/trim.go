package disklog

import (
	"slices"

	"github.com/nvcnvn/rock-solid/internal/core"
)

const (
	// DefaultTrimRate is trim_rate, the bytes a second of freed segments the log discards, CockroachDB's file deletion rate.
	DefaultTrimRate = 128 << 20
	// TrimTimer is the timer the log arms for its next paced TRIM; its owner hands the log that timer's event.
	TrimTimer  core.TimerID = 0x7472696d
	backlogAge              = core.Tick(5 * 60 * 1000 * 1000)
)

type queuedTrim struct {
	segment uint32
	at      core.Tick
	slot    bool
}

// trimPacer paces the TRIMs of freed segments at a base rate, one segment per discard, as
// Pebble's delete pacer paces deletions: once segments have waited 5 minutes, it adds the rate that
// clears that backlog within the next 5 minutes, and holds that rate until the backlog is gone.
type trimPacer struct {
	rate     float64
	segBytes float64
	queue    []queuedTrim
	extra    float64
	nextAt   core.Tick
}

func (p *trimPacer) push(seg uint32, now core.Tick, slot bool) {
	p.queue = append(p.queue, queuedTrim{segment: seg, at: now, slot: slot})
}

// drop takes seg out of the queue, as when it is allocated again before its TRIM ran.
func (p *trimPacer) drop(seg uint32) {
	p.queue = slices.DeleteFunc(p.queue, func(q queuedTrim) bool { return q.segment == seg })
}

// next is the segment to discard now, if its turn has come, and otherwise when to ask again, 0
// with nothing queued.
func (p *trimPacer) next(now core.Tick) (queuedTrim, bool, core.Tick) {
	if len(p.queue) == 0 {
		return queuedTrim{}, false, 0
	}
	if now < p.nextAt {
		return queuedTrim{}, false, p.nextAt
	}
	var backlog float64
	for _, q := range p.queue {
		if now-q.at >= backlogAge {
			backlog += p.segBytes
		}
	}
	if backlog == 0 {
		p.extra = 0
	} else {
		p.extra = max(p.extra, backlog/backlogAge.Duration().Seconds())
	}
	q := p.queue[0]
	p.queue = p.queue[1:]
	p.nextAt = now + core.Tick(p.segBytes/(p.rate+p.extra)*1e6)
	return q, true, 0
}

// trimState is a freed segment's way to its TRIM: freed before the checkpoint that will record it
// begins, recorded by a running checkpoint, recorded by a durable manifest that no segment header
// names yet, then queued, then issued.
type trimState struct {
	waiting []uint32
	named   []namedTrim
	pacer   trimPacer
	issued  []issuedTrim
	timerAt core.Tick
}

type namedTrim struct {
	segment uint32
	lsn     uint64
}

type issuedTrim struct {
	segment uint32
	io      core.IOID
}

func (l *Log) trimmed(segment uint32) bool {
	return slices.ContainsFunc(l.trims.issued, func(t issuedTrim) bool { return t.segment == segment })
}

// noteFreed starts a freed segment on its way to its TRIM; a disk that is a file never trims.
func (l *Log) noteFreed(segment uint32) {
	if !l.cfg.NoTrim {
		l.trims.waiting = append(l.trims.waiting, segment)
	}
}

// reuseTrim drops the TRIM of a segment allocated again, wherever it is on its way.
func (l *Log) reuseTrim(segment uint32) {
	t := &l.trims
	del := func(s []uint32) []uint32 { return slices.DeleteFunc(s, func(x uint32) bool { return x == segment }) }
	t.waiting = del(t.waiting)
	if l.cp != nil {
		l.cp.trims = del(l.cp.trims)
	}
	t.named = slices.DeleteFunc(t.named, func(q namedTrim) bool { return q.segment == segment })
	t.pacer.drop(segment)
}

// trimNamed queues the TRIMs of segments a manifest freed once a segment header names it: until
// then recovery loads an older manifest, which lists them, and a listed segment that reads as zeros
// is damage.
func (l *Log) trimNamed() {
	t := &l.trims
	k := 0
	for _, q := range t.named {
		if q.lsn <= l.namedLSN {
			t.pacer.push(q.segment, l.now, false)
			continue
		}
		t.named[k] = q
		k++
	}
	t.named = t.named[:k]
}

// pumpTrims issues the TRIMs whose turn has come and arms the timer for the next.
func (l *Log) pumpTrims(out *Output) {
	for l.err == nil {
		q, ok, wake := l.trims.pacer.next(l.now)
		if !ok {
			if wake != 0 && wake != l.trims.timerAt {
				l.trims.timerAt = wake
				out.Actions = append(out.Actions, core.Action{Kind: core.ActSetTimer, Timer: TrimTimer, At: wake})
			}
			return
		}
		io := l.io()
		if q.slot {
			l.slots.trims = append(l.slots.trims, trimIO{io: io, segment: q.segment})
		} else {
			l.trims.issued = append(l.trims.issued, issuedTrim{segment: q.segment, io: io})
		}
		out.Actions = append(out.Actions, core.Action{Kind: core.ActDiskTrim, IO: io, LBA: l.sb.LBA(q.segment, 0), Len: uint64(l.sb.SegmentBlocks) * core.BlockSize})
	}
}

// trimSlotSegment queues the TRIM of a slot segment freed with its last slot, which stays out of
// the free segments until the write of zeros that follows the TRIM completes; a disk that is a
// file skips the TRIM and writes the zeros at once.
func (l *Log) trimSlotSegment(segment uint32, out *Output) {
	if !l.cfg.NoTrim {
		l.trims.pacer.push(segment, l.now, true)
		return
	}
	io := l.io()
	l.slots.trims = append(l.slots.trims, trimIO{io: io, segment: segment, zeroing: true})
	out.Actions = append(out.Actions, core.Action{Kind: core.ActDiskWrite, IO: io, LBA: l.sb.LBA(segment, 0), Buf: l.slots.zero, Len: core.BlockSize})
}

// trimDone takes a TRIM's completion; a failed TRIM costs only the discard, as one lost at a crash
// does, and is ignored.
func (l *Log) trimDone(io core.IOID) bool {
	i := slices.IndexFunc(l.trims.issued, func(t issuedTrim) bool { return t.io == io })
	if i < 0 {
		return false
	}
	l.trims.issued = slices.Delete(l.trims.issued, i, i+1)
	return true
}
