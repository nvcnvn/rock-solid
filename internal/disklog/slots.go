package disklog

import (
	"math/bits"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// slotTable is the log's chunk slots: which each slot segment has taken, by an item queued or a
// round in flight, in use, or freed and held; the slot segments of each fill; and the TRIMs of
// segments freed with their last slot.
type slotTable struct {
	per   uint16
	taken []uint16
	// fill[k] marks the slot segments with k slots taken, 0 < k < per, and fillN[k] counts them.
	fill  [][]uint64
	fillN []int
	held  []heldSlot
	trims []trimIO
}

// heldSlot is a slot its owner freed, held from reuse and TRIM until the round of LSN until,
// the first begun after the free, is durable: that round's header names durable every record
// that wrote the slot, so recovery never checks the slot's data against them.
type heldSlot struct {
	segment uint32
	slot    uint16
	until   uint64
}

type trimIO struct {
	io      core.IOID
	segment uint32
}

func newSlotTable(g Geometry) slotTable {
	t := slotTable{per: g.SlotsPerSegment()}
	if t.per == 0 {
		return t
	}
	t.taken = make([]uint16, g.SegmentCount)
	t.fill, t.fillN = make([][]uint64, t.per), make([]int, t.per)
	for k := 1; k < int(t.per); k++ {
		t.fill[k] = make([]uint64, (g.SegmentCount+63)/64)
	}
	return t
}

// setTaken records segment s's slots taken, keeping the fill bitmaps.
func (t *slotTable) setTaken(s uint32, taken uint16) {
	for i, bitsSet := range [2]uint16{t.taken[s], taken} {
		if k := bits.OnesCount16(bitsSet); k > 0 && k < int(t.per) {
			if i == 0 {
				t.fill[k][s/64] &^= 1 << (s % 64)
				t.fillN[k]--
			} else {
				t.fill[k][s/64] |= 1 << (s % 64)
				t.fillN[k]++
			}
		}
	}
	t.taken[s] = taken
}

// Slots is how many chunk slots a slot segment holds, 0 on a disk whose segments are smaller than a chunk.
func (l *Log) Slots() uint16 { return l.slots.per }

// SlotAvailable reports whether a whole chunk submitted now goes to a chunk slot: the disk has
// slots and one is free, in a slot segment or a free segment outside the reserve. Otherwise the
// chunk goes to the log as several items.
func (l *Log) SlotAvailable() bool {
	_, _, _, ok := l.slotFor()
	return l.err == nil && ok
}

// SlotsInUse appends the first LBA of every chunk slot in use.
func (l *Log) SlotsInUse(dst []core.LBA) []core.LBA {
	for s := range l.usage {
		if u := &l.usage[s]; u.State == SegmentSlot {
			for set := u.Live; set != 0; set &= set - 1 {
				dst = append(dst, l.sb.SlotLBA(uint32(s), uint16(bits.TrailingZeros32(set))))
			}
		}
	}
	return dst
}

// slotFor picks the slot a slot write takes: the lowest free slot of the fullest slot segment
// with one, the lowest such segment first, or else slot 0 of a free segment, which an ordinary
// item may open only while more than the reserve are free.
func (l *Log) slotFor() (segment uint32, slot uint16, open bool, ok bool) {
	t := &l.slots
	for k := int(t.per) - 1; k > 0; k-- {
		if t.fillN[k] == 0 {
			continue
		}
		for w, set := range t.fill[k] {
			if set != 0 {
				s := uint32(w*64 + bits.TrailingZeros64(set))
				return s, uint16(bits.TrailingZeros16(^t.taken[s])), false, true
			}
		}
	}
	if t.per == 0 {
		return 0, 0, false, false
	}
	segment, ok = l.firstFree()
	if !ok || l.freeCount() <= l.reserve {
		return 0, 0, false, false
	}
	return segment, 0, true, true
}

// takeSlot gives a whole chunk's write a slot as it is submitted, so it never waits for one.
func (l *Log) takeSlot(e *Entry) bool {
	s, slot, open, ok := l.slotFor()
	if !ok {
		return false
	}
	if open {
		l.free[s/64] &^= 1 << (s % 64)
	}
	l.slots.setTaken(s, l.slots.taken[s]|1<<slot)
	*e = SlotEntry(e.Pool, e.Chunk, e.Version, s, slot)
	return true
}

// untake gives back the slot of a slot write that failed before its round; a segment opened for
// it alone, never written, is free again.
func (l *Log) untake(e *Entry) {
	s := e.Segment
	l.slots.setTaken(s, l.slots.taken[s]&^(1<<e.Slot))
	if l.slots.taken[s] == 0 && l.usage[s].State != SegmentSlot {
		l.free[s/64] |= 1 << (s % 64)
	}
}

// useSlots marks the slots a durable round wrote in use.
func (l *Log) useSlots(r *round) {
	for i := range r.entries {
		if e := &r.entries[i]; e.Kind == EntrySlot {
			l.changed[e.Segment/64] |= 1 << (e.Segment % 64)
			u := &l.usage[e.Segment]
			u.State, u.Newest = SegmentSlot, r.lsn
			u.Live |= 1 << e.Slot
		}
	}
}

// FreeSlot frees the chunk slot whose first block is lba, which its owner's index no longer refers to since a durable record rewrote or deleted its chunk. The slot is reused, and its segment, freed with its last slot, TRIMmed, only once a round begun after the free is durable. A slot not in use is ErrMalformed.
func (l *Log) FreeSlot(lba core.LBA, out *Output) error {
	if l.err != nil {
		return l.err
	}
	s, slot, ok := l.sb.SlotOf(lba)
	if !ok || slot >= l.slots.per || l.usage[s].State != SegmentSlot || l.usage[s].Live&(1<<slot) == 0 {
		return ErrMalformed
	}
	u := &l.usage[s]
	u.Live &^= 1 << slot
	if u.Live == 0 {
		*u = SegmentUsage{}
	}
	l.changed[s/64] |= 1 << (s % 64)
	l.slots.held = append(l.slots.held, heldSlot{segment: s, slot: slot, until: l.nextLSN})
	return nil
}

// releaseHeld makes the held slots whose round is durable free, and TRIMs each segment that
// frees.
func (l *Log) releaseHeld(out *Output) {
	t := &l.slots
	t.held = slices.DeleteFunc(t.held, func(h heldSlot) bool {
		if h.until > l.durableLSN {
			return false
		}
		if t.setTaken(h.segment, t.taken[h.segment]&^(1<<h.slot)); t.taken[h.segment] == 0 {
			io := l.io()
			t.trims = append(t.trims, trimIO{io: io, segment: h.segment})
			out.Actions = append(out.Actions, core.Action{Kind: core.ActDiskTrim, IO: io, LBA: l.sb.LBA(h.segment, 0), Len: l.sb.SegmentBytes()})
		}
		return true
	})
}

// trimDone frees a segment whose TRIM completed, failed or not: a failed discard leaves old data
// that new writes overwrite, and is no longer in flight to erase them.
func (l *Log) trimDone(io core.IOID) bool {
	t := &l.slots
	j := slices.IndexFunc(t.trims, func(t trimIO) bool { return t.io == io })
	if j < 0 {
		return false
	}
	s := t.trims[j].segment
	t.trims = slices.Delete(t.trims, j, j+1)
	l.free[s/64] |= 1 << (s % 64)
	return true
}
