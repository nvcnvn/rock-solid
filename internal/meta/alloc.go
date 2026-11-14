package meta

// Allocator hands out the inode IDs and slots a daemon reserved on one shard of a volume, from
// memory, and says when to reserve the next batch: before the last one runs out, so a create
// waits on a reservation round only at the daemon's first in a slot.
type Allocator struct {
	slots batches
	// free is the shard's f key read present; it is never deleted once written.
	free   bool
	inodes map[uint32]*batches
}

// batches is what is left of a reservation, and of the one after it that arrived early.
type batches struct {
	cur, spare [2]uint64
	// asked is a reservation in flight; full is a slot whose sequence numbers, or a shard whose slots, ran out.
	asked, full bool
}

func (b *batches) take() (uint64, bool) {
	if b.cur[0] == b.cur[1] {
		b.cur, b.spare = b.spare, [2]uint64{}
	}
	if b.cur[0] == b.cur[1] {
		return 0, false
	}
	b.cur[0]++
	return b.cur[0] - 1, true
}

func (b *batches) need(prefetch uint64) bool {
	if b.asked || b.full || b.cur[1]-b.cur[0]+b.spare[1]-b.spare[0] > prefetch {
		return false
	}
	b.asked = true
	return true
}

// add takes the batch [lo, hi) of a source that ends at limit, and marks the source run out once hi reaches it.
func (b *batches) add(lo, hi, limit uint64) {
	b.asked, b.full = false, b.full || hi >= limit
	switch {
	case lo >= hi:
	case b.cur[0] == b.cur[1]:
		b.cur = [2]uint64{lo, hi}
	default:
		b.spare = [2]uint64{lo, hi}
	}
}

func (a *Allocator) slot(s uint32) *batches {
	if a.inodes == nil {
		a.inodes = make(map[uint32]*batches)
	}
	b := a.inodes[s]
	if b == nil {
		b = &batches{}
		a.inodes[s] = b
	}
	return b
}

// Inode takes the next reserved inode ID of slot; false when none is left.
func (a *Allocator) Inode(slot uint32) (uint64, bool) {
	seq, ok := a.slot(slot).take()
	return uint64(slot)<<SeqBits | seq, ok
}

// NeedInodes reports that the caller should send ReserveInodes for slot now; once it is true,
// it stays false until InodesReserved or InodesFailed.
func (a *Allocator) NeedInodes(slot uint32) bool { return a.slot(slot).need(InodePrefetch) }

// InodesReserved takes a reservation of slot whose Update returned end.
func (a *Allocator) InodesReserved(slot uint32, end uint64) {
	a.slot(slot).add(max(end, InodeBatch+1)-InodeBatch, min(end, 1<<SeqBits), 1<<SeqBits)
}

// InodesFailed ends a reservation of slot that got no answer; the next NeedInodes asks again.
func (a *Allocator) InodesFailed(slot uint32) { a.slot(slot).asked = false }

// SlotFull reports that slot's sequence numbers ran out, so new files go to a fresh slot.
func (a *Allocator) SlotFull(slot uint32) bool {
	b := a.slot(slot)
	return b.full && b.cur[0] == b.cur[1] && b.spare[0] == b.spare[1]
}

// Adopt takes sequences 2 … InodeBatch + 1 of a slot this daemon's Mkdir took fresh.
func (a *Allocator) Adopt(slot uint32) { a.slot(slot).add(2, 2+InodeBatch, 1<<SeqBits) }

// Slot takes the next reserved slot; false when none is left.
func (a *Allocator) Slot() (uint32, bool) {
	s, ok := a.slots.take()
	return uint32(s), ok
}

// NeedSlots reports that the caller should reserve slots now, as NeedInodes does. Until Free,
// it reads the shard's f key and passes what it found to FreeRead instead of sending ReserveSlots.
func (a *Allocator) NeedSlots() bool { return a.slots.need(SlotPrefetch) }

// Free reports that the shard's f key exists, so ReserveSlots may be sent: an Update would create an absent one.
func (a *Allocator) Free() bool { return a.free }

// FreeRead takes the read of the shard's f key; a shard without one, which no split writes yet
// (phase 13), counts as used up.
func (a *Allocator) FreeRead(present bool) {
	a.free = present
	if !present {
		a.slots.add(0, 0, 0)
	}
}

// SlotsReserved takes a reservation whose Update returned end, on a shard whose slots end at limit.
func (a *Allocator) SlotsReserved(end uint64, limit uint32) {
	a.slots.add(end-SlotBatch, min(end, uint64(limit)), uint64(limit))
}

// SlotsFailed ends a slot reservation that got no answer.
func (a *Allocator) SlotsFailed() { a.slots.asked = false }

// NoSlots reports that the shard has no free slot left, so a new directory shares its parent's slot.
func (a *Allocator) NoSlots() bool {
	return a.slots.full && a.slots.cur[0] == a.slots.cur[1] && a.slots.spare[0] == a.slots.spare[1]
}
