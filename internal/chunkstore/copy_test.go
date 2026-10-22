package chunkstore

import (
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// Copy entries in replay: chunk 7 version 1 block 0 at LBA 101, and a copy record at LBA 600
// moving it from LBA 101 to LBA 601. Logged after a write of version 2 of block 0 the copy
// changes nothing; logged before it, it applies and the write then replaces it. Either way a
// rebuild equals the index the owner kept, and applying the records twice changes nothing. A
// plain write entry in the copy's place maps block 0 back to version 1.
func TestCopyEntriesInReplay(t *testing.T) {
	w1 := record(100, disklog.WriteEntry(1, chunk(7), 1, 0, 1))
	w2 := record(200, disklog.WriteEntry(1, chunk(7), 2, 0, 1))
	cp := record(600, disklog.CopyEntry(1, chunk(7), 1, 0, 1, 101))
	for _, c := range []struct {
		name  string
		order []*disklog.Event
	}{
		{"copy after the overwrite", []*disklog.Event{w1, w2, cp}},
		{"copy before the overwrite", []*disklog.Event{w1, cp, w2}},
	} {
		owner := newIndex(t)
		for _, ev := range c.order {
			apply(t, owner, ev)
		}
		if e, _ := owner.Lookup(1, chunk(7), 0); e.LBA != 201 || owner.Live(100) != 0 || owner.Live(600) != 0 {
			t.Fatalf("%s: block 0 at %+v, record 100 refers to %d blocks, record 600 to %d", c.name, e, owner.Live(100), owner.Live(600))
		}
		twice := newIndex(t)
		for _, ev := range append(slices.Clone(c.order), c.order...) {
			apply(t, twice, ev)
		}
		if !slices.Equal(dumpIndex(twice), dumpIndex(owner)) {
			t.Fatalf("%s: applying the records twice gives another index", c.name)
		}
	}

	plain := newIndex(t)
	for _, ev := range []*disklog.Event{w1, w2, record(600, disklog.WriteEntry(1, chunk(7), 1, 0, 1))} {
		apply(t, plain, ev)
	}
	if e, _ := plain.Lookup(1, chunk(7), 0); e.Version != 1 {
		t.Fatalf("a write entry in the copy's place left block 0 at %+v", e)
	}
}

// What happens between the worker's copy of chunk 7 block 3 from LBA 100 to LBA 5000 and the
// owner's apply decides where the maps point after it.
func TestCopyAppliesWhereAMapStillPointsAtTheSource(t *testing.T) {
	const copied = 5000
	cp := record(copied-1, disklog.CopyEntry(1, chunk(7), 1, 3, 1, 100))
	base := record(99, disklog.WriteEntry(1, chunk(7), 1, 3, 1))
	for _, c := range []struct {
		name         string
		between      []*disklog.Event
		live, frozen core.LBA
		copyLive     uint32
	}{
		{"nothing", nil, copied, 0, 1},
		{"a write of block 3 to LBA 900", []*disklog.Event{record(899, disklog.WriteEntry(1, chunk(7), 2, 3, 1))}, 900, 0, 0},
		{"a punch of block 3", []*disklog.Event{record(300, disklog.PunchEntry(1, chunk(7), 2, 3, 1))}, 0, 0, 0},
		{"a snapshot cut and a freeze of the version", []*disklog.Event{record(300, disklog.FreezeEntry(1, chunk(7), 1, 1))}, copied, copied, 2},
		{"a freeze, then a write of block 3 to LBA 900", []*disklog.Event{record(300, disklog.FreezeEntry(1, chunk(7), 1, 1)), record(899, disklog.WriteEntry(1, chunk(7), 2, 3, 1))}, 900, copied, 1},
		{"a freeze, then a whole-chunk punch", []*disklog.Event{record(300, disklog.FreezeEntry(1, chunk(7), 1, 1)), record(400, disklog.PunchEntry(1, chunk(7), 2, 0, core.BlocksPerChunk))}, 0, copied, 1},
	} {
		x := newIndex(t)
		apply(t, x, base)
		for _, ev := range c.between {
			apply(t, x, ev)
		}
		apply(t, x, cp)
		var live core.LBA
		if e, ok := x.Lookup(1, chunk(7), 3); ok {
			live = e.BlockLBA(3)
		}
		var frozen core.LBA
		if e, ok := x.LookupAt(1, chunk(7), 3, 1); ok && len(c.between) > 0 && c.between[0].Entries[0].Kind == disklog.EntryFreeze {
			frozen = e.BlockLBA(3)
		}
		if live != c.live || frozen != c.frozen || x.Live(copied-1) != c.copyLive {
			t.Fatalf("%s: live map at %d, frozen at %d, the copy referred to %d times; want %d, %d, %d", c.name, live, frozen, x.Live(copied-1), c.live, c.frozen, c.copyLive)
		}
	}
}

// A copy names its source block by block, so a write landing in the middle of the copied
// extent leaves the blocks around it to move.
func TestCopyMovesTheBlocksStillAtTheSource(t *testing.T) {
	x := newIndex(t)
	apply(t, x, record(100, disklog.WriteEntry(1, chunk(7), 1, 0, 4)))
	apply(t, x, record(200, disklog.WriteEntry(1, chunk(7), 2, 1, 1)))
	apply(t, x, record(500, disklog.CopyEntry(1, chunk(7), 1, 0, 4, 101)))
	want := []Extent{{0, 1, 1, 501, 500}, {1, 1, 2, 201, 200}, {2, 2, 1, 503, 500}}
	if got := extents(x, chunk(7), 0); !slices.Equal(got, want) || x.Live(100) != 0 || x.Live(500) != 3 {
		t.Fatalf("live map %+v, records 100 and 500 refer to %d and %d blocks; want %+v, 0 and 3", got, x.Live(100), x.Live(500), want)
	}
}

// [property] With tracking on, the record counts equal the blocks each durable record wrote
// plus every LiveChange the index kept, so the owner can keep the log's usage table to the
// index's counts by handing it the changes.
func TestLiveChangesKeepTheCountsOfEveryRecord(t *testing.T) {
	r := core.NewRand(31)
	x := slotIndex(t)
	x.TrackLive(true)
	want := map[core.LBA]int{}
	lba := core.LBA(100)
	var changes []LiveChange
	for i := range 3000 {
		c := chunk(uint32(r.Below(4)))
		first := uint16(r.Below(16))
		count := uint16(r.Range(1, 4))
		var e disklog.Entry
		switch r.Below(8) {
		case 0, 1:
			e = disklog.WriteEntry(1, c, uint64(i+1), first, count)
		case 2:
			e = disklog.PunchEntry(1, c, uint64(i+1), first, count)
		case 3:
			e = disklog.FreezeEntry(1, c, uint64(i), uint64(i))
		case 6:
			vers := x.frozen(nil, 1, c)
			if len(vers) == 0 {
				continue
			}
			x.Release(1, c, vers[2*r.Below(uint64(len(vers)/2))])
			changes = x.TakeChanges(changes[:0])
			for _, ch := range changes {
				want[ch.Header] += int(ch.Delta)
			}
			continue
		case 7:
			slot := uint16(r.Below(4))
			if x.Live(slotLBA(slot)) != 0 {
				continue
			}
			e = disklog.SlotEntry(1, c, uint64(i+1), slotSegment, slot)
		default:
			ext, ok := x.Lookup(1, c, first)
			if _, slot := x.slotOf(ext); !ok || ext.LBA == 0 || slot {
				continue
			}
			n := min(count, uint16(ext.end()-int(first)))
			e = disklog.CopyEntry(1, c, ext.Version, first, n, ext.BlockLBA(first))
		}
		if end := lba%slotSegmentBlocks + 1 + core.LBA(e.Count); e.HasBlocks() && end > slotSegmentBlocks {
			lba += slotSegmentBlocks - lba%slotSegmentBlocks
		}
		ev := record(lba, e)
		lba++
		if e.HasBlocks() || e.Kind == disklog.EntrySlot {
			want[ev.LBA] += int(e.Count)
		}
		if e.HasBlocks() {
			lba += core.LBA(e.Count)
		}
		if err := x.Apply(ev); err != nil {
			t.Fatal(err)
		}
		changes = x.TakeChanges(changes[:0])
		for _, ch := range changes {
			want[ch.Header] += int(ch.Delta)
		}
	}
	for h, n := range want {
		if got := int(x.Live(h)); got != n {
			t.Fatalf("record %d refers to %d blocks; its own and the changes kept say %d", h, got, n)
		}
	}
	if err := x.Check(); err != nil {
		t.Fatal(err)
	}
}

// slotSegment is where TestLiveChangesKeepTheCountsOfEveryRecord puts its chunk slots, past its records.
const slotSegment = 50

func slotLBA(slot uint16) core.LBA {
	return core.LBA(slotSegment*slotSegmentBlocks) + core.LBA(slot)*core.BlocksPerChunk
}
