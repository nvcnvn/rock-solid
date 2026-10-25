package chunkstore

import (
	"bytes"
	"errors"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

func fullTable(pgs, width int) Table {
	p := PoolTable{ID: 1, Replicas: uint8(width), MinLive: 1, PGs: make([]PGEntry, pgs)}
	for i := range p.PGs {
		p.PGs[i] = PGEntry{Interval: 1, Activated: 1}
		for j := range width {
			p.PGs[i].Members = append(p.PGs[i].Members, Member{Disk: uint32(j)})
		}
	}
	return Table{Epoch: 1, Pools: []PoolTable{p}}
}

// A table's size depends on PG counts and widths: 26 bytes per rep3 entry, 101 per ec16-2 one, before past intervals.
func TestTableSize(t *testing.T) {
	for _, c := range []struct {
		pgs, width, want int
	}{{2048, 3, 53_270}, {4096, 18, 413_718}} {
		tbl := fullTable(c.pgs, c.width)
		if got := len(AppendTable(nil, &tbl)); got != c.want {
			t.Errorf("%d PGs × %d members: %d bytes, want %d", c.pgs, c.width, got, c.want)
		}
	}
}

// A delta costs a 30-byte header for one pool, 34 bytes per changed rep3 entry, 20 more with its previous interval's members, 5 more during a move for the member added.
func TestDeltaSize(t *testing.T) {
	prev := fullTable(2048, 3)
	next := fullTable(2048, 3)
	next.Epoch = 2
	e := &next.Pools[0].PGs[7]
	e.Past = []PastInterval{{1, e.Members}}
	e.Interval, e.Members = 2, []Member{{Disk: 0}, {Disk: 1}, {Disk: 9}}
	next.Pools[0].PGs[8].Pinned = true
	d := MakeDelta(&prev, &next)
	if got := len(AppendDelta(nil, &d)); got != 30+2*34+20 {
		t.Fatalf("a delta of two entries, one with a past interval: %d bytes", got)
	}
	moving := &next.Pools[0].PGs[9]
	moving.Members = append(moving.Members, Member{Disk: 9})
	moving.Members[0].Leaving, moving.Interval = true, 2
	d = MakeDelta(&prev, &next)
	if got := len(AppendDelta(nil, &d)); got != 30+3*34+20+5 {
		t.Fatalf("a delta of three entries, one moving: %d bytes", got)
	}
}

// A delta applies only to the table it follows: another base at its epoch fails the CRC, one at another epoch is a gap; a node then fetches the whole table, and one that jumps five tables that way finds no interval gone backwards.
func TestDeltaBases(t *testing.T) {
	l := placed(t, nodesOf(rep(3, 256), 4, 1, 2))
	var tables []Table
	for i := range 6 {
		tables = append(tables, l.table)
		l.mark([]Mark{MarkDown, MarkUp}[i%2], l.onNode(uint32(i / 2))[0])
		l.step(t, l.inputs(0))
	}
	tables = append(tables, l.table)
	e := len(tables) - 2
	d := MakeDelta(&tables[e], &tables[e+1])
	other := tables[e-2]
	other.Epoch = tables[e].Epoch
	if _, err := other.Apply(&d); !errors.Is(err, ErrTableCRC) {
		t.Fatalf("a delta on another base at its epoch: %v", err)
	}
	if _, err := tables[e-1].Apply(&d); !errors.Is(err, ErrTableGap) {
		t.Fatalf("a delta on table e - 1: %v", err)
	}
	if _, err := tables[e-2].Apply(&d); !errors.Is(err, ErrTableGap) {
		t.Fatalf("the delta for e + 2 on table e: %v", err)
	}
	whole, err := DecodeTable(AppendTable(nil, &tables[len(tables)-1]))
	if err != nil {
		t.Fatal(err)
	}
	if whole.Epoch != tables[0].Epoch+6 {
		t.Fatalf("jumped to table %d from %d", whole.Epoch, tables[0].Epoch)
	}
	for i, en := range whole.Pools[0].PGs {
		if en.Interval < tables[0].Pools[0].PGs[i].Interval {
			t.Fatalf("PG %d went back from interval %d to %d", i, tables[0].Pools[0].PGs[i].Interval, en.Interval)
		}
	}
}

// A truncated delta, an entry for a PG past its pool's count, an interval number that goes backwards and two leaving members are malformed.
func TestMalformedDeltas(t *testing.T) {
	prev := fullTable(16, 3)
	next := fullTable(16, 3)
	next.Epoch = 2
	next.Pools[0].PGs[3].Interval = 2
	d := MakeDelta(&prev, &next)
	b := AppendDelta(nil, &d)
	if _, err := DecodeDelta(b[:len(b)-1]); !errors.Is(err, ErrMalformed) {
		t.Fatalf("a truncated delta: %v", err)
	}
	past := d
	past.Entries = []DeltaEntry{{core.PGID{Pool: 1, Index: 16}, d.Entries[0].Entry}}
	if _, err := DecodeDelta(AppendDelta(nil, &past)); !errors.Is(err, ErrMalformed) {
		t.Fatalf("an entry past pg_count: %v", err)
	}
	if _, err := next.Apply(&Delta{Epoch: 3, CRC: prev.CRC(), Pools: d.Pools, Entries: []DeltaEntry{{core.PGID{Pool: 1, Index: 3}, prev.Pools[0].PGs[3]}}}); !errors.Is(err, ErrMalformed) {
		t.Fatalf("an interval going backwards: %v", err)
	}
	bad := fullTable(16, 3)
	bad.Epoch = 2
	e := &bad.Pools[0].PGs[3]
	e.Interval, e.Members[0].Leaving, e.Members[1].Leaving = 2, true, true
	d = MakeDelta(&prev, &bad)
	if _, err := DecodeDelta(AppendDelta(nil, &d)); !errors.Is(err, ErrMalformed) {
		t.Fatalf("two leaving members: %v", err)
	}
}

// A delta for a new or resized pool carries every one of its PGs: one entry under a pg_count of 2³¹ is malformed before the table is allocated.
func TestDeltaCarriesEveryNewPG(t *testing.T) {
	prev := fullTable(4, 3)
	huge := hugeDelta(&prev)
	got, err := DecodeDelta(AppendDelta(nil, &huge))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := prev.Apply(&got); !errors.Is(err, ErrMalformed) {
		t.Fatalf("a new pool of 2³¹ PGs with one entry: %v", err)
	}
	resized := fullTable(8, 3)
	resized.Epoch = 2
	d := MakeDelta(&prev, &resized)
	d.Entries = d.Entries[1:]
	if _, err := prev.Apply(&d); !errors.Is(err, ErrMalformed) {
		t.Fatalf("a split pool missing an entry: %v", err)
	}
}

func hugeDelta(prev *Table) Delta {
	e := prev.Pools[0].PGs[0]
	return Delta{Epoch: prev.Epoch + 1, Pools: []DeltaPool{{1, 3, 1, 4}, {2, 3, 1, 1 << 31}}, Entries: []DeltaEntry{{core.PGID{Pool: 2, Index: 1<<31 - 1}, e}}}
}

func FuzzDecodeTable(f *testing.F) {
	tbl := fullTable(4, 3)
	tbl.Pools[0].PGs[1].Interval = 3
	tbl.Pools[0].PGs[1].Past = []PastInterval{{1, tbl.Pools[0].PGs[1].Members}, {2, nil}}
	moving := &tbl.Pools[0].PGs[3]
	moving.Members = append(moving.Members, Member{Disk: 7})
	moving.Members[1].Leaving, moving.Interval = true, 2
	f.Add(AppendTable(nil, &tbl))
	stalled := &tbl.Pools[0].PGs[2]
	for i := range uint32(300) {
		stalled.Past = append(stalled.Past, PastInterval{Interval: i + 1, Members: stalled.Members})
	}
	stalled.Interval = 301
	f.Add(AppendTable(nil, &tbl))
	f.Fuzz(func(t *testing.T, b []byte) {
		got, err := DecodeTable(b)
		if err != nil {
			return
		}
		if again := AppendTable(nil, &got); !bytes.Equal(again, b) {
			t.Fatalf("%x decodes to %+v, which encodes to %x", b, got, again)
		}
	})
}

func FuzzDecodeDelta(f *testing.F) {
	prev := fullTable(4, 3)
	next := fullTable(8, 3)
	next.Epoch = 2
	d := MakeDelta(&prev, &next)
	f.Add(AppendDelta(nil, &d))
	next = fullTable(4, 3)
	next.Epoch, next.Pools[0].PGs[2].Interval = 2, 2
	d = MakeDelta(&prev, &next)
	f.Add(AppendDelta(nil, &d))
	moving := &next.Pools[0].PGs[3]
	moving.Members = append(moving.Members, Member{Disk: 7})
	moving.Members[0].Leaving, moving.Interval = true, 2
	d = MakeDelta(&prev, &next)
	f.Add(AppendDelta(nil, &d))
	d = hugeDelta(&prev)
	f.Add(AppendDelta(nil, &d))
	f.Fuzz(func(t *testing.T, b []byte) {
		got, err := DecodeDelta(b)
		if err != nil {
			return
		}
		if again := AppendDelta(nil, &got); !bytes.Equal(again, b) {
			t.Fatalf("%x decodes to %+v, which encodes to %x", b, got, again)
		}
		if applied, err := prev.Apply(&got); err == nil && applied.CRC() != got.CRC {
			t.Fatal("an applied delta's result fails its CRC")
		}
	})
}
