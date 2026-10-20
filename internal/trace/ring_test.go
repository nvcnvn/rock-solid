package trace

import (
	"bytes"
	"errors"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

func newRing(t testing.TB, records int) *Ring {
	t.Helper()
	g, err := NewRing(make([]byte, records*RecordSize))
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func TestNewRingRejectsPartialRecords(t *testing.T) {
	for _, n := range []int{0, 1, RecordSize - 1, RecordSize + 1} {
		if _, err := NewRing(make([]byte, n)); !errors.Is(err, ErrRingSize) {
			t.Errorf("%d bytes gave %v", n, err)
		}
	}
}

func TestRingHoldsWhatWasAdded(t *testing.T) {
	g := newRing(t, 8)
	rng := core.NewRand(3)
	var want []byte
	var recs []Record
	for range 5 {
		r := randomRecord(rng)
		g.Add(&r)
		recs = append(recs, r)
		want = append(r.Append(want), '\n')
	}
	for i, r := range recs {
		if got := g.At(i); got != r {
			t.Fatalf("record %d: got %+v, want %+v", i, got, r)
		}
	}
	if got := g.Dump(nil); !bytes.Equal(got, want) {
		t.Fatalf("dump\n%s\nwant\n%s", got, want)
	}
}

func TestAWrappedRingDumpsTheNewestRecordsWhole(t *testing.T) {
	g := newRing(t, DefaultRecords)
	var r Record
	r.Name, r.Loop = SysRead, 2
	for i := range DefaultRecords + 1 {
		r.Tick, r.Pass, r.Fields[0], r.Fields[1] = core.Tick(1000+i), uint64(i), 17, uint64(i)
		g.Add(&r)
	}
	if g.Len() != DefaultRecords || g.Added() != DefaultRecords+1 {
		t.Fatalf("len %d, added %d", g.Len(), g.Added())
	}
	dump := g.Dump(nil)
	lines := bytes.Split(bytes.TrimSuffix(dump, []byte("\n")), []byte("\n"))
	if len(lines) != DefaultRecords {
		t.Fatalf("%d lines", len(lines))
	}
	for i, l := range lines {
		got, err := Parse(l)
		if err != nil {
			t.Fatalf("line %d %q: %v", i+1, l, err)
		}
		if want := uint64(i + 1); got.Pass != want || got.Fields[1] != want || got.Tick != core.Tick(1000+want) {
			t.Fatalf("line %d is %q, want record %d", i+1, l, want)
		}
	}
	if err := Check(dump, ReadBudget(256<<10)); err != nil {
		t.Fatal(err)
	}
}

func TestTwoRingsFedOneSeedDumpIdentically(t *testing.T) {
	dump := func() []byte {
		g := newRing(t, 64)
		rng := core.NewRand(11)
		for range 200 {
			r := randomRecord(rng)
			g.Add(&r)
		}
		return g.Dump(nil)
	}
	if a, b := dump(), dump(); !bytes.Equal(a, b) {
		t.Fatal("one seed gave two dumps")
	}
}

func TestRecordingAllocatesNothing(t *testing.T) {
	g := newRing(t, 16)
	r, err := Parse([]byte(everyName[7]))
	if err != nil {
		t.Fatal(err)
	}
	if n := testing.AllocsPerRun(1000, func() {
		r.Pass++
		g.Add(&r)
	}); n != 0 {
		t.Fatalf("Add allocates %v times", n)
	}
}

func BenchmarkRingAdd(b *testing.B) {
	g := newRing(b, DefaultRecords)
	r, err := Parse([]byte(everyName[7]))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		r.Pass++
		g.Add(&r)
	}
}
