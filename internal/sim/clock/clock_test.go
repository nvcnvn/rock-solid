package clock

import (
	"go/build"
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

var start = core.Clocks{Boot: 5_000_000, Raw: 3_000_000, Mono: 4_000_000, Marker: 7}

func TestTheClocksRunAtTheirRates(t *testing.T) {
	m := New(100, start, Unit+40_000_000)
	m.SetSlew(100, -226_000_000)
	got := m.Read(100 + 10_000_000)
	if got.Raw-start.Raw != 10_400_000 {
		t.Fatalf("raw gained %d in 10 s at +4%%", got.Raw-start.Raw)
	}
	if b := got.Boot - start.Boot; b != 8_049_600 || got.Mono-start.Mono != b {
		t.Fatalf("BOOTTIME gained %d and MONOTONIC %d, want 10.4 s slewed by -22.6%%", b, got.Mono-start.Mono)
	}
	if got.Marker != start.Marker {
		t.Fatalf("marker %d", got.Marker)
	}
}

func TestARateChangeKeepsTheClocksContinuous(t *testing.T) {
	m := New(0, start, Unit)
	before := m.Read(1_000_000)
	m.SetRate(1_000_000, Unit+5_000_000)
	if m.Read(1_000_000) != before {
		t.Fatalf("a rate step moved the clocks: %+v, then %+v", before, m.Read(1_000_000))
	}
	if d := m.Read(2_000_000).Raw - before.Raw; d != 1_005_000 {
		t.Fatalf("raw gained %d in the second after a +0.5%% step", d)
	}
	if m.Rate() != Unit+5_000_000 || m.Slew() != 0 {
		t.Fatalf("rate %d slew %d", m.Rate(), m.Slew())
	}
}

// When is exact: CLOCK_BOOTTIME reaches the deadline at the time it names and not a tick sooner,
// whatever the rate, including rates that skip ticks.
func TestWhenIsTheFirstRealTimeBOOTTIMEReachesADeadline(t *testing.T) {
	r := core.NewRand(1)
	for range 10_000 {
		m := New(50, start, Unit-40_000_000+r.Below(80_000_001))
		m.SetSlew(50, int64(r.Below(452_000_001))-226_000_000)
		at := start.Boot + core.Tick(r.Below(5_000_000))
		w, ok := m.When(60, at)
		if !ok || m.Read(w).Boot < at || w > 60 && m.Read(w-1).Boot >= at {
			t.Fatalf("rate %d slew %d: When(%d) = %d, %v; BOOTTIME there %d", m.Rate(), m.Slew(), at, w, ok, m.Read(w).Boot)
		}
	}
}

func TestASuspendStopsRawAndMonotonicAndBoottimeGainsWholeSeconds(t *testing.T) {
	m := New(0, start, Unit)
	before := m.Read(1_000_000)
	m.Stop(1_000_000, Suspend)
	if _, ok := m.When(1_000_000, start.Boot+5_000_000); ok {
		t.Fatal("a suspended node's timer has a real deadline")
	}
	m.Resume(3_700_000)
	got := m.Read(3_700_000)
	want := core.Clocks{Boot: start.Boot + 3_000_000, Raw: start.Raw + 1_000_000, Mono: start.Mono + 1_000_000, Marker: start.Marker}
	if got != want {
		t.Fatalf("after a 2.7 s suspend: %+v, want %+v", got, want)
	}
	if !core.Resumed(before, got, Threshold) {
		t.Fatal("no Resumed after a 2 s gain")
	}
	m.Stop(4_000_000, Suspend)
	m.Resume(4_900_000)
	if prev, cur := got, m.Read(4_900_000); core.Resumed(prev, cur, Threshold) {
		t.Fatalf("a 0.9 s suspend, rounded to 0, gave Resumed: %+v then %+v", prev, cur)
	}
}

func TestEachStopMovesTheClocksAndMarkerAsMeasured(t *testing.T) {
	const pause = 10_000_000
	for _, c := range []struct {
		stop    Stop
		gain    core.Tick
		marker  uint64
		resumed bool
	}{
		{PauseCounted, pause, 0, false},
		{PauseFrozen, 0, 0, false},
		{Restore, 0, 1, true},
		{HeldMigration, pause, 1, true},
	} {
		m := New(0, start, Unit)
		before := m.Read(1_000_000)
		m.Stop(1_000_000, c.stop)
		m.Resume(1_000_000 + pause)
		after := m.Read(1_000_000 + pause)
		if after.Boot-before.Boot != c.gain || after.Raw-before.Raw != c.gain || after.Mono-before.Mono != c.gain || after.Marker-before.Marker != c.marker {
			t.Errorf("stop %d: %+v then %+v, want every clock to gain %d and the marker %d", c.stop, before, after, c.gain, c.marker)
		}
		if core.Resumed(before, after, Threshold) != c.resumed {
			t.Errorf("stop %d: Resumed %v", c.stop, !c.resumed)
		}
		if d := m.Read(2_000_000 + pause).Raw - after.Raw; d != 1_000_000 {
			t.Errorf("stop %d: raw gained %d in the second after resuming", c.stop, d)
		}
	}
}

func TestThePackageImportsNothingOfTheSimulator(t *testing.T) {
	p, err := build.ImportDir(".", 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, imp := range p.Imports {
		if strings.Contains(imp, "/internal/") && imp != "github.com/nvcnvn/rock-solid/internal/core" {
			t.Errorf("imports %s", imp)
		}
	}
}
