package lease

import (
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

var cfg = DefaultConfig

var grantor = core.NodeID{Node: 2}

// clocks are a node's two clocks as linear functions of real time, their rates in parts per billion.
type clocks struct {
	boot, raw         int64
	bootRate, rawRate int64
}

func (c clocks) at(real core.Tick) Time {
	return Time{Boot: core.Tick(c.boot + int64(real)*c.bootRate/1e9), Raw: core.Tick(c.raw + int64(real)*c.rawRate/1e9)}
}

func rate(ppb int64) int64 { return 1e9 + ppb }

// round is a request sent at real time ask and granted at real time ask + d, arriving at ask + 2d.
func round(h *Holder, gr *Grantor, hc, gc clocks, ask, d core.Tick, id core.RequestID) bool {
	h.Ask(grantor, id, hc.at(ask))
	sup, ok := gr.Grant(core.NodeID{Node: 1}, 1, 1, gc.at(ask+d))
	if !ok {
		return false
	}
	return h.OnGrant(grantor, Grant{ID: id, Run: gr.Run(), Raw: gc.at(ask + d).Raw, Support: sup}, hc.at(ask+2*d))
}

// lastReal is the last real time, in µs, at which ok holds, ok holding at from and failing by to.
func lastReal(from, to core.Tick, ok func(core.Tick) bool) core.Tick {
	for to-from > 1 {
		mid := (from + to) / 2
		if ok(mid) {
			from = mid
		} else {
			to = mid
		}
	}
	return from
}

// Each side counts the lease on both clocks, the holder ending it lease_drift early on the faster and the grantor keeping it until the slower shows the whole lease, so a slew or a bad calibration only shortens the holder's lease or lengthens the grantor's wait.
func TestLeaseTime(t *testing.T) {
	const slew, chrony, osc = 226_000_000, 83_333_333, 100_000
	for _, tc := range []struct {
		name                     string
		hBoot, hRaw, gBoot, gRaw int64
		holderEnd, grantorEnd    core.Tick
	}{
		{"exact", 0, 0, 0, 0, 3_960_000, 4_000_500},
		{"holder boot fast", slew, 0, 0, 0, 3_230_000, 4_000_500},
		{"holder boot slow", -slew, 0, 0, 0, 3_960_000, 4_000_500},
		{"grantor boot fast", 0, 0, slew, 0, 3_960_000, 4_000_500},
		{"grantor boot slow", 0, 0, -slew, 0, 3_960_000, 5_168_000},
		{"opposite slews", -slew, 0, slew, 0, 3_960_000, 4_000_500},
		{"chrony", -chrony, 0, chrony, 0, 3_960_000, 4_000_500},
		{"raw 1% apart, margin widened to 2.52%", 0, -10_000_000, 0, 10_000_000, 3_899_000, 4_000_500},
		{"oscillators", -osc, -osc, osc, osc, 3_960_000, 4_000_000},
		{"no daemon, raw 1% apart, full window", -10_000_000, -10_000_000, 10_000_000, 10_000_000, 3_938_000, 3_961_000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hc := clocks{boot: 5e9, raw: 7e9, bootRate: rate(tc.hBoot), rawRate: rate(tc.hRaw)}
			gc := clocks{boot: 11e9, raw: 13e9, bootRate: rate(tc.gBoot), rawRate: rate(tc.gRaw)}
			const base = 30_000_000
			h, g := NewHolder(cfg), NewGrantor(cfg, 1, gc.at(0))
			id := core.RequestID(0)
			for at := core.Tick(base - 10_000_000); at < base; at += 1_000_000 {
				id++
				round(h, g, hc, gc, at, 500, id)
			}
			id++
			round(h, g, hc, gc, base, 500, id)
			if !h.Valid(grantor, hc.at(base+1000)) {
				t.Fatal("no lease after a full window")
			}
			he := lastReal(base, base+10_000_000, func(r core.Tick) bool { return h.Valid(grantor, hc.at(r)) }) - base
			ge := lastReal(base, base+10_000_000, func(r core.Tick) bool { return g.Live(core.NodeID{Node: 1}, gc.at(r)) }) - base
			if !near(he+1, tc.holderEnd) || !near(ge+1, tc.grantorEnd) {
				t.Fatalf("holder ends at %d µs, grantor at %d; want %d and %d", he+1, ge+1, tc.holderEnd, tc.grantorEnd)
			}
			if he >= ge {
				t.Fatal("the holder's lease outlasts the grant")
			}
		})
	}
}

// near reports whether a and b are within a millisecond, the tables' precision.
func near(a, b core.Tick) bool { return max(a, b)-min(a, b) <= 1000 }

// grants feeds h the window's grants from g as (s, r, g) triples on the holder's raw clock and the grantor's, the grantor's run 1.
func grants(h *Holder, triples ...[3]core.Tick) {
	for i, x := range triples {
		h.Ask(grantor, core.RequestID(i+1), Time{Boot: x[0], Raw: x[0]})
		h.OnGrant(grantor, Grant{ID: core.RequestID(i + 1), Run: 1, Raw: x[2], Support: 1}, Time{Boot: x[1], Raw: x[1]})
	}
}

// The holder pairs the shortest-round-trip grants of the first and last thirds of its window, widens its margin by the bound's excess over 1 + lease_drift / 2 and takes no lease at max_margin.
func TestRateBound(t *testing.T) {
	const g0 = 50_000_000
	for _, tc := range []struct {
		name   string
		win    [][3]core.Tick
		bound  uint64
		margin uint64
		lease  bool
	}{
		{"exact, 1 ms", [][3]core.Tick{{0, 1000, g0}, {3_333_333, 3_334_333, g0 + 3_334_333}}, 1_000_601, 10_000, true},
		{"exact, 100 µs", [][3]core.Tick{{0, 100, g0}, {3_333_333, 3_333_433, g0 + 3_333_433}}, 1_000_061, 10_000, true},
		{"1% fast", [][3]core.Tick{{0, 100, g0 + 50}, {3_333_333, 3_333_433, g0 + 50 + 3_366_666}}, 1_010_031, 15_031, true},
		{"0.6% fast", [][3]core.Tick{{0, 100, g0}, {3_333_333, 3_333_433, g0 + 3_353_333}}, 1_006_031, 11_031, true},
		{"0.4% fast", [][3]core.Tick{{0, 100, g0}, {3_333_333, 3_333_433, g0 + 3_346_666}}, 1_004_031, 10_000, true},
		{"1% slow", [][3]core.Tick{{0, 100, g0}, {3_333_333, 3_333_433, g0 + 3_300_000}}, 990_030, 10_000, true},
		{"slow first third", [][3]core.Tick{{0, 50_000, g0}, {3_333_333, 3_333_433, g0 + 3_333_433}}, 1_015_259, 20_259, true},
		{"one slow grant in the first third", [][3]core.Tick{{0, 50_000, g0 - 9_000}, {100_000, 100_100, g0 + 100_100}, {3_333_333, 3_333_433, g0 + 3_333_433}}, 1_000_031, 10_000, true},
		{"6 s span", [][3]core.Tick{{0, 100, g0}, {4_000_000, 4_000_100, g0 + 4_000_100}, {6_000_000, 6_000_100, g0 + 6_000_100}}, 1_000_051, 10_000, true},
		{"second grant rate_gap after the first, 100 µs", [][3]core.Tick{{0, 100, g0}, {100_100, 100_200, g0 + 100_200}}, 1_002_000, 10_000, true},
		{"second grant rate_gap after the first, 1 ms", [][3]core.Tick{{0, 1000, g0}, {101_000, 102_000, g0 + 102_000}}, 1_020_000, 25_000, true},
		{"50% fast", [][3]core.Tick{{0, 100, g0}, {1_000_100, 1_000_200, g0 + 1_505_000}}, 1_505_000, 510_000, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHolder(cfg)
			grants(h, tc.win...)
			last := tc.win[len(tc.win)-1]
			m, widened := h.Margin(grantor)
			if b := h.Bound(grantor); b != tc.bound || m != tc.margin || widened != (tc.margin > cfg.Drift) {
				t.Fatalf("bound %d margin %d widened %v, want %d and %d", b, m, widened, tc.bound, tc.margin)
			}
			if got := h.Valid(grantor, Time{Boot: last[1], Raw: last[1]}); got != tc.lease {
				t.Fatalf("lease %v, want %v", got, tc.lease)
			}
		})
	}
}

// A window's first grant gives no lease; the holder asks again rate_gap after it arrives, and its second grant gives the first lease.
func TestTheFirstGrantOfAWindowGivesNoLease(t *testing.T) {
	h := NewHolder(cfg)
	if !h.Due(grantor, Time{}) {
		t.Fatal("first contact is not due")
	}
	grants(h, [3]core.Tick{0, 100, 50_000_000})
	if h.Valid(grantor, Time{Boot: 100, Raw: 100}) {
		t.Fatal("a lease before the first bound")
	}
	if h.Due(grantor, Time{Boot: 100_099, Raw: 100_099}) || !h.Due(grantor, Time{Boot: 100_100, Raw: 100_100}) {
		t.Fatal("the second ask is not due exactly rate_gap after the first grant arrived")
	}
	if n := h.Next(grantor, Time{Boot: 100, Raw: 100}); n != 100_100 {
		t.Fatalf("next ask at %d", n)
	}
}

// A grant or heartbeat from a grantor's new run, or with a raw reading below the last, ends the lease taken from its previous run and starts a new window.
func TestANewRunEndsTheOldRunsLease(t *testing.T) {
	for _, viaHeartbeat := range []bool{false, true} {
		for _, newRun := range []bool{false, true} {
			h := NewHolder(cfg)
			grants(h, [3]core.Tick{0, 100, 50_000_000}, [3]core.Tick{100_100, 100_200, 50_100_200})
			at := Time{Boot: 200_000, Raw: 200_000}
			if !h.Valid(grantor, at) {
				t.Fatal("no lease")
			}
			run, raw := uint64(1), core.Tick(1_200)
			if newRun {
				run, raw = 2, 50_200_000
			}
			if viaHeartbeat {
				if h.OnHeartbeat(grantor, run, raw, at) {
					t.Fatal("a new run tripped the step check")
				}
			} else {
				h.Ask(grantor, 9, at)
				h.OnGrant(grantor, Grant{ID: 9, Run: run, Raw: raw, Support: 1}, at)
			}
			if h.Valid(grantor, at) || h.Bound(grantor) != 0 {
				t.Fatalf("heartbeat %v new run %v: the old run's lease or bound survives", viaHeartbeat, newRun)
			}
		}
	}
}

// pauseHolder holds a lease of margin m from its last grant a: s_a = 1,000,000, r_a = 1,001,000, g_a = 50,000,500.
func pauseHolder(t *testing.T, ratePPM uint64) *Holder {
	h := NewHolder(cfg)
	dr := core.Tick(1_000_000 - 901_000)
	grants(h, [3]core.Tick{900_000, 901_000, 50_000_500 - core.Tick(uint64(dr)*ratePPM/PPM)}, [3]core.Tick{1_000_000, 1_001_000, 50_000_500})
	if !h.Valid(grantor, Time{Boot: 1_001_000, Raw: 1_001_000}) {
		t.Fatal("no lease")
	}
	return h
}

// A heartbeat whose raw reading moved further since the last grant than that lease's margin allows ends every lease.
func TestTheStepCheck(t *testing.T) {
	for _, tc := range []struct {
		name  string
		rate  uint64
		r, g  core.Tick
		trips bool
	}{
		{"steady", PPM, 2_001_000, 51_000_500, false},
		{"at the limit", PPM, 2_001_000, 51_011_611, false},
		{"past it", PPM, 2_001_000, 51_011_612, true},
		{"2% fast with its 2.5% margin", 1_020_000, 2_001_000, 51_020_500, false},
		{"holder frozen 10 s", PPM, 2_001_000, 61_000_500, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := pauseHolder(t, tc.rate)
			other := core.NodeID{Node: 3}
			h.Ask(other, 1, Time{Boot: 900_000, Raw: 900_000})
			at := Time{Boot: tc.r, Raw: tc.r}
			if got := h.OnHeartbeat(grantor, 1, tc.g, at); got != tc.trips {
				t.Fatalf("tripped %v", got)
			}
			if h.Valid(grantor, at) == tc.trips {
				t.Fatal("the lease outlived the trip, or a trip that did not happen ended it")
			}
			if tc.trips && (!h.Due(grantor, at) || !h.Due(other, at) || h.Bound(grantor) != 0) {
				t.Fatal("after a trip the holder does not ask every grantor again at once with a new window")
			}
		})
	}
}

// The step check runs only against a grant that gave a lease: a window's first grant gave none, so a grantor 4% fast forms its bound on the second.
func TestTheStepCheckSkipsAGrantThatGaveNoLease(t *testing.T) {
	h := NewHolder(cfg)
	grants(h, [3]core.Tick{1_000_000, 1_001_000, 50_000_500})
	h.Ask(grantor, 7, Time{Boot: 1_100_900, Raw: 1_100_900})
	if h.OnGrant(grantor, Grant{ID: 7, Run: 1, Raw: 50_104_500, Support: 1}, Time{Boot: 1_101_000, Raw: 1_101_000}) {
		t.Fatal("tripped on a window's second grant")
	}
	if !h.Valid(grantor, Time{Boot: 1_101_000, Raw: 1_101_000}) {
		t.Fatal("no lease from the second grant")
	}
}

// Resumed ends every lease a holder holds, and a grantor keeps its grants and its restart wait, drawing only a new run ID.
func TestResumed(t *testing.T) {
	h := pauseHolder(t, PPM)
	h.Resumed()
	at := Time{Boot: 1_001_000, Raw: 1_001_000}
	if h.Valid(grantor, at) || !h.Due(grantor, at) {
		t.Fatal("a lease outlived Resumed, or the holder does not ask again at once")
	}
	g := NewGrantor(cfg, 1, Time{})
	if _, ok := g.Grant(grantor, 1, 1, Time{Boot: 5_000_000, Raw: 5_000_000}); !ok {
		t.Fatal("no grant after the restart wait")
	}
	g.Resumed(2)
	if g.Run() != 2 || !g.Live(grantor, Time{Boot: 6_000_000, Raw: 6_000_000}) {
		t.Fatal("Resumed changed the grantor's grants or kept its run ID")
	}
}

// A grant that lapsed on both of the grantor's clocks is never renewed: the next grant takes a new support epoch, whatever request it answers.
func TestSupportEpochs(t *testing.T) {
	g := NewGrantor(cfg, 1, Time{})
	at := func(s core.Tick) Time { return Time{Boot: s * 1_000_000, Raw: s * 1_000_000} }
	s1, _ := g.Grant(grantor, 5, 5, at(10))
	s2, _ := g.Grant(grantor, 5, 5, at(12))
	if s1 != s2 {
		t.Fatalf("a renewal changed the support epoch, %d to %d", s1, s2)
	}
	if !g.Live(grantor, at(15)) || g.Live(grantor, at(16)) {
		t.Fatal("the grant does not end once both clocks show the lease")
	}
	if s3, _ := g.Grant(grantor, 5, 5, at(26)); s3 != s2+1 {
		t.Fatalf("a grant after a lapse took support epoch %d, want %d", s3, s2+1)
	}
	if _, ok := g.Grant(grantor, 4, 5, at(27)); ok {
		t.Fatal("a renewal for a holder that reported an older table")
	}
	if _, ok := g.Grant(core.NodeID{Node: 9}, 4, 5, at(27)); ok {
		t.Fatal("a fresh grant for a holder that reported an older table")
	}
}

// A node that starts grants nothing and joins no peering until both its clocks show one lease, however fast either runs.
func TestTheRestartWait(t *testing.T) {
	start := Time{Boot: 100_000_000, Raw: 77_000_000}
	g := NewGrantor(cfg, 1, start)
	fastBoot := func(real core.Tick) Time {
		return Time{Boot: start.Boot + real*101/100, Raw: start.Raw + real}
	}
	if !g.Waiting(fastBoot(3_999_999)) || g.Waiting(fastBoot(4_000_000)) {
		t.Fatal("the wait does not end when the slower clock shows the lease")
	}
	if _, ok := g.Grant(grantor, 1, 1, fastBoot(3_999_999)); ok {
		t.Fatal("a grant during the restart wait")
	}
	if end := g.WaitEnd(fastBoot(0)); end != start.Boot+4_000_000 {
		t.Fatalf("wait end %d", end)
	}
}

// A grantor's wait is a timer on CLOCK_BOOTTIME; one that fires while the raw clock shows less than the lease finds the grant live and names the remainder.
func TestUntilSetsAnotherTimerForTheRemainder(t *testing.T) {
	g := NewGrantor(cfg, 1, Time{})
	g.Grant(grantor, 1, 1, Time{Boot: 10_000_000, Raw: 10_000_000})
	fired := Time{Boot: 14_000_000, Raw: 13_900_000}
	if !g.Live(grantor, fired) {
		t.Fatal("the grant ended before the raw clock showed the lease")
	}
	if u := g.Until(grantor, fired); u != 14_100_000 {
		t.Fatalf("next timer at %d", u)
	}
}

// Random clock rates per node, random renewal timing and requests lost for up to 8 s at a time: no holder's lease ever ends after its grantor's, with BOOTTIME slewed up to 22.6% and raw rates within 100 ppm, or raw within 4% and BOOTTIME slewed or at the raw rate, from the first lease.
func TestNoLeaseOutlastsItsGrant(t *testing.T) {
	seeds.Run(t, 0, 100, func(seed uint64) {
		rng := core.NewRand(seed)
		pick := func(lim int64) int64 { return int64(rng.Below(uint64(2*lim+1))) - lim }
		rawLim := int64(100_000)
		if rng.Chance(1, 2) {
			rawLim = 40_000_000
		}
		node := func() clocks {
			c := clocks{boot: int64(rng.Below(1e12)), raw: int64(rng.Below(1e12)), rawRate: rate(pick(rawLim))}
			c.bootRate = c.rawRate + pick(226_000_000)
			if rng.Chance(1, 3) {
				c.bootRate = c.rawRate
			}
			return c
		}
		hc, gc := node(), node()
		h, g := NewHolder(cfg), NewGrantor(cfg, rng.Uint64(), gc.at(0))
		holder := core.NodeID{Node: 1}
		real := core.Tick(5_000_000)
		id := core.RequestID(0)
		var cutUntil core.Tick
		for real < 60_000_000 {
			if real >= cutUntil && rng.Chance(1, 2000) {
				cutUntil = real + core.Tick(rng.Below(8_000_000))
			}
			ht := hc.at(real)
			if h.Valid(grantor, ht) && !g.Live(holder, gc.at(real)) {
				t.Fatalf("seed %d: at real %d µs the holder holds a lease its grantor no longer keeps", seed, real)
			}
			if h.Due(grantor, ht) && rng.Chance(9, 10) {
				id++
				h.Ask(grantor, id, ht)
				d1, d2 := core.Tick(rng.Below(20_000)), core.Tick(rng.Below(20_000))
				if real < cutUntil {
					real += core.Tick(1 + rng.Below(10_000))
					continue
				}
				sup, ok := g.Grant(holder, 1, 1, gc.at(real+d1))
				if ok && rng.Chance(19, 20) {
					h.OnGrant(grantor, Grant{ID: id, Run: g.Run(), Raw: gc.at(real + d1).Raw, Support: sup}, hc.at(real+d1+d2))
				}
				for at := real; at < real+d1+d2; at += 1_000 {
					if h.Valid(grantor, hc.at(at)) && !g.Live(holder, gc.at(at)) {
						t.Fatalf("seed %d: at real %d µs, inside a round trip, the holder outlasts its grant", seed, at)
					}
				}
				real += d1 + d2
			}
			real += core.Tick(1 + rng.Below(10_000))
		}
	})
}
