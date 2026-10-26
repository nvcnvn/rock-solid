// Package lease is the arithmetic of node-pair leases: a holder counting each grantor's lease on both its clocks within a margin it bounds from that grantor's raw clock, and a grantor keeping each grant until both its clocks show the whole lease.
package lease

import (
	"cmp"
	"math/bits"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// PPM is one in parts per million, the unit of margins and rate bounds.
const PPM = 1_000_000

// Config is the lease settings.
type Config struct {
	// Lease is primary_lease, Heartbeat how often a grantor beats and a holder renews, RateGap how long after a window's first grant the holder asks again, and Window rate_window.
	Lease     core.Tick
	Heartbeat core.Tick
	RateGap   core.Tick
	Window    core.Tick
	// Drift is lease_drift and MaxMargin max_margin, in parts per million of the lease.
	Drift     uint64
	MaxMargin uint64
}

// DefaultConfig is a 4 s lease, a 1 s heartbeat, a 100 ms rate gap, a 10 s rate window, a lease_drift of 1% and a max_margin of 50%.
var DefaultConfig = Config{Lease: 4_000_000, Heartbeat: 1_000_000, RateGap: 100_000, Window: 10_000_000, Drift: 10_000, MaxMargin: 500_000}

// Time is one reading of a node's two lease clocks: CLOCK_BOOTTIME and CLOCK_MONOTONIC_RAW.
type Time struct {
	Boot core.Tick
	Raw  core.Tick
}

// Now is the time ctx reads.
func Now(ctx *core.Context) Time { return Time{Boot: ctx.Now, Raw: ctx.Raw} }

// Margin is the margin a holder takes on a grantor whose raw clock runs at most bound parts per million of its own: lease_drift widened by the bound's excess over 1 + lease_drift / 2.
func (c *Config) Margin(bound uint64) uint64 {
	return c.Drift + max(bound, PPM+c.Drift/2) - (PPM + c.Drift/2)
}

// windowCap is the grants a window holds without growing: a rate_window of renewals a heartbeat apart, with room for asks after detections.
const windowCap = 32

// sample is a grant in a holder's rate window: its request's send and its arrival on the holder's raw clock, and the grantor's raw reading.
type sample struct {
	s, r, g core.Tick
}

// held is a holder's state for one grantor: the run its window counts, the window, the bound, the last grant, whether that grant gave a lease, and the request outstanding.
type held struct {
	peer    core.NodeID
	run     uint64
	win     []sample
	bound   uint64
	newestG core.Tick
	// last is the last grant: its ask on both clocks, the grantor's raw reading, the margin and support epoch of the lease it gave, if it gave one.
	as, asB, ag core.Tick
	am          uint64
	support     uint64
	gave        bool
	// asked is the outstanding request, sent at askS and askSB, and askAt the boot time of the last ask.
	asked       bool
	askID       core.RequestID
	askS, askSB core.Tick
	askAt       core.Tick
	widened     bool
}

// Holder is a node's leases taken from its grantors.
type Holder struct {
	cfg   Config
	peers []held
	plant plants
}

// plants are bugs a test plants in a holder or a grantor, each the epoch change model's
// counterexample of the same name: the holder ending its lease on the slower of its clocks; a
// lease before the first bound, on lease_drift alone; widening only past 1 + 2 lease_drift; the
// margin max(lease_drift, 1 − 1/B); no new window at a grantor's new run ID; no end of leases on
// Resumed; no step check, or one on grants only, at 1 + lease_drift / 2, or against a grant that
// gave no lease; the grantor ending its grant on the faster of its clocks; no restart wait.
type plants struct {
	slowerClock, beforeBound, widenLate, marginExact, noRunWindow, noResumedEnd bool
	noStepCheck, stepOnGrantsOnly, halfStep, checkBeforeBound                   bool
	fasterClock, noRestartWait                                                  bool
}

// NewHolder holds no lease.
func NewHolder(cfg Config) *Holder { return &Holder{cfg: cfg} }

func (h *Holder) find(g core.NodeID) *held {
	i, ok := slices.BinarySearchFunc(h.peers, g, func(x held, g core.NodeID) int { return compareNode(x.peer, g) })
	if !ok {
		h.peers = slices.Insert(h.peers, i, held{peer: g})
	}
	return &h.peers[i]
}

func compareNode(a, b core.NodeID) int {
	return cmp.Or(cmp.Compare(a.Node, b.Node), cmp.Compare(a.Loop, b.Loop))
}

// Due reports whether the holder should ask grantor g now: at first contact or after a detection, RateGap after a window's first grant, when its lease ended, and a heartbeat after its last ask, renewals included; an ask unanswered for a heartbeat is asked again.
func (h *Holder) Due(g core.NodeID, t Time) bool {
	p := h.find(g)
	if p.asked && t.Boot-p.askAt < h.cfg.Heartbeat {
		return false
	}
	switch {
	case len(p.win) == 0:
		return true
	case p.bound == 0:
		return t.Raw-p.win[0].r >= h.cfg.RateGap
	case !h.valid(p, t) && p.am < h.cfg.MaxMargin:
		return true
	}
	return t.Boot-p.askAt >= h.cfg.Heartbeat
}

// Next is the boot time at which Due may next turn true for g, which a timer on CLOCK_BOOTTIME may reach early: Due then says no and Next names a later time.
func (h *Holder) Next(g core.NodeID, t Time) core.Tick {
	if h.Due(g, t) {
		return t.Boot
	}
	p := h.find(g)
	at := p.askAt + h.cfg.Heartbeat
	if p.asked {
		return max(at, t.Boot)
	}
	if len(p.win) > 0 && p.bound == 0 {
		return t.Boot + sub(h.cfg.RateGap, t.Raw-p.win[0].r)
	}
	if p.gave {
		at = min(at, h.end(p, t))
	}
	return max(at, t.Boot)
}

// Ask records a request to g sent now under id; a grant answers only the latest.
func (h *Holder) Ask(g core.NodeID, id core.RequestID, t Time) {
	p := h.find(g)
	p.asked, p.askID, p.askS, p.askSB, p.askAt = true, id, t.Raw, t.Boot, t.Boot
}

// Grant is a grant as the holder reads it: the request it answers, the grantor's run ID, raw reading and support epoch.
type Grant struct {
	ID      core.RequestID
	Run     uint64
	Raw     core.Tick
	Support uint64
}

// OnGrant takes a grant from g arriving now. A grant from the run the holder counts that shows g's raw clock moved further since the last grant than that grant's lease allows trips the step check, which ends every lease the holder holds; tripped reports it. Otherwise the grant starts a new window at first contact, a new run, a raw reading below the last or once the window's last grant aged out, or joins the window, and gives a lease once the window yields a bound whose margin is below max_margin.
func (h *Holder) OnGrant(g core.NodeID, gr Grant, t Time) (tripped bool) {
	p := h.find(g)
	if !p.asked || gr.ID != p.askID {
		return false
	}
	p.asked = false
	if h.trips(p, gr.Run, gr.Raw, t) {
		h.EndAll()
		return true
	}
	s := sample{s: p.askS, r: t.Raw, g: gr.Raw}
	if p.win == nil {
		p.win = make([]sample, 0, windowCap)
	}
	if len(p.win) == 0 || gr.Run != p.run && !h.plant.noRunWindow || gr.Raw < p.newestG || t.Raw-p.win[len(p.win)-1].r > h.cfg.Window {
		p.win, p.bound, p.run = append(p.win[:0], s), 0, gr.Run
	} else {
		p.win = append(p.win, s)
		drop := 0
		for drop < len(p.win)-2 && t.Raw-p.win[drop].r > h.cfg.Window {
			drop++
		}
		p.win = append(p.win[:0], p.win[drop:]...)
		if b, ok := h.windowBound(p.win); ok {
			p.bound = b
		}
	}
	p.newestG = gr.Raw
	p.as, p.asB, p.ag, p.support = p.askS, p.askSB, gr.Raw, gr.Support
	p.am = h.cfg.Drift
	switch {
	case p.bound == 0:
	case h.plant.widenLate:
		p.am = h.cfg.Drift + max(p.bound, PPM+2*h.cfg.Drift) - (PPM + 2*h.cfg.Drift)
	case h.plant.marginExact:
		p.am = max(h.cfg.Drift, (max(p.bound, PPM)-PPM)*PPM/max(p.bound, PPM))
	default:
		p.am = h.cfg.Margin(p.bound)
	}
	p.widened = p.am > h.cfg.Drift
	p.gave = (p.bound != 0 || h.plant.beforeBound) && p.am < h.cfg.MaxMargin
	return false
}

// OnHeartbeat takes g's heartbeat arriving now, carrying its run ID and raw reading: the step check runs as on a grant, and a new run ID or a raw reading below the last ends the lease taken from g's previous run and its window.
func (h *Holder) OnHeartbeat(g core.NodeID, run uint64, raw core.Tick, t Time) (tripped bool) {
	p := h.find(g)
	if !h.plant.stepOnGrantsOnly && h.trips(p, run, raw, t) {
		h.EndAll()
		return true
	}
	if len(p.win) > 0 && (run != p.run && !h.plant.noRunWindow || raw < p.newestG) {
		p.win, p.bound, p.gave = p.win[:0], 0, false
	}
	p.newestG = max(p.newestG, raw)
	return false
}

// trips is the step check against the last grant from g, run only if it gave a lease and of the same run: g's raw clock moved further since than the lease's margin m allows, g − g_a > (r − s_a) / (1 − m).
func (h *Holder) trips(p *held, run uint64, raw core.Tick, t Time) bool {
	if h.plant.noStepCheck || !p.gave && (!h.plant.checkBeforeBound || len(p.win) == 0) || run != p.run || raw < p.ag {
		return false
	}
	gHi, gLo := bits.Mul64(uint64(raw-p.ag), PPM-p.am)
	rHi, rLo := bits.Mul64(uint64(sub(t.Raw, p.as)), PPM)
	if h.plant.halfStep {
		gHi, gLo = bits.Mul64(uint64(raw-p.ag), PPM)
		rHi, rLo = bits.Mul64(uint64(sub(t.Raw, p.as)), PPM+h.cfg.Drift/2)
	}
	return gHi > rHi || gHi == rHi && gLo > rLo
}

// windowBound pairs the shortest-round-trip grant of the first third of the window's span with that of its last third: g's raw clock ran at most (g_b − g_a) / (s_b − r_a) times as fast as the holder's, in parts per million, rounded up.
func (h *Holder) windowBound(win []sample) (uint64, bool) {
	r0, rn := win[0].r, win[len(win)-1].r
	third := (rn - r0) / 3
	a, b := -1, -1
	for i, x := range win {
		if x.r <= r0+third && (a < 0 || x.r-x.s < win[a].r-win[a].s) {
			a = i
		}
		if x.r >= rn-third && (b < 0 || x.r-x.s < win[b].r-win[b].s) {
			b = i
		}
	}
	if a < 0 || b <= a || win[b].s <= win[a].r {
		return 0, false
	}
	return rateBound(sub(win[b].g, win[a].g), win[b].s-win[a].r), true
}

// rateBound is ⌈PPM × dg / dr⌉, at least 1, saturating far above any margin.
func rateBound(dg, dr core.Tick) uint64 {
	if dg == 0 {
		return 1
	}
	hi, lo := bits.Mul64(uint64(dg), PPM)
	if hi >= uint64(dr) {
		return 1 << 62
	}
	q, rem := bits.Div64(hi, lo, uint64(dr))
	if rem != 0 {
		q++
	}
	return max(q, 1)
}

// Resumed ends every lease, as after a pause: each grantor is asked again at once and its window restarts.
func (h *Holder) Resumed() {
	if !h.plant.noResumedEnd {
		h.EndAll()
	}
}

// EndAll ends every lease and window, so the holder asks each grantor again at once.
func (h *Holder) EndAll() {
	for i := range h.peers {
		p := &h.peers[i]
		p.win, p.bound, p.gave, p.asked, p.newestG = p.win[:0], 0, false, false, 0
	}
}

// Valid reports whether the holder's lease from g runs now: neither clock shows the lease less its margin since the ask that won it.
func (h *Holder) Valid(g core.NodeID, t Time) bool {
	i, ok := slices.BinarySearchFunc(h.peers, g, func(x held, g core.NodeID) int { return compareNode(x.peer, g) })
	return ok && h.valid(&h.peers[i], t)
}

func (h *Holder) valid(p *held, t Time) bool {
	if !p.gave {
		return false
	}
	left := h.left(p)
	if h.plant.slowerClock {
		return sub(t.Raw, p.as) < left || sub(t.Boot, p.asB) < left
	}
	return sub(t.Raw, p.as) < left && sub(t.Boot, p.asB) < left
}

// left is the lease less the margin, rounded down.
func (h *Holder) left(p *held) core.Tick {
	hi, lo := bits.Mul64(uint64(h.cfg.Lease), PPM-p.am)
	q, _ := bits.Div64(hi, lo, PPM)
	return core.Tick(q)
}

// end is the boot time at which the lease from p ends, the earlier of the two clocks' ends read as boot ticks.
func (h *Holder) end(p *held, t Time) core.Tick {
	left := h.left(p)
	return min(p.asB+left, t.Boot+sub(p.as+left, t.Raw))
}

// Margin is the margin of the lease from g's last grant, in parts per million, and whether it widened past lease_drift, which the holder reports with the pair; a margin of max_margin or more gave no lease.
func (h *Holder) Margin(g core.NodeID) (m uint64, widened bool) {
	p := h.find(g)
	return p.am, p.widened
}

// Bound is the rate bound on g of the holder's window, in parts per million, or 0 before it has one.
func (h *Holder) Bound(g core.NodeID) uint64 { return h.find(g).bound }

// Support is the support epoch of the last grant from g, and whether it gave a lease.
func (h *Holder) Support(g core.NodeID) (uint64, bool) {
	p := h.find(g)
	return p.support, p.gave
}

// granted is a grantor's state for one holder: the support epoch of the pair's current run of grants and when the last grant was given on both clocks.
type granted struct {
	peer    core.NodeID
	support uint64
	on      bool
	at      Time
}

// Grantor is a node's grants to its holders, its run ID and its restart wait.
type Grantor struct {
	cfg     Config
	run     uint64
	start   Time
	holders []granted
	plant   plants
}

// NewGrantor is a grantor that starts now, drawing run as its run ID; it forgot every grant an earlier run gave, so it grants nothing until both clocks show one lease.
func NewGrantor(cfg Config, run uint64, t Time) *Grantor {
	return &Grantor{cfg: cfg, run: run, start: t}
}

// Run is the run ID every grant and heartbeat carries.
func (g *Grantor) Run() uint64 { return g.run }

// Resumed takes the run ID the grantor draws after a pause, so no holder's rate bound spans it; grants and waits stand, since a clock that stopped only makes them longer.
func (g *Grantor) Resumed(run uint64) { g.run = run }

// Waiting reports whether the restart wait runs: either clock shows less than one lease since the start, during which the node grants nothing and joins no peering.
func (g *Grantor) Waiting(t Time) bool {
	return !g.plant.noRestartWait && (sub(t.Raw, g.start.Raw) < g.cfg.Lease || sub(t.Boot, g.start.Boot) < g.cfg.Lease)
}

// WaitEnd is the boot time at which the restart wait may end, which a timer on CLOCK_BOOTTIME may reach early.
func (g *Grantor) WaitEnd(t Time) core.Tick {
	return max(g.start.Boot+g.cfg.Lease, t.Boot+sub(g.start.Raw+g.cfg.Lease, t.Raw))
}

func (g *Grantor) find(h core.NodeID) *granted {
	i, ok := slices.BinarySearchFunc(g.holders, h, func(x granted, h core.NodeID) int { return compareNode(x.peer, h) })
	if !ok {
		g.holders = slices.Insert(g.holders, i, granted{peer: h})
	}
	return &g.holders[i]
}

// Grant grants or renews h's lease now if h reported a table at least as new as the grantor's and the restart wait is over, returning the support epoch: the pair's current one while the last grant still runs, else the next, so a grant that lapsed is never renewed.
func (g *Grantor) Grant(h core.NodeID, holderEpoch, ownEpoch uint64, t Time) (support uint64, ok bool) {
	if holderEpoch < ownEpoch || g.Waiting(t) {
		return 0, false
	}
	x := g.find(h)
	if !g.live(x, t) {
		x.support++
	}
	x.on, x.at = true, t
	return x.support, true
}

// Live reports whether h's grant still runs now: until both clocks show the whole lease since it was given.
func (g *Grantor) Live(h core.NodeID, t Time) bool {
	i, ok := slices.BinarySearchFunc(g.holders, h, func(x granted, h core.NodeID) int { return compareNode(x.peer, h) })
	return ok && g.live(&g.holders[i], t)
}

func (g *Grantor) live(x *granted, t Time) bool {
	if g.plant.fasterClock {
		return x.on && sub(t.Raw, x.at.Raw) < g.cfg.Lease && sub(t.Boot, x.at.Boot) < g.cfg.Lease
	}
	return x.on && (sub(t.Raw, x.at.Raw) < g.cfg.Lease || sub(t.Boot, x.at.Boot) < g.cfg.Lease)
}

// Until is the boot time at which h's grant may end, which a timer on CLOCK_BOOTTIME may reach early: Live then still holds, and the grantor sets another for the remainder.
func (g *Grantor) Until(h core.NodeID, t Time) core.Tick {
	x := g.find(h)
	return max(x.at.Boot+g.cfg.Lease, t.Boot+sub(x.at.Raw+g.cfg.Lease, t.Raw))
}

// Granting appends to dst the holders whose grants run now, which the grantor heartbeats.
func (g *Grantor) Granting(dst []core.NodeID, t Time) []core.NodeID {
	for i := range g.holders {
		if g.live(&g.holders[i], t) {
			dst = append(dst, g.holders[i].peer)
		}
	}
	return dst
}

// sub is a − b, or 0 when b is later.
func sub(a, b core.Tick) core.Tick {
	if a < b {
		return 0
	}
	return a - b
}
