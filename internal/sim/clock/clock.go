// Package clock models one node's clocks as functions of real time, for the simulator and the event loop's stepped tests; it imports nothing of the simulator.
package clock

import (
	"math/bits"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// Unit is the rate of a clock that keeps real time, in parts per billion.
const Unit = 1_000_000_000

// Threshold is the gain of CLOCK_BOOTTIME over CLOCK_MONOTONIC that the simulator counts as a suspend.
const Threshold core.Tick = 1_000_000

const second core.Tick = 1_000_000

// Stop is how a node's clocks behave while it stands still.
type Stop uint8

// Stops, as the VM pause spike measured them.
const (
	Running Stop = iota
	// Suspend stops CLOCK_MONOTONIC_RAW and CLOCK_MONOTONIC; CLOCK_BOOTTIME gains the sleep rounded down to whole seconds.
	Suspend
	// PauseCounted is a stop in place that every clock counts: a guest on the TSC clocksource, or any Firecracker pause.
	PauseCounted
	// PauseFrozen is a stop in place on a QEMU guest on kvm-clock: every clock frozen, the marker unchanged.
	PauseFrozen
	// Restore is a snapshot restore or a live migration: every clock frozen, the marker changed.
	Restore
	// HeldMigration is a QEMU migration held at switchover, seen by a kvm-clock guest: every clock counts the hold, the marker changed.
	HeldMigration
)

// Model is one node's clocks, piecewise linear in real time: CLOCK_MONOTONIC_RAW at its rate, CLOCK_BOOTTIME and CLOCK_MONOTONIC at that rate slewed by the time daemon.
type Model struct {
	at    core.Tick
	base  core.Clocks
	raw   uint64
	slew  int64
	stop  Stop
	since core.Tick
}

// New starts clocks reading start at real time at, the raw clock running at raw parts per billion of real time.
func New(at core.Tick, start core.Clocks, raw uint64) *Model {
	return &Model{at: at, base: start, raw: raw}
}

// Read is the clocks at real time t, no earlier than the last change.
func (m *Model) Read(t core.Tick) core.Clocks {
	c := m.base
	if m.frozen() {
		return c
	}
	d := uint64(t - m.at)
	c.Raw += core.Tick(scale(d, m.raw))
	b := core.Tick(scale(d, m.boot()))
	c.Boot += b
	c.Mono += b
	return c
}

// When is the earliest real time from t at which CLOCK_BOOTTIME reads at least boot if nothing changes; false while the clocks stand still.
func (m *Model) When(t, boot core.Tick) (core.Tick, bool) {
	if m.frozen() {
		return 0, false
	}
	if boot <= m.base.Boot {
		return t, true
	}
	r := m.boot()
	hi, lo := bits.Mul64(uint64(boot-m.base.Boot), Unit)
	lo, carry := bits.Add64(lo, r-1, 0)
	d, _ := bits.Div64(hi+carry, lo, r)
	return max(t, m.at+core.Tick(d)), true
}

// Rate is the raw clock's rate in parts per billion of real time.
func (m *Model) Rate() uint64 { return m.raw }

// Slew is how far the time daemon sets CLOCK_BOOTTIME's rate from the raw clock's, in parts per billion.
func (m *Model) Slew() int64 { return m.slew }

// SetRate runs the raw clock at raw parts per billion of real time from t.
func (m *Model) SetRate(t core.Tick, raw uint64) {
	m.rebase(t)
	m.raw = raw
}

// SetSlew sets CLOCK_BOOTTIME's and CLOCK_MONOTONIC's rate slew parts per billion away from the raw clock's from t.
func (m *Model) SetSlew(t core.Tick, slew int64) {
	m.rebase(t)
	m.slew = slew
}

// Stop stands the node still from t, its clocks as s says, until Resume.
func (m *Model) Stop(t core.Tick, s Stop) {
	m.rebase(t)
	m.stop, m.since = s, t
}

// Resume ends the stop at t.
func (m *Model) Resume(t core.Tick) {
	switch m.stop {
	case Suspend:
		m.base.Boot += (t - m.since) / second * second
	case Restore, HeldMigration:
		m.base.Marker++
	}
	if m.frozen() {
		m.at = t
	}
	m.stop = Running
}

func (m *Model) frozen() bool {
	return m.stop == Suspend || m.stop == PauseFrozen || m.stop == Restore
}

func (m *Model) rebase(t core.Tick) {
	m.base = m.Read(t)
	m.at = t
}

func (m *Model) boot() uint64 { return uint64(int64(m.raw) + int64(m.raw)*m.slew/Unit) }

func scale(d, rate uint64) uint64 {
	hi, lo := bits.Mul64(d, rate)
	q, _ := bits.Div64(hi, lo, Unit)
	return q
}
