package looptest

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/loop"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/clock"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
	"github.com/nvcnvn/rock-solid/internal/trace"
)

const second core.Tick = 1_000_000

type seen struct {
	kind     core.EventKind
	timer    core.TimerID
	now, raw core.Tick
}

// recorder records every event with the clocks it came at; Timer 0, posted to the inbox, arms
// the timers of arm at their At after Now.
type recorder struct {
	seen []seen
	arm  []core.Action
}

func (m *recorder) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	m.seen = append(m.seen, seen{ev.Kind, ev.Timer, ctx.Now, ctx.Raw})
	if ev.Kind == core.EvTimer && ev.Timer == 0 {
		for _, a := range m.arm {
			a.At += ctx.Now
			out = append(out, a)
		}
	}
	return out
}

func (m *recorder) since(i int) string {
	var b []byte
	for _, s := range m.seen[i:] {
		switch s.kind {
		case core.EvResumed:
			b = append(b, "Resumed "...)
		case core.EvTimer:
			b = fmt.Appendf(b, "Timer%d ", s.timer)
		case core.EvNetRecv:
			b = append(b, "NetRecv "...)
		default:
			b = fmt.Appendf(b, "kind%d ", s.kind)
		}
	}
	return string(b)
}

func timer(id core.TimerID, after core.Tick) core.Action {
	return core.Action{Kind: core.ActSetTimer, Timer: id, At: after}
}

// clocked is a stepped loop with a client and a peer connection on a node of its own whose raw
// clock keeps real time and whose clocks start at start; seam, when set, wraps the kernel.
func clocked(t *testing.T, start sim.Clock, m loop.Machine, seam func(*Kernel) loop.Kernel) (*sim.Scheduler, *Kernel, *loop.Loop, *trace.Ring, []int) {
	t.Helper()
	s := sim.New(1, sim.Settings{PoolBytes: 1 << 20})
	k := NewKernel(s, s.AddNode(start, nil, func(*sim.Node) {}).ID())
	var sk loop.Kernel = k
	if seam != nil {
		sk = seam(k)
	}
	l, ring, ends := clientAndPeerOver(t, k, sk, 0, m, nil)
	return s, k, l, ring, ends
}

func TestSteppedPausesDeliverResumedAsTheVMPauseTableSays(t *testing.T) {
	for _, c := range []struct {
		name       string
		stop       clock.Stop
		d          core.Tick
		times      int
		resumed    bool
		gain, raw  core.Tick
		timersFire []core.TimerID
	}{
		{"suspend to RAM", clock.Suspend, 2*second + 300_000, 1, true, 2 * second, 0, []core.TimerID{2, 3, 1}},
		{"suspend rounded below the threshold", clock.Suspend, 800_000, 1, false, 0, 0, nil},
		{"snapshot restore or live migration", clock.Restore, 10 * second, 1, true, 0, 0, nil},
		{"one snapshot restored twice", clock.Restore, 10 * second, 2, true, 0, 0, nil},
		{"migration held 10 s at switchover", clock.HeldMigration, 10 * second, 1, true, 10 * second, 10 * second, []core.TimerID{2, 3, 1}},
		{"stop and cont on kvm-clock", clock.PauseFrozen, 10 * second, 1, false, 0, 0, nil},
		{"stop on tsc, Firecracker pause or SIGSTOP", clock.PauseCounted, 10 * second, 1, false, 10 * second, 10 * second, []core.TimerID{2, 3, 1}},
	} {
		t.Run(c.name, func(t *testing.T) {
			m := &recorder{arm: []core.Action{timer(1, 1_500_000), timer(2, 500_000), timer(3, second)}}
			s, k, l, ring, ends := clocked(t, sim.DefaultClock, m, nil)
			l.Inbox().Post(&core.Event{Kind: core.EvTimer})
			l.Pass()
			for range c.times {
				was := *l.Context()
				s.Pause(k.node, c.stop, c.d, false)
				sendLast(t, ends[0])
				sendLast(t, ends[1])
				if !s.Step() || s.Node(k.node).Paused() {
					t.Fatal("the node did not resume")
				}
				from := len(m.seen)
				l.Pass()
				var want []string
				if c.resumed {
					want = append(want, "Resumed")
				}
				for _, id := range c.timersFire {
					want = append(want, fmt.Sprintf("Timer%d", id))
				}
				want = append(want, "NetRecv", "NetRecv")
				if got := m.since(from); got != strings.Join(want, " ")+" " {
					t.Fatalf("pass after the stop handled %s; want %v\n%s", got, want, ring.Dump(nil))
				}
				now := l.Context()
				if now.Now-was.Now != c.gain || now.Raw-was.Raw != c.raw {
					t.Fatalf("Now gained %d and Raw %d; want %d and %d", now.Now-was.Now, now.Raw-was.Raw, c.gain, c.raw)
				}
				c.timersFire = nil
			}
		})
	}
}

// back reads the stepped clocks by ticks behind.
type back struct {
	*Kernel
	by core.Tick
}

func (b *back) Clocks() core.Clocks {
	c := b.Kernel.Clocks()
	c.Boot -= b.by
	c.Raw -= b.by
	c.Mono -= b.by
	return c
}

func TestSteppedClocksThatGoBackHoldAndDeliverResumed(t *testing.T) {
	var bk *back
	m := &recorder{}
	s, _, l, _, _ := clocked(t, sim.Clock{Now: 100 * second, Raw: 200 * second}, m, func(k *Kernel) loop.Kernel {
		bk = &back{Kernel: k}
		return bk
	})
	l.Pass()
	first := *l.Context()
	bk.by = 500_000
	s.Advance(s.Time() + 100_000)
	l.Pass()
	if c := l.Context(); c.Now != first.Now || c.Raw != first.Raw {
		t.Fatalf("clocks read 400 ms back gave Now %d and Raw %d, after %d and %d", c.Now, c.Raw, first.Now, first.Raw)
	}
	if got := m.since(0); got != "Resumed " {
		t.Fatalf("clocks read 400 ms back gave %s; want one Resumed", got)
	}
	s.Advance(s.Time() + 600_000)
	l.Pass()
	if c := l.Context(); c.Now != first.Now+200_000 || c.Raw != first.Raw+200_000 {
		t.Fatalf("clocks 200 ms ahead again gave Now %d and Raw %d, after %d and %d", c.Now, c.Raw, first.Now, first.Raw)
	}
	if got := m.since(0); got != "Resumed " {
		t.Fatalf("clocks going forward again gave %s; want no second Resumed", got)
	}
}

func TestSteppedSlewMovesNowAgainstRaw(t *testing.T) {
	m := &recorder{arm: []core.Action{timer(1, second)}}
	s, k, l, _, _ := clocked(t, sim.DefaultClock, m, nil)
	node := s.Node(k.node)
	node.Clock().SetSlew(s.Time(), -clock.Unit/10)
	Add(node, k, l)
	l.Inbox().Post(&core.Event{Kind: core.EvTimer})
	for len(m.seen) < 2 && s.Step() {
	}
	if len(m.seen) != 2 || m.seen[1].timer != 1 {
		t.Fatalf("events %s; want the kick and timer 1", m.since(0))
	}
	kick, fired := m.seen[0], m.seen[1]
	if now, raw := fired.now-kick.now, fired.raw-kick.raw; now < second || raw < 1_111_111 || raw > 1_111_112 {
		t.Fatalf("a 1 s timer under a -10%% slew fired after %d of Now and %d of Raw; want 1 s and 1.111 s", now, raw)
	}
}

// deaf never reports the timerfd.
type deaf struct{ *Kernel }

func (d deaf) Wait(ready []loop.Ready, block bool) []loop.Ready {
	return slices.DeleteFunc(d.Kernel.Wait(ready, block), func(r loop.Ready) bool { return r.Token == loop.TokenTimer })
}

func TestSteppedTimersFireFromTheHeapNotTheTimerfd(t *testing.T) {
	m := &recorder{arm: []core.Action{timer(1, 10_000)}}
	s, _, l, _, _ := clocked(t, sim.DefaultClock, m, func(k *Kernel) loop.Kernel { return deaf{k} })
	l.Inbox().Post(&core.Event{Kind: core.EvTimer})
	l.Pass()
	s.Advance(s.Time() + 10_000)
	l.Pass()
	if got := m.since(0); got != "Timer0 Timer1 " {
		t.Fatalf("events %s; want timer 1 fired with no timerfd reported", got)
	}
}

// arms counts the loop's Arm calls and keeps the last deadline armed.
type arms struct {
	*Kernel
	n    int
	last core.Tick
}

func (a *arms) Arm(deadline core.Tick) {
	a.n++
	a.last = deadline
	a.Kernel.Arm(deadline)
}

// ticking arms timer 1 every 50 ms and, at random, timers 2 to 4 within 300 ms, and counts the
// Resumed events it gets and any clock reading that went back.
type ticking struct {
	rand     *core.Rand
	resumed  int
	now, raw core.Tick
	back     bool
}

func (m *ticking) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	m.back = m.back || ctx.Now < m.now || ctx.Raw < m.raw
	m.now, m.raw = ctx.Now, ctx.Raw
	switch ev.Kind {
	case core.EvResumed:
		m.resumed++
	case core.EvTimer:
		if ev.Timer <= 1 {
			out = append(out, timer(1, ctx.Now+50_000))
		}
		if m.rand.Chance(1, 2) {
			out = append(out, timer(2+core.TimerID(m.rand.Below(3)), ctx.Now+core.Tick(m.rand.Below(300_000))))
		}
	}
	return out
}

// counter counts the Resumed events the simulator hands the stepped loop's own machine.
type counter struct{ resumed int }

func (c *counter) OnEvent(_ *core.Context, ev *core.Event, out []core.Action) []core.Action {
	if ev.Kind == core.EvResumed {
		c.resumed++
	}
	return out
}

const (
	maxSlew  = 226_000_000
	maxDrift = clock.Unit / 25
)

// TestSteppedResumedComesOncePerStop runs a stepped loop through seeded slews of up to ±22.6%,
// raw rates up to 4% off, every kind of stop and inbox events that fire no timer, and checks that the loop and the simulator each
// deliver one Resumed for every stop that moved the marker or suspended for at least the
// threshold, and no other; that Now and Raw never go back; and that the loop arms its timerfd at
// most once a pass, only when the earliest deadline changed, always to that deadline.
func TestSteppedResumedComesOncePerStop(t *testing.T) {
	seeds.Run(t, 1, 50, func(seed uint64) {
		s := sim.New(seed, sim.Settings{PoolBytes: 1 << 20})
		node := s.AddNode(sim.DefaultClock, nil, func(*sim.Node) {})
		r := node.Fork()
		k := NewKernel(s, node.ID())
		a := &arms{Kernel: k}
		m := &ticking{rand: node.Fork()}
		ring, err := trace.NewRing(make([]byte, 1<<14*trace.RecordSize))
		if err != nil {
			t.Fatal(err)
		}
		l := loop.New(loop.Config{Node: node.ID(), Pool: sim.NewPool(1<<20, 0), Trace: ring}, a, m, &Wire{})
		k.Attach(l)
		simSide := &counter{}
		node.AddActor(node.Start(simSide, nil), &Actor{K: k, L: l})
		l.Inbox().Post(&core.Event{Kind: core.EvTimer})
		step := func() {
			d0, n0 := l.Pending().Deadline, a.n
			if !s.Step() {
				t.Fatalf("seed %d: the run stopped", seed)
			}
			d1 := l.Pending().Deadline
			switch {
			case a.n > n0+1:
				t.Fatalf("seed %d: %d timerfd_settime in pass %d", seed, a.n-n0, l.Passes())
			case a.n == n0+1 && d1 == d0 && d0 > l.Context().Now:
				t.Fatalf("seed %d: pass %d armed the timerfd with its earliest deadline %d unchanged", seed, l.Passes(), d0)
			case d1 != 0 && a.last != d1:
				t.Fatalf("seed %d: after pass %d the timerfd holds %d, the heap's earliest is %d", seed, l.Passes(), a.last, d1)
			}
		}
		step()
		want := 0
		for range 40 {
			now := s.Time()
			switch r.Below(4) {
			case 0:
				node.Clock().SetSlew(now, int64(r.Below(2*maxSlew+1))-maxSlew)
			case 1:
				node.Clock().SetRate(now, clock.Unit-maxDrift+r.Below(2*maxDrift+1))
			case 2:
				stop := clock.Stop(1 + r.Below(uint64(clock.HeldMigration)))
				d := 1 + core.Tick(r.Below(uint64(3*second)))
				s.Pause(node.ID(), stop, d, false)
				if stop == clock.Restore || stop == clock.HeldMigration || stop == clock.Suspend && d/second*second >= core.ResumeThreshold {
					want++
				}
			case 3:
				l.Inbox().Post(&core.Event{Kind: core.EvNetRecv})
			}
			for p := l.Passes(); l.Passes() < p+2; {
				step()
			}
		}
		if err := k.Err(); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		if m.resumed != want || simSide.resumed != want || m.back {
			t.Fatalf("seed %d: the loop got %d Resumed, the simulator %d, for %d stops that called for one; clocks went back: %v", seed, m.resumed, simSide.resumed, want, m.back)
		}
		if ring.Added() > uint64(ring.Len()) {
			t.Fatalf("seed %d: the trace ring overflowed", seed)
		}
		tick := map[uint64]core.Tick{}
		for i := range ring.Len() {
			rec := ring.At(i)
			if at, ok := tick[rec.Pass]; ok && at != rec.Tick {
				t.Fatalf("seed %d: pass %d holds records at ticks %d and %d", seed, rec.Pass, at, rec.Tick)
			}
			tick[rec.Pass] = rec.Tick
		}
	})
}
