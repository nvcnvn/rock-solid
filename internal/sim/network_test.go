package sim_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/clock"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
	"github.com/nvcnvn/rock-solid/internal/sim/toy"
	"github.com/nvcnvn/rock-solid/internal/trace"
)

const (
	msgRequest byte = 1
	msgReply   byte = 2
	second          = core.Tick(1_000_000)
)

// echo sends requests carrying a pool buffer through core.PeerRequests and answers each request
// with a Reply carrying one; it frees every buffer the driver contract hands back to it, unless
// early, when it frees a request's buffer once the request times out.
type echo struct {
	timeout  core.Tick
	early    bool
	reqs     *core.PeerRequests
	next     core.RequestID
	fails    []core.Failure
	events   []core.Event
	at       []core.Context
	rtts     []core.Tick
	timeouts int
	resumed  int
	// lostWith is the requests each PeerLost failed, and heldAtLost the buffers then live beyond the requests still tracked.
	lostWith   []int
	heldAtLost []int
}

func newEcho() *echo { return &echo{timeout: 1000, reqs: core.NewPeerRequests(), next: 1} }

func echoMsg(kind byte, id core.RequestID) core.Msg {
	var m core.Msg
	m.Body[0] = kind
	binary.LittleEndian.PutUint64(m.Body[1:], uint64(id))
	m.Len = 9
	return m
}

func (e *echo) ping(ctx *core.Context, peer core.NodeID, out []core.Action) []core.Action {
	buf, err := ctx.Pool.Alloc(core.BlockSize)
	if err != nil {
		return out
	}
	id := e.next
	e.next++
	m := echoMsg(msgRequest, id)
	return e.reqs.Send(ctx, id, peer, core.TimerID(id), e.timeout, &m, buf, out)
}

func (e *echo) free(ctx *core.Context, b core.IOBuf) {
	if !e.early {
		ctx.Pool.Free(b)
	}
}

func (e *echo) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	e.events = append(e.events, *ev)
	e.at = append(e.at, *ctx)
	switch ev.Kind {
	case core.EvNetRecv:
		if ev.Msg.Buf != core.NoBuf {
			ctx.Pool.Free(ev.Msg.Buf)
		}
		id := core.RequestID(binary.LittleEndian.Uint64(ev.Msg.Body[1:]))
		switch ev.Msg.Body[0] {
		case msgRequest:
			a := core.Action{Kind: core.ActNetSend, Peer: ev.Peer, Msg: echoMsg(msgReply, id), Reply: true}
			if b, err := ctx.Pool.Alloc(core.BlockSize); err == nil {
				a.Msg.Buf = b
			}
			out = append(out, a)
		case msgReply:
			if r, ok := e.reqs.Complete(id); ok {
				e.free(ctx, r.Buf)
				e.rtts = append(e.rtts, ctx.Now-(r.Deadline-e.timeout))
			}
		}
	case core.EvTimer:
		e.fails = e.reqs.OnTimer(ctx, ev.Timer, e.fails[:0])
		e.timeouts += len(e.fails)
		for _, f := range e.fails {
			if e.early {
				ctx.Pool.Free(f.Buf)
			}
		}
	case core.EvPeerLost:
		e.fails = e.reqs.OnPeerLost(ev.Peer, e.fails[:0])
		for _, f := range e.fails {
			e.free(ctx, f.Buf)
		}
		e.lostWith = append(e.lostWith, len(e.fails))
		e.heldAtLost = append(e.heldAtLost, ctx.Pool.(*sim.Pool).Live()-e.reqs.InFlight())
	case core.EvResumed:
		e.resumed++
	}
	return out
}

func (e *echo) received(kind byte) []core.Event {
	var got []core.Event
	for _, ev := range e.events {
		if ev.Kind == core.EvNetRecv && ev.Msg.Body[0] == kind {
			got = append(got, ev)
		}
	}
	return got
}

// cluster is nodes running one echo each; a restart gives a node a new echo.
type cluster struct {
	s  *sim.Scheduler
	m  []*echo
	id []core.NodeID
}

func newCluster(seed uint64, set sim.Settings, clocks ...sim.Clock) *cluster {
	c := &cluster{s: sim.New(seed, set), m: make([]*echo, len(clocks)), id: make([]core.NodeID, len(clocks))}
	for i, ck := range clocks {
		c.s.AddNode(ck, nil, func(n *sim.Node) {
			c.m[i] = newEcho()
			c.id[i] = n.Start(c.m[i], nil)
		})
	}
	return c
}

func nodes(k int) []sim.Clock { return slices.Repeat([]sim.Clock{sim.DefaultClock}, k) }

func (c *cluster) ping(from, to int) {
	c.s.Node(uint32(from)).Call(c.id[from], func(ctx *core.Context, out []core.Action) []core.Action {
		return c.m[from].ping(ctx, c.id[to], out)
	})
}

func (c *cluster) live(i int) int { return c.s.Node(uint32(i)).Pool().Live() }

// settle takes steps until nothing is pending, failing a run that does not stop.
func (c *cluster) settle(t *testing.T) {
	t.Helper()
	if n := c.s.Run(100_000); n == 100_000 {
		t.Fatalf("the run did not settle in %d steps", n)
	}
}

func TestALostRequestTimesOutAndNothingHangs(t *testing.T) {
	for _, failAfter := range []core.Tick{10 * second, 0} {
		set := sim.DefaultSettings()
		set.DropNum, set.DropDen = 1, 1
		set.FailAfter = failAfter
		c := newCluster(1, set, nodes(2)...)
		c.ping(0, 1)
		c.settle(t)
		if c.m[0].timeouts != 1 || len(c.m[1].received(msgRequest)) != 0 {
			t.Fatalf("FailAfter %d: %d timeouts, the request arrived", failAfter, c.m[0].timeouts)
		}
		if failAfter == 0 {
			if c.m[0].lostWith != nil || c.live(0) != 1 {
				t.Fatalf("a connection that never fails: PeerLost %v, %d buffers live, want the request's kept", c.m[0].lostWith, c.live(0))
			}
			continue
		}
		if !slices.Equal(c.m[0].lostWith, []int{1}) || !slices.Equal(c.m[1].lostWith, []int{0}) || c.live(0) != 0 {
			t.Fatalf("PeerLost failed %v and %v, %d buffers live", c.m[0].lostWith, c.m[1].lostWith, c.live(0))
		}
	}
}

// arrivals is who sent each request node 0 received, and when after the sends at time 0.
func arrivals(seed uint64, set sim.Settings) (from []uint32, after []core.Tick) {
	c := newCluster(seed, set, nodes(3)...)
	for range 5 {
		c.ping(1, 0)
		c.ping(2, 0)
	}
	c.s.Run(1000)
	for i, ev := range c.m[0].events {
		if ev.Kind == core.EvNetRecv {
			from = append(from, ev.Peer.Node)
			after = append(after, c.m[0].at[i].Now-sim.DefaultClock.Now)
		}
	}
	return from, after
}

// Each connection keeps its order, so only messages from different senders can arrive swapped.
func TestMessagesArriveAfterTheirDelayInSeededOrderAndSwapOnlyAcrossConnections(t *testing.T) {
	set := sim.DefaultSettings()
	set.NetDelay, set.NetJitter = 100, 400
	orders := map[string]bool{}
	swapped := false
	for seed := range uint64(20) {
		from, after := arrivals(seed, set)
		from2, after2 := arrivals(seed, set)
		if len(from) != 10 || !slices.Equal(from, from2) || !slices.Equal(after, after2) {
			t.Fatalf("seed %d: arrivals %v at %v, then %v at %v", seed, from, after, from2, after2)
		}
		for _, d := range after {
			if d < 100 || d > 500 {
				t.Fatalf("seed %d: a message arrived %d after it was sent", seed, d)
			}
		}
		orders[fmt.Sprint(from)] = true
		swapped = swapped || from[0] == 2
	}
	if len(orders) < 2 || !swapped {
		t.Fatalf("20 seeds gave %d orders, swapped %v", len(orders), swapped)
	}
	for seed := range uint64(20) {
		c := newCluster(seed, set, nodes(2)...)
		for range 10 {
			c.ping(1, 0)
		}
		c.s.Run(1000)
		var ids []uint64
		for _, ev := range c.m[0].received(msgRequest) {
			ids = append(ids, binary.LittleEndian.Uint64(ev.Msg.Body[1:]))
		}
		if len(ids) != 10 || !slices.IsSorted(ids) {
			t.Fatalf("seed %d: one connection delivered %v", seed, ids)
		}
	}
}

func TestEachMessageDrawsItsOwnDelay(t *testing.T) {
	set := sim.DefaultSettings()
	set.NetDelay, set.NetJitter = 50, 1000
	c := newCluster(1, set, nodes(2)...)
	c.m[0].timeout = 10 * second
	for range 30 {
		c.ping(0, 1)
		c.s.Run(100)
	}
	rtts := c.m[0].rtts
	distinct := slices.Compact(slices.Sorted(slices.Values(rtts)))
	if len(rtts) != 30 || len(distinct) < 20 || distinct[0] < 100 || distinct[len(distinct)-1] > 2100 {
		t.Fatalf("round trips %v", rtts)
	}
}

func TestAQueueDelaysOneDirectionOnly(t *testing.T) {
	c := newCluster(1, sim.DefaultSettings(), nodes(2)...)
	c.m[0].timeout = second
	c.s.Queue(0, 1, 50_000)
	c.ping(0, 1)
	c.s.Run(100)
	if got := c.m[1].at[0].Now - sim.DefaultClock.Now; got != 50_050 {
		t.Fatalf("the request took %d one way", got)
	}
	if !slices.Equal(c.m[0].rtts, []core.Tick{50_100}) {
		t.Fatalf("round trip %v, want the queue on the way out only", c.m[0].rtts)
	}
}

func TestADuplicateArrivesTwiceInARowAndChangesNothing(t *testing.T) {
	set := sim.DefaultSettings()
	set.DupNum, set.DupDen = 1, 1
	set.NetJitter = 1000
	c := newCluster(1, set, nodes(2)...)
	c.ping(0, 1)
	c.ping(0, 1)
	c.settle(t)
	var ids []uint64
	for _, ev := range c.m[1].received(msgRequest) {
		ids = append(ids, binary.LittleEndian.Uint64(ev.Msg.Body[1:]))
	}
	if !slices.Equal(ids, []uint64{1, 1, 2, 2}) || len(c.m[0].received(msgReply)) != 8 || len(c.m[0].rtts) != 2 {
		t.Fatalf("requests %v, replies %d, completed %d", ids, len(c.m[0].received(msgReply)), len(c.m[0].rtts))
	}
	if c.live(0) != 0 || c.live(1) != 0 {
		t.Fatalf("buffers live: %d and %d", c.live(0), c.live(1))
	}
	set = sim.DefaultSettings()
	set.DupNum, set.DupDen = 1, 2
	set.CrashNum, set.CrashDen = 1, 300
	for seed := range uint64(20) {
		tc := toy.NewCluster(seed, set, 40, toy.NoBug)
		if err := tc.Run(3000); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		if tc.Acked() == 0 || !bytes.Contains(tc.Scheduler().Trace(), []byte("NetRecv peer=0 msg=write")) {
			t.Fatalf("seed %d: %d acknowledged", seed, tc.Acked())
		}
	}
}

func TestAPartitionLosesMessagesAndFailsItsConnectionOnceOrNever(t *testing.T) {
	for _, failAfter := range []core.Tick{1000, 0} {
		for _, oneWay := range []bool{false, true} {
			set := sim.DefaultSettings()
			set.FailAfter = failAfter
			c := newCluster(1, set, nodes(2)...)
			c.ping(0, 1)
			c.s.Run(100)
			c.s.Partition(0, 1, oneWay)
			for range 3 {
				c.ping(0, 1)
			}
			c.settle(t)
			if len(c.m[1].received(msgRequest)) != 1 {
				t.Fatalf("FailAfter %d one-way %v: the partition let %d requests through", failAfter, oneWay, len(c.m[1].received(msgRequest))-1)
			}
			if failAfter == 0 {
				if c.m[0].lostWith != nil || c.m[1].lostWith != nil || c.m[0].timeouts != 3 {
					t.Fatalf("a connection that never fails: PeerLost %v and %v, %d timeouts", c.m[0].lostWith, c.m[1].lostWith, c.m[0].timeouts)
				}
				continue
			}
			if !slices.Equal(c.m[0].lostWith, []int{3}) || !slices.Equal(c.m[1].lostWith, []int{0}) {
				t.Fatalf("one-way %v: PeerLost failed %v and %v, want every request in one", oneWay, c.m[0].lostWith, c.m[1].lostWith)
			}
			if c.live(0) != 0 || c.live(1) != 0 || slices.ContainsFunc(append(c.m[0].heldAtLost, c.m[1].heldAtLost...), func(n int) bool { return n != 0 }) {
				t.Fatalf("buffers live %d and %d, held at PeerLost %v and %v", c.live(0), c.live(1), c.m[0].heldAtLost, c.m[1].heldAtLost)
			}
		}
	}
	c := newCluster(1, sim.DefaultSettings(), nodes(2)...)
	c.s.Partition(0, 1, true)
	c.ping(1, 0)
	c.settle(t)
	if len(c.m[0].received(msgRequest)) != 1 || len(c.m[1].received(msgReply)) != 0 || !slices.Equal(c.m[1].lostWith, []int{1}) {
		t.Fatalf("one way from 0 to 1: node 0 got %d requests, node 1 %d replies and PeerLost %v", len(c.m[0].received(msgRequest)), len(c.m[1].received(msgReply)), c.m[1].lostWith)
	}
}

// A crashed process's kernel still sends what it had sent; a reboot loses it.
func TestACrashGivesEachPeerOnePeerLostAfterWhatTheNodeSent(t *testing.T) {
	for _, reboot := range []bool{false, true} {
		c := newCluster(1, sim.DefaultSettings(), nodes(3)...)
		c.ping(1, 0)
		c.ping(2, 0)
		c.s.Run(100)
		c.ping(0, 1)
		c.ping(0, 2)
		want := []core.EventKind{core.EvNetRecv, core.EvPeerLost}
		if reboot {
			c.s.Reboot(0)
			want = want[1:]
		} else {
			c.s.CrashRandom(0)
		}
		c.settle(t)
		for _, p := range []int{1, 2} {
			var kinds []core.EventKind
			for _, ev := range c.m[p].events {
				if ev.Peer.Node == 0 && (ev.Kind == core.EvPeerLost || ev.Kind == core.EvNetRecv && ev.Msg.Body[0] == msgRequest) {
					kinds = append(kinds, ev.Kind)
				}
			}
			if !slices.Equal(kinds, want) {
				t.Fatalf("reboot %v: node %d saw %v from the crashed node", reboot, p, kinds)
			}
		}
	}
}

func TestARestartedSendersPeerSeesPeerLostBeforeItsNewMessages(t *testing.T) {
	set := sim.DefaultSettings()
	set.FailAfter = 10 * second
	c := newCluster(1, set, nodes(2)...)
	c.ping(0, 1)
	c.settle(t)
	c.s.CrashRandom(0)
	c.s.Restart(0)
	restarted := c.s.Time()
	c.ping(0, 1)
	for len(c.m[1].received(msgRequest)) < 2 {
		c.s.Step()
	}
	var kinds []core.EventKind
	for _, ev := range c.m[1].events {
		if ev.Peer.Node == 0 && (ev.Kind == core.EvPeerLost || ev.Kind == core.EvNetRecv && ev.Msg.Body[0] == msgRequest) {
			kinds = append(kinds, ev.Kind)
		}
	}
	if !slices.Equal(kinds, []core.EventKind{core.EvNetRecv, core.EvPeerLost, core.EvNetRecv}) || c.s.Time()-restarted > 1000 {
		t.Fatalf("node 1 saw %v from node 0, the new request %d after the restart", kinds, c.s.Time()-restarted)
	}
}

func TestAReplysBufferIsFreedOnceSentOrLost(t *testing.T) {
	c := newCluster(1, sim.DefaultSettings(), nodes(2)...)
	c.ping(0, 1)
	c.settle(t)
	if c.live(1) != 0 || len(c.m[0].rtts) != 1 {
		t.Fatalf("after a delivered reply: %d buffers live, %d completed", c.live(1), len(c.m[0].rtts))
	}
	c.ping(0, 1)
	c.s.Partition(1, 0, true)
	for len(c.m[1].received(msgRequest)) < 2 {
		c.s.Step()
	}
	if c.live(1) != 0 {
		t.Fatalf("a reply lost at once still holds %d buffers", c.live(1))
	}
}

func TestAPeerLostComesAfterThatPeersBuffersAreReleased(t *testing.T) {
	c := newCluster(1, sim.DefaultSettings(), nodes(2)...)
	c.m[0].timeout = second
	for range 3 {
		c.ping(0, 1)
	}
	for len(c.m[1].received(msgRequest)) < 3 {
		c.s.Step()
	}
	if c.live(1) != 3 {
		t.Fatalf("%d replies in flight", c.live(1))
	}
	c.s.Partition(0, 1, true)
	c.ping(0, 1)
	c.settle(t)
	if !slices.Equal(c.m[1].heldAtLost, []int{0}) || !slices.Equal(c.m[0].lostWith, []int{4}) || c.live(0) != 0 {
		t.Fatalf("held at PeerLost %v, failed %v, %d live", c.m[1].heldAtLost, c.m[0].lostWith, c.live(0))
	}
}

func TestATimedOutRequestKeepsItsBufferUntilItsReply(t *testing.T) {
	set := sim.DefaultSettings()
	set.NetDelay = 5000
	c := newCluster(1, set, nodes(2)...)
	c.ping(0, 1)
	for c.m[0].timeouts == 0 {
		c.s.Step()
	}
	if c.live(0) != 1 {
		t.Fatalf("%d buffers live after the timeout", c.live(0))
	}
	c.settle(t)
	if len(c.m[0].rtts) != 1 || c.live(0) != 0 || c.s.DriverContract() != nil {
		t.Fatalf("completed %d, %d live, contract %v", len(c.m[0].rtts), c.live(0), c.s.DriverContract())
	}
}

func TestFreeingASendsBufferEarlyBreaksTheDriverContract(t *testing.T) {
	set := sim.DefaultSettings()
	set.NetDelay = 5000
	c := newCluster(1, set, nodes(2)...)
	c.m[0].early = true
	c.ping(0, 1)
	c.settle(t)
	if c.s.DriverContract() == nil {
		t.Fatal("a request buffer freed before its reply passed")
	}
}

// ticker re-arms its timer every 100 ms until its node's CLOCK_BOOTTIME passes until.
type ticker struct{ until core.Tick }

func (k *ticker) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	if ev.Kind != core.EvTimer || ctx.Now > k.until {
		return out
	}
	return append(out, core.Action{Kind: core.ActSetTimer, Timer: 1, At: ctx.Now + 100_000})
}

func tick(n *sim.Node, until core.Tick) {
	k := &ticker{until: until}
	id := n.Start(k, nil)
	n.Feed(id, core.Event{Kind: core.EvTimer, Timer: 1})
}

func TestEachNodesRawClockRunsAtAFixedRateWithinFourPercent(t *testing.T) {
	set := sim.DefaultSettings()
	set.ClockNum, set.ClockDen = 0, 1
	rates := map[uint64]bool{}
	for seed := range uint64(10) {
		s := sim.New(seed, set)
		for range 3 {
			s.AddNode(sim.DefaultClock, nil, func(n *sim.Node) { tick(n, 10*second) })
		}
		before := s.Node(0).Context()
		s.Run(1000)
		real := s.Time()
		for i := range uint32(3) {
			r := s.Node(i).Clock().Rate()
			if r < clock.Unit*24/25 || r > clock.Unit*26/25 {
				t.Fatalf("seed %d node %d: raw rate %d", seed, i, r)
			}
			rates[r] = true
		}
		got := s.Node(0).Context().Raw - before.Raw
		if want := core.Tick(uint64(real) * s.Node(0).Clock().Rate() / clock.Unit); got != want || real < 9*second {
			t.Fatalf("seed %d: raw gained %d over %d real, want %d", seed, got, real, want)
		}
	}
	if len(rates) < 25 {
		t.Fatalf("30 nodes drew %d rates", len(rates))
	}
}

func TestARawRateStepsOncePerBootAfterTheRateWindowAndSlewsStayWithinBounds(t *testing.T) {
	set := sim.DefaultSettings()
	set.ClockNum, set.ClockDen = 1, 1
	stepped, chrony, wide := 0, false, false
	for seed := range uint64(10) {
		s := sim.New(seed, set)
		start := make([]uint64, 3)
		models := make([]*clock.Model, 3)
		booted := make([]core.Tick, 3)
		steps := make([]int, 3)
		for i := range 3 {
			s.AddNode(sim.DefaultClock, nil, func(n *sim.Node) { tick(n, 30*second) })
			models[i] = s.Node(uint32(i)).Clock()
			start[i] = models[i].Rate()
		}
		rebooted := false
		for s.Step() {
			if !rebooted && s.Time() >= 15*second {
				rebooted = true
				for i := range uint32(3) {
					s.Reboot(i)
				}
			}
			for i := range uint32(3) {
				m := s.Node(i).Clock()
				if m != models[i] {
					models[i], start[i], booted[i], steps[i] = m, m.Rate(), s.Time(), 0
				}
				if m.Rate() != start[i] {
					d := int64(m.Rate()) - int64(start[i])
					if steps[i] > 0 || s.Time()-booted[i] < 10*second || d > clock.Unit/200 || d < -clock.Unit/200 {
						t.Fatalf("seed %d node %d: raw rate stepped by %d at %d, %d steps into its boot at %d", seed, i, d, s.Time(), steps[i], booted[i])
					}
					stepped++
					steps[i]++
					start[i] = m.Rate()
				}
				if sl := m.Slew(); sl > 226_000_000 || sl < -226_000_000 {
					t.Fatalf("seed %d node %d: slew %d", seed, i, sl)
				} else if sl == 83_333_333 || sl == -83_333_333 {
					chrony = true
				} else if sl != 0 {
					wide = true
				}
			}
		}
	}
	if stepped < 45 || stepped > 60 || !chrony || !wide {
		t.Fatalf("%d of 60 boots stepped; chrony's slew drawn %v, others %v", stepped, chrony, wide)
	}
}

func TestARebootRestartsTheClocksAndAProcessRestartKeepsThem(t *testing.T) {
	changed := 0
	for seed := range uint64(10) {
		c := newCluster(seed, sim.DefaultSettings(), nodes(1)...)
		n := c.s.Node(0)
		c.s.CrashRandom(0)
		before := n.Context()
		c.s.Restart(0)
		if after := n.Context(); after.Raw < before.Raw || n.Clock().Rate() != clock.Unit {
			t.Fatalf("a process restart moved the clocks from %+v to %+v", before, after)
		}
		c.s.Reboot(0)
		at := c.s.Time()
		for _, ch := range c.s.Choices() {
			if ch.Kind == sim.ChooseRestart && c.s.Time() < at+second {
				t.Fatalf("a rebooted node can restart at %d", c.s.Time()-at)
			}
		}
		c.s.Run(1)
		if !n.Up() || c.s.Time() < at+second {
			t.Fatalf("up %v after %d down", n.Up(), c.s.Time()-at)
		}
		after := n.Context()
		d := int64(n.Clock().Rate()) - clock.Unit
		if after.Raw < second || after.Raw > 2*second || after.Now < second || after.Now > 2*second || d > clock.Unit/50 || d < -clock.Unit/50 {
			t.Fatalf("seed %d: after a reboot %+v at rate %+d ppb", seed, after, d)
		}
		if d != 0 {
			changed++
		}
	}
	if changed == 0 {
		t.Fatal("no reboot changed the raw rate")
	}
}

// pauseNode pauses node 0 of a two-node cluster at once and returns its readings before and after, then runs two passes on it.
func pauseNode(t *testing.T, stop clock.Stop, d core.Tick) (c *cluster, before, after core.Context) {
	t.Helper()
	c = newCluster(1, sim.DefaultSettings(), nodes(2)...)
	before = c.s.Node(0).Context()
	c.s.Pause(0, stop, d, false)
	c.settle(t)
	after = c.s.Node(0).Context()
	c.ping(0, 1)
	c.ping(0, 1)
	return c, before, after
}

func TestASuspendGivesOneResumedOnceItsGainReachesTheThreshold(t *testing.T) {
	c, before, after := pauseNode(t, clock.Suspend, 2*second+500_000)
	if c.m[0].resumed != 1 || after.Now-before.Now != 2*second || after.Raw != before.Raw {
		t.Fatalf("%d Resumed; from %+v to %+v, want BOOTTIME to gain 2 s and raw nothing", c.m[0].resumed, before, after)
	}
	if c, _, _ := pauseNode(t, clock.Suspend, 900_000); c.m[0].resumed != 0 {
		t.Fatal("a suspend the kernel rounded to 0 gave Resumed")
	}
}

func TestEachVMPauseMovesTheClocksAndMarkerAsMeasured(t *testing.T) {
	const pause = 4 * second
	for _, k := range []struct {
		stop    clock.Stop
		gain    core.Tick
		resumed int
	}{
		{clock.PauseCounted, pause, 0},
		{clock.PauseFrozen, 0, 0},
		{clock.Restore, 0, 1},
		{clock.HeldMigration, pause, 1},
	} {
		c, before, after := pauseNode(t, k.stop, pause)
		if c.m[0].resumed != k.resumed || after.Now-before.Now != k.gain || after.Raw-before.Raw != k.gain {
			t.Errorf("stop %d: %d Resumed, clocks from %+v to %+v", k.stop, c.m[0].resumed, before, after)
		}
	}
}

// once is a client on a loop that takes one operation, recording how many events its loop had seen by then.
type once struct {
	ready bool
	m     *echo
	seen  int
}

func (o *once) Ready() bool { return o.ready }

func (o *once) Next(_ *core.Context, out []core.Action) []core.Action {
	o.ready = false
	o.seen = len(o.m.events)
	return out
}

func TestMessagesToAPausedNodeWaitForItOrAreLost(t *testing.T) {
	c := newCluster(1, sim.DefaultSettings(), nodes(2)...)
	c.m[1].timeout = 10 * second
	c.ping(1, 0)
	c.settle(t)
	client := &once{m: c.m[0]}
	c.s.Node(0).AddActor(c.id[0], client)
	c.s.Pause(0, clock.PauseCounted, second, false)
	client.ready = true
	c.ping(1, 0)
	c.settle(t)
	got := c.m[0].received(msgRequest)
	if len(got) != 2 || c.m[0].at[len(c.m[0].at)-1].Now < second || client.seen != len(c.m[0].events) || len(c.m[1].rtts) != 2 {
		t.Fatalf("requests %d, the client ran after %d of %d events, %d round trips", len(got), client.seen, len(c.m[0].events), len(c.m[1].rtts))
	}

	c = newCluster(1, sim.DefaultSettings(), nodes(2)...)
	c.m[1].timeout = 10 * second
	c.ping(1, 0)
	c.settle(t)
	c.s.Pause(0, clock.PauseCounted, second, true)
	c.ping(1, 0)
	for len(c.m[1].lostWith) == 0 {
		c.s.Step()
	}
	c.ping(1, 0)
	c.settle(t)
	if len(c.m[0].received(msgRequest)) != 1 || !slices.Equal(c.m[1].lostWith, []int{1, 1}) || !slices.Equal(c.m[0].lostWith, []int{0}) {
		t.Fatalf("lost: node 0 got %d requests; PeerLost %v and %v", len(c.m[0].received(msgRequest)), c.m[0].lostWith, c.m[1].lostWith)
	}

	c = newCluster(1, sim.DefaultSettings(), nodes(2)...)
	c.m[1].timeout = 10 * second
	c.s.Pause(0, clock.PauseCounted, second, true)
	c.ping(1, 0)
	c.settle(t)
	if len(c.m[0].received(msgRequest)) != 0 || !slices.Equal(c.m[1].lostWith, []int{1}) || c.m[0].lostWith != nil {
		t.Fatalf("lost with no connection: node 0 got %d requests; PeerLost %v and %v", len(c.m[0].received(msgRequest)), c.m[0].lostWith, c.m[1].lostWith)
	}
}

func TestANodeSeesOnlyItsOwnClocks(t *testing.T) {
	set := sim.DefaultSettings()
	set.ClockNum, set.ClockDen = 0, 1
	c := newCluster(1, set, sim.DefaultClock, sim.Clock{Now: 5 * second, Raw: 7 * second})
	c.s.Run(1)
	c.ping(0, 1)
	c.s.Run(1)
	got, want := c.m[1].at[0], c.s.Node(1).Context()
	if got.Now != want.Now || got.Raw != want.Raw || got.Now < 5*second {
		t.Fatalf("the receiver saw %+v, its clocks read %+v", got, want)
	}
}

// resumedRule checks a trace against core.Resumed's rule: after a pause that moved the marker or a
// suspend whose BOOTTIME gain reached the threshold, each loop's next pass starts with one Resumed,
// and there is no other Resumed.
func resumedRule(tr []byte) error {
	type loopKey struct {
		node uint32
		loop uint16
	}
	owed := map[loopKey]bool{}
	pausedAt := map[loopKey]trace.Record{}
	for line := range bytes.Lines(tr) {
		r, err := trace.Parse(bytes.TrimSuffix(line, []byte{'\n'}))
		if err != nil {
			return err
		}
		k := loopKey{r.Node, r.Loop}
		switch r.Name {
		case trace.EvDrop:
		case trace.EvCrash, trace.EvRestart:
			owed[k] = false
		case trace.EvPause:
			pausedAt[k] = r
		case trace.EvResume:
			p := pausedAt[k]
			switch clock.Stop(p.Fields[0]) {
			case clock.Restore, clock.HeldMigration:
				owed[k] = true
			case clock.Suspend:
				owed[k] = owed[k] || r.Tick-p.Tick >= clock.Threshold
			}
		case trace.EvResumed:
			if !owed[k] {
				return fmt.Errorf("a Resumed no pause called for: %s", line)
			}
			owed[k] = false
		default:
			if owed[k] {
				return fmt.Errorf("a pass after a pause began without Resumed: %s", line)
			}
		}
	}
	return nil
}

// pinger pings a random peer with up to 4 requests outstanding, 30 times per incarnation.
type pinger struct {
	m     *echo
	peers []core.NodeID
	r     *core.Rand
	left  int
}

func (p *pinger) Ready() bool { return p.left > 0 && p.m.reqs.InFlight() < 4 }

func (p *pinger) Next(ctx *core.Context, out []core.Action) []core.Action {
	p.left--
	return p.m.ping(ctx, p.peers[p.r.Below(uint64(len(p.peers)))], out)
}

func faultSettings() sim.Settings {
	set := sim.DefaultSettings()
	set.NetJitter = 200
	set.FailAfter = 3 * second
	set.DropNum, set.DropDen = 1, 50
	set.DupNum, set.DupDen = 1, 50
	set.CrashNum, set.CrashDen = 1, 500
	set.RebootNum, set.RebootDen = 1, 500
	set.PauseNum, set.PauseDen = 1, 300
	set.PartitionNum, set.PartitionDen = 1, 200
	set.ClockNum, set.ClockDen = 1, 50
	return set
}

// faultRun runs three pinging nodes under every network and clock fault, checking the driver
// contract after every step, then heals the network and settles it with no more faults.
func faultRun(t *testing.T, seed uint64) *cluster {
	const n = 3
	c := &cluster{s: sim.New(seed, faultSettings()), m: make([]*echo, n), id: make([]core.NodeID, n)}
	for i := range n {
		c.s.AddNode(sim.DefaultClock, nil, func(nd *sim.Node) {
			c.m[i] = newEcho()
			c.id[i] = nd.Start(c.m[i], nil)
			var peers []core.NodeID
			for j := range uint32(n) {
				if j != uint32(i) {
					peers = append(peers, core.NodeID{Node: j})
				}
			}
			nd.AddActor(c.id[i], &pinger{m: c.m[i], peers: peers, r: nd.Fork(), left: 30})
		})
	}
	var inv sim.Invariants
	inv.Register("driver contract", c.s.DriverContract)
	inv.Register("clock rates", func() error {
		for i := range uint32(n) {
			m := c.s.Node(i).Clock()
			if r := m.Rate(); r < clock.Unit-clock.Unit*9/200 || r > clock.Unit+clock.Unit*9/200 {
				return fmt.Errorf("node %d raw rate %d", i, r)
			}
			if sl := m.Slew(); sl > 226_000_000 || sl < -226_000_000 {
				return fmt.Errorf("node %d slew %d", i, sl)
			}
		}
		return nil
	})
	for range 3000 {
		if !c.s.Step() {
			break
		}
		if err := inv.Check(); err != nil {
			t.Fatalf("seed %d step %d: %v", seed, c.s.Steps(), err)
		}
	}
	for a := range uint32(n) {
		for b := range uint32(n) {
			c.s.Heal(a, b)
		}
	}
	for range 100_000 {
		cs := c.s.Choices()
		if len(cs) == 0 {
			break
		}
		c.s.Take(cs[0])
		if err := inv.Check(); err != nil {
			t.Fatalf("seed %d step %d: %v", seed, c.s.Steps(), err)
		}
	}
	return c
}

func TestFaultsKeepTheDriverContractAndTheResumedRule(t *testing.T) {
	seen := map[string]int{}
	seeds.Run(t, 0, 20, func(seed uint64) {
		c := faultRun(t, seed)
		for _, name := range []string{"Resumed", "PeerLost", "Drop", "Pause stop=1", "Pause stop=3", "Pause stop=4", "Pause stop=5", "lost=1", "Restart", "NetRecv"} {
			seen[name] += bytes.Count(c.s.Trace(), []byte(" "+name))
		}
		for i, m := range c.m {
			if !c.s.Node(uint32(i)).Up() || c.live(i) != 0 || m.reqs.InFlight() != 0 {
				t.Fatalf("seed %d node %d: up %v, %d buffers live, %d requests tracked once settled", seed, i, c.s.Node(uint32(i)).Up(), c.live(i), m.reqs.InFlight())
			}
		}
		tr := c.s.Trace()
		if err := resumedRule(tr); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		if err := trace.Check(tr); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		if err := sim.CompareTraces(tr, faultRun(t, seed).s.Trace()); err != nil {
			t.Fatalf("seed %d replays differently: %v", seed, err)
		}
	})
	if !seeds.Default() {
		return
	}
	for name, n := range seen {
		if n == 0 {
			t.Errorf("no %q in 20 seeds' traces", name)
		}
	}
	t.Logf("%v", seen)
}
