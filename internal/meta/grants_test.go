package meta

import (
	"cmp"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/lease"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

// mib is n MiB in blocks.
func mib(n uint64) uint64 { return n << 20 / core.BlockSize }

// gb is n × 10⁹ bytes in blocks.
func gb(n float64) uint64 { return uint64(n * 1e9 / core.BlockSize) }

// grantRun is a ctlRun with pool 1 committed and sessions started, each a daemon on node i % 5.
type grantRun struct {
	*ctlRun
	in              ControlInputs
	blocks, reserve uint64
	live            []uint64
	sess            []*sessClient
	// flushed is each session's last cumulative flush count, which rounds sends again, and from
	// the issuer of its last answer.
	flushed []uint64
	from    []Issuer
}

// newGrantRun commits pool 1, one copy with the given write_limit on one disk per live count, disk
// i+1 on node i, each of blocks with a log reserve and live blocks, hears their sizes and a fill
// FillCover after the leader started, and starts n sessions.
func newGrantRun(t *testing.T, blocks, reserve uint64, limit uint8, n int, live ...uint64) *grantRun {
	r := &grantRun{ctlRun: newCtlRun(t), blocks: blocks, reserve: reserve, live: live}
	for i, l := range live {
		r.in.Disks = append(r.in.Disks, chunkstore.Disk{ID: uint32(i + 1), Node: uint32(i), Blocks: blocks, Live: l})
	}
	one := 1
	sp := chunkstore.PoolSpec{Replicas: &one, MinLive: &one, WriteLimit: &limit, PGCount: new(uint32(8))}
	if err := r.ctl.SetPool(1, sp, r.in.Disks); err != nil {
		t.Fatal(err)
	}
	r.settle(r.inputs())
	r.opened()
	r.tick(FillCover)
	r.fills()
	r.sess = r.startSessions(n)
	r.flushed, r.from = make([]uint64, n), make([]Issuer, n)
	return r
}

// opened sends each disk's node's report of it opened, with its size and log reserve.
func (r *grantRun) opened() {
	for i := range r.live {
		n := uint32(i)
		r.heartbeat(&NodeReport{Node: n, Meta: true, Opened: []OpenedDisk{{Device: devOf(n), Log: logOf(n), Blocks: r.blocks, Reserve: r.reserve, Class: r.classOf(n)}}})
	}
}

func (r *grantRun) inputs() *ControlInputs {
	r.in.Now, r.in.Raw, r.in.Leased = r.now, r.raw, true
	return &r.in
}

// fills sends each disk's fill and steps the leader until it proposes nothing.
func (r *grantRun) fills() {
	for n := range uint32(5) {
		if nr, ok := r.reports[n]; ok {
			r.heartbeat(&nr)
		}
	}
	for i, l := range r.live {
		r.event(uint32(i), chunkstore.AppendReport(nil, &chunkstore.Report{ID: 1, Disks: []chunkstore.DiskState{{Log: logOf(uint32(i)), Fill: l}}}))
	}
	for r.settle(r.inputs()) {
	}
}

// beat is session i's heartbeat naming pool 1, answered at once; it returns the grant.
func (r *grantRun) beat(i int, pb PoolBeat) uint64 {
	r.t.Helper()
	g, ok := r.send(i, pb)
	if !ok {
		r.t.Fatal("no answer under the leader lease")
	}
	return g
}

// send is session i's heartbeat naming pool 1; it returns the grant and whether the leader answered.
func (r *grantRun) send(i int, pb PoolBeat) (uint64, bool) {
	c := r.sess[i]
	c.req++
	pb.Pool = 1
	r.flushed[i] = pb.Flushed
	var a SessionAnswer
	if !r.ctl.Renew(r.at(), true, &SessionBeat{Session: c.id, Request: c.req, Sent: r.raw, Answered: c.answered, Arrived: c.arrived, Issuer: r.from[i], Pools: []PoolBeat{pb}}, &a) {
		return 0, false
	}
	c.answered, c.arrived, r.from[i] = a.Request, r.raw, a.Issuer
	return a.Grants[0].Grant, true
}

// rounds steps the leader and sends every session's heartbeat, asking asks[i] with buffered[i],
// echoing its last grant, n times, and returns the last grants.
func (r *grantRun) rounds(n int, asks, buffered []uint64) []uint64 {
	gs := make([]uint64, len(r.sess))
	for range n {
		for r.settle(r.inputs()) {
		}
		for i := range r.sess {
			gs[i] = r.beat(i, PoolBeat{Ask: asks[i], Buffered: buffered[i], Held: gs[i], Flushed: r.flushed[i]})
		}
	}
	return gs
}

func equal(t *testing.T, what string, got, want []uint64) {
	t.Helper()
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s: got %v MiB, want %v MiB", what, inMiB(got), inMiB(want))
		}
	}
}

func inMiB(bs []uint64) []float64 {
	out := make([]float64, len(bs))
	for i, b := range bs {
		out[i] = float64(b*core.BlockSize) / (1 << 20)
	}
	return out
}

func sum(bs []uint64) uint64 {
	var s uint64
	for _, b := range bs {
		s += b
	}
	return s
}

// roomy is a disk of 1 TB at 50% under a 90% write_limit, with an 8-segment reserve of 64 MiB.
func roomy(t *testing.T, n int) *grantRun {
	return newGrantRun(t, gb(1000), 8*mib(64), 90, n, gb(500))
}

// availableOf is a disk under a 90% write_limit whose available figure is a: 40 GB, room a.
func availableOf(t *testing.T, a uint64, n int) *grantRun {
	b := gb(40)
	return newGrantRun(t, b, 8*mib(16), 90, n, b*90/100-a)
}

// G1, G2, G2b: with room everywhere each node gets its ask, at most writeback_buffer, and an idle
// node's grant comes back to the pool once a heartbeat echoes it.
func TestGrantsMeetEveryAskWithRoom(t *testing.T) {
	r := roomy(t, 4)
	all := []uint64{mib(256), mib(256), mib(256), mib(256)}
	gs := r.rounds(1, all, make([]uint64, 4))
	equal(t, "G1", gs, all)
	if g := r.Granted(); g != mib(1024) {
		t.Fatalf("G1: Σ %v MiB, want 1024", inMiB([]uint64{g}))
	}
	if g := r.beat(0, PoolBeat{Ask: mib(300) + mib(1024), Buffered: mib(300), Held: gs[0]}); g != mib(256) {
		t.Fatalf("G2: granted %v MiB", inMiB([]uint64{g}))
	}
	r.beat(1, PoolBeat{Held: gs[1]})
	r.beat(1, PoolBeat{})
	if g := r.Granted(); g != mib(768) {
		t.Fatalf("G2b: Σ %v MiB after an idle node's echo, want 768", inMiB([]uint64{g}))
	}
}

func (r *grantRun) Granted() uint64 { return r.ctl.Granted(1) }

// G3 to G3d: a cap of half of available is divided by water-filling with buffered floors, and a
// reply never carries more than the room under the cap: floors above it are counted, not granted.
func TestGrantsAreWaterFilledUnderHalfOfAvailable(t *testing.T) {
	r := availableOf(t, mib(1024), 4)
	equal(t, "G3", r.rounds(4, []uint64{mib(256), mib(256), mib(256), mib(256)}, make([]uint64, 4)), []uint64{mib(128), mib(128), mib(128), mib(128)})
	asks := []uint64{mib(256), mib(256), mib(64), mib(16)}
	equal(t, "G3b", r.rounds(4, asks, make([]uint64, 4)), []uint64{mib(216), mib(216), mib(64), mib(16)})
	equal(t, "G3b buffered 200", r.rounds(4, asks, []uint64{mib(200), 0, 0, 0}), []uint64{mib(216), mib(216), mib(64), mib(16)})
	equal(t, "G3c", r.rounds(4, asks, []uint64{mib(250), 0, 0, 0}), []uint64{mib(250), mib(182), mib(64), mib(16)})

	asks = []uint64{mib(256), mib(256), mib(256), mib(256)}
	r = availableOf(t, mib(2048), 4)
	equal(t, "G3d before", r.rounds(2, asks, make([]uint64, 4)), asks)
	r.live[0] += mib(1024)
	r.tick(FillCover)
	r.fills()
	equal(t, "G3d", r.rounds(4, asks, []uint64{mib(200), mib(200), mib(200), 0}), []uint64{mib(112), mib(112), mib(112), 0})
	if g := r.Granted(); g != mib(600) {
		t.Fatalf("G3d: Σ %v MiB with 600 MiB buffered", inMiB([]uint64{g}))
	}
}

// G4, G5: the grants total at most g_d ÷ (1 + k), which binds before half of available at 80%
// under a 90% write_limit, with no disk write rate.
func TestTheGapCapsGrants(t *testing.T) {
	asks := make([]uint64, 10)
	for i := range asks {
		asks[i] = mib(256)
	}
	r := newGrantRun(t, gb(100), 8*mib(16), 90, 10, gb(80))
	gs := r.rounds(3, asks, make([]uint64, 10))
	equal(t, "G4", gs, asks)
	if lim, gap := r.ctl.limit(r.ctl.poolGrant(1)), gb(100)-gb(100)*90/100-8*mib(16); lim != gap/3 {
		t.Fatalf("G4: the pool's cap is %d blocks, want g_d ÷ 3 = %d", lim, gap/3)
	}
	r = newGrantRun(t, gb(32), 8*mib(4), 90, 10, gb(25.6))
	gs = r.rounds(4, asks, make([]uint64, 10))
	gap := gb(32) - gb(32)*90/100 - 8*mib(4)
	if s := sum(gs); s > gap/3 || s < gap/3-10 || gs[0] < mib(100) || gs[0] > mib(101) {
		t.Fatalf("G5: grants %v MiB, Σ %d blocks against g_d ÷ 3 = %d", inMiB(gs), s, gap/3)
	}
}

// G6, G7, G13: no gap, available 0 or a disk past write_limit leave nothing to grant; a node's
// buffered bytes stay counted (G12c).
func TestNoRoomGrantsOnlyWhatIsBuffered(t *testing.T) {
	for _, c := range []struct {
		name string
		live uint64
	}{{"G7: available 0", gb(36)}, {"G13: a disk at 92%", gb(36.8)}} {
		r := newGrantRun(t, gb(40), 8*mib(16), 90, 1, gb(20))
		r.live[0] = c.live
		r.tick(FillCover)
		r.fills()
		if g := r.beat(0, PoolBeat{Ask: mib(256)}); g != 0 {
			t.Fatalf("%s: granted %v MiB", c.name, inMiB([]uint64{g}))
		}
		if _, cond, ok := r.ctl.Capacity(1); !ok || !cond.Full {
			t.Fatalf("%s: pool not Full", c.name)
		}
		if gap, want := r.ctl.poolGrant(1).gap, gb(40)-c.live-8*mib(16); gap != want {
			t.Fatalf("%s: g_d %d blocks, want %d", c.name, gap, want)
		}
	}
	r := availableOf(t, mib(1024), 2)
	r.rounds(2, []uint64{mib(256), mib(256)}, []uint64{mib(200), 0})
	r.beat(1, PoolBeat{Ask: mib(256), Flushed: gb(40)})
	if g := r.beat(1, PoolBeat{Ask: mib(256), Flushed: gb(40)}); g != 0 {
		t.Fatalf("G6: granted %v MiB with F past g_d", inMiB([]uint64{g}))
	}
	if g := r.beat(0, PoolBeat{Ask: mib(256), Buffered: mib(200), Held: mib(200)}); g != 0 {
		t.Fatalf("G12c: a floor of 200 MiB granted %v MiB", inMiB([]uint64{g}))
	}
	if g := r.Granted(); g < mib(200) {
		t.Fatalf("G12c: Σ %v MiB with 200 MiB buffered", inMiB([]uint64{g}))
	}
}

// askers is the total two askers of 256 MiB, sessions 1 and 2 of r, are granted after n rounds
// in which session 0 asks nothing.
func (r *grantRun) askers(n int) uint64 {
	gs := r.rounds(n, []uint64{0, mib(256), mib(256)}, make([]uint64, 3))
	return gs[1] + gs[2]
}

// G8, G8b: flushes reported since the fills count in F, from cumulative counts so a lost
// heartbeat loses none, until a fill received FillCover after them covers them.
func TestFlushesCountUntilAFillCoversThem(t *testing.T) {
	r := availableOf(t, mib(1024), 3)
	r.beat(0, PoolBeat{Flushed: mib(1000)})
	r.beat(0, PoolBeat{Flushed: mib(1100)})
	if g := r.askers(3); g != mib(462) {
		t.Fatalf("G8: granted Σ %v MiB with 100 MiB flushed, want 462", inMiB([]uint64{g}))
	}
	r.beat(0, PoolBeat{Flushed: mib(1250)})
	if g := r.askers(3); g != mib(387) {
		t.Fatalf("G8b: granted Σ %v MiB after a lost heartbeat, want 387", inMiB([]uint64{g}))
	}
	r.fills()
	if g := r.askers(2); g != mib(387) {
		t.Fatalf("G8: a fill received at once covered the flushes: granted Σ %v MiB", inMiB([]uint64{g}))
	}
	r.tick(FillCover)
	r.fills()
	if g := r.askers(2); g != mib(512) {
		t.Fatalf("G8: granted Σ %v MiB once a fill covers F, want 512", inMiB([]uint64{g}))
	}
}

// G11b: a shrink whose reply is lost counts at the old grant until a heartbeat echoes the new one.
func TestAShrinkCountsUntilEchoed(t *testing.T) {
	r := shrinkRun(t, grantPlants{})
	if g := r.Granted(); g != mib(768) {
		t.Fatalf("G11b: Σ %v MiB with the shrinks unechoed, want 768", inMiB([]uint64{g}))
	}
	for i := range 3 {
		r.beat(i, PoolBeat{Ask: mib(50), Held: mib(50)})
	}
	if g := r.Granted(); g != mib(150) {
		t.Fatalf("G11b: Σ %v MiB once echoed, want 150", inMiB([]uint64{g}))
	}
}

// shrinkRun shrinks three nodes' grants from 256 to 50 MiB, each reply lost, and heartbeats each
// once more still holding 256 MiB.
func shrinkRun(t *testing.T, pl grantPlants) *grantRun {
	r := roomy(t, 3)
	r.ctl.gplant = pl
	asks := []uint64{mib(256), mib(256), mib(256)}
	r.rounds(2, asks, make([]uint64, 3))
	for i := range 3 {
		if g := r.beat(i, PoolBeat{Ask: mib(50), Held: mib(256)}); g != mib(50) {
			t.Fatalf("shrunk to %v MiB", inMiB([]uint64{g}))
		}
	}
	for i := range 3 {
		r.beat(i, PoolBeat{Ask: mib(50), Held: mib(256)})
	}
	return r
}

// A daemon buffers under its old grant until a reply shrinks it, so the heartbeat that echoes the
// shrink may report more buffered than the new grant: the leader counts what it holds buffered.
func TestAnEchoedShrinkCountsWhatIsBuffered(t *testing.T) {
	r := newGrantRun(t, mib(2560), mib(16), 90, 2, mib(1280))
	r.rounds(2, []uint64{mib(80), 0}, make([]uint64, 2))
	r.beat(0, PoolBeat{Held: mib(80)})
	r.beat(1, PoolBeat{Flushed: mib(180)})
	r.beat(0, PoolBeat{Ask: mib(80), Buffered: mib(80)})
	if g := r.Granted(); g < mib(80) {
		t.Fatalf("Σ %v MiB with 80 MiB buffered", inMiB([]uint64{g}))
	}
}

// G12d: a daemon whose successor lost its cumulative count has its old (1 + k) × grant counted in
// F until fills after that heartbeat cover it.
func TestARestartedCountKeepsTheOldBudgetInF(t *testing.T) {
	r := availableOf(t, mib(1024), 3)
	r.beat(0, PoolBeat{Ask: mib(64), Flushed: mib(5000)})
	r.rounds(2, []uint64{mib(64), 0, 0}, make([]uint64, 3))
	r.beat(0, PoolBeat{})
	if g := r.askers(3); g != mib(416) {
		t.Fatalf("G12d: granted Σ %v MiB with 3 × 64 MiB in F, want 416", inMiB([]uint64{g}))
	}
	r.tick(FillCover)
	r.fills()
	if g := r.askers(2); g != mib(512) {
		t.Fatalf("G12d: granted Σ %v MiB once fills cover it", inMiB([]uint64{g}))
	}
}

// G9, G10: a new leader counts every live session at writeback_buffer until it echoes a grant of
// the new leader's, and a fenced session's grant until its fence is acknowledged.
func TestANewLeaderCountsEverySessionAtAFullBuffer(t *testing.T) {
	r := roomy(t, 10)
	asks := make([]uint64, 10)
	for i := range asks {
		asks[i] = mib(10)
	}
	r.rounds(2, asks, make([]uint64, 10))
	r.failover()
	r.hearAll()
	r.tick(FillCover)
	r.fills()
	if g := r.Granted(); g != 10*WritebackBuffer {
		t.Fatalf("G9: the new leader counts Σ %v MiB, want 10 × 256", inMiB([]uint64{g}))
	}
	r.opened()
	r.tick(FillCover)
	r.fills()
	if g := r.beat(0, PoolBeat{Ask: mib(10), Held: mib(10)}); g != mib(10) {
		t.Fatalf("G9: granted %v MiB once fills cover the new leader's lease", inMiB([]uint64{g}))
	}
	if g := r.Granted(); g != 10*WritebackBuffer {
		t.Fatalf("G9: Σ %v MiB once one session renewed, want 10 × 256 until it echoes", inMiB([]uint64{g}))
	}
	r.beat(0, PoolBeat{Ask: mib(10), Held: mib(10)})
	if g := r.Granted(); g != 9*WritebackBuffer+mib(10) {
		t.Fatalf("G9: Σ %v MiB once one session echoed this leader's grant", inMiB([]uint64{g}))
	}

	r = roomy(t, 2)
	r.rounds(2, []uint64{mib(100), mib(100)}, []uint64{0, 0})
	for r.tick(sec); !r.fenced(r.sess[1].id); r.tick(sec) {
		r.beat(0, PoolBeat{Ask: mib(100), Held: mib(100)})
		r.fills()
	}
	if g := r.Granted(); g != mib(200) {
		t.Fatalf("G10: Σ %v MiB with a session fenced and unacknowledged, want 200", inMiB([]uint64{g}))
	}
	for n, nr := range r.reports {
		nr.Sessions = r.ctl.SessionTable().Version
		r.reports[n] = nr
	}
	r.fills()
	if g := r.Granted(); g != mib(100) {
		t.Fatalf("G10: Σ %v MiB once the fence is acknowledged, want 100", inMiB([]uint64{g}))
	}
}

// The old leader's reply granting session 0 80 MiB reaches it after it sent its first heartbeat to
// the new leader, holding nothing, and the new leader's reply to that is lost: the new leader
// counts it at writeback_buffer until it echoes a grant of the new leader's.
func TestALateReplyOfAnEarlierLeaderIsCounted(t *testing.T) {
	r := newGrantRun(t, mib(2560), mib(16), 90, 3, mib(1280))
	r.rounds(2, []uint64{0, 0, 0}, make([]uint64, 3))
	held := r.beat(0, PoolBeat{Ask: mib(80)})
	r.failover()
	r.hearAll()
	r.opened()
	for range 2 {
		r.tick(FillCover)
		r.fills()
	}
	r.beat(0, PoolBeat{})
	gs := make([]uint64, 3)
	for range 4 {
		for i := 1; i < 3; i++ {
			gs[i] = r.beat(i, PoolBeat{Ask: mib(80), Held: gs[i]})
		}
	}
	if land, gap := (1+ReuseBudget)*(held+gs[1]+gs[2]), r.ctl.poolGrant(1).gap; held == 0 || land > gap {
		t.Fatalf("daemons hold %v MiB: they can land %d blocks over a gap of %d", inMiB([]uint64{held, gs[1], gs[2]}), land, gap)
	}
}

// Session 0 holds 80 MiB buffered under the old leader's grant and heartbeats the new leader before
// its first step under its lease. It then flushes them onto disk 1, 10 MiB below its write_limit,
// after the fill that readies the pool was summed, and its heartbeat at the first step is lost. Had
// the new leader answered it with 0, the flushes would be counted nowhere: unanswered, the session
// still holds the old grant, which its next heartbeat adds to F.
func TestAFlushAfterAnUnansweredHeartbeatCountsInF(t *testing.T) {
	blocks := mib(2560)
	r := newGrantRun(t, blocks, mib(16), 90, 3, blocks*90/100-mib(10), blocks/2)
	gs := r.rounds(2, []uint64{0, 0, 0}, make([]uint64, 3))
	gs[0] = r.beat(0, PoolBeat{Ask: mib(80)})
	r.failover()
	r.hearAll()
	r.opened()
	held := gs[0]
	if g, ok := r.send(0, PoolBeat{Ask: mib(80), Buffered: held, Held: held}); ok {
		held = g
	}
	for range 2 {
		r.tick(FillCover)
		r.fills()
	}
	r.beat(0, PoolBeat{Held: held, Flushed: gs[0]})
	var g1, g2 uint64
	for range 4 {
		g1 = r.beat(1, PoolBeat{Ask: mib(80), Held: g1})
		g2 = r.beat(2, PoolBeat{Ask: mib(80), Held: g2})
	}
	if fill, land := r.live[0]+gs[0], (1+ReuseBudget)*(g1+g2); fill+land > blocks-r.reserve {
		t.Fatalf("disk 1 at %v MiB with %v MiB granted can land %v MiB past its top", inMiB([]uint64{fill}), inMiB([]uint64{g1, g2}), inMiB([]uint64{fill + land - (blocks - r.reserve)}))
	}
}

// A leader that loses its lease and regains it starts its grants over, as a new leader does: it
// counts every session at writeback_buffer until it echoes a grant of the regained lease, and
// grants nothing until fills received FillCover after the lease's first step.
func TestARegainedLeaseStartsGrantsOver(t *testing.T) {
	r := roomy(t, 2)
	gs := r.rounds(2, []uint64{mib(64), mib(64)}, make([]uint64, 2))
	before := r.from[0]
	in := r.inputs()
	in.Leased = false
	r.settle(in)
	r.tick(sec)
	r.fills()
	if g := r.beat(0, PoolBeat{Ask: mib(64), Held: gs[0]}); g != 0 || r.from[0] == before {
		t.Fatalf("granted %v MiB before fills cover the regained lease, under %+v", inMiB([]uint64{g}), r.from[0])
	}
	r.tick(FillCover)
	r.fills()
	g := r.beat(0, PoolBeat{Ask: mib(64), Held: gs[0]})
	if s := r.Granted(); g != mib(64) || s != 2*WritebackBuffer {
		t.Fatalf("granted %v MiB, Σ %v MiB before an echo", inMiB([]uint64{g}), inMiB([]uint64{s}))
	}
	r.beat(0, PoolBeat{Ask: mib(64), Held: g})
	if s := r.Granted(); s != WritebackBuffer+mib(64) {
		t.Fatalf("Σ %v MiB once session 0 echoed", inMiB([]uint64{s}))
	}
}

// A Control rebuilt for the same term answers under a newer issuer than the one before it, so a
// daemon takes its answers and no echo of the earlier one's grant matches it.
func TestAControlRebuiltInItsTermIssuesANewerIssuer(t *testing.T) {
	r := roomy(t, 1)
	r.beat(0, PoolBeat{})
	before := r.from[0]
	r.tick(sec)
	r.ctl = r.load(r.l)
	r.fills()
	r.beat(0, PoolBeat{})
	if after := r.from[0]; after.Term != before.Term || after.Start <= before.Start {
		t.Fatalf("issuer %+v after %+v", after, before)
	}
}

// Seed 30 of the grant run: a new leader leaves a heartbeat before its first step under its lease
// unanswered and counts the session at writeback_buffer.
func TestAHeartbeatBeforeTheFirstStepIsUnanswered(t *testing.T) {
	r := roomy(t, 2)
	r.rounds(2, []uint64{mib(256), 0}, make([]uint64, 2))
	r.failover()
	if _, ok := r.send(0, PoolBeat{Ask: mib(256), Held: mib(256)}); ok {
		t.Fatal("answered before the first step")
	}
	r.tick(FillCover)
	r.fills()
	if g := r.Granted(); g != 2*WritebackBuffer {
		t.Fatalf("Σ %v MiB, want 2 × 256", inMiB([]uint64{g}))
	}
}

// BenchmarkSessionHeartbeatWithGrants answers heartbeats of 1,000 sessions asking 64 MiB each of
// one pool, whose gap cap binds, so the water level is searched on every heartbeat.
func BenchmarkSessionHeartbeatWithGrants(b *testing.B) {
	const n = 1000
	r := roomy(&testing.T{}, n)
	asks := make([]uint64, n)
	for i := range asks {
		asks[i] = mib(64)
	}
	gs := r.rounds(2, asks, make([]uint64, n))
	beat := SessionBeat{Issuer: r.ctl.issuer, Pools: []PoolBeat{{Pool: 1, Ask: mib(64)}}}
	a := SessionAnswer{Grants: make([]PoolGrant, 0, 1)}
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		c := r.sess[i%n]
		c.req++
		beat.Session, beat.Request, beat.Pools[0].Held = c.id, c.req, gs[i%n]
		r.ctl.Renew(r.at(), true, &beat, &a)
		gs[i%n] = a.Grants[0].Grant
	}
}

const (
	invGrantFlush = "no flush under a grant is refused for space"
	invGrantRoom  = "what daemons can still land under their grants fits every disk above the higher of its fill and write_limit"
	invGrantCap   = "every nonzero grant keeps (1 + k) × (the others' count + grant) + F within the smallest gap and 2 × (the others' count + grant) + F within available"
)

// grantDisk is a disk in the grant run, in blocks: its size, log reserve and write_limit level,
// its live blocks, and the fill its cleaner frees space down to.
type grantDisk struct{ blocks, reserve, limit, live, target uint64 }

// land lands b blocks on d, a granted flush up to its size less its reserve and a write through
// up to its write_limit level, and reports whether d took them.
func (d *grantDisk) land(b uint64, granted bool) bool {
	top := d.limit
	if granted {
		top = d.blocks - d.reserve
	}
	if d.live+b > top {
		return false
	}
	d.live += b
	return true
}

// grantDaemon is a client daemon in the grant run: its session's heartbeats, the leader it sends
// them to and when it learns a new one, the grant it holds and its issuer, what it holds buffered, its cumulative
// count of blocks flushed and that count at each heartbeat it sent, and the most it writes and
// flushes per step; it asks for a second of its writes. since is the flushed count when it sent the heartbeat whose
// reply set its grant, from which its reuse budget counts; early that it sent its heartbeat at
// half the budget since. A dead daemon is cut off from the leader but not from storage, and an
// idle one writes nothing new.
type grantDaemon struct {
	sessClient
	to                   int
	learn, next          core.Tick
	grant, buffered      uint64
	from                 Issuer
	flushed              uint64
	since                uint64
	sent                 []sentAt
	early, dead, fenced  bool
	idle                 bool
	writeRate, flushRate uint64
}

type sentAt struct{ req, flushed uint64 }

// landable is the most g can still land under its grant: what it holds buffered, or what its
// reuse budget leaves.
func (g *grantDaemon) landable() uint64 {
	used := g.flushed - g.since
	return max(g.buffered, (1+ReuseBudget)*g.grant-min(used, (1+ReuseBudget)*g.grant))
}

// buffer writes w blocks: buffered while within its grant and its reuse budget, through to d
// otherwise.
func (g *grantDaemon) buffer(w uint64, d *grantDisk) {
	if g.buffered+w <= g.grant && g.flushed-g.since+g.buffered+w <= (1+ReuseBudget)*g.grant {
		g.buffered += w
		return
	}
	d.land(w, false)
}

// grantMsg is a session heartbeat to control-plane node, its answer, or a disk's fill from node.
type grantMsg struct {
	at     core.Tick
	daemon int
	node   int
	kind   int
	beat   SessionBeat
	ans    SessionAnswer
	fill   uint64
}

const (
	grantBeat = iota
	grantReply
	grantFill
)

// grantsRun runs 6 to 10 daemons buffering, flushing and heartbeating against pool 1 on 1 to 3
// small disks for 60 s, tight with heavier writers, 8 GiB disks and more deaths, through leader failovers with the old leader answering a while, lost and
// delayed heartbeats and fills, daemons cut off from the leader that stream under their grants
// until fenced, and space freed, checking the invariants after every 10 ms step.
func grantsRun(t *testing.T, seed uint64, tight bool, pl grantPlants, only ...string) error {
	const step = core.Tick(10_000)
	const faultsEnd, end = 40 * sec, 60 * sec
	rng := rand.New(rand.NewPCG(seed, 9))
	ticks := func(n int) core.Tick { return core.Tick(rng.IntN(n+1)) * step }

	r := newCtlRun(t)
	nd := 1 + rng.IntN(3)
	ds := make([]grantDisk, nd)
	in := ControlInputs{Leased: true}
	for i := range ds {
		d := &ds[i]
		d.blocks = mib(8192) << rng.IntN(5) * (100 + rng.Uint64N(100)) / 100
		if tight {
			d.blocks = mib(8192)
		}
		d.reserve = 8 * mib(4)
		d.limit = d.blocks * 90 / 100
		d.live = d.blocks * (60 + rng.Uint64N(29)) / 100
		d.target = d.live
		in.Disks = append(in.Disks, chunkstore.Disk{ID: uint32(i) + 1, Node: uint32(i), Blocks: d.blocks, Live: d.live})
	}
	deathOdds := 3000
	if tight {
		deathOdds = 300
	}
	gs := make([]*grantDaemon, 6+rng.IntN(5))
	for i, c := range r.startSessions(len(gs)) {
		gs[i] = &grantDaemon{sessClient: *c, to: r.l, next: ticks(100), writeRate: rng.Uint64N(mib(3)), flushRate: 1 + rng.Uint64N(mib(4))}
		if tight {
			gs[i].writeRate += mib(3)
		}
	}
	one, limit := 1, uint8(90)
	if err := r.ctl.SetPool(1, chunkstore.PoolSpec{Replicas: &one, MinLive: &one, WriteLimit: &limit, PGCount: new(uint32(8))}, in.Disks); err != nil {
		t.Fatal(err)
	}
	r.ctl.gplant = pl
	var now core.Tick
	step1 := func(leased bool) {
		r.now, r.raw = now, now
		in.Now, in.Raw, in.Leased = now, now, leased
		r.settle(&in)
	}
	step1(true)
	var on []int
	j, _ := r.ctl.tablePool(1)
	for _, e := range r.ctl.table.Pools[j].PGs {
		if i := int(e.Members[0].Disk) - 1; !slices.Contains(on, i) {
			on = append(on, i)
		}
	}
	hot := on[0]
	pick := func() *grantDisk {
		if rng.IntN(4) != 0 {
			return &ds[hot]
		}
		return &ds[on[rng.IntN(len(on))]]
	}

	var leaseFrom core.Tick
	var old *Control
	oldNode, oldTo := -1, core.Tick(0)
	var held [5]uint64
	var refused, capErr error
	deaths := 0

	var inv sim.Invariants
	inv.Register(invGrantFlush, func() error { return refused })
	inv.Register(invGrantRoom, func() error {
		var sum uint64
		for _, g := range gs {
			if !g.fenced && slices.ContainsFunc(r.ctl.SessionTable().Sessions, func(e SessionEntry) bool { return e.ID == g.id }) {
				sum += g.landable()
			}
		}
		for _, i := range on {
			if d := &ds[i]; max(d.live, d.limit)+sum > d.blocks-d.reserve {
				return fmt.Errorf("disk %d at %d of %d, limit %d, reserve %d: daemons can land %d at %v", i+1, d.live, d.blocks, d.limit, d.reserve, sum, now)
			}
		}
		return nil
	})
	inv.Register(invGrantCap, func() error { return capErr })
	for _, name := range only {
		inv.Only(name)
	}

	var msgs, due []grantMsg
	nextFail := 5*sec + ticks(500)
	for now = step; now < end; now += step {
		faults := now < faultsEnd
		r.now, r.raw = now, now
		if old != nil && now >= oldTo {
			old, oldNode = nil, -1
		}
		if faults && now >= nextFail && old == nil && now >= leaseFrom {
			nextFail = now + 3*sec + ticks(800)
			oldTo = now
			if rng.IntN(2) == 0 {
				old, oldNode, oldTo = r.ctl, r.l, now+ticks(300)
			}
			r.failover()
			r.ctl.gplant = pl
			leaseFrom = oldTo + ticks(100)
			for _, g := range gs {
				g.learn = now + ticks(300)
			}
		}
		if faults && deaths < 2 && rng.IntN(deathOdds) == 0 {
			g := gs[rng.IntN(len(gs))]
			g.dead, g.idle = true, rng.IntN(2) == 0
			deaths++
		}

		due, msgs = append(due[:0], msgs...), msgs[:0]
		for _, m := range due {
			if m.at > now {
				msgs = append(msgs, m)
				continue
			}
			switch m.kind {
			case grantFill:
				r.event(uint32(m.node), chunkstore.AppendReport(nil, &chunkstore.Report{ID: 1, Disks: []chunkstore.DiskState{{Log: logOf(uint32(m.node)), Fill: m.fill}}}))
			case grantReply:
				g := gs[m.daemon]
				if g.dead || g.fenced || m.ans.Request <= g.answered || cmp.Or(cmp.Compare(m.ans.Issuer.Term, g.from.Term), cmp.Compare(m.ans.Issuer.Start, g.from.Start)) < 0 {
					continue
				}
				g.answered, g.arrived = m.ans.Request, now
				if m.ans.Status == SessionFenced {
					g.fenced, g.buffered = true, 0
					continue
				}
				i := slices.IndexFunc(g.sent, func(s sentAt) bool { return s.req == m.ans.Request })
				g.grant, g.since, g.early, g.from = m.ans.Grants[0].Grant, g.sent[i].flushed, false, m.ans.Issuer
				g.sent = g.sent[i+1:]
			case grantBeat:
				a := SessionAnswer{Grants: make([]PoolGrant, 0, 1)}
				ok := false
				switch m.node {
				case r.l:
					ok = r.ctl.Renew(r.at(), now >= leaseFrom, &m.beat, &a)
					if ok && a.Grants[0].Grant > 0 && capErr == nil {
						pg, sum := r.ctl.poolGrant(1), r.ctl.counted(1, m.beat.Session)+a.Grants[0].Grant
						if f := pg.flushed(); (1+ReuseBudget)*sum+f > pg.gap || 2*sum+f > pg.room {
							capErr = fmt.Errorf("others and grant %d, F %d: g_d %d, available %d at %v", sum, f, pg.gap, pg.room, now)
						}
					}
				case oldNode:
					ok = old.Renew(lease.Time{Boot: now, Raw: now}, true, &m.beat, &a)
				}
				if ok && rng.IntN(20) != 0 {
					msgs = append(msgs, grantMsg{at: now + ticks(3), daemon: m.daemon, kind: grantReply, ans: a})
				}
			}
		}

		for i, g := range gs {
			if g.fenced {
				continue
			}
			if !g.idle && (int(now/sec)+i)%4 != 0 {
				g.buffer(rng.Uint64N(g.writeRate+1), pick())
			}
			if f := min(g.buffered, g.flushRate); f > 0 {
				g.buffered -= f
				g.flushed += f
				if !slices.ContainsFunc(r.ctl.SessionTable().Sessions, func(e SessionEntry) bool { return e.ID == g.id }) {
					continue
				}
				if d := pick(); !d.land(f, true) && refused == nil {
					refused = fmt.Errorf("daemon %d's flush of %d blocks on a disk at %d of %d less %d at %v", i, f, d.live, d.blocks, d.reserve, now)
				}
			}
			if now >= g.learn {
				g.to = r.l
			}
			early := !g.early && g.grant > 0 && g.flushed-g.since >= g.grant
			if now < g.next && !early {
				continue
			}
			if early {
				g.early = true
			} else {
				g.next = now + sec
			}
			g.req++
			g.sent = append(g.sent, sentAt{g.req, g.flushed})
			pb := PoolBeat{Pool: 1, Ask: min(WritebackBuffer, g.buffered+100*g.writeRate), Buffered: g.buffered, Held: g.grant, Flushed: g.flushed}
			if !g.dead && rng.IntN(20) != 0 {
				msgs = append(msgs, grantMsg{at: now + ticks(3), daemon: i, node: g.to, kind: grantBeat, beat: SessionBeat{Session: g.id, Request: g.req, Sent: now, Answered: g.answered, Arrived: g.arrived, Issuer: g.from, Pools: []PoolBeat{pb}}})
			}
		}

		if now%(5*sec) == 0 {
			hot = on[rng.IntN(len(on))]
		}
		if now%sec == 0 {
			for i := range ds {
				d := &ds[i]
				if now%(10*sec) == 0 {
					d.target = d.blocks * (60 + rng.Uint64N(41)) / 100
				}
				if d.live > d.target && rng.IntN(2) == 0 {
					d.live -= rng.Uint64N(d.live - d.target + 1)
				}
				if !faults || rng.IntN(10) != 0 {
					msgs = append(msgs, grantMsg{at: now + ticks(30), node: i, kind: grantFill, fill: d.live})
				}
			}
			for n := range uint32(5) {
				if !faults || rng.IntN(3) == 0 {
					held[n] = r.ctl.SessionTable().Version
				}
				nr := NodeReport{Node: n, Meta: true, Opened: []OpenedDisk{{Device: devOf(n), Log: logOf(n)}}, Sessions: held[n]}
				if int(n) < nd {
					nr.Opened[0].Blocks, nr.Opened[0].Reserve = ds[n].blocks, ds[n].reserve
				}
				r.heartbeat(&nr)
			}
		}
		if now%(sec/10) == 0 {
			step1(now >= leaseFrom)
		}
		if err := inv.Check(); err != nil {
			return fmt.Errorf("seed %d: %w", seed, err)
		}
	}
	return nil
}

// No flush under a grant is refused for space, and no reply grants past (1 + k) × (the others'
// count + grant) + F ≤ g_d or 2 × (the others' count + grant) + F ≤ available: through failovers
// with the old leader answering a while, lost and delayed heartbeats and fills, daemons cut off
// from the leader streaming under their grants until their fences are acknowledged, and space
// freed, each seed run as it is and tight.
func TestNoGrantedFlushIsRefusedForSpace(t *testing.T) {
	seeds.Run(t, 0, 20, func(seed uint64) {
		for _, tight := range []bool{false, true} {
			if err := grantsRun(t, seed, tight, grantPlants{}); err != nil {
				t.Fatal(err)
			}
		}
	})
}

// Seed 102 of the tight grant run: a reply that renews a node's buffered floor above the room
// left under the cap opens a reuse window of (1 + k) × the floor, and the daemons land past g_d.
func TestAReplyNeverGrantsPastTheCap(t *testing.T) {
	if err := grantsRun(t, 102, true, grantPlants{}); err != nil {
		t.Fatal(err)
	}
}

// lapseRun has session 0 hold its grant on a disk whose gap is three times the pool's cap, then
// lapse with its fence unacknowledged while sessions 1 and 2 ask, and reports what the three can
// still land past the gap as the grant run's invGrantRoom.
func lapseRun(t *testing.T, pl grantPlants) error {
	r := newGrantRun(t, mib(2560), mib(16), 90, 3, mib(1280))
	r.ctl.gplant = pl
	gs := r.rounds(3, []uint64{mib(80), 0, 0}, make([]uint64, 3))
	for r.tick(sec); !r.fenced(r.sess[0].id); r.tick(sec) {
		for i := 1; i < 3; i++ {
			gs[i] = r.beat(i, PoolBeat{Ask: mib(80), Held: gs[i]})
		}
		r.fills()
	}
	for range 3 {
		for i := 1; i < 3; i++ {
			gs[i] = r.beat(i, PoolBeat{Ask: mib(80), Held: gs[i]})
		}
	}
	if land, gap := (1+ReuseBudget)*sum(gs), r.ctl.poolGrant(1).gap; land > gap {
		return fmt.Errorf("invariant %q: grants %v MiB can land %d blocks over a gap of %d", invGrantRoom, inMiB(gs), land, gap)
	}
	return nil
}

// Each planted counting bug fails an invariant: a lapsed session's grant dropped before its fence
// is acknowledged, which the grant run's disks have too much slack to show, and a new leader
// counting a session at no more than it says it holds, at seed 4 of the tight run.
func TestPlantedGrantBugsOverrun(t *testing.T) {
	if err := lapseRun(t, grantPlants{}); err != nil {
		t.Fatal(err)
	}
	if err := lapseRun(t, grantPlants{dropLapsed: true}); err == nil || !strings.Contains(err.Error(), invGrantRoom) {
		t.Errorf("no lapsed count, lapse while others ask: %v", err)
	}
	if err := grantsRun(t, 4, true, grantPlants{unheardZero: true}); err == nil || !strings.Contains(err.Error(), invGrantRoom) {
		t.Errorf("no failover count at seed 4 of the tight run: %v", err)
	}
	if !grantRoomBreaksWithin(t, 6, grantPlants{noGapCap: true}) {
		t.Error("grants capped by half of available only broke nothing in 6 seeds of the tight run")
	}
	if g := shrinkRun(t, grantPlants{shrinkAtOnce: true}).Granted(); g != mib(150) {
		t.Errorf("a shrink counted at once counted %v MiB, want the three shrunk grants' 150", inMiB([]uint64{g}))
	}
}

// grantRoomBreaksWithin reports whether the tight grant run with pl breaks invGrantRoom in one of
// the first n seeds, n about 5 / the plant's rate over 500 seeds.
func grantRoomBreaksWithin(t *testing.T, n uint64, pl grantPlants) bool {
	for seed := range n {
		if err := grantsRun(t, seed, true, pl, invGrantRoom); err != nil && strings.Contains(err.Error(), invGrantRoom) {
			return true
		}
	}
	return false
}
