package chunkstore

import (
	"bytes"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

const invGrantedLands = "no granted flush is refused or held"

// grantSpan is the blocks of each fill write and granted flush: with its record header, three fill
// a 64-block segment past its header, as a chunk's 60 blocks do in one recovery record.
const grantSpan = 20

// grantRoom is P20's cluster on the real control plane: four disks, each filled to a given share of
// its size with a rep3 pool's four PGs under a 71% write_limit, and a daemon's session holding the grant the leader sized
// to the disks' room. Each log has 200 segments outside its reserve; the disks report as their
// size below the reserve the live blocks those segments hold, less six lanes' open segments, since
// the leader and the replicas count live blocks and a 64-block segment's headers are 6% of it.
type grantRoom struct {
	a     *authority
	c     *rcluster
	r     *soleRun
	lim   *limitRun
	g     uint64
	data  []byte
	t     *testing.T
	stuck bool
}

// newGrantRoom builds the cluster on seed's scheduler, plant changing it before its nodes start,
// fills its fullest disk to permille thousandths of its size, and takes the grant.
func newGrantRoom(t *testing.T, seed, permille uint64, plant func(*authority, *rcluster)) *grantRoom {
	const segs, reserve, lanes, limit = 200, 24, 6, 71
	geo, err := disklog.GeometryFor(diskBytes)
	if err != nil {
		t.Fatal(err)
	}
	a := &authority{pool: Pool{Replicas: 3, MinLive: 2, FailureDomain: DomainNode, MaxPerDomain: 1, Locality: LocalitySpread, Overcommit: true, WriteLimit: limit, PGCount: 4},
		down: map[uint32]core.Tick{}, drain: map[uint32]bool{}, opened: map[uint32][]OpenedLog{}}
	a.size = OpenedLog{Blocks: reserve*uint64(geo.SegmentBlocks) + (segs-lanes)*60, Reserve: reserve * uint64(geo.SegmentBlocks)}
	tbl := realControl(t, a, 4)
	c := newRClusterOnDisks(t, tbl, sim.New(seed, sim.DefaultSettings()), func(cfg *Config) { cfg.Log.NoReserve, cfg.Log.Reserve = false, geo.SegmentCount-segs }, geo)
	if plant != nil {
		plant(a, c)
	}
	a.attach(c)
	x := &grantRoom{a: a, c: c, r: &soleRun{c: c, a: a, isolated: map[uint32]bool{}, acked: map[core.ChunkID]uint64{}}, data: bytes.Repeat([]byte{1}, grantSpan*core.BlockSize), t: t}
	x.lim = &limitRun{c: c, pct: limit, reserve: a.size.Reserve, blocks: make([]uint64, len(c.reps)), boots: make([]int, len(c.reps)),
		seen: map[[3]uint64]bool{}, past: map[[3]uint64]bool{}, over: map[verKey]bool{}, live: make([]uint64, len(c.reps)), mapped: make([][]uint64, len(c.reps))}
	for n := range c.reps {
		x.lim.blocks[n] = a.size.Blocks
	}
	for n := range c.reps {
		x.lim.setLimits(n)
	}
	k := uint32(0)
	for ; x.most() < a.size.Blocks*permille/1000; k++ {
		pg := core.PGID{Pool: testPool, Index: k % 4}
		ch := chunkIn(pg, 4, k/4)
		for b := range uint32(3) {
			if id := c.write(pg, ch, b*grantSpan*core.BlockSize, x.data); c.status(id) != StatusOK {
				t.Fatalf("filling write %d answered %v at %d blocks", k, c.reply(id).Status, x.most())
			}
		}
	}
	for i := range uint32(flushChunks + 16) {
		x.lim.chunks = append(x.lim.chunks, chunkIn(core.PGID{Pool: testPool, Index: i % 4}, 4, k/4+1+i/4))
	}
	for n := range c.reps {
		x.lim.mapped[n] = make([]uint64, len(x.lim.chunks))
	}
	x.r.tick(3_000_000)
	x.renew(1 << 20)
	if x.g = x.renew(1<<20) / grantSpan * grantSpan; x.g == 0 || (1+ReuseBudget)*x.g > flushChunks*60 {
		t.Fatalf("granted %d blocks with the disks at %d of %d", x.g, x.most(), a.size.Blocks)
	}
	return x
}

// most is the most live blocks a disk not lost holds.
func (x *grantRoom) most() uint64 {
	var m uint64
	for n := range x.c.reps {
		if !x.r.isolated[x.c.ids[n]] {
			m = max(m, x.c.reps[n].Store().Log().LiveBlocks())
		}
	}
	return m
}

// renew sends the session's heartbeat asking for ask, holding x.g and as much buffered, hands every
// replica not lost the pool's grants outstanding, as the leader's heartbeat replies carry them, and
// notes whether the pool is Stuck.
func (x *grantRoom) renew(ask uint64) uint64 {
	now, raw := x.a.now()
	g, ok := x.a.cp.Renew(now, raw, testPool, ask, x.g, x.g, 0)
	if !ok {
		x.t.Fatalf("the session heartbeat at %d s went unanswered", x.c.s.Time()/1_000_000)
	}
	for n := range x.c.reps {
		if !x.r.isolated[x.c.ids[n]] {
			x.c.call(n, func(ctx *core.Context, rep *Replica, out *ReplicaOutput) {
				rep.SetGrants(testPool, x.a.cp.Granted(testPool))
			})
		}
	}
	cond, ok := x.a.cp.Conditions(testPool)
	x.stuck = x.stuck || ok && len(cond.Stuck) > 0
	return g
}

// flushChunks is how many of the run's chunks take granted flushes, each new blocks, before the
// 16 that take plain writes.
const flushChunks = 48

// flushTo is where granted flush j goes: its chunk and block.
func flushTo(j uint64) (int, uint32) { return int(j % flushChunks), uint32(j/flushChunks) * grantSpan }

// flush sends a write of grantSpan blocks at block b of chunk i of the run's chunks, a flush under
// the grant if granted.
func (x *grantRoom) flush(i int, b uint32, granted bool) core.RequestID {
	ch := x.lim.chunks[i]
	pg := core.PGID{Pool: testPool, Index: uint32(i) % 4}
	return x.c.update(x.c.pgFields(pg), ch, b*core.BlockSize, x.data, 0, func(u *Update) { u.Granted = granted })
}

// P20 on the real control plane: a disk is lost and its PGs refill onto the other three, past
// write_limit as recovery may, until each target would no longer hold its log reserve plus (1 + k)
// × the grants outstanding; the copying stops there and the leader raises Stuck. Every flush the
// grant covers, its k reuses included, then lands.
func TestARefillLeavesTheRoomOfTheGrantsOut(t *testing.T) {
	for _, refill := range []bool{false, true} {
		if landed, flushes, stuck := grantRefill(t, refill, nil); landed != flushes || refill && !stuck {
			t.Fatalf("refill %v: %d of %d granted flushes landed, Stuck %v", refill, landed, flushes, stuck)
		}
	}
}

// The grants' room needs both the replicas' stop and the leader's view of the log reserve.
func TestARefillPlantedPastTheGrantsTakesTheirRoom(t *testing.T) {
	for _, name := range slices.Sorted(maps.Keys(grantPlants)) {
		plant := grantPlants[name]
		if landed, flushes, _ := grantRefill(t, true, plant); landed == flushes {
			t.Errorf("%s: every one of %d granted flushes landed", name, flushes)
		}
	}
}

var grantPlants = map[string]func(*authority, *rcluster){
	"copy past the grants": func(a *authority, c *rcluster) { plantAll(c, func(p *replicaPlants) { p.copyPastGrants = true }) },
	"reserve reported 0":   func(a *authority, c *rcluster) { a.size.Reserve = 0 },
}

// grantRefill loses disk 4 if refill and waits out its PGs' recovery and Stuck, then flushes
// (1 + k) × the grant, one after another. The disks start 60% full, which the leader grants 760
// blocks against, so that k × the grant exceeds the slack a whole chunk's budget per copy leaves. It returns how many flushes landed, each within 2 s,
// before the first that did not, how many the grant covers, and whether the pool went Stuck.
func grantRefill(t *testing.T, refill bool, plant func(*authority, *rcluster)) (uint64, uint64, bool) {
	x := newGrantRoom(t, 1, 600, plant)
	lost := uint32(4)
	if refill {
		x.r.lose(lost)
	}
	for i := 0; refill && x.r.lost(lost) != nil; i++ {
		if i == 30 {
			t.Fatalf("disk %d never declared lost", lost)
		}
		x.r.tick(1_000_000)
		x.renew(x.g)
	}
	for at := x.c.s.Time(); refill && !x.stuck && x.c.s.Time()-at < StuckAfter+30_000_000; {
		x.r.tick(1_000_000)
		x.renew(x.g)
	}
	flushes := (1 + ReuseBudget) * x.g / grantSpan
	for i := range flushes {
		ci, b := flushTo(i)
		if rp, ok := x.r.answered(x.flush(ci, b, true), 2_000_000); !ok || rp.Status != StatusOK {
			return i, flushes, x.stuck
		}
	}
	return flushes, flushes, x.stuck
}

// runGrantRoom is P20 seeded: plain writes and granted flushes to the run's chunks, a random disk
// lost and declared lost, and more granted flushes while its PGs refill, (1 + k) × the grant in
// all, then Stuck awaited. It fails t on a broken invariant registered with the simulator; with an
// invariant to plant against, plant changes the cluster and the run checks that invariant alone,
// returning its first violation.
func runGrantRoom(t *testing.T, seed uint64, plant func(*authority, *rcluster), only string) error {
	x := newGrantRoom(t, seed, 685, plant)
	c := x.c
	var held error
	c.inv.Register(invWriteLimit, func() error { return x.lim.err })
	c.inv.Register(invGrantedLands, func() error { return held })
	if only != "" {
		c.planted = true
		c.inv.Only(only)
	}
	c.onEvent = x.lim.onEvent
	rng := core.NewRand(seed ^ 0x6a7a)
	type flushAt struct {
		id core.RequestID
		at core.Tick
		i  int
		b  uint32
	}
	var sent []flushAt
	budget, j := (1+ReuseBudget)*x.g/grantSpan, uint64(0)
	early := budget / 2
	send := func(plain bool) {
		if plain {
			x.flush(flushChunks+int(rng.Below(16)), uint32(rng.Below(60-grantSpan+1)), false)
			return
		}
		i, b := flushTo(j)
		sent = append(sent, flushAt{x.flush(i, b, true), c.s.Time(), i, b})
		budget, j = budget-1, j+1
	}
	check := func() {
		for k := range sent {
			if rp, ok := c.cl.replies[sent[k].id]; ok && rp.Status == StatusRefused {
				sent[k].id, sent[k].at = x.flush(sent[k].i, sent[k].b, true), c.s.Time()
			}
		}
		sent = slices.DeleteFunc(sent, func(f flushAt) bool {
			rp, ok := c.cl.replies[f.id]
			switch {
			case ok && rp.Status != StatusOK && held == nil:
				held = fmt.Errorf("a granted flush answered %v at %d ms", rp.Status, c.s.Time()/1000)
			case !ok && c.s.Time()-f.at >= 2_000_000 && held == nil:
				held = fmt.Errorf("a granted flush unanswered for 2 s at %d ms", c.s.Time()/1000)
			}
			return ok || held != nil
		})
	}
	run := func(d core.Tick, each func()) error {
		for end := c.s.Time() + d; c.s.Time() < end; {
			c.runFor(100_000)
			if c.s.Time()%1_000_000 < 100_000 {
				x.r.publish()
				x.renew(x.g)
			}
			each()
			check()
			if c.found != nil {
				return c.found
			}
		}
		return nil
	}
	quiet := func() {}
	if err := run(core.Tick(2_000_000+rng.Below(3_000_000)), func() {
		if rng.Below(2) == 0 {
			send(true)
		}
		if j < early && rng.Below(4) == 0 {
			send(false)
		}
	}); err != nil {
		return err
	}
	if err := run(2_000_000, quiet); err != nil {
		return err
	}
	lost := 1 + uint32(rng.Below(4))
	x.r.lose(lost)
	for i := 0; x.r.lost(lost) != nil; i++ {
		if i == 30 {
			t.Fatalf("seed %d: disk %d never declared lost", seed, lost)
		}
		if err := run(1_000_000, quiet); err != nil {
			return err
		}
	}
	if err := run(1_000_000, quiet); err != nil {
		return err
	}
	if err := run(10_000_000, func() {
		if j < early && rng.Below(3) == 0 {
			send(false)
		}
	}); err != nil {
		return err
	}
	for at := c.s.Time(); !x.stuck && c.s.Time()-at < StuckAfter+20_000_000; {
		if err := run(1_000_000, quiet); err != nil {
			return err
		}
	}
	for budget > 0 {
		for budget > 0 && len(sent) < 8 {
			send(false)
		}
		if err := run(100_000, quiet); err != nil {
			return err
		}
	}
	if err := run(5_000_000, quiet); err != nil {
		return err
	}
	if only == "" && !x.stuck {
		t.Fatalf("seed %d: the refill after disk %d was lost never went Stuck, %d live blocks at most", seed, lost, x.most())
	}
	return c.inv.Check()
}

// P20 seeded: a refill never takes the room of the grants outstanding, so no granted flush is
// refused or held, and a plain write is never admitted onto a disk whose fill with its share of the
// grants has reached write_limit.
func TestARefillLeavesTheGrantsTheirRoomOnTheRealGroup(t *testing.T) {
	seeds.Run(t, 0, 10, func(seed uint64) {
		if err := runGrantRoom(t, seed, nil, ""); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
	})
}

// Each plant breaks its invariant at seed 0: copying into the grants' room or a reserve reported as
// 0 holds a granted flush, and an admission that leaves out the grants passes write_limit.
func TestPlantedGrantRoomBugsBreakTheRefillRun(t *testing.T) {
	for _, name := range slices.Sorted(maps.Keys(grantPlants)) {
		plant := grantPlants[name]
		if err := runGrantRoom(t, 0, plant, invGrantedLands); err == nil || !strings.Contains(err.Error(), invGrantedLands) {
			t.Errorf("%s at seed 0: %v", name, err)
		}
	}
	uncounted := func(a *authority, c *rcluster) { plantAll(c, func(p *replicaPlants) { p.grantsUncounted = true }) }
	if err := runGrantRoom(t, 0, uncounted, invWriteLimit); err == nil || !strings.Contains(err.Error(), invWriteLimit) {
		t.Errorf("grants uncounted at seed 0: %v", err)
	}
}
