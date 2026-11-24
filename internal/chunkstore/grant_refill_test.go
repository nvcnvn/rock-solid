package chunkstore

import (
	"bytes"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// P20's open case on the real control plane: four disks of 52 MiB, each a 2 MiB log reserve and
// about 85% full of a rep3 pool's four PGs, all under a 90% write_limit. A daemon's session holds
// a grant the leader sized to the smallest gap, then a disk is lost and its PGs refill onto the
// other three, whose recovery passes write_limit, as it may, and fills each log to its reserve.
// The daemon then flushes what it buffered under its grant, and the full disks hold it, waiting for room:
// recovery takes the room the grant was promised, which neither the leader's gap nor the
// replicas' admission counts. This pins that behaviour until recovery leaves the grants' room.
func TestARefillTakesTheRoomOfTheGrantsOut(t *testing.T) {
	if refused, g := grantRefill(t, false); refused != 0 {
		t.Fatalf("with no disk lost, %d of %d granted flushes of 16 blocks did not land", refused, g/16)
	}
	refused, g := grantRefill(t, true)
	if refused == 0 {
		t.Fatalf("every granted flush of %d blocks landed after the refill", g)
	}
	t.Logf("%d of %d granted flushes of 16 blocks did not land after the refill", refused, g/16)
}

// grantRefill fills the disks, takes a grant, loses disk 4 if refill and waits out its PGs' recovery, then
// flushes the grant 16 blocks at a time, returning how many flushes did not land within 2 s and the grant.
func grantRefill(t *testing.T, refill bool) (int, uint64) {
	const segs, reserve = 200, 8
	a := &authority{pool: Pool{Replicas: 3, MinLive: 2, FailureDomain: DomainNode, MaxPerDomain: 1, Locality: LocalitySpread, Overcommit: true, WriteLimit: 60, PGCount: 4},
		down: map[uint32]core.Tick{}, drain: map[uint32]bool{}, opened: map[uint32][]OpenedLog{}}
	geo, err := disklog.GeometryFor(diskBytes)
	if err != nil {
		t.Fatal(err)
	}
	a.size = OpenedLog{Blocks: (segs + reserve) * uint64(geo.SegmentBlocks), Reserve: reserve * uint64(geo.SegmentBlocks)}
	tbl := realControl(t, a, 4)
	c := newRClusterWith(t, tbl, sim.New(1, sim.DefaultSettings()), func(cfg *Config) { cfg.Log.NoReserve, cfg.Log.Reserve = false, geo.SegmentCount-segs })
	a.attach(c)
	r := &soleRun{c: c, a: a, isolated: map[uint32]bool{}, acked: map[core.ChunkID]uint64{}}
	for n := range c.reps {
		c.call(n, func(ctx *core.Context, rep *Replica, out *ReplicaOutput) {
			rep.SetPool(testPool, Pool{WriteLimit: 60})
			for d := uint32(1); d <= 4; d++ {
				rep.SetDisk(d, a.size.Blocks)
			}
		})
	}
	most := func() uint64 {
		var m uint64
		for n := range c.reps {
			if r.isolated[c.ids[n]] {
				continue
			}
			m = max(m, c.reps[n].Store().Log().LiveBlocks())
		}
		return m
	}
	data := bytes.Repeat([]byte{1}, 16*core.BlockSize)
	k := uint32(0)
	for ; most() < a.size.Blocks*55/100; k++ {
		pg := core.PGID{Pool: testPool, Index: k % 4}
		ch := chunkIn(pg, 4, k/4)
		for b := range uint32(4) {
			if id := c.write(pg, ch, b*16*core.BlockSize, data); c.status(id) != StatusOK {
				var free []int
				for n := range c.reps {
					free = append(free, c.reps[n].Store().Log().FreeOutsideReserve())
				}
				t.Fatalf("filling write %d answered %v at %d blocks, free %v", k, c.reply(id).Status, most(), free)
			}
		}
	}
	r.tick(3_000_000)

	renew := func(ask, held, buffered uint64) uint64 {
		now, raw := a.now()
		g, ok := a.cp.Renew(now, raw, testPool, ask, held, buffered, 0)
		if !ok {
			t.Fatalf("the session heartbeat at %d s went unanswered", c.s.Time()/1_000_000)
		}
		for n := range c.reps {
			if !r.isolated[c.ids[n]] {
				c.call(n, func(ctx *core.Context, rep *Replica, out *ReplicaOutput) {
					rep.SetGrants(testPool, a.cp.Granted(testPool))
				})
			}
		}
		return g
	}
	renew(256, 0, 0)
	g := renew(256, 0, 0)
	if g == 0 || g%16 != 0 {
		t.Fatalf("granted %d blocks with the disks at %d of %d", g, most(), a.size.Blocks)
	}
	g = g / 16 * 16

	lost := uint32(4)
	if refill {
		r.lose(lost)
	}
	for i := 0; refill && r.lost(lost) != nil; i++ {
		if i == 30 {
			t.Fatalf("disk %d never declared lost", lost)
		}
		r.tick(1_000_000)
		renew(g, g, g)
	}
	at := c.s.Time()
	for live := uint64(0); c.s.Time()-at < 5_000_000; {
		r.tick(1_000_000)
		renew(g, g, g)
		if l := most(); l != live {
			live, at = l, c.s.Time()
		}
	}

	refused := 0
	for i := range g / 16 {
		pg := core.PGID{Pool: testPool, Index: uint32(i) % 4}
		id := c.update(c.pgFields(pg), chunkIn(pg, 4, k+uint32(i)), 0, data, 0, func(u *Update) { u.Granted = true })
		if rp, ok := r.answered(id, 2_000_000); !ok || rp.Status != StatusOK {
			refused++
		}
	}
	return refused, g
}
