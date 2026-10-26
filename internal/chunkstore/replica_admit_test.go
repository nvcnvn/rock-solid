package chunkstore

import (
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

// admitCluster is pg0 led by disk 1 under a pool write_limit of limit, every disk 1,000 blocks; disk 3 (X) reports fill blocks.
func admitCluster(t *testing.T, limit uint8) *rcluster {
	c := newRCluster(t, rtable(4, 2))
	c.call(0, func(ctx *core.Context, r *Replica, out *ReplicaOutput) {
		r.SetPool(testPool, Pool{WriteLimit: limit})
		for d := range uint32(3) {
			r.SetDisk(d+1, 1000)
		}
	})
	return c
}

func (c *rcluster) xFills(live uint64) {
	c.call(0, func(ctx *core.Context, r *Replica, out *ReplicaOutput) { r.MemberFill(3, live) })
}

func (c *rcluster) status(id core.RequestID) Status {
	c.t.Helper()
	c.settle()
	return c.reply(id).Status
}

func TestWriteLimitAdmission(t *testing.T) {
	c := admitCluster(t, 90)
	ch := chunkIn(pg0, 4, 0)
	c.writeWhole(pg0, ch, 3, 2, 0xaa)
	if live := c.reps[0].disks[2].live; live == 0 || live != c.reps[2].Store().Log().LiveBlocks() {
		t.Fatalf("the primary holds X's fill as %d from its acknowledgment, X counts %d", live, c.reps[2].Store().Log().LiveBlocks())
	}

	c.xFills(910)
	if st := c.status(c.write(pg0, ch, 0, []byte{1})); st != StatusNoSpace {
		t.Fatalf("a write adding live data with X at 91%%: %v", st)
	}
	c.xFills(910)
	if st := c.status(c.update(c.pgFields(pg0), ch, 0, []byte{1}, 0, func(u *Update) { u.Granted = true })); st != StatusOK {
		t.Fatalf("a granted flush: %v", st)
	}
	c.xFills(910)
	if st := c.status(c.write(pg0, ch, 3*core.BlockSize+10, make([]byte, core.BlockSize))); st != StatusOK {
		t.Fatalf("an overwrite of blocks 3 and 4, held live: %v", st)
	}
	c.xFills(910)
	if st := c.status(c.write(pg0, ch, 3*core.BlockSize, make([]byte, 3*core.BlockSize))); st != StatusNoSpace {
		t.Fatalf("an overwrite of blocks 3 and 4 with block 5 never written: %v", st)
	}
	c.xFills(910)
	if st := c.status(c.update(c.pgFields(pg0), ch, 3*core.BlockSize, []byte{1}, 0, func(u *Update) { u.Snapshot = 1 })); st != StatusNoSpace {
		t.Fatalf("an overwrite a freeze is due for: %v", st)
	}
	c.xFills(910)
	if st := c.status(c.update(c.pgFields(pg0), ch, 3*core.BlockSize, nil, 100, nil)); st != StatusOK {
		t.Fatalf("a punch: %v", st)
	}

	c.xFills(880)
	c.call(0, func(ctx *core.Context, r *Replica, out *ReplicaOutput) { r.SetGrants(testPool, 0) })
	if st := c.status(c.write(pg0, ch, 6*core.BlockSize, []byte{1})); st != StatusOK {
		t.Fatalf("every member under 90%%: %v", st)
	}
	// rep3 on three disks puts a member of every PG on X, a share of 1: each block granted counts on it.
	c.xFills(880)
	c.call(0, func(ctx *core.Context, r *Replica, out *ReplicaOutput) { r.SetGrants(testPool, 20) })
	if st := c.status(c.write(pg0, ch, 7*core.BlockSize, []byte{1})); st != StatusNoSpace {
		t.Fatalf("X at 88%% with its share of grants reaching 90%%: %v", st)
	}
	c.xFills(880)
	c.call(0, func(ctx *core.Context, r *Replica, out *ReplicaOutput) { r.SetGrants(testPool, 19) })
	if st := c.status(c.write(pg0, ch, 7*core.BlockSize, []byte{1})); st != StatusOK {
		t.Fatalf("X at 88%% with grants short of 90%%: %v", st)
	}
}

// Two pools on one disk at 92% total fill: each checks its own limit against the disk's total fill.
func TestEachPoolChecksItsOwnLimit(t *testing.T) {
	tbl := rtable(4, 2)
	p2 := tbl.Pools[0]
	p2.ID = testPool + 1
	tbl.Pools = append(tbl.Pools, p2)
	c := newRCluster(t, tbl)
	c.call(0, func(ctx *core.Context, r *Replica, out *ReplicaOutput) {
		r.SetPool(testPool, Pool{WriteLimit: 95})
		r.SetPool(testPool+1, Pool{WriteLimit: 90})
		for d := range uint32(3) {
			r.SetDisk(d+1, 1000)
		}
		r.MemberFill(3, 920)
	})
	p1 := c.write(pg0, chunkIn(pg0, 4, 0), 0, []byte{1})
	pg := core.PGID{Pool: testPool + 1}
	if st := c.status(c.write(pg, chunkIn(pg, 4, 0), 0, []byte{1})); st != StatusNoSpace {
		t.Fatalf("P2 at 92%% of a 90%% limit: %v", st)
	}
	if st := c.status(p1); st != StatusOK {
		t.Fatalf("P1 at 92%% of a 95%% limit: %v", st)
	}
}

const invWriteLimit = "a disk passes write_limit only through granted flushes, overwrites, punches and recovery"

// limitSpan is the blocks of each chunk the write-limit run writes, and limitRoom the blocks each disk may fill past its fill at the start before it reaches its write_limit.
const (
	limitSpan = 64
	limitRoom = 300
)

type verKey struct {
	chunk   core.ChunkID
	version uint64
}

type blockAt struct {
	chunk core.ChunkID
	block uint32
}

// limitRun is a detector cluster whose disks each reach their pool's write_limit limitRoom blocks above their fill at the start, while a client writes new blocks, overwrites, punches and granted flushes to the chunks of every PG, so that every PG's primary fills each disk at once, and its disk nodes reboot or slow on a path one at a time. A primary decides from the fills its members last reported, so a disk may pass its limit by the plain writes its primaries ordered before they heard: what must never raise a disk past its limit is a plain write ordered while a member's fill its primary held had reached its limit.
type limitRun struct {
	d      *dcluster
	rng    *core.Rand
	blocks []uint64
	boots  []int
	chunks []core.ChunkID
	// seen is each version a primary ordered, by node, boot and store request; past those of them that are plain writes ordered while a member's held fill had reached its limit, and over their versions once their stores numbered them.
	seen map[[3]uint64]bool
	past map[[3]uint64]bool
	over map[verKey]bool
	// live is each node's live blocks before its current event and mapped the version its index maps of each chunk.
	live    []uint64
	mapped  [][]uint64
	err     error
	acked   []blockAt
	noSpace int
}

func (r *limitRun) limit(n int) uint64 { return r.blocks[n] * 90 / 100 }

func (r *limitRun) setLimits(n int) {
	rep := r.d.reps[n]
	rep.SetPool(testPool, Pool{WriteLimit: 90})
	for k, b := range r.blocks {
		rep.SetDisk(r.d.ids[k], b)
	}
	r.boots[n] = r.d.boots[n]
}

// heldFull reports whether node n's replica holds a fill at or past its limit for a member of pg.
func (r *limitRun) heldFull(n int, pg core.PGID) bool {
	rep := r.d.reps[n]
	_, e := rep.table.entry(pg)
	if e == nil {
		return false
	}
	for _, m := range e.Members {
		var live uint64
		if int(m.Disk) < len(rep.disks) {
			live = rep.disks[m.Disk].live
		}
		if m.Disk == rep.cfg.Disk {
			live = rep.live
		}
		if k := r.d.node(m.Disk); k >= 0 && live >= r.limit(k) {
			return true
		}
	}
	return false
}

// number records the version of each write in past that node n's store has numbered.
func (r *limitRun) number(n int) {
	for i := range r.d.reps[n].vers {
		v := &r.d.reps[n].vers[i]
		if v.ordered && r.past[[3]uint64{uint64(n), uint64(r.d.boots[n]), v.req}] {
			r.over[verKey{v.u.Chunk, v.u.Version}] = true
		}
	}
}

func (r *limitRun) maps(n int, i int) uint64 {
	ix := r.d.reps[n].Store().Index()
	if ix == nil {
		return 0
	}
	ch, _ := ix.Chunk(testPool, r.chunks[i])
	return ch.Version
}

func (r *limitRun) onEvent(n int, ev *core.Event, done bool) {
	rep := r.d.reps[n]
	if r.boots[n] != r.d.boots[n] {
		r.setLimits(n)
	}
	if !rep.Store().Serving() {
		r.live[n] = math.MaxUint64
		return
	}
	if !done {
		r.number(n)
		r.live[n] = rep.Store().Log().LiveBlocks()
		for i := range r.chunks {
			r.mapped[n][i] = r.maps(n, i)
		}
		return
	}
	for i := range rep.vers {
		v := &rep.vers[i]
		key := [3]uint64{uint64(n), uint64(r.d.boots[n]), v.req}
		if r.seen[key] {
			continue
		}
		r.seen[key] = true
		if u := &v.u; u.Kind == KindWrite && !u.Granted && r.heldFull(n, u.PG.PG) && !rep.Store().Overwrites(u.Chunk, u.Off, u.Len, u.Snapshot) {
			r.past[key] = true
		}
	}
	r.number(n)
	live := rep.Store().Log().LiveBlocks()
	if live <= r.limit(n) || live <= r.live[n] || r.err != nil {
		return
	}
	for i, ch := range r.chunks {
		if v := r.maps(n, i); v != r.mapped[n][i] && r.over[verKey{ch, v}] {
			r.err = fmt.Errorf("disk %d mapped %#x of %v, a plain write ordered while a member's held fill had reached its limit, at %d live blocks, past its limit of %d", r.d.ids[n], v, ch, live, r.limit(n))
		}
	}
}

// limitClient writes and punches the chunks of every PG with up to 8 requests unanswered: a punch in 10, an overwrite of a block written before in 3, a granted flush of a new block in 10, and otherwise a new block.
type limitClient struct {
	r   *limitRun
	rng *core.Rand
	at  map[core.RequestID]core.Tick
	ops map[core.RequestID]blockAt
}

func (lc *limitClient) Ready() bool {
	c := lc.r.d.rcluster
	out := 0
	for id, at := range lc.at {
		rp, done := c.cl.replies[id]
		switch {
		case done && rp.Status == StatusOK:
			if b, ok := lc.ops[id]; ok {
				lc.r.acked = append(lc.r.acked, b)
			}
		case done && rp.Status == StatusNoSpace:
			lc.r.noSpace++
		case !done && c.s.Time()-at < abandonAfter:
			out++
			continue
		}
		delete(lc.at, id)
	}
	return out < 8
}

func (lc *limitClient) Next(ctx *core.Context, acts []core.Action) []core.Action {
	r, c := lc.r, lc.r.d.rcluster
	b := blockAt{chunk: r.chunks[lc.rng.Below(uint64(len(r.chunks)))], block: uint32(lc.rng.Below(limitSpan))}
	k := lc.rng.Below(10)
	if k >= 1 && k < 4 && len(r.acked) > 0 {
		b = r.acked[lc.rng.Below(uint64(len(r.acked)))]
	}
	pg := core.PGID{Pool: testPool, Index: core.PGIndexOf(b.chunk.Hash(), 4)}
	_, e := c.table.entry(pg)
	lead, ok := e.primary()
	if !ok {
		return acts
	}
	c.nextID++
	id := c.nextID
	lc.at[id] = c.s.Time()
	u := Update{Kind: KindPunch, ID: id, PG: c.pgFields(pg), Session: 1, Incarnation: 1, Chunk: b.chunk, Off: b.block * core.BlockSize, Len: (1 + uint32(lc.rng.Below(4))) * core.BlockSize}
	a := core.Action{Kind: core.ActNetSend, Peer: core.NodeID{Node: uint32(c.node(lead))}}
	if k != 0 {
		u.Kind, u.Len, u.Granted = KindWrite, core.BlockSize, k == 4
		buf, err := ctx.Pool.AllocZeroed(core.BlockSize)
		if err != nil {
			panic(err)
		}
		ctx.Pool.Bytes(buf)[0] = byte(id)
		a.Msg.Buf = buf
		c.cl.sent[id] = buf
		lc.ops[id] = b
	}
	a.Msg.Len = uint16(copy(a.Msg.Body[:], AppendUpdate(nil, &u)))
	return append(acts, a)
}

// runLimit runs the write-limit workload for 20 s of faults and 5 s after, failing t on a broken invariant registered with the simulator; planted, the primaries admit plain writes past the limit and it returns the invariant's first violation.
func runLimit(t *testing.T, seed uint64, planted bool) error {
	d := newDCluster(t, seed, sim.DefaultSettings())
	c := d.rcluster
	for _, x := range d.a.rec.Disks() {
		d.faulty[x.ID] = true
	}
	r := &limitRun{d: d, rng: core.NewRand(seed ^ 0x11417), blocks: make([]uint64, len(c.reps)), boots: make([]int, len(c.reps)),
		seen: map[[3]uint64]bool{}, past: map[[3]uint64]bool{}, over: map[verKey]bool{}, live: make([]uint64, len(c.reps)), mapped: make([][]uint64, len(c.reps))}
	for i := range uint32(4) {
		for k := range uint32(4) {
			r.chunks = append(r.chunks, chunkIn(core.PGID{Pool: testPool, Index: i}, 4, k))
		}
	}
	for n := range c.reps {
		r.blocks[n] = (c.reps[n].Store().Log().LiveBlocks()+limitRoom)*100/90 + 1
		r.mapped[n] = make([]uint64, len(r.chunks))
	}
	if planted {
		c.replicaPlants.admitPastLimit = true
		c.planted = true
	}
	for n := range c.reps {
		c.reps[n].plant = c.replicaPlants
		r.setLimits(n)
	}
	c.inv.Register(invWriteLimit, func() error { return r.err })
	if planted {
		c.inv.Only(invWriteLimit)
	}
	c.onEvent = r.onEvent
	c.s.Node(c.client.Node).AddActor(c.client, &limitClient{r: r, rng: r.rng.Fork(), at: map[core.RequestID]core.Tick{}, ops: map[core.RequestID]blockAt{}})
	faulted, kind, other := -1, uint64(0), uint32(0)
	next, until := c.s.Time()+sec, core.Tick(0)
	end := c.s.Time() + 20*sec
	for c.s.Time() < end+5*sec {
		d.tend()
		switch now := c.s.Time(); {
		case faulted >= 0 && now >= until:
			if kind == 1 {
				c.s.Queue(uint32(faulted), other, 0)
			}
			faulted, next = -1, now+sec+core.Tick(r.rng.Below(uint64(2*sec)))
		case faulted < 0 && now >= next && now < end:
			n := int(r.rng.Below(uint64(len(c.reps))))
			if !c.s.Node(uint32(n)).Up() {
				break
			}
			faulted, kind = n, r.rng.Below(2)
			if kind == 0 {
				c.s.Reboot(uint32(n))
				until = now + sec
			} else {
				other = (uint32(n) + 1 + uint32(r.rng.Below(uint64(len(c.reps)-1)))) % uint32(len(c.reps))
				c.s.Queue(uint32(n), other, 200_000+core.Tick(r.rng.Below(800_000)))
				until = now + 200_000 + core.Tick(r.rng.Below(1_800_000))
			}
		}
		if !c.step() {
			break
		}
		if c.found != nil {
			return c.found
		}
	}
	if !planted && r.noSpace == 0 {
		t.Fatalf("seed %d: no write was refused for space", seed)
	}
	return nil
}

// A disk passes write_limit only through granted flushes, overwrites, punches and recovery: the primaries of four PGs fill three disks to their limits at once while disk nodes reboot or slow on a path.
func TestWritesPassTheWriteLimitOnlyWhereAdmitted(t *testing.T) {
	seeds.Run(t, 0, 10, func(seed uint64) {
		if err := runLimit(t, seed, false); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
	})
}

// The planted primary that orders plain writes past a member's write_limit breaks the invariant at seed 0 (500 of 500 seeds).
func TestPlantedLimitBugBreaksTheLimitRun(t *testing.T) {
	if err := runLimit(t, 0, true); err == nil || !strings.Contains(err.Error(), invWriteLimit) {
		t.Fatalf("seed 0: %v", err)
	}
}
