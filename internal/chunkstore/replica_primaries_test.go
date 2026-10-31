package chunkstore

import (
	"bytes"
	"cmp"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

const (
	invAckedKept   = "no acknowledged version is ever dropped"
	invOneVersion  = "no two members hold different data under one version"
	invKeptWritten = "a member listing nothing maps the kept writes applied in version order"
	invRemoval     = "no move lowers the members of a PG holding an acknowledged version below its width"
)

// authority stands in for the control plane: it knows each disk by the log ID it reports, computes every table with Next from the disks' marks and drains and operators' edits, and records the activations and asks the replicas report; with moves it makes a balance move per table and hands Next every recovered report again each time, as reports delivered late or twice.
type authority struct {
	rec        DiskRecord
	pool       Pool
	down       map[uint32]core.Tick
	drain      map[uint32]bool
	edits      []Edit
	asks       []PGInterval
	acts, seen int
	moves      bool
	plant      placementPlants
	// det, when set, gives the disks' marks in place of down.
	det *Detector
	// keepID plants the bug of a reformatted disk keeping its old disk ID.
	keepID bool
}

func (a *authority) inputs(c *rcluster) *Inputs {
	in := &Inputs{Now: c.s.Time(), Pools: a.pools(), Edits: a.edits, Asks: a.asks}
	a.edits, a.asks = nil, nil
	for _, x := range c.activations[a.acts:] {
		in.Activations = append(in.Activations, PGInterval{x.PG, x.Interval})
	}
	for _, x := range c.asks[a.seen:] {
		in.Asks = append(in.Asks, PGInterval{x.PG, x.Interval})
	}
	a.acts, a.seen = len(c.activations), len(c.asks)
	if a.moves {
		in.Moves = 1
		for _, x := range c.recovered {
			in.Recovered = append(in.Recovered, PGInterval{x.PG, x.Interval})
		}
	}
	in.plant = a.plant
	return in
}

// pools is the pool's inputs: its disks not replaced, marked down since the time the authority marked them.
func (a *authority) pools() []PoolInputs {
	var disks []Disk
	for _, d := range a.rec.Disks() {
		if d.Gone {
			continue
		}
		dk := Disk{ID: d.ID, Node: d.Node, Blocks: tb(1), Drain: a.drain[d.ID]}
		if since, ok := a.down[d.ID]; ok {
			dk.Mark, dk.DownSince = MarkDown, since
		}
		if a.det != nil {
			dk.Mark, dk.DownSince = a.det.Mark(d.ID)
		}
		disks = append(disks, dk)
	}
	return []PoolInputs{{ID: testPool, Pool: a.pool, Disks: disks}}
}

// primaryRun is clients pipelining sub-block writes to two chunks of PG 0 while the authority moves the PG's primary, marking it down and the next member it picks up, marks replicas down and up, restarts the primary, formats a member's disk again and drains a disk and ends the drain, which moves members to another disk and back, after which the PG peers. With returns, a member marked up again may lead though it missed writes; without, the primary only moves to members that stayed live throughout (sub-block spike M6). With duringPeering, changes also land while the PG peers, at a tenth of the rate.
type primaryRun struct {
	c             *rcluster
	a             *authority
	rng           *core.Rand
	chunks        [2]core.ChunkID
	returns       bool
	duringPeering bool
	ended         bool
	ops           map[core.RequestID]faultOp
	// version is each write's version once a primary ordered it, kept each activated interval's decision, and tainted the disks that may lack writes.
	version map[core.RequestID]uint64
	kept    map[uint32][]keptChunk
	tainted map[uint32]bool
	// sent is each update a primary sent, by its node and request ID; acked the bytes a member acknowledged under each block and version.
	sent       map[[2]uint64]Update
	acked      map[blockVersion][]byte
	errs       map[string]error
	reformated bool
	clients    []*primaryClient
}

type blockVersion struct {
	chunk   core.ChunkID
	block   uint16
	version uint64
}

type primaryClient struct {
	r    *primaryRun
	rng  *core.Rand
	left int
	pipe int
	ids  []core.RequestID
	at   map[core.RequestID]core.Tick
}

// abandonAfter is how long a client waits for a reply before it counts the write as unanswered and sends another: a write to a crashed primary, or held on a chunk the primary lacks, gets none.
const abandonAfter = 2_000_000

func (pc *primaryClient) Ready() bool {
	if pc.left == 0 {
		return false
	}
	out := 0
	for _, id := range pc.ids {
		if _, ok := pc.r.c.cl.replies[id]; !ok && pc.r.c.s.Time()-pc.at[id] < abandonAfter {
			out++
		}
	}
	return out < pc.pipe
}

func (pc *primaryClient) Next(ctx *core.Context, acts []core.Action) []core.Action {
	pc.left--
	r, c := pc.r, pc.r.c
	off := uint32(pc.rng.Below(faultSpan))
	n := min(1+uint32(pc.rng.Below(300)), core.ChunkSize-off)
	if pc.rng.Chance(1, 4) {
		off, n = uint32(pc.rng.Below(3))*core.BlockSize, uint32(1+pc.rng.Below(2))*core.BlockSize
	}
	data := make([]byte, n)
	for i := range data {
		data[i] = byte(pc.rng.Below(255) + 1)
	}
	c.nextID++
	id := c.nextID
	pc.ids = append(pc.ids, id)
	pc.at[id] = c.s.Time()
	op := faultOp{c: r.chunks[pc.rng.Below(2)], off: off, n: n, data: data}
	r.ops[id] = op
	u := Update{Kind: KindWrite, ID: id, PG: c.pgFields(pg0), Session: 1, Incarnation: 1, Chunk: op.c, Off: off, Len: n}
	a := core.Action{Kind: core.ActNetSend, Peer: core.NodeID{Node: uint32(max(0, c.primary(pg0)))}}
	a.Msg.Len = uint16(copy(a.Msg.Body[:], AppendUpdate(nil, &u)))
	buf, err := ctx.Pool.AllocZeroed(dataBytes(off, n))
	if err != nil {
		panic(err)
	}
	copy(ctx.Pool.Bytes(buf)[off%core.BlockSize:], data)
	a.Msg.Buf = buf
	c.cl.sent[id] = buf
	return append(acts, a)
}

func (r *primaryRun) fail(inv string, err error) {
	if r.errs[inv] == nil {
		r.errs[inv] = fmt.Errorf("%s: %w", inv, err)
	}
}

// publish computes the next table and hands it to every running replica, with the address of every disk ID, if it changes any entry.
func (r *primaryRun) publish() {
	c := r.c
	next, _ := Next(&c.table, r.a.inputs(c))
	if d := MakeDelta(&c.table, &next); len(d.Entries) == 0 {
		return
	}
	r.removal(&next)
	c.table = next
	for n := range c.reps {
		if !c.s.Node(uint32(n)).Up() {
			continue
		}
		c.call(n, func(ctx *core.Context, rep *Replica, out *ReplicaOutput) {
			for k, id := range c.ids {
				rep.SetAddr(id, core.NodeID{Node: uint32(k)})
			}
			rep.SetTable(ctx, next, out)
		})
	}
}

// removal checks a table that removes PG 0's leaving member, its disk still in the pool: every other member holds every acknowledged version and lists nothing of its chunk, but one whose store is starting, which it cannot see.
func (r *primaryRun) removal(next *Table) {
	c := r.c
	ms := r.members()
	j := slices.IndexFunc(ms, func(m Member) bool { return m.Leaving })
	_, e := next.entry(pg0)
	if j < 0 || slices.ContainsFunc(e.Members, func(m Member) bool { return m.Disk == ms[j].Disk }) ||
		!slices.ContainsFunc(r.a.rec.Disks(), func(d RecordedDisk) bool { return d.ID == ms[j].Disk && !d.Gone }) {
		return
	}
	for id := range c.nextID + 1 {
		rp, ok := c.cl.replies[id]
		if _, op := r.ops[id]; !ok || !op || rp.Status != StatusOK {
			continue
		}
		ch := r.ops[id].c
		for _, m := range e.Members {
			n := c.node(m.Disk)
			if m.Leaving || n >= 0 && c.reps[n].Store().Index() == nil {
				continue
			}
			if x, _ := c.reps[max(n, 0)].Store().Index().Chunk(testPool, ch); n < 0 || x.ListedIn != 0 || x.Version < rp.Version {
				r.fail(invRemoval, fmt.Errorf("interval %d removes disk %d while disk %d lacks write %d's %#x of %v", e.Interval, ms[j].Disk, m.Disk, id, rp.Version, ch))
				return
			}
		}
	}
}

// pass follows each replica's loop: the versions a primary orders and sends, the decision of each interval it activates, and each member's acknowledgments, checked against the member's index and the bytes other members acknowledged under the same version.
func (r *primaryRun) pass(d int, acts []core.Action) {
	c, rep := r.c, r.c.reps[d]
	for _, v := range rep.vers {
		if _, ok := r.ops[v.msgID]; ok && v.u.Version != 0 && r.version[v.msgID] == 0 {
			r.version[v.msgID] = v.u.Version
		}
	}
	if i := rep.lead(pg0); i >= 0 && rep.leads[i].active && r.kept[rep.leads[i].interval] == nil {
		r.kept[rep.leads[i].interval] = append([]keptChunk{}, rep.leads[i].kept...)
	}
	for i := range acts {
		a := &acts[i]
		if a.Kind != core.ActNetSend || a.Msg.Len < headSize {
			continue
		}
		switch k := Kind(a.Msg.Body[1]); k {
		case KindReplicaWrite, KindReplicaPunch, KindRecover:
			var u Update
			data := 0
			if a.Msg.Buf != core.NoBuf {
				data = len(c.s.Node(uint32(d)).Pool().Bytes(a.Msg.Buf))
			}
			if DecodeUpdate(a.Msg.Inline(), data, &u) == nil {
				r.sent[[2]uint64{uint64(d), uint64(u.ID)}] = u
			}
		case KindAck:
			var rp Response
			if DecodeResponse(a.Msg.Inline(), 0, &rp) != nil || rp.Status != StatusOK {
				continue
			}
			u, ok := r.sent[[2]uint64{uint64(a.Peer.Node), uint64(rp.ID)}]
			if !ok || !slices.Contains(r.chunks[:], u.Chunk) {
				continue
			}
			if u.Kind == KindRecover {
				if x, _ := rep.Store().Index().Chunk(testPool, u.Chunk); x.Whole < u.Version {
					r.fail(invAckedWhole, fmt.Errorf("disk %d acknowledged recovery %#x of %v holding %+v", rep.cfg.Disk, u.Version, u.Chunk, x))
				}
				continue
			}
			first, last := spanOf(u.Off, u.Len)
			for b := first; b <= last; b++ {
				e, ok := rep.Store().Index().Lookup(testPool, u.Chunk, b)
				if !ok || e.Version < u.Version {
					r.fail(invAckedWhole, fmt.Errorf("disk %d acknowledged %#x and maps block %d at %#x", rep.cfg.Disk, u.Version, b, e.Version))
					continue
				}
				if e.Version == u.Version {
					r.sameData(blockVersion{u.Chunk, b, u.Version}, c.diskBlock(d, u.Chunk, b), rep.cfg.Disk)
				}
			}
		}
	}
}

func (r *primaryRun) sameData(k blockVersion, got []byte, disk uint32) {
	if want, ok := r.acked[k]; !ok {
		r.acked[k] = got
	} else if !bytes.Equal(got, want) {
		r.fail(invOneVersion, fmt.Errorf("disk %d holds block %d of %v at %#x unlike another member", disk, k.block, k.chunk, k.version))
	}
}

// members is PG 0's members in the cluster's table, primary first.
func (r *primaryRun) members() []Member {
	_, e := r.c.table.entry(pg0)
	return e.Members
}

// running reports whether disk is up in the table and its node runs.
func (r *primaryRun) running(m Member) bool {
	n := r.c.node(m.Disk)
	return m.Mark == MarkUp && n >= 0 && r.c.s.Node(uint32(n)).Up()
}

// order is PG 0's members' disks with first at the front and last at the back, either left out when 0.
func (r *primaryRun) order(first, last uint32) []uint32 {
	var ds []uint32
	if first != 0 {
		ds = append(ds, first)
	}
	for _, m := range r.members() {
		if m.Disk != first && m.Disk != last {
			ds = append(ds, m.Disk)
		}
	}
	if last != 0 {
		ds = append(ds, last)
	}
	return ds
}

// inject draws one change while every member runs and every led PG is active.
func (r *primaryRun) inject() {
	c, a, now := r.c, r.a, r.c.s.Time()
	ms := r.members()
	if !slices.ContainsFunc(ms, func(m Member) bool { return m.Mark == MarkUp }) {
		return
	}
	primary := ms[slices.IndexFunc(ms, func(m Member) bool { return m.Mark == MarkUp })].Disk
	all := !slices.ContainsFunc(ms, func(m Member) bool { return !r.running(m) })
	moving := slices.ContainsFunc(ms, func(m Member) bool { return m.Leaving })
	switch r.rng.Below(6) {
	case 0:
		next := slices.IndexFunc(ms, func(m Member) bool { return m.Disk != primary && (r.returns || !r.tainted[m.Disk]) })
		if !all || moving || next < 0 {
			return
		}
		a.down[primary], r.tainted[primary] = now, true
		a.edits = append(a.edits, Edit{PG: pg0, Disks: r.order(ms[next].Disk, primary)})
		if r.rng.Chance(1, 2) {
			c.s.CrashRandom(uint32(c.node(primary)))
		}
	case 1:
		m := ms[1+r.rng.Below(uint64(len(ms)-1))].Disk
		if !all {
			return
		}
		a.down[m], r.tainted[m] = now, true
		if r.rng.Chance(1, 2) {
			c.s.CrashRandom(uint32(c.node(m)))
		}
	case 2:
		for _, m := range ms {
			if n := c.node(m.Disk); !moving && m.Mark == MarkDown && c.s.Node(uint32(n)).Up() {
				delete(a.down, m.Disk)
				if r.returns && r.rng.Chance(1, 2) {
					a.edits = append(a.edits, Edit{PG: pg0, Disks: r.order(m.Disk, 0)})
				} else {
					a.edits = append(a.edits, Edit{PG: pg0, Disks: r.order(0, m.Disk)})
				}
				break
			}
		}
	case 3:
		if all {
			c.s.CrashRandom(uint32(c.node(primary)))
		}
	case 4:
		m := ms[1+r.rng.Below(uint64(len(ms)-1))].Disk
		if !all || r.reformated {
			return
		}
		r.reformated = true
		n := c.node(m)
		c.logs[n], c.reformat[n] = 0xf0f0+uint64(n), true
		c.s.CrashRandom(uint32(n))
	case 5:
		if len(a.drain) > 0 {
			clear(a.drain)
		} else if d := a.rec.Disks()[r.rng.Below(uint64(len(a.rec.Disks())))]; all && !d.Gone {
			a.drain[d.ID] = true
		}
	}
	r.publish()
}

// formatted is the disk ID the authority gives node n's disk formatted again: a new one for its new log ID, the old one replaced and its members refilled at once.
func (r *primaryRun) formatted(n int) uint32 {
	c := r.c
	if r.a.keepID {
		return c.ids[n]
	}
	id, err := r.a.rec.Report(uint32(n), 0, c.logs[n])
	if err != nil {
		panic(err)
	}
	r.tainted[id] = true
	return id
}

// keptVersion reports whether the run keeps write id's version: the client heard OK, or the first activated interval after the version's kept it.
func (r *primaryRun) keptVersion(id core.RequestID) (uint64, bool) {
	if rp, ok := r.c.cl.replies[id]; ok && rp.Status == StatusOK {
		return rp.Version, true
	}
	v := r.version[id]
	if v == 0 {
		return 0, false
	}
	k, ok := r.decision(r.ops[id].c, intervalOf(v))
	return v, ok && v <= k
}

// decision is the version of ch kept by the first activated interval after interval i.
func (r *primaryRun) decision(ch core.ChunkID, i uint32) (uint64, bool) {
	_, e := r.c.table.entry(pg0)
	for k := i + 1; k <= e.Interval; k++ {
		if ks, ok := r.kept[k]; ok {
			j := slices.IndexFunc(ks, func(x keptChunk) bool { return x.chunk == ch })
			if j < 0 {
				return 0, true
			}
			return ks[j].kept, true
		}
	}
	return 0, false
}

// check holds the run's end against its oracle: every acknowledged version kept, and every member that runs, is up and lists nothing mapping the kept writes applied in version order, and every two members mapping a block at one version holding the same bytes.
// check checks the run's end: no acknowledged version dropped, and every member listing nothing maps the kept writes, holding no version unlike another's.
func (r *primaryRun) check() {
	c := r.c
	type keptOp struct {
		version uint64
		id      core.RequestID
	}
	var all []keptOp
	for id := range c.nextID + 1 {
		if _, ok := r.ops[id]; !ok {
			continue
		}
		v, ok := r.keptVersion(id)
		if rp, acked := c.cl.replies[id]; acked && rp.Status == StatusOK {
			if k, decided := r.decision(r.ops[id].c, intervalOf(v)); decided && v > k {
				r.fail(invAckedKept, fmt.Errorf("write %d acknowledged as %#x, interval %d kept %#x", id, v, intervalOf(v), k))
			}
		}
		if ok {
			all = append(all, keptOp{v, id})
		}
	}
	slices.SortFunc(all, func(a, b keptOp) int { return cmp.Compare(a.version, b.version) })
	for _, ch := range r.chunks {
		for _, m := range r.members() {
			if n := c.node(m.Disk); r.running(m) {
				for b := range uint16(2 * faultSpan / core.BlockSize) {
					if e, ok := c.reps[n].Store().Index().Lookup(testPool, ch, b); ok {
						r.sameData(blockVersion{ch, b, e.Version}, c.diskBlock(n, ch, b), m.Disk)
					}
				}
			}
		}
	}
	if r.errs[invOneVersion] != nil {
		return
	}
	for _, ch := range r.chunks {
		want := make([]byte, 2*faultSpan)
		for _, k := range all {
			if op := r.ops[k.id]; op.c == ch {
				copy(want[op.off:], op.data)
			}
		}
		for _, m := range r.members() {
			n := c.node(m.Disk)
			if !r.running(m) {
				continue
			}
			if x, _ := c.reps[n].Store().Index().Chunk(testPool, ch); x.ListedIn != 0 {
				continue
			}
			for b := range uint16(len(want) / core.BlockSize) {
				got := c.diskBlock(n, ch, b)
				if w := want[int(b)*core.BlockSize:][:core.BlockSize]; !bytes.Equal(got, w) {
					i := 0
					for got[i] == w[i] {
						i++
					}
					r.fail(invKeptWritten, fmt.Errorf("disk %d maps byte %d of block %d of %v as %#x, the kept writes give %#x", m.Disk, i, b, ch, got[i], w[i]))
					return
				}
			}
		}
	}
}

// recover steps the cluster, the authority tending, until no running member lists a chunk but an unfound one, for up to 30 s: a member that still does fails the copy count.
func (r *primaryRun) recover() {
	c := r.c
	end := c.s.Time() + 30_000_000
	for {
		var left error
		p := c.reps[c.node(r.members()[0].Disk)]
		i := p.lead(pg0)
		for _, ch := range r.chunks {
			if i >= 0 && p.leads[i].isUnfound(ch) {
				continue
			}
			for _, m := range r.members() {
				if r.running(m) {
					if x, _ := c.reps[c.node(m.Disk)].Store().Index().Chunk(testPool, ch); x.ListedIn != 0 && left == nil {
						left = fmt.Errorf("disk %d lists %+v of %v", m.Disk, x, ch)
					}
				}
			}
		}
		if left == nil {
			return
		}
		if c.s.Time() >= end {
			r.fail(invRecovered, left)
			return
		}
		r.tend()
		if !c.step() {
			r.fail(invRecovered, left)
			return
		}
	}
}

// settled steps the cluster, the authority answering asks and recording activations, until every led PG is active and nothing but periodic timers is pending.
func (r *primaryRun) settled() {
	c := r.c
	for range 1_000_000 {
		r.tend()
		if c.active() && c.quiet() {
			return
		}
		if !c.step() {
			break
		}
	}
	c.t.Fatal("the cluster did not settle")
}

// readEach reads PG 0's chunks back, recorded, with each up member its primary in turn, so a member
// holding other bytes than the primary shows in the history once it leads.
func (r *primaryRun) readEach(x *chunkHist) {
	for _, m := range slices.Clone(r.members()) {
		if m.Mark != MarkUp || !r.running(m) {
			continue
		}
		ds := []uint32{m.Disk}
		for _, o := range r.members() {
			if o.Disk != m.Disk {
				ds = append(ds, o.Disk)
			}
		}
		r.a.edits = append(r.a.edits, Edit{PG: pg0, Disks: ds})
		r.publish()
		r.settled()
		if unread := x.readBack(2*faultSpan, r.chunks[:]...); len(unread) > 0 && !r.c.planted {
			r.c.t.Fatalf("disk %d as primary answered no read back of %v", m.Disk, unread)
		}
	}
}

// tend publishes a table for the asks and activations replicas reported, and for a disk formatted again.
func (r *primaryRun) tend() {
	c := r.c
	if len(c.asks) > r.a.seen || len(c.activations) > r.a.acts {
		r.publish()
	}
}

// peerPlant is a bug planted in the primary-change run and the invariant it must break.
type peerPlant struct {
	inv string
	set func(r *primaryRun)
}

// runPrimaries runs the primary-change workload, failing t on a broken invariant registered with the
// simulator and returning a broken end check; with a plant, it checks only the plant's invariant and
// returns its first violation.
func runPrimaries(t *testing.T, seed uint64, plant *peerPlant) error {
	s := sim.New(seed, sim.DefaultSettings())
	a := &authority{pool: Pool{Replicas: 3, MinLive: 2, FailureDomain: DomainNode, MaxPerDomain: 1, Locality: LocalitySpread, Overcommit: true, WriteLimit: 90, PGCount: 4},
		down: map[uint32]core.Tick{}, drain: map[uint32]bool{}, moves: true}
	for n := range 4 {
		if _, err := a.rec.Report(uint32(n), 0, 0x5eed+uint64(n)); err != nil {
			t.Fatal(err)
		}
	}
	var empty Table
	tbl, _ := Next(&empty, &Inputs{Pools: a.pools()})
	c := newRClusterWith(t, tbl, s, func(cfg *Config) { cfg.Log.MaxRecordBytes = 3 * core.BlockSize })
	r := &primaryRun{c: c, a: a, rng: core.NewRand(seed ^ 0x9e3), chunks: [2]core.ChunkID{chunkIn(pg0, 4, 0), chunkIn(pg0, 4, 1)}, ops: map[core.RequestID]faultOp{},
		version: map[core.RequestID]uint64{}, kept: map[uint32][]keptChunk{}, tainted: map[uint32]bool{},
		sent: map[[2]uint64]Update{}, acked: map[blockVersion][]byte{}, errs: map[string]error{}}
	r.returns = r.rng.Chance(1, 2)
	for _, inv := range []string{invAckedWhole, invOneVersion, invRemoval, invAckedKept, invKeptWritten, invRecovered} {
		c.inv.Register(inv, func() error { return r.errs[inv] })
	}
	c.inv.Register(invActivates, func() error {
		if !r.ended {
			return nil
		}
		return c.inactive()
	})
	// Drawn from the seed so other draws keep their order.
	r.duringPeering = seed%4 == 3
	if plant != nil {
		plant.set(r)
	}
	for n := range c.reps {
		c.reps[n].plant, c.reps[n].Store().plant = c.replicaPlants, c.storePlants
	}
	c.onPass = r.pass
	c.onFormat = r.formatted
	watchEpochs(c, &a.rec)
	x := c.record()
	c.inv.Register(invLinearizable, func() error {
		if !r.ended {
			return nil
		}
		return x.check()
	})
	if plant != nil {
		c.planted = true
		c.inv.Only(plant.inv)
	}
	for range 2 {
		pc := &primaryClient{r: r, rng: r.rng.Fork(), left: 48, pipe: 8, at: map[core.RequestID]core.Tick{}}
		r.clients = append(r.clients, pc)
		c.addClient(pc)
	}
	busy := func() bool {
		return slices.ContainsFunc(r.clients, func(pc *primaryClient) bool { return pc.left > 0 })
	}
	for range 3_000_000 {
		r.tend()
		if !busy() && c.quiet() || !c.step() {
			break
		}
		if c.found != nil {
			return c.found
		}
		if busy() && r.rng.Chance(1, 40) && (c.active() || r.duringPeering && r.rng.Chance(1, 10)) && !slices.ContainsFunc(r.members(), func(m Member) bool { return m.Mark == MarkUp && !r.running(m) }) {
			r.inject()
		}
	}
	for n := range c.reps {
		for !c.s.Node(uint32(n)).Up() && c.step() {
		}
	}
	clear(a.down)
	_, e := c.table.entry(pg0)
	a.asks = append(a.asks, PGInterval{pg0, e.Interval})
	r.publish()
	r.settled()
	r.recover()
	r.check()
	r.readEach(x)
	r.ended = true
	if err := c.inv.Check(); err != nil {
		return err
	}
	return c.found
}

// Sub-block spike M6 on replicas: two clients pipeline 8 writes each to two chunks while the authority moves the PG's primary, to members that stayed live throughout or, in half the seeds, to members marked up again that missed writes, marks replicas down and up, restarts the primary, formats a member again and drains a disk of four and ends the drain, moving members. Every acknowledged version is kept, members hold the same bytes under a version, every member that lists nothing maps the kept writes applied in version order, and no move removes a leaving member while another member lacks an acknowledged version.
func TestPeeringKeepsAcknowledgedWritesAcrossPrimaries(t *testing.T) {
	seeds.Run(t, 0, 10, func(seed uint64) {
		if err := runPrimaries(t, seed, nil); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
	})
}

// Each planted bug breaks its invariant, checked alone, at a seed of the first 500: peering dropping any subset of a chunk's pending versions rather than a suffix (sub-block spike M7, 4 of 500), a partial-block write merged before the primary holds the versions peering kept (M8, 47 of 500), a restarted primary peering in its old interval (32 of 500), a disk formatted again keeping its old disk ID (211 of 500), the epoch change model's neg-compare-table-epochs (497 of 500) and neg-compare-member-lists (303 of 500), which breaks property 3 first: a member that compares member lists logs the old primary's write tagged with an interval its table left; and the model's neg-remove-before-recovered (123 of 500), here a leaving member removed once its PG's interval activates, and neg-accept-old-report (145 of 500), each removing a moved member while its replacement lacks acknowledged writes; and neg-report-before-activate (500 of 500), a primary that reports its interval activated once it decides, before it activates; and a primary that recovers only its own copies (434 of 500), whose members, those a drain refilled among them, never reach the copy count.
func TestPlantedPeeringBugsBreakTheOracle(t *testing.T) {
	for _, p := range []struct {
		name string
		seed uint64
		peerPlant
	}{
		{"M7", 38, peerPlant{invKeptWritten, func(r *primaryRun) { r.c.storePlants.settleSubset = true }}},
		{"M8", 37, peerPlant{invKeptWritten, func(r *primaryRun) { r.c.replicaPlants.mergeWhileLacking = true }}},
		{"restart in the old interval", 6, peerPlant{invOneVersion, func(r *primaryRun) { r.c.replicaPlants.peerInOldInterval = true }}},
		{"old disk ID kept", 2, peerPlant{invDiskIDs, func(r *primaryRun) { r.a.keepID = true }}},
		{"accepted only in the send epoch", 0, peerPlant{invSameInterval, func(r *primaryRun) { r.c.replicaPlants.sendEpoch = true }}},
		{"member lists compared", 1, peerPlant{invLateLog, func(r *primaryRun) { r.c.replicaPlants.sameMembers = true }}},
		{"removed before recovered", 3, peerPlant{invRemoval, func(r *primaryRun) { r.a.plant.removeOnActivation = true }}},
		{"old recovered report", 3, peerPlant{invRemoval, func(r *primaryRun) { r.a.plant.oldReport = true }}},
		{"activation reported once decided", 0, peerPlant{invActivations, func(r *primaryRun) { r.c.replicaPlants.reportDecided = true }}},
		{"members never recovered", 1, peerPlant{invRecovered, func(r *primaryRun) { r.c.replicaPlants.recoverSelfOnly = true }}},
	} {
		if err := runPrimaries(t, p.seed, &p.peerPlant); err == nil || !strings.Contains(err.Error(), p.inv) {
			t.Errorf("%s at seed %d: %v", p.name, p.seed, err)
		}
	}
}
