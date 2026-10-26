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
)

// authority stands in for the control plane: it knows each disk by the log ID it reports, computes every table with Next from the disks' marks and operators' edits, and records the activations and asks the replicas report.
type authority struct {
	rec        DiskRecord
	pool       Pool
	down       map[uint32]core.Tick
	edits      []Edit
	asks       []PGInterval
	acts, seen int
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
	return in
}

// pools is the pool's inputs: its disks not replaced, marked down since the time the authority marked them.
func (a *authority) pools() []PoolInputs {
	var disks []Disk
	for _, d := range a.rec.Disks() {
		if d.Gone {
			continue
		}
		dk := Disk{ID: d.ID, Node: d.Node, Blocks: tb(1)}
		if since, ok := a.down[d.ID]; ok {
			dk.Mark, dk.DownSince = MarkDown, since
		}
		disks = append(disks, dk)
	}
	return []PoolInputs{{ID: testPool, Pool: a.pool, Disks: disks}}
}

// primaryRun is clients pipelining sub-block writes to two chunks of PG 0 while the authority moves the PG's primary, marking it down and the next member it picks up, marks replicas down and up, restarts the primary and formats a member's disk again, after which the PG peers. With returns, a member marked up again may lead though it missed writes; without, the primary only moves to members that stayed live throughout (sub-block spike M6). With duringPeering, changes also land while the PG peers, at a tenth of the rate.
type primaryRun struct {
	c             *rcluster
	a             *authority
	rng           *core.Rand
	chunks        [2]core.ChunkID
	returns       bool
	duringPeering bool
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

// failed is the first invariant the run's passes found broken.
func (r *primaryRun) failed() error {
	return cmp.Or(r.errs[invAckedWhole], r.errs[invOneVersion])
}

// publish computes the next table and hands it to every running replica, with the address of every disk ID, if it changes any entry.
func (r *primaryRun) publish() {
	c := r.c
	next, _ := Next(&c.table, r.a.inputs(c))
	if d := MakeDelta(&c.table, &next); len(d.Entries) == 0 {
		return
	}
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
		case KindReplicaWrite, KindReplicaPunch:
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
	switch r.rng.Below(5) {
	case 0:
		next := slices.IndexFunc(ms, func(m Member) bool { return m.Disk != primary && (r.returns || !r.tainted[m.Disk]) })
		if !all || next < 0 {
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
			if n := c.node(m.Disk); m.Mark == MarkDown && c.s.Node(uint32(n)).Up() {
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
func (r *primaryRun) check() error {
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
				return fmt.Errorf("%s: write %d acknowledged as %#x, interval %d kept %#x", invAckedKept, id, v, intervalOf(v), k)
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
	if err := r.errs[invOneVersion]; err != nil {
		return err
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
					return fmt.Errorf("%s: disk %d maps byte %d of block %d of %v as %#x, the kept writes give %#x", invKeptWritten, m.Disk, i, b, ch, got[i], w[i])
				}
			}
		}
	}
	return nil
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

// tend publishes a table for the asks and activations replicas reported, and for a disk formatted again.
func (r *primaryRun) tend() {
	c := r.c
	if len(c.asks) > r.a.seen || len(c.activations) > r.a.acts {
		r.publish()
	}
}

func runPrimaries(t *testing.T, seed uint64, edit func(r *primaryRun)) error {
	s := sim.New(seed, sim.DefaultSettings())
	a := &authority{pool: Pool{Replicas: 3, MinLive: 2, FailureDomain: DomainNode, MaxPerDomain: 1, Locality: LocalitySpread, Overcommit: true, WriteLimit: 90, PGCount: 4}, down: map[uint32]core.Tick{}}
	for n := range 3 {
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
	for _, inv := range []string{invAckedWhole, invOneVersion} {
		c.inv.Register(inv, func() error { return r.errs[inv] })
		if edit != nil {
			// A planted bug's run returns the invariant it broke, through failed, rather than failing the test.
			c.inv.Off(inv)
		}
	}
	// Drawn from the seed rather than r.rng, so that the other seeds run as they did before it existed.
	r.duringPeering = seed%4 == 3
	if edit != nil {
		edit(r)
	}
	for n := range c.reps {
		c.reps[n].plant, c.reps[n].Store().plant = c.replicaPlants, c.storePlants
	}
	c.onPass = r.pass
	c.onFormat = r.formatted
	for range 2 {
		pc := &primaryClient{r: r, rng: r.rng.Fork(), left: 48, pipe: 8, at: map[core.RequestID]core.Tick{}}
		r.clients = append(r.clients, pc)
		c.s.Node(c.client.Node).AddActor(c.client, pc)
	}
	busy := func() bool {
		return slices.ContainsFunc(r.clients, func(pc *primaryClient) bool { return pc.left > 0 })
	}
	for range 3_000_000 {
		r.tend()
		if !busy() && c.quiet() || !c.step() {
			break
		}
		if err := r.failed(); err != nil {
			return err
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
	if err := r.failed(); err != nil {
		return err
	}
	return r.check()
}

// Sub-block spike M6 on replicas: two clients pipeline 8 writes each to two chunks while the authority moves the PG's primary, to members that stayed live throughout or, in half the seeds, to members marked up again that missed writes, marks replicas down and up, restarts the primary and formats a member again. Every acknowledged version is kept, members hold the same bytes under a version, and every member that lists nothing maps the kept writes applied in version order.
func TestPeeringKeepsAcknowledgedWritesAcrossPrimaries(t *testing.T) {
	seeds.Run(t, 0, 10, func(seed uint64) {
		if err := runPrimaries(t, seed, nil); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
	})
}

// Each planted bug breaks the run's oracle at a seed of the first 500: peering dropping any subset of a chunk's pending versions rather than a suffix (sub-block spike M7, 4 of 500), a partial-block write merged before the primary holds the versions peering kept (M8, 45 of 500), a restarted primary peering in its old interval (48 of 500) and a disk formatted again keeping its old disk ID (97 of 500).
func TestPlantedPeeringBugsBreakTheOracle(t *testing.T) {
	for _, p := range []struct {
		name  string
		seed  uint64
		inv   string
		plant func(r *primaryRun)
	}{
		{"M7", 242, invKeptWritten, func(r *primaryRun) { r.c.storePlants.settleSubset = true }},
		{"M8", 6, invKeptWritten, func(r *primaryRun) { r.c.replicaPlants.mergeWhileLacking = true }},
		{"restart in the old interval", 1, invOneVersion, func(r *primaryRun) { r.c.replicaPlants.peerInOldInterval = true }},
		{"old disk ID kept", 0, invAckedKept, func(r *primaryRun) { r.a.keepID = true }},
	} {
		if err := runPrimaries(t, p.seed, p.plant); err == nil || !strings.HasPrefix(err.Error(), p.inv) {
			t.Errorf("%s at seed %d: %v", p.name, p.seed, err)
		}
	}
}
