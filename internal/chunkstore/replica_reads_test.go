package chunkstore

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim/clock"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

const (
	invOneReader   = "no PG serves reads from two primaries at once"
	invIncarnation = "no write of an incarnation is acknowledged after a later incarnation's write to its chunk"
	invReadVersion = "no read returns a version before it commits, or one later lost"
)

// readRun is a detector cluster on noisy clocks whose disk nodes are, one at a time, cut off from every other node, the authority's included, so that they miss tables, and in half the cuts from the client too, paused, rebooted or slowed on a path, while a client reads every PG at every node, tagged with one of the last few tables, and two incarnations of one client session write: the first dies partway, and its successor says hello to every node, reissues the writes left unanswered and writes on.
type readRun struct {
	d       *dcluster
	rng     *core.Rand
	reads   map[core.RequestID]readVersion
	writes  map[core.RequestID]incWrite
	tables  []Table
	faulted int
	kind    uint64
	other   uint32
	until   core.Tick
	next    core.Tick
	err     error
	// epochs holds the decisions of the PGs' activated intervals, acked each version a primary answered a write OK with, verErr the first read answered OK with a version neither acknowledged nor kept, or once ended one later lost.
	epochs *epochChecks
	acked  map[chunkVersion]bool
	verErr error
	ended  bool
	// returns has a node cut off lead its PGs once marked up again; back is the node cut off that gets no tables until then, or until backBy.
	returns bool
	back    int
	backBy  core.Tick
}

// readVersion is a read's PG and chunk and, once answered OK, the version it returned.
type readVersion struct {
	pg    core.PGID
	chunk core.ChunkID
	ver   uint64
}

// chunkVersion is a version of a chunk.
type chunkVersion struct {
	chunk core.ChunkID
	ver   uint64
}

type incWrite struct {
	inc   uint64
	chunk core.ChunkID
}

// readPlant is a bug planted in the read run and the invariant it must break.
type readPlant struct {
	inv string
	set func(r *readRun)
}

// serving is a node other than skip that may serve pg's reads now: running, not standing still, and leading pg with a lease on every other up member of its table's entry; -1 when none.
func (c *rcluster) serving(pg core.PGID, skip int) int {
	for n, rep := range c.reps {
		nd := c.s.Node(uint32(n))
		if n == skip || !nd.Up() || nd.Paused() {
			continue
		}
		i := rep.lead(pg)
		if i < 0 || !rep.leads[i].serving {
			continue
		}
		p, e := rep.table.entry(pg)
		ctx := nd.Context()
		if e != nil && rep.readable(&ctx, p, e) {
			return n
		}
	}
	return -1
}

func (r *readRun) oneReader() error {
	if r.err != nil {
		return r.err
	}
	return r.d.oneReader(4)
}

// oneReader checks that no two nodes may serve the reads of any of the first pgs PGs.
func (c *rcluster) oneReader(pgs uint32) error {
	for i := range pgs {
		pg := core.PGID{Pool: testPool, Index: i}
		if a := c.serving(pg, -1); a >= 0 {
			if b := c.serving(pg, a); b >= 0 {
				return fmt.Errorf("nodes %d and %d may both serve reads of %v", a, b, pg)
			}
		}
	}
	return nil
}

// read notes the first read a node serves while another may serve its PG's reads.
func (r *readRun) read(d int, ev *core.Event) {
	if err := r.d.readArrives(d, ev); err != nil && r.err == nil {
		r.err = err
	}
}

// readArrives checks a client read node d is about to take: if it would serve it as the PG's primary, no other node may serve the PG's reads.
func (c *rcluster) readArrives(d int, ev *core.Event) error {
	nd := c.s.Node(uint32(d))
	if ev.Kind != core.EvNetRecv || ev.Peer != c.client || ev.Msg.Len < headSize || Kind(ev.Msg.Body[1]) != KindRead {
		return nil
	}
	frame, data := Frame(nd.Pool(), &ev.Msg)
	var rd Read
	if DecodeRead(frame, data, &rd) != nil {
		return nil
	}
	rep := c.reps[d]
	i := rep.lead(rd.PG.PG)
	p, e := rep.table.entry(rd.PG.PG)
	ctx := nd.Context()
	if i < 0 || !rep.leads[i].serving || rd.PG.Interval < rep.leads[i].since || rd.PG.Interval > rep.leads[i].interval || !rep.readable(&ctx, p, e) {
		return nil
	}
	if b := c.serving(rd.PG.PG, d); b >= 0 {
		return fmt.Errorf("node %d serves read %d of %v while node %d may serve its reads", d, rd.ID, rd.PG.PG, b)
	}
	return nil
}

// pass notes the versions node d answers the client with: a write's acknowledgment, and a read's, which must return a version acknowledged or kept by a later activated interval.
func (r *readRun) pass(d int, acts []core.Action) {
	c := r.d.rcluster
	for i := range acts {
		a := &acts[i]
		if a.Kind != core.ActNetSend || a.Peer != c.client || a.Msg.Len < headSize {
			continue
		}
		data := 0
		if a.Msg.Buf != core.NoBuf {
			data = len(c.s.Node(uint32(d)).Pool().Bytes(a.Msg.Buf))
		}
		var rp Response
		if DecodeResponse(a.Msg.Inline(), data, &rp) != nil || rp.Status != StatusOK {
			continue
		}
		if w, ok := r.writes[rp.ID]; ok && rp.Kind == KindReply {
			r.acked[chunkVersion{w.chunk, rp.Version}] = true
		}
		rd, ok := r.reads[rp.ID]
		if !ok || rp.Kind != KindReadReply || rp.Version == 0 {
			continue
		}
		rd.ver = rp.Version
		r.reads[rp.ID] = rd
		if !r.acked[chunkVersion{rd.chunk, rd.ver}] && !r.keptLater(rd, func(kept uint64) bool { return kept >= rd.ver }) && r.verErr == nil {
			r.verErr = fmt.Errorf("%s: node %d answers read %d of %v with %#x, neither acknowledged nor kept", invReadVersion, d, rp.ID, rd.chunk, rd.ver)
		}
	}
}

// keptLater reports whether the kept version of rd's chunk passes ok in some activated interval of rd's PG after its version's.
func (r *readRun) keptLater(rd readVersion, ok func(kept uint64) bool) bool {
	for k, kept := range r.epochs.decisions[rd.pg] {
		if k <= intervalOf(rd.ver) {
			continue
		}
		v := uint64(0)
		if j := slices.IndexFunc(kept, func(kc keptChunk) bool { return kc.chunk == rd.chunk }); j >= 0 {
			v = kept[j].kept
		}
		if ok(v) {
			return true
		}
	}
	return false
}

// lost checks that no read returned a version an activated interval after it dropped.
func (r *readRun) lost() error {
	for _, id := range slices.Sorted(maps.Keys(r.reads)) {
		rd := r.reads[id]
		if rd.ver != 0 && r.keptLater(rd, func(kept uint64) bool { return kept < rd.ver }) {
			return fmt.Errorf("%s: read %d returned %#x of %v, which a later interval dropped", invReadVersion, id, rd.ver, rd.chunk)
		}
	}
	return nil
}

// incarnations checks that no acknowledged write of the session's first incarnation has a later version of its chunk than an acknowledged write of the second.
func (c *rcluster) incarnations(writes map[core.RequestID]incWrite) error {
	first, second := map[core.ChunkID]core.RequestID{}, map[core.ChunkID]core.RequestID{}
	for _, id := range slices.Sorted(maps.Keys(writes)) {
		w := writes[id]
		rp, ok := c.cl.replies[id]
		if !ok || rp.Status != StatusOK {
			continue
		}
		if w.inc == 1 {
			if k, ok := first[w.chunk]; !ok || rp.Version > c.cl.replies[k].Version {
				first[w.chunk] = id
			}
		} else if k, ok := second[w.chunk]; !ok || rp.Version < c.cl.replies[k].Version {
			second[w.chunk] = id
		}
	}
	for _, ch := range slices.SortedFunc(maps.Keys(first), compareChunk) {
		a := first[ch]
		if b, ok := second[ch]; ok && c.cl.replies[a].Version > c.cl.replies[b].Version {
			return fmt.Errorf("%s: write %d of incarnation 1 acknowledged as %#x of %v, write %d of incarnation 2 as %#x", invIncarnation, a, c.cl.replies[a].Version, ch, b, c.cl.replies[b].Version)
		}
	}
	return nil
}

// inject draws a fault on a running disk node that stands still.
func (r *readRun) inject() {
	c, now := r.d.rcluster, r.d.s.Time()
	n := int(r.rng.Below(uint64(len(c.reps))))
	if nd := c.s.Node(uint32(n)); !nd.Up() || nd.Paused() {
		return
	}
	r.faulted, r.kind = n, r.rng.Below(4)
	r.until = now + 500_000 + core.Tick(r.rng.Below(5_500_000))
	switch r.kind {
	case 0:
		for k := range uint32(len(c.reps) + 2) {
			if k != uint32(n) && (k != c.client.Node || r.rng.Chance(1, 2)) {
				c.s.Partition(uint32(n), k, false)
			}
		}
		r.d.cut[n] = true
	case 1:
		stops := []clock.Stop{clock.Suspend, clock.PauseCounted, clock.Restore, clock.HeldMigration}
		c.s.Pause(uint32(n), stops[r.rng.Below(uint64(len(stops)))], r.until-now, r.rng.Chance(1, 2))
	case 2:
		c.s.Reboot(uint32(n))
		r.until = now + 1_000_000
	case 3:
		r.other = (uint32(n) + 1 + uint32(r.rng.Below(uint64(len(c.reps)-1)))) % uint32(len(c.reps))
		c.s.Queue(uint32(n), r.other, 200_000+core.Tick(r.rng.Below(1_800_000)))
	}
}

// heal ends the fault; a node cut off gets no table until it returns.
func (r *readRun) heal() {
	c, n := r.d.rcluster, r.faulted
	switch r.kind {
	case 0:
		for k := range uint32(len(c.reps) + 2) {
			c.s.Heal(uint32(n), k)
		}
		r.back, r.backBy = n, c.s.Time()+3_000_000
	case 3:
		c.s.Queue(uint32(n), r.other, 0)
	}
	r.faulted = -1
	r.next = c.s.Time() + 500_000 + core.Tick(r.rng.Below(2_500_000))
}

// rejoin hands the node cut off the newest table, as its next fetch would, once the detector marks its disk up again or 3 s after the cut healed; with returns, the table that marks it up has it lead every PG it is a member of.
func (r *readRun) rejoin() {
	c, n := r.d.rcluster, r.back
	up, _ := r.d.det.Mark(c.ids[n])
	if up != MarkUp && c.s.Time() < r.backBy {
		return
	}
	if r.returns && up == MarkUp {
		for pi := range c.table.Pools {
			p := &c.table.Pools[pi]
			for i := range p.PGs {
				ms := p.PGs[i].Members
				if !slices.ContainsFunc(ms, func(m Member) bool { return m.Disk == c.ids[n] }) || ms[0].Disk == c.ids[n] {
					continue
				}
				ds := []uint32{c.ids[n]}
				for _, m := range ms {
					if m.Disk != c.ids[n] {
						ds = append(ds, m.Disk)
					}
				}
				r.d.a.edits = append(r.d.a.edits, Edit{PG: core.PGID{Pool: p.ID, Index: uint32(i)}, Disks: ds})
			}
		}
	}
	r.back = -1
	delete(r.d.cut, n)
	r.d.tend()
	if c.s.Node(uint32(n)).Up() {
		t := c.table
		c.call(n, func(ctx *core.Context, rep *Replica, out *ReplicaOutput) { rep.SetTable(ctx, t, out) })
	}
}

// incWriter is one incarnation of client session 1, writing blocks and parts of blocks of two chunks of every PG with up to 6 writes unanswered: the first stops at stop, the second starts at start with a hello to every node and the first's unanswered writes.
type incWriter struct {
	r       *readRun
	rng     *core.Rand
	inc     uint64
	start   core.Tick
	stop    core.Tick
	at      map[core.RequestID]core.Tick
	ops     map[core.RequestID]faultOp
	reissue []faultOp
	hello   bool
}

func (w *incWriter) Ready() bool {
	c := w.r.d.rcluster
	now := c.s.Time()
	if now < w.start || now >= w.stop {
		return false
	}
	out := 0
	for id, at := range w.at {
		if _, done := c.cl.replies[id]; done || now-at >= abandonAfter {
			delete(w.at, id)
		} else {
			out++
		}
	}
	return out < 6
}

func (w *incWriter) Next(ctx *core.Context, acts []core.Action) []core.Action {
	c := w.r.d.rcluster
	if !w.hello {
		w.hello = true
		for n := range c.reps {
			c.nextID++
			a := core.Action{Kind: core.ActNetSend, Peer: core.NodeID{Node: uint32(n)}}
			a.Msg.Len = uint16(copy(a.Msg.Body[:], AppendHello(nil, &Hello{ID: c.nextID, Session: 1, Incarnation: w.inc})))
			acts = append(acts, a)
		}
	}
	var op faultOp
	if len(w.reissue) > 0 {
		op, w.reissue = w.reissue[0], w.reissue[1:]
	} else {
		pg := core.PGID{Pool: testPool, Index: uint32(w.rng.Below(4))}
		op = faultOp{c: chunkIn(pg, 4, uint32(w.rng.Below(2))), off: uint32(w.rng.Below(4)) * core.BlockSize, n: core.BlockSize}
		if w.rng.Chance(1, 4) {
			op.off, op.n = uint32(w.rng.Below(4*core.BlockSize-300)), 1+uint32(w.rng.Below(300))
		}
		op.data = make([]byte, op.n)
		for i := range op.data {
			op.data[i] = byte(w.rng.Below(255) + 1)
		}
	}
	pg := core.PGID{Pool: testPool, Index: core.PGIndexOf(op.c.Hash(), 4)}
	_, e := c.table.entry(pg)
	lead, ok := e.primary()
	if !ok {
		return acts
	}
	c.nextID++
	id := c.nextID
	w.at[id], w.ops[id] = c.s.Time(), op
	w.r.writes[id] = incWrite{inc: w.inc, chunk: op.c}
	u := Update{Kind: KindWrite, ID: id, PG: c.pgFields(pg), Session: 1, Incarnation: w.inc, Chunk: op.c, Off: op.off, Len: op.n}
	a := core.Action{Kind: core.ActNetSend, Peer: core.NodeID{Node: uint32(c.node(lead))}}
	a.Msg.Len = uint16(copy(a.Msg.Body[:], AppendUpdate(nil, &u)))
	buf, err := ctx.Pool.AllocZeroed(dataBytes(op.off, op.n))
	if err != nil {
		panic(err)
	}
	copy(ctx.Pool.Bytes(buf)[op.off%core.BlockSize:], op.data)
	a.Msg.Buf = buf
	c.cl.sent[id] = buf
	return append(acts, a)
}

// pgReader reads a block of a chunk of a random PG at a random disk node every 5 to 25 ms, tagged with one of the last four tables.
type pgReader struct {
	r    *readRun
	rng  *core.Rand
	next core.Tick
}

func (rd *pgReader) Ready() bool { return rd.r.d.s.Time() >= rd.next }

func (rd *pgReader) Next(ctx *core.Context, acts []core.Action) []core.Action {
	r, c := rd.r, rd.r.d.rcluster
	rd.next = c.s.Time() + 5_000 + core.Tick(rd.rng.Below(20_000))
	tbl := r.tables[len(r.tables)-1-int(rd.rng.Below(uint64(min(4, len(r.tables)))))]
	pg := core.PGID{Pool: testPool, Index: uint32(rd.rng.Below(4))}
	_, e := tbl.entry(pg)
	c.nextID++
	id := c.nextID
	read := Read{ID: id, PG: PGFields{Epoch: tbl.Epoch, PG: pg, Interval: e.Interval}, Chunk: chunkIn(pg, 4, uint32(rd.rng.Below(2))), Len: core.BlockSize}
	r.reads[id] = readVersion{pg: pg, chunk: read.Chunk}
	a := core.Action{Kind: core.ActNetSend, Peer: core.NodeID{Node: uint32(rd.rng.Below(uint64(len(c.reps))))}}
	a.Msg.Len = uint16(copy(a.Msg.Body[:], AppendRead(nil, &read)))
	return append(acts, a)
}

// runReads runs the read workload for 30 s of faults and 10 s after, failing t on a broken invariant registered with the simulator; with a plant, it checks only the plant's invariant and returns its first violation.
func runReads(t *testing.T, seed uint64, plant *readPlant) error {
	set := noisy()
	r := &readRun{rng: core.NewRand(seed ^ 0x4ead), reads: map[core.RequestID]readVersion{}, writes: map[core.RequestID]incWrite{}, acked: map[chunkVersion]bool{}, faulted: -1, back: -1}
	r.returns = r.rng.Chance(1, 2)
	d := newDCluster(t, seed, set)
	r.d, d.cut = d, map[int]bool{}
	c := d.rcluster
	for _, x := range d.a.rec.Disks() {
		d.faulty[x.ID] = true
	}
	if plant != nil && plant.set != nil {
		plant.set(r)
	}
	for n := range c.reps {
		c.reps[n].plant, c.reps[n].Store().plant = c.replicaPlants, c.storePlants
	}
	c.inv.Register(invOneReader, r.oneReader)
	r.epochs = watchEpochs(c, &d.a.rec)
	c.inv.Register(invReadVersion, func() error { return r.verErr })
	c.inv.Register(invIncarnation, func() error {
		if !r.ended {
			return nil
		}
		return c.incarnations(r.writes)
	})
	c.onPass = r.pass
	epochs := c.onEvent
	c.onEvent = func(d int, ev *core.Event, done bool) {
		if !done {
			r.read(d, ev)
		}
		epochs(d, ev, done)
	}
	if plant != nil {
		c.planted = true
		if plant.inv != "" {
			c.inv.Only(plant.inv)
		}
	}
	r.tables = append(r.tables, c.table)
	now := c.s.Time()
	die := now + 4_000_000 + core.Tick(r.rng.Below(8_000_000))
	first := &incWriter{r: r, rng: r.rng.Fork(), inc: 1, start: now, stop: die, at: map[core.RequestID]core.Tick{}, ops: map[core.RequestID]faultOp{}, hello: true}
	second := &incWriter{r: r, rng: r.rng.Fork(), inc: 2, start: die + core.Tick(r.rng.Below(1_000_000)), stop: now + 25_000_000, at: map[core.RequestID]core.Tick{}, ops: map[core.RequestID]faultOp{}}
	cl := c.s.Node(c.client.Node)
	cl.AddActor(c.client, first)
	cl.AddActor(c.client, second)
	cl.AddActor(c.client, &pgReader{r: r, rng: r.rng.Fork()})
	r.next = now + 1_000_000
	end := now + 30_000_000
	for c.s.Time() < end+10_000_000 {
		d.tend()
		if t := c.table; t.Epoch != r.tables[len(r.tables)-1].Epoch {
			r.tables = append(r.tables, t)
		}
		if second.reissue == nil && c.s.Time() >= second.start {
			second.reissue = []faultOp{}
			for _, id := range slices.Sorted(maps.Keys(first.at)) {
				if _, done := c.cl.replies[id]; !done {
					second.reissue = append(second.reissue, first.ops[id])
				}
			}
		}
		if r.back >= 0 {
			r.rejoin()
		}
		switch now := c.s.Time(); {
		case r.faulted >= 0 && now >= r.until:
			r.heal()
		case r.faulted < 0 && r.back < 0 && now >= r.next && now < end:
			r.inject()
		}
		if !c.step() {
			break
		}
		if c.found != nil {
			return c.found
		}
	}
	if r.verErr == nil {
		r.verErr = r.lost()
	}
	r.ended = true
	if err := c.inv.Check(); err != nil {
		if plant == nil {
			t.Fatal(err)
		}
		return err
	}
	return nil
}

// Property 1 of the epoch change model on replicas, and property 11: the read run's disk nodes are cut off, missing tables, paused, rebooted and slowed one at a time on noisy clocks; no two nodes may serve a PG's reads at once, no read returns a version neither acknowledged nor kept, or one a later interval dropped, and no acknowledged write of a dead client incarnation follows, in its chunk, an acknowledged write of its successor.
func TestNoPGServesReadsFromTwoPrimaries(t *testing.T) {
	seeds.Run(t, 0, 10, func(seed uint64) {
		if err := runReads(t, seed, nil); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
	})
}

// Each planted bug breaks its invariant, checked alone, at a seed of the first 500: the epoch change model's neg-fence-on-arrival (27 of 500), a primary that checks a write's incarnation as it arrives rather than as its store orders it, so that a dead incarnation's write held on a chunk the primary lacks is ordered after its successor's; and a primary that commits a version once its own log holds it (279 of 500), so that a read returns a version the next interval drops.
func TestPlantedReadBugsBreakTheReadRun(t *testing.T) {
	for _, p := range []struct {
		name string
		seed uint64
		readPlant
	}{
		{"incarnation checked on arrival", 29, readPlant{inv: invIncarnation, set: func(r *readRun) { r.d.replicaPlants.fenceOnArrival = true }}},
		{"commit on the primary's own log", 0, readPlant{inv: invReadVersion, set: func(r *readRun) { r.d.replicaPlants.commitOnOwnLog = true }}},
	} {
		if err := runReads(t, p.seed, &p.readPlant); err == nil || !strings.Contains(err.Error(), p.inv) {
			t.Errorf("%s at seed %d: %v", p.name, p.seed, err)
		}
	}
}
