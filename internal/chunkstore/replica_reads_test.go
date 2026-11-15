package chunkstore

import (
	"fmt"
	"maps"
	"math"
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
	invFencedWrite = "no fenced session's write is applied"
	invLockOrder   = "every write of a fenced lock holder that completes is ordered before every write of the next holder"
)

// readRun is a detector cluster on noisy clocks whose disk nodes are, one at a time, cut off from every other node, the authority's included, so that they miss tables, and in half the cuts from the client too (with fence, half the cuts take a second node, which still reaches the first), paused, rebooted or slowed on a path, while a client reads every PG at every node, tagged with one of the last few tables, and two incarnations of one client session write: the first dies partway, and its successor says hello to every node, reissues the writes left unanswered and writes on.
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
	// returns has a node cut off lead its PGs once marked up again; cut is the nodes the fault cuts
	// off, and back those whose cut healed that get no tables until then, or until backBy.
	returns bool
	cut     []int
	back    []int
	backBy  core.Tick
	// fence has the writers be sessions 1 and 2: the first, cut off from the control plane, writes
	// on for up to 2 s after its fence, which reaches node n at due[n], and the second starts only
	// once every node with a disk marked up holds the fence; applied is, for each store as it took
	// that table, the session 1 writes it had ordered, and fenceErr the first it ordered after.
	fence    bool
	applied  map[*Store]map[uint64]bool
	fenceErr error
	due      []core.Tick
	// reqSession is the session of each write a primary took, and verSession of each it ordered.
	reqSession map[opKey]uint64
	verSession map[chunkVersion]uint64
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

// incWrite is a write of the first writer (inc 1) or of its successor (inc 2).
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
	if r.fence && r.fenceErr == nil {
		r.fenceErr = r.fencedApplied(d)
	}
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

// fencedApplied notes the session of each write node d takes as a primary, by its store request
// and, once ordered, by its version, and reports a write of session 1 that its store ordered once it
// held the table that fences it.
func (r *readRun) fencedApplied(d int) error {
	st := r.d.reps[d].Store()
	for _, v := range r.d.reps[d].vers {
		r.reqSession[opKey{st, v.req}] = v.u.Session
		if v.ordered {
			r.verSession[chunkVersion{v.u.Chunk, v.u.Version}] = v.u.Session
		}
	}
	if st.Sessions().Version == 0 {
		return nil
	}
	for i := range st.ops {
		if o := &st.ops[i]; r.ordered1(st, o) && o.err == nil && !r.applied[st][o.req.ID] {
			return fmt.Errorf("%s: node %d ordered session 1's write of %v as %#x after it held the fence", invFencedWrite, d, o.req.Chunk, o.version)
		}
	}
	return nil
}

// ordered1 reports whether o is a write or punch of session 1 that st ordered, by the session of
// its primary's write, since a plant may clear the store's.
func (r *readRun) ordered1(st *Store, o *op) bool {
	if o.kind == 0 || o.kind == opRead || o.req.Recover || o.version == 0 {
		return false
	}
	s, ok := r.reqSession[opKey{st, o.req.ID}]
	if !ok {
		s = r.verSession[chunkVersion{o.req.Chunk, o.version}]
	}
	return s == 1
}

// opKey is a store's request.
type opKey struct {
	st *Store
	id uint64
}

// fenceNode hands node n's replica the newest session table, noting the session 1 writes its store
// has ordered.
func (r *readRun) fenceNode(n int) {
	c := r.d.rcluster
	t := c.sessions
	c.call(n, func(ctx *core.Context, rep *Replica, out *ReplicaOutput) {
		st, seen := rep.Store(), map[uint64]bool{}
		for i := range st.ops {
			if o := &st.ops[i]; r.ordered1(st, o) {
				seen[o.req.ID] = true
			}
		}
		if st.Sessions().Version < t.Version {
			r.applied[st] = seen
		}
		rep.SetSessions(ctx, t, c.sessionAsks[n], out)
	})
}

// fenced reports whether every node with a disk the detector marks up holds the newest session table.
func (r *readRun) fenced() bool {
	c := r.d.rcluster
	for _, x := range r.d.a.rec.Disks() {
		if m, _ := r.d.det.Mark(x.ID); m == MarkUp && c.reps[x.Node].Store().Sessions().Version < c.sessions.Version {
			return false
		}
	}
	return true
}

// incarnations checks that no acknowledged write of the session's first incarnation has a later version of its chunk than an acknowledged write of the second.
func (c *rcluster) incarnations(writes map[core.RequestID]incWrite, inv string) error {
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
			return fmt.Errorf("%s: write %d of writer 1 acknowledged as %#x of %v, write %d of writer 2 as %#x", inv, a, c.cl.replies[a].Version, ch, b, c.cl.replies[b].Version)
		}
	}
	return nil
}

// inject draws a fault on a running disk node that stands still; with fence, half its cuts take a
// second such node with it, the two still reaching each other, as a rack cut off would.
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
		r.cut = append(r.cut[:0], n)
		if r.fence && r.rng.Chance(1, 2) {
			if m := (n + 1 + int(r.rng.Below(uint64(len(c.reps)-1)))) % len(c.reps); c.s.Node(uint32(m)).Up() && !c.s.Node(uint32(m)).Paused() {
				r.cut = append(r.cut, m)
			}
		}
		for _, x := range r.cut {
			for k := range uint32(len(c.reps) + 2) {
				if !slices.Contains(r.cut, int(k)) && (k != c.client.Node || r.rng.Chance(1, 2)) {
					c.s.Partition(uint32(x), k, false)
				}
			}
			r.d.cut[x] = true
		}
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
		for _, x := range r.cut {
			for k := range uint32(len(c.reps) + 2) {
				c.s.Heal(uint32(x), k)
			}
		}
		r.back, r.backBy = slices.Clone(r.cut), c.s.Time()+3_000_000
	case 3:
		c.s.Queue(uint32(n), r.other, 0)
	}
	r.faulted = -1
	r.next = c.s.Time() + 500_000 + core.Tick(r.rng.Below(2_500_000))
}

// rejoined hands node n, cut off, the newest table, as its next fetch would, once the detector marks its disk up again or 3 s after the cut healed, and reports whether it did; with returns, the table that marks it up has it lead every PG it is a member of.
func (r *readRun) rejoined(n int) bool {
	c := r.d.rcluster
	up, _ := r.d.det.Mark(c.ids[n])
	if up != MarkUp && c.s.Time() < r.backBy {
		return false
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
	delete(r.d.cut, n)
	r.d.tend()
	if c.s.Node(uint32(n)).Up() {
		t := c.table
		c.call(n, func(ctx *core.Context, rep *Replica, out *ReplicaOutput) { rep.SetTable(ctx, t, out) })
	}
	return true
}

// tendSessions fences session 1 at die and hands the table to each running node not cut off once
// its own delay of up to a heartbeat has passed, starting the second writer once every node with a
// disk up holds it.
func (r *readRun) tendSessions(die core.Tick, second *incWriter) {
	c, now := r.d.rcluster, r.d.s.Time()
	if now < die {
		return
	}
	if c.sessions.Version == 0 {
		c.sessions = SessionTable{Version: 1, Next: 3, Live: []uint64{2}}
		for range c.reps {
			r.due = append(r.due, now+core.Tick(r.rng.Below(uint64(RetryAfter))))
		}
	}
	for n := range c.reps {
		if c.s.Node(uint32(n)).Up() && !r.d.cut[n] && now >= r.due[n] && c.reps[n].Store().Sessions().Version < c.sessions.Version {
			r.fenceNode(n)
		}
	}
	if second.start == math.MaxInt64 && r.fenced() {
		second.start = now
	}
}

// incWriter is one incarnation of a client session, writing blocks and parts of blocks of two chunks of every PG with up to 6 writes unanswered: the first stops at stop, the second starts at start with a hello to every node and the first's unanswered writes.
type incWriter struct {
	r       *readRun
	rng     *core.Rand
	session uint64
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
			a.Msg.Len = uint16(copy(a.Msg.Body[:], AppendHello(nil, &Hello{ID: c.nextID, Session: w.session, Incarnation: w.inc})))
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
	u := Update{Kind: KindWrite, ID: id, PG: c.pgFields(pg), Session: w.session, Incarnation: w.inc, Chunk: op.c, Off: op.off, Len: op.n}
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

// pgReader reads the first 4 blocks of a chunk of a random PG at a random disk node every 5 to 25 ms, tagged with one of the last four tables or, half the time, with the node's own, as a client that missed tables with it would.
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
	read := Read{ID: id, PG: PGFields{Epoch: tbl.Epoch, PG: pg, Interval: e.Interval}, Chunk: chunkIn(pg, 4, uint32(rd.rng.Below(2))), Len: 4 * core.BlockSize}
	r.reads[id] = readVersion{pg: pg, chunk: read.Chunk}
	a := core.Action{Kind: core.ActNetSend, Peer: core.NodeID{Node: uint32(rd.rng.Below(uint64(len(c.reps))))}}
	if own := c.reps[a.Peer.Node].table; rd.rng.Chance(1, 2) {
		if _, e := own.entry(pg); e != nil {
			read.PG = PGFields{Epoch: own.Epoch, PG: pg, Interval: e.Interval}
		}
	}
	a.Msg.Len = uint16(copy(a.Msg.Body[:], AppendRead(nil, &read)))
	return append(acts, a)
}

// runReads runs the read workload for 30 s of faults and 10 s after, failing t on a broken invariant registered with the simulator; with a plant, it checks only the plant's invariant and returns its first violation.
func runReads(t *testing.T, seed uint64, plant *readPlant) error {
	return runReadsWith(t, seed, false, plant)
}

// runReadsWith is runReads, with fence the second writer a new session, which writes once every
// node with a disk up holds the session table that fences the first.
func runReadsWith(t *testing.T, seed uint64, fence bool, plant *readPlant) error {
	set := noisy()
	r := &readRun{rng: core.NewRand(seed ^ 0x4ead), reads: map[core.RequestID]readVersion{}, writes: map[core.RequestID]incWrite{}, acked: map[chunkVersion]bool{}, faulted: -1,
		fence: fence, applied: map[*Store]map[uint64]bool{}, reqSession: map[opKey]uint64{}, verSession: map[chunkVersion]uint64{}}
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
	order := invIncarnation
	if fence {
		order = invLockOrder
		c.sessions = SessionTable{Next: 3, Live: []uint64{1, 2}}
		for n := range c.reps {
			c.reps[n].Store().SetSessions(c.sessions)
		}
		d.det.Lags = func(node uint32) bool { return c.reps[node].Store().Sessions().Version < c.sessions.Version }
		c.inv.Register(invFencedWrite, func() error { return r.fenceErr })
	}
	c.inv.Register(order, func() error {
		if !r.ended {
			return nil
		}
		return c.incarnations(r.writes, order)
	})
	x := c.record()
	c.inv.Register(invLinearizable, func() error {
		if !r.ended {
			return nil
		}
		return x.check()
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
	first := &incWriter{r: r, rng: r.rng.Fork(), session: 1, inc: 1, start: now, stop: die, at: map[core.RequestID]core.Tick{}, ops: map[core.RequestID]faultOp{}, hello: true}
	second := &incWriter{r: r, rng: r.rng.Fork(), session: 1, inc: 2, start: die + core.Tick(r.rng.Below(1_000_000)), stop: now + 25_000_000, at: map[core.RequestID]core.Tick{}, ops: map[core.RequestID]faultOp{}}
	if fence {
		first.stop += core.Tick(r.rng.Below(2_000_000))
		second.session, second.start = 2, math.MaxInt64
	}
	c.addClient(first)
	c.addClient(second)
	c.addClient(&pgReader{r: r, rng: r.rng.Fork()})
	r.next = now + 1_000_000
	end := now + 30_000_000
	for c.s.Time() < end+10_000_000 {
		d.tend()
		if t := c.table; t.Epoch != r.tables[len(r.tables)-1].Epoch {
			r.tables = append(r.tables, t)
		}
		if fence {
			r.tendSessions(die, second)
		}
		if second.reissue == nil && c.s.Time() >= second.start {
			second.reissue = []faultOp{}
			for _, id := range slices.Sorted(maps.Keys(first.at)) {
				if _, done := c.cl.replies[id]; !done {
					second.reissue = append(second.reissue, first.ops[id])
				}
			}
		}
		r.back = slices.DeleteFunc(r.back, r.rejoined)
		switch now := c.s.Time(); {
		case r.faulted >= 0 && now >= r.until:
			r.heal()
		case r.faulted < 0 && len(r.back) == 0 && now >= r.next && now < end:
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

// The read run's writers as two sessions: the first is fenced when it dies, and the second, its
// locks' next holder, writes once every node with a disk up holds the fence. No store orders a
// write of the fenced session once it holds the fence, and every write of the first that completes
// is ordered, in its chunk, before every write of the second.
func TestNoFencedSessionsWriteIsApplied(t *testing.T) {
	seeds.Run(t, 0, 10, func(seed uint64) {
		if err := runReadsWith(t, seed, true, nil); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
	})
}

// Each planted bug breaks its invariant, checked alone, at a seed of the first 500: the epoch change model's neg-fence-on-arrival (21 of 500), a primary that checks a write's incarnation as it arrives rather than as its store orders it, so that a dead incarnation's write held on a chunk the primary lacks is ordered after its successor's; and a primary that commits a version once its own log holds it (368 of 500), so that a read returns a version the next interval drops.
// With the writers as two sessions, the sessions model's neg-check-on-arrival (3 of 500), the same
// primary, orders a fenced session's write that waited in its store as the fence came; and its
// neg-replica-no-check (132 of 500), a member that takes a replicated write without checking its
// session, logs one its primary ordered before the fence reached it.
func TestPlantedReadBugsBreakTheReadRun(t *testing.T) {
	for _, p := range []struct {
		name  string
		seed  uint64
		fence bool
		readPlant
	}{
		{"incarnation checked on arrival", 29, false, readPlant{inv: invIncarnation, set: func(r *readRun) { r.d.replicaPlants.fenceOnArrival = true }}},
		{"commit on the primary's own log", 0, false, readPlant{inv: invReadVersion, set: func(r *readRun) { r.d.replicaPlants.commitOnOwnLog = true }}},
		{"session checked on arrival", 52, true, readPlant{inv: invFencedWrite, set: func(r *readRun) { r.d.replicaPlants.fenceOnArrival = true }}},
		{"member takes a fenced session's write", 5, true, readPlant{inv: invFencedWrite, set: func(r *readRun) { r.d.replicaPlants.memberNoFence = true }}},
	} {
		if err := runReadsWith(t, p.seed, p.fence, &p.readPlant); err == nil || !strings.Contains(err.Error(), p.inv) {
			t.Errorf("%s at seed %d: %v", p.name, p.seed, err)
		}
	}
}
