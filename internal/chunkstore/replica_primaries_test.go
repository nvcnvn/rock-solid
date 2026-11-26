package chunkstore

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
	"github.com/nvcnvn/rock-solid/internal/trace"
)

const (
	invAckedKept   = "no acknowledged version is ever dropped"
	invOneVersion  = "no two members hold different data under one version"
	invKeptWritten = "a member listing nothing maps the kept writes applied in version order"
	invRemoval     = "no move lowers the members of a PG holding an acknowledged version below its width"
	invStranded    = "an acknowledged write a disk can serve, a member can serve"
	invOnlyLost    = "a give-up loses only what no disk left holds"
)

// authority stands in for the control plane: it knows each disk by the log ID it reports, computes every table with Next from the disks' marks and drains and operators' edits, and records the activations and asks the replicas report; with moves it makes a balance move per table and hands Next every recovered report again each time, as reports delivered late or twice.
type authority struct {
	rec        DiskRecord
	pool       Pool
	down       map[uint32]core.Tick
	drain      map[uint32]bool
	maint      map[uint32]bool
	edits      []Edit
	giveUps    []PGGiveUp
	asks       []PGInterval
	acts, seen int
	moves      bool
	plant      placementPlants
	// det, when set, gives the disks' marks in place of down.
	det *Detector
	// full is the disks at their log reserve whose members of PG 0, Stuck, its primary reports short.
	full []uint32
	// keepID plants the bug of a reformatted disk keeping its old disk ID.
	keepID bool
	// cp, when set, is the real control plane on c's node auth, which commits every table in place
	// of Next (control_plane_test.go): cut is the nodes cut off from it, told the drains it took,
	// rcv the recovered reports counted, reports the reports forwarded to it, opened the logs each
	// node reports beside its replica's, refused the heartbeats it refused, and latencies the member
	// latencies its heartbeats carried; late is the nodes attach leaves to join later, and size
	// the size and log reserve each replica's log reports, diskBlocks and none if unset, or sizes
	// a node's.
	t         *testing.T
	c         *rcluster
	cp        ControlPlane
	cut       map[uint32]bool
	told      map[uint32]bool
	rcv       int
	reports   uint64
	opened    map[uint32][]OpenedLog
	refused   []error
	latencies int
	late      map[int]bool
	size      OpenedLog
	sizes     map[uint32]OpenedLog
}

func (a *authority) inputs(c *rcluster) *Inputs {
	in := &Inputs{Now: c.s.Time(), Pools: a.pools(), Edits: a.edits, Asks: a.asks, GiveUps: a.giveUps}
	a.edits, a.asks, a.giveUps = nil, nil, nil
	in.Activations = c.table.reported(nil, c.activations[a.acts:], a.plant)
	for _, x := range c.asks[a.seen:] {
		in.Asks = append(in.Asks, PGInterval{x.PG, x.Interval})
	}
	a.acts, a.seen = len(c.activations), len(c.asks)
	if a.moves {
		in.Moves = 1
	}
	in.Recovered = c.table.reported(nil, c.recovered, a.plant)
	in.plant = a.plant
	if len(a.full) > 0 {
		_, e := c.table.entry(pg0)
		s := StuckPG{PG: pg0.Index, Interval: e.Interval}
		for p, m := range e.Members {
			if slices.Contains(a.full, m.Disk) {
				s.Short |= 1 << p
			}
		}
		in.Pools[0].Stuck = []StuckPG{s}
	}
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
		if slices.Contains(a.full, d.ID) {
			dk.Live = dk.Blocks
		}
		if since, ok := a.down[d.ID]; ok {
			dk.Mark, dk.DownSince = MarkDown, since
		}
		if a.maint[d.ID] {
			dk.Mark = MarkMaint
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
	// runOn keeps a planted run going past its first broken invariant.
	runOn bool
	ended bool
	ops   map[core.RequestID]faultOp
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
	// crashLeaving crashes a leaving member of PG 0 as the table removing it is published; leftDrops counts the chunks
	// whose versions a restart's replay gated and that the disk dropped once no interval of their PG named it.
	crashLeaving bool
	leftDrops    int
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
	u := Update{Kind: KindWrite, ID: id, Oldest: sendOnceOldest(id), PG: c.pgFields(pg0), Session: 1, Incarnation: 1, Chunk: op.c, Off: off, Len: n}
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
	next := r.a.next(c)
	if d := MakeDelta(&c.table, &next); len(d.Entries) == 0 {
		return
	}
	r.removal(&next)
	r.stranded(&next)
	if r.crashLeaving {
		_, e := next.entry(pg0)
		for _, m := range r.members() {
			if n := c.node(m.Disk); m.Leaving && n >= 0 && c.s.Node(uint32(n)).Up() && !slices.ContainsFunc(e.Members, func(x Member) bool { return x.Disk == m.Disk }) {
				c.s.CrashRandom(uint32(n))
			}
		}
	}
	c.noteTable(&next)
	c.table = next
	for n := range c.reps {
		if !c.s.Node(uint32(n)).Up() {
			continue
		}
		c.call(n, func(ctx *core.Context, rep *Replica, out *ReplicaOutput) {
			c.address(rep)
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

// stranded checks every acknowledged write against the table next.
func (r *primaryRun) stranded(next *Table) {
	_, e := next.entry(pg0)
	for id, op := range r.ops {
		if rp, ok := r.c.cl.replies[id]; ok && rp.Status == StatusOK {
			if err := r.c.stranded(e, op.c, rp.Version, nil); err != nil {
				r.fail(invStranded, err)
			}
		}
	}
}

// pass follows each replica's loop: the versions a primary orders and sends, the decision of each interval it activates, and each member's acknowledgments, checked against the member's index and the bytes other members acknowledged under the same version.
func (r *primaryRun) pass(d int, acts []core.Action) {
	c, rep := r.c, r.c.reps[d]
	if rep.cfg.Restarted {
		r.leftDrops += rep.leftDropped
	}
	rep.leftDropped = 0
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
			if n := c.node(m.Disk); !moving && m.Mark == MarkDown && n >= 0 && c.s.Node(uint32(n)).Up() {
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
	id, err := r.a.report(n, c.logs[n])
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

// settled steps the cluster, the authority answering asks and recording activations, until every led PG is active with no ask of its primary unanswered and nothing but periodic timers is pending.
func (r *primaryRun) settled() {
	c := r.c
	for range 1_000_000 {
		r.tend()
		if c.active() && c.quiet() && !c.askOpen() {
			return
		}
		if !c.step() {
			break
		}
	}
	if !c.planted {
		c.t.Fatal("the cluster did not settle")
	}
	c.found = cmp.Or(c.found, errors.New("the cluster did not settle"))
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
	if len(c.asks) > r.a.seen || len(c.activations) > r.a.acts || r.a.due() {
		r.publish()
	}
}

// peerPlant is a bug planted in the primary-change run and the invariant it must break; with none named, every invariant is checked.
type peerPlant struct {
	inv string
	set func(r *primaryRun)
}

// runPrimaries runs the primary-change workload, failing t on a broken invariant registered with the
// simulator and returning a broken end check; with a plant, it checks only the plant's invariant, if it
// names one, and returns its first violation.
func runPrimaries(t *testing.T, seed uint64, plant *peerPlant) error {
	_, err := runPrimaryRun(t, seed, plant, false)
	return err
}

// runPrimaryRun is runPrimaries, returning the run too; with crashLeaving, each leaving member of PG 0 crashes as the
// table removing it is published.
func runPrimaryRun(t *testing.T, seed uint64, plant *peerPlant, crashLeaving bool) (*primaryRun, error) {
	return runPrimaryRunAt(t, seed, plant, crashLeaving, 2)
}

// runPrimaryRunAt is runPrimaryRun on a pool with min_live minLive.
func runPrimaryRunAt(t *testing.T, seed uint64, plant *peerPlant, crashLeaving bool, minLive int) (*primaryRun, error) {
	return runPrimaryRunOn(t, seed, plant, crashLeaving, minLive, false)
}

// runPrimaryRunOn is runPrimaryRunAt, on the real control plane if real.
func runPrimaryRunOn(t *testing.T, seed uint64, plant *peerPlant, crashLeaving bool, minLive int, real bool) (*primaryRun, error) {
	s := sim.New(seed, sim.DefaultSettings())
	a := &authority{pool: Pool{Replicas: 3, MinLive: minLive, FailureDomain: DomainNode, MaxPerDomain: 1, Locality: LocalitySpread, Overcommit: true, WriteLimit: 90, PGCount: 4},
		down: map[uint32]core.Tick{}, drain: map[uint32]bool{}, moves: true}
	var tbl Table
	if real {
		tbl = realControl(t, a, 4)
	} else {
		for n := range 4 {
			if _, err := a.rec.Report(uint32(n), 0, 0x5eed+uint64(n), ""); err != nil {
				t.Fatal(err)
			}
		}
		var empty Table
		tbl, _, _ = Next(&empty, &Inputs{Pools: a.pools()})
	}
	c := newRClusterWith(t, tbl, s, func(cfg *Config) {
		cfg.Log.MaxRecordBytes = 3 * core.BlockSize
		// A quarter of the seeds keep two rounds in flight, so a crash can leave a gap in the log and a traced run reaches the gap half of the spec's LostAbove; none of them is a planted bug's seed.
		if seed%8 == 4 || seed%8 == 7 {
			cfg.Log.CommitDepth = 2
		}
	})
	if real {
		a.attach(c)
	}
	c.conformBegin()
	r := &primaryRun{c: c, a: a, rng: core.NewRand(seed ^ 0x9e3), chunks: [2]core.ChunkID{chunkIn(pg0, 4, 0), chunkIn(pg0, 4, 1)}, ops: map[core.RequestID]faultOp{},
		version: map[core.RequestID]uint64{}, kept: map[uint32][]keptChunk{}, tainted: map[uint32]bool{},
		sent: map[[2]uint64]Update{}, acked: map[blockVersion][]byte{}, errs: map[string]error{}}
	r.returns, r.crashLeaving = r.rng.Chance(1, 2), crashLeaving
	for _, inv := range []string{invAckedWhole, invOneVersion, invRemoval, invAckedKept, invKeptWritten, invRecovered} {
		c.inv.Register(inv, func() error { return r.errs[inv] })
	}
	c.inv.Register(invActivates, func() error {
		if !r.ended {
			return nil
		}
		return c.inactive()
	})
	c.inv.Register(invLeftGated, c.leftGated)
	c.inv.Register(invHeldOrdered, func() error {
		if !r.ended {
			return nil
		}
		return c.heldWrites()
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
		if plant.inv != "" {
			c.inv.Only(plant.inv)
		}
	}
	// The real control plane's tables lag its steps, so a client writes more there, or its writes end before a move's interval starts.
	writes := 48
	if real {
		writes = 192
	}
	for range 2 {
		pc := &primaryClient{r: r, rng: r.rng.Fork(), left: writes, pipe: 8, at: map[core.RequestID]core.Tick{}}
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
		if c.found != nil && !r.runOn {
			return r, c.found
		}
		// In the leaving variant a lever pulled while a down mark waits stacks on the PG as it was, and every leaving member's crash lands after the clients end.
		if busy() && !(crashLeaving && a.cutting()) && r.rng.Chance(1, 40) && (c.active() || r.duringPeering && r.rng.Chance(1, 10)) && !slices.ContainsFunc(r.members(), func(m Member) bool { return m.Mark == MarkUp && !r.running(m) }) {
			r.inject()
		}
	}
	for n := range c.reps {
		for !c.s.Node(uint32(n)).Up() && c.step() {
		}
	}
	a.rejoin(c, r.tend)
	clear(a.down)
	_, e := c.table.entry(pg0)
	a.asks = append(a.asks, PGInterval{pg0, e.Interval})
	if c.conforming {
		c.s.Node(c.client.Node).Output(c.client, trace.Record{Name: trace.OutAsk, Fields: [trace.MaxFields]uint64{pgField(pg0), uint64(e.Interval)}})
	}
	r.publish()
	r.settled()
	if c.found != nil && !r.runOn {
		return r, c.found
	}
	r.recover()
	r.check()
	r.readEach(x)
	r.ended = true
	if plant == nil {
		name := "primaries"
		if crashLeaving {
			name = "leftdown"
		}
		if real {
			name = "real-" + name
		}
		conformWrite(t, fmt.Sprintf("%s-%d", name, seed), c)
	}
	if err := c.inv.Check(); err != nil {
		return r, err
	}
	return r, c.found
}

// Sub-block spike M6 on replicas: two clients pipeline 8 writes each to two chunks while the authority moves the PG's primary, to members that stayed live throughout or, in half the seeds, to members marked up again that missed writes, marks replicas down and up, restarts the primary, formats a member again and drains a disk of four and ends the drain, moving members. Every acknowledged version is kept, members hold the same bytes under a version, every member that lists nothing maps the kept writes applied in version order, and no move removes a leaving member while another member lacks an acknowledged version.
func TestPeeringKeepsAcknowledgedWritesAcrossPrimaries(t *testing.T) {
	seeds.Run(t, 0, 10, func(seed uint64) {
		if err := runPrimaries(t, seed, nil); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
	})
}

// The same run with every table computed and committed by the real control plane, meta.Control at
// the leader of a three-replica group, from the replicas' heartbeats, asks, activations and recovered
// reports, the operator's edits and drains through its calls, and a disk marked down by cutting its
// node off from the control plane, whose detector marks it down once silent and up once it heartbeats.
// That delay lets an edit move a primary while it stays up, so it can restart holding in part a
// version the other members hold whole, as TestAMemberHoldingAVersionInPartDoesNotVetoIt scripts.
func TestPeeringKeepsAcknowledgedWritesAcrossPrimariesOnTheRealGroup(t *testing.T) {
	seeds.Run(t, 0, 10, func(seed uint64) {
		if _, err := runPrimaryRunOn(t, seed, nil, false, 2, true); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
	})
}

// The same run with each leaving member crashed as the table removing it is published: restarted, its replay gates
// the writes it logged in the move's interval, which it drops once a table names it in no interval of the PG. The run
// fails if its seeds, more than one, never reach that.
func TestALeftMemberDownAtItsMovesEndKeepsAcknowledgedWrites(t *testing.T) {
	runLeftDown(t, false)
}

// The same run on the real control plane, as TestPeeringKeepsAcknowledgedWritesAcrossPrimariesOnTheRealGroup.
func TestALeftMemberDownAtItsMovesEndKeepsAcknowledgedWritesOnTheRealGroup(t *testing.T) {
	runLeftDown(t, true)
}

// runLeftDown runs the leaving-member variant over the seeds, failing t if more than one ran and
// none dropped a left PG's gated writes.
func runLeftDown(t *testing.T, real bool) {
	run, left := 0, 0
	seeds.Run(t, 0, 10, func(seed uint64) {
		r, err := runPrimaryRunOn(t, seed, nil, true, 2, real)
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		run++
		if r.leftDrops > 0 {
			left++
		}
	})
	t.Logf("%d seeds, %d with a restarted disk dropping the gated writes of a PG no interval names it in", run, left)
	if run > 1 && left == 0 {
		t.Fatalf("none of %d seeds dropped a left PG's gated writes", run)
	}
}

// A disk that never drops the gated writes of a PG it left keeps them past the table naming it in no interval of the PG
// (seed 4, the first of the run's to break it).
func TestALeftPGNeverSettledBreaksItsInvariant(t *testing.T) {
	if _, err := runPrimaryRun(t, 4, &peerPlant{invLeftGated, func(r *primaryRun) { r.c.replicaPlants.leftNeverSettles = true }}, true); err == nil || !strings.Contains(err.Error(), invLeftGated) {
		t.Fatalf("leftNeverSettles at seed 4: %v", err)
	}
}

// Seed 67 formats node 1's disk again, crashes cut that format and the next short, and the disk comes back as disk 7 while the table still names gone disk 6 a down member no node serves: the run must not mark it up.
func TestPeeringNeverMarksUpADiskNoNodeServes(t *testing.T) {
	if err := runPrimaries(t, 67, nil); err != nil {
		t.Fatal(err)
	}
}

// Seed 194 orders a write in interval 3 while one member lists its chunk, and the primary answers it OK while it peers
// interval 4, whose list names the primary's own copy: a list persisted for a later interval than the one answering does
// not uncount a member that logged the version.
func TestAnOKAnsweredWhileThePrimaryPeersCountsItsOwnLoggedCopy(t *testing.T) {
	if err := runPrimaries(t, 194, nil); err != nil {
		t.Fatal(err)
	}
}

// Each planted bug breaks its invariant, checked alone, at a seed of the first 500: peering dropping any subset of a chunk's pending versions rather than a suffix (sub-block spike M7, 26 of 500), a partial-block write merged before the primary holds the versions peering kept (M8, 7 of 500), the epoch change model's neg-merge-on-dropped, a partial-block write merged on a version the primary mapped and peering dropped (7 of 500), and neg-edit-keeps-interval, a table that reorders a PG's disks and keeps its interval (5 of 500), a restarted primary peering in its old interval (16 of 500 at min_live 1; none at 2, where the write that reuses a version goes to a chunk with too few clean copies and waits for recovery), a disk formatted again keeping its old disk ID (212 of 500), the epoch change model's neg-compare-table-epochs (441 of 500) and neg-compare-member-lists (310 of 500), which breaks property 3 first: a member that compares member lists logs the old primary's write tagged with an interval its table left; and the model's neg-remove-before-recovered (120 of 500), here a leaving member removed once its PG's interval activates, and neg-accept-old-report (136 of 500), each removing a moved member while its replacement lacks acknowledged writes; and neg-report-before-activate (500 of 500), a primary that reports its interval activated once it decides, before it activates; a primary that recovers only its own copies (462 of 500), whose members, those a drain refilled among them, never reach the copy count; and neg-no-degraded-hold, a write ordered at once on a chunk with fewer than min_live clean copies (143 of 500); and neg-lister-orders, a write ordered by a primary that lists its chunk, which leaves the chunk unfound with every disk kept (6 of 500 at min_live 1; none at 2, where the count of clean copies holds the write too).
func TestPlantedPeeringBugsBreakTheOracle(t *testing.T) {
	for _, p := range []struct {
		name string
		seed uint64
		peerPlant
	}{
		{"acknowledged on a lone copy", 291, peerPlant{invAckCopies, func(r *primaryRun) { r.c.replicaPlants.ackOnLoneCopy = true }}},
		{"ordered with too few clean copies", 0, peerPlant{invAckCopies, func(r *primaryRun) { r.c.replicaPlants.noDegradedHold = true }}},
		{"M7", 169, peerPlant{invKeptWritten, func(r *primaryRun) { r.c.storePlants.settleSubset = true }}},
		{"M8", 16, peerPlant{invKeptWritten, func(r *primaryRun) { r.c.replicaPlants.mergeWhileLacking, r.c.replicaPlants.listerOrders = true, true }}},
		{"recovered from a primary that lacks the chunk", 1, peerPlant{invOneVersion, func(r *primaryRun) { r.c.replicaPlants.recoverFromListed = true }}},
		{"a list cleared with no source", 378, peerPlant{invKeptWritten, func(r *primaryRun) { r.c.replicaPlants.clearNoSource = true }}},
		{"refused as gone while up", 0, peerPlant{invSameInterval, func(r *primaryRun) { r.c.replicaPlants.goneWhileUp = true }}},
		{"merged on a dropped version", 0, peerPlant{invKeptWritten, func(r *primaryRun) { r.c.replicaPlants.mergeOnDropped = true }}},
		{"edit keeps its interval", 21, peerPlant{invOneVersion, func(r *primaryRun) { r.a.plant.editKeepsInterval = true }}},
		{"old disk ID kept", 2, peerPlant{invDiskIDs, func(r *primaryRun) { r.a.keepID = true }}},
		{"a new disk answers for its old ID", 2, peerPlant{invAckedKept, func(r *primaryRun) { r.c.replicaPlants.answerForOldDisk = true }}},
		{"accepted only in the send epoch", 0, peerPlant{invSameInterval, func(r *primaryRun) { r.c.replicaPlants.sendEpoch = true }}},
		{"member lists compared", 1, peerPlant{invLateLog, func(r *primaryRun) { r.c.replicaPlants.sameMembers = true }}},
		{"removed before recovered", 3, peerPlant{invRemoval, func(r *primaryRun) { r.a.plant.removeOnActivation = true }}},
		{"old recovered report", 3, peerPlant{invRemoval, func(r *primaryRun) { r.a.plant.oldReport = true }}},
		{"activation reported once decided", 0, peerPlant{invActivations, func(r *primaryRun) { r.c.replicaPlants.reportDecided = true }}},
		{"members never recovered", 0, peerPlant{invRecovered, func(r *primaryRun) { r.c.replicaPlants.recoverSelfOnly = true }}},
	} {
		if err := runPrimaries(t, p.seed, &p.peerPlant); err == nil || !strings.Contains(err.Error(), p.inv) {
			t.Errorf("%s at seed %d: %v", p.name, p.seed, err)
		}
	}
	if _, err := runPrimaryRunAt(t, 57, &peerPlant{invOneVersion, func(r *primaryRun) { r.c.replicaPlants.peerInOldInterval = true }}, false, 1); err == nil || !strings.Contains(err.Error(), invOneVersion) {
		t.Errorf("restart in the old interval at min_live 1, seed 57: %v", err)
	}
	if _, err := runPrimaryRunAt(t, 20, &peerPlant{invHasSource, func(r *primaryRun) { r.c.replicaPlants.listerOrders = true }}, false, 1); err == nil || !strings.Contains(err.Error(), invHasSource) {
		t.Errorf("a write ordered by a primary that lists its chunk at min_live 1, seed 20: %v", err)
	}
	if err := fullParkRun(t, func(a *authority) { a.plant.swapFull = true }, false); err == nil || !strings.Contains(err.Error(), invStranded) {
		t.Errorf("a full member swapped out: %v", err)
	}
	if err := fullUnpairedRun(t, func(a *authority) { a.plant.parkUnpaired = true }); err == nil || !strings.Contains(err.Error(), invRemoval) {
		t.Errorf("full members parked with no member added: %v", err)
	}
	soleHolder := func(a *authority) { a.plant.refillSoleHolder = true }
	if err := lastSourceRun(t, soleHolder, false, ""); err == nil || !strings.Contains(err.Error(), invStranded) {
		t.Errorf("a refill that deletes a member merely down: %v", err)
	}
	if err := staySourceRun(t, soleHolder, ""); err == nil || !strings.Contains(err.Error(), invStranded) {
		t.Errorf("a Stay that deletes a member merely down: %v", err)
	}
}
