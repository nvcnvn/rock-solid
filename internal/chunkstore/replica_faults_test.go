package chunkstore

import (
	"bytes"
	"fmt"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/lsm"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

const (
	invAckedWhole    = "a member acknowledges a version only once it maps every block of it"
	invOneReply      = "every client write or punch gets one reply"
	invMappedAnswers = "the primary maps exactly the writes and punches it answered OK and those peering kept, in version order, in every chunk it lists nothing of"
	invRecovered     = "once recovery settles, every member lists nothing and maps the kept writes, but in an unfound chunk"
)

// faultSpan is the bytes of each chunk the fault workload writes: 3 blocks.
const faultSpan = 3 * core.BlockSize

// faultOp is a client's write, or a punch when data is nil, of n bytes at off of chunk c.
type faultOp struct {
	c    core.ChunkID
	off  uint32
	n    uint32
	data []byte
}

type answered struct {
	version uint64
	id      core.RequestID
}

// faultRun runs clients pipelining sub-block writes and punches to two chunks of PG 0 while a seeded schedule cuts members off from the primary, crashes them, often while they hold a version in part, fails their disks and at times the primary's, has them refuse, rots the primary's copy of blocks, sometimes cutting off the member it asks first for a base, changes the PG's interval, crashes the primary, rebuilds its index and moves it to another member, after which the PG peers and recovers. Records of at most 3 blocks cut a version into several.
type faultRun struct {
	t      *testing.T
	c      *rcluster
	rng    *core.Rand
	chunks [2]core.ChunkID
	ops    map[core.RequestID]faultOp
	// ok is the writes and punches a primary answered OK, in version order, and replied each it answered; refused is those it refused, version each one's version once a primary sent it, and kept the decision of the peering of each interval of PG 0.
	ok      []answered
	replied map[core.RequestID]int
	refused []core.RequestID
	version map[core.RequestID]uint64
	kept    map[uint32][]keptChunk
	dirty   bool
	// images is each chunk's bytes, by its place in chunks and a version, as the first disk that
	// answered for that version with its chunk whole at it and listing nothing mapped them; ended
	// is set once the run stops for its final checks.
	images map[[2]uint64][]byte
	ended  bool
	// sent is each update a primary sent, by its node and the update's ID; flips the bits rotted on each node's disk; to the node each request went to, and lost the requests a crash of that node left unanswered.
	sent  map[[2]uint64]Update
	flips map[flipAt][]flip
	to    map[core.RequestID]int
	lost  map[core.RequestID]bool
	errs  map[string]error
	// heals is when each cut between two nodes is healed, and crashes when each node is crashed at the latest, 0 for none; peering is whether PG 0's new interval waits for the primary to peer.
	heals   map[[2]uint32]core.Tick
	crashes [3]core.Tick
	peering bool
	asked   int
	// a is the stand-in authority, whose detector marks the disks; changes is the detector's count of marks the table follows.
	a       *authority
	changes uint64
	// primaryFailed is set once a primary's disk has been made to fail, and rebuilt once a primary's index has rebuilt; listed is the primary's listing of each chunk at its last pass.
	primaryFailed bool
	rebuilt       bool
	listed        [2]uint32
	// fails counts the restarts of each node since its store last served, and gone is the nodes
	// whose store failed again as it opened, which stay down: the detector marks them down for good.
	fails [3]int
	gone  [3]bool
	// failing is the nodes a disk fault was injected into until the fault stops the store and it
	// serves again; one at a time, and none once a disk is gone, so no more than one disk is ever
	// lost.
	failing [3]bool
}

// sound reports whether a disk fault may be injected: none is pending and no disk is gone.
func (f *faultRun) sound() bool {
	return f.failing == [3]bool{} && f.gone == [3]bool{}
}

// rotted reports whether bits rotted on node n's disk, whose log would then fail to replay: such a
// node is neither crashed, rebuilt nor given a disk fault, whose stop a restart would follow.
func (f *faultRun) rotted(n int) bool {
	for k := range f.flips {
		if k.node == n {
			return true
		}
	}
	return false
}

type flipAt struct {
	node int
	lba  core.LBA
}

type flip struct {
	off  int
	mask byte
}

type faultClient struct {
	f    *faultRun
	rng  *core.Rand
	left int
	ids  []core.RequestID
	pipe int
}

func (fc *faultClient) Ready() bool {
	if fc.left == 0 || !fc.f.c.s.Node(uint32(fc.f.p())).Up() {
		return false
	}
	out := 0
	for _, id := range fc.ids {
		if _, ok := fc.f.c.cl.replies[id]; !ok && !fc.f.lost[id] {
			out++
		}
	}
	return out < fc.pipe
}

func (fc *faultClient) Next(ctx *core.Context, acts []core.Action) []core.Action {
	fc.left--
	f, c := fc.f, fc.f.c
	op := faultOp{c: f.chunks[fc.rng.Below(2)], off: uint32(fc.rng.Below(faultSpan))}
	limit := uint32(300)
	if fc.rng.Chance(1, 2) {
		limit = faultSpan
	}
	op.n = min(1+uint32(fc.rng.Below(uint64(limit))), core.ChunkSize-op.off)
	if !fc.rng.Chance(1, 8) {
		op.data = make([]byte, op.n)
		for i := range op.data {
			op.data[i] = byte(fc.rng.Below(255) + 1)
		}
	}
	c.nextID++
	id := c.nextID
	fc.ids = append(fc.ids, id)
	f.ops[id] = op
	f.to[id] = f.p()
	u := Update{Kind: KindPunch, ID: id, PG: c.pgFields(pg0), Session: 1, Incarnation: 1, Chunk: op.c, Off: op.off, Len: op.n}
	a := core.Action{Kind: core.ActNetSend, Peer: core.NodeID{Node: uint32(f.p())}}
	if op.data != nil {
		u.Kind = KindWrite
		buf, err := ctx.Pool.AllocZeroed(dataBytes(op.off, op.n))
		if err != nil {
			panic(err)
		}
		copy(ctx.Pool.Bytes(buf)[op.off%core.BlockSize:], op.data)
		a.Msg.Buf = buf
		c.cl.sent[id] = buf
	}
	a.Msg.Len = uint16(copy(a.Msg.Body[:], AppendUpdate(nil, &u)))
	return append(acts, a)
}

func (f *faultRun) fail(inv string, err error) {
	if f.errs[inv] == nil {
		f.errs[inv] = err
	}
}

// p is the node of PG 0's primary in the cluster's table, or of its first member while every member is marked down.
func (f *faultRun) p() int {
	if n := f.c.primary(pg0); n >= 0 {
		return n
	}
	_, e := f.c.table.entry(pg0)
	return f.c.node(e.Members[0].Disk)
}

// pass follows what each replica's loop sends: a primary's client replies and member updates, and each member's acknowledgments, which it checks against the member's index as it sends them.
func (f *faultRun) pass(d int, acts []core.Action) {
	if r := f.c.reps[d]; d == f.p() {
		if i := r.lead(pg0); i >= 0 && r.leads[i].active && f.kept[r.leads[i].interval] == nil {
			f.kept[r.leads[i].interval] = append([]keptChunk{}, r.leads[i].kept...)
			f.dirty = true
		}
		for k, ch := range f.chunks {
			if ix := r.Store().Index(); ix != nil {
				if x, _ := ix.Chunk(testPool, ch); x.ListedIn != f.listed[k] {
					f.listed[k], f.dirty = x.ListedIn, true
				}
			}
		}
	}
	for i := range acts {
		a := &acts[i]
		if a.Kind != core.ActNetSend || a.Msg.Len < headSize {
			continue
		}
		switch k := Kind(a.Msg.Body[1]); {
		case k == KindReplicaWrite || k == KindReplicaPunch || k == KindRecover:
			var u Update
			data := 0
			if a.Msg.Buf != core.NoBuf {
				data = len(f.c.s.Node(uint32(d)).Pool().Bytes(a.Msg.Buf))
			}
			if DecodeUpdate(a.Msg.Inline(), data, &u) == nil {
				f.sent[[2]uint64{uint64(d), uint64(u.ID)}] = u
				r := f.c.reps[d]
				if j := r.versionOf(uint64(u.ID & (1<<sendShift - 1))); j >= 0 && !r.vers[j].recover {
					f.version[r.vers[j].msgID] = u.Version
				}
			}
		case k == KindReply:
			var rp Response
			if DecodeResponse(a.Msg.Inline(), 0, &rp) != nil {
				continue
			}
			if _, ok := f.ops[rp.ID]; !ok {
				continue
			}
			if f.replied[rp.ID]++; f.replied[rp.ID] > 1 {
				f.fail(invOneReply, fmt.Errorf("request %d answered twice", rp.ID))
			}
			if rp.Status == StatusOK {
				f.sameImage(d, f.ops[rp.ID].c, rp.Version)
				j, _ := slices.BinarySearchFunc(f.ok, rp.Version, func(a answered, v uint64) int { return cmpU64(a.version, v) })
				f.ok = slices.Insert(f.ok, j, answered{version: rp.Version, id: rp.ID})
				f.dirty = true
			} else {
				f.refused = append(f.refused, rp.ID)
			}
		case k == KindAck:
			var rp Response
			if DecodeResponse(a.Msg.Inline(), 0, &rp) != nil || rp.Status != StatusOK {
				continue
			}
			u, ok := f.sent[[2]uint64{uint64(a.Peer.Node), uint64(rp.ID)}]
			if !ok {
				continue
			}
			f.sameImage(d, u.Chunk, u.Version)
			ix := f.c.reps[d].Store().Index()
			if u.Kind == KindRecover {
				if x, _ := ix.Chunk(testPool, u.Chunk); x.Whole < u.Version {
					f.fail(invAckedWhole, fmt.Errorf("disk %d acknowledged recovery %#x of %v holding %+v", d+1, u.Version, u.Chunk, x))
				}
				continue
			}
			first, last := spanOf(u.Off, u.Len)
			for b := first; b <= last; b++ {
				if e, ok := ix.Lookup(testPool, u.Chunk, b); !ok || e.Version < u.Version {
					f.fail(invAckedWhole, fmt.Errorf("disk %d acknowledged %#x of %v and maps block %d at %#x", d+1, u.Version, u.Chunk, b, e.Version))
				}
			}
		}
	}
}

func cmpU64(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// keptRefused reports whether peering kept a version a primary refused or never answered: the first peering after its interval kept it or a later version of its chunk.
func (f *faultRun) keptRefused(id core.RequestID) (uint64, bool) {
	v, ok := f.version[id]
	if !ok {
		return 0, false
	}
	k := uint32(v>>32) + 1
	for ; f.kept[k] == nil; k++ {
		if _, e := f.c.table.entry(pg0); k > e.Interval {
			return 0, false
		}
	}
	for _, x := range f.kept[k] {
		if x.chunk == f.ops[id].c {
			return v, v <= x.kept
		}
	}
	return 0, false
}

// want is each chunk's bytes as the writes and punches a primary answered OK, and those it refused or a lost primary never answered that peering kept, give them, applied in version order.
func (f *faultRun) want() [2][]byte {
	all := slices.Clone(f.ok)
	for id := range f.c.nextID + 1 {
		if _, ok := f.ops[id]; !ok || f.replied[id] > 0 && !slices.Contains(f.refused, id) {
			continue
		}
		if v, ok := f.keptRefused(id); ok {
			all = append(all, answered{version: v, id: id})
		}
	}
	slices.SortFunc(all, func(a, b answered) int { return cmpU64(a.version, b.version) })
	var want [2][]byte
	for i, ch := range f.chunks {
		want[i] = make([]byte, 2*faultSpan)
		for _, a := range all {
			op := f.ops[a.id]
			if op.c != ch {
				continue
			}
			if op.data != nil {
				copy(want[i][op.off:], op.data)
			} else {
				clear(want[i][op.off:][:op.n])
			}
		}
	}
	return want
}

// maps checks that node n's index maps chunk k's blocks as want gives them.
func (f *faultRun) maps(n, k int, want []byte) error {
	ch := f.chunks[k]
	for b := range uint16(len(want) / core.BlockSize) {
		if got := f.block(n, ch, b); !bytes.Equal(got, want[int(b)*core.BlockSize:][:core.BlockSize]) {
			i := 0
			for got[i] == want[int(b)*core.BlockSize+i] {
				i++
			}
			return fmt.Errorf("disk %d maps byte %d of block %d of %v as %#x, the writes the primaries answered give %#x", f.c.ids[n], i, b, ch, got[i], want[int(b)*core.BlockSize+i])
		}
	}
	return nil
}

// mapped checks that the primary, once it activated, maps each chunk it lists nothing of as want gives it: a primary that crashed, rebuilt its index or took over maps the versions peering dropped until recovery supersedes them, listing the chunk meanwhile.
func (f *faultRun) mapped() error {
	n := f.p()
	if !f.dirty || n < 0 || !f.c.s.Node(uint32(n)).Up() || !f.c.reps[n].Store().Serving() || !f.activated() {
		return nil
	}
	f.dirty = false
	want := f.want()
	for k, ch := range f.chunks {
		if x, _ := f.c.reps[n].Store().Index().Chunk(testPool, ch); x.ListedIn != 0 {
			continue
		}
		if err := f.maps(n, k, want[k]); err != nil {
			return err
		}
	}
	return nil
}

// sameImage checks that node n, if its chunk ch is whole at version v and listed nowhere, maps the
// same bytes of it as every other disk that answered for v so.
func (f *faultRun) sameImage(n int, ch core.ChunkID, v uint64) {
	k := slices.Index(f.chunks[:], ch)
	if x, _ := f.c.reps[n].Store().Index().Chunk(testPool, ch); k < 0 || x.Version != v || x.Whole != v || x.ListedIn != 0 {
		return
	}
	var img []byte
	for b := range uint16(2 * faultSpan / core.BlockSize) {
		img = append(img, f.block(n, ch, b)...)
	}
	key := [2]uint64{uint64(k), v}
	if want, ok := f.images[key]; !ok {
		f.images[key] = img
	} else if !bytes.Equal(img, want) {
		f.fail(invOneVersion, fmt.Errorf("disk %d holds %v at %#x unlike another disk", f.c.ids[n], ch, v))
	}
}

// block is block b of ch as node n maps it, the bits rotted on its disk put back.
func (f *faultRun) block(n int, ch core.ChunkID, b uint16) []byte {
	e, ok := f.c.reps[n].Store().Index().Lookup(testPool, ch, b)
	if !ok || e.LBA == 0 {
		return make([]byte, core.BlockSize)
	}
	lba := e.BlockLBA(b)
	got := f.c.s.Node(uint32(n)).Disk(0).Read(lba, 1)
	for _, fl := range f.flips[flipAt{n, lba}] {
		got[fl.off] ^= fl.mask
	}
	return got
}

// publish hands every running replica t, marking no PG active.
func (f *faultRun) publish(t Table) {
	f.c.table = t
	for d := range f.c.reps {
		if f.c.s.Node(uint32(d)).Up() {
			f.c.call(d, func(ctx *core.Context, r *Replica, out *ReplicaOutput) { r.SetTable(ctx, t, out) })
		}
	}
}

// activated reports whether the primary has peered in PG 0's interval in the cluster's table.
func (f *faultRun) activated() bool {
	_, e := f.c.table.entry(pg0)
	r := f.c.reps[f.p()]
	i := r.lead(pg0)
	return i >= 0 && r.leads[i].interval == e.Interval && r.leads[i].active
}

// newInterval starts the PG's next interval, as the control plane does when the primary asks for one, with node first as its primary if not negative.
func (f *faultRun) newInterval(first int) {
	_, e := f.c.table.entry(pg0)
	k := e.Interval + 1
	f.publish(f.c.next(func(t *Table) {
		setInterval(t, pg0, k)
		if first >= 0 {
			_, e := t.entry(pg0)
			d := f.c.ids[first]
			e.Members = append([]Member{{Disk: d}}, slices.DeleteFunc(e.Members, func(m Member) bool { return m.Disk == d })...)
		}
	}))
	f.peering = true
}

// answerOthers starts the next interval of each PG but PG 0 a restarted member asked for, which takes no writes.
func (f *faultRun) answerOthers(asks []PGFields) {
	var pgs []core.PGID
	for _, a := range asks {
		if _, e := f.c.table.entry(a.PG); a.PG != pg0 && e.Interval == a.Interval && !slices.Contains(pgs, a.PG) {
			pgs = append(pgs, a.PG)
		}
	}
	if len(pgs) == 0 {
		return
	}
	f.publish(f.c.next(func(t *Table) {
		for _, pg := range pgs {
			_, e := t.entry(pg)
			setInterval(t, pg, e.Interval+1)
		}
	}))
}

// cut partitions nodes a and b until a seeded heal, unless they are cut already.
func (f *faultRun) cut(a, b int, d core.Tick) {
	k := [2]uint32{uint32(min(a, b)), uint32(max(a, b))}
	if f.heals[k] == 0 {
		f.c.s.Partition(k[0], k[1], false)
		f.heals[k] = f.c.s.Time() + 1 + core.Tick(f.rng.Below(uint64(d)))
	}
}

// crash crashes node n now, its requests in flight lost if it leads PG 0.
func (f *faultRun) crash(n int) {
	if n == f.p() {
		for id, to := range f.to {
			if _, ok := f.c.cl.replies[id]; !ok && to == n {
				f.lost[id] = true
			}
		}
	}
	f.c.s.CrashRandom(uint32(n))
}

// inject draws one fault.
func (f *faultRun) inject() {
	c, p := f.c, f.p()
	others := []int{(p + 1) % 3, (p + 2) % 3}
	m := others[f.rng.Below(2)]
	up := c.s.Node(uint32(m)).Up()
	// While the PG peers, member crashes and disk failures land at a tenth of the rate: each restart waits 4 s, so at the full rate they would keep it peering.
	k := f.rng.Below(10)
	if (k == 1 || k == 2) && f.peering && !f.rng.Chance(1, 10) {
		return
	}
	switch k {
	case 0:
		f.cut(p, m, 30_000)
	case 1:
		if up && f.crashes[m] == 0 && !f.rotted(m) {
			f.crashes[m] = c.s.Time() + 1 + core.Tick(f.rng.Below(20_000))
		}
	case 2:
		if up && f.sound() && !f.rotted(m) {
			f.failing[m] = true
			c.s.Node(uint32(m)).Disk(0).Inject(sim.Fault{Effect: sim.FailIO, When: sim.Once(sim.OnKind(core.ActDiskWrite))})
		}
	case 3:
		if up {
			refusing := cloneTable(c.table)
			_, e := refusing.entry(pg0)
			for i := range e.Members {
				if e.Members[i].Disk == c.ids[m] {
					e.Members[i].Mark = MarkMaint
				}
			}
			c.call(m, func(ctx *core.Context, r *Replica, out *ReplicaOutput) { r.table = refusing })
		}
	case 4:
		ch := f.chunks[f.rng.Below(2)]
		b := uint16(f.rng.Below(2 * faultSpan / core.BlockSize))
		if !c.s.Node(uint32(p)).Up() || c.reps[p].Store().Index() == nil {
			return
		}
		if e, ok := c.reps[p].Store().Index().Lookup(testPool, ch, b); ok && e.LBA != 0 {
			fl := flip{off: int(f.rng.Below(core.BlockSize)), mask: 1 << f.rng.Below(8)}
			c.s.Node(uint32(p)).Disk(0).FlipBit(e.BlockLBA(b), fl.off, fl.mask)
			k := flipAt{p, e.BlockLBA(b)}
			f.flips[k] = append(f.flips[k], fl)
			if f.rng.Chance(1, 2) {
				f.cut(p, others[0], 30_000)
			}
		}
	case 5:
		if !f.peering {
			f.newInterval(-1)
		}
	case 6:
		if !f.primaryFailed && f.rng.Chance(1, 40) && c.s.Node(uint32(p)).Up() && f.sound() && !f.rotted(p) {
			f.primaryFailed, f.failing[p] = true, true
			c.s.Node(uint32(p)).Disk(0).Inject(sim.Fault{Effect: sim.FailIO, When: sim.Once(sim.OnKind(core.ActDiskWrite))})
		}
	case 7:
		if !f.peering && f.rng.Chance(1, 4) && c.s.Node(uint32(p)).Up() && !f.rotted(p) {
			f.crash(p)
		}
	case 8:
		if !f.rebuilt && !f.peering && f.rng.Chance(1, 4) && c.s.Node(uint32(p)).Up() && c.reps[p].Store().Serving() && !f.rotted(p) {
			f.rebuilt = true
			c.call(p, func(ctx *core.Context, r *Replica, out *ReplicaOutput) {
				r.st.indexBroken(ctx, lsm.ErrCheck, &r.sout)
				r.st.pump(ctx, &r.sout)
				r.drain(ctx, out)
			})
		}
	case 9:
		if mk, _ := f.a.det.Mark(c.ids[m]); !f.peering && f.rng.Chance(1, 4) && up && mk == MarkUp {
			f.newInterval(m)
		}
	}
}

// inPart reports whether node m has logged some of a version's records and not the rest, the moment a crash leaves it holding the version in part.
func (f *faultRun) inPart(m int) bool {
	if !f.c.s.Node(uint32(m)).Up() || !f.c.reps[m].Store().Serving() {
		return false
	}
	st := f.c.reps[m].Store()
	for ci := range st.chunks {
		for _, oi := range st.chunks[ci].pending {
			if o := &st.ops[oi]; o.left > 0 && slices.ContainsFunc(o.items, func(it item) bool { return it.state == itemDurable }) {
				return true
			}
		}
	}
	return false
}

// tend crashes the nodes whose crash is due, early if one holds a version in part, and heals the cuts whose time has come, or all of them with all; crashes a node whose disk failed so the simulator restarts it; and moves peering on.
func (f *faultRun) tend(all bool) {
	c, now := f.c, f.c.s.Time()
	for m := range 3 {
		if at := f.crashes[m]; at != 0 && (all || now >= at || f.inPart(m)) {
			f.crashes[m] = 0
			if c.s.Node(uint32(m)).Up() && !f.rotted(m) {
				f.crash(m)
				if f.rng.Chance(1, 2) {
					f.cut(f.p(), m, 40_000)
				}
			}
		}
		switch st := c.reps[m].Store(); {
		case !c.s.Node(uint32(m)).Up() || f.gone[m]:
		case st.Serving():
			if f.fails[m] > 0 && st.Err() == nil && c.reps[m].Store().Log() != nil {
				f.failing[m] = false
			}
			f.fails[m] = 0
		case st.Err() != nil && f.fails[m] < 2 && f.downMarked(m):
			f.fails[m]++
			f.crash(m)
		case st.Err() != nil && f.fails[m] == 2:
			f.gone[m] = true
		}
	}
	for k, at := range f.heals {
		if all || now >= at {
			c.s.Heal(k[0], k[1])
			delete(f.heals, k)
		}
	}
	if f.a.det.Changes() != f.changes {
		f.remark()
	}
	if len(c.asks) > f.asked {
		asks := c.asks[f.asked:]
		f.asked = len(c.asks)
		f.answerOthers(asks)
		_, e := c.table.entry(pg0)
		if slices.ContainsFunc(asks, func(a PGFields) bool { return a.PG == pg0 && (!f.peering || a.Interval == e.Interval) }) {
			f.newInterval(-1)
		}
	}
	if f.peering && f.activated() {
		f.peering = false
	}
}

// remark starts the next interval of each PG whose members' marks the detector changed, with the detector's marks.
func (f *faultRun) remark() {
	f.changes = f.a.det.Changes()
	moved := false
	f.publish(f.c.next(func(t *Table) {
		for i := range t.Pools[0].PGs {
			e := &t.Pools[0].PGs[i]
			pg := core.PGID{Pool: testPool, Index: uint32(i)}
			if !slices.ContainsFunc(e.Members, func(m Member) bool { mk, _ := f.a.det.Mark(m.Disk); return mk != m.Mark }) {
				continue
			}
			setInterval(t, pg, e.Interval+1)
			for j := range e.Members {
				e.Members[j].Mark, _ = f.a.det.Mark(e.Members[j].Disk)
			}
			moved = moved || pg == pg0
		}
	}))
	f.peering = f.peering || moved
}

// downMarked reports whether the detector has node m's disk down, which a failed store waits for before its node restarts.
func (f *faultRun) downMarked(m int) bool {
	mk, _ := f.a.det.Mark(f.c.ids[m])
	return mk == MarkDown
}

// marked reports whether the table follows the detector's marks and the detector has every disk up but a gone one.
func (f *faultRun) marked() bool {
	for n := range 3 {
		if mk, _ := f.a.det.Mark(f.c.ids[n]); mk == MarkDown && !f.gone[n] {
			return false
		}
	}
	return f.a.det.Changes() == f.changes
}

// recovered reports whether PG 0's primary reported its interval recovered.
func (f *faultRun) recovered() bool {
	_, e := f.c.table.entry(pg0)
	return slices.ContainsFunc(f.c.recovered, func(r PGFields) bool { return r.PG == pg0 && r.Interval == e.Interval })
}

// unreadable reports whether every source lead i could recover ch from failed a read of it on a disk whose bits the run rotted: a second fault, which leaves the chunk listed as unfound does.
func (f *faultRun) unreadable(i int, ch core.ChunkID) bool {
	r := f.c.reps[f.p()]
	_, e := r.table.entry(pg0)
	for _, t := range r.leads[i].todo {
		if t.chunk != ch || t.targets == 0 {
			continue
		}
		for s, m := range e.Members {
			if m.Mark == MarkUp && t.targets&(1<<s) == 0 && (t.failed&(1<<s) == 0 || !f.rotted(f.c.node(m.Disk))) {
				return false
			}
		}
		return true
	}
	return false
}

// settled checks the run's end: every member but a lost one runs, lists nothing and maps what the primaries answered, but in a chunk the last peering found no member to recover from, which faults that overlap before recovery ends can leave (unfound), or whose every source holds rotted bits of it.
func (f *faultRun) settled() error {
	for n := range 3 {
		if !f.gone[n] && (!f.c.s.Node(uint32(n)).Up() || !f.c.reps[n].Store().Serving()) {
			return fmt.Errorf("%s: disk %d does not serve", invRecovered, f.c.ids[n])
		}
	}
	want := f.want()
	r := f.c.reps[f.p()]
	i := r.lead(pg0)
	for n := range 3 {
		if f.gone[n] {
			continue
		}
		for k, ch := range f.chunks {
			if i >= 0 && (r.leads[i].isUnfound(ch) || f.unreadable(i, ch)) {
				continue
			}
			if x, _ := f.c.reps[n].Store().Index().Chunk(testPool, ch); x.ListedIn != 0 {
				return fmt.Errorf("%s: disk %d lists %+v of %v", invRecovered, f.c.ids[n], x, ch)
			}
			if err := f.maps(n, k, want[k]); err != nil {
				return fmt.Errorf("%s: %w", invRecovered, err)
			}
		}
	}
	return nil
}

// faultPlant is a bug planted in the fault run and the invariant it must break.
type faultPlant struct {
	inv string
	set func(c *rcluster)
}

// runFaults runs the fault workload, failing t on a broken invariant; with a plant, it checks only
// the plant's invariant and returns its first violation instead.
func runFaults(t *testing.T, seed uint64, clients, writes int, plant *faultPlant) error {
	s := sim.New(seed, sim.DefaultSettings())
	c := newRClusterWith(t, rtable(4, 2), s, func(cfg *Config) { cfg.Log.MaxRecordBytes = 3 * core.BlockSize })
	f := &faultRun{t: t, c: c, rng: core.NewRand(seed ^ 0xfa17), chunks: [2]core.ChunkID{chunkIn(pg0, 4, 0), chunkIn(pg0, 4, 1)},
		ops: map[core.RequestID]faultOp{}, replied: map[core.RequestID]int{}, sent: map[[2]uint64]Update{}, flips: map[flipAt][]flip{},
		errs: map[string]error{}, version: map[core.RequestID]uint64{}, kept: map[uint32][]keptChunk{}, to: map[core.RequestID]int{},
		lost: map[core.RequestID]bool{}, heals: map[[2]uint32]core.Tick{}, images: map[[2]uint64][]byte{}}
	f.a = &authority{pool: Pool{Replicas: 3, MinLive: 2, FailureDomain: DomainNode, MaxPerDomain: 1, Locality: LocalitySpread, Overcommit: true, WriteLimit: 90, PGCount: 4}}
	for n := range 3 {
		if _, err := f.a.rec.Report(uint32(n), 0, c.logs[n]); err != nil {
			t.Fatal(err)
		}
	}
	c.detect(f.a)
	watchEpochs(c, &f.a.rec)
	c.onPass = f.pass
	c.cl.lost = func(peer core.NodeID) {
		for id, to := range f.to {
			if _, ok := c.cl.replies[id]; !ok && to == int(peer.Node) {
				f.lost[id] = true
			}
		}
	}
	for _, inv := range []string{invAckedWhole, invOneReply, invOneVersion} {
		c.inv.Register(inv, func() error { return f.errs[inv] })
	}
	c.inv.Register(invMappedAnswers, f.mapped)
	if plant != nil {
		plant.set(c)
		for n := range c.reps {
			c.reps[n].plant = c.replicaPlants
		}
	}
	c.inv.Register(invActivates, func() error {
		if !f.ended {
			return nil
		}
		return c.inactive()
	})
	c.inv.Register(invRecovered, func() error {
		if !f.ended {
			return nil
		}
		return f.settled()
	})
	if plant != nil {
		c.planted = true
		c.inv.Only(plant.inv)
	}
	if f.rng.Chance(1, 2) {
		c.reps[0].Store().cfg.MergeCache = 0
	}
	var fcs []*faultClient
	for range clients {
		fc := &faultClient{f: f, rng: f.rng.Fork(), left: writes, pipe: 1 + int(f.rng.Below(8))}
		fcs = append(fcs, fc)
		c.s.Node(c.client.Node).AddActor(c.client, fc)
	}
	busy := func() bool { return slices.ContainsFunc(fcs, func(fc *faultClient) bool { return fc.left > 0 }) }
	down := func() bool {
		return slices.ContainsFunc([]int{0, 1, 2}, func(n int) bool { return !f.gone[n] && (!c.s.Node(uint32(n)).Up() || !c.reps[n].Store().Serving()) })
	}
	idle := 0
	for range 2_000_000 {
		if c.quiet() && !f.peering && !down() || !c.step() {
			calm := len(f.heals) == 0 && f.crashes == [3]core.Tick{} && !f.peering && len(c.asks) == f.asked && !down()
			if calm && f.marked() && (!busy() && f.recovered() || idle >= 60) {
				break
			}
			f.tend(true)
			if idle++; calm {
				c.runFor(RetryAfter)
			}
			continue
		}
		idle = 0
		if c.found != nil {
			return c.found
		}
		if busy() && f.rng.Chance(1, 200) {
			f.inject()
		}
		f.tend(false)
	}
	for id := core.RequestID(1); id <= c.nextID; id++ {
		if _, ok := f.ops[id]; ok && f.replied[id] != 1 && !(f.replied[id] == 0 && f.lost[id]) && plant == nil {
			t.Fatalf("request %d answered %d times", id, f.replied[id])
		}
	}
	f.dirty, f.ended = true, true
	if err := c.inv.Check(); err != nil {
		if plant == nil {
			t.Fatal(err)
		}
		return err
	}
	return c.found
}

// Clients pipeline sub-block writes and punches to two chunks while members are cut off and restarted, their disks and at times the primary's fail, the stand-in authority's failure detector marking them down and up, and they refuse, the primary's bases rot, the PG's interval changes and the primary crashes, rebuilds its index and moves, peering keeping some pending versions: every acknowledged version is whole on every member that acknowledged it, every write is answered once unless a crash of its primary lost it, the primary maps exactly the writes it answered OK in every chunk it lists nothing of, and once recovery settles every member lists nothing and maps them.
func TestReplicatedWritesSurviveFaults(t *testing.T) {
	seeds.Run(t, 0, 20, func(seed uint64) {
		if err := runFaults(t, seed, 2, 24, nil); err != nil {
			t.Fatal(err)
		}
	})
}

// A recovery write that leaves out the blocks covered when it read its source, not when it was
// ordered (the epoch change model's neg-recovery-covers-at-read), puts a source's block over a
// write ordered in between, and two disks then hold different bytes under one version, the first
// invariant it breaks, checked alone, at 183 of the first 500 seeds.
func TestPlantedRecoveryBugBreaksTheFaultRun(t *testing.T) {
	if runFaults(t, 4, 2, 24, &faultPlant{invOneVersion, func(c *rcluster) { c.replicaPlants.coverAtRead = true }}) == nil {
		t.Fatalf("seed 4 kept %q", invOneVersion)
	}
}

// A restarted primary that never asks for a new interval (the epoch change model's
// neg-restart-no-ask) leaves its PGs unactivated once the run settles, at 69 of the first 500 seeds.
func TestPlantedRestartWithoutAskBreaksTheFaultRun(t *testing.T) {
	if runFaults(t, 7, 2, 24, &faultPlant{invActivates, func(c *rcluster) { c.replicaPlants.neverAsk = true }}) == nil {
		t.Fatalf("seed 7 kept %q", invActivates)
	}
}
