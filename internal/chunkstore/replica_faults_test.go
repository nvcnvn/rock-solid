package chunkstore

import (
	"bytes"
	"fmt"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

const (
	invAckedWhole    = "a member acknowledges a version only once it maps every block of it"
	invOneReply      = "every client write or punch gets one reply"
	invMappedAnswers = "the primary maps exactly the writes and punches it answered OK, in version order"
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

// faultRun runs clients pipelining sub-block writes and punches to two chunks of PG 0, led by disk 1, while a seeded schedule cuts members off, crashes them, often while they hold a version in part, fails their disks and at times the primary's, has them refuse, rots the primary's copy of blocks, sometimes cutting off the member it asks first for a base, and changes the PG's interval, with peering stood in by Settle, which at times keeps pending versions, and Activate. Records of at most 3 blocks cut a version into several.
type faultRun struct {
	t      *testing.T
	c      *rcluster
	rng    *core.Rand
	chunks [2]core.ChunkID
	ops    map[core.RequestID]faultOp
	// ok is the writes and punches the primary answered OK, in version order, and replied each it answered.
	ok      []answered
	replied map[core.RequestID]int
	dirty   bool
	// sent is each version the primary sent, by its sequence, as an update; flips the bits rotted on the primary's disk.
	sent  map[core.RequestID]Update
	flips map[core.LBA][]flip
	errs  map[string]error
	// heals is when each cut member is healed, and crashes when each member is crashed at the latest, 0 for none; peering is whether an interval change waits for Settle.
	heals   [3]core.Tick
	crashes [3]core.Tick
	peering bool
	asked   int
	// primaryFailed is set once the primary's disk has been made to fail.
	primaryFailed bool
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
	if fc.left == 0 {
		return false
	}
	out := 0
	for _, id := range fc.ids {
		if _, ok := fc.f.c.cl.replies[id]; !ok {
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
	u := Update{Kind: KindPunch, ID: id, PG: c.pgFields(pg0), Session: 1, Incarnation: 1, Chunk: op.c, Off: op.off, Len: op.n}
	a := core.Action{Kind: core.ActNetSend, Peer: core.NodeID{Node: 0}}
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

// pass follows what each replica's loop sends: the primary's client replies and member updates, and each member's acknowledgments, which it checks against the member's index as it sends them.
func (f *faultRun) pass(d int, acts []core.Action) {
	for i := range acts {
		a := &acts[i]
		if a.Kind != core.ActNetSend || a.Msg.Len < headSize {
			continue
		}
		switch k := Kind(a.Msg.Body[1]); {
		case d == 0 && (k == KindReplicaWrite || k == KindReplicaPunch):
			var u Update
			data := 0
			if a.Msg.Buf != core.NoBuf {
				data = len(f.c.s.Node(0).Pool().Bytes(a.Msg.Buf))
			}
			if DecodeUpdate(a.Msg.Inline(), data, &u) == nil {
				f.sent[u.ID] = u
			}
		case d == 0 && k == KindReply:
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
				j, _ := slices.BinarySearchFunc(f.ok, rp.Version, func(a answered, v uint64) int { return cmpU64(a.version, v) })
				f.ok = slices.Insert(f.ok, j, answered{version: rp.Version, id: rp.ID})
				f.dirty = true
			}
		case d != 0 && k == KindAck:
			var rp Response
			if DecodeResponse(a.Msg.Inline(), 0, &rp) != nil || rp.Status != StatusOK {
				continue
			}
			u, ok := f.sent[rp.ID]
			if !ok {
				continue
			}
			first, last := spanOf(u.Off, u.Len)
			for b := first; b <= last; b++ {
				if e, ok := f.c.reps[d].Store().Index().Lookup(testPool, u.Chunk, b); !ok || e.Version < u.Version {
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

// mapped checks that the primary's index maps each chunk's blocks as the writes and punches it answered OK give them, applied in version order.
func (f *faultRun) mapped() error {
	if !f.dirty {
		return nil
	}
	f.dirty = false
	for _, ch := range f.chunks {
		want := make([]byte, 2*faultSpan)
		for _, a := range f.ok {
			op := f.ops[a.id]
			if op.c != ch {
				continue
			}
			if op.data != nil {
				copy(want[op.off:], op.data)
			} else {
				clear(want[op.off:][:op.n])
			}
		}
		for b := range uint16(len(want) / core.BlockSize) {
			if got := f.block(ch, b); !bytes.Equal(got, want[int(b)*core.BlockSize:][:core.BlockSize]) {
				i := 0
				for got[i] == want[int(b)*core.BlockSize+i] {
					i++
				}
				return fmt.Errorf("the primary maps byte %d of block %d of %v as %#x, the writes it answered give %#x", i, b, ch, got[i], want[int(b)*core.BlockSize+i])
			}
		}
	}
	return nil
}

// block is block b of ch as the primary maps it, the bits rotted on its disk put back.
func (f *faultRun) block(ch core.ChunkID, b uint16) []byte {
	e, ok := f.c.reps[0].Store().Index().Lookup(testPool, ch, b)
	if !ok || e.LBA == 0 {
		return make([]byte, core.BlockSize)
	}
	lba := e.BlockLBA(b)
	got := f.c.s.Node(0).Disk(0).Read(lba, 1)
	for _, fl := range f.flips[lba] {
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

// settle stands in for the end of peering: once every local write of each chunk has resolved, the primary keeps, at times, a prefix of the chunk's pending versions whose clients wait, drops the rest and activates the PG's interval.
func (f *faultRun) settle() {
	st := f.c.reps[0].Store()
	for _, ch := range f.chunks {
		if ci, ok := st.chunkOf[ch]; ok && (len(st.chunks[ci].waiting) > 0 || slices.ContainsFunc(st.chunks[ci].pending, func(oi int32) bool { return st.ops[oi].left > 0 })) {
			return
		}
	}
	_, e := f.c.table.entry(pg0)
	f.c.call(0, func(ctx *core.Context, r *Replica, out *ReplicaOutput) {
		for _, ch := range f.chunks {
			var keep uint64
			if x, ok := r.Store().Index().Chunk(testPool, ch); ok {
				keep = x.Version
			}
			if ci, ok := st.chunkOf[ch]; ok && f.rng.Chance(1, 2) {
				for _, oi := range st.chunks[ci].pending {
					o := &st.ops[oi]
					j := r.versionOf(o.req.ID)
					if o.err != nil || o.dropped || j < 0 || r.vers[j].done || !f.rng.Chance(2, 3) {
						break
					}
					keep = o.version
				}
			}
			if err := r.Settle(ctx, ch, keep, out); err != nil {
				f.t.Fatalf("Settle of %v with its writes resolved: %v", ch, err)
			}
		}
		if err := r.Activate(ctx, pg0, e.Interval, out); err != nil {
			f.t.Fatalf("Activate with every chunk settled: %v", err)
		}
		f.peering = false
	})
}

// newInterval starts the PG's next interval, as the control plane does when the primary asks for one.
func (f *faultRun) newInterval() {
	_, e := f.c.table.entry(pg0)
	k := e.Interval + 1
	f.publish(f.c.next(func(t *Table) { setInterval(t, pg0, k) }))
	f.peering = true
}

// inject draws one fault.
func (f *faultRun) inject() {
	c, now := f.c, f.c.s.Time()
	m := uint32(1 + f.rng.Below(2))
	up := c.s.Node(m).Up()
	switch f.rng.Below(7) {
	case 0:
		if f.heals[m] == 0 {
			c.s.Partition(0, m, false)
			f.heals[m] = now + 1 + core.Tick(f.rng.Below(30_000))
		}
	case 1:
		if up && f.crashes[m] == 0 {
			f.crashes[m] = now + 1 + core.Tick(f.rng.Below(20_000))
		}
	case 2:
		if up {
			c.s.Node(m).Disk(0).Inject(sim.Fault{Effect: sim.FailIO, When: sim.Once(sim.OnKind(core.ActDiskWrite))})
		}
	case 3:
		if up {
			refusing := cloneTable(c.table)
			_, e := refusing.entry(pg0)
			e.Members[m].Mark = MarkMaint
			c.call(int(m), func(ctx *core.Context, r *Replica, out *ReplicaOutput) { r.table = refusing })
		}
	case 4:
		ch := f.chunks[f.rng.Below(2)]
		b := uint16(f.rng.Below(2 * faultSpan / core.BlockSize))
		if e, ok := c.reps[0].Store().Index().Lookup(testPool, ch, b); ok && e.LBA != 0 {
			fl := flip{off: int(f.rng.Below(core.BlockSize)), mask: 1 << f.rng.Below(8)}
			c.s.Node(0).Disk(0).FlipBit(e.BlockLBA(b), fl.off, fl.mask)
			f.flips[e.BlockLBA(b)] = append(f.flips[e.BlockLBA(b)], fl)
			if f.heals[1] == 0 && f.rng.Chance(1, 2) {
				c.s.Partition(0, 1, false)
				f.heals[1] = now + 1 + core.Tick(f.rng.Below(30_000))
			}
		}
	case 5:
		if !f.peering {
			f.newInterval()
		}
	case 6:
		if !f.primaryFailed && f.rng.Chance(1, 40) {
			f.primaryFailed = true
			c.s.Node(0).Disk(0).Inject(sim.Fault{Effect: sim.FailIO, When: sim.Once(sim.OnKind(core.ActDiskWrite))})
		}
	}
}

// inPart reports whether member m has logged some of a version's records and not the rest, the moment a crash leaves it holding the version in part.
func (f *faultRun) inPart(m uint32) bool {
	if !f.c.s.Node(m).Up() || !f.c.reps[m].Store().Serving() {
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

// tend crashes the members whose crash is due, early if one holds a version in part, and heals the cuts whose time has come, or all of them with all; crashes a member whose disk failed so the simulator restarts it; and moves peering on.
func (f *faultRun) tend(all bool) {
	c, now := f.c, f.c.s.Time()
	for m := uint32(1); m <= 2; m++ {
		if at := f.crashes[m]; at != 0 && (all || now >= at || f.inPart(m)) {
			f.crashes[m] = 0
			if c.s.Node(m).Up() {
				c.s.CrashRandom(m)
				if f.heals[m] == 0 && f.rng.Chance(1, 2) {
					c.s.Partition(0, m, false)
					f.heals[m] = now + 40_000 + core.Tick(f.rng.Below(40_000))
				}
			}
		}
		if at := f.heals[m]; at != 0 && (all || now >= at) {
			c.s.Heal(0, m)
			f.heals[m] = 0
		}
		if c.s.Node(m).Up() && c.reps[m].Store().Err() != nil {
			c.s.CrashRandom(m)
		}
	}
	if len(c.asks) > f.asked {
		f.asked = len(c.asks)
		if !f.peering {
			f.newInterval()
		}
	}
	if f.peering {
		f.settle()
	}
}

func runFaults(t *testing.T, seed uint64, clients, writes int) {
	s := sim.New(seed, sim.DefaultSettings())
	c := newRClusterWith(t, rtable(4, 2), s, func(cfg *Config) { cfg.Log.MaxRecordBytes = 3 * core.BlockSize })
	f := &faultRun{t: t, c: c, rng: core.NewRand(seed ^ 0xfa17), chunks: [2]core.ChunkID{chunkIn(pg0, 4, 0), chunkIn(pg0, 4, 1)},
		ops: map[core.RequestID]faultOp{}, replied: map[core.RequestID]int{}, sent: map[core.RequestID]Update{}, flips: map[core.LBA][]flip{},
		errs: map[string]error{}}
	c.onPass = f.pass
	for _, inv := range []string{invAckedWhole, invOneReply} {
		c.inv.Register(inv, func() error { return f.errs[inv] })
	}
	c.inv.Register(invMappedAnswers, f.mapped)
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
	for range 2_000_000 {
		if c.quiet() || !c.step() {
			if f.heals == [3]core.Tick{} && f.crashes == [3]core.Tick{} && !f.peering && len(c.asks) == f.asked {
				break
			}
			f.tend(true)
			continue
		}
		if busy() && f.rng.Chance(1, 200) {
			f.inject()
		}
		f.tend(false)
	}
	f.dirty = true
	if err := c.inv.Check(); err != nil {
		t.Fatal(err)
	}
	for id := core.RequestID(1); id <= c.nextID; id++ {
		if _, ok := f.ops[id]; ok && f.replied[id] != 1 {
			t.Fatalf("request %d answered %d times", id, f.replied[id])
		}
	}
}

// Clients pipeline sub-block writes and punches to two chunks while members are cut off and restarted, their disks and at times the primary's fail and they refuse, the primary's bases rot and the PG's interval changes, peering keeping some pending versions: every acknowledged version is whole on every member that acknowledged it, every write is answered once, and the primary maps exactly the writes it answered OK.
func TestReplicatedWritesSurviveFaults(t *testing.T) {
	seeds.Run(t, 0, 20, func(seed uint64) {
		runFaults(t, seed, 2, 24)
	})
}
