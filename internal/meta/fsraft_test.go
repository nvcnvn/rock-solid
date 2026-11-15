package meta

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

const (
	invInodeNew = "an inode ID is never reused"
	invMtime    = "no file's mtime goes backwards except by utimensat"
	invSlot     = "a file's keys and its directory's entry are in one slot, unless a link or a cross-directory rename put the entry there"
	invAncestor = "no directory is its own ancestor"
	invReplay   = "a replayed request returns its first final result and changes nothing"
)

// fsSlotLimit is where the run's shard's slots end, low enough that directories come to share their parent's.
const fsSlotLimit = 20

// fsDaemon is a mount's daemon: its allocator and the reservations it is to send.
type fsDaemon struct {
	a      Allocator
	inodes []uint32
	slots  bool
}

// fsRaftRun drives the filesystem workload of three daemons through a three-replica shard, the
// volume's root and every inode ID and slot proposed through it, with leaders cut off while a
// Txn is in flight, replicas restarted, replies lost, daemons crashed with their reservations and
// requests outstanding and the root's creation resent, and checks the filesystem and replay
// invariants on the leader's state after every Txn; only, when set, keeps one invariant.
func fsRaftRun(t testing.TB, seed uint64, sp statePlants, resetRoot bool, only string) error {
	rng := rand.New(rand.NewPCG(seed, 1))
	c := newGroup(t, 3, false)
	c.elect()
	plant := func() {
		for _, r := range c.n {
			r.s.State().plant = sp
		}
	}
	plant()
	lead := func() *State { return c.n[c.leader()].s.State() }
	scan := func() []KV {
		var r ScanResult
		if err := lead().Scan(nil, nil, 1<<20, &r); err != nil {
			t.Fatal(err)
		}
		return slices.Clone(r.KVs)
	}

	var token uint64
	// sendAll proposes txs to the leader in one round and returns each one's result, and whether its answer arrived.
	sendAll := func(txs []Txn) ([]Result, []bool) {
		l := c.leader()
		for _, tx := range txs {
			token++
			c.txn(l, tx, token)
		}
		switch rng.IntN(10) {
		case 0:
			c.cut[l] = true
			c.elect()
			c.cut[l] = false
			c.heartbeat()
		case 1:
			if rng.IntN(2) == 0 {
				c.settle()
			}
			c.restart(rng.IntN(len(c.n)))
			plant()
			c.elect()
			c.heartbeat()
		}
		c.settle()
		res, known := make([]Result, len(txs)), make([]bool, len(txs))
		for k := range txs {
			a, ok := c.n[l].answers[token-uint64(len(txs)-1-k)]
			if known[k] = ok && a.Err == nil && rng.IntN(10) != 0; known[k] {
				res[k] = a.Result
			}
		}
		return res, known
	}
	send := func(tx Txn) (Result, bool) {
		res, known := sendAll([]Txn{tx})
		return res[0], known[0]
	}

	h := &fsT{t: t, f: FS{Volume: 5, CaseIndex: seed%2 == 0, resetRoot: resetRoot}}
	if err := h.f.Root(&rootDir); err != nil {
		t.Fatal(err)
	}
	root := h.built()
	for {
		if _, ok := send(root); ok {
			break
		}
	}

	var ds [3]fsDaemon
	ids := map[uint64]bool{}
	var cur int
	var fresh uint32
	var idErr, replayErr error
	w := &fsWork{h: h, moved: map[uint64]bool{}}
	w.child = func(dir uint64, mkdir bool) (uint64, bool) {
		d := &ds[cur]
		id, ok := uint64(0), false
		if mkdir {
			var s uint32
			if s, ok = d.a.Slot(); ok {
				id, fresh = ino(s, 1), s
			}
			d.slots = d.slots || d.a.NeedSlots()
			if !ok && !d.a.NoSlots() {
				return 0, false
			}
		}
		if !ok {
			slot := slotOf(dir)
			id, ok = d.a.Inode(slot)
			if d.a.NeedInodes(slot) {
				d.inodes = append(d.inodes, slot)
			}
		}
		if ok && ids[id] {
			idErr = fmt.Errorf("inode %x handed out twice", id)
		}
		ids[id] = true
		return id, ok
	}
	reserve := func(d *fsDaemon) {
		for _, slot := range d.inodes {
			h.s = lead()
			if err := h.f.ReserveInodes(slot); err != nil {
				t.Fatal(err)
			}
			if r, ok := send(h.built()); ok && r.FailedCheck == -1 {
				d.a.InodesReserved(slot, r.Updated[0])
			} else {
				d.a.InodesFailed(slot)
			}
		}
		d.inodes = d.inodes[:0]
		if !d.slots {
			return
		}
		d.slots = false
		if !d.a.Free() {
			_, ok := lead().tree.Get(h.slotKey(0, SlotFree, 0))
			d.a.FreeRead(ok)
		}
		if !d.a.Free() {
			return
		}
		if err := h.f.ReserveSlots(0); err != nil {
			t.Fatal(err)
		}
		if r, ok := send(h.built()); ok && r.FailedCheck == -1 {
			d.a.SlotsReserved(r.Updated[0], fsSlotLimit)
		} else {
			d.a.SlotsFailed()
		}
	}

	mtimes := map[uint64]int64{}
	var inv sim.Invariants
	inv.Register(invInodeNew, func() error { return idErr })
	inv.Register(invMtime, func() error {
		for _, kv := range scan() {
			var k Key
			if DecodeKey(kv.Key, &k) != nil || k.Space != SpaceFS || k.Tag != InodeAttr || k.Inode&seqMask == 0 {
				continue
			}
			var a Attr
			if err := DecodeAttr(kv.Value, &a); err != nil {
				return err
			}
			if old, ok := mtimes[k.Inode]; ok && a.Mtime < old {
				return fmt.Errorf("inode %x's mtime went from %d to %d", k.Inode, old, a.Mtime)
			}
			mtimes[k.Inode] = a.Mtime
		}
		return nil
	})
	inv.Register(invSlot, func() error {
		h.s = lead()
		_, entries := w.walk()
		for _, e := range entries {
			if e.mode != ModeDir && slotOf(e.inode) != slotOf(e.dir) && !w.moved[e.inode] {
				return fmt.Errorf("file %x has its entry %q in directory %x", e.inode, e.name, e.dir)
			}
		}
		return nil
	})
	inv.Register(invAncestor, func() error {
		up := map[uint64]uint64{}
		var dirs []uint64
		for _, kv := range scan() {
			var k Key
			if DecodeKey(kv.Key, &k) != nil || k.Space != SpaceFS || k.Tag != InodeParent {
				continue
			}
			p, err := DecodeParent(kv.Value)
			if err != nil {
				return err
			}
			up[k.Inode] = p
			dirs = append(dirs, k.Inode)
		}
		for _, dir := range dirs {
			d := dir
			for range len(up) + 1 {
				if d = up[d]; d == dir {
					return fmt.Errorf("directory %x is its own ancestor", dir)
				}
			}
		}
		return nil
	})
	inv.Register(invReplay, func() error { return replayErr })
	if only != "" {
		inv.Only(only)
	}

	type fsReq struct {
		mount  uint32
		id     uint64
		build  func() (Txn, bool)
		resend int
		first  *Result
		fresh  uint32
		unsure bool
	}
	var out []*fsReq
	var next [3]uint64
	for step := 0; step < 600; step++ {
		if rng.IntN(60) == 0 {
			ds[rng.IntN(3)] = fsDaemon{}
		}
		if rng.IntN(40) == 0 {
			send(root)
		}
		for i := range ds {
			reserve(&ds[i])
		}
		if len(out) == 0 || rng.IntN(2) == 0 {
			mount := uint32(rng.IntN(3))
			next[mount]++
			oldest := next[mount]
			for _, r := range out {
				if r.mount == mount {
					oldest = min(oldest, r.id)
				}
			}
			cur, fresh = int(mount), 0
			h.s = lead()
			build := w.request(rng, Request{Session: 7, Mount: mount, ID: next[mount], Oldest: oldest})
			out = append(out, &fsReq{mount: mount, id: next[mount], build: build, resend: rng.IntN(4), fresh: fresh})
		}
		n := 1
		if len(out) > 1 && rng.IntN(3) == 0 {
			n = 2
		}
		var rs []*fsReq
		var txs []Txn
		h.s = lead()
		for _, i := range rng.Perm(len(out))[:n] {
			if tx, sent := out[i].build(); sent {
				rs, txs = append(rs, out[i]), append(txs, h.copyOf(&tx))
			} else {
				out[i].resend = -1
			}
		}
		before := scan()
		results, answered := sendAll(txs)
		after := scan()
		for k, r := range rs {
			tx, res, known := txs[k], results[k], answered[k]
			// After a send whose answer was lost, a result may be an earlier Txn's, read from its record.
			_, stored := lead().tree.Get(tx.Replay)
			final := known && (res.FailedCheck < 0 || r.unsure && stored || !r.unsure && tx.Ops[res.FailedCheck].Final)
			r.unsure = r.unsure || !known
			switch {
			case r.first != nil && len(txs) == 1 && !slices.EqualFunc(before, after, func(a, b KV) bool { return bytes.Equal(a.Key, b.Key) && bytes.Equal(a.Value, b.Value) }):
				replayErr = fmt.Errorf("step %d: resent %x changed the state", step, tx.Replay)
			case r.first != nil && known && !sameResult(&res, r.first, true):
				replayErr = fmt.Errorf("step %d: resent %x returned %+v, its first final result %+v", step, tx.Replay, res, *r.first)
			case r.first == nil && final:
				r.first = &Result{FailedCheck: res.FailedCheck, Updated: slices.Clone(res.Updated)}
				if r.fresh != 0 && res.FailedCheck == -1 {
					ds[r.mount].a.Adopt(r.fresh)
				}
			case r.first == nil && known && stored:
				replayErr = fmt.Errorf("step %d: %x failed check %d, not final, and left a record", step, tx.Replay, res.FailedCheck)
			}
			if known {
				r.resend--
			}
		}
		out = slices.DeleteFunc(out, func(r *fsReq) bool { return r.resend < 0 })
		if err := inv.Check(); err != nil {
			return fmt.Errorf("seed %d: %w", seed, err)
		}
		if step%50 == 49 {
			c.heartbeat()
			c.agree()
		}
	}
	return nil
}

// An inode ID is never reused, no file's mtime goes backwards, a file and its entry share a slot,
// no directory is its own ancestor and a resent request returns its first final result and
// changes nothing, under three daemons' filesystem workload through a Raft shard whose leaders
// are cut off with Txns in flight, whose replicas restart, whose replies are lost and whose
// daemons crash with reservations and requests outstanding.
func TestTheFilesystemWorkloadKeepsItsInvariantsAcrossElections(t *testing.T) {
	seeds.Run(t, 1, 20, func(seed uint64) {
		if err := fsRaftRun(t, seed, statePlants{}, false, ""); err != nil {
			t.Fatal(err)
		}
	})
}

// Each plant, a replay record ignored, a failed check that is not final stored as the request's
// result and the root's creation without its check, breaks its invariant within about 5 ÷ the
// share of 500 seeds that catch it: 500, 200 and 499.
func TestFilesystemPlantsAreCaughtAcrossElections(t *testing.T) {
	for _, p := range []struct {
		sp        statePlants
		resetRoot bool
		inv       string
		seeds     uint64
	}{
		{statePlants{replayIgnored: true}, false, invReplay, 5},
		{statePlants{nonFinalStored: true}, false, invReplay, 13},
		{statePlants{}, true, invInodeNew, 5},
	} {
		caught := false
		for seed := uint64(1); seed <= p.seeds && !caught; seed++ {
			caught = fsRaftRun(t, seed, p.sp, p.resetRoot, p.inv) != nil
		}
		if !caught {
			t.Errorf("plant %+v, root reset %v passed %d seeds", p.sp, p.resetRoot, p.seeds)
		}
	}
}
