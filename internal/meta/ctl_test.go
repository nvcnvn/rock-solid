package meta

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
)

// ctlRun drives a Control at the leader of a 3-replica group, as the control-plane leader's loop would.
type ctlRun struct {
	t     *testing.T
	c     *group
	l     int
	ctl   *Control
	token uint64
	// txn is the last Txn proposed, out the output of the last answer.
	txn Txn
	out ControlOutput
}

func newCtlRun(t *testing.T) *ctlRun {
	c := newGroup(t, 3, false)
	r := &ctlRun{t: t, c: c, l: c.elect()}
	r.ctl = r.load(r.l)
	return r
}

func (r *ctlRun) load(i int) *Control {
	ctl, err := NewControl(r.c.n[i].s.State())
	if err != nil {
		r.t.Fatal(err)
	}
	return ctl
}

// propose runs a Step and, when it proposes, hands the Txn to the leader; it reports whether it proposed.
func (r *ctlRun) propose(in *ControlInputs) bool {
	r.t.Helper()
	var out ControlOutput
	r.token++
	if err := r.ctl.Step(in, r.token, &out); err != nil {
		r.t.Fatal(err)
	}
	if !out.Propose {
		return false
	}
	if len(out.Plans) > 0 || out.Push {
		r.t.Fatalf("a proposal handed out %+v", out)
	}
	r.txn = out.Txn
	r.c.txn(r.l, r.txn, r.token)
	return true
}

// answer settles the group and hands the leader's answer to the Control.
func (r *ctlRun) answer() Answer {
	r.t.Helper()
	r.c.heartbeat()
	a := r.c.answer(r.l, r.token)
	r.out.Reset()
	if err := r.ctl.Answer(r.c.n[r.l].s.State(), &a, &r.out); err != nil {
		r.t.Fatal(err)
	}
	return a
}

// step proposes and commits what in changes, failing unless it commits.
func (r *ctlRun) step(in *ControlInputs) *ControlOutput {
	r.t.Helper()
	if !r.propose(in) {
		r.t.Fatal("nothing proposed")
	}
	if a := r.answer(); a.Err != nil || a.Result.FailedCheck >= 0 {
		r.t.Fatalf("the commit answered %+v", a)
	}
	r.c.agree()
	return &r.out
}

func disks(nodes ...uint32) []chunkstore.Disk {
	var ds []chunkstore.Disk
	for _, n := range nodes {
		ds = append(ds, chunkstore.Disk{ID: n + 1, Node: n, Blocks: 1 << 28})
	}
	return ds
}

func spec(replicas int, pgs uint32) chunkstore.PoolSpec {
	return chunkstore.PoolSpec{Replicas: &replicas, PGCount: &pgs}
}

// pushed applies the delta the commit handed out to prev, as a node holding prev does, and fails
// unless it gives the committed table and its CRC32C.
func pushed(t *testing.T, out *ControlOutput, prev chunkstore.Table, ctl *Control) {
	t.Helper()
	if !out.Push {
		t.Fatal("the commit pushed no table")
	}
	next, err := prev.Apply(&out.Delta)
	if err != nil {
		t.Fatalf("the delta does not apply: %v", err)
	}
	if !reflect.DeepEqual(next, *ctl.Table()) || next.CRC() != out.Delta.CRC {
		t.Fatalf("the delta gives %+v, the leader committed %+v", next, *ctl.Table())
	}
}

func intervals(t *chunkstore.Table) []uint32 {
	var ivs []uint32
	for _, p := range t.Pools {
		for _, e := range p.PGs {
			ivs = append(ivs, e.Interval)
		}
	}
	return ivs
}

// A pool the rules refuse stages nothing and commits nothing; one they accept commits under
// ctl/pool/ in the Txn that commits its first table, which every node gets as a delta.
func TestPoolsAreCheckedThenCommittedUnderCtl(t *testing.T) {
	r := newCtlRun(t)
	in := &ControlInputs{Disks: disks(0, 1, 2, 3)}
	if err := r.ctl.SetPool(1, spec(4, 8), in.Disks); err == nil {
		t.Fatal("a pool of 4 replicas was accepted")
	}
	if err := r.ctl.SetPool(1, spec(3, 8), in.Disks[:2]); !errors.Is(err, chunkstore.ErrTooFewDomains) {
		t.Fatalf("a rep3 pool on 2 nodes answered %v", err)
	}
	if r.propose(in) {
		t.Fatal("a refused pool was proposed")
	}
	if err := r.ctl.SetPool(1, spec(3, 8), in.Disks); err != nil {
		t.Fatal(err)
	}
	out := r.step(in)
	pushed(t, out, chunkstore.Table{}, r.ctl)
	if _, ok := get(r.c.n[r.l].s.State(), string(poolKey(1))); !ok {
		t.Fatal("the pool is not under ctl/pool/")
	}
	if p := r.ctl.Table().Pools; len(p) != 1 || len(p[0].PGs) != 8 || len(p[0].PGs[0].Members) != 3 {
		t.Fatalf("the first table is %+v", p)
	}
	if err := r.ctl.SetPool(1, spec(3, 32), in.Disks); err == nil {
		t.Fatal("pg_count grew fourfold")
	}
	if r.propose(in) {
		t.Fatal("an unchanged table was proposed")
	}
}

// Raising replicas starts a new interval of every PG of the pool.
func TestRaisingReplicasStartsAnIntervalOfEveryPG(t *testing.T) {
	r := newCtlRun(t)
	in := &ControlInputs{Disks: disks(0, 1, 2, 3)}
	if err := r.ctl.SetPool(1, spec(2, 8), in.Disks); err != nil {
		t.Fatal(err)
	}
	r.step(in)
	prev := *r.ctl.Table()
	if err := r.ctl.SetPool(1, spec(3, 8), in.Disks); err != nil {
		t.Fatal(err)
	}
	pushed(t, r.step(in), prev, r.ctl)
	for i, iv := range intervals(r.ctl.Table()) {
		if iv <= intervals(&prev)[i] {
			t.Fatalf("PG %d kept interval %d", i, iv)
		}
	}
}

// A table commits as one Txn of its head and the entries it changes: recovered reports, a down
// mark and an added disk each raise the epoch, and only the PGs whose members change start a new interval.
func TestATableCommitsAsItsDelta(t *testing.T) {
	r := newCtlRun(t)
	in := &ControlInputs{Disks: disks(0, 1, 2, 3)}
	if err := r.ctl.SetPool(1, spec(3, 8), in.Disks); err != nil {
		t.Fatal(err)
	}
	r.step(in)
	for i, e := range r.ctl.Table().Pools[0].PGs[:3] {
		r.ctl.Report(&chunkstore.Report{Recovered: []chunkstore.PGFields{{Epoch: 1, PG: core.PGID{Pool: 1, Index: uint32(i)}, Interval: e.Interval}}})
	}
	for step := range 3 {
		prev := *r.ctl.Table()
		switch step {
		case 1:
			in.Disks, in.Moves = append(in.Disks, disks(4)...), 1
		case 2:
			in.Disks[0].Mark, in.Disks[0].DownSince = chunkstore.MarkDown, 1
		}
		out := r.step(in)
		pushed(t, out, prev, r.ctl)
		puts := 0
		for _, op := range r.txn.Ops {
			if op.Kind == OpPut && op.Key[1] == CtlPlacement && len(op.Key) == 10 {
				puts++
			}
		}
		if out.Delta.Epoch != prev.Epoch+1 || puts != len(out.Delta.Entries) || len(out.Delta.Entries) == 0 || len(out.Delta.Entries) == 8 && step != 2 {
			t.Fatalf("epoch %d after %d, %d entry puts for %d changed entries", out.Delta.Epoch, prev.Epoch, puts, len(out.Delta.Entries))
		}
		for i, e := range r.ctl.Table().Pools[0].PGs {
			old := prev.Pools[0].PGs[i]
			if (e.Interval != old.Interval) != !reflect.DeepEqual(e.Members, old.Members) {
				t.Fatalf("PG %d went from %+v to %+v", i, old, e)
			}
		}
	}
}

// A primary's activation of interval k, reported on its heartbeat, is recorded with the members
// of the intervals before it dropped, and starts no interval.
func TestAnActivationIsRecordedWithoutANewInterval(t *testing.T) {
	r := newCtlRun(t)
	in := &ControlInputs{Disks: disks(0, 1, 2, 3)}
	if err := r.ctl.SetPool(1, spec(3, 8), in.Disks); err != nil {
		t.Fatal(err)
	}
	r.step(in)
	in.Disks[0].Mark, in.Disks[0].DownSince = chunkstore.MarkDown, 1
	r.step(in)
	pg := slices.IndexFunc(r.ctl.Table().Pools[0].PGs, func(e chunkstore.PGEntry) bool { return len(e.Past) > 0 })
	if pg < 0 {
		t.Fatal("no PG has past intervals")
	}
	e := r.ctl.Table().Pools[0].PGs[pg]
	r.ctl.Report(&chunkstore.Report{Activations: []chunkstore.PGFields{{Epoch: 2, PG: core.PGID{Pool: 1, Index: uint32(pg)}, Interval: e.Interval}}})
	r.step(in)
	got := r.ctl.Table().Pools[0].PGs[pg]
	if got.Activated != e.Interval || got.Interval != e.Interval || len(got.Past) != 0 {
		t.Fatalf("PG %d went from %+v to %+v", pg, e, got)
	}
}

func twoGroups() *ShardMap {
	return &ShardMap{Groups: []Group{{ID: 1}, {ID: 2, Start: PrefixEnd([]byte{SpaceCtl})}}}
}

func placeInputs(t *testing.T) *ControlInputs {
	rec, nodes := cluster(t, 3)
	return &ControlInputs{Recorded: rec.Disks(), MetaNodes: nodes}
}

// A new leader and a Raft snapshot of the group read back the pools, the table and the shard map
// whole, and the new leader's next table follows the last committed one.
func TestANewLeaderAndASnapshotReadBackTheWholeState(t *testing.T) {
	r := newCtlRun(t)
	in := placeInputs(t)
	in.Disks = disks(0, 1, 2, 3)
	if err := r.ctl.SetPool(1, spec(3, 8), in.Disks); err != nil {
		t.Fatal(err)
	}
	if err := r.ctl.SetShardMap(twoGroups()); err != nil {
		t.Fatal(err)
	}
	r.step(in)
	in.Disks[0].Mark, in.Disks[0].DownSince = chunkstore.MarkDown, 1
	r.step(in)
	same := func(c *Control) {
		t.Helper()
		// A slice read back empty is nil where the leader's may be empty, so the states compare as printed.
		if fmt.Sprintf("%+v", c.ctlState) != fmt.Sprintf("%+v", r.ctl.ctlState) {
			t.Fatalf("read back %+v, the leader holds %+v", c.ctlState, r.ctl.ctlState)
		}
	}
	st := r.c.n[r.l].s.State()
	snap := newTestState(t)
	if err := snap.Restore(st.Applied(), st.SnapshotData(st.Applied())); err != nil {
		t.Fatal(err)
	}
	c, err := NewControl(snap)
	if err != nil {
		t.Fatal(err)
	}
	same(c)
	r.c.cut[r.l] = true
	r.l = r.c.elect()
	r.ctl = r.load(r.l)
	same(r.ctl)
	prev := *r.ctl.Table()
	in.Disks[1].Mark, in.Disks[1].DownSince = chunkstore.MarkDown, 1
	pushed(t, r.step(in), prev, r.ctl)
}

// The shard map's entries reach their groups' leaders only once committed: a leader deposed
// before its commit hands out nothing, and its Txn, dropped, leaves the next leader to place the
// voters under IDs no group has seen.
func TestAPlanIsHandedOutOnlyOnceCommitted(t *testing.T) {
	r := newCtlRun(t)
	in := placeInputs(t)
	if err := r.ctl.SetShardMap(twoGroups()); err != nil {
		t.Fatal(err)
	}
	if !r.propose(in) {
		t.Fatal("no shard map proposed")
	}
	old, oldL := r.ctl, r.l
	r.c.cut[oldL] = true
	r.c.settle()
	r.l = r.c.elect()
	r.ctl = r.load(r.l)
	if len(r.ctl.ShardMap().Groups) != 0 {
		t.Fatalf("an uncommitted shard map was read back: %+v", r.ctl.ShardMap())
	}
	if err := r.ctl.SetShardMap(twoGroups()); err != nil {
		t.Fatal(err)
	}
	out := r.step(in)
	if len(out.Plans) != 2 || len(out.Plans[0].Voters) != 3 || out.Plans[0].LastID != 3 {
		t.Fatalf("the plans handed out are %+v", out.Plans)
	}
	r.c.cut[oldL] = false
	r.c.heartbeat()
	var oldOut ControlOutput
	a := r.c.answer(oldL, r.token-1)
	if err := old.Answer(r.c.n[oldL].s.State(), &a, &oldOut); err != nil {
		t.Fatal(err)
	}
	if a.Err == nil && a.Result.FailedCheck < 0 || len(oldOut.Plans) != 0 {
		t.Fatalf("the deposed leader's commit answered %+v and handed out %+v", a, oldOut.Plans)
	}
}

// Of two machines that each think they lead, the second's commit fails its checks against what
// the first committed; it reads the state again and its next commit lands on top.
func TestARacingCommitFailsItsChecks(t *testing.T) {
	r := newCtlRun(t)
	in := &ControlInputs{Disks: disks(0, 1, 2, 3)}
	other := r.load(r.l)
	if err := r.ctl.SetPool(1, spec(3, 8), in.Disks); err != nil {
		t.Fatal(err)
	}
	r.step(in)
	r.ctl = other
	if err := r.ctl.SetPool(2, spec(2, 8), in.Disks); err != nil {
		t.Fatal(err)
	}
	if !r.propose(in) {
		t.Fatal("nothing proposed")
	}
	if a := r.answer(); a.Result.FailedCheck < 0 || r.out.Push {
		t.Fatalf("a racing commit answered %+v", a)
	}
	r.step(in)
	if p := r.ctl.Pools(); len(p) != 2 || len(r.ctl.Table().Pools) != 2 || r.ctl.Table().Epoch != 2 {
		t.Fatalf("pools %+v, table epoch %d", p, r.ctl.Table().Epoch)
	}
}

// A value past MaxValue - 1 bytes is stored as a head and pieces, and a shorter one written over
// it drops the pieces it no longer has.
func TestALongValueIsStoredInPieces(t *testing.T) {
	s := newTestState(t)
	k := entryKey(core.PGID{Pool: 1, Index: 2})
	old := 0
	for _, n := range []int{2500, 1200, 10} {
		v := bytes.Repeat([]byte{byte(n)}, n)
		o, err := putSplit(nil, k, v, old)
		old = n
		if err != nil {
			t.Fatal(err)
		}
		run(t, s, ops(o...))
		var sr ScanResult
		if err := s.Scan(k, PrefixEnd(k), 100, &sr); err != nil {
			t.Fatal(err)
		}
		if len(sr.KVs) != 1+Pieces(n) {
			t.Fatalf("a %d-byte value is %d keys", n, len(sr.KVs))
		}
		if last, got, err := joinSplit(sr.KVs, 0); err != nil || last != Pieces(n) || !bytes.Equal(got, v) {
			t.Fatalf("a %d-byte value reads back as %d bytes, %v", n, len(got), err)
		}
	}
}

// A pool keeps its pg_count until splits are built: a change of it is refused, as is a pool
// change the operator stages twice before a step, which stages only the last.
func TestAPoolChangeIsCheckedAgainstTheCommittedPool(t *testing.T) {
	r := newCtlRun(t)
	in := &ControlInputs{Disks: disks(0, 1, 2, 3)}
	if err := r.ctl.SetPool(1, spec(2, 8), in.Disks); err != nil {
		t.Fatal(err)
	}
	r.step(in)
	if err := r.ctl.SetPool(1, spec(2, 16), in.Disks); err == nil {
		t.Fatal("a pg_count doubling was accepted")
	}
	for _, n := range []int{3, 1} {
		if err := r.ctl.SetPool(1, spec(n, 8), in.Disks); err != nil {
			t.Fatal(err)
		}
	}
	r.step(in)
	if p := r.ctl.Pools(); len(p) != 1 || p[0].Pool.Replicas != 1 || r.ctl.Table().Pools[0].Replicas != 1 {
		t.Fatalf("committed %+v", p)
	}
	if err := r.ctl.SetPool(1, spec(1, 8), in.Disks); err != nil || r.propose(in) {
		t.Fatalf("a pool set to the settings it has answered %v or proposed", err)
	}
}

// A machine whose commit lost a race reads the state back and checks what it staged against the
// pool now committed: a change the rules refuse there is dropped with an error naming its pool,
// and the next step commits the rest.
func TestAStagedPoolIsCheckedAgainAfterAReload(t *testing.T) {
	r := newCtlRun(t)
	in := &ControlInputs{Disks: disks(0, 1, 2, 3)}
	other := r.load(r.l)
	if err := r.ctl.SetPool(1, spec(3, 8), in.Disks); err != nil {
		t.Fatal(err)
	}
	r.step(in)
	r.ctl = other
	if err := r.ctl.SetPool(1, spec(3, 16), in.Disks); err != nil {
		t.Fatal(err)
	}
	if !r.propose(in) {
		t.Fatal("nothing proposed")
	}
	if a := r.answer(); a.Result.FailedCheck < 0 {
		t.Fatalf("a racing commit answered %+v", a)
	}
	var out ControlOutput
	if err := r.ctl.Step(in, 99, &out); err == nil || out.Propose {
		t.Fatalf("a stale pool change answered %v, proposed %v", err, out.Propose)
	}
	in.Disks[0].Mark, in.Disks[0].DownSince = chunkstore.MarkDown, 1
	r.step(in)
	if len(r.ctl.Table().Pools[0].PGs) != 8 {
		t.Fatalf("pool 1 has %d PGs", len(r.ctl.Table().Pools[0].PGs))
	}
}

// A pool whose refill after a rack loss may be more than one ctl/ Txn holds is refused: a rep3
// pool of 16,384 PGs, though its first table fits, and not one of 8,192. A pool that fits alone
// but not beside a step's other changes is dropped with ErrTooLarge, and the next step commits them.
func TestAPoolTooLargeForAStepIsRefused(t *testing.T) {
	r := newCtlRun(t)
	in := &ControlInputs{Disks: disks(0, 1, 2, 3)}
	if err := r.ctl.SetPool(1, spec(3, 1<<14), in.Disks); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("a pool of 16,384 PGs answered %v", err)
	}
	for id := range core.PoolID(3) {
		if err := r.ctl.SetPool(id+1, spec(3, 1<<13), in.Disks); err != nil {
			t.Fatal(err)
		}
		if id < 2 {
			r.step(in)
		}
	}
	in.Disks[0].Mark, in.Disks[0].DownSince = chunkstore.MarkDown, 1
	var out ControlOutput
	if err := r.ctl.Step(in, 99, &out); !errors.Is(err, ErrTooLarge) || out.Propose {
		t.Fatalf("a step past the limit answered %v, proposed %v", err, out.Propose)
	}
	r.step(in)
	if n, _ := r.txn.Check(); len(r.ctl.Pools()) != 2 || n <= MaxTxnBytes {
		t.Fatalf("committed pools %+v in a Txn of %d bytes", r.ctl.Pools(), n)
	}
}

// The control plane writes its keys as the golden vectors freeze them, a piece as its value's key
// and the piece number.
func TestCtlKeysAreTheGoldenOnes(t *testing.T) {
	g := func(name string) []byte { k := goldenKeys[name]; return mustKey(&k) }
	pg := core.PGID{Pool: 5, Index: 9}
	v := make([]byte, MaxValue)
	ops, err := putSplit(nil, entryKey(pg), v, 0)
	if err != nil {
		t.Fatal(err)
	}
	gops, err := putSplit(nil, groupKey(3), v, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		got  []byte
		name string
	}{
		{ctlKey(CtlPlacement, nil), "ctl/placement"},
		{ctlKey(CtlShardMap, nil), "ctl/shardmap"},
		{poolKey(5), "ctl/pool/5"},
		{placementKey(5), "ctl/placement/5"},
		{entryKey(pg), "ctl/placement/5/9"},
		{ops[1].Key, "ctl/placement/5/9#1"},
		{groupKey(3), "ctl/shardmap/3"},
		{gops[1].Key, "ctl/shardmap/3#1"},
	} {
		if !bytes.Equal(c.got, g(c.name)) {
			t.Errorf("%s is %x, golden %x", c.name, c.got, g(c.name))
		}
	}
}

// A table computed from a pool's settings another machine has since changed fails its checks,
// though the change left the table alone.
func TestATableIsCheckedAgainstEveryPoolItReads(t *testing.T) {
	r := newCtlRun(t)
	in := &ControlInputs{Disks: disks(0, 1, 2, 3)}
	if err := r.ctl.SetPool(1, spec(3, 8), in.Disks); err != nil {
		t.Fatal(err)
	}
	r.step(in)
	other := r.load(r.l)
	sp := spec(3, 8)
	sp.Overcommit = new(bool)
	if err := r.ctl.SetPool(1, sp, in.Disks); err != nil {
		t.Fatal(err)
	}
	if r.step(in).Push {
		t.Fatal("an overcommit change pushed a table")
	}
	r.ctl = other
	in.Disks[0].Mark, in.Disks[0].DownSince = chunkstore.MarkDown, 1
	if !r.propose(in) {
		t.Fatal("nothing proposed")
	}
	if a := r.answer(); a.Result.FailedCheck < 0 {
		t.Fatalf("a table from stale pool settings answered %+v", a)
	}
}
