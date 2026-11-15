package meta

import (
	"errors"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
)

// heal reports every PG's current interval activated and recovered, as its primary would once it
// peered and copied.
func (r *ctlRun) heal() {
	var rp chunkstore.Report
	for i, e := range r.ctl.Table().Pools[0].PGs {
		f := chunkstore.PGFields{Epoch: r.ctl.Table().Epoch, PG: core.PGID{Pool: 1, Index: uint32(i)}, Interval: e.Interval}
		rp.Activations, rp.Recovered = append(rp.Activations, f), append(rp.Recovered, f)
	}
	r.ctl.Report(&rp)
}

// named counts the PGs that name disk id as a member, leaving or not, or, with past, in an
// interval since their last activated one.
func named(t *chunkstore.Table, id uint32, past bool) int {
	n := 0
	on := func(m chunkstore.Member) bool { return m.Disk == id }
	for _, e := range t.Pools[0].PGs {
		if slices.ContainsFunc(e.Members, on) || past && slices.ContainsFunc(e.Past, func(p chunkstore.PastInterval) bool { return slices.ContainsFunc(p.Members, on) }) {
			n++
		}
	}
	return n
}

// A dead node's two disks, refilled and kept as leaving members, are declared lost: the
// declarations are refused while the disks are up or unknown and change nothing, then commit,
// end every leaving member on them with no recovered report, and a later report of their logs is
// refused.
func TestADeadNodesDisksDeclaredLostEndTheirLeavingMembers(t *testing.T) {
	r := newCtlRun(t)
	second := OpenedDisk{Device: 0x100, Log: 0x1000}
	r.heartbeat(&NodeReport{Node: 0, Meta: true, Opened: []OpenedDisk{{Device: devOf(0), Log: logOf(0)}, second}})
	r.event(0, chunkstore.AppendReport(nil, &chunkstore.Report{ID: 1, Disks: []chunkstore.DiskState{{Log: second.Log}}}))
	in := &ControlInputs{Disks: append(disks(0, 1, 2, 3, 4), chunkstore.Disk{ID: 6, Node: 0, Blocks: 1 << 28})}
	if err := r.ctl.SetPool(1, spec(3, 16), in.Disks); err != nil {
		t.Fatal(err)
	}
	r.settle(in)
	if named(r.ctl.Table(), 1, false) == 0 || named(r.ctl.Table(), 6, false) == 0 {
		t.Fatal("node 0's disks hold no PG")
	}
	if err := r.ctl.Lost(r.now, 1); !errors.Is(err, chunkstore.ErrLostRefused) {
		t.Fatalf("a declaration for an up disk answered %v", err)
	}
	if err := r.ctl.Lost(r.now, 9); !errors.Is(err, ErrUnknownDisk) {
		t.Fatalf("a declaration for an unknown disk answered %v", err)
	}
	if in.Now = r.now; r.propose(in) {
		t.Fatalf("refused declarations proposed %+v", r.txn.Ops)
	}

	delete(r.reports, 0)
	r.run(chunkstore.NodeDownAfter, 0)
	r.settle(in)
	r.run(chunkstore.RefillAfter+2*sec, 0)
	r.settle(in)
	dead := func(m chunkstore.Member) bool { return m.Disk == 1 || m.Disk == 6 }
	leaving := 0
	for _, e := range r.ctl.Table().Pools[0].PGs {
		if slices.ContainsFunc(e.Members, func(m chunkstore.Member) bool { return dead(m) && !m.Leaving }) {
			t.Fatalf("a PG keeps a dead disk as a member after refill_after: %+v", e)
		}
		if slices.ContainsFunc(e.Members, dead) {
			leaving++
		}
	}
	if leaving == 0 {
		t.Fatal("no PG keeps a dead disk as a leaving member")
	}

	for _, id := range []uint32{1, 6} {
		if err := r.ctl.Lost(r.now, id); err != nil {
			t.Fatalf("disk %d: %v", id, err)
		}
	}
	r.settle(in)
	for _, id := range []uint32{1, 6} {
		if !r.disk(id).Gone || !r.load(r.l).disks[id-1].Gone || named(r.ctl.Table(), id, false) != 0 {
			t.Fatalf("disk %d declared lost: %+v, named by %d PGs", id, r.disk(id), named(r.ctl.Table(), id, false))
		}
		if err := r.ctl.Lost(r.now, id); err != nil {
			t.Fatalf("a repeated declaration of disk %d answered %v", id, err)
		}
	}
	var out ControlOutput
	err := r.ctl.Heartbeat(r.now, &NodeReport{Node: 0, Meta: true, Opened: []OpenedDisk{{Device: devOf(0), Log: logOf(0)}, second}}, &out)
	if !errors.Is(err, chunkstore.ErrLogRefused) || len(r.ctl.Disks()) != 6 {
		t.Fatalf("a lost disk's log reported again answered %v, record %+v", err, r.ctl.Disks())
	}
}

// A declaration staged on a leader whose commit fails, another commit having raced it, is applied
// again over the state read back, and commits with the next Step.
func TestADeclarationOutlivesAFailedCommit(t *testing.T) {
	r, in := poolRun(t)
	delete(r.reports, 0)
	r.run(chunkstore.NodeDownAfter, 0)
	r.settle(in)
	mine, other := r.ctl, r.load(r.l)
	if err := mine.Lost(r.now, 1); err != nil {
		t.Fatal(err)
	}
	if err := other.Drain(3, true); err != nil {
		t.Fatal(err)
	}
	r.ctl = other
	r.settle(in)
	r.ctl = mine
	if !r.propose(in) {
		t.Fatal("nothing proposed")
	}
	if a := r.answer(); a.Result.FailedCheck < 0 {
		t.Fatalf("a raced commit answered %+v", a)
	}
	r.settle(in)
	if !r.disk(1).Gone || named(r.ctl.Table(), 1, false) != 0 {
		t.Fatalf("disk 1 %+v named by %d PGs after the retried commit", r.disk(1), named(r.ctl.Table(), 1, false))
	}
}

// A drained disk is removed only once its drain has committed and no PG names it, a leaving
// member included; up, it then leaves the pool, and its log is refused.
func TestADrainedDiskIsRemovedOnceNoPGNamesIt(t *testing.T) {
	r, in := poolRun(t)
	if err := r.ctl.Remove(r.now, 5); !errors.Is(err, ErrNotDrained) {
		t.Fatalf("removing an undrained disk answered %v", err)
	}
	if err := r.ctl.Remove(r.now, 9); !errors.Is(err, ErrUnknownDisk) {
		t.Fatalf("removing an unknown disk answered %v", err)
	}
	if err := r.ctl.Drain(5, true); err != nil {
		t.Fatal(err)
	}
	in.Moves = 16
	refused := false
	for range 20 {
		r.heal()
		r.run(sec)
		r.settle(in)
		if named(r.ctl.Table(), 5, true) == 0 {
			break
		}
		if err := r.ctl.Remove(r.now, 5); !errors.Is(err, ErrNotDrained) {
			t.Fatalf("removing a disk %d PGs name answered %v", named(r.ctl.Table(), 5, true), err)
		}
		refused = true
	}
	if !refused || named(r.ctl.Table(), 5, true) != 0 {
		t.Fatalf("the drain ended with %d PGs naming disk 5, a removal refused %v", named(r.ctl.Table(), 5, true), refused)
	}
	prev := *r.ctl.Table()
	if err := r.ctl.Remove(r.now, 5); err != nil {
		t.Fatal(err)
	}
	r.settle(in)
	if d := r.disk(5); !d.Gone || d.Up || d.Drain || r.ctl.Table().Epoch != prev.Epoch {
		t.Fatalf("removed disk 5 is %+v, table %d after %d", d, r.ctl.Table().Epoch, prev.Epoch)
	}
	var out ControlOutput
	if err := r.ctl.Heartbeat(r.now, &NodeReport{Node: 4, Meta: true, Opened: []OpenedDisk{{Device: devOf(4), Log: logOf(4)}}}, &out); !errors.Is(err, chunkstore.ErrLogRefused) {
		t.Fatalf("a removed disk's log reported again answered %v", err)
	}
}
