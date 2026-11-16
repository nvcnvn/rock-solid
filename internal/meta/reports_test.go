package meta

import (
	"fmt"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
)

// reportRun is a grantRun with no sessions whose pool 1, one copy of 8 PGs of disk class pool, is on
// one disk per class given, each of class classes[i] as its caller gives it, and is ready and
// recovered; each step makes up to 8 balance moves.
func reportRun(t *testing.T, pool string, classes ...string) *grantRun {
	r := &grantRun{ctlRun: newCtlRun(t), blocks: gb(1000), reserve: 8 * mib(64)}
	r.in.Moves = 8
	for i, c := range classes {
		r.live = append(r.live, gb(1))
		r.in.Disks = append(r.in.Disks, chunkstore.Disk{ID: uint32(i + 1), Node: uint32(i), Blocks: r.blocks, Live: gb(1), Class: c})
	}
	sp := spec(1, 8)
	sp.MinLive, sp.DiskClass = new(1), pool
	if err := r.ctl.SetPool(1, sp, r.in.Disks); err != nil {
		t.Fatal(err)
	}
	r.settle(r.inputs())
	r.opened()
	r.tick(FillCover)
	r.fills()
	r.recover()
	return r
}

// recover reports every PG's current interval recovered and steps the leader until it proposes nothing.
func (r *grantRun) recover() {
	r.heal(true)
	for r.settle(r.inputs()) {
	}
}

// leads sends node's heartbeat naming the PGs it leads.
func (r *grantRun) leads(node uint32, ls ...chunkstore.LeadState) {
	r.event(node, chunkstore.AppendReport(nil, &chunkstore.Report{ID: 1, Leads: ls}))
}

// conditions steps the leader and returns pool 1's conditions.
func (r *grantRun) conditions() *chunkstore.Conditions {
	r.t.Helper()
	for r.settle(r.inputs()) {
	}
	_, c, ok := r.ctl.Capacity(1)
	if !ok {
		r.t.Fatal("pool 1 not ready")
	}
	return c
}

// A drained disk's PG with no active report for its interval goes Stuck and Blocked, Stopped, after
// StuckAfter; once its primary reports copies lacking it is no longer Stopped, and it clears when
// the copies lacking fall.
func TestALeavingMemberGoesStuckAndClearsWhenItCopies(t *testing.T) {
	r := reportRun(t, "", "", "", "")
	if err := r.ctl.Drain(1, true); err != nil {
		t.Fatal(err)
	}
	r.conditions()
	pgs := r.ctl.Table().Pools[0].PGs
	i := slices.IndexFunc(pgs, func(e chunkstore.PGEntry) bool {
		return slices.ContainsFunc(e.Members, func(m chunkstore.Member) bool { return m.Leaving })
	})
	if i < 0 {
		t.Fatal("the drain left no leaving member")
	}
	pg, iv := uint32(i), pgs[i].Interval
	if c := r.conditions(); slices.Contains(c.Stuck, pg) {
		t.Fatalf("PG %d Stuck at once: %+v", pg, c)
	}
	r.run(chunkstore.StuckAfter)
	c := r.conditions()
	b := slices.IndexFunc(c.Blocked, func(b chunkstore.BlockedPG) bool { return b.PG == pg })
	if !slices.Contains(c.Stuck, pg) || b < 0 || !c.Blocked[b].Stopped {
		t.Fatalf("PG %d not Stuck and Blocked, Stopped, after StuckAfter: %+v", pg, c)
	}
	lead := chunkstore.LeadState{PG: core.PGID{Pool: 1, Index: pg}, Interval: iv, Active: true, Lacking: 5}
	r.leads(1, lead)
	c = r.conditions()
	b = slices.IndexFunc(c.Blocked, func(b chunkstore.BlockedPG) bool { return b.PG == pg })
	if !slices.Contains(c.Stuck, pg) || b < 0 || c.Blocked[b].Stopped {
		t.Fatalf("PG %d with its primary's report: %+v", pg, c)
	}
	lead.Lacking = 3
	r.leads(1, lead)
	if c := r.conditions(); slices.Contains(c.Stuck, pg) {
		t.Fatalf("PG %d still Stuck after its copies lacking fell: %+v", pg, c)
	}
}

// An active report of unfound chunks Blocks its PG with that count, not Stuck, until a report of
// none; reports naming a PG or pool the table does not have are dropped.
func TestAReportOfUnfoundChunksBlocksItsPG(t *testing.T) {
	r := reportRun(t, "", "", "", "")
	iv := r.ctl.Table().Pools[0].PGs[2].Interval
	r.leads(0,
		chunkstore.LeadState{PG: core.PGID{Pool: 1, Index: 2}, Interval: iv, Active: true, Unfound: 2},
		chunkstore.LeadState{PG: core.PGID{Pool: 1, Index: 99}, Interval: 1, Active: true, Unfound: 1},
		chunkstore.LeadState{PG: core.PGID{Pool: 7}, Interval: 1, Active: true, Unfound: 1})
	c := r.conditions()
	if len(c.Stuck) != 0 || len(c.Blocked) != 1 || c.Blocked[0].PG != 2 || c.Blocked[0].Unfound != 2 || c.Blocked[0].Stopped {
		t.Fatalf("conditions %+v, want PG 2 Blocked with 2 unfound", c)
	}
	r.leads(0, chunkstore.LeadState{PG: core.PGID{Pool: 1, Index: 2}, Interval: iv, Active: true})
	if c := r.conditions(); len(c.Blocked) != 0 {
		t.Fatalf("conditions %+v once the chunks are found", c)
	}
}

// A disk the caller gives no class takes the class it reports, for placement and figures; a later
// report naming another class, of that disk or of one the caller classed, moves no PG and changes
// no figure.
func TestAReportedClassIsSetOnce(t *testing.T) {
	r := reportRun(t, "ssd", "ssd", "ssd", "")
	if members(r.ctl.Table(), 3) != 0 {
		t.Fatal("an unclassed disk holds a PG")
	}
	class := func(node uint32, c string) {
		r.event(node, chunkstore.AppendReport(nil, &chunkstore.Report{ID: 1, Disks: []chunkstore.DiskState{{Log: logOf(node), Fill: gb(1), Class: c}}}))
	}
	class(2, "ssd")
	r.recover()
	f, _, _ := r.ctl.Capacity(1)
	if members(r.ctl.Table(), 3) == 0 || !slices.ContainsFunc(f.Disks, func(d chunkstore.DiskFigures) bool { return d.ID == 3 }) {
		t.Fatalf("disk 3, reported ssd, not used: figures %+v", f.Disks)
	}
	table, figures := fmt.Sprint(r.ctl.Table().Pools[0].PGs), fmt.Sprint(*f)
	class(2, "hdd")
	class(0, "hdd")
	r.conditions()
	f, _, _ = r.ctl.Capacity(1)
	if got := fmt.Sprint(r.ctl.Table().Pools[0].PGs); got != table {
		t.Fatalf("a class flip moved PGs:\n%s\nwas\n%s", got, table)
	}
	if got := fmt.Sprint(*f); got != figures {
		t.Fatalf("a class flip changed the figures:\n%s\nwas\n%s", got, figures)
	}
}
