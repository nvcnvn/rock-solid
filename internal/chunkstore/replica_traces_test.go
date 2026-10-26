package chunkstore

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/lease"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/clock"
)

// pg2Table is the epoch change model's PG 2: one rep2 pool with min_live 2 whose one PG has disks 2 and 3, led by disk 2, beside a disk 1 in no PG.
func pg2Table() Table {
	return Table{Epoch: 1, Pools: []PoolTable{{ID: testPool, Replicas: 2, MinLive: 2, PGs: []PGEntry{{Members: []Member{{Disk: 2}, {Disk: 3}}, Interval: 1}}}}}
}

// members starts pg0's next interval with members ms.
func members(ms ...Member) func(t *Table) {
	return func(t *Table) {
		_, e := t.entry(pg0)
		setInterval(t, pg0, e.Interval+1)
		e.Members = ms
	}
}

// hand makes t the cluster's table and hands it to the replicas on nodes alone, the others cut off from the control plane.
func (c *rcluster) hand(t Table, nodes ...int) {
	c.table = t
	for _, n := range nodes {
		c.call(n, func(ctx *core.Context, r *Replica, out *ReplicaOutput) { r.SetTable(ctx, t, out) })
	}
}

// hello sends node n a hello of session 1's incarnation inc.
func (c *rcluster) hello(n int, inc uint64) {
	c.nextID++
	h := Hello{ID: c.nextID, Session: 1, Incarnation: inc}
	c.s.Node(c.client.Node).Call(c.client, func(ctx *core.Context, acts []core.Action) []core.Action {
		a := core.Action{Kind: core.ActNetSend, Peer: core.NodeID{Node: uint32(n)}}
		a.Msg.Len = uint16(copy(a.Msg.Body[:], AppendHello(nil, &h)))
		return append(acts, a)
	})
}

// incWrite sends pg0's primary a whole-block write of block 0 of ch filled with fill from session 1's incarnation inc, noting it in writes.
func (c *rcluster) incWrite(writes map[core.RequestID]incWrite, ch core.ChunkID, inc uint64, fill byte) core.RequestID {
	data := make([]byte, core.BlockSize)
	for i := range data {
		data[i] = fill
	}
	id := c.update(c.pgFields(pg0), ch, 0, data, 0, func(u *Update) { u.Incarnation = inc })
	writes[id] = incWrite{inc: inc, chunk: ch}
	return id
}

// reorderTrace is neg-reorder-after-other-primary's trace: primary P (disk 2) holds a dead incarnation's write W while it peers cut off from member Q (disk 3); interval 3 makes Q primary with P in maintenance, and the successor, which P never hears from, has its reissue W′ and its next write W2 acknowledged there; interval 4 makes P primary again.
func reorderTrace(t *testing.T, plant bool) error {
	c := newRCluster(t, pg2Table())
	c.replicaPlants.reorderAfterOther = plant
	for _, r := range c.reps {
		r.plant = c.replicaPlants
	}
	writes := map[core.RequestID]incWrite{}
	c.planted = true
	c.inv.Register(invIncarnation, func() error { return c.incarnations(writes) })
	ch := chunkIn(pg0, 1, 0)
	p, q, client := 1, 2, c.client.Node
	c.s.Partition(uint32(p), uint32(q), false)
	c.hand(c.next(members(Member{Disk: 2}, Member{Disk: 3})), p)
	c.runFor(100_000)
	w := c.incWrite(writes, ch, 1, 1)
	c.runFor(100_000)
	if i := c.reps[p].lead(pg0); i < 0 || len(c.reps[p].leads[i].held) != 1 {
		t.Fatal("P holds no write")
	}
	c.s.Partition(uint32(p), client, false)
	c.hand(c.next(members(Member{Disk: 3}, Member{Disk: 2, Mark: MarkMaint})), 0, q)
	c.untilActive()
	c.hello(q, 2)
	for fill := byte(2); fill <= 3; fill++ {
		id := c.incWrite(writes, ch, 2, fill)
		c.until(id)
		if rp := c.reply(id); rp.Status != StatusOK {
			t.Fatalf("incarnation 2's write answered %+v", rp)
		}
	}
	c.s.Heal(uint32(p), uint32(q))
	c.s.Heal(uint32(p), client)
	c.hand(c.next(members(Member{Disk: 2}, Member{Disk: 3})), 0, p, q)
	c.settle()
	if _, ok := c.cl.replies[w]; !ok {
		t.Fatal("W was never answered")
	}
	return c.found
}

// keepsAcked checks that every active lead of pg0 keeps every version of ch acknowledged to ids in an earlier interval.
func (c *rcluster) keepsAcked(ch core.ChunkID, ids ...core.RequestID) func() error {
	return func() error {
		for _, id := range ids {
			rp, ok := c.cl.replies[id]
			if !ok || rp.Status != StatusOK {
				continue
			}
			for n, r := range c.reps {
				i := r.lead(pg0)
				if i < 0 || !r.leads[i].active || r.leads[i].interval <= intervalOf(rp.Version) {
					continue
				}
				l := &r.leads[i]
				kept := uint64(0)
				if j := slices.IndexFunc(l.kept, func(k keptChunk) bool { return k.chunk == ch }); j >= 0 {
					kept = l.kept[j].kept
				}
				if kept < rp.Version {
					return fmt.Errorf("write %d acknowledged as %#x, disk %d's interval %d keeps %#x", id, rp.Version, c.ids[n], l.interval, kept)
				}
			}
		}
		return nil
	}
}

// planted starts a cluster on pg2Table with set planted in its replicas, noting the first broken invariant.
func plantedPG2(t *testing.T, set func(p *replicaPlants)) *rcluster {
	c := newRCluster(t, pg2Table())
	set(&c.replicaPlants)
	for _, r := range c.reps {
		r.plant = c.replicaPlants
	}
	c.planted = true
	return c
}

// stopPreviousTrace is neg-stop-previous-only's trace: W is acknowledged on disks 2 and 3 in interval 1; cut off from the control plane, neither learns of interval 2, which replaces primary 2 with disk 1, or of interval 3, which puts disk 3 in maintenance; peering of 3 reaches only disk 1.
func stopPreviousTrace(t *testing.T, plant bool) error {
	c := plantedPG2(t, func(p *replicaPlants) { p.stopPreviousOnly = plant })
	ch := chunkIn(pg0, 1, 0)
	w := c.incWrite(map[core.RequestID]incWrite{}, ch, 1, 1)
	c.until(w)
	c.inv.Register(invAckedKept, c.keepsAcked(ch, w))
	c.hand(c.next(members(Member{Disk: 1}, Member{Disk: 3})), 0)
	c.hand(c.next(members(Member{Disk: 1}, Member{Disk: 3, Mark: MarkMaint})), 0)
	c.runFor(2_000_000)
	return c.found
}

// reportDecidedTrace is neg-report-before-activate's trace: primary 2's write W is on its way to disk 3 when interval 2 replaces disk 3 with disk 1; primary 2 decides to keep W, but its missing list never reaches disk 1, and W completes; interval 3 puts disk 2 in maintenance, cut off from the control plane like disk 3, and makes disk 1 primary.
func reportDecidedTrace(t *testing.T, plant bool) error {
	c := plantedPG2(t, func(p *replicaPlants) { p.reportDecided = plant })
	ch := chunkIn(pg0, 1, 0)
	c.s.Queue(1, 2, 500_000)
	w := c.incWrite(map[core.RequestID]incWrite{}, ch, 1, 1)
	c.inv.Register(invAckedKept, c.keepsAcked(ch, w))
	c.runFor(100_000)
	c.drop = func(d int, a *core.Action) bool {
		return d == 1 && a.Kind == core.ActNetSend && a.Peer.Node == 0 && Kind(a.Msg.Body[1]) == KindPersist
	}
	c.hand(c.next(members(Member{Disk: 2}, Member{Disk: 1})), 0, 1)
	c.until(w)
	c.hand(c.next(members(Member{Disk: 1}, Member{Disk: 2, Mark: MarkMaint})), 0)
	c.runFor(2 * lease.DefaultConfig.Lease)
	return c.found
}

// instantTrace is neg-instant-reboot's trace: grantor 3 reboots the instant after it renewed primary 2's lease, cut off from it, and its new boot runs 2% fast; it comes back primary with disk 2 in maintenance, which disk 2, cut off from the control plane, never learns.
func instantTrace(t *testing.T, plant bool) error {
	set := sim.DefaultSettings()
	set.RebootInstantly = plant
	c := newRClusterOn(t, pg2Table(), sim.New(1, set))
	c.planted = true
	c.inv.Register(invOneReader, func() error { return c.oneReader(1) })
	granted := false
	c.onEvent = func(d int, ev *core.Event, done bool) {
		if d == 1 && done && ev.Kind == core.EvNetRecv && ev.Peer.Node == 2 && ev.Msg.Len >= headSize && Kind(ev.Msg.Body[1]) == KindGrant {
			granted = true
		}
	}
	for !granted && c.step() {
	}
	c.onEvent = nil
	c.s.Reboot(2)
	c.s.Partition(1, 2, false)
	c.hand(c.next(members(Member{Disk: 3}, Member{Disk: 2, Mark: MarkMaint})))
	for !c.s.Node(2).Up() && c.step() {
	}
	c.s.Node(2).Clock().SetRate(c.s.Time(), clock.Unit*102/100)
	for len(c.asks) == 0 && c.step() {
	}
	c.hand(c.next(members(Member{Disk: 3}, Member{Disk: 2, Mark: MarkMaint})), 0, 2)
	c.runFor(2 * lease.DefaultConfig.Lease)
	if c.serving(pg0, -1) != 2 {
		t.Fatal("disk 3 never serves reads")
	}
	return c.found
}

// Each planted bug whose counterexample needs a trace no seeded run reaches breaks its invariant on that trace, which the correct code passes.
func TestPlantedBugsBreakTheirTraces(t *testing.T) {
	for _, tc := range []struct {
		name  string
		inv   string
		trace func(t *testing.T, plant bool) error
	}{
		{"held write reordered after another primary led", invIncarnation, reorderTrace},
		{"peering stops only the previous interval", invAckedKept, stopPreviousTrace},
		{"activation reported once decided", invAckedKept, reportDecidedTrace},
		{"reboot that takes no time", invOneReader, instantTrace},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.trace(t, false); err != nil {
				t.Fatalf("unplanted: %v", err)
			}
			err := tc.trace(t, true)
			if v, ok := err.(*sim.Violation); !ok || v.Name != tc.inv {
				t.Fatalf("planted: %v", err)
			}
			if !strings.Contains(err.Error(), tc.inv) {
				t.Fatalf("planted: %v", err)
			}
		})
	}
}
