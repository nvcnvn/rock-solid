package chunkstore

import (
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim"
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

// Each planted bug whose counterexample needs a trace no seeded run reaches breaks its invariant on that trace, which the correct code passes.
func TestPlantedBugsBreakTheirTraces(t *testing.T) {
	for _, tc := range []struct {
		name  string
		inv   string
		trace func(t *testing.T, plant bool) error
	}{
		{"held write reordered after another primary led", invIncarnation, reorderTrace},
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
