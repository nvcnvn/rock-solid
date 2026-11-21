package chunkstore

import (
	"bytes"
	"cmp"
	"fmt"
	"maps"
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
	c.inv.Register(invIncarnation, func() error { return c.incarnations(writes, invIncarnation) })
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

// keepsAcked checks that no interval of pg0 activated after the interval of each version of ch acknowledged to ids keeps a version below it. Every such interval is remembered once it activates, since the first one after the version's is the one whose number compare is exact: a later interval's newer version may rest on a base that dropped it.
func (c *rcluster) keepsAcked(ch core.ChunkID, ids ...core.RequestID) func() error {
	decided := map[uint32]uint64{}
	return func() error {
		for _, r := range c.reps {
			if i := r.lead(pg0); i >= 0 && r.leads[i].active {
				if _, ok := decided[r.leads[i].interval]; !ok {
					k, _ := r.leads[i].decision(ch)
					decided[r.leads[i].interval] = k.kept
				}
			}
		}
		for _, id := range ids {
			rp, ok := c.cl.replies[id]
			if !ok || rp.Status != StatusOK {
				continue
			}
			for _, k := range slices.Sorted(maps.Keys(decided)) {
				if k > intervalOf(rp.Version) && decided[k] < rp.Version {
					return fmt.Errorf("write %d acknowledged as %#x, interval %d keeps %#x", id, rp.Version, k, decided[k])
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

// settleChainTrace is the trace of a returning primary settling its pending versions by number: primary 2, cut off from disk 3, orders W as 1.1; interval 2, handed to disks 1 and 3 alone, makes disk 3 primary without disk 2 and drops W, and W2 is acknowledged there as 2.1; interval 3 makes disk 2 primary again beside both, and its peering keeps 2.1, on two copies, above W's 1.1 by number but not resting on it.
func settleChainTrace(t *testing.T, plant bool) error {
	c := newRCluster(t, pg2Table())
	c.storePlants.settleByNumber = plant
	for _, r := range c.reps {
		r.Store().plant = c.storePlants
	}
	c.planted = true
	ch := chunkIn(pg0, 1, 0)
	c.s.Partition(1, 2, false)
	w := c.incWrite(map[core.RequestID]incWrite{}, ch, 1, 1)
	c.inv.Register(invAckedKept, c.keepsAcked(ch, w))
	c.runFor(100_000)
	c.hand(c.next(members(Member{Disk: 3}, Member{Disk: 1})), 0, 2)
	c.untilActive()
	w2 := c.incWrite(map[core.RequestID]incWrite{}, ch, 1, 2)
	c.until(w2)
	if rp := c.reply(w2); rp.Status != StatusOK || rp.Version != MakeVersion(2, 1) {
		t.Fatalf("W2 answered %+v", rp)
	}
	c.s.Heal(1, 2)
	c.hand(c.next(members(Member{Disk: 2}, Member{Disk: 3}, Member{Disk: 1})), 0, 1, 2)
	c.settle()
	if rp, ok := c.cl.replies[w]; !ok || rp.Status == StatusOK && c.found == nil {
		t.Fatalf("W answered %+v, %v", rp, ok)
	}
	return c.found
}

// A primary that returns after an interval it missed dropped its pending version refuses that version's client, though the version its peering keeps is above it by number.
func TestAReturningPrimaryRefusesAPendingVersionOffTheKeptChain(t *testing.T) {
	if err := settleChainTrace(t, false); err != nil {
		t.Fatal(err)
	}
}

// instantTrace is neg-instant-reboot's trace: grantor 3 reboots the instant after it renewed primary 2's lease, cut off from it, and its new boot runs 2% fast, while primary 2 sleeps 0.99 s, a suspend that delivers no Resumed and so spends the margin left for one; grantor 3 comes back primary with disk 2 in maintenance, which disk 2, cut off from the control plane, never learns.
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
	c.s.Pause(1, clock.Suspend, 990_000, false)
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

// minLiveTrace is neg-read-below-min-live's trace: interval 2 marks disk 3 down, so primary 2 reads needing no lease; disk 3 restarts, and interval 3 makes it primary with disk 2 in maintenance, handed to disks 1 and 3 alone, so disk 2 reads on in interval 2 while peering of 3 skips that interval, which had fewer than min_live live members.
func minLiveTrace(t *testing.T, plant bool) error {
	c := plantedPG2(t, func(p *replicaPlants) { p.readBelowMinLive = plant })
	c.inv.Register(invOneReader, func() error { return c.oneReader(1) })
	c.hand(c.next(members(Member{Disk: 2}, Member{Disk: 3, Mark: MarkDown})), 0, 1, 2)
	c.s.CrashRandom(2)
	c.s.Restart(2)
	for !c.s.Node(2).Up() && c.step() {
	}
	c.hand(c.next(members(Member{Disk: 3}, Member{Disk: 2, Mark: MarkMaint})), 0, 2)
	c.runFor(2 * lease.DefaultConfig.Lease)
	if c.serving(pg0, 1) != 2 {
		t.Fatal("disk 3 never serves reads")
	}
	return c.found
}

// freshGrantTrace is neg-fresh-grant-no-table-check's trace: interval 2 puts primary 2 in maintenance and makes disk 3 primary, handed to disks 1 and 3 alone; disk 3 refuses disk 2's renewals, peers once its grant ends and serves, while disk 2, still on interval 1's table, asks for a lease afresh.
func freshGrantTrace(t *testing.T, plant bool) error {
	c := plantedPG2(t, func(p *replicaPlants) { p.freshGrantNoCheck = plant })
	c.inv.Register(invOneReader, func() error { return c.oneReader(1) })
	c.hand(c.next(members(Member{Disk: 3}, Member{Disk: 2, Mark: MarkMaint})), 0, 2)
	c.runFor(3 * lease.DefaultConfig.Lease)
	if c.serving(pg0, 1) != 2 {
		t.Fatal("disk 3 never serves reads")
	}
	return c.found
}

// lastActivatedTrace is the trace of peering that reaches only the last activated interval and the current one: interval 2 replaces disk 3 with disk 1 under primary 2, and interval 3 puts disk 1 in maintenance; primary 2 activates both, but the leader records neither; cut off from disks 1 and 3, disk 2 reads on in 3 with no lease while interval 4 makes disk 3, live in interval 1 only, primary with disk 2 in maintenance, handed to disks 1 and 3; once the cut heals, disk 2 gets it too.
func lastActivatedTrace(t *testing.T, plant bool) error {
	c := plantedPG2(t, func(p *replicaPlants) { p.lastActivatedOnly = plant })
	c.inv.Register(invOneReader, func() error { return c.oneReader(1) })
	c.publish(c.next(members(Member{Disk: 2}, Member{Disk: 1})), true)
	c.activations = nil
	c.publish(c.next(members(Member{Disk: 2}, Member{Disk: 1, Mark: MarkMaint})), true)
	c.activations = nil
	c.s.Partition(1, 0, false)
	c.s.Partition(1, 2, false)
	if c.serving(pg0, -1) != 1 {
		t.Fatal("disk 2 does not read in interval 3")
	}
	c.hand(c.next(members(Member{Disk: 3}, Member{Disk: 2, Mark: MarkMaint})), 0, 2)
	c.runFor(2 * lease.DefaultConfig.Lease)
	c.s.Heal(1, 0)
	c.s.Heal(1, 2)
	c.hand(c.table, 1)
	c.runFor(2 * lease.DefaultConfig.Lease)
	if c.serving(pg0, 1) != 2 {
		t.Fatal("disk 3 never serves reads")
	}
	return c.found
}

// readsFirstTrace is the trace of peer messages handled after reads in a pass: primary 2 stops in place with its clocks frozen for 10 s; interval 2 makes disk 3 primary with disk 2 in maintenance, handed to disks 1 and 3, and disk 3 serves once its grant to disk 2 ends; a read tagged with interval 1 reaches disk 2 at 9 s, behind disk 3's heartbeats.
func readsFirstTrace(t *testing.T, plant bool) error {
	set := sim.DefaultSettings()
	if plant {
		set.ReadsFirst = func(m *core.Msg) bool { return m.Len >= headSize && Kind(m.Body[1]) == KindRead }
	}
	c := newRClusterOn(t, pg2Table(), sim.New(1, set))
	c.planted = true
	var served error
	c.onEvent = func(d int, ev *core.Event, done bool) {
		if err := c.readArrives(d, ev); !done && err != nil && served == nil {
			served = err
		}
	}
	c.inv.Register(invOneReader, func() error { return cmp.Or(served, c.oneReader(1)) })
	c.runFor(1_000_000)
	if c.serving(pg0, -1) != 1 {
		t.Fatal("disk 2 does not serve reads")
	}
	old := c.table
	c.s.Pause(1, clock.PauseFrozen, 10_000_000, false)
	c.hand(c.next(members(Member{Disk: 3}, Member{Disk: 2, Mark: MarkMaint})), 0, 2)
	c.runFor(9_000_000)
	if c.serving(pg0, 1) != 2 {
		t.Fatal("disk 3 never serves reads")
	}
	cur := c.table
	c.table = old
	id := c.read(pg0, chunkIn(pg0, 1, 0), 0, 1, 1)
	c.table = cur
	c.until(id)
	return c.found
}

// logPastFailedTrace is neg-log-past-failed's trace: disk 3, its table marking it in maintenance for a moment, refuses 1.1,
// block 0, and the refusal is lost; back up in the same interval, it takes 1.2, block 1. Interval 2's peering then keeps
// 1.2 and counts disk 3, which holds 1.2 whole but lacks 1.1's block, as holding it.
func logPastFailedTrace(t *testing.T, plant bool) error {
	c := plantedPG2(t, func(p *replicaPlants) { p.logPastFailed = plant })
	ch := chunkIn(pg0, 1, 0)
	refusing := cloneTable(c.table)
	_, e := refusing.entry(pg0)
	e.Members[1].Mark = MarkMaint
	c.call(2, func(ctx *core.Context, r *Replica, out *ReplicaOutput) { r.table = refusing })
	lost := false
	c.drop = func(d int, a *core.Action) bool {
		if d == 2 && !lost && a.Kind == core.ActNetSend && a.Msg.Len >= headSize && Kind(a.Msg.Body[1]) == KindAck {
			lost = true
			return true
		}
		return false
	}
	c.write(pg0, ch, 0, []byte{1})
	for !lost && c.step() {
	}
	c.call(2, func(ctx *core.Context, r *Replica, out *ReplicaOutput) { r.table = c.table })
	c.write(pg0, ch, core.BlockSize, []byte{2})
	c.runFor(100_000)
	c.publish(c.next(func(t *Table) { setInterval(t, pg0, 2) }), true)
	c.settle()
	for b := range uint16(2) {
		if p, m := c.diskBlock(1, ch, b), c.diskBlock(2, ch, b); !bytes.Equal(p, m) {
			return cmp.Or(c.found, error(&sim.Violation{Name: invKeptWritten, Err: fmt.Errorf("disk 3 maps block %d as %d, disk 2 as %d", b, m[0], p[0])}))
		}
	}
	return c.found
}

// currentMinLiveTrace is neg-current-min-live's trace: interval 2 lowers min_live to 1 and marks disk 3 down, and W is acknowledged on disk 2 alone; interval 3 sets min_live back to 2, makes disk 3 primary and puts disk 2 in maintenance, while disk 2 is cut off from disk 3: peering of 3 must wait to reach disk 2, live in interval 2, which ran under min_live 1, and keeps W once the cut heals. No seeded run changes min_live.
func currentMinLiveTrace(t *testing.T, plant bool) error {
	c := plantedPG2(t, func(p *replicaPlants) { p.currentMinLive = plant })
	ch := chunkIn(pg0, 1, 0)
	c.publish(c.next(func(t *Table) {
		members(Member{Disk: 2}, Member{Disk: 3, Mark: MarkDown})(t)
		t.Pools[0].MinLive = 1
	}), true)
	w := c.incWrite(map[core.RequestID]incWrite{}, ch, 1, 1)
	c.until(w)
	if rp := c.reply(w); rp.Status != StatusOK {
		t.Fatalf("W answered %+v", rp)
	}
	c.inv.Register(invAckedKept, c.keepsAcked(ch, w))
	c.s.Partition(1, 2, false)
	c.publish(c.next(func(t *Table) {
		members(Member{Disk: 3}, Member{Disk: 2, Mark: MarkMaint})(t)
		t.Pools[0].MinLive = 2
	}), false)
	c.runFor(2 * lease.DefaultConfig.Lease)
	c.s.Heal(1, 2)
	c.runFor(2 * lease.DefaultConfig.Lease)
	return c.found
}

// noDemoteTrace is neg-no-demote's trace: primary 1 orders 1.1, its sends to both members lost, and interval 2 makes disk 2
// primary with disk 1 in maintenance. Disk 1, no longer leading, must refuse 1.1's client itself: no member refuses 1.1 and
// peering, which drops it, commits nothing that would answer it. In the seeded runs a member reached by the demoted primary's
// version answers it, a lost send comes only with a lost connection, whose resend a member refuses, and a version peering
// keeps commits and is answered OK.
func noDemoteTrace(t *testing.T, plant bool) error {
	c := newRCluster(t, rtable(4, 2))
	c.replicaPlants.noDemote = plant
	for _, r := range c.reps {
		r.plant = c.replicaPlants
	}
	c.planted = true
	ch := chunkIn(pg0, 4, 0)
	c.drop = func(d int, a *core.Action) bool {
		return d == 0 && a.Kind == core.ActNetSend && a.Msg.Len >= headSize && Kind(a.Msg.Body[1]) == KindReplicaWrite
	}
	w := c.write(pg0, ch, 0, []byte{1})
	c.runFor(100_000)
	c.publish(c.next(func(t *Table) {
		_, e := t.entry(pg0)
		e.Members = []Member{{Disk: 2}, {Disk: 3}, {Disk: 1, Mark: MarkMaint}}
		e.Interval = 2
	}), true)
	c.settle()
	if _, ok := c.cl.replies[w]; !ok {
		return cmp.Or(c.found, error(&sim.Violation{Name: invOneReply, Err: fmt.Errorf("1.1 unanswered once disk 1 lost its lead")}))
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
		{"kept primary reads below min_live", invOneReader, minLiveTrace},
		{"fresh grant with no table check", invOneReader, freshGrantTrace},
		{"peering reaches only the last activated interval", invOneReader, lastActivatedTrace},
		{"peer messages handled after reads in a pass", invOneReader, readsFirstTrace},
		{"pending versions settled by number", invAckedKept, settleChainTrace},
		{"past interval judged by the current min_live", invAckedKept, currentMinLiveTrace},
		{"logged past a failed version", invKeptWritten, logPastFailedTrace},
		{"pending versions left unanswered on demotion", invOneReply, noDemoteTrace},
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
