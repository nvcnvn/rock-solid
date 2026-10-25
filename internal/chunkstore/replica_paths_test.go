package chunkstore

import (
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

func cloneTable(t Table) Table {
	t.Pools = slices.Clone(t.Pools)
	for i := range t.Pools {
		p := &t.Pools[i]
		p.PGs = slices.Clone(p.PGs)
		for j := range p.PGs {
			p.PGs[j].Members = slices.Clone(p.PGs[j].Members)
			p.PGs[j].Past = slices.Clone(p.PGs[j].Past)
		}
	}
	return t
}

// next is the cluster's table one epoch on, edited by edit.
func (c *rcluster) next(edit func(t *Table)) Table {
	t := cloneTable(c.table)
	t.Epoch++
	edit(&t)
	return t
}

// mark sets disk's mark in pg and starts the PG's next interval.
func mark(t *Table, pg core.PGID, disk uint32, m Mark) {
	_, e := t.entry(pg)
	for i := range e.Members {
		if e.Members[i].Disk == disk {
			e.Members[i].Mark = m
		}
	}
	e.Interval++
}

func setInterval(t *Table, pg core.PGID, k uint32) {
	_, e := t.entry(pg)
	e.Interval = k
}

func TestThePrimaryFansOutWhileItLogs(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	mark := len(c.passes[0])
	c.write(pg0, chunkIn(pg0, 4, 0), 0, []byte{1})
	c.settle()
	for _, acts := range c.passes[0][mark:] {
		sends := slices.IndexFunc(acts, func(a core.Action) bool { return a.Kind == core.ActNetSend && !a.Reply })
		if sends < 0 {
			continue
		}
		peers := 0
		for _, a := range acts {
			if a.Kind == core.ActNetSend && !a.Reply {
				peers |= 1 << a.Peer.Node
			}
		}
		if peers != 0b110 || !slices.ContainsFunc(acts, func(a core.Action) bool { return a.Kind == core.ActDiskWrite }) {
			t.Fatalf("the pass that ordered the write sends to nodes %b and writes the disk: %v", peers, acts)
		}
		return
	}
	t.Fatal("the primary sent nothing")
}

func TestAWriteWaitsForEveryLiveMember(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	ch := chunkIn(pg0, 4, 0)
	c.write(pg0, ch, 0, []byte{1})
	c.settle()
	c.s.Partition(0, 2, false)
	w := c.write(pg0, ch, 0, []byte{2})
	c.runFor(100_000)
	if _, ok := c.cl.replies[w]; ok {
		t.Fatal("acknowledged without the cut-off member")
	}
	if c.newest(1, ch) != MakeVersion(1, 2) || c.newest(0, ch) != MakeVersion(1, 2) {
		t.Fatalf("the reachable members hold %#x and %#x", c.newest(0, ch), c.newest(1, ch))
	}
	if got := c.readNow(pg0, ch, 0, 1); got[0] != 1 {
		t.Fatalf("a read while 1.2 is pending returns %d, want 1.1's", got[0])
	}
}

func TestMinLiveAndMaintenance(t *testing.T) {
	for _, tc := range []struct {
		name     string
		replicas int
		minLive  uint8
		marks    []Mark
		ok       bool
		holders  int
	}{
		{"rep3, min_live 2, one down", 3, 2, []Mark{MarkUp, MarkUp, MarkDown}, true, 2},
		{"rep3, two down", 3, 2, []Mark{MarkUp, MarkDown, MarkDown}, false, 0},
		{"rep2, min_live 2, one down", 2, 2, []Mark{MarkUp, MarkDown}, false, 0},
		{"rep2, that node in maintenance", 2, 2, []Mark{MarkUp, MarkMaint}, true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tbl := rtable(4, tc.minLive)
			p := &tbl.Pools[0]
			p.Replicas = uint8(tc.replicas)
			_, e := tbl.entry(pg0)
			e.Members = e.Members[:tc.replicas]
			for i, m := range tc.marks {
				e.Members[i].Mark = m
			}
			c := newRCluster(t, tbl)
			ch := chunkIn(pg0, 4, 0)
			w := c.write(pg0, ch, 0, []byte{7})
			c.settle()
			rp := c.reply(w)
			if (rp.Status == StatusOK) != tc.ok {
				t.Fatalf("answered %+v", rp)
			}
			if !tc.ok && (rp.Status != StatusRefused || rp.Interval != 1) {
				t.Fatalf("refused as %+v", rp)
			}
			holders := 0
			for d := range 3 {
				if c.newest(d, ch) != 0 {
					holders++
				}
			}
			if holders != tc.holders {
				t.Fatalf("%d disks hold the write, want %d", holders, tc.holders)
			}
		})
	}
}

// A member whose table has PG 7 in interval 42 accepts only messages tagged 42, and fetches a newer table before it judges a higher tag.
func TestAMemberAcceptsOnlyThePGsCurrentInterval(t *testing.T) {
	pg7 := core.PGID{Pool: testPool, Index: 7}
	tbl := rtable(8, 2)
	tbl.Epoch = 10
	setInterval(&tbl, pg7, 42)
	c := newRCluster(t, tbl)
	member := 2 // PG 7 is led by disk 2 and has disks 3 and 1 as members.
	ch := chunkIn(pg7, 8, 0)
	ver := MakeVersion(42, 0)
	send := func(epoch uint64, tag uint32) core.RequestID {
		ver++
		c.nextID++
		id := c.nextID
		u := Update{Kind: KindReplicaWrite, ID: id, PG: PGFields{Epoch: epoch, PG: pg7, Interval: tag}, Chunk: ch, Len: core.BlockSize, Version: ver}
		c.s.Node(c.client.Node).Call(c.client, func(ctx *core.Context, acts []core.Action) []core.Action {
			a := core.Action{Kind: core.ActNetSend, Peer: core.NodeID{Node: uint32(member)}}
			a.Msg.Len = uint16(copy(a.Msg.Body[:], AppendUpdate(nil, &u)))
			a.Msg.Buf, _ = ctx.Pool.AllocZeroed(core.BlockSize)
			c.cl.sent[id] = a.Msg.Buf
			return append(acts, a)
		})
		c.settle()
		return id
	}
	for _, epoch := range []uint64{9, 11} {
		if rp := c.reply(send(epoch, 42)); rp.Status != StatusOK {
			t.Fatalf("tag 42 at sender epoch %d: %+v", epoch, rp)
		}
	}
	if rp := c.reply(send(12, 41)); rp.Status != StatusRefused || rp.Epoch != 10 || rp.Interval != 42 {
		t.Fatalf("tag 41: %+v", rp)
	}
	id := send(12, 43)
	if _, ok := c.cl.replies[id]; ok || c.fetch[member] != 12 {
		t.Fatalf("tag 43 from epoch 12: answered before a fetch (asked for %d)", c.fetch[member])
	}
	c.publish(c.next(func(t *Table) { t.Epoch = 12; setInterval(t, pg7, 43) }), true)
	c.settle()
	if rp := c.reply(id); rp.Status != StatusOK {
		t.Fatalf("tag 43 once PG 7 is at 43: %+v", rp)
	}
	id = send(13, 44)
	c.publish(c.next(func(t *Table) { t.Epoch = 13; setInterval(t, pg7, 45) }), true)
	c.settle()
	if rp := c.reply(id); rp.Status != StatusRefused || rp.Interval != 45 {
		t.Fatalf("tag 44 once PG 7 is at 45: %+v", rp)
	}
	if rp := c.reply(send(12, 46)); rp.Status != StatusRefused || rp.Interval != 45 {
		t.Fatalf("tag 46 from an older table: %+v", rp)
	}
}

// Primary B's term began in interval 42 and PG 7 is at 44 after two replica swaps.
func TestClientWritesAreTakenWithinThePrimarysTerm(t *testing.T) {
	pg7 := core.PGID{Pool: testPool, Index: 7}
	tbl := rtable(8, 2)
	setInterval(&tbl, pg7, 42)
	c := newRCluster(t, tbl)
	c.publish(c.next(func(t *Table) { mark(t, pg7, 3, MarkDown) }), true)
	c.publish(c.next(func(t *Table) { mark(t, pg7, 3, MarkUp) }), true)
	ch := chunkIn(pg7, 8, 0)
	tag := func(k uint32) core.RequestID {
		return c.update(PGFields{Epoch: c.table.Epoch, PG: pg7, Interval: k}, ch, 0, []byte{byte(k)}, 0, nil)
	}
	ws := []core.RequestID{tag(42), tag(43), tag(44)}
	refused := tag(41)
	c.settle()
	for i, w := range ws {
		if rp := c.reply(w); rp.Status != StatusOK || rp.Version != MakeVersion(44, uint32(i+1)) {
			t.Fatalf("tag %d: %+v", 42+i, rp)
		}
	}
	if rp := c.reply(refused); rp.Status != StatusRefused || rp.Interval != 44 {
		t.Fatalf("tag 41: %+v", rp)
	}
	c.update(PGFields{Epoch: c.table.Epoch + 1, PG: pg7, Interval: 45}, ch, 0, []byte{45}, 0, nil)
	c.settle()
	if c.fetch[1] != c.table.Epoch+1 {
		t.Fatalf("tag 45 asked for table %d", c.fetch[1])
	}
}

func TestAWriteOfAnOlderIncarnationIsRefused(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	ch := chunkIn(pg0, 4, 0)
	c.s.Node(c.client.Node).Call(c.client, func(ctx *core.Context, acts []core.Action) []core.Action {
		a := core.Action{Kind: core.ActNetSend, Peer: core.NodeID{Node: 0}}
		a.Msg.Len = uint16(copy(a.Msg.Body[:], AppendHello(nil, &Hello{ID: 99, Session: 1, Incarnation: 2})))
		return append(acts, a)
	})
	w := c.write(pg0, ch, 0, []byte{1})
	c.settle()
	if rp := c.reply(w); rp.Status != StatusRefused {
		t.Fatalf("incarnation 1 after 2's hello: %+v", rp)
	}
	w = c.update(c.pgFields(pg0), ch, 0, []byte{1}, 0, func(u *Update) { u.Incarnation = 2 })
	c.settle()
	if rp := c.reply(w); rp.Status != StatusOK {
		t.Fatalf("incarnation 2: %+v", rp)
	}
}

func TestReadsSeeOnlyCommittedVersions(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	ch := chunkIn(pg0, 4, 0)
	c.write(pg0, ch, 0, []byte{1})
	c.settle()
	c.s.Partition(0, 2, false)
	c.write(pg0, ch, 0, []byte{2})
	c.runFor(100_000)
	if got := c.readNow(pg0, ch, 0, 1); got[0] != 1 {
		t.Fatalf("read %d while 1.2 is pending", got[0])
	}
	c.nextID++
	id := c.nextID
	rd := Read{ID: id, PG: c.pgFields(pg0), Chunk: ch, Len: 1, HasAt: true, At: MakeVersion(1, 2)}
	c.s.Node(c.client.Node).Call(c.client, func(ctx *core.Context, acts []core.Action) []core.Action {
		a := core.Action{Kind: core.ActNetSend, Peer: core.NodeID{Node: 1}}
		a.Msg.Len = uint16(copy(a.Msg.Body[:], AppendRead(nil, &rd)))
		return append(acts, a)
	})
	c.runFor(100_000)
	if rp := c.reply(id); rp.Status == StatusOK {
		t.Fatalf("a member served at_ver of a version it holds pending: %+v", rp)
	}
}

func TestVersionsFollowThePGsInterval(t *testing.T) {
	tbl := rtable(4, 2)
	setInterval(&tbl, pg0, 7)
	c := newRCluster(t, tbl)
	ch := chunkIn(pg0, 4, 0)
	w := c.write(pg0, ch, 0, []byte{1})
	c.settle()
	if rp := c.reply(w); rp.Version != MakeVersion(7, 1) {
		t.Fatalf("first write in interval 7: %+v", rp)
	}
	c.publish(c.next(func(t *Table) { mark(t, pg0, 3, MarkDown) }), true)
	w = c.write(pg0, ch, 0, []byte{2})
	c.settle()
	if rp := c.reply(w); rp.Version != MakeVersion(8, 1) {
		t.Fatalf("first write in interval 8, after 7.1: %+v", rp)
	}
}
