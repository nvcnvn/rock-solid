package chunkstore

import (
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// admitCluster is pg0 led by disk 1 under a pool write_limit of limit, every disk 1,000 blocks; disk 3 (X) reports fill blocks.
func admitCluster(t *testing.T, limit uint8) *rcluster {
	c := newRCluster(t, rtable(4, 2))
	c.call(0, func(ctx *core.Context, r *Replica, out *ReplicaOutput) {
		r.SetPool(testPool, Pool{WriteLimit: limit})
		for d := range uint32(3) {
			r.SetDisk(d+1, 1000)
		}
	})
	return c
}

func (c *rcluster) xFills(live uint64) {
	c.call(0, func(ctx *core.Context, r *Replica, out *ReplicaOutput) { r.MemberFill(3, live) })
}

func (c *rcluster) status(id core.RequestID) Status {
	c.t.Helper()
	c.settle()
	return c.reply(id).Status
}

func TestWriteLimitAdmission(t *testing.T) {
	c := admitCluster(t, 90)
	ch := chunkIn(pg0, 4, 0)
	c.writeWhole(pg0, ch, 3, 2, 0xaa)
	if live := c.reps[0].disks[2].live; live == 0 || live != c.reps[2].Store().Log().LiveBlocks() {
		t.Fatalf("the primary holds X's fill as %d from its acknowledgment, X counts %d", live, c.reps[2].Store().Log().LiveBlocks())
	}

	c.xFills(910)
	if st := c.status(c.write(pg0, ch, 0, []byte{1})); st != StatusNoSpace {
		t.Fatalf("a write adding live data with X at 91%%: %v", st)
	}
	c.xFills(910)
	if st := c.status(c.update(c.pgFields(pg0), ch, 0, []byte{1}, 0, func(u *Update) { u.Granted = true })); st != StatusOK {
		t.Fatalf("a granted flush: %v", st)
	}
	c.xFills(910)
	if st := c.status(c.write(pg0, ch, 3*core.BlockSize+10, make([]byte, core.BlockSize))); st != StatusOK {
		t.Fatalf("an overwrite of blocks 3 and 4, held live: %v", st)
	}
	c.xFills(910)
	if st := c.status(c.write(pg0, ch, 3*core.BlockSize, make([]byte, 3*core.BlockSize))); st != StatusNoSpace {
		t.Fatalf("an overwrite of blocks 3 and 4 with block 5 never written: %v", st)
	}
	c.xFills(910)
	if st := c.status(c.update(c.pgFields(pg0), ch, 3*core.BlockSize, []byte{1}, 0, func(u *Update) { u.Snapshot = 1 })); st != StatusNoSpace {
		t.Fatalf("an overwrite a freeze is due for: %v", st)
	}
	c.xFills(910)
	if st := c.status(c.update(c.pgFields(pg0), ch, 3*core.BlockSize, nil, 100, nil)); st != StatusOK {
		t.Fatalf("a punch: %v", st)
	}

	c.xFills(880)
	c.call(0, func(ctx *core.Context, r *Replica, out *ReplicaOutput) { r.SetGrants(testPool, 0) })
	if st := c.status(c.write(pg0, ch, 6*core.BlockSize, []byte{1})); st != StatusOK {
		t.Fatalf("every member under 90%%: %v", st)
	}
	// rep3 on three disks puts a member of every PG on X, a share of 1: each block granted counts on it.
	c.xFills(880)
	c.call(0, func(ctx *core.Context, r *Replica, out *ReplicaOutput) { r.SetGrants(testPool, 20) })
	if st := c.status(c.write(pg0, ch, 7*core.BlockSize, []byte{1})); st != StatusNoSpace {
		t.Fatalf("X at 88%% with its share of grants reaching 90%%: %v", st)
	}
	c.xFills(880)
	c.call(0, func(ctx *core.Context, r *Replica, out *ReplicaOutput) { r.SetGrants(testPool, 19) })
	if st := c.status(c.write(pg0, ch, 7*core.BlockSize, []byte{1})); st != StatusOK {
		t.Fatalf("X at 88%% with grants short of 90%%: %v", st)
	}
}

// Two pools on one disk at 92% total fill: each checks its own limit against the disk's total fill.
func TestEachPoolChecksItsOwnLimit(t *testing.T) {
	tbl := rtable(4, 2)
	p2 := tbl.Pools[0]
	p2.ID = testPool + 1
	tbl.Pools = append(tbl.Pools, p2)
	c := newRCluster(t, tbl)
	c.call(0, func(ctx *core.Context, r *Replica, out *ReplicaOutput) {
		r.SetPool(testPool, Pool{WriteLimit: 95})
		r.SetPool(testPool+1, Pool{WriteLimit: 90})
		for d := range uint32(3) {
			r.SetDisk(d+1, 1000)
		}
		r.MemberFill(3, 920)
	})
	p1 := c.write(pg0, chunkIn(pg0, 4, 0), 0, []byte{1})
	pg := core.PGID{Pool: testPool + 1}
	if st := c.status(c.write(pg, chunkIn(pg, 4, 0), 0, []byte{1})); st != StatusNoSpace {
		t.Fatalf("P2 at 92%% of a 90%% limit: %v", st)
	}
	if st := c.status(p1); st != StatusOK {
		t.Fatalf("P1 at 92%% of a 95%% limit: %v", st)
	}
}
