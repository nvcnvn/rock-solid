package node

import (
	"testing"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/meta"
	"github.com/nvcnvn/rock-solid/internal/ops"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// writingDevice is a device whose replica, disk 5, is the active primary of PG 1.0 and knows
// session 5.
func writingDevice(tb testing.TB) *devRun {
	d := newDevRun(tb, sim.NewDisk(devBlocks))
	d.start(1)
	d.settle()
	d.out.Reset()
	d.d.Hand(&d.ctx, &Hand{Device: 7, Table: chunkstore.SessionTable{Version: 2, Next: 6, Live: []uint64{5}}}, &d.out)
	d.d.Table(&d.ctx, &chunkstore.Table{Epoch: 2, Pools: []chunkstore.PoolTable{{ID: 1, Replicas: 1, MinLive: 1,
		PGs: []chunkstore.PGEntry{{Members: []chunkstore.Member{{Disk: 5}}, Interval: 1, Activated: 1}}}}}, &d.out)
	d.d.Disks(&d.ctx, []meta.DiskAddr{{Disk: 5, Log: d.opened.Log, At: core.NodeID{Node: 4, Loop: 3}}}, nil, &d.out)
	d.take()
	d.settle()
	d.ctx.Now, d.ctx.Raw = d.ctx.Now+60_000_000, d.ctx.Raw+60_000_000
	d.out.Reset()
	d.d.OnEvent(&d.ctx, &core.Event{Kind: core.EvTimer, Timer: chunkstore.PeerTimer}, &d.out)
	d.take()
	d.settle()
	return d
}

// write has the device take a one-block write of session 5, settles it and reports whether it was acknowledged.
func (r *devRun) write(id uint64) bool {
	u := chunkstore.Update{Kind: chunkstore.KindWrite, ID: core.RequestID(id), PG: chunkstore.PGFields{Epoch: 2, PG: core.PGID{Pool: 1}, Interval: 1},
		Session: 5, Incarnation: 1, Chunk: core.ChunkID{Volume: 1, Inode: 7}, Len: core.BlockSize}
	ev := core.Event{Kind: core.EvNetRecv, Peer: client}
	ev.Msg.Len = uint16(len(chunkstore.AppendUpdate(ev.Msg.Body[:0], &u)))
	buf, err := r.ctx.Pool.AllocZeroed(core.BlockSize)
	if err != nil {
		r.t.Fatal(err)
	}
	ev.Msg.Buf = buf
	r.out.Reset()
	r.d.OnEvent(&r.ctx, &ev, &r.out)
	r.take()
	r.settle()
	rps := r.replies()
	for _, a := range r.sent {
		if a.Msg.Buf != core.NoBuf {
			r.ctx.Pool.Free(a.Msg.Buf)
		}
	}
	r.sent = r.sent[:0]
	return len(rps) == 1 && rps[0].Status == chunkstore.StatusOK
}

func TestADeviceReportsItsDiskMembersAndReplicas(t *testing.T) {
	d := writingDevice(t)
	if !d.write(1) {
		t.Fatal("the write was not acknowledged")
	}
	var f ops.Facts
	d.d.Facts(&d.ctx, &f)
	k := f.Disks[0]
	if len(f.Disks) != 1 || k.Loop != 3 || k.Device != 7 || k.State != "open" || k.Disk != 5 || k.Log != d.opened.Log || k.Blocks != devBlocks || k.Epoch != 2 || k.Sessions != 2 || k.Run == 0 {
		t.Fatalf("disk %+v", f.Disks)
	}
	if want := (ops.Member{Disk: 5, Pool: 1, PG: 0, Interval: 1, Role: "member"}); len(f.Members) != 1 || f.Members[0] != want {
		t.Fatalf("members %+v before the primary's heartbeat named its lead", f.Members)
	}
}

// A facts request copies the device's part on its loop between writes; the copy reuses the part's
// arrays, so the write path allocates nothing more while facts are requested.
func BenchmarkWriteWhileFactsRequested(b *testing.B) {
	d := writingDevice(b)
	var f ops.Facts
	fill := func() {
		f.Reset()
		d.d.Facts(&d.ctx, &f)
	}
	fill()
	if n := testing.AllocsPerRun(100, fill); n != 0 {
		b.Fatalf("a device's facts allocate %v times per copy", n)
	}
	id := uint64(1)
	b.ReportAllocs()
	for b.Loop() {
		id++
		if !d.write(id) {
			b.Fatalf("write %d was not acknowledged", id)
		}
		fill()
	}
}
