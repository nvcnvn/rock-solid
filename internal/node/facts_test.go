package node

import (
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/meta"
	"github.com/nvcnvn/rock-solid/internal/ops"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// writingDevice is a device whose replica, disk 5, is the active primary of PG 1.0, a member of
// PG 1.1 behind disk 6 and knows session 5.
func writingDevice(tb testing.TB) *devRun {
	d := newDevRun(tb, sim.NewDisk(devBlocks))
	d.start(1)
	d.settle()
	d.out.Reset()
	d.d.Hand(&d.ctx, &Hand{Device: 7, Table: chunkstore.SessionTable{Version: 2, Next: 6, Live: []uint64{5}}}, &d.out)
	d.d.Table(&d.ctx, &chunkstore.Table{Epoch: 2, Pools: []chunkstore.PoolTable{{ID: 1, Replicas: 1, MinLive: 1,
		PGs: []chunkstore.PGEntry{{Members: []chunkstore.Member{{Disk: 5}}, Interval: 1, Activated: 1},
			{Members: []chunkstore.Member{{Disk: 6}, {Disk: 5}}, Interval: 1, Activated: 1}}}}}, &d.out)
	d.d.Disks(&d.ctx, []meta.DiskAddr{{Disk: 5, Log: d.opened.Log, At: core.NodeID{Node: 4, Loop: 3}}, {Disk: 6, Log: 66, At: core.NodeID{Node: 9, Loop: 1}}}, nil, &d.out)
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
	r.onEvent(&ev)
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
	want := []ops.Member{{Disk: 5, Pool: 1, PG: 0, Interval: 1, Role: "primary", Active: true}, {Disk: 5, Pool: 1, PG: 1, Interval: 1, Role: "member"}}
	if !slices.Equal(f.Members, want) {
		t.Fatalf("members %+v, before the replica's first heartbeat", f.Members)
	}
}

// factsRun is a writing device and a copy of its facts that writes and fill take turns at.
type factsRun struct {
	*devRun
	f  ops.Facts
	id uint64
}

func newFactsRun(tb testing.TB) *factsRun {
	r := &factsRun{devRun: writingDevice(tb), id: 1}
	r.fill()
	if len(r.f.Members) != 2 || r.f.Members[0].Role != "primary" {
		tb.Fatalf("the device's members %+v miss its lead", r.f.Members)
	}
	return r
}

func (r *factsRun) fill() {
	r.f.Reset()
	r.d.Facts(&r.ctx, &r.f)
}

// step has the device take a write, and copy its facts if facts.
func (r *factsRun) step(facts bool) {
	r.id++
	if !r.write(r.id) {
		r.t.Fatalf("write %d was not acknowledged", r.id)
	}
	if facts {
		r.counted(r.fill)
	}
}

// A facts request copies the device's part on its loop between writes, reusing the part's arrays:
// the copy allocates nothing, and writes allocate as often with facts requested as without. The
// allocs/op and B/op reported are the device's own, the simulated disk's and the harness's left
// out; ns/op includes the counting's stops of the world.
func BenchmarkWriteWhileFactsRequested(b *testing.B) {
	var allocs [2]uint64
	for i, facts := range []bool{false, true} {
		r := newFactsRun(b)
		if n := testing.AllocsPerRun(100, r.fill); n != 0 {
			b.Fatalf("a device's facts allocate %v times per copy", n)
		}
		r.counting = true
		for range 300 {
			r.step(facts)
		}
		allocs[i] = r.allocs
	}
	if allocs[0] != allocs[1] {
		b.Fatalf("300 writes allocate %d times with facts requested, %d without", allocs[1], allocs[0])
	}
	for _, facts := range []bool{false, true} {
		b.Run(map[bool]string{false: "baseline", true: "facts"}[facts], func(b *testing.B) {
			r := newFactsRun(b)
			r.counting = true
			b.ReportAllocs()
			for b.Loop() {
				r.step(facts)
			}
			b.ReportMetric(float64(r.allocs)/float64(b.N), "allocs/op")
			b.ReportMetric(float64(r.bytes)/float64(b.N), "B/op")
		})
	}
}
