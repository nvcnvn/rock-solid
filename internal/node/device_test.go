package node

import (
	"bytes"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/chunkstore/storetest"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/meta"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// devBlocks is a device of 1 GiB, room for the log's reserve.
const devBlocks = 1 << 18

// devRun drives one Device on a simulated disk as its loop would, running disk actions one at a
// time.
type devRun struct {
	t      *testing.T
	disk   *sim.Disk
	ctx    core.Context
	d      *Device
	out    DeviceOutput
	work   []core.Action
	sent   []core.Action
	opened *meta.OpenedDisk
	failed bool
}

func newDevRun(t *testing.T, disk *sim.Disk) *devRun {
	return &devRun{t: t, disk: disk, ctx: core.Context{Now: 1, Raw: 1, Pool: sim.NewPool(16<<20, 0)}}
}

// start starts the device with its source seeded with seed.
func (r *devRun) start(seed uint64) {
	r.out.Reset()
	cfg := storetest.Config()
	cfg.Log.NoReserve = false
	r.d = NewDevice(&r.ctx, DeviceConfig{Device: 7, Blocks: devBlocks, Class: "ssd", Loop: 3, Store: cfg, Seed: seed, Group: group}, &r.out)
	r.take()
}

func (r *devRun) take() {
	for _, a := range r.out.Actions {
		switch a.Kind {
		case core.ActSetTimer:
		case core.ActNetSend:
			r.sent = append(r.sent, a)
		default:
			r.work = append(r.work, a)
		}
	}
	if r.out.Opened != nil {
		o := *r.out.Opened
		r.opened = &o
	}
	r.failed = r.failed || r.out.Failed
	r.out.Actions = r.out.Actions[:0]
}

// step runs the oldest disk action and hands the device its completion; false once none is left.
func (r *devRun) step() bool {
	if len(r.work) == 0 {
		return false
	}
	a := r.work[0]
	r.work = r.work[1:]
	ev := r.disk.Execute(r.ctx.Pool, &a)
	r.out.Reset()
	r.d.OnEvent(&r.ctx, &ev, &r.out)
	r.take()
	return true
}

func (r *devRun) settle() {
	for r.step() {
	}
}

// Two starts over a device whose format a crash cut short draw different log IDs: the second
// formats it again under a log ID of its own source, or opens the first's log once its format
// landed; the device is reported opened only once its format settles.
func TestTwoStartsOverAFormatCutShortDrawDifferentLogIDs(t *testing.T) {
	_, first := chunkstore.StartDevice(core.NewRand(1), disklog.ErrNotFormatted)
	formattedAgain := 0
	for k := 0; ; k++ {
		disk := sim.NewDisk(devBlocks)
		r := newDevRun(t, disk)
		r.start(1)
		for i := 0; i < k && r.step(); i++ {
		}
		if r.opened != nil {
			if r.opened.Log != first || r.opened.Device != 7 || r.opened.Blocks != devBlocks || r.opened.Class != "ssd" || r.opened.Loop != 3 || r.opened.Reserve == 0 {
				t.Fatalf("opened %+v, log %#x drawn", r.opened, first)
			}
			break
		}
		disk.Crash(func(int, core.LBA) bool { return false })
		again := newDevRun(t, disk)
		again.start(2)
		again.settle()
		switch {
		case again.opened == nil || again.failed:
			t.Fatalf("%d disk actions: the second start opened %+v, failed %v", k, again.opened, again.failed)
		case again.opened.Log != first:
			formattedAgain++
		}
	}
	if formattedAgain == 0 {
		t.Fatal("no crash left the format to be made again")
	}
}

// A device with no valid superblock that is not blank is reported failed and left as it was.
func TestADeviceWithNoValidSuperblockFailsUntouched(t *testing.T) {
	disk := sim.NewDisk(devBlocks)
	g, err := disklog.GeometryFor(devBlocks * core.BlockSize)
	if err != nil {
		t.Fatal(err)
	}
	junk := bytes.Repeat([]byte{0xa5}, core.BlockSize)
	disk.Put(0, junk)
	disk.Put(g.SuperblockCopyLBA(), junk)
	r := newDevRun(t, disk)
	r.start(1)
	r.settle()
	if !r.failed || r.opened != nil || r.d.Store() != nil {
		t.Fatalf("failed %v, opened %+v", r.failed, r.opened)
	}
	if !bytes.Equal(disk.Read(0, 1), junk) || !bytes.Equal(disk.Read(g.SuperblockCopyLBA(), 1), junk) {
		t.Fatal("a failed device was written")
	}
	r.out.Reset()
	r.d.Disks(&r.ctx, []meta.DiskAddr{{Disk: 1, Log: 0}}, &r.out)
	if r.d.Replica() != nil {
		t.Fatal("a failed device runs a replica")
	}
}

// The replica starts only once the membership names the device's log, as that disk, with the
// session table, placement table and authority handed to the device before it ran; a message
// before then is dropped.
func TestTheReplicaStartsAsTheDiskTheMembershipNames(t *testing.T) {
	r := newDevRun(t, sim.NewDisk(devBlocks))
	r.start(1)
	r.settle()
	if r.opened == nil {
		t.Fatal("a blank device never opened")
	}
	r.out.Reset()
	r.d.Hand(&r.ctx, &Hand{Device: 7, Table: chunkstore.SessionTable{Version: 3, Next: 4}}, &r.out)
	r.d.Table(&r.ctx, &chunkstore.Table{Epoch: 2}, &r.out)
	r.d.Authority(&r.ctx, ctl, &r.out)
	ev := core.Event{Kind: core.EvNetRecv, Peer: ctl}
	if err := meta.EncodeTableFetch(r.ctx.Pool, group, &ev.Msg); err != nil {
		t.Fatal(err)
	}
	r.d.OnEvent(&r.ctx, &ev, &r.out)
	r.d.Disks(&r.ctx, []meta.DiskAddr{{Disk: 4, Log: r.opened.Log + 1}}, &r.out)
	if r.d.Replica() != nil || len(r.out.Actions) != 0 || r.out.Held != 0 {
		t.Fatalf("a replica ran before the membership named its log: %+v", r.out)
	}
	r.d.Disks(&r.ctx, []meta.DiskAddr{{Disk: 4, Log: r.opened.Log + 1}, {Disk: 5, Log: r.opened.Log, At: core.NodeID{Node: 4, Loop: 2}}}, &r.out)
	if r.d.Replica() == nil || r.out.Held != 3 || r.out.Epoch != 2 {
		t.Fatalf("the replica started holding sessions %d, epoch %d", r.out.Held, r.out.Epoch)
	}
	reported := false
	for _, a := range r.out.Actions {
		reported = reported || a.Kind == core.ActNetSend && a.Peer == ctl
	}
	if !reported {
		t.Fatal("the replica never reported to its authority")
	}
}

// A device whose log the membership never names, as a lost disk's log reported again is not,
// runs no replica and leaves the node's report: the version the running replica holds is reported.
func TestAnUnnamedDeviceDoesNotPinTheReport(t *testing.T) {
	a := newDevRun(t, sim.NewDisk(devBlocks))
	a.start(1)
	a.settle()
	b := newDevRun(t, sim.NewDisk(devBlocks))
	b.start(2)
	b.settle()
	r := newRun(t, 1)
	r.n.Opened(meta.OpenedDisk{Device: 1, Log: a.opened.Log}, &r.out)
	r.n.Opened(meta.OpenedDisk{Device: 2, Log: b.opened.Log}, &r.out)
	ds := []meta.DiskAddr{{Disk: 1, Log: a.opened.Log}}
	r.from(ctl, func(pool core.BufPool, m *core.Msg) error { return meta.EncodeDisks(pool, group, ds, m) })
	named := slices.Clone(r.out.Disks)
	hs := r.table(0, chunkstore.SessionTable{Version: 5, Next: 9})
	for dev, h := range hs {
		run := a
		if dev == 2 {
			run = b
		}
		run.out.Reset()
		run.d.Hand(&run.ctx, &h, &run.out)
		run.d.Disks(&run.ctx, named, &run.out)
		if run.d.Replica() != nil {
			r.n.Held(dev, run.out.Held)
			r.n.HeldEpoch(dev, run.out.Epoch)
		}
	}
	if b.d.Replica() != nil {
		t.Fatal("an unnamed device runs a replica")
	}
	if nr := r.report(); nr.Sessions != 5 {
		t.Fatalf("the running replica holds 5, the report says %d", nr.Sessions)
	}
}

// A replica restarted over its store asks for a new interval of its PG in its first table, and the
// device sends the ask to its authority.
func TestARestartedReplicasAskGoesToItsAuthority(t *testing.T) {
	disk := sim.NewDisk(devBlocks)
	first := newDevRun(t, disk)
	first.start(1)
	first.settle()
	r := newDevRun(t, disk)
	r.start(2)
	r.settle()
	if r.opened == nil || r.opened.Log != first.opened.Log {
		t.Fatalf("the restart opened %+v", r.opened)
	}
	r.out.Reset()
	r.d.Authority(&r.ctx, ctl, &r.out)
	r.d.Disks(&r.ctx, []meta.DiskAddr{{Disk: 5, Log: r.opened.Log, At: core.NodeID{Node: 4, Loop: 3}}}, &r.out)
	pg := chunkstore.PGEntry{Members: []chunkstore.Member{{Disk: 5}}, Interval: 2, Activated: 2}
	r.d.Table(&r.ctx, &chunkstore.Table{Epoch: 3, Pools: []chunkstore.PoolTable{{ID: 1, Replicas: 1, MinLive: 1, PGs: []chunkstore.PGEntry{pg}}}}, &r.out)
	var asks []chunkstore.PGFields
	for _, a := range r.out.Actions {
		if _, k, _, _ := meta.Peek(&a.Msg); a.Kind == core.ActNetSend && k == meta.KindAsks && a.Peer == ctl {
			got, err := meta.DecodeAsks(r.ctx.Pool, &a.Msg, nil)
			if err != nil {
				t.Fatal(err)
			}
			asks = append(asks, got...)
		}
	}
	if want := (chunkstore.PGFields{Epoch: 3, PG: core.PGID{Pool: 1}, Interval: 2}); !slices.Contains(asks, want) {
		t.Fatalf("asked %+v, want %+v", asks, want)
	}
}
