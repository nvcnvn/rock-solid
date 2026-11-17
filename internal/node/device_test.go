package node

import (
	"bytes"
	"errors"
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

// A device the process could not open, as a drive gone from the bus, is reported failed and the
// node reports it beside the device that opened, so the others still serve.
func TestADeviceThatCannotBeOpenedIsReportedFailed(t *testing.T) {
	r := newDevRun(t, nil)
	r.d = NewDevice(&r.ctx, DeviceConfig{Device: 7, Class: "ssd", Loop: 3, Store: storetest.Config(), Seed: 1, Group: group, Ctl: ctl, Err: errors.New("no such device")}, &r.out)
	r.take()
	if !r.failed || r.opened != nil || r.d.Store() != nil || len(r.work) != 0 {
		t.Fatalf("failed %v, opened %+v, %d disk actions", r.failed, r.opened, len(r.work))
	}
	n := newRun(t, 1, 10)
	n.out.Reset()
	n.n.Take(&n.ctx, 7, &r.out, &n.out)
	if nr := n.report(); !slices.Equal(nr.Failed, []uint64{7}) || len(nr.Opened) != 1 || nr.Opened[0].Device != 10 {
		t.Fatalf("the node reported opened %+v, failed %v", nr.Opened, nr.Failed)
	}
}

// The replica starts only once the membership names the device's log, as that disk, with the
// session table, placement table and authority handed to the device before it ran, and the loop of
// every disk the membership names: as the primary of a PG with a member on disk 4 it peers with
// disk 4's loop. A message before then is dropped.
func TestTheReplicaStartsAsTheDiskTheMembershipNames(t *testing.T) {
	r := newDevRun(t, sim.NewDisk(devBlocks))
	r.start(1)
	r.settle()
	if r.opened == nil {
		t.Fatal("a blank device never opened")
	}
	r.out.Reset()
	r.d.Hand(&r.ctx, &Hand{Device: 7, Table: chunkstore.SessionTable{Version: 3, Next: 4}}, &r.out)
	pg := chunkstore.PGEntry{Members: []chunkstore.Member{{Disk: 5}, {Disk: 4}}, Interval: 1}
	r.d.Table(&r.ctx, &chunkstore.Table{Epoch: 2, Pools: []chunkstore.PoolTable{{ID: 1, Replicas: 2, MinLive: 1, PGs: []chunkstore.PGEntry{pg}}}}, &r.out)
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
	member := core.NodeID{Node: 6, Loop: 2}
	r.d.Disks(&r.ctx, []meta.DiskAddr{{Disk: 4, Log: r.opened.Log + 1, At: member}, {Disk: 5, Log: r.opened.Log, At: core.NodeID{Node: 4, Loop: 3}}}, &r.out)
	if r.d.Replica() == nil || r.out.Held != 3 || r.out.Epoch != 2 {
		t.Fatalf("the replica started holding sessions %d, epoch %d", r.out.Held, r.out.Epoch)
	}
	reported, peered := false, false
	for _, a := range r.out.Actions {
		reported = reported || a.Kind == core.ActNetSend && a.Peer == ctl
		peered = peered || a.Kind == core.ActNetSend && a.Peer == member
	}
	if !reported || !peered {
		t.Fatalf("the replica reported to its authority: %v; peered with disk 4's loop: %v", reported, peered)
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

// The node's output reaches a device: the voter moved to, the membership naming its log, the
// session table for this device and not another's, and a placement table; a Raft group's message,
// which is the node loop's, is dropped.
func TestTheNodesOutputFeedsADevice(t *testing.T) {
	r := newDevRun(t, sim.NewDisk(devBlocks))
	r.start(1)
	r.settle()
	pg := chunkstore.PGEntry{Members: []chunkstore.Member{{Disk: 5}}, Interval: 1}
	o := Output{
		Moved: true, Ctl: ctl,
		Disks: []meta.DiskAddr{{Disk: 5, Log: r.opened.Log, At: core.NodeID{Node: 4, Loop: 3}}},
		Hands: []Hand{{Device: 8, Table: chunkstore.SessionTable{Version: 6, Next: 7}}, {Device: 7, Table: chunkstore.SessionTable{Version: 3, Next: 4}}},
		Table: &chunkstore.Table{Epoch: 2, Pools: []chunkstore.PoolTable{{ID: 1, Replicas: 1, MinLive: 1, PGs: []chunkstore.PGEntry{pg}}}},
	}
	r.out.Reset()
	r.d.Take(&r.ctx, &o, &r.out)
	if r.d.Replica() == nil || r.out.Held != 3 || r.out.Epoch != 2 {
		t.Fatalf("the replica holds sessions %d, epoch %d", r.out.Held, r.out.Epoch)
	}
	if !slices.ContainsFunc(r.out.Actions, func(a core.Action) bool { return a.Kind == core.ActNetSend && a.Peer == ctl }) {
		t.Fatal("the replica reported to no authority")
	}
	r.out.Reset()
	r.d.Take(&r.ctx, &Output{Table: o.Table}, &r.out)
	if r.out.Held != 0 || r.out.Epoch != 0 {
		t.Fatalf("an unchanged replica output sessions %d, epoch %d", r.out.Held, r.out.Epoch)
	}
	live := r.ctx.Pool.(*sim.Pool).Live()
	ev := core.Event{Kind: core.EvNetRecv, Peer: ctl}
	if err := meta.EncodeTable(r.ctx.Pool, group, o.Table, &ev.Msg); err != nil {
		t.Fatal(err)
	}
	r.out.Reset()
	r.d.OnEvent(&r.ctx, &ev, &r.out)
	if len(r.out.Actions) != 0 || r.ctx.Pool.(*sim.Pool).Live() != live {
		t.Fatalf("a group message gave %d actions", len(r.out.Actions))
	}
}

var client = core.NodeID{Node: 3}

// toDevice hands the device the node's last output, cloned as the process mails it between loops,
// and runs the disk actions that follow.
func toDevice(n *run, d *devRun) {
	o, ok := n.out.Clone()
	if !ok {
		return
	}
	d.out.Reset()
	d.d.Take(&d.ctx, &o, &d.out)
	d.take()
	d.settle()
}

// replies are the client's answers among what the device sent.
func (r *devRun) replies() []chunkstore.Response {
	var rps []chunkstore.Response
	for _, a := range r.sent {
		frame, data := chunkstore.Frame(r.ctx.Pool, &a.Msg)
		var rp chunkstore.Response
		if a.Peer == client && chunkstore.DecodeResponse(frame, data, &rp) == nil {
			rps = append(rps, rp)
		}
	}
	return rps
}

// A node and a device wired as the process wires them: the node's output cloned into Device.Take,
// the device's into Node.Take. A write of a session newer than the device's table waits and the
// node fetches the table; a table pushed meanwhile that does not know the session leaves the write
// waiting; the fetch's answer acknowledges it when it knows the session, and refuses it when not.
func TestOnlyTheAnswerToItsFetchSettlesAWaitingWrite(t *testing.T) {
	for _, knows := range []bool{true, false} {
		d := newDevRun(t, sim.NewDisk(devBlocks))
		d.start(1)
		d.settle()
		n := newRun(t, 1)
		n.out.Reset()
		n.n.Take(&n.ctx, 7, &DeviceOutput{Opened: d.opened}, &n.out)
		ds := []meta.DiskAddr{{Disk: 5, Log: d.opened.Log, At: core.NodeID{Node: 4, Loop: 3}}}
		n.from(ctl, func(pool core.BufPool, m *core.Msg) error { return meta.EncodeDisks(pool, group, ds, m) })
		toDevice(n, d)
		tbl := chunkstore.Table{Epoch: 2, Pools: []chunkstore.PoolTable{{ID: 1, Replicas: 1, MinLive: 1,
			PGs: []chunkstore.PGEntry{{Members: []chunkstore.Member{{Disk: 5}}, Interval: 1, Activated: 1}}}}}
		n.from(ctl, func(pool core.BufPool, m *core.Msg) error { return meta.EncodeTable(pool, group, &tbl, m) })
		toDevice(n, d)
		// The replica peers, and so activates PG 0, only once its start's lease wait is over.
		d.ctx.Now, d.ctx.Raw = d.ctx.Now+60_000_000, d.ctx.Raw+60_000_000
		d.out.Reset()
		d.d.OnEvent(&d.ctx, &core.Event{Kind: core.EvTimer, Timer: chunkstore.PeerTimer}, &d.out)
		d.take()
		d.settle()

		u := chunkstore.Update{Kind: chunkstore.KindWrite, ID: 1, PG: chunkstore.PGFields{Epoch: 2, PG: core.PGID{Pool: 1}, Interval: 1},
			Session: 5, Incarnation: 1, Chunk: core.ChunkID{Volume: 1, Inode: 7}, Len: core.BlockSize}
		ev := core.Event{Kind: core.EvNetRecv, Peer: client}
		ev.Msg.Len = uint16(copy(ev.Msg.Body[:], chunkstore.AppendUpdate(nil, &u)))
		buf, err := d.ctx.Pool.AllocZeroed(core.BlockSize)
		if err != nil {
			t.Fatal(err)
		}
		ev.Msg.Buf = buf
		d.out.Reset()
		d.d.OnEvent(&d.ctx, &ev, &d.out)
		d.take()
		o, _ := d.out.Clone()
		n.out.Reset()
		n.n.Take(&n.ctx, 7, &o, &n.out)
		token := n.fetch()
		d.settle()
		if rps := d.replies(); len(rps) != 0 || token == 0 {
			t.Fatalf("a write of session 5 before any table knew it: answered %+v, fetch %d", rps, token)
		}

		n.table(0, chunkstore.SessionTable{Version: 1, Next: 2, Live: []uint64{1}})
		toDevice(n, d)
		if rps := d.replies(); len(rps) != 0 {
			t.Fatalf("a pushed table answered the waiting write: %+v", rps)
		}

		answer, want := chunkstore.SessionTable{Version: 2, Next: 6, Live: []uint64{1, 5}}, chunkstore.StatusOK
		if !knows {
			answer, want = chunkstore.SessionTable{Version: 2, Next: 3, Live: []uint64{1, 2}}, chunkstore.StatusRefused
		}
		n.table(token, answer)
		toDevice(n, d)
		if rps := d.replies(); len(rps) != 1 || rps[0].ID != u.ID || rps[0].Status != want {
			t.Fatalf("the fetch's answer %+v gave %+v", answer, rps)
		}
	}
}

// A device started with a voter reports to it before any move.
func TestADeviceReportsToTheVoterItStartsWith(t *testing.T) {
	r := newDevRun(t, sim.NewDisk(devBlocks))
	cfg := storetest.Config()
	cfg.Log.NoReserve = false
	r.d = NewDevice(&r.ctx, DeviceConfig{Device: 7, Blocks: devBlocks, Class: "ssd", Loop: 3, Store: cfg, Seed: 1, Group: group, Ctl: ctl}, &r.out)
	r.take()
	r.settle()
	r.out.Reset()
	r.d.Disks(&r.ctx, []meta.DiskAddr{{Disk: 5, Log: r.opened.Log}}, &r.out)
	if !slices.ContainsFunc(r.out.Actions, func(a core.Action) bool { return a.Kind == core.ActNetSend && a.Peer == ctl }) {
		t.Fatal("the replica reported to no authority")
	}
}

// A node whose start's table fetch gets lost fetches again on its next heartbeat: the replica,
// holding no table, refuses a write, and once the retry's answer lands the same write is
// acknowledged.
func TestALostTableFetchIsMadeUpBeforeAWriteIsAcknowledged(t *testing.T) {
	d := newDevRun(t, sim.NewDisk(devBlocks))
	d.start(1)
	d.settle()
	n := newRun(t, 1)
	n.out.Reset()
	n.n.Take(&n.ctx, 7, &DeviceOutput{Opened: d.opened}, &n.out)
	ds := []meta.DiskAddr{{Disk: 5, Log: d.opened.Log, At: core.NodeID{Node: 4, Loop: 3}}}
	n.from(ctl, func(pool core.BufPool, m *core.Msg) error { return meta.EncodeDisks(pool, group, ds, m) })
	toDevice(n, d)
	n.table(0, chunkstore.SessionTable{Version: 1, Next: 6, Live: []uint64{5}})
	toDevice(n, d)
	n.out.Reset()
	n.n.Start(&n.ctx, &n.out)
	if len(n.sends(meta.KindTableFetch)) != 1 {
		t.Fatal("the start sent no table fetch")
	}

	write := func(id core.RequestID) []chunkstore.Response {
		u := chunkstore.Update{Kind: chunkstore.KindWrite, ID: id, PG: chunkstore.PGFields{Epoch: 2, PG: core.PGID{Pool: 1}, Interval: 1},
			Session: 5, Incarnation: 1, Chunk: core.ChunkID{Volume: 1, Inode: 7}, Len: core.BlockSize}
		ev := core.Event{Kind: core.EvNetRecv, Peer: client}
		ev.Msg.Len = uint16(copy(ev.Msg.Body[:], chunkstore.AppendUpdate(nil, &u)))
		buf, err := d.ctx.Pool.AllocZeroed(core.BlockSize)
		if err != nil {
			t.Fatal(err)
		}
		ev.Msg.Buf = buf
		d.sent = nil
		d.out.Reset()
		d.d.OnEvent(&d.ctx, &ev, &d.out)
		d.take()
		o, _ := d.out.Clone()
		n.out.Reset()
		n.n.Take(&n.ctx, 7, &o, &n.out)
		d.settle()
		return d.replies()
	}
	if rps := write(1); len(rps) != 1 || rps[0].Status == chunkstore.StatusOK {
		t.Fatalf("a write with no table answered %+v", rps)
	}

	n.ctx.Now += Interval
	n.event(core.Event{Kind: core.EvTimer, Timer: HeartbeatTimer})
	if len(n.sends(meta.KindTableFetch)) != 1 {
		t.Fatal("the heartbeat after a lost table fetch sent none")
	}
	tbl := chunkstore.Table{Epoch: 2, Pools: []chunkstore.PoolTable{{ID: 1, Replicas: 1, MinLive: 1,
		PGs: []chunkstore.PGEntry{{Members: []chunkstore.Member{{Disk: 5}}, Interval: 1, Activated: 1}}}}}
	n.from(ctl, func(pool core.BufPool, m *core.Msg) error { return meta.EncodeTable(pool, group, &tbl, m) })
	toDevice(n, d)
	d.ctx.Now, d.ctx.Raw = d.ctx.Now+60_000_000, d.ctx.Raw+60_000_000
	d.out.Reset()
	d.d.OnEvent(&d.ctx, &core.Event{Kind: core.EvTimer, Timer: chunkstore.PeerTimer}, &d.out)
	d.take()
	d.settle()
	if rps := write(2); len(rps) != 1 || rps[0].Status != chunkstore.StatusOK {
		t.Fatalf("a write after the retry's table answered %+v", rps)
	}
}
