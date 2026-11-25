package node

import (
	"fmt"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/lease"
	"github.com/nvcnvn/rock-solid/internal/meta"
)

// DeviceConfig is one device of a node: its stable identity, its size in blocks, its class, the
// loop that owns it, its store's settings, the seed of its source, which the daemon takes from
// the operating system at each start, so a format a crash cut short is formatted again under
// another log ID, the control-plane group's ID, the voter its replica reports to until
// Authority names another, and the error opening the device, if any, which leaves it failed.
type DeviceConfig struct {
	Device uint64
	Blocks uint64
	Class  string
	Loop   uint32
	Store  chunkstore.Config
	Seed   uint64
	Group  uint64
	Ctl    core.NodeID
	Err    error
}

// DeviceOutput is what a call into a Device asks of its loop and of the node's machine: actions;
// once its start ends, the disk Opened for Node.Opened, or Failed for Node.Failed; and from its
// replica, a session ask for Node.Ask, a table fetch for Node.FetchTable, and the session version
// and placement epoch it holds when they changed, for Node.Held and Node.HeldEpoch.
type DeviceOutput struct {
	Actions []core.Action
	Opened  *meta.OpenedDisk
	Failed  bool
	Ask     uint64
	Fetch   bool
	Held    uint64
	Epoch   uint64
}

// Reset empties the output for reuse.
func (o *DeviceOutput) Reset() { *o = DeviceOutput{Actions: o.Actions[:0]} }

// Clone is o without its actions, for the node's loop; false when it holds nothing for the node.
func (o *DeviceOutput) Clone() (DeviceOutput, bool) {
	c := *o
	c.Actions = nil
	if o.Opened != nil {
		d := *o.Opened
		c.Opened = &d
	}
	return c, c.Opened != nil || c.Failed || c.Ask != 0 || c.Fetch || c.Held != 0 || c.Epoch != 0
}

type devicePhase uint8

const (
	recovering devicePhase = iota
	formatting
	opened
	failed
)

// Device is one device's machine on the loop that owns it. It recovers the device's store, and
// formats it or leaves it failed as chunkstore.StartDevice says; once the store serves, it runs
// the replica as the disk the committed membership names for its log, which the replica's
// placement table names it by. Until then it keeps the newest session and placement tables and
// the authority, which the replica starts with.
type Device struct {
	cfg   DeviceConfig
	rng   *core.Rand
	start lease.Time
	phase devicePhase
	st    *chunkstore.Store
	rep   *chunkstore.Replica
	log   uint64
	// restarted is set when the store was opened rather than formatted.
	restarted bool
	// held and epoch are the session version and placement epoch last output.
	held, epoch uint64
	// dropped is the newest epoch of a delta dropped before the replica ran.
	dropped uint64
	opened  meta.OpenedDisk
	sess    chunkstore.SessionTable
	table   chunkstore.Table
	auth    core.NodeID
	hasAuth bool
	sout    chunkstore.Output
	rout    chunkstore.ReplicaOutput
	rp      chunkstore.Report
	frame   []byte
}

// NewDevice starts recovering the device's store, or fails it at once on cfg.Err.
func NewDevice(ctx *core.Context, cfg DeviceConfig, out *DeviceOutput) *Device {
	d := &Device{cfg: cfg, rng: core.NewRand(cfg.Seed), start: lease.Now(ctx), auth: cfg.Ctl, hasAuth: cfg.Ctl != core.NodeID{}}
	err := cfg.Err
	if err == nil {
		d.st, err = chunkstore.Open(ctx, cfg.Store, cfg.Blocks, &d.sout)
	}
	d.settle(ctx, err, out)
	return d
}

// Replica is the device's replica, nil until the membership names its log.
func (d *Device) Replica() *chunkstore.Replica { return d.rep }

// Store is the device's store, nil once its start failed.
func (d *Device) Store() *chunkstore.Store { return d.st }

// OnEvent takes a disk completion, a timer, a message or a lost peer for the device's store or
// replica; a message before the replica runs is dropped, and the replica drops a Raft group's.
func (d *Device) OnEvent(ctx *core.Context, ev *core.Event, out *DeviceOutput) {
	switch {
	case d.rep != nil:
		d.rep.OnEvent(ctx, ev, &d.rout)
		d.take(ctx, out)
		return
	case ev.Kind == core.EvNetRecv:
		free(ctx, &ev.Msg)
		return
	case d.st != nil && (ev.Kind == core.EvDiskDone || ev.Kind == core.EvTimer):
		d.st.OnEvent(ctx, ev, &d.sout)
	}
	d.settle(ctx, nil, out)
}

// OnLog takes an event of the store's log, or the nil that follows each disk completion, from the
// loop that owns the log.
func (d *Device) OnLog(ctx *core.Context, ev *disklog.Event, out *DeviceOutput) {
	switch {
	case d.rep != nil:
		d.rep.OnLog(ctx, ev, &d.rout)
		d.take(ctx, out)
		return
	case d.st != nil:
		d.st.OnLog(ctx, ev, &d.sout)
	}
	d.settle(ctx, nil, out)
}

// settle moves the start on once recovery or the format ends: an open store serves, StartDevice
// says to format or fail one that did not recover, and a format that does not serve has failed.
func (d *Device) settle(ctx *core.Context, err error, out *DeviceOutput) {
	if err == nil && d.st != nil {
		err = d.st.Err()
		if err == nil && !d.st.Serving() {
			d.flush(out)
			return
		}
	}
	switch act, log := chunkstore.StartDevice(d.rng, err); {
	case d.phase == recovering && act == chunkstore.StartFormat:
		d.phase = formatting
		g, gerr := disklog.GeometryFor(d.cfg.Blocks * core.BlockSize)
		if gerr == nil {
			d.closeStore()
			d.st, gerr = chunkstore.Format(ctx, d.cfg.Store, disklog.Superblock{LogID: log, Geometry: g}, &d.sout)
		}
		if gerr != nil {
			d.fail(out)
		}
	case (d.phase == recovering || d.phase == formatting) && act == chunkstore.StartOpened:
		sb := d.st.Log().Superblock()
		d.log = sb.LogID
		d.opened = meta.OpenedDisk{Device: d.cfg.Device, Log: d.log, Blocks: d.cfg.Blocks,
			Reserve: uint64(d.st.Log().Reserve()) * uint64(sb.SegmentBlocks), Class: d.cfg.Class, Loop: d.cfg.Loop}
		out.Opened = &d.opened
		d.restarted = d.phase == recovering
		d.phase = opened
	case d.phase == recovering || d.phase == formatting:
		d.fail(out)
	}
	d.flush(out)
}

func (d *Device) fail(out *DeviceOutput) {
	d.closeStore()
	d.phase, out.Failed = failed, true
}

// closeStore gives the store's page slabs back and drops it.
func (d *Device) closeStore() {
	if d.st != nil {
		d.st.Close()
		d.st = nil
	}
}

func (d *Device) flush(out *DeviceOutput) {
	if len(d.sout.Mail) > 0 {
		panic(fmt.Sprintf("node: the store mailed %+v, and a device hosts no shard on the round driver", d.sout.Mail[0]))
	}
	out.Actions = append(out.Actions, d.sout.Actions...)
	d.sout.Reset()
}

// Disks takes the committed membership and the pools' grants outstanding, gs: the replica starts
// as the disk named for the device's log, once the store serves, and learns the loop, size, log
// reserve and fill of every disk and the grants. One that starts on a table older than a delta
// dropped before it ran fetches the whole table.
func (d *Device) Disks(ctx *core.Context, ds []meta.DiskAddr, gs []meta.PoolGrant, out *DeviceOutput) {
	if d.phase != opened {
		return
	}
	if d.rep == nil {
		i := slices.IndexFunc(ds, func(a meta.DiskAddr) bool { return a.Log == d.log })
		if i < 0 {
			return
		}
		d.rep = chunkstore.NewReplica(chunkstore.ReplicaConfig{Disk: ds[i].Disk, Seed: d.rng.Uint64(), Start: d.start, Log: d.log, Restarted: d.restarted}, d.st)
		TakeMembership(ctx, d.rep, ds, gs)
		if d.hasAuth {
			d.rep.SetAuthority(ctx, d.auth, &d.rout)
		}
		if d.sess.Version != 0 || d.sess.Next != 0 {
			d.rep.SetSessions(ctx, d.sess, 0, &d.rout)
		}
		if d.table.Epoch != 0 {
			d.rep.SetTable(ctx, d.table, &d.rout)
		}
		out.Fetch = out.Fetch || d.table.Epoch < d.dropped
	} else {
		TakeMembership(ctx, d.rep, ds, gs)
	}
	d.take(ctx, out)
}

// TakeMembership hands rep each disk's loop, size, log reserve and fill, and the pools' grants, from the leader's answer to a node's report.
func TakeMembership(ctx *core.Context, rep *chunkstore.Replica, ds []meta.DiskAddr, gs []meta.PoolGrant) {
	for _, a := range ds {
		rep.SetAddr(a.Disk, a.At)
		rep.SetDisk(a.Disk, a.Blocks, a.Reserve)
		if a.Filled {
			rep.MemberFill(ctx, a.Disk, a.Live)
		}
	}
	for _, g := range gs {
		rep.SetGrants(g.Pool, g.Grant)
	}
}

// Hand hands the replica the session table h carries, answering its ask h.Answers.
func (d *Device) Hand(ctx *core.Context, h *Hand, out *DeviceOutput) {
	if h.Table.Version > d.sess.Version || h.Table.Version == d.sess.Version && h.Table.Next > d.sess.Next {
		d.sess = h.Table
	}
	if d.rep != nil {
		d.rep.SetSessions(ctx, h.Table, h.Answers, &d.rout)
		d.take(ctx, out)
	}
}

// Table hands the replica a whole placement table.
func (d *Device) Table(ctx *core.Context, t *chunkstore.Table, out *DeviceOutput) {
	if t.Epoch > d.table.Epoch {
		d.table = *t
	}
	if d.rep != nil {
		d.rep.SetTable(ctx, *t, &d.rout)
		d.take(ctx, out)
	}
}

// Delta hands the replica a placement delta; before the replica runs it is dropped, and the
// replica fetches the whole table when it starts on an older one or the next delta does not
// follow its own.
func (d *Device) Delta(ctx *core.Context, dl *chunkstore.Delta, out *DeviceOutput) {
	if d.rep == nil {
		d.dropped = max(d.dropped, dl.Epoch)
	} else {
		d.rep.ApplyDelta(ctx, dl, &d.rout)
		d.take(ctx, out)
	}
}

// Take hands the device what the node's machine output, o, holds for it: the voter it moved to,
// the membership, the session tables for this device, and a placement table or delta.
func (d *Device) Take(ctx *core.Context, o *Output, out *DeviceOutput) {
	if o.Moved {
		d.Authority(ctx, o.Ctl, out)
	}
	if o.Disks != nil {
		d.Disks(ctx, o.Disks, o.Grants, out)
	}
	for i := range o.Hands {
		if o.Hands[i].Device == d.cfg.Device {
			d.Hand(ctx, &o.Hands[i], out)
		}
	}
	if o.Table != nil {
		d.Table(ctx, o.Table, out)
	}
	if o.Delta != nil {
		d.Delta(ctx, o.Delta, out)
	}
}

// Authority points the replica's reports at the control-plane voter at.
func (d *Device) Authority(ctx *core.Context, at core.NodeID, out *DeviceOutput) {
	d.auth, d.hasAuth = at, true
	if d.rep != nil {
		d.rep.SetAuthority(ctx, at, &d.rout)
		d.take(ctx, out)
	}
}

// take gathers the replica's output and the versions it holds, and sends the authority the
// replica's asks for new intervals, and its activations and recovered intervals in a report; one
// lost, or made before the device has an authority, is made again every RetryAfter until a table
// answers it.
func (d *Device) take(ctx *core.Context, out *DeviceOutput) {
	out.Actions = append(out.Actions, d.rout.Actions...)
	if d.hasAuth && len(d.rout.Asks) != 0 {
		a := core.Action{Kind: core.ActNetSend, Peer: d.auth}
		if meta.EncodeAsks(ctx.Pool, d.cfg.Group, d.rout.Asks, &a.Msg) == nil {
			out.Actions = append(out.Actions, a)
		}
	}
	if d.hasAuth && len(d.rout.Activations)+len(d.rout.Recovered) != 0 {
		d.rp.ID++
		d.rp.Activations, d.rp.Recovered = d.rout.Activations, d.rout.Recovered
		d.frame = chunkstore.AppendReport(d.frame[:0], &d.rp)
		a := core.Action{Kind: core.ActNetSend, Peer: d.auth}
		if chunkstore.Attach(ctx.Pool, &a.Msg, d.frame, core.NoBuf) == nil {
			out.Actions = append(out.Actions, a)
		}
	}
	out.Ask = max(out.Ask, d.rout.FetchSessions)
	out.Fetch = out.Fetch || d.rout.Fetch != 0 || d.rout.Whole
	if v := d.st.Sessions().Version; v != d.held {
		out.Held, d.held = v, v
	}
	if e := d.rep.Table().Epoch; e != d.epoch {
		out.Epoch, d.epoch = e, e
	}
	d.rout.Reset()
}
