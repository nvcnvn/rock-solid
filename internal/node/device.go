package node

import (
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
// another log ID, and the control-plane group's ID.
type DeviceConfig struct {
	Device uint64
	Blocks uint64
	Class  string
	Loop   uint32
	Store  chunkstore.Config
	Seed   uint64
	Group  uint64
}

// DeviceOutput is what a call into a Device asks of its loop and of the node's machine: actions;
// once its start ends, the disk Opened for Node.Opened, or Failed for Node.Failed; and from its
// replica, a session ask for Node.Ask, a table fetch for Node.FetchTable, and the session version
// and placement epoch it holds, for Node.Held and Node.HeldEpoch.
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
	opened    meta.OpenedDisk
	sess      chunkstore.SessionTable
	table     chunkstore.Table
	auth      core.NodeID
	hasAuth   bool
	sout      chunkstore.Output
	rout      chunkstore.ReplicaOutput
	rp        chunkstore.Report
	frame     []byte
}

// NewDevice starts recovering the device's store.
func NewDevice(ctx *core.Context, cfg DeviceConfig, out *DeviceOutput) *Device {
	d := &Device{cfg: cfg, rng: core.NewRand(cfg.Seed), start: lease.Now(ctx)}
	st, err := chunkstore.Open(ctx, cfg.Store, cfg.Blocks, &d.sout)
	d.st = st
	d.settle(ctx, err, out)
	return d
}

// Replica is the device's replica, nil until the membership names its log.
func (d *Device) Replica() *chunkstore.Replica { return d.rep }

// Store is the device's store, nil once its start failed.
func (d *Device) Store() *chunkstore.Store { return d.st }

// OnEvent takes a disk completion, a timer, a message or a lost peer for the device's store or
// replica; a message before the replica runs is dropped.
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
	d.phase, d.st, out.Failed = failed, nil, true
}

func (d *Device) flush(out *DeviceOutput) {
	out.Actions = append(out.Actions, d.sout.Actions...)
	d.sout.Reset()
}

// Disks takes the committed membership: the replica starts as the disk named for the device's
// log, once the store serves, and learns the loop of every disk.
func (d *Device) Disks(ctx *core.Context, ds []meta.DiskAddr, out *DeviceOutput) {
	if d.phase != opened {
		return
	}
	if d.rep == nil {
		i := slices.IndexFunc(ds, func(a meta.DiskAddr) bool { return a.Log == d.log })
		if i < 0 {
			return
		}
		d.rep = chunkstore.NewReplica(chunkstore.ReplicaConfig{Disk: ds[i].Disk, Seed: d.rng.Uint64(), Start: d.start, Log: d.log, Restarted: d.restarted}, d.st)
		d.addrs(ds)
		if d.hasAuth {
			d.rep.SetAuthority(ctx, d.auth, &d.rout)
		}
		if d.sess.Version != 0 || d.sess.Next != 0 {
			d.rep.SetSessions(ctx, d.sess, 0, &d.rout)
		}
		if d.table.Epoch != 0 {
			d.rep.SetTable(ctx, d.table, &d.rout)
		}
	} else {
		d.addrs(ds)
	}
	d.take(ctx, out)
}

func (d *Device) addrs(ds []meta.DiskAddr) {
	for _, a := range ds {
		d.rep.SetAddr(a.Disk, a.At)
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
// replica fetches the whole table when the next delta does not follow its own.
func (d *Device) Delta(ctx *core.Context, dl *chunkstore.Delta, out *DeviceOutput) {
	if d.rep != nil {
		d.rep.ApplyDelta(ctx, dl, &d.rout)
		d.take(ctx, out)
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
	out.Held, out.Epoch = d.st.Sessions().Version, d.rep.Table().Epoch
	d.rout.Reset()
}
