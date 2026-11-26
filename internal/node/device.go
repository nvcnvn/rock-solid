package node

import (
	"encoding/binary"
	"errors"
	"fmt"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/lease"
	"github.com/nvcnvn/rock-solid/internal/loop"
	"github.com/nvcnvn/rock-solid/internal/meta"
	"github.com/nvcnvn/rock-solid/internal/raftshard"
)

// MetaTimer is the Raft timer of the MetaStore replica a device hosts first, MetaTimer+i that of
// the i-th.
const MetaTimer core.TimerID = 0x6d657461 << 32

// DeviceConfig is one device of a node: its stable identity, its size in blocks, its class, the
// loop that owns it, its store's settings, the seed of its source, which the daemon takes from
// the operating system at each start, so a format a crash cut short is formatted again under
// another log ID, the control-plane group's ID, the voter its replica reports to until
// Authority names another, and the error opening the device, if any, which leaves it failed;
// Host is the loop that owns the device, Rounds its log there, and Metas the settings of the
// MetaStore replicas the disk may hold, one per group: a store opened restores each replica its
// manifest names, under the group, replica ID and owner the disk records, with the rest of the
// settings of its group's entry here.
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
	Host   *loop.Loop
	Rounds *loop.Log
	Metas  []meta.RoundConfig
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
// the authority, which the replica starts with. It hosts the disk's MetaStore replicas on the
// round driver of its loop, routes the store's mail to them and their replies to the store, and
// serves the Txns sent to them.
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
	// metas are the MetaStore replicas by the slot their Raft timer names, a removed one keeping
	// its slot and taking no event, groups each hosted one's slot by its group, sv the server of
	// their Txns, and removed the replicas removed that a manifest a restart may load still names.
	metas   []*meta.RoundShard
	groups  map[uint64]int
	sv      *meta.Server
	removed []removedMeta
}

// removedMeta is a removed MetaStore replica: its group, replica ID and owner key, and whether
// the store has taken its removal, which waits for its rounds in flight (loop.ReplyRemoved).
type removedMeta struct {
	group, id uint64
	owner     uint32
	reported  bool
}

// NewDevice starts recovering the device's store, or fails it at once on cfg.Err.
func NewDevice(ctx *core.Context, cfg DeviceConfig, out *DeviceOutput) *Device {
	d := &Device{cfg: cfg, rng: core.NewRand(cfg.Seed), start: lease.Now(ctx), auth: cfg.Ctl, hasAuth: cfg.Ctl != core.NodeID{},
		groups: map[uint64]int{}, sv: meta.NewServer()}
	d.cfg.Store.Replicas = d.restore
	err := cfg.Err
	if err == nil {
		d.st, err = chunkstore.Open(ctx, d.cfg.Store, cfg.Blocks, &d.sout)
	}
	d.settle(ctx, err, out)
	return d
}

// ErrNoSettings stops a restart whose disk holds a MetaStore replica of a group Metas has no
// settings for, or two of one group.
var ErrNoSettings = errors.New("node: the disk holds a MetaStore replica with no settings")

// restore hosts the replica the manifest a restart loaded names, before the replay.
func (d *Device) restore(owner uint32, rep disklog.Replica) error {
	i := slices.IndexFunc(d.cfg.Metas, func(c meta.RoundConfig) bool { return c.Shard.Raft.Group == rep.Group })
	if _, hosted := d.groups[rep.Group]; i < 0 || hosted {
		return fmt.Errorf("%w: replica %d of group %d", ErrNoSettings, rep.ID, rep.Group)
	}
	cfg := d.cfg.Metas[i]
	cfg.Owner, cfg.Shard.Raft.ID, cfg.Open = owner, rep.ID, true
	d.host(cfg)
	return nil
}

// ErrHosted refuses a MetaStore replica of a group the device hosts one of, or whose owner key
// its store may not give a new shard yet.
var ErrHosted = errors.New("node: the device hosts the group, or its owner key is not reusable")

// ErrBlobBound fails a device whose log's blob bound is not the one a log formatted at its size
// has, which its replicas took: a log formatted at another size, as a grown volume's.
var ErrBlobBound = errors.New("node: the log's blob bound is not a log of the device's size's")

// ErrNotServing refuses a MetaStore replica on a device whose store does not serve.
var ErrNotServing = errors.New("node: the device's store does not serve")

// AddMeta hosts a new MetaStore replica, which writes its group's boot state as its first round.
func (d *Device) AddMeta(cfg meta.RoundConfig) error {
	_, hosted := d.groups[cfg.Shard.Raft.Group]
	switch {
	case d.phase != opened:
		return ErrNotServing
	case hosted || !d.st.Reusable(cfg.Owner):
		return ErrHosted
	}
	cfg.Open = false
	d.host(cfg)
	return nil
}

// Meta is the device's MetaStore replica of group, or nil.
func (d *Device) Meta(group uint64) *meta.RoundShard {
	if i, ok := d.groups[group]; ok {
		return d.metas[i]
	}
	return nil
}

// RemoveMeta takes the device's replica of group off its loop and deletes it, answering every Txn
// it holds dropped; false when the device hosts none. The disk's next manifest leaves it out.
func (d *Device) RemoveMeta(ctx *core.Context, group uint64, out *DeviceOutput) bool {
	i, ok := d.groups[group]
	if !ok {
		return false
	}
	a := d.metas[i]
	delete(d.groups, group)
	d.removed = append(d.removed, removedMeta{group: group, id: a.ReplicaID(), owner: a.Owner()})
	out.Actions = a.Remove(ctx, out.Actions)
	return true
}

// leftOut reports whether a restart would restore replica id of group no more, if the device
// removed it: the manifest a restart loads leaves it out.
func (d *Device) leftOut(group, id uint64) bool {
	keep, ok := 0, true
	for _, r := range d.removed {
		if !r.reported || !d.st.LeftOut(r.owner) {
			ok = ok && (r.group != group || r.id != id)
			d.removed[keep] = r
			keep++
		}
	}
	d.removed = d.removed[:keep]
	return ok
}

// host puts the replica cfg describes on the device's loop and makes it known to the store, in
// one call.
func (d *Device) host(cfg meta.RoundConfig) {
	maxBlob, err := d.maxBlob()
	if err != nil {
		panic(err)
	}
	i := len(d.metas)
	cfg.Shard.Raft.Timer, cfg.MaxBlob, cfg.Answered = MetaTimer+core.TimerID(i), maxBlob, d.sv.Reply
	a := meta.NewRoundShard(d.cfg.Host, d.cfg.Rounds, cfg)
	d.st.AddRemote(cfg.Owner, int(d.cfg.Loop), uint32(a.ID()), disklog.Replica{Group: cfg.Shard.Raft.Group, ID: cfg.Shard.Raft.ID})
	d.metas = append(d.metas, a)
	d.groups[cfg.Shard.Raft.Group] = i
}

// maxBlob is MaxBlobBytes of a log formatted at the device's size, which its replicas take before
// a log serves.
func (d *Device) maxBlob() (uint64, error) {
	g, err := disklog.GeometryFor(d.cfg.Blocks * core.BlockSize)
	return disklog.MaxBlobFor(d.cfg.Store.Log, g), err
}

// dropMetas takes the replicas the device restored off the loop when its store does not serve:
// they wrote nothing, and no store knows them.
func (d *Device) dropMetas() {
	for _, a := range d.metas {
		d.cfg.Host.RemoveShard(a.ID())
		a.Close()
	}
	d.metas = d.metas[:0]
	clear(d.groups)
}

// Replica is the device's replica, nil until the membership names its log.
func (d *Device) Replica() *chunkstore.Replica { return d.rep }

// Store is the device's store, nil once its start failed.
func (d *Device) Store() *chunkstore.Store { return d.st }

// OnEvent takes a disk completion, a timer, a message or a lost peer for the device's store or
// replica; a message before the replica runs is dropped, and the replica drops a Raft group's.
func (d *Device) OnEvent(ctx *core.Context, ev *core.Event, out *DeviceOutput) {
	switch {
	case ev.Kind == core.EvNetRecv && ev.Msg.Group():
		out.Actions = d.recvGroup(ctx, ev, out.Actions)
		return
	case ev.Kind == core.EvTimer && ev.Timer >= MetaTimer && ev.Timer-MetaTimer < core.TimerID(len(d.metas)):
		out.Actions = d.metas[ev.Timer-MetaTimer].OnEvent(ctx, ev, out.Actions)
		return
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

// recvGroup hands a MetaStore group's message to the device's replica of the group: a Txn through
// the server, which answers one for a group hosted nowhere here as NotLeader; an ask about a
// replica the device does not host, once its store serves and so has restored every replica the
// disk holds, is answered Gone, unless the device removed it and a restart would still restore
// it; another message for no replica here is dropped.
func (d *Device) recvGroup(ctx *core.Context, ev *core.Event, acts []core.Action) []core.Action {
	in := ev.Msg.Inline()
	var a *meta.RoundShard
	if len(in) > 9 {
		a = d.Meta(binary.LittleEndian.Uint64(in[1:]))
	}
	var gone core.Action
	switch group, id, asked := raftshard.Asked(&ev.Msg); {
	case len(in) > 9 && in[9] == meta.KindTxn:
		return d.sv.ServeRound(ctx, ev, a, acts)
	case asked && d.phase == opened && (a == nil || a.ReplicaID() != id):
		if d.leftOut(group, id) && raftshard.Gone(&ev.Msg, ev.Peer, &gone) {
			acts = append(acts, gone)
		}
	case a != nil:
		return a.OnEvent(ctx, ev, acts)
	}
	free(ctx, &ev.Msg)
	return acts
}

// OnReply takes what a MetaStore replica replied its disk's owner, for the store, which acts on it
// at the next OnLog.
func (d *Device) OnReply(r loop.Reply) {
	if d.st == nil {
		return
	}
	switch r.Kind {
	case loop.ReplyReplay:
		d.st.RemoteReplay(r.Owner, r.LSN)
	case loop.ReplyFull:
		d.st.RemoteFull()
	case loop.ReplyPiece:
		d.st.RemotePiece(r.Owner, r.LSN, r.Data, r.Root)
	case loop.ReplyRuns:
		d.st.RemoteRuns(r.Owner, r.LSN, r.Refs, r.Bounds)
	case loop.ReplyDead:
		d.st.RemoteDead(r.Owner, r.Refs)
	case loop.ReplyCosts:
		d.st.RemoteCosts(r.Owner, r.Costs)
	case loop.ReplyRemoved:
		d.st.RemoveRemote(r.Owner, r.LSN)
		for i := range d.removed {
			d.removed[i].reported = d.removed[i].reported || d.removed[i].owner == r.Owner
		}
	}
}

// mailKinds is the loop's kind of each raftshard.MailKind.
var mailKinds = [...]loop.MailKind{
	raftshard.MailReplayed:     loop.MailReplayed,
	raftshard.MailRecovered:    loop.MailRecovered,
	raftshard.MailCheckpointed: loop.MailCheckpointed,
	raftshard.MailShare:        loop.MailShare,
	raftshard.MailFlush:        loop.MailFlush,
	raftshard.MailPiece:        loop.MailPiece,
	raftshard.MailFlushFailed:  loop.MailFlushFailed,
	raftshard.MailRun:          loop.MailRun,
	raftshard.MailRewrite:      loop.MailRewrite,
}

// mail sends the store's mail to its shards on the round driver.
func (d *Device) mail(ms []raftshard.Mail) {
	for i := range ms {
		m := &ms[i]
		d.cfg.Rounds.Send(m.Loop, loop.Mail{Shard: loop.ShardID(m.Shard), Kind: mailKinds[m.Kind], Owner: m.Owner, Ref: m.Ref, Blob: m.Blob,
			Data: m.Data, Pieces: m.Pieces, Base: m.Base, Share: m.Share, Bound: m.Bound})
	}
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
	if err == nil && d.st != nil && d.phase != opened {
		if maxBlob, gerr := d.maxBlob(); gerr != nil || maxBlob != d.st.Log().MaxBlobBytes() {
			err = ErrBlobBound
		}
	}
	switch act, log := chunkstore.StartDevice(d.rng, err); {
	case d.phase == recovering && act == chunkstore.StartFormat:
		d.phase = formatting
		g, gerr := disklog.GeometryFor(d.cfg.Blocks * core.BlockSize)
		if gerr == nil {
			d.dropMetas()
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
	d.dropMetas()
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
	d.mail(d.sout.Mail)
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
	d.mail(d.rout.Mail)
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
