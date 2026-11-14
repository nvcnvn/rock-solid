package meta

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/lease"
)

// AgeStep is how finely a down disk's committed age is kept: the age is written again each time
// it grows by a step, so a failover restarts a disk's refill_after count by at most a step plus
// the failover.
const AgeStep = core.Tick(60_000_000)

// ErrFormatLevel reports a format level that is not above the committed one, that a node heard
// from runs a release below, or raised before every node with a disk up has been heard from.
var ErrFormatLevel = errors.New("meta: format level refused")

// ErrUnknownDisk reports an operator's drain of a disk the record does not know or whose log is gone.
var ErrUnknownDisk = errors.New("meta: unknown disk")

// OpenedDisk is a disk a node opened: the drive's stable identity and the log ID its superblock holds.
type OpenedDisk struct {
	Device, Log uint64
}

// HaltReport is a replica halted at a committed entry it could not decode: its group, its replica
// ID, the entry's index, and how long it has been halted, as the node measured it.
type HaltReport struct {
	Group, ID, Index uint64
	Age              core.Tick
}

// NodeReport is what a node's heartbeat tells the control-plane leader beside its disks' own
// heartbeats: the format level its release writes, whether it holds metadata and, with role
// meta, nothing else, the table epoch and session-table version it holds, the disks it opened and
// its halted replicas.
type NodeReport struct {
	Node     uint32
	Release  uint32
	Meta     bool
	MetaOnly bool
	Epoch    uint64
	Sessions uint64
	Opened   []OpenedDisk
	Halts    []HaltReport
}

// nodeState is what the leader last heard from a node and when, and the resend round it last sent
// the node the whole table in.
type nodeState struct {
	id      uint32
	heard   core.Tick
	release uint32
	meta    bool
	only    bool
	sent    uint64
	sessV   uint64
	halts   []Halt
}

// DiskEntry is a disk as the record commits it: Up and Gone as the record has them, DownSince unset, its age while down and not gone in whole AgeSteps, and its drain.
type DiskEntry struct {
	chunkstore.RecordedDisk
	Age   core.Tick
	Drain bool
}

// Heartbeat takes a node's report at now: a node whose release is below the format level counts
// as silent, and a disk it opened joins the record. A behind node whose table the leader has not
// resent since a failover or an error answer is added to out.Tables. A log the record refuses,
// such as a cloned drive's or a replaced one's, or one of two disks reported on one device, is
// the error.
func (c *Control) Heartbeat(now core.Tick, nr *NodeReport, out *ControlOutput) error {
	c.begin(now)
	i, ok := slices.BinarySearchFunc(c.nodes, nr.Node, func(n nodeState, id uint32) int { return cmp.Compare(n.id, id) })
	if !ok {
		c.nodes = slices.Insert(c.nodes, i, nodeState{id: nr.Node})
	}
	n := &c.nodes[i]
	n.heard, n.release, n.meta, n.only, n.halts = now, nr.Release, nr.Meta, nr.MetaOnly, n.halts[:0]
	for _, h := range nr.Halts {
		n.halts = append(n.halts, Halt{Group: h.Group, ID: h.ID, Index: h.Index, Since: now - min(h.Age, now)})
	}
	n.sessV = nr.Sessions
	if nr.Sessions < c.sess.Version {
		out.SessionTables = append(out.SessionTables, nr.Node)
	}
	if nr.Release < c.format {
		return nil
	}
	if nr.Epoch < c.table.Epoch && n.sent < c.round {
		n.sent = c.round
		out.Tables = append(out.Tables, nr.Node)
	}
	var errs []error
	for k, o := range nr.Opened {
		onDevice := func(x OpenedDisk) bool { return x.Device == o.Device }
		if slices.IndexFunc(nr.Opened, onDevice) != k || slices.ContainsFunc(nr.Opened[k+1:], onDevice) {
			errs = append(errs, fmt.Errorf("meta: log %#x on node %d shares device %#x: %w", o.Log, nr.Node, o.Device, chunkstore.ErrLogRefused))
			continue
		}
		ds := c.rec.Disks()
		if j := slices.IndexFunc(ds, func(d chunkstore.RecordedDisk) bool { return d.Log == o.Log }); j >= 0 && ds[j].Node == nr.Node && ds[j].Device == o.Device && !ds[j].Gone {
			continue
		}
		if _, err := c.rec.Report(nr.Node, o.Device, o.Log); err != nil {
			errs = append(errs, fmt.Errorf("meta: log %#x on node %d: %w", o.Log, nr.Node, err))
		}
	}
	return errors.Join(errs...)
}

// OnEvent hands the failure detector a disk's heartbeat, a primary's member_timeout report, a
// probe's answer or its timer, and returns the probes and timers it asks for. The heartbeat of a
// disk on a node not heard from within node_down_after, or whose release is below the format
// level, is dropped, so the disk is never marked up.
func (c *Control) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	c.begin(ctx.Now)
	t := lease.Time{Boot: ctx.Now, Raw: ctx.Raw}
	if ev.Kind == core.EvResumed {
		c.resume(t)
	}
	c.stall(t)
	if ev.Kind == core.EvNetRecv && !slices.ContainsFunc(c.nodes, func(n nodeState) bool { return n.id == ev.Peer.Node && n.release >= c.format }) {
		frame, _ := chunkstore.Frame(ctx.Pool, &ev.Msg)
		if k, err := chunkstore.Peek(frame); err == nil && k == chunkstore.KindReport {
			if ev.Msg.Buf != core.NoBuf {
				ctx.Pool.Free(ev.Msg.Buf)
			}
			return out
		}
	}
	return c.det.OnEvent(ctx, ev, out)
}

// begin forgets the nodes not heard from within node_down_after of now and, once after a failover
// or a reload, starts the failure detector over the committed record at now, the start of this
// leader's lease: each down disk down since its committed age before now.
func (c *Control) begin(now core.Tick) {
	c.nodes = slices.DeleteFunc(c.nodes, func(n nodeState) bool { return now-min(n.heard, now) >= chunkstore.NodeDownAfter })
	if c.det != nil {
		return
	}
	ds := make([]chunkstore.RecordedDisk, len(c.disks))
	for i, e := range c.disks {
		ds[i] = e.RecordedDisk
		if !e.Up {
			ds[i].DownSince = now - min(e.Age, now)
		}
	}
	c.rec = chunkstore.NewDiskRecord(ds)
	c.det = chunkstore.NewDetector(&c.rec, now)
	c.det.View(&c.table, c.pin)
}

// Drain sets or ends an operator's drain of disk id, which the next Step commits.
func (c *Control) Drain(id uint32, on bool) error {
	if id == 0 || int(id) > len(c.disks) || c.disks[id-1].Gone {
		return ErrUnknownDisk
	}
	c.drains = slices.DeleteFunc(c.drains, func(x uint32) bool { return x == id })
	if on {
		c.drains = append(c.drains, id)
	}
	return nil
}

// SetFormat stages level as the cluster's format level for the next Step; it must be above the
// committed one, and this leader must have heard, within node_down_after, from every node with a
// disk committed up and from none on a release below it.
func (c *Control) SetFormat(level uint32) error {
	if err := c.checkFormat(level); err != nil {
		return err
	}
	c.stagedFormat = level
	return nil
}

func (c *Control) checkFormat(level uint32) error {
	if level <= c.format || slices.ContainsFunc(c.nodes, func(n nodeState) bool { return n.release < level }) {
		return ErrFormatLevel
	}
	for _, e := range c.disks {
		if e.Up && !slices.ContainsFunc(c.nodes, func(n nodeState) bool { return n.id == e.Node }) {
			return ErrFormatLevel
		}
	}
	return nil
}

// Format is the committed format level.
func (c *Control) Format() uint32 { return c.format }

// OldReleases lists the nodes heard from whose release is below the format level, which a
// condition names; their disks are not marked up.
func (c *Control) OldReleases() []uint32 {
	var ns []uint32
	for _, n := range c.nodes {
		if n.release < c.format {
			ns = append(ns, n.id)
		}
	}
	return ns
}

// Disks is the disk record as last committed.
func (c *Control) Disks() []DiskEntry { return slices.Clone(c.disks) }

// liveRecord is the record with each disk's mark from the detector.
func (c *Control) liveRecord() []chunkstore.RecordedDisk {
	ds := slices.Clone(c.rec.Disks())
	for i := range ds {
		if !ds[i].Gone {
			m, since := c.det.Mark(ds[i].ID)
			ds[i].Up, ds[i].DownSince = m == chunkstore.MarkUp, since
		}
	}
	return ds
}

// diskView is the live record as it commits at now: each disk's age while down in whole AgeSteps.
func (c *Control) diskView(live []chunkstore.RecordedDisk, now core.Tick) []DiskEntry {
	v := make([]DiskEntry, len(live))
	for i, d := range live {
		v[i] = DiskEntry{RecordedDisk: d, Drain: !d.Gone && slices.Contains(c.drains, d.ID)}
		if !d.Up && !d.Gone {
			v[i].Age = (now - min(d.DownSince, now)) / AgeStep * AgeStep
		}
		v[i].DownSince = 0
	}
	return v
}

// poolDisks is each disk of in, with its capacity figures, that the record knows, whose log is
// not gone and whose node is not role meta, at the node, mark and drain the record gives it.
func (c *Control) poolDisks(in []chunkstore.Disk) []chunkstore.Disk {
	ds := c.rec.Disks()
	var out []chunkstore.Disk
	for _, d := range in {
		if d.ID == 0 || int(d.ID) > len(ds) || ds[d.ID-1].Gone || slices.ContainsFunc(c.nodes, func(n nodeState) bool { return n.id == ds[d.ID-1].Node && n.only }) {
			continue
		}
		d.Node, d.Drain = ds[d.ID-1].Node, slices.Contains(c.drains, d.ID)
		d.Mark, d.DownSince = c.det.Mark(d.ID)
		out = append(out, d)
	}
	return out
}

// metaNodes and halts are what the nodes' heartbeats report for PlaceVoters.
func (c *Control) metaNodes() []uint32 {
	var ns []uint32
	for _, n := range c.nodes {
		if n.meta && n.release >= c.format {
			ns = append(ns, n.id)
		}
	}
	return ns
}

func (c *Control) halts() []Halt {
	var hs []Halt
	for _, n := range c.nodes {
		hs = append(hs, n.halts...)
	}
	return hs
}

// memberOps appends the writes of the disks whose entry v changes, the record's version and the format level.
func (c *Control) memberOps(ops []Op, n *ctlState) []Op {
	changed := false
	for i := range n.disks {
		e := appendDisk(nil, &n.disks[i])
		if i >= len(c.disks) || !bytes.Equal(e, appendDisk(nil, &c.disks[i])) {
			ops, changed = append(ops, Put(diskKey(n.disks[i].ID), e)), true
		}
	}
	if changed {
		n.diskV++
		ops = append(ops, Put(ctlKey(CtlMembership, nil), bigEndian.AppendUint64(nil, n.diskV)))
	}
	if n.format != c.format {
		ops = append(ops, Put(ctlKey(CtlFormat, nil), binary.AppendUvarint(nil, uint64(n.format))))
	}
	return ops
}

func diskKey(id uint32) []byte { return ctlKey(CtlMembership, bigEndian.AppendUint32(nil, id)) }

const (
	diskUp = 1 << iota
	diskGone
	diskDrain
)

// A disk's entry: its log ID and device, 8 bytes each, its node as a uvarint, a flags byte of up,
// gone and drain, and its age while down in ticks as a uvarint.
func appendDisk(dst []byte, e *DiskEntry) []byte {
	dst = bigEndian.AppendUint64(bigEndian.AppendUint64(dst, e.Log), e.Device)
	dst = binary.AppendUvarint(dst, uint64(e.Node))
	f := byte(0)
	for i, b := range [...]bool{e.Up, e.Gone, e.Drain} {
		if b {
			f |= 1 << i
		}
	}
	return binary.AppendUvarint(append(dst, f), uint64(e.Age))
}

// decodeDisk decodes disk id's entry; ErrMalformed for a disk up and gone, drained once gone, an
// age while up, or anything appendDisk would not write.
func decodeDisk(id uint32, b []byte) (DiskEntry, error) {
	if len(b) < 16 {
		return DiskEntry{}, ErrMalformed
	}
	d := decoder{b: b[16:]}
	node := d.uvarint()
	f := d.byte()
	e := DiskEntry{RecordedDisk: chunkstore.RecordedDisk{ID: id, Log: bigEndian.Uint64(b), Device: bigEndian.Uint64(b[8:]), Node: uint32(node), Up: f&diskUp != 0, Gone: f&diskGone != 0}, Drain: f&diskDrain != 0}
	e.Age = core.Tick(d.uvarint())
	if d.bad || len(d.b) != 0 || node > 1<<32-1 || f >= 1<<3 || e.Up && (e.Gone || e.Age != 0) || e.Gone && e.Drain || !bytes.Equal(appendDisk(nil, &e), b) {
		return DiskEntry{}, ErrMalformed
	}
	return e, nil
}
