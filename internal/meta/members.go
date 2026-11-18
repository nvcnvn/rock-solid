package meta

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
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

// ErrUnknownDisk reports an operator's drain of a disk the record does not know or whose log is
// gone, or a declaration or removal of one it has not committed.
var ErrUnknownDisk = errors.New("meta: unknown disk")

// ErrNotDrained reports an operator's removal of a disk whose drain has not committed or that a PG still names.
var ErrNotDrained = errors.New("meta: disk not drained")

// OpenedDisk is a disk a node opened: the drive's stable identity, the log ID its superblock
// holds, its size and its log's reserve in blocks, its class, which the record takes only when
// the disk joins, and the loop of the node that owns it, where its replica runs.
type OpenedDisk struct {
	Device, Log     uint64
	Blocks, Reserve uint64
	Class           string
	Loop            uint32
}

// HaltReport is a replica halted at a committed entry it could not decode: its group, its replica
// ID, the entry's index, and how long it has been halted, as the node measured it.
type HaltReport struct {
	Group, ID, Index uint64
	Age              core.Tick
}

// ReplicaReport is a metadata replica a node holds: its group, its replica ID, the index its
// applied configuration holds at (raftshard.Shard.ConfIndex), whether that configuration is
// joint, whether the replica learned its removal, and the configuration's voters, the incoming
// half's when joint.
type ReplicaReport struct {
	Group, ID, ConfIndex uint64
	Joint, Removed       bool
	Voters               []uint64
}

// ReplicaRef names a replica of a group.
type ReplicaRef struct {
	Group, ID uint64
}

// NodeReport is what a node's heartbeat tells the control-plane leader beside its disks' own
// heartbeats: the format level its release writes, whether it holds metadata and, with role
// meta, nothing else, the table epoch and session-table version it holds, the disks it opened, the
// devices it left to the operator (chunkstore.StartFailed), its halted replicas and its metadata
// replicas.
type NodeReport struct {
	Node     uint32
	Release  uint32
	Meta     bool
	MetaOnly bool
	Epoch    uint64
	Sessions uint64
	Opened   []OpenedDisk
	Failed   []uint64
	Halts    []HaltReport
	Replicas []ReplicaReport
}

// FailedDevice is a device a node reports failed, by its stable identity, for the operator.
type FailedDevice struct {
	Node   uint32
	Device uint64
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
	failed  []uint64
	reps    []ReplicaReport
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
// the error, and its device is listed failed with those the node reports.
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
	n.sessV, n.failed, n.reps = nr.Sessions, append(n.failed[:0], nr.Failed...), n.reps[:0]
	for _, r := range nr.Replicas {
		r.Voters = slices.Clone(r.Voters)
		n.reps = append(n.reps, r)
	}
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
	refuse := func(device uint64, err error) {
		errs = append(errs, err)
		if !slices.Contains(n.failed, device) {
			n.failed = append(n.failed, device)
		}
	}
	for k, o := range nr.Opened {
		onDevice := func(x OpenedDisk) bool { return x.Device == o.Device }
		if slices.IndexFunc(nr.Opened, onDevice) != k || slices.ContainsFunc(nr.Opened[k+1:], onDevice) {
			refuse(o.Device, fmt.Errorf("meta: log %#x on node %d shares device %#x: %w", o.Log, nr.Node, o.Device, chunkstore.ErrLogRefused))
			continue
		}
		ds := c.rec.Disks()
		if j := slices.IndexFunc(ds, func(d chunkstore.RecordedDisk) bool { return d.Log == o.Log }); j >= 0 && ds[j].Node == nr.Node && ds[j].Device == o.Device && !ds[j].Gone {
			continue
		}
		if _, err := c.rec.Report(nr.Node, o.Device, o.Log, o.Class); err != nil {
			refuse(o.Device, fmt.Errorf("meta: log %#x on node %d: %w", o.Log, nr.Node, err))
		}
	}
	for _, o := range nr.Opened {
		if id := c.diskOfLog(o.Log); id != 0 && o.Blocks != 0 {
			f := c.diskFill(id)
			f.blocks, f.reserve = o.Blocks, o.Reserve
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
	if ev.Kind == core.EvNetRecv {
		frame, data := chunkstore.Frame(ctx.Pool, &ev.Msg)
		if k, err := chunkstore.Peek(frame); err == nil && k == chunkstore.KindReport {
			if !c.reporting(ev.Peer.Node) {
				if ev.Msg.Buf != core.NoBuf {
					ctx.Pool.Free(ev.Msg.Buf)
				}
				return out
			}
			if chunkstore.DecodeReport(frame, data, &c.rep) == nil {
				c.fill(ctx.Now, &c.rep)
				c.watch.Hear(c.rep.Leads)
				c.Report(&c.rep)
			}
		}
	}
	return c.det.OnEvent(ctx, ev, out)
}

// reporting is whether node was heard from within node_down_after at a release at the format level.
func (c *Control) reporting(node uint32) bool {
	return slices.ContainsFunc(c.nodes, func(n nodeState) bool { return n.id == node && n.release >= c.format })
}

// Asked takes a replica's asks for new intervals from node, dropped unless node was heard from at the format level.
func (c *Control) Asked(node uint32, fs []chunkstore.PGFields) {
	if c.reporting(node) {
		for _, f := range fs {
			c.Ask(f)
		}
	}
}

// VoterReport takes, at now, the report of the control-plane group's voter on node: its replicas,
// which count as a node's do toward a plan's completion and its deletes, and nothing else, since
// the voter is not a storage node.
func (c *Control) VoterReport(now core.Tick, node uint32, reps []ReplicaReport) {
	c.begin(now)
	i, ok := slices.BinarySearchFunc(c.voters, node, func(n nodeState, id uint32) int { return cmp.Compare(n.id, id) })
	if !ok {
		c.voters = slices.Insert(c.voters, i, nodeState{id: node})
	}
	v := &c.voters[i]
	v.heard, v.reps = now, v.reps[:0]
	for _, r := range reps {
		r.Voters = slices.Clone(r.Voters)
		v.reps = append(v.reps, r)
	}
}

// Deletes appends to dst each replica that node, or the control-plane voter on node, last
// reported having learned its removal and whose group's plan, its shard-map entry, no longer names
// it, once that plan is complete. It appends nothing unless leased: the leader holds its lease.
func (c *Control) Deletes(node uint32, leased bool, dst []ReplicaRef) []ReplicaRef {
	if !leased {
		return dst
	}
	for _, ns := range [...][]nodeState{c.nodes, c.voters} {
		if i, ok := slices.BinarySearchFunc(ns, node, func(n nodeState, id uint32) int { return cmp.Compare(n.id, id) }); ok {
			dst = c.deletes(ns[i].reps, dst)
		}
	}
	return dst
}

func (c *Control) deletes(reps []ReplicaReport, dst []ReplicaRef) []ReplicaRef {
	for _, r := range reps {
		g, ok := c.group(r.Group)
		if ok && r.Removed && !slices.ContainsFunc(g.Voters, func(v Replica) bool { return v.ID == r.ID }) && c.complete(g) {
			dst = append(dst, ReplicaRef{Group: r.Group, ID: r.ID})
		}
	}
	return dst
}

// complete reports whether every voter of g's plan reported having applied its leave: an index at
// or past the lowest at which any replica reported a configuration of the plan's voters alone. A
// report of the joint configuration, whose incoming half is the plan's, does not count, since a
// voter that applied only that can still lead it and need the voters the plan drops. A plan whose
// voter is down for good never completes, but the next plan, which replaces that voter, does, and
// so a plan never waits for the one before it.
func (c *Control) complete(g *Group) bool {
	leave := uint64(math.MaxUint64)
	c.eachReport(func(r *ReplicaReport) {
		if r.Group == g.ID && (!r.Joint || c.jointCounts) && len(r.Voters) == len(g.Voters) && !slices.ContainsFunc(r.Voters, func(id uint64) bool {
			return !slices.ContainsFunc(g.Voters, func(v Replica) bool { return v.ID == id })
		}) {
			leave = min(leave, r.ConfIndex)
		}
	})
	if leave == math.MaxUint64 {
		return false
	}
	for _, v := range g.Voters {
		applied := false
		c.eachReport(func(r *ReplicaReport) { applied = applied || r.Group == g.ID && r.ID == v.ID && r.ConfIndex >= leave })
		if !applied {
			return false
		}
	}
	return true
}

// eachReport calls f with each replica report the nodes and the control-plane voters last sent.
func (c *Control) eachReport(f func(r *ReplicaReport)) {
	for _, ns := range [...][]nodeState{c.nodes, c.voters} {
		for i := range ns {
			for j := range ns[i].reps {
				f(&ns[i].reps[j])
			}
		}
	}
}

// begin forgets the nodes and control-plane voters not heard from within node_down_after of now and, once after a failover
// or a reload, starts the failure detector over the committed record at now, the start of this
// leader's lease: each down disk down since its committed age before now.
func (c *Control) begin(now core.Tick) {
	silent := func(n nodeState) bool { return now-min(n.heard, now) >= chunkstore.NodeDownAfter }
	c.nodes, c.voters = slices.DeleteFunc(c.nodes, silent), slices.DeleteFunc(c.voters, silent)
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
	c.det.Lags = c.lagsTable
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

// Lost takes an operator's declaration at now that disk id, down, will not return, which the next
// Step commits: its log is gone, so its members refill at once, its leaving members go with no
// report, and a later report of its log is refused. ErrUnknownDisk for a disk the record has not
// committed; chunkstore.ErrLostRefused for one the record has up, or that a PG's current members
// hold not marked down. It has committed once Disks shows the disk gone.
func (c *Control) Lost(now core.Tick, id uint32) error { return c.stageExit(now, diskExit{id, false}) }

// Remove takes an operator's removal at now of disk id, drained, which the next Step commits as
// Lost does a declaration. ErrUnknownDisk for a disk the record has not committed; ErrNotDrained
// unless its drain has committed and no PG of the committed table names it, in its members, leaving
// ones included, or in an interval since its last activated one.
func (c *Control) Remove(now core.Tick, id uint32) error { return c.stageExit(now, diskExit{id, true}) }

// diskExit is a declaration or a removal staged until it commits.
type diskExit struct {
	id     uint32
	remove bool
}

func (c *Control) stageExit(now core.Tick, x diskExit) error {
	c.begin(now)
	if err := c.exit(x); err != nil {
		return err
	}
	if !c.disks[x.id-1].Gone && !slices.Contains(c.exits, x) {
		c.exits = append(c.exits, x)
	}
	return nil
}

// exit checks x against the committed state and applies it to the record; a disk already gone
// passes, so an operator may repeat a command whose answer was lost.
func (c *Control) exit(x diskExit) error {
	if x.id == 0 || int(x.id) > len(c.disks) {
		return ErrUnknownDisk
	}
	if c.disks[x.id-1].Gone {
		return nil
	}
	if x.remove {
		if !c.disks[x.id-1].Drain || c.names(x.id, true) {
			return ErrNotDrained
		}
		return c.rec.Remove(x.id)
	}
	if c.names(x.id, false) {
		return chunkstore.ErrLostRefused
	}
	return c.rec.Lost(x.id)
}

// names reports whether a PG of the committed table names disk id: with past, as a member or in
// an interval since its last activated one; without, as a member of its current interval not marked
// down, which the epoch model's Lost needs.
func (c *Control) names(id uint32, past bool) bool {
	for _, pt := range c.table.Pools {
		for _, e := range pt.PGs {
			if slices.ContainsFunc(e.Members, func(m chunkstore.Member) bool { return m.Disk == id && (past || m.Mark != chunkstore.MarkDown) }) {
				return true
			}
			if past && slices.ContainsFunc(e.Past, func(p chunkstore.PastInterval) bool {
				return slices.ContainsFunc(p.Members, func(m chunkstore.Member) bool { return m.Disk == id })
			}) {
				return true
			}
		}
	}
	return false
}

// reapply applies again each staged declaration and removal, which a reload of the record drops,
// and drops those the committed state now refuses, returning their errors.
func (c *Control) reapply() error {
	var errs []error
	c.exits = slices.DeleteFunc(c.exits, func(x diskExit) bool {
		err := c.exit(x)
		if err != nil {
			errs = append(errs, fmt.Errorf("meta: disk %d: %w", x.id, err))
		}
		return err != nil
	})
	return errors.Join(errs...)
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

// lagsTable reports whether node holds an older session table than the committed one, or has not
// said which it holds: a disk of it marked up would order a fenced session's write it held.
func (c *Control) lagsTable(node uint32) bool {
	i := slices.IndexFunc(c.nodes, func(n nodeState) bool { return n.id == node })
	return !c.plant.upWithOldTable && (i < 0 || c.nodes[i].sessV < c.sess.Version)
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
// not gone and whose node is not role meta, at the node, class, mark and drain the record gives it.
func (c *Control) poolDisks(in []chunkstore.Disk) []chunkstore.Disk {
	ds := c.rec.Disks()
	var out []chunkstore.Disk
	for _, d := range in {
		if d.ID == 0 || int(d.ID) > len(ds) || ds[d.ID-1].Gone || slices.ContainsFunc(c.nodes, func(n nodeState) bool { return n.id == ds[d.ID-1].Node && n.only }) {
			continue
		}
		d.Node, d.Class, d.Drain = ds[d.ID-1].Node, ds[d.ID-1].Class, slices.Contains(c.drains, d.ID)
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

// FailedDevices is every device the nodes heard from report failed, or report opened under a log
// the record refuses, by node.
func (c *Control) FailedDevices() []FailedDevice {
	var fs []FailedDevice
	for _, n := range c.nodes {
		for _, d := range n.failed {
			fs = append(fs, FailedDevice{Node: n.id, Device: d})
		}
	}
	return fs
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
// gone and drain, its age while down in ticks as a uvarint, and its class as length and bytes.
func appendDisk(dst []byte, e *DiskEntry) []byte {
	dst = bigEndian.AppendUint64(bigEndian.AppendUint64(dst, e.Log), e.Device)
	dst = binary.AppendUvarint(dst, uint64(e.Node))
	f := byte(0)
	for i, b := range [...]bool{e.Up, e.Gone, e.Drain} {
		if b {
			f |= 1 << i
		}
	}
	return appendBytes(binary.AppendUvarint(append(dst, f), uint64(e.Age)), []byte(e.Class))
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
	e.Age, e.Class = core.Tick(d.uvarint()), string(d.bytes())
	if d.bad || len(d.b) != 0 || node > 1<<32-1 || f >= 1<<3 || e.Up && (e.Gone || e.Age != 0) || e.Gone && e.Drain || !bytes.Equal(appendDisk(nil, &e), b) {
		return DiskEntry{}, ErrMalformed
	}
	return e, nil
}
