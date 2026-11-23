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

// The ctl/ keys and the commit rule are design.md's "`ctl/` keys and commits".

var bigEndian = binary.BigEndian

// PoolRecord is a pool the control plane holds: its ID and its settings.
type PoolRecord struct {
	ID   core.PoolID
	Pool chunkstore.Pool
}

// ControlInputs are what a step is computed from beside what the control plane committed and
// heard: the leader's two clocks and whether it holds its leader lease, each disk's capacity
// figures as last reported, a pool taking those of its class, and the operator's edits and
// balance moves.
type ControlInputs struct {
	Now    core.Tick
	Raw    core.Tick
	Leased bool
	Disks  []chunkstore.Disk
	Edits  []chunkstore.Edit
	Moves  int
}

// ControlOutput is what one call into a Control hands its driver: a Txn to propose to the
// control-plane group under Token; once one commits, its table as a delta for every node and the
// shard map entries it changed for their groups' leaders; after a failover or an error answer,
// every committed entry again, and the nodes to send the whole committed table. Of sessions: the
// starts committed, whether a fence committed so every node gets the session table, and the nodes
// a heartbeat shows behind on it.
type ControlOutput struct {
	Propose       bool
	Txn           Txn
	Token         uint64
	Push          bool
	Delta         chunkstore.Delta
	Plans         []Group
	Tables        []uint32
	Started       []SessionStart
	SessionPush   bool
	SessionTables []uint32
}

// Reset empties the output for reuse.
func (o *ControlOutput) Reset() {
	*o = ControlOutput{Plans: o.Plans[:0], Tables: o.Tables[:0], Started: o.Started[:0], SessionTables: o.SessionTables[:0]}
}

// ctlState is what the control plane committed.
type ctlState struct {
	pools  []PoolRecord
	table  chunkstore.Table
	crc    uint32
	smap   ShardMap
	smapV  uint64
	disks  []DiskEntry
	diskV  uint64
	format uint32
	sess   SessionTable
}

// Control is the control-plane leader's machine beside its group: it commits pools, placement
// tables, the shard map, the disk record with the failure detector's marks, the format level and
// the session table as the group's Txns, one at a time, and hands out a table or a plan only once
// it has committed. It renews sessions in its memory.
// A new leader starts one from its applied state once it has applied an entry of its term; a
// commit another leader raced fails its checks, and the machine reads the state again.
type Control struct {
	ctlState
	staged       []PoolRecord
	stagedMap    *ShardMap
	stagedFormat uint32
	asks         []chunkstore.PGInterval
	acts, rcv    []chunkstore.PGReport
	pending      *commit
	// from is the PG the last table over its budget stopped at, where the next takes its changes from.
	from    core.PGID
	rec     chunkstore.DiskRecord
	det     *chunkstore.Detector
	pin     []chunkstore.PoolInputs
	drains  []uint32
	exits   []diskExit
	giveUps []chunkstore.PGInterval
	nodes   []nodeState
	// voters are the control-plane group's voters heard from, by node, each holding only its replicas.
	voters []nodeState
	// round counts failovers and error answers; resend is set while every committed shard-map
	// entry is still to be handed out again.
	round  uint64
	resend bool
	starts []SessionStart
	timers []sessTimer
	// seen is the time of the last input, leased whether the leader held its lease then,
	// tableTurn whether a session commit went last, so the table's step goes next, and current
	// whether a session Txn of this machine committed since it last read the state, its head check
	// proving the session table current.
	seen      lease.Time
	leased    bool
	tableTurn bool
	current   bool
	plant     sessionPlants
	// Space grants: this leader lease, each session's grants, the sessions heard from or started under it,
	// each pool's room, each disk's fill, when this leader first stepped, and a scratch report.
	issuer     Issuer
	grants     []grantEntry
	heard      []uint64
	pgrants    []poolGrants
	fills      []diskFill
	grantsFrom struct {
		set bool
		at  core.Tick
	}
	rep    chunkstore.Report
	gplant grantPlants
	// jointCounts is a bug a test plants: a report of the joint configuration counts toward a plan's completion.
	jointCounts bool
	watch       chunkstore.Watcher
}

// commit is the Txn in flight: the state it gives and how much of the staged reports it took.
type commit struct {
	next             ctlState
	token            uint64
	table, mapStaged bool
	delta            chunkstore.Delta
	plans            []Group
	asks, acts, rc   int
	fenced, session  bool
	started          []SessionStart
}

// NewControl reads the control plane's state from st, the control-plane group's applied state, for
// its leader in term.
func NewControl(st *State, term uint64) (*Control, error) {
	c := &Control{round: 1, resend: true, issuer: Issuer{Term: term}}
	if err := c.load(st); err != nil {
		return nil, err
	}
	for _, e := range c.disks {
		if e.Drain {
			c.drains = append(c.drains, e.ID)
		}
	}
	return c, nil
}

// Table is the committed placement table, which a node fetches whole after a gap or a CRC mismatch.
func (c *Control) Table() *chunkstore.Table { return &c.table }

// Pools is the committed pools in ascending ID order.
func (c *Control) Pools() []PoolRecord { return c.pools }

// ShardMap is the committed shard map.
func (c *Control) ShardMap() *ShardMap { return &c.smap }

// SetPool checks a pool's create or update against the pool rules and the committed pool, and
// stages it for the next commit in place of any change of the pool staged before; an update keeps
// each setting spec leaves out as the next commit gives it. disks are the cluster's, each at the
// class the record gives it, for an unset pg_count and the layout check. A refused one stages
// nothing, and one that changes nothing unstages the pool. A new pool, or a change of its
// replicas, min_live or pg_count, whose first table is more than one Txn holds is ErrTooLarge.
func (c *Control) SetPool(id core.PoolID, spec chunkstore.PoolSpec, disks []chunkstore.Disk) error {
	i, ok := slices.BinarySearchFunc(c.pools, id, cmpPool)
	if ok {
		spec = keep(spec, c.nextPool(id))
	}
	disks = slices.Clone(disks)
	for i, d := range disks {
		if rec := c.rec.Disks(); d.ID >= 1 && int(d.ID) <= len(rec) {
			disks[i].Class = rec[d.ID-1].Class
		}
	}
	p, err := chunkstore.NewPool(spec, len(disks))
	if err == nil && ok {
		err = c.pools[i].Pool.Change(p)
	}
	if err == nil {
		_, err = p.CheckLayout(disks)
	}
	if err == nil && (!ok || p.PGCount != c.pools[i].Pool.PGCount || p.Replicas != c.pools[i].Pool.Replicas || p.MinLive != c.pools[i].Pool.MinLive) {
		err = fits(PoolRecord{id, p}, disks, &c.table)
	}
	if err != nil {
		return err
	}
	c.staged = slices.DeleteFunc(c.staged, func(s PoolRecord) bool { return s.ID == id })
	if !ok || p != c.pools[i].Pool {
		c.staged = append(c.staged, PoolRecord{id, p})
	}
	return nil
}

// keep fills each setting spec leaves out from p. An empty string is left out, so it cannot clear
// p's disk_class.
func keep(spec chunkstore.PoolSpec, p chunkstore.Pool) chunkstore.PoolSpec {
	spec.Replicas = cmp.Or(spec.Replicas, &p.Replicas)
	spec.MinLive = cmp.Or(spec.MinLive, &p.MinLive)
	spec.MaxPerDomain = cmp.Or(spec.MaxPerDomain, &p.MaxPerDomain)
	spec.Overcommit = cmp.Or(spec.Overcommit, &p.Overcommit)
	spec.PGCount = cmp.Or(spec.PGCount, &p.PGCount)
	if p.WriteLimit != 0 {
		spec.WriteLimit = cmp.Or(spec.WriteLimit, &p.WriteLimit)
	}
	spec.FailureDomain = cmp.Or(spec.FailureDomain, p.FailureDomain)
	spec.Locality = cmp.Or(spec.Locality, p.Locality)
	spec.DiskClass = cmp.Or(spec.DiskClass, p.DiskClass)
	return spec
}

// fits reports ErrTooLarge when pool cp's first table from prev, its creation, split or settings
// change, beside a disk-record entry of every disk, as when every down mark commits with it, is more
// than one Txn holds, since a table holds a pool whose head changes whole.
func fits(cp PoolRecord, disks []chunkstore.Disk, prev *chunkstore.Table) error {
	t, _, _ := chunkstore.Next(prev, &chunkstore.Inputs{Pools: []chunkstore.PoolInputs{poolInputs(cp, disks)}})
	var old []chunkstore.PGEntry
	if i := slices.IndexFunc(prev.Pools, func(p chunkstore.PoolTable) bool { return p.ID == cp.ID }); i >= 0 {
		old = prev.Pools[i].PGs
	}
	n := 0
	for i := range t.Pools[0].PGs {
		var o *chunkstore.PGEntry
		if i < len(old) {
			o = &old[i]
		}
		n += entryCost(core.PGID{Pool: cp.ID, Index: uint32(i)}, o, &t.Pools[0].PGs[i])
	}
	dops := []Op{Put(ctlKey(CtlMembership, nil), bigEndian.AppendUint64(nil, 0))}
	for _, d := range disks {
		dops = append(dops, Put(diskKey(d.ID), appendDisk(nil, &DiskEntry{RecordedDisk: chunkstore.RecordedDisk{ID: d.ID, Node: d.Node}, Age: ^core.Tick(0)})))
	}
	m, err := (&Txn{Ops: dops}).Check()
	if err == nil && n+m > MaxCtlTxnBytes {
		err = ErrTooLarge
	}
	return err
}

func poolInputs(cp PoolRecord, disks []chunkstore.Disk) chunkstore.PoolInputs {
	pin := chunkstore.PoolInputs{ID: cp.ID, Pool: cp.Pool}
	for _, d := range disks {
		if cp.Pool.DiskClass == "" || d.Class == cp.Pool.DiskClass {
			pin.Disks = append(pin.Disks, d)
		}
	}
	return pin
}

// SetShardMap stages m, whose ranges CheckRanges accepts and whose replica IDs are fresh, in place of the shard map.
func (c *Control) SetShardMap(m *ShardMap) error {
	if err := m.CheckRanges(); err != nil {
		return err
	}
	for i := range m.Groups {
		if old, ok := c.group(m.Groups[i].ID); !m.Groups[i].freshAfter(old, ok) {
			return ErrReplicaIDs
		}
	}
	m2 := ShardMap{Groups: slices.Clone(m.Groups)}
	c.stagedMap = &m2
	return nil
}

// Report folds a heartbeat's activation and recovered reports into the next table.
func (c *Control) Report(rp *chunkstore.Report) {
	c.acts = append(c.acts, rp.Activations...)
	c.rcv = append(c.rcv, rp.Recovered...)
}

// Ask takes a primary's or member's ask for a new interval of its PG, keeping one of each until a commit takes it;
// an ask tagged with a table older than its pool's split names a PG of the numbering before it, and is dropped, as
// are those taken while the split's commit was pending.
func (c *Control) Ask(f chunkstore.PGFields) {
	if f.Epoch < splitOf(&c.table, f.PG.Pool) {
		return
	}
	if a := (chunkstore.PGInterval{PG: f.PG, Interval: f.Interval}); !slices.Contains(c.asks, a) {
		c.asks = append(c.asks, a)
	}
}

// splitOf is the epoch of the table that last split pool in t, 0 for none.
func splitOf(t *chunkstore.Table, pool core.PoolID) uint64 {
	if i := slices.IndexFunc(t.Pools, func(p chunkstore.PoolTable) bool { return p.ID == pool }); i >= 0 {
		return t.Pools[i].Split
	}
	return 0
}

// Step computes the next table and shard map and, with no commit in flight, proposes under token
// whatever they and the staged pools change, or what the session table owes, the two taking turns.
// A table holds the PG changes the rest of the Txn leaves room for, keeping every report and ask
// for the next table, which takes the changes left from where this one stopped; a disk's down mark
// commits in the record only with the table that marks its last PG.
// A staged pool change the committed pool no longer takes, after a reload, is dropped with its
// error, as are a staged format level SetFormat would now refuse and a staged declaration or
// removal the committed state now refuses, and as are the staged pool changes of a step that is
// more than one Txn holds, with ErrTooLarge; the next Step commits the rest.
func (c *Control) Step(in *ControlInputs, token uint64, out *ControlOutput) error {
	c.begin(in.Now)
	lt := lease.Time{Boot: in.Now, Raw: in.Raw}
	c.sessClock(lt, in.Leased)
	if c.pending != nil {
		return nil
	}
	turn := c.tableTurn
	c.tableTurn = false
	if !turn {
		if ok, err := c.sessionStep(lt, token, false, out); ok || err != nil {
			c.tableTurn = ok
			return err
		}
	}
	if err := c.recheck(); err != nil {
		return err
	}
	giveUps, err := c.reapply()
	if err != nil {
		return err
	}
	p := &commit{token: token, asks: len(c.asks), acts: len(c.acts), rc: len(c.rcv)}
	n := &p.next
	n.pools = c.nextPools()
	pin := make([]chunkstore.PoolInputs, len(n.pools))
	ds := c.poolDisks(in.Disks)
	for i, cp := range n.pools {
		pin[i] = poolInputs(cp, ds)
	}
	c.pin = pin
	c.det.View(&c.table, pin)
	base := &c.smap
	if c.stagedMap != nil {
		base, p.mapStaged = c.stagedMap, true
	}
	live := c.liveRecord()
	n.smap = PlaceVoters(base, c.metaNodes(), live, c.halts(), in.Now)
	n.smapV = c.smapV
	n.disks, n.diskV, n.format, n.sess = c.diskView(live, in.Now), c.diskV, max(c.format, c.stagedFormat), c.sess
	var ops []Op
	for _, g := range n.smap.Groups {
		if old, ok := c.group(g.ID); !ok || !bytes.Equal(appendGroup(nil, old), appendGroup(nil, &g)) {
			p.plans = append(p.plans, g)
			if ops, err = putSplit(ops, groupKey(g.ID), appendGroup(nil, &g), groupLen(old)); err != nil {
				return err
			}
		}
	}
	for _, g := range c.smap.Groups {
		if !slices.ContainsFunc(n.smap.Groups, func(o Group) bool { return o.ID == g.ID }) {
			k := groupKey(g.ID)
			ops = append(ops, DeleteRange(k, PrefixEnd(k)))
		}
	}
	if len(ops) > 0 {
		n.smapV++
		ops = append(ops, Put(ctlKey(CtlShardMap, nil), bigEndian.AppendUint64(nil, n.smapV)))
	}
	pre, diskV := len(ops), n.diskV
	ops = c.memberOps(ops, n)
	var staged []Op
	for _, cp := range c.staged {
		staged = append(staged, Put(poolKey(cp.ID), appendPool(nil, &cp.Pool)))
	}
	used, _ := (&Txn{Ops: slices.Concat(c.guards(n.pools), ops, staged)}).Check()
	var left *core.PGID
	n.table, _, left = chunkstore.Next(&c.table, &chunkstore.Inputs{Now: in.Now, Pools: pin, Asks: c.asks, Activations: c.table.Reported(nil, c.acts), Recovered: c.table.Reported(nil, c.rcv), Edits: in.Edits, GiveUps: giveUps, Moves: in.Moves,
		Budget: MaxCtlTxnBytes - used - headOps*(len(pin)+len(c.table.Pools)+1), Cost: entryCost, From: c.from})
	if left != nil {
		c.from, p.asks, p.acts, p.rc = *left, 0, 0, 0
	}
	if c.holdDown(n) {
		n.diskV = diskV
		ops = c.memberOps(ops[:pre], n)
	}
	p.delta = chunkstore.MakeDelta(&c.table, &n.table)
	var moved []core.PoolID
	for _, de := range p.delta.Entries {
		moved = append(moved, de.PG.Pool)
	}
	c.grantStep(in.Now, in.Leased, ds, moved)
	p.table = len(p.delta.Entries) > 0 || !slices.Equal(poolHeads(&c.table, nil), poolHeads(&n.table, nil))
	if !p.table {
		n.table, n.crc = c.table, c.crc
	} else {
		n.crc = p.delta.CRC
	}
	if !p.table && len(ops) == 0 && len(c.staged) == 0 {
		c.asks, c.acts, c.rcv, c.stagedMap = c.asks[:0], c.acts[:0], c.rcv[:0], nil
		c.resendPlans(out)
		if turn || !c.current {
			_, err := c.sessionStep(lt, token, true, out)
			return err
		}
		return nil
	}
	if ops, err = c.tableOps(ops, p); err != nil {
		return err
	}
	t := Txn{Ops: slices.Concat(c.guards(n.pools), ops, staged)}
	if _, err := t.Check(); err != nil {
		if errors.Is(err, ErrTooLarge) && len(c.staged) > 0 {
			ids := make([]core.PoolID, len(c.staged))
			for i, cp := range c.staged {
				ids[i] = cp.ID
			}
			c.staged = c.staged[:0]
			return fmt.Errorf("meta: pool changes %v dropped: %w", ids, err)
		}
		return err
	}
	c.pending = p
	out.Propose, out.Txn, out.Token = true, t, token
	return nil
}

// guards are the checks that the table head, the shard-map and disk-record versions, the format level and every pool the next table
// is computed from hold what this machine committed, so a Txn computed from a state another leader
// has since changed commits nothing.
func (c *Control) guards(pools []PoolRecord) []Op {
	g := []Op{guard(ctlKey(CtlPlacement, nil), c.table.Epoch != 0, tableHead(c.table.Epoch, c.crc))}
	g = append(g, guard(ctlKey(CtlShardMap, nil), c.smapV != 0, bigEndian.AppendUint64(nil, c.smapV)))
	g = append(g, guard(ctlKey(CtlMembership, nil), c.diskV != 0, bigEndian.AppendUint64(nil, c.diskV)))
	g = append(g, guard(ctlKey(CtlFormat, nil), c.format != 0, binary.AppendUvarint(nil, uint64(c.format))))
	for _, cp := range pools {
		i, ok := slices.BinarySearchFunc(c.pools, cp.ID, cmpPool)
		var old []byte
		if ok {
			old = appendPool(nil, &c.pools[i].Pool)
		}
		g = append(g, guard(poolKey(cp.ID), ok, old))
	}
	return g
}

// recheck drops each staged pool change the committed pool does not take, or that changes nothing,
// and a staged format level SetFormat would now refuse, and returns the errors of those refused.
func (c *Control) recheck() error {
	var errs []error
	if c.stagedFormat > c.format {
		if err := c.checkFormat(c.stagedFormat); err != nil {
			errs = append(errs, fmt.Errorf("meta: format level %d: %w", c.stagedFormat, err))
			c.stagedFormat = 0
		}
	}
	c.staged = slices.DeleteFunc(c.staged, func(s PoolRecord) bool {
		i, ok := slices.BinarySearchFunc(c.pools, s.ID, cmpPool)
		if !ok {
			return false
		}
		if err := c.pools[i].Pool.Change(s.Pool); err != nil {
			errs = append(errs, fmt.Errorf("meta: pool %d: %w", s.ID, err))
			return true
		}
		return s.Pool == c.pools[i].Pool
	})
	return errors.Join(errs...)
}

func guard(k []byte, present bool, v []byte) Op {
	if !present {
		return CheckAbsent(k, false)
	}
	return Check(k, v, false)
}

// tableOps appends the writes of p's table: its head, each pool header it changes, a dropped pool's keys and each entry its delta holds.
func (c *Control) tableOps(ops []Op, p *commit) ([]Op, error) {
	if !p.table {
		return ops, nil
	}
	n := &p.next
	ops = append(ops, Put(ctlKey(CtlPlacement, nil), tableHead(n.table.Epoch, n.crc)))
	for _, pt := range c.table.Pools {
		if _, ok := n.tablePool(pt.ID); !ok {
			k := placementKey(pt.ID)
			ops = append(ops, DeleteRange(k, PrefixEnd(k)))
		}
	}
	for i := range n.table.Pools {
		pt := &n.table.Pools[i]
		h := poolHead(pt)
		if j, ok := c.tablePool(pt.ID); !ok || !bytes.Equal(h, poolHead(&c.table.Pools[j])) {
			ops = append(ops, Put(placementKey(pt.ID), h))
		}
	}
	var err error
	for _, de := range p.delta.Entries {
		old := 0
		if j, ok := c.tablePool(de.PG.Pool); ok && int(de.PG.Index) < len(c.table.Pools[j].PGs) {
			old = len(chunkstore.AppendEntry(nil, &c.table.Pools[j].PGs[de.PG.Index]))
		}
		if ops, err = putSplit(ops, entryKey(de.PG), chunkstore.AppendEntry(nil, &de.Entry), old); err != nil {
			return nil, err
		}
	}
	return ops, nil
}

// Answer takes the group's answer to the Txn proposed under its token: once it commits, the
// machine adopts what it holds and hands out the table and the plans; otherwise, a check failed
// or the Txn may not have applied, it reads the state again from st, restarts the failure
// detector over it at the next event, and hands out every committed plan again, and the whole
// table to each node a heartbeat then shows behind, since a commit whose answer was lost is never
// handed out otherwise.
func (c *Control) Answer(st *State, a *Answer, out *ControlOutput) error {
	p := c.pending
	if p == nil || a.Token != p.token {
		return nil
	}
	c.pending = nil
	if a.Err != nil || a.Result.FailedCheck >= 0 {
		c.current = false
		err := c.load(st)
		c.det, c.round, c.resend = nil, c.round+1, true
		c.resendPlans(out)
		return err
	}
	c.ctlState, c.current = p.next, c.current || p.session
	c.exits = slices.DeleteFunc(c.exits, func(x diskExit) bool { return c.disks[x.id-1].Gone })
	c.asks, c.acts, c.rcv = c.asks[p.asks:], c.acts[p.acts:], c.rcv[p.rc:]
	if p.table {
		c.asks = slices.DeleteFunc(c.asks, func(a chunkstore.PGInterval) bool { return splitOf(&c.table, a.PG.Pool) == c.table.Epoch })
	}
	if p.mapStaged {
		c.stagedMap = nil
	}
	if c.stagedFormat <= c.format {
		c.stagedFormat = 0
	}
	if p.table {
		out.Push, out.Delta = true, p.delta
	}
	c.starts = c.starts[len(p.started):]
	for _, s := range p.started {
		c.hear(s.ID)
	}
	out.Started, out.SessionPush = append(out.Started, p.started...), p.fenced
	if !c.resend {
		out.Plans = append(out.Plans, p.plans...)
	}
	c.resendPlans(out)
	return nil
}

// resendPlans hands out every committed shard-map entry once after a failover or an error answer.
func (c *Control) resendPlans(out *ControlOutput) {
	if c.resend {
		out.Plans, c.resend = append(out.Plans, c.smap.Groups...), false
	}
}

func (c *Control) nextPools() []PoolRecord {
	ps := slices.Clone(c.pools)
	for _, s := range c.staged {
		i, ok := slices.BinarySearchFunc(ps, s.ID, cmpPool)
		if ok {
			ps[i] = s
		} else {
			ps = slices.Insert(ps, i, s)
		}
	}
	return ps
}

// nextPool is pool id's settings as the next commit gives them: staged, or else committed.
func (c *Control) nextPool(id core.PoolID) chunkstore.Pool {
	if i := slices.IndexFunc(c.staged, func(s PoolRecord) bool { return s.ID == id }); i >= 0 {
		return c.staged[i].Pool
	}
	if i, ok := slices.BinarySearchFunc(c.pools, id, cmpPool); ok {
		return c.pools[i].Pool
	}
	return chunkstore.Pool{}
}

func cmpPool(p PoolRecord, id core.PoolID) int { return cmp.Compare(p.ID, id) }

func (s *ctlState) tablePool(id core.PoolID) (int, bool) {
	return slices.BinarySearchFunc(s.table.Pools, id, func(p chunkstore.PoolTable, id core.PoolID) int { return cmp.Compare(p.ID, id) })
}

func (s *ctlState) group(id uint64) (*Group, bool) {
	i := slices.IndexFunc(s.smap.Groups, func(g Group) bool { return g.ID == id })
	if i < 0 {
		return nil, false
	}
	return &s.smap.Groups[i], true
}

// poolHeads appends each pool's settings as a table carries them.
func poolHeads(t *chunkstore.Table, dst []chunkstore.DeltaPool) []chunkstore.DeltaPool {
	for i := range t.Pools {
		dst = append(dst, t.Pools[i].Head())
	}
	return dst
}

func ctlKey(tag byte, name []byte) []byte { return append([]byte{SpaceCtl, tag}, name...) }

func poolKey(id core.PoolID) []byte { return ctlKey(CtlPool, bigEndian.AppendUint32(nil, uint32(id))) }

func placementKey(id core.PoolID) []byte {
	return ctlKey(CtlPlacement, bigEndian.AppendUint32(nil, uint32(id)))
}

func entryKey(pg core.PGID) []byte { return bigEndian.AppendUint32(placementKey(pg.Pool), pg.Index) }

func groupKey(id uint64) []byte { return ctlKey(CtlShardMap, bigEndian.AppendUint64(nil, id)) }

func groupLen(g *Group) int {
	if g == nil {
		return 0
	}
	return len(appendGroup(nil, g))
}

func tableHead(epoch uint64, crc uint32) []byte {
	return bigEndian.AppendUint32(bigEndian.AppendUint64(nil, epoch), crc)
}

func poolHead(p *chunkstore.PoolTable) []byte {
	return bigEndian.AppendUint64(bigEndian.AppendUint32([]byte{p.Replicas, p.MinLive}, uint32(len(p.PGs))), p.Split)
}

// headOps bounds the bytes of each write tableOps makes beside the entries, the table head, a pool
// header and a dropped pool's range, with two to spare for the Txn's op count, which grows as the
// entries join it.
const headOps = 32

// entryCost is the bytes of the writes tableOps makes for PG pg's entry e in place of old.
func entryCost(pg core.PGID, old, e *chunkstore.PGEntry) int {
	n := 0
	if old != nil {
		n = len(chunkstore.AppendEntry(nil, old))
	}
	ops, err := putSplit(nil, entryKey(pg), chunkstore.AppendEntry(nil, e), n)
	size, err2 := (&Txn{Ops: ops}).Check()
	if err != nil || err2 != nil {
		return MaxCtlTxnBytes + 1
	}
	return size - 1 - uvarintLen(uint64(len(ops)))
}

// putSplit appends the writes that store v under k as a head and pieces, and drop the pieces past
// them that a value of old bytes left.
func putSplit(ops []Op, k, v []byte, old int) ([]Op, error) {
	h, err := AppendHead(nil, v)
	if err != nil {
		return ops, err
	}
	ops = append(ops, Put(k, h))
	m := Pieces(len(v))
	for i := 1; i <= m; i++ {
		ops = append(ops, Put(append(slices.Clip(k), byte(i)), Piece(v, i)))
	}
	if Pieces(old) > m {
		ops = append(ops, DeleteRange(append(slices.Clip(k), byte(m+1)), PrefixEnd(k)))
	}
	return ops, nil
}

// joinSplit is the value whose head is kvs[i], and the index of its last piece.
func joinSplit(kvs []KV, i int) (int, []byte, error) {
	m, v, err := DecodeHead(kvs[i].Value)
	if err != nil {
		return 0, nil, ErrMalformed
	}
	v = slices.Clone(v)
	for p := 1; p <= m; p++ {
		if i+p >= len(kvs) || !bytes.Equal(kvs[i+p].Key, append(slices.Clip(kvs[i].Key), byte(p))) {
			return 0, nil, ErrMalformed
		}
		if v, err = AppendPiece(v, kvs[i+p].Value, p, m); err != nil {
			return 0, nil, ErrMalformed
		}
	}
	return i + m, v, nil
}

// load reads every ctl/ key of st and replaces the committed state with what they hold.
func (c *Control) load(st *State) error {
	var kvs []KV
	var r ScanResult
	start, end := []byte{SpaceCtl}, PrefixEnd([]byte{SpaceCtl})
	for {
		if err := st.Scan(start, end, 1<<16, &r); err != nil {
			return err
		}
		for _, kv := range r.KVs {
			kvs = append(kvs, KV{slices.Clone(kv.Key), slices.Clone(kv.Value)})
		}
		if !r.More {
			break
		}
		start = append(slices.Clone(kvs[len(kvs)-1].Key), 0)
	}
	s, err := decodeCtl(kvs)
	if err != nil {
		return err
	}
	c.ctlState = s
	return nil
}

// decodeCtl is the state the ctl/ keys kvs, in key order, hold; ErrMalformed for any key or
// value the machine would not write, a pool missing entries, or a table whose CRC32C is not its head's.
func decodeCtl(kvs []KV) (ctlState, error) {
	var s ctlState
	var count []uint32
	var crc uint32
	for i := 0; i < len(kvs); i++ {
		k, v := kvs[i].Key, kvs[i].Value
		if len(k) < 2 || k[0] != SpaceCtl {
			return s, ErrMalformed
		}
		name, bad := k[2:], false
		switch k[1] {
		case CtlPool:
			p, err := decodePool(v)
			bad = len(name) != 4 || err != nil
			if !bad {
				s.pools = append(s.pools, PoolRecord{core.PoolID(bigEndian.Uint32(name)), p})
			}
		case CtlPlacement:
			switch {
			case len(name) == 0 && len(v) == 12:
				s.table.Epoch, crc = bigEndian.Uint64(v), bigEndian.Uint32(v[8:])
				bad = s.table.Epoch == 0
			case len(name) == 4 && len(v) == 14:
				dp := chunkstore.DeltaPool{ID: core.PoolID(bigEndian.Uint32(name)), Replicas: v[0], MinLive: v[1], PGCount: bigEndian.Uint32(v[2:]), Split: bigEndian.Uint64(v[6:])}
				bad = dp.Replicas == 0 || dp.MinLive == 0 || dp.MinLive > dp.Replicas || core.CheckPGCount(dp.PGCount) != nil || dp.Split > s.table.Epoch
				s.table.Pools = append(s.table.Pools, chunkstore.PoolTable{ID: dp.ID, Replicas: dp.Replicas, MinLive: dp.MinLive, Split: dp.Split})
				count = append(count, dp.PGCount)
			case len(name) == 8:
				j := len(s.table.Pools) - 1
				if j < 0 || s.table.Pools[j].ID != core.PoolID(bigEndian.Uint32(name)) || bigEndian.Uint32(name[4:]) != uint32(len(s.table.Pools[j].PGs)) || len(s.table.Pools[j].PGs) >= int(count[j]) {
					return s, ErrMalformed
				}
				var ev []byte
				var err error
				if i, ev, err = joinSplit(kvs, i); err != nil {
					return s, err
				}
				e, err := chunkstore.DecodeEntry(ev)
				bad = err != nil
				s.table.Pools[j].PGs = append(s.table.Pools[j].PGs, e)
			default:
				bad = true
			}
		case CtlMembership:
			switch {
			case len(name) == 0 && len(v) == 8:
				s.diskV = bigEndian.Uint64(v)
				bad = s.diskV == 0
			case len(name) == 4 && bigEndian.Uint32(name) == uint32(len(s.disks)+1):
				e, err := decodeDisk(bigEndian.Uint32(name), v)
				bad = err != nil
				s.disks = append(s.disks, e)
			default:
				bad = true
			}
		case CtlSession:
			if len(name) != 8 {
				return s, ErrMalformed
			}
			if id := bigEndian.Uint64(name); id == 0 {
				bad = len(v) != 24
				if !bad {
					s.sess.Next, s.sess.Version, s.sess.Acked = bigEndian.Uint64(v), bigEndian.Uint64(v[8:]), bigEndian.Uint64(v[16:])
					bad = s.sess.Next < 2 || s.sess.Acked > s.sess.Version
				}
			} else {
				e, err := decodeSession(id, v)
				bad = err != nil || id >= s.sess.Next || e.Fenced > s.sess.Version
				s.sess.Sessions = append(s.sess.Sessions, e)
			}
		case CtlFormat:
			d := decoder{b: v}
			f := d.uvarint()
			bad = len(name) != 0 || d.bad || len(d.b) != 0 || f == 0 || f > math.MaxUint32 || !bytes.Equal(binary.AppendUvarint(nil, f), v)
			s.format = uint32(f)
		case CtlShardMap:
			switch {
			case len(name) == 0 && len(v) == 8:
				s.smapV = bigEndian.Uint64(v)
			case len(name) == 8:
				var gv []byte
				var err error
				if i, gv, err = joinSplit(kvs, i); err != nil {
					return s, err
				}
				g, err := decodeGroup(bigEndian.Uint64(name), gv)
				bad = err != nil
				s.smap.Groups = append(s.smap.Groups, g)
			default:
				bad = true
			}
		}
		if bad {
			return s, ErrMalformed
		}
	}
	for j, p := range s.table.Pools {
		if len(p.PGs) != int(count[j]) {
			return s, ErrMalformed
		}
	}
	if s.table.Epoch == 0 && len(s.table.Pools) > 0 || s.table.Epoch != 0 && s.table.CRC() != crc {
		return s, ErrMalformed
	}
	s.crc = crc
	logs := make([]uint64, len(s.disks))
	for i, e := range s.disks {
		logs[i] = e.Log
	}
	slices.Sort(logs)
	if len(s.disks) > 0 && s.diskV == 0 || len(slices.Compact(logs)) != len(s.disks) {
		return s, ErrMalformed
	}
	slices.SortFunc(s.smap.Groups, func(a, b Group) int { return bytes.Compare(a.Start, b.Start) })
	if len(s.smap.Groups) > 0 && (s.smapV == 0 || s.smap.CheckRanges() != nil) {
		return s, ErrMalformed
	}
	return s, nil
}

// A pool's settings: replicas, min_live, data, parity, max_per_domain and pg_count as uvarints,
// overcommit and write_limit as bytes, then failure_domain, locality and disk class as length and bytes.
func appendPool(dst []byte, p *chunkstore.Pool) []byte {
	for _, n := range [...]int{p.Replicas, p.MinLive, p.Data, p.Parity, p.MaxPerDomain, int(p.PGCount)} {
		dst = binary.AppendUvarint(dst, uint64(n))
	}
	over := byte(0)
	if p.Overcommit {
		over = 1
	}
	dst = append(dst, over, p.WriteLimit)
	for _, s := range [...]string{p.FailureDomain, p.Locality, p.DiskClass} {
		dst = appendBytes(dst, []byte(s))
	}
	return dst
}

// decodePool decodes a pool's settings; ErrMalformed for any the pool rules refuse or appendPool would not write.
func decodePool(b []byte) (chunkstore.Pool, error) {
	d := decoder{b: b}
	var n [6]uint64
	for i := range n {
		if n[i] = d.uvarint(); n[i] > math.MaxInt32 {
			d.bad = true
		}
	}
	over, wl := d.byte(), d.byte()
	p := chunkstore.Pool{Replicas: int(n[0]), MinLive: int(n[1]), Data: int(n[2]), Parity: int(n[3]), MaxPerDomain: int(n[4]), PGCount: uint32(n[5]), Overcommit: over == 1, WriteLimit: wl}
	p.FailureDomain, p.Locality, p.DiskClass = string(d.bytes()), string(d.bytes()), string(d.bytes())
	if d.bad || len(d.b) != 0 || p.Check() != nil || !bytes.Equal(appendPool(nil, &p), b) {
		return chunkstore.Pool{}, ErrMalformed
	}
	return p, nil
}

// A group's shard map entry: its range start as length and bytes, its last replica ID, its voter
// count, then each voter's replica ID, node and disk, all as uvarints.
func appendGroup(dst []byte, g *Group) []byte {
	dst = binary.AppendUvarint(appendBytes(dst, g.Start), g.LastID)
	dst = binary.AppendUvarint(dst, uint64(len(g.Voters)))
	for _, v := range g.Voters {
		dst = binary.AppendUvarint(binary.AppendUvarint(binary.AppendUvarint(dst, v.ID), uint64(v.Node)), uint64(v.Disk))
	}
	return dst
}

// freshAfter reports whether g's voters are distinct, none above g's last ID, and, when g follows
// old (ok), its last ID is no lower and each voter old's plan lacks is above old's last ID, and a
// plan that differs from old's names one: a plan of voters some earlier configuration had would let
// a stale report of that configuration complete it.
func (g *Group) freshAfter(old *Group, ok bool) bool {
	fresh := false
	for i, v := range g.Voters {
		if v.ID == 0 || v.ID > g.LastID || slices.ContainsFunc(g.Voters[:i], func(o Replica) bool { return o.ID == v.ID }) {
			return false
		}
		if !ok || slices.ContainsFunc(old.Voters, func(o Replica) bool { return o.ID == v.ID }) {
			continue
		}
		if v.ID <= old.LastID {
			return false
		}
		fresh = true
	}
	return !ok || g.LastID >= old.LastID && (fresh || len(g.Voters) == len(old.Voters))
}

// decodeGroup decodes group id's entry; ErrMalformed for a start over MaxKey, a voter ID above the
// last one handed out or repeated, or anything appendGroup would not write.
func decodeGroup(id uint64, b []byte) (Group, error) {
	d := decoder{b: b}
	g := Group{ID: id, Start: slices.Clone(d.bytes()), LastID: d.uvarint()}
	n := d.uvarint()
	for i := uint64(0); i < n && !d.bad; i++ {
		v := Replica{ID: d.uvarint(), Node: uint32(d.uvarint()), Disk: uint32(d.uvarint())}
		if v.ID == 0 || v.ID > g.LastID || slices.ContainsFunc(g.Voters, func(o Replica) bool { return o.ID == v.ID }) {
			d.bad = true
		}
		g.Voters = append(g.Voters, v)
	}
	if d.bad || len(d.b) != 0 || len(g.Start) > MaxKey || !bytes.Equal(appendGroup(nil, &g), b) {
		return Group{}, ErrMalformed
	}
	return g, nil
}
