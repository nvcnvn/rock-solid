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
	staged          []PoolRecord
	stagedMap       *ShardMap
	stagedFormat    uint32
	asks, acts, rcv []chunkstore.PGInterval
	pending         *commit
	rec             chunkstore.DiskRecord
	det             *chunkstore.Detector
	pin             []chunkstore.PoolInputs
	drains          []uint32
	exits           []diskExit
	nodes           []nodeState
	// round counts failovers and error answers; resend is set while every committed shard-map
	// entry is still to be handed out again.
	round  uint64
	resend bool
	starts []SessionStart
	timers []sessTimer
	// seen is the time of the last input, leased whether the leader held its lease then, and
	// tableTurn whether a session commit went last, so the table's step goes next.
	seen      lease.Time
	leased    bool
	tableTurn bool
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
}

// commit is the Txn in flight: the state it gives and how much of the staged reports it took.
type commit struct {
	next             ctlState
	token            uint64
	table, mapStaged bool
	delta            chunkstore.Delta
	plans            []Group
	asks, acts, rc   int
	fenced           bool
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
// stages it for the next commit in place of any change of the pool staged before; disks are the
// cluster's, for an unset pg_count and the layout check. A refused one stages nothing, and one that
// changes nothing unstages the pool. A new pool whose worst step may be more than one Txn holds is
// ErrTooLarge.
func (c *Control) SetPool(id core.PoolID, spec chunkstore.PoolSpec, disks []chunkstore.Disk) error {
	i, ok := slices.BinarySearchFunc(c.pools, id, cmpPool)
	if ok && spec.PGCount == nil {
		spec.PGCount = &c.pools[i].Pool.PGCount
	}
	p, err := chunkstore.NewPool(spec, len(disks))
	if err == nil && ok {
		err = changePool(c.pools[i].Pool, p)
	}
	if err == nil {
		_, err = p.CheckLayout(disks)
	}
	if err == nil && !ok {
		err = fits(PoolRecord{id, p}, disks)
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

// changePool checks next as the settings that replace committed ones.
func changePool(old, next chunkstore.Pool) error {
	if next.PGCount != old.PGCount {
		return &chunkstore.SettingError{Setting: "pg_count", Reason: "is fixed until pool splits are built"}
	}
	return old.Change(next)
}

// fits reports ErrTooLarge when a step of pool cp alone may be more than one Txn holds: its
// refill after losing every third node, the largest step measured, as design.md's "One step is at
// most max_ctl_txn_bytes" states, beside a disk-record entry of every disk.
func fits(cp PoolRecord, disks []chunkstore.Disk) error {
	t, _ := chunkstore.Next(&chunkstore.Table{}, &chunkstore.Inputs{Pools: []chunkstore.PoolInputs{poolInputs(cp, disks)}})
	var ops []Op
	var err error
	for i := range t.Pools[0].PGs {
		pg := core.PGID{Pool: cp.ID, Index: uint32(i)}
		if ops, err = putSplit(ops, entryKey(pg), chunkstore.AppendEntry(nil, &t.Pools[0].PGs[i]), 0); err != nil {
			return err
		}
	}
	n, err := (&Txn{Ops: ops}).Check()
	if err != nil {
		return err
	}
	dops := []Op{Put(ctlKey(CtlMembership, nil), bigEndian.AppendUint64(nil, 0))}
	for _, d := range disks {
		dops = append(dops, Put(diskKey(d.ID), appendDisk(nil, &DiskEntry{RecordedDisk: chunkstore.RecordedDisk{ID: d.ID, Node: d.Node}, Age: ^core.Tick(0)})))
	}
	m, err := (&Txn{Ops: dops}).Check()
	// ponytail: refill = 2.2 × creation, the most measured (rep3 2.175, rep2 up to 2.05); item 5
	// lifts this limit by committing a step over several tables.
	if err == nil && n*11/5+m > MaxCtlTxnBytes {
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

// SetShardMap stages m, whose ranges CheckRanges accepts, in place of the shard map.
func (c *Control) SetShardMap(m *ShardMap) error {
	if err := m.CheckRanges(); err != nil {
		return err
	}
	m2 := ShardMap{Groups: slices.Clone(m.Groups)}
	c.stagedMap = &m2
	return nil
}

// Report folds a heartbeat's activation and recovered reports into the next table.
func (c *Control) Report(rp *chunkstore.Report) {
	c.acts = c.fold(c.acts, rp.Activations)
	c.rcv = c.fold(c.rcv, rp.Recovered)
}

// Ask takes a primary's ask for a new interval of its PG, as Report takes a report.
func (c *Control) Ask(f chunkstore.PGFields) {
	c.asks = c.fold(c.asks, []chunkstore.PGFields{f})
}

func (c *Control) fold(dst []chunkstore.PGInterval, fs []chunkstore.PGFields) []chunkstore.PGInterval {
	for _, f := range fs {
		dst = append(dst, chunkstore.PGInterval{PG: f.PG, Interval: f.Interval})
	}
	return dst
}

// Step computes the next table and shard map and, with no commit in flight, proposes under token
// whatever they and the staged pools change, or what the session table owes, the two taking turns.
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
		if ok, err := c.sessionStep(lt, token, out); ok || err != nil {
			c.tableTurn = ok
			return err
		}
	}
	if err := c.recheck(); err != nil {
		return err
	}
	if err := c.reapply(); err != nil {
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
	n.table, _ = chunkstore.Next(&c.table, &chunkstore.Inputs{Now: in.Now, Pools: pin, Asks: c.asks, Activations: c.acts, Recovered: c.rcv, Edits: in.Edits, Moves: in.Moves})
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
	base := &c.smap
	if c.stagedMap != nil {
		base, p.mapStaged = c.stagedMap, true
	}
	live := c.liveRecord()
	n.smap = PlaceVoters(base, c.metaNodes(), live, c.halts(), in.Now)
	n.smapV = c.smapV
	n.disks, n.diskV, n.format, n.sess = c.diskView(live, in.Now), c.diskV, max(c.format, c.stagedFormat), c.sess
	var ops []Op
	var err error
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
	ops = c.memberOps(ops, n)
	if !p.table && len(ops) == 0 && len(c.staged) == 0 {
		c.asks, c.acts, c.rcv, c.stagedMap = c.asks[:0], c.acts[:0], c.rcv[:0], nil
		c.resendPlans(out)
		if turn {
			_, err := c.sessionStep(lt, token, out)
			return err
		}
		return nil
	}
	if ops, err = c.tableOps(ops, p); err != nil {
		return err
	}
	for _, cp := range c.staged {
		ops = append(ops, Put(poolKey(cp.ID), appendPool(nil, &cp.Pool)))
	}
	t := Txn{Ops: append(c.guards(n.pools), ops...)}
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
		if err := changePool(c.pools[i].Pool, s.Pool); err != nil {
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
		err := c.load(st)
		c.det, c.round, c.resend = nil, c.round+1, true
		c.resendPlans(out)
		return err
	}
	c.ctlState = p.next
	c.exits = slices.DeleteFunc(c.exits, func(x diskExit) bool { return c.disks[x.id-1].Gone })
	c.asks, c.acts, c.rcv = c.asks[p.asks:], c.acts[p.acts:], c.rcv[p.rc:]
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
	for _, p := range t.Pools {
		dst = append(dst, chunkstore.DeltaPool{ID: p.ID, Replicas: p.Replicas, MinLive: p.MinLive, PGCount: uint32(len(p.PGs))})
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
	return bigEndian.AppendUint32([]byte{p.Replicas, p.MinLive}, uint32(len(p.PGs)))
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
			case len(name) == 4 && len(v) == 6:
				dp := chunkstore.DeltaPool{ID: core.PoolID(bigEndian.Uint32(name)), Replicas: v[0], MinLive: v[1], PGCount: bigEndian.Uint32(v[2:])}
				bad = dp.Replicas == 0 || dp.MinLive == 0 || dp.MinLive > dp.Replicas || core.CheckPGCount(dp.PGCount) != nil
				s.table.Pools = append(s.table.Pools, chunkstore.PoolTable{ID: dp.ID, Replicas: dp.Replicas, MinLive: dp.MinLive})
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
