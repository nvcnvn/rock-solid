package meta

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/lease"
	"github.com/nvcnvn/rock-solid/internal/lease/leasetest"
	"github.com/nvcnvn/rock-solid/internal/lsm"
	"github.com/nvcnvn/rock-solid/internal/raftshard"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

const invReadsCurrent = "a Read's answer holds every Txn acknowledged before the Read was sent"

const (
	// readKeys is how many keys the run's Txns write, each by one client.
	readKeys = 4
	// readClients are nodes 3 and 4.
	readClients = 2
	readFor     = core.Tick(30_000_000)
	// readGap is how often a client sends; readIsolate how long the run cuts a leader off, at most
	// readCutGap after the last heal; readRebootGap the most time between two reboots.
	readGap        = core.Tick(5_000)
	readIsolate    = core.Tick(6_000_000)
	readCutGap     = core.Tick(6_000_000)
	readRebootGap  = core.Tick(16_000_000)
	readRouter     = core.TimerID(0x726f7574)
	readClientTick = core.TimerID(0x7469636b)
)

// readRun runs a three-replica shard on nodes 0 to 2, each answering Txns and Reads through a
// Server and renewing node-pair leases with the others, and two clients on nodes 3 and 4, each
// sending through a Client, every readGap, a Put of a key of its own with a value above the last,
// one in flight per key, and a Read of every key. Faults: a random replica's node rebooted up to
// readRebootGap apart, and, up to readCutGap after the last heal, the leader of the moment cut off
// for readIsolate from the other replicas and from one client, while the other client still
// reaches it.
type readRun struct {
	t     testing.TB
	s     *sim.Scheduler
	rng   *core.Rand
	plant readPlants
	inv   sim.Invariants
	reps  [3]*readRep
	cls   [readClients]*readClient
	// acked is, per key, the highest value acknowledged; next the next value to write.
	acked             [readKeys]uint64
	next              uint64
	rebootAt, faultAt core.Tick
	healAt            core.Tick
	cut               [][2]uint32
	served            int
	err               error
}

// readKey is key k of the run.
func readKey(k int) []byte { return attr(1, 2, uint64(k)+1) }

func newReadRun(t testing.TB, seed uint64, p readPlants) *readRun {
	set := sim.DefaultSettings()
	set.PoolBytes = 16 << 20
	r := &readRun{t: t, s: sim.New(seed, set), rng: core.NewRand(seed), plant: p}
	r.rebootAt = core.Tick(r.rng.Below(uint64(readRebootGap)))
	r.faultAt = core.Tick(r.rng.Below(uint64(readCutGap)))
	r.inv.Register("driver contract", r.s.DriverContract)
	r.inv.Register(invReadsCurrent, func() error { return r.err })
	for i := range 3 {
		r.reps[i] = &readRep{r: r, i: i}
		r.s.AddNode(storageClock, []sim.DiskSpec{{Blocks: 16, Settings: sim.DefaultDiskSettings()}}, r.reps[i].boot)
	}
	for i := range readClients {
		r.cls[i] = &readClient{r: r, i: i, inflight: map[int]bool{}, txns: map[uint64]readTxn{}, reads: map[uint64][readKeys]uint64{}}
		r.s.AddNode(sim.DefaultClock, nil, r.cls[i].boot)
	}
	return r
}

// storageClock starts the replicas' nodes' clocks past a lease, so a new group's grantors, which
// start at 0, have no restart wait left.
var storageClock = sim.Clock{Now: 5_000_000, Raw: 5_000_226}

func (r *readRun) run() error {
	for steps := 0; r.s.Time() < readFor; steps++ {
		r.faults()
		if !r.s.Step() {
			break
		}
		if steps%1000 == 999 {
			if err := r.inv.Check(); err != nil {
				return err
			}
		}
	}
	if err := r.inv.Check(); err != nil {
		return err
	}
	if r.served == 0 {
		return errors.New("no Read was answered")
	}
	return nil
}

// leader is the replica that leads the newest term, or -1.
func (r *readRun) leader() int {
	l, term := -1, uint64(0)
	for i, m := range r.reps {
		if m.s == nil || !r.s.Node(uint32(i)).Up() {
			continue
		}
		if rf := m.s.Raft(); rf.Leader() == uint64(i)+1 && rf.Term() > term {
			l, term = i, rf.Term()
		}
	}
	return l
}

func (r *readRun) faults() {
	now := r.s.Time()
	if r.healAt != 0 && now >= r.healAt {
		for _, c := range r.cut {
			r.s.Heal(c[0], c[1])
		}
		r.cut, r.healAt = nil, 0
		r.faultAt = now + core.Tick(r.rng.Below(uint64(readCutGap)))
	}
	if r.healAt == 0 && now >= r.faultAt {
		if l := r.leader(); l >= 0 {
			r.cut = append(r.cut, [2]uint32{uint32(l), uint32(l+1) % 3}, [2]uint32{uint32(l), uint32(l+2) % 3}, [2]uint32{uint32(l), uint32(3 + r.rng.Below(readClients))})
			for _, c := range r.cut {
				r.s.Partition(c[0], c[1], false)
			}
			r.healAt = now + readIsolate
		} else {
			r.faultAt = now + 100_000
		}
	}
	if now >= r.rebootAt {
		r.rebootAt = now + core.Tick(r.rng.Below(uint64(readRebootGap)))
		if i := uint32(r.rng.Below(3)); r.s.Node(i).Up() {
			r.s.Reboot(i)
		}
	}
}

// readRep is a replica's node: its shard, the server answering Txns and Reads, its node-pair
// leases, and the rounds its disk made durable, which a restart replays.
type readRep struct {
	r      *readRun
	i      int
	s      *Shard
	sv     *Server
	pair   *leasetest.Pair
	out    Output
	recs   [][]byte
	flight []byte
}

func (m *readRep) boot(n *sim.Node) {
	if m.s != nil {
		m.s.Close()
	}
	st := bootStorage(m.r.t, raftpb.ConfState{Voters: []uint64{1, 2, 3}})
	for _, rec := range m.recs {
		if err := st.Replay(disklog.BlobRaftState, append([]byte{1}, rec...)); err != nil {
			m.r.t.Fatal(err)
		}
	}
	rng := n.Fork()
	first := m.s == nil
	m.flight, m.sv = nil, NewServer()
	m.sv.plant = m.r.plant
	loop := n.Start(sim.MachineFunc(m.onEvent), n.Disk(0))
	n.Call(loop, func(ctx *core.Context, acts []core.Action) []core.Action {
		start := lease.Now(ctx)
		if first {
			start = lease.Time{}
		}
		h, g := lease.NewHolder(lease.DefaultConfig), lease.NewGrantor(lease.DefaultConfig, rng.Uint64(), start)
		cfg := Config{Raft: raftshard.Config{Group: dataGroup, ID: uint64(m.i) + 1, Storage: st, Seed: rng.Uint64(), Timer: 1, Holder: h, Grantor: g},
			Tree: lsm.Config{MaxPiece: 64 << 10}}
		var peers []core.NodeID
		for j := range 3 {
			cfg.Raft.Peers = append(cfg.Raft.Peers, raftshard.Peer{ID: uint64(j) + 1, Node: core.NodeID{Node: uint32(j)}})
			if j != m.i {
				peers = append(peers, core.NodeID{Node: uint32(j)})
			}
		}
		m.pair = leasetest.New(h, g, peers, rng)
		var err error
		if m.s, err = NewShard(cfg); err != nil {
			m.r.t.Fatal(err)
		}
		m.out.Reset()
		m.s.Start(ctx, &m.out)
		return m.finish(ctx, acts)
	})
}

func (m *readRep) onEvent(ctx *core.Context, ev *core.Event, acts []core.Action) []core.Action {
	m.out.Reset()
	acts, isLease := m.pair.OnEvent(ctx, ev, acts)
	_, kind, _, _ := Peek(&ev.Msg)
	switch {
	case isLease:
		return acts
	case ev.Kind == core.EvNetRecv && kind == KindTxn:
		acts = m.sv.Serve(ctx, ev, m.s, &m.out, acts)
	case ev.Kind == core.EvNetRecv && kind == KindRead:
		acts = m.sv.ServeRead(ctx, ev, m.s, acts)
	case ev.Kind == core.EvDiskDone:
		if ev.Err != core.DiskOK {
			panic(ev.Err)
		}
		m.recs, m.flight = append(m.recs, m.flight), nil
		m.s.Durable(ctx, uint64(ev.IO), &m.out)
	default:
		if ev.Kind == core.EvTimer {
			acts = m.pair.Tick(ctx, acts)
		}
		m.s.OnEvent(ctx, ev, &m.out)
	}
	return m.finish(ctx, acts)
}

func (m *readRep) finish(ctx *core.Context, acts []core.Action) []core.Action {
	acts = m.sv.Reply(ctx, m.out.Answers, acts)
	acts = append(acts, m.out.Raft.Actions...)
	if f := m.out.Raft.Flush; f != 0 {
		m.flight = raftshard.AppendRound(nil, &m.out.Raft.Round)
		acts = append(acts, core.Action{Kind: core.ActDiskFsync, IO: core.IOID(f)})
	}
	return acts
}

// readClient is a client node: its Client, the Txns and Reads it has in flight, and for each Read
// the values acknowledged when it was sent.
type readClient struct {
	r        *readRun
	i        int
	c        *Client
	out      RouterOutput
	inflight map[int]bool
	txns     map[uint64]readTxn
	reads    map[uint64][readKeys]uint64
}

type readTxn struct {
	key int
	val uint64
}

func (c *readClient) boot(n *sim.Node) {
	m := oneShard(0)
	rt, err := NewRouter(&m, readRouter)
	if err != nil {
		c.r.t.Fatal(err)
	}
	c.c = NewClient(rt, 1)
	loop := n.Start(sim.MachineFunc(c.onEvent), nil)
	n.Call(loop, func(ctx *core.Context, acts []core.Action) []core.Action {
		return append(acts, core.Action{Kind: core.ActSetTimer, Timer: readClientTick, At: ctx.Now + readGap})
	})
}

func (c *readClient) onEvent(ctx *core.Context, ev *core.Event, acts []core.Action) []core.Action {
	c.out.Reset()
	if ev.Kind == core.EvTimer && ev.Timer == readClientTick {
		c.send(ctx)
		acts = append(acts, core.Action{Kind: core.ActSetTimer, Timer: readClientTick, At: ctx.Now + readGap})
	} else {
		c.c.OnEvent(ctx, ev, &c.out)
	}
	c.settle()
	return append(acts, c.out.Actions...)
}

// send sends a Put of each of the client's keys with none in flight, and a Read of every key.
func (c *readClient) send(ctx *core.Context) {
	r := c.r
	for k := c.i; k < readKeys; k += readClients {
		if c.inflight[k] {
			continue
		}
		r.next++
		tok, err := c.c.Txn(ctx, &Txn{Ops: []Op{Put(readKey(k), binary.BigEndian.AppendUint64(nil, r.next))}}, &c.out)
		if err != nil {
			r.t.Fatal(err)
		}
		c.inflight[k], c.txns[tok] = true, readTxn{k, r.next}
	}
	if len(c.reads) < 4 {
		tok, err := c.c.Read(ctx, &Read{Start: readKey(0), End: readKey(readKeys), Limit: 100}, &c.out)
		if err != nil {
			r.t.Fatal(err)
		}
		c.reads[tok] = r.acked
	}
}

// settle takes the answers the client's last call gave.
func (c *readClient) settle() {
	r := c.r
	for _, a := range c.out.Answers {
		tx, ok := c.txns[a.Token]
		if !ok {
			continue
		}
		delete(c.txns, a.Token)
		c.inflight[tx.key] = false
		if a.Err == nil {
			r.acked[tx.key] = max(r.acked[tx.key], tx.val)
		}
	}
	for _, a := range c.out.Reads {
		need, ok := c.reads[a.Token]
		if !ok {
			continue
		}
		delete(c.reads, a.Token)
		if a.Err != nil {
			continue
		}
		r.served++
		got := [readKeys]uint64{}
		for _, kv := range a.KVs {
			for k := range readKeys {
				if bytes.Equal(kv.Key, readKey(k)) {
					got[k] = binary.BigEndian.Uint64(kv.Value)
				}
			}
		}
		for k := range readKeys {
			if got[k] < need[k] && r.err == nil {
				r.err = fmt.Errorf("at %d client %d read key %d at %d, acknowledged at %d before the Read was sent", r.s.Time(), c.i, k, got[k], need[k])
			}
		}
	}
}

// Every Read's answer holds every Txn acknowledged before it was sent, with leaders cut off from
// their peers and one client while the other client still reaches them, and replicas rebooted.
func TestReadsSeeEveryTxnAcknowledgedBeforeThem(t *testing.T) {
	seeds.Run(t, 1, 20, func(seed uint64) {
		if err := newReadRun(t, seed, readPlants{}).run(); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
	})
}

// A leader that answers Reads without its lease, and a follower that answers them, each break the
// Read run.
func TestPlantedLeaseReadBugsBreakTheMetaReadRun(t *testing.T) {
	for _, p := range []readPlants{{noLease: true}, {anyReplica: true}} {
		caught := false
		for seed := uint64(1); seed <= 5 && !caught; seed++ {
			var v *sim.Violation
			caught = errors.As(newReadRun(t, seed, p).run(), &v) && v.Name == invReadsCurrent
		}
		if !caught {
			t.Errorf("plant %+v passed 5 seeds", p)
		}
	}
}
