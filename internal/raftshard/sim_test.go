package raftshard

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/lease"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/clock"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

const (
	simNodes  = 3
	crashAt   = 20
	simRunFor = core.Tick(30_000_000)
)

// simCluster is one Raft group of three shards on the simulator, each flushing its rounds as an
// fsync on its node's disk and reading its node's node-pair leases, which the test's lease frames
// renew. Leaders propose a numbered value per tick, the first leader to apply crashAt values
// crashes, and the run is done once a value proposed after the crash is applied.
type simCluster struct {
	s       *sim.Scheduler
	rng     *core.Rand
	plant   plants
	inv     sim.Invariants
	storage []*raft.MemoryStorage
	reps    []*simReplica
	// rep is the voter replacement the run makes, if any.
	rep *replacement
	// crashes is whether the first leader to apply crashAt values crashes; afterCrash whether a value proposed after that crash, if any, applied.
	crashes    bool
	afterCrash bool
	promoted   error
	// log is every applied entry by index, and history every application in order.
	log        []raftpb.Entry
	history    []byte
	next       uint64
	crashed    bool
	sinceCrash uint64
	done       bool
	// crashedAt is when the leader crashed, crashedID its replica, and campaignAt when another replica first asked for a pre-vote after it.
	crashedAt, campaignAt core.Tick
	crashedID             int
	early                 error
	disagree              error
	// leaders is each term's leader, maxApplied the highest index any replica applied while it led, and leaseReads the passes that could serve a lease read.
	leaders    map[uint64]int
	maxApplied uint64
	leaseReads int
	twoLeaders error
	overlap    error
	stale      error
	removed    error
}

type simReplica struct {
	c       *simCluster
	id      int
	s       *Shard
	out     Output
	applied int
	leads   bool
	// index is the last index it applied, clocks its clocks at its last pass, and the rest its node-pair leases.
	index   uint64
	clocks  core.Clocks
	rng     *core.Rand
	holder  *lease.Holder
	grantor *lease.Grantor
	leaseID uint64
	beatAt  core.Tick
}

func newSimCluster(seed uint64, p plants) *simCluster {
	set := sim.DefaultSettings()
	set.NetJitter = 200
	set.DropNum, set.DropDen = 1, 100
	set.DupNum, set.DupDen = 1, 50
	set.PartitionNum, set.PartitionDen = 1, 3000
	set.ClockNum, set.ClockDen = 1, 2000
	return newSimClusterOn(seed, p, set)
}

// newSimClusterOn is newSimCluster on the scheduler settings set.
func newSimClusterOn(seed uint64, p plants, set sim.Settings) *simCluster {
	return newCluster(seed, p, set, nil)
}

// newCluster runs a group of three, and with rep a fourth node whose fresh replica, ID 4, replaces one of the three.
func newCluster(seed uint64, p plants, set sim.Settings, rep *replacement) *simCluster {
	set.MsgType = simMsgType
	c := &simCluster{s: sim.New(seed, set), rng: core.NewRand(seed), plant: p, next: 1, leaders: map[uint64]int{}, rep: rep, crashes: rep == nil || rep.crash}
	c.afterCrash = !c.crashes
	c.inv.Register("driver contract", c.s.DriverContract)
	c.inv.Register("acknowledged only once durable", func() error { return c.early })
	c.inv.Register("applied entries agree", func() error { return c.disagree })
	c.inv.Register(invOneLeader, func() error { return c.twoLeaders })
	c.inv.Register(invOneLeaseReader, func() error { return c.overlap })
	c.inv.Register(invLeaseReadsCurrent, func() error { return c.stale })
	c.inv.Register(invCaughtUp, func() error { return c.promoted })
	c.inv.Register(invOnlyRemoved, func() error { return c.removed })
	nodes := simNodes
	if rep != nil {
		nodes++
	}
	c.storage, c.reps = make([]*raft.MemoryStorage, nodes), make([]*simReplica, nodes)
	disk := []sim.DiskSpec{{Blocks: 16, Settings: sim.DefaultDiskSettings()}}
	for id := range nodes {
		// The storage outlives crashes: it is what the replica's rounds made durable.
		c.storage[id] = bootStorage(simNodes)
		if id == simNodes {
			c.storage[id] = raft.NewMemoryStorage()
		}
		c.s.AddNode(sim.DefaultClock, disk, func(n *sim.Node) { c.boot(n, id) })
	}
	return c
}

func (c *simCluster) boot(n *sim.Node, id int) {
	rng := n.Fork()
	cfg := testConfig(len(c.reps), id, c.storage[id], rng.Uint64())
	r := &simReplica{c: c, id: id, rng: rng, holder: lease.NewHolder(lease.DefaultConfig)}
	c.reps[id] = r
	loop := n.Start(sim.MachineFunc(r.onEvent), n.Disk(0))
	n.Call(loop, func(ctx *core.Context, acts []core.Action) []core.Action {
		r.grantor = lease.NewGrantor(lease.DefaultConfig, rng.Uint64(), lease.Now(ctx))
		cfg.Holder, cfg.Grantor = r.holder, r.grantor
		s, err := newShard(cfg, c.plant)
		if err != nil {
			panic(err)
		}
		r.s = s
		if c.rep != nil {
			s.SetPlan(c.rep.plan)
		}
		r.out.Reset()
		s.Start(ctx, &r.out)
		return r.finish(ctx, acts)
	})
}

// run steps the group under faults for simRunFor, then, if it is not done, heals every partition
// and runs it with only drops left for simRunFor more, since a partition that never heals stops it.
func (c *simCluster) run(t *testing.T, seed uint64) *simCluster {
	for c.s.Time() < simRunFor && c.s.Step() {
		c.pause()
		c.after(t, seed)
	}
	for a := range uint32(len(c.reps)) {
		for b := range uint32(len(c.reps)) {
			c.s.Heal(a, b)
		}
	}
	for end := c.s.Time() + simRunFor; c.s.Time() < end && !c.done; {
		cs := c.s.Choices()
		if len(cs) == 0 {
			break
		}
		c.s.Take(cs[0])
		c.after(t, seed)
	}
	return c
}

// pauseStops are the stops a run draws: every kind but a stop in place on kvm-clock, which no
// Resumed reports and whose lost heartbeats leave a holder serving on an old lease, an open question.
var pauseStops = []clock.Stop{clock.Suspend, clock.PauseCounted, clock.Restore, clock.HeldMigration}

// pause stands a running node still, one step in 20,000, for up to 5 s, the messages sent to it
// queued or lost, and one step in 2,000 holds one direction of a link for up to 50 ms more.
func (c *simCluster) pause() {
	n := uint64(len(c.reps))
	if c.rng.Chance(1, 2000) {
		from := uint32(c.rng.Below(n))
		c.s.Queue(from, (from+1+uint32(c.rng.Below(n-1)))%uint32(n), core.Tick(c.rng.Below(50_001)))
	}
	if !c.rng.Chance(1, 20000) {
		return
	}
	id := uint32(c.rng.Below(n))
	if nd := c.s.Node(id); nd.Up() && !nd.Paused() {
		c.s.Pause(id, pauseStops[c.rng.Below(uint64(len(pauseStops)))], 1+core.Tick(c.rng.Below(5_000_000)), c.rng.Chance(1, 2))
	}
}

// after checks the invariants and crashes the first leader to apply crashAt values.
func (c *simCluster) after(t *testing.T, seed uint64) {
	if err := c.inv.Check(); err != nil {
		t.Fatalf("seed %d step %d: %v", seed, c.s.Steps(), err)
	}
	c.replace()
	if c.crashed || !c.crashes {
		return
	}
	for id, r := range c.reps {
		if c.s.Node(uint32(id)).Up() && r.leads && r.applied >= crashAt {
			c.s.CrashRandom(uint32(id))
			c.crashed, c.sinceCrash, c.crashedAt, c.crashedID = true, c.next, c.s.Time(), id
			return
		}
	}
}

// With no faults but the crash, the followers of a leader whose node crashes and restarts in place
// campaign on its grants' lapse: within primary_lease, a heartbeat for the last renewal and
// raft_lapse_campaign of the crash, and not only once the restarted replica campaigns after its
// restart wait and election timeout. The run still applies a value proposed after the crash.
func TestALeaderRestartedInPlaceFailsOverOnTheLapse(t *testing.T) {
	seeds.Run(t, 0, 20, func(seed uint64) {
		c := newSimClusterOn(seed, plants{}, sim.DefaultSettings())
		for !c.done && c.s.Time() < simRunFor && c.s.Step() {
			c.after(t, seed)
		}
		bound := lease.DefaultConfig.Lease + lease.DefaultConfig.Heartbeat + LapseCampaignMax + 2*TickInterval
		if !c.done || c.campaignAt == 0 || c.campaignAt-c.crashedAt > bound {
			t.Fatalf("seed %d: crashed at %d, a follower campaigned at %d, past %d, or no later value applied (%v)", seed, c.crashedAt, c.campaignAt, bound, c.done)
		}
	})
}

func simMsgType(m *core.Msg) uint64 {
	b := m.Inline()
	switch {
	case m.Buf != core.NoBuf:
		return 1 << 8
	case b[groupBytes] != kindRaft:
		return 1<<9 | uint64(b[groupBytes])
	}
	var pm raftpb.Message
	if err := pm.Unmarshal(b[kindBytes:]); err != nil {
		panic(err)
	}
	return uint64(pm.Type)
}

// Lease frames carry the test's node-pair leases: leaseGroup, a kind, then a request ID, the grantor's run ID, raw reading and support epoch.
const (
	leaseGroup                = 0
	leaseAsk             byte = 'a'
	leaseGrant           byte = 'g'
	leaseBeat            byte = 'h'
	leaseFrameBytes           = kindBytes + 4*8
	invOneLeader              = "at most one leader per shard per term"
	invOneLeaseReader         = "no two replicas of a shard serve lease reads at overlapping times"
	invLeaseReadsCurrent      = "a lease read sees every applied entry"
)

func (r *simReplica) sendLease(kind byte, to core.NodeID, id, run uint64, raw core.Tick, support uint64) {
	a := core.Action{Kind: core.ActNetSend, Peer: to}
	b := le.AppendUint64(nil, leaseGroup)
	b = append(b, kind)
	for _, v := range [...]uint64{id, run, uint64(raw), support} {
		b = le.AppendUint64(b, v)
	}
	a.Msg.Len = uint16(copy(a.Msg.Body[:], b))
	r.out.Actions = append(r.out.Actions, a)
}

// lease handles a lease frame, reporting false for any other event.
func (r *simReplica) lease(ctx *core.Context, ev *core.Event) bool {
	t := lease.Now(ctx)
	switch {
	case ev.Kind == core.EvResumed:
		r.holder.Resumed()
		r.grantor.Resumed(r.rng.Uint64())
		return false
	case ev.Kind != core.EvNetRecv || ev.Msg.Buf != core.NoBuf || ev.Msg.Len != leaseFrameBytes || le.Uint64(ev.Msg.Body[:]) != leaseGroup:
		return false
	}
	b := ev.Msg.Body[kindBytes:]
	id, run, raw, support := le.Uint64(b), le.Uint64(b[8:]), core.Tick(le.Uint64(b[16:])), le.Uint64(b[24:])
	switch ev.Msg.Body[groupBytes] {
	case leaseAsk:
		if sup, ok := r.grantor.Grant(ev.Peer, 1, 1, t); ok {
			r.sendLease(leaseGrant, ev.Peer, id, r.grantor.Run(), ctx.Raw, sup)
			if r.beatAt == 0 {
				r.beatAt = ctx.Now + lease.DefaultConfig.Heartbeat
			}
		}
	case leaseGrant:
		r.holder.OnGrant(ev.Peer, lease.Grant{ID: core.RequestID(id), Run: run, Raw: raw, Support: support}, t)
	case leaseBeat:
		r.holder.OnHeartbeat(ev.Peer, run, raw, t)
	}
	return true
}

// leaseTick asks every peer due a request, none during the restart wait, and heartbeats every holder whose grant runs once a heartbeat is due.
func (r *simReplica) leaseTick(ctx *core.Context) {
	t := lease.Now(ctx)
	for id := range len(r.c.reps) {
		g := core.NodeID{Node: uint32(id)}
		if id == r.id || r.grantor.Waiting(t) || !r.holder.Due(g, t) {
			continue
		}
		r.leaseID++
		r.holder.Ask(g, core.RequestID(r.leaseID), t)
		r.sendLease(leaseAsk, g, r.leaseID, 0, 0, 0)
	}
	if r.beatAt == 0 || ctx.Now < r.beatAt {
		return
	}
	r.beatAt = 0
	for _, h := range r.grantor.Granting(nil, t) {
		r.sendLease(leaseBeat, h, 0, r.grantor.Run(), ctx.Raw, 0)
		r.beatAt = ctx.Now + lease.DefaultConfig.Heartbeat
	}
}

func (r *simReplica) onEvent(ctx *core.Context, ev *core.Event, acts []core.Action) []core.Action {
	r.out.Reset()
	if r.lease(ctx, ev) {
		return r.finish(ctx, acts)
	}
	if ev.Kind == core.EvTimer {
		r.leaseTick(ctx)
	}
	if ev.Kind == core.EvDiskDone {
		if ev.Err != core.DiskOK {
			panic(ev.Err)
		}
		r.s.Durable(ctx, uint64(ev.IO), &r.out)
		return r.finish(ctx, acts)
	}
	r.s.OnEvent(ctx, ev, &r.out)
	if c := r.c; ev.Kind == core.EvTimer && r.leads && !c.done {
		v := binary.LittleEndian.AppendUint64(nil, c.next)
		if c.next%3 == 0 {
			v = append(v, make([]byte, 300)...)
		}
		_ = r.s.Propose(ctx, v, &r.out)
		c.next++
	}
	return r.finish(ctx, acts)
}

// finish checks what the pass sent against what its replica holds durable, records what it applied and hands its actions to the scheduler.
func (r *simReplica) finish(ctx *core.Context, acts []core.Action) []core.Action {
	c := r.c
	for i := range r.out.Actions {
		if a := &r.out.Actions[i]; a.Kind == core.ActNetSend && c.early == nil {
			c.early = r.durableFor(ctx, a)
		}
	}
	for i := range r.out.Actions {
		if a := &r.out.Actions[i]; c.crashed && c.campaignAt == 0 && r.id != c.crashedID && a.Kind == core.ActNetSend && simMsgType(&a.Msg) == uint64(raftpb.MsgPreVote) {
			c.campaignAt = c.s.Time()
		}
	}
	acts = append(acts, r.out.Actions...)
	if r.out.Flush != 0 {
		acts = append(acts, core.Action{Kind: core.ActDiskFsync, IO: core.IOID(r.out.Flush)})
	}
	// Only a leader acknowledges an entry, on applying it. A follower can apply an entry before its
	// leader does: the leader's commit index goes out with the next round's appends, while the leader
	// applies what that round's Ready commits only once the round is durable.
	leading := r.leads || r.s.rn.BasicStatus().RaftState == raft.StateLeader
	for _, e := range r.out.Committed {
		c.history = fmt.Appendf(c.history, "%d n%d %d/%d %x\n", ctx.Now, r.id, e.Index, e.Term, e.Data)
		for uint64(len(c.log)) <= e.Index {
			c.log = append(c.log, raftpb.Entry{})
		}
		switch got := &c.log[e.Index]; {
		case got.Index == 0:
			*got = e
		case got.Term != e.Term || !bytes.Equal(got.Data, e.Data):
			if c.disagree == nil {
				c.disagree = fmt.Errorf("replica %d applied %d/%d %x where another applied %d/%d %x", r.id, e.Index, e.Term, e.Data, got.Index, got.Term, got.Data)
			}
		}
		if e.Type == raftpb.EntryNormal && len(e.Data) > 0 {
			r.applied++
			v := binary.LittleEndian.Uint64(e.Data)
			c.afterCrash = c.afterCrash || c.crashed && v >= c.sinceCrash
			c.applied(r, e.Index, v)
		}
		r.index = e.Index
		if leading {
			c.maxApplied = max(c.maxApplied, e.Index)
		}
	}
	c.done = c.afterCrash && (c.rep == nil || c.rep.removed && c.rep.caughtUp)
	st := r.s.rn.BasicStatus()
	r.leads = st.RaftState == raft.StateLeader
	if c.rep != nil {
		c.replaced(r, &st)
	}
	if l, ok := c.leaders[st.Term]; r.leads && ok && l != r.id && c.twoLeaders == nil {
		c.twoLeaders = fmt.Errorf("replicas %d and %d both led term %d", l, r.id, st.Term)
	}
	if r.leads {
		c.leaders[st.Term] = r.id
	}
	nd := c.s.Node(uint32(r.id))
	r.clocks = nd.Clock().Read(c.s.Time())
	if r.leads && r.s.LeaseRead(ctx) {
		c.leaseReads++
		if r.index < c.maxApplied && c.stale == nil {
			c.stale = fmt.Errorf("replica %d may serve a lease read in term %d having applied up to %d, a leader up to %d", r.id, st.Term, r.index, c.maxApplied)
		}
		if o := c.leaseReader(r.id); o >= 0 && c.overlap == nil {
			c.overlap = fmt.Errorf("replicas %d and %d may both serve lease reads", r.id, o)
		}
	}
	return acts
}

// leaseReader is a replica other than skip that may serve a lease read now: its node runs, and a pass now would neither see Resumed first nor find its lease ended.
func (c *simCluster) leaseReader(skip int) int {
	for id, r := range c.reps {
		nd := c.s.Node(uint32(id))
		if id == skip || r == nil || r.s == nil || !nd.Up() || nd.Paused() {
			continue
		}
		if core.Resumed(r.clocks, nd.Clock().Read(c.s.Time()), clock.Threshold) {
			continue
		}
		if ctx := nd.Context(); r.s.LeaseRead(&ctx) {
			return id
		}
	}
	return -1
}

// durableFor reports an acknowledgment of entries, or a granted vote, that a sends before the
// replica's storage holds them. One Ready can hold a vote and a later term the replica moved to
// before the Ready was taken; once that term is durable the replica never votes in the older one
// again, so the vote may leave.
func (r *simReplica) durableFor(ctx *core.Context, a *core.Action) error {
	var m raftpb.Message
	if a.Msg.Buf == core.NoBuf && le.Uint64(a.Msg.Body[:]) == leaseGroup {
		return nil
	}
	if _, _, err := decode(ctx.Pool, &a.Msg, &m, &fortify{}); err != nil {
		return err
	}
	st := r.c.storage[r.id]
	hs, _, _ := st.InitialState()
	switch {
	case m.Type == raftpb.MsgAppResp && !m.Reject:
		if last, _ := st.LastIndex(); last < m.Index {
			return fmt.Errorf("replica %d acknowledged index %d in term %d holding up to %d in term %d", r.id, m.Index, m.Term, last, hs.Term)
		}
	case m.Type == raftpb.MsgVoteResp && !m.Reject && hs.Term <= m.Term:
		if hs.Term < m.Term || hs.Vote != m.To {
			return fmt.Errorf("replica %d granted %d its vote in term %d holding %+v", r.id, m.To, m.Term, hs)
		}
	}
	return nil
}

func TestRaftRunsReplayFromTheirSeed(t *testing.T) {
	seen, leaseReads := map[string]int{}, 0
	seeds.Run(t, 0, 20, func(seed uint64) {
		a := newSimCluster(seed, plants{}).run(t, seed)
		for _, name := range []string{"msg=256", " Pause ", " Restart", " PeerLost "} {
			seen[name] += bytes.Count(a.s.Trace(), []byte(name))
		}
		leaseReads += a.leaseReads
		if !a.done {
			t.Fatalf("seed %d: crashed %v, no value proposed after the crash applied", seed, a.crashed)
		}
		b := newSimCluster(seed, plants{}).run(t, seed)
		if err := sim.CompareTraces(a.s.Trace(), b.s.Trace()); err != nil {
			t.Fatalf("seed %d: trace replays differently: %v", seed, err)
		}
		if err := sim.CompareTraces(a.history, b.history); err != nil {
			t.Fatalf("seed %d: applied history replays differently: %v", seed, err)
		}
	})
	if !seeds.Default() {
		return
	}
	for name, n := range seen {
		if n == 0 {
			t.Errorf("no %q in 20 seeds' traces", name)
		}
	}
	if leaseReads == 0 {
		t.Error("no pass in 20 seeds could serve a lease read")
	}
}

func TestFollowersVotingWhileSupportingBreakTheLeaseInvariant(t *testing.T) {
	for seed := range uint64(20) {
		c := newSimCluster(seed, plants{disloyal: true})
		c.inv.Only(invOneLeaseReader)
		for c.s.Time() < simRunFor && c.s.Step() {
			c.pause()
			if c.inv.Check() != nil {
				return
			}
		}
	}
	t.Fatal("20 seeds whose followers vote and campaign while supporting a leader broke no lease reader invariant")
}

func TestUnseededElectionTimeoutsBreakReplay(t *testing.T) {
	a, b := newSimCluster(1, plants{unseeded: true}).run(t, 1), newSimCluster(1, plants{unseeded: true}).run(t, 1)
	if sim.CompareTraces(a.s.Trace(), b.s.Trace()) == nil {
		t.Fatal("two runs of seed 1 with crypto/rand election timeouts gave one trace, so the replay test would miss unseeded randomness")
	}
}

func TestSendingBeforeTheFlushBreaksTheDurabilityInvariant(t *testing.T) {
	for seed := range uint64(20) {
		c := newSimCluster(seed, plants{early: true})
		c.inv.Only("acknowledged only once durable")
		for c.s.Time() < simRunFor && c.s.Step() {
			if c.inv.Check() != nil {
				return
			}
		}
	}
	t.Fatal("20 seeds sending every message before its round's flush broke no invariant")
}

// A leader serving lease reads before an entry of its term commits may not have applied what the
// previous leader applied. The seeds are ones where that shows; most leaders apply in time.
func TestLeaseReadsBeforeAnEntryOfTheTermCommitsBreakTheInvariant(t *testing.T) {
	for _, seed := range []uint64{5, 159} {
		c := newSimCluster(seed, plants{uncommitted: true})
		c.inv.Only(invLeaseReadsCurrent)
		for c.s.Time() < simRunFor && c.s.Step() {
			c.pause()
			if c.inv.Check() != nil {
				return
			}
			c.after(t, seed)
		}
	}
	t.Fatal("seeds 5 and 159 serving lease reads before an entry of the term commits broke no invariant")
}
