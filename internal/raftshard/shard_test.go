package raftshard

import (
	"bytes"
	"testing"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/lease"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

const testGroup = 5

// replica is one shard of a testNet with its durable storage, pool, node-pair leases, pending flush and what it applied; its raw clock runs rawLag behind CLOCK_BOOTTIME.
type replica struct {
	s       *Shard
	st      *Storage
	pool    *sim.Pool
	holder  *lease.Holder
	grantor *lease.Grantor
	rawLag  core.Tick
	out     Output
	ctx     core.Context
	flush   uint64
	applied []raftpb.Entry
	reads   []raft.ReadState
	// forts are the fortification messages the replica sent in the last call.
	forts []fortify
	// snaps counts the snapshots it restored, and removed whether it learned it left the group.
	snaps   int
	removed bool
}

type envelope struct {
	from, to int
	msg      core.Msg
}

// testNet runs replicas by hand: the test delivers messages, completes flushes and moves the clock.
type testNet struct {
	t     testing.TB
	now   core.Tick
	seed  uint64
	r     []*replica
	inbox []envelope
	// lean skips decoding sends and recording what replicas apply, for a benchmark.
	lean    bool
	leaseID uint64
}

func bootStorage(n int) *Storage {
	var voters []uint64
	for i := range n {
		voters = append(voters, uint64(i)+1)
	}
	return confStorage(raftpb.ConfState{Voters: voters})
}

func confStorage(cs raftpb.ConfState) *Storage {
	st := NewStorage()
	if err := st.ApplySnapshot(raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 1, Term: 1, ConfState: cs}}); err != nil {
		panic(err)
	}
	return st
}

func testConfig(n, i int, st *Storage, seed uint64) Config {
	cfg := Config{Group: testGroup, ID: uint64(i) + 1, Storage: st, Seed: seed + uint64(i), Timer: 1}
	for j := range n {
		cfg.Peers = append(cfg.Peers, Peer{ID: uint64(j) + 1, Node: core.NodeID{Node: uint32(j)}})
	}
	return cfg
}

func newNet(t testing.TB, n int, seed uint64) *testNet {
	return newConfNet(t, n, seed, func(int) *Storage { return bootStorage(n) })
}

// newConfNet starts n replicas from the storage boot gives each, past their nodes' restart wait.
func newConfNet(t testing.TB, n int, seed uint64, boot func(i int) *Storage) *testNet {
	net := &testNet{t: t, now: 5_000_000, seed: seed}
	for i := range n {
		net.r = append(net.r, &replica{st: boot(i), pool: sim.NewPool(4<<20, 0)})
	}
	for i := range n {
		net.start(i, lease.Time{})
	}
	return net
}

// restart drops replica i's shard, pending flush and node-pair leases, as a crash does, and starts a new one from its storage, its node in its restart wait.
func (n *testNet) restart(i int) {
	ctx := n.ctx(i)
	n.start(i, lease.Now(&ctx))
}

func (n *testNet) start(i int, at lease.Time) {
	r := n.r[i]
	r.holder = lease.NewHolder(lease.DefaultConfig)
	r.grantor = lease.NewGrantor(lease.DefaultConfig, uint64(at.Boot)+uint64(i), at)
	cfg := testConfig(len(n.r), i, r.st, n.seed)
	cfg.Holder, cfg.Grantor = r.holder, r.grantor
	s, err := New(cfg)
	if err != nil {
		n.t.Fatal(err)
	}
	r.s, r.flush = s, 0
	n.call(i, func(ctx *core.Context, out *Output) { s.Start(ctx, out) })
}

func (n *testNet) ctx(i int) core.Context {
	return core.Context{Now: n.now, Raw: n.now - n.r[i].rawLag, Pool: n.r[i].pool}
}

func (n *testNet) leaseTime(i int) lease.Time {
	ctx := n.ctx(i)
	return lease.Now(&ctx)
}

// renew has node h ask node g for a lease now and take its answer at once; it reports whether g granted.
func (n *testNet) renew(h, g int) bool {
	n.leaseID++
	id := core.RequestID(n.leaseID)
	hn, gn := core.NodeID{Node: uint32(h)}, core.NodeID{Node: uint32(g)}
	n.r[h].holder.Ask(gn, id, n.leaseTime(h))
	gt := n.leaseTime(g)
	sup, ok := n.r[g].grantor.Grant(hn, 1, 1, gt)
	if ok {
		n.r[h].holder.OnGrant(gn, lease.Grant{ID: id, Run: n.r[g].grantor.Run(), Raw: gt.Raw, Support: sup}, n.leaseTime(h))
	}
	return ok
}

// leases gives node h a lease from each of grantors, two grants rate_gap apart for a rate bound, the second asked now.
func (n *testNet) leases(h int, grantors ...int) {
	n.now -= lease.DefaultConfig.RateGap
	for _, g := range grantors {
		n.renew(h, g)
	}
	n.now += lease.DefaultConfig.RateGap
	for _, g := range grantors {
		n.renew(h, g)
		if !n.r[h].holder.Valid(core.NodeID{Node: uint32(g)}, n.leaseTime(h)) {
			n.t.Fatalf("node %d holds no lease from node %d after two grants", h, g)
		}
	}
}

// call runs f on replica i and returns the messages it sent, queued for delivery.
func (n *testNet) call(i int, f func(ctx *core.Context, out *Output)) []raftpb.Message {
	r := n.r[i]
	r.ctx = n.ctx(i)
	r.out.Reset()
	f(&r.ctx, &r.out)
	if !n.lean {
		r.applied = append(r.applied, r.out.Committed...)
		r.reads = append(r.reads, r.out.Reads...)
		if !raft.IsEmptySnap(r.out.Snapshot) {
			r.snaps++
		}
		r.removed = r.removed || r.out.Removed
	}
	if r.out.Flush != 0 {
		if r.flush != 0 {
			n.t.Fatalf("replica %d took round %d with round %d in flight", i, r.out.Flush, r.flush)
		}
		r.flush = r.out.Flush
	}
	var sent []raftpb.Message
	r.forts = r.forts[:0]
	for _, a := range r.out.Actions {
		if a.Kind != core.ActNetSend {
			continue
		}
		if !n.lean {
			var m raftpb.Message
			var f fortify
			kind, err := decodeKind(r.pool, &a.Msg, &m, &f)
			switch {
			case err != nil:
				n.t.Fatalf("replica %d sent an undecodable message: %v", i, err)
			case kind == kindRaft:
				sent = append(sent, m)
			default:
				r.forts = append(r.forts, f)
			}
		}
		e := envelope{from: i, to: int(a.Peer.Node), msg: a.Msg}
		if a.Msg.Buf != core.NoBuf {
			to := n.r[e.to].pool
			b, err := to.Alloc(len(r.pool.Bytes(a.Msg.Buf)))
			if err != nil {
				n.t.Fatal(err)
			}
			copy(to.Bytes(b), r.pool.Bytes(a.Msg.Buf))
			r.pool.Free(a.Msg.Buf)
			e.msg.Buf = b
		}
		n.inbox = append(n.inbox, e)
	}
	return sent
}

func (n *testNet) event(i int, ev core.Event) []raftpb.Message {
	return n.call(i, func(ctx *core.Context, out *Output) { n.r[i].s.OnEvent(ctx, &ev, out) })
}

// recv hands replica i a message from replica from, framed as a peer's.
func (n *testNet) recv(i, from int, m raftpb.Message) []raftpb.Message {
	ev := core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{Node: uint32(from)}}
	if !encode(n.r[i].pool, testGroup, &m, &ev.Msg) {
		n.t.Fatal("no buffer")
	}
	return n.event(i, ev)
}

func (n *testNet) durable(i int) []raftpb.Message {
	r := n.r[i]
	round := r.flush
	if round == 0 {
		n.t.Fatalf("replica %d has no round in flight", i)
	}
	r.flush = 0
	return n.call(i, func(ctx *core.Context, out *Output) { r.s.Durable(ctx, round, out) })
}

// deliver hands over every queued message, those it causes included, keeping those keep refuses out.
func (n *testNet) deliver(keep func(e envelope) bool) {
	for i := 0; i < len(n.inbox); i++ {
		e := n.inbox[i]
		if keep != nil && !keep(e) {
			if e.msg.Buf != core.NoBuf {
				n.r[e.to].pool.Free(e.msg.Buf)
			}
			continue
		}
		n.event(e.to, core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{Node: uint32(e.from)}, Msg: e.msg})
	}
	n.inbox = n.inbox[:0]
}

// settle delivers and flushes until nothing is left to do.
func (n *testNet) settle() {
	for range 1000 {
		n.deliver(nil)
		flushed := false
		for i, r := range n.r {
			if r.flush != 0 {
				n.durable(i)
				flushed = true
			}
		}
		if !flushed && len(n.inbox) == 0 {
			for i, r := range n.r {
				if r.pool.Live() != 0 {
					n.t.Fatalf("replica %d holds %d buffers once the group settled", i, r.pool.Live())
				}
			}
			return
		}
	}
	n.t.Fatal("the group did not settle")
}

// advance moves the clock by d and hands each replica in who, or every one, a timer event; it returns each one's sends.
func (n *testNet) advance(d core.Tick, who ...int) [][]raftpb.Message {
	n.now += d
	if who == nil {
		for i := range n.r {
			who = append(who, i)
		}
	}
	sent := make([][]raftpb.Message, len(n.r))
	for _, i := range who {
		sent[i] = n.event(i, core.Event{Kind: core.EvTimer, Timer: 1})
	}
	return sent
}

// elect makes replica i leader and settles the group.
func (n *testNet) elect(i int) {
	n.call(i, func(ctx *core.Context, out *Output) {
		if err := n.r[i].s.rn.Campaign(); err != nil {
			n.t.Fatal(err)
		}
		n.r[i].s.ready(ctx, out)
	})
	n.settle()
	if st := n.r[i].s.rn.BasicStatus(); st.RaftState != raft.StateLeader {
		n.t.Fatalf("replica %d is %v after campaigning", i, st.RaftState)
	}
}

func (n *testNet) propose(i int, data []byte) []raftpb.Message {
	return n.call(i, func(ctx *core.Context, out *Output) {
		if err := n.r[i].s.Propose(ctx, data, out); err != nil {
			n.t.Fatal(err)
		}
	})
}

func decodeKind(pool core.BufPool, msg *core.Msg, m *raftpb.Message, f *fortify) (byte, error) {
	_, kind, err := decode(pool, msg, m, f)
	return kind, err
}

func count(ms []raftpb.Message, t raftpb.MessageType) int {
	c := 0
	for _, m := range ms {
		if m.Type == t {
			c++
		}
	}
	return c
}

func types(ms []raftpb.Message) []raftpb.MessageType {
	var ts []raftpb.MessageType
	for _, m := range ms {
		ts = append(ts, m.Type)
	}
	return ts
}

func TestConfig(t *testing.T) {
	rc := raftConfig(&Config{ID: 1, Storage: NewStorage()})
	if rc.AsyncStorageWrites {
		t.Error("asynchronous storage writes are on")
	}
	if rc.CheckQuorum || !rc.PreVote {
		t.Errorf("CheckQuorum %v, PreVote %v: want off and on", rc.CheckQuorum, rc.PreVote)
	}
	if !rc.DisableProposalForwarding {
		t.Error("followers forward proposals")
	}
	if TickInterval != 10_000 || rc.HeartbeatTick != 10 || rc.ElectionTick != 100 {
		t.Errorf("tick %v, HeartbeatTick %d, ElectionTick %d: want 10 ms, 10 and 100", TickInterval.Duration(), rc.HeartbeatTick, rc.ElectionTick)
	}
	if rc.Rand == nil {
		t.Error("election timeouts are drawn from crypto/rand")
	}
}

func TestTicksComeFromTheClock(t *testing.T) {
	heartbeats := func(sent [][]raftpb.Message) int { return count(sent[0], raftpb.MsgHeartbeat) }
	t.Run("10 ms is one tick and 100 ms a heartbeat", func(t *testing.T) {
		n := newNet(t, 3, 1)
		n.elect(0)
		for k := range 9 {
			if h := heartbeats(n.advance(10_000, 0)); h != 0 {
				t.Fatalf("%d heartbeats after %d ms", h, 10*(k+1))
			}
		}
		if h := heartbeats(n.advance(10_000, 0)); h != 2 {
			t.Fatalf("%d heartbeats after 100 ms, want one to each follower", h)
		}
	})
	t.Run("a 35 ms step carries its 5 ms", func(t *testing.T) {
		n := newNet(t, 3, 1)
		n.elect(0)
		for _, d := range []core.Tick{35_000, 35_000} {
			if h := heartbeats(n.advance(d, 0)); h != 0 {
				t.Fatalf("%d heartbeats", h)
			}
		}
		if h := heartbeats(n.advance(30_000, 0)); h != 2 {
			t.Fatalf("%d heartbeats after 35 + 35 + 30 ms, want 2: the remainders make the tenth tick", h)
		}
	})
	t.Run("5 s in one event credits one heartbeat of ticks", func(t *testing.T) {
		n := newNet(t, 3, 1)
		n.elect(0)
		sent := n.advance(5_000_000)
		if h := heartbeats(sent); h != 2 {
			t.Fatalf("%d heartbeats from the leader, want one to each follower", h)
		}
		for i := 1; i < 3; i++ {
			if len(sent[i]) != 0 {
				t.Fatalf("follower %d sent %v after 5 s in one event", i, types(sent[i]))
			}
		}
	})
}

func TestAFollowerWithNoLeaderCampaignsAtItsTimeout(t *testing.T) {
	seen := map[core.Tick]bool{}
	for seed := range uint64(20) {
		n := newNet(t, 3, seed*7)
		start := n.now
		for {
			sent := n.advance(10_000, 0)[0]
			if len(sent) == 0 {
				continue
			}
			if count(sent, raftpb.MsgPreVote) != 2 || len(sent) != 2 {
				t.Fatalf("seed %d: campaign sent %v, want a pre-vote to each peer", seed, types(sent))
			}
			break
		}
		at := n.now - start
		if at < 1_000_000 || at > 1_990_000 {
			t.Fatalf("seed %d: campaigned after %v", seed, at.Duration())
		}
		seen[at] = true
	}
	if len(seen) < 10 {
		t.Fatalf("20 seeds campaigned at only %d distinct times", len(seen))
	}
}

func TestAMessageOver256BytesTravelsInABuffer(t *testing.T) {
	pool := sim.NewPool(1<<20, 0)
	found := map[int]bool{}
	for size := range 300 {
		m := raftpb.Message{Type: raftpb.MsgApp, To: 2, From: 1, Term: 3, Index: 9, LogTerm: 3, Commit: 8,
			Entries: []raftpb.Entry{{Term: 3, Index: 10, Data: bytes.Repeat([]byte{7}, size)}}}
		total := kindBytes + m.Size()
		if total != 256 && total != 257 {
			continue
		}
		var msg core.Msg
		if !encode(pool, testGroup, &m, &msg) {
			t.Fatal("no buffer")
		}
		if inline := msg.Buf == core.NoBuf; inline != (total == 256) {
			t.Fatalf("a %d-byte message inline: %v", total, inline)
		}
		var got raftpb.Message
		g, _, err := decode(pool, &msg, &got, &fortify{})
		if err != nil || g != testGroup || !bytes.Equal(mustMarshal(&got), mustMarshal(&m)) {
			t.Fatalf("a %d-byte message decodes to group %d, %v, err %v", total, g, raft.DescribeMessage(got, nil), err)
		}
		if msg.Buf != core.NoBuf {
			pool.Free(msg.Buf)
		}
		found[total] = true
	}
	if !found[256] || !found[257] {
		t.Fatalf("no entry size gave a message of exactly 256 and 257 bytes: %v", found)
	}
}

func mustMarshal(m *raftpb.Message) []byte {
	b, err := m.Marshal()
	if err != nil {
		panic(err)
	}
	return b
}

func TestAnAppendWithNoBufferIsLostAndResent(t *testing.T) {
	n := newNet(t, 3, 1)
	n.elect(0)
	full := n.r[0].pool
	n.r[0].pool = sim.NewPool(core.BlockSize, 0)
	hold, err := n.r[0].pool.Alloc(1)
	if err != nil {
		t.Fatal(err)
	}
	sent := n.propose(0, bytes.Repeat([]byte{1}, 1000))
	if len(sent) != 0 || n.r[0].s.Stats().Unsent != 2 {
		t.Fatalf("sent %v with the pool exhausted, %+v", types(sent), n.r[0].s.Stats())
	}
	n.r[0].pool.Free(hold)
	n.r[0].pool = full
	last := n.r[0].st.LastIndex
	for range 20 {
		n.settle()
		n.advance(100_000)
	}
	n.settle()
	want, _ := last()
	for i := 1; i < 3; i++ {
		if got, _ := n.r[i].st.LastIndex(); got != want {
			t.Fatalf("follower %d holds up to %d, the leader %d: the lost append was not resent", i, got, want)
		}
	}
}

func TestUndecodableMessagesAreDroppedAndCounted(t *testing.T) {
	n := newNet(t, 3, 1)
	n.elect(0)
	f := n.r[1]
	good := raftpb.Message{Type: raftpb.MsgHeartbeat, To: 2, From: 1, Term: f.s.rn.BasicStatus().Term}
	frame := func(m raftpb.Message) core.Msg {
		var msg core.Msg
		encode(f.pool, testGroup, &m, &msg)
		return msg
	}
	buffered := func(head []byte, size int) core.Msg {
		b, err := f.pool.Alloc(size)
		if err != nil {
			t.Fatal(err)
		}
		msg := core.Msg{Buf: b}
		if err := msg.SetInline(head); err != nil {
			t.Fatal(err)
		}
		return msg
	}
	live := f.pool.Live()
	head := func(n uint32) []byte { return le.AppendUint32(append(le.AppendUint64(nil, testGroup), kindRaft), n) }
	fort := func(kind byte, size int) core.Msg {
		var m core.Msg
		_ = m.SetInline(append(append(le.AppendUint64(nil, testGroup), kind), make([]byte, size)...))
		return m
	}
	cases := map[string]core.Msg{
		"truncated header": func() core.Msg { var m core.Msg; _ = m.SetInline([]byte{5, 0, 0}); return m }(),
		"garbage body": func() core.Msg {
			var m core.Msg
			_ = m.SetInline(append(le.AppendUint64(nil, testGroup), kindRaft, 0xff, 0xff, 0xff))
			return m
		}(),
		"unknown type":           frame(raftpb.Message{Type: 99, To: 2, From: 1, Term: 1}),
		"local type":             frame(raftpb.Message{Type: raftpb.MsgHup, To: 2, From: 1}),
		"forwarded proposal":     frame(raftpb.Message{Type: raftpb.MsgProp, To: 2, From: 1, Entries: []raftpb.Entry{{Data: []byte("x")}}}),
		"buffer length past end": buffered(head(core.BlockSize+1), 10),
		"buffer header short":    buffered(head(3)[:kindBytes+1], 10),
		"unknown kind":           fort(kindRemoved+1, 40),
		"short fortification":    fort(kindFortify, 39),
		"long fortification":     fort(kindFortified, 41),
		"buffered fortification": buffered(append(append(le.AppendUint64(nil, testGroup), kindFortify), make([]byte, 40)...), 10),
	}
	for name, msg := range cases {
		before := f.s.rn.Status()
		was := f.s.Stats().Undecodable
		sent := n.event(1, core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{Node: 0}, Msg: msg})
		if len(sent) != 0 || f.flush != 0 || f.s.Stats().Undecodable != was+1 {
			t.Fatalf("%s: sent %v, round %d, %+v", name, types(sent), f.flush, f.s.Stats())
		}
		if after := f.s.rn.Status(); after.HardState != before.HardState || after.Lead != before.Lead || f.s.rn.HasReady() {
			t.Fatalf("%s: stepped into RawNode", name)
		}
	}
	if f.pool.Live() != live {
		t.Fatalf("%d buffers live after the drops, %d before", f.pool.Live(), live)
	}
	if sent := n.event(1, core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{Node: 0}, Msg: frame(good)}); count(sent, raftpb.MsgHeartbeatResp) != 1 {
		t.Fatalf("a well-formed heartbeat after the drops got %v", types(sent))
	}
}

func TestMessagesForAnotherGroupOrReplicaAreDropped(t *testing.T) {
	n := newNet(t, 3, 1)
	n.elect(0)
	f := n.r[1]
	for _, c := range []struct {
		group uint64
		to    uint64
	}{{testGroup + 1, 2}, {testGroup, 3}} {
		m := raftpb.Message{Type: raftpb.MsgHeartbeat, To: c.to, From: 1, Term: 1}
		ev := core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{Node: 0}}
		encode(f.pool, c.group, &m, &ev.Msg)
		was := f.s.Stats().Misrouted
		if sent := n.event(1, ev); len(sent) != 0 || f.s.Stats().Misrouted != was+1 {
			t.Fatalf("group %d to %d: answered %v, %+v", c.group, c.to, types(sent), f.s.Stats())
		}
	}
}

func TestPeerLostReportsTheFollowerUnreachable(t *testing.T) {
	n := newNet(t, 3, 1)
	n.elect(0)
	n.propose(0, []byte("x"))
	n.settle()
	l := n.r[0].s
	if st := l.rn.Status().Progress[2].State; st != tracker.StateReplicate {
		t.Fatalf("follower 2 is in %v before the loss", st)
	}
	n.event(0, core.Event{Kind: core.EvPeerLost, Peer: core.NodeID{Node: 1}})
	if st := l.rn.Status().Progress[2].State; st != tracker.StateProbe {
		t.Fatalf("follower 2 is in %v after PeerLost, want probe", st)
	}
	if st := l.rn.Status().Progress[3].State; st != tracker.StateReplicate {
		t.Fatalf("follower 3 is in %v after another node's PeerLost", st)
	}
}

func TestALeadersAppendsLeaveWithItsWrite(t *testing.T) {
	n := newNet(t, 3, 1)
	n.elect(0)
	sent := n.propose(0, []byte("x"))
	if n.r[0].flush == 0 || count(sent, raftpb.MsgApp) != 2 || len(sent) != 2 {
		t.Fatalf("a proposal took round %d and sent %v, want a round and an append to each follower", n.r[0].flush, types(sent))
	}
	if sent := n.durable(0); count(sent, raftpb.MsgApp) != 0 {
		t.Fatalf("the round's flush sent %v, the appends again", types(sent))
	}
}

// follow has the leader replicate one entry, leaving it on follower 1 in a round not yet durable.
func follow(t *testing.T, n *testNet, data string) (index uint64, sent []raftpb.Message) {
	n.propose(0, []byte(data))
	n.durable(0)
	app := n.inbox[0]
	n.inbox = n.inbox[1:]
	if app.to != 1 {
		t.Fatalf("first append goes to %d", app.to)
	}
	sent = n.event(1, core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{Node: 0}, Msg: app.msg})
	last, _ := n.r[0].st.LastIndex()
	return last, sent
}

func TestAFollowerAcknowledgesAnAppendOnceItIsDurable(t *testing.T) {
	n := newNet(t, 3, 1)
	n.elect(0)
	index, sent := follow(t, n, "x")
	f := n.r[1]
	if f.flush == 0 || len(sent) != 0 {
		t.Fatalf("an append took round %d and sent %v before its flush", f.flush, types(sent))
	}
	if sent := n.advance(100_000, 1)[1]; len(sent) != 0 {
		t.Fatalf("sent %v while the round was in flight", types(sent))
	}
	if last, _ := f.st.LastIndex(); last >= index {
		t.Fatalf("storage holds %d before the round is durable", last)
	}
	sent = n.durable(1)
	if count(sent, raftpb.MsgAppResp) != 1 || sent[0].Index != index || sent[0].Reject {
		t.Fatalf("sent %v once durable, want the acknowledgment of %d", types(sent), index)
	}
	if last, _ := f.st.LastIndex(); last != index {
		t.Fatalf("storage holds %d once durable, want %d", last, index)
	}
}

func TestAVoteLeavesOnceItIsDurable(t *testing.T) {
	n := newNet(t, 3, 1)
	n.elect(0)
	f := n.r[1]
	last, _ := f.st.LastIndex()
	term := f.s.rn.BasicStatus().Term
	sent := n.recv(1, 2, raftpb.Message{Type: raftpb.MsgVote, To: 2, From: 3, Term: term + 1, Index: last, LogTerm: term})
	if f.flush == 0 || len(sent) != 0 {
		t.Fatalf("a vote took round %d and sent %v before its flush", f.flush, types(sent))
	}
	if hs, _, _ := f.st.InitialState(); hs.Vote == 3 {
		t.Fatal("the vote is in storage before its round is durable")
	}
	sent = n.durable(1)
	if len(sent) != 1 || sent[0].Type != raftpb.MsgVoteResp || sent[0].Reject {
		t.Fatalf("sent %v once durable, want the granted vote", types(sent))
	}
	if hs, _, _ := f.st.InitialState(); hs.Vote != 3 || hs.Term != term+1 {
		t.Fatalf("storage holds %+v once durable", hs)
	}
}

func TestAPreVoteAnswerDoesNotWaitForTheFlush(t *testing.T) {
	n := newNet(t, 3, 1)
	n.elect(0)
	follow(t, n, "a")
	f := n.r[1]
	n.propose(0, []byte("b"))
	n.durable(0)
	n.deliver(func(e envelope) bool { return e.to == 1 })
	term := f.s.rn.BasicStatus().Term
	n.recv(1, 2, raftpb.Message{Type: raftpb.MsgPreVote, To: 2, From: 3, Term: term + 1, Index: 100, LogTerm: term})
	sent := n.durable(1)
	if f.flush == 0 {
		t.Fatal("the second append took no round")
	}
	if count(sent, raftpb.MsgAppResp) != 1 || count(sent, raftpb.MsgPreVoteResp) != 1 || len(sent) != 2 {
		t.Fatalf("sent %v, want the first append's acknowledgment and the pre-vote answer, the second acknowledgment held", types(sent))
	}
}

func TestCommittedEntriesAreAppliedOnceDurable(t *testing.T) {
	n := newNet(t, 3, 1)
	n.elect(0)
	f := n.r[1]
	last, _ := f.st.LastIndex()
	term := f.s.rn.BasicStatus().Term
	app := raftpb.Message{Type: raftpb.MsgApp, To: 2, From: 1, Term: term, Index: last, LogTerm: term, Commit: last + 1,
		Entries: []raftpb.Entry{{Term: term, Index: last + 1, Data: []byte("x")}}}
	applied := len(f.applied)
	n.recv(1, 0, app)
	if f.flush == 0 || len(f.applied) != applied {
		t.Fatalf("round %d, %d entries applied before the flush", f.flush, len(f.applied)-applied)
	}
	more := app
	more.Index, more.Commit = last+1, last+2
	more.Entries = []raftpb.Entry{{Term: term, Index: last + 2, Data: []byte("y")}}
	round := f.flush
	if sent := n.recv(1, 0, more); len(sent) != 0 || f.flush != round || len(f.applied) != applied {
		t.Fatalf("a second message during the round sent %v, round %d, applied %d", types(sent), f.flush, len(f.applied)-applied)
	}
	n.durable(1)
	if len(f.applied) != applied+1 || string(f.applied[applied].Data) != "x" {
		t.Fatalf("applied %v once durable, want x", f.applied[applied:])
	}
	if f.flush == round || f.flush == 0 {
		t.Fatal("the second append took no round of its own after Advance")
	}
	n.durable(1)
	if len(f.applied) != applied+2 || string(f.applied[applied+1].Data) != "y" {
		t.Fatalf("applied %v after the second round", f.applied[applied:])
	}
}

func TestACrashBeforeTheFlushLosesTheEntryAndTheLeaderResendsIt(t *testing.T) {
	n := newNet(t, 3, 1)
	n.elect(0)
	index, sent := follow(t, n, "x")
	if len(sent) != 0 {
		t.Fatalf("sent %v before the flush", types(sent))
	}
	n.restart(1)
	if last, _ := n.r[1].st.LastIndex(); last >= index {
		t.Fatalf("the restarted follower holds %d", last)
	}
	for range 10 {
		n.settle()
		n.advance(100_000)
	}
	n.settle()
	if last, _ := n.r[1].st.LastIndex(); last != index {
		t.Fatalf("the restarted follower holds %d, want %d resent", last, index)
	}
}

// BenchmarkProposal is one 128-byte proposal replicated, flushed and committed on three replicas
// delivered by hand; its allocations are etcd-raft's and the Storage.Entries results it reads,
// which TestCostPerGroup budgets.
func BenchmarkProposal(b *testing.B) {
	n := newNet(b, 3, 1)
	n.elect(0)
	n.r[0].s.SetPlan([]Peer{{ID: 1}, {ID: 2}, {ID: 3}})
	n.lean = true
	data := make([]byte, 128)
	b.ReportAllocs()
	for b.Loop() {
		n.propose(0, data)
		n.settle()
	}
}
