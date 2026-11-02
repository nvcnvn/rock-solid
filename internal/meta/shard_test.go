package meta

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"testing"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/lease"
	"github.com/nvcnvn/rock-solid/internal/lsm"
	"github.com/nvcnvn/rock-solid/internal/raftshard"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// node is one replica of a group: its shard, durable Raft storage, pool, the round in flight,
// the answers it gave by token and, per round it took, how many Txn entries the round held.
type node struct {
	s       *Shard
	st      *raftshard.Storage
	pool    *sim.Pool
	out     Output
	ctx     core.Context
	flush   uint64
	answers map[uint64]Answer
	rounds  []int
	// recs are the records of the rounds it took, which a restart replays as the disk log would.
	recs [][]byte
	// late holds what the replica handed over, for a shard that applies it only once the call returns.
	late []raftpb.Entry
}

type envelope struct {
	from, to int
	msg      core.Msg
}

// group runs a group's replicas by hand: the test delivers messages, completes flushes and moves the clock.
type group struct {
	t     testing.TB
	now   core.Tick
	n     []*node
	inbox []envelope
	cut   []bool
	// late builds shards that apply what Raft hands over once each call returns, not within it.
	late bool
}

func newGroup(t testing.TB, size int, late bool) *group {
	c := &group{t: t, now: 5_000_000, cut: make([]bool, size), late: late}
	cs := raftpb.ConfState{}
	for i := range size {
		cs.Voters = append(cs.Voters, uint64(i)+1)
	}
	for range size {
		c.n = append(c.n, &node{st: bootStorage(t, cs), pool: sim.NewPool(64<<20, 0)})
	}
	for i := range size {
		c.start(i)
	}
	return c
}

func bootStorage(t testing.TB, cs raftpb.ConfState) *raftshard.Storage {
	st := raftshard.NewStorage()
	if err := st.ApplySnapshot(raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 1, Term: 1, ConfState: cs}}); err != nil {
		t.Fatal(err)
	}
	return st
}

// restart crashes replica i and starts it on a storage rebuilt from its boot base and its rounds' records.
func (c *group) restart(i int) {
	r := c.n[i]
	r.st = bootStorage(c.t, raftpb.ConfState{Voters: []uint64{1, 2, 3}})
	for _, rec := range r.recs {
		if err := r.st.Replay(disklog.BlobRaftState, append([]byte{1}, rec...)); err != nil {
			c.t.Fatal(err)
		}
	}
	c.start(i)
}

// start starts replica i from its storage, as after a crash: its state empty at the storage's base.
func (c *group) start(i int) {
	r := c.n[i]
	cfg := Config{Raft: raftshard.Config{Group: 5, ID: uint64(i) + 1, Storage: r.st, Seed: uint64(i), Timer: 1}, Tree: lsm.Config{MaxPiece: 64 << 10, Check: true}}
	for j := range c.n {
		cfg.Raft.Peers = append(cfg.Raft.Peers, raftshard.Peer{ID: uint64(j) + 1, Node: core.NodeID{Node: uint32(j)}})
	}
	cfg.Raft.Holder = lease.NewHolder(lease.DefaultConfig)
	cfg.Raft.Grantor = lease.NewGrantor(lease.DefaultConfig, uint64(i)+1, lease.Time{})
	var err error
	if c.late {
		r.s, err = newLateShard(cfg, r)
	} else {
		r.s, err = NewShard(cfg)
	}
	if err != nil {
		c.t.Fatal(err)
	}
	r.flush, r.answers = 0, map[uint64]Answer{}
	c.call(i, func(ctx *core.Context, out *Output) { r.s.Start(ctx, out) })
}

// newLateShard is NewShard with the seam this item replaced: what Raft hands over is applied after the call.
func newLateShard(cfg Config, r *node) (*Shard, error) {
	st, err := NewState(cfg.Tree, cfg.Raft.Storage.Base().Index)
	if err != nil {
		return nil, err
	}
	s := &Shard{state: st, log: cfg.Raft.Storage, id: cfg.Raft.ID, newest: entryFormat}
	cfg.Raft.SnapshotData, cfg.Raft.CheckSnapshot = st.SnapshotData, CheckSnapshot
	cfg.Raft.Apply = func(_ raftpb.Snapshot, ents []raftpb.Entry) { r.late = append(r.late, ents...) }
	s.raft, err = raftshard.New(cfg.Raft)
	return s, err
}

func (c *group) ctx(i int) core.Context {
	return core.Context{Now: c.now, Raw: c.now, Pool: c.n[i].pool}
}

// call runs f on replica i, records its answers and its round, and queues its messages.
func (c *group) call(i int, f func(ctx *core.Context, out *Output)) {
	r := c.n[i]
	r.ctx = c.ctx(i)
	r.out.Reset()
	f(&r.ctx, &r.out)
	if len(r.late) > 0 {
		r.s.out = &r.out
		r.s.apply(raftpb.Snapshot{}, r.late)
		r.late = r.late[:0]
	}
	for _, a := range r.out.Answers {
		a.Result.Actual, a.Result.Updated = slices.Clone(a.Result.Actual), slices.Clone(a.Result.Updated)
		if _, ok := r.answers[a.Token]; ok {
			c.t.Fatalf("replica %d answered token %d twice", i, a.Token)
		}
		r.answers[a.Token] = a
	}
	if r.out.Raft.Flush != 0 {
		if r.flush != 0 {
			c.t.Fatalf("replica %d took round %d with round %d in flight", i, r.out.Raft.Flush, r.flush)
		}
		r.flush = r.out.Raft.Flush
		r.recs = append(r.recs, raftshard.AppendRound(nil, &r.out.Raft.Round))
		n := 0
		for _, e := range r.out.Raft.Round.Entries {
			if e.Type == raftpb.EntryNormal && len(e.Data) > 0 {
				n++
			}
		}
		r.rounds = append(r.rounds, n)
	}
	for _, a := range r.out.Raft.Actions {
		if a.Kind != core.ActNetSend {
			continue
		}
		e := envelope{from: i, to: int(a.Peer.Node), msg: a.Msg}
		if a.Msg.Buf != core.NoBuf {
			to := c.n[e.to].pool
			buf, err := to.Alloc(len(r.pool.Bytes(a.Msg.Buf)))
			if err != nil {
				c.t.Fatal(err)
			}
			copy(to.Bytes(buf), r.pool.Bytes(a.Msg.Buf))
			r.pool.Free(a.Msg.Buf)
			e.msg.Buf = buf
		}
		c.inbox = append(c.inbox, e)
	}
}

// deliver hands over every queued message, those it causes included, that keep allows and no cut drops.
func (c *group) deliver(keep func(e envelope) bool) {
	for k := 0; k < len(c.inbox); k++ {
		e := c.inbox[k]
		if c.cut[e.from] || c.cut[e.to] || keep != nil && !keep(e) {
			if e.msg.Buf != core.NoBuf {
				c.n[e.to].pool.Free(e.msg.Buf)
			}
			continue
		}
		c.call(e.to, func(ctx *core.Context, out *Output) {
			c.n[e.to].s.OnEvent(ctx, &core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{Node: uint32(e.from)}, Msg: e.msg}, out)
		})
	}
	c.inbox = c.inbox[:0]
}

func (c *group) durable(i int) {
	r := c.n[i]
	round := r.flush
	if round == 0 {
		c.t.Fatalf("replica %d has no round in flight", i)
	}
	r.flush = 0
	c.call(i, func(ctx *core.Context, out *Output) { r.s.Durable(ctx, round, out) })
}

// settle delivers and flushes until nothing is left to do.
func (c *group) settle() {
	for range 10_000 {
		c.deliver(nil)
		flushed := false
		for i, r := range c.n {
			if r.flush != 0 {
				c.durable(i)
				flushed = true
			}
		}
		if !flushed && len(c.inbox) == 0 {
			return
		}
	}
	c.t.Fatal("the group did not settle")
}

func (c *group) leader() int {
	for i, r := range c.n {
		if !c.cut[i] && r.s.Raft().Leader() == uint64(i)+1 {
			return i
		}
	}
	return -1
}

// elect ticks the replicas not cut off 10 ms at a time until one of them leads, and returns it.
func (c *group) elect() int {
	for range 1000 {
		if l := c.leader(); l >= 0 {
			c.settle()
			return l
		}
		c.tick()
	}
	c.t.Fatal("no leader")
	return -1
}

// heartbeat ticks for a heartbeat, so a leader tells every replica its commit index.
func (c *group) heartbeat() {
	for range raftshard.HeartbeatTick {
		c.tick()
	}
}

// tick moves the clock 10 ms, hands every replica not cut off a timer event and settles.
func (c *group) tick() {
	c.now += raftshard.TickInterval
	for i := range c.n {
		if !c.cut[i] {
			c.call(i, func(ctx *core.Context, out *Output) {
				c.n[i].s.OnEvent(ctx, &core.Event{Kind: core.EvTimer, Timer: 1}, out)
			})
		}
	}
	c.settle()
}

func (c *group) txn(i int, t Txn, token uint64) {
	c.call(i, func(ctx *core.Context, out *Output) {
		if err := c.n[i].s.Txn(ctx, &t, token, out); err != nil {
			c.t.Fatalf("replica %d refused %+v: %v", i, t, err)
		}
	})
}

// dump is replica i's whole state.
func (c *group) dump(i int) string {
	var r ScanResult
	if err := c.n[i].s.Scan(nil, nil, 1<<20, &r); err != nil {
		c.t.Fatal(err)
	}
	var b bytes.Buffer
	for _, kv := range r.KVs {
		fmt.Fprintf(&b, "%x=%x\n", kv.Key, kv.Value)
	}
	return b.String()
}

// agree fails unless every replica not cut off applied to the same index and holds the same state.
func (c *group) agree() {
	c.t.Helper()
	want, first := "", true
	for i, r := range c.n {
		if c.cut[i] {
			continue
		}
		if applied, _ := r.s.Raft().Applied(); r.s.State().Applied() != applied {
			c.t.Fatalf("replica %d's state applied %d, its Raft replica handed over %d", i, r.s.State().Applied(), applied)
		}
		if d := c.dump(i); first {
			want, first = d, false
		} else if d != want {
			c.t.Fatalf("replica %d holds\n%s\nanother\n%s", i, d, want)
		}
	}
}

func (c *group) answer(i int, token uint64) Answer {
	c.t.Helper()
	a, ok := c.n[i].answers[token]
	if !ok {
		c.t.Fatalf("replica %d never answered token %d", i, token)
	}
	return a
}

func create(dir string, name int, inode uint64, now uint64) Txn {
	entry := []byte(fmt.Sprintf("%s/d/%04d", dir, name))
	return ops(CheckAbsent(entry, true), Put(entry, be(inode)), Put(append(be(inode), 'a'), be(now)), Update([]byte(dir+"/a"), 0, Bump, now), Update([]byte(dir+"/a"), 8, Bump, now))
}

// The Txns that arrive at a leader during a round go out as one entry in its next round; of two
// racing compare-and-sets in it exactly one commits, and 64 creates in one directory all commit,
// since the parent's times move by Update.
func TestOneEntryPerLeaderPerRound(t *testing.T) {
	c := newGroup(t, 3, false)
	l := c.elect()
	c.n[l].rounds = nil
	c.txn(l, ops(Put(b("first"), b("1"))), 0)
	for i := range 100 {
		c.txn(l, ops(Put([]byte(fmt.Sprintf("k%03d", i)), b("v"))), uint64(i)+1)
	}
	c.txn(l, ops(CheckAbsent(b("lock"), true), Put(b("lock"), b("x"))), 101)
	c.txn(l, ops(CheckAbsent(b("lock"), true), Put(b("lock"), b("y"))), 102)
	for i := range 64 {
		c.txn(l, create("dir", i, uint64(1000+i), 7), uint64(200+i))
	}
	c.settle()
	if !slices.Equal(c.n[l].rounds, []int{1, 1}) {
		t.Fatalf("the leader's rounds held %v Txn entries, want [1 1]", c.n[l].rounds)
	}
	for token := range uint64(101) {
		if a := c.answer(l, token); a.Err != nil || a.Result.FailedCheck != -1 {
			t.Fatalf("token %d: %+v", token, a)
		}
	}
	if a, b := c.answer(l, 101), c.answer(l, 102); a.Result.FailedCheck != -1 || b.Result.FailedCheck != 0 || string(b.Result.Actual) != "x" {
		t.Fatalf("racing compare-and-sets answered %+v and %+v, want the first to commit", a, b)
	}
	for i := range 64 {
		if a := c.answer(l, uint64(200+i)); a.Err != nil || a.Result.FailedCheck != -1 || !slices.Equal(a.Result.Updated, []uint64{7 + uint64(i), 7 + uint64(i)}) {
			t.Fatalf("create %d answered %+v", i, a)
		}
	}
	c.agree()
}

func TestAThousandCreatesOverAHundredRoundsAllCommit(t *testing.T) {
	c := newGroup(t, 3, false)
	l := c.elect()
	c.n[l].rounds = nil
	for round := range 100 {
		for i := range 10 {
			name := round*10 + i
			c.txn(l, create("dir", name, uint64(1000+name), 7), uint64(name))
		}
		c.settle()
	}
	if len(c.n[l].rounds) > 200 {
		t.Fatalf("1,000 creates took %d rounds", len(c.n[l].rounds))
	}
	for token := range uint64(1000) {
		if a := c.answer(l, token); a.Err != nil || a.Result.FailedCheck != -1 {
			t.Fatalf("create %d answered %+v", token, a)
		}
	}
	c.agree()
}

// A follower refuses a Txn, a leader's Txns of a term that lost its log are answered dropped once
// a later term's entry applies, and a restarted replica rebuilds its state from its log.
func TestTxnsOutsideTheLeadersLogAreAnsweredDropped(t *testing.T) {
	c := newGroup(t, 3, false)
	l := c.elect()
	c.txn(l, ops(Put(b("a"), b("1"))), 1)
	c.settle()
	f := (l + 1) % 3
	tx := ops(Put(b("b"), b("1")))
	c.call(f, func(ctx *core.Context, out *Output) {
		if err := c.n[f].s.Txn(ctx, &tx, 9, out); !errors.Is(err, raft.ErrProposalDropped) {
			t.Fatalf("a follower took a Txn: %v", err)
		}
	})
	c.cut[l] = true
	c.txn(l, ops(Put(b("lost"), b("1"))), 2)
	c.txn(l, ops(Put(b("lost2"), b("1"))), 3)
	c.settle()
	nl := c.elect()
	c.cut[l] = false
	c.txn(nl, ops(Put(b("c"), b("1"))), 4)
	c.heartbeat()
	for _, token := range []uint64{2, 3} {
		if a := c.answer(l, token); !errors.Is(a.Err, raft.ErrProposalDropped) {
			t.Fatalf("token %d of the deposed leader answered %+v", token, a)
		}
	}
	if a := c.answer(nl, 4); a.Err != nil || a.Result.FailedCheck != -1 {
		t.Fatalf("the new leader answered %+v", a)
	}
	c.agree()
	if _, ok := get(c.n[l].s.State(), "lost"); ok {
		t.Fatal("a Txn answered dropped applied")
	}
	c.restart(3 - l - nl)
	c.heartbeat()
	c.agree()
}

// A snapshot the leader builds in the call that hands entries over to apply holds every one of
// them, since the state machine applies them within the call; one applying them after the call
// returns would have the snapshot built at an index its state has not reached.
func TestASnapshotHoldsTheEntriesHandedOverInItsCall(t *testing.T) {
	for _, late := range []bool{false, true} {
		built := func() (panicked any) {
			defer func() { panicked = recover() }()
			c := newGroup(t, 3, late)
			l := c.elect()
			g, f := (l+1)%3, (l+2)%3
			c.txn(l, ops(Put(b("a"), b("1"))), 1)
			c.settle()
			c.cut[f] = true
			c.txn(l, ops(Put(b("b"), b("1"))), 2)
			c.settle()
			if err := c.n[l].st.Compact(c.n[l].s.Applied()); err != nil {
				t.Fatal(err)
			}
			c.txn(l, ops(Put(b("c"), b("1"))), 3)
			c.txn(l, ops(Put(b("d"), b("1"))), 4)
			toG := func(e envelope) bool { return e.to == g }
			c.deliver(toG)
			c.durable(g)
			c.deliver(func(e envelope) bool { return e.from == g })
			c.durable(l)
			c.deliver(toG)
			c.durable(g)
			c.deliver(func(e envelope) bool { return e.from == g })
			c.cut[f] = false
			c.call(l, func(ctx *core.Context, out *Output) {
				c.n[l].s.OnEvent(ctx, &core.Event{Kind: core.EvPeerLost, Peer: core.NodeID{Node: uint32(f)}}, out)
			})
			before := c.n[l].s.State().Applied()
			c.durable(l)
			if c.n[l].s.State().Applied() == before {
				t.Fatal("the leader's durable round handed nothing over")
			}
			c.settle()
			if c.n[f].st.Base().Index <= 1 {
				t.Fatal("the cut-off follower restored no snapshot")
			}
			c.agree()
			c.restart(f)
			c.heartbeat()
			c.agree()
			return nil
		}()
		if (built != nil) != late {
			t.Fatalf("applied after the call %v: %v", late, built)
		}
	}
}

// A deposed leader's entry that the next leader commits applies at it under the entry's own
// term, so it answers the Txn with its result, not dropped.
func TestADeposedLeadersCommittedTxnIsAnsweredWithItsResult(t *testing.T) {
	c := newGroup(t, 3, false)
	l := c.elect()
	g := (l + 1) % 3
	c.txn(l, ops(Put(b("a"), b("1"))), 1)
	c.deliver(func(e envelope) bool { return e.to == g })
	c.durable(g)
	c.cut[l] = true
	c.settle()
	if nl := c.elect(); nl != g {
		t.Fatalf("replica %d won the election, not %d, which holds the deposed leader's entry", nl, g)
	}
	c.cut[l] = false
	c.heartbeat()
	if a := c.answer(l, 1); a.Err != nil || a.Result.FailedCheck != -1 {
		t.Fatalf("the deposed leader answered its committed Txn %+v", a)
	}
	c.agree()
	if v, _ := get(c.n[l].s.State(), "a"); v != "1" {
		t.Fatalf("a = %q", v)
	}
}

// proposeNewer has replica i propose, as a newer release would, an entry of format 2 holding
// t, which only replicas whose newest format is 2 decode, and returns its index.
func (c *group) proposeNewer(i int, t Txn) uint64 {
	data := appendTxn([]byte{entryFormat + 1}, &t)
	c.call(i, func(ctx *core.Context, out *Output) {
		if err := c.n[i].s.Raft().Propose(ctx, data, &out.Raft); err != nil {
			c.t.Fatal(err)
		}
	})
	last, _ := c.n[i].s.Raft().Applied()
	return last + 1
}

// A follower that cannot decode a committed entry stops applying before it, refuses Txns and
// reads, and halts there again after a restart, while the others keep committing.
func TestAFollowerHaltsAtAnEntryItCannotDecode(t *testing.T) {
	c := newGroup(t, 3, false)
	l := c.elect()
	g, f := (l+1)%3, (l+2)%3
	c.n[l].s.newest, c.n[g].s.newest = 2, 2
	c.txn(l, ops(Put(b("a"), b("1"))), 1)
	c.settle()
	at := c.proposeNewer(l, ops(Put(b("x"), b("1"))))
	c.settle()
	c.txn(l, ops(Put(b("c"), b("1"))), 2)
	c.settle()
	halted := func() {
		t.Helper()
		if h, applied := c.n[f].s.Halted(), c.n[f].s.State().Applied(); h != at || applied != at-1 {
			t.Fatalf("the follower halted at %d with %d applied, want %d and %d", h, applied, at, at-1)
		}
		if i, _ := c.n[f].s.Applied(); i >= at {
			t.Fatalf("the follower would compact its log to %d, past its state's %d", i, at-1)
		}
	}
	halted()
	if a := c.answer(l, 2); a.Err != nil || c.dump(l) != c.dump(g) {
		t.Fatalf("the others stopped committing: %+v", a)
	}
	tx := ops(Put(b("d"), b("1")))
	var r ScanResult
	c.call(f, func(ctx *core.Context, out *Output) {
		if err := c.n[f].s.Txn(ctx, &tx, 9, out); !errors.Is(err, ErrHalted) {
			t.Fatalf("a halted replica took a Txn: %v", err)
		}
	})
	if err := c.n[f].s.Scan(nil, nil, 10, &r); !errors.Is(err, ErrHalted) {
		t.Fatalf("a halted replica served a scan: %v", err)
	}
	c.restart(f)
	c.heartbeat()
	halted()
}

// A leader that cannot decode a committed entry halts, answers its Txns dropped and hands its
// leadership to a voter, under which the group keeps committing.
func TestAHaltedLeaderHandsOverItsLeadership(t *testing.T) {
	c := newGroup(t, 3, false)
	l := c.elect()
	for i := range c.n {
		if i != l {
			c.n[i].s.newest = 2
		}
	}
	c.txn(l, ops(Put(b("a"), b("1"))), 1)
	at := c.proposeNewer(l, ops(Put(b("x"), b("1"))))
	c.txn(l, ops(Put(b("b"), b("1"))), 2)
	c.settle()
	c.heartbeat()
	if h := c.n[l].s.Halted(); h < at {
		t.Fatalf("the leader halted at %d, want at least %d", h, at)
	}
	if a := c.answer(l, 1); a.Err != nil {
		t.Fatalf("the Txn before the entry answered %+v", a)
	}
	if a := c.answer(l, 2); !errors.Is(a.Err, raft.ErrProposalDropped) {
		t.Fatalf("the Txn after the entry answered %+v", a)
	}
	nl := c.leader()
	if nl < 0 || nl == l {
		t.Fatalf("leader %d after the halted leader stepped down", nl)
	}
	c.txn(nl, ops(Put(b("c"), b("1"))), 3)
	c.settle()
	if a := c.answer(nl, 3); a.Err != nil || a.Result.FailedCheck != -1 {
		t.Fatalf("the new leader answered %+v", a)
	}
}

// spin delivers every message and completes every flush n times, with the clock standing still.
func (c *group) spin(n int) {
	for range n {
		c.deliver(nil)
		for i, r := range c.n {
			if r.flush != 0 {
				c.durable(i)
			}
		}
	}
}

// When every replica halts at an entry, each hands its leadership on at most once an election
// timeout, so the term does not climb at the network's speed.
func TestHaltedReplicasHandOffLeadershipAtMostOnceAnElectionTimeout(t *testing.T) {
	c := newGroup(t, 3, false)
	l := c.elect()
	at := c.proposeNewer(l, ops(Put(b("x"), b("1"))))
	term := c.n[l].s.Raft().Term()
	c.spin(200)
	for i := range c.n {
		if h := c.n[i].s.Halted(); h != at {
			t.Fatalf("replica %d halted at %d, want %d", i, h, at)
		}
		if got := c.n[i].s.Raft().Term(); got > term+uint64(len(c.n)) {
			t.Fatalf("replica %d is at term %d, %d after the halt", i, got, got-term)
		}
	}
}

// An entry holding a Txn that decodes and then one that does not halts every replica before it:
// none applies the first.
func TestNoReplicaAppliesTheDecodablePrefixOfAnEntry(t *testing.T) {
	c := newGroup(t, 3, false)
	l := c.elect()
	good := ops(Put(b("x"), b("1")))
	data := append(appendTxn([]byte{entryFormat}, &good), 0, 1, 9, 1, 'k')
	c.call(l, func(ctx *core.Context, out *Output) {
		if err := c.n[l].s.Raft().Propose(ctx, data, &out.Raft); err != nil {
			t.Fatal(err)
		}
	})
	c.spin(20)
	for i := range c.n {
		if c.n[i].s.Halted() == 0 {
			t.Fatalf("replica %d did not halt", i)
		}
		if _, ok := get(c.n[i].s.State(), "x"); ok {
			t.Fatalf("replica %d applied the Txn before the one that does not decode", i)
		}
	}
}

// A halted follower acts as a down one: its log stops growing while the other two keep
// committing, it never campaigns, and it still learns that it was removed.
func TestAHaltedFollowerTakesNoAppends(t *testing.T) {
	c := newGroup(t, 3, false)
	l := c.elect()
	g, f := (l+1)%3, (l+2)%3
	c.n[l].s.newest, c.n[g].s.newest = 2, 2
	c.proposeNewer(l, ops(Put(b("x"), b("1"))))
	c.settle()
	last, _ := c.n[f].st.LastIndex()
	for k := range uint64(5) {
		c.txn(l, ops(Put(b("k"), be(k))), k+1)
		c.settle()
	}
	c.heartbeat()
	if a := c.answer(l, 5); a.Err != nil {
		t.Fatalf("the others stopped committing: %+v", a)
	}
	if now, _ := c.n[f].st.LastIndex(); now != last {
		t.Fatalf("the halted follower's log grew from %d to %d", last, now)
	}
	term := c.n[f].s.Raft().Term()
	c.cut[l] = true
	for range 3 * raftshard.ElectionTick {
		c.tick()
	}
	if got := c.n[f].s.Raft().Term(); got != term {
		t.Fatalf("the halted follower moved from term %d to %d", term, got)
	}
	c.cut[l] = false
	c.tellRemoved(f, l)
}

// peerLost tells replica i that its connection to replica to failed, so its leader probes to again.
func (c *group) peerLost(i, to int) {
	c.call(i, func(ctx *core.Context, out *Output) {
		c.n[i].s.OnEvent(ctx, &core.Event{Kind: core.EvPeerLost, Peer: core.NodeID{Node: uint32(to)}}, out)
	})
}

// A halted follower restores no snapshot, as it takes no Raft message: one that did would hold
// a log past its state, and restart from the snapshot no longer halted.
func TestAHaltedFollowerRestoresNoSnapshot(t *testing.T) {
	c := newGroup(t, 3, false)
	l := c.elect()
	g, f := (l+1)%3, (l+2)%3
	c.n[l].s.newest, c.n[g].s.newest = 2, 2
	c.proposeNewer(l, ops(Put(b("x"), b("1"))))
	c.settle()
	c.txn(l, ops(Put(b("a"), b("1"))), 1)
	c.settle()
	if err := c.n[l].st.Compact(c.n[l].s.Applied()); err != nil {
		t.Fatal(err)
	}
	base := c.n[f].st.Base().Index
	c.peerLost(l, f)
	c.txn(l, ops(Put(b("b"), b("1"))), 2)
	c.settle()
	c.heartbeat()
	if got := c.n[f].st.Base().Index; got != base || c.n[f].s.Halted() == 0 {
		t.Fatalf("the halted follower's base moved from %d to %d, halted %d", base, got, c.n[f].s.Halted())
	}
}

// A halted leader builds no snapshot data, as its state stopped before the index the snapshot
// is at, so a follower behind its compaction refuses the snapshot rather than restore a state
// that lacks the entries before that index.
func TestAHaltedLeaderBuildsNoSnapshotData(t *testing.T) {
	c := newGroup(t, 3, false)
	l := c.elect()
	f := (l + 2) % 3
	c.n[f].s.newest = 2
	c.txn(l, ops(Put(b("a"), b("1"))), 1)
	c.settle()
	c.cut[f] = true
	c.txn(l, ops(Put(b("b"), b("1"))), 2)
	c.settle()
	if err := c.n[l].st.Compact(c.n[l].s.Applied()); err != nil {
		t.Fatal(err)
	}
	c.proposeNewer(l, ops(Put(b("x"), b("1"))))
	c.settle()
	if c.n[l].s.Halted() == 0 {
		t.Fatal("the leader did not halt")
	}
	c.cut[f] = false
	applied := c.n[f].s.State().Applied()
	c.peerLost(l, f)
	c.heartbeat()
	if c.n[f].st.Base().Index != 1 || c.n[f].s.State().Applied() != applied {
		t.Fatalf("the follower moved from %d to base %d and %d applied", applied, c.n[f].st.Base().Index, c.n[f].s.State().Applied())
	}
}

// A deposed leader that catches up by a snapshot answers the Txns it still waits for dropped at
// the restore: the snapshot may or may not hold them, and nothing after it says which.
func TestWaitingTxnsAreAnsweredDroppedAtASnapshotRestore(t *testing.T) {
	c := newGroup(t, 3, false)
	l := c.elect()
	c.txn(l, ops(Put(b("a"), b("1"))), 1)
	c.settle()
	c.cut[l] = true
	c.txn(l, ops(Put(b("lost"), b("1"))), 2)
	c.settle()
	nl := c.elect()
	c.txn(nl, ops(Put(b("b"), b("1"))), 3)
	c.settle()
	if err := c.n[nl].st.Compact(c.n[nl].s.Applied()); err != nil {
		t.Fatal(err)
	}
	c.cut[l] = false
	c.heartbeat()
	if base := c.n[l].st.Base().Index; base <= 1 {
		t.Fatalf("the deposed leader's base is %d: it restored no snapshot", base)
	}
	if a := c.answer(l, 2); !errors.Is(a.Err, raft.ErrProposalDropped) {
		t.Fatalf("the deposed leader answered its waiting Txn %+v", a)
	}
	c.agree()
}

// A replica told it was removed from its group does nothing more, so it answers every Txn it
// still waits for or gathered dropped at once.
func TestARemovedReplicaAnswersItsTxnsDropped(t *testing.T) {
	c := newGroup(t, 3, false)
	l := c.elect()
	c.cut[l] = true
	c.txn(l, ops(Put(b("a"), b("1"))), 1)
	c.txn(l, ops(Put(b("b"), b("1"))), 2)
	c.tellRemoved(l, (l+1)%3)
	for _, token := range []uint64{1, 2} {
		if a := c.answer(l, token); !errors.Is(a.Err, raft.ErrProposalDropped) {
			t.Fatalf("token %d answered %+v", token, a)
		}
	}
}

// tellRemoved hands replica i from replica from raftshard's answer to a replica its sender's
// applied configuration does not hold, and fails unless i reports itself removed: the group,
// kind 4, then from, to, the sender's applied index, and two unused words.
func (c *group) tellRemoved(i, from int) {
	c.t.Helper()
	body := binary.LittleEndian.AppendUint64(nil, 5)
	body = append(body, 4)
	for _, v := range []uint64{uint64(from) + 1, uint64(i) + 1, 1 << 40, 0, 0} {
		body = binary.LittleEndian.AppendUint64(body, v)
	}
	var msg core.Msg
	if err := msg.SetInline(body); err != nil {
		c.t.Fatal(err)
	}
	c.call(i, func(ctx *core.Context, out *Output) {
		c.n[i].s.OnEvent(ctx, &core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{Node: uint32(from)}, Msg: msg}, out)
		if !out.Raft.Removed {
			c.t.Fatal("the replica did not learn it was removed")
		}
	})
}
