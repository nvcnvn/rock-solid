package meta

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math/rand/v2"
	"slices"
	"testing"

	"go.etcd.io/raft/v3"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

const (
	ctlGroup  = 1
	dataGroup = 5
)

// oneShard is the phase 4 shard map: the control-plane group and one data shard, whose voters are
// replicas 1 to 3 on nodes 0 to 2, listed from the one on node first.
func oneShard(first int) ShardMap {
	m := ShardMap{Groups: []Group{{ID: ctlGroup}, {ID: dataGroup, Start: ctlEnd}}}
	for i := range 3 {
		n := (first + i) % 3
		m.Groups[1].Voters = append(m.Groups[1].Voters, Replica{ID: uint64(n) + 1, Node: uint32(n)})
	}
	return m
}

// twoShards splits the data shard at slot 9 of volume 1, into group 7.
func twoShards() ShardMap {
	m := oneShard(0)
	m.Groups = append(m.Groups, Group{ID: 7, Start: mustKey(&Key{Space: SpaceFS, Volume: 1, Inode: ino(9, 0), Tag: SlotNext})[:13]})
	return m
}

func attr(vol uint32, slot uint32, seq uint64) []byte {
	return mustKey(&Key{Space: SpaceFS, Volume: vol, Inode: ino(slot, seq), Tag: InodeAttr})
}

func wantCross(t *testing.T, err error, groups ...uint64) {
	t.Helper()
	var c *CrossShard
	if !errors.As(err, &c) || !errors.Is(err, ErrCrossShard) || !slices.Equal(c.Groups, groups) {
		t.Fatalf("got %v, want ErrCrossShard naming %v", err, groups)
	}
}

// With one data shard every key but ctl/'s routes to it, and ctl/ to the control-plane group;
// a Txn or Scan that touches both is refused, since they are two Raft groups.
func TestOneShardRoutesEveryKeyToItsGroup(t *testing.T) {
	m := oneShard(0)
	if err := m.CheckRanges(); err != nil {
		t.Fatal(err)
	}
	ctl := mustKey(&Key{Space: SpaceCtl, Tag: CtlPool, Name: b("fast")})
	vol := mustKey(&Key{Space: SpaceVolume, Volume: 3, Tag: VolumeAttach})
	s3 := mustKey(&Key{Space: SpaceS3, Volume: 3, Name: b("photos/a")})
	for _, c := range []struct {
		t     Txn
		group uint64
	}{
		{ops(Put(attr(1, 2, 1), b("v")), Update(attr(1, 9, 1), 0, Bump, 1), Put(vol, b("v")), Put(s3, b("v"))), dataGroup},
		{ops(DeleteRange(attr(1, 2, 1), PrefixEnd([]byte{SpaceS3}))), dataGroup},
		{ops(Check(ctl, b("1"), false), Put(ctl, b("2"))), ctlGroup},
		{ops(CheckEmpty(ctl, ctlEnd, false)), ctlGroup},
		{Txn{Ops: []Op{Put(attr(1, 2, 1), b("v"))}, Replay: replayKey(1, 4)}, dataGroup},
	} {
		if g, err := m.Route(&c.t); err != nil || g != c.group {
			t.Fatalf("%+v routed to %d, %v; want %d", c.t, g, err, c.group)
		}
	}
	_, err := m.Route(&Txn{Ops: []Op{Put(ctl, b("1")), Put(attr(1, 2, 1), b("v"))}})
	wantCross(t, err, ctlGroup, dataGroup)
	_, err = m.Route(&Txn{Ops: []Op{DeleteRange([]byte{SpaceCtl}, []byte{SpaceFS})}})
	wantCross(t, err, ctlGroup, dataGroup)
	_, err = m.RouteScan(nil, nil)
	wantCross(t, err, ctlGroup, dataGroup)
	if g, err := m.RouteScan([]byte{SpaceCtl}, ctlEnd); err != nil || g != ctlGroup {
		t.Fatalf("a scan of ctl/ routed to %d, %v", g, err)
	}
	if g, err := m.RouteScan(ctlEnd, nil); err != nil || g != dataGroup {
		t.Fatalf("a scan of the data keys routed to %d, %v", g, err)
	}
	if _, err := m.Route(&Txn{}); !errors.Is(err, ErrNoOps) {
		t.Fatalf("a Txn without ops routed: %v", err)
	}
}

// A Txn whose keys, ranges or replay key reach two data shards is refused naming both; a range
// that ends at a shard's first key stays in the shard before it.
func TestTwoShardsRefuseATxnSpanningThem(t *testing.T) {
	m := twoShards()
	if err := m.CheckRanges(); err != nil {
		t.Fatal(err)
	}
	split := m.Groups[2].Start
	if g, err := m.Route(&Txn{Ops: []Op{CheckEmpty(attr(1, 2, 1), split, false)}}); err != nil || g != dataGroup {
		t.Fatalf("a range ending at the split routed to %d, %v", g, err)
	}
	if g, err := m.Route(&Txn{Ops: []Op{Put(attr(1, 9, 1), b("v"))}}); err != nil || g != 7 {
		t.Fatalf("a key past the split routed to %d, %v", g, err)
	}
	_, err := m.Route(&Txn{Ops: []Op{Put(attr(1, 2, 1), b("v")), Put(attr(1, 9, 1), b("v"))}})
	wantCross(t, err, dataGroup, 7)
	_, err = m.Route(&Txn{Ops: []Op{DeleteRange(attr(1, 2, 1), attr(1, 9, 1))}})
	wantCross(t, err, dataGroup, 7)
	_, err = m.Route(&Txn{Ops: []Op{Put(attr(1, 9, 1), b("v"))}, Replay: replayKey(1, 4)})
	wantCross(t, err, dataGroup, 7)
	_, err = m.Route(&Txn{Ops: []Op{Put([]byte{SpaceCtl, CtlPool}, b("v")), DeleteRange(attr(1, 2, 1), attr(1, 9, 1))}})
	wantCross(t, err, ctlGroup, dataGroup, 7)
	_, err = m.RouteScan(attr(1, 2, 1), nil)
	wantCross(t, err, dataGroup, 7)

	r, err := NewRouter(&m, 1)
	if err != nil {
		t.Fatal(err)
	}
	var out RouterOutput
	ctx := core.Context{Now: 1}
	tx := Txn{Ops: []Op{Put(attr(1, 2, 1), b("v")), Put(attr(1, 9, 1), b("v"))}}
	if err := r.Txn(&ctx, &tx, 1, &out); !errors.Is(err, ErrCrossShard) || len(out.Sends) != 0 {
		t.Fatalf("the router took a cross-shard Txn: %v, %+v", err, out)
	}
}

// The control-plane group's range is [ctl/, PrefixEnd(ctl/)) and the first, and never splits.
func TestCheckRangesRefusesASplitInsideTheControlPlaneGroup(t *testing.T) {
	for name, edit := range map[string]func(m *ShardMap){
		"split inside ctl/": func(m *ShardMap) {
			m.Groups = slices.Insert(m.Groups, 1, Group{ID: 9, Start: []byte{SpaceCtl, CtlSession}})
		},
		"ctl/ ends early":   func(m *ShardMap) { m.Groups[1].Start = []byte{SpaceCtl, 0xff} },
		"ctl/ ends late":    func(m *ShardMap) { m.Groups[1].Start = []byte{SpaceFS} },
		"no first range":    func(m *ShardMap) { m.Groups[0].Start = []byte{0} },
		"ranges not rising": func(m *ShardMap) { m.Groups[2].Start = []byte{SpaceCtl + 1} },
		"ctl group twice":   func(m *ShardMap) { m.Groups[2].ID = ctlGroup },
		"data group twice":  func(m *ShardMap) { m.Groups[2].ID = dataGroup },
		"no data shard":     func(m *ShardMap) { m.Groups = m.Groups[:1] },
	} {
		m := twoShards()
		edit(&m)
		if err := m.CheckRanges(); !errors.Is(err, ErrRanges) {
			t.Fatalf("%s: %v", name, err)
		}
		if _, err := NewRouter(&m, 1); !errors.Is(err, ErrRanges) {
			t.Fatalf("%s: a router took the map: %v", name, err)
		}
	}
}

// A client Txn, one with a replay key, that writes or removes a replay-tagged key is refused
// before proposing, whether or not the key decodes; reading one is not, nor the internal Txn that
// drops a departed session's records.
func TestAClientTxnWritingAReplayTaggedKeyIsRefused(t *testing.T) {
	c := newGroup(t, 3, false)
	l := c.elect()
	other := replayKey(2, 3)
	raw := append(slices.Clone(replayKey(1, 1)), 0)
	session := replays
	slot := mustKey(&Key{Space: SpaceFS, Volume: 1, Inode: ino(2, 0), Tag: SlotNext})[:13]
	refused := []Op{
		Put(other, b("x")),
		Delete(other),
		Update(other, 0, Add, 1),
		Put(raw, b("x")),
		Put(replays[:14], b("x")),
		DeleteRange(session, PrefixEnd(session)),
		DeleteRange(slot, PrefixEnd(slot)),
		DeleteRange(attr(1, 2, 1), attr(1, 3, 1)),
		DeleteRange([]byte{SpaceFS}, []byte{SpaceS3}),
	}
	rounds := len(c.n[l].rounds)
	for i, op := range refused {
		tx := Txn{Ops: []Op{op}, Replay: replayKey(1, 9), Oldest: 9}
		c.call(l, func(ctx *core.Context, out *Output) {
			if err := c.n[l].s.Txn(ctx, &tx, uint64(i)+1, out); !errors.Is(err, ErrReplayTagged) {
				t.Fatalf("op %d %+v: %v", i, op, err)
			}
		})
	}
	if len(c.n[l].rounds) != rounds {
		t.Fatal("a refused Txn was proposed")
	}
	c.txn(l, Txn{Ops: []Op{CheckAbsent(other, false), DeleteRange(attr(1, 2, 1), attr(1, 2, 2))}, Replay: replayKey(1, 10)}, 100)
	c.txn(l, ops(DeleteRange(session, PrefixEnd(session))), 102)
	c.settle()
	for _, token := range []uint64{100, 102} {
		if a := c.answer(l, token); a.Err != nil || a.Result.FailedCheck != -1 {
			t.Fatalf("token %d answered %+v", token, a)
		}
	}
}

// replayIn agrees with its definition, checked by brute force: [start, end) holds a key that
// starts with some slot's replay-tagged prefix. The keys' volume and slot bytes are 0 but the
// slot's last, so the slots are 256 and one past them stands for every higher slot.
func TestReplayInMatchesItsDefinition(t *testing.T) {
	pick := func(rng *rand.Rand, opts ...byte) byte {
		if rng.IntN(8) == 0 {
			return byte(rng.Uint32())
		}
		return opts[rng.IntN(len(opts))]
	}
	gen := func(rng *rand.Rand) []byte {
		k := make([]byte, rng.IntN(18))
		for i := range k {
			switch {
			case i == 0:
				k[i] = pick(rng, SpaceVolume, SpaceFS, SpaceS3)
			case i < 7:
			case i == 7:
				k[i] = pick(rng, 0, 1, 0xfe, 0xff)
			case i < 13:
				k[i] = pick(rng, 0, 0, 0, 1)
			case i == 13:
				k[i] = pick(rng, SlotPurge, SlotReplay, SlotFree)
			default:
				k[i] = pick(rng, 0, 0xff)
			}
		}
		return k
	}
	prefix := func(x uint64) []byte {
		p := make([]byte, replayPrefix)
		p[0], p[13] = SpaceFS, SlotReplay
		var w [8]byte
		binary.BigEndian.PutUint64(w[:], x)
		copy(p[1:8], w[1:])
		return p
	}
	var prefixes, ends [][]byte
	for x := range uint64(257) {
		prefixes = append(prefixes, prefix(x))
		ends = append(ends, PrefixEnd(prefix(x)))
	}
	seeds.Run(t, 1, 100, func(seed uint64) {
		rng := rand.New(rand.NewPCG(seed, 0))
		for range 300 {
			start, end := gen(rng), gen(rng)
			if i := rng.IntN(len(start) + 1); rng.IntN(2) == 0 && i < len(end) {
				end = append(slices.Clone(start[:i]), end[i:]...)
			}
			if bytes.Compare(start, end) >= 0 {
				continue
			}
			want := false
			for x, p := range prefixes {
				want = want || bytes.Compare(p, end) < 0 && bytes.Compare(start, ends[x]) < 0
			}
			if got := replayIn(start, end); got != want {
				t.Fatalf("replayIn(%x, %x) = %v, want %v", start, end, got, want)
			}
		}
	})
	top := prefix(1<<56 - 1)
	above := append(slices.Clone(top[:8]), 1)
	if replayIn(above, []byte{SpaceS3}) || !replayIn(top[:8], []byte{SpaceS3}) {
		t.Fatal("replayIn is wrong at the last slot")
	}
}

// sendTo hands a router's sends to the group's replicas and the replicas' answers back, until
// none is left, and returns the nodes each Txn went to.
func sendTo(c *group, r *Router, out *RouterOutput) []uint32 {
	var to []uint32
	for len(out.Sends) > 0 {
		sends := slices.Clone(out.Sends)
		out.Sends = out.Sends[:0]
		for _, s := range sends {
			to = append(to, s.Node)
			n := int(s.Node)
			var err error
			c.call(n, func(ctx *core.Context, o *Output) { err = c.n[n].s.Txn(ctx, s.Txn, s.Token, o) })
			var a Answer
			if err != nil {
				a = Answer{Token: s.Token, Err: err}
			} else {
				c.settle()
				a = c.answer(n, s.Token)
			}
			ctx := c.ctx(n)
			r.Answer(&ctx, &a, out)
		}
	}
	return to
}

// entries is how many Txn entries replica l's rounds held since its rounds were last cleared.
func entries(c *group, l int) int {
	n := 0
	for _, k := range c.n[l].rounds {
		n += k
	}
	return n
}

// A follower answers NotLeader naming the leader, which the router caches and resends to: the Txn
// is proposed once, at the leader, and the next Txn goes straight there.
func TestTheRouterResendsToTheLeaderAFollowerNames(t *testing.T) {
	c := newGroup(t, 3, false)
	l := c.elect()
	f := (l + 1) % 3
	m := oneShard(f)
	r, err := NewRouter(&m, 1)
	if err != nil {
		t.Fatal(err)
	}
	c.n[l].rounds = nil
	var out RouterOutput
	ctx := c.ctx(f)
	tx := Txn{Ops: []Op{Put(attr(1, 2, 1), b("v"))}, Replay: replayKey(1, 1)}
	if err := r.Txn(&ctx, &tx, 1, &out); err != nil {
		t.Fatal(err)
	}
	if to := sendTo(c, r, &out); !slices.Equal(to, []uint32{uint32(f), uint32(l)}) {
		t.Fatalf("the Txn went to %v; the leader is %d", to, l)
	}
	if len(out.Answers) != 1 || out.Answers[0].Token != 1 || out.Answers[0].Err != nil || out.Answers[0].Result.FailedCheck != -1 {
		t.Fatalf("the router answered %+v", out.Answers)
	}
	if n := entries(c, l); n != 1 {
		t.Fatalf("the leader proposed %d entries", n)
	}
	out.Reset()
	tx2 := Txn{Ops: []Op{Put(attr(1, 2, 2), b("v"))}}
	if err := r.Txn(&ctx, &tx2, 2, &out); err != nil {
		t.Fatal(err)
	}
	if to := sendTo(c, r, &out); !slices.Equal(to, []uint32{uint32(l)}) {
		t.Fatalf("the second Txn went to %v; the leader is %d", to, l)
	}
	c.settle()
	c.agree()
}

// With no leader known, a replica answers NotLeader naming none; the router holds the Txn for an
// election timeout, then tries the next voter, and the Txn is proposed once.
func TestTheRouterHoldsATxnUntilTheElection(t *testing.T) {
	c := newGroup(t, 3, false)
	m := oneShard(0)
	r, err := NewRouter(&m, 1)
	if err != nil {
		t.Fatal(err)
	}
	var out RouterOutput
	ctx := c.ctx(0)
	tx := Txn{Ops: []Op{Put(attr(1, 2, 1), b("v"))}, Replay: replayKey(1, 1)}
	if err := r.Txn(&ctx, &tx, 1, &out); err != nil {
		t.Fatal(err)
	}
	if to := sendTo(c, r, &out); !slices.Equal(to, []uint32{0}) || len(out.Answers) != 0 {
		t.Fatalf("with no leader the Txn went to %v and answered %+v", to, out.Answers)
	}
	want := core.Action{Kind: core.ActSetTimer, Timer: 1, At: ctx.Now + electionWait}
	if len(out.Actions) != 1 || out.Actions[0].Kind != want.Kind || out.Actions[0].Timer != want.Timer || out.Actions[0].At != want.At {
		t.Fatalf("the router armed %+v", out.Actions)
	}
	l := c.elect()
	for _, n := range c.n {
		n.rounds = nil
	}
	out.Reset()
	ctx = c.ctx(0)
	r.OnEvent(&ctx, &core.Event{Kind: core.EvTimer, Timer: 1}, &out)
	to := sendTo(c, r, &out)
	if to[0] != 1 || to[len(to)-1] != uint32(l) || len(to) > 2 {
		t.Fatalf("after the election the Txn went to %v; the leader is %d", to, l)
	}
	if len(out.Answers) != 1 || out.Answers[0].Err != nil {
		t.Fatalf("the router answered %+v", out.Answers)
	}
	if n := entries(c, l); n != 1 {
		t.Fatalf("the leader proposed %d entries", n)
	}
}

// A Read a new owner refuses ErrGrace waits for the router's timer and then goes again to the
// same leader, rather than being answered: a new open waits out the grace period.
func TestARouterHoldsAReadRefusedInTheGracePeriod(t *testing.T) {
	m := oneShard(0)
	r, err := NewRouter(&m, 1)
	if err != nil {
		t.Fatal(err)
	}
	var out RouterOutput
	ctx := core.Context{Now: 1}
	rd := Read{Start: attr(1, 2, 1), Limit: 1, Reg: RegOpen, Session: 7, Mount: 1}
	if err := r.Read(&ctx, &rd, 1, &out); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	r.ReadAnswer(&ctx, &ReadAnswer{Token: 1, Err: NotLeader{Group: dataGroup, Leader: 2, Node: 1, Term: 3}}, &out)
	out.Reset()
	r.ReadAnswer(&ctx, &ReadAnswer{Token: 1, Err: ErrGrace}, &out)
	if len(out.Reads) != 0 || len(out.Sends) != 0 || len(out.Actions) != 1 || out.Actions[0].Timer != 1 {
		t.Fatalf("a Read refused ErrGrace answered %+v, sent %+v and armed %+v", out.Reads, out.Sends, out.Actions)
	}
	ctx.Now = out.Actions[0].At
	out.Reset()
	r.OnEvent(&ctx, &core.Event{Kind: core.EvTimer, Timer: 1}, &out)
	if len(out.Sends) != 1 || out.Sends[0].Node != 1 || out.Sends[0].Read != &rd {
		t.Fatalf("at the timer the router sent %+v", out.Sends)
	}
	out.Reset()
	r.ReadAnswer(&ctx, &ReadAnswer{Token: 1}, &out)
	if len(out.Reads) != 1 || out.Reads[0].Err != nil {
		t.Fatalf("the Read resent answered %+v", out.Reads)
	}
}

// A NotLeader of an older term leaves the cache as it is; a lost node fails the Txns sent to it
// as dropped, since each may have applied, and the router forgets it led.
func TestTheRouteCacheKeepsTheNewestTerm(t *testing.T) {
	m := oneShard(0)
	r, err := NewRouter(&m, 1)
	if err != nil {
		t.Fatal(err)
	}
	var out RouterOutput
	ctx := core.Context{Now: 1}
	tx := Txn{Ops: []Op{Put(attr(1, 2, 1), b("v"))}}
	answer := func(token uint64, err error) {
		out.Reset()
		r.Answer(&ctx, &Answer{Token: token, Result: Result{FailedCheck: -1}, Err: err}, &out)
	}
	sentTo := func(node uint32) {
		t.Helper()
		if len(out.Sends) != 1 || out.Sends[0].Node != node {
			t.Fatalf("sent %+v, want node %d", out.Sends, node)
		}
	}
	for token := range uint64(2) {
		out.Reset()
		if err := r.Txn(&ctx, &tx, token+1, &out); err != nil {
			t.Fatal(err)
		}
		sentTo(uint32(token))
	}
	if err := r.Txn(&ctx, &tx, 1, &out); !errors.Is(err, ErrTokenInFlight) {
		t.Fatalf("a token in flight was taken again: %v", err)
	}
	answer(1, NotLeader{Group: dataGroup, Leader: 3, Node: 2, Term: 7})
	sentTo(2)
	answer(2, NotLeader{Group: dataGroup, Leader: 2, Node: 1, Term: 6})
	sentTo(2)
	answer(2, NotLeader{Group: dataGroup, Term: 7})
	if len(out.Sends) != 0 || len(out.Actions) != 1 {
		t.Fatalf("the cached leader said it heard no leader in its term, and the router sent %+v and armed %+v", out.Sends, out.Actions)
	}
	answer(2, NotLeader{Group: dataGroup, Leader: 2, Node: 1, Term: 8})
	if len(out.Sends) != 0 {
		t.Fatalf("an answer to a held Txn was taken: %+v", out.Sends)
	}
	answer(1, NotLeader{Group: dataGroup, Leader: 2, Node: 1, Term: 8})
	sentTo(1)
	answer(1, nil)
	if len(out.Answers) != 1 || out.Answers[0].Token != 1 {
		t.Fatalf("answered %+v", out.Answers)
	}
	out.Reset()
	r.OnEvent(&ctx, &core.Event{Kind: core.EvPeerLost, Peer: core.NodeID{Node: 1}}, &out)
	if err := r.Txn(&ctx, &tx, 3, &out); err != nil {
		t.Fatal(err)
	}
	sentTo(2)
	out.Reset()
	r.OnEvent(&ctx, &core.Event{Kind: core.EvPeerLost, Peer: core.NodeID{Node: 2}}, &out)
	if len(out.Answers) != 1 || out.Answers[0].Token != 3 || !errors.Is(out.Answers[0].Err, raft.ErrProposalDropped) {
		t.Fatalf("a lost node's Txn answered %+v", out.Answers)
	}
	answer(9, nil)
	if len(out.Answers)+len(out.Sends) != 0 {
		t.Fatalf("an answer to no Txn in flight was taken: %+v", out)
	}
}

// The cached leader restarts while the router is idle and comes back a follower of its term that
// heard no leader. Whether or not its lost connection reached the router, a Txn reaches the
// leader or waits for the election; it is never resent at once in a loop (a review's repro).
func TestARestartedLeaderIsNotResentToInALoop(t *testing.T) {
	for _, lost := range []bool{false, true} {
		c := newGroup(t, 3, false)
		l := c.elect()
		m := oneShard((l + 1) % 3)
		r, err := NewRouter(&m, 1)
		if err != nil {
			t.Fatal(err)
		}
		var out RouterOutput
		ctx := c.ctx(l)
		tx := Txn{Ops: []Op{Put(attr(1, 2, 1), b("v"))}}
		if err := r.Txn(&ctx, &tx, 1, &out); err != nil {
			t.Fatal(err)
		}
		sendTo(c, r, &out)
		out.Reset()
		c.restart(l)
		if lost {
			r.OnEvent(&ctx, &core.Event{Kind: core.EvPeerLost, Peer: core.NodeID{Node: uint32(l)}}, &out)
		}
		tx2 := Txn{Ops: []Op{Put(attr(1, 2, 2), b("v"))}}
		if err := r.Txn(&ctx, &tx2, 2, &out); err != nil {
			t.Fatal(err)
		}
		sends, proposed := 0, false
		for len(out.Sends) > 0 && sends < 10 {
			s := out.Sends[0]
			out.Sends = out.Sends[:0]
			sends++
			n := int(s.Node)
			var err error
			c.call(n, func(ctx *core.Context, o *Output) { err = c.n[n].s.Txn(ctx, s.Txn, s.Token, o) })
			if proposed = err == nil; proposed {
				break
			}
			r.Answer(&ctx, &Answer{Token: s.Token, Err: err}, &out)
		}
		if sends > 2 || !proposed && (!r.flights[0].parked || len(out.Actions) != 1) {
			t.Fatalf("lost %v: %d sends, proposed %v, timer %+v", lost, sends, proposed, out.Actions)
		}
	}
}

// A new shard map fails the held Txns of a group it no longer holds, and the router keeps no
// slice of the caller's map.
func TestSetMapFailsHeldTxnsOfAGroupItDropped(t *testing.T) {
	m := twoShards()
	r, err := NewRouter(&m, 1)
	if err != nil {
		t.Fatal(err)
	}
	var out RouterOutput
	ctx := core.Context{Now: 1}
	tx := Txn{Ops: []Op{Put(attr(1, 9, 1), b("v"))}}
	if err := r.Txn(&ctx, &tx, 1, &out); err != nil {
		t.Fatal(err)
	}
	r.Answer(&ctx, &Answer{Token: 1, Err: NotLeader{Group: 7, Term: 1}}, &out)
	m.Groups[1].Voters[0].Node = 9
	m.Groups[2].Start[1] = 0xff
	if r.m.Groups[1].Voters[0].Node == 9 || r.m.Groups[2].Start[1] == 0xff {
		t.Fatal("the router's map shares the caller's slices")
	}
	out.Reset()
	one := oneShard(0)
	if err := r.SetMap(&one, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Answers) != 1 || !errors.Is(out.Answers[0].Err, ErrGroupGone) || len(r.flights) != 0 {
		t.Fatalf("the held Txn of the dropped group answered %+v", out.Answers)
	}
}

// An internal Txn repeated after an election, its first reply lost, changes nothing it did not
// read: the purger's guarded delete of a purge entry a truncate merged into meanwhile fails its
// check, as a control-plane table's version bump does; a blind delete would lose the merged range.
func TestInternalTxnsRepeatedAfterAnElectionFailTheirChecks(t *testing.T) {
	c := newGroup(t, 3, false)
	l := c.elect()
	purge := mustKey(&Key{Space: SpaceFS, Volume: 1, Inode: ino(2, 0), Tag: SlotPurge, ID: ino(2, 7)})
	table := mustKey(&Key{Space: SpaceCtl, Tag: CtlPlacement})
	c.txn(l, ops(Put(purge, b("0-4096")), Put(table, be(1))), 1)
	c.settle()
	guarded := ops(Check(purge, b("0-4096"), false), Delete(purge))
	bump := ops(Check(table, be(1), false), Put(table, be(2)))
	c.txn(l, guarded, 2)
	c.txn(l, bump, 3)
	c.settle()
	c.cut[l] = true
	nl := c.elect()
	c.cut[l] = false
	c.txn(nl, ops(Put(purge, b("0-4096,8192-"))), 4)
	c.txn(nl, guarded, 5)
	c.txn(nl, bump, 6)
	c.settle()
	for _, token := range []uint64{5, 6} {
		if a := c.answer(nl, token); a.Err != nil || a.Result.FailedCheck != 0 {
			t.Fatalf("the repeat under token %d answered %+v", token, a)
		}
	}
	if v, _ := get(c.n[nl].s.State(), string(purge)); v != "0-4096,8192-" {
		t.Fatalf("the merged purge entry is %q", v)
	}
	c.txn(nl, ops(Delete(purge)), 7)
	c.settle()
	if _, ok := get(c.n[nl].s.State(), string(purge)); ok {
		t.Fatal("a blind delete kept the purge entry")
	}
	c.heartbeat()
	c.agree()
}
