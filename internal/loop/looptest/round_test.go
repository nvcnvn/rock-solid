package looptest

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/loop"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
	"github.com/nvcnvn/rock-solid/internal/trace"
)

// stub is a stand-in shard: proposals of 128 bytes queue on it, a round takes every one queued
// in one blob entry, and it keeps one round in flight, as a Raft shard does.
type stub struct {
	l                       *loop.Loop
	id                      loop.ShardID
	pool                    core.BufPool
	queued, flight, durable int
	readies, asked          int
	again                   bool
	err                     error
	// at holds when each proposal queued or in flight was made; lat, each durable one's latency.
	at  []core.Tick
	lat *[]core.Tick
	// kind is the blob kind of its entries, BlobRaftEntries unless set; size a proposal's bytes,
	// proposal unless set; order, where set, collects the IDs of the shards collected.
	kind  disklog.BlobKind
	size  int
	order *[]loop.ShardID
	// refs are the places of its durable blobs, mail what its owner sent it.
	refs []disklog.BlobRef
	mail []loop.Mail
	// early, where set, records a blob reported durable with no place, out of order, or, where log
	// is set, past the log's durable LSN.
	early *error
	log   *disklog.Log
	last  uint64
	// told counts its Durable calls. hollow says it has records and gives none; keep keeps its
	// buffers, freeing each on Durable; ping, where set, arms timer 6 and sends the shard mail on
	// the next Durable; a Mail arms timer 100 plus the mail's count.
	told   int
	hollow bool
	keep   bool
	kept   []core.IOBuf
	ping   *loop.Log
}

const proposal = 128

func (s *stub) HasReady() bool {
	s.asked++
	return s.hollow || s.queued > 0 && s.flight == 0
}

func (s *stub) Ready(items []disklog.Item) []disklog.Item {
	if s.hollow {
		s.hollow = false
		return items
	}
	n := uint64(s.queued * proposal)
	if s.size != 0 {
		n = uint64(s.queued * s.size)
	}
	if s.order != nil {
		*s.order = append(*s.order, s.id)
	}
	buf, err := s.pool.Alloc(int(n))
	if err != nil {
		panic(err)
	}
	s.flight, s.queued = s.queued, 0
	s.readies++
	kind := disklog.BlobRaftEntries
	if s.kind != 0 {
		kind = s.kind
	}
	if s.keep {
		s.kept = append(s.kept, buf)
	}
	return append(items, disklog.Item{Entry: disklog.BlobEntry(kind, uint32(s.id), n), Buf: buf, Keep: s.keep})
}

func (s *stub) Durable(ctx *core.Context, err error, refs []disklog.BlobRef, out []core.Action) []core.Action {
	if err != nil && s.err == nil {
		s.err = err
	}
	s.told++
	if s.kind == 0 {
		s.refs = append(s.refs, refs...)
	}
	if s.keep {
		s.pool.Free(s.kept[0])
		s.kept = s.kept[:copy(s.kept, s.kept[1:])]
	}
	if s.early != nil && *s.early == nil && err == nil && len(refs) != 1 {
		*s.early = fmt.Errorf("shard %d told %d places for its one record", s.id, len(refs))
	}
	for _, ref := range refs {
		switch {
		case s.early == nil || *s.early != nil:
		case ref.LSN <= s.last:
			*s.early = fmt.Errorf("shard %d told lsn %d durable after lsn %d", s.id, ref.LSN, s.last)
		case s.log != nil && ref.LSN > s.log.DurableLSN():
			*s.early = fmt.Errorf("shard %d told lsn %d durable, the log's durable lsn %d", s.id, ref.LSN, s.log.DurableLSN())
		}
		s.last = max(s.last, ref.LSN)
	}
	s.durable += s.flight
	if s.lat != nil {
		for _, at := range s.at[:s.flight] {
			*s.lat = append(*s.lat, ctx.Now-at)
		}
	}
	s.at = s.at[:copy(s.at, s.at[s.flight:])]
	s.flight = 0
	if s.again {
		s.queued++
		s.at = append(s.at, ctx.Now)
	}
	if s.queued > 0 {
		s.l.Mark(s.id)
	}
	if lg := s.ping; lg != nil {
		s.ping = nil
		out = append(out, core.Action{Kind: core.ActSetTimer, Timer: 6, At: ctx.Now + 10})
		lg.Send(0, loop.Mail{Shard: s.id, Kind: loop.MailRewrite})
	}
	return out
}

func (s *stub) Mail(ctx *core.Context, m *loop.Mail, out []core.Action) []core.Action {
	s.mail = append(s.mail, *m)
	return append(out, core.Action{Kind: core.ActSetTimer, Timer: core.TimerID(100 + len(s.mail)), At: ctx.Now + 10})
}

// propose queues a proposal made at now; a shard with a round in flight is marked again when it
// is durable.
func (s *stub) propose(now core.Tick) {
	s.queued++
	s.at = append(s.at, now)
	if s.flight == 0 {
		s.l.Mark(s.id)
	}
}

// proposer proposes to a random shard of its loop every gap until it has proposed left times.
type proposer struct {
	shards []*stub
	rand   *core.Rand
	gap    core.Tick
	left   int
}

func (p *proposer) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	if ev.Kind != core.EvTimer || p.left == 0 {
		return out
	}
	p.left--
	p.shards[p.rand.Below(uint64(len(p.shards)))].propose(ctx.Now)
	if p.left > 0 {
		out = append(out, core.Action{Kind: core.ActSetTimer, Timer: 1, At: ctx.Now + p.gap})
	}
	return out
}

// diskDriver runs a loop's disk actions on its ring; loops in these tests send nothing.
type diskDriver struct{ r *Disk }

func (d diskDriver) Read(loop.ConnID, []byte) {}
func (d diskDriver) Closed(loop.ConnID)       {}
func (d diskDriver) Accept(loop.ConnID)       {}
func (d diskDriver) Flush() bool              { return false }
func (d diskDriver) Execute(a *core.Action) {
	if d.r != nil {
		d.r.Execute(a)
	}
}

func roundLogConfig(maxRecord uint64) disklog.Config {
	cfg := disklog.DefaultConfig()
	cfg.MaxReplayBytes, cfg.NoReserve, cfg.NoTrim = 1<<60, true, true
	if maxRecord > 0 {
		cfg.MaxRecordBytes = maxRecord
	}
	return cfg
}

// logIOs is where a test's first log's IO IDs end and its second's begin.
const logIOs core.IOID = 1 << 32

// formatLog formats a log on d with IO IDs from firstIO, running its actions at once.
func formatLog(t testing.TB, d *sim.Disk, pool core.BufPool, cfg disklog.Config, firstIO core.IOID) *disklog.Log {
	t.Helper()
	geo, err := disklog.GeometryFor(1 << 30)
	if err != nil {
		t.Fatal(err)
	}
	ctx := core.Context{Now: 1, Raw: 1, Pool: pool}
	var out disklog.Output
	log, err := disklog.Format(&ctx, cfg, disklog.Superblock{LogID: 1, Geometry: geo}, firstIO, &out)
	if err != nil {
		t.Fatal(err)
	}
	for len(out.Actions) > 0 {
		acts := append([]core.Action(nil), out.Actions...)
		out.Reset()
		for i := range acts {
			ev := core.Event{Kind: core.EvDiskDone, IO: acts[i].IO}
			if d != nil {
				ev = d.Execute(pool, &acts[i])
			}
			log.OnEvent(&ctx, &ev, &out)
		}
	}
	return log
}

type roundRig struct {
	s      *sim.Scheduler
	loops  []*loop.Loop
	rings  []*trace.Ring
	ks     []*Kernel
	shards [][]*stub
	ms     []*proposer
	ring   *Disk
	pools  []core.BufPool
	log    *disklog.Log
	lg     *loop.Log
	lat    []core.Tick
	inv    sim.Invariants
	early  error
	disk   *sim.Disk
	// open is the disk ring until close, the cell a cleanup reads, so the test holds no more of
	// the rig once a seed closes it.
	open **Disk
}

func (r *roundRig) close() {
	(*r.open).Close()
	*r.open = nil
}

// flush is the stepped disk's flush latency in µs, the raft rounds spike's 0.95 ms.
const flush = 950

// roundRig steps n producer loops of perLoop shards each, proposing every gap µs until each has
// proposed proposals times, and an owner loop, ID n, of the disk; each loop has its own pool, and
// with n 0 the shards sit on the owner.
func newRoundRig(t *testing.T, seed uint64, n, perLoop, proposals int, gap core.Tick, maxRecord uint64) *roundRig {
	t.Helper()
	s := sim.New(seed, sim.Settings{PoolBytes: 1 << 20})
	node := s.AddNode(sim.DefaultClock, nil, func(*sim.Node) {})
	lat := sim.DefaultDiskLatency
	lat.Flush = flush
	d := sim.NewDiskWith(1<<30/core.BlockSize, sim.DiskSettings{Latency: lat})
	r := &roundRig{s: s, disk: d}
	total := max(n, 1)
	var log *disklog.Log
	var pools []core.BufPool
	for i := range total + 1 {
		if n == 0 && i == 1 {
			break
		}
		size := 1 << 20
		if i == n {
			size = 8 << 20
		}
		pool := sim.NewPool(size, 0)
		pools = append(pools, pool)
		ring, err := trace.NewRing(make([]byte, 1<<14*trace.RecordSize))
		if err != nil {
			t.Fatal(err)
		}
		k := NewKernel(s, node.ID())
		m := &proposer{rand: node.Fork(), gap: gap, left: proposals}
		var drv diskDriver
		id := uint16(i)
		if n == 0 {
			id = 0
		}
		l := loop.New(loop.Config{Node: node.ID(), ID: id, Pool: pool, Trace: ring}, k, m, &drv)
		r.loops, r.rings, r.ks = append(r.loops, l), append(r.rings, ring), append(r.ks, k)
		if i == n {
			log = formatLog(t, d, pool, roundLogConfig(maxRecord), 1)
			dr, err := NewDisk(k, d, pool)
			if err != nil {
				t.Fatal(err)
			}
			open := &dr
			t.Cleanup(func() {
				if *open != nil {
					(*open).Close()
				}
			})
			r.open = open
			if err := l.AttachDisk(dr); err != nil {
				t.Fatal(err)
			}
			drv.r, r.ring = dr, dr
			AddWithDisk(node, k, l, dr)
		} else {
			Add(node, k, l)
		}
		r.shards, r.ms = append(r.shards, nil), append(r.ms, m)
	}
	owner := r.loops[len(r.loops)-1]
	lg := owner.AttachLog(log, 1, logIOs, total+1)
	for i, l := range r.loops {
		if n > 0 && i == n {
			break
		}
		var stubs []*stub
		for range perLoop {
			st := &stub{l: l, pool: pools[i], lat: &r.lat, early: &r.early, log: log}
			st.id = l.AddShard(st, lg)
			stubs = append(stubs, st)
		}
		r.shards[i], r.ms[i].shards = stubs, stubs
		l.Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: 1})
	}
	r.pools, r.log, r.lg = pools, log, lg
	r.inv.Register("driver contract", s.DriverContract)
	r.inv.Register("durable only once the log is", func() error { return r.early })
	return r
}

func (r *roundRig) run(t *testing.T) {
	t.Helper()
	for r.s.Step() {
		if err := r.inv.Check(); err != nil {
			t.Fatal(err)
		}
	}
	for i, k := range r.ks {
		if err := k.Err(); err != nil {
			t.Fatal(err)
		}
		if r.rings[i].Added() > uint64(r.rings[i].Len()) {
			t.Fatalf("loop %d overflowed its trace ring", i)
		}
	}
}

func (r *roundRig) records(i int) []trace.Record {
	recs := make([]trace.Record, r.rings[i].Len())
	for k := range recs {
		recs[k] = r.rings[i].At(k)
	}
	return recs
}

// TestRoundsHandOffOncePerRoundAndFlushOncePerRound also checks that rounds pipeline: more
// producer loops add at most one round to the p99 commit latency of one.
func TestRoundsHandOffOncePerRoundAndFlushOncePerRound(t *testing.T) {
	var one core.Tick
	for _, n := range []int{1, 2, 4, 8} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			r := newRoundRig(t, uint64(n), n, 16, 1000, 20, 0)
			r.run(t)
			_, p99, best := checkRounds(t, r, n)
			if best != n {
				t.Fatalf("at most %d of %d producer loops in one round", best, n)
			}
			if n == 1 {
				one = p99
			}
			if n == 8 && takenWakes(r) == 0 {
				t.Fatal("no loop was woken when the owner took a batch it sent too late for its round")
			}
			if round := core.Tick(sim.DefaultDiskLatency.Write + flush); one > 0 && p99 > one+round {
				t.Fatalf("p99 %d µs with %d producer loops, %d with one", p99, n, one)
			}
		})
	}
}

func takenWakes(r *roundRig) int {
	n := 0
	for _, rec := range r.records(len(r.loops) - 1) {
		if rec.Name == trace.OutNotice && rec.Fields[2] == 0 {
			n++
		}
	}
	return n
}

// checkRounds checks the trace of a finished run: every proposal durable; one DiskFsync per
// DiskWrite on the owner; between two of its round writes, at most one batch taken from each
// producer loop and one wake to it; one durable notice per round to each loop with a batch in it;
// and batches counted in order. It returns the latencies' p50 and p99, and the most producer loops
// one round held.
func checkRounds(t *testing.T, r *roundRig, n int) (p50, p99 core.Tick, best int) {
	t.Helper()
	loops := max(n, 1)
	sent := make([]uint64, loops)
	places := map[disklog.BlobRef]bool{}
	for i := range loops {
		for _, st := range r.shards[i] {
			if st.err != nil || st.queued != 0 || st.flight != 0 {
				t.Fatalf("loop %d shard %d: err %v, %d queued, %d in flight", i, st.id, st.err, st.queued, st.flight)
			}
			for _, ref := range st.refs {
				if places[ref] {
					t.Fatalf("loop %d shard %d told %+v, another blob's place", i, st.id, ref)
				}
				places[ref] = true
			}
		}
		for _, rec := range r.records(i) {
			if rec.Name == trace.OutHandoff {
				if sent[i]++; rec.Fields[3] != sent[i] {
					t.Fatalf("loop %d sent batch %d after %d", i, rec.Fields[3], sent[i]-1)
				}
			}
		}
	}
	rounds, fsyncs := 0, 0
	taken, told := make([]int, loops), make([]int, loops)
	durable, last := make([]uint64, loops), make([]uint64, loops)
	wakes := 0
	lsns := map[uint64]int{}
	for _, rec := range r.records(len(r.loops) - 1) {
		switch rec.Name {
		case trace.ActDiskWrite:
			rounds++
		case trace.ActDiskFsync:
			fsyncs++
		case trace.EvTake:
			if i := rec.Fields[0]; taken[i] == rounds+1 {
				t.Fatalf("owner took two batches of loop %d for round %d", i, rounds+1)
			} else {
				taken[i] = rounds + 1
			}
		case trace.OutNotice:
			i, lsn := rec.Fields[0], rec.Fields[2]
			switch {
			case lsn == 0 && told[i] == rounds+1:
				t.Fatalf("owner woke loop %d twice in round %d", i, rounds)
			case lsn == 0:
				told[i] = rounds + 1
				wakes++
			case lsn <= last[i] || rec.Fields[3] <= durable[i]:
				t.Fatalf("owner told loop %d lsn %d, %d batches durable after lsn %d, %d batches", i, lsn, rec.Fields[3], last[i], durable[i])
			default:
				last[i], durable[i] = lsn, rec.Fields[3]
				lsns[lsn]++
			}
		}
	}
	if rounds == 0 || fsyncs != rounds {
		t.Fatalf("owner wrote %d rounds with %d flushes", rounds, fsyncs)
	}
	for i := range loops {
		if sent[i] == 0 || durable[i] != sent[i] {
			t.Fatalf("loop %d: %d batches sent, %d durable", i, sent[i], durable[i])
		}
	}
	for _, c := range lsns {
		best = max(best, c)
	}
	slices.Sort(r.lat)
	p50, p99 = r.lat[len(r.lat)/2], r.lat[len(r.lat)*99/100]
	t.Logf("%d producers: %d rounds, %d wakes, latency p50 %d µs, p99 %d µs, max %d µs", n, rounds, wakes, p50, p99, r.lat[len(r.lat)-1])
	return p50, p99, best
}

func TestRoundAsksOnlyMarkedShards(t *testing.T) {
	r := newRoundRig(t, 1, 1, 1001, 0, 20, 0)
	active := r.shards[0][500]
	active.propose(0)
	r.run(t)
	asked := 0
	for _, st := range r.shards[0] {
		asked += st.asked
	}
	if asked != 1 || active.durable != 1 {
		t.Fatalf("HasReady called %d times for one active shard of 1,001; %d durable", asked, active.durable)
	}
}

// instant is one loop with machine m owning a log on a disk ring whose actions complete at once
// and store nothing.
type instant struct {
	s    *sim.Scheduler
	l    *loop.Loop
	pool *sim.Pool
	log  *disklog.Log
	lg   *loop.Log
}

func newInstant(tb testing.TB, m loop.Machine) *instant {
	s := sim.New(1, sim.Settings{PoolBytes: 1 << 20})
	node := s.AddNode(sim.DefaultClock, nil, func(*sim.Node) {})
	pool := sim.NewPool(16<<20, 0)
	log := formatLog(tb, nil, pool, roundLogConfig(0), 1)
	k := NewKernel(s, node.ID())
	var drv diskDriver
	l := loop.New(loop.Config{Node: node.ID(), Pool: pool}, k, m, &drv)
	dr, err := NewDisk(k, nil, pool)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(dr.Close)
	if err := l.AttachDisk(dr); err != nil {
		tb.Fatal(err)
	}
	drv.r = dr
	AddWithDisk(node, k, l, dr)
	return &instant{s: s, l: l, pool: pool, log: log, lg: l.AttachLog(log, 1, logIOs, 1)}
}

func (in *instant) run() {
	for in.s.Step() {
	}
}

// newInstantRig is an instant loop with shards stubs on it; round runs one proposal on the first
// to durable.
func newInstantRig(tb testing.TB, shards int) (round func()) {
	in := newInstant(tb, &proposer{})
	l, lg, pool := in.l, in.lg, in.pool
	var st []*stub
	for range shards {
		// The log keeps a Raft blob's pieces until its shard drops them, which a stub never does;
		// a kind it keeps nothing for leaves the round's own allocations to measure.
		s := &stub{l: l, pool: pool, kind: 99}
		s.id = l.AddShard(s, lg)
		st = append(st, s)
	}
	return func() {
		st[0].propose(0)
		for d, i := st[0].durable, 0; st[0].durable == d; i++ {
			if i == 10 {
				panic("looptest: a round took more than 10 passes")
			}
			l.Pass()
		}
	}
}

func TestRoundAllocatesNothing(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector's runtime allocates in a round")
	}
	round := newInstantRig(t, 1001)
	if n := testing.AllocsPerRun(1000, round); n != 0 {
		t.Fatalf("%v allocations per round", n)
	}
}

// BenchmarkRoundIdleShards runs rounds of one active shard among 9, 99 and 999 idle ones; the time
// per round does not grow with them.
func BenchmarkRoundIdleShards(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			round := newInstantRig(b, n)
			b.ReportAllocs()
			for b.Loop() {
				round()
			}
		})
	}
}

// TestRoundCapLeavesTheRestInOrder marks 48 shards of 2 KiB each, more than one 64 KiB round holds
// and less than two: the first round takes them in the order they were marked up to the cap, and
// the second the rest.
func TestRoundCapLeavesTheRestInOrder(t *testing.T) {
	r := newRoundRig(t, 1, 1, 48, 0, 20, 64<<10)
	var order []loop.ShardID
	for _, st := range r.shards[0] {
		st.size, st.order = 2<<10, &order
		st.propose(0)
	}
	r.run(t)
	for i, id := range order {
		if id != loop.ShardID(i) {
			t.Fatalf("shards collected in order %v", order)
		}
	}
	rounds := 0
	for _, rec := range r.records(1) {
		if rec.Name == trace.ActDiskWrite {
			rounds++
		}
	}
	if len(order) != 48 || rounds != 2 {
		t.Fatalf("%d of 48 shards collected, in %d rounds", len(order), rounds)
	}
	for _, rec := range r.records(0) {
		if rec.Name == trace.OutHandoff && rec.Fields[2] > 33 {
			t.Fatalf("batch %d held %d records of 2 KiB against a 64 KiB cap", rec.Fields[3], rec.Fields[2])
		}
	}
}

// TestRoundOnOwnerSkipsTheInbox runs shards on the disk's owner: neither their batches nor
// their notices pass through its inbox, which only the proposer's first timer wakes.
func TestRoundOnOwnerSkipsTheInbox(t *testing.T) {
	r := newRoundRig(t, 1, 0, 16, 1000, 20, 0)
	r.run(t)
	if _, _, best := checkRounds(t, r, 0); best != 1 {
		t.Fatalf("the owner's shards spread over rounds, at most %d in one", best)
	}
	if w := r.loops[0].Inbox().Wakes(); w != 1 {
		t.Fatalf("the owner's inbox woke it %d times", w)
	}
}

// TestRoundFullOwnerPoolDelaysBatches leaves the owner's pool room for two producer batches'
// copies: the batches that find it full wait for rounds to free it, which shows in the latency
// and in the owner's trace, none fails, and every proposal becomes durable.
func TestRoundFullOwnerPoolDelaysBatches(t *testing.T) {
	const n = 8
	free := newRoundRig(t, 1, n, 16, 1000, 20, 0)
	free.run(t)
	p50, _, _ := checkRounds(t, free, n)
	r := newRoundRig(t, 1, n, 16, 1000, 20, 0)
	pool := r.pools[n].(*sim.Pool)
	if _, err := pool.Alloc(pool.Available() - 32*core.BlockSize); err != nil {
		t.Fatal(err)
	}
	r.run(t)
	if full, _, _ := checkRounds(t, r, n); full < 3*p50/2 {
		t.Fatalf("p50 %d µs with the owner's pool nearly full, %d µs with room", full, p50)
	}
	if waits, none := poolWaits(r), poolWaits(free); waits == 0 || none != 0 {
		t.Fatalf("%d batches traced waiting for the nearly full pool, %d for the free one", waits, none)
	}
}

func poolWaits(r *roundRig) int {
	n := 0
	for _, rec := range r.records(len(r.loops) - 1) {
		if rec.Name == trace.EvPoolWait {
			n++
		}
	}
	return n
}

// TestRoundMailAndDeadBlobsCrossLoops sends a shard on a producer loop, and one on the owner, a
// replayed blob and a rewrite, and has each report half its durable blobs dead: the mail reaches
// the shard with its bytes, and the owner's log drops those blobs and keeps the rest.
func TestRoundMailAndDeadBlobsCrossLoops(t *testing.T) {
	for _, n := range []int{0, 1} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			r := newRoundRig(t, 1, n, 1, 20, 2000, 0)
			r.run(t)
			st := r.shards[0][0]
			if len(st.refs) < 4 || st.refs[0].LSN == 0 {
				t.Fatalf("durable refs %v", st.refs)
			}
			data := []byte("replayed")
			r.lg.Send(0, loop.Mail{Shard: st.id, Kind: loop.MailReplayed, Ref: st.refs[0], Blob: disklog.BlobRaftEntries, Data: data})
			r.lg.Send(0, loop.Mail{Shard: st.id, Kind: loop.MailRewrite, Ref: disklog.BlobRef{Segment: 3}})
			data[0] = 'X'
			half := st.refs[:len(st.refs)/2]
			r.loops[0].Dead(st.id, half)
			r.ms[len(r.ms)-1].left = 0
			r.loops[len(r.loops)-1].Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: 1})
			r.run(t)
			if len(st.mail) != 2 || string(st.mail[0].Data) != "replayed" || st.mail[0].Ref != st.refs[0] || st.mail[1].Kind != loop.MailRewrite || st.mail[1].Ref.Segment != 3 {
				t.Fatalf("mail %+v", st.mail)
			}
			if err := r.log.RaftBlobsDead(half[:1]); err == nil {
				t.Fatal("the log still holds a blob reported dead")
			}
			if err := r.log.RaftBlobsDead(st.refs[len(half):]); err != nil {
				t.Fatalf("the log dropped blobs not reported dead: %v", err)
			}
		})
	}
}

// TestRoundsKeepTheirRules runs a seed's shape: up to 8 producer loops or the shards on the owner,
// up to 32 shards a loop, a proposal gap, a round cap, and at times an owner pool with room for
// a few batches' copies. Invariants hold at every step; at the end every proposal is durable, the
// trace keeps the round rules, and every buffer is back in its pool.
func TestRoundsKeepTheirRules(t *testing.T) {
	seeds.Run(t, 1, 20, func(seed uint64) {
		rnd := rand.New(rand.NewPCG(seed, 0))
		n, perLoop := rnd.IntN(9), 1+rnd.IntN(32)
		proposals, gap := 50+rnd.IntN(250), core.Tick(5+rnd.IntN(200))
		maxRecord := []uint64{0, 64 << 10, 256 << 10}[rnd.IntN(3)]
		r := newRoundRig(t, seed, n, perLoop, proposals, gap, maxRecord)
		defer r.close()
		before := make([]int, len(r.pools))
		for i, p := range r.pools {
			before[i] = p.(*sim.Pool).Available()
		}
		filler := core.NoBuf
		if n > 0 && rnd.IntN(3) == 0 {
			pool := r.pools[n]
			var err error
			if filler, err = pool.Alloc(before[n] - 4*perLoop*core.BlockSize); err != nil {
				t.Fatal(err)
			}
		}
		r.run(t)
		if filler != core.NoBuf {
			r.pools[n].Free(filler)
		}
		checkRounds(t, r, n)
		made := 0
		for _, stubs := range r.shards {
			for _, st := range stubs {
				made += st.durable
			}
		}
		if want := proposals * max(n, 1); made != want {
			t.Fatalf("seed %d: %d of %d proposals durable", seed, made, want)
		}
		for i, p := range r.pools {
			if got := p.(*sim.Pool).Available(); got != before[i] {
				t.Fatalf("seed %d: loop %d's pool has %d bytes free, %d before the run", seed, i, got, before[i])
			}
		}
	})
}

// sender, on timer 1, arms timer 5 and sends its shard mail from inside its OnEvent, as recovery
// on the owner would; it counts the timers it gets.
type sender struct {
	lg    *loop.Log
	shard loop.ShardID
	fired map[core.TimerID]int
}

func (m *sender) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	if ev.Kind != core.EvTimer {
		return out
	}
	m.fired[ev.Timer]++
	if ev.Timer == 1 {
		out = append(out, core.Action{Kind: core.ActSetTimer, Timer: 5, At: ctx.Now + 10})
		m.lg.Send(0, loop.Mail{Shard: m.shard, Kind: loop.MailRewrite})
	}
	return out
}

// TestRoundSendOnOwnerKeepsTheCallersActions sends mail to a shard on the owner from inside the
// owner machine's OnEvent and from inside the shard's Durable: the caller's actions and each
// mail's run once each, and the shard still hears of its next record.
func TestRoundSendOnOwnerKeepsTheCallersActions(t *testing.T) {
	m := &sender{fired: map[core.TimerID]int{}}
	in := newInstant(t, m)
	st := &stub{l: in.l, pool: in.pool, ping: in.lg}
	st.id = in.l.AddShard(st, in.lg)
	m.lg, m.shard = in.lg, st.id
	st.propose(0)
	in.l.Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: 1})
	in.run()
	if len(st.mail) != 2 || st.durable != 1 {
		t.Fatalf("%d mails, %d durable", len(st.mail), st.durable)
	}
	for _, id := range []core.TimerID{5, 6, 101, 102} {
		if m.fired[id] != 1 {
			t.Fatalf("timers fired %v: want 5, 6, 101 and 102 once each", m.fired)
		}
	}
	st.propose(0)
	in.l.Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: 2})
	in.run()
	if st.durable != 2 {
		t.Fatalf("%d durable after a second proposal: mail sent while its loop was told stopped later notices", st.durable)
	}
}

// diskUser is an owner machine that reads its own blocks and submits to the log besides the
// round driver: on timer 1 it reads IO far past the log's, and submits a record.
type diskUser struct {
	log     *disklog.Log
	pool    core.BufPool
	out     disklog.Output
	ticket  disklog.Ticket
	read    int
	durable int
}

const userIO core.IOID = 1 << 40

func (m *diskUser) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	switch {
	case ev.Kind == core.EvDiskDone && ev.IO == userIO:
		m.read++
	case ev.Kind == core.EvTimer && ev.Timer == 1:
		out = append(out, core.Action{Kind: core.ActDiskRead, IO: userIO})
		buf, err := m.pool.Alloc(proposal)
		if err != nil {
			panic(err)
		}
		if m.ticket, err = m.log.Submit(ctx, disklog.Hot, disklog.Item{Entry: disklog.BlobEntry(99, 0, proposal), Buf: buf}, &m.out); err != nil {
			panic(err)
		}
		out = append(out, m.out.Actions...)
		m.out.Reset()
	}
	return out
}

type logUser struct{ *diskUser }

func (m logUser) OnLog(ctx *core.Context, lg *loop.Log, ev *disklog.Event, out []core.Action) []core.Action {
	if ev.Kind == disklog.Durable && slices.Contains(ev.Tickets, m.ticket) {
		m.durable++
	}
	return out
}

// TestRoundOwnerMachineKeepsItsDiskAndLogEvents runs a machine on a log's owner that reads its own
// blocks and submits to the log itself, beside a shard: it gets its read's completion and, as a
// LogMachine, its record's Durable; a machine that is not one stops the loop rather than lose it.
func TestRoundOwnerMachineKeepsItsDiskAndLogEvents(t *testing.T) {
	for _, lm := range []bool{true, false} {
		t.Run(fmt.Sprint(lm), func(t *testing.T) {
			m := &diskUser{}
			var mach loop.Machine = m
			if lm {
				mach = logUser{m}
			}
			in := newInstant(t, mach)
			m.log, m.pool = in.log, in.pool
			st := &stub{l: in.l, pool: in.pool, kind: 99}
			st.id = in.l.AddShard(st, in.lg)
			st.propose(0)
			in.l.Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: 1})
			defer func() {
				p := recover()
				if !lm && (p == nil || !strings.Contains(fmt.Sprint(p), "not a LogMachine")) {
					t.Fatalf("a log event with no LogMachine to take it: %v", p)
				}
			}()
			in.run()
			if m.read != 1 || m.durable != 1 || st.durable != 1 {
				t.Fatalf("the machine got %d reads and %d durables, the shard %d durable", m.read, m.durable, st.durable)
			}
		})
	}
}

// TestRoundSeveralLogsOnOneLoop gives one loop two logs, with a shard on each from the loop and
// from a second loop: every proposal is durable on its own log. Logs whose IO IDs overlap are
// refused.
func TestRoundSeveralLogsOnOneLoop(t *testing.T) {
	s := sim.New(1, sim.Settings{PoolBytes: 1 << 20})
	node := s.AddNode(sim.DefaultClock, nil, func(*sim.Node) {})
	var loops []*loop.Loop
	var pools []*sim.Pool
	var logs []*disklog.Log
	var lgs []*loop.Log
	for i := range 2 {
		pool := sim.NewPool(16<<20, 0)
		k := NewKernel(s, node.ID())
		var drv diskDriver
		l := loop.New(loop.Config{Node: node.ID(), ID: uint16(i), Pool: pool}, k, &proposer{}, &drv)
		if i == 1 {
			dr, err := NewDisk(k, nil, pool)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(dr.Close)
			if err := l.AttachDisk(dr); err != nil {
				t.Fatal(err)
			}
			drv.r = dr
			AddWithDisk(node, k, l, dr)
			for j := range 2 {
				log := formatLog(t, nil, pool, roundLogConfig(0), 1+core.IOID(j)*logIOs)
				logs, lgs = append(logs, log), append(lgs, l.AttachLog(log, 1+core.IOID(j)*logIOs, core.IOID(j+1)*logIOs, 2))
			}
		} else {
			Add(node, k, l)
		}
		loops, pools = append(loops, l), append(pools, pool)
	}
	var stubs []*stub
	var early error
	for i, l := range loops {
		for j, lg := range lgs {
			st := &stub{l: l, pool: pools[i], early: &early, log: logs[j]}
			st.id = l.AddShard(st, lg)
			stubs = append(stubs, st)
		}
	}
	for range 5 {
		for _, st := range stubs {
			st.propose(0)
		}
		for s.Step() {
		}
	}
	for _, st := range stubs {
		if st.durable != 5 || st.err != nil {
			t.Fatalf("shard %d: %d of 5 durable, err %v", st.id, st.durable, st.err)
		}
	}
	if early != nil || logs[0].DurableLSN() == 0 || logs[1].DurableLSN() == 0 {
		t.Fatalf("%v; durable lsns %d and %d", early, logs[0].DurableLSN(), logs[1].DurableLSN())
	}
	defer func() {
		if p := recover(); p == nil || !strings.Contains(fmt.Sprint(p), "overlap") {
			t.Fatalf("a log whose IO IDs overlap another's: %v", p)
		}
	}()
	loops[1].AttachLog(logs[0], logIOs+5, logIOs+6, 2)
}

// TestRoundShardWithNothingReadyIsToldNothing marks a shard that says it has records and gives
// none, alone in a round: a later round's Durable goes only to the shard that gave records.
func TestRoundShardWithNothingReadyIsToldNothing(t *testing.T) {
	in := newInstant(t, &proposer{})
	hollow := &stub{l: in.l, pool: in.pool, hollow: true}
	hollow.id = in.l.AddShard(hollow, in.lg)
	st := &stub{l: in.l, pool: in.pool}
	st.id = in.l.AddShard(st, in.lg)
	in.l.Mark(hollow.id)
	in.run()
	st.propose(0)
	in.run()
	if hollow.told != 0 || st.told != 1 || st.durable != 1 {
		t.Fatalf("Durable told the hollow shard %d times, the other %d times with %d durable", hollow.told, st.told, st.durable)
	}
}

// TestRoundKeptBuffers: a shard on the owner keeps the buffer of a record the log refuses and
// frees it once told, the round driver leaving it alone; a shard on another loop may not keep one,
// since the owner writes a copy no one would free.
func TestRoundKeptBuffers(t *testing.T) {
	t.Run("refused on the owner", func(t *testing.T) {
		in := newInstant(t, &proposer{})
		before := in.pool.Available()
		st := &stub{l: in.l, pool: in.pool, keep: true, kind: disklog.BlobRun}
		st.id = in.l.AddShard(st, in.lg)
		st.propose(0)
		in.run()
		if !errors.Is(st.err, disklog.ErrReservedKind) || st.durable != 1 || in.pool.Available() != before {
			t.Fatalf("err %v, %d told, pool %d bytes free of %d", st.err, st.durable, in.pool.Available(), before)
		}
	})
	t.Run("on another loop", func(t *testing.T) {
		r := newRoundRig(t, 1, 1, 1, 1, 20, 0)
		r.shards[0][0].keep = true
		defer func() {
			if p := recover(); p == nil || !strings.Contains(fmt.Sprint(p), "kept the buffer") {
				t.Fatalf("a kept buffer handed to another loop's disk: %v", p)
			}
		}()
		r.run(t)
	})
}

// TestRoundFailedDiskResolvesEveryBatch fails a round's write or its flush with 2 producer loops:
// every batch, in flight or sent later, is told an error, and every
// buffer is back in its pool.
func TestRoundFailedDiskResolvesEveryBatch(t *testing.T) {
	for _, kind := range []core.ActionKind{core.ActDiskWrite, core.ActDiskFsync} {
		t.Run(fmt.Sprint(kind), func(t *testing.T) {
			r := newRoundRig(t, 1, 2, 8, 200, 20, 0)
			n := 0
			r.disk.Inject(sim.Fault{Effect: sim.FailIO, When: func(op sim.DiskOp, _ *core.Rand) bool {
				if op.Kind == kind {
					n++
				}
				return op.Kind == kind && n == 6
			}})
			before := make([]int, len(r.pools))
			for i, p := range r.pools {
				before[i] = p.(*sim.Pool).Available()
			}
			r.run(t)
			failed, made := 0, 0
			for i, stubs := range r.shards {
				for _, st := range stubs {
					if st.queued != 0 || st.flight != 0 {
						t.Fatalf("loop %d shard %d: %d queued, %d in flight", i, st.id, st.queued, st.flight)
					}
					if st.err != nil {
						failed++
					}
					made += st.durable
				}
			}
			if failed == 0 || made != 2*200 {
				t.Fatalf("%d shards told an error, %d of 400 proposals resolved", failed, made)
			}
			for i, p := range r.pools {
				if got := p.(*sim.Pool).Available(); got != before[i] {
					t.Fatalf("loop %d's pool has %d bytes free, %d before the run", i, got, before[i])
				}
			}
		})
	}
}

// TestRoundWaitingBatchesShareTheOwnerPool fills the owner's pool, but for 20 blocks, under 16
// shards on the owner and 16 on a producer loop: a batch that waits for room is retried every
// pass, and every proposal on both loops becomes durable.
func TestRoundWaitingBatchesShareTheOwnerPool(t *testing.T) {
	r := newRoundRig(t, 1, 1, 16, 200, 20, 0)
	owner := r.loops[1]
	var own []*stub
	for range 16 {
		st := &stub{l: owner, pool: r.pools[1], lat: &r.lat, early: &r.early, log: r.log}
		st.id = owner.AddShard(st, r.lg)
		own = append(own, st)
	}
	r.ms[1].shards, r.ms[1].left = own, 200
	owner.Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: 1})
	pool := r.pools[1].(*sim.Pool)
	if _, err := pool.Alloc(pool.Available() - 20*core.BlockSize); err != nil {
		t.Fatal(err)
	}
	r.run(t)
	if poolWaits(r) == 0 {
		t.Fatal("no batch waited for room in the owner's pool")
	}
	for _, st := range append(own, r.shards[0]...) {
		if st.err != nil || st.queued != 0 || st.flight != 0 {
			t.Fatalf("shard %d: err %v, %d queued, %d in flight", st.id, st.err, st.queued, st.flight)
		}
	}
}
