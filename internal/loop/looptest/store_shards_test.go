package looptest

import (
	"reflect"
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/chunkstore/storetest"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/loop"
	"github.com/nvcnvn/rock-solid/internal/raftshard"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// storeShards runs a store on a device loop, ID 1, that owns its log, with a Raft shard on the
// round driver of that loop and one on a producer loop, ID 0, both known to the store; each new
// process starts its loops, and its shards on the bases their runs would restore, when the store
// opens.
type storeShards struct {
	t       *testing.T
	sd      *storetest.SimDisk
	pool    core.BufPool
	s       *sim.Scheduler
	ring    *Disk
	loops   [2]*loop.Loop
	lg      *loop.Log
	r       *storetest.Rig
	maxBlob uint64
	rounds  int
	shards  [2]*raftRounds
}

func (sh *storeShards) opened(st *chunkstore.Store) {
	if sh.pool != sh.sd.Pool() {
		sh.start()
	}
	for i, l := range sh.loops {
		var base raftpb.SnapshotMetadata
		if sh.shards[i] != nil {
			base = sh.shards[i].st.Base()
		}
		sh.shards[i] = addRaftRounds(l, l.Context().Pool, sh.lg, 1001+uint32(i), sh.maxBlob, sh.rounds, base)
		st.AddRemote(1001+uint32(i), i, uint32(sh.shards[i].id))
		l.Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: 1})
	}
}

func (sh *storeShards) start() {
	sh.s = sim.New(1, sim.Settings{PoolBytes: 1 << 20})
	node := sh.s.AddNode(sim.DefaultClock, nil, func(*sim.Node) {})
	sh.pool = sh.sd.Pool()
	for i := range sh.loops {
		k := NewKernel(sh.s, node.ID())
		if i == 0 {
			sh.loops[i] = loop.New(loop.Config{Node: node.ID(), ID: 0, Pool: sim.NewPool(1<<20, 0)}, k, &proposer{}, &diskDriver{})
			Add(node, k, sh.loops[i])
			continue
		}
		dr, err := NewDisk(k, sh.sd.D, sh.pool)
		if err != nil {
			sh.t.Fatal(err)
		}
		sh.t.Cleanup(dr.Close)
		l := loop.New(loop.Config{Node: node.ID(), ID: 1, Pool: sh.pool}, k, storeShardsMachine{sh}, diskDriver{dr})
		if err := l.AttachDisk(dr); err != nil {
			sh.t.Fatal(err)
		}
		sh.loops[i], sh.ring = l, dr
		sh.lg = l.AttachLog(nil, storetest.Config().Log, chunkstore.FirstLogIO, chunkstore.EndLogIO, 2)
		AddWithDisk(node, k, l, dr)
	}
}

func (sh *storeShards) mail(m *raftshard.Mail) {
	kind := loop.MailReplayed
	if m.End {
		kind = loop.MailRecovered
	}
	sh.lg.Send(m.Loop, loop.Mail{Shard: loop.ShardID(m.Shard), Kind: kind, Ref: m.Ref, Blob: m.Kind, Data: m.Data})
}

func (sh *storeShards) Pool() core.BufPool     { return sh.sd.Pool() }
func (sh *storeShards) Blocks() uint64         { return sh.sd.Blocks() }
func (sh *storeShards) Execute(a *core.Action) { sh.ring.Execute(a) }
func (sh *storeShards) Complete([]core.Event) []core.Event {
	panic("looptest: the loop hands the store its completions")
}
func (sh *storeShards) Restart() { sh.sd.Restart() }

func (sh *storeShards) Serve(log *disklog.Log) {
	if sh.lg.Log() != log {
		sh.lg.Serve(log)
	}
}

func (sh *storeShards) Step(r *storetest.Rig) bool {
	sh.r = r
	return sh.s.Step()
}

type storeShardsMachine struct{ sh *storeShards }

func (m storeShardsMachine) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	m.sh.r.OnEvent(ctx, ev)
	return out
}

func (m storeShardsMachine) OnLog(ctx *core.Context, _ *loop.Log, ev *disklog.Event, out []core.Action) []core.Action {
	m.sh.r.OnLog(ctx, ev)
	return out
}

// TestRaftShardsOnTwoLoopsRecoverTheirStorage writes rounds of a Raft shard on the device loop and
// one on another loop, each compacting as it goes, and restarts the process: recovery mails each
// shard its blobs, and each storage equals its state before, with every blob of its live rounds,
// and no other, live in the log.
func TestRaftShardsOnTwoLoopsRecoverTheirStorage(t *testing.T) {
	sh := &storeShards{t: t, sd: storetest.NewSimDisk(1), rounds: 7}
	sh.start()
	r := storetest.NewRig(t, sh, storetest.Config())
	r.Opened, r.Mail = sh.opened, sh.mail
	sh.r, sh.maxBlob = r, r.S.Log().MaxBlobBytes()
	sh.opened(r.S)
	r.Settle()
	type state struct {
		hs          raftpb.HardState
		first, last uint64
		ents        []raftpb.Entry
	}
	snap := func() (out [2]state) {
		for i, rr := range sh.shards {
			if rr.err != nil {
				t.Fatalf("shard %d: %v", i, rr.err)
			}
			first, _ := rr.st.FirstIndex()
			last, _ := rr.st.LastIndex()
			ents, err := rr.st.Entries(first, last+1, 1<<62)
			if err != nil {
				t.Fatal(err)
			}
			out[i] = state{rr.st.HardState(), first, last, ents}
		}
		return out
	}
	for i, rr := range sh.shards {
		if rr.done != 7 || len(rr.dead) == 0 {
			t.Fatalf("shard %d made %d rounds of 7, %d blobs dead", i, rr.done, len(rr.dead))
		}
	}
	before := snap()
	live := r.S.Log().LiveBlocks()
	sh.rounds = 0
	r.Restart()
	for i, rr := range sh.shards {
		if !rr.recovered {
			t.Fatalf("shard %d was never told the replay ended", i)
		}
	}
	after := snap()
	for i := range after {
		a, b := after[i], before[i]
		if a.hs != b.hs || a.first != b.first || a.last != b.last || !reflect.DeepEqual(a.ents, b.ents) {
			t.Fatalf("shard %d recovered %+v and entries %d to %d, held %+v and %d to %d", i, a.hs, a.first, a.last, b.hs, b.first, b.last)
		}
	}
	if got := r.S.Log().LiveBlocks(); got != live {
		t.Fatalf("the recovered log holds %d live blocks, %d before", got, live)
	}
}
