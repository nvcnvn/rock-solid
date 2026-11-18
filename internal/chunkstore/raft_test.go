package chunkstore

import (
	"encoding/binary"
	"fmt"
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/lsm"
	"github.com/nvcnvn/rock-solid/internal/raftshard"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// kvShard is a single-voter shard whose committed entry i puts its payload under key i mod
// kvKeys, so its state at an applied index is known without replaying anything. It applies a
// round once the store names it durable, as a lone voter commits on its own flush.
type kvShard struct {
	owner   uint32
	st      *raftshard.Storage
	tree    *lsm.Tree
	applied uint64
	rounds  map[uint64]raftshard.Round
	flush   uint64
}

const kvKeys = 40

var kvConf = raftpb.ConfState{Voters: []uint64{1}}

var kvMark = []byte{0}

func kvKey(i uint64) []byte { return []byte(fmt.Sprintf("k%03d", i%kvKeys)) }

// kvPayload is entry i's payload, of n bytes.
func kvPayload(i uint64, n int) []byte {
	b := make([]byte, n)
	binary.LittleEndian.PutUint64(b, i)
	return b
}

func newKVShard(t *testing.T, owner uint32, maxPiece uint64) *kvShard {
	tree, err := lsm.New(lsm.Config{MaxPiece: int(maxPiece), Check: true})
	if err != nil {
		t.Fatal(err)
	}
	return &kvShard{owner: owner, st: raftshard.NewStorage(), tree: tree, rounds: map[uint64]raftshard.Round{}}
}

func (k *kvShard) Tree() *lsm.Tree { return k.tree }

func (k *kvShard) Mark() (uint64, raftpb.ConfState) {
	b := k.st.Base()
	v := binary.BigEndian.AppendUint64(nil, k.applied)
	v = binary.BigEndian.AppendUint64(v, b.Index)
	v = binary.BigEndian.AppendUint64(v, b.Term)
	if err := k.tree.Put(kvMark, v); err != nil {
		panic(err)
	}
	return k.applied, kvConf
}

func (k *kvShard) Restore() error {
	v, ok := k.tree.Get(kvMark)
	if !ok || len(v) != 24 {
		return lsm.ErrMalformed
	}
	k.applied = binary.BigEndian.Uint64(v)
	md := raftpb.SnapshotMetadata{Index: binary.BigEndian.Uint64(v[8:]), Term: binary.BigEndian.Uint64(v[16:]), ConfState: kvConf}
	if md.Index == 0 {
		return nil
	}
	return k.st.ApplySnapshot(raftpb.Snapshot{Metadata: md})
}

// apply applies the committed entries past the applied index.
func (k *kvShard) apply() {
	last, _ := k.st.LastIndex()
	commit := min(k.st.HardState().Commit, last)
	if commit <= k.applied {
		return
	}
	ents, err := k.st.Entries(k.applied+1, commit+1, 1<<62)
	if err != nil {
		panic(err)
	}
	for _, e := range ents {
		if err := k.tree.Put(kvKey(e.Index), e.Data[:8]); err != nil {
			panic(err)
		}
	}
	k.applied = commit
}

// round is the next round: n entries of size bytes each, committing them.
func (k *kvShard) round(n, size int) raftshard.Round {
	last, _ := k.st.LastIndex()
	for _, r := range k.rounds {
		if l := r.Entries[len(r.Entries)-1].Index; l > last {
			last = l
		}
	}
	var r raftshard.Round
	for i := range uint64(n) {
		r.Entries = append(r.Entries, raftpb.Entry{Term: 1, Index: last + 1 + i, Data: kvPayload(last+1+i, size)})
	}
	r.HardState = raftpb.HardState{Term: 1, Vote: 1, Commit: last + uint64(n)}
	return r
}

// durable takes round flush into the storage and applies it.
func (k *kvShard) durable(flush uint64) {
	r := k.rounds[flush]
	delete(k.rounds, flush)
	k.st.Append(r.Entries)
	k.st.SetHardState(r.HardState)
	k.apply()
}

// check fails t unless the state is the one every entry up to the applied index leaves.
func (k *kvShard) check(t *testing.T) {
	t.Helper()
	for j := uint64(0); j < kvKeys && j <= k.applied; j++ {
		i := k.applied - j
		if i == 0 {
			break
		}
		v, ok := k.tree.Get(kvKey(i))
		if !ok || binary.LittleEndian.Uint64(v) != i {
			t.Fatalf("shard %d at applied %d: key %q holds %v, want entry %d", k.owner, k.applied, kvKey(i), v, i)
		}
	}
}

// raftRig runs a store hosting kv shards on a simulated disk, actions run oldest first.
type raftRig struct {
	t        *testing.T
	pool     *sim.Pool
	ctx      core.Context
	disk     *sim.Disk
	cfg      Config
	s        *Store
	out      Output
	queue    []core.Action
	timerAt  core.Tick
	shards   []*kvShard
	maxPiece uint64
	// acked is each shard's commit index the store made durable before the last crash.
	acked map[uint32]uint64
	wrote uint64
}

func newRaftRig(t *testing.T, cfg Config, geo disklog.Geometry, owners ...uint32) *raftRig {
	r := &raftRig{t: t, pool: sim.NewPool(32<<20, 0), disk: sim.NewDisk(geo.DiskBlocks()), cfg: cfg, acked: map[uint32]uint64{}}
	r.ctx = core.Context{Now: 1, Raw: 2, Pool: r.pool}
	var err error
	if r.s, err = Format(&r.ctx, cfg, disklog.Superblock{LogID: 0x5eed, Geometry: geo}, &r.out); err != nil {
		t.Fatal(err)
	}
	r.maxPiece = r.s.Log().MaxBlobBytes()
	for _, o := range owners {
		k := newKVShard(t, o, r.maxPiece)
		r.s.AddRaft(o, k.st, k)
		r.shards = append(r.shards, k)
	}
	r.take()
	r.settle()
	return r
}

func (r *raftRig) take() {
	r.queue = append(r.queue, r.out.Actions...)
	for _, d := range r.out.Rounds {
		for _, k := range r.shards {
			if k.owner == d.Owner {
				k.durable(d.Round)
			}
		}
	}
	r.out.Reset()
}

// step runs the oldest queued action, or fires the TRIM timer when nothing is queued.
func (r *raftRig) step() bool {
	if len(r.queue) == 0 {
		if r.timerAt == 0 {
			return false
		}
		r.ctx.Now, r.ctx.Raw, r.timerAt = max(r.ctx.Now, r.timerAt), max(r.ctx.Raw, r.timerAt), 0
		r.s.OnEvent(&r.ctx, &core.Event{Kind: core.EvTimer, Timer: disklog.TrimTimer}, &r.out)
		r.take()
		return true
	}
	a := r.queue[0]
	r.queue = r.queue[1:]
	if a.Kind == core.ActSetTimer {
		if a.Timer == disklog.TrimTimer {
			r.timerAt = a.At
		}
		return true
	}
	if a.Kind == core.ActDiskWrite {
		r.wrote += a.Len
	}
	ev := r.disk.Execute(r.pool, &a)
	r.s.OnEvent(&r.ctx, &ev, &r.out)
	r.take()
	for c := r.s.MergeDue(); c != nil; c = r.s.MergeDue() {
		c.Run()
		if err := r.s.InstallMerge(&r.ctx, c, &r.out); err != nil {
			r.t.Fatal(err)
		}
		r.take()
	}
	return true
}

func (r *raftRig) settle() {
	r.t.Helper()
	for n := 0; r.step(); n++ {
		if n == settleSteps {
			r.t.Fatal("the store did not settle")
		}
	}
	if err := r.s.Err(); err != nil {
		r.t.Fatal(err)
	}
}

// write submits a round of n entries of size bytes to shard k.
func (r *raftRig) write(k *kvShard, n, size int) {
	r.t.Helper()
	k.flush++
	k.rounds[k.flush] = k.round(n, size)
	rd := k.rounds[k.flush]
	if err := r.s.WriteRound(&r.ctx, k.owner, k.flush, &rd, &r.out); err != nil {
		r.t.Fatal(err)
	}
	r.take()
}

// crash drops the store with its actions queued, landing unflushed writes at random, and
// restarts it with fresh shards that restore from the disk alone.
func (r *raftRig) crash(rnd *core.Rand) {
	r.t.Helper()
	for _, k := range r.shards {
		r.acked[k.owner] = k.applied
	}
	r.queue, r.timerAt, r.out = nil, 0, Output{}
	r.s.Close()
	r.disk.CrashRandom(rnd)
	r.pool = sim.NewPool(32<<20, 0)
	r.ctx.Pool = r.pool
	var err error
	if r.s, err = Open(&r.ctx, r.cfg, r.disk.Blocks(), &r.out); err != nil {
		r.t.Fatal(err)
	}
	for i, k := range r.shards {
		k.tree.Close()
		n := newKVShard(r.t, k.owner, r.maxPiece)
		n.flush = k.flush
		r.s.AddRaft(n.owner, n.st, n)
		r.shards[i] = n
	}
	r.take()
	r.settle()
	if !r.s.Serving() {
		r.t.Fatalf("the store did not recover: %v", r.s.Err())
	}
	for _, k := range r.shards {
		k.apply()
		if k.applied < r.acked[k.owner] {
			r.t.Fatalf("shard %d recovered to applied %d, below its acknowledged %d", k.owner, k.applied, r.acked[k.owner])
		}
		k.check(r.t)
	}
}

func raftConfig() Config {
	cfg := testConfig()
	cfg.Log.MaxReplayBytes = 64 * core.BlockSize
	return cfg
}

// raftGeometry is a disk of 32 segments of 512 KiB, which the shards' rounds fill many times over.
var raftGeometry = disklog.Geometry{SegmentBlocks: 128, SegmentCount: 32}

// [property] Raft shards on a disk owner's log checkpoint their trees with the chunk index,
// compact their logs, and recover from the manifest's runs and the log after their replay LSNs
// across crashes that land unflushed writes at random: each recovered shard holds every entry
// committed before the crash, its state that of its applied index, while the shards write the
// disk over many times, so the cleaner frees the segments of their dead blobs.
func TestRaftShardsOnTheStoreCheckpointCompactAndRecover(t *testing.T) {
	r := newRaftRig(t, raftConfig(), raftGeometry, 7, 9)
	rnd := core.NewRand(1)
	restored, compacted := 0, 0
	for round := 0; round < 2000; round++ {
		k := r.shards[rnd.Below(2)]
		r.write(k, 1+int(rnd.Below(8)), 1000+int(rnd.Below(7000)))
		for range rnd.Below(6) {
			r.step()
		}
		if round%100 == 99 {
			r.crash(rnd)
			for _, k := range r.shards {
				if k.st.Base().Index > 0 {
					restored++
				}
			}
		}
		for _, k := range r.shards {
			if k.st.Base().Index > 0 {
				compacted++
			}
		}
	}
	r.settle()
	if restored == 0 || compacted == 0 {
		t.Fatalf("no restart restored a compacted base (%d), or no shard compacted (%d)", restored, compacted)
	}
	if size := r.disk.Blocks() * core.BlockSize; r.wrote < 2*size {
		t.Fatalf("the run wrote %d bytes, not twice the disk's %d", r.wrote, size)
	}
	for _, k := range r.shards {
		k.check(t)
	}
}

// A shard added while a checkpoint runs joins the next one: the running checkpoint neither
// writes nor ends a flush of its tree, and a restart after the next restores it.
func TestAShardAddedDuringACheckpointJoinsTheNext(t *testing.T) {
	r := newRaftRig(t, raftConfig(), raftGeometry, 7)
	for !r.s.checkpointing {
		r.write(r.shards[0], 4, 4000)
		for !r.s.checkpointing && r.step() {
		}
	}
	k := newKVShard(t, 11, r.maxPiece)
	r.s.AddRaft(k.owner, k.st, k)
	r.shards = append(r.shards, k)
	rnd := core.NewRand(2)
	for round := range 200 {
		r.write(r.shards[round%2], 1+int(rnd.Below(8)), 1000+int(rnd.Below(7000)))
		r.step()
	}
	r.settle()
	r.crash(rnd)
	if k := r.shards[1]; k.applied == 0 || k.st.Base().Index == 0 {
		t.Fatalf("the added shard restarted at applied %d on base %d", k.applied, k.st.Base().Index)
	}
}
