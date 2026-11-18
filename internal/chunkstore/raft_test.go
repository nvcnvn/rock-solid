package chunkstore

import (
	"encoding/binary"
	"fmt"
	"slices"
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/lsm"
	"github.com/nvcnvn/rock-solid/internal/raftshard"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
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
	// stuck holds every new round's commit at frozen, as a follower's whose leader is gone.
	stuck  bool
	frozen uint64
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

func (k *kvShard) Applied() uint64 { return k.applied }

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
	if k.stuck {
		r.HardState.Commit = k.frozen
	}
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
	// chunkIDs numbers the chunk writes, and failed is the first one's reply that failed.
	chunkIDs uint64
	failed   *Reply
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
	for _, rp := range r.out.Replies {
		if rp.Buf != core.NoBuf {
			r.pool.Free(rp.Buf)
		}
		if rp.Err != nil && r.failed == nil {
			r.failed = &rp
		}
	}
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
		n.flush, n.stuck, n.frozen = k.flush, k.stuck, k.frozen
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

// raftCrashRun writes rounds of random sizes to two kv shards on seed's draws, 2000 of them,
// crashing the store every 50 with unflushed writes landing at random, and counts the restarts
// that restored a compacted base, the rounds after which a shard had compacted and the crashes in
// each window of the store's checkpoint.
func raftCrashRun(t *testing.T, seed uint64, rounds int) (restored, compacted int, windows storeCrashWindows, wrote uint64) {
	r := newRaftRig(t, raftConfig(), raftGeometry, 7, 9)
	rnd := core.NewRand(seed)
	for round := range rounds {
		k := r.shards[rnd.Below(2)]
		r.write(k, 1+int(rnd.Below(8)), 1000+int(rnd.Below(7000)))
		for range rnd.Below(6) {
			r.step()
		}
		if round%50 == 49 {
			windows.add(r.s)
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
	for _, k := range r.shards {
		k.check(t)
	}
	return restored, compacted, windows, r.wrote
}

// storeCrashWindows counts a store's crashes by where its checkpoint stands: none running, a
// piece in flight or waiting for a buffer, or every piece durable and the manifest in flight.
type storeCrashWindows [3]int

func (w *storeCrashWindows) add(s *Store) {
	switch {
	case !s.checkpointing:
		w[0]++
	case s.inPiece || s.pieceWait:
		w[1]++
	default:
		w[2]++
	}
}

// [property] Raft shards on a disk owner's log checkpoint their trees with the chunk index,
// compact their logs, and recover from the manifest's runs and the log after their replay LSNs
// across crashes that land unflushed writes at random: each recovered shard holds every entry
// committed before the crash, its state that of its applied index, while the shards write the
// disk over many times, so the cleaner frees the segments of their dead blobs. The crashes land in
// every window of the checkpoint.
func TestRaftShardsOnTheStoreCheckpointCompactAndRecover(t *testing.T) {
	size := raftGeometry.DiskBlocks() * core.BlockSize
	var windows storeCrashWindows
	restored, compacted := 0, 0
	seeds.Run(t, 0, 4, func(seed uint64) {
		res, comp, w, wrote := raftCrashRun(t, seed, 2000)
		if wrote < 2*size {
			t.Fatalf("the run wrote %d bytes, not twice the disk's %d", wrote, size)
		}
		restored, compacted = restored+res, compacted+comp
		for i := range w {
			windows[i] += w[i]
		}
	})
	t.Logf("crashes with no checkpoint running %d, with a piece in flight %d, with the manifest in flight %d", windows[0], windows[1], windows[2])
	if restored == 0 || compacted == 0 {
		t.Fatalf("no restart restored a compacted base (%d), or no shard compacted (%d)", restored, compacted)
	}
	if seeds.Default() && slices.Contains(windows[:], 0) {
		t.Fatal("a window of the checkpoint had no crash")
	}
}

// Regression (seed 98 of the crash run): a restart's replay adds again every Raft blob past the
// replay floor, those that died before the manifest included, so the log must recount its usage
// before the host reports them dead again, or the dead report underflows a segment's live count.
func TestARestartReportsBlobsDeadAfterTheLogRecounts(t *testing.T) {
	raftCrashRun(t, 98, 1000)
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

// The cleaner rewrites the runs of every tree with a live piece in a metadata victim, the shards'
// as well as the chunk index's, so a segment holding only a shard's pieces is freed once they move.
func TestTheCleanerFreesASegmentHoldingOnlyShardPieces(t *testing.T) {
	cfg := raftConfig()
	cfg.CleanFree = int(raftGeometry.SegmentCount)
	r := newRaftRig(t, cfg, raftGeometry, 7, 9)
	var victims []uint32
	r.s.metaVictim = func(seg uint32) {
		if p, _ := r.s.ix.Tree().RunsIn(seg, nil); len(p) > 0 {
			return
		}
		for _, k := range r.shards {
			if p, _ := k.tree.RunsIn(seg, nil); len(p) > 0 && !slices.Contains(victims, seg) {
				victims = append(victims, seg)
			}
		}
	}
	rnd := core.NewRand(3)
	for round := 0; round < 3000; round++ {
		r.write(r.shards[min(1, round/300+int(rnd.Below(2)))], 1+int(rnd.Below(8)), 1000+int(rnd.Below(7000)))
		for range rnd.Below(6) {
			r.step()
		}
		for _, seg := range victims {
			if u, _ := r.s.log.Usage(seg); u.State == disklog.SegmentFree {
				r.settle()
				return
			}
		}
	}
	t.Fatalf("no metadata victim holding only shard pieces was freed (%d were victims)", len(victims))
}

// A due checkpoint that fails leaves each shard named with the replay LSN of the last that ended,
// so a step checkpoint after it pairs the runs it carries with the replay LSN they were marked at.
func TestAFailedDueCheckpointKeepsTheShardsReplayLSN(t *testing.T) {
	r := newRaftRig(t, raftConfig(), raftGeometry, 7)
	k := r.shards[0]
	for r.s.shards[0].replay == 0 || k.st.Base().Index == 0 {
		r.write(k, 4, 4000)
		r.settle()
	}
	old := r.s.shards[0].replay
	for range 5 {
		r.write(k, 4, 4000)
	}
	r.settle()
	durable := r.s.log.DurableLSN()
	idx, err := r.s.beginShards(&r.ctx, true, durable, nil)
	if err != nil {
		t.Fatal(err)
	}
	if idx[0].ReplayLSN == old {
		t.Fatalf("the failed due checkpoint names the shard with %d, its last's replay LSN", old)
	}
	r.s.shardFlushesFailed()
	if idx, err = r.s.beginShards(&r.ctx, false, durable, nil); err != nil {
		t.Fatal(err)
	}
	if len(idx) != 1 || idx[0].ReplayLSN != old {
		t.Fatalf("the step checkpoint after a failed due one names %v, want replay LSN %d", idx, old)
	}
}

// chunkWrite writes data at the start of chunk c.
func (r *raftRig) chunkWrite(c core.ChunkID, data []byte) {
	buf, err := r.pool.Alloc(len(data))
	if err != nil {
		r.t.Fatal(err)
	}
	copy(r.pool.Bytes(buf), data)
	r.chunkIDs++
	r.s.Write(&r.ctx, Request{ID: r.chunkIDs, Chunk: c, Len: uint32(len(data)), Buf: buf}, &r.out)
	r.take()
}

// A follower shard whose leader is gone keeps being sent entries it cannot commit. Past its share
// of twice max_replay_bytes it takes no more, and the store's checkpoints write the entries it
// retains again, so it never holds the replay floor back for good: the chunk writes go on over the
// disk three times, every one answered, and a restart finds every entry it acknowledged. Once its
// leader is back it commits and applies them all.
func TestAStuckFollowerShardNeverStallsChunkWrites(t *testing.T) {
	r := newRaftRig(t, raftConfig(), raftGeometry, 7)
	k := r.shards[0]
	for range 20 {
		r.write(k, 4, 4000)
		r.step()
	}
	r.settle()
	k.stuck, k.frozen = true, k.st.HardState().Commit
	bound := 2*r.cfg.Log.MaxReplayBytes + 8*8000
	rnd := core.NewRand(4)
	data := make([]byte, 32<<10)
	size := raftGeometry.DiskBlocks() * core.BlockSize
	took := 0
	for i := uint64(0); i*uint64(len(data)) < 3*size; i++ {
		r.chunkWrite(core.ChunkID{Index: uint32(i % 32)}, data)
		if !r.s.RaftFull(k.owner) {
			r.write(k, 1+int(rnd.Below(8)), 1000+int(rnd.Below(7000)))
			took++
		}
		for range rnd.Below(6) {
			r.step()
		}
		if r.failed != nil || !r.s.Serving() {
			t.Fatalf("chunk write %d of %d: reply %+v, store error %v", i, 3*size/uint64(len(data)), r.failed, r.s.Err())
		}
		if b := k.st.Bytes(); b > bound {
			t.Fatalf("the stuck follower retains %d bytes, past its bound %d", b, bound)
		}
		if i%500 == 499 {
			r.settle()
			last, _ := k.st.LastIndex()
			r.crash(rnd)
			k = r.shards[0]
			if got, _ := k.st.LastIndex(); got < last {
				t.Fatalf("the stuck follower restarted with entries to %d, below the %d it acknowledged", got, last)
			}
		}
	}
	r.settle()
	if r.failed != nil {
		t.Fatalf("a chunk write failed: %+v", r.failed)
	}
	last, _ := k.st.LastIndex()
	if took == 0 || last <= k.frozen {
		t.Fatalf("the stuck follower took %d rounds and holds entries to %d, committed %d", took, last, k.frozen)
	}
	k.stuck = false
	for range 3 {
		r.write(k, 1, 1000)
		r.settle()
	}
	if k.applied != last+3 {
		t.Fatalf("the follower applied %d after its leader came back, want %d", k.applied, last+3)
	}
	k.check(t)
	if r.s.RaftFull(k.owner) {
		t.Fatal("the follower is still full once it applied its entries")
	}
}
