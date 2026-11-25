package chunkstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
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
	k.st.Took(flush)
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
	// hold, when set, holds back each durable round from its shard for up to hold pumps, drawn
	// from delay, as a shard on another loop learns of it late; held is those rounds, in order.
	hold  uint64
	delay *core.Rand
	pumps uint64
	held  []heldRound
	// mail is what the store mailed its shards on the round driver, but for a checkpoint's asks
	// for their pieces, which take answers that they have none, or with remotePiece set a run of
	// two such pieces each due checkpoint, remoteSent the pieces of the running one sent.
	mail        []raftshard.Mail
	remotePiece []byte
	remoteSent  int
}

type heldRound struct {
	owner uint32
	round uint64
	at    uint64
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
		at := r.pumps
		if r.hold > 0 {
			at += r.delay.Below(r.hold + 1)
		}
		for _, h := range r.held {
			if h.owner == d.Owner {
				at = max(at, h.at)
			}
		}
		r.held = append(r.held, heldRound{d.Owner, d.Round, at})
	}
	mail := r.out.Mail
	r.out.Mail = nil
	r.out.Reset()
	for _, m := range mail {
		if m.Kind == raftshard.MailFlush || m.Kind == raftshard.MailPiece {
			if m.Kind == raftshard.MailFlush {
				r.remoteSent = 0
			}
			var piece []byte
			if r.remotePiece != nil && r.remoteSent < 2 {
				piece = r.remotePiece
				r.remoteSent++
			}
			r.s.RemotePiece(m.Owner, m.Base, piece, piece != nil && r.remoteSent == 2)
			r.s.OnLog(&r.ctx, nil, &r.out)
			r.take()
			continue
		}
		r.mail = append(r.mail, m)
	}
	r.deliver(r.pumps)
}

// deliver tells each shard of the durable rounds held back until pump at or before now.
func (r *raftRig) deliver(now uint64) {
	keep := 0
	for _, h := range r.held {
		if h.at > now {
			r.held[keep] = h
			keep++
			continue
		}
		for _, k := range r.shards {
			if k.owner == h.owner {
				k.durable(h.round)
			}
		}
	}
	r.held = r.held[:keep]
}

// step runs the oldest queued action, or fires the TRIM timer when nothing is queued.
func (r *raftRig) step() bool {
	r.pumps++
	r.deliver(r.pumps)
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
	for n := 0; r.step() || len(r.held) > 0; n++ {
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
	r.queue, r.timerAt, r.out, r.held = nil, 0, Output{}, nil
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

// Regression (seed 8 of the crash run): a restart's replay adds again every Raft blob past the
// replay floor, those that died before the manifest included, so the log must recount its usage
// before the host reports them dead again, or the dead report underflows a segment's live count.
func TestARestartReportsBlobsDeadAfterTheLogRecounts(t *testing.T) {
	raftCrashRun(t, 8, 2000)
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

// Regression: a follower that keeps taking entries it cannot commit, under its bound, while chunk
// writes drive due checkpoints, has a round durable in the handler that begins one; the full
// rewrite waits for the shard to take it, and every restart finds every entry it acknowledged.
func TestAStuckFollowerTakingRoundsKeepsThemAcrossRestarts(t *testing.T) {
	r := newRaftRig(t, raftConfig(), raftGeometry, 7)
	k := r.shards[0]
	for range 20 {
		r.write(k, 4, 4000)
		r.step()
	}
	r.settle()
	k.stuck, k.frozen = true, k.st.HardState().Commit
	rnd := core.NewRand(4)
	data := make([]byte, 32<<10)
	for i := range 3000 {
		r.chunkWrite(core.ChunkID{Index: uint32(i % 32)}, data)
		if !r.s.RaftFull(k.owner) {
			r.write(k, 1, 64)
		}
		for range rnd.Below(6) {
			r.step()
		}
		if r.failed != nil || !r.s.Serving() {
			t.Fatalf("chunk write %d: reply %+v, store error %v", i, r.failed, r.s.Err())
		}
		if i%100 == 99 {
			r.settle()
			last, _ := k.st.LastIndex()
			r.crash(rnd)
			k = r.shards[0]
			if got, _ := k.st.LastIndex(); got < last {
				t.Fatalf("restart after chunk write %d: entries to %d, below the %d acknowledged", i, got, last)
			}
		}
	}
}

// held is the entry payload bytes of the rounds k wrote that its storage has not taken: in
// flight, waiting for buffers, or durable and held back from it.
func (k *kvShard) held() uint64 {
	var n uint64
	for _, r := range k.rounds {
		for _, e := range r.Entries {
			n += uint64(len(e.Data))
		}
	}
	return n
}

// raftBoundRun hosts three kv shards beside chunk writes that go over the disk twice, the last a
// follower whose leader is gone. On seed's draws each shard is sent bursts of rounds, each taken
// only while the store does not report the shard full, durable rounds reach their shard up to 12
// pumps late, and the store crashes every 256 chunk writes. It fails unless each shard retains at
// most its share of twice max_replay_bytes and a round, and every chunk write is answered. A
// restart restores a shard on the base before its last compaction, so until it compacts again it
// may hold what it held at that checkpoint and what it took after, each within the bound.
func raftBoundRun(t *testing.T, seed uint64) {
	r := newRaftRig(t, raftConfig(), raftGeometry, 7, 9, 11)
	rnd := core.NewRand(seed)
	r.hold, r.delay = 12, rnd
	for range 20 {
		r.write(r.shards[2], 4, 4000)
		r.step()
	}
	r.settle()
	stuck := r.shards[2]
	stuck.stuck, stuck.frozen = true, stuck.st.HardState().Commit
	const round = 8 * 8000
	share := 2 * r.cfg.Log.MaxReplayBytes / uint64(len(r.shards))
	data := make([]byte, 32<<10)
	size := raftGeometry.DiskBlocks() * core.BlockSize
	// restored is each shard's bytes and base at the last restart, while it has not compacted since.
	type restart struct{ bytes, base uint64 }
	restored := map[uint32]restart{}
	bounded := func(i uint64) {
		for _, k := range r.shards {
			bound := share + round
			if at, ok := restored[k.owner]; ok && k.st.Base().Index == at.base {
				bound = max(bound, at.bytes)
			}
			if b := k.st.Bytes() + k.held(); b > bound {
				t.Fatalf("chunk write %d: shard %d retains %d bytes, past its share %d and a round", i, k.owner, b, share)
			}
		}
	}
	for i := uint64(0); i*uint64(len(data)) < 2*size; i++ {
		r.chunkWrite(core.ChunkID{Index: uint32(i % 32)}, data)
		if rnd.Below(3) == 0 {
			k := r.shards[rnd.Below(3)]
			for range 1 + rnd.Below(6) {
				if r.s.RaftFull(k.owner) {
					break
				}
				r.write(k, 1+int(rnd.Below(8)), 1000+int(rnd.Below(7000)))
			}
			bounded(i)
		}
		for range rnd.Below(6) {
			r.step()
		}
		if r.failed != nil || !r.s.Serving() {
			t.Fatalf("chunk write %d: reply %+v, store error %v", i, r.failed, r.s.Err())
		}
		bounded(i)
		if i%256 == 255 {
			r.crash(rnd)
			for _, k := range r.shards {
				if b := k.st.Bytes(); b > 2*(share+round) {
					t.Fatalf("chunk write %d: shard %d restarted with %d bytes, past twice its share %d and a round", i, k.owner, b, share)
				}
				restored[k.owner] = restart{k.st.Bytes(), k.st.Base().Index}
			}
		}
	}
	r.settle()
	if r.failed != nil {
		t.Fatalf("a chunk write failed: %+v", r.failed)
	}
}

// [property] Shards on a chunk store retain at most their share of twice max_replay_bytes and a
// round, under bursts of proposals, durable rounds reaching their shards late, crashes and a
// stuck follower among them, and never stall the chunk writes beside them.
func TestRaftShardsRetainAtMostTheirShare(t *testing.T) {
	seeds.Run(t, 0, 4, func(seed uint64) { raftBoundRun(t, seed) })
}

// Regression (seed 0 of the bound run): a shard told of its durable rounds late retained past its
// share, since the store counted such a round neither in flight nor in the shard's storage and
// let the shard take more.
func TestAShardToldLateOfItsRoundsKeepsWithinItsShare(t *testing.T) {
	raftBoundRun(t, 0)
}

// A removed shard stays removed: a crash before the checkpoint after its removal, whose manifest
// still names it, and one after it restart without it and with every other shard whole; its owner
// goes to no new shard until the replay floor passes its blobs, the segments of its tree's pieces
// are freed, and a new shard given its owner then
// restarts with its own entries alone, none of the removed shard's runs or rounds.
func TestARemovedShardStaysRemovedAcrossACrash(t *testing.T) {
	for _, after := range []bool{false, true} {
		r := newRaftRig(t, raftConfig(), raftGeometry, 7, 9)
		rnd := core.NewRand(5)
		for range 300 {
			r.write(r.shards[rnd.Below(2)], 1+int(rnd.Below(8)), 1000+int(rnd.Below(7000)))
			for range rnd.Below(6) {
				r.step()
			}
		}
		r.settle()
		names9 := func() bool {
			return slices.ContainsFunc(r.s.idx, func(ix disklog.CheckpointIndex) bool { return ix.Owner == 9 })
		}
		if !names9() {
			t.Fatal("the last manifest before the removal does not name the shard")
		}
		pieces := map[uint32]bool{}
		for _, p := range r.shards[1].tree.Pieces(nil) {
			pieces[p.Segment] = true
		}
		if err := r.s.RemoveRaft(&r.ctx, 9, &r.out); err != nil {
			t.Fatal(err)
		}
		r.take()
		r.shards = r.shards[:1]
		if r.s.shardOf(9) >= 0 || r.s.raft.Reusable(9) {
			t.Fatalf("after the removal: hosted %v, owner reusable %v", r.s.shardOf(9) >= 0, r.s.raft.Reusable(9))
		}
		if after {
			for names9() || r.s.checkpointing {
				r.write(r.shards[0], 4, 4000)
				r.settle()
			}
			for i := 0; len(pieces) > 0; i++ {
				if i == 3000 {
					t.Fatalf("segments %v holding the removed shard's pieces were never freed", pieces)
				}
				r.write(r.shards[0], 1+int(rnd.Below(8)), 1000+int(rnd.Below(7000)))
				r.step()
				for seg := range pieces {
					if u, _ := r.s.log.Usage(seg); u.State == disklog.SegmentFree {
						delete(pieces, seg)
					}
				}
			}
		}
		r.crash(rnd)
		for i := 0; !r.s.raft.Reusable(9); i++ {
			if i == 2000 {
				t.Fatalf("after %v: the removed shard's owner never became reusable", after)
			}
			r.write(r.shards[0], 4, 4000)
			r.step()
		}
		r.settle()
		k := newKVShard(t, 9, r.maxPiece)
		r.s.AddRaft(9, k.st, k)
		r.shards = append(r.shards, k)
		for range 3 {
			r.write(k, 1, 1000)
		}
		r.settle()
		r.crash(rnd)
		k = r.shards[1]
		if last, _ := k.st.LastIndex(); k.applied != 3 || last != 3 {
			t.Fatalf("after %v: the new shard under the removed one's owner restarted at applied %d with entries to %d, want 3", after, k.applied, last)
		}
	}
}

// A removed shard's tree pieces stay live until a checkpoint begun after the removal ends, since
// until then the newest manifest recovery may load names them: their segments keep every live
// block, and a crash at the last step before that checkpoint ends restarts the shard, added again
// as a driver that learns its deletion from the replay would, with all it had applied.
func TestARemovedShardsPiecesStayLiveUntilAManifestLeavesItOut(t *testing.T) {
	// run removes the shard and steps until the checkpoint after it ends, crashing instead at
	// step crashAt, and returns the steps it took.
	cfg := raftConfig()
	cfg.Log.MaxReplayBytes = 2 * uint64(raftGeometry.SegmentBlocks) * core.BlockSize
	run := func(crashAt int) int {
		r := newRaftRig(t, cfg, raftGeometry, 7, 9)
		rnd := core.NewRand(5)
		for range 300 {
			r.write(r.shards[rnd.Below(2)], 1+int(rnd.Below(8)), 1000+int(rnd.Below(7000)))
			for range rnd.Below(6) {
				r.step()
			}
		}
		r.settle()
		removed := r.shards[1]
		live := map[uint32]uint32{}
		for _, p := range removed.tree.Pieces(nil) {
			u, _ := r.s.log.Usage(p.Segment)
			live[p.Segment] = u.Live
		}
		if err := r.s.RemoveRaft(&r.ctx, 9, &r.out); err != nil {
			t.Fatal(err)
		}
		r.take()
		r.shards = r.shards[:1]
		began, n := false, 0
		for ; !began || r.s.checkpointing; n++ {
			if n == crashAt {
				r.shards = append(r.shards, removed)
				r.crash(rnd)
				return n
			}
			for seg, was := range live {
				if u, _ := r.s.log.Usage(seg); u.Live < was || u.State == disklog.SegmentFree {
					t.Fatalf("step %d: segment %d holding the removed shard's pieces fell to %d live blocks from %d before a manifest left it out", n, seg, u.Live, was)
				}
			}
			r.write(r.shards[0], 4, 4000)
			r.step()
			began = began || r.s.checkpointing
		}
		return n
	}
	run(run(-1) - 1)
}

// A shard on the round driver removed part way through its tree's flush under a checkpoint keeps
// its pieces live through that checkpoint and the next, begun after the removal, whose manifest is
// the first to leave the shard out: that one reports dead its runs' pieces and those of the flush,
// written before the removal or after, and once a segment header names its manifest the log holds
// them no more. The first due checkpoint that wrote its runs begins with nothing durable.
func TestARemovedRemoteShardsPiecesDieOnceAManifestLeavesItOut(t *testing.T) {
	r := newRaftRig(t, raftConfig(), raftGeometry, 7)
	r.s.AddRemote(9, 2, 4)
	r.remotePiece = fill(core.BlockSize, 9)
	for i := range 2 {
		r.s.RemoteFull()
		r.chunkWrite(chunk(1), fill(core.BlockSize, byte(i)))
		r.settle()
	}
	runs := r.s.raft.RemoteIndexes(nil, r.s.log.DurableLSN())[0].Runs
	if len(runs) != 2 {
		t.Fatalf("two due checkpoints left the shard on the round driver runs %v", runs)
	}
	busy := func(n int, why string) {
		if n == settleSteps {
			t.Fatal(why)
		}
		r.write(r.shards[0], 4, 4000)
		r.step()
	}
	r.s.RemoteFull()
	for n := 0; !r.s.raft.Flushing() || r.remoteSent < 2; n++ {
		busy(n, "the shard on the round driver never had a piece durable under a due checkpoint")
	}
	r.s.RemoveRemote(9, 0)
	manifest, ended := r.s.log.Manifest(), 0
	var dead []disklog.BlobRef
	for n := 0; ended < 2; n++ {
		busy(n, "the checkpoints running at the removal and after never ended")
		if m := r.s.log.Manifest(); m != manifest {
			manifest, ended = m, ended+1
		}
		if ended == 1 && r.s.checkpointing {
			dead = slices.Clone(r.s.removedPieces)
		}
	}
	if len(dead) != 3*2 || !slices.Contains(dead, runs[0]) || !slices.Contains(dead, runs[1]) {
		t.Fatalf("the checkpoint begun after the removal will report dead %v, want the two pieces of each of runs %v and of the flush", dead, runs)
	}
	for n := 0; r.s.log.NamedLSN() < manifest.LSN; n++ {
		busy(n, "no segment header named the manifest that left the removed shard out")
	}
	for _, p := range dead {
		if err := r.s.log.PiecesDead([]disklog.BlobRef{p}); !errors.Is(err, disklog.ErrMalformed) {
			t.Fatalf("the log still holds the removed shard's piece %v once a segment header names the manifest that left it out", p)
		}
	}
}

// RemoveRaft refuses an owner the store does not host, or one it is already removing.
func TestRemoveRaftRefusesAnOwnerNotHosted(t *testing.T) {
	r := newRaftRig(t, raftConfig(), raftGeometry, 7)
	if err := r.s.RemoveRaft(&r.ctx, 9, &r.out); !errors.Is(err, ErrNotHosted) {
		t.Fatalf("removing an owner never hosted: %v", err)
	}
	if err := r.s.RemoveRaft(&r.ctx, 7, &r.out); err != nil {
		t.Fatal(err)
	}
	if err := r.s.RemoveRaft(&r.ctx, 7, &r.out); !errors.Is(err, ErrNotHosted) {
		t.Fatalf("removing an owner twice: %v", err)
	}
	r.take()
}

// A store hosting a shard and knowing one on the round driver mails the latter its share, half of
// twice max_replay_bytes, which the hosted one is full at too. Asked for a checkpoint by it, the
// store takes one at its next pump, though no checkpoint is due, and mails it the durable manifest
// with the base of the due one before; the next names the base of that one, and a step checkpoint
// mails nothing. A third shard on the round driver makes the store mail both the new share, a
// third.
func TestAStoreMailsAShardOnTheRoundDriverItsShareAndCheckpoints(t *testing.T) {
	cfg := raftConfig()
	r := newRaftRig(t, cfg, raftGeometry, 5)
	r.s.AddRemote(9, 2, 4)
	r.chunkWrite(chunk(1), fill(core.BlockSize, 1))
	r.settle()
	share := cfg.Log.MaxReplayBytes
	if want := []raftshard.Mail{{Kind: raftshard.MailShare, Loop: 2, Shard: 4, Share: share}}; !reflect.DeepEqual(r.mail, want) || r.s.raft.Share(r.s.raftBytes()) != share {
		t.Fatalf("the store mailed %+v and shares %d, want %+v", r.mail, r.s.raft.Share(r.s.raftBytes()), want)
	}
	for i := range 2 {
		r.mail = r.mail[:0]
		before := r.s.lastBase
		r.s.RemoteFull()
		r.chunkWrite(chunk(2), fill(core.BlockSize, byte(i)))
		r.settle()
		if len(r.mail) != 1 || r.mail[0].Kind != raftshard.MailCheckpointed || r.mail[0].Loop != 2 || r.mail[0].Shard != 4 || r.mail[0].Base != before || r.s.lastBase <= before {
			t.Fatalf("asked for checkpoint %d after a manifest of base %d, the store mailed %+v and wrote one of base %d", i, before, r.mail, r.s.lastBase)
		}
	}
	r.mail = r.mail[:0]
	manifest := r.s.Log().Manifest()
	r.s.beginCheckpoint(&r.ctx, false, &r.out)
	r.take()
	r.chunkWrite(chunk(2), fill(core.BlockSize, 2))
	r.settle()
	if r.s.Log().Manifest() == manifest || len(r.mail) != 0 {
		t.Fatalf("a step checkpoint wrote manifest %+v after %+v and mailed %+v", r.s.Log().Manifest(), manifest, r.mail)
	}
	r.s.AddRemote(10, 2, 5)
	r.chunkWrite(chunk(3), fill(core.BlockSize, 3))
	r.settle()
	third := 2 * share / 3
	if want := []raftshard.Mail{{Kind: raftshard.MailShare, Loop: 2, Shard: 4, Share: third}, {Kind: raftshard.MailShare, Loop: 2, Shard: 5, Share: third}}; !reflect.DeepEqual(r.mail, want) {
		t.Fatalf("with a third shard the store mailed %+v, want %+v", r.mail, want)
	}
}
