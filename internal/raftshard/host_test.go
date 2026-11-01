package raftshard

import (
	"bytes"
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// rigGeometry is a disk of 16 segments of 64 blocks.
var rigGeometry = disklog.Geometry{SegmentBlocks: 64, SegmentCount: 16}

func rigConfig() disklog.Config {
	cfg := disklog.DefaultConfig()
	cfg.MaxRecordBytes, cfg.ReadDepth, cfg.NoReserve, cfg.NoTrim, cfg.Check = 16<<10, 4, true, true, true
	return cfg
}

// rig is a host and its log on a simulated disk, running the log's actions one at a time in order.
type rig struct {
	t      testing.TB
	ctx    core.Context
	disk   *sim.Disk
	rec    *disklog.Recovery
	host   *Host
	out    disklog.Output
	queue  []core.Action
	done   []Done
	fsyncs int
	// durable is each Durable event's tickets and BlobRefs, in order.
	durable [][]disklog.Ticket
}

func newRig(t testing.TB) *rig {
	r := &rig{t: t, disk: sim.NewDisk(rigGeometry.DiskBlocks())}
	r.ctx = core.Context{Now: 1, Raw: 1, Pool: sim.NewPool(8<<20, 0)}
	log, err := disklog.Format(&r.ctx, rigConfig(), disklog.Superblock{LogID: 7, Geometry: rigGeometry}, 1, &r.out)
	if err != nil {
		t.Fatal(err)
	}
	r.host = NewHost(log)
	r.run()
	return r
}

// absorb takes the log's output: Durable events go to the host, ManifestLoaded starts the replay,
// Replayed events go to the host, and Recovered hands it the log.
func (r *rig) absorb() {
	r.t.Helper()
	r.queue = append(r.queue, r.out.Actions...)
	for i := range r.out.Events {
		ev := &r.out.Events[i]
		var err error
		switch ev.Kind {
		case disklog.Durable:
			r.durable = append(r.durable, append([]disklog.Ticket(nil), ev.Tickets...))
			var more disklog.Output
			r.done, err = r.host.Durable(&r.ctx, ev, &more, r.done)
			r.queue = append(r.queue, more.Actions...)
		case disklog.ManifestLoaded:
			var more disklog.Output
			err = r.rec.Replay(&r.ctx, &more)
			defer func() { r.out = more; r.absorb() }()
		case disklog.Replayed:
			err = r.host.Replayed(ev)
		case disklog.Recovered:
			var log *disklog.Log
			if log, err = r.rec.Log(&r.ctx); err == nil {
				r.host.Recovered(log)
				r.rec = nil
			}
		case disklog.RecoveryFailed, disklog.Failed, disklog.InDoubt:
			err = ev.Err
		}
		if err != nil {
			r.t.Fatalf("%+v: %v", ev, err)
		}
	}
	r.out.Reset()
}

// step runs the oldest queued action and feeds its completion back.
func (r *rig) step() {
	r.t.Helper()
	a := r.queue[0]
	r.queue = r.queue[1:]
	if a.Kind == core.ActDiskFsync {
		r.fsyncs++
	}
	ev := r.disk.Execute(r.ctx.Pool, &a)
	r.ctx.Now++
	if r.rec != nil {
		r.rec.OnEvent(&r.ctx, &ev, &r.out)
	} else {
		r.host.Log.OnEvent(&r.ctx, &ev, &r.out)
	}
	r.absorb()
}

func (r *rig) run() {
	r.t.Helper()
	for len(r.queue) > 0 {
		r.step()
	}
	r.absorb()
	for len(r.queue) > 0 {
		r.step()
	}
}

func (r *rig) write(owner uint32, flush uint64, rd *Round) {
	r.t.Helper()
	if err := r.host.Write(&r.ctx, owner, flush, rd, &r.out); err != nil {
		r.t.Fatal(err)
	}
	r.absorb()
}

func (r *rig) takeDone() []Done {
	d := r.done
	r.done = nil
	return d
}

// crash drops what the disk did not flush and recovers the log into a new host holding a fresh storage for each owner.
func (r *rig) crash(owners ...uint32) map[uint32]*Storage {
	r.t.Helper()
	r.disk.Crash(func(int, core.LBA) bool { return false })
	r.queue, r.done, r.durable = nil, nil, nil
	r.out.Reset()
	r.ctx.Pool = sim.NewPool(8<<20, 0)
	r.host = NewHost(nil)
	sts := map[uint32]*Storage{}
	for _, o := range owners {
		sts[o] = NewStorage()
		r.host.Add(o, sts[o])
	}
	var err error
	if r.rec, err = disklog.Recover(&r.ctx, rigConfig(), rigGeometry.DiskBlocks(), disklog.ModeOpen, 1<<20, &r.out); err != nil {
		r.t.Fatal(err)
	}
	r.absorb()
	r.run()
	if r.rec != nil || r.host.Log == nil {
		r.t.Fatal("recovery did not finish")
	}
	return sts
}

// live is the live blocks the usage table counts over every segment.
func (r *rig) live() uint32 {
	var n uint32
	for s := uint32(1); s < rigGeometry.SegmentCount; s++ {
		u, _ := r.host.Log.Usage(s)
		n += u.Live
	}
	return n
}

func entries(first, n uint64, term uint64, size int) []raftpb.Entry {
	var es []raftpb.Entry
	for i := first; i < first+n; i++ {
		es = append(es, raftpb.Entry{Index: i, Term: term, Data: bytes.Repeat([]byte{byte(i)}, size)})
	}
	return es
}

func TestARoundIsDoneOnceItsLastBlobIsDurable(t *testing.T) {
	r := newRig(t)
	st := NewStorage()
	r.host.Add(5, st)
	big := Round{Entries: entries(1, 3, 1, 10<<10), HardState: raftpb.HardState{Term: 1, Vote: 1, Commit: 1}}
	r.write(5, 1, &big)
	if n := len(r.host.flights); n < 2 {
		t.Fatalf("a 30 KiB round went out as %d blobs, want at least 2 of a 16 KiB record", n)
	}
	for len(r.durable) == 0 {
		r.step()
	}
	if len(r.done) != 0 {
		t.Fatalf("the round was done with only its first blob durable: %v", r.done)
	}
	r.run()
	if d := r.takeDone(); len(d) != 1 || d[0] != (Done{Owner: 5, Round: 1}) {
		t.Fatalf("done %v, want owner 5's round 1", d)
	}
}

// A round's blobs die once every index it holds is compacted and a later round holds a newer
// hard state; a round without a hard state needs only the compaction.
func TestARoundsBlobsDieOnceCompactedAndItsHardStateIsReplaced(t *testing.T) {
	r := newRig(t)
	st := NewStorage()
	r.host.Add(5, st)
	write := func(flush uint64, rd Round) uint32 {
		before := r.live()
		r.write(5, flush, &rd)
		r.run()
		st.Append(rd.Entries)
		if d := r.takeDone(); len(d) != 1 || d[0].Round != flush {
			t.Fatalf("round %d: done %v", flush, d)
		}
		return r.live() - before
	}
	a := write(1, Round{Entries: entries(1, 2, 1, 5000), HardState: raftpb.HardState{Term: 1, Vote: 1}})
	b := write(2, Round{Entries: entries(3, 1, 1, 5000)})
	if a == 0 || b == 0 {
		t.Fatalf("rounds counted %d and %d live blocks", a, b)
	}
	total := r.live()
	if err := r.host.Compact(5, 3, raftpb.ConfState{Voters: []uint64{1}}); err != nil {
		t.Fatal(err)
	}
	if r.live() != total-b {
		t.Fatalf("after compacting both rounds %d blocks are live, want %d: the round without a hard state stays, or the newest hard state's round left", r.live(), total-b)
	}
	write(3, Round{HardState: raftpb.HardState{Term: 2, Vote: 1}})
	if r.live() != 1 {
		t.Fatalf("with a newer hard state durable %d blocks are live, want the 1 of its round", r.live())
	}
	r.host.Remove(5)
	if r.live() != 0 {
		t.Fatalf("a removed shard's blobs count %d live blocks", r.live())
	}
}

// A removed shard's round still in flight is dead once durable, and never done.
func TestARemovedShardsRoundInFlightDiesOnceDurable(t *testing.T) {
	r := newRig(t)
	r.host.Add(5, NewStorage())
	r.write(5, 1, &Round{Entries: entries(1, 3, 1, 10<<10)})
	r.host.Remove(5)
	r.run()
	if len(r.done) != 0 || r.live() != 0 {
		t.Fatalf("a removed shard's round was done (%v) or its blobs count %d live blocks", r.done, r.live())
	}
}

// A removed owner hosted again while its old round is in flight would take that round's Done.
func TestAnOwnerWithARoundInFlightIsNotHostedAgain(t *testing.T) {
	r := newRig(t)
	r.host.Add(5, NewStorage())
	r.write(5, 1, &Round{Entries: entries(1, 1, 1, 100)})
	r.host.Remove(5)
	defer func() {
		if recover() == nil {
			t.Fatal("hosting an owner with a round in flight did not panic")
		}
	}()
	r.host.Add(5, NewStorage())
}

// A round the pool has no buffers for waits, and goes out on the next Durable event once buffers are back.
func TestARoundWaitsForPoolBuffers(t *testing.T) {
	r := newRig(t)
	r.host.Add(5, NewStorage())
	r.write(5, 1, &Round{Entries: entries(1, 1, 1, 100)})
	var fill []core.IOBuf
	for {
		b, err := r.ctx.Pool.Alloc(4 << 10)
		if err != nil {
			break
		}
		fill = append(fill, b)
	}
	r.run()
	r.takeDone()
	r.write(5, 2, &Round{Entries: entries(2, 1, 1, 100)})
	if len(r.host.waiting) != 1 {
		t.Fatal("a round with no pool buffers did not wait")
	}
	for _, b := range fill {
		r.ctx.Pool.Free(b)
	}
	r.host.Add(6, NewStorage())
	r.write(6, 1, &Round{Entries: entries(1, 1, 1, 100)})
	r.run()
	if d := r.takeDone(); len(d) != 2 || d[0] != (Done{Owner: 6, Round: 1}) || d[1] != (Done{Owner: 5, Round: 2}) {
		t.Fatalf("done %v, want owner 6's round then owner 5's waiting one", d)
	}
}

// A chunk write and two shards' rounds submitted together go out in one record, with one write and one flush.
func TestAChunkWriteAndTwoShardsRoundsShareOneRecord(t *testing.T) {
	r := newRig(t)
	r.host.Add(5, NewStorage())
	r.host.Add(6, NewStorage())
	r.host.Add(7, NewStorage())
	fsyncs := r.fsyncs
	r.write(7, 1, &Round{Entries: entries(1, 1, 1, 10)})
	buf, err := r.ctx.Pool.Alloc(core.BlockSize)
	if err != nil {
		t.Fatal(err)
	}
	chunk, err := r.host.Log.Submit(&r.ctx, disklog.Hot, disklog.Item{Entry: disklog.WriteEntry(1, core.ChunkID{Volume: 1}, 1, 0, 1), Buf: buf}, &r.out)
	if err != nil {
		t.Fatal(err)
	}
	r.absorb()
	r.write(5, 1, &Round{HardState: raftpb.HardState{Term: 1, Vote: 5}})
	r.write(6, 1, &Round{Entries: entries(1, 2, 1, 300), HardState: raftpb.HardState{Term: 1, Vote: 6, Commit: 1}})
	r.run()
	if len(r.durable) != 2 || len(r.durable[1]) != 3 || r.durable[1][0] != chunk {
		t.Fatalf("records hold %v, want the chunk write and both rounds in the second", r.durable)
	}
	if r.fsyncs-fsyncs != 2 {
		t.Fatalf("two records took %d flushes", r.fsyncs-fsyncs)
	}
	if d := r.takeDone(); len(d) != 3 || d[1] != (Done{Owner: 5, Round: 1}) || d[2] != (Done{Owner: 6, Round: 1}) {
		t.Fatalf("done %v", d)
	}
}

// After a crash each shard's storage holds what its durable rounds wrote; a round torn at the
// log's tail changes nothing and its blobs are dead, and an owner no shard claims has its blobs dead.
func TestReplayRebuildsEachShardsStorage(t *testing.T) {
	r := newRig(t)
	r.host.Add(5, NewStorage())
	r.host.Add(6, NewStorage())
	r.host.Add(9, NewStorage())
	hs := raftpb.HardState{Term: 2, Vote: 5, Commit: 3}
	r.write(5, 1, &Round{Entries: entries(1, 3, 1, 100), HardState: raftpb.HardState{Term: 1, Vote: 5}})
	r.run()
	r.write(5, 2, &Round{Entries: entries(3, 2, 2, 100), HardState: hs})
	r.write(6, 1, &Round{Snapshot: raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 9, Term: 3, ConfState: raftpb.ConfState{Voters: []uint64{6}}}, Data: []byte("state")}})
	r.write(9, 1, &Round{Entries: entries(1, 1, 1, 100)})
	r.run()
	r.durable, r.done = nil, nil
	r.write(5, 3, &Round{Entries: entries(5, 3, 2, 10<<10), HardState: raftpb.HardState{Term: 2, Vote: 5, Commit: 4}})
	for len(r.durable) == 0 {
		r.step()
	}
	if len(r.done) != 0 {
		t.Fatal("the torn round was done")
	}
	sts := r.crash(5, 6)
	if got, _ := sts[5].LastIndex(); got != 4 || sts[5].HardState() != hs {
		t.Fatalf("owner 5 replayed to last index %d, hard state %+v; want 4 and %+v", got, sts[5].HardState(), hs)
	}
	if term, _ := sts[5].Term(3); term != 2 {
		t.Fatalf("index 3 has term %d after replay, want the later round's 2", term)
	}
	if b := sts[6].Base(); b.Index != 9 || b.Term != 3 {
		t.Fatalf("owner 6's base is %+v, want the snapshot's", b)
	}
	if len(r.host.shard(5).rounds) != 2 || len(r.host.shard(6).rounds) != 1 {
		t.Fatalf("the host holds %d and %d rounds, want 2 and 1", len(r.host.shard(5).rounds), len(r.host.shard(6).rounds))
	}
	r.host.Remove(5)
	r.host.Remove(6)
	if r.live() != 0 {
		t.Fatalf("with every shard removed %d blocks are live: the torn round's or the unclaimed owner's blobs stayed", r.live())
	}
}

// BenchmarkHostRound writes a shard's round of 16 entries of 128 bytes and a hard state through
// the log, completing its write and flush at once, takes it done, and compacts every 64 rounds.
func BenchmarkHostRound(b *testing.B) {
	ctx := core.Context{Now: 1, Raw: 1, Pool: sim.NewPool(4<<20, 0)}
	geo, err := disklog.GeometryFor(1 << 40)
	if err != nil {
		b.Fatal(err)
	}
	cfg := disklog.DefaultConfig()
	cfg.MaxReplayBytes, cfg.NoReserve = 1<<60, true
	var out disklog.Output
	log, err := disklog.Format(&ctx, cfg, disklog.Superblock{LogID: 1, Geometry: geo}, 1, &out)
	if err != nil {
		b.Fatal(err)
	}
	h, st := NewHost(log), NewStorage()
	h.Add(5, st)
	var acts []core.Action
	var done []Done
	complete := func() {
		for len(out.Actions) > 0 {
			acts = append(acts[:0], out.Actions...)
			out.Reset()
			for i := range acts {
				ev := core.Event{Kind: core.EvDiskDone, IO: acts[i].IO}
				log.OnEvent(&ctx, &ev, &out)
			}
			for i := range out.Events {
				if out.Events[i].Kind == disklog.Durable {
					if done, err = h.Durable(&ctx, &out.Events[i], &out, done[:0]); err != nil {
						b.Fatal(err)
					}
				}
			}
		}
		out.Reset()
	}
	complete()
	batch := entries(1, 16, 1, 128)
	var n uint64
	round := func() {
		n++
		for j := range batch {
			batch[j].Index = (n-1)*16 + uint64(j) + 1
		}
		rd := Round{Entries: batch, HardState: raftpb.HardState{Term: 1, Vote: 5, Commit: (n - 1) * 16}}
		if err := h.Write(&ctx, 5, n, &rd, &out); err != nil {
			b.Fatal(err)
		}
		complete()
		if len(done) != 1 {
			b.Fatalf("round %d: done %v", n, done)
		}
		st.Append(batch)
		if n%64 == 0 {
			if err := h.Compact(5, n*16-8, raftpb.ConfState{}); err != nil {
				b.Fatal(err)
			}
		}
	}
	for range 256 {
		round()
	}
	b.ReportAllocs()
	for b.Loop() {
		round()
	}
}

// Regression (seed 5003 of TestCommittedEntriesSurviveCrashesOnTheDiskLog): a round torn at the
// log's tail stays in the log, so the replay after a second crash meets its first blob again, and
// the rounds written after the first restart must not be read as its continuation.
func TestATornRoundIsDroppedAgainAfterALaterCrash(t *testing.T) {
	r := newRig(t)
	r.host.Add(5, NewStorage())
	r.write(5, 1, &Round{Entries: entries(1, 1, 1, 100), HardState: raftpb.HardState{Term: 1, Vote: 5}})
	r.run()
	r.durable, r.done = nil, nil
	r.write(5, 2, &Round{Entries: entries(2, 3, 1, 10<<10)})
	for len(r.durable) == 0 {
		r.step()
	}
	r.crash(5)
	hs := raftpb.HardState{Term: 2, Vote: 5, Commit: 1}
	r.write(5, 1, &Round{Entries: entries(2, 1, 2, 100), HardState: hs})
	r.run()
	sts := r.crash(5)
	if last, _ := sts[5].LastIndex(); last != 2 || sts[5].HardState() != hs {
		t.Fatalf("after the second crash owner 5 holds up to %d with hard state %+v, want 2 and %+v", last, sts[5].HardState(), hs)
	}
}
