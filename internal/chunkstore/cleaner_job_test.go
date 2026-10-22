package chunkstore

import (
	"errors"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/lsm"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// heldJob runs the cleaner of a store primed with cfg until it hands out a job, which the harness
// holds, and returns it with the victim it copies.
func heldJob(t *testing.T, cfg Config) (*harness, *byteOracle, *CleanJob, uint32) {
	h, o := primedWith(t, cfg)
	h.hold = true
	h.s.cfg.CleanFree = 100
	h.readNow(chunk(1), 0, 1)
	if len(h.held) == 0 {
		t.Fatal("the cleaner handed out no job")
	}
	j := h.held[0]
	return h, o, j, victimOf(t, h, j)
}

// victimOf is the victim the store's pass holding job j copies.
func victimOf(t *testing.T, h *harness, j *CleanJob) uint32 {
	t.Helper()
	for _, p := range h.s.cl.passes {
		if &p.job.CleanJob == j {
			return p.job.segment
		}
	}
	t.Fatal("no pass holds the job")
	return 0
}

func bufsOf(j *CleanJob) int {
	n := 0
	for _, pc := range j.pieces {
		if pc.buf != core.NoBuf {
			n++
		}
	}
	return n
}

// dropLate hands the store job j after the store dropped its pass: the store must take no action
// and free exactly the job's buffers.
func dropLate(t *testing.T, h *harness, j *CleanJob) {
	t.Helper()
	live, n := h.pool.Live(), bufsOf(j)
	if n == 0 {
		t.Fatal("the late job holds no buffer")
	}
	h.held = slices.DeleteFunc(h.held, func(x *CleanJob) bool { return x == j })
	j.Run(h.pool)
	h.s.InstallClean(&h.ctx, j, &h.out)
	if len(h.out.Actions) != 0 || h.pool.Live() != live-n || bufsOf(j) != 0 {
		t.Fatalf("a late job: %d actions, %d of its %d buffers freed", len(h.out.Actions), live-h.pool.Live(), n)
	}
}

// The store hands a victim's checksum work out as a job and does none of it on its own loop: it
// copies nothing while the job is out, and a job handed back without its Run copies a block
// damaged in its buffer unchecked, under the checksum its source record stored, by neither the
// store nor the log, so the block reads as damaged at its new place. The log's test-only decode
// check, which would catch the bad checksum as the record goes out, is off.
func TestTheOwnerChecksNoBlockItCopies(t *testing.T) {
	cfg := cleanConfig()
	cfg.Log.Check = false
	h, _, j, _ := heldJob(t, cfg)
	if st := h.s.CleanStats(); st.Copied != 0 {
		t.Fatalf("the store copied with its job out: %+v", st)
	}
	pc := j.pieces[0]
	h.pool.Bytes(pc.buf)[100] ^= 0xff
	h.held = h.held[1:]
	h.s.InstallClean(&h.ctx, j, &h.out)
	h.take()
	h.settle()
	if st := h.s.CleanStats(); st.Quarantined != 0 || st.Copied == 0 {
		t.Fatalf("stats %+v", st)
	}
	if e, ok := h.s.Index().Lookup(testPool, pc.chunk, pc.first); !ok || e.BlockLBA(pc.first) == pc.src {
		t.Fatalf("block %d of %v did not move from %d", pc.first, pc.chunk, pc.src)
	}
	id := h.read(pc.chunk, uint32(pc.first)*core.BlockSize, 1)
	h.settle()
	if err := h.reply(id).Err; !errors.Is(err, disklog.ErrBadChecksum) {
		t.Fatalf("read of the block damaged after its check: %v", err)
	}
}

// A job whose result comes back while the store rebuilds its index is dropped with its buffers,
// and the rebuilt store cleans and reads as before.
func TestALateCleanJobDuringARebuildIsDropped(t *testing.T) {
	h, o, j, _ := heldJob(t, cleanConfig())
	h.s.rebuilt = false
	h.s.indexBroken(&h.ctx, lsm.ErrCheck, &h.out)
	h.s.pump(&h.ctx, &h.out)
	h.take()
	for (h.s.phase == serving || h.s.phase == draining) && h.step() {
	}
	dropLate(t, h, j)
	h.hold = false
	h.settle()
	if !h.s.Serving() || h.s.CleanStats().Freed == 0 {
		t.Fatalf("serving %v, stats %+v: %v", h.s.Serving(), h.s.CleanStats(), h.s.Err())
	}
	if held := h.pool.Live(); held != h.own {
		t.Fatalf("the store holds %d pool buffers, want its log's %d", held, h.own)
	}
	o.check(t, h, 0, 63, 0)
}

// A job from before a rebuild that comes back once the rebuilt store's cleaner has handed out a
// job for the same victim is dropped with its buffers, leaving the new job to copy it.
func TestALateCleanJobForAVictimTakenAgainIsDropped(t *testing.T) {
	h, o, j, victim := heldJob(t, cleanConfig())
	old := slices.Clone(h.held)
	h.s.rebuilt = false
	h.s.indexBroken(&h.ctx, lsm.ErrCheck, &h.out)
	h.s.pump(&h.ctx, &h.out)
	h.take()
	h.settle()
	h.readNow(chunk(1), 0, 1)
	k := slices.IndexFunc(h.held, func(x *CleanJob) bool { return !slices.Contains(old, x) })
	if !h.s.Serving() || k < 0 || victimOf(t, h, h.held[k]) != victim {
		t.Fatalf("serving %v; the rebuilt cleaner holds no job for victim %d", h.s.Serving(), victim)
	}
	again := h.held[k]
	bufs := bufsOf(again)
	dropLate(t, h, j)
	if bufsOf(again) != bufs || h.s.CleanDue() != nil {
		t.Fatal("the late job touched the new one")
	}
	h.hold = false
	h.settle()
	if st := h.s.CleanStats(); st.Copied == 0 {
		t.Fatalf("the new job copied nothing: %+v", st)
	}
	if held := h.pool.Live(); held != h.own {
		t.Fatalf("the store holds %d pool buffers, want its log's %d", held, h.own)
	}
	o.check(t, h, 0, 63, 0)
}

// A job whose result comes back after a failed disk stopped the store is dropped with its buffers.
func TestALateCleanJobAfterAStopIsDropped(t *testing.T) {
	h, _, j, _ := heldJob(t, cleanConfig())
	h.disk.Inject(sim.Fault{Effect: sim.FailIO, When: sim.OnKind(core.ActDiskFsync)})
	h.write(chunk(1), 0, fill(core.BlockSize, 1), 0)
	h.settle()
	if h.s.Err() == nil {
		t.Fatal("the store serves on a failed disk")
	}
	dropLate(t, h, j)
}
