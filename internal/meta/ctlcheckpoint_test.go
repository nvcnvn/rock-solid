package meta

import (
	"encoding/binary"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// smallDisk is a disk of 16 segments of 256 KiB, which the crash run's Txns fill several times over.
var smallDisk = disklog.Geometry{SegmentBlocks: 64, SegmentCount: 16}

var counterKey = attr(1, 2, 1)

// compactingRun is a single voter whose log checkpoints once 64 KiB of records follow the last
// manifest, on a small disk, its tree merging in steps of stepBytes, its state checked at every
// index across crashes.
func compactingRun(t *testing.T, stepBytes int) *hostRun {
	lc := ctlLogConfig()
	lc.MaxReplayBytes = 64 << 10
	r := newHostGroupOnDisks(t, 1, lc, smallDisk, stepBytes)
	r.states = []map[uint64][]byte{{}}
	r.lead()
	return r
}

// count is the counter the run's Txns add to.
func (r *hostRun) count() uint64 {
	v, ok := r.h.Shard().State().Tree().Get(counterKey)
	if !ok {
		return 0
	}
	return binary.BigEndian.Uint64(v)
}

// crashRound asks n Txns next makes, calling answered after each one answered, then hands one
// more and crashes once its disk actions have run steps at a time while a checkpoint runs, or
// mid-round. It returns the blocks the log wrote.
func (r *hostRun) crashRound(n, steps int, midCheckpoint bool, token *uint64, next func() *Txn, answered func()) uint64 {
	r.t.Helper()
	for range n {
		*token++
		if a := r.ask(0, *token, next()); a != nil && a.Err == nil {
			answered()
		}
	}
	for range 200 {
		*token++
		var m core.Msg
		if err := EncodeTxn(r.pool, hostGroup, *token, next(), &m); err != nil {
			r.t.Fatal(err)
		}
		r.do(0, func(ctx *core.Context, out *CtlHostOutput) {
			r.h.OnEvent(ctx, &core.Event{Kind: core.EvNetRecv, Peer: hostNode, Msg: m}, out)
		})
		if !midCheckpoint || r.h.cp.running {
			break
		}
		r.drain()
	}
	for range steps {
		if len(r.work) == 0 {
			break
		}
		w := r.work[0]
		r.work = r.work[1:]
		ev := r.disks[w.at].Execute(r.pool, &w.a)
		r.do(w.at, func(ctx *core.Context, out *CtlHostOutput) { r.hs[w.at].OnEvent(ctx, &ev, out) })
	}
	return r.h.disk.Log.Writes().Disk
}

// addOne is the Txn that adds one to the counter.
func addOne() *Txn { return &Txn{Ops: []Op{Update(counterKey, 0, Add, 1)}} }

// The control-plane host checkpoints its group's tree once max_replay_bytes of records follow the
// last manifest, compacts the log to the index the runs hold, and frees the segments left with
// nothing live, so its Txns fill the disk several times over. Each crash, while a checkpoint runs
// or mid-round, landing the unflushed writes or not, restarts it from its manifest's runs, the
// base and applied index they carry, and the log after its replay LSN, the blobs it reported dead
// gone with the segments reused: its state at every index equals what it was before (MetaStore
// invariants), and the counter holds every increment answered.
func TestAHostCompactsAndRestartsFromItsManifest(t *testing.T) {
	r := compactingRun(t, 0)
	var token, want, written uint64
	compacted := false
	for crash := range 16 {
		written += r.crashRound(40, crash%5, crash%3 != 0, &token, addOne, func() { want++ })
		compacted = compacted || r.h.Shard().log.Base().Index > 1
		r.keep = crash%2 == 1
		r.restart(0)
		r.lead()
		got := r.count()
		if got < want {
			t.Fatalf("crash %d: the counter is %d after a restart, %d increments were answered", crash, got, want)
		}
		want = got
	}
	if !compacted || written < 2*smallDisk.DiskBlocks() {
		t.Fatalf("the log compacted %v and wrote %d blocks on a disk of %d", compacted, written, smallDisk.DiskBlocks())
	}
}

// Planted: a restore from runs that takes the base they carry for the index applied hands the
// entries after the base to the state again, and the state check of the crash run fires.
func TestARestoreThatReappliesEntriesBreaksTheStateCheck(t *testing.T) {
	r := compactingRun(t, 0)
	r.plant = func(h *CtlHost) { h.cp.plantAppliedIsBase = true }
	var token uint64
	for range 4 {
		r.crashRound(40, 0, false, &token, addOne, func() {})
		r.restart(0)
		r.lead()
		if r.diverged > 0 {
			return
		}
	}
	t.Fatal("a restore that reapplies entries kept every state check")
}

// bigTreeRun crashes a single voter 24 times, at a different step each time, while its Txns each
// put a key of 900 bytes and delete the key put 200 Txns before, so that its tree holds about 200
// live keys in runs of several pieces, merged in steps of stepBytes, or whole for 0. No answered
// delete comes back after a restart.
func bigTreeRun(t *testing.T, stepBytes int) {
	r := compactingRun(t, stepBytes)
	val := make([]byte, 900)
	var token, seq uint64
	gone := map[uint64]bool{}
	next := func() *Txn {
		seq++
		ops := []Op{Put(attr(1, 2, seq), val)}
		if seq > 200 {
			ops = append(ops, Delete(attr(1, 2, seq-200)))
		}
		return &Txn{Ops: ops}
	}
	answered := func() {
		if seq > 200 {
			gone[seq-200] = true
		}
	}
	for crash := range 24 {
		r.crashRound(40, crash%9, crash%3 != 0, &token, next, answered)
		r.keep = crash%2 == 1
		r.restart(0)
		r.lead()
		tree := r.h.Shard().State().Tree()
		for k := range seq {
			if _, ok := tree.Get(attr(1, 2, k+1)); ok && gone[k+1] {
				t.Fatalf("crash %d: key %d, deleted by an answered Txn, came back", crash, k+1)
			}
		}
	}
}

// A tree whose merges take several steps of 8 KiB takes each step at a checkpoint until the merge
// ends, so its runs stay few and the log keeps room across crashes.
func TestAHostMergesItsTreeInStepsAcrossCrashes(t *testing.T) { bigTreeRun(t, 8<<10) }
