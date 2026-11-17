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
// manifest, on a small disk, its state checked at every index across crashes.
func compactingRun(t *testing.T) *hostRun {
	lc := ctlLogConfig()
	lc.MaxReplayBytes = 64 << 10
	r := newHostGroupOnDisks(t, 1, lc, smallDisk)
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

// crashRound asks n Txns that add to the counter, counting those answered, then hands one more
// and crashes once its disk actions have run steps at a time while a checkpoint runs, or mid-round.
func (r *hostRun) crashRound(n, steps int, midCheckpoint bool, token *uint64) (answered uint64, written uint64) {
	r.t.Helper()
	add := &Txn{Ops: []Op{Update(counterKey, 0, Add, 1)}}
	for range n {
		*token++
		if a := r.ask(0, *token, add); a != nil && a.Err == nil {
			answered++
		}
	}
	for range 200 {
		*token++
		var m core.Msg
		if err := EncodeTxn(r.pool, hostGroup, *token, add, &m); err != nil {
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
	return answered, r.h.disk.Log.Writes().Disk
}

// The control-plane host checkpoints its group's tree once max_replay_bytes of records follow the
// last manifest, compacts the log to the index the runs hold, and frees the segments left with
// nothing live, so its Txns fill the disk several times over. Each crash, while a checkpoint runs
// or mid-round, landing the unflushed writes or not, restarts it from its manifest's runs, the
// base and applied index they carry, and the log after its replay LSN, the blobs it reported dead
// gone with the segments reused: its state at every index equals what it was before (MetaStore
// invariants), and the counter holds every increment answered.
func TestAHostCompactsAndRestartsFromItsManifest(t *testing.T) {
	r := compactingRun(t)
	var token, want, written uint64
	compacted := false
	for crash := range 16 {
		answered, w := r.crashRound(40, crash%5, crash%3 != 0, &token)
		want += answered
		written += w
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
	r := compactingRun(t)
	r.plant = func(h *CtlHost) { h.cp.plantAppliedIsBase = true }
	var token uint64
	for range 4 {
		r.crashRound(40, 0, false, &token)
		r.restart(0)
		r.lead()
		if r.diverged > 0 {
			return
		}
	}
	t.Fatal("a restore that reapplies entries kept every state check")
}
