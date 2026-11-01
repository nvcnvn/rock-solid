package chunkstore

import (
	"bytes"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/anishathalye/porcupine"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/lincheck"
)

// scripted is an operation of a history written by hand on one chunk, with its call and return, 0
// for none, and its output.
type scripted struct {
	histOp
	call, ret int64
	out       [sim.OpWords]uint64
}

func linearizable(ops []scripted) bool {
	x := &chunkHist{chunks: []core.ChunkID{{}}, activated: map[PGInterval]bool{}, decided: map[chunkInterval]uint64{}}
	var recorded []sim.Op
	var pops []porcupine.Operation
	for i, o := range ops {
		x.ops = append(x.ops, o.histOp)
		in := [sim.OpWords]uint64{o.kind, 0, uint64(o.off)<<32 | uint64(o.n), uint64(i)}
		recorded = append(recorded, sim.Op{Invoke: core.Tick(o.call), Complete: core.Tick(o.ret), Done: o.ret != 0, Input: in, Output: o.out})
		ret := int64(math.MaxInt64)
		if o.ret != 0 {
			ret = o.ret
		}
		pops = append(pops, porcupine.Operation{Input: in, Call: o.call, Output: lincheck.Result{Done: o.ret != 0, Output: o.out}, Return: ret, Metadata: i})
	}
	x.prepare(recorded)
	return porcupine.CheckOperations(x.model(), pops)
}

// write is a write of data at off called at call and answered OK as ver at ret, or, with ret 0,
// ordered as ver and never answered.
func write(off uint32, data string, call, ret int64, ver uint64) scripted {
	s := scripted{histOp: histOp{kind: histWrite, off: off, n: uint32(len(data)), data: []byte(data), version: ver}, call: call, ret: ret}
	if ret != 0 {
		s.out = [sim.OpWords]uint64{ver, uint64(StatusOK)}
	}
	return s
}

func punch(off, n uint32, call, ret int64, ver uint64) scripted {
	s := write(off, "", call, ret, ver)
	s.kind, s.n = histPunch, n
	return s
}

func read(off uint32, data string, call, ret int64, ver uint64) scripted {
	return scripted{histOp: histOp{kind: histRead, off: off, n: uint32(len(data))}, call: call, ret: ret, out: [sim.OpWords]uint64{ver, uint64(StatusOK), hashOf([]byte(data))}}
}

func expecting(s scripted, ver uint64) scripted {
	s.expect, s.hasExpect = ver, true
	return s
}

func readAt(s scripted, ver uint64) scripted {
	s.at, s.hasAt = ver, true
	return s
}

// The chunk model of the consistency check: each history is linearizable as written and not with
// the operation that follows it.
func TestTheChunkModel(t *testing.T) {
	v1, v2, v3 := MakeVersion(1, 1), MakeVersion(1, 2), MakeVersion(1, 3)
	for _, c := range []struct {
		name    string
		legal   []scripted
		illegal scripted
		why     string
	}{
		{"a write then a read", []scripted{write(10, "ab", 1, 2, v1), read(9, "\x00ab\x00", 3, 4, v1)},
			read(9, "\x00a\x00\x00", 5, 6, v1), "a read missing a byte the write gave"},
		{"a punch", []scripted{write(0, "abcd", 1, 2, v1), punch(1, 2, 3, 4, v2), read(0, "a\x00\x00d", 5, 6, v2)},
			read(0, "abcd", 7, 8, v1), "a read of the version the punch replaced"},
		{"a write expecting the chunk's version", []scripted{write(0, "a", 1, 2, v1), expecting(write(0, "b", 3, 4, v2), v1)},
			expecting(write(0, "c", 5, 6, v3), v1), "a write expecting a version the chunk passed, answered OK"},
		{"a read at a version", []scripted{write(0, "a", 1, 2, v1), write(0, "b", 3, 4, v2), readAt(read(0, "a", 5, 6, 0), v1)},
			readAt(read(0, "b", 7, 8, 0), v1), "a read at a version returning a later version's bytes"},
		{"an unknown write seen", []scripted{write(0, "a", 1, 2, v1), write(0, "b", 3, 0, v2), read(0, "b", 5, 6, v2)},
			read(0, "a", 7, 8, v1), "a later read undoing the unknown write it saw"},
		{"an unknown write never seen", []scripted{write(0, "a", 1, 2, v1), write(0, "b", 3, 0, v2), read(0, "a", 5, 6, v1), write(0, "c", 7, 8, MakeVersion(2, 1))},
			read(0, "b", 9, 10, v2), "a read of the unknown write after a later interval's write"},
		{"a punch of an absent chunk", []scripted{punch(0, 4, 1, 2, 0), write(0, "a", 3, 4, v1)},
			punch(0, 4, 5, 6, 0), "an unordered punch after a write"},
		{"versions without a gap", []scripted{write(0, "a", 1, 2, v1), write(0, "b", 3, 4, v2), write(0, "c", 5, 6, MakeVersion(2, 1))},
			write(0, "d", 7, 8, MakeVersion(2, 3)), "a version skipping 2.2"},
	} {
		if !linearizable(c.legal) {
			t.Errorf("%s: rejected", c.name)
		}
		if linearizable(append(slices.Clone(c.legal), c.illegal)) {
			t.Errorf("%s: accepted %s", c.name, c.why)
		}
	}
}

// Each planted bug breaks the check of chunk histories, checked alone, at the first seed that fails
// it: a primary committing a version once its own log holds it, whose reads return a pending version
// the next interval drops (80 of 100); an old primary serving reads with no lease check (P1, 9 of
// 100), which a client that missed the new primary's table reads from after the new one
// acknowledged a write; merging on the committed version (sub-block spike M2, 100 of 100), which
// loses bytes a read back sees; and peering dropping a subset of a chunk's pending versions rather
// than a suffix (M7, 5 of 500), which leaves a member with other bytes that a read sees once that
// member leads.
func TestPlantedBugsBreakChunkHistories(t *testing.T) {
	for _, p := range []struct {
		name string
		seed uint64
		run  func(seed uint64) error
	}{
		{"commit on the primary's own log", 0, func(seed uint64) error {
			return runReads(t, seed, &readPlant{inv: invLinearizable, set: func(r *readRun) { r.d.replicaPlants.commitOnOwnLog = true }})
		}},
		{"no lease check", 8, func(seed uint64) error {
			return runReads(t, seed, &readPlant{inv: invLinearizable, set: func(r *readRun) { r.d.replicaPlants.noLeaseCheck = true }})
		}},
		{"M2", 0, func(seed uint64) error {
			return replicatedSubBlocks{clients: 1, pipeline: 4, writes: 8, maxLen: 300, span: core.BlockSize, mergeCommitted: true}.run(t, seed)
		}},
		{"M7", 169, func(seed uint64) error {
			return runPrimaries(t, seed, &peerPlant{invLinearizable, func(r *primaryRun) { r.c.storePlants.settleSubset = true }})
		}},
	} {
		if err := p.run(p.seed); err == nil || !strings.Contains(err.Error(), invLinearizable) {
			t.Errorf("%s at seed %d: %v", p.name, p.seed, err)
		}
	}
}

// A planted resend after a lost acknowledgment, with another client's write between: client A's
// write of X commits as 1.1 and its reply is lost, a read sees X, client B writes Y as 1.2, a read
// sees Y, A sends X again, which commits as 1.3, and a read sees X. With each attempt its own write,
// as the recorder logs them, the history is linearizable; with the two attempts one write answered
// 1.3, no write takes the 1.1 the first read returned, so no order fits.
func TestAResendAfterALostAcknowledgmentIsASecondWrite(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	x := c.record()
	ch := chunkIn(pg0, 4, 0)
	xs, ys := bytes.Repeat([]byte{'x'}, 100), bytes.Repeat([]byte{'y'}, 100)
	lost := c.nextID + 1
	c.drop = func(_ int, a *core.Action) bool {
		var rp Response
		return a.Reply && DecodeResponse(a.Msg.Inline(), 0, &rp) == nil && rp.Kind == KindReply && rp.ID == lost
	}
	var attempts []core.RequestID
	for _, data := range [][]byte{xs, ys, xs} {
		id := c.write(pg0, ch, 0, data)
		if !bytes.Equal(data, ys) {
			attempts = append(attempts, id)
		}
		c.settle()
		if got := c.readNow(pg0, ch, 0, 100); !bytes.Equal(got, data) {
			t.Fatalf("read %q after writing %q", got[:1], data[:1])
		}
	}
	if _, ok := c.cl.replies[lost]; ok || c.reply(attempts[1]).Version != MakeVersion(1, 3) {
		t.Fatalf("the first attempt answered, or the second answered %+v", c.reply(attempts[1]))
	}
	if err := x.check(); err != nil {
		t.Fatalf("each attempt its own write: %v", err)
	}
	ops := lincheck.Operations(x.h)
	first, again := x.byID[attempts[0]], x.byID[attempts[1]]
	ops[first].Return, ops[first].Output = ops[again].Return, ops[again].Output
	ops = slices.Delete(ops, again, again+1)
	if porcupine.CheckOperations(x.model(), ops) {
		t.Fatal("the two attempts as one write: a linearizable order found")
	}
}
