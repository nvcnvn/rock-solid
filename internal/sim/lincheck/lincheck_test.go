package lincheck_test

import (
	"math"
	"testing"

	"github.com/anishathalye/porcupine"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/lincheck"
)

const (
	opWrite = iota + 1
	opRead
)

// register is a register per key, Input {kind, key, value} and Output {value read}.
var register = porcupine.Model{
	Partition: func(h []porcupine.Operation) [][]porcupine.Operation {
		var keys []uint64
		var parts [][]porcupine.Operation
		for _, o := range h {
			k := o.Input.([sim.OpWords]uint64)[1]
			i := 0
			for i < len(keys) && keys[i] != k {
				i++
			}
			if i == len(keys) {
				keys = append(keys, k)
				parts = append(parts, nil)
			}
			parts[i] = append(parts[i], o)
		}
		return parts
	},
	Init: func() any { return uint64(0) },
	Step: func(state, input, output any) (bool, any) {
		in, out, v := input.([sim.OpWords]uint64), output.(lincheck.Result), state.(uint64)
		switch in[0] {
		case opWrite:
			return true, in[2]
		case opRead:
			return !out.Done || out.Output[0] == v, v
		}
		return false, v
	},
}

// timed is an operation of a scripted history, invoked at at and completed at done, or never if done is 0.
type timed struct {
	client   uint32
	in       [sim.OpWords]uint64
	out      uint64
	at, done core.Tick
}

func scripted(t *testing.T, ops []timed) *sim.History {
	t.Helper()
	set := sim.DefaultSettings()
	set.StaleDen = 0
	s := sim.New(1, set)
	clock := s.AddNode(sim.DefaultClock, nil, func(n *sim.Node) {
		n.Start(sim.MachineFunc(func(_ *core.Context, _ *core.Event, out []core.Action) []core.Action { return out }), nil)
	})
	h := sim.NewHistory(s, len(ops))
	type edge struct {
		at     core.Tick
		op     int
		finish bool
	}
	var edges []edge
	for i, o := range ops {
		edges = append(edges, edge{o.at, i, false})
		if o.done != 0 {
			edges = append(edges, edge{o.done, i, true})
		}
	}
	idx := make([]int, len(ops))
	for len(edges) > 0 {
		next := 0
		for i, e := range edges {
			if e.at < edges[next].at {
				next = i
			}
		}
		e := edges[next]
		edges = append(edges[:next], edges[next+1:]...)
		if e.at > s.Time() {
			clock.Call(core.NodeID{}, func(ctx *core.Context, out []core.Action) []core.Action {
				return append(out, core.Action{Kind: core.ActSetTimer, Timer: 1, At: ctx.Now + e.at - s.Time()})
			})
			s.Step()
		}
		if s.Time() != e.at {
			t.Fatalf("scheduler at %d, want %d", s.Time(), e.at)
		}
		if e.finish {
			h.Complete(idx[e.op], [sim.OpWords]uint64{ops[e.op].out})
		} else {
			idx[e.op] = h.Invoke(ops[e.op].client, ops[e.op].in)
		}
	}
	return h
}

func regWrite(key, v uint64, at, done core.Tick) timed {
	return timed{in: [sim.OpWords]uint64{opWrite, key, v}, at: at, done: done}
}

func regRead(key, v uint64, at, done core.Tick) timed {
	return timed{in: [sim.OpWords]uint64{opRead, key}, out: v, at: at, done: done}
}

func TestPorcupineAcceptsALinearizableRegisterHistory(t *testing.T) {
	const a, b = 10, 11
	h := scripted(t, []timed{regWrite(7, a, 10, 20), regRead(7, a, 15, 30), regWrite(7, b, 25, 40), regRead(7, b, 45, 50)})
	if i, bad := lincheck.Illegal(register, h); bad {
		t.Fatalf("op %d named in a linearizable history", i)
	}
}

func TestPorcupineRejectsAStaleReadAndNamesIt(t *testing.T) {
	const a, b = 10, 11
	h := scripted(t, []timed{regWrite(7, a, 10, 20), regWrite(7, b, 25, 40), regRead(7, a, 45, 50), regWrite(8, a, 10, 20), regRead(8, a, 45, 50)})
	i, bad := lincheck.Illegal(register, h)
	if !bad {
		t.Fatal("a read of A after B was written was accepted")
	}
	if op := h.Ops()[i]; op.Input[0] != opRead || op.Input[1] != 7 || op.Output[0] != a {
		t.Fatalf("named op %d %+v, want read A", i, op)
	}
}

func TestAnIncompleteWriteMayTakeEffectOrNot(t *testing.T) {
	const a, b = 10, 11
	for _, seen := range []uint64{a, b} {
		h := scripted(t, []timed{regWrite(7, a, 10, 20), regWrite(7, b, 25, 0), regRead(7, seen, 45, 50)})
		if i, bad := lincheck.Illegal(register, h); bad {
			t.Fatalf("reading %d after an incomplete write: op %d named", seen, i)
		}
	}
	h := scripted(t, []timed{regWrite(7, a, 10, 20), regWrite(7, b, 25, 0), regRead(7, b, 45, 50), regRead(7, a, 55, 60)})
	if _, bad := lincheck.Illegal(register, h); !bad {
		t.Fatal("an incomplete write was undone after a read saw it")
	}
}

func TestPipelinedOperationsOfOneClientOverlap(t *testing.T) {
	const a, b = 10, 11
	h := scripted(t, []timed{regWrite(7, a, 10, 30), regWrite(7, b, 15, 25), regRead(7, a, 40, 50)})
	if i, bad := lincheck.Illegal(register, h); bad {
		t.Fatalf("op %d named; B may take effect before A", i)
	}
}

func TestOperationsRoundTrip(t *testing.T) {
	ops := []timed{regWrite(7, 10, 10, 20), regRead(7, 10, 15, 0), regRead(9, 0, 30, 31)}
	ops[1].client, ops[2].client = 1, 2
	h := scripted(t, ops)
	conv := lincheck.Operations(h)
	if len(conv) != 3 || conv[1].Return != math.MaxInt64 || conv[0].Call != 10 || conv[0].Return != 20 || conv[2].ClientId != 2 {
		t.Fatalf("%+v", conv)
	}
	for i, o := range conv {
		if o.Metadata.(int) != i || lincheck.OpOf(o) != h.Ops()[i] {
			t.Fatalf("op %d: %+v back as %+v", i, h.Ops()[i], lincheck.OpOf(o))
		}
	}
}

func TestTheNamedOperationIsTheSameEveryRun(t *testing.T) {
	h := scripted(t, []timed{regWrite(7, 1, 1, 5), regWrite(7, 2, 1, 5), regRead(7, 1, 10, 20), regRead(7, 2, 10, 21)})
	first, bad := lincheck.Illegal(register, h)
	if !bad {
		t.Fatal("two reads of different values after both writes were accepted")
	}
	for range 100 {
		if i, _ := lincheck.Illegal(register, h); i != first {
			t.Fatalf("named op %d, then op %d", first, i)
		}
	}
	if first != 3 {
		t.Fatalf("named op %d, want 3, which the order of the smallest indices leaves out", first)
	}
}

func TestOfTwoUnplaceableOperationsTheFirstToReturnIsNamed(t *testing.T) {
	const a, b, c = 10, 11, 12
	h := scripted(t, []timed{regWrite(7, a, 10, 20), regWrite(7, b, 25, 30), regRead(7, c, 40, 60), regRead(7, c, 41, 50)})
	if i, bad := lincheck.Illegal(register, h); !bad || i != 3 {
		t.Fatalf("named op %d (illegal %v), want 3, the read of a value never written that returns first", i, bad)
	}
}
