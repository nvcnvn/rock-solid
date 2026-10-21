package sim_test

import (
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

func TestTheHistoryUsesTheSchedulersTimeNotANodesClock(t *testing.T) {
	s := sim.New(1, sim.DefaultSettings())
	h := sim.NewHistory(s, 1)
	s.AddNode(sim.Clock{Now: 5000, Raw: 9000}, nil, func(n *sim.Node) {
		n.Start(sim.MachineFunc(func(ctx *core.Context, _ *core.Event, out []core.Action) []core.Action {
			h.Complete(0, [sim.OpWords]uint64{uint64(ctx.Now)})
			return out
		}), nil)
	})
	i := h.Invoke(3, [sim.OpWords]uint64{1})
	s.Node(0).Call(core.NodeID{}, func(ctx *core.Context, out []core.Action) []core.Action {
		return append(out, core.Action{Kind: core.ActSetTimer, Timer: 1, At: ctx.Now + 70})
	})
	s.Run(1)
	op := h.Ops()[i]
	if op.Invoke != 0 || op.Complete != 70 || op.Output[0] != 5070 || op.Client != 3 {
		t.Fatalf("%+v", op)
	}
}

func TestACompletionTwiceIsABrokenInvariant(t *testing.T) {
	h := sim.NewHistory(sim.New(1, sim.DefaultSettings()), 1)
	i := h.Invoke(0, [sim.OpWords]uint64{})
	h.Complete(i, [sim.OpWords]uint64{})
	defer func() {
		if recover() == nil {
			t.Fatal("no panic")
		}
	}()
	h.Complete(i, [sim.OpWords]uint64{})
}

func BenchmarkHistoryRecord(b *testing.B) {
	b.ReportAllocs()
	h := sim.NewHistory(sim.New(1, sim.DefaultSettings()), b.N)
	in := [sim.OpWords]uint64{1, 7, 10}
	b.ResetTimer()
	for range b.N {
		h.Complete(h.Invoke(0, in), in)
	}
}
