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

// A machine's client reply completes its operation from the send itself, whichever machine sends it; a send not marked Reply, or one the decoder does not name, completes nothing.
func TestRepliesAMachineSendsCompleteTheHistory(t *testing.T) {
	s := sim.New(1, sim.DefaultSettings())
	h := sim.NewHistory(s, 2)
	h.Replies(func(m *core.Msg) (int, [sim.OpWords]uint64, bool) {
		b := m.Inline()
		if len(b) != 2 || b[0] != 'r' {
			return 0, [sim.OpWords]uint64{}, false
		}
		return int(b[1]), [sim.OpWords]uint64{uint64(b[1]) + 10}, true
	})
	s.AddNode(sim.DefaultClock, nil, func(n *sim.Node) { n.Start(sim.MachineFunc(func(_ *core.Context, _ *core.Event, out []core.Action) []core.Action { return out }), nil) })
	a, b := h.Invoke(0, [sim.OpWords]uint64{}), h.Invoke(1, [sim.OpWords]uint64{})
	send := func(body string, reply bool) {
		s.Node(0).Call(core.NodeID{}, func(_ *core.Context, out []core.Action) []core.Action {
			act := core.Action{Kind: core.ActNetSend, Peer: core.NodeID{Node: 7}, Reply: reply}
			act.Msg.Len = uint16(copy(act.Msg.Body[:], body))
			return append(out, act)
		})
	}
	send("r\x01", false)
	send("x\x00", true)
	send("r\x01", true)
	ops := h.Ops()
	if ops[a].Done || !ops[b].Done || ops[b].Output[0] != 11 {
		t.Fatalf("%+v", ops)
	}
}
