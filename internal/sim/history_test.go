package sim_test

import (
	"slices"
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
	h.Replies(func(a *core.Action, _ []byte) (int, [sim.OpWords]uint64, bool) {
		b := a.Msg.Inline()
		if len(b) != 2 || b[0] != 'r' {
			return 0, [sim.OpWords]uint64{}, false
		}
		return int(b[1]), [sim.OpWords]uint64{uint64(b[1]) + 10}, true
	})
	s.AddNode(sim.DefaultClock, nil, func(n *sim.Node) {
		n.Start(sim.MachineFunc(func(_ *core.Context, _ *core.Event, out []core.Action) []core.Action { return out }), nil)
	})
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

// Events from several clients, interleaved in the file as their processes wrote them, read back as
// the operations they record, ordered by invocation; one with no completion never completed.
func TestAHistoryFileReadsBackAsItsOperations(t *testing.T) {
	var file []byte
	put := func(complete bool, client uint32, op uint64, at core.Tick, w uint64) {
		var e [sim.EventBytes]byte
		sim.PutEvent(&e, complete, client, op, at, [sim.OpWords]uint64{w, 0, 0, w + 1})
		file = append(file, e[:]...)
	}
	put(false, 2, 1, 30, 5)
	put(false, 1, 1, 10, 7)
	put(true, 2, 1, 40, 6)
	put(false, 1, 2, 50, 8)
	h, err := sim.ReadHistory(file)
	if err != nil {
		t.Fatal(err)
	}
	want := []sim.Op{
		{Client: 1, Invoke: 10, Input: [sim.OpWords]uint64{7, 0, 0, 8}},
		{Client: 2, Invoke: 30, Complete: 40, Done: true, Input: [sim.OpWords]uint64{5, 0, 0, 6}, Output: [sim.OpWords]uint64{6, 0, 0, 7}},
		{Client: 1, Invoke: 50, Input: [sim.OpWords]uint64{8, 0, 0, 9}},
	}
	if !slices.Equal(h.Ops(), want) {
		t.Fatalf("%+v", h.Ops())
	}
	for _, bad := range [][]byte{file[:len(file)-1], append(slices.Clone(file), file[:sim.EventBytes]...), file[2*sim.EventBytes : 3*sim.EventBytes]} {
		if _, err := sim.ReadHistory(bad); err == nil {
			t.Errorf("read %d bytes without an error", len(bad))
		}
	}
}

func FuzzReadHistory(f *testing.F) {
	var e [sim.EventBytes]byte
	sim.PutEvent(&e, false, 1, 1, 10, [sim.OpWords]uint64{1})
	f.Add(e[:])
	f.Fuzz(func(t *testing.T, b []byte) {
		h, err := sim.ReadHistory(b)
		if err != nil {
			return
		}
		var file []byte
		for i, op := range h.Ops() {
			sim.PutEvent(&e, false, op.Client, uint64(i), op.Invoke, op.Input)
			file = append(file, e[:]...)
			if op.Done {
				sim.PutEvent(&e, true, op.Client, uint64(i), op.Complete, op.Output)
				file = append(file, e[:]...)
			}
		}
		again, err := sim.ReadHistory(file)
		if err != nil || !slices.Equal(again.Ops(), h.Ops()) {
			t.Fatalf("%+v read back as %+v, %v", h.Ops(), again, err)
		}
	})
}
