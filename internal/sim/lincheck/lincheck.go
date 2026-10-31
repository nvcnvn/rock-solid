// Package lincheck checks a simulator history for a linearizable order with Porcupine, outside the deterministic simulator.
package lincheck

import (
	"math"
	"slices"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// Result is an operation's output as Porcupine sees it, with Done false for one that never completed.
type Result struct {
	Done   bool
	Output [sim.OpWords]uint64
}

// Operations converts a history to Porcupine's operations, each one's index in its Metadata.
func Operations(h *sim.History) []porcupine.Operation {
	ops := make([]porcupine.Operation, len(h.Ops()))
	for i, op := range h.Ops() {
		// An operation that never completed returns at Porcupine's infinite time, so it may take
		// effect at any point after its call or never. One client's pipelined operations may
		// overlap: Porcupine uses the client ID only to draw the history.
		ret := int64(math.MaxInt64)
		if op.Done {
			ret = int64(op.Complete)
		}
		ops[i] = porcupine.Operation{
			ClientId: int(op.Client),
			Input:    op.Input,
			Call:     int64(op.Invoke),
			Output:   Result{Done: op.Done, Output: op.Output},
			Return:   ret,
			Metadata: i,
		}
	}
	return ops
}

// OpOf converts an operation from Operations back.
func OpOf(o porcupine.Operation) sim.Op {
	r := o.Output.(Result)
	op := sim.Op{Client: uint32(o.ClientId), Invoke: core.Tick(o.Call), Done: r.Done, Input: o.Input.([sim.OpWords]uint64), Output: r.Output}
	if r.Done {
		op.Complete = core.Tick(o.Return)
	}
	return op
}

// Illegal checks the history against model within timeout, 0 for none, and, if no order is legal, returns porcupine.Illegal with the index of an operation the longest legal prefixes leave out; porcupine.Unknown reports a check that ran out of time.
func Illegal(model porcupine.Model, h *sim.History, timeout time.Duration) (int, porcupine.CheckResult) {
	ops := Operations(h)
	res, info := porcupine.CheckOperationsVerbose(model, ops, timeout)
	if res != porcupine.Illegal {
		return 0, res
	}
	// Porcupine gathers the partial orders in a map, so their order varies from run to run; the
	// choice below depends only on which operations they hold.
	// An operation the model's partition leaves out cannot be the one at fault.
	checked := make([]bool, len(ops))
	parts := [][]porcupine.Operation{ops}
	if model.Partition != nil {
		parts = model.Partition(ops)
	}
	for _, part := range parts {
		for _, o := range part {
			checked[o.Metadata.(int)] = true
		}
	}
	placed := make([]bool, len(ops))
	partitions := info.PartialLinearizationsOperations()
	for _, orders := range partitions {
		for _, order := range orders {
			for _, o := range order {
				placed[o.Metadata.(int)] = true
			}
		}
	}
	if bad := firstToReturn(ops, func(i int) bool { return checked[i] && !placed[i] }); bad >= 0 {
		return bad, res
	}
	// Every operation fits some partial order, as when two conflict symmetrically: take each
	// partition's longest order with the smallest sorted indices and name what it leaves out.
	clear(placed)
	member := make([]bool, len(ops))
	for _, orders := range partitions {
		var best []int
		for _, order := range orders {
			idx := make([]int, len(order))
			for k, o := range order {
				idx[k] = o.Metadata.(int)
				member[idx[k]] = true
			}
			slices.Sort(idx)
			if len(idx) > len(best) || len(idx) == len(best) && slices.Compare(idx, best) < 0 {
				best = idx
			}
		}
		for _, i := range best {
			placed[i] = true
		}
	}
	return firstToReturn(ops, func(i int) bool { return member[i] && !placed[i] }), res
}

func firstToReturn(ops []porcupine.Operation, left func(i int) bool) int {
	bad := -1
	for i, o := range ops {
		if left(i) && (bad < 0 || o.Return < ops[bad].Return) {
			bad = i
		}
	}
	return bad
}
