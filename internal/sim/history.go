package sim

import (
	"fmt"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// OpWords is the number of words in an operation's input and in its output.
const OpWords = 4

// Op is one client operation; Done stays false if it never completes, as when its node crashes first.
type Op struct {
	Client   uint32
	Invoke   core.Tick
	Complete core.Tick
	Done     bool
	Input    [OpWords]uint64
	Output   [OpWords]uint64
}

// History records client operations on the scheduler's time, not a node's clock, which drifts and slews.
type History struct {
	s   *Scheduler
	ops []Op
}

// NewHistory records operations at s's time, with room for capacity of them before it allocates.
func NewHistory(s *Scheduler, capacity int) *History {
	return &History{s: s, ops: make([]Op, 0, capacity)}
}

// Invoke records the start of an operation and returns its index.
func (h *History) Invoke(client uint32, in [OpWords]uint64) int {
	h.ops = append(h.ops, Op{Client: client, Invoke: h.s.now, Input: in})
	return len(h.ops) - 1
}

// Complete records operation i's result.
func (h *History) Complete(i int, out [OpWords]uint64) {
	op := &h.ops[i]
	if op.Done {
		panic(fmt.Sprintf("sim: operation %d completes twice", i))
	}
	op.Complete, op.Done, op.Output = h.s.now, true, out
}

// Ops lists the operations in the order they were invoked; the list is valid until the next Invoke.
func (h *History) Ops() []Op { return h.ops }
