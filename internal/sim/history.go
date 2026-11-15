package sim

import (
	"cmp"
	"encoding/binary"
	"fmt"
	"slices"

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

// History records client operations on the scheduler's time, not a node's clock, which drifts and
// slews, or, read from a file by ReadHistory, on the clock its recorders stamped.
type History struct {
	s      *Scheduler
	ops    []Op
	decode func(a *core.Action, data []byte) (op int, out [OpWords]uint64, ok bool)
}

// Replies completes operations from the client replies any machine sends: decode, given a send marked Reply and the sender's bytes of its buffer, nil for none, names the operation it answers and its result, or reports false for any other, such as a one-way message between machines.
func (h *History) Replies(decode func(a *core.Action, data []byte) (op int, out [OpWords]uint64, ok bool)) {
	h.decode = decode
	h.s.history = h
}

func (h *History) sent(p *Pool, a *core.Action) {
	if !a.Reply {
		return
	}
	var data []byte
	if a.Msg.Buf != core.NoBuf {
		data = p.Bytes(a.Msg.Buf)
	}
	if op, out, ok := h.decode(a, data); ok && op >= 0 && op < len(h.ops) {
		h.Complete(op, out)
	}
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

// EventBytes is the size of one event in a history file: little-endian words of the completion flag
// and client, the operation's number within its client, the time, and the operation's input or output.
const EventBytes = 8 * (3 + OpWords)

// PutEvent encodes into b the invocation of client's operation op at t with input words, or with
// complete its completion with output words.
func PutEvent(b *[EventBytes]byte, complete bool, client uint32, op uint64, t core.Tick, words [OpWords]uint64) {
	head := uint64(client)
	if complete {
		head |= 1 << 32
	}
	binary.LittleEndian.PutUint64(b[0:], head)
	binary.LittleEndian.PutUint64(b[8:], op)
	binary.LittleEndian.PutUint64(b[16:], uint64(t))
	for i, w := range words {
		binary.LittleEndian.PutUint64(b[24+8*i:], w)
	}
}

// ReadHistory decodes a history file's events, written by any number of clients in any order, into
// a history of its operations, one with no completion never completed; it records nothing more.
func ReadHistory(b []byte) (*History, error) {
	if len(b)%EventBytes != 0 {
		return nil, fmt.Errorf("sim: a history of %d bytes ends inside an event", len(b))
	}
	type key struct {
		client uint32
		op     uint64
	}
	h := &History{ops: make([]Op, 0, len(b)/EventBytes)}
	at := map[key]int{}
	for ; len(b) > 0; b = b[EventBytes:] {
		head := binary.LittleEndian.Uint64(b)
		if head>>33 != 0 {
			return nil, fmt.Errorf("sim: a history event's head %#x has unknown bits", head)
		}
		k := key{uint32(head), binary.LittleEndian.Uint64(b[8:])}
		t := core.Tick(binary.LittleEndian.Uint64(b[16:]))
		var words [OpWords]uint64
		for i := range words {
			words[i] = binary.LittleEndian.Uint64(b[24+8*i:])
		}
		i, seen := at[k]
		if head>>32 == 0 {
			if seen {
				return nil, fmt.Errorf("sim: client %d invokes operation %d twice", k.client, k.op)
			}
			at[k] = len(h.ops)
			h.ops = append(h.ops, Op{Client: k.client, Invoke: t, Input: words})
			continue
		}
		if !seen || h.ops[i].Done || t < h.ops[i].Invoke {
			return nil, fmt.Errorf("sim: client %d completes operation %d not invoked, again or before its invocation", k.client, k.op)
		}
		h.ops[i].Complete, h.ops[i].Done, h.ops[i].Output = t, true, words
	}
	slices.SortStableFunc(h.ops, func(a, b Op) int { return cmp.Compare(a.Invoke, b.Invoke) })
	return h, nil
}
