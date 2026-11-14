package loop

import (
	"fmt"
	"sync/atomic"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// Inbox is how other loops and worker goroutines hand a loop events: a bounded lock-free
// multi-producer queue, and a wake of the loop's kernel only when it holds no wake already, so a
// burst costs one wakeup.
type Inbox struct {
	cells    []cell
	mask     uint64
	tail     atomic.Uint64
	_        [56]byte
	notified atomic.Bool
	wakes    atomic.Uint64
	head     uint64
	k        Kernel
}

type cell struct {
	seq atomic.Uint64
	ev  core.Event
}

func newInbox(size int, k Kernel) *Inbox {
	if size <= 0 || size&(size-1) != 0 {
		panic(fmt.Sprintf("loop: inbox of %d events is not a power of two", size))
	}
	in := &Inbox{cells: make([]cell, size), mask: uint64(size - 1), k: k}
	for i := range in.cells {
		in.cells[i].seq.Store(uint64(i))
	}
	return in
}

// Post queues ev for the loop from any goroutine; false when the inbox is full, for the caller to retry.
func (in *Inbox) Post(ev *core.Event) bool {
	for {
		pos := in.tail.Load()
		c := &in.cells[pos&in.mask]
		seq := c.seq.Load()
		if seq < pos {
			return false
		}
		if seq == pos && in.tail.CompareAndSwap(pos, pos+1) {
			c.ev = *ev
			c.seq.Store(pos + 1)
			break
		}
	}
	if !in.notified.Swap(true) {
		in.wakes.Add(1)
		in.k.Wake()
	}
	return true
}

// Wakes is the number of times posts have woken the loop.
func (in *Inbox) Wakes() uint64 { return in.wakes.Load() }

func (in *Inbox) ready() bool {
	return in.cells[in.head&in.mask].seq.Load() == in.head+1
}

func (in *Inbox) pop(ev *core.Event) bool {
	c := &in.cells[in.head&in.mask]
	if c.seq.Load() != in.head+1 {
		return false
	}
	*ev = c.ev
	c.seq.Store(in.head + in.mask + 1)
	in.head++
	return true
}
