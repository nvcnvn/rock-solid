package looptest

import (
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/loop"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"golang.org/x/sys/unix"
)

// Disk is a stepped loop's disk ring over a simulated disk: an action runs on the disk when the
// loop executes it and completes after the disk's latency, never before an action executed
// earlier, as a linked flush follows its write.
type Disk struct {
	K    loop.Kernel
	D    *sim.Disk
	Pool core.BufPool
	fds  [2]int
	done []completion
	last core.Tick
}

type completion struct {
	ev core.Event
	at core.Tick
}

// NewDisk makes a ring over d whose clocks are k's; its descriptor never turns readable, since
// Waiting reports what is due.
func NewDisk(k loop.Kernel, d *sim.Disk, pool core.BufPool) (*Disk, error) {
	r := &Disk{K: k, D: d, Pool: pool}
	if err := unix.Pipe(r.fds[:]); err != nil {
		return nil, err
	}
	return r, nil
}

// Close closes the ring's descriptor.
func (r *Disk) Close() {
	_ = unix.Close(r.fds[0])
	_ = unix.Close(r.fds[1])
}

// Execute runs a disk action now; with no simulated disk it succeeds at once and stores nothing.
func (r *Disk) Execute(a *core.Action) {
	ev, took := core.Event{Kind: core.EvDiskDone, IO: a.IO}, core.Tick(0)
	if r.D != nil {
		ev, took = r.D.ExecuteTimed(r.Pool, a)
	}
	r.last = max(r.last, r.K.Clocks().Boot+took)
	r.done = append(r.done, completion{ev: ev, at: r.last})
}

// Fd is the ring's descriptor.
func (r *Disk) Fd() int { return r.fds[0] }

// Reap appends the completions due.
func (r *Disk) Reap(evs []core.Event) []core.Event {
	now, n := r.K.Clocks().Boot, 0
	for n < len(r.done) && r.done[n].at <= now {
		evs = append(evs, r.done[n].ev)
		n++
	}
	r.done = r.done[:copy(r.done, r.done[n:])]
	return evs
}

// Submit makes no call; actions ran when executed.
func (r *Disk) Submit() loop.Enter { return loop.Enter{} }

// Waiting reports a completion due.
func (r *Disk) Waiting() bool { return len(r.done) > 0 && r.done[0].at <= r.K.Clocks().Boot }

// Next is when the earliest completion in flight is due.
func (r *Disk) Next() (core.Tick, bool) {
	if len(r.done) == 0 {
		return 0, false
	}
	return r.done[0].at, true
}
