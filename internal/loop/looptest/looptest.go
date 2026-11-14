// Package looptest steps event loops from the simulator's scheduler: a kernel whose Wait polls the
// loop's real descriptors without blocking and whose clocks are the node's scripted clock model.
package looptest

import (
	"fmt"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/loop"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"golang.org/x/sys/unix"
)

// Kernel is the stepped kernel behind a loop's seam.
type Kernel struct {
	s      *sim.Scheduler
	node   uint32
	fds    []unix.PollFd
	tokens []uint32
	woken  bool
	armed  core.Tick
	l      *loop.Loop
	err    error
}

// NewKernel makes a stepped kernel whose clocks are node's in s.
func NewKernel(s *sim.Scheduler, node uint32) *Kernel { return &Kernel{s: s, node: node} }

// Attach names the loop the kernel serves, whose Pending a blocking Wait checks.
func (k *Kernel) Attach(l *loop.Loop) { k.l = l }

// Err is the first time the loop asked to block with work pending.
func (k *Kernel) Err() error { return k.err }

// Watch adds fd.
func (k *Kernel) Watch(fd int, token uint32) error {
	k.fds = append(k.fds, unix.PollFd{Fd: int32(fd), Events: unix.POLLIN})
	k.tokens = append(k.tokens, token)
	return nil
}

func (k *Kernel) find(fd int) int {
	for i := range k.fds {
		if k.fds[i].Fd == int32(fd) {
			return i
		}
	}
	return -1
}

// Modify sets whether fd is polled for reading and for writing.
func (k *Kernel) Modify(fd int, _ uint32, in, out bool) error {
	i := k.find(fd)
	if i < 0 {
		return fmt.Errorf("looptest: fd %d is not watched", fd)
	}
	k.fds[i].Events = 0
	if in {
		k.fds[i].Events |= unix.POLLIN
	}
	if out {
		k.fds[i].Events |= unix.POLLOUT
	}
	return nil
}

// Unwatch removes fd, keeping the others in the order they were added.
func (k *Kernel) Unwatch(fd int) error {
	if i := k.find(fd); i >= 0 {
		k.fds = slices.Delete(k.fds, i, i+1)
		k.tokens = slices.Delete(k.tokens, i, i+1)
	}
	return nil
}

func (k *Kernel) poll() int {
	if len(k.fds) == 0 {
		return 0
	}
	n, err := unix.Poll(k.fds, 0)
	if err != nil {
		return 0
	}
	return n
}

// Wait polls without blocking, in the order the descriptors were added; with block set while the
// loop reports work pending, it records the run's failure.
func (k *Kernel) Wait(ready []loop.Ready, block bool) []loop.Ready {
	if block && k.err == nil && k.l.Pending().Work {
		k.err = fmt.Errorf("looptest: loop on node %d asked to block in Wait with work pending, pass %d", k.node, k.l.Passes()+1)
	}
	if k.poll() > 0 {
		for i := range k.fds {
			ev := k.fds[i].Revents
			if ev == 0 {
				continue
			}
			ready = append(ready, loop.Ready{
				Token: k.tokens[i],
				In:    ev&unix.POLLIN != 0,
				Out:   ev&unix.POLLOUT != 0,
				Hup:   ev&(unix.POLLHUP|unix.POLLERR) != 0,
			})
		}
	}
	if k.woken {
		k.woken = false
		ready = append(ready, loop.Ready{Token: loop.TokenWake, In: true})
	}
	if k.armed != 0 && k.Clocks().Boot >= k.armed {
		k.armed = 0
		ready = append(ready, loop.Ready{Token: loop.TokenTimer, In: true})
	}
	return ready
}

// Wake marks the kernel woken; stepped loops share one goroutine.
func (k *Kernel) Wake() { k.woken = true }

// Clocks reads the node's clock model at the scheduler's time.
func (k *Kernel) Clocks() core.Clocks { return k.s.Node(k.node).Clock().Read(k.s.Time()) }

// Arm records the deadline Wait reports once the node's CLOCK_BOOTTIME reaches it.
func (k *Kernel) Arm(deadline core.Tick) { k.armed = deadline }

// Actor steps a loop as one of the scheduler's actors: ready when its kernel has something to
// report or it has work, a pass per step, and a wake at its earliest timer.
type Actor struct {
	K *Kernel
	L *loop.Loop
}

// Ready reports whether a pass would find something to do.
func (a *Actor) Ready() bool {
	if a.K.err != nil {
		return false
	}
	p := a.L.Pending()
	return p.Work || a.K.woken || a.K.poll() > 0 || p.Deadline != 0 && a.K.Clocks().Boot >= p.Deadline
}

// Next makes one pass; the loop executes its own actions.
func (a *Actor) Next(_ *core.Context, out []core.Action) []core.Action {
	a.L.Pass()
	return out
}

// Wake is the loop's earliest timer.
func (a *Actor) Wake() (core.Tick, bool) {
	d := a.L.Pending().Deadline
	return d, d != 0 && a.K.err == nil
}

var idle = sim.MachineFunc(func(_ *core.Context, _ *core.Event, out []core.Action) []core.Action { return out })

// Add runs the loop behind k on node n as its next loop, stepped by the scheduler.
func Add(n *sim.Node, k *Kernel, l *loop.Loop) {
	k.Attach(l)
	n.AddActor(n.Start(idle, nil), &Actor{K: k, L: l})
}
