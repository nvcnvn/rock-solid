package loop

import (
	"errors"
	"fmt"
	"io/fs"
	"math"
	"unsafe"

	"github.com/nvcnvn/rock-solid/internal/core"
	"golang.org/x/sys/unix"
)

// Epoll is the real kernel behind the seam: an epoll set, an eventfd for wakes, a timerfd on
// CLOCK_BOOTTIME and, where the hypervisor offers one, the VMClock device.
type Epoll struct {
	ep      int
	wake    int
	timer   int
	vm      *VMClock
	vmErr   error
	events  [1024]unix.EpollEvent
	drain   [8]byte
	ts      unix.Timespec
	retries uint64
	widest  int64
}

const (
	// bracket is ε, the most two CLOCK_MONOTONIC readings around one clock read may lie apart, in ns.
	bracket = 1_000_000
	// bracketTries bounds the reads of one Clocks call; past it the narrowest is taken.
	bracketTries = 8
)

var wakeOne = [8]byte{1}

// NewEpoll makes an epoll set with its wake eventfd and its timerfd in it, and maps the VMClock
// device at vmclock, VMClockPath outside tests. Where no device exists the marker reads 0; a device
// that exists but cannot be mapped, or is not a VMClock, fails NewEpoll.
func NewEpoll(vmclock string) (*Epoll, error) {
	vm, err := OpenVMClock(vmclock)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	e := &Epoll{ep: -1, wake: -1, timer: -1, vm: vm, vmErr: err}
	if e.ep, err = unix.EpollCreate1(unix.EPOLL_CLOEXEC); err != nil {
		e.Close()
		return nil, fmt.Errorf("loop: epoll_create1: %w", err)
	}
	if e.wake, err = unix.Eventfd(0, unix.EFD_NONBLOCK|unix.EFD_CLOEXEC); err != nil {
		e.Close()
		return nil, fmt.Errorf("loop: eventfd: %w", err)
	}
	if e.timer, err = unix.TimerfdCreate(unix.CLOCK_BOOTTIME, unix.TFD_NONBLOCK|unix.TFD_CLOEXEC); err != nil {
		e.Close()
		return nil, fmt.Errorf("loop: timerfd_create: %w", err)
	}
	for _, w := range []struct {
		fd  int
		tok uint32
	}{{e.wake, TokenWake}, {e.timer, TokenTimer}} {
		if err := e.Watch(w.fd, w.tok); err != nil {
			e.Close()
			return nil, err
		}
	}
	return e, nil
}

// VMClock is nil when the clocks read include the VMClock marker, or else why they do not.
func (e *Epoll) VMClock() error { return e.vmErr }

// Bracket reports how many clock reads Clocks found wider than ε, their two CLOCK_MONOTONIC
// readings lying further apart, and the widest such gap in ns.
func (e *Epoll) Bracket() (retries uint64, widest int64) { return e.retries, e.widest }

// Close closes the set, the eventfd and the timerfd, and unmaps the VMClock device.
func (e *Epoll) Close() {
	for _, fd := range []int{e.ep, e.wake, e.timer} {
		if fd >= 0 {
			_ = unix.Close(fd)
		}
	}
	_ = e.vm.Close()
}

func (e *Epoll) ctl(op, fd int, token uint32, events uint32) error {
	ev := unix.EpollEvent{Events: events, Fd: int32(token)}
	if err := unix.EpollCtl(e.ep, op, fd, &ev); err != nil {
		return fmt.Errorf("loop: epoll_ctl of fd %d: %w", fd, err)
	}
	return nil
}

// Watch adds fd, level-triggered, so a connection left unread past its budget is reported again.
func (e *Epoll) Watch(fd int, token uint32) error {
	return e.ctl(unix.EPOLL_CTL_ADD, fd, token, unix.EPOLLIN|unix.EPOLLRDHUP)
}

// Modify sets whether fd is reported when readable, half-closed by its peer, or writable.
func (e *Epoll) Modify(fd int, token uint32, in, out bool) error {
	var ev uint32
	if in {
		ev |= unix.EPOLLIN | unix.EPOLLRDHUP
	}
	if out {
		ev |= unix.EPOLLOUT
	}
	return e.ctl(unix.EPOLL_CTL_MOD, fd, token, ev)
}

// Unwatch removes fd from the set. Closing it would not while a forked child or a dup holds the
// same open file, and the entry would go on firing under its old token.
func (e *Epoll) Unwatch(fd int) error {
	if err := unix.EpollCtl(e.ep, unix.EPOLL_CTL_DEL, fd, nil); err != nil {
		return fmt.Errorf("loop: epoll_ctl DEL of fd %d: %w", fd, err)
	}
	return nil
}

// Wait is one epoll_wait, draining the wake eventfd and the timerfd when they are reported.
func (e *Epoll) Wait(ready []Ready, block bool) []Ready {
	timeout := 0
	if block {
		timeout = -1
	}
	n, err := unix.EpollWait(e.ep, e.events[:], timeout)
	if err != nil {
		return ready
	}
	for i := range n {
		ev := &e.events[i]
		tok := uint32(ev.Fd)
		switch tok {
		case TokenWake:
			_, _ = rawRead(e.wake, e.drain[:])
		case TokenTimer:
			_, _ = rawRead(e.timer, e.drain[:])
		}
		ready = append(ready, Ready{
			Token: tok,
			In:    ev.Events&unix.EPOLLIN != 0,
			Out:   ev.Events&unix.EPOLLOUT != 0,
			Hup:   ev.Events&(unix.EPOLLHUP|unix.EPOLLRDHUP|unix.EPOLLERR) != 0,
		})
	}
	return ready
}

// Wake writes the eventfd.
func (e *Epoll) Wake() {
	_, _ = unix.Write(e.wake, wakeOne[:])
}

// Clocks reads CLOCK_BOOTTIME and CLOCK_MONOTONIC_RAW between two CLOCK_MONOTONIC readings, again
// until those lie within ε, at most bracketTries times, keeping the narrowest: a stall between the
// reads would otherwise look like a suspend.
func (e *Epoll) Clocks() core.Clocks {
	var best core.Clocks
	narrowest := int64(math.MaxInt64)
	for range bracketTries {
		mono := nanotime()
		boot := e.clock(unix.CLOCK_BOOTTIME)
		raw := e.clock(unix.CLOCK_MONOTONIC_RAW)
		gap := nanotime() - mono
		if gap < narrowest {
			narrowest = gap
			best = core.Clocks{Boot: boot, Raw: raw, Mono: core.Tick(mono / 1000)}
		}
		if gap <= bracket {
			break
		}
		e.retries++
		e.widest = max(e.widest, gap)
	}
	best.Marker = e.vm.Marker()
	return best
}

func (e *Epoll) clock(id uintptr) core.Tick {
	_, _, _ = unix.RawSyscall(unix.SYS_CLOCK_GETTIME, id, uintptr(unsafe.Pointer(&e.ts)), 0)
	return core.Tick(e.ts.Nano() / 1000)
}

// nanotime is CLOCK_MONOTONIC through the vDSO.
//
//go:linkname nanotime runtime.nanotime
func nanotime() int64

// Arm sets the timerfd to deadline, absolute on CLOCK_BOOTTIME.
func (e *Epoll) Arm(deadline core.Tick) {
	ns := int64(deadline) * 1000
	spec := unix.ItimerSpec{Value: unix.NsecToTimespec(max(ns, 1))}
	_ = unix.TimerfdSettime(e.timer, unix.TFD_TIMER_ABSTIME, &spec, nil)
}
