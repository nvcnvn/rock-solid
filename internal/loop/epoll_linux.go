package loop

import (
	"fmt"

	"github.com/nvcnvn/rock-solid/internal/core"
	"golang.org/x/sys/unix"
)

// Epoll is the real kernel behind the seam: an epoll set, an eventfd for wakes and a timerfd on CLOCK_BOOTTIME.
type Epoll struct {
	ep     int
	wake   int
	timer  int
	events [1024]unix.EpollEvent
	drain  [8]byte
}

var wakeOne = [8]byte{1}

// NewEpoll makes an epoll set with its wake eventfd and its timerfd in it.
func NewEpoll() (*Epoll, error) {
	e := &Epoll{ep: -1, wake: -1, timer: -1}
	var err error
	if e.ep, err = unix.EpollCreate1(unix.EPOLL_CLOEXEC); err != nil {
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

// Close closes the set, the eventfd and the timerfd.
func (e *Epoll) Close() {
	for _, fd := range []int{e.ep, e.wake, e.timer} {
		if fd >= 0 {
			_ = unix.Close(fd)
		}
	}
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

// Clocks reads the clocks one after another; the bracketed read and the VMClock marker belong to the clock item.
func (e *Epoll) Clocks() core.Clocks {
	return core.Clocks{Boot: clock(unix.CLOCK_BOOTTIME), Raw: clock(unix.CLOCK_MONOTONIC_RAW), Mono: clock(unix.CLOCK_MONOTONIC)}
}

func clock(id int32) core.Tick {
	var ts unix.Timespec
	_ = unix.ClockGettime(id, &ts)
	return core.Tick(ts.Nano() / 1000)
}

// Arm sets the timerfd to deadline, absolute on CLOCK_BOOTTIME.
func (e *Epoll) Arm(deadline core.Tick) {
	ns := int64(deadline) * 1000
	spec := unix.ItimerSpec{Value: unix.NsecToTimespec(max(ns, 1))}
	_ = unix.TimerfdSettime(e.timer, unix.TFD_TIMER_ABSTIME, &spec, nil)
}
