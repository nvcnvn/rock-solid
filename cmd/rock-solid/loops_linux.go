package main

import (
	"sync"
	"sync/atomic"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/driver/tcp"
	"github.com/nvcnvn/rock-solid/internal/driver/uring"
	"github.com/nvcnvn/rock-solid/internal/loop"
)

const (
	poolBytes = 64 << 20
	// pageRegion is the regions a loop's page slab maps, for its trees' memtable pages.
	pageRegion  = 64 << 20
	ringEntries = 256
	// bootTimer is the event that starts a loop's machine, mailTimer the one that wakes it for its mail.
	bootTimer core.TimerID = 0x6374626f6f74
	mailTimer core.TimerID = 0x6d61696c
)

// wake reports whether ev only wakes a loop, to start its machine or take its mail.
func wake(ev *core.Event) bool {
	return ev.Kind == core.EvTimer && (ev.Timer == bootTimer || ev.Timer == mailTimer)
}

// mail is how other goroutines hand a loop work off the data path: calls the loop runs at its
// next event.
type mail struct {
	mu      sync.Mutex
	fs      []func(*core.Context)
	waiting atomic.Bool
	in      *loop.Inbox
}

// post queues f and wakes the loop. A full inbox drops the wake but not f: the loop is busy, and
// its next event runs f.
func (m *mail) post(f func(*core.Context)) {
	m.mu.Lock()
	m.fs = append(m.fs, f)
	m.mu.Unlock()
	m.waiting.Store(true)
	m.in.Post(&core.Event{Kind: core.EvTimer, Timer: mailTimer})
}

// run runs the calls posted, on the loop's goroutine.
func (m *mail) run(ctx *core.Context) {
	if !m.waiting.Swap(false) {
		return
	}
	m.mu.Lock()
	fs := m.fs
	m.fs = nil
	m.mu.Unlock()
	for _, f := range fs {
		f(ctx)
	}
}

// loopDriver sends over TCP and runs disk actions on the ring.
type loopDriver struct {
	*tcp.Driver
	disk *uring.Disk
}

func (d loopDriver) Execute(a *core.Action) {
	if a.Kind == core.ActNetSend {
		d.Driver.Execute(a)
		return
	}
	d.disk.Execute(a)
}
