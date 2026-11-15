package uring

import (
	"testing"

	"github.com/nvcnvn/rock-solid/internal/chunkstore/storetest"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/loop"
	"golang.org/x/sys/unix"
)

// suiteDisk is a file-backed disk on its own ring, which Restart closes and opens again over the
// same file, as a process restart does.
type suiteDisk struct {
	t    testing.TB
	path string
	pool *loop.Pool
	r    *Ring
	d    *Disk
}

func newSuiteDisk(t *testing.T) storetest.Disk {
	sd := &suiteDisk{t: t, path: diskFile(t, storetest.DiskBytes/bs)}
	sd.start()
	return sd
}

func (sd *suiteDisk) start() {
	sd.r, sd.pool = newRing(sd.t, storetest.PoolBytes, 256)
	var err error
	if sd.d, err = sd.r.Open(sd.path, false); err != nil {
		sd.t.Fatal(err)
	}
}

func (sd *suiteDisk) stop() {
	sd.r.Close()
	_ = sd.pool.Close()
}

func (sd *suiteDisk) Pool() core.BufPool     { return sd.pool }
func (sd *suiteDisk) Blocks() uint64         { return sd.d.Blocks() }
func (sd *suiteDisk) Execute(a *core.Action) { sd.d.Execute(a) }

func (sd *suiteDisk) Complete(evs []core.Event) []core.Event {
	n := len(evs)
	var drain [8]byte
	for range 100_000 {
		if e := sd.r.Submit(); e.Errno != 0 {
			sd.t.Fatalf("io_uring_enter: %v", e.Errno)
		}
		if evs = sd.r.Reap(evs); len(evs) > n {
			return evs
		}
		if _, err := unix.Poll([]unix.PollFd{{Fd: int32(sd.r.Fd()), Events: unix.POLLIN}}, 100); err != nil && err != unix.EINTR {
			sd.t.Fatal(err)
		}
		_, _ = unix.Read(sd.r.Fd(), drain[:])
	}
	sd.t.Fatalf("no completion, %d in flight", sd.r.InFlight())
	return nil
}

func (sd *suiteDisk) Restart() {
	sd.stop()
	sd.start()
}

func TestTheChunkStoreSuite(t *testing.T) {
	storetest.Run(t, newSuiteDisk)
	for _, c := range storetest.SimCases {
		t.Run(c.Name, func(t *testing.T) { t.Skip("simulator-only: the case injects disk faults") })
	}
}
