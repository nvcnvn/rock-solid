package chunkstore

import (
	"errors"
	"testing"
)

// Each disk is known by the log ID it reports: a disk back from a restart keeps its ID, a disk formatted again or replaced joins as a new disk under a new ID while the one it replaced stays down with its log gone, and a log ID already up on another device is refused.
func TestDisksAreKnownByTheirLogIDs(t *testing.T) {
	var r DiskRecord
	report := func(node, device uint32, log uint64) uint32 {
		t.Helper()
		id, err := r.Report(node, device, log)
		if err != nil {
			t.Fatalf("report of log %#x on %d/%d: %v", log, node, device, err)
		}
		return id
	}
	a, b, c := report(0, 0, 0xa), report(1, 0, 0xb), report(2, 0, 0xc)
	if a != 1 || b != 2 || c != 3 {
		t.Fatalf("disk IDs %d %d %d", a, b, c)
	}
	r.Down(c)
	if got := report(2, 0, 0xc); got != c || !r.Disks()[c-1].Up {
		t.Fatalf("a restarted disk came back as %d", got)
	}
	if got := report(2, 0, 0xc2); got != 4 || r.Disks()[c-1].Up || !r.Disks()[c-1].Gone {
		t.Fatalf("a formatted disk joined as %d, its old ID %+v", got, r.Disks()[c-1])
	}
	if _, err := r.Report(2, 1, 0xc); !errors.Is(err, ErrLogRefused) {
		t.Fatalf("the replaced log came back: %v", err)
	}
	if _, err := r.Report(1, 1, 0xa); !errors.Is(err, ErrLogRefused) {
		t.Fatalf("a cloned drive's log ID, already up: %v", err)
	}
	if d := r.Disks()[a-1]; d.Node != 0 || d.Device != 0 || !d.Up {
		t.Fatalf("the disk that reported first lost its place: %+v", d)
	}
	r.Down(a)
	if got := report(1, 1, 0xa); got != a {
		t.Fatalf("a down disk moved to another device came back as %d", got)
	}
}
