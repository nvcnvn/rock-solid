package chunkstore

import (
	"errors"
	"fmt"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// Each disk is known by the log ID it reports: a disk back from a restart, or moved to another node, keeps its ID, a disk formatted again or replaced joins as a new disk under a new ID while the one it replaced stays down with its log gone, and a log ID on another device than its own, up or down, is a copy and refused.
func TestDisksAreKnownByTheirLogIDs(t *testing.T) {
	var r DiskRecord
	report := func(node uint32, device, log uint64) uint32 {
		t.Helper()
		id, err := r.Report(node, device, log, "")
		if err != nil {
			t.Fatalf("report of log %#x on %d/%d: %v", log, node, device, err)
		}
		return id
	}
	a, b, c := report(0, 0, 0xa), report(1, 0, 0xb), report(2, 0, 0xc)
	if a != 1 || b != 2 || c != 3 {
		t.Fatalf("disk IDs %d %d %d", a, b, c)
	}
	r.Down(c, 1)
	if got := report(2, 0, 0xc); got != c || !r.Disks()[c-1].Up {
		t.Fatalf("a restarted disk came back as %d", got)
	}
	if got := report(2, 0, 0xc2); got != 4 || r.Disks()[c-1].Up || !r.Disks()[c-1].Gone {
		t.Fatalf("a formatted disk joined as %d, its old ID %+v", got, r.Disks()[c-1])
	}
	if _, err := r.Report(2, 1, 0xc, ""); !errors.Is(err, ErrLogRefused) {
		t.Fatalf("the replaced log came back: %v", err)
	}
	if _, err := r.Report(1, 1, 0xa, ""); !errors.Is(err, ErrLogRefused) {
		t.Fatalf("a cloned drive's log ID, already up: %v", err)
	}
	if d := r.Disks()[a-1]; d.Node != 0 || d.Device != 0 || !d.Up {
		t.Fatalf("the disk that reported first lost its place: %+v", d)
	}
	r.Down(a, 1)
	if _, err := r.Report(1, 1, 0xa, ""); !errors.Is(err, ErrLogRefused) {
		t.Fatalf("a cloned drive's log ID, its disk down: %v", err)
	}
	if got := report(3, 0, 0xa); got != a || r.Disks()[a-1].Node != 3 {
		t.Fatalf("a down disk moved to another node came back as %d: %+v", got, r.Disks()[a-1])
	}
}

// A disk down can be declared lost: it is gone and its log is refused. A disk up, or unknown, cannot.
func TestADiskDownCanBeDeclaredLost(t *testing.T) {
	var r DiskRecord
	id, err := r.Report(1, 0, 0xa, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Lost(id); !errors.Is(err, ErrLostRefused) {
		t.Fatalf("an up disk declared lost: %v", err)
	}
	if err := r.Lost(id + 1); !errors.Is(err, ErrLostRefused) {
		t.Fatalf("an unknown disk declared lost: %v", err)
	}
	r.Down(id, 5)
	if err := r.Lost(id); err != nil || !r.Disks()[id-1].Gone {
		t.Fatalf("%v, %+v", err, r.Disks()[id-1])
	}
	if _, err := r.Report(1, 0, 0xa, ""); !errors.Is(err, ErrLogRefused) {
		t.Fatalf("a lost disk reported again: %v", err)
	}
}

// A device whose block 0 and copy both read as zeros is formatted under a new log ID, drawn again
// at each start; one that opened is reported; anything else is reported failed and never written.
func TestADeviceIsFormattedOnlyWhenBothSuperblocksAreZeros(t *testing.T) {
	rng := core.NewRand(1)
	if act, _ := StartDevice(rng, nil); act != StartOpened {
		t.Fatalf("an opened device: %v", act)
	}
	seen := map[uint64]bool{}
	for start := range 100 {
		act, log := StartDevice(rng, fmt.Errorf("open: %w", disklog.ErrNotFormatted))
		if act != StartFormat || seen[log] {
			t.Fatalf("start %d of a blank device: %v under log %#x, drawn before: %v", start, act, log, seen[log])
		}
		seen[log] = true
	}
	for _, err := range []error{disklog.ErrUnrecognized, &disklog.CopyOnlyError{Block0: disklog.ErrUnrecognized}, core.DiskIO, disklog.ErrCorrupt} {
		if act, _ := StartDevice(rng, err); act != StartFailed {
			t.Fatalf("a device that failed with %v: %v", err, act)
		}
	}
}
