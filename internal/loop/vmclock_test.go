package loop_test

import (
	"encoding/binary"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/nvcnvn/rock-solid/internal/loop"
)

// vmclockFile writes a crafted VMClock page holding marker and returns its path.
func vmclockFile(t *testing.T, marker uint64) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "vmclock0")
	setMarker(t, path, marker)
	return path
}

// setMarker writes a VMClock page with magic, version 1, an even sequence count and marker to path.
func setMarker(t *testing.T, path string, marker uint64) {
	t.Helper()
	page := make([]byte, 4096)
	binary.LittleEndian.PutUint32(page[0:], 0x4b4c4356)
	binary.LittleEndian.PutUint32(page[4:], 4096)
	binary.LittleEndian.PutUint16(page[8:], 1)
	binary.LittleEndian.PutUint32(page[12:], 2*uint32(marker))
	binary.LittleEndian.PutUint64(page[16:], marker)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteAt(page, 0); err != nil {
		t.Fatal(err)
	}
}

func TestTheVMClockMarkerIsReadFromTheMappedPage(t *testing.T) {
	path := vmclockFile(t, 7)
	v, err := loop.OpenVMClock(path)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	if m := v.Marker(); m != 7 {
		t.Fatalf("marker %d; want 7", m)
	}
	setMarker(t, path, 8)
	if m := v.Marker(); m != 8 {
		t.Fatalf("after the page changed, marker %d; want 8", m)
	}
}

func TestAMissingOrForeignVMClockIsRefusedAndReadsZero(t *testing.T) {
	_, err := loop.OpenVMClock(filepath.Join(t.TempDir(), "vmclock0"))
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a missing device gave %v", err)
	}
	foreign := filepath.Join(t.TempDir(), "zeros")
	if err := os.WriteFile(foreign, make([]byte, 4096), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loop.OpenVMClock(foreign); err == nil {
		t.Fatal("a page without VMClock's magic was mapped")
	}
	var none *loop.VMClock
	if m := none.Marker(); m != 0 {
		t.Fatalf("no device read marker %d", m)
	}
}

func TestATornVMClockReadIsRetriedAndAStuckOneReadsAsChanged(t *testing.T) {
	path := vmclockFile(t, 7)
	v, err := loop.OpenVMClock(path)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var b [8]byte
	binary.LittleEndian.PutUint32(b[:], 15)
	if _, err := f.WriteAt(b[:4], 12); err != nil {
		t.Fatal(err)
	}
	if m := v.Marker(); m != loop.MarkerStuck {
		t.Fatalf("a sequence count held odd read marker %d; want the stuck marker", m)
	}
	var wg sync.WaitGroup
	var werr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(20 * time.Millisecond)
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], 8)
		if _, werr = f.WriteAt(b[:], 16); werr != nil {
			return
		}
		binary.LittleEndian.PutUint32(b[:], 16)
		_, werr = f.WriteAt(b[:4], 12)
	}()
	start := time.Now()
	m := v.MarkerWithin(math.MaxInt)
	waited := time.Since(start)
	wg.Wait()
	if werr != nil {
		t.Fatal(werr)
	}
	if m != 8 || waited < 20*time.Millisecond {
		t.Fatalf("read marker %d after %v of an update that took 20 ms; want 8 after it", m, waited)
	}
}
