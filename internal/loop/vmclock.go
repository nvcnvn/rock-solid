package loop

import (
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/unix"
)

// VMClockPath is the hypervisor's VMClock device on a Linux guest whose kernel has the driver.
const VMClockPath = "/dev/vmclock0"

// Offsets in struct vmclock_abi (include/uapi/linux/vmclock-abi.h).
const (
	vmclockMagic     = 0x4b4c4356
	vmclockSeq       = 12
	vmclockMarker    = 16
	vmclockMapLength = vmclockMarker + 8
	// vmclockTries bounds Marker's retries while the host holds the sequence count odd or moves it.
	vmclockTries = 1 << 16
	// markerStuck is the marker read when the sequence count never settles: it differs from the
	// device's marker, so the loop delivers one Resumed, and none more while the count stays stuck.
	markerStuck = ^uint64(0)
)

// VMClock is a read-only mapping of a VMClock device, read for its disruption marker.
type VMClock struct {
	page []byte
}

// OpenVMClock maps the VMClock device at path and checks its magic number.
func OpenVMClock(path string) (*VMClock, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("loop: opening %s: %w", path, err)
	}
	page, err := unix.Mmap(fd, 0, vmclockMapLength, unix.PROT_READ, unix.MAP_SHARED)
	_ = unix.Close(fd)
	if err != nil {
		return nil, fmt.Errorf("loop: mapping %s: %w", path, err)
	}
	if m := binary.LittleEndian.Uint32(page); m != vmclockMagic {
		_ = unix.Munmap(page)
		return nil, fmt.Errorf("loop: %s has magic %#x, not VMClock's %#x", path, m, vmclockMagic)
	}
	return &VMClock{page: page}, nil
}

// Marker is the disruption marker, read under the device's sequence count; 0 on a nil VMClock.
// A count that does not settle within vmclockTries reads gives markerStuck.
func (v *VMClock) Marker() uint64 { return v.marker(vmclockTries) }

// The device's fields are little-endian, read here with native loads: amd64 and arm64 are both.
func (v *VMClock) marker(tries int) uint64 {
	if v == nil {
		return 0
	}
	seq := (*uint32)(unsafe.Pointer(&v.page[vmclockSeq]))
	marker := (*uint64)(unsafe.Pointer(&v.page[vmclockMarker]))
	for range tries {
		s := atomic.LoadUint32(seq) &^ 1
		m := atomic.LoadUint64(marker)
		if atomic.LoadUint32(seq) == s {
			return m
		}
	}
	return markerStuck
}

// Close unmaps the device.
func (v *VMClock) Close() error {
	if v == nil {
		return nil
	}
	return unix.Munmap(v.page)
}
