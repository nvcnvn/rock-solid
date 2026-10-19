package gocost

import (
	"sync/atomic"
	"syscall"
	"testing"
	"unsafe"
)

const clockMonotonicRaw = 4

func rawGettime(id uintptr) int64 {
	var ts syscall.Timespec
	syscall.RawSyscall(syscall.SYS_CLOCK_GETTIME, id, uintptr(unsafe.Pointer(&ts)), 0)
	return ts.Nano()
}

// page stands in for the mmap of /dev/vmclock0: a shared read-only mapping of
// one page, which under KVM is ordinary guest RAM.
func page(b *testing.B) []byte {
	p, err := syscall.Mmap(-1, 0, 4096, syscall.PROT_READ, syscall.MAP_SHARED|syscall.MAP_ANONYMOUS)
	if err != nil {
		b.Fatal(err)
	}
	return p
}

// marker is the seqlock read of vmclock_abi's disruption_marker (offset 16)
// under seq_count (offset 12).
func marker(p []byte) uint64 {
	seq := (*uint32)(unsafe.Pointer(&p[12]))
	m := (*uint64)(unsafe.Pointer(&p[16]))
	for {
		s := atomic.LoadUint32(seq) &^ 1
		v := atomic.LoadUint64(m)
		if atomic.LoadUint32(seq) == s {
			return v
		}
	}
}

func BenchmarkMarker(b *testing.B) {
	p := page(b)
	b.ReportAllocs()
	var sum uint64
	for b.Loop() {
		sum += marker(p)
	}
	_ = sum
}

func BenchmarkRawSyscallMonotonicRaw(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_ = rawGettime(clockMonotonicRaw)
	}
}
