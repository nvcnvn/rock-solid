package gocost

import (
	"syscall"
	"testing"
	"time"
	"unsafe"
)

const (
	clockMonotonicRaw = 4
	clockBoottime     = 7
)

func rawGettime(id uintptr) int64 {
	var ts syscall.Timespec
	syscall.RawSyscall(syscall.SYS_CLOCK_GETTIME, id, uintptr(unsafe.Pointer(&ts)), 0)
	return ts.Nano()
}

func BenchmarkTimeNow(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_ = time.Now()
	}
}

func BenchmarkRawSyscallMonotonicRaw(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_ = rawGettime(clockMonotonicRaw)
	}
}

func BenchmarkRawSyscallBoottime(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_ = rawGettime(clockBoottime)
	}
}
