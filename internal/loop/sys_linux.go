package loop

import (
	"unsafe"

	"golang.org/x/sys/unix"
)

// rawRead, rawWritev and rawReadv are non-blocking, so they skip the scheduler's syscall bookkeeping.

func rawRead(fd int, p []byte) (int, unix.Errno) {
	n, _, e := unix.RawSyscall(unix.SYS_READ, uintptr(fd), uintptr(unsafe.Pointer(&p[0])), uintptr(len(p)))
	return int(n), e
}

func rawWritev(fd int, iov []unix.Iovec) (int, unix.Errno) {
	n, _, e := unix.RawSyscall(unix.SYS_WRITEV, uintptr(fd), uintptr(unsafe.Pointer(&iov[0])), uintptr(len(iov)))
	return int(n), e
}

func rawReadv(fd int, iov []unix.Iovec) (int, unix.Errno) {
	n, _, e := unix.RawSyscall(unix.SYS_READV, uintptr(fd), uintptr(unsafe.Pointer(&iov[0])), uintptr(len(iov)))
	return int(n), e
}
