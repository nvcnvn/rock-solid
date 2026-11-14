package loop

import (
	"unsafe"

	"golang.org/x/sys/unix"
)

// macOS runs only the stepped tests; the same raw calls as Linux keep their allocation check meaningful.

func rawRead(fd int, p []byte) (int, unix.Errno) {
	//lint:ignore SA1019 libSystem's read allocates nothing either, but the stepped tests should run Linux's code path
	n, _, e := unix.RawSyscall(unix.SYS_READ, uintptr(fd), uintptr(unsafe.Pointer(&p[0])), uintptr(len(p)))
	return int(n), e
}

func rawWritev(fd int, iov []unix.Iovec) (int, unix.Errno) {
	//lint:ignore SA1019 unix.Writev allocates its iovecs, which the stepped allocation check would count
	n, _, e := unix.RawSyscall(unix.SYS_WRITEV, uintptr(fd), uintptr(unsafe.Pointer(&iov[0])), uintptr(len(iov)))
	return int(n), e
}
