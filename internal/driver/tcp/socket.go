package tcp

import (
	"net/netip"

	"golang.org/x/sys/unix"
)

func sockaddr(ap netip.AddrPort) (int, unix.Sockaddr) {
	if ap.Addr().Is4() {
		return unix.AF_INET, &unix.SockaddrInet4{Port: int(ap.Port()), Addr: ap.Addr().As4()}
	}
	return unix.AF_INET6, &unix.SockaddrInet6{Port: int(ap.Port()), Addr: ap.Addr().As16()}
}

// ListenTCP opens a non-blocking listening socket on ap, port 0 for any, and returns it and its port.
func ListenTCP(ap netip.AddrPort) (int, int, error) {
	family, sa := sockaddr(ap)
	fd, err := unix.Socket(family, unix.SOCK_STREAM, 0)
	if err != nil {
		return -1, 0, err
	}
	unix.CloseOnExec(fd)
	err = unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1)
	if err == nil {
		err = unix.Bind(fd, sa)
	}
	if err == nil {
		err = unix.Listen(fd, 1024)
	}
	if err == nil {
		err = unix.SetNonblock(fd, true)
	}
	var bound unix.Sockaddr
	if err == nil {
		bound, err = unix.Getsockname(fd)
	}
	if err != nil {
		_ = unix.Close(fd)
		return -1, 0, err
	}
	switch b := bound.(type) {
	case *unix.SockaddrInet4:
		return fd, b.Port, nil
	case *unix.SockaddrInet6:
		return fd, b.Port, nil
	}
	return fd, 0, nil
}

// DialTCP opens a non-blocking stream to ap with TCP_NODELAY, connected or connecting: a failed
// connect shows as an error on the connection's first read or write.
func DialTCP(ap netip.AddrPort) (int, error) {
	family, sa := sockaddr(ap)
	fd, err := unix.Socket(family, unix.SOCK_STREAM, 0)
	if err != nil {
		return -1, err
	}
	unix.CloseOnExec(fd)
	err = unix.SetNonblock(fd, true)
	if err == nil {
		err = unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_NODELAY, 1)
	}
	if err == nil {
		if err = unix.Connect(fd, sa); err == unix.EINPROGRESS {
			err = nil
		}
	}
	if err != nil {
		_ = unix.Close(fd)
		return -1, err
	}
	return fd, nil
}
