package fuse

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// Mount opens /dev/fuse and mounts it at path, which needs root: allow_other, so that every uid
// reaches the volume, as CSI's bind mounts into pods do, and default_permissions, so that the kernel
// checks each access against the attributes the daemon answers with. The daemon never unmounts
// ("Daemon restart and upgrade"): umount(8), or CSI's NodeUnpublishVolume, does.
func Mount(path string) (Device, error) {
	fd, err := unix.Open("/dev/fuse", unix.O_RDWR|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return Device{}, fmt.Errorf("fuse: open /dev/fuse: %w", err)
	}
	data := fmt.Sprintf("fd=%d,rootmode=40000,user_id=%d,group_id=%d,allow_other,default_permissions", fd, os.Getuid(), os.Getgid())
	if err := unix.Mount("rock-solid", path, "fuse.rock-solid", unix.MS_NOSUID|unix.MS_NODEV, data); err != nil {
		_ = unix.Close(fd)
		return Device{}, fmt.Errorf("fuse: mount %s: %w", path, err)
	}
	return Device{Fd: fd}, nil
}
