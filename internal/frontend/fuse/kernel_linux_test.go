package fuse

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/fsclient"
	"github.com/nvcnvn/rock-solid/internal/fuseproto"
)

// mountDir is a fresh directory to mount on under $RS_FUSE_DIR, as root, unmounted at the end; the
// test skips without both, since a mount with allow_other needs root.
func mountDir(t *testing.T) string {
	base := os.Getenv("RS_FUSE_DIR")
	switch {
	case base == "":
		t.Skip("RS_FUSE_DIR names no directory to mount under")
	case os.Geteuid() != 0:
		t.Skip("mounting with allow_other needs root")
	}
	dir, err := os.MkdirTemp(base, "mnt")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(dir) })
	return dir
}

// serveRoot answers the kernel's requests on dev until the connection ends: INIT as the daemon
// does, GETATTR with a root directory of mode 0755, and everything else ENOSYS.
func serveRoot(dev Device) error {
	buf := make([]byte, ReadBuffer)
	for {
		if _, err := unix.Poll([]unix.PollFd{{Fd: int32(dev.Fd), Events: unix.POLLIN}}, -1); err != nil && err != unix.EINTR {
			return err
		}
		n, err := dev.Read(buf)
		if err != nil {
			return err
		}
		if n == 0 {
			continue
		}
		var r fuseproto.Request
		if err := fuseproto.Decode(buf[:n], &r); err != nil {
			return err
		}
		var reply []byte
		switch r.Opcode {
		case fuseproto.OpInit:
			o, err := fsclient.Negotiate(&r.Init)
			if err != nil {
				return err
			}
			reply = fuseproto.AppendInit(nil, r.Unique, &o)
		case fuseproto.OpGetattr:
			reply = fuseproto.AppendAttr(nil, r.Unique, 0, 0, &fuseproto.Attr{Ino: 1, Mode: unix.S_IFDIR | 0o755, Nlink: 2})
		case fuseproto.OpForget, fuseproto.OpBatchForget:
			continue
		default:
			reply = fuseproto.AppendError(nil, r.Unique, fuseproto.ENOSYS)
		}
		if err := dev.Write(reply); err != nil {
			return err
		}
	}
}

// Through the kernel: the mount takes allow_other and default_permissions, the device's framing
// serves the INIT and GETATTR a stat needs, and another uid stats the mount.
func TestAMountServesStatToAnotherUID(t *testing.T) {
	dir := mountDir(t)
	dev, err := Mount(dir)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- serveRoot(dev) }()
	t.Cleanup(func() {
		if err := unix.Unmount(dir, unix.MNT_DETACH); err != nil {
			t.Errorf("unmount: %v", err)
		}
		if err := <-done; !errors.Is(err, ErrGone) {
			t.Errorf("the server ended with %v", err)
		}
		_ = unix.Close(dev.Fd)
	})
	st, err := os.Stat(dir)
	if err != nil || st.Mode() != os.ModeDir|0o755 {
		t.Fatalf("stat of the mount: %v, %v", st, err)
	}
	info, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		t.Fatal(err)
	}
	var line string
	for l := range strings.SplitSeq(string(info), "\n") {
		if f := strings.Fields(l); len(f) > 4 && f[4] == filepath.Clean(dir) {
			line = l
		}
	}
	if !strings.Contains(line, "allow_other") || !strings.Contains(line, "default_permissions") || !strings.Contains(line, "fuse.rock-solid") {
		t.Fatalf("mountinfo: %q", line)
	}
	if out, err := asNobody(t, dir, "stat", "-L", "-c", "%a", "/dev/fd/3/").CombinedOutput(); err != nil || strings.TrimSpace(string(out)) != "755" {
		t.Fatalf("stat as uid 65534: %s, %v", out, err)
	}
}

// asNobody is a command run as uid 65534 holding dir as its fd 3, so that it reaches the mount by
// /dev/fd/3 whatever the permissions of the directories above it.
func asNobody(t *testing.T, dir string, args ...string) *exec.Cmd {
	fd, err := unix.Open(dir, unix.O_PATH|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	f := os.NewFile(uintptr(fd), dir)
	t.Cleanup(func() { _ = f.Close() })
	c := exec.Command("setpriv", append([]string{"--reuid=65534", "--regid=65534", "--clear-groups"}, args...)...)
	c.ExtraFiles = []*os.File{f}
	return c
}

// The kernel answers EINVAL to a reply frame it cannot take, which only an encoder bug writes:
// Serve returns it, so the mount ends. An eventfd stands in for the device, as it refuses a write
// of other than 8 bytes with EINVAL.
func TestAReplyTheKernelRefusesEndsServe(t *testing.T) {
	fd, err := unix.Eventfd(0, unix.EFD_NONBLOCK|unix.EFD_CLOEXEC)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = unix.Close(fd) })
	d := New(Config{})
	ctx := core.Context{Pool: core.NewPool([][]byte{make([]byte, 4<<20)}, 0)}
	if err := d.Attach(&ctx, Device{Fd: fd}); err != nil {
		t.Fatal(err)
	}
	d.write(make([]byte, fuseproto.OutHeaderSize))
	if _, err := d.Serve(&ctx, nil); !errors.Is(err, unix.EINVAL) {
		t.Fatalf("Serve after a refused frame: %v", err)
	}
}

// Root of a user namespace of its own holds CAP_DAC_OVERRIDE over that namespace's objects, not
// the mount's: its credentials carry no override, though its status shows the capability.
func TestAnOpenerInAnotherUserNamespaceHasNoOverride(t *testing.T) {
	c := exec.Command("sleep", "60")
	c.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags:  syscall.CLONE_NEWUSER,
		UidMappings: []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Getuid(), Size: 1}},
		GidMappings: []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Getgid(), Size: 1}},
	}
	if err := c.Start(); err != nil {
		t.Skipf("no user namespace to start a process in: %v", err)
	}
	t.Cleanup(func() { _ = c.Process.Kill(); _ = c.Wait() })
	pid := uint32(c.Process.Pid)
	var in fsclient.Creds
	if b, err := os.ReadFile("/proc/" + strconv.Itoa(c.Process.Pid) + "/status"); err != nil || !parseStatus(b, &in) || !in.Override {
		t.Fatalf("the process's status gave %+v, %v", in, err)
	}
	if cr := New(Config{}).opener(pid); cr == nil || cr.Override {
		t.Fatalf("the opener's credentials are %+v", cr)
	}
}
