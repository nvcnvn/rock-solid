package main

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/driver/tcp"
	"github.com/nvcnvn/rock-solid/internal/frontend/fuse"
	"github.com/nvcnvn/rock-solid/internal/loop"
	"github.com/nvcnvn/rock-solid/internal/meta"
)

// mounted reports whether dir is a mount point in this process's namespace.
func mounted(dir string) bool {
	info, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return false
	}
	for l := range strings.SplitSeq(string(info), "\n") {
		if f := strings.Fields(l); len(f) > 4 && f[4] == dir {
			return true
		}
	}
	return false
}

// Through the kernel, on a control plane and one storage node over a file-backed disk mounting
// volume 1: a file created, written and closed reads back the same bytes from another process and
// from another uid, `ls` lists it, and once unlinked it is gone. Needs root and RS_FUSE_DIR.
func TestAMountCreatesWritesReadsAndUnlinksThroughTheKernel(t *testing.T) {
	base0 := os.Getenv("RS_FUSE_DIR")
	switch {
	case base0 == "":
		t.Skip("RS_FUSE_DIR names no directory to mount under")
	case os.Geteuid() != 0:
		t.Skip("mounting with allow_other needs root")
	}
	mnt, err := os.MkdirTemp(base0, "mnt")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(mnt) })
	dir := testDir(t)
	base := freeBase(t, 3)
	pc, _ := runCtlOn(t, dir, base, true)
	dev := filepath.Join(dir, "mount-dev")
	device(t, dev, false)
	var out output
	startNode(t, fmt.Sprintf("-node 2 -ctl 1=127.0.0.1:%d -group %d -listen 127.0.0.1:%d -device %s -mount 1:%s", base, ctlGroup, base+1, dev, mnt), &out)
	t.Cleanup(func() {
		if mounted(mnt) {
			if err := unix.Unmount(mnt, unix.MNT_DETACH); err != nil {
				t.Errorf("unmount %s: %v", mnt, err)
			}
		}
	})
	if !within(60*time.Second, func() bool {
		return onCtl(t, pc, func(_ *core.Context, c *meta.Control) bool { return hosts(c, 2, 1, 1) })
	}) {
		t.Fatalf("the node never hosted the first data group's voter; the node said:\n%s", out.String())
	}
	if code, said := admin(pc, "pool", "1", "-replicas", "1", "-min-live", "1", "-failure-domain", "disk", "-pg-count", "8"); code != 0 {
		t.Fatalf("the pool command exited %d: %s", code, said)
	}
	if code, said := volume(base, base+1, "-pool", "1", "-size", "1073741824"); code != 0 || !strings.Contains(said, "volume 1 created") {
		t.Fatalf("the create exited %d: %s\nthe node said:\n%s", code, said, out.String())
	}
	if !within(60*time.Second, func() bool { return mounted(mnt) }) {
		t.Fatalf("the node never mounted %s; it said:\n%s", mnt, out.String())
	}
	f := filepath.Join(mnt, "f")
	data := bytes.Repeat([]byte("rock-solid "), 30000)
	if err := os.WriteFile(f, data, 0o644); err != nil {
		t.Fatalf("write %s: %v; the node said:\n%s", f, err, out.String())
	}
	if got, err := exec.Command("cat", f).Output(); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("another process read %d bytes of %d: %v", len(got), len(data), err)
	}
	if got, err := asNobody(t, mnt, "cat", "/dev/fd/3/f").Output(); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("uid 65534 read %d bytes of %d: %v", len(got), len(data), err)
	}
	if es, err := os.ReadDir(mnt); err != nil || len(es) != 1 || es[0].Name() != "f" {
		t.Fatalf("ls: %v, %v", es, err)
	}
	if err := os.Remove(f); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(f); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("stat after unlink: %v", err)
	}
	if es, err := os.ReadDir(mnt); err != nil || len(es) != 0 {
		t.Fatalf("ls after unlink: %v, %v", es, err)
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

// refuseWatch is an epoll kernel that watches no descriptor, so that the loop refuses each one.
type refuseWatch struct{ loop.Kernel }

func (refuseWatch) Watch(int, uint64) error { return unix.ENOSPC }

// A mount whose device the loop refuses unmounts it again, rather than leave a mount nobody
// serves, and says why. Needs root and RS_FUSE_DIR.
func TestAMountWhoseDeviceTheLoopRefusesLeavesNothingMounted(t *testing.T) {
	base := os.Getenv("RS_FUSE_DIR")
	switch {
	case base == "":
		t.Skip("RS_FUSE_DIR names no directory to mount under")
	case os.Geteuid() != 0:
		t.Skip("mounting with allow_other needs root")
	}
	mnt, err := os.MkdirTemp(base, "mnt")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if mounted(mnt) {
			_ = unix.Unmount(mnt, unix.MNT_DETACH)
		}
		_ = os.Remove(mnt)
	})
	k, err := loop.NewEpoll("/nonexistent/vmclock0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(k.Close)
	m := newMount(nodeConfig{Mount: 1, MountPath: mnt}, tcp.New(tcp.Config{Self: core.NodeID{Node: 2}, Seed: 1}))
	m.drv.l = loop.New(loop.Config{Node: 2}, refuseWatch{k}, nil, m.drv)
	if err := m.attach(); !errors.Is(err, unix.ENOSPC) || mounted(mnt) {
		t.Fatalf("attach: %v; %s mounted: %v", err, mnt, mounted(mnt))
	}
}

// serveTimes is a loop's machine for a mount alone, keeping when the mount's device was reported readable.
type serveTimes struct {
	m  *mount
	at []core.Tick
}

func (s *serveTimes) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	if ev.Kind == core.EvTimer && ev.Timer == fuse.ReadyTimer {
		s.at = append(s.at, ctx.Now)
	}
	return s.m.onEvent(ctx, ev, out)
}

// A mount whose pool has no buffer to read a request into leaves its readable device unread for
// poolWait, rather than wake its loop every pass, and then reads it again.
func TestAMountShortOfBuffersWaitsBeforeReadingAgain(t *testing.T) {
	k, err := loop.NewEpoll("/nonexistent/vmclock0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(k.Close)
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_DGRAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = unix.Close(fds[0]); _ = unix.Close(fds[1]) })
	if _, err := unix.Write(fds[0], []byte("a request")); err != nil {
		t.Fatal(err)
	}
	td := tcp.New(tcp.Config{Self: core.NodeID{Node: 2}, Seed: 1})
	s := &serveTimes{m: newMount(nodeConfig{Mount: 1}, td)}
	l := loop.New(loop.Config{Node: 2, Pool: core.NewPool([][]byte{make([]byte, 4096)}, 0)}, k, s, s.m.drv)
	td.L, s.m.drv.l = l, l
	c, err := l.AddConn(fds[1], true)
	if err != nil {
		t.Fatal(err)
	}
	l.Own(c)
	s.m.d.Attach(fuse.Device{Fd: fds[1]})
	s.m.drv.c, s.m.drv.on = c, true
	for end := time.Now().Add(time.Second); len(s.at) < 2 && time.Now().Before(end); {
		l.Pass()
	}
	if len(s.at) < 2 || s.at[1]-s.at[0] < poolWait {
		t.Fatalf("the device was read at %v, %d µs apart at least", s.at, poolWait)
	}
}
