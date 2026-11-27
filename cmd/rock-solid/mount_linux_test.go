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
