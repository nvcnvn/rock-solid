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

// A mount whose device the daemon has no buffer for, or the loop refuses, unmounts it again,
// rather than leave a mount nobody serves, and says why. Needs root and RS_FUSE_DIR.
func TestAMountNobodyServesLeavesNothingMounted(t *testing.T) {
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
	var short *core.PoolExhaustedError
	for _, c := range []struct {
		name string
		k    loop.Kernel
		pool *core.Pool
		is   func(error) bool
	}{
		{"the loop refuses the device", refuseWatch{k}, core.NewPool([][]byte{make([]byte, 4<<20)}, 0), func(err error) bool { return errors.Is(err, unix.ENOSPC) }},
		{"the pool has no buffer", k, core.NewPool([][]byte{make([]byte, core.BlockSize)}, 0), func(err error) bool { return errors.As(err, &short) }},
	} {
		m := newMount(nodeConfig{Mount: 1, MountPath: mnt}, tcp.New(tcp.Config{Self: core.NodeID{Node: 2}, Seed: 1}))
		m.drv.l = loop.New(loop.Config{Node: 2}, c.k, nil, m.drv)
		if err := m.attach(&core.Context{Pool: c.pool}); !c.is(err) || mounted(mnt) {
			t.Fatalf("%s: attach: %v; %s mounted: %v", c.name, err, mnt, mounted(mnt))
		}
	}
}

// A new owner's ask to re-register comes from a storage node's replica to loop 0 of the session's
// node, where the mount runs beside the node's machine, and goes to the mount, not the machine.
func TestANewOwnersAskToReregisterGoesToTheMount(t *testing.T) {
	m := newMount(nodeConfig{Mount: 1}, tcp.New(tcp.Config{Self: core.NodeID{Node: 2}, Seed: 1}))
	pool := core.NewPool([][]byte{make([]byte, 1<<16)}, 0)
	ev := core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{Node: 3, Loop: 1}}
	if err := meta.EncodeReclaim(pool, 5, 7, 2, &ev.Msg); err != nil {
		t.Fatal(err)
	}
	if !m.takes([]core.NodeID{{Node: 1}}, &ev) {
		t.Fatal("the node's machine takes a new owner's ask to re-register")
	}
}
