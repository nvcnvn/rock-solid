package main

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/driver/tcp"
	"github.com/nvcnvn/rock-solid/internal/driver/uring"
	"github.com/nvcnvn/rock-solid/internal/meta"
)

// TestNodeProcess is the storage node process the reviews start and kill: the test binary run
// again with its flags in RS_NODE_ARGS.
func TestNodeProcess(t *testing.T) {
	args := os.Getenv("RS_NODE_ARGS")
	if args == "" {
		t.Skip("run by the node reviews as the storage node process")
	}
	cfg, err := parseNode(strings.Fields(args))
	if err != nil {
		t.Fatal(err)
	}
	p, err := newNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Run(); err != nil {
		t.Fatal(err)
	}
}

// A start that fails returns its error: a node whose port is taken, and a voter whose disk file
// is missing.
func TestAStartThatFailsReturnsItsError(t *testing.T) {
	fd, port, err := tcp.ListenTCP(netip.MustParseAddrPort("127.0.0.1:0"))
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	taken := netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(port))
	free := netip.MustParseAddrPort("127.0.0.1:0")
	if _, err := newNode(nodeConfig{Node: 2, Ctls: []voter{{1, free}}, Group: 1, Listen: taken, Devices: []string{filepath.Join(t.TempDir(), "dev")}, Class: "ssd", Pool: 1}); err == nil {
		t.Error("a node started on a port taken")
	}
	if _, err := newCtl(ctlConfig{Node: 1, Voters: []voter{{1, free}}, Group: 1, Listen: free, Disk: filepath.Join(t.TempDir(), "missing")}); err == nil {
		t.Error("a voter started on a missing disk")
	}
}

// output is a child's output, read while it writes.
type output struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (o *output) Write(p []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.b.Write(p)
}

func (o *output) String() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.b.String()
}

// startNode starts the node process with args, its output going to out.
func startNode(t *testing.T, args string, out *output) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestNodeProcess$", "-test.v")
	cmd.Env = append(os.Environ(), "RS_NODE_ARGS="+args)
	cmd.Stdout, cmd.Stderr = out, out
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	return cmd
}

func testDir(t *testing.T) string {
	if dir := os.Getenv("RS_URING_DIR"); dir != "" {
		return dir
	}
	return t.TempDir()
}

// runCtl runs a single-voter control plane of group ctlGroup in the test process, on a disk file
// in dir, listening on a port of its own.
func runCtl(t *testing.T, dir string) *ctl {
	disk := filepath.Join(dir, "node-ctl-disk")
	t.Cleanup(func() { _ = os.Remove(disk) })
	local := netip.MustParseAddrPort("127.0.0.1:0")
	p, err := newCtl(ctlConfig{Node: 1, Voters: []voter{{1, local}}, Group: ctlGroup, Listen: local, Disk: disk, Format: true, DiskBytes: 512 << 20, MaxReplayBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- p.Run() }()
	t.Cleanup(func() {
		p.Stop()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	return p
}

// onCtl runs f on the voter's loop with its Control, nil while it does not lead.
func onCtl[T any](t *testing.T, p *ctl, f func(ctx *core.Context, c *meta.Control) T) T {
	t.Helper()
	ch := make(chan T, 1)
	p.call(func(ctx *core.Context, h *meta.CtlHost) { ch <- f(ctx, h.Control()) })
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatal("the voter's loop ran no call")
	}
	panic("unreachable")
}

// within reports whether cond holds within d, trying it every 100 ms.
func within(d time.Duration, cond func() bool) bool {
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(100 * time.Millisecond) {
		if cond() {
			return true
		}
	}
	return false
}

// freeBase is the first of n ports, each free when probed, below the kernel's ephemeral range, from
// which outbound connections draw their ports, so a process started again finds its ports free.
func freeBase(t *testing.T, n int) int {
	ephemeral := 32768
	if b, err := os.ReadFile("/proc/sys/net/ipv4/ip_local_port_range"); err == nil {
		if f := strings.Fields(string(b)); len(f) == 2 {
			if lo, err := strconv.Atoi(f[0]); err == nil {
				ephemeral = lo
			}
		}
	}
	const low = 10000
	if ephemeral-n <= low {
		t.Fatalf("no room for %d ports below the ephemeral range at %d", n, ephemeral)
	}
	for range 100 {
		if base := low + rand.IntN(ephemeral-n-low); free(base, n) {
			return base
		}
	}
	t.Fatalf("no %d ports free below %d", n, ephemeral)
	return 0
}

// free reports whether ports base to base+n-1 can each be listened on.
func free(base, n int) bool {
	var fds []int
	defer func() {
		for _, fd := range fds {
			_ = unix.Close(fd)
		}
	}()
	for port := base; port < base+n; port++ {
		fd, _, err := tcp.ListenTCP(netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(port)))
		if err != nil {
			return false
		}
		fds = append(fds, fd)
	}
	return true
}

// device makes a device file of 1 GiB at path, its block 0 junk if junk is set.
func device(t *testing.T, path string, junk bool) {
	t.Cleanup(func() { _ = os.Remove(path) })
	_ = os.Remove(path)
	if err := uring.CreateFile(path, 1<<30); err != nil {
		t.Fatal(err)
	}
	if junk {
		f, err := os.OpenFile(path, os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteAt(bytes.Repeat([]byte{0xa5}, core.BlockSize), 0); err != nil {
			t.Fatal(err)
		}
		_ = f.Close()
	}
}

var epochLine = regexp.MustCompile(`device (0x[0-9a-f]+) holds placement epoch (\d+)`)

// epochs is the newest placement epoch the node's output says each device holds.
func epochs(out string) map[uint64]uint64 {
	es := map[uint64]uint64{}
	for _, m := range epochLine.FindAllStringSubmatch(out, -1) {
		d, _ := strconv.ParseUint(m[1], 0, 64)
		e, _ := strconv.ParseUint(m[2], 10, 64)
		es[d] = max(es[d], e)
	}
	return es
}

// upDisks is the IDs of node's disks the record holds up.
func upDisks(c *meta.Control, node uint32) []uint32 {
	var ids []uint32
	for _, d := range c.Disks() {
		if d.Node == node && d.Up && !d.Gone {
			ids = append(ids, d.ID)
		}
	}
	return ids
}

// A storage node process over TCP: its two blank devices are formatted, reported and join the
// record up; a device with no valid superblock and one that cannot be opened reach the leader's
// FailedDevices; and a pool created at the leader gives a placement table that reaches the
// replica of every device, and the leader has each replica's fill from its heartbeats, which
// start once the membership the control plane sends names the replica's log.
func TestANodeStartsItsDevicesAndTakesItsDisksAndTablesOverTCP(t *testing.T) {
	dir := testDir(t)
	p := runCtl(t, dir)
	a, b, bad, missing := filepath.Join(dir, "node-dev-a"), filepath.Join(dir, "node-dev-b"), filepath.Join(dir, "node-dev-bad"), filepath.Join(dir, "node-dev-missing")
	device(t, a, false)
	device(t, b, false)
	device(t, bad, true)
	base := freeBase(t, 4)
	var out output
	startNode(t, fmt.Sprintf("-node 2 -ctl 1=127.0.0.1:%d -group %d -listen 127.0.0.1:%d -device %s -device %s -device %s -device %s",
		p.Port, ctlGroup, base, a, b, bad, missing), &out)
	failed := []meta.FailedDevice{{Node: 2, Device: deviceID(bad)}, {Node: 2, Device: deviceID(missing)}}
	if !within(60*time.Second, func() bool {
		return onCtl(t, p, func(_ *core.Context, c *meta.Control) bool {
			return c != nil && len(upDisks(c, 2)) == 2 && slices.Contains(c.FailedDevices(), failed[0]) && slices.Contains(c.FailedDevices(), failed[1])
		})
	}) {
		t.Fatalf("the node's disks never came up, or its bad or missing device was never reported failed; the node said:\n%s", out.String())
	}
	replicas, pgs := 1, uint32(16)
	if err := onCtl(t, p, func(_ *core.Context, c *meta.Control) error {
		return c.SetPool(1, chunkstore.PoolSpec{Replicas: &replicas, PGCount: &pgs}, c.AppendDisks(nil))
	}); err != nil {
		t.Fatal(err)
	}
	var epoch uint64
	if !within(30*time.Second, func() bool {
		epoch = onCtl(t, p, func(_ *core.Context, c *meta.Control) uint64 {
			if c == nil || len(c.Pools()) == 0 {
				return 0
			}
			return c.Table().Epoch
		})
		return epoch != 0
	}) {
		t.Fatal("the pool never committed")
	}
	if !within(30*time.Second, func() bool {
		return onCtl(t, p, func(_ *core.Context, c *meta.Control) bool {
			if c == nil {
				return false
			}
			_, _, ok := c.Capacity(1)
			return ok
		})
	}) {
		t.Fatalf("the leader never had a fill from each replica's heartbeat; the node said:\n%s", out.String())
	}
	if !within(30*time.Second, func() bool {
		es := epochs(out.String())
		return es[deviceID(a)] >= epoch && es[deviceID(b)] >= epoch
	}) {
		t.Fatalf("the replicas never held epoch %d; the node said:\n%s", epoch, out.String())
	}
}
