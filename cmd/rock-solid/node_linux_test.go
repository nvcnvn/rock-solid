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
	"github.com/nvcnvn/rock-solid/internal/loop"
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
	p, _ := runCtlOn(t, dir, 0, true)
	return p
}

// runCtlOn is runCtl listening on port, formatting its disk only if format, and returns what stops
// it as well.
func runCtlOn(t *testing.T, dir string, port int, format bool) (*ctl, func()) {
	disk := filepath.Join(dir, "node-ctl-disk")
	t.Cleanup(func() { _ = os.Remove(disk) })
	local := netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(port))
	p, err := newCtl(ctlConfig{Node: 1, Voters: []voter{{1, local}}, Group: ctlGroup, Listen: local, Disk: disk, Format: format, DiskBytes: 512 << 20, MaxReplayBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- p.Run() }()
	stop := sync.OnceFunc(func() {
		p.Stop()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	t.Cleanup(stop)
	return p, stop
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

// dataGroup is the first data group, which the control-plane leader adds once a metadata node reports.
const dataGroup = ctlGroup + 1

// hosts reports whether node last reported running the first data group's replica id in a
// configuration of voters alone.
func hosts(c *meta.Control, node uint32, id uint64, voters ...uint64) bool {
	if c == nil {
		return false
	}
	return slices.ContainsFunc(c.Reported(node), func(r meta.ReplicaReport) bool {
		return r.Group == dataGroup && r.ID == id && !r.Joint && slices.Equal(slices.Sorted(slices.Values(r.Voters)), voters)
	})
}

// A storage node hosts the MetaStore replica the control plane's shard map places on its disk, the
// one voter of the first data group, which the leader adds once the node reports, and reports it
// running; killed and started again, the node restores it from its disk with the settings the map
// gives, and it runs again.
func TestANodeHostsTheMetaStoreReplicaItsShardMapPlacesAcrossAKill(t *testing.T) {
	dir := testDir(t)
	p := runCtl(t, dir)
	a := filepath.Join(dir, "node-meta-dev")
	device(t, a, false)
	t.Cleanup(func() { _ = os.Remove(a) })
	base := freeBase(t, 2)
	args := fmt.Sprintf("-node 2 -ctl 1=127.0.0.1:%d -group %d -listen 127.0.0.1:%d -device %s", p.Port, ctlGroup, base, a)
	var out output
	cmd := startNode(t, args, &out)
	if !within(60*time.Second, func() bool {
		return onCtl(t, p, func(_ *core.Context, c *meta.Control) bool { return c != nil && len(upDisks(c, 2)) == 1 })
	}) {
		t.Fatalf("the node's disk never came up; the node said:\n%s", out.String())
	}
	runs := func() bool {
		return within(60*time.Second, func() bool {
			return onCtl(t, p, func(_ *core.Context, c *meta.Control) bool { return hosts(c, 2, 1, 1) && len(c.Reported(2)) == 1 })
		})
	}
	if !runs() {
		t.Fatalf("the node never reported the replica the map placed; the node said:\n%s", out.String())
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	if !within(30*time.Second, func() bool {
		return onCtl(t, p, func(_ *core.Context, c *meta.Control) bool { return c != nil && len(c.Reported(2)) == 0 })
	}) {
		t.Fatal("the killed node's report outlived it")
	}
	var again output
	startNode(t, args, &again)
	if !runs() {
		t.Fatalf("the restarted node never reported the replica its disk holds; the node said:\n%s", again.String())
	}
}

// The first data group's two voters, on two storage nodes, elect and take a Txn after a cold start
// of the whole cluster: both nodes start again before the control plane, so the first membership
// each gets, with which its device restores its replica, names no disk of the other, and the
// replicas learn each other's loop from the memberships that follow.
func TestAGroupOnTwoNodesTakesATxnAfterAColdStart(t *testing.T) {
	dir := testDir(t)
	base := freeBase(t, 5)
	p, stop := runCtlOn(t, dir, base, true)
	starts := func(fresh bool) ([]*exec.Cmd, []*output) {
		var cmds []*exec.Cmd
		var outs []*output
		for i, n := range []int{2, 3} {
			dev := filepath.Join(dir, fmt.Sprintf("node-cold-dev%d", n))
			if fresh {
				device(t, dev, false)
			}
			args := fmt.Sprintf("-node %d -ctl 1=127.0.0.1:%d -group %d -listen 127.0.0.1:%d -peers %d=127.0.0.1:%d -device %s",
				n, base, ctlGroup, base+1+2*i, 5-n, base+3-2*i, dev)
			outs = append(outs, &output{})
			cmds = append(cmds, startNode(t, args, outs[i]))
		}
		return cmds, outs
	}
	said := func(outs []*output) string { return outs[0].String() + "\n---- node 3:\n" + outs[1].String() }
	cmds, outs := starts(true)
	if !within(60*time.Second, func() bool {
		return onCtl(t, p, func(_ *core.Context, c *meta.Control) bool {
			return c != nil && len(upDisks(c, 2)) == 1 && len(upDisks(c, 3)) == 1
		})
	}) {
		t.Fatalf("the nodes' disks never came up; node 2 said:\n%s", said(outs))
	}
	if !within(90*time.Second, func() bool {
		return onCtl(t, p, func(_ *core.Context, c *meta.Control) bool {
			if c == nil || len(c.ShardMap().Groups) != 2 {
				return false
			}
			g := c.ShardMap().Groups[1]
			if len(g.Voters) != 2 {
				return false
			}
			ids := slices.Sorted(slices.Values([]uint64{g.Voters[0].ID, g.Voters[1].ID}))
			return hosts(c, g.Voters[0].Node, g.Voters[0].ID, ids...) && hosts(c, g.Voters[1].Node, g.Voters[1].ID, ids...)
		})
	}) {
		t.Fatalf("the first data group never ran with a voter on each node; node 2 said:\n%s", said(outs))
	}
	x := newTxner(t, dataGroup, base, core.NodeID{Node: 2, Loop: 1}, core.NodeID{Node: 3, Loop: 1})
	if !x.applies(60 * time.Second) {
		t.Fatalf("the group never took a Txn; node 2 said:\n%s", said(outs))
	}
	for _, cmd := range cmds {
		if err := cmd.Process.Kill(); err != nil {
			t.Fatal(err)
		}
		_ = cmd.Wait()
	}
	stop()
	_, outs = starts(false)
	time.Sleep(time.Second)
	runCtlOn(t, dir, base, false)
	if !x.applies(90 * time.Second) {
		t.Fatalf("after the cold start the group never took a Txn; node 2 said:\n%s", said(outs))
	}
}

// txner is a client's loop as far as a test needs: every 100 ms it sends a Txn putting a key to
// group's replica at each of at, and keeps whether one was applied since applies began.
type txner struct {
	t       *testing.T
	l       *loop.Loop
	group   uint64
	at      []core.NodeID
	token   uint64
	applied bool
}

func (x *txner) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	switch ev.Kind {
	case core.EvTimer:
		for _, at := range x.at {
			x.token++
			a := core.Action{Kind: core.ActNetSend, Peer: at}
			tx := meta.Txn{Ops: []meta.Op{meta.Put([]byte{meta.SpaceVolume, 'k'}, []byte("v"))}}
			if err := meta.EncodeTxn(ctx.Pool, x.group, x.token, &tx, &a.Msg); err != nil {
				x.t.Error(err)
			}
			out = append(out, a)
		}
		out = append(out, core.Action{Kind: core.ActSetTimer, Timer: probeTimer, At: ctx.Now + 100_000})
	case core.EvNetRecv:
		var a meta.Answer
		if group, kind, _, ok := meta.Peek(&ev.Msg); ok && group == x.group && kind == meta.KindAnswer && meta.DecodeAnswer(ctx.Pool, &ev.Msg, &a) == nil {
			x.applied = x.applied || a.Err == nil
			return out
		}
		if ev.Msg.Buf != core.NoBuf {
			ctx.Pool.Free(ev.Msg.Buf)
		}
	}
	return out
}

// newTxner starts a txner on node 9, reaching loop k of node n at port base + 2(n-2) + 1 + k.
func newTxner(t *testing.T, group uint64, base int, at ...core.NodeID) *txner {
	k, err := loop.NewEpoll("/nonexistent/vmclock0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(k.Close)
	pool, err := loop.NewPool(16<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	x := &txner{t: t, group: group, at: at}
	td := tcp.New(tcp.Config{Self: core.NodeID{Node: 9}, Seed: 9, Dial: func(id core.NodeID) (int, error) {
		return tcp.DialTCP(netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(base+2*(int(id.Node)-2)+1+int(id.Loop))))
	}})
	x.l = loop.New(loop.Config{Node: 9, Pool: pool}, k, x, td)
	td.L = x.l
	x.l.Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: probeTimer})
	return x
}

// applies runs the txner until a Txn sent from now on is applied, within d.
func (x *txner) applies(d time.Duration) bool {
	x.applied = false
	for end := time.Now().Add(d); time.Now().Before(end) && !x.applied; {
		x.l.Pass()
	}
	return x.applied
}
