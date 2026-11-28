package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/meta"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

const (
	sharedFiles = 4
	fileBytes   = 1 << 20
	pieceBytes  = 128 << 10
)

// recorder keeps the history of every process on both mounts, as sim.CloseToOpen reads it, on
// the test process's monotonic clock.
type recorder struct {
	mu  sync.Mutex
	t0  time.Time
	ops []sim.Op
}

// do runs f as client's operation with input in, recording its invocation before and its output
// after, the start rounded down and the end rounded up.
func (r *recorder) do(client uint32, in [sim.OpWords]uint64, f func() [sim.OpWords]uint64) [sim.OpWords]uint64 {
	r.mu.Lock()
	i := len(r.ops)
	r.ops = append(r.ops, sim.Op{Client: client, Invoke: core.Tick(time.Since(r.t0) / time.Microsecond), Input: in})
	r.mu.Unlock()
	out := f()
	r.mu.Lock()
	op := &r.ops[i]
	op.Complete, op.Done, op.Output = core.Tick((time.Since(r.t0)+time.Microsecond-1)/time.Microsecond), true, out
	r.mu.Unlock()
	return out
}

// returns reports whether f returns within d; one that does not is left to fail once the test's
// cleanup kills the nodes serving its mount.
func returns(d time.Duration, f func()) bool {
	done := make(chan struct{})
	go func() {
		f()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(d):
		return false
	}
}

func errnoOf(err error) uint64 {
	var e unix.Errno
	switch {
	case err == nil:
		return 0
	case errors.As(err, &e):
		return uint64(e)
	}
	return uint64(unix.EIO)
}

// write writes shared file f on mnt through a handle of its own, every word id<<32 | its
// offset/8, and reports whether every write and the close succeeded.
func (r *recorder) write(client uint32, mnt string, f int, id uint64) bool {
	ino := uint64(f + 1)
	var fd int
	if out := r.do(client, [sim.OpWords]uint64{sim.FileOpen | ino<<8, unix.O_WRONLY}, func() [sim.OpWords]uint64 {
		var err error
		fd, err = unix.Open(filepath.Join(mnt, fmt.Sprint("f", f)), unix.O_WRONLY|unix.O_CREAT|unix.O_CLOEXEC, 0o644)
		return [sim.OpWords]uint64{errnoOf(err), ino}
	}); out[0] != 0 {
		return false
	}
	ok := true
	buf := make([]byte, pieceBytes)
	for off := 0; off < fileBytes && ok; off += pieceBytes {
		for i := 0; i < len(buf); i += 8 {
			binary.LittleEndian.PutUint64(buf[i:], id<<32|uint64(off+i)/8)
		}
		out := r.do(client, [sim.OpWords]uint64{sim.FileWrite | ino<<8, uint64(off), pieceBytes, binary.LittleEndian.Uint64(buf)}, func() [sim.OpWords]uint64 {
			n, err := unix.Pwrite(fd, buf, int64(off))
			return [sim.OpWords]uint64{errnoOf(err), uint64(max(n, 0))}
		})
		ok = out[0] == 0 && out[1] == pieceBytes
	}
	out := r.do(client, [sim.OpWords]uint64{sim.FileClose | ino<<8}, func() [sim.OpWords]uint64 {
		return [sim.OpWords]uint64{errnoOf(unix.Close(fd))}
	})
	return ok && out[0] == 0
}

// read reads shared file f on mnt through a handle of its own, recording what each read's words
// say as sim.Process does, and returns the bytes and whether every read was whole and the close
// succeeded.
func (r *recorder) read(client uint32, mnt string, f int) ([]byte, bool) {
	ino := uint64(f + 1)
	var fd int
	if out := r.do(client, [sim.OpWords]uint64{sim.FileOpen | ino<<8, unix.O_RDONLY}, func() [sim.OpWords]uint64 {
		var err error
		fd, err = unix.Open(filepath.Join(mnt, fmt.Sprint("f", f)), unix.O_RDONLY|unix.O_CLOEXEC, 0)
		return [sim.OpWords]uint64{errnoOf(err), ino}
	}); out[0] != 0 {
		return nil, false
	}
	data := make([]byte, fileBytes)
	ok := true
	for off := 0; off < fileBytes && ok; off += pieceBytes {
		out := r.do(client, [sim.OpWords]uint64{sim.FileRead | ino<<8, uint64(off), pieceBytes}, func() [sim.OpWords]uint64 {
			n, err := unix.Pread(fd, data[off:off+pieceBytes], int64(off))
			n = max(n, 0)
			least, zero := ^uint64(0), uint64(off+n)
			for i := 0; i+8 <= n; i += 8 {
				at := uint64(off + i)
				switch v := binary.LittleEndian.Uint64(data[off+i:]); {
				case v == 0:
					zero = min(zero, at)
				case uint32(v) != uint32(at/8):
					least = 0
				default:
					least = min(least, v>>32)
				}
			}
			if n > 0 {
				err = nil
			}
			return [sim.OpWords]uint64{errnoOf(err), uint64(n), least, zero}
		})
		ok = out[0] == 0 && out[1] == pieceBytes
	}
	out := r.do(client, [sim.OpWords]uint64{sim.FileClose | ino<<8}, func() [sim.OpWords]uint64 {
		return [sim.OpWords]uint64{errnoOf(unix.Close(fd))}
	})
	return data, ok && out[0] == 0
}

// Through the kernel, over TCP, a control plane and four node processes: storage nodes 2, 3 and 4,
// each a disk of a pool of three replicas and a voter of the first data group, and client-only
// node 5, with volume 1 mounted on node 2 and on node 5. A file written and closed on either
// mount reads back the same bytes on the other. Then three processes on each mount write and
// read four files, one writer of a file at a time, while the group's leader is paused until
// another voter leads, and then the leader, or node 3 if node 2 leads, is killed and started
// again; the history of both mounts passes close-to-open. Needs root and RS_FUSE_DIR.
func TestAMountOnEachOfTwoNodesKeepsCloseToOpenAcrossAPauseAndAKill(t *testing.T) {
	base0 := os.Getenv("RS_FUSE_DIR")
	switch {
	case base0 == "":
		t.Skip("RS_FUSE_DIR names no directory to mount under")
	case os.Geteuid() != 0:
		t.Skip("mounting with allow_other needs root")
	}
	var stopped atomic.Bool
	var wg sync.WaitGroup
	mnts := map[int]string{}
	for _, n := range []int{2, 5} {
		mnt, err := os.MkdirTemp(base0, "mnt")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Remove(mnt) })
		mnts[n] = mnt
	}
	t.Cleanup(wg.Wait)
	dir := testDir(t)
	base := freeBase(t, 8)
	pc, _ := runCtlOn(t, dir, base, true)
	port := func(n int) int { return base + 1 + 2*(n-2) }
	storage := []int{2, 3, 4}
	peers := func(self int) string {
		var ps []string
		for _, n := range storage {
			if n != self {
				ps = append(ps, fmt.Sprintf("%d=127.0.0.1:%d", n, port(n)))
			}
		}
		return strings.Join(ps, ",")
	}
	ctlArgs := fmt.Sprintf("-ctl 1=127.0.0.1:%d -group %d", base, ctlGroup)
	args := map[int]string{5: fmt.Sprintf("-node 5 %s -listen 127.0.0.1:%d -peers %s -role client -mount 1:%s", ctlArgs, port(5), peers(5), mnts[5])}
	for _, n := range storage {
		dev := filepath.Join(dir, fmt.Sprintf("mounts-dev%d", n))
		device(t, dev, false)
		args[n] = fmt.Sprintf("-node %d %s -listen 127.0.0.1:%d -peers %s -device %s", n, ctlArgs, port(n), peers(n), dev)
	}
	args[2] += " -mount 1:" + mnts[2]
	outs, cmds := map[int]*output{}, map[int]*exec.Cmd{}
	start := func(n int) {
		outs[n] = &output{}
		cmds[n] = startNode(t, args[n], outs[n])
	}
	for _, n := range []int{2, 3, 4, 5} {
		start(n)
	}
	t.Cleanup(func() {
		for _, mnt := range mnts {
			if mounted(mnt) {
				if err := unix.Unmount(mnt, unix.MNT_DETACH); err != nil {
					t.Errorf("unmount %s: %v", mnt, err)
				}
			}
		}
	})
	t.Cleanup(func() { stopped.Store(true) })
	said := func() string {
		var b strings.Builder
		for _, n := range []int{2, 3, 4, 5} {
			s := outs[n].String()
			fmt.Fprintf(&b, "---- node %d:\n%s\n", n, s[max(0, len(s)-6000):])
		}
		return b.String()
	}
	if !within(60*time.Second, func() bool {
		return onCtl(t, pc, func(_ *core.Context, c *meta.Control) bool {
			return c != nil && len(upDisks(c, 2)) == 1 && len(upDisks(c, 3)) == 1 && len(upDisks(c, 4)) == 1
		})
	}) {
		t.Fatalf("the storage nodes' disks never came up:\n%s", said())
	}
	if code, out := admin(pc, "pool", "1", "-replicas", "3", "-min-live", "2", "-failure-domain", "disk", "-pg-count", "8"); code != 0 {
		t.Fatalf("the pool command exited %d: %s", code, out)
	}
	var vout bytes.Buffer
	if code := runVolume([]string{"volume", "create", "-ctl", fmt.Sprintf("1=127.0.0.1:%d", base), "-group", fmt.Sprint(ctlGroup),
		"-peers", peers(0), "-timeout", "90s", "-pool", "1", "-size", "1073741824"}, &vout, &vout); code != 0 || !strings.Contains(vout.String(), "volume 1 created") {
		t.Fatalf("the create exited %d: %s\n%s", code, vout.String(), said())
	}
	if !within(90*time.Second, func() bool { return mounted(mnts[2]) && mounted(mnts[5]) }) {
		t.Fatalf("the nodes never mounted both of %v:\n%s", mnts, said())
	}

	pool := core.NewPool([][]byte{make([]byte, 1<<20)}, 0)
	var token uint64
	leader := func(d time.Duration, nodes ...int) int {
		tx := meta.Txn{Ops: []meta.Op{meta.Put([]byte{meta.SpaceVolume, 'k'}, []byte("v"))}}
		for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(100 * time.Millisecond) {
			for _, n := range nodes {
				token++
				addr := netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(port(n)+1))
				if a, _, err := tryTxn(pool, addr, core.NodeID{Node: 1<<31 | 9}, dataGroup, token, &tx, time.Now().Add(time.Second)); err == nil && a.Err == nil {
					return n
				}
			}
		}
		return 0
	}
	others := func(x int) []int {
		var ns []int
		for _, n := range storage {
			if n != x {
				ns = append(ns, n)
			}
		}
		return ns
	}

	r := &recorder{t0: time.Now()}
	var id atomic.Uint64
	for f := range sharedFiles {
		w, o := mnts[2], mnts[5]
		if f%2 == 1 {
			w, o = o, w
		}
		i := id.Add(1)
		var wrote, whole bool
		var got []byte
		if !returns(time.Minute, func() {
			if wrote = r.write(uint32(100+f), w, f, i); wrote {
				got, whole = r.read(uint32(100+f), o, f)
			}
		}) {
			t.Fatalf("writing f%d on %s and reading it on %s took a minute:\n%s", f, w, o, said())
		}
		want := make([]byte, fileBytes)
		for j := 0; j < fileBytes; j += 8 {
			binary.LittleEndian.PutUint64(want[j:], i<<32|uint64(j)/8)
		}
		if !wrote || !whole || !bytes.Equal(got, want) {
			t.Fatalf("f%d written and closed on %s (%v) read back otherwise on %s (whole: %v):\n%s", f, w, wrote, o, whole, said())
		}
	}

	var locks [sharedFiles]sync.RWMutex
	var writes, reads [2]atomic.Int64
	var gaveUp atomic.Int64
	for m, n := range []int{2, 5} {
		for k := range 3 {
			client := uint32(10*n + k)
			rng := rand.New(rand.NewPCG(uint64(client), 1))
			wg.Go(func() {
				for !stopped.Load() {
					f := rng.IntN(sharedFiles)
					if rng.IntN(2) == 0 {
						locks[f].RLock()
						if _, ok := r.read(client, mnts[n], f); ok {
							reads[m].Add(1)
						}
						locks[f].RUnlock()
						continue
					}
					locks[f].Lock()
					for end := time.Now().Add(90 * time.Second); ; time.Sleep(100 * time.Millisecond) {
						if r.write(client, mnts[n], f, id.Add(1)) {
							writes[m].Add(1)
							break
						}
						if stopped.Load() || time.Now().After(end) {
							gaveUp.Add(1)
							break
						}
					}
					locks[f].Unlock()
				}
			})
		}
	}

	time.Sleep(5 * time.Second)
	x := leader(30*time.Second, storage...)
	if x == 0 {
		t.Fatalf("no voter led the first data group:\n%s", said())
	}
	if err := cmds[x].Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	y := leader(8*time.Second, others(x)...)
	if err := cmds[x].Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	if y == 0 {
		t.Fatalf("no other voter led while node %d, the leader, was paused:\n%s", x, said())
	}
	t.Logf("node %d led; paused, node %d took over", x, y)
	time.Sleep(3 * time.Second)
	z := leader(30*time.Second, storage...)
	if z == 0 {
		t.Fatalf("no voter led after node %d resumed:\n%s", x, said())
	}
	k := z
	if k == 2 {
		k = 3
	}
	if err := cmds[k].Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmds[k].Wait()
	if w := leader(30*time.Second, others(k)...); w == 0 {
		t.Fatalf("no voter led with node %d killed:\n%s", k, said())
	} else {
		t.Logf("node %d led; node %d killed, node %d leads", z, k, w)
	}
	time.Sleep(3 * time.Second)
	start(k)
	if leader(60*time.Second, storage...) == 0 {
		t.Fatalf("no voter led once node %d started again:\n%s", k, said())
	}
	before := writes[0].Load() + writes[1].Load()
	time.Sleep(5 * time.Second)
	stopped.Store(true)
	if !returns(2*time.Minute, wg.Wait) {
		t.Fatalf("the processes did not stop within 2 minutes:\n%s", said())
	}

	if n := gaveUp.Load(); n != 0 {
		t.Errorf("%d writers gave up on writing their file whole", n)
	}
	w, rd := [2]int64{writes[0].Load(), writes[1].Load()}, [2]int64{reads[0].Load(), reads[1].Load()}
	if w[0] == 0 || w[1] == 0 || rd[0] == 0 || rd[1] == 0 || w[0]+w[1] == before {
		t.Errorf("files written whole %v and read whole %v on nodes 2 and 5, %d of the writes before the last 5 s", w, rd, before)
	}
	if i, bad := sim.CloseToOpen(r.ops); bad {
		t.Fatalf("operation %d of %d breaks close-to-open: %+v\n%s", i, len(r.ops), r.ops[i], said())
	}
	t.Logf("%d operations; files written whole %v and read whole %v on nodes 2 and 5", len(r.ops), w, rd)
}
