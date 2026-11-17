package main

import (
	"bytes"
	"fmt"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/driver/tcp"
	"github.com/nvcnvn/rock-solid/internal/loop"
	"github.com/nvcnvn/rock-solid/internal/meta"
)

// ctlGroup is 257, whose ID begins with the bytes a chunkstore frame of kind 1 begins with.
const ctlGroup = 257

var ctlNode = core.NodeID{Node: 1}

const probeTimer core.TimerID = 1

// probe is a storage node's loop as far as the review needs: every 100 ms it sends the voter its
// report, a session fetch and a Txn adding 1 to a counter, each under a new token, and keeps the
// session tables and counts answered.
type probe struct {
	t       *testing.T
	l       *loop.Loop
	token   uint64
	answers map[uint64]chunkstore.SessionTable
	counts  map[uint64]uint64
	other   int
}

var probeTxn = meta.Txn{Ops: []meta.Op{meta.Update([]byte("probe"), 0, meta.Add, 1)}}

func (p *probe) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	switch ev.Kind {
	case core.EvTimer:
		p.token++
		r := core.Action{Kind: core.ActNetSend, Peer: ctlNode}
		f := core.Action{Kind: core.ActNetSend, Peer: ctlNode}
		if err := meta.EncodeNodeReport(ctx.Pool, ctlGroup, &meta.NodeReport{Node: 2}, &r.Msg); err != nil {
			p.t.Error(err)
		}
		if err := meta.EncodeSessionFetch(ctx.Pool, ctlGroup, p.token, &f.Msg); err != nil {
			p.t.Error(err)
		}
		c := core.Action{Kind: core.ActNetSend, Peer: ctlNode}
		if err := meta.EncodeTxn(ctx.Pool, ctlGroup, p.token, &probeTxn, &c.Msg); err != nil {
			p.t.Error(err)
		}
		out = append(out, r, f, c, core.Action{Kind: core.ActSetTimer, Timer: probeTimer, At: ctx.Now + 100_000})
	case core.EvNetRecv:
		group, kind, token, ok := meta.Peek(&ev.Msg)
		var st chunkstore.SessionTable
		if ok && group == ctlGroup && kind == meta.KindSessions && meta.DecodeSessions(ctx.Pool, &ev.Msg, &st) == nil {
			p.answers[token] = st
			return out
		}
		var a meta.Answer
		if ok && group == ctlGroup && kind == meta.KindAnswer && meta.DecodeAnswer(ctx.Pool, &ev.Msg, &a) == nil {
			if a.Err == nil && len(a.Result.Updated) == 1 {
				p.counts[token] = a.Result.Updated[0]
			}
			return out
		}
		p.other++
		if ev.Msg.Buf != core.NoBuf {
			ctx.Pool.Free(ev.Msg.Buf)
		}
	}
	return out
}

func newProbe(t *testing.T, port int) *probe {
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
	p := &probe{t: t, answers: map[uint64]chunkstore.SessionTable{}, counts: map[uint64]uint64{}}
	td := tcp.New(tcp.Config{Self: core.NodeID{Node: 2}, Seed: 2, Dial: func(core.NodeID) (int, error) {
		return tcp.DialTCP(netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(port)))
	}})
	p.l = loop.New(loop.Config{Node: 2, Pool: pool}, k, p, td)
	td.L = p.l
	p.l.Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: probeTimer})
	return p
}

// answered runs the probe until a fetch and a Txn made from now on are answered, and returns the
// lowest and the highest count answered to the Txns made from now on.
func (p *probe) answered(within time.Duration) (lo, hi uint64, ok bool) {
	from := p.token
	for end := time.Now().Add(within); time.Now().Before(end); {
		p.l.Pass()
		fetched := false
		for tok := range p.answers {
			fetched = fetched || tok > from
		}
		lo, hi = 0, 0
		for tok, n := range p.counts {
			if tok > from && (lo == 0 || n < lo) {
				lo = n
			}
			if tok > from {
				hi = max(hi, n)
			}
		}
		if fetched && hi != 0 {
			return lo, hi, true
		}
	}
	return 0, 0, false
}

// TestCtlProcess is the ctl process the review starts and kills: the test binary run again with
// its flags in RS_CTL_ARGS.
func TestCtlProcess(t *testing.T) {
	args := os.Getenv("RS_CTL_ARGS")
	if args == "" {
		t.Skip("run by TestASingleVoterControlPlaneAnswersOverTCPAndSurvivesAKill as the ctl process")
	}
	cfg, err := parseCtl(strings.Fields(args))
	if err != nil {
		t.Fatal(err)
	}
	p, err := newCtl(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Run(); err != nil {
		t.Fatal(err)
	}
}

// startCtl starts the ctl process with args, its output going to out.
func startCtl(t *testing.T, args string, out *bytes.Buffer) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCtlProcess$", "-test.v")
	cmd.Env = append(os.Environ(), "RS_CTL_ARGS="+args)
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

// A single-voter control plane of group 257, a process over TCP, elects itself, leases, commits a
// session table of its own and answers a node's fetch under its token, so a group-257 frame is
// never taken for a chunkstore one. Killed and started again on its disk file, it recovers its
// log and answers again, its counter past every count it answered before, where a control plane
// started over would count from 1 again.
func TestASingleVoterControlPlaneAnswersOverTCPAndSurvivesAKill(t *testing.T) {
	dir := os.Getenv("RS_URING_DIR")
	if dir == "" {
		dir = t.TempDir()
	}
	disk := filepath.Join(dir, "ctl-disk")
	t.Cleanup(func() { _ = os.Remove(disk) })
	port := freeBase(t, 1)
	flags := fmt.Sprintf("-node 1 -voters 1=127.0.0.1:%d -group %d -disk %s -disk-bytes %d", port, ctlGroup, disk, 512<<20)
	var out bytes.Buffer
	cmd := startCtl(t, flags+" -format", &out)
	pr := newProbe(t, port)
	_, before, ok := pr.answered(60 * time.Second)
	if !ok {
		t.Fatalf("no fetch answered; %d other messages; the process said:\n%s", pr.other, out.String())
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	out.Reset()
	startCtl(t, flags, &out)
	after, _, ok := pr.answered(60 * time.Second)
	if !ok {
		t.Fatalf("no fetch answered after a kill and a restart; %d other messages; the process said:\n%s", pr.other, out.String())
	}
	if after <= before {
		t.Fatalf("after a kill and a restart the counter answered %d, before it %d", after, before)
	}
}

// -format makes a file that is not there, and makes again one whose superblock blocks both read
// as zeros, as a format cut short leaves it; a file with a byte in either block is refused and
// left as it was.
func TestFormatMakesAFileNeverWrittenAndRefusesAnyOther(t *testing.T) {
	dir := os.Getenv("RS_URING_DIR")
	if dir == "" {
		dir = t.TempDir()
	}
	const size = 8 << 20
	g, err := disklog.GeometryFor(size)
	if err != nil {
		t.Fatal(err)
	}
	copyAt := int64(g.SuperblockCopyLBA()) * core.BlockSize
	for _, c := range []struct {
		name  string
		bytes int64
		at    int64
		ok    bool
	}{
		{"absent", -1, 0, true},
		{"zeros", size, -1, true},
		{"short zeros", size / 4, -1, true},
		{"superblock", size, 0, false},
		{"copy", size, copyAt, false},
	} {
		path := filepath.Join(dir, "ctl-format-"+strings.ReplaceAll(c.name, " ", "-"))
		t.Cleanup(func() { _ = os.Remove(path) })
		if c.bytes >= 0 {
			if err := os.WriteFile(path, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Truncate(path, c.bytes); err != nil {
				t.Fatal(err)
			}
		}
		if c.at >= 0 && c.bytes >= 0 {
			f, err := os.OpenFile(path, os.O_WRONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.WriteAt([]byte{1}, c.at+7); err != nil {
				t.Fatal(err)
			}
			_ = f.Close()
		}
		err := createDisk(path, size)
		st, serr := os.Stat(path)
		switch {
		case serr != nil:
			t.Fatalf("%s: %v", c.name, serr)
		case c.ok && (err != nil || st.Size() != size):
			t.Fatalf("%s: -format gave %v and a file of %d bytes", c.name, err, st.Size())
		case !c.ok && (err == nil || st.Size() != c.bytes):
			t.Fatalf("%s: -format gave %v and left a file of %d bytes", c.name, err, st.Size())
		}
		if !c.ok {
			b := make([]byte, 1)
			f, _ := os.Open(path)
			_, _ = f.ReadAt(b, c.at+7)
			_ = f.Close()
			if b[0] != 1 {
				t.Fatalf("%s: a refused -format changed the file", c.name)
			}
		}
	}
}
