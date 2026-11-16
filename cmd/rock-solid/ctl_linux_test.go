package main

import (
	"bytes"
	"fmt"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/driver/tcp"
	"github.com/nvcnvn/rock-solid/internal/loop"
	"github.com/nvcnvn/rock-solid/internal/meta"
)

// ctlGroup is 257, whose ID begins with the bytes a chunkstore frame of kind 1 begins with.
const ctlGroup = 257

var ctlNode = core.NodeID{Node: 1}

const probeTimer core.TimerID = 1

// probe is a storage node's loop as far as the review needs: every 100 ms it sends the voter its
// report and a session fetch under a new token, and keeps the session tables answered.
type probe struct {
	t       *testing.T
	l       *loop.Loop
	token   uint64
	answers map[uint64]chunkstore.SessionTable
	other   int
}

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
		out = append(out, r, f, core.Action{Kind: core.ActSetTimer, Timer: probeTimer, At: ctx.Now + 100_000})
	case core.EvNetRecv:
		group, kind, token, ok := meta.Peek(&ev.Msg)
		var st chunkstore.SessionTable
		if ok && group == ctlGroup && kind == meta.KindSessions && meta.DecodeSessions(ctx.Pool, &ev.Msg, &st) == nil {
			p.answers[token] = st
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
	p := &probe{t: t, answers: map[uint64]chunkstore.SessionTable{}}
	td := tcp.New(tcp.Config{Self: core.NodeID{Node: 2}, Seed: 2, Dial: func(core.NodeID) (int, error) {
		return tcp.DialTCP(netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(port)))
	}})
	p.l = loop.New(loop.Config{Node: 2, Pool: pool}, k, p, td)
	td.L = p.l
	p.l.Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: probeTimer})
	return p
}

// answered runs the probe until a fetch made from now on is answered.
func (p *probe) answered(within time.Duration) bool {
	from := p.token
	for end := time.Now().Add(within); time.Now().Before(end); {
		p.l.Pass()
		for tok := range p.answers {
			if tok > from {
				return true
			}
		}
	}
	return false
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
// log and answers again.
func TestASingleVoterControlPlaneAnswersOverTCPAndSurvivesAKill(t *testing.T) {
	dir := os.Getenv("RS_URING_DIR")
	if dir == "" {
		dir = t.TempDir()
	}
	disk := filepath.Join(dir, "ctl-disk")
	t.Cleanup(func() { _ = os.Remove(disk) })
	fd, port, err := tcp.ListenTCP(netip.MustParseAddrPort("127.0.0.1:0"))
	if err != nil {
		t.Fatal(err)
	}
	_ = unix.Close(fd)
	flags := fmt.Sprintf("-node 1 -voters 1=127.0.0.1:%d -group %d -disk %s -disk-bytes %d", port, ctlGroup, disk, 512<<20)
	var out bytes.Buffer
	cmd := startCtl(t, flags+" -format", &out)
	pr := newProbe(t, port)
	if !pr.answered(60 * time.Second) {
		t.Fatalf("no fetch answered; %d other messages; the process said:\n%s", pr.other, out.String())
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	out.Reset()
	startCtl(t, flags, &out)
	if !pr.answered(60 * time.Second) {
		t.Fatalf("no fetch answered after a kill and a restart; %d other messages; the process said:\n%s", pr.other, out.String())
	}
}
