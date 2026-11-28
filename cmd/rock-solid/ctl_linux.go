package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/netip"
	"os"
	"slices"
	"sync"

	"go.etcd.io/raft/v3/raftpb"
	"golang.org/x/sys/unix"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/driver/tcp"
	"github.com/nvcnvn/rock-solid/internal/driver/uring"
	"github.com/nvcnvn/rock-solid/internal/lease"
	"github.com/nvcnvn/rock-solid/internal/loop"
	"github.com/nvcnvn/rock-solid/internal/lsm"
	"github.com/nvcnvn/rock-solid/internal/meta"
	"github.com/nvcnvn/rock-solid/internal/node"
	"github.com/nvcnvn/rock-solid/internal/ops"
	"github.com/nvcnvn/rock-solid/internal/raftshard"
)

// raftTimer is the timer of the voter's replica.
const raftTimer core.TimerID = 0x6374726674

// ctl is a control-plane voter: one loop running its CtlHost over the TCP driver and an io_uring
// ring on its disk file.
type ctl struct {
	cfg   ctlConfig
	l     *loop.Loop
	pool  *loop.Pool
	pages *loop.Slab
	ring  *uring.Ring
	k     *loop.Epoll
	m     *ctlMachine
	mail  mail
	// lfd is the listening socket, closed with the rest once Run ends, so a voter started again in
	// the same process can listen on its port.
	lfd int
	// Port is the port the voter listens on.
	Port  int
	mu    sync.Mutex
	err   error
	facts facts
}

// ctlMachine boots the host on the loop's first event and hands it every event.
type ctlMachine struct {
	p    *ctl
	disk *uring.Disk
	h    *meta.CtlHost
	out  meta.CtlHostOutput
	// fits is set once the served log's formatted size passed replayFits: a file grown since its
	// format holds no more log.
	fits bool
}

func newCtl(cfg ctlConfig) (_ *ctl, err error) {
	if cfg.Format {
		if err := createDisk(cfg.Disk, cfg.DiskBytes); err != nil {
			return nil, err
		}
	}
	p := &ctl{cfg: cfg}
	defer func() {
		if err != nil {
			p.close()
		}
	}()
	if p.pool, err = loop.NewPool(poolBytes, 0); err != nil {
		return nil, err
	}
	limit := newHeapLimit(cfg.Memory)
	limit.add(p.pool.Mapped())
	if err := limit.check(pageRegion); err != nil {
		return nil, err
	}
	p.pages = loop.NewSlab(pageRegion)
	p.pages.OnMap = limit.add
	if p.k, err = loop.NewEpoll(loop.VMClockPath); err != nil {
		return nil, err
	}
	if p.ring, err = uring.New(p.pool, ringEntries); err != nil {
		return nil, err
	}
	disk, err := openDisk(p.ring, cfg.Disk)
	if err != nil {
		return nil, err
	}
	addrs := map[uint32]netip.AddrPort{}
	for _, v := range cfg.Voters {
		addrs[v.Node] = v.Addr
	}
	td := tcp.New(tcp.Config{Self: core.NodeID{Node: cfg.Node}, Seed: rand.Uint64(), Dial: func(peer core.NodeID) (int, error) {
		ap, ok := addrs[peer.Node]
		if !ok || peer.Loop != 0 {
			return -1, fmt.Errorf("no address for %v", peer)
		}
		return tcp.DialTCP(ap)
	}})
	p.m = &ctlMachine{p: p, disk: disk}
	p.l = loop.New(loop.Config{Node: cfg.Node, Pool: p.pool}, p.k, p.m, loopDriver{td, disk})
	td.L, p.mail.in = p.l, p.l.Inbox()
	if err := p.l.AttachDisk(p.ring); err != nil {
		return nil, err
	}
	lfd, port, err := tcp.ListenTCP(cfg.Listen)
	if err != nil {
		return nil, err
	}
	p.lfd = lfd
	if err := td.Listen(lfd); err != nil {
		return nil, err
	}
	p.Port = port
	p.facts = facts{node: cfg.Node, flags: cfg.Flags, hosts: hostsOf(cfg.Voters, true), mails: []*mail{&p.mail},
		fill: func(_ int, ctx *core.Context, f *ops.Facts) { node.CtlFacts(ctx, p.m.h, cfg.Node, release, f) }, pg: func(int, *ops.PGFacts) bool { return false }}
	if err := p.facts.listen(cfg.Admin); err != nil {
		return nil, err
	}
	if !p.l.Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: bootTimer}) {
		return nil, errors.New("ctl: the loop's inbox is full at start")
	}
	return p, nil
}

// createDisk makes the file -format formats, of size bytes. A file already there is made again
// only if both its superblock blocks read as zeros, as a format cut short leaves them and as a
// device's start formats one (chunkstore.StartDevice); any other file is refused, so -format
// never overwrites a log.
func createDisk(path string, size int64) error {
	err := uring.CreateFile(path, size)
	if !errors.Is(err, unix.EEXIST) {
		return err
	}
	blank, berr := neverWritten(path)
	switch {
	case berr != nil:
		return berr
	case !blank:
		return fmt.Errorf("ctl: -format: %s holds a superblock or other data, so it is never formatted over: %w", path, err)
	}
	if err := os.Remove(path); err != nil {
		return err
	}
	return uring.CreateFile(path, size)
}

// neverWritten reports whether block 0 of the file and the superblock copy its size places read
// as zeros, or lie past its end.
func neverWritten(path string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer func() { _ = f.Close() }()
	st, err := f.Stat()
	if err != nil {
		return false, err
	}
	at := []int64{0}
	if g, err := disklog.GeometryFor(uint64(st.Size())); err == nil {
		at = append(at, int64(g.SuperblockCopyLBA())*core.BlockSize)
	}
	b := make([]byte, core.BlockSize)
	for _, off := range at {
		n, err := f.ReadAt(b, off)
		if err != nil && !errors.Is(err, io.EOF) {
			return false, err
		}
		if slices.ContainsFunc(b[:n], func(c byte) bool { return c != 0 }) {
			return false, nil
		}
	}
	return true, nil
}

// Run runs the loop until Stop or the host stops for good, which it returns.
func (p *ctl) Run() error {
	p.facts.serve()
	p.l.Run()
	p.close()
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

// Stop ends Run.
func (p *ctl) Stop() { p.l.Stop() }

// call runs f on the voter's loop at its next event, once its host has started.
func (p *ctl) call(f func(ctx *core.Context, h *meta.CtlHost)) {
	p.mail.post(func(ctx *core.Context) { f(ctx, p.m.h) })
}

func (p *ctl) fail(err error) {
	p.mu.Lock()
	if p.err == nil {
		p.err = err
	}
	p.mu.Unlock()
	p.l.Stop()
}

func (p *ctl) close() {
	p.facts.close()
	if p.lfd > 0 {
		_ = unix.Close(p.lfd)
	}
	if p.ring != nil {
		p.ring.Close()
	}
	if p.k != nil {
		p.k.Close()
	}
	if p.pool != nil {
		_ = p.pool.Close()
	}
	if p.pages != nil {
		_ = p.pages.Close()
	}
}

func (m *ctlMachine) OnEvent(ctx *core.Context, ev *core.Event, acts []core.Action) []core.Action {
	m.out.Reset()
	if m.h == nil {
		h, err := m.boot(ctx)
		if err != nil {
			m.p.fail(err)
			return acts
		}
		m.h = h
	}
	m.p.mail.run(ctx)
	if !wake(ev) {
		m.h.OnEvent(ctx, ev, &m.out)
	}
	for _, err := range m.out.Errs {
		log.Print(err)
	}
	if err := m.h.Err(); err != nil {
		m.p.fail(err)
	}
	if log := m.h.Log(); log != nil && !m.fits {
		m.fits = true
		if err := replayFits(m.p.cfg.MaxReplayBytes, log.Superblock().DiskBlocks()*core.BlockSize); err != nil {
			m.p.fail(err)
		}
	}
	return append(acts, m.out.Actions...)
}

// boot formats the disk or recovers it, the replica's boot state the configuration of the voters,
// each under its node ID as its Raft ID.
func (m *ctlMachine) boot(ctx *core.Context) (*meta.CtlHost, error) {
	cfg := m.p.cfg
	cs := raftpb.ConfState{}
	var peers []raftshard.Peer
	for _, v := range cfg.Voters {
		cs.Voters = append(cs.Voters, uint64(v.Node))
		peers = append(peers, raftshard.Peer{ID: uint64(v.Node), Node: core.NodeID{Node: v.Node}})
	}
	st := raftshard.NewStorage()
	if err := st.ApplySnapshot(raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 1, Term: 1, ConfState: cs}}); err != nil {
		return nil, err
	}
	lc := disklog.DefaultConfig()
	lc.NoTrim, lc.MaxReplayBytes = true, cfg.MaxReplayBytes
	mc := meta.Config{Raft: raftshard.Config{Group: cfg.Group, ID: uint64(cfg.Node), Peers: peers, Storage: st, Seed: rand.Uint64(), Timer: raftTimer,
		Holder: lease.NewHolder(lease.DefaultConfig), Grantor: lease.NewGrantor(lease.DefaultConfig, rand.Uint64(), lease.Now(ctx))},
		Tree: lsm.Config{MaxPiece: 64 << 10, StepBytes: int(lc.CompactionStepBytes), Pages: m.p.pages}}
	if !cfg.Format {
		return meta.OpenCtlHost(ctx, mc, lc, m.disk.Blocks(), &m.out)
	}
	g, err := disklog.GeometryFor(m.disk.Blocks() * core.BlockSize)
	if err != nil {
		return nil, err
	}
	return meta.FormatCtlHost(ctx, mc, lc, disklog.Superblock{LogID: rand.Uint64() | 1, Geometry: g}, &m.out)
}
