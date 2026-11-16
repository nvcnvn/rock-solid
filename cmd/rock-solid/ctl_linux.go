package main

import (
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"net/netip"
	"sync"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/driver/tcp"
	"github.com/nvcnvn/rock-solid/internal/driver/uring"
	"github.com/nvcnvn/rock-solid/internal/lease"
	"github.com/nvcnvn/rock-solid/internal/loop"
	"github.com/nvcnvn/rock-solid/internal/lsm"
	"github.com/nvcnvn/rock-solid/internal/meta"
	"github.com/nvcnvn/rock-solid/internal/raftshard"
)

const (
	ctlPoolBytes = 64 << 20
	ringEntries  = 256
	// raftTimer is the timer of the voter's replica; bootTimer the event that starts the host.
	raftTimer core.TimerID = 0x6374726674
	bootTimer core.TimerID = 0x6374626f6f74
)

// ctl is a control-plane voter: one loop running its CtlHost over the TCP driver and an io_uring
// ring on its disk file.
type ctl struct {
	cfg  ctlConfig
	l    *loop.Loop
	pool *loop.Pool
	ring *uring.Ring
	k    *loop.Epoll
	// Port is the port the voter listens on.
	Port int
	mu   sync.Mutex
	err  error
}

// ctlMachine boots the host on the loop's first event and hands it every event.
type ctlMachine struct {
	p    *ctl
	disk *uring.Disk
	h    *meta.CtlHost
	out  meta.CtlHostOutput
}

// ctlDriver sends over TCP and runs disk actions on the ring.
type ctlDriver struct {
	*tcp.Driver
	disk *uring.Disk
}

func (d ctlDriver) Execute(a *core.Action) {
	if a.Kind == core.ActNetSend {
		d.Driver.Execute(a)
		return
	}
	d.disk.Execute(a)
}

func newCtl(cfg ctlConfig) (p *ctl, err error) {
	if cfg.Format {
		if err := uring.CreateFile(cfg.Disk, cfg.DiskBytes); err != nil {
			return nil, err
		}
	}
	p = &ctl{cfg: cfg}
	defer func() {
		if err != nil {
			p.close()
		}
	}()
	if p.pool, err = loop.NewPool(ctlPoolBytes, 0); err != nil {
		return nil, err
	}
	if p.k, err = loop.NewEpoll(loop.VMClockPath); err != nil {
		return nil, err
	}
	if p.ring, err = uring.New(p.pool, ringEntries); err != nil {
		return nil, err
	}
	disk, err := p.ring.Open(cfg.Disk, false)
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
	m := &ctlMachine{p: p, disk: disk}
	p.l = loop.New(loop.Config{Node: cfg.Node, Pool: p.pool}, p.k, m, ctlDriver{td, disk})
	td.L = p.l
	if err := p.l.AttachDisk(p.ring); err != nil {
		return nil, err
	}
	lfd, port, err := tcp.ListenTCP(cfg.Listen)
	if err != nil {
		return nil, err
	}
	if err := td.Listen(lfd); err != nil {
		return nil, err
	}
	p.Port = port
	if !p.l.Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: bootTimer}) {
		return nil, errors.New("ctl: the loop's inbox is full at start")
	}
	return p, nil
}

// Run runs the loop until Stop or the host stops for good, which it returns.
func (p *ctl) Run() error {
	p.l.Run()
	p.close()
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

// Stop ends Run.
func (p *ctl) Stop() { p.l.Stop() }

func (p *ctl) fail(err error) {
	p.mu.Lock()
	if p.err == nil {
		p.err = err
	}
	p.mu.Unlock()
	p.l.Stop()
}

func (p *ctl) close() {
	if p.ring != nil {
		p.ring.Close()
	}
	if p.k != nil {
		p.k.Close()
	}
	if p.pool != nil {
		_ = p.pool.Close()
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
	if ev.Kind != core.EvTimer || ev.Timer != bootTimer {
		m.h.OnEvent(ctx, ev, &m.out)
	}
	for _, err := range m.out.Errs {
		log.Print(err)
	}
	if err := m.h.Err(); err != nil {
		m.p.fail(err)
	}
	return append(acts, m.out.Actions...)
}

// boot formats the disk or recovers it, the replica's boot state the configuration of the voters.
func (m *ctlMachine) boot(ctx *core.Context) (*meta.CtlHost, error) {
	cfg := m.p.cfg
	cs := raftpb.ConfState{}
	var peers []raftshard.Peer
	var id uint64
	for i, v := range cfg.Voters {
		cs.Voters = append(cs.Voters, uint64(i)+1)
		peers = append(peers, raftshard.Peer{ID: uint64(i) + 1, Node: core.NodeID{Node: v.Node}})
		if v.Node == cfg.Node {
			id = uint64(i) + 1
		}
	}
	st := raftshard.NewStorage()
	if err := st.ApplySnapshot(raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 1, Term: 1, ConfState: cs}}); err != nil {
		return nil, err
	}
	mc := meta.Config{Raft: raftshard.Config{Group: cfg.Group, ID: id, Peers: peers, Storage: st, Seed: rand.Uint64(), Timer: raftTimer,
		Holder: lease.NewHolder(lease.DefaultConfig), Grantor: lease.NewGrantor(lease.DefaultConfig, rand.Uint64(), lease.Now(ctx))},
		Tree: lsm.Config{MaxPiece: 64 << 10}}
	lc := disklog.DefaultConfig()
	lc.NoTrim = true
	if !cfg.Format {
		return meta.OpenCtlHost(ctx, mc, lc, m.disk.Blocks(), &m.out)
	}
	g, err := disklog.GeometryFor(m.disk.Blocks() * core.BlockSize)
	if err != nil {
		return nil, err
	}
	return meta.FormatCtlHost(ctx, mc, lc, disklog.Superblock{LogID: rand.Uint64() | 1, Geometry: g}, &m.out)
}
