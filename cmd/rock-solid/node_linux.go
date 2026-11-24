package main

import (
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"math/rand/v2"
	"net/netip"
	"sync"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/driver/tcp"
	"github.com/nvcnvn/rock-solid/internal/driver/uring"
	"github.com/nvcnvn/rock-solid/internal/loop"
	"github.com/nvcnvn/rock-solid/internal/node"
)

// storageNode is a storage node's process: loop 0 runs its node.Node, and loop k its k-th device's
// node.Device, each on a TCP driver of its own and the device's on an io_uring ring of its own.
// The machines' outputs for one another go between the loops as mail.
type storageNode struct {
	cfg   nodeConfig
	limit *heapLimit
	addrs map[uint32]netip.AddrPort
	loops []*nodeLoop
	nm    *nodeMachine
}

type nodeLoop struct {
	l     *loop.Loop
	k     *loop.Epoll
	pool  *loop.Pool
	pages *loop.Slab
	ring  *uring.Ring
	dm    *deviceMachine
	mail  mail
}

func newNode(cfg nodeConfig) (_ *storageNode, err error) {
	p := &storageNode{cfg: cfg, addrs: map[uint32]netip.AddrPort{cfg.Node: cfg.Listen}, limit: newHeapLimit(cfg.Memory)}
	defer func() {
		if err != nil {
			p.close()
		}
	}()
	for _, v := range append(cfg.Ctls, cfg.Peers...) {
		p.addrs[v.Node] = v.Addr
	}
	p.nm = &nodeMachine{p: p, cfg: cfg.machine()}
	if err := p.add(nil, ""); err != nil {
		return nil, err
	}
	for i, path := range cfg.Devices {
		store := chunkstore.Config{Log: disklog.DefaultConfig(), Pool: core.PoolID(cfg.Pool), MergeCache: chunkstore.DefaultMergeCache}
		dm := &deviceMachine{p: p, cfg: node.DeviceConfig{Device: deviceID(path), Class: cfg.Class, Loop: uint32(i + 1), Store: store,
			Seed: rand.Uint64(), Group: cfg.Group, Ctl: p.nm.cfg.Ctls[0]}}
		if err := p.add(dm, path); err != nil {
			return nil, err
		}
	}
	if err := p.limit.check(int64(len(cfg.Devices)) * pageRegion); err != nil {
		return nil, err
	}
	return p, nil
}

// deviceID is the device's stable identity: its path, which names the same drive at every start,
// as a /dev/disk/by-id link does.
func deviceID(path string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(path))
	return h.Sum64()
}

// add starts the next loop: the node's for a nil dm, else dm's on the device at path, each with
// its kernel, its TCP driver and listener, and a device's with its ring.
func (p *storageNode) add(dm *deviceMachine, path string) error {
	nl := &nodeLoop{dm: dm}
	p.loops = append(p.loops, nl)
	id := uint32(len(p.loops) - 1)
	var err error
	if nl.pool, err = loop.NewPool(poolBytes, 0); err != nil {
		return err
	}
	p.limit.add(nl.pool.Mapped())
	if nl.k, err = loop.NewEpoll(loop.VMClockPath); err != nil {
		return err
	}
	td := tcp.New(tcp.Config{Self: core.NodeID{Node: p.cfg.Node, Loop: id}, Seed: rand.Uint64(), Dial: p.dial})
	var m loop.Machine = p.nm
	var d loop.Driver = td
	if dm != nil {
		m = dm
		nl.pages = loop.NewSlab(pageRegion)
		nl.pages.OnMap = p.limit.add
		dm.cfg.Store.Pages = nl.pages
		if nl.ring, err = uring.New(nl.pool, ringEntries); err != nil {
			return err
		}
		if disk, err := nl.ring.Open(path, false); err != nil {
			log.Printf("device %s: %v", path, err)
			dm.cfg.Err = err
			nl.ring.Close()
			nl.ring = nil
		} else {
			dm.cfg.Blocks, d = disk.Blocks(), loopDriver{td, disk}
		}
	}
	nl.l = loop.New(loop.Config{Node: p.cfg.Node, ID: uint16(id), Pool: nl.pool}, nl.k, m, d)
	td.L, nl.mail.in = nl.l, nl.l.Inbox()
	if nl.ring != nil {
		if err := nl.l.AttachDisk(nl.ring); err != nil {
			return err
		}
	}
	lfd, _, err := tcp.ListenTCP(netip.AddrPortFrom(p.cfg.Listen.Addr(), p.cfg.Listen.Port()+uint16(id)))
	if err != nil {
		return err
	}
	if err := td.Listen(lfd); err != nil {
		return err
	}
	if !nl.l.Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: bootTimer}) {
		return errors.New("node: a loop's inbox is full at start")
	}
	return nil
}

// dial reaches loop k of a node at its first loop's port plus k.
func (p *storageNode) dial(peer core.NodeID) (int, error) {
	ap, ok := p.addrs[peer.Node]
	if !ok || int(ap.Port())+int(peer.Loop) > 65535 {
		return -1, fmt.Errorf("no address for %v", peer)
	}
	return tcp.DialTCP(netip.AddrPortFrom(ap.Addr(), ap.Port()+uint16(peer.Loop)))
}

// Run runs every loop until Stop.
func (p *storageNode) Run() error {
	var wg sync.WaitGroup
	for _, nl := range p.loops[1:] {
		wg.Go(nl.l.Run)
	}
	p.loops[0].l.Run()
	wg.Wait()
	p.close()
	return nil
}

// Stop ends Run.
func (p *storageNode) Stop() {
	for _, nl := range p.loops {
		nl.l.Stop()
	}
}

func (p *storageNode) close() {
	for _, nl := range p.loops {
		if nl.ring != nil {
			nl.ring.Close()
		}
		if nl.k != nil {
			nl.k.Close()
		}
		if nl.pool != nil {
			_ = nl.pool.Close()
		}
		if nl.pages != nil {
			_ = nl.pages.Close()
		}
	}
}

// nodeMachine runs the node.Node on loop 0 and mails each device's loop what it outputs for it.
type nodeMachine struct {
	p   *storageNode
	cfg node.Config
	n   *node.Node
	out node.Output
}

func (m *nodeMachine) OnEvent(ctx *core.Context, ev *core.Event, acts []core.Action) []core.Action {
	m.out.Reset()
	if m.n == nil {
		m.n = node.New(m.cfg)
		m.n.Start(ctx, &m.out)
	}
	m.p.loops[0].mail.run(ctx)
	if !wake(ev) {
		m.n.OnEvent(ctx, ev, &m.out)
	}
	for _, nl := range m.p.loops[1:] {
		if o, ok := m.out.Clone(); ok {
			dm := nl.dm
			nl.mail.post(func(ctx *core.Context) { dm.d.Take(ctx, &o, &dm.out) })
		}
	}
	return append(acts, m.out.Actions...)
}

// take hands the node what device's machine output.
func (m *nodeMachine) take(ctx *core.Context, device uint64, o *node.DeviceOutput) {
	switch {
	case o.Opened != nil:
		log.Printf("device %#x opened log %#x", device, o.Opened.Log)
	case o.Failed:
		log.Printf("device %#x failed", device)
	}
	if o.Epoch != 0 {
		log.Printf("device %#x holds placement epoch %d", device, o.Epoch)
	}
	m.n.Take(ctx, device, o, &m.out)
}

// deviceMachine runs one device's node.Device on its loop and mails the node's loop what it
// outputs for the node.
type deviceMachine struct {
	p   *storageNode
	cfg node.DeviceConfig
	d   *node.Device
	out node.DeviceOutput
}

func (m *deviceMachine) OnEvent(ctx *core.Context, ev *core.Event, acts []core.Action) []core.Action {
	m.out.Reset()
	if m.d == nil {
		m.d = node.NewDevice(ctx, m.cfg, &m.out)
	}
	m.p.loops[m.cfg.Loop].mail.run(ctx)
	if !wake(ev) {
		m.d.OnEvent(ctx, ev, &m.out)
	}
	if o, ok := m.out.Clone(); ok {
		m.p.loops[0].mail.post(func(ctx *core.Context) { m.p.nm.take(ctx, m.cfg.Device, &o) })
	}
	return append(acts, m.out.Actions...)
}
