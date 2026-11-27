package main

import (
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/driver/tcp"
	"github.com/nvcnvn/rock-solid/internal/frontend/fuse"
	"github.com/nvcnvn/rock-solid/internal/fsclient"
	"github.com/nvcnvn/rock-solid/internal/loop"
	"github.com/nvcnvn/rock-solid/internal/node"
)

// mount is a node's FUSE mount, served on loop 0 beside the node's machine, a loop that hosts no
// MetaStore replica: the daemon mounts once the node's session has started and its shard map
// places every voter, and hands the daemon every session, table and membership the node takes.
type mount struct {
	d       *fuse.Daemon
	drv     *mountDriver
	path    string
	mounted bool
	// stop stops the node, and err is why: a mount that could not start.
	stop func()
	err  error
}

func newMount(cfg nodeConfig, td *tcp.Driver) *mount {
	return &mount{d: fuse.New(fuse.Config{Volume: cfg.Mount, Pool: core.PoolID(cfg.Pool), Inc: rand.Uint64()}), drv: &mountDriver{Driver: td}, path: cfg.MountPath}
}

// takes reports whether ev is the mount's rather than the node's: a message or lost connection of
// a peer other than a control-plane voter, or one of the mount's timers.
func (m *mount) takes(ctls []core.NodeID, ev *core.Event) bool {
	switch ev.Kind {
	case core.EvNetRecv, core.EvPeerLost:
		return !slices.Contains(ctls, ev.Peer)
	case core.EvTimer:
		return ev.Timer == fsclient.MountTimer || ev.Timer == fuse.ReadyTimer || ev.Timer == chunkstore.ClientTimer
	}
	return false
}

func (m *mount) onEvent(ctx *core.Context, ev *core.Event, acts []core.Action) []core.Action {
	if ev.Kind != core.EvTimer || ev.Timer != fuse.ReadyTimer {
		acts = m.d.OnEvent(ctx, ev, acts)
		m.logRefused()
		return acts
	}
	acts, err := m.d.Serve(ctx, acts)
	m.logRefused()
	if err != nil {
		if !errors.Is(err, fuse.ErrGone) {
			log.Printf("mount %s: %v", m.path, err)
		}
		log.Printf("mount %s is gone", m.path)
		m.drv.l.Close(m.drv.c)
		m.drv.on = false
	}
	return acts
}

func (m *mount) logRefused() {
	if err := m.d.Refused(); err != nil {
		log.Printf("mount %s: the kernel refused a reply: %v", m.path, err)
	}
}

// session hands the daemon the node's session, first in a pass, since a new one fails what the old
// one's chunk client holds and frees its buffers, which a send of this pass may still name.
func (m *mount) session(ctx *core.Context, n *node.Node, acts []core.Action) []core.Action {
	id, _ := n.Session(ctx)
	acts = m.d.SetSession(ctx, id, acts)
	m.logRefused()
	return acts
}

// take hands the daemon what the node output and its session, mounting once the daemon is ready,
// and asks the node for the placement table while the chunk client waits for one.
func (m *mount) take(ctx *core.Context, n *node.Node, out *node.Output, acts []core.Action) []core.Action {
	acts = m.session(ctx, n, acts)
	acts = m.d.SetTable(ctx, out.Table, out.Delta, acts)
	if out.Member {
		acts = m.d.SetMembership(ctx, out.Plans, out.Disks, acts)
	}
	m.logRefused()
	if m.d.WantsTable() {
		n.FetchTable(ctx, out)
	}
	if m.mounted || !m.d.Ready() {
		return acts
	}
	m.mounted = true
	if err := m.attach(ctx); err != nil {
		m.err = err
		m.stop()
	}
	return acts
}

// attach mounts the volume and serves its device on the loop; a device the daemon has no buffer
// for or the loop cannot take is unmounted again, as nobody would serve it.
func (m *mount) attach(ctx *core.Context) error {
	dev, err := fuse.Mount(m.path)
	if err != nil {
		return err
	}
	if err := m.d.Attach(ctx, dev); err != nil {
		return errors.Join(fmt.Errorf("mount %s: %w", m.path, err), fuse.Unmount(dev, m.path))
	}
	c, err := m.drv.l.AddConn(dev.Fd, true)
	if err != nil {
		return errors.Join(fmt.Errorf("mount %s: %w", m.path, err), fuse.Unmount(dev, m.path))
	}
	m.drv.l.Own(c)
	m.drv.c, m.drv.on = c, true
	log.Printf("volume %d mounted at %s", m.d.Volume(), m.path)
	return nil
}

// mountDriver is loop 0's TCP driver, which also reports the mount's device readable to the
// machine; the daemon reads it itself, one request a read.
type mountDriver struct {
	*tcp.Driver
	l  *loop.Loop
	c  loop.ConnID
	on bool
	ev core.Event
}

func (d *mountDriver) Read(c loop.ConnID, in []byte) {
	if d.on && c == d.c {
		d.ev = core.Event{Kind: core.EvTimer, Timer: fuse.ReadyTimer}
		d.l.Deliver(&d.ev)
		return
	}
	d.Driver.Read(c, in)
}

func (d *mountDriver) Closed(c loop.ConnID) {
	if d.on && c == d.c {
		d.on = false
		return
	}
	d.Driver.Closed(c)
}
