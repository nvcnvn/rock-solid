// Package fuse is the FUSE frontend's driver: it mounts a volume on /dev/fuse and serves its
// device with the filesystem client's Mount on the node's event loop.
package fuse

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"golang.org/x/sys/unix"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/fsclient"
	"github.com/nvcnvn/rock-solid/internal/meta"
)

// ReadyTimer is never armed but delivered by the loop's driver when the device has requests to read.
const ReadyTimer core.TimerID = 0x66726479

// ReadBuffer is the smallest buffer that holds any request the kernel sends under MaxWrite: the
// kernel refuses a read of /dev/fuse into less with EINVAL.
const ReadBuffer = fsclient.MaxWrite + 4096

// batch is the most requests one Serve reads, so a busy mount leaves the loop its other work.
const batch = 64

// ErrGone is a device whose connection the kernel ended: the mount was unmounted or aborted.
var ErrGone = errors.New("fuse: the mount's connection is gone")

// Device is a mount's /dev/fuse descriptor, non-blocking: each read takes one request, each write
// one reply.
type Device struct{ Fd int }

// Read reads one request into b, 0 when none waits.
func (d Device) Read(b []byte) (int, error) {
	n, err := unix.Read(d.Fd, b)
	switch {
	case err == unix.EAGAIN || err == unix.EINTR:
		return 0, nil
	case err == unix.ENODEV:
		return 0, ErrGone
	case err != nil:
		return 0, err
	}
	return n, nil
}

// Write writes one reply. ENOENT, a reply to a request the kernel no longer waits for, as an
// interrupted one, is no error.
func (d Device) Write(b []byte) error {
	_, err := unix.Write(d.Fd, b)
	switch {
	case err == nil, err == unix.ENOENT:
		return nil
	case err == unix.ENODEV:
		return ErrGone
	}
	return err
}

// Config is a daemon's mount: its volume, the pool its chunks go to, and Inc, which names this
// start of the process and differs between starts (meta.NewClient).
type Config struct {
	Volume uint32
	Pool   core.PoolID
	Inc    uint64
}

// Daemon serves one mount on one event loop: it reads the device's requests into the filesystem
// client's Mount, writes the replies it frames, reads the /proc credentials it asks for, and hands
// it the node's sessions, tables and membership.
type Daemon struct {
	cfg     Config
	g       *fsclient.Mount
	dev     Device
	buf     core.IOBuf
	out     fsclient.MountOutput
	refused error
	broken  error
	creds   fsclient.Creds
}

// New is a daemon of cfg's mount, with no session, shard map or device yet. The volume's one data
// group spans every slot.
func New(cfg Config) *Daemon {
	return &Daemon{cfg: cfg, g: fsclient.NewMount(fsclient.Config{Volume: cfg.Volume, Pool: cfg.Pool, Mount: 1, SlotLimit: 1<<32 - 1}, cfg.Inc)}
}

// Ready reports whether the daemon can serve: it holds a session and a shard map that places
// every voter's loop.
func (d *Daemon) Ready() bool { return d.g.Ready() }

// WantsTable reports that the daemon holds no placement table, or its chunk client waits for a newer one.
func (d *Daemon) WantsTable() bool { return d.g.WantsTable() }

// Refused takes the last error the device gave a reply since the last call, nil if none: the
// kernel failed that request and the mount serves on. EINVAL is not among them: the kernel gives
// it a frame it cannot take, before it finds the request, which then never ends; only an encoder
// bug writes one, so Serve returns it and the mount ends.
func (d *Daemon) Refused() error {
	err := d.refused
	d.refused = nil
	return err
}

// Volume is the mount's volume.
func (d *Daemon) Volume() uint32 { return d.cfg.Volume }

// Attach makes dev the device the daemon serves, taking the buffer it reads requests into.
func (d *Daemon) Attach(ctx *core.Context, dev Device) error {
	b, err := ctx.Pool.Alloc(ReadBuffer)
	if err != nil {
		return fmt.Errorf("fuse: a buffer to read requests into: %w", err)
	}
	d.dev, d.buf = dev, b
	return nil
}

// SetSession hands the mount the node's session (fsclient.Mount.SetSession), first in a pass.
func (d *Daemon) SetSession(ctx *core.Context, session uint64, acts []core.Action) []core.Action {
	d.out.Reset()
	d.g.SetSession(ctx, session, &d.out)
	return d.flush(ctx, acts)
}

// SetTable hands the mount placement table t or a delta of the one it holds.
func (d *Daemon) SetTable(ctx *core.Context, t *chunkstore.Table, delta *chunkstore.Delta, acts []core.Action) []core.Action {
	d.out.Reset()
	d.g.SetTable(ctx, t, delta, &d.out)
	return d.flush(ctx, acts)
}

// SetMembership hands the mount the disks' loops and, when not nil, the shard map's groups.
func (d *Daemon) SetMembership(ctx *core.Context, plans []meta.Group, disks []meta.DiskAddr, acts []core.Action) []core.Action {
	d.out.Reset()
	d.g.SetMembership(ctx, plans, disks, &d.out)
	return d.flush(ctx, acts)
}

// OnEvent hands the mount a meta answer, a chunk reply, either client's timer or a lost peer.
func (d *Daemon) OnEvent(ctx *core.Context, ev *core.Event, acts []core.Action) []core.Action {
	d.out.Reset()
	d.g.OnEvent(ctx, ev, &d.out)
	return d.flush(ctx, acts)
}

// Serve reads and serves up to batch requests from the device; ErrGone once the kernel ended the
// connection.
func (d *Daemon) Serve(ctx *core.Context, acts []core.Action) ([]core.Action, error) {
	for range batch {
		buf := ctx.Pool.Bytes(d.buf)[:ReadBuffer]
		n, err := d.dev.Read(buf)
		if err != nil {
			return acts, err
		}
		if n == 0 {
			break
		}
		d.out.Reset()
		err = d.g.Request(ctx, time.Now().UnixNano(), buf[:n], &d.out)
		acts = d.flush(ctx, acts)
		if err != nil {
			return acts, err
		}
		if d.broken != nil {
			return acts, d.broken
		}
	}
	return acts, d.broken
}

// flush reads the credentials of each opener the mount asked for, writes the replies it framed and
// appends its actions to acts.
func (d *Daemon) flush(ctx *core.Context, acts []core.Action) []core.Action {
	for i := 0; i < len(d.out.Openers); i++ {
		p := d.out.Openers[i]
		d.g.OpenerDone(ctx, p.Token, d.opener(p.PID), &d.out)
	}
	for b := d.out.Replies; len(b) > 0; {
		n := binary.LittleEndian.Uint32(b)
		d.write(b[:n])
		b = b[n:]
	}
	for _, buf := range d.out.Data {
		b := ctx.Pool.Bytes(buf)
		d.write(b[:binary.LittleEndian.Uint32(b)])
		ctx.Pool.Free(buf)
	}
	return append(acts, d.out.Actions...)
}

// opener reads process pid's credentials from /proc, nil when it cannot, as once it exited. Its
// capabilities count only in the daemon's user namespace: root of a namespace of its own holds
// them over that namespace's objects, not the mount's.
func (d *Daemon) opener(pid uint32) *fsclient.Creds {
	proc := "/proc/" + strconv.FormatUint(uint64(pid), 10)
	b, err := os.ReadFile(proc + "/status")
	if err != nil || pid == 0 || !parseStatus(b, &d.creds) {
		return nil
	}
	ns, err := os.Readlink(proc + "/ns/user")
	if err != nil {
		return nil
	}
	if self, err := os.Readlink("/proc/self/ns/user"); err != nil || ns != self {
		d.creds.Override = false
	}
	return &d.creds
}

const capDacOverride = 1

// parseStatus takes the supplementary groups of a /proc/<pid>/status and whether its effective
// capabilities hold CAP_DAC_OVERRIDE into c; false unless it holds both lines, well formed.
func parseStatus(b []byte, c *fsclient.Creds) bool {
	c.Groups, c.Override = c.Groups[:0], false
	var groups, caps bool
	for line := range bytes.Lines(b) {
		k, v, _ := bytes.Cut(line, []byte(":"))
		switch string(k) {
		case "Groups":
			for f := range bytes.FieldsSeq(v) {
				g, err := strconv.ParseUint(string(f), 10, 32)
				if err != nil {
					return false
				}
				c.Groups = append(c.Groups, uint32(g))
			}
			groups = true
		case "CapEff":
			e, err := strconv.ParseUint(string(bytes.TrimSpace(v)), 16, 64)
			if err != nil {
				return false
			}
			c.Override, caps = e&(1<<capDacOverride) != 0, true
		}
	}
	return groups && caps
}

func (d *Daemon) write(frame []byte) {
	switch err := d.dev.Write(frame); {
	case errors.Is(err, unix.EINVAL):
		d.broken = fmt.Errorf("fuse: the kernel refused a reply's frame: %w", err)
	case err != nil && !errors.Is(err, ErrGone):
		d.refused = err
	}
}
