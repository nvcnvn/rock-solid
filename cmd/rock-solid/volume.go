package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/netip"
	"slices"
	"time"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/driver/tcp"
	"github.com/nvcnvn/rock-solid/internal/meta"
	"go.etcd.io/raft/v3"
)

const volumeUsage = `usage:
  rock-solid volume create -pool <pool> -size <bytes> [-case-index] -ctl node=host:port,... -peers node=host:port,...
      make a filesystem volume; -peers names the storage nodes by their first loop's address`

// volumeConfig is a volume create's settings: where the control plane and the storage nodes are,
// and the volume's record.
type volumeConfig struct {
	adminConfig
	Peers  []voter
	Volume meta.Volume
}

// runVolume runs `rock-solid volume create`, args after the program's name, and returns its exit
// code as runAdmin does.
func runVolume(args []string, stdout, stderr io.Writer) int {
	cfg, err := parseVolume(args)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "rock-solid: %v\n%s\n", err, volumeUsage)
		return 2
	}
	id, err := createVolume(cfg, time.Now().UnixNano())
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "rock-solid: %v\n", err)
		return 1
	}
	_, _ = fmt.Fprintf(stdout, "volume %d created\n", id)
	return 0
}

func parseVolume(args []string) (volumeConfig, error) {
	var cfg volumeConfig
	fs := flag.NewFlagSet("rock-solid", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	ctls := fs.String("ctl", "", "the control-plane group's voters as node=host:port, comma-separated")
	peers := fs.String("peers", "", "the storage nodes as node=host:port of their first loop, comma-separated")
	fs.Uint64Var(&cfg.Group, "group", 1, "the control-plane group's ID")
	fs.DurationVar(&cfg.Timeout, "timeout", 30*time.Second, "how long to keep trying")
	pool := fs.Uint("pool", 0, "the pool the volume's data goes to")
	fs.Uint64Var(&cfg.Volume.Size, "size", 0, "the volume's size in bytes")
	fs.BoolVar(&cfg.Volume.CaseIndex, "case-index", false, "keep case-folded names, for volumes shared over SMB")
	pos, err := parseMixed(fs, args)
	switch {
	case err != nil:
		return cfg, err
	case !slices.Equal(pos, []string{"volume", "create"}):
		return cfg, errors.New("no such command")
	case *pool == 0 || *pool > math.MaxUint32:
		return cfg, fmt.Errorf("-pool %d is not a pool ID above 0", *pool)
	case cfg.Volume.Size == 0:
		return cfg, errors.New("-size must be above 0")
	}
	cfg.Volume.Pool = core.PoolID(*pool)
	if cfg.Ctls, err = parseAddrs("-ctl", *ctls); err != nil {
		return cfg, err
	}
	cfg.Peers, err = parseAddrs("-peers", *peers)
	return cfg, err
}

// createVolume creates a volume with cfg's record and a root directory of time now, and returns
// its ID. It reads the shard map and the disks' loops from the control-plane leader until they
// place the create, sends it to the data group's leader under a replay key, sending the same Txn
// again until it is answered, and builds it again only for the ID a failed counter check answers
// with.
func createVolume(cfg volumeConfig, now int64) (uint32, error) {
	root := meta.Attr{Mode: meta.ModeDir | 0o755, Nlink: 2, Atime: now, Mtime: now, Ctime: now}
	f := meta.FS{Volume: 1, Req: &meta.Request{ID: rand.Uint64()}}
	end := time.Now().Add(cfg.Timeout)
	var m meta.Membership
	for {
		if err := f.CreateVolume(&cfg.Volume, &root); err != nil {
			return 0, err
		}
		g, err := placed(cfg, &m, &f.Txn, end)
		if err != nil {
			return 0, err
		}
		r, err := sendTxn(cfg.Peers, &m, g, &f.Txn, end)
		switch {
		case errors.Is(err, errUnanswered):
			return 0, fmt.Errorf("%w; volume %d may exist, and a second create would make another: no command lists volumes yet, its record is volume/%d/record in group %d", err, f.Volume, f.Volume, g.ID)
		case err != nil:
			return 0, err
		case r.FailedCheck == -1:
			return f.Volume, nil
		case r.FailedCheck != 0:
			return 0, fmt.Errorf("volume %d's root exists, though the volume ID counter hands out its ID", f.Volume)
		}
		next := uint64(1)
		if len(r.Actual) == 8 {
			next = binary.BigEndian.Uint64(r.Actual)
		}
		if next == 0 || next > math.MaxUint32 {
			return 0, fmt.Errorf("the volume ID counter holds %x", r.Actual)
		}
		f.Volume = uint32(next)
	}
}

// errNotYet is a membership that does not yet say where a create goes.
var errNotYet = errors.New("not yet known")

// placed reads the membership into m until it places t, or until end, and returns the group t routes to.
func placed(cfg volumeConfig, m *meta.Membership, t *meta.Txn, end time.Time) (*meta.Group, error) {
	for {
		if _, err := sendAdmin(cfg.adminConfig, &meta.Admin{Op: meta.AdminMembership}, m); err != nil {
			return nil, err
		}
		g, err := target(m, cfg.Volume.Pool, t)
		if !errors.Is(err, errNotYet) || !time.Now().Before(end) {
			return g, err
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// target is the group of m that t routes to. It refuses a pool m's grants do not hold, unless they
// hold none, as a control-plane leader's do until its first grant step; that, no shard map or a
// group with no voter, as one committed before its node's disk was up, wraps errNotYet.
func target(m *meta.Membership, pool core.PoolID, t *meta.Txn) (*meta.Group, error) {
	switch {
	case len(m.Grants) == 0:
		return nil, fmt.Errorf("%w: the control plane knows no pool", errNotYet)
	case !slices.ContainsFunc(m.Grants, func(g meta.PoolGrant) bool { return g.Pool == pool }):
		return nil, fmt.Errorf("pool %d does not exist", pool)
	}
	smap := meta.ShardMap{Groups: m.Plans}
	if err := smap.CheckRanges(); err != nil {
		return nil, fmt.Errorf("%w: no shard map: %w", errNotYet, err)
	}
	id, err := smap.Route(t)
	if err != nil {
		return nil, err
	}
	g := &m.Plans[slices.IndexFunc(m.Plans, func(g meta.Group) bool { return g.ID == id })]
	if len(g.Voters) == 0 {
		return nil, fmt.Errorf("%w: group %d has no voter", errNotYet, id)
	}
	return g, nil
}

// errUnanswered is a Txn sent and never answered, which may have committed.
var errUnanswered = errors.New("sent and not answered")

// sendTxn sends t to group g's leader and returns its result. It tries g's voters in turn at the
// loops m names for their disks, each at its node's first-loop address in peers plus the loop,
// goes to the leader a NotLeader names, and sends t again, unchanged, after a dropped answer, a
// lost connection or attemptWait of silence, until end; any other error is the Txn's.
func sendTxn(peers []voter, m *meta.Membership, g *meta.Group, t *meta.Txn, end time.Time) (meta.Result, error) {
	pool := core.NewPool([][]byte{make([]byte, tcp.MaxData+core.BlockSize)}, 0)
	self := core.NodeID{Node: 1<<31 | rand.Uint32()>>1}
	token := rand.Uint64() >> 1
	last, sent := errors.New("no voter tried"), false
	for at := 0; time.Now().Before(end); time.Sleep(200 * time.Millisecond) {
		v := g.Voters[at%len(g.Voters)]
		at++
		addr, err := loopAddr(peers, m, v)
		if err != nil {
			last = err
			continue
		}
		deadline := time.Now().Add(attemptWait)
		if deadline.After(end) {
			deadline = end
		}
		a, out, err := tryTxn(pool, addr, self, g.ID, token, t, deadline)
		sent = sent || out
		var nl meta.NotLeader
		switch {
		case err != nil:
			last = fmt.Errorf("replica %d of group %d at %v: %w", v.ID, g.ID, addr, err)
		case errors.As(a.Err, &nl):
			last = a.Err
			if i := slices.IndexFunc(g.Voters, func(r meta.Replica) bool { return r.ID == nl.Leader }); nl.Leader != 0 && i >= 0 {
				at = i
			} else {
				time.Sleep(time.Second)
			}
		case errors.Is(a.Err, raft.ErrProposalDropped) || errors.Is(a.Err, meta.ErrOverloaded):
			last = a.Err
			at--
		case errors.Is(a.Err, meta.ErrHalted):
			last = a.Err
		case a.Err != nil:
			return meta.Result{}, a.Err
		default:
			return a.Result, nil
		}
	}
	if sent {
		return meta.Result{}, fmt.Errorf("%w by group %d's leader: %w", errUnanswered, g.ID, last)
	}
	return meta.Result{}, fmt.Errorf("no answer from group %d's leader: %w", g.ID, last)
}

// loopAddr is the address of the loop m names for voter v's disk.
func loopAddr(peers []voter, m *meta.Membership, v meta.Replica) (netip.AddrPort, error) {
	i := slices.IndexFunc(m.Disks, func(d meta.DiskAddr) bool { return d.Disk == v.Disk })
	if i < 0 {
		return netip.AddrPort{}, fmt.Errorf("disk %d of replica %d has no loop in the membership", v.Disk, v.ID)
	}
	at := m.Disks[i].At
	j := slices.IndexFunc(peers, func(p voter) bool { return p.Node == at.Node })
	if j < 0 {
		return netip.AddrPort{}, fmt.Errorf("-peers names no node %d", at.Node)
	}
	return netip.AddrPortFrom(peers[j].Addr.Addr(), peers[j].Addr.Port()+uint16(at.Loop)), nil
}

// tryTxn sends t under token to the loop at addr and waits until deadline for its answer; it
// reports whether it sent t.
func tryTxn(pool core.BufPool, addr netip.AddrPort, self core.NodeID, group, token uint64, t *meta.Txn, deadline time.Time) (meta.Answer, bool, error) {
	c, err := tcp.Dial(addr, self, pool, time.Until(deadline))
	if err != nil {
		return meta.Answer{}, false, err
	}
	defer func() { _ = c.Close() }()
	var msg core.Msg
	if err := meta.EncodeTxn(pool, group, token, t, &msg); err != nil {
		return meta.Answer{}, false, err
	}
	if err := c.Send(&msg); err != nil {
		return meta.Answer{}, true, err
	}
	for {
		if err := c.Recv(deadline, &msg); err != nil {
			return meta.Answer{}, true, err
		}
		g, kind, tok, ok := meta.Peek(&msg)
		if ok && g == group && kind == meta.KindAnswer && tok == token {
			var a meta.Answer
			err := meta.DecodeAnswer(pool, &msg, &a)
			freeMsg(pool, &msg)
			return a, true, err
		}
		freeMsg(pool, &msg)
	}
}
