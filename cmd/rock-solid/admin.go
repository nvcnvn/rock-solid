package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/driver/tcp"
	"github.com/nvcnvn/rock-solid/internal/meta"
)

// adminConfig is where an operator's command goes: the control-plane group's voters, the group,
// and how long to keep trying.
type adminConfig struct {
	Ctls    []voter
	Group   uint64
	Timeout time.Duration
}

// attemptWait is how long the command line waits on one voter before it sends the command again,
// to the next voter.
const attemptWait = 5 * time.Second

const adminUsage = `usage:
  rock-solid disk lost <disk> -ctl node=host:port,...      declare a down disk lost for good
  rock-solid disk drain <disk> -ctl node=host:port,...     move every PG member off a disk
  rock-solid disk undrain <disk> -ctl node=host:port,...   end a disk's drain
  rock-solid disk remove <disk> -ctl node=host:port,...    remove a drained disk
  rock-solid disk failed -ctl node=host:port,...           list the devices nodes left failed at start
  rock-solid pool <pool> -ctl node=host:port,... [settings] create or change a pool
  rock-solid pg give-up <pool> <pg> -ctl node=host:port,...  give up what a PG's lost disks alone held
  rock-solid pg edit <pool> <pg> <disk,...> -ctl node=host:port,...    reorder a PG's members, primary first, unpinned
  rock-solid pg pin <pool> <pg> [<disk,...>] -ctl node=host:port,...   pin a PG, in that order if given
  rock-solid pg unpin <pool> <pg> -ctl node=host:port,...              unpin a PG`

// runAdmin runs an operator's command line, args after the program's name, and returns its exit
// code: 0 once the command took effect, 1 when the leader refused it or none answered, 2 for a
// command line that does not parse.
func runAdmin(args []string, stdout, stderr io.Writer) int {
	a, cfg, err := parseAdmin(args)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "rock-solid: %v\n%s\n", err, adminUsage)
		return 2
	}
	var unfound uint32
	if a.Op == meta.AdminGiveUp {
		an, err := sendAdmin(cfg, &meta.Admin{Op: meta.AdminPG, Pool: a.Pool, PG: a.PG}, nil)
		if err == nil && an.Refused != "" {
			err = errors.New("refused: " + an.Refused)
		}
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "rock-solid: %v\n", err)
			return 1
		}
		a.Interval, unfound = an.GiveUp.Interval, an.Unfound
	}
	an, err := sendAdmin(cfg, &a, nil)
	an.Unfound = unfound
	switch {
	case err != nil:
		_, _ = fmt.Fprintf(stderr, "rock-solid: %v\n", err)
		return 1
	case an.Refused != "":
		_, _ = fmt.Fprintf(stderr, "rock-solid: refused: %s\n", an.Refused)
		return 1
	}
	printDone(stdout, &a, &an)
	return 0
}

// printDone prints what command a, answered an, did; for a give-up, an.Unfound is the chunks its PG's primary reported
// unfound before it.
func printDone(stdout io.Writer, a *meta.Admin, an *meta.AdminAnswer) {
	switch a.Op {
	case meta.AdminLost:
		_, _ = fmt.Fprintf(stdout, "disk %d declared lost\n", a.Disk)
	case meta.AdminRemove:
		_, _ = fmt.Fprintf(stdout, "disk %d removed\n", a.Disk)
	case meta.AdminDrain:
		_, _ = fmt.Fprintf(stdout, "disk %d draining\n", a.Disk)
	case meta.AdminUndrain:
		_, _ = fmt.Fprintf(stdout, "disk %d no longer draining\n", a.Disk)
	case meta.AdminPool:
		_, _ = fmt.Fprintf(stdout, "pool %d set\n", a.Pool)
	case meta.AdminEdit:
		pin := "unpinned"
		if a.Pin {
			pin = "pinned"
		}
		_, _ = fmt.Fprintf(stdout, "PG %d.%d %s\n", a.Pool, a.PG, pin)
	case meta.AdminGiveUp:
		g := an.GiveUp
		switch {
		case g.All && g.Drop:
			_, _ = fmt.Fprintf(stdout, "PG %d.%d interval %d given up, every chunk dropped\n", a.Pool, a.PG, g.Interval)
		case g.All:
			_, _ = fmt.Fprintf(stdout, "PG %d.%d interval %d given up, every chunk kept as its members hold it\n", a.Pool, a.PG, g.Interval)
		default:
			_, _ = fmt.Fprintf(stdout, "PG %d.%d interval %d given up, %d chunks dropped\n", a.Pool, a.PG, g.Interval, len(g.Chunks))
		}
		for _, c := range g.Chunks {
			_, _ = fmt.Fprintf(stdout, "chunk %d/%d/%d dropped\n", c.Volume, c.Inode, c.Index)
		}
		if n := len(g.Chunks); !g.All && int(an.Unfound) > n {
			_, _ = fmt.Fprintf(stdout, "%d more chunks stay unfound: give up again\n", int(an.Unfound)-n)
		}
	case meta.AdminFailed:
		if len(an.Failed) == 0 {
			_, _ = fmt.Fprintln(stdout, "no failed devices")
		}
		for _, f := range an.Failed {
			_, _ = fmt.Fprintf(stdout, "node %d device %#x failed\n", f.Node, f.Device)
		}
	}
}

// parseAdmin parses `disk lost|remove|drain|undrain <disk>`, `disk failed`, `pool <pool> [settings]`, `pg give-up <pool> <pg>`,
// `pg edit <pool> <pg> <disks>`, `pg pin <pool> <pg> [<disks>]` or `pg unpin <pool> <pg>` with the flags that say where the command goes, before or after the positional arguments.
func parseAdmin(args []string) (meta.Admin, adminConfig, error) {
	var a meta.Admin
	var cfg adminConfig
	fs := flag.NewFlagSet("rock-solid", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	ctls := fs.String("ctl", "", "the control-plane group's voters as node=host:port, comma-separated")
	fs.Uint64Var(&cfg.Group, "group", 1, "the control-plane group's ID")
	fs.DurationVar(&cfg.Timeout, "timeout", 30*time.Second, "how long to keep trying the voters")
	pool := poolFlags(fs)
	pos, err := parseMixed(fs, args)
	if err != nil {
		return a, cfg, err
	}
	if cfg.Ctls, err = parseAddrs("-ctl", *ctls); err != nil {
		return a, cfg, err
	}
	pooled := false
	fs.Visit(func(f *flag.Flag) { pooled = pooled || pool.names[f.Name] })
	number := func(s string) (uint32, error) {
		n, err := strconv.ParseUint(s, 10, 32)
		if err != nil || n == 0 {
			return 0, fmt.Errorf("%q is not a number above 0", s)
		}
		return uint32(n), nil
	}
	switch {
	case len(pos) == 3 && pos[0] == "disk" && diskOps[pos[1]] != 0 && !pooled:
		a.Op = diskOps[pos[1]]
		a.Disk, err = number(pos[2])
	case len(pos) == 2 && pos[0] == "disk" && pos[1] == "failed" && !pooled:
		a.Op = meta.AdminFailed
	case len(pos) == 2 && pos[0] == "pool":
		a.Op = meta.AdminPool
		var id uint32
		id, err = number(pos[1])
		a.Pool = core.PoolID(id)
		if err == nil {
			a.Spec, err = pool.spec(fs)
		}
	case (len(pos) == 4 || len(pos) == 5) && pos[0] == "pg" && !pooled && pgOps[pos[1]].args[len(pos)-4]:
		a.Op, a.Pin = pgOps[pos[1]].op, pos[1] == "pin"
		var id uint32
		id, err = number(pos[2])
		a.Pool = core.PoolID(id)
		if err == nil {
			var n uint64
			n, err = strconv.ParseUint(pos[3], 10, 32)
			a.PG = uint32(n)
			if err != nil {
				err = fmt.Errorf("PG %q is not a number", pos[3])
			}
		}
		if len(pos) == 5 && err == nil {
			for d := range strings.SplitSeq(pos[4], ",") {
				var disk uint32
				if disk, err = number(d); err != nil {
					break
				}
				a.Disks = append(a.Disks, disk)
			}
		}
	default:
		err = errors.New("no such command")
	}
	return a, cfg, err
}

// diskOps are the commands on one disk, by name.
var diskOps = map[string]meta.AdminOp{"lost": meta.AdminLost, "remove": meta.AdminRemove, "drain": meta.AdminDrain, "undrain": meta.AdminUndrain}

// pgOps are the commands on one PG, by name, each with whether it takes no disk list and a disk list after the PG.
var pgOps = map[string]struct {
	op   meta.AdminOp
	args [2]bool
}{"give-up": {meta.AdminGiveUp, [2]bool{true, false}}, "edit": {meta.AdminEdit, [2]bool{false, true}},
	"pin": {meta.AdminEdit, [2]bool{true, true}}, "unpin": {meta.AdminEdit, [2]bool{true, false}}}

// parseMixed parses fs over args, whose positional arguments may come before, between or after the
// flags, and returns the positional ones.
func parseMixed(fs *flag.FlagSet, args []string) ([]string, error) {
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		if args = fs.Args(); len(args) == 0 {
			return pos, nil
		}
		pos, args = append(pos, args[0]), args[1:]
	}
}

// poolSettings are the pool command's flags, one per pool setting.
type poolSettings struct {
	preset                                                  *string
	replicas, minLive, data, parity, maxPerDomain, writeLim *int
	pgCount                                                 *uint
	encodeOnWrite, overcommit                               *bool
	sealed, domain, locality, class                         *string
	names                                                   map[string]bool
}

func poolFlags(fs *flag.FlagSet) *poolSettings {
	p := &poolSettings{
		preset:        fs.String("preset", "", "start from a preset pool: none, rep2 or rep3"),
		replicas:      fs.Int("replicas", 0, "replicas"),
		minLive:       fs.Int("min-live", 0, "min_live"),
		data:          fs.Int("data", 0, "data"),
		parity:        fs.Int("parity", 0, "parity"),
		maxPerDomain:  fs.Int("max-per-domain", 0, "max_per_domain"),
		writeLim:      fs.Int("write-limit", 0, "write_limit, in percent"),
		pgCount:       fs.Uint("pg-count", 0, "pg_count"),
		encodeOnWrite: fs.Bool("encode-on-write", false, "encode_on_write"),
		overcommit:    fs.Bool("overcommit", true, "overcommit"),
		sealed:        fs.String("sealed", "", "sealed: replicated or ec"),
		domain:        fs.String("failure-domain", "", "failure_domain: node or disk"),
		locality:      fs.String("locality", "", "locality: node or spread"),
		class:         fs.String("disk-class", "", "disk_class"),
	}
	p.names = map[string]bool{}
	fs.VisitAll(func(f *flag.Flag) { p.names[f.Name] = !slices.Contains([]string{"ctl", "group", "timeout"}, f.Name) })
	return p
}

// spec is the pool spec of the settings given, over the preset's if one is: a setting not given
// stays unset, so the leader keeps the pool's or, for a new pool, takes its default.
func (p *poolSettings) spec(fs *flag.FlagSet) (chunkstore.PoolSpec, error) {
	var s chunkstore.PoolSpec
	if *p.preset != "" {
		var ok bool
		if s, ok = chunkstore.Preset(*p.preset); !ok {
			return s, fmt.Errorf("-preset %q is none of none, rep2 or rep3", *p.preset)
		}
	}
	var err error
	fs.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "replicas":
			s.Replicas = new(*p.replicas)
		case "min-live":
			s.MinLive = new(*p.minLive)
		case "data":
			s.Data = new(*p.data)
		case "parity":
			s.Parity = new(*p.parity)
		case "max-per-domain":
			s.MaxPerDomain = new(*p.maxPerDomain)
		case "write-limit":
			if *p.writeLim < 0 || *p.writeLim > math.MaxUint8 {
				err = fmt.Errorf("-write-limit %d is past a percent", *p.writeLim)
			}
			s.WriteLimit = new(uint8(*p.writeLim))
		case "pg-count":
			if *p.pgCount > math.MaxUint32 {
				err = fmt.Errorf("-pg-count %d is past 32 bits", *p.pgCount)
			}
			s.PGCount = new(uint32(*p.pgCount))
		case "encode-on-write":
			s.EncodeOnWrite = new(*p.encodeOnWrite)
		case "overcommit":
			s.Overcommit = new(*p.overcommit)
		case "sealed":
			s.Sealed = *p.sealed
		case "failure-domain":
			s.FailureDomain = *p.domain
		case "locality":
			s.Locality = *p.locality
		case "disk-class":
			s.DiskClass = *p.class
		}
	})
	for _, n := range []*int{s.Replicas, s.MinLive, s.Data, s.Parity, s.MaxPerDomain} {
		if n != nil && (*n < 0 || *n > math.MaxInt32) {
			err = fmt.Errorf("a setting of %d is out of range", *n)
		}
	}
	return s, err
}

// sendAdmin sends command a to the control-plane leader and returns its answer, or for
// AdminMembership decodes the membership answering it into m. It tries the voters in turn, going
// to the leader a voter names, and sends the command again under the same request, which the
// leader answers once, until cfg.Timeout runs out.
func sendAdmin(cfg adminConfig, a *meta.Admin, m *meta.Membership) (meta.AdminAnswer, error) {
	pool := core.NewPool([][]byte{make([]byte, tcp.MaxData+core.BlockSize)}, 0)
	self := core.NodeID{Node: 1<<31 | rand.Uint32()>>1}
	request := rand.Uint64() | 1
	end := time.Now().Add(cfg.Timeout)
	last := errors.New("no voter tried")
	for at := 0; time.Now().Before(end); time.Sleep(200 * time.Millisecond) {
		v, deadline := cfg.Ctls[at], time.Now().Add(attemptWait)
		if deadline.After(end) {
			deadline = end
		}
		an, leader, known, err := tryAdmin(pool, cfg, v, self, request, a, m, deadline)
		switch {
		case err != nil:
			last = fmt.Errorf("voter %d at %v: %w", v.Node, v.Addr, err)
		case leader == nil:
			return an, nil
		case known:
			last = fmt.Errorf("voter %d is not the leader, node %d is", v.Node, leader.Node)
			if i := slices.IndexFunc(cfg.Ctls, func(c voter) bool { return c.Node == leader.Node }); i >= 0 && i != at {
				at = i
				continue
			}
		default:
			last = fmt.Errorf("voter %d is not the leader and knows none", v.Node)
		}
		at = (at + 1) % len(cfg.Ctls)
	}
	return meta.AdminAnswer{}, fmt.Errorf("no answer from the control-plane leader within %v: %w", cfg.Timeout, last)
}

// tryAdmin sends command a to voter v and waits until deadline for its answer, the membership into
// m for AdminMembership, or for the leader the voter names instead, nil when it answered.
func tryAdmin(pool core.BufPool, cfg adminConfig, v voter, self core.NodeID, request uint64, a *meta.Admin, m *meta.Membership, deadline time.Time) (meta.AdminAnswer, *core.NodeID, bool, error) {
	c, err := tcp.Dial(v.Addr, self, pool, time.Until(deadline))
	if err != nil {
		return meta.AdminAnswer{}, nil, false, err
	}
	defer func() { _ = c.Close() }()
	var msg core.Msg
	if err := meta.EncodeAdmin(pool, cfg.Group, request, a, &msg); err != nil {
		return meta.AdminAnswer{}, nil, false, err
	}
	if err := c.Send(&msg); err != nil {
		return meta.AdminAnswer{}, nil, false, err
	}
	for {
		if err := c.Recv(deadline, &msg); err != nil {
			return meta.AdminAnswer{}, nil, false, err
		}
		group, kind, token, ok := meta.Peek(&msg)
		switch {
		case !ok || group != cfg.Group:
		case kind == meta.KindAdminAnswer && token == request:
			an, err := meta.DecodeAdminAnswer(pool, &msg)
			freeMsg(pool, &msg)
			return an, nil, false, err
		case kind == meta.KindDisks && m != nil:
			err := meta.DecodeDisks(pool, &msg, m)
			freeMsg(pool, &msg)
			return meta.AdminAnswer{}, nil, false, err
		case kind == meta.KindLeader:
			at, known, err := meta.DecodeLeader(pool, &msg)
			freeMsg(pool, &msg)
			return meta.AdminAnswer{}, &at, known, err
		}
		freeMsg(pool, &msg)
	}
}

func freeMsg(pool core.BufPool, m *core.Msg) {
	if m.Buf != core.NoBuf {
		pool.Free(m.Buf)
		m.Buf = core.NoBuf
	}
}
