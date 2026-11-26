// Command rock-solid runs a rock-solid daemon: `rock-solid ctl` a voter of the control-plane group,
// `rock-solid node` a storage node; and the operator's commands, `rock-solid disk` and
// `rock-solid pool`, which the control-plane leader answers.
package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand/v2"
	"net/netip"
	"os"
	"os/signal"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/meta"
	"github.com/nvcnvn/rock-solid/internal/node"
)

// minHeap is the least GOMEMLIMIT a budget sets: at 0 the collector would run back to back.
const minHeap = 64 << 20

// heapLimit holds the Go heap's limit, GOMEMLIMIT, at the daemon's memory budget less the slabs it
// has mapped, as each maps, and never below minHeap; a budget of 0 leaves the limit alone.
type heapLimit struct {
	budget  int64
	mu      sync.Mutex
	mapped  int64
	floored bool
	// set is debug.SetMemoryLimit, which tests replace.
	set func(int64) int64
}

func newHeapLimit(budgetMiB int64) *heapLimit {
	return &heapLimit{budget: budgetMiB << 20, set: debug.SetMemoryLimit}
}

// add counts n more bytes mapped.
func (h *heapLimit) add(n int) {
	if h.budget == 0 {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.mapped += int64(n)
	if h.budget-h.mapped < minHeap && !h.floored {
		h.floored = true
		log.Printf("memory: %d MiB of slabs leave less than %d MiB of the -memory %d MiB for the Go heap; GOMEMLIMIT stays at %d MiB",
			h.mapped>>20, minHeap>>20, h.budget>>20, minHeap>>20)
	}
	h.set(max(h.budget-h.mapped, minHeap))
}

// check reports a budget that cannot hold the slabs mapped at start, pages more bytes of page slabs
// the start will map, and minHeap.
func (h *heapLimit) check(pages int64) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.budget != 0 && h.budget < h.mapped+pages+minHeap {
		return fmt.Errorf("-memory %d MiB is under the %d MiB of slabs at start plus a %d MiB Go heap",
			h.budget>>20, (h.mapped+pages)>>20, minHeap>>20)
	}
	return nil
}

// ctlConfig is a control-plane voter's settings.
type ctlConfig struct {
	// Node is this voter's node ID, one of Voters.
	Node uint32
	// Voters are the voters, by node ID, each at its address: the group's configuration only at the first start.
	Voters []voter
	Group  uint64
	// Listen is the address this voter takes connections on.
	Listen netip.AddrPort
	// Disk is the file the group's log lives in; Format makes it, of DiskBytes bytes, and formats it.
	Disk      string
	Format    bool
	DiskBytes int64
	// Memory is the daemon's memory budget in MiB, 0 for none.
	Memory int64
	// MaxReplayBytes is the log's max_replay_bytes.
	MaxReplayBytes uint64
}

type voter struct {
	Node uint32
	Addr netip.AddrPort
}

// nodeConfig is a storage node's settings.
type nodeConfig struct {
	Node uint32
	// Ctls are the control-plane group's voters, each at its address.
	Ctls  []voter
	Group uint64
	// Listen is the address of the node's first loop; loop k listens on its port plus k.
	Listen netip.AddrPort
	// Peers are the other storage nodes, each at its first loop's address.
	Peers []voter
	// Devices are the paths of the node's devices, its loops 1 on.
	Devices []string
	Class   string
	Pool    uint32
	// Memory is the daemon's memory budget in MiB, 0 for none.
	Memory int64
	// MetaOnly is role meta: the node holds metadata and no PG member; Client is role client: it
	// holds a client session and neither metadata nor a PG member.
	MetaOnly bool
	Client   bool
}

// release is the format level this build's nodes report.
const release = 1

// machine is the configuration of cfg's node machine.
func (cfg nodeConfig) machine() node.Config {
	nc := node.Config{Node: cfg.Node, Release: release, Group: cfg.Group, Seed: rand.Uint64(), MetaOnly: cfg.MetaOnly, Client: cfg.Client}
	for _, v := range cfg.Ctls {
		nc.Ctls = append(nc.Ctls, core.NodeID{Node: v.Node})
	}
	return nc
}

// daemon is a process's loops, run until Stop.
type daemon interface {
	Run() error
	Stop()
}

func main() {
	var p daemon
	var err error
	switch {
	case len(os.Args) >= 2 && os.Args[1] == "ctl":
		var cfg ctlConfig
		if cfg, err = parseCtl(os.Args[2:]); err == nil {
			p, err = newCtl(cfg)
		}
	case len(os.Args) >= 2 && os.Args[1] == "node":
		var cfg nodeConfig
		if cfg, err = parseNode(os.Args[2:]); err == nil {
			p, err = newNode(cfg)
		}
	case len(os.Args) >= 2 && (os.Args[1] == "disk" || os.Args[1] == "pool" || os.Args[1] == "pg"):
		os.Exit(runAdmin(os.Args[1:], os.Stdout, os.Stderr))
	case len(os.Args) >= 2 && os.Args[1] == "volume":
		os.Exit(runVolume(os.Args[1:], os.Stdout, os.Stderr))
	default:
		fmt.Fprintln(os.Stderr, "usage: rock-solid ctl|node|disk|pool|pg|volume [flags]")
		os.Exit(2)
	}
	if err != nil {
		log.Fatal(err)
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		p.Stop()
	}()
	if err := p.Run(); err != nil {
		log.Print(err)
		os.Exit(exitStatus(err))
	}
}

// exitDeleted is the exit status of a control-plane voter whose group deleted it: it exits so at
// every start, so a service manager should not restart it (systemd's RestartPreventExitStatus).
const exitDeleted = 3

func exitStatus(err error) int {
	if errors.Is(err, meta.ErrDeleted) {
		return exitDeleted
	}
	return 1
}

func parseCtl(args []string) (ctlConfig, error) {
	var cfg ctlConfig
	fs := flag.NewFlagSet("ctl", flag.ContinueOnError)
	node := fs.Uint("node", 0, "this voter's node ID, one of -voters")
	voters := fs.String("voters", "", "the group's voters as node=host:port, comma-separated: their addresses, and the group's configuration at the voter's first start alone")
	fs.Uint64Var(&cfg.Group, "group", 1, "the control-plane group's ID")
	listen := fs.String("listen", "", "the address to take connections on (default: this voter's address in -voters)")
	fs.StringVar(&cfg.Disk, "disk", "", "the file the group's log lives in")
	fs.BoolVar(&cfg.Format, "format", false, "create the disk file and format it, as at a voter's first start; a file already there is made again only if its superblocks were never written")
	fs.Int64Var(&cfg.DiskBytes, "disk-bytes", 1<<30, "the size of the disk file -format creates")
	fs.Int64Var(&cfg.Memory, "memory", 0, "the daemon's memory budget in MiB, which GOMEMLIMIT is held at less the slabs mapped outside the Go heap; 0 leaves GOMEMLIMIT alone")
	fs.Uint64Var(&cfg.MaxReplayBytes, "max-replay-bytes", disklog.DefaultConfig().MaxReplayBytes, "the bytes of log records after the last checkpoint that begin the next, at most a quarter of the disk; a restart replays at most twice this")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if *node >= daemonNodes {
		return cfg, fmt.Errorf("-node %d is not below %d, where a daemon's node lies", *node, daemonNodes)
	}
	if cfg.Group == math.MaxUint64 {
		return cfg, errors.New("-group leaves no ID for the first data group, the next one")
	}
	if cfg.Memory < 0 || cfg.Memory >= 1<<43 {
		return cfg, fmt.Errorf("-memory %d MiB is not 0 or a budget", cfg.Memory)
	}
	cfg.Node = uint32(*node)
	var err error
	if cfg.Voters, err = parseAddrs("-voters", *voters); err != nil {
		return cfg, err
	}
	i := slices.IndexFunc(cfg.Voters, func(v voter) bool { return v.Node == cfg.Node })
	switch {
	case i < 0:
		return cfg, fmt.Errorf("-node %d is not one of -voters", cfg.Node)
	case cfg.Disk == "":
		return cfg, errors.New("-disk names no file")
	case *listen == "":
		cfg.Listen = cfg.Voters[i].Addr
	default:
		ap, err := netip.ParseAddrPort(*listen)
		if err != nil {
			return cfg, fmt.Errorf("-listen: %w", err)
		}
		cfg.Listen = ap
	}
	return cfg, nil
}

// replayFits refuses a max_replay_bytes above a quarter of a disk of diskBytes: the log's reserve
// keeps room for a checkpoint of up to twice it, and the records it bounds take up to twice it again.
func replayFits(maxReplayBytes, diskBytes uint64) error {
	if maxReplayBytes > diskBytes/4 {
		return fmt.Errorf("-max-replay-bytes %d is above a quarter of the disk's %d bytes", maxReplayBytes, diskBytes)
	}
	return nil
}

// daemonNodes bounds every daemon's node ID; the command line draws its own above it, so it never
// takes a daemon's.
const daemonNodes = 1 << 31

// parseAddrs parses s, node=host:port pairs separated by commas, each node above 0, below
// daemonNodes and named once.
func parseAddrs(name, s string) ([]voter, error) {
	var vs []voter
	for _, v := range strings.Split(s, ",") {
		id, addr, ok := strings.Cut(v, "=")
		n, err := strconv.ParseUint(id, 10, 32)
		if !ok || err != nil || n == 0 || n >= daemonNodes {
			return nil, fmt.Errorf("%s: %q is not node=host:port with a node above 0 and below %d", name, v, uint64(daemonNodes))
		}
		ap, err := netip.ParseAddrPort(addr)
		if err != nil {
			return nil, fmt.Errorf("%s: %q: %w", name, v, err)
		}
		if slices.ContainsFunc(vs, func(x voter) bool { return x.Node == uint32(n) }) {
			return nil, fmt.Errorf("%s: node %d named twice", name, n)
		}
		vs = append(vs, voter{Node: uint32(n), Addr: ap})
	}
	return vs, nil
}

// devices is the -device flag, given once per device.
type devices []string

func (d *devices) String() string     { return strings.Join(*d, ",") }
func (d *devices) Set(s string) error { *d = append(*d, s); return nil }

func parseNode(args []string) (nodeConfig, error) {
	var cfg nodeConfig
	fs := flag.NewFlagSet("node", flag.ContinueOnError)
	node := fs.Uint("node", 0, "this node's ID, above 0 and no voter's")
	ctls := fs.String("ctl", "", "the control-plane group's voters as node=host:port, comma-separated")
	fs.Uint64Var(&cfg.Group, "group", 1, "the control-plane group's ID")
	listen := fs.String("listen", "", "the address of the node's first loop; device k's loop listens on its port plus k")
	peers := fs.String("peers", "", "the other storage nodes as node=host:port, comma-separated, each its first loop's address")
	fs.Var((*devices)(&cfg.Devices), "device", "a device of the node, by a path that names the drive at every start, such as its /dev/disk/by-id link; given once per device")
	fs.StringVar(&cfg.Class, "class", "ssd", "the class of the node's devices")
	pool := fs.Uint("pool", 1, "the pool the node's stores serve")
	fs.Int64Var(&cfg.Memory, "memory", 0, "the daemon's memory budget in MiB, which GOMEMLIMIT is held at less the slabs mapped outside the Go heap; 0 leaves GOMEMLIMIT alone")
	role := fs.String("role", "storage", "storage, holding PG members and metadata, meta, holding metadata alone, or client, holding a client session and no device")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if *role != "storage" && *role != "meta" && *role != "client" {
		return cfg, fmt.Errorf("-role %q is not storage, meta or client", *role)
	}
	cfg.MetaOnly, cfg.Client = *role == "meta", *role == "client"
	if *node >= daemonNodes || *pool > math.MaxUint32 {
		return cfg, fmt.Errorf("-node %d is not below %d, where a daemon's node lies, or -pool %d is past 32 bits", *node, daemonNodes, *pool)
	}
	if cfg.Group == math.MaxUint64 {
		return cfg, errors.New("-group leaves no ID for the first data group, the next one")
	}
	if cfg.Memory < 0 || cfg.Memory >= 1<<43 {
		return cfg, fmt.Errorf("-memory %d MiB is not 0 or a budget", cfg.Memory)
	}
	cfg.Node, cfg.Pool = uint32(*node), uint32(*pool)
	var err error
	if cfg.Ctls, err = parseAddrs("-ctl", *ctls); err != nil {
		return cfg, err
	}
	if *peers != "" {
		if cfg.Peers, err = parseAddrs("-peers", *peers); err != nil {
			return cfg, err
		}
	}
	if cfg.Listen, err = netip.ParseAddrPort(*listen); err != nil {
		return cfg, fmt.Errorf("-listen: %w", err)
	}
	named := func(n uint32) bool {
		return slices.ContainsFunc(append(slices.Clone(cfg.Ctls), cfg.Peers...), func(v voter) bool { return v.Node == n })
	}
	switch {
	case cfg.Node == 0 || named(cfg.Node):
		return cfg, fmt.Errorf("-node %d is 0, a voter or a peer", cfg.Node)
	case slices.ContainsFunc(cfg.Peers, func(v voter) bool {
		return slices.ContainsFunc(cfg.Ctls, func(c voter) bool { return c.Node == v.Node })
	}):
		return cfg, errors.New("-peers names a voter")
	case len(cfg.Devices) == 0 && !cfg.Client:
		return cfg, errors.New("no -device")
	case len(cfg.Devices) != 0 && cfg.Client:
		return cfg, errors.New("a -device on role client")
	case len(slices.Compact(slices.Sorted(slices.Values(cfg.Devices)))) != len(cfg.Devices):
		return cfg, errors.New("a -device is named twice")
	case int(cfg.Listen.Port())+len(cfg.Devices) > 65535 || cfg.Listen.Port() == 0:
		return cfg, fmt.Errorf("-listen: port %d leaves no port for each device's loop", cfg.Listen.Port())
	}
	return cfg, nil
}
