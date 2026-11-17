// Command rock-solid runs a rock-solid daemon: `rock-solid ctl` a voter of the control-plane group,
// `rock-solid node` a storage node.
package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"net/netip"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"syscall"
)

// ctlConfig is a control-plane voter's settings.
type ctlConfig struct {
	// Node is this voter's node ID, one of Voters.
	Node uint32
	// Voters are the group's voters, by node ID, each at its address.
	Voters []voter
	Group  uint64
	// Listen is the address this voter takes connections on.
	Listen netip.AddrPort
	// Disk is the file the group's log lives in; Format makes it, of DiskBytes bytes, and formats it.
	Disk      string
	Format    bool
	DiskBytes int64
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
	default:
		fmt.Fprintln(os.Stderr, "usage: rock-solid ctl|node [flags]")
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
		log.Fatal(err)
	}
}

func parseCtl(args []string) (ctlConfig, error) {
	var cfg ctlConfig
	fs := flag.NewFlagSet("ctl", flag.ContinueOnError)
	node := fs.Uint("node", 0, "this voter's node ID, one of -voters")
	voters := fs.String("voters", "", "the group's voters as node=host:port, comma-separated, the same voters on every voter and at every start")
	fs.Uint64Var(&cfg.Group, "group", 1, "the control-plane group's ID")
	listen := fs.String("listen", "", "the address to take connections on (default: this voter's address in -voters)")
	fs.StringVar(&cfg.Disk, "disk", "", "the file the group's log lives in")
	fs.BoolVar(&cfg.Format, "format", false, "create the disk file and format it, as at a voter's first start; a file already there is made again only if its superblocks were never written")
	fs.Int64Var(&cfg.DiskBytes, "disk-bytes", 1<<30, "the size of the disk file -format creates")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if *node > math.MaxUint32 {
		return cfg, fmt.Errorf("-node %d is past 32 bits", *node)
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

// parseAddrs parses s, node=host:port pairs separated by commas, each node above 0 and named once.
func parseAddrs(name, s string) ([]voter, error) {
	var vs []voter
	for _, v := range strings.Split(s, ",") {
		id, addr, ok := strings.Cut(v, "=")
		n, err := strconv.ParseUint(id, 10, 32)
		if !ok || err != nil || n == 0 {
			return nil, fmt.Errorf("%s: %q is not node=host:port with a node above 0", name, v)
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
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if *node > math.MaxUint32 || *pool > math.MaxUint32 {
		return cfg, fmt.Errorf("-node %d or -pool %d is past 32 bits", *node, *pool)
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
	case len(cfg.Devices) == 0:
		return cfg, errors.New("no -device")
	case len(slices.Compact(slices.Sorted(slices.Values(cfg.Devices)))) != len(cfg.Devices):
		return cfg, errors.New("a -device is named twice")
	case int(cfg.Listen.Port())+len(cfg.Devices) > 65535 || cfg.Listen.Port() == 0:
		return cfg, fmt.Errorf("-listen: port %d leaves no port for each device's loop", cfg.Listen.Port())
	}
	return cfg, nil
}
