// Command rock-solid runs a rock-solid daemon; `rock-solid ctl` runs a voter of the control-plane group.
package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
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

func main() {
	if len(os.Args) < 2 || os.Args[1] != "ctl" {
		fmt.Fprintln(os.Stderr, "usage: rock-solid ctl [flags]")
		os.Exit(2)
	}
	cfg, err := parseCtl(os.Args[2:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	p, err := newCtl(cfg)
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
	fs.BoolVar(&cfg.Format, "format", false, "create the disk file and format it, as at a voter's first start")
	fs.Int64Var(&cfg.DiskBytes, "disk-bytes", 1<<30, "the size of the disk file -format creates")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	cfg.Node = uint32(*node)
	for _, v := range strings.Split(*voters, ",") {
		id, addr, ok := strings.Cut(v, "=")
		n, err := strconv.ParseUint(id, 10, 32)
		if !ok || err != nil || n == 0 {
			return cfg, fmt.Errorf("-voters: %q is not node=host:port with a node above 0", v)
		}
		ap, err := netip.ParseAddrPort(addr)
		if err != nil {
			return cfg, fmt.Errorf("-voters: %q: %w", v, err)
		}
		if slices.ContainsFunc(cfg.Voters, func(x voter) bool { return x.Node == uint32(n) }) {
			return cfg, fmt.Errorf("-voters: node %d named twice", n)
		}
		cfg.Voters = append(cfg.Voters, voter{Node: uint32(n), Addr: ap})
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
