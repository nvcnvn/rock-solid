package main

import (
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/ops"
)

// runNode runs a node of cfg in the test process, its admin port on a port of its own.
func runNode(t *testing.T, cfg nodeConfig) *storageNode {
	t.Helper()
	cfg.Admin = "127.0.0.1:0"
	p, err := newNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- p.Run() }()
	t.Cleanup(func() {
		p.Stop()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	return p
}

// getFacts is the node's answer on GET /facts.
func getFacts(t *testing.T, p *storageNode) (ops.Facts, error) {
	t.Helper()
	rp, err := http.Get("http://" + p.facts.ln.Addr().String() + "/facts")
	if err != nil {
		return ops.Facts{}, err
	}
	defer func() { _ = rp.Body.Close() }()
	b, err := io.ReadAll(rp.Body)
	if err != nil {
		return ops.Facts{}, err
	}
	if rp.StatusCode != http.StatusOK {
		return ops.Facts{}, fmt.Errorf("%s: %s", rp.Status, b)
	}
	return ops.DecodeFacts(b)
}

func local(port int) netip.AddrPort {
	return netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(port))
}

// A storage node's answer holds each loop's part, each copied by one call on its loop per request:
// the node machine's on loop 0, each device's disk and the MetaStore replica the shard map places
// on it on the device's loop; a control-plane voter's holds its replica and, as leader, the table.
func TestFactsAnsweredByEachLoop(t *testing.T) {
	dir := testDir(t)
	c := runCtl(t, dir)
	a, b := filepath.Join(dir, "facts-dev-a"), filepath.Join(dir, "facts-dev-b")
	device(t, a, false)
	device(t, b, false)
	base := freeBase(t, 3)
	p := runNode(t, nodeConfig{Node: 2, Ctls: []voter{{1, local(c.Port)}}, Group: ctlGroup, Listen: local(base), Devices: []string{a, b},
		Class: "ssd", Pool: 1, Flags: []ops.Flag{{Name: "node", Value: "2"}}})
	var mu sync.Mutex
	calls := map[int]int{}
	fill := p.facts.fill
	p.facts.fill = func(i int, ctx *core.Context, f *ops.Facts) {
		mu.Lock()
		calls[i]++
		mu.Unlock()
		fill(i, ctx, f)
	}
	var f ops.Facts
	if !within(60*time.Second, func() bool {
		var err error
		if f, err = getFacts(t, p); err != nil {
			t.Fatal(err)
		}
		return len(f.Disks) == 2 && f.Disks[0].State == "open" && f.Disks[1].State == "open" && len(f.Replicas) == 1 && f.Replicas[0].Leader == f.Replicas[0].ID
	}) {
		t.Fatalf("the node never reported both disks open and a replica leading: %+v", f)
	}
	mu.Lock()
	if calls[0] != calls[1] || calls[1] != calls[2] || len(calls) != 3 {
		t.Fatalf("calls per loop %v", calls)
	}
	mu.Unlock()
	if f.Node.ID != 2 || !slices.Equal(f.Node.Roles, []string{"storage"}) || f.Node.BootUs == 0 || f.Node.Run == 0 || f.Node.Wall == "" ||
		f.Map.ReportsTo != 1 || len(f.Node.Hosts) != 1 || f.Node.Hosts[0] != (ops.Host{Node: 1, Addr: local(c.Port).String(), Ctl: true}) {
		t.Fatalf("the node's part: %+v %+v", f.Node, f.Map)
	}
	for i, k := range f.Disks {
		if k.Loop != uint32(i+1) || k.Path != []string{a, b}[i] || k.Device != deviceID(k.Path) || k.Disk == 0 || k.Log == 0 || k.Run == 0 {
			t.Fatalf("disk %d: %+v", i, k)
		}
	}
	if r := f.Replicas[0]; r.Group != dataGroup || r.Loop == 0 || r.Leader != r.ID || r.Commit == 0 {
		t.Fatalf("the replica %+v", r)
	}

	if err := c.facts.listen(""); err != nil {
		t.Fatal(err)
	}
	body, err := c.facts.answer("/facts", c.facts.facts)
	if err != nil {
		t.Fatal(err)
	}
	v, err := ops.DecodeFacts(body)
	if err != nil {
		t.Fatal(err)
	}
	if len(v.Replicas) != 1 || v.Replicas[0].Group != ctlGroup || v.Replicas[0].Leader != 1 || v.Map.ReportsTo != 1 ||
		!slices.Equal(v.Node.Roles, []string{"control-plane"}) {
		t.Fatalf("the voter's answer: %+v", v)
	}
}

// A storage node and a client-only node whose voter does not answer still answer from their own
// state: the device waiting for its first membership, the client's session starting.
func TestFactsWithoutControlPlane(t *testing.T) {
	dir := testDir(t)
	a := filepath.Join(dir, "facts-alone-dev")
	device(t, a, false)
	base := freeBase(t, 4)
	ctls := []voter{{1, local(base + 3)}}
	p := runNode(t, nodeConfig{Node: 2, Ctls: ctls, Group: ctlGroup, Listen: local(base), Devices: []string{a}, Class: "ssd", Pool: 1})
	q := runNode(t, nodeConfig{Node: 3, Ctls: ctls, Group: ctlGroup, Listen: local(base + 2), Client: true})
	f, err := getFacts(t, p)
	if err != nil {
		t.Fatal(err)
	}
	if f.Node.ID != 2 || len(f.Disks) != 1 || f.Disks[0].State != "waiting" || f.Disks[0].Path != a || f.Map.Epoch != 0 || f.Map.ReportsTo != 1 {
		t.Fatalf("the storage node's answer: %+v", f)
	}
	g, err := getFacts(t, q)
	if err != nil {
		t.Fatal(err)
	}
	if g.Node.ID != 3 || !slices.Equal(g.Node.Roles, []string{"client"}) || len(g.Disks) != 0 || g.Session.LeaseLeftUs != 0 {
		t.Fatalf("the client's answer: %+v", g)
	}
}
