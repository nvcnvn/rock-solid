package main

import (
	"fmt"
	"io"
	"maps"
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

// get is the answer on path from the admin port s listens on, an error unless 200 OK.
func get(s *facts, path string) ([]byte, error) {
	rp, err := http.Get("http://" + s.ln.Addr().String() + path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rp.Body.Close() }()
	b, err := io.ReadAll(rp.Body)
	if err != nil {
		return nil, err
	}
	if rp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: %s", rp.Status, b)
	}
	return b, nil
}

// getFacts is the decoded answer on GET /facts from s's admin port.
func getFacts(t *testing.T, s *facts) ops.Facts {
	t.Helper()
	b, err := get(s, "/facts")
	if err != nil {
		t.Fatal(err)
	}
	f, err := ops.DecodeFacts(b)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func local(port int) netip.AddrPort {
	return netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(port))
}

// countFills counts each loop's calls of s.fill and s.pg.
func countFills(s *facts) func() map[int]int {
	var mu sync.Mutex
	calls := map[int]int{}
	fill, pg := s.fill, s.pg
	s.fill = func(i int, ctx *core.Context, f *ops.Facts) {
		mu.Lock()
		calls[i]++
		mu.Unlock()
		fill(i, ctx, f)
	}
	s.pg = func(i int, p *ops.PGFacts) bool {
		mu.Lock()
		calls[i]++
		mu.Unlock()
		return pg(i, p)
	}
	return func() map[int]int {
		mu.Lock()
		defer mu.Unlock()
		return maps.Clone(calls)
	}
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
	calls := countFills(&p.facts)
	var f ops.Facts
	if !within(60*time.Second, func() bool {
		f = getFacts(t, &p.facts)
		return len(f.Disks) == 2 && f.Disks[0].State == "open" && f.Disks[1].State == "open" && len(f.Replicas) == 1 && f.Replicas[0].Leader == f.Replicas[0].ID
	}) {
		t.Fatalf("the node never reported both disks open and a replica leading: %+v", f)
	}
	if n := calls(); n[0] != n[1] || n[1] != n[2] || len(n) != 3 {
		t.Fatalf("calls per loop %v", n)
	}
	if f.Node.ID != 2 || !slices.Equal(f.Node.Roles, []string{"storage"}) || f.Node.BootUs == 0 || f.Node.Run == 0 || f.Node.Wall == "" ||
		f.Map.ReportsTo != 1 || len(f.Node.Hosts) != 1 || f.Node.Hosts[0] != (ops.Host{Node: 1, Addr: local(c.Port).String(), Ctl: true}) || f.Unanswered != nil {
		t.Fatalf("the node's part: %+v %+v %v", f.Node, f.Map, f.Unanswered)
	}
	for i, k := range f.Disks {
		if k.Loop != uint32(i+1) || k.Path != []string{a, b}[i] || k.Device != deviceID(k.Path) || k.Disk == 0 || k.Log == 0 || k.Run == 0 {
			t.Fatalf("disk %d: %+v", i, k)
		}
	}
	if r := f.Replicas[0]; r.Group != dataGroup || r.Loop == 0 || r.Leader != r.ID || r.Commit == 0 {
		t.Fatalf("the replica %+v", r)
	}

	v := getFacts(t, &c.facts)
	if len(v.Replicas) != 1 || v.Replicas[0].Group != ctlGroup || v.Replicas[0].Leader != 1 || v.Map.ReportsTo != 1 ||
		!slices.Equal(v.Node.Roles, []string{"control-plane"}) {
		t.Fatalf("the voter's answer: %+v", v)
	}
}

// Requests within a second, at once or one after another, build one answer per path: each loop
// is called once. A PG no loop's table holds is refused, and its refusal is not kept.
func TestFactsServerBuildsOncePerSecond(t *testing.T) {
	base := freeBase(t, 2)
	p := runNode(t, nodeConfig{Node: 3, Ctls: []voter{{1, local(base + 1)}}, Group: ctlGroup, Listen: local(base), Client: true})
	calls := countFills(&p.facts)
	start := time.Now()
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if _, err := get(&p.facts, "/facts"); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if _, err := get(&p.facts, "/facts"); err != nil {
		t.Fatal(err)
	}
	if n := calls(); n[0] != 1 && time.Since(start) < time.Second {
		t.Fatalf("nine requests within a second called loop 0 %d times", n[0])
	}
	for range 2 {
		if _, err := get(&p.facts, "/facts/pg/1.0"); err == nil {
			t.Fatal("a PG the node holds no table of was answered")
		}
	}
	if n := calls(); n[0] != 3 {
		t.Fatalf("two refused PG requests and /facts called loop 0 %d times, not once each", n[0])
	}
	time.Sleep(time.Second)
	if _, err := get(&p.facts, "/facts"); err != nil {
		t.Fatal(err)
	}
	if n := calls(); n[0] != 4 {
		t.Fatalf("a request a second later called loop 0 %d times in all, not 4", n[0])
	}
}

// A loop that does not answer in time is named in the answer, beside the parts the other loops
// gave; it gets no new call while it holds one, so later answers come without waiting for it, and
// once it runs its call it answers again.
func TestFactsWithAStuckLoop(t *testing.T) {
	dir := testDir(t)
	a, b := filepath.Join(dir, "stuck-dev-a"), filepath.Join(dir, "stuck-dev-b")
	device(t, a, false)
	device(t, b, false)
	base := freeBase(t, 2)
	p := runNode(t, nodeConfig{Node: 2, Ctls: []voter{{1, local(base + 1)}}, Group: ctlGroup, Listen: local(base), Devices: []string{a, b}, Class: "ssd", Pool: 1})
	stuck := make(chan struct{})
	release := sync.OnceFunc(func() { close(stuck) })
	t.Cleanup(release)
	fill := p.facts.fill
	p.facts.fill = func(i int, ctx *core.Context, f *ops.Facts) {
		if i == 2 {
			<-stuck
		}
		fill(i, ctx, f)
	}
	p.facts.wait = 300 * time.Millisecond
	f := getFacts(t, &p.facts)
	if !slices.Equal(f.Unanswered, []uint32{2}) || len(f.Disks) != 1 || f.Disks[0].Loop != 1 || f.Node.ID != 2 || len(f.Node.Roles) == 0 {
		t.Fatalf("the answer with loop 2 stuck: %+v", f)
	}
	time.Sleep(time.Second)
	at := time.Now()
	f = getFacts(t, &p.facts)
	if took := time.Since(at); took >= p.facts.wait || !slices.Equal(f.Unanswered, []uint32{2}) || len(f.Disks) != 1 {
		t.Fatalf("a second answer took %v: %+v", took, f)
	}
	m := &p.loops[2].mail
	m.mu.Lock()
	queued := len(m.fs)
	m.mu.Unlock()
	if queued != 0 {
		t.Fatalf("the stuck loop was posted %d calls more", queued)
	}
	release()
	time.Sleep(time.Second)
	if f = getFacts(t, &p.facts); f.Unanswered != nil || len(f.Disks) != 2 {
		t.Fatalf("the answer once loop 2 ran its call: %+v", f)
	}
}

// A storage node and a client-only node whose voter does not answer still answer from their own
// state: the device waiting for its first membership. A node that joined, then lost its voter,
// still reports its table, its members and its replica.
func TestFactsWithoutControlPlane(t *testing.T) {
	t.Run("never joined", func(t *testing.T) {
		dir := testDir(t)
		a := filepath.Join(dir, "facts-alone-dev")
		device(t, a, false)
		base := freeBase(t, 4)
		ctls := []voter{{1, local(base + 3)}}
		p := runNode(t, nodeConfig{Node: 2, Ctls: ctls, Group: ctlGroup, Listen: local(base), Devices: []string{a}, Class: "ssd", Pool: 1})
		q := runNode(t, nodeConfig{Node: 3, Ctls: ctls, Group: ctlGroup, Listen: local(base + 2), Client: true})
		f := getFacts(t, &p.facts)
		if f.Node.ID != 2 || len(f.Disks) != 1 || f.Disks[0].State != "waiting" || f.Disks[0].Path != a || f.Map.Epoch != 0 || f.Map.ReportsTo != 1 {
			t.Fatalf("the storage node's answer: %+v", f)
		}
		g := getFacts(t, &q.facts)
		if g.Node.ID != 3 || !slices.Equal(g.Node.Roles, []string{"client"}) || len(g.Disks) != 0 || g.Session.LeaseLeftUs != 0 {
			t.Fatalf("the client's answer: %+v", g)
		}
	})
	t.Run("joined", func(t *testing.T) {
		dir := testDir(t)
		c, stop := runCtlOn(t, dir, 0, true)
		a := filepath.Join(dir, "facts-joined-dev")
		device(t, a, false)
		p := runNode(t, nodeConfig{Node: 2, Ctls: []voter{{1, local(c.Port)}}, Group: ctlGroup, Listen: local(freeBase(t, 1)), Devices: []string{a}, Class: "ssd", Pool: 1})
		var f ops.Facts
		if !within(60*time.Second, func() bool {
			f = getFacts(t, &p.facts)
			return len(f.Disks) == 1 && f.Disks[0].State == "open" && len(f.Replicas) == 1
		}) {
			t.Fatalf("the node never joined: %+v", f)
		}
		if code, said := admin(c, "pool", "1", "-replicas", "1", "-min-live", "1", "-failure-domain", "disk", "-pg-count", "4"); code != 0 {
			t.Fatalf("the pool command exited %d: %s", code, said)
		}
		if !within(60*time.Second, func() bool {
			f = getFacts(t, &p.facts)
			return len(f.Members) == 4 && f.Map.Epoch != 0 && f.Disks[0].Epoch == f.Map.Epoch &&
				!slices.ContainsFunc(f.Members, func(m ops.Member) bool { return m.Role != "primary" || !m.Active })
		}) {
			t.Fatalf("the node never led the pool's PGs: %+v", f)
		}
		stop()
		time.Sleep(2 * time.Second)
		g := getFacts(t, &p.facts)
		if g.Map != f.Map || !slices.Equal(g.Members, f.Members) || len(g.Replicas) != 1 || g.Replicas[0].Group != f.Replicas[0].Group ||
			g.Disks[0].State != "open" || g.Disks[0].Epoch != f.Map.Epoch || g.Unanswered != nil {
			t.Fatalf("with its voter gone the node answers %+v, having answered %+v", g, f)
		}
	})
}

// Every voter answers on its admin port; a follower reports its replica supporting the leader.
func TestFactsOfAFollowingVoter(t *testing.T) {
	dir := testDir(t)
	base := freeBase(t, 3)
	var vs []voter
	for i := range 3 {
		vs = append(vs, voter{uint32(i + 1), local(base + i)})
	}
	var cs []*ctl
	for i, v := range vs {
		p, err := newCtl(ctlConfig{Node: v.Node, Voters: vs, Group: ctlGroup, Listen: v.Addr, Disk: filepath.Join(dir, fmt.Sprintf("facts-voter-%d", i)),
			Format: true, DiskBytes: 512 << 20, MaxReplayBytes: 64 << 20, Admin: "127.0.0.1:0"})
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- p.Run() }()
		t.Cleanup(func() {
			p.Stop()
			<-done
		})
		cs = append(cs, p)
	}
	var fs []ops.Facts
	if !within(60*time.Second, func() bool {
		fs = fs[:0]
		for _, p := range cs {
			b, err := get(&p.facts, "/facts")
			if err != nil {
				return false
			}
			f, err := ops.DecodeFacts(b)
			if err != nil {
				t.Fatal(err)
			}
			fs = append(fs, f)
		}
		leaders := 0
		for _, f := range fs {
			if len(f.Replicas) != 1 || f.Replicas[0].Leader == 0 || f.Replicas[0].Leader != fs[0].Replicas[0].Leader {
				return false
			}
			if f.Replicas[0].Leader == f.Replicas[0].ID {
				leaders++
			}
		}
		return leaders == 1
	}) {
		t.Fatalf("the voters never agreed on one leader: %+v", fs)
	}
	for _, f := range fs {
		if r := f.Replicas[0]; r.Leader != r.ID && (r.Group != ctlGroup || f.Map.ReportsTo != 0 || len(r.Followers) != 0 || r.Term == 0) {
			t.Fatalf("a follower's answer: %+v", f)
		}
	}
}
