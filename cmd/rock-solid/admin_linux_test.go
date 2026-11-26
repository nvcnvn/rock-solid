package main

import (
	"bytes"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/meta"
)

// admin runs the operator's command line with args against the voter p, and returns its exit
// code and what it printed.
func admin(p *ctl, args ...string) (int, string) {
	var out bytes.Buffer
	args = append(args, "-ctl", fmt.Sprintf("1=127.0.0.1:%d", p.Port), "-group", fmt.Sprint(ctlGroup), "-timeout", "20s")
	code := runAdmin(args, &out, &out)
	return code, out.String()
}

// diskOf is the ID of the disk the record holds for the device at path, 0 for none.
func diskOf(c *meta.Control, path string) uint32 {
	for _, d := range c.Disks() {
		if d.Device == deviceID(path) && !d.Gone {
			return d.ID
		}
	}
	return 0
}

// members are the disks every PG of pool 1 names, leaving ones marked.
func members(c *meta.Control) []chunkstore.Member {
	var ms []chunkstore.Member
	for _, pt := range c.Table().Pools {
		for _, e := range pt.PGs {
			ms = append(ms, e.Members...)
		}
	}
	return ms
}

// The operator's command line over TCP against a single-voter control plane with two storage
// node processes: the first data group gets a voter on each node, both reporting they hold
// metadata, and the control-plane group's entry keeps its own voter; a pool created from it places its PGs on the disks the nodes reported; a PG pinned from it is pinned in the table and unpinned again; a disk holding
// members, drained, is refused removal until balance has moved them off, then removed; a
// declaration for an up disk prints the leader's refusal; the device with no valid superblock is
// printed failed; and once node 3 is killed and its disks' PGs refill, keeping them as leaving
// members, its disks declared lost end every one of those members.
func TestTheOperatorsCommandLineOverTCP(t *testing.T) {
	dir := testDir(t)
	p := runCtl(t, dir)
	a, b, bad := filepath.Join(dir, "adm-dev-a"), filepath.Join(dir, "adm-dev-b"), filepath.Join(dir, "adm-dev-bad")
	c3, d3, e := filepath.Join(dir, "adm-dev-c"), filepath.Join(dir, "adm-dev-d"), filepath.Join(dir, "adm-dev-e")
	for _, path := range []string{a, b, c3, d3, e} {
		device(t, path, false)
	}
	device(t, bad, true)
	base := freeBase(t, 8)
	ctlFlag := fmt.Sprintf("-ctl 1=127.0.0.1:%d -group %d", p.Port, ctlGroup)
	var out2, out3 output
	startNode(t, fmt.Sprintf("-node 2 %s -listen 127.0.0.1:%d -peers 3=127.0.0.1:%d -device %s -device %s -device %s -device %s", ctlFlag, base, base+5, a, b, bad, e), &out2)
	node3 := startNode(t, fmt.Sprintf("-node 3 %s -listen 127.0.0.1:%d -peers 2=127.0.0.1:%d -device %s -device %s", ctlFlag, base+5, base, c3, d3), &out3)
	if !within(60*time.Second, func() bool {
		return onCtl(t, p, func(_ *core.Context, c *meta.Control) bool {
			return c != nil && len(upDisks(c, 2)) == 3 && len(upDisks(c, 3)) == 2
		})
	}) {
		t.Fatalf("the nodes' disks never came up; node 2 said:\n%s\nnode 3 said:\n%s", out2.String(), out3.String())
	}

	voters := func() [][]uint32 {
		return onCtl(t, p, func(_ *core.Context, c *meta.Control) [][]uint32 {
			var ns [][]uint32
			for _, g := range c.ShardMap().Groups {
				var on []uint32
				for _, v := range g.Voters {
					on = append(on, v.Node)
				}
				ns = append(ns, slices.Sorted(slices.Values(on)))
			}
			return ns
		})
	}
	if !within(10*time.Second, func() bool {
		vs := voters()
		return len(vs) == 2 && slices.Equal(vs[0], []uint32{1}) && slices.Equal(vs[1], []uint32{2, 3})
	}) {
		t.Fatalf("the groups' voters are on nodes %v, not the control plane's on its voter and the data group's on 2 and 3", voters())
	}

	if code, said := admin(p, "pool", "1", "-replicas", "2", "-min-live", "2", "-failure-domain", "disk", "-pg-count", "16"); code != 0 || !strings.Contains(said, "pool 1 set") {
		t.Fatalf("the pool command exited %d: %s", code, said)
	}
	reported := onCtl(t, p, func(_ *core.Context, c *meta.Control) []uint32 { return append(upDisks(c, 2), upDisks(c, 3)...) })
	placed := onCtl(t, p, func(_ *core.Context, c *meta.Control) []chunkstore.PGEntry {
		if len(c.Table().Pools) != 1 {
			return nil
		}
		return slices.Clone(c.Table().Pools[0].PGs)
	})
	if len(placed) != 16 {
		t.Fatalf("pool 1 committed with %d PGs", len(placed))
	}
	for i, e := range placed {
		if len(e.Members) != 2 || !slices.ContainsFunc(reported, func(id uint32) bool { return id == e.Members[0].Disk }) ||
			!slices.ContainsFunc(reported, func(id uint32) bool { return id == e.Members[1].Disk }) {
			t.Fatalf("PG %d placed on %+v, the disks reported being %v", i, e.Members, reported)
		}
	}

	for _, pin := range []string{"pin", "unpin"} {
		if code, said := admin(p, "pg", pin, "1", "0"); code != 0 || !strings.Contains(said, "PG 1.0 "+pin+"ned") {
			t.Fatalf("pg %s exited %d: %s", pin, code, said)
		}
		if got := onCtl(t, p, func(_ *core.Context, c *meta.Control) bool { return c.Table().Pools[0].PGs[0].Pinned }); got != (pin == "pin") {
			t.Fatalf("PG 1.0 pinned %v after pg %s", got, pin)
		}
	}

	drained := onCtl(t, p, func(_ *core.Context, c *meta.Control) uint32 { return diskOf(c, e) })
	if !slices.ContainsFunc(placed, func(pg chunkstore.PGEntry) bool {
		return slices.ContainsFunc(pg.Members, func(m chunkstore.Member) bool { return m.Disk == drained })
	}) {
		t.Fatalf("disk %d holds no member to drain", drained)
	}
	if code, said := admin(p, "disk", "drain", fmt.Sprint(drained)); code != 0 || !strings.Contains(said, fmt.Sprintf("disk %d draining", drained)) {
		t.Fatalf("disk drain %d exited %d: %s", drained, code, said)
	}
	if code, said := admin(p, "disk", "remove", fmt.Sprint(drained)); code != 1 || !strings.Contains(said, "PGs still name it") {
		t.Fatalf("a removal of disk %d still holding members exited %d: %s", drained, code, said)
	}
	start := time.Now()
	if !within(90*time.Second, func() bool {
		code, said := admin(p, "disk", "remove", fmt.Sprint(drained))
		return code == 0 && strings.Contains(said, fmt.Sprintf("disk %d removed", drained))
	}) {
		t.Fatalf("disk %d never emptied; members %+v", drained, onCtl(t, p, func(_ *core.Context, c *meta.Control) []chunkstore.Member { return members(c) }))
	}
	t.Logf("disk %d drained and removed in %v", drained, time.Since(start))

	upA := onCtl(t, p, func(_ *core.Context, c *meta.Control) uint32 { return diskOf(c, a) })
	if code, said := admin(p, "disk", "lost", fmt.Sprint(upA)); code != 1 || !strings.Contains(said, "refused") || !strings.Contains(said, chunkstore.ErrLostRefused.Error()) {
		t.Fatalf("a declaration for up disk %d exited %d: %s", upA, code, said)
	}
	if code, said := admin(p, "disk", "failed"); code != 0 || !strings.Contains(said, fmt.Sprintf("node 2 device %#x failed", deviceID(bad))) {
		t.Fatalf("the failed devices command exited %d: %s", code, said)
	}

	dead := onCtl(t, p, func(_ *core.Context, c *meta.Control) []uint32 { return []uint32{diskOf(c, c3), diskOf(c, d3)} })
	onDead := func(m chunkstore.Member) bool { return slices.Contains(dead, m.Disk) }
	if err := node3.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if !within(150*time.Second, func() bool {
		return onCtl(t, p, func(_ *core.Context, c *meta.Control) bool {
			return slices.ContainsFunc(members(c), func(m chunkstore.Member) bool { return onDead(m) && m.Leaving })
		})
	}) {
		t.Fatalf("no PG kept a dead disk of %v as a leaving member; members %+v", dead, onCtl(t, p, func(_ *core.Context, c *meta.Control) []chunkstore.Member { return members(c) }))
	}
	for _, id := range dead {
		if code, said := admin(p, "disk", "lost", fmt.Sprint(id)); code != 0 || !strings.Contains(said, fmt.Sprintf("disk %d declared lost", id)) {
			t.Fatalf("the declaration of disk %d exited %d: %s", id, code, said)
		}
	}
	left := func() []chunkstore.Member {
		return onCtl(t, p, func(_ *core.Context, c *meta.Control) []chunkstore.Member {
			return slices.DeleteFunc(members(c), func(m chunkstore.Member) bool { return !onDead(m) })
		})
	}
	if !within(10*time.Second, func() bool { return len(left()) == 0 }) {
		t.Fatalf("PGs still name the lost disks %v: %+v", dead, left())
	}
}
