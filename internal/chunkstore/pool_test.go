package chunkstore

import (
	"cmp"
	"errors"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

func TestPoolDefaults(t *testing.T) {
	p, err := NewPool(PoolSpec{}, 60)
	if err != nil {
		t.Fatal(err)
	}
	want := Pool{Replicas: 3, MinLive: 2, FailureDomain: DomainNode, MaxPerDomain: 1, Locality: LocalitySpread, Overcommit: true, PGCount: 2048}
	if p != want {
		t.Fatalf("defaults %+v, want %+v", p, want)
	}
	for _, c := range []struct {
		replicas, minLive int
		locality          string
	}{{1, 1, LocalityNode}, {2, 2, LocalitySpread}} {
		p, err := NewPool(PoolSpec{Replicas: new(c.replicas)}, 3)
		if err != nil || p.MinLive != c.minLive || p.Locality != c.locality {
			t.Fatalf("replicas %d: min_live %d, locality %s, %v", c.replicas, p.MinLive, p.Locality, err)
		}
	}
	installed, _ := Preset(DefaultPool)
	if p, _ := NewPool(installed, 60); p != want {
		t.Fatalf("the installed pool %+v is not the default %+v", p, want)
	}
}

func TestPoolSettingsRefused(t *testing.T) {
	on := true
	for _, c := range []struct {
		setting string
		spec    PoolSpec
	}{
		{"replicas", PoolSpec{Replicas: new(0)}},
		{"replicas", PoolSpec{Replicas: new(4)}},
		{"sealed", PoolSpec{Sealed: "ec"}},
		{"sealed", PoolSpec{Sealed: "striped"}},
		{"data", PoolSpec{Data: new(4)}},
		{"parity", PoolSpec{Parity: new(2)}},
		{"encode_on_write", PoolSpec{EncodeOnWrite: &on}},
		{"failure_domain", PoolSpec{FailureDomain: "rack"}},
		{"pg_count", PoolSpec{PGCount: new(uint32(0))}},
		{"pg_count", PoolSpec{PGCount: new(uint32(3))}},
		{"pg_count", PoolSpec{PGCount: new(uint32(1000))}},
		{"min_live", PoolSpec{MinLive: new(0)}},
		{"min_live", PoolSpec{Replicas: new(2), MinLive: new(3)}},
		{"write_limit", PoolSpec{WriteLimit: new(uint8(0))}},
		{"write_limit", PoolSpec{WriteLimit: new(uint8(10))}},
		{"write_limit", PoolSpec{WriteLimit: new(uint8(101))}},
	} {
		_, err := NewPool(c.spec, 3)
		var se *SettingError
		if !errors.As(err, &se) || se.Setting != c.setting {
			t.Fatalf("%+v: %v, want a refusal of %s", c.spec, err, c.setting)
		}
	}
	if _, err := NewPool(PoolSpec{WriteLimit: new(uint8(11))}, 3); err != nil {
		t.Fatal(err)
	}
}

func TestPoolChanges(t *testing.T) {
	p, _ := NewPool(PoolSpec{}, 12)
	for _, change := range []func(*Pool){
		func(p *Pool) { p.Replicas, p.MinLive = 2, 1 },
		func(p *Pool) { p.FailureDomain = DomainDisk },
		func(p *Pool) { p.WriteLimit = 95 },
		func(p *Pool) { p.DiskClass = "nvme" },
		func(p *Pool) { p.Overcommit = false },
		func(p *Pool) { p.PGCount *= 2 },
	} {
		next := p
		change(&next)
		if err := p.Change(next); err != nil {
			t.Fatalf("%+v: %v", next, err)
		}
	}
	for _, pgs := range []uint32{p.PGCount / 2, p.PGCount * 4, 0} {
		next := p
		next.PGCount = pgs
		if err := p.Change(next); err == nil {
			t.Fatalf("pg_count %d from %d was accepted", pgs, p.PGCount)
		}
	}
}

// An unset write_limit gives each disk its segment size's default; a set one applies to every disk as set.
func TestWriteLimitPerDisk(t *testing.T) {
	sizes := []uint64{1 << 40, 256 << 30, 128 << 30, 64 << 30, 32 << 30, 16 << 30, 8 << 30, 1 << 30}
	defaults := []uint8{90, 90, 90, 85, 85, 75, 55, 50}
	disks := make([]Disk, len(sizes))
	for i, s := range sizes {
		disks[i] = Disk{ID: uint32(i), Node: uint32(i), Blocks: s / core.BlockSize, Slots: 1}
	}
	for _, limit := range []uint8{0, 90} {
		p := Pool{Replicas: 1, FailureDomain: DomainNode, PGCount: 1, WriteLimit: limit}
		f := p.Capacity(disks, 0, 0)
		for i, d := range f.Disks {
			pct := cmp.Or(limit, defaults[i])
			if d.Percent != pct || d.Limit != d.Blocks*uint64(pct)/100 || d.level(10) != d.Blocks*uint64(pct-10)/100 {
				t.Fatalf("write_limit %d, %d GiB disk: %d%%, limit %d, NearFull at %d", limit, sizes[i]>>30, d.Percent, d.Limit, d.level(10))
			}
		}
	}
}

func nodes(sizes ...float64) []Disk {
	disks := make([]Disk, len(sizes))
	for i, s := range sizes {
		disks[i] = Disk{ID: uint32(i), Node: uint32(i), Blocks: uint64(s*1e12) / core.BlockSize, Slots: 1}
	}
	return disks
}

func TestPoolCreationRules(t *testing.T) {
	pool := func(spec PoolSpec) Pool {
		p, err := NewPool(spec, 3)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	rep3, rep2, none := pool(PoolSpec{}), pool(PoolSpec{Replicas: new(2)}), pool(PoolSpec{Replicas: new(1)})
	for _, c := range []struct {
		name   string
		pool   Pool
		disks  []Disk
		ok     bool
		layout Layout
		spare  bool
	}{
		{"rep3 on 3 nodes", rep3, nodes(1, 1, 1), true, Layout{Domains: 3, NodeFailures: 2}, false},
		{"rep3 on 4 nodes", rep3, nodes(1, 1, 1, 1), true, Layout{Domains: 4, NodeFailures: 2}, true},
		{"rep3 on 2 nodes", rep3, nodes(1, 1), false, Layout{}, false},
		{"rep3 at 2 per node on 2 nodes", pool(PoolSpec{MaxPerDomain: new(2)}), nodes(1, 1), false, Layout{}, false},
		{"rep2 on 2 nodes", rep2, nodes(1, 1), true, Layout{Domains: 2, NodeFailures: 1}, false},
		{"rep3 by disk on 1 node of 3 disks", pool(PoolSpec{FailureDomain: DomainDisk}),
			[]Disk{{ID: 0, Node: 7, Slots: 1}, {ID: 1, Node: 7, Slots: 1}, {ID: 2, Node: 7, Slots: 1}}, true, Layout{Domains: 3}, false},
		{"rep3 of NVMe on 3 nodes, 2 with NVMe", pool(PoolSpec{DiskClass: "nvme"}),
			[]Disk{{ID: 0, Node: 0, Class: "nvme"}, {ID: 1, Node: 1, Class: "nvme"}, {ID: 2, Node: 2, Class: "hdd"}}, false, Layout{}, false},
		{"none on 1 node", none, nodes(1), true, Layout{Domains: 1}, false},
	} {
		l, err := c.pool.CheckLayout(c.disks)
		if c.ok != (err == nil) || c.ok && l != c.layout || !c.ok && !errors.Is(err, ErrTooFewDomains) {
			t.Fatalf("%s: %+v, %v", c.name, l, err)
		}
		if f := c.pool.Capacity(c.disks, 0, 0); c.ok && f.SpareDomain != c.spare {
			t.Fatalf("%s: spare domain %v", c.name, f.SpareDomain)
		}
	}
}

// The presets survive nothing, 1 and 2 failures, need 1, 2 and 3 failure domains, and hold 1, 1/2 and 1/3 of raw.
func TestPresets(t *testing.T) {
	for i, name := range []string{"none", "rep2", "rep3"} {
		spec, ok := Preset(name)
		p, err := NewPool(spec, 6)
		if !ok || err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if _, err := p.CheckLayout(nodes(1, 1, 1)[:i]); err == nil {
			t.Fatalf("%s fits %d domains", name, i)
		}
		l, err := p.CheckLayout(nodes(1, 1, 1)[:i+1])
		if err != nil || l.NodeFailures != i {
			t.Fatalf("%s survives %d node failures, %v", name, l.NodeFailures, err)
		}
		f := p.Capacity(nodes(1, 1, 1, 1, 1, 1), 0, 0)
		if f.Balanced != f.Capacity/uint64(i+1) {
			t.Fatalf("%s holds %d of %d", name, f.Balanced, f.Capacity)
		}
	}
}
