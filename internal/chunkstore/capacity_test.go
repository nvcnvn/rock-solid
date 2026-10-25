package chunkstore

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/nvcnvn/rock-solid/internal/core"
)

// capacityCase is a data-driven pool: its settings, its disks by name with nodes by name, and its conditions across steps.
type capacityCase struct {
	pool   Pool
	disks  []Disk
	names  []string
	nodes  []string
	grants uint64
	cond   Conditions
}

// TestPoolCapacityCases runs testdata/capacity: the capacity model spike's C1 to C6 with their converged tables.
func TestPoolCapacityCases(t *testing.T) {
	datadriven.Walk(t, "testdata/capacity", func(t *testing.T, path string) {
		c := &capacityCase{}
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			return c.run(t, d)
		})
	})
}

func (c *capacityCase) run(t *testing.T, d *datadriven.TestData) string {
	switch d.Cmd {
	case "pool":
		c.pool = Pool{Replicas: 3, MinLive: 2, FailureDomain: DomainNode, MaxPerDomain: 1, Locality: LocalitySpread, Overcommit: true, WriteLimit: 90, PGCount: 128}
		for _, a := range d.CmdArgs {
			v := a.Vals[0]
			switch a.Key {
			case "replicas":
				c.pool.Replicas = atoi(t, v)
			case "data":
				c.pool.Data = atoi(t, v)
			case "parity":
				c.pool.Parity = atoi(t, v)
			case "per":
				c.pool.MaxPerDomain = atoi(t, v)
			case "pgs":
				c.pool.PGCount = uint32(atoi(t, v))
			case "domain":
				c.pool.FailureDomain = v
			case "overcommit":
				c.pool.Overcommit = v == "on"
			default:
				t.Fatalf("unknown pool argument %s", a.Key)
			}
		}
		c.disks, c.names, c.nodes, c.cond = nil, nil, nil, Conditions{}
		return ""
	case "disks":
		c.disks, c.names, c.nodes = nil, nil, nil
		for line := range strings.Lines(d.Input) {
			f := strings.Fields(line)
			disk := Disk{ID: uint32(len(c.disks))}
			c.names = append(c.names, f[0])
			for _, kv := range f[1:] {
				k, v, _ := strings.Cut(kv, "=")
				switch k {
				case "node":
					disk.Node = c.node(v)
				case "size":
					disk.Blocks = blocksOf(t, v)
				case "other":
					disk.Live += blocksOf(t, v)
				case "slots":
					disk.Slots = uint32(atoi(t, v))
				default:
					t.Fatalf("unknown disk field %s", k)
				}
			}
			c.disks = append(c.disks, disk)
		}
		return ""
	case "hold":
		c.hold(blocksOf(t, d.CmdArgs[0].Key))
		return ""
	case "grants":
		c.grants = blocksOf(t, d.CmdArgs[0].Key)
		return ""
	case "figures":
		f := c.pool.Capacity(c.disks, c.grants, 0)
		if bound := mulDiv(f.Capacity, uint64(c.pool.dataShards()), uint64(c.pool.width())); f.Available+f.Held > f.Balanced || f.Balanced > bound {
			t.Errorf("available %d + held %d, balanced %d, capacity ÷ b %d", f.Available, f.Held, f.Balanced, bound)
		}
		return c.print(&f)
	case "conditions":
		f := c.pool.Capacity(c.disks, c.grants, 0)
		c.cond.Update(&f, d.HasArg("settled"))
		return c.conditions()
	case "confirm":
		f := c.pool.Capacity(c.disks, c.grants, 0)
		c.cond.Confirm(&f)
		return fmt.Sprintf("confirmed %.1f%%\n", 100*c.cond.Confirmed)
	}
	t.Fatalf("unknown command %s", d.Cmd)
	return ""
}

func (c *capacityCase) node(name string) uint32 {
	if i := slices.Index(c.nodes, name); i >= 0 {
		return uint32(i)
	}
	c.nodes = append(c.nodes, name)
	return uint32(len(c.nodes) - 1)
}

func (c *capacityCase) domainName(id uint32) string {
	if c.pool.FailureDomain == DomainDisk {
		return c.diskName(id)
	}
	return c.nodes[id]
}

func (c *capacityCase) diskName(id uint32) string {
	return c.names[slices.IndexFunc(c.disks, func(d Disk) bool { return d.ID == id })]
}

// hold puts held blocks of the pool's data on its disks at balanced proportions: each domain its load at
// balance scaled to held, split over the domain's disks by capacity.
func (c *capacityCase) hold(held uint64) {
	f := c.pool.Capacity(c.disks, 0, 0)
	for i := range c.disks {
		d := &c.disks[i]
		di := slices.IndexFunc(f.Disks, func(x DiskFigures) bool { return x.ID == d.ID })
		dom := f.Domains[f.domainIndex(c.pool.domainOf(d))]
		if di < 0 || dom.Capacity == 0 {
			continue
		}
		own := mulDiv(mulDiv(dom.Load, held, f.Balanced), f.Disks[di].Capacity, dom.Capacity)
		d.Own += own
		d.Live += own
	}
}

func (c *capacityCase) print(f *Figures) string {
	var b strings.Builder
	fmt.Fprintf(&b, "capacity %s:", size(f.Capacity))
	for i, d := range f.Domains {
		fmt.Fprintf(&b, "%s %s %s", sep(i), c.domainName(d.ID), size(d.Capacity))
	}
	w, k := c.pool.width(), c.pool.dataShards()
	fmt.Fprintf(&b, "\ncapacity/b %s\nbalanced %s\n", size(mulDiv(f.Capacity, uint64(k), uint64(w))), size(f.Balanced))
	if f.SetsAvailable >= 0 {
		d := &f.Disks[f.SetsAvailable]
		fmt.Fprintf(&b, "available %s, set by %s (%d of %d slots, target %.1f); available + held %.1f%% under balanced\n",
			size(f.Available), c.diskName(d.ID), d.Slots, c.pool.PGCount, d.Target, 100*(1-float64(f.Ungranted+f.Held)/float64(f.Balanced)))
	}
	fmt.Fprintf(&b, "held %s\nf %.1f%%, stranded room:", size(f.Held), 100*f.Stranded)
	n := 0
	for _, d := range f.Domains {
		if d.Stranded > 0 {
			fmt.Fprintf(&b, "%s %s %s (%.0f%%)", sep(n), c.domainName(d.ID), size(d.Stranded), 100*float64(d.Stranded)/float64(d.Capacity))
			n++
		}
	}
	if n == 0 {
		b.WriteString(" none")
	}
	fmt.Fprintf(&b, "\nasks %v, spare domain %v\n", f.Asks(), f.SpareDomain)
	switch f.Loss.Kind {
	case LossNone:
		b.WriteString("reprotectable none\n")
	case LossDomain:
		fmt.Fprintf(&b, "reprotectable %s, losing domain %s\n", size(f.Reprotectable), c.domainName(f.Loss.ID))
	case LossDisk:
		fmt.Fprintf(&b, "reprotectable %s, losing disk %s\n", size(f.Reprotectable), c.diskName(f.Loss.ID))
	}
	return b.String()
}

func (c *capacityCase) conditions() string {
	var raised []string
	for _, r := range []struct {
		on   bool
		name string
	}{{c.cond.Full, "Full"}, {c.cond.NearFull, "NearFull"}, {c.cond.Stranded, "Stranded"}, {c.cond.Unbalanced, "Unbalanced"}, {c.cond.CannotReprotect, "CannotReprotect"}} {
		if r.on {
			raised = append(raised, r.name)
		}
	}
	if len(raised) == 0 {
		return "none\n"
	}
	return strings.Join(raised, " ") + "\n"
}

func sep(i int) string {
	if i == 0 {
		return ""
	}
	return ","
}

// size prints blocks in decimal TB, or GB below 0.1 TB, as the spike prints them.
func size(blocks uint64) string {
	b := float64(blocks) * core.BlockSize
	if b >= 1e11 {
		return fmt.Sprintf("%.3f TB", b/1e12)
	}
	return fmt.Sprintf("%.3f GB", b/1e9)
}

// blocksOf reads a decimal size such as 0.7GB or 1.2TB as whole blocks, rounded down.
func blocksOf(t *testing.T, s string) uint64 {
	for _, u := range []struct {
		suffix string
		bytes  float64
	}{{"GB", 1e9}, {"TB", 1e12}, {"MiB", 1 << 20}, {"GiB", 1 << 30}, {"TiB", 1 << 40}} {
		if v, ok := strings.CutSuffix(s, u.suffix); ok {
			x, err := strconv.ParseFloat(v, 64)
			if err != nil {
				t.Fatal(err)
			}
			return uint64(x*u.bytes+0.5) / core.BlockSize
		}
	}
	t.Fatalf("size %q has no unit", s)
	return 0
}

func atoi(t *testing.T, s string) int {
	v, err := strconv.Atoi(s)
	if err != nil {
		t.Fatal(err)
	}
	return v
}
