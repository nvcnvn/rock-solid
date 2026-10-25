package chunkstore

import (
	"cmp"
	"errors"
	"fmt"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// Failure domains and localities a pool can name.
const (
	DomainNode = "node"
	DomainDisk = "disk"

	LocalityNode   = "node"
	LocalitySpread = "spread"
)

// DefaultPool is the pool installation creates.
const DefaultPool = "rep3"

// ErrTooFewDomains refuses a pool whose PG width does not fit its failure domains.
var ErrTooFewDomains = errors.New("chunkstore: too few failure domains for the pool's width")

// SettingError is a pool setting refused, naming the setting.
type SettingError struct {
	Setting string
	Reason  string
}

func (e *SettingError) Error() string {
	return "chunkstore: pool setting " + e.Setting + ": " + e.Reason
}

var writeLimitRefused = &SettingError{"write_limit", "must be above 10% and at most 100%, so NearFull has a range below it"}

// PoolSpec is a pool as written in its Pool resource: a nil or empty field takes its default.
type PoolSpec struct {
	Replicas      *int
	MinLive       *int
	Sealed        string
	Data          *int
	Parity        *int
	EncodeOnWrite *bool
	FailureDomain string
	MaxPerDomain  *int
	Locality      string
	Overcommit    *bool
	WriteLimit    *uint8
	DiskClass     string
	PGCount       *uint32
}

// Pool is a pool's settings with defaults resolved: Data and Parity 0 for copies, WriteLimit in percent or 0 for each disk's default.
type Pool struct {
	Replicas      int
	MinLive       int
	Data          int
	Parity        int
	FailureDomain string
	MaxPerDomain  int
	Locality      string
	Overcommit    bool
	WriteLimit    uint8
	DiskClass     string
	PGCount       uint32
}

// Preset is the spec of a named preset pool: none, rep2 or rep3.
func Preset(name string) (PoolSpec, bool) {
	switch name {
	case "none":
		return PoolSpec{Replicas: new(1)}, true
	case "rep2":
		return PoolSpec{Replicas: new(2)}, true
	case "rep3":
		return PoolSpec{Replicas: new(3)}, true
	}
	return PoolSpec{}, false
}

// NewPool resolves spec's defaults, sizing an unset pg_count for disks, and checks every setting.
func NewPool(spec PoolSpec, disks int) (Pool, error) {
	switch {
	case spec.Sealed == "ec":
		return Pool{}, ecRefused("sealed")
	case spec.Sealed != "" && spec.Sealed != "replicated":
		return Pool{}, &SettingError{"sealed", "must be replicated or ec"}
	case spec.Data != nil:
		return Pool{}, ecRefused("data")
	case spec.Parity != nil:
		return Pool{}, ecRefused("parity")
	case spec.EncodeOnWrite != nil:
		return Pool{}, ecRefused("encode_on_write")
	case spec.WriteLimit != nil && *spec.WriteLimit == 0:
		return Pool{}, writeLimitRefused
	}
	p := Pool{
		Replicas:      value(spec.Replicas, 3),
		FailureDomain: cmp.Or(spec.FailureDomain, DomainNode),
		MaxPerDomain:  value(spec.MaxPerDomain, 1),
		Overcommit:    value(spec.Overcommit, true),
		WriteLimit:    value(spec.WriteLimit, 0),
		DiskClass:     spec.DiskClass,
	}
	p.MinLive = value(spec.MinLive, min(p.Replicas, 2))
	p.Locality = spec.Locality
	if p.Locality == "" {
		p.Locality = LocalitySpread
		if p.Replicas == 1 {
			p.Locality = LocalityNode
		}
	}
	p.PGCount = value(spec.PGCount, PGCountFor(disks, p.width()))
	return p, p.Check()
}

func ecRefused(setting string) error {
	return &SettingError{setting, "erasure coding is not supported before phase 10"}
}

func value[T any](v *T, def T) T {
	if v == nil {
		return def
	}
	return *v
}

// PGCountFor is the smallest power of two that gives each of disks at least 100 members of a pool of width.
func PGCountFor(disks, width int) uint32 {
	p := uint32(1)
	for p < 1<<31 && int(p)*max(width, 1) < 100*disks {
		p *= 2
	}
	return p
}

// Check refuses a setting out of range.
func (p Pool) Check() error {
	switch {
	case p.Replicas < 1 || p.Replicas > 3:
		return &SettingError{"replicas", "must be 1 to 3"}
	case p.MinLive < 1 || p.MinLive > p.Replicas:
		return &SettingError{"min_live", "must be 1 to replicas"}
	case p.FailureDomain != DomainNode && p.FailureDomain != DomainDisk:
		return &SettingError{"failure_domain", "must be node or disk"}
	case p.MaxPerDomain < 1:
		return &SettingError{"max_per_domain", "must be at least 1"}
	case p.Locality != LocalityNode && p.Locality != LocalitySpread:
		return &SettingError{"locality", "must be node or spread"}
	case p.WriteLimit != 0 && (p.WriteLimit <= 10 || p.WriteLimit > 100):
		return writeLimitRefused
	case p.PGCount == 0 || p.PGCount&(p.PGCount-1) != 0:
		return &SettingError{"pg_count", "must be a non-zero power of two"}
	}
	return nil
}

// Change checks next as the settings that replace p: any setting may change but data and parity, and pg_count only doubles.
func (p Pool) Change(next Pool) error {
	if next.PGCount != p.PGCount && next.PGCount != 2*p.PGCount {
		return &SettingError{"pg_count", fmt.Sprintf("can only double, from %d to %d", p.PGCount, 2*p.PGCount)}
	}
	if next.Data != p.Data || next.Parity != p.Parity {
		return &SettingError{"data", "data and parity are fixed at creation"}
	}
	return next.Check()
}

func (p Pool) width() int {
	if p.Data > 0 {
		return p.Data + p.Parity
	}
	return p.Replicas
}

// perDomain is the most members of a PG one failure domain may hold: copies always sit in distinct domains.
func (p Pool) perDomain() int {
	if p.Data > 0 {
		return p.MaxPerDomain
	}
	return 1
}

// dataShards is the overhead's denominator: b = width ÷ dataShards, a = perDomain ÷ dataShards.
func (p Pool) dataShards() int { return max(p.Data, 1) }

// Layout is what the CLI states when a pool is created on its disks.
type Layout struct {
	Domains      int
	NodeFailures int
}

// CheckLayout refuses p unless its copies fit in distinct domains with disks of its class, and states what the layout survives.
func (p Pool) CheckLayout(disks []Disk) (Layout, error) {
	var keys []uint32
	for i := range disks {
		if p.DiskClass == "" || disks[i].Class == p.DiskClass {
			if k := p.domainOf(&disks[i]); !slices.Contains(keys, k) {
				keys = append(keys, k)
			}
		}
	}
	l := Layout{Domains: len(keys)}
	if l.Domains < p.width() {
		return l, fmt.Errorf("%w: width %d, %d domains with disks of the class", ErrTooFewDomains, p.width(), l.Domains)
	}
	// With disks as the domains, nothing keeps a PG's copies on distinct nodes.
	if p.FailureDomain == DomainNode {
		l.NodeFailures = p.Replicas - 1
	}
	return l, nil
}

func (p Pool) domainOf(d *Disk) uint32 {
	if p.FailureDomain == DomainDisk {
		return d.ID
	}
	return d.Node
}

// DefaultWriteLimit is the write_limit, in percent, a pool that leaves it unset gives a disk of diskBytes, by its segment size.
func DefaultWriteLimit(diskBytes uint64) uint8 {
	g, err := disklog.GeometryFor(diskBytes)
	if err != nil {
		return 45
	}
	switch seg := g.SegmentBytes(); {
	case seg >= 32<<20:
		return 90
	case seg >= 8<<20:
		return 85
	case seg >= 4<<20:
		return 75
	case seg >= 512<<10:
		return 55
	case seg >= 128<<10:
		return 50
	}
	return 45
}

func (p Pool) limitPercent(blocks uint64) uint8 {
	if p.WriteLimit != 0 {
		return p.WriteLimit
	}
	return DefaultWriteLimit(blocks * core.BlockSize)
}
