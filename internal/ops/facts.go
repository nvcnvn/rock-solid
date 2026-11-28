// Package ops is the operator's view of a cluster: the facts each node reports, their schema and decoders.
package ops

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// FactsV1 is the schema every v1 answer of GET /facts names; PGFactsV1 that of /facts/pg/<pool>.<pg>.
const (
	FactsV1   = "rock-solid.io/facts/v1"
	PGFactsV1 = "rock-solid.io/facts-pg/v1"
)

// Facts is what a node reports of itself; within v1 fields are only added, and a string's name tag drives redaction.
type Facts struct {
	Schema   string    `json:"schema" name:"no"`
	Node     Node      `json:"node"`
	Map      Map       `json:"cluster_map"`
	Disks    []Disk    `json:"disks"`
	Members  []Member  `json:"members"`
	Replicas []Replica `json:"replicas"`
	Session  Session   `json:"session,omitzero"`
	Mounts   []Mount   `json:"mounts"`
	// Unanswered are the loops that did not answer within the node's wait: their disks, members
	// and replicas are missing from the answer, not absent from the node.
	Unanswered []uint32 `json:"unanswered_loops"`
}

// Node is the process: its identity, clocks at the answer, flags and the hosts it knows.
type Node struct {
	ID       uint32    `json:"id"`
	Release  uint32    `json:"release"`
	Roles    []string  `json:"roles" name:"no"`
	Run      uint64    `json:"run,string" name:"no"`
	UptimeUs core.Tick `json:"uptime_us"`
	BootUs   core.Tick `json:"boottime_us"`
	RawUs    core.Tick `json:"monotonic_raw_us"`
	Wall     string    `json:"wall" name:"no"`
	Flags    []Flag    `json:"flags"`
	Hosts    []Host    `json:"hosts"`
}

// Flag is a command-line flag the process runs with.
type Flag struct {
	Name  string `json:"name" name:"no"`
	Value string `json:"value" name:"yes"`
}

// Host is a daemon the node knows the address of, by node ID.
type Host struct {
	Node uint32 `json:"node"`
	Addr string `json:"addr" name:"yes"`
	Ctl  bool   `json:"ctl"`
}

// Map is the versions of the cluster map the node holds and the voter it reports to, 0 for none.
type Map struct {
	Epoch     uint64 `json:"table_epoch"`
	Sessions  uint64 `json:"session_table_version"`
	ShardMap  uint64 `json:"shard_map_version"`
	Format    uint32 `json:"format_level,omitempty"`
	ReportsTo uint32 `json:"reports_to"`
}

// Disk is one device as the loop that owns it holds it; MemberAckUs is from its replica's last heartbeat.
type Disk struct {
	Loop        uint32    `json:"loop"`
	Path        string    `json:"path" name:"yes"`
	Device      uint64    `json:"device,string" name:"yes"`
	State       string    `json:"state" name:"no"`
	Disk        uint32    `json:"disk"`
	Log         uint64    `json:"log,string" name:"no"`
	Blocks      uint64    `json:"blocks"`
	Reserve     uint64    `json:"reserve_blocks"`
	Fill        uint64    `json:"fill_blocks"`
	OldestIOUs  core.Tick `json:"oldest_io_age_us"`
	Run         uint64    `json:"run,string" name:"no"`
	Epoch       uint64    `json:"table_epoch"`
	Sessions    uint64    `json:"session_table_version"`
	MemberAckUs []Latency `json:"member_ack_p99"`
}

// Latency is a member disk's 99th-percentile acknowledgment time.
type Latency struct {
	Disk uint32    `json:"disk"`
	Us   core.Tick `json:"us"`
}

// Member is a PG member on one of the node's disks: its role (primary, member or leaving) and, as primary, its state now.
type Member struct {
	Disk     uint32 `json:"disk"`
	Pool     uint32 `json:"pool"`
	PG       uint32 `json:"pg"`
	Interval uint32 `json:"interval"`
	Role     string `json:"role" name:"no"`
	Active   bool   `json:"active"`
	Unfound  uint32 `json:"unfound"`
	Lacking  uint32 `json:"lacking"`
}

// Replica is a metadata or control-plane replica's Raft state; a leader adds its followers' match indexes.
type Replica struct {
	Group     uint64     `json:"group"`
	ID        uint64     `json:"id"`
	Loop      uint32     `json:"loop"`
	Term      uint64     `json:"term"`
	Leader    uint64     `json:"leader"`
	Commit    uint64     `json:"commit"`
	Applied   uint64     `json:"applied"`
	Lease     bool       `json:"leader_lease"`
	HaltIndex uint64     `json:"halt_index,omitempty"`
	HaltAgeUs core.Tick  `json:"halt_age_us,omitempty"`
	Followers []Follower `json:"followers,omitempty"`
}

// Follower is a follower's match index, as its leader tracks it.
type Follower struct {
	ID    uint64 `json:"id"`
	Match uint64 `json:"match"`
}

// Session is the client session the node holds, its renewal age on CLOCK_MONOTONIC_RAW and 0 before one.
type Session struct {
	ID           uint64    `json:"id"`
	Incarnation  uint64    `json:"incarnation"`
	LeaseLeftUs  core.Tick `json:"lease_left_us"`
	RenewalAgeUs core.Tick `json:"renewal_age_us"`
}

// Mount is a volume the node mounts, and where.
type Mount struct {
	Volume uint32 `json:"volume"`
	Path   string `json:"path" name:"yes"`
}

// PGFacts is the node's answer on /facts/pg/<pool>.<pg>: its members of the PG and, as primary, up to chunkstore.MaxReportUnfound unfound chunks.
type PGFacts struct {
	Schema  string   `json:"schema" name:"no"`
	Node    uint32   `json:"node"`
	Pool    uint32   `json:"pool"`
	PG      uint32   `json:"pg"`
	Members []Member `json:"members"`
	Unfound []Chunk  `json:"unfound"`
	// Unanswered are the loops that did not answer within the node's wait.
	Unanswered []uint32 `json:"unanswered_loops"`
}

// Chunk names a chunk by volume, inode and index.
type Chunk struct {
	Volume uint32 `json:"volume"`
	Inode  uint64 `json:"inode,string" name:"no"`
	Index  uint32 `json:"index"`
}

// Add appends p's parts that one loop answers: its disks, members and replicas.
func (f *Facts) Add(p *Facts) {
	f.Disks = append(f.Disks, p.Disks...)
	f.Members = append(f.Members, p.Members...)
	f.Replicas = append(f.Replicas, p.Replicas...)
}

// Reset empties f for reuse, keeping its slices' arrays.
func (f *Facts) Reset() {
	*f = Facts{Node: Node{Roles: f.Node.Roles[:0], Flags: f.Node.Flags[:0], Hosts: f.Node.Hosts[:0]},
		Disks: f.Disks[:0], Members: f.Members[:0], Replicas: f.Replicas[:0], Mounts: f.Mounts[:0]}
}

// ErrSchema reports an answer that names a schema other than the one asked for.
var ErrSchema = errors.New("ops: not a v1 answer")

// DecodeFacts decodes a v1 answer, ignoring fields a later v1 release added.
func DecodeFacts(b []byte) (Facts, error) {
	var f Facts
	if err := decode(b, &f, &f.Schema, FactsV1); err != nil {
		return Facts{}, err
	}
	return f, nil
}

// DecodePGFacts decodes a facts-pg/v1 answer, ignoring fields a later v1 release added.
func DecodePGFacts(b []byte) (PGFacts, error) {
	var p PGFacts
	if err := decode(b, &p, &p.Schema, PGFactsV1); err != nil {
		return PGFacts{}, err
	}
	return p, nil
}

func decode(b []byte, v any, schema *string, want string) error {
	if err := json.Unmarshal(b, v); err != nil {
		return fmt.Errorf("ops: %s: %w", want, err)
	}
	if *schema != want {
		return fmt.Errorf("%w: schema %q, not %q", ErrSchema, *schema, want)
	}
	return nil
}

// Cache holds the last answer built and when, so a node builds at most one a second.
type Cache struct {
	at   core.Tick
	body []byte
}

// Fresh is the answer built within a second before now, or nil.
func (c *Cache) Fresh(now core.Tick) []byte {
	if c.body == nil || now-c.at >= 1_000_000 {
		return nil
	}
	return c.body
}

// Keep takes body, built at now.
func (c *Cache) Keep(now core.Tick, body []byte) { c.at, c.body = now, body }
