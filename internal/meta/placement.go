// Package meta is the MetaStore: a shard's state machine and the replica that proposes and applies its Txns, and where the control-plane leader places each Raft group's voters.
package meta

import (
	"slices"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
)

// Voters is how many voters each group has.
const Voters = 3

// Replica is one voter of a group in the shard map: its Raft replica ID and the node and disk that hold it.
type Replica struct {
	ID   uint64
	Node uint32
	Disk uint32
}

// Group is a Raft group's entry in the shard map: its voters, and the last replica ID it handed out, which no later replica reuses.
type Group struct {
	ID     uint64
	Voters []Replica
	LastID uint64
}

// ShardMap is the control-plane leader's plan for every group's voters, which each group's leader drives its configuration toward.
type ShardMap struct {
	Groups []Group
}

// Halt is a replica that halted at a committed entry it could not decode, as its node reports: its group, its replica ID, and since when.
type Halt struct {
	Group, ID uint64
	Since     core.Tick
}

// PlaceVoters returns the shard map after m: a voter whose disk's log is gone, whose disk has
// been down for refill_after, or whose disk now reports from another node, replaced, and one that
// has been halted for refill_after replaced on another node, and each group filled to Voters, one
// voter per node in nodes, each new one under a new replica ID on the up disk with the fewest
// voters of any group. A voter with nowhere to go stays.
func PlaceVoters(m *ShardMap, nodes []uint32, disks []chunkstore.RecordedDisk, halts []Halt, now core.Tick) ShardMap {
	load := make([]int, len(disks))
	next := ShardMap{Groups: make([]Group, len(m.Groups))}
	for i, g := range m.Groups {
		g.Voters = slices.Clone(g.Voters)
		next.Groups[i] = g
		for _, v := range g.Voters {
			if k := find(disks, v.Disk); k >= 0 {
				load[k]++
			}
		}
	}
	for gi := range next.Groups {
		g := &next.Groups[gi]
		for i, v := range g.Voters {
			k := find(disks, v.Disk)
			if k < 0 {
				continue
			}
			h := halted(halts, g.ID, v.ID, now)
			if !h && !replaced(&v, &disks[k], now) {
				continue
			}
			if c := best(g, i, h, nodes, disks, load); c >= 0 {
				load[k]--
				g.Voters[i] = g.add(&disks[c], load, c)
			}
		}
		for len(g.Voters) < Voters {
			c := best(g, -1, false, nodes, disks, load)
			if c < 0 {
				break
			}
			g.Voters = append(g.Voters, g.add(&disks[c], load, c))
		}
	}
	return next
}

// replaced reports whether voter v on disk d is replaced at now: d's log is gone, d has been down
// for refill_after, or d now reports from another node than v was placed on, which would put two
// voters of a group on one node.
func replaced(v *Replica, d *chunkstore.RecordedDisk, now core.Tick) bool {
	return d.Gone || d.Node != v.Node || !d.Up && now-min(d.DownSince, now) >= chunkstore.RefillAfter
}

// halted reports whether voter id of group has been halted for refill_after at now.
func halted(halts []Halt, group, id uint64, now core.Tick) bool {
	for _, h := range halts {
		if h.Group == group && h.ID == id && now-min(h.Since, now) >= chunkstore.RefillAfter {
			return true
		}
	}
	return false
}

// best is the index in disks of the up disk with the fewest voters, the lower ID on a tie, on a
// node in nodes that holds no voter of g but the one at skip, and not that voter's disk, nor,
// offNode, its node, or -1.
func best(g *Group, skip int, offNode bool, nodes []uint32, disks []chunkstore.RecordedDisk, load []int) int {
	c := -1
	for k := range disks {
		d := &disks[k]
		if !d.Up || !slices.Contains(nodes, d.Node) || holds(g, skip, d.Node, disks) || skip >= 0 && (d.ID == g.Voters[skip].Disk || offNode && d.Node == g.Voters[skip].Node) {
			continue
		}
		if c < 0 || load[k] < load[c] || load[k] == load[c] && d.ID < disks[c].ID {
			c = k
		}
	}
	return c
}

// holds reports whether a voter of g other than the one at skip is on a disk that reports from node.
func holds(g *Group, skip int, node uint32, disks []chunkstore.RecordedDisk) bool {
	for i, v := range g.Voters {
		on := v.Node
		if k := find(disks, v.Disk); k >= 0 {
			on = disks[k].Node
		}
		if i != skip && on == node {
			return true
		}
	}
	return false
}

// add hands out g's next replica ID on disk d, the k-th, counting it in load.
func (g *Group) add(d *chunkstore.RecordedDisk, load []int, k int) Replica {
	g.LastID++
	load[k]++
	return Replica{ID: g.LastID, Node: d.Node, Disk: d.ID}
}

func find(disks []chunkstore.RecordedDisk, id uint32) int {
	return slices.IndexFunc(disks, func(d chunkstore.RecordedDisk) bool { return d.ID == id })
}
