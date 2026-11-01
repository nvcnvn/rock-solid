package meta

import (
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
)

const minute = core.Tick(60_000_000)

// cluster records two disks on each of n metadata nodes: node i's disks have IDs 2i+1 and 2i+2 and logs 10i+1 and 10i+2.
func cluster(t *testing.T, n int) (*chunkstore.DiskRecord, []uint32) {
	var r chunkstore.DiskRecord
	var nodes []uint32
	for i := range uint32(n) {
		nodes = append(nodes, i)
		for dev := range uint32(2) {
			if _, err := r.Report(i, dev, uint64(10*i+dev+1)); err != nil {
				t.Fatal(err)
			}
		}
	}
	return &r, nodes
}

// downNode marks every disk of node down since since, as the failure detector does when the node stops.
func downNode(r *chunkstore.DiskRecord, node uint32, since core.Tick) {
	for _, d := range r.Disks() {
		if d.Node == node {
			r.Down(d.ID, since)
		}
	}
}

func voterNodes(g *Group) []uint32 {
	var ns []uint32
	for _, v := range g.Voters {
		ns = append(ns, v.Node)
	}
	return ns
}

func TestEachGroupGetsThreeVotersOnePerNodeOnTheLeastLoadedDisk(t *testing.T) {
	r, nodes := cluster(t, 3)
	m := ShardMap{Groups: []Group{{ID: 0}, {ID: 1}, {ID: 2}, {ID: 3}}}
	m = PlaceVoters(&m, nodes, r.Disks(), 0)
	load := map[uint32]int{}
	for _, g := range m.Groups {
		if !slices.Equal(voterNodes(&g), []uint32{0, 1, 2}) {
			t.Fatalf("group %d on nodes %v", g.ID, voterNodes(&g))
		}
		for k, v := range g.Voters {
			if v.ID != uint64(k)+1 {
				t.Fatalf("group %d voter %d has ID %d", g.ID, k, v.ID)
			}
			load[v.Disk]++
		}
	}
	for d := uint32(1); d <= 6; d++ {
		if load[d] != 2 {
			t.Fatalf("disk loads %v, want two voters on each of six disks", load)
		}
	}
}

func TestAVoterDownForRefillAfterIsReplacedOnAFourthNode(t *testing.T) {
	r, nodes := cluster(t, 4)
	m := ShardMap{Groups: []Group{{ID: 7}}}
	m = PlaceVoters(&m, nodes[:3], r.Disks(), 0)
	before := m.Groups[0]
	downNode(r, 2, minute)
	m = PlaceVoters(&m, nodes, r.Disks(), minute+chunkstore.RefillAfter-1)
	if !slices.Equal(m.Groups[0].Voters, before.Voters) {
		t.Fatalf("replaced a voter down 9 minutes 59 s: %+v", m.Groups[0].Voters)
	}
	m = PlaceVoters(&m, nodes, r.Disks(), minute+chunkstore.RefillAfter)
	g := m.Groups[0]
	want := Replica{ID: 4, Node: 3, Disk: 7}
	if !slices.Equal(g.Voters[:2], before.Voters[:2]) || g.Voters[2] != want || g.LastID != 4 {
		t.Fatalf("voters %+v, last ID %d: want C replaced by %+v", g.Voters, g.LastID, want)
	}
}

func TestOnThreeNodesAVoterWhoseNodeIsDownStays(t *testing.T) {
	r, nodes := cluster(t, 3)
	m := ShardMap{Groups: []Group{{ID: 0}, {ID: 1}}}
	m = PlaceVoters(&m, nodes, r.Disks(), 0)
	before := slices.Clone(m.Groups)
	downNode(r, 2, 0)
	m = PlaceVoters(&m, nodes, r.Disks(), 60*minute)
	for i, g := range m.Groups {
		if !slices.Equal(g.Voters, before[i].Voters) || g.LastID != before[i].LastID {
			t.Fatalf("group %d changed with only its own voters' nodes left: %+v", g.ID, g)
		}
	}
}

// A disk whose log is gone, a new log having been reported on its device, loses its voters at once,
// to another disk of the same node when no other node is free: the new disk, which holds none.
func TestAVoterWhoseLogIsGoneMovesAtOnceToAnotherDiskOfItsNode(t *testing.T) {
	r, nodes := cluster(t, 3)
	m := ShardMap{Groups: []Group{{ID: 0}, {ID: 1}, {ID: 2}}}
	m = PlaceVoters(&m, nodes, r.Disks(), 0)
	var on []int
	for i, g := range m.Groups {
		if g.Voters[2].Disk == 5 {
			on = append(on, i)
		}
	}
	fresh, err := r.Report(2, 0, 99)
	if err != nil {
		t.Fatal(err)
	}
	m = PlaceVoters(&m, nodes, r.Disks(), 1)
	if len(on) == 0 {
		t.Fatal("no group had a voter on disk 5")
	}
	for _, i := range on {
		v := m.Groups[i].Voters[2]
		if v.Node != 2 || v.Disk == 5 || v.ID != 4 {
			t.Fatalf("group %d's voter on the lost disk became %+v", m.Groups[i].ID, v)
		}
	}
	if v := m.Groups[on[0]].Voters[2]; v.Disk != fresh {
		t.Fatalf("the first voter moved to disk %d, not the empty new disk %d", v.Disk, fresh)
	}
}

// A replacement whose own disk fails before the swap is planned elsewhere the same way, under a
// new ID; the group's leader then removes the planned learner and adds the new one.
func TestAPlannedReplacementThatFailsIsReplacedAgain(t *testing.T) {
	r, nodes := cluster(t, 5)
	m := ShardMap{Groups: []Group{{ID: 0}}}
	m = PlaceVoters(&m, nodes[:3], r.Disks(), 0)
	downNode(r, 2, 0)
	m = PlaceVoters(&m, nodes[:4], r.Disks(), chunkstore.RefillAfter)
	if v := m.Groups[0].Voters[2]; v.Node != 3 || v.ID != 4 {
		t.Fatalf("first replacement %+v", v)
	}
	downNode(r, 3, chunkstore.RefillAfter)
	m = PlaceVoters(&m, nodes, r.Disks(), 2*chunkstore.RefillAfter)
	if v := m.Groups[0].Voters[2]; v.Node != 4 || v.ID != 5 {
		t.Fatalf("second replacement %+v", v)
	}
}

func TestADiskOffTheMetadataNodesTakesNoVoter(t *testing.T) {
	r, nodes := cluster(t, 4)
	m := ShardMap{Groups: []Group{{ID: 0}}}
	m = PlaceVoters(&m, nodes[1:], r.Disks(), 0)
	if !slices.Equal(voterNodes(&m.Groups[0]), []uint32{1, 2, 3}) {
		t.Fatalf("voters on nodes %v", voterNodes(&m.Groups[0]))
	}
}
