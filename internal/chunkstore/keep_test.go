package chunkstore

import (
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

func v41(n uint32) uint64 { return MakeVersion(41, n) }

// keepEntry is the PG of the keep rule's table: last activated 41, peering in 43, disks 1 to 3
// (A, B, C) up in 41, 42 and 43 unless down names a disk down in an interval.
func keepEntry(down map[uint32][]uint32) *PGEntry {
	ms := func(i uint32) []Member {
		var out []Member
		for d := uint32(1); d <= 4; d++ {
			if d == 4 && i < 43 {
				continue
			}
			m := Member{Disk: d}
			if slices.Contains(down[i], d) {
				m.Mark = MarkDown
			}
			out = append(out, m)
		}
		return out
	}
	return &PGEntry{Members: ms(43), Interval: 43, Activated: 41, Past: []PastInterval{{Interval: 41, Members: ms(41)}, {Interval: 42, Members: ms(42)}}}
}

// The keep rule's table in test-scenarios.md, one chunk: the version kept, whether it is unfound,
// and each up member's missing list.
func TestTheKeepRule(t *testing.T) {
	ch := core.ChunkID{Volume: 1, Inode: 2}
	holds := func(v uint64) []ChunkPeer { return []ChunkPeer{{Chunk: ch, Whole: v, Logged: v}} }
	lists := func(whole, kept uint64) []ChunkPeer {
		return []ChunkPeer{{Chunk: ch, Whole: whole, Logged: whole, Listed: true, Kept: kept}}
	}
	for _, tc := range []struct {
		name    string
		down    map[uint32][]uint32
		replies []peerReply
		kept    uint64
		unfound bool
		listed  []uint32
	}{
		{"C lacks 41.8", nil, []peerReply{{disk: 2, decided: 41, chunks: holds(v41(8))}, {disk: 3, decided: 41, chunks: holds(v41(7))}}, v41(7), false, []uint32{2}},
		{"C down for all of 41", map[uint32][]uint32{41: {3}}, []peerReply{{disk: 2, decided: 41, chunks: holds(v41(8))}, {disk: 3}}, v41(8), false, []uint32{3}},
		{"B and C hold 41.8", nil, []peerReply{{disk: 2, decided: 41, chunks: holds(v41(8))}, {disk: 3, decided: 41, chunks: holds(v41(8))}}, v41(8), false, nil},
		{"C lists 41.8 from 42", nil, []peerReply{{disk: 2, decided: 41, chunks: holds(v41(8))}, {disk: 3, decided: 42, chunks: lists(v41(7), v41(8))}}, v41(8), false, []uint32{3}},
		{"a suffix", nil, []peerReply{{disk: 2, decided: 41, chunks: holds(v41(9))}, {disk: 3, decided: 41, chunks: holds(v41(8))}}, v41(8), false, []uint32{2}},
		{"A lacks 41.8", nil, []peerReply{{disk: 1, decided: 41, chunks: holds(v41(7))}, {disk: 2, decided: 41, chunks: holds(v41(8))}, {disk: 3, decided: 41, chunks: holds(v41(8))}}, v41(7), false, []uint32{2, 3}},
		{"unfound", nil, []peerReply{{disk: 2, decided: 41, chunks: lists(MakeVersion(39, 3), MakeVersion(39, 4))}, {disk: 3, decided: 41, chunks: lists(0, MakeVersion(39, 4))}}, MakeVersion(39, 4), true, []uint32{2, 3}},
		{"42's decision, report lost", map[uint32][]uint32{41: {3}, 42: {1}}, []peerReply{{disk: 2, decided: 42, chunks: lists(v41(8), v41(7))}, {disk: 3, decided: 42, chunks: holds(v41(7))}}, v41(7), false, []uint32{2}},
		{"42 kept 41.8", map[uint32][]uint32{41: {3}, 42: {1}}, []peerReply{{disk: 1, decided: 41, chunks: holds(v41(7))}, {disk: 3, decided: 42, chunks: lists(0, v41(8))}}, v41(8), true, []uint32{1, 3}},
		{"D new in 43", nil, []peerReply{{disk: 2, decided: 41, chunks: holds(v41(8))}, {disk: 3, decided: 41, chunks: holds(v41(7))}, {disk: 4}}, v41(7), false, []uint32{2, 4}},
		{"C holds 41.8 in part", nil, []peerReply{{disk: 2, decided: 41, chunks: holds(v41(8))}, {disk: 3, decided: 41, chunks: []ChunkPeer{{Chunk: ch, Whole: v41(7), Logged: v41(8)}}}}, v41(7), true, []uint32{2, 3}},
	} {
		_, got := keep(keepEntry(tc.down), tc.replies, nil, nil, replicaPlants{})
		if len(got) != 1 || got[0].kept != tc.kept || got[0].unfound != tc.unfound {
			t.Errorf("%s: decided %+v, want %#x unfound %v", tc.name, got, tc.kept, tc.unfound)
			continue
		}
		var listed []uint32
		for _, r := range tc.replies {
			if len(r.list) > 0 {
				if r.list[0] != (ChunkVersion{Chunk: ch, Version: tc.kept}) {
					t.Errorf("%s: disk %d lists %+v", tc.name, r.disk, r.list)
				}
				listed = append(listed, r.disk)
			}
		}
		if !slices.Equal(listed, tc.listed) {
			t.Errorf("%s: listed on %v, want %v", tc.name, listed, tc.listed)
		}
	}
}

// A chunk kept at 0 lists only the member that logged a dropped version of it, a chunk a member
// never logged lists the member, and several chunks come out in chunk order.
func TestTheKeepRuleOverSeveralChunks(t *testing.T) {
	a, b := core.ChunkID{Volume: 1, Inode: 1}, core.ChunkID{Volume: 1, Inode: 2}
	replies := []peerReply{
		{disk: 1, decided: 41, chunks: []ChunkPeer{{Chunk: b, Whole: v41(3), Logged: v41(3)}, {Chunk: a, Whole: MakeVersion(43, 1), Logged: MakeVersion(43, 1)}}},
		{disk: 2, decided: 41, chunks: []ChunkPeer{{Chunk: b, Whole: v41(3), Logged: v41(3)}}},
		{disk: 3, decided: 41},
	}
	e := keepEntry(map[uint32][]uint32{41: {3}})
	e.Interval, e.Members = 44, e.Members[:3]
	e.Past = append(e.Past, PastInterval{Interval: 43, Members: e.Members})
	_, got := keep(e, replies, nil, nil, replicaPlants{})
	if want := []keptChunk{{chunk: a}, {chunk: b, kept: v41(3)}}; !slices.Equal(got, want) {
		t.Fatalf("decided %+v, want %+v", got, want)
	}
	if !slices.Equal(replies[0].list, []ChunkVersion{{Chunk: a}}) || len(replies[1].list) != 0 || !slices.Equal(replies[2].list, []ChunkVersion{{Chunk: b, Version: v41(3)}}) {
		t.Fatalf("lists %+v, %+v, %+v", replies[0].list, replies[1].list, replies[2].list)
	}
}
