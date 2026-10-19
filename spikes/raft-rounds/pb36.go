//go:build raft36

package main

import (
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

const raftVersion = "v3.6.0 (raftpb on gogoproto, value fields)"

type entryT = pb.Entry
type msgT = pb.Message

func eData(e entryT) []byte       { return e.Data }
func eIndex(e entryT) uint64      { return e.Index }
func eNormal(e entryT) bool       { return e.Type == pb.EntryNormal }
func mType(m msgT) pb.MessageType { return m.Type }
func mTo(m msgT) uint64           { return m.To }

func mAppend(b []byte, m msgT) []byte {
	n := m.Size()
	l := len(b)
	b = growTo(b, l+n)
	_, _ = m.MarshalToSizedBuffer(b[l : l+n])
	return b
}

func mDecode(b []byte) (msgT, error) {
	var m pb.Message
	err := m.Unmarshal(b)
	return m, err
}

func step(rn *raft.RawNode, m msgT) error { return rn.Step(m) }

func needsPersist(rd *raft.Ready) bool {
	return len(rd.Entries) > 0 || rd.MustSync || !raft.IsEmptySnap(rd.Snapshot)
}

func hardStateSet(rd *raft.Ready) bool { return !raft.IsEmptyHardState(rd.HardState) }

func persist(st *raft.MemoryStorage, rd *raft.Ready) {
	if !raft.IsEmptySnap(rd.Snapshot) {
		_ = st.ApplySnapshot(rd.Snapshot)
	}
	if !raft.IsEmptyHardState(rd.HardState) {
		_ = st.SetHardState(rd.HardState)
	}
	if len(rd.Entries) > 0 {
		_ = st.Append(rd.Entries)
	}
}

var confState pb.ConfState

func bootstrap(st *raft.MemoryStorage, voters []uint64) error {
	confState = pb.ConfState{Voters: voters}
	return st.ApplySnapshot(pb.Snapshot{Metadata: pb.SnapshotMetadata{
		Index: 1, Term: 1, ConfState: confState,
	}})
}

func snapshotAndCompact(st *raft.MemoryStorage, i uint64) {
	if _, err := st.CreateSnapshot(i, &confState, nil); err == nil {
		_ = st.Compact(i)
	}
}
