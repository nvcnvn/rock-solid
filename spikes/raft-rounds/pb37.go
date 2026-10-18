//go:build !raft36

package main

import (
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const raftVersion = "v3.7.0 (raftpb on google.golang.org/protobuf, pointer fields)"

type entryT = *pb.Entry
type msgT = *pb.Message

func eData(e entryT) []byte       { return e.GetData() }
func eIndex(e entryT) uint64      { return e.GetIndex() }
func eNormal(e entryT) bool       { return e.GetType() == pb.EntryNormal }
func mType(m msgT) pb.MessageType { return m.GetType() }
func mTo(m msgT) uint64           { return m.GetTo() }

func mAppend(b []byte, m msgT) []byte {
	b, _ = proto.MarshalOptions{}.MarshalAppend(b, m)
	return b
}

func mDecode(b []byte) (msgT, error) {
	m := new(pb.Message)
	err := proto.Unmarshal(b, m)
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

var confState = &pb.ConfState{}

func bootstrap(st *raft.MemoryStorage, voters []uint64) error {
	confState = &pb.ConfState{Voters: voters}
	return st.ApplySnapshot(&pb.Snapshot{Metadata: &pb.SnapshotMetadata{
		Index: proto.Uint64(1), Term: proto.Uint64(1), ConfState: confState,
	}})
}

func snapshotAndCompact(st *raft.MemoryStorage, i uint64) {
	if _, err := st.CreateSnapshot(i, confState, nil); err == nil {
		_ = st.Compact(i)
	}
}
