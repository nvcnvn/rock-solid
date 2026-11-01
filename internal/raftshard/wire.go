package raftshard

import (
	"encoding/binary"
	"errors"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// A message is its group's ID and a kind byte, then for a Raft message the encoded
// raftpb.Message inline when all of it fits in core.MaxInlineMsg bytes, or else the encoding's
// length inline and the encoding in a pool buffer; a fortification message is fortBytes inline.
const (
	groupBytes = 8
	kindBytes  = groupBytes + 1
	headBytes  = kindBytes + 4
	fortBytes  = kindBytes + 5*8
)

// Message kinds.
const (
	kindRaft byte = iota
	// kindFortify is a leader asking a follower to support its term.
	kindFortify
	// kindFortified is a follower's answer: the run and support epoch of its node's grant to the leader's node.
	kindFortified
	// kindMember is a replica that heard from no leader for an election timeout asking a peer whether it is still in the group.
	kindMember
	// kindRemoved answers a replica that its sender's applied configuration does not hold, with the sender's applied index in place of a term.
	kindRemoved
)

var le = binary.LittleEndian

// errMalformed reports a message that does not decode, or names a type no peer sends: a local one, or a proposal, which no replica forwards.
var errMalformed = errors.New("raftshard: malformed message")

// fortify is a fortification message: the leader's request names its term, the follower's answer adds its node's grant.
type fortify struct {
	kind           byte
	from, to, term uint64
	run, support   uint64
}

// encode frames m for group, reporting false when the pool has no buffer for it.
func encode(pool core.BufPool, group uint64, m *raftpb.Message, msg *core.Msg) bool {
	n := m.Size()
	le.PutUint64(msg.Body[:], group)
	msg.Body[groupBytes] = kindRaft
	if kindBytes+n <= core.MaxInlineMsg {
		if _, err := m.MarshalTo(msg.Body[kindBytes : kindBytes+n]); err != nil {
			panic(err)
		}
		msg.Len, msg.Buf = uint16(kindBytes+n), core.NoBuf
		return true
	}
	buf, err := pool.Alloc(n)
	if err != nil {
		return false
	}
	if _, err := m.MarshalTo(pool.Bytes(buf)[:n]); err != nil {
		panic(err)
	}
	le.PutUint32(msg.Body[kindBytes:], uint32(n))
	msg.Len, msg.Buf = headBytes, buf
	return true
}

// encodeNode is a node's ID as a configuration change's context carries it.
func encodeNode(n core.NodeID) []byte {
	return le.AppendUint32(le.AppendUint32(nil, n.Node), n.Loop)
}

// decodeNode reads the node a configuration change's context names.
func decodeNode(b []byte) (core.NodeID, bool) {
	if len(b) != 8 {
		return core.NodeID{}, false
	}
	return core.NodeID{Node: le.Uint32(b), Loop: le.Uint32(b[4:])}, true
}

func encodeFortify(group uint64, f *fortify, msg *core.Msg) {
	b := msg.Body[:fortBytes]
	le.PutUint64(b, group)
	b[groupBytes] = f.kind
	for i, v := range [...]uint64{f.from, f.to, f.term, f.run, f.support} {
		le.PutUint64(b[kindBytes+8*i:], v)
	}
	msg.Len, msg.Buf = fortBytes, core.NoBuf
}

// decode reads a message into m, or a fortification message into f, and returns its group and kind; it leaves msg's buffer with the caller.
func decode(pool core.BufPool, msg *core.Msg, m *raftpb.Message, f *fortify) (uint64, byte, error) {
	b := msg.Inline()
	if len(b) < kindBytes {
		return 0, 0, errMalformed
	}
	group, kind, body := le.Uint64(b), b[groupBytes], b[kindBytes:]
	switch kind {
	case kindRaft:
	case kindFortify, kindFortified, kindMember, kindRemoved:
		if msg.Buf != core.NoBuf || len(b) != fortBytes {
			return 0, 0, errMalformed
		}
		f.kind = kind
		for i, v := range [...]*uint64{&f.from, &f.to, &f.term, &f.run, &f.support} {
			*v = le.Uint64(body[8*i:])
		}
		return group, kind, nil
	default:
		return 0, 0, errMalformed
	}
	if msg.Buf != core.NoBuf {
		if len(body) != headBytes-kindBytes {
			return 0, 0, errMalformed
		}
		data := pool.Bytes(msg.Buf)
		n := le.Uint32(body)
		if uint64(n) > uint64(len(data)) {
			return 0, 0, errMalformed
		}
		body = data[:n]
	}
	if err := m.Unmarshal(body); err != nil {
		return 0, 0, errMalformed
	}
	if _, ok := raftpb.MessageType_name[int32(m.Type)]; !ok || raft.IsLocalMsg(m.Type) || m.Type == raftpb.MsgProp {
		return 0, 0, errMalformed
	}
	return group, kindRaft, nil
}
