package raftshard

import (
	"encoding/binary"
	"errors"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// A message is its group's ID followed by the encoded raftpb.Message inline when both fit in
// core.MaxInlineMsg bytes, or else the group's ID and the encoding's length inline and the encoding
// in a pool buffer.
const (
	groupBytes = 8
	headBytes  = groupBytes + 4
)

var le = binary.LittleEndian

// errMalformed reports a message that does not decode, or names a type no peer sends: a local one, or a proposal, which no replica forwards.
var errMalformed = errors.New("raftshard: malformed message")

// encode frames m for group, reporting false when the pool has no buffer for it.
func encode(pool core.BufPool, group uint64, m *raftpb.Message, msg *core.Msg) bool {
	n := m.Size()
	le.PutUint64(msg.Body[:], group)
	if groupBytes+n <= core.MaxInlineMsg {
		if _, err := m.MarshalTo(msg.Body[groupBytes : groupBytes+n]); err != nil {
			panic(err)
		}
		msg.Len, msg.Buf = uint16(groupBytes+n), core.NoBuf
		return true
	}
	buf, err := pool.Alloc(n)
	if err != nil {
		return false
	}
	if _, err := m.MarshalTo(pool.Bytes(buf)[:n]); err != nil {
		panic(err)
	}
	le.PutUint32(msg.Body[groupBytes:], uint32(n))
	msg.Len, msg.Buf = headBytes, buf
	return true
}

// decode reads a message into m and returns its group; it leaves msg's buffer with the caller.
func decode(pool core.BufPool, msg *core.Msg, m *raftpb.Message) (uint64, error) {
	b := msg.Inline()
	if len(b) < groupBytes {
		return 0, errMalformed
	}
	group, body := le.Uint64(b), b[groupBytes:]
	if msg.Buf != core.NoBuf {
		if len(body) != headBytes-groupBytes {
			return 0, errMalformed
		}
		data := pool.Bytes(msg.Buf)
		n := le.Uint32(body)
		if uint64(n) > uint64(len(data)) {
			return 0, errMalformed
		}
		body = data[:n]
	}
	if err := m.Unmarshal(body); err != nil {
		return 0, errMalformed
	}
	if _, ok := raftpb.MessageType_name[int32(m.Type)]; !ok || raft.IsLocalMsg(m.Type) || m.Type == raftpb.MsgProp {
		return 0, errMalformed
	}
	return group, nil
}
