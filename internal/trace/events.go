package trace

import (
	"fmt"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// MsgType is a message's type as mt gives it, or its first inline byte when mt is nil, 0 for an empty message.
func MsgType(m *core.Msg, mt func(*core.Msg) uint64) uint64 {
	if mt != nil {
		return mt(m)
	}
	if m.Len == 0 {
		return 0
	}
	return uint64(m.Body[0])
}

// EventRecord is the record of an event a loop handles; mt gives a message its type, the first inline byte when nil.
func EventRecord(ev *core.Event, mt func(*core.Msg) uint64) Record {
	var r Record
	switch ev.Kind {
	case core.EvNetRecv:
		r.Name = EvNetRecv
		r.Fields = [MaxFields]uint64{Peer(ev.Peer), MsgType(&ev.Msg, mt)}
	case core.EvPeerLost:
		r.Name = EvPeerLost
		r.Fields[0] = Peer(ev.Peer)
	case core.EvDiskDone:
		r.Name = EvDiskDone
		r.Fields = [MaxFields]uint64{uint64(ev.IO), uint64(ev.Err)}
	case core.EvTimer:
		r.Name = EvTimer
		r.Fields[0] = uint64(ev.Timer)
	case core.EvResumed:
		r.Name = EvResumed
	default:
		panic(fmt.Sprintf("trace: event %+v", *ev))
	}
	return r
}

// ActionRecord is the record of an action a loop executes; mt gives a message its type, the first inline byte when nil.
func ActionRecord(a *core.Action, mt func(*core.Msg) uint64) Record {
	var r Record
	switch a.Kind {
	case core.ActNetSend:
		r.Name = ActNetSend
		r.Fields = [MaxFields]uint64{Peer(a.Peer), MsgType(&a.Msg, mt)}
	case core.ActDiskRead:
		r.Name = ActDiskRead
		r.Fields = [MaxFields]uint64{uint64(a.IO), uint64(a.LBA), a.Len}
	case core.ActDiskWrite:
		r.Name = ActDiskWrite
		r.Fields = [MaxFields]uint64{0, uint64(a.IO), uint64(a.LBA), a.Len, uint64(max(len(a.Vec), 1))}
	case core.ActDiskFsync:
		r.Name = ActDiskFsync
		r.Fields = [MaxFields]uint64{0, uint64(a.IO), 0}
		if a.Linked {
			r.Fields[2] = 1
		}
	case core.ActDiskTrim:
		r.Name = ActDiskTrim
		r.Fields = [MaxFields]uint64{uint64(a.IO), uint64(a.LBA), a.Len}
	case core.ActSetTimer:
		r.Name = ActSetTimer
		r.Fields = [MaxFields]uint64{uint64(a.Timer), uint64(a.At)}
	default:
		panic(fmt.Sprintf("trace: action %+v", *a))
	}
	return r
}
