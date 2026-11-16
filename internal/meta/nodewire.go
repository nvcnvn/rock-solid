package meta

import (
	"encoding/binary"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
)

// A storage node and the control plane talk in the sharded API's framing, addressed to the
// control-plane group, every number a uvarint. A NodeReport's body is its node, release, a flags
// byte (1 Meta, 2 MetaOnly), epoch and session version, then a count and each opened disk's device,
// log, blocks, reserve, class and loop, a count and each failed device, a count and each halt's group,
// ID, index and age. A session fetch has no body; its token is the node's. A session table's
// token is the fetch it answers, 0 when pushed unasked, and its body is its version, next ID, a
// count and each live ID as its gap above the one before. A placement table's body is
// chunkstore.AppendTable's encoding and a delta's AppendDelta's, both with token 0; a table fetch
// has no body. A voter that does not lead answers a report, a fetch or a session start with the
// leader it knows: a presence byte, then its node and loop. A session start's token is its
// request and it has no body; its answer, once the start commits, carries the request as its token
// and the session's ID as its body. The disk membership, the leader's answer to a report, is a
// count and, in rising disk ID order, each disk's ID, log, and the node and loop that own it.
const (
	KindNodeReport   byte = 0x42
	KindSessionFetch byte = 0x43
	KindSessions     byte = 0x44
	KindTable        byte = 0x45
	KindDelta        byte = 0x46
	KindTableFetch   byte = 0x47
	KindLeader       byte = 0x48
	KindSessionStart byte = 0x49
	KindStarted      byte = 0x4a
	KindDisks        byte = 0x4b
)

// DiskAddr is a disk of the committed membership record: its ID, its log, and the loop that owns it.
type DiskAddr struct {
	Disk uint32
	Log  uint64
	At   core.NodeID
}

// encodeBody frames body as a message of kind into msg, or returns the pool's error.
func encodeBody(pool core.BufPool, group uint64, kind byte, token uint64, body []byte, msg *core.Msg) error {
	dst, err := frameMsg(pool, group, kind, token, len(body), msg)
	if err != nil {
		return err
	}
	copy(dst[:len(body)], body)
	return nil
}

// EncodeNodeReport writes nr, sent to the control-plane group, into msg, or returns the pool's error.
func EncodeNodeReport(pool core.BufPool, group uint64, nr *NodeReport, msg *core.Msg) error {
	return encodeBody(pool, group, KindNodeReport, 0, appendNodeReport(nil, nr), msg)
}

func appendNodeReport(dst []byte, nr *NodeReport) []byte {
	var flags byte
	if nr.Meta {
		flags |= 1
	}
	if nr.MetaOnly {
		flags |= 2
	}
	dst = binary.AppendUvarint(binary.AppendUvarint(dst, uint64(nr.Node)), uint64(nr.Release))
	dst = binary.AppendUvarint(binary.AppendUvarint(append(dst, flags), nr.Epoch), nr.Sessions)
	dst = binary.AppendUvarint(dst, uint64(len(nr.Opened)))
	for _, o := range nr.Opened {
		dst = binary.AppendUvarint(binary.AppendUvarint(dst, o.Device), o.Log)
		dst = binary.AppendUvarint(binary.AppendUvarint(dst, o.Blocks), o.Reserve)
		dst = binary.AppendUvarint(appendBytes(dst, []byte(o.Class)), uint64(o.Loop))
	}
	dst = binary.AppendUvarint(dst, uint64(len(nr.Failed)))
	for _, f := range nr.Failed {
		dst = binary.AppendUvarint(dst, f)
	}
	dst = binary.AppendUvarint(dst, uint64(len(nr.Halts)))
	for _, h := range nr.Halts {
		dst = binary.AppendUvarint(binary.AppendUvarint(dst, h.Group), h.ID)
		dst = binary.AppendUvarint(binary.AppendUvarint(dst, h.Index), uint64(h.Age))
	}
	return dst
}

// DecodeNodeReport decodes a NodeReport message into nr, reusing its slices; one that does not
// decode, or has a token, is ErrMalformed.
func DecodeNodeReport(pool core.BufPool, msg *core.Msg, nr *NodeReport) error {
	b, err := msgBody(pool, KindNodeReport, msg)
	if err != nil {
		return err
	}
	if _, _, token, _ := Peek(msg); token != 0 {
		return ErrMalformed
	}
	d := decoder{b: b}
	node, release := d.uvarint(), d.uvarint()
	flags := d.byte()
	*nr = NodeReport{Node: uint32(node), Release: uint32(release), Meta: flags&1 != 0, MetaOnly: flags&2 != 0,
		Epoch: d.uvarint(), Sessions: d.uvarint(), Opened: nr.Opened[:0], Failed: nr.Failed[:0], Halts: nr.Halts[:0]}
	if node > 0xffffffff || release > 0xffffffff || flags > 3 {
		return ErrMalformed
	}
	for n := d.count(); n > 0; n-- {
		o := OpenedDisk{Device: d.uvarint(), Log: d.uvarint(), Blocks: d.uvarint(), Reserve: d.uvarint()}
		o.Class = string(d.bytes())
		loop := d.uvarint()
		if loop > 0xffffffff {
			return ErrMalformed
		}
		o.Loop = uint32(loop)
		nr.Opened = append(nr.Opened, o)
	}
	for n := d.count(); n > 0; n-- {
		nr.Failed = append(nr.Failed, d.uvarint())
	}
	for n := d.count(); n > 0; n-- {
		nr.Halts = append(nr.Halts, HaltReport{Group: d.uvarint(), ID: d.uvarint(), Index: d.uvarint(), Age: core.Tick(d.uvarint())})
	}
	if d.bad || len(d.b) != 0 {
		return ErrMalformed
	}
	return nil
}

// count is a count of items of at least a byte each, or 0 and bad for more than the bytes left.
func (d *decoder) count() uint64 {
	n := d.uvarint()
	if n > uint64(len(d.b)) {
		d.bad, d.b = true, nil
		return 0
	}
	return n
}

// EncodeSessionFetch writes a node's ask for the newest session table under token into msg.
func EncodeSessionFetch(pool core.BufPool, group, token uint64, msg *core.Msg) error {
	return encodeBody(pool, group, KindSessionFetch, token, nil, msg)
}

// EncodeSessions writes t, answering the fetch token or pushed with token 0, into msg, or returns
// the pool's error.
func EncodeSessions(pool core.BufPool, group, token uint64, t *chunkstore.SessionTable, msg *core.Msg) error {
	body := binary.AppendUvarint(binary.AppendUvarint(nil, t.Version), t.Next)
	body = binary.AppendUvarint(body, uint64(len(t.Live)))
	last := uint64(0)
	for _, id := range t.Live {
		body, last = binary.AppendUvarint(body, id-last), id
	}
	return encodeBody(pool, group, KindSessions, token, body, msg)
}

// DecodeSessions decodes a session table message into t, reusing its Live; one that does not
// decode, or whose live IDs are not rising, nonzero and below its next ID, is ErrMalformed. Peek
// gives the fetch it answers.
func DecodeSessions(pool core.BufPool, msg *core.Msg, t *chunkstore.SessionTable) error {
	b, err := msgBody(pool, KindSessions, msg)
	if err != nil {
		return err
	}
	d := decoder{b: b}
	*t = chunkstore.SessionTable{Version: d.uvarint(), Next: d.uvarint(), Live: t.Live[:0]}
	last := uint64(0)
	for n := d.count(); n > 0; n-- {
		gap := d.uvarint()
		if gap == 0 || last+gap < last {
			return ErrMalformed
		}
		last += gap
		t.Live = append(t.Live, last)
	}
	if d.bad || len(d.b) != 0 || len(t.Live) > 0 && last >= t.Next {
		return ErrMalformed
	}
	return nil
}

// EncodeTable writes placement table t into msg, or returns the pool's error.
func EncodeTable(pool core.BufPool, group uint64, t *chunkstore.Table, msg *core.Msg) error {
	return encodeBody(pool, group, KindTable, 0, chunkstore.AppendTable(nil, t), msg)
}

// untokened is the body of a message of kind that carries no token, or ErrMalformed.
func untokened(pool core.BufPool, kind byte, msg *core.Msg) ([]byte, error) {
	if _, _, token, _ := Peek(msg); token != 0 {
		return nil, ErrMalformed
	}
	return msgBody(pool, kind, msg)
}

// DecodeTable decodes a placement table message; one that does not decode, or has a token, is
// ErrMalformed.
func DecodeTable(pool core.BufPool, msg *core.Msg) (chunkstore.Table, error) {
	b, err := untokened(pool, KindTable, msg)
	if err != nil {
		return chunkstore.Table{}, err
	}
	t, err := chunkstore.DecodeTable(b)
	if err != nil {
		return chunkstore.Table{}, ErrMalformed
	}
	return t, nil
}

// EncodeDelta writes placement table delta d into msg, or returns the pool's error.
func EncodeDelta(pool core.BufPool, group uint64, d *chunkstore.Delta, msg *core.Msg) error {
	return encodeBody(pool, group, KindDelta, 0, chunkstore.AppendDelta(nil, d), msg)
}

// DecodeDelta decodes a delta message; one that does not decode, or has a token, is ErrMalformed.
func DecodeDelta(pool core.BufPool, msg *core.Msg) (chunkstore.Delta, error) {
	b, err := untokened(pool, KindDelta, msg)
	if err != nil {
		return chunkstore.Delta{}, err
	}
	d, err := chunkstore.DecodeDelta(b)
	if err != nil {
		return chunkstore.Delta{}, ErrMalformed
	}
	return d, nil
}

// EncodeTableFetch writes a node's ask for the whole placement table into msg.
func EncodeTableFetch(pool core.BufPool, group uint64, msg *core.Msg) error {
	return encodeBody(pool, group, KindTableFetch, 0, nil, msg)
}

// EncodeLeader writes the leader a voter knows, at leader if known, into msg.
func EncodeLeader(pool core.BufPool, group uint64, leader core.NodeID, known bool, msg *core.Msg) error {
	body := []byte{0}
	if known {
		body = binary.AppendUvarint(binary.AppendUvarint([]byte{1}, uint64(leader.Node)), uint64(leader.Loop))
	}
	return encodeBody(pool, group, KindLeader, 0, body, msg)
}

// DecodeLeader decodes a leader message: the leader and whether the voter knows one; one that
// does not decode, or has a token, is ErrMalformed.
func DecodeLeader(pool core.BufPool, msg *core.Msg) (core.NodeID, bool, error) {
	b, err := untokened(pool, KindLeader, msg)
	if err != nil {
		return core.NodeID{}, false, err
	}
	d := decoder{b: b}
	known := d.byte()
	var n, l uint64
	if known == 1 {
		n, l = d.uvarint(), d.uvarint()
	}
	if d.bad || len(d.b) != 0 || known > 1 || n > 0xffffffff || l > 0xffffffff {
		return core.NodeID{}, false, ErrMalformed
	}
	return core.NodeID{Node: uint32(n), Loop: uint32(l)}, known == 1, nil
}

// EncodeSessionStart writes a client's ask to start a session, its request numbered request, into msg.
func EncodeSessionStart(pool core.BufPool, group, request uint64, msg *core.Msg) error {
	return encodeBody(pool, group, KindSessionStart, request, nil, msg)
}

// EncodeStarted writes the answer to the start request: the session id it committed.
func EncodeStarted(pool core.BufPool, group, request, id uint64, msg *core.Msg) error {
	return encodeBody(pool, group, KindStarted, request, binary.AppendUvarint(nil, id), msg)
}

// DecodeStarted decodes a started message: the session's ID, never 0; one that does not decode is
// ErrMalformed. Peek gives the request it answers.
func DecodeStarted(pool core.BufPool, msg *core.Msg) (uint64, error) {
	b, err := msgBody(pool, KindStarted, msg)
	if err != nil {
		return 0, err
	}
	d := decoder{b: b}
	id := d.uvarint()
	if d.bad || len(d.b) != 0 || id == 0 {
		return 0, ErrMalformed
	}
	return id, nil
}

// EncodeDisks writes the disk membership ds, in rising disk ID order, into msg, or returns the
// pool's error.
func EncodeDisks(pool core.BufPool, group uint64, ds []DiskAddr, msg *core.Msg) error {
	body := binary.AppendUvarint(nil, uint64(len(ds)))
	for _, d := range ds {
		body = binary.AppendUvarint(binary.AppendUvarint(body, uint64(d.Disk)), d.Log)
		body = binary.AppendUvarint(binary.AppendUvarint(body, uint64(d.At.Node)), uint64(d.At.Loop))
	}
	return encodeBody(pool, group, KindDisks, 0, body, msg)
}

// DecodeDisks decodes a disk membership message into dst; one that does not decode, has a token,
// or whose disk IDs are not nonzero and rising, is ErrMalformed.
func DecodeDisks(pool core.BufPool, msg *core.Msg, dst []DiskAddr) ([]DiskAddr, error) {
	dst = dst[:0]
	b, err := untokened(pool, KindDisks, msg)
	if err != nil {
		return dst, err
	}
	d := decoder{b: b}
	last := uint64(0)
	for n := d.count(); n > 0; n-- {
		id, log, node, loop := d.uvarint(), d.uvarint(), d.uvarint(), d.uvarint()
		if id <= last || id > 0xffffffff || node > 0xffffffff || loop > 0xffffffff {
			return dst[:0], ErrMalformed
		}
		last = id
		dst = append(dst, DiskAddr{Disk: uint32(id), Log: log, At: core.NodeID{Node: uint32(node), Loop: uint32(loop)}})
	}
	if d.bad || len(d.b) != 0 {
		return dst[:0], ErrMalformed
	}
	return dst, nil
}
