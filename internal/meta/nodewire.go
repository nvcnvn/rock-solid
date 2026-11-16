package meta

import (
	"encoding/binary"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
)

// A storage node and the control plane talk in the sharded API's framing, addressed to the
// control-plane group, every number a uvarint. A NodeReport's body is its node, release, a flags
// byte (1 Meta, 2 MetaOnly), epoch and session version, then a count and each opened disk's device,
// log, blocks, reserve and class, a count and each failed device, a count and each halt's group,
// ID, index and age. A session fetch has no body; its token is the node's. A session table's
// token is the fetch it answers, 0 when pushed unasked, and its body is its version, next ID, a
// count and each live ID as its gap above the one before.
const (
	KindNodeReport   byte = 0x42
	KindSessionFetch byte = 0x43
	KindSessions     byte = 0x44
)

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
		dst = appendBytes(dst, []byte(o.Class))
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
