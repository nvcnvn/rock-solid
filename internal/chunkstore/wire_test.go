package chunkstore

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

var (
	wirePG    = PGFields{Epoch: 77, PG: core.PGID{Pool: 1, Index: 17}, Interval: 41}
	wireChunk = core.ChunkID{Volume: 2, Inode: 3, Index: 4}
)

type wireCase struct {
	name string
	msg  any
	data int
}

// wireCases holds a message of every kind, each optional field present and absent; testdata/wire_golden.txt freezes their frames.
func wireCases() []wireCase {
	v := MakeVersion(41, 7)
	upd := func(k Kind, off, n uint32, ver uint64, hasExpect bool, expect uint64) *Update {
		return &Update{Kind: k, ID: 5, PG: wirePG, Session: 9, Incarnation: 1_000_000, Chunk: wireChunk,
			Off: off, Len: n, Snapshot: 2, Version: ver, HasExpect: hasExpect, Expect: expect}
	}
	return []wireCase{
		{"hello", &Hello{ID: 1, Session: 9, Incarnation: 1_000_000}, 0},
		{"write", upd(KindWrite, 100, 200, 0, false, 0), core.BlockSize},
		{"write_expect", upd(KindWrite, 4000, 200, 0, true, MakeVersion(3, 9)), 2 * core.BlockSize},
		{"replica_write", upd(KindReplicaWrite, 4000, 200, v, true, MakeVersion(3, 9)), 2 * core.BlockSize},
		{"punch", upd(KindPunch, 0, core.ChunkSize, 0, false, 0), 0},
		{"punch_expect_absent_chunk", upd(KindPunch, 8192, 1, 0, true, 0), 0},
		{"replica_punch", upd(KindReplicaPunch, 0, core.ChunkSize, v, false, 0), 0},
		{"purger_punch", &Update{Kind: KindPunch, ID: 5, PG: wirePG, Session: 9, Incarnation: 1_000_000, Chunk: wireChunk,
			Len: core.ChunkSize, Snapshot: 2, Shard: 0x51, Term: 6}, 0},
		{"ack", &Response{Kind: KindAck, ID: 5, Version: v, Fill: 123_456}, 0},
		{"ack_no_space", &Response{Kind: KindAck, ID: 5, Status: StatusNoSpace, Fill: 900_000}, 0},
		{"reply", &Response{Kind: KindReply, ID: 5, Version: v}, 0},
		{"reply_refused", &Response{Kind: KindReply, ID: 5, Status: StatusRefused, Epoch: 80, Interval: 42}, 0},
		{"reply_mismatch", &Response{Kind: KindReply, ID: 5, Status: StatusMismatch, Version: v}, 0},
		{"read", &Read{ID: 6, PG: wirePG, Chunk: wireChunk, Off: 100, Len: 5000}, 0},
		{"read_at", &Read{ID: 6, PG: wirePG, Chunk: wireChunk, Off: 0, Len: 4096, HasAt: true, At: v}, 0},
		{"read_reply", &Response{Kind: KindReadReply, ID: 6, Version: v}, 2 * core.BlockSize},
		{"read_reply_missing", &Response{Kind: KindReadReply, ID: 6, Status: StatusMissing}, 0},
		{"read_reply_unfound", &Response{Kind: KindReadReply, ID: 6, Status: StatusUnfound}, 0},
		{"lease_request", &LeaseRequest{ID: 7, Epoch: 77}, 0},
		{"grant", &Grant{ID: 7, Support: 8, Run: 0x0123456789abcdef, Raw: 51_020_500}, 0},
		{"grant_refused", &Grant{ID: 7, Status: StatusRefused, Epoch: 80}, 0},
		{"heartbeat", &Heartbeat{ID: 8, Epoch: 77, Run: 0x0123456789abcdef, Raw: 51_020_500}, 0},
		{"report_empty", &Report{ID: 9}, 0},
		{"report", &Report{ID: 9, Activations: []PGFields{wirePG}, Latencies: []Latency{{Disk: 3, Latency: 250}},
			Disks: []DiskState{{Log: 0x0123456789abcdef, Fill: 1000, Oldest: 40}, {Log: 2, Fill: 5, Oldest: 1_500_000, Failed: true}}}, 0},
		{"member_timeout", &MemberTimeout{ID: 10, PG: wirePG, Disk: 3}, 0},
		{"probe", &Probe{ID: 11, Disk: 3}, 0},
		{"probe_reply", &ProbeReply{ID: 11, Log: 0x0123456789abcdef, Oldest: 1_500_000, Failed: true}, 0},
		{"peer", &Peering{Kind: KindPeer, ID: 12, PG: wirePG}, 0},
		{"peer_reply", &Peering{Kind: KindPeerReply, ID: 12, PG: wirePG,
			Newest:  []ChunkVersion{{Chunk: wireChunk, Version: v}, {Chunk: core.ChunkID{Volume: 2, Inode: 3, Index: 5}, Version: MakeVersion(40, 1)}},
			Missing: []ChunkVersion{{Chunk: wireChunk, Version: v}}}, 0},
	}
}

func encodeMsg(msg any) []byte {
	switch m := msg.(type) {
	case *Hello:
		return AppendHello(nil, m)
	case *Update:
		return AppendUpdate(nil, m)
	case *Read:
		return AppendRead(nil, m)
	case *Response:
		return AppendResponse(nil, m)
	case *LeaseRequest:
		return AppendLeaseRequest(nil, m)
	case *Grant:
		return AppendGrant(nil, m)
	case *Heartbeat:
		return AppendHeartbeat(nil, m)
	case *Report:
		return AppendReport(nil, m)
	case *MemberTimeout:
		return AppendMemberTimeout(nil, m)
	case *Probe:
		return AppendProbe(nil, m)
	case *ProbeReply:
		return AppendProbeReply(nil, m)
	case *Peering:
		return AppendPeering(nil, m)
	}
	panic("unknown message type")
}

func decodeMsg(b []byte, data int) (any, error) {
	k, err := Peek(b)
	if err != nil {
		return nil, err
	}
	switch k {
	case KindHello:
		var m Hello
		return &m, DecodeHello(b, data, &m)
	case KindWrite, KindReplicaWrite, KindPunch, KindReplicaPunch:
		var m Update
		return &m, DecodeUpdate(b, data, &m)
	case KindRead:
		var m Read
		return &m, DecodeRead(b, data, &m)
	case KindAck, KindReply, KindReadReply:
		var m Response
		return &m, DecodeResponse(b, data, &m)
	case KindLeaseRequest:
		var m LeaseRequest
		return &m, DecodeLeaseRequest(b, data, &m)
	case KindGrant:
		var m Grant
		return &m, DecodeGrant(b, data, &m)
	case KindHeartbeat:
		var m Heartbeat
		return &m, DecodeHeartbeat(b, data, &m)
	case KindReport:
		var m Report
		return &m, DecodeReport(b, data, &m)
	case KindMemberTimeout:
		var m MemberTimeout
		return &m, DecodeMemberTimeout(b, data, &m)
	case KindProbe:
		var m Probe
		return &m, DecodeProbe(b, data, &m)
	case KindProbeReply:
		var m ProbeReply
		return &m, DecodeProbeReply(b, data, &m)
	case KindPeer, KindPeerReply:
		var m Peering
		return &m, DecodePeering(b, data, &m)
	}
	panic("Peek returned an unknown kind")
}

// padded is a frame as Attach carries it: past MaxInlineMsg, zero-padded to whole blocks.
func padded(frame []byte) []byte {
	if len(frame) <= core.MaxInlineMsg {
		return frame
	}
	return append(frame, make([]byte, (core.BlockSize-len(frame)%core.BlockSize)%core.BlockSize)...)
}

func mustMalformed(t *testing.T, what string, b []byte, data int) {
	t.Helper()
	if _, err := decodeMsg(b, data); !errors.Is(err, ErrMalformed) {
		t.Fatalf("%s: got %v, want ErrMalformed", what, err)
	}
}

func TestWireRoundTrip(t *testing.T) {
	kinds := map[Kind]bool{}
	for _, c := range wireCases() {
		b := encodeMsg(c.msg)
		got, err := decodeMsg(b, c.data)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if !reflect.DeepEqual(got, c.msg) {
			t.Fatalf("%s: decoded %+v, want %+v", c.name, got, c.msg)
		}
		kinds[Kind(b[1])] = true
	}
	for k := KindHello; k < kindEnd; k++ {
		if !kinds[k] {
			t.Errorf("kind %d has no case", k)
		}
	}
}

func TestWireVersionPacking(t *testing.T) {
	if v := MakeVersion(1, 1); v != 0x0000000100000001 {
		t.Fatalf("1.1 packs to %#016x", v)
	}
}

func readWireGolden(t *testing.T) map[string][]byte {
	t.Helper()
	f, err := os.Open("testdata/wire_golden.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	golden := map[string][]byte{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, h, ok := strings.Cut(line, " ")
		b, err := hex.DecodeString(h)
		if !ok || err != nil {
			t.Fatalf("bad golden line %q", line)
		}
		golden[name] = b
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return golden
}

// wire_golden.txt is frozen wire format; the test reads it, never the reverse.
func TestWireGolden(t *testing.T) {
	golden := readWireGolden(t)
	for _, c := range wireCases() {
		want, ok := golden[c.name]
		if !ok {
			t.Fatalf("%s: missing from the fixture", c.name)
		}
		if got := encodeMsg(c.msg); !bytes.Equal(got, want) {
			t.Fatalf("%s: encoded\n%x\nwant\n%x", c.name, got, want)
		}
	}
	if len(golden) != len(wireCases()) {
		t.Fatalf("fixture has %d frames, cases %d", len(golden), len(wireCases()))
	}
}

func TestWireRejectsOtherVersion(t *testing.T) {
	for _, c := range wireCases() {
		for _, v := range []byte{0, 2, 255} {
			b := encodeMsg(c.msg)
			b[0] = v
			if _, err := decodeMsg(b, c.data); !errors.Is(err, ErrWireVersion) {
				t.Fatalf("%s version %d: got %v", c.name, v, err)
			}
		}
	}
}

func TestWireRejectsUnknownKind(t *testing.T) {
	b := AppendHello(nil, &Hello{ID: 1})
	for _, k := range []byte{0, byte(kindEnd), 255} {
		b[1] = k
		if _, err := Peek(b); !errors.Is(err, ErrMalformed) {
			t.Fatalf("kind %d: Peek got %v", k, err)
		}
		if err := DecodeHello(b, 0, &Hello{}); !errors.Is(err, ErrMalformed) {
			t.Fatalf("kind %d: DecodeHello got %v", k, err)
		}
	}
	if err := DecodeHello(AppendProbe(nil, &Probe{ID: 1}), 0, &Hello{}); !errors.Is(err, ErrMalformed) {
		t.Fatalf("a probe decoded as a hello: %v", err)
	}
}

func TestWireRejectsTruncatedAndTrailing(t *testing.T) {
	for _, c := range wireCases() {
		b := encodeMsg(c.msg)
		for n := range len(b) {
			mustMalformed(t, c.name+" truncated", b[:n], c.data)
		}
		mustMalformed(t, c.name+" with a trailing 0", append(b, 0), c.data)
		mustMalformed(t, c.name+" with a trailing 1", append(b, 1), c.data)
	}
}

func TestWireRejectsRanges(t *testing.T) {
	for _, r := range []struct{ off, n uint32 }{{core.ChunkSize - 100, 101}, {core.ChunkSize, 1}, {0, core.ChunkSize + 1}, {0xffffffff, 2}, {100, 0}} {
		for _, k := range []Kind{KindWrite, KindReplicaWrite, KindPunch, KindReplicaPunch} {
			u := &Update{Kind: k, PG: wirePG, Off: r.off, Len: r.n}
			data := 0
			if k == KindReplicaWrite || k == KindReplicaPunch {
				u.Version = 1<<32 | 1
			}
			if k == KindWrite || k == KindReplicaWrite {
				data = core.BlockSize
			}
			mustMalformed(t, "update range", AppendUpdate(nil, u), data)
		}
		mustMalformed(t, "read range", AppendRead(nil, &Read{PG: wirePG, Off: r.off, Len: r.n}), 0)
	}
}

func TestWireRejectsDataLength(t *testing.T) {
	for _, c := range wireCases() {
		b := encodeMsg(c.msg)
		deltas := []int{-1, 1, -core.BlockSize, core.BlockSize}
		if Kind(b[1]) == KindReadReply {
			deltas = deltas[:2]
		}
		for _, delta := range deltas {
			if d := c.data + delta; d >= 0 {
				mustMalformed(t, c.name+" data length", b, d)
			}
		}
	}
	whole := AppendUpdate(nil, &Update{Kind: KindReplicaWrite, PG: wirePG, Off: 0, Len: core.ChunkSize, Version: 1<<32 | 1})
	if _, err := decodeMsg(whole, core.ChunkSize); err != nil {
		t.Fatal(err)
	}
	mustMalformed(t, "1,023 blocks for 1,024", whole, core.ChunkSize-core.BlockSize)
	mustMalformed(t, "read reply with no data", encodeMsg(&Response{Kind: KindReadReply}), 0)
	mustMalformed(t, "read reply past a chunk", encodeMsg(&Response{Kind: KindReadReply}), core.ChunkSize+core.BlockSize)
	mustMalformed(t, "read reply refused with data", encodeMsg(&Response{Kind: KindReadReply, Status: StatusRefused, Interval: 42}), core.BlockSize)
}

func TestWireRejectsVersions(t *testing.T) {
	for _, v := range []uint64{MakeVersion(0, 5), MakeVersion(5, 0)} {
		mustMalformed(t, "replica write version", AppendUpdate(nil, &Update{Kind: KindReplicaWrite, PG: wirePG, Len: 1, Version: v}), core.BlockSize)
		mustMalformed(t, "expect_ver", AppendUpdate(nil, &Update{Kind: KindWrite, PG: wirePG, Len: 1, HasExpect: true, Expect: v}), core.BlockSize)
		mustMalformed(t, "at_ver", AppendRead(nil, &Read{PG: wirePG, Len: 1, HasAt: true, At: v}), 0)
		mustMalformed(t, "reply version", AppendResponse(nil, &Response{Kind: KindAck, Version: v}), 0)
		mustMalformed(t, "peering entry", AppendPeering(nil, &Peering{Kind: KindPeerReply, PG: wirePG, Missing: []ChunkVersion{{Version: v}}}), 0)
	}
	mustMalformed(t, "replica punch at version 0", AppendUpdate(nil, &Update{Kind: KindReplicaPunch, PG: wirePG, Len: 1}), 0)
	mustMalformed(t, "client write with a version", AppendUpdate(nil, &Update{Kind: KindWrite, PG: wirePG, Len: 1, Version: 1<<32 | 1}), core.BlockSize)
	mustMalformed(t, "absent expect_ver not zero", AppendUpdate(nil, &Update{Kind: KindWrite, PG: wirePG, Len: 1, Expect: 1<<32 | 1}), core.BlockSize)
}

func TestWireRejectsIntervalZero(t *testing.T) {
	pg := wirePG
	pg.Interval = 0
	for what, b := range map[string][]byte{
		"update":         AppendUpdate(nil, &Update{Kind: KindPunch, PG: pg, Len: 1}),
		"read":           AppendRead(nil, &Read{PG: pg, Len: 1}),
		"member timeout": AppendMemberTimeout(nil, &MemberTimeout{PG: pg}),
		"peering":        AppendPeering(nil, &Peering{Kind: KindPeer, PG: pg}),
		"activation":     AppendReport(nil, &Report{Activations: []PGFields{pg}}),
		"refusal":        AppendResponse(nil, &Response{Kind: KindReply, Status: StatusRefused, Epoch: 80}),
	} {
		mustMalformed(t, what, b, 0)
	}
}

func TestWireRejectsBadFields(t *testing.T) {
	for _, c := range []struct {
		k  Kind
		st Status
	}{{KindAck, StatusMismatch}, {KindAck, StatusMissing}, {KindReply, StatusUnfound}, {KindReadReply, StatusNoSpace}, {KindReadReply, StatusMismatch}, {KindReply, 6}, {KindAck, 255}} {
		mustMalformed(t, "a status its reply kind does not take", AppendResponse(nil, &Response{Kind: c.k, Status: c.st, Interval: 1}), 0)
	}
	mustMalformed(t, "an acknowledgment of version 0", AppendResponse(nil, &Response{Kind: KindAck}), 0)
	mustMalformed(t, "a purger's shard on a write", AppendUpdate(nil, &Update{Kind: KindWrite, PG: wirePG, Len: 1, Shard: 1}), core.BlockSize)
	mustMalformed(t, "a purger's term on a member write", AppendUpdate(nil, &Update{Kind: KindReplicaWrite, PG: wirePG, Len: 1, Version: 1<<32 | 1, Term: 1}), core.BlockSize)
	mustMalformed(t, "a grant neither granted nor refused", AppendGrant(nil, &Grant{Status: StatusNoSpace}), 0)
	mustMalformed(t, "peering request with lists", AppendPeering(nil, &Peering{Kind: KindPeer, PG: wirePG, Newest: []ChunkVersion{{}}}), 0)
	b := AppendProbeReply(nil, &ProbeReply{})
	b[len(b)-1] = 2
	mustMalformed(t, "a flag of 2", b, 0)
	b = AppendUpdate(nil, &Update{Kind: KindPunch, PG: wirePG, Len: 1})
	b[len(b)-9] = 2
	mustMalformed(t, "an optional's flag of 2", b, 0)
	b = AppendReport(nil, &Report{})
	binary.LittleEndian.PutUint32(b[headSize:], 1<<31)
	mustMalformed(t, "a count past the frame", b, 0)
}

func TestWireDataStaysInItsBuffer(t *testing.T) {
	pool := sim.NewPool(8<<20, 0)
	for _, blocks := range []int{1, core.BlocksPerChunk} {
		data, err := pool.Alloc(blocks * core.BlockSize)
		if err != nil {
			t.Fatal(err)
		}
		for i := range blocks {
			binary.LittleEndian.PutUint32(pool.Bytes(data)[i*core.BlockSize:], uint32(i))
		}
		u := Update{Kind: KindReplicaWrite, ID: 3, PG: wirePG, Chunk: wireChunk, Len: uint32(blocks * core.BlockSize), Version: MakeVersion(41, 1)}
		var m core.Msg
		if err := Attach(pool, &m, AppendUpdate(nil, &u), data); err != nil {
			t.Fatal(err)
		}
		frame, n := Frame(pool, &m)
		var got Update
		if err := DecodeUpdate(frame, n, &got); err != nil {
			t.Fatal(err)
		}
		if got != u || m.Buf != data {
			t.Fatalf("%d blocks: decoded %+v with buffer %v, want %+v with %v", blocks, got, m.Buf, u, data)
		}
		for i := range blocks {
			if v := binary.LittleEndian.Uint32(pool.Bytes(m.Buf)[i*core.BlockSize:]); v != uint32(i) {
				t.Fatalf("%d blocks: block %d reads %d at its aligned offset", blocks, i, v)
			}
		}
		pool.Free(data)
	}
}

func TestWireLongFrameTravelsInABuffer(t *testing.T) {
	pool := sim.NewPool(1<<20, 0)
	report := func(activations, latencies, disks int) *Report {
		r := &Report{ID: 1, Activations: make([]PGFields, activations), Latencies: make([]Latency, latencies), Disks: make([]DiskState, disks)}
		if activations == 0 {
			r.Activations, r.Latencies = nil, nil
		}
		return r
	}
	for _, c := range []struct {
		r      *Report
		inline bool
	}{{report(2, 12, 2), true}, {report(2, 10, 3), false}, {report(0, 0, 200), false}} {
		for i := range c.r.Activations {
			c.r.Activations[i] = wirePG
		}
		frame := AppendReport(nil, c.r)
		var m core.Msg
		if err := Attach(pool, &m, frame, core.NoBuf); err != nil {
			t.Fatal(err)
		}
		if inline := m.Buf == core.NoBuf; inline != c.inline || inline && int(m.Len) != len(frame) {
			t.Fatalf("%d-byte frame: inline %d bytes, buffer %v", len(frame), m.Len, m.Buf)
		}
		got, n := Frame(pool, &m)
		var r Report
		if err := DecodeReport(got, n, &r); err != nil {
			t.Fatalf("%d-byte frame: %v", len(frame), err)
		}
		if !reflect.DeepEqual(&r, c.r) {
			t.Fatalf("%d-byte frame decoded %+v", len(frame), r)
		}
		if !c.inline {
			b := pool.Bytes(m.Buf)
			b[len(b)-1] = 1
			mustMalformed(t, "a non-zero pad byte", b, 0)
			mustMalformed(t, "an extra block of padding", append(padded(frame), make([]byte, core.BlockSize)...), 0)
			pool.Free(m.Buf)
		}
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("a long frame with a data buffer did not panic")
			}
		}()
		_ = Attach(pool, &core.Msg{}, make([]byte, core.MaxInlineMsg+1), core.IOBuf{Slot: 1, Gen: 1})
	}()
	mustMalformed(t, "a short frame padded to a block", padded(append(AppendHello(nil, &Hello{}), make([]byte, 300)...)), 0)
}

// Each kind that carries data has a reply kind that carries a refusal, which is what lets a driver release the send buffer.
func TestWireDataRequestsHaveRefusals(t *testing.T) {
	for req, reply := range map[Kind]Kind{KindWrite: KindReply, KindReplicaWrite: KindAck} {
		for _, st := range []Status{StatusRefused, StatusNoSpace} {
			r := &Response{Kind: reply, ID: 5, Status: st, Epoch: 80, Interval: 42}
			got, err := decodeMsg(encodeMsg(r), 0)
			if err != nil || !reflect.DeepEqual(got, r) {
				t.Fatalf("reply to kind %d, status %d: %+v, %v", req, st, got, err)
			}
		}
	}
}

// FuzzDecodeMessage feeds arbitrary frames and data lengths: decoding never panics, and a frame that
// decodes encodes back to the same bytes.
func FuzzDecodeMessage(f *testing.F) {
	for _, c := range wireCases() {
		f.Add(encodeMsg(c.msg), c.data)
	}
	f.Add(padded(encodeMsg(&Report{Disks: make([]DiskState, 20)})), 0)
	f.Fuzz(func(t *testing.T, b []byte, data int) {
		msg, err := decodeMsg(b, data)
		if err != nil {
			return
		}
		if got := padded(encodeMsg(msg)); !bytes.Equal(got, b) {
			t.Fatalf("frame %x decodes to %+v, which encodes to %x", b, msg, got)
		}
	})
}

type wireHot struct {
	m    core.Msg
	upd  Update
	read Read
	resp Response
}

// hotPaths encode a message onto a reused Msg and decode it back, for each message a write or read sends.
func (h *wireHot) hotPaths() map[string]func() error {
	upd := Update{Kind: KindReplicaWrite, ID: 5, PG: wirePG, Session: 9, Incarnation: 1, Chunk: wireChunk, Len: core.BlockSize, Version: MakeVersion(41, 7), HasExpect: true}
	read := Read{ID: 6, PG: wirePG, Chunk: wireChunk, Len: core.BlockSize}
	resp := Response{Kind: KindReadReply, ID: 6, Version: MakeVersion(41, 7)}
	return map[string]func() error{
		"update": func() error {
			h.m.Len = uint16(len(AppendUpdate(h.m.Body[:0], &upd)))
			return DecodeUpdate(h.m.Inline(), core.BlockSize, &h.upd)
		},
		"read": func() error {
			h.m.Len = uint16(len(AppendRead(h.m.Body[:0], &read)))
			return DecodeRead(h.m.Inline(), 0, &h.read)
		},
		"response": func() error {
			h.m.Len = uint16(len(AppendResponse(h.m.Body[:0], &resp)))
			return DecodeResponse(h.m.Inline(), core.BlockSize, &h.resp)
		},
	}
}

func TestWireAllocs(t *testing.T) {
	var h wireHot
	for name, fn := range h.hotPaths() {
		if a := testing.AllocsPerRun(1000, func() {
			if err := fn(); err != nil {
				t.Fatal(err)
			}
		}); a != 0 {
			t.Errorf("%s: %v allocations per encode and decode", name, a)
		}
	}
}

func BenchmarkWire(b *testing.B) {
	var h wireHot
	for name, fn := range h.hotPaths() {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if err := fn(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
