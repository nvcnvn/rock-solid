package chunkstore

import (
	"encoding/binary"
	"errors"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// WireVersion is the first byte of every message.
const WireVersion = 1

var (
	// ErrMalformed reports a message that does not decode.
	ErrMalformed = errors.New("chunkstore: malformed message")
	// ErrWireVersion reports a message of another wire version.
	ErrWireVersion = errors.New("chunkstore: message of another wire version")
)

// Kind tags a message; zero is never a kind.
type Kind uint8

// Message kinds.
const (
	KindHello Kind = iota + 1
	KindWrite
	KindReplicaWrite
	KindPunch
	KindReplicaPunch
	KindAck
	KindReply
	KindRead
	KindReadReply
	KindGrant
	KindHeartbeat
	KindReport
	KindMemberTimeout
	KindProbe
	KindProbeReply
	KindPeer
	KindPeerReply
	kindEnd
)

// Status is how a Response answers.
type Status uint8

// Reply statuses.
const (
	StatusOK Status = iota
	StatusNoSpace
	StatusRefused
	StatusMismatch
	StatusMissing
	StatusUnfound
	statusEnd
)

const headSize = 10

var le = binary.LittleEndian

// MakeVersion packs interval k and counter n into the version k.n.
func MakeVersion(interval, counter uint32) uint64 { return uint64(interval)<<32 | uint64(counter) }

func validVersion(v uint64) bool { return (v>>32 == 0) == (uint32(v) == 0) }

func validRange(off, n uint32) bool { return n > 0 && uint64(off)+uint64(n) <= core.ChunkSize }

func dataBytes(off, n uint32) int {
	first, last := spanOf(off, n)
	return (int(last) - int(first) + 1) * core.BlockSize
}

// PGFields name a PG as its sender's table has it: the table's epoch and the PG's interval in it.
type PGFields struct {
	Epoch    uint64
	PG       core.PGID
	Interval uint32
}

// Hello opens a client's connection with its session and incarnation.
type Hello struct {
	ID          core.RequestID
	Session     uint64
	Incarnation uint64
}

// Update is a chunk write or punch, from a client to the primary or, with the version it ordered, from the primary to a member.
type Update struct {
	Kind        Kind
	ID          core.RequestID
	PG          PGFields
	Session     uint64
	Incarnation uint64
	Chunk       core.ChunkID
	Off         uint32
	Len         uint32
	Snapshot    uint64
	Version     uint64
	HasExpect   bool
	Expect      uint64
}

// Read asks for a chunk's committed bytes, or for version At when HasAt.
type Read struct {
	ID    core.RequestID
	PG    PGFields
	Chunk core.ChunkID
	Off   uint32
	Len   uint32
	HasAt bool
	At    uint64
}

// Response answers an Update or a Read; a refusal names the refuser's table epoch and the PG's interval in it.
type Response struct {
	Kind     Kind
	ID       core.RequestID
	Status   Status
	Version  uint64
	Epoch    uint64
	Interval uint32
	Fill     uint64
}

// Grant is a node-pair lease under a support epoch, with the grantor's CLOCK_MONOTONIC_RAW reading and boot ID.
type Grant struct {
	ID      core.RequestID
	Support uint64
	Raw     core.Tick
	Boot    [16]byte
}

// Heartbeat between a node pair carries the sender's table epoch, the support epoch it holds, its CLOCK_MONOTONIC_RAW reading and boot ID.
type Heartbeat struct {
	ID      core.RequestID
	Epoch   uint64
	Support uint64
	Raw     core.Tick
	Boot    [16]byte
}

// Latency is a member disk's acknowledgment latency as its primary saw it.
type Latency struct {
	Disk    uint32
	Latency core.Tick
}

// DiskState is what a node reports of one of its disks.
type DiskState struct {
	Log    uint64
	Fill   uint64
	Oldest core.Tick
	Failed bool
}

// Report is a node's heartbeat to the authority: the PGs whose primary activated, member latencies and its disks.
type Report struct {
	ID          core.RequestID
	Activations []PGFields
	Latencies   []Latency
	Disks       []DiskState
}

// MemberTimeout reports a member disk that kept a primary waiting past member_timeout.
type MemberTimeout struct {
	ID   core.RequestID
	PG   PGFields
	Disk uint32
}

// Probe asks a disk's owner loop how the disk is.
type Probe struct {
	ID   core.RequestID
	Disk uint32
}

// ProbeReply answers a Probe from memory.
type ProbeReply struct {
	ID     core.RequestID
	Log    uint64
	Oldest core.Tick
	Failed bool
}

// ChunkVersion is a chunk and one of its versions.
type ChunkVersion struct {
	Chunk   core.ChunkID
	Version uint64
}

// Peering asks a member to adopt the interval in PG (KindPeer), and answers with its newest version of each chunk and its missing list (KindPeerReply).
type Peering struct {
	Kind    Kind
	ID      core.RequestID
	PG      PGFields
	Newest  []ChunkVersion
	Missing []ChunkVersion
}

// Attach puts a frame and its data buffer on m: inline up to MaxInlineMsg bytes, and a longer frame, which has no data, alone in a pool buffer zero-padded to whole blocks.
func Attach(pool core.BufPool, m *core.Msg, frame []byte, data core.IOBuf) error {
	if len(frame) <= core.MaxInlineMsg {
		m.Len, m.Buf = uint16(copy(m.Body[:], frame)), data
		return nil
	}
	buf, err := pool.AllocZeroed(len(frame))
	if err != nil {
		return err
	}
	copy(pool.Bytes(buf), frame)
	m.Len, m.Buf = 0, buf
	return nil
}

// Frame returns the frame m carries and the length of its data buffer.
func Frame(pool core.BufPool, m *core.Msg) (frame []byte, data int) {
	switch {
	case m.Buf == core.NoBuf:
		return m.Inline(), 0
	case m.Len == 0:
		return pool.Bytes(m.Buf), 0
	}
	return m.Inline(), len(pool.Bytes(m.Buf))
}

// Peek returns a frame's kind.
func Peek(b []byte) (Kind, error) {
	if len(b) == 0 {
		return 0, ErrMalformed
	}
	if b[0] != WireVersion {
		return 0, ErrWireVersion
	}
	if len(b) < headSize || b[1] == 0 || Kind(b[1]) >= kindEnd {
		return 0, ErrMalformed
	}
	return Kind(b[1]), nil
}

func appendHead(dst []byte, k Kind, id core.RequestID) []byte {
	return le.AppendUint64(append(dst, WireVersion, byte(k)), uint64(id))
}

func appendPG(dst []byte, p *PGFields) []byte {
	dst = le.AppendUint64(dst, p.Epoch)
	dst = le.AppendUint32(dst, uint32(p.PG.Pool))
	dst = le.AppendUint32(dst, p.PG.Index)
	return le.AppendUint32(dst, p.Interval)
}

func appendBool(dst []byte, v bool) []byte {
	if v {
		return append(dst, 1)
	}
	return append(dst, 0)
}

// AppendHello appends h's frame.
func AppendHello(dst []byte, h *Hello) []byte {
	dst = appendHead(dst, KindHello, h.ID)
	return le.AppendUint64(le.AppendUint64(dst, h.Session), h.Incarnation)
}

// DecodeHello decodes a hello frame.
func DecodeHello(b []byte, data int, h *Hello) error {
	r, err := open(b, data, KindHello)
	h.ID = r.id
	h.Session, h.Incarnation = r.u64(), r.u64()
	return r.done(err)
}

// AppendUpdate appends u's frame; a write's data travels as the message's buffer.
func AppendUpdate(dst []byte, u *Update) []byte {
	dst = appendPG(appendHead(dst, u.Kind, u.ID), &u.PG)
	dst = le.AppendUint64(le.AppendUint64(dst, u.Session), u.Incarnation)
	dst = u.Chunk.AppendTo(dst)
	dst = le.AppendUint32(le.AppendUint32(dst, u.Off), u.Len)
	dst = le.AppendUint64(le.AppendUint64(dst, u.Snapshot), u.Version)
	return le.AppendUint64(appendBool(dst, u.HasExpect), u.Expect)
}

// DecodeUpdate decodes a write or punch frame whose data buffer holds data bytes: a write's are the blocks its range touches.
func DecodeUpdate(b []byte, data int, u *Update) error {
	r, err := open(b, 0, KindWrite, KindReplicaWrite, KindPunch, KindReplicaPunch)
	u.Kind, u.ID = r.kind, r.id
	u.PG = r.pg()
	u.Session, u.Incarnation = r.u64(), r.u64()
	u.Chunk = r.chunk()
	u.Off, u.Len = r.u32(), r.u32()
	u.Snapshot, u.Version = r.u64(), r.u64()
	u.HasExpect, u.Expect = r.optional()
	replica := u.Kind == KindReplicaWrite || u.Kind == KindReplicaPunch
	want := 0
	if u.Kind == KindWrite || u.Kind == KindReplicaWrite {
		want = dataBytes(u.Off, u.Len)
	}
	if !validRange(u.Off, u.Len) || data != want || !validVersion(u.Version) || replica != (u.Version != 0) {
		r.bad = true
	}
	return r.done(err)
}

// AppendRead appends rd's frame.
func AppendRead(dst []byte, rd *Read) []byte {
	dst = appendPG(appendHead(dst, KindRead, rd.ID), &rd.PG)
	dst = rd.Chunk.AppendTo(dst)
	dst = le.AppendUint32(le.AppendUint32(dst, rd.Off), rd.Len)
	return le.AppendUint64(appendBool(dst, rd.HasAt), rd.At)
}

// DecodeRead decodes a read frame.
func DecodeRead(b []byte, data int, rd *Read) error {
	r, err := open(b, data, KindRead)
	rd.ID = r.id
	rd.PG = r.pg()
	rd.Chunk = r.chunk()
	rd.Off, rd.Len = r.u32(), r.u32()
	rd.HasAt, rd.At = r.optional()
	if !validRange(rd.Off, rd.Len) {
		r.bad = true
	}
	return r.done(err)
}

// AppendResponse appends rp's frame; a read reply's blocks travel as the message's buffer, laid out as a write's.
func AppendResponse(dst []byte, rp *Response) []byte {
	dst = append(appendHead(dst, rp.Kind, rp.ID), byte(rp.Status))
	dst = le.AppendUint64(le.AppendUint64(dst, rp.Version), rp.Epoch)
	return le.AppendUint64(le.AppendUint32(dst, rp.Interval), rp.Fill)
}

// DecodeResponse decodes an acknowledgment, reply or read reply frame whose data buffer holds data bytes.
func DecodeResponse(b []byte, data int, rp *Response) error {
	r, err := open(b, 0, KindAck, KindReply, KindReadReply)
	rp.Kind, rp.ID = r.kind, r.id
	rp.Status = Status(r.u8())
	rp.Version, rp.Epoch = r.u64(), r.u64()
	rp.Interval, rp.Fill = r.u32(), r.u64()
	dataOK := data == 0
	if rp.Kind == KindReadReply && rp.Status == StatusOK {
		dataOK = data > 0 && data%core.BlockSize == 0
	}
	if rp.Status >= statusEnd || !validVersion(rp.Version) || rp.Status == StatusRefused && rp.Interval == 0 || !dataOK {
		r.bad = true
	}
	return r.done(err)
}

// AppendGrant appends g's frame.
func AppendGrant(dst []byte, g *Grant) []byte {
	dst = appendHead(dst, KindGrant, g.ID)
	dst = le.AppendUint64(le.AppendUint64(dst, g.Support), uint64(g.Raw))
	return append(dst, g.Boot[:]...)
}

// DecodeGrant decodes a grant frame.
func DecodeGrant(b []byte, data int, g *Grant) error {
	r, err := open(b, data, KindGrant)
	g.ID = r.id
	g.Support, g.Raw = r.u64(), core.Tick(r.u64())
	g.Boot = [16]byte(r.take(16))
	return r.done(err)
}

// AppendHeartbeat appends h's frame.
func AppendHeartbeat(dst []byte, h *Heartbeat) []byte {
	dst = appendHead(dst, KindHeartbeat, h.ID)
	dst = le.AppendUint64(le.AppendUint64(dst, h.Epoch), h.Support)
	return append(le.AppendUint64(dst, uint64(h.Raw)), h.Boot[:]...)
}

// DecodeHeartbeat decodes a node-pair heartbeat frame.
func DecodeHeartbeat(b []byte, data int, h *Heartbeat) error {
	r, err := open(b, data, KindHeartbeat)
	h.ID = r.id
	h.Epoch, h.Support, h.Raw = r.u64(), r.u64(), core.Tick(r.u64())
	h.Boot = [16]byte(r.take(16))
	return r.done(err)
}

const (
	pgFieldsSize  = 20
	latencySize   = 12
	diskStateSize = 25
	chunkVerSize  = core.ChunkIDSize + 8
)

// AppendReport appends rp's frame.
func AppendReport(dst []byte, rp *Report) []byte {
	dst = le.AppendUint32(appendHead(dst, KindReport, rp.ID), uint32(len(rp.Activations)))
	for i := range rp.Activations {
		dst = appendPG(dst, &rp.Activations[i])
	}
	dst = le.AppendUint32(dst, uint32(len(rp.Latencies)))
	for _, l := range rp.Latencies {
		dst = le.AppendUint64(le.AppendUint32(dst, l.Disk), uint64(l.Latency))
	}
	dst = le.AppendUint32(dst, uint32(len(rp.Disks)))
	for _, d := range rp.Disks {
		dst = le.AppendUint64(le.AppendUint64(le.AppendUint64(dst, d.Log), d.Fill), uint64(d.Oldest))
		dst = appendBool(dst, d.Failed)
	}
	return dst
}

// DecodeReport decodes a report frame into rp, reusing its slices.
func DecodeReport(b []byte, data int, rp *Report) error {
	r, err := open(b, data, KindReport)
	rp.ID = r.id
	rp.Activations = rp.Activations[:0]
	for n := r.count(pgFieldsSize); n > 0; n-- {
		rp.Activations = append(rp.Activations, r.pg())
	}
	rp.Latencies = rp.Latencies[:0]
	for n := r.count(latencySize); n > 0; n-- {
		rp.Latencies = append(rp.Latencies, Latency{Disk: r.u32(), Latency: core.Tick(r.u64())})
	}
	rp.Disks = rp.Disks[:0]
	for n := r.count(diskStateSize); n > 0; n-- {
		rp.Disks = append(rp.Disks, DiskState{Log: r.u64(), Fill: r.u64(), Oldest: core.Tick(r.u64()), Failed: r.bool()})
	}
	return r.done(err)
}

// AppendMemberTimeout appends mt's frame.
func AppendMemberTimeout(dst []byte, mt *MemberTimeout) []byte {
	return le.AppendUint32(appendPG(appendHead(dst, KindMemberTimeout, mt.ID), &mt.PG), mt.Disk)
}

// DecodeMemberTimeout decodes a member_timeout report frame.
func DecodeMemberTimeout(b []byte, data int, mt *MemberTimeout) error {
	r, err := open(b, data, KindMemberTimeout)
	mt.ID = r.id
	mt.PG, mt.Disk = r.pg(), r.u32()
	return r.done(err)
}

// AppendProbe appends p's frame.
func AppendProbe(dst []byte, p *Probe) []byte {
	return le.AppendUint32(appendHead(dst, KindProbe, p.ID), p.Disk)
}

// DecodeProbe decodes a probe frame.
func DecodeProbe(b []byte, data int, p *Probe) error {
	r, err := open(b, data, KindProbe)
	p.ID, p.Disk = r.id, r.u32()
	return r.done(err)
}

// AppendProbeReply appends p's frame.
func AppendProbeReply(dst []byte, p *ProbeReply) []byte {
	dst = appendHead(dst, KindProbeReply, p.ID)
	return appendBool(le.AppendUint64(le.AppendUint64(dst, p.Log), uint64(p.Oldest)), p.Failed)
}

// DecodeProbeReply decodes a probe reply frame.
func DecodeProbeReply(b []byte, data int, p *ProbeReply) error {
	r, err := open(b, data, KindProbeReply)
	p.ID = r.id
	p.Log, p.Oldest, p.Failed = r.u64(), core.Tick(r.u64()), r.bool()
	return r.done(err)
}

// AppendPeering appends p's frame.
func AppendPeering(dst []byte, p *Peering) []byte {
	dst = appendPG(appendHead(dst, p.Kind, p.ID), &p.PG)
	return appendChunkVersions(appendChunkVersions(dst, p.Newest), p.Missing)
}

func appendChunkVersions(dst []byte, cvs []ChunkVersion) []byte {
	dst = le.AppendUint32(dst, uint32(len(cvs)))
	for _, cv := range cvs {
		dst = le.AppendUint64(cv.Chunk.AppendTo(dst), cv.Version)
	}
	return dst
}

// DecodePeering decodes a peering frame into p, reusing its slices; a request carries no lists.
func DecodePeering(b []byte, data int, p *Peering) error {
	r, err := open(b, data, KindPeer, KindPeerReply)
	p.Kind, p.ID = r.kind, r.id
	p.PG = r.pg()
	p.Newest = r.chunkVersions(p.Newest[:0])
	p.Missing = r.chunkVersions(p.Missing[:0])
	if p.Kind == KindPeer && len(p.Newest)+len(p.Missing) > 0 {
		r.bad = true
	}
	return r.done(err)
}

func (r *reader) chunkVersions(cvs []ChunkVersion) []ChunkVersion {
	for n := r.count(chunkVerSize); n > 0; n-- {
		cv := ChunkVersion{Chunk: r.chunk(), Version: r.u64()}
		if !validVersion(cv.Version) {
			r.bad = true
		}
		cvs = append(cvs, cv)
	}
	return cvs
}

type reader struct {
	b    []byte
	n    int
	kind Kind
	id   core.RequestID
	bad  bool
}

var zeroField [core.ChunkIDSize]byte

// open checks a frame's head against the kinds the caller decodes; data is the length of a data buffer that none of them may have.
func open(b []byte, data int, kinds ...Kind) (reader, error) {
	r := reader{b: b, n: len(b)}
	k, err := Peek(b)
	if err != nil {
		r.bad = true
		return r, err
	}
	r.take(2)
	r.kind, r.id = k, core.RequestID(r.u64())
	r.bad = data != 0
	for _, want := range kinds {
		if k == want {
			return r, nil
		}
	}
	r.bad = true
	return r, nil
}

func (r *reader) take(n int) []byte {
	if r.bad || len(r.b) < n {
		r.bad = true
		return zeroField[:n]
	}
	p := r.b[:n]
	r.b = r.b[n:]
	return p
}

func (r *reader) u8() uint8   { return r.take(1)[0] }
func (r *reader) u32() uint32 { return le.Uint32(r.take(4)) }
func (r *reader) u64() uint64 { return le.Uint64(r.take(8)) }

func (r *reader) bool() bool {
	v := r.u8()
	if v > 1 {
		r.bad = true
	}
	return v == 1
}

func (r *reader) chunk() core.ChunkID {
	return core.ChunkIDFromBytes([core.ChunkIDSize]byte(r.take(core.ChunkIDSize)))
}

func (r *reader) pg() PGFields {
	p := PGFields{Epoch: r.u64(), PG: core.PGID{Pool: core.PoolID(r.u32()), Index: r.u32()}, Interval: r.u32()}
	if p.Interval == 0 {
		r.bad = true
	}
	return p
}

// optional reads a flag byte and the version it makes present; an absent version must be zero.
func (r *reader) optional() (bool, uint64) {
	has, v := r.bool(), r.u64()
	if !validVersion(v) || !has && v != 0 {
		r.bad = true
	}
	return has, v
}

// count reads a list's length and checks that the frame holds that many entries of size bytes.
func (r *reader) count(size int) int {
	n := r.u32()
	if r.bad || uint64(n)*uint64(size) > uint64(len(r.b)) {
		r.bad = true
		return 0
	}
	return int(n)
}

// done ends a decode: an inline frame must end here, and a longer one, which travels in a pool buffer, be zero to the end of its last block.
func (r *reader) done(err error) error {
	if err != nil {
		return err
	}
	used := r.n - len(r.b)
	end := used
	if used > core.MaxInlineMsg {
		end = (used + core.BlockSize - 1) / core.BlockSize * core.BlockSize
	}
	if r.bad || r.n != end || !allZero(r.b) {
		return ErrMalformed
	}
	return nil
}

func allZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}
