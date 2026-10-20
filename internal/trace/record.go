// Package trace holds the fixed-field records of events, actions, syscalls and outputs, their text lines and the rules that check them.
package trace

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// Kind is what a record describes.
type Kind uint8

// Record kinds.
const (
	KindEvent Kind = iota + 1
	KindAction
	KindSyscall
	KindOutput
)

var kindText = [...]string{KindEvent: "event", KindAction: "action", KindSyscall: "syscall", KindOutput: "output"}

// String is the kind as a trace line writes it.
func (k Kind) String() string {
	if k == 0 || int(k) >= len(kindText) {
		return "kind(" + strconv.Itoa(int(k)) + ")"
	}
	return kindText[k]
}

// Name is the event, action, syscall or output a record holds; it fixes the record's kind and fields.
type Name uint8

// Record names, their fields in order.
const (
	// EvNetRecv: peer msg.
	EvNetRecv Name = iota + 1
	// EvPeerLost: peer.
	EvPeerLost
	// EvDiskDone: io err.
	EvDiskDone
	// EvTimer: timer.
	EvTimer
	// EvResumed has no fields.
	EvResumed
	// EvCrash, the simulator crashing the loop's node, has no fields.
	EvCrash
	// EvRestart, the simulator restarting the loop's node, has no fields.
	EvRestart
	// EvDrop, the simulator dropping a message the loop sent: peer msg.
	EvDrop
	// ActNetSend: peer msg.
	ActNetSend
	// ActDiskRead: io lba len.
	ActDiskRead
	// ActDiskWrite: round io lba len vec; round 0 is a write outside any round.
	ActDiskWrite
	// ActDiskFsync: round io linked; round 0 is a flush outside any round.
	ActDiskFsync
	// ActDiskTrim: io lba len.
	ActDiskTrim
	// ActSetTimer: timer at.
	ActSetTimer
	// ActEntry is one entry of a round's log record: round type vol inode index.
	ActEntry
	// SysEpollWait: ready.
	SysEpollWait
	// SysRead: fd bytes.
	SysRead
	// SysWritev: fd iov bytes.
	SysWritev
	// SysIOUringEnter: submit wait.
	SysIOUringEnter
	// SysTimerfdSettime: at.
	SysTimerfdSettime
	// SysEventfdWrite: fd.
	SysEventfdWrite
	// OutDurable: round lsn.
	OutDurable
	// OutFailed: err.
	OutFailed
	// OutCheckpointDone: lsn.
	OutCheckpointDone
	numNames
)

// MaxFields is the most fields a record holds.
const MaxFields = 5

type fieldType uint8

const (
	typeNum fieldType = iota
	typeBool
	typePeer
	typeMsg
	typeResult
	typeEntry
)

type field struct {
	key string
	typ fieldType
}

type schema struct {
	text   string
	kind   Kind
	fields []field
}

var (
	fRound  = field{"round", typeNum}
	fIO     = field{"io", typeNum}
	fLBA    = field{"lba", typeNum}
	fLen    = field{"len", typeNum}
	fPeer   = field{"peer", typePeer}
	fMsg    = field{"msg", typeMsg}
	fTimer  = field{"timer", typeNum}
	fAt     = field{"at", typeNum}
	fFD     = field{"fd", typeNum}
	fBytes  = field{"bytes", typeNum}
	fLSN    = field{"lsn", typeNum}
	fResult = field{"err", typeResult}
)

var schemas = [numNames]schema{
	EvNetRecv:         {"NetRecv", KindEvent, []field{fPeer, fMsg}},
	EvPeerLost:        {"PeerLost", KindEvent, []field{fPeer}},
	EvDiskDone:        {"DiskDone", KindEvent, []field{fIO, fResult}},
	EvTimer:           {"Timer", KindEvent, []field{fTimer}},
	EvResumed:         {"Resumed", KindEvent, nil},
	EvCrash:           {"Crash", KindEvent, nil},
	EvRestart:         {"Restart", KindEvent, nil},
	EvDrop:            {"Drop", KindEvent, []field{fPeer, fMsg}},
	ActNetSend:        {"NetSend", KindAction, []field{fPeer, fMsg}},
	ActDiskRead:       {"DiskRead", KindAction, []field{fIO, fLBA, fLen}},
	ActDiskWrite:      {"DiskWrite", KindAction, []field{fRound, fIO, fLBA, fLen, {"vec", typeNum}}},
	ActDiskFsync:      {"DiskFsync", KindAction, []field{fRound, fIO, {"linked", typeBool}}},
	ActDiskTrim:       {"DiskTrim", KindAction, []field{fIO, fLBA, fLen}},
	ActSetTimer:       {"SetTimer", KindAction, []field{fTimer, fAt}},
	ActEntry:          {"Entry", KindAction, []field{fRound, {"type", typeEntry}, {"vol", typeNum}, {"inode", typeNum}, {"index", typeNum}}},
	SysEpollWait:      {"epoll_wait", KindSyscall, []field{{"ready", typeNum}}},
	SysRead:           {"read", KindSyscall, []field{fFD, fBytes}},
	SysWritev:         {"writev", KindSyscall, []field{fFD, {"iov", typeNum}, fBytes}},
	SysIOUringEnter:   {"io_uring_enter", KindSyscall, []field{{"submit", typeNum}, {"wait", typeNum}}},
	SysTimerfdSettime: {"timerfd_settime", KindSyscall, []field{fAt}},
	SysEventfdWrite:   {"eventfd_write", KindSyscall, []field{fFD}},
	OutDurable:        {"Durable", KindOutput, []field{fRound, fLSN}},
	OutFailed:         {"Failed", KindOutput, []field{fResult}},
	OutCheckpointDone: {"CheckpointDone", KindOutput, []field{fLSN}},
}

// Kind is the kind of record the name belongs to, or 0 for an unknown name.
func (n Name) Kind() Kind {
	if n == 0 || n >= numNames {
		return 0
	}
	return schemas[n].kind
}

// String is the name as a trace line writes it.
func (n Name) String() string {
	if n == 0 || n >= numNames {
		return "name(" + strconv.Itoa(int(n)) + ")"
	}
	return schemas[n].text
}

// Fields is the number of fields a record of this name holds.
func (n Name) Fields() int {
	if n == 0 || n >= numNames {
		return 0
	}
	return len(schemas[n].fields)
}

// Msg is the type of a network message, a msg field's value.
type Msg uint64

// Message types.
const (
	MsgWrite Msg = iota
	MsgWriteReply
	MsgRead
	MsgReadReply
	MsgPeering
	MsgPeeringReply
	MsgCut
)

var msgText = []string{"write", "write_reply", "read", "read_reply", "peering", "peering_reply", "cut"}

// Result is the outcome an err field holds; its first three values are core.DiskError's.
type Result uint64

// Results.
const (
	ResultOK      = Result(core.DiskOK)
	ResultIO      = Result(core.DiskIO)
	ResultRange   = Result(core.DiskInvalidRange)
	ResultNoSpace = ResultRange + 1
)

var resultText = []string{"ok", "io", "range", "nospace"}

// EntryType is the type of a log record entry, an Entry's type field.
type EntryType uint64

// Entry types.
const (
	EntryFreeze EntryType = iota
	EntryWrite
	EntryPunch
	EntryCopy
)

var entryText = []string{"freeze", "write", "punch", "copy"}

// Peer packs a peer loop into a peer field.
func Peer(id core.NodeID) uint64 { return uint64(id.Node)<<32 | uint64(id.Loop) }

// PeerID unpacks a peer field.
func PeerID(v uint64) core.NodeID { return core.NodeID{Node: uint32(v >> 32), Loop: uint32(v)} }

// Record is one line of a trace: when, which loop and pass, and a name with its fields in schema order.
type Record struct {
	Tick   core.Tick
	Pass   uint64
	Fields [MaxFields]uint64
	Node   uint32
	Loop   uint16
	Name   Name
	Sim    bool
}

// Kind is the kind of the record's name.
func (r *Record) Kind() Kind { return r.Name.Kind() }

// Append appends the record's text line, without a newline: `tick loop kind pass=N name key=value...`.
func (r *Record) Append(b []byte) []byte {
	b = strconv.AppendUint(b, uint64(r.Tick), 10)
	b = append(b, ' ')
	if r.Sim {
		b = strconv.AppendUint(b, uint64(r.Node), 10)
		b = append(b, '.')
	}
	b = strconv.AppendUint(b, uint64(r.Loop), 10)
	b = append(b, ' ')
	b = append(b, r.Name.Kind().String()...)
	b = append(b, " pass="...)
	b = strconv.AppendUint(b, r.Pass, 10)
	b = append(b, ' ')
	b = append(b, r.Name.String()...)
	if r.Name == 0 || r.Name >= numNames {
		return b
	}
	for i, f := range schemas[r.Name].fields {
		b = append(b, ' ')
		b = append(b, f.key...)
		b = append(b, '=')
		b = appendValue(b, f.typ, r.Fields[i])
	}
	return b
}

func appendValue(b []byte, t fieldType, v uint64) []byte {
	switch t {
	case typePeer:
		b = strconv.AppendUint(b, v>>32, 10)
		if loop := uint32(v); loop != 0 {
			b = append(b, '.')
			b = strconv.AppendUint(b, uint64(loop), 10)
		}
		return b
	case typeMsg:
		return appendEnum(b, msgText, v)
	case typeResult:
		return appendEnum(b, resultText, v)
	case typeEntry:
		return appendEnum(b, entryText, v)
	}
	return strconv.AppendUint(b, v, 10)
}

func appendEnum(b []byte, names []string, v uint64) []byte {
	if v < uint64(len(names)) {
		return append(b, names[v]...)
	}
	return strconv.AppendUint(b, v, 10)
}

// ErrSyntax reports a trace line that is not a record's text form.
var ErrSyntax = errors.New("malformed trace line")

// Parse reads one line, without its newline, into a record, accepting only the form Append writes.
func Parse(line []byte) (Record, error) {
	var r Record
	p := lineParser{rest: line}
	tick, err := p.number("tick", 64)
	if err != nil {
		return Record{}, err
	}
	r.Tick = core.Tick(tick)
	if err := p.loop(&r); err != nil {
		return Record{}, err
	}
	tok, err := p.token("kind")
	if err != nil {
		return Record{}, err
	}
	k := lookup(kindText[:], tok)
	if k < 0 {
		return Record{}, fmt.Errorf("%w: unknown kind %q", ErrSyntax, tok)
	}
	kind := Kind(k)
	pass, err := p.keyed("pass")
	if err != nil {
		return Record{}, err
	}
	if r.Pass, err = parseNumber(pass, 64); err != nil {
		return Record{}, fmt.Errorf("pass: %w", err)
	}
	tok, err = p.token("name")
	if err != nil {
		return Record{}, err
	}
	r.Name = lookupName(tok)
	if r.Name == 0 {
		return Record{}, fmt.Errorf("%w: unknown name %q", ErrSyntax, tok)
	}
	if r.Name.Kind() != kind {
		return Record{}, fmt.Errorf("%w: %s is not a %s", ErrSyntax, r.Name, kind)
	}
	fields := schemas[r.Name].fields
	for i, f := range fields {
		if p.done() {
			return Record{}, fmt.Errorf("%w: %s is missing field %s", ErrSyntax, r.Name, f.key)
		}
		tok, err := p.token(f.key)
		if err != nil {
			return Record{}, err
		}
		key, val, ok := cut(tok, '=')
		if !ok {
			return Record{}, fmt.Errorf("%w: field %q has no value", ErrSyntax, tok)
		}
		if string(key) != f.key {
			if fieldIndex(fields, key) >= 0 {
				return Record{}, fmt.Errorf("%w: %s field %s out of order, want %s", ErrSyntax, r.Name, key, f.key)
			}
			return Record{}, fmt.Errorf("%w: %s has no field %s", ErrSyntax, r.Name, key)
		}
		if r.Fields[i], err = parseValue(f.typ, val); err != nil {
			return Record{}, fmt.Errorf("%s: %w", f.key, err)
		}
	}
	if !p.done() {
		return Record{}, fmt.Errorf("%w: extra field after %s's fields", ErrSyntax, r.Name)
	}
	return r, nil
}

type lineParser struct {
	rest []byte
}

func (p *lineParser) done() bool { return p.rest == nil }

func (p *lineParser) token(what string) ([]byte, error) {
	if p.rest == nil {
		return nil, fmt.Errorf("%w: missing %s", ErrSyntax, what)
	}
	tok, rest, more := cut(p.rest, ' ')
	if len(tok) == 0 {
		return nil, fmt.Errorf("%w: empty %s", ErrSyntax, what)
	}
	if more {
		p.rest = rest
	} else {
		p.rest = nil
	}
	return tok, nil
}

func (p *lineParser) number(what string, bits int) (uint64, error) {
	tok, err := p.token(what)
	if err != nil {
		return 0, err
	}
	v, err := parseNumber(tok, bits)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", what, err)
	}
	return v, nil
}

func (p *lineParser) keyed(key string) ([]byte, error) {
	tok, err := p.token(key)
	if err != nil {
		return nil, err
	}
	k, v, ok := cut(tok, '=')
	if !ok || string(k) != key {
		return nil, fmt.Errorf("%w: want %s=, got %q", ErrSyntax, key, tok)
	}
	return v, nil
}

func (p *lineParser) loop(r *Record) error {
	tok, err := p.token("loop")
	if err != nil {
		return err
	}
	node, loop, sim := cut(tok, '.')
	if sim {
		n, err := parseNumber(node, 32)
		if err != nil {
			return fmt.Errorf("node: %w", err)
		}
		r.Node, r.Sim = uint32(n), true
	} else {
		loop = node
	}
	l, err := parseNumber(loop, 16)
	if err != nil {
		return fmt.Errorf("loop: %w", err)
	}
	r.Loop = uint16(l)
	return nil
}

func parseValue(t fieldType, v []byte) (uint64, error) {
	switch t {
	case typeBool:
		if len(v) == 1 && (v[0] == '0' || v[0] == '1') {
			return uint64(v[0] - '0'), nil
		}
		return 0, fmt.Errorf("%w: %q is not 0 or 1", ErrSyntax, v)
	case typePeer:
		node, loop, dotted := cut(v, '.')
		n, err := parseNumber(node, 32)
		if err != nil {
			return 0, err
		}
		if !dotted {
			return n << 32, nil
		}
		l, err := parseNumber(loop, 32)
		if err != nil {
			return 0, err
		}
		if l == 0 {
			return 0, fmt.Errorf("%w: peer %q writes loop 0", ErrSyntax, v)
		}
		return n<<32 | l, nil
	case typeMsg:
		return parseEnum(msgText, v)
	case typeResult:
		return parseEnum(resultText, v)
	case typeEntry:
		return parseEnum(entryText, v)
	}
	return parseNumber(v, 64)
}

func parseEnum(names []string, v []byte) (uint64, error) {
	if i := lookup(names, v); i >= 0 {
		return uint64(i), nil
	}
	return 0, fmt.Errorf("%w: unknown value %q", ErrSyntax, v)
}

func parseNumber(s []byte, bits int) (uint64, error) {
	if len(s) == 0 || len(s) > 1 && s[0] == '0' {
		return 0, fmt.Errorf("%w: %q is not a canonical number", ErrSyntax, s)
	}
	var v uint64
	limit := uint64(1)<<bits - 1
	if bits == 64 {
		limit = ^uint64(0)
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("%w: %q is not a number", ErrSyntax, s)
		}
		d := uint64(c - '0')
		if v > (limit-d)/10 {
			return 0, fmt.Errorf("%w: %q overflows %d bits", ErrSyntax, s, bits)
		}
		v = v*10 + d
	}
	return v, nil
}

func cut(s []byte, sep byte) (before, after []byte, found bool) {
	for i, c := range s {
		if c == sep {
			return s[:i], s[i+1:], true
		}
	}
	return s, nil, false
}

func lookup(names []string, s []byte) int {
	for i, n := range names {
		if n != "" && n == string(s) {
			return i
		}
	}
	return -1
}

func lookupName(s []byte) Name {
	for n := Name(1); n < numNames; n++ {
		if schemas[n].text == string(s) {
			return n
		}
	}
	return 0
}

func fieldIndex(fields []field, key []byte) int {
	for i, f := range fields {
		if f.key == string(key) {
			return i
		}
	}
	return -1
}
