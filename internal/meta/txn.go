package meta

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/nvcnvn/rock-solid/internal/lsm"
)

const (
	// MaxKey is the longest key a Txn writes or checks.
	MaxKey = lsm.MaxKey
	// MaxValue is the longest value a Txn writes or checks.
	MaxValue = lsm.MaxValue
	// MaxBound is the longest bound of a DeleteRange or CheckEmpty.
	MaxBound = lsm.MaxBound
	// MaxTxnBytes is max_txn_bytes, the most a Txn encodes to.
	MaxTxnBytes = 256 << 10
	// MaxScanBytes is max_scan_bytes, the most keys and values a Scan returns.
	MaxScanBytes = 256 << 10
	// MaxReplayUpdates is the most Updates a Txn with a replay key holds, so its record, a byte and 8 per Update, fits MaxValue.
	MaxReplayUpdates = (MaxValue - 1) / 8
	// requestBytes is the request ID that ends a replay key, big-endian, after the prefix its mount's records share.
	requestBytes = 8
	// fieldBytes is the big-endian field an Update changes.
	fieldBytes = 8
)

var (
	// ErrNoOps reports a Txn without ops.
	ErrNoOps = errors.New("meta: txn has no ops")
	// ErrTooLarge reports a key, value, range bound or Txn over its limit.
	ErrTooLarge = errors.New("meta: too large")
	// ErrEmptyRange reports a DeleteRange or CheckEmpty whose start is not below its end.
	ErrEmptyRange = errors.New("meta: empty range")
	// ErrBadOp reports an op of an unknown kind or Update mode.
	ErrBadOp = errors.New("meta: bad op")
	// ErrBadReplay reports a replay key too short to end in a request ID, an oldest outstanding request above its own, or too many Updates for its record.
	ErrBadReplay = errors.New("meta: bad replay key")
	// ErrMalformed reports a log entry whose Txns do not decode; none of them applies.
	ErrMalformed = errors.New("meta: malformed entry")
	// ErrHalted reports a Txn or Scan refused by a replica that halted at an entry it could not decode.
	ErrHalted = errors.New("meta: replica halted at an entry it does not decode")
	// ErrLimit reports a Scan limit below 1.
	ErrLimit = errors.New("meta: scan limit below 1")
)

// OpKind is what an op does.
type OpKind uint8

// The op kinds, the whole set: a new one needs a raised format level, since every release must
// apply an old entry the same way.
const (
	OpPut OpKind = iota + 1
	OpDelete
	OpDeleteRange
	OpCheck
	OpCheckEmpty
	OpUpdate
)

// Mode is how an Update changes its field.
type Mode uint8

// Update modes: Set stores n, Add adds it with wraparound, Bump stores max(n, field + 1), saturating.
const (
	Set Mode = iota + 1
	Add
	Bump
)

// Op is one op of a Txn.
type Op struct {
	Kind OpKind
	// Key is the op's key, or its range's start.
	Key []byte
	// End is a DeleteRange's or CheckEmpty's end.
	End []byte
	// Value is a Put's value, or the value a Check expects.
	Value []byte
	// Absent makes a Check expect the key absent.
	Absent bool
	// Final marks a Check or CheckEmpty whose failure the caller returns as is, so a replay record stores it.
	Final bool
	// Off is the byte offset of an Update's field.
	Off  uint16
	Mode Mode
	N    uint64
}

// Put sets key to value.
func Put(key, value []byte) Op { return Op{Kind: OpPut, Key: key, Value: value} }

// Delete removes key, and succeeds if it is absent.
func Delete(key []byte) Op { return Op{Kind: OpDelete, Key: key} }

// DeleteRange removes every key in [start, end).
func DeleteRange(start, end []byte) Op { return Op{Kind: OpDeleteRange, Key: start, End: end} }

// Check passes if key holds exactly value.
func Check(key, value []byte, final bool) Op {
	return Op{Kind: OpCheck, Key: key, Value: value, Final: final}
}

// CheckAbsent passes if key is absent.
func CheckAbsent(key []byte, final bool) Op {
	return Op{Kind: OpCheck, Key: key, Absent: true, Final: final}
}

// CheckEmpty passes if no key is in [start, end).
func CheckEmpty(start, end []byte, final bool) Op {
	return Op{Kind: OpCheckEmpty, Key: start, End: end, Final: final}
}

// Update changes the 8-byte big-endian field at off of key's value; an absent key or a short value reads as zeros.
func Update(key []byte, off uint16, mode Mode, n uint64) Op {
	return Op{Kind: OpUpdate, Key: key, Off: off, Mode: mode, N: n}
}

// Txn is a shard's transaction: its checks against the state before it, then its writes in list order, all or nothing.
type Txn struct {
	Ops []Op
	// Replay is the replay record's key, ending in the request's 8-byte big-endian ID, or nil for an internal Txn.
	Replay []byte
	// Oldest is the mount's oldest outstanding request; its records below it are dropped.
	Oldest uint64
}

// Result is what a Txn's apply returned.
type Result struct {
	// FailedCheck is the index of the first failed check, or -1.
	FailedCheck int
	// Actual is that check's key's value; nil if absent, a CheckEmpty, or a replayed result.
	Actual []byte
	// Updated is each Update's new field value, in op order.
	Updated []uint64
	buf     []byte
}

// A Txn's encoding: the replay key's length and bytes, Oldest when there is a key, the op count,
// then each op's kind and its fields, lengths and integers as uvarints. Check and CheckEmpty
// carry a flags byte, Update its mode.
const (
	flagAbsent byte = 1 << iota
	flagFinal
)

// Check reports whether t is within the limits a Txn is proposed under, and its encoded size.
func (t *Txn) Check() (int, error) {
	if len(t.Ops) == 0 {
		return 0, ErrNoOps
	}
	n := uvarintLen(uint64(len(t.Replay))) + len(t.Replay) + uvarintLen(uint64(len(t.Ops)))
	if t.Replay == nil && t.Oldest != 0 {
		return 0, ErrBadReplay
	}
	if t.Replay != nil {
		if len(t.Replay) < requestBytes || len(t.Replay) > MaxBound || t.Oldest > binary.BigEndian.Uint64(t.Replay[len(t.Replay)-requestBytes:]) {
			return 0, ErrBadReplay
		}
		n += uvarintLen(t.Oldest)
	}
	updates := 0
	for i := range t.Ops {
		op := &t.Ops[i]
		if op.Kind < OpPut || op.Kind > OpUpdate || !carries(op) {
			return 0, ErrBadOp
		}
		n += 1 + bytesLen(op.Key)
		key := MaxKey
		switch op.Kind {
		case OpPut:
			n += bytesLen(op.Value)
		case OpDelete:
		case OpDeleteRange, OpCheckEmpty:
			key = MaxBound
			n += bytesLen(op.End)
			if len(op.End) > MaxBound {
				return 0, ErrTooLarge
			}
			if bytes.Compare(op.Key, op.End) >= 0 {
				return 0, ErrEmptyRange
			}
			if op.Kind == OpCheckEmpty {
				n++
			}
		case OpCheck:
			n++
			if !op.Absent {
				n += bytesLen(op.Value)
			}
		case OpUpdate:
			if op.Mode < Set || op.Mode > Bump {
				return 0, ErrBadOp
			}
			if int(op.Off)+fieldBytes > MaxValue {
				return 0, ErrTooLarge
			}
			updates++
			n += 1 + uvarintLen(uint64(op.Off)) + uvarintLen(op.N)
		default:
			return 0, ErrBadOp
		}
		if len(op.Key) > key || len(op.Value) > MaxValue {
			return 0, ErrTooLarge
		}
	}
	if t.Replay != nil && updates > MaxReplayUpdates {
		return 0, ErrBadReplay
	}
	if n > MaxTxnBytes {
		return 0, ErrTooLarge
	}
	return n, nil
}

// uses is, per op kind, the fields its encoding carries; a field set outside them would not survive the log entry.
var uses = [...]struct{ end, value, absent, final, update bool }{
	OpPut:         {value: true},
	OpDelete:      {},
	OpDeleteRange: {end: true},
	OpCheck:       {value: true, absent: true, final: true},
	OpCheckEmpty:  {end: true, final: true},
	OpUpdate:      {update: true},
}

func carries(op *Op) bool {
	u := uses[op.Kind]
	return (u.end || op.End == nil) && (u.value || op.Value == nil) && (u.absent || !op.Absent) && (u.final || !op.Final) &&
		(u.update || op.Off == 0 && op.Mode == 0 && op.N == 0) && !(op.Absent && op.Value != nil)
}

// appendTxn appends t's encoding to dst; t must have passed Check.
func appendTxn(dst []byte, t *Txn) []byte {
	dst = appendBytes(dst, t.Replay)
	if t.Replay != nil {
		dst = binary.AppendUvarint(dst, t.Oldest)
	}
	dst = binary.AppendUvarint(dst, uint64(len(t.Ops)))
	for i := range t.Ops {
		op := &t.Ops[i]
		dst = append(dst, byte(op.Kind))
		switch op.Kind {
		case OpPut:
			dst = appendBytes(appendBytes(dst, op.Key), op.Value)
		case OpDelete:
			dst = appendBytes(dst, op.Key)
		case OpDeleteRange:
			dst = appendBytes(appendBytes(dst, op.Key), op.End)
		case OpCheck:
			dst = append(dst, flags(op))
			dst = appendBytes(dst, op.Key)
			if !op.Absent {
				dst = appendBytes(dst, op.Value)
			}
		case OpCheckEmpty:
			dst = append(dst, flags(op))
			dst = appendBytes(appendBytes(dst, op.Key), op.End)
		case OpUpdate:
			dst = append(dst, byte(op.Mode))
			dst = appendBytes(dst, op.Key)
			dst = binary.AppendUvarint(binary.AppendUvarint(dst, uint64(op.Off)), op.N)
		}
	}
	return dst
}

func flags(op *Op) byte {
	var f byte
	if op.Absent {
		f |= flagAbsent
	}
	if op.Final {
		f |= flagFinal
	}
	return f
}

// decodeTxn decodes the Txn at the start of b into t, reusing its ops, whose keys and values
// alias b, and returns the rest of b. A Txn that does not decode, is outside the limits or is
// not the one encoding appendTxn gives it is ErrMalformed.
func decodeTxn(b []byte, t *Txn) ([]byte, error) {
	d := decoder{b: b}
	t.Replay, t.Oldest = d.bytes(), 0
	if len(t.Replay) == 0 {
		t.Replay = nil
	} else {
		t.Oldest = d.uvarint()
	}
	n := d.uvarint()
	if d.bad || n == 0 || n > uint64(len(d.b)) {
		return nil, ErrMalformed
	}
	t.Ops = t.Ops[:0]
	for range n {
		var op Op
		op.Kind = OpKind(d.byte())
		switch op.Kind {
		case OpPut:
			op.Key, op.Value = d.bytes(), d.bytes()
		case OpDelete:
			op.Key = d.bytes()
		case OpDeleteRange:
			op.Key, op.End = d.bytes(), d.bytes()
		case OpCheck, OpCheckEmpty:
			f := d.byte()
			op.Absent, op.Final = f&flagAbsent != 0, f&flagFinal != 0
			if f&^(flagAbsent|flagFinal) != 0 {
				return nil, ErrMalformed
			}
			op.Key = d.bytes()
			switch {
			case op.Kind == OpCheckEmpty:
				op.End = d.bytes()
			case !op.Absent:
				op.Value = d.bytes()
			}
		case OpUpdate:
			op.Mode = Mode(d.byte())
			op.Key = d.bytes()
			off := d.uvarint()
			op.N = d.uvarint()
			if off > MaxValue {
				return nil, ErrMalformed
			}
			op.Off = uint16(off)
		default:
			return nil, ErrMalformed
		}
		if d.bad {
			return nil, ErrMalformed
		}
		t.Ops = append(t.Ops, op)
	}
	if _, err := t.Check(); err != nil {
		return nil, ErrMalformed
	}
	return d.b, nil
}

// A replay record is FailedCheck + 1 as a uvarint, then each Update's new field value, 8 bytes
// big-endian: a final failure stores its index alone, as the caller returns it as an errno and
// needs no Actual.
func appendRecord(dst []byte, r *Result) []byte {
	dst = binary.AppendUvarint(dst, uint64(r.FailedCheck+1))
	for _, u := range r.Updated {
		dst = binary.BigEndian.AppendUint64(dst, u)
	}
	return dst
}

// decodeRecord decodes a replay record into r; one that does not decode is ErrMalformed.
func decodeRecord(b []byte, r *Result) error {
	f, n := binary.Uvarint(b)
	if n <= 0 || n != uvarintLen(f) || f > MaxTxnBytes || (len(b)-n)%8 != 0 || f != 0 && len(b) != n {
		return ErrMalformed
	}
	r.FailedCheck, r.Actual, r.Updated = int(f)-1, nil, r.Updated[:0]
	for b = b[n:]; len(b) > 0; b = b[8:] {
		r.Updated = append(r.Updated, binary.BigEndian.Uint64(b))
	}
	return nil
}

func uvarintLen(x uint64) int {
	n := 1
	for ; x >= 0x80; x >>= 7 {
		n++
	}
	return n
}

func bytesLen(b []byte) int { return uvarintLen(uint64(len(b))) + len(b) }

func appendBytes(dst, b []byte) []byte {
	return append(binary.AppendUvarint(dst, uint64(len(b))), b...)
}

type decoder struct {
	b   []byte
	bad bool
}

func (d *decoder) uvarint() uint64 {
	x, n := binary.Uvarint(d.b)
	if n <= 0 || n != uvarintLen(x) {
		d.bad, d.b = true, nil
		return 0
	}
	d.b = d.b[n:]
	return x
}

func (d *decoder) byte() byte {
	if d.bad || len(d.b) == 0 {
		d.bad = true
		return 0
	}
	c := d.b[0]
	d.b = d.b[1:]
	return c
}

func (d *decoder) bytes() []byte {
	n := d.uvarint()
	if d.bad || n > uint64(len(d.b)) {
		d.bad, d.b = true, nil
		return nil
	}
	b := d.b[:n:n]
	d.b = d.b[n:]
	return b
}
