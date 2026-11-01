package meta

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/nvcnvn/rock-solid/internal/lsm"
)

var zeros [MaxValue]byte

// minPiece is the smallest run piece a shard's tree takes: one holding a key of MaxKey with a value of MaxValue.
const minPiece = 4 << 10

// State is a shard's state machine: its LSM tree and the index of the last entry applied to it.
type State struct {
	cfg     lsm.Config
	tree    *lsm.Tree
	applied uint64
	it      lsm.Iter
	val     []byte
	rec     []byte
	plant   statePlants
}

// statePlants are bugs a test plants: a replay record ignored, and a failed check that is not final stored as the request's result.
type statePlants struct {
	replayIgnored, nonFinalStored bool
}

// NewState is an empty state at index applied.
func NewState(cfg lsm.Config, applied uint64) (*State, error) {
	if cfg.MaxPiece < minPiece {
		return nil, lsm.ErrBadConfig
	}
	tree, err := lsm.New(cfg)
	if err != nil {
		return nil, err
	}
	return &State{cfg: cfg, tree: tree, applied: applied}, nil
}

// Tree is the state's LSM tree.
func (s *State) Tree() *lsm.Tree { return s.tree }

// Applied is the index of the last entry applied.
func (s *State) Applied() uint64 { return s.applied }

// Apply evaluates t, which passed Check, into r, whose slices it reuses; ErrMalformed when t's replay key holds no record.
func (s *State) Apply(t *Txn, r *Result) error {
	// A replay record found answers the request and nothing changes. Otherwise every check runs
	// against the state before t, then, if all pass, every write in list order; the mount's
	// records below Oldest are dropped, and the result is stored under the replay key when final.
	// A client can write any key, so a replay key may hold a value that is no record.
	if t.Replay != nil && !s.plant.replayIgnored {
		if v, ok := s.tree.Get(t.Replay); ok {
			return decodeRecord(v, r)
		}
	}
	r.FailedCheck, r.Actual, r.Updated = -1, nil, r.Updated[:0]
	final := true
	for i := range t.Ops {
		if op := &t.Ops[i]; (op.Kind == OpCheck || op.Kind == OpCheckEmpty) && !s.check(op, r) {
			r.FailedCheck, final = i, op.Final || s.plant.nonFinalStored
			break
		}
	}
	if r.FailedCheck < 0 {
		for i := range t.Ops {
			s.write(&t.Ops[i], r)
		}
	}
	if t.Replay == nil {
		return nil
	}
	if t.Oldest > 0 {
		end := binary.BigEndian.AppendUint64(append(s.rec[:0], t.Replay[:len(t.Replay)-requestBytes]...), t.Oldest)
		must(s.tree.DeleteRange(end[:len(end)-requestBytes], end))
		s.rec = end
	}
	if final {
		s.rec = appendRecord(s.rec[:0], r)
		must(s.tree.Put(t.Replay, s.rec))
	}
	return nil
}

// check evaluates a Check or CheckEmpty, and on a failed Check sets r's Actual to the key's value.
func (s *State) check(op *Op, r *Result) bool {
	if op.Kind == OpCheckEmpty {
		s.tree.Iter(&s.it)
		s.it.SetUpperBound(op.End)
		return !s.it.SeekGE(op.Key)
	}
	v, ok := s.tree.Get(op.Key)
	if ok == !op.Absent && (!ok || bytes.Equal(v, op.Value)) {
		return true
	}
	if ok {
		if r.buf == nil {
			r.buf = make([]byte, 0, len(v))
		}
		r.buf = append(r.buf[:0], v...)
		r.Actual = r.buf
	}
	return false
}

func (s *State) write(op *Op, r *Result) {
	switch op.Kind {
	case OpPut:
		must(s.tree.Put(op.Key, op.Value))
	case OpDelete:
		must(s.tree.Delete(op.Key))
	case OpDeleteRange:
		must(s.tree.DeleteRange(op.Key, op.End))
	case OpUpdate:
		v, _ := s.tree.Get(op.Key)
		s.val = append(s.val[:0], v...)
		if end := int(op.Off) + fieldBytes; len(s.val) < end {
			s.val = append(s.val, zeros[:end-len(s.val)]...)
		}
		field := s.val[op.Off : op.Off+fieldBytes]
		cur := binary.BigEndian.Uint64(field)
		next := op.N
		switch op.Mode {
		case Add:
			next = cur + op.N
		case Bump:
			next = max(op.N, cur+1)
			if cur == 1<<64-1 {
				next = cur
			}
		}
		binary.BigEndian.PutUint64(field, next)
		r.Updated = append(r.Updated, next)
		must(s.tree.Put(op.Key, s.val))
	}
}

// must panics on a tree write's error: every Txn applied passed Check, which keeps its writes within the tree's limits.
func must(err error) {
	if err != nil {
		panic(fmt.Sprintf("meta: a checked Txn's write failed: %v", err))
	}
}

// KV is a key and its value.
type KV struct {
	Key, Value []byte
}

// ScanResult is a Scan's reply, which the caller reuses: its KVs alias its own buffer.
type ScanResult struct {
	KVs []KV
	// More reports that the scan stopped at its limit or at MaxScanBytes with keys left in its range.
	More bool
	buf  []byte
}

// Scan reads [start, end), a nil end the rest of the key space, into r: at most limit keys and MaxScanBytes of keys and values.
func (s *State) Scan(start, end []byte, limit int, r *ScanResult) error {
	// A caller resumes with start = last key + 0x00, so a bound may be MaxKey + 1 bytes.
	if limit < 1 {
		return ErrLimit
	}
	if len(start) > MaxKey+1 || len(end) > MaxKey+1 {
		return ErrTooLarge
	}
	r.KVs, r.More, r.buf = r.KVs[:0], false, r.buf[:0]
	if end != nil && bytes.Compare(start, end) >= 0 {
		return nil
	}
	s.tree.Iter(&s.it)
	s.it.SetUpperBound(end)
	for ok := s.it.SeekGE(start); ok; ok = s.it.Next() {
		k, v := s.it.Key(), s.it.Value()
		if len(r.KVs) == limit || len(r.buf)+len(k)+len(v) > MaxScanBytes {
			r.More = true
			break
		}
		// Once the buffer grows, the earlier KVs stay in the old array, which nothing writes again.
		off := len(r.buf)
		r.buf = append(append(r.buf, k...), v...)
		kl, n := off+len(k), len(r.buf)
		r.KVs = append(r.KVs, KV{Key: r.buf[off:kl:kl], Value: r.buf[kl:n:n]})
	}
	return nil
}

// PrefixEnd is the first key above every key with prefix p, or nil when there is none.
func PrefixEnd(p []byte) []byte {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] != 0xff {
			end := append([]byte(nil), p[:i+1]...)
			end[i]++
			return end
		}
	}
	return nil
}

// Snapshot data is a format byte, then each key and its value in key order, each a uvarint length and its bytes.
const snapshotFormat = 1

// SnapshotData is the state's contents at index, which must be the index last applied.
func (s *State) SnapshotData(index uint64) []byte {
	if index != s.applied {
		panic(fmt.Sprintf("meta: snapshot asked at index %d with index %d applied", index, s.applied))
	}
	data := []byte{snapshotFormat}
	s.tree.Iter(&s.it)
	s.it.SetUpperBound(nil)
	for ok := s.it.SeekGE(nil); ok; ok = s.it.Next() {
		data = appendBytes(appendBytes(data, s.it.Key()), s.it.Value())
	}
	return data
}

// CheckSnapshot reports whether data is snapshot data a state restores: keys ascending and within the limits.
func CheckSnapshot(data []byte) bool {
	return walkSnapshot(data, nil) == nil
}

// Restore replaces the state with snapshot data at index; data that does not restore leaves the state as it was.
func (s *State) Restore(index uint64, data []byte) error {
	tree, err := lsm.New(s.cfg)
	if err != nil {
		return err
	}
	if err := walkSnapshot(data, tree); err != nil {
		return err
	}
	s.tree, s.applied = tree, index
	return nil
}

// walkSnapshot checks snapshot data and puts each of its keys into tree, if not nil. A restored
// snapshot enters the memtable until the shard's checkpoint names runs (docs/tech-debts.md).
func walkSnapshot(data []byte, tree *lsm.Tree) error {
	if len(data) == 0 || data[0] != snapshotFormat {
		return ErrMalformed
	}
	d := decoder{b: data[1:]}
	var prev []byte
	for first := true; len(d.b) > 0; first = false {
		k, v := d.bytes(), d.bytes()
		if d.bad || len(k) > MaxKey || len(v) > MaxValue || !first && bytes.Compare(prev, k) >= 0 {
			return ErrMalformed
		}
		if tree != nil {
			if err := tree.Put(k, v); err != nil {
				return err
			}
		}
		prev = k
	}
	return nil
}
