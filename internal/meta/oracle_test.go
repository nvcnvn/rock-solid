package meta

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

// model is the oracle a State is checked against: its keys as a map and its replay records as
// results, each Txn evaluated as the design states it, with no tree and no encoding.
type model struct {
	kv   map[string]string
	recs map[string]Result
}

func newModel() *model { return &model{kv: map[string]string{}, recs: map[string]Result{}} }

func (m *model) apply(t *Txn) Result {
	if r, ok := m.recs[string(t.Replay)]; ok && t.Replay != nil {
		return r
	}
	r, final := Result{FailedCheck: -1}, true
	for i, op := range t.Ops {
		if op.Kind != OpCheck && op.Kind != OpCheckEmpty {
			continue
		}
		v, ok := m.kv[string(op.Key)]
		pass := ok == !op.Absent && (!ok || v == string(op.Value))
		if op.Kind == OpCheckEmpty {
			pass, ok = true, false
			for k := range m.kv {
				pass = pass && !(k >= string(op.Key) && k < string(op.End))
			}
		}
		if !pass {
			r.FailedCheck, final = i, op.Final
			if ok {
				r.Actual = []byte(v)
			}
			break
		}
	}
	for _, op := range t.Ops {
		if r.FailedCheck >= 0 {
			break
		}
		switch k := string(op.Key); op.Kind {
		case OpPut:
			m.kv[k] = string(op.Value)
		case OpDelete:
			delete(m.kv, k)
		case OpDeleteRange:
			for key := range m.kv {
				if key >= k && key < string(op.End) {
					delete(m.kv, key)
				}
			}
		case OpUpdate:
			v := []byte(m.kv[k])
			for len(v) < int(op.Off)+8 {
				v = append(v, 0)
			}
			cur, next := binary.BigEndian.Uint64(v[op.Off:]), op.N
			switch op.Mode {
			case Add:
				next = cur + op.N
			case Bump:
				next = max(op.N, cur+1)
			}
			binary.BigEndian.PutUint64(v[op.Off:], next)
			m.kv[k] = string(v)
			r.Updated = append(r.Updated, next)
		}
	}
	if t.Replay == nil {
		return r
	}
	mount := string(t.Replay[:len(t.Replay)-8])
	for k := range m.recs {
		if strings.HasPrefix(k, mount) && len(k) == len(t.Replay) && binary.BigEndian.Uint64([]byte(k[len(mount):])) < t.Oldest {
			delete(m.recs, k)
		}
	}
	if final {
		m.recs[string(t.Replay)] = Result{FailedCheck: r.FailedCheck, Updated: r.Updated}
	}
	return r
}

// gen draws Txns over a few keys and values, so checks pass and fail, ranges cover keys and
// Updates land on short values and on each other.
type gen struct {
	rng *rand.Rand
}

func (g gen) key() []byte   { return []byte{'k', byte('a' + g.rng.IntN(6))} }
func (g gen) value() []byte { return []byte([]string{"", "x", "y", "xy", "xyzzyxyzzy"}[g.rng.IntN(5)]) }
func (g gen) range_() ([]byte, []byte) {
	a, b := g.key(), g.key()
	if bytes.Compare(a, b) > 0 {
		a, b = b, a
	}
	return a, append(b, 0)
}

func (g gen) txn() Txn {
	var t Txn
	for range 1 + g.rng.IntN(5) {
		var op Op
		switch g.rng.IntN(8) {
		case 0, 1:
			op = Put(g.key(), g.value())
		case 2:
			op = Delete(g.key())
		case 3:
			op = DeleteRange(g.range_())
		case 4:
			op = Check(g.key(), g.value(), g.rng.IntN(2) == 0)
		case 5:
			op = CheckAbsent(g.key(), g.rng.IntN(2) == 0)
		case 6:
			s, e := g.range_()
			op = CheckEmpty(s, e, g.rng.IntN(2) == 0)
		case 7:
			n := g.rng.Uint64N(4)
			if g.rng.IntN(4) == 0 {
				n = -n
			}
			op = Update(g.key(), uint16(g.rng.IntN(3)*4), Mode(1+g.rng.IntN(3)), n)
		}
		t.Ops = append(t.Ops, op)
	}
	return t
}

func sameResult(got, want *Result, replayed bool) bool {
	actual := bytes.Equal(got.Actual, want.Actual) && (got.Actual == nil) == (want.Actual == nil)
	return got.FailedCheck == want.FailedCheck && (actual || replayed && got.Actual == nil) && slices.Equal(got.Updated, want.Updated)
}

// matches reports how s differs from m, comparing every key and decoding every replay record.
func matches(s *State, m *model) error {
	var r ScanResult
	if err := s.Scan(nil, nil, 1<<20, &r); err != nil {
		return err
	}
	kv, recs := 0, 0
	for _, e := range r.KVs {
		if e.Key[0] == 'r' {
			var got Result
			want, ok := m.recs[string(e.Key)]
			if err := decodeRecord(e.Value, &got); err != nil || !ok || !sameResult(&got, &want, true) {
				return fmt.Errorf("record %x holds %+v (%v), the model %+v (%v)", e.Key, got, err, want, ok)
			}
			recs++
			continue
		}
		if v, ok := m.kv[string(e.Key)]; !ok || v != string(e.Value) {
			return fmt.Errorf("%q = %q, the model %q (%v)", e.Key, e.Value, v, ok)
		}
		kv++
	}
	if kv != len(m.kv) || recs != len(m.recs) {
		return fmt.Errorf("%d keys and %d records, the model %d and %d", kv, recs, len(m.kv), len(m.recs))
	}
	return nil
}

// oracleRun applies random Txns, some under replay keys of a few mounts, to a State and to the
// model, and compares each result and the whole state after each.
func oracleRun(tb testing.TB, seed uint64) error {
	g := gen{rng: rand.New(rand.NewPCG(seed, 0))}
	s, m := newTestState(tb), newModel()
	next := [3]uint64{}
	for i := range 300 {
		t := g.txn()
		if g.rng.IntN(2) == 0 {
			mount := g.rng.IntN(3)
			if next[mount] == 0 || g.rng.IntN(4) > 0 {
				next[mount]++
			}
			t.Replay = replayKey(byte(mount), next[mount])
			t.Oldest = next[mount] - g.rng.Uint64N(min(next[mount], 4))
		}
		want := m.apply(&t)
		got := run(tb, s, t)
		if !sameResult(&got, &want, false) {
			return fmt.Errorf("txn %d %+v: got %+v, the model %+v", i, t, got, want)
		}
		if err := matches(s, m); err != nil {
			return fmt.Errorf("after txn %d %+v: %w", i, t, err)
		}
	}
	return nil
}

func TestTxnsMatchTheOracle(t *testing.T) {
	seeds.Run(t, 1, 200, func(seed uint64) {
		if err := oracleRun(t, seed); err != nil {
			t.Fatal(err)
		}
	})
}

// replayRun sends requests from a few mounts, each resent 0 to 3 times at random later points
// with honest oldest-outstanding values, and reports the first resend that does not return its
// request's first final result, that changes the state, or a request whose result was not final
// that left a record.
func replayRun(tb testing.TB, seed uint64, p statePlants) error {
	g := gen{rng: rand.New(rand.NewPCG(seed, 1))}
	s := newTestState(tb)
	s.plant = p
	type request struct {
		txn    Txn
		resend int
		first  *Result
	}
	var out []*request
	next := [3]uint64{}
	var before, after ScanResult
	for step := 0; step < 400; step++ {
		if len(out) == 0 || g.rng.IntN(3) == 0 {
			mount := g.rng.IntN(3)
			next[mount]++
			oldest := next[mount]
			for _, r := range out {
				if r.txn.Replay[1] == byte(mount) {
					oldest = min(oldest, binary.BigEndian.Uint64(r.txn.Replay[2:]))
				}
			}
			r := &request{txn: g.txn(), resend: g.rng.IntN(4)}
			r.txn.Replay, r.txn.Oldest = replayKey(byte(mount), next[mount]), oldest
			out = append(out, r)
		}
		i := g.rng.IntN(len(out))
		r := out[i]
		if err := s.Scan(nil, nil, 1<<20, &before); err != nil {
			return err
		}
		res := run(tb, s, r.txn)
		if err := s.Scan(nil, nil, 1<<20, &after); err != nil {
			return err
		}
		final := res.FailedCheck < 0 || r.txn.Ops[res.FailedCheck].Final
		switch {
		case r.first != nil && !sameResult(&res, r.first, true):
			return fmt.Errorf("step %d: resent %x returned %+v, its first final result %+v", step, r.txn.Replay, res, *r.first)
		case r.first != nil && !slices.EqualFunc(before.KVs, after.KVs, func(a, b KV) bool { return bytes.Equal(a.Key, b.Key) && bytes.Equal(a.Value, b.Value) }):
			return fmt.Errorf("step %d: resent %x changed the state", step, r.txn.Replay)
		case r.first == nil && final:
			r.first = &Result{FailedCheck: res.FailedCheck, Updated: slices.Clone(res.Updated)}
		case r.first == nil:
			if _, ok := s.tree.Get(r.txn.Replay); ok {
				return fmt.Errorf("step %d: %x failed check %d, not final, and left a record", step, r.txn.Replay, res.FailedCheck)
			}
		}
		if r.resend--; r.resend < 0 {
			out = slices.Delete(out, i, i+1)
		}
	}
	return nil
}

func TestReplayedRequestsReturnTheirFirstFinalResult(t *testing.T) {
	seeds.Run(t, 1, 200, func(seed uint64) {
		if err := replayRun(t, seed, statePlants{}); err != nil {
			t.Fatal(err)
		}
	})
}

func TestReplayPlantsAreCaught(t *testing.T) {
	for _, p := range []statePlants{{replayIgnored: true}, {nonFinalStored: true}} {
		caught := false
		for seed := uint64(1); seed <= 20 && !caught; seed++ {
			caught = replayRun(t, seed, p) != nil
		}
		if !caught {
			t.Errorf("plant %+v passed 20 seeds", p)
		}
	}
}
