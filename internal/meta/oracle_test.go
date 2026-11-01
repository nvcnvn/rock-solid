package meta

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

// model is the oracle a State is checked against: its keys as a map, each Txn evaluated as the
// design states it, with no tree. Replay records are kept in the map as their encoding, since a
// client can overwrite one.
type model map[string]string

func (m model) apply(t *Txn) (Result, error) {
	if v, ok := m[string(t.Replay)]; ok && t.Replay != nil {
		var r Result
		if err := decodeRecord([]byte(v), &r); err != nil {
			return Result{}, ErrMalformed
		}
		return r, nil
	}
	r, final := Result{FailedCheck: -1}, true
	for i, op := range t.Ops {
		if op.Kind != OpCheck && op.Kind != OpCheckEmpty {
			continue
		}
		v, ok := m[string(op.Key)]
		pass := ok == !op.Absent && (!ok || v == string(op.Value))
		if op.Kind == OpCheckEmpty {
			pass, ok = true, false
			for k := range m {
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
			m[k] = string(op.Value)
		case OpDelete:
			delete(m, k)
		case OpDeleteRange:
			m.deleteRange(k, string(op.End))
		case OpUpdate:
			v := []byte(m[k])
			for len(v) < int(op.Off)+8 {
				v = append(v, 0)
			}
			cur, next := binary.BigEndian.Uint64(v[op.Off:]), op.N
			switch op.Mode {
			case Add:
				next = cur + op.N
			case Bump:
				next = max(op.N, cur+1)
				if cur == 1<<64-1 {
					next = cur
				}
			}
			binary.BigEndian.PutUint64(v[op.Off:], next)
			m[k] = string(v)
			r.Updated = append(r.Updated, next)
		}
	}
	if t.Replay == nil {
		return r, nil
	}
	mount := string(t.Replay[:len(t.Replay)-8])
	if t.Oldest > 0 {
		m.deleteRange(mount, mount+string(be(t.Oldest)))
	}
	if final {
		m[string(t.Replay)] = string(appendRecord(nil, &r))
	}
	return r, nil
}

func (m model) deleteRange(start, end string) {
	for k := range m {
		if k >= start && k < end {
			delete(m, k)
		}
	}
}

// gen draws Txns over a few keys and values, so checks pass and fail, ranges cover keys and
// Updates land on short values and on each other.
type gen struct {
	rng     *rand.Rand
	replays bool
}

// key is one of six keys, or now and then a replay key, which a client can write too.
func (g gen) key() []byte {
	if g.replays && g.rng.IntN(8) == 0 {
		return replayKey(byte(g.rng.IntN(3)), 1+g.rng.Uint64N(4))
	}
	return []byte{'k', byte('a' + g.rng.IntN(6))}
}
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

// matches reports how s differs from m.
func matches(s *State, m model) error {
	var r ScanResult
	if err := s.Scan(nil, nil, 1<<20, &r); err != nil {
		return err
	}
	for _, e := range r.KVs {
		if v, ok := m[string(e.Key)]; !ok || v != string(e.Value) {
			return fmt.Errorf("%q = %q, the model %q (%v)", e.Key, e.Value, v, ok)
		}
	}
	if len(r.KVs) != len(m) {
		return fmt.Errorf("%d keys, the model %d", len(r.KVs), len(m))
	}
	return nil
}

// oracleRun applies random Txns, some under replay keys of a few mounts, to a State and to the
// model, and compares each result and the whole state after each.
func oracleRun(tb testing.TB, seed uint64) error {
	g := gen{rng: rand.New(rand.NewPCG(seed, 0)), replays: true}
	s, m := newTestState(tb), model{}
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
		want, wantErr := m.apply(&t)
		got, err := apply(tb, s, t)
		if err != wantErr || err == nil && !sameResult(&got, &want, false) {
			return fmt.Errorf("txn %d %+v: got %+v (%v), the model %+v (%v)", i, t, got, err, want, wantErr)
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
