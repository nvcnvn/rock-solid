package meta

import (
	"bytes"
	"encoding/binary"
	"errors"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/lsm"
)

func newTestState(t testing.TB) *State {
	t.Helper()
	s, err := NewState(lsm.Config{MaxPiece: 64 << 10, Check: *checkTree}, 0)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// run checks t, round-trips it through its encoding as a log entry carries it, and applies it.
func run(tb testing.TB, s *State, t Txn) Result {
	tb.Helper()
	if _, err := t.Check(); err != nil {
		tb.Fatalf("%+v refused: %v", t, err)
	}
	var d Txn
	if rest, err := decodeTxn(appendTxn(nil, &t), &d); err != nil || len(rest) != 0 {
		tb.Fatalf("%+v does not decode back: %v, %d bytes left", t, err, len(rest))
	}
	var r Result
	s.Apply(&d, &r)
	return r
}

func ops(o ...Op) Txn { return Txn{Ops: o} }

func get(s *State, k string) (string, bool) {
	v, ok := s.tree.Get([]byte(k))
	return string(v), ok
}

func field(s *State, k string, off int) uint64 {
	v, _ := s.tree.Get([]byte(k))
	v = append(slices.Clone(v), make([]byte, off+8)...)
	return binary.BigEndian.Uint64(v[off:])
}

func set(s *State, kv ...string) {
	for i := 0; i < len(kv); i += 2 {
		must(s.tree.Put([]byte(kv[i]), []byte(kv[i+1])))
	}
}

func b(s string) []byte { return []byte(s) }

func be(n uint64) []byte { return binary.BigEndian.AppendUint64(nil, n) }

func TestTxnChecksFirstThenWritesInOrder(t *testing.T) {
	s := newTestState(t)
	if r := run(t, s, ops(Put(b("a"), b("1")))); r.FailedCheck != -1 {
		t.Fatalf("put failed check %d", r.FailedCheck)
	}
	if v, _ := get(s, "a"); v != "1" {
		t.Fatalf("a = %q, want 1", v)
	}
	if r := run(t, s, ops(Check(b("a"), b("1"), false), Put(b("a"), b("2")))); r.FailedCheck != -1 {
		t.Fatalf("check a == 1 failed")
	}
	r := run(t, s, ops(Check(b("a"), b("1"), false), Put(b("a"), b("3")), Put(b("b"), b("1"))))
	if r.FailedCheck != 0 || string(r.Actual) != "2" {
		t.Fatalf("check a == 1 with a = 2: FailedCheck %d, Actual %q; want 0 and 2", r.FailedCheck, r.Actual)
	}
	if v, _ := get(s, "a"); v != "2" {
		t.Fatalf("a = %q after a failed Txn, want 2", v)
	}
	if _, ok := get(s, "b"); ok {
		t.Fatal("b written by a failed Txn")
	}
	if r := run(t, s, ops(CheckAbsent(b("a"), true), Put(b("a"), b("1")))); r.FailedCheck != 0 || string(r.Actual) != "2" {
		t.Fatalf("check a absent where a exists: FailedCheck %d, Actual %q", r.FailedCheck, r.Actual)
	}
	if r := run(t, s, ops(Delete(b("c")))); r.FailedCheck != -1 {
		t.Fatal("a delete of an absent key failed")
	}
	run(t, s, ops(Put(b("d"), b("1")), Delete(b("d"))))
	if _, ok := get(s, "d"); ok {
		t.Fatal("put d, delete d left d")
	}
	if r := run(t, s, ops(Put(b("e"), b("1")), CheckAbsent(b("e"), false))); r.FailedCheck != -1 {
		t.Fatal("a check saw its own Txn's earlier write")
	}
	if r := run(t, s, ops(Put(b("f"), nil), Check(b("f"), nil, false))); r.FailedCheck != 1 || r.Actual != nil {
		t.Fatalf("check f == empty where f is absent: FailedCheck %d, Actual %v", r.FailedCheck, r.Actual)
	}
	run(t, s, ops(Put(b("f"), nil)))
	if r := run(t, s, ops(CheckAbsent(b("f"), false))); r.FailedCheck != 0 || r.Actual == nil || len(r.Actual) != 0 {
		t.Fatalf("check f absent where f is empty: FailedCheck %d, Actual %v, want a non-nil empty value", r.FailedCheck, r.Actual)
	}
}

func TestUpdatesComposeAndMoveForward(t *testing.T) {
	s := newTestState(t)
	r := run(t, s, ops(Update(b("a"), 0, Add, 5), Update(b("a"), 8, Bump, 3), Put(b("b"), b("x")), Delete(b("c"))))
	if r.FailedCheck != -1 || field(s, "a", 0) != 5 || field(s, "a", 8) != 3 || !slices.Equal(r.Updated, []uint64{5, 3}) {
		t.Fatalf("fields %d, %d, Updated %v; want 5, 3 and [5 3]", field(s, "a", 0), field(s, "a", 8), r.Updated)
	}
	set(s, "m", string(be(20)))
	if r := run(t, s, ops(Update(b("m"), 0, Bump, 10))); r.Updated[0] != 21 {
		t.Fatalf("Bump 10 over 20 stored %d, want 21", r.Updated[0])
	}
	set(s, "w", string(be(1<<64-1)))
	if r := run(t, s, ops(Update(b("w"), 0, Add, 1))); r.Updated[0] != 0 {
		t.Fatalf("Add 1 over 2^64-1 stored %d, want 0", r.Updated[0])
	}
	set(s, "s", "xy")
	run(t, s, ops(Update(b("s"), 4, Set, 7)))
	if v, _ := get(s, "s"); v != "xy\x00\x00"+string(be(7)) {
		t.Fatalf("Set on a short value left %x", v)
	}
	if v, _ := get(s, "a"); len(v) != 16 {
		t.Fatalf("a holds %d bytes, want its two fields' 16", len(v))
	}
	// The directory's mtime is 10 s ahead of a creating node's clock: Bump still moves it forward.
	set(s, "dir", string(be(50_000_000_000)))
	if r := run(t, s, ops(Update(b("dir"), 0, Bump, 40_000_000_000))); r.Updated[0] != 50_000_000_001 {
		t.Fatalf("a create from a clock 10 s behind moved mtime to %d", r.Updated[0])
	}
}

func TestRangesAreHalfOpen(t *testing.T) {
	s := newTestState(t)
	set(s, "d/x", "1")
	dir := b("d/")
	if r := run(t, s, ops(CheckEmpty(dir, PrefixEnd(dir), true))); r.FailedCheck != 0 || r.Actual != nil {
		t.Fatalf("CheckEmpty over a directory with an entry: FailedCheck %d, Actual %q", r.FailedCheck, r.Actual)
	}
	run(t, s, ops(Delete(b("d/x"))))
	if r := run(t, s, ops(CheckEmpty(dir, PrefixEnd(dir), true))); r.FailedCheck != -1 {
		t.Fatal("CheckEmpty over an emptied directory failed")
	}
	set(s, "d/y", "1")
	if r := run(t, s, ops(CheckEmpty(b("d/"), b("d/y"), true))); r.FailedCheck != -1 {
		t.Fatal("CheckEmpty's end is inside its range")
	}
	set(s, "k1", "", "k2", "", "k3", "")
	run(t, s, ops(DeleteRange(b("k1"), b("k3"))))
	for k, want := range map[string]bool{"k1": false, "k2": false, "k3": true} {
		if _, ok := get(s, k); ok != want {
			t.Fatalf("after DeleteRange(k1, k3), %s present %v", k, ok)
		}
	}
}

func TestLimitsAreCheckedBeforeProposing(t *testing.T) {
	long := func(n int) []byte { return bytes.Repeat([]byte("k"), n) }
	pieces := func(total int) Txn {
		var t Txn
		for i := 0; total > 0; i++ {
			n := min(total, MaxValue)
			t.Ops = append(t.Ops, Put(append(long(40), byte(i), byte(i>>8)), long(n)))
			total -= n
		}
		return t
	}
	for _, c := range []struct {
		name string
		txn  Txn
		err  error
	}{
		{"no ops", Txn{}, ErrNoOps},
		{"key of MaxKey", ops(Put(long(MaxKey), nil)), nil},
		{"key of MaxKey+1", ops(Put(long(MaxKey+1), nil)), ErrTooLarge},
		{"checked key of MaxKey+1", ops(CheckAbsent(long(MaxKey+1), false)), ErrTooLarge},
		{"value of MaxValue", ops(Put(b("a"), long(MaxValue))), nil},
		{"value of MaxValue+1", ops(Put(b("a"), long(MaxValue+1))), ErrTooLarge},
		{"checked value of MaxValue+1", ops(Check(b("a"), long(MaxValue+1), false)), ErrTooLarge},
		{"range bound of 1000", ops(DeleteRange(long(MaxBound-1), long(MaxBound))), nil},
		{"range start of 1001", ops(DeleteRange(long(MaxBound+1), b("z"))), ErrTooLarge},
		{"range end of 1001", ops(CheckEmpty(b("a"), long(MaxBound+1), false)), ErrTooLarge},
		{"empty range", ops(DeleteRange(b("b"), b("b"))), ErrEmptyRange},
		{"reversed range", ops(CheckEmpty(b("c"), b("b"), false)), ErrEmptyRange},
		{"field past MaxValue", ops(Update(b("a"), MaxValue-7, Set, 1)), ErrTooLarge},
		{"last field", ops(Update(b("a"), MaxValue-8, Set, 1)), nil},
		{"unknown mode", ops(Update(b("a"), 0, 9, 1)), ErrBadOp},
		{"unknown op", ops(Op{Kind: 9, Key: b("a")}), ErrBadOp},
		{"82 KiB xattr in pieces", pieces(82 << 10), nil},
		{"1 MiB in pieces", pieces(1 << 20), ErrTooLarge},
		{"replay key short of a request ID", Txn{Ops: []Op{Delete(b("a"))}, Replay: b("r/1")}, ErrBadReplay},
		{"oldest above its own request", Txn{Ops: []Op{Delete(b("a"))}, Replay: append(b("r/"), be(5)...), Oldest: 6}, ErrBadReplay},
		{"oldest at its own request", Txn{Ops: []Op{Delete(b("a"))}, Replay: append(b("r/"), be(5)...), Oldest: 5}, nil},
	} {
		if _, err := c.txn.Check(); !errors.Is(err, c.err) {
			t.Errorf("%s: %v, want %v", c.name, err, c.err)
		}
	}
	updates := Txn{Replay: append(b("r/"), be(1)...)}
	for range MaxReplayUpdates {
		updates.Ops = append(updates.Ops, Update(b("a"), 0, Add, 1))
	}
	if _, err := updates.Check(); err != nil {
		t.Fatalf("%d Updates under a replay key: %v", MaxReplayUpdates, err)
	}
	s := newTestState(t)
	r := run(t, s, updates)
	if v, _ := get(s, string(updates.Replay)); len(v) > MaxValue || r.Updated[MaxReplayUpdates-1] != MaxReplayUpdates {
		t.Fatalf("record of %d bytes, last Update %d", len(v), r.Updated[MaxReplayUpdates-1])
	}
	updates.Ops = append(updates.Ops, Update(b("a"), 0, Add, 1))
	if _, err := updates.Check(); !errors.Is(err, ErrBadReplay) {
		t.Fatalf("%d Updates under a replay key: %v, want ErrBadReplay", MaxReplayUpdates+1, err)
	}
}

func scanKeys(t *testing.T, s *State, start, end []byte, limit int) ([]string, bool) {
	t.Helper()
	var r ScanResult
	if err := s.Scan(start, end, limit, &r); err != nil {
		t.Fatal(err)
	}
	var ks []string
	for _, kv := range r.KVs {
		ks = append(ks, string(kv.Key))
	}
	return ks, r.More
}

func TestScan(t *testing.T) {
	s := newTestState(t)
	set(s, "a", "", "aa", "", "ab", "", "abc", "", "b", "")
	a, ab := b("a"), b("ab")
	for _, c := range []struct {
		start, end []byte
		limit      int
		want       []string
		more       bool
	}{
		{a, PrefixEnd(a), 10, []string{"a", "aa", "ab", "abc"}, false},
		{ab, PrefixEnd(ab), 10, []string{"ab", "abc"}, false},
		{b("aa\x00"), PrefixEnd(a), 10, []string{"ab", "abc"}, false},
		{a, PrefixEnd(a), 2, []string{"a", "aa"}, true},
		{b("aa\x00"), PrefixEnd(a), 2, []string{"ab", "abc"}, false},
		{b("zz"), PrefixEnd(a), 10, nil, false},
		{b("c"), PrefixEnd(b("c")), 10, nil, false},
		{b("ab"), b("ab\x00"), 1, []string{"ab"}, false},
		{nil, nil, 10, []string{"a", "aa", "ab", "abc", "b"}, false},
	} {
		if got, more := scanKeys(t, s, c.start, c.end, c.limit); !slices.Equal(got, c.want) || more != c.more {
			t.Errorf("Scan(%q, %q, %d) = %q, more %v; want %q, more %v", c.start, c.end, c.limit, got, more, c.want, c.more)
		}
	}
	var r ScanResult
	if err := s.Scan(a, PrefixEnd(a), 0, &r); !errors.Is(err, ErrLimit) {
		t.Fatalf("limit 0: %v", err)
	}
	if err := s.Scan(make([]byte, MaxKey+2), nil, 1, &r); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("a bound of MaxKey+2: %v", err)
	}
	maxKey := bytes.Repeat([]byte{0xfe}, MaxKey)
	set(s, string(maxKey), "v")
	if got, _ := scanKeys(t, s, append(maxKey, 0), nil, 1); got != nil {
		t.Fatalf("resuming after a key of MaxKey returned %q", got)
	}
}

func TestScanStopsAtItsByteBudget(t *testing.T) {
	s := newTestState(t)
	val := bytes.Repeat([]byte("v"), MaxValue)
	n := 0
	for total := 0; total < 1<<20; n++ {
		k := binary.BigEndian.AppendUint32(nil, uint32(n))
		must(s.tree.Put(k, val))
		total += len(k) + len(val)
	}
	var r ScanResult
	got, start := 0, []byte{}
	for scans := 0; ; scans++ {
		if err := s.Scan(start, nil, 10_000, &r); err != nil {
			t.Fatal(err)
		}
		size := 0
		for i, kv := range r.KVs {
			if binary.BigEndian.Uint32(kv.Key) != uint32(got+i) || !bytes.Equal(kv.Value, val) {
				t.Fatalf("scan %d returned key %x at %d", scans, kv.Key, got+i)
			}
			size += len(kv.Key) + len(kv.Value)
		}
		if size > MaxScanBytes || r.More && size+4+MaxValue <= MaxScanBytes {
			t.Fatalf("scan %d returned %d bytes, more %v", scans, size, r.More)
		}
		got += len(r.KVs)
		if !r.More {
			break
		}
		start = append(slices.Clone(r.KVs[len(r.KVs)-1].Key), 0)
	}
	if got != n {
		t.Fatalf("scans returned %d keys of %d", got, n)
	}
}

// A scan's reply is taken from one state; puts applied after it change neither it nor the reply.
func TestAScanReplyIsAPointInTimeView(t *testing.T) {
	s := newTestState(t)
	for i := range 1000 {
		set(s, "d/"+string(be(uint64(i))), "1")
	}
	var r ScanResult
	d := b("d/")
	if err := s.Scan(d, PrefixEnd(d), 1000, &r); err != nil {
		t.Fatal(err)
	}
	for i := range 1000 {
		run(t, s, ops(Put(append(b("d/"), be(uint64(i))...), b("2")), Put(append(b("d/"), be(uint64(i+1000))...), b("2"))))
	}
	if len(r.KVs) != 1000 || r.More {
		t.Fatalf("scan of 1,000 entries returned %d, more %v", len(r.KVs), r.More)
	}
	for i, kv := range r.KVs {
		if !bytes.Equal(kv.Key, append(b("d/"), be(uint64(i))...)) || string(kv.Value) != "1" {
			t.Fatalf("entry %d is %q = %q after later puts", i, kv.Key, kv.Value)
		}
	}
}

// A purge list consumed in key order loses each consumed run in one Txn: a DeleteRange carrying
// a Check on every entry it consumed and a CheckEmpty over each gap, so an entry added inside
// the run after the purger read it fails the Txn rather than being removed unread.
func TestAConsumedRunIsRemovedByOneDeleteRange(t *testing.T) {
	s := newTestState(t)
	key := func(i uint64) []byte { return append(b("p/"), be(i)...) }
	for _, i := range []uint64{1, 3, 5, 8} {
		set(s, string(key(i)), "x")
	}
	consume := func(read []uint64) Txn {
		var t Txn
		for j, i := range read {
			t.Ops = append(t.Ops, Check(key(i), b("x"), false))
			if j > 0 {
				t.Ops = append(t.Ops, CheckEmpty(append(key(read[j-1]), 0), key(i), false))
			}
		}
		t.Ops = append(t.Ops, DeleteRange(key(read[0]), append(key(read[len(read)-1]), 0)))
		return t
	}
	set(s, string(key(4)), "x")
	if r := run(t, s, consume([]uint64{1, 3, 5})); r.FailedCheck != 4 {
		t.Fatalf("an entry added inside the consumed run: FailedCheck %d, want the gap check 4", r.FailedCheck)
	}
	if r := run(t, s, consume([]uint64{1, 3, 4, 5})); r.FailedCheck != -1 {
		t.Fatalf("consuming 1 to 5 failed check %d", r.FailedCheck)
	}
	if got, _ := scanKeys(t, s, b("p/"), PrefixEnd(b("p/")), 10); !slices.Equal(got, []string{string(key(8))}) {
		t.Fatalf("purge list holds %q after consuming its head", got)
	}
}

func replayKey(mount byte, request uint64) []byte {
	return append([]byte{'r', mount}, be(request)...)
}

func TestReplayRecords(t *testing.T) {
	s := newTestState(t)
	mkdir := Txn{Ops: []Op{CheckAbsent(b("d/x"), true), Put(b("d/x"), b("1")), Update(b("a"), 0, Add, 1)}, Replay: replayKey(1, 42)}
	first := run(t, s, mkdir)
	again := run(t, s, mkdir)
	if first.FailedCheck != -1 || again.FailedCheck != -1 || !slices.Equal(again.Updated, []uint64{1}) || field(s, "a", 0) != 1 {
		t.Fatalf("a resent mkdir returned FailedCheck %d, Updated %v, nlink %d; want its first success and no change", again.FailedCheck, again.Updated, field(s, "a", 0))
	}
	exists := mkdir
	exists.Replay = replayKey(1, 43)
	if r := run(t, s, exists); r.FailedCheck != 0 {
		t.Fatal("mkdir over an existing name passed")
	}
	run(t, s, ops(Delete(b("d/x"))))
	if r := run(t, s, exists); r.FailedCheck != 0 || r.Actual != nil {
		t.Fatalf("a resent EEXIST, after the name went, returned FailedCheck %d, Actual %q: a final failure is stored without its value", r.FailedCheck, r.Actual)
	}
	race := Txn{Ops: []Op{Check(b("f"), b("old"), false), Put(b("f"), b("new"))}, Replay: replayKey(1, 44)}
	if r := run(t, s, race); r.FailedCheck != 0 {
		t.Fatal("a lost compare-and-set passed")
	}
	if _, ok := get(s, string(race.Replay)); ok {
		t.Fatal("a failure that is not final was stored")
	}
	set(s, "f", "old")
	if r := run(t, s, race); r.FailedCheck != -1 {
		t.Fatal("the retry of a lost compare-and-set did not run again")
	}
	other := mkdir
	other.Replay = replayKey(2, 42)
	if r := run(t, s, other); r.FailedCheck != -1 || field(s, "a", 0) != 2 {
		t.Fatal("another mount's request 42 was answered by mount 1's record")
	}
	oldest := Txn{Ops: []Op{Put(b("g"), nil)}, Replay: replayKey(1, 60), Oldest: 50}
	set(s, string(replayKey(1, 49)), "\x00", string(replayKey(1, 50)), "\x00", string(replayKey(0, 10)), "\x00")
	run(t, s, oldest)
	got, _ := scanKeys(t, s, b("r"), PrefixEnd(b("r")), 100)
	want := []string{string(replayKey(0, 10)), string(replayKey(1, 50)), string(replayKey(1, 60)), string(replayKey(2, 42))}
	if !slices.Equal(got, want) {
		t.Fatalf("records after oldest 50 from mount 1: %q, want %q", got, want)
	}
	// The session leaves the session table: its leader removes its records with an internal Txn.
	run(t, s, ops(DeleteRange(b("r"), PrefixEnd(b("r")))))
	if r := run(t, s, mkdir); r.FailedCheck != 0 {
		t.Fatal("a resend after its record was dropped did not run again")
	}
}
