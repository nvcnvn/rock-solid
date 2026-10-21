package sim_test

import (
	"errors"
	"maps"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/sim"
)

type put struct{ key, val int }

type kv map[int]int

func (m kv) Apply(p put) { m[p.key] = p.val }
func (m kv) Clone() kv   { return maps.Clone(m) }

// window is the test scenarios' picture: LSNs 1 to 7 acknowledged, 8 and 9 in flight, each
// LSN putting its own number at key lsn%4, so every LSN changes the model.
func window(t *testing.T) *sim.Oracle[put, kv] {
	t.Helper()
	o := sim.NewOracle[put](0, kv{})
	for lsn := 1; lsn <= 9; lsn++ {
		if err := o.Written(uint64(lsn), put{lsn % 4, lsn}); err != nil {
			t.Fatal(err)
		}
	}
	if err := o.Acked(7); err != nil {
		t.Fatal(err)
	}
	return o
}

func TestOracleWindow(t *testing.T) {
	o := window(t)
	if a, n := o.Window(); a != 7 || n != 9 {
		t.Fatalf("window %d to %d, want 7 to 9", a, n)
	}
	want := map[uint64]kv{
		7: {1: 5, 2: 6, 3: 7, 0: 4},
		8: {1: 5, 2: 6, 3: 7, 0: 8},
		9: {1: 9, 2: 6, 3: 7, 0: 8},
	}
	for r, w := range want {
		got, err := o.Expect(r)
		if err != nil || !maps.Equal(got, w) {
			t.Fatalf("Expect(%d) = %v, %v; want %v", r, got, err, w)
		}
	}
	if _, err := o.Expect(6); !errors.Is(err, sim.ErrLostAcked) {
		t.Fatalf("Expect(6): %v, want ErrLostAcked", err)
	}
	if _, err := o.Expect(10); !errors.Is(err, sim.ErrPastWritten) {
		t.Fatalf("Expect(10): %v, want ErrPastWritten", err)
	}
}

// A recovery reporting 9 whose index lacks 8's record differs from what the oracle expects at 9.
func TestOracleFindsAHole(t *testing.T) {
	o := window(t)
	hole, _ := o.Expect(7)
	hole.Apply(put{1, 9})
	if got, _ := o.Expect(9); maps.Equal(got, hole) {
		t.Fatalf("9 without 8 reads as 9: %v", hole)
	}
}

func TestOracleExpectLeavesTheModel(t *testing.T) {
	o := window(t)
	m, _ := o.Expect(9)
	m[0] = 100
	again, _ := o.Expect(9)
	if again[0] != 8 {
		t.Fatalf("a caller's change to an expected model reached the oracle: %v", again)
	}
	if a, n := o.Window(); a != 7 || n != 9 {
		t.Fatalf("Expect moved the window to %d to %d", a, n)
	}
}

func TestOracleRecovered(t *testing.T) {
	o := window(t)
	m, err := o.Recovered(8)
	if err != nil || !maps.Equal(m, kv{1: 5, 2: 6, 3: 7, 0: 8}) {
		t.Fatalf("Recovered(8) = %v, %v", m, err)
	}
	m[0] = 100
	if a, n := o.Window(); a != 8 || n != 8 {
		t.Fatalf("window after recovering to 8 is %d to %d", a, n)
	}
	if again, err := o.Recovered(8); err != nil || again[0] != 8 {
		t.Fatalf("a second recovery to 8 gives %v, %v", again, err)
	}
	if err := o.Written(10, put{0, 10}); !errors.Is(err, sim.ErrOutOfOrder) {
		t.Fatalf("record 10 after recovering to 8: %v, want ErrOutOfOrder", err)
	}
	if err := o.Written(9, put{0, 90}); err != nil {
		t.Fatal(err)
	}
	if got, _ := o.Expect(9); got[0] != 90 {
		t.Fatalf("the record written at 9 after recovery reads %d at key 0", got[0])
	}
	if _, err := o.Recovered(7); !errors.Is(err, sim.ErrLostAcked) {
		t.Fatalf("Recovered(7) after recovering to 8: %v", err)
	}
}

func TestOracleAckOrder(t *testing.T) {
	o := window(t)
	if err := o.Acked(7); !errors.Is(err, sim.ErrOutOfOrder) {
		t.Fatalf("acknowledging 7 twice: %v", err)
	}
	if err := o.Acked(10); !errors.Is(err, sim.ErrPastWritten) {
		t.Fatalf("acknowledging 10, never written: %v", err)
	}
	if err := o.Written(9, put{}); !errors.Is(err, sim.ErrOutOfOrder) {
		t.Fatalf("writing 9 twice: %v", err)
	}
	if err := o.Acked(9); err != nil {
		t.Fatal(err)
	}
	if got, _ := o.Expect(9); !maps.Equal(got, kv{1: 9, 2: 6, 3: 7, 0: 8}) {
		t.Fatalf("model at 9 after acknowledging 8 and 9 together: %v", got)
	}
}
