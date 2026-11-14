package core

import (
	"slices"
	"testing"
)

func TestTimersFireInDeadlineOrderAndReplace(t *testing.T) {
	for seed := range uint64(200) {
		r := NewRand(seed)
		var tm Timers
		want := map[TimerID]Tick{}
		for range 200 {
			id := TimerID(r.Below(30))
			at := Tick(r.Below(1000))
			tm.Set(id, at)
			want[id] = at
		}
		if tm.Len() != len(want) {
			t.Fatalf("seed %d: %d timers, want %d", seed, tm.Len(), len(want))
		}
		type fired struct {
			at Tick
			id TimerID
		}
		var exp []fired
		for id, at := range want {
			exp = append(exp, fired{at, id})
		}
		slices.SortFunc(exp, func(a, b fired) int {
			if a.at != b.at {
				return int(a.at) - int(b.at)
			}
			return int(a.id) - int(b.id)
		})
		var got []fired
		for now := Tick(0); now < 1000; now += 37 {
			if e, ok := tm.Earliest(); ok && e <= now {
				for {
					id, ok := tm.PopDue(now)
					if !ok {
						break
					}
					got = append(got, fired{want[id], id})
				}
			}
			if e, ok := tm.Earliest(); ok && e <= now {
				t.Fatalf("seed %d: timer at %d left due at %d", seed, e, now)
			}
		}
		for {
			id, ok := tm.PopDue(1 << 40)
			if !ok {
				break
			}
			got = append(got, fired{want[id], id})
		}
		if !slices.Equal(got, exp) {
			t.Fatalf("seed %d: fired %v, want %v", seed, got, exp)
		}
	}
}

func TestTimersAllocateNothingOnceArmed(t *testing.T) {
	var tm Timers
	for id := range TimerID(8) {
		tm.Set(id, Tick(id))
	}
	at := Tick(100)
	if n := testing.AllocsPerRun(100, func() {
		at++
		id, _ := tm.PopDue(at)
		tm.Set(id, at+8)
		tm.Set((id+3)%8, at+2)
	}); n != 0 {
		t.Fatalf("%v allocations per re-arm", n)
	}
}
