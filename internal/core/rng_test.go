package core

import "testing"

func TestRandSameSeedSameStream(t *testing.T) {
	a, b := NewRand(0xDEADBEEFCAFEBABE), NewRand(0xDEADBEEFCAFEBABE)
	var fa, fb [13]byte
	for i := range 256 {
		if a.Uint64() != b.Uint64() || a.Uint32() != b.Uint32() || a.Below(97) != b.Below(97) ||
			a.Range(1000, 2000) != b.Range(1000, 2000) {
			t.Fatalf("streams diverge at draw %d", i)
		}
		a.Fill(fa[:])
		b.Fill(fb[:])
		if fa != fb {
			t.Fatalf("fills diverge at draw %d", i)
		}
	}
}

// Changing the generator changes every recorded regression seed; it must fail here first.
func TestRandGoldenStream(t *testing.T) {
	want := [4]uint64{0xac8a366dce7e87d9, 0x6bc727c69e416f1a, 0x1ea1417ca31ffb1b, 0x7494d8486add6aa4}
	r := NewRand(0)
	var got [4]uint64
	for i := range got {
		got[i] = r.Uint64()
	}
	if got != want {
		t.Fatalf("seed 0 stream = %#x, want %#x", got, want)
	}
}

func TestRandFork(t *testing.T) {
	p1, p2 := NewRand(42), NewRand(42)
	c1, c2 := p1.Fork(), p2.Fork()
	for range 64 {
		if c1.Uint64() != c2.Uint64() {
			t.Fatal("children of equal parents differ")
		}
	}
	if p1.Uint64() != p2.Uint64() {
		t.Fatal("parents differ after forking")
	}
	p := NewRand(42)
	c := p.Fork()
	same := 0
	for range 64 {
		if p.Uint64() == c.Uint64() {
			same++
		}
	}
	if same > 0 {
		t.Fatal("child repeats the parent's stream")
	}
}

func TestRandBounds(t *testing.T) {
	r := NewRand(3)
	if r.Below(0) != 0 || r.Below(1) != 0 || r.Range(5, 5) != 5 {
		t.Fatal("degenerate bounds")
	}
	if r.Chance(0, 10) || !r.Chance(10, 10) {
		t.Fatal("chance at the ends")
	}
	for range 512 {
		if v := r.Range(10, 20); v < 10 || v > 20 {
			t.Fatalf("range gave %d", v)
		}
	}
	counts := make([]int, 3)
	for range 30000 {
		counts[r.Below(3)]++
	}
	for i, c := range counts {
		if c < 9000 || c > 11000 {
			t.Fatalf("Below(3) is biased: value %d drawn %d of 30000 times", i, c)
		}
	}
}

func TestRandShuffle(t *testing.T) {
	perm := func() [16]int {
		var p [16]int
		for i := range p {
			p[i] = i
		}
		NewRand(99).Shuffle(16, func(i, j int) { p[i], p[j] = p[j], p[i] })
		return p
	}
	a, b := perm(), perm()
	if a != b {
		t.Fatal("same seed, different permutation")
	}
	var identity [16]int
	for i := range identity {
		identity[i] = i
	}
	if a == identity {
		t.Fatal("shuffle left the identity")
	}
}
