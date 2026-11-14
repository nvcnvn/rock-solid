package core

import (
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

// TestPoolIsFirstFit checks the pool's slots against a plain first-fit scan over the same
// allocations and frees, with buffers that never cross a region.
func TestPoolIsFirstFit(t *testing.T) {
	seeds.Run(t, 1, 50, func(seed uint64) {
		r := NewRand(seed)
		const perRegion, regions = 8, 3
		mem := make([][]byte, regions)
		for i := range mem {
			mem[i] = make([]byte, perRegion*BlockSize)
		}
		mem[regions-1] = mem[regions-1][:5*BlockSize]
		p := NewPool(mem, 0)
		taken := make([]bool, len(p.blocks))
		var live []IOBuf
		for step := range 400 {
			if len(live) > 0 && r.Chance(1, 2) {
				i := int(r.Below(uint64(len(live))))
				b := live[i]
				for j := range p.blocks[b.Slot].run {
					taken[b.Slot+j] = false
				}
				p.Free(b)
				live = append(live[:i], live[i+1:]...)
				continue
			}
			want := 1 + int(r.Below(4))
			expect := -1
			for s := 0; s+want <= len(taken) && expect < 0; s++ {
				ok := s/perRegion == (s+want-1)/perRegion
				for j := s; j < s+want && ok; j++ {
					ok = !taken[j]
				}
				if ok {
					expect = s
				}
			}
			b, err := p.Alloc(want * BlockSize)
			if expect < 0 {
				if err == nil {
					t.Fatalf("step %d: allocated slot %d where first fit finds no run of %d", step, b.Slot, want)
				}
				continue
			}
			if err != nil || int(b.Slot) != expect {
				t.Fatalf("step %d: run of %d at slot %d (%v), first fit says %d", step, want, b.Slot, err, expect)
			}
			for j := expect; j < expect+want; j++ {
				taken[j] = true
			}
			live = append(live, b)
		}
	})
}

func TestNewPoolRefusesRegionsItCannotAddress(t *testing.T) {
	block := func(n int) []byte { return make([]byte, n*BlockSize) }
	for _, c := range []struct {
		name    string
		regions [][]byte
		want    string
	}{
		{"a region under a block", [][]byte{block(2), make([]byte, BlockSize-1)}, "under a block"},
		{"a last region larger than the first", [][]byte{block(2), block(3)}, "more than region 0's"},
		{"a short region before the last", [][]byte{block(2), block(1), block(2)}, "follows a short region"},
	} {
		func() {
			defer func() {
				if msg, _ := recover().(string); !strings.Contains(msg, c.want) {
					t.Errorf("%s: panic %q, want %q", c.name, msg, c.want)
				}
			}()
			NewPool(c.regions, 0)
		}()
	}
}
