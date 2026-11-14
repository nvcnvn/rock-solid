package loop

import (
	"runtime"
	"runtime/metrics"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/core/pooltest"
)

func testPool(t testing.TB, capacity, maxLive, region int) *Pool {
	t.Helper()
	p, err := newPool(capacity, maxLive, region)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	})
	return p
}

func TestPool(t *testing.T) {
	pooltest.Run(t, func(t *testing.T, capacity, maxLive int) pooltest.Pool {
		return testPool(t, capacity, maxLive, MaxRegion)
	})
}

func address(b []byte) uintptr { return uintptr(unsafe.Pointer(unsafe.SliceData(b))) }

func TestBuffersArePageAlignedAcrossRegions(t *testing.T) {
	p := testPool(t, 5*core.BlockSize, 0, 2*core.BlockSize)
	one := mustAlloc(t, p, 1)
	two := mustAlloc(t, p, 2*core.BlockSize)
	if one.Slot != 0 || two.Slot != 2 {
		t.Fatalf("slots %d and %d, want 0 and 2: a buffer crossed a region", one.Slot, two.Slot)
	}
	gap, last := mustAlloc(t, p, 1), mustAlloc(t, p, 1)
	if gap.Slot != 1 || last.Slot != 4 {
		t.Fatalf("slots %d and %d, want 1 and then 4 in the short last region", gap.Slot, last.Slot)
	}
	if _, err := p.Alloc(1); err == nil {
		t.Fatal("allocated past the capacity")
	}
	for i, b := range []core.IOBuf{one, gap, two, last} {
		mem := p.Bytes(b)
		if address(mem)%core.BlockSize != 0 {
			t.Fatalf("buffer %d at %#x", b.Slot, address(mem))
		}
		for j := range mem {
			mem[j] = byte(i + 1)
		}
	}
	for i, b := range []core.IOBuf{one, gap, two, last} {
		for _, c := range p.Bytes(b) {
			if c != byte(i+1) {
				t.Fatalf("buffer %d reads %d, want %d", b.Slot, c, i+1)
			}
		}
	}
}

func mustAlloc(t testing.TB, p *Pool, n int) core.IOBuf {
	t.Helper()
	b, err := p.Alloc(n)
	if err != nil {
		t.Fatalf("Alloc(%d): %v", n, err)
	}
	return b
}

func TestCloseDropsEveryHandle(t *testing.T) {
	p := testPool(t, 2*core.BlockSize, 0, MaxRegion)
	b := mustAlloc(t, p, 1)
	var kept core.BufPool = p.Pool
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	for name, pool := range map[string]core.BufPool{"the pool": p, "a kept *core.Pool": kept} {
		if msg := pooltest.MustPanic(t, func() { pool.Bytes(b) }); !strings.Contains(msg, "beyond the pool's 0 blocks") {
			t.Fatalf("a live handle through %s after Close: %q", name, msg)
		}
	}
	if _, err := p.Alloc(1); err == nil {
		t.Fatal("a closed pool allocated")
	}
}

func TestSlabGrowsWithoutMovingRegions(t *testing.T) {
	s := NewSlab(3 * core.BlockSize)
	defer func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	}()
	first := s.Carve(core.BlockSize + 1)
	if len(first) != 2*core.BlockSize || address(first)%core.BlockSize != 0 {
		t.Fatalf("%d bytes at %#x", len(first), address(first))
	}
	for i := range first {
		first[i] = 7
	}
	at := address(first)
	second := s.Carve(2 * core.BlockSize)
	if s.Mapped() != 6*core.BlockSize || len(s.regions) != 2 {
		t.Fatalf("mapped %d in %d regions", s.Mapped(), len(s.regions))
	}
	clear(second)
	if address(first) != at || address(second)%core.BlockSize != 0 {
		t.Fatal("a region moved or a carve is unaligned")
	}
	for _, c := range first {
		if c != 7 {
			t.Fatal("growth changed an older region's bytes")
		}
	}
	if msg := pooltest.MustPanic(t, func() { s.Carve(4 * core.BlockSize) }); !strings.Contains(msg, "carving") {
		t.Fatalf("carve over a region: %q", msg)
	}
}

func heapObjectBytes() uint64 {
	runtime.GC()
	s := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(s)
	return s[0].Value.Uint64()
}

func fastestCollection() time.Duration {
	best := time.Duration(1 << 62)
	for range 5 {
		start := time.Now()
		runtime.GC()
		best = min(best, time.Since(start))
	}
	return best
}

// poolBlockBytes is the size of core.Pool's entry per block: its generation and run.
const poolBlockBytes = 8

func TestHeapStaysFlatWhileSlabsFill(t *testing.T) {
	const half = 512 << 20
	handles := make([]core.IOBuf, 0, half/core.BlockSize)
	var pattern [core.BlockSize]byte
	for i := range pattern {
		pattern[i] = byte(i)
	}
	empty := fastestCollection()
	before := heapObjectBytes()

	p, err := NewPool(half, 0)
	if err != nil {
		t.Fatal(err)
	}
	pages := NewSlab(MaxRegion)
	defer func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
		if err := pages.Close(); err != nil {
			t.Error(err)
		}
	}()
	for {
		b, err := p.Alloc(core.BlockSize)
		if err != nil {
			break
		}
		copy(p.Bytes(b), pattern[:])
		handles = append(handles, b)
	}
	for range half / (1 << 20) {
		mem := pages.Carve(1 << 20)
		for off := 0; off < len(mem); off += len(pattern) {
			copy(mem[off:], pattern[:])
		}
	}
	if len(handles) != half/core.BlockSize || p.Available() != 0 {
		t.Fatalf("%d buffers, %d bytes left", len(handles), p.Available())
	}

	rise := int64(heapObjectBytes()) - int64(before)
	tables := int64(len(handles))*poolBlockBytes + 64<<10
	t.Logf("heap objects rose by %d bytes, handle tables %d", rise, tables)
	if rise > tables {
		t.Fatalf("heap objects rose by %d bytes with 1 GiB in slabs, over the handle tables' %d", rise, tables)
	}
	full := fastestCollection()
	t.Logf("collection: %v with empty slabs, %v with full", empty, full)
	if full > empty+time.Millisecond {
		t.Fatalf("a collection took %v with full slabs, %v with none", full, empty)
	}
	for _, b := range handles {
		p.Free(b)
	}
}

func allocFree(t testing.TB, p *Pool, live []core.IOBuf) {
	for i := range live {
		p.Free(live[i])
		b, err := p.Alloc(core.BlockSize)
		if err != nil {
			t.Fatal(err)
		}
		p.Bytes(b)[0] = 1
		live[i] = b
	}
}

func TestAllocAndFreeAllocateNothing(t *testing.T) {
	p := testPool(t, 1<<20, 0, MaxRegion)
	live := make([]core.IOBuf, 64)
	for i := range live {
		live[i] = mustAlloc(t, p, core.BlockSize)
	}
	if n := testing.AllocsPerRun(100, func() { allocFree(t, p, live) }); n != 0 {
		t.Fatalf("%v allocations per run", n)
	}
}

func BenchmarkAllocFree(b *testing.B) {
	p := testPool(b, 64<<20, 0, MaxRegion)
	live := make([]core.IOBuf, 4096)
	for i := range live {
		live[i] = mustAlloc(b, p, core.BlockSize)
	}
	b.ReportAllocs()
	for b.Loop() {
		allocFree(b, p, live[:1])
	}
}
