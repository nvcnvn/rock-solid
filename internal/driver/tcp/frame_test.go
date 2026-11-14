package tcp

import (
	"bytes"
	"errors"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

func testPool(n int) *core.Pool { return core.NewPool([][]byte{make([]byte, n)}, 0) }

// encode is a frame of kind with inline and data, as putFrame writes it.
func encode(kind byte, inline, data []byte) []byte {
	b := make([]byte, frameLen(len(inline), len(data)))
	putFrame(b, kind, inline, data)
	return b
}

// decodeAll reads stream into a decoder at most piece bytes a read and returns its frames'
// inline bytes and data, and its error.
func decodeAll(stream []byte, piece int, pool *core.Pool) ([][]byte, [][]byte, error) {
	var d decoder
	var f frame
	var inl, data [][]byte
	scratch := make([]byte, aheadBytes)
	var in []byte
	for {
		ok, err := d.take(pool, &in, &f)
		if err != nil {
			return inl, data, err
		}
		if ok {
			inl = append(inl, bytes.Clone(f.inline))
			var b []byte
			if f.data != core.NoBuf {
				b = bytes.Clone(pool.Bytes(f.data))
				pool.Free(f.data)
			}
			data = append(data, b)
			continue
		}
		if len(stream) == 0 {
			break
		}
		v, _ := d.space(pool, scratch)
		k := 0
		for _, b := range v {
			m := copy(b, stream[k:min(piece, len(stream))])
			k += m
			if m < len(b) {
				break
			}
		}
		stream = stream[k:]
		in = scratch[:d.read(k)]
	}
	d.reset(pool)
	return inl, data, nil
}

func TestFramesDecodeFromAnyPieces(t *testing.T) {
	block := bytes.Repeat([]byte{3}, core.BlockSize)
	var stream []byte
	stream = append(stream, encode(kindHello, putHello(make([]byte, helloLen), core.NodeID{Node: 4, Loop: 2}, 9), nil)...)
	stream = append(stream, encode(kindMsg, []byte("inline only"), nil)...)
	stream = append(stream, encode(kindMsg, bytes.Repeat([]byte{1}, core.MaxInlineMsg), block)...)
	stream = append(stream, encode(kindMsg, nil, block)...)
	for _, piece := range []int{1, 5, 12, 13, 100, 4096, len(stream)} {
		pool := testPool(1 << 20)
		inl, data, err := decodeAll(stream, piece, pool)
		if err != nil || len(inl) != 4 || string(inl[1]) != "inline only" || !bytes.Equal(data[2], block) || len(inl[3]) != 0 || !bytes.Equal(data[3], block) {
			t.Fatalf("pieces of %d: %d frames, err %v", piece, len(inl), err)
		}
		if id, inc := getHello(inl[0]); id != (core.NodeID{Node: 4, Loop: 2}) || inc != 9 {
			t.Fatalf("hello %v %d", id, inc)
		}
		if pool.Live() != 0 {
			t.Fatalf("pieces of %d: %d buffers live", piece, pool.Live())
		}
	}
}

func TestBadFramesAreRefused(t *testing.T) {
	good := encode(kindMsg, []byte("x"), []byte("data"))
	mut := func(f func(b []byte)) []byte {
		b := bytes.Clone(good)
		f(b)
		return b
	}
	for _, tc := range []struct {
		name  string
		frame []byte
		pool  int
		want  error
	}{
		{"version", mut(func(b []byte) { b[0] = 2 }), 1 << 20, ErrFrame},
		{"kind", mut(func(b []byte) { b[1] = 9 }), 1 << 20, ErrFrame},
		{"inline over 256", mut(func(b []byte) { b[2], b[3] = 1, 1 }), 1 << 20, ErrFrame},
		{"data over the limit", mut(func(b []byte) { b[7] = 0x10 }), 1 << 20, ErrFrame},
		{"hello with data", encode(kindHello, make([]byte, helloLen), []byte{1}), 1 << 20, ErrFrame},
		{"short hello", encode(kindHello, make([]byte, 4), nil), 1 << 20, ErrFrame},
		{"checksum", mut(func(b []byte) { b[len(b)-1] ^= 1 }), 1 << 20, ErrChecksum},
		{"pool exhausted", encode(kindMsg, nil, make([]byte, 5000)), core.BlockSize, core.ErrPoolExhausted},
	} {
		pool := testPool(tc.pool)
		_, _, err := decodeAll(tc.frame, 3, pool)
		if !errors.Is(err, tc.want) || pool.Live() != 0 {
			t.Errorf("%s: %v, %d buffers live; want %v", tc.name, err, pool.Live(), tc.want)
		}
	}
}

func FuzzDecoder(f *testing.F) {
	f.Add(encode(kindMsg, []byte("x"), []byte("data")), uint8(3))
	f.Add(encode(kindHello, make([]byte, helloLen), nil), uint8(1))
	f.Fuzz(func(t *testing.T, stream []byte, piece uint8) {
		pool := testPool(64 << 10)
		whole, wdata, werr := decodeAll(stream, len(stream)+1, pool)
		inl, data, err := decodeAll(stream, int(piece)+1, pool)
		if (err == nil) != (werr == nil) || len(inl) != len(whole) {
			t.Fatalf("pieces of %d: %d frames, %v; whole: %d, %v", piece, len(inl), err, len(whole), werr)
		}
		for i := range inl {
			if !bytes.Equal(inl[i], whole[i]) || !bytes.Equal(data[i], wdata[i]) {
				t.Fatalf("frame %d differs", i)
			}
		}
		if pool.Live() != 0 {
			t.Fatalf("%d buffers live", pool.Live())
		}
	})
}

func TestManySmallFramesArriveInOneRead(t *testing.T) {
	var stream []byte
	for i := range 300 {
		stream = append(stream, encode(kindMsg, []byte("heartbeat"), bytes.Repeat([]byte{byte(i)}, 100))...)
	}
	pool := testPool(1 << 20)
	scratch := make([]byte, aheadBytes)
	var d decoder
	v, size := d.space(pool, scratch)
	if len(v) != 1 || size < len(stream) {
		t.Fatalf("one read takes %d bytes, the frames are %d", size, len(stream))
	}
	in := scratch[:d.read(copy(v[0], stream))]
	var f frame
	for i := range 300 {
		ok, err := d.take(pool, &in, &f)
		if !ok || err != nil || string(f.inline) != "heartbeat" || pool.Bytes(f.data)[99] != byte(i) {
			t.Fatalf("frame %d: %v %v", i, ok, err)
		}
		pool.Free(f.data)
	}
	if ok, _ := d.take(pool, &in, &f); ok || len(in) != 0 || pool.Live() != 0 {
		t.Fatalf("left %d bytes, %d buffers", len(in), pool.Live())
	}
}

func TestALargeFrameCopiesAtMostCopyMaxOutOfTheReadAhead(t *testing.T) {
	big := bytes.Repeat([]byte{5}, 1<<20)
	var stream []byte
	for range 3 {
		stream = append(stream, encode(kindMsg, nil, big)...)
	}
	pool := testPool(4 << 20)
	scratch := make([]byte, aheadBytes)
	var d decoder
	var f frame
	var in []byte
	frames := 0
	for frames < 3 {
		fresh := d.want == 0 || d.out
		ok, err := d.take(pool, &in, &f)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			if !bytes.Equal(pool.Bytes(f.data)[:len(big)], big) {
				t.Fatalf("frame %d data differs", frames)
			}
			pool.Free(f.data)
			frames++
			continue
		}
		if fresh && frames > 0 && d.got > copyMax {
			t.Fatalf("frame %d: %d bytes copied out of the read-ahead", frames, d.got)
		}
		v, _ := d.space(pool, scratch)
		k := 0
		for _, b := range v {
			k += copy(b, stream[k:])
		}
		stream = stream[k:]
		in = scratch[:d.read(k)]
	}
}
