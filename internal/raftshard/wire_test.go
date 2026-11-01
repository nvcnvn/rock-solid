package raftshard

import (
	"bytes"
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// FuzzDecode feeds arbitrary bytes as a message, inline or with a buffer: decoding never panics,
// never accepts a type no peer sends, and a message it accepts encodes and decodes back to itself.
func FuzzDecode(f *testing.F) {
	pool := sim.NewPool(1<<20, 0)
	for _, m := range []raftpb.Message{
		{Type: raftpb.MsgHeartbeat, To: 2, From: 1, Term: 3, Commit: 7},
		{Type: raftpb.MsgApp, To: 2, From: 1, Term: 3, Index: 6, LogTerm: 3, Commit: 6, Entries: []raftpb.Entry{{Term: 3, Index: 7, Data: bytes.Repeat([]byte{9}, 400)}}},
	} {
		var msg core.Msg
		encode(pool, testGroup, &m, &msg)
		var body []byte
		if msg.Buf != core.NoBuf {
			body = bytes.Clone(pool.Bytes(msg.Buf)[:m.Size()])
			pool.Free(msg.Buf)
		}
		f.Add(bytes.Clone(msg.Inline()), body, msg.Buf != core.NoBuf)
	}
	f.Fuzz(func(t *testing.T, inline, body []byte, buffered bool) {
		var msg core.Msg
		if err := msg.SetInline(inline); err != nil {
			return
		}
		if buffered {
			b, err := pool.Alloc(len(body) + 1)
			if err != nil {
				t.Fatal(err)
			}
			copy(pool.Bytes(b), body)
			msg.Buf = b
			defer pool.Free(b)
		}
		var m raftpb.Message
		group, err := decode(pool, &msg, &m)
		if err != nil {
			return
		}
		var again core.Msg
		if !encode(pool, group, &m, &again) {
			t.Fatal("no buffer")
		}
		if again.Buf != core.NoBuf {
			defer pool.Free(again.Buf)
		}
		var back raftpb.Message
		if g, err := decode(pool, &again, &back); err != nil || g != group || !bytes.Equal(mustMarshal(&back), mustMarshal(&m)) {
			t.Fatalf("%+v re-encodes to group %d, %+v, err %v", m, g, back, err)
		}
	})
}

func BenchmarkEncode(b *testing.B) {
	pool := sim.NewPool(1<<20, 0)
	for _, size := range []int{128, 4096} {
		m := raftpb.Message{Type: raftpb.MsgApp, To: 2, From: 1, Term: 3, Index: 6, LogTerm: 3, Commit: 6,
			Entries: []raftpb.Entry{{Term: 3, Index: 7, Data: make([]byte, size)}}}
		b.Run(map[int]string{128: "inline", 4096: "buffer"}[size], func(b *testing.B) {
			b.ReportAllocs()
			var msg core.Msg
			for b.Loop() {
				encode(pool, testGroup, &m, &msg)
				if msg.Buf != core.NoBuf {
					pool.Free(msg.Buf)
				}
			}
		})
	}
}
