package raftshard

import (
	"bytes"
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// FuzzDecode feeds arbitrary bytes as a message, inline or with a buffer: decoding never panics,
// never accepts a type no peer sends, and a message it accepts, Raft or fortification, encodes and
// decodes back to itself.
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
	for _, fo := range []fortify{{kind: kindFortify, from: 1, to: 2, term: 3}, {kind: kindFortified, from: 2, to: 1, term: 3, run: 9, support: 4}, {kind: kindMember, from: 3, to: 1}, {kind: kindRemoved, from: 1, to: 3, term: 40}} {
		var msg core.Msg
		encodeFortify(testGroup, &fo, &msg)
		f.Add(bytes.Clone(msg.Inline()), []byte(nil), false)
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
		var fo fortify
		group, kind, err := decode(pool, &msg, &m, &fo)
		if err != nil {
			return
		}
		var again core.Msg
		if kind != kindRaft {
			encodeFortify(group, &fo, &again)
		} else if !encode(pool, group, &m, &again) {
			t.Fatal("no buffer")
		}
		if again.Buf != core.NoBuf {
			defer pool.Free(again.Buf)
		}
		var back raftpb.Message
		var fb fortify
		g, k, err := decode(pool, &again, &back, &fb)
		if err != nil || g != group || k != kind || fb != fo || !bytes.Equal(mustMarshal(&back), mustMarshal(&m)) {
			t.Fatalf("%+v %+v re-encodes to group %d kind %d, %+v %+v, err %v", m, fo, g, k, back, fb, err)
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
