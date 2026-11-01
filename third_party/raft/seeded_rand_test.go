package raft

import (
	crand "crypto/rand"
	"math/big"
	mathrand "math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"

	pb "go.etcd.io/raft/v3/raftpb"
)

func seededDraws(seed uint64, n int) []int {
	cfg := newTestConfig(1, 100, 10, newTestMemoryStorage(withPeers(1, 2, 3)))
	cfg.Rand = mathrand.New(mathrand.NewPCG(seed, 0))
	r := newRaft(cfg)
	draws := make([]int, n)
	for i := range draws {
		r.resetRandomizedElectionTimeout()
		draws[i] = r.randomizedElectionTimeout
	}
	return draws
}

func TestSeededElectionTimeoutsRepeat(t *testing.T) {
	first := seededDraws(1, 100)
	require.Equal(t, first, seededDraws(1, 100))
	require.NotEqual(t, first, seededDraws(2, 100))
}

func TestSeededElectionTimeoutsSpanTheirRange(t *testing.T) {
	seen := map[int]bool{}
	for _, d := range seededDraws(1, 10000) {
		require.GreaterOrEqual(t, d, 100)
		require.LessOrEqual(t, d, 199)
		seen[d] = true
	}
	require.True(t, seen[100] && seen[199], "draws reach both ends of [100, 199]")
}

type panicReader struct{}

func (panicReader) Read([]byte) (int, error) { panic("crypto/rand read") }

func TestSeededElectionsNeverReadCryptoRand(t *testing.T) {
	saved := crand.Reader
	crand.Reader = panicReader{}
	defer func() { crand.Reader = saved }()

	// globalRand draws this way; calling it directly keeps its mutex from staying locked after the panic.
	require.Panics(t, func() { _, _ = crand.Int(crand.Reader, big.NewInt(10)) }, "crypto/rand.Int reads Reader")

	nt := newNetworkWithConfig(func(c *Config) { c.Rand = mathrand.New(mathrand.NewPCG(7, c.ID)) }, nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	require.Equal(t, StateLeader, nt.peers[1].(*raft).state)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("x")}}})

	nt.isolate(1)
	for range 40 {
		for _, id := range []uint64{2, 3} {
			r := nt.peers[id].(*raft)
			r.tick()
			nt.send(nt.filter(r.readMessages())...)
		}
		if nt.peers[2].(*raft).state == StateLeader || nt.peers[3].(*raft).state == StateLeader {
			return
		}
	}
	t.Fatal("no follower of the isolated leader won an election on its own timeout")
}
