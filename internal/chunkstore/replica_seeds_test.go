package chunkstore

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

// replicatedSubBlocks is the sub-block writes spike's workload on a replica primary: clients each pipelining writes of 1 to maxLen bytes at random offsets in the first span bytes of one chunk.
type replicatedSubBlocks struct {
	clients, pipeline, writes int
	maxLen, span              uint32
	// mergeCommitted plants the spike's M2 and M3 bug in the primary's store: merging on the committed version.
	mergeCommitted bool
}

var errOracle = errors.New("a disk differs from the acknowledged writes applied in version order")

type subClient struct {
	c    *rcluster
	h    *sim.History
	rng  *core.Rand
	run  *replicatedSubBlocks
	id   uint32
	left int
	ids  []core.RequestID
	ops  *[]subOp
}

type subOp struct {
	off  uint32
	data []byte
}

func (sc *subClient) Ready() bool {
	if sc.left == 0 {
		return false
	}
	out := 0
	for _, id := range sc.ids {
		if _, ok := sc.c.cl.replies[id]; !ok {
			out++
		}
	}
	return out < sc.run.pipeline
}

func (sc *subClient) Next(ctx *core.Context, acts []core.Action) []core.Action {
	sc.left--
	off := uint32(sc.rng.Below(uint64(sc.run.span)))
	n := min(1+uint32(sc.rng.Below(uint64(sc.run.maxLen))), core.ChunkSize-off)
	data := make([]byte, n)
	for i := range data {
		data[i] = byte(sc.rng.Below(255) + 1)
	}
	op := sc.h.Invoke(sc.id, [sim.OpWords]uint64{uint64(off), uint64(n)})
	*sc.ops = append(*sc.ops, subOp{off: off, data: data})
	id := core.RequestID(op + 1)
	sc.ids = append(sc.ids, id)
	ch := chunkIn(pg0, 4, 0)
	u := Update{Kind: KindWrite, ID: id, PG: sc.c.pgFields(pg0), Session: 1, Incarnation: 1, Chunk: ch, Off: off, Len: n}
	a := core.Action{Kind: core.ActNetSend, Peer: core.NodeID{Node: uint32(sc.c.primary(pg0))}}
	a.Msg.Len = uint16(copy(a.Msg.Body[:], AppendUpdate(nil, &u)))
	buf, err := ctx.Pool.AllocZeroed(dataBytes(off, n))
	if err != nil {
		panic(err)
	}
	copy(ctx.Pool.Bytes(buf)[off%core.BlockSize:], data)
	a.Msg.Buf = buf
	sc.c.cl.sent[id] = buf
	return append(acts, a)
}

// clientReply decodes a replica primary's reply to a client's write as the history's operation ID − 1, with its version and status.
func clientReply(m *core.Msg) (int, [sim.OpWords]uint64, bool) {
	var rp Response
	if m.Len == 0 || DecodeResponse(m.Inline(), 0, &rp) != nil || rp.Kind != KindReply {
		return 0, [sim.OpWords]uint64{}, false
	}
	return int(rp.ID) - 1, [sim.OpWords]uint64{rp.Version, uint64(rp.Status)}, true
}

// run reports the first byte where the primary's reads or a member's disk differ from the acknowledged writes applied in version order.
func (r replicatedSubBlocks) run(t *testing.T, seed uint64) error {
	s := sim.New(seed, sim.DefaultSettings())
	h := sim.NewHistory(s, r.clients*r.writes)
	h.Replies(clientReply)
	c := newRClusterOn(t, rtable(4, 2), s)
	c.reps[c.primary(pg0)].Store().plant.mergeCommitted = r.mergeCommitted
	rng := core.NewRand(seed)
	var ops []subOp
	for i := range r.clients {
		c.s.Node(c.client.Node).AddActor(c.client, &subClient{c: c, h: h, rng: rng.Fork(), run: &r, id: uint32(i), left: r.writes, ops: &ops})
	}
	c.settle()
	type acked struct {
		version uint64
		op      int
	}
	var done []acked
	for i, op := range h.Ops() {
		if !op.Done || Status(op.Output[1]) != StatusOK {
			return fmt.Errorf("write %d answered %+v", i, op)
		}
		done = append(done, acked{op.Output[0], i})
	}
	if len(done) != r.clients*r.writes {
		return fmt.Errorf("%d writes of %d made", len(done), r.clients*r.writes)
	}
	slices.SortFunc(done, func(a, b acked) int { return cmp.Compare(a.version, b.version) })
	want := make([]byte, r.span+r.maxLen+core.BlockSize)
	for _, a := range done {
		copy(want[ops[a.op].off:], ops[a.op].data)
	}
	ch := chunkIn(pg0, 4, 0)
	blocks := uint16(len(want) / core.BlockSize)
	for d := range 3 {
		for b := range blocks {
			if got := c.diskBlock(d, ch, b); !bytes.Equal(got, want[int(b)*core.BlockSize:][:core.BlockSize]) {
				i := 0
				for got[i] == want[int(b)*core.BlockSize+i] {
					i++
				}
				return fmt.Errorf("%w: disk %d block %d byte %d is %#x, the writes give %#x", errOracle, d+1, b, i, got[i], want[int(b)*core.BlockSize+i])
			}
		}
	}
	return nil
}

// Sub-block spike M1 and M9: two clients pipelining 8 writes each, of up to 300 bytes or 3 blocks.
func TestSubBlockWritesMatchTheByteOracle(t *testing.T) {
	for _, r := range []replicatedSubBlocks{
		{clients: 2, pipeline: 8, writes: 24, maxLen: 300, span: 3 * core.BlockSize},
		{clients: 2, pipeline: 8, writes: 24, maxLen: 3 * core.BlockSize, span: 3 * core.BlockSize},
	} {
		seeds.Run(t, 0, 50, func(seed uint64) {
			if err := r.run(t, seed); err != nil {
				t.Fatalf("seed %d, writes up to %d bytes: %v", seed, r.maxLen, err)
			}
		})
	}
}

// Sub-block spike M2 and M3: merging on the committed version loses bytes with one client pipelining 4 writes, or two clients with one write each.
func TestMergingOnTheCommittedVersionLosesBytes(t *testing.T) {
	for _, r := range []replicatedSubBlocks{
		{clients: 1, pipeline: 4, writes: 8, maxLen: 300, span: core.BlockSize, mergeCommitted: true},
		{clients: 2, pipeline: 1, writes: 8, maxLen: 300, span: core.BlockSize, mergeCommitted: true},
	} {
		found := false
		for seed := uint64(0); seed < 500 && !found; seed++ {
			err := r.run(t, seed)
			if found = err != nil; found && !errors.Is(err, errOracle) {
				t.Fatalf("seed %d: %v", seed, err)
			}
		}
		if !found {
			t.Fatalf("%d clients pipelining %d: no seed of 500 lost a byte", r.clients, r.pipeline)
		}
	}
}
