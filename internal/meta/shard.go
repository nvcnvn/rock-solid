package meta

import (
	"fmt"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/lsm"
	"github.com/nvcnvn/rock-solid/internal/raftshard"
)

// maxBatchBytes is max_group_commit_bytes, the most Txns one log entry carries.
const maxBatchBytes = 1 << 20

// A log entry is a format byte, then its Txns' encodings back to back.
const entryFormat = 1

// Config is a shard replica's settings.
type Config struct {
	// Raft is the replica's Raft settings, whose SnapshotData, Apply and CheckSnapshot the shard sets.
	Raft raftshard.Config
	// Tree is the settings of the state's LSM tree, with MaxPiece at least 4 KiB.
	Tree lsm.Config
}

// Answer is the outcome of the Txn proposed under Token.
type Answer struct {
	Token  uint64
	Result Result
	// Err is raft.ErrProposalDropped for a Txn that may or may not have applied, or ErrMalformed.
	Err error
}

// Output is what one call into a shard hands its driver: the Raft replica's output and the answers to Txns this replica proposed.
type Output struct {
	Raft    raftshard.Output
	Answers []Answer
}

// Reset empties the output for reuse, keeping each answer's buffers.
func (o *Output) Reset() {
	o.Raft.Reset()
	o.Answers = o.Answers[:0]
}

// Shard is one replica of a MetaStore shard: its Raft replica and its state machine.
type Shard struct {
	raft  *raftshard.Shard
	state *State
	id    uint64
	// out is the output of the call in progress, which answers go to.
	out *Output
	// batch holds the Txns gathered for the next entry, and queued their tokens.
	batch  []byte
	queued []uint64
	// waits are the proposed entries not yet applied, in log order, and sent their Txns' tokens from head.
	waits []proposal
	sent  []uint64
	head  int
	free  [][]byte
	txn   Txn
	res   Result
}

// proposal is an entry this replica proposed as leader: its term, its Txn count, and its buffer,
// which Raft holds until the entry applies or a later term's does.
type proposal struct {
	term uint64
	n    int
	buf  []byte
}

// NewShard starts a replica from its Raft storage, its state at the storage's base: the restored snapshot's a replay made it, or else empty.
func NewShard(cfg Config) (*Shard, error) {
	base := cfg.Raft.Storage.Base().Index
	st, err := NewState(cfg.Tree, base)
	if err != nil {
		return nil, err
	}
	if data := cfg.Raft.Storage.TakeBaseData(); data != nil {
		if err := st.Restore(base, data); err != nil {
			return nil, err
		}
	}
	s := &Shard{state: st, id: cfg.Raft.ID}
	cfg.Raft.SnapshotData, cfg.Raft.Apply, cfg.Raft.CheckSnapshot = st.SnapshotData, s.apply, CheckSnapshot
	if s.raft, err = raftshard.New(cfg.Raft); err != nil {
		return nil, err
	}
	return s, nil
}

// Raft is the replica, for calls that hand no entries over: Start, OnEvent, Durable and ReadIndex go through the shard.
func (s *Shard) Raft() *raftshard.Shard { return s.raft }

// State is the replica's state machine.
func (s *Shard) State() *State { return s.state }

// Start starts the replica's clock, as raftshard.Shard.Start.
func (s *Shard) Start(ctx *core.Context, out *Output) {
	s.out = out
	s.raft.Start(ctx, &out.Raft)
}

// OnEvent hands the replica an event, as raftshard.Shard.OnEvent.
func (s *Shard) OnEvent(ctx *core.Context, ev *core.Event, out *Output) {
	s.out = out
	s.raft.OnEvent(ctx, ev, &out.Raft)
	s.dropIfRemoved()
}

// ReadIndex asks for a read's commit index, as raftshard.Shard.ReadIndex.
func (s *Shard) ReadIndex(ctx *core.Context, rctx []byte, out *Output) {
	s.out = out
	s.raft.ReadIndex(ctx, rctx, &out.Raft)
	s.dropIfRemoved()
}

// Durable reports the round in flight durable, as raftshard.Shard.Durable, after proposing the Txns gathered during it.
func (s *Shard) Durable(ctx *core.Context, round uint64, out *Output) {
	s.out = out
	// The Txns that arrived during the round go out as one entry, which the next round carries.
	if len(s.queued) > 0 {
		s.propose(ctx)
	}
	s.raft.Durable(ctx, round, &out.Raft)
	s.dropIfRemoved()
}

// Txn proposes t, which an Answer under token settles, or refuses it with Check's error or, off the leader, raft.ErrProposalDropped.
func (s *Shard) Txn(ctx *core.Context, t *Txn, token uint64, out *Output) error {
	// A Txn is proposed at once when no round is in flight, or else gathered with the others
	// arriving during the round into one entry, which the round's Durable proposes.
	n, err := t.Check()
	if err != nil {
		return err
	}
	if s.raft.Leader() != s.id {
		return raft.ErrProposalDropped
	}
	s.out = out
	if len(s.batch)+n > maxBatchBytes {
		s.propose(ctx)
	}
	if len(s.batch) == 0 {
		s.batch = append(s.takeBuf(), entryFormat)
	}
	s.batch = appendTxn(s.batch, t)
	s.queued = append(s.queued, token)
	if !s.raft.Flushing() {
		s.propose(ctx)
	}
	s.dropIfRemoved()
	return nil
}

// dropIfRemoved answers every Txn waiting or gathered dropped once the replica learns it left its group, as it does nothing more.
func (s *Shard) dropIfRemoved() {
	if !s.out.Raft.Removed {
		return
	}
	for len(s.waits) > 0 {
		s.settle(0, raft.ErrProposalDropped)
	}
	for _, token := range s.queued {
		s.answer(token).Err = raft.ErrProposalDropped
	}
	s.batch, s.queued = nil, s.queued[:0]
}

// Scan reads [start, end) from the replica's applied state, as State.Scan; whether a replica may serve it is its caller's to decide.
func (s *Shard) Scan(start, end []byte, limit int, r *ScanResult) error {
	return s.state.Scan(start, end, limit, r)
}

func (s *Shard) propose(ctx *core.Context) {
	term := s.raft.Term()
	if err := s.raft.Propose(ctx, s.batch, &s.out.Raft); err != nil {
		for _, token := range s.queued {
			s.answer(token).Err = err
		}
		s.free = append(s.free, s.batch[:0])
	} else {
		s.waits = append(s.waits, proposal{term: term, n: len(s.queued), buf: s.batch})
		s.sent = append(s.sent, s.queued...)
	}
	s.batch, s.queued = nil, s.queued[:0]
}

func (s *Shard) takeBuf() []byte {
	if n := len(s.free); n > 0 {
		b := s.free[n-1]
		s.free = s.free[:n-1]
		return b
	}
	return make([]byte, 0, 4<<10)
}

// apply restores a snapshot and applies committed entries as the Raft replica hands them over.
func (s *Shard) apply(snap raftpb.Snapshot, ents []raftpb.Entry) {
	if !raft.IsEmptySnap(snap) {
		if err := s.state.Restore(snap.Metadata.Index, snap.Data); err != nil {
			panic(fmt.Sprintf("meta: a snapshot that passed CheckSnapshot does not restore: %v", err))
		}
		for len(s.waits) > 0 {
			s.settle(0, raft.ErrProposalDropped)
		}
	}
	for i := range ents {
		s.applyEntry(&ents[i])
	}
}

// applyEntry applies e's Txns, answering those this replica proposed. A proposal of an earlier
// term than e's still waiting never committed, since a log's terms only rise.
func (s *Shard) applyEntry(e *raftpb.Entry) {
	s.state.applied = e.Index
	for len(s.waits) > 0 && s.waits[0].term < e.Term {
		s.settle(0, raft.ErrProposalDropped)
	}
	if e.Type != raftpb.EntryNormal || len(e.Data) == 0 {
		return
	}
	mine := len(s.waits) > 0 && s.waits[0].term == e.Term
	b, n := e.Data[1:], 0
	if e.Data[0] != entryFormat {
		b = nil
	}
	var err error
	for ; len(b) > 0; n++ {
		if b, err = decodeTxn(b, &s.txn); err != nil {
			break
		}
		if !mine {
			_ = s.state.Apply(&s.txn, &s.res)
			continue
		}
		a := s.answer(s.sent[s.head])
		s.head++
		a.Err = s.state.Apply(&s.txn, &a.Result)
	}
	if mine {
		s.settle(n, ErrMalformed)
	}
}

// settle answers the oldest proposal's Txns from the done-th on with err, and frees its buffer.
func (s *Shard) settle(done int, err error) {
	p := s.waits[0]
	for range p.n - done {
		s.answer(s.sent[s.head]).Err = err
		s.head++
	}
	s.free = append(s.free, p.buf[:0])
	s.waits = append(s.waits[:0], s.waits[1:]...)
	if s.head == len(s.sent) {
		s.sent, s.head = s.sent[:0], 0
	}
}

// answer is a fresh answer for token in the call's output, reusing an earlier one's buffers.
func (s *Shard) answer(token uint64) *Answer {
	o := s.out
	if n := len(o.Answers); n < cap(o.Answers) {
		o.Answers = o.Answers[:n+1]
	} else {
		o.Answers = append(o.Answers, Answer{})
	}
	a := &o.Answers[len(o.Answers)-1]
	a.Token, a.Err = token, nil
	a.Result.FailedCheck, a.Result.Actual, a.Result.Updated = -1, nil, a.Result.Updated[:0]
	return a
}
