package trace

import (
	"fmt"
)

// OneFsyncPerRound checks that each round has at most one DiskWrite and one DiskFsync, the flush after the write; round 0 is exempt.
func OneFsyncPerRound() Rule {
	return Rule{Name: "one-fsync-per-round", New: func() Scanner { return &fsyncPerRound{} }}
}

type roundActions struct {
	writes map[uint64]bool
	fsyncs map[uint64]bool
}

type fsyncPerRound struct{ loops perLoop[roundActions] }

func (s *fsyncPerRound) Scan(r *Record) string {
	if r.Name != ActDiskWrite && r.Name != ActDiskFsync || r.Fields[0] == 0 {
		return ""
	}
	round := r.Fields[0]
	st := s.loops.get(r)
	if st.writes == nil {
		st.writes, st.fsyncs = make(map[uint64]bool), make(map[uint64]bool)
	}
	if r.Name == ActDiskWrite {
		if st.writes[round] {
			return fmt.Sprintf("round %d has a second DiskWrite", round)
		}
		st.writes[round] = true
		return ""
	}
	if !st.writes[round] {
		return fmt.Sprintf("round %d's DiskFsync comes before its DiskWrite", round)
	}
	if st.fsyncs[round] {
		return fmt.Sprintf("round %d has a second DiskFsync", round)
	}
	st.fsyncs[round] = true
	return ""
}

func (s *fsyncPerRound) End() string { return "" }

// WriteAndFlushTogether checks that a round's DiskWrite emitted with no write in flight has its DiskFsync in the same pass.
func WriteAndFlushTogether() Rule {
	return Rule{Name: "write-and-flush-together", New: func() Scanner { return &together{} }}
}

type togetherState struct {
	inflight     map[uint64]bool
	pending      bool
	pendingRound uint64
	pendingPass  uint64
}

type together struct{ loops perLoop[togetherState] }

func (s *together) Scan(r *Record) string {
	st := s.loops.get(r)
	if st.inflight == nil {
		st.inflight = make(map[uint64]bool)
	}
	if st.pending && r.Pass != st.pendingPass {
		st.pending = false
		return fmt.Sprintf("round %d's DiskWrite went out in pass %d with no write in flight, its DiskFsync did not", st.pendingRound, st.pendingPass)
	}
	switch r.Name {
	case ActDiskWrite:
		if round := r.Fields[0]; round != 0 && len(st.inflight) == 0 {
			st.pending, st.pendingRound, st.pendingPass = true, round, r.Pass
		}
		st.inflight[r.Fields[1]] = true
	case ActDiskFsync:
		if st.pending && r.Fields[0] == st.pendingRound {
			st.pending = false
		}
	case EvDiskDone:
		delete(st.inflight, r.Fields[0])
	}
	return ""
}

func (s *together) End() string { return "" }

// CommitDepth checks that at most depth rounds' DiskWrites are in flight at once.
func CommitDepth(depth int) Rule {
	return Rule{Name: "commit-depth", New: func() Scanner { return &commitDepth{depth: depth} }}
}

type commitDepth struct {
	depth int
	loops perLoop[map[uint64]bool]
}

func (s *commitDepth) Scan(r *Record) string {
	if r.Name != ActDiskWrite && r.Name != EvDiskDone {
		return ""
	}
	st := s.loops.get(r)
	if *st == nil {
		*st = make(map[uint64]bool)
	}
	inflight := *st
	if r.Name == EvDiskDone {
		delete(inflight, r.Fields[0])
		return ""
	}
	if r.Fields[0] == 0 {
		return ""
	}
	if len(inflight) >= s.depth {
		return fmt.Sprintf("round %d's DiskWrite with %d rounds' writes in flight, commit_depth %d", r.Fields[0], len(inflight), s.depth)
	}
	inflight[r.Fields[1]] = true
	return ""
}

func (s *commitDepth) End() string { return "" }

// DurableAfterFlush checks that a round is reported Durable only after a flush that ran after its DiskWrite completed has completed.
func DurableAfterFlush() Rule {
	return Rule{Name: "durable-after-flush", New: func() Scanner { return &durable{} }}
}

type flush struct {
	doneMark  int
	linked    uint64
	hasLinked bool
}

type durableState struct {
	roundWrite    map[uint64]uint64
	writes        map[uint64]bool
	doneIndex     map[uint64]int
	done          int
	flushes       map[uint64]flush
	durablePrefix int
	linkedDurable map[uint64]bool
	lastWrite     uint64
	lastWritePass uint64
	lastWasWrite  bool
}

type durable struct{ loops perLoop[durableState] }

func (s *durable) Scan(r *Record) string {
	switch r.Name {
	case ActDiskWrite, ActDiskFsync, EvDiskDone, OutDurable:
	default:
		if r.Name.Kind() == KindAction {
			s.loops.get(r).lastWasWrite = false
		}
		return ""
	}
	st := s.loops.get(r)
	if st.writes == nil {
		st.roundWrite = make(map[uint64]uint64)
		st.writes = make(map[uint64]bool)
		st.doneIndex = make(map[uint64]int)
		st.flushes = make(map[uint64]flush)
		st.linkedDurable = make(map[uint64]bool)
	}
	switch r.Name {
	case ActDiskWrite:
		io := r.Fields[1]
		st.writes[io] = true
		if r.Fields[0] != 0 {
			st.roundWrite[r.Fields[0]] = io
		}
		st.lastWrite, st.lastWritePass, st.lastWasWrite = io, r.Pass, true
	case ActDiskFsync:
		f := flush{doneMark: st.done}
		if r.Fields[2] == 1 && st.lastWasWrite && st.lastWritePass == r.Pass {
			f.linked, f.hasLinked = st.lastWrite, true
		}
		st.flushes[r.Fields[1]] = f
		st.lastWasWrite = false
	case EvDiskDone:
		io, ok := r.Fields[0], r.Fields[1] == uint64(ResultOK)
		if st.writes[io] && ok {
			st.done++
			st.doneIndex[io] = st.done
		}
		if f, isFlush := st.flushes[io]; isFlush && ok {
			st.durablePrefix = max(st.durablePrefix, f.doneMark)
			if f.hasLinked {
				st.linkedDurable[f.linked] = true
			}
		}
	case OutDurable:
		round := r.Fields[0]
		io, ok := st.roundWrite[round]
		if !ok {
			return fmt.Sprintf("round %d reported durable with no DiskWrite", round)
		}
		if i := st.doneIndex[io]; !st.linkedDurable[io] && (i == 0 || i > st.durablePrefix) {
			return fmt.Sprintf("round %d reported durable before a flush that ran after its DiskWrite io=%d completed", round, io)
		}
	}
	return ""
}

func (s *durable) End() string { return "" }

// NoDurableAfterFailure checks that once a loop's DiskWrite or DiskFsync completes with an error, the loop reports no round Durable until its node restarts.
func NoDurableAfterFailure() Rule {
	return Rule{Name: "no-durable-after-failure", New: func() Scanner { return &afterFailure{} }}
}

type failureState struct {
	issued   map[uint64]bool
	failed   bool
	failedIO uint64
}

type afterFailure struct{ loops perLoop[failureState] }

func (s *afterFailure) Scan(r *Record) string {
	st := s.loops.get(r)
	switch r.Name {
	case EvRestart:
		*st = failureState{}
	case ActDiskWrite, ActDiskFsync:
		if st.issued == nil {
			st.issued = make(map[uint64]bool)
		}
		st.issued[r.Fields[1]] = true
	case EvDiskDone:
		if io := r.Fields[0]; st.issued[io] {
			delete(st.issued, io)
			if r.Fields[1] != uint64(ResultOK) && !st.failed {
				st.failed, st.failedIO = true, io
			}
		}
	case OutDurable:
		if st.failed {
			return fmt.Sprintf("round %d reported durable after io=%d failed", r.Fields[0], st.failedIO)
		}
	}
	return ""
}

func (s *afterFailure) End() string { return "" }

// FreezeAfterCut checks that, after a cut message, each chunk's first write entry follows a freeze entry for that chunk in the same round.
func FreezeAfterCut() Rule {
	return Rule{Name: "freeze-after-cut", New: func() Scanner { return &freeze{} }}
}

type chunkKey struct{ vol, inode, index uint64 }

type roundChunk struct {
	round uint64
	chunk chunkKey
}

type freezeState struct {
	cut     bool
	written map[chunkKey]bool
	frozen  map[roundChunk]bool
}

type freeze struct{ loops perLoop[freezeState] }

func (s *freeze) Scan(r *Record) string {
	switch {
	case r.Name == EvNetRecv && Msg(r.Fields[1]) == MsgCut:
		st := s.loops.get(r)
		st.cut, st.written, st.frozen = true, make(map[chunkKey]bool), make(map[roundChunk]bool)
	case r.Name == ActEntry:
		st := s.loops.get(r)
		if !st.cut {
			return ""
		}
		rc := roundChunk{round: r.Fields[0], chunk: chunkKey{r.Fields[2], r.Fields[3], r.Fields[4]}}
		switch EntryType(r.Fields[1]) {
		case EntryFreeze:
			st.frozen[rc] = true
		case EntryWrite:
			first := !st.written[rc.chunk]
			st.written[rc.chunk] = true
			if first && !st.frozen[rc] {
				return fmt.Sprintf("chunk %d/%d/%d's first write after the cut, in round %d, has no freeze", rc.chunk.vol, rc.chunk.inode, rc.chunk.index, rc.round)
			}
		}
	}
	return ""
}

func (s *freeze) End() string { return "" }

// PeeringBeforeWrite checks that a loop that sent peering messages sends no write until every peer it asked has replied.
func PeeringBeforeWrite() Rule {
	return Rule{Name: "peering-before-write", New: func() Scanner { return &peering{gated: MsgWrite} }}
}

// PeeringBeforeRead checks that a loop that sent peering messages sends no read reply until every peer it asked has replied.
func PeeringBeforeRead() Rule {
	return Rule{Name: "peering-before-read", New: func() Scanner { return &peering{gated: MsgReadReply} }}
}

type peeringState struct {
	waiting map[uint64]bool
}

type peering struct {
	gated Msg
	loops perLoop[peeringState]
}

func (s *peering) Scan(r *Record) string {
	if r.Name != ActNetSend && r.Name != EvNetRecv {
		return ""
	}
	peer, msg := r.Fields[0], Msg(r.Fields[1])
	st := s.loops.get(r)
	switch {
	case r.Name == ActNetSend && msg == MsgPeering:
		if st.waiting == nil {
			st.waiting = make(map[uint64]bool)
		}
		st.waiting[peer] = true
	case r.Name == EvNetRecv && msg == MsgPeeringReply:
		delete(st.waiting, peer)
	case r.Name == ActNetSend && msg == s.gated && len(st.waiting) > 0:
		return fmt.Sprintf("NetSend msg=%s with %d peering replies outstanding", msgText[msg], len(st.waiting))
	}
	return ""
}

func (s *peering) End() string { return "" }

// OneWritevPerFD checks that a pass makes at most one writev per descriptor.
func OneWritevPerFD() Rule {
	return Rule{Name: "one-writev-per-fd", New: func() Scanner { return &writevPerFD{} }}
}

type passFDs struct {
	pass uint64
	fds  map[uint64]uint64
}

func (p *passFDs) at(pass uint64) map[uint64]uint64 {
	if p.fds == nil || p.pass != pass {
		p.pass, p.fds = pass, make(map[uint64]uint64)
	}
	return p.fds
}

type writevPerFD struct{ loops perLoop[passFDs] }

func (s *writevPerFD) Scan(r *Record) string {
	if r.Name != SysWritev {
		return ""
	}
	fds := s.loops.get(r).at(r.Pass)
	fd := r.Fields[0]
	fds[fd]++
	if fds[fd] > 1 {
		return fmt.Sprintf("pass %d has a second writev on fd %d", r.Pass, fd)
	}
	return ""
}

func (s *writevPerFD) End() string { return "" }

// ReadBudget checks that a pass reads at most budget bytes from each descriptor.
func ReadBudget(budget uint64) Rule {
	return Rule{Name: "read-budget", New: func() Scanner { return &readBudget{budget: budget} }}
}

type readBudget struct {
	budget uint64
	loops  perLoop[passFDs]
}

func (s *readBudget) Scan(r *Record) string {
	if r.Name != SysRead {
		return ""
	}
	fds := s.loops.get(r).at(r.Pass)
	fd := r.Fields[0]
	fds[fd] += r.Fields[1]
	if fds[fd] > s.budget {
		return fmt.Sprintf("pass %d read %d bytes from fd %d, over the budget of %d", r.Pass, fds[fd], fd, s.budget)
	}
	return ""
}

func (s *readBudget) End() string { return "" }

// OneEpollWaitPerPass checks that every pass making syscalls makes exactly one epoll_wait, except a loop's first pass, which a wrapped ring may hold only in part.
func OneEpollWaitPerPass() Rule {
	return Rule{Name: "one-epoll-wait-per-pass", New: func() Scanner { return &epollPerPass{} }}
}

type epollState struct {
	seen     bool
	first    bool
	pass     uint64
	syscalls int
	waits    int
}

func (st *epollState) close() string {
	if !st.first && st.syscalls > 0 && st.waits == 0 {
		return fmt.Sprintf("pass %d makes syscalls without an epoll_wait", st.pass)
	}
	return ""
}

type epollPerPass struct{ loops perLoop[epollState] }

func (s *epollPerPass) Scan(r *Record) string {
	st := s.loops.get(r)
	switch {
	case !st.seen:
		*st = epollState{seen: true, first: true, pass: r.Pass}
	case r.Pass != st.pass:
		why := st.close()
		*st = epollState{seen: true, pass: r.Pass}
		if why != "" {
			return why
		}
	}
	if r.Name.Kind() != KindSyscall {
		return ""
	}
	st.syscalls++
	if r.Name == SysEpollWait {
		st.waits++
		if st.waits > 1 {
			return fmt.Sprintf("pass %d has a second epoll_wait", r.Pass)
		}
	}
	return ""
}

func (s *epollPerPass) End() string {
	for i := range s.loops.states {
		if why := s.loops.states[i].close(); why != "" {
			return why
		}
	}
	return ""
}
