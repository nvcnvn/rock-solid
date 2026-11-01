package raftshard

import (
	"fmt"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// chunkBytes is the size of the chunks entry payloads are copied into; a larger payload takes a chunk of its own.
const chunkBytes = 64 << 10

// slot is a retained entry: its term and type, and where its payload lies among the chunks.
type slot struct {
	term  uint64
	typ   raftpb.EntryType
	chunk uint32
	off   uint32
	n     uint32
}

// Storage is a replica's raft.Storage: its hard state, its base (the index, term and
// configuration of its last compaction or restored snapshot) and every entry after the base,
// their payloads in pointer-free chunks the collector does not scan. Its snapshots are built on
// demand at the replica's applied index.
type Storage struct {
	hs   raftpb.HardState
	base raftpb.SnapshotMetadata
	ents []slot
	// chunks[i] is chunk number first+i; the last one takes new payloads.
	chunks [][]byte
	first  uint32
	// build is the shard's snapshot at its applied index, nil before a shard runs on the storage.
	build func() (raftpb.Snapshot, error)
	// pending holds the blob payloads of a round replay has not yet seen whole.
	pending []byte
}

// NewStorage is an empty storage whose base is index 0.
func NewStorage() *Storage { return &Storage{} }

// InitialState is the hard state and the base's configuration.
func (s *Storage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	return s.hs, s.base.ConfState, nil
}

// HardState is the replica's hard state.
func (s *Storage) HardState() raftpb.HardState { return s.hs }

// Base is the index, term and configuration of the last compaction or restored snapshot.
func (s *Storage) Base() raftpb.SnapshotMetadata { return s.base }

// FirstIndex is the index after the base.
func (s *Storage) FirstIndex() (uint64, error) { return s.base.Index + 1, nil }

// LastIndex is the index of the last entry, or the base's.
func (s *Storage) LastIndex() (uint64, error) { return s.last(), nil }

func (s *Storage) last() uint64 { return s.base.Index + uint64(len(s.ents)) }

// Term is entry i's term: raft.ErrCompacted below the base, raft.ErrUnavailable past the last entry.
func (s *Storage) Term(i uint64) (uint64, error) {
	switch {
	case i < s.base.Index:
		return 0, raft.ErrCompacted
	case i == s.base.Index:
		return s.base.Term, nil
	case i > s.last():
		return 0, raft.ErrUnavailable
	}
	return s.ents[i-s.base.Index-1].term, nil
}

// Entries returns the entries [lo, hi), at least one and no more than maxSize bytes encoded, their
// payloads still in the storage's chunks; raft.ErrCompacted for lo at or below the base. A hi past
// the last entry's next is a broken invariant of the caller.
func (s *Storage) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	if lo <= s.base.Index {
		return nil, raft.ErrCompacted
	}
	if hi > s.last()+1 || lo > hi {
		panic(fmt.Sprintf("raftshard: entries [%d, %d) asked of a storage holding (%d, %d]", lo, hi, s.base.Index, s.last()))
	}
	if lo == hi {
		return nil, nil
	}
	out := make([]raftpb.Entry, 0, hi-lo)
	var size uint64
	for i := lo; i < hi; i++ {
		e := s.entry(i)
		size += uint64(e.Size())
		if len(out) > 0 && size > maxSize {
			break
		}
		out = append(out, e)
	}
	return out, nil
}

func (s *Storage) entry(i uint64) raftpb.Entry {
	sl := &s.ents[i-s.base.Index-1]
	e := raftpb.Entry{Term: sl.term, Index: i, Type: sl.typ}
	if sl.n > 0 {
		e.Data = s.chunks[sl.chunk-s.first][sl.off : sl.off+sl.n : sl.off+sl.n]
	}
	return e
}

// Snapshot is a snapshot at the replica's applied index, built when asked, or the base before a shard runs.
func (s *Storage) Snapshot() (raftpb.Snapshot, error) {
	if s.build != nil {
		return s.build()
	}
	return raftpb.Snapshot{Metadata: s.base}, nil
}

// SetHardState replaces the hard state.
func (s *Storage) SetHardState(hs raftpb.HardState) { s.hs = hs }

// ApplySnapshot makes the snapshot's metadata the base and drops every entry; raft.ErrSnapOutOfDate for one not past the base.
func (s *Storage) ApplySnapshot(snap raftpb.Snapshot) error {
	if snap.Metadata.Index <= s.base.Index {
		return raft.ErrSnapOutOfDate
	}
	s.base = snap.Metadata
	s.ents, s.chunks, s.first = s.ents[:0], nil, 0
	return nil
}

// Append adds entries, replacing those from the first one's index on; entries at or below the
// base are skipped, and a gap after the last entry is a broken invariant of the caller. Entries
// all at or below the base replaced every entry after it, which only a replay meets: a live append
// starts above the commit index.
func (s *Storage) Append(ents []raftpb.Entry) {
	if len(ents) == 0 {
		return
	}
	if ents[len(ents)-1].Index <= s.base.Index {
		s.ents = s.ents[:0]
		return
	}
	for ents[0].Index <= s.base.Index {
		ents = ents[1:]
	}
	if ents[0].Index > s.last()+1 {
		panic(fmt.Sprintf("raftshard: entry %d appended after last entry %d", ents[0].Index, s.last()))
	}
	s.ents = s.ents[:ents[0].Index-s.base.Index-1]
	for i := range ents {
		s.ents = append(s.ents, s.copyIn(&ents[i]))
	}
}

func (s *Storage) copyIn(e *raftpb.Entry) slot {
	sl := slot{term: e.Term, typ: e.Type, n: uint32(len(e.Data))}
	if sl.n == 0 {
		return sl
	}
	n := len(s.chunks)
	if n == 0 || len(s.chunks[n-1])+len(e.Data) > cap(s.chunks[n-1]) {
		s.chunks = append(s.chunks, make([]byte, 0, max(chunkBytes, len(e.Data))))
		n++
	}
	c := &s.chunks[n-1]
	sl.chunk, sl.off = s.first+uint32(n-1), uint32(len(*c))
	*c = append(*c, e.Data...)
	return sl
}

// Compact makes entry i the base, with cs the configuration at it, dropping it and every entry
// before it and the chunks only they used; raft.ErrCompacted for i at or below the base. An i past
// the last entry is a broken invariant of the caller.
func (s *Storage) Compact(i uint64, cs raftpb.ConfState) error {
	if i <= s.base.Index {
		return raft.ErrCompacted
	}
	if i > s.last() {
		panic(fmt.Sprintf("raftshard: compaction to %d past last entry %d", i, s.last()))
	}
	k := i - s.base.Index
	s.base = raftpb.SnapshotMetadata{Index: i, Term: s.ents[k-1].term, ConfState: cs}
	s.ents = append(s.ents[:0], s.ents[k:]...)
	drop := uint32(max(len(s.chunks)-1, 0))
	for j := range s.ents {
		if s.ents[j].n > 0 {
			drop = s.ents[j].chunk - s.first
			break
		}
	}
	clear(s.chunks[:drop])
	s.chunks = append(s.chunks[:0], s.chunks[drop:]...)
	s.first += drop
	return nil
}
