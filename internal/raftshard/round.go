package raftshard

import (
	"errors"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// Round is what one round makes durable: a restored snapshot, entries and a hard state, any of them empty.
type Round struct {
	HardState raftpb.HardState
	Entries   []raftpb.Entry
	Snapshot  raftpb.Snapshot
}

// A round's record is a flags byte, then, if flagged, the snapshot (its index, term, configuration
// and data), the entries (the first index, the count, and each entry's term, type, length and
// payload) and the hard state (term, vote, commit), little-endian, in that order: replaying a
// snapshot drops the entries before it, and a hard state's commit index needs its entries. It goes
// to the log as blobs of at most a record each, every one but the last of kind BlobRaftEntries
// and the last of kind BlobRaftState, so a round torn at the log's tail is dropped whole.
const (
	flagSnapshot byte = 1 << iota
	flagEntries
	flagHardState
)

// ErrBadRound reports a round record that does not decode.
var ErrBadRound = errors.New("raftshard: malformed round record")

// AppendRound appends r's record to dst.
func AppendRound(dst []byte, r *Round) []byte {
	var flags byte
	if r.Snapshot.Metadata.Index != 0 {
		flags |= flagSnapshot
	}
	if len(r.Entries) > 0 {
		flags |= flagEntries
	}
	if r.HardState.Term != 0 || r.HardState.Vote != 0 || r.HardState.Commit != 0 {
		flags |= flagHardState
	}
	dst = append(dst, flags)
	if flags&flagSnapshot != 0 {
		m := &r.Snapshot.Metadata
		dst = le.AppendUint64(le.AppendUint64(dst, m.Index), m.Term)
		for _, ids := range [...][]uint64{m.ConfState.Voters, m.ConfState.Learners, m.ConfState.VotersOutgoing, m.ConfState.LearnersNext} {
			dst = le.AppendUint32(dst, uint32(len(ids)))
			for _, id := range ids {
				dst = le.AppendUint64(dst, id)
			}
		}
		var auto byte
		if m.ConfState.AutoLeave {
			auto = 1
		}
		dst = append(dst, auto)
		dst = append(le.AppendUint32(dst, uint32(len(r.Snapshot.Data))), r.Snapshot.Data...)
	}
	if flags&flagEntries != 0 {
		dst = le.AppendUint32(le.AppendUint64(dst, r.Entries[0].Index), uint32(len(r.Entries)))
		for i := range r.Entries {
			e := &r.Entries[i]
			dst = append(le.AppendUint64(dst, e.Term), byte(e.Type))
			dst = append(le.AppendUint32(dst, uint32(len(e.Data))), e.Data...)
		}
	}
	if flags&flagHardState != 0 {
		hs := &r.HardState
		dst = le.AppendUint64(le.AppendUint64(le.AppendUint64(dst, hs.Term), hs.Vote), hs.Commit)
	}
	return dst
}

// RoundItems cuts a round's record into blob items for owner of at most maxBlob bytes each, every
// one in a pool buffer, and appends them to dst; on an error it frees what it took.
func RoundItems(pool core.BufPool, owner uint32, maxBlob uint64, rec []byte, dst []disklog.Item) ([]disklog.Item, error) {
	n := len(dst)
	for len(rec) > 0 {
		k := min(uint64(len(rec)), maxBlob)
		buf, err := pool.Alloc(int(k))
		if err != nil {
			for _, it := range dst[n:] {
				pool.Free(it.Buf)
			}
			return dst[:n], err
		}
		copy(pool.Bytes(buf), rec[:k])
		rec = rec[k:]
		kind := disklog.BlobRaftEntries
		if len(rec) == 0 {
			kind = disklog.BlobRaftState
		}
		dst = append(dst, disklog.Item{Entry: disklog.BlobEntry(kind, owner, k), Buf: buf})
	}
	return dst, nil
}

// Replay applies a replayed blob of the storage's owner: a round is applied once its last blob,
// of kind BlobRaftState, arrives, as a later record replaces what an earlier one wrote. A round
// that does not decode is ErrBadRound and changes nothing.
func (s *Storage) Replay(kind disklog.BlobKind, payload []byte) error {
	switch kind {
	case disklog.BlobRaftEntries:
		s.pending = append(s.pending, payload...)
		return nil
	case disklog.BlobRaftState:
	default:
		return ErrBadRound
	}
	rec := payload
	if len(s.pending) > 0 {
		s.pending = append(s.pending, payload...)
		rec = s.pending
	}
	var r Round
	err := DecodeRound(rec, &r)
	s.pending = s.pending[:0]
	if err != nil {
		return err
	}
	if r.Snapshot.Metadata.Index != 0 && r.Snapshot.Metadata.Index > s.base.Index {
		s.base, s.ents, s.chunks, s.first = r.Snapshot.Metadata, s.ents[:0], nil, 0
	}
	if len(r.Entries) > 0 {
		if r.Entries[0].Index > s.last()+1 {
			return ErrBadRound
		}
		s.Append(r.Entries)
	}
	if r.HardState != (raftpb.HardState{}) {
		s.hs = r.HardState
	}
	return nil
}

// DecodeRound decodes a round's record into r, whose payloads alias rec.
func DecodeRound(rec []byte, r *Round) error {
	*r = Round{}
	d := decoder{b: rec}
	flags := d.byte()
	if flags&^(flagSnapshot|flagEntries|flagHardState) != 0 {
		return ErrBadRound
	}
	if flags&flagSnapshot != 0 {
		m := &r.Snapshot.Metadata
		m.Index, m.Term = d.u64(), d.u64()
		for _, ids := range [...]*[]uint64{&m.ConfState.Voters, &m.ConfState.Learners, &m.ConfState.VotersOutgoing, &m.ConfState.LearnersNext} {
			n := d.u32()
			if uint64(n)*8 > uint64(len(d.b)) {
				return ErrBadRound
			}
			for range n {
				*ids = append(*ids, d.u64())
			}
		}
		m.ConfState.AutoLeave = d.byte() == 1
		r.Snapshot.Data = d.bytes(d.u32())
		if m.Index == 0 {
			return ErrBadRound
		}
	}
	if flags&flagEntries != 0 {
		first, n := d.u64(), d.u32()
		if n == 0 || uint64(n)*13 > uint64(len(d.b)) || first == 0 || first+uint64(n) < first {
			return ErrBadRound
		}
		r.Entries = make([]raftpb.Entry, n)
		for i := range r.Entries {
			e := &r.Entries[i]
			e.Index, e.Term, e.Type = first+uint64(i), d.u64(), raftpb.EntryType(d.byte())
			e.Data = d.bytes(d.u32())
			if e.Type > raftpb.EntryConfChangeV2 {
				return ErrBadRound
			}
		}
	}
	if flags&flagHardState != 0 {
		r.HardState = raftpb.HardState{Term: d.u64(), Vote: d.u64(), Commit: d.u64()}
	}
	if d.bad || len(d.b) != 0 {
		return ErrBadRound
	}
	return nil
}

type decoder struct {
	b   []byte
	bad bool
}

func (d *decoder) take(n uint64) []byte {
	if d.bad || n > uint64(len(d.b)) {
		d.bad, d.b = true, nil
		return nil
	}
	b := d.b[:n:n]
	d.b = d.b[n:]
	return b
}

func (d *decoder) byte() byte {
	if b := d.take(1); b != nil {
		return b[0]
	}
	return 0
}

func (d *decoder) u32() uint32 {
	if b := d.take(4); b != nil {
		return le.Uint32(b)
	}
	return 0
}

func (d *decoder) u64() uint64 {
	if b := d.take(8); b != nil {
		return le.Uint64(b)
	}
	return 0
}

func (d *decoder) bytes(n uint32) []byte {
	if n == 0 {
		return nil
	}
	return d.take(uint64(n))
}
