package chunkstore

import (
	"cmp"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// peerReply is a member's answer to peering: its disk, the newest interval whose decision it persisted for the PG, its chunks, and, for an up member of the interval peered, the missing list the decision gives it.
type peerReply struct {
	disk    uint32
	decided uint32
	chunks  []ChunkPeer
	list    []ChunkVersion
}

// keptChunk is peering's decision on a chunk: the version it keeps, whether no member it reached holds that version whole with nothing newer logged and nothing listed, so none can serve it, and whether the primary's copy may differ from the kept version's, so a merge on it waits for recovery: its index maps another version once it settled the chunk, or its store listed the chunk before this decision, which leaves blocks below the chunk's newest version that may miss a kept version or hold a dropped one.
type keptChunk struct {
	chunk   core.ChunkID
	kept    uint64
	unfound bool
	lacking bool
}

// replyChunk is one member's report of a chunk, by its reply's index.
type replyChunk struct {
	ChunkPeer
	reply int
}

func compareChunk(a, b core.ChunkID) int {
	return cmp.Or(cmp.Compare(a.Volume, b.Volume), cmp.Compare(a.Inode, b.Inode), cmp.Compare(a.Index, b.Index))
}

// upIn reports whether disk was an up member of interval i of the PG in e: its current interval or one of the past intervals the entry lists.
func upIn(e *PGEntry, disk, i uint32) bool {
	ms := e.Members
	if i != e.Interval {
		j := slices.IndexFunc(e.Past, func(p PastInterval) bool { return p.Interval == i })
		if j < 0 {
			return false
		}
		ms = e.Past[j].Members
	}
	return slices.ContainsFunc(ms, func(m Member) bool { return m.Disk == disk && m.Mark == MarkUp })
}

// keep is the keep rule over the replies peering of e's interval reached, appending to dst each chunk a reply names, in chunk order, and setting the list of each reply from an up member of the interval. LS, the newest interval whose decision a reached member persisted and at least e's last activated one, fixes every version below it: a member that persisted LS lists the version LS kept or, listing nothing, holds it as its newest below LS. A version of LS or later is kept unless a reached member up in its interval holds neither it nor a later version nor a listing of one, and every version a kept one rests on is kept. Members report only their newest whole version, which suffices: the newest kept version is the newest whole version of some reached member, since a later interval that took writes activated, so its members persisted it and LS would be at least it. A member is left off the list only if it holds the kept version as its newest, logged nothing later and lists nothing.
func keep(e *PGEntry, replies []peerReply, all []replyChunk, dst []keptChunk, plant replicaPlants) ([]replyChunk, []keptChunk) {
	ls := e.Activated
	for _, r := range replies {
		ls = max(ls, r.decided)
	}
	all = all[:0]
	for i := range replies {
		replies[i].list = replies[i].list[:0]
		for _, c := range replies[i].chunks {
			all = append(all, replyChunk{ChunkPeer: c, reply: i})
		}
	}
	slices.SortFunc(all, func(a, b replyChunk) int {
		return cmp.Or(compareChunk(a.Chunk, b.Chunk), cmp.Compare(a.reply, b.reply))
	})
	for lo := 0; lo < len(all); {
		hi := lo + 1
		for hi < len(all) && all[hi].Chunk == all[lo].Chunk {
			hi++
		}
		g := all[lo:hi]
		k := keepChunk(e, replies, g, ls, plant.ownInterval)
		k.unfound = k.kept != 0 && !slices.ContainsFunc(g, func(c replyChunk) bool { return clean(&c.ChunkPeer, k.kept) })
		dst = append(dst, k)
		for i := range replies {
			r := &replies[i]
			if !upIn(e, r.disk, e.Interval) {
				continue
			}
			j := slices.IndexFunc(g, func(c replyChunk) bool { return c.reply == i })
			if j < 0 && k.kept != 0 || j >= 0 && !clean(&g[j].ChunkPeer, k.kept) && !(plant.relistNever && g[j].Whole == k.kept && g[j].Logged == k.kept) {
				r.list = append(r.list, ChunkVersion{Chunk: k.chunk, Version: k.kept})
			}
		}
		lo = hi
	}
	return all, dst
}

// clean reports whether a member holds version v as its newest, logged nothing later and lists nothing.
func clean(c *ChunkPeer, v uint64) bool { return c.Whole == v && c.Logged == v && !c.Listed }

func keepChunk(e *PGEntry, replies []peerReply, g []replyChunk, ls uint32, own bool) keptChunk {
	k := keptChunk{chunk: g[0].Chunk}
	for _, c := range g {
		switch {
		case own || replies[c.reply].decided != ls:
		case c.Listed:
			k.kept = max(k.kept, c.Kept)
		case intervalOf(c.Whole) < ls:
			k.kept = max(k.kept, c.Whole)
		}
	}
	for _, c := range g {
		if v := c.Whole; v > k.kept && (own || intervalOf(v) >= ls) && heldByUp(e, replies, g, v) {
			k.kept = v
		}
	}
	return k
}

// heldByUp reports whether every reached member up in version v's interval holds v or later, whole or listed.
func heldByUp(e *PGEntry, replies []peerReply, g []replyChunk, v uint64) bool {
	for i, r := range replies {
		if !upIn(e, r.disk, intervalOf(v)) {
			continue
		}
		var h uint64
		if j := slices.IndexFunc(g, func(c replyChunk) bool { return c.reply == i }); j >= 0 {
			h = g[j].Whole
			if g[j].Listed {
				h = max(h, g[j].Kept)
			}
		}
		if h < v {
			return false
		}
	}
	return true
}

func intervalOf(v uint64) uint32 { return uint32(v >> 32) }
