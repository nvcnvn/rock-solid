package meta

import (
	"errors"
	"slices"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/lsm"
)

// ctlCheckpoint is the host's checkpoint of the group's tree: the one running, its next piece in
// flight or waiting for a pool buffer, the index and configuration it compacts the log to once
// its manifest is durable and the base LSN of the last manifest it wrote. A restart loads the runs
// of the manifest recovery found, each root with its pieces, before the replay.
type ctlCheckpoint struct {
	running   bool
	inPiece   bool
	pieceWait bool
	piece     disklog.Ticket
	compact   uint64
	conf      raftpb.ConfState
	begunAt   uint64
	lastBase  uint64
	idx       []disklog.CheckpointIndex
	manifest  disklog.Manifest
	loads     []runLoad
	pieceOf   map[disklog.BlobRef]int
	loading   int
	// plantAppliedIsBase plants, in the state the runs restore, the applied index taken for the base.
	plantAppliedIsBase bool
}

// runLoad is a run being read back: its root, its merge bound if a merge consumed part of it, the
// root's bytes and the earlier pieces it lists that are still live.
type runLoad struct {
	root  disklog.BlobRef
	bound *disklog.MergeBound
	data  []byte
	refs  []disklog.BlobRef
	got   [][]byte
	left  int
}

// checkpoint flushes the group's tree, which the replica's applied index and its log's base mark,
// under a manifest naming the group's replay LSN; the log compacts to that applied index once the
// manifest is durable. It first writes again the group's hard state if only an old round holds it.
func (h *CtlHost) checkpoint(ctx *core.Context) error {
	cp, log := &h.cp, h.disk.Log
	if err := h.disk.Rewrite(ctx, cp.lastBase, &h.lout); err != nil {
		return err
	}
	st := h.shard.State()
	cp.compact, cp.conf = h.shard.Applied()
	st.Mark(h.shard.log.Base())
	tree := st.Tree()
	if c := tree.PickCompaction(); c != nil {
		c.Run()
		if err := tree.Install(c); err != nil {
			return err
		}
	}
	if _, err := tree.BeginFlush(); err != nil {
		return err
	}
	cp.begunAt = log.DurableLSN()
	cp.idx = h.disk.Indexes(cp.idx[:0], cp.begunAt)
	cp.idx[0].Runs, cp.idx[0].Bounds = tree.CarriedRuns(), tree.Bounds()
	if err := log.BeginCheckpoint(cp.idx); err != nil {
		return err
	}
	cp.running = true
	return h.nextPiece(ctx)
}

// nextPiece hands the log the flush's next run piece, or ends the checkpoint once none is left.
func (h *CtlHost) nextPiece(ctx *core.Context) error {
	cp, log := &h.cp, h.disk.Log
	buf, err := ctx.Pool.Alloc(int(log.MaxBlobBytes()))
	if cp.pieceWait = err != nil; cp.pieceWait {
		return nil
	}
	tree := h.shard.State().Tree()
	n, root, err := tree.NextPiece(ctx.Pool.Bytes(buf))
	if err != nil || n == 0 {
		ctx.Pool.Free(buf)
		if err != nil {
			return err
		}
		return log.EndCheckpoint(ctx, &h.lout)
	}
	if w := tree.PieceWorker(); w > 0 {
		cp.piece, err = log.MergePiece(ctx, w, ctlOwner, buf, uint64(n), root, &h.lout)
	} else {
		cp.piece, err = log.CheckpointPiece(ctx, ctlOwner, buf, uint64(n), root, &h.lout)
	}
	if err != nil {
		ctx.Pool.Free(buf)
		return err
	}
	cp.inPiece = true
	return nil
}

// pieceDurable takes a Durable event that may hold the piece in flight.
func (h *CtlHost) pieceDurable(ctx *core.Context, ev *disklog.Event) error {
	cp := &h.cp
	i := slices.Index(ev.Tickets, cp.piece)
	if !cp.inPiece || i < 0 {
		return nil
	}
	cp.inPiece = false
	ref := disklog.BlobRef{Segment: ev.Segment, Block: ev.Block, LSN: ev.LSN, Entry: uint16(i)}
	if err := h.shard.State().Tree().PieceDurable(ref); err != nil {
		return err
	}
	return h.nextPiece(ctx)
}

// checkpointDone reports the pieces the flush made dead, compacts the log to the index the runs
// hold, and frees each segment left with nothing live: the host runs no cleaner, which copies.
func (h *CtlHost) checkpointDone(ctx *core.Context) error {
	cp, log := &h.cp, h.disk.Log
	dead, err := h.shard.State().Tree().CheckpointDone()
	if err == nil {
		err = log.PiecesDead(dead)
	}
	if err != nil {
		return err
	}
	cp.running, cp.lastBase = false, cp.begunAt
	if cp.compact > h.shard.log.Base().Index {
		if err := h.disk.Compact(ctlOwner, cp.compact, cp.conf); err != nil {
			return err
		}
	}
	for s := range log.Superblock().Geometry.SegmentCount {
		if err := log.Free(ctx, s, &h.lout); err != nil && !errors.Is(err, disklog.ErrNotCleanable) {
			return err
		}
	}
	return nil
}

// loadManifest reads the runs of the group's tree the manifest recovery loaded names, or starts
// the replay when it names none.
func (h *CtlHost) loadManifest(ctx *core.Context) error {
	cp := &h.cp
	m, ok := h.rec.Manifest()
	k := slices.IndexFunc(m.Indexes, func(ix disklog.ManifestIndex) bool { return ix.Owner == ctlOwner })
	if !ok || k < 0 {
		return h.rec.Replay(ctx, &h.lout)
	}
	st, err := NewState(h.cfg.Tree, 0)
	if err != nil {
		return err
	}
	st.plant.appliedIsBase = cp.plantAppliedIsBase
	h.cfg.State, cp.manifest, cp.pieceOf = st, m, map[disklog.BlobRef]int{}
	ix := &m.Indexes[k]
	for _, root := range ix.Runs {
		l := runLoad{root: root}
		if b := slices.IndexFunc(ix.Bounds, func(b disklog.MergeBound) bool { return b.Run == root }); b >= 0 {
			l.bound = &ix.Bounds[b]
		}
		cp.pieceOf[root] = len(cp.loads)
		cp.loads = append(cp.loads, l)
		if err := h.rec.ReadBlob(ctx, root, &h.lout); err != nil {
			return err
		}
	}
	cp.loading = len(cp.loads)
	return h.restoreIfLoaded(ctx)
}

// blobRead takes a run piece read back: a root lists the pieces to read before it, and a run
// whose pieces are all read loads into the state.
func (h *CtlHost) blobRead(ctx *core.Context, ev *disklog.Event) error {
	cp := &h.cp
	k, ok := cp.pieceOf[ev.Ref]
	if !ok {
		return lsm.ErrMalformed
	}
	l := &cp.loads[k]
	if ev.Ref == l.root {
		refs, err := lsm.RootRefs(ev.Data, nil)
		if err != nil {
			return err
		}
		if l.bound != nil {
			refs = refs[min(int(l.bound.Dead), len(refs)):]
		}
		l.data, l.refs, l.got, l.left = slices.Clone(ev.Data), refs, make([][]byte, len(refs)), len(refs)
		for _, r := range refs {
			cp.pieceOf[r] = k
			if err := h.rec.ReadBlob(ctx, r, &h.lout); err != nil {
				return err
			}
		}
	} else {
		l.got[slices.Index(l.refs, ev.Ref)] = slices.Clone(ev.Data)
		l.left--
	}
	if l.data == nil || l.left > 0 {
		return nil
	}
	tree, pieces := h.cfg.State.Tree(), append(l.got, l.data)
	var err error
	if l.bound != nil {
		err = tree.LoadBoundedRun(*l.bound, pieces)
	} else {
		err = tree.LoadRun(l.root, pieces)
	}
	if err != nil {
		return err
	}
	cp.loading--
	return h.restoreIfLoaded(ctx)
}

// restoreIfLoaded, once every run is loaded, makes the base their mark records the storage's
// base, hands the host the manifest, takes its base LSN for the last manifest's and starts the
// replay.
func (h *CtlHost) restoreIfLoaded(ctx *core.Context) error {
	cp := &h.cp
	if cp.loading > 0 {
		return nil
	}
	base, err := h.cfg.State.Loaded()
	if err == nil {
		err = h.cfg.Raft.Storage.ApplySnapshot(raftpb.Snapshot{Metadata: base})
	}
	if err != nil {
		return err
	}
	h.disk.Loaded(&cp.manifest)
	h.replayed = true
	cp.lastBase = cp.manifest.BaseLSN
	cp.manifest, cp.loads, cp.pieceOf = disklog.Manifest{}, nil, nil
	return h.rec.Replay(ctx, &h.lout)
}
