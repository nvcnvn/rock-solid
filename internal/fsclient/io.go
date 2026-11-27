package fsclient

import (
	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/fuseproto"
	"github.com/nvcnvn/rock-solid/internal/meta"
)

// The steps of a WRITE.
const (
	writeReread = iota + 10
	writeRaise
	writeChunk
	writeLanded
)

func (m *Machine) file(ino uint64) chunkstore.File {
	return chunkstore.File{Pool: m.cfg.Pool, Volume: core.VolumeID(m.cfg.Volume), Inode: core.InodeID(ino)}
}

// chunks sends one chunk-client call for o, which waits for a completion per chunk it covers.
func (m *Machine) chunks(o *op, c ChunkCall, out *Output) {
	c.Token, c.File = m.token(o, 0), m.file(o.req.NodeID)
	n := uint64(c.N)
	if !c.Read {
		n = uint64(len(c.Data))
	}
	o.pending += int((c.Off+n-1)/core.ChunkSize - c.Off/core.ChunkSize)
	out.Chunks = append(out.Chunks, c)
}

// past reports that a write of o's reaches a chunk at or past n's alloc_end.
func past(o *op, n *node) bool {
	return (o.req.Offset+uint64(o.req.Size)-1)/core.ChunkSize >= n.attr.AllocEnd
}

// write writes data through to its chunks' primaries, raising alloc_end first when it reaches a
// chunk at or past it: a crash between would leave a chunk that the inode's purge, which frees
// chunks below alloc_end, misses.
func (m *Machine) write(o *op, data []byte, out *Output) {
	end := o.req.Offset + uint64(len(data))
	switch {
	case len(data) == 0:
		out.Replies = fuseproto.AppendWrite(out.Replies, o.req.Unique, 0)
		m.done(o)
		return
	case end < o.req.Offset || end > core.ChunkSize<<32:
		m.fail(o, EFBIG, out)
		return
	}
	o.req.Size = uint32(len(data))
	n := m.node(o.req.NodeID)
	if !past(o, n) || m.plant.raiseAfterWrite {
		o.step = writeChunk
		m.chunks(o, ChunkCall{Off: o.req.Offset, Data: data}, out)
		return
	}
	o.data = append(o.data[:0], data...)
	o.step = writeRaise
	m.run(o, out)
}

// raised moves a WRITE on: its alloc_end raise, by 1, 2, 4, … up to 16 chunks a file, committing
// the size written so far, then its chunk write.
func (m *Machine) raised(o *op, out *Output) {
	ino := o.req.NodeID
	n := m.node(ino)
	switch o.step {
	case 0:
		m.fetch(o, ino, false, out)
		o.step = writeReread
	case writeReread:
		if m.older(o, ino) {
			m.fetch(o, ino, false, out)
			return
		}
		if _, ok := m.took(o, ino); !ok {
			m.fail(o, ENOENT, out)
			return
		}
		o.step = writeRaise
		fallthrough
	case writeRaise:
		if !m.lock(o, n) {
			return
		}
		if !past(o, n) {
			m.unlock(o, out)
			m.landed(o, n, out)
			return
		}
		a := n.attr
		inc := max(n.step, 1)
		b := a
		b.AllocEnd = max((o.req.Offset+uint64(o.req.Size)-1)/core.ChunkSize+1, a.AllocEnd+inc)
		b.Size = max(a.Size, n.written)
		o.attr, o.step = b, writeRaise
		m.build(o, o.fs.SetAttr(ino, &a, &b, nil), out)
	case -1:
		n.attr = o.attr
		m.change(n)
		n.step = min(maxRaise, max(n.step, 1)*2)
		m.unlock(o, out)
		m.landed(o, n, out)
	}
}

// landed sends o's chunk write once alloc_end covers it, or answers a write already sent.
func (m *Machine) landed(o *op, n *node, out *Output) {
	if o.step == writeLanded || m.plant.raiseAfterWrite && o.data == nil {
		out.Replies = fuseproto.AppendWrite(out.Replies, o.req.Unique, o.req.Size)
		m.done(o)
		return
	}
	o.step = writeChunk
	m.chunks(o, ChunkCall{Off: o.req.Offset, Data: o.data}, out)
}

// wrote takes a WRITE's chunk completions.
func (m *Machine) wrote(o *op, out *Output) {
	if o.errno != 0 {
		m.fail(o, o.errno, out)
		return
	}
	n := m.node(o.req.NodeID)
	n.written = max(n.written, o.req.Offset+uint64(o.req.Size))
	n.wrote, n.lastWrite = true, max(n.lastWrite, o.now)
	if m.plant.raiseAfterWrite && past(o, n) {
		o.data, o.step = nil, writeRaise
		m.raised(o, out)
		return
	}
	o.step = writeLanded
	m.landed(o, n, out)
}

// read reads through to the chunks, clamped to the file's size as the node holds it.
func (m *Machine) read(o *op, out *Output) {
	if o.step == writeChunk {
		if o.errno != 0 {
			m.fail(o, o.errno, out)
			return
		}
		fuseproto.PutDataReply(o.reply, o.req.Unique)
		out.Replies = append(out.Replies, o.reply...)
		m.done(o)
		return
	}
	size := shown(m.node(o.req.NodeID)).Size
	if o.req.Offset >= size || o.req.Size == 0 {
		m.empty(o, out)
		return
	}
	n := uint32(min(uint64(o.req.Size), size-o.req.Offset))
	o.reply = append(o.reply[:0], make([]byte, fuseproto.OutHeaderSize+int(n))...)
	o.step = writeChunk
	m.chunks(o, ChunkCall{Read: true, Off: o.req.Offset, N: n}, out)
}

// ChunkDone takes a chunk-client completion, freeing its buffer.
func (m *Machine) ChunkDone(ctx *core.Context, c *chunkstore.Completion, out *Output) {
	w, ok := m.waits[c.Token]
	if c.Buf != core.NoBuf {
		defer ctx.Pool.Free(c.Buf)
	}
	if !ok {
		return
	}
	o := w.o
	if o.pending--; o.pending == 0 {
		delete(m.waits, c.Token)
	}
	switch {
	case c.Status == chunkstore.StatusOK && o.req.Opcode == fuseproto.OpRead && c.Buf != core.NoBuf:
		b := ctx.Pool.Bytes(c.Buf)[c.Off%core.BlockSize:]
		copy(o.reply[fuseproto.OutHeaderSize+int(c.Off-o.req.Offset):], b[:min(len(b), int(c.Len))])
	case c.Status == chunkstore.StatusOK, c.Status == chunkstore.StatusUnfound && o.req.Opcode == fuseproto.OpRead:
	case c.Status == chunkstore.StatusNoSpace:
		o.errno = ENOSPC
	default:
		o.errno = EIO
	}
	if o.pending > 0 {
		return
	}
	if o.req.Opcode == fuseproto.OpRead {
		m.read(o, out)
	} else {
		m.wrote(o, out)
	}
}

// commit serves FLUSH and FSYNC: the node's writes since its last commit set its size and its
// mtime, the latest of its last write's time and the stored mtime plus 1 ns.
func (m *Machine) commit(o *op, out *Output) {
	ino := o.req.NodeID
	n := m.nodes[ino]
	switch o.step {
	case 0:
		if n == nil || !n.wrote {
			m.empty(o, out)
			return
		}
		if !m.lock(o, n) {
			return
		}
		o.step = 2
		if o.again {
			m.get(o, 0, ino, meta.InodeAttr, nil, out)
			o.step = 1
			return
		}
		fallthrough
	case 1, 2:
		if o.step == 1 {
			if _, ok := m.took(o, ino); !ok {
				n.wrote = false
				m.empty(o, out)
				return
			}
		}
		a := n.attr
		b := a
		b.Size = max(a.Size, n.written)
		b.Mtime = meta.Mtime(a.Mtime, n.lastWrite, 0)
		b.Ctime = max(a.Ctime, b.Mtime)
		o.attr, o.wtime, o.step = b, n.lastWrite, 3
		m.build(o, o.fs.SetAttr(ino, &a, &b, nil), out)
	case -1:
		n.attr = o.attr
		m.change(n)
		if n.lastWrite <= o.wtime && n.written <= o.attr.Size {
			n.wrote, n.written = false, 0
		}
		m.empty(o, out)
	}
}
