package fsclient

import (
	"github.com/nvcnvn/rock-solid/internal/fuseproto"
	"github.com/nvcnvn/rock-solid/internal/meta"
)

// fetch reads ino's attributes into slot 0, and a directory's parent into slot 1.
func (m *Machine) fetch(o *op, ino uint64, dir bool, out *Output) {
	m.get(o, 0, ino, meta.InodeAttr, nil, out)
	if dir && ino != meta.RootInode {
		m.get(o, 1, ino, meta.InodeParent, nil, out)
	}
	o.fetch = m.seq
}

// stale reports attributes of ino that o fetched before the machine read a later request for the
// inode, or a later LOOKUP that has not yet read its entry and so may name it: the kernel drops
// the reply to a request sent before the inode's attributes last changed, so a reply of older
// attributes written after a later request was sent would land and the fresh one be dropped.
func (m *Machine) stale(o *op, ino uint64) bool {
	if m.plant.staleKept {
		return false
	}
	if m.older(o, ino) || m.node(ino).last > o.fetch {
		return true
	}
	for _, p := range m.ops {
		if p.req.Opcode == fuseproto.OpLookup && !p.resolved && p.seq > o.fetch {
			return true
		}
	}
	return false
}

// older reports attributes of ino that o fetched before the machine last changed them, which
// may predate that change.
func (m *Machine) older(o *op, ino uint64) bool { return m.node(ino).changed > o.fetch }

// took decodes the attributes fetch read for ino into its node; false when the inode is gone.
func (m *Machine) took(o *op, ino uint64) (*node, bool) {
	if !o.got[0].ok || meta.DecodeAttr(o.got[0].v, &o.attr) != nil {
		return nil, false
	}
	n := m.node(ino)
	n.attr = o.attr
	if o.got[1].ok {
		if p, err := meta.DecodeParent(o.got[1].v); err == nil {
			n.parent = p
		}
	}
	return n, true
}

// shown is the attributes the kernel is given for n: its size counts the node's own writes not yet committed.
func shown(n *node) meta.Attr {
	a := n.attr
	a.Size = max(a.Size, n.written)
	return a
}

func (m *Machine) lookup(o *op, out *Output) {
	switch o.step {
	case 0:
		if len(o.req.Name) > NameMax {
			m.fail(o, ENAMETOOLONG, out)
			return
		}
		m.get(o, 0, o.req.NodeID, meta.InodeEntry, o.req.Name, out)
		o.step = 1
	case 1:
		if !o.got[0].ok {
			m.fail(o, ENOENT, out)
			return
		}
		child, mode, err := meta.DecodeEntry(o.got[0].v)
		if err != nil {
			m.fail(o, EIO, out)
			return
		}
		o.child, o.mode, o.resolved = child, mode, true
		n := m.node(child)
		n.last = max(n.last, o.seq)
		m.fetch(o, child, mode == meta.ModeDir, out)
		o.step = 2
	case 2:
		if m.stale(o, o.child) {
			m.fetch(o, o.child, o.mode == meta.ModeDir, out)
			return
		}
		n, ok := m.took(o, o.child)
		if !ok {
			o.step, o.resolved = 0, false
			m.run(o, out)
			return
		}
		n.lookups++
		a := shown(n)
		e := m.entry(o.child, &a)
		out.Replies = fuseproto.AppendEntry(out.Replies, o.req.Unique, &e)
		m.end(o, out)
	}
}

func (m *Machine) getattr(o *op, out *Output) {
	ino := o.req.NodeID
	if o.step == 0 {
		m.fetch(o, ino, false, out)
		o.step = 1
		return
	}
	if m.stale(o, ino) {
		m.fetch(o, ino, false, out)
		return
	}
	n, ok := m.took(o, ino)
	if !ok {
		m.fail(o, ENOENT, out)
		return
	}
	a := shown(n)
	fa := fuseAttr(ino, &a)
	_, valid := m.timeouts(a.Mode)
	if o.req.Opcode == fuseproto.OpStatx {
		out.Replies = fuseproto.AppendStatx(out.Replies, o.req.Unique, valid, 0, &fa)
	} else {
		out.Replies = fuseproto.AppendAttr(out.Replies, o.req.Unique, valid, 0, &fa)
	}
	m.end(o, out)
}

// open answers with the owner's current attributes in the node, and without keep_cache when
// they changed since the node's last open, so the kernel drops pages a change left in place.
func (m *Machine) open(o *op, out *Output) {
	ino := o.req.NodeID
	if o.step == 0 {
		m.fetch(o, ino, false, out)
		o.step = 1
		return
	}
	if m.older(o, ino) {
		m.fetch(o, ino, false, out)
		return
	}
	n, ok := m.took(o, ino)
	if !ok {
		m.fail(o, ENOENT, out)
		return
	}
	m.handOut(o, n, ino, false, out)
}

// handOut gives o a handle of ino, answering an OPEN, or a CREATE with ino's entry; keep_cache
// only when the attributes are those of the node's previous open.
func (m *Machine) handOut(o *op, n *node, ino uint64, create bool, out *Output) {
	var flags uint32
	if n.opened == n.attr && !create {
		flags = fuseproto.OpenKeepCache
	}
	n.opened = n.attr
	n.opens++
	fh := m.handle(ino)
	if create {
		n.lookups++
		a := shown(n)
		e := m.entry(ino, &a)
		out.Replies = fuseproto.AppendCreate(out.Replies, o.req.Unique, &e, fuseproto.Open{Fh: fh})
	} else {
		out.Replies = fuseproto.AppendOpen(out.Replies, o.req.Unique, fuseproto.Open{Fh: fh, Flags: flags})
	}
	m.end(o, out)
}

// readdirLimit bounds the entries one READDIRPLUS scans.
const readdirLimit = 1024

// readdir answers READDIRPLUS with names and types, each with node ID 0, which the kernel takes as
// a name whose attributes it must look up: so `ls -l` sends a LOOKUP per name, as a regular file's
// entry timeout of 0 makes it do anyway, and a directory listing costs one scan.
func (m *Machine) readdir(o *op, out *Output) {
	h := m.handles[o.req.Fh]
	if h == nil {
		m.fail(o, EINVAL, out)
		return
	}
	dir := o.req.NodeID
	skip := o.req.Offset
	if o.step == 0 {
		if skip == h.count && h.count > 0 {
			o.vbuf = append(m.key(o.vbuf, dir, meta.InodeEntry, h.last), 0)
			skip = 0
		} else {
			o.vbuf = m.bound(o.vbuf, dir, meta.InodeEntry)
		}
		start := len(o.vbuf)
		o.vbuf = m.bound(o.vbuf, dir, meta.InodeEntry+1)
		limit := min(readdirLimit, int(skip)+int(o.req.Size)/fuseproto.DirentPlusSize(1)+1)
		out.Reads = append(out.Reads, Read{Token: m.token(o, -1), Start: o.vbuf[:start:start], End: o.vbuf[start:], Limit: limit})
		o.step = 1
		return
	}
	if skip == h.count && h.count > 0 {
		skip = 0
	}
	count := o.req.Offset
	o.reply = append(o.reply[:0], make([]byte, fuseproto.OutHeaderSize)...)
	for i, kv := range o.scan {
		if uint64(i) < skip {
			continue
		}
		var k meta.Key
		child, mode, err := meta.DecodeEntry(kv.Value)
		if err != nil || meta.DecodeKey(kv.Key, &k) != nil {
			continue
		}
		if len(o.reply)-fuseproto.OutHeaderSize+fuseproto.DirentPlusSize(len(k.Name)) > int(o.req.Size) {
			break
		}
		count++
		e := fuseproto.Entry{Attr: fuseproto.Attr{Ino: child}}
		o.reply = fuseproto.AppendDirentPlus(o.reply, &e, count, uint32(mode>>12), k.Name)
		h.last = append(h.last[:0], k.Name...)
	}
	h.count = count
	fuseproto.PutDataReply(o.reply, o.req.Unique)
	out.Replies = append(out.Replies, o.reply...)
	m.end(o, out)
}
