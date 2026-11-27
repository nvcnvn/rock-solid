package fsclient

import (
	"github.com/nvcnvn/rock-solid/internal/fuseproto"
	"github.com/nvcnvn/rock-solid/internal/meta"
)

// up reads directory dir's parent into slot 0 unless the node holds it and o is no retry;
// it reports whether it read.
func (m *Machine) up(o *op, dir uint64, out *Output) bool {
	if dir == meta.RootInode || (m.node(dir).parent != 0 && !o.again) {
		return false
	}
	m.get(o, 0, dir, meta.InodeParent, nil, out)
	return true
}

// gotUp takes what up read; false when the directory is gone.
func (m *Machine) gotUp(o *op, dir uint64) bool {
	if dir == meta.RootInode || (m.node(dir).parent != 0 && !o.again) {
		return true
	}
	p, err := meta.DecodeParent(o.got[0].v)
	if !o.got[0].ok || err != nil {
		return false
	}
	m.node(dir).parent = p
	return true
}

func (m *Machine) build(o *op, err error, out *Output) {
	if err != nil {
		m.fail(o, EIO, out)
		return
	}
	m.sendTxn(o, out)
}

// create serves CREATE and MKDIR.
// The steps of a CREATE without O_EXCL whose name another node created first, which opens that file.
const (
	createOpen = iota + 20
	createFetch
	createOpened
)

func (m *Machine) create(o *op, out *Output) {
	dir, mkdir := o.req.NodeID, o.req.Opcode == fuseproto.OpMkdir
	switch o.step {
	case createOpen:
		m.get(o, 0, dir, meta.InodeEntry, o.req.Name, out)
		o.step = createFetch
	case createFetch:
		if !m.child(o, out) {
			return
		}
		if o.mode == meta.ModeDir {
			m.fail(o, EISDIR, out)
			return
		}
		m.node(o.child).last = max(m.node(o.child).last, o.seq)
		m.fetch(o, o.child, false, out)
		o.step = createOpened
	case createOpened:
		if m.refetch(o, o.child) {
			m.fetch(o, o.child, false, out)
			return
		}
		n, ok := m.took(o, o.child)
		if !ok {
			o.step = 0
			m.run(o, out)
			return
		}
		m.handOut(o, n, o.child, true, out)
	case 0:
		if len(o.req.Name) > NameMax {
			m.fail(o, ENAMETOOLONG, out)
			return
		}
		o.step = 1
		if m.up(o, dir, out) {
			return
		}
		fallthrough
	case 1:
		if !m.gotUp(o, dir) {
			m.fail(o, ENOENT, out)
			return
		}
		o.step = 2
		fallthrough
	case 2:
		if o.child == 0 && !m.inode(o, mkdir, out) {
			return
		}
		o.attr = meta.Attr{Mode: uint64(o.req.Mode), Nlink: 1, UID: uint64(o.req.UID), GID: uint64(o.req.GID), Atime: o.now, Mtime: o.now, Ctime: o.now}
		up := m.node(dir).parent
		var err error
		if mkdir {
			o.attr.Mode, o.attr.Nlink = meta.ModeDir|uint64(o.req.Mode)&^meta.ModeType, 2
			err = o.fs.Mkdir(dir, up, o.req.Name, o.child, &o.attr, o.fresh, o.now)
		} else {
			err = o.fs.Create(dir, up, o.req.Name, o.child, &o.attr, nil, o.now)
		}
		o.step = 3
		m.build(o, err, out)
	case -1:
		if o.fresh {
			m.alloc.Adopt(o.slot)
		}
		n := m.node(o.child)
		n.attr = o.attr
		m.change(n)
		if mkdir {
			n.parent, n.lookups = dir, n.lookups+1
			e := m.entry(o.child, &o.attr)
			out.Replies = fuseproto.AppendEntry(out.Replies, o.req.Unique, &e)
			m.end(o, out)
		} else {
			m.handOut(o, n, o.child, true, out)
		}
	}
}

// child decodes the entry read into slot 0; false when absent, with ENOENT answered.
func (m *Machine) child(o *op, out *Output) bool {
	if !o.got[0].ok {
		m.fail(o, ENOENT, out)
		return false
	}
	c, mode, err := meta.DecodeEntry(o.got[0].v)
	if err != nil {
		m.fail(o, EIO, out)
		return false
	}
	o.child, o.mode = c, mode
	return true
}

// victim reads into slots s and s + 1 the attributes and purge entry of a file losing a link.
func (m *Machine) victim(o *op, s int, ino uint64, out *Output) {
	m.get(o, s, ino, meta.InodeAttr, nil, out)
	m.get(o, s+1, ino, meta.SlotPurge, nil, out)
}

// took2 decodes what victim read; false when the inode is gone.
func took2(o *op, s int, a *meta.Attr, p *meta.Purge) (pending *meta.Purge, ok bool) {
	if !o.got[s].ok || meta.DecodeAttr(o.got[s].v, a) != nil {
		return nil, false
	}
	if o.got[s+1].ok {
		var err error
		if *p, err = meta.DecodePurge(o.got[s+1].v); err != nil {
			return nil, false
		}
		return p, true
	}
	return nil, true
}

// open reports whether this mount has ino open, which stands for the owner's open registry.
func (m *Machine) opened(ino uint64) bool {
	n := m.nodes[ino]
	return n != nil && n.opens > 0
}

// unlink serves UNLINK and RMDIR.
func (m *Machine) unlink(o *op, out *Output) {
	dir, rmdir := o.req.NodeID, o.req.Opcode == fuseproto.OpRmdir
	switch o.step {
	case 0:
		m.get(o, 0, dir, meta.InodeEntry, o.req.Name, out)
		o.step = 1
	case 1:
		if !m.child(o, out) {
			return
		}
		switch {
		case rmdir && o.mode != meta.ModeDir:
			m.fail(o, ENOTDIR, out)
		case rmdir:
			o.step = 3
			m.build(o, o.fs.Rmdir(dir, o.req.Name, o.child, o.now), out)
		case o.mode == meta.ModeDir:
			m.fail(o, EISDIR, out)
		default:
			m.victim(o, 0, o.child, out)
			o.step = 2
		}
	case 2:
		pending, ok := took2(o, 0, &o.attr, &o.pend)
		if !ok {
			o.step, o.again = 0, true
			m.run(o, out)
			return
		}
		o.step = 3
		m.build(o, o.fs.Unlink(dir, o.req.Name, o.child, &o.attr, m.opened(o.child), pending, o.now), out)
	case -1:
		if n := m.nodes[o.child]; n != nil && n.attr.Nlink > 0 {
			n.attr.Nlink--
			m.change(n)
		}
		m.empty(o, out)
	}
}

// rename serves a rename within one directory; one across directories is EXDEV until the rest of
// the namespace, so mv copies.
func (m *Machine) rename(o *op, out *Output) {
	dir := o.req.NodeID
	switch o.step {
	case 0:
		if o.req.NewDir != dir {
			m.fail(o, EXDEV, out)
			return
		}
		if len(o.req.NewName) > NameMax {
			m.fail(o, ENAMETOOLONG, out)
			return
		}
		m.get(o, 0, dir, meta.InodeEntry, o.req.Name, out)
		m.get(o, 1, dir, meta.InodeEntry, o.req.NewName, out)
		o.step = 1
	case 1:
		if !m.child(o, out) {
			return
		}
		o.over = 0
		if o.got[1].ok {
			over, omode, err := meta.DecodeEntry(o.got[1].v)
			if err != nil {
				m.fail(o, EIO, out)
				return
			}
			switch {
			case o.mode == meta.ModeDir && omode != meta.ModeDir:
				m.fail(o, ENOTDIR, out)
				return
			case o.mode != meta.ModeDir && omode == meta.ModeDir:
				m.fail(o, EISDIR, out)
				return
			}
			o.over, o.omode = over, omode
			if over != o.child {
				m.victim(o, 2, over, out)
			}
		}
		o.step = 2
		m.run(o, out)
	case 2:
		r := meta.Rename{SrcDir: dir, DstDir: dir, Src: o.req.Name, Dst: o.req.NewName, Child: o.child, ChildMode: o.mode, Over: o.over}
		if o.over != 0 && o.over != o.child {
			pending, ok := took2(o, 2, &o.attr, &o.pend)
			if !ok {
				o.step, o.again = 0, true
				m.run(o, out)
				return
			}
			r.OverAttr, r.OverOpen, r.OverPending = &o.attr, m.opened(o.over), pending
		}
		o.step = 3
		m.build(o, o.fs.Rename(&r, o.now), out)
	case -1:
		if n := m.nodes[o.over]; o.over != o.child && n != nil && n.attr.Nlink > 0 {
			n.attr.Nlink--
			m.change(n)
		}
		m.empty(o, out)
	}
}

func setTime(t uint64, ns uint32) int64 { return int64(t)*1e9 + int64(ns) }

// setattr serves SETATTR of mode, owner and times; a size other than the current one is the
// Truncate item's, answered EOPNOTSUPP until then.
func (m *Machine) setattr(o *op, out *Output) {
	ino := o.req.NodeID
	n := m.node(ino)
	s := &o.req.Setattr
	switch o.step {
	case 0:
		if !m.lock(o, n) {
			return
		}
		m.fetch(o, ino, o.again || n.attr.Mode&meta.ModeType == meta.ModeDir, out)
		o.step = 1
	case 1:
		if _, ok := m.took(o, ino); !ok {
			m.fail(o, ENOENT, out)
			return
		}
		a := o.attr
		if s.Valid&fuseproto.AttrSize != 0 && s.Size != shown(n).Size {
			m.fail(o, EOPNOTSUPP, out)
			return
		}
		b, f := a, o.fields[:0]
		if s.Valid&fuseproto.AttrMode != 0 {
			b.Mode = a.Mode&meta.ModeType | uint64(s.Mode)&^meta.ModeType
			f = append(f, meta.Field{Off: meta.AttrMode, V: b.Mode})
		}
		if s.Valid&fuseproto.AttrUID != 0 {
			b.UID = uint64(s.UID)
			f = append(f, meta.Field{Off: meta.AttrUID, V: b.UID})
		}
		if s.Valid&fuseproto.AttrGID != 0 {
			b.GID = uint64(s.GID)
			f = append(f, meta.Field{Off: meta.AttrGID, V: b.GID})
		}
		switch {
		case s.Valid&fuseproto.AttrAtimeNow != 0:
			b.Atime = o.now
		case s.Valid&fuseproto.AttrAtime != 0:
			b.Atime = setTime(s.Atime, s.AtimeNsec)
		}
		if b.Atime != a.Atime {
			f = append(f, meta.Field{Off: meta.AttrAtime, V: meta.TimeField(b.Atime)})
		}
		switch {
		case s.Valid&fuseproto.AttrMtimeNow != 0:
			b.Mtime = o.now
		case s.Valid&fuseproto.AttrMtime != 0:
			b.Mtime = setTime(s.Mtime, s.MtimeNsec)
		}
		if b.Mtime != a.Mtime {
			f = append(f, meta.Field{Off: meta.AttrMtime, V: meta.TimeField(b.Mtime)})
		}
		b.Ctime = max(o.now, a.Ctime+1)
		o.attr = b
		o.step = 3
		if a.Mode&meta.ModeType == meta.ModeDir {
			m.build(o, o.fs.SetDirAttr(ino, n.parent, o.now, f...), out)
		} else {
			m.build(o, o.fs.SetAttr(ino, &a, &b, nil), out)
		}
	case -1:
		n.attr = o.attr
		m.change(n)
		a := shown(n)
		fa := fuseAttr(ino, &a)
		_, valid := m.timeouts(a.Mode)
		out.Replies = fuseproto.AppendAttr(out.Replies, o.req.Unique, valid, 0, &fa)
		m.end(o, out)
	}
}
