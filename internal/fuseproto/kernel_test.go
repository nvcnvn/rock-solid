package fuseproto

import (
	"bytes"
	"testing"
)

var kernelRequests = []string{"init", "lookup", "getattr", "open", "read", "write", "flush", "release", "fsync"}

func TestGoldenRequestsEncode(t *testing.T) {
	frames := golden(t)
	for _, name := range kernelRequests {
		r := goldenRequests[name]
		if got := AppendRequest(nil, &r); !bytes.Equal(got, frames[name]) {
			t.Errorf("%s:\n got %x\nwant %x", name, got, frames[name])
		}
	}
}

func TestGoldenRepliesDecode(t *testing.T) {
	frames := golden(t)
	body := func(name string, unique uint64) []byte {
		t.Helper()
		h, b, err := DecodeReply(frames[name])
		if err != nil || h != (OutHeader{Unique: unique}) {
			t.Fatalf("%s: %+v %v", name, h, err)
		}
		return b
	}
	if o, err := DecodeInitOut(body("init_reply", 2)); err != nil || o != (InitOut{Major: 7, Minor: 45, MaxReadahead: 131072,
		Flags:         FlagAsyncRead | FlagParallelDirops | FlagMaxPages | FlagInitExt | FlagAutoInvalData,
		MaxBackground: 16, CongestionThreshold: 12, MaxWrite: 128 << 10, TimeGran: 1, MaxPages: 32}) {
		t.Errorf("init: %+v %v", o, err)
	}
	if e, err := DecodeEntry(body("entry_reply", 0x800000000000014a)); err != nil || e != entry7 {
		t.Errorf("entry: %+v %v", e, err)
	}
	if v, n, a, err := DecodeAttrOut(body("attr_reply", 0x154)); err != nil || v != 2 || n != 600 || a != entry7.Attr {
		t.Errorf("attr: %d %d %+v %v", v, n, a, err)
	}
	if v, n, a, err := DecodeStatxOut(body("statx_reply", 0x188)); err != nil || v != 2 || n != 600 || a != entry7.Attr {
		t.Errorf("statx: %d %d %+v %v", v, n, a, err)
	}
	if o, err := DecodeOpen(body("open_reply", 0x15a)); err != nil || o != (Open{Fh: 7, Flags: OpenDirectIO}) {
		t.Errorf("open: %+v %v", o, err)
	}
	if n, err := DecodeWrite(body("write_reply", 0x15e)); err != nil || n != 8 {
		t.Errorf("write: %d %v", n, err)
	}
	if b := body("read_reply", 0x15c); string(b) != "w0:00001" {
		t.Errorf("read: %q", b)
	}
	if h, b, err := DecodeReply(frames["error_reply"]); err != nil || h != (OutHeader{Error: -17, Unique: 0x800000000000014c}) || len(b) != 0 {
		t.Errorf("error: %+v %x %v", h, b, err)
	}
	h, b, err := DecodeReply(frames["inval_inode"])
	if err != nil || h != (OutHeader{Error: NotifyInvalInode}) {
		t.Fatalf("inval_inode: %+v %v", h, err)
	}
	if ino, off, n, err := DecodeInvalInode(b); err != nil || ino != 7 || off != 0 || n != -1 {
		t.Errorf("inval_inode: %d %d %d %v", ino, off, n, err)
	}
}

func TestMalformedRepliesAreErrors(t *testing.T) {
	frames := golden(t)
	for _, f := range [][]byte{nil, frames["write_reply"][:OutHeaderSize-1], frames["write_reply"][:OutHeaderSize+1]} {
		if _, _, err := DecodeReply(f); err != ErrReply {
			t.Errorf("%x: %v", f, err)
		}
	}
	decoders := map[string]func([]byte) error{
		"init_reply":  func(b []byte) error { _, err := DecodeInitOut(b); return err },
		"entry_reply": func(b []byte) error { _, err := DecodeEntry(b); return err },
		"attr_reply":  func(b []byte) error { _, _, _, err := DecodeAttrOut(b); return err },
		"open_reply":  func(b []byte) error { _, err := DecodeOpen(b); return err },
		"write_reply": func(b []byte) error { _, err := DecodeWrite(b); return err },
		"inval_inode": func(b []byte) error { _, _, _, err := DecodeInvalInode(b); return err },
	}
	for name, decode := range decoders {
		b := frames[name][OutHeaderSize:]
		if decode(b[:len(b)-1]) != ErrReply || decode(append(bytes.Clone(b), 0)) != ErrReply {
			t.Errorf("%s: a body one byte short or long decodes", name)
		}
	}
}

func FuzzDecodeReply(f *testing.F) {
	for _, frame := range golden(f) {
		f.Add(frame)
	}
	f.Fuzz(func(t *testing.T, frame []byte) {
		h, b, err := DecodeReply(frame)
		if err != nil {
			return
		}
		again := func(name string, re []byte) {
			if !bytes.Equal(re, frame) {
				t.Fatalf("%s re-encoded %x from %x", name, re, frame)
			}
		}
		if h.Unique == 0 && h.Error == NotifyInvalInode {
			if ino, off, n, err := DecodeInvalInode(b); err == nil {
				again("inval_inode", AppendInvalInode(nil, ino, off, n))
			}
			return
		}
		if h.Error != 0 {
			return
		}
		if o, err := DecodeInitOut(b); err == nil && bytes.Equal(b[42:], make([]byte, 22)) {
			again("init", AppendInit(nil, h.Unique, &o))
		}
		if e, err := DecodeEntry(b); err == nil {
			again("entry", AppendEntry(nil, h.Unique, &e))
		}
		if v, n, a, err := DecodeAttrOut(b); err == nil && le.Uint32(b[12:]) == 0 {
			again("attr", AppendAttr(nil, h.Unique, v, n, &a))
		}
		if v, n, a, err := DecodeStatxOut(b); err == nil {
			if v2, n2, a2, err := DecodeStatxOut(AppendStatx(nil, h.Unique, v, n, &a)[OutHeaderSize:]); err != nil || v2 != v || n2 != n || a2 != a {
				t.Fatalf("statx %+v came back %+v", a, a2)
			}
		}
		if o, err := DecodeOpen(b); err == nil && le.Uint32(b[12:]) == 0 {
			again("open", AppendOpen(nil, h.Unique, o))
		}
		if n, err := DecodeWrite(b); err == nil && le.Uint32(b[4:]) == 0 {
			again("write", AppendWrite(nil, h.Unique, n))
		}
	})
}
