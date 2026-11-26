package fuseproto

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
)

// golden reads testdata/golden.txt, frames built by testdata/golden.c from linux/fuse.h.
func golden(t testing.TB) map[string][]byte {
	t.Helper()
	f, err := os.Open("testdata/golden.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	frames := map[string][]byte{}
	s := bufio.NewScanner(f)
	for s.Scan() {
		name, h, _ := strings.Cut(s.Text(), " ")
		b, err := hex.DecodeString(h)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		frames[name] = b
	}
	return frames
}

func hdr(op Opcode, unique, node uint64) Request {
	return Request{Opcode: op, Unique: unique, NodeID: node, UID: 1000, GID: 1001, PID: 4242}
}

func with(r Request, f func(*Request)) Request {
	f(&r)
	return r
}

var goldenRequests = map[string]Request{
	"init": with(hdr(OpInit, 2, 0), func(r *Request) {
		r.Init = InitIn{Major: 7, Minor: 45, MaxReadahead: 131072,
			Flags: FlagAsyncRead | FlagParallelDirops | FlagMaxPages | FlagInitExt | FlagAutoInvalData | FlagHasResend | FlagRequestTimeout}
	}),
	"lookup": with(hdr(OpLookup, 0x800000000000014a, 1), func(r *Request) { r.Name = []byte("hang1") }),
	"forget": with(hdr(OpForget, 0x150, 7), func(r *Request) { r.NLookup = 3 }),
	"batch_forget": with(hdr(OpBatchForget, 0x152, 0), func(r *Request) {
		r.Forgets = []byte{7, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0}
	}),
	"getattr": with(hdr(OpGetattr, 0x154, 7), func(r *Request) { r.OpFlags = GetattrFh; r.Fh = 77 }),
	"setattr": with(hdr(OpSetattr, 0x156, 7), func(r *Request) {
		r.Setattr = Setattr{Valid: AttrSize | AttrMtime | AttrFh, Fh: 77, Size: 4096, LockOwner: 0x1234,
			Atime: 1700000001, Mtime: 1700000002, Ctime: 1700000003, AtimeNsec: 21, MtimeNsec: 22, CtimeNsec: 23,
			Mode: 0100600, UID: 1002, GID: 1003}
	}),
	"create": with(hdr(OpCreate, 0x158, 1), func(r *Request) {
		r.Flags = 0101102
		r.Mode = 0100644
		r.Umask = 022
		r.Name = []byte("f0")
	}),
	"create_groups": with(hdr(OpCreate, 0x159, 1), func(r *Request) {
		r.Flags = 0101102
		r.Mode = 0100644
		r.Umask = 022
		r.Name = []byte("f0")
	}),
	"open": with(hdr(OpOpen, 0x15a, 7), func(r *Request) { r.Flags = 0100002 }),
	"read": with(hdr(OpRead, 0x15c, 7), func(r *Request) {
		r.Fh, r.Offset, r.Size, r.OpFlags, r.LockOwner, r.Flags = 7, 8192, 4096, 2, 0x99, 0100002
	}),
	"write": with(hdr(OpWrite, 0x15e, 7), func(r *Request) {
		r.Fh, r.Offset, r.Size, r.OpFlags, r.LockOwner, r.Flags = 7, 4096, 8, 2, 0x99, 0100002
		r.Data = []byte("w0:00001")
	}),
	"flush": with(hdr(OpFlush, 0x160, 7), func(r *Request) { r.Fh, r.LockOwner = 7, 0x99 }),
	"release": with(hdr(OpRelease, 0x162, 7), func(r *Request) {
		r.Fh, r.Flags, r.OpFlags, r.LockOwner = 7, 0100002, ReleaseFlush, 0x99
	}),
	"fsync":  with(hdr(OpFsync, 0x164, 7), func(r *Request) { r.Fh, r.OpFlags = 7, FsyncFdatasync }),
	"unlink": with(hdr(OpUnlink, 0x166, 1), func(r *Request) { r.Name = []byte("f0") }),
	"mkdir": with(hdr(OpMkdir, 0x800000000000014c, 1), func(r *Request) {
		r.Mode, r.Umask, r.Name = 040755, 022, []byte("mkhangdir")
	}),
	"rmdir": with(hdr(OpRmdir, 0x168, 1), func(r *Request) { r.Name = []byte("mkhangdir") }),
	"rename": with(hdr(OpRename, 0x16a, 1), func(r *Request) {
		r.NewDir, r.Name, r.NewName = 8, []byte("old"), []byte("new")
	}),
	"opendir":     with(hdr(OpOpendir, 0x16c, 1), func(r *Request) { r.Flags = 0200000 }),
	"readdirplus": with(hdr(OpReaddirplus, 0x16e, 1), func(r *Request) { r.Fh, r.Offset, r.Size = 1, 2, 4096 }),
	"releasedir":  with(hdr(OpReleasedir, 0x170, 1), func(r *Request) { r.Fh, r.Flags = 1, 0200000 }),
	"statfs":      hdr(OpStatfs, 0x172, 1),
	"link":        with(hdr(OpLink, 0x176, 8), func(r *Request) { r.OldNodeID, r.Name = 7, []byte("f0-link") }),
	"access":      with(hdr(OpAccess, 0x178, 7), func(r *Request) { r.Mask = 6 }),
	"fsyncdir":    with(hdr(OpFsyncdir, 0x17a, 1), func(r *Request) { r.Fh, r.OpFlags = 3, FsyncFdatasync }),
	"rename2": with(hdr(OpRename2, 0x17c, 1), func(r *Request) {
		r.NewDir, r.OpFlags, r.Name, r.NewName = 8, 1, []byte("old"), []byte("new")
	}),
	"tmpfile": with(hdr(OpTmpfile, 0x17e, 1), func(r *Request) {
		r.Flags, r.Mode, r.Umask, r.OpFlags, r.Name = 020200002, 0100600, 022, 1, []byte("/")
	}),
	"statx": with(hdr(OpStatx, 0x180, 7), func(r *Request) {
		r.OpFlags, r.Fh, r.StatxFlags, r.StatxMask = GetattrFh, 77, 0x2000, 0xfff
	}),
	"setxattr": with(hdr(OpSetxattr, 0x182, 7), func(r *Request) {
		r.Size, r.OpFlags, r.Name, r.Data = 4, 1, []byte("user.DOSATTRIB"), []byte{0, 4, 0, 0x20}
	}),
	"getxattr":    with(hdr(OpGetxattr, 0x184, 7), func(r *Request) { r.Size, r.Name = 256, []byte("user.DOSATTRIB") }),
	"listxattr":   with(hdr(OpListxattr, 0x186, 7), func(r *Request) { r.Size = 4096 }),
	"removexattr": with(hdr(OpRemovexattr, 0x188, 7), func(r *Request) { r.Name = []byte("user.DOSATTRIB") }),
}

func TestGoldenRequestsDecode(t *testing.T) {
	frames := golden(t)
	for name, want := range goldenRequests {
		var r Request
		if err := Decode(frames[name], &r); err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if !reflect.DeepEqual(r, want) {
			t.Errorf("%s:\n got %+v\nwant %+v", name, r, want)
		}
	}

	var r Request
	if err := Decode(frames["unknown"], &r); !errors.Is(err, ErrOpcode) || r.Unique != 0x174 || r.Opcode != 36 {
		t.Errorf("unknown opcode: %v, %+v", err, r)
	}
}

func TestResentRequestsCarryUniqueResend(t *testing.T) {
	frames := golden(t)
	for name, id := range map[string]uint64{"lookup": 0x14a, "mkdir": 0x14c} {
		var r Request
		if err := Decode(frames[name], &r); err != nil || r.Unique&UniqueResend == 0 || r.Unique&^UniqueResend != id {
			t.Errorf("%s: %v, unique %#x", name, err, r.Unique)
		}
	}
}

func TestBatchForgetRecords(t *testing.T) {
	var r Request
	if err := Decode(golden(t)["batch_forget"], &r); err != nil {
		t.Fatal(err)
	}
	if r.ForgetCount() != 2 {
		t.Fatalf("count %d", r.ForgetCount())
	}
	for i, want := range [][2]uint64{{7, 3}, {9, 1}} {
		if n, l := r.Forget(i); n != want[0] || l != want[1] {
			t.Errorf("record %d: %d %d, want %v", i, n, l, want)
		}
	}
}

func TestWriteDataStaysInItsFrame(t *testing.T) {
	frame := golden(t)["write"]
	var r Request
	if err := Decode(frame, &r); err != nil {
		t.Fatal(err)
	}
	if &r.Data[0] != &frame[InHeaderSize+40] || len(r.Data) != 8 {
		t.Errorf("data not at the kernel's offset in the frame")
	}
}

var entry7 = Entry{NodeID: 7, Generation: 3, EntryValid: 1, AttrValid: 2, EntryValidNsec: 500, AttrValidNsec: 600, Attr: attr(7)}

func attr(ino uint64) Attr {
	return Attr{Ino: ino, Size: 5000, Blocks: 16, Atime: 1700000001, Mtime: 1700000002, Ctime: 1700000003,
		AtimeNsec: 11, MtimeNsec: 12, CtimeNsec: 13, Mode: 0100644, Nlink: 1, UID: 1000, GID: 1001, Rdev: 7, Blksize: 4096}
}

func TestGoldenRepliesEncode(t *testing.T) {
	frames := golden(t)
	entry9, entry11, entry13 := entry7, entry7, entry7
	entry9.NodeID, entry9.Attr.Ino = 9, 9
	entry11.NodeID, entry11.Attr.Ino = 11, 11
	entry13.NodeID, entry13.Attr.Ino = 13, 13
	replies := map[string][]byte{
		"init_reply": AppendInit(nil, 2, &InitOut{Major: 7, Minor: 45, MaxReadahead: 131072,
			Flags:         FlagAsyncRead | FlagParallelDirops | FlagMaxPages | FlagInitExt | FlagAutoInvalData,
			MaxBackground: 16, CongestionThreshold: 12, MaxWrite: 128 << 10, TimeGran: 1, MaxPages: 32}),
		"entry_reply":  AppendEntry(nil, 0x800000000000014a, &entry7),
		"attr_reply":   AppendAttr(nil, 0x154, 2, 600, &entry7.Attr),
		"statx_reply":  AppendStatx(nil, 0x188, 2, 600, &entry7.Attr),
		"open_reply":   AppendOpen(nil, 0x15a, Open{Fh: 7, Flags: OpenDirectIO}),
		"create_reply": AppendCreate(nil, 0x158, &entry7, Open{Fh: 7, Flags: OpenKeepCache}),
		"read_reply": func() []byte {
			b := append(make([]byte, OutHeaderSize), "w0:00001"...)
			PutDataReply(b, 0x15c)
			return b
		}(),
		"write_reply":      AppendWrite(nil, 0x15e, 8),
		"xattr_size_reply": AppendXattrSize(nil, 0x184, 23),
		"statfs_reply": AppendStatfs(nil, 0x172, &Statfs{Blocks: 1000, Bfree: 900, Bavail: 800, Files: 100, Ffree: 90,
			Bsize: 4096, NameLen: 255, Frsize: 4096}),
		"empty_reply": AppendEmpty(nil, 0x162),
		"error_reply": AppendError(nil, 0x800000000000014c, 17),
		"readdirplus_reply": func() []byte {
			b := make([]byte, OutHeaderSize)
			b = AppendDirentPlus(b, &entry7, 3, 8, []byte("a"))
			b = AppendDirentPlus(b, &entry9, 4, 8, []byte("longer-name"))
			b = AppendDirentPlus(b, &entry11, 5, 8, []byte("eight-ch"))
			b = AppendDirentPlus(b, &entry13, 6, 8, []byte("sixteen-chars-ab"))
			PutDataReply(b, 0x16e)
			return b
		}(),
		"inval_inode": AppendInvalInode(nil, 7, 0, -1),
		"inval_entry": AppendInvalEntry(nil, 1, []byte("f0"), ExpireOnly),
		"resend":      AppendResend(nil),
	}
	for name, got := range replies {
		if want, ok := frames[name]; !ok || !bytes.Equal(got, want) {
			t.Errorf("%s:\n got %x\nwant %x", name, got, want)
		}
	}
	for name := range frames {
		if _, ok := replies[name]; !ok && goldenRequests[name].Opcode == 0 && name != "unknown" {
			t.Errorf("golden frame %s is checked by no test", name)
		}
	}
}

func TestDirentPlusSizeMatchesWhatIsAppended(t *testing.T) {
	for n := range 20 {
		if got := len(AppendDirentPlus(nil, &entry7, 1, 8, make([]byte, n))); got != DirentPlusSize(n) || got%8 != 0 {
			t.Errorf("name of %d: %d bytes, DirentPlusSize %d", n, got, DirentPlusSize(n))
		}
	}
}

func setLen(frame []byte) []byte {
	le.PutUint32(frame, uint32(len(frame)))
	return frame
}

func TestMalformedFramesAreErrors(t *testing.T) {
	frames := golden(t)
	for n := range InHeaderSize {
		var r Request
		if err := Decode(frames["getattr"][:n], &r); !errors.Is(err, ErrShortHeader) {
			t.Errorf("header of %d bytes: %v", n, err)
		}
	}
	check := func(what string, frame []byte, want error) {
		t.Helper()
		var r Request
		err := Decode(frame, &r)
		if !errors.Is(err, want) {
			t.Errorf("%s: %v, want %v", what, err, want)
		}
		if r.Unique == 0 {
			t.Errorf("%s: header not decoded", what)
		}
	}
	lookup := frames["lookup"]
	check("length past the frame", lookup[:len(lookup)-1], ErrLength)
	check("extensions past the frame", func() []byte {
		b := bytes.Clone(lookup)
		le.PutUint16(b[36:], 2)
		return b
	}(), ErrLength)
	check("name without its zero", setLen(bytes.Clone(lookup[:len(lookup)-1])), ErrName)
	check("bytes after the name", setLen(append(bytes.Clone(lookup), 'x')), ErrBody)
	check("rename's second name without its zero", setLen(bytes.Clone(frames["rename"][:len(frames["rename"])-1])), ErrName)
	check("write shorter than its size", setLen(bytes.Clone(frames["write"][:len(frames["write"])-1])), ErrBody)
	check("batch forget count past the frame", func() []byte {
		b := bytes.Clone(frames["batch_forget"])
		le.PutUint32(b[InHeaderSize:], 3)
		return b
	}(), ErrBody)
	for name, r := range goldenRequests {
		f := frames[name]
		if name == "init" {
			continue
		}
		check(name+" one byte long", setLen(append(bytes.Clone(f), 0)), ErrBody)
		short := error(ErrBody)
		if r.Name != nil && r.Data == nil {
			short = ErrName
		}
		if name != "statfs" {
			check(name+" one byte short", setLen(bytes.Clone(f[:len(f)-1])), short)
		}
	}
	check("init shorter than its four fields", setLen(bytes.Clone(frames["init"][:InHeaderSize+15])), ErrBody)
}

func TestShortInitHasNoFlags2(t *testing.T) {
	var r Request
	if err := Decode(setLen(bytes.Clone(golden(t)["init"][:InHeaderSize+16])), &r); err != nil {
		t.Fatal(err)
	}
	if r.Init.Flags>>32 != 0 || r.Init.Minor != 45 {
		t.Errorf("%+v", r.Init)
	}
}

var params = Params{
	Kernel: InitIn{Major: 7, Minor: 45, MaxReadahead: 131072, Flags: FlagInitExt | FlagHasResend | FlagParallelDirops},
	Daemon: InitOut{Major: 7, Minor: 45, MaxReadahead: 131072, Flags: FlagInitExt | FlagParallelDirops | FlagRequestTimeout,
		MaxBackground: 16, CongestionThreshold: 12, MaxWrite: 128 << 10, TimeGran: 1, MaxPages: 32,
		MapAlignment: 2, MaxStackDepth: 3, RequestTimeout: 4},
}

func TestParamsRoundTripAndDifferInEveryField(t *testing.T) {
	b := AppendParams(nil, &params)
	if got, err := DecodeParams(b); err != nil || got != params || len(b) != ParamsSize {
		t.Fatalf("round trip: %v %+v", err, got)
	}
	for _, half := range []reflect.Value{reflect.ValueOf(&params.Kernel).Elem(), reflect.ValueOf(&params.Daemon).Elem()} {
		for i := range half.NumField() {
			f := half.Field(i)
			old := f.Uint()
			f.SetUint(old ^ 1<<(f.Type().Bits()-1))
			changed := params
			f.SetUint(old)
			got, err := DecodeParams(AppendParams(nil, &changed))
			if err != nil || got != changed || got == params {
				t.Errorf("%s.%s: %v, decoded %+v", half.Type().Name(), half.Type().Field(i).Name, err, got)
			}
		}
	}
	for _, n := range []int{0, ParamsSize - 1, ParamsSize + 1} {
		if _, err := DecodeParams(make([]byte, n)); !errors.Is(err, ErrParams) {
			t.Errorf("%d bytes: %v", n, err)
		}
	}
}

func TestDecodeAndReplyAllocateNothing(t *testing.T) {
	frames := golden(t)
	out := make([]byte, 0, 1<<16)
	var r Request
	bodyOnly := [][]byte{frames["access"], frames["fsyncdir"], frames["rename2"], frames["statx"], frames["setxattr"], frames["removexattr"]}
	cases := map[string]func(){
		"write": func() {
			_ = Decode(frames["write"], &r)
			out = AppendWrite(out[:0], r.Unique, uint32(len(r.Data)))
		},
		"statx": func() {
			_ = Decode(frames["statx"], &r)
			out = AppendStatx(out[:0], r.Unique, 1, 0, &entry7.Attr)
		},
		"lookup": func() {
			_ = Decode(frames["lookup"], &r)
			out = AppendEntry(out[:0], r.Unique, &entry7)
		},
		"readdirplus": func() {
			_ = Decode(frames["readdirplus"], &r)
			out = AppendDirentPlus(out[:OutHeaderSize], &entry7, 1, 8, []byte("a"))
			PutDataReply(out, r.Unique)
		},
		"statfs": func() {
			_ = Decode(frames["statfs"], &r)
			out = AppendStatfs(out[:0], r.Unique, &Statfs{})
		},
		"init": func() {
			_ = Decode(frames["init"], &r)
			out = AppendInit(out[:0], r.Unique, &params.Daemon)
			out = AppendParams(out[:0], &params)
		},
		"link": func() {
			_ = Decode(frames["link"], &r)
			out = AppendEntry(out[:0], r.Unique, &entry7)
		},
		"tmpfile": func() {
			_ = Decode(frames["tmpfile"], &r)
			out = AppendCreate(out[:0], r.Unique, &entry7, Open{Fh: 7})
		},
		"access, fsyncdir, rename2, statx, setxattr, removexattr": func() {
			for _, f := range bodyOnly {
				_ = Decode(f, &r)
				out = AppendEmpty(out[:0], r.Unique)
			}
		},
		"getxattr, listxattr": func() {
			_ = Decode(frames["getxattr"], &r)
			out = AppendXattrSize(out[:0], r.Unique, 0)
			_ = Decode(frames["listxattr"], &r)
			out = AppendEmpty(out[:0], r.Unique)
		},
		"notify": func() {
			out = AppendInvalEntry(AppendInvalInode(AppendResend(out[:0]), 7, 0, -1), 1, []byte("f0"), 0)
		},
	}
	for name, f := range cases {
		if n := testing.AllocsPerRun(100, f); n != 0 {
			t.Errorf("%s: %v allocations", name, n)
		}
	}
}

func BenchmarkWrite(b *testing.B) {
	frame := golden(b)["write"]
	out := make([]byte, 0, 64)
	var r Request
	b.ReportAllocs()
	for b.Loop() {
		if err := Decode(frame, &r); err != nil {
			b.Fatal(err)
		}
		out = AppendWrite(out[:0], r.Unique, uint32(len(r.Data)))
	}
}

func BenchmarkLookup(b *testing.B) {
	frame := golden(b)["lookup"]
	out := make([]byte, 0, 256)
	var r Request
	b.ReportAllocs()
	for b.Loop() {
		if err := Decode(frame, &r); err != nil {
			b.Fatal(err)
		}
		out = AppendEntry(out[:0], r.Unique, &entry7)
	}
}

func FuzzDecode(f *testing.F) {
	for _, frame := range golden(f) {
		f.Add(frame)
	}
	f.Fuzz(func(t *testing.T, frame []byte) {
		checkDecode(t, frame)
		if len(frame) >= 4 {
			// A mutation that changes the length fails the header check; a copy with the length
			// patched takes it to the body decoders.
			checkDecode(t, setLen(bytes.Clone(frame)))
		}
	})
}

func checkDecode(t *testing.T, frame []byte) {
	t.Helper()
	var r Request
	err := Decode(frame, &r)
	switch {
	case err == nil:
	case errors.Is(err, ErrShortHeader), errors.Is(err, ErrLength), errors.Is(err, ErrBody),
		errors.Is(err, ErrName), errors.Is(err, ErrOpcode):
		return
	default:
		t.Fatalf("unexpected error %v", err)
	}
	if bytes.IndexByte(r.Name, 0) >= 0 || bytes.IndexByte(r.NewName, 0) >= 0 {
		t.Fatalf("name holds a zero: %q %q", r.Name, r.NewName)
	}
	if (r.Opcode == OpWrite || r.Opcode == OpSetxattr) && len(r.Data) != int(r.Size) {
		t.Fatalf("write of %d bytes carries %d", r.Size, len(r.Data))
	}
	if len(r.Forgets) != r.ForgetCount()*forgetOneSize {
		t.Fatalf("%d bytes of forgets", len(r.Forgets))
	}
	for i := range r.ForgetCount() {
		r.Forget(i)
	}
}

func FuzzDecodeParams(f *testing.F) {
	f.Add(AppendParams(nil, &params))
	f.Fuzz(func(t *testing.T, b []byte) {
		p, err := DecodeParams(b)
		if err != nil {
			return
		}
		const used = initInSize + 42
		if again := AppendParams(nil, &p); !bytes.Equal(again[:used], b[:used]) || !bytes.Equal(again[used:], make([]byte, ParamsSize-used)) {
			t.Fatalf("re-encoded %x from %x", again, b)
		}
	})
}
