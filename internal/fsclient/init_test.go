package fsclient

import (
	"testing"

	"github.com/nvcnvn/rock-solid/internal/fuseproto"
)

// INIT takes FUSE_PARALLEL_DIROPS and auto_inval_data from a kernel that offers everything, and
// never writeback_cache, a request timeout, FUSE over io_uring or SETXATTR_EXT; a flag the kernel
// does not offer is never taken, and another major version is refused.
func TestInitTakesParallelDiropsAndAutoInvalDataAndNeverWritebackOrTimeout(t *testing.T) {
	all := fuseproto.InitIn{Major: 7, Minor: 45, MaxReadahead: 128 << 10, Flags: 1<<64 - 1}
	o, err := Negotiate(&all)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range []uint64{fuseproto.FlagParallelDirops, fuseproto.FlagAutoInvalData, fuseproto.FlagDoReaddirplus} {
		if o.Flags&f == 0 {
			t.Errorf("flag %#x not taken: %#x", f, o.Flags)
		}
	}
	const setxattrExt = 1 << 29
	for _, f := range []uint64{fuseproto.FlagWritebackCache, fuseproto.FlagRequestTimeout, fuseproto.FlagOverIOUring, setxattrExt} {
		if o.Flags&f != 0 {
			t.Errorf("flag %#x taken: %#x", f, o.Flags)
		}
	}
	if o.RequestTimeout != 0 || o.Major != 7 || o.Minor != 45 || o.MaxWrite != MaxWrite || o.MaxReadahead != all.MaxReadahead {
		t.Errorf("INIT answered %+v", o)
	}
	none := fuseproto.InitIn{Major: 7, Minor: 45}
	if o, _ := Negotiate(&none); o.Flags != 0 {
		t.Errorf("flags taken that the kernel did not offer: %#x", o.Flags)
	}
	if _, err := Negotiate(&fuseproto.InitIn{Major: 8}); err != ErrMajor {
		t.Errorf("major 8 answered %v", err)
	}
}
