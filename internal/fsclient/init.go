package fsclient

import (
	"errors"

	"github.com/nvcnvn/rock-solid/internal/fuseproto"
)

// MaxWrite is the largest WRITE the kernel sends, and so the most data a READ asks for: 32 pages.
const MaxWrite = 128 << 10

// wanted are the INIT flags the daemon takes when the kernel offers them. Never writeback_cache,
// which hides writes from the daemon ("Client design"), a request timeout, which aborts the whole
// mount once a request outlives it ("Daemon restart and upgrade"), FUSE over io_uring, which loses
// requests in a crash, or SETXATTR_EXT, whose longer SETXATTR body the codec does not decode.
const wanted = fuseproto.FlagAsyncRead | fuseproto.FlagBigWrites | fuseproto.FlagAutoInvalData |
	fuseproto.FlagDoReaddirplus | fuseproto.FlagParallelDirops | fuseproto.FlagMaxPages

// ErrMajor is an INIT of a protocol major version other than 7, the one the codec speaks.
var ErrMajor = errors.New("fsclient: the kernel speaks a FUSE major version other than 7")

// Negotiate is the daemon's answer to the kernel's INIT.
func Negotiate(in *fuseproto.InitIn) (fuseproto.InitOut, error) {
	if in.Major != 7 {
		return fuseproto.InitOut{}, ErrMajor
	}
	return fuseproto.InitOut{Major: 7, Minor: 45, MaxReadahead: in.MaxReadahead, Flags: in.Flags & wanted,
		MaxWrite: MaxWrite, TimeGran: 1, MaxPages: MaxWrite / 4096}, nil
}
