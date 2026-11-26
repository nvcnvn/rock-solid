package fuseproto

import "errors"

// ParamsSize is the length of encoded Params.
const ParamsSize = initInSize + initOutSize

// ErrParams is the error of saved INIT parameters of the wrong length.
var ErrParams = errors.New("fuseproto: saved INIT parameters of the wrong length")

// Params are a mount's negotiated INIT: what the kernel offered and what the daemon answered. The
// kernel never negotiates again, so a successor adopting the mount compares them with its own.
type Params struct {
	Kernel InitIn
	Daemon InitOut
}

// AppendParams appends p as fuse_init_in's first five fields and the whole fuse_init_out.
func AppendParams(dst []byte, p *Params) []byte {
	dst = le.AppendUint32(dst, p.Kernel.Major)
	dst = le.AppendUint32(dst, p.Kernel.Minor)
	dst = le.AppendUint32(dst, p.Kernel.MaxReadahead)
	dst = le.AppendUint32(dst, uint32(p.Kernel.Flags))
	dst = le.AppendUint32(dst, uint32(p.Kernel.Flags>>32))
	return appendInitOut(dst, &p.Daemon)
}

// DecodeParams decodes what AppendParams encoded.
func DecodeParams(b []byte) (Params, error) {
	if len(b) != ParamsSize {
		return Params{}, ErrParams
	}
	var p Params
	p.Kernel = InitIn{Major: le.Uint32(b), Minor: le.Uint32(b[4:]), MaxReadahead: le.Uint32(b[8:]),
		Flags: uint64(le.Uint32(b[12:])) | uint64(le.Uint32(b[16:]))<<32}
	p.Daemon = decodeInitOut(b[initInSize:])
	return p, nil
}
