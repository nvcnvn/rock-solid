package tcp

import (
	"math/rand/v2"
	"net"
	"net/netip"
	"time"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// Conn is a blocking connection to one loop for a tool off the data path, such as the operator's
// command line: it names itself with a hello as a loop does, then sends and receives messages in
// the driver's frames.
type Conn struct {
	c    net.Conn
	pool core.BufPool
	dec  decoder
	buf  []byte
	in   []byte
	f    frame
}

// Dial connects to the loop at addr as self, its messages' buffers in pool, giving up after timeout.
func Dial(addr netip.AddrPort, self core.NodeID, pool core.BufPool, timeout time.Duration) (*Conn, error) {
	nc, err := net.DialTimeout("tcp", addr.String(), timeout)
	if err != nil {
		return nil, err
	}
	c := &Conn{c: nc, pool: pool, buf: make([]byte, aheadBytes)}
	var h [helloLen]byte
	if err := c.write(kindHello, putHello(h[:], self, rand.Uint64()), nil); err != nil {
		_ = nc.Close()
		return nil, err
	}
	return c, nil
}

func (c *Conn) write(kind byte, inline, data []byte) error {
	b := make([]byte, frameLen(len(inline), len(data)))
	putFrame(b, kind, inline, data)
	_, err := c.c.Write(b)
	return err
}

// Send sends m and frees its buffer.
func (c *Conn) Send(m *core.Msg) error {
	var data []byte
	if m.Buf != core.NoBuf {
		data = c.pool.Bytes(m.Buf)
		defer c.pool.Free(m.Buf)
	}
	return c.write(kindMsg, m.Inline(), data)
}

// Recv waits until deadline for the next message, skipping the loop's hello; its buffer is the
// caller's to free.
func (c *Conn) Recv(deadline time.Time, m *core.Msg) error {
	if err := c.c.SetReadDeadline(deadline); err != nil {
		return err
	}
	for {
		ok, err := c.dec.take(c.pool, &c.in, &c.f)
		switch {
		case err != nil:
			return err
		case ok && c.f.kind == kindMsg:
			*m = core.Msg{Buf: c.f.data}
			m.Len = uint16(copy(m.Body[:], c.f.inline))
			return nil
		case ok:
			continue
		}
		n, err := c.c.Read(c.buf)
		if err != nil {
			return err
		}
		c.in = c.buf[:n]
	}
}

// Close closes the connection.
func (c *Conn) Close() error { return c.c.Close() }
