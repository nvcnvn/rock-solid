package core

import (
	"errors"
	"fmt"
	"time"
)

// Tick is a time in microseconds, which a state machine reads only from its Context.
type Tick uint64

// TickFromDuration converts a duration to ticks, truncating below a microsecond.
func TickFromDuration(d time.Duration) Tick { return Tick(d / time.Microsecond) }

// Millis is the tick in whole milliseconds.
func (t Tick) Millis() uint64 { return uint64(t) / 1000 }

// Duration converts ticks back to a duration.
func (t Tick) Duration() time.Duration { return time.Duration(t) * time.Microsecond }

// NodeID names a peer event loop: the node and the loop within it.
type NodeID struct {
	Node uint32
	Loop uint32
}

// IOID names one disk action, chosen by the machine that emits it.
type IOID uint64

// TimerID names one timer, chosen by the machine that arms it.
type TimerID uint64

// LBA is a block address on one disk.
type LBA uint64

// IOBuf is a handle to a block-aligned pool buffer: its slot and the slot's generation, so a stale handle is caught.
type IOBuf struct {
	Slot uint32
	Gen  uint32
}

// NoBuf is the zero handle, meaning no buffer.
var NoBuf = IOBuf{}

// BufPool hands out the driver-owned buffers that data crosses a driver in, so SPDK and RDMA register the pool once.
type BufPool interface {
	// Alloc returns a buffer of n bytes, rounded up to whole blocks, with undefined
	// contents. ErrPoolExhausted is an ordinary outcome the caller retries later.
	Alloc(n int) (IOBuf, error)
	// AllocZeroed is Alloc with the buffer zeroed.
	AllocZeroed(n int) (IOBuf, error)
	// Bytes gives access to a buffer's memory; a freed or stale handle is a fatal error.
	Bytes(b IOBuf) []byte
	// Free returns a buffer; freeing twice or freeing a stale handle is a fatal error.
	Free(b IOBuf)
}

// PoolExhaustedError reports an allocation the pool cannot satisfy now.
type PoolExhaustedError struct {
	Requested int
	Available int
}

// Error names the size requested and the bytes available.
func (e *PoolExhaustedError) Error() string {
	return fmt.Sprintf("buffer pool exhausted: requested %d bytes, %d available", e.Requested, e.Available)
}

// ErrPoolExhausted matches any PoolExhaustedError with errors.Is.
var ErrPoolExhausted = errors.New("buffer pool exhausted")

// Is lets errors.Is match ErrPoolExhausted.
func (e *PoolExhaustedError) Is(target error) bool { return target == ErrPoolExhausted }

// MaxInlineMsg is the largest control message carried inline; anything bigger travels as an IOBuf.
const MaxInlineMsg = 256

// Msg is a control message: up to MaxInlineMsg inline bytes and optionally one pool buffer of data.
type Msg struct {
	Len  uint16
	Buf  IOBuf
	Body [MaxInlineMsg]byte
}

// ErrMsgTooLong reports an inline message over MaxInlineMsg bytes.
var ErrMsgTooLong = errors.New("inline message too long")

// SetInline copies b into the message's inline body.
func (m *Msg) SetInline(b []byte) error {
	if len(b) > MaxInlineMsg {
		return fmt.Errorf("%w: %d bytes", ErrMsgTooLong, len(b))
	}
	m.Len = uint16(copy(m.Body[:], b))
	return nil
}

// Inline returns the inline bytes.
func (m *Msg) Inline() []byte { return m.Body[:m.Len] }

// DiskError is the result of a disk action.
type DiskError uint8

// Disk action results.
const (
	DiskOK DiskError = iota
	DiskIO
	DiskInvalidRange
)

// Error names the error.
func (e DiskError) Error() string {
	switch e {
	case DiskOK:
		return "ok"
	case DiskIO:
		return "I/O error"
	case DiskInvalidRange:
		return "invalid block range"
	}
	return fmt.Sprintf("disk error %d", uint8(e))
}

// EventKind tags an Event.
type EventKind uint8

// Event kinds.
const (
	EvNone EventKind = iota
	EvNetRecv
	EvPeerLost
	EvDiskDone
	EvTimer
	EvResumed
)

// Event is one input to a state machine. Which fields are set depends on Kind.
type Event struct {
	Kind  EventKind
	Peer  NodeID
	Msg   Msg
	IO    IOID
	Err   DiskError
	Timer TimerID
}

// ActionKind tags an Action.
type ActionKind uint8

// Action kinds.
const (
	ActNone ActionKind = iota
	ActNetSend
	ActDiskRead
	ActDiskWrite
	ActDiskFsync
	ActDiskTrim
	ActSetTimer
)

// IOVec is the first Len bytes of a pool buffer, one element of a vectored write.
type IOVec struct {
	Buf IOBuf
	Len uint32
}

// Action is one output of a state machine, executed by a driver: a DiskWrite gathers Vec, or Buf when Vec is empty; a DiskRead fills Buf; Len is in bytes; a SetTimer re-arms Timer at At; a DiskFsync makes durable every write completed before it runs, and with Linked set runs only once the DiskWrite just before it in the same output completes.
type Action struct {
	Kind   ActionKind
	Peer   NodeID
	Msg    Msg
	IO     IOID
	LBA    LBA
	Buf    IOBuf
	Vec    []IOVec
	Len    uint64
	Linked bool
	Timer  TimerID
	At     Tick
}

// Context is what a state machine may read besides its event: the time on CLOCK_BOOTTIME and on CLOCK_MONOTONIC_RAW, and the pool.
type Context struct {
	Now  Tick
	Raw  Tick
	Pool BufPool
}
