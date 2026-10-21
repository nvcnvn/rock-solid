package sim

import (
	"errors"
	"fmt"
)

var (
	// ErrLostAcked reports a recovery to an LSN below the acknowledged one.
	ErrLostAcked = errors.New("recovery lost an acknowledged record")
	// ErrPastWritten reports a recovery, or an acknowledgment, past the newest record written.
	ErrPastWritten = errors.New("past the newest record written")
	// ErrOutOfOrder reports a record written at an LSN other than the next one, or an LSN acknowledged twice.
	ErrOutOfOrder = errors.New("LSN out of order")
)

// OracleModel is a plain model of every index on a disk, which Apply changes by one record and Clone copies.
type OracleModel[R, M any] interface {
	Apply(R)
	Clone() M
}

// Oracle is the crash oracle of one disk: the model at the newest acknowledged LSN and the records written since, in LSN order.
type Oracle[R any, M OracleModel[R, M]] struct {
	acked   uint64
	model   M
	written []R
}

// NewOracle starts an oracle that owns m, its model at the acknowledged LSN acked.
func NewOracle[R any, M OracleModel[R, M]](acked uint64, m M) *Oracle[R, M] {
	return &Oracle[R, M]{acked: acked, model: m}
}

// Window is the LSNs a recovery may report: from the acknowledged one to the newest written.
func (o *Oracle[R, M]) Window() (acked, newest uint64) {
	return o.acked, o.acked + uint64(len(o.written))
}

// Written records the items of the record at lsn, which must be the one after the newest written.
func (o *Oracle[R, M]) Written(lsn uint64, r R) error {
	if _, newest := o.Window(); lsn != newest+1 {
		return fmt.Errorf("record written at LSN %d after %d: %w", lsn, newest, ErrOutOfOrder)
	}
	o.written = append(o.written, r)
	return nil
}

// Acked applies every record up to lsn to the model at the acknowledged LSN.
func (o *Oracle[R, M]) Acked(lsn uint64) error {
	acked, newest := o.Window()
	switch {
	case lsn <= acked:
		return fmt.Errorf("LSN %d acknowledged after %d: %w", lsn, acked, ErrOutOfOrder)
	case lsn > newest:
		return fmt.Errorf("LSN %d acknowledged, newest written %d: %w", lsn, newest, ErrPastWritten)
	}
	n := int(lsn - acked)
	for i := range n {
		o.model.Apply(o.written[i])
	}
	o.written = append(o.written[:0], o.written[n:]...)
	o.acked = lsn
	return nil
}

// Expect is a copy of the model with every record up to r applied, which every index must equal after a recovery to r.
func (o *Oracle[R, M]) Expect(r uint64) (M, error) {
	acked, newest := o.Window()
	switch {
	case r < acked:
		var zero M
		return zero, fmt.Errorf("recovered to LSN %d, acknowledged %d: %w", r, acked, ErrLostAcked)
	case r > newest:
		var zero M
		return zero, fmt.Errorf("recovered to LSN %d, newest written %d: %w", r, newest, ErrPastWritten)
	}
	m := o.model.Clone()
	for _, rec := range o.written[:r-acked] {
		m.Apply(rec)
	}
	return m, nil
}

// Recovered is Expect, then makes r the acknowledged LSN and forgets the records the resumed log writes over.
func (o *Oracle[R, M]) Recovered(r uint64) (M, error) {
	m, err := o.Expect(r)
	if err != nil {
		return m, err
	}
	o.acked, o.model, o.written = r, m.Clone(), o.written[:0]
	return m, nil
}
