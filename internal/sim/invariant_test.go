package sim

import (
	"errors"
	"slices"
	"testing"
)

// The registry reports the first broken invariant it checks, by name, and skips one turned off.
func TestInvariantsNameTheFirstBrokenOne(t *testing.T) {
	var r Invariants
	broken := errors.New("broken")
	var ran []string
	for _, name := range []string{"holds", "first", "second"} {
		r.Register(name, func() error {
			ran = append(ran, name)
			if name == "holds" {
				return nil
			}
			return broken
		})
	}
	var v *Violation
	if err := r.Check(); !errors.As(err, &v) || v.Name != "first" || !errors.Is(err, broken) {
		t.Fatalf("check: %v", err)
	}
	r.Off("first")
	if err := r.Check(); !errors.As(err, &v) || v.Name != "second" {
		t.Fatalf("check with the first off: %v", err)
	}
	r.Off("second")
	if err := r.Check(); err != nil {
		t.Fatalf("check with both off: %v", err)
	}
	if want := []string{"holds", "first", "holds", "second", "holds"}; !slices.Equal(ran, want) {
		t.Fatalf("ran %v, want %v", ran, want)
	}
}
