package sim

import "fmt"

// Invariants is a run's registry of named invariants, which the run checks together at its check points.
type Invariants struct {
	list []invariant
}

type invariant struct {
	name  string
	check func() error
	off   bool
}

// Violation is an invariant a check found broken: its name and what its check reported.
type Violation struct {
	Name string
	Err  error
}

func (v *Violation) Error() string { return fmt.Sprintf("invariant %q: %v", v.Name, v.Err) }

func (v *Violation) Unwrap() error { return v.Err }

// Register adds the invariant name, which check reports broken by returning an error.
func (r *Invariants) Register(name string, check func() error) {
	if r.index(name) >= 0 {
		panic("sim: invariant " + name + " registered twice")
	}
	r.list = append(r.list, invariant{name: name, check: check})
}

// Off stops checking the registered invariant name, for a run that breaks it on purpose.
func (r *Invariants) Off(name string) { r.list[r.find(name)].off = true }

// On checks the registered invariant name again after Off.
func (r *Invariants) On(name string) { r.list[r.find(name)].off = false }

// Only checks the registered invariant name alone, if registered, for a run that plants a bug which must break it.
func (r *Invariants) Only(name string) {
	for i := range r.list {
		r.list[i].off = r.list[i].name != name
	}
}

// Check runs every invariant not turned off, in the order registered, and returns the first broken one as a *Violation.
func (r *Invariants) Check() error {
	for _, inv := range r.list {
		if inv.off {
			continue
		}
		if err := inv.check(); err != nil {
			return &Violation{Name: inv.name, Err: err}
		}
	}
	return nil
}

func (r *Invariants) find(name string) int {
	if i := r.index(name); i >= 0 {
		return i
	}
	panic("sim: no invariant " + name)
}

func (r *Invariants) index(name string) int {
	for i, inv := range r.list {
		if inv.name == name {
			return i
		}
	}
	return -1
}
