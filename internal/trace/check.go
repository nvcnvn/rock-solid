package trace

import (
	"fmt"
)

// Rule is a named property of a trace; New starts a scan of one trace.
type Rule struct {
	Name string
	New  func() Scanner
}

// Scanner checks one trace against a rule, returning why a record or the trace's end breaks it, or "".
type Scanner interface {
	Scan(r *Record) string
	End() string
}

// SyntaxRule is the rule a Violation names for a line that does not parse.
const SyntaxRule = "syntax"

// Violation is the first line of a trace that breaks a rule, numbered from 1.
type Violation struct {
	Line   int
	Rule   string
	Reason string
}

// Error names the line, the rule and the reason.
func (v *Violation) Error() string {
	return fmt.Sprintf("line %d breaks %s: %s", v.Line, v.Rule, v.Reason)
}

// Check parses a trace of newline-terminated lines and returns the first Violation of any rule, or nil.
func Check(text []byte, rules ...Rule) error {
	c := newChecker(rules)
	line := 0
	for len(text) > 0 {
		line++
		l, rest, ok := cut(text, '\n')
		if !ok {
			return &Violation{Line: line, Rule: SyntaxRule, Reason: "last line has no newline"}
		}
		text = rest
		r, err := Parse(l)
		if err != nil {
			return &Violation{Line: line, Rule: SyntaxRule, Reason: err.Error()}
		}
		if v := c.scan(line, &r); v != nil {
			return v
		}
	}
	return c.end(line)
}

// CheckRecords returns the first Violation of any rule in recs, where record i is line i+1, or nil.
func CheckRecords(recs []Record, rules ...Rule) error {
	c := newChecker(rules)
	for i := range recs {
		if v := c.scan(i+1, &recs[i]); v != nil {
			return v
		}
	}
	return c.end(len(recs))
}

type checker struct {
	rules    []Rule
	scanners []Scanner
}

func newChecker(rules []Rule) *checker {
	c := &checker{rules: rules, scanners: make([]Scanner, len(rules))}
	for i, r := range rules {
		c.scanners[i] = r.New()
	}
	return c
}

func (c *checker) scan(line int, r *Record) error {
	for i, s := range c.scanners {
		if why := s.Scan(r); why != "" {
			return &Violation{Line: line, Rule: c.rules[i].Name, Reason: why}
		}
	}
	return nil
}

func (c *checker) end(line int) error {
	for i, s := range c.scanners {
		if why := s.End(); why != "" {
			return &Violation{Line: line, Rule: c.rules[i].Name, Reason: why}
		}
	}
	return nil
}

type loopKey struct {
	node uint32
	loop uint16
	sim  bool
}

// perLoop keeps one T per loop in the order loops first appear, so a scan over loops is deterministic.
type perLoop[T any] struct {
	index  map[loopKey]int
	states []T
}

func (p *perLoop[T]) get(r *Record) *T {
	k := loopKey{node: r.Node, loop: r.Loop, sim: r.Sim}
	i, ok := p.index[k]
	if !ok {
		if p.index == nil {
			p.index = make(map[loopKey]int)
		}
		i = len(p.states)
		p.index[k] = i
		var zero T
		p.states = append(p.states, zero)
	}
	return &p.states[i]
}
