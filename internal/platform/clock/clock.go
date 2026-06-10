// Package clock provides a small abstraction over time so that time-dependent
// logic (retry backoff, scheduling, lease expiry) is deterministically testable.
package clock

import "time"

// Clock is the port for reading the current time.
type Clock interface {
	Now() time.Time
}

// System is the production Clock backed by the wall clock.
type System struct{}

// New returns a system clock.
func New() System { return System{} }

// Now returns the current wall-clock time in UTC.
func (System) Now() time.Time { return time.Now().UTC() }

// Fake is a controllable Clock for tests.
type Fake struct {
	current time.Time
}

// NewFake returns a Fake clock initialized to t.
func NewFake(t time.Time) *Fake { return &Fake{current: t.UTC()} }

// Now returns the fake clock's current time.
func (f *Fake) Now() time.Time { return f.current }

// Advance moves the fake clock forward by d.
func (f *Fake) Advance(d time.Duration) { f.current = f.current.Add(d) }

// Set sets the fake clock to t.
func (f *Fake) Set(t time.Time) { f.current = t.UTC() }
