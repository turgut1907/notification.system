package clock

import (
	"testing"
	"time"
)

func TestFakeClock(t *testing.T) {
	start := time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC)
	f := NewFake(start)
	if !f.Now().Equal(start) {
		t.Fatal("now mismatch")
	}
	f.Advance(time.Hour)
	if !f.Now().Equal(start.Add(time.Hour)) {
		t.Fatal("advance failed")
	}
	f.Set(start.Add(2 * time.Hour))
	if !f.Now().Equal(start.Add(2 * time.Hour)) {
		t.Fatal("set failed")
	}
}

func TestSystemClock(t *testing.T) {
	if New().Now().IsZero() {
		t.Fatal("system clock zero")
	}
}
