package infra

import (
	"testing"
	"time"
)

func TestFixedClock(t *testing.T) {
	c := NewFixedClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	if !c.Now().Equal(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("unexpected time")
	}
	c.Advance(time.Hour)
	if !c.Now().Equal(time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)) {
		t.Fatalf("advance failed")
	}
	c.Set(time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC))
	if !c.Now().Equal(time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("set failed")
	}
}

func TestFixedClockConcurrent(t *testing.T) {
	c := NewFixedClock(time.Now())
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			c.Advance(time.Millisecond)
		}
	}()
	for i := 0; i < 100; i++ {
		_ = c.Now()
	}
	<-done
}

func TestSequenceIDSource(t *testing.T) {
	s := NewSequenceIDSource("evt")
	a := s.NewID()
	b := s.NewID()
	if a == b {
		t.Fatalf("ids should differ: %s", a)
	}
	if a != "evt-00000001" || b != "evt-00000002" {
		t.Fatalf("unexpected ids: %s %s", a, b)
	}
}

func TestRealIDSourceUnique(t *testing.T) {
	s := RealIDSource{}
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		id := s.NewID()
		if seen[id] {
			t.Fatalf("duplicate id: %s", id)
		}
		seen[id] = true
	}
}

func TestTimeWindow(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	w := TimeWindow{MaxPast: time.Hour, MaxFuture: time.Hour}
	if !w.Contains(now, now) {
		t.Fatalf("now should be in window")
	}
	if !w.Contains(now, now.Add(30*time.Minute)) {
		t.Fatalf("30m future in window")
	}
	if w.Contains(now, now.Add(2*time.Hour)) {
		t.Fatalf("2h future out of window")
	}
	if w.Contains(now, now.Add(-2*time.Hour)) {
		t.Fatalf("2h past out of window")
	}
	// Zero window disables bound.
	open := TimeWindow{}
	if !open.Contains(now, now.Add(100*time.Hour)) {
		t.Fatalf("open window should contain anything")
	}
}

func TestCountingFault(t *testing.T) {
	f := NewCountingFault("pre_commit", 2)
	if err := f.Fault("pre_commit"); err == nil {
		t.Fatalf("expected fault")
	}
	if err := f.Fault("pre_commit"); err == nil {
		t.Fatalf("expected fault")
	}
	if err := f.Fault("pre_commit"); err != nil {
		t.Fatalf("quota exhausted, want nil: %v", err)
	}
	// Other stages are never faulted.
	if err := f.Fault("other"); err != nil {
		t.Fatalf("other stage should not fault")
	}
}

func TestNoFault(t *testing.T) {
	var n NoFault
	if err := n.Fault("anything"); err != nil {
		t.Fatalf("NoFault should never fault")
	}
}
