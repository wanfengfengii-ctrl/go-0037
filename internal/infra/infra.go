// Package infra provides the testable infrastructure seams for the ledger:
// a replaceable Clock, a replaceable event-ID source and a transaction-level
// fault injector. Production code uses the real implementations; tests inject
// fixed clocks, deterministic IDs and controlled failures so they never depend
// on wall-clock time, real network timing or sleep.
package infra

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

// Clock returns the current time. The ledger uses it to stamp received_at and
// to validate occurred_at windows, so tests can pin time deterministically.
type Clock interface {
	Now() time.Time
}

// RealClock returns the wall clock.
type RealClock struct{}

// Now returns time.Now.
func (RealClock) Now() time.Time { return time.Now() }

// FixedClock is a mutable, deterministic clock for tests. It is safe for
// concurrent use.
type FixedClock struct {
	mu sync.Mutex
	t  time.Time
}

// NewFixedClock returns a FixedClock pinned at t.
func NewFixedClock(t time.Time) *FixedClock { return &FixedClock{t: t} }

// Now returns the current pinned time.
func (c *FixedClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

// Set jumps the clock to t.
func (c *FixedClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}

// Advance moves the clock forward by d.
func (c *FixedClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// IDSource generates unique, sortable-ish identifiers for server-generated
// event IDs. Identifiers are UUIDv4 strings.
type IDSource interface {
	NewID() string
}

// RealIDSource generates random UUIDv4 identifiers using crypto/rand.
type RealIDSource struct{}

// NewID returns a freshly generated UUIDv4 string.
func (RealIDSource) NewID() string { return newUUIDv4() }

// SequenceIDSource returns deterministic, monotonic identifiers in the form
// "evt-00000001". It is intended for tests that want stable, predictable IDs.
type SequenceIDSource struct {
	mu  sync.Mutex
	n   uint64
	pfx string
}

// NewSequenceIDSource returns a source emitting identifiers prefixed with pfx.
func NewSequenceIDSource(pfx string) *SequenceIDSource {
	return &SequenceIDSource{pfx: pfx}
}

// NewID returns the next deterministic identifier.
func (s *SequenceIDSource) NewID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.n++
	return fmt.Sprintf("%s-%08d", s.pfx, s.n)
}

// newUUIDv4 returns a random RFC 4122 version-4 UUID string.
func newUUIDv4() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand should never fail on supported platforms; fall back to
		// a panic so the failure is loud rather than silently non-unique.
		panic(fmt.Sprintf("infra: crypto/rand failed: %v", err))
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%s-%s-%s-%s-%s",
		hex.EncodeToString(b[0:4]),
		hex.EncodeToString(b[4:6]),
		hex.EncodeToString(b[6:8]),
		hex.EncodeToString(b[8:10]),
		hex.EncodeToString(b[10:16]),
	)
}

// TimeWindow bounds how far occurred_at may lie in the past or future relative
// to the clock's now. A zero value disables the bound on that side.
type TimeWindow struct {
	MaxPast   time.Duration
	MaxFuture time.Duration
}

// Contains reports whether t falls inside the window centred on now.
func (w TimeWindow) Contains(now, t time.Time) bool {
	if !t.Before(now) {
		// t is now or in the future.
		if w.MaxFuture == 0 {
			return true
		}
		return t.Sub(now) <= w.MaxFuture
	}
	// t is in the past.
	if w.MaxPast == 0 {
		return true
	}
	return now.Sub(t) <= w.MaxPast
}

// FaultInjector lets tests inject a failure at a well-defined point in the
// transaction pipeline. The stage argument identifies the call site so a test
// can fail only at, say, "pre_commit" while leaving other stages healthy.
//
// Returning a non-nil error aborts the surrounding transaction (the store
// rolls back every write performed in it) and surfaces a storage_failure to
// the caller.
type FaultInjector interface {
	Fault(stage string) error
}

// NoFault never injects a failure.
type NoFault struct{}

// Fault always returns nil.
func (NoFault) Fault(string) error { return nil }

// CountingFault fails the first N calls at the given stage, then succeeds.
// It is safe for concurrent use.
type CountingFault struct {
	mu    sync.Mutex
	stage string
	n     int
}

// NewCountingFault returns an injector that fails n times at stage.
func NewCountingFault(stage string, n int) *CountingFault {
	return &CountingFault{stage: stage, n: n}
}

// Fault fails (returns ErrInjected) while the quota remains, else nil.
func (c *CountingFault) Fault(stage string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if stage == c.stage && c.n > 0 {
		c.n--
		return ErrInjected
	}
	return nil
}

// ErrInjected is the sentinel error returned by CountingFault.
var ErrInjected = fmt.Errorf("injected fault")
