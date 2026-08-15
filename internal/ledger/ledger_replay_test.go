package ledger

import (
	"testing"
	"time"

	"medcold-handoff-ledger/internal/domain"
	"medcold-handoff-ledger/internal/infra"
	"medcold-handoff-ledger/internal/store"
)

// submitNonMonotronicSequence submits, from a single terminal in terminal_sequence
// order, box_created -> segment_started -> arrival_scanned, where the
// arrival_scanned occurred_at is EARLIER than the segment_started occurred_at.
// All three are accepted because the live coordinator applies them in
// submission (state-machine) order, not occurred_at order. This is the
// configuration that previously broke restart.
func submitNonMonotonicSequence(t *testing.T, l *Ledger, boxID string) {
	t.Helper()
	// box_created at 09:00.
	mustSubmit(t, l, &domain.Event{
		EventID: "e-create", IdempotencyKey: "k-create", TerminalID: "T1", TerminalSequence: 1,
		BoxID: boxID, Type: domain.EventBoxCreated, Role: domain.RolePharmacy, OccurredAt: lt0,
		Payload: boxCreatedPayload(),
	})
	// segment_started at 09:30.
	mustSubmit(t, l, &domain.Event{
		EventID: "e-seg", IdempotencyKey: "k-seg", TerminalID: "T1", TerminalSequence: 2,
		BoxID: boxID, Type: domain.EventSegmentStarted, Role: domain.RoleCarrier, OccurredAt: lt0.Add(30 * time.Minute),
		Payload: &domain.SegmentStartedPayload{SegmentID: "SEG1", CarrierID: "C1", Origin: "DEPOT", Destination: "PHARM"},
	})
	// arrival_scanned at 09:15 -- earlier than segment_started's 09:30.
	mustSubmit(t, l, &domain.Event{
		EventID: "e-arr", IdempotencyKey: "k-arr", TerminalID: "T1", TerminalSequence: 3,
		BoxID: boxID, Type: domain.EventArrivalScanned, Role: domain.RoleCarrier, OccurredAt: lt0.Add(15 * time.Minute),
		Payload: &domain.ArrivalScannedPayload{Location: "PHARM"},
	})
}

// TestReopenWithNonMonotonicOccurredAt reproduces the original defect: a box
// whose accepted events have occurred_at timestamps that are non-monotonic
// relative to the state-machine progression could not be reopened, because the
// rebuild replayed events in occurred_at order and tripped an illegal
// transition (arrival_scanned before segment_started). Restart must instead
// recover the exact pre-close state.
func TestReopenWithNonMonotonicOccurredAt(t *testing.T) {
	dir := t.TempDir()
	clock := infra.NewFixedClock(lt0)
	openLedger := func() *Ledger {
		l, err := Open(Config{DataDir: dir, Clock: clock, IDs: infra.NewSequenceIDSource("evt")})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		return l
	}
	l1 := openLedger()
	submitNonMonotonicSequence(t, l1, "B1")

	before, _ := l1.Coordinator().GetBox("B1")
	if before == nil {
		t.Fatalf("box missing before close")
	}
	if before.State != string(domain.StateAwaitingReceive) {
		t.Fatalf("live state = %s, want awaiting_receive", before.State)
	}
	if before.Revision != 3 {
		t.Fatalf("live revision = %d, want 3", before.Revision)
	}
	// The segment must record both the started (09:30) and arrived (09:15)
	// occurred_at timestamps as actually submitted.
	if len(before.Segments) != 1 {
		t.Fatalf("segments = %d, want 1", len(before.Segments))
	}
	seg := before.Segments[0]
	if !seg.StartedAt.Equal(lt0.Add(30 * time.Minute)) {
		t.Fatalf("segment started_at = %v, want 09:30", seg.StartedAt)
	}
	if !seg.ArrivedAt.Equal(lt0.Add(15 * time.Minute)) {
		t.Fatalf("segment arrived_at = %v, want 09:15", seg.ArrivedAt)
	}

	if err := l1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen must succeed and recover the identical projection.
	l2 := openLedger()
	t.Cleanup(func() { _ = l2.Close() })
	after, _ := l2.Coordinator().GetBox("B1")
	if after == nil {
		t.Fatalf("box missing after reopen")
	}
	if after.State != before.State {
		t.Fatalf("state after reopen = %s, want %s", after.State, before.State)
	}
	if after.Revision != before.Revision {
		t.Fatalf("revision after reopen = %d, want %d", after.Revision, before.Revision)
	}
	if after.LastEventID != before.LastEventID {
		t.Fatalf("last_event_id after reopen = %s, want %s", after.LastEventID, before.LastEventID)
	}
	if !after.UpdatedAt.Equal(before.UpdatedAt) {
		t.Fatalf("updated_at after reopen = %v, want %v", after.UpdatedAt, before.UpdatedAt)
	}
	if len(after.Segments) != 1 ||
		!after.Segments[0].StartedAt.Equal(seg.StartedAt) ||
		!after.Segments[0].ArrivedAt.Equal(seg.ArrivedAt) {
		t.Fatalf("segment after reopen = %+v, want started/arrived preserved", after.Segments)
	}
}

// TestVerifyWithNonMonotonicOccurredAt asserts the integrity check passes on a
// ledger whose accepted events have non-monotonic occurred_at.
func TestVerifyWithNonMonotonicOccurredAt(t *testing.T) {
	l, _, _ := newLedger(t)
	submitNonMonotonicSequence(t, l, "B1")
	if err := l.Verify(); err != nil {
		t.Fatalf("verify on non-monotonic ledger: %v", err)
	}
}

// TestRebuildWithNonMonotonicOccurredAt asserts that forcibly rebuilding the
// projection from accepted events recovers the correct state when occurred_at
// is non-monotonic.
func TestRebuildWithNonMonotonicOccurredAt(t *testing.T) {
	l, _, _ := newLedger(t)
	submitNonMonotonicSequence(t, l, "B1")
	before, _ := l.Coordinator().GetBox("B1")

	if err := l.Store().Update(func(tx *store.Tx) error { return tx.DeleteBox("B1") }); err != nil {
		t.Fatalf("delete projection: %v", err)
	}
	if gone, _ := l.Coordinator().GetBox("B1"); gone != nil {
		t.Fatalf("projection should be gone")
	}
	if err := l.Rebuild(); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	after, _ := l.Coordinator().GetBox("B1")
	if after == nil {
		t.Fatalf("box missing after rebuild")
	}
	if after.State != before.State || after.Revision != before.Revision {
		t.Fatalf("rebuilt box differs: state=%s rev=%d (want %s/%d)",
			after.State, after.Revision, before.State, before.Revision)
	}
	if len(after.Segments) != 1 ||
		!after.Segments[0].StartedAt.Equal(before.Segments[0].StartedAt) ||
		!after.Segments[0].ArrivedAt.Equal(before.Segments[0].ArrivedAt) {
		t.Fatalf("rebuilt segment differs: %+v", after.Segments)
	}
}

// TestTimelineOrderingPreservedWithNonMonotonicOccurredAt asserts the audit
// timeline still uses the deterministic (occurred_at, terminal_id,
// terminal_sequence, event_id) ordering -- independent of the replay order --
// so arrival_scanned (09:15) sorts before segment_started (09:30) even though
// it was applied after.
func TestTimelineOrderingPreservedWithNonMonotonicOccurredAt(t *testing.T) {
	l, _, _ := newLedger(t)
	submitNonMonotonicSequence(t, l, "B1")
	page, err := l.Coordinator().Timeline("B1", "", 100)
	if err != nil {
		t.Fatalf("timeline: %v", err)
	}
	if len(page.Items) != 3 {
		t.Fatalf("timeline items = %d, want 3", len(page.Items))
	}
	// Deterministic audit order by occurred_at: create (09:00), arr (09:15), seg (09:30).
	want := []string{"e-create", "e-arr", "e-seg"}
	for i, w := range want {
		if page.Items[i].EventID != w {
			t.Fatalf("timeline order at %d = %s, want %s (full: %+v)", i, page.Items[i].EventID, w, page.Items)
		}
	}
}
