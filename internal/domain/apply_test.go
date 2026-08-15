package domain

import (
	"testing"
	"time"

	"medcold-handoff-ledger/internal/apperr"
)

var base = time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)

func ev(et EventType, role Role, payload Payload) *Event {
	return &Event{
		EventID:          string(et) + "-1",
		IdempotencyKey:   string(et) + "-1",
		TerminalID:       "T1",
		TerminalSequence: 1,
		BoxID:            "BOX-1",
		Type:             et,
		Role:             role,
		OccurredAt:       base,
		Payload:          payload,
	}
}

func boxCreated(t *testing.T) *Box {
	t.Helper()
	p := &BoxCreatedPayload{
		ProductName: "Insulin",
		Batch:       "B001",
		Origin:      "DEPOT",
		Destination: "PHARMACY-A",
		CarrierID:   "CARRIER-1",
		LowerBound:  2.0,
		UpperBound:  8.0,
	}
	res, err := ApplyEvent(nil, ev(EventBoxCreated, RolePharmacy, p), base)
	if err != nil {
		t.Fatalf("box_created: %v", err)
	}
	return res.Box
}

func TestBoxCreatedRequiresNoExistingBox(t *testing.T) {
	box := boxCreated(t)
	if box.State != StateDrafted {
		t.Fatalf("state = %s, want drafted", box.State)
	}
	if box.Revision != 1 {
		t.Fatalf("revision = %d, want 1", box.Revision)
	}
	// Re-creating an existing box is illegal.
	if _, err := ApplyEvent(box, ev(EventBoxCreated, RolePharmacy, &BoxCreatedPayload{
		ProductName: "x", Batch: "b", Origin: "o", Destination: "d", CarrierID: "c",
	}), base); err == nil || err.Code != apperr.CodeIllegalTransition {
		t.Fatalf("re-create: want illegal_transition, got %v", err)
	}
}

func TestFullHandoffSequence(t *testing.T) {
	box := boxCreated(t)
	seg := ev(EventSegmentStarted, RoleCarrier, &SegmentStartedPayload{
		SegmentID: "SEG-1", CarrierID: "CARRIER-1", Origin: "DEPOT", Destination: "PHARMACY-A",
	})
	box = mustApply(t, box, seg)
	if box.State != StateInTransit {
		t.Fatalf("state = %s, want in_transit", box.State)
	}
	// In-range temperature summary keeps state.
	box = mustApply(t, box, ev(EventTemperatureSummary, RoleCarrier, &TemperatureSummaryPayload{
		SummaryID: "TS-1", SampleStart: base, SampleEnd: base.Add(time.Hour),
		SampleCount: 10, MinTemp: 4, MaxTemp: 6, AvgTemp: 5, LowerBound: 2, UpperBound: 8,
	}))
	if box.State != StateInTransit {
		t.Fatalf("state = %s, want in_transit (in-range temp)", box.State)
	}
	box = mustApply(t, box, ev(EventArrivalScanned, RoleCarrier, &ArrivalScannedPayload{Location: "PHARMACY-A"}))
	if box.State != StateAwaitingReceive {
		t.Fatalf("state = %s, want awaiting_receive", box.State)
	}
	box = mustApply(t, box, ev(EventCarrierSignedOut, RoleCarrier, &SignoffPayload{PartyID: "C1", PartyName: "Carrier One"}))
	if box.State != StateAwaitingPickup {
		t.Fatalf("state = %s, want awaiting_pickup", box.State)
	}
	box = mustApply(t, box, ev(EventPharmacyReceived, RolePharmacy, &SignoffPayload{PartyID: "P1", PartyName: "Pharmacy A"}))
	if box.State != StateHandedOver {
		t.Fatalf("state = %s, want handed_over", box.State)
	}
	box = mustApply(t, box, ev(EventClosed, RolePharmacy, &ClosedPayload{Reason: "done"}))
	if box.State != StateClosed {
		t.Fatalf("state = %s, want closed", box.State)
	}
	if box.Revision != 7 {
		t.Fatalf("revision = %d, want 7", box.Revision)
	}
}

func TestForbiddenTransitions(t *testing.T) {
	cases := []struct {
		name string
		from func(t *testing.T) *Box
		et   EventType
		role Role
		pl   Payload
	}{
		{"close from drafted", func(t *testing.T) *Box { return boxCreated(t) }, EventClosed, RolePharmacy, &ClosedPayload{Reason: "x"}},
		{"segment from awaiting_receive", func(t *testing.T) *Box {
			b := boxCreated(t)
			b = mustApply(t, b, ev(EventSegmentStarted, RoleCarrier, &SegmentStartedPayload{SegmentID: "S", CarrierID: "C", Origin: "O", Destination: "D"}))
			return mustApply(t, b, ev(EventArrivalScanned, RoleCarrier, &ArrivalScannedPayload{Location: "P"}))
		}, EventSegmentStarted, RoleCarrier, &SegmentStartedPayload{SegmentID: "S2", CarrierID: "C", Origin: "O", Destination: "D"}},
		{"pharmacy_received from in_transit", func(t *testing.T) *Box {
			b := boxCreated(t)
			return mustApply(t, b, ev(EventSegmentStarted, RoleCarrier, &SegmentStartedPayload{SegmentID: "S", CarrierID: "C", Origin: "O", Destination: "D"}))
		}, EventPharmacyReceived, RolePharmacy, &SignoffPayload{PartyID: "P", PartyName: "P"}},
		{"segment by pharmacy (wrong role)", func(t *testing.T) *Box { return boxCreated(t) }, EventSegmentStarted, RolePharmacy, &SegmentStartedPayload{SegmentID: "S", CarrierID: "C", Origin: "O", Destination: "D"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			box := c.from(t)
			_, err := ApplyEvent(box, ev(c.et, c.role, c.pl), base)
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if err.Code != apperr.CodeIllegalTransition {
				t.Fatalf("code = %s, want illegal_transition", err.Code)
			}
			if err.Category != apperr.CategoryDomain {
				t.Fatalf("category = %s, want domain", err.Category)
			}
			if err.Retryable {
				t.Fatalf("expected non-retryable")
			}
		})
	}
}

func TestTemperatureQuarantineBlocksOrdinaryOperations(t *testing.T) {
	box := boxCreated(t)
	box = mustApply(t, box, ev(EventSegmentStarted, RoleCarrier, &SegmentStartedPayload{SegmentID: "S", CarrierID: "C", Origin: "O", Destination: "D"}))
	// Out-of-range summary -> quarantine.
	box = mustApply(t, box, ev(EventTemperatureSummary, RoleCarrier, &TemperatureSummaryPayload{
		SummaryID: "TS", SampleStart: base, SampleEnd: base.Add(time.Hour),
		SampleCount: 5, MinTemp: 9, MaxTemp: 12, AvgTemp: 10, ExcursionDuration: time.Minute,
		LowerBound: 2, UpperBound: 8,
	}))
	if box.State != StateTemperatureQuarantine {
		t.Fatalf("state = %s, want temperature_quarantine", box.State)
	}
	if box.PreQuarantineState != StateInTransit {
		t.Fatalf("pre-quarantine = %s, want in_transit", box.PreQuarantineState)
	}
	// Ordinary close (pharmacy role) blocked.
	_, err := ApplyEvent(box, ev(EventClosed, RolePharmacy, &ClosedPayload{Reason: "x"}), base)
	if err == nil || err.Code != apperr.CodeIllegalTransition {
		t.Fatalf("close from quarantine by pharmacy: want illegal_transition, got %v", err)
	}
	// Exception handler can close.
	box = mustApply(t, box, ev(EventClosed, RoleExceptionHandler, &ClosedPayload{Reason: "disposed"}))
	if box.State != StateClosed {
		t.Fatalf("state = %s, want closed", box.State)
	}
}

func TestQuarantineReleaseRestoresState(t *testing.T) {
	box := boxCreated(t)
	box = mustApply(t, box, ev(EventSegmentStarted, RoleCarrier, &SegmentStartedPayload{SegmentID: "S", CarrierID: "C", Origin: "O", Destination: "D"}))
	box = mustApply(t, box, ev(EventArrivalScanned, RoleCarrier, &ArrivalScannedPayload{Location: "P"}))
	box = mustApply(t, box, ev(EventCarrierSignedOut, RoleCarrier, &SignoffPayload{PartyID: "C", PartyName: "C"}))
	box = mustApply(t, box, ev(EventTemperatureExceptionIsolated, RoleExceptionHandler, &TemperatureExceptionPayload{Reason: "manual hold"}))
	if box.State != StateTemperatureQuarantine {
		t.Fatalf("state = %s, want quarantine", box.State)
	}
	box = mustApply(t, box, ev(EventTemperatureExceptionIsolated, RoleExceptionHandler, &TemperatureExceptionPayload{Release: true, Reason: "cleared"}))
	if box.State != StateAwaitingPickup {
		t.Fatalf("state = %s, want awaiting_pickup (restored)", box.State)
	}
}

func TestStaleTransitionIsRevisionConflict(t *testing.T) {
	box := boxCreated(t)
	box = mustApply(t, box, ev(EventSegmentStarted, RoleCarrier, &SegmentStartedPayload{SegmentID: "S1", CarrierID: "C", Origin: "O", Destination: "D"}))
	// A second segment_started now sees in_transit (already in target) -> stale.
	e := ev(EventSegmentStarted, RoleCarrier, &SegmentStartedPayload{SegmentID: "S2", CarrierID: "C", Origin: "O", Destination: "D"})
	e.EventID = "segment_started-2"
	e.IdempotencyKey = "segment_started-2"
	e.TerminalSequence = 2
	_, err := ApplyEvent(box, e, base)
	if err == nil || err.Code != apperr.CodeRevisionConflict {
		t.Fatalf("stale segment: want revision_conflict, got %v", err)
	}
}

func TestTemperatureSummaryValidation(t *testing.T) {
	box := boxCreated(t)
	box = mustApply(t, box, ev(EventSegmentStarted, RoleCarrier, &SegmentStartedPayload{SegmentID: "S", CarrierID: "C", Origin: "O", Destination: "D"}))
	cases := []struct {
		name string
		pl   *TemperatureSummaryPayload
	}{
		{"start after end", &TemperatureSummaryPayload{SummaryID: "x", SampleStart: base.Add(time.Hour), SampleEnd: base, SampleCount: 1, MinTemp: 1, MaxTemp: 2, AvgTemp: 1.5, LowerBound: 0, UpperBound: 5}},
		{"avg out of [min,max]", &TemperatureSummaryPayload{SummaryID: "x", SampleStart: base, SampleEnd: base.Add(time.Hour), SampleCount: 1, MinTemp: 1, MaxTemp: 2, AvgTemp: 3, LowerBound: 0, UpperBound: 5}},
		{"excursion but in bounds", &TemperatureSummaryPayload{SummaryID: "x", SampleStart: base, SampleEnd: base.Add(time.Hour), SampleCount: 1, MinTemp: 1, MaxTemp: 2, AvgTemp: 1.5, ExcursionDuration: time.Minute, LowerBound: 0, UpperBound: 5}},
		{"out of bounds but no excursion", &TemperatureSummaryPayload{SummaryID: "x", SampleStart: base, SampleEnd: base.Add(time.Hour), SampleCount: 1, MinTemp: 9, MaxTemp: 10, AvgTemp: 9.5, LowerBound: 0, UpperBound: 5}},
		{"zero sample count", &TemperatureSummaryPayload{SummaryID: "x", SampleStart: base, SampleEnd: base.Add(time.Hour), SampleCount: 0, MinTemp: 1, MaxTemp: 2, AvgTemp: 1.5, LowerBound: 0, UpperBound: 5}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := ApplyEvent(box, ev(EventTemperatureSummary, RoleCarrier, c.pl), base)
			if err == nil || err.Code != apperr.CodeSchemaViolation {
				t.Fatalf("want schema_violation, got %v", err)
			}
		})
	}
}

var seqCounter int64

func mustApply(t *testing.T, box *Box, e *Event) *Box {
	t.Helper()
	seqCounter++
	e.TerminalSequence = seqCounter
	res, err := ApplyEvent(box, e, base)
	if err != nil {
		t.Fatalf("apply %s: %v", e.Type, err)
	}
	return res.Box
}
