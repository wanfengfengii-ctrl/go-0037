package domain

import (
	"fmt"
	"time"

	"medcold-handoff-ledger/internal/apperr"
)

// ApplyResult describes the effect of applying an event to a box.
type ApplyResult struct {
	Box          *Box
	StateChanged bool
	NewState     State
}

// ApplyEvent attempts to apply ev to box (which may be nil for box_created).
// On success it returns a new box (a copy; the input is never mutated) and a
// nil error. On failure it returns (nil, *apperr.Error).
//
// ApplyEvent only performs the pure domain transition. It does not consult
// causal dependencies, idempotency keys or persistence; the coordinator layer
// wraps it with those concerns.
func ApplyEvent(box *Box, ev *Event, now time.Time) (*ApplyResult, *apperr.Error) {
	// First validate payload-specific invariants.
	if err := ev.Payload.Validate(); err != nil {
		if ae, ok := err.(*apperr.Error); ok {
			return nil, ae
		}
		return nil, apperr.SchemaViolation(err.Error())
	}

	if box == nil {
		if ev.Type != EventBoxCreated {
			return nil, apperr.IllegalTransition(fmt.Sprintf("box does not exist; %s requires an existing box", ev.Type))
		}
	} else {
		// An existing box may never be re-created.
		if ev.Type == EventBoxCreated {
			return nil, apperr.IllegalTransition("box already exists")
		}
	}

	cls, reason := ValidateStatic(boxState(box), ev)
	switch cls {
	case TransitionStale:
		return nil, apperr.RevisionConflict(reason)
	case TransitionIllegal:
		return nil, apperr.IllegalTransition(reason)
	}

	switch ev.Type {
	case EventBoxCreated:
		return applyBoxCreated(box, ev, now)
	case EventSegmentStarted:
		return applySegmentStarted(box, ev, now)
	case EventTemperatureSummary:
		return applyTemperatureSummary(box, ev, now)
	case EventArrivalScanned:
		return applyArrivalScanned(box, ev, now)
	case EventCarrierSignedOut:
		return applyCarrierSignedOut(box, ev, now)
	case EventPharmacyReceived:
		return applyPharmacyReceived(box, ev, now)
	case EventTemperatureExceptionIsolated:
		return applyTemperatureException(box, ev, now)
	case EventClosed:
		return applyClosed(box, ev, now)
	}
	return nil, apperr.IllegalTransition(fmt.Sprintf("unknown event type %s", ev.Type))
}

func boxState(box *Box) State {
	if box == nil {
		return StateNone
	}
	return box.State
}

func bump(box *Box, newState State, changed bool, ev *Event, now time.Time) *ApplyResult {
	box.Revision++
	box.State = newState
	box.UpdatedAt = now
	box.LastEventID = ev.EventID
	if changed {
		box.Quarantined = newState == StateTemperatureQuarantine
	}
	return &ApplyResult{Box: box, StateChanged: changed, NewState: newState}
}

func applyBoxCreated(_ *Box, ev *Event, now time.Time) (*ApplyResult, *apperr.Error) {
	p := ev.Payload.(*BoxCreatedPayload)
	box := &Box{
		BoxID:       ev.BoxID,
		State:       StateDrafted,
		Revision:    1,
		CreatedAt:   ev.OccurredAt,
		UpdatedAt:   now,
		LastEventID: ev.EventID,
		Metadata: map[string]string{
			"product_name": p.ProductName,
			"batch":        p.Batch,
			"origin":       p.Origin,
			"destination":  p.Destination,
			"carrier_id":   p.CarrierID,
		},
	}
	box.Quarantined = false
	return &ApplyResult{Box: box, StateChanged: true, NewState: StateDrafted}, nil
}

func applySegmentStarted(box *Box, ev *Event, now time.Time) (*ApplyResult, *apperr.Error) {
	p := ev.Payload.(*SegmentStartedPayload)
	box = box.Clone()
	box.Segments = append(box.Segments, Segment{
		SegmentID:   p.SegmentID,
		CarrierID:   p.CarrierID,
		Origin:      p.Origin,
		Destination: p.Destination,
		StartedAt:   ev.OccurredAt,
		Status:      "started",
	})
	return bump(box, StateInTransit, true, ev, now), nil
}

func applyTemperatureSummary(box *Box, ev *Event, now time.Time) (*ApplyResult, *apperr.Error) {
	p := ev.Payload.(*TemperatureSummaryPayload)
	box = box.Clone()
	box.TempSummaries = append(box.TempSummaries, TemperatureSummary{
		SummaryID:         p.SummaryID,
		SampleStart:       p.SampleStart,
		SampleEnd:         p.SampleEnd,
		SampleCount:       p.SampleCount,
		MinTemp:           p.MinTemp,
		MaxTemp:           p.MaxTemp,
		AvgTemp:           p.AvgTemp,
		ExcursionDuration: p.ExcursionDuration,
		LowerBound:        p.LowerBound,
		UpperBound:        p.UpperBound,
		OutOfRange:        p.OutOfRange(),
		EventID:           ev.EventID,
	})
	if p.OutOfRange() {
		box.PreQuarantineState = box.State
		return bump(box, StateTemperatureQuarantine, true, ev, now), nil
	}
	return bump(box, box.State, false, ev, now), nil
}

func applyArrivalScanned(box *Box, ev *Event, now time.Time) (*ApplyResult, *apperr.Error) {
	p := ev.Payload.(*ArrivalScannedPayload)
	box = box.Clone()
	if seg := box.CurrentSegment(); seg != nil {
		seg.ArrivedAt = ev.OccurredAt
		seg.Status = "arrived"
	}
	_ = p
	return bump(box, StateAwaitingReceive, true, ev, now), nil
}

func applyCarrierSignedOut(box *Box, ev *Event, now time.Time) (*ApplyResult, *apperr.Error) {
	p := ev.Payload.(*SignoffPayload)
	box = box.Clone()
	if seg := box.CurrentSegment(); seg != nil {
		seg.SignedOutAt = ev.OccurredAt
		seg.Status = "signed_out"
	}
	box.Signoffs.Carrier = &Signoff{
		EventID:   ev.EventID,
		PartyID:   p.PartyID,
		PartyName: p.PartyName,
		SignedAt:  ev.OccurredAt,
	}
	return bump(box, StateAwaitingPickup, true, ev, now), nil
}

func applyPharmacyReceived(box *Box, ev *Event, now time.Time) (*ApplyResult, *apperr.Error) {
	p := ev.Payload.(*SignoffPayload)
	box = box.Clone()
	box.Signoffs.Pharmacy = &Signoff{
		EventID:   ev.EventID,
		PartyID:   p.PartyID,
		PartyName: p.PartyName,
		SignedAt:  ev.OccurredAt,
	}
	return bump(box, StateHandedOver, true, ev, now), nil
}

func applyTemperatureException(box *Box, ev *Event, now time.Time) (*ApplyResult, *apperr.Error) {
	p := ev.Payload.(*TemperatureExceptionPayload)
	box = box.Clone()
	if p.Release {
		// Restore pre-quarantine state; if unknown, default to handed_over.
		target := box.PreQuarantineState
		if target == StateNone {
			target = StateHandedOver
		}
		box.PreQuarantineState = StateNone
		box.Quarantined = false
		return bump(box, target, true, ev, now), nil
	}
	box.PreQuarantineState = box.State
	return bump(box, StateTemperatureQuarantine, true, ev, now), nil
}

func applyClosed(box *Box, ev *Event, now time.Time) (*ApplyResult, *apperr.Error) {
	box = box.Clone()
	box.Quarantined = false
	return bump(box, StateClosed, true, ev, now), nil
}
