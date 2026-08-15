package domain

import "fmt"

// Transition classifies the outcome of attempting an event against a box.
type Transition int

const (
	// TransitionLegal means the event is valid from the current state.
	TransitionLegal Transition = iota
	// TransitionStale means the box has already advanced to (or past) the
	// event's target state, so the event is a redundant or concurrent loser.
	// This is surfaced as revision_conflict.
	TransitionStale
	// TransitionIllegal means the event is never valid from the current state.
	// This is surfaced as illegal_transition.
	TransitionIllegal
)

// ClassifyTransition decides how to treat an event given the current state.
//
//   - If the current state is one of the event's valid source states, the
//     transition is legal.
//   - Else, if the current state equals the event's target state (a concurrent
//     event already performed this exact transition), the transition is stale
//     and the caller returns revision_conflict.
//   - Otherwise the transition is illegal.
//
// The target is computed by TargetFor, which accounts for the dynamic targets
// of the temperature summary and isolation events.
// validFrom returns the source states from which ev may legally originate.
// It is payload-aware: a release-temperature-exception is valid only from
// quarantine, while an isolation is valid from the active transport states.
func validFrom(ev *Event) []State {
	if ev.Type == EventTemperatureExceptionIsolated {
		if p, ok := ev.Payload.(*TemperatureExceptionPayload); ok && p.Release {
			return []State{StateTemperatureQuarantine}
		}
	}
	return assertRule(ev.Type).from
}

func ClassifyTransition(current State, ev *Event) Transition {
	if current == StateNone {
		// Only box_created is legal from the absent box.
		if ev.Type == EventBoxCreated {
			return TransitionLegal
		}
		return TransitionIllegal
	}
	if stateIn(validFrom(ev), current) {
		return TransitionLegal
	}
	target := TargetFor(current, ev)
	if target == current && current != StateNone {
		// Already in the target state: a concurrent winner got there first.
		// This only counts as stale for genuinely state-changing events.
		if isStateChanging(ev) {
			return TransitionStale
		}
	}
	return TransitionIllegal
}

// isStateChanging reports whether the event changes the box state for the
// given current state (as opposed to merely appending data, like an in-range
// temperature summary).
func isStateChanging(ev *Event) bool {
	switch ev.Type {
	case EventTemperatureSummary:
		// Changes state only when out of range.
		ts, ok := ev.Payload.(*TemperatureSummaryPayload)
		return ok && ts.OutOfRange()
	default:
		return true
	}
}

// TargetFor returns the state the box would enter if ev were applied to a box
// currently in `current`. It does not validate that the transition is legal;
// callers should use ClassifyTransition first.
func TargetFor(current State, ev *Event) State {
	switch ev.Type {
	case EventBoxCreated:
		return StateDrafted
	case EventSegmentStarted:
		return StateInTransit
	case EventArrivalScanned:
		return StateAwaitingReceive
	case EventCarrierSignedOut:
		return StateAwaitingPickup
	case EventPharmacyReceived:
		return StateHandedOver
	case EventClosed:
		return StateClosed
	case EventTemperatureExceptionIsolated:
		p := ev.Payload.(*TemperatureExceptionPayload)
		if p.Release {
			// Restores the pre-quarantine state if known, else handed_over.
			return StateHandedOver
		}
		return StateTemperatureQuarantine
	case EventTemperatureSummary:
		ts, _ := ev.Payload.(*TemperatureSummaryPayload)
		if ts != nil && ts.OutOfRange() {
			return StateTemperatureQuarantine
		}
		return current
	}
	return current
}

// RoleAllowed reports whether role is permitted to submit ev from the current
// state. Role constraints may depend on the source state (for example, closing
// from quarantine requires the exception_handler role).
func RoleAllowed(current State, ev *Event, role Role) bool {
	r := assertRule(ev.Type)
	if !roleAllowed(r.roles, role) {
		return false
	}
	// Closing from quarantine requires the exception handler specifically.
	if ev.Type == EventClosed && current == StateTemperatureQuarantine {
		return role == RoleExceptionHandler
	}
	return true
}

// ValidateStatic performs the state-machine-level validation shared by all
// events before their payload-specific effects are applied. It returns the
// transition classification and, if illegal, the apperr code that should be
// reported.
func ValidateStatic(current State, ev *Event) (Transition, string) {
	if !RoleAllowed(current, ev, ev.Role) {
		return TransitionIllegal, fmt.Sprintf("role %s is not authorized for %s from %s", ev.Role, ev.Type, current)
	}
	t := ClassifyTransition(current, ev)
	switch t {
	case TransitionStale:
		return t, fmt.Sprintf("box already in %s; %s is stale", current, ev.Type)
	case TransitionIllegal:
		return t, fmt.Sprintf("%s is not valid from %s", ev.Type, current)
	}
	return t, ""
}
