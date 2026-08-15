// Package domain models the medical cold-chain handoff ledger's pure domain:
// box aggregate state, event types, responsible roles, the state machine that
// governs transitions, and the projection that is rebuilt from accepted events.
//
// The package has no I/O dependencies: it knows nothing about JSON transport,
// storage or concurrency. Those concerns live in the protocol, store and
// coordinator packages, which build on top of the types defined here.
package domain

import "fmt"

// State is the lifecycle stage of a box aggregate.
type State string

const (
	// StateNone is the absence of a box (used only as a source for
	// box_created). It is never persisted as a box state.
	StateNone State = ""
	// StateDrafted (已建档): the box has been registered by the pharmacy but
	// transport has not begun.
	StateDrafted State = "drafted"
	// StateAwaitingPickup (待提货): the carrier has signed the box out to the
	// receiving area and the pharmacy is expected to pick it up.
	StateAwaitingPickup State = "awaiting_pickup"
	// StateInTransit (运输中): the box is moving between origin and destination.
	StateInTransit State = "in_transit"
	// StateAwaitingReceive (待接收): the box has arrived at the destination
	// and is awaiting the carrier's handoff.
	StateAwaitingReceive State = "awaiting_receive"
	// StateHandedOver (已交接): the pharmacy has signed for the box.
	StateHandedOver State = "handed_over"
	// StateTemperatureQuarantine (温度隔离): an out-of-range excursion or
	// manual isolation has quarantined the box until an exception handler
	// resolves or closes it.
	StateTemperatureQuarantine State = "temperature_quarantine"
	// StateClosed (已关闭): the box lifecycle is terminal.
	StateClosed State = "closed"
)

// String returns a human-readable label for the state.
func (s State) String() string {
	switch s {
	case StateNone:
		return "none"
	case StateDrafted:
		return "drafted"
	case StateAwaitingPickup:
		return "awaiting_pickup"
	case StateInTransit:
		return "in_transit"
	case StateAwaitingReceive:
		return "awaiting_receive"
	case StateHandedOver:
		return "handed_over"
	case StateTemperatureQuarantine:
		return "temperature_quarantine"
	case StateClosed:
		return "closed"
	}
	return string(s)
}

// IsTerminal reports whether no further state-changing transition is possible.
func (s State) IsTerminal() bool { return s == StateClosed }

// EventType enumerates the events that may be recorded against a box.
type EventType string

const (
	EventBoxCreated                   EventType = "box_created"
	EventSegmentStarted               EventType = "segment_started"
	EventTemperatureSummary           EventType = "temperature_summary_submitted"
	EventArrivalScanned               EventType = "arrival_scanned"
	EventCarrierSignedOut             EventType = "carrier_signed_out"
	EventPharmacyReceived             EventType = "pharmacy_received"
	EventTemperatureExceptionIsolated EventType = "temperature_exception_isolated"
	EventClosed                       EventType = "closed"
)

// SupportedEventTypes is the closed set of event types accepted by the ledger.
var SupportedEventTypes = []EventType{
	EventBoxCreated,
	EventSegmentStarted,
	EventTemperatureSummary,
	EventArrivalScanned,
	EventCarrierSignedOut,
	EventPharmacyReceived,
	EventTemperatureExceptionIsolated,
	EventClosed,
}

// IsValidEventType reports whether t is a supported event type.
func IsValidEventType(t EventType) bool {
	for _, s := range SupportedEventTypes {
		if s == t {
			return true
		}
	}
	return false
}

// Role identifies the responsible party submitting an event.
type Role string

const (
	RolePharmacy         Role = "pharmacy"
	RoleCarrier          Role = "carrier"
	RoleExceptionHandler Role = "exception_handler"
)

// SupportedRoles is the closed set of roles.
var SupportedRoles = []Role{RolePharmacy, RoleCarrier, RoleExceptionHandler}

// IsValidRole reports whether r is a supported role.
func IsValidRole(r Role) bool {
	for _, s := range SupportedRoles {
		if s == r {
			return true
		}
	}
	return false
}

// ProtocolVersion is the only wire protocol version currently supported.
const ProtocolVersion = "1.0"

// rule captures the static part of a transition: the states an event may
// originate from and the roles permitted to submit it. Dynamic targets (for
// the temperature summary and isolation events) are resolved in apply.go.
type rule struct {
	from  []State
	roles []Role
}

// rules maps each event type to its static rule. box_created uses a nil
// "from" to denote the absent-box source; every other event lists the states
// it may legally follow.
var rules = map[EventType]rule{
	EventBoxCreated:                   {from: nil, roles: []Role{RolePharmacy}},
	EventSegmentStarted:               {from: []State{StateDrafted}, roles: []Role{RoleCarrier}},
	EventTemperatureSummary:           {from: []State{StateInTransit, StateAwaitingReceive, StateAwaitingPickup}, roles: []Role{RoleCarrier, RolePharmacy}},
	EventArrivalScanned:               {from: []State{StateInTransit}, roles: []Role{RoleCarrier}},
	EventCarrierSignedOut:             {from: []State{StateAwaitingReceive}, roles: []Role{RoleCarrier}},
	EventPharmacyReceived:             {from: []State{StateAwaitingPickup}, roles: []Role{RolePharmacy}},
	EventTemperatureExceptionIsolated: {from: []State{StateInTransit, StateAwaitingReceive, StateAwaitingPickup, StateHandedOver}, roles: []Role{RoleExceptionHandler}},
	EventClosed:                       {from: []State{StateHandedOver, StateTemperatureQuarantine}, roles: []Role{RolePharmacy, RoleExceptionHandler}},
}

func roleAllowed(allowed []Role, r Role) bool {
	for _, a := range allowed {
		if a == r {
			return true
		}
	}
	return false
}

func stateIn(set []State, s State) bool {
	for _, x := range set {
		if x == s {
			return true
		}
	}
	return false
}

// String implements fmt.Stringer for EventType to satisfy linters and logging.
func (e EventType) String() string { return string(e) }

// String implements fmt.Stringer for Role.
func (r Role) String() string { return string(r) }

// assertRule returns the rule for an event type, panicking if none is
// registered. This guards against a misconfigured event table at startup.
func assertRule(t EventType) rule {
	r, ok := rules[t]
	if !ok {
		panic(fmt.Sprintf("domain: no rule registered for event type %s", t))
	}
	return r
}
