package domain

import (
	"fmt"
	"math"
	"time"

	"medcold-handoff-ledger/internal/apperr"
)

// Payload is the typed, validated body of an event. Each concrete payload
// knows its event type and can validate its own invariants.
type Payload interface {
	EventType() EventType
	Validate() error // returns *apperr.Error on failure
}

// Event is the decoded, validated representation of a submitted event. The
// coordinator builds it from the wire envelope and feeds it to ApplyEvent.
type Event struct {
	EventID            string
	IdempotencyKey     string
	TerminalID         string
	TerminalSequence   int64
	BoxID              string
	Type               EventType
	Role               Role
	OccurredAt         time.Time
	PredecessorEventID string
	Payload            Payload
}

// ---- Concrete payloads ----

// BoxCreatedPayload registers a new box.
type BoxCreatedPayload struct {
	ProductName string  `json:"product_name"`
	Batch       string  `json:"batch"`
	Origin      string  `json:"origin"`
	Destination string  `json:"destination"`
	CarrierID   string  `json:"carrier_id"`
	LowerBound  float64 `json:"lower_bound"`
	UpperBound  float64 `json:"upper_bound"`
}

// EventType implements Payload.
func (p *BoxCreatedPayload) EventType() EventType { return EventBoxCreated }

// Validate implements Payload.
func (p *BoxCreatedPayload) Validate() error {
	if p.ProductName == "" {
		return apperr.SchemaViolation("product_name is required")
	}
	if p.Batch == "" {
		return apperr.SchemaViolation("batch is required")
	}
	if p.Origin == "" {
		return apperr.SchemaViolation("origin is required")
	}
	if p.Destination == "" {
		return apperr.SchemaViolation("destination is required")
	}
	if p.CarrierID == "" {
		return apperr.SchemaViolation("carrier_id is required")
	}
	if err := checkFinite("lower_bound", p.LowerBound); err != nil {
		return err
	}
	if err := checkFinite("upper_bound", p.UpperBound); err != nil {
		return err
	}
	if p.LowerBound > p.UpperBound {
		return apperr.SchemaViolation("lower_bound must not exceed upper_bound")
	}
	return nil
}

// SegmentStartedPayload opens a transport segment.
type SegmentStartedPayload struct {
	SegmentID   string `json:"segment_id"`
	CarrierID   string `json:"carrier_id"`
	Origin      string `json:"origin"`
	Destination string `json:"destination"`
}

// EventType implements Payload.
func (p *SegmentStartedPayload) EventType() EventType { return EventSegmentStarted }

// Validate implements Payload.
func (p *SegmentStartedPayload) Validate() error {
	if p.SegmentID == "" {
		return apperr.SchemaViolation("segment_id is required")
	}
	if p.CarrierID == "" {
		return apperr.SchemaViolation("carrier_id is required")
	}
	if p.Origin == "" {
		return apperr.SchemaViolation("origin is required")
	}
	if p.Destination == "" {
		return apperr.SchemaViolation("destination is required")
	}
	return nil
}

// TemperatureSummaryPayload records a sampled temperature window.
type TemperatureSummaryPayload struct {
	SummaryID         string        `json:"summary_id"`
	SampleStart       time.Time     `json:"sample_start"`
	SampleEnd         time.Time     `json:"sample_end"`
	SampleCount       int           `json:"sample_count"`
	MinTemp           float64       `json:"min_temp"`
	MaxTemp           float64       `json:"max_temp"`
	AvgTemp           float64       `json:"avg_temp"`
	ExcursionDuration time.Duration `json:"excursion_duration"`
	LowerBound        float64       `json:"lower_bound"`
	UpperBound        float64       `json:"upper_bound"`
}

// EventType implements Payload.
func (p *TemperatureSummaryPayload) EventType() EventType { return EventTemperatureSummary }

// OutOfRange reports whether the summary indicates a temperature excursion.
func (p *TemperatureSummaryPayload) OutOfRange() bool {
	return p.MinTemp < p.LowerBound || p.MaxTemp > p.UpperBound || p.ExcursionDuration > 0
}

// Validate implements Payload.
func (p *TemperatureSummaryPayload) Validate() error {
	if p.SummaryID == "" {
		return apperr.SchemaViolation("summary_id is required")
	}
	if p.SampleCount <= 0 {
		return apperr.SchemaViolation("sample_count must be positive")
	}
	if !p.SampleStart.Before(p.SampleEnd) {
		return apperr.SchemaViolation("sample_start must precede sample_end")
	}
	if err := checkFinite("min_temp", p.MinTemp); err != nil {
		return err
	}
	if err := checkFinite("max_temp", p.MaxTemp); err != nil {
		return err
	}
	if err := checkFinite("avg_temp", p.AvgTemp); err != nil {
		return err
	}
	if err := checkFinite("lower_bound", p.LowerBound); err != nil {
		return err
	}
	if err := checkFinite("upper_bound", p.UpperBound); err != nil {
		return err
	}
	if p.MinTemp > p.MaxTemp {
		return apperr.SchemaViolation("min_temp must not exceed max_temp")
	}
	if p.LowerBound > p.UpperBound {
		return apperr.SchemaViolation("lower_bound must not exceed upper_bound")
	}
	if p.AvgTemp < p.MinTemp || p.AvgTemp > p.MaxTemp {
		return apperr.SchemaViolation("avg_temp must lie within [min_temp, max_temp]")
	}
	if p.ExcursionDuration < 0 {
		return apperr.SchemaViolation("excursion_duration must not be negative")
	}
	// Consistency between excursion duration and observed bounds.
	boundsViolation := p.MinTemp < p.LowerBound || p.MaxTemp > p.UpperBound
	if p.ExcursionDuration > 0 && !boundsViolation {
		return apperr.SchemaViolation("excursion_duration > 0 but all samples within bounds")
	}
	if p.ExcursionDuration == 0 && boundsViolation {
		return apperr.SchemaViolation("samples out of bounds but excursion_duration is 0")
	}
	return nil
}

// ArrivalScannedPayload records a box arrival at the destination.
type ArrivalScannedPayload struct {
	Location string `json:"location"`
}

// EventType implements Payload.
func (p *ArrivalScannedPayload) EventType() EventType { return EventArrivalScanned }

// Validate implements Payload.
func (p *ArrivalScannedPayload) Validate() error {
	if p.Location == "" {
		return apperr.SchemaViolation("location is required")
	}
	return nil
}

// SignoffPayload records a responsible-party signature (carrier or pharmacy).
type SignoffPayload struct {
	PartyID   string `json:"party_id"`
	PartyName string `json:"party_name"`
}

// EventType implements Payload.
func (p *SignoffPayload) EventType() EventType { return EventCarrierSignedOut }

// Validate implements Payload.
func (p *SignoffPayload) Validate() error {
	if p.PartyID == "" {
		return apperr.SchemaViolation("party_id is required")
	}
	if p.PartyName == "" {
		return apperr.SchemaViolation("party_name is required")
	}
	return nil
}

// TemperatureExceptionPayload either isolates a box or releases it from
// quarantine. Release requires the exception_handler role.
type TemperatureExceptionPayload struct {
	Release bool   `json:"release"`
	Reason  string `json:"reason"`
}

// EventType implements Payload.
func (p *TemperatureExceptionPayload) EventType() EventType {
	return EventTemperatureExceptionIsolated
}

// Validate implements Payload.
func (p *TemperatureExceptionPayload) Validate() error {
	if p.Reason == "" {
		return apperr.SchemaViolation("reason is required")
	}
	return nil
}

// ClosedPayload terminates a box lifecycle.
type ClosedPayload struct {
	Reason string `json:"reason"`
}

// EventType implements Payload.
func (p *ClosedPayload) EventType() EventType { return EventClosed }

// Validate implements Payload.
func (p *ClosedPayload) Validate() error {
	if p.Reason == "" {
		return apperr.SchemaViolation("reason is required")
	}
	return nil
}

// NewPayload returns a zero-value payload for the given event type, or nil if
// the type is unknown. The caller is expected to have already validated the
// event type.
func NewPayload(t EventType) Payload {
	switch t {
	case EventBoxCreated:
		return &BoxCreatedPayload{}
	case EventSegmentStarted:
		return &SegmentStartedPayload{}
	case EventTemperatureSummary:
		return &TemperatureSummaryPayload{}
	case EventArrivalScanned:
		return &ArrivalScannedPayload{}
	case EventCarrierSignedOut, EventPharmacyReceived:
		return &SignoffPayload{}
	case EventTemperatureExceptionIsolated:
		return &TemperatureExceptionPayload{}
	case EventClosed:
		return &ClosedPayload{}
	}
	return nil
}

// PayloadForType returns the concrete payload type name for diagnostics.
func PayloadForType(t EventType) string {
	switch t {
	case EventBoxCreated:
		return "BoxCreatedPayload"
	case EventSegmentStarted:
		return "SegmentStartedPayload"
	case EventTemperatureSummary:
		return "TemperatureSummaryPayload"
	case EventArrivalScanned:
		return "ArrivalScannedPayload"
	case EventCarrierSignedOut, EventPharmacyReceived:
		return "SignoffPayload"
	case EventTemperatureExceptionIsolated:
		return "TemperatureExceptionPayload"
	case EventClosed:
		return "ClosedPayload"
	}
	return "unknown"
}

func checkFinite(name string, v float64) error {
	if math.IsNaN(v) {
		return apperr.SchemaViolation(fmt.Sprintf("%s must be a finite number, got NaN", name))
	}
	if math.IsInf(v, 0) {
		return apperr.SchemaViolation(fmt.Sprintf("%s must be a finite number, got infinity", name))
	}
	return nil
}
