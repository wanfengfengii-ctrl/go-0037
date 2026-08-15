package domain

import (
	"time"
)

// Box is the aggregate root projection rebuilt from accepted events. It is
// pure data: the apply logic in apply.go produces new copies rather than
// mutating in place, so callers can safely keep references for comparisons.
type Box struct {
	BoxID              string               `json:"box_id"`
	State              State                `json:"state"`
	Revision           int64                `json:"revision"`
	Segments           []Segment            `json:"segments,omitempty"`
	Signoffs           SignoffSummary       `json:"signoffs"`
	TempSummaries      []TemperatureSummary `json:"temperature_summaries,omitempty"`
	PreQuarantineState State                `json:"pre_quarantine_state,omitempty"`
	Metadata           map[string]string    `json:"metadata,omitempty"`
	CreatedAt          time.Time            `json:"created_at"`
	UpdatedAt          time.Time            `json:"updated_at"`
	LastEventID        string               `json:"last_event_id"`
	Quarantined        bool                 `json:"quarantined,omitempty"`
}

// Segment summarises a transport leg for a box.
type Segment struct {
	SegmentID   string    `json:"segment_id"`
	CarrierID   string    `json:"carrier_id"`
	Origin      string    `json:"origin"`
	Destination string    `json:"destination"`
	StartedAt   time.Time `json:"started_at,omitempty"`
	ArrivedAt   time.Time `json:"arrived_at,omitempty"`
	SignedOutAt time.Time `json:"signed_out_at,omitempty"`
	Status      string    `json:"status"` // started | arrived | signed_out
}

// SignoffSummary records both parties' handoff signatures.
type SignoffSummary struct {
	Carrier  *Signoff `json:"carrier,omitempty"`
	Pharmacy *Signoff `json:"pharmacy,omitempty"`
}

// Signoff is a single responsible-party signature.
type Signoff struct {
	EventID   string    `json:"event_id"`
	PartyID   string    `json:"party_id"`
	PartyName string    `json:"party_name"`
	SignedAt  time.Time `json:"signed_at"`
}

// TemperatureSummary records a sampled temperature window for a box.
type TemperatureSummary struct {
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
	OutOfRange        bool          `json:"out_of_range"`
	EventID           string        `json:"event_id"`
}

// CurrentSegment returns the most recently started segment, or nil if none.
func (b *Box) CurrentSegment() *Segment {
	if b == nil || len(b.Segments) == 0 {
		return nil
	}
	return &b.Segments[len(b.Segments)-1]
}

// Clone returns a deep copy of the box so apply logic can mutate freely.
func (b *Box) Clone() *Box {
	if b == nil {
		return nil
	}
	out := *b
	if b.Segments != nil {
		out.Segments = append([]Segment(nil), b.Segments...)
	}
	if b.TempSummaries != nil {
		out.TempSummaries = append([]TemperatureSummary(nil), b.TempSummaries...)
	}
	if b.Metadata != nil {
		out.Metadata = make(map[string]string, len(b.Metadata))
		for k, v := range b.Metadata {
			out.Metadata[k] = v
		}
	}
	if b.Signoffs.Carrier != nil {
		c := *b.Signoffs.Carrier
		out.Signoffs.Carrier = &c
	}
	if b.Signoffs.Pharmacy != nil {
		p := *b.Signoffs.Pharmacy
		out.Signoffs.Pharmacy = &p
	}
	return &out
}
