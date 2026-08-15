package coordinator

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"medcold-handoff-ledger/internal/domain"
	"medcold-handoff-ledger/internal/store"
)

// BoxView is the query projection of a box, including its audit revision.
type BoxView struct {
	BoxID         string                      `json:"box_id"`
	State         string                      `json:"state"`
	Revision      int64                       `json:"revision"`
	Segments      []domain.Segment            `json:"segments,omitempty"`
	Signoffs      domain.SignoffSummary       `json:"signoffs"`
	TempSummaries []domain.TemperatureSummary `json:"temperature_summaries,omitempty"`
	Quarantined   bool                        `json:"quarantined,omitempty"`
	CreatedAt     time.Time                   `json:"created_at"`
	UpdatedAt     time.Time                   `json:"updated_at"`
	LastEventID   string                      `json:"last_event_id"`
}

// GetBox returns the current projection of a box, or nil if it does not exist.
func (c *Coordinator) GetBox(boxID string) (*BoxView, error) {
	box, err := c.boxProjection(boxID)
	if err != nil {
		return nil, err
	}
	if box == nil {
		return nil, nil
	}
	return &BoxView{
		BoxID:         box.BoxID,
		State:         string(box.State),
		Revision:      box.Revision,
		Segments:      box.Segments,
		Signoffs:      box.Signoffs,
		TempSummaries: box.TempSummaries,
		Quarantined:   box.Quarantined,
		CreatedAt:     box.CreatedAt,
		UpdatedAt:     box.UpdatedAt,
		LastEventID:   box.LastEventID,
	}, nil
}

// EventLineItem is one entry in a box's audit timeline.
type EventLineItem struct {
	EventID            string    `json:"event_id"`
	EventType          string    `json:"event_type"`
	Role               string    `json:"role"`
	Status             string    `json:"status"`
	OccurredAt         time.Time `json:"occurred_at"`
	ReceivedAt         time.Time `json:"received_at"`
	TerminalID         string    `json:"terminal_id"`
	TerminalSequence   int64     `json:"terminal_sequence"`
	Revision           int64     `json:"revision,omitempty"`
	NewState           string    `json:"new_state,omitempty"`
	PredecessorEventID string    `json:"predecessor_event_id,omitempty"`
}

// TimelinePage is a page of a box's audit timeline.
type TimelinePage struct {
	Items      []EventLineItem `json:"items"`
	NextCursor string          `json:"next_cursor,omitempty"`
	Revision   int64           `json:"revision"`
}

// EventSortKey is the stable ordering tuple for audit events: occurred_at,
// terminal_id, terminal_sequence, event_id.
type EventSortKey struct {
	OccurredAt       time.Time
	TerminalID       string
	TerminalSequence int64
	EventID          string
}

// Cursor encodes a sort key for pagination.
type Cursor struct {
	OccurredAt       time.Time `json:"occurred_at"`
	TerminalID       string    `json:"terminal_id"`
	TerminalSequence int64     `json:"terminal_sequence"`
	EventID          string    `json:"event_id"`
}

// sortKeyFor derives the sort key from a record.
func sortKeyFor(r *store.Record) EventSortKey {
	return EventSortKey{r.OccurredAt, r.TerminalID, r.TerminalSequence, r.EventID}
}

// after reports whether the record sorts strictly after the cursor.
func (c Cursor) after(r *store.Record) bool {
	if !r.OccurredAt.Equal(c.OccurredAt) {
		return r.OccurredAt.After(c.OccurredAt)
	}
	if r.TerminalID != c.TerminalID {
		return r.TerminalID > c.TerminalID
	}
	if r.TerminalSequence != c.TerminalSequence {
		return r.TerminalSequence > c.TerminalSequence
	}
	return r.EventID > c.EventID
}

// EncodeCursor builds an opaque pagination cursor from a record.
func EncodeCursor(r *store.Record) (string, error) {
	c := Cursor{r.OccurredAt, r.TerminalID, r.TerminalSequence, r.EventID}
	b, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	return encodeBase64URL(b), nil
}

// DecodeCursor parses an opaque pagination cursor.
func DecodeCursor(s string) (*Cursor, error) {
	if s == "" {
		return nil, nil
	}
	b, err := decodeBase64URL(s)
	if err != nil {
		return nil, err
	}
	var c Cursor
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	return &c, nil
}

// Timeline returns a page of the box's audit events in stable order.
func (c *Coordinator) Timeline(boxID string, cursor string, limit int) (*TimelinePage, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	cur, err := DecodeCursor(cursor)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor: %w", err)
	}
	var records []*store.Record
	viewErr := c.store.View(func(tx *store.Tx) error {
		return tx.ForEachRecord(func(r *store.Record) error {
			if r.BoxID == boxID {
				records = append(records, r)
			}
			return nil
		})
	})
	if viewErr != nil {
		return nil, viewErr
	}
	sort.Slice(records, func(i, j int) bool {
		ki, kj := sortKeyFor(records[i]), sortKeyFor(records[j])
		if !ki.OccurredAt.Equal(kj.OccurredAt) {
			return ki.OccurredAt.Before(kj.OccurredAt)
		}
		if ki.TerminalID != kj.TerminalID {
			return ki.TerminalID < kj.TerminalID
		}
		if ki.TerminalSequence != kj.TerminalSequence {
			return ki.TerminalSequence < kj.TerminalSequence
		}
		return ki.EventID < kj.EventID
	})
	page := &TimelinePage{}
	var last *store.Record
	for _, r := range records {
		if cur != nil && !cur.after(r) {
			continue
		}
		if len(page.Items) >= limit {
			page.NextCursor, _ = EncodeCursor(last)
			break
		}
		page.Items = append(page.Items, EventLineItem{
			EventID:            r.EventID,
			EventType:          r.EventType,
			Role:               r.Role,
			Status:             string(r.Status),
			OccurredAt:         r.OccurredAt,
			ReceivedAt:         r.ReceivedAt,
			TerminalID:         r.TerminalID,
			TerminalSequence:   r.TerminalSequence,
			Revision:           r.Revision,
			NewState:           r.NewState,
			PredecessorEventID: r.PredecessorEventID,
		})
		last = r
	}
	// Revision is the box's current revision.
	if box, _ := c.boxProjection(boxID); box != nil {
		page.Revision = box.Revision
	}
	return page, nil
}

func (c *Coordinator) boxProjection(boxID string) (*domain.Box, error) {
	var box *domain.Box
	err := c.store.View(func(tx *store.Tx) error {
		data, err := tx.GetBox(boxID)
		if err != nil {
			return err
		}
		if data == nil {
			return nil
		}
		var b domain.Box
		if err := json.Unmarshal(data, &b); err != nil {
			return err
		}
		box = &b
		return nil
	})
	return box, err
}

// PendingView summarises a pending event for queries.
type PendingView struct {
	EventID            string    `json:"event_id"`
	BoxID              string    `json:"box_id"`
	EventType          string    `json:"event_type"`
	MissingPredecessor string    `json:"missing_predecessor,omitempty"`
	MissingTerminalID  string    `json:"missing_terminal_id,omitempty"`
	MissingTerminalSeq int64     `json:"missing_terminal_seq,omitempty"`
	Reasons            []string  `json:"reasons,omitempty"`
	ReceivedAt         time.Time `json:"received_at"`
}

// Pending returns pending events, optionally filtered by box.
func (c *Coordinator) Pending(boxID string) ([]PendingView, error) {
	var out []PendingView
	err := c.store.View(func(tx *store.Tx) error {
		return tx.ForEachPending(func(p *store.PendingRecord) error {
			if boxID != "" && p.BoxID != boxID {
				return nil
			}
			rec, _ := tx.GetRecord(p.EventID)
			pv := PendingView{
				EventID:            p.EventID,
				BoxID:              p.BoxID,
				MissingPredecessor: p.MissingPredecessor,
				MissingTerminalID:  p.MissingTerminalID,
				MissingTerminalSeq: p.MissingTerminalSeq,
				Reasons:            p.Reasons,
			}
			if rec != nil {
				pv.EventType = rec.EventType
				pv.ReceivedAt = rec.ReceivedAt
			}
			out = append(out, pv)
			return nil
		})
	})
	return out, err
}

// RejectedView summarises a rejected event for queries.
type RejectedView struct {
	EventID    string    `json:"event_id"`
	BoxID      string    `json:"box_id"`
	EventType  string    `json:"event_type"`
	Code       string    `json:"code"`
	Reason     string    `json:"reason"`
	ReceivedAt time.Time `json:"received_at"`
}

// Rejected returns rejected events, optionally filtered by box.
func (c *Coordinator) Rejected(boxID string) ([]RejectedView, error) {
	var out []RejectedView
	err := c.store.View(func(tx *store.Tx) error {
		return tx.ForEachRejected(func(r *store.Record) error {
			if boxID != "" && r.BoxID != boxID {
				return nil
			}
			out = append(out, RejectedView{
				EventID:    r.EventID,
				BoxID:      r.BoxID,
				EventType:  r.EventType,
				Code:       r.Code,
				Reason:     r.Reason,
				ReceivedAt: r.ReceivedAt,
			})
			return nil
		})
	})
	return out, err
}
