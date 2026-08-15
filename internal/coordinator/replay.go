package coordinator

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"medcold-handoff-ledger/internal/domain"
	"medcold-handoff-ledger/internal/store"
)

// replayPending attempts to advance any pending events whose dependencies are
// now satisfied, applying them in a deterministic order. It runs to a fixpoint:
// it keeps sweeping until a full pass makes no progress.
//
// The sweep is global because a terminal sequence may span boxes; the
// deterministic order is the event_id (a stable secondary key) so that
// repeated runs reach the same outcome.
func (c *Coordinator) replayPending(tx *store.Tx, now time.Time) error {
	for {
		progress := false
		var pending []*store.PendingRecord
		_ = tx.ForEachPending(func(p *store.PendingRecord) error {
			pending = append(pending, p)
			return nil
		})
		sortPending(pending)
		for _, p := range pending {
			rec, err := tx.GetRecord(p.EventID)
			if err != nil || rec == nil {
				continue
			}
			env, err := recordToEvent(rec)
			if err != nil {
				continue
			}
			missing := c.missingDependencies(tx, env)
			if len(missing) > 0 {
				continue
			}
			box, ae := c.loadBox(tx, rec.BoxID)
			if ae != nil {
				continue
			}
			applyRes, ae := domain.ApplyEvent(box, env, now)
			if ae != nil {
				c.recordRejected(tx, env, rec.Envelope, rec.PayloadHash, ae, now)
				progress = true
				continue
			}
			if err := c.persistAccepted(tx, env, rec.Envelope, rec.PayloadHash, applyRes, now); err != nil {
				return err
			}
			progress = true
		}
		if !progress {
			return nil
		}
	}
}

func sortPending(pending []*store.PendingRecord) {
	sort.SliceStable(pending, func(i, j int) bool {
		return pending[i].EventID < pending[j].EventID
	})
}

// recordToEvent reconstructs a domain.Event from a stored record. The original
// envelope (kept verbatim in the record) is re-decoded to recover the payload.
func recordToEvent(rec *store.Record) (*domain.Event, error) {
	et := domain.EventType(rec.EventType)
	payload := domain.NewPayload(et)
	if payload == nil {
		return nil, fmt.Errorf("no payload type for %s", et)
	}
	if err := json.Unmarshal(rec.Envelope, payload); err != nil {
		return nil, err
	}
	return &domain.Event{
		EventID:            rec.EventID,
		IdempotencyKey:     rec.IdempotencyKey,
		TerminalID:         rec.TerminalID,
		TerminalSequence:   rec.TerminalSequence,
		BoxID:              rec.BoxID,
		Type:               et,
		Role:               domain.Role(rec.Role),
		OccurredAt:         rec.OccurredAt,
		PredecessorEventID: rec.PredecessorEventID,
		Payload:            payload,
	}, nil
}

// parseInt64 parses a non-negative integer; returns 0 on failure.
func parseInt64(s string) int64 {
	var n int64
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0
		}
		n = n*10 + int64(r-'0')
	}
	return n
}
