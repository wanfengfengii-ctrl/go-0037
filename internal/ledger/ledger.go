// Package ledger is the top-level facade that ties together the store,
// coordinator and testable infrastructure. It is the construct that the HTTP
// service and the CLI both depend on.
//
// A Ledger is opened against a data directory; on open it rebuilds box
// projections from accepted events in memory and verifies they match the
// stored projections, surfacing an integrity failure if they diverge.
package ledger

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"medcold-handoff-ledger/internal/apperr"
	"medcold-handoff-ledger/internal/coordinator"
	"medcold-handoff-ledger/internal/domain"
	"medcold-handoff-ledger/internal/infra"
	"medcold-handoff-ledger/internal/store"
)

// Ledger ties a store and a coordinator together.
type Ledger struct {
	store   *store.Store
	coord   *coordinator.Coordinator
	clock   infra.Clock
	dataDir string
}

// Config configures a Ledger.
type Config struct {
	DataDir string
	Clock   infra.Clock
	IDs     infra.IDSource
	Fault   infra.FaultInjector
	Window  infra.TimeWindow
}

// Open opens (or creates) a ledger and verifies projection integrity.
func Open(cfg Config) (*Ledger, error) {
	return open(cfg, false)
}

// OpenReadOnly opens an existing ledger without creating or initializing its
// backing store and verifies projection integrity.
func OpenReadOnly(cfg Config) (*Ledger, error) {
	return open(cfg, true)
}

func open(cfg Config, readOnly bool) (*Ledger, error) {
	path := cfg.DataDir + "/ledger.db"
	var s *store.Store
	var err error
	if readOnly {
		s, err = store.OpenReadOnly(path)
	} else {
		s, err = store.Open(path)
	}
	if err != nil {
		return nil, err
	}
	clock := cfg.Clock
	if clock == nil {
		clock = infra.RealClock{}
	}
	coord := coordinator.New(s, coordinator.Options{
		Clock:  clock,
		IDs:    cfg.IDs,
		Fault:  cfg.Fault,
		Window: cfg.Window,
	})
	l := &Ledger{store: s, coord: coord, clock: clock, dataDir: cfg.DataDir}
	if err := l.Verify(); err != nil {
		_ = s.Close()
		return nil, err
	}
	return l, nil
}

// Close closes the underlying store.
func (l *Ledger) Close() error { return l.store.Close() }

// Coordinator exposes the coordinator for submission and queries.
func (l *Ledger) Coordinator() *coordinator.Coordinator { return l.coord }

// Store exposes the store (used by the CLI for verification).
func (l *Ledger) Store() *store.Store { return l.store }

// Submit is the synchronous submission entry point.
func (l *Ledger) Submit(env *domain.Event) (*coordinator.Result, *apperr.Error) {
	return l.coord.Process(env)
}

// Verify rebuilds box projections from accepted events in memory and compares
// them with the stored projections. It returns an integrity_failure if they
// differ, and never modifies the stored data.
func (l *Ledger) Verify() error {
	rebuilt, hashes, err := l.rebuildInMemory()
	if err != nil {
		return err
	}
	var mismatches []string
	err = l.store.View(func(tx *store.Tx) error {
		for boxID, newBox := range rebuilt {
			stored, err := tx.GetBox(boxID)
			if err != nil {
				return err
			}
			if stored == nil {
				mismatches = append(mismatches, fmt.Sprintf("box %s: rebuilt exists but stored is missing", boxID))
				continue
			}
			var s domain.Box
			if err := json.Unmarshal(stored, &s); err != nil {
				return err
			}
			if !boxesEqual(&s, newBox) {
				mismatches = append(mismatches, fmt.Sprintf("box %s: projection mismatch", boxID))
			}
		}
		// Check stored boxes that have no rebuilt counterpart (orphaned).
		_ = tx.ForEachBox(func(boxID string, _ []byte) error {
			if _, ok := rebuilt[boxID]; !ok {
				mismatches = append(mismatches, fmt.Sprintf("box %s: stored projection orphaned", boxID))
			}
			return nil
		})
		// Verify event chain: payload hashes match stored records.
		_ = tx.ForEachRecord(func(r *store.Record) error {
			if h, ok := hashes[r.EventID]; ok && h != r.PayloadHash {
				mismatches = append(mismatches, fmt.Sprintf("event %s: payload hash mismatch", r.EventID))
			}
			return nil
		})
		return nil
	})
	if err != nil {
		return apperr.IntegrityFailure(err.Error())
	}
	if len(mismatches) > 0 {
		sort.Strings(mismatches)
		return apperr.IntegrityFailure(fmt.Sprintf("integrity check failed: %v", mismatches))
	}
	return nil
}

// Rebuild forcibly discards stored projections and rebuilds them from accepted
// events. Used by recovery tooling.
func (l *Ledger) Rebuild() error {
	rebuilt, _, err := l.rebuildInMemory()
	if err != nil {
		return err
	}
	return l.store.Update(func(tx *store.Tx) error {
		if err := tx.ClearBoxes(); err != nil {
			return err
		}
		for boxID, box := range rebuilt {
			data, err := json.Marshal(box)
			if err != nil {
				return err
			}
			if err := tx.PutBox(boxID, data); err != nil {
				return err
			}
		}
		return nil
	})
}

// rebuildInMemory replays every accepted event onto empty boxes in stable
// order and returns the resulting projections plus per-event payload hashes.
func (l *Ledger) rebuildInMemory() (map[string]*domain.Box, map[string]string, error) {
	type accepted struct {
		rec *store.Record
	}
	var records []*store.Record
	if err := l.store.View(func(tx *store.Tx) error {
		return tx.ForEachRecord(func(r *store.Record) error {
			if r.Status == store.StatusAccepted {
				records = append(records, r)
			}
			return nil
		})
	}); err != nil {
		return nil, nil, err
	}
	sort.SliceStable(records, func(i, j int) bool {
		return sortKey(records[i]).less(sortKey(records[j]))
	})
	boxes := map[string]*domain.Box{}
	hashes := map[string]string{}
	for _, r := range records {
		et := domain.EventType(r.EventType)
		payload := domain.NewPayload(et)
		if payload == nil {
			return nil, nil, fmt.Errorf("no payload type for %s", et)
		}
		if err := json.Unmarshal(r.Envelope, payload); err != nil {
			return nil, nil, fmt.Errorf("decode payload for %s: %w", r.EventID, err)
		}
		// Recompute the canonical payload hash from the decoded payload rather
		// than trusting the stored value, so that a tampered hash is detectable.
		canon, err := json.Marshal(payload)
		if err != nil {
			return nil, nil, fmt.Errorf("encode payload for %s: %w", r.EventID, err)
		}
		sum := sha256.Sum256(canon)
		hashes[r.EventID] = hex.EncodeToString(sum[:])
		ev := &domain.Event{
			EventID:            r.EventID,
			IdempotencyKey:     r.IdempotencyKey,
			TerminalID:         r.TerminalID,
			TerminalSequence:   r.TerminalSequence,
			BoxID:              r.BoxID,
			Type:               et,
			Role:               domain.Role(r.Role),
			OccurredAt:         r.OccurredAt,
			PredecessorEventID: r.PredecessorEventID,
			Payload:            payload,
		}
		box := boxes[r.BoxID]
		res, ae := domain.ApplyEvent(box, ev, r.ReceivedAt)
		if ae != nil {
			return nil, nil, fmt.Errorf("rebuild: event %s should be applicable but failed: %s", r.EventID, ae.Message)
		}
		boxes[r.BoxID] = res.Box
	}
	return boxes, hashes, nil
}

type evKey struct {
	occurred time.Time
	terminal string
	seq      int64
	id       string
}

func sortKey(r *store.Record) evKey {
	return evKey{r.OccurredAt, r.TerminalID, r.TerminalSequence, r.EventID}
}

func (k evKey) less(o evKey) bool {
	if !k.occurred.Equal(o.occurred) {
		return k.occurred.Before(o.occurred)
	}
	if k.terminal != o.terminal {
		return k.terminal < o.terminal
	}
	if k.seq != o.seq {
		return k.seq < o.seq
	}
	return k.id < o.id
}

// boxesEqual compares two boxes field-by-field for integrity verification.
func boxesEqual(a, b *domain.Box) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if a.BoxID != b.BoxID || a.State != b.State || a.Revision != b.Revision {
		return false
	}
	if a.Quarantined != b.Quarantined || a.PreQuarantineState != b.PreQuarantineState {
		return false
	}
	if a.LastEventID != b.LastEventID {
		return false
	}
	if !a.CreatedAt.Equal(b.CreatedAt) || !a.UpdatedAt.Equal(b.UpdatedAt) {
		return false
	}
	if len(a.Segments) != len(b.Segments) {
		return false
	}
	for i := range a.Segments {
		if !segmentsEqual(a.Segments[i], b.Segments[i]) {
			return false
		}
	}
	if len(a.TempSummaries) != len(b.TempSummaries) {
		return false
	}
	for i := range a.TempSummaries {
		if !tempSummariesEqual(a.TempSummaries[i], b.TempSummaries[i]) {
			return false
		}
	}
	if !signoffEqual(a.Signoffs.Carrier, b.Signoffs.Carrier) {
		return false
	}
	if !signoffEqual(a.Signoffs.Pharmacy, b.Signoffs.Pharmacy) {
		return false
	}
	return mapsEqual(a.Metadata, b.Metadata)
}

func segmentsEqual(a, b domain.Segment) bool {
	return a.SegmentID == b.SegmentID && a.CarrierID == b.CarrierID && a.Origin == b.Origin &&
		a.Destination == b.Destination && a.Status == b.Status && a.StartedAt.Equal(b.StartedAt) &&
		a.ArrivedAt.Equal(b.ArrivedAt) && a.SignedOutAt.Equal(b.SignedOutAt)
}

func tempSummariesEqual(a, b domain.TemperatureSummary) bool {
	return a.SummaryID == b.SummaryID && a.SampleStart.Equal(b.SampleStart) && a.SampleEnd.Equal(b.SampleEnd) &&
		a.SampleCount == b.SampleCount && a.MinTemp == b.MinTemp && a.MaxTemp == b.MaxTemp && a.AvgTemp == b.AvgTemp &&
		a.ExcursionDuration == b.ExcursionDuration && a.LowerBound == b.LowerBound && a.UpperBound == b.UpperBound &&
		a.OutOfRange == b.OutOfRange && a.EventID == b.EventID
}

func signoffEqual(a, b *domain.Signoff) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.EventID == b.EventID && a.PartyID == b.PartyID && a.PartyName == b.PartyName && a.SignedAt.Equal(b.SignedAt)
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
