// Package coordinator implements the ledger semantics that sit between the
// wire protocol and the storage layer: idempotency, terminal-sequence
// ordering, causal-dependency tracking, out-of-order coordination and
// deterministic replay.
//
// The coordinator is the single linearisation point for event submission:
// every decision (accept, pend, reject, duplicate) is made inside a single
// storage transaction so that concurrent submissions observe a consistent
// history.
package coordinator

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"medcold-handoff-ledger/internal/apperr"
	"medcold-handoff-ledger/internal/domain"
	"medcold-handoff-ledger/internal/infra"
	"medcold-handoff-ledger/internal/store"
)

// hashBytes returns the hex-encoded SHA-256 digest of b.
func hashBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// ResultStatus is the outcome category of a Process call.
type ResultStatus string

const (
	StatusAccepted  ResultStatus = "accepted"
	StatusPending   ResultStatus = "pending"
	StatusRejected  ResultStatus = "rejected"
	StatusDuplicate ResultStatus = "duplicate"
)

// Result is the outcome of processing a submitted event.
type Result struct {
	Status     ResultStatus   `json:"status"`
	EventID    string         `json:"event_id"`
	Revision   int64          `json:"revision,omitempty"`
	BoxState   string         `json:"box_state,omitempty"`
	Code       string         `json:"code,omitempty"`
	Category   string         `json:"category,omitempty"`
	Message    string         `json:"message,omitempty"`
	Retryable  bool           `json:"retryable,omitempty"`
	Details    map[string]any `json:"details,omitempty"`
	ReceivedAt time.Time      `json:"received_at"`
}

// Coordinator processes events against a store.
type Coordinator struct {
	store  *store.Store
	clock  infra.Clock
	ids    infra.IDSource
	fault  infra.FaultInjector
	window infra.TimeWindow
}

// Options configures a Coordinator.
type Options struct {
	Clock  infra.Clock
	IDs    infra.IDSource
	Fault  infra.FaultInjector
	Window infra.TimeWindow
}

// New creates a Coordinator.
func New(s *store.Store, opts Options) *Coordinator {
	if opts.Clock == nil {
		opts.Clock = infra.RealClock{}
	}
	if opts.IDs == nil {
		opts.IDs = infra.RealIDSource{}
	}
	if opts.Fault == nil {
		opts.Fault = infra.NoFault{}
	}
	return &Coordinator{store: s, clock: opts.Clock, ids: opts.IDs, fault: opts.Fault, window: opts.Window}
}

// SetFault replaces the fault injector (used by tests).
func (c *Coordinator) SetFault(f infra.FaultInjector) { c.fault = f }

// Process is the synchronous submission entry point. It performs strict
// validation, deduplication, dependency checking and state transition, all
// within a single storage transaction.
func (c *Coordinator) Process(env *domain.Event) (*Result, *apperr.Error) {
	// Assign a server-side event id if the client omitted it.
	if env.EventID == "" {
		env.EventID = c.ids.NewID()
	}

	canon, ae := canonicalPayload(env.Payload)
	if ae != nil {
		return nil, ae
	}
	payloadHash := hashBytes(canon)
	now := c.clock.Now()

	var result *Result
	err := c.store.Update(func(tx *store.Tx) error {
		r, err := c.processTx(tx, env, canon, payloadHash, now)
		if err != nil {
			return err
		}
		result = r
		// Fault injection point: failing here rolls back the entire tx.
		if ferr := c.fault.Fault("pre_commit"); ferr != nil {
			return ferr
		}
		return nil
	})
	if err != nil {
		if ae, ok := err.(*apperr.Error); ok {
			return nil, ae
		}
		return nil, apperr.StorageFailure(err.Error())
	}
	return result, nil
}

// processTx implements the per-transaction processing logic.
func (c *Coordinator) processTx(tx *store.Tx, env *domain.Event, canon []byte, payloadHash string, now time.Time) (*Result, error) {
	if dup := c.checkDuplicate(tx, env, payloadHash, now); dup != nil {
		return dup, nil
	}
	missing := c.missingDependencies(tx, env)
	if len(missing) > 0 {
		r := c.recordPending(tx, env, canon, payloadHash, missing, now)
		// A newly pending event cannot unblock others, but run the sweep once
		// to keep the ledger in a fixpoint after every submission.
		if err := c.replayPending(tx, now); err != nil {
			return nil, err
		}
		return r, nil
	}
	box, ae := c.loadBox(tx, env.BoxID)
	if ae != nil {
		return nil, ae
	}
	applyRes, ae := domain.ApplyEvent(box, env, now)
	if ae != nil {
		r := c.recordRejected(tx, env, canon, payloadHash, ae, now)
		// A rejection may unblock terminal-sequence followers whose immediate
		// predecessor has now reached a terminal state.
		if err := c.replayPending(tx, now); err != nil {
			return nil, err
		}
		return r, nil
	}
	if err := c.persistAccepted(tx, env, canon, payloadHash, applyRes, now); err != nil {
		return nil, err
	}
	if err := c.replayPending(tx, now); err != nil {
		return nil, err
	}
	return &Result{
		Status:     StatusAccepted,
		EventID:    env.EventID,
		Revision:   applyRes.Box.Revision,
		BoxState:   string(applyRes.Box.State),
		ReceivedAt: now,
	}, nil
}

func (c *Coordinator) checkDuplicate(tx *store.Tx, env *domain.Event, payloadHash string, now time.Time) *Result {
	type entry struct{ key, hash string }
	var found *entry
	if rec, _ := tx.GetRecord(env.EventID); rec != nil {
		found = &entry{"event_id", rec.PayloadHash}
	} else if idx, _ := tx.GetIdem(env.IdempotencyKey); idx != nil {
		found = &entry{"idempotency_key", idx.PayloadHash}
	} else if idx, _ := tx.GetTerminal(env.TerminalID, env.TerminalSequence); idx != nil {
		found = &entry{"terminal_sequence", idx.PayloadHash}
	}
	if found == nil {
		return nil
	}
	if found.hash != payloadHash {
		return &Result{
			Status:     StatusRejected,
			EventID:    env.EventID,
			Code:       string(apperr.CodeDuplicateConflict),
			Category:   string(apperr.CategoryConflict),
			Message:    fmt.Sprintf("key matched on %s but payload differs", found.key),
			ReceivedAt: now,
		}
	}
	return c.idempotentResult(tx, env, now)
}

func (c *Coordinator) idempotentResult(tx *store.Tx, env *domain.Event, now time.Time) *Result {
	rec, _ := tx.GetRecord(env.EventID)
	if rec == nil {
		if idx, _ := tx.GetIdem(env.IdempotencyKey); idx != nil {
			rec, _ = tx.GetRecord(idx.EventID)
		}
	}
	if rec == nil {
		if idx, _ := tx.GetTerminal(env.TerminalID, env.TerminalSequence); idx != nil {
			rec, _ = tx.GetRecord(idx.EventID)
		}
	}
	if rec == nil {
		return nil
	}
	r := &Result{Status: StatusDuplicate, EventID: rec.EventID, Revision: rec.Revision, BoxState: rec.NewState, ReceivedAt: now}
	switch rec.Status {
	case store.StatusAccepted:
		r.Status = StatusAccepted
	case store.StatusPending:
		r.Status = StatusPending
		r.Code = string(apperr.CodeDependencyMissing)
		r.Category = string(apperr.CategoryDomain)
		r.Message = "event is pending missing dependencies"
		r.Retryable = true
	case store.StatusRejected:
		r.Status = StatusRejected
		r.Code = rec.Code
		r.Message = rec.Reason
		r.Retryable = apperr.Code(rec.Code) == apperr.CodeDependencyMissing
	}
	return r
}

func (c *Coordinator) missingDependencies(tx *store.Tx, env *domain.Event) []string {
	var missing []string
	if env.PredecessorEventID != "" {
		rec, _ := tx.GetRecord(env.PredecessorEventID)
		if rec == nil || rec.Status != store.StatusAccepted {
			missing = append(missing, "predecessor_event_id="+env.PredecessorEventID)
		}
	}
	if env.TerminalSequence > 1 {
		idx, _ := tx.GetTerminal(env.TerminalID, env.TerminalSequence-1)
		if idx == nil {
			// The predecessor sequence has not been submitted yet: a gap.
			missing = append(missing, fmt.Sprintf("terminal_sequence=%s:%d", env.TerminalID, env.TerminalSequence-1))
		} else {
			// Predecessor was submitted. We only wait if it is still pending;
			// an accepted or rejected predecessor is terminal and does not
			// block this event.
			rec, _ := tx.GetRecord(idx.EventID)
			if rec == nil || rec.Status == store.StatusPending {
				missing = append(missing, fmt.Sprintf("terminal_sequence=%s:%d", env.TerminalID, env.TerminalSequence-1))
			}
		}
	}
	return missing
}

func (c *Coordinator) recordPending(tx *store.Tx, env *domain.Event, canon []byte, payloadHash string, missing []string, now time.Time) *Result {
	rec := &store.Record{
		EventID:            env.EventID,
		IdempotencyKey:     env.IdempotencyKey,
		TerminalID:         env.TerminalID,
		TerminalSequence:   env.TerminalSequence,
		BoxID:              env.BoxID,
		EventType:          string(env.Type),
		Role:               string(env.Role),
		OccurredAt:         env.OccurredAt,
		ReceivedAt:         now,
		PredecessorEventID: env.PredecessorEventID,
		Envelope:           canon,
		PayloadHash:        payloadHash,
		Status:             store.StatusPending,
		Code:               string(apperr.CodeDependencyMissing),
		Reason:             "waiting for dependencies",
	}
	_ = tx.PutRecord(rec)
	_ = tx.PutIdem(env.IdempotencyKey, &store.IdemIndex{EventID: env.EventID, PayloadHash: payloadHash})
	_ = tx.PutTerminal(env.TerminalID, env.TerminalSequence, &store.IdemIndex{EventID: env.EventID, PayloadHash: payloadHash})
	pending := &store.PendingRecord{EventID: env.EventID, BoxID: env.BoxID, Reasons: missing}
	for _, m := range missing {
		switch {
		case strings.HasPrefix(m, "predecessor_event_id="):
			pending.MissingPredecessor = strings.TrimPrefix(m, "predecessor_event_id=")
		case strings.HasPrefix(m, "terminal_sequence="):
			rest := strings.TrimPrefix(m, "terminal_sequence=")
			if parts := strings.SplitN(rest, ":", 2); len(parts) == 2 {
				pending.MissingTerminalID = parts[0]
				pending.MissingTerminalSeq = parseInt64(parts[1])
			}
		}
	}
	_ = tx.PutPending(pending)
	return &Result{
		Status:     StatusPending,
		EventID:    env.EventID,
		Code:       string(apperr.CodeDependencyMissing),
		Category:   string(apperr.CategoryDomain),
		Message:    "event is pending missing dependencies",
		Retryable:  true,
		Details:    map[string]any{"missing": missing},
		ReceivedAt: now,
	}
}

func (c *Coordinator) recordRejected(tx *store.Tx, env *domain.Event, canon []byte, payloadHash string, ae *apperr.Error, now time.Time) *Result {
	rec := &store.Record{
		EventID:            env.EventID,
		IdempotencyKey:     env.IdempotencyKey,
		TerminalID:         env.TerminalID,
		TerminalSequence:   env.TerminalSequence,
		BoxID:              env.BoxID,
		EventType:          string(env.Type),
		Role:               string(env.Role),
		OccurredAt:         env.OccurredAt,
		ReceivedAt:         now,
		PredecessorEventID: env.PredecessorEventID,
		Envelope:           canon,
		PayloadHash:        payloadHash,
		Status:             store.StatusRejected,
		Code:               string(ae.Code),
		Reason:             ae.Message,
	}
	_ = tx.PutRecord(rec)
	_ = tx.PutRejected(rec)
	_ = tx.PutIdem(env.IdempotencyKey, &store.IdemIndex{EventID: env.EventID, PayloadHash: payloadHash})
	_ = tx.PutTerminal(env.TerminalID, env.TerminalSequence, &store.IdemIndex{EventID: env.EventID, PayloadHash: payloadHash})
	_ = tx.DeletePending(env.EventID)
	return &Result{
		Status:     StatusRejected,
		EventID:    env.EventID,
		Code:       string(ae.Code),
		Category:   string(ae.Category),
		Message:    ae.Message,
		Retryable:  ae.Retryable,
		ReceivedAt: now,
	}
}

func (c *Coordinator) loadBox(tx *store.Tx, boxID string) (*domain.Box, *apperr.Error) {
	data, err := tx.GetBox(boxID)
	if err != nil {
		return nil, apperr.StorageFailure(err.Error())
	}
	if data == nil {
		return nil, nil
	}
	var box domain.Box
	if err := json.Unmarshal(data, &box); err != nil {
		return nil, apperr.StorageFailure(fmt.Sprintf("decode box: %v", err))
	}
	return &box, nil
}

func (c *Coordinator) persistAccepted(tx *store.Tx, env *domain.Event, canon []byte, payloadHash string, res *domain.ApplyResult, now time.Time) error {
	boxData, err := json.Marshal(res.Box)
	if err != nil {
		return err
	}
	if err := tx.PutBox(env.BoxID, boxData); err != nil {
		return err
	}
	rec := &store.Record{
		EventID:            env.EventID,
		IdempotencyKey:     env.IdempotencyKey,
		TerminalID:         env.TerminalID,
		TerminalSequence:   env.TerminalSequence,
		BoxID:              env.BoxID,
		EventType:          string(env.Type),
		Role:               string(env.Role),
		OccurredAt:         env.OccurredAt,
		ReceivedAt:         now,
		PredecessorEventID: env.PredecessorEventID,
		Envelope:           canon,
		PayloadHash:        payloadHash,
		Status:             store.StatusAccepted,
		Revision:           res.Box.Revision,
		NewState:           string(res.Box.State),
	}
	if err := tx.PutRecord(rec); err != nil {
		return err
	}
	if err := tx.PutIdem(env.IdempotencyKey, &store.IdemIndex{EventID: env.EventID, PayloadHash: payloadHash}); err != nil {
		return err
	}
	if err := tx.PutTerminal(env.TerminalID, env.TerminalSequence, &store.IdemIndex{EventID: env.EventID, PayloadHash: payloadHash}); err != nil {
		return err
	}
	_ = tx.DeletePending(env.EventID)
	_ = tx.DeleteRejected(env.EventID)
	return nil
}

func canonicalPayload(p domain.Payload) ([]byte, *apperr.Error) {
	canon, err := json.Marshal(p)
	if err != nil {
		return nil, apperr.SchemaViolation(fmt.Sprintf("encode payload: %v", err))
	}
	return canon, nil
}
