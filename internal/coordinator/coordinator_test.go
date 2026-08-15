package coordinator

import (
	"path/filepath"
	"sync"
	"testing"
	"time"

	"medcold-handoff-ledger/internal/apperr"
	"medcold-handoff-ledger/internal/domain"
	"medcold-handoff-ledger/internal/infra"
	"medcold-handoff-ledger/internal/store"
)

var t0 = time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)

type fixture struct {
	t       *testing.T
	coord   *Coordinator
	store   *store.Store
	clock   *infra.FixedClock
	ids     *infra.SequenceIDSource
	dataDir string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	dir := t.TempDir()
	s, err := store.Open(filepath.Join(dir, "ledger.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	clock := infra.NewFixedClock(t0)
	ids := infra.NewSequenceIDSource("evt")
	coord := New(s, Options{
		Clock: clock,
		IDs:   ids,
		Fault: infra.NoFault{},
	})
	return &fixture{t: t, coord: coord, store: s, clock: clock, ids: ids, dataDir: dir}
}

func (f *fixture) process(e *domain.Event) *Result {
	f.t.Helper()
	r, err := f.coord.Process(e)
	if err != nil {
		f.t.Fatalf("process %s: %v", e.Type, err)
	}
	return r
}

func (f *fixture) processRaw(e *domain.Event) (*Result, *apperr.Error) {
	f.t.Helper()
	return f.coord.Process(e)
}

func (f *fixture) processErr(e *domain.Event) *apperr.Error {
	f.t.Helper()
	_, err := f.coord.Process(e)
	return err
}

func mk(eventID, idemKey, terminal string, seq int64, box string, et domain.EventType, role domain.Role, occurred time.Time, pred string, payload domain.Payload) *domain.Event {
	return &domain.Event{
		EventID:            eventID,
		IdempotencyKey:     idemKey,
		TerminalID:         terminal,
		TerminalSequence:   seq,
		BoxID:              box,
		Type:               et,
		Role:               role,
		OccurredAt:         occurred,
		PredecessorEventID: pred,
		Payload:            payload,
	}
}

func boxCreatedPayload() *domain.BoxCreatedPayload {
	return &domain.BoxCreatedPayload{
		ProductName: "Insulin", Batch: "B1", Origin: "DEPOT",
		Destination: "PHARM", CarrierID: "C1", LowerBound: 2, UpperBound: 8,
	}
}

func fullSequenceBox(f *fixture, boxID string) {
	f.process(mk("e-create", "k-create", "T1", 1, boxID, domain.EventBoxCreated, domain.RolePharmacy, t0, "", boxCreatedPayload()))
	f.process(mk("e-seg", "k-seg", "T1", 2, boxID, domain.EventSegmentStarted, domain.RoleCarrier, t0.Add(time.Minute), "",
		&domain.SegmentStartedPayload{SegmentID: "SEG1", CarrierID: "C1", Origin: "DEPOT", Destination: "PHARM"}))
	f.process(mk("e-arr", "k-arr", "T1", 3, boxID, domain.EventArrivalScanned, domain.RoleCarrier, t0.Add(2*time.Hour), "",
		&domain.ArrivalScannedPayload{Location: "PHARM"}))
}

func TestProcessAcceptsBoxCreated(t *testing.T) {
	f := newFixture(t)
	r := f.process(mk("e1", "k1", "T1", 1, "B1", domain.EventBoxCreated, domain.RolePharmacy, t0, "", boxCreatedPayload()))
	if r.Status != StatusAccepted {
		t.Fatalf("status = %s, want accepted", r.Status)
	}
	if r.Revision != 1 {
		t.Fatalf("revision = %d, want 1", r.Revision)
	}
	if r.BoxState != string(domain.StateDrafted) {
		t.Fatalf("box_state = %s, want drafted", r.BoxState)
	}
}

func TestIdempotencySamePayload(t *testing.T) {
	f := newFixture(t)
	e := mk("e1", "k1", "T1", 1, "B1", domain.EventBoxCreated, domain.RolePharmacy, t0, "", boxCreatedPayload())
	r1 := f.process(e)

	// Several concurrent retries with the same idempotency key + payload.
	var wg sync.WaitGroup
	results := make([]*Result, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			e2 := mk("e1", "k1", "T1", 1, "B1", domain.EventBoxCreated, domain.RolePharmacy, t0, "", boxCreatedPayload())
			rr, err := f.coord.Process(e2)
			if err != nil {
				t.Errorf("retry %d: %v", i, err)
				return
			}
			results[i] = rr
		}(i)
	}
	wg.Wait()

	for i, r := range results {
		if r == nil {
			continue
		}
		if r.EventID != r1.EventID {
			t.Fatalf("retry %d: event_id = %s, want %s", i, r.EventID, r1.EventID)
		}
		if r.Status != StatusAccepted || r.Revision != 1 {
			t.Fatalf("retry %d: status=%s revision=%d, want accepted/1", i, r.Status, r.Revision)
		}
	}
	// Only one event should be stored.
	n := 0
	_ = f.store.View(func(tx *store.Tx) error {
		return tx.ForEachRecord(func(*store.Record) error { n++; return nil })
	})
	if n != 1 {
		t.Fatalf("stored events = %d, want 1", n)
	}
}

func TestIdempotencyDifferentPayload(t *testing.T) {
	f := newFixture(t)
	f.process(mk("e1", "k1", "T1", 1, "B1", domain.EventBoxCreated, domain.RolePharmacy, t0, "", boxCreatedPayload()))

	// Same idempotency key but different payload.
	p2 := boxCreatedPayload()
	p2.Batch = "B2"
	r, err := f.processRaw(mk("e2", "k1", "T1", 1, "B1", domain.EventBoxCreated, domain.RolePharmacy, t0, "", p2))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r == nil || r.Code != string(apperr.CodeDuplicateConflict) {
		t.Fatalf("want duplicate_conflict, got %v", r)
	}
	if r.Retryable {
		t.Fatalf("duplicate_conflict should not be retryable")
	}
	// Nothing was written for the conflicting attempt.
	n := 0
	_ = f.store.View(func(tx *store.Tx) error {
		return tx.ForEachRecord(func(rec *store.Record) error { n++; return nil })
	})
	if n != 1 {
		t.Fatalf("stored events = %d, want 1", n)
	}
}

func TestTerminalSequenceIdempotencyAndConflict(t *testing.T) {
	f := newFixture(t)
	// Same terminal seq + same payload returns original result (idempotent).
	e1 := mk("e1", "k1", "T1", 1, "B1", domain.EventBoxCreated, domain.RolePharmacy, t0, "", boxCreatedPayload())
	f.process(e1)
	e1b := mk("e1b", "k1", "T1", 1, "B1", domain.EventBoxCreated, domain.RolePharmacy, t0, "", boxCreatedPayload())
	r := f.process(e1b)
	if r.EventID != "e1" {
		t.Fatalf("idempotent terminal seq: event_id = %s, want e1", r.EventID)
	}

	// Same terminal seq, different event/payload -> duplicate_conflict.
	p2 := boxCreatedPayload()
	p2.Batch = "B9"
	r2, err := f.processRaw(mk("e1c", "k-other", "T1", 1, "B1", domain.EventBoxCreated, domain.RolePharmacy, t0, "", p2))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r2 == nil || r2.Code != string(apperr.CodeDuplicateConflict) {
		t.Fatalf("want duplicate_conflict for same seq diff payload, got %v", r2)
	}
}

func TestConcurrentExclusiveTransitionOneWins(t *testing.T) {
	f := newFixture(t)
	f.process(mk("e-create", "k-create", "T1", 1, "B1", domain.EventBoxCreated, domain.RolePharmacy, t0, "", boxCreatedPayload()))

	var wg sync.WaitGroup
	const N = 8
	results := make([]*Result, N)
	errs := make([]*apperr.Error, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Distinct terminals competing for the same exclusive transition;
			// each terminal's own sequence space starts at 1 so there is no
			// cross-terminal causal dependency to wait on.
			terminal := "TERM-" + string(rune('a'+i))
			e := mk("", "comp-"+terminal, terminal, 1, "B1", domain.EventSegmentStarted, domain.RoleCarrier,
				t0.Add(time.Duration(i)*time.Minute), "",
				&domain.SegmentStartedPayload{SegmentID: "S", CarrierID: "C1", Origin: "DEPOT", Destination: "PHARM"})
			r, err := f.coord.Process(e)
			results[i] = r
			errs[i] = err
		}(i)
	}
	wg.Wait()

	accepted := 0
	var winnerRev int64
	var lossCodes []string
	for i, r := range results {
		if errs[i] == nil && r != nil && r.Status == StatusAccepted {
			accepted++
			winnerRev = r.Revision
		} else if r != nil && r.Status == StatusRejected {
			lossCodes = append(lossCodes, r.Code)
		}
	}
	if accepted != 1 {
		t.Fatalf("accepted = %d, want 1 (results=%v errs=%v)", accepted, results, errs)
	}
	if winnerRev != 2 {
		t.Fatalf("winner revision = %d, want 2", winnerRev)
	}
	// Every loser must have received a classifiable conflict code.
	for _, c := range lossCodes {
		if c != string(apperr.CodeRevisionConflict) && c != string(apperr.CodeIllegalTransition) {
			t.Fatalf("loser got non-classifiable code %q", c)
		}
	}
	box, _ := f.coord.GetBox("B1")
	if box.State != string(domain.StateInTransit) {
		t.Fatalf("final state = %s, want in_transit", box.State)
	}
	if box.Revision != 2 {
		t.Fatalf("final revision = %d, want 2", box.Revision)
	}
}

func TestOutOfOrderPharmacyReceivedBeforeCarrierSignedOut(t *testing.T) {
	f := newFixture(t)
	fullSequenceBox(f, "B1")
	// pharmacy_received depends (by predecessor) on carrier_signed_out which
	// has not yet been submitted -> pending.
	recv := mk("e-recv", "k-recv", "T1", 5, "B1", domain.EventPharmacyReceived, domain.RolePharmacy,
		t0.Add(3*time.Hour), "e-signout",
		&domain.SignoffPayload{PartyID: "P1", PartyName: "Pharmacy A"})
	r := f.process(recv)
	if r.Status != StatusPending {
		t.Fatalf("status = %s, want pending", r.Status)
	}
	if r.Code != string(apperr.CodeDependencyMissing) {
		t.Fatalf("code = %s, want dependency_missing", r.Code)
	}
	// The box should not have moved to handed_over yet (no pollution).
	box, _ := f.coord.GetBox("B1")
	if box.State != string(domain.StateAwaitingReceive) {
		t.Fatalf("state = %s, want awaiting_receive (pending must not pollute)", box.State)
	}
	// Now submit the predecessor carrier_signed_out (terminal seq 4, before recv's 5).
	signout := mk("e-signout", "k-signout", "T1", 4, "B1", domain.EventCarrierSignedOut, domain.RoleCarrier,
		t0.Add(150*time.Minute), "",
		&domain.SignoffPayload{PartyID: "C1", PartyName: "Carrier One"})
	r2 := f.process(signout)
	if r2.Status != StatusAccepted {
		t.Fatalf("signout status = %s, want accepted", r2.Status)
	}
	// After signout, the pending pharmacy_received should have been replayed.
	box, _ = f.coord.GetBox("B1")
	if box.State != string(domain.StateHandedOver) {
		t.Fatalf("state = %s, want handed_over after replay", box.State)
	}
	if box.Signoffs.Pharmacy == nil {
		t.Fatalf("pharmacy signoff missing after replay")
	}
	// Retrying the original pharmacy_received returns accepted (now applied).
	r3 := f.process(recv)
	if r3.Status != StatusAccepted && r3.Status != StatusDuplicate {
		t.Fatalf("retry recv status = %s", r3.Status)
	}
}

func TestDeterministicReplayRepeatable(t *testing.T) {
	// Run the same out-of-order orchestration twice on fresh ledgers; the
	// final state and event classifications must match.
	runOnce := func(t *testing.T) (string, map[string]string) {
		f := newFixture(t)
		fullSequenceBox(f, "B1")
		f.process(mk("e-recv", "k-recv", "T1", 5, "B1", domain.EventPharmacyReceived, domain.RolePharmacy,
			t0.Add(3*time.Hour), "e-signout", &domain.SignoffPayload{PartyID: "P1", PartyName: "Pharmacy A"}))
		f.process(mk("e-signout", "k-signout", "T1", 4, "B1", domain.EventCarrierSignedOut, domain.RoleCarrier,
			t0.Add(150*time.Minute), "", &domain.SignoffPayload{PartyID: "C1", PartyName: "Carrier One"}))
		box, _ := f.coord.GetBox("B1")
		classification := map[string]string{}
		_ = f.store.View(func(tx *store.Tx) error {
			return tx.ForEachRecord(func(r *store.Record) error {
				classification[r.EventID] = string(r.Status)
				return nil
			})
		})
		return string(box.State), classification
	}
	s1, c1 := runOnce(t)
	s2, c2 := runOnce(t)
	if s1 != s2 {
		t.Fatalf("final state differs: %s vs %s", s1, s2)
	}
	if len(c1) != len(c2) {
		t.Fatalf("classification count differs: %d vs %d", len(c1), len(c2))
	}
	for k, v := range c1 {
		if c2[k] != v {
			t.Fatalf("classification for %s differs: %s vs %s", k, v, c2[k])
		}
	}
}

func TestPermanentlyMissingDependencyStaysPending(t *testing.T) {
	f := newFixture(t)
	fullSequenceBox(f, "B1")
	recv := mk("e-recv", "k-recv", "T1", 5, "B1", domain.EventPharmacyReceived, domain.RolePharmacy,
		t0.Add(3*time.Hour), "e-never",
		&domain.SignoffPayload{PartyID: "P1", PartyName: "Pharmacy A"})
	r := f.process(recv)
	if r.Status != StatusPending {
		t.Fatalf("status = %s, want pending", r.Status)
	}
	pending, _ := f.coord.Pending("B1")
	if len(pending) != 1 {
		t.Fatalf("pending count = %d, want 1", len(pending))
	}
	if pending[0].MissingPredecessor != "e-never" {
		t.Fatalf("missing predecessor = %s, want e-never", pending[0].MissingPredecessor)
	}
	// Current projection must be unaffected.
	box, _ := f.coord.GetBox("B1")
	if box.State != string(domain.StateAwaitingReceive) {
		t.Fatalf("state = %s, projection polluted by pending", box.State)
	}
}

func TestTerminalSequenceGapPending(t *testing.T) {
	f := newFixture(t)
	// Submit terminal seq 1 (box_created), then seq 3 (segment_started) without
	// seq 2. Seq 3 must be pending (gap at 2).
	f.process(mk("e1", "k1", "T1", 1, "B1", domain.EventBoxCreated, domain.RolePharmacy, t0, "", boxCreatedPayload()))
	r := f.process(mk("e3", "k3", "T1", 3, "B1", domain.EventSegmentStarted, domain.RoleCarrier, t0.Add(time.Minute), "",
		&domain.SegmentStartedPayload{SegmentID: "S", CarrierID: "C1", Origin: "DEPOT", Destination: "PHARM"}))
	if r.Status != StatusPending {
		t.Fatalf("seq 3 status = %s, want pending", r.Status)
	}
	pending, _ := f.coord.Pending("B1")
	if len(pending) != 1 || pending[0].MissingTerminalSeq != 2 {
		t.Fatalf("pending = %+v, want missing seq 2", pending)
	}
	// Fill the gap with seq 2 (a valid segment_started that moves drafted->in_transit).
	f.process(mk("e2", "k2", "T1", 2, "B1", domain.EventSegmentStarted, domain.RoleCarrier, t0.Add(time.Second), "",
		&domain.SegmentStartedPayload{SegmentID: "S2", CarrierID: "C1", Origin: "DEPOT", Destination: "PHARM"}))
	// Now seq 3 (segment_started from in_transit) should have been replayed and
	// rejected as a stale (revision_conflict) transition.
	box, _ := f.coord.GetBox("B1")
	if box.State != string(domain.StateInTransit) {
		t.Fatalf("state = %s, want in_transit", box.State)
	}
	pending, _ = f.coord.Pending("B1")
	if len(pending) != 0 {
		t.Fatalf("pending count = %d, want 0 after gap filled", len(pending))
	}
	rejected, _ := f.coord.Rejected("B1")
	found := false
	for _, rej := range rejected {
		if rej.EventID == "e3" && rej.Code == string(apperr.CodeRevisionConflict) {
			found = true
		}
	}
	if !found {
		t.Fatalf("e3 should be rejected as revision_conflict, got %+v", rejected)
	}
}

func TestFaultInjectionRollsBack(t *testing.T) {
	f := newFixture(t)
	// Inject a fault at pre_commit: the first submission fails and must leave
	// nothing behind.
	f.coord.SetFault(infra.NewCountingFault("pre_commit", 1))
	err := f.processErr(mk("e1", "k1", "T1", 1, "B1", domain.EventBoxCreated, domain.RolePharmacy, t0, "", boxCreatedPayload()))
	if err == nil || err.Code != apperr.CodeStorageFailure {
		t.Fatalf("want storage_failure, got %v", err)
	}
	// No event, no idempotency key, no box.
	n := 0
	_ = f.store.View(func(tx *store.Tx) error {
		_ = tx.ForEachRecord(func(*store.Record) error { n++; return nil })
		return nil
	})
	if n != 0 {
		t.Fatalf("events after rollback = %d, want 0", n)
	}
	box, _ := f.coord.GetBox("B1")
	if box != nil {
		t.Fatalf("box should not exist after rollback")
	}
	// Retry succeeds (fault quota exhausted).
	f.coord.SetFault(infra.NoFault{})
	r := f.process(mk("e1", "k1", "T1", 1, "B1", domain.EventBoxCreated, domain.RolePharmacy, t0, "", boxCreatedPayload()))
	if r.Status != StatusAccepted {
		t.Fatalf("retry status = %s, want accepted", r.Status)
	}
}

func TestTemperatureQuarantineBlocksSignoff(t *testing.T) {
	f := newFixture(t)
	fullSequenceBox(f, "B1")
	// Out-of-range summary during in_transit... but box is awaiting_receive.
	// Move to a state where temp summary is valid: awaiting_receive is valid.
	r := f.process(mk("e-ts", "k-ts", "T1", 4, "B1", domain.EventTemperatureSummary, domain.RoleCarrier, t0.Add(90*time.Minute), "",
		&domain.TemperatureSummaryPayload{
			SummaryID: "TS1", SampleStart: t0.Add(2 * time.Hour), SampleEnd: t0.Add(3 * time.Hour),
			SampleCount: 10, MinTemp: 9, MaxTemp: 12, AvgTemp: 10, ExcursionDuration: time.Minute,
			LowerBound: 2, UpperBound: 8,
		}))
	if r.Status != StatusAccepted {
		t.Fatalf("temp summary status = %s, want accepted", r.Status)
	}
	box, _ := f.coord.GetBox("B1")
	if box.State != string(domain.StateTemperatureQuarantine) {
		t.Fatalf("state = %s, want temperature_quarantine", box.State)
	}
	// carrier_signed_out from quarantine is blocked with an illegal_transition.
	soRes := f.process(mk("e-so", "k-so", "T1", 5, "B1", domain.EventCarrierSignedOut, domain.RoleCarrier, t0.Add(95*time.Minute), "",
		&domain.SignoffPayload{PartyID: "C1", PartyName: "Carrier One"}))
	if soRes.Status != StatusRejected || soRes.Code != string(apperr.CodeIllegalTransition) {
		t.Fatalf("signoff from quarantine: want rejected/illegal_transition, got %+v", soRes)
	}
	box, _ = f.coord.GetBox("B1")
	if box.State != string(domain.StateTemperatureQuarantine) {
		t.Fatalf("state changed despite blocked signoff: %s", box.State)
	}
	// Ordinary close by pharmacy is blocked; only exception_handler can close.
	pharmClose := f.process(mk("e-close-pharm", "k-close-pharm", "T1", 6, "B1", domain.EventClosed, domain.RolePharmacy, t0.Add(3*time.Hour), "",
		&domain.ClosedPayload{Reason: "x"}))
	if pharmClose.Status != StatusRejected || pharmClose.Code != string(apperr.CodeIllegalTransition) {
		t.Fatalf("pharmacy close from quarantine: want rejected/illegal_transition, got %+v", pharmClose)
	}
	// Exception handler closes the box.
	r2 := f.process(mk("e-close", "k-close", "T1", 7, "B1", domain.EventClosed, domain.RoleExceptionHandler, t0.Add(4*time.Hour), "",
		&domain.ClosedPayload{Reason: "disposed"}))
	if r2.Status != StatusAccepted {
		t.Fatalf("close status = %s, want accepted", r2.Status)
	}
	box, _ = f.coord.GetBox("B1")
	if box.State != string(domain.StateClosed) {
		t.Fatalf("state = %s, want closed", box.State)
	}
}

func TestTimelinePaginationStableAndComplete(t *testing.T) {
	f := newFixture(t)
	fullSequenceBox(f, "B1")
	f.process(mk("e-signout", "k-so", "T1", 4, "B1", domain.EventCarrierSignedOut, domain.RoleCarrier, t0.Add(150*time.Minute), "",
		&domain.SignoffPayload{PartyID: "C1", PartyName: "Carrier One"}))

	seen := map[string]bool{}
	cursor := ""
	for {
		page, err := f.coord.Timeline("B1", cursor, 2)
		if err != nil {
			t.Fatalf("timeline: %v", err)
		}
		for _, it := range page.Items {
			if seen[it.EventID] {
				t.Fatalf("duplicate event in pagination: %s", it.EventID)
			}
			seen[it.EventID] = true
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	if len(seen) != 4 {
		t.Fatalf("saw %d events, want 4", len(seen))
	}
}
