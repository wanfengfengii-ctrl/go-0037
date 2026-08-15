package ledger

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"medcold-handoff-ledger/internal/domain"
	"medcold-handoff-ledger/internal/infra"
	"medcold-handoff-ledger/internal/store"
)

var lt0 = time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)

func newLedger(t *testing.T) (*Ledger, *infra.FixedClock, string) {
	t.Helper()
	dir := t.TempDir()
	clock := infra.NewFixedClock(lt0)
	l, err := Open(Config{
		DataDir: dir,
		Clock:   clock,
		IDs:     infra.NewSequenceIDSource("evt"),
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l, clock, dir
}

func boxCreatedPayload() *domain.BoxCreatedPayload {
	return &domain.BoxCreatedPayload{
		ProductName: "Insulin", Batch: "B1", Origin: "DEPOT",
		Destination: "PHARM", CarrierID: "C1", LowerBound: 2, UpperBound: 8,
	}
}

func submitFull(t *testing.T, l *Ledger, boxID string) {
	t.Helper()
	mustSubmit(t, l, &domain.Event{
		EventID: "e-create", IdempotencyKey: "k-create", TerminalID: "T1", TerminalSequence: 1,
		BoxID: boxID, Type: domain.EventBoxCreated, Role: domain.RolePharmacy, OccurredAt: lt0, Payload: boxCreatedPayload(),
	})
	mustSubmit(t, l, &domain.Event{
		EventID: "e-seg", IdempotencyKey: "k-seg", TerminalID: "T1", TerminalSequence: 2,
		BoxID: boxID, Type: domain.EventSegmentStarted, Role: domain.RoleCarrier, OccurredAt: lt0.Add(time.Minute),
		Payload: &domain.SegmentStartedPayload{SegmentID: "SEG1", CarrierID: "C1", Origin: "DEPOT", Destination: "PHARM"},
	})
	mustSubmit(t, l, &domain.Event{
		EventID: "e-arr", IdempotencyKey: "k-arr", TerminalID: "T1", TerminalSequence: 3,
		BoxID: boxID, Type: domain.EventArrivalScanned, Role: domain.RoleCarrier, OccurredAt: lt0.Add(2 * time.Hour),
		Payload: &domain.ArrivalScannedPayload{Location: "PHARM"},
	})
	mustSubmit(t, l, &domain.Event{
		EventID: "e-so", IdempotencyKey: "k-so", TerminalID: "T1", TerminalSequence: 4,
		BoxID: boxID, Type: domain.EventCarrierSignedOut, Role: domain.RoleCarrier, OccurredAt: lt0.Add(150 * time.Minute),
		Payload: &domain.SignoffPayload{PartyID: "C1", PartyName: "Carrier One"},
	})
	mustSubmit(t, l, &domain.Event{
		EventID: "e-recv", IdempotencyKey: "k-recv", TerminalID: "T1", TerminalSequence: 5,
		BoxID: boxID, Type: domain.EventPharmacyReceived, Role: domain.RolePharmacy, OccurredAt: lt0.Add(3 * time.Hour),
		PredecessorEventID: "e-so",
		Payload:            &domain.SignoffPayload{PartyID: "P1", PartyName: "Pharmacy A"},
	})
}

func mustSubmit(t *testing.T, l *Ledger, e *domain.Event) {
	t.Helper()
	r, ae := l.Submit(e)
	if ae != nil {
		t.Fatalf("submit %s: %v", e.Type, ae)
	}
	if r == nil {
		t.Fatalf("nil result for %s", e.Type)
	}
}

func TestReopenRestoresLedger(t *testing.T) {
	dir := t.TempDir()
	clock := infra.NewFixedClock(lt0)
	l1, err := Open(Config{DataDir: dir, Clock: clock, IDs: infra.NewSequenceIDSource("evt")})
	if err != nil {
		t.Fatalf("open1: %v", err)
	}
	submitFull(t, l1, "B1")
	box, _ := l1.Coordinator().GetBox("B1")
	if box == nil || box.State != string(domain.StateHandedOver) {
		t.Fatalf("box state = %v", box)
	}
	if err := l1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	l2, err := Open(Config{DataDir: dir, Clock: clock, IDs: infra.NewSequenceIDSource("evt")})
	if err != nil {
		t.Fatalf("open2: %v", err)
	}
	t.Cleanup(func() { _ = l2.Close() })
	box2, _ := l2.Coordinator().GetBox("B1")
	if box2 == nil {
		t.Fatalf("box missing after reopen")
	}
	if box2.State != string(domain.StateHandedOver) {
		t.Fatalf("state after reopen = %s, want handed_over", box2.State)
	}
	if box2.Revision != box.Revision {
		t.Fatalf("revision after reopen = %d, want %d", box2.Revision, box.Revision)
	}
	if box2.Signoffs.Pharmacy == nil || box2.Signoffs.Carrier == nil {
		t.Fatalf("signoffs not restored")
	}
	// Idempotency preserved across restart.
	r, ae := l2.Submit(&domain.Event{
		EventID: "e-create", IdempotencyKey: "k-create", TerminalID: "T1", TerminalSequence: 1,
		BoxID: "B1", Type: domain.EventBoxCreated, Role: domain.RolePharmacy, OccurredAt: lt0, Payload: boxCreatedPayload(),
	})
	if ae != nil {
		t.Fatalf("idempotent retry error: %v", ae)
	}
	if r.EventID != "e-create" {
		t.Fatalf("idempotent retry event_id = %s, want e-create", r.EventID)
	}
}

func TestRebuildAfterProjectionDeletion(t *testing.T) {
	l, _, _ := newLedger(t)
	submitFull(t, l, "B1")
	box, _ := l.Coordinator().GetBox("B1")
	if box == nil {
		t.Fatalf("box missing")
	}
	if err := l.Store().Update(func(tx *store.Tx) error { return tx.DeleteBox("B1") }); err != nil {
		t.Fatalf("delete projection: %v", err)
	}
	if gone, _ := l.Coordinator().GetBox("B1"); gone != nil {
		t.Fatalf("projection should be gone")
	}
	if err := l.Rebuild(); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	boxRebuilt, _ := l.Coordinator().GetBox("B1")
	if boxRebuilt == nil {
		t.Fatalf("box missing after rebuild")
	}
	if boxRebuilt.State != box.State || boxRebuilt.Revision != box.Revision {
		t.Fatalf("rebuilt box differs: state=%s rev=%d (want %s/%d)", boxRebuilt.State, boxRebuilt.Revision, box.State, box.Revision)
	}
	if !signoffDeepEqual(boxRebuilt.Signoffs, box.Signoffs) {
		t.Fatalf("rebuilt signoffs differ")
	}
}

func TestVerifyCleanLedger(t *testing.T) {
	l, _, _ := newLedger(t)
	submitFull(t, l, "B1")
	if err := l.Verify(); err != nil {
		t.Fatalf("verify clean ledger: %v", err)
	}
}

func TestVerifyDetectsCorruption(t *testing.T) {
	l, _, _ := newLedger(t)
	submitFull(t, l, "B1")
	if err := l.Store().Update(func(tx *store.Tx) error {
		data, err := tx.GetBox("B1")
		if err != nil || data == nil {
			return err
		}
		var b domain.Box
		if err := json.Unmarshal(data, &b); err != nil {
			return err
		}
		b.State = domain.StateClosed // corrupt
		out, err := json.Marshal(b)
		if err != nil {
			return err
		}
		return tx.PutBox("B1", out)
	}); err != nil {
		t.Fatalf("corrupt: %v", err)
	}
	if err := l.Verify(); err == nil {
		t.Fatalf("verify should detect corruption")
	}
}

func TestVerifyDoesNotModifyData(t *testing.T) {
	l, _, _ := newLedger(t)
	submitFull(t, l, "B1")
	before, _ := l.Coordinator().GetBox("B1")
	_ = l.Verify()
	after, _ := l.Coordinator().GetBox("B1")
	if before.State != after.State || before.Revision != after.Revision {
		t.Fatalf("verify modified data: before=%v after=%v", before, after)
	}
}

// TestOpenReadOnlyDoesNotCreateMissingLedger is the regression test for the
// silent-creation bug at the facade layer: opening a ledger in read-only mode
// against a directory that has never existed must fail and must not create the
// directory, the database file or any bucket structure.
func TestOpenReadOnlyDoesNotCreateMissingLedger(t *testing.T) {
	missingDir := filepath.Join(t.TempDir(), "does-not-exist")
	dbPath := filepath.Join(missingDir, "ledger.db")

	l, err := Open(Config{
		DataDir:  missingDir,
		Clock:    infra.NewFixedClock(lt0),
		IDs:      infra.NewSequenceIDSource("evt"),
		ReadOnly: true,
	})
	if err == nil {
		_ = l.Close()
		t.Fatalf("read-only open of missing ledger should fail")
	}
	if _, statErr := os.Stat(missingDir); !os.IsNotExist(statErr) {
		t.Fatalf("read-only open created the data directory: %v", statErr)
	}
	if _, statErr := os.Stat(dbPath); !os.IsNotExist(statErr) {
		t.Fatalf("read-only open created the database file: %v", statErr)
	}
}

// TestOpenReadOnlyVerifiesExistingLedger confirms that a read-only open of an
// existing, valid ledger succeeds and that verification still passes, while a
// read-only open of a corrupt ledger reports the integrity failure.
func TestOpenReadOnlyVerifiesExistingLedger(t *testing.T) {
	dir := t.TempDir()
	clock := infra.NewFixedClock(lt0)
	// First create and populate a ledger with the normal (create) path.
	l1, err := Open(Config{DataDir: dir, Clock: clock, IDs: infra.NewSequenceIDSource("evt")})
	if err != nil {
		t.Fatalf("open1: %v", err)
	}
	submitFull(t, l1, "B1")
	if err := l1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen read-only: must succeed and verify clean.
	l2, err := Open(Config{DataDir: dir, Clock: clock, IDs: infra.NewSequenceIDSource("evt"), ReadOnly: true})
	if err != nil {
		t.Fatalf("read-only open of existing ledger: %v", err)
	}
	t.Cleanup(func() { _ = l2.Close() })
	if err := l2.Verify(); err != nil {
		t.Fatalf("verify existing ledger read-only: %v", err)
	}
	box, _ := l2.Coordinator().GetBox("B1")
	if box == nil || box.State != string(domain.StateHandedOver) {
		t.Fatalf("box not readable read-only: %v", box)
	}

	// Corrupt the projection, then reopen read-only: must report the failure.
	if err := l2.Close(); err != nil {
		t.Fatalf("close before corrupt: %v", err)
	}
	l3, err := Open(Config{DataDir: dir, Clock: clock, IDs: infra.NewSequenceIDSource("evt")})
	if err != nil {
		t.Fatalf("open to corrupt: %v", err)
	}
	if err := l3.Store().Update(func(tx *store.Tx) error {
		return tx.PutBox("B1", []byte(`{"box_id":"B1","state":"closed","revision":99}`))
	}); err != nil {
		t.Fatalf("corrupt: %v", err)
	}
	if err := l3.Close(); err != nil {
		t.Fatalf("close after corrupt: %v", err)
	}
	l4, err := Open(Config{DataDir: dir, Clock: clock, IDs: infra.NewSequenceIDSource("evt"), ReadOnly: true})
	if err == nil {
		_ = l4.Close()
		t.Fatalf("read-only open of corrupt ledger should fail")
	}
}

func signoffDeepEqual(a, b domain.SignoffSummary) bool {
	eq := func(x, y *domain.Signoff) bool {
		if x == nil || y == nil {
			return x == nil && y == nil
		}
		return x.EventID == y.EventID && x.PartyID == y.PartyID && x.PartyName == y.PartyName
	}
	return eq(a.Carrier, b.Carrier) && eq(a.Pharmacy, b.Pharmacy)
}

// TestPendingSurvivesRestart verifies that a pending event remains pending and
// does not pollute the projection across a close/reopen, and that submitting
// the missing predecessor after restart advances the pending event.
func TestPendingSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	clock := infra.NewFixedClock(lt0)
	openLedger := func() *Ledger {
		l, err := Open(Config{DataDir: dir, Clock: clock, IDs: infra.NewSequenceIDSource("evt")})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		return l
	}
	l1 := openLedger()
	t.Cleanup(func() { _ = l1.Close() })
	// Create + transport + arrival.
	mustSubmit(t, l1, &domain.Event{
		EventID: "e-create", IdempotencyKey: "k-create", TerminalID: "T1", TerminalSequence: 1,
		BoxID: "B1", Type: domain.EventBoxCreated, Role: domain.RolePharmacy, OccurredAt: lt0, Payload: boxCreatedPayload(),
	})
	mustSubmit(t, l1, &domain.Event{
		EventID: "e-seg", IdempotencyKey: "k-seg", TerminalID: "T1", TerminalSequence: 2,
		BoxID: "B1", Type: domain.EventSegmentStarted, Role: domain.RoleCarrier, OccurredAt: lt0.Add(time.Minute),
		Payload: &domain.SegmentStartedPayload{SegmentID: "SEG1", CarrierID: "C1", Origin: "DEPOT", Destination: "PHARM"},
	})
	mustSubmit(t, l1, &domain.Event{
		EventID: "e-arr", IdempotencyKey: "k-arr", TerminalID: "T1", TerminalSequence: 3,
		BoxID: "B1", Type: domain.EventArrivalScanned, Role: domain.RoleCarrier, OccurredAt: lt0.Add(2 * time.Hour),
		Payload: &domain.ArrivalScannedPayload{Location: "PHARM"},
	})
	// pharmacy_received references a not-yet-submitted carrier_signed_out -> pending.
	r, ae := l1.Submit(&domain.Event{
		EventID: "e-recv", IdempotencyKey: "k-recv", TerminalID: "T1", TerminalSequence: 5,
		BoxID: "B1", Type: domain.EventPharmacyReceived, Role: domain.RolePharmacy, OccurredAt: lt0.Add(3 * time.Hour),
		PredecessorEventID: "e-so", Payload: &domain.SignoffPayload{PartyID: "P1", PartyName: "Pharmacy A"},
	})
	if ae != nil || r.Status != "pending" {
		t.Fatalf("recv before restart: r=%v ae=%v", r, ae)
	}
	if err := l1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen: pending must survive and projection must be unaffected.
	l2 := openLedger()
	t.Cleanup(func() { _ = l2.Close() })
	pending, err := l2.Coordinator().Pending("B1")
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 1 || pending[0].EventID != "e-recv" {
		t.Fatalf("pending after restart = %+v", pending)
	}
	if pending[0].MissingPredecessor != "e-so" {
		t.Fatalf("missing predecessor = %s, want e-so", pending[0].MissingPredecessor)
	}
	box, _ := l2.Coordinator().GetBox("B1")
	if box.State != string(domain.StateAwaitingReceive) {
		t.Fatalf("state polluted by pending: %s", box.State)
	}

	// Submit the missing predecessor; the pending event should advance.
	mustSubmit(t, l2, &domain.Event{
		EventID: "e-so", IdempotencyKey: "k-so", TerminalID: "T1", TerminalSequence: 4,
		BoxID: "B1", Type: domain.EventCarrierSignedOut, Role: domain.RoleCarrier, OccurredAt: lt0.Add(150 * time.Minute),
		Payload: &domain.SignoffPayload{PartyID: "C1", PartyName: "Carrier One"},
	})
	box, _ = l2.Coordinator().GetBox("B1")
	if box.State != string(domain.StateHandedOver) {
		t.Fatalf("state = %s, want handed_over after predecessor supplied", box.State)
	}
	if box.Signoffs.Pharmacy == nil {
		t.Fatalf("pharmacy signoff missing after replay")
	}
	// No more pending.
	pending, _ = l2.Coordinator().Pending("B1")
	if len(pending) != 0 {
		t.Fatalf("pending after advance = %d, want 0", len(pending))
	}
}

// TestVerifyDetectsPayloadHashCorruption corrupts a stored payload hash and
// expects verify to report an integrity failure without modifying the data.
func TestVerifyDetectsPayloadHashCorruption(t *testing.T) {
	l, _, _ := newLedger(t)
	submitFull(t, l, "B1")
	var eventID string
	_ = l.Store().View(func(tx *store.Tx) error {
		return tx.ForEachRecord(func(r *store.Record) error {
			eventID = r.EventID
			return nil
		})
	})
	// Corrupt the payload hash of one accepted record.
	if err := l.Store().Update(func(tx *store.Tx) error {
		rec, err := tx.GetRecord(eventID)
		if err != nil || rec == nil {
			return err
		}
		rec.PayloadHash = "corrupted"
		return tx.PutRecord(rec)
	}); err != nil {
		t.Fatalf("corrupt: %v", err)
	}
	before, _ := l.Coordinator().GetBox("B1")
	if err := l.Verify(); err == nil {
		t.Fatalf("verify should detect payload hash corruption")
	}
	after, _ := l.Coordinator().GetBox("B1")
	if before.State != after.State || before.Revision != after.Revision {
		t.Fatalf("verify modified data despite corruption")
	}
}

// TestVerifyDetectsEventChainCorruption corrupts an accepted event's
// event_type so it can no longer be replayed against its box, which means the
// event chain is internally inconsistent. Verify must report integrity_failure.
func TestVerifyDetectsEventChainCorruption(t *testing.T) {
	l, _, _ := newLedger(t)
	submitFull(t, l, "B1")
	// Mutate the event_type of one accepted record so the payload no longer
	// matches the recorded type and the event fails to replay.
	var eventID string
	_ = l.Store().View(func(tx *store.Tx) error {
		return tx.ForEachRecord(func(r *store.Record) error {
			if r.EventType == string(domain.EventSegmentStarted) {
				eventID = r.EventID
			}
			return nil
		})
	})
	if eventID == "" {
		t.Fatalf("no segment_started record found")
	}
	if err := l.Store().Update(func(tx *store.Tx) error {
		rec, err := tx.GetRecord(eventID)
		if err != nil || rec == nil {
			return err
		}
		rec.EventType = string(domain.EventClosed) // mismatched type
		return tx.PutRecord(rec)
	}); err != nil {
		t.Fatalf("corrupt: %v", err)
	}
	if err := l.Verify(); err == nil {
		t.Fatalf("verify should detect event-chain corruption")
	}
}

// TestCursorConsistentAcrossRestart verifies that paginating a timeline before
// and after a restart yields the same set of events in the same order.
func TestCursorConsistentAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	clock := infra.NewFixedClock(lt0)
	l1, err := Open(Config{DataDir: dir, Clock: clock, IDs: infra.NewSequenceIDSource("evt")})
	if err != nil {
		t.Fatalf("open1: %v", err)
	}
	submitFull(t, l1, "B1")
	// Collect full timeline before restart.
	collect := func(l *Ledger) []string {
		var ids []string
		cursor := ""
		for {
			page, err := l.Coordinator().Timeline("B1", cursor, 2)
			if err != nil {
				t.Fatalf("timeline: %v", err)
			}
			for _, it := range page.Items {
				ids = append(ids, it.EventID)
			}
			if page.NextCursor == "" {
				break
			}
			cursor = page.NextCursor
		}
		return ids
	}
	before := collect(l1)
	if err := l1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	l2, err := Open(Config{DataDir: dir, Clock: clock, IDs: infra.NewSequenceIDSource("evt")})
	if err != nil {
		t.Fatalf("open2: %v", err)
	}
	defer func() { _ = l2.Close() }()
	after := collect(l2)
	if len(before) != len(after) {
		t.Fatalf("timeline length differs: %d vs %d", len(before), len(after))
	}
	for i := range before {
		if before[i] != after[i] {
			t.Fatalf("timeline order differs at %d: %s vs %s", i, before[i], after[i])
		}
	}
}
