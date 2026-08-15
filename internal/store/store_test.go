package store

import (
	"fmt"
	"path/filepath"
	"testing"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ledger.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestPutGetRecord(t *testing.T) {
	s := newTestStore(t)
	rec := &Record{EventID: "E1", IdempotencyKey: "K1", Status: StatusAccepted, PayloadHash: "h"}
	if err := s.Update(func(tx *Tx) error { return tx.PutRecord(rec) }); err != nil {
		t.Fatalf("put: %v", err)
	}
	var got *Record
	if err := s.View(func(tx *Tx) error {
		r, err := tx.GetRecord("E1")
		got = r
		return err
	}); err != nil {
		t.Fatalf("view: %v", err)
	}
	if got == nil || got.EventID != "E1" {
		t.Fatalf("got = %v", got)
	}
}

func TestRollbackLeavesNothing(t *testing.T) {
	s := newTestStore(t)
	// Write a record but roll the transaction back.
	if err := s.Update(func(tx *Tx) error {
		_ = tx.PutRecord(&Record{EventID: "E1", Status: StatusAccepted})
		return fmt.Errorf("simulated failure")
	}); err == nil {
		t.Fatal("expected error from update")
	}
	if err := s.View(func(tx *Tx) error {
		r, err := tx.GetRecord("E1")
		if err != nil {
			return err
		}
		if r != nil {
			t.Fatalf("expected nil after rollback, got %v", r)
		}
		return nil
	}); err != nil {
		t.Fatalf("view: %v", err)
	}
}

func TestIdempotencyIndex(t *testing.T) {
	s := newTestStore(t)
	idx := &IdemIndex{EventID: "E1", PayloadHash: "h"}
	if err := s.Update(func(tx *Tx) error { return tx.PutIdem("K1", idx) }); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := s.View(func(tx *Tx) error {
		got, err := tx.GetIdem("K1")
		if err != nil {
			return err
		}
		if got == nil || got.EventID != "E1" {
			t.Fatalf("got = %v", got)
		}
		return nil
	}); err != nil {
		t.Fatalf("view: %v", err)
	}
}

func TestPendingLifecycle(t *testing.T) {
	s := newTestStore(t)
	p := &PendingRecord{EventID: "E1", BoxID: "B1", Reasons: []string{"missing predecessor"}}
	if err := s.Update(func(tx *Tx) error { return tx.PutPending(p) }); err != nil {
		t.Fatalf("put: %v", err)
	}
	var count int
	if err := s.View(func(tx *Tx) error {
		n, err := tx.CountPending()
		count = n
		return err
	}); err != nil {
		t.Fatalf("view: %v", err)
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}
	if err := s.Update(func(tx *Tx) error { return tx.DeletePending("E1") }); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := s.View(func(tx *Tx) error {
		n, err := tx.CountPending()
		if err != nil {
			return err
		}
		if n != 0 {
			t.Fatalf("count = %d, want 0", n)
		}
		return nil
	}); err != nil {
		t.Fatalf("view: %v", err)
	}
}

func TestTerminalKeyRoundTrip(t *testing.T) {
	k := TerminalKey("T1", 5)
	tid, seq, ok := ParseTerminalKey(k)
	if !ok || tid != "T1" || seq != 5 {
		t.Fatalf("round trip: tid=%q seq=%d ok=%v", tid, seq, ok)
	}
}
