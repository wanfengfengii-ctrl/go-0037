package main

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"medcold-handoff-ledger/internal/domain"
	"medcold-handoff-ledger/internal/infra"
	"medcold-handoff-ledger/internal/ledger"
	"medcold-handoff-ledger/internal/store"
)

// TestVerifyCLIOnCleanLedger builds the ledgerctl binary, populates a clean
// ledger, and asserts the verify subcommand exits 0.
func TestVerifyCLIOnCleanLedger(t *testing.T) {
	bin := buildLedgerctl(t)

	dataDir := t.TempDir()
	clock := infra.NewFixedClock(time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC))
	l, err := ledger.Open(ledger.Config{DataDir: dataDir, Clock: clock, IDs: infra.NewSequenceIDSource("evt")})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	mustSubmit(t, l, &domain.Event{
		EventID: "e1", IdempotencyKey: "k1", TerminalID: "T1", TerminalSequence: 1,
		BoxID: "B1", Type: domain.EventBoxCreated, Role: domain.RolePharmacy, OccurredAt: clock.Now(),
		Payload: &domain.BoxCreatedPayload{ProductName: "Insulin", Batch: "B1", Origin: "DEPOT", Destination: "PHARM", CarrierID: "C1", LowerBound: 2, UpperBound: 8},
	})
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	out, err := exec.Command(bin, "verify", "--data-dir", dataDir).CombinedOutput()
	if err != nil {
		t.Fatalf("verify on clean ledger failed: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "OK") {
		t.Fatalf("unexpected output: %s", out)
	}
}

// TestVerifyCLIOnCorruptLedger corrupts a projection and asserts verify exits
// non-zero.
func TestVerifyCLIOnCorruptLedger(t *testing.T) {
	bin := buildLedgerctl(t)
	dataDir := t.TempDir()
	clock := infra.NewFixedClock(time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC))
	l, err := ledger.Open(ledger.Config{DataDir: dataDir, Clock: clock, IDs: infra.NewSequenceIDSource("evt")})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	mustSubmit(t, l, &domain.Event{
		EventID: "e1", IdempotencyKey: "k1", TerminalID: "T1", TerminalSequence: 1,
		BoxID: "B1", Type: domain.EventBoxCreated, Role: domain.RolePharmacy, OccurredAt: clock.Now(),
		Payload: &domain.BoxCreatedPayload{ProductName: "Insulin", Batch: "B1", Origin: "DEPOT", Destination: "PHARM", CarrierID: "C1", LowerBound: 2, UpperBound: 8},
	})
	// Corrupt the stored projection directly.
	_ = l.Store().Update(func(tx *store.Tx) error {
		return tx.PutBox("B1", []byte(`{"box_id":"B1","state":"closed","revision":99}`))
	})
	_ = l.Close()

	out, err := exec.Command(bin, "verify", "--data-dir", dataDir).CombinedOutput()
	if err == nil {
		t.Fatalf("verify on corrupt ledger should fail, got: %s", out)
	}
}

func buildLedgerctl(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "ledgerctl")
	cmd := exec.Command("go", "build", "-o", bin, ".")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build ledgerctl: %v\n%s", err, out)
	}
	return bin
}

func mustSubmit(t *testing.T, l *ledger.Ledger, e *domain.Event) {
	t.Helper()
	if _, ae := l.Submit(e); ae != nil {
		t.Fatalf("submit %s: %v", e.Type, ae)
	}
}
