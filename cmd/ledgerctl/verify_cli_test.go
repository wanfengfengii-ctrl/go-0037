package main

import (
	"os"
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

// TestVerifyCLIMissingDataDirFailsAndDoesNotCreate is the regression test for the
// silent-creation bug: verifying a data directory that has never existed must
// NOT create the directory, the database file or any bucket structure, and must
// exit non-zero with a clear error instead of reporting "ledger OK".
func TestVerifyCLIMissingDataDirFailsAndDoesNotCreate(t *testing.T) {
	bin := buildLedgerctl(t)

	// A data directory that does not exist and is never created by the test.
	missingDir := filepath.Join(t.TempDir(), "does-not-exist")
	dbPath := filepath.Join(missingDir, "ledger.db")

	out, err := exec.Command(bin, "verify", "--data-dir", missingDir).CombinedOutput()
	if err == nil {
		t.Fatalf("verify on missing data dir should fail, got: %s", out)
	}
	if !strings.Contains(string(out), "no such file") &&
		!strings.Contains(string(out), "not found") &&
		!strings.Contains(string(out), "missing") {
		t.Fatalf("verify on missing data dir should report a clear error, got: %s", out)
	}

	// Nothing may have been created: neither the directory nor the database file.
	if _, statErr := os.Stat(missingDir); !os.IsNotExist(statErr) {
		t.Fatalf("verify created the missing data directory: %v", statErr)
	}
	if _, statErr := os.Stat(dbPath); !os.IsNotExist(statErr) {
		t.Fatalf("verify created a database file: %v", statErr)
	}
}

// TestVerifyCLIMissingDBFileFailsAndDoesNotCreate covers the case where the data
// directory exists but the ledger database file does not: verify must still
// fail without creating the file.
func TestVerifyCLIMissingDBFileFailsAndDoesNotCreate(t *testing.T) {
	bin := buildLedgerctl(t)

	// The directory exists but contains no ledger.db.
	emptyDir := t.TempDir()
	dbPath := filepath.Join(emptyDir, "ledger.db")

	out, err := exec.Command(bin, "verify", "--data-dir", emptyDir).CombinedOutput()
	if err == nil {
		t.Fatalf("verify on missing ledger.db should fail, got: %s", out)
	}
	if !strings.Contains(string(out), "no such file") &&
		!strings.Contains(string(out), "not found") &&
		!strings.Contains(string(out), "missing") {
		t.Fatalf("verify on missing ledger.db should report a clear error, got: %s", out)
	}
	if _, statErr := os.Stat(dbPath); !os.IsNotExist(statErr) {
		t.Fatalf("verify created a database file: %v", statErr)
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
