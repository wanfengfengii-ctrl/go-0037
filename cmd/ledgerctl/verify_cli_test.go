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

func TestVerifyCLIOnMissingLedgerDoesNotCreateFiles(t *testing.T) {
	bin := buildLedgerctl(t)
	root := t.TempDir()

	for _, tc := range []struct {
		name    string
		dataDir string
	}{
		{name: "missing data directory", dataDir: filepath.Join(root, "missing")},
		{name: "missing database file", dataDir: filepath.Join(root, "empty")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name == "missing database file" {
				if err := os.Mkdir(tc.dataDir, 0o755); err != nil {
					t.Fatalf("create empty data directory: %v", err)
				}
			}

			out, err := exec.Command(bin, "verify", "--data-dir", tc.dataDir).CombinedOutput()
			if err == nil {
				t.Fatalf("verify on missing ledger succeeded: %s", out)
			}
			if !strings.Contains(string(out), "ledger.db") {
				t.Fatalf("verify error does not identify missing ledger.db: %s", out)
			}
			if _, statErr := os.Stat(filepath.Join(tc.dataDir, "ledger.db")); !os.IsNotExist(statErr) {
				t.Fatalf("verify created ledger.db or returned unexpected stat error: %v", statErr)
			}
			if tc.name == "missing data directory" {
				if _, statErr := os.Stat(tc.dataDir); !os.IsNotExist(statErr) {
					t.Fatalf("verify created data directory or returned unexpected stat error: %v", statErr)
				}
			} else {
				entries, readErr := os.ReadDir(tc.dataDir)
				if readErr != nil {
					t.Fatalf("read data directory after verify: %v", readErr)
				}
				if len(entries) != 0 {
					t.Fatalf("verify created files in data directory: %v", entries)
				}
			}
		})
	}
}

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
