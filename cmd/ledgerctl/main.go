// Command ledgerctl is the read-only administration tool for the cold-chain
// handoff ledger. Its primary subcommand, verify, opens a ledger database and
// checks that the stored projections can be rebuilt from the accepted event
// log, that event payload hashes match, and that no orphaned projections
// exist. It never modifies the data.
//
// Usage:
//
//	ledgerctl verify --data-dir /var/lib/ledger
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"medcold-handoff-ledger/internal/infra"
	"medcold-handoff-ledger/internal/ledger"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "verify":
		verify()
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: ledgerctl <subcommand> [flags]")
	fmt.Fprintln(os.Stderr, "subcommands:")
	fmt.Fprintln(os.Stderr, "  verify    verify ledger integrity (read-only)")
	fmt.Fprintln(os.Stderr, "flags for verify:")
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.String("data-dir", "./data", "directory holding the ledger database")
	fs.Usage()
}

func verify() {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	dataDir := fs.String("data-dir", "./data", "directory holding the ledger database")
	_ = fs.Parse(os.Args[2:])

	l, err := ledger.OpenReadOnly(ledger.Config{
		DataDir: *dataDir,
		Clock:   infra.RealClock{},
		IDs:     infra.RealIDSource{},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "integrity failure: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = l.Close() }()

	if err := l.Verify(); err != nil {
		fmt.Fprintf(os.Stderr, "integrity failure: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("ledger OK: verified at %s\n", time.Now().UTC().Format(time.RFC3339Nano))
}
