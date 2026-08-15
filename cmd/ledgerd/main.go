// Command ledgerd runs the cold-chain handoff ledger HTTP service. It is a
// single-node, file-backed service suitable for a hospital pharmacy or a
// carrier depot.
//
// Usage:
//
//	ledgerd --data-dir /var/lib/ledger --addr :8080
//
// The service supports graceful shutdown on SIGINT/SIGTERM, structured
// (slog) logging, a /healthz endpoint and a configurable occurred_at window.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"medcold-handoff-ledger/internal/api"
	"medcold-handoff-ledger/internal/infra"
	"medcold-handoff-ledger/internal/ledger"
	"medcold-handoff-ledger/internal/protocol"
)

func main() {
	dataDir := flag.String("data-dir", "./data", "directory holding the ledger database")
	addr := flag.String("addr", ":8080", "HTTP listen address")
	maxPast := flag.Duration("max-past", 72*time.Hour, "maximum age of occurred_at timestamps")
	maxFuture := flag.Duration("max-future", 1*time.Hour, "maximum future skew of occurred_at timestamps")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	l, err := ledger.Open(ledger.Config{
		DataDir: *dataDir,
		Clock:   infra.RealClock{},
		IDs:     infra.RealIDSource{},
		Window:  infra.TimeWindow{MaxPast: *maxPast, MaxFuture: *maxFuture},
	})
	if err != nil {
		logger.Error("failed to open ledger", "error", err)
		os.Exit(1)
	}
	defer func() { _ = l.Close() }()

	srv := api.New(l, api.Options{
		Logger: logger,
		Clock:  infra.RealClock{},
		Window: protocol.TimeWindow{MaxPast: *maxPast, MaxFuture: *maxFuture},
	})
	httpSrv := &http.Server{
		Addr:              *addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("ledgerd listening", "addr", *addr, "data-dir", *dataDir)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server error", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down gracefully")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
	}
	logger.Info("ledgerd stopped")
}
