// Package api implements the HTTP layer of the cold-chain handoff ledger.
// It exposes a versioned event submission endpoint and read-only query,
// audit and health endpoints, all of which map coordinator results and
// classified errors to machine-readable JSON responses with stable status
// codes.
package api

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"medcold-handoff-ledger/internal/apperr"
	"medcold-handoff-ledger/internal/coordinator"
	"medcold-handoff-ledger/internal/domain"
	"medcold-handoff-ledger/internal/infra"
	"medcold-handoff-ledger/internal/ledger"
	"medcold-handoff-ledger/internal/protocol"
)

// Server is the HTTP API.
type Server struct {
	led    *ledger.Ledger
	logger *slog.Logger
	window protocol.TimeWindow
	clock  infra.Clock
}

// Options configures the server.
type Options struct {
	Logger *slog.Logger
	Clock  infra.Clock
	Window protocol.TimeWindow
}

// New creates a Server backed by the given ledger.
func New(l *ledger.Ledger, opts Options) *Server {
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	if opts.Clock == nil {
		opts.Clock = infra.RealClock{}
	}
	return &Server{led: l, logger: opts.Logger, window: opts.Window, clock: opts.Clock}
}

// Handler returns an http.Handler for the API.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/events", s.handlePostEvents)
	mux.HandleFunc("GET /v1/boxes/{box_id}", s.handleGetBox)
	mux.HandleFunc("GET /v1/boxes/{box_id}/events", s.handleTimeline)
	mux.HandleFunc("GET /v1/boxes/{box_id}/pending", s.handlePending)
	mux.HandleFunc("GET /v1/boxes/{box_id}/rejected", s.handleRejected)
	mux.HandleFunc("GET /v1/pending", s.handlePendingAll)
	mux.HandleFunc("GET /v1/rejected", s.handleRejectedAll)
	mux.HandleFunc("GET /healthz", s.handleHealth)
	return s.withLogging(mux)
}

func (s *Server) withLogging(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := s.clock.Now()
		h.ServeHTTP(w, r)
		s.logger.Debug("http", "method", r.Method, "path", r.URL.Path, "duration", s.clock.Now().Sub(start))
	})
}

// handlePostEvents accepts an event submission.
func (s *Server) handlePostEvents(w http.ResponseWriter, r *http.Request) {
	ev, ae := protocol.DecodeEvent(r.Body, protocol.DecodeOptions{
		TimeWindow: s.window,
		Now:        s.clock.Now(),
	})
	if ae != nil {
		writeError(w, ae, protocolStatus(ae))
		return
	}
	result, ae := s.led.Submit(ev)
	if ae != nil {
		writeError(w, ae, protocolStatus(ae))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resultStatus(result))
	_ = json.NewEncoder(w).Encode(result)
}

func (s *Server) handleGetBox(w http.ResponseWriter, r *http.Request) {
	boxID := r.PathValue("box_id")
	if boxID == "" {
		writeError(w, apperr.SchemaViolation("box_id is required"), http.StatusBadRequest)
		return
	}
	box, err := s.led.Coordinator().GetBox(boxID)
	if err != nil {
		writeError(w, apperr.StorageFailure(err.Error()), http.StatusInternalServerError)
		return
	}
	if box == nil {
		writeError(w, apperr.New(apperr.CodeSchemaViolation, apperr.CategoryDomain, "box not found", false), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(box)
}

func (s *Server) handleTimeline(w http.ResponseWriter, r *http.Request) {
	boxID := r.PathValue("box_id")
	cursor := r.URL.Query().Get("cursor")
	decodedCursor, err := coordinator.DecodeCursor(cursor)
	if err != nil || decodedCursor != nil && (decodedCursor.OccurredAt.IsZero() || decodedCursor.TerminalID == "" || decodedCursor.TerminalSequence <= 0 || decodedCursor.EventID == "") {
		message := "invalid cursor"
		if err != nil {
			message += ": " + err.Error()
		}
		writeError(w, apperr.SchemaViolation(message), http.StatusBadRequest)
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	page, err := s.led.Coordinator().Timeline(boxID, cursor, limit)
	if err != nil {
		writeError(w, apperr.StorageFailure(err.Error()), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(page)
}

func (s *Server) handlePending(w http.ResponseWriter, r *http.Request) {
	boxID := r.PathValue("box_id")
	pending, err := s.led.Coordinator().Pending(boxID)
	if err != nil {
		writeError(w, apperr.StorageFailure(err.Error()), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"pending": pending, "count": len(pending)})
}

func (s *Server) handleRejected(w http.ResponseWriter, r *http.Request) {
	boxID := r.PathValue("box_id")
	rejected, err := s.led.Coordinator().Rejected(boxID)
	if err != nil {
		writeError(w, apperr.StorageFailure(err.Error()), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"rejected": rejected, "count": len(rejected)})
}

func (s *Server) handlePendingAll(w http.ResponseWriter, r *http.Request) {
	pending, err := s.led.Coordinator().Pending("")
	if err != nil {
		writeError(w, apperr.StorageFailure(err.Error()), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"pending": pending, "count": len(pending)})
}

func (s *Server) handleRejectedAll(w http.ResponseWriter, r *http.Request) {
	rejected, err := s.led.Coordinator().Rejected("")
	if err != nil {
		writeError(w, apperr.StorageFailure(err.Error()), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"rejected": rejected, "count": len(rejected)})
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "time": s.clock.Now().UTC().Format(time.RFC3339Nano)})
}

// errorBody is the JSON shape of an error response.
type errorBody struct {
	Code      string         `json:"code"`
	Category  string         `json:"category"`
	Message   string         `json:"message"`
	Retryable bool           `json:"retryable"`
	Details   map[string]any `json:"details,omitempty"`
}

func writeError(w http.ResponseWriter, ae *apperr.Error, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorBody{
		Code:      string(ae.Code),
		Category:  string(ae.Category),
		Message:   ae.Message,
		Retryable: ae.Retryable,
		Details:   ae.Details,
	})
}

// protocolStatus maps a protocol/apperr error to an HTTP status code.
func protocolStatus(ae *apperr.Error) int {
	switch ae.Category {
	case apperr.CategoryProtocol:
		return http.StatusBadRequest
	case apperr.CategoryDomain:
		if ae.Code == apperr.CodeDependencyMissing {
			return http.StatusAccepted // 202
		}
		return http.StatusUnprocessableEntity // 422
	case apperr.CategoryConflict:
		return http.StatusConflict // 409
	case apperr.CategoryStorage:
		return http.StatusInternalServerError // 500
	case apperr.CategoryIntegrity:
		return http.StatusInternalServerError // 500
	}
	return http.StatusBadRequest
}

// resultStatus maps a coordinator Result to an HTTP status code.
func resultStatus(r *coordinator.Result) int {
	switch r.Status {
	case coordinator.StatusAccepted, coordinator.StatusDuplicate:
		if r.Revision > 0 || r.Status == coordinator.StatusAccepted {
			return http.StatusOK
		}
		return http.StatusOK
	case coordinator.StatusPending:
		return http.StatusAccepted // 202
	case coordinator.StatusRejected:
		if r.Code == string(apperr.CodeDuplicateConflict) {
			return http.StatusConflict
		}
		if r.Code == string(apperr.CodeRevisionConflict) {
			return http.StatusConflict
		}
		if r.Code == string(apperr.CodeIllegalTransition) {
			return http.StatusUnprocessableEntity
		}
		if r.Code == string(apperr.CodeDependencyMissing) {
			return http.StatusAccepted
		}
		return http.StatusUnprocessableEntity
	}
	return http.StatusOK
}

// AppErrorDomainStatusCode is reserved for callers needing the underlying category
// without importing apperr directly.
const AppErrorDomainStatusCode = http.StatusUnprocessableEntity

// boxStateLabel is used by the health check for diagnostics.
func boxStateLabel(s domain.State) string { return string(s) }

var _ = strings.TrimSpace
