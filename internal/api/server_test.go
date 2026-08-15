package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"medcold-handoff-ledger/internal/apperr"
	"medcold-handoff-ledger/internal/domain"
	"medcold-handoff-ledger/internal/infra"
	"medcold-handoff-ledger/internal/ledger"
	"medcold-handoff-ledger/internal/protocol"
)

var at0 = time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)

func newServer(t *testing.T) (*Server, *infra.FixedClock) {
	t.Helper()
	dir := t.TempDir()
	clock := infra.NewFixedClock(at0)
	l, err := ledger.Open(ledger.Config{
		DataDir: dir,
		Clock:   clock,
		IDs:     infra.NewSequenceIDSource("evt"),
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	srv := New(l, Options{
		Clock:  clock,
		Window: protocol.TimeWindow{MaxPast: 24 * time.Hour, MaxFuture: 24 * time.Hour},
	})
	return srv, clock
}

func do(t *testing.T, srv *Server, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	return rec
}

func validBoxCreated() string {
	return `{
		"protocol_version": "1.0",
		"idempotency_key": "k1",
		"terminal_id": "T1",
		"terminal_sequence": 1,
		"box_id": "B1",
		"event_type": "box_created",
		"role": "pharmacy",
		"occurred_at": "2026-01-01T09:00:00Z",
		"payload": {
			"product_name": "Insulin",
			"batch": "B1",
			"origin": "DEPOT",
			"destination": "PHARM",
			"carrier_id": "C1",
			"lower_bound": 2.0,
			"upper_bound": 8.0
		}
	}`
}

func TestSubmitAccept(t *testing.T) {
	srv, _ := newServer(t)
	rec := do(t, srv, "POST", "/v1/events", validBoxCreated())
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var res map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &res)
	if res["status"] != "accepted" {
		t.Fatalf("status field = %v, want accepted", res["status"])
	}
}

func TestSubmitMalformedJSON(t *testing.T) {
	srv, _ := newServer(t)
	rec := do(t, srv, "POST", "/v1/events", "{not json")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	var body map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if body["code"] != string(apperr.CodeMalformedJSON) {
		t.Fatalf("code = %v, want malformed_json", body["code"])
	}
}

func TestSubmitTopLevelJSONStringReturnsClientError(t *testing.T) {
	const body = `"not-an-envelope"`

	t.Run("direct_decode", func(t *testing.T) {
		_, err := protocol.DecodeEvent(strings.NewReader(body), protocol.DecodeOptions{})
		if err == nil || err.Code != apperr.CodeSchemaViolation {
			t.Fatalf("want schema_violation, got %v", err)
		}
	})

	t.Run("http", func(t *testing.T) {
		srv, _ := newServer(t)
		rec := do(t, srv, http.MethodPost, "/v1/events", body)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
		}

		var response map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if response["code"] != string(apperr.CodeSchemaViolation) {
			t.Fatalf("code = %v, want schema_violation", response["code"])
		}

		health := do(t, srv, http.MethodGet, "/healthz", "")
		if health.Code != http.StatusOK {
			t.Fatalf("health status after invalid request = %d, want 200", health.Code)
		}
	})
}

func TestSubmitUnknownField(t *testing.T) {
	srv, _ := newServer(t)
	body := strings.Replace(validBoxCreated(), `"box_id": "B1",`, `"box_id": "B1", "extra": 1,`, 1)
	rec := do(t, srv, "POST", "/v1/events", body)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	var b map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &b)
	if b["code"] != string(apperr.CodeSchemaViolation) {
		t.Fatalf("code = %v, want schema_violation", b["code"])
	}
}

func TestSubmitUnsupportedVersion(t *testing.T) {
	srv, _ := newServer(t)
	body := strings.Replace(validBoxCreated(), `"protocol_version": "1.0"`, `"protocol_version": "2.0"`, 1)
	rec := do(t, srv, "POST", "/v1/events", body)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	var b map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &b)
	if b["code"] != string(apperr.CodeUnsupportedVersion) {
		t.Fatalf("code = %v, want unsupported_version", b["code"])
	}
}

func TestGetBoxNotFound(t *testing.T) {
	srv, _ := newServer(t)
	rec := do(t, srv, "GET", "/v1/boxes/NOPE", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestHealth(t *testing.T) {
	srv, _ := newServer(t)
	rec := do(t, srv, "GET", "/healthz", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}

func TestTimelineAfterSubmit(t *testing.T) {
	srv, _ := newServer(t)
	do(t, srv, "POST", "/v1/events", validBoxCreated())
	rec := do(t, srv, "GET", "/v1/boxes/B1/events?limit=10", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var page map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &page)
	items := page["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("items = %d, want 1", len(items))
	}
}

func TestIdempotentRetriesSameEventID(t *testing.T) {
	srv, _ := newServer(t)
	r1 := do(t, srv, "POST", "/v1/events", validBoxCreated())
	var res1 map[string]any
	_ = json.Unmarshal(r1.Body.Bytes(), &res1)
	r2 := do(t, srv, "POST", "/v1/events", validBoxCreated())
	var res2 map[string]any
	_ = json.Unmarshal(r2.Body.Bytes(), &res2)
	if res1["event_id"] != res2["event_id"] {
		t.Fatalf("idempotent event_id differs: %v vs %v", res1["event_id"], res2["event_id"])
	}
}

func TestIllegalTransition(t *testing.T) {
	srv, _ := newServer(t)
	// box_created then immediately close (illegal from drafted).
	do(t, srv, "POST", "/v1/events", validBoxCreated())
	closeBody := `{
		"protocol_version": "1.0",
		"idempotency_key": "k-close",
		"terminal_id": "T1",
		"terminal_sequence": 2,
		"box_id": "B1",
		"event_type": "closed",
		"role": "pharmacy",
		"occurred_at": "2026-01-01T09:05:00Z",
		"payload": {"reason": "x"}
	}`
	rec := do(t, srv, "POST", "/v1/events", closeBody)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422; body=%s", rec.Code, rec.Body.String())
	}
	var b map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &b)
	if b["code"] != string(apperr.CodeIllegalTransition) {
		t.Fatalf("code = %v, want illegal_transition", b["code"])
	}
	if b["category"] != string(apperr.CategoryDomain) {
		t.Fatalf("category = %v, want domain", b["category"])
	}
}

func TestBoxStateInQuery(t *testing.T) {
	srv, _ := newServer(t)
	submitFullViaAPI(t, srv)
	rec := do(t, srv, "GET", "/v1/boxes/B1", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; body=%s", rec.Code, rec.Body.String())
	}
	var box map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &box)
	if box["state"] != string(domain.StateHandedOver) {
		t.Fatalf("state = %v, want handed_over", box["state"])
	}
	rev, _ := box["revision"].(float64)
	if rev != 5 {
		t.Fatalf("revision = %v, want 5", box["revision"])
	}
}

func submitFullViaAPI(t *testing.T, srv *Server) {
	t.Helper()
	do(t, srv, "POST", "/v1/events", validBoxCreated())
	seg := `{"protocol_version":"1.0","idempotency_key":"k-seg","terminal_id":"T1","terminal_sequence":2,"box_id":"B1","event_type":"segment_started","role":"carrier","occurred_at":"2026-01-01T09:01:00Z","payload":{"segment_id":"SEG1","carrier_id":"C1","origin":"DEPOT","destination":"PHARM"}}`
	do(t, srv, "POST", "/v1/events", seg)
	arr := `{"protocol_version":"1.0","idempotency_key":"k-arr","terminal_id":"T1","terminal_sequence":3,"box_id":"B1","event_type":"arrival_scanned","role":"carrier","occurred_at":"2026-01-01T11:00:00Z","payload":{"location":"PHARM"}}`
	do(t, srv, "POST", "/v1/events", arr)
	so := `{"protocol_version":"1.0","idempotency_key":"k-so","terminal_id":"T1","terminal_sequence":4,"box_id":"B1","event_type":"carrier_signed_out","role":"carrier","occurred_at":"2026-01-01T11:30:00Z","payload":{"party_id":"C1","party_name":"Carrier One"}}`
	do(t, srv, "POST", "/v1/events", so)
	recv := `{"protocol_version":"1.0","idempotency_key":"k-recv","terminal_id":"T1","terminal_sequence":5,"box_id":"B1","event_type":"pharmacy_received","role":"pharmacy","occurred_at":"2026-01-01T12:00:00Z","predecessor_event_id":"k-so","payload":{"party_id":"P1","party_name":"Pharmacy A"}}`
	// Note: predecessor must be the carrier_signed_out event id, which is
	// server-generated here; use terminal-sequence ordering instead by relying
	// on predecessor being the prior accepted event. Since the server generated
	// the event id, we omit predecessor and rely on terminal ordering.
	recv = strings.Replace(recv, `,"predecessor_event_id":"k-so"`, "", 1)
	do(t, srv, "POST", "/v1/events", recv)
}

func TestAuditResponseHasRevision(t *testing.T) {
	srv, _ := newServer(t)
	submitFullViaAPI(t, srv)
	rec := do(t, srv, "GET", "/v1/boxes/B1/events?limit=10", "")
	var page map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &page)
	if page["revision"] == nil {
		t.Fatalf("audit response missing revision")
	}
}

func init() {
	// Ensure the package compiles with bytes import used somewhere.
	_ = bytes.Buffer{}
	_ = filepath.Join
}
