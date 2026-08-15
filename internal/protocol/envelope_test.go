package protocol

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"medcold-handoff-ledger/internal/apperr"
	"medcold-handoff-ledger/internal/domain"
)

var now = time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)

func opts() DecodeOptions {
	return DecodeOptions{
		Now: now,
		TimeWindow: TimeWindow{
			MaxPast:   24 * time.Hour,
			MaxFuture: 1 * time.Hour,
		},
	}
}

func validBoxCreatedJSON() string {
	return `{
		"protocol_version": "1.0",
		"idempotency_key": "k1",
		"terminal_id": "T1",
		"terminal_sequence": 1,
		"box_id": "BOX-1",
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

func TestDecodeValid(t *testing.T) {
	ev, err := DecodeEvent(strings.NewReader(validBoxCreatedJSON()), opts())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.BoxID != "BOX-1" {
		t.Fatalf("box_id = %s", ev.BoxID)
	}
	if ev.Type != domain.EventBoxCreated {
		t.Fatalf("type = %s", ev.Type)
	}
	if ev.Role != domain.RolePharmacy {
		t.Fatalf("role = %s", ev.Role)
	}
}

func TestEmptyBody(t *testing.T) {
	_, err := DecodeEvent(strings.NewReader(""), opts())
	if err == nil || err.Code != apperr.CodeMalformedJSON {
		t.Fatalf("want malformed_json, got %v", err)
	}
}

func TestTrailingData(t *testing.T) {
	body := validBoxCreatedJSON() + "\n{}}}"
	_, err := DecodeEvent(strings.NewReader(body), opts())
	if err == nil || err.Code != apperr.CodeMalformedJSON {
		t.Fatalf("want malformed_json for trailing data, got %v", err)
	}
}

func TestDuplicateKeys(t *testing.T) {
	body := strings.Replace(validBoxCreatedJSON(), `"box_id": "BOX-1"`, `"box_id": "BOX-1", "box_id": "BOX-2"`, 1)
	_, err := DecodeEvent(strings.NewReader(body), opts())
	if err == nil || err.Code != apperr.CodeSchemaViolation {
		t.Fatalf("want schema_violation for duplicate key, got %v", err)
	}
}

func TestUnknownField(t *testing.T) {
	body := strings.Replace(validBoxCreatedJSON(), `"box_id": "BOX-1",`, `"box_id": "BOX-1", "unknown_field": 5,`, 1)
	_, err := DecodeEvent(strings.NewReader(body), opts())
	if err == nil || err.Code != apperr.CodeSchemaViolation {
		t.Fatalf("want schema_violation for unknown field, got %v", err)
	}
}

func TestMalformedJSON(t *testing.T) {
	_, err := DecodeEvent(strings.NewReader("{not json"), opts())
	if err == nil || err.Code != apperr.CodeMalformedJSON {
		t.Fatalf("want malformed_json, got %v", err)
	}
}

// TestTopLevelNonObjectRejected covers the case where the request body is a
// valid JSON value that is not an event object. A top-level JSON string used
// to panic ("index out of range") inside the duplicate-key scanner because it
// treated any string token as an object key and indexed an empty stack. All
// such inputs must now be rejected with a stable protocol error and never
// panic.
func TestTopLevelNonObjectRejected(t *testing.T) {
	cases := map[string]string{
		"string": `"a plain top-level string"`,
		"number": "42",
		"bool":   "true",
		"null":   "null",
		"array":  `["a", "b"]`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("decoding top-level %s panicked: %v", name, r)
				}
			}()
			ev, err := DecodeEvent(strings.NewReader(body), opts())
			if err == nil {
				t.Fatalf("want error for top-level %s, got event %+v", name, ev)
			}
			if err.Category != apperr.CategoryProtocol {
				t.Fatalf("want protocol category for top-level %s, got %v", name, err)
			}
		})
	}
}

// TestTopLevelStringRejected pins the reported regression: a top-level JSON
// string must yield a structured schema_violation, not a panic.
func TestTopLevelStringRejected(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("decoding top-level string panicked: %v", r)
		}
	}()
	_, err := DecodeEvent(strings.NewReader(`"a plain top-level string"`), opts())
	if err == nil || err.Code != apperr.CodeSchemaViolation {
		t.Fatalf("want schema_violation for top-level string, got %v", err)
	}
}

func TestWrongType(t *testing.T) {
	body := strings.Replace(validBoxCreatedJSON(), `"terminal_sequence": 1`, `"terminal_sequence": "abc"`, 1)
	_, err := DecodeEvent(strings.NewReader(body), opts())
	if err == nil || err.Code != apperr.CodeSchemaViolation {
		t.Fatalf("want schema_violation for wrong type, got %v", err)
	}
}

func TestMissingField(t *testing.T) {
	body := strings.Replace(validBoxCreatedJSON(), `"box_id": "BOX-1",`, "", 1)
	_, err := DecodeEvent(strings.NewReader(body), opts())
	if err == nil || err.Code != apperr.CodeSchemaViolation {
		t.Fatalf("want schema_violation for missing field, got %v", err)
	}
}

func TestUnsupportedVersion(t *testing.T) {
	body := strings.Replace(validBoxCreatedJSON(), `"protocol_version": "1.0"`, `"protocol_version": "9.9"`, 1)
	_, err := DecodeEvent(strings.NewReader(body), opts())
	if err == nil || err.Code != apperr.CodeUnsupportedVersion {
		t.Fatalf("want unsupported_version, got %v", err)
	}
}

func TestUnsupportedEventType(t *testing.T) {
	body := strings.Replace(validBoxCreatedJSON(), `"event_type": "box_created"`, `"event_type": "teleported"`, 1)
	_, err := DecodeEvent(strings.NewReader(body), opts())
	if err == nil || err.Code != apperr.CodeSchemaViolation {
		t.Fatalf("want schema_violation for unknown event_type, got %v", err)
	}
}

func TestUnsupportedRole(t *testing.T) {
	body := strings.Replace(validBoxCreatedJSON(), `"role": "pharmacy"`, `"role": "wizard"`, 1)
	_, err := DecodeEvent(strings.NewReader(body), opts())
	if err == nil || err.Code != apperr.CodeSchemaViolation {
		t.Fatalf("want schema_violation for unknown role, got %v", err)
	}
}

func TestNaN(t *testing.T) {
	// NaN is not a valid JSON literal, so it is rejected as malformed input
	// (a fixed error code) and never reaches the ledger.
	body := strings.Replace(validBoxCreatedJSON(), `"lower_bound": 2.0`, `"lower_bound": NaN`, 1)
	_, err := DecodeEvent(strings.NewReader(body), opts())
	if err == nil {
		t.Fatalf("want error for NaN, got nil")
	}
	if err.Code != apperr.CodeMalformedJSON && err.Code != apperr.CodeSchemaViolation {
		t.Fatalf("want malformed_json or schema_violation for NaN, got %v", err)
	}
}

func TestTimeOutOfRangeFuture(t *testing.T) {
	body := strings.Replace(validBoxCreatedJSON(), `"2026-01-01T09:00:00Z"`, `"2030-01-01T09:00:00Z"`, 1)
	_, err := DecodeEvent(strings.NewReader(body), opts())
	if err == nil || err.Code != apperr.CodeTimeOutOfRange {
		t.Fatalf("want time_out_of_range, got %v", err)
	}
}

func TestTimeOutOfRangePast(t *testing.T) {
	body := strings.Replace(validBoxCreatedJSON(), `"2026-01-01T09:00:00Z"`, `"2020-01-01T09:00:00Z"`, 1)
	_, err := DecodeEvent(strings.NewReader(body), opts())
	if err == nil || err.Code != apperr.CodeTimeOutOfRange {
		t.Fatalf("want time_out_of_range, got %v", err)
	}
}

func TestPayloadUnknownField(t *testing.T) {
	body := strings.Replace(validBoxCreatedJSON(), `"carrier_id": "C1"`, `"carrier_id": "C1", "extra": true`, 1)
	_, err := DecodeEvent(strings.NewReader(body), opts())
	if err == nil || err.Code != apperr.CodeSchemaViolation {
		t.Fatalf("want schema_violation for payload unknown field, got %v", err)
	}
}

func TestTemperatureContradiction(t *testing.T) {
	body := `{
		"protocol_version": "1.0",
		"idempotency_key": "k1",
		"terminal_id": "T1",
		"terminal_sequence": 1,
		"box_id": "BOX-1",
		"event_type": "temperature_summary_submitted",
		"role": "carrier",
		"occurred_at": "2026-01-01T09:00:00Z",
		"payload": {
			"summary_id": "TS1",
			"sample_start": "2026-01-01T09:00:00Z",
			"sample_end": "2026-01-01T10:00:00Z",
			"sample_count": 5,
			"min_temp": 4,
			"max_temp": 6,
			"avg_temp": 5,
			"excursion_duration": 30000000000,
			"lower_bound": 2,
			"upper_bound": 8
		}
	}`
	_, err := DecodeEvent(strings.NewReader(body), opts())
	if err == nil || err.Code != apperr.CodeSchemaViolation {
		t.Fatalf("want schema_violation for temperature contradiction, got %v", err)
	}
}

func TestCanonicalPayloadStable(t *testing.T) {
	p1 := &domain.BoxCreatedPayload{ProductName: "X", Batch: "B", Origin: "O", Destination: "D", CarrierID: "C", LowerBound: 2, UpperBound: 8}
	c1, _ := CanonicalPayload(p1)
	c2, _ := CanonicalPayload(p1)
	if !bytes.Equal(c1, c2) {
		t.Fatalf("canonical payload not stable")
	}
}
