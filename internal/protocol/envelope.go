// Package protocol implements the versioned JSON wire protocol for the
// cold-chain handoff ledger. It performs strict, defensive decoding of the
// event envelope and its typed payload, rejecting unknown fields, duplicate
// JSON keys, trailing data, unsupported versions, missing fields, illegal
// enums and out-of-range timestamps.
//
// The package exposes a single high-level entry point, DecodeEvent, which
// returns a fully validated domain.Event. Lower-level helpers (duplicate-key
// detection, strict unmarshalling) are exported for direct testing.
package protocol

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"medcold-handoff-ledger/internal/apperr"
	"medcold-handoff-ledger/internal/domain"
)

// Envelope is the wire representation of a submitted event.
type Envelope struct {
	ProtocolVersion    string          `json:"protocol_version"`
	EventID            string          `json:"event_id,omitempty"`
	IdempotencyKey     string          `json:"idempotency_key"`
	TerminalID         string          `json:"terminal_id"`
	TerminalSequence   int64           `json:"terminal_sequence"`
	BoxID              string          `json:"box_id"`
	EventType          string          `json:"event_type"`
	Role               string          `json:"role"`
	OccurredAt         time.Time       `json:"occurred_at"`
	PredecessorEventID string          `json:"predecessor_event_id,omitempty"`
	Payload            json.RawMessage `json:"payload"`
}

// DecodeOptions tunes protocol decoding. A zero TimeWindow disables the
// occurred_at bound check.
type DecodeOptions struct {
	TimeWindow TimeWindow
	Now        time.Time
}

// TimeWindow bounds the occurred_at timestamp.
type TimeWindow struct {
	MaxPast   time.Duration
	MaxFuture time.Duration
}

// Contains reports whether t falls inside the window centred on now.
func (w TimeWindow) Contains(now, t time.Time) bool {
	if !t.Before(now) {
		if w.MaxFuture == 0 {
			return true
		}
		return t.Sub(now) <= w.MaxFuture
	}
	if w.MaxPast == 0 {
		return true
	}
	return now.Sub(t) <= w.MaxPast
}

// DecodeEvent reads a request body and returns a validated domain.Event. It is
// the single entry point used by the API layer.
func DecodeEvent(r io.Reader, opts DecodeOptions) (*domain.Event, *apperr.Error) {
	data, err := io.ReadAll(io.LimitReader(r, 1<<20)) // 1 MiB cap
	if err != nil {
		return nil, apperr.MalformedJSON(fmt.Sprintf("read body: %v", err))
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, apperr.MalformedJSON("request body is empty")
	}
	if dup, perr := checkDuplicateKeys(data); perr != nil {
		// A JSON syntax error discovered while scanning is malformed input.
		return nil, apperr.MalformedJSON(perr.Error())
	} else if dup != "" {
		return nil, apperr.SchemaViolation(fmt.Sprintf("duplicate JSON key %q", dup))
	}

	var env Envelope
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&env); err != nil {
		return nil, mapDecodeError(err)
	}
	if dec.More() {
		return nil, apperr.MalformedJSON("request body has trailing data")
	}
	return validateEnvelope(&env, opts)
}

func validateEnvelope(env *Envelope, opts DecodeOptions) (*domain.Event, *apperr.Error) {
	if env.ProtocolVersion == "" {
		return nil, apperr.SchemaViolation("protocol_version is required")
	}
	if env.ProtocolVersion != domain.ProtocolVersion {
		return nil, apperr.UnsupportedVersion(fmt.Sprintf("unsupported protocol_version %q; supported %q", env.ProtocolVersion, domain.ProtocolVersion))
	}
	if env.IdempotencyKey == "" {
		return nil, apperr.SchemaViolation("idempotency_key is required")
	}
	if env.TerminalID == "" {
		return nil, apperr.SchemaViolation("terminal_id is required")
	}
	if env.TerminalSequence <= 0 {
		return nil, apperr.SchemaViolation("terminal_sequence must be positive")
	}
	if env.BoxID == "" {
		return nil, apperr.SchemaViolation("box_id is required")
	}
	if env.Role == "" {
		return nil, apperr.SchemaViolation("role is required")
	}
	role := domain.Role(env.Role)
	if !domain.IsValidRole(role) {
		return nil, apperr.SchemaViolation(fmt.Sprintf("unsupported role %q", env.Role))
	}
	if env.EventType == "" {
		return nil, apperr.SchemaViolation("event_type is required")
	}
	et := domain.EventType(env.EventType)
	if !domain.IsValidEventType(et) {
		return nil, apperr.SchemaViolation(fmt.Sprintf("unsupported event_type %q", env.EventType))
	}
	if env.OccurredAt.IsZero() {
		return nil, apperr.SchemaViolation("occurred_at is required")
	}
	if !opts.TimeWindow.Contains(opts.Now, env.OccurredAt) {
		return nil, apperr.TimeOutOfRange(fmt.Sprintf("occurred_at %s is outside the allowed window", env.OccurredAt.UTC().Format(time.RFC3339Nano)))
	}
	if len(env.Payload) == 0 {
		return nil, apperr.SchemaViolation("payload is required")
	}

	payload := domain.NewPayload(et)
	if payload == nil {
		return nil, apperr.SchemaViolation(fmt.Sprintf("no payload decoder for event_type %q", et))
	}
	if err := strictUnmarshal(env.Payload, payload); err != nil {
		return nil, mapDecodeError(err)
	}
	if err := payload.Validate(); err != nil {
		if ae, ok := err.(*apperr.Error); ok {
			return nil, ae
		}
		return nil, apperr.SchemaViolation(err.Error())
	}

	return &domain.Event{
		EventID:            env.EventID,
		IdempotencyKey:     env.IdempotencyKey,
		TerminalID:         env.TerminalID,
		TerminalSequence:   env.TerminalSequence,
		BoxID:              env.BoxID,
		Type:               et,
		Role:               role,
		OccurredAt:         env.OccurredAt,
		PredecessorEventID: env.PredecessorEventID,
		Payload:            payload,
	}, nil
}

// strictUnmarshal decodes data into v rejecting unknown fields. It is used for
// both the envelope and each typed payload.
func strictUnmarshal(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if dec.More() {
		return fmt.Errorf("payload has trailing data")
	}
	return nil
}

// mapDecodeError converts encoding/json errors into the closest apperr code.
func mapDecodeError(err error) *apperr.Error {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "invalid character") || strings.Contains(msg, "unexpected end of JSON"):
		return apperr.MalformedJSON(msg)
	case strings.Contains(msg, "unknown field"):
		return apperr.SchemaViolation(msg)
	case strings.Contains(msg, "cannot unmarshal"):
		return apperr.SchemaViolation(msg)
	case strings.Contains(msg, "trailing data"):
		return apperr.MalformedJSON(msg)
	case strings.Contains(msg, "unsupported value"):
		// NaN/Infinity surface here.
		return apperr.SchemaViolation(msg)
	}
	return apperr.SchemaViolation(msg)
}

// checkDuplicateKeys scans a JSON document and returns the first duplicate
// object key encountered anywhere in the structure, plus any syntax error.
// The standard library's json.Unmarshal silently keeps the last value for
// duplicate keys; this function makes that rejection explicit.
func checkDuplicateKeys(data []byte) (string, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	return scanTokens(dec, nil)
}

func scanTokens(dec *json.Decoder, stack []map[string]bool) (string, error) {
	tok, err := dec.Token()
	if err != nil {
		if err == io.EOF {
			return "", nil
		}
		return "", err
	}
	switch t := tok.(type) {
	case json.Delim:
		switch t {
		case '{':
			stack = append(stack, map[string]bool{})
			dup, err := scanTokens(dec, stack)
			if err != nil || dup != "" {
				return dup, err
			}
			stack = stack[:len(stack)-1]
		case '[':
			dup, err := scanTokens(dec, stack)
			if err != nil || dup != "" {
				return dup, err
			}
		case '}', ']':
			// End of the current object/array; return to caller.
			return "", nil
		}
	case string:
		if len(stack) == 0 {
			// A string outside an object is a scalar value, not an object key.
			break
		}
		// Inside an object, a string token is a key followed by its value.
		frame := stack[len(stack)-1]
		if frame[t] {
			return t, nil
		}
		frame[t] = true
		if dup, err := scanValue(dec, stack); err != nil || dup != "" {
			return dup, err
		}
		dup, err := scanTokens(dec, stack)
		if err != nil || dup != "" {
			return dup, err
		}
		return "", nil
	default:
		// Scalar at the top level or in an array; nothing else to do here.
	}
	return scanTokens(dec, stack)
}

func scanValue(dec *json.Decoder, stack []map[string]bool) (string, error) {
	tok, err := dec.Token()
	if err != nil {
		return "", err
	}
	if d, ok := tok.(json.Delim); ok {
		switch d {
		case '{':
			stack = append(stack, map[string]bool{})
			dup, err := scanTokens(dec, stack)
			if err != nil || dup != "" {
				return dup, err
			}
			stack = stack[:len(stack)-1]
		case '[':
			dup, err := scanTokens(dec, stack)
			if err != nil || dup != "" {
				return dup, err
			}
		}
	}
	return "", nil
}

// CanonicalPayload returns a stable JSON encoding of a payload for hashing.
// Re-marshalling through the typed struct normalises key order and whitespace
// so that two semantically-equal payloads hash identically.
func CanonicalPayload(p domain.Payload) ([]byte, error) {
	return json.Marshal(p)
}
