// Package apperr defines the machine-readable error taxonomy used across the
// cold-chain handoff ledger. Every error carries a stable code, a coarse
// category, a human-readable message, a retryable hint and optional details.
//
// The taxonomy is deliberately closed: callers switch on Code to decide how to
// react to a failure, while HTTP and CLI layers map the Category to transport
// concerns (status codes, exit codes).
package apperr

import "fmt"

// Code is a stable, machine-readable error identifier.
type Code string

const (
	CodeMalformedJSON      Code = "malformed_json"
	CodeUnsupportedVersion Code = "unsupported_version"
	CodeSchemaViolation    Code = "schema_violation"
	CodeTimeOutOfRange     Code = "time_out_of_range"
	CodeDuplicateConflict  Code = "duplicate_conflict"
	CodeDependencyMissing  Code = "dependency_missing"
	CodeIllegalTransition  Code = "illegal_transition"
	CodeRevisionConflict   Code = "revision_conflict"
	CodeStorageFailure     Code = "storage_failure"
	CodeIntegrityFailure   Code = "integrity_failure"
)

// Category groups codes by their nature for transport-level handling.
type Category string

const (
	CategoryProtocol  Category = "protocol"
	CategoryDomain    Category = "domain"
	CategoryConflict  Category = "conflict"
	CategoryStorage   Category = "storage"
	CategoryIntegrity Category = "integrity"
)

// Error is the canonical, JSON-serialisable ledger error.
type Error struct {
	Code      Code           `json:"code"`
	Category  Category       `json:"category"`
	Message   string         `json:"message"`
	Retryable bool           `json:"retryable"`
	Details   map[string]any `json:"details,omitempty"`
}

// Error implements the error interface.
func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%s/%s: %s", e.Category, e.Code, e.Message)
}

// Is lets errors.Is match on the Code regardless of other fields.
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	if !ok {
		return false
	}
	return e.Code == t.Code
}

// WithDetail returns a shallow copy of e with an extra detail entry. It does
// not mutate the receiver so that package-level sentinels stay pristine.
func (e *Error) WithDetail(k string, v any) *Error {
	out := *e
	if out.Details == nil {
		out.Details = map[string]any{}
	}
	// Copy to avoid aliasing the sentinel's map.
	d := make(map[string]any, len(out.Details)+1)
	for k, v := range out.Details {
		d[k] = v
	}
	d[k] = v
	out.Details = d
	return &out
}

// New constructs an Error. Prefer the named constructors below for clarity.
func New(code Code, category Category, msg string, retryable bool) *Error {
	return &Error{Code: code, Category: category, Message: msg, Retryable: retryable}
}

// Named constructors. Each returns a fresh value so callers may safely call
// WithDetail without corrupting shared state.

func MalformedJSON(msg string) *Error {
	return New(CodeMalformedJSON, CategoryProtocol, msg, false)
}

func UnsupportedVersion(msg string) *Error {
	return New(CodeUnsupportedVersion, CategoryProtocol, msg, false)
}

func SchemaViolation(msg string) *Error {
	return New(CodeSchemaViolation, CategoryProtocol, msg, false)
}

func TimeOutOfRange(msg string) *Error {
	return New(CodeTimeOutOfRange, CategoryProtocol, msg, false)
}

func DuplicateConflict(msg string) *Error {
	return New(CodeDuplicateConflict, CategoryConflict, msg, false)
}

func DependencyMissing(msg string) *Error {
	return New(CodeDependencyMissing, CategoryDomain, msg, true)
}

func IllegalTransition(msg string) *Error {
	return New(CodeIllegalTransition, CategoryDomain, msg, false)
}

func RevisionConflict(msg string) *Error {
	return New(CodeRevisionConflict, CategoryConflict, msg, true)
}

func StorageFailure(msg string) *Error {
	return New(CodeStorageFailure, CategoryStorage, msg, true)
}

func IntegrityFailure(msg string) *Error {
	return New(CodeIntegrityFailure, CategoryIntegrity, msg, false)
}

// FromStorage converts a raw storage error into a classified apperr.Error.
// Callers that need to surface a different classification (for example
// integrity failures detected during verification) should construct the
// apperr.Error explicitly instead.
func FromStorage(err error) *Error {
	if err == nil {
		return nil
	}
	if ae, ok := err.(*Error); ok {
		return ae
	}
	return StorageFailure(err.Error())
}
