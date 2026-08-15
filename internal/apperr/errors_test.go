package apperr

import (
	"errors"
	"testing"
)

func TestErrorString(t *testing.T) {
	e := IllegalTransition("nope")
	if e.Error() != "domain/illegal_transition: nope" {
		t.Fatalf("unexpected string: %s", e.Error())
	}
}

func TestErrorIs(t *testing.T) {
	e := RevisionConflict("stale")
	if !errors.Is(e, &Error{Code: CodeRevisionConflict}) {
		t.Fatalf("errors.Is should match on code")
	}
	if errors.Is(e, &Error{Code: CodeIllegalTransition}) {
		t.Fatalf("errors.Is should not match different code")
	}
}

func TestWithDetailDoesNotMutateSentinel(t *testing.T) {
	base := DuplicateConflict("conflict")
	withDetail := base.WithDetail("event_id", "E1")
	if _, ok := base.Details["event_id"]; ok {
		t.Fatalf("WithDetail mutated the base error")
	}
	if withDetail.Details["event_id"] != "E1" {
		t.Fatalf("detail not set")
	}
	if base.Code != withDetail.Code {
		t.Fatalf("code changed")
	}
}

func TestCategoryMapping(t *testing.T) {
	cases := []struct {
		fn   func(string) *Error
		code Code
		cat  Category
		ret  bool
	}{
		{MalformedJSON, CodeMalformedJSON, CategoryProtocol, false},
		{UnsupportedVersion, CodeUnsupportedVersion, CategoryProtocol, false},
		{SchemaViolation, CodeSchemaViolation, CategoryProtocol, false},
		{TimeOutOfRange, CodeTimeOutOfRange, CategoryProtocol, false},
		{DuplicateConflict, CodeDuplicateConflict, CategoryConflict, false},
		{DependencyMissing, CodeDependencyMissing, CategoryDomain, true},
		{IllegalTransition, CodeIllegalTransition, CategoryDomain, false},
		{RevisionConflict, CodeRevisionConflict, CategoryConflict, true},
		{StorageFailure, CodeStorageFailure, CategoryStorage, true},
		{IntegrityFailure, CodeIntegrityFailure, CategoryIntegrity, false},
	}
	for _, c := range cases {
		e := c.fn("x")
		if e.Code != c.code || e.Category != c.cat || e.Retryable != c.ret {
			t.Fatalf("%s: code=%s cat=%s retry=%v want %s/%s/%v", c.code, e.Code, e.Category, e.Retryable, c.code, c.cat, c.ret)
		}
	}
}

func TestFromStorage(t *testing.T) {
	if FromStorage(nil) != nil {
		t.Fatalf("nil should map to nil")
	}
	if FromStorage(errors.New("boom")).Code != CodeStorageFailure {
		t.Fatalf("raw error should map to storage_failure")
	}
	// Already-classified errors pass through.
	orig := IntegrityFailure("bad")
	if FromStorage(orig).Code != CodeIntegrityFailure {
		t.Fatalf("classified error should pass through")
	}
}

func TestNilError(t *testing.T) {
	var e *Error
	if e.Error() != "<nil>" {
		t.Fatalf("nil error string: %s", e.Error())
	}
}
