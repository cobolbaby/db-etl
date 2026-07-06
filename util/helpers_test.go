package util

import "testing"

func TestSanitizeStringKeepsReservedNullSentinelAsLiteral(t *testing.T) {
	if got := SanitizeString(NullSentinel); got != `"`+NullSentinel+`"` {
		t.Fatalf("expected quoted sentinel, got %q", got)
	}
}

func TestSanitizeStringReturnsEmptyForBlank(t *testing.T) {
	if got := SanitizeString("   "); got != "" {
		t.Fatalf("expected empty string, got %q", got)
	}
}
