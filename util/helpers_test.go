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

func TestSanitizeStringQuotesCarriageReturn(t *testing.T) {
	if got := SanitizeString("abc\rdef"); got != "\"abc\rdef\"" {
		t.Fatalf("expected carriage return to be quoted, got %q", got)
	}
	if got := SanitizeString("abc\r\ndef"); got != "\"abc\r\ndef\"" {
		t.Fatalf("expected CRLF to be quoted, got %q", got)
	}
}
