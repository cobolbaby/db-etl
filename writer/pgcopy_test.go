package writer

import (
	"db-etl/reader"
	"testing"
	"time"
)

func TestEncodeCopyValueDistinguishesNullFromEmptyString(t *testing.T) {
	if got := encodeCopyValue(reader.KindString, nil); got != nullSentinel {
		t.Fatalf("expected nil to encode as null sentinel %q, got %q", nullSentinel, got)
	}

	if got := encodeCopyValue(reader.KindString, ""); got != "" {
		t.Fatalf("expected empty string to stay empty, got %q", got)
	}
}

func TestEncodeCopyValueQuotesEmbeddedDelimiters(t *testing.T) {
	// 含逗号的 PG 数组字面量必须被引号包裹，否则会破坏 COPY 的列分隔。
	if got := encodeCopyValue(reader.KindString, "{1,2,3}"); got != `"{1,2,3}"` {
		t.Fatalf("expected quoted array literal, got %q", got)
	}
}

func TestEncodeCopyValueUsesHexForBytes(t *testing.T) {
	if got := encodeCopyValue(reader.KindBytes, []byte{0xDE, 0xAD}); got != `\xdead` {
		t.Fatalf("expected bytea hex literal, got %q", got)
	}

	if got := encodeCopyValue(reader.KindBytes, []byte{}); got != `\x` {
		t.Fatalf("expected empty bytea literal, got %q", got)
	}
}

func TestEncodeCopyValueFormatsTime(t *testing.T) {
	ts := time.Date(2024, 3, 1, 9, 8, 7, 123456000, time.UTC)
	if got := encodeCopyValue(reader.KindTime, ts); got != "2024-03-01 09:08:07.123456" {
		t.Fatalf("unexpected timestamp encoding %q", got)
	}
}

func TestSanitizeCSVKeepsReservedNullSentinelAsLiteral(t *testing.T) {
	if got := sanitizeCSV(nullSentinel); got != `"`+nullSentinel+`"` {
		t.Fatalf("expected quoted sentinel, got %q", got)
	}
}

func TestSanitizeCSVReturnsEmptyForBlank(t *testing.T) {
	if got := sanitizeCSV("   "); got != "" {
		t.Fatalf("expected empty string, got %q", got)
	}
}

func TestSanitizeCSVQuotesCarriageReturn(t *testing.T) {
	if got := sanitizeCSV("abc\rdef"); got != "\"abc\rdef\"" {
		t.Fatalf("expected carriage return to be quoted, got %q", got)
	}
	if got := sanitizeCSV("abc\r\ndef"); got != "\"abc\r\ndef\"" {
		t.Fatalf("expected CRLF to be quoted, got %q", got)
	}
}
