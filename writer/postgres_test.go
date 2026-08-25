package writer

import (
	"strings"
	"testing"
)

func TestBuildCopySQLUsesReservedNullSentinel(t *testing.T) {
	sql := buildCopySQL("public.orders", []string{"id", "name"})
	if !strings.Contains(sql, "NULL '"+nullSentinel+"'") {
		t.Fatalf("expected copy SQL to use reserved null sentinel, got %q", sql)
	}
}
