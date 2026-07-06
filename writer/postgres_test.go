package writer

import (
	"strings"
	"testing"

	"db-etl/util"
)

func TestBuildCopySQLUsesReservedNullSentinel(t *testing.T) {
	sql := buildCopySQL("public.orders", []string{"id", "name"})
	if !strings.Contains(sql, "NULL '"+util.NullSentinel+"'") {
		t.Fatalf("expected copy SQL to use reserved null sentinel, got %q", sql)
	}
}
