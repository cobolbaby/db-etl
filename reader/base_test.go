package reader

import (
	"db-etl/config"
	"db-etl/util"
	"strings"
	"testing"
)

func TestBuildWhereClauseIgnoresPlaceholderWhenEmptyResult(t *testing.T) {
	reader := &BaseReader{Source: &config.SourceConfig{WhereStatement: "updated_at > ${INCR_POINT}"}}
	clause := reader.buildWhereClause(true)
	if clause != "1=0" {
		t.Fatalf("expected 1=0, got %q", clause)
	}
}

func TestResolveProjectionForMSSQL(t *testing.T) {
	source := &config.SourceConfig{
		FieldsMapping: config.FieldsMapping{Items: map[string]string{
			"CamelCase":    "TargetName",
			"GETDATE()":    "cdt",
			"dbo.sourceId": "ID",
		}},
	}

	reader := &BaseReader{Source: source, dialect: mssqlDialect{}}
	projection, err := reader.resolveProjection()
	if err != nil {
		t.Fatalf("resolveProjection returned error: %v", err)
	}

	for _, want := range []string{
		"[CamelCase] AS [TargetName]",
		"GETDATE() AS [cdt]",
		"dbo.sourceId AS [ID]",
	} {
		if !strings.Contains(projection, want) {
			t.Fatalf("projection %q does not contain %q", projection, want)
		}
	}
}

func TestResolveProjectionForPostgres(t *testing.T) {
	source := &config.SourceConfig{
		FieldsMapping: config.FieldsMapping{Items: map[string]string{
			"CamelCase":      "TargetName",
			"now()":          "cdt",
			"public.OrderID": "OrderID",
		}},
	}

	reader := &BaseReader{Source: source, dialect: pgDialect{}}
	projection, err := reader.resolveProjection()
	if err != nil {
		t.Fatalf("resolveProjection returned error: %v", err)
	}

	for _, want := range []string{
		`"CamelCase" AS "TargetName"`,
		`now() AS "cdt"`,
		`public.OrderID AS "OrderID"`,
	} {
		if !strings.Contains(projection, want) {
			t.Fatalf("projection %q does not contain %q", projection, want)
		}
	}
}

func TestDefaultColumnHandlerDistinguishesNilAndEmptyString(t *testing.T) {
	if got := defaultColumnHandler(nil); got != util.NullSentinel {
		t.Fatalf("expected nil to map to null sentinel %q, got %q", util.NullSentinel, got)
	}

	if got := defaultColumnHandler(""); got != "" {
		t.Fatalf("expected empty string to remain empty, got %q", got)
	}
}
