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

func TestResolveProjectionQuotesSpacedAndUnicodeColumns(t *testing.T) {
	source := &config.SourceConfig{
		FieldsMapping: config.FieldsMapping{Items: map[string]string{
			"Analysis Result Judge": "analysis_result_judge",
			"Cost Saving":           "cost_saving",
			"故障DC/LC":               "dclc",
			"GETDATE()":             "cdt",
			"dbo.sourceId":          "ID",
		}},
	}

	reader := &BaseReader{Source: source, dialect: mssqlDialect{}}
	projection, err := reader.resolveProjection()
	if err != nil {
		t.Fatalf("resolveProjection returned error: %v", err)
	}

	for _, want := range []string{
		"[Analysis Result Judge] AS [analysis_result_judge]", // 含空格的列名须加方括号
		"[Cost Saving] AS [cost_saving]",
		"[故障DC/LC] AS [dclc]", // 含中文与斜杠的列名须加方括号
		"GETDATE() AS [cdt]",   // 表达式保持原样
		"dbo.sourceId AS [ID]", // 限定名保持原样
	} {
		if !strings.Contains(projection, want) {
			t.Fatalf("projection %q does not contain %q", projection, want)
		}
	}
}

func TestResolveProjectionDoesNotDoubleQuoteAlreadyQuoted(t *testing.T) {
	// 源字段已由配置显式引用（含圆括号的列名只能这样表达），不能再套一层引号。
	source := &config.SourceConfig{
		FieldsMapping: config.FieldsMapping{Items: map[string]string{
			"[Price(USD)]": "priceusd",
			`"OrderID"`:    "order_id",
		}},
	}

	reader := &BaseReader{Source: source, dialect: mssqlDialect{}}
	projection, err := reader.resolveProjection()
	if err != nil {
		t.Fatalf("resolveProjection returned error: %v", err)
	}

	for _, want := range []string{
		"[Price(USD)] AS [priceusd]", // 保持单层方括号，不得变成 [[Price(USD)]]
		`"OrderID" AS [order_id]`,     // 已用双引号引用，原样保留
	} {
		if !strings.Contains(projection, want) {
			t.Fatalf("projection %q does not contain %q", projection, want)
		}
	}
	if strings.Contains(projection, "[[") {
		t.Fatalf("projection %q double-quoted an already-quoted identifier", projection)
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

func TestPGDialectResolvesArrayColumnHandler(t *testing.T) {
	// pgx 对数组列的 DatabaseTypeName 返回 "_" 前缀名称，须走显式的数组 handler。
	for _, dbType := range []string{"_INT4", "_text", "_Timestamp", "_numeric"} {
		handler := pgDialect{}.getColumnHandler(dbType)

		if got := handler(nil); got != util.NullSentinel {
			t.Fatalf("type %q: expected nil -> null sentinel %q, got %q", dbType, util.NullSentinel, got)
		}

		// 含逗号的数组字面量必须被 CSV 引号包裹，避免破坏 COPY 的列分隔。
		if got := handler("{1,2,3}"); got != `"{1,2,3}"` {
			t.Fatalf("type %q: expected quoted array literal, got %q", dbType, got)
		}
	}
}

