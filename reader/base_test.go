package reader

import (
	"db-etl/config"
	"strings"
	"testing"
	"time"
)

func TestBuildWhereClauseUsesConfiguredStatement(t *testing.T) {
	reader := &BaseReader{Source: &config.SourceConfig{WhereStatement: "updated_at > ${INCR_POINT}"}}
	clause := reader.buildWhereClause()
	if clause != "updated_at > ${INCR_POINT}" {
		t.Fatalf("expected where statement passthrough, got %q", clause)
	}
}

func TestBuildWhereClauseDefaultsToAlwaysTrue(t *testing.T) {
	reader := &BaseReader{Source: &config.SourceConfig{}}
	if clause := reader.buildWhereClause(); clause != "1=1" {
		t.Fatalf("expected 1=1, got %q", clause)
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
		"[故障DC/LC] AS [dclc]",  // 含中文与斜杠的列名须加方括号
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
		`"OrderID" AS [order_id]`,    // 已用双引号引用，原样保留
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

func TestFormatTextRendersByKind(t *testing.T) {
	if got := FormatText(KindString, nil); got != "" {
		t.Fatalf("expected nil to render as empty string, got %q", got)
	}

	if got := FormatText(KindString, []byte("abc")); got != "abc" {
		t.Fatalf("expected text bytes to render as string, got %q", got)
	}

	if got := FormatText(KindBytes, []byte{0xDE, 0xAD}); got != "dead" {
		t.Fatalf("expected binary bytes to render as hex, got %q", got)
	}

	ts := time.Date(2024, 3, 1, 9, 8, 7, 0, time.UTC)
	if got := FormatText(KindTime, ts); got != "2024-03-01 09:08:07" {
		t.Fatalf("expected trailing zeros to be trimmed, got %q", got)
	}
}

func TestPGDialectResolvesArrayColumnKind(t *testing.T) {
	// pgx 对数组列的 DatabaseTypeName 返回 "_" 前缀名称，
	// 其文本字面量已是 COPY 可识别的输入语法，故按字符串透传。
	for _, dbType := range []string{"_INT4", "_text", "_Timestamp", "_numeric"} {
		if got := (pgDialect{}).columnKind(dbType); got != KindString {
			t.Fatalf("type %q: expected KindString, got %v", dbType, got)
		}
	}
}

func TestMSSQLDialectNormalizesUniqueidentifierOnly(t *testing.T) {
	if got := (mssqlDialect{}).valueNormalizer("VARCHAR"); got != nil {
		t.Fatal("expected no normalizer for varchar")
	}

	normalize := (mssqlDialect{}).valueNormalizer("uniqueidentifier")
	if normalize == nil {
		t.Fatal("expected a normalizer for uniqueidentifier")
	}

	// SQL Server 以混合字节序存储前三组，需重排后才是通用 UUID 文本。
	raw := []byte{0x78, 0x56, 0x34, 0x12, 0xBC, 0x9A, 0xF0, 0xDE, 1, 2, 3, 4, 5, 6, 7, 8}
	got, ok := normalize(raw).(string)
	if !ok {
		t.Fatalf("expected normalized value to be a string, got %T", normalize(raw))
	}
	if want := "12345678-9ABC-DEF0-0102-030405060708"; got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}
