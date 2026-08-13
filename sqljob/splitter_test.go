package sqljob

import (
	"reflect"
	"testing"

	"db-etl/config"
)

func TestSplitBySemicolon_Basic(t *testing.T) {
	in := "SELECT 1;\nSELECT 2;\n"
	got := newSplitter(config.DBTypePG).Split(in)
	want := []string{"SELECT 1", "SELECT 2"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v want %#v", got, want)
	}
}

func TestSplitBySemicolon_TrailingWithoutSemicolon(t *testing.T) {
	in := "SELECT 1;\nSELECT 2"
	got := newSplitter(config.DBTypePG).Split(in)
	want := []string{"SELECT 1", "SELECT 2"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v want %#v", got, want)
	}
}

func TestSplitBySemicolon_DollarQuotedFunction(t *testing.T) {
	in := `CREATE OR REPLACE FUNCTION dw.func_score(x int)
RETURNS int AS $$
BEGIN
  RETURN x * 2;
END;
$$ LANGUAGE plpgsql;
REFRESH MATERIALIZED VIEW dw.dim_calendar;`
	got := newSplitter(config.DBTypePG).Split(in)
	if len(got) != 2 {
		t.Fatalf("expected 2 statements, got %d: %#v", len(got), got)
	}
	if got[1] != "REFRESH MATERIALIZED VIEW dw.dim_calendar" {
		t.Fatalf("unexpected second statement: %q", got[1])
	}
}

func TestSplitBySemicolon_TaggedDollarQuote(t *testing.T) {
	in := `DO $body$ BEGIN PERFORM 1; PERFORM 2; END $body$;
SELECT 9;`
	got := newSplitter(config.DBTypePG).Split(in)
	if len(got) != 2 {
		t.Fatalf("expected 2 statements, got %d: %#v", len(got), got)
	}
}

func TestSplitBySemicolon_SemicolonInString(t *testing.T) {
	in := `INSERT INTO t(a) VALUES ('a;b');
SELECT 1;`
	got := newSplitter(config.DBTypePG).Split(in)
	want := []string{"INSERT INTO t(a) VALUES ('a;b')", "SELECT 1"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v want %#v", got, want)
	}
}

func TestSplitBySemicolon_SemicolonInComments(t *testing.T) {
	in := "SELECT 1; -- inline ; comment\n/* block ; comment */ SELECT 2;"
	got := newSplitter(config.DBTypePG).Split(in)
	if len(got) != 2 {
		t.Fatalf("expected 2 statements, got %d: %#v", len(got), got)
	}
}

func TestSplitBySemicolon_PositionalParamsNotDollarQuote(t *testing.T) {
	in := "SELECT $1 WHERE a = $2;\nSELECT 3;"
	got := newSplitter(config.DBTypePG).Split(in)
	if len(got) != 2 {
		t.Fatalf("expected 2 statements, got %d: %#v", len(got), got)
	}
}

func TestSplitMSSQLBatches_GoSeparator(t *testing.T) {
	in := "CREATE TABLE t(id int)\nGO\nINSERT INTO t VALUES (1)\ngo\nSELECT * FROM t"
	got := newSplitter(config.DBTypeMSSQL).Split(in)
	want := []string{"CREATE TABLE t(id int)", "INSERT INTO t VALUES (1)", "SELECT * FROM t"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v want %#v", got, want)
	}
}

func TestSplitMSSQLBatches_GoWithCount(t *testing.T) {
	in := "PRINT 'hi'\nGO 3\nPRINT 'bye'"
	got := newSplitter(config.DBTypeMSSQL).Split(in)
	want := []string{"PRINT 'hi'", "PRINT 'bye'"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v want %#v", got, want)
	}
}

func TestSplitMSSQLBatches_SemicolonNotSplit(t *testing.T) {
	// SQL Server 内批处理不以分号拆分，仅以 GO 分隔。
	in := "SELECT 1; SELECT 2\nGO\nSELECT 3"
	got := newSplitter(config.DBTypeMSSQL).Split(in)
	want := []string{"SELECT 1; SELECT 2", "SELECT 3"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v want %#v", got, want)
	}
}

func TestSplitStatements_EmptyInput(t *testing.T) {
	if got := newSplitter(config.DBTypePG).Split("\n\n  \n"); len(got) != 0 {
		t.Fatalf("expected 0 statements, got %#v", got)
	}
	if got := newSplitter(config.DBTypeMSSQL).Split("GO\n\n"); len(got) != 0 {
		t.Fatalf("expected 0 statements, got %#v", got)
	}
}
