package sqljob

import (
	"reflect"
	"testing"
)

func TestSplitStatements(t *testing.T) {
	cases := []struct {
		name   string
		script string
		want   []string
	}{
		{
			name:   "simple",
			script: "SELECT 1; SELECT 2;",
			want:   []string{"SELECT 1", "SELECT 2"},
		},
		{
			name:   "trailing without semicolon",
			script: "SELECT 1;\nSELECT 2",
			want:   []string{"SELECT 1", "SELECT 2"},
		},
		{
			name:   "semicolon in single quote",
			script: "INSERT INTO t VALUES ('a;b'); SELECT 1;",
			want:   []string{"INSERT INTO t VALUES ('a;b')", "SELECT 1"},
		},
		{
			name:   "escaped single quote",
			script: "SELECT 'it''s; ok'; SELECT 2;",
			want:   []string{"SELECT 'it''s; ok'", "SELECT 2"},
		},
		{
			name:   "line comment",
			script: "SELECT 1; -- a; b\nSELECT 2;",
			want:   []string{"SELECT 1", "-- a; b\nSELECT 2"},
		},
		{
			name:   "block comment nested",
			script: "SELECT 1 /* a; /* nested; */ b */; SELECT 2;",
			want:   []string{"SELECT 1 /* a; /* nested; */ b */", "SELECT 2"},
		},
		{
			name: "dollar quoted function body",
			script: `CREATE FUNCTION f() RETURNS int AS $$
BEGIN
  RETURN 1; -- inner semicolon
END;
$$ LANGUAGE plpgsql;
SELECT f();`,
			want: []string{
				"CREATE FUNCTION f() RETURNS int AS $$\nBEGIN\n  RETURN 1; -- inner semicolon\nEND;\n$$ LANGUAGE plpgsql",
				"SELECT f()",
			},
		},
		{
			name:   "tagged dollar quote",
			script: `SELECT $body$ a; b $body$; SELECT 2;`,
			want:   []string{"SELECT $body$ a; b $body$", "SELECT 2"},
		},
		{
			name:   "dollar param is not a quote",
			script: "SELECT * FROM t WHERE id = $1; SELECT 2;",
			want:   []string{"SELECT * FROM t WHERE id = $1", "SELECT 2"},
		},
		{
			name:   "empty and whitespace only",
			script: "  ;\n; SELECT 1;",
			want:   []string{"SELECT 1"},
		},
		{
			name:   "trailing comment only is dropped",
			script: "SELECT 1;\n-- done, nothing after this\n",
			want:   []string{"SELECT 1"},
		},
		{
			name:   "block comment only is dropped",
			script: "SELECT 1; /* trailing note; still a comment */",
			want:   []string{"SELECT 1"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := splitStatements(tc.script)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("splitStatements() =\n%#v\nwant\n%#v", got, tc.want)
			}
		})
	}
}
