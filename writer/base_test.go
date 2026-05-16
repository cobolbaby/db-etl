package writer

import (
	"strings"
	"testing"

	"db-etl/config"
	"db-etl/transform"
)

type stubWriterDialect struct {
	called string
}

func (d *stubWriterDialect) writeCopy(in <-chan transform.CSVBatch, target *config.TargetConfig) error {
	d.called = "copy"
	return nil
}

func (d *stubWriterDialect) writeFull(in <-chan transform.CSVBatch, target *config.TargetConfig) error {
	d.called = "full"
	return nil
}

func (d *stubWriterDialect) writeAppend(in <-chan transform.CSVBatch, target *config.TargetConfig, source *config.SourceConfig, jobName string) error {
	d.called = "append"
	return nil
}

func (d *stubWriterDialect) writeMerge(in <-chan transform.CSVBatch, target *config.TargetConfig, source *config.SourceConfig, jobName string) error {
	d.called = "merge"
	return nil
}

func (d *stubWriterDialect) getWatermark(target *config.TargetConfig, source *config.SourceConfig, jobName string) (string, error) {
	return "", nil
}

func TestSourceIdentityAllowsThreePartTableForMSSQL(t *testing.T) {
	source := &config.SourceConfig{
		Table:  "sales.dbo.orders",
		DBType: config.DBTypeMSSQL,
	}

	identity, err := sourceIdentity(source)
	if err != nil {
		t.Fatalf("expected mssql three-part table to be allowed, got %v", err)
	}

	if identity.Database != "sales" || identity.Schema != "dbo" || identity.Table != "orders" {
		t.Fatalf("unexpected source identity: %#v", identity)
	}
}

func TestSourceIdentityRejectsThreePartTableForNonMSSQL(t *testing.T) {
	source := &config.SourceConfig{
		Table:  "sales.public.orders",
		DBType: config.DBTypePG,
	}

	_, err := sourceIdentity(source)
	if err == nil {
		t.Fatal("expected non-mssql three-part table to be rejected")
	}
	if !strings.Contains(err.Error(), "only supported for mssql sources") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestTargetIdentityRejectsThreePartTable(t *testing.T) {
	target := &config.TargetConfig{Table: "sales.public.orders"}

	_, err := targetIdentity(target)
	if err == nil {
		t.Fatal("expected three-part target table to be rejected")
	}
	if !strings.Contains(err.Error(), "target table must be table or schema.table") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBaseWriterDispatchesFullMode(t *testing.T) {
	dialect := &stubWriterDialect{}
	writer := &BaseWriter{
		Target:  &config.TargetConfig{Table: "public.orders", Mode: config.ModeTypeFull},
		dialect: dialect,
	}

	in := make(chan transform.CSVBatch)
	close(in)

	if err := writer.WriteBatch(nil, in); err != nil {
		t.Fatalf("WriteBatch returned error: %v", err)
	}

	if dialect.called != "full" {
		t.Fatalf("expected full mode dispatch, got %q", dialect.called)
	}
}