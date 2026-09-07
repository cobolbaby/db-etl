package writer

import (
	"context"
	"testing"

	"db-etl/config"
	"db-etl/reader"
)

type stubWriterDialect struct {
	called string
}

func (d *stubWriterDialect) writeInitial(ctx context.Context, in <-chan reader.Batch, source *config.SourceConfig) error {
	d.called = "initial"
	return nil
}

func (d *stubWriterDialect) writeFull(ctx context.Context, in <-chan reader.Batch) error {
	d.called = "full"
	return nil
}

func (d *stubWriterDialect) writeAppend(ctx context.Context, in <-chan reader.Batch, source *config.SourceConfig) error {
	d.called = "append"
	return nil
}

func (d *stubWriterDialect) writeMerge(ctx context.Context, in <-chan reader.Batch, source *config.SourceConfig) error {
	d.called = "merge"
	return nil
}

func (d *stubWriterDialect) getWatermark(source *config.SourceConfig) (string, error) {
	return "", nil
}

func (d *stubWriterDialect) close(ctx context.Context) error {
	d.called = "close"
	return nil
}

func TestBaseWriterDispatchesFullMode(t *testing.T) {
	dialect := &stubWriterDialect{}
	writer := &BaseWriter{
		Target:  &config.TargetConfig{Table: "public.orders", Mode: config.ModeTypeFull},
		dialect: dialect,
	}

	in := make(chan reader.Batch)
	close(in)

	if err := writer.WriteBatch(context.Background(), nil, in); err != nil {
		t.Fatalf("WriteBatch returned error: %v", err)
	}

	if dialect.called != "full" {
		t.Fatalf("expected full mode dispatch, got %q", dialect.called)
	}
}
