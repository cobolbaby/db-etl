package writer

import (
	"context"
	"testing"

	"db-etl/config"
	"db-etl/reader"
)

type stubWriterDialect struct {
	called           string
	fullTruncate     bool
	truncateCalled   bool
	recordSyncOnce   bool
	recordSyncStatus int
	recordSyncCalled bool
}

func (d *stubWriterDialect) writeFull(ctx context.Context, in <-chan reader.Batch, truncate bool) error {
	d.called = "full"
	d.fullTruncate = truncate
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

func (d *stubWriterDialect) truncateTarget(ctx context.Context) error {
	d.truncateCalled = true
	return nil
}

func (d *stubWriterDialect) getWatermark(source *config.SourceConfig) (string, error) {
	return "", nil
}

func (d *stubWriterDialect) finalizeSyncResult(ctx context.Context, source *config.SourceConfig, status int, once bool) error {
	d.recordSyncCalled = true
	d.recordSyncStatus = status
	d.recordSyncOnce = once
	return nil
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

	if !dialect.fullTruncate {
		t.Fatalf("expected truncate=true when source is nil")
	}
}

func TestBaseWriterFullModeSkipsTruncateForNonFirstSource(t *testing.T) {
	dialect := &stubWriterDialect{}
	writer := &BaseWriter{
		Target:  &config.TargetConfig{Table: "public.orders", Mode: config.ModeTypeFull},
		dialect: dialect,
	}

	in := make(chan reader.Batch)
	close(in)

	source := &config.SourceConfig{SkipTargetTruncate: true}
	if err := writer.WriteBatch(context.Background(), source, in); err != nil {
		t.Fatalf("WriteBatch returned error: %v", err)
	}

	if dialect.fullTruncate {
		t.Fatalf("expected truncate=false when source.SkipTargetTruncate is set")
	}
}

func TestBaseWriterTruncateTarget(t *testing.T) {
	dialect := &stubWriterDialect{}
	writer := &BaseWriter{
		Target:  &config.TargetConfig{Table: "public.orders", Mode: config.ModeTypeFull},
		dialect: dialect,
	}

	if err := writer.TruncateTarget(context.Background()); err != nil {
		t.Fatalf("TruncateTarget returned error: %v", err)
	}

	if !dialect.truncateCalled {
		t.Fatalf("expected dialect.truncateTarget to be called")
	}
}

func TestBaseWriterFullModeExplicitNoTruncate(t *testing.T) {
	dialect := &stubWriterDialect{}
	writer := &BaseWriter{
		Target:  &config.TargetConfig{Table: "public.orders", Mode: config.ModeTypeFull, SkipTruncate: true},
		dialect: dialect,
	}

	in := make(chan reader.Batch)
	close(in)

	if err := writer.WriteBatch(context.Background(), nil, in); err != nil {
		t.Fatalf("WriteBatch returned error: %v", err)
	}

	if dialect.fullTruncate {
		t.Fatalf("expected truncate=false when target.SkipTruncate is explicitly set to true")
	}
}

func TestBaseWriterWriteBatchDoesNotFinalizeSyncResult(t *testing.T) {
	// deactivateOnceJob 已并入 FinalizeSyncResult（由 main.go 在整条 pipeline 结束后
	// 单独调用），WriteBatch 本身不再负责 once/收尾逻辑，只做数据写入分发。
	dialect := &stubWriterDialect{}
	writer := &BaseWriter{
		Target:  &config.TargetConfig{Table: "public.orders", Mode: config.ModeTypeAppend, Once: true},
		dialect: dialect,
	}

	in := make(chan reader.Batch)
	close(in)

	source := &config.SourceConfig{IncrField: "updated_at"}
	if err := writer.WriteBatch(context.Background(), source, in); err != nil {
		t.Fatalf("WriteBatch returned error: %v", err)
	}

	if dialect.called != "append" {
		t.Fatalf("expected append mode dispatch, got %q", dialect.called)
	}

	if dialect.recordSyncCalled {
		t.Fatalf("expected WriteBatch not to call finalizeSyncResult; that's now the caller's responsibility")
	}
}

func TestBaseWriterFinalizeSyncResultForwardsOnce(t *testing.T) {
	dialect := &stubWriterDialect{}
	writer := &BaseWriter{
		Target:  &config.TargetConfig{Table: "public.orders", Mode: config.ModeTypeAppend, Once: true},
		dialect: dialect,
	}

	source := &config.SourceConfig{IncrField: "updated_at"}
	if err := writer.FinalizeSyncResult(source, SyncStatusSuccess, true); err != nil {
		t.Fatalf("FinalizeSyncResult returned error: %v", err)
	}

	if !dialect.recordSyncCalled {
		t.Fatalf("expected dialect.finalizeSyncResult to be called")
	}
	if !dialect.recordSyncOnce {
		t.Fatalf("expected once=true to be forwarded to dialect.finalizeSyncResult")
	}
	if dialect.recordSyncStatus != SyncStatusSuccess {
		t.Fatalf("expected status=%d, got %d", SyncStatusSuccess, dialect.recordSyncStatus)
	}
}
