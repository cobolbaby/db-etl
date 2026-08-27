package writer

import (
	"context"
	"db-etl/config"
	"db-etl/reader"
	"fmt"
)

type writerDialect interface {
	writeInitial(ctx context.Context, in <-chan reader.Batch, target *config.TargetConfig, source *config.SourceConfig, jobName string) error
	writeFull(ctx context.Context, in <-chan reader.Batch, target *config.TargetConfig) error
	writeAppend(ctx context.Context, in <-chan reader.Batch, target *config.TargetConfig, source *config.SourceConfig, jobName string) error
	writeMerge(ctx context.Context, in <-chan reader.Batch, target *config.TargetConfig, source *config.SourceConfig, jobName string) error
	getWatermark(target *config.TargetConfig, source *config.SourceConfig, jobName string) (string, error)
	// close 释放底层连接。
	close(ctx context.Context) error
}

type BaseWriter struct {
	Target  *config.TargetConfig
	JobName string
	dialect writerDialect
}

func (w *BaseWriter) WriteBatch(ctx context.Context, source *config.SourceConfig, in <-chan reader.Batch) error {
	if w.Target == nil {
		return fmt.Errorf("target config is required")
	}

	switch w.Target.Mode {
	case config.ModeTypeInitial:
		return w.dialect.writeInitial(ctx, in, w.Target, source, w.JobName)
	case config.ModeTypeFull:
		return w.dialect.writeFull(ctx, in, w.Target)
	case config.ModeTypeAppend:
		return w.dialect.writeAppend(ctx, in, w.Target, source, w.JobName)
	case config.ModeTypeMerge:
		if w.Target.PK == "" {
			return fmt.Errorf("pk is required for merge mode")
		}
		return w.dialect.writeMerge(ctx, in, w.Target, source, w.JobName)
	default:
		return fmt.Errorf("unsupported mode: %s", w.Target.Mode)
	}
}

func (w *BaseWriter) GetWatermark(source *config.SourceConfig) (string, error) {
	return w.dialect.getWatermark(w.Target, source, w.JobName)
}

// Close 释放底层数据库连接。
func (w *BaseWriter) Close() error {
	if w.dialect == nil {
		return nil
	}
	return w.dialect.close(context.Background())
}
