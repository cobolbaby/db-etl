package writer

import (
	"context"
	"db-etl/config"
	"db-etl/reader"
	"fmt"
)

type writerDialect interface {
	writeInitial(ctx context.Context, in <-chan reader.Batch, source *config.SourceConfig) error
	writeFull(ctx context.Context, in <-chan reader.Batch) error
	writeAppend(ctx context.Context, in <-chan reader.Batch, source *config.SourceConfig) error
	writeMerge(ctx context.Context, in <-chan reader.Batch, source *config.SourceConfig) error
	getWatermark(source *config.SourceConfig) (string, error)
	// recordSyncResult 把最近一次同步结果（成功/失败）登记到 manager.job_data_sync.last_status。
	recordSyncResult(ctx context.Context, source *config.SourceConfig, status int) error
	// close 释放底层连接。
	close(ctx context.Context) error
}

// BaseWriter 仅保留 Target 用于按 Mode/PK 做写入路由与校验；
// 目标标识、jobName 等写入端不变量由各 dialect 自行持有（构造时注入）。
type BaseWriter struct {
	Target  *config.TargetConfig
	dialect writerDialect
}

func (w *BaseWriter) WriteBatch(ctx context.Context, source *config.SourceConfig, in <-chan reader.Batch) error {
	if w.Target == nil {
		return fmt.Errorf("target config is required")
	}

	switch w.Target.Mode {
	case config.ModeTypeInitial:
		return w.dialect.writeInitial(ctx, in, source)
	case config.ModeTypeFull:
		return w.dialect.writeFull(ctx, in)
	case config.ModeTypeAppend:
		return w.dialect.writeAppend(ctx, in, source)
	case config.ModeTypeMerge:
		if w.Target.PK == "" {
			return fmt.Errorf("pk is required for merge mode")
		}
		return w.dialect.writeMerge(ctx, in, source)
	default:
		return fmt.Errorf("unsupported mode: %s", w.Target.Mode)
	}
}

func (w *BaseWriter) GetWatermark(source *config.SourceConfig) (string, error) {
	return w.dialect.getWatermark(source)
}

// RecordSyncResult 登记最近一次同步结果（成功 0 / 失败 1）到 manager.job_data_sync.last_status。
// 刻意使用独立的 background context：管道因取消/超时失败时，沿用被取消的 ctx 会导致
// 连该状态也写不回；用新 context 保证失败状态（last_status=1）仍能落库。
func (w *BaseWriter) RecordSyncResult(source *config.SourceConfig, status int) error {
	if w.dialect == nil {
		return nil
	}
	return w.dialect.recordSyncResult(context.Background(), source, status)
}

// Close 释放底层数据库连接。
func (w *BaseWriter) Close() error {
	if w.dialect == nil {
		return nil
	}
	return w.dialect.close(context.Background())
}
