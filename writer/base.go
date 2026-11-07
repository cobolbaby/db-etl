package writer

import (
	"context"
	"db-etl/config"
	"db-etl/reader"
	"fmt"
)

type writerDialect interface {
	// writeFull 全量覆盖写入；truncate 为 false 时跳过清空，只做 COPY——
	// 供同一 task 下多个 source 合并写同一目标表的场景使用（见 BaseWriter.WriteBatch）。
	writeFull(ctx context.Context, in <-chan reader.Batch, truncate bool) error
	writeAppend(ctx context.Context, in <-chan reader.Batch, source *config.SourceConfig) error
	writeMerge(ctx context.Context, in <-chan reader.Batch, source *config.SourceConfig) error
	getWatermark(source *config.SourceConfig) (string, error)
	// finalizeSyncResult 把最近一次同步结果（成功/失败）登记到 job_data_sync.last_status；
	// once=true 且本次成功时，在同一次更新里顺带把 inuse 置为 false，避免下次调度重复执行
	finalizeSyncResult(ctx context.Context, source *config.SourceConfig, status int, once bool) error
	// truncateTarget 独立执行一次目标清空，不写入数据。供多 source 的 full 任务在
	// source 循环开始前调用一次；DB 目标执行 TRUNCATE/DELETE，对象存储目标为空操作。
	truncateTarget(ctx context.Context) error
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
	case config.ModeTypeFull:
		// 多个 source 合并写同一目标表时，main.go runTask 会在 source 循环开始前单独清空一次，
		// 并把每个 source 的 SkipTargetTruncate 置为 true，避免后一个 source 的 writeFull
		// 把前一个刚写入的数据清掉。Target.ShouldTruncate() 为 false 时（显式 skip_truncate: true）
		// 无论如何都跳过清空。
		truncate := w.Target.ShouldTruncate() && (source == nil || !source.SkipTargetTruncate)
		return w.dialect.writeFull(ctx, in, truncate)
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

// TruncateTarget 独立清空一次目标，不写入数据。供多 source 的 full 任务在
// main.go runTask 的 source 循环开始前调用一次（见该方法的调用方）。
func (w *BaseWriter) TruncateTarget(ctx context.Context) error {
	if w.dialect == nil {
		return nil
	}
	return w.dialect.truncateTarget(ctx)
}

func (w *BaseWriter) GetWatermark(source *config.SourceConfig) (string, error) {
	return w.dialect.getWatermark(source)
}

// FinalizeSyncResult 登记最近一次同步结果（成功 0 / 失败 1）到 manager.job_data_sync.last_status；
// once=true 且本次成功时，顺带把 inuse 置为 false，避免下次调度重复执行该 once 任务。
// 刻意使用独立的 background context：管道因取消/超时失败时，沿用被取消的 ctx 会导致
// 连该状态也写不回；用新 context 保证失败状态（last_status=1）与下线动作仍能落库。
func (w *BaseWriter) FinalizeSyncResult(source *config.SourceConfig, status int, once bool) error {
	if w.dialect == nil {
		return nil
	}
	return w.dialect.finalizeSyncResult(context.Background(), source, status, once)
}

// Close 释放底层数据库连接。
func (w *BaseWriter) Close() error {
	if w.dialect == nil {
		return nil
	}
	return w.dialect.close(context.Background())
}
